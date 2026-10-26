//! The mover pass over a real filesystem store and a fake cold plane.
//!
//! Each test builds a real [`FsStorage`] in a temporary directory and a
//! [`FakeCold`] implementing [`ObjectContent`] as the cold plane's byte store.
//! The capacity probe is a [`FakeCapacity`] reporting a scripted volume
//! pressure, so a pass's demotion decisions are deterministic. The tests assert
//! the real index, row, cold-copy and file state each pass leaves behind, not a
//! mock of the code under test.

// This target is test context, but its file-level helpers are not `#[test]`
// functions, and clippy's `allow-*-in-tests` keys reach `#[test]` functions
// (and `#[cfg(test)]` modules) only — outside one, test-support code reads as
// production to the lint (docs/style.md §Panics).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: an integration-test target's file-level helpers (docs/style.md §Panics)"
)]

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_core::{
    Class,
    acl::Acl,
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    object,
    storage::{self, BodyStream, BucketOps, ByteRange, ObjectOps, Storage, VersionOps},
    version,
};
use tinio_fs::{FsStorage, testing};
use tinio_tier::{
    Capacity, ColdInfo, Config, Error, FlushMode, HeatEntry, Index, MoveOutcome, Mover, Placement,
    State, Tier, TierStorage,
};
use tinio_util::testing::{body, read_body};
use tokio::{sync::Semaphore, task::yield_now, time::sleep};

/// A gate a fake cold plane's publish waits on: every caller that reaches it
/// while armed counts itself and blocks until the test adds a permit. Unlike a
/// one-shot gate, every caller waits, so two concurrent moves can be observed
/// in flight together.
#[derive(Clone, Default)]
struct Gate {
    armed: Arc<Mutex<Option<Arc<Semaphore>>>>,
    entered: Arc<AtomicUsize>,
}

impl Gate {
    /// Arm the gate and answer the semaphore that releases its callers.
    fn arm(&self) -> Arc<Semaphore> {
        let semaphore = Arc::new(Semaphore::new(0));
        *self.armed.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&semaphore));
        semaphore
    }

    /// Wait while the gate is armed; a gate that was never armed is a no-op.
    async fn pass(&self) {
        let semaphore = self
            .armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(semaphore) = semaphore else {
            return;
        };
        self.entered.fetch_add(1, Ordering::SeqCst);
        let _permit = semaphore.acquire().await.expect("the gate stays open");
    }

    /// How many callers have entered the gate.
    fn entered(&self) -> usize {
        self.entered.load(Ordering::SeqCst)
    }
}

/// The fake plane's object map: `(bucket, key)` → bytes.
type ColdMap = HashMap<(String, String), Vec<u8>>;

/// A key-value byte plane standing in for PostgreSQL's large objects.
#[derive(Clone, Default)]
struct FakeCold {
    inner: Arc<Mutex<ColdMap>>,
    /// A gate on `publish`: a capacity move's copy waits while the test holds
    /// the pass.
    publish_gate: Gate,
    /// When set, every publish fails — the failed-move fixture.
    fail_publish: Arc<AtomicBool>,
    /// The keys whose removal fails — the collection pass's per-object failure
    /// fixture.
    fail_removals: Arc<Mutex<HashSet<(String, String)>>>,
}

impl FakeCold {
    fn insert(&self, bucket: &bucket::Name, key: &object::Key, data: &[u8]) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                (bucket.as_ref().to_owned(), key.as_ref().to_owned()),
                data.to_vec(),
            );
    }

    fn contains(&self, bucket: &bucket::Name, key: &object::Key) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&(bucket.as_ref().to_owned(), key.as_ref().to_owned()))
    }

    fn fail_publish(&self) {
        self.fail_publish.store(true, Ordering::SeqCst);
    }

    /// Make `key`'s cold removal fail — the collection pass's per-object
    /// failure fixture.
    fn fail_remove(&self, bucket: &bucket::Name, key: &object::Key) {
        self.fail_removals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((bucket.as_ref().to_owned(), key.as_ref().to_owned()));
    }

    fn remove(&self, bucket: &bucket::Name, key: &object::Key) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(bucket.as_ref().to_owned(), key.as_ref().to_owned()));
    }
}

#[async_trait]
impl ObjectContent for FakeCold {
    type Error = storage::Error;
    type Staged = FakeStaged;

    async fn stage(
        &self,
        _bucket: &bucket::Name,
        _key: &ContentKey,
    ) -> Result<Self::Staged, Self::Error> {
        Ok(FakeStaged(Vec::new()))
    }

    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Self::Error> {
        self.publish_gate.pass().await;
        if self.fail_publish.load(Ordering::SeqCst) {
            return Err(storage::io(io::Error::other(
                "the fake cold plane refuses the copy",
            )));
        }
        let Some(object_key) = key.object() else {
            return Ok(());
        };
        self.insert(bucket, object_key, &staged.0);
        Ok(())
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        let Some(object_key) = key.object() else {
            return Err(storage::invalid_part(0));
        };
        let Some(data) = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(bucket.as_ref().to_owned(), object_key.as_ref().to_owned()))
            .cloned()
        else {
            return Err(storage::no_such_key(object_key));
        };
        let (start, end) = match range {
            Some(range) => range.resolve(data.len() as u64)?,
            None if data.is_empty() => return Ok(Box::pin(stream::empty())),
            None => (0, data.len() as u64 - 1),
        };
        let window = data[start as usize..=end as usize].to_vec();
        Ok(Box::pin(stream::iter([Ok(Bytes::from(window))])))
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error> {
        if let Some(object_key) = key.object() {
            let id = (bucket.as_ref().to_owned(), object_key.as_ref().to_owned());
            if self
                .fail_removals
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&id)
            {
                return Err(storage::io(io::Error::other(
                    "the fake cold plane refuses the removal",
                )));
            }
            self.remove(bucket, object_key);
        }
        Ok(())
    }

    async fn scan(
        &self,
        _bucket: &bucket::Name,
        _prefix: &str,
        _after: Option<&object::Key>,
        _limit: usize,
    ) -> Result<Vec<ContentEntry>, Self::Error> {
        Ok(Vec::new())
    }

    async fn rename(
        &self,
        _bucket: &bucket::Name,
        _from: &object::Key,
        _to: &object::Key,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn copy(
        &self,
        _bucket: &bucket::Name,
        _from: &object::Key,
        _to: &object::Key,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Self::Error> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(inner
            .get(&(bucket.as_ref().to_owned(), key.as_ref().to_owned()))
            .map(|data| ContentEntry {
                key: key.clone(),
                len: data.len() as u64,
                identity: 0,
                mtime: None,
            }))
    }

    async fn create_bucket(&self, _bucket: &bucket::Name) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(name, _), _| name != bucket.as_ref());
        Ok(())
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(!inner.keys().any(|(name, _)| name == bucket.as_ref()))
    }
}

/// A staged write of the fake plane: the bytes land in a vector.
struct FakeStaged(Vec<u8>);

#[async_trait]
impl StagedObject for FakeStaged {
    type Error = storage::Error;

    async fn write(&mut self, chunk: Bytes) -> Result<(), Self::Error> {
        self.0.extend_from_slice(&chunk);
        Ok(())
    }
}

/// A capacity probe reporting a scripted sequence of volume pressures: each
/// measurement pops the next value, and readings past the script repeat the
/// last one. The total defaults to 1 GiB, large enough that a few tiny test
/// objects never cover a watermark gap; a test that needs the byte
/// accounting to bound a pass sets its own.
#[derive(Debug)]
struct FakeCapacity {
    plan: Mutex<(VecDeque<u8>, u8)>,
    total: u64,
}

impl FakeCapacity {
    /// A probe answering `readings` in order, then repeating the last.
    fn new(readings: impl IntoIterator<Item = u8>) -> Self {
        Self::with_total(readings, 1024 * 1024 * 1024)
    }

    /// A probe answering `readings` in order on a volume of `total` bytes.
    fn with_total(readings: impl IntoIterator<Item = u8>, total: u64) -> Self {
        let plan: VecDeque<u8> = readings.into_iter().collect();
        let last = plan.back().copied().unwrap_or(0);
        Self {
            plan: Mutex::new((plan, last)),
            total,
        }
    }
}

impl Capacity for FakeCapacity {
    fn used_percent(&self) -> Result<u8, Error> {
        let mut plan = self.plan.lock().unwrap_or_else(PoisonError::into_inner);
        let reading = plan.0.pop_front().unwrap_or(plan.1);
        plan.1 = reading;
        Ok(reading)
    }

    fn total_bytes(&self) -> Result<u64, Error> {
        Ok(self.total)
    }
}

/// The runtime configuration every test starts from: small limits,
/// synchronous writes, a one-hour dwell and a five-minute collection grace.
fn config() -> Config {
    Config {
        capacity_bytes: 1024 * 1024,
        max_object_bytes: 64 * 1024,
        flush_mode: FlushMode::Sync,
        batch_bytes: 8 * 1024,
        batch_interval: Duration::from_secs(1),
        heat_window: Duration::from_secs(60),
        promote_reads: 2,
        stats_flush: Duration::from_secs(10),
        high_water_percent: 90,
        low_water_percent: 80,
        // These legs exercise capacity selection and promotion; idle demotion has its own module tests.
        demote_idle: Duration::ZERO,
        dwell: Duration::from_secs(3600),
        scan_interval: Duration::from_secs(60),
        move_concurrency: 1,
        index_compact_percent: 30,
        gc_grace: Duration::from_secs(300),
    }
}

/// A real warm plane, a fake cold plane and the layer's own in-memory index.
struct Fixture {
    root: tempfile::TempDir,
    fs: Arc<FsStorage>,
    cold: FakeCold,
    index: Arc<Index>,
    bucket: bucket::Name,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().expect("a temporary storage root");
        let fs = FsStorage::new(root.path(), testing::fs_options()).expect("the fs store opens");
        let bucket = bucket::name("data").expect("a valid bucket name");
        fs.create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        Self {
            root,
            fs: Arc::new(fs),
            cold: FakeCold::default(),
            index: Arc::new(Index::open_in_memory().expect("the index opens")),
            bucket,
        }
    }

    fn tier(&self) -> TierStorage<FakeCold> {
        self.tier_with(config())
    }

    fn tier_with(&self, config: Config) -> TierStorage<FakeCold> {
        TierStorage::from_parts(
            Arc::clone(&self.fs),
            self.cold.clone(),
            Arc::clone(&self.index),
            config,
        )
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.path().join(self.bucket.as_ref()).join(key)
    }

    /// Write an object through the tier so it starts warm with the placement
    /// and row the write path records.
    async fn put_warm(&self, tier: &TierStorage<FakeCold>, key: &str, data: &[u8]) -> object::Info {
        self.put_into(tier, &self.bucket, key, data).await
    }

    async fn put_into(
        &self,
        tier: &TierStorage<FakeCold>,
        bucket: &bucket::Name,
        key: &str,
        data: &[u8],
    ) -> object::Info {
        let key = object::key(key).expect("a valid object key");
        tier.put_object(bucket, &key, body(data))
            .await
            .expect("the object is written");
        tier.head_object(bucket, &key)
            .await
            .expect("the object is readable")
    }

    /// Plant the settled cold shape (T4): the row and the cold copy, no warm
    /// file, and a live pg placement.
    async fn make_cold(
        &self,
        tier: &TierStorage<FakeCold>,
        key: &str,
        data: &[u8],
    ) -> object::Info {
        let info = self.put_warm(tier, key, data).await;
        fs::remove_file(self.path(key)).expect("the warm file is removed");
        self.cold.insert(&self.bucket, &info.key, data);
        self.place(
            &self.bucket,
            &info.key,
            Placement {
                tier: Tier::Pg,
                state: State::Live,
                cold: Some(ColdInfo::from_info(&info)),
                mtime: info.last_modified,
            },
        );
        // The settled cold object is no candidate; a demote retires it.
        self.index
            .warm_remove(&self.bucket, &info.key, info.last_modified)
            .expect("the warm candidate is retired");
        info
    }

    fn place(&self, bucket: &bucket::Name, key: &object::Key, placement: Placement) {
        self.index
            .set_placement(bucket, key, &placement)
            .expect("the placement is written");
    }

    async fn row(&self, key: &object::Key) -> Option<object::Info> {
        self.fs
            .tier_rows()
            .info(&self.bucket, key)
            .await
            .expect("the row is read")
    }

    /// Stamp `key`'s row with `at` — the warm ordering key the rebuild reads —
    /// replacing its candidate exactly as a write does.
    async fn retime(&self, key: &object::Key, at: SystemTime) {
        let mut info = self.row(key).await.expect("the row exists");
        info.last_modified = at;
        self.fs
            .tier_rows()
            .set_row(&self.bucket, &info, at)
            .await
            .expect("the row is retimed");
        self.index
            .warm_replace(&self.bucket, key, at)
            .expect("the candidate is replaced");
    }

    /// Drop every warm candidate, leaving the rows and placements in place —
    /// the state a restart with a lost warm table starts from.
    fn clear_warm(&self) {
        let warm = self.index.scan_warm(usize::MAX).expect("the scan runs");
        for candidate in warm {
            self.index
                .warm_remove(&candidate.bucket, &candidate.key, candidate.mtime)
                .expect("the candidate is retired");
        }
    }
}

fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

/// Plant a file symlink, answering whether the platform allowed it.
#[cfg(unix)]
fn plant_symlink(target: &Path, link: &Path) -> bool {
    use std::os::unix::fs::symlink;
    symlink(target, link).is_ok()
}

/// Plant a file symlink, answering whether the platform allowed it. Windows
/// needs Developer Mode or the create-symlink privilege.
#[cfg(windows)]
fn plant_symlink(target: &Path, link: &Path) -> bool {
    use std::os::windows::fs::symlink_file;
    symlink_file(target, link).is_ok()
}

/// Wait until the gate has admitted `count` callers.
async fn wait_entered(gate: &Gate, count: usize) {
    for _ in 0..10_000 {
        if gate.entered() >= count {
            return;
        }
        yield_now().await;
    }
    panic!("the gate was never entered");
}

/// Read a warm or cold object's bytes through the layer.
async fn get_bytes(tier: &TierStorage<FakeCold>, fixture: &Fixture, key: &object::Key) -> Vec<u8> {
    let got = tier
        .get_object(&fixture.bucket, key, None)
        .await
        .expect("the object is served");
    read_body(got.body).await.expect("the body reads")
}

#[tokio::test]
async fn capacity_demotes_the_oldest_warm_objects() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let oldest = fixture.put_warm(&tier, "oldest.bin", b"first").await;
    let middle = fixture.put_warm(&tier, "middle.bin", b"second").await;
    let newest = fixture.put_warm(&tier, "newest.bin", b"third").await;
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    fixture.retime(&oldest.key, base).await;
    fixture
        .retime(&middle.key, base + Duration::from_secs(60))
        .await;
    fixture
        .retime(&newest.key, base + Duration::from_secs(120))
        .await;

    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 85, 75])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.demoted, 2, "the pass stops at the low watermark");
    assert_eq!(summary.failed, 0);
    for info in [&oldest, &middle] {
        assert_eq!(
            tier.storage_class(&fixture.bucket, &info.key)
                .await
                .expect("the class reads"),
            Class::StandardIa
        );
    }
    assert_eq!(
        tier.storage_class(&fixture.bucket, &newest.key)
            .await
            .expect("the class reads"),
        Class::Standard,
        "the newest candidate stays warm"
    );
    assert!(fixture.path("newest.bin").exists());
    let warm = fixture.index.scan_warm(10).expect("the scan runs");
    assert_eq!(warm.len(), 1);
    assert_eq!(warm[0].key, newest.key);
}

#[tokio::test]
async fn a_probe_that_cannot_fall_bounds_the_capacity_pass() {
    // The settled demote leaves its warm file for garbage collection, gated by
    // `gc_grace`, so a real probe stays above `high` for the whole pass. The
    // pass must reclaim the measured overflow — 15% of a 1000-byte volume is
    // 150 bytes — and stop, not drain every candidate into the cold plane.
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let payload = [b'x'; 100];
    let mut infos = Vec::new();
    for index in 0..5 {
        infos.push(
            fixture
                .put_warm(&tier, &format!("bounded-{index}.bin"), &payload)
                .await,
        );
    }
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    for (position, info) in infos.iter().enumerate() {
        fixture
            .retime(&info.key, base + Duration::from_secs(position as u64))
            .await;
    }

    let mover = Mover::new(
        tier.clone(),
        Arc::new(FakeCapacity::with_total([95], 1_000)),
        1,
    );
    mover.bind().await.expect("the warm table is rebuilt");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(
        summary.demoted, 2,
        "two 100-byte demotions cover the 150-byte gap to the low watermark"
    );
    assert_eq!(summary.failed, 0);
    for info in &infos[..2] {
        assert_eq!(
            tier.storage_class(&fixture.bucket, &info.key)
                .await
                .expect("the class reads"),
            Class::StandardIa
        );
    }
    for info in &infos[2..] {
        assert_eq!(
            tier.storage_class(&fixture.bucket, &info.key)
                .await
                .expect("the class reads"),
            Class::Standard,
            "the pass leaves the candidates behind its pending reclaim warm"
        );
    }
    let warm = fixture.index.scan_warm(10).expect("the scan runs");
    assert_eq!(
        warm.len(),
        3,
        "three candidates stay queued for the next pass"
    );
}

#[tokio::test]
async fn a_failed_warm_table_bind_is_retried_by_the_next_pass() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    fixture
        .put_warm(&tier, "retry.bin", b"wait for the retry")
        .await;
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.fail_next_bind();

    let first = mover
        .pass()
        .await
        .expect("a bind failure never aborts the pass");
    assert_eq!(first.demoted, 0, "the unbound table selects nothing");
    assert!(!mover.warm_ready());

    let second = mover.pass().await.expect("the next pass retries the bind");
    assert!(
        mover.warm_ready(),
        "the retry bound the warm table without a restart"
    );
    assert_eq!(second.demoted, 1, "candidates flow once the table is bound");
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn a_symlinked_object_path_refuses_the_mtime_stamp() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .put_warm(&tier, "linked.bin", b"the real file")
        .await;
    let target = fixture.root.path().join("outside.bin");
    fs::write(&target, b"outside the root").expect("the target is written");
    let link = fixture.path("linked.bin");
    fs::remove_file(&link).expect("the object's file is removed");
    if !plant_symlink(&target, &link) {
        // No Developer Mode / create-symlink privilege: the refusal is
        // unobservable on this run.
        eprintln!("skipping the symlink refusal: the platform refused to plant the link");
        return;
    }

    let refused = tier
        .set_file_mtime_for_test(&fixture.bucket, &info.key, SystemTime::now())
        .await;

    assert!(
        matches!(&refused, Err(Error::Io(err)) if err.kind() == io::ErrorKind::PermissionDenied),
        "a symlinked object path is refused, not followed: {refused:?}"
    );
    assert!(
        fs::symlink_metadata(&link)
            .expect("the link still exists")
            .file_type()
            .is_symlink(),
        "the refused write left the link alone"
    );
}

#[tokio::test]
async fn a_never_read_object_is_demoted() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .put_warm(&tier, "never-read.bin", b"no heat at all")
        .await;
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");

    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.demoted, 1, "capacity does not need a heat signal");
    assert_eq!(
        tier.storage_class(&fixture.bucket, &info.key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
    assert_eq!(
        tier.heat().score(&fixture.bucket, &info.key),
        0,
        "the object was never read"
    );
    assert!(
        fixture
            .index
            .scan_heat(10)
            .expect("the scan runs")
            .is_empty()
    );
}

#[tokio::test]
async fn a_put_after_bind_is_a_capacity_candidate() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    assert!(
        fixture
            .index
            .scan_warm(10)
            .expect("the scan runs")
            .is_empty()
    );

    let info = fixture
        .put_warm(&tier, "after-bind.bin", b"written after the bind")
        .await;

    let warm = fixture.index.scan_warm(10).expect("the scan runs");
    assert_eq!(warm.len(), 1, "the warm write records a candidate");
    assert_eq!(warm[0].key, info.key);
    let summary = mover.pass().await.expect("the pass runs");
    assert_eq!(summary.demoted, 1, "the pass sees the late write");
    assert_eq!(
        tier.storage_class(&fixture.bucket, &info.key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
    assert!(
        fixture
            .index
            .scan_warm(10)
            .expect("the scan runs")
            .is_empty()
    );
}

#[tokio::test]
async fn an_overwrite_replaces_the_warm_candidate() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let rewritten = fixture.put_warm(&tier, "rewritten.bin", b"first").await;
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    fixture.retime(&rewritten.key, base).await;
    let older = fixture.put_warm(&tier, "older.bin", b"older").await;
    fixture
        .retime(&older.key, base + Duration::from_secs(60))
        .await;

    let updated = fixture.put_warm(&tier, "rewritten.bin", b"second").await;

    assert!(
        updated.last_modified > base + Duration::from_secs(60),
        "the rewrite records a newer time"
    );
    let warm = fixture.index.scan_warm(usize::MAX).expect("the scan runs");
    let rewritten_rows: Vec<_> = warm
        .iter()
        .filter(|candidate| candidate.key == rewritten.key)
        .collect();
    assert_eq!(
        rewritten_rows.len(),
        1,
        "a rewrite replaces the superseded row"
    );
    assert_eq!(rewritten_rows[0].mtime, updated.last_modified);

    let summary = mover.pass().await.expect("the pass runs");
    assert_eq!(summary.demoted, 1, "only the genuinely older object moves");
    assert_eq!(
        tier.storage_class(&fixture.bucket, &older.key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &rewritten.key)
            .await
            .expect("the class reads"),
        Class::Standard,
        "the rewritten object keeps its new age"
    );
}

#[tokio::test]
async fn a_delete_or_cold_move_removes_the_warm_row() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let doomed = fixture.put_warm(&tier, "doomed.bin", b"delete me").await;
    let moved = fixture.put_warm(&tier, "moved.bin", b"demote me").await;
    assert_eq!(
        fixture.index.scan_warm(10).expect("the scan runs").len(),
        2,
        "both writes are candidates"
    );

    tier.delete_object(&fixture.bucket, &doomed.key)
        .await
        .expect("the delete commits");
    let warm = fixture.index.scan_warm(10).expect("the scan runs");
    assert_eq!(warm.len(), 1, "the delete removes its candidate");
    assert_eq!(warm[0].key, moved.key);

    let summary = mover.pass().await.expect("the pass runs");
    assert_eq!(summary.demoted, 1);
    assert!(
        fixture
            .index
            .scan_warm(10)
            .expect("the scan runs")
            .is_empty(),
        "the cold move removes its candidate"
    );
}

#[tokio::test]
async fn dwell_blocks_immediate_re_promotion() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .put_warm(&tier, "dwelling.bin", b"hot but fresh")
        .await;
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let demoted = mover.pass().await.expect("the demoting pass runs");
    assert_eq!(demoted.demoted, 1);

    for _ in 0..config().promote_reads {
        assert_eq!(
            get_bytes(&tier, &fixture, &info.key).await,
            b"hot but fresh",
            "a cold read works"
        );
    }
    let promoting = mover.pass().await.expect("the promoting pass runs");

    assert_eq!(
        promoting.promoted, 0,
        "a just-demoted key is not promoted inside the dwell"
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &info.key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
}

#[tokio::test]
async fn heat_promotes_after_dwell() {
    let fixture = Fixture::new().await;
    let mut config = config();
    config.dwell = Duration::from_millis(20);
    let tier = fixture.tier_with(config.clone());
    let info = fixture.put_warm(&tier, "hot.bin", b"read me again").await;
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let demoted = mover.pass().await.expect("the demoting pass runs");
    assert_eq!(demoted.demoted, 1);

    sleep(Duration::from_millis(40)).await;
    for _ in 0..config.promote_reads {
        assert_eq!(
            get_bytes(&tier, &fixture, &info.key).await,
            b"read me again"
        );
    }
    let promoting = mover.pass().await.expect("the promoting pass runs");

    assert_eq!(promoting.promoted, 1, "the dwell elapsed");
    assert_eq!(
        tier.storage_class(&fixture.bucket, &info.key)
            .await
            .expect("the class reads"),
        Class::Standard
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
    assert_eq!(
        fs::read(fixture.path("hot.bin")).expect("the file exists"),
        b"read me again"
    );
    let warm = fixture.index.scan_warm(10).expect("the scan runs");
    assert_eq!(warm.len(), 1, "the promoted object is a capacity candidate");
    assert_eq!(warm[0].key, info.key);
}

#[tokio::test]
async fn a_heat_promotion_is_not_demoted_in_the_same_pass() {
    // I1: a promoted key re-enters the warm table at its old modification
    // time, so it can be older than every warm candidate. The heat leg's keys
    // are excluded from the pass's demotion, or the promotion would be undone
    // inside the very pass that made it.
    let fixture = Fixture::new().await;
    let mut config = config();
    config.dwell = Duration::from_millis(20);
    let tier = fixture.tier_with(config.clone());
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    // The warm object the capacity leg is allowed to demote. Its recorded time
    // is newer than the cold key's, so it is not the first candidate by itself.
    let warm = fixture.put_warm(&tier, "older.bin", b"older").await;
    fixture
        .retime(&warm.key, base + Duration::from_secs(60))
        .await;

    // The cold object carries the oldest recorded time in the table and its
    // move is well past the dwell, so the heat leg promotes it.
    let hot = fixture.make_cold(&tier, "hot.bin", b"hot").await;
    let mut cold_info = hot.clone();
    cold_info.last_modified = base;
    fixture.place(
        &fixture.bucket,
        &hot.key,
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&cold_info)),
            mtime: base,
        },
    );
    for _ in 0..config.promote_reads {
        assert_eq!(get_bytes(&tier, &fixture, &hot.key).await, b"hot");
    }

    // The readings cover the first pass's probe and its post-demote probe, then
    // a second pass above the high watermark for the eligibility leg below.
    let mover = Mover::new(
        tier.clone(),
        Arc::new(FakeCapacity::new([95, 85, 95, 85])),
        1,
    );
    mover.bind().await.expect("the warm table is rebuilt");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.promoted, 1, "the queued key is promoted");
    assert_eq!(
        summary.demoted, 1,
        "only the warm candidate is demoted in this pass"
    );
    assert_eq!(summary.failed, 0);
    assert_eq!(
        tier.storage_class(&fixture.bucket, &hot.key)
            .await
            .expect("the class reads"),
        Class::Standard,
        "the key this pass promoted is not handed back to the cold plane"
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &warm.key)
            .await
            .expect("the class reads"),
        Class::StandardIa,
        "the older warm candidate is the one demoted"
    );
    let table = fixture.index.scan_warm(10).expect("the scan runs");
    assert_eq!(table.len(), 1);
    assert_eq!(table[0].key, hot.key, "the promotion is the survivor");

    // The exclusion lasts one pass: the next capacity pass may demote the key
    // the previous pass promoted.
    let later = mover.pass().await.expect("the next pass runs");
    assert_eq!(
        later.demoted, 1,
        "the promoted key is an ordinary candidate again next pass"
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &hot.key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
}

#[tokio::test]
async fn the_warm_table_is_rebuilt_after_restart() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let oldest = fixture.put_warm(&tier, "old.bin", b"older").await;
    let newest = fixture.put_warm(&tier, "new.bin", b"newer").await;
    // Read before stamping: a warm read heals a row whose recorded time
    // disagrees with the file, so the read must not follow the retime.
    assert_eq!(
        get_bytes(&tier, &fixture, &oldest.key).await,
        b"older",
        "reads keep working before the rebuild"
    );
    assert_eq!(tier.heat().score(&fixture.bucket, &oldest.key), 1);
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    fixture.retime(&oldest.key, base).await;
    fixture
        .retime(&newest.key, base + Duration::from_secs(60))
        .await;
    fixture.clear_warm();
    assert!(
        fixture
            .index
            .scan_warm(10)
            .expect("the scan runs")
            .is_empty(),
        "a restart starts with an empty warm table"
    );

    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([20, 95, 70])), 1);
    assert!(!mover.warm_ready());

    // A restart needs no explicit bind: the pass retries the rebuild itself.
    let first = mover.pass().await.expect("the first pass runs");
    assert!(mover.warm_ready(), "the pass rebuilt the warm table");
    assert_eq!(
        first.demoted, 0,
        "the quiet volume demotes nothing after the rebuild"
    );
    let rebuilt = fixture.index.scan_warm(10).expect("the scan runs");
    assert_eq!(rebuilt.len(), 2);
    assert_eq!(rebuilt[0].key, oldest.key, "the rebuild keeps mtime order");

    let summary = mover.pass().await.expect("the pass runs");
    assert_eq!(summary.demoted, 1);
    assert_eq!(
        tier.storage_class(&fixture.bucket, &oldest.key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &newest.key)
            .await
            .expect("the class reads"),
        Class::Standard
    );
}

#[tokio::test]
async fn the_heat_queue_is_restored_after_restart() {
    let fixture = Fixture::new().await;
    let mut config = config();
    config.dwell = Duration::ZERO;
    let tier = fixture.tier_with(config);
    let fresh = fixture.make_cold(&tier, "fresh.bin", b"fresh heat").await;
    let stale = fixture.make_cold(&tier, "stale.bin", b"stale heat").await;
    let now = SystemTime::now();
    let window = Duration::from_secs(60);
    fixture
        .index
        .absorb_heat(&[
            HeatEntry {
                bucket: fixture.bucket.clone(),
                key: fresh.key.clone(),
                reads: 5,
                last_read: now,
            },
            HeatEntry {
                bucket: fixture.bucket.clone(),
                key: stale.key.clone(),
                reads: 5,
                last_read: now - window - window,
            },
        ])
        .expect("the durable heat rows are planted");

    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([20])), 1);
    assert!(!mover.warm_ready());
    mover.bind().await.expect("the bind restores the queue");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.promoted, 1, "only the in-window heat promotes");
    assert_eq!(
        tier.storage_class(&fixture.bucket, &fresh.key)
            .await
            .expect("the class reads"),
        Class::Standard
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &fresh.key));
    assert_eq!(
        tier.storage_class(&fixture.bucket, &stale.key)
            .await
            .expect("the class reads"),
        Class::StandardIa,
        "a stale heat row is not restored"
    );
    assert!(fixture.cold.contains(&fixture.bucket, &stale.key));
}

#[tokio::test]
async fn a_versioned_or_suspended_bucket_is_never_moved() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let enabled = bucket::name("enabled").expect("a valid bucket name");
    let suspended = bucket::name("suspended").expect("a valid bucket name");
    for name in [&enabled, &suspended] {
        tier.create_bucket(name, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
    }
    tier.set_bucket_versioning(&enabled, version::State::Enabled)
        .await
        .expect("versioning is enabled");
    tier.set_bucket_versioning(&suspended, version::State::Enabled)
        .await
        .expect("versioning is enabled");
    tier.set_bucket_versioning(&suspended, version::State::Suspended)
        .await
        .expect("versioning is suspended");
    let versioned = fixture
        .put_into(&tier, &enabled, "versioned.bin", b"v")
        .await;
    let suspended_key = fixture
        .put_into(&tier, &suspended, "suspended.bin", b"s")
        .await;

    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.demoted, 0, "a versioned bucket never moves");
    for (name, info) in [(&enabled, &versioned), (&suspended, &suspended_key)] {
        assert_eq!(
            tier.storage_class(name, &info.key)
                .await
                .expect("the class reads"),
            Class::Standard
        );
        assert_eq!(
            fixture
                .index
                .placement(name, &info.key)
                .expect("the placement is read")
                .map(|placement| placement.tier),
            Some(Tier::Fs)
        );
        assert!(!fixture.cold.contains(name, &info.key));
    }
}

#[tokio::test]
async fn a_folder_marker_is_never_moved() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .put_warm(&tier, "object.bin", b"a real object")
        .await;
    let marker = key("dir/");
    tier.put_object(&fixture.bucket, &marker, body(b""))
        .await
        .expect("the folder marker is created");
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    fixture
        .retime(&info.key, base + Duration::from_secs(60))
        .await;
    // The fs row scan never records a marker, so plant the oldest candidate
    // directly: a stale warm row the selection must discard.
    fixture
        .index
        .warm_insert(&fixture.bucket, &marker, base)
        .expect("the marker candidate is planted");
    assert!(
        fixture
            .root
            .path()
            .join(fixture.bucket.as_ref())
            .join("dir")
            .is_dir()
    );

    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.demoted, 1, "the real object is demoted");
    assert_eq!(
        tier.storage_class(&fixture.bucket, &marker)
            .await
            .expect("the class reads"),
        Class::Standard,
        "the marker is never moved"
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &marker));
    assert!(
        fixture
            .index
            .scan_warm(10)
            .expect("the scan runs")
            .iter()
            .all(|candidate| candidate.key != marker),
        "the stale marker candidate is discarded"
    );
}

#[tokio::test]
async fn concurrency_two_moves_distinct_keys() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let first = fixture.put_warm(&tier, "first.bin", b"first").await;
    let second = fixture.put_warm(&tier, "second.bin", b"second").await;
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    fixture.retime(&first.key, base).await;
    fixture
        .retime(&second.key, base + Duration::from_secs(60))
        .await;
    let release = fixture.cold.publish_gate.arm();
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 2);
    mover.bind().await.expect("the warm table is rebuilt");

    let running = tokio::spawn(async move { mover.pass().await });
    wait_entered(&fixture.cold.publish_gate, 2).await;
    for _ in 0..32 {
        yield_now().await;
    }
    assert_eq!(
        fixture.cold.publish_gate.entered(),
        2,
        "move_concurrency bounds the copies in flight"
    );

    release.add_permits(2);
    let summary = running
        .await
        .expect("the pass task joins")
        .expect("the pass runs");
    assert_eq!(summary.demoted, 2);
    for info in [&first, &second] {
        assert_eq!(
            tier.storage_class(&fixture.bucket, &info.key)
                .await
                .expect("the class reads"),
            Class::StandardIa
        );
    }
    assert!(fixture.cold.contains(&fixture.bucket, &first.key));
    assert!(fixture.cold.contains(&fixture.bucket, &second.key));
}

#[tokio::test]
async fn gc_runs_on_the_configured_cadence() {
    let fixture = Fixture::new().await;
    let mut config = config();
    config.gc_grace = Duration::from_millis(50);
    let tier = fixture.tier_with(config);
    let info = fixture
        .make_cold(&tier, "leftover.bin", b"cold bytes")
        .await;
    let now = SystemTime::now();
    fixture.place(
        &fixture.bucket,
        &info.key,
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: now - Duration::from_secs(10),
        },
    );
    fs::write(fixture.path("leftover.bin"), b"cold bytes").expect("the leftover is planted");
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([20])), 1);

    let first = mover
        .gc()
        .await
        .expect("the first call answers")
        .expect("the first pass is due");
    assert_eq!(first.files, 1, "the leftover file is collected");
    assert!(!fixture.path("leftover.bin").exists());

    fs::write(fixture.path("leftover.bin"), b"cold bytes").expect("the leftover is replanted");
    let gated = mover.gc().await.expect("the second call answers");
    assert_eq!(gated, None, "the cadence has not elapsed");
    assert!(
        fixture.path("leftover.bin").exists(),
        "a gated call collects nothing"
    );

    sleep(Duration::from_millis(80)).await;
    let due = mover
        .gc()
        .await
        .expect("the third call answers")
        .expect("the cadence elapsed");
    assert_eq!(due.files, 1);
    assert!(!fixture.path("leftover.bin").exists());
}

#[tokio::test]
async fn a_failed_move_leaves_placement_unchanged() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture.put_warm(&tier, "failing.bin", b"the bytes").await;
    let warm = Placement {
        tier: Tier::Fs,
        state: State::Live,
        cold: None,
        mtime: info.last_modified,
    };
    fixture.place(&fixture.bucket, &info.key, warm.clone());
    fixture.cold.fail_publish();
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");

    let summary = mover.pass().await.expect("the pass survives the failure");

    assert_eq!(summary.failed, 1);
    assert_eq!(summary.demoted, 0);
    assert_eq!(
        fixture
            .index
            .placement(&fixture.bucket, &info.key)
            .expect("the placement is read"),
        Some(warm),
        "the failed copy restores the placement it replaced"
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
    assert_eq!(
        fs::read(fixture.path("failing.bin")).expect("the file survives"),
        b"the bytes"
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &info.key)
            .await
            .expect("the class reads"),
        Class::Standard
    );
    assert_eq!(
        fixture.index.scan_warm(10).expect("the scan runs").len(),
        1,
        "the failed candidate is retried by the next pass"
    );
}

/// A heat-promotion candidate in a versioned bucket is skipped: it stays cold
/// with its placement intact, and the skip does not requeue it (T11).
#[tokio::test]
async fn a_promotion_candidate_in_a_versioned_bucket_is_skipped_and_stays_cold() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let versioned = bucket::name("versioned").expect("a valid bucket name");
    tier.create_bucket(&versioned, None, &Acl::default_private(None))
        .await
        .expect("the versioned bucket is created");
    tier.set_bucket_versioning(&versioned, version::State::Enabled)
        .await
        .expect("versioning is enabled");

    let info = fixture.put_into(&tier, &versioned, "hot.bin", b"hot").await;
    // The settled cold shape is planted directly: a demote itself refuses a
    // bucket that records versions.
    fs::remove_file(fixture.root.path().join(versioned.as_ref()).join("hot.bin"))
        .expect("the warm file is removed");
    fixture.cold.insert(&versioned, &info.key, b"hot");
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let cold = Placement {
        tier: Tier::Pg,
        state: State::Live,
        cold: Some(ColdInfo::from_info(&info)),
        mtime: base,
    };
    fixture.place(&versioned, &info.key, cold.clone());
    fixture
        .index
        .warm_remove(&versioned, &info.key, info.last_modified)
        .expect("the warm candidate is retired");
    // The placement is well past the dwell, so only the versioning rule can
    // skip this promotion.
    for _ in 0..config().promote_reads {
        let got = tier
            .get_object(&versioned, &info.key, None)
            .await
            .expect("the cold object is read");
        assert_eq!(read_body(got.body).await.expect("the body reads"), b"hot");
    }

    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([20])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.promoted, 0, "a versioned bucket never promotes");
    assert_eq!(summary.skipped, 1, "the queued candidate is a counted skip");
    assert_eq!(summary.failed, 0);
    assert_eq!(
        fixture
            .index
            .placement(&versioned, &info.key)
            .expect("the placement is read"),
        Some(cold),
        "the skipped promotion leaves the placement untouched"
    );
    assert_eq!(
        tier.storage_class(&versioned, &info.key)
            .await
            .expect("the class reads"),
        Class::StandardIa,
        "the object stays cold"
    );
    assert!(fixture.cold.contains(&versioned, &info.key));
    assert!(
        !fixture
            .root
            .path()
            .join(versioned.as_ref())
            .join("hot.bin")
            .exists(),
        "the skip copies no bytes warm"
    );
    assert!(
        tier.heat().promote_queue().is_empty(),
        "the skip does not requeue the candidate"
    );
}

/// A warm candidate whose row vanished is retired without failing the pass:
/// the stale entry leaves the table and nothing is copied (T21).
#[tokio::test]
async fn a_candidate_whose_row_vanished_is_retired_without_error() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .put_warm(&tier, "vanished.bin", b"gone already")
        .await;
    // The state a crashed delete leaves: the row and file are gone while the
    // warm candidate and its warm placement survive.
    fixture
        .fs
        .delete_object(&fixture.bucket, &info.key)
        .await
        .expect("the row is deleted under the surviving candidate");
    let placement = fixture
        .index
        .placement(&fixture.bucket, &info.key)
        .expect("the placement is read")
        .expect("the write's warm placement survives the row's deletion");

    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");
    let summary = mover.pass().await.expect("the pass runs");

    assert_eq!(summary.failed, 0, "a vanished row is not an error");
    assert_eq!(summary.demoted, 0);
    assert_eq!(summary.skipped, 1, "the stale candidate is a counted skip");
    assert!(
        fixture
            .index
            .scan_warm(10)
            .expect("the scan runs")
            .is_empty(),
        "the stale candidate is retired"
    );
    assert_eq!(
        fixture
            .index
            .placement(&fixture.bucket, &info.key)
            .expect("the placement is read"),
        Some(placement),
        "a retired candidate leaves its warm placement alone"
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
}

/// With `move_concurrency = 1` two candidates move one after the other: the
/// second copy only starts once the first has finished, so the plane never
/// holds two copies in flight. This pins the mover's serial behaviour for a
/// concurrency of one; the configured default is the CLI config's test.
#[tokio::test]
async fn a_serial_mover_moves_one_candidate_at_a_time() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let first = fixture.put_warm(&tier, "first.bin", b"first").await;
    let second = fixture.put_warm(&tier, "second.bin", b"second").await;
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    fixture.retime(&first.key, base).await;
    fixture
        .retime(&second.key, base + Duration::from_secs(60))
        .await;

    let release = fixture.cold.publish_gate.arm();
    // The probe stays above the high watermark for the second selection, so
    // both candidates are attempted by this pass.
    let mover = Mover::new(tier.clone(), Arc::new(FakeCapacity::new([95, 95, 70])), 1);
    mover.bind().await.expect("the warm table is rebuilt");

    let running = tokio::spawn(async move { mover.pass().await });
    wait_entered(&fixture.cold.publish_gate, 1).await;
    for _ in 0..64 {
        yield_now().await;
    }
    assert_eq!(
        fixture.cold.publish_gate.entered(),
        1,
        "a serial pass keeps exactly one copy in flight"
    );

    release.add_permits(1);
    for _ in 0..100 {
        if fixture.cold.publish_gate.entered() >= 2 {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        fixture.cold.publish_gate.entered(),
        2,
        "the second copy starts only after the first finished"
    );
    release.add_permits(1);

    let summary = running
        .await
        .expect("the pass task joins")
        .expect("the pass runs");
    assert_eq!(summary.demoted, 2, "both candidates move, one at a time");
    assert_eq!(summary.failed, 0);
    for info in [&first, &second] {
        assert_eq!(
            tier.storage_class(&fixture.bucket, &info.key)
                .await
                .expect("the class reads"),
            Class::StandardIa
        );
        assert!(fixture.cold.contains(&fixture.bucket, &info.key));
    }
}

/// One object's failed collection never aborts the pass: the failure is
/// counted, the object keeps its tombstone and its cold copy for a later
/// pass, and the other collectable objects still settle.
#[tokio::test]
async fn gc_survives_one_objects_failure() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let mut placed = Vec::new();
    for name in ["failed.bin", "settled.bin"] {
        let info = fixture.put_warm(&tier, name, b"tombstone").await;
        let moved = tier
            .demote(&fixture.bucket, &info.key, SystemTime::now())
            .await
            .expect("the demote runs");
        assert!(
            matches!(moved, MoveOutcome::Moved { .. }),
            "the fixture object is cold"
        );
        // The row and the leftover file go; the tombstone is what a delete's
        // crash leaves behind.
        fixture
            .fs
            .delete_object(&fixture.bucket, &info.key)
            .await
            .expect("the row is deleted under the tombstone");
        fixture.place(
            &fixture.bucket,
            &info.key,
            Placement {
                tier: Tier::Pg,
                state: State::Deleting,
                cold: Some(ColdInfo::from_info(&info)),
                mtime: SystemTime::now() - Duration::from_secs(3600),
            },
        );
        placed.push(info);
    }
    fixture.cold.fail_remove(&fixture.bucket, &placed[0].key);

    let summary = tier
        .gc(SystemTime::now())
        .await
        .expect("the pass survives one object's failure");

    assert_eq!(summary.failed, 1, "the failed removal is counted");
    assert_eq!(summary.tombstones, 1, "the other tombstone still settles");
    assert_eq!(
        fixture
            .index
            .placement(&fixture.bucket, &placed[0].key)
            .expect("the placement is read")
            .map(|placement| placement.state),
        Some(State::Deleting),
        "the failed object keeps its tombstone"
    );
    assert!(
        fixture.cold.contains(&fixture.bucket, &placed[0].key),
        "the failed object keeps its cold copy"
    );
    assert_eq!(
        fixture
            .index
            .placement(&fixture.bucket, &placed[1].key)
            .expect("the placement is read"),
        None,
        "the other tombstone is cleared"
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &placed[1].key));
}
