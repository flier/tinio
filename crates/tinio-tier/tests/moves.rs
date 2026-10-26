//! The move state machine over a real filesystem store and a fake cold plane.
//!
//! Each test builds a real [`FsStorage`] in a temporary directory and a
//! [`FakeCold`] implementing [`ObjectContent`] as the cold plane's byte store.
//! The crash states are planted directly — a `copying` placement with a
//! complete or partial cold copy, a `deleting` tombstone over an absent row, a
//! leftover warm file beside a settled cold object — so the tests assert the
//! real index, row, cold-copy and file state each pass leaves behind, not a
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
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_core::{
    Class, ETag,
    acl::Acl,
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    object,
    storage::{self, BodyStream, BucketOps, ByteRange, ObjectOps, Storage, VersionOps},
    version,
};
use tinio_fs::{FsStorage, testing};
use tinio_tier::{
    ColdInfo, Config, FlushMode, Index, MoveOutcome, Placement, State, Tier, TierStorage,
};
use tinio_util::testing::{body, metrics::exposition, read_body};
use tokio::{
    sync::Semaphore,
    time::{Instant, sleep, timeout},
};

/// A one-shot gate: the next caller of [`Gate::pass`] waits until the test
/// adds a permit.
#[derive(Clone, Default)]
struct Gate {
    armed: Arc<Mutex<Option<Arc<Semaphore>>>>,
    entered: Arc<AtomicUsize>,
}

impl Gate {
    /// Arm the gate for its next caller and answer the semaphore that releases
    /// it.
    fn arm(&self) -> Arc<Semaphore> {
        let semaphore = Arc::new(Semaphore::new(0));
        *self.armed.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&semaphore));
        semaphore
    }

    /// Wait when the gate is armed; a gate that was never armed is a no-op.
    async fn pass(&self) {
        let semaphore = self
            .armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
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

/// A key-value byte plane standing in for PostgreSQL's large objects.
#[derive(Clone, Default)]
struct FakeCold {
    inner: Arc<Mutex<FakeInner>>,
    /// A gate on the next `open`: the read's body waits while a test holds the
    /// per-key read lock.
    open_gate: Gate,
    /// A gate on the next `stage`: a move's copy pauses before it reads the
    /// source bytes, so a test can interleave a write that lands first.
    stage_gate: Gate,
    /// A gate on the next `remove`: a promotion's finalize waits here after it
    /// has published, so a test can hold the phase whose duration the move
    /// timer must cover.
    remove_gate: Gate,
}

/// The state every clone of a [`FakeCold`] shares.
#[derive(Default)]
struct FakeInner {
    objects: HashMap<(String, String), Vec<u8>>,
}

impl FakeCold {
    fn insert(&self, bucket: &bucket::Name, key: &object::Key, data: &[u8]) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .objects
            .insert(
                (bucket.as_ref().to_owned(), key.as_ref().to_owned()),
                data.to_vec(),
            );
    }

    fn contains(&self, bucket: &bucket::Name, key: &object::Key) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .objects
            .contains_key(&(bucket.as_ref().to_owned(), key.as_ref().to_owned()))
    }

    fn bytes(&self, bucket: &bucket::Name, key: &object::Key) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .objects
            .get(&(bucket.as_ref().to_owned(), key.as_ref().to_owned()))
            .cloned()
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
        self.stage_gate.pass().await;
        Ok(FakeStaged(Vec::new()))
    }

    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Self::Error> {
        let Some(object_key) = key.object() else {
            return Ok(());
        };
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .objects
            .insert(
                (bucket.as_ref().to_owned(), object_key.as_ref().to_owned()),
                staged.0,
            );
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
        // The bytes are captured before the gate opens, so a test that
        // overwrites or deletes the key meanwhile cannot change what the
        // in-flight read is holding.
        let Some(data) = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .objects
            .get(&(bucket.as_ref().to_owned(), object_key.as_ref().to_owned()))
            .cloned()
        else {
            return Err(storage::no_such_key(object_key));
        };
        self.open_gate.pass().await;
        let (start, end) = match range {
            Some(range) => range.resolve(data.len() as u64)?,
            None if data.is_empty() => return Ok(Box::pin(stream::empty())),
            None => (0, data.len() as u64 - 1),
        };
        let window = data[start as usize..=end as usize].to_vec();
        Ok(Box::pin(stream::iter([Ok(Bytes::from(window))])))
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error> {
        self.remove_gate.pass().await;
        let Some(object_key) = key.object() else {
            return Ok(());
        };
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .objects
            .remove(&(bucket.as_ref().to_owned(), object_key.as_ref().to_owned()));
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
            .objects
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
            .objects
            .retain(|(name, _), _| name != bucket.as_ref());
        Ok(())
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(!inner
            .objects
            .keys()
            .any(|(name, _)| name == bucket.as_ref()))
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

/// The grace the fixture's configuration records.
const GRACE_SECS: u64 = 300;

/// A real warm plane, a fake cold plane and the layer's own in-memory index.
struct Fixture {
    root: tempfile::TempDir,
    fs: Arc<FsStorage>,
    cold: FakeCold,
    index: Arc<Index>,
    bucket: bucket::Name,
}

/// The runtime configuration every test uses: small limits, synchronous
/// writes, and a five-minute collection grace.
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
        demote_idle: Duration::from_secs(3600),
        dwell: Duration::from_secs(3600),
        scan_interval: Duration::from_secs(60),
        move_concurrency: 1,
        index_compact_percent: 30,
        gc_grace: Duration::from_secs(GRACE_SECS),
    }
}

/// The same limits with the batch write buffer in front of the warm plane.
fn batch_config() -> Config {
    Config {
        flush_mode: FlushMode::Batch,
        ..config()
    }
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
    /// the write path records.
    async fn put_warm(&self, tier: &TierStorage<FakeCold>, key: &str, data: &[u8]) -> object::Info {
        let key = object::key(key).expect("a valid object key");
        tier.put_object(&self.bucket, &key, body(data))
            .await
            .expect("the object is written");
        tier.head_object(&self.bucket, &key)
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
            &info.key,
            Placement {
                tier: Tier::Pg,
                state: State::Live,
                cold: Some(ColdInfo::from_info(&info)),
                mtime: info.last_modified,
            },
        );
        info
    }

    fn place(&self, key: &object::Key, placement: Placement) {
        self.index
            .set_placement(&self.bucket, key, &placement)
            .expect("the placement is written");
    }

    fn placement(&self, key: &object::Key) -> Option<Placement> {
        self.index
            .placement(&self.bucket, key)
            .expect("the placement is read")
    }

    async fn row(&self, key: &object::Key) -> Option<object::Info> {
        self.fs
            .tier_rows()
            .info(&self.bucket, key)
            .await
            .expect("the row is read")
    }

    fn old(&self, now: SystemTime) -> SystemTime {
        now - Duration::from_secs(GRACE_SECS + 1)
    }
}

fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

async fn get_bytes(tier: &TierStorage<FakeCold>, fixture: &Fixture, name: &str) -> Vec<u8> {
    let key = key(name);
    let got = tier
        .get_object(&fixture.bucket, &key, None)
        .await
        .expect("the object is served");
    read_body(got.body).await.expect("the body reads")
}

/// Wait until the gate has admitted `count` callers.
async fn wait_entered(gate: &Gate, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if gate.entered() >= count {
            return;
        }
        sleep(Duration::from_millis(1)).await;
    }
    panic!("the gate was never entered");
}

#[tokio::test]
async fn demote_revalidates_and_finishes() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture.put_warm(&tier, "archive.bin", b"demote me").await;
    let now = SystemTime::now();

    let summary = tier
        .demote(&fixture.bucket, &info.key, now)
        .await
        .expect("the demote runs");

    assert!(
        matches!(summary, MoveOutcome::Moved { .. }),
        "the object moved to the cold plane"
    );
    let placement = fixture
        .placement(&info.key)
        .expect("the demote records a placement");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert_eq!(
        placement.mtime, now,
        "the settled pg placement records the move's time"
    );
    let cold = placement.cold.expect("the cold authority is recorded");
    assert_eq!(cold.etag, info.etag.to_string());
    assert_eq!(cold.last_modified, info.last_modified);
    assert_eq!(
        fixture.row(&info.key).await.map(|row| row.etag),
        Some(info.etag.clone()),
        "the row survives the demote"
    );
    assert!(
        fixture.path("archive.bin").exists(),
        "the warm file is left for garbage collection"
    );
    assert!(fixture.cold.contains(&fixture.bucket, &info.key));
    assert_eq!(
        get_bytes(&tier, &fixture, "archive.bin").await,
        b"demote me"
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &info.key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
}

#[tokio::test]
async fn a_concurrent_overwrite_aborts_the_demote() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .put_warm(&tier, "racing.bin", b"the old bytes")
        .await;
    let release = fixture.cold.stage_gate.arm();

    let moving = {
        let tier = tier.clone();
        let bucket = fixture.bucket.clone();
        let key = info.key.clone();
        tokio::spawn(async move { tier.demote(&bucket, &key, SystemTime::now()).await })
    };
    wait_entered(&fixture.cold.stage_gate, 1).await;

    tier.put_object(&fixture.bucket, &info.key, body(b"the new bytes"))
        .await
        .expect("the overwrite commits");
    let overwritten = tier
        .head_object(&fixture.bucket, &info.key)
        .await
        .expect("the overwrite is readable");
    release.add_permits(1);

    let summary = moving
        .await
        .expect("the move task joins")
        .expect("the demote answers");
    assert_eq!(
        summary,
        MoveOutcome::Aborted,
        "the revalidation abandoned the copy"
    );
    let placement = fixture
        .placement(&info.key)
        .expect("the overwrite's placement survives");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert!(
        !fixture.cold.contains(&fixture.bucket, &info.key),
        "the discarded copy is removed"
    );
    let row = fixture.row(&info.key).await.expect("the overwrite's row");
    assert_eq!(row.etag, overwritten.etag, "the row is the overwrite's");
    assert_eq!(
        fs::read(fixture.path("racing.bin")).expect("the warm file exists"),
        b"the new bytes"
    );
    assert_eq!(
        get_bytes(&tier, &fixture, "racing.bin").await,
        b"the new bytes"
    );
}

#[tokio::test]
async fn a_losing_demote_never_overwrites_the_winners_cold_copy() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let first = fixture
        .put_warm(&tier, "two-copies.bin", b"version one")
        .await;
    let release = fixture.cold.stage_gate.arm();

    let loser = {
        let tier = tier.clone();
        let bucket = fixture.bucket.clone();
        let key = first.key.clone();
        tokio::spawn(async move { tier.demote(&bucket, &key, now).await })
    };
    wait_entered(&fixture.cold.stage_gate, 1).await;
    assert_eq!(
        fixture.cold.bytes(&fixture.bucket, &first.key),
        None,
        "the paused demote published nothing"
    );

    // The winner's write and demote complete while the loser is paused before
    // it reads the warm bytes.
    tier.put_object(&fixture.bucket, &first.key, body(b"version two"))
        .await
        .expect("the overwrite commits");
    let second = tier
        .head_object(&fixture.bucket, &first.key)
        .await
        .expect("the overwrite is readable");
    let winner = tier
        .demote(&fixture.bucket, &first.key, now)
        .await
        .expect("the winner demotes");
    assert!(
        matches!(winner, MoveOutcome::Moved { .. }),
        "the second demote is the winner"
    );
    release.add_permits(1);

    let summary = loser
        .await
        .expect("the move task joins")
        .expect("the loser answers");
    assert_eq!(
        summary,
        MoveOutcome::Aborted,
        "the loser revalidated before publishing"
    );
    assert_eq!(
        fixture.cold.bytes(&fixture.bucket, &first.key).as_deref(),
        Some(&b"version two"[..]),
        "the loser never overwrites the winner's cold bytes"
    );
    let placement = fixture
        .placement(&first.key)
        .expect("the winner's placement survives");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert_eq!(
        placement.cold.expect("the cold authority").etag,
        second.etag.to_string()
    );
    assert_eq!(
        get_bytes(&tier, &fixture, "two-copies.bin").await,
        b"version two"
    );
}

#[tokio::test]
async fn promote_preserves_composed_etag_and_last_modified() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let recorded = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let key = key("composed.bin");
    let info = object::Info {
        key: key.clone(),
        size: 13,
        last_modified: recorded,
        etag: "d41d8cd98f00b204e9800998ecf8427e-2"
            .parse::<ETag>()
            .expect("a composed ETag"),
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
        version_id: None,
        bucket_records_versions: false,
    };
    fixture
        .fs
        .tier_rows()
        .set_row(&fixture.bucket, &info, recorded)
        .await
        .expect("the cold row is planted");
    fixture.cold.insert(&fixture.bucket, &key, b"cold composed");
    fixture.place(
        &key,
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: recorded,
        },
    );

    let summary = tier
        .promote(&fixture.bucket, &key)
        .await
        .expect("the promote runs");

    assert!(
        matches!(summary, MoveOutcome::Moved { .. }),
        "the object returned to the warm plane"
    );
    let placement = fixture
        .placement(&key)
        .expect("the promote records a warm placement");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert_eq!(placement.mtime, recorded);
    let row = fixture.row(&key).await.expect("the row is restored");
    assert_eq!(row.etag, info.etag, "the composed ETag survives");
    assert_eq!(row.last_modified, recorded);
    assert_eq!(
        fs::read(fixture.path("composed.bin")).expect("the warm file exists"),
        b"cold composed"
    );
    let file = fs::metadata(fixture.path("composed.bin")).expect("the file stats");
    assert_eq!(
        file.modified().expect("the file mtime reads"),
        recorded,
        "the promoted file carries the cold authority's modification time"
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &key));
    assert_eq!(
        get_bytes(&tier, &fixture, "composed.bin").await,
        b"cold composed"
    );
}

#[tokio::test]
async fn a_promote_does_not_clobber_a_completed_warm_overwrite() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .make_cold(&tier, "cold-race.bin", b"the old cold bytes")
        .await;
    // The crash between `commit_object` and `reset_to_fs`: the row and the
    // file hold the newer bytes, while the placement still says `pg/live`.
    fixture
        .fs
        .put_object(&fixture.bucket, &info.key, body(b"the newer warm bytes"))
        .await
        .expect("the overwrite commits");
    let overwritten = tier
        .head_object(&fixture.bucket, &info.key)
        .await
        .expect("the overwrite is readable");

    let summary = tier
        .promote(&fixture.bucket, &info.key)
        .await
        .expect("the promote answers");

    assert_eq!(
        summary,
        MoveOutcome::Aborted,
        "the T5 fs-won revalidation wins"
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
    let placement = fixture
        .placement(&info.key)
        .expect("the placement resets to the warm plane");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert_eq!(
        fs::read(fixture.path("cold-race.bin")).expect("the warm file survives"),
        b"the newer warm bytes",
        "the stale cold bytes never overwrite the warm file"
    );
    let row = fixture.row(&info.key).await.expect("the warm row survives");
    assert_eq!(
        row.etag, overwritten.etag,
        "the stale row is never restored"
    );
    assert_eq!(
        get_bytes(&tier, &fixture, "cold-race.bin").await,
        b"the newer warm bytes"
    );
}

#[tokio::test]
async fn a_deleted_key_is_not_resurrected() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture.make_cold(&tier, "deleted.bin", b"gone soon").await;
    // A delete half that removed the row but left the `pg/live` placement —
    // the crash state a failed delete leaves for collection.
    fixture
        .fs
        .delete_object(&fixture.bucket, &info.key)
        .await
        .expect("the row delete simulates the crash");

    let summary = tier
        .promote(&fixture.bucket, &info.key)
        .await
        .expect("the promote answers");

    assert_eq!(
        summary,
        MoveOutcome::Aborted,
        "the revalidation found the row gone"
    );
    assert!(!fixture.path("deleted.bin").exists());
    assert!(fixture.row(&info.key).await.is_none());
    assert!(fixture.placement(&info.key).is_none());
    assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
    let missing = tier.head_object(&fixture.bucket, &info.key).await;
    assert!(
        matches!(
            missing.map_err(storage::Error::from),
            Err(storage::Error::NoSuchKey(_))
        ),
        "the object stays deleted"
    );
}

#[tokio::test]
async fn gc_finishes_a_tombstone_after_the_grace() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let old = fixture.old(now);
    let crashed = fixture.make_cold(&tier, "crashed.bin", b"delete me").await;
    let fresh = fixture
        .make_cold(&tier, "fresh.bin", b"still deleting")
        .await;
    for info in [&crashed, &fresh] {
        fixture
            .fs
            .delete_object(&fixture.bucket, &info.key)
            .await
            .expect("the row delete simulates the crash");
    }
    fixture.place(
        &crashed.key,
        Placement {
            tier: Tier::Pg,
            state: State::Deleting,
            cold: Some(ColdInfo::from_info(&crashed)),
            mtime: old,
        },
    );
    fixture.place(
        &fresh.key,
        Placement {
            tier: Tier::Pg,
            state: State::Deleting,
            cold: Some(ColdInfo::from_info(&fresh)),
            mtime: now,
        },
    );

    let summary = tier.gc(now).await.expect("the collection runs");

    assert_eq!(summary.tombstones, 1, "only the aged tombstone is finished");
    assert!(fixture.placement(&crashed.key).is_none());
    assert!(
        !fixture.cold.contains(&fixture.bucket, &crashed.key),
        "the finished tombstone's cold copy is removed"
    );
    let remaining = fixture
        .placement(&fresh.key)
        .expect("the fresh tombstone stays");
    assert_eq!(remaining.state, State::Deleting);
    assert!(fixture.cold.contains(&fixture.bucket, &fresh.key));
}

#[tokio::test]
async fn gc_finishes_a_complete_copying_placement_after_the_grace() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let info = fixture
        .put_warm(&tier, "copying.bin", b"half demoted")
        .await;
    fixture
        .cold
        .insert(&fixture.bucket, &info.key, b"half demoted");
    fixture.place(
        &info.key,
        Placement {
            tier: Tier::Fs,
            state: State::Copying,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.old(now),
        },
    );

    let summary = tier.gc(now).await.expect("the collection runs");

    assert_eq!(
        summary.completed, 1,
        "the complete copy finishes the demote"
    );
    let placement = fixture.placement(&info.key).expect("the demote is settled");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.mtime, now);
    assert!(fixture.row(&info.key).await.is_some(), "the row is kept");
    assert!(fixture.cold.contains(&fixture.bucket, &info.key));
    assert!(
        fixture.path("copying.bin").exists(),
        "the file waits for the file pass"
    );

    let later = now + Duration::from_secs(GRACE_SECS + 1);
    let summary = tier.gc(later).await.expect("the second pass runs");
    assert_eq!(summary.files, 1, "the leftover warm file is removed");
    assert!(!fixture.path("copying.bin").exists());
    assert!(fixture.row(&info.key).await.is_some());
    assert_eq!(
        get_bytes(&tier, &fixture, "copying.bin").await,
        b"half demoted"
    );
}

#[tokio::test]
async fn gc_reverts_a_partial_copying_placement() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let info = fixture
        .put_warm(&tier, "partial.bin", b"the warm bytes")
        .await;
    fixture.place(
        &info.key,
        Placement {
            tier: Tier::Fs,
            state: State::Copying,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.old(now),
        },
    );

    let summary = tier.gc(now).await.expect("the collection runs");

    assert_eq!(summary.reverted, 1, "the partial copy reverts to warm");
    let placement = fixture.placement(&info.key).expect("the placement reverts");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert_eq!(placement.mtime, info.last_modified);
    assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
    assert_eq!(
        fs::read(fixture.path("partial.bin")).expect("the warm file survives"),
        b"the warm bytes"
    );
    assert_eq!(
        get_bytes(&tier, &fixture, "partial.bin").await,
        b"the warm bytes"
    );
}

#[tokio::test]
async fn verify_reports_an_unresolved_copying() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let info = fixture
        .put_warm(&tier, "unresolved.bin", b"no snapshot")
        .await;
    fixture.place(
        &info.key,
        Placement {
            tier: Tier::Fs,
            state: State::Copying,
            cold: None,
            mtime: fixture.old(now),
        },
    );
    let waiting = fixture
        .put_warm(&tier, "waiting.bin", b"inside the grace")
        .await;
    fixture.place(
        &waiting.key,
        Placement {
            tier: Tier::Fs,
            state: State::Copying,
            cold: None,
            mtime: now,
        },
    );

    let report = tier.verify(now).await.expect("the verification runs");
    assert!(!report.is_clean());
    assert_eq!(
        report.unresolved_copying,
        vec![(fixture.bucket.clone(), info.key.clone())],
        "only the aged, unreadable copy is reported"
    );

    let summary = tier.gc(now).await.expect("the collection runs");
    assert_eq!(summary.unresolved, 1, "collection cannot decide the copy");
    let placement = fixture
        .placement(&info.key)
        .expect("the unresolved placement is left alone");
    assert_eq!(placement.state, State::Copying);
    assert_eq!(placement.cold, None);
}

#[tokio::test]
async fn gc_removes_a_pg_placed_file_with_no_readers() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let info = fixture
        .make_cold(&tier, "leftover.bin", b"cold bytes")
        .await;
    fixture.place(
        &info.key,
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.old(now),
        },
    );
    fs::write(fixture.path("leftover.bin"), b"cold bytes").expect("the leftover file is planted");

    let summary = tier.gc(now).await.expect("the collection runs");

    assert_eq!(summary.files, 1, "the leftover warm file is removed");
    assert!(!fixture.path("leftover.bin").exists());
    assert!(fixture.row(&info.key).await.is_some(), "the row is kept");
    assert!(fixture.cold.contains(&fixture.bucket, &info.key));
    assert_eq!(
        get_bytes(&tier, &fixture, "leftover.bin").await,
        b"cold bytes"
    );
}

#[tokio::test]
async fn gc_reconciles_a_warm_overwrite_of_a_cold_object() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let info = fixture
        .make_cold(&tier, "rewritten.bin", b"the old cold bytes")
        .await;
    fixture.place(
        &info.key,
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.old(now),
        },
    );
    // An out-of-band warm write lands beside the cold placement; the tier's
    // own write path is what normally clears the placement (T5, Task 7).
    fixture
        .fs
        .put_object(&fixture.bucket, &info.key, body(b"the newer warm copy"))
        .await
        .expect("the out-of-band write commits");

    let summary = tier.gc(now).await.expect("the collection runs");

    assert_eq!(summary.reverted, 1, "the warm plane won the overwrite");
    let placement = fixture
        .placement(&info.key)
        .expect("the placement is reconciled");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.cold, None);
    assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
    assert_eq!(
        get_bytes(&tier, &fixture, "rewritten.bin").await,
        b"the newer warm copy"
    );
}

#[tokio::test]
async fn gc_never_auto_deletes_unplaced_pg() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let key = key("orphan.bin");
    fixture.cold.insert(&fixture.bucket, &key, b"an orphan");

    let summary = tier.gc(now).await.expect("the collection runs");

    assert_eq!(
        summary,
        Default::default(),
        "nothing is placed or collected"
    );
    assert!(
        fixture.cold.contains(&fixture.bucket, &key),
        "unplaced cold content is never deleted by the automatic pass"
    );
    assert!(fixture.row(&key).await.is_none());
    assert!(fixture.placement(&key).is_none());
}

#[tokio::test]
async fn an_in_flight_read_does_not_see_a_torn_file() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let now = SystemTime::now();
    let info = fixture
        .make_cold(&tier, "reading.bin", b"read me whole")
        .await;
    fixture.place(
        &info.key,
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.old(now),
        },
    );
    fs::write(fixture.path("reading.bin"), b"read me whole").expect("the leftover file is planted");
    let release = fixture.cold.open_gate.arm();

    let reading = {
        let tier = tier.clone();
        let bucket = fixture.bucket.clone();
        let key = info.key.clone();
        tokio::spawn(async move {
            let got = tier
                .get_object(&bucket, &key, None)
                .await
                .expect("the read serves");
            read_body(got.body).await.expect("the body reads")
        })
    };
    wait_entered(&fixture.cold.open_gate, 1).await;

    let summary = tier.gc(now).await.expect("the collection runs");
    assert_eq!(
        summary.busy, 1,
        "the in-flight reader's file is not removed"
    );
    assert!(
        fixture.path("reading.bin").exists(),
        "the reader's file is still there"
    );

    release.add_permits(1);
    let bytes = reading.await.expect("the read task joins");
    assert_eq!(bytes, b"read me whole", "the in-flight read is intact");

    let summary = tier.gc(now).await.expect("the second pass runs");
    assert_eq!(summary.files, 1);
    assert!(!fixture.path("reading.bin").exists());
    assert_eq!(
        get_bytes(&tier, &fixture, "reading.bin").await,
        b"read me whole"
    );
}

#[tokio::test]
async fn transition_to_standard_ia_demotes() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .put_warm(&tier, "classed.bin", b"move me down")
        .await;

    tier.transition(
        &fixture.bucket,
        &info.key,
        Class::StandardIa,
        SystemTime::now(),
    )
    .await
    .expect("the transition runs");

    let placement = fixture
        .placement(&info.key)
        .expect("the transition records a placement");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
}

#[tokio::test]
async fn transition_to_the_current_class_is_a_noop() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture.put_warm(&tier, "noop.bin", b"warm already").await;

    tier.transition(
        &fixture.bucket,
        &info.key,
        Class::StandardIa,
        SystemTime::now(),
    )
    .await
    .expect("the demote runs");
    let cold = fixture.placement(&info.key);
    tier.transition(
        &fixture.bucket,
        &info.key,
        Class::StandardIa,
        SystemTime::now(),
    )
    .await
    .expect("a cold target on a cold object is a no-op");
    assert_eq!(fixture.placement(&info.key), cold);
}

#[tokio::test]
async fn transition_to_standard_is_invalid_request() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .make_cold(&tier, "promote-refused.bin", b"cold bytes")
        .await;

    let refused = tier
        .transition(
            &fixture.bucket,
            &info.key,
            Class::Standard,
            SystemTime::now(),
        )
        .await
        .expect_err("a transition to STANDARD is refused");
    let contract: storage::Error = refused.into();
    assert!(
        matches!(contract, storage::Error::InvalidRequest(_)),
        "the refusal is the contract's InvalidRequest, got {contract:?}"
    );
    assert_eq!(
        fixture.placement(&info.key).map(|placement| placement.tier),
        Some(Tier::Pg),
        "the refused transition moved nothing"
    );
}

#[tokio::test]
async fn transition_of_a_missing_key_is_no_such_key() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let missing = key("missing.bin");

    let refused = tier
        .transition(
            &fixture.bucket,
            &missing,
            Class::StandardIa,
            SystemTime::now(),
        )
        .await
        .expect_err("a missing object cannot transition");
    let contract: storage::Error = refused.into();
    assert!(
        matches!(contract, storage::Error::NoSuchKey(_)),
        "the lifecycle treats a vanished object as done, got {contract:?}"
    );
}

#[tokio::test]
async fn transition_flushes_a_buffered_key_first() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier_with(batch_config());
    let buffered = key("buffered.bin");
    tier.put_object(&fixture.bucket, &buffered, body(b"buffered bytes"))
        .await
        .expect("the write is acknowledged");
    assert!(
        fixture.row(&buffered).await.is_none(),
        "the acknowledged write waits in the buffer"
    );

    tier.transition(
        &fixture.bucket,
        &buffered,
        Class::StandardIa,
        SystemTime::now(),
    )
    .await
    .expect("the transition flushes before it reads the row");

    let placement = fixture
        .placement(&buffered)
        .expect("the demote records a placement");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert_eq!(
        get_bytes(&tier, &fixture, "buffered.bin").await,
        b"buffered bytes"
    );
}

/// A transition is skipped on both versioning states — enabled and suspended —
/// while expiration (a delete) still runs on both; a plain unversioned
/// bucket's transition demotes (FR-046, T11).
#[tokio::test]
async fn transitions_skip_versioned_buckets_while_expiration_runs() {
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

    let versioned = key("classed.bin");
    for name in [&enabled, &suspended] {
        tier.put_object(name, &versioned, body(b"stays warm"))
            .await
            .expect("the versioned object is written");
    }
    let plain = fixture.put_warm(&tier, "classed.bin", b"demotes").await;

    for name in [&enabled, &suspended] {
        tier.transition(name, &versioned, Class::StandardIa, SystemTime::now())
            .await
            .expect("a versioned bucket's transition is a skip, not an error");
    }
    tier.transition(
        &fixture.bucket,
        &plain.key,
        Class::StandardIa,
        SystemTime::now(),
    )
    .await
    .expect("the unversioned transition runs");

    for name in [&enabled, &suspended] {
        assert_eq!(
            tier.storage_class(name, &versioned)
                .await
                .expect("the class reads"),
            Class::Standard,
            "a versioned or suspended object stays warm"
        );
        let placement = fixture
            .index
            .placement(name, &versioned)
            .expect("the placement is read")
            .expect("the write's placement survives");
        assert_eq!(placement.tier, Tier::Fs);
        assert_eq!(placement.state, State::Live);
        assert!(
            fixture
                .root
                .path()
                .join(name.as_ref())
                .join("classed.bin")
                .exists()
        );
        assert!(!fixture.cold.contains(name, &versioned));
    }
    assert_eq!(
        tier.storage_class(&fixture.bucket, &plain.key)
            .await
            .expect("the class reads"),
        Class::StandardIa,
        "a plain unversioned bucket's transition demotes"
    );
    assert!(fixture.cold.contains(&fixture.bucket, &plain.key));

    // Expiration still runs on both states: the delete hides the key.
    for name in [&enabled, &suspended] {
        let marker = tier
            .delete_object(name, &versioned)
            .await
            .expect("expiration still runs on both versioning states");
        assert!(
            marker.is_delete_marker,
            "the expiration's marker is written"
        );
        let gone = tier
            .head_object(name, &versioned)
            .await
            .expect_err("the expired object is hidden");
        let contract: storage::Error = gone.into();
        assert!(
            matches!(contract, storage::Error::NoSuchKey(_)),
            "the expired object answers NoSuchKey, got {contract:?}"
        );
        assert!(
            !fixture
                .root
                .path()
                .join(name.as_ref())
                .join("classed.bin")
                .exists(),
            "the expiration removed the warm file"
        );
    }
    assert_eq!(
        tier.storage_class(&fixture.bucket, &plain.key)
            .await
            .expect("the class reads"),
        Class::StandardIa,
        "the expiration runs left the transitioned plain object cold"
    );
}

#[tokio::test]
async fn a_busy_key_is_skipped_by_a_mover() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture.make_cold(&tier, "busy.bin", b"busy bytes").await;
    let release = fixture.cold.open_gate.arm();

    let reading = {
        let tier = tier.clone();
        let bucket = fixture.bucket.clone();
        let key = info.key.clone();
        tokio::spawn(async move {
            let got = tier
                .get_object(&bucket, &key, None)
                .await
                .expect("the read serves");
            read_body(got.body).await.expect("the body reads")
        })
    };
    wait_entered(&fixture.cold.open_gate, 1).await;

    let summary = tier
        .promote(&fixture.bucket, &info.key)
        .await
        .expect("the promote answers");
    assert_eq!(
        summary,
        MoveOutcome::Skipped,
        "a mover never waits for a busy key"
    );
    assert_eq!(
        fixture.placement(&info.key).map(|placement| placement.tier),
        Some(Tier::Pg),
        "the skipped move changed nothing"
    );

    release.add_permits(1);
    let bytes = reading.await.expect("the read task joins");
    assert_eq!(bytes, b"busy bytes");
}

#[tokio::test]
async fn an_in_flight_read_does_not_wait_for_the_promotion_copy() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .make_cold(&tier, "promoting.bin", b"copy me warm")
        .await;
    let release = fixture.cold.open_gate.arm();

    let promoting = {
        let tier = tier.clone();
        let bucket = fixture.bucket.clone();
        let key = info.key.clone();
        tokio::spawn(async move { tier.promote(&bucket, &key).await })
    };
    wait_entered(&fixture.cold.open_gate, 1).await;

    // The promotion's cold copy is held inside the fake plane. The read takes
    // the key's read side and serves from the cold plane; it must not wait for
    // the copy, which the copy's stage runs without the write lock.
    let read = timeout(
        Duration::from_secs(5),
        get_bytes(&tier, &fixture, "promoting.bin"),
    )
    .await;
    let bytes = read.expect("the read must not wait for the copy in progress");
    assert_eq!(bytes, b"copy me warm");

    release.add_permits(1);
    let summary = promoting
        .await
        .expect("the promote task joins")
        .expect("the promote answers");
    assert!(
        matches!(summary, MoveOutcome::Moved { .. }),
        "the gated copy finishes after the read"
    );
    assert_eq!(
        get_bytes(&tier, &fixture, "promoting.bin").await,
        b"copy me warm"
    );
}

/// One move-duration histogram's accumulated seconds, read from the process
/// exposition. The legs assert a lower bound on a delta, so a move another
/// test runs concurrently can only inflate it.
fn move_duration_seconds(direction: &str) -> f64 {
    let prefix = format!(r#"tinio_tier_move_duration_seconds_sum{{direction="{direction}"}}"#);
    exposition()
        .lines()
        .find_map(|line| {
            line.strip_prefix(&prefix)
                .and_then(|value| value.trim().parse::<f64>().ok())
        })
        .unwrap_or(0.0)
}

/// A promotion's booked duration covers the finalize, not only the cold copy:
/// holding the cold removal after the publish makes the recorded sample cover
/// the wait. A timer that stopped after the copy would book only the few
/// milliseconds before the finalize.
#[tokio::test]
async fn a_promotion_duration_includes_the_finalize_phase() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();
    let info = fixture
        .make_cold(&tier, "finalize.bin", b"the cold bytes")
        .await;
    let release = fixture.cold.remove_gate.arm();

    let before = move_duration_seconds("promote");
    let promoting = {
        let tier = tier.clone();
        let bucket = fixture.bucket.clone();
        let key = info.key.clone();
        tokio::spawn(async move { tier.promote(&bucket, &key).await })
    };
    wait_entered(&fixture.cold.remove_gate, 1).await;
    // The publish is done and the finalize is now waiting on the cold removal.
    let held = Instant::now();
    sleep(Duration::from_millis(400)).await;
    let held_for = held.elapsed();
    release.add_permits(1);

    let summary = promoting
        .await
        .expect("the promotion task joins")
        .expect("the promotion answers");
    assert!(
        matches!(summary, MoveOutcome::Moved { .. }),
        "the object returned to the warm plane"
    );
    let recorded = move_duration_seconds("promote") - before;
    assert!(
        recorded >= held_for.as_secs_f64() * 0.9,
        "the recorded duration {recorded}s covers the {held_for:?} finalize wait"
    );
}
