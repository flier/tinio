//! Construction: the marker, the index, the non-tier refusal seam and the
//! read-only run (T17, T27).
//!
//! Every leg asserts real filesystem state. The marker must exist — complete
//! and owner-only — before the index is touched; a root carrying either file
//! must be refused by a non-tier start; and a read-only construction writes
//! nothing while it serves a cold object from its placement's [`ColdInfo`].

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
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_core::{
    AccessMode, Class,
    acl::Acl,
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    object,
    storage::{self, BodyStream, BucketOps, ByteRange, ListObjectsParams, ObjectOps, Storage},
};
use tinio_fs::{FsOptions, testing};
use tinio_tier::{
    Config, Error, FlushMode, Index, Placement, State, Tier, TierStorage, index_path, is_tier_root,
    marker_path, refuse_non_tier_root,
};
use tinio_util::testing::{body, read_body};

/// A key-value byte plane standing in for PostgreSQL's large objects.
///
/// Every mutating method counts itself, so a read-only run can be proved to
/// have written nothing to the cold plane.
#[derive(Clone, Default)]
struct FakeCold {
    inner: Arc<Mutex<FakeInner>>,
}

/// The state every clone of a [`FakeCold`] shares.
#[derive(Default)]
struct FakeInner {
    objects: HashMap<(String, String), Vec<u8>>,
    opens: usize,
    mutations: usize,
}

impl FakeCold {
    fn opens(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .opens
    }

    fn mutations(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .mutations
    }

    fn count_mutation(&self) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .mutations += 1;
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
        let Some(object_key) = key.object() else {
            return Err(storage::invalid_part(0));
        };
        self.count_mutation();
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
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.opens += 1;
        let Some(data) = inner
            .objects
            .get(&(bucket.as_ref().to_owned(), object_key.as_ref().to_owned()))
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
        self.count_mutation();
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
        self.count_mutation();
        Ok(())
    }

    async fn copy(
        &self,
        _bucket: &bucket::Name,
        _from: &object::Key,
        _to: &object::Key,
    ) -> Result<(), Self::Error> {
        self.count_mutation();
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
        self.count_mutation();
        Ok(())
    }

    async fn remove_bucket(&self, _bucket: &bucket::Name) -> Result<(), Self::Error> {
        self.count_mutation();
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

/// The runtime configuration every leg uses: small limits, synchronous writes.
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
        gc_grace: Duration::from_secs(300),
    }
}

/// The read-only run's filesystem options.
fn read_only_options() -> FsOptions {
    FsOptions {
        access: AccessMode::ReadOnly,
        ..testing::fs_options()
    }
}

/// One key's warm placement at `mtime`.
fn fs_placement(mtime: SystemTime) -> Placement {
    Placement {
        tier: Tier::Fs,
        state: State::Live,
        cold: None,
        mtime,
    }
}

/// The whole tree under `root`: a directory is `None`, a file its bytes.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("the directory is readable") {
            let path = entry.expect("a directory entry").path();
            let meta = fs::symlink_metadata(&path).expect("the entry is readable");
            if meta.is_dir() {
                out.insert(path.clone(), None);
                stack.push(path);
            } else {
                out.insert(
                    path.clone(),
                    Some(fs::read(&path).expect("the file is readable")),
                );
            }
        }
    }
    out
}

/// The marker is the root's first record and precedes the index: an index
/// present but unreadable refuses before any marker is written, while a
/// construction that reaches the writable index open has already written the
/// marker, and an index opened over a marker adopts the recorded identity.
#[test]
fn the_marker_precedes_the_index() {
    // A root whose index is present but unreadable: the identity cannot be
    // known, so the construction refuses before writing a marker.
    let blocked = tempfile::tempdir().expect("a temporary root");
    let blocked_index = index_path(blocked.path());
    fs::create_dir_all(&blocked_index).expect("the index path is blocked");
    let refused = TierStorage::open(
        blocked.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .err()
    .expect("an unreadable index refuses the construction");
    assert!(!refused.is_read_only(), "{refused}");
    assert!(
        !marker_path(blocked.path()).exists(),
        "no marker records a guessed identity"
    );
    assert!(
        !marker_path(blocked.path())
            .with_file_name("tier.mode.tmp")
            .exists(),
        "no marker temp is left"
    );
    assert!(
        fs::metadata(&blocked_index)
            .expect("the index path exists")
            .is_dir(),
        "the blocked path is untouched"
    );

    // A fresh root: the construction creates the marker, and the index it then
    // opens records the same identity.
    let fresh = tempfile::tempdir().expect("a temporary root");
    let tier = TierStorage::open(
        fresh.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .expect("the fresh root opens");
    let marker = marker_path(fresh.path());
    let recorded = fs::read(&marker).expect("the marker was created");
    let identity: [u8; 16] = recorded
        .as_slice()
        .try_into()
        .expect("the marker records the 16-byte root identity");
    assert_eq!(
        tier.index().root_id().expect("the root id is read"),
        Some(identity),
        "the index records the marker's identity"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = fs::metadata(&marker)
            .expect("the marker exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the marker is owner-only");
    }
    drop(tier);

    // A marker whose index is missing: the construction reuses the recorded
    // identity and does not rewrite the marker.
    let root = tempfile::tempdir().expect("a temporary root");
    let identity = [0x5a; 16];
    fs::create_dir_all(root.path().join(".tinio")).expect("the state dir is created");
    fs::write(marker_path(root.path()), identity).expect("the marker is written");
    let written_at = fs::metadata(marker_path(root.path()))
        .expect("the marker stats")
        .modified()
        .expect("the marker records a modification time");
    let tier = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .expect("the root opens over the recorded marker");
    assert_eq!(
        tier.index().root_id().expect("the root id is read"),
        Some(identity),
        "the index adopts the marker's recorded identity"
    );
    assert_eq!(
        fs::read(marker_path(root.path())).expect("the marker survives"),
        identity.to_vec(),
        "the recorded identity is not minted again"
    );
    assert_eq!(
        fs::metadata(marker_path(root.path()))
            .expect("the marker stats")
            .modified()
            .expect("the marker records a modification time"),
        written_at,
        "the construction never rewrites the marker"
    );
    assert!(
        is_tier_root(root.path()).expect("the seam answers"),
        "a marker alone makes the root a tier root"
    );
}

/// The writable open is what fails after the marker write: a valid but
/// read-only index passes the identity probe, the marker is recorded, and the
/// writable open then refuses. A read-only file is not read-only to root, so
/// the leg is Unix-only; Windows has no read-only file mode a redb open
/// observes.
#[cfg(unix)]
#[test]
fn a_failed_writable_open_leaves_the_marker_behind() {
    let root = tempfile::tempdir().expect("a temporary root");
    let index = index_path(root.path());
    drop(Index::open(&index).expect("a valid empty index is planted"));
    let mut permissions = fs::metadata(&index).expect("the index stats").permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&index, permissions).expect("the index is made read-only");

    let refused = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .err()
    .expect("a read-only index refuses the writable open");
    assert!(!refused.is_read_only(), "{refused}");

    let marker = marker_path(root.path());
    let recorded = fs::read(&marker).expect("the marker was created before the index open");
    assert_eq!(
        recorded.len(),
        16,
        "the marker records a complete root identity"
    );

    // The refused construction wrote no identity into the index it could not
    // open: the planted index still records none.
    let index = Index::open_read_only(index_path(root.path())).expect("the index is readable");
    assert_eq!(
        index.root_id().expect("the root id is read"),
        None,
        "the refused writable open wrote no identity"
    );
}

/// See the Unix leg: the read-only index file is the only failure a writable
/// open reaches after the marker write, and Windows has no equivalent.
#[cfg(windows)]
#[test]
fn a_failed_writable_open_leaves_the_marker_behind() {
    eprintln!(
        "skipping the writable-open ordering leg: Windows has no read-only file mode a redb open \
         observes"
    );
}

/// A root carrying the marker, the index or both is a tier root: a non-tier
/// start must be refused, and only a root with neither may start as fs or pg.
#[test]
fn a_marker_or_index_present_refuses_a_non_tier_start() {
    let root = tempfile::tempdir().expect("a temporary root");
    assert!(
        !is_tier_root(root.path()).expect("the seam answers"),
        "a plain root is no tier root"
    );
    refuse_non_tier_root(root.path()).expect("a plain root may start as fs or pg");

    let tier = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .expect("the tier root opens");
    drop(tier);

    // Both files: the marker and the index.
    assert!(is_tier_root(root.path()).expect("the seam answers"));
    let refused = refuse_non_tier_root(root.path()).expect_err("the start is refused");
    assert!(
        matches!(
            refused,
            Error::TierRoot {
                marker: true,
                index: true
            }
        ),
        "{refused}"
    );

    // The marker alone: a construction interrupted before its index existed.
    fs::remove_file(index_path(root.path())).expect("the index is removed");
    assert!(is_tier_root(root.path()).expect("the seam answers"));
    let refused = refuse_non_tier_root(root.path()).expect_err("the start is refused");
    assert!(
        matches!(
            refused,
            Error::TierRoot {
                marker: true,
                index: false
            }
        ),
        "{refused}"
    );

    // The index alone: a root that predates the marker.
    fs::remove_file(marker_path(root.path())).expect("the marker is removed");
    drop(Index::open(index_path(root.path())).expect("the index is recreated"));
    assert!(is_tier_root(root.path()).expect("the seam answers"));
    let refused = refuse_non_tier_root(root.path()).expect_err("the start is refused");
    assert!(
        matches!(
            refused,
            Error::TierRoot {
                marker: false,
                index: true
            }
        ),
        "{refused}"
    );
}

/// A read-only run opens the existing index read-only, serves a cold object
/// from its placement's [`tinio_tier::ColdInfo`], and writes nothing — neither
/// into the root, nor through the index, nor on the cold plane.
#[tokio::test]
async fn read_only_writes_nothing_and_serves_cold() {
    let root = tempfile::tempdir().expect("a temporary root");
    let cold = FakeCold::default();
    let name = bucket::name("data").expect("a valid bucket name");
    let key = object::key("cold.bin").expect("a valid object key");
    {
        let tier = TierStorage::open(root.path(), testing::fs_options(), config(), cold.clone())
            .expect("the tier root opens");
        tier.create_bucket(&name, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        tier.put_object(&name, &key, body(b"cold bytes"))
            .await
            .expect("the object is written");
        tier.transition(&name, &key, Class::StandardIa, SystemTime::now())
            .await
            .expect("the object is demoted");
        // The settled cold shape (T4): a row and a cold copy, no warm file.
        fs::remove_file(root.path().join("data").join("cold.bin"))
            .expect("the warm file is removed");
    }

    let mutations = cold.mutations();
    let before = snapshot(root.path());

    let read_only = TierStorage::open(root.path(), read_only_options(), config(), cold.clone())
        .expect("the read-only run opens");

    let got = read_only
        .get_object(&name, &key, None)
        .await
        .expect("the cold object is served");
    assert_eq!(
        read_body(got.body).await.expect("the body reads"),
        b"cold bytes"
    );
    assert_eq!(cold.opens(), 1, "the GET read the cold copy once");

    let info = read_only
        .head_object(&name, &key)
        .await
        .expect("the cold metadata is served");
    assert_eq!(info.size, 10);
    assert_eq!(
        cold.opens(),
        1,
        "HEAD is answered from ColdInfo without opening the cold copy"
    );

    let listing = read_only
        .list_objects(ListObjectsParams {
            bucket: name.clone(),
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_keys: 100,
        })
        .await
        .expect("the listing runs");
    assert!(
        listing.objects.iter().any(|entry| entry.key == key),
        "the cold object is listed"
    );

    let refused = read_only
        .put_object_tags(
            &name,
            &key,
            &object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())]).expect("valid tags"),
        )
        .await;
    assert!(
        matches!(refused, Err(Error::ReadOnly)),
        "a cold metadata write is refused by the read-only index: {refused:?}"
    );

    drop(read_only);
    assert_eq!(
        snapshot(root.path()),
        before,
        "the read-only run wrote nothing into the root"
    );
    assert_eq!(
        cold.mutations(),
        mutations,
        "the read-only run wrote nothing on the cold plane"
    );
}

/// A read-only run cannot create the index: a root with the marker but no
/// index — the crash window between the two — refuses, and nothing is created.
#[test]
fn read_only_without_an_index_refuses() {
    let root = tempfile::tempdir().expect("a temporary root");
    fs::create_dir_all(root.path().join(".tinio")).expect("the state dir is created");
    let identity = [0xab; 16];
    fs::write(marker_path(root.path()), identity).expect("the marker is written");

    let refused = TierStorage::open(
        root.path(),
        read_only_options(),
        config(),
        FakeCold::default(),
    )
    .err()
    .expect("a read-only run needs the index");
    assert!(matches!(refused, Error::IndexMissing { .. }), "{refused}");
    assert!(
        !index_path(root.path()).exists(),
        "the read-only run created no index"
    );
    assert_eq!(
        fs::read(marker_path(root.path())).expect("the marker survives"),
        identity.to_vec()
    );

    // A root with neither file refuses as well, and creates no state dir.
    let empty = tempfile::tempdir().expect("a temporary root");
    let refused = TierStorage::open(
        empty.path(),
        read_only_options(),
        config(),
        FakeCold::default(),
    )
    .err()
    .expect("a read-only run needs the index");
    assert!(matches!(refused, Error::IndexMissing { .. }), "{refused}");
    assert!(
        !empty.path().join(".tinio").exists(),
        "the read-only run created no state dir"
    );
}

/// A bucket teardown clears every index row it owned — the placement a crashed
/// delete left behind, its warm candidate and its heat row — and a fresh open
/// of the same root does not resurrect them (T18).
#[tokio::test]
async fn a_bucket_teardown_drops_its_index_rows_for_a_fresh_open() {
    let root = tempfile::tempdir().expect("a temporary root");
    let name = bucket::name("data").expect("a valid bucket name");
    let key = object::key("stale.bin").expect("a valid object key");
    {
        let tier = TierStorage::open(
            root.path(),
            testing::fs_options(),
            config(),
            FakeCold::default(),
        )
        .expect("the tier root opens");
        tier.create_bucket(&name, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        tier.put_object(&name, &key, body(b"stale bytes"))
            .await
            .expect("the object is written");
        // The state a crashed delete leaves: the row and file are gone while
        // the placement, the warm candidate and the heat row survive.
        tier.fs()
            .delete_object(&name, &key)
            .await
            .expect("the row is deleted under the surviving placement");
        tier.index().record_read(&name, &key);
        tier.index().flush_heat().expect("the heat row is durable");
        assert!(
            tier.index()
                .placement(&name, &key)
                .expect("the placement is read")
                .is_some(),
            "the stale placement is planted"
        );
        assert_eq!(
            tier.index()
                .scan_warm(10)
                .expect("the warm scan runs")
                .len(),
            1,
            "the stale warm candidate is planted"
        );
        assert_eq!(
            tier.index()
                .scan_heat(10)
                .expect("the heat scan runs")
                .len(),
            1,
            "the stale heat row is planted"
        );

        tier.delete_bucket(&name)
            .await
            .expect("the empty bucket is torn down");
        assert_eq!(
            tier.index()
                .placement(&name, &key)
                .expect("the placement is read"),
            None,
            "the teardown clears the placement rows"
        );
        assert!(
            tier.index()
                .scan_warm(10)
                .expect("the warm scan runs")
                .is_empty(),
            "the teardown clears the warm candidates"
        );
        assert!(
            tier.index()
                .scan_heat(10)
                .expect("the heat scan runs")
                .is_empty(),
            "the teardown clears the heat rows"
        );
    }

    let reopened = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .expect("the root reopens");
    assert_eq!(
        reopened
            .index()
            .placement(&name, &key)
            .expect("the placement is read"),
        None,
        "a fresh open does not resurrect the placement"
    );
    assert!(
        reopened
            .index()
            .scan_warm(10)
            .expect("the warm scan runs")
            .is_empty(),
        "a fresh open does not resurrect the warm candidate"
    );
    assert!(
        reopened
            .index()
            .scan_heat(10)
            .expect("the heat scan runs")
            .is_empty(),
        "a fresh open does not resurrect the heat row"
    );
}

/// The index compacts while the construction still owns it exclusively: a
/// churned index shrinks when the layer opens it, and the surviving placement
/// is still served.
#[test]
fn compaction_runs_at_construction() {
    let root = tempfile::tempdir().expect("a temporary root");
    let path = index_path(root.path());
    let data = bucket::name("data").expect("a valid bucket name");
    let kept = object::key("kept.txt").expect("a valid object key");
    let placement = fs_placement(SystemTime::UNIX_EPOCH);
    {
        let index = Index::open(&path).expect("the index opens");
        for i in 0..2_000u32 {
            index
                .set_placement(
                    &data,
                    &object::key(format!("churn/{i}")).expect("a valid churn key"),
                    &fs_placement(SystemTime::UNIX_EPOCH),
                )
                .expect("a churn placement is written");
        }
        for i in 0..2_000u32 {
            index
                .clear(
                    &data,
                    &object::key(format!("churn/{i}")).expect("a valid churn key"),
                )
                .expect("a churn placement is cleared");
        }
        index
            .set_placement(&data, &kept, &placement)
            .expect("the kept placement is written");
    }
    let before = fs::metadata(&path).expect("the index exists").len();

    let tier = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .expect("the tier root opens");
    drop(tier);

    let after = fs::metadata(&path).expect("the index exists").len();
    assert!(
        after < before,
        "the construction compacted the index: {before} -> {after}"
    );

    let index = Index::open(&path).expect("the index reopens");
    assert_eq!(
        index
            .placement(&data, &kept)
            .expect("the placement is read"),
        Some(placement)
    );
    assert!(
        index
            .scan_placement(&data, "churn/", None, 10)
            .expect("the scan runs")
            .is_empty()
    );
    let recorded: [u8; 16] = fs::read(marker_path(root.path()))
        .expect("the marker records the root identity")
        .as_slice()
        .try_into()
        .expect("the marker records a 16-byte identity");
    assert_eq!(
        index.root_id().expect("the index identity reads"),
        Some(recorded),
        "the marker and the index record one root identity"
    );
}

/// A marker the process cannot read must refuse the construction: it is never
/// treated as absent, and the index is left untouched.
#[test]
fn a_marker_read_error_fails_closed() {
    let root = tempfile::tempdir().expect("a temporary root");
    fs::create_dir_all(marker_path(root.path())).expect("the marker path is blocked");

    let refused = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .err()
    .expect("an unreadable marker refuses the construction");
    assert!(matches!(refused, Error::Io(_)), "{refused}");
    assert!(
        !index_path(root.path()).exists(),
        "the construction refused before the index was opened"
    );

    // A marker recording something other than a root identity is refused too.
    let root = tempfile::tempdir().expect("a temporary root");
    fs::create_dir_all(root.path().join(".tinio")).expect("the state dir is created");
    fs::write(marker_path(root.path()), b"tier").expect("a corrupt marker is written");
    let refused = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(),
        FakeCold::default(),
    )
    .err()
    .expect("a corrupt marker refuses the construction");
    assert!(
        matches!(refused, Error::CorruptMarker { len: 4, .. }),
        "{refused}"
    );
    assert!(!index_path(root.path()).exists(), "the index is left alone");
}
