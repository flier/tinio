//! The bounded listing merge over a real filesystem store and a fake cold
//! plane.
//!
//! Each test builds a real [`FsStorage`] in a temporary directory — the warm
//! plane the layer decorates — and a [`FakeCold`] implementing
//! [`ObjectContent`] as the cold plane's byte store. A cold object is a row
//! with no file, exactly the shape demotion leaves (T4), so the tests exercise
//! the merge against real row and placement state rather than a mock of the
//! code under test.

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
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_core::{
    ListObjectsParams,
    acl::Acl,
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    object,
    storage::{
        self, BodyStream, BucketOps, ByteRange, ObjectListing, ObjectOps, Storage, VersionOps,
    },
    version,
};
use tinio_fs::{FsStorage, testing};
use tinio_tier::{ColdInfo, Config, FlushMode, Index, Placement, State, Tier, TierStorage};
use tinio_util::testing::body;

/// The ETag the cold authority records in the tests that pin its precedence.
const COLD_ETAG: &str = "11111111111111111111111111111111";

/// The fake plane's `(bucket, key)` → bytes table.
type ObjectMap = HashMap<(String, String), Vec<u8>>;

/// A key-value byte plane standing in for PostgreSQL's large objects.
#[derive(Clone, Default)]
struct FakeCold {
    objects: Arc<Mutex<ObjectMap>>,
}

impl FakeCold {
    fn insert(&self, bucket: &bucket::Name, key: &object::Key, data: &[u8]) {
        self.objects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                (bucket.as_ref().to_owned(), key.as_ref().to_owned()),
                data.to_vec(),
            );
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
        _bucket: &bucket::Name,
        _staged: Self::Staged,
        _key: &ContentKey,
    ) -> Result<(), Self::Error> {
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
        let inner = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(data) = inner.get(&(bucket.as_ref().to_owned(), object_key.as_ref().to_owned()))
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
        let Some(object_key) = key.object() else {
            return Ok(());
        };
        self.objects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
        let inner = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
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

    async fn remove_bucket(&self, _bucket: &bucket::Name) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
        let inner = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
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

/// A real warm plane, a fake cold plane and the layer's own in-memory index.
struct Fixture {
    root: tempfile::TempDir,
    fs: Arc<FsStorage>,
    cold: FakeCold,
    index: Arc<Index>,
    bucket: bucket::Name,
}

/// The runtime configuration every test uses: small limits, synchronous
/// writes, and a promotion threshold of two reads.
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
        TierStorage::from_parts(
            Arc::clone(&self.fs),
            self.cold.clone(),
            Arc::clone(&self.index),
            config(),
        )
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.path().join(self.bucket.as_ref()).join(key)
    }

    fn params(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> ListObjectsParams {
        ListObjectsParams {
            bucket: self.bucket.clone(),
            prefix: prefix.to_owned(),
            delimiter: delimiter.map(str::to_owned),
            start_after: start_after.map(str::to_owned),
            max_keys,
        }
    }

    async fn put(&self, key: &str, data: &[u8]) -> object::Info {
        let key = object::key(key).expect("a valid object key");
        self.fs
            .put_object(&self.bucket, &key, body(data))
            .await
            .expect("the object is written");
        self.fs
            .head_object(&self.bucket, &key)
            .await
            .expect("the object is readable")
    }

    /// Remove the object's file and plant its cold copy and placement — the
    /// settled shape of a demoted object (T4). The row stays behind as the
    /// mirror every demoted object keeps.
    async fn make_cold(&self, key: &str, data: &[u8]) -> object::Info {
        let info = self.put(key, data).await;
        fs::remove_file(self.path(key)).expect("the warm file is removed");
        self.cold.insert(&self.bucket, &info.key, data);
        self.place_cold(&info, ColdInfo::from_info(&info));
        info
    }

    fn place_cold(&self, info: &object::Info, cold: ColdInfo) {
        self.index
            .set_placement(
                &self.bucket,
                &info.key,
                &Placement {
                    tier: Tier::Pg,
                    state: State::Live,
                    cold: Some(cold),
                    mtime: info.last_modified,
                },
            )
            .expect("the cold placement is written");
    }
}

/// The cold authority with values a listing can tell apart from the warm
/// mirror's row.
fn distinctive_cold(info: &object::Info) -> ColdInfo {
    let mut cold = ColdInfo::from_info(info);
    cold.size = 4_242;
    cold.etag = COLD_ETAG.to_owned();
    cold.last_modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    cold
}

/// The keys of a page, in order.
fn keys(page: &ObjectListing) -> Vec<String> {
    page.objects
        .iter()
        .map(|info| info.key.as_ref().to_owned())
        .collect()
}

/// The keys of a version page, in order.
fn version_keys(page: &version::Listing) -> Vec<String> {
    page.entries
        .iter()
        .map(|entry| entry.key.as_ref().to_owned())
        .collect()
}

#[tokio::test]
async fn warm_and_cold_keys_merge_in_order() {
    let fixture = Fixture::new().await;
    fixture.put("a.txt", b"a").await;
    fixture.make_cold("b.txt", b"bb").await;
    fixture.put("c.txt", b"ccc").await;
    fixture.make_cold("d.txt", b"dddd").await;
    let tier = fixture.tier();

    let page = tier
        .list_objects(fixture.params("", None, None, 100))
        .await
        .expect("the merged listing answers");

    assert_eq!(keys(&page), ["a.txt", "b.txt", "c.txt", "d.txt"]);
    assert_eq!(
        page.objects[1].size, 2,
        "the cold key's row metadata answered"
    );
    assert!(!page.truncated, "the whole bucket fits one page");
    assert!(page.common_prefixes.is_empty());
    assert_eq!(page.next_start_after, None);
}

#[tokio::test]
async fn prefix_and_delimiter_cross_the_boundary() {
    let fixture = Fixture::new().await;
    fixture.put("dir/a.txt", b"a").await;
    fixture.make_cold("dir/b.txt", b"b").await;
    fixture.put("dir/sub/c.txt", b"c").await;
    fixture.make_cold("dir/sub/d.txt", b"d").await;
    fixture.make_cold("elsewhere/e.txt", b"e").await;
    let tier = fixture.tier();

    let page = tier
        .list_objects(fixture.params("dir/", Some("/"), None, 100))
        .await
        .expect("the grouped listing answers");

    assert_eq!(keys(&page), ["dir/a.txt", "dir/b.txt"]);
    assert_eq!(
        page.common_prefixes,
        ["dir/sub/"],
        "a group spanning both planes rolls up once"
    );
    assert!(!page.truncated);
    assert_eq!(page.next_start_after, None);
}

#[tokio::test]
async fn pagination_spans_pages() {
    let fixture = Fixture::new().await;
    fixture.put("a.txt", b"a").await;
    fixture.make_cold("b.txt", b"b").await;
    fixture.put("c.txt", b"c").await;
    fixture.make_cold("d.txt", b"d").await;
    fixture.put("e.txt", b"e").await;
    fixture.make_cold("f.txt", b"f").await;
    let tier = fixture.tier();

    let mut after: Option<String> = None;
    let mut seen: Vec<String> = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = tier
            .list_objects(fixture.params("", None, after.as_deref(), 2))
            .await
            .expect("a page answers");
        pages += 1;
        assert!(page.objects.len() <= 2, "a page never exceeds max_keys");
        seen.extend(keys(&page));
        if page.truncated {
            let next = page.next_start_after.expect("a truncated page resumes");
            assert!(next.as_str() > after.as_deref().unwrap_or(""));
            after = Some(next);
        } else {
            assert_eq!(page.next_start_after, None);
            break;
        }
        assert!(pages < 10, "the walk terminates");
    }

    assert_eq!(pages, 3, "six keys at two per page");
    assert_eq!(
        seen,
        ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt", "f.txt"],
        "no key repeats and none is skipped across the pages"
    );
}

/// A delimiter group is one page entry however many keys it absorbs, and a
/// page cut at it resumes after the group — never inside it.
#[tokio::test]
async fn a_rolled_prefix_is_never_split_across_pages() {
    let fixture = Fixture::new().await;
    fixture.put("a.txt", b"a").await;
    fixture.make_cold("dir/x", b"x").await;
    fixture.put("dir/y", b"y").await;
    fixture.make_cold("e.txt", b"e").await;
    let tier = fixture.tier();

    let first = tier
        .list_objects(fixture.params("", Some("/"), None, 1))
        .await
        .expect("the first page answers");
    assert_eq!(keys(&first), ["a.txt"]);
    assert!(first.common_prefixes.is_empty());
    assert!(first.truncated);
    assert_eq!(first.next_start_after.as_deref(), Some("a.txt"));

    let second = tier
        .list_objects(fixture.params("", Some("/"), first.next_start_after.as_deref(), 1))
        .await
        .expect("the second page answers");
    assert!(
        second.objects.is_empty(),
        "no key under the delimiter is ever emitted as an object"
    );
    assert_eq!(
        second.common_prefixes,
        ["dir/"],
        "the group is one page entry"
    );
    assert!(second.truncated, "the group's own cut is a page cut");
    assert_eq!(
        second.next_start_after.as_deref(),
        Some("dir/"),
        "a rolled-up prefix resumes after the prefix itself"
    );

    let third = tier
        .list_objects(fixture.params("", Some("/"), second.next_start_after.as_deref(), 1))
        .await
        .expect("the third page answers");
    assert_eq!(keys(&third), ["e.txt"]);
    assert!(third.common_prefixes.is_empty());
    assert!(!third.truncated);
    assert_eq!(third.next_start_after, None);

    let mut seen = keys(&first);
    seen.extend(keys(&second));
    seen.extend(keys(&third));
    assert_eq!(
        seen,
        ["a.txt", "e.txt"],
        "the group's members are never listed as keys"
    );
}

#[tokio::test]
async fn a_duplicate_key_resolves_to_cold() {
    let fixture = Fixture::new().await;
    let cold = fixture.make_cold("dup.txt", b"mirror").await;
    fixture.place_cold(&cold, distinctive_cold(&cold));
    fixture.put("warm.txt", b"warm").await;
    let tier = fixture.tier();

    let page = tier
        .list_objects(fixture.params("", None, None, 100))
        .await
        .expect("the merged listing answers");

    assert_eq!(
        keys(&page),
        ["dup.txt", "warm.txt"],
        "the duplicated key appears once"
    );
    assert_eq!(
        page.objects[0].size, 4_242,
        "the cold authority wins the duplicate"
    );
    assert_eq!(page.objects[0].etag.to_string(), COLD_ETAG);
    assert_eq!(
        page.objects[0].last_modified,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    );
}

#[tokio::test]
async fn cold_keys_appear_in_list_object_versions() {
    let fixture = Fixture::new().await;
    fixture.put("a.txt", b"a").await;
    let cold = fixture.make_cold("b.txt", b"cold").await;
    fixture.place_cold(&cold, distinctive_cold(&cold));
    let tier = fixture.tier();

    let page = tier
        .list_object_versions(
            &fixture.bucket,
            version::ListParams {
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .expect("the version listing answers");

    assert_eq!(version_keys(&page), ["a.txt", "b.txt"]);
    assert!(
        page.entries.iter().all(|entry| entry.id.is_none()),
        "an unversioned bucket lists the null version of every key"
    );
    assert_eq!(
        page.entries[1].size, 4_242,
        "the cold key is built from ColdInfo"
    );
    assert_eq!(page.entries[1].etag.to_string(), COLD_ETAG);
    assert!(!page.entries[1].is_delete_marker);
    assert!(!page.is_truncated);

    let first = tier
        .list_object_versions(
            &fixture.bucket,
            version::ListParams {
                max_keys: 1,
                ..Default::default()
            },
        )
        .await
        .expect("the first page answers");
    assert_eq!(version_keys(&first), ["a.txt"]);
    assert!(first.is_truncated);
    assert_eq!(first.next_key_marker.as_deref(), Some("a.txt"));
    assert_eq!(first.next_version_id_marker.as_deref(), Some("null"));

    let second = tier
        .list_object_versions(
            &fixture.bucket,
            version::ListParams {
                key_marker: first.next_key_marker.clone(),
                version_id_marker: first.next_version_id_marker.clone().map(version::IdMarker),
                max_keys: 1,
                ..Default::default()
            },
        )
        .await
        .expect("the second page answers");
    assert_eq!(version_keys(&second), ["b.txt"]);
    assert!(!second.is_truncated);
}

#[tokio::test]
async fn the_metrics_scan_does_not_flush() {
    let fixture = Fixture::new().await;
    fixture.put("a.txt", b"a").await;
    let cold = fixture.make_cold("b.txt", b"cold").await;
    fixture.place_cold(&cold, distinctive_cold(&cold));
    let tier = fixture.tier();

    let client = tier
        .list_objects(fixture.params("", None, None, 100))
        .await
        .expect("the client listing answers");
    // The metrics scan takes the merged, non-flushing path: it counts the
    // same merged page without the client listing's pre-flight hook. The
    // buffer it must not flush lands in Task 11; that task's own
    // `the_metrics_scan_does_not_flush` pins the buffer state.
    let metrics = tier
        .metrics_scan(fixture.params("", None, None, 100))
        .await
        .expect("the metrics listing answers");

    assert_eq!(metrics, client, "both paths cut the same merged page");
    assert_eq!(keys(&metrics), ["a.txt", "b.txt"]);
    assert_eq!(
        metrics.objects[1].size, 4_242,
        "the metrics path reads the cold authority, not just the warm forward"
    );
}

#[tokio::test]
async fn an_empty_bucket_is_empty() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier();

    let page = tier
        .list_objects(fixture.params("", None, None, 100))
        .await
        .expect("an empty listing answers");
    assert!(page.objects.is_empty());
    assert!(page.common_prefixes.is_empty());
    assert!(!page.truncated);
    assert_eq!(page.next_start_after, None);

    let versions = tier
        .list_object_versions(
            &fixture.bucket,
            version::ListParams {
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .expect("an empty version listing answers");
    assert!(versions.entries.is_empty());
    assert!(versions.common_prefixes.is_empty());
    assert!(!versions.is_truncated);

    let metrics = tier
        .metrics_scan(fixture.params("", None, None, 100))
        .await
        .expect("an empty metrics listing answers");
    assert!(metrics.objects.is_empty());
    assert!(!metrics.truncated);
}
