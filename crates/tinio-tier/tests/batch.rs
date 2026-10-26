//! The batch write buffer over a real filesystem store and a fake cold plane.
//!
//! Each test builds a real [`FsStorage`] in a temporary directory — the warm
//! plane the layer decorates — and a [`FakeCold`] implementing
//! [`ObjectContent`] as the cold plane's byte store. A plain commit in
//! [`FlushMode::Batch`] answers from the buffer before the filesystem sees the
//! bytes; a flush writes them, preserves the acknowledged Last-Modified,
//! restores the buffered row and clears the destination's placement and cold
//! copy (T8). Every element and byte operation flushes its key first, a
//! listing flushes the whole buffer while the metrics scan never does, the
//! buffer's hard cap stops acknowledging until it is flushed, a write whose
//! flush keeps failing is quarantined instead of wedging the layer, and a
//! shutdown drains what remains.

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
    Class, ETag,
    acl::Acl,
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    object,
    storage::{
        self, BodyStream, BucketOps, ByteRange, ListObjectsParams, ObjectOps, Storage, VersionOps,
    },
    version,
};
use tinio_fs::{FsStorage, testing};
use tinio_tier::{
    ColdInfo, Config, FlushMode, HeatEntry, Index, Mover, Placement, State, Tier, TierStaged,
    TierStorage, VolumeCapacity,
};
use tinio_util::testing::{body, read_body};

/// A key-value byte plane standing in for PostgreSQL's large objects.
#[derive(Clone, Default)]
struct FakeCold {
    inner: Arc<Mutex<FakeInner>>,
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
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
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

/// A real warm plane, a fake cold plane and the layer's own in-memory index.
struct Fixture {
    root: tempfile::TempDir,
    fs: Arc<FsStorage>,
    cold: FakeCold,
    index: Arc<Index>,
    bucket: bucket::Name,
}

/// The runtime configuration every test uses: batch writes, small limits,
/// under an explicit shared budget (`capacity_bytes`).
fn config(capacity_bytes: u64, batch_bytes: u64) -> Config {
    Config {
        capacity_bytes,
        max_object_bytes: 64 * 1024,
        flush_mode: FlushMode::Batch,
        batch_bytes,
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

    fn tier(&self, batch_bytes: u64) -> TierStorage<FakeCold> {
        self.tier_at(1024 * 1024, batch_bytes)
    }

    fn tier_at(&self, capacity_bytes: u64, batch_bytes: u64) -> TierStorage<FakeCold> {
        TierStorage::from_parts(
            Arc::clone(&self.fs),
            self.cold.clone(),
            Arc::clone(&self.index),
            config(capacity_bytes, batch_bytes),
        )
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.path().join(self.bucket.as_ref()).join(key)
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

    /// Write through the filesystem and plant the cold copy and placement —
    /// the settled shape of a demoted object (T4).
    async fn make_cold(&self, key: &str, data: &[u8]) -> object::Info {
        let key = key_of(key);
        self.fs
            .put_object(&self.bucket, &key, body(data))
            .await
            .expect("the object is written");
        let info = self
            .fs
            .head_object(&self.bucket, &key)
            .await
            .expect("the object is readable");
        fs::remove_file(self.path(key.as_ref())).expect("the warm file is removed");
        self.cold.insert(&self.bucket, &key, data);
        self.index
            .set_placement(
                &self.bucket,
                &key,
                &Placement {
                    tier: Tier::Pg,
                    state: State::Live,
                    cold: Some(ColdInfo::from_info(&info)),
                    mtime: info.last_modified,
                },
            )
            .expect("the cold placement is written");
        info
    }
}

fn key_of(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

fn list_params(bucket: &bucket::Name) -> ListObjectsParams {
    ListObjectsParams {
        bucket: bucket.clone(),
        prefix: String::new(),
        delimiter: None,
        start_after: None,
        max_keys: 1000,
    }
}

fn version_list_params() -> version::ListParams {
    version::ListParams {
        prefix: None,
        delimiter: None,
        key_marker: None,
        version_id_marker: None,
        max_keys: 1000,
    }
}

/// A plain write through the layer, answering the committed metadata.
async fn put(
    tier: &TierStorage<FakeCold>,
    bucket: &bucket::Name,
    name: &str,
    data: &[u8],
) -> object::Info {
    let key = key_of(name);
    let staged = tier
        .stage_body(bucket, &key, body(data), None, None)
        .await
        .expect("the body stages");
    tier.commit_object(
        bucket,
        &key,
        staged,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
    )
    .await
    .expect("the write commits")
}

async fn read(tier: &TierStorage<FakeCold>, bucket: &bucket::Name, key: &object::Key) -> Vec<u8> {
    let got = tier
        .get_object(bucket, key, None)
        .await
        .expect("the object is served");
    read_body(got.body).await.expect("the body reads")
}

#[tokio::test]
async fn a_plain_commit_answers_before_the_fs_write() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    let info = put(&tier, &fixture.bucket, "plain.bin", b"buffered bytes").await;

    assert_eq!(info.size, 14);
    assert_eq!(info.etag, ETag::from_content(b"buffered bytes"));
    assert!(
        !fixture.path("plain.bin").exists(),
        "the commit answered before the filesystem write"
    );
    assert!(
        fixture.row(&info.key).await.is_none(),
        "the buffered write records no row before the flush"
    );
    assert!(
        tier.buffer().contains(&fixture.bucket, &info.key),
        "the bytes wait in the write buffer"
    );
    assert_eq!(tier.buffer().len_bytes(), 14);
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_none(),
        "the buffered bytes invalidate the object cache"
    );

    let classed = key_of("classed.bin");
    let staged = tier
        .stage_body_with_class(
            &fixture.bucket,
            &classed,
            body(b"classed"),
            None,
            None,
            Class::StandardIa,
        )
        .await
        .expect("the classed body stages");
    assert!(
        matches!(staged, TierStaged::Fs(_)),
        "a classed body never enters the memory buffer"
    );
    let classed_info = tier
        .commit_object(
            &fixture.bucket,
            &classed,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the classed write commits");
    assert!(
        fixture.path("classed.bin").exists(),
        "a classed commit writes through"
    );
    assert!(!tier.buffer().contains(&fixture.bucket, &classed));
    assert_eq!(classed_info.size, 7);
}

#[tokio::test]
async fn flush_preserves_last_modified() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    let acked = put(&tier, &fixture.bucket, "kept.bin", b"keep my time").await;
    assert!(!fixture.path("kept.bin").exists());

    tier.list_objects(list_params(&fixture.bucket))
        .await
        .expect("the listing flushes");

    let row = fixture.row(&acked.key).await.expect("the flushed row");
    assert_eq!(
        row.last_modified, acked.last_modified,
        "the row keeps the acknowledged time"
    );
    assert_eq!(row.etag, acked.etag, "the row keeps the acknowledged ETag");
    let modified = fs::metadata(fixture.path("kept.bin"))
        .expect("the flushed file")
        .modified()
        .expect("the file time");
    assert_eq!(
        modified, acked.last_modified,
        "the file itself keeps the acknowledged time"
    );
    let head = tier
        .head_object(&fixture.bucket, &acked.key)
        .await
        .expect("the flushed object is headed");
    assert_eq!(head.last_modified, acked.last_modified);
    assert_eq!(head.size, 12);
    assert!(tier.buffer().is_empty());
}

#[tokio::test]
async fn flush_resets_placement() {
    let fixture = Fixture::new().await;
    let cold = fixture.make_cold("replaced.bin", b"cold bytes").await;
    let tier = fixture.tier(64 * 1024);
    tier.cache().insert(
        &fixture.bucket,
        &cold.key,
        Bytes::from_static(b"cold bytes"),
    );

    let acked = put(&tier, &fixture.bucket, "replaced.bin", b"new buffered").await;
    assert_eq!(
        fixture
            .placement(&cold.key)
            .expect("the old placement")
            .tier,
        Tier::Pg,
        "the buffered commit leaves the destination placement until the flush"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &cold.key).is_none(),
        "the buffered commit invalidates the cached bytes"
    );
    assert!(!fixture.path("replaced.bin").exists());

    tier.list_objects(list_params(&fixture.bucket))
        .await
        .expect("the listing flushes");

    assert!(
        fixture.placement(&cold.key).is_none(),
        "the flush clears the destination placement"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &cold.key),
        "the flush removes the destination's pg copy"
    );
    assert_eq!(
        fs::read(fixture.path("replaced.bin")).expect("the flushed file"),
        b"new buffered"
    );
    let row = fixture.row(&acked.key).await.expect("the flushed row");
    assert_eq!(row.etag, acked.etag);
    assert_eq!(row.last_modified, acked.last_modified);
}

#[tokio::test]
async fn list_flushes_first() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    put(&tier, &fixture.bucket, "listed.bin", b"listed").await;
    assert!(!fixture.path("listed.bin").exists());

    let page = tier
        .list_objects(list_params(&fixture.bucket))
        .await
        .expect("the listing runs");

    assert_eq!(page.objects.len(), 1, "the buffered key is listed");
    assert_eq!(page.objects[0].key.as_ref(), "listed.bin");
    assert!(
        fixture.path("listed.bin").exists(),
        "the client listing flushed the buffer first"
    );
    assert!(tier.buffer().is_empty());
}

#[tokio::test]
async fn list_object_versions_flushes_first() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    put(&tier, &fixture.bucket, "versioned.bin", b"versioned").await;
    assert!(!fixture.path("versioned.bin").exists());

    let page = tier
        .list_object_versions(&fixture.bucket, version_list_params())
        .await
        .expect("the version listing runs");

    assert_eq!(page.entries.len(), 1, "the buffered key is listed");
    assert_eq!(page.entries[0].key.as_ref(), "versioned.bin");
    assert!(
        fixture.path("versioned.bin").exists(),
        "the client version listing flushed the buffer first"
    );
    assert!(tier.buffer().is_empty());
}

#[tokio::test]
async fn a_buffered_key_is_flushed_before_element_or_byte_ops() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);

    put(&tier, &fixture.bucket, "get.bin", b"get me").await;
    let get = key_of("get.bin");
    assert_eq!(read(&tier, &fixture.bucket, &get).await, b"get me");
    assert!(fixture.path("get.bin").exists(), "GET flushed the key");
    assert!(tier.buffer().is_empty());

    put(&tier, &fixture.bucket, "head.bin", b"head me").await;
    let head = key_of("head.bin");
    let info = tier
        .head_object(&fixture.bucket, &head)
        .await
        .expect("HEAD flushed the key");
    assert_eq!(info.size, 7);
    assert!(fixture.path("head.bin").exists());

    put(&tier, &fixture.bucket, "tagged.bin", b"tag me").await;
    let tagged = key_of("tagged.bin");
    let tags = tier
        .get_object_tags(&fixture.bucket, &tagged)
        .await
        .expect("the tag read flushed the key");
    assert!(tags.is_empty());
    assert!(fixture.path("tagged.bin").exists());

    put(&tier, &fixture.bucket, "tagged2.bin", b"tag me too").await;
    let tagged2 = key_of("tagged2.bin");
    let written = object::Tags::from_pairs([("tier".to_owned(), "batch".to_owned())])
        .expect("a valid tag set");
    tier.put_object_tags(&fixture.bucket, &tagged2, &written)
        .await
        .expect("the tag write flushed the key");
    assert_eq!(
        fixture.row(&tagged2).await.expect("the tagged row").tags,
        written
    );
    assert!(fixture.path("tagged2.bin").exists());

    put(&tier, &fixture.bucket, "acl.bin", b"acl me").await;
    let acl = key_of("acl.bin");
    let grants = tier
        .get_object_acl(&fixture.bucket, &acl)
        .await
        .expect("the ACL read flushed the key");
    assert_eq!(grants.grants, Acl::default_private(None).grants);
    assert!(grants.owner.is_none());
    assert!(fixture.path("acl.bin").exists());

    put(&tier, &fixture.bucket, "source.bin", b"copy me").await;
    let source = key_of("source.bin");
    let destination = key_of("destination.bin");
    tier.copy_object(
        &fixture.bucket,
        &source,
        &fixture.bucket,
        &destination,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
        None,
    )
    .await
    .expect("the copy flushed the source");
    assert_eq!(
        fs::read(fixture.path("source.bin")).expect("the flushed source"),
        b"copy me"
    );
    assert_eq!(
        fs::read(fixture.path("destination.bin")).expect("the copy landed"),
        b"copy me"
    );
    assert!(tier.buffer().is_empty());
}

#[tokio::test]
async fn deleting_a_buffered_key_delegates() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    let acked = put(&tier, &fixture.bucket, "doomed.bin", b"delete me").await;
    assert!(!fixture.path("doomed.bin").exists());

    let marker = tier
        .delete_object(&fixture.bucket, &acked.key)
        .await
        .expect("the delete flushes and then deletes");

    assert!(
        !marker.is_delete_marker,
        "an unversioned delete is no marker"
    );
    assert!(
        !fixture.path("doomed.bin").exists(),
        "the flushed object is deleted"
    );
    assert!(
        fixture.row(&acked.key).await.is_none(),
        "the row is gone after the delete"
    );
    assert!(fixture.placement(&acked.key).is_none());
    assert!(tier.buffer().is_empty(), "the key left the buffer");
    let contract: storage::Error = tier
        .get_object(&fixture.bucket, &acked.key, None)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(contract, storage::Error::NoSuchKey(_)));
}

#[tokio::test]
async fn the_buffer_cap_stops_acknowledging() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(16);
    put(&tier, &fixture.bucket, "a.bin", b"aaaaaaaa").await;
    put(&tier, &fixture.bucket, "b.bin", b"bbbbbbbb").await;
    assert!(
        !fixture.path("a.bin").exists() && !fixture.path("b.bin").exists(),
        "the acknowledged writes wait in the buffer"
    );
    assert_eq!(
        tier.buffer().len_bytes(),
        16,
        "the buffer holds exactly its cap"
    );

    // A directory at each object path makes each publish fail: the
    // cap-crossing admission flushes first, quarantines both poisoned entries
    // instead of wedging, and the crossing write is acknowledged into the
    // space they freed.
    fs::create_dir_all(fixture.path("a.bin").join("blocker")).expect("the blocker exists");
    fs::create_dir_all(fixture.path("b.bin").join("blocker")).expect("the blocker exists");
    let crossed = key_of("c.bin");
    let staged = tier
        .stage_body(&fixture.bucket, &crossed, body(b"c"), None, None)
        .await
        .expect("the crossing body stages");
    tier.commit_object(
        &fixture.bucket,
        &crossed,
        staged,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
    )
    .await
    .expect("the poison entries are quarantined and the crossing write is acknowledged");
    assert!(
        tier.buffer().contains(&fixture.bucket, &crossed),
        "the crossing write is buffered once the poison entries left"
    );
    assert_eq!(tier.buffer().len_bytes(), 1);
    assert!(!fixture.path("c.bin").exists());

    let quarantined = tier.quarantined().expect("the quarantine is listed");
    let names: Vec<String> = quarantined
        .iter()
        .map(|record| record.key.to_string())
        .collect();
    assert_eq!(names, ["a.bin", "b.bin"]);
    assert_eq!(
        tier.clear_quarantine().expect("the pairs clear"),
        2,
        "both poison entries are removed"
    );

    fs::remove_dir_all(fixture.path("a.bin")).expect("the blocker is removed");
    fs::remove_dir_all(fixture.path("b.bin")).expect("the blocker is removed");
    tier.shutdown().await.expect("the drain reaches the end");
    assert_eq!(
        fs::read(fixture.path("c.bin")).expect("the buffered write landed"),
        b"c"
    );
    assert!(
        !fixture.path("a.bin").exists() && !fixture.path("b.bin").exists(),
        "a quarantined write is not published by a later drain"
    );
    assert!(tier.buffer().is_empty());
}

#[tokio::test]
async fn shutdown_drains() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    let first = put(&tier, &fixture.bucket, "first.bin", b"first").await;
    let second = put(&tier, &fixture.bucket, "second.bin", b"second").await;
    assert_eq!(tier.buffer().len_bytes(), 11);

    tier.shutdown()
        .await
        .expect("the shutdown drains the buffer");

    assert!(tier.buffer().is_empty());
    for (acked, expected) in [(&first, &b"first"[..]), (&second, &b"second"[..])] {
        assert_eq!(
            fs::read(fixture.path(acked.key.as_ref())).expect("the drained file"),
            expected
        );
        let row = fixture.row(&acked.key).await.expect("the drained row");
        assert_eq!(
            row.last_modified, acked.last_modified,
            "the drain keeps the acknowledged time"
        );
    }
}

#[tokio::test]
async fn the_metrics_scan_does_not_flush() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    let acked = put(&tier, &fixture.bucket, "counted.bin", b"count me").await;

    let page = tier
        .metrics_scan(list_params(&fixture.bucket))
        .await
        .expect("the metrics scan runs");

    assert!(
        page.objects.is_empty(),
        "the metrics scan never flushes the buffer"
    );
    assert!(
        !fixture.path("counted.bin").exists(),
        "the metrics scan wrote nothing"
    );
    assert_eq!(
        tier.buffer().len_bytes(),
        acked.size,
        "the acknowledged write is still buffered"
    );
}

#[tokio::test]
async fn a_folder_marker_is_never_buffered() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    let marker = key_of("dir/");
    let staged = tier
        .stage_body(&fixture.bucket, &marker, body(b""), None, None)
        .await
        .expect("the marker stages");
    let info = tier
        .commit_object(
            &fixture.bucket,
            &marker,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the marker commits");

    assert!(
        !tier.buffer().contains(&fixture.bucket, &marker),
        "a folder marker never waits in the buffer: a flush cannot publish a directory"
    );
    assert!(tier.buffer().is_empty());
    assert_eq!(
        info.etag,
        ETag::EMPTY,
        "the filesystem marker branch answers the empty ETag"
    );
    assert_eq!(info.size, 0);
    assert!(
        fixture.path("dir").is_dir(),
        "the marker is the directory the filesystem commit created"
    );
    assert!(
        fixture.row(&marker).await.is_none(),
        "a marker never records an object row"
    );

    let page = tier
        .list_objects(list_params(&fixture.bucket))
        .await
        .expect("the listing does not try to flush the marker");
    assert!(page.objects.is_empty());
    tier.shutdown()
        .await
        .expect("the drain has no buffered marker to fail on");
}

#[tokio::test]
async fn an_oversize_plain_write_writes_through() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(8);
    let data = b"this is larger than the batch cap";
    let info = put(&tier, &fixture.bucket, "oversize.bin", data).await;

    assert!(
        fixture.path("oversize.bin").exists(),
        "a write larger than the buffer cap is written through before it is acknowledged"
    );
    assert!(tier.buffer().is_empty());
    assert_eq!(info.size, data.len() as u64);
    assert_eq!(info.etag, ETag::from_content(data));
    assert!(
        fixture.row(&info.key).await.is_some(),
        "the write-through records its row"
    );
}

#[tokio::test]
async fn a_cache_held_budget_writes_through() {
    let fixture = Fixture::new().await;
    let capacity = 64 * 1024;
    let tier = fixture.tier_at(capacity, 8 * 1024);
    let filler = key_of("filler.bin");
    assert!(
        tier.cache().insert(
            &fixture.bucket,
            &filler,
            Bytes::from(vec![b'x'; capacity as usize])
        ),
        "the cache takes the whole shared budget"
    );
    assert_eq!(tier.cache().len_bytes(), capacity);

    let info = put(&tier, &fixture.bucket, "small.bin", b"tiny").await;

    assert!(
        fixture.path("small.bin").exists(),
        "a write the shared budget cannot charge is written through before it is acknowledged"
    );
    assert!(tier.buffer().is_empty());
    assert_eq!(
        tier.cache().len_bytes(),
        capacity,
        "the cache keeps its share; the buffer charged nothing"
    );
    assert_eq!(info.size, 4);
}

#[tokio::test]
async fn a_buffered_overwrite_forgets_the_keys_heat() {
    let fixture = Fixture::new().await;
    let tier = fixture.tier(64 * 1024);
    let info = put(&tier, &fixture.bucket, "heat.bin", b"first").await;
    // The first write reaches the warm plane through the same flush a read
    // drives, so the key has a row and a file before the overwrite.
    tier.head_object(&fixture.bucket, &info.key)
        .await
        .expect("the first buffered write flushes");
    // Two reads cross the promotion threshold (promote_reads is two) and the
    // durable row is planted exactly as a stats flush would plant it.
    tier.heat().record_read(&fixture.bucket, &info.key);
    tier.heat().record_read(&fixture.bucket, &info.key);
    fixture
        .index
        .absorb_heat(&[HeatEntry {
            bucket: fixture.bucket.clone(),
            key: info.key.clone(),
            reads: 2,
            last_read: SystemTime::now(),
        }])
        .expect("the durable heat row is planted");
    assert_eq!(tier.heat().score(&fixture.bucket, &info.key), 2);
    assert_eq!(
        tier.heat().promote_queue().len(),
        1,
        "the reads queued the key for promotion"
    );

    // The overwrite is buffered; its flush is the write that completes it.
    put(&tier, &fixture.bucket, "heat.bin", b"second").await;
    assert!(tier.buffer().contains(&fixture.bucket, &info.key));
    tier.head_object(&fixture.bucket, &info.key)
        .await
        .expect("the overwrite flushes");

    assert_eq!(
        tier.heat().score(&fixture.bucket, &info.key),
        0,
        "no in-memory counter survives the buffered overwrite"
    );
    assert!(
        tier.heat().promote_queue().is_empty(),
        "no queued promotion survives the buffered overwrite"
    );
    assert!(
        fixture
            .index
            .scan_heat(10)
            .expect("the heat scan runs")
            .is_empty(),
        "no durable heat row survives the buffered overwrite"
    );

    let mover = Mover::new(
        tier.clone(),
        Arc::new(VolumeCapacity::new(fixture.root.path())),
        1,
    );
    mover.flush_stats().expect("the stats flush runs");
    assert!(
        fixture
            .index
            .scan_heat(10)
            .expect("the heat scan runs")
            .is_empty(),
        "the following stats flush does not recreate the row"
    );
}
