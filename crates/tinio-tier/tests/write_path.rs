//! The tier write path over a real filesystem store and a fake cold plane.
//!
//! Each test builds a real [`FsStorage`] in a temporary directory — the warm
//! plane the layer decorates — and a [`FakeCold`] implementing
//! [`ObjectContent`] as the cold plane's byte store. A cold object is a row
//! with no file, exactly the shape demotion leaves (T4), so the tests assert
//! the real row, placement, cold-copy and cache state each mutation leaves
//! behind rather than a mock of the code under test.

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
    fs, io,
    path::PathBuf,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_core::{
    ETag,
    acl::Acl,
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    multipart::{self, CompletedPart},
    object, object_lock,
    storage::{self, BodyStream, BucketOps, ByteRange, MultipartOps, ObjectOps, VersionOps},
    version,
};
use tinio_fs::{FsStorage, testing};
use tinio_tier::{ColdInfo, Config, FlushMode, Index, Placement, State, Tier, TierStorage};
use tinio_util::testing::{body, read_body};

/// A key-value byte plane standing in for PostgreSQL's large objects.
#[derive(Clone, Default)]
struct FakeCold {
    inner: Arc<Mutex<FakeInner>>,
    /// Whether the next removal fails, so a delete's rollback is observable.
    fail_remove: Arc<AtomicBool>,
}

/// The state every clone of a [`FakeCold`] shares.
#[derive(Default)]
struct FakeInner {
    objects: HashMap<(String, String), Vec<u8>>,
    opens: usize,
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

    fn opens(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .opens
    }

    fn set_fail_remove(&self, fail: bool) {
        self.fail_remove.store(fail, Ordering::SeqCst);
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
        if self.fail_remove.load(Ordering::SeqCst) {
            return Err(storage::io(io::Error::other(
                "the fake cold plane refuses the removal",
            )));
        }
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

    fn bucket_path(&self) -> PathBuf {
        self.root.path().join(self.bucket.as_ref())
    }

    fn path(&self, key: &str) -> PathBuf {
        self.bucket_path().join(key)
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
    /// settled shape of a demoted object (T4).
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
}

async fn get_bytes(
    tier: &TierStorage<FakeCold>,
    bucket: &bucket::Name,
    key: &object::Key,
    range: Option<ByteRange>,
) -> Vec<u8> {
    let got = tier
        .get_object(bucket, key, range)
        .await
        .expect("the object is served");
    read_body(got.body).await.expect("the body reads")
}

fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

#[tokio::test]
async fn overwrite_returns_to_fs_and_invalidates_cache() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("object.bin", b"cold bytes").await;
    let tier = fixture.tier();
    tier.cache().insert(
        &fixture.bucket,
        &info.key,
        Bytes::from_static(b"cold bytes"),
    );

    tier.put_object(&fixture.bucket, &info.key, body(b"new bytes"))
        .await
        .expect("the overwrite commits");

    assert_eq!(
        fs::read(fixture.path("object.bin")).expect("the warm file exists"),
        b"new bytes",
        "the overwrite landed on the warm plane"
    );
    let placement = fixture
        .placement(&info.key)
        .expect("the destination's placement is reset");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert!(
        !fixture.cold.contains(&fixture.bucket, &info.key),
        "the destination's pg copy is removed"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_none(),
        "the destination's cached bytes are invalidated"
    );
    assert_eq!(
        get_bytes(&tier, &fixture.bucket, &info.key, None).await,
        b"new bytes"
    );
}

#[tokio::test]
async fn delete_removes_row_index_pg_and_cache() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("gone.bin", b"delete me").await;
    let tier = fixture.tier();
    tier.cache()
        .insert(&fixture.bucket, &info.key, Bytes::from_static(b"delete me"));

    let marker = tier
        .delete_object(&fixture.bucket, &info.key)
        .await
        .expect("the delete settles");

    assert!(
        !marker.is_delete_marker,
        "an unversioned delete is no marker"
    );
    assert!(
        !fixture.path("gone.bin").exists(),
        "the warm file is absent"
    );
    assert!(
        fixture.row(&info.key).await.is_none(),
        "the object row is removed"
    );
    assert!(
        fixture.placement(&info.key).is_none(),
        "the placement is cleared"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &info.key),
        "the cold copy is removed"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_none(),
        "the cached bytes are invalidated"
    );
    let err = tier
        .head_object(&fixture.bucket, &info.key)
        .await
        .expect_err("the object is gone");
    assert!(matches!(
        storage::Error::from(err),
        storage::Error::NoSuchKey(_)
    ));
}

#[tokio::test]
async fn a_null_version_delete_clears_placement_and_pg() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("null.bin", b"null version").await;
    let tier = fixture.tier();
    tier.cache().insert(
        &fixture.bucket,
        &info.key,
        Bytes::from_static(b"null version"),
    );

    // An unversioned bucket's null address is the layer's own object address
    // (T6), so it routes through `delete_object`.
    tier.delete_object_version(&fixture.bucket, &info.key, None, object_lock::Bypass::None)
        .await
        .expect("the unversioned null-version delete routes through the object delete");

    assert!(
        fixture.row(&info.key).await.is_none(),
        "the null-version row is removed"
    );
    assert!(
        fixture.placement(&info.key).is_none(),
        "the placement is cleared"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &info.key),
        "the cold copy is removed"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_none(),
        "the cached bytes are invalidated"
    );
}

/// A null-address delete of a warm key routes to the plain object delete
/// without the read-side placement guard (T6), and an absent address answers
/// exactly what the plain delete answers.
#[tokio::test]
async fn a_null_version_delete_on_a_warm_and_an_absent_key_routes_to_the_object_delete() {
    let fixture = Fixture::new().await;
    let warm = fixture.put("warm-null.bin", b"warm null version").await;
    let tier = fixture.tier();
    tier.cache().insert(
        &fixture.bucket,
        &warm.key,
        Bytes::from_static(b"warm null version"),
    );

    tier.delete_object_version(&fixture.bucket, &warm.key, None, object_lock::Bypass::None)
        .await
        .expect("the unversioned null-version delete removes the warm object");

    assert!(
        !fixture.path("warm-null.bin").exists(),
        "the warm file is removed"
    );
    assert!(
        fixture.row(&warm.key).await.is_none(),
        "the warm row is removed"
    );
    assert!(
        fixture.placement(&warm.key).is_none(),
        "the placement is cleared"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &warm.key).is_none(),
        "the cached bytes are invalidated"
    );

    // An absent address answers what the plain delete answers: no error, and
    // the same marker shape.
    let absent = object::key("absent-null.bin").expect("a valid object key");
    let plain = tier
        .delete_object(&fixture.bucket, &absent)
        .await
        .expect("the plain delete of an absent key answers");
    let addressed = tier
        .delete_object_version(&fixture.bucket, &absent, None, object_lock::Bypass::None)
        .await
        .expect("the null address of an absent key answers without the guard");
    assert_eq!(
        addressed, plain,
        "the null address answers the plain delete's marker"
    );
}

/// The one state the read-side guard refuses — a cold placement that names a
/// real version — must not reach the delete route: a null-address delete
/// proceeds exactly as it did before the guard existed.
#[tokio::test]
async fn a_null_version_delete_does_not_run_the_read_side_placement_guard() {
    let fixture = Fixture::new().await;
    let mut info = fixture.make_cold("guarded-null.bin", b"guarded").await;
    info.version_id =
        Some(version::id("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY").expect("a valid version identifier"));
    fixture.place_cold(&info, ColdInfo::from_info(&info));

    let tier = fixture.tier();
    tier.delete_object_version(&fixture.bucket, &info.key, None, object_lock::Bypass::None)
        .await
        .expect("a null-address delete never runs the read-side placement guard");

    assert!(fixture.row(&info.key).await.is_none(), "the row is removed");
    assert!(
        fixture.placement(&info.key).is_none(),
        "the placement is cleared"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &info.key),
        "the cold copy is removed"
    );
}

#[tokio::test]
async fn a_null_version_delete_on_a_suspended_bucket_removes_the_version() {
    let fixture = Fixture::new().await;
    // The write happens before the suspend, so the key carries the null
    // version the address names.
    let info = fixture.put("suspended.bin", b"null version").await;
    let tier = fixture.tier();
    tier.set_bucket_versioning(&fixture.bucket, version::State::Suspended)
        .await
        .expect("the bucket is suspended");

    let marker = tier
        .delete_object_version(&fixture.bucket, &info.key, None, object_lock::Bypass::None)
        .await
        .expect("the null version is removed");

    assert!(
        !marker.is_delete_marker,
        "versionId=null on a recording bucket removes the null version, never inserts a marker"
    );
    assert_eq!(marker.id, None, "the removed version is the null version");
    assert_eq!(
        tier.current_version(&fixture.bucket, &info.key)
            .await
            .expect("the probe answers"),
        version::CurrentVersion::Absent,
        "the version delete leaves no delete marker where a plain delete would"
    );
    let err = tier
        .head_object(&fixture.bucket, &info.key)
        .await
        .expect_err("the version delete removed the object");
    assert!(matches!(
        storage::Error::from(err),
        storage::Error::NoSuchKey(_)
    ));
    assert!(
        !fixture.path("suspended.bin").exists(),
        "the null version's file is removed"
    );
    assert!(
        fixture.row(&info.key).await.is_none(),
        "the null version's row is removed"
    );
    let page = tier
        .list_object_versions(
            &fixture.bucket,
            version::ListParams {
                prefix: Some("suspended.bin".into()),
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .expect("the version listing runs");
    assert!(
        page.entries.iter().all(|entry| entry.key != info.key),
        "the null version and any marker row are gone"
    );
}

#[tokio::test]
async fn copy_of_a_cold_source_leaves_it_warm() {
    let fixture = Fixture::new().await;
    let source = fixture
        .make_cold("source.bin", b"from the cold plane")
        .await;
    // The destination starts cold too, so the test can catch a copy that
    // leaves the overwritten key's pg copy behind.
    let stale = fixture.make_cold("destination.bin", b"stale bytes").await;
    let destination = stale.key.clone();
    let tier = fixture.tier();
    tier.cache().insert(
        &fixture.bucket,
        &destination,
        Bytes::from_static(b"stale bytes"),
    );

    let info = tier
        .copy_object(
            &fixture.bucket,
            &source.key,
            &fixture.bucket,
            &destination,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            None,
        )
        .await
        .expect("the copy settles");

    assert_eq!(
        info.etag,
        ETag::from_content(b"from the cold plane"),
        "the copy carries the source's content"
    );
    assert_eq!(
        fs::read(fixture.path("destination.bin")).expect("the destination's file"),
        b"from the cold plane"
    );
    assert_eq!(
        fs::read(fixture.path("source.bin")).expect("the source's file"),
        b"from the cold plane",
        "the cold source is promoted before the copy"
    );
    let source_placement = fixture
        .placement(&source.key)
        .expect("the promoted source keeps a placement");
    assert_eq!(source_placement.tier, Tier::Fs);
    assert_eq!(source_placement.cold, None);
    assert!(
        !fixture.cold.contains(&fixture.bucket, &source.key),
        "the promoted source's pg copy is removed"
    );
    assert_eq!(
        tier.head_object(&fixture.bucket, &source.key)
            .await
            .expect("the promoted source is headed")
            .last_modified,
        source.last_modified,
        "the promoted row keeps the cold modification time"
    );
    let destination_placement = fixture
        .placement(&destination)
        .expect("the destination is reset");
    assert_eq!(destination_placement.tier, Tier::Fs);
    assert!(
        !fixture.cold.contains(&fixture.bucket, &destination),
        "the overwritten destination's pg copy is removed"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &destination).is_none(),
        "the destination's cached bytes are invalidated"
    );
    let opens = fixture.cold.opens();
    assert_eq!(
        get_bytes(&tier, &fixture.bucket, &source.key, None).await,
        b"from the cold plane"
    );
    assert_eq!(
        fixture.cold.opens(),
        opens,
        "the promoted source serves from the warm plane"
    );
}

#[tokio::test]
async fn rename_resets_both_keys() {
    let fixture = Fixture::new().await;
    let source = fixture.make_cold("old.bin", b"moving bytes").await;
    let replaced = fixture.make_cold("new.bin", b"replaced bytes").await;
    let tier = fixture.tier();
    tier.cache().insert(
        &fixture.bucket,
        &source.key,
        Bytes::from_static(b"moving bytes"),
    );
    tier.cache().insert(
        &fixture.bucket,
        &replaced.key,
        Bytes::from_static(b"replaced bytes"),
    );

    let info = tier
        .rename_object(&fixture.bucket, &source.key, &replaced.key)
        .await
        .expect("the rename settles");

    assert_eq!(
        info.etag,
        ETag::from_content(b"moving bytes"),
        "the moved object keeps its content"
    );
    assert!(!fixture.path("old.bin").exists(), "the source file moved");
    assert_eq!(
        fs::read(fixture.path("new.bin")).expect("the destination's file"),
        b"moving bytes"
    );
    assert!(
        fixture.placement(&source.key).is_none(),
        "the source key's placement is cleared"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &source.key),
        "the source key's cold copy is removed"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &source.key).is_none(),
        "the source key's cached bytes are invalidated"
    );
    let placement = fixture
        .placement(&replaced.key)
        .expect("the destination is reset");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.cold, None);
    assert!(
        !fixture.cold.contains(&fixture.bucket, &replaced.key),
        "the overwritten destination's cold copy is removed"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &replaced.key).is_none(),
        "the destination's cached bytes are invalidated"
    );
}

#[tokio::test]
async fn upload_part_copy_from_cold_copies_real_bytes() {
    let fixture = Fixture::new().await;
    let source = fixture.make_cold("source.bin", b"cold source bytes").await;
    let destination = key("upload.bin");
    let tier = fixture.tier();
    let upload = tier
        .create_multipart_upload(
            &fixture.bucket,
            &destination,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the upload starts");

    let part = tier
        .copy_part(
            &fixture.bucket,
            &source.key,
            &fixture.bucket,
            &destination,
            &upload.upload_id,
            multipart::part_number(1).expect("a valid part number"),
            None,
        )
        .await
        .expect("the part copy settles");

    assert_eq!(part.size, 17);
    assert_eq!(
        part.etag,
        ETag::from_content(b"cold source bytes"),
        "the intercepted copy read the real promoted bytes"
    );
    assert_eq!(
        fs::read(fixture.path("source.bin")).expect("the source's file"),
        b"cold source bytes",
        "the cold source is promoted before the part copy"
    );
    assert_eq!(
        fixture
            .placement(&source.key)
            .expect("the promoted source keeps a placement")
            .tier,
        Tier::Fs
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &source.key));

    let completed = [CompletedPart {
        part_number: part.part_number,
        etag: part.etag.clone(),
    }];
    tier.complete_multipart_upload(
        &fixture.bucket,
        &destination,
        &upload.upload_id,
        &completed,
        None,
    )
    .await
    .expect("the upload completes");
    assert_eq!(
        get_bytes(&tier, &fixture.bucket, &destination, None).await,
        b"cold source bytes"
    );
}

#[tokio::test]
async fn an_aborted_upload_part_copy_to_a_fresh_key_leaves_no_placement() {
    let fixture = Fixture::new().await;
    let source = fixture.make_cold("source.bin", b"cold source bytes").await;
    let destination = key("fresh-upload.bin");
    let tier = fixture.tier();
    let upload = tier
        .create_multipart_upload(
            &fixture.bucket,
            &destination,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the upload starts");

    tier.copy_part(
        &fixture.bucket,
        &source.key,
        &fixture.bucket,
        &destination,
        &upload.upload_id,
        multipart::part_number(1).expect("a valid part number"),
        None,
    )
    .await
    .expect("the part copy settles");

    assert!(
        fixture.placement(&destination).is_none(),
        "an UploadPartCopy writes a part, not the object: a fresh destination \
         must not gain a placement"
    );
    assert!(
        !fixture
            .index
            .scan_warm(10)
            .expect("the warm scan runs")
            .iter()
            .any(|candidate| candidate.key == destination),
        "a fresh destination must not become a capacity candidate"
    );

    tier.abort_multipart_upload(&fixture.bucket, &destination, &upload.upload_id)
        .await
        .expect("the upload aborts");

    assert!(
        fixture.placement(&destination).is_none(),
        "the aborted upload leaves no phantom placement behind"
    );
    let report = tier
        .verify(SystemTime::now())
        .await
        .expect("the verification runs");
    assert!(
        report.is_clean(),
        "the aborted part copy leaves nothing for verification: {report:?}"
    );
}

#[tokio::test]
async fn copy_part_from_a_cold_source_resets_a_cold_destination() {
    let fixture = Fixture::new().await;
    let source = fixture.make_cold("source.bin", b"cold source bytes").await;
    let destination = fixture
        .make_cold("upload.bin", b"old destination bytes")
        .await;
    let tier = fixture.tier();
    tier.cache().insert(
        &fixture.bucket,
        &destination.key,
        Bytes::from_static(b"old destination bytes"),
    );
    let upload = tier
        .create_multipart_upload(
            &fixture.bucket,
            &destination.key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the upload starts");

    let part = tier
        .copy_part(
            &fixture.bucket,
            &source.key,
            &fixture.bucket,
            &destination.key,
            &upload.upload_id,
            multipart::part_number(1).expect("a valid part number"),
            None,
        )
        .await
        .expect("the part copy settles");

    assert_eq!(
        part.etag,
        ETag::from_content(b"cold source bytes"),
        "the part holds the promoted source's bytes"
    );
    assert_eq!(
        fixture
            .placement(&source.key)
            .expect("the promoted source keeps a placement")
            .tier,
        Tier::Fs
    );
    assert!(!fixture.cold.contains(&fixture.bucket, &source.key));
    let placement = fixture
        .placement(&destination.key)
        .expect("the cold destination is reset");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert!(
        !fixture.cold.contains(&fixture.bucket, &destination.key),
        "the destination's pg copy is removed"
    );
    assert!(
        tier.cache()
            .get(&fixture.bucket, &destination.key)
            .is_none(),
        "the destination's cached bytes are invalidated"
    );
    assert_eq!(
        fs::read(fixture.path("upload.bin")).expect("the promoted destination's file"),
        b"old destination bytes",
        "the promotion leaves the destination's own bytes in place until completion"
    );
    assert_eq!(
        get_bytes(&tier, &fixture.bucket, &destination.key, None).await,
        b"old destination bytes",
        "the destination stays readable through the upload"
    );
}

#[tokio::test]
async fn complete_over_cold_resets_placement() {
    let fixture = Fixture::new().await;
    let previous = fixture.make_cold("target.bin", b"old cold bytes").await;
    let tier = fixture.tier();
    let upload = tier
        .create_multipart_upload(
            &fixture.bucket,
            &previous.key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the upload starts");
    let part = tier
        .upload_part(
            &fixture.bucket,
            &previous.key,
            &upload.upload_id,
            multipart::part_number(1).expect("a valid part number"),
            body(b"new multipart bytes"),
            None,
        )
        .await
        .expect("the part uploads");
    tier.cache().insert(
        &fixture.bucket,
        &previous.key,
        Bytes::from_static(b"old cold bytes"),
    );

    let info = tier
        .complete_multipart_upload(
            &fixture.bucket,
            &previous.key,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag,
            }],
            None,
        )
        .await
        .expect("the upload completes");

    assert!(info.size > 0);
    assert_eq!(
        fs::read(fixture.path("target.bin")).expect("the completed file"),
        b"new multipart bytes"
    );
    let placement = fixture
        .placement(&previous.key)
        .expect("the destination is reset");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert!(
        !fixture.cold.contains(&fixture.bucket, &previous.key),
        "the overwritten cold copy is removed"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &previous.key).is_none(),
        "the destination's cached bytes are invalidated"
    );
}

#[tokio::test]
async fn versioning_gate_promotes_all_and_rechecks() {
    let fixture = Fixture::new().await;
    let first = fixture.make_cold("first.bin", b"first cold").await;
    let second = fixture.make_cold("second.bin", b"second cold").await;
    let warm = fixture.put("warm.bin", b"already warm").await;
    let tier = fixture.tier();

    tier.set_bucket_versioning(&fixture.bucket, version::State::Enabled)
        .await
        .expect("the versioning gate settles");

    for (info, bytes) in [(&first, &b"first cold"[..]), (&second, &b"second cold"[..])] {
        assert_eq!(
            fs::read(fixture.path(info.key.as_ref())).expect("the promoted file"),
            bytes,
            "every cold object is promoted through the warm content plane"
        );
        let placement = fixture
            .placement(&info.key)
            .expect("the promoted object keeps a placement");
        assert_eq!(placement.tier, Tier::Fs);
        assert_eq!(placement.cold, None);
        assert!(
            !fixture.cold.contains(&fixture.bucket, &info.key),
            "the recheck finds no cold copy left behind"
        );
    }
    assert_eq!(
        fixture.placement(&warm.key).map(|p| p.tier),
        None,
        "a warm key without a cold placement is left alone"
    );
    let cold_left: Vec<_> = fixture
        .index
        .scan_placement(&fixture.bucket, "", None, 100)
        .expect("the placement scan runs")
        .into_iter()
        .filter(|(_, placement)| placement.tier == Tier::Pg)
        .collect();
    assert!(
        cold_left.is_empty(),
        "the recheck leaves no cold placement in the bucket"
    );
    assert_eq!(
        tier.bucket_versioning(&fixture.bucket)
            .await
            .expect("the state is read"),
        version::State::Enabled
    );
}

#[tokio::test]
async fn versioning_suspend_promotes_all_and_rechecks() {
    let fixture = Fixture::new().await;
    let first = fixture.make_cold("first.bin", b"first cold").await;
    let second = fixture.make_cold("second.bin", b"second cold").await;
    let tier = fixture.tier();

    tier.set_bucket_versioning(&fixture.bucket, version::State::Suspended)
        .await
        .expect("the suspend gate settles");

    for (info, bytes) in [(&first, &b"first cold"[..]), (&second, &b"second cold"[..])] {
        assert_eq!(
            fs::read(fixture.path(info.key.as_ref())).expect("the promoted file"),
            bytes,
            "a suspend promotes every cold object through the same branch"
        );
        assert_eq!(
            fixture
                .placement(&info.key)
                .expect("the promoted object keeps a placement")
                .tier,
            Tier::Fs
        );
        assert!(!fixture.cold.contains(&fixture.bucket, &info.key));
    }
    let cold_left: Vec<_> = fixture
        .index
        .scan_placement(&fixture.bucket, "", None, 100)
        .expect("the placement scan runs")
        .into_iter()
        .filter(|(_, placement)| placement.tier == Tier::Pg)
        .collect();
    assert!(
        cold_left.is_empty(),
        "the suspend's recheck leaves no cold placement in the bucket"
    );
    assert_eq!(
        tier.bucket_versioning(&fixture.bucket)
            .await
            .expect("the state is read"),
        version::State::Suspended
    );
}

#[tokio::test]
async fn delete_bucket_flushes_and_refuses_cold() {
    let fixture = Fixture::new().await;
    let object = fixture.make_cold("blocker.bin", b"still cold").await;
    let tier = fixture.tier();

    let err = tier
        .delete_bucket(&fixture.bucket)
        .await
        .expect_err("the index still holds a cold object");
    assert!(
        matches!(storage::Error::from(err), storage::Error::NotEmpty(_)),
        "a cold object makes the bucket not empty"
    );
    assert!(
        fixture.bucket_path().exists(),
        "the refused delete leaves the bucket"
    );

    tier.delete_object(&fixture.bucket, &object.key)
        .await
        .expect("the cold object is deleted");
    tier.delete_bucket(&fixture.bucket)
        .await
        .expect("the emptied bucket is deleted");

    assert!(
        !fixture.bucket_path().exists(),
        "the bucket directory is removed"
    );
    assert!(
        fixture
            .index
            .scan_placement(&fixture.bucket, "", None, 10)
            .expect("the placement scan runs")
            .is_empty(),
        "the bucket's index rows are removed"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &object.key),
        "the pg namespace is removed"
    );
    assert!(
        fixture
            .cold
            .is_empty(&fixture.bucket)
            .await
            .expect("the cold plane answers"),
        "no cold content survives the bucket"
    );
}

#[tokio::test]
async fn a_failed_delete_rolls_the_tombstone_back() {
    let fixture = Fixture::new().await;
    let object = fixture.make_cold("fragile.bin", b"cannot remove me").await;
    let previous = fixture
        .placement(&object.key)
        .expect("the cold placement is planted");
    // The warm plane refuses the delete before the row disappears (the
    // bucket's directory is gone, the row is not), so the rollback's
    // row-survives branch runs.
    fs::remove_dir(fixture.bucket_path()).expect("the empty bucket directory is removed");
    let tier = fixture.tier();

    tier.delete_object(&fixture.bucket, &object.key)
        .await
        .expect_err("the warm delete fails");

    assert_eq!(
        fixture.placement(&object.key),
        Some(previous),
        "the tombstone is rolled back to the placement it replaced"
    );
    assert!(
        fixture.cold.contains(&fixture.bucket, &object.key),
        "the cold copy survives the failed delete"
    );
    assert!(
        fixture.row(&object.key).await.is_some(),
        "the row survives a delete the warm plane refused"
    );
}

#[tokio::test]
async fn a_failed_delete_after_the_row_leaves_a_completable_tombstone() {
    let fixture = Fixture::new().await;
    let object = fixture.make_cold("fragile.bin", b"cannot remove me").await;
    let previous = fixture
        .placement(&object.key)
        .expect("the cold placement is planted");
    fixture.cold.set_fail_remove(true);
    let tier = fixture.tier();

    tier.delete_object(&fixture.bucket, &object.key)
        .await
        .expect_err("the cold removal fails after the row is gone");

    let tombstone = fixture
        .placement(&object.key)
        .expect("the tombstone stays written for garbage collection");
    assert_eq!(
        tombstone.state,
        State::Deleting,
        "a failed delete over an absent row leaves a tombstone, not a live placement"
    );
    assert_eq!(tombstone.tier, Tier::Pg);
    assert_eq!(tombstone.cold, previous.cold);
    assert!(
        fixture.row(&object.key).await.is_none(),
        "the warm half of the delete had already landed"
    );
    assert!(
        fixture.cold.contains(&fixture.bucket, &object.key),
        "the cold copy the failure left is the tombstone's to remove"
    );

    tier.delete_bucket(&fixture.bucket)
        .await
        .expect("a tombstone never blocks the bucket teardown");
    assert!(
        !fixture.bucket_path().exists(),
        "the bucket directory is removed"
    );
    assert!(
        fixture
            .index
            .scan_placement(&fixture.bucket, "", None, 10)
            .expect("the placement scan runs")
            .is_empty(),
        "the teardown removes the tombstone"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &object.key),
        "the teardown removes the pg namespace the tombstone named"
    );
}

#[tokio::test]
async fn a_tag_write_updates_cold_info_and_the_row() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("tagged.bin", b"tag bytes").await;
    let recorded = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut cold = ColdInfo::from_info(&info);
    cold.last_modified = recorded;
    fixture.place_cold(&info, cold);
    let placement_before = fixture
        .placement(&info.key)
        .expect("the cold placement is planted");
    let tags = object::Tags::from_pairs([("tier".to_owned(), "cold".to_owned())])
        .expect("a valid tag set");
    let tier = fixture.tier();
    tier.cache()
        .insert(&fixture.bucket, &info.key, Bytes::from_static(b"tag bytes"));

    let answer = tier
        .put_object_tags(&fixture.bucket, &info.key, &tags)
        .await
        .expect("the tag write settles");

    assert_eq!(answer.version_id, None);
    assert!(!answer.bucket_records_versions);
    let placement = fixture
        .placement(&info.key)
        .expect("the promoted placement is kept");
    assert_eq!(
        placement.cold.as_ref().map(|cold| cold.tags.clone()),
        Some(tags.to_wire()),
        "the cold authority answers the new tags"
    );
    assert_eq!(
        placement.mtime, placement_before.mtime,
        "a tag write never moves the placement's recorded time"
    );
    let row = fixture
        .row(&info.key)
        .await
        .expect("the warm mirror row is updated");
    assert_eq!(row.tags, tags, "the row mirrors the cold authority");
    assert_eq!(row.last_modified, recorded);
    assert_eq!(
        tier.get_object_tags(&fixture.bucket, &info.key)
            .await
            .expect("the tags are read"),
        tags
    );
    assert_eq!(
        tier.head_object(&fixture.bucket, &info.key)
            .await
            .expect("the object is headed")
            .last_modified,
        recorded,
        "the cold last-modified is preserved"
    );
    assert!(
        fixture.cold.contains(&fixture.bucket, &info.key),
        "a tag write never touches the bytes"
    );
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_none(),
        "the cached bytes are invalidated"
    );
}
