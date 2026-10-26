//! The tier read path over a real filesystem store and a fake cold plane.
//!
//! Each test builds a real [`FsStorage`] in a temporary directory — the warm
//! plane the layer decorates — and a [`FakeCold`] implementing
//! [`ObjectContent`] as the cold plane's byte store. A cold object is a row
//! with no file, exactly the shape demotion leaves (T4), so the tests exercise
//! the layer's reconciliation against real row state rather than a mock of the
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
    Class,
    acl::Acl,
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    meta::bucket::encode_grants,
    multipart::{self, CompletedPart},
    object,
    storage::{
        self, BodyStream, BucketOps, ByteRange, MultipartOps, ObjectOps, Storage, VersionOps,
    },
    version,
};
use tinio_fs::{FsStorage, testing};
use tinio_tier::{ColdInfo, Config, FlushMode, Index, Placement, State, Tier, TierStorage};
use tinio_util::testing::{body, conform_owner, read_body};

/// A key-value byte plane standing in for PostgreSQL's large objects.
#[derive(Clone, Default)]
struct FakeCold {
    inner: Arc<Mutex<FakeInner>>,
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

    fn remove(&self, bucket: &bucket::Name, key: &object::Key) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .objects
            .remove(&(bucket.as_ref().to_owned(), key.as_ref().to_owned()));
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
        let Some(object_key) = key.object() else {
            return Ok(());
        };
        self.remove(bucket, object_key);
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

    async fn remove_bucket(&self, _bucket: &bucket::Name) -> Result<(), Self::Error> {
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

    fn path(&self, key: &str) -> PathBuf {
        self.root.path().join(self.bucket.as_ref()).join(key)
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

    fn placement_is_set(&self, key: &object::Key) -> bool {
        self.index
            .placement(&self.bucket, key)
            .expect("the placement is read")
            .is_some()
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

#[tokio::test]
async fn a_warm_get_delegates() {
    let fixture = Fixture::new().await;
    let info = fixture.put("warm.txt", b"hello").await;
    let tier = fixture.tier();

    let got = tier
        .get_object(&fixture.bucket, &info.key, None)
        .await
        .expect("the warm object is served");

    assert_eq!(read_body(got.body).await.expect("the body"), b"hello");
    assert_eq!(got.info.etag, info.etag);
    assert_eq!(got.served_range, None);
    assert!(
        fixture.path("warm.txt").exists(),
        "the warm plane's own file answered"
    );
    assert_eq!(fixture.cold.opens(), 0, "the cold plane was never opened");
}

#[tokio::test]
async fn a_cold_get_streams_pg() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("cold.txt", b"from postgres").await;
    let tier = fixture.tier();

    assert!(
        !fixture.path("cold.txt").exists(),
        "a cold object has no file"
    );
    let bytes = get_bytes(&tier, &fixture.bucket, &info.key, None).await;

    assert_eq!(bytes, b"from postgres");
    assert_eq!(
        fixture.cold.opens(),
        1,
        "the bytes came from the cold plane"
    );
}

#[tokio::test]
async fn cold_info_comes_from_cold_info() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("cold.txt", b"hello").await;
    let tags = object::Tags::from_pairs([("tier".to_owned(), "cold".to_owned())])
        .expect("a valid tag set");
    let recorded = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut cold = ColdInfo::from_info(&info);
    cold.tags = tags.to_wire();
    cold.last_modified = recorded;
    fixture.place_cold(&info, cold);
    let tier = fixture.tier();

    let head = tier
        .head_object(&fixture.bucket, &info.key)
        .await
        .expect("the cold object is headed");

    assert_eq!(head.tags, tags, "the cold authority's tags answered");
    assert_eq!(head.last_modified, recorded);
    assert_eq!(head.size, info.size);
    assert_eq!(head.etag, info.etag);
    assert!(!head.bucket_records_versions);
}

#[tokio::test]
async fn a_missing_row_means_deleted() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("gone.txt", b"data").await;
    fixture
        .fs
        .delete_object(&fixture.bucket, &info.key)
        .await
        .expect("the row is deleted");
    let tier = fixture.tier();

    let err = tier
        .get_object(&fixture.bucket, &info.key, None)
        .await
        .expect_err("the object was deleted out from under the cold placement");
    let contract: storage::Error = err.into();

    assert!(matches!(contract, storage::Error::NoSuchKey(_)));
    assert!(contract.to_string().contains("gone.txt"));
    assert!(
        !fixture.placement_is_set(&info.key),
        "the reconcile clears the dead placement"
    );
    assert!(
        !fixture.cold.contains(&fixture.bucket, &info.key),
        "the reconcile removes the cold copy"
    );
}

#[tokio::test]
async fn a_file_with_a_different_etag_means_fs_won() {
    let fixture = Fixture::new().await;
    let info = fixture.put("both.txt", b"fs bytes").await;
    fixture
        .cold
        .insert(&fixture.bucket, &info.key, b"cold bytes");
    let mut cold = ColdInfo::from_info(&info);
    cold.etag = "d41d8cd98f00b204e9800998ecf8427e".to_owned();
    fixture.place_cold(&info, cold);
    let tier = fixture.tier();
    // A stale cached body of the same length: a read that kept it would serve
    // it from the memory plane instead of the winning file.
    tier.cache()
        .insert(&fixture.bucket, &info.key, Bytes::from_static(b"stale!!!"));

    let bytes = get_bytes(&tier, &fixture.bucket, &info.key, None).await;

    assert_eq!(bytes, b"fs bytes", "the completed overwrite is the file's");
    assert_eq!(fixture.cold.opens(), 0);
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_none(),
        "the read drops the cached bytes it can no longer trust"
    );
    assert!(
        fixture.placement_is_set(&info.key),
        "the read leaves the placement to the write path and garbage collection"
    );
    assert!(
        fixture.cold.contains(&fixture.bucket, &info.key),
        "the read never touches the cold copy"
    );
}

#[tokio::test]
async fn a_file_with_the_same_etag_is_removed_later() {
    let fixture = Fixture::new().await;
    let info = fixture.put("crash.txt", b"hello").await;
    fixture.cold.insert(&fixture.bucket, &info.key, b"cold!");
    fixture.place_cold(&info, ColdInfo::from_info(&info));
    let tier = fixture.tier();

    let bytes = get_bytes(&tier, &fixture.bucket, &info.key, None).await;

    assert_eq!(
        bytes, b"cold!",
        "a same-ETag file is a crash before removal: the cold plane answers"
    );
    assert!(
        fixture.path("crash.txt").exists(),
        "the leftover file is left for garbage collection"
    );
    assert!(fixture.placement_is_set(&info.key));
}

#[tokio::test]
async fn a_missing_pg_copy_errors() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("nopg.txt", b"hello").await;
    fixture.cold.remove(&fixture.bucket, &info.key);
    let tier = fixture.tier();

    let err = tier
        .get_object(&fixture.bucket, &info.key, None)
        .await
        .expect_err("the cold copy is missing");
    let contract: storage::Error = err.into();

    assert!(matches!(contract, storage::Error::NoSuchKey(_)));
    assert!(contract.to_string().contains("nopg.txt"));
}

#[tokio::test]
async fn a_length_mismatch_errors() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("short.txt", b"hello").await;
    fixture.cold.insert(&fixture.bucket, &info.key, b"hi");
    let tier = fixture.tier();

    let err = tier
        .get_object(&fixture.bucket, &info.key, None)
        .await
        .expect_err("the cold copy's length disagrees with the metadata");
    let message = err.to_string();

    assert!(
        message.contains("short.txt"),
        "the error names the key: {message}"
    );
    assert!(message.contains('5'), "the recorded length: {message}");
    assert!(message.contains('2'), "the stored length: {message}");
}

#[tokio::test]
async fn a_full_get_fills_mem() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("cached.txt", b"hello").await;
    let tier = fixture.tier();

    let first = get_bytes(&tier, &fixture.bucket, &info.key, None).await;
    assert_eq!(first, b"hello");
    assert_eq!(
        tier.cache().get(&fixture.bucket, &info.key),
        Some(Bytes::from_static(b"hello")),
        "a full GET admits the object to the cache"
    );

    let second = get_bytes(&tier, &fixture.bucket, &info.key, None).await;
    assert_eq!(second, b"hello");
    assert_eq!(fixture.cold.opens(), 1, "the second GET is a memory hit");
}

#[tokio::test]
async fn a_range_get_does_not_fill_mem() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("ranged.txt", b"hello").await;
    let tier = fixture.tier();

    let got = tier
        .get_object(&fixture.bucket, &info.key, Some(ByteRange::Inclusive(1, 3)))
        .await
        .expect("the ranged read is served");
    assert_eq!(got.served_range, Some((1, 3)));
    assert_eq!(read_body(got.body).await.expect("the body"), b"ell");
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_none(),
        "a range never fills the cache"
    );
}

#[tokio::test]
async fn a_mem_hit_records_heat() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("hot.txt", b"hello").await;
    let tier = fixture.tier();

    get_bytes(&tier, &fixture.bucket, &info.key, None).await;
    get_bytes(&tier, &fixture.bucket, &info.key, None).await;

    assert_eq!(fixture.cold.opens(), 1, "the second GET is a memory hit");
    assert_eq!(
        tier.heat().score(&fixture.bucket, &info.key),
        2,
        "every GET counts, the memory hits included"
    );
}

#[tokio::test]
async fn cache_is_invalidated_on_every_mutation() {
    let fixture = Fixture::new().await;
    let info = fixture.put("mut.txt", b"one").await;
    let key = info.key.clone();
    let tier = fixture.tier();
    let bucket = fixture.bucket.clone();
    let stale = || Bytes::from_static(b"stale");

    // A commit.
    tier.cache().insert(&bucket, &key, stale());
    tier.put_object(&bucket, &key, body(b"two"))
        .await
        .expect("the overwrite commits");
    assert!(tier.cache().get(&bucket, &key).is_none(), "commit");

    // A copy, onto a destination.
    let copy = object::key("copy.txt").expect("a valid key");
    tier.cache().insert(&bucket, &copy, stale());
    tier.copy_object(
        &bucket,
        &key,
        &bucket,
        &copy,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
        None,
    )
    .await
    .expect("the copy commits");
    assert!(tier.cache().get(&bucket, &copy).is_none(), "copy");

    // A rename, from the source key to the destination.
    let moved = object::key("moved.txt").expect("a valid key");
    tier.cache().insert(&bucket, &key, stale());
    tier.rename_object(&bucket, &key, &moved)
        .await
        .expect("the move commits");
    assert!(tier.cache().get(&bucket, &key).is_none(), "rename source");
    assert!(tier.cache().get(&bucket, &moved).is_none(), "rename target");

    // A multipart upload's part copy and completion, on the upload's key.
    let multi = object::key("multi.txt").expect("a valid key");
    let upload = tier
        .create_multipart_upload(
            &bucket,
            &multi,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the upload is created");
    let part_number = multipart::part_number(1).expect("a valid part number");
    let part = tier
        .upload_part(
            &bucket,
            &multi,
            &upload.upload_id,
            part_number,
            body(b"part one"),
            None,
        )
        .await
        .expect("the part lands");

    tier.cache().insert(&bucket, &multi, stale());
    let copied_number = multipart::part_number(2).expect("a valid part number");
    tier.copy_part(
        &bucket,
        &moved,
        &bucket,
        &multi,
        &upload.upload_id,
        copied_number,
        None,
    )
    .await
    .expect("the part copy lands");
    assert!(tier.cache().get(&bucket, &multi).is_none(), "copy_part");

    tier.cache().insert(&bucket, &multi, stale());
    tier.complete_multipart_upload(
        &bucket,
        &multi,
        &upload.upload_id,
        &[CompletedPart {
            part_number,
            etag: part.etag,
        }],
        None,
    )
    .await
    .expect("the upload completes");
    assert!(
        tier.cache().get(&bucket, &multi).is_none(),
        "complete_multipart_upload"
    );

    // A delete.
    tier.cache().insert(&bucket, &moved, stale());
    tier.delete_object(&bucket, &moved)
        .await
        .expect("the delete commits");
    assert!(tier.cache().get(&bucket, &moved).is_none(), "delete");
}

#[tokio::test]
async fn a_null_version_read_matches_the_placement() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("nullv.txt", b"hello").await;
    let tier = fixture.tier();

    let read = tier
        .get_object_version(&fixture.bucket, &info.key, None, None)
        .await
        .expect("the null version routes through the cold path");
    let version::ReadResult::Object(got) = read else {
        panic!("a cold object is not a delete marker");
    };
    assert_eq!(read_body(got.body).await.expect("the body"), b"hello");

    // The same null address against a placement naming a real identity names
    // no version at all.
    let mut cold = ColdInfo::from_info(&info);
    cold.version_id = "v1".to_owned();
    fixture.place_cold(&info, cold);
    let err = tier
        .get_object_version(&fixture.bucket, &info.key, None, None)
        .await
        .expect_err("the null address does not name the placement's version");
    let contract: storage::Error = err.into();
    assert!(matches!(contract, storage::Error::NoSuchVersion(_)));
}

#[tokio::test]
async fn cold_tags_acl_and_parts_work_without_content() {
    let fixture = Fixture::new().await;
    let bucket = fixture.bucket.clone();
    let key = object::key("parts.bin").expect("a valid key");
    let upload = fixture
        .fs
        .create_multipart_upload(
            &bucket,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the upload is created");
    let part_number = multipart::part_number(1).expect("a valid part number");
    let part = fixture
        .fs
        .upload_part(
            &bucket,
            &key,
            &upload.upload_id,
            part_number,
            body(b"hello"),
            None,
        )
        .await
        .expect("the part lands");
    let info = fixture
        .fs
        .complete_multipart_upload(
            &bucket,
            &key,
            &upload.upload_id,
            &[CompletedPart {
                part_number,
                etag: part.etag,
            }],
            None,
        )
        .await
        .expect("the upload completes");
    fs::remove_file(fixture.path("parts.bin")).expect("the warm file is removed");
    fixture.cold.insert(&bucket, &key, b"hello");

    let tags =
        object::Tags::from_pairs([("job".to_owned(), "kept".to_owned())]).expect("a valid tag set");
    let owner = conform_owner();
    let acl = Acl::default_private(Some(owner.clone()));
    let mut cold = ColdInfo::from_info(&info);
    cold.tags = tags.to_wire();
    cold.owner = owner.as_str().to_owned();
    cold.acl = encode_grants(&acl);
    fixture.place_cold(&info, cold);
    let tier = fixture.tier();

    assert_eq!(
        tier.get_object_tags(&bucket, &key)
            .await
            .expect("tags read"),
        tags,
        "the cold authority answers the tags"
    );
    let got_acl = tier
        .get_object_acl(&bucket, &key)
        .await
        .expect("the ACL reads");
    assert_eq!(got_acl.owner, Some(owner));
    assert_eq!(got_acl.grants, acl.grants);

    let parts = tier
        .list_object_parts(&bucket, &key)
        .await
        .expect("the retained parts read");
    assert_eq!(parts.len(), 1, "the fs row keeps the retained parts");
    assert_eq!(parts[0].part_number, part_number);
    assert_eq!(parts[0].size, 5);
}

#[tokio::test]
async fn storage_class_is_ia_for_pg() {
    let fixture = Fixture::new().await;
    let warm = fixture.put("warm.txt", b"x").await;
    let cold = fixture.make_cold("cold.txt", b"y").await;
    let tier = fixture.tier();

    assert_eq!(
        tier.storage_class(&fixture.bucket, &warm.key)
            .await
            .expect("the warm class reads"),
        Class::Standard
    );
    assert_eq!(
        tier.storage_class(&fixture.bucket, &cold.key)
            .await
            .expect("the cold class reads"),
        Class::StandardIa
    );
}

/// Enabling versioning changes the version facts every cached object's
/// metadata carries, so a warm memory hit must not answer the facts the entry
/// was filled under.
#[tokio::test]
async fn a_versioning_transition_invalidates_the_cached_version_facts() {
    let fixture = Fixture::new().await;
    let info = fixture.make_cold("facts.bin", b"version facts").await;
    let tier = fixture.tier();

    // The full GET fills the memory plane while the bucket records no
    // versions.
    let before = tier
        .get_object(&fixture.bucket, &info.key, None)
        .await
        .expect("the cold object is served");
    assert!(!before.info.bucket_records_versions);
    assert!(
        tier.cache().get(&fixture.bucket, &info.key).is_some(),
        "the full GET fills the cache"
    );
    // Drop the served body before the promotion takes the key's write lock.
    drop(before);

    tier.set_bucket_versioning(&fixture.bucket, version::State::Enabled)
        .await
        .expect("versioning is enabled");

    let after = tier
        .get_object(&fixture.bucket, &info.key, None)
        .await
        .expect("the promoted object is served");
    assert_eq!(
        read_body(after.body).await.expect("the body reads"),
        b"version facts"
    );
    assert!(
        after.info.bucket_records_versions,
        "the warm hit does not answer the pre-transition version facts"
    );
}
