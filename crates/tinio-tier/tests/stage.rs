//! The write stage over a real filesystem store and a fake cold plane.
//!
//! Each test builds a real [`FsStorage`] in a temporary directory and a
//! [`FakeCold`] implementing [`ObjectContent`] as the cold plane's byte store.
//! The stage is observed where it is visible: the filesystem's `tmp/` staging
//! directory, the installed object file and the layer's memory plane. A sync
//! write streams through the filesystem stage and never holds the body; a
//! batch-eligible write buffers up to `max_object_bytes` and spills past the
//! cap (T7). Nothing an uncommitted stage wrote is visible, and a classed write
//! never becomes batch-eligible.

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
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_core::{
    Class, ETag,
    acl::Acl,
    bucket, checksum,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    object,
    storage::{self, BodyStream, BucketOps, ByteRange, ObjectOps},
};
use tinio_fs::{FsStorage, testing};
use tinio_tier::{Config, FlushMode, Index, TierStaged, TierStorage};
use tinio_util::testing::{body, checksum_tee, read_body};
use tokio::time::sleep;

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

/// A staged write of the fake plane: the bytes land in a vector.
struct FakeStaged(Vec<u8>);

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
    config: Config,
}

/// The runtime configuration every test uses: one flush mode, one object cap,
/// and otherwise small limits.
fn config(flush_mode: FlushMode, max_object_bytes: u64) -> Config {
    Config {
        capacity_bytes: 1024 * 1024,
        max_object_bytes,
        flush_mode,
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
    async fn new(flush_mode: FlushMode, max_object_bytes: u64) -> Self {
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
            config: config(flush_mode, max_object_bytes),
        }
    }

    fn tier(&self) -> TierStorage<FakeCold> {
        TierStorage::from_parts(
            Arc::clone(&self.fs),
            self.cold.clone(),
            Arc::clone(&self.index),
            self.config.clone(),
        )
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.path().join(self.bucket.as_ref()).join(key)
    }

    /// The files under the filesystem's `tmp/` staging directory.
    fn staged_files(&self) -> Vec<PathBuf> {
        let tmp = self.fs.state_dir().join("tmp");
        let Ok(entries) = fs::read_dir(&tmp) else {
            return Vec::new();
        };
        entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .collect()
    }

    /// Wait for an uncommitted stage's asynchronous cleanup to land.
    async fn wait_for_staged_cleanup(&self) {
        for _ in 0..200 {
            if self.staged_files().is_empty() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            self.staged_files().is_empty(),
            "an aborted stage left a temp file: {:?}",
            self.staged_files()
        );
    }
}

/// A body stream that delivers `data` in `chunk`-sized pieces.
fn chunked(data: &[u8], chunk: usize) -> BodyStream {
    let pieces: Vec<io::Result<Bytes>> = data
        .chunks(chunk)
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect();
    Box::pin(stream::iter(pieces))
}

async fn get_bytes(
    tier: &TierStorage<FakeCold>,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Vec<u8> {
    let got = tier
        .get_object(bucket, key, None)
        .await
        .expect("the object is served");
    read_body(got.body).await.expect("the body reads")
}

#[tokio::test]
async fn a_sync_put_streams_and_does_not_buffer() {
    let fixture = Fixture::new(FlushMode::Sync, 16 * 1024).await;
    let tier = fixture.tier();
    let key = object::key("sync.bin").expect("a valid key");
    // Sixteen times the object cap: a whole-body buffer would be visible.
    let data = vec![b'x'; 256 * 1024];

    let staged = tier
        .stage_body(&fixture.bucket, &key, chunked(&data, 8 * 1024), None, None)
        .await
        .expect("the body stages");

    assert!(
        matches!(staged, TierStaged::Fs(_)),
        "a sync write streams through the filesystem stage"
    );
    let staged_files = fixture.staged_files();
    assert_eq!(
        staged_files.len(),
        1,
        "the whole body is on disk, not in a memory buffer"
    );
    assert_eq!(
        fs::metadata(&staged_files[0])
            .expect("the staged temp")
            .len(),
        data.len() as u64,
        "the temp holds the streamed body"
    );
    assert!(
        !fixture.path("sync.bin").exists(),
        "an uncommitted stage is invisible"
    );
    assert_eq!(
        tier.cache().len_bytes(),
        0,
        "no memory plane holds the body"
    );
    let err = tier
        .head_object(&fixture.bucket, &key)
        .await
        .expect_err("an uncommitted stage has no row");
    assert!(matches!(
        storage::Error::from(err),
        storage::Error::NoSuchKey(_)
    ));

    let info = tier
        .commit_object(
            &fixture.bucket,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the staged body commits");

    assert_eq!(info.size, data.len() as u64);
    assert_eq!(info.etag, ETag::from_content(&data));
    assert!(
        fixture.staged_files().is_empty(),
        "the commit consumes the temp"
    );
    assert_eq!(
        get_bytes(&tier, &fixture.bucket, &key).await,
        data,
        "the committed bytes are the streamed body"
    );
}

#[tokio::test]
async fn a_large_batch_put_spills_to_the_fs_stage() {
    let fixture = Fixture::new(FlushMode::Batch, 8 * 1024).await;
    let tier = fixture.tier();

    // At the cap a batch-eligible body stays in memory and writes no temp.
    let small_key = object::key("small.bin").expect("a valid key");
    let small = vec![b's'; 8 * 1024];
    let staged = tier
        .stage_body(
            &fixture.bucket,
            &small_key,
            body(small.as_slice()),
            None,
            None,
        )
        .await
        .expect("the small body stages");
    assert!(
        matches!(staged, TierStaged::Mem { .. }),
        "a body at the cap is buffered, not spilled"
    );
    assert!(
        fixture.staged_files().is_empty(),
        "a memory stage writes no temp"
    );
    drop(staged);

    // Past the cap the same batch stage spills to the filesystem stage.
    let key = object::key("large.bin").expect("a valid key");
    let data = vec![b'l'; 64 * 1024];
    let staged = tier
        .stage_body(&fixture.bucket, &key, chunked(&data, 4 * 1024), None, None)
        .await
        .expect("the large body stages");

    assert!(
        matches!(staged, TierStaged::Fs(_)),
        "past `max_object_bytes` the bounded buffer spills to the fs stage"
    );
    let staged_files = fixture.staged_files();
    assert_eq!(staged_files.len(), 1, "the spilled body has one temp");
    assert_eq!(
        fs::metadata(&staged_files[0])
            .expect("the staged temp")
            .len(),
        data.len() as u64
    );

    let info = tier
        .commit_object(
            &fixture.bucket,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the spilled body commits");
    assert_eq!(info.etag, ETag::from_content(&data));
    assert_eq!(get_bytes(&tier, &fixture.bucket, &key).await, data);
}

#[tokio::test]
async fn a_checksum_survives_the_mem_path() {
    let fixture = Fixture::new(FlushMode::Batch, 64 * 1024).await;
    let tier = fixture.tier();
    let key = object::key("checksum.bin").expect("a valid key");
    let data = b"a checksummed small body";
    let tee = checksum_tee(checksum::Algorithm::Crc32, "NhCmhg==");

    let staged = tier
        .stage_body(&fixture.bucket, &key, body(data), Some(tee), None)
        .await
        .expect("the body stages");
    assert!(
        matches!(staged, TierStaged::Mem { .. }),
        "the body is inside the cap: the memory path"
    );

    let info = tier
        .commit_object(
            &fixture.bucket,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the memory stage commits");

    assert_eq!(
        info.checksum,
        Some(checksum::Recorded {
            part: checksum::Part {
                algorithm: checksum::Algorithm::Crc32,
                value: checksum::Value("NhCmhg==".into()),
            },
            kind: checksum::Type::FullObject,
        }),
        "the tee's streamed digest rides the memory path into the commit"
    );
    let head = tier
        .head_object(&fixture.bucket, &key)
        .await
        .expect("the committed object is headed");
    assert_eq!(
        head.checksum, info.checksum,
        "the row records it atomically"
    );
    assert_eq!(get_bytes(&tier, &fixture.bucket, &key).await, data);
}

#[tokio::test]
async fn an_aborted_stage_leaves_nothing() {
    // The sync path stages a temp: dropping it must remove the temp and
    // leave no row.
    let fixture = Fixture::new(FlushMode::Sync, 16 * 1024).await;
    let tier = fixture.tier();
    let key = object::key("aborted.bin").expect("a valid key");
    let staged = tier
        .stage_body(
            &fixture.bucket,
            &key,
            chunked(b"aborted body", 4),
            None,
            None,
        )
        .await
        .expect("the body stages");
    assert!(matches!(staged, TierStaged::Fs(_)));
    assert_eq!(fixture.staged_files().len(), 1, "the temp exists");
    drop(staged);
    fixture.wait_for_staged_cleanup().await;

    assert!(
        !fixture.path("aborted.bin").exists(),
        "no object file survives an abort"
    );
    let err = tier
        .head_object(&fixture.bucket, &key)
        .await
        .expect_err("no row survives an abort");
    assert!(matches!(
        storage::Error::from(err),
        storage::Error::NoSuchKey(_)
    ));
    assert_eq!(tier.cache().len_bytes(), 0, "no memory entry survives");

    // The batch path stages in memory only: dropping it leaves nothing at
    // all, and no buffer entry exists yet (Task 11 owns the buffer).
    let batch = Fixture::new(FlushMode::Batch, 64 * 1024).await;
    let batch_tier = batch.tier();
    let mem_key = object::key("aborted-mem.bin").expect("a valid key");
    let staged = batch_tier
        .stage_body(
            &batch.bucket,
            &mem_key,
            body(b"never committed"),
            None,
            None,
        )
        .await
        .expect("the body stages");
    assert!(matches!(staged, TierStaged::Mem { .. }));
    drop(staged);
    batch.wait_for_staged_cleanup().await;

    assert!(
        !batch.path("aborted-mem.bin").exists(),
        "an uncommitted memory stage writes no file"
    );
    let err = batch_tier
        .head_object(&batch.bucket, &mem_key)
        .await
        .expect_err("an uncommitted memory stage has no row");
    assert!(matches!(
        storage::Error::from(err),
        storage::Error::NoSuchKey(_)
    ));
    assert_eq!(batch_tier.cache().len_bytes(), 0);
    assert!(
        batch.staged_files().is_empty(),
        "a memory stage never writes a temp"
    );
}

#[tokio::test]
async fn a_classed_put_is_never_batch_eligible() {
    let fixture = Fixture::new(FlushMode::Batch, 64 * 1024).await;
    let tier = fixture.tier();

    // A plain small body is batch-eligible.
    let plain_key = object::key("plain.bin").expect("a valid key");
    let plain = tier
        .stage_body(&fixture.bucket, &plain_key, body(b"plain"), None, None)
        .await
        .expect("the plain body stages");
    assert!(
        matches!(plain, TierStaged::Mem { .. }),
        "a plain small write is batch-eligible"
    );
    drop(plain);

    // The same body under a non-STANDARD class streams instead (T7).
    let key = object::key("classed.bin").expect("a valid key");
    let data = b"a classed body";
    let staged = tier
        .stage_body_with_class(
            &fixture.bucket,
            &key,
            body(data),
            None,
            None,
            Class::StandardIa,
        )
        .await
        .expect("the classed body stages");
    assert!(
        matches!(staged, TierStaged::Fs(_)),
        "a classed write is never batch-eligible"
    );
    assert_eq!(
        fixture.staged_files().len(),
        1,
        "the classed body is on the filesystem stage"
    );

    let info = tier
        .commit_object(
            &fixture.bucket,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the classed body commits");
    assert_eq!(info.etag, ETag::from_content(data));
    assert_eq!(get_bytes(&tier, &fixture.bucket, &key).await, data);
}
