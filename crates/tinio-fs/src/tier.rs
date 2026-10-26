//! The row-only tier seam ([`Rows`], reached through
//! `FsStorage::tier_rows`).
//!
//! The tier layer places object content outside the filesystem tree while
//! keeping the object row in the fs metadata store: a cold object is a row
//! with no file. This module is the one place the layer reads and restores
//! that row. It never opens, hashes or walks object content — [`Rows::scan`]
//! walks the key-ordered row table only, and the single file access is the
//! `stat` [`Rows::set_row`] takes for the recorded file identity when the
//! key has a file.
//!
//! `set_row` overwrites only the `OBJECT_META` row: the version slot recorded
//! on that row is carried over from the row being replaced, and the version
//! history, retained part rows and Object Lock rows live in other tables that
//! a row-only write never touches.

use std::{io::ErrorKind, ops::Bound, sync::Arc, time::SystemTime};

#[cfg(feature = "versioning")]
use crate::_core::version::State;
#[cfg(feature = "versioning")]
use crate::_store_redb::bucket_versioning;
use crate::{
    _core::{
        ETag, from_nanos, meta::object::PartRow, multipart::ObjectPart, object, storage, to_nanos,
    },
    _store_redb::{object_meta, object_part},
    Error, FsStorage,
    bucket::Name,
    database::Error as DatabaseError,
    fsutil,
};

/// The recorded identity of one object row, without its metadata elements —
/// the tier layer's reconcile input (a cold read compares a placement's ETag
/// with the row's).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// The recorded ETag.
    pub etag: ETag,
    /// The recorded size in bytes.
    pub size: u64,
    /// The recorded modification time.
    pub mtime: SystemTime,
}

/// The row-only handle over an fs backend's object metadata.
///
/// Cloning shares the same metadata store. Every read answers `None` (or an
/// empty page) for a key without a row, and a **delete marker's** row answers
/// `None` exactly as a listing skips it: a marker records an absent object,
/// not an object.
///
/// # Examples
///
/// ```rust
/// use std::{fs, sync::Arc};
///
/// use tinio_core::{
///     acl,
///     mode::AccessMode,
///     pipeline::InlineRunner,
///     storage::{
///         BucketOps, DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES,
///         DEFAULT_META_BATCH_SIZE, ObjectOps,
///     },
/// };
/// use tinio_fs::{FsOptions, FsStorage};
/// use tinio_util::testing::{body, bucket_name, object_key};
/// use tokio::runtime::Runtime;
///
/// let root = tempfile::tempdir().unwrap();
/// let options = FsOptions {
///     follow_symlinks: false,
///     state_dir: None,
///     access: AccessMode::ReadWrite,
///     compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///     meta_batch_size: DEFAULT_META_BATCH_SIZE,
///     meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
///     max_concurrent_uploads: tinio_core::storage::DEFAULT_MAX_CONCURRENT_UPLOADS,
///     owner_uids: std::collections::HashMap::new(),
///     io_pipeline: Arc::new(InlineRunner::default()),
///     remove_pipeline: Arc::new(InlineRunner::default()),
///     db_pipeline: Arc::new(InlineRunner::default()),
/// };
/// let storage = FsStorage::new(root.path(), options).unwrap();
/// Runtime::new().unwrap().block_on(async {
///     let b = bucket_name("data");
///     storage
///         .create_bucket(&b, None, &acl::Acl::default_private(None))
///         .await
///         .unwrap();
///     let k = object_key("cold.bin");
///     storage.put_object(&b, &k, body(b"hello")).await.unwrap();
///     // The tier layer's cold shape: content gone, row kept.
///     fs::remove_file(root.path().join("data/cold.bin")).unwrap();
///
///     let rows = storage.tier_rows();
///     let facts = rows.facts(&b, &k).await.unwrap().unwrap();
///     assert_eq!(facts.size, 5);
///     let info = rows.info(&b, &k).await.unwrap().unwrap();
///     assert_eq!(info.etag, facts.etag);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Rows {
    /// The backend whose metadata store and path mapping this handle uses.
    storage: Arc<FsStorage>,
}

impl Rows {
    /// A handle over `storage`'s metadata store (the construction
    /// `FsStorage::tier_rows` performs).
    pub(crate) fn new(storage: Arc<FsStorage>) -> Self {
        Self { storage }
    }

    /// The object row's metadata (`None` for a missing row or a current
    /// delete marker).
    pub async fn info(
        &self,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<object::Info>, Error> {
        self.live_row(bucket, key, |stored, txn| {
            Ok(Some(info_of(key, stored, records_versions(txn, bucket)?)))
        })
        .await
    }

    /// The object row's recorded identity (`None` for a missing row or a
    /// current delete marker).
    pub async fn facts(&self, bucket: &Name, key: &object::Key) -> Result<Option<Facts>, Error> {
        self.live_row(bucket, key, |stored, _| {
            Ok(Some(Facts {
                etag: stored.etag.clone(),
                size: stored.size,
                mtime: from_nanos(stored.mtime),
            }))
        })
        .await
    }

    /// The live row's mapped value — the read `info` and `facts` share:
    /// `None` for a missing row, and for a row whose current state is a
    /// delete marker (a marker records an absent object, not an object).
    async fn live_row<T>(
        &self,
        bucket: &Name,
        key: &object::Key,
        of: impl FnOnce(
            &object_meta::Stored,
            &redb::ReadTransaction,
        ) -> Result<Option<T>, DatabaseError>,
    ) -> Result<Option<T>, Error> {
        self.storage
            .handle()
            .read(|txn| {
                let Some(stored) = object_meta::Table::open_readonly(txn)?.get(bucket, key)? else {
                    return Ok(None);
                };
                if stored.is_delete_marker {
                    return Ok(None);
                }
                of(&stored, txn)
            })
            .map_err(Into::into)
    }

    /// Restore `info`'s metadata elements on `key`'s row, recording `mtime` as
    /// the row's modification time.
    ///
    /// The ETag, size, tags, checksum, owner and ACL come from `info`. The
    /// version slot recorded on the row being replaced — identity, sequence
    /// and the delete-marker flag — is carried over unchanged; a row created
    /// here takes `info`'s identity and no sequence. The file identity is
    /// taken from the file at the key when one exists, and `0` when the key
    /// has no file (a cold object) or the file is a directory marker. Retained
    /// part rows and Object Lock rows live in their own tables and are never
    /// touched.
    pub async fn set_row(
        &self,
        bucket: &Name,
        info: &object::Info,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        let key = info.key.clone();
        let file_identity = self.file_identity(bucket, &key).await?;
        let bucket = bucket.clone();
        let info = info.clone();
        self.storage
            .handle()
            .write(move |txn| {
                let (version_id, version_seq, is_delete_marker) = {
                    let table = object_meta::Table::open(txn)?;
                    match table.get(&bucket, &key)? {
                        Some(existing) => (
                            existing.version_id,
                            existing.version_seq,
                            existing.is_delete_marker,
                        ),
                        None => (info.version_id.clone(), 0, false),
                    }
                };
                object_meta::Table::open(txn)?.put(
                    &bucket,
                    &key,
                    &object_meta::Stored {
                        etag: info.etag.clone(),
                        size: info.size,
                        mtime: to_nanos(mtime),
                        file_identity,
                        tags: info.tags.clone(),
                        checksum: info.checksum.clone(),
                        owner: info.owner.clone(),
                        acl: info.acl.clone(),
                        version_id,
                        version_seq,
                        is_delete_marker,
                    },
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// The object's retained part rows, in part-number order (empty for an
    /// object that was never multipart-completed).
    pub async fn parts(&self, bucket: &Name, key: &object::Key) -> Result<Vec<ObjectPart>, Error> {
        let rows: Vec<object_part::Stored> = self.storage.handle().read(|txn| {
            object_part::Table::open_readonly(txn)?
                .list(bucket, key)
                .map_err(Into::into)
        })?;
        rows.into_iter()
            .map(|row| {
                ObjectPart::try_from(PartRow {
                    number: row.part_number,
                    size: row.size,
                    checksum: row.checksum,
                })
                .map_err(Error::from)
            })
            .collect()
    }

    /// The object rows of `bucket` in key order, starting at `prefix`, after
    /// `after` (exclusive) when given, at most `limit` rows.
    ///
    /// This is a row-table range scan: it never walks the filesystem, so a
    /// cold row (no file) is returned like any other. Current delete markers
    /// are skipped.
    pub async fn scan(
        &self,
        bucket: &Name,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<object::Info>, Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.storage
            .handle()
            .read(|txn| {
                let table = object_meta::Table::open_readonly(txn)?;
                let records = records_versions(txn, bucket)?;
                let start = match after {
                    Some(after) if after >= prefix => Bound::Excluded(after),
                    _ => Bound::Included(prefix),
                };
                let mut page = Vec::new();
                table.for_bucket_gated_from(bucket, start, |key, stored| {
                    if !key.as_ref().starts_with(prefix) {
                        return Ok(false);
                    }
                    if let Some(stored) = stored
                        && !stored.is_delete_marker
                    {
                        page.push(info_of(&key, &stored, records));
                        if page.len() >= limit {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })?;
                Ok(page)
            })
            .map_err(Into::into)
    }

    /// The recorded identity of the file at `key`, or `0` when the key has no
    /// file (a cold object, a missing bucket, or a directory marker).
    async fn file_identity(&self, bucket: &Name, key: &object::Key) -> Result<u64, Error> {
        let bucket_dir = match self.storage.bucket_dir(bucket).await {
            Ok(dir) => dir,
            Err(Error::Storage(storage::Error::NoSuchBucket(_))) => return Ok(0),
            Err(err) => return Err(err),
        };
        let path = match self.storage.resolve_key(&bucket_dir, key).await {
            Ok(path) => path,
            Err(Error::Storage(storage::Error::NoSuchKey(_))) => return Ok(0),
            Err(err) => return Err(err),
        };
        match fsutil::object_metadata(&path, *self.storage.follow_symlinks()).await {
            Ok(metadata) if metadata.is_file() => Ok(fsutil::file_identity(&path, &metadata)),
            Ok(_) => Ok(0),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(0),
            Err(err) => Err(err.into()),
        }
    }
}

/// Whether `bucket` records versions — the flag [`object::Info`] carries for
/// the version-header rule. The feature-off build answers `false`, exactly
/// as `FsStorage::records_versions` does (the state machinery is not compiled
/// in).
#[cfg(feature = "versioning")]
fn records_versions(txn: &redb::ReadTransaction, bucket: &Name) -> Result<bool, DatabaseError> {
    let state = bucket_versioning::Table::open_readonly(txn)?.get(bucket)?;
    Ok(state.is_some_and(State::records_versions))
}

/// See the feature-on arm: a stripped build answers no version state.
#[cfg(not(feature = "versioning"))]
fn records_versions(txn: &redb::ReadTransaction, bucket: &Name) -> Result<bool, DatabaseError> {
    let _ = (txn, bucket);
    Ok(false)
}

/// One row's [`object::Info`] from its recorded elements.
fn info_of(key: &object::Key, row: &object_meta::Stored, records: bool) -> object::Info {
    object::Info {
        key: key.clone(),
        size: row.size,
        last_modified: from_nanos(row.mtime),
        etag: row.etag.clone(),
        tags: row.tags.clone(),
        checksum: row.checksum.clone(),
        owner: row.owner.clone(),
        acl: row.acl.clone(),
        version_id: row.version_id.clone(),
        bucket_records_versions: records,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs as std_fs,
        path::PathBuf,
        time::{Duration, SystemTime},
    };

    use crate::{
        _core::{
            acl::Acl,
            checksum::{self, Algorithm, Part, Type, Value},
            multipart::{ObjectPart, part_number},
            object,
            object_lock::{LegalHold, Lock},
            storage::{BucketOps, ObjectOps},
            version,
        },
        _store_redb::{bucket_versioning, object_meta, object_part, version_object_lock},
        _util::testing::body,
        FsStorage, bucket, fsutil, testutil, tier,
    };

    const KEY: &str = "k.bin";

    async fn setup() -> (tempfile::TempDir, FsStorage, bucket::Name) {
        let (root, storage) = testutil::storage();
        let b = bucket::name("data").expect("a valid bucket name");
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .expect("bucket created");
        (root, storage, b)
    }

    async fn put(storage: &FsStorage, b: &bucket::Name, key: &str, data: &[u8]) -> object::Key {
        let k = object::key(key).expect("a valid object key");
        storage
            .put_object(b, &k, body(data))
            .await
            .expect("object put");
        k
    }

    fn object_path(root: &tempfile::TempDir, b: &bucket::Name, key: &str) -> PathBuf {
        root.path().join(b.as_ref()).join(key)
    }

    async fn read_row(
        storage: &FsStorage,
        b: &bucket::Name,
        k: &object::Key,
    ) -> object_meta::Stored {
        storage
            .handle()
            .read(|txn| Ok(object_meta::Table::open_readonly(txn)?.get(b, k)?))
            .expect("row read")
            .expect("row present")
    }

    fn keys(page: Vec<object::Info>) -> Vec<String> {
        page.into_iter()
            .map(|info| info.key.as_ref().to_owned())
            .collect()
    }

    #[tokio::test]
    async fn info_and_facts_read_without_content() {
        let (root, storage, b) = setup().await;
        let k = put(&storage, &b, KEY, b"hello").await;
        let rows: tier::Rows = storage.tier_rows();
        let before: tier::Facts = rows
            .facts(&b, &k)
            .await
            .expect("facts")
            .expect("a recorded row");

        let path = object_path(&root, &b, KEY);
        std_fs::remove_file(&path).expect("the object file removed");
        assert!(!path.exists(), "the row is read with no content");

        let info = rows
            .info(&b, &k)
            .await
            .expect("info")
            .expect("a recorded row");
        assert_eq!(info.key, k);
        assert_eq!(info.size, 5);
        assert_eq!(info.etag, before.etag);
        assert_eq!(info.last_modified, before.mtime);
        assert!(!info.bucket_records_versions);
        assert_eq!(rows.facts(&b, &k).await.expect("facts"), Some(before));

        let missing = object::key("missing.bin").expect("a valid key");
        assert_eq!(rows.info(&b, &missing).await.expect("info"), None);
        assert_eq!(rows.facts(&b, &missing).await.expect("facts"), None);
    }

    #[tokio::test]
    async fn set_row_restores_tags_acl_and_checksum() {
        let (root, storage, b) = setup().await;
        let k = put(&storage, &b, KEY, b"hello").await;
        let rows = storage.tier_rows();
        let owner = testutil::owner_id();
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let tags = object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())])
            .expect("a valid tag set");
        let checksum = checksum::Recorded {
            part: Part {
                algorithm: Algorithm::Crc32,
                value: Value("NhCmhg==".into()),
            },
            kind: Type::FullObject,
        };
        let acl = Acl::default_private(Some(owner.clone()));
        let info = object::Info {
            key: k.clone(),
            size: 5,
            last_modified: mtime,
            etag: "5d41402abc4b2a76b9719d911017c592"
                .parse()
                .expect("a valid etag"),
            tags: tags.clone(),
            checksum: Some(checksum.clone()),
            owner: Some(owner.clone()),
            acl: acl.clone(),
            version_id: None,
            bucket_records_versions: false,
        };
        rows.set_row(&b, &info, mtime)
            .await
            .expect("the row restored");

        let got = rows.info(&b, &k).await.expect("info").expect("a row");
        assert_eq!(got.size, 5);
        assert_eq!(got.etag, info.etag);
        assert_eq!(got.tags, tags);
        assert_eq!(got.checksum, Some(checksum));
        assert_eq!(got.owner, Some(owner));
        assert_eq!(got.acl.grants, acl.grants);
        assert_eq!(got.last_modified, mtime);

        let stored = read_row(&storage, &b, &k).await;
        let path = object_path(&root, &b, KEY);
        let metadata = std_fs::metadata(&path).expect("the object file");
        assert_eq!(
            stored.file_identity,
            fsutil::file_identity(&path, &metadata),
            "the identity is the file's when one exists"
        );
    }

    #[tokio::test]
    async fn set_row_preserves_parts_and_the_version_slot() {
        let (_root, storage, b) = setup().await;
        let k = put(&storage, &b, KEY, b"hello").await;
        let slot = version::Id::new("slot-1").expect("a valid version id");
        {
            let b = b.clone();
            let k = k.clone();
            let slot = slot.clone();
            storage
                .handle()
                .write(move |txn| {
                    {
                        let mut rows = object_meta::Table::open(txn)?;
                        let mut row = rows.get(&b, &k)?.expect("a recorded row");
                        row.version_id = Some(slot.clone());
                        row.version_seq = 7;
                        rows.put(&b, &k, &row)?;
                    }
                    {
                        let mut parts = object_part::Table::open(txn)?;
                        parts.put(
                            &b,
                            &k,
                            &object_part::Stored {
                                part_number: 1,
                                size: 3,
                                checksum: None,
                            },
                        )?;
                        parts.put(
                            &b,
                            &k,
                            &object_part::Stored {
                                part_number: 2,
                                size: 2,
                                checksum: None,
                            },
                        )?;
                    }
                    {
                        let lock = Lock {
                            retention: None,
                            legal_hold: LegalHold::On,
                        };
                        version_object_lock::Table::open(txn)?.put(
                            &b,
                            &k,
                            &version::wire_of(Some(&slot)),
                            &lock,
                        )?;
                    }
                    bucket_versioning::Table::open(txn)?.set(&b, version::State::Enabled)?;
                    Ok(())
                })
                .await
                .expect("the slot, parts and lock planted");
        }
        let rows = storage.tier_rows();
        let parts_before = rows.parts(&b, &k).await.expect("parts");
        assert_eq!(
            parts_before,
            vec![
                ObjectPart {
                    part_number: part_number(1).expect("a valid part number"),
                    size: 3,
                    checksum: None,
                },
                ObjectPart {
                    part_number: part_number(2).expect("a valid part number"),
                    size: 2,
                    checksum: None,
                },
            ]
        );
        let planted = rows.info(&b, &k).await.expect("info").expect("a row");
        assert_eq!(planted.version_id, Some(slot.clone()));
        assert!(planted.bucket_records_versions);

        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
        let info = object::Info {
            // A different identity: the row's own slot must win.
            version_id: version::Id::new("other").ok(),
            key: k.clone(),
            size: 9,
            last_modified: mtime,
            etag: "9e107d9d372bb6826bd81d3542a419d6"
                .parse()
                .expect("a valid etag"),
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            bucket_records_versions: false,
        };
        rows.set_row(&b, &info, mtime)
            .await
            .expect("the row restored");

        let stored = read_row(&storage, &b, &k).await;
        assert_eq!(stored.version_id, Some(slot.clone()), "the version slot");
        assert_eq!(stored.version_seq, 7, "the version sequence");
        assert!(!stored.is_delete_marker);
        assert_eq!(stored.etag, info.etag);
        assert_eq!(stored.size, 9);
        assert_eq!(rows.parts(&b, &k).await.expect("parts"), parts_before);
        let lock = storage
            .handle()
            .read(|txn| {
                Ok(version_object_lock::Table::open_readonly(txn)?.get(
                    &b,
                    &k,
                    &version::wire_of(Some(&slot)),
                )?)
            })
            .expect("lock read")
            .expect("the lock row");
        assert_eq!(lock.legal_hold, LegalHold::On, "the lock row");
    }

    /// A row restored for a key with no file — the cold shape — records no
    /// file identity while every metadata element comes from `info`, and the
    /// version slot and the retained part rows are left untouched.
    #[tokio::test]
    async fn set_row_without_a_file_restores_metadata_and_keeps_the_slot_and_parts() {
        let (root, storage, b) = setup().await;
        let k = put(&storage, &b, KEY, b"hello").await;
        let slot = version::Id::new("cold-slot").expect("a valid version id");
        {
            let b = b.clone();
            let k = k.clone();
            let slot = slot.clone();
            storage
                .handle()
                .write(move |txn| {
                    {
                        let mut rows = object_meta::Table::open(txn)?;
                        let mut row = rows.get(&b, &k)?.expect("a recorded row");
                        row.version_id = Some(slot);
                        row.version_seq = 4;
                        rows.put(&b, &k, &row)?;
                    }
                    object_part::Table::open(txn)?.put(
                        &b,
                        &k,
                        &object_part::Stored {
                            part_number: 1,
                            size: 3,
                            checksum: None,
                        },
                    )?;
                    Ok(())
                })
                .await
                .expect("the version slot and part row are planted");
        }
        std_fs::remove_file(object_path(&root, &b, KEY)).expect("the object file is removed");

        let rows = storage.tier_rows();
        let parts_before = rows.parts(&b, &k).await.expect("parts");
        let owner = testutil::owner_id();
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_123);
        let tags = object::Tags::from_pairs([("tier".to_owned(), "cold".to_owned())])
            .expect("a valid tag set");
        let checksum = checksum::Recorded {
            part: Part {
                algorithm: Algorithm::Crc32,
                value: Value("NhCmhg==".into()),
            },
            kind: Type::FullObject,
        };
        let acl = Acl::default_private(Some(owner.clone()));
        let info = object::Info {
            key: k.clone(),
            size: 11,
            last_modified: mtime,
            etag: "9e107d9d372bb6826bd81d3542a419d6"
                .parse()
                .expect("a valid etag"),
            tags: tags.clone(),
            checksum: Some(checksum.clone()),
            owner: Some(owner.clone()),
            acl: acl.clone(),
            version_id: version::Id::new("incoming").ok(),
            bucket_records_versions: false,
        };
        rows.set_row(&b, &info, mtime)
            .await
            .expect("the row is restored without a file");

        let stored = read_row(&storage, &b, &k).await;
        assert_eq!(
            stored.file_identity, 0,
            "a key with no file records no file identity"
        );
        assert_eq!(stored.etag, info.etag.clone());
        assert_eq!(stored.size, info.size);
        assert_eq!(stored.tags, tags.clone());
        assert_eq!(stored.checksum, Some(checksum.clone()));
        assert_eq!(stored.owner, Some(owner.clone()));
        assert_eq!(stored.acl.grants, acl.grants);
        assert_eq!(
            stored.version_id,
            Some(slot.clone()),
            "the row's version slot wins over the incoming identity"
        );
        assert_eq!(stored.version_seq, 4, "the version sequence survives");
        assert!(!stored.is_delete_marker);
        assert_eq!(
            rows.parts(&b, &k).await.expect("parts"),
            parts_before,
            "the retained part rows survive a row-only restore"
        );

        let got = rows.info(&b, &k).await.expect("info").expect("a row");
        assert_eq!(got.etag, info.etag);
        assert_eq!(got.size, info.size);
        assert_eq!(got.last_modified, mtime);
        assert_eq!(got.tags, tags);
        assert_eq!(got.checksum, Some(checksum));
        assert_eq!(got.owner, Some(owner));
        assert_eq!(got.version_id, Some(slot));
        assert!(!got.bucket_records_versions);
    }

    #[tokio::test]
    async fn scan_is_prefix_after_limit_bounded() {
        let (root, storage, b) = setup().await;
        for key in ["p/a", "p/b", "p/c", "q/a"] {
            put(&storage, &b, key, b"x").await;
            std_fs::remove_file(object_path(&root, &b, key)).expect("the object file removed");
        }
        let rows = storage.tier_rows();
        assert_eq!(
            keys(rows.scan(&b, "p/", None, 2).await.expect("scan")),
            ["p/a", "p/b"]
        );
        assert_eq!(
            keys(rows.scan(&b, "p/", Some("p/a"), 2).await.expect("scan")),
            ["p/b", "p/c"]
        );
        assert!(
            rows.scan(&b, "p/", Some("p/c"), 2)
                .await
                .expect("scan")
                .is_empty()
        );
        assert_eq!(
            keys(rows.scan(&b, "p/", None, 10).await.expect("scan")),
            ["p/a", "p/b", "p/c"]
        );
        assert!(
            rows.scan(&b, "p/", None, 0).await.expect("scan").is_empty(),
            "a zero limit is an empty page"
        );
        assert_eq!(
            keys(rows.scan(&b, "q/", Some("p/zzz"), 5).await.expect("scan")),
            ["q/a"],
            "an after below the prefix starts at the prefix"
        );
        assert_eq!(
            keys(rows.scan(&b, "", None, 10).await.expect("scan")),
            ["p/a", "p/b", "p/c", "q/a"]
        );
    }

    #[tokio::test]
    async fn scan_skips_delete_markers() {
        let (_root, storage, b) = setup().await;
        let live = put(&storage, &b, "live.txt", b"x").await;
        let marker = put(&storage, &b, "marker.txt", b"y").await;
        {
            let b = b.clone();
            let marker = marker.clone();
            storage
                .handle()
                .write(move |txn| {
                    let mut rows = object_meta::Table::open(txn)?;
                    let mut row = rows.get(&b, &marker)?.expect("a recorded row");
                    row.is_delete_marker = true;
                    rows.put(&b, &marker, &row)?;
                    Ok(())
                })
                .await
                .expect("the marker planted");
        }
        let rows = storage.tier_rows();
        assert_eq!(
            keys(rows.scan(&b, "", None, 10).await.expect("scan")),
            ["live.txt"]
        );
        assert_eq!(rows.info(&b, &marker).await.expect("info"), None);
        assert_eq!(rows.facts(&b, &marker).await.expect("facts"), None);
        assert!(rows.info(&b, &live).await.expect("info").is_some());
    }

    #[tokio::test]
    async fn a_delete_marker_row_round_trips() {
        let (_root, storage, b) = setup().await;
        let k = put(&storage, &b, KEY, b"hello").await;
        let slot = version::Id::new("slot-2").expect("a valid version id");
        {
            let b = b.clone();
            let k = k.clone();
            let slot = slot.clone();
            storage
                .handle()
                .write(move |txn| {
                    let mut rows = object_meta::Table::open(txn)?;
                    let mut row = rows.get(&b, &k)?.expect("a recorded row");
                    row.is_delete_marker = true;
                    row.version_id = Some(slot);
                    row.version_seq = 3;
                    rows.put(&b, &k, &row)?;
                    Ok(())
                })
                .await
                .expect("the marker planted");
        }
        let rows = storage.tier_rows();
        assert_eq!(
            rows.info(&b, &k).await.expect("info"),
            None,
            "a delete marker is not an object"
        );
        assert_eq!(rows.facts(&b, &k).await.expect("facts"), None);

        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
        let info = object::Info {
            key: k.clone(),
            size: 5,
            last_modified: mtime,
            etag: "9e107d9d372bb6826bd81d3542a419d6"
                .parse()
                .expect("a valid etag"),
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            version_id: None,
            bucket_records_versions: false,
        };
        rows.set_row(&b, &info, mtime)
            .await
            .expect("the row restored");

        let stored = read_row(&storage, &b, &k).await;
        assert!(
            stored.is_delete_marker,
            "the recorded marker survives a metadata restore"
        );
        assert_eq!(stored.version_id, Some(slot));
        assert_eq!(stored.version_seq, 3);
        assert_eq!(stored.etag, info.etag);
        assert_eq!(stored.size, 5);
        assert_eq!(rows.info(&b, &k).await.expect("info"), None);
        assert!(
            rows.scan(&b, "", None, 10).await.expect("scan").is_empty(),
            "a marker is never listed"
        );
    }
}
