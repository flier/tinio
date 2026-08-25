//! The in-memory database: table layout, shared helpers, and the
//! [`MemoryStorage`] core.
//!
//! All state lives in a redb database over the [`redb::InMemoryBackend`],
//! organized into six tables (buckets, objects, object_meta, uploads,
//! parts, part_meta). Every check-and-write sequence (e.g. `put_object` checking the
//! bucket before inserting) runs inside one redb **write transaction**:
//! transactions are atomic and serialized (redb is a single-writer
//! database), so `delete_bucket`'s empty-check + removal and a concurrent
//! `put_object`'s bucket-check + insert cannot interleave — there is no
//! TOCTOU window. Reads use read transactions with zero-copy `&str` /
//! `&[u8]` access; object bodies are copied out before the transaction ends
//! (streams are `'static` and cannot borrow the transaction guard).
//!
//! The `BucketOps` / `ObjectOps` / `MultipartOps` implementations live in
//! [`crate::bucket`], [`crate::object`], and [`crate::multipart`] and share
//! the items below.

use redb::backends::InMemoryBackend;
use redb::{Database, ReadableDatabase, ReadableTable, Table, TableDefinition};

use tinio_core::{Storage, bucket, object};

use crate::{
    Error,
    error::{database_storage, no_such_bucket, no_such_upload},
};

/// `name` → creation time (unix nanoseconds).
pub(crate) const BUCKETS: TableDefinition<&str, u64> = TableDefinition::new("buckets");
/// `bucket\0key` → object bytes.
pub(crate) const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
/// `bucket\0key` → `(etag wire form, size, last-modified unix nanos)`.
pub(crate) const OBJECT_META: TableDefinition<&str, (&str, u64, u64)> =
    TableDefinition::new("object_meta");
/// `bucket\0key\0upload_id` → initiated unix nanos.
///
/// Scans under the `bucket\0` prefix bound a listing to one bucket, and the
/// compound key makes the bucket/key identity check a single lookup.
pub(crate) const UPLOADS: TableDefinition<&str, u64> = TableDefinition::new("uploads");
/// `upload_id\0part_number` (zero-padded) → part bytes.
pub(crate) const PARTS: TableDefinition<&str, &[u8]> = TableDefinition::new("parts");
/// `upload_id\0part_number` → `(etag wire form, size, last-modified unix nanos)`.
pub(crate) const PART_META: TableDefinition<&str, (&str, u64, u64)> =
    TableDefinition::new("part_meta");

/// A minimal in-memory backend over redb's in-memory backend.
///
/// Buckets and objects live in the tables above; multipart parts are stored
/// alongside their metadata. Folder markers (`dir/`) count as bucket content
/// but are never objects. Nothing is persisted — the in-memory backend is
/// discarded with the instance.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, BucketOps};
/// use tinio_mem::MemoryStorage;
///
/// let storage = MemoryStorage::new().unwrap();
/// let bucket = bucket::name("data").unwrap();
/// tokio::runtime::Runtime::new()
///     .unwrap()
///     .block_on(storage.create_bucket(&bucket))
///     .unwrap();
/// ```
pub struct MemoryStorage {
    pub(crate) db: Database,
}

impl MemoryStorage {
    /// Create an empty in-memory backend.
    ///
    /// Fails only if the redb database cannot be created (programmer error —
    /// the in-memory backend cannot fail in practice).
    pub fn new() -> Result<Self, Error> {
        let db = Database::builder().create_with_backend(InMemoryBackend::new())?;
        {
            // Create all tables up front: read transactions refuse to open
            // a table that does not exist yet.
            let txn = db.begin_write()?;
            txn.open_table(BUCKETS)?;
            txn.open_table(OBJECTS)?;
            txn.open_table(OBJECT_META)?;
            txn.open_table(UPLOADS)?;
            txn.open_table(PARTS)?;
            txn.open_table(PART_META)?;
            txn.commit()?;
        }
        Ok(Self { db })
    }

    /// Fast-fail bucket existence check (own read transaction). Backend
    /// failures propagate — a real database fault is never misclassified
    /// as `NoSuchBucket`.
    pub(crate) fn has_bucket(&self, name: &bucket::Name) -> Result<bool, Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BUCKETS)?;
        Ok(table.get(name.as_ref().as_str())?.is_some())
    }
}

/// `bucket\0key` — `\0` is safe as a separator: object keys cannot contain
/// control characters (validated at [`tinio_core::object::key`]).
pub(crate) fn object_key(bucket: &str, key: &str) -> String {
    format!("{bucket}\0{key}")
}

/// `upload_id\0NNNNNNNNNN` — zero-padded so string order == part-number order.
pub(crate) fn part_key(upload_id: &str, part_number: u32) -> String {
    format!("{upload_id}\0{part_number:010}")
}

/// `bucket\0key\0upload_id` — the compound `UPLOADS` key; `\0` is safe as a
/// separator (object keys cannot contain control characters, and upload ids
/// are UUID v4 strings).
pub(crate) fn upload_key(bucket: &str, key: &str, upload_id: &str) -> String {
    format!("{}\0{upload_id}", object_key(bucket, key))
}

/// Parse the zero-padded part number out of a `PARTS` key (after the
/// `upload_id\0` prefix).
pub(crate) fn parse_part_number(rest: &str) -> Result<u32, Error> {
    Ok(rest.parse()?)
}

/// Collect the `PARTS` keys under `prefix` (used to remove them).
pub(crate) fn collect_part_keys(
    parts: &redb::Table<'_, &str, &[u8]>,
    prefix: &str,
) -> Result<Vec<String>, Error> {
    let mut iter = parts.range(prefix..)?;
    let mut out = Vec::new();
    loop {
        match iter.next() {
            Some(Ok((k, _))) => {
                if !k.value().starts_with(prefix) {
                    break;
                }
                out.push(k.value().to_string());
            }
            Some(Err(e)) => return Err(database_storage(e)),
            None => break,
        }
    }
    Ok(out)
}

/// Check that `bucket` exists (`NoSuchBucket` otherwise). Multipart
/// operations answer bucket existence first — the fs backend's
/// `ensure_bucket` precedes everything else, and NoParts/NoSuchUpload
/// must not mask a missing bucket.
pub(crate) fn check_bucket<T>(buckets: &T, name: &bucket::Name) -> Result<(), Error>
where
    T: ReadableTable<&'static str, u64>,
{
    if buckets.get(name.as_ref().as_str())?.is_none() {
        return Err(no_such_bucket(name));
    }
    Ok(())
}

/// Check that `upload_id` names an upload for exactly this bucket/key
/// (a mismatched identity is `NoSuchUpload`).
///
/// The caller must drop any `UPLOADS` handle before opening it again in the
/// same transaction (redb refuses a second `open_table` of the same table).
pub(crate) fn check_upload<T>(
    uploads: &T,
    upload_id: &str,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<(), Error>
where
    T: ReadableTable<&'static str, u64>,
{
    let ukey = upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id);
    if uploads.get(ukey.as_str())?.is_none() {
        return Err(no_such_upload(upload_id));
    }
    Ok(())
}

/// Remove all `PARTS` and `PART_META` keys under `prefix` (complete/abort).
pub(crate) fn remove_all_parts(
    parts: &mut Table<'_, &str, &[u8]>,
    meta: &mut Table<'_, &str, (&str, u64, u64)>,
    prefix: &str,
) -> Result<(), Error> {
    for key in collect_part_keys(parts, prefix)? {
        parts.remove(key.as_str())?;
        meta.remove(key.as_str())?;
    }
    Ok(())
}

impl Storage for MemoryStorage {
    type Error = Error;
}

#[cfg(test)]
mod tests {
    use tinio_core::{BucketOps, ListObjectsParams, MultipartOps, ObjectOps, bucket, object};
    use tinio_util::testing::{assert_conformance, assert_send_sync, body};

    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn conformance() {
        let storage = MemoryStorage::new().unwrap();
        assert_conformance(&storage).await;
    }

    #[tokio::test]
    async fn put_returns_rfc_md5_vector() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("data").unwrap();
        storage.create_bucket(&bucket).await.unwrap();
        let key = object::key("abc").unwrap();
        let put = storage
            .put_object(&bucket, &key, body(b"abc".to_vec()))
            .await
            .unwrap();
        assert_eq!(put.etag.as_str(), "900150983cd24fb0d6963f7d28e17f72");
        let empty = object::key("empty").unwrap();
        let put = storage
            .put_object(&bucket, &empty, body(b"".to_vec()))
            .await
            .unwrap();
        assert_eq!(put.etag.as_str(), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[tokio::test]
    async fn multipart_etag_is_served_on_subsequent_reads() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("data").unwrap();
        storage.create_bucket(&bucket).await.unwrap();
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        let p1 = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"abc".to_vec()),
            )
            .await
            .unwrap();
        let p2 = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                2.into(),
                body(b"def".to_vec()),
            )
            .await
            .unwrap();
        let completed = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[
                    tinio_core::CompletedPart {
                        part_number: p1.part_number,
                        etag: p1.etag.clone(),
                    },
                    tinio_core::CompletedPart {
                        part_number: p2.part_number,
                        etag: p2.etag.clone(),
                    },
                ],
            )
            .await
            .unwrap();
        let head = storage.head_object(&bucket, &key).await.unwrap();
        assert_eq!(head.etag, completed.etag);
        let get = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(get.info.etag, completed.etag);
        let page = storage
            .list_objects(ListObjectsParams {
                bucket: bucket.clone(),
                prefix: String::new(),
                delimiter: None,
                start_after: None,
                max_keys: 1000,
            })
            .await
            .unwrap();
        assert_eq!(page.objects[0].etag, completed.etag);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_put_and_delete_never_orphans() {
        // redb serializes write transactions: a delete_bucket that succeeds
        // cannot leave objects behind, and a put_object that succeeds
        // cannot target a deleted bucket. After the storm, every object
        // entry under the bucket prefix must have a live bucket.
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let bucket = bucket::name("race").unwrap();
        storage.create_bucket(&bucket).await.unwrap();
        let key = object::key("a.txt").unwrap();

        let mut handles = Vec::new();
        for i in 0..32 {
            let s = Arc::clone(&storage);
            let b = bucket.clone();
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                if i % 2 == 0 {
                    let _ = s.put_object(&b, &k, body(b"x".to_vec())).await;
                } else {
                    let _ = s.delete_bucket(&b).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let txn = storage.db.begin_read().unwrap();
        let objects = txn.open_table(OBJECTS).unwrap();
        let prefix = format!("{}\0", bucket.as_ref().as_str());
        let orphans: Vec<String> = objects
            .range(prefix.as_str()..)
            .unwrap()
            .take_while(|e| {
                e.as_ref()
                    .map(|(k, _)| k.value().starts_with(&prefix))
                    .unwrap_or(false)
            })
            .map(|e| e.unwrap().0.value().to_string())
            .collect();
        if !orphans.is_empty() {
            let buckets = txn.open_table(BUCKETS).unwrap();
            assert!(
                buckets.get(bucket.as_ref().as_str()).unwrap().is_some(),
                "objects {orphans:?} outlive their bucket"
            );
        }
    }

    #[test]
    fn storage_is_send_sync() {
        assert_send_sync::<MemoryStorage>();
    }
}
