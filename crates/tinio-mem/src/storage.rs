//! The in-memory database: table layout, shared helpers, and the
//! [`MemoryStorage`] core.
//!
//! All state lives in a redb database over the [`redb::InMemoryBackend`].
//! The seven common tables (buckets, object_meta, uploads, parts,
//! upload_checksums, part_checksums, object_parts) come from tinio-store —
//! row shapes, typed handles, and the scan/drain helpers are shared with
//! the fs backend — plus three mem-local content tables (objects,
//! part_data, part_meta) with no fs counterpart, keyed like the shared
//! tables. Every table is tuple-keyed: `(bucket, key)` for object rows,
//! `(bucket, upload_id[, part_number])` for the multipart rows. Every
//! check-and-write sequence (e.g. `put_object` checking the bucket before
//! inserting) runs inside one redb **write transaction**: transactions are
//! atomic and serialized (redb is a single-writer database), so
//! `delete_bucket`'s empty-check + removal and a concurrent `put_object`'s
//! bucket-check + insert cannot interleave — there is no TOCTOU window.
//! Reads use read transactions with zero-copy `&str` / `&[u8]` access;
//! object bodies are copied out before the transaction ends (streams are
//! `'static` and cannot borrow the transaction guard).
//!
//! The `BucketOps` / `ObjectOps` / `MultipartOps` implementations live in
//! [`crate::bucket`], [`crate::object`], and [`crate::multipart`] and share
//! the items below.

use std::{
    cell::Cell,
    sync::atomic::{AtomicU64, Ordering},
};

use redb::{Database, ReadableTable, backends::InMemoryBackend};

#[cfg(test)]
use crate::_core::bucket::name;
#[cfg(test)]
use crate::_store::scan::for_each_pair;
use crate::{
    _core::{Storage, bucket::Name, object},
    _store::{bucket, ensure_all, objects, part_data, part_meta, store, table::TableDef, upload},
    Error,
    error::{entity_too_large, no_such_bucket, no_such_upload},
};

/// Check that `bucket` exists (`NoSuchBucket` otherwise). Multipart
/// operations answer bucket existence first — the fs backend's
/// `ensure_bucket` precedes everything else, and NoParts/NoSuchUpload
/// must not mask a missing bucket.
pub(crate) fn check_bucket<T>(buckets: &bucket::Table<'_, T>, name: &Name) -> Result<(), Error>
where
    T: ReadableTable<<bucket::Def as TableDef>::Key, <bucket::Def as TableDef>::Value>,
{
    if !buckets.exists(name.as_ref().as_str())? {
        return Err(no_such_bucket(name));
    }
    Ok(())
}

/// Check that `upload_id` names an upload for exactly this bucket/key
/// (a mismatched identity is `NoSuchUpload`): a point get on the shared
/// `(bucket, upload_id)` key plus the stored-key comparison — the fs
/// backend's `key_matches` pattern.
pub(crate) fn check_upload<T>(
    uploads: &upload::Table<'_, T>,
    upload_id: &str,
    bucket: &Name,
    key: &object::Key,
) -> Result<(), Error>
where
    T: ReadableTable<<upload::Def as TableDef>::Key, <upload::Def as TableDef>::Value>,
{
    if !uploads.key_matches(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id)? {
        return Err(no_such_upload(upload_id));
    }
    Ok(())
}

/// A minimal in-memory backend over redb's in-memory backend.
///
/// Buckets and objects live in the tables above; multipart parts are stored
/// alongside their metadata. Folder markers (`dir/`) count as bucket content
/// but are never objects. Nothing is persisted — the in-memory backend is
/// discarded with the instance.
///
/// Resource limits are optional and configurable via [`MemoryOptions`]
/// ([`MemoryStorage::with_options`]): `max_object_bytes` caps one object or
/// part, `max_total_bytes` caps the sum of all object and part bytes. The
/// default (`MemoryStorage::new`) is **unlimited**, matching the project's
/// documented no-limit posture (CHK028); a server wiring the in-memory
/// backend should set explicit limits from its configuration. The total
/// accounting is a best-effort soft limit — concurrent writers may briefly
/// overshoot by the number of simultaneous writes.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{BucketOps, bucket};
/// use tinio_mem::{MemoryOptions, MemoryStorage};
/// use tokio::runtime::Runtime;
///
/// let storage = MemoryStorage::new().unwrap();
/// let bucket = bucket::name("data").unwrap();
/// Runtime::new()
///     .unwrap()
///     .block_on(storage.create_bucket(&bucket))
///     .unwrap();
///
/// let limited = MemoryStorage::with_options(MemoryOptions {
///     max_object_bytes: Some(1024),
///     max_total_bytes: Some(10 * 1024),
/// })
/// .unwrap();
/// ```
pub struct MemoryStorage {
    pub(crate) db: store::Handle,
    options: MemoryOptions,
    /// Current total stored bytes across `objects` and `part_data`
    /// (updated only when a limit is configured).
    total_bytes: AtomicU64,
}

/// Optional resource limits for [`MemoryStorage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemoryOptions {
    /// Maximum size of a single object or multipart part in bytes.
    /// `None` (default) = unlimited.
    pub max_object_bytes: Option<u64>,
    /// Maximum total stored bytes across all objects and parts.
    /// `None` (default) = unlimited.
    pub max_total_bytes: Option<u64>,
}

impl MemoryStorage {
    /// Create an empty in-memory backend with **no** resource limits
    /// (the project's documented default; see [`MemoryOptions`]).
    ///
    /// Fails only if the redb database cannot be created (programmer error —
    /// the in-memory backend cannot fail in practice).
    pub fn new() -> Result<Self, Error> {
        Self::with_options(MemoryOptions::default())
    }

    /// Create an empty in-memory backend with the given resource limits
    /// (see [`MemoryOptions`]).
    ///
    /// Fails only if the redb database cannot be created (programmer error —
    /// the in-memory backend cannot fail in practice).
    pub fn with_options(options: MemoryOptions) -> Result<Self, Error> {
        let handle = store::Handle::new(
            Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .map_err(|e| Error::Database(e.into()))?,
        );
        handle.write(|txn| -> Result<(), Error> {
            // Create all tables up front: read transactions refuse to open
            // a table that does not exist yet. The seven shared tables and
            // the three mem-local content tables land in one write
            // transaction (the fs backend's open pattern).
            ensure_all(txn)?;
            objects::Table::ensure(txn)?;
            part_data::Table::ensure(txn)?;
            part_meta::Table::ensure(txn)?;
            Ok(())
        })?;
        Ok(Self {
            db: handle,
            options,
            total_bytes: AtomicU64::new(0),
        })
    }

    /// Enforce `max_object_bytes` on a single object/part of `size` bytes.
    pub(crate) fn check_object_size(&self, size: u64) -> Result<(), Error> {
        if let Some(limit) = self.options.max_object_bytes
            && size > limit
        {
            return Err(entity_too_large(size, limit));
        }
        Ok(())
    }

    /// Apply a signed byte delta to the tracked total and enforce
    /// `max_total_bytes`. Call inside the write transaction before the
    /// commit; a failing limit leaves the total unchanged. No-op when no
    /// total limit is configured.
    pub(crate) fn adjust_total(&self, delta: i64) -> Result<(), Error> {
        let Some(limit) = self.options.max_total_bytes else {
            return Ok(());
        };
        // The projected size is carried out of the closure (T04): the
        // error must report the size the write WOULD have produced, not
        // the current total (`fetch_update`'s `Err` carries only the
        // latter, and it is racy under concurrent deltas).
        let projected = Cell::new(0u64);
        self.total_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                let new_total = total as i128 + delta as i128;
                if new_total < 0 {
                    // Defensive: internal accounting must never go negative.
                    return Some(0);
                }
                let new_total = new_total as u64;
                if new_total > limit {
                    projected.set(new_total);
                    return None;
                }
                Some(new_total)
            })
            .map_err(|_| entity_too_large(projected.get(), limit))?;
        Ok(())
    }

    /// Roll back a [`Self::adjust_total`] delta after a failed commit
    /// (tested against the direct-accounting surface; the commit-failure
    /// path is unreachable on the in-memory backend — no disk writes).
    #[cfg(test)]
    pub(crate) fn rollback_total(&self, delta: i64) {
        self.total_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(((total as i128 - delta as i128).max(0)) as u64)
            })
            .ok();
    }

    /// The currently tracked total bytes (objects + parts). Meaningful only
    /// when a total limit is configured; 0 otherwise. Test hook.
    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Fast-fail bucket existence check (own read transaction). Backend
    /// failures propagate — a real database fault is never misclassified
    /// as `NoSuchBucket`.
    pub(crate) fn has_bucket(&self, name: &Name) -> Result<bool, Error> {
        self.db.read(|txn| {
            let buckets = bucket::Table::open_readonly(txn)?;
            Ok(buckets.get(name.as_ref().as_str())?.is_some())
        })
    }
}

impl Storage for MemoryStorage {
    type Error = Error;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use redb::ReadableDatabase;

    use super::*;
    use crate::{
        _core::{BucketOps, ListObjectsParams, MultipartOps, ObjectOps, object},
        _util::testing::{assert_conformance, assert_send_sync, body},
    };

    #[tokio::test]
    async fn conformance() {
        let storage = MemoryStorage::new().unwrap();
        assert_conformance(&storage).await;
    }

    #[tokio::test]
    async fn put_returns_rfc_md5_vector() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = name("data").unwrap();
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
        let bucket = name("data").unwrap();
        storage.create_bucket(&bucket).await.unwrap();
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty())
            .await
            .unwrap();
        // The first part is non-final in the two-part list — it must be
        // >= the 5 MiB minimum the complete enforces in-txn.
        let min = crate::_core::multipart::MIN_PART_BYTES as usize;
        let p1 = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(vec![b'a'; min]),
                None,
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
                None,
            )
            .await
            .unwrap();
        let completed = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[
                    crate::_core::CompletedPart {
                        part_number: p1.part_number,
                        etag: p1.etag.clone(),
                    },
                    crate::_core::CompletedPart {
                        part_number: p2.part_number,
                        etag: p2.etag.clone(),
                    },
                ],
                None,
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
        let bucket = name("race").unwrap();
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

        let txn = storage.db.db().begin_read().unwrap();
        let objects = objects::Table::open_readonly(&txn).unwrap();
        let bucket_str = bucket.as_ref().as_str();
        // The tuple-keyed scan of one bucket: `(bucket, "")` lower bound,
        // keep boundary on the first element — the shared helper's
        // no-exclusive-upper-bound rule.
        let mut orphans: Vec<String> = Vec::new();
        for_each_pair(
            &*objects,
            (bucket_str, ""),
            |b, _| b == bucket_str,
            |_b, k, _| -> Result<(), Error> {
                orphans.push(k.to_string());
                Ok(())
            },
        )
        .unwrap();
        if !orphans.is_empty() {
            let buckets = bucket::Table::open_readonly(&txn).unwrap();
            assert!(
                buckets.get(bucket_str).unwrap().is_some(),
                "objects {orphans:?} outlive their bucket"
            );
        }
    }

    #[test]
    fn storage_is_send_sync() {
        assert_send_sync::<MemoryStorage>();
    }

    #[test]
    fn total_accounting_clamps_and_rolls_back() {
        // `adjust_total` must never let the tracked total go negative
        // (defensive), and `rollback_total` clamps at zero too — the
        // failed-commit recovery path.
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(8),
        })
        .unwrap();
        storage.adjust_total(4).unwrap();
        assert_eq!(storage.total_bytes(), 4);
        // A delta below zero clamps the total to 0 instead of going
        // negative.
        storage.adjust_total(-5).unwrap();
        assert_eq!(storage.total_bytes(), 0);
        // Rollback subtracts without going negative.
        storage.adjust_total(3).unwrap();
        storage.rollback_total(2);
        assert_eq!(storage.total_bytes(), 1);
        storage.rollback_total(10);
        assert_eq!(storage.total_bytes(), 0);
    }

    #[test]
    fn limit_breach_reports_the_projected_size() {
        // T04: the `EntityTooLarge` payload is the size the write WOULD
        // have produced (current 4 + delta 10), not the current total —
        // the old code reported 4 and was racy under concurrency.
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(8),
        })
        .unwrap();
        storage.adjust_total(4).unwrap();
        let err = storage.adjust_total(10).unwrap_err();
        let Error::Storage(crate::_core::storage::Error::EntityTooLarge { size, limit }) = err
        else {
            panic!("expected EntityTooLarge, got {err:?}");
        };
        assert_eq!((size, limit), (14, 8));
        // The failed delta left the total unchanged.
        assert_eq!(storage.total_bytes(), 4);
    }
}
