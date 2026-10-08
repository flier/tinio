//! The in-memory database: construction, the shared handles, and the
//! byte-accounting entry points.
//!
//! All state lives in a redb database over the [`redb::InMemoryBackend`].
//! The seven common tables (buckets, object_meta, uploads, parts,
//! upload_checksums, part_checksums, object_parts) come from tinio-store-redb —
//! row shapes, typed handles, and the scan/drain helpers are shared with
//! the fs backend. Beside them sit the byte tables [`RedbContent`] writes,
//! `objects` and `part_data`, and mem's part-stat plane `part_meta`, which
//! carries the size and mtime of a part (no contract row does). Every table
//! is tuple-keyed: `(bucket, key)` for object rows,
//! `(bucket, upload_id[, part_number])` for the multipart rows.
//!
//! The metadata access layer is the shared implementation's (design
//! 2026-10-03 §4): mem holds [`RedbMetadata`] behind an `Arc` and delegates
//! every row operation to the contracts, mapping the contract error onto
//! [`Error::Database`]. What stays mem-side is the byte plane's accounting —
//! [`CountingContent`] decorates the shared `RedbContent` and owns every
//! count of stored object and part bytes (round 29) — plus the two paths the
//! contracts cannot express: the versioned write/delete, which needs the
//! outgoing bytes and the history row in one transaction, and the part-stat
//! rows, which read and write straight through the [`store::Handle`].
//!
//! The `BucketOps` / `ObjectOps` / `MultipartOps` implementations live in
//! [`crate::bucket`], [`crate::object`], and [`crate::multipart`].

use std::sync::Arc;

use futures::lock::MutexGuard as AsyncMutexGuard;

#[cfg(test)]
use crate::_core::bucket::name;
#[cfg(test)]
use crate::_store_redb::{bucket, scan::for_each_pair};
use crate::{
    _core::{
        Storage,
        bucket::Name,
        content::{ContentKey, ObjectContent, PublishId},
        meta::BucketMeta,
    },
    _store_redb::{Driver, RedbContent, RedbMetadata, ensure_all, objects, part_meta, store},
    Error,
    counting::CountingContent,
    error::no_such_bucket,
};

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
/// default ([`MemoryStorage::new`]) is **unlimited**, matching the project's
/// documented no-limit posture (CHK028); a server wiring the in-memory
/// backend should set explicit limits from its configuration. The total
/// accounting is a best-effort soft limit — concurrent writers may briefly
/// overshoot by the number of simultaneous writes.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{BucketOps, acl, bucket};
/// use tinio_mem::{MemoryOptions, MemoryStorage};
/// use tokio::runtime::Runtime;
///
/// let storage = MemoryStorage::new().unwrap();
/// let bucket = bucket::name("data").unwrap();
/// Runtime::new()
///     .unwrap()
///     .block_on(storage.create_bucket(&bucket, None, &acl::Acl::default_private(None)))
///     .unwrap();
///
/// let limited = MemoryStorage::with_options(MemoryOptions {
///     max_object_bytes: Some(1024),
///     max_total_bytes: Some(10 * 1024),
/// })
/// .unwrap();
/// ```
pub struct MemoryStorage {
    /// The database: mem's own tables (the version history's blobs, and the
    /// unit tests' raw tampering) read and write through this handle. The
    /// shared metadata implementation runs on the same database.
    pub(crate) db: Arc<store::Handle>,
    /// The shared metadata implementation: every bucket, object, upload and
    /// version row mem serves goes through these contracts.
    pub(crate) metadata: Arc<RedbMetadata>,
    /// The byte plane behind the accounting decorator: object and part bytes
    /// are published through it, and every count passes through it. The
    /// configured limits and the shared total-byte counter live inside it
    /// (round 29: one accounting home).
    pub(crate) content: Arc<CountingContent<RedbContent>>,
}

/// Optional resource limits for [`MemoryStorage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemoryOptions {
    /// Maximum size of a single object or multipart part in bytes.
    /// `None` (default) = unlimited.
    pub max_object_bytes: Option<u64>,
    /// Maximum total stored bytes across all objects, parts and version
    /// history blobs. `None` (default) = unlimited.
    ///
    /// The accounting covers every byte the backend holds: the current
    /// objects and their parts, **and** the superseded version blobs a
    /// versioned bucket's history keeps in the mem-local `blobs` table
    /// (spec 2026-10-03) — a versioned bucket's history is bounded by this
    /// limit exactly as its live content is. A blob enters the total when its
    /// row is written (a content-addressed dedup hit adds nothing, its bytes
    /// are already counted) and leaves it when the row does: when the last
    /// version row naming that content is dropped — an explicit version
    /// delete, the one-null-row convergence's displacement — or when the
    /// bucket teardown drains the table. mem has no sweep, so a blob that no
    /// version row names any more is still released the moment its last
    /// naming row goes; the total is the bytes actually held, never a
    /// reference count that could drift above or below them.
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
        // One database serves the metadata rows and the byte plane: the
        // content handle wraps the metadata handle's own driver, and the
        // mem-local tables are created through that same database below.
        let metadata = Arc::new(RedbMetadata::in_memory()?);
        let driver = metadata.driver();
        let content = CountingContent::new(RedbContent::from_driver(Arc::clone(&driver)), options);
        // The same database, not a second one: mem's own tables (the version
        // blobs, the part-stat rows, the unit tests' raw tampering) must see
        // the metadata rows the contracts write.
        let handle = metadata.store_handle();
        handle.write(|txn| -> Result<(), Error> {
            // Create all tables up front: read transactions refuse to open
            // a table that does not exist yet. The seven shared tables and
            // the three mem-local content tables land in one write
            // transaction (the fs backend's open pattern).
            ensure_all(txn)?;
            objects::Table::ensure(txn)?;
            part_meta::Table::ensure(txn)?;
            #[cfg(feature = "versioning")]
            crate::version::blobs::Table::ensure(txn)?;
            Ok(())
        })?;
        Ok(Self {
            db: handle,
            metadata,
            content: Arc::new(content),
        })
    }

    /// The shared metadata implementation — the handle every row operation
    /// of this backend delegates to (test-visible).
    pub fn metadata(&self) -> &Arc<RedbMetadata> {
        &self.metadata
    }

    /// The byte plane behind the accounting decorator.
    pub fn content(&self) -> &Arc<CountingContent<RedbContent>> {
        &self.content
    }

    /// The redb driver the metadata and content handles share — the handle
    /// a test inspects transaction counts through.
    pub fn driver(&self) -> Arc<Driver> {
        self.metadata.driver()
    }

    /// Take mem's accounting lock: the critical section a counted mutation
    /// reads and writes in. The versioned single-transaction paths hold this
    /// guard across their own transaction and meter their raw writes with
    /// the `*_unlocked` entry points. The lock is **not** reentrant: a
    /// caller holding it must never call a `*_locked` method.
    #[cfg_attr(not(feature = "versioning"), allow(dead_code))]
    pub(crate) async fn lock_accounting(&self) -> AsyncMutexGuard<'_, ()> {
        self.content.lock_accounting().await
    }

    /// Apply a signed byte delta to the tracked total and enforce
    /// `max_total_bytes`
    /// ([`CountingContent::adjust_total_unlocked`]). Call with the
    /// accounting lock held, inside the write transaction before the commit;
    /// a failing limit leaves the total unchanged. No-op when no total limit
    /// is configured.
    #[cfg_attr(not(feature = "versioning"), allow(dead_code))]
    pub(crate) fn adjust_total_unlocked(&self, delta: i64) -> Result<(), Error> {
        self.content.adjust_total_unlocked(delta)
    }

    /// [`Self::adjust_total_unlocked`] with a `credit` the caller is about
    /// to release: the `max_total_bytes` check forgives those bytes while
    /// the reservation stays the exact delta, so the total never drifts
    /// ([[`CountingContent::adjust_total_with_credit_unlocked`]]).
    #[cfg_attr(not(feature = "versioning"), allow(dead_code))]
    pub(crate) fn adjust_total_with_credit_unlocked(
        &self,
        delta: i64,
        credit: u64,
    ) -> Result<(), Error> {
        self.content
            .adjust_total_with_credit_unlocked(delta, credit)
    }

    /// Apply a signed byte delta to the tracked total **without** the
    /// `max_total_bytes` check. The paths that only move or drop bytes the
    /// backend already holds — a compensation after a failed publish, a
    /// version delete (whose materialize can re-publish content onto the key
    /// path), a bucket teardown — must never fail for capacity: the limit
    /// governs what a client write may **add**, and a backend that refused a
    /// delete for space would be unusable. The delta is clamped at zero, so a
    /// compensation that would otherwise fail still lands; a delta that grows
    /// the total can leave it above the limit, which the next write's check
    /// then sees (the limit is best-effort, exactly as the type doc says).
    #[cfg_attr(not(feature = "versioning"), allow(dead_code))]
    pub(crate) fn release_total_unlocked(&self, delta: i64) {
        self.content.release_total_unlocked(delta);
    }

    /// [`Self::adjust_total_unlocked`] behind the accounting lock.
    #[cfg_attr(not(feature = "versioning"), allow(dead_code))]
    pub(crate) async fn adjust_total_locked(&self, delta: i64) -> Result<(), Error> {
        self.content.adjust_total_locked(delta).await
    }

    /// The single-object cap check (`max_object_bytes`) without the
    /// accounting lock — for a caller that already holds it. Never touches
    /// the total.
    #[cfg_attr(not(feature = "versioning"), allow(dead_code))]
    pub(crate) fn check_object_bytes_unlocked(&self, len: u64) -> Result<(), Error> {
        self.content.check_object_bytes_unlocked(len)
    }

    /// The currently tracked total bytes (objects and parts). Meaningful only
    /// when a total limit is configured; 0 otherwise. Test hook.
    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> u64 {
        self.content.total_bytes()
    }

    /// The drop half of a refused publish's compensation: take the bytes this
    /// call published back out — but only while the key still holds them
    /// *and* still holds this call's publish: the tag the publish recorded
    /// carries the identity the bytes cannot (two writers, one empty body).
    /// The comparison and the removal are the contract's one primitive
    /// ([`ObjectContent::remove_if_tagged`]), so a writer that published this
    /// key in the meantime keeps its bytes: a false answer means the key's
    /// content is that writer's, no row names ours any more, and the ledger
    /// entry the publish left is the later write's own length — so nothing is
    /// deleted and nothing is released. `row` names the refused row in the
    /// report.
    pub(crate) async fn roll_back_publish(
        &self,
        bucket: &Name,
        key: &ContentKey,
        published: &[u8],
        publish_id: PublishId,
        row: &str,
    ) -> Result<(), Error> {
        if !self
            .content()
            .remove_if_tagged(bucket, key, published, publish_id)
            .await?
        {
            tracing::info!(
                "a refused {row} row's published bytes were replaced by another writer; left \
                 in place (their bytes, their accounting entry)"
            );
        }
        Ok(())
    }

    /// The bucket guard every mem operation opens with: the bucket contract
    /// answers whether the row exists, and its absence becomes
    /// `NoSuchBucket`. This is the ordering half (`NoSuchBucket` answers before
    /// a key's own validation), never the authority — the guarded row writes
    /// re-check in their own transaction, and backend failures propagate, so a
    /// real database fault is never misclassified as `NoSuchBucket`.
    pub(crate) async fn ensure_bucket(&self, name: &Name) -> Result<(), Error> {
        if self.metadata().created_at(name).await?.is_some() {
            Ok(())
        } else {
            Err(no_such_bucket(name))
        }
    }
}

impl Storage for MemoryStorage {
    type Error = Error;
}

#[cfg(test)]
mod tests {
    use std::{panic, sync::Arc};

    use redb::ReadableDatabase;

    use super::*;
    #[cfg(feature = "versioning")]
    use crate::_util::testing::{conformance_versioning, unique_bucket};
    use crate::{
        _core::{
            BucketOps, CompletedPart, ListObjectsParams, MultipartOps, ObjectOps, acl::Acl,
            multipart::MIN_PART_BYTES, object, storage,
        },
        _util::testing::{assert_conformance, assert_send_sync, body},
    };

    #[tokio::test]
    async fn conformance() {
        let storage = MemoryStorage::new().unwrap();
        assert_conformance(&storage).await;
        #[cfg(feature = "versioning")]
        {
            // The versioning leg runs beside the suite on its own bucket: the
            // harness keeps the bucket it created to itself, and the leg
            // leaves its bucket versioned.
            let b = name(unique_bucket("conform-versioning")).unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            conformance_versioning(&storage, &b).await;
        }
    }

    #[tokio::test]
    async fn put_returns_rfc_md5_vector() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = name("data").unwrap();
        storage
            .create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .unwrap();
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
        storage
            .create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &bucket,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // The first part is non-final in the two-part list — it must be
        // >= the 5 MiB minimum the complete enforces in-txn.
        let min = MIN_PART_BYTES as usize;
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
                    CompletedPart {
                        part_number: p1.part_number,
                        etag: p1.etag.clone(),
                    },
                    CompletedPart {
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
        storage
            .create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .unwrap();
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

        let txn = storage.db.db().unwrap().begin_read().unwrap();
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

    /// A release that lands exactly on zero is the rollback path; a delta
    /// that would go *below* zero is a broken invariant, which
    /// [`a_total_underflow_is_loud_and_clamped`] covers.
    #[tokio::test]
    async fn total_accounting_releases_to_zero_and_rolls_back() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(8),
        })
        .unwrap();
        storage.adjust_total_locked(4).await.unwrap();
        assert_eq!(storage.total_bytes(), 4);
        // A rollback subtracts what it reserved, landing exactly on zero.
        storage.release_total_unlocked(-2);
        assert_eq!(storage.total_bytes(), 2);
        storage.release_total_unlocked(-2);
        assert_eq!(storage.total_bytes(), 0);
    }

    /// The clamp is defensive, not silent: a delta that would take the total
    /// below zero means another path moved the counter outside the
    /// accounting lock, so it is asserted in debug builds, reported in all
    /// of them, and still clamps.
    #[tokio::test]
    async fn a_total_underflow_is_loud_and_clamped() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(8),
        })
        .unwrap();
        storage.adjust_total_unlocked(4).unwrap();
        assert_eq!(storage.total_bytes(), 4);
        let underflow = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            storage.adjust_total_unlocked(-5).ok();
        }));
        assert_eq!(storage.total_bytes(), 0, "the clamp still lands");
        if cfg!(debug_assertions) {
            assert!(
                underflow.is_err(),
                "a debug build asserts the underflow instead of hiding it"
            );
        }
    }

    /// A credit widens the cap check but never the reservation, so the total
    /// stays exact across the write and the release that follows — the
    /// unversioned complete's pre-release, through the mem-level entry
    /// points.
    #[tokio::test]
    async fn the_credit_check_leaves_the_total_exact() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: Some(6),
            max_total_bytes: Some(10),
        })
        .unwrap();
        {
            // The single-transaction posture: hold the lock, meter with the
            // unlocked pair.
            let _section = storage.lock_accounting().await;
            storage.adjust_total_unlocked(6).unwrap();
            assert_eq!(storage.total_bytes(), 6);
            // Six more bytes cross the cap without a credit...
            let err = storage.adjust_total_unlocked(6).unwrap_err();
            assert!(matches!(
                err,
                Error::Storage(storage::Error::EntityTooLarge {
                    size: 12,
                    limit: 10
                })
            ));
            assert_eq!(storage.total_bytes(), 6, "a refusal moves nothing");
            // ... and land with the six bytes the caller is about to release.
            storage.adjust_total_with_credit_unlocked(6, 6).unwrap();
            assert_eq!(
                storage.total_bytes(),
                12,
                "the credit widens the check, never the reservation"
            );
            storage.release_total_unlocked(-6);
            assert_eq!(storage.total_bytes(), 6);
            // The object cap is checked without the total moving.
            storage.check_object_bytes_unlocked(6).unwrap();
            assert!(matches!(
                storage.check_object_bytes_unlocked(7).unwrap_err(),
                Error::Storage(storage::Error::EntityTooLarge { size: 7, limit: 6 })
            ));
            assert_eq!(storage.total_bytes(), 6);
        }
    }

    #[tokio::test]
    async fn limit_breach_reports_the_projected_size() {
        // T04: the `EntityTooLarge` payload is the size the write WOULD
        // have produced (current 4 + delta 10), not the current total —
        // the old code reported 4 and was racy under concurrency.
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(8),
        })
        .unwrap();
        storage.adjust_total_locked(4).await.unwrap();
        let err = storage.adjust_total_locked(10).await.unwrap_err();
        let Error::Storage(storage::Error::EntityTooLarge { size, limit }) = err else {
            panic!("expected EntityTooLarge, got {err:?}");
        };
        assert_eq!((size, limit), (14, 8));
        // The failed delta left the total unchanged.
        assert_eq!(storage.total_bytes(), 4);
    }
}
