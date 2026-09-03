//! The in-memory database: table layout, shared helpers, and the
//! [`MemoryStorage`] core.
//!
//! All state lives in a redb database over the [`redb::InMemoryBackend`],
//! organized into nine tables (buckets, objects, object_meta, uploads,
//! parts, part_meta, upload_checksums, part_checksums, object_parts). Every check-and-write sequence (e.g. `put_object` checking the
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

use std::{
    cell::Cell,
    ops::Bound,
    sync::atomic::{AtomicU64, Ordering},
};

use redb::{
    Database, ReadableDatabase, ReadableTable, Table, TableDefinition, backends::InMemoryBackend,
};

use crate::{
    _core::{Storage, bucket, object},
    Error,
    error::{database_storage, entity_too_large, no_such_bucket, no_such_upload},
};

/// The range start of a mem listing scan: the later of the key-prefix
/// band and the resume marker (exclusive-after) — a deep resume never
/// re-reads the rows before the marker. One home for the rule the
/// object, bucket, and multipart scans all apply (A2).
pub(crate) fn band_start<'a>(band: &'a str, marker: Option<&'a str>) -> Bound<&'a str> {
    match marker {
        Some(marker) if marker > band => Bound::Excluded(marker),
        _ => Bound::Included(band),
    }
}

/// `name` → `(creation time unix nanos, tags wire)` — the tags element is
/// empty when the bucket has none (spec 2026-08-31).
pub(crate) const BUCKETS: TableDefinition<&str, (u64, &str)> = TableDefinition::new("buckets");
/// `bucket\0key` → object bytes.
pub(crate) const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
/// `bucket\0key` → `(etag wire form, size, last-modified unix nanos, tags
/// wire, checksum wire)` — the tags element is the object's tag set, the
/// checksum element `<algorithm wire>:<base64 value>:<kind>` its recorded
/// checksum; both are empty strings when the object has none (spec
/// 2026-08-31). The kind is recorded at write time so read paths never
/// derive it.
pub(crate) const OBJECT_META: TableDefinition<&str, (&str, u64, u64, &str, &str)> =
    TableDefinition::new("object_meta");
/// `bucket\0key\0upload_id` → `(initiated unix nanos, tags wire)` — the
/// tags element is the create-time object tag set, applied to the
/// completed object.
///
/// Scans under the `bucket\0` prefix bound a listing to one bucket, and the
/// compound key makes the bucket/key identity check a single lookup.
pub(crate) const UPLOADS: TableDefinition<&str, (u64, &str)> = TableDefinition::new("uploads");
/// `upload_id\0part_number` (zero-padded) → part bytes.
pub(crate) const PARTS: TableDefinition<&str, &[u8]> = TableDefinition::new("parts");
/// `upload_id\0part_number` → `(etag wire form, size, last-modified unix nanos)`.
pub(crate) const PART_META: TableDefinition<&str, (&str, u64, u64)> =
    TableDefinition::new("part_meta");
/// `bucket\0key\0upload_id` → `(algorithm wire name, checksum-type wire
/// name or "")` — the upload's create-time checksum spec (spec
/// 2026-08-31). Same key shape as `UPLOADS`.
pub(crate) const UPLOAD_CHECKSUMS: TableDefinition<&str, (&str, &str)> =
    TableDefinition::new("upload_checksums");
/// `upload_id\0part_number` → `(algorithm wire name, base64 value)` —
/// one part's computed checksum (spec 2026-08-31). Same key shape as
/// `PARTS`/`PART_META`.
pub(crate) const PART_CHECKSUMS: TableDefinition<&str, (&str, &str)> =
    TableDefinition::new("part_checksums");
/// `bucket\0key\0part_number` (zero-padded) → `(size, algorithm wire
/// name or "", base64 checksum value or "")` — the completed object's
/// retained part list (spec 2026-08-31, GetObjectAttributes
/// ObjectParts): the parts the object was composed of at its last
/// multipart completion, in part order, with the stored per-part
/// checksums. `""` marks a part stored without a checksum.
pub(crate) const OBJECT_PARTS: TableDefinition<&str, (u64, &str, &str)> =
    TableDefinition::new("object_parts");

// --- the stored-element wire codecs (spec 2026-08-31) ---
//
// The mem rows hold the tags and recorded-checksum values as canonical
// wire strings, like the fs backend's rows. Rows are written by the API
// only; the read-side parses below self-heal a domain-invalid element
// (tampering) instead of failing the row — the same tolerance as the fs
// backend: tags decode via `object::Tags::parse_wire_limited` + the
// empty-set fallback, checksum elements via
// `checksum::Recorded::from_wire_opt`.

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
    pub(crate) db: Database,
    options: MemoryOptions,
    /// Current total stored bytes across `OBJECTS` and `PARTS` (updated
    /// only when a limit is configured).
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
            txn.open_table(UPLOAD_CHECKSUMS)?;
            txn.open_table(PART_CHECKSUMS)?;
            txn.open_table(OBJECT_PARTS)?;
            txn.commit()?;
        }
        Ok(Self {
            db,
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

    /// Roll back a [`Self::adjust_total`] delta after a failed commit.
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

/// `bucket\0key\0NNNNNNNNNN` — zero-padded so string order == part-number
/// order (mirrors [`part_key`]); `ok` is the key's `object_key`.
pub(crate) fn object_part_key(ok: &str, part_number: u32) -> String {
    format!("{ok}\0{part_number:010}")
}

/// The `OBJECT_PARTS` rows of one object key (`ok` = the key's
/// `object_key`), in part-number order — the shared scan of the part
/// listing and the rename migration (rows are returned owned — the redb
/// guards borrow the transaction).
pub(crate) fn collect_part_rows<T>(
    parts: &T,
    ok: &str,
) -> Result<Vec<(u32, u64, String, String)>, Error>
where
    T: redb::ReadableTable<&'static str, (u64, &'static str, &'static str)>,
{
    let prefix = format!("{ok} ");
    let mut range = parts.range(prefix.as_str()..)?;
    let mut rows = Vec::new();
    loop {
        match range.next() {
            Some(Ok((k, v))) => {
                if !k.value().starts_with(&prefix) {
                    break;
                }
                let part_number = parse_part_number(&k.value()[prefix.len()..])?;
                let (size, algorithm, value) = v.value();
                rows.push((part_number, size, algorithm.to_string(), value.to_string()));
            }
            Some(Err(e)) => return Err(database_storage(e)),
            None => break,
        }
    }
    Ok(rows)
}

/// Remove every `OBJECT_PARTS` row of one object key (`ok` = the key's
/// `object_key`) — an overwrite, completion, or delete must not leave a
/// completed object's stale parts behind (the new object has none).
/// Idempotent.
pub(crate) fn remove_object_parts(
    parts: &mut redb::Table<'_, &str, (u64, &str, &str)>,
    ok: &str,
) -> Result<(), Error> {
    let prefix = format!("{ok}\0");
    let mut keys = Vec::new();
    let mut iter = parts.range(prefix.as_str()..)?;
    loop {
        match iter.next() {
            Some(Ok((k, _))) => {
                if !k.value().starts_with(&prefix) {
                    break;
                }
                keys.push(k.value().to_string());
            }
            Some(Err(e)) => return Err(database_storage(e)),
            None => break,
        }
    }
    for key in keys {
        parts.remove(key.as_str())?;
    }
    Ok(())
}

/// Check that `bucket` exists (`NoSuchBucket` otherwise). Multipart
/// operations answer bucket existence first — the fs backend's
/// `ensure_bucket` precedes everything else, and NoParts/NoSuchUpload
/// must not mask a missing bucket.
pub(crate) fn check_bucket<T>(buckets: &T, name: &bucket::Name) -> Result<(), Error>
where
    T: ReadableTable<&'static str, (u64, &'static str)>,
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
    T: ReadableTable<&'static str, (u64, &'static str)>,
{
    let ukey = upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id);
    if uploads.get(ukey.as_str())?.is_none() {
        return Err(no_such_upload(upload_id));
    }
    Ok(())
}

/// Remove all `PARTS`, `PART_META`, and `PART_CHECKSUMS` keys under
/// `prefix` (complete/abort).
pub(crate) fn remove_all_parts(
    parts: &mut Table<'_, &str, &[u8]>,
    meta: &mut Table<'_, &str, (&str, u64, u64)>,
    checksums: &mut Table<'_, &str, (&str, &str)>,
    prefix: &str,
) -> Result<(), Error> {
    for key in collect_part_keys(parts, prefix)? {
        parts.remove(key.as_str())?;
        meta.remove(key.as_str())?;
        checksums.remove(key.as_str())?;
    }
    Ok(())
}

impl Storage for MemoryStorage {
    type Error = Error;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        _core::{BucketOps, ListObjectsParams, MultipartOps, ObjectOps, bucket, object},
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

    #[test]
    fn collect_part_keys_stops_at_a_non_prefix_key() {
        // A part-key scan is bounded by the `upload_id\0` prefix: a key
        // of another upload ends the scan, never crossing into it.
        let storage = MemoryStorage::new().unwrap();
        let txn = storage.db.begin_write().unwrap();
        {
            let mut parts = txn.open_table(PARTS).unwrap();
            parts
                .insert(part_key("u1", 1).as_str(), b"a".as_slice())
                .unwrap();
            parts
                .insert(part_key("u2", 1).as_str(), b"b".as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
        let txn = storage.db.begin_write().unwrap();
        let parts = txn.open_table(PARTS).unwrap();
        let keys = collect_part_keys(&parts, "u1\0").unwrap();
        assert_eq!(keys, [part_key("u1", 1)]);
    }
}
