//! Multipart upload storage (task T044, migrated to redb per
//! meta-redb-spec).
//!
//! Upload records live in the `UPLOADS` table (`(bucket, upload_id)` →
//! `(key, initiated_at)`) and part ETags in `PARTS` (`(bucket, upload_id,
//! part_number)` → etag hex) of `<state-dir>/meta.redb`. The file system
//! keeps only the part contents at `<state-dir>/multipart/<bucket>/<upload_id>/part-<n>`
//! (the upload directory is created by the first `put_part`, never by
//! `create` — the orphan-cleanup TOCTOU order depends on it, §5.7).
//! Assembly streams all parts into a temp file, then renames atomically
//! onto the object path; the composed ETag `MD5-of-MD5s-N` matches the AWS
//! reference composition. Parts survive restarts, so cross-restart
//! completion/abort is legal (quickstart §7). No 5 MB minimum is enforced
//! (FR-014).
//!
//! Redb transactions replace the old `upload.json` + `.etag` sidecar writes
//! under an in-process lock: `complete`'s consume is one atomic
//! UPLOADS+PARTS deletion; `list_parts` is DB-driven (a part whose PARTS
//! record never committed is invisible — the client retransmits, and the
//! point query recomputes from the file as a fallback, §5.6).

use std::{
    collections::{HashMap, HashSet},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use md5::{Digest, Md5};

use tinio_core::{
    BodyStream, ETag, bucket, from_nanos,
    multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber},
    object::{self},
    storage::{self},
};
use tinio_util::lockmap;

use crate::{
    Error, database,
    fsutil::ok_if_missing,
    path::MULTIPART_DIR_NAME,
    write::{AtomicWriter, CHUNK_SIZE, md5_of_file},
};

/// The on-disk path of a part file: `<upload-dir>/part-<n>`.
fn part_path(dir: &Path, n: u32) -> PathBuf {
    dir.join(format!("part-{n}"))
}

/// The per-upload part-write lock map: `(bucket, upload_id)` → the
/// [`Store::part_lock`] mutex slot.
type PartLocks = lockmap::Map<(String, String)>;

/// Delete one upload's `UPLOADS` + `PARTS` rows in the caller's write
/// transaction (`complete` consume, `abort`, and the backend's
/// `complete_object_state` share this).
pub(crate) fn drain_upload(
    txn: &mut redb::WriteTransaction,
    bucket: &bucket::Name,
    upload_id: &str,
) -> Result<(), database::Error> {
    // The UPLOADS half is one exact key (idempotent remove — no range
    // scan); only the PARTS half needs the range.
    database::UploadsTable::open(txn)?.remove(bucket, upload_id)?;
    database::PartsTable::open(txn)?.drain_upload(bucket, upload_id)?;
    Ok(())
}

/// Delete every `UPLOADS` + `PARTS` row of a bucket in the caller's write
/// transaction (bucket teardown — `remove_bucket_state`; the directory is
/// removed by the caller).
pub(crate) fn drain_bucket_uploads(
    txn: &mut redb::WriteTransaction,
    bucket: &bucket::Name,
) -> Result<(), database::Error> {
    database::UploadsTable::open(txn)?.drain_bucket(bucket)?;
    database::PartsTable::open(txn)?.drain_bucket(bucket)?;
    Ok(())
}

/// Sort uploads by the composite `(key, upload_id)` order the pagination
/// engine requires.
fn sort_uploads(uploads: &mut [MultipartUpload]) {
    uploads.sort_by(|a, b| {
        (a.key.as_ref(), a.upload_id.as_str()).cmp(&(b.key.as_ref(), b.upload_id.as_str()))
    });
}

/// Build a `MultipartUpload` from a stored `UPLOADS` row. Domain-invalid
/// rows are skipped (`None` — self-healing; `list_uploads`/`walk_uploads`
/// share this conversion).
fn upload_from_row(
    bucket: &bucket::Name,
    upload_id: &str,
    key: &str,
    initiated_at: u64,
) -> Option<MultipartUpload> {
    let key = object::key(key).ok()?;
    if uuid::Uuid::parse_str(upload_id).is_err() {
        return None;
    }
    Some(MultipartUpload {
        upload_id: upload_id.to_owned(),
        bucket: bucket.clone(),
        key,
        initiated_at: from_nanos(initiated_at),
    })
}

/// Multipart parts storage of a state dir.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{
///     bucket,
///     multipart::{CompletedPart, part_number},
///     object,
/// };
/// use tinio_fs::multipart;
/// use tinio_util::testing::body;
///
/// let state = tempfile::tempdir().unwrap();
/// let store = multipart::store(state.path()).unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let key = object::key("big.bin").unwrap();
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     let upload = store.create(&bucket, &key).await.unwrap();
///     let part = store
///         .put_part(&bucket, &key, &upload.upload_id, part_number(1).unwrap(), body(b"abc"))
///         .await
///         .unwrap();
///     assert_eq!(u32::from(part.part_number), 1);
///     let target = state.path().join("assembled.bin");
///     let completed = CompletedPart {
///         part_number: part.part_number,
///         etag: part.etag,
///     };
///     let (temp, _etag) = store
///         .complete(&bucket, &key, &upload.upload_id, &[completed])
///         .await
///         .unwrap();
///     tokio::fs::rename(&temp, &target).await.unwrap();
///     assert_eq!(tokio::fs::metadata(&target).await.unwrap().len(), 3);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Store {
    /// The shared state-database handle (upload records + part ETags).
    handle: Arc<database::Handle>,
    /// `<state-dir>/multipart/` — part content files only.
    root: PathBuf,
    /// Atomic writer (staging under `<state-dir>/tmp/`).
    writer: AtomicWriter,
    /// Serializes the rename + record of one upload's part files (see
    /// [`Store::part_lock`]) — the map slot is evicted when the last lock
    /// handle drops, so the table stays bounded by the number of
    /// concurrently locked uploads.
    part_locks: PartLocks,
}

/// The held per-upload part-write lock ([`Store::part_lock`]).
type PartLock = lockmap::Guard<(String, String)>;

impl Store {
    /// Create a store over a shared state-database handle (the `FsStorage`
    /// construction path — one handle across all stores).
    pub(crate) fn from_handle(handle: Arc<database::Handle>, state_dir: &Path) -> Self {
        Self {
            handle,
            root: state_dir.join(MULTIPART_DIR_NAME),
            writer: AtomicWriter::new(state_dir),
            part_locks: PartLocks::new(),
        }
    }

    /// The per-upload part-write lock: held across `put_part`'s rename and
    /// record transaction, so two concurrent same-part overwrites can
    /// never leave the file and the record disagreeing (the last rename
    /// and the last record must describe the same content — `complete`
    /// verifies the record, then re-hashes the file).
    async fn part_lock(&self, bucket: &bucket::Name, upload_id: &str) -> PartLock {
        self.part_locks
            .lock((bucket.to_string(), upload_id.to_string()))
            .await
    }

    /// Start a multipart upload (fresh UUID v4 id; the `UPLOADS` record is
    /// committed before the store answers — the upload directory is only
    /// created by the first `put_part`). The bucket must already exist —
    /// callers check `head_bucket` first.
    pub async fn create(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<MultipartUpload, Error> {
        // Fresh UUID v4 (122 random bits): collisions cannot happen in
        // practice, so a failed insert is a real I/O error.
        let upload = MultipartUpload {
            upload_id: uuid::Uuid::new_v4().to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: SystemTime::now(),
        };
        self.handle
            .write(|txn| {
                database::UploadsTable::open(txn)?.put(
                    bucket,
                    &upload.upload_id,
                    key,
                    upload.initiated_at,
                )
            })
            .map_err(Error::from)?;
        Ok(upload)
    }

    /// Stream one part (number `1..=10000`) into the upload. `NoSuchUpload`
    /// when the upload does not exist.
    pub async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
    ) -> Result<PartInfo, Error> {
        let n = u32::from(part_number);
        // Refuse a nonexistent upload BEFORE any disk write: the body is
        // not streamed and no directory or part file is created (an
        // UploadPart for an aborted/completed upload must not leave
        // residue). The check repeats in the record transaction below —
        // the upload can be consumed while the body streams.
        let dir = self.require_upload(bucket, key, upload_id).await?;
        // Stream the part body first — a slow client must not stall any
        // other multipart operation. The temp+rename happens after the
        // existence check, so a part file only ever becomes visible whole.
        let (temp, etag) = match self.writer.stage(body).await {
            Ok(staged) => staged,
            Err(err) => {
                // An abort may have removed the upload mid-stream.
                if self.require_upload(bucket, key, upload_id).await.is_err() {
                    return Err(storage::no_such_upload(upload_id).into());
                }
                return Err(err);
            }
        };
        // The rename and the PARTS upsert are one critical section (the
        // per-upload lock): a concurrent same-part overwrite must never
        // interleave as rename(A), rename(B), txn(B), txn(A) — the file
        // and the record would disagree and wedge the upload.
        let _lock = self.part_lock(bucket, upload_id).await;
        if let Err(err) = tokio::fs::create_dir_all(&dir).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err.into());
        }
        let part = part_path(&dir, n);
        if let Err(err) = tokio::fs::rename(&temp, &part).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err.into());
        }
        // One write transaction: the upload-existence check AND the PARTS
        // upsert (no separate read transaction). An upload aborted while
        // the body streamed answers NoSuchUpload and the renamed part is
        // discarded (the empty directory is reclaimed by the orphan
        // stage).
        let recorded = match self.handle.write(|txn| {
            let uploads = database::UploadsTable::open(txn)?;
            if !uploads.key_matches(bucket, key, upload_id)? {
                return Ok(false);
            }
            drop(uploads);
            database::PartsTable::open(txn)?.put(bucket, upload_id, n, &etag)?;
            Ok(true)
        }) {
            Ok(recorded) => recorded,
            Err(err) => {
                // The upload is gone (or a real DB failure) — remove the
                // renamed part and the empty directory (best-effort; a
                // racing abort already removed the dir).
                let _ = tokio::fs::remove_file(&part).await;
                let _ = tokio::fs::remove_dir(&dir).await;
                return Err(Error::from(err));
            }
        };
        if !recorded {
            let _ = tokio::fs::remove_file(&part).await;
            let _ = tokio::fs::remove_dir(&dir).await;
            return Err(storage::no_such_upload(upload_id).into());
        }
        let metadata = tokio::fs::metadata(&part).await?;
        Ok(PartInfo {
            part_number,
            size: metadata.len(),
            etag,
            last_modified: metadata.modified()?,
        })
    }

    /// List the parts of an upload, in part-number order, paginated
    /// inside the read transaction (raw `PARTS` rows from the exclusive
    /// marker, capped at `max_parts` — the same order the shared engine
    /// uses, at O(page) rows per request).
    ///
    /// DB-driven: only parts with a committed `PARTS` record appear (a part
    /// whose record never committed is invisible — the client retransmits,
    /// §5.6); size/mtime come from the part files. `max_parts = 0` returns
    /// an empty, untruncated page (an exclusive-after marker would skip
    /// the first part of the next page forever). Returns the page, whether
    /// more parts follow, and the **raw** last part number of the page —
    /// the resume marker must come from the raw rows, not the emitted
    /// page: a truncated page whose parts all vanished in pass 2 would
    /// otherwise report `truncated` with no marker and the client would
    /// re-request the same page forever.
    pub async fn list_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        marker: Option<u32>,
        max_parts: usize,
    ) -> Result<(Vec<PartInfo>, bool, Option<u32>), Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        // One read transaction: the upload check AND the committed PARTS
        // rows of the page (raw rows from `marker + 1`, capped at
        // `max_parts`, plus one lookahead for `truncated` — a page costs
        // O(page) rows, not a full-upload scan). The ETags are parsed only
        // for the emitted page (an invalid page row is skipped — corrupted
        // rows are rare and self-healing; listing is DB-driven and does
        // not fall back to the file, only the point query does, §5.5.7).
        // `max_parts = 0` requests nothing, and no marker either (an
        // exclusive-after marker would skip the first part of the next
        // page forever).
        let (found, page, truncated) = self
            .handle
            .read(|txn| {
                let uploads = database::UploadsTable::open_readonly(txn)?;
                if !uploads.key_matches(bucket, key, upload_id)? {
                    return Ok((false, Vec::new(), false));
                }
                if max_parts == 0 {
                    return Ok((true, Vec::new(), false));
                }
                // The marker is exclusive — but `u32::MAX` has no
                // successor: saturating the start would re-include the
                // marker part itself. (Unreachable via S3's 1..=10000
                // range; this layer does not rely on that.)
                let Some(start) = marker.map_or(Some(0), |m| m.checked_add(1)) else {
                    return Ok((true, Vec::new(), false));
                };
                let (recorded, truncated) = database::PartsTable::open_readonly(txn)?
                    .list_from(bucket, upload_id, start, max_parts)?;
                Ok((true, recorded, truncated))
            })
            .map_err(Error::from)?;
        if !found {
            return Err(storage::no_such_upload(upload_id).into());
        }
        // The resume marker: the last RAW row of the page (before pass-2
        // filtering) — a page that survives filtering is the same thing,
        // and a truncated page whose parts all vanished still advances
        // the client past them.
        let raw_last = page.last().map(|(n, _)| *n);
        // Pass 2: size/mtime from the part files of this page only.
        let mut parts = Vec::with_capacity(page.len());
        for (n, hex) in page {
            let Ok(etag) = ETag::new(&hex) else {
                continue;
            };
            let path = part_path(&dir, n);
            let metadata = match tokio::fs::metadata(&path).await {
                Ok(metadata) => metadata,
                // A part can vanish between the passes (a concurrent
                // abort) — skip it rather than fail the listing.
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            parts.push(PartInfo {
                part_number: n.into(),
                size: metadata.len(),
                etag,
                last_modified: metadata.modified()?,
            });
        }
        Ok((parts, truncated, raw_last))
    }

    /// The ETag of one part: the `PARTS` record, or recomputed streaming
    /// when the record is missing (a crash between the file rename and the
    /// record commit — §5.6). Only the point query falls back; listings
    /// stay DB-driven.
    #[cfg(test)]
    async fn part_etag(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        n: u32,
    ) -> Result<ETag, Error> {
        let stored = self
            .handle
            .read(|txn| database::PartsTable::open_readonly(txn)?.get_hex(bucket, upload_id, n))
            .map_err(Error::from)?;
        if let Some(hex) = stored {
            // A domain-invalid record: fall through to the recompute.
            if let Ok(etag) = ETag::new(&hex) {
                return Ok(etag);
            }
        }
        let (digest, _) = md5_of_file(&part_path(&self.upload_dir(bucket, upload_id)?, n)).await?;
        Ok(ETag::Single(digest))
    }

    /// Verify the listed parts and assemble them into a fresh temp file
    /// (the caller renames it onto the object path under its own lock —
    /// bucket mutations serialize with `delete_bucket`, and then calls
    /// [`Self::consume`] to delete the upload's records). The upload is
    /// NOT consumed here: its records must outlive the rename so a crash
    /// between the rename and the consume leaves the upload listed and a
    /// client retry completes idempotently (meta-redb-spec §5.3, §5.6).
    ///
    /// - empty `parts` → `storage::Error::NoParts`;
    /// - missing / mismatched / out-of-order part → `InvalidPart`;
    /// - a part file vanishing mid-verify/assembly when the upload was
    ///   consumed (a concurrent abort/sweep) → `NoSuchUpload` (§5.6);
    /// - the upload's recorded key differs from `key` → `NoSuchUpload`.
    ///
    /// Returns the temp path and the composed ETag (`MD5-of-MD5s-N`).
    pub async fn complete(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<(PathBuf, ETag), Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        if parts.is_empty() {
            self.require_upload(bucket, key, upload_id).await?;
            return Err(storage::no_parts().into());
        }
        // One read transaction: the upload check AND the `PARTS` rows of
        // the requested parts only (point lookups — no full-upload scan;
        // the verify loop below looks each part up here instead of opening
        // a transaction per part).
        let (found, records) = self
            .handle
            .read(|txn| {
                let uploads = database::UploadsTable::open_readonly(txn)?;
                if !uploads.key_matches(bucket, key, upload_id)? {
                    return Ok((false, HashMap::new()));
                }
                let table = database::PartsTable::open_readonly(txn)?;
                let mut records = HashMap::with_capacity(parts.len());
                for part in parts {
                    let n = u32::from(part.part_number);
                    if let Some(hex) = table.get_hex(bucket, upload_id, n)? {
                        records.insert(n, hex);
                    }
                }
                Ok((true, records))
            })
            .map_err(Error::from)?;
        if !found {
            return Err(storage::no_such_upload(upload_id).into());
        }
        // Verify: strictly ascending, each part exists with a matching
        // ETag (the `PARTS` row, or the file recompute when the row is
        // missing — the crash-window fallback, §5.6).
        let mut infos = Vec::with_capacity(parts.len());
        let mut last = 0u32;
        for part in parts {
            let n = u32::from(part.part_number);
            if n <= last {
                return Err(storage::invalid_part(n).into());
            }
            last = n;
            // A missing part file (NotFound) is an InvalidPart — unless
            // a concurrent abort/sweep consumed the upload (then
            // NoSuchUpload, §5.6).
            let path = part_path(&dir, n);
            let metadata = match tokio::fs::metadata(&path).await {
                Ok(metadata) if !metadata.is_dir() => metadata,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    return Err(self.vanished_part(bucket, key, upload_id, n).await);
                }
                Ok(_) | Err(_) => return Err(storage::invalid_part(n).into()),
            };
            let stored = match records.get(&n).and_then(|hex| ETag::new(hex).ok()) {
                // A domain-invalid record (or no record — the crash-window
                // fallback): recompute from the file, §5.6.
                None => match md5_of_file(&path).await {
                    Ok((digest, _)) => ETag::Single(digest),
                    Err(Error::Io(err)) if err.kind() == io::ErrorKind::NotFound => {
                        return Err(self.vanished_part(bucket, key, upload_id, n).await);
                    }
                    Err(err) => return Err(err),
                },
                Some(etag) => etag,
            };
            if stored != part.etag {
                return Err(storage::invalid_part(n).into());
            }
            infos.push(PartInfo {
                part_number: part.part_number,
                size: metadata.len(),
                etag: stored,
                last_modified: metadata.modified()?,
            });
        }
        // Assemble before consuming the upload (part files stay readable
        // until the delete). The bytes copied are hashed in the same
        // pass — the verify-then-copy race closes here: a concurrent
        // put_part overwriting a part mid-assembly yields copied bytes
        // that disagree with the verified ETag and fails the completion
        // (no post-assembly re-read of the parts, §5.6).
        let tmp_dir = self.writer.tmp_dir();
        tokio::fs::create_dir_all(tmp_dir).await?;
        let temp = tmp_dir.join(format!("multipart-{}", uuid::Uuid::new_v4()));
        let assemble = async {
            let mut out = tokio::fs::File::create(&temp).await?;
            let mut buf = vec![0u8; CHUNK_SIZE];
            for info in &infos {
                let n = u32::from(info.part_number);
                let mut file = match tokio::fs::File::open(part_path(&dir, n)).await {
                    Ok(file) => file,
                    // A part can vanish between the verify pass and the
                    // copy (a concurrent abort/sweep consumed the upload,
                    // or out-of-band removal): classify via UPLOADS (§5.6).
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        return Err(self.vanished_part(bucket, key, upload_id, n).await);
                    }
                    Err(err) => return Err(err.into()),
                };
                let mut hasher = Md5::new();
                loop {
                    let read = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    tokio::io::AsyncWriteExt::write_all(&mut out, &buf[..read]).await?;
                    hasher.update(&buf[..read]);
                }
                if ETag::Single(hasher.finalize().into()) != info.etag {
                    return Err(Error::from(storage::invalid_part(n)));
                }
            }
            tokio::io::AsyncWriteExt::flush(&mut out).await?;
            Ok::<_, Error>(())
        }
        .await;
        if let Err(err) = assemble {
            let _ = tokio::fs::remove_file(&temp).await;
            // A concurrent abort/sweep may have consumed the upload while
            // the parts streamed — report that accurately (the upload is
            // gone either way) instead of a raw I/O error. Only the
            // not-found upload maps; a database failure must not be
            // masked as NoSuchUpload.
            if matches!(&err, Error::Io(io_err) if io_err.kind() == io::ErrorKind::NotFound)
                && matches!(
                    self.require_upload(bucket, key, upload_id).await,
                    Err(Error::Storage(storage::Error::NoSuchUpload(_)))
                )
            {
                return Err(storage::no_such_upload(upload_id).into());
            }
            return Err(err);
        }
        // The upload records are intentionally NOT touched here (see the
        // doc comment) — the caller renames onto the object, then
        // consumes (§5.3).
        let etag =
            ETag::composed_from_parts(&infos).ok_or_else(|| Error::from(storage::no_parts()))?;
        Ok((temp, etag))
    }

    /// Delete an upload's `UPLOADS` + `PARTS` records in one transaction,
    /// then remove its directory best-effort. Called by the backend AFTER
    /// the assembled temp is renamed onto the object path (meta-redb-spec
    /// §5.3: rename → single-txn delete → best-effort dir removal), so a
    /// crash between the rename and this call leaves the upload listed
    /// and a client retry completes idempotently (§5.6).
    ///
    /// Idempotent: an upload that is already gone (consumed by a retry,
    /// or removed by a concurrent abort) is a no-op — the rename has
    /// already committed, so the caller must not fail afterwards.
    pub async fn consume(&self, bucket: &bucket::Name, upload_id: &str) -> Result<(), Error> {
        self.drain_upload_rows(bucket, upload_id).await?;
        let dir = self.upload_dir(bucket, upload_id)?;
        ok_if_missing(tokio::fs::remove_dir_all(&dir).await)?;
        Ok(())
    }

    /// Remove an upload's part directory (best-effort). The backend calls
    /// this AFTER the completion's state transaction committed — the
    /// records are gone, only the part files remain on disk; a failure is
    /// logged (never reported to the client: the object is committed, and
    /// a retry must not fail) and the residue is reclaimed by the startup
    /// orphan stage after the idle grace.
    pub(crate) async fn remove_part_dir(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        ok_if_missing(tokio::fs::remove_dir_all(&dir).await)?;
        Ok(())
    }

    /// Delete an upload's `UPLOADS` + `PARTS` rows (no directory removal,
    /// no UUID check). Cleanup uses this on orphan directories whose
    /// names are not necessarily upload ids.
    pub(crate) async fn drain_upload_rows(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), Error> {
        self.handle
            .write(|txn| drain_upload(txn, bucket, upload_id))
            .map_err(Error::from)
    }

    /// Abort an upload: one transaction deletes its `UPLOADS` + `PARTS`
    /// records, then the part directory is removed best-effort.
    /// `NoSuchUpload` when the upload does not exist.
    pub async fn abort(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        // The dir path (UUID-validated) is needed for the best-effort
        // removal. One write transaction: the existence + key check and
        // the UPLOADS+PARTS drain are atomic (the drain is inlined because
        // redb refuses a second `open_table` of `UPLOADS` in one
        // transaction — [`drain_upload`] opens it itself).
        let dir = self.upload_dir(bucket, upload_id)?;
        let found = self
            .handle
            .write(|txn| {
                let mut uploads = database::UploadsTable::open(txn)?;
                if !uploads.key_matches(bucket, key, upload_id)? {
                    return Ok(false);
                }
                uploads.remove(bucket, upload_id)?;
                drop(uploads);
                database::PartsTable::open(txn)?.drain_upload(bucket, upload_id)?;
                Ok(true)
            })
            .map_err(Error::from)?;
        if !found {
            return Err(storage::no_such_upload(upload_id).into());
        }
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    /// Every in-progress upload of a bucket, in `(key, upload_id)` order
    /// — the composite order the pagination engine requires, so a page
    /// can resume inside a same-key group (from the `UPLOADS` records).
    pub async fn list_uploads(&self, bucket: &bucket::Name) -> Result<Vec<MultipartUpload>, Error> {
        let mut uploads = self
            .handle
            .read(|txn| {
                let table = database::UploadsTable::open_readonly(txn)?;
                let mut uploads = Vec::new();
                table.for_bucket(bucket, |upload_id, (key, initiated_at)| {
                    if let Some(upload) = upload_from_row(bucket, upload_id, key, initiated_at) {
                        uploads.push(upload);
                    }
                    Ok(())
                })?;
                Ok(uploads)
            })
            .map_err(Error::from)?;
        sort_uploads(&mut uploads);
        Ok(uploads)
    }

    /// The latest part mtime of an upload (`UNIX_EPOCH` when no parts exist
    /// yet) — the sweep's idle computation (idle = max(initiated_at,
    /// latest part mtime), data-model.md).
    pub async fn idle_since(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<SystemTime, Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        match crate::fsutil::latest_part_mtime(&dir).await {
            // `UNIX_EPOCH` when no parts exist yet (or the dir is gone).
            Ok(latest) => Ok(latest.unwrap_or(SystemTime::UNIX_EPOCH)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(SystemTime::UNIX_EPOCH),
            Err(err) => Err(err.into()),
        }
    }

    /// Whether a bucket has any in-progress upload (bucket-delete check:
    /// in-progress uploads make the bucket non-empty).
    pub async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        self.handle
            .read(|txn| database::UploadsTable::open_readonly(txn)?.has_bucket(bucket))
            .map_err(Error::from)
    }

    /// Remove the whole multipart state of a bucket: one transaction
    /// deletes its `UPLOADS` + `PARTS` records, then the directory subtree
    /// is removed best-effort. Test-only since the production teardown
    /// goes through [`crate::FsStorage::remove_bucket_state`].
    #[cfg(test)]
    pub async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        self.handle
            .write(|txn| drain_bucket_uploads(txn, bucket))
            .map_err(Error::from)?;
        let dir = self.root.join(&**bucket);
        ok_if_missing(tokio::fs::remove_dir_all(&dir).await)?;
        Ok(())
    }

    /// Upload records of every bucket, in `(key, upload_id)` order — the
    /// sweep's idle-expiry walk.
    pub async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, Error> {
        let mut uploads = self
            .handle
            .read(|txn| {
                let table = database::UploadsTable::open_readonly(txn)?;
                let mut uploads = Vec::new();
                table.for_each(|b, upload_id, key, initiated_at| {
                    let Ok(bucket) = bucket::name(b) else {
                        return Ok(());
                    };
                    if let Some(upload) = upload_from_row(&bucket, upload_id, key, initiated_at) {
                        uploads.push(upload);
                    }
                    Ok(())
                })?;
                Ok(uploads)
            })
            .map_err(Error::from)?;
        sort_uploads(&mut uploads);
        Ok(uploads)
    }

    /// Raw `(bucket, upload_id)` membership of the `UPLOADS` table — the
    /// orphan cleanup's liveness set (meta-redb-spec §5.7). NO domain
    /// validation, on purpose: a live upload whose stored key fails
    /// validation is still live — judging by the validated
    /// [`Self::walk_uploads`] view would skip its row and the cleanup
    /// would delete a live upload's directory.
    pub(crate) async fn live_upload_ids(&self) -> Result<HashSet<(String, String)>, Error> {
        self.handle
            .read(|txn| {
                let table = database::UploadsTable::open_readonly(txn)?;
                let mut ids = HashSet::new();
                table.for_each(|bucket, upload_id, _, _| {
                    ids.insert((bucket.to_string(), upload_id.to_string()));
                    Ok(())
                })?;
                Ok(ids)
            })
            .map_err(Error::from)
    }

    /// Test hook: overwrite an upload's stored key WITHOUT domain
    /// validation — the §5.7 regression fixture (a live upload whose
    /// stored key is domain-invalid must not be judged an orphan).
    #[cfg(test)]
    pub(crate) async fn overwrite_stored_key(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        key: &str,
    ) -> Result<(), Error> {
        self.handle
            .write(|txn| {
                database::UploadsTable::open(txn)?.insert((&**bucket, upload_id), (key, 0))?;
                Ok(())
            })
            .map_err(Error::from)
    }

    /// Resolve the upload directory and refuse a key mismatch as
    /// `NoSuchUpload` (S3 identity is `(bucket, key, uploadId)`). The
    /// upload must have a committed `UPLOADS` record.
    async fn require_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<PathBuf, Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        let valid = self
            .handle
            .read(|txn| {
                let table = database::UploadsTable::open_readonly(txn)?;
                table.key_matches(bucket, key, upload_id)
            })
            .map_err(Error::from)?;
        if valid {
            Ok(dir)
        } else {
            Err(storage::no_such_upload(upload_id).into())
        }
    }

    /// Classify a vanished part file (§5.6): the upload may have been
    /// consumed by a concurrent abort/sweep (→ `NoSuchUpload`) or the file
    /// went missing out-of-band with the upload still live (→
    /// `InvalidPart`). A database failure propagates — it is never masked
    /// as `NoSuchUpload`.
    async fn vanished_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        n: u32,
    ) -> Error {
        match self.require_upload(bucket, key, upload_id).await {
            Ok(_) => storage::invalid_part(n).into(),
            Err(Error::Storage(storage::Error::NoSuchUpload(_))) => {
                storage::no_such_upload(upload_id).into()
            }
            Err(err) => err,
        }
    }

    /// `<state-dir>/multipart/<bucket>/<upload_id>` — the id is
    /// client-supplied, so only UUIDs (as `create` allocates) may map to a
    /// state-dir path; anything else (e.g. `../`) answers `NoSuchUpload`.
    fn upload_dir(&self, bucket: &bucket::Name, upload_id: &str) -> Result<PathBuf, Error> {
        if uuid::Uuid::parse_str(upload_id).is_err() {
            return Err(storage::no_such_upload(upload_id).into());
        }
        Ok(self.root.join(&**bucket).join(upload_id))
    }
}

/// Create a store rooted at `<state_dir>/multipart/` over its **own**
/// state database.
///
/// Each call opens the `meta.redb` file exclusively — creating two
/// standalone stores (of any kind) over the same state dir at once fails
/// with `DatabaseAlreadyOpen`. Production code constructs one
/// [`crate::FsStorage`] per root and shares its single handle; this
/// constructor is for standalone/embedded use and tests.
///
/// # Errors
///
/// When the state database cannot be opened (a corrupt or unwritable
/// `meta.redb`).
#[inline]
pub fn store(state_dir: &Path) -> Result<Store, Error> {
    Ok(Store::from_handle(
        database::Handle::open(state_dir)?,
        state_dir,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use tinio_core::storage::Error as StorageError;
    use tinio_util::testing::{body, etag};

    fn fixture() -> (tempfile::TempDir, Store) {
        let state = tempfile::tempdir().unwrap();
        let store = store(state.path()).unwrap();
        (state, store)
    }

    #[test]
    fn create_records_upload_without_creating_directory() {
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            assert!(!upload.upload_id.is_empty());
            // The record is DB-driven; the directory appears only with the
            // first part (the orphan-cleanup TOCTOU order depends on it).
            let uploads = store.list_uploads(&b).await.unwrap();
            assert_eq!(uploads.len(), 1);
            assert_eq!(uploads[0].upload_id, upload.upload_id);
            assert!(
                tokio::fs::metadata(state.path().join("multipart/data").join(&upload.upload_id))
                    .await
                    .is_err(),
                "create must not create the upload directory"
            );
        });
    }

    #[test]
    fn put_list_part_round_trip() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let part = store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"part-one"))
                .await
                .unwrap();
            assert_eq!(part.size, 8);
            assert_eq!(part.etag, etag("dede9db222ee612853f44e6e6b1ca792"));
            let (parts, truncated, _) = store
                .list_parts(&b, &k, &upload.upload_id, None, 1000)
                .await
                .unwrap();
            assert_eq!(parts.len(), 1);
            assert!(!truncated);
            assert_eq!(parts[0].etag, part.etag);
        });
    }

    #[test]
    fn put_part_missing_upload_is_no_such_upload() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = MultipartUpload {
                upload_id: "ghost".into(),
                bucket: b.clone(),
                key: k.clone(),
                initiated_at: SystemTime::now(),
            };
            let err = store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn non_uuid_upload_ids_are_no_such_upload() {
        // The upload id is client-supplied and maps into the state-dir
        // path — only UUIDs may do so (no `../` traversal).
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            for evil in ["../victim/abc", "a/b", "..", ""] {
                let err = store
                    .put_part(&b, &k, evil, 1.into(), body(b"x"))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, Error::Storage(StorageError::NoSuchUpload(_))),
                    "{evil}"
                );
            }
        });
    }

    #[test]
    fn complete_under_a_different_key_is_no_such_upload() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            let completed = [CompletedPart {
                part_number: 1.into(),
                etag: etag("9dd4e461268c8034f5c8564e155c67a6"),
            }];
            let err = store
                .complete(
                    &b,
                    &object::key("b.bin").unwrap(),
                    &upload.upload_id,
                    &completed,
                )
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn put_part_under_a_different_key_is_no_such_upload() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let err = store
                .put_part(
                    &b,
                    &object::key("b.bin").unwrap(),
                    &upload.upload_id,
                    1.into(),
                    body(b"x"),
                )
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn complete_assembles_byte_exact_with_composed_etag() {
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let mut parts = Vec::new();
            let parts_data: [&[u8]; 3] = [b"part-one-", b"part-two-", b"part-three"];
            for (i, data) in parts_data.iter().enumerate() {
                let part = store
                    .put_part(
                        &b,
                        &k,
                        &upload.upload_id,
                        ((i + 1) as u32).into(),
                        body(data.to_vec()),
                    )
                    .await
                    .unwrap();
                parts.push(part);
            }
            let completed: Vec<CompletedPart> = parts
                .iter()
                .map(|p| CompletedPart {
                    part_number: p.part_number,
                    etag: p.etag.clone(),
                })
                .collect();
            let target = state.path().join("out.bin");
            let (temp, etag) = store
                .complete(&b, &k, &upload.upload_id, &completed)
                .await
                .unwrap();
            // Hard-coded reference composition (MD5 of raw part digests).
            assert_eq!(etag.as_str(), "aed23cbfc502f1e851e828efe2ca50d0-3");
            // Not consumed yet: the records and subtree must outlive the
            // assemble step (the caller renames, then consumes — §5.3).
            assert!(store.has_uploads(&b).await.unwrap());
            let upload_dir = state.path().join("multipart/data").join(&upload.upload_id);
            assert!(tokio::fs::metadata(&upload_dir).await.is_ok());
            // Rename onto the object, then consume the upload.
            tokio::fs::rename(&temp, &target).await.unwrap();
            store.consume(&b, &upload.upload_id).await.unwrap();
            assert_eq!(
                tokio::fs::read(&target).await.unwrap(),
                b"part-one-part-two-part-three"
            );
            // The upload records and subtree are gone.
            assert!(!store.has_uploads(&b).await.unwrap());
            assert!(tokio::fs::metadata(&upload_dir).await.is_err());
            // The object is retrievable as a regular object.
            let content = tokio::fs::read(&target).await.unwrap();
            assert_eq!(content, b"part-one-part-two-part-three");
        });
    }

    #[test]
    fn complete_no_parts_is_error() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let err = store
                .complete(&b, &k, &upload.upload_id, &[])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoParts)));
        });
    }

    #[test]
    fn complete_mismatched_or_missing_part_is_invalid_part() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();

            // Wrong ETag.
            let wrong = CompletedPart {
                part_number: 1.into(),
                etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
            };
            let err = store
                .complete(&b, &k, &upload.upload_id, &[wrong])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::InvalidPart(1))));

            // Missing part number 2 after 1.
            let good = CompletedPart {
                part_number: 1.into(),
                etag: etag("9dd4e461268c8034f5c8564e155c67a6"),
            };
            let missing = CompletedPart {
                part_number: 3.into(),
                etag: etag("9dd4e461268c8034f5c8564e155c67a6"),
            };
            let err = store
                .complete(&b, &k, &upload.upload_id, &[good.clone(), missing.clone()])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::InvalidPart(3))));

            // Out of order (a repeated part number is not strictly
            // ascending).
            let err = store
                .complete(&b, &k, &upload.upload_id, &[good.clone(), good.clone()])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::InvalidPart(1))));
        });
    }

    #[test]
    fn abort_removes_parts_and_is_no_such_upload_after() {
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            store.abort(&b, &k, &upload.upload_id).await.unwrap();
            assert!(
                tokio::fs::metadata(state.path().join("multipart/data").join(&upload.upload_id))
                    .await
                    .is_err()
            );
            let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn complete_retry_after_rename_is_idempotent() {
        // §5.6 crash window: the object is renamed before the upload is
        // consumed — a crash in between leaves the upload listed, and a
        // client retry completes again without error.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let part = store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            let completed = [CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            }];
            let target = state.path().join("out.bin");
            // First attempt: the rename lands, the consume does not
            // (the crash). The upload is still listed.
            let (temp, _) = store
                .complete(&b, &k, &upload.upload_id, &completed)
                .await
                .unwrap();
            tokio::fs::rename(&temp, &target).await.unwrap();
            assert!(store.has_uploads(&b).await.unwrap());
            // Retry: verify + assemble + rename + consume all succeed.
            let (temp, _) = store
                .complete(&b, &k, &upload.upload_id, &completed)
                .await
                .unwrap();
            tokio::fs::rename(&temp, &target).await.unwrap();
            store.consume(&b, &upload.upload_id).await.unwrap();
            assert!(!store.has_uploads(&b).await.unwrap());
            assert_eq!(tokio::fs::read(&target).await.unwrap(), b"x");
        });
    }

    #[test]
    fn abort_after_complete_consume_is_no_such_upload() {
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let part = store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            let completed = [CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            }];
            let target = state.path().join("out.bin");
            let (temp, _) = store
                .complete(&b, &k, &upload.upload_id, &completed)
                .await
                .unwrap();
            tokio::fs::rename(&temp, &target).await.unwrap();
            store.consume(&b, &upload.upload_id).await.unwrap();
            let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn consume_is_idempotent_for_a_missing_upload() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            // No part dir was ever created; consuming twice is a no-op.
            store.consume(&b, &upload.upload_id).await.unwrap();
            store.consume(&b, &upload.upload_id).await.unwrap();
            assert!(!store.has_uploads(&b).await.unwrap());
        });
    }

    #[test]
    fn racing_complete_and_abort_leave_a_consistent_state() {
        // The caller's flow is rename-then-consume, so a racing abort can
        // never fail a complete after its rename committed: whichever
        // wins, the upload ends with no records and a late abort answers
        // NoSuchUpload.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let mut completed = Vec::new();
            for i in 1..=4u32 {
                let data = vec![i as u8; 32 * 1024];
                let part = store
                    .put_part(&b, &k, &upload.upload_id, i.into(), body(data))
                    .await
                    .unwrap();
                completed.push(CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                });
            }
            let (complete, abort) = tokio::join!(
                store.complete(&b, &k, &upload.upload_id, &completed),
                store.abort(&b, &k, &upload.upload_id),
            );
            // If the assemble won, the caller renames and consumes; the
            // consume is a no-op if the abort already removed the records.
            if let Ok((temp, _)) = complete {
                let target = state.path().join("out.bin");
                tokio::fs::rename(&temp, &target).await.unwrap();
                store.consume(&b, &upload.upload_id).await.unwrap();
            } else {
                assert!(abort.is_ok());
            }
            // Whatever won, no records remain and a late abort answers
            // NoSuchUpload.
            assert!(!store.has_uploads(&b).await.unwrap());
            let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn list_parts_skips_a_part_whose_file_vanished() {
        // A part file can disappear under a concurrent abort (or out-of-
        // band): the listing skips it instead of failing the whole page.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let p1 = store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            let p2 = store
                .put_part(&b, &k, &upload.upload_id, 2.into(), body(b"yy"))
                .await
                .unwrap();
            let part_dir = state.path().join("multipart/data").join(&upload.upload_id);
            // Remove part 1's file out-of-band (the record stays).
            tokio::fs::remove_file(part_dir.join("part-1"))
                .await
                .unwrap();
            let (parts, truncated, _) = store
                .list_parts(&b, &k, &upload.upload_id, None, 100)
                .await
                .unwrap();
            assert!(!truncated);
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].part_number, p2.part_number);
            assert_eq!(parts[0].etag, p2.etag);
            let _ = p1;
        });
    }

    #[test]
    fn list_parts_skips_a_part_with_invalid_stored_etag() {
        // Listing is DB-driven: a domain-invalid PARTS hex is omitted,
        // not recomputed from the file (that fallback is the point query).
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            store
                .handle
                .write(|txn| {
                    let mut parts = database::PartsTable::open(txn).unwrap();
                    parts.insert((&*b, upload.upload_id.as_str(), 1u32), "not-an-etag")?;
                    Ok(())
                })
                .unwrap();
            let (parts, _, _) = store
                .list_parts(&b, &k, &upload.upload_id, None, 100)
                .await
                .unwrap();
            assert!(parts.is_empty(), "{parts:?}");
            let etag = store.part_etag(&b, &upload.upload_id, 1).await.unwrap();
            assert_eq!(etag, ETag::from_content(b"x"));
        });
    }

    #[test]
    fn list_uploads_and_has_uploads() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k1 = object::key("a.bin").unwrap();
            let k2 = object::key("b.bin").unwrap();
            let u1 = store.create(&b, &k1).await.unwrap();
            let u2 = store.create(&b, &k2).await.unwrap();
            let uploads = store.list_uploads(&b).await.unwrap();
            assert_eq!(uploads.len(), 2);
            assert!(store.has_uploads(&b).await.unwrap());
            store.abort(&b, &k1, &u1.upload_id).await.unwrap();
            store.abort(&b, &k2, &u2.upload_id).await.unwrap();
            assert!(!store.has_uploads(&b).await.unwrap());
        });
    }

    #[test]
    fn list_uploads_orders_same_key_group_by_upload_id() {
        // The composite `key\0upload_id` order positions a page inside a
        // same-key group: pagination must never skip an upload because a
        // page cut between two uploads of one key.
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("same.bin").unwrap();
            store.create(&b, &k).await.unwrap();
            store.create(&b, &k).await.unwrap();
            let uploads = store.list_uploads(&b).await.unwrap();
            assert_eq!(uploads.len(), 2);
            assert!(
                uploads[0].upload_id < uploads[1].upload_id,
                "same-key uploads must be ordered by upload id: {:?}",
                uploads.iter().map(|u| &u.upload_id).collect::<Vec<_>>()
            );
        });
    }

    #[test]
    fn remove_bucket_clears_uploads() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store.remove_bucket(&b).await.unwrap();
            assert!(!store.has_uploads(&b).await.unwrap());
            let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn walk_uploads_finds_all() {
        rt(async {
            let (_, store) = fixture();
            let b1 = bucket::name("alpha").unwrap();
            let b2 = bucket::name("zeta").unwrap();
            store
                .create(&b1, &object::key("a.bin").unwrap())
                .await
                .unwrap();
            store
                .create(&b2, &object::key("z.bin").unwrap())
                .await
                .unwrap();
            let uploads = store.walk_uploads().await.unwrap();
            assert_eq!(uploads.len(), 2);
        });
    }

    #[test]
    fn list_parts_truncated_page_with_vanished_parts_still_marks_resume() {
        // A truncated page whose parts all vanished in pass 2 must still
        // report a resume marker (from the raw rows) — an empty page with
        // truncated=true and no marker would make the client re-request
        // the identical page forever.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            for n in 1..=4u32 {
                store
                    .put_part(&b, &k, &upload.upload_id, n.into(), body(b"x"))
                    .await
                    .unwrap();
            }
            let dir = state.path().join("multipart/data").join(&upload.upload_id);
            // Remove the first two part files out-of-band (records stay).
            tokio::fs::remove_file(dir.join("part-1")).await.unwrap();
            tokio::fs::remove_file(dir.join("part-2")).await.unwrap();
            let (parts, truncated, raw_last) = store
                .list_parts(&b, &k, &upload.upload_id, None, 2)
                .await
                .unwrap();
            assert!(truncated);
            assert!(parts.is_empty(), "{parts:?}");
            // The marker is the raw last row of the page — the client
            // resumes past the vanished parts.
            assert_eq!(raw_last, Some(2));
            let (parts, truncated, _) = store
                .list_parts(&b, &k, &upload.upload_id, raw_last, 10)
                .await
                .unwrap();
            assert!(!truncated);
            assert_eq!(parts.len(), 2, "parts 3-4 still listed: {parts:?}");
        });
    }

    #[test]
    fn part_lock_slots_are_evicted_after_use() {
        // The per-upload lock map must not grow: every `put_part` releases
        // its slot when the last handle drops (a leak would be unbounded
        // memory on a long-running server with churning uploads).
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            assert!(
                store.part_locks.is_empty(),
                "lock slots must be evicted after use"
            );
        });
    }

    #[test]
    fn concurrent_same_part_overwrites_never_mismatch_file_and_record() {
        // Two put_parts of the same part interleaving rename/txn must end
        // with the file and the PARTS record agreeing (the per-upload lock
        // serializes rename + upsert) — otherwise complete verifies the
        // record, then re-hashes the file and fails InvalidPart forever.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let id = upload.upload_id.clone();
            let (a, b2) = tokio::join!(
                store.put_part(&b, &k, &id, 1.into(), body(b"AAAA")),
                store.put_part(&b, &k, &id, 1.into(), body(b"BBBB")),
            );
            a.unwrap();
            b2.unwrap();
            let (parts, _, _) = store.list_parts(&b, &k, &id, None, 10).await.unwrap();
            assert_eq!(parts.len(), 1);
            // The record's etag must be the hash of the file's content.
            let dir = state.path().join("multipart/data").join(&id);
            let content = tokio::fs::read(dir.join("part-1")).await.unwrap();
            assert_eq!(parts[0].etag, ETag::from_content(&content));
        });
    }

    #[test]
    fn list_parts_is_db_driven_no_ghost_parts() {
        // A part file whose PARTS record never committed (crash window)
        // must not appear in listings — the client retransmits.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            // Write a part file out-of-band (no PARTS record).
            let dir = state.path().join("multipart/data").join(&upload.upload_id);
            tokio::fs::create_dir_all(&dir).await.unwrap();
            tokio::fs::write(dir.join("part-1"), b"orphan")
                .await
                .unwrap();
            let (parts, _, _) = store
                .list_parts(&b, &k, &upload.upload_id, None, 1000)
                .await
                .unwrap();
            assert!(parts.is_empty(), "no record, no listing: {parts:?}");
            // But the point query recomputes from the file (complete
            // fallback for the crash window).
            let etag = store.part_etag(&b, &upload.upload_id, 1).await.unwrap();
            assert_eq!(etag, ETag::from_content(b"orphan"));
        });
    }

    #[test]
    fn list_parts_pages_by_part_number() {
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            for n in 1..=5u32 {
                store
                    .put_part(&b, &k, &upload.upload_id, n.into(), body(vec![n as u8]))
                    .await
                    .unwrap();
            }
            let (page, truncated, _) = store
                .list_parts(&b, &k, &upload.upload_id, None, 2)
                .await
                .unwrap();
            assert!(truncated);
            assert_eq!(
                page.iter()
                    .map(|p| u32::from(p.part_number))
                    .collect::<Vec<_>>(),
                [1, 2]
            );
            let (page, truncated, _) = store
                .list_parts(&b, &k, &upload.upload_id, Some(2), 2)
                .await
                .unwrap();
            assert!(truncated);
            assert_eq!(
                page.iter()
                    .map(|p| u32::from(p.part_number))
                    .collect::<Vec<_>>(),
                [3, 4]
            );
            let (page, truncated, _) = store
                .list_parts(&b, &k, &upload.upload_id, Some(4), 2)
                .await
                .unwrap();
            assert!(!truncated);
            assert_eq!(
                page.iter()
                    .map(|p| u32::from(p.part_number))
                    .collect::<Vec<_>>(),
                [5]
            );
        });
    }

    #[test]
    fn complete_racing_put_part_never_mismatches_content() {
        // #6: a put_part overwriting a part between complete's
        // verification pass and its copy must either fail the completion
        // (InvalidPart via the post-assembly re-verification) or produce
        // an object whose bytes match the composed ETag — never a success
        // whose content disagrees with the returned ETag.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let parts_data: [&[u8]; 3] = [b"part-1", b"part-2", b"part-3-original"];
            for (i, data) in parts_data.iter().enumerate() {
                store
                    .put_part(
                        &b,
                        &k,
                        &upload.upload_id,
                        ((i + 1) as u32).into(),
                        body(data.to_vec()),
                    )
                    .await
                    .unwrap();
            }
            // The client's completion list carries the REAL etags of the
            // parts it uploaded.
            let (real, _, _) = store
                .list_parts(&b, &k, &upload.upload_id, None, 100)
                .await
                .unwrap();
            let completed: Vec<CompletedPart> = real
                .iter()
                .map(|p| CompletedPart {
                    part_number: p.part_number,
                    etag: p.etag.clone(),
                })
                .collect();
            // A racer retries UploadPart(3) while the completion runs
            // (the client misbehaves; errors are fine — the upload may be
            // consumed).
            let racer = tokio::spawn({
                let store = store.clone();
                let b = b.clone();
                let k = k.clone();
                let upload_id = upload.upload_id.clone();
                async move {
                    for i in 0..200u32 {
                        let _ = store
                            .put_part(
                                &b,
                                &k,
                                &upload_id,
                                3.into(),
                                body(format!("part-3-overwrite-{i}")),
                            )
                            .await;
                    }
                }
            });
            let outcome = store.complete(&b, &k, &upload.upload_id, &completed).await;
            racer.await.unwrap();
            match outcome {
                Ok((temp, etag)) => {
                    // Success means the re-verification passed: no part
                    // changed between verify and copy — the bytes are
                    // exactly the original parts and the ETag matches.
                    let target = state.path().join("out.bin");
                    tokio::fs::rename(&temp, &target).await.unwrap();
                    assert_eq!(
                        tokio::fs::read(&target).await.unwrap(),
                        b"part-1part-2part-3-original"
                    );
                    assert_eq!(etag, ETag::composed_from_parts(&real).unwrap());
                }
                Err(err) => {
                    // Failure is fine — the completion must simply not
                    // have committed a mismatched object.
                    assert!(
                        matches!(
                            err,
                            Error::Storage(StorageError::InvalidPart(_))
                                | Error::Storage(StorageError::NoSuchUpload(_))
                        ),
                        "{err:?}"
                    );
                }
            }
        });
    }

    #[test]
    fn abort_during_assembly_is_no_such_upload() {
        // §5.6: a part file vanishing mid-verify/assembly because a
        // concurrent abort consumed the upload must surface NoSuchUpload
        // (the NotFound re-checks UPLOADS), never a bare InvalidPart or
        // I/O error. The abort commits its record deletion before
        // touching the directory, so any NotFound implies the upload is
        // gone.
        rt(async {
            let (state, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let mut saw_no_such_upload = false;
            // Enough parts that the assembly window (per-part opens)
            // reliably intersects the abort; retry on the off chance the
            // completion wins the race outright.
            for _ in 0..8 {
                let upload = store.create(&b, &k).await.unwrap();
                let mut completed = Vec::new();
                for n in 1..=8u32 {
                    let part = store
                        .put_part(
                            &b,
                            &k,
                            &upload.upload_id,
                            n.into(),
                            body(vec![n as u8; 256 * 1024]),
                        )
                        .await
                        .unwrap();
                    completed.push(CompletedPart {
                        part_number: part.part_number,
                        etag: part.etag,
                    });
                }
                let (complete, abort) = tokio::join!(
                    store.complete(&b, &k, &upload.upload_id, &completed),
                    store.abort(&b, &k, &upload.upload_id),
                );
                match complete {
                    Ok((temp, _)) => {
                        // The completion won — finish the caller flow and
                        // retry the race with a fresh upload.
                        let target = state.path().join(format!("out-{}.bin", upload.upload_id));
                        tokio::fs::rename(&temp, &target).await.unwrap();
                        store.consume(&b, &upload.upload_id).await.unwrap();
                        assert!(
                            abort.is_ok()
                                || matches!(
                                    abort,
                                    Err(Error::Storage(StorageError::NoSuchUpload(_)))
                                )
                        );
                    }
                    Err(err) => {
                        assert!(
                            matches!(err, Error::Storage(StorageError::NoSuchUpload(_))),
                            "abort mid-assembly must surface NoSuchUpload: {err:?}"
                        );
                        saw_no_such_upload = true;
                        break;
                    }
                }
            }
            assert!(saw_no_such_upload, "the abort never landed mid-assembly");
        });
    }

    #[test]
    fn list_parts_marker_at_u32_max_returns_empty_page() {
        // The exclusive marker has no successor at u32::MAX — a
        // saturating +1 would re-include the marker part itself.
        // (Unreachable via S3's 1..=10000 range, but this layer must not
        // rely on that.)
        rt(async {
            let (_, store) = fixture();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            // A raw row at the boundary (no part file needed — pass 2
            // skips missing files; the marker logic sees the raw rows).
            store
                .handle
                .write(|txn| {
                    database::PartsTable::open(txn).unwrap().insert(
                        (&*b, upload.upload_id.as_str(), u32::MAX),
                        "9dd4e461268c8034f5c8564e155c67a6",
                    )?;
                    Ok(())
                })
                .unwrap();
            // Control: the boundary row is reachable from just below it.
            let (_, truncated, raw_last) = store
                .list_parts(&b, &k, &upload.upload_id, Some(u32::MAX - 1), 10)
                .await
                .unwrap();
            assert!(!truncated);
            assert_eq!(raw_last, Some(u32::MAX));
            // At the boundary: an empty, untruncated page — the marker
            // part is never re-included.
            let (parts, truncated, raw_last) = store
                .list_parts(&b, &k, &upload.upload_id, Some(u32::MAX), 10)
                .await
                .unwrap();
            assert!(parts.is_empty(), "{parts:?}");
            assert!(!truncated);
            assert_eq!(raw_last, None);
        });
    }
}
