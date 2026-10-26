//! Multipart upload storage (task T044, migrated to redb per
//! meta-redb-spec).
//!
//! The upload records live in the state database's `UPLOADS` /
//! `UPLOAD_CHECKSUMS` and `PARTS` / `PART_CHECKSUMS` tables
//! (`<state-dir>/meta.redb`). Their decode, key check, refusal
//! projection, and one-transaction-per-method discipline are the shared
//! contract implementation's (`RedbMetadata`) — this module is the
//! file-system facade over it (Task 7).
//!
//! The file system keeps only the part contents at
//! `<state-dir>/multipart/<bucket>/<upload_id>/part-<n>` (the upload
//! directory is created by the first `put_part`, never by `create` — the
//! orphan-cleanup TOCTOU order depends on it, §5.7). Assembly streams all
//! parts into a temp file, then renames atomically onto the object path;
//! the composed ETag `MD5-of-MD5s-N` matches the AWS reference
//! composition. Parts survive restarts, so cross-restart completion/abort
//! is legal (quickstart §7). The S3 5 MiB minimum for non-final parts is
//! enforced authoritatively in [`Store::complete`]'s verify loop
//! (EntityTooSmall) — part uploads themselves accept any size.
//! The number of concurrently in-progress uploads is capped by
//! `max_concurrent_uploads` (default `DEFAULT_MAX_CONCURRENT_UPLOADS`,
//! `[s3] max_concurrent_uploads`).
//!
//! `list_parts` stays DB-driven (a part whose PARTS record never committed
//! is invisible — the client retransmits, and the point query recomputes
//! from the file as a fallback, §5.6).
//!
//! The backend's S3 multipart operations are the ops engine's bodies
//! (`tinio_ops::MultipartOps`, called from this crate's backend module): they stage
//! through this crate's content plane and take this store's per-upload part
//! lock through `Hooks::lock_part`. The store's own `create` / `put_part` /
//! `list_parts` / `abort` are its standalone and embedded entry points — the
//! same primitives (the cap, the part files and the lock live here once).

// `put_part_copy` (the unix copy_file_range fast path) takes the std
// handle — tokio's `File` stays for the async assembly reads below.
#[cfg(unix)]
use std::fs::File as StdFile;
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use md5::{Digest, Md5};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    _core::{
        BodyStream, ETag, acl,
        acl::Acl,
        bucket, checksum,
        meta::{UploadMeta, upload as row},
        multipart::{
            CompletedPart, MultipartUpload, PartInfo, PartNumber, check_part_minimum, part_number,
        },
        object::{self},
        storage::{self, DEFAULT_MAX_CONCURRENT_UPLOADS, Error::NoSuchUpload},
    },
    _store_redb::{
        RedbMetadata, object_part, part, part_checksum, part_stamp, upload, upload_checksum,
        upload_object_lock,
    },
    _util::lockmap::{Guard, Map},
    Error,
    database::{self, Driver},
    fsutil,
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
type PartLocks = Map<(String, String)>;

/// One assembled part of a completed upload, as [`Store::complete`]
/// returns it: the [`object_part::Stored`] row — part number, size, and
/// the stored per-part checksum (`None` when the part was uploaded
/// without one, or the stored wire is domain-invalid). The caller
/// persists these into `OBJECT_PARTS` (spec 2026-08-31).
pub(crate) type CompletedPartRow = object_part::Stored;

/// Delete one upload's `UPLOADS`/`UPLOAD_CHECKSUMS` + `PARTS`/
/// `PART_CHECKSUMS`/`PART_STAMPS` rows in the caller's write transaction
/// (`complete`'s consume and the backend's `complete_object_state` share
/// this).
pub(crate) fn drain_upload(
    txn: &mut redb::WriteTransaction,
    bucket: &bucket::Name,
    upload_id: &str,
) -> Result<(), database::Error> {
    upload::Table::open(txn)?.remove(bucket, upload_id)?;
    drain_upload_rest(txn, bucket, upload_id)
}

/// The checksum + PARTS + stamp half of the drain, after [`drain_upload`]
/// removed the `UPLOADS` row.
///
/// The five-table delete list has two homes on purpose: the contract's
/// `drain_upload`/`drain_bucket` are the engine's and the facade's home,
/// while this helper serves callers that must delete the rows inside the
/// caller's own write transaction (the backend's completed-object state and
/// the bucket teardown) and so cannot delegate to the contract. A table
/// added to the upload lifecycle must be drained in both.
fn drain_upload_rest(
    txn: &mut redb::WriteTransaction,
    bucket: &bucket::Name,
    upload_id: &str,
) -> Result<(), database::Error> {
    // The UPLOADS/checksum halves are one exact key each (idempotent
    // remove — no range scan); only the PARTS half needs the range. OL6: the
    // lock intent leaves with the upload row.
    upload_checksum::Table::open(txn)?.remove(bucket, upload_id)?;
    upload_object_lock::Table::open(txn)?.remove(bucket, upload_id)?;
    part::Table::open(txn)?.drain_upload(bucket, upload_id)?;
    part_checksum::Table::open(txn)?.drain_upload(bucket, upload_id)?;
    part_stamp::Table::open(txn)?.drain_upload(bucket, upload_id)?;
    Ok(())
}

/// Delete every `UPLOADS`/`UPLOAD_CHECKSUMS` + `PARTS`/`PART_CHECKSUMS`/
/// `PART_STAMPS` row of a bucket in the caller's write transaction (bucket
/// teardown — `remove_bucket_state`; the directory is removed by the
/// caller).
pub(crate) fn drain_bucket_uploads(
    txn: &mut redb::WriteTransaction,
    bucket: &bucket::Name,
) -> Result<(), database::Error> {
    upload::Table::open(txn)?.drain_bucket(bucket)?;
    upload_checksum::Table::open(txn)?.drain_bucket(bucket)?;
    upload_object_lock::Table::open(txn)?.drain_bucket(bucket)?;
    part::Table::open(txn)?.drain_bucket(bucket)?;
    part_checksum::Table::open(txn)?.drain_bucket(bucket)?;
    part_stamp::Table::open(txn)?.drain_bucket(bucket)?;
    Ok(())
}

/// Sort uploads by the composite `(key, upload_id)` order the pagination
/// engine requires.
fn sort_uploads(uploads: &mut [MultipartUpload]) {
    uploads.sort_by(|a, b| {
        (a.key.as_ref(), a.upload_id.as_str()).cmp(&(b.key.as_ref(), b.upload_id.as_str()))
    });
}

/// Multipart parts storage of a state dir.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{
///     acl, bucket,
///     multipart::{CompletedPart, part_number},
///     object,
/// };
/// use tinio_fs::multipart;
/// use tinio_util::testing::body;
/// use tokio::{fs, runtime::Runtime};
///
/// let state = tempfile::tempdir().unwrap();
/// let store = multipart::store(state.path()).unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let key = object::key("big.bin").unwrap();
/// Runtime::new().unwrap().block_on(async {
///     let upload = store
///         .create(
///             &bucket,
///             &key,
///             None,
///             object::Tags::empty(),
///             None,
///             &acl::Acl::default_private(None),
///         )
///         .await
///         .unwrap();
///     let part = store
///         .put_part(
///             &bucket,
///             &key,
///             &upload.upload_id,
///             part_number(1).unwrap(),
///             body(b"abc"),
///             None,
///         )
///         .await
///         .unwrap();
///     assert_eq!(u32::from(part.part_number), 1);
///     let target = state.path().join("assembled.bin");
///     let completed = CompletedPart {
///         part_number: part.part_number,
///         etag: part.etag,
///     };
///     let (temp, _etag, _parts) = store
///         .complete(&bucket, &key, &upload.upload_id, &[completed])
///         .await
///         .unwrap();
///     fs::rename(&temp, &target).await.unwrap();
///     assert_eq!(fs::metadata(&target).await.unwrap().len(), 3);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Store {
    /// The shared state-database handle (upload records + part ETags).
    handle: Arc<database::Driver>,
    /// The contract's upload engine over that same handle: every record
    /// read/write goes through it (Task 7's delegation — one home for the
    /// decoded rows, the key check, and the refusal projection).
    meta: Arc<RedbMetadata>,
    /// The cap on concurrently in-progress uploads (`[s3]
    /// max_concurrent_uploads`; default
    /// [`DEFAULT_MAX_CONCURRENT_UPLOADS`]). [`Store::ensure_capacity`]
    /// counts the live `UPLOADS` rows and refuses new uploads at the cap;
    /// both the backend's create and [`Store::create`] run it. A
    /// construction-time input: [`Store::from_handle`] takes it once.
    max_concurrent_uploads: u32,
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
type PartLock = Guard<(String, String)>;

impl Store {
    /// Create a store over a shared state-database handle (the `FsStorage`
    /// construction path — one handle across all stores).
    pub(crate) fn from_handle(
        handle: Arc<database::Driver>,
        state_dir: &Path,
        max_concurrent_uploads: u32,
    ) -> Self {
        Self {
            meta: Arc::new(RedbMetadata::from_driver(handle.shared())),
            handle,
            max_concurrent_uploads,
            root: state_dir.join(MULTIPART_DIR_NAME),
            writer: AtomicWriter::new(state_dir),
            part_locks: PartLocks::new(),
        }
    }

    /// The per-upload part-write lock, held across a part write's rename and
    /// record transaction, so two concurrent same-part overwrites can never
    /// leave the file and the record disagreeing (the last rename and the last
    /// record must describe the same content — `complete` verifies the record,
    /// then re-hashes the file).
    ///
    /// Two callers take it, never nested: this store's own part writes
    /// ([`Store::put_part`], [`Store::put_part_copy`]) under the lock their
    /// `publish_part` takes, and the ops engine's `upload_part` body through
    /// `tinio_ops::Hooks::lock_part`'s guard — which is this call, the
    /// backend's `FsHooks` being the seam. The engine takes the guard **after**
    /// the body has streamed and drops it once the part row is written (E15),
    /// so the two spans are the same critical section, not two.
    pub(crate) async fn part_lock(&self, bucket: &bucket::Name, upload_id: &str) -> PartLock {
        self.part_locks
            .lock((bucket.to_string(), upload_id.to_string()))
            .await
    }

    /// Refuse a new upload when the store already holds
    /// `max_concurrent_uploads` of them (CWE-770): without a cap an
    /// authenticated client can accumulate an unbounded number of uploads,
    /// each holding up to 10,000 part files + `PARTS` rows. The count check is
    /// best-effort atomic (a read snapshot before the insert write);
    /// concurrent creators can overshoot by the number of simultaneous
    /// creates, which the cap tolerates.
    ///
    /// The backend's `create_multipart_upload` runs this before the ops
    /// engine's create body (the cap is this store's policy, not the
    /// engine's), and [`Store::create`] runs it for its own callers — one
    /// home for the rule.
    pub(crate) async fn ensure_capacity(&self) -> Result<(), Error> {
        let cap = self.max_concurrent_uploads;
        let live = self.live_upload_ids().await?.len() as u32;
        if live >= cap {
            return Err(storage::too_many_uploads(cap).into());
        }
        Ok(())
    }

    /// Start a multipart upload (fresh UUID v4 id; the `UPLOADS` record is
    /// committed before the store answers — the upload directory is only
    /// created by the first `put_part`). `checksum` is the create-time
    /// checksum spec, persisted alongside the `UPLOADS` row (spec
    /// 2026-08-31); `tags` is the create-time object tag set, persisted
    /// in the `UPLOADS` row and applied to the completed object (spec
    /// 2026-08-31). The bucket must already exist — callers check
    /// `head_bucket` first.
    ///
    /// The store-level primitive: the backend's `create_multipart_upload` runs
    /// the ops engine's create body instead, behind the same concurrency cap
    /// (`Store::ensure_capacity`). This entry point serves
    /// standalone/embedded callers and this module's own tests.
    pub async fn create(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<MultipartUpload, Error> {
        self.ensure_capacity().await?;
        // Fresh UUID v4 (122 random bits): collisions cannot happen in
        // practice, so a failed insert is a real I/O error.
        let upload = MultipartUpload {
            upload_id: Uuid::new_v4().to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: SystemTime::now(),
            checksum: checksum.clone(),
            tags: tags.clone(),
            owner: owner.cloned(),
            acl: acl.clone(),
        };
        // The contract's `create` writes the UPLOADS row and the checksum
        // spec's row in one transaction (the record layer's home; the
        // blocking-pool dispatch is the driver's, G3).
        self.meta
            .create(
                bucket,
                &upload.upload_id,
                &row::Row {
                    upload_id: upload.upload_id.clone(),
                    key: upload.key.clone(),
                    initiated_at: upload.initiated_at,
                    checksum: upload.checksum.clone(),
                    tags: upload.tags.clone(),
                    owner: upload.owner.clone(),
                    acl: upload.acl.clone(),
                },
            )
            .await
            .map_err(database::map_contract_error)?;
        Ok(upload)
    }

    /// The upload's persisted state (create-time checksum spec included).
    /// `NoSuchUpload` when absent or the key does not match.
    pub async fn get_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        // The contract's `get` does the one key-checked lookup (the decoded
        // row, its checksum spec joined). The facade's identity guard: a
        // client-supplied id may only name a UUID (as `create` allocates)
        // — anything else answers NoSuchUpload rather than a record that
        // no part path can address.
        self.meta
            .get(bucket, key, upload_id)
            .await
            .map_err(database::map_contract_error)?
            .filter(|_| Uuid::parse_str(upload_id).is_ok())
            .map(|row| row.to_upload(bucket))
            .ok_or_else(|| storage::no_such_upload(upload_id).into())
    }

    /// Stream one part (number `1..=10000`) into the upload. `checksum`
    /// is the server's tee slot (spec 2026-08-31): its digest is
    /// persisted in the SAME transaction as the part row (atomic — no
    /// CAS), and the slot's `etag` cell supplies the part ETag.
    /// `NoSuchUpload` when the upload does not exist.
    ///
    /// The store-level primitive, with the store's own per-upload lock: the
    /// backend's `upload_part` runs the ops engine's part body instead, which
    /// stages through the same content plane and takes the **same** lock
    /// through `Hooks::lock_part` (never nested — the two paths never call one
    /// another). This entry point serves standalone/embedded callers and this
    /// module's own tests.
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bucket = %bucket, key = %key)
    )]
    pub async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, Error> {
        // Refuse a nonexistent upload BEFORE any disk write: the body is
        // not streamed and no directory or part file is created (an
        // UploadPart for an aborted/completed upload must not leave
        // residue). The check repeats in the record transaction below —
        // the upload can be consumed while the body streams.
        let _dir = self.require_upload(bucket, key, upload_id).await?;
        // Stream the part body first — a slow client must not stall any
        // other multipart operation. The temp+rename happens after the
        // existence check, so a part file only ever becomes visible whole.
        let (temp, etag, _size) = match self.writer.stage(body, checksum.as_deref()).await {
            Ok(staged) => staged,
            Err(err) => {
                // An abort may have removed the upload mid-stream.
                if self.require_upload(bucket, key, upload_id).await.is_err() {
                    return Err(storage::no_such_upload(upload_id).into());
                }
                return Err(err);
            }
        };
        self.publish_part(bucket, key, upload_id, part_number, temp, etag, checksum)
            .await
    }

    /// UploadPartCopy's part write: stage `len` bytes at `offset` of an
    /// already-open `source` through the writer's copy stage (the
    /// kernel-side `copy_file_range` fast path — no userspace
    /// buffering), then the shared publish. A part's ETag is always the
    /// content MD5 of the part bytes (the staged copy's own hash). The
    /// copy path carries no client checksum (R1) — no tee slot.
    /// Unix-only: the backend's `copy_part` uses the contract's stream
    /// default elsewhere.
    #[cfg(unix)]
    #[expect(
        clippy::too_many_arguments,
        reason = "the copy's own field set — bucket, key, upload id, part number, the staged source, offset and length — the fast path's inputs exactly as the caller holds them"
    )]
    pub(crate) async fn put_part_copy(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        source: StdFile,
        offset: u64,
        len: u64,
    ) -> Result<PartInfo, Error> {
        // Refuse a nonexistent upload BEFORE any disk write (same rule
        // as `put_part` — the check repeats in the record transaction).
        let _dir = self.require_upload(bucket, key, upload_id).await?;
        let (temp, etag) = self.writer.stage_copy(source, offset, len).await?;
        self.publish_part(bucket, key, upload_id, part_number, temp, etag, None)
            .await
    }

    /// The rename + record critical section shared by [`Store::put_part`]
    /// and [`Store::put_part_copy`]: rename the staged temp onto the part
    /// path, then ONE write transaction re-checks the upload exists and
    /// upserts the PARTS row (and the checksum row when the tee slot
    /// holds a digest — the two rows commit atomically, so a re-upload
    /// overwrites both and no CAS is needed) — the file and the record
    /// can never disagree (a concurrent same-part overwrite must never
    /// interleave as rename(A), rename(B), txn(B), txn(A), which would
    /// wedge the upload). A failed record removes the renamed part and
    /// the (empty) directory.
    #[expect(
        clippy::too_many_arguments,
        reason = "the rename-and-record section's field set — bucket, key, upload id, part number, the staged temp, etag and the tee slot — both callers forward them unchanged"
    )]
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bucket = %bucket, key = %key)
    )]
    async fn publish_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        temp: PathBuf,
        etag: ETag,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, Error> {
        let n = u32::from(part_number);
        let dir = self.upload_dir(bucket, upload_id)?;
        // The rename and the PARTS upsert are one critical section (the
        // per-upload lock): a concurrent same-part overwrite must never
        // interleave as rename(A), rename(B), txn(B), txn(A) — the file
        // and the record would disagree and wedge the upload.
        let _lock = self.part_lock(bucket, upload_id).await;
        if let Err(err) = fs::create_dir_all(&dir).await {
            let _ = fs::remove_file(&temp).await;
            let err = Error::from(err);
            return Err(err);
        }
        let part = part_path(&dir, n);
        if let Err(err) = fs::rename(&temp, &part).await {
            let _ = fs::remove_file(&temp).await;
            let err = Error::from(err);
            return Err(err);
        }
        // The contract's `put_part` re-checks the upload INSIDE the
        // transaction that writes the PARTS + PART_CHECKSUMS rows: an
        // upload aborted while the body streamed answers NoSuchUpload
        // and the renamed part is discarded (the empty directory is
        // reclaimed by the orphan stage).
        if let Err(err) = self
            .meta
            .put_part(
                bucket,
                key,
                upload_id,
                part_number,
                &etag,
                checksum.as_deref(),
            )
            .await
        {
            let _ = fs::remove_file(&part).await;
            let _ = fs::remove_dir(&dir).await;
            let err = database::map_contract_error(err);
            return Err(err);
        }
        // The part file's own stat; the digest committed atomically with the
        // part row.
        self.stamped_part(
            bucket,
            upload_id,
            u32::from(part_number),
            etag,
            checksum.as_ref().and_then(|c| c.digest.get()).cloned(),
        )
        .await
    }

    /// List the parts of an upload, in part-number order, paginated
    /// inside the read transaction (raw `PARTS` rows from the exclusive
    /// marker, capped at `max_parts` — the same order the shared engine
    /// uses, at O(page) rows per request).
    ///
    /// The store-level primitive: the backend's `list_parts` runs the ops
    /// engine's page for the identity/marker/resume rules and joins the
    /// stat itself, so the backend's listing and this method answer the same
    /// page. This entry point serves standalone/embedded callers and this
    /// module's own tests.
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
        // The id's mapping is proven before anything else: the page's part
        // files resolve through the same one, per served row, but a page no
        // row reaches resolves none — and a client-supplied id that is not the
        // UUID `create` allocated must answer `NoSuchUpload` either way.
        self.upload_dir(bucket, upload_id)?;
        // The identity check and the page are two contract calls: `get`
        // does the key check (an absent or key-mismatched upload answers
        // NoSuchUpload) and `list_parts_page` does the page — committed
        // `PARTS` rows from the raw lower bound, capped at `max_parts`,
        // plus one lookahead for `truncated`, so a page costs O(page) rows
        // and not a full-upload scan, with the `PART_CHECKSUMS` join the
        // contract owns. This layer keeps only the marker arithmetic (the
        // exclusive marker's `+1`, which must express every raw `u32`: the
        // `u32::MAX` boundary has no successor) and pass 2's file stat.
        // The ETags are parsed by the contract for the emitted page (an
        // invalid page row is skipped — corrupted rows are rare and
        // self-healing; listing is DB-driven and does not fall back to the
        // file, only the point query does, §5.5.7).
        if self
            .meta
            .get(bucket, key, upload_id)
            .await
            .map_err(database::map_contract_error)?
            .is_none()
        {
            return Err(storage::no_such_upload(upload_id).into());
        }
        // The marker is exclusive — but `u32::MAX` has no successor:
        // saturating the start would re-include the marker part itself.
        // (Unreachable via S3's 1..=10000 range; this layer does not rely on
        // that.) `max_parts = 0` requests nothing, and no marker either (an
        // exclusive-after marker would skip the first part of the next page
        // forever) — the contract answers that empty, untruncated page.
        let Some(start) = marker.map_or(Some(0), |m| m.checked_add(1)) else {
            return Ok((Vec::new(), false, None));
        };
        let page = self
            .meta
            .list_parts_page(bucket, upload_id, start, max_parts)
            .await
            .map_err(database::map_contract_error)?;
        // The resume marker: the last row the page SERVED (before pass-2
        // filtering) — a page that survives filtering is the same thing,
        // and a truncated page whose parts all vanished still advances
        // the client past them. A truncated page that served *nothing* has
        // no served row to name: every raw row of it carried a
        // domain-invalid ETag and self-healed away, so the contract's raw
        // last number is the marker then. That page is unreachable through
        // storage's own writes (they validate ETags) — an out-of-band
        // tamper — but a markerless truncated page would send the client
        // back to the same page forever, so the fallback rides here.
        let served_last = page.rows.last().map(|row| row.number);
        let raw_last = if page.more && served_last.is_none() {
            page.raw_last
        } else {
            served_last
        };
        // Pass 2: size/mtime from the part files of this page only. A part
        // that vanished between the passes (a concurrent abort) is skipped
        // rather than failing the listing.
        let mut parts = Vec::with_capacity(page.rows.len());
        for row in page.rows {
            match self
                .stamped_part(bucket, upload_id, row.number, row.etag, row.checksum)
                .await
            {
                Ok(part) => parts.push(part),
                Err(Error::Io(err)) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            }
        }
        Ok((parts, page.more, raw_last))
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
            .read(|txn| {
                part::Table::open_readonly(txn)?
                    .get_hex(bucket, upload_id, n)
                    .map_err(Into::into)
            })
            .map_err(Error::from)?;
        if let Some(hex) = stored {
            // A domain-invalid record: fall through to the recompute.
            if let Ok(etag) = hex.parse::<ETag>() {
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
    /// - empty `parts` → `Error::NoParts`;
    /// - missing / mismatched / out-of-order part → `InvalidPart`;
    /// - a non-final listed part below
    ///   [`tinio_core::multipart::MIN_PART_BYTES`] → `PartTooSmall`
    ///   (the authoritative S3 5 MiB rule — the S3 layer pre-checks its
    ///   own listing snapshot, but a part overwritten between that listing
    ///   and this verify loop is caught here);
    /// - a part file vanishing mid-verify/assembly when the upload was
    ///   consumed (a concurrent abort/sweep) → `NoSuchUpload` (§5.6);
    /// - the upload's recorded key differs from `key` → `NoSuchUpload`.
    ///
    /// Returns the temp path, the composed ETag (`MD5-of-MD5s-N`), and
    /// the assembled parts' rows — `CompletedPartRow`s in completion
    /// order — for the caller to persist into `OBJECT_PARTS` (the
    /// completed object's retained part list, spec 2026-08-31; the
    /// checksum row is the stored `PART_CHECKSUMS` row of each part:
    /// `None` when the part was uploaded without one).
    pub async fn complete(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<(PathBuf, ETag, Vec<CompletedPartRow>), Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        if parts.is_empty() {
            self.require_upload(bucket, key, upload_id).await?;
            return Err(storage::no_parts().into());
        }
        // The contract's completion-time join: the upload's key check AND
        // the requested parts' PARTS + PART_CHECKSUMS rows in one read
        // transaction (the rows commit atomically with the part rows, so
        // the join is consistent with the verified ETag). `None` is the
        // key mismatch.
        let numbers: Vec<PartNumber> = parts.iter().map(|part| part.part_number).collect();
        let Some(joined) = self
            .meta
            .part_records(bucket, key, upload_id, &numbers)
            .await
            .map_err(database::map_contract_error)?
        else {
            return Err(storage::no_such_upload(upload_id).into());
        };
        // The verify loop's point map: `n -> (stored ETag, checksum row)`;
        // a part with no PARTS row (or a domain-invalid stored ETag the
        // join dropped) reads as absent — the crash-window recompute
        // fallback, §5.6.
        let records: HashMap<u32, (ETag, Option<checksum::Part>)> = numbers
            .iter()
            .zip(joined)
            .filter_map(|(n, record)| {
                record.map(|record| (u32::from(*n), (record.etag, record.checksum)))
            })
            .collect();
        // Verify: strictly ascending, each part exists with a matching
        // ETag (the `PARTS` row, or the file recompute when the row is
        // missing — the crash-window fallback, §5.6).
        let mut infos = Vec::with_capacity(parts.len());
        let mut part_rows = Vec::with_capacity(parts.len());
        let mut last = 0u32;
        for (index, part) in parts.iter().enumerate() {
            let n = u32::from(part.part_number);
            if n <= last {
                return Err(storage::invalid_part(n).into());
            }
            last = n;
            // A missing part file (NotFound) is an InvalidPart — unless
            // a concurrent abort/sweep consumed the upload (then
            // NoSuchUpload, §5.6).
            let path = part_path(&dir, n);
            let metadata = match fs::metadata(&path).await {
                Ok(metadata) if !metadata.is_dir() => metadata,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    return Err(self.vanished_part(bucket, key, upload_id, n).await);
                }
                Ok(_) | Err(_) => return Err(storage::invalid_part(n).into()),
            };
            // The S3 non-final minimum (shared `check_part_minimum`),
            // enforced authoritatively against the file the assembly
            // below will compose (a part overwritten since the S3
            // layer's listing snapshot is caught here, not by the
            // pre-check). A part shrunk between this metadata read and
            // the assembly copy fails the assembly's re-hash →
            // InvalidPart — never an undersized commit.
            check_part_minimum(n, metadata.len(), index + 1 == parts.len())?;
            let stored = match records.get(&n).map(|(etag, _)| etag.clone()) {
                // A domain-invalid record (or no record — the crash-window
                // fallback): recompute from the file, §5.6.
                None => match md5_of_file(&path).await {
                    Ok((digest, _)) => ETag::Single(digest),
                    Err(Error::Io(err)) if err.kind() == ErrorKind::NotFound => {
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
                checksum: None,
            });
            // The assembled part's OBJECT_PARTS row: the verified size
            // and the joined checksum row (the upload's drain removes the
            // `PART_CHECKSUMS` rows at completion — the retained list
            // must be captured before that).
            part_rows.push(object_part::Stored {
                part_number: n,
                size: metadata.len(),
                checksum: records.get(&n).and_then(|(_, c)| c.clone()),
            });
        }
        // Assemble before consuming the upload (part files stay readable
        // until the delete). The bytes copied are hashed in the same
        // pass — the verify-then-copy race closes here: a concurrent
        // put_part overwriting a part mid-assembly yields copied bytes
        // that disagree with the verified ETag and fails the completion
        // (no post-assembly re-read of the parts, §5.6).
        let tmp_dir = self.writer.tmp_dir();
        fs::create_dir_all(tmp_dir).await?;
        let temp = tmp_dir.join(format!("multipart-{}", Uuid::new_v4()));
        let assemble = async {
            let mut out = File::create(&temp).await?;
            let mut buf = vec![0u8; CHUNK_SIZE];
            for info in &infos {
                let n = u32::from(info.part_number);
                let mut file = match File::open(part_path(&dir, n)).await {
                    Ok(file) => file,
                    // A part can vanish between the verify pass and the
                    // copy (a concurrent abort/sweep consumed the upload,
                    // or out-of-band removal): classify via UPLOADS (§5.6).
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        return Err(self.vanished_part(bucket, key, upload_id, n).await);
                    }
                    Err(err) => return Err(err.into()),
                };
                let mut hasher = Md5::new();
                loop {
                    let read = file.read(&mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    out.write_all(&buf[..read]).await?;
                    hasher.update(&buf[..read]);
                }
                if ETag::Single(hasher.finalize().into()) != info.etag {
                    return Err(Error::from(storage::invalid_part(n)));
                }
            }
            out.flush().await?;
            // D1 — content durability: the assembled object's bytes must
            // be on disk before `AtomicWriter::commit` renames it (the
            // rename's directory entry is synced by commit).
            out.sync_all().await?;
            Ok::<_, Error>(())
        }
        .await;
        if let Err(err) = assemble {
            let _ = fs::remove_file(&temp).await;
            // A concurrent abort/sweep may have consumed the upload while
            // the parts streamed — report that accurately (the upload is
            // gone either way) instead of a raw I/O error. Only the
            // not-found upload maps; a database failure must not be
            // masked as NoSuchUpload.
            if matches!(&err, Error::Io(io_err) if io_err.kind() == ErrorKind::NotFound)
                && matches!(
                    self.require_upload(bucket, key, upload_id).await,
                    Err(Error::Storage(NoSuchUpload(_)))
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
        Ok((temp, etag, part_rows))
    }

    /// Delete an upload's `UPLOADS`/`UPLOAD_CHECKSUMS` + `PARTS`/
    /// `PART_CHECKSUMS` records in one transaction, then remove its
    /// directory best-effort. Called by the backend AFTER
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
        ok_if_missing(fs::remove_dir_all(&dir).await)?;
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
        ok_if_missing(fs::remove_dir_all(&dir).await)?;
        Ok(())
    }

    /// Delete an upload's `UPLOADS`/`UPLOAD_CHECKSUMS` + `PARTS`/
    /// `PART_CHECKSUMS` rows (no directory removal, no UUID check). Cleanup uses this on orphan directories whose
    /// names are not necessarily upload ids.
    pub(crate) async fn drain_upload_rows(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), Error> {
        self.meta
            .drain_upload(bucket, upload_id)
            .await
            .map_err(database::map_contract_error)
    }

    /// Abort an upload — one transaction, then the best-effort directory
    /// removal. The contract's `drain_upload_checked` does the identity
    /// (key) check and the `UPLOADS` / `UPLOAD_CHECKSUMS` + `PARTS` /
    /// `PART_CHECKSUMS` deletion in **one transaction**, so a racing
    /// complete cannot slip between the check and the removal; an absent
    /// or key-mismatched upload projects to `NoSuchUpload` and nothing is
    /// drained.
    ///
    /// The store-level primitive: the backend's `abort_multipart_upload` runs
    /// the ops engine's abort body (the same checked drain) and then this
    /// module's best-effort directory removal. This entry point serves
    /// standalone/embedded callers and this module's own tests.
    pub async fn abort(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        // The dir path (UUID-validated) is needed for the best-effort
        // removal — resolved before the drain (a non-UUID id is not an
        // upload at all) and used after it: the records are gone either
        // way, so a failed removal is residue the orphan stage reclaims,
        // never a client-visible error.
        let dir = self.upload_dir(bucket, upload_id)?;
        self.meta
            .drain_upload_checked(bucket, key, upload_id)
            .await
            .map_err(database::map_contract_error)?;
        let _ = fs::remove_dir_all(&dir).await;
        Ok(())
    }

    /// Every in-progress upload of a bucket, in `(key, upload_id)` order
    /// — the composite order the pagination engine requires, so a page
    /// can resume inside a same-key group (from the `UPLOADS` records).
    /// The full-bucket materialization remains for tests/standalone use;
    /// the backend's `list_multipart_uploads` pages through
    /// [`Self::list_uploads_page`] instead (item 7e).
    pub async fn list_uploads(&self, bucket: &bucket::Name) -> Result<Vec<MultipartUpload>, Error> {
        let mut uploads = self
            .meta
            .list_uploads(bucket)
            .await
            .map_err(database::map_contract_error)?;
        // The facade's identity guard (see [`Self::get_upload`]): only a
        // UUID id can name a part path.
        uploads.retain(|upload| Uuid::parse_str(&upload.upload_id).is_ok());
        sort_uploads(&mut uploads);
        Ok(uploads)
    }

    /// One page of the bucket's in-progress uploads per the S3
    /// ListMultipartUploads semantics. The record materialization and the
    /// pagination are the contract's (`UploadMeta::list_uploads_page`):
    /// one short read transaction on the blocking pool, then the shared
    /// unordered engine (F18/F35 — the marker, rollup, truncation, and
    /// resume rules have one home).
    ///
    /// `marker` is the composite `key\0upload_id` order (the backend
    /// builds it from the S3 `key-marker`/`upload-id-marker` pair; a bare
    /// key marker uses the sentinel upload id). `max_uploads = 0` returns
    /// an empty, untruncated page with no marker. Returns `(uploads,
    /// common_prefixes, truncated, next)` — `next` is the composite
    /// resume marker when truncated.
    ///
    /// The facade's identity guard (see [`Self::get_upload`]) is applied
    /// to the page the contract served: only a UUID id can name a part
    /// path, and the contract's decode deliberately does not validate the
    /// id, so a tampered or corrupt `UPLOADS` row is dropped here — the
    /// same rule the two unpaged listings apply.
    ///
    /// The guard drops entries from the served page, **after** the
    /// contract's marker arithmetic: `truncated`/`next` remain the raw
    /// page's. The resume protocol survives that because `next` is the raw
    /// page's last composite order and the marker is exclusive-after —
    /// every valid row at or below `next` was served (the page holds the
    /// `max` smallest entries after the marker), so a client resuming on
    /// `next` can neither re-see a served upload nor skip a live one. What
    /// it can see is a page shorter than `max_uploads` (in principle
    /// empty) while `truncated` is true; the next request then advances
    /// past `next` as usual. Filtering *before* the arithmetic would mean
    /// materializing only UUID rows, which is the contract's business, not
    /// this facade's.
    pub async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), Error> {
        let (mut uploads, common_prefixes, truncated, next) = self
            .meta
            .list_uploads_page(bucket, prefix, delimiter, marker, max_uploads)
            .await
            .map_err(database::map_contract_error)?;
        uploads.retain(|upload| Uuid::parse_str(&upload.upload_id).is_ok());
        Ok((uploads, common_prefixes, truncated, next))
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
        match fsutil::latest_part_mtime(&dir).await {
            // `UNIX_EPOCH` when no parts exist yet (or the dir is gone).
            Ok(latest) => Ok(latest.unwrap_or(SystemTime::UNIX_EPOCH)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(SystemTime::UNIX_EPOCH),
            Err(err) => Err(err.into()),
        }
    }

    /// Whether a bucket has any in-progress upload (bucket-delete check:
    /// in-progress uploads make the bucket non-empty).
    pub async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        self.meta
            .has_uploads(bucket)
            .await
            .map_err(database::map_contract_error)
    }

    /// Remove the whole multipart state of a bucket: one transaction
    /// deletes its `UPLOADS`/`UPLOAD_CHECKSUMS` + `PARTS`/`PART_CHECKSUMS`
    /// records, then the directory subtree
    /// is removed best-effort. Test-only since the production teardown
    /// goes through [`FsStorage::remove_bucket_state`].
    #[cfg(test)]
    pub async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let dir = self.root.join(&**bucket);
        self.meta
            .drain_bucket(bucket)
            .await
            .map_err(database::map_contract_error)?;
        ok_if_missing(fs::remove_dir_all(&dir).await)?;
        Ok(())
    }

    /// Upload records of every bucket, in `(key, upload_id)` order — the
    /// sweep's idle-expiry walk.
    pub async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, Error> {
        let mut uploads = self
            .meta
            .walk_uploads()
            .await
            .map_err(database::map_contract_error)?;
        // The facade's identity guard (see [`Self::get_upload`]).
        uploads.retain(|upload| Uuid::parse_str(&upload.upload_id).is_ok());
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
                let table = upload::Table::open_readonly(txn)?;
                let mut ids = HashSet::new();
                // Membership only: tags/key are ignored — a live upload
                // whose stored key fails validation must still count.
                table.for_each(|bucket, upload_id, _, _, _, _, _| {
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
        let bucket = bucket.clone();
        let upload_id = upload_id.to_string();
        let key = key.to_string();
        self.handle
            .write(move |txn| {
                upload::Table::open(txn)?
                    .insert(
                        (&*bucket, upload_id.as_str()),
                        (key.as_str(), 0, "", "", ""),
                    )
                    .map_err(|e| database::Error::Redb(e.into()))?;
                Ok(())
            })
            .await
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
        if self
            .meta
            .get(bucket, key, upload_id)
            .await
            .map_err(database::map_contract_error)?
            .is_some()
        {
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
            Err(Error::Storage(NoSuchUpload(_))) => storage::no_such_upload(upload_id).into(),
            Err(err) => err,
        }
    }

    /// `<state-dir>/multipart/<bucket>/<upload_id>` — the id is
    /// client-supplied, so only UUIDs (as `create` allocates) may map to a
    /// state-dir path; anything else (e.g. `../`) answers `NoSuchUpload`.
    fn upload_dir(&self, bucket: &bucket::Name, upload_id: &str) -> Result<PathBuf, Error> {
        if Uuid::parse_str(upload_id).is_err() {
            return Err(storage::no_such_upload(upload_id).into());
        }
        Ok(self.root.join(&**bucket).join(upload_id))
    }

    /// One part as this backend serves it: the part's identity (number, ETag,
    /// checksum) joined with the **part file's own** stat — a part's size and
    /// mtime are its file's, never the engine's counted length or wall clock
    /// (the contract carries no part stat, D23).
    ///
    /// That mtime **is** the part's own write stamp, which is what a listed
    /// part answers as `LastModified`
    /// ([`tinio_core::meta::upload::PartRow::written_at`]'s rule): the part is
    /// the file, so the instant the store last wrote it is the file's own, and
    /// no upload-level instant stands in for it. The metadata plane records no
    /// per-part stamp, so this join is the whole of the answer.
    ///
    /// A missing part file (`NotFound`) is the caller's to classify: a
    /// listing skips such a part (it vanished between the row read and this
    /// join, and a stored number an S3 page cannot carry must not fail a
    /// listing whose rows are gone anyway), while the point paths, which just
    /// wrote the file, propagate the error.
    pub(crate) async fn stamped_part(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        number: u32,
        etag: ETag,
        checksum: Option<checksum::Part>,
    ) -> Result<PartInfo, Error> {
        let path = self.part_file(bucket, upload_id, number)?;
        let metadata = fs::metadata(&path).await?;
        Ok(PartInfo {
            part_number: part_number(number)?,
            size: metadata.len(),
            last_modified: metadata.modified()?,
            etag,
            checksum,
        })
    }

    /// The content plane's part mapping: the same layout every multipart
    /// call uses (`<state>/multipart/<bucket>/<upload_id>/<n>`, the upload
    /// id validated as the UUID the create allocated — a client-supplied
    /// id never reaches the filesystem unvalidated).
    pub(crate) fn part_file(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        number: u32,
    ) -> Result<PathBuf, Error> {
        Ok(part_path(&self.upload_dir(bucket, upload_id)?, number))
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
        Driver::new(database::open(state_dir)?.db),
        state_dir,
        DEFAULT_MAX_CONCURRENT_UPLOADS,
    ))
}

#[cfg(test)]
mod tests {
    use prop::collection;
    use proptest::prelude::*;

    use super::*;
    use crate::{
        _core::{
            ETag,
            acl::Acl,
            checksum::Algorithm,
            multipart::{self, MIN_PART_BYTES},
            storage::{Error as StorageError, group_and_paginate_ordered, uploads_order},
        },
        _util::testing::body,
        testutil,
    };

    fn fixture() -> (tempfile::TempDir, Store) {
        let state = tempfile::tempdir().unwrap();
        let store = store(state.path()).unwrap();
        (state, store)
    }

    /// A store opened with `cap` as its upload cap: the cap is a
    /// construction-time input (the `FsStorage` path passes
    /// `FsOptions::max_concurrent_uploads` through this same constructor), so
    /// a test that needs a non-default cap supplies it here.
    fn fixture_with_cap(cap: u32) -> (tempfile::TempDir, Store) {
        let state = tempfile::tempdir().unwrap();
        let store = Store::from_handle(
            Driver::new(database::open(state.path()).unwrap().db),
            state.path(),
            cap,
        );
        (state, store)
    }

    #[tokio::test]
    async fn create_refuses_uploads_at_the_concurrency_cap() {
        let (_, store) = fixture_with_cap(1);
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();

        let err = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Storage(StorageError::TooManyMultipartUploads { limit: 1 })
            ),
            "second create must hit the cap, got {err:?}"
        );

        // A completed upload frees a slot (the count reads live rows).
        let uploads = store.list_uploads(&b).await.unwrap();
        assert_eq!(uploads.len(), 1);
    }

    #[tokio::test]
    async fn uploads_page_matches_the_engine_over_the_full_bucket() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let keys = ["a.txt", "dir/x.bin", "dir/sub/y.bin", "dir/z.bin", "z.txt"];
        let mut uploads = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            for _ in 0..(i % 3 + 1) {
                uploads.push(
                    store
                        .create(
                            &b,
                            &object::key(*key).unwrap(),
                            None,
                            object::Tags::empty(),
                            None,
                            &Acl::default_private(None),
                        )
                        .await
                        .unwrap(),
                );
            }
        }
        // The old path: the full sorted load + the shared engine.
        async fn engine_page(
            store: &Store,
            b: &bucket::Name,
            prefix: &str,
            delim: Option<&str>,
            marker: Option<&str>,
            max: usize,
        ) -> (Vec<MultipartUpload>, Vec<String>, bool, Option<String>) {
            // The old backend filtered by prefix before the engine
            // (the engine uses the prefix only for rollups).
            let all = store
                .list_uploads(b)
                .await
                .unwrap()
                .into_iter()
                .filter(|u| u.key.starts_with(prefix))
                .collect::<Vec<_>>();
            group_and_paginate_ordered(
                all,
                prefix,
                delim,
                marker,
                max,
                |u| u.key.as_ref(),
                |u| uploads_order(&u.key, &u.upload_id),
            )
        }
        // Markers: inside a same-key group, and a bare key marker
        // (the sentinel upload id).
        let inside = uploads_order(keys[0], &uploads[0].upload_id);
        let bare = uploads_order(keys[3], "\u{10FFFF}");
        let mut combos = 0usize;
        for (prefix, delim) in [
            ("", None),
            ("", Some("/")),
            ("dir/", Some("/")),
            ("z", None),
        ] {
            for marker in [
                None,
                Some("a.txt"),
                Some(inside.as_str()),
                Some(bare.as_str()),
            ] {
                for max in [0usize, 1, 2, 3, 1000] {
                    combos += 1;
                    let (u1, p1, t1, n1) =
                        engine_page(&store, &b, prefix, delim, marker, max).await;
                    let (u2, p2, t2, n2) = store
                        .list_uploads_page(&b, prefix, delim, marker, max)
                        .await
                        .unwrap();
                    fn ids(uploads: &[MultipartUpload]) -> Vec<(&str, &str)> {
                        uploads
                            .iter()
                            .map(|u| (u.key.as_ref().as_str(), u.upload_id.as_str()))
                            .collect()
                    }
                    assert_eq!(
                        (ids(&u1), p1, t1, n1),
                        (ids(&u2), p2, t2, n2),
                        "prefix={prefix:?} delim={delim:?} marker={marker:?} max={max}"
                    );
                }
            }
        }
        assert_eq!(combos, 80, "the matrix ran");
    }

    /// A tampered `UPLOADS` row whose id is not a UUID never reaches the
    /// listings: only a UUID id can name a part path, so the facade drops
    /// it — the paged listing exactly as the two unpaged ones. The row here
    /// is a direct database write (the API can never produce one).
    #[tokio::test]
    async fn the_paged_listing_drops_a_non_uuid_upload_row() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let live = store
            .create(
                &b,
                &object::key("a.txt").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let tampered = "../../tampered";
        {
            let bucket = b.clone();
            let tampered = tampered.to_string();
            store
                .handle
                .write(move |txn| {
                    upload::Table::open(txn)?
                        .insert((&*bucket, tampered.as_str()), ("a.txt", 0, "", "", ""))
                        .map_err(|e| database::Error::Redb(e.into()))?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        let listed = store.list_uploads(&b).await.unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|upload| upload.upload_id.as_str())
                .collect::<Vec<_>>(),
            [live.upload_id.as_str()],
            "the unpaged listing drops the tampered row"
        );
        let (page, _, truncated, _) = store
            .list_uploads_page(&b, "", None, None, 100)
            .await
            .unwrap();
        assert!(!truncated);
        assert_eq!(
            page.iter()
                .map(|upload| upload.upload_id.as_str())
                .collect::<Vec<_>>(),
            [live.upload_id.as_str()],
            "and so does the paged one"
        );
    }

    #[tokio::test]
    async fn marker_inside_a_rollup_absorbs_the_group() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        store
            .create(
                &b,
                &object::key("dir/a.txt").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .create(
                &b,
                &object::key("dir/c.txt").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .create(
                &b,
                &object::key("z.txt").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let marker = uploads_order("dir/a.txt", "\u{10FFFF}");
        let (uploads, prefixes, truncated, next) = store
            .list_uploads_page(&b, "", Some("/"), Some(&marker), 1000)
            .await
            .unwrap();
        let keys: Vec<&str> = uploads.iter().map(|u| u.key.as_ref().as_str()).collect();
        assert_eq!(keys, ["z.txt"], "{keys:?}");
        assert_eq!(prefixes, Vec::<String>::new(), "{prefixes:?}");
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[tokio::test]
    async fn create_records_upload_without_creating_directory() {
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        assert!(!upload.upload_id.is_empty());
        // The record is DB-driven; the directory appears only with the
        // first part (the orphan-cleanup TOCTOU order depends on it).
        let uploads = store.list_uploads(&b).await.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].upload_id, upload.upload_id);
        assert!(
            fs::metadata(state.path().join("multipart/data").join(&upload.upload_id))
                .await
                .is_err(),
            "create must not create the upload directory"
        );
    }

    #[tokio::test]
    async fn abort_drains_the_checksum_rows() {
        // F1 regression: `abort`'s inlined drain must remove the
        // `UPLOAD_CHECKSUMS` + `PART_CHECKSUMS` rows too — the shared
        // `drain_upload` helper drains them; the inlined copy was
        // missing them (orphaned rows on every fs abort).
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                Some(checksum::Upload {
                    algorithm: Algorithm::Crc32,
                    r#type: None,
                }),
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let slot = Arc::new(checksum::PartChecksum::default());
        let _ = slot.digest.set(checksum::Part {
            algorithm: Algorithm::Crc32,
            value: checksum::Value("y/Q5Jg==".into()),
        });
        store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"x"),
                Some(slot),
            )
            .await
            .unwrap();
        store.abort(&b, &k, &upload.upload_id).await.unwrap();
        let b2 = b.clone();
        let id = upload.upload_id.clone();
        let (upload_row, part_row) = store
            .handle
            .read(move |txn| {
                let u =
                    upload_checksum::Table::open_readonly(txn)?.get(b2.as_ref().as_str(), &id)?;
                let p =
                    part_checksum::Table::open_readonly(txn)?.get(b2.as_ref().as_str(), &id, 1)?;
                Ok((u, p))
            })
            .unwrap();
        assert!(
            upload_row.is_none() && part_row.is_none(),
            "abort must drain the checksum rows, got {upload_row:?} / {part_row:?}"
        );
    }

    #[tokio::test]
    async fn list_parts_joins_a_stored_checksum() {
        // The probe (F03) finds the upload's row and joins it — a part
        // with a stored PART_CHECKSUMS entry comes back with its checksum.
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let slot = Arc::new(checksum::PartChecksum::default());
        let _ = slot.digest.set(checksum::Part {
            algorithm: Algorithm::Crc32,
            value: checksum::Value("y/Q5Jg==".into()),
        });
        store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"x"),
                Some(slot),
            )
            .await
            .unwrap();
        let (parts, _, _) = store
            .list_parts(&b, &k, &upload.upload_id, None, 10)
            .await
            .unwrap();
        assert_eq!(
            parts[0].checksum.as_ref().map(|c| c.algorithm),
            Some(Algorithm::Crc32),
            "the stored checksum joins"
        );
    }

    /// The **served** part stamp is the part file's own write instant, never
    /// the upload's creation time.
    ///
    /// The shared suite's `assert_own_part_stamps` pins the row's recorded
    /// stamp, and it runs over `Metadata` implementations; fs serves a listed
    /// part's `LastModified` from the **part file's** stat instead
    /// ([`Store::stamped_part`]), so this half is only assertable here, against
    /// the value the backend actually serves.
    ///
    /// The file's stamp is planted in 2023 — before the upload was created —
    /// so an answer taken from the upload row's `initiated_at` cannot pass
    /// (it would be ≥ the creation instant), and a fresh stat of the same file
    /// pins **which** stamp was served rather than merely bounding it.
    #[tokio::test]
    async fn a_listed_part_serves_its_own_file_stamp_not_the_uploads_creation_time() {
        // The std handle under the name the module already uses for it, so a
        // reader never has to ask which `File` this is.
        use std::{fs::File as StdFile, time::Duration};

        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let created = SystemTime::now();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"x"),
                None,
            )
            .await
            .unwrap();

        let path = store.part_file(&b, &upload.upload_id, 1).unwrap();
        let written = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        StdFile::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(written)
            .unwrap();
        let on_disk = fs::metadata(&path).await.unwrap().modified().unwrap();
        assert!(
            on_disk < created,
            "the planted stamp predates the upload, so the upload's instant cannot be the served one"
        );

        let (parts, _, _) = store
            .list_parts(&b, &k, &upload.upload_id, None, 10)
            .await
            .unwrap();
        assert_eq!(parts.len(), 1, "the upload holds exactly one part");
        assert_eq!(
            parts[0].last_modified, on_disk,
            "a listed part serves the part file's own write stamp"
        );
        assert!(
            parts[0].last_modified < created,
            "…and never the upload's creation instant"
        );
    }

    #[tokio::test]
    async fn complete_retry_after_rename_is_idempotent() {
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let part = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"x"),
                None,
            )
            .await
            .unwrap();
        let completed = [CompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
        }];
        let target = state.path().join("out.bin");
        // First attempt: the rename lands, the consume does not
        // (the crash). The upload is still listed.
        let (temp, _, _) = store
            .complete(&b, &k, &upload.upload_id, &completed)
            .await
            .unwrap();
        fs::rename(&temp, &target).await.unwrap();
        assert!(store.has_uploads(&b).await.unwrap());
        // Retry: verify + assemble + rename + consume all succeed.
        let (temp, _, _) = store
            .complete(&b, &k, &upload.upload_id, &completed)
            .await
            .unwrap();
        fs::rename(&temp, &target).await.unwrap();
        store.consume(&b, &upload.upload_id).await.unwrap();
        assert!(!store.has_uploads(&b).await.unwrap());
        assert_eq!(fs::read(&target).await.unwrap(), b"x");
    }

    #[tokio::test]
    async fn consume_is_idempotent_for_a_missing_upload() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // No part dir was ever created; consuming twice is a no-op.
        store.consume(&b, &upload.upload_id).await.unwrap();
        store.consume(&b, &upload.upload_id).await.unwrap();
        assert!(!store.has_uploads(&b).await.unwrap());
    }

    #[tokio::test]
    async fn racing_complete_and_abort_leave_a_consistent_state() {
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let mut completed = Vec::new();
        for i in 1..=4u32 {
            // Non-final listed parts must be >= the 5 MiB minimum (the
            // authoritative verify-loop check); the final part may stay
            // small.
            let size = if i == 4 {
                32 * 1024
            } else {
                MIN_PART_BYTES as usize
            };
            let data = vec![i as u8; size];
            let part = store
                .put_part(
                    &b,
                    &k,
                    &upload.upload_id,
                    multipart::part_number(i).expect("a valid part number"),
                    body(data),
                    None,
                )
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
        if let Ok((temp, _, _)) = complete {
            let target = state.path().join("out.bin");
            fs::rename(&temp, &target).await.unwrap();
            store.consume(&b, &upload.upload_id).await.unwrap();
        } else {
            assert!(abort.is_ok());
        }
        // Whatever won, no records remain and a late abort answers
        // NoSuchUpload.
        assert!(!store.has_uploads(&b).await.unwrap());
        let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
        assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
    }

    #[tokio::test]
    async fn list_parts_skips_a_part_whose_file_vanished() {
        // A part file can disappear under a concurrent abort (or out-of-
        // band): the listing skips it instead of failing the whole page.

        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let p1 = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"x"),
                None,
            )
            .await
            .unwrap();
        let p2 = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(2).expect("a valid part number"),
                body(b"yy"),
                None,
            )
            .await
            .unwrap();
        let part_dir = state.path().join("multipart/data").join(&upload.upload_id);
        // Remove part 1's file out-of-band (the record stays).
        fs::remove_file(part_dir.join("part-1")).await.unwrap();
        let (parts, truncated, _) = store
            .list_parts(&b, &k, &upload.upload_id, None, 100)
            .await
            .unwrap();
        assert!(!truncated);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, p2.part_number);
        assert_eq!(parts[0].etag, p2.etag);
        let _ = p1;
    }

    #[tokio::test]
    async fn list_parts_skips_a_part_with_invalid_stored_etag() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"x"),
                None,
            )
            .await
            .unwrap();
        let bucket = b.clone();
        let upload_id = upload.upload_id.clone();
        store
            .handle
            .write(move |txn| {
                let mut parts = part::Table::open(txn).unwrap();
                parts
                    .insert((&*bucket, upload_id.as_str(), 1u32), "not-an-etag")
                    .map_err(|e| database::Error::Redb(e.into()))?;
                Ok(())
            })
            .await
            .unwrap();
        let (parts, _, _) = store
            .list_parts(&b, &k, &upload.upload_id, None, 100)
            .await
            .unwrap();
        assert!(parts.is_empty(), "{parts:?}");
        let etag = store.part_etag(&b, &upload.upload_id, 1).await.unwrap();
        assert_eq!(etag, ETag::from_content(b"x"));
    }

    #[tokio::test]
    async fn list_uploads_and_has_uploads() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k1 = object::key("a.bin").unwrap();
        let k2 = object::key("b.bin").unwrap();
        let u1 = store
            .create(
                &b,
                &k1,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let u2 = store
            .create(
                &b,
                &k2,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let uploads = store.list_uploads(&b).await.unwrap();
        assert_eq!(uploads.len(), 2);
        assert!(store.has_uploads(&b).await.unwrap());
        store.abort(&b, &k1, &u1.upload_id).await.unwrap();
        store.abort(&b, &k2, &u2.upload_id).await.unwrap();
        assert!(!store.has_uploads(&b).await.unwrap());
    }

    #[tokio::test]
    async fn list_uploads_orders_same_key_group_by_upload_id() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("same.bin").unwrap();
        store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let uploads = store.list_uploads(&b).await.unwrap();
        assert_eq!(uploads.len(), 2);
        assert!(
            uploads[0].upload_id < uploads[1].upload_id,
            "same-key uploads must be ordered by upload id: {:?}",
            uploads.iter().map(|u| &u.upload_id).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn remove_bucket_clears_uploads() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store.remove_bucket(&b).await.unwrap();
        assert!(!store.has_uploads(&b).await.unwrap());
        let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
        assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
    }

    #[tokio::test]
    async fn walk_uploads_finds_all() {
        let (_, store) = fixture();
        let b1 = bucket::name("alpha").unwrap();
        let b2 = bucket::name("zeta").unwrap();
        store
            .create(
                &b1,
                &object::key("a.bin").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .create(
                &b2,
                &object::key("z.bin").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let uploads = store.walk_uploads().await.unwrap();
        assert_eq!(uploads.len(), 2);
    }

    #[tokio::test]
    async fn list_parts_truncated_page_with_vanished_parts_still_marks_resume() {
        // A truncated page whose parts all vanished in pass 2 must still
        // report a resume marker (from the raw rows) — an empty page with
        // truncated=true and no marker would make the client re-request
        // the identical page forever.

        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        for n in 1..=4u32 {
            store
                .put_part(
                    &b,
                    &k,
                    &upload.upload_id,
                    multipart::part_number(n).expect("a valid part number"),
                    body(b"x"),
                    None,
                )
                .await
                .unwrap();
        }
        let dir = state.path().join("multipart/data").join(&upload.upload_id);
        // Remove the first two part files out-of-band (records stay).
        fs::remove_file(dir.join("part-1")).await.unwrap();
        fs::remove_file(dir.join("part-2")).await.unwrap();
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
    }

    #[tokio::test]
    async fn part_lock_slots_are_evicted_after_use() {
        // The per-upload lock map must not grow: every `put_part` releases
        // its slot when the last handle drops (a leak would be unbounded
        // memory on a long-running server with churning uploads).

        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"x"),
                None,
            )
            .await
            .unwrap();
        assert!(
            store.part_locks.is_empty(),
            "lock slots must be evicted after use"
        );
    }

    #[tokio::test]
    async fn concurrent_same_part_overwrites_never_mismatch_file_and_record() {
        // Two put_parts of the same part interleaving rename/txn must end
        // with the file and the PARTS record agreeing (the per-upload lock
        // serializes rename + upsert) — otherwise complete verifies the
        // record, then re-hashes the file and fails InvalidPart forever.

        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let id = upload.upload_id.clone();
        let (a, b2) = tokio::join!(
            store.put_part(
                &b,
                &k,
                &id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"AAAA"),
                None
            ),
            store.put_part(
                &b,
                &k,
                &id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"BBBB"),
                None
            ),
        );
        a.unwrap();
        b2.unwrap();
        let (parts, _, _) = store.list_parts(&b, &k, &id, None, 10).await.unwrap();
        assert_eq!(parts.len(), 1);
        // The record's etag must be the hash of the file's content.
        let dir = state.path().join("multipart/data").join(&id);
        let content = fs::read(dir.join("part-1")).await.unwrap();
        assert_eq!(parts[0].etag, ETag::from_content(&content));
    }

    #[tokio::test]
    async fn list_parts_is_db_driven_no_ghost_parts() {
        // A part file whose PARTS record never committed (crash window)
        // must not appear in listings — the client retransmits.

        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // Write a part file out-of-band (no PARTS record).
        let dir = state.path().join("multipart/data").join(&upload.upload_id);
        fs::create_dir_all(&dir).await.unwrap();
        fs::write(dir.join("part-1"), b"orphan").await.unwrap();
        let (parts, _, _) = store
            .list_parts(&b, &k, &upload.upload_id, None, 1000)
            .await
            .unwrap();
        assert!(parts.is_empty(), "no record, no listing: {parts:?}");
        // But the point query recomputes from the file (complete
        // fallback for the crash window).
        let etag = store.part_etag(&b, &upload.upload_id, 1).await.unwrap();
        assert_eq!(etag, ETag::from_content(b"orphan"));
    }

    #[tokio::test]
    async fn complete_racing_put_part_never_mismatches_content() {
        // #6: a put_part overwriting a part between complete's
        // verification pass and its copy must either fail the completion
        // (InvalidPart via the post-assembly re-verification) or produce
        // an object whose bytes match the composed ETag — never a success
        // whose content disagrees with the returned ETag.

        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // Non-final listed parts must be >= the 5 MiB minimum (the
        // authoritative verify-loop check); the final (part 3) stays small
        // — the racer overwrites it, so it must be cheap to re-upload.
        let parts_data: [Vec<u8>; 3] = [
            vec![b'1'; MIN_PART_BYTES as usize],
            vec![b'2'; MIN_PART_BYTES as usize],
            b"part-3-original".to_vec(),
        ];
        for (i, data) in parts_data.iter().enumerate() {
            store
                .put_part(
                    &b,
                    &k,
                    &upload.upload_id,
                    multipart::part_number((i + 1) as u32).expect("a valid part number"),
                    body(data.clone()),
                    None,
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
                            multipart::part_number(3).expect("a valid part number"),
                            body(format!("part-3-overwrite-{i}")),
                            None,
                        )
                        .await;
                }
            }
        });
        let outcome = store.complete(&b, &k, &upload.upload_id, &completed).await;
        racer.await.unwrap();
        match outcome {
            Ok((temp, etag, _)) => {
                // Success means the re-verification passed: no part
                // changed between verify and copy — the bytes are
                // exactly the original parts and the ETag matches.
                let target = state.path().join("out.bin");
                fs::rename(&temp, &target).await.unwrap();
                let expect = parts_data.concat();
                assert_eq!(fs::read(&target).await.unwrap(), expect);
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
    }

    #[tokio::test]
    async fn abort_during_assembly_is_no_such_upload() {
        // §5.6: a part file vanishing mid-verify/assembly because a
        // concurrent abort consumed the upload must surface NoSuchUpload
        // (the NotFound re-checks UPLOADS), never a bare InvalidPart or
        // I/O error. The abort commits its record deletion before
        // touching the directory, so any NotFound implies the upload is
        // gone.

        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let mut saw_no_such_upload = false;
        // Enough parts that the assembly window (per-part opens)
        // reliably intersects the abort; retry on the off chance the
        // completion wins the race outright. The non-final 5 MiB minimum
        // keeps each round's assembly long (see below) — four rounds
        // suffice where the small-part original needed eight.
        for _ in 0..4 {
            let upload = store
                .create(
                    &b,
                    &k,
                    None,
                    object::Tags::empty(),
                    None,
                    &Acl::default_private(None),
                )
                .await
                .unwrap();
            let mut completed = Vec::new();
            for n in 1..=8u32 {
                // Non-final listed parts must be >= the 5 MiB minimum —
                // only the final part may stay small. The assembly is
                // therefore long, which widens the race window the test
                // needs (the abort commits while parts are streaming).
                let size = if n == 8 {
                    256 * 1024
                } else {
                    MIN_PART_BYTES as usize
                };
                let part = store
                    .put_part(
                        &b,
                        &k,
                        &upload.upload_id,
                        multipart::part_number(n).expect("a valid part number"),
                        body(vec![n as u8; size]),
                        None,
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
                Ok((temp, _, _)) => {
                    // The completion won — finish the caller flow and
                    // retry the race with a fresh upload.
                    let target = state.path().join(format!("out-{}.bin", upload.upload_id));
                    fs::rename(&temp, &target).await.unwrap();
                    store.consume(&b, &upload.upload_id).await.unwrap();
                    assert!(
                        abort.is_ok()
                            || matches!(abort, Err(Error::Storage(StorageError::NoSuchUpload(_))))
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
    }

    /// The corrupt-ETag resume delta, from the consumer side (the redb pin
    /// owns the contract answer; this pins what fs *does* with it): a truncated
    /// page whose raw rows all carry a domain-invalid ETag serves no part, and
    /// the resume marker must still name the raw read's last number — a
    /// markerless truncated page sends the client back to the same page
    /// forever. Only out-of-band tampering reaches this page (`put_part` takes
    /// a validated `&ETag`), through the same raw-table seam the sibling tests
    /// use.
    #[tokio::test]
    async fn a_truncated_page_of_invalid_etags_still_names_a_resume_marker() {
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // Three raw `PARTS` rows carrying an invalid ETag wire and no part
        // files: pass 2 skips missing files, so the page logic sees the raw
        // rows — a two-part page whose truncation probe is the third row.
        let bucket = b.clone();
        let upload_id = upload.upload_id.clone();
        store
            .handle
            .write(move |txn| {
                let mut parts = part::Table::open(txn).unwrap();
                for number in [1u32, 2, 3] {
                    parts
                        .insert((&*bucket, upload_id.as_str(), number), "not-an-etag")
                        .map_err(|e| database::Error::Redb(e.into()))?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let (parts, truncated, marker) = store
            .list_parts(&b, &k, &upload.upload_id, None, 2)
            .await
            .unwrap();
        assert!(
            parts.is_empty(),
            "every raw row self-heals away, so the page serves nothing: {parts:?}"
        );
        assert!(truncated, "the raw read saw a row beyond the page");
        assert_eq!(
            marker,
            Some(2),
            "the marker is the raw read's last number — a markerless truncated page loops"
        );
    }

    #[tokio::test]
    async fn list_parts_marker_at_u32_max_returns_empty_page() {
        // The exclusive marker has no successor at u32::MAX — a
        // saturating +1 would re-include the marker part itself.
        // (Unreachable via S3's 1..=10000 range, but this layer must not
        // rely on that.)

        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // A raw row at the boundary (no part file needed — pass 2
        // skips missing files; the marker logic sees the raw rows).
        let bucket = b.clone();
        let upload_id = upload.upload_id.clone();
        store
            .handle
            .write(move |txn| {
                part::Table::open(txn)
                    .unwrap()
                    .insert(
                        (&*bucket, upload_id.as_str(), u32::MAX),
                        "9dd4e461268c8034f5c8564e155c67a6",
                    )
                    .map_err(|e| database::Error::Redb(e.into()))?;
                Ok(())
            })
            .await
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
    }

    #[tokio::test]
    async fn list_parts_max_parts_zero_is_an_empty_page() {
        // `max_parts = 0` asks for nothing: an empty, untruncated page
        // with no marker (an exclusive-after marker would skip the first
        // part of the next page forever).
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"aaa"),
                None,
            )
            .await
            .unwrap();
        let (parts, truncated, next) = store
            .list_parts(&b, &k, &upload.upload_id, None, 0)
            .await
            .unwrap();
        assert!(parts.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[tokio::test]
    async fn list_parts_of_a_mismatched_stored_key_is_no_such_upload() {
        // The upload check is against the RECORDED key: a stored key
        // diverged out-of-band answers NoSuchUpload, never a page of
        // parts belonging to another key.
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .overwrite_stored_key(&b, &upload.upload_id, "other.bin")
            .await
            .unwrap();
        let err = store
            .list_parts(&b, &k, &upload.upload_id, None, 100)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(StorageError::NoSuchUpload(_))),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn complete_recomputes_a_part_whose_record_is_missing() {
        // A part file WITHOUT a PARTS row (a crash between the file
        // rename and the record commit, §5.6): the completion recomputes
        // the ETag from the file instead of failing.
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // The first part is non-final in the two-part list — it must be
        // >= the 5 MiB minimum the verify loop enforces.
        let big = vec![b'a'; MIN_PART_BYTES as usize];
        let p1 = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(big.clone()),
                None,
            )
            .await
            .unwrap();
        // An out-of-band part file with no record (the final part may be
        // small).
        let dir = state.path().join("multipart/data").join(&upload.upload_id);
        fs::write(dir.join("part-2"), b"bbb").await.unwrap();
        let recomputed = ETag::from_content(b"bbb");
        let completed = [
            CompletedPart {
                part_number: multipart::part_number(1).expect("a valid part number"),
                etag: p1.etag,
            },
            CompletedPart {
                part_number: multipart::part_number(2).expect("a valid part number"),
                etag: recomputed,
            },
        ];
        let (temp, _etag, _parts) = store
            .complete(&b, &k, &upload.upload_id, &completed)
            .await
            .unwrap();
        let expect = [big, b"bbb".to_vec()].concat();
        assert_eq!(
            fs::read(&temp).await.unwrap(),
            expect,
            "the recomputed part joins the assembly"
        );
    }

    #[tokio::test]
    async fn complete_prefers_the_size_error_over_an_etag_mismatch() {
        // The verify loop checks the 5 MiB minimum BEFORE the per-part
        // ETag compare within one iteration: an undersized part whose
        // list etag is deliberately wrong answers PartTooSmall, never
        // InvalidPart. (The rule's pass and plain-failure shapes are the
        // shared conformance leg.)
        let (_, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let min = MIN_PART_BYTES as usize;
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let under = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(vec![b'b'; min - 1]),
                None,
            )
            .await
            .unwrap();
        let small = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(2).expect("a valid part number"),
                body(b"x".to_vec()),
                None,
            )
            .await
            .unwrap();
        let err = store
            .complete(
                &b,
                &k,
                &upload.upload_id,
                &[
                    CompletedPart {
                        part_number: under.part_number,
                        etag: ETag::from_content(b"zzz"),
                    },
                    CompletedPart {
                        part_number: small.part_number,
                        etag: small.etag,
                    },
                ],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Storage(StorageError::PartTooSmall {
                    part_number: 1,
                    min_bytes: MIN_PART_BYTES,
                    actual,
                }) if actual == min as u64 - 1
            ),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn complete_refuses_a_part_that_is_a_directory() {
        // A part path that is a directory is never a part: InvalidPart
        // (a directory open would otherwise classify as a permission
        // denial).
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let part = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"aaa"),
                None,
            )
            .await
            .unwrap();
        let dir = state.path().join("multipart/data").join(&upload.upload_id);
        fs::remove_file(dir.join("part-1")).await.unwrap();
        fs::create_dir(dir.join("part-1")).await.unwrap();
        let err = store
            .complete(
                &b,
                &k,
                &upload.upload_id,
                &[CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                }],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(StorageError::InvalidPart(_))),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn complete_rejects_part_content_that_disagrees_with_the_record() {
        // The verify pass trusts the PARTS record, but the assembly
        // re-hashes the file: out-of-band content that disagrees with
        // the record fails the completion (the verify-then-copy race
        // closes here, §5.6).
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let part = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"aaa"),
                None,
            )
            .await
            .unwrap();
        let dir = state.path().join("multipart/data").join(&upload.upload_id);
        fs::write(dir.join("part-1"), b"zzz").await.unwrap();
        let err = store
            .complete(
                &b,
                &k,
                &upload.upload_id,
                &[CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                }],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(StorageError::InvalidPart(_))),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn put_part_returns_the_stage_error_when_the_upload_is_live() {
        // The staging write fails (state `tmp/` is a file) while the
        // upload is still live: the original stage error is returned —
        // never NoSuchUpload.
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        fs::write(state.path().join("tmp"), b"blocked")
            .await
            .unwrap();
        let err = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"aaa"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Io(_)),
            "a live upload must keep the stage error: {err:?}"
        );
        assert!(store.has_uploads(&b).await.unwrap());
    }

    #[tokio::test]
    async fn publish_part_cleans_the_temp_when_the_upload_dir_cannot_be_created() {
        // `multipart/<bucket>` is a file: the upload dir cannot be
        // created — the staged temp is removed and the error returned.
        // The `multipart` parent is created by the test (the store
        // creates upload dirs lazily, so it must exist to plant the
        // blocking file).
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        fs::create_dir_all(state.path().join("multipart"))
            .await
            .unwrap();
        fs::write(state.path().join("multipart/data"), b"blocked")
            .await
            .unwrap();
        let err = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"aaa"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Io(_)),
            "the dir-creation error propagates: {err:?}"
        );
        let temp = state.path().join("tmp");
        assert!(
            fs::read_dir(&temp)
                .await
                .unwrap()
                .next_entry()
                .await
                .unwrap()
                .is_none(),
            "no staged temp survives the failed publish"
        );
    }

    #[tokio::test]
    async fn publish_part_cleans_the_temp_when_the_part_path_is_a_directory() {
        // The rename cannot land onto a directory: the staged temp is
        // removed and the error returned (a same-part overwrite never
        // leaves a half-published file).
        let (state, store) = fixture();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = store
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"aaa"),
                None,
            )
            .await
            .unwrap();
        let dir = state.path().join("multipart/data").join(&upload.upload_id);
        fs::remove_file(dir.join("part-1")).await.unwrap();
        fs::create_dir(dir.join("part-1")).await.unwrap();
        let err = store
            .put_part(
                &b,
                &k,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"bbb"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Io(_)),
            "the rename error propagates: {err:?}"
        );
        let temp = state.path().join("tmp");
        assert!(
            fs::read_dir(&temp)
                .await
                .unwrap()
                .next_entry()
                .await
                .unwrap()
                .is_none(),
            "no staged temp survives"
        );
    }

    // --- property tests (T029; moved inline from
    // `tests/proptest_multipart.rs`, 2026-10-01) ---
    //
    // Arbitrary part counts and sizes assemble into the exact concatenation,
    // and the composed ETag `MD5-of-MD5s-N` matches an independent reference
    // implementation (raw part digests concatenated, per the AWS composition —
    // NOT the same function the backend uses).

    /// Independent reference composition: MD5 of the raw part digests
    /// concatenated, then `-N` (the AWS composition, computed here from
    /// scratch).
    fn reference_composed(parts: &[PartInfo]) -> String {
        let mut joined = Vec::with_capacity(parts.len() * 16);
        for part in parts {
            joined.extend_from_slice(&part.etag[..]);
        }
        format!("{}-{}", hex::encode(Md5::digest(&joined)), parts.len())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 8, ..testutil::proptest_config() })]

        /// Random part counts (1..=3) and sizes assemble byte-exact, with
        /// the reference composed ETag. Non-final parts must clear the 5 MiB
        /// minimum the store enforces at complete — they vary just above it;
        /// the final part is 0..=4 KiB. Each case assembles up to ~10 MiB,
        /// hence the bounded case count.
        #[test]
        fn assembly_is_exact_concatenation(
            non_final in collection::vec((MIN_PART_BYTES as usize)..(MIN_PART_BYTES as usize + 1024), 0..=2),
            final_size in 0usize..4096,
        ) {
            let mut part_sizes = non_final;
            part_sizes.push(final_size);
            let runtime = testutil::runtime();
            runtime.block_on(async {
                let state = tempfile::tempdir().unwrap();
                let store = store(state.path()).unwrap();
                let b = bucket::name("data").unwrap();
                let key = object::key("big.bin").unwrap();
                let upload = store
                    .create(&b, &key, None, object::Tags::empty(), None, &Acl::default_private(None)).await.unwrap();

                let mut parts = Vec::new();
                let mut expected = Vec::new();
                for (i, size) in part_sizes.iter().enumerate() {
                    let data: Vec<u8> = (0..*size).map(|j| (i * 31 + j) as u8).collect();
                    expected.extend_from_slice(&data);
                    let part = store
                        .put_part(&b, &key, &upload.upload_id, multipart::part_number((i + 1) as u32).expect("a valid part number"), body(data), None)
                        .await
                        .unwrap();
                    parts.push(part);
                }

                // Independent ETag check (before the object is assembled).
                let expected_etag = reference_composed(&parts);
                let completed: Vec<CompletedPart> = parts
                    .iter()
                    .map(|p| CompletedPart {
                        part_number: p.part_number,
                        etag: p.etag.clone(),
                    })
                    .collect();
                let target = state.path().join("assembled.bin");
                let (temp, etag, _parts) = store
                    .complete(&b, &key, &upload.upload_id, &completed)
                    .await
                    .unwrap();
                prop_assert_eq!(etag.as_str(), expected_etag);
                fs::rename(&temp, &target).await.unwrap();
                let assembled = fs::read(&target).await.unwrap();
                prop_assert_eq!(&assembled, &expected);
                // The caller renames, then consumes; the upload is gone and
                // abort is now NoSuchUpload.
                store.consume(&b, &upload.upload_id).await.unwrap();
                let err = store.abort(&b, &key, &upload.upload_id).await.unwrap_err();
                prop_assert!(matches!(
                    err,
                    Error::Storage(NoSuchUpload(_))
                ));
                Ok(())
            })?;
        }

        /// Overwriting a part keeps the final assembly consistent (last write
        /// wins per part number).
        #[test]
        fn part_overwrite_last_writer_wins(n in 1u32..10_000, first in 0usize..1024, second in 0usize..1024) {
            let runtime = testutil::runtime();
            runtime.block_on(async {
                let state = tempfile::tempdir().unwrap();
                let store = store(state.path()).unwrap();
                let b = bucket::name("data").unwrap();
                let key = object::key("big.bin").unwrap();
                let upload = store
                    .create(&b, &key, None, object::Tags::empty(), None, &Acl::default_private(None)).await.unwrap();
                let first_data: Vec<u8> = (0..first).map(|i| i as u8).collect();
                let second_data: Vec<u8> = (0..second).map(|i| (i as u8).wrapping_mul(7)).collect();
                let p1 = store.put_part(&b, &key, &upload.upload_id, multipart::part_number(n).expect("a valid part number"), body(first_data.clone()), None).await.unwrap();
                let p2 = store.put_part(&b, &key, &upload.upload_id, multipart::part_number(n).expect("a valid part number"), body(second_data.clone()), None).await.unwrap();
                prop_assert_eq!(p1.part_number, p2.part_number);
                // The stored part is the second write.
                let (listed, truncated, _) = store.list_parts(&b, &key, &upload.upload_id, None, 1000).await.unwrap();
                prop_assert!(!truncated);
                prop_assert_eq!(listed.len(), 1);
                prop_assert_eq!(&listed[0].etag, &ETag::from_content(&second_data));
                prop_assert_eq!(listed[0].size, second as u64);
                let completed = [CompletedPart {
                    part_number: p2.part_number,
                    etag: p2.etag.clone(),
                }];
                let target = state.path().join("out.bin");
                let (temp, _etag, _parts) = store
                    .complete(&b, &key, &upload.upload_id, &completed)
                    .await
                    .unwrap();
                fs::rename(&temp, &target).await.unwrap();
                let metadata = fs::metadata(&target).await.unwrap();
                prop_assert_eq!(metadata.len(), second as u64);
                let assembled = fs::read(&target).await.unwrap();
                prop_assert_eq!(&assembled, &second_data);
                Ok(())
            })?;
        }

        /// Part numbers validate 1..=10000 (numbers outside are rejected by
        /// the checked constructor, not by the store).
        #[test]
        fn part_numbers_bounds(n in 1u32..10_000) {
            let runtime = testutil::runtime();
            runtime.block_on(async {
                let state = tempfile::tempdir().unwrap();
                let store = store(state.path()).unwrap();
                let b = bucket::name("data").unwrap();
                let key = object::key("big.bin").unwrap();
                let upload = store
                    .create(&b, &key, None, object::Tags::empty(), None, &Acl::default_private(None)).await.unwrap();
                let pn = part_number(n).expect("the strategy's range is inside 1..=10000");
                prop_assert!(u32::from(pn) == n);
                let part = store.put_part(&b, &key, &upload.upload_id, pn, body(b"x"), None).await.unwrap();
                prop_assert_eq!(u32::from(part.part_number), n);
                // A part at the extreme edge is listable.
                let (listed, _, _) = store.list_parts(&b, &key, &upload.upload_id, None, 1000).await.unwrap();
                prop_assert!(listed.iter().any(|p| u32::from(p.part_number) == n));
                let _ = SystemTime::now();
                Ok(())
            })?;
        }
    }
}
