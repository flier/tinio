//! The `Cleanup` trait implementation for the fs backend (task T070
//! foundation; scanner reclamation per T045).
//!
//! Per fs-backend.md §8 and failure-handling.md §3: startup repair handles
//! the fast, deterministic items (full `tmp/` clear, unpublished delete
//! tombstones under `<root>/.tinio/deleting/`, bucket `.tinio/` staging
//! residue, bucket-orphaned multipart subtrees, stale bucket records); the
//! full repair adds meta-orphan reclamation; `reclaim_meta_orphans` is the
//! scanner's background path. All modes share one code path with a
//! `dry_run` flag ([`CleanupOptions`]); **user data (live bucket
//! directories and objects) is never touched** — only tinio-private state
//! (including unpublished tombstones).
//!
//! Home root-state-dir GC (part of the `Full` scope per failure-handling.md
//! §3) needs read-only-mode state relocation and lands with US2 (T144).

#[cfg(feature = "versioning")]
use std::collections::HashSet;
use std::{
    future::Future,
    io::{self, Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use derive_more::Debug;
#[cfg(feature = "versioning")]
use futures::StreamExt;
use futures::stream;
#[cfg(feature = "versioning")]
use sha2::{Digest, Sha256};
use tokio::fs;

#[cfg(feature = "versioning")]
use crate::{
    _blob::LiveSet,
    _core::{
        blob::{BlockHash, BlockStore, Entry},
        bucket::Name,
        version::State,
    },
    _store_redb::version as rows,
    error::projected,
    metrics::record_blocks_corrupt,
};
use crate::{
    _core::{
        cleanup::{
            ActionStream, Cleanup, CleanupOptions, RepairAction, RepairActionLevel, RepairKind,
        },
        mode::AccessMode,
    },
    backend::FsStorage,
    bucket,
    error::Error,
    fsutil,
    fsutil::{entries_of, ok_if_missing, remove_tree},
    path::{MULTIPART_DIR_NAME, STATE_DIR_NAME, TMP_DIR_NAME},
    sweep::Options,
    tombstone,
};

/// The idle anchor of an orphan upload dir: the latest part mtime,
/// falling back to the directory mtime so a part-less orphan still has an
/// idle age (a vanished dir propagates `NotFound` — the caller skips).
async fn orphan_idle_since(dir: &Path) -> io::Result<SystemTime> {
    if let Some(latest) = fsutil::latest_part_mtime(dir).await? {
        return Ok(latest);
    }
    fs::metadata(dir).await?.modified()
}

/// Report one repair operation: dry-run pushes the "would …" action;
/// otherwise the op runs and its outcome is reported ("did …" on success,
/// the error otherwise).
async fn record_repair(
    actions: &mut Vec<Result<RepairAction, Error>>,
    dry_run: bool,
    would: String,
    did: String,
    op: impl Future<Output = Result<(), Error>>,
) {
    if dry_run {
        actions.push(Ok(RepairAction {
            level: RepairActionLevel::Warn,
            description: would,
        }));
        return;
    }
    match op.await {
        Ok(()) => actions.push(Ok(RepairAction {
            level: RepairActionLevel::Warn,
            description: did,
        })),
        Err(err) => actions.push(Err(err)),
    }
}

/// Verify one stored block: read it back in bounded chunks, hashing as they
/// arrive, and answer the finding when the stored bytes are not what the
/// block's name addresses — `None` when the block verifies. The block is
/// never buffered whole (constitution V): each chunk is hashed and dropped
/// before the next arrives. A truncated or corrupt file is the same finding
/// — the name is the content's hash, and the store records no independent
/// length to compare against (the walk's entry length is the file's own
/// metadata) — and a block that cannot be read at all is its own finding.
#[cfg(feature = "versioning")]
async fn verify_block<B: BlockStore>(blocks: &B, entry: &Entry) -> Result<Option<String>, Error> {
    let mut body = match blocks.open(&entry.hash, None).await {
        Ok(body) => body,
        Err(err) => {
            return Ok(Some(format!(
                "could not be read for verification: {}",
                projected(err)
            )));
        }
    };
    let mut hasher = Sha256::new();
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(chunk) => hasher.update(&chunk),
            Err(err) => {
                return Ok(Some(format!(
                    "could not be read to its end for verification: {}",
                    projected(err)
                )));
            }
        }
    }
    let found = BlockHash::from_digest(hasher.finalize().into());
    if found != entry.hash {
        return Ok(Some(format!(
            "does not hash to its own name (corrupt or truncated): its bytes hash to {}",
            found.to_hex()
        )));
    }
    Ok(None)
}

/// The fs implementation of the [`Cleanup`] contract.
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
///
/// use futures::StreamExt;
/// use tinio_core::{
///     cleanup::{Cleanup, CleanupOptions, RepairKind},
///     mode::AccessMode,
///     pipeline::InlineRunner,
///     storage::{
///         DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
///     },
/// };
/// use tinio_fs::{FsCleanup, FsOptions, FsStorage};
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
///     owner_uids: std::collections::HashMap::new(),
///     io_pipeline: Arc::new(InlineRunner::default()),
///     remove_pipeline: Arc::new(InlineRunner::default()),
///     db_pipeline: Arc::new(InlineRunner::default()),
/// };
/// let storage = FsStorage::new(root.path(), options).unwrap();
/// let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
/// Runtime::new().unwrap().block_on(async {
///     let mut actions = cleanup.repair(RepairKind::Startup).await.unwrap();
///     while let Some(action) = actions.next().await {
///         let action = action.unwrap();
///         assert!(!action.description.is_empty());
///     }
/// });
/// ```
#[derive(Debug, Clone)]
pub struct FsCleanup {
    /// Storage root (bucket dirs at the top level).
    root: PathBuf,
    /// The reserved state directory.
    state_dir: PathBuf,
    /// The storage handle for the meta store and bucket walk.
    storage: FsStorage,
    /// Report-only mode (doctor `--dry-run`): never touches anything.
    dry_run: bool,
    /// The plane's write policy: read-only mode forbids the stages that
    /// write the storage root (FR-023).
    access: AccessMode,
    /// Idle grace for upload directories without a `UPLOADS` record:
    /// an orphan is deleted only when its parts have been idle at least
    /// this long (the sweep's `multipart_ttl` default; the startup
    /// orchestration T068 passes the configured `multipart_expire_days`).
    multipart_grace: Duration,
}

impl FsCleanup {
    /// Construct the cleanup pipeline for `storage`. The delete-tombstone
    /// stage always routes through the storage's removal lane (D-B — see
    /// `repair_delete_tombstones`).
    pub fn new(storage: &FsStorage, options: CleanupOptions) -> Self {
        Self {
            root: storage.root().to_path_buf(),
            state_dir: storage.state_dir().to_path_buf(),
            storage: storage.clone(),
            dry_run: options.dry_run,
            access: options.access,
            multipart_grace: Options::default().multipart_ttl(),
        }
    }

    /// Override the orphan-upload idle grace (default: the sweep's
    /// `multipart_ttl`, 7 days). The startup orchestration (T068) passes
    /// the configured `[s3] multipart_expire_days` through this.
    pub fn with_multipart_grace(mut self, grace: Duration) -> Self {
        self.multipart_grace = grace;
        self
    }

    /// The orphan-upload idle grace this cleanup judges by — the value
    /// [`Self::with_multipart_grace`] set, or the sweep's default when no
    /// override was given. A trivial accessor: it exists so the
    /// orchestration's wiring (which cleanup the caller builds) can be
    /// asserted beside the value it was built from.
    pub fn multipart_grace(&self) -> Duration {
        self.multipart_grace
    }

    /// Verify every stored block of every versioned bucket against its own
    /// name — the design's optional pass for a start that followed an
    /// unclean shutdown (2026-10-06 review): the sweep's read path trusts
    /// the store, and this pass is where that trust is checked in the
    /// background while serving continues. Report-only, never a repair; the
    /// corrupt count feeds `tinio_storage_blocks_corrupt_total` and each
    /// finding is logged.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use tinio_core::{
    ///     cleanup::CleanupOptions,
    ///     mode::AccessMode,
    ///     pipeline::InlineRunner,
    ///     storage::{
    ///         DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
    ///     },
    /// };
    /// use tinio_fs::{FsCleanup, FsOptions, FsStorage};
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
    ///     owner_uids: std::collections::HashMap::new(),
    ///     io_pipeline: Arc::new(InlineRunner::default()),
    ///     remove_pipeline: Arc::new(InlineRunner::default()),
    ///     db_pipeline: Arc::new(InlineRunner::default()),
    /// };
    /// let storage = FsStorage::new(root.path(), options).unwrap();
    /// let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
    /// Runtime::new().unwrap().block_on(async {
    ///     let (checked, corrupt) = cleanup.verify_all_blocks().await.unwrap();
    ///     assert_eq!(checked, 0, "an empty root has no blocks to verify");
    ///     assert_eq!(corrupt, 0);
    /// });
    /// ```
    #[cfg(feature = "versioning")]
    pub async fn verify_all_blocks(&self) -> Result<(u64, u64), Error> {
        let mut checked = 0u64;
        let mut corrupt = 0u64;
        for bucket in self.storage.versioned_buckets().await? {
            let versions = self.storage.versions(&bucket);
            let blocks = versions.blobs().blocks();
            let mut walk = blocks.walk(None).await.map_err(projected)?;
            while let Some(entry) = walk.next().await {
                let entry = entry.map_err(projected)?;
                checked += 1;
                if let Some(finding) = verify_block(blocks, &entry).await? {
                    corrupt += 1;
                    tracing::warn!(
                        bucket = %bucket,
                        block = %entry.hash,
                        finding = %finding,
                        "a stored block failed verification"
                    );
                }
            }
        }
        record_blocks_corrupt(corrupt);
        Ok((checked, corrupt))
    }

    /// Stage 1: full `tmp/` clear (no active writers at startup).
    async fn repair_tmp(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let entries = match entries_of(&self.state_dir.join(TMP_DIR_NAME)).await {
            Ok(entries) => entries,
            Err(err) => {
                actions.push(Err(err.into()));
                return;
            }
        };
        for (path, name) in entries {
            record_repair(
                actions,
                self.dry_run,
                format!("would clear leftover temp file {name}"),
                format!("cleared leftover temp file {name}"),
                async {
                    match fs::remove_file(&path).await {
                        Ok(()) => Ok(()),
                        // Already gone (a concurrent sweep): nothing to report.
                        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                        Err(err) => Err(err.into()),
                    }
                },
            )
            .await;
        }
    }

    /// Stage: `<state-dir>/blobs/<bucket>/tmp` residue — the staged temps
    /// of block writes a crash interrupted (the block store's own commit
    /// removes a stage that completes). Cleared like `tmp/`, per bucket
    /// subtree, because no writer holds one at startup; a bucket that
    /// never staged a block has no `tmp` and is skipped by the empty
    /// listing.
    #[cfg(feature = "versioning")]
    async fn repair_block_residue(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let entries = match entries_of(&self.storage.blobs_dir()).await {
            Ok(entries) => entries,
            Err(err) => {
                actions.push(Err(err.into()));
                return;
            }
        };
        for (dir, bucket) in entries {
            // A bucket that never staged a block has no tmp/; an
            // out-of-band *file* wearing a bucket's name is skipped
            // rather than probed as a directory.
            if !fs::metadata(&dir)
                .await
                .map(|meta| meta.is_dir())
                .unwrap_or(false)
            {
                continue;
            }
            let staged = match entries_of(&dir.join(TMP_DIR_NAME)).await {
                Ok(staged) => staged,
                Err(err) => {
                    actions.push(Err(err.into()));
                    continue;
                }
            };
            for (path, name) in staged {
                record_repair(
                    actions,
                    self.dry_run,
                    format!("would clear leftover block staging file {name} of {bucket}"),
                    format!("cleared leftover block staging file {name} of {bucket}"),
                    async {
                        match fs::remove_file(&path).await {
                            Ok(()) => Ok(()),
                            // Already gone (a concurrent sweep): nothing to
                            // report.
                            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                            Err(err) => Err(err.into()),
                        }
                    },
                )
                .await;
            }
        }
    }

    /// A build without the block engine has no `blobs/` subtree: nothing
    /// to clear.
    #[cfg(not(feature = "versioning"))]
    async fn repair_block_residue(&self, _actions: &mut Vec<Result<RepairAction, Error>>) {}

    /// The level a report-stage finding carries on this run: the finding's
    /// own severity on a report-only (`--dry-run`) pass, and
    /// [`RepairActionLevel::Lint`] on a `--fix` pass — the level that means
    /// "a check finding, no action needed", so the doctor's repair ledger
    /// (which counts applied repairs) never counts a finding as one.
    #[cfg(feature = "versioning")]
    fn finding_level(&self, level: RepairActionLevel) -> RepairActionLevel {
        if self.dry_run {
            level
        } else {
            RepairActionLevel::Lint
        }
    }

    /// The doctor's block-store residue check — the one place the read
    /// path's trust-the-store rule is compensated (the design's
    /// **Verification** ruling): every version row's blocks are probed
    /// against the store, every stored block against the rows (and read back
    /// and re-hashed against its own name by
    /// [`Self::check_bucket_blocks`]), and every `blobs/<bucket>/` subtree
    /// against the buckets that exist. It
    /// reports and never repairs: a block referenced by no row is what the
    /// block sweep reclaims, a row whose block is missing is content
    /// no repair can bring back, and an orphaned subtree is what the
    /// sweep's own orphan pass reclaims (this check names it one pass
    /// early, and a `--dry-run` can therefore name it in the same pass
    /// that the sweep will clear it).
    ///
    /// Report-only in **both** modes: the check runs on a `--fix` pass as
    /// well as a reporting pass — an operator who asked for a repair still
    /// gets the block answer without a second invocation — and it repairs
    /// nothing. Its findings carry [`RepairActionLevel::Lint`] on a `--fix`
    /// pass (the level means "a check finding, no action needed"), so the
    /// run's repair ledger counts none of them as an applied repair while
    /// the reporting pass keeps their own severity.
    #[cfg(feature = "versioning")]
    async fn report_block_residue(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let buckets = match self.storage.bucket_names("").await {
            Ok(buckets) => buckets,
            Err(err) => {
                actions.push(Err(err));
                return;
            }
        };
        for bucket in buckets {
            let versions = self.storage.versions(&bucket);
            let state = match versions.state(&bucket).await {
                Ok(state) => state,
                Err(err) => {
                    actions.push(Err(projected(err)));
                    continue;
                }
            };
            if !matches!(state, State::Enabled | State::Suspended) {
                continue; // no version rows, no block store
            }
            if let Err(err) = self.check_bucket_blocks(&bucket, actions).await {
                actions.push(Err(err));
            }
        }
        // Subtrees whose bucket is gone. `remove_bucket_state` drains the
        // version rows in one transaction and then removes the block subtree,
        // so a crash between the two leaves blocks under a bucket name that
        // no longer exists — and both the per-bucket walk above and the
        // sweep's `versioned_buckets` iterate existing buckets only, so no
        // pass reaches it through them. The sweep's `orphan_blobs` pass is
        // what reclaims it (under the bucket's mutation lock, on the same
        // absence proof this check makes); a finding here is the one-pass-
        // early notice, never a repair — the doctor leaves the bytes in
        // place so they stay inspectable until then.
        let entries = match entries_of(&self.storage.blobs_dir()).await {
            Ok(entries) => entries,
            Err(err) => {
                actions.push(Err(err.into()));
                return;
            }
        };
        for (dir, bucket) in entries {
            // The proof — the bucket directory's absence (the backend's own
            // existence authority: `ensure_bucket` reads it, never the
            // `BUCKETS` row, so a lost row alone is re-derived for a live
            // directory whose history still needs these blocks), under the
            // same mutation lock the sweep's reclamation holds — is
            // `FsStorage::orphaned_block_subtree`'s; the doctor only reports
            // (`reclaim = false`), leaving the bytes inspectable until the
            // sweep clears them.
            match self
                .storage
                .orphaned_block_subtree(&dir, &bucket, false)
                .await
            {
                Ok(true) => actions.push(Ok(RepairAction {
                    level: self.finding_level(RepairActionLevel::Lint),
                    description: format!(
                        "the block store holds a subtree for bucket {bucket} which no longer exists; the sweep reclaims it on its next pass"
                    ),
                })),
                Ok(false) => {}
                Err(err) => actions.push(Err(err)),
            }
        }
    }

    /// A build without the block engine has no rows and no blocks to
    /// check.
    #[cfg(not(feature = "versioning"))]
    async fn report_block_residue(&self, _actions: &mut Vec<Result<RepairAction, Error>>) {}

    /// One bucket's half of [`Self::report_block_residue`]: the rows whose
    /// blocks are missing, then one store walk per stored block — its name
    /// re-verified, and its membership in the live set that the rows imply
    /// tested (the residue).
    #[cfg(feature = "versioning")]
    async fn check_bucket_blocks(
        &self,
        bucket: &Name,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) -> Result<(), Error> {
        let versions = self.storage.versions(bucket);
        // The rows, and the live set they imply.
        let scanned = bucket.clone();
        let history = self
            .storage
            .handle()
            .clone()
            .read_blocking(move |txn| {
                let table = rows::Table::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_bucket_of(scanned.as_ref(), |key, row| {
                    out.push((key.to_owned(), row));
                    Ok(())
                })?;
                Ok(out)
            })
            .await
            .map_err(Error::from)?;
        let blocks = versions.blobs().blocks();
        // A marker holds the empty blob, which is never stored: only a row
        // that names content can have a missing block. One probe per
        // distinct block — a block this pass has already found in the store
        // is not probed again for the next row that names it (a version and
        // its superseding sibling routinely share blocks), while a block
        // found *missing* is reported per row, because each row's finding
        // names the row.
        let mut confirmed = HashSet::new();
        for (key, row) in &history {
            if row.kind == rows::Kind::Marker || row.blob().is_empty() {
                continue;
            }
            for block in row.blob().blocks() {
                if confirmed.contains(&block.hash) {
                    continue;
                }
                let present = blocks.contains(&block.hash).await.map_err(projected)?;
                if present {
                    confirmed.insert(block.hash);
                    continue;
                }
                actions.push(Ok(RepairAction {
                    level: self.finding_level(RepairActionLevel::Warn),
                    description: format!(
                        "version `{key}` ({}) of {bucket} names block {} which the store does not hold",
                        row.id,
                        block.hash.to_hex()
                    ),
                }));
            }
        }
        // The live set is the history's own answer — the same reduction
        // [`Versions::live_blocks`] feeds ([`LiveSet::from_blobs`]), derived
        // here from the rows this pass already holds rather than read back
        // through a second walk of every bucket's rows.
        let live = LiveSet::from_blobs(history.iter().map(|(_, row)| row.blob()));
        // **One walk of the store answers both questions**: whether the
        // block's bytes still hash to the name that addresses them (the
        // **verification** ruling — every stored block read back in bounded
        // chunks and re-hashed), and whether any row names it (residue —
        // young ones are a publish in flight or a version the sweep has not
        // reclaimed yet: the doctor names them, the sweep clears them). A
        // second traversal would answer one of the two per entry.
        let mut walk = blocks.walk(None).await.map_err(projected)?;
        let mut residue = 0u64;
        let mut bytes = 0u64;
        while let Some(entry) = walk.next().await {
            let entry = entry.map_err(projected)?;
            if let Some(finding) = verify_block(blocks, &entry).await? {
                actions.push(Ok(RepairAction {
                    level: self.finding_level(RepairActionLevel::Warn),
                    description: format!("block {} of {bucket} {finding}", entry.hash.to_hex()),
                }));
            }
            if !live.contains(&entry.hash) {
                residue += 1;
                bytes += entry.len;
            }
        }
        if residue > 0 {
            actions.push(Ok(RepairAction {
                level: self.finding_level(RepairActionLevel::Lint),
                description: format!(
                    "the block store of {bucket} holds {residue} block(s) ({bytes} bytes) referenced by no version row"
                ),
            }));
        }
        Ok(())
    }

    /// Unpublished delete-bucket tombstones under `<root>/.tinio/deleting/`
    /// — a crash after the unpublish rename leaves the tree as private
    /// residue (the live name is already gone). Not the relocated state
    /// dir: the rename stays on the data volume (FR-023).
    async fn repair_delete_tombstones(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let entries = match tombstone::leftovers(&self.root).await {
            Ok(entries) => entries,
            Err(err) => {
                actions.push(Err(err));
                return;
            }
        };
        // D-B: this stage always routes through the removal lane — one
        // `RemoveTask` per leftover (fire-and-forget; an enqueue failure
        // is warned inside and the leftover stays for the scanner; a
        // removal failure is error-logged from the detached awaiter,
        // F03). The lane is the storage's own
        // (`FsStorage::remove_pipeline`): offline contexts wire an
        // `InlineRunner` there, so the inline form is the same lane's
        // synchronous run.
        let runner = self.storage.remove_pipeline();
        for (path, name) in entries {
            record_repair(
                actions,
                self.dry_run,
                format!("would enqueue leftover bucket tombstone {name} on the removal lane"),
                format!("enqueued leftover bucket tombstone {name} on the removal lane"),
                async {
                    if tombstone::enqueue_one(path, &runner).await {
                        Ok(())
                    } else {
                        // Truthful report: the action stream must never
                        // say "enqueued" when the enqueue failed.
                        Err(Error::Io(IoError::other(
                            "bucket tombstone not enqueued; the scanner covers it",
                        )))
                    }
                },
            )
            .await;
        }
    }

    /// Stage 2: bucket `.tinio/` staging residue — a crashed or failed
    /// cross-volume commit (the EXDEV fallback in `write.rs`) leaves its
    /// staging file under a `.tinio/` directory at any depth of a bucket.
    /// The segment is never served or listed (FR-020) and the delete walk
    /// skips it (the bucket stays deletable), but the bytes must be
    /// reclaimed: cleared at startup like `tmp/` (no concurrent writers).
    /// A `.tinio` *file* (out-of-band) is cleared too — the reserved name
    /// is tinio's at any depth.
    async fn repair_bucket_staging(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let buckets = match self.storage.bucket_names("").await {
            Ok(buckets) => buckets,
            Err(err) => {
                actions.push(Err(err));
                return;
            }
        };
        for bucket in buckets {
            let Ok(bucket_dir) = self.storage.bucket_dir(&bucket).await else {
                continue;
            };
            // Collect every `.tinio` entry of the bucket tree (iterative
            // walk; symlinked dirs are descended only when following is
            // enabled — the follow policy, one source of truth).
            let mut residue = Vec::new();
            let mut stack = vec![bucket_dir];
            while let Some(dir) = stack.pop() {
                let mut entries = match fs::read_dir(&dir).await {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };
                loop {
                    let Ok(Some(entry)) = entries.next_entry().await else {
                        break;
                    };
                    let name = entry.file_name();
                    if name == STATE_DIR_NAME {
                        residue.push(entry.path());
                        continue; // never descend into a staging dir
                    }
                    let lmeta = match fs::symlink_metadata(entry.path()).await {
                        Ok(metadata) => metadata,
                        Err(_) => continue,
                    };
                    let is_dir = if fsutil::is_symlink_or_reparse(&lmeta) {
                        *self.storage.follow_symlinks()
                            && fs::metadata(entry.path())
                                .await
                                .map(|m| m.is_dir())
                                .unwrap_or(false)
                    } else {
                        lmeta.is_dir()
                    };
                    if is_dir {
                        stack.push(entry.path());
                    }
                }
            }
            for dir in residue {
                let name = dir
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                record_repair(
                    actions,
                    self.dry_run,
                    format!("would clear bucket staging residue {name} of {bucket}"),
                    format!("cleared bucket staging residue {name} of {bucket}"),
                    async {
                        // Tree, or a `.tinio` *file* planted out-of-band:
                        // the shared tree-or-file removal.
                        remove_tree(&dir).await?;
                        Ok(())
                    },
                )
                .await;
            }
        }
    }

    /// Stage 3: bucket-orphaned multipart subtrees (uploads whose bucket
    /// directory no longer exists — cross-restart uploads are never
    /// touched, failure-handling.md §2D).
    async fn repair_multipart_orphans(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let multipart = self.state_dir.join(MULTIPART_DIR_NAME);
        let mut buckets = match fs::read_dir(&multipart).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return,
            Err(err) => {
                actions.push(Err(err.into()));
                return;
            }
        };
        loop {
            let Ok(Some(entry)) = buckets.next_entry().await else {
                break;
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            match fsutil::is_absent(&self.root.join(&name)).await {
                // F11: a probe error is not "gone" — the live bucket's
                // subtree is kept and the error reported.
                Ok(false) => {}
                Err(err) => actions.push(Err(err.into())),
                Ok(true) => {
                    record_repair(
                        actions,
                        self.dry_run,
                        format!("would remove multipart subtree of missing bucket {name}"),
                        format!("removed multipart subtree of missing bucket {name}"),
                        async {
                            // Drain UPLOADS/PARTS (and the other derived
                            // tables) before deleting the tree — a missing
                            // bucket directory used to leave ghost uploads.
                            if let Ok(bucket) = bucket::name(&name) {
                                self.storage.remove_bucket_state(&bucket).await?;
                            }
                            ok_if_missing(fs::remove_dir_all(entry.path()).await)?;
                            Ok(())
                        },
                    )
                    .await;
                }
            }
        }
    }

    /// Stage 4: upload directories with no committed `UPLOADS` record —
    /// the residue of complete/abort whose directory removal failed, and
    /// revived subtrees from a `put_part` racing a bucket removal
    /// (meta-redb-spec §5.3/§5.6).
    ///
    /// TOCTOU order (pinned, §5.7): **enumerate the multipart tree first,
    /// then read `UPLOADS` in one transaction**. A directory exists only
    /// after its `UPLOADS` commit (`create` commits the record; the first
    /// `put_part` creates the directory), so the read opened after the
    /// enumeration sees every live upload. The reverse order (read first,
    /// enumerate after) would misjudge a fresh upload. The liveness check
    /// is RAW table membership (`live_upload_ids`), never the validated
    /// `walk_uploads` view: a live upload whose stored key fails domain
    /// validation is still live — skipping its row would delete a live
    /// upload's directory. An orphan is deleted only after its parts have
    /// been idle past `multipart_grace` — a slow `put_part` on a dying
    /// upload must not be interrupted mid-write.
    async fn repair_orphan_upload_dirs(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        // 1. Enumerate the tree first.
        let multipart = self.state_dir.join(MULTIPART_DIR_NAME);
        let mut orphans: Vec<(String, String, PathBuf)> = Vec::new();
        let mut buckets = match fs::read_dir(&multipart).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return,
            Err(err) => {
                actions.push(Err(err.into()));
                return;
            }
        };
        loop {
            let Ok(Some(bucket_entry)) = buckets.next_entry().await else {
                break;
            };
            let Ok(file_type) = bucket_entry.file_type().await else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let bucket = bucket_entry.file_name().to_string_lossy().into_owned();
            let mut uploads = match fs::read_dir(bucket_entry.path()).await {
                Ok(uploads) => uploads,
                Err(err) => {
                    actions.push(Err(err.into()));
                    continue;
                }
            };
            loop {
                let Ok(Some(upload_entry)) = uploads.next_entry().await else {
                    break;
                };
                let Ok(file_type) = upload_entry.file_type().await else {
                    continue;
                };
                if !file_type.is_dir() {
                    continue;
                }
                let upload_id = upload_entry.file_name().to_string_lossy().into_owned();
                orphans.push((bucket.clone(), upload_id, upload_entry.path()));
            }
        }
        // 2. One read transaction over `UPLOADS` (after the enumeration) —
        // RAW membership, not the domain-validated `walk_uploads` view
        // (a live upload with an invalid stored key is still live, §5.7).
        let live = match self.storage.multipart_store().live_upload_ids().await {
            Ok(live) => live,
            Err(err) => {
                actions.push(Err(err));
                return;
            }
        };
        // 3. Judge + idle grace + remove.
        let now = SystemTime::now();
        for (bucket, upload_id, dir) in orphans {
            if live.contains(&(bucket.clone(), upload_id.clone())) {
                continue;
            }
            let idle_since = match orphan_idle_since(&dir).await {
                Ok(idle_since) => idle_since,
                // The dir vanished between enumeration and stat (a
                // concurrent abort) — nothing to do.
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    actions.push(Err(err.into()));
                    continue;
                }
            };
            if now
                .duration_since(idle_since)
                .map(|age| age < self.multipart_grace)
                .unwrap_or(true)
            {
                continue;
            }
            record_repair(
                actions,
                self.dry_run,
                format!("would remove orphaned upload directory {upload_id} of {bucket}"),
                format!("removed orphaned upload directory {upload_id} of {bucket}"),
                async {
                    // Drain leftover PARTS (a put_part racing
                    // remove_bucket, §5.3). The directory name is not
                    // necessarily a UUID — `consume` would reject it.
                    if let Ok(name) = bucket::name(&bucket) {
                        self.storage
                            .multipart_store()
                            .drain_upload_rows(&name, &upload_id)
                            .await?;
                    }
                    ok_if_missing(fs::remove_dir_all(&dir).await)?;
                    Ok(())
                },
            )
            .await;
        }
    }

    /// Stage 5: stale bucket records (bucket directory gone).
    async fn repair_buckets(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let entries = match self.storage.bucket_store().load_all().await {
            Ok(entries) => entries,
            Err(err) => {
                actions.push(Err(err));
                return;
            }
        };
        for (name, _) in entries {
            let name = match bucket::name(name) {
                Ok(name) => name,
                Err(err) => {
                    actions.push(Err(Error::Storage(err)));
                    continue;
                }
            };
            match fsutil::is_absent(&self.root.join(&*name)).await {
                // F11: a probe error is not "gone" — the live record is
                // kept and the error reported.
                Ok(false) => {}
                Err(err) => actions.push(Err(err.into())),
                Ok(true) => {
                    record_repair(
                        actions,
                        self.dry_run,
                        format!("would prune stale bucket record for {name}"),
                        format!("pruned stale bucket record for {name}"),
                        async { self.storage.remove_bucket_state(&name).await },
                    )
                    .await;
                }
            }
        }
    }

    /// The stale-bucket-records stage alone — the scanner's per-pass path
    /// (item 2, data-path review 2026-08-27: the meta-orphan half of
    /// [`Cleanup::reclaim_meta_orphans`] is derived from each bucket's
    /// reconcile pass in scanner.rs, so only the stale-`BUCKETS` half
    /// remains per pass). A bucket directory removed out-of-band also
    /// orphans its derived state — and the scanner's reconcile loop only
    /// visits live directories. Cheap: one `BUCKETS` read plus an
    /// existence probe per row. Returns the number of records pruned; a
    /// failed prune is warned and skipped (the trait path keeps its
    /// action reporting — this is the count-only form the scanner needs).
    ///
    /// The probe + wipe run under that bucket's mutation lock (F02): a
    /// bucket deleted out-of-band and RECREATED between the probe and
    /// the wipe would have its fresh derived state (BUCKETS, UPLOADS,
    /// PARTS, OBJECT_META rows) destroyed in one write transaction. Under
    /// the lock the recreation cannot interleave — the probe sees the
    /// fresh directory, or the wipe drains before any of its rows commit
    /// (create/put of the same name hold the same per-bucket lock).
    pub(crate) async fn reclaim_stale_buckets(&self) -> Result<usize, Error> {
        let entries = self.storage.bucket_store().load_all().await?;
        let mut pruned = 0usize;
        for (name, _) in entries {
            let Ok(name) = bucket::name(name) else {
                continue;
            };
            let _guard = self.storage.lock_bucket_mutations(&name).await;
            match fsutil::is_absent(&self.root.join(&*name)).await {
                Ok(true) => {}         // the bucket dir is gone — prune
                Ok(false) => continue, // live bucket — keep the record
                Err(err) => {
                    // F11: a probe error is not "gone" — keep the record
                    // (the next pass re-probes).
                    tracing::warn!(bucket = %name, error = %err, "stale-bucket probe failed; the record is kept");
                    continue;
                }
            }
            if let Err(err) = self.storage.remove_bucket_state(&name).await {
                tracing::warn!(error = %err, "stale bucket record not pruned");
                continue;
            }
            pruned += 1;
        }
        Ok(pruned)
    }

    /// Stage 6: meta-orphan reclamation — meta entries whose object file no
    /// longer exists are deleted (fs-backend.md §8.3).
    async fn repair_meta_orphans(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        // Every live bucket (the store's own source of truth), then the
        // `OBJECT_META` entries of each — one bucket range scan per walk.
        let buckets = match self.storage.bucket_names("").await {
            Ok(buckets) => buckets,
            Err(err) => {
                actions.push(Err(err));
                return;
            }
        };
        for bucket in buckets {
            let records = match self.storage.meta_store().walk(&bucket).await {
                Ok(records) => records,
                Err(err) => {
                    actions.push(Err(err));
                    continue;
                }
            };
            for record in records {
                // The object path through the crate's own mapping (one
                // source of truth) — unrepresentable keys cannot be
                // object files. Cleanup always enforces the boundary (it
                // must never address outside the bucket); the bucket dir
                // resolves through the symlink policy, so a symlinked
                // bucket (follow_symlinks=true) is reclaimed against its
                // canonical target.
                let Ok(bucket_dir) = self.storage.bucket_dir(&bucket).await else {
                    continue;
                };
                let Ok(path) = self.storage.key_path(&bucket_dir, &record.key, true).await else {
                    continue;
                };
                match fsutil::is_absent(&path).await {
                    // F11: a probe error is not "gone" — the entry is
                    // kept and the error reported.
                    Ok(false) => continue,
                    Err(err) => {
                        actions.push(Err(err.into()));
                        continue;
                    }
                    Ok(true) => {}
                }
                record_repair(
                    actions,
                    self.dry_run,
                    format!(
                        "would reclaim orphaned meta entry for {} in {bucket}",
                        record.key
                    ),
                    format!(
                        "reclaimed orphaned meta entry for {} in {bucket}",
                        record.key
                    ),
                    async { self.storage.meta_store().remove(&bucket, &record.key).await },
                )
                .await;
            }
        }
    }
}

#[async_trait]
impl Cleanup for FsCleanup {
    type Error = Error;

    async fn repair(&self, kind: RepairKind) -> Result<ActionStream<Error>, Error> {
        let mut actions = Vec::new();
        // FR-023: a read-only run writes nothing through the storage — and
        // since the read-only amendment the run's state dir **is** the root's
        // own `<root>/.tinio` (`FsStorage::resolve`), so every
        // state-dir-writing stage below would write the served root itself.
        // None of them may run for real; a dry-run pass still walks them (it
        // records what it would clear and removes nothing), which is the
        // shape the doctor's read-only check needs.
        if self.access.is_read_only() && !self.dry_run {
            tracing::debug!(
                "read-only mode: skipping every state-dir-writing repair stage \
                 (the state dir is the root's own .tinio)"
            );
            return Ok(Box::pin(stream::iter(actions)));
        }
        self.repair_tmp(&mut actions).await;
        // The block store's own `tmp/` is the same class of residue as
        // `tmp/` itself (a crashed stage).
        self.repair_block_residue(&mut actions).await;
        // FR-023: the two stages that also write the served tree's own
        // `.tinio` (unpublished delete tombstones and in-bucket staging
        // residue) are skipped whenever the run is read-only, dry-run
        // included. A root previously served read-write may legitimately
        // still hold one — private state, deliberately left alone: the mode
        // promises not to write the root, not to clean it.
        if self.access.is_read_only() {
            tracing::debug!(
                "read-only mode: skipping the root-writing repair stages \
                 (delete tombstones, bucket staging residue)"
            );
        } else {
            self.repair_delete_tombstones(&mut actions).await;
            self.repair_bucket_staging(&mut actions).await;
        }
        self.repair_multipart_orphans(&mut actions).await;
        self.repair_orphan_upload_dirs(&mut actions).await;
        self.repair_buckets(&mut actions).await;
        if kind == RepairKind::Full {
            self.repair_meta_orphans(&mut actions).await;
            self.report_block_residue(&mut actions).await;
        }
        Ok(Box::pin(stream::iter(actions)))
    }

    async fn reclaim_meta_orphans(&self) -> Result<ActionStream<Error>, Error> {
        let mut actions = Vec::new();
        // A bucket directory removed out-of-band mid-run also orphans its
        // derived state — BUCKETS, OBJECT_META, UPLOADS, PARTS. The
        // meta-orphan walk below only visits *live* buckets, so the
        // stale-record stage must run here too (the scanner's background
        // path is otherwise blind until the next startup repair). Cheap:
        // one read of the BUCKETS table plus an existence probe per row.
        self.repair_buckets(&mut actions).await;
        self.repair_meta_orphans(&mut actions).await;
        Ok(Box::pin(stream::iter(actions)))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs::Permissions;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{fs::OpenOptions, sync::Arc, time::SystemTime};

    use futures::StreamExt;
    use tokio::fs;

    use super::*;
    use crate::{
        _core::{
            acl::Acl,
            bucket,
            meta::version::Stored,
            object,
            pipeline::InlineRunner,
            storage::{BucketOps, ObjectOps},
        },
        _util::testing::body,
        FsOptions,
        testutil::fs_options,
    };

    async fn collect(actions: ActionStream<Error>) -> Vec<RepairAction> {
        let mut out = Vec::new();
        let mut actions = actions;
        while let Some(action) = actions.next().await {
            out.push(action.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn startup_repair_clears_tmp_and_orphans() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();

        // Leftover temp file.
        fs::create_dir(state.path().join("tmp")).await.unwrap();
        fs::write(state.path().join("tmp/upload-leftover"), b"x")
            .await
            .unwrap();

        // Bucket-orphaned multipart subtree.
        fs::create_dir_all(state.path().join("multipart/gone-bucket/u1"))
            .await
            .unwrap();
        fs::write(state.path().join("multipart/gone-bucket/u1/part-1"), b"x")
            .await
            .unwrap();

        // Stale bucket record.
        storage
            .bucket_store()
            .record(&bucket::name("gone-bucket").unwrap(), SystemTime::now())
            .await
            .unwrap();

        // A live bucket + upload must be untouched (a real upload: the
        // record commits before its directory exists).
        let live = bucket::name("live-bucket").unwrap();
        storage
            .create_bucket(&live, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .multipart_store()
            .create(
                &live,
                &object::key("k").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions.iter().any(|a| a.description.contains("temp file")),
            "{actions:?}"
        );
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("gone-bucket")),
            "{actions:?}"
        );

        assert!(!state.path().join("tmp/upload-leftover").exists());
        assert!(!state.path().join("multipart/gone-bucket").exists());
        let all = storage.bucket_store().load_all().await.unwrap();
        assert_eq!(all.len(), 1, "stale entry pruned, live entry kept: {all:?}");
        assert_eq!(all[0].0, "live-bucket");
        // The live upload's record is untouched.
        assert!(storage.multipart_store().has_uploads(&live).await.unwrap());
    }

    #[tokio::test]
    async fn startup_repair_clears_bucket_staging_residue() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/.tinio"))
            .await
            .unwrap();
        fs::write(root.path().join("data/.tinio/aaaa"), b"residue")
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/sub/.tinio"))
            .await
            .unwrap();
        fs::write(root.path().join("data/sub/.tinio/bbbb"), b"residue")
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/emptydir/.tinio"))
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("staging residue")),
            "{actions:?}"
        );
        assert!(!root.path().join("data/.tinio").exists());
        assert!(!root.path().join("data/sub/.tinio").exists());
        assert!(!root.path().join("data/emptydir/.tinio").exists());
        assert!(root.path().join("data/sub").is_dir());
    }

    #[tokio::test]
    async fn startup_repair_clears_delete_tombstones() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let tombstone = root
            .path()
            .join(".tinio")
            .join("deleting")
            .join("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        fs::create_dir_all(&tombstone).await.unwrap();
        fs::write(tombstone.join("leftover.bin"), b"was-a-bucket")
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions.iter().any(|a| a.description.contains("tombstone")),
            "{actions:?}"
        );
        assert!(!tombstone.exists());
    }

    #[tokio::test]
    async fn startup_repair_enqueues_delete_tombstones_on_the_removal_lane() {
        // The tombstone stage always routes through the storage's removal
        // lane (D-B) — the action text says "enqueued", never "cleared",
        // and the lane (an inline runner under test) clears the leftover.
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let tombstone = root
            .path()
            .join(".tinio")
            .join("deleting")
            .join("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        fs::create_dir_all(&tombstone).await.unwrap();
        fs::write(tombstone.join("leftover.bin"), b"was-a-bucket")
            .await
            .unwrap();
        // A stale bucket record the repair must still prune.
        storage
            .bucket_store()
            .record(&bucket::name("gone-bucket").unwrap(), SystemTime::now())
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("enqueued") && a.description.contains("tombstone")),
            "{actions:?}"
        );
        assert!(
            !tombstone.exists(),
            "the removal lane (inline runner) cleared the leftover"
        );
        // The other stages still run: the stale record is pruned.
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("stale bucket record")),
            "{actions:?}"
        );
        assert_eq!(storage.bucket_store().load_all().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn startup_repair_drains_uploads_of_a_missing_bucket() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let gone = bucket::name("gone-bucket").unwrap();
        storage
            .create_bucket(&gone, None, &Acl::default_private(None))
            .await
            .unwrap();
        let k = object::key("k").unwrap();
        storage
            .multipart_store()
            .create(
                &gone,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        assert!(storage.multipart_store().has_uploads(&gone).await.unwrap());
        fs::remove_dir_all(root.path().join("gone-bucket"))
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let _ = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            !storage.multipart_store().has_uploads(&gone).await.unwrap(),
            "UPLOADS of a missing bucket must be drained"
        );
    }

    #[tokio::test]
    async fn orphan_upload_dirs_are_removed_after_grace() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("live").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();

        // A live upload: record committed, directory present.
        let k = object::key("k").unwrap();
        let upload = storage
            .multipart_store()
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
        let live_dir = state.path().join("multipart/live").join(&upload.upload_id);
        fs::create_dir_all(&live_dir).await.unwrap();
        fs::write(live_dir.join("part-1"), b"x").await.unwrap();

        // An orphan: directory with an old part, no UPLOADS record.
        let orphan_dir = state.path().join("multipart/live/u-orphan");
        fs::create_dir_all(&orphan_dir).await.unwrap();
        let orphan_part = orphan_dir.join("part-1");
        fs::write(&orphan_part, b"x").await.unwrap();
        let handle = OpenOptions::new().write(true).open(&orphan_part).unwrap();
        handle
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000))
            .unwrap();
        drop(handle);

        // A young orphan (fresh parts) must survive the grace.
        let young_dir = state.path().join("multipart/live/u-young");
        fs::create_dir_all(&young_dir).await.unwrap();
        fs::write(young_dir.join("part-1"), b"x").await.unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default())
            .with_multipart_grace(Duration::from_secs(60));
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        let removed: Vec<&str> = actions
            .iter()
            .filter(|a| a.description.contains("orphaned upload"))
            .map(|a| a.description.as_str())
            .collect();
        assert_eq!(removed.len(), 1, "{actions:?}");
        assert!(!orphan_dir.exists());
        assert!(young_dir.exists());
        assert!(live_dir.exists());
        assert!(storage.multipart_store().has_uploads(&b).await.unwrap());
    }

    #[tokio::test]
    async fn live_upload_with_domain_invalid_stored_key_is_not_an_orphan() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("live").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let k = object::key("k").unwrap();
        let upload = storage
            .multipart_store()
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
        storage
            .multipart_store()
            .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"), None)
            .await
            .unwrap();
        // Corrupt the stored key out-of-band: the validated view no
        // longer lists the upload, but the row is still there.
        storage
            .multipart_store()
            .overwrite_stored_key(&b, &upload.upload_id, "../evil")
            .await
            .unwrap();
        assert!(
            storage
                .multipart_store()
                .walk_uploads()
                .await
                .unwrap()
                .is_empty(),
            "the validated view skips the invalid-key row"
        );

        // Zero grace: only the liveness check can shield the upload.
        let cleanup = FsCleanup::new(&storage, CleanupOptions::default())
            .with_multipart_grace(Duration::ZERO);
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            !actions
                .iter()
                .any(|a| a.description.contains("orphaned upload")),
            "a live upload must never be judged an orphan: {actions:?}"
        );
        let dir = state.path().join("multipart/live").join(&upload.upload_id);
        assert!(dir.exists(), "the live upload's directory survives");
        assert!(
            storage.multipart_store().has_uploads(&b).await.unwrap(),
            "the live upload's rows are not drained"
        );
    }

    #[tokio::test]
    async fn orphan_stage_toctou_order_protects_live_uploads() {
        // §5.7 pins the TOCTOU order: enumerate the `multipart/` tree
        // first, then read UPLOADS in one read transaction. `create`
        // commits the UPLOADS row before any directory exists (the first
        // put_part creates it), so an enumerated directory always has its
        // row visible to the later transaction — a `create` racing between
        // the two cannot be misjudged. Both halves are pinned with a ZERO
        // grace: the idle check protects nothing, so only the raw UPLOADS
        // membership shields a live upload.

        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("live").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let k = object::key("k").unwrap();
        let cleanup = || {
            FsCleanup::new(&storage, CleanupOptions::default()).with_multipart_grace(Duration::ZERO)
        };

        // Half 1: record committed, no directory yet (the state of a
        // `create` racing the enumeration) — nothing to judge, the
        // record survives.
        let upload = storage
            .multipart_store()
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
        let actions = collect(cleanup().repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            !actions
                .iter()
                .any(|a| a.description.contains("orphaned upload")),
            "{actions:?}"
        );
        assert!(storage.multipart_store().has_uploads(&b).await.unwrap());

        // Half 2: the directory now exists (first put_part) — the
        // enumeration+read order must see its committed record even
        // without any idle grace.
        storage
            .multipart_store()
            .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"), None)
            .await
            .unwrap();
        let live_dir = state.path().join("multipart/live").join(&upload.upload_id);
        let actions = collect(cleanup().repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            !actions
                .iter()
                .any(|a| a.description.contains("orphaned upload")),
            "{actions:?}"
        );
        assert!(live_dir.exists());
        assert!(storage.multipart_store().has_uploads(&b).await.unwrap());

        // Control: a true orphan (no UPLOADS row) IS removed under
        // zero grace — the stage is not vacuously disabled.
        let orphan_dir = state.path().join("multipart/live/u-orphan");
        fs::create_dir_all(&orphan_dir).await.unwrap();
        fs::write(orphan_dir.join("part-1"), b"x").await.unwrap();
        let actions = collect(cleanup().repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("orphaned upload")),
            "{actions:?}"
        );
        assert!(!orphan_dir.exists());
        assert!(live_dir.exists());
    }

    #[tokio::test]
    async fn dry_run_reports_orphan_upload_dirs_without_removing() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let orphan_dir = state.path().join("multipart/live/u-orphan");
        fs::create_dir_all(&orphan_dir).await.unwrap();
        let orphan_part = orphan_dir.join("part-1");
        fs::write(&orphan_part, b"x").await.unwrap();
        let handle = OpenOptions::new().write(true).open(&orphan_part).unwrap();
        handle
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000))
            .unwrap();
        drop(handle);

        let cleanup = FsCleanup::new(
            &storage,
            CleanupOptions {
                dry_run: true,
                ..Default::default()
            },
        )
        .with_multipart_grace(Duration::from_secs(60));
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.starts_with("would remove orphaned upload")),
            "{actions:?}"
        );
        assert!(orphan_dir.exists());
    }

    #[tokio::test]
    async fn dry_run_touches_nothing() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        fs::create_dir(state.path().join("tmp")).await.unwrap();
        fs::write(state.path().join("tmp/upload-leftover"), b"x")
            .await
            .unwrap();

        let cleanup = FsCleanup::new(
            &storage,
            CleanupOptions {
                dry_run: true,
                ..Default::default()
            },
        );
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.starts_with("would clear")),
            "{actions:?}"
        );
        assert!(state.path().join("tmp/upload-leftover").exists());
    }

    #[tokio::test]
    async fn partless_orphan_dir_uses_dir_mtime_for_idle() {
        // An orphan upload dir with NO part files has no part mtime to
        // anchor the idle age — `orphan_idle_since` falls back to the
        // directory mtime. A zero grace forces the fallback into the
        // judgment (the dir is fresh, so only the grace can be the
        // discriminator).

        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("live").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();

        // A part-less orphan (no UPLOADS record, no part files).
        let orphan_dir = state.path().join("multipart/live/u-partless");
        fs::create_dir_all(&orphan_dir).await.unwrap();

        // Zero grace: the fresh dir mtime must not block removal
        // (age 0 is not < 0), and the fallback must have run.
        let cleanup = FsCleanup::new(&storage, CleanupOptions::default())
            .with_multipart_grace(Duration::ZERO);
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("orphaned upload")),
            "{actions:?}"
        );
        assert!(!orphan_dir.exists());

        // The same dir under a generous grace survives (dir mtime is
        // fresh — younger than the grace).
        fs::create_dir_all(&orphan_dir).await.unwrap();
        let cleanup = FsCleanup::new(&storage, CleanupOptions::default())
            .with_multipart_grace(Duration::from_secs(3600));
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            !actions
                .iter()
                .any(|a| a.description.contains("orphaned upload")),
            "{actions:?}"
        );
        assert!(orphan_dir.exists());
    }

    #[tokio::test]
    async fn bucket_staging_file_residue_is_cleared() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/sub"))
            .await
            .unwrap();
        fs::write(root.path().join("data/.tinio"), b"residue")
            .await
            .unwrap();
        fs::write(root.path().join("data/sub/.tinio"), b"residue")
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert_eq!(
            actions
                .iter()
                .filter(|a| a.description.contains("staging residue"))
                .count(),
            2,
            "{actions:?}"
        );
        assert!(!root.path().join("data/.tinio").exists());
        assert!(!root.path().join("data/sub/.tinio").exists());
        assert!(root.path().join("data/sub").is_dir());
    }

    #[tokio::test]
    async fn reclaim_stale_buckets_prunes_and_counts() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let stale = bucket::name("stale-bucket").unwrap();
        let live = bucket::name("live-bucket").unwrap();
        storage
            .create_bucket(&live, None, &Acl::default_private(None))
            .await
            .unwrap();
        // A stale record: the bucket directory is gone out-of-band.
        storage
            .bucket_store()
            .record(&stale, SystemTime::now())
            .await
            .unwrap();
        fs::create_dir(root.path().join("stale-bucket"))
            .await
            .unwrap();
        fs::remove_dir_all(root.path().join("stale-bucket"))
            .await
            .unwrap();
        assert_eq!(storage.bucket_store().load_all().await.unwrap().len(), 2);

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let pruned = cleanup.reclaim_stale_buckets().await.unwrap();
        assert_eq!(pruned, 1, "the stale record is pruned exactly once");
        let all = storage.bucket_store().load_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, "live-bucket");
        // A second pass finds nothing to prune.
        assert_eq!(cleanup.reclaim_stale_buckets().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn full_repair_reclaims_meta_orphans() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let k = object::key("a.txt").unwrap();
        storage.put_object(&b, &k, body(b"x")).await.unwrap();

        // Out-of-band deletion: the object vanishes, the meta entry stays.
        fs::remove_file(root.path().join("data/a.txt"))
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Full).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("orphaned meta")),
            "{actions:?}"
        );
        assert!(storage.meta_store().walk(&b).await.unwrap().is_empty());

        // Startup repair does NOT reclaim meta orphans (scanner's job).
        storage.put_object(&b, &k, body(b"y")).await.unwrap();
        fs::remove_file(root.path().join("data/a.txt"))
            .await
            .unwrap();
        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            !actions
                .iter()
                .any(|a| a.description.contains("orphaned meta")),
            "{actions:?}"
        );
        assert_eq!(storage.meta_store().walk(&b).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reclaim_meta_orphans_cleans_a_vanished_bucket() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let k = object::key("a.txt").unwrap();
        storage.put_object(&b, &k, body(b"x")).await.unwrap();
        assert_eq!(storage.meta_store().walk(&b).await.unwrap().len(), 1);
        fs::remove_dir_all(root.path().join("data")).await.unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.reclaim_meta_orphans().await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("stale bucket record")),
            "{actions:?}"
        );
        assert!(storage.bucket_store().load_all().await.unwrap().is_empty());
        assert!(storage.meta_store().walk(&b).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn reclaim_meta_orphans_removes_only_missing() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let gone = object::key("gone.txt").unwrap();
        let alive = object::key("alive.txt").unwrap();
        storage.put_object(&b, &gone, body(b"x")).await.unwrap();
        storage.put_object(&b, &alive, body(b"y")).await.unwrap();
        fs::remove_file(root.path().join("data/gone.txt"))
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.reclaim_meta_orphans().await.unwrap()).await;
        assert_eq!(actions.len(), 1, "{actions:?}");
        assert!(storage.meta_store().walk(&b).await.unwrap().len() == 1);
    }

    #[tokio::test]
    async fn tmp_directory_entry_reports_a_remove_error() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        fs::create_dir_all(state.path().join("tmp/subdir"))
            .await
            .unwrap();
        fs::write(state.path().join("tmp/upload-leftover"), b"x")
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let mut actions = cleanup.repair(RepairKind::Startup).await.unwrap();
        let mut errs = 0;
        while let Some(action) = actions.next().await {
            if action.is_err() {
                errs += 1;
            }
        }
        assert_eq!(errs, 1, "the directory entry fails as a file removal");
        assert!(!state.path().join("tmp/upload-leftover").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn repair_reports_unreadable_tmp_and_multipart_trees() {
        // Permission failures are REPORTED, never silently skipped: an
        // unreadable `tmp/` and an unreadable `multipart/` root both
        // surface as error actions (the cleanup must not pretend the
        // state is clean when it could not even enumerate it).

        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        fs::create_dir(state.path().join("tmp")).await.unwrap();
        fs::write(state.path().join("tmp/leftover"), b"x")
            .await
            .unwrap();
        fs::create_dir_all(state.path().join("multipart/gone/u1"))
            .await
            .unwrap();
        fs::write(state.path().join("multipart/gone/u1/part-1"), b"x")
            .await
            .unwrap();

        fs::set_permissions(state.path().join("tmp"), Permissions::from_mode(0o000))
            .await
            .unwrap();
        fs::set_permissions(
            state.path().join("multipart"),
            Permissions::from_mode(0o000),
        )
        .await
        .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let mut actions = cleanup.repair(RepairKind::Startup).await.unwrap();
        let mut errs = 0;
        while let Some(action) = actions.next().await {
            if action.is_err() {
                errs += 1;
            }
        }
        // tmp read (stage 1) + multipart read (stages 3 and 4).
        assert!(errs >= 2, "unreadable trees must be reported: {errs}");

        // The repair must not have touched anything.
        fs::set_permissions(state.path().join("tmp"), Permissions::from_mode(0o700))
            .await
            .unwrap();
        fs::set_permissions(
            state.path().join("multipart"),
            Permissions::from_mode(0o700),
        )
        .await
        .unwrap();
        assert!(state.path().join("tmp/leftover").exists());
        assert!(state.path().join("multipart/gone/u1/part-1").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn staging_walk_survives_an_unreadable_subdir() {
        use crate::_core::acl::Acl;
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/blocked"))
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/ok/.tinio"))
            .await
            .unwrap();
        fs::write(root.path().join("data/ok/.tinio/aaaa"), b"x")
            .await
            .unwrap();
        fs::set_permissions(
            root.path().join("data/blocked"),
            Permissions::from_mode(0o000),
        )
        .await
        .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("staging residue")),
            "{actions:?}"
        );
        assert!(!root.path().join("data/ok/.tinio").exists());
        fs::set_permissions(
            root.path().join("data/blocked"),
            Permissions::from_mode(0o700),
        )
        .await
        .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bucket_staging_descents_symlinked_dirs_when_following() {
        use crate::_core::acl::Acl;
        // A symlinked directory INSIDE a bucket is part of the bucket
        // only when following is enabled (the follow policy, one source
        // of truth): with following the residue behind the link is
        // cleared; without it the link is not descended and the residue
        // survives.
        async fn run(follow_symlinks: bool) -> (bool, bool, bool) {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            fs::create_dir_all(outside.path().join(".tinio"))
                .await
                .unwrap();
            fs::write(outside.path().join(".tinio/aaaa"), b"residue")
                .await
                .unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks,
                    ..fs_options()
                },
            )
            .unwrap();
            let b = bucket::name("data").unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            fs::create_dir(root.path().join("data/real")).await.unwrap();
            symlink(outside.path(), root.path().join("data/link")).unwrap();

            let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
            let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
            (
                follow_symlinks,
                actions
                    .iter()
                    .any(|a| a.description.contains("staging residue")),
                outside.path().join(".tinio").exists(),
            )
        }
        let (following, reported, residue_exists) = run(true).await;
        assert!(following && reported && !residue_exists);
        let (follow, reported, residue_exists) = run(false).await;
        assert!(!follow && !reported && residue_exists);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn full_repair_reports_unreadable_roots_and_probes() {
        use crate::_core::acl::Acl;
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let k = object::key("a.txt").unwrap();
        storage.put_object(&b, &k, body(b"x")).await.unwrap();
        // A multipart subtree + a stale bucket record to probe.
        storage
            .bucket_store()
            .record(&bucket::name("gone-bucket").unwrap(), SystemTime::now())
            .await
            .unwrap();
        fs::create_dir_all(state.path().join("multipart/gone-bucket/u1"))
            .await
            .unwrap();

        fs::set_permissions(root.path(), Permissions::from_mode(0o000))
            .await
            .unwrap();
        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let mut actions = cleanup.repair(RepairKind::Full).await.unwrap();
        let mut errs = 0;
        while let Some(action) = actions.next().await {
            if action.is_err() {
                errs += 1;
            }
        }
        fs::set_permissions(root.path(), Permissions::from_mode(0o700))
            .await
            .unwrap();
        assert!(errs >= 3, "unreadable root must be reported: {errs}");
        assert!(
            storage.meta_store().walk(&b).await.unwrap().len() == 1,
            "the record is kept (a probe error is not gone)"
        );

        // The scanner count path: probe errors warn and keep the row.
        fs::set_permissions(root.path(), Permissions::from_mode(0o000))
            .await
            .unwrap();
        let pruned = cleanup.reclaim_stale_buckets().await.unwrap();
        fs::set_permissions(root.path(), Permissions::from_mode(0o700))
            .await
            .unwrap();
        assert_eq!(pruned, 0, "no record is pruned on a probe error");
        assert_eq!(storage.bucket_store().load_all().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn invalid_bucket_names_skip_the_drain_but_not_the_removal() {
        // A multipart subtree under an INVALID bucket name (e.g. too
        // short) cannot be drained — the `bucket::name` guard skips the
        // drain (never a panic) but the subtree itself is still removed:
        // stage 3 for a missing bucket dir, stage 4 for a live one.

        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();

        // Stage 3: invalid name + missing bucket dir — removed, drain
        // skipped (the `bucket::name` guard answers `Err`).
        fs::create_dir_all(state.path().join("multipart/xx/u1"))
            .await
            .unwrap();
        fs::write(state.path().join("multipart/xx/u1/part-1"), b"x")
            .await
            .unwrap();

        // Stage 4: invalid name + PRESENT bucket dir — the upload dir
        // is an orphan (never live) and is removed with the drain
        // skipped.
        fs::create_dir(root.path().join("xx")).await.unwrap();
        let orphan_dir = state.path().join("multipart/xx/u-orphan");
        fs::create_dir_all(&orphan_dir).await.unwrap();
        fs::write(orphan_dir.join("part-1"), b"x").await.unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default())
            .with_multipart_grace(Duration::ZERO);
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(!state.path().join("multipart/xx/u1").exists());
        assert!(!orphan_dir.exists());
        // Both removals are reported; neither drain panics.
        assert_eq!(
            actions
                .iter()
                .filter(
                    |a| a.description.contains("multipart") || a.description.contains("orphaned")
                )
                .count(),
            2,
            "{actions:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn orphan_stage_reports_an_unreadable_upload_dir() {
        use crate::_core::acl::Acl;
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("live").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let blocked = state.path().join("multipart/live/u-blocked");
        fs::create_dir_all(&blocked).await.unwrap();
        fs::write(blocked.join("part-1"), b"x").await.unwrap();
        fs::set_permissions(&blocked, Permissions::from_mode(0o000))
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let mut actions = cleanup.repair(RepairKind::Startup).await.unwrap();
        let mut errs = 0;
        while let Some(action) = actions.next().await {
            if action.is_err() {
                errs += 1;
            }
        }
        fs::set_permissions(&blocked, Permissions::from_mode(0o700))
            .await
            .unwrap();
        assert_eq!(errs, 1, "the unreadable upload dir must be reported");
        assert!(blocked.exists(), "nothing is removed on a read failure");
    }

    #[tokio::test]
    async fn stray_files_in_the_multipart_tree_are_ignored() {
        // Stage 4 (orphan upload dirs) skips non-directory entries at
        // both levels of the `multipart/` tree: a stray file where a
        // bucket dir is expected, and one where an upload dir is
        // expected. Neither is an orphan, neither is removed, and the
        // repair reports no error for them.
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("live").unwrap();
        let stray = bucket::name("stray").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .create_bucket(&stray, None, &Acl::default_private(None))
            .await
            .unwrap();
        fs::create_dir_all(state.path().join("multipart/live"))
            .await
            .unwrap();
        // A stray file where the upload dir is expected.
        fs::write(state.path().join("multipart/live/stray-part"), b"x")
            .await
            .unwrap();
        // A stray file where a bucket dir is expected (the name is a
        // live bucket, so stage 3's orphan probe skips it too).
        fs::write(state.path().join("multipart/stray"), b"x")
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let mut actions = cleanup.repair(RepairKind::Startup).await.unwrap();
        let mut errs = 0;
        while let Some(action) = actions.next().await {
            if action.is_err() {
                errs += 1;
            }
        }
        assert_eq!(errs, 0, "stray files must not fail the repair");
        assert!(state.path().join("multipart/live/stray-part").exists());
        assert!(state.path().join("multipart/stray").exists());
    }

    #[tokio::test]
    async fn read_only_repair_writes_nothing_in_the_root_that_is_its_state_dir() {
        // FR-023: read-only mode never writes the storage root — and the
        // read-only run's state dir IS the root's own `<root>/.tinio`, so
        // every state-dir stage (tmp, block staging, multipart residue,
        // stale records) would write the root and must be skipped.
        let root = tempfile::tempdir().unwrap();
        // The residue a crashed read-write run left in the root's own state
        // dir: one temp file, one block staging file, one multipart residue
        // tree — plus a tombstone and an in-bucket staging file.
        fs::create_dir_all(root.path().join(".tinio/tmp"))
            .await
            .unwrap();
        fs::write(root.path().join(".tinio/tmp/upload-leftover"), b"x")
            .await
            .unwrap();
        fs::create_dir_all(root.path().join(".tinio/blobs/data/tmp"))
            .await
            .unwrap();
        fs::write(
            root.path().join(".tinio/blobs/data/tmp/block-leftover"),
            b"y",
        )
        .await
        .unwrap();
        fs::create_dir_all(root.path().join(".tinio/multipart/data/aaaa"))
            .await
            .unwrap();
        fs::write(root.path().join(".tinio/multipart/data/aaaa/part"), b"z")
            .await
            .unwrap();
        let tombstone = tombstone::dir(root.path()).join("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        fs::create_dir_all(&tombstone).await.unwrap();
        fs::write(tombstone.join("leftover.bin"), b"was-a-bucket")
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/.tinio"))
            .await
            .unwrap();
        fs::write(root.path().join("data/.tinio/aaaa"), b"residue")
            .await
            .unwrap();

        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                access: AccessMode::ReadOnly,
                ..fs_options()
            },
        )
        .unwrap();
        let cleanup = FsCleanup::new(
            &storage,
            CleanupOptions {
                access: AccessMode::ReadOnly,
                ..Default::default()
            },
        );
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;

        assert!(
            actions.is_empty(),
            "no stage may run read-only: {actions:?}"
        );
        for path in [
            ".tinio/tmp/upload-leftover",
            ".tinio/blobs/data/tmp/block-leftover",
            ".tinio/multipart/data/aaaa/part",
            "data/.tinio/aaaa",
        ] {
            assert!(
                root.path().join(path).exists(),
                "{path} must survive a read-only start"
            );
        }
        assert!(tombstone.exists(), "the tombstone is left alone");

        // A dry-run pass (the doctor's read-only check) still reports what it
        // would clear — and removes nothing.
        let dry = FsCleanup::new(
            &storage,
            CleanupOptions {
                dry_run: true,
                access: AccessMode::ReadOnly,
            },
        );
        let reported = collect(dry.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            reported
                .iter()
                .any(|a| a.description.contains("would clear")),
            "a dry-run read-only pass still reports residue: {reported:?}"
        );
        assert!(root.path().join(".tinio/tmp/upload-leftover").exists());
    }

    #[tokio::test]
    async fn delete_tombstone_enqueue_failure_is_reported() {
        // A tombstone whose removal-lane enqueue is rejected (a
        // shut-down pipeline) is REPORTED as an error action — the
        // action stream must never say "enqueued" when it was not — and
        // the leftover stays for the scanner.
        let root = tempfile::tempdir().unwrap();
        let lane = Arc::new(InlineRunner::default());
        lane.shutdown();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                remove_pipeline: lane,
                ..fs_options()
            },
        )
        .unwrap();
        let tombstone = root
            .path()
            .join(".tinio")
            .join("deleting")
            .join("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        fs::create_dir_all(&tombstone).await.unwrap();
        fs::write(tombstone.join("leftover.bin"), b"was-a-bucket")
            .await
            .unwrap();

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let mut actions = cleanup.repair(RepairKind::Startup).await.unwrap();
        let mut errs = 0;
        while let Some(action) = actions.next().await {
            if action.is_err() {
                errs += 1;
            }
        }
        assert_eq!(errs, 1, "the rejected enqueue must be reported");
        assert!(
            tombstone.exists(),
            "the leftover stays for the scanner (never a false removal)"
        );
    }

    /// A versioned bucket holding `notes.txt` overwritten once: `one` is a
    /// history row naming a stored block, `two` is current.
    #[cfg(feature = "versioning")]
    async fn versioned_fixture(storage: &FsStorage) -> (Name, object::Key, Stored) {
        use crate::{_core::version::State, testutil};

        let (b, key) = testutil::versioned_bucket(storage, State::Enabled, &[b"one", b"two"]).await;
        let row = storage
            .versions(&b)
            .latest(&b, &key)
            .await
            .unwrap()
            .expect("the superseded version's row")
            .1;
        (b, key, row)
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn startup_repair_clears_block_staging_residue() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let staged = state.path().join("blobs/data/tmp/aaaa");
        fs::create_dir_all(staged.parent().unwrap()).await.unwrap();
        fs::write(&staged, b"torn").await.unwrap();

        // Dry run first: the proposal names the file and removes nothing.
        let dry = FsCleanup::new(
            &storage,
            CleanupOptions {
                dry_run: true,
                ..CleanupOptions::default()
            },
        );
        let actions = collect(dry.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("block staging file aaaa")),
            "{actions:?}"
        );
        assert!(staged.exists(), "a dry run never removes");

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
        assert!(
            actions.iter().any(|a| a
                .description
                .contains("cleared leftover block staging file")),
            "{actions:?}"
        );
        assert!(!staged.exists(), "the residue is cleared at startup");
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn doctor_reports_missing_blocks_and_block_residue() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let (b, _key, row) = versioned_fixture(&storage).await;
        let versions = storage.versions(&b);
        let dry = FsCleanup::new(
            &storage,
            CleanupOptions {
                dry_run: true,
                ..CleanupOptions::default()
            },
        );

        // A healthy store: the check says nothing about blocks.
        let actions = collect(dry.repair(RepairKind::Full).await.unwrap()).await;
        assert!(
            !actions.iter().any(|a| a.description.contains("block")),
            "a clean block store is no finding: {actions:?}"
        );

        // A row whose block the store does not hold, and a block no row
        // names: both are reported, neither is repaired.
        let missing = *row.blob.hash();
        versions.blobs().blocks().remove(&missing).await.unwrap();
        let orphan = versions.blobs().put(body(b"orphan")).await.unwrap();
        let actions = collect(dry.repair(RepairKind::Full).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("does not hold")
                    && a.description.contains(&missing.to_hex())),
            "the missing block is named: {actions:?}"
        );
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("referenced by no version row")),
            "the residue is reported: {actions:?}"
        );
        assert!(
            versions
                .blobs()
                .blocks()
                .contains(orphan.hash())
                .await
                .unwrap(),
            "the check reports; the sweep reclaims"
        );

        // The block check runs on the `--fix` path too (the doctor's
        // `--fix` invokes `repair` once; a report-only stage that only ran
        // on the dry-run pass would leave the fix run without its answer).
        // It stays report-only there: the findings ride the pass, and the
        // fixed build's ledger cannot count them as repairs — their level
        // is `Lint`, the "check finding, no action needed" class.
        let fixing = FsCleanup::new(&storage, CleanupOptions::default());
        let actions = collect(fixing.repair(RepairKind::Full).await.unwrap()).await;
        let check_rows: Vec<&RepairAction> = actions
            .iter()
            .filter(|a| {
                a.description.contains("referenced by no version row")
                    || a.description.contains("does not hold")
            })
            .collect();
        assert!(
            !check_rows.is_empty(),
            "a fix run reports the block check's findings too: {actions:?}"
        );
        assert!(
            check_rows
                .iter()
                .all(|a| a.level == RepairActionLevel::Lint),
            "the fix pass's check rows are findings, never applied repairs: {check_rows:?}"
        );
        assert!(
            versions
                .blobs()
                .blocks()
                .contains(orphan.hash())
                .await
                .unwrap(),
            "the check is report-only on the fix path: the sweep reclaims the orphan"
        );
        assert!(
            !versions.blobs().blocks().contains(&missing).await.unwrap(),
            "the report never re-creates a missing block"
        );
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn the_startup_verify_counts_corrupt_blocks() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let (b, _key, row) = versioned_fixture(&storage).await;

        let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
        let (checked, corrupt) = cleanup.verify_all_blocks().await.unwrap();
        assert!(checked >= 1, "the fixture's block is checked");
        assert_eq!(corrupt, 0, "an intact store verifies clean");

        // Corrupted bytes (same length): the digest check catches them.
        let path = storage
            .versions(&b)
            .blobs()
            .blocks()
            .block_path(row.blob.hash());
        let mut corrupted = fs::read(&path).await.unwrap();
        corrupted[0] ^= 0xff;
        fs::write(&path, &corrupted).await.unwrap();

        let (checked_again, corrupt) = cleanup.verify_all_blocks().await.unwrap();
        assert_eq!(checked_again, checked);
        assert_eq!(corrupt, 1, "the corrupted block is counted");
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn doctor_verifies_blocks_against_their_own_names() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let (b, _key, row) = versioned_fixture(&storage).await;
        let versions = storage.versions(&b);
        let dry = FsCleanup::new(
            &storage,
            CleanupOptions {
                dry_run: true,
                ..CleanupOptions::default()
            },
        );
        // The stored block's path: the file name is the content hash.
        let path = versions.blobs().blocks().block_path(row.blob.hash());
        let stored = fs::read(&path).await.unwrap();

        // Corrupted bytes (same length): the digest check names the block.
        let mut corrupted = stored.clone();
        corrupted[0] ^= 0xff;
        fs::write(&path, &corrupted).await.unwrap();
        let actions = collect(dry.repair(RepairKind::Full).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("does not hash to its own name")
                    && a.description.contains(&row.blob.hash().to_hex())),
            "the corruption is reported: {actions:?}"
        );

        // Truncated bytes: the same name check catches them (a truncated
        // file cannot hash to the content address its name carries).
        fs::write(&path, &stored[..stored.len() - 1]).await.unwrap();
        let actions = collect(dry.repair(RepairKind::Full).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("does not hash to its own name")
                    && a.description.contains("corrupt or truncated")),
            "the truncation is reported: {actions:?}"
        );
        assert!(path.exists(), "the verify pass reports and repairs nothing");
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn doctor_reports_a_block_subtree_of_a_vanished_bucket() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        // The crash window between `remove_bucket_state`'s row drain and its
        // subtree removal: blocks under a bucket name whose directory is gone.
        let orphan = state.path().join("blobs/gone-bucket");
        fs::create_dir_all(orphan.join("blocks")).await.unwrap();
        fs::write(orphan.join("blocks/aaaa"), b"block")
            .await
            .unwrap();
        // A live bucket's subtree is not residue, and a live bucket whose
        // `BUCKETS` row is lost is still recoverable (the directory is the
        // authority `ensure_bucket` reads).
        let live = bucket::name("live-bucket").unwrap();
        storage
            .create_bucket(&live, None, &Acl::default_private(None))
            .await
            .unwrap();
        fs::create_dir_all(state.path().join("blobs/live-bucket"))
            .await
            .unwrap();

        let dry = FsCleanup::new(
            &storage,
            CleanupOptions {
                dry_run: true,
                ..CleanupOptions::default()
            },
        );
        let actions = collect(dry.repair(RepairKind::Full).await.unwrap()).await;
        assert!(
            actions
                .iter()
                .any(|a| a.description.contains("subtree for bucket gone-bucket")),
            "the orphaned subtree must be reported: {actions:?}"
        );
        assert!(
            !actions
                .iter()
                .any(|a| a.description.contains("subtree for bucket live-bucket")),
            "a live bucket's block subtree is not residue: {actions:?}"
        );
        assert!(
            orphan.exists(),
            "the doctor reports the subtree one pass early; only the sweep's orphan pass reclaims"
        );
    }
}
