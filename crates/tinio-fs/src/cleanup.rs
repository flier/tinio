//! The `Cleanup` trait implementation for the fs backend (task T070
//! foundation; scanner reclamation per T045).
//!
//! Per fs-backend.md §8 and failure-handling.md §3: startup repair handles
//! the fast, deterministic items (full `tmp/` clear, bucket `.tinio/`
//! staging residue, bucket-orphaned multipart subtrees, stale bucket
//! records); the full repair adds meta-orphan reclamation;
//! `reclaim_meta_orphans` is the scanner's background path. All modes
//! share one code path with a `dry_run` flag ([`CleanupOptions`]); **user
//! data (bucket directories and objects) is never touched** — only
//! tinio-private state.
//!
//! Home root-state-dir GC (part of the `Full` scope per failure-handling.md
//! §3) needs read-only-mode state relocation and lands with US2 (T076).

use std::{
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use futures::stream;
use tinio_core::cleanup::{
    ActionStream, Cleanup, CleanupOptions, RepairAction, RepairActionLevel, RepairKind,
};

use crate::{
    backend::FsStorage,
    bucket,
    error::Error,
    fsutil::{ok_if_missing, tmp_entries},
    path::{MULTIPART_DIR_NAME, STATE_DIR_NAME},
    sweep,
};

/// The idle anchor of an orphan upload dir: the latest part mtime,
/// falling back to the directory mtime so a part-less orphan still has an
/// idle age (a vanished dir propagates `NotFound` — the caller skips).
async fn orphan_idle_since(dir: &Path) -> io::Result<SystemTime> {
    if let Some(latest) = crate::fsutil::latest_part_mtime(dir).await? {
        return Ok(latest);
    }
    tokio::fs::metadata(dir).await?.modified()
}

/// Report one repair operation: dry-run pushes the "would …" action;
/// otherwise the op runs and its outcome is reported ("did …" on success,
/// the error otherwise).
async fn record_repair(
    actions: &mut Vec<Result<RepairAction, Error>>,
    dry_run: bool,
    would: String,
    did: String,
    op: impl std::future::Future<Output = Result<(), Error>>,
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

/// The fs implementation of the [`Cleanup`] contract.
///
/// # Examples
///
/// ```rust
/// use tinio_core::cleanup::{Cleanup, CleanupOptions, RepairKind};
/// use futures::StreamExt;
/// use tinio_fs::{FsCleanup, FsOptions, FsStorage};
///
/// let root = tempfile::tempdir().unwrap();
/// let storage = FsStorage::new(root.path(), FsOptions::default()).unwrap();
/// let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
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
    /// Idle grace for upload directories without a `UPLOADS` record:
    /// an orphan is deleted only when its parts have been idle at least
    /// this long (the sweep's `multipart_ttl` default; the startup
    /// orchestration T068 will pass the configured `multipart_expire_days`).
    multipart_grace: Duration,
}

impl FsCleanup {
    /// Construct the cleanup pipeline for `storage`.
    pub fn new(storage: &FsStorage, options: CleanupOptions) -> Self {
        Self {
            root: storage.root().to_path_buf(),
            state_dir: storage.state_dir().to_path_buf(),
            storage: storage.clone(),
            dry_run: options.dry_run,
            multipart_grace: sweep::Options::default().multipart_ttl,
        }
    }

    /// Override the orphan-upload idle grace (default: the sweep's
    /// `multipart_ttl`, 7 days). The startup orchestration (T068) passes
    /// the configured `[s3] multipart_expire_days` through this.
    pub fn with_multipart_grace(mut self, grace: Duration) -> Self {
        self.multipart_grace = grace;
        self
    }

    /// Stage 1: full `tmp/` clear (no active writers at startup).
    async fn repair_tmp(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        let entries = match tmp_entries(&self.state_dir).await {
            Ok(entries) => entries,
            Err(err) => {
                actions.push(Err(err));
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
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => Ok(()),
                        // Already gone (a concurrent sweep): nothing to report.
                        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
                        Err(err) => Err(err.into()),
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
        let buckets = match self.storage.bucket_names().await {
            Ok(buckets) => buckets,
            Err(err) => {
                actions.push(Err(err));
                return;
            }
        };
        for bucket in buckets {
            let Ok(bucket_dir) = self.storage.bucket_dir(&bucket) else {
                continue;
            };
            // Collect every `.tinio` entry of the bucket tree (iterative
            // walk; symlinked dirs are descended only when following is
            // enabled — the follow policy, one source of truth).
            let mut residue = Vec::new();
            let mut stack = vec![bucket_dir];
            while let Some(dir) = stack.pop() {
                let mut entries = match tokio::fs::read_dir(&dir).await {
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
                    let lmeta = match tokio::fs::symlink_metadata(entry.path()).await {
                        Ok(metadata) => metadata,
                        Err(_) => continue,
                    };
                    let is_dir = if crate::fsutil::is_symlink_or_reparse(&lmeta) {
                        *self.storage.follow_symlinks()
                            && tokio::fs::metadata(entry.path())
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
                        if let Err(err) = ok_if_missing(tokio::fs::remove_dir_all(&dir).await) {
                            // A `.tinio` *file* (out-of-band): remove as a
                            // file.
                            if err.kind() == io::ErrorKind::NotADirectory {
                                ok_if_missing(tokio::fs::remove_file(&dir).await)?;
                            } else {
                                return Err(err.into());
                            }
                        }
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
        let mut buckets = match tokio::fs::read_dir(&multipart).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return,
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
            if !tokio::fs::try_exists(self.root.join(&name))
                .await
                .unwrap_or(false)
            {
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
                        ok_if_missing(tokio::fs::remove_dir_all(entry.path()).await)?;
                        Ok(())
                    },
                )
                .await;
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
        let mut buckets = match tokio::fs::read_dir(&multipart).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return,
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
            let mut uploads = match tokio::fs::read_dir(bucket_entry.path()).await {
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
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
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
                    ok_if_missing(tokio::fs::remove_dir_all(&dir).await)?;
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
            if !tokio::fs::try_exists(self.root.join(&name))
                .await
                .unwrap_or(false)
            {
                let name = match bucket::name(name) {
                    Ok(name) => name,
                    Err(err) => {
                        actions.push(Err(Error::Storage(err)));
                        continue;
                    }
                };
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

    /// Stage 6: meta-orphan reclamation — meta entries whose object file no
    /// longer exists are deleted (fs-backend.md §8.3).
    async fn repair_meta_orphans(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        // Every live bucket (the store's own source of truth), then the
        // `OBJECT_META` entries of each — one bucket range scan per walk.
        let buckets = match self.storage.bucket_names().await {
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
                let Ok(bucket_dir) = self.storage.bucket_dir(&bucket) else {
                    continue;
                };
                let Ok(path) = self.storage.key_path(&bucket_dir, &record.key, true) else {
                    continue;
                };
                if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                    continue;
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
        self.repair_tmp(&mut actions).await;
        self.repair_bucket_staging(&mut actions).await;
        self.repair_multipart_orphans(&mut actions).await;
        self.repair_orphan_upload_dirs(&mut actions).await;
        self.repair_buckets(&mut actions).await;
        if kind == RepairKind::Full {
            self.repair_meta_orphans(&mut actions).await;
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
    use super::*;
    use crate::{FsOptions, testutil::rt};
    use futures::StreamExt;
    use std::fs;
    use std::time::SystemTime;
    use tinio_core::storage::{BucketOps, ObjectOps};
    use tinio_core::{bucket, object};
    use tinio_util::testing::body;

    async fn collect(actions: ActionStream<Error>) -> Vec<RepairAction> {
        let mut out = Vec::new();
        let mut actions = actions;
        while let Some(action) = actions.next().await {
            out.push(action.unwrap());
        }
        out
    }

    #[test]
    fn startup_repair_clears_tmp_and_orphans() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    compact_threshold_percent: 20,
                    state_dir: Some(state.path().to_path_buf()),
                },
            )
            .unwrap();

            // Leftover temp file.
            fs::create_dir(state.path().join("tmp")).unwrap();
            fs::write(state.path().join("tmp/upload-leftover"), b"x").unwrap();

            // Bucket-orphaned multipart subtree.
            fs::create_dir_all(state.path().join("multipart/gone-bucket/u1")).unwrap();
            fs::write(state.path().join("multipart/gone-bucket/u1/part-1"), b"x").unwrap();

            // Stale bucket record.
            storage
                .bucket_store()
                .record(&bucket::name("gone-bucket").unwrap(), SystemTime::now())
                .await
                .unwrap();

            // A live bucket + upload must be untouched (a real upload: the
            // record commits before its directory exists).
            let live = bucket::name("live-bucket").unwrap();
            storage.create_bucket(&live).await.unwrap();
            storage
                .multipart_store()
                .create(&live, &object::key("k").unwrap())
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
        });
    }

    #[test]
    fn startup_repair_clears_bucket_staging_residue() {
        // A crashed/failed cross-volume commit leaves `.tinio/` staging
        // dirs at any depth of a bucket — cleared at startup; user data
        // (the accidental parent dirs) is untouched.
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            fs::create_dir_all(root.path().join("data/.tinio")).unwrap();
            fs::write(root.path().join("data/.tinio/aaaa"), b"residue").unwrap();
            fs::create_dir_all(root.path().join("data/sub/.tinio")).unwrap();
            fs::write(root.path().join("data/sub/.tinio/bbbb"), b"residue").unwrap();
            fs::create_dir_all(root.path().join("data/emptydir/.tinio")).unwrap();

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
        });
    }

    #[test]
    fn startup_repair_drains_uploads_of_a_missing_bucket() {
        // Hand-deleting a bucket directory (mirror semantics) must also
        // drain UPLOADS/PARTS — otherwise ListMultipartUploads shows
        // ghosts that cannot complete, and has_uploads blocks delete.
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            let gone = bucket::name("gone-bucket").unwrap();
            storage.create_bucket(&gone).await.unwrap();
            let k = object::key("k").unwrap();
            storage.multipart_store().create(&gone, &k).await.unwrap();
            assert!(storage.multipart_store().has_uploads(&gone).await.unwrap());
            tokio::fs::remove_dir_all(root.path().join("gone-bucket"))
                .await
                .unwrap();

            let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
            let _ = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
            assert!(
                !storage.multipart_store().has_uploads(&gone).await.unwrap(),
                "UPLOADS of a missing bucket must be drained"
            );
        });
    }

    #[test]
    fn orphan_upload_dirs_are_removed_after_grace() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    compact_threshold_percent: 20,
                    state_dir: Some(state.path().to_path_buf()),
                },
            )
            .unwrap();
            let b = bucket::name("live").unwrap();
            storage.create_bucket(&b).await.unwrap();

            // A live upload: record committed, directory present.
            let k = object::key("k").unwrap();
            let upload = storage.multipart_store().create(&b, &k).await.unwrap();
            let live_dir = state.path().join("multipart/live").join(&upload.upload_id);
            fs::create_dir_all(&live_dir).unwrap();
            fs::write(live_dir.join("part-1"), b"x").unwrap();

            // An orphan: directory with an old part, no UPLOADS record.
            let orphan_dir = state.path().join("multipart/live/u-orphan");
            fs::create_dir_all(&orphan_dir).unwrap();
            let orphan_part = orphan_dir.join("part-1");
            fs::write(&orphan_part, b"x").unwrap();
            let handle = fs::OpenOptions::new()
                .write(true)
                .open(&orphan_part)
                .unwrap();
            handle
                .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000))
                .unwrap();
            drop(handle);

            // A young orphan (fresh parts) must survive the grace.
            let young_dir = state.path().join("multipart/live/u-young");
            fs::create_dir_all(&young_dir).unwrap();
            fs::write(young_dir.join("part-1"), b"x").unwrap();

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
        });
    }

    #[test]
    fn live_upload_with_domain_invalid_stored_key_is_not_an_orphan() {
        // §5.7 regression: the orphan judgment must query raw UPLOADS
        // membership — a live upload whose stored key fails domain
        // validation is still live. The validated `walk_uploads` view
        // skips such a row, and judging by it would delete a live
        // upload's directory and drain its rows.
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    compact_threshold_percent: 20,
                    state_dir: Some(state.path().to_path_buf()),
                },
            )
            .unwrap();
            let b = bucket::name("live").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("k").unwrap();
            let upload = storage.multipart_store().create(&b, &k).await.unwrap();
            storage
                .multipart_store()
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
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
        });
    }

    #[test]
    fn orphan_stage_toctou_order_protects_live_uploads() {
        // §5.7 pins the TOCTOU order: enumerate the `multipart/` tree
        // first, then read UPLOADS in one read transaction. `create`
        // commits the UPLOADS row before any directory exists (the first
        // put_part creates it), so an enumerated directory always has its
        // row visible to the later transaction — a `create` racing between
        // the two cannot be misjudged. Both halves are pinned with a ZERO
        // grace: the idle check protects nothing, so only the raw UPLOADS
        // membership shields a live upload.
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    compact_threshold_percent: 20,
                    state_dir: Some(state.path().to_path_buf()),
                },
            )
            .unwrap();
            let b = bucket::name("live").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("k").unwrap();
            let cleanup = || {
                FsCleanup::new(&storage, CleanupOptions::default())
                    .with_multipart_grace(Duration::ZERO)
            };

            // Half 1: record committed, no directory yet (the state of a
            // `create` racing the enumeration) — nothing to judge, the
            // record survives.
            let upload = storage.multipart_store().create(&b, &k).await.unwrap();
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
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
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
            fs::create_dir_all(&orphan_dir).unwrap();
            fs::write(orphan_dir.join("part-1"), b"x").unwrap();
            let actions = collect(cleanup().repair(RepairKind::Startup).await.unwrap()).await;
            assert!(
                actions
                    .iter()
                    .any(|a| a.description.contains("orphaned upload")),
                "{actions:?}"
            );
            assert!(!orphan_dir.exists());
            assert!(live_dir.exists());
        });
    }

    #[test]
    fn dry_run_reports_orphan_upload_dirs_without_removing() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    compact_threshold_percent: 20,
                    state_dir: Some(state.path().to_path_buf()),
                },
            )
            .unwrap();
            let orphan_dir = state.path().join("multipart/live/u-orphan");
            fs::create_dir_all(&orphan_dir).unwrap();
            let orphan_part = orphan_dir.join("part-1");
            fs::write(&orphan_part, b"x").unwrap();
            let handle = fs::OpenOptions::new()
                .write(true)
                .open(&orphan_part)
                .unwrap();
            handle
                .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000))
                .unwrap();
            drop(handle);

            let cleanup = FsCleanup::new(&storage, CleanupOptions { dry_run: true })
                .with_multipart_grace(Duration::from_secs(60));
            let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
            assert!(
                actions
                    .iter()
                    .any(|a| a.description.starts_with("would remove orphaned upload")),
                "{actions:?}"
            );
            assert!(orphan_dir.exists());
        });
    }

    #[test]
    fn dry_run_touches_nothing() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    compact_threshold_percent: 20,
                    state_dir: Some(state.path().to_path_buf()),
                },
            )
            .unwrap();
            fs::create_dir(state.path().join("tmp")).unwrap();
            fs::write(state.path().join("tmp/upload-leftover"), b"x").unwrap();

            let cleanup = FsCleanup::new(&storage, CleanupOptions { dry_run: true });
            let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
            assert!(
                actions
                    .iter()
                    .any(|a| a.description.starts_with("would clear")),
                "{actions:?}"
            );
            assert!(state.path().join("tmp/upload-leftover").exists());
        });
    }

    #[test]
    fn full_repair_reclaims_meta_orphans() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("a.txt").unwrap();
            storage.put_object(&b, &k, body(b"x")).await.unwrap();

            // Out-of-band deletion: the object vanishes, the meta entry stays.
            fs::remove_file(root.path().join("data/a.txt")).unwrap();

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
            fs::remove_file(root.path().join("data/a.txt")).unwrap();
            let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
            let actions = collect(cleanup.repair(RepairKind::Startup).await.unwrap()).await;
            assert!(
                !actions
                    .iter()
                    .any(|a| a.description.contains("orphaned meta")),
                "{actions:?}"
            );
            assert_eq!(storage.meta_store().walk(&b).await.unwrap().len(), 1);
        });
    }

    #[test]
    fn reclaim_meta_orphans_cleans_a_vanished_bucket() {
        // A bucket directory removed out-of-band mid-run: the scanner's
        // background reclamation must also prune the stale BUCKETS row
        // and drain the orphaned OBJECT_META — not wait for the next
        // startup repair.
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("a.txt").unwrap();
            storage.put_object(&b, &k, body(b"x")).await.unwrap();
            assert_eq!(storage.meta_store().walk(&b).await.unwrap().len(), 1);
            fs::remove_dir_all(root.path().join("data")).unwrap();

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
        });
    }

    #[test]
    fn reclaim_meta_orphans_removes_only_missing() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let gone = object::key("gone.txt").unwrap();
            let alive = object::key("alive.txt").unwrap();
            storage.put_object(&b, &gone, body(b"x")).await.unwrap();
            storage.put_object(&b, &alive, body(b"y")).await.unwrap();
            fs::remove_file(root.path().join("data/gone.txt")).unwrap();

            let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
            let actions = collect(cleanup.reclaim_meta_orphans().await.unwrap()).await;
            assert_eq!(actions.len(), 1, "{actions:?}");
            assert!(storage.meta_store().walk(&b).await.unwrap().len() == 1);
        });
    }
}
