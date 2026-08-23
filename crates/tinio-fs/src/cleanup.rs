//! The `Cleanup` trait implementation for the fs backend (task T070
//! foundation; scanner reclamation per T045).
//!
//! Per fs-backend.md §8 and failure-handling.md §3: startup repair handles
//! the fast, deterministic items (full `tmp/` clear, bucket-orphaned
//! multipart subtrees, stale `buckets.json` entries); the full repair adds
//! meta-orphan reclamation; `reclaim_meta_orphans` is the scanner's
//! background path. All modes share one code path with a `dry_run` flag
//! ([`CleanupOptions`]); **user data (bucket directories and objects) is
//! never touched** — only tinio-private state.
//!
//! Home root-state-dir GC (part of the `Full` scope per failure-handling.md
//! §3) needs read-only-mode state relocation and lands with US2 (T076).

use std::{io, path::PathBuf};

use async_trait::async_trait;
use futures::stream;
use tinio_core::cleanup::{
    ActionStream, Cleanup, CleanupOptions, RepairAction, RepairActionLevel, RepairKind,
};

use crate::{
    BackendError,
    backend::FsStorage,
    error::Error,
    fsutil::{ok_if_missing, tmp_entries},
    path::{MULTIPART_DIR_NAME, key_path},
};

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
}

impl FsCleanup {
    /// Construct the cleanup pipeline for `storage`.
    pub fn new(storage: &FsStorage, options: CleanupOptions) -> Self {
        Self {
            root: storage.root().to_path_buf(),
            state_dir: storage.state_dir().to_path_buf(),
            storage: storage.clone(),
            dry_run: options.dry_run,
        }
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

    /// Stage 2: bucket-orphaned multipart subtrees (uploads whose bucket
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
                        ok_if_missing(tokio::fs::remove_dir_all(entry.path()).await)?;
                        Ok(())
                    },
                )
                .await;
            }
        }
    }

    /// Stage 3: stale `buckets.json` entries (bucket directory gone).
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
                let name = match tinio_core::bucket::name(name) {
                    Ok(name) => name,
                    Err(err) => {
                        actions.push(Err(BackendError::Storage(err)));
                        continue;
                    }
                };
                record_repair(
                    actions,
                    self.dry_run,
                    format!("would prune stale buckets.json entry for {name}"),
                    format!("pruned stale buckets.json entry for {name}"),
                    async { self.storage.bucket_store().remove(&name).await },
                )
                .await;
            }
        }
    }

    /// Stage 4: meta-orphan reclamation — meta entries whose object file no
    /// longer exists are deleted (fs-backend.md §8.3).
    async fn repair_meta_orphans(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
        // The walk starts at the meta root (the store's own layout — one
        // source of truth).
        let mut buckets = match tokio::fs::read_dir(self.storage.meta_store().root()).await {
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
            let Ok(bucket) = tinio_core::bucket::name(name.clone()) else {
                continue;
            };
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
                // object files.
                let Ok(path) = key_path(&self.root.join(&name), &record.key) else {
                    continue;
                };
                if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                    continue;
                }
                record_repair(
                    actions,
                    self.dry_run,
                    format!(
                        "would reclaim orphaned meta entry for {} in {name}",
                        record.key
                    ),
                    format!("reclaimed orphaned meta entry for {} in {name}", record.key),
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
        self.repair_multipart_orphans(&mut actions).await;
        self.repair_buckets(&mut actions).await;
        if kind == RepairKind::Full {
            self.repair_meta_orphans(&mut actions).await;
        }
        Ok(Box::pin(stream::iter(actions)))
    }

    async fn reclaim_meta_orphans(&self) -> Result<ActionStream<Error>, Error> {
        let mut actions = Vec::new();
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
    use tinio_core::testing::body;
    use tinio_core::{bucket, object};

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

            // Stale buckets.json entry.
            storage
                .bucket_store()
                .record(&bucket::name("gone-bucket").unwrap(), SystemTime::now())
                .await
                .unwrap();

            // A live bucket + upload must be untouched.
            let live = bucket::name("live-bucket").unwrap();
            storage.create_bucket(&live).await.unwrap();
            fs::create_dir_all(state.path().join("multipart/live-bucket/u1")).unwrap();
            fs::write(
                state.path().join("multipart/live-bucket/u1/upload.json"),
                r#"{"upload_id":"u1","bucket":"live-bucket","key":"k","initiated_at":1}"#,
            )
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
            assert!(state.path().join("multipart/live-bucket/u1").exists());
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
