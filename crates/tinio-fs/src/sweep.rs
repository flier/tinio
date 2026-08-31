//! The async sweep (task T046, FR-014).
//!
//! Time-driven, mtime-based cleanup that runs while the server is live
//! (fs-backend.md §7): temp files older than `temp_ttl` (default 24 h) and
//! multipart uploads idle longer than `multipart_ttl` (default 7 days,
//! idle = max(initiated_at, latest part mtime)). Non-blocking: the sweep
//! sleeps in bounded chunks and re-checks shutdown, so it never delays
//! request handling or shutdown. Complements (does not replace) the
//! event-driven `FsCleanup` startup repair.

use std::{
    io::ErrorKind,
    time::{Duration, SystemTime},
};

use smart_default::SmartDefault;
use tokio::{fs, sync::watch};

use crate::{backend::FsStorage, error::Error, fsutil::entries_of, pacing, path::TMP_DIR_NAME};

/// Sweep construction options (contracts/config.md `[s3]` TTLs).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_fs::sweep::Options;
///
/// let options = Options::default();
/// assert_eq!(options.temp_ttl, Duration::from_secs(86400));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault)]
pub struct Options {
    /// Stale temp-file timeout (`[s3] temp_ttl_hours`).
    #[default(_code = "Duration::from_secs(24 * 3600)")]
    pub temp_ttl: Duration,
    /// Abandoned-upload timeout (`[s3] multipart_expire_days`).
    #[default(_code = "Duration::from_secs(7 * 24 * 3600)")]
    pub multipart_ttl: Duration,
}

/// Sweep cadence: one pass per hour. Not a config knob — the contract's
/// `[s3]` section defines only the two TTLs (contracts/config.md).
const SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

/// The async sweeper of a storage backend.
///
/// # Examples
///
/// ```rust
/// use std::{sync::Arc, time::Duration};
///
/// use tinio_core::{
///     pipeline::InlineRunner,
///     storage::{
///         DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
///     },
/// };
/// use tinio_fs::{
///     FsOptions, FsStorage,
///     sweep::{Options, Sweeper},
/// };
/// use tokio::{runtime::Runtime, sync::watch};
///
/// let root = tempfile::tempdir().unwrap();
/// let options = FsOptions {
///     follow_symlinks: false,
///     state_dir: None,
///     compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///     meta_batch_size: DEFAULT_META_BATCH_SIZE,
///     meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
///     io_pipeline: Arc::new(InlineRunner::default()),
///     remove_pipeline: Arc::new(InlineRunner::default()),
///     db_pipeline: Arc::new(InlineRunner::default()),
/// };
/// let storage = FsStorage::new(root.path(), options).unwrap();
/// let sweeper = Sweeper::new(storage.clone(), Options::default());
/// let (tx, rx) = watch::channel(false);
/// Runtime::new().unwrap().block_on(async {
///     let task = tokio::spawn(async move { sweeper.run(rx).await });
///     tx.send(true).unwrap();
///     task.await.unwrap();
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Sweeper {
    storage: FsStorage,
    options: Options,
}

impl Sweeper {
    /// Construct the sweeper.
    pub fn new(storage: FsStorage, options: Options) -> Self {
        Self { storage, options }
    }

    /// Run the sweep loop until `shutdown` turns true. One sweep pass runs
    /// every hour (fixed cadence); the sleep is bounded by 1 s chunks so
    /// shutdown stays prompt.
    pub async fn run(self, shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            match self.sweep_once(SystemTime::now()).await {
                Ok(summary) => {
                    tracing::debug!(
                        temps = summary.temp_files,
                        uploads = summary.uploads,
                        "sweep pass complete"
                    );
                }
                Err(err) => tracing::warn!(error = %err, "sweep pass failed"),
            }
            // Sleep the interval in bounded chunks, re-checking shutdown.
            pacing::sleep_checked(SWEEP_INTERVAL, Duration::from_secs(1), &shutdown).await;
        }
    }

    /// One sweep pass against a synthetic clock (`now`): removes temp
    /// files older than the TTL and multipart uploads idle longer than the
    /// TTL. Returns the counts.
    ///
    /// Every pass ends with one low-frequency fragmentation evaluation
    /// (the stats call takes the write lock): over the `[storage.fs]
    /// compact_threshold_percent` threshold the `compact_needed` marker is
    /// set, and the next startup compacts (meta-redb-spec §5.9). Best
    /// effort — an evaluation failure is logged, never a sweep failure.
    pub async fn sweep_once(&self, now: SystemTime) -> Result<Summary, Error> {
        let summary = Summary {
            temp_files: self.sweep_tmp(now).await?,
            uploads: self.sweep_multipart(now).await?,
        };
        self.evaluate_compact_needed().await;
        Ok(summary)
    }

    /// Evaluate fragmentation and set the compact-needed marker when the
    /// threshold is reached (best effort; one write transaction).
    async fn evaluate_compact_needed(&self) {
        if let Err(err) = self
            .storage
            .evaluate_compact(self.storage.compact_threshold_percent())
            .await
        {
            tracing::warn!(error = %err, "compact evaluation failed");
        }
    }

    async fn sweep_tmp(&self, now: SystemTime) -> Result<usize, Error> {
        let entries = entries_of(&self.storage.state_dir().join(TMP_DIR_NAME)).await?;
        let mut removed = 0;
        for (path, name) in entries {
            let metadata = match fs::metadata(&path).await {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            if metadata.is_dir() {
                continue; // only files stage under tmp/
            }
            if let Ok(modified) = metadata.modified()
                && now
                    .duration_since(modified)
                    .map(|age| age >= self.options.temp_ttl)
                    .unwrap_or(false)
            {
                match fs::remove_file(&path).await {
                    Ok(()) => {
                        tracing::info!("swept stale temp file {name}");
                        removed += 1;
                    }
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }
            }
        }
        Ok(removed)
    }

    async fn sweep_multipart(&self, now: SystemTime) -> Result<usize, Error> {
        let uploads = self.storage.multipart_store().walk_uploads().await?;
        let mut removed = 0;
        for upload in uploads {
            let idle_since = self
                .storage
                .multipart_store()
                .idle_since(&upload.bucket, &upload.upload_id)
                .await?;
            let idle = idle_since.max(upload.initiated_at);
            if now
                .duration_since(idle)
                .map(|age| age >= self.options.multipart_ttl)
                .unwrap_or(false)
            {
                tracing::info!(
                    upload_id = %upload.upload_id,
                    "swept abandoned multipart upload"
                );
                match self
                    .storage
                    .multipart_store()
                    .abort(&upload.bucket, &upload.key, &upload.upload_id)
                    .await
                {
                    Ok(()) => removed += 1,
                    // A concurrent complete/abort won the race — nothing to do.
                    Err(err) => tracing::warn!(error = %err, "sweep could not abort upload"),
                }
            }
        }
        Ok(removed)
    }
}

/// Result of one sweep pass (for logs and tests).
///
/// # Examples
///
/// ```rust
/// use tinio_fs::sweep::Summary;
///
/// let summary = Summary {
///     temp_files: 1,
///     uploads: 0,
/// };
/// assert_eq!(summary.temp_files, 1);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    /// Stale temp files removed.
    pub temp_files: usize,
    /// Abandoned multipart uploads removed.
    pub uploads: usize,
}

#[cfg(test)]
mod tests {
    use std::fs::{self, FileTimes, OpenOptions};

    use super::*;
    use crate::{
        _core::{
            bucket,
            storage::{BucketOps, MultipartOps},
        },
        _util::testing::body,
        FsOptions,
        testutil::fs_options,
    };

    fn old_ttl_options() -> Options {
        Options {
            temp_ttl: Duration::from_secs(60),
            multipart_ttl: Duration::from_secs(60),
        }
    }

    #[tokio::test]
    async fn sweeps_stale_temps_only() {
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
        // Stale temp: backdate its mtime deterministically (std
        // `set_times` — no extra dependency).
        fs::create_dir(state.path().join("tmp")).unwrap();
        fs::write(state.path().join("tmp/stale"), b"x").unwrap();
        fs::write(state.path().join("tmp/fresh"), b"x").unwrap();
        // Windows requires write access to set times.
        let f = OpenOptions::new()
            .write(true)
            .open(state.path().join("tmp/stale"))
            .unwrap();
        f.set_times(FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(3600)))
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let now = SystemTime::now();
        let summary = sweeper.sweep_once(now).await.unwrap();
        assert_eq!(summary.temp_files, 1);
        assert!(!state.path().join("tmp/stale").exists());
        assert!(state.path().join("tmp/fresh").exists());
    }

    #[tokio::test]
    async fn sweeps_idle_multipart_uploads() {
        let (root, storage) = {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), fs_options()).unwrap();
            (root, storage)
        };
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        storage
            .create_multipart_upload(&b, &"big.bin".into())
            .await
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        // No parts exist, so idle = initiated_at (a moment ago): at the
        // real `now` the upload is newer than the TTL.
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.uploads, 0);
        // Well past the TTL, the upload is swept.
        let far = SystemTime::now() + Duration::from_secs(3600);
        let summary = sweeper.sweep_once(far).await.unwrap();
        assert_eq!(summary.uploads, 1);
        assert!(
            storage
                .multipart_store()
                .list_uploads(&b)
                .await
                .unwrap()
                .is_empty()
        );
        let _ = root;
    }

    #[tokio::test]
    async fn fresh_uploads_and_temps_survive() {
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
        fs::create_dir(state.path().join("tmp")).unwrap();
        fs::write(state.path().join("tmp/fresh"), b"x").unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let upload = storage
            .create_multipart_upload(&b, &"big.bin".into())
            .await
            .unwrap();
        storage
            .upload_part(
                &b,
                &"big.bin".into(),
                &upload.upload_id,
                1.into(),
                body(b"x"),
            )
            .await
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.temp_files, 0);
        assert_eq!(summary.uploads, 0);
    }

    #[tokio::test]
    async fn run_loop_stops_on_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            sweeper.run(rx).await;
        });
        tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn run_loop_executes_a_pass_then_stops_on_shutdown() {
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
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            // One full pass runs (the Ok branch), then the 1 h interval
            // sleep notices the shutdown within one 1 s chunk.
            sweeper.run(rx).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn run_loop_warns_and_keeps_running_on_a_failed_pass() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        // `tmp` as a file: the pass fails at entries_of (NotADirectory) —
        // the loop must warn and continue, then stop on shutdown.
        fs::write(state.path().join("tmp"), b"not a directory").unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            sweeper.run(rx).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn directories_under_tmp_are_never_swept() {
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
        fs::create_dir_all(state.path().join("tmp/nested")).unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.temp_files, 0);
        assert!(state.path().join("tmp/nested").is_dir());
    }

    #[tokio::test]
    async fn future_dated_temp_survives() {
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
        fs::create_dir(state.path().join("tmp")).unwrap();
        let stale = state.path().join("tmp/future");
        fs::write(&stale, b"x").unwrap();
        let f = OpenOptions::new().write(true).open(&stale).unwrap();
        f.set_times(FileTimes::new().set_modified(SystemTime::now() + Duration::from_secs(3600)))
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.temp_files, 0);
        assert!(stale.exists());
    }
}
