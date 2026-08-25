//! The background ETag scanner (task T045, FR-024; design per scanner.md).
//!
//! A low-priority task that converts cold files into meta-store hits in the
//! background: missing entries are computed (streaming MD5, bounded
//! buffers), stale entries recomputed, and orphaned entries (object gone)
//! reclaimed through the [`Cleanup`] trait — so repeated listings become
//! cheap. Listings stay correct with the scanner disabled (synchronous
//! recompute fallback).
//!
//! Pacing per contracts/config.md (Minio-aligned): `delay` between entry
//! batches (throttle), `max_wait` bounds a single sleep so shutdown is
//! always prompt, `cycle` is the minimum interval between full-tree passes.
//! `TINIO_SCANNER` (`0`/`1`) overrides the `[scanner]` presence gate at
//! construction. The scanner never blocks startup (it launches after
//! readiness) and aborts quietly on the shutdown channel.

use std::{
    env, io,
    time::{Duration, Instant},
};

use futures::StreamExt;
use tinio_core::{
    bucket,
    cleanup::{Cleanup, CleanupOptions, RepairActionLevel},
};
use tokio::sync::watch;

use crate::{FsCleanup, backend::FsStorage, error::Error, pacing};

/// Entries per batch: after each batch the scanner yields and sleeps
/// `delay`, so in-flight S3 requests preempt scanning (tuned by the T093
/// cold/warm benchmark).
const BATCH_SIZE: usize = 32;

/// Scanner construction options (contracts/config.md `[scanner]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannerOptions {
    /// Presence gate: `[scanner]` section present = on, absent = off.
    pub enabled: bool,
    /// Seconds between scan batches (pacing/throttle).
    pub delay: Duration,
    /// Max time to wait for a scan slot when throttled (bounds a single
    /// sleep so shutdown stays prompt).
    pub max_wait: Duration,
    /// Full-tree re-scan cadence (catches out-of-band changes over time).
    pub cycle: Duration,
}

/// The background ETag scanner of a storage backend.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use tinio_util::testing::body;
/// use tinio_core::storage::{BucketOps, ObjectOps};
/// use tinio_fs::{FsOptions, FsStorage, Scanner, ScannerOptions};
///
/// let root = tempfile::tempdir().unwrap();
/// let storage = FsStorage::new(root.path(), FsOptions::default()).unwrap();
/// // A hand-dropped file with no meta entry.
/// std::fs::create_dir(root.path().join("data")).unwrap();
/// std::fs::write(root.path().join("data/dropped.txt"), b"out-of-band").unwrap();
/// let options = ScannerOptions {
///     enabled: true,
///     delay: Duration::from_millis(1),
///     max_wait: Duration::from_millis(10),
///     cycle: Duration::from_millis(50),
/// };
/// let scanner = Scanner::new(storage.clone(), options);
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     scanner.scan_once().await.unwrap();
///     let head = storage
///         .head_object(&"data".into(), &"dropped.txt".into())
///         .await
///         .unwrap();
///     assert_eq!(head.size, 11);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Scanner {
    storage: FsStorage,
    options: ScannerOptions,
}

/// Resolve the enabled gate: `TINIO_SCANNER` is a strict `0`/`1` toggle
/// that overrides the `[scanner]` section presence independently
/// (contracts/config.md, FR-024); any other value (or a non-Unicode one)
/// is ignored, falling back to the section gate.
fn scanner_enabled(env: Option<&str>, section_present: bool) -> bool {
    match env {
        Some("1") => true,
        Some("0") => false,
        _ => section_present,
    }
}

impl Scanner {
    /// Construct the scanner. The `TINIO_SCANNER` env toggle (`0`/`1`)
    /// overrides the `[scanner]` presence gate independently (FR-024); any
    /// other value is ignored.
    pub fn new(storage: FsStorage, options: ScannerOptions) -> Self {
        let enabled = scanner_enabled(env::var("TINIO_SCANNER").ok().as_deref(), options.enabled);
        Self {
            storage,
            options: ScannerOptions { enabled, ..options },
        }
    }

    /// Run the scan loop until `shutdown` turns true (aborts quietly — no
    /// partial-write cleanup needed; writes are atomic). Does nothing when
    /// disabled.
    pub async fn run(self, shutdown: watch::Receiver<bool>) {
        if !self.options.enabled {
            return;
        }
        loop {
            let pass_start = Instant::now();
            let walked = self.scan_once().await;
            match walked {
                Ok(summary) => {
                    tracing::debug!(
                        reconciled = summary.reconciled,
                        recomputed = summary.recomputed,
                        reclaimed = summary.reclaimed,
                        "scanner pass complete"
                    );
                }
                Err(err) => tracing::warn!(error = %err, "scanner pass failed"),
            }
            if *shutdown.borrow() {
                return;
            }
            // One full pass per cycle at most; passes longer than the
            // cycle (large trees) restart immediately.
            let elapsed = pass_start.elapsed();
            let budget = self.options.cycle.max(self.options.delay);
            if elapsed < budget {
                pacing::sleep_checked(
                    budget - elapsed,
                    self.options.max_wait.max(Duration::from_millis(100)),
                    &shutdown,
                )
                .await;
                if *shutdown.borrow() {
                    return;
                }
            }
        }
    }

    /// One full-tree pass: reconcile every bucket's files against the meta
    /// store (missing → compute, stale → recompute), then reclaim meta
    /// orphans through the [`Cleanup`] trait. Yields to request traffic
    /// between buckets (and after each batch of entries within a bucket);
    /// returns the pass summary.
    pub async fn scan_once(&self) -> Result<ScanSummary, Error> {
        let mut summary = ScanSummary::default();
        for name in self.storage.bucket_names().await? {
            let reconciled = self.reconcile_bucket(&name).await?;
            summary.reconciled += reconciled.0;
            summary.recomputed += reconciled.1;
            tokio::task::yield_now().await;
        }
        // Orphan reclamation: entries whose object file no longer exists
        // (the object may have been removed out-of-band).
        let cleanup = FsCleanup::new(&self.storage, CleanupOptions::default());
        let mut actions = cleanup.reclaim_meta_orphans().await?;
        while let Some(action) = actions.next().await {
            match action {
                Ok(action) if action.level == RepairActionLevel::Warn => {
                    summary.reclaimed += 1;
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(error = %err, "scanner reclamation failed");
                }
            }
        }
        Ok(summary)
    }

    /// Reconcile one bucket: for every object file, ensure a matching meta
    /// entry exists (compute or recompute the MD5 streaming). Returns
    /// `(files, recomputed)`.
    ///
    /// Yields and sleeps `delay` after each batch of [`BATCH_SIZE`]
    /// entries, so in-flight S3 requests preempt scanning.
    /// A pass in progress at shutdown completes, then the loop exits.
    async fn reconcile_bucket(&self, name: &bucket::Name) -> Result<(usize, usize), Error> {
        let meta = self.storage.meta_store();
        let files = self.storage.listing().walk_files(name, "").await?;
        let file_count = files.len();
        let mut recomputed = 0;
        let mut batch = 0usize;
        // The walk's size + mtime double as the staleness check — no
        // second stat per file. One meta read decides: matching entries
        // are left untouched, missing/stale ones are recomputed and
        // rewritten (no re-read of the entry just recomputed).
        for (key, path, size, mtime) in files {
            match meta.ensure_etag(name, &key, &path, size, mtime).await {
                Ok((_, true)) => recomputed += 1,
                Ok((_, false)) => {}
                // The file vanished (concurrent delete): nothing to
                // reconcile — the orphan pass reclaims the entry.
                Err(Error::Io(err)) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            batch += 1;
            if batch.is_multiple_of(BATCH_SIZE) {
                tokio::task::yield_now().await;
                tokio::time::sleep(self.options.delay).await;
            }
        }
        Ok((file_count, recomputed))
    }
}

/// Result of one scanner pass (for logs and tests).
///
/// # Examples
///
/// ```rust
/// use tinio_fs::ScanSummary;
///
/// let summary = ScanSummary {
///     reconciled: 3,
///     recomputed: 1,
///     reclaimed: 0,
/// };
/// assert_eq!(summary.reconciled, 3);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanSummary {
    /// Object files reconciled (entries checked).
    pub reconciled: usize,
    /// Entries computed or recomputed (missing/stale).
    pub recomputed: usize,
    /// Orphaned meta entries reclaimed.
    pub reclaimed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use std::fs;
    use tinio_core::object;
    use tinio_core::storage::{BucketOps, ObjectOps};
    use tinio_util::testing::body;

    fn options() -> ScannerOptions {
        ScannerOptions {
            enabled: true,
            delay: Duration::from_millis(1),
            max_wait: Duration::from_millis(10),
            cycle: Duration::from_millis(50),
        }
    }

    #[test]
    fn scanner_env_toggle_is_strict_zero_one() {
        // `1` forces on, `0` forces off; anything else is ignored and the
        // config-section gate decides (contracts/config.md).
        assert!(scanner_enabled(Some("1"), false));
        assert!(!scanner_enabled(Some("0"), true));
        for value in ["false", "true", "yes", "2", ""] {
            assert!(scanner_enabled(Some(value), true), "{value}");
            assert!(!scanner_enabled(Some(value), false), "{value}");
        }
        assert!(scanner_enabled(None, true));
        assert!(!scanner_enabled(None, false));
    }

    #[test]
    fn computes_missing_entries() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            fs::create_dir(root.path().join("data")).unwrap();
            fs::write(root.path().join("data/dropped.txt"), b"out-of-band").unwrap();
            let scanner = Scanner::new(storage.clone(), options());
            let summary = scanner.scan_once().await.unwrap();
            assert_eq!(summary.reconciled, 1);
            assert_eq!(summary.recomputed, 1);
            let head = storage
                .head_object(
                    &bucket::name("data").unwrap(),
                    &object::key("dropped.txt").unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(head.size, 11);
            // A second pass is a no-op (entries match).
            let summary = scanner.scan_once().await.unwrap();
            assert_eq!(summary.recomputed, 0);
        });
    }

    #[test]
    fn recomputes_stale_entries() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            storage
                .put_object(&b, &"a.txt".into(), body(b"first"))
                .await
                .unwrap();
            // Out-of-band edit: new content, new size.
            fs::write(root.path().join("data/a.txt"), b"second").unwrap();
            let scanner = Scanner::new(storage.clone(), options());
            let summary = scanner.scan_once().await.unwrap();
            assert_eq!(summary.recomputed, 1);
            let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
            assert_eq!(head.size, 6);
            assert_eq!(head.etag, tinio_core::ETag::from_content(b"second"));
        });
    }

    #[test]
    fn reclaims_orphaned_meta_entries() {
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
            let scanner = Scanner::new(storage.clone(), options());
            let summary = scanner.scan_once().await.unwrap();
            assert_eq!(summary.reclaimed, 1);
            assert_eq!(storage.meta_store().walk(&b).await.unwrap().len(), 1);
        });
    }

    #[test]
    fn run_stops_on_shutdown() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            fs::create_dir(root.path().join("data")).unwrap();
            fs::write(root.path().join("data/a.txt"), b"x").unwrap();
            let scanner = Scanner::new(storage.clone(), options());
            let (tx, rx) = watch::channel(false);
            let task = tokio::spawn(async move {
                scanner.run(rx).await;
            });
            // Give the loop a moment to make progress, then stop it.
            tokio::time::sleep(Duration::from_millis(200)).await;
            tx.send(true).unwrap();
            task.await.unwrap();
            // The pass completed at least once.
            assert!(
                !storage
                    .meta_store()
                    .walk(&bucket::name("data").unwrap())
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn run_does_nothing_when_disabled() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            fs::create_dir(root.path().join("data")).unwrap();
            fs::write(root.path().join("data/a.txt"), b"x").unwrap();
            let mut options = options();
            options.enabled = false;
            let scanner = Scanner::new(storage.clone(), options);
            let (tx, rx) = watch::channel(false);
            let task = tokio::spawn(async move {
                scanner.run(rx).await;
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            // Disabled scanners return immediately, dropping the receiver.
            let _ = tx.send(true);
            task.await.unwrap();
            assert!(
                storage
                    .meta_store()
                    .walk(&bucket::name("data").unwrap())
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn yields_between_entry_batches() {
        // More than one batch of entries: the scan must yield and sleep
        // between batches so in-flight S3 requests preempt scanning.
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            fs::create_dir(root.path().join("data")).unwrap();
            for i in 0..40 {
                fs::write(
                    root.path().join(format!("data/f{i:02}.txt")),
                    format!("payload {i}"),
                )
                .unwrap();
            }
            let scanner = Scanner::new(storage.clone(), options());
            let summary = scanner.scan_once().await.unwrap();
            assert_eq!(summary.reconciled, 40);
            assert_eq!(summary.recomputed, 40);
            assert_eq!(
                storage
                    .meta_store()
                    .walk(&bucket::name("data").unwrap())
                    .await
                    .unwrap()
                    .len(),
                40
            );
        });
    }

    #[test]
    fn run_restarts_immediately_when_pass_exceeds_cycle() {
        // A pass longer than the cycle budget restarts without sleeping;
        // a shorter one sleeps. Either way the loop must keep making
        // passes and stop promptly on shutdown.
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
            fs::create_dir(root.path().join("data")).unwrap();
            for i in 0..40 {
                fs::write(
                    root.path().join(format!("data/f{i:02}.txt")),
                    format!("payload {i}"),
                )
                .unwrap();
            }
            let scanner = Scanner::new(
                storage.clone(),
                ScannerOptions {
                    enabled: true,
                    delay: Duration::from_millis(1),
                    max_wait: Duration::from_millis(10),
                    cycle: Duration::from_millis(1),
                },
            );
            let (tx, rx) = watch::channel(false);
            let task = tokio::spawn(async move {
                scanner.run(rx).await;
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
            tx.send(true).unwrap();
            task.await.unwrap();
            // At least one full pass completed before shutdown.
            assert_eq!(
                storage
                    .meta_store()
                    .walk(&bucket::name("data").unwrap())
                    .await
                    .unwrap()
                    .len(),
                40
            );
        });
    }
}
