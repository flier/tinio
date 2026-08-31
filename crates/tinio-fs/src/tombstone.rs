//! Delete-bucket tombstones: unpublished bucket trees under
//! `<root>/.tinio/deleting/` (same volume as the name so the unpublish
//! `rename` cannot hit `EXDEV` when the state dir is relocated, FR-023).
//!
//! The live name is gone at the rename; tree removal is fire-and-forget
//! removal-pipeline work (Q4, D-A — physically isolated from ETag
//! compute). A leftover is reclaimed by doctor / the scanner's repair
//! pass, or enqueued on the removal lane at startup (D-B).

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    _core::pipeline::{self, Completion},
    error::Error,
    fsutil::{ensure_dir, entries_of, remove_tree},
    path::STATE_DIR_NAME,
};

/// The delete-bucket tombstone directory name inside `<root>/.tinio/`.
const DIR_NAME: &str = "deleting";

/// `<root>/.tinio/deleting`.
pub(crate) fn dir(root: &Path) -> PathBuf {
    root.join(STATE_DIR_NAME).join(DIR_NAME)
}

/// Create `<root>/.tinio/deleting/` and return a unique tombstone path.
///
/// # Errors
///
/// [`Error::Io`] when the parent directory cannot be created.
pub(crate) async fn prepare(root: &Path) -> Result<PathBuf, Error> {
    let dir = dir(root);
    // The deleting dir exists in steady state — one probe instead of
    // the per-component walk (the create still runs on the first
    // delete).
    ensure_dir(&dir).await?;
    Ok(dir.join(Uuid::new_v4().to_string()))
}

/// Unpublished leftover trees (and stray files) under [`dir`], as the
/// shared `(path, name)` pairs of [`entries_of`].
///
/// A missing directory is empty. Any other read error is returned.
pub(crate) async fn leftovers(root: &Path) -> Result<Vec<(PathBuf, String)>, Error> {
    entries_of(&dir(root)).await
}

/// Enqueue one [`RemoveTask`] on the removal pipeline — the shared
/// fire-and-forget handoff of [`reclaim`] and the cleanup stage (D-B):
/// the completion is awaited on a detached task so a failed removal is
/// logged at error level (F03 — a dropped completion would swallow the
/// failure the task now propagates), and an enqueue failure is warned
/// and the leftover is left for doctor / scanner repair. Returns
/// whether the enqueue succeeded.
pub(crate) async fn enqueue_one(
    path: PathBuf,
    pipeline: &Arc<dyn pipeline::Runner<Result<(), Error>>>,
) -> bool {
    // The enqueue handoff is [`enqueue_tracked`]'s; this wrapper adds
    // only the fire-and-forget error log.
    match enqueue_tracked(path.clone(), pipeline).await {
        Ok(done) => {
            tokio::spawn(async move {
                if let Err(err) = done.await {
                    tracing::error!(
                        path = %path.display(),
                        error = %err,
                        "bucket tombstone not removed; left for doctor / scanner repair"
                    );
                }
            });
            true
        }
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "bucket tombstone not enqueued; the scanner covers it"
            );
            false
        }
    }
}

/// Enqueue one [`RemoveTask`] and return its completion — the scanner's
/// leftover stage awaits it so the pass summary counts ACTUAL removals,
/// not enqueues (F03). The completion resolves the removal's own
/// `Result` — `Err` is the tree that could not be removed.
pub(crate) async fn enqueue_tracked(
    path: PathBuf,
    pipeline: &Arc<dyn pipeline::Runner<Result<(), Error>>>,
) -> Result<Completion<Result<(), Error>>, pipeline::Error> {
    pipeline.enqueue(Box::new(RemoveTask { path })).await
}

/// Enqueue `remove_tree` on the removal pipeline and
/// return immediately. Enqueue backpressure and the tree walk run off the
/// request; a failure is warned and the leftover is left for doctor /
/// scanner repair. Shutdown rejects the enqueue the same way.
pub(crate) fn reclaim(pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>, path: PathBuf) {
    tokio::spawn(async move {
        enqueue_one(path, &pipeline).await;
    });
}

/// Removal-pipeline work: `remove_tree` of an
/// unpublished bucket tree — or a stray file under the tombstone dir —
/// with the IO on the tokio blocking pool; one task occupies one worker.
struct RemoveTask {
    path: PathBuf,
}

#[async_trait]
impl pipeline::Task for RemoveTask {
    type Output = Result<(), Error>;

    fn kind(&self) -> &'static str {
        "tombstone"
    }

    async fn run(&mut self) -> Result<(), Error> {
        // F03: the failure PROPAGATES to the awaiter instead of being
        // swallowed by a warn — the scanner counts only actual removals
        // and logs a stuck tree once; the fire-and-forget paths log it
        // from their detached awaiter (`enqueue_one`). The removal
        // lane's worker warns on the `Err` without escalating (D-A).
        remove_tree(&self.path).await.map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[tokio::test]
    async fn remove_task_completes_as_done() {
        use crate::_core::pipeline::{InlineRunner, Runner};

        let root = tempfile::tempdir().unwrap();
        let path = dir(root.path()).join("gone");
        fs::create_dir_all(&path).unwrap();
        let runner = InlineRunner::default();
        runner
            .enqueue(Box::new(RemoveTask { path: path.clone() }))
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn enqueue_one_returns_false_after_shutdown() {
        use crate::_core::pipeline::{InlineRunner, Runner};

        let root = tempfile::tempdir().unwrap();
        let path = dir(root.path()).join("gone");
        let runner: Arc<dyn Runner<Result<(), Error>>> = Arc::new(InlineRunner::default());
        // A shut-down runner rejects the enqueue (Q3) — the leftover is
        // left for doctor / scanner repair, and `false` is reported.
        runner.shutdown();
        let ok = enqueue_one(path, &runner).await;
        assert!(!ok);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn remove_task_reports_failure_when_removal_fails() {
        use std::os::windows::fs::OpenOptionsExt;

        use crate::_core::pipeline::{InlineRunner, Runner};

        let root = tempfile::tempdir().unwrap();
        let path = dir(root.path()).join("stuck");
        fs::create_dir_all(&path).unwrap();
        // An open handle with sharing denied blocks the Windows tree
        // removal (std's default share mode includes FILE_SHARE_DELETE,
        // and `remove_dir_all` clears the read-only attribute, so only a
        // share-mode-0 handle counts as a lock); the task must report
        // the failure (F03) so the awaiting scanner counts no removal
        // and logs the stuck tree — fire-and-forget callers log it from
        // their detached awaiter instead.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .share_mode(0)
            .open(path.join("open.txt"))
            .unwrap();
        let runner = InlineRunner::default();
        let done = runner
            .enqueue(Box::new(RemoveTask { path: path.clone() }))
            .await
            .unwrap();
        assert!(done.await.unwrap().is_err(), "the failure must propagate");
        assert!(path.exists());
        drop(file);
    }
}
