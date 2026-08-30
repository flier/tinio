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
use tinio_core::pipeline;
use tokio::fs;
use uuid::Uuid;

use crate::{
    error::Error,
    fsutil::{entries_of, remove_tree},
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
    // Item 7d: the deleting dir exists in steady state — one probe
    // instead of the per-component walk (the create still runs on the
    // first delete).
    if !fs::try_exists(&dir).await? {
        fs::create_dir_all(&dir).await?;
    }
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
/// the completion is dropped, an enqueue failure is warned and the
/// leftover is left for doctor / scanner repair. Returns whether the
/// enqueue succeeded.
pub(crate) async fn enqueue_one(
    path: PathBuf,
    pipeline: &Arc<dyn pipeline::Runner<Result<(), Error>>>,
) -> bool {
    match pipeline
        .enqueue(Box::new(RemoveTask { path: path.clone() }))
        .await
    {
        Ok(done) => {
            drop(done);
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
        if let Err(err) = remove_tree(&self.path).await {
            tracing::warn!(
                path = %self.path.display(),
                error = %err,
                "bucket tombstone not removed after delete"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[tokio::test]
    async fn remove_task_completes_as_done() {
        use tinio_core::pipeline::{InlineRunner, Runner};

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
}
