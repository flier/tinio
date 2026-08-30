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

use crate::{
    error::Error,
    fsutil::{entries_of, remove_tree, remove_tree_blocking},
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
    tokio::fs::create_dir_all(&dir).await?;
    Ok(dir.join(uuid::Uuid::new_v4().to_string()))
}

/// Unpublished leftover trees (and stray files) under [`dir`].
///
/// A missing directory is empty. Any other read error is returned.
pub(crate) async fn leftovers(root: &Path) -> Result<Vec<(String, PathBuf)>, Error> {
    Ok(entries_of(&dir(root))
        .await?
        .into_iter()
        .map(|(path, name)| (name, path))
        .collect())
}

/// Remove one leftover path (`remove_dir_all`, or `remove_file` if it is
/// not a directory). Missing is success.
pub(crate) async fn clear_one(path: &Path) -> Result<(), Error> {
    remove_tree(path).await?;
    Ok(())
}

/// Clear every leftover under [`dir`]. Returns the number removed; a
/// failed entry is warned and skipped (the scanner's count-only path).
pub(crate) async fn clear_leftovers(root: &Path) -> Result<usize, Error> {
    let mut cleared = 0usize;
    for (_, path) in leftovers(root).await? {
        match clear_one(&path).await {
            Ok(()) => cleared += 1,
            Err(err) => tracing::warn!(
                path = %path.display(),
                error = %err,
                "delete tombstone not reclaimed"
            ),
        }
    }
    Ok(cleared)
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

/// Enqueue blocking `remove_tree_blocking` on the removal pipeline and
/// return immediately. Enqueue backpressure and the tree walk run off the
/// request; a failure is warned and the leftover is left for doctor /
/// scanner repair. Shutdown rejects the enqueue the same way.
pub(crate) fn reclaim(pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>, path: PathBuf) {
    tokio::spawn(async move {
        enqueue_one(path, &pipeline).await;
    });
}

/// Blocking removal-pipeline work: `remove_tree_blocking` of an
/// unpublished bucket tree — or a stray file under the tombstone dir —
/// with no internal `.await`; one task occupies one worker.
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
        if let Err(err) = remove_tree_blocking(&self.path) {
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
    use super::*;
    use crate::testutil::rt;
    use std::fs;

    #[test]
    fn clear_leftovers_removes_unpublished_trees() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let leftover = dir(root.path()).join("dead-bucket");
            fs::create_dir_all(&leftover).unwrap();
            fs::write(leftover.join("leftover.bin"), b"was-a-bucket").unwrap();
            assert_eq!(clear_leftovers(root.path()).await.unwrap(), 1);
            assert!(!leftover.exists());
        });
    }

    #[test]
    fn remove_task_completes_as_done() {
        use tinio_core::pipeline::{InlineRunner, Runner};
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let path = dir(root.path()).join("gone");
            fs::create_dir_all(&path).unwrap();
            let runner = InlineRunner::default();
            let done = runner
                .enqueue(Box::new(RemoveTask { path: path.clone() }))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(done, ());
            assert!(!path.exists());
        });
    }
}
