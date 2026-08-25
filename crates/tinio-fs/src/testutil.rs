//! Shared test helpers (`#[cfg(test)]` only).

use std::{future::Future, path::Path};

use crate::FsStorage;

/// Run `f` to completion on a fresh multi-thread runtime.
pub(crate) fn rt<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

/// A fresh storage root + backend (default options) — the shared backend
/// test fixture.
pub(crate) fn storage() -> (tempfile::TempDir, FsStorage) {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), Default::default()).unwrap();
    (root, storage)
}

/// Retarget a followed bucket symlink while a write is blocked between
/// staging/assembly and the rename: hold the mutation lock, spawn `op`,
/// wait until `ready` (phase 1 done), swap `link` to `new_target`, then
/// release the lock and await `op`.
pub(crate) async fn retarget_bucket_during_commit<F, Fut, R, T>(
    storage: &FsStorage,
    link: &Path,
    new_target: &Path,
    ready: R,
    op: F,
) -> T
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    R: Future<Output = ()>,
    T: Send + 'static,
{
    let guard = storage.lock_bucket_mutations().await;
    let handle = tokio::spawn(op());
    ready.await;
    replace_dir_link(link, new_target);
    drop(guard);
    handle.await.unwrap()
}

/// Wait until a file appears under `<state-dir>/tmp/` (assembly / first
/// stage has finished).
pub(crate) async fn wait_for_tmp(storage: &FsStorage) {
    let tmp = storage.state_dir().join("tmp");
    let appeared = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(mut entries) = tokio::fs::read_dir(&tmp).await
                && entries.next_entry().await.ok().flatten().is_some()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(appeared.is_ok(), "phase-1 temp never appeared under tmp/");
}

/// Yield long enough for a spawned commit that is waiting on the
/// mutation lock to finish its pre-lock resolve.
pub(crate) async fn wait_for_lock_waiter() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}

/// Create a directory symlink (Unix) or directory symlink (Windows).
pub(crate) fn link_dir(original: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(original, link).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(original, link).unwrap();
}

fn replace_dir_link(link: &Path, new_target: &Path) {
    #[cfg(unix)]
    {
        std::fs::remove_file(link).unwrap();
        std::os::unix::fs::symlink(new_target, link).unwrap();
    }
    #[cfg(windows)]
    {
        std::fs::remove_dir(link).unwrap();
        std::os::windows::fs::symlink_dir(new_target, link).unwrap();
    }
}
