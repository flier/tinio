//! Offline/test construction helpers (F33).
//!
//! [`FsOptions`] deliberately has no `Default` (the two pipelines are
//! mandatory construction-time decisions, P4), so every offline context —
//! tests, benches, doctor — used to spell out all six fields. The helpers
//! here are the single home of that boilerplate; production wiring
//! (`tinio-server`'s `serve`) passes its real pipeline runtimes instead.

use std::sync::Arc;

use tinio_core::{
    pipeline::InlineRunner,
    storage::{
        DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
    },
};

use crate::FsOptions;

/// The standard offline [`FsOptions`]: the fs defaults plus the mandatory
/// inline pipelines (P4 — offline contexts pass [`InlineRunner`], Q1).
/// The former copy-paste across six test and bench files (F33) now lives
/// here.
///
/// # Examples
///
/// ```rust
/// use tinio_fs::{FsOptions, FsStorage, testing};
///
/// let root = tempfile::tempdir().unwrap();
/// let storage = FsStorage::new(root.path(), testing::fs_options()).unwrap();
/// ```
pub fn fs_options() -> FsOptions {
    FsOptions {
        follow_symlinks: false,
        state_dir: None,
        compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
        meta_batch_size: DEFAULT_META_BATCH_SIZE,
        meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
        io_pipeline: Arc::new(InlineRunner::default()),
        remove_pipeline: Arc::new(InlineRunner::default()),
        db_pipeline: Arc::new(InlineRunner::default()),
    }
}
