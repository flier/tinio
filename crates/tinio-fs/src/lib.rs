//! Filesystem backend for tinio.
//!
//! Implements the `tinio-core` `Storage` contract over the local filesystem:
//! buckets map to top-level subdirectories of the storage root, objects to
//! files. Private state lives in the reserved `<root>/.tinio/` directory
//! (the `meta.redb` state database, multipart part files, temp files).
//!
//! The implementation is split by concern per fs-backend.md: `path` (path
//! mapping), `write` (atomic streaming writes), `meta` (the ETag store),
//! `bucket` (creation times), `listing`, `multipart`, `scanner`, `sweep`,
//! `tombstone` (unpublished delete-bucket trees), and `cleanup`
//! (`Cleanup` trait impl); the `backend/` modules implement the
//! `Storage` contract over those primitives. The IO pipeline's job
//! output is [`etag::Result`]; the removal pipeline's is
//! `Result<(), Error>`.

#[doc(hidden)]
pub extern crate tinio_blob as _blob;
#[doc(hidden)]
pub extern crate tinio_core as _core;
#[doc(hidden)]
pub extern crate tinio_store_redb as _store_redb;
#[doc(hidden)]
pub extern crate tinio_util as _util;
#[doc(hidden)]
pub extern crate tinio_version as _version;

mod backend;
pub mod bucket;
mod cleanup;
mod content;
pub mod database;
mod error;
pub mod etag;
mod fsutil;
mod listing;
pub mod meta;
#[cfg(feature = "versioning")]
mod metrics;
pub mod multipart;
pub mod path;
mod scanner;
pub mod sweep;
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: compiled outside `cfg(test)` — offline construction helpers for tests, benches and the doctor, not production code (docs/style.md §Panics, design 2026-10-09 D7)"
)]
pub mod testing;
#[cfg(test)]
mod testutil;
pub mod tombstone;
#[cfg(feature = "versioning")]
mod version;
#[cfg(feature = "versioning")]
mod version_adapter;
mod write;
mod write_task;

pub use self::{
    backend::{FsOptions, FsStorage, StagedBody},
    cleanup::FsCleanup,
    content::FsContent,
    error::Error,
    fsutil::is_symlink_or_reparse,
    listing::FsListing,
    path::state_dir,
    scanner::{ScanSummary, Scanner, ScannerOptions},
    write::AtomicWriter,
};
