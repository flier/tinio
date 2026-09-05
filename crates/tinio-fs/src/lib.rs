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
pub extern crate tinio_core as _core;
#[doc(hidden)]
pub extern crate tinio_store as _store;
#[doc(hidden)]
pub extern crate tinio_util as _util;

mod backend;
pub mod bucket;
mod cleanup;
pub mod database;
mod error;
pub mod etag;
mod fsutil;
mod listing;
pub mod meta;
pub mod multipart;
mod pacing;
pub mod path;
mod scanner;
pub mod sweep;
pub mod testing;
#[cfg(test)]
mod testutil;
pub mod tombstone;
mod write;
mod write_task;

pub use self::{
    backend::{FsOptions, FsStorage, StagedBody},
    cleanup::FsCleanup,
    error::Error,
    listing::FsListing,
    path::state_dir,
    scanner::{ScanSummary, Scanner, ScannerOptions},
    write::AtomicWriter,
};
