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
//! and `cleanup` (`Cleanup` trait impl); the `backend/` modules implement
//! the `Storage` contract over those primitives.

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
mod write;
mod write_task;

pub use self::backend::{FsOptions, FsStorage, StagedBody};
pub use self::cleanup::FsCleanup;
pub use self::error::Error;
pub use self::listing::FsListing;
pub use self::path::state_dir;
pub use self::scanner::{ScanSummary, Scanner, ScannerOptions};
pub use self::write::AtomicWriter;
