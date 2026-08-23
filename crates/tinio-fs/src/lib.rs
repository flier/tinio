//! Filesystem backend for tinio.
//!
//! Implements the `tinio-core` `Storage` contract over the local filesystem:
//! buckets map to top-level subdirectories of the storage root, objects to
//! files. Private state lives in the reserved `<root>/.tinio/` directory
//! (meta store, buckets.json, multipart parts, temp files).
//!
//! The implementation is split by concern per fs-backend.md: `path` (path
//! mapping), `write` (atomic streaming writes), `meta` (the ETag store),
//! `buckets` (creation times), `listing`, `multipart`, `scanner`, `sweep`,
//! and `cleanup` (`Cleanup` trait impl); the `backend/` modules implement
//! the `Storage` contract over those primitives.

mod backend;
mod buckets;
mod cleanup;
mod error;
mod fsutil;
mod listing;
mod meta;
mod multipart;
mod pacing;
mod path;
mod scanner;
mod sweep;
#[cfg(test)]
mod testutil;
mod write;

pub use self::buckets::BucketStore;
pub use self::cleanup::FsCleanup;
pub use self::error::{BackendError, Error};
pub use self::listing::FsListing;
pub use self::meta::{MetaRecord, MetaStore};
pub use self::multipart::MultipartStore;
pub use self::path::state_dir;
pub use self::scanner::{ScanSummary, Scanner, ScannerOptions};
pub use self::sweep::{SweepOptions, SweepSummary, Sweeper};
pub use self::write::AtomicWriter;
pub use backend::{FsOptions, FsStorage};
