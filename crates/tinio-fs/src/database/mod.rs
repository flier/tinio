//! The redb state database — `<state-dir>/meta.redb` (meta-redb-spec §5).
//!
//! All derived metadata lives in one file across five tables (`OBJECT_META`,
//! `BUCKETS`, `UPLOADS`, `PARTS`, `STATE`); the file system keeps only the
//! multipart part contents and the `tmp/` staging directory.
//!
//! [`open`] opens-or-creates the database and its five tables in one write
//! transaction (read transactions refuse to open a table that does not
//! exist yet) and checks the `STATE` version: a missing version is written
//! (fresh database), a mismatch is [`Error::UnsupportedVersion`] (nested
//! under [`crate::Error::Database`] at the crate boundary).
//!
//! [`Handle`] is the shared access handle (meta-redb-spec G2): closure
//! based — a transaction's lifetime is sealed inside the closure, so a
//! transaction guard cannot escape — and multi-table operations run as one
//! write closure. Calls block directly (G3); the pipeline stage wraps
//! `write` for write-lock timing.
//!
//! Per-kind redb errors live in [`Error`]; every function in this module
//! returns it. The crate lifts it via [`From`] into [`crate::Error::Database`]
//! (database I/O unwraps to [`crate::Error::Io`]).

mod compact;
mod error;
mod handle;
mod open;
mod scan;
mod tables;

#[cfg(test)]
mod tests;

pub use compact::{Compaction, Stats, compact_if_needed};
pub use error::Error;
pub use open::{Integrity, Open, check_integrity, open};

pub use handle::Handle;
#[cfg(test)]
pub(crate) use tables::StateTable;
pub(crate) use tables::{BucketsTable, PartsTable, UploadsTable};
pub use tables::{ObjectMetaTable, StoredMeta};
