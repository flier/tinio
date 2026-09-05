//! The redb state database — `<state-dir>/meta.redb` (meta-redb-spec §5).
//!
//! All derived metadata lives in one file across eight tables
//! (`OBJECT_META`, `BUCKETS`, `UPLOADS`, `PARTS`, `UPLOAD_CHECKSUMS`,
//! `PART_CHECKSUMS`, `OBJECT_PARTS`, `STATE`); the file system keeps only
//! the multipart part contents and the `tmp/` staging directory.
//!
//! [`open`] opens-or-creates the database and its tables in one write
//! transaction (read transactions refuse to open a table that does not
//! exist yet) and checks the `STATE` version: a missing version is written
//! (fresh database), a mismatch is [`Error::UnsupportedVersion`] (nested
//! under [`crate::error::Error::Database`] at the crate boundary).
//!
//! [`Handle`] is the shared access handle (meta-redb-spec G2): closure
//! based — a transaction's lifetime is sealed inside the closure, so a
//! transaction guard cannot escape — and multi-table operations run as one
//! write closure. Write transactions run on the tokio blocking pool
//! (G3, revised by the data-path review 2026-08-27 — every `Immediate`
//! commit is an fsync, so `Handle::write` is async and `spawn_blocking`s
//! the closure + commit); reads stay inline. `Handle` times every write
//! transaction (write-lock histograms, pipeline-spec.md §4).
//!
//! Per-kind redb errors live in [`Error`]; every function in this module
//! returns it. The crate lifts it via [`From`] into
//! [`crate::error::Error::Database`] (database I/O unwraps to
//! [`Error::Io`]).

mod compact;
mod error;
mod handle;
mod open;
mod tables;

#[cfg(test)]
mod tests;

pub use compact::{Compaction, Stats, compact_if_needed};
pub use error::Error;
pub use handle::{Handle, WriteLockSnapshot};
pub use open::{Integrity, Open, check_integrity, open};
pub(crate) use tables::for_bucket_strict;

pub(crate) use crate::_core::object::{BUCKET_TAGS_MAX, OBJECT_TAGS_MAX};
#[cfg(test)]
pub(crate) use crate::_store::state::Table as StateTable;
