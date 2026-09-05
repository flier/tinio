//! redb error nesting (the shared [`crate::_store::Error`] under
//! `Redb`) and the version-mismatch constructor.

use std::{io, path::PathBuf};

use crate::_core::storage;

/// A redb or state-database failure.
///
/// Conversions derive via thiserror: `#[from]` on `Io`, `Compaction`,
/// and `Redb` emits `From<io::Error>`, `From<redb::CompactionError>`,
/// and From<[`crate::_store::Error`]> — all three are hand-free, and
/// no `From` impls are hand-written. The five raw redb errors are not
/// forwarded (`From` is not transitive): they hop through the shared
/// error first, wrapped explicitly at the fs sites (`Error::Redb(e.into())`).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Filesystem I/O around the state database.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A compaction failed.
    #[error("compaction error: {0}")]
    Compaction(#[from] redb::CompactionError),
    /// The `STATE` table version does not match.
    #[error(
        "unsupported {} version {found} (expected {expected})",
        .path.display()
    )]
    UnsupportedVersion {
        /// The state file path.
        path: PathBuf,
        /// The version read from disk.
        found: u64,
        /// The supported version.
        expected: u64,
    },
    /// A stored `OBJECT_META` row failed domain validation (key or etag).
    #[error("corrupt object_meta entry for key `{key}`: {source}")]
    CorruptMeta {
        /// The raw key as stored.
        key: String,
        /// The domain validation failure (`InvalidKey` / etag parse).
        #[source]
        source: storage::Error,
    },
    /// A shared redb failure (the five mapping kinds of
    /// [`crate::_store::Error`]).
    #[error(transparent)]
    Redb(#[from] crate::_store::Error),
}

/// The `STATE` table version does not match.
#[inline]
pub(crate) fn unsupported_version(path: impl Into<PathBuf>, found: u64, expected: u64) -> Error {
    Error::UnsupportedVersion {
        path: path.into(),
        found,
        expected,
    }
}

/// A stored `OBJECT_META` row failed domain validation.
#[inline]
pub(crate) fn corrupt_meta(key: impl Into<String>, source: storage::Error) -> Error {
    Error::CorruptMeta {
        key: key.into(),
        source,
    }
}
