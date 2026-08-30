//! redb error nesting (`#[from]` per kind) and the version-mismatch
//! constructor.

use std::{io, path::PathBuf};

use crate::_core::storage;

/// A redb or state-database failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database open/create failed.
    #[error("database error: {0}")]
    Open(#[from] redb::DatabaseError),
    /// A transaction failed.
    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Opening a table failed.
    #[error("table error: {0}")]
    Table(#[from] redb::TableError),
    /// A get/insert/range failed.
    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
    /// A compaction failed.
    #[error("compaction error: {0}")]
    Compaction(#[from] redb::CompactionError),
    /// Commit failed.
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
    /// Filesystem I/O around the state database.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
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
