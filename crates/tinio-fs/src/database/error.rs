//! redb error nesting (the shared [`crate::_store::Error`] under
//! `Redb`) and the version-mismatch constructor.

use std::{io, path::PathBuf};

use crate::{_core::storage, _store};

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
    /// A shared redb failure (the kinds of [`crate::_store::Error`] — the
    /// five redb mappings and the read-only refusal the shared handle's
    /// arms answer).
    #[error(transparent)]
    Redb(#[from] _store::Error),
}

impl Error {
    /// Whether this failure is "another process holds the store" — the redb lock's
    /// `DatabaseAlreadyOpen`, which is a fact about the calling run rather than about the
    /// database: `doctor` reports it as "not inspected", never as corruption.
    ///
    /// The predicate lives here because this crate owns the error and the `_store` nesting: a
    /// consumer asking the question would otherwise have to reach `redb`'s variant through a
    /// message string or take a dependency on `tinio-store` to match the shape.
    pub fn is_already_open(&self) -> bool {
        matches!(
            self,
            Self::Redb(_store::Error::Open(
                redb::DatabaseError::DatabaseAlreadyOpen
            ))
        )
    }

    /// Whether this failure is "the handle is read-only" — the write was
    /// refused because the run opened the root's database read-only
    /// (FR-023 amended), not because anything is broken. The refusal is
    /// the shared handle's arm answering
    /// ([`crate::_store::Error::ReadOnly`]), nested here like every other
    /// shared failure.
    ///
    /// The read paths that recompute a missing or stale entry ask this to
    /// serve the recomputed row without persisting it: in a read-only run
    /// that recomputation "is simply not written back" (design 2026-10-03,
    /// **Read paths**), and the read still answers.
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::Redb(_store::Error::ReadOnly))
    }
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
