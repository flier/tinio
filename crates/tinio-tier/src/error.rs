//! The tier layer's error type and its projection onto the contract error.
//!
//! Every failure the layer raises is a contract condition: a filesystem, cold
//! or index failure is projected onto [`storage::Error`](tinio_core::storage::Error),
//! the type the S3 layer classifies, while the original carrier stays available
//! for the layer's own diagnostics.

use std::{
    io::{Error as IoError, ErrorKind},
    path::PathBuf,
};

use garde::Report;

use crate::{
    _core::{object, storage},
    _fs, _store_pg,
};

/// A tier-layer failure.
///
/// The carrier names the plane that failed; the projection keeps a contract
/// condition unchanged and turns an engine or configuration failure into an
/// opaque I/O error, because only the contract error's own kinds are
/// classified on the wire.
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// use tinio_core::storage;
/// use tinio_tier::Error;
///
/// let contract = storage::io(io::Error::other("the disk is gone"));
/// let projected: storage::Error = Error::Storage(contract).into();
/// assert!(matches!(projected, storage::Error::Io(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The filesystem plane failed.
    #[error(transparent)]
    Fs(#[from] _fs::Error),
    /// A contract condition passed through unchanged.
    #[error(transparent)]
    Storage(#[from] storage::Error),
    /// The placement index failed.
    #[error("tier index error: {0}")]
    Index(#[from] redb::Error),
    /// The cold plane failed.
    #[error(transparent)]
    Cold(#[from] _store_pg::Error),
    /// The cold plane's copy does not have the length the object's metadata
    /// records — the state a read must never serve as a short or padded body.
    #[error("cold content for `{key}` is {actual} bytes but the object records {expected}")]
    ColdLength {
        /// The object whose copy disagrees.
        key: object::Key,
        /// The length the cold metadata records.
        expected: u64,
        /// The length the cold copy holds.
        actual: u64,
    },
    /// The runtime configuration violates a cross-field rule.
    #[error("invalid tier configuration: {0}")]
    Config(#[from] Report),
    /// The index file, its directory or its permissions could not be reached.
    #[error("tier index I/O error: {0}")]
    Io(#[from] IoError),
    /// A persisted index payload could not be encoded or decoded.
    #[error("tier index encoding error: {0}")]
    Json(#[from] serde_json::Error),
    /// The index records a format version this build does not serve.
    #[error("unsupported tier index {} version {found} (expected {expected})", .path.display())]
    UnsupportedVersion {
        /// The index file path.
        path: PathBuf,
        /// The version read from disk.
        found: u64,
        /// The version this build writes.
        expected: u64,
    },
    /// A write was asked of a read-only index.
    #[error("the tier index is read-only")]
    ReadOnly,
    /// The cold database records a root identity this index does not hold
    /// (T18): `--adopt` is refused unless the two identities match.
    #[error(
        "the cold database belongs to root {cold}, but this root is {index}; `--adopt` refuses a \
         root-ID mismatch"
    )]
    RootMismatch {
        /// The root identity the reserved namespace records.
        cold: String,
        /// The root identity this index records.
        index: String,
    },
    /// A fresh index over a non-empty cold database (T18): the operator's
    /// `--adopt` is the only way to bind the two.
    #[error("the cold database already holds data; rerun with `--adopt` to bind this root to it")]
    AdoptRequired,
    /// The reserved root marker is not a 16-byte root identity.
    #[error("the reserved root marker is {len} bytes, not a 16-byte root identity")]
    CorruptRootId {
        /// The length the marker holds.
        len: u64,
    },
    /// A root that already holds the reserved bucket name cannot enter tier
    /// mode (T18): the pg plane's root identity lives under that name.
    #[error("the root already holds the reserved namespace `{name}`")]
    ReservedRoot {
        /// The reserved bucket name the root holds.
        name: String,
    },
    /// The marker records a root identity the index does not (T17): the two
    /// records of one root must agree, and a mismatch is never reconciled
    /// silently.
    #[error("the tier marker records root {marker}, but the index records {index}")]
    MarkerMismatch {
        /// The identity the marker records.
        marker: String,
        /// The identity the index records (`none` when it records none).
        index: String,
    },
    /// The marker is not a 16-byte root identity: a partial write or a foreign
    /// file at the marker's path is refused rather than re-created (T17).
    #[error(
        "the tier marker {} is {len} bytes, not a 16-byte root identity",
        .path.display()
    )]
    CorruptMarker {
        /// The marker's path.
        path: PathBuf,
        /// The length the file holds.
        len: u64,
    },
    /// A read-only run needs the index an earlier writable run created (T27):
    /// the read-only open creates nothing.
    #[error(
        "the tier index {} is missing; a read-only run cannot create it",
        .path.display()
    )]
    IndexMissing {
        /// The index path the run expected.
        path: PathBuf,
    },
    /// The root is a tier root, so a non-tier backend must not serve it
    /// (T17): the marker, the index or both make the root tier-owned.
    #[error(
        "the root is in tier mode (marker: {marker}, index: {index}); a non-tier start must not \
         serve it"
    )]
    TierRoot {
        /// Whether the marker is present.
        marker: bool,
        /// Whether the index is present.
        index: bool,
    },
}

/// Every redb failure kind lifts into [`Error::Index`], so the index code
/// spells `?` on the driver's own error types without a `map_err` at each
/// call site.
macro_rules! index_error_from {
    ($($kind:ty),+ $(,)?) => {
        $(
            impl From<$kind> for Error {
                fn from(err: $kind) -> Self {
                    Self::Index(err.into())
                }
            }
        )+
    };
}

index_error_from!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::CompactionError,
    redb::SetDurabilityError,
);

impl Error {
    /// Whether this failure is the read-only index's write refusal rather
    /// than an index fault.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Error;
    ///
    /// assert!(Error::ReadOnly.is_read_only());
    /// assert!(!Error::Json(serde_json::Error::io(std::io::Error::other("boom"))).is_read_only());
    /// ```
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(e) => e,
            Error::Fs(e) => e.into(),
            // A read-only cold plane refuses a write through the pg session's
            // own read-only kind; the store's projection is the one place that
            // condition is spelled, so a read whose cold entry would heal a row
            // degrades on it instead of failing.
            Error::Cold(e) => storage::Error::from(e),
            err @ Error::ColdLength { .. } => storage::io(IoError::other(err)),
            Error::Index(e) => storage::io(IoError::other(e)),
            Error::Config(e) => storage::io(IoError::other(e)),
            Error::Io(e) => storage::io(e),
            Error::Json(e) => storage::io(IoError::other(e)),
            Error::UnsupportedVersion {
                path,
                found,
                expected,
            } => storage::io(IoError::other(Error::UnsupportedVersion {
                path,
                found,
                expected,
            })),
            Error::ReadOnly => {
                storage::io(IoError::new(ErrorKind::ReadOnlyFilesystem, Error::ReadOnly))
            }
            err @ Error::RootMismatch { .. }
            | err @ Error::AdoptRequired
            | err @ Error::CorruptRootId { .. }
            | err @ Error::ReservedRoot { .. }
            | err @ Error::MarkerMismatch { .. }
            | err @ Error::CorruptMarker { .. }
            | err @ Error::IndexMissing { .. }
            | err @ Error::TierRoot { .. } => storage::io(IoError::other(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use garde::{Error as GardeError, Path as GardePath, Report};

    use super::*;
    use crate::{
        _core::{
            object,
            storage::{self, Error as StorageError},
        },
        _fs, _store_pg,
    };

    #[test]
    fn error_projects_to_storage() {
        let key = object::key("missing").expect("a valid key");
        let contract = storage::no_such_key(&key);
        let expected = contract.to_string();
        let projected: StorageError = Error::Storage(contract).into();
        assert!(matches!(projected, StorageError::NoSuchKey(_)));
        assert_eq!(projected.to_string(), expected);

        let projected: StorageError = Error::Fs(_fs::Error::InvalidPath("a/../b".into())).into();
        assert!(matches!(projected, StorageError::InvalidKey(_)));

        let mut report = Report::new();
        report.append(
            GardePath::new("capacity_bytes"),
            GardeError::new("too small"),
        );
        let projected: StorageError = Error::Config(report).into();
        let StorageError::Io(err) = projected else {
            panic!("a configuration failure projects through `Io`");
        };
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("capacity_bytes"));
    }

    /// The cold plane's read-only refusal keeps the contract's read-only kind
    /// — the same condition `tinio-pg` spells so a read can degrade — while
    /// every other cold failure stays an opaque I/O error.
    #[test]
    fn a_read_only_cold_refusal_projects_to_read_only() {
        let message = "postgres: the session is read-only: read-only transaction";
        let projected: StorageError =
            Error::Cold(_store_pg::Error::ReadOnlySession(message.to_owned())).into();
        let StorageError::Io(err) = projected else {
            panic!("a cold failure projects through `Io`");
        };
        assert_eq!(err.kind(), io::ErrorKind::ReadOnlyFilesystem);
        assert_eq!(err.to_string(), message);
        assert!(StorageError::Io(err).is_read_only());

        let projected: StorageError = Error::Cold(_store_pg::Error::Database("boom".into())).into();
        let StorageError::Io(err) = projected else {
            panic!("a cold failure projects through `Io`");
        };
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("boom"));
    }
}
