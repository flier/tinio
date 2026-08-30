//! Filesystem backend errors (task T019).
//!
//! One [`Error`] type for the crate. The contract boundary is
//! [`tinio_core::storage::Error`] via [`From`].
//!
//! # Examples
//!
//! ```rust
//! use tinio_core::storage;
//! use tinio_fs::Error;
//!
//! let err: Error = Error::InvalidPath("traversal".into());
//! let core: storage::Error = err.into();
//! assert!(matches!(core, Error::InvalidKey(_)));
//! ```

use std::{
    error::Error as StdError,
    io::{self, Error as IoError},
    path::PathBuf,
};

use crate::{
    _core::{pipeline, storage},
    database::{self, Error as DatabaseError},
};

/// A filesystem backend failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A filesystem I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A task-pipeline failure (pipeline-spec.md P7): the task was not
    /// accepted (shutdown, Q3) or dropped before its result was sent
    /// (panic, R6). The original [`pipeline::Error`] is kept.
    #[error("pipeline error: {0}")]
    Pipeline(#[from] pipeline::Error),
    /// A path-mapping violation (traversal, platform charset, reserved
    /// segments) — rejected before any filesystem access.
    #[error("invalid path: {}", .0.display())]
    InvalidPath(PathBuf),
    /// A contract-domain error passed through (key/bucket validation,
    /// not-found conditions).
    #[error("{0}")]
    Storage(#[from] storage::Error),
    /// A redb state-database failure (per-kind, see [`database::Error`]).
    #[error(transparent)]
    Database(database::Error),
    /// A construction-option value violates validation rules.
    #[error("invalid options: {0}")]
    InvalidValue(garde::Report),
    /// The storage root exists but is not a directory.
    #[error("storage root is not a directory: {}", .0.display())]
    RootNotDirectory(PathBuf),
}

/// A path-mapping violation (rejected before any filesystem access).
#[inline]
pub(crate) fn invalid_path(path: impl Into<PathBuf>) -> Error {
    Error::InvalidPath(path.into())
}

/// A construction-option value violates validation rules.
#[inline]
pub(crate) fn invalid_value(report: garde::Report) -> Error {
    Error::InvalidValue(report)
}

/// The storage root exists but is not a directory.
#[inline]
pub(crate) fn root_not_directory(path: impl Into<PathBuf>) -> Error {
    Error::RootNotDirectory(path.into())
}

impl From<database::Error> for Error {
    fn from(err: database::Error) -> Self {
        match err {
            // Database I/O unwraps to the public `Io`; everything else —
            // including the version mismatch (a top-level duplicate
            // variant was removed) — stays nested under `Database`.
            DatabaseError::Io(e) => Error::Io(e),
            other => Error::Database(other),
        }
    }
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(e) => storage::io(e),
            Error::InvalidPath(p) => storage::invalid_key(p.to_string_lossy().into_owned()),
            Error::Storage(e) => e,
            Error::Database(e) => storage::io(IoError::other(e)),
            Error::Pipeline(e) => storage::io(IoError::other(e)),
            Error::InvalidValue(_) | Error::RootNotDirectory(_) => storage::io(IoError::other(err)),
        }
    }
}

/// View this error as a [`std::error::Error`] trait object — the
/// `pipeline::Outcome` blanket (`Result<T, E>` with `E:
/// AsRef<dyn StdError + Send + Sync>`, pipeline.rs) requires it, so the
/// task-pipeline runtimes can log the original failure (R8, never
/// stringified).
impl AsRef<dyn StdError + Send + Sync> for Error {
    fn as_ref(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }
}

#[cfg(test)]
mod tests {
    use garde::Validate;
    use redb::DatabaseError::DatabaseAlreadyOpen;

    use super::*;
    use crate::{
        _core::{
            pipeline::Error::{Dropped, ShutDown},
            storage::{COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT, Error::*},
        },
        _util::testing::assert_send_sync,
        database::Error::{Open, UnsupportedVersion},
    };

    #[derive(Validate)]
    struct Probe {
        #[garde(
            range(
                min = COMPACT_THRESHOLD_MIN_PERCENT,
                max = COMPACT_THRESHOLD_MAX_PERCENT
            )
        )]
        compact_threshold_percent: u8,
    }

    #[test]
    fn displays_variants() {
        let path = PathBuf::from("/data/root");
        let report = Probe {
            compact_threshold_percent: 0,
        }
        .validate()
        .unwrap_err();
        let cases: [(Error, &str); 7] = [
            (Error::Io(IoError::other("boom")), "I/O error: boom"),
            (Error::InvalidPath("a/../b".into()), "invalid path: a/../b"),
            (invalid_value(report), "invalid options:"),
            (Error::Storage(NoSuchKey("x".into())), "no such object: `x`"),
            (
                root_not_directory(&path),
                "storage root is not a directory: /data/root",
            ),
            (
                Error::Database(UnsupportedVersion {
                    path,
                    found: 9,
                    expected: 1,
                }),
                "unsupported /data/root version 9 (expected 1)",
            ),
            (
                Error::Pipeline(ShutDown),
                "pipeline error: pipeline is shut down",
            ),
        ];
        for (err, prefix) in cases {
            assert!(
                err.to_string().starts_with(prefix),
                "got {:?}, expected prefix {:?}",
                err,
                prefix
            );
        }
    }

    #[test]
    fn converts_into_contract_error() {
        let io_err = Error::Io(IoError::other("disk full"));
        let core: storage::Error = io_err.into();
        assert!(matches!(core, Io(_)));

        let path_err = Error::InvalidPath("traversal".into());
        let core: storage::Error = path_err.into();
        assert!(matches!(core, InvalidKey(_)));

        // redb failures project onto Io (never misclassified as a
        // contract-domain condition).
        let db_err: Error = Open(DatabaseAlreadyOpen).into();
        let core: storage::Error = db_err.into();
        assert!(matches!(core, Io(_)));

        // Pipeline failures (shutdown/dropped) also project onto Io —
        // they are never contract-domain conditions.
        let pipeline_err: Error = Dropped.into();
        let core: storage::Error = pipeline_err.into();
        assert!(matches!(core, Io(_)));
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
    }
}
