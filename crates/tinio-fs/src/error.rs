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
//! assert!(matches!(core, storage::Error::InvalidKey(_)));
//! ```

use std::{
    error::Error as StdError,
    io::{self, Error as IoError},
    path::PathBuf,
};

use garde::{Error as GardeError, Path as GardePath, Report};
#[cfg(test)]
use tinio_util::testing::object_key;

use crate::{
    _core::{pipeline, storage},
    _util::metrics::{Backend, WriteFailureKind, WriteOp, record_write_failure},
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
    /// An internal invariant this crate believed unreachable was violated —
    /// the fallible replacement for a would-be panic (design 2026-10-09 D5,
    /// named there). The payload names the broken invariant for `Debug` and
    /// the log; `Display` stays generic, so the wire body never carries an
    /// internal symbol.
    #[error("internal error")]
    Invariant(&'static str),
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

/// The `state_dir` option violates a read-only-mode rule: `garde` validates
/// field by field, so a cross-field rule (a read-only run's state dir must
/// be outside the root) builds its report by hand (FR-023).
#[inline]
pub(crate) fn invalid_state_dir(message: &str) -> Error {
    let mut report = Report::new();
    report.append(GardePath::new("state_dir"), GardeError::new(message));
    Error::InvalidValue(report)
}

/// The storage root exists but is not a directory.
#[inline]
pub(crate) fn root_not_directory(path: impl Into<PathBuf>) -> Error {
    Error::RootNotDirectory(path.into())
}

/// Project an engine failure onto this crate's error (the engines' one error
/// type projects onto [`storage::Error`], which this crate wraps).
#[cfg(feature = "versioning")]
#[inline]
pub(crate) fn projected(err: impl Into<storage::Error>) -> Error {
    Error::from(err.into())
}

/// Project this crate's failure onto the contract error the version engine's
/// ports answer (the engine has one error type; the projection is what the
/// port discipline exists for).
#[cfg(feature = "versioning")]
#[inline]
pub(crate) fn contract(err: impl Into<Error>) -> storage::Error {
    err.into().into()
}

impl Error {
    /// Whether this failure is "the handle is read-only" — a write was
    /// refused because the run opened the root's database read-only
    /// (FR-023 amended; [`database::Error::is_read_only`]'s crate-level
    /// spelling). The read paths that rewrite a row they could not hit
    /// ask this to serve the recomputed row without persisting it.
    ///
    /// Both of this crate's error carriers answer: a failure raised on this
    /// crate's own file/state paths ([`database::Error::is_read_only`]), and
    /// the one an **ops-engine** body raises, which reaches this crate as a
    /// contract error carrying the same marker
    /// ([`storage::Error::is_read_only`] — the engine's bodies answer
    /// `storage::Error`).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::io;
    ///
    /// use tinio_fs::{Error, database};
    ///
    /// // The shared handle's arm answer, nested like every shared failure.
    /// let refused: Error = database::Error::from(tinio_store_redb::Error::ReadOnly).into();
    /// assert!(refused.is_read_only());
    ///
    /// // The engine's own projection of the same refusal.
    /// let engine: Error =
    ///     tinio_core::storage::Error::from(io::Error::from(io::ErrorKind::ReadOnlyFilesystem)).into();
    /// assert!(engine.is_read_only());
    ///
    /// // Anything else keeps propagating: only the read-only refusal is
    /// // the designed "recomputed but not written back" answer.
    /// let unrelated = Error::InvalidPath("a/../b".into());
    /// assert!(!unrelated.is_read_only());
    /// ```
    pub fn is_read_only(&self) -> bool {
        match self {
            Self::Database(err) => err.is_read_only(),
            Self::Storage(err) => err.is_read_only(),
            _ => false,
        }
    }
}

/// The closed failure class of a failed fs write, as the shared
/// `tinio_storage_write_failures_total` family's `kind` label spells it: a
/// read-only refusal is its own class, every other failure is `io`.
pub(crate) fn write_failure_kind(err: &Error) -> WriteFailureKind {
    if err.is_read_only() {
        WriteFailureKind::ReadOnly
    } else {
        WriteFailureKind::Io
    }
}

/// Record one failed fs write under its bounded operation — the one home of
/// this backend's label, so no call site can spell it differently.
pub(crate) fn record_failed_write(op: WriteOp, err: &Error) {
    record_write_failure(Backend::Fs, op, write_failure_kind(err));
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
            // A read-only refusal keeps its kind across the conversion: the
            // shared redb error's own projection already carries it (and its
            // payload), so this delegates rather than re-deriving the marker.
            Error::Database(DatabaseError::Redb(e)) if e.is_read_only() => e.into(),
            Error::Database(e) => storage::io(IoError::other(e)),
            Error::Pipeline(e) => storage::io(IoError::other(e)),
            Error::InvalidValue(_) | Error::RootNotDirectory(_) | Error::Invariant(_) => {
                storage::io(IoError::other(err))
            }
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
        _store_redb,
        _util::testing::assert_send_sync,
        database::Error::{Redb, UnsupportedVersion},
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
            (
                Error::Storage(NoSuchKey(object_key("x"))),
                "no such object: `x`",
            ),
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
        let db_err: Error = Redb(DatabaseAlreadyOpen.into()).into();
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

    #[test]
    fn invalid_value_and_root_errors_project_onto_io() {
        let report = Probe {
            compact_threshold_percent: 0,
        }
        .validate()
        .unwrap_err();
        let core: storage::Error = invalid_value(report).into();
        assert!(matches!(core, Io(_)));
        let core: storage::Error = root_not_directory("/x").into();
        assert!(matches!(core, Io(_)));
    }

    #[test]
    fn an_invariant_failure_displays_generically_and_names_itself_in_debug() {
        // D5: the broken invariant is named for `Debug` and the log; the
        // wire body — the storage projection's own display — stays generic,
        // so no internal symbol reaches a response.
        let err = Error::Invariant("the etag tee's promise is filled when the body drains");
        assert_eq!(err.to_string(), "internal error");
        assert!(
            format!("{err:?}").contains("the etag tee's promise is filled"),
            "{err:?}"
        );
        let core: storage::Error = err.into();
        assert_eq!(core.to_string(), "I/O error: internal error");
    }

    #[test]
    fn a_read_only_database_refusal_keeps_its_marker() {
        // The state database's read-only arm is the same refusal the engine
        // projects, so the conversion must not flatten it into a generic I/O
        // fault: the plane answers the contract's `403` by asking this marker.
        let err: Error = database::Error::Redb(_store_redb::Error::ReadOnly).into();
        assert!(err.is_read_only(), "the fs error knows it is read-only");
        let core: storage::Error = err.into();
        assert!(core.is_read_only(), "and the conversion keeps it: {core:?}");
    }

    #[test]
    fn database_io_errors_unwrap_to_io() {
        // A database I/O failure surfaces as the public `Io` — everything
        // else stays nested under `Database`.
        let err: Error = database::Error::Io(IoError::other("boom")).into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn error_serves_as_a_std_error_trait_object() {
        // The pipeline runtimes require `AsRef<dyn StdError + Send + Sync>`
        // (R8) — the failure must stay typed through the object.
        let err = Error::InvalidPath("a/../b".into());
        let obj: &(dyn StdError + Send + Sync) = err.as_ref();
        assert_eq!(obj.to_string(), "invalid path: a/../b");
    }
}
