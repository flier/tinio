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
//! let err: Error = tinio_fs::BackendError::InvalidPath("traversal".into());
//! let core: storage::Error = err.into();
//! assert!(matches!(core, storage::Error::InvalidKey(_)));
//! ```

use std::{io, path::PathBuf};

use tinio_core::storage;

/// Alias kept for existing call sites and doctests.
pub type BackendError = Error;

/// A filesystem backend failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A filesystem I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A path-mapping violation (traversal, platform charset, reserved
    /// segments) — rejected before any filesystem access.
    #[error("invalid path: {}", .0.display())]
    InvalidPath(PathBuf),
    /// A contract-domain error passed through (key/bucket validation,
    /// not-found conditions).
    #[error("{0}")]
    Storage(#[from] storage::Error),
    /// A JSON serialization failure (meta entries, buckets.json).
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    /// The storage root exists but is not a directory.
    #[error("storage root is not a directory: {}", .0.display())]
    RootNotDirectory(PathBuf),
    /// A private state file contains invalid JSON.
    #[error("corrupt state file `{}`: {source}", .path.display())]
    CorruptStateFile {
        /// The unreadable file.
        path: PathBuf,
        /// The JSON parse failure.
        #[source]
        source: serde_json::Error,
    },
    /// A private state file has an unsupported format version.
    #[error(
        "unsupported {} version {found} (expected {expected})",
        .path.display()
    )]
    UnsupportedStateVersion {
        /// The state file path.
        path: PathBuf,
        /// The version read from disk.
        found: u32,
        /// The supported version.
        expected: u32,
    },
}

/// A path-mapping violation (rejected before any filesystem access).
#[inline]
pub(crate) fn invalid_path(path: impl Into<PathBuf>) -> Error {
    Error::InvalidPath(path.into())
}

/// The storage root exists but is not a directory.
#[inline]
pub(crate) fn root_not_directory(path: impl Into<PathBuf>) -> Error {
    Error::RootNotDirectory(path.into())
}

/// A private state file contains invalid JSON.
#[inline]
pub(crate) fn corrupt_state_file(path: impl Into<PathBuf>, source: serde_json::Error) -> Error {
    Error::CorruptStateFile {
        path: path.into(),
        source,
    }
}

/// A private state file has an unsupported format version.
#[inline]
pub(crate) fn unsupported_state_version(
    path: impl Into<PathBuf>,
    found: u32,
    expected: u32,
) -> Error {
    Error::UnsupportedStateVersion {
        path: path.into(),
        found,
        expected,
    }
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(e) => storage::io(e),
            Error::InvalidPath(p) => storage::invalid_key(p.to_string_lossy().into_owned()),
            Error::Storage(e) => e,
            Error::Json(e) => storage::io(io::Error::other(e)),
            Error::RootNotDirectory(_)
            | Error::CorruptStateFile { .. }
            | Error::UnsupportedStateVersion { .. } => storage::io(io::Error::other(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinio_core::storage::Error::*;
    use tinio_core::testing::assert_send_sync;

    #[test]
    fn displays_variants() {
        let path = PathBuf::from("/data/root");
        let cases: [(Error, &str); 6] = [
            (Error::Io(io::Error::other("boom")), "I/O error: boom"),
            (Error::InvalidPath("a/../b".into()), "invalid path: a/../b"),
            (Error::Storage(NoSuchKey("x".into())), "no such object: `x`"),
            (
                root_not_directory(&path),
                "storage root is not a directory: /data/root",
            ),
            (
                corrupt_state_file(&path, serde_json::from_str::<()>("{").unwrap_err()),
                "corrupt state file `/data/root`:",
            ),
            (
                unsupported_state_version(&path, 9, 1),
                "unsupported /data/root version 9 (expected 1)",
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
        let io_err = Error::Io(io::Error::other("disk full"));
        let core: storage::Error = io_err.into();
        assert!(matches!(core, Io(_)));

        let path_err = Error::InvalidPath("traversal".into());
        let core: storage::Error = path_err.into();
        assert!(matches!(core, InvalidKey(_)));
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
    }
}
