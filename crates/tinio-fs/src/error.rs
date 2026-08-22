//! Filesystem backend errors (task T019).
//!
//! The backend error type of `tinio-fs`: I/O failures and path-mapping
//! violations. It converts into the contract error
//! ([`tinio_core::storage::Error`]) so the S3 mapping layer and the
//! conformance harness can translate any backend failure.

use std::{io, path::PathBuf};

use tinio_core::storage;

/// A filesystem backend failure.
///
/// The conversion into the contract error ([`From<Error> for
/// tinio_core::storage::Error`]) maps I/O failures transparently and path
/// violations onto [`tinio_core::storage::Error::InvalidKey`].
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage;
/// use tinio_core::storage::Error::*;
/// use tinio_fs::Error;
///
/// let err = Error::InvalidPath("traversal".into());
/// let core: storage::Error = err.into();
/// assert!(matches!(core, InvalidKey(_)));
/// ```
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
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(e) => Self::io(e),
            Error::InvalidPath(p) => Self::invalid_key(p.to_string_lossy().into_owned()),
            Error::Storage(e) => e,
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
        let cases = [
            (
                Error::Io(io::Error::other("boom")),
                "I/O error: boom".into(),
            ),
            (
                Error::InvalidPath("a/../b".into()),
                format!("invalid path: {}", PathBuf::from("a/../b").display()),
            ),
            (
                Error::Storage(NoSuchKey("x".into())),
                "no such object: `x`".into(),
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
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

        let passthrough = Error::Storage(NotEmpty("data".into()));
        let core: storage::Error = passthrough.into();
        assert!(matches!(core, NotEmpty(_)));
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
    }
}
