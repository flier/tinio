//! The engine's error type and its projection onto the contract error.

use std::io::Error as IoError;

use crate::_core::{blob, storage};

/// A block-engine failure.
///
/// Every failure reaching a caller is a contract condition: a backend error is
/// projected onto the contract error [`storage::Error`], the type the S3 layer
/// classifies, rather than boxed as an opaque failure that would lose its kind.
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// use tinio_blob::Error;
/// use tinio_core::storage;
///
/// let err: storage::Error = Error::Io(io::Error::other("the disk is gone")).into();
/// assert!(matches!(err, storage::Error::Io(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A blob reference was malformed.
    #[error(transparent)]
    Blob(#[from] blob::ParseError),
    /// A filesystem operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] IoError),
    /// A contract-domain error passed through unchanged.
    #[error(transparent)]
    Storage(#[from] storage::Error),
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(e) => e,
            Error::Blob(e) => storage::io(IoError::other(e)),
            Error::Io(e) => storage::io(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::_core::{
        object,
        storage::{self, Error as StorageError},
    };

    #[test]
    fn a_storage_error_passes_through_the_projection_unchanged() {
        let contract = storage::no_such_key(&object::Key::from("missing"));
        let expected = contract.to_string();
        let err: StorageError = Error::Storage(contract).into();
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn the_remaining_variants_project_through_io_other() {
        let malformed = blob::BlockHash::new("not hex").unwrap_err();
        let projected: StorageError = Error::Blob(malformed).into();
        let StorageError::Io(io) = projected else {
            panic!("expected an I/O projection, got {projected:?}");
        };
        assert_eq!(
            io.kind(),
            io::ErrorKind::Other,
            "a malformed reference is an opaque `Other`, not a not-found"
        );

        let failed: StorageError = Error::Io(io::Error::other("the disk is gone")).into();
        let StorageError::Io(io) = failed else {
            panic!("expected an I/O projection, got {failed:?}");
        };
        assert_eq!(io.to_string(), "the disk is gone");
    }

    #[test]
    fn a_not_found_projection_keeps_its_kind() {
        // The sweep and the S3 layer classify on the kind, so a projection
        // that wrapped the error in `io::Error::other` — invisible through the
        // `other` case above — would turn "block not there" into an opaque
        // failure.
        let missing: StorageError = Error::Io(io::Error::from(io::ErrorKind::NotFound)).into();
        let StorageError::Io(io) = missing else {
            panic!("expected an I/O projection, got {missing:?}");
        };
        assert_eq!(
            io.kind(),
            io::ErrorKind::NotFound,
            "the projection passes the kind through unchanged"
        );
    }
}
