//! In-memory backend errors.
//!
//! [`Error`] is a superset of [`storage::Error`]: contract failures wrap
//! that type; redb failures wrap [`DatabaseError`], the alias of the
//! shared five-variant redb error core in tinio-store. `#[from]` converts
//! [`storage::Error`] automatically. Projection onto the contract unwraps
//! `Storage` and maps [`DatabaseError`] onto [`Error::Io`].

use std::io::{self, Error as IoError};

use crate::_core::{bucket, etag, object, storage, storage::Error::*};

/// An in-memory backend failure.
///
/// Contract-domain failures are [`storage::Error`] (via [`Error::Storage`]).
/// Redb failures are [`DatabaseError`] (via [`Error::Database`]).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{storage, storage::Error::*};
/// use tinio_mem::Error;
///
/// let err: Error = NoSuchBucket("data".into()).into();
/// assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A contract-domain failure.
    #[error(transparent)]
    Storage(#[from] storage::Error),
    /// A redb database failure.
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

/// A redb failure: the shared five-variant mapping core
/// ([`tinio_store::error::Error`]). The alias keeps the historical name.
///
/// # Examples
///
/// ```rust
/// use redb::StorageError::ValueTooLarge;
/// use tinio_mem::{DatabaseError, Error};
///
/// let err = Error::Database(DatabaseError::from(ValueTooLarge(1)));
/// assert!(matches!(err, Error::Database(DatabaseError::Storage(_))));
/// ```
pub use crate::_store::Error as DatabaseError;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        self::io(err)
    }
}

impl From<etag::Error> for Error {
    fn from(err: etag::Error) -> Self {
        invalid_etag(err)
    }
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(e) => e,
            Error::Database(e) => Io(IoError::other(e)),
        }
    }
}

// Constructors wrapping `storage::Error` and `DatabaseError`. The contract
// mapping (payload clone-from-ref etc.) lives in `storage`'s constructors;
// these wrappers only lift it into the backend error.

/// The referenced bucket does not exist.
#[inline]
pub(crate) fn no_such_bucket(name: &bucket::Name) -> Error {
    Error::Storage(storage::no_such_bucket(name))
}

/// The referenced object (key) does not exist.
#[inline]
pub(crate) fn no_such_key(key: &object::Key) -> Error {
    Error::Storage(storage::no_such_key(key))
}

/// The referenced multipart upload does not exist.
#[inline]
pub(crate) fn no_such_upload(upload_id: &str) -> Error {
    Error::Storage(storage::no_such_upload(upload_id))
}

/// A key cannot be stored (folder markers as multipart targets).
#[inline]
pub(crate) fn invalid_key(key: String) -> Error {
    Error::Storage(storage::invalid_key(key))
}

/// The entity already exists (e.g. bucket creation on an existing name).
#[inline]
pub(crate) fn already_exists(name: &bucket::Name) -> Error {
    Error::Storage(storage::already_exists(name))
}

/// The bucket still contains objects and cannot be deleted.
#[inline]
pub(crate) fn not_empty(name: &bucket::Name) -> Error {
    Error::Storage(storage::not_empty(name))
}

/// Stored or wire-format ETag could not be parsed.
#[inline]
pub(crate) fn invalid_etag(err: etag::Error) -> Error {
    Error::Storage(storage::invalid_etag(err))
}

/// Part number outside `1..=10000`.
///
/// The storage contract validates part numbers via [`tinio_core::PartNumber`];
/// this constructor remains for `From` / tests and future defensive checks.
#[inline]
#[allow(dead_code)]
pub(crate) fn invalid_part_number(part_number: u32) -> Error {
    Error::Storage(storage::invalid_part_number(part_number))
}

/// Complete listed a missing, out-of-order, or ETag-mismatched part.
#[inline]
pub(crate) fn invalid_part(part_number: u32) -> Error {
    Error::Storage(storage::invalid_part(part_number))
}

/// Complete called with no parts uploaded.
#[inline]
pub(crate) fn no_parts() -> Error {
    Error::Storage(storage::no_parts())
}

/// The object (or multipart part) exceeds the backend's configured size
/// limit (`MemoryOptions.max_object_bytes` / `max_total_bytes`).
#[inline]
pub(crate) fn entity_too_large(size: u64, limit: u64) -> Error {
    Error::Storage(storage::entity_too_large(size, limit))
}

/// The operation is refused (reserved `.tinio` segment or read-only mode).
#[inline]
pub(crate) fn access_denied(key: &object::Key) -> Error {
    Error::Storage(storage::access_denied(key))
}

/// A backend I/O failure; the underlying error is preserved.
#[inline]
pub(crate) fn io(err: io::Error) -> Error {
    Error::Storage(storage::io(err))
}

#[cfg(test)]
mod tests {
    use redb::{
        CommitError::TransactionPoisoned,
        DatabaseError::DatabaseAlreadyOpen,
        StorageError::{Corrupted, ValueTooLarge},
        TableError::TableDoesNotExist,
        TransactionError::Storage as TxnStorage,
    };

    use super::*;
    use crate::{
        _core::{etag::Error::InvalidFormat, storage::ByteRange},
        _util::testing::assert_send_sync,
    };

    #[test]
    fn displays_wrapped_contract_errors() {
        let cases: [(storage::Error, &str); 4] = [
            (NoSuchBucket("data".into()), "no such bucket: `data`"),
            (NoSuchKey("a.txt".into()), "no such object: `a.txt`"),
            (
                InvalidRange {
                    range: ByteRange::From(10),
                    size: 10,
                },
                "invalid byte range: requested From(10) on object of 10 bytes",
            ),
            (Io(IoError::other("boom")), "I/O error: boom"),
        ];
        for (src, expected) in cases {
            let err: Error = src.into();
            assert_eq!(err.to_string(), expected);
        }
    }

    #[test]
    fn contract_errors_convert_into_backend() {
        let err: Error = NoSuchBucket("data".into()).into();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));

        let err: Error = InvalidPartNumber(0).into();
        assert!(matches!(err, Error::Storage(InvalidPartNumber(0))));

        let err: Error = NoParts.into();
        assert!(matches!(err, Error::Storage(NoParts)));

        let err: Error = InvalidFormat.into();
        assert!(matches!(err, Error::Storage(InvalidETag(_))));
    }

    #[test]
    fn extras_project_onto_contract_io() {
        let core: storage::Error = Error::Database(DatabaseError::from(ValueTooLarge(1))).into();
        assert!(matches!(core, Io(_)));
    }

    #[test]
    fn redb_errors_wrap_as_database() {
        let err = Error::Database(DatabaseError::from(ValueTooLarge(99)));
        assert!(
            matches!(err, Error::Database(DatabaseError::Storage(_))),
            "{err}"
        );
        assert!(err.to_string().starts_with("storage error:"));
    }

    #[test]
    fn constructors_wrap_payloads_from_references() {
        let bucket = bucket::name("data").unwrap();
        let key = object::key("a.txt").unwrap();
        assert!(matches!(
            already_exists(&bucket),
            Error::Storage(AlreadyExists(_))
        ));
        assert!(matches!(not_empty(&bucket), Error::Storage(NotEmpty(_))));
        assert!(matches!(
            no_such_bucket(&bucket),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(no_such_key(&key), Error::Storage(NoSuchKey(_))));
        assert!(matches!(
            no_such_upload("u"),
            Error::Storage(NoSuchUpload(_))
        ));
        assert!(matches!(
            invalid_key("dir/".into()),
            Error::Storage(InvalidKey(_))
        ));
        assert!(matches!(
            access_denied(&key),
            Error::Storage(AccessDenied(_))
        ));
        assert!(matches!(no_parts(), Error::Storage(NoParts)));
        assert!(matches!(
            invalid_part_number(0),
            Error::Storage(InvalidPartNumber(0))
        ));
        assert!(matches!(invalid_part(2), Error::Storage(InvalidPart(2))));
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
        assert_send_sync::<DatabaseError>();
    }

    #[test]
    fn io_error_lifts_into_storage_wrapper() {
        let err = Error::from(IoError::other("boom"));
        assert!(matches!(err, Error::Storage(Io(_))));
        assert!(matches!(
            self::io(IoError::other("boom")),
            Error::Storage(Io(_))
        ));
    }

    #[test]
    fn every_database_variant_wraps_and_displays() {
        let cases: [(Error, &str); 5] = [
            (
                Error::Database(DatabaseError::from(DatabaseAlreadyOpen)),
                "database error:",
            ),
            (
                Error::Database(DatabaseError::from(TxnStorage(ValueTooLarge(1)))),
                "transaction error:",
            ),
            (
                Error::Database(DatabaseError::from(TableDoesNotExist("x".into()))),
                "table error:",
            ),
            (
                Error::Database(DatabaseError::from(Corrupted("boom".into()))),
                "storage error:",
            ),
            (
                Error::Database(DatabaseError::from(TransactionPoisoned)),
                "commit error:",
            ),
        ];
        for (err, prefix) in cases {
            assert!(matches!(err, Error::Database(_)), "{err}");
            assert!(err.to_string().starts_with(prefix), "{err}");
        }
    }
}
