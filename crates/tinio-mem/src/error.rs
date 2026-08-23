//! In-memory backend errors.
//!
//! [`Error`] is a superset of [`storage::Error`]: contract failures wrap
//! that type; redb failures wrap [`DatabaseError`]. `#[from]` converts
//! [`storage::Error`] automatically. Projection onto the contract unwraps
//! `Storage` and maps [`DatabaseError`] onto [`storage::Error::Io`].

use std::{io, num::ParseIntError};

use tinio_core::{bucket, etag, object, storage, storage::Error::*};

/// An in-memory backend failure.
///
/// Contract-domain failures are [`storage::Error`] (via [`Error::Storage`]).
/// Redb failures are [`DatabaseError`] (via [`Error::Database`]).
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage;
/// use tinio_core::storage::Error::*;
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

/// A redb failure: open, transaction, table, storage, or commit.
///
/// # Examples
///
/// ```rust
/// use tinio_mem::{DatabaseError, Error};
///
/// let err = Error::from(redb::StorageError::ValueTooLarge(1));
/// assert!(matches!(err, Error::Database(DatabaseError::Storage(_))));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
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
    /// Commit failed.
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
}

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

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Self {
        invalid_part_key(err)
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(err: redb::DatabaseError) -> Self {
        database_open(err)
    }
}

impl From<redb::TransactionError> for Error {
    fn from(err: redb::TransactionError) -> Self {
        database_transaction(err)
    }
}

impl From<redb::TableError> for Error {
    fn from(err: redb::TableError) -> Self {
        database_table(err)
    }
}

impl From<redb::StorageError> for Error {
    fn from(err: redb::StorageError) -> Self {
        database_storage(err)
    }
}

impl From<redb::CommitError> for Error {
    fn from(err: redb::CommitError) -> Self {
        database_commit(err)
    }
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(e) => e,
            Error::Database(e) => Io(io::Error::other(e)),
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

/// A multipart part-key suffix is not a `u32`.
#[inline]
pub(crate) fn invalid_part_key(err: ParseIntError) -> Error {
    Error::Storage(storage::invalid_part_key(err))
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

/// Database open/create failed.
#[inline]
pub(crate) fn database_open(err: redb::DatabaseError) -> Error {
    Error::Database(DatabaseError::Open(err))
}

/// A transaction failed.
#[inline]
pub(crate) fn database_transaction(err: redb::TransactionError) -> Error {
    Error::Database(DatabaseError::Transaction(err))
}

/// Opening a table failed.
#[inline]
pub(crate) fn database_table(err: redb::TableError) -> Error {
    Error::Database(DatabaseError::Table(err))
}

/// A get/insert/range failed.
#[inline]
pub(crate) fn database_storage(err: redb::StorageError) -> Error {
    Error::Database(DatabaseError::Storage(err))
}

/// Commit failed.
#[inline]
pub(crate) fn database_commit(err: redb::CommitError) -> Error {
    Error::Database(DatabaseError::Commit(err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinio_core::testing::assert_send_sync;

    #[test]
    fn displays_wrapped_contract_errors() {
        let cases: [(storage::Error, &str); 4] = [
            (NoSuchBucket("data".into()), "no such bucket: `data`"),
            (NoSuchKey("a.txt".into()), "no such object: `a.txt`"),
            (
                InvalidRange {
                    range: storage::ByteRange::From(10),
                    size: 10,
                },
                "invalid byte range: requested From(10) on object of 10 bytes",
            ),
            (Io(io::Error::other("boom")), "I/O error: boom"),
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

        let err: Error = etag::Error::InvalidFormat.into();
        assert!(matches!(err, Error::Storage(InvalidETag(_))));
    }

    #[test]
    fn extras_project_onto_contract_io() {
        let core: storage::Error = Error::from(redb::StorageError::ValueTooLarge(1)).into();
        assert!(matches!(core, Io(_)));
    }

    #[test]
    fn redb_errors_wrap_as_database() {
        let err = Error::from(redb::StorageError::ValueTooLarge(99));
        assert!(
            matches!(err, Error::Database(DatabaseError::Storage(_))),
            "{err}"
        );
        assert!(err.to_string().starts_with("storage error:"));
    }

    #[test]
    fn parse_int_error_funnels_through_storage() {
        let src = "x".parse::<u32>().unwrap_err();
        let err = Error::from(src.clone());
        assert!(matches!(err, Error::Storage(InvalidPartKey(_))));
        assert_eq!(err.to_string(), format!("invalid part key: {src}"));
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
        let err = Error::from(io::Error::other("boom"));
        assert!(matches!(err, Error::Storage(Io(_))));
        assert!(matches!(
            self::io(io::Error::other("boom")),
            Error::Storage(Io(_))
        ));
    }

    #[test]
    fn every_database_variant_wraps_and_displays() {
        let cases: [(Error, &str); 5] = [
            (
                Error::from(redb::DatabaseError::DatabaseAlreadyOpen),
                "database error:",
            ),
            (
                Error::from(redb::TransactionError::Storage(
                    redb::StorageError::ValueTooLarge(1),
                )),
                "transaction error:",
            ),
            (
                Error::from(redb::TableError::TableDoesNotExist("x".into())),
                "table error:",
            ),
            (
                Error::from(redb::StorageError::Corrupted("boom".into())),
                "storage error:",
            ),
            (
                Error::from(redb::CommitError::TransactionPoisoned),
                "commit error:",
            ),
        ];
        for (err, prefix) in cases {
            assert!(matches!(err, Error::Database(_)), "{err}");
            assert!(err.to_string().starts_with(prefix), "{err}");
        }
    }

    #[test]
    fn database_constructors_cover_every_variant() {
        let open = database_open(redb::DatabaseError::DatabaseAlreadyOpen);
        assert!(matches!(open, Error::Database(DatabaseError::Open(_))));
        let txn = database_transaction(redb::TransactionError::Storage(
            redb::StorageError::ValueTooLarge(1),
        ));
        assert!(matches!(
            txn,
            Error::Database(DatabaseError::Transaction(_))
        ));
        let table = database_table(redb::TableError::TableDoesNotExist("x".into()));
        assert!(matches!(table, Error::Database(DatabaseError::Table(_))));
        let commit = database_commit(redb::CommitError::TransactionPoisoned);
        assert!(matches!(commit, Error::Database(DatabaseError::Commit(_))));
        assert!(matches!(
            database_storage(redb::StorageError::Corrupted("boom".into())),
            Error::Database(DatabaseError::Storage(_))
        ));
    }

    #[test]
    fn every_database_variant_projects_onto_contract_io() {
        for err in [
            database_open(redb::DatabaseError::DatabaseAlreadyOpen),
            database_transaction(redb::TransactionError::Storage(
                redb::StorageError::ValueTooLarge(1),
            )),
            database_table(redb::TableError::TableDoesNotExist("x".into())),
            database_commit(redb::CommitError::TransactionPoisoned),
            database_storage(redb::StorageError::Corrupted("boom".into())),
        ] {
            let core: storage::Error = err.into();
            assert!(matches!(core, Io(_)), "{core}");
        }
    }
}
