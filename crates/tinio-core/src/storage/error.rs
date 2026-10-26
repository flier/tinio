//! Backend-agnostic storage failures.

use std::{io, num::ParseIntError};

use super::range::ByteRange;
use crate::{bucket, etag, object, object_lock};

/// A backend-agnostic storage failure.
///
/// All backend operations report failures with this type. It is split into
/// two not-found variants on purpose: the S3 mapping layer must distinguish
/// a missing bucket (`NoSuchBucket`) from a missing object (`NoSuchKey`),
/// and backends can naturally tell them apart (e.g. by which path component
/// is absent).
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// use tinio_core::{
///     bucket,
///     storage::{self, Error::*},
/// };
///
/// let err = NoSuchBucket(tinio_util::testing::bucket_name("data"));
/// assert_eq!(err.to_string(), "no such bucket: `data`");
///
/// // I/O errors convert into the domain error transparently.
/// let io_err: storage::Error = io::Error::new(io::ErrorKind::NotFound, "gone").into();
/// assert!(matches!(io_err, Io(_)));
/// ```

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The referenced bucket does not exist.
    #[error("no such bucket: `{0}`")]
    NoSuchBucket(bucket::Name),
    /// The referenced object (key) does not exist.
    #[error("no such object: `{0}`")]
    NoSuchKey(object::Key),
    /// The referenced multipart upload does not exist.
    #[error("no such multipart upload: `{0}`")]
    NoSuchUpload(String),
    /// The referenced object version does not exist: a well-formed version
    /// identifier that names no version of the key (the wire's `404
    /// NoSuchVersion`).
    #[error("no such version: `{0}`")]
    NoSuchVersion(String),
    /// The entity already exists (e.g. bucket creation on an existing name).
    #[error("already exists: `{0}`")]
    AlreadyExists(bucket::Name),
    /// The bucket still contains objects and cannot be deleted.
    #[error("bucket is not empty: `{0}`")]
    NotEmpty(bucket::Name),
    /// The object key violates the universal validation rules (traversal,
    /// absolute path, control characters — FR-006). The payload is the
    /// rejected input — it cannot be [`object::Key`].
    #[error("invalid key: `{0}`")]
    InvalidKey(String),
    /// The bucket name violates the S3 naming rules (FR-012). The payload
    /// is the rejected input — it cannot be [`bucket::Name`].
    #[error("invalid bucket name: `{0}`")]
    InvalidBucketName(String),
    /// The version identifier (or versioning state) violates the wire rules —
    /// empty, or longer than the 1024-byte AWS cap. The payload is the
    /// rejected input — it cannot be a [`crate::version::Id`].
    #[error("invalid version: `{0}`")]
    InvalidVersion(String),
    /// Stored or wire-format ETag could not be parsed.
    #[error("invalid etag: {0}")]
    InvalidETag(#[from] etag::Error),
    /// Stored checksum algorithm or type wire name could not be parsed.
    #[error("invalid checksum: `{0}`")]
    InvalidChecksum(String),
    /// Part number outside `1..=10000`.
    #[error("invalid part number: {0}")]
    InvalidPartNumber(u32),
    /// Complete listed a part that is missing, out of order, or whose ETag
    /// does not match the stored part.
    #[error("invalid part: {0}")]
    InvalidPart(u32),
    /// Complete called with no parts uploaded.
    #[error("no parts uploaded")]
    NoParts,
    /// A non-final multipart part is below the S3 5 MiB minimum
    /// (EntityTooSmall — enforced authoritatively by the storage
    /// backends at complete, against the part state the commit reads;
    /// the S3 layer additionally pre-checks its own listing snapshot).
    #[error(
        "multipart part {part_number} is {actual} bytes, below the {min_bytes}-byte minimum for non-final parts"
    )]
    PartTooSmall {
        /// The offending part number.
        part_number: u32,
        /// The enforced minimum for non-final parts.
        min_bytes: u64,
        /// The actual stored size of the part.
        actual: u64,
    },
    /// The number of in-progress multipart uploads exceeds the configured
    /// limit (mapped to `SlowDown` at the S3 layer).
    #[error("too many in-progress multipart uploads (limit: {limit})")]
    TooManyMultipartUploads {
        /// The configured concurrent-upload limit.
        limit: u32,
    },
    /// The object (or multipart part) exceeds the backend's configured
    /// size limit (mapped to `EntityTooLarge` at the S3 layer).
    #[error("entity too large: {size} bytes exceeds the {limit}-byte limit")]
    EntityTooLarge {
        /// The actual size of the entity.
        size: u64,
        /// The configured limit.
        limit: u64,
    },
    /// A multipart part-key suffix is not a `u32`.
    #[error("invalid part key: {0}")]
    InvalidPartKey(#[from] ParseIntError),
    /// A byte range cannot be satisfied (mapped to `InvalidRange`, HTTP 416).
    #[error("invalid byte range: requested {range:?} on object of {size} bytes")]
    InvalidRange {
        /// The requested range.
        range: ByteRange,
        /// Size of the object in bytes.
        size: u64,
    },
    /// The operation is refused (reserved `.tinio` segment — FR-020;
    /// read-only mode — FR-023). The payload names the operation's own
    /// subject: the object key for a key-scoped operation, the bucket for a
    /// bucket-scoped one.
    #[error("access denied: `{0}`")]
    AccessDenied(Denied),
    /// An Object Lock refused a permanent delete or a lock write (OL8, OL11).
    /// The payload is the non-bypassable class; the operator-facing detail
    /// stays in the store's log, never on the wire.
    #[error("object lock refused the operation: {0}")]
    Locked(object_lock::RefusalReason),
    /// The bucket's versioning state refuses the operation (OL13): a
    /// lock-enabled bucket may not be suspended, and a per-object lock write
    /// requires versioning `Enabled`. The payload is the bucket whose state
    /// refused.
    #[error("invalid bucket state: `{0}`")]
    InvalidBucketState(bucket::Name),
    /// The backend has no storage tiers, so a transition cannot run
    /// (mapped to `NotImplemented` at the S3 layer). The other tier hooks
    /// still answer: the capability is `false`, a class read is
    /// `STANDARD`, and the metrics scan is the plain listing.
    #[error("storage tiers are not supported by this backend")]
    NoTiers,
    /// The request asks a backend for a state it cannot reach — a tier
    /// transition to a class the backend refuses (mapped to `InvalidRequest`
    /// at the S3 layer). The payload is a fixed operator-facing detail, never
    /// a wire value.
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    /// A stored row violates an invariant this contract guarantees — the
    /// undecodable lock row a fail-closed read or delete refuses rather than
    /// guessing an unlocked version. The payload is the operator-facing
    /// detail, never a wire value.
    #[error("corrupt stored state: {0}")]
    Corrupt(&'static str),
    /// A backend I/O failure; the underlying error is preserved.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

impl Error {
    /// Whether this failure is the **read-only refusal** of a non-writable
    /// store (FR-023): a write was asked of a handle opened read-only.
    ///
    /// The read paths that recompute or record a row they could not hit ask
    /// this before deciding the work "is simply not written back" — the
    /// store's refusal spells itself as an I/O failure whose kind is
    /// [`io::ErrorKind::ReadOnlyFilesystem`] (the workspace's one spelling of
    /// read-only), so no caller hand-rolls a match over the inner error.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::io;
    ///
    /// use tinio_core::storage;
    ///
    /// let refused = storage::Error::from(io::Error::from(io::ErrorKind::ReadOnlyFilesystem));
    /// assert!(refused.is_read_only());
    /// assert!(
    ///     !storage::Error::from(io::Error::other("boom")).is_read_only(),
    ///     "a real I/O fault is never read as the read-only refusal"
    /// );
    /// ```
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::Io(err) if err.kind() == io::ErrorKind::ReadOnlyFilesystem)
    }
}

/// The subject of an [`Error::AccessDenied`] refusal: the object key a
/// key-scoped operation named, or the bucket a bucket-scoped one named. A
/// bucket-scoped operation has no key, and fabricating one (the reserved
/// `.tinio` segment, say) would make the payload a lie about what was
/// refused — so the payload is shaped like the subject.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, object, storage};
///
/// let key = object::key("notes.txt").unwrap();
/// let refused = storage::access_denied(&key);
/// assert_eq!(refused.to_string(), "access denied: `notes.txt`");
///
/// let bucket = bucket::name("data").unwrap();
/// let refused = storage::access_denied_bucket(&bucket);
/// assert_eq!(refused.to_string(), "access denied: `data`");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, derive_more::From)]
pub enum Denied {
    /// A key-scoped refusal, naming the object key.
    #[display("{_0}")]
    Key(object::Key),
    /// A bucket-scoped refusal, naming the bucket.
    #[display("{_0}")]
    Bucket(bucket::Name),
}

/// The referenced bucket does not exist.
#[inline]
pub fn no_such_bucket(name: &bucket::Name) -> Error {
    Error::NoSuchBucket(name.clone())
}

/// The referenced object (key) does not exist.
#[inline]
pub fn no_such_key(key: &object::Key) -> Error {
    Error::NoSuchKey(key.clone())
}

/// The referenced multipart upload does not exist.
#[inline]
pub fn no_such_upload(upload_id: &str) -> Error {
    Error::NoSuchUpload(upload_id.into())
}

/// The referenced object version does not exist (a well-formed identifier
/// naming no version).
#[inline]
pub fn no_such_version(version_id: impl Into<String>) -> Error {
    Error::NoSuchVersion(version_id.into())
}

/// The entity already exists (e.g. bucket creation on an existing name).
#[inline]
pub fn already_exists(name: &bucket::Name) -> Error {
    Error::AlreadyExists(name.clone())
}

/// The bucket still contains objects and cannot be deleted.
#[inline]
pub fn not_empty(name: &bucket::Name) -> Error {
    Error::NotEmpty(name.clone())
}

/// Invalid object key (rejected input — it cannot be [`object::Key`]).
#[inline]
pub fn invalid_key(raw: String) -> Error {
    Error::InvalidKey(raw)
}

/// Invalid bucket name (rejected input — it cannot be [`bucket::Name`]).
#[inline]
pub fn invalid_bucket_name(raw: String) -> Error {
    Error::InvalidBucketName(raw)
}

/// Invalid version identifier or versioning state (rejected input — it cannot
/// be a [`crate::version::Id`] or [`crate::version::State`]).
#[inline]
pub fn invalid_version(raw: impl Into<String>) -> Error {
    Error::InvalidVersion(raw.into())
}

/// Stored or wire-format ETag could not be parsed.
#[inline]
pub fn invalid_etag(err: etag::Error) -> Error {
    Error::InvalidETag(err)
}

/// Stored checksum algorithm or type wire name could not be parsed.
#[inline]
pub fn invalid_checksum(raw: impl Into<String>) -> Error {
    Error::InvalidChecksum(raw.into())
}

/// Part number outside `1..=10000`.
#[inline]
pub fn invalid_part_number(part_number: u32) -> Error {
    Error::InvalidPartNumber(part_number)
}

/// Complete listed a missing, out-of-order, or ETag-mismatched part.
#[inline]
pub fn invalid_part(part_number: u32) -> Error {
    Error::InvalidPart(part_number)
}

/// Complete called with no parts uploaded.
#[inline]
pub fn no_parts() -> Error {
    Error::NoParts
}

/// A non-final multipart part below the S3 5 MiB minimum.
#[inline]
pub fn part_too_small(part_number: u32, min_bytes: u64, actual: u64) -> Error {
    Error::PartTooSmall {
        part_number,
        min_bytes,
        actual,
    }
}

/// The concurrent in-progress multipart upload limit was reached.
#[inline]
pub fn too_many_uploads(limit: u32) -> Error {
    Error::TooManyMultipartUploads { limit }
}

/// The entity exceeds the backend's configured size limit.
#[inline]
pub fn entity_too_large(size: u64, limit: u64) -> Error {
    Error::EntityTooLarge { size, limit }
}

/// A multipart part-key suffix is not a `u32`.
#[inline]
pub fn invalid_part_key(err: ParseIntError) -> Error {
    Error::InvalidPartKey(err)
}

/// A byte range cannot be satisfied (the S3 mapping layer answers 416).
#[inline]
pub fn invalid_range(range: ByteRange, size: u64) -> Error {
    Error::InvalidRange { range, size }
}

/// The operation is refused (reserved `.tinio` segment or read-only mode).
#[inline]
pub fn access_denied(key: &object::Key) -> Error {
    Error::AccessDenied(Denied::Key(key.clone()))
}

/// The bucket-scoped twin of [`access_denied`]: the refusal names the bucket
/// the operation named, never a fabricated key.
#[inline]
pub fn access_denied_bucket(name: &bucket::Name) -> Error {
    Error::AccessDenied(Denied::Bucket(name.clone()))
}

/// An Object Lock refusal, carrying the class the S3 surface maps.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{object_lock::RefusalReason, storage};
///
/// let refused = storage::locked(RefusalReason::Compliance);
/// assert!(matches!(
///     refused,
///     storage::Error::Locked(RefusalReason::Compliance)
/// ));
/// ```
#[inline]
pub fn locked(reason: object_lock::RefusalReason) -> Error {
    Error::Locked(reason)
}

/// The versioning-state refusal, naming the bucket whose state refused.
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage;
///
/// let bucket = tinio_util::testing::bucket_name("data");
/// assert!(matches!(
///     storage::invalid_bucket_state(&bucket),
///     storage::Error::InvalidBucketState(_)
/// ));
/// ```
#[inline]
pub fn invalid_bucket_state(name: &bucket::Name) -> Error {
    Error::InvalidBucketState(name.clone())
}

/// A stored row that violates a contract invariant (an undecodable lock row).
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage;
///
/// assert_eq!(
///     storage::corrupt("the lock row has no legal hold").to_string(),
///     "corrupt stored state: the lock row has no legal hold"
/// );
/// ```
#[inline]
pub fn corrupt(detail: &'static str) -> Error {
    Error::Corrupt(detail)
}

/// A backend I/O failure; the underlying error is preserved.
#[inline]
pub fn io(err: io::Error) -> Error {
    Error::Io(err)
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        io::{Error as IoError, ErrorKind},
    };

    use super::*;
    use crate::{_util::testing::assert_send_sync, etag::Error::*, object_lock::RefusalReason};

    #[test]
    fn displays_variants() {
        let cases = [
            (
                Error::NoSuchBucket(bucket::name("my-bucket").expect("a valid bucket name")),
                "no such bucket: `my-bucket`",
            ),
            (
                Error::NoSuchKey(object::key("dir/file.txt").expect("a valid object key")),
                "no such object: `dir/file.txt`",
            ),
            (
                Error::NoSuchUpload("abc-123".into()),
                "no such multipart upload: `abc-123`",
            ),
            (
                Error::NoSuchVersion("abc-123".into()),
                "no such version: `abc-123`",
            ),
            (
                Error::InvalidVersion("bad id".into()),
                "invalid version: `bad id`",
            ),
            (
                Error::AlreadyExists(bucket::name("my-bucket").expect("a valid bucket name")),
                "already exists: `my-bucket`",
            ),
            (
                Error::NotEmpty(bucket::name("my-bucket").expect("a valid bucket name")),
                "bucket is not empty: `my-bucket`",
            ),
            (
                Error::InvalidKey("../evil".into()),
                "invalid key: `../evil`",
            ),
            (
                Error::InvalidBucketName("Bad_Name".into()),
                "invalid bucket name: `Bad_Name`",
            ),
            (
                Error::InvalidETag(InvalidFormat),
                "invalid etag: invalid ETag format",
            ),
            (
                Error::InvalidChecksum("BLAKE3".into()),
                "invalid checksum: `BLAKE3`",
            ),
            (Error::InvalidPartNumber(0), "invalid part number: 0"),
            (Error::InvalidPart(2), "invalid part: 2"),
            (Error::NoParts, "no parts uploaded"),
            (
                Error::InvalidRange {
                    range: ByteRange::From(10),
                    size: 5,
                },
                "invalid byte range: requested From(10) on object of 5 bytes",
            ),
            (
                Error::AccessDenied(Denied::Key(
                    object::key("a/.tinio/b").expect("a valid object key"),
                )),
                "access denied: `a/.tinio/b`",
            ),
            (
                Error::Locked(RefusalReason::Governance),
                "object lock refused the operation: governance",
            ),
            (
                Error::InvalidBucketState(bucket::name("my-bucket").expect("a valid bucket name")),
                "invalid bucket state: `my-bucket`",
            ),
            (
                Error::Corrupt("the stored lock row has no legal hold"),
                "corrupt stored state: the stored lock row has no legal hold",
            ),
            (
                Error::Io(IoError::from(ErrorKind::NotFound)),
                "I/O error: entity not found",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }

        let src = "x".parse::<u32>().unwrap_err();
        assert_eq!(
            Error::InvalidPartKey(src.clone()).to_string(),
            format!("invalid part key: {src}")
        );
    }

    #[test]
    fn original_errors_convert_with_from() {
        let err = Error::from(IoError::other("boom"));
        assert!(matches!(err, Error::Io(_)));

        let err = Error::from(InvalidFormat);
        assert!(matches!(err, Error::InvalidETag(_)));

        let src = "x".parse::<u32>().unwrap_err();
        let err = Error::from(src.clone());
        assert!(matches!(err, Error::InvalidPartKey(_)));
        assert_eq!(
            err.source().map(ToString::to_string).as_deref(),
            Some(src.to_string().as_str())
        );
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
    }

    #[test]
    fn error_constructor_helpers() {
        assert!(matches!(
            invalid_key("../evil".into()),
            Error::InvalidKey(_)
        ));
        assert!(matches!(
            invalid_bucket_name("Bad_Name".into()),
            Error::InvalidBucketName(_)
        ));
        assert!(matches!(
            invalid_version("bad id"),
            Error::InvalidVersion(_)
        ));
        assert!(matches!(
            no_such_version("abc-123"),
            Error::NoSuchVersion(_)
        ));
        assert!(matches!(invalid_etag(InvalidFormat), Error::InvalidETag(_)));
        assert!(matches!(
            invalid_checksum("BLAKE3"),
            Error::InvalidChecksum(_)
        ));
        let err = part_too_small(2, 5 * 1024 * 1024, 100);
        match err {
            Error::PartTooSmall {
                part_number,
                min_bytes,
                actual,
            } => assert_eq!((part_number, min_bytes, actual), (2, 5 * 1024 * 1024, 100)),
            other => panic!("expected PartTooSmall, got {other:?}"),
        }
    }

    #[test]
    fn error_helpers_build_their_own_variant() {
        // The constructors are the crate's one spelling per failure — the
        // backends call these, never the variants — and the variant IS
        // what the S3 layer maps to a status and an error code (404
        // `NoSuchBucket` vs 409 `BucketAlreadyOwnedByYou` vs 416
        // `InvalidRange`). A helper that built its neighbour's variant,
        // or swapped a two-number payload, would answer the wrong code
        // with every other test still green: each variant renders its
        // own message, so the rendering pins both.
        let bucket = bucket::name("data").unwrap();
        let key = object::key("dir/file.txt").unwrap();

        let cases = [
            (no_such_bucket(&bucket), "no such bucket: `data`"),
            (no_such_key(&key), "no such object: `dir/file.txt`"),
            (
                no_such_upload("abc-123"),
                "no such multipart upload: `abc-123`",
            ),
            (no_such_version("abc-123"), "no such version: `abc-123`"),
            (invalid_version("bad id"), "invalid version: `bad id`"),
            (already_exists(&bucket), "already exists: `data`"),
            (not_empty(&bucket), "bucket is not empty: `data`"),
            (invalid_part_number(0), "invalid part number: 0"),
            (invalid_part(3), "invalid part: 3"),
            (no_parts(), "no parts uploaded"),
            (
                too_many_uploads(4),
                "too many in-progress multipart uploads (limit: 4)",
            ),
            (
                entity_too_large(7, 6),
                "entity too large: 7 bytes exceeds the 6-byte limit",
            ),
            (access_denied(&key), "access denied: `dir/file.txt`"),
            (
                io(IoError::from(ErrorKind::NotFound)),
                "I/O error: entity not found",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }

        let src = "x".parse::<u32>().unwrap_err();
        assert_eq!(
            invalid_part_key(src.clone()).to_string(),
            format!("invalid part key: {src}")
        );
    }
}
