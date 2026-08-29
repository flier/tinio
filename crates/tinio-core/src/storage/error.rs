//! Backend-agnostic storage failures.

use std::{io, num::ParseIntError};

use crate::{bucket, etag, object};

use super::range::ByteRange;

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
/// use tinio_core::storage::{self, Error::*};
///
/// let err = NoSuchBucket("data".into());
/// assert_eq!(err.to_string(), "no such bucket: `data`");
///
/// // I/O errors convert into the domain error transparently.
/// let io_err: storage::Error =
///     std::io::Error::new(std::io::ErrorKind::NotFound, "gone").into();
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
    /// Stored or wire-format ETag could not be parsed.
    #[error("invalid etag: {0}")]
    InvalidETag(#[from] etag::Error),
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
    /// (EntityTooSmall — enforced at the S3 mapping layer).
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
    /// read-only mode — FR-023).
    #[error("access denied: `{0}`")]
    AccessDenied(object::Key),
    /// A backend I/O failure; the underlying error is preserved.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
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

/// Stored or wire-format ETag could not be parsed.
#[inline]
pub fn invalid_etag(err: etag::Error) -> Error {
    Error::InvalidETag(err)
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
    Error::AccessDenied(key.clone())
}

/// A backend I/O failure; the underlying error is preserved.
#[inline]
pub fn io(err: io::Error) -> Error {
    Error::Io(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::error::Error as StdError;

    use tinio_util::testing::assert_send_sync;

    #[test]
    fn displays_variants() {
        let cases = [
            (
                Error::NoSuchBucket("my-bucket".into()),
                "no such bucket: `my-bucket`",
            ),
            (
                Error::NoSuchKey("dir/file.txt".into()),
                "no such object: `dir/file.txt`",
            ),
            (
                Error::NoSuchUpload("abc-123".into()),
                "no such multipart upload: `abc-123`",
            ),
            (
                Error::AlreadyExists("my-bucket".into()),
                "already exists: `my-bucket`",
            ),
            (
                Error::NotEmpty("my-bucket".into()),
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
                Error::InvalidETag(crate::etag::Error::InvalidFormat),
                "invalid etag: invalid ETag format",
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
                Error::AccessDenied("a/.tinio/b".into()),
                "access denied: `a/.tinio/b`",
            ),
            (
                Error::Io(io::Error::from(io::ErrorKind::NotFound)),
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
        let err = Error::from(io::Error::other("boom"));
        assert!(matches!(err, Error::Io(_)));

        let err = Error::from(crate::etag::Error::InvalidFormat);
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
            invalid_etag(crate::etag::Error::InvalidFormat),
            Error::InvalidETag(_)
        ));
    }
}
