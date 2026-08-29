use s3s::{S3Error, s3_error};
use tinio_core::storage::Error as StorageError;

/// Map a backend error (any `S::Error`, which converts into the contract
/// error) onto its S3 error code (FR-005).
pub(crate) fn map_backend_error<E: Into<StorageError>>(err: E) -> S3Error {
    match err.into() {
        StorageError::NoSuchBucket(_) => s3_error!(NoSuchBucket),
        StorageError::NoSuchKey(_) => s3_error!(NoSuchKey),
        StorageError::NoSuchUpload(id) => s3_error!(NoSuchUpload, "no such upload: {id}"),
        // A duplicate create on a locally-owned bucket answers
        // `BucketAlreadyOwnedByYou` (AWS/MinIO semantics) — clients such as
        // rclone treat this as the idempotent-create case and continue.
        StorageError::AlreadyExists(_) => s3_error!(BucketAlreadyOwnedByYou),
        StorageError::NotEmpty(_) => s3_error!(BucketNotEmpty),
        StorageError::InvalidKey(key) => s3_error!(InvalidArgument, "invalid object key: {key}"),
        StorageError::InvalidBucketName(name) => {
            s3_error!(InvalidBucketName, "invalid bucket name: {name}")
        }
        StorageError::InvalidETag(_) => s3_error!(InvalidArgument, "invalid ETag"),
        StorageError::InvalidPartNumber(n) => {
            s3_error!(InvalidArgument, "invalid part number: {n}")
        }
        StorageError::InvalidPart(n) => s3_error!(InvalidPart, "invalid part: {n}"),
        StorageError::NoParts => s3_error!(InvalidRequest, "no parts uploaded"),
        StorageError::PartTooSmall {
            part_number,
            min_bytes,
            actual,
        } => s3_error!(
            EntityTooSmall,
            "part {part_number} is {actual} bytes, below the {min_bytes}-byte minimum for non-final parts"
        ),
        StorageError::TooManyMultipartUploads { limit } => s3_error!(
            SlowDown,
            "too many in-progress multipart uploads (limit: {limit}); retry later"
        ),
        StorageError::EntityTooLarge { size, limit } => s3_error!(
            EntityTooLarge,
            "entity is {size} bytes, exceeding the {limit}-byte limit"
        ),
        StorageError::InvalidPartKey(_) => s3_error!(InvalidArgument, "invalid part key"),
        StorageError::InvalidRange { .. } => s3_error!(InvalidRange),
        StorageError::AccessDenied(_) => s3_error!(AccessDenied),
        StorageError::Io(err) => s3_error!(InternalError, "storage I/O error: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use s3s::S3ErrorCode;
    use tinio_core::storage::{ByteRange, Error as StorageError};
    use tinio_core::{ETag, bucket, object};

    use super::*;

    fn s3_code(err: StorageError) -> S3ErrorCode {
        map_backend_error(err).code().clone()
    }

    #[test]
    fn maps_every_storage_error_to_its_s3_code() {
        let b = bucket::name("data").unwrap();
        let k = object::key("k.bin").unwrap();
        let cases = [
            (
                StorageError::NoSuchBucket(b.clone()),
                S3ErrorCode::NoSuchBucket,
            ),
            (StorageError::NoSuchKey(k.clone()), S3ErrorCode::NoSuchKey),
            (
                StorageError::NoSuchUpload("u-1".into()),
                S3ErrorCode::NoSuchUpload,
            ),
            (
                StorageError::AlreadyExists(b.clone()),
                S3ErrorCode::BucketAlreadyOwnedByYou,
            ),
            (
                StorageError::NotEmpty(b.clone()),
                S3ErrorCode::BucketNotEmpty,
            ),
            (
                StorageError::InvalidKey("a/../b".into()),
                S3ErrorCode::InvalidArgument,
            ),
            (
                StorageError::InvalidBucketName("BAD_NAME".into()),
                S3ErrorCode::InvalidBucketName,
            ),
            (
                StorageError::InvalidETag("x".parse::<ETag>().unwrap_err()),
                S3ErrorCode::InvalidArgument,
            ),
            (
                StorageError::InvalidPartNumber(10001),
                S3ErrorCode::InvalidArgument,
            ),
            (StorageError::InvalidPart(2), S3ErrorCode::InvalidPart),
            (StorageError::NoParts, S3ErrorCode::InvalidRequest),
            (
                StorageError::InvalidPartKey("abc".parse::<u32>().unwrap_err()),
                S3ErrorCode::InvalidArgument,
            ),
            (
                StorageError::InvalidRange {
                    range: ByteRange::From(10),
                    size: 5,
                },
                S3ErrorCode::InvalidRange,
            ),
            (StorageError::AccessDenied(k), S3ErrorCode::AccessDenied),
            (
                StorageError::Io(io::Error::other("boom")),
                S3ErrorCode::InternalError,
            ),
        ];
        for (storage_err, expected) in cases {
            assert_eq!(s3_code(storage_err), expected);
        }
    }
}
