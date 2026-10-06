use s3s::{S3Error, s3_error};

use crate::_core::storage::Error as StorageError;

/// A client-supplied value made safe to reflect into an XML error body
/// (2026-10-06 review): the version-id rules are **length-only**, so a
/// request may carry control characters, and quick-xml escapes markup but not
/// control bytes — a reflected one would make the body non-well-formed. Each
/// control character becomes `U+FFFD`.
fn printable(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_control() { '\u{FFFD}' } else { c })
        .collect()
}

/// Map a backend error (any `S::Error`, which converts into the contract
/// error) onto its S3 error code (FR-005).
pub(crate) fn map_backend_error<E: Into<StorageError>>(err: E) -> S3Error {
    match err.into() {
        StorageError::NoSuchBucket(_) => s3_error!(NoSuchBucket),
        StorageError::NoSuchKey(_) => s3_error!(NoSuchKey),
        StorageError::NoSuchUpload(id) => {
            s3_error!(NoSuchUpload, "no such upload: {}", printable(&id))
        }
        StorageError::NoSuchVersion(id) => {
            s3_error!(NoSuchVersion, "no such version: {}", printable(&id))
        }
        StorageError::InvalidVersion(raw) => {
            s3_error!(InvalidArgument, "invalid version: `{}`", printable(&raw))
        }
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
        StorageError::InvalidChecksum(raw) => {
            s3_error!(InternalError, "invalid checksum: `{raw}`")
        }
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
        // The 403-ambiguity note (backend/mod.rs module doc): a storage
        // AccessDenied is an OS-level permission denial or the
        // symlink-policy refusal — NOT an ACL decision (those answer
        // directly from the access layer). The distinction is invisible
        // on the wire; the source is logged at debug.
        StorageError::AccessDenied(subject) => {
            tracing::debug!(
                subject = subject.to_string(),
                "storage AccessDenied — OS-level permission denial or symlink-policy refusal, not an ACL decision"
            );
            s3_error!(AccessDenied)
        }
        StorageError::Io(err) => s3_error!(InternalError, "storage I/O error: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use io::Error as IoError;
    use s3s::S3ErrorCode;

    use super::*;
    use crate::_core::{
        ETag, bucket, object,
        storage::{ByteRange, Denied, Error as StorageError},
    };

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
                StorageError::NoSuchVersion("v-1".into()),
                S3ErrorCode::NoSuchVersion,
            ),
            (
                StorageError::InvalidVersion("bad id".into()),
                S3ErrorCode::InvalidArgument,
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
                StorageError::InvalidChecksum("BLAKE3".into()),
                S3ErrorCode::InternalError,
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
            (
                StorageError::AccessDenied(Denied::Key(k)),
                S3ErrorCode::AccessDenied,
            ),
            (
                StorageError::Io(IoError::other("boom")),
                S3ErrorCode::InternalError,
            ),
            (
                StorageError::PartTooSmall {
                    part_number: 1,
                    min_bytes: 5 * 1024 * 1024,
                    actual: 42,
                },
                S3ErrorCode::EntityTooSmall,
            ),
            (
                StorageError::TooManyMultipartUploads { limit: 7 },
                S3ErrorCode::SlowDown,
            ),
            (
                StorageError::EntityTooLarge {
                    size: 1024,
                    limit: 100,
                },
                S3ErrorCode::EntityTooLarge,
            ),
        ];
        for (storage_err, expected) in cases {
            assert_eq!(s3_code(storage_err), expected);
        }
    }

    #[test]
    fn a_reflected_version_id_never_carries_a_control_byte() {
        // The version-id rules are length-only, so a request may carry
        // control characters (the repo's own pin builds `"bad\nid"`), and
        // quick-xml does not escape control bytes: a reflected one would
        // make the error body non-well-formed. The message replaces them.
        let err = map_backend_error(StorageError::NoSuchVersion("bad\nid".into()));
        assert_eq!(err.code().as_str(), "NoSuchVersion");
        assert_eq!(err.message(), Some("no such version: bad\u{FFFD}id"));

        let err = map_backend_error(StorageError::InvalidVersion("a\u{1}b\u{0}".into()));
        assert_eq!(err.code().as_str(), "InvalidArgument");
        assert!(
            !err.message()
                .unwrap_or_default()
                .chars()
                .any(char::is_control),
            "no control byte survives the reflection: {:?}",
            err.message()
        );
    }
}
