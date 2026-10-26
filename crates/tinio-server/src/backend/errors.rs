use s3s::{S3Error, s3_error};

use crate::_core::{lifecycle::InvalidKind, storage::Error as StorageError};

/// A model refusal as the wire's (R14, LC8, OL17): the class picks the code
/// (`Shape` → `MalformedXML`, `Semantic` → `InvalidRequest`) and the reason is
/// the body. `rule_id` is the lifecycle rule the refusal belongs to; an
/// object-lock refusal has none.
pub(crate) fn invalid_error(kind: InvalidKind, reason: &str, rule_id: Option<&str>) -> S3Error {
    match kind {
        InvalidKind::Shape => match rule_id {
            Some(id) => s3_error!(MalformedXML, "lifecycle rule {id}: {reason}"),
            None => s3_error!(MalformedXML, "{reason}"),
        },
        InvalidKind::Semantic => match rule_id {
            Some(id) => s3_error!(InvalidRequest, "lifecycle rule {id}: {reason}"),
            None => s3_error!(InvalidRequest, "{reason}"),
        },
    }
}

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
        // Object Lock (OL17): a protected permanent delete is the 403 the AWS
        // surface answers; the reason is the log's, never the body's.
        StorageError::Locked(reason) => {
            tracing::debug!(reason = %reason, "object lock refused the operation");
            s3_error!(AccessDenied)
        }
        // A lock-enabled bucket's versioning state refuses the operation
        // (OL13): the 409 `InvalidBucketState` the design pins.
        StorageError::InvalidBucketState(name) => s3_error!(
            InvalidBucketState,
            "invalid bucket state: `{}`",
            printable(name.as_ref())
        ),
        // The tier hooks' refusal: a backend without a cold plane cannot run
        // a transition (T10, T26).
        StorageError::NoTiers => s3_error!(NotImplemented),
        // A tier transition to a class the backend refuses (T10): the payload
        // is the backend's fixed explanation, never client data.
        StorageError::InvalidRequest(detail) => s3_error!(InvalidRequest, "{}", detail),
        // An invariant the contract guarantees was violated on disk — an
        // undecodable lock row. A server fault, never a client error.
        StorageError::Corrupt(detail) => {
            tracing::error!(detail, "corrupt stored state");
            s3_error!(InternalError)
        }
        // D5: the detail is the log's, never the body's. A backend `Io` can
        // wrap tinio-fs's `Invariant` (that crate's `From<Error> for
        // storage::Error`), whose `Debug` names the broken invariant while its
        // `Display` stays generic — so reflecting `Display` here would drop
        // the one diagnostic there is; an OS error's text, in turn, can embed
        // on-disk paths (the rule the select mapping applies). Log the whole
        // error and answer the generic 500.
        StorageError::Io(err) => {
            tracing::error!(error = ?err, "storage I/O error");
            s3_error!(InternalError)
        }
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
        object_lock::RefusalReason,
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
                StorageError::Locked(RefusalReason::Governance),
                S3ErrorCode::AccessDenied,
            ),
            (
                StorageError::InvalidBucketState(b.clone()),
                S3ErrorCode::InvalidBucketState,
            ),
            (StorageError::NoTiers, S3ErrorCode::NotImplemented),
            (
                StorageError::InvalidRequest("target class"),
                S3ErrorCode::InvalidRequest,
            ),
            (
                StorageError::Corrupt("bad lock row"),
                S3ErrorCode::InternalError,
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
    fn an_io_detail_stays_out_of_the_body() {
        // D5 (carried from the A1 review): the detail is the log's. A storage
        // `Io` is where tinio-fs's `Invariant` lands, so reflecting its text
        // would put an internal symbol (or an on-disk path) in the response;
        // the client sees the code and nothing else.
        let err = map_backend_error(StorageError::Io(IoError::other("secret/path")));
        assert_eq!(err.code().as_str(), "InternalError");
        assert!(
            !err.message().unwrap_or_default().contains("secret/path"),
            "the internal detail is not reflected: {:?}",
            err.message()
        );
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
