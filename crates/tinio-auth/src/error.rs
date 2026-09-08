//! The access-decision vocabulary the authorization pipeline and its
//! tests share, before mapping to `S3Result`.

use crate::_core::storage::Error as StorageError;

/// The outcome of one access evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessDecision {
    /// The request is authorized — proceed to the handler.
    Allow,
    /// Fail closed: `AccessDenied` (never reveals existence).
    Deny,
    /// The resource is missing: pass through so the handler answers its
    /// normal 404 (the caller proves the ownership/read tier first).
    PassThroughMissing,
}

/// Contract error → decision mapping per the spec (fail-closed). The
/// [`StorageError::NoSuchBucket`]/[`StorageError::NoSuchKey`] cases reach
/// [`AccessDecision::PassThroughMissing`]; `access.rs` then proves the
/// ownership/read tier over the bucket row — and passes a genuinely
/// missing bucket straight through to the handler's `NoSuchBucket`
/// (AWS + contract FR-005). Here the error is just classified.
pub fn classify(err: &StorageError) -> AccessDecision {
    match err {
        StorageError::NoSuchBucket(_) => AccessDecision::PassThroughMissing,
        StorageError::NoSuchKey(_) => AccessDecision::PassThroughMissing,
        _ => AccessDecision::Deny,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use io::Error as IoError;

    use super::*;
    use crate::_core::{bucket, object};

    #[test]
    fn missing_bucket_and_key_pass_through() {
        let b = bucket::name("data").unwrap();
        let k = object::key("k.bin").unwrap();
        assert_eq!(
            classify(&StorageError::NoSuchBucket(b)),
            AccessDecision::PassThroughMissing
        );
        assert_eq!(
            classify(&StorageError::NoSuchKey(k)),
            AccessDecision::PassThroughMissing
        );
    }

    #[test]
    fn other_errors_deny_closed() {
        let b = bucket::name("data").unwrap();
        assert_eq!(
            classify(&StorageError::NoSuchUpload("u-1".into())),
            AccessDecision::Deny
        );
        assert_eq!(
            classify(&StorageError::AlreadyExists(b)),
            AccessDecision::Deny
        );
        assert_eq!(
            classify(&StorageError::Io(IoError::other("boom"))),
            AccessDecision::Deny
        );
    }
}
