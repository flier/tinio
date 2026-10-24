//! The three Object Lock write headers of `PutObject`/`CopyObject`/
//! `CreateMultipartUpload` (`PostObject` reaches the same handler through
//! s3s's mapped `PutObjectInput`, OL1/OL16), as the write path's resolution
//! consumes them.
//!
//! The two `cfg` constructors are the feature split: with `object-lock` they
//! parse the request's headers; without it there are no lock headers to read,
//! and the empty value still lets the stored bucket default (and the checksum
//! that lock requires) apply (OL20, OL16).
//!
//! Always compiled: [`S3Backend::write_lock`](super::S3Backend::write_lock)
//! takes this value in every posture, so a stripped build states the "no
//! request headers" case once instead of repeating an empty argument list at
//! each write site.

#[cfg(feature = "object-lock")]
use s3s::s3_error;
use s3s::{S3Result, dto};

#[cfg(feature = "object-lock")]
use crate::_core::object_lock::Mode;
use crate::_core::object_lock::{LegalHold, Retention};

/// The three write headers, resolved into the model the write path consumes.
pub(crate) struct LockHeaders {
    /// The explicit retention the request's mode+date pair names.
    pub(crate) retention: Option<Retention>,
    /// The requested legal hold (`OFF` when the header is absent).
    pub(crate) legal_hold: LegalHold,
    /// Whether the request carried a lock header at all — the fact the
    /// write path's configuration check keys on.
    pub(crate) requested: bool,
}

#[cfg(feature = "object-lock")]
impl LockHeaders {
    /// Parse the three headers into the model. A mode without a date (or the
    /// reverse) is `InvalidRequest`; an unknown mode or hold literal is
    /// `InvalidArgument` (the header enums are closed).
    pub(crate) fn from_request(
        mode: Option<&dto::ObjectLockMode>,
        retain_until: Option<&dto::ObjectLockRetainUntilDate>,
        hold: Option<&dto::ObjectLockLegalHoldStatus>,
    ) -> S3Result<Self> {
        let retention = match (mode, retain_until) {
            (None, None) => None,
            (Some(mode), Some(date)) => Some(Retention {
                mode: mode_from_header(mode.as_str())?,
                retain_until: time::OffsetDateTime::from(date.clone()).into(),
            }),
            _ => {
                return Err(s3_error!(
                    InvalidRequest,
                    "x-amz-object-lock-mode and x-amz-object-lock-retain-until-date must both be supplied"
                ));
            }
        };
        let legal_hold = match hold.map(|hold| hold.as_str()) {
            None => LegalHold::Off,
            Some(value) => LegalHold::parse(value).map_err(|_| {
                s3_error!(
                    InvalidArgument,
                    "invalid x-amz-object-lock-legal-hold: {value}"
                )
            })?,
        };
        Ok(Self {
            requested: mode.is_some() || retain_until.is_some() || hold.is_some(),
            retention,
            legal_hold,
        })
    }
}

#[cfg(not(feature = "object-lock"))]
impl LockHeaders {
    /// A stripped build has no `x-amz-object-lock-*` headers: the empty value
    /// is the request's whole contribution, so the bucket's stored default
    /// (and the checksum it requires) still reaches the seam (OL20, OL16).
    pub(crate) fn from_request(
        _mode: Option<&dto::ObjectLockMode>,
        _retain_until: Option<&dto::ObjectLockRetainUntilDate>,
        _hold: Option<&dto::ObjectLockLegalHoldStatus>,
    ) -> S3Result<Self> {
        Ok(Self {
            retention: None,
            legal_hold: LegalHold::default(),
            requested: false,
        })
    }
}

/// The header's mode literal as the model, with the closed-enum error the
/// write surface answers (`InvalidArgument`).
#[cfg(feature = "object-lock")]
fn mode_from_header(value: &str) -> S3Result<Mode> {
    value
        .parse()
        .map_err(|_| s3_error!(InvalidArgument, "invalid Object Lock mode: {value}"))
}
