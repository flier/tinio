use std::time::{Duration, SystemTime};

use derive_more::Display;
use s3s::{
    S3Error, S3Result,
    dto::{self, ETag as WireETag},
    s3_error,
};
use time::OffsetDateTime;

use crate::_core::ETag;

/// The conditional header whose evaluation failed (the read and write
/// paths map them to different S3 error codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum ConditionFailure {
    #[display("If-Match failed")]
    Match,
    #[display("If-None-Match matched")]
    NoneMatch,
    #[display("not modified since")]
    ModifiedSince,
    #[display("If-Unmodified-Since failed")]
    UnmodifiedSince,
}

/// Map a failed condition onto its S3 error: the write path always
/// answers `412`; the read path answers `304` for the not-modified
/// conditions (RFC 7232).
pub(crate) fn condition_error(fail: ConditionFailure, write_path: bool) -> S3Error {
    if !write_path
        && matches!(
            fail,
            ConditionFailure::NoneMatch | ConditionFailure::ModifiedSince
        )
    {
        s3_error!(NotModified, "not modified")
    } else {
        s3_error!(PreconditionFailed, "{fail}")
    }
}

/// The conditional headers of one request (RFC 7232), bundled so the
/// read, write and copy paths share one evaluation signature. The ETag
/// conditions are borrowed (the request outlives the evaluation); the
/// dates are owned `Copy` DTOs.
#[derive(Debug)]
pub(crate) struct ConditionalHeaders<'a> {
    /// `If-Match` (also CopyObject's `x-amz-if-match` destination header).
    if_match: Option<&'a dto::ETagCondition>,
    /// `If-None-Match` (also CopyObject's `x-amz-if-none-match`).
    if_none_match: Option<&'a dto::ETagCondition>,
    /// `If-Modified-Since`.
    if_modified_since: Option<dto::IfModifiedSince>,
    /// `If-Unmodified-Since`.
    if_unmodified_since: Option<dto::IfUnmodifiedSince>,
}

impl<'a> ConditionalHeaders<'a> {
    /// The conditional set of a request.
    pub(crate) fn new(
        if_match: Option<&'a dto::ETagCondition>,
        if_none_match: Option<&'a dto::ETagCondition>,
        if_modified_since: Option<dto::IfModifiedSince>,
        if_unmodified_since: Option<dto::IfUnmodifiedSince>,
    ) -> Self {
        Self {
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
        }
    }

    /// Evaluate against the object's ETag and mtime, reporting the
    /// failing header.
    ///
    /// Precedence per RFC 9110 §13.2.2: If-Match, then If-Unmodified-Since,
    /// then If-None-Match, then If-Modified-Since (each date header is
    /// ignored while its ETag counterpart is present). In particular a
    /// matching If-None-Match combined with a failing If-Unmodified-Since
    /// answers 412, never 304 — a caching client must not reuse a stale
    /// body.
    fn eval(self, etag: &ETag, last_modified: SystemTime) -> Result<(), ConditionFailure> {
        let wire = etag.as_str();
        if let Some(cond) = self.if_match {
            let ok = cond.is_any()
                || cond
                    .as_etag()
                    .map(|e| e.strong_cmp(&WireETag::Strong(wire.to_string())))
                    .unwrap_or(false);
            if !ok {
                return Err(ConditionFailure::Match);
            }
        }
        // RFC 9110 §13.2.2: If-Unmodified-Since comes BEFORE If-None-Match
        // — a failing date must answer 412 even when the ETag condition
        // would 304. It is ignored while If-Match is present (§13.1.4).
        // Both date comparisons run against the second-truncated mtime
        // (F08): HTTP dates carry no sub-second part, and a date equal to
        // the echoed Last-Modified must not count as "modified after".
        let last_modified = truncate_to_second(last_modified);
        if self.if_match.is_none()
            && let Some(since) = self.if_unmodified_since
            && last_modified > to_system_time(since)
        {
            return Err(ConditionFailure::UnmodifiedSince);
        }
        if let Some(cond) = self.if_none_match {
            let matched = cond.is_any()
                || cond
                    .as_etag()
                    .map(|e| e.weak_cmp(&WireETag::Strong(wire.to_string())))
                    .unwrap_or(false);
            if matched {
                return Err(ConditionFailure::NoneMatch);
            }
        }
        // Ignored while If-None-Match is present (§13.2.2).
        if self.if_none_match.is_none()
            && let Some(since) = self.if_modified_since
            && last_modified <= to_system_time(since)
        {
            return Err(ConditionFailure::ModifiedSince);
        }
        Ok(())
    }

    /// Evaluate the conditional headers against the object's ETag and
    /// mtime (RFC 7232). The read path answers 304 for If-None-Match /
    /// If-Modified-Since and 412 for If-Match / If-Unmodified-Since; the
    /// write path answers 412 for every failure (never 304).
    pub(crate) fn check(
        self,
        etag: &ETag,
        last_modified: SystemTime,
        write_path: bool,
    ) -> S3Result<()> {
        self.eval(etag, last_modified)
            .map_err(|fail| condition_error(fail, write_path))
    }
}

/// A response timestamp into a [`SystemTime`] (conditional-header
/// comparison).
fn to_system_time(t: dto::Timestamp) -> SystemTime {
    OffsetDateTime::from(t).into()
}

/// The storage mtime truncated to whole seconds — the granularity HTTP
/// dates (and S3's stored mtimes) carry (F08). A 12:00:00.500 write
/// echoes `Last-Modified: 12:00:00Z`; a client that round-trips that
/// date into a condition header must get S3's answer, not "modified
/// after" by half a second.
fn truncate_to_second(t: SystemTime) -> SystemTime {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use s3s::{
        S3ErrorCode,
        dto::{self},
    };

    use super::*;
    use crate::_core::ETag;

    fn etag(value: &str) -> ETag {
        value.parse().unwrap()
    }

    const LM: u64 = 100;

    fn check(
        etag: &ETag,
        if_match: Option<dto::ETagCondition>,
        if_none_match: Option<dto::ETagCondition>,
        if_modified_since: Option<dto::Timestamp>,
        if_unmodified_since: Option<dto::Timestamp>,
        write_path: bool,
    ) -> Option<S3ErrorCode> {
        check_at(
            etag,
            SystemTime::UNIX_EPOCH + Duration::from_secs(LM),
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            write_path,
        )
    }

    /// [`check`] against an explicit storage mtime (the F08 sub-second
    /// cases).
    fn check_at(
        etag: &ETag,
        mtime: SystemTime,
        if_match: Option<dto::ETagCondition>,
        if_none_match: Option<dto::ETagCondition>,
        if_modified_since: Option<dto::Timestamp>,
        if_unmodified_since: Option<dto::Timestamp>,
        write_path: bool,
    ) -> Option<S3ErrorCode> {
        let result = ConditionalHeaders::new(
            if_match.as_ref(),
            if_none_match.as_ref(),
            if_modified_since,
            if_unmodified_since,
        )
        .check(etag, mtime, write_path);
        match result {
            Ok(()) => None,
            Err(err) => Some(err.code().clone()),
        }
    }

    #[test]
    fn no_conditions_pass() {
        let e = etag("5d41402abc4b2a76b9719d911017c592");
        assert_eq!(check(&e, None, None, None, None, false), None);
    }

    #[test]
    fn date_conditions_compare_at_second_granularity() {
        // F08: the storage mtime is sub-second; HTTP dates are
        // second-truncated (the echoed Last-Modified). A client that
        // round-trips the echoed date must get S3 behavior: a date equal
        // to the truncated mtime is "not modified" — If-Unmodified-Since
        // passes, If-Modified-Since answers 304.
        let e = etag("5d41402abc4b2a76b9719d911017c592");
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(LM) + Duration::from_millis(500);
        let since = dto::Timestamp::from(OffsetDateTime::from_unix_timestamp(LM as i64).unwrap());
        // The truncated-second boundary: the object was stored 500 ms
        // into second 100; the echoed Last-Modified is 100:00Z.
        assert_eq!(
            check_at(&e, mtime, None, None, None, Some(since.clone()), false),
            None,
            "If-Unmodified-Since at the truncated mtime must pass"
        );
        assert_eq!(
            check_at(&e, mtime, None, None, Some(since), None, false),
            Some(S3ErrorCode::NotModified),
            "If-Modified-Since at the truncated mtime must answer 304"
        );
        // The second before still counts as modified-after / not-modified.
        let earlier =
            dto::Timestamp::from(OffsetDateTime::from_unix_timestamp(LM as i64 - 1).unwrap());
        assert_eq!(
            check_at(&e, mtime, None, None, None, Some(earlier.clone()), false),
            Some(S3ErrorCode::PreconditionFailed)
        );
        assert_eq!(
            check_at(&e, mtime, None, None, Some(earlier), None, false),
            None
        );
    }

    #[test]
    fn condition_error_maps_failures() {
        assert_eq!(
            condition_error(ConditionFailure::NoneMatch, false)
                .code()
                .clone(),
            S3ErrorCode::NotModified
        );
        assert_eq!(
            condition_error(ConditionFailure::ModifiedSince, false)
                .code()
                .clone(),
            S3ErrorCode::NotModified
        );
        assert_eq!(
            condition_error(ConditionFailure::Match, false)
                .code()
                .clone(),
            S3ErrorCode::PreconditionFailed
        );
        assert_eq!(
            condition_error(ConditionFailure::UnmodifiedSince, true)
                .code()
                .clone(),
            S3ErrorCode::PreconditionFailed
        );
    }
}
