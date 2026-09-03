//! The conditional-header machinery (RFC 9110 §13 + the S3
//! conditional-write extensions).
//!
//! The missing-object answers are three deliberate per-operation
//! policies — they differ on purpose, and this module is their single
//! home:
//! - PutObject / CopyObject destination: `If-Match` on an absent object
//!   → 412 via [`ConditionalHeaders::check_missing`] (create-if-absent).
//! - CompleteMultipartUpload destination: `If-Match` on an absent object
//!   → 404 NoSuchKey via [`check_complete_conditions`].
//! - DeleteObject: an absent object answers 204 under every conditional
//!   header — delete is idempotent, the conditions gate an existing
//!   object only, and [`DeleteConditions::absent`] decides whether a
//!   delete is conditional at all.
//!
//! `If-None-Match` passes on an absent object on every destination path.

/// The copy-only conditionals parse an ETag-condition wire value.
#[cfg(feature = "copy")]
use std::str::FromStr;
use std::{cmp::Ordering, time::SystemTime};

use derive_more::Display;
use s3s::{
    S3Error, S3Result,
    dto::{ETagCondition, IfModifiedSince, IfUnmodifiedSince, Timestamp, TimestampFormat},
    s3_error,
};
use time::OffsetDateTime;

use crate::_core::{ETag, object};

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
/// dates are owned DTOs.
#[derive(Debug)]
pub(crate) struct ConditionalHeaders<'a> {
    /// `If-Match` (also CopyObject's `x-amz-if-match` destination header).
    if_match: Option<&'a ETagCondition>,
    /// `If-None-Match` (also CopyObject's `x-amz-if-none-match`).
    if_none_match: Option<&'a ETagCondition>,
    /// `If-Modified-Since`.
    if_modified_since: Option<IfModifiedSince>,
    /// `If-Unmodified-Since`.
    if_unmodified_since: Option<IfUnmodifiedSince>,
}

impl<'a> ConditionalHeaders<'a> {
    /// The conditional set of a request.
    pub(crate) fn new(
        if_match: Option<&'a ETagCondition>,
        if_none_match: Option<&'a ETagCondition>,
        if_modified_since: Option<IfModifiedSince>,
        if_unmodified_since: Option<IfUnmodifiedSince>,
    ) -> Self {
        Self {
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
        }
    }

    /// The destination-write etag-only set (PutObject, CopyObject's
    /// destination, CompleteMultipartUpload — AWS conditional writes):
    /// those protocols carry no date headers, and every construction
    /// site states that invariant by reaching for this instead of
    /// `new` with two literal `None` date arguments.
    pub(crate) fn etag_only(
        if_match: Option<&'a ETagCondition>,
        if_none_match: Option<&'a ETagCondition>,
    ) -> Self {
        Self::new(if_match, if_none_match, None, None)
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
    fn eval(&self, etag: &ETag, last_modified: SystemTime) -> Result<(), ConditionFailure> {
        if let Some(cond) = self.if_match
            && !strong_matches(cond, etag)
        {
            return Err(ConditionFailure::Match);
        }
        // RFC 9110 §13.2.2: If-Unmodified-Since comes BEFORE If-None-Match
        // — a failing date must answer 412 even when the ETag condition
        // would 304. It is ignored while If-Match is present (§13.1.4).
        // Both date comparisons run at whole-second precision (F08) via
        // [`whole_second_ordering`]: HTTP dates carry no sub-second part,
        // and a date equal to the echoed Last-Modified must not count as
        // "modified after".
        if self.if_match.is_none()
            && let Some(since) = self.if_unmodified_since.as_ref()
            && whole_second_ordering(last_modified, since) == Ordering::Greater
        {
            return Err(ConditionFailure::UnmodifiedSince);
        }
        if let Some(cond) = self.if_none_match {
            // Weak comparison (RFC 9110 §13.2.2 — the 304 answer uses
            // weak semantics): s3s's `weak_cmp` is value equality, so the
            // condition's tag compares against the stored value directly
            // — no wire `ETag` constructed per request (the
            // `strong_matches` pattern).
            let matched =
                cond.is_any() || cond.as_etag().is_some_and(|e| e.value() == etag.as_str());
            if matched {
                return Err(ConditionFailure::NoneMatch);
            }
        }
        // Ignored while If-None-Match is present (§13.2.2).
        if self.if_none_match.is_none()
            && let Some(since) = self.if_modified_since.as_ref()
            && whole_second_ordering(last_modified, since) != Ordering::Greater
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
        &self,
        etag: &ETag,
        last_modified: SystemTime,
        write_path: bool,
    ) -> S3Result<()> {
        self.eval(etag, last_modified)
            .map_err(|fail| condition_error(fail, write_path))
    }

    /// True when every header is absent — the fast-path decision (skip
    /// the head) for the destination and read paths. Named for what it
    /// checks, not for an imagined "some header present" reading: the
    /// conditional machinery always phrases the fast path as "no
    /// conditions".
    pub(crate) fn absent(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.if_modified_since.is_none()
            && self.if_unmodified_since.is_none()
    }

    /// The destination-write (PutObject, CopyObject-destination)
    /// missing-object policy: only `If-Match` can fail against an absent
    /// object — create-if-absent — and it answers 412
    /// (`ConditionFailure::Match` maps to 412 on both paths — the 304
    /// mapping covers only NoneMatch/ModifiedSince, which a missing
    /// object can never raise). The complete (404 NoSuchKey) and delete
    /// (204, idempotent) paths deliberately answer differently — see the
    /// module doc for the central policy statement.
    pub(crate) fn check_missing(&self) -> S3Result<()> {
        if self.if_match.is_some() {
            return Err(condition_error(ConditionFailure::Match, true));
        }
        Ok(())
    }
}

/// The strong ETag-match rule shared by every condition site (the
/// evaluator, the delete checker, and If-Range): the wildcard matches
/// any existing object; a specific tag must be a strong,
/// character-identical match. (s3s's `strong_cmp` is exactly this
/// equality on the strong values — compared without constructing a
/// wire `ETag` per request.)
fn strong_matches(cond: &ETagCondition, etag: &ETag) -> bool {
    cond.is_any()
        || cond
            .as_etag()
            .is_some_and(|e| e.as_strong().is_some_and(|v| v == etag.as_str()))
}

/// Parse an ETag-condition VALUE (`x-amz-if-match` / `x-amz-if-none-match`
/// wire strings, and the String-typed RenameObject source fields) into
/// the DTO type when present — the sites not part of the s3s DTO, so they
/// are read from the headers / String fields here. A malformed value is a
/// request-shape error, 400 `InvalidArgument` "invalid {name} header".
#[cfg(feature = "copy")]
pub(crate) fn parse_etag_condition_value(
    value: Option<&str>,
    name: &'static str,
) -> S3Result<Option<ETagCondition>> {
    value
        .map(|v| {
            ETagCondition::from_str(v)
                .map_err(|_| s3_error!(InvalidArgument, "invalid {name} header"))
        })
        .transpose()
}

/// Parse an ETag-condition header (`x-amz-if-match`, `x-amz-if-none-match`)
/// into the DTO type when present — the [`parse_etag_condition_value`]
/// rule over the request's `HeaderMap`. CopyObject's destination
/// conditionals are not part of the s3s DTO, so they are read from the
/// headers here.
#[cfg(feature = "copy")]
pub(crate) fn parse_etag_condition_header(
    headers: &http::HeaderMap,
    name: &'static str,
) -> Result<Option<ETagCondition>, S3Error> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let text = value
        .to_str()
        .map_err(|_| s3_error!(InvalidArgument, "invalid {name} header"))?;
    parse_etag_condition_value(Some(text), name)
}

/// The `If-Range` value (RFC 9110 §13.1.5): an entity-tag or an
/// HTTP-date. The wildcard is not a valid value; anything unparseable
/// is ignored (the header is dropped, serving the Range as usual).
#[derive(Debug)]
pub(crate) enum IfRange {
    Etag(ETagCondition),
    Date(Timestamp),
}

impl IfRange {
    /// True when the current representation still matches the If-Range
    /// condition: the strong ETag-match rule of [`strong_matches`], or
    /// the RFC 9110 §13.1.5 date rule — a date matches when the stored
    /// mtime is not strictly newer than the header date
    /// (`whole_second_ordering(last_modified, header) != Greater`: an
    /// equal or later date matches, a future date matches, only an older
    /// date fails). The delete/abort timestamp conditions compare at the
    /// same precision but with equality semantics — see
    /// [`same_whole_second`] and [`whole_second_ordering`].
    pub(crate) fn matches(&self, etag: &ETag, last_modified: SystemTime) -> bool {
        match self {
            // A weak tag never strong-matches; the wildcard is not a
            // valid If-Range value (the parser rejects it).
            IfRange::Etag(cond) => strong_matches(cond, etag),
            IfRange::Date(d) => whole_second_ordering(last_modified, d) != Ordering::Greater,
        }
    }
}

/// Parse the `If-Range` header; `None` when absent, invalid, or `*`.
///
/// The ETag branch accepts only the RFC 9110 §8.8.3 shapes (quoted, or
/// weak-quoted): s3s's condition parser also tolerates bare unquoted
/// tokens (S3 compatibility), and a bare token like `garbage` is not a
/// valid If-Range — it must be ignored, not treated as a mismatching
/// validator.
pub(crate) fn parse_if_range(headers: &http::HeaderMap) -> Option<IfRange> {
    let value = headers.get("if-range")?.to_str().ok()?;
    let etag = if value.starts_with('"') || value.starts_with("W/\"") {
        value.parse::<ETagCondition>().ok().map(IfRange::Etag)
    } else {
        None
    };
    etag.or_else(|| {
        Timestamp::parse(TimestampFormat::HttpDate, value)
            .ok()
            .map(IfRange::Date)
    })
}

/// Whether a same-key write raced between the GET's head and its body
/// fetch. The gate compares ETag OR mtime: a byte-identical re-PUT
/// changes the mtime but not the content-MD5 ETag, and date conditions /
/// date If-Ranges must re-evaluate on it (the old etag-only gate missed
/// exactly that class). Coarse filesystem mtimes (FAT's 2 s ticks) can
/// still miss a same-tick rewrite — the same gap every lock-free GET has.
pub(crate) fn generation_changed(head: &object::Info, fetched: &object::Info) -> bool {
    head.etag != fetched.etag || head.last_modified != fetched.last_modified
}

/// Whether a raced fetch must be dropped and the full object fetched
/// once more (the head generation differs from the fetched one; the
/// conditions have already re-evaluated against the fetched snapshot
/// and passed). A stale If-Range forces the refetch ONLY when the fetch
/// actually served a range — a full body already fetched is never
/// discarded: the Range was dropped pre-fetch exactly when If-Range
/// failed the head, and the full body is then the right answer even
/// when the generation changed since.
pub(crate) fn decide_fetch(
    head: &object::Info,
    fetched: &object::Info,
    if_range: Option<&IfRange>,
    served_range: Option<(u64, u64)>,
) -> bool {
    generation_changed(head, fetched)
        && matches!(
            (if_range, served_range),
            (Some(ir), Some(_)) if !ir.matches(&fetched.etag, fetched.last_modified)
        )
}

/// Whether a ranged-fetch `InvalidRange` must drop the Range and serve
/// the full current object — vs answering 416. The storage error
/// carries the CURRENT object size; the head carries the validator the
/// Range was honored (or would be honored) under. The head-first flow
/// honored the Range only when the If-Range matched the head at gate
/// time, so a size differing from the head's means the generation shrank
/// or was replaced mid-flight; the head-less If-Range flow (no RFC 7232
/// conditions) fetched blind and evaluates the validator against its
/// lazy head right here. Either way — stale validator, or changed size —
/// the If-Range can no longer be validated (RFC 9110 §13.1.5: a stale
/// If-Range drops the Range → full 200). A deliberate one-refetch
/// simplification: the refetched full body is served even when the new
/// generation's validator would actually match the If-Range again.
pub(crate) fn decide_range_error(
    if_range: Option<&IfRange>,
    head: Option<&object::Info>,
    err_size: u64,
) -> bool {
    match (if_range, head) {
        (Some(ir), Some(h)) => !ir.matches(&h.etag, h.last_modified) || h.size != err_size,
        _ => false,
    }
}

/// DeleteObject's conditional trio (If-Match / If-Match-Last-Modified-
/// Time / If-Match-Size), bundled like [`ConditionalHeaders`] so the
/// presence test and the evaluation cannot drift apart: every provided
/// condition must pass (AND) — ETags compare strong (`*` matches any
/// existing object), the timestamp compares at whole-second precision,
/// the size compares exactly — and every failure answers 412 (the same
/// `ConditionFailure::Match` mapping as the evaluator). The
/// request-shape rejection (a negative `If-Match-Size` → 400) happens
/// up front in [`checked_if_match_size`]; the size arrives here already
/// in the unsigned domain.
pub(crate) struct DeleteConditions<'a> {
    if_match: Option<&'a ETagCondition>,
    if_match_last_modified_time: Option<Timestamp>,
    if_match_size: Option<u64>,
}

impl<'a> DeleteConditions<'a> {
    pub(crate) fn new(
        if_match: Option<&'a ETagCondition>,
        if_match_last_modified_time: Option<Timestamp>,
        if_match_size: Option<u64>,
    ) -> Self {
        Self {
            if_match,
            if_match_last_modified_time,
            if_match_size,
        }
    }

    /// True when every header is absent — the delete answers 204 on a
    /// missing object under every conditional header, so the op only
    /// checks conditions when the head exists (see the module doc for
    /// the central missing-object policy statement).
    pub(crate) fn absent(&self) -> bool {
        self.if_match.is_none()
            && self.if_match_last_modified_time.is_none()
            && self.if_match_size.is_none()
    }

    /// Evaluate every provided condition against the delete head.
    pub(crate) fn check(&self, info: &object::Info) -> S3Result<()> {
        if let Some(cond) = self.if_match
            && !strong_matches(cond, &info.etag)
        {
            return Err(condition_error(ConditionFailure::Match, true));
        }
        if let Some(t) = self.if_match_last_modified_time.as_ref()
            && !same_whole_second(info.last_modified, t)
        {
            return Err(condition_error(ConditionFailure::Match, true));
        }
        if self.if_match_size.is_some_and(|cond| cond != info.size) {
            return Err(condition_error(ConditionFailure::Match, true));
        }
        Ok(())
    }
}

/// `If-Match-Size` into the unsigned domain: the stored size is `u64`,
/// and a negative value is a request-shape error (400 `InvalidArgument`)
/// — it can never match any object. Validated up front,
/// state-independently, like the both-present 400 — a delete of a
/// missing key with a negative size must still answer 400, not the
/// idempotent 204.
pub(crate) fn checked_if_match_size(size: i64) -> S3Result<u64> {
    u64::try_from(size).map_err(|_| s3_error!(InvalidArgument, "invalid If-Match-Size"))
}

/// The request-shape gate of every destination write op (PutObject,
/// CopyObject destination, CompleteMultipartUpload — AWS conditional
/// writes): `If-Match` + `If-None-Match` together → 400
/// `InvalidRequest`, and `If-None-Match` accepts `*` only (a specific
/// value → 501 `NotImplemented` — AWS does not implement specific
/// If-None-Match comparisons on the write path; a non-matching value
/// must never fall through to a silent overwrite). The ops call this up
/// front, before any body is staged, parts parsed, or lock taken — a
/// rejected request must not pay for them. The copy-source family
/// (`x-amz-copy-source-if-*`) and the read paths are exempt and keep
/// the RFC 9110 evaluation.
pub(crate) fn check_write_shape(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
) -> S3Result<()> {
    // The both-present rule is the gate's first half (400 InvalidRequest
    // — the single body of the rule is this branch; there is no checker
    // level below it to keep in sync).
    if if_match.is_some() && if_none_match.is_some() {
        return Err(s3_error!(
            InvalidRequest,
            "If-Match and If-None-Match cannot both be present"
        ));
    }
    if let Some(cond) = if_none_match
        && !cond.is_any()
    {
        return Err(s3_error!(
            NotImplemented,
            "If-None-Match only accepts '*' (AWS: a specific ETag is not implemented)"
        ));
    }
    Ok(())
}

/// CompleteMultipartUpload's destination conditions (AWS conditional
/// writes, verified 2026-08-31): the checks evaluate against the
/// object CURRENTLY at the key (the one being replaced) — not the
/// composed ETag of the completing upload. `If-Match` strong-compares
/// (`*` = any existing object); a missing destination → 404
/// `NoSuchKey`, a mismatch → 412. `If-None-Match` accepts `*` only —
/// missing destination passes, an existing object → 412. The
/// request-shape rejections (both headers → 400, a specific value →
/// 501) live in [`check_write_shape`], the shared destination-write
/// gate (put and copy call it too), which the ops call before the
/// lock.
#[cfg(feature = "multipart")]
pub(crate) fn check_complete_conditions(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    current: Option<&object::Info>,
) -> S3Result<()> {
    match current {
        Some(info) => ConditionalHeaders::etag_only(if_match, if_none_match).check(
            &info.etag,
            info.last_modified,
            true,
        ),
        None if if_match.is_some() => Err(s3_error!(
            NoSuchKey,
            "the destination object does not exist"
        )),
        None => Ok(()),
    }
}

/// A `SystemTime` truncated to whole seconds (the Last-Modified wire
/// precision) — conditional timestamp comparisons.
pub(crate) fn to_whole_seconds(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A response timestamp into a [`SystemTime`] (conditional-header
/// comparison).
pub(crate) fn to_system_time(t: &Timestamp) -> SystemTime {
    OffsetDateTime::from(t.clone()).into()
}

/// Whether a stored time and a header timestamp land on the same whole
/// second — the delete and abort timestamp conditions, whose equality
/// semantics differ from If-Range's `<=` rule (see
/// [`whole_second_ordering`]).
pub(crate) fn same_whole_second(stored: SystemTime, header: &Timestamp) -> bool {
    whole_second_ordering(stored, header) == Ordering::Equal
}

/// The whole-second ordering of a stored time against a header date —
/// the single comparison primitive of every date condition (F08: HTTP
/// dates carry no sub-second part, so both sides compare at the wire
/// precision of the echoed Last-Modified). A 12:00:00.500 write echoes
/// `Last-Modified: 12:00:00Z`; a client that round-trips that date into
/// a condition header must get S3's answer, not "modified after" by half
/// a second. The rules built on it: If-Unmodified-Since fails on
/// `Greater`, If-Modified-Since and a date If-Range fail on `!= Greater`,
/// and the delete/abort equality conditions on `== Equal`.
pub(crate) fn whole_second_ordering(stored: SystemTime, header: &Timestamp) -> Ordering {
    to_whole_seconds(stored).cmp(&to_whole_seconds(to_system_time(header)))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use s3s::S3ErrorCode;

    use super::*;
    use crate::_core::{
        ETag,
        object::{self, Tags},
    };

    fn etag(value: &str) -> ETag {
        value.parse().unwrap()
    }

    /// A wire condition value (`"etag"`, `*`, `W/"etag"`).
    fn cond(value: &str) -> ETagCondition {
        value.parse().unwrap()
    }

    /// A `Timestamp` at whole-second `secs` since the epoch.
    fn timestamp(secs: u64) -> Timestamp {
        Timestamp::from(OffsetDateTime::from_unix_timestamp(secs as i64).unwrap())
    }

    const LM: u64 = 100;

    fn check(
        etag: &ETag,
        if_match: Option<ETagCondition>,
        if_none_match: Option<ETagCondition>,
        if_modified_since: Option<Timestamp>,
        if_unmodified_since: Option<Timestamp>,
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
        if_match: Option<ETagCondition>,
        if_none_match: Option<ETagCondition>,
        if_modified_since: Option<Timestamp>,
        if_unmodified_since: Option<Timestamp>,
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
        let since = Timestamp::from(OffsetDateTime::from_unix_timestamp(LM as i64).unwrap());
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
        let earlier = Timestamp::from(OffsetDateTime::from_unix_timestamp(LM as i64 - 1).unwrap());
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
    fn whole_second_ordering_compares_at_second_precision() {
        // The primitive under every date rule: a 500 ms offset inside
        // second 100 compares Equal to a header date at 100, Greater than
        // 99, Less than 101.
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(LM) + Duration::from_millis(500);
        assert_eq!(
            whole_second_ordering(mtime, &timestamp(LM)),
            Ordering::Equal
        );
        assert_eq!(
            whole_second_ordering(mtime, &timestamp(LM - 1)),
            Ordering::Greater
        );
        assert_eq!(
            whole_second_ordering(mtime, &timestamp(LM + 1)),
            Ordering::Less
        );
        // A pre-epoch stored time (to_whole_seconds floors to 0) is Less
        // than any positive header date.
        let ancient = SystemTime::UNIX_EPOCH - Duration::from_secs(5);
        assert_eq!(
            whole_second_ordering(ancient, &timestamp(1)),
            Ordering::Less
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

    #[test]
    fn absent_true_only_when_no_header_is_present() {
        let specific = cond(r#""abc""#);
        let star = cond("*");
        let since = timestamp(1);
        assert!(ConditionalHeaders::new(None, None, None, None).absent());
        assert!(!ConditionalHeaders::new(Some(&specific), None, None, None).absent());
        assert!(!ConditionalHeaders::new(None, Some(&star), None, None).absent());
        assert!(!ConditionalHeaders::new(None, None, Some(since.clone()), None).absent());
        assert!(!ConditionalHeaders::new(None, None, None, Some(since)).absent());
    }

    #[test]
    fn check_missing_fails_only_if_match() {
        let specific = cond(r#""abc""#);
        let star = cond("*");
        let im = ConditionalHeaders::new(Some(&specific), None, None, None);
        assert_eq!(
            im.check_missing().unwrap_err().code().clone(),
            S3ErrorCode::PreconditionFailed
        );
        let inm = ConditionalHeaders::new(None, Some(&star), None, None);
        assert!(
            inm.check_missing().is_ok(),
            "If-None-Match passes on a missing object"
        );
        let none = ConditionalHeaders::new(None, None, None, None);
        assert!(none.check_missing().is_ok());
    }

    #[test]
    fn if_range_parses_etag_date_and_ignores_garbage() {
        let mut headers = http::HeaderMap::new();
        assert!(parse_if_range(&headers).is_none(), "absent header");
        headers.insert("if-range", r#""abc""#.parse().unwrap());
        assert!(matches!(parse_if_range(&headers), Some(IfRange::Etag(_))));
        headers.insert("if-range", "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap());
        assert!(matches!(parse_if_range(&headers), Some(IfRange::Date(_))));
        // RFC 9110 §13.1.5: a wildcard is not a valid If-Range value.
        headers.insert("if-range", "*".parse().unwrap());
        assert!(parse_if_range(&headers).is_none());
        // A bare unquoted token is not a valid entity-tag (RFC 9110
        // §8.8.3) — ignored even though s3s's condition parser would
        // tolerate it as an S3-compat strong tag.
        headers.insert("if-range", "garbage".parse().unwrap());
        assert!(parse_if_range(&headers).is_none());
        headers.insert("if-range", "not a date".parse().unwrap());
        assert!(parse_if_range(&headers).is_none());
    }

    #[test]
    fn if_range_matches_strong_etag_and_second_precision_date() {
        let e = etag("5d41402abc4b2a76b9719d911017c592");
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(LM);
        assert!(IfRange::Etag(cond(r#""5d41402abc4b2a76b9719d911017c592""#)).matches(&e, t));
        assert!(!IfRange::Etag(cond(r#""zzz""#)).matches(&e, t));
        // A weak tag never strong-matches.
        assert!(!IfRange::Etag(cond(r#"W/"5d41402abc4b2a76b9719d911017c592""#)).matches(&e, t));
        // Date — RFC 9110 §13.1.5 match rule, NOT equality: a date matches
        // when `last_modified <= header_date` (an equal or later date
        // matches, a future date matches, only an older date fails).
        assert!(IfRange::Date(timestamp(LM)).matches(&e, t));
        assert!(IfRange::Date(timestamp(LM + 1)).matches(&e, t));
        assert!(!IfRange::Date(timestamp(LM - 1)).matches(&e, t));
    }

    /// An `object::Info` fixture (whole-second mtime).
    fn info(etag_hex: &str, mtime_secs: u64, size: u64) -> object::Info {
        object::Info {
            key: object::key("k.bin").unwrap(),
            size,
            last_modified: SystemTime::UNIX_EPOCH + Duration::from_secs(mtime_secs),
            etag: etag(etag_hex),
            tags: Tags::empty(),
            checksum: None,
        }
    }

    #[test]
    fn generation_changed_detects_mtime_only_rewrites() {
        let e0 = "5d41402abc4b2a76b9719d911017c592";
        let head = info(e0, 100, 5);
        // A byte-identical re-PUT: same content-MD5 ETag, newer mtime —
        // the rewrite the old etag-only gate missed.
        assert!(generation_changed(&head, &info(e0, 101, 5)));
        // An etag change is a generation change too…
        assert!(generation_changed(
            &head,
            &info("deadbeefdeadbeefdeadbeefdeadbeef", 100, 5)
        ));
        // …and identical snapshots are one generation.
        assert!(!generation_changed(&head, &info(e0, 100, 5)));
    }

    #[test]
    fn decide_fetch_refetches_only_a_served_range_under_a_stale_if_range() {
        let e0 = "5d41402abc4b2a76b9719d911017c592";
        let e1 = "deadbeefdeadbeefdeadbeefdeadbeef";
        let head = info(e0, 100, 5);
        let same = info(e0, 100, 5);
        let raced = info(e1, 101, 5);
        let stale = IfRange::Etag(cond(&format!("\"{e0}\"")));

        // No generation change → serve, whatever the If-Range state.
        assert!(!decide_fetch(&head, &same, Some(&stale), Some((0, 4))));
        // A raced write with a stale If-Range over a SERVED range →
        // refetch the full object…
        assert!(decide_fetch(&head, &raced, Some(&stale), Some((0, 4))));
        // …but a full body already fetched is never discarded (the Range
        // was dropped when If-Range failed the head).
        assert!(!decide_fetch(&head, &raced, Some(&stale), None));
        // A date If-Range that still matches the raced generation keeps
        // the 206 (the honored range is valid against the served bytes).
        assert!(!decide_fetch(
            &head,
            &raced,
            Some(&IfRange::Date(timestamp(200))),
            Some((0, 4))
        ));
        // No If-Range → serve (the conditions already re-evaluated).
        assert!(!decide_fetch(&head, &raced, None, Some((0, 4))));
    }

    #[test]
    fn decide_range_error_distinguishes_shrink_from_unsatisfiable() {
        let e0 = "5d41402abc4b2a76b9719d911017c592";
        let e1 = "deadbeefdeadbeefdeadbeefdeadbeef";
        let head = info(e0, 100, 500);
        let ir = IfRange::Etag(cond(&format!("\"{e0}\"")));

        // No If-Range (a plain Range): 416, whatever the size changed to.
        assert!(!decide_range_error(None, Some(&head), 50));
        // Honored If-Range, SAME size as the head → the object did not
        // change: the range is genuinely unsatisfiable → 416.
        assert!(!decide_range_error(Some(&ir), Some(&head), 500));
        // Honored If-Range, size differs → the object shrank or was
        // replaced: the If-Range is stale → drop the Range and serve the
        // full object.
        assert!(decide_range_error(Some(&ir), Some(&head), 50));
        // Without a head there is no shrink to classify (defensive).
        assert!(!decide_range_error(Some(&ir), None, 50));
        // A stale validator over an UNCHANGED size (the head-less flow's
        // lazy head, where no pre-fetch match was guaranteed): the Range
        // must be dropped → full object.
        let stale_head = info(e1, 101, 500);
        assert!(decide_range_error(Some(&ir), Some(&stale_head), 500));
        assert!(!decide_range_error(None, Some(&stale_head), 500));
    }

    #[test]
    fn delete_conditions_require_every_provided_header() {
        // Every check evaluates against an object::Info (the delete head).
        let e0 = "5d41402abc4b2a76b9719d911017c592";
        let head = info(e0, LM, 100);
        let pass = |im: Option<ETagCondition>, lmt: Option<Timestamp>, size: Option<u64>| {
            DeleteConditions::new(im.as_ref(), lmt, size).check(&head)
        };

        // All three matching → pass.
        let ok = cond(&format!("\"{e0}\""));
        assert!(pass(Some(ok), Some(timestamp(LM)), Some(100)).is_ok());

        // ETag mismatch → 412; weak tag never matches; wildcard matches.
        let zzz = cond(r#""zzz""#);
        assert_eq!(
            pass(Some(zzz), None, None).unwrap_err().code().clone(),
            S3ErrorCode::PreconditionFailed
        );
        let weak = cond(&format!("W/\"{e0}\""));
        assert_eq!(
            pass(Some(weak), None, None).unwrap_err().code().clone(),
            S3ErrorCode::PreconditionFailed
        );
        let star = cond("*");
        assert!(pass(Some(star), None, None).is_ok());

        // Last-modified-time and size compare exactly (second precision).
        assert_eq!(
            pass(None, Some(timestamp(LM + 1)), None)
                .unwrap_err()
                .code()
                .clone(),
            S3ErrorCode::PreconditionFailed
        );
        assert_eq!(
            pass(None, None, Some(101)).unwrap_err().code().clone(),
            S3ErrorCode::PreconditionFailed
        );
        assert!(pass(None, Some(timestamp(LM)), Some(100)).is_ok());

        // No conditions → pass.
        assert!(pass(None, None, None).is_ok());
    }

    #[test]
    fn delete_conditions_absent_detects_the_trio() {
        let specific = cond(r#""abc""#);
        let lmt = timestamp(1);
        let absent = |im: Option<ETagCondition>, lmt: Option<Timestamp>, size: Option<u64>| {
            DeleteConditions::new(im.as_ref(), lmt, size).absent()
        };
        assert!(absent(None, None, None));
        assert!(!absent(Some(specific.clone()), None, None));
        assert!(!absent(None, Some(lmt.clone()), None));
        assert!(!absent(None, None, Some(0)));
        assert!(!absent(Some(specific), Some(lmt), Some(0)));
    }

    #[test]
    fn checked_if_match_size_rejects_negatives() {
        // The size is compared in the unsigned domain — a negative value
        // is malformed (400), not a precondition failure.
        assert_eq!(
            checked_if_match_size(-1).unwrap_err().code().clone(),
            S3ErrorCode::InvalidArgument
        );
        assert_eq!(checked_if_match_size(0).unwrap(), 0);
        assert_eq!(checked_if_match_size(100).unwrap(), 100);
    }

    #[test]
    fn write_shape_rejects_both_headers_and_specific_inm() {
        // The destination-write shape gate shared by put, copy and the
        // complete (both headers → 400, a specific If-None-Match → 501);
        // the state checkers themselves hold no shape.
        let im_star = cond("*");
        let inm_star = cond("*");
        assert_eq!(
            check_write_shape(Some(&im_star), Some(&inm_star))
                .unwrap_err()
                .code()
                .clone(),
            S3ErrorCode::InvalidRequest
        );
        let inm_abc = cond(r#""abc""#);
        assert_eq!(
            check_write_shape(None, Some(&inm_abc))
                .unwrap_err()
                .code()
                .clone(),
            S3ErrorCode::NotImplemented
        );
        assert!(check_write_shape(Some(&im_star), None).is_ok());
        assert!(check_write_shape(None, Some(&inm_star)).is_ok());
    }

    #[test]
    #[cfg(feature = "multipart")]
    fn complete_conditions_follow_aws_conditional_writes() {
        let e0 = "5d41402abc4b2a76b9719d911017c592";
        let current = Some(info(e0, LM, 100));
        let inm_star = cond("*");
        let check = |im: Option<ETagCondition>, inm: Option<ETagCondition>| {
            check_complete_conditions(im.as_ref(), inm.as_ref(), current.as_ref())
        };

        // If-Match: matching passes; mismatch → 412; missing → 404.
        let im_ok = cond(&format!("\"{e0}\""));
        assert!(check(Some(im_ok), None).is_ok());
        let im_zzz = cond(r#""zzz""#);
        assert_eq!(
            check(Some(im_zzz), None).unwrap_err().code().clone(),
            S3ErrorCode::PreconditionFailed
        );
        let im_any = cond("*");
        assert_eq!(
            check_complete_conditions(Some(&im_any), None, None)
                .unwrap_err()
                .code()
                .clone(),
            S3ErrorCode::NoSuchKey
        );

        // If-None-Match: * — existing → 412, missing → pass.
        assert_eq!(
            check(None, Some(inm_star.clone()))
                .unwrap_err()
                .code()
                .clone(),
            S3ErrorCode::PreconditionFailed
        );
        assert!(check_complete_conditions(None, Some(&inm_star), None).is_ok());
        assert!(check_complete_conditions(None, None, None).is_ok());
    }
}
