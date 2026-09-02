# S3 Conditional-Headers Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify tinio's conditional-header handling (RFC 7232) and fill the missing support: DeleteObject's conditional trio, CompleteMultipartUpload / AbortMultipartUpload conditions, If-Range on GET, write-path both-header 400 rules, and the GET read-path fix (head → check → stream).

**Architecture:** The existing `ConditionalHeaders` evaluator in `crates/tinio-server/src/backend/conditions.rs` stays as the RFC 7232 4-header engine. This plan adds shared helpers next to it (`any()`, `check_missing()`, `parse_if_range`, `to_whole_seconds`) and small AND-composition checkers (`check_delete_conditions`, `check_complete_conditions`) that share the parse helper, the missing-key policy, and the error mapping. All changes are inside `tinio-server` — no storage-contract changes (the tagging plan, `2026-08-31-s3-tagging-ops.md`, owns those).

**Tech Stack:** Rust, s3s 0.15.0 (S3 server framework), tokio, cucumber 0.23 (e2e), redb-backed fs/mem backends (untouched here).

**Spec:** `docs/superpowers/specs/2026-08-31-s3-conditionals-design.md` — the conditional-headers spec. Companion: `docs/superpowers/specs/2026-08-31-s3-tagging-ops-design.md` (tagging/RenameObject/GetObjectAttributes, implemented by Plan B `2026-08-31-s3-tagging-ops.md`).

> **STALENESS MARKER (2026-09-02, concurrency code review):** this plan predates the review fixes recorded in the design doc's "review pass 2026-09-02 #2" header entry and the `tasks.md` Addendum 2026-09-02 #2 (T110-T114). Superseded here: Task 6's read-path sketch (a no-Range conditional GET is now a single fetch + post-check, and the reconciliation gate fires on ETag OR mtime with served-range-guarded, re-validated refetches); Task 5's abort-lock snippet and Task 4's locking rationale (the conditional abort takes no lock; complete's upload fetch and destination head run concurrently); the shared helpers Task 1 names (`any()` → `absent()`; the missing-object and date-comparison rules consolidated; the CMU 501 shape gate generalized to `check_write_shape` over put/copy/complete); and the "no storage-contract changes" architecture claim (EntityTooSmall is now enforced authoritatively in the storage commit — fs verify loop / mem write txn). Where this plan conflicts with the design doc or `tasks.md`, the review entries win.

## Global Constraints

- **English only** — code, comments, feature files, docs (project rule).
- **No git writes** — never `git add`/`commit`/`push`; leave changes in the tree and report at each checkpoint; the user commits (project rule).
- **Task 7 deferred (user decision, 2026-08-31)** — Task 7 (cucumber `conditions.feature`) and Task 8 Steps 4-5 wait for the BDD-migration implementation to complete; the e2e crate is being actively edited and **does not compile today** (in-flight migration: `World.parts` not yet defined) — do not touch it until then. Tasks 1-6 and Task 8 Steps 1-3 proceed now.
- **TDD** — write the failing test, run it to see it fail, implement, run to see it pass (each task's steps).
- **Async tests** — `#[tokio::test]` / `async fn` directly; no `Runtime::block_on` wrappers (project rule).
- **s3s 0.15.0 pinned** — the dto surface (field names, `Timestamp::parse`, `ETagCondition`, `S3ErrorCode`) is the 0.15.0 API; do not "fix" s3s.
- **No version compatibility** — the project is pre-release with no released data; behavior changes (the 400 rules, conditional deletes, the head-first GET) land directly with no compat shims or migration paths.
- **Existing tests stay green** — `cargo test -p tinio-server` on Windows and WSL2; the read-path fix must keep the unconditional-GET fast path and the `range_requests` / `conditional_requests` tests passing.
- **Test scaffold vocabulary** (already in the codebase, reused verbatim): `setup()`/`setup_name()` (backend + bucket), `s3_request(dto::Input)`, `body(&[u8])`, `read_body(...)`, `upload_part_copy_input(&b, &upload_id, n)`, `etag("...")` / `cond("...")` parsers in `conditions.rs` tests.

---

### Task 1: Shared machinery in `conditions.rs`

**Files:**
- Modify: `crates/tinio-server/src/backend/conditions.rs` (helpers + tests)
- Modify: `crates/tinio-server/src/backend/objects.rs:33-46` (delete the moved `parse_etag_condition_header`, import it from `conditions`)
- Modify: `crates/tinio-server/src/backend/mod.rs` (re-export the new items if `mod.rs` lists them individually — check how `ConditionalHeaders`/`condition_error` are re-exported and mirror it)

**Interfaces:**
- Consumes: nothing new — the existing `ConditionalHeaders`, `ConditionFailure`, `condition_error`, `to_system_time`, `dto::ETagCondition`, `dto::Timestamp`.
- Produces (used by Tasks 2-6):
  - `pub(crate) fn parse_etag_condition_header(headers: &http::HeaderMap, name: &'static str) -> Result<Option<dto::ETagCondition>, S3Error>` — moved verbatim from `objects.rs:33-46`.
  - `impl ConditionalHeaders { pub(crate) fn any(&self) -> bool }` — true iff all four headers are `None`.
  - `impl ConditionalHeaders { pub(crate) fn check_missing(self, write_path: bool) -> S3Result<()> }` — `If-Match` present → 412 via `condition_error(ConditionFailure::Match, write_path)`; otherwise `Ok(())`.
  - `pub(crate) fn reject_both_etag_headers(if_match: Option<&dto::ETagCondition>, if_none_match: Option<&dto::ETagCondition>) -> S3Result<()>` — the write-path both-present rule (AWS conditional writes): both headers in one request → 400 `InvalidRequest`. The ops call it **up front, before any I/O** (PutObject before `stage_body`, CopyObject before the source head, CMU before the parts parse and the lock — a request-shape error must not pay the body stream). The destination checkers keep their own 400 branch too: the checker remains the unit-tested authority; the op placement just rejects earlier.
  - `pub(crate) enum IfRange { Etag(dto::ETag), Date(dto::Timestamp) }` with `pub(crate) fn matches(&self, etag: &ETag, last_modified: SystemTime) -> bool` — strong ETag compare, or second-precision date equality.
  - `pub(crate) fn parse_if_range(headers: &http::HeaderMap) -> Option<IfRange>` — ETag (wildcard `*` → `None`), else HTTP-date via `dto::Timestamp::parse(dto::TimestampFormat::HttpDate, text)`; any failure → `None` (header ignored).
  - `pub(crate) fn to_whole_seconds(t: SystemTime) -> u64` — `t.duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)`.
  - `to_system_time` becomes `pub(crate)` (Task 5's abort path uses it from `multipart.rs`).

- [ ] **Step 1: Write the failing tests** in `conditions.rs`'s test module (append after `condition_error_maps_failures`):

```rust
#[test]
fn any_true_only_when_every_header_is_absent() {
    assert!(ConditionalHeaders::new(None, None, None, None).any());
    assert!(!ConditionalHeaders::new(Some(cond(r#""abc""#)), None, None, None).any());
    assert!(!ConditionalHeaders::new(None, Some(cond("*")), None, None).any());
    assert!(!ConditionalHeaders::new(None, None, Some(timestamp(1)), None).any());
    assert!(!ConditionalHeaders::new(None, None, None, Some(timestamp(1))).any());
}

#[test]
fn check_missing_fails_only_if_match() {
    let im = ConditionalHeaders::new(Some(cond(r#""abc""#)), None, None, None);
    assert_eq!(
        im.check_missing(true).unwrap_err().code().clone(),
        S3ErrorCode::PreconditionFailed
    );
    let inm = ConditionalHeaders::new(None, Some(cond("*")), None, None);
    assert!(inm.check_missing(true).is_ok(), "If-None-Match passes on a missing object");
    let none = ConditionalHeaders::new(None, None, None, None);
    assert!(none.check_missing(true).is_ok());
}

#[test]
fn if_range_parses_etag_date_and_ignores_garbage() {
    let mut headers = http::HeaderMap::new();
    assert_eq!(parse_if_range(&headers), None, "absent header");
    headers.insert("if-range", r#""abc""#.parse().unwrap());
    assert!(matches!(parse_if_range(&headers), Some(IfRange::Etag(_))));
    headers.insert("if-range", "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap());
    assert!(matches!(parse_if_range(&headers), Some(IfRange::Date(_))));
    // RFC 9110 §13.1.5: a wildcard is not a valid If-Range value.
    headers.insert("if-range", "*".parse().unwrap());
    assert_eq!(parse_if_range(&headers), None);
    headers.insert("if-range", "not a date".parse().unwrap());
    assert_eq!(parse_if_range(&headers), None);
}

#[test]
fn if_range_matches_strong_etag_and_second_precision_date() {
    let e = etag("5d41402abc4b2a76b9719d911017c592");
    let t = SystemTime::UNIX_EPOCH + Duration::from_secs(LM);
    let etag_of = |v: &str| cond(v).as_etag().unwrap().clone();
    assert!(IfRange::Etag(etag_of(r#""5d41402abc4b2a76b9719d911017c592""#)).matches(&e, t));
    assert!(!IfRange::Etag(etag_of(r#""zzz""#)).matches(&e, t));
    // A weak tag never strong-matches.
    assert!(!IfRange::Etag(etag_of(r#"W/"5d41402abc4b2a76b9719d911017c592""#)).matches(&e, t));
    // Date — RFC 9110 §13.1.5 match rule, NOT equality: a date matches
    // when `last_modified <= header_date` (an equal or later date
    // matches, a future date matches, only an older date fails).
    assert!(IfRange::Date(timestamp(LM)).matches(&e, t));
    assert!(IfRange::Date(timestamp(LM + 1)).matches(&e, t));
    assert!(!IfRange::Date(timestamp(LM - 1)).matches(&e, t));
}

#[test]
fn reject_both_etag_headers_is_400_only_when_both_present() {
    let im = Some(cond("*"));
    let inm = Some(cond("*"));
    assert_eq!(
        reject_both_etag_headers(im.as_ref(), inm.as_ref())
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::InvalidRequest
    );
    assert!(reject_both_etag_headers(im.as_ref(), None).is_ok());
    assert!(reject_both_etag_headers(None, inm.as_ref()).is_ok());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-server conditions::tests::any_true conditions::tests::check_missing conditions::tests::if_range conditions::tests::reject_both` (or the whole conditions module: `cargo test -p tinio-server backend::conditions`)
Expected: FAIL — `any`, `check_missing`, `IfRange`, `parse_if_range`, `to_whole_seconds`, `reject_both_etag_headers` not found.

- [ ] **Step 3: Implement the helpers** in `conditions.rs` (after `ConditionalHeaders::check`; `parse_etag_condition_header` is moved here from `objects.rs:33-46`):

```rust
impl ConditionalHeaders<'_> {
    /// True when every header is absent — the fast-path decision (skip
    /// the head) for the destination and read paths.
    pub(crate) fn any(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.if_modified_since.is_none()
            && self.if_unmodified_since.is_none()
    }

    /// The missing-object policy (RFC 7232): only `If-Match` can fail
    /// against an absent object (412); every other condition passes —
    /// create-if-absent on the destination paths, idempotent 204 on
    /// delete. `write_path` is passed through to `condition_error` for
    /// signature symmetry with `check`.
    pub(crate) fn check_missing(self, write_path: bool) -> S3Result<()> {
        if self.if_match.is_some() {
            return Err(condition_error(ConditionFailure::Match, write_path));
        }
        Ok(())
    }
}
```

```rust
/// The write-path both-present rule (AWS conditional writes): `If-Match`
/// and `If-None-Match` in one request are a request-shape error — 400
/// `InvalidRequest`. The ops call this up front, before any body is
/// staged or any lock is taken; the destination checkers keep their own
/// branch so the rule stays unit-tested at the checker level too.
pub(crate) fn reject_both_etag_headers(
    if_match: Option<&dto::ETagCondition>,
    if_none_match: Option<&dto::ETagCondition>,
) -> S3Result<()> {
    if if_match.is_some() && if_none_match.is_some() {
        return Err(s3_error!(InvalidRequest,
            "If-Match and If-None-Match cannot both be present"));
    }
    Ok(())
}

/// Parse an ETag-condition header (`x-amz-if-match`, `x-amz-if-none-match`)
/// into the DTO type when present. CopyObject's destination conditionals
/// are not part of the s3s DTO, so they are read from the headers here.
pub(crate) fn parse_etag_condition_header(
    headers: &http::HeaderMap,
    name: &'static str,
) -> Result<Option<dto::ETagCondition>, S3Error> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let text = value
        .to_str()
        .map_err(|_| s3_error!(InvalidArgument, "invalid {name} header"))?;
    ETagCondition::from_str(text)
        .map(Some)
        .map_err(|_| s3_error!(InvalidArgument, "invalid {name} header"))
}

/// The `If-Range` value (RFC 9110 §13.1.5): an entity-tag or an
/// HTTP-date. The wildcard is not a valid value; anything unparseable
/// is ignored (the header is dropped, serving the Range as usual).
#[derive(Debug, Clone)]
pub(crate) enum IfRange {
    Etag(dto::ETag),
    Date(dto::Timestamp),
}

impl IfRange {
    /// True when the current representation still matches the If-Range
    /// condition: a strong ETag comparison, or the RFC 9110 §13.1.5 date
    /// match rule — a date matches when `last_modified <= header_date`
    /// (whole-second precision; an equal or later date matches, a future
    /// date matches, only an older date fails). NOT the equality
    /// semantics of `if_match_last_modified_time`.
    pub(crate) fn matches(&self, etag: &ETag, last_modified: SystemTime) -> bool {
        match self {
            IfRange::Etag(e) => e.strong_cmp(&WireETag::Strong(etag.as_str().to_string())),
            IfRange::Date(d) => {
                to_whole_seconds(last_modified) <= to_whole_seconds(to_system_time(*d))
            }
        }
    }
}

/// Parse the `If-Range` header; `None` when absent, invalid, or `*`.
pub(crate) fn parse_if_range(headers: &http::HeaderMap) -> Option<IfRange> {
    let value = headers.get("if-range")?.to_str().ok()?;
    match value.parse::<dto::ETagCondition>() {
        Ok(cond) => cond.as_etag().cloned().map(IfRange::Etag),
        Err(_) => dto::Timestamp::parse(dto::TimestampFormat::HttpDate, value)
            .ok()
            .map(IfRange::Date),
    }
}

/// A `SystemTime` truncated to whole seconds (the Last-Modified wire
/// precision) — conditional timestamp comparisons.
pub(crate) fn to_whole_seconds(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
```

Imports needed at the top of `conditions.rs` (add to the existing `use` list): `std::str::FromStr`, `http::HeaderMap` (as `http::HeaderMap` — check how `objects.rs` refers to it; the move keeps `&http::HeaderMap`), and `s3s::dto::ETagCondition` is already reachable via `dto::{self, ETag as WireETag}` — reference it as `dto::ETagCondition` and `FromStr` must be in scope (`std::str::FromStr`).

- [ ] **Step 4: Delete the moved helper from `objects.rs`** — remove lines 30-46 (the `parse_etag_condition_header` fn and its doc comment) and add `parse_etag_condition_header` to the `crate::backend::{...}` import in `objects.rs` (it already imports `ConditionFailure, ConditionalHeaders, S3Backend, byte_range, condition_error, map_backend_error`).

- [ ] **Step 5: Run the full tinio-server test suite**

Run: `cargo test -p tinio-server`
Expected: PASS (all existing tests plus the four new ones).

- [ ] **Step 6: Checkpoint** — report the changed files and the new helpers to the user; do not commit.

---

### Task 2: Destination conditions converge on `any()` / `check_missing`, both-header 400

**Files:**
- Modify: `crates/tinio-server/src/backend/objects.rs:55-86` (`check_destination_conditions`)
- Test: `crates/tinio-server/src/backend/objects.rs` test module (append to `conditional_put_failures_are_412`'s module)

**Interfaces:**
- Consumes: `ConditionalHeaders::any()`, `ConditionalHeaders::check_missing()`, `reject_both_etag_headers()` (Task 1).
- Produces: the shared missing-key policy for Put/Copy destinations; the both-present → 400 rule, validated **up front by the callers** (before any body is staged — request-shape error), which Task 4's CMU checker also implements.

- [ ] **Step 1: Write the failing test** (append to the objects.rs test module):

```rust
#[tokio::test]
async fn destination_conditions_with_both_etag_headers_is_400() {
    let (backend, b) = setup_name().await;
    backend
        .storage()
        .put_object(&b, &"hello.txt".into(), body(b"hello"))
        .await
        .unwrap();
    // AWS conditional writes reject If-Match + If-None-Match together.
    let err = backend
        .put_object(s3_request(dto::PutObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            body: Some(StreamingBlob::wrap(stream::once(async {
                Ok::<_, io::Error>(Bytes::from_static(b"hello"))
            }))),
            if_match: Some("*".parse().unwrap()),
            if_none_match: Some("*".parse().unwrap()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "InvalidRequest");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-server destination_conditions_with_both_etag_headers_is_400`
Expected: FAIL — the current code evaluates both headers (If-Match `*` passes on an existing object, so the request succeeds, and the test's `unwrap_err` panics).

- [ ] **Step 3: Refactor `check_destination_conditions`** — replace the body of `objects.rs:55-86` with:

```rust
    /// The destination-conditional protocol (`x-amz-if-match` /
    /// `x-amz-if-none-match`): evaluate against the CURRENT object at
    /// (bucket, key), 412 on failure. Shared by the conditional put and
    /// the conditional copy — a missing object is the "no current
    /// version" case (If-None-Match: *); any real failure must not look
    /// like an absent object, or the precondition would pass and
    /// overwrite. The both-present 400 is NOT here — the callers call
    /// `reject_both_etag_headers` up front (request-shape error, before
    /// the body is staged); this checker keeps only the state-dependent
    /// part.
    async fn check_destination_conditions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        if_match: Option<&dto::ETagCondition>,
        if_none_match: Option<&dto::ETagCondition>,
    ) -> S3Result<()> {
        let conditions = ConditionalHeaders::new(if_match, if_none_match, None, None);
        // No conditions ⇒ the fast path (skip the head) — `any()` folds
        // the old early return into the evaluator.
        if conditions.any() {
            return Ok(());
        }
        let current = match self.storage.head_object(bucket, key).await {
            Ok(info) => Some(info),
            Err(err) => {
                let err: StorageError = err.into();
                match err {
                    StorageError::NoSuchKey(_) => None,
                    err => return Err(map_backend_error(err)),
                }
            }
        };
        if let Some(info) = current {
            conditions.check(&info.etag, info.last_modified, true)?;
        } else {
            conditions.check_missing(true)?;
        }
        Ok(())
    }
```

The 400 is validated at the top of both callers, before any I/O:

- `op_put_object` (`objects.rs:101`): right after the bucket/key parse, **before `stage_body`** — a rejected request must not pay the body stream. `op_post_object` inherits the check via the put delegation (the s3s default `post_object` maps into `put_object`).
- `op_copy_object` (`objects.rs:303`): the destination headers are parsed at the top of the op (a pure header parse, no I/O), then `reject_both_etag_headers` runs **before the source head** and the destination lock.

The copy-source family (`copy_source_if_match` + `copy_source_if_none_match`, CopyObject and UploadPartCopy) is NOT subject to the rule — it keeps today's RFC 9110 §13.2.2 evaluation order (If-Match first, then If-None-Match), no 400 (AWS documents no such restriction for the copy-source headers). The position is locked by a cucumber scenario in Task 7.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS — the new test plus `conditional_put_failures_are_412`, `copy_object_destination_conditionals_are_enforced`, `conditional_requests` all green (the missing-key policy is unchanged: If-Match → 412, If-None-Match → pass).

- [ ] **Step 5: Checkpoint** — report; do not commit.

---

### Task 3: DeleteObject conditional trio

**Files:**
- Modify: `crates/tinio-server/src/backend/conditions.rs` (`check_delete_conditions` + tests)
- Modify: `crates/tinio-server/src/backend/objects.rs:217-233` (`op_delete_object`)
- Test: `crates/tinio-server/src/backend/objects.rs` test module

**Interfaces:**
- Consumes: `to_whole_seconds` (Task 1), `condition_error`/`ConditionFailure` (existing), `StorageError`, `map_backend_error` (existing).
- Produces: `pub(crate) fn check_delete_conditions(if_match: Option<&dto::ETagCondition>, if_match_last_modified_time: Option<dto::Timestamp>, if_match_size: Option<i64>, etag: &ETag, last_modified: SystemTime, size: u64) -> S3Result<()>` — all provided conditions must pass (AND); every failure maps to 412.

- [ ] **Step 1: Write the failing checker tests** in `conditions.rs`:

```rust
#[test]
fn delete_conditions_require_every_provided_header() {
    let e = etag("5d41402abc4b2a76b9719d911017c592");
    let t = SystemTime::UNIX_EPOCH + Duration::from_secs(LM);
    let im = |v: &str| Some(cond(v));

    // All three matching → pass.
    assert!(check_delete_conditions(
        im(r#""5d41402abc4b2a76b9719d911017c592""#),
        Some(timestamp(LM)),
        Some(100),
        &e,
        t,
        100,
    )
    .is_ok());

    // ETag mismatch → 412; weak tag never matches; wildcard matches.
    assert_eq!(
        check_delete_conditions(im(r#""zzz""#), None, None, &e, t, 100)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::PreconditionFailed
    );
    assert_eq!(
        check_delete_conditions(im(r#"W/"5d41402abc4b2a76b9719d911017c592""#), None, None, &e, t, 100)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::PreconditionFailed
    );
    assert!(check_delete_conditions(im("*"), None, None, &e, t, 100).is_ok());

    // Last-modified-time and size compare exactly (second precision).
    assert_eq!(
        check_delete_conditions(None, Some(timestamp(LM + 1)), None, &e, t, 100)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::PreconditionFailed
    );
    assert_eq!(
        check_delete_conditions(None, None, Some(101), &e, t, 100)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::PreconditionFailed
    );
    assert!(check_delete_conditions(None, Some(timestamp(LM)), Some(100), &e, t, 100).is_ok());

    // A negative size is malformed, not a precondition failure.
    assert_eq!(
        check_delete_conditions(None, None, Some(-1), &e, t, 100)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::InvalidArgument
    );

    // No conditions → pass.
    assert!(check_delete_conditions(None, None, None, &e, t, 100).is_ok());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p tinio-server delete_conditions_require_every_provided_header`
Expected: FAIL — `check_delete_conditions` not found.

- [ ] **Step 3: Implement `check_delete_conditions`** in `conditions.rs` (after `parse_if_range`):

```rust
/// DeleteObject's conditional trio (ETag / last-modified-time / size):
/// every provided condition must pass (AND). ETags compare strong (`*`
/// matches any existing object); the timestamps compare at whole-second
/// precision; the size compares exactly. Every failure answers 412 —
/// the same `ConditionFailure::Match` mapping as the evaluator.
pub(crate) fn check_delete_conditions(
    if_match: Option<&dto::ETagCondition>,
    if_match_last_modified_time: Option<dto::Timestamp>,
    if_match_size: Option<i64>,
    etag: &ETag,
    last_modified: SystemTime,
    size: u64,
) -> S3Result<()> {
    if let Some(cond) = if_match {
        let ok = cond.is_any()
            || cond
                .as_etag()
                .map(|e| e.strong_cmp(&WireETag::Strong(etag.as_str().to_string())))
                .unwrap_or(false);
        if !ok {
            return Err(condition_error(ConditionFailure::Match, true));
        }
    }
    if let Some(t) = if_match_last_modified_time
        && to_whole_seconds(last_modified) != to_whole_seconds(to_system_time(t))
    {
        return Err(condition_error(ConditionFailure::Match, true));
    }
    if let Some(size_cond) = if_match_size
        && size != u64::try_from(size_cond).map_err(|_| s3_error!(InvalidArgument, "invalid If-Match-Size"))?
    {
        return Err(condition_error(ConditionFailure::Match, true));
    }
    Ok(())
}
```

- [ ] **Step 4: Rewrite `op_delete_object`** (`objects.rs:217-233`) — conditional delete with the missing-key policy:

```rust
    pub(crate) async fn op_delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        // Serialize with the write lock: a delete landing between a
        // conditional put's check and commit must not erase the state
        // the precondition was evaluated against.
        let _guard = self.lock_object(&bucket, &key).await;
        // Conditional delete: ETag / last-modified-time / size, all
        // provided headers must match. Unconditional deletes stay
        // idempotent (missing objects still answer 204); a missing
        // object with If-Match → 412, with only the date/size headers
        // → 204 (AWS treats these two as idempotent on a missing
        // object).
        let if_match = req.input.if_match.as_ref();
        let lmt = req.input.if_match_last_modified_time;
        let size = req.input.if_match_size;
        if if_match.is_some() || lmt.is_some() || size.is_some() {
            match self.storage.head_object(&bucket, &key).await {
                Ok(info) => check_delete_conditions(if_match, lmt, size, &info.etag, info.last_modified, info.size)?,
                Err(err) => {
                    let err: StorageError = err.into();
                    match err {
                        StorageError::NoSuchKey(_) if if_match.is_some() => {
                            return Err(condition_error(ConditionFailure::Match, true));
                        }
                        StorageError::NoSuchKey(_) => {}
                        err => return Err(map_backend_error(err)),
                    }
                }
            }
        }
        self.storage
            .delete_object(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }
```

Add `check_delete_conditions` to the `crate::backend::{...}` import in `objects.rs`.

- [ ] **Step 5: Write the op-level test** (append to the objects.rs test module):

```rust
#[tokio::test]
async fn conditional_delete_enforces_the_trio() {
    let (backend, b) = setup_name().await;
    let etag = "5d41402abc4b2a76b9719d911017c592";
    backend
        .storage()
        .put_object(&b, &"hello.txt".into(), body(b"hello"))
        .await
        .unwrap();

    // Matching conditions delete (204).
    backend
        .delete_object(s3_request(dto::DeleteObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            if_match: Some(format!("\"{etag}\"").parse().unwrap()),
            ..Default::default()
        }))
        .await
        .unwrap();

    // The object is gone: If-Match now fails with 412 (not 404).
    let err = backend
        .delete_object(s3_request(dto::DeleteObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            if_match: Some(format!("\"{etag}\"").parse().unwrap()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");

    // Only the date/size conditions on a missing object stay idempotent 204.
    backend
        .delete_object(s3_request(dto::DeleteObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            if_match_last_modified_time: Some(Timestamp::from(
                OffsetDateTime::from_unix_timestamp(0).unwrap(),
            )),
            if_match_size: Some(0),
            ..Default::default()
        }))
        .await
        .unwrap();

    // A mismatching size on an existing object → 412.
    backend
        .storage()
        .put_object(&b, &"hello.txt".into(), body(b"hello"))
        .await
        .unwrap();
    let err = backend
        .delete_object(s3_request(dto::DeleteObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            if_match_size: Some(999),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");

    // A mismatching last-modified-time on an existing object → 412.
    let err = backend
        .delete_object(s3_request(dto::DeleteObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            if_match_last_modified_time: Some(Timestamp::from(
                OffsetDateTime::from_unix_timestamp(0).unwrap(),
            )),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");
}
```

(The `Timestamp` / `OffsetDateTime` imports already exist in the objects.rs test module — `conditional_requests` uses them.)

- [ ] **Step 6: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS.

- [ ] **Step 7: Checkpoint** — report; do not commit.

---

### Task 4: CompleteMultipartUpload destination conditions

**Files:**
- Modify: `crates/tinio-server/src/backend/conditions.rs` (`check_complete_conditions` + tests)
- Modify: `crates/tinio-server/src/backend/multipart.rs:496-506` (condition block at the top of `op_complete_multipart_upload`)

**Interfaces:**
- Consumes: `ConditionalHeaders` (existing), `s3_error!` (existing).
- Produces: `pub(crate) fn check_complete_conditions(if_match: Option<&dto::ETagCondition>, if_none_match: Option<&dto::ETagCondition>, current: Option<(&ETag, SystemTime)>) -> S3Result<()>` — AWS conditional-writes semantics for CMU: both headers → 400 `InvalidRequest`; non-`*` If-None-Match → 501 `NotImplemented`; `If-Match` against the current object (missing → 404 `NoSuchKey`, mismatch → 412); `If-None-Match: *` (existing → 412, missing → pass).

- [ ] **Step 1: Write the failing checker tests** in `conditions.rs`:

```rust
#[test]
fn complete_conditions_follow_aws_conditional_writes() {
    let e = etag("5d41402abc4b2a76b9719d911017c592");
    let t = SystemTime::UNIX_EPOCH + Duration::from_secs(LM);
    let im = |v: &str| Some(cond(v));
    let inm = |v: &str| Some(cond(v));
    let current = Some((&e, t));

    // Both headers in one request → 400.
    assert_eq!(
        check_complete_conditions(im("*"), inm("*"), current)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::InvalidRequest
    );
    // If-None-Match accepts `*` only.
    assert_eq!(
        check_complete_conditions(None, inm(r#""abc""#), current)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::NotImplemented
    );

    // If-Match: matching passes; mismatch → 412; missing → 404.
    assert!(check_complete_conditions(im(r#""5d41402abc4b2a76b9719d911017c592""#), None, current).is_ok());
    assert_eq!(
        check_complete_conditions(im(r#""zzz""#), None, current)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::PreconditionFailed
    );
    assert_eq!(
        check_complete_conditions(im("*"), None, None)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::NoSuchKey
    );

    // If-None-Match: * — existing → 412, missing → pass.
    assert_eq!(
        check_complete_conditions(None, inm("*"), current)
            .unwrap_err()
            .code()
            .clone(),
        S3ErrorCode::PreconditionFailed
    );
    assert!(check_complete_conditions(None, inm("*"), None).is_ok());
    assert!(check_complete_conditions(None, None, None).is_ok());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p tinio-server complete_conditions_follow_aws_conditional_writes`
Expected: FAIL — `check_complete_conditions` not found.

- [ ] **Step 3: Implement `check_complete_conditions`** in `conditions.rs`:

```rust
/// CompleteMultipartUpload's destination conditions (AWS conditional
/// writes, verified 2026-08-31): the checks evaluate against the
/// object CURRENTLY at the key (the one being replaced) — not the
/// composed ETag of the completing upload. `If-Match` strong-compares
/// (`*` = any existing object); a missing destination → 404
/// `NoSuchKey`, a mismatch → 412. `If-None-Match` accepts `*` only —
/// missing destination passes, an existing object → 412. Both headers
/// in one request → 400; a specific If-None-Match value → 501
/// `NotImplemented` (AWS: "a header you provided implies functionality
/// that is not implemented").
pub(crate) fn check_complete_conditions(
    if_match: Option<&dto::ETagCondition>,
    if_none_match: Option<&dto::ETagCondition>,
    current: Option<(&ETag, SystemTime)>,
) -> S3Result<()> {
    if if_match.is_some() && if_none_match.is_some() {
        return Err(s3_error!(InvalidRequest,
            "If-Match and If-None-Match cannot both be present"));
    }
    if let Some(cond) = if_none_match
        && !cond.is_any()
    {
        return Err(s3_error!(NotImplemented,
            "If-None-Match only accepts '*' (AWS: a specific ETag is not implemented)"));
    }
    match current {
        Some((etag, last_modified)) => {
            ConditionalHeaders::new(if_match, if_none_match, None, None)
                .check(etag, last_modified, true)
        }
        None if if_match.is_some() => Err(s3_error!(NoSuchKey, "the destination object does not exist")),
        None => Ok(()),
    }
}
```

- [ ] **Step 4: Wire it into `op_complete_multipart_upload`** — two placements: the both-present 400 goes at the **top of the op** (after the bucket/key parse, **before the parts parse and the lock** — request shape, no state needed), and the head-check block goes right after the existing lock (`multipart.rs:542`, the `let _guard = self.lock_object(&bucket, &key).await;` line), before the upload-state fetch:

```rust
        // Both-present 400 up front (request shape): before the parts
        // parse and the lock — a rejected request must not pay for them.
        reject_both_etag_headers(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
        )?;
        // Destination conditionals (AWS conditional writes): If-Match /
        // If-None-Match against the object currently at the key —
        // atomic with the complete under the write lock (the head-check
        // runs INSIDE the lock, after the parts parse). Missing
        // destination + If-Match → 404 NoSuchKey. `check_complete_conditions`
        // keeps its own both-present 400 branch — the checker is the
        // unit-tested authority; the op placement above just rejects
        // earlier.
        if req.input.if_match.is_some() || req.input.if_none_match.is_some() {
            let current = match self.storage.head_object(&bucket, &key).await {
                Ok(info) => Some((info.etag, info.last_modified)),
                Err(err) => {
                    let err: StorageError = err.into();
                    match err {
                        StorageError::NoSuchKey(_) => None,
                        err => return Err(map_backend_error(err)),
                    }
                }
            };
            check_complete_conditions(
                req.input.if_match.as_ref(),
                req.input.if_none_match.as_ref(),
                current.as_ref().map(|(e, t)| (e, *t)),
            )?;
        }
```

Add `check_complete_conditions` to the `crate::backend::{...}` import in `multipart.rs` (it already imports `ConditionalHeaders, S3Backend, byte_range, ...`).

- [ ] **Step 5: Write the op-level test** in the multipart.rs test module (modeled on `upload_part_copy_range_and_conditionals` at `multipart.rs:1289`):

```rust
#[tokio::test]
async fn complete_multipart_upload_honors_destination_conditions() {
    let (backend, b) = setup().await;
    let create = |key: &str| {
        backend.create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
            bucket: b.clone(),
            key: key.into(),
            ..Default::default()
        }))
    };
    let upload_one_part = async |key: &str, upload_id: &str| {
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: key.into(),
                upload_id: upload_id.into(),
                part_number: 1,
                body: Some(StreamingBlob::wrap(stream::once(async {
                    Ok::<_, io::Error>(Bytes::from_static(b"hello"))
                }))),
                ..Default::default()
            }))
            .await
            .unwrap();
        part.output.e_tag.unwrap()
    };
    // `CompletedPart.e_tag` is `Option<dto::ETag>` in s3s 0.15 (the
    // Strong/Weak enum, not a String) — the closure takes the wire ETag
    // that `upload_one_part` already returns.
    let complete = |key: &str, upload_id: &str, etag: dto::ETag, if_match: Option<dto::IfMatch>, if_none_match: Option<dto::IfNoneMatch>| {
        backend.complete_multipart_upload(s3_request(dto::CompleteMultipartUploadInput {
            bucket: b.clone(),
            key: key.into(),
            upload_id: upload_id.into(),
            multipart_upload: Some(dto::CompletedMultipartUpload {
                parts: Some(vec![dto::CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(etag),
                    ..Default::default()
                }]),
            }),
            if_match,
            if_none_match,
            ..Default::default()
        }))
    };

    // If-None-Match: * succeeds on a fresh key.
    let create1 = create("cond.bin").await.unwrap();
    let id1 = create1.output.upload_id.unwrap();
    let etag1 = upload_one_part("cond.bin", &id1).await;
    complete("cond.bin", &id1, etag1.clone(), None, Some("*".parse().unwrap()))
        .await
        .unwrap();

    // A second complete of the same key with If-None-Match: * → 412.
    let create2 = create("cond.bin").await.unwrap();
    let id2 = create2.output.upload_id.unwrap();
    let etag2 = upload_one_part("cond.bin", &id2).await;
    let err = complete("cond.bin", &id2, etag2, None, Some("*".parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");

    // Both headers → 400; a specific If-None-Match → 501.
    let err = complete("cond.bin", &id2, etag1.clone(), Some("*".parse().unwrap()), Some("*".parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "InvalidRequest");
    let err = complete("cond.bin", &id2, etag1, None, Some(r#""abc""#.parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NotImplemented");
}
```

(The test module of `multipart.rs` already imports `dto`, `StreamingBlob`, `stream`, `Bytes`, `io` — check its `use super::*` + top-of-module imports and mirror the `upload_part_copy_range_and_conditionals` test's usage.)

- [ ] **Step 6: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS.

- [ ] **Step 7: Checkpoint** — report; do not commit.

---

### Task 5: AbortMultipartUpload initiated-time condition

**Files:**
- Modify: `crates/tinio-server/src/backend/multipart.rs:736-748` (`op_abort_multipart_upload`)
- Test: `crates/tinio-server/src/backend/multipart.rs` test module

**Interfaces:**
- Consumes: `to_whole_seconds` (Task 1), `self.storage.get_multipart_upload` (existing contract, returns `MultipartUpload { initiated_at: SystemTime, .. }`).
- Produces: the abort path serializes with the per-key lock and honors `if_match_initiated_time` (second-precision equality → else 412).

- [ ] **Step 1: Write the failing test** (append to the multipart.rs test module):

```rust
#[tokio::test]
async fn abort_multipart_upload_honors_if_match_initiated_time() {
    let (backend, b) = setup().await;
    let create = backend
        .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
            bucket: b.clone(),
            key: "abort.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let upload_id = create.output.upload_id.unwrap();
    // s3s 0.15's CreateMultipartUploadOutput has NO `initiated` field —
    // the storage read is the source of the wire timestamp.
    let initiated = dto::Timestamp::from(
        backend
            .storage()
            .get_multipart_upload(
                &bucket::name(&b).unwrap(),
                &object::key("abort.bin").unwrap(),
                &upload_id,
            )
            .await
            .unwrap()
            .initiated_at,
    );

    // A matching initiated time aborts (204).
    backend
        .abort_multipart_upload(s3_request(dto::AbortMultipartUploadInput {
            bucket: b.clone(),
            key: "abort.bin".into(),
            upload_id: upload_id.clone(),
            if_match_initiated_time: Some(initiated),
            ..Default::default()
        }))
        .await
        .unwrap();

    // A stale time → 412; the upload still exists.
    let create = backend
        .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
            bucket: b.clone(),
            key: "abort.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let upload_id = create.output.upload_id.unwrap();
    let err = backend
        .abort_multipart_upload(s3_request(dto::AbortMultipartUploadInput {
            bucket: b.clone(),
            key: "abort.bin".into(),
            upload_id: upload_id.clone(),
            if_match_initiated_time: Some(dto::Timestamp::from(
                time::OffsetDateTime::from_unix_timestamp(0).unwrap(),
            )),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");

    // A missing upload answers NoSuchUpload regardless of the condition.
    let err = backend
        .abort_multipart_upload(s3_request(dto::AbortMultipartUploadInput {
            bucket: b.clone(),
            key: "abort.bin".into(),
            upload_id: "missing".into(),
            if_match_initiated_time: Some(dto::Timestamp::from(
                time::OffsetDateTime::from_unix_timestamp(0).unwrap(),
            )),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NoSuchUpload");
}
```

(Note: `CreateMultipartUploadOutput` has no `initiated` field in s3s 0.15 — the test reads `initiated_at` from the storage via `MultipartOps::get_multipart_upload` and converts it with `dto::Timestamp::from(SystemTime)` (the test module already imports `bucket`, `object`, `MultipartOps`, and `dto`). `time::OffsetDateTime` is still needed for the stale-time literals — import it like the objects.rs tests do if the module lacks it.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p tinio-server abort_multipart_upload_honors_if_match_initiated_time`
Expected: FAIL — the condition is ignored, so the stale-time abort answers 204 and `unwrap_err` panics.

- [ ] **Step 3: Implement** — replace `op_abort_multipart_upload` (`multipart.rs:736-748`):

```rust
    pub(crate) async fn op_abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        // Serialize with the write lock: the initiated-time check and
        // the abort must be atomic against a concurrent complete.
        let _guard = self.lock_object(&bucket, &key).await;
        // If-Match-Initiated-Time: the upload must still be the one the
        // client started (second precision). A missing upload answers
        // NoSuchUpload regardless of the condition.
        if let Some(t) = req.input.if_match_initiated_time {
            let upload = self
                .storage
                .get_multipart_upload(&bucket, &key, &req.input.upload_id)
                .await
                .map_err(map_backend_error)?;
            if to_whole_seconds(upload.initiated_at) != to_whole_seconds(to_system_time(t)) {
                return Err(s3_error!(PreconditionFailed, "If-Match-Initiated-Time failed"));
            }
        }
        self.storage
            .abort_multipart_upload(&bucket, &key, &req.input.upload_id)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }
```

Add `to_whole_seconds` and `to_system_time` to the imports from `conditions.rs` in `multipart.rs` — check how `conditions.rs`'s `to_system_time` is exported (it is currently `fn` private; make it `pub(crate)` in Task 1) and re-export both through `backend/mod.rs` or import them directly (`crate::backend::conditions::{to_system_time, to_whole_seconds}` — mirror the existing import style).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS.

- [ ] **Step 5: Checkpoint** — report; do not commit.

---

### Task 6: If-Range on GET + read-path fix

**Files:**
- Modify: `crates/tinio-server/src/backend/objects.rs:139-187` (`op_get_object`)
- Test: `crates/tinio-server/src/backend/objects.rs` test module

**Interfaces:**
- Consumes: `parse_if_range`, `IfRange::matches`, `ConditionalHeaders::any()` (Task 1).
- Produces: the new GET flow — head-first when a conditional header or an If-Range-over-Range is present; the body fetch then uses the Range decided by If-Range. The unconditional-GET fast path (no conditions, no If-Range) is unchanged.

- [ ] **Step 1: Write the failing If-Range test** — s3s 0.15's `GetObjectInput` has no If-Range field, but `S3Request.headers` is public and the `s3_request` helper leaves it mutable — the same header injection the copy-destination tests already use (`req.headers.insert(...)`, objects.rs:804). This is a real red test: today's code ignores If-Range, so the stale case answers 206 with the range body, not the full 200.

```rust
#[tokio::test]
async fn get_object_if_range_gates_the_range() {
    let (backend, b) = setup_name().await;
    let etag = "5d41402abc4b2a76b9719d911017c592";
    backend
        .storage()
        .put_object(&b, &"hello.txt".into(), body(b"hello"))
        .await
        .unwrap();
    // Matching validator → the Range is honored (206).
    let mut req = s3_request(dto::GetObjectInput {
        bucket: b.to_string(),
        key: "hello.txt".into(),
        range: Some(Range::Int { first: 1, last: Some(3) }),
        ..Default::default()
    });
    req.headers.insert(
        "if-range",
        HeaderValue::from_str(&format!("\"{etag}\"")).unwrap(),
    );
    let got = backend.get_object(req).await.unwrap();
    assert_eq!(got.output.content_range.as_deref(), Some("bytes 1-3/5"));
    assert_eq!(read_body(got.output.body.unwrap()).await.unwrap(), b"ell");
    // Stale validator → the Range is ignored (full 200).
    let mut req = s3_request(dto::GetObjectInput {
        bucket: b.to_string(),
        key: "hello.txt".into(),
        range: Some(Range::Int { first: 1, last: Some(3) }),
        ..Default::default()
    });
    req.headers.insert(
        "if-range",
        HeaderValue::from_str("\"deadbeefdeadbeefdeadbeefdeadbeef\"").unwrap(),
    );
    let got = backend.get_object(req).await.unwrap();
    assert_eq!(got.output.content_range, None);
    assert_eq!(read_body(got.output.body.unwrap()).await.unwrap(), b"hello");
}
```

(`HeaderValue` and `Range` are already imported by the objects.rs test module.)

- [ ] **Step 2: Write the fast-path regression guard** — the read-path fix must not regress the unconditional GET. The existing `range_requests` (objects.rs:460) and `conditional_requests` (objects.rs:505) tests already cover the condition and range outcomes; add one test that pins the unchanged outcomes under the new head-first flow:

```rust
#[tokio::test]
async fn get_object_conditional_ordering_keeps_outcomes() {
    let (backend, b) = setup_name().await;
    let etag = "5d41402abc4b2a76b9719d911017c592";
    backend
        .storage()
        .put_object(&b, &"hello.txt".into(), body(b"hello"))
        .await
        .unwrap();
    // A failing If-None-Match still answers 304 (no body)…
    let err = backend
        .get_object(s3_request(dto::GetObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            if_none_match: Some(format!("\"{etag}\"").parse().unwrap()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NotModified");
    // …and a passing one still answers 200 with the full body.
    let got = backend
        .get_object(s3_request(dto::GetObjectInput {
            bucket: b.to_string(),
            key: "hello.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        read_body(got.output.body.unwrap()).await.unwrap(),
        b"hello"
    );
}
```

- [ ] **Step 3: Run to verify the If-Range test fails**

Run: `cargo test -p tinio-server get_object_if_range_gates_the_range`
Expected: FAIL — the stale case answers 206 with the range body (If-Range is ignored today), so the `content_range == None` assertion panics. The regression guard may pass on the baseline; Step 4 must still make `conditional_requests` + `range_requests` pass unchanged.

- [ ] **Step 4: Implement the read-path fix** — replace the body of `op_get_object` (`objects.rs:139-187`):

```rust
    pub(crate) async fn op_get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;

        let range = req.input.range.map(byte_range);
        let conditions = ConditionalHeaders::new(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since,
            req.input.if_unmodified_since,
        );
        // If-Range gates the Range header only (RFC 9110 §13.1.5); a
        // parse failure or a wildcard drops the header. Head-first
        // evaluation: a conditional header, or an If-Range over a
        // Range, needs the object info BEFORE the body fetch — an
        // unconditional GET keeps today's direct fetch (no extra
        // metadata read).
        let if_range = parse_if_range(&req.headers);
        let head = if conditions.any() && !(range.is_some() && if_range.is_some()) {
            None
        } else {
            Some(
                self.storage
                    .head_object(&bucket, &key)
                    .await
                    .map_err(map_backend_error)?,
            )
        };
        if let Some(info) = head.as_ref() {
            conditions.check(&info.etag, info.last_modified, false)?;
        }
        // The Range honored by the body fetch: an If-Range mismatch
        // drops it (the full 200 is served).
        let range = match (range, if_range, head.as_ref()) {
            (Some(r), Some(ir), Some(info)) if !ir.matches(&info.etag, info.last_modified) => None,
            (r, _, _) => r,
        };
        let GetObjectResult {
            body,
            served_range,
            info: fetched,
        } = self
            .storage
            .get_object(&bucket, &key, range)
            .await
            .map_err(map_backend_error)?;
        // The response metadata is the HEAD's info — the snapshot that
        // passed the check: a concurrent overwrite between the head and
        // the body fetch must not serve an ETag different from the one
        // the precondition passed. An unconditional GET has no head and
        // uses the fetched info. TOCTOU between the head and the body
        // stream is accepted — identical to today's conditional-put
        // semantics.
        let info = head.unwrap_or(fetched);

        let (content_length, content_range) = match served_range {
            Some((start, end)) => (
                end - start + 1,
                Some(format!("bytes {start}-{end}/{}", info.size)),
            ),
            None => (info.size, None),
        };
        let content_type = req
            .input
            .response_content_type
            .or(Some(Self::content_type(info.key.as_ref())));
        Ok(S3Response::new(dto::GetObjectOutput {
            accept_ranges: Some("bytes".into()),
            body: Some(Self::stream_out(body)),
            content_length: Some(content_length as i64),
            content_range,
            content_type,
            e_tag: Some(Self::etag_wire(&info.etag)),
            last_modified: Some(Self::last_modified(info.last_modified)),
            ..Default::default()
        }))
    }
```

Add `parse_if_range` to the `crate::backend::{...}` import in `objects.rs`.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS — `conditional_requests`, `range_requests`, `get_object_if_range_gates_the_range`, and the regression guard all green.

- [ ] **Step 6: Checkpoint** — report; do not commit.

---

### Task 7: Cucumber `conditions.feature` + step module

> **Deferred (user decision, 2026-08-31)**: start only after the BDD-migration implementation (`2026-08-31-cucumber-bdd-migration.md`) is complete — the user is actively editing the e2e crate, which **does not compile today** (in-flight migration: `World.parts` not yet defined, `steps/multipart.rs` errors). The cucumber verification of this plan depends on that migration landing first.

**Files:**
- Create: `crates/tinio-e2e/tests/features/conditions.feature`
- Create: `crates/tinio-e2e/tests/steps/conditions.rs`
- Modify: `crates/tinio-e2e/tests/steps/mod.rs` (declare `pub mod conditions;`)

**Interfaces:**
- Consumes: `world.client.request(method, path, headers, body)` (common.rs), `world.stored_etag`, `world.upload_id` / `world.upload_key` (mod.rs World), the existing `I create bucket {string}` / `the response status is {int}` / `the error code is {string}` / `the response header {string} is {string}` / `I start a multipart upload for {string}` steps (buckets.rs / errors.rs / multipart.rs).
- Produces: the step vocabulary the `conditions.feature` scenarios use; `{etag}` substitution from `world.stored_etag`.

- [ ] **Step 1: Write the feature file** (`tests/features/conditions.feature`):

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (conditionals)
Feature: Conditional requests over real HTTP

  Scenario: GET If-None-Match matching answers 304
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    And the response header "ETag" is stored
    When I send a "GET" request to "/data/a.txt" with header "If-None-Match" "{etag}"
    Then the response status is 304

  Scenario: GET If-Match mismatching answers 412
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "GET" request to "/data/a.txt" with header "If-Match" "\"deadbeef\""
    Then the response status is 412

  Scenario: PUT If-None-Match star on an existing object answers 412
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "PUT" request to "/data/a.txt" with header "If-None-Match" "*" with body "world"
    Then the response status is 412

  Scenario: PUT If-Match and If-None-Match together answer 400
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "PUT" request to "/data/a.txt" with header "If-Match" "*" and header "If-None-Match" "*" with body "world"
    Then the response status is 400
    And the error code is "InvalidRequest"

  Scenario: Conditional delete with a stale ETag answers 412
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "DELETE" request to "/data/a.txt" with header "If-Match" "\"deadbeef\""
    Then the response status is 412

  Scenario: Conditional delete with a matching ETag deletes
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    And the response header "ETag" is stored
    When I send a "DELETE" request to "/data/a.txt" with header "If-Match" "{etag}"
    Then the response status is 204

  Scenario: If-Range matching serves the Range as a 206
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    And the response header "ETag" is stored
    When I send a "GET" request to "/data/a.txt" with header "If-Range" "{etag}" and header "Range" "bytes=2-4"
    Then the response status is 206
    And the response body is "llo"

  Scenario: If-Range stale ignores the Range and serves the full 200
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "GET" request to "/data/a.txt" with header "If-Range" "\"deadbeef\"" and header "Range" "bytes=2-4"
    Then the response status is 200
    And the response body is "hello"

  Scenario: If-Range garbage serves the Range as if absent
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "GET" request to "/data/a.txt" with header "If-Range" "garbage" and header "Range" "bytes=2-4"
    Then the response status is 206
    And the response body is "llo"

  Scenario: If-Range wildcard serves the Range as if absent
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "GET" request to "/data/a.txt" with header "If-Range" "*" and header "Range" "bytes=2-4"
    Then the response status is 206
    And the response body is "llo"

  Scenario: If-Range stale with an unsatisfiable Range serves the full 200
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "GET" request to "/data/a.txt" with header "If-Range" "\"deadbeef\"" and header "Range" "bytes=99-100"
    Then the response status is 200
    And the response body is "hello"

  Scenario: CopyObject source both-present is not a 400 (RFC order)
    Given I create bucket "data"
    And I upload "data/src.txt" with body "hello"
    When I copy "/data/src.txt" to "/data/dst.txt" with header "x-amz-copy-source-if-match" "*" and header "x-amz-copy-source-if-none-match" "*"
    Then the response status is 412

  Scenario: CompleteMultipartUpload honors If-None-Match
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "hello"
    When I complete the multipart upload with header "If-None-Match" "*"
    Then the response status is 200
    Given I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "world"
    When I complete the multipart upload with header "If-None-Match" "*"
    Then the response status is 412

  Scenario: CompleteMultipartUpload rejects a specific If-None-Match
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "hello"
    When I complete the multipart upload with header "If-None-Match" "\"abc\""
    Then the response status is 501

  Scenario: CompleteMultipartUpload rejects If-Match and If-None-Match together
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "hello"
    When I complete the multipart upload with header "If-Match" "*" and header "If-None-Match" "*"
    Then the response status is 400
    And the error code is "InvalidRequest"

  Scenario: AbortMultipartUpload honors If-Match-Initiated-Time
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    When I abort the multipart upload with If-Match-Initiated-Time "Wed, 21 Oct 2015 07:28:00 GMT"
    Then the response status is 412
```

- [ ] **Step 2: Write the step module** (`tests/steps/conditions.rs`) — the raw-request-with-headers vocabulary plus the multipart-condition helpers. The regex makes the header groups optional so one step covers one or two headers (and an optional body); the `{etag}` placeholder resolves from `world.stored_etag`:

```rust
//! Conditional-request steps (conditions.feature): raw requests with
//! If-Match / If-None-Match / If-Range / Range headers, and the
//! multipart conditional completions. The `{etag}` placeholder in a
//! step argument resolves to the ETag stored by "the response header
//! ... is stored".

use cucumber::{given, then, when};

use super::World;

/// Substitute `{etag}` in a step argument with the stored ETag.
fn substitute(world: &World, arg: String) -> String {
    if arg == "{etag}" {
        world.stored_etag.clone()
    } else {
        arg
    }
}

/// A raw request with one or two headers and an optional body. The
/// regex's optional groups capture as empty strings when absent; a
/// request without headers uses the errors.rs steps instead.
#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" with header "([^"]+)" "([^"]*)"(?: and header "([^"]+)" "([^"]*)")?(?: with body "([^"]*)")?"#)]
#[when(regex = r#"I send a "(\w+)" request to "([^"]+)" with header "([^"]+)" "([^"]*)"(?: and header "([^"]+)" "([^"]*)")?(?: with body "([^"]*)")?"#)]
#[then(regex = r#"I send a "(\w+)" request to "([^"]+)" with header "([^"]+)" "([^"]*)"(?: and header "([^"]+)" "([^"]*)")?(?: with body "([^"]*)")?"#)]
async fn raw_request_with_headers(
    world: &mut World,
    method: String,
    path: String,
    h1: String,
    v1: String,
    h2: String,
    v2: String,
    body: String,
) {
    // Bind the substitutions to locals first: `&substitute(...)` on a
    // temporary would dangle once the statement ends (E0716).
    let v1 = substitute(world, v1);
    let v2 = substitute(world, v2);
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if !h1.is_empty() {
        headers.push((&h1, &v1));
    }
    if !h2.is_empty() {
        headers.push((&h2, &v2));
    }
    world.last = world
        .client
        .request(&method, &path, &headers, body.as_bytes())
        .await;
}

/// A plain part upload (no checksum) for the conditional-complete
/// scenarios.
#[given(expr = "I upload part {int} with body {string}")]
async fn upload_part(world: &mut World, part: u32, body: String) {
    world.last = world
        .client
        .request(
            "PUT",
            &format!(
                "/{}?partNumber={part}&uploadId={}",
                world.upload_key, world.upload_id
            ),
            &[],
            body.as_bytes(),
        )
        .await;
}

/// Complete the scenario's multipart upload with one or two
/// conditional headers (If-Match / If-None-Match). The part list
/// reuses the stored part ETag from the part upload.
#[when(regex = r#"I complete the multipart upload with header "([^"]+)" "([^"]*)"(?: and header "([^"]+)" "([^"]*)")?"#)]
async fn complete_with_condition(
    world: &mut World,
    name: String,
    value: String,
    h2: String,
    v2: String,
) {
    let part_etag = world
        .last
        .headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("etag"))
        .map(|(_, v)| v.clone())
        .expect("part upload must return an ETag");
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    // Same temporary-borrow fix as the raw-request step (E0716).
    let value = substitute(world, value);
    let v2 = substitute(world, v2);
    let mut headers: Vec<(&str, &str)> = vec![(&name, &value)];
    if !h2.is_empty() {
        headers.push((&h2, &v2));
    }
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{}?uploadId={}", world.upload_key, world.upload_id),
            &headers,
            body.as_bytes(),
        )
        .await;
}

/// CopyObject with two copy-source conditional headers. The both-present
/// 400 rule applies to destinations only — the copy-source family keeps
/// RFC 9110 §13.2.2 order (If-Match first, then If-None-Match), so `*` +
/// `*` against an existing source answers 412, never 400.
#[when(regex = r#"I copy "([^"]+)" to "([^"]+)" with header "([^"]+)" "([^"]*)" and header "([^"]+)" "([^"]*)""#)]
async fn copy_with_source_conditions(
    world: &mut World,
    src: String,
    dst: String,
    h1: String,
    v1: String,
    h2: String,
    v2: String,
) {
    let v1 = substitute(world, v1);
    let v2 = substitute(world, v2);
    let headers: Vec<(&str, &str)> = vec![("x-amz-copy-source", &src), (&h1, &v1), (&h2, &v2)];
    world.last = world.client.request("PUT", &dst, &headers, &[]).await;
}

/// Abort the scenario's multipart upload with If-Match-Initiated-Time.
#[when(regex = r#"I abort the multipart upload with If-Match-Initiated-Time "([^"]+)""#)]
async fn abort_with_condition(world: &mut World, value: String) {
    world.last = world
        .client
        .request(
            "DELETE",
            &format!("/{}?uploadId={}", world.upload_key, world.upload_id),
            &[("x-amz-if-match-initiated-time", &value)],
            &[],
        )
        .await;
}
```

Note: the abort scenario's header name is `x-amz-if-match-initiated-time` — verified against s3s 0.15.0 (`X_AMZ_IF_MATCH_INITIATED_TIME`, `src/header/generated.rs:249`).

- [ ] **Step 3: Wire the module** — add `pub mod conditions;` to `tests/steps/mod.rs` (alphabetical: after `common`, before `errors`).

- [ ] **Step 4: Run the e2e suite**

Run: `cargo test -p tinio-e2e --test cucumber`
Expected: PASS — all conditions.feature scenarios green (plus the existing buckets.feature / error_codes.feature scenarios).

- [ ] **Step 5: Checkpoint** — report; do not commit.

---

### Task 8: Specs & docs — conditional surface

**Files:**
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (conditional-headers section)
- Modify: `specs/001-s3-local-server/tasks.md` (task entries)
- Modify: `specs/001-s3-local-server/checklists/` (compatibility checklist items)

**Interfaces:**
- Consumes: the behavior locked by Tasks 1-7.
- Produces: the spec IDs the feature-file scenarios carry (the `conditions.feature` scenarios get their `@FR-xxx`/`@SC-xxx`/`@T0xx` tags in a follow-up pass once the IDs exist — the feature file written in Task 7 starts without them, and this task assigns them).

- [ ] **Step 1: Extend `s3-surface.md`** — document the conditional surface: DeleteObject trio (412 on mismatch; missing + If-Match → 412, missing + date/size-only → 204), CMU conditions (If-Match vs current object, missing → 404; If-None-Match `*` only; both → 400), Abort initiated-time (412), If-Range (GET only; mismatch → full 200), write-path both-header 400 rule, the head-first read path. Assign new FR/SC IDs following the file's numbering scheme (read the existing IDs first).

- [ ] **Step 2: Add task entries to `tasks.md`** — one entry per behavior group (DeleteObject conditions, CMU conditions, Abort condition, If-Range, 400 rules, read-path fix), each referencing its spec ID and its cucumber scenarios.

- [ ] **Step 3: Add checklist items to `checklists/`** — extend the compatibility checklist (the file that already holds CHK004 / CHK015 for conditionals) with the new surfaces.

- [ ] **Step 4: Tag the feature scenarios** — apply the new spec IDs to the `conditions.feature` scenarios from Task 7 (runs after Task 7 lands — deferred with it).

- [ ] **Step 5: Verify + checkpoint** — run `cargo test -p tinio-server` and (once Task 7 lands) `cargo test -p tinio-e2e --test cucumber`; report all changes; do not commit.

---

## Self-Review Notes

- **Spec coverage**: shared machinery (Task 1), destination convergence + both-400 (Task 2), DeleteObject trio (Task 3), CMU (Task 4), Abort (Task 5), If-Range + read-path fix (Task 6), cucumber conditions.feature (Task 7), specs/docs (Task 8). The RenameObject conditions, tagging, bucket tagging, and GetObjectAttributes sections of the spec belong to Plan B (`2026-08-31-s3-tagging-ops.md`).
- **Second-precision rule**: `to_whole_seconds` is used by DeleteObject lmt, Abort initiated-time, and If-Range dates — one helper, **two operators**: second-precision equality (DeleteObject lmt, Abort initiated-time) vs the RFC 9110 §13.1.5 `<=` match rule (If-Range); the spec documents both.
- **CMU 404 NoSuchKey**: emitted by `check_complete_conditions`, not by `check_missing` (which answers 412) — the two policies deliberately differ; the spec documents both.
- **Unconditional paths unchanged**: DELETE without conditions skips the head; GET without conditions skips the head; PUT/Copy without conditions skips the head — the fast paths are preserved by the `any()` checks.
- **PostObject both-present 400**: no dedicated test — the s3s default `post_object` delegates to the same `op_put_object`, zero divergence risk (user-confirmed, 2026-08-31).
- **Coverage gaps closed (user decision, 2026-08-31)**: negative `if_match_size` → `InvalidArgument` (Task 3 unit), date-mismatch at op level (Task 3), missing-upload + condition → `NoSuchUpload` (Task 5).
