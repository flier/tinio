# S3 Bucket CORS Config Ops + OPTIONS Preflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

> **Status: COMPLETE — 2026-09-06.** All 11 tasks implemented by subagent-driven development (fresh implementer + task review per task), full gates green (`cargo test --workspace`, clippy, cucumber fs/mem + `@cors`/`@cors-off` 6/6, boto3 journey, double-gate `--no-default-features` matrix); final whole-branch review (opus) + one fix wave clean. The plan's only material correction (final review F1): the additive-tuple/no-bump premise is false for redb 4.2 — an older-arity `meta.redb` refuses to open (`TableTypeMismatch`) and the user ruled NO migration (no historical-version support; recovery = delete the state dir) — see the Schema-ruling correction in Architecture and the design spec. Branch merged to `dev` as a fast-forward after rebasing onto the latest `dev`; commit messages carry no agent trailers.

**Goal:** Implement the bucket CORS configuration API (`get_bucket_cors` / `put_bucket_cors` / `delete_bucket_cors`) on tinio-server plus the server-layer CORS behavior — OPTIONS preflight answering and `Access-Control-*` decoration of actual responses — unlocking browser clients (aws-sdk-js uploads, Web consoles) against tinio.

**Architecture:** CORS domain types and the canonical wire codec land in tinio-core (`cors.rs`, mirroring the `Tags`/`Acl` pattern); the shared `tinio-store` BUCKETS row gains one appended `cors_wire` element (self-healing decode, `STATE_VERSION` stays 1 — see the Schema-ruling correction: an older-arity state dir refuses to open, recovery = delete it); the fs and mem backends get `cors`/`set_cors`/`clear_cors` store methods and the `BucketOps` contract trio. tinio-server implements the three s3s trait methods as thin op forwards (the tagging-ops pattern) under a **double gate** — `cors` cargo feature (default on) **and** `Capabilities.cors` runtime toggle (grilling Q2 — user override of the tagging-only precedent; feature off → s3s trait defaults, capability off → `"{name} is disabled"`). Preflight is answered by an s3s 0.15 **custom route** (`s3s::route::S3Route` via `S3ServiceBuilder::set_route`): s3s 0.15 still has no built-in OPTIONS/preflight operation, but the custom-route seam intercepts requests before op dispatch and can opt out of auth (`check_access` override) — exactly what a browser's unsigned preflight needs. Actual (non-OPTIONS) responses are decorated in the data-plane middleware when the request carries an `Origin` matching a stored rule (origin **and** method — AWS's own documented match semantics; verified MinIO has no per-bucket CORS at all). Route and middleware share one `CorsConfigs<S>` lookup over `Arc<S>`. Semantics follow AWS exactly where verified: rule-list `Allow-Methods`, the two verbatim 403 message variants, `Vary` trio (append semantics), bare-`*` rule → literal `*` ACAO with `Allow-Credentials` omitted.

**Tech Stack:** Rust, redb (shared table layer via tinio-store), s3s 0.15 (`S3` trait + `route::S3Route`), hyper/tower (data-plane middleware), tokio, thiserror.

**Spec:** `docs/superpowers/specs/2026-09-05-s3-cors-design.md` (the grilled + op-reviewed design — the plan argues from it; executors read both) and `docs/superpowers/specs/2026-09-04-s3s-api-coverage-gap-analysis.md` (Tier A#2). The gap analysis claimed "the tinio server HTTP layer must answer preflight itself" — the design discovered s3s 0.15's `S3Route` custom-route seam and uses it; `route.rs` has no OPTIONS *operation*, which is why the ops (not the route seam) stay untouched.

## Global Constraints

- **Precondition: the ACL plan (2026-09-05) merges FIRST** (grilling Q5). BUCKETS baseline = the ACL plan's post-merge 4-tuple `(created_at_nanos, tags_wire, owner_wire, acl_wire)`; the CORS element appends **5th**. If the tree is somehow still the shared-store 2-tuple when this plan starts, Task 2's first step reconciles the shapes before extending. `expected_bucket_owner` on the three CORS ops is **not** validated here — the ACL plan's `S3Access` becomes the single enforcement point once merged (the CORS ops fall under its owner-only mapping).
- **No auto git commit** — leave changes in the tree; report pending changes; the user decides when to commit (project rule, CLAUDE.md).
- **Double gate** (grilling Q2): `cors = []` cargo feature on tinio-server, added to `default`; **and** `Capabilities.cors: bool` (default true). Feature off → the three s3s trait defaults answer `NotImplemented "{name} is not implemented yet"` and no CORS code exists (`--no-default-features` must compile); capability off (feature on) → `NotImplemented "{name} is disabled"` via `require_cap` (`backend/mod.rs:243-249`).
- **AWS-exact response semantics** (op-reviewed): preflight success sends the **rule's** method list (not the requested method), `Vary: Origin, Access-Control-Request-Headers, Access-Control-Request-Method` with **append** semantics, `Content-Length: 0` explicit, and a bare-`*`-containing origin rule answers ACAO `*` **without** `Access-Control-Allow-Credentials`; denials are 403 `AccessDenied` with the two verbatim AWS messages and **no** CORS headers; syntactically invalid bucket names are answered by s3s itself (400 `InvalidBucketName`/`InvalidURI`) before preflight/ops ever run.
- **Security hard rules** (op-review S1/C2): all header values derived from request or config data are constructed **fallibly** (`HeaderValue::from_str`, failure skips the header — never unwrap/panic); the wire codec percent-encodes **every byte outside `[A-Za-z0-9-._~]`** (the `object.rs` tags codec escapes only `% = & + space` and must NOT be copied as-is); put validation rejects C0 control bytes/DEL in any config string, `max_age_seconds < 0`, and decoded configs over 64 KB (`CORS_CONFIG_BYTES_MAX`).
- **CORS rules are order-preserving and first-origin-match** (AWS "select first matching rule" semantics): the wire codec never sorts, dedupes, or reorders rules; preflight and decoration select the **first** rule (in stored order) whose **origin** matches, then validate method (and, for preflight, headers) **within that rule only** — a rule that matches origin but not method/headers does not fall through to a later rule.
- **Schema ruling:** extend the tuple arity by **appending the element last**; `STATE_VERSION` stays `1`; self-healing decode (invalid wire → empty config; `''` wire = "no config" = 404 on get); the storage layer normalizes an empty rule set to the `''` wire (op-review G2). **Correction 2026-09-06 (final review F1; user ruling — NO migration):** "no bump" covers only the `STATE_VERSION` number. redb 4.2 binds a table's value type name at the `TableDefinition` — changing the tuple arity makes `check_match` fail with `TableError::TableTypeMismatch` **on open**, so a state dir (`meta.redb`) written under an older row shape will NOT open (server refuses to start — loud failure, no silent data loss). No migration mechanism, no historical-version support; the documented recovery is deleting the state dir (dev-local derived metadata, recomputed by the scanner). Pinned by the store's `legacy_buckets_arity_fails_loudly_on_open` test (final review F2).
- **English only** — comments, docs, commit messages (CLAUDE.md).
- **Code style:** `docs/style.md` — imports (3+ segments, `tokio::fs` style), newtypes, `Error` rules, compressed prose.
- **Tests:** `docs/tests.md` — unit in-module, conformance via `crates/tinio-util` `assert_conformance`-style harness, cucumber e2e in `crates/tinio-e2e`.
- **Platforms:** Windows + WSL2.

## File Structure

| File | Responsibility |
|---|---|
| `crates/tinio-core/src/cors.rs` (new) | `CorsRule`, `CorsConfig`, `PreflightMatch`, wire codec, wildcard/header matching, validation consts |
| `crates/tinio-core/src/lib.rs` (modify) | `pub mod cors;` |
| `crates/tinio-store/src/bucket.rs` (modify) | BUCKETS value 5-tuple (ACL baseline), accessor arities, `decode_cors_wire` |
| `crates/tinio-store/src/…schema-pin test` (modify) | arity pin 4 → 5 |
| `crates/tinio-core/src/storage/bucket.rs` (modify) | `BucketOps` CORS trio |
| `crates/tinio-fs/src/bucket.rs` (modify) | `Store::{cors, set_cors, clear_cors}`; `BucketOps` impl |
| `crates/tinio-mem/src/bucket.rs` (modify) | mem mirror |
| `crates/tinio-util/src/testing.rs` (modify) | conformance CORS blocks |
| `crates/tinio-config/src/schema/s3.rs` (modify) | `Capabilities.cors` (default true) + config test |
| `crates/tinio-server/src/backend/cors.rs` (new) | the three ops, dto conversions, Content-MD5 + config validation, `CorsConfigs`, `CorsLookup`, `CorsPreflightRoute`, `bucket_from_uri` |
| `crates/tinio-server/Cargo.toml` (modify) | `cors = []` feature, added to `default` (double gate) |
| `crates/tinio-server/src/backend/{s3,mod,errors}.rs` (modify) | trait forwards (`#[cfg(feature = "cors")]`), `new_shared` (UNGATED — shared `Arc<S>` constructor; `S3Backend::new` delegates to it), `#[cfg(feature = "cors")] mod cors;` |
| `crates/tinio-server/src/metrics.rs` (modify) | three MetricS3 wrappers (cfg-gated) |
| `crates/tinio-server/src/data.rs` (modify) | wiring (`new_shared` + route + `CorsLookup`), Origin decoration middleware (cfg-gated) |
| `crates/tinio-e2e/tests/features/cors.feature` (new) | cucumber scenarios + `@cors-off` |
| `crates/tinio-server/tests/boto3_journey.py` (modify) | CORS legs |
| `specs/001-s3-local-server/{contracts/s3-surface.md,contracts/config.md,tasks.md}` (modify) | FR/SC + capability docs |
| `docs/superpowers/specs/2026-09-04-s3s-api-coverage-gap-analysis.md` (modify) | Tier A#2 row: status note + pointer to this plan |

---

### Task 1: tinio-core — CORS types, wire codec, preflight matching

**Files:**
- Create: `crates/tinio-core/src/cors.rs`
- Modify: `crates/tinio-core/src/lib.rs` (`pub mod cors;`)
- Test: `crates/tinio-core/src/cors.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: nothing new (the percent encode/decode helpers are private copies of the logic in `object.rs`; if `crates/tinio-core/src/percent.rs` already exists because the ACL plan merged, import `crate::percent::{encode, decode}` instead of the local private copies and delete the copies).
- Produces:
  - `pub const CORS_RULES_MAX: usize = 100;`, `pub const CORS_RULE_ID_MAX: usize = 255;`, `pub const CORS_CONFIG_BYTES_MAX: usize = 64 * 1024;` (op-review P1), `pub const CORS_METHODS: [&str; 5] = ["GET", "PUT", "HEAD", "POST", "DELETE"];`
  - `pub struct CorsRule { pub id: Option<String>, pub allowed_methods: Vec<String>, pub allowed_origins: Vec<String>, pub allowed_headers: Option<Vec<String>>, pub expose_headers: Option<Vec<String>>, pub max_age_seconds: Option<i32> }` — `Clone/Debug/PartialEq/Eq`; methods: `origin_matches(&self, origin: &str) -> bool`, `method_allows(&self, method: &str) -> bool`, `headers_allow(&self, requested: &[String]) -> bool`
  - `pub struct CorsConfig { pub rules: Vec<CorsRule> }` — `Default/Clone/Debug/PartialEq/Eq`; `to_wire(&self) -> String`, `from_wire(s: &str) -> Self` (self-heals to `Default`), `preflight(&self, origin: &str, method: &str, requested_headers: &[String]) -> Option<PreflightMatch>`, `rule_for(&self, origin: &str, method: &str) -> Option<&CorsRule>` (first **origin**-matching rule; method validated within that rule only — used by response decoration)
  - `pub struct PreflightMatch { pub rule: CorsRule, pub origin: String, pub method: String, pub requested_headers: Vec<String> }` — `Clone/Debug/PartialEq/Eq`
- Wire grammar (**order-preserving, first-match — no sort/dedupe**): `rules := rule ('&' rule)*`; `rule := methods ',' origins ',' headers ',' expose ',' id ',' max_age`; each field is percent-encoded (`percent::encode` escapes every byte outside `[A-Za-z0-9-._~]` — replaces `%`, `&`, `,`, and everything else non-unreserved, so raw `,`/`&` never appear inside an encoded field); optional fields encode as `''`; `max_age` is bare decimal digits (or `''`).

- [x] **Step 1: Write the failing tests in `cors.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn rule() -> CorsRule {
        CorsRule {
            id: Some("allow-example".into()),
            allowed_methods: vec!["GET".into(), "PUT".into()],
            allowed_origins: vec!["https://example.com".into(), "https://*.example.net".into()],
            allowed_headers: Some(vec!["x-amz-*".into(), "content-type".into()]),
            expose_headers: Some(vec!["ETag".into()]),
            max_age_seconds: Some(300),
        }
    }

    #[test]
    fn wire_round_trip_preserves_order_and_fields() {
        let cfg = CorsConfig { rules: vec![rule(), CorsRule { id: None, allowed_methods: vec!["DELETE".into()],
            allowed_origins: vec!["*".into()], allowed_headers: None, expose_headers: None, max_age_seconds: None }] };
        let wire = cfg.to_wire();
        let back = CorsConfig::from_wire(&wire);
        assert_eq!(back, cfg, "{wire}");
    }

    #[test]
    fn wire_keeps_rule_order_first_match_semantics() {
        // Rules must NOT be sorted or deduped — the wire preserves stored order.
        let cfg = CorsConfig { rules: vec![
            CorsRule { id: None, allowed_methods: vec!["GET".into()], allowed_origins: vec!["https://example.com".into()],
                allowed_headers: None, expose_headers: None, max_age_seconds: None },
            CorsRule { id: Some("second".into()), allowed_methods: vec!["GET".into()], allowed_origins: vec!["*".into()],
                allowed_headers: None, expose_headers: None, max_age_seconds: None },
        ] };
        let back = CorsConfig::from_wire(&cfg.to_wire());
        assert_eq!(back.rules[0].id, None); // the tighter rule stayed first
        assert_eq!(back.rules[1].id.as_deref(), Some("second"));
    }

    #[test]
    fn wire_self_heals_to_empty_on_garbage() {
        assert_eq!(CorsConfig::from_wire("garbage!%"), CorsConfig::default());
        assert_eq!(CorsConfig::from_wire("a,b,c"), CorsConfig::default());     // wrong field count
        assert_eq!(CorsConfig::from_wire("a,b,*,*,*,abc"), CorsConfig::default()); // bad max_age
        assert_eq!(CorsConfig::from_wire(""), CorsConfig::default());
    }

    #[test]
    fn wire_escapes_field_separators_inside_values() {
        let cfg = CorsConfig { rules: vec![CorsRule { id: Some("a&b,c;d".into()),
            allowed_methods: vec!["GET".into()], allowed_origins: vec!["https://a.example.com/?x=1,y;z".into()],
            allowed_headers: None, expose_headers: None, max_age_seconds: None }] };
        let wire = cfg.to_wire();
        assert!(!wire.contains("&,c"), "{wire}"); // the value's ',' must be encoded
        assert!(!wire.contains(",;"), "{wire}"); // and so must ';' (F1 separator class, not a field sep)
        assert_eq!(CorsConfig::from_wire(&wire), cfg);
    }

    #[test]
    fn wire_round_trips_control_bytes() {
        // op-review S1/C2: control bytes inside a value must be encoded away
        // (the tags codec's `% = & + space`-only escaping cannot do this —
        // the full unreserved-set rule is required by the wire grammar).
        let cfg = CorsConfig { rules: vec![CorsRule { id: Some("id\t\r\nx".into()),
            allowed_methods: vec!["GET".into()], allowed_origins: vec!["https://a.example.com".into()],
            allowed_headers: Some(vec!["x\r\n-amz-b".into()]), expose_headers: None, max_age_seconds: None }] };
        let wire = cfg.to_wire();
        assert_eq!(CorsConfig::from_wire(&wire), cfg, "control bytes must round-trip, {wire}");
    }

    #[test]
    fn multi_star_pattern_is_a_safe_non_match() {
        // The put layer rejects >1 `*` (400); a stored row with one
        // (e.g. from a legacy/corrupt row) must never panic — and must
        // not match a plain value.
        let rule = CorsRule { id: None, allowed_methods: vec!["GET".into()],
            allowed_origins: vec!["a*b*c".into()], allowed_headers: None,
            expose_headers: None, max_age_seconds: None };
        assert!(!rule.origin_matches("abc"));
    }

    #[test]
    fn origin_patterns_exact_wildcard_and_dot_wildcard() {
        assert!(CorsConfig { rules: vec![rule()] }.preflight("https://a.example.net", "GET", &[]).is_some(),
            "https://*.example.net suffix match");
        assert!(CorsConfig { rules: vec![rule()] }.preflight("https://example.com", "GET", &[]).is_some());
        assert!(CorsConfig { rules: vec![rule()] }.preflight("https://example.net", "GET", &[]).is_none(),
            "bare '*.example.net' must not match the apex (no subdomain dot)");
        assert!(CorsConfig { rules: vec![CorsRule { id: None,
            allowed_methods: vec!["GET".into()], allowed_origins: vec!["*".into()],
            allowed_headers: None, expose_headers: None, max_age_seconds: None }] }
            .preflight("https://any.where", "GET", &[]).is_some(),
            "bare '*' matches any origin");
    }

    #[test]
    fn preflight_matches_first_rule_with_origin_method_and_headers() {
        let cfg = CorsConfig { rules: vec![rule()] };
        let hit = cfg.preflight("https://example.com", "PUT", &["x-amz-foo".into()]).unwrap();
        assert_eq!(hit.origin, "https://example.com");      // echoed
        assert_eq!(hit.method, "PUT");                      // echoed
        assert_eq!(hit.requested_headers, vec!["x-amz-foo".to_string()]);
        // method not allowed by any rule → no match
        assert!(cfg.preflight("https://example.com", "DELETE", &[]).is_none());
        // unknown header → no match; `*` header pattern → match
        assert!(cfg.preflight("https://example.com", "GET", &["x-evil".into()]).is_none());
        assert!(cfg.preflight("https://example.com", "GET", &["x-amz-anything".into()]).is_some());
        // header matching is case-insensitive (HTTP header names)
        assert!(cfg.preflight("https://example.com", "GET", &["X-AmZ-Foo".into()]).is_some());
        // no AllowedHeaders = no headers allowed
        let strict = CorsConfig { rules: vec![CorsRule { id: None, allowed_methods: vec!["GET".into()],
            allowed_origins: vec!["*".into()], allowed_headers: None, expose_headers: None, max_age_seconds: None }] };
        assert!(strict.preflight("https://example.com", "GET", &["a".into()]).is_none());
        assert!(strict.preflight("https://example.com", "GET", &[]).is_some());
    }

    #[test]
    fn rule_for_returns_first_origin_and_method_match() {
        let cfg = CorsConfig { rules: vec![
            CorsRule { id: Some("who".into()), allowed_methods: vec!["GET".into()], allowed_origins: vec!["*".into()],
                allowed_headers: None, expose_headers: None, max_age_seconds: None },
        ] };
        assert_eq!(cfg.rule_for("https://example.com", "GET").unwrap().id.as_deref(), Some("who"));
        assert!(cfg.rule_for("https://example.com", "DELETE").is_none());
    }

    #[test]
    fn method_match_is_case_insensitive() {
        assert!(rule().method_allows("get"));
        assert!(!rule().method_allows("PATCH"));
    }

    #[test]
    fn preflight_does_not_fall_through_past_an_origin_matching_rule() {
        // First-origin-match (AWS): rule1 matches the origin but not the
        // method; rule2 matches everything. AWS DENIES — rule2 must NOT be
        // consulted once rule1 claims the origin.
        let cfg = CorsConfig { rules: vec![
            CorsRule { id: Some("r1".into()), allowed_methods: vec!["GET".into()], allowed_origins: vec!["https://example.com".into()],
                allowed_headers: None, expose_headers: None, max_age_seconds: None },
            CorsRule { id: Some("r2".into()), allowed_methods: vec!["PUT".into()], allowed_origins: vec!["*".into()],
                allowed_headers: None, expose_headers: None, max_age_seconds: None },
        ] };
        // origin matches r1, but PUT is not in r1's methods → deny (no r2 fall-through)
        assert!(cfg.preflight("https://example.com", "PUT", &[]).is_none());
        // rule_for (decoration) must behave identically: origin matches r1,
        // PUT not allowed by r1 → no rule (r2's origin "*" is never consulted)
        assert!(cfg.rule_for("https://example.com", "PUT").is_none());
        // and a method r1 DOES allow still resolves to r1 (the first origin match)
        assert_eq!(cfg.preflight("https://example.com", "GET", &[]).unwrap().rule.id.as_deref(), Some("r1"));
        assert_eq!(cfg.rule_for("https://example.com", "GET").unwrap().id.as_deref(), Some("r1"));
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-core cors::` — Expected: FAIL (module `cors` not found).

- [x] **Step 3: Implement `cors.rs`**

```rust
//! Bucket CORS configuration: domain types, the canonical wire, and
//! preflight matching.
//!
//! Rules are ORDER-PRESERVING and first-origin-match (AWS
//! select-first-origin-rule semantics): the wire codec never sorts or dedupes
//! rules, and [`CorsConfig::preflight`]/[`CorsConfig::rule_for`] select the
//! first rule whose ORIGIN matches, then validate method (and, for preflight,
//! headers) within that rule only — never falling through to a later rule.

/// Maximum number of rules per configuration (AWS: 100).
pub const CORS_RULES_MAX: usize = 100;
/// Maximum length of a rule ID (AWS: 255).
pub const CORS_RULE_ID_MAX: usize = 255;
/// The `AllowedMethod` values AWS accepts (uppercase, exact).
pub const CORS_METHODS: [&str; 5] = ["GET", "PUT", "HEAD", "POST", "DELETE"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Option<Vec<String>>,
    pub expose_headers: Option<Vec<String>>,
    pub max_age_seconds: Option<i32>,
}

impl CorsRule {
    /// Whether `origin` matches any allowed-origin pattern: exact match,
    /// a bare `*`, or a single `*` wildcard (e.g. `https://*.example.net`).
    pub fn origin_matches(&self, origin: &str) -> bool {
        self.allowed_origins.iter().any(|p| pattern_matches(p, origin))
    }

    /// Whether `method` is allowed (case-insensitive exact).
    pub fn method_allows(&self, method: &str) -> bool {
        self.allowed_methods.iter().any(|m| m.eq_ignore_ascii_case(method))
    }

    /// Whether every requested header is allowed: any pattern match
    /// (`*`-wildcards; HTTP header names are case-insensitive) or, when
    /// no `allowed_headers` is set, none.
    pub fn headers_allow(&self, requested: &[String]) -> bool {
        match &self.allowed_headers {
            None => requested.is_empty(),
            Some(patterns) => {
                let patterns: Vec<String> = patterns.iter().map(|p| p.to_ascii_lowercase()).collect();
                requested.iter().all(|h| {
                    let h = h.to_ascii_lowercase();
                    patterns.iter().any(|p| pattern_matches(p, &h))
                })
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CorsConfig {
    pub rules: Vec<CorsRule>,
}

/// A preflight decision: the winning rule plus the echoed request values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightMatch {
    pub rule: CorsRule,
    pub origin: String,
    pub method: String,
    pub requested_headers: Vec<String>,
}

impl CorsConfig {
    /// First-origin-match semantics (AWS select-first-rule): iterate rules in
    /// stored order, select the FIRST rule whose `origin` matches, then
    /// validate method AND headers WITHIN that rule only — a rule that
    /// matches origin but fails method/headers returns `None` (deny) and
    /// never falls through to a later rule (see the matching-rules note).
    pub fn preflight(&self, origin: &str, method: &str, requested_headers: &[String]) -> Option<PreflightMatch> {
        let rule = self.rules.iter()
            .find(|r| r.origin_matches(origin))
            .filter(|r| r.method_allows(method) && r.headers_allow(requested_headers))?;
        Some(PreflightMatch {
            rule: rule.clone(),
            origin: origin.to_string(),
            method: method.to_string(),
            requested_headers: requested_headers.to_vec(),
        })
    }

    /// The decoration lookup for actual (non-OPTIONS) responses: the first
    /// origin-matching rule, then method validated within THAT rule only (no
    /// fall-through past an origin-match/method-mismatch rule).
    pub fn rule_for(&self, origin: &str, method: &str) -> Option<&CorsRule> {
        self.rules.iter().find(|r| r.origin_matches(origin)).filter(|r| r.method_allows(method))
    }

    pub fn to_wire(&self) -> String {
        self.rules.iter().map(|r| {
            let methods = encode(&r.allowed_methods.join(","));
            let origins = encode(&r.allowed_origins.join(","));
            let headers = r.allowed_headers.as_ref().map(|v| encode(&v.join(","))).unwrap_or_default();
            let expose = r.expose_headers.as_ref().map(|v| encode(&v.join(","))).unwrap_or_default();
            let id = r.id.as_deref().map(encode).unwrap_or_default();
            let max_age = r.max_age_seconds.map(|s| s.to_string()).unwrap_or_default();
            [methods, origins, headers, expose, id, max_age].join(",")
        }).collect::<Vec<_>>().join("&")
    }

    /// Decode the wire; any parse failure self-heals to an empty config
    /// (the `''` wire — "no CORS configuration" — is `Default`).
    pub fn from_wire(s: &str) -> Self {
        Self::parse_wire(s).unwrap_or_default()
    }

    fn parse_wire(s: &str) -> Option<Self> {
        let mut rules = Vec::new();
        for record in s.split('&').filter(|r| !r.is_empty()) {
            let mut it = record.splitn(6, ',');
            let methods = decode(it.next()?)?;
            let origins = decode(it.next()?)?;
            let headers = decode(it.next()?)?;
            let expose = decode(it.next()?)?;
            let id = decode(it.next()?)?;
            let max_age = decode(it.next()?)?;
            if rules.len() >= CORS_RULES_MAX { return None; }
            rules.push(CorsRule {
                id: if id.is_empty() { None } else { Some(id) },
                allowed_methods: split_list(&methods),
                allowed_origins: split_list(&origins),
                allowed_headers: if headers.is_empty() { None } else { Some(split_list(&headers)) },
                expose_headers: if expose.is_empty() { None } else { Some(split_list(&expose)) },
                max_age_seconds: if max_age.is_empty() { None } else { max_age.parse::<i32>().ok()? },
            });
        }
        Some(Self { rules })
    }
}

fn split_list(s: &str) -> Vec<String> {
    s.split(',').map(str::to_string).collect()
}

/// Match `pattern` (exact, `*`, or one `*` wildcard with prefix+suffix)
/// against `value`.
fn pattern_matches(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some(("*", "")) => true,             // "*" alone
        Some((prefix, suffix)) => {
            value.starts_with(prefix) && value.ends_with(suffix)
                && value.len() >= prefix.len() + suffix.len() + 1 // at least one wildcard char
        }
    }
}

// Percent encode/decode. NOTE: the full unreserved-set rule here is
// REQUIRED for the wire grammar (raw `,`/`&`/control bytes would
// mis-frame the 6-field record or smuggle header values — op-review
// C2/S1). The object.rs tags codec escapes only `% = & + space` and must
// NOT be copied as-is; when the ACL plan lands `crate::percent`, its
// encode must adopt this set (one-line coordination point).
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex(bytes.get(i + 1)?)?;
            let lo = hex(bytes.get(i + 2)?)?;
            out.push(hi << 4 | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex(b: &u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
```

(`lib.rs`: add `pub mod cors;` in the module list, alphabetical with the neighbors.)

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tinio-core` — Expected: PASS (existing suites plus the new cors tests; the tags wire paths are untouched).

- [x] **Step 5: Report pending changes** (no commit — project rule).

---

### Task 2: tinio-store — BUCKETS row extension + decode + arity pin

**Files:**
- Modify: `crates/tinio-store/src/bucket.rs`, the schema-arithmetic pin test (in `crates/tinio-store/tests/` per the shared-store plan — find it with `cargo test -p tinio-store -- --list | grep -i schema` if the location differs)
- Test: the store's schema-assertion test

**Interfaces:**
- Consumes: `tinio_core::cors::CorsConfig` from Task 1.
- Produces:
  - BUCKETS `TableDef::Value` becomes `(u64, &'static str, &'static str, &'static str, &'static str)` — `(created_at_nanos, tags_wire, owner_wire, acl_wire, cors_wire)` — **5-tuple, the ACL plan is the baseline** (grilling Q5: this plan lands after the ACL plan; the CORS element appends **fifth**)
  - `pub fn row(&self, name: &str) -> Result<Option<(SystemTime, String, String, String, String)>, Error>`
  - `pub fn put_full(&mut self, name: &str, created_at: SystemTime, tags_wire: &str, owner_wire: &str, acl_wire: &str, cors_wire: &str) -> Result<(), Error>`
  - `pub(crate) fn decode_cors_wire(wire: &str) -> CorsConfig` (empty → `Default`)

- [x] **Step 1: Reconcile the current shape, then extend the arity pin (failing)**

If the tree is somehow still the shared-store **2-tuple** (ACL not yet merged — should not happen given the Q5 sequencing), reconcile it to the ACL 4-tuple first (per the ACL plan's Task 2), then apply this task: the CORS element goes **fifth**, existing elements untouched. Never reorder elements.

**Correction 2026-09-06 (final review F1; user ruling — NO migration):** "existing elements untouched / never reorder" is the WRITE-side rule for the 5-tuple writer — it does NOT mean an older state dir keeps opening. redb 4.2 binds the BUCKETS value type name at the `TableDefinition`: an arity change makes `check_match` fail with `TableTypeMismatch` at open, so a `meta.redb` written under the 2-tuple (or any pre-CORS arity) will not open — the server refuses to start (loud failure, no silent data loss). No migration, no historical-version support; recovery = delete the state dir (user ruling). Pinned by the store test `legacy_buckets_arity_fails_loudly_on_open`.

Update the schema pin:

```rust
assert_eq!(BUCKETS.value_arity(), 5);
```

Run: `cargo test -p tinio-store schema` (or the pin test's name) — Expected: FAIL (arity still 4).

- [x] **Step 2: Extend `bucket.rs`**

```rust
use tinio_core::cors::CorsConfig;

const DEF: TableDefinition<'static, &'static str, (u64, &'static str, &'static str, &'static str, &'static str)> =
    TableDefinition::new("buckets");
```

(Keep `Def`/`Table` as is; only the `Value` type and the accessors change.)

Readable impl:
```rust
pub fn row(&self, name: &str) -> Result<Option<(SystemTime, String, String, String, String)>, Error> {
    // existing body, but read the 5-tuple and destructure (created, tags, owner, acl, cors) —
    // the SystemTime conversion stays; return the cors wire as the fifth element
}
```

Writable impl:
```rust
pub fn put_full(&mut self, name: &str, created_at: SystemTime, tags_wire: &str,
                owner_wire: &str, acl_wire: &str, cors_wire: &str) -> Result<(), Error> {
    // existing body; write (created_at_nanos, tags_wire, owner_wire, acl_wire, cors_wire)
}
pub fn put(&mut self, name: &str, created_at: SystemTime) -> Result<(), Error> {
    self.put_full(name, created_at, "", "", "", "")   // a fresh bucket has no tags/owner/ACL/CORS
}
```

Add the decode helper (same module):
```rust
/// Self-healing decode of the stored CORS wire: an empty or corrupt wire
/// is an empty config (`''` = "no configuration" = 404 on get).
pub(crate) fn decode_cors_wire(wire: &str) -> CorsConfig {
    if wire.is_empty() { CorsConfig::default() } else { CorsConfig::from_wire(wire) }
}
```

Every other accessor that destructures the row (the fs/mem call sites land in Tasks 4/5; the store's own `for_each`, `get`, `exists`, `get_or_insert`, `remove` only touch the key or the first element — verify each compiles and adapt its destructuring: `for_each` currently yields `(name, SystemTime)` — unchanged since it reads only element 0).

- [x] **Step 3: Run tests to verify they pass**

Run: `cargo test -p tinio-store` — Expected: PASS (arity pin 5; existing codec/schema tests green; the fs/mem backend suites may not compile yet until Tasks 4/5 — if `cargo test -p tinio-store` alone compiles it, it does; the workspace is red from here through Task 5, expected).

- [x] **Step 4: Report pending changes.**

---

### Task 3: tinio-core — storage contract CORS trio

**Files:**
- Modify: `crates/tinio-core/src/storage/bucket.rs`
- Test: (compile-driven; the backends implement in Tasks 4/5)

**Interfaces:**
- Consumes: `tinio_core::cors::CorsConfig` from Task 1.
- Produces (next to the tagging trio at `storage/bucket.rs:117-138`):
  - `async fn get_bucket_cors(&self, name: &bucket::Name) -> Result<Option<cors::CorsConfig>, <Self as Storage>::Error> where Self: Storage;` — missing bucket → `NoSuchBucket`; bucket with no CORS config → `Ok(None)`
  - `async fn put_bucket_cors(&self, name: &bucket::Name, cors: &cors::CorsConfig) -> Result<(), <Self as Storage>::Error> where Self: Storage;` — missing bucket → `NoSuchBucket`; replace-all (no merge). The caller (server) guarantees ≥1 rule; the contract stores what it receives
  - `async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), <Self as Storage>::Error> where Self: Storage;` — missing bucket → `NoSuchBucket`; idempotent otherwise (the delete-tagging precedent, `storage/bucket.rs:136-138`)

- [x] **Step 1: Add the three methods with doc comments in the spec/contract style** (the tagging trio's doc style — one line of intent, the 404 vs empty semantics spelled out).

- [x] **Step 2: Compile check tinio-core**

Run: `cargo test -p tinio-core --lib` — Expected: PASS (core tests don't call the backend impls; stubs in core test fixtures, if any, compile because the trait methods have no defaults — nothing to adapt in core-only tests).

- [x] **Step 3: Report pending changes.**

---

### Task 4: tinio-fs — bucket store CORS accessors + `BucketOps` impl

**Files:**
- Modify: `crates/tinio-fs/src/bucket.rs` (Store accessors + `BucketOps` methods), `crates/tinio-fs/src/lib.rs` if the `_core::cors` alias needs adding (check how `_core::object` is aliased at `bucket.rs:16-21` — mirror)
- Test: `crates/tinio-fs/src/bucket.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: Task 1 types, Task 2 rows, Task 3 contract.
- Produces: `store.cors(&bucket)`, `store.set_cors(&bucket, &config)`, `store.clear_cors(&bucket)`; the `BucketOps` impl methods used by the server (`storage.get_bucket_cors(&bucket)` etc. through the Arc deref).

- [x] **Step 1: Write the failing tests** (mirror the tags tests at the bottom of `bucket.rs`):

```rust
#[tokio::test]
async fn fs_bucket_cors_round_trip_preserves_order_and_optional_fields() {
    let (store, name) = temp_store().await.unwrap();
    store.create_bucket(&name).await.unwrap();
    let cfg = CorsConfig { rules: vec![
        CorsRule { id: Some("one".into()), allowed_methods: vec!["GET".into()], allowed_origins: vec!["*".into()],
            allowed_headers: Some(vec!["x-amz-*".into()]), expose_headers: Some(vec!["ETag".into()]), max_age_seconds: Some(60) },
        CorsRule { id: None, allowed_methods: vec!["PUT".into(), "DELETE".into()], allowed_origins: vec!["https://example.com".into()],
            allowed_headers: None, expose_headers: None, max_age_seconds: None },
    ] };
    store.set_cors(&name, &cfg).await.unwrap();
    assert_eq!(store.cors(&name).await.unwrap(), Some(cfg)); // order + fields preserved
    store.clear_cors(&name).await.unwrap();
    assert_eq!(store.cors(&name).await.unwrap(), None);      // clear → "no config"
}

#[tokio::test]
async fn fs_bucket_cors_empty_config_normalizes_to_no_config() {
    // op-review G2: a zero-rule config stored through the backend must be
    // indistinguishable from "no configuration" ('' wire → get → None).
    let (store, name) = temp_store().await.unwrap();
    store.create_bucket(&name).await.unwrap();
    store.set_cors(&name, &CorsConfig::default()).await.unwrap();
    assert_eq!(store.cors(&name).await.unwrap(), None);
}

#[tokio::test]
async fn fs_bucket_cors_missing_bucket_is_no_such_bucket() {
    let (store, name) = temp_store().await.unwrap();
    store.delete_bucket(&name).await.unwrap();
    let cfg = CorsConfig { rules: vec![CorsRule { id: None, allowed_methods: vec!["GET".into()],
        allowed_origins: vec!["*".into()], allowed_headers: None, expose_headers: None, max_age_seconds: None }] };
    assert!(store.set_cors(&name, &cfg).await.unwrap_err().is_no_such_bucket());
    assert!(store.cors(&name).await.unwrap_err().is_no_such_bucket()); // probes exist on missing bucket
}
```

(Adjust `is_no_such_bucket()` to the fs error API used by the tags tests; garbage self-heal is pinned by the store `decode_cors_wire` test in Task 2 rather than duplicating here.)

- [x] **Step 2: Run to verify failure** — `cargo test -p tinio-fs fs_bucket_cors_` — Expected: FAIL (methods missing).

- [x] **Step 3: Implement the Store accessors** (next to `tags`/`set_tags`/`clear_tags`, `bucket.rs:70-135`):

```rust
pub async fn cors(&self, name: &Name) -> Result<Option<cors::CorsConfig>, Error> {
    self.handle.read(move |txn| {
        let table = bucket::Table::open_readonly(txn)?;
        Ok(table.row(name.as_str())?.map(|r| r.value()).transpose()?
            .map(|(_, _, _, _, cors_wire)| bucket::decode_cors_wire(&cors_wire))
            .filter(|c| !c.rules.is_empty()))
    }).await
}

pub async fn set_cors(&self, name: &Name, config: &cors::CorsConfig) -> Result<(), Error> {
    self.handle.write(move |txn| {
        let mut table = bucket::Table::open(txn)?;
        let row = match table.row(name.as_str())?.map(|r| r.value()).transpose()? {
            Some(r) => r,
            None => return Err(Error::NoSuchBucket(name.clone())),
        };
        let (created, tags_wire, owner_wire, acl_wire, _) = row;
        // op-review G2: an empty rule set normalizes to the `''` wire — a
        // zero-rule config must never be stored as a non-empty row.
        let cors_wire = if config.rules.is_empty() { String::new() } else { config.to_wire() };
        table.put_full(name.as_str(), created, &tags_wire, &owner_wire, &acl_wire, &cors_wire)?;
        Ok(())
    }).await
}

/// Idempotent: a bucket with no CORS config clears to the same state.
pub async fn clear_cors(&self, name: &Name) -> Result<(), Error> {
    self.handle.write(move |txn| {
        let mut table = bucket::Table::open(txn)?;
        let row = match table.row(name.as_str())?.map(|r| r.value()).transpose()? {
            Some(r) => r,
            None => return Err(Error::NoSuchBucket(name.clone())),
        };
        let (created, tags_wire, owner_wire, acl_wire, _) = row;
        table.put_full(name.as_str(), created, &tags_wire, &owner_wire, &acl_wire, "")?;
        Ok(())
    }).await
}
```

(The exact read/write path shape follows the file's `tags`/`set_tags` bodies — same `self.handle.read`/`write` + `Error` mapping conventions; `decode_cors_wire` is `tinio_store::bucket::decode_cors_wire`, re-exported through the crate's `_store` alias if the store's module isn't public — check and make it `pub` in `tinio_store::bucket` and re-export.)

Then the `BucketOps` impl: `get_bucket_cors` → `self.bucket_store.cors(name)`, `put_bucket_cors` → `self.bucket_store.set_cors(name, config)`, `delete_bucket_cors` → `self.bucket_store.clear_cors(name)` (follow the existing tagging impl bodies — where `BucketOps for FsStorage` lives, probably `backend/buckets.rs` or `bucket.rs`; the fs `create_bucket`/`put_bucket_tags` impls currently call `Self::bucket_store(...)` or an owned `bucket_store` handle — mirror exactly).

- [x] **Step 4: Run tests** — `cargo test -p tinio-fs` — Expected: PASS.

- [x] **Step 5: Report pending changes.**

---

### Task 5: tinio-mem — mirror + workspace green

**Files:**
- Modify: `crates/tinio-mem/src/bucket.rs`
- Test: in-module tests mirroring Task 4

**Interfaces:**
- Produces: the `BucketOps` trio on `MemoryStorage`; helper `rewrite_bucket_cors_element(&self, name: &Name, cors_wire: &str) -> Result<bool, Error>` (mirrors `rewrite_bucket_tags_element`, `bucket.rs:164`).

- [x] **Step 1: Implement** — `get_bucket_cors`/`put_bucket_cors`/`delete_bucket_cors` mirroring the tags trio (`bucket.rs:118-160`); the put/delete read-modify-write goes through `rewrite_bucket_cors_element` exactly like the tags counterpart:

```rust
pub async fn put_bucket_cors(&self, name: &Name, config: &cors::CorsConfig) -> Result<(), Error> {
    // op-review G2: an empty rule set normalizes to the `''` wire.
    let wire = if config.rules.is_empty() { String::new() } else { config.to_wire() };
    if let Some(bucket_exists) = self.rewrite_bucket_cors_element(name, &wire).await? {
        let _ = bucket_exists;
        Ok(())
    } else {
        Err(Error::no_such_bucket(name))
    }
}
```

(Adapt to the existing local `put_bucket_tags` body — the mem store opens `bucket::Table` over its own redb backend and its `NoSuchBucket` construction; read `bucket.rs:136-146` first and keep its shape. The 5-tuple rewrite must preserve all prior elements and set the CORS element 5th.)

- [x] **Step 2: Verify the workspace compiles again**

`cargo check -p tinio-core -p tinio-store -p tinio-fs -p tinio-mem -p tinio-server` — Expected: green (server currently only compiles because the contract additions have defaults-free but no call sites yet — the server jobs in Tasks 7/8).

- [x] **Step 3: Run** `cargo test -p tinio-mem` — Expected: PASS.

- [x] **Step 4: Report pending changes.**

---

### Task 6: tinio-util conformance — CORS blocks

**Files:**
- Modify: `crates/tinio-util/src/testing.rs` (after the tagging blocks)
- Test: run via the conformance suites of both backends

**Interfaces:**
- Consumes: the contract trio (Tasks 3-5).

- [x] **Step 1: Add the conformance block** (the harness is backend-agnostic):

```rust
// in conformance_buckets (after the tagging block), using the existing store fixture:
let config = CorsConfig { rules: vec![
    CorsRule { id: Some("one".into()), allowed_methods: vec!["GET".into()], allowed_origins: vec!["*".into()],
        allowed_headers: None, expose_headers: None, max_age_seconds: None },
] };
store.put_bucket_cors(&bucket, &config).await.unwrap();
assert_eq!(store.get_bucket_cors(&bucket).await.unwrap(), Some(config));
// op-review G2: a zero-rule config goes through the *storage* layer as "no config".
store.put_bucket_cors(&bucket, &CorsConfig::default()).await.unwrap();
assert_eq!(store.get_bucket_cors(&bucket).await.unwrap(), None);
store.put_bucket_cors(&bucket, &config).await.unwrap();
assert_eq!(store.get_bucket_cors(&bucket).await.unwrap(), Some(config));
assert_eq!(store.get_bucket_cors(&missing_bucket).await.unwrap_err(), /* NoSuchBucket */);
store.delete_bucket_cors(&bucket).await.unwrap();
assert_eq!(store.get_bucket_cors(&bucket).await.unwrap(), None);   // deleted → "no config"
assert!(store.delete_bucket_cors(&bucket).await.is_ok());          // idempotent delete
```

(Exact placement and the `NoSuchBucket` comparison follow the existing block style at the tagging section; both backends run the harness.)

- [x] **Step 2: Run and fix** — `cargo test -p tinio-fs -p tinio-mem conformance` — Expected: PASS.

- [x] **Step 3: Report pending changes.**

---

### Task 7: tinio-config — `Capabilities.cors`

**Files:**
- Modify: `crates/tinio-config/src/schema/s3.rs` (the `Capabilities` struct at `:25-87`)
- Test: config in-module test (`cors_defaults_on`, next to `tagging_defaults_on`)

**Interfaces:**
- Produces: `Capabilities.cors: bool` (SmartDefault `true`, next to `tagging`); `From<&Config>` at `s3.rs:163-167` carries it automatically (field-for-field).

- [x] **Step 1: Write the failing test**

```rust
#[test]
fn cors_defaults_on() {
    let caps = Capabilities::default();
    assert!(caps.cors);
    // a config with `cors = false` surfaces the toggle:
    let cfg: Config = toml::from_str("[s3.capabilities]\ncors = false\n").unwrap();
    assert!(!Capabilities::from(&cfg).cors);
}
```

(Follow the exact `tagging_defaults_on` test shape in the config crate — it may assert via the full `Config` parse; mirror it.)

- [x] **Step 2: Implement** — add the field + default; also update the `@minimal-caps` e2e spawn helper — **required, not conditional**: in `crates/tinio-e2e/tests/steps/mod.rs` `config_from_tags` (the `minimal-caps` block, `:109-116`), the cleared set gets `caps.cors = false;` alongside the other toggles (the block sets every toggle false explicitly, so `cors` must be added here or `@minimal-caps` scenarios would run with CORS on).

- [x] **Step 3: Run** — `cargo test -p tinio-config` — Expected: PASS; `cargo test -p tinio-e2e --no-run` compiles (the minimal-caps helper edit lands here so the e2e crate stays green).

- [x] **Step 4: Report pending changes.**

---

### Task 8: tinio-server — feature gate + the three CORS ops

**Files:**
- Create: `crates/tinio-server/src/backend/cors.rs`
- Modify: `crates/tinio-server/Cargo.toml` (`cors = []`, added to `default`), `crates/tinio-server/src/backend/mod.rs` (`#[cfg(feature = "cors")] mod cors;`, `S3Backend::new_shared`), `crates/tinio-server/src/backend/s3.rs` (three forwards, `#[cfg(feature = "cors")]`), `crates/tinio-server/src/metrics.rs` (three wrappers, cfg-gated)
- Test: `crates/tinio-server/src/backend/cors.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: Tasks 1/3 storage, `map_backend_error` (`backend/errors.rs:7`), `self.bucket(raw: String)` (`backend/mod.rs`), `Self::require_cap(caps, name)`.
- Produces: `op_get_bucket_cors` / `op_put_bucket_cors` / `op_delete_bucket_cors` on `S3Backend` (trait forwards read them via `self.op_*`), `cors_rules_to_dto(&CorsConfig) -> dto::CORSRules`, `cors_config_from_dto(dto::CORSConfiguration) -> S3Result<CorsConfig>`, `validate_content_md5(Option<&str>) -> S3Result<()>` (AWS three-state: missing → `InvalidRequest` with the verified AWS message, malformed → `InvalidDigest` — grilling Q8), `fn config_bytes(&CorsConfig) -> usize` (decoded size cap, op-review P1).
- `S3Backend::new_shared(storage: Arc<S>, caps) -> Self` — **UNGATED** (exists in every build; the shared-`Arc<S>` constructor so the route, decorator, and backend share one storage handle). `S3Backend::new(storage, caps)` (by value) **delegates to `new_shared`** (wrapping `storage` in `Arc` first), so existing callers are unchanged and feature-off builds keep a constructor; only the `CorsLookup` parameter and the cors wiring (route registration, decorator, lookup field on `DataPlaneService`) are `#[cfg(feature = "cors")]`.
- Double gate: `[features]` gains `cors = []` and `default = ["multipart", "copy", "list-v1", "list-v2", "cors"]`; `#[cfg(feature = "cors")]` on: `mod cors;`, the three s3.rs forwards, the three MetricS3 wrappers. Capability off (feature on) → the op `require_cap` 501 `"{name} is disabled"`; feature off → the code is not compiled and s3s's trait defaults answer `"{name} is not implemented yet"` (op-review C3 split).

- [x] **Step 1: Write the failing tests** (in `backend/cors.rs`, using the existing `testutil` fixtures/tagged ops test style — see `buckets.rs` tests at `:801-935`):

```rust
#[tokio::test]
async fn bucket_cors_ops_round_trip_and_delete() {
    // put → get echoes (order preserved, optional fields); delete → get 404 NoSuchCORSConfiguration
    // (build the plane via the testutil helper and dispatch the three ops through the S3 impl)
}

#[tokio::test]
async fn put_bucket_cors_requires_content_md5() {
    // missing → 400 InvalidRequest "Missing required header for this request: Content-MD5"
    //   (grilling Q8, verified AWS wire behavior — NOT InvalidDigest, differs from the ACL plan's A7);
    // malformed (not 16-byte base64) → 400 InvalidDigest
}

#[tokio::test]
async fn put_bucket_cors_validation_rejects_bad_config() {
    // empty rules → 400 InvalidRequest; 101 rules → 400 InvalidRequest; id > 255 → 400 InvalidRequest;
    // allowed_method row "PATCH" → 400 InvalidRequest; pattern with two '*' → 400 InvalidRequest (grilling Q6);
    // pattern/ID/expose value with a C0 control byte or DEL → 400 InvalidRequest (op-review S1);
    // a ',' inside an origin/header/expose pattern or a rule ID → 400 InvalidRequest (F1);
    // a rule with an EMPTY AllowedMethod list, or an EMPTY AllowedOrigin list → 400 InvalidRequest (F5);
    // max_age_seconds < 0 → 400 InvalidRequest (op-review G1);
    // decoded config over CORS_CONFIG_BYTES_MAX (64 KB) → 400 InvalidRequest (op-review P1)
}

#[tokio::test]
async fn cors_toggle_off_gates_the_bucket_cors_ops() { /* caps.cors = false → NotImplemented 501 "{name} is disabled" for all three */ }
```

- [x] **Step 2: Implement `backend/cors.rs`** (the tags-ops pattern, plus the two validation helpers):

```rust
use s3s::{
    Body, S3Request, S3Response, S3Result, dto,
    s3_error,
};
use crate::{
    _core::{bucket, cors},
    backend::{buckets as _, errors::map_backend_error, mod::S3Backend},
}; // adjust to the crate's real import idioms (3+ segments, docs/style.md)

impl<S> S3Backend<S> where S: crate::_core::storage::Storage {
    pub(crate) async fn op_get_bucket_cors(
        &self,
        req: S3Request<dto::GetBucketCorsInput>,
    ) -> S3Result<S3Response<dto::GetBucketCorsOutput>> {
        Self::require_cap(self.caps.cors, "GetBucketCors")?;
        let bucket = self.bucket(req.input.bucket)?;
        match self.storage
            .get_bucket_cors(&bucket)
            .await
            .map_err(map_backend_error)?
        {
            Some(config) => Ok(S3Response::new(dto::GetBucketCorsOutput {
                cors_rules: Some(cors_rules_to_dto(&config)),
            })),
            None => Err(s3_error!(NoSuchCORSConfiguration, "The CORS configuration does not exist")),
        }
    }

    pub(crate) async fn op_put_bucket_cors(
        &self,
        req: S3Request<dto::PutBucketCorsInput>,
    ) -> S3Result<S3Response<dto::PutBucketCorsOutput>> {
        Self::require_cap(self.caps.cors, "PutBucketCors")?;
        let bucket = self.bucket(req.input.bucket)?;
        validate_content_md5(req.input.content_md5.as_deref())?;
        let config = cors_config_from_dto(&req.input.cors_configuration)?;
        self.storage
            .put_bucket_cors(&bucket, &config)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutBucketCorsOutput::default()))
    }

    pub(crate) async fn op_delete_bucket_cors(
        &self,
        req: S3Request<dto::DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketCorsOutput>> {
        Self::require_cap(self.caps.cors, "DeleteBucketCors")?;
        let bucket = self.bucket(req.input.bucket)?;
        self.storage
            .delete_bucket_cors(&bucket)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::DeleteBucketCorsOutput::default()))
    }
}

/// Content-MD5 on PutBucketCors, AWS three-state (grilling Q8): missing →
/// 400 InvalidRequest with the verified AWS message; malformed (not
/// 16-byte base64) → 400 InvalidDigest; digest equality vs the XML body is
/// unverifiable here (s3s consumes the body — recorded deviation, AWS's
/// BadDigest path unreachable).
/// NOTE (coordination): the ACL plan's A7 ruled "missing OR malformed →
/// InvalidDigest" for the put-ACL ops; when ACL merges, its helper is
/// aligned to this three-state behavior (or this helper is made shared
/// with the AWS behavior) — one-line change at that merge.
fn validate_content_md5(md5: Option<&str>) -> S3Result<()> {
    let md5 = md5.ok_or_else(|| s3_error!(InvalidRequest, "Missing required header for this request: Content-MD5"))?;
    let raw = base64::engine::general_purpose::STANDARD.decode(md5)
        .map_err(|_| s3_error!(InvalidDigest, "The Content-MD5 you specified is not valid"))?;
    if raw.len() == 16 { Ok(()) } else { Err(s3_error!(InvalidDigest, "The Content-MD5 you specified is not valid")) }
}

/// Any C0 control byte (<0x20) or DEL (0x7f) — such bytes may not reach
/// response headers (op-review S1): HeaderValue construction rejects them,
/// and a stored poison would panic the per-connection task.
fn has_control_bytes(s: &str) -> bool {
    s.bytes().any(|b| b < 0x20 || b == 0x7f)
}

/// Decoded config size in bytes (op-review P1 — bounds the per-Origin
/// decode/scan amplification; the put body is otherwise limited only by
/// s3s's 20 MB XML cap).
fn config_bytes(config: &cors::CorsConfig) -> usize {
    config.rules.iter().map(|r| {
        r.id.as_deref().map_or(0, str::len)
            + r.allowed_methods.iter().map(String::len).sum::<usize>()
            + r.allowed_origins.iter().map(String::len).sum::<usize>()
            + r.allowed_headers.as_ref().map_or(0, |v| v.iter().map(String::len).sum())
            + r.expose_headers.as_ref().map_or(0, |v| v.iter().map(String::len).sum())
    }).sum()
}

/// dto → core conversion with request-level validation (400 InvalidRequest on
/// malformed configs; the storage codec's self-heal applies to rows, never input).
fn cors_config_from_dto(xml: &dto::CORSConfiguration) -> S3Result<cors::CorsConfig> {
    let rules = &xml.cors_rules;
    if rules.is_empty() {
        return Err(s3_error!(InvalidRequest, "The CORS configuration must have at least one rule"));
    }
    if rules.len() > cors::CORS_RULES_MAX {
        return Err(s3_error!(InvalidRequest, format!("The CORS configuration must have at most {} rules", cors::CORS_RULES_MAX)));
    }
    let mut out = Vec::with_capacity(rules.len());
    for r in rules {
        if let Some(id) = &r.id && id.chars().count() > cors::CORS_RULE_ID_MAX {
            return Err(s3_error!(InvalidRequest, "The rule ID must be at most 255 characters"));
        }
        // F5: the dto Vecs are non-Option but can be EMPTY from an XML body with
        // zero such elements — every rule must name ≥1 method and ≥1 origin.
        if r.allowed_methods.is_empty() {
            return Err(s3_error!(InvalidRequest, "Each CORS rule must have at least one AllowedMethod"));
        }
        if r.allowed_origins.is_empty() {
            return Err(s3_error!(InvalidRequest, "Each CORS rule must have at least one AllowedOrigin"));
        }
        for m in &r.allowed_methods {
            if !cors::CORS_METHODS.iter().any(|v| v.eq_ignore_ascii_case(m)) {
                return Err(s3_error!(InvalidRequest, format!("Invalid AllowedMethod: {m}")));
            }
        }
        // grilling Q6 = (b): ≤1 `*` per pattern; op-review S1: no control bytes;
        // F1: no `,` in any list item (an unescaped `,` would split the 6-field wire record).
        for (what, patterns) in [("AllowedOrigin", &r.allowed_origins), ("AllowedHeader", r.allowed_headers.as_ref())] {
            if let Some(patterns) = patterns {
                if patterns.iter().any(|p| p.bytes().filter(|b| *b == b'*').count() > 1 || has_control_bytes(p) || p.contains(',')) {
                    return Err(s3_error!(InvalidRequest, format!("Invalid {what} pattern in the CORS configuration")));
                }
            }
        }
        if let Some(expose) = &r.expose_headers && expose.iter().any(|e| has_control_bytes(e) || e.contains(',')) {
            return Err(s3_error!(InvalidRequest, "Invalid ExposeHeader in the CORS configuration"));
        }
        if r.id.as_ref().is_some_and(|id| has_control_bytes(id) || id.contains(',')) {
            return Err(s3_error!(InvalidRequest, "Invalid rule ID in the CORS configuration"));
        }
        if r.max_age_seconds.is_some_and(|m| m < 0) {
            return Err(s3_error!(InvalidRequest, "MaxAgeSeconds must be non-negative"));
        }
        out.push(cors::CorsRule {
            id: r.id.clone(),
            allowed_methods: r.allowed_methods.iter().map(|m| m.to_ascii_uppercase()).collect(),
            allowed_origins: r.allowed_origins.clone(),
            allowed_headers: r.allowed_headers.clone(),
            expose_headers: r.expose_headers.clone(),
            max_age_seconds: r.max_age_seconds,
        });
    }
    let config = cors::CorsConfig { rules: out };
    if config_bytes(&config) > cors::CORS_CONFIG_BYTES_MAX {
        return Err(s3_error!(InvalidRequest, "The CORS configuration is too large"));
    }
    Ok(config)
}

fn cors_rules_to_dto(config: &cors::CorsConfig) -> dto::CORSRules {
    config.rules.iter().map(|r| dto::CORSRule {
        id: r.id.clone(),
        allowed_methods: r.allowed_methods.clone(),
        allowed_origins: r.allowed_origins.clone(),
        allowed_headers: r.allowed_headers.clone(),
        expose_headers: r.expose_headers.clone(),
        max_age_seconds: r.max_age_seconds,
    }).collect()
}
```

(Arm alignment: write the helpers as free functions — the ops above are `impl S3Backend<S>`; the existing ops use the `buckets.rs` module with `use super::*`-style imports — match the file's idiom.)

`backend/s3.rs` forwards (three, exactly like the tagging trio at `:46-65` — each in a `#[cfg(feature = "cors")]` block, so a feature-off build compiles no overrides and s3s's trait defaults answer):

```rust
#[cfg(feature = "cors")]
async fn get_bucket_cors(&self, req: S3Request<dto::GetBucketCorsInput>) -> S3Result<S3Response<dto::GetBucketCorsOutput>> {
    self.op_get_bucket_cors(req).await
}
```

`backend/mod.rs`: `#[cfg(feature = "cors")] mod cors;` + the shared constructor (final-spec ruling: `new_shared` is **UNGATED** — it exists in every build, wrapping `storage` in the shared `Arc<S>`; `new` **delegates to it**, so feature-off builds still have a constructor; only the `CorsLookup` param and the cors wiring are gated):

```rust
pub fn new(storage: S, caps: Capabilities) -> Self {
    Self::new_shared(Arc::new(storage), caps)   // delegates to the ungated new_shared
}

pub fn new_shared(storage: Arc<S>, caps: Capabilities) -> Self {
    // today's `new` body, with `storage` already an Arc — the shared handle the
    // route, decorator, and backend all use
}
```
(Feature-off builds compile `new_shared` too — no `#[cfg]` on it — so the `--no-default-features`/`DataPlane::new` constructor is preserved, per the final spec §5.)

`metrics.rs` wrappers (`MetricS3`, next to the tagging wrappers at `:1130-1136`; each `#[cfg(feature = "cors")]`):

```rust
async fn get_bucket_cors(&self, req: S3Request<dto::GetBucketCorsInput>) -> S3Result<S3Response<dto::GetBucketCorsOutput>> {
    self.record("GetBucketCors", self.inner.get_bucket_cors(req)).await
}
// + put_bucket_cors ("PutBucketCors"), delete_bucket_cors ("DeleteBucketCors")
```

- [x] **Step 3: Run** — `cargo test -p tinio-server bucket_cors_` and `cargo test -p tinio-server cors_` — Expected: PASS.

- [x] **Step 4: Report pending changes.**

---

### Task 9: tinio-server — preflight route (`CorsPreflightRoute`)

**Files:**
- Modify: `crates/tinio-server/src/backend/cors.rs` (add `CorsConfigs`, `CorsLookup`, `CorsPreflightRoute`, `bucket_from_uri`, `cors_denied()`), `crates/tinio-server/src/data.rs` (wiring: `new`/`new_with_auth` build `Arc<S>`, `new_shared` (ungated), `set_route`, and thread a cors-gated `Arc<dyn CorsLookup>` through `from_service` → `DataPlaneService::new` field), `crates/tinio-server/src/data.rs` tests (`spawn_plane` + a new raw-HTTP helper)
- Test: route unit tests + one plane-level OPTIONS test

**Interfaces:**
- Consumes: Tasks 1/3/8, `s3s::route::S3Route`, `s3s::path::parse_path_style`.
- Produces:
  - `pub(crate) trait CorsLookup: Send + Sync { async fn get(&self, bucket: &str) -> Option<cors::CorsConfig>; }` — **`#[async_trait::async_trait]`** (a native async-fn-in-trait is NOT dyn-compatible on stable Rust, E0038 — the macro is required because the decorator holds the erased `Arc<dyn CorsLookup>`; the workspace's `async-trait` 0.1 pattern, cf. the storage traits)
  - `pub(crate) struct CorsConfigs<S: Storage> { storage: Arc<S> }` — `new(Arc<S>)`, `async fn get(&self, bucket: &str) -> Option<cors::CorsConfig>` (missing bucket → `None`; empty config → `None`), `#[async_trait::async_trait] impl CorsLookup for CorsConfigs<S>`
  - `#[derive(Clone)] pub(crate) struct CorsPreflightRoute<S: Storage> { configs: Arc<CorsConfigs<S>> }` — `new(Arc<CorsConfigs<S>>) -> Self`; `impl S3Route`
  - `pub(crate) fn bucket_from_uri(uri: &Uri) -> Option<&str>`

- [x] **Step 1: Route unit tests**

```rust
#[tokio::test]
async fn preflight_matches_allowed_origin_and_answers_headers() {
    let cfg = CorsConfig { rules: vec![/* one rule: GET+PUT, https://example.com, headers x-amz-*, expose ETag, max 300 */] };
    let route = test_route(cfg);                      // a CorsConfigs over a tiny MemoryStorage with the config set
    let req = preflight_req("https://example.com", "PUT", Some("x-amz-foo"));
    let resp = route.call(req).await.unwrap();
    assert_eq!(resp.headers["access-control-allow-origin"], "https://example.com");
    // grilling Q9: Allow-Methods is the RULE's method list, not the requested method
    assert_eq!(resp.headers["access-control-allow-methods"], "GET, PUT");
    assert_eq!(resp.headers["access-control-allow-headers"], "x-amz-foo"); // request's own case/spelling
    assert_eq!(resp.headers["access-control-expose-headers"], "ETag");
    assert_eq!(resp.headers["access-control-max-age"], "300");
    assert_eq!(resp.headers["access-control-allow-credentials"], "true");
    // grilling Q4: Vary trio present
    assert_eq!(resp.headers["vary"], "Origin, Access-Control-Request-Headers, Access-Control-Request-Method");
    assert_eq!(resp.headers["content-length"], "0"); // op-review G4: set explicitly
}

#[tokio::test]
async fn preflight_bare_star_rule_answers_literal_star_without_credentials() {
    // grilling Q11: origin rule = "*" → ACAO literal "*", Allow-Credentials OMITTED
    let cfg = CorsConfig { rules: vec![/* one rule: GET, origins ["*"] */] };
    let route = test_route(cfg);
    let resp = route.call(preflight_req("https://example.com", "GET", None)).await.unwrap();
    assert_eq!(resp.headers["access-control-allow-origin"], "*");
    assert!(!resp.headers.contains_key("access-control-allow-credentials"));
}

#[tokio::test]
async fn preflight_disallowed_and_no_config_answer_403_with_aws_messages() {
    // grilling Q10: rule mismatch → 403, code AccessDenied, message
    // "CORSResponse: This CORS request is not allowed. This is usually because the evalution of ..." (verbatim);
    // no CORS config → 403 "CORS is not enabled for this bucket."; neither carries any Access-Control-* header.
}

#[tokio::test]
async fn preflight_missing_bucket_uses_the_same_no_config_message() {
    // N1 / existence-oracle closure: a well-formed but MISSING bucket resolves
    // to `CorsConfigs::get → None`, which the route maps to the SAME
    // "CORS is not enabled for this bucket." message as the no-config case —
    // a probe cannot distinguish "bucket exists, no CORS" from "bucket missing".
}

#[tokio::test]
async fn preflight_invalid_bucket_name_is_400_by_s3s_not_403() {
    // op-review C1: a syntactically invalid name (e.g. an uppercase bucket or a
    // path with an invalid char) is answered by s3s with 400 InvalidBucketName
    // BEFORE the route matches — the route's own parse only sees valid names.
    // Exercise via the plane: OPTIONS /MyBucket with Origin+ACRM → 400, not 403.
}
```

(Follow the in-module test fixtures of `backend/cors.rs`; the preflight request is built by hand: `S3Request { input: Body::empty(), method: Method::OPTIONS, uri: "/bucket".parse(), headers: [origin + access-control-request-method], .. }`.)

- [x] **Step 2: Implement the route**

```rust
use http::header;   // or the crate's http-import idiom

#[async_trait::async_trait]
pub(crate) trait CorsLookup: Send + Sync {
    async fn get(&self, bucket: &str) -> Option<cors::CorsConfig>;
}

pub(crate) struct CorsConfigs<S: Storage> { storage: Arc<S> }
impl<S: Storage> CorsConfigs<S> {
    pub fn new(storage: Arc<S>) -> Self { Self { storage } }
    pub async fn get(&self, bucket: &str) -> Option<cors::CorsConfig> {
        match bucket::name(bucket) {
            Ok(name) => self.storage.get_bucket_cors(&name).await.ok().flatten(),
            Err(_) => None,
        }
    }
}
#[async_trait::async_trait]
impl<S: Storage> CorsLookup for CorsConfigs<S> {
    async fn get(&self, bucket: &str) -> Option<cors::CorsConfig> { CorsConfigs::get(self, bucket).await }
}

/// The bucket for path-style requests ("/bucket/key" → "bucket"). The tinio
/// data plane serves path-style only (no virtual-hosted S3Host configured),
/// so this parse is consistent with s3s's own op routing.
pub(crate) fn bucket_from_uri(uri: &Uri) -> Option<&str> {
    s3s::path::parse_path_style(uri.path()).ok()?.as_bucket()
}

fn cors_denied() -> S3Result<S3Response<Body>> {
    Err(s3_error!(AccessDenied, "CORSResponse: This CORS request is not allowed."))
}

#[async_trait::async_trait]
impl<S: Storage> s3s::route::S3Route for CorsPreflightRoute<S> {
    fn is_match(&self, method: &Method, _uri: &Uri, headers: &HeaderMap, _extensions: &mut Extensions) -> bool {
        // A true preflight: browsers send Origin + Access-Control-Request-Method.
        // Bare OPTIONS (non-browser probes) fall through to s3s → 501 as today.
        *method == Method::OPTIONS
            && headers.contains_key(header::ORIGIN)
            && headers.contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
    }

    // Preflight is anonymous by definition (browsers can't sign it); the
    // default S3Route check_access would demand credentials — override.
    async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> { Ok(()) }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let origin = req.headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()).ok_or_else(|| cors_denied_err())?;
        let method = req.headers.get(header::ACCESS_CONTROL_REQUEST_METHOD).and_then(|v| v.to_str().ok()).ok_or_else(|| cors_denied_err())?;
        let requested_headers: Vec<String> = req.headers
            .get_all(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .collect();
        // op-review C1: s3s already validated the path in `prepare` (400 on
        // invalid names) before the route matches — parse succeeds here or
        // the request never reached us; the guard stays as defense.
        let bucket = bucket_from_uri(&req.uri).ok_or_else(|| cors_denied_err())?;
        let Some(config) = self.configs.get(bucket).await else { return Err(cors_denied_err_at("CORS is not enabled for this bucket.")); };
        let Some(matched) = config.preflight(origin, method, &requested_headers) else {
            return Err(cors_denied_err_at(CORS_DENIED_MISMATCH_MSG));
        };

        let mut resp = S3Response::new(Body::empty());
        // op-review S1: fallible HeaderValue construction — a value that is
        // not settable as a header is SKIPPED, never unwrapped/panicked.
        let star_rule = matched.rule.allowed_origins.iter().any(|o| o == "*"); // grilling Q11
        let acao = if star_rule { "*" } else { matched.origin.as_str() };
        if let Ok(v) = header::HeaderValue::from_str(acao) {
            resp.headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
        }
        // grilling Q9: the RULE's method list (requested-method echo is stale AWS docs behavior).
        let methods = matched.rule.allowed_methods.join(", ");
        if let Ok(v) = header::HeaderValue::from_str(&methods) {
            resp.headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, v);
        }
        if !matched.requested_headers.is_empty() {
            let joined = matched.requested_headers.join(", ");
            if let Ok(v) = header::HeaderValue::from_str(&joined) {
                resp.headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, v);
            }
        }
        if !star_rule {
            resp.headers.insert(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true"); // omitted for `*` (Q11)
        }
        if let Some(expose) = &matched.rule.expose_headers && !expose.is_empty() {
            let joined = expose.join(", ");
            if let Ok(v) = header::HeaderValue::from_str(&joined) {
                resp.headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
            }
        }
        if let Some(max_age) = matched.rule.max_age_seconds {
            resp.headers.insert(header::ACCESS_CONTROL_MAX_AGE, max_age.to_string());
        }
        // grilling Q4 + op-review G3: append (merge), never replace an existing Vary.
        for v in ["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method"] {
            resp.headers.append(header::VARY, v);
        }
        // op-review G4: s3s copies custom-route headers verbatim and sets nothing.
        resp.headers.insert(header::CONTENT_LENGTH, "0");
        Ok(resp)
    }
}
```

(The two 403 helpers: `cors_denied_err_at(msg) -> S3Error` builds `s3_error!(AccessDenied, msg)`, with `CORS_DENIED_MISMATCH_MSG` = the verbatim AWS `"CORSResponse: This CORS request is not allowed. This is usually because the evalution of Origin, request method / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by the resource's CORS spec."` — the "evalution" typo included, grilling Q10; the no-config variant is `"CORS is not enabled for this bucket."`. The `?`-sites and the `return`-sites both route through the helper so the code stays shared. Denials carry no `Access-Control-*` headers and no `Vary` — `serialize_error` produces only the XML body.)

- [x] **Step 3: Wire `data.rs`** — `new`/`new_with_auth` share the `Arc<S>`, build the backend via `new_shared`, and thread the cors lookup onto `DataPlaneService`:

```rust
pub fn new<S: Storage>(storage: S, caps: Capabilities) -> Self {
    let storage = Arc::new(storage);
    // new_shared is UNGATED — `S3Backend::new` delegates to it (Task 8).
    let backend = MetricS3::new(S3Backend::new_shared(Arc::clone(&storage), caps));
    let mut builder = S3ServiceBuilder::new(backend);
    // Only the cors wiring is feature-gated; the field is set (or None) below.
    #[cfg(feature = "cors")]
    {
        // double gate: the feature AND the runtime capability arm the route.
        let configs = Arc::new(CorsConfigs::new(Arc::clone(&storage)));
        if caps.cors {
            builder.set_route(CorsPreflightRoute::new(Arc::clone(&configs)));
        }
        // The erased lookup rides on DataPlaneService (set via its `new`),
        // threaded through a cors-gated from_service parameter. When the
        // capability is off the field is None (no decoration, no route).
        return Self::from_service(builder.build(), caps.cors.then(|| configs as Arc<dyn CorsLookup>));
    }
    #[cfg(not(feature = "cors"))]
    Self::from_service(builder.build(), None)
}
```

(`new_with_auth` mirrors: the `#[cfg(feature = "cors")]` arm additionally `set_auth(StaticAuth…)`; the non-feature arm is today's body verbatim. `DataPlaneService.cors` is `Option<Arc<dyn CorsLookup>>` — `None` when feature or capability is off.)

(`data.rs` structure: the `Option<Arc<dyn CorsLookup>>` field lives on `DataPlaneService` (`data.rs:238-243`); `DataPlaneService::new` (`data.rs:256-262`) gains the field (default `None`), and `DataPlane::from_service` (`data.rs:139-143`) gains a cors-gated `Option<Arc<dyn CorsLookup>>` parameter to thread it through for Task 10. `S3Backend::new` (non-shared, delegating) stays for tests.)

- [x] **Step 4: Plane-level test** — extend the `data.rs` test module with a raw-HTTP helper (mirror the existing `spawn_plane` + the metrics test's client plumbing; the workspace has no reqwest — use the hyper legacy client or a raw `TcpStream` + `http` types per the existing tests):

```rust
#[tokio::test]
async fn options_preflight_answered_on_the_plane() {
    // seed the mem storage with a CORS config for "b" (via the storage handle),
    // spawn_plane (Capabilities::default → cors on), then:
    //   OPTIONS /b/key  Origin: https://example.com, Access-Control-Request-Method: PUT
    // → 200, access-control-allow-origin: https://example.com, allow-methods: PUT
    //   without Origin → 501 (s3s unknown operation — the old behavior)
}
```

- [x] **Step 5: Run** — `cargo test -p tinio-server preflight` and `cargo test -p tinio-server options_` — Expected: PASS.

- [x] **Step 6: Report pending changes.**

---

### Task 10: tinio-server — Origin decoration of actual responses

**Files:**
- Modify: `crates/tinio-server/src/data.rs` (`DataPlaneService.cors` field — set in Task 9 — and the decoration in `call_with_peer`)
- Test: `crates/tinio-server/src/data.rs` tests

**Interfaces:**
- Consumes: Task 9's `CorsLookup`/`bucket_from_uri`, Task 1's `rule_for`.
- Produces: the `Access-Control-*` headers on non-preflight responses.

- [x] **Step 1: Failing test**

```rust
#[tokio::test]
async fn get_with_matching_origin_is_decorated() {
    // seed CORS config (rule: GET, https://example.com, expose ETag);
    // GET /b/key with Origin: https://example.com → 200 + access-control-allow-origin
    //   + access-control-allow-methods: GET + access-control-expose-headers: ETag
    //   + allow-credentials: true + vary: Origin, Access-Control-Request-Headers, Access-Control-Request-Method
    // GET with Origin: https://evil.com → no access-control-* headers
    // GET with no Origin → none
    // a bare-"*"-origin rule decorates with ACAO "*" and NO allow-credentials (Q11);
    // 404 responses (missing object) are still decorated with ACAO (op-review/G3-adjacent)
}

#[tokio::test]
async fn get_with_origin_match_but_method_mismatch_is_not_decorated() {
    // B5/S3 pin — first-origin-match applies to decoration too: rule1 allows
    // GET for https://example.com; a PUT to the same origin must NOT be
    // decorated (rule1's origin matches but the method is disallowed, and the
    // decorator never falls through to a later rule). Assert no
    // access-control-* headers on the PUT response.
}
```

- [x] **Step 2: Implement** — in `call_with_peer`, before `req.into_parts()`, capture `let origin = req.headers().get(header::ORIGIN).and_then(|v| v.to_str().ok()).map(str::to_owned);` — and after the inner call returns `Ok(resp)`, before the body wrap:

```rust
if let (Some(cors), Some(origin)) = (&self.cors, origin.as_deref())
    && let Some(bucket) = bucket_from_uri(&uri)
    && let Some(config) = cors.get(bucket).await
    && let Some(rule) = config.rule_for(origin, method_str)   // first origin-matching rule; method validated within it (no fall-through)
{
    let headers = resp.headers_mut();
    // grilling Q11: a rule whose allowed_origins contains bare "*" answers
    // ACAO "*" and OMITS Allow-Credentials; otherwise echo + true.
    let star_rule = rule.allowed_origins.iter().any(|o| o == "*");
    if let Ok(v) = header::HeaderValue::from_str(if star_rule { "*" } else { origin }) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
    }
    let methods = rule.allowed_methods.join(", ");
    if let Ok(v) = header::HeaderValue::from_str(&methods) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, v);
    }
    if let Some(expose) = &rule.expose_headers && !expose.is_empty() {
        if let Ok(v) = header::HeaderValue::from_str(&expose.join(", ")) {
            headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
        }
    }
    if !star_rule {
        headers.insert(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
    }
    // grilling Q4 + op-review G3: append, never replace an existing Vary.
    for v in ["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method"] {
        headers.append(header::VARY, v);
    }
}
```

(`method_str` = `req.method()`'s `as_str()`; `uri` is captured before `into_parts`; the decoration applies on `Ok` responses only — s3s encodes op errors as `Ok(Response)` bodies, so 4xx XML answers are also decorated, which matches AWS.)

- [x] **Step 3: Run** — `cargo test -p tinio-server decorated` / `cargo test -p tinio-server data::` — Expected: PASS.

- [x] **Step 4: Report pending changes.**

---

### Task 11: cucumber, boto3 smoke, docs, full verification

**Files:**
- Create: `crates/tinio-e2e/tests/features/cors.feature`
- Modify: `crates/tinio-server/tests/boto3_journey.py`, `specs/001-s3-local-server/contracts/s3-surface.md`, `specs/001-s3-local-server/contracts/config.md`, `specs/001-s3-local-server/tasks.md`, `docs/superpowers/specs/2026-09-04-s3s-api-coverage-gap-analysis.md`
- Test: the cucumber run, the journey, the full gates

- [x] **Step 1: `cors.feature`** — scenarios (step wrappers mirroring `tagging.feature`):

```gherkin
Feature: Bucket CORS configuration and OPTIONS preflight

  @cors
  Scenario: Bucket CORS round trip, replace-all, and delete
    # put two rules (PUT XML) → get echoes both in order → delete → get answers 404 NoSuchCORSConfiguration

  @cors
  Scenario: PutBucketCors validates the configuration and requires Content-MD5
    # empty/101 rules → 400 InvalidRequest; AllowedMethod PATCH → 400; missing/malformed Content-MD5 → 400 InvalidDigest

  @cors
  Scenario: OPTIONS preflight answers allowed origins and methods
    # OPTIONS with Origin + Access-Control-Request-Method → 200 + the Access-Control-* header set;
    # wildcard origin rule (https://*.example.net) matches

  @cors
  Scenario: OPTIONS preflight denies disallowed and unconfigured buckets
    # disallowed origin / method → 403 AccessDenied; no CORS config → 403 (never reveals existence)

  @cors
  Scenario: Actual GET responses carry Access-Control-Allow-Origin
    # GET with Origin matching a rule → decorated; non-matching origin → no decoration

  @cors-off
  Scenario: Disabled cors answers NotImplemented on the bucket trio and leaves OPTIONS to s3s
    # get/put/delete → 501 "{name} is disabled"; OPTIONS (with Origin+ACRM) → 501 Unknown operation
```

- [x] **Step 2: Run** — `cargo test -p tinio-e2e cors` (the cucumber invocation the crate uses) — Expected: PASS; the legacy feature files untouched and green (Task 7 already added `cors: false` to the minimal-caps spawn helper).

- [x] **Step 3: boto3 journey** — add legs: `put-bucket-cors`/`get-bucket-cors`(+ a no-config `delete-bucket-cors` then 404)/`delete-bucket-cors`, and a raw-`requests` OPTIONS preflight (matching the journey's existing raw-HTTP style): OPTIONS on the bucket with `Origin`/`Access-Control-Request-Method` → 200 + `access-control-allow-origin`. Run the journey per its documented invocation.

- [x] **Step 4: Docs** — `s3-surface.md` CORS contract with FR/SC IDs (follow the numbering the tagging contract used; FR: config round-trip + replace-all + delete, request validation (Content-MD5 + rules + methods), preflight answering (allowed/deny/403), response decoration; SC: the errors — `NoSuchCORSConfiguration` 404, `AccessDenied` 403, `InvalidRequest`/`InvalidDigest` 400, `NotImplemented` 501 when disabled); `config.md` capability table + `cors` line (default true); `tasks.md` entries; the gap-analysis doc's Tier A#2 row gets a status note pointing at this plan.

- [x] **Step 5: Full gates** — `cargo test --workspace` (Windows + WSL2), `cargo clippy --workspace`, cucumber `@fs`/`@mem` as the e2e suite runs them, plus the double-gate compile matrix (the ACL plan's Task 16 pattern): `cargo check -p tinio-server --no-default-features` and `cargo check -p tinio-server --no-default-features --features multipart,copy,list-v1,list-v2` — feature-off builds compile, no CORS overrides, no `cors` module, route/decorator absent (the ungated `S3Backend::new_shared`, with `S3Backend::new` delegating to it, is what keeps this green — final-spec §5).

- [x] **Step 6: Report pending changes** — with the design decisions recorded: decoration uses the first **origin**-matching rule with the method validated within it, no fall-through (AWS's own documented semantics); `Access-Control-Allow-Credentials: true` except on a bare-`*` origin rule (ACAO `*` then, credentials omitted — Q11); the two verbatim AWS 403 messages (Q10); `Vary` trio with append semantics (Q4); bare OPTIONS probes fall through to the s3s 501.

---

## Self-Review

- **Spec coverage:** Tier A#2 of the gap analysis (CORS 3 methods + server-layer preflight) — the ops (Tasks 1–8), preflight (Task 9), actual-response decoration (Task 10, the second half of the "server layer" the analysis flagged), docs/gates (Task 11). The analysis's "s3s does not route OPTIONS" is corrected in the plan header with the 0.15 `S3Route` discovery — preflight rides the sanctioned seam instead of a hand-rolled HTTP-layer intercept; the analysis's core claim (no OPTIONS *operation* in s3s) still holds. Final-spec review corrections are folded in: first-origin-match semantics (first origin-matching rule, method/headers validated within it, no fall-through) — Task 1/9/10; ungated `S3Backend::new_shared` with `S3Backend::new` delegating to it (F7/F8) — Task 8/9; `#[async_trait]` on `CorsLookup` for `Arc<dyn CorsLookup>` erasure (F4/E0038) — Task 9; the two added request validations (≥1 `AllowedMethod`/`AllowedOrigin` per rule; no `,` in any pattern/ID/expose) — Task 8; preflight missing-bucket = same no-config message (oracle closed) — Task 9.
- **Placeholders:** none — every code step carries concrete code or a pinned file/line to mirror; the two state-dependent spots (ACL-merged arity in Task 2, `percent.rs` extraction in Task 1) carry explicit one-line reconcile rules, not TODOs.
- **Type consistency:** `CorsConfig`/`CorsRule`/`PreflightMatch` (Task 1) flow unchanged into Tasks 2–5 (store decode, contract, backends) and 8–10 (ops, route, decoration); `get_bucket_cors/put_bucket_cors/delete_bucket_cors` names consistent across contract (Task 3), backends (4/5), conformance (6), server ops (8); `CorsConfigs`/`CorsLookup`/`CorsPreflightRoute`/`bucket_from_uri` (Task 9) consumed by Task 10; `Capabilities.cors` (Task 7) + `cors` cargo feature (Task 8) consumed by Tasks 8–10 and the e2e helper.
- **Key decisions recorded in-task** (grilling 2026-09-05 + op-review 2026-09-05, all applied — see the design doc's Decisions for the full rulings): `''` wire = no config (404) vs empty rule set (400 on put; storage normalizes anyway, op-review G2) — Tasks 1/3/4/6; Content-MD5 AWS three-state (missing → `InvalidRequest` + verbatim AWS message, malformed → `InvalidDigest`; A7-alignment note) — Task 8; preflight requires both `Origin` AND `Access-Control-Request-Method` (bare OPTIONS keeps the s3s 501) — Task 9; wildcard matching (`*`, single-`*` prefix/suffix, apex excluded) pinned by unit tests — Task 1; first-origin-match selection (first origin-matching rule, method/headers within it, no fall-through) never reorders rules — Task 1/8/9/10 + pinning tests; rule-list `Allow-Methods` (Q9) + verbatim 403 messages + never-reveal 403 scoped to well-formed names (C1) + preflight missing-bucket = the same no-config message (oracle closed) + `*`-rule ACAO/credentials split (Q11) + `Vary` append trio (Q4/G3) + explicit `Content-Length: 0` (G4) + fallible `HeaderValue` construction (S1) — Task 9/10; control-byte / negative-max-age / 64-KB / no-`,`-in-item / ≥1-method-and-origin validation — Task 8; double gate with the two different 501 messages (Q2/C3) — Tasks 8/11; ACL-sequenced 5-tuple (Q5/D1) — Task 2; ungated `S3Backend::new_shared` + `new` delegating (final-spec §5) — Task 8; `#[async_trait]` `CorsLookup` for the `Arc<dyn CorsLookup>` erasure (E0038) — Task 9; bare-OPTIONS probes keep the s3s 501 — Task 9; `/metrics` stays outside the S3 path — noted in `data.rs` docs.
