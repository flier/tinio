# Design: ListBuckets pagination and unified listing page-size policy

**Date**: 2026-08-29
**Status**: reviewed — decisions confirmed by grilling (2026-08-29); review findings of 2026-08-30 incorporated; pending plan
**Scope**: `tinio-core` storage contract (`BucketOps::list_buckets`), both backends (`tinio-mem`, `tinio-fs`), the S3 mapping layer (`tinio-server`: `backend/buckets.rs`, `backend/listing.rs`, `backend/multipart.rs`, `backend/capabilities.rs`), the `[s3]` config schema (`tinio-config`), the server startup wiring (`tinio-server/examples/serve.rs`), the conformance harness (`tinio-util`), the boto3 interop scripts, the `s3-surface.md` ops table, and the `contracts/config.md` `[s3]` section.

## Goal

1. Give the S3 `ListBuckets` operation pagination per the 2025-03 AWS semantics: `continuation-token`, `max-buckets`, and `prefix` query parameters; a `ContinuationToken` response element when more buckets remain.
2. Unify the listing page-size policy across every listing operation: a page size < 1 is rejected with `InvalidArgument` (mapping layer), `max-buckets` outside the AWS-documented 1..=10,000 is rejected (never a silent clamp), and ListBuckets/ListObjects page sizes are clamped to operator-configurable caps (0 = unlimited).

Follow the ListObjects precedent for the pagination shape: S3 listing semantics live in the storage contract (params + listing types), the shared pagination engine does the page slicing, and the mapping layer translates dto → params → dto and enforces wire-level validation.

## Non-goals

- No `IsTruncated` response element: s3s 0.15's `ListBucketsOutput` has no such field, so the wire cannot carry it without forking s3s. Presence of `ContinuationToken` is the truncation signal — which is what AWS documents.
- No `bucket-region` support: single-region server (`GetBucketLocation` answers `us-east-1`); the input is ignored, as today.
- No `Owner` element in the response (unchanged).
- No delimiter/grouping for bucket names (S3 has none for ListBuckets).
- No feature gate: pagination and the page-size policy ship unconditionally. "Additive" applies to ListBuckets only — the `< 1` rejection is a deliberate wire-level behavior change for the pre-existing `max-keys` / `max-parts` / `max-uploads` surfaces (see Call sites / compatibility). An operator escape hatch, `[s3] allow_zero_page_size` (default false), restores the legacy empty-page behavior on those pre-existing surfaces (2026-08-30 review fix #1).
- No cap knobs for multipart listings (`max-parts` / `max-uploads` stay uncapped — AWS documents no cap).
- The storage contract stays permissive: `max_buckets = 0` keeps the engine's empty-page semantics; strictness is a wire-level policy in the mapping layer (same as `ListObjectsParams.max_keys = 0` today).

## Wire surface (s3s 0.15 — already parsed and serialized, zero s3s changes)

- `ListBucketsInput`: `continuation_token: Option<Token>` (String), `max_buckets: Option<MaxBuckets>` (i32), `prefix: Option<Prefix>` (String), plus `bucket_region` (ignored) — query params `continuation-token`, `max-buckets`, `prefix` (`s3s/src/ops/generated.rs`).
- `ListBucketsOutput`: `buckets`, `continuation_token: Option<NextToken>` (String), `owner`, `prefix` — XML elements `Buckets`/`Bucket`, `ContinuationToken`, `Owner`, `Prefix` (`s3s/src/xml/generated.rs`). **No `IsTruncated`.**
- AWS documented constraints (2025-03 API): `max-buckets` valid range **1..=10,000**; default page size **10,000** when `prefix`/`continuation-token`/`bucket-region` are given without `max-buckets`; `continuation-token` min length 0; the response `ContinuationToken` "is obfuscated and is not a real bucket". `max-keys`, `max-parts`, `max-uploads` have **no documented range** in current AWS docs (historical ListObjects behavior allows 0 → empty page).

## Architecture

```
 dto::ListBucketsInput                          dto::ListBucketsOutput
 (continuation_token, max_buckets, prefix)      (buckets, continuation_token, prefix)
        │  mapping (tinio-server):                        ▲
        │    validate (max_buckets ≥ 1)                   │ mapping
        │    decode token (base64 → name)                 │
        │    clamp (cap 0 = unlimited, else min)          │
        ▼                                                │
 ListBucketsParams            list_buckets            BucketsListing
 {prefix, start_after,  ──────────────────►  {buckets, truncated, next_start_after}
  max_buckets}
        ▲  backends (mem/fs): prefix filter
        │  + paginate_ordered (name-sorted stream)
   bucket names in name order (both backends already
   iterate their name-keyed stores sorted)
```

### Contract — `tinio-core/src/storage/bucket.rs`

New types (no `Default` derive — same convention as `ListObjectsParams`):

```rust
pub struct ListBucketsParams {
    /// Only buckets whose name starts with this prefix are returned.
    pub prefix: String,
    /// Resume the listing after this bucket name (exclusive).
    pub start_after: Option<String>,
    /// Maximum number of buckets per page (default 10_000 at the mapping).
    pub max_buckets: usize,
}

pub struct BucketsListing {
    /// Bucket metadata in name order (lexicographic, S3 semantics).
    pub buckets: Vec<Bucket>,
    /// Whether more results exist after this page.
    pub truncated: bool,
    /// Resume marker for the next page (`start_after` of the next call).
    pub next_start_after: Option<String>,
}
```

`BucketOps::list_buckets(&self, params: ListBucketsParams) -> Result<BucketsListing, <Self as Storage>::Error>` replaces the current `() -> Result<Vec<Bucket>, …>`; the method's doc comment ("All buckets, in name order") is rewritten for the paginated contract. Re-export both types from `storage/mod.rs`, and update its `ignore`d doc example (`storage/mod.rs`, the `storage.list_buckets()` snippet) so it does not go stale.

### Backends

Both iterate their name-keyed stores in name order (mem: `BUCKETS` redb table iteration; fs: a prefix-aware root scan, sorted — see below). Page slicing reuses the shared `paginate_ordered` engine (`tinio-core/src/storage/listing.rs`) — pure order pagination over a sorted stream: exclusive-after marker, one probe past the page, resume marker on truncation. Prefix filtering is a plain `starts_with` on the name, applied **before** the engine (names are the sort key; filtering first keeps the engine's "probe = next entry past the page" correct and marker semantics identical to ListObjects).

- **mem** (`tinio-mem/src/bucket.rs`): filter the table iteration by prefix, then pass the filtered iterator straight to `paginate_ordered` — no intermediate full `Vec`.
- **fs** (`tinio-fs/src/backend/buckets.rs`): follow the ListObjects precedent (`FsListing::list` over `walk_files`, `tinio-fs/src/listing.rs`) — **no per-page full scan with per-entry stat**. The root scan becomes prefix-aware (a `bucket_names(prefix)` variant of `backend/mod.rs::bucket_names`; the scanner keeps the `""` form): an entry's name comes from `file_name()` with no I/O, so UTF-8 validity, bucket-name validity (incl. the `.tinio` refusal), and the `starts_with` prefix filter all run **before** any stat — only prefix-matching candidate names pay the `symlink_metadata` (and follow-policy `metadata`) call, the bucket-level analogue of `walk_files` pruning non-matching subtrees by prefix before descending. The dirent sweep itself cannot seek (`read_dir` order is unsorted — the same reason `walk_files` collects its prefix-pruned walk before sorting): the matching names are collected (O(matches) memory), sorted, and handed to `paginate_ordered` for the marker skip, page, and probe. Creation times (and lazy first-sight recording via `get_or_record`) resolve **only for the page's buckets** — the probe's creation time is never needed (only its name); this is the metadata-per-page analogue of P3 ("pagination happens on the walked keys first, so only the emitted page's keys are gated"). First-sight recording becomes page-driven — a bucket not reached by pagination stays unrecorded until listed; still lazy by design, no visible behavior change.

### Mapping — `op_list_buckets` (`tinio-server/src/backend/buckets.rs`)

1. **Validate**: `req.input.max_buckets` outside **1..=10,000** (when present) → `s3_error!(InvalidArgument, "max-buckets must be between 1 and 10000")` — the AWS documented range. Above the ceiling is rejected, never silently clamped (2026-08-30 review fix #2: a clamp would hand a buggy client a `ContinuationToken` it did not ask for).
2. **Decode token**: `continuation_token` is URL-safe base64 (no padding) of the previous page's last bucket name. Decode failure → `s3_error!(InvalidArgument, "invalid continuation token")` — this covers **both** failure modes of attacker-controlled input: bad base64 **and** a base64 payload that is not valid UTF-8 (bucket names are UTF-8 strings; `String::from_utf8` failure is the same `InvalidArgument`). An empty token decodes to the empty string — a marker that skips nothing (all names > "") — a natural no-op (AWS: min length 0). A decoded name that is not a valid bucket name is harmless (the marker is a plain string comparison).
3. **Clamp**: `requested = max_buckets.unwrap_or(10_000)`; `effective = if caps.max_buckets == 0 { requested } else { min(requested, caps.max_buckets as usize) }`. **Cap 0 means "no clamp" and must be special-cased** — a literal `min(requested, 0)` yields 0, which the permissive contract turns into an empty, untruncated page (for ListObjects, whose cap defaults to 0, that would break the default configuration). The clamp applies to the default page size too (a cap of 5 clamps the no-parameter request to 5).
4. **Call** `storage.list_buckets(ListBucketsParams { prefix, start_after: decoded_name, max_buckets: effective })`.
5. **Output**: `buckets` from the listing; `continuation_token = base64(listing.next_start_after)` (only when truncated — `paginate_ordered` returns no marker when the page is complete or exhausted); `prefix` echoed only when the client sent one (AWS: "If `Prefix` was sent with the request, it is included in the response").

### Unified page-size policy — all listing operations

Reject a page size < 1 with `InvalidArgument` at the mapping layer (before any storage call). One rule across the four surfaces:

| Operation | Request param | Current handling | New handling |
|-----------|---------------|------------------|--------------|
| ListBuckets | `max-buckets` | ignored | `< 1` or `> 10,000` → `InvalidArgument`; clamp to `[s3] max_buckets` (default 10,000, 0 = no clamp) |
| ListObjects V1/V2 (`list_page`, `tinio-server/src/backend/listing.rs`) | `max-keys` | `.unwrap_or(1000).max(0)` | `< 1` → `InvalidArgument`; clamp to `[s3] max_keys` (default 0 = no clamp) |
| ListParts (`backend/multipart.rs`) | `max-parts` | `.unwrap_or(1000).max(0)` | `< 1` → `InvalidArgument`; uncapped |
| ListMultipartUploads (`backend/multipart.rs`) | `max-uploads` | `.unwrap_or(1000).max(0)` | `< 1` → `InvalidArgument`; uncapped |

The contract stays permissive: backends keep the engine's `max = 0` empty-page semantics for direct contract calls.

**Escape hatch** (2026-08-30 review fix #1): `[s3] allow_zero_page_size = true` (default false) restores the legacy clamp-to-0 empty page — negatives included, the old `.max(0)` — on the three pre-existing surfaces (`max-keys`, `max-parts`, `max-uploads`). ListBuckets stays strict: its range is AWS-documented and the hatch does not apply to it. One home for the boundary rule: `normalize_page_size(requested, param, allow_zero)` in `backend/mod.rs`, shared by `listing.rs` and `multipart.rs`; `buckets.rs` validates its range inline.

**Echo rule (pinned)**: the V1/V2/ListParts/ListMultipartUploads response `MaxKeys`/`MaxParts`/`MaxUploads` elements echo the **effective** page size — the requested value after clamping, the default when absent (this is today's behavior: `list_page` echoes the effective value). AWS echoes the requested value; we deliberately echo what was actually applied, so a clamped request is visible to the client.

### Config — `[s3]` knobs (`tinio-config/src/schema/s3.rs` + `Capabilities`)

```rust
/// Cap on the ListBuckets page size: larger `max-buckets` requests are
/// clamped to this value. 0 = unlimited (no clamp). Default 10,000 —
/// the AWS documented maximum.
#[serde(default = "max_buckets")]
#[default = 10000]
pub max_buckets: u32,

/// Cap on the ListObjects page size: larger `max-keys` requests are
/// clamped to this value. 0 = unlimited (no clamp). Default 0 —
/// unlimited, preserving current behavior (AWS documents no max-keys cap).
#[serde(default = "max_keys")]
#[default = 0]
pub max_keys: u32,
```

Both are `u32` (no garde constraint — 0 is legal and meaningful). They flow through the capability pipeline: `s3::Config` → `Capabilities::from` (two new `u32` fields on `Capabilities` — its first numeric fields; defaults 10_000 / 0) → `S3Backend::new`. Note: `max_concurrent_uploads` is **not** a precedent for this path — it bypasses `Capabilities` entirely (`serve.rs` applies it straight to the fs backend via `set_max_concurrent_uploads`). `tinio-server` gains `base64 = "0.22"` as a direct dependency (already in the lock at 0.22.1 — no new download; s3s itself uses `base64-simd`, `base64` arrives via other crates).

A third knob rides the same pipeline (2026-08-30 review fix #1):

```rust
/// Escape hatch for the pre-existing listing surfaces: when true,
/// `max-keys` (V1/V2), `max-parts`, and `max-uploads` accept 0 — and
/// clamp negative values to 0 — answering the empty page the
/// pre-2026-08 behavior answered instead of `InvalidArgument`.
/// ListBuckets keeps the AWS-documented 1..=10,000 validation
/// regardless. Default false (strict).
#[serde(default)]
#[default = false]
pub allow_zero_page_size: bool,
```

The `contracts/config.md` `[s3]` sample and its validation-rules list gain the three keys (`s3-surface.md` is covered under Call sites / compatibility).

### Server startup wiring (`tinio-server/examples/serve.rs`) — hard prerequisite

`serve.rs` currently builds the plane with `Capabilities::default()`, so even the existing bool toggles are unwired (`Capabilities::from(config.s3)` is only exercised in its own unit test). This design adds the wiring: when the loaded config has an `[s3]` section, build `Capabilities::from(s3)` — carrying the toggles **and** the two new caps — and pass it to the plane constructor; otherwise keep `Capabilities::default()`.

This is a hard prerequisite for the e2e plan below: `boto3.sh` launches the serve binary via `start_server` (`e2e/interop/lib.sh`), so "page size forced small via the config cap" only works once the binary honors `[s3] max_buckets`. (The Rust port passes `Capabilities` directly through `Server::fs(caps)` and is unaffected either way.)

### Token semantics

A token is URL-safe base64 of an exclusive-after bucket name — opaque to clients ("obfuscated and not a real bucket", AWS wording), no server-side token state. Stale or foreign-but-decodable tokens degrade gracefully: names ≤ decoded marker are skipped, exactly like a ListObjects `start_after`. Changing `prefix` mid-pagination is the client's responsibility; since filtering happens before pagination, a token that does not match the new prefix is simply never reached — no error, an empty page when exhausted. Only undecodable (or non-UTF-8) input is an error (400, step 2 above).

### Performance notes (known bounds)

- **fs**: per page, one unsorted `read_dir` sweep of the storage root (unavoidable for any non-empty page — dirent order is arbitrary, the same reason the ListObjects walk collects its prefix-pruned subtree before sorting; `max_buckets = 0` short-circuits before the sweep, 2026-08-30 review fix #3), but with **zero per-entry syscalls for non-matching names**: the prefix/name filter runs on `file_name()` before any stat, so a `prefix=` listing stats only its candidates (the bucket-level analogue of `walk_files`' subtree pruning). Creation-time resolution is per page. A full paginated sweep without a prefix still costs O(N² / page_size) bare dirent reads — cheap, stat-free for non-buckets, acceptable at bucket counts (the S3 account ceiling is ~1,000 buckets).
- **mem**: each page scans the redb table from the head, skipping names ≤ marker (O(marker position) per page), and `paginate_ordered` allocates one `String` order per scanned entry (the reason `group_and_paginate` keeps a borrowed-key variant). A `BUCKETS.range(start_after..)` seek would avoid both; the engine reuse is kept for semantic parity with ListParts/ListObjects — immaterial at bucket counts.
- **fs first-sight recording**: page-driven `get_or_record` performs the same total writes as today's `load_all` + per-miss record — no regression.

## Data flow

1. s3s parses `continuation-token` / `max-buckets` / `prefix` into `ListBucketsInput`.
2. Mapping validates (`max-buckets ≥ 1`), decodes the token, clamps the page size (cap 0 = no clamp), translates to `ListBucketsParams`.
3. Backend filters names by prefix, pages via `paginate_ordered`, resolves page metadata.
4. Mapping translates `BucketsListing` → `ListBucketsOutput` (`ContinuationToken` present iff truncated, base64-encoded; `Prefix` echoed iff requested).

## Error handling

New error paths are wire-level only, all `InvalidArgument` at the mapping layer (no new storage error variants, `map_backend_error` unchanged): `max-buckets` outside 1..=10,000, `max-keys < 1` (V1/V2), `max-parts < 1`, `max-uploads < 1`, undecodable or non-UTF-8 continuation token. With `[s3] allow_zero_page_size = true` the three pre-existing surfaces' `< 1` rejection is replaced by the legacy clamp-to-0 empty page. `prefix` matching nothing → empty, untruncated page (no token) — same shape as ListObjects with an unmatched prefix.

## Testing

- **tinio-core**: types construct (mirror `listing_types_construct`).
- **tinio-mem / tinio-fs** (per backend): pagination test — prefix filtering, marker resume across pages, truncation probe, exact fill is not truncated, contract-level `max_buckets = 0` yields an empty page without a resume marker.
- **conformance harness** (`tinio-util/src/testing.rs`, `conformance_buckets`): the two `list_buckets()` call sites move to the new signature and assert on `BucketsListing.buckets`; add a paginated-listing parity check (multi-page resume + prefix filter) so the harness pins **both** backends to identical page/marker semantics, not just the per-backend unit tests.
- **tinio-server** (`backend/buckets.rs`, `listing.rs`, `multipart.rs` tests):
  - dto-level round trip — `max-buckets` page splits with `continuation-token` resume; `prefix` filter + echo; default page size (10,000, no `max-buckets` sent); token exhaustion yields an empty page.
  - rejection matrix — `max-buckets = 0` / negative, `max-keys = 0` (V1 and V2), `max-parts = 0`, `max-uploads = 0` → `InvalidArgument`.
  - clamp — a small configured cap (e.g. 3) clamps a `max-buckets = 10` request and the no-parameter default; cap 0 (default for keys) clamps nothing; a `max-buckets` request above 10,000 (10,001, 50,000) is rejected — `InvalidArgument`, never a silent clamp — and 10,000 itself is legal.
  - escape hatch — with `allow_zero_page_size = true`, `max-keys = 0` (V1 and V2), `max-parts = 0`, and `max-uploads = 0` answer the empty page (negatives clamped to 0); ListBuckets stays strict under the hatch.
  - echo — after a clamp, the V1/V2 `MaxKeys` response element carries the effective (clamped) value.
  - token — undecodable token → `InvalidArgument`; base64 of non-UTF-8 bytes → `InvalidArgument`; empty token → full listing; stale-but-decodable token → resumed listing.
- **tinio-config** (`schema/s3.rs` tests): `max_buckets` / `max_keys` defaults, parse, 0 accepted; `allow_zero_page_size` defaults false, parses true.
- **tinio-server** `Capabilities` tests: `from` maps both knobs.
- **serve wiring**: a test or assertion path proving a configured `[s3] max_buckets` reaches the plane's `Capabilities` (the e2e below depends on it).
- **e2e interop** (`e2e/interop/boto3.sh` + the Rust port `crates/tinio-server/tests/boto3.rs`): create more buckets than one page (page size forced small via the config cap — requires the serve wiring above), paginate with the boto3 `list_buckets` paginator, assert every bucket is seen exactly once.

## Call sites / compatibility

All `list_buckets()` callers are in-workspace: mem/fs internal tests, the conformance harness, the server mapping, and the `tinio-core/src/storage/mod.rs` doc example (an `ignore`d snippet — updated for accuracy). No external consumers. Behavior changes for existing clients: none for ListBuckets under the default caps (max-buckets was ignored before — now validated against 1..=10,000 and clamped, which is the AWS-documented ceiling); requests with `max-keys = 0` (or parts/uploads, or a negative value previously clamped to an empty page) now fail with `InvalidArgument` instead of returning an empty page — a deliberate wire-level behavior change on the pre-existing listing surfaces, restorable via `[s3] allow_zero_page_size = true`. `s3-surface.md` ops table gets the `ListBuckets` entry extended with "pagination (2025-03 semantics: continuation-token / max-buckets / prefix)" and a behavior note for the page-size policy and the `[s3] max_buckets` / `max_keys` / `allow_zero_page_size` knobs; `contracts/config.md` documents the three keys.

## Decisions (locked by grilling 2026-08-29)

- **Contract-level pagination**, not mapping-layer: ListObjects precedent (params + listing in the contract, engine does the slicing); the fs backend gains per-page creation-time loading as a side benefit.
- **Token = URL-safe base64 (no padding) of the last bucket name**: stateless, opaque to clients (AWS: "obfuscated"), no token namespace; matches the `start_after` precedent underneath. Bad base64 and non-UTF-8 payloads are both `InvalidArgument`.
- **No `IsTruncated`**: s3s 0.15 cannot carry it; `ContinuationToken` presence is the documented signal.
- **Unified page-size policy**: every listing page size < 1 → `InvalidArgument`, and `max-buckets` above 10,000 → `InvalidArgument` (AWS documents 1..=10,000 for `max-buckets`; no documented ranges elsewhere — the strictness is a deliberate tinio-wide policy; the ceiling rejection is 2026-08-30 review fix #2, never a silent clamp). Strictness lives in the mapping layer; the contract keeps the engine's `max = 0` semantics.
- **Configurable caps**: `[s3] max_buckets` (default 10,000, 0 = unlimited) clamps ListBuckets; `[s3] max_keys` (default 0 = unlimited, preserving current behavior) clamps ListObjects. Multipart listings stay uncapped. **Cap 0 is special-cased as "no clamp" — never a literal `min(requested, 0)`.**
- **Echo the effective page size** in V1/V2/ListParts/ListMultipartUploads responses (clamped value visible to the client; today's behavior), not the requested value.
- **Unconditional release**: no feature gate — additive for ListBuckets; the `< 1` rejection on the other listing surfaces is an acknowledged wire-level change, escape-hatched by `[s3] allow_zero_page_size` (default false, strict) for operators with legacy clients (2026-08-30 review fix #1).
- **e2e coverage**: boto3 paginator check in the interop scripts and their Rust port; gated on the serve-binary `Capabilities` wiring (the config cap must reach the running server).
