# Code Review — ListBuckets pagination diff vs. the 2026-08-29 design spec

**Date**: 2026-08-30
**Scope**: `git diff HEAD` (uncommitted, staged) — 29 files, +3,337/−141. Implements `docs/superpowers/specs/2026-08-29-list-buckets-pagination-design.md` per `docs/superpowers/plans/2026-08-30-list-buckets-pagination.md`.
**Method**: two-axis review (Standards / Spec) run as parallel sub-agents, per the `code-review` skill. Standards sources: `CLAUDE.md`, `docs/style.md`, `docs/cargo.md`, plus the Fowler smell baseline. Spec axis quotes the design doc for every finding. Line numbers refer to the current working tree.

## Verdict

The implementation is faithful to the spec: **no wrong-logic findings** on the Spec axis. All locked decisions verified in code — cap-0 special-cased as "no clamp", clamp applied to the 10,000 default page size, `URL_SAFE_NO_PAD` tokens with bad-base64 and non-UTF-8 both → `InvalidArgument`, empty token as no-op, `Prefix` echoed only when sent, `ContinuationToken` only when truncated, fs prefix/UTF-8/`.tinio` filtering on `file_name()` before any stat, creation times resolved per page, effective-value echo. Plan/spec conflicts: none.

Findings below: 3 hard standards violations, 4 baseline smells, 2 missing/partial spec items, 2 scope-creep items.

**Status (updated 2026-08-30, second pass):** all findings closed — R01, R02, R05 fixed in the working tree; R03, R04, R06, R08, R09 fixed (2026-08-30); R07, R10, R11 keep their accepted/kept dispositions. A second review pass added S01–S04 — all fixed, TDD-verified (see "Follow-up pass" below).

**Status (updated 2026-08-31, fourth pass):** F01–F10 added from a `/code-review max` run over the grown diff (58 files, +6,388/−474) — all **OPEN** (see "Fourth pass" below). F01 (Unix build break) and F02 (lockmap lock split) re-verified line-by-line by the parent session; the remaining eight verified by the review agent against the current working tree. One claimed finding was dropped during re-verification (cleanup.rs false-success report — already fixed in the current tree).

**Status (updated 2026-08-31, fifth pass):** all Third-pass (T01–T09) and Fourth-pass (F01–F10) findings **FIXED** in the working tree (see "Fifth pass" below). Dispositions per the author's decisions: F06 documented only (no default flip — no release history exists to migrate; `config.md` carries the breaking-change note + knob); F04 rejected at config parse (garde `max = MAX_BUCKETS`, consistent with the existing `max_concurrent_uploads` validation); F03+F07 full fix (scanner awaits leftover removals and counts actual outcomes; `RemoveTask` propagates failures; fire-and-forget paths log the error from a detached awaiter; `Pipelines::drain` awaits the removal lane with a bounded 10 s timeout overlapping the IO drain; s3-surface.md documents the 204-before-gone contract); T07 documented as a platform assumption (tokio exposes no raw d_type, so the blocking-pool fallback is unobservable and would cost a stat on every filesystem). Verification: `cargo test --workspace` green, `cargo fmt --check` clean, `cargo check --target x86_64-unknown-linux-gnu` on tinio-fs green (F01's Unix branch compiles), `bash -n` on the e2e scripts clean.

## Standards — hard violations

### R01 — Root `Cargo.toml` dev-dependency out of alpha order — **[FIXED — verified in tree 2026-08-30]**
- **File**: `Cargo.toml:11` · **Standard**: `docs/cargo.md` → Groups ("Alpha within each group")
- **Finding**: `base64 = "0.22"` is inserted before `assert_cmd`; alphabetically it belongs after it.
- **Fix**: Move the `base64 = "0.22"` line below `assert_cmd` in the workspace dev-dependencies group. (`crates/tinio-server/Cargo.toml` places it correctly.)

### R02 — Inline 3+-segment paths in `op_list_buckets` — **[FIXED]** (same finding as S04)
- **File**: `crates/tinio-server/src/backend/buckets.rs:94, 132, 185, 556` · **Standard**: `docs/style.md` → Imports ("3+ (`a::b::c`): `use` then short form")
- **Finding**: `base64::engine::general_purpose::URL_SAFE_NO_PAD` is spelled out inline in the decode path (line 94), the encode path (line 132), and twice in tests (lines 185, 556), despite `use base64::Engine` already present.
- **Fix**: Add `use base64::engine::general_purpose::URL_SAFE_NO_PAD;` (module) and `use base64::engine::general_purpose::URL_SAFE_NO_PAD;` in the test module, then call `URL_SAFE_NO_PAD.decode(...)` / `URL_SAFE_NO_PAD.encode(...)` at all four sites.

### R03 — Inline `crate::_core::BucketsListing` in test fn-pointer types — **[FIXED]**
- **Files**: `crates/tinio-fs/src/backend/buckets.rs`, `crates/tinio-mem/src/bucket.rs` · **Standard**: `docs/style.md` → Imports (same rule as R02)
- **Finding**: `let names: for<'a> fn(&'a crate::_core::BucketsListing) -> Vec<&'a String> = ...` spells the type inline in both backends' pagination tests.
- **Fix**: Import `BucketsListing` with the existing `crate::_core::{...}` block in each test module and use the short form.
- **Done**: `BucketsListing` added to each test module's `_core::{...}` block; the inline spelling is gone. Fixed together with R06.

## Standards — baseline smells (judgement calls)

### R04 — Duplicated Code: boto3 pagination script exists twice — **[FIXED]**
- **Files**: `crates/tinio-server/tests/boto3_buckets_pagination.py` and the heredoc in `e2e/interop/boto3.sh`
- **Finding**: Same client setup, six-bucket fixture, paginator assertions, and marker string in two near-verbatim copies — two homes to keep in sync.
- **Fix**: Have `boto3.sh` invoke the checked-in `.py` file (pass server address/bucket count as argv or env) instead of embedding the heredoc.
- **Done**: `boto3.sh` invokes `"$REPO/crates/tinio-server/tests/boto3_buckets_pagination.py" "$PAGINATE_ENDPOINT"`; the pagination heredoc is removed (the basic-journey heredoc is a separate scenario, not a duplicate).

### R05 — Duplicated Code (mild): `< 1 → InvalidArgument` re-implemented at four sites — **[RESOLVED via `normalize_page_size`]** (see S01)
- **Files**: `crates/tinio-server/src/backend/buckets.rs`, `backend/listing.rs`, `backend/multipart.rs` (×2)
- **Finding**: The same reject-below-one shape recurs with only the parameter name differing. The change already extracts `clamp_page_size` for the clamp half, setting the precedent.
- **Fix**: Extract a shared helper (e.g. `reject_page_size_below_one(param: &str, value: i64) -> Result<usize, S3Error>`) next to `clamp_page_size` in `backend/mod.rs`. Defensible as-is given the per-parameter messages; fix only if the shape grows a fifth site.

### R06 — Roundabout fn-pointer coercion in backend tests — **[FIXED]**
- **Files**: `crates/tinio-fs/src/backend/buckets.rs`, `crates/tinio-mem/src/bucket.rs`
- **Finding**: The `for<'a> fn(...) -> ...` coercion forces a plain closure into a function-pointer type for no reuse gain.
- **Fix**: Write a plain closure or map at each call site; together with R03 the helper collapses to one line.
- **Done**: the helper became a nested `fn names(p: &BucketsListing) -> Vec<&String>`. Correction to the suggested fix: a `let`-bound plain closure does not compile here — closures cannot be higher-ranked over the input lifetime, which is exactly what the coercion provided; a fn item instantiates lifetimes per call and keeps the same call shape.

### R07 — Test-fixture duplication across fs / mem / server tests (accepted) — **[ACCEPTED, unchanged]**
- **Files**: `crates/tinio-fs/src/backend/tests.rs`, `crates/tinio-mem/src/bucket.rs`, `crates/tinio-server/src/backend/buckets.rs`
- **Finding**: The six-bucket fixture + page-walk loop repeats in three crates.
- **Disposition**: Accepted — per-backend pinning is intentional, and the conformance harness (`tinio-util/src/testing.rs`) already covers cross-backend parity.

## Spec — missing / partial

### R08 — Serve-wiring proof is not CI-gated — **[FIXED]**
- **File**: `crates/tinio-server/tests/boto3.rs` (`list_buckets_pagination`) · **Spec**: "serve wiring: a test or assertion path proving a configured `[s3] max_buckets` reaches the plane's `Capabilities`"
- **Finding**: The only proof is an `#[ignore]`d, boto3-venv-gated e2e test; nothing CI-gated exercises `Capabilities::from(s3)` in `examples/serve.rs`. A regression that drops the cap wiring (e.g. back to `Capabilities::default()`) would pass CI silently.
- **Fix**: Add a cheap always-on assertion path — e.g. factor the capabilities construction in `serve.rs` into a testable `fn capabilities_for(config: &Config) -> Capabilities` and unit-test that an `[s3]` section with `max_buckets = 3` yields `caps.max_buckets == 3`.
- **Done**: `fn capabilities_for(config: &Option<Config>) -> Capabilities` extracted in `examples/serve.rs` and called from `main`; two CI-gated unit tests in the example (`capabilities_for_maps_s3_max_buckets`, `capabilities_for_defaults_without_s3_section`) — `cargo test --workspace` builds and runs example test targets. The seam takes `&Option<Config>` rather than `&Config` so the None→default branch is pinned too, not just the `[s3]` branch.

### R09 — fs backend test misses the exact-fill exhaustion assertion — **[FIXED]**
- **File**: `crates/tinio-fs/src/backend/buckets.rs` tests · **Spec**: testing section — "exact fill is not truncated"
- **Finding**: `list_buckets_paginates_and_filters_prefix` omits `assert_eq!(exact.next_start_after, None)` on the exact-fill case; the mem backend's twin test has it.
- **Fix**: Add the assertion to the fs test for parity with mem.
- **Done**: `assert_eq!(exact.next_start_after, None)` added after the exact-fill block.

## Spec — scope creep (out of spec/plan)

### R10 — Cosmetic `dto::X` → flattened-import rewrite of untouched handlers — **[KEPT]**
- **File**: `crates/tinio-server/src/backend/buckets.rs` (create/delete/head/location handlers)
- **Finding**: Wholesale import-style churn on handlers the spec does not touch, inflating the diff.
- **Fix**: None required — harmless, but flag for the author: keep or revert at their discretion; a review-focused diff would leave these handlers alone.

### R11 — `stop_server` fix and `pages >= 2` assertion beyond the plan — **[KEPT, unchanged]**
- **Files**: `e2e/interop/boto3.sh:92-95`, `crates/tinio-server/tests/boto3_buckets_pagination.py`
- **Finding**: The added `stop_server` fixes a pre-existing PID-overwrite leak (justified by the new second-server scenario but not in spec or plan); the `pages >= 2` assertion strengthens the e2e beyond the plan — it pins the config cap actually reaching the server.
- **Disposition**: Keep both — the leak fix is load-bearing for the new scenario and the assertion is a genuine improvement (partially mitigates R08). Noted here for traceability, not for rework.

## Follow-up pass (2026-08-30, post-review)

A second review pass (background `/code-review` fork) confirmed the implementation's correctness — no crash-level or data-corruption bugs in the pagination/clamping logic, workspace compiles with all features — and raised four findings. All four were fixed, TDD-style, in the working tree (uncommitted; the user decides when to commit):

### S01 — The `< 1` rejection ships ungated on the pre-existing surfaces — **FIXED**
- **Files**: `crates/tinio-server/src/backend/listing.rs` (+ `backend/multipart.rs`), `crates/tinio-config/src/schema/s3.rs`
- **Finding**: `max-keys = 0` (V1/V2), `max-parts = 0`, `max-uploads = 0` previously returned a 200 empty page (`unwrap_or(1000).max(0)`); the unified policy turned them into 400 `InvalidArgument` with no escape hatch for operators with legacy clients.
- **Fix**: new `[s3] allow_zero_page_size` knob (bool, default `false` = strict) on `Capabilities`; one boundary-rule home, `normalize_page_size(requested, param, allow_zero)` in `backend/mod.rs`, shared by `listing.rs` and both multipart listings. With the hatch on, 0 — and negatives, the old `.max(0)` — answer the legacy empty page. ListBuckets stays strict (AWS-documented range), pinned by `list_buckets_stays_strict_under_the_zero_page_escape_hatch`. New tests: config parse, V1/V2 gate, parts/uploads gate, helper unit test — each watched failing before the implementation.
- Also resolves **R05**: the three pre-existing surfaces now share `normalize_page_size` instead of re-implementing the rejection (ListBuckets validates its AWS range inline — deliberately a different shape, not a duplicate).

### S02 — `max-buckets` above 10,000 is silently clamped — **FIXED**
- **File**: `crates/tinio-server/src/backend/buckets.rs`
- **Finding**: a `max-buckets = 50000` request returned 200 with a truncated page plus a `ContinuationToken` the client did not ask for; AWS answers `InvalidArgument` for out-of-range values.
- **Fix**: `max-buckets` outside 1..=10,000 → `InvalidArgument` ("max-buckets must be between 1 and 10000"), never a silent clamp. The pinned clamp test became `list_buckets_rejects_page_size_above_the_aws_ceiling` (10,001 / 50,000 rejected; 10,000 itself legal).

### S03 — fs backend sweeps the root for the contract-level `max_buckets = 0` — **FIXED**
- **File**: `crates/tinio-fs/src/backend/buckets.rs`
- **Finding**: `list_buckets(max_buckets = 0)` ran the full root dirent sweep plus a stat per prefix match before `paginate_ordered` returned the empty page, while the mem backend never drains the table — inconsistent cost profiles for the documented contract-level empty-page request.
- **Fix**: the empty, untruncated page is returned before `bucket_names`. Pinned by `list_buckets_max_zero_short_circuits_the_root_sweep` — the test relocates the state dir, renames the root aside, and asserts the empty-page call never touches the root (watched failing with `Io NotFound` from the sweep first).

### S04 — 4-segment `base64::engine::general_purpose::URL_SAFE_NO_PAD` path used inline — **FIXED** (= R02)
- **File**: `crates/tinio-server/src/backend/buckets.rs`
- Same finding as R02, from the second pass: `use base64::engine::general_purpose::URL_SAFE_NO_PAD;` at the top (and in the test module), short form at all four sites (decode, encode, `token()`, bad-token test).

**Docs updated to match**: `docs/superpowers/specs/2026-08-29-list-buckets-pagination-design.md` (validation step, call-sites table, escape-hatch config block, error handling, test plan, decisions, fs performance note), `specs/001-s3-local-server/contracts/config.md` (`[s3]` sample + validation rules), `specs/001-s3-local-server/contracts/s3-surface.md` (page-size behavior note).

**Verification**: `cargo test --workspace` green (server lib 107, fs lib 243, config 23 + doctests); `cargo build --workspace --all-features` clean (one now-unused `s3_error` import in `listing.rs` removed).

## Summary

| Axis | Findings | Status (2026-08-30) |
|------|----------|-------------|
| Standards | 3 hard violations + 4 smells | R01, R02 fixed; R03, R04, R06 fixed; R05 resolved via S01; R07 accepted |
| Spec | 0 wrong-logic, 2 missing/partial, 2 scope creep | R08, R09 fixed; R10 kept; R11 kept |
| Follow-up pass | S01–S04 | all fixed, TDD-verified |
| Fourth pass (2026-08-31) | F01–F10 — 1 critical, 1 high, 2 medium, 6 low | all OPEN; F06/F07 deliberate-but-unannounced (cross-ref S01 / T03) |

All findings closed: R03 + R06 (import `BucketsListing`; the fn-pointer coercion became a nested `fn names` — a plain closure cannot be higher-ranked over the input lifetime), R09 (one-line test parity), R04 (`boto3.sh` points at the checked-in `.py`), R08 (testable wiring seam with two CI-gated unit tests). Verification: `cargo fmt --check` clean; `cargo test --workspace` 683 passed / 0 failed; `cargo build --workspace --all-features` clean; `bash -n` + `py_compile` on the e2e scripts OK.

## Performance review — list bucket/object data paths (2026-08-30)

A dedicated pass over the listing data paths (fs walk/scanner, core pagination engines, mem redb scans, server mapping). Bounds the spec already documents are excluded (fs per-page root sweep, mem head-rescan with one `String` per scanned entry, probe-past-page). All findings verified against source. **Status update (2026-08-30, later the same day): all eight bottlenecks FIXED — per-finding evidence in "Performance fixes verification" below.**

### Real bottlenecks

#### P01 — ListObjects collects + sorts the whole prefix range before paginating — **OPEN** (highest impact)
- **File**: `crates/tinio-fs/src/listing.rs:526-538` (`walk_files`) + `:342` (`group_and_paginate`)
- **Finding**: `walk_files` collects every prefix-matching file (one stat each) and sorts it; only then does the engine apply `start_after`/`max_keys`. The subtree pruning (`:710-726`) skips directories, never keys below the marker.
- **Cost**: O(M) stats + O(M log M) sort per page → O(M²/max_keys) syscalls for a full client sweep; `max_keys=1` still pays M stats.
- **Fix direction**: feed the existing `walk_files_streaming` (no collection, no sort) into `group_and_paginate_unordered` (`tinio-core/src/storage/listing.rs:262` — the bounded-heap variant that already exists): memory and sort cost drop to O(max_keys). Syscalls stay O(M)/page — `read_dir` cannot seek, a hard constraint. Design caveat: page order comes from the heap, not the walk; confirm the unordered engine's marker/delimiter semantics match `group_and_paginate` exactly before switching.

#### P02 — Double stat per walked file on Unix — **OPEN**
- **File**: `crates/tinio-fs/src/listing.rs:668` (`symlink_metadata`) + `:747` (`entry.metadata()`)
- **Finding**: every dirent pays `symlink_metadata` for the symlink/dir check, then non-symlink files pay `entry.metadata()` again. tokio's `DirEntry::metadata` is free on Windows (FindNextFile data) but a second lstat on Unix. The scanner, every ListObjects page, and `walk_files` all pay it.
- **Cost**: 2 syscalls per file per walk.
- **Fix direction**: classify with `entry.file_type()` (d_type — free on both platforms), fall back to `symlink_metadata` only for DT_UNKNOWN filesystems, and reuse that one metadata for size/mtime. Halves the walk's syscall count; the scanner benefits too.

#### P03 — fs `list_buckets` re-stats + re-sorts all candidates per page — **OPEN**
- **File**: `crates/tinio-fs/src/backend/mod.rs:524-558` (`bucket_names`)
- **Finding**: beyond the spec's accepted "bare dirent reads", every page also pays one `symlink_metadata` per prefix-matching name (`:541`, plus a follow-up `metadata` for links) and an O(N log N) sort (`:557`) — all re-done per page, no caching.
- **Cost**: O(N) lstats + O(N log N) per page → O(N²/page_size) stats for a full sweep.
- **Fix direction**: same levers as P01/P02 — `entry.file_type()` gates the stat to links/DT_UNKNOWN only (plain dirs need zero stats); the bounded-heap engine caps collect/sort at page size.

#### P04 — Orphan reclamation: one redb write txn per orphan, under the bucket mutation lock — **OPEN**
- **File**: `crates/tinio-fs/src/scanner.rs:471-508`
- **Finding**: each orphan candidate costs an `is_absent` probe plus a `meta_store().remove` in its own write transaction. redb's write lock is global, so M orphans = M global-write-lock acquisitions serialized against live PUTs.
- **Cost**: zero in steady state; O(orphans) write txns after a mass out-of-band `rm`.
- **Fix direction**: probe under the lock, batch the removes into one write txn (the `MetaWriteBatchTask` pattern already exists).

#### P05 — mem `list_buckets` prefix filter never stops past the prefix band — **OPEN**
- **File**: `crates/tinio-mem/src/bucket.rs:99-123`
- **Finding**: the `filter_map` prefix check never terminates iteration — once the scan passes the prefix band, every remaining row is still visited until the table ends (unless the engine filled the page). The object listing stops correctly (`object.rs:255-257` ends `from_fn`); `list_buckets` alone lacks the early exit.
- **Cost**: O(rows after the prefix band) on every untruncated prefix-filtered page.
- **Fix direction**: in sorted order prefix matches are a contiguous band — stop when `name > prefix && !name.starts_with(prefix)` (a `take_while` before the `filter_map`).

#### P06 — mem `list_multipart_uploads` materializes the whole bucket range — **OPEN**
- **File**: `crates/tinio-mem/src/multipart.rs:347-371`
- **Finding**: the full `UPLOADS` range for the bucket is collected into a `Vec` before the engine runs — per row: `upload_id.to_string()`, full `object::key()` validation, and `params.bucket.clone()` (an owned `Name` cloned per row though identical). The streaming engine never short-circuits.
- **Cost**: O(total uploads in bucket) per page, independent of `max_uploads`; uploads per bucket are uncapped.
- **Fix direction**: feed the lazily filtered range iterator straight into `group_and_paginate_ordered` and attach `params.bucket` when building the page (contrast `list_objects`, which streams).

#### P07 — mem `list_objects` re-validates every scanned row, incl. rollup-collapsed ones — **OPEN**
- **File**: `crates/tinio-mem/src/object.rs:258-271`
- **Finding**: every row the engine touches gets a `String` alloc, `validate_object_key` (~4 passes over the key — `object.rs:138-173`), and an `etag.parse()` alloc — including rows inside a delimiter rollup. Rows were already validated at insert; the read-side re-validation is defense-in-depth charged per scan.
- **Cost**: O(scanned rows × key length); with `delimiter=/`, a one-prefix page scans and fully validates all N keys under it (no redb seek past the group).
- **Fix direction**: either defer validation/ETag parse until the row survives grouping (needs a lighter engine item type), or re-seek the range past a collapsed rollup (`Excluded(bucket\0<cp>\u{10FFFF})`) instead of consuming each row.

#### P08 — Sync redb scans run inline on the async executor — **OPEN** (mem is dev-grade; document or fix)
- **Files**: `crates/tinio-mem/src/bucket.rs:86`, `object.rs:217`, `multipart.rs:130,334`
- **Finding**: all listing scans run inline in `async fn` with no `spawn_blocking`; a scan over a large prefix blocks a runtime worker for its full duration. redb read txns are MVCC (no lock guard held), so it is purely executor starvation — amplified by P05–P07.
- **Fix direction**: document, or wrap scans in `tokio::task::spawn_blocking` (items are already owned).

### Micro-optimizations (not worth dedicated work; do when touching the code)

- `crates/tinio-fs/src/backend/mod.rs:528` — `file_name().to_str().map(str::to_string)` allocates a `String` for every root dirent per page, including prefix-rejected ones: check `starts_with` on the borrowed `&str`, allocate on match.
- `crates/tinio-fs/src/listing.rs` — ~4 extra heap allocs per dirent: `entry.path()` called 2–3× (`:668, :688/:697, :747, :757`, each a fresh PathBuf join) plus `prefix.join(name)` + `to_string_lossy().into_owned()` (`:678-679`). Cache `entry.path()` in a local.
- `crates/tinio-core/src/storage/listing.rs:85` — `last_emitted = Some(key.to_string())` allocates one `String` per *emitted* object; only the last is ever used. Recompute from `page.last()` via `key_of` at the truncation return. Same for the double `cp.to_string()` per new prefix (`:64-65`) and the `last_emitted` clone (`:82`).
- Page Vecs start at `Vec::new()` — `with_capacity(min(max, 1024))` avoids log₂(max) regrows per request.
- Server response construction re-clones every key: `crates/tinio-server/src/backend/listing.rs:85` (`o.key.to_string()` on a consumed value), `buckets.rs:125`, `multipart.rs:370`. One `String` per row per layer.
- `crates/tinio-server/src/backend/listing.rs:136-143` (V2) — `continuation_token.clone().or(start_after.clone())` clones both tokens before choosing; `list_page` re-clones `prefix`/`delimiter`/`start_after` (`:67-70`). ~4 avoidable allocs per request.
- `crates/tinio-fs/src/backend/buckets.rs:67-80` — `get_or_record` on a miss does a redundant read txn before the write; `list_buckets` already knows the row is missing from `load_many` — a record-only path saves one txn per first-sight (2k+1 → k+1 txns for k new buckets in a page). Steady state is the single `load_many` txn — fine.
- `crates/tinio-fs/src/listing.rs:451-455` — final assembly clones each emitted key/ETag out of `page` instead of consuming it (`page.into_iter()`).
- `crates/tinio-server/src/backend/buckets.rs:99-107` — base64 decode allocates `Vec<u8>` then `String`; `from_utf8` could reuse the vec. Negligible at bucket counts.

### Checked, no issue

- No N+1 on the meta side: `load_entries` is one read txn, index-aligned with the page (`crates/tinio-fs/src/listing.rs:357`); per-key work only for gate misses (documented cold-list cost). P3 holds — ETags are computed for the emitted page only.
- No double `ensure_bucket`: `list_objects` (`objects.rs:618`) goes straight to `listing.list`; the bucket-existence stat is the walk's own `symlink_metadata` (`listing.rs:563`).
- List paths take no mutation locks; the scanner's gating snapshot is materialized and released before the walk (no pinned read txn). No lock guard is held across iteration anywhere on these paths.
- Windows per-page identity opens (`listing.rs:397`) are in-code documented and bounded by `max_keys`.
- The core engines themselves do no full-`Vec` materialization; `group_and_paginate` rollup dedup is O(1) against `last_prefix` — no quadratic behavior.
- mem `list_buckets` decodes no values it does not need (the BUCKETS value is a `u64`).

### Priority

**P01** (large-bucket small-page sweeps go O(M²/max_keys)) > **P02** (halves walk syscalls; scanner benefits too) > **P05/P06** (small, semantics-preserving mem fixes). P03 rides the same levers as P01/P02; P04 matters only after mass out-of-band deletes; P07/P08 are mem-backend-grade concerns.

### Performance fixes verification (2026-08-30, same-day follow-up)

Re-verified against the current working tree (58 files, +6,388/−474). All eight bottlenecks are **FIXED**; mechanisms confirmed in code:

- **P01 — FIXED.** The collecting `walk_files` is gone from the list path; `FsListing::list` (`crates/tinio-fs/src/listing.rs:351-368`) drives `walk_files_streaming` into `UnorderedPager` (`tinio-core/src/storage/listing.rs:288`, bounded `BinaryHeap::with_capacity(max+1)`). Inline comment cites P01. The flagged semantics shift (page order from the heap, not the walk) was accepted per the design caveat; a new F15 guard (`listing.rs:484-488`) suppresses the resume marker when an entire truncated page vanishes mid-listing.
- **P02 — FIXED.** `WalkState::next_file` (`listing.rs:668-798`): Unix classifies via `entry.file_type()` (d_type, std falls back on DT_UNKNOWN), Windows reuses the free find-data `entry.metadata()` as the object stat. Regular files pay exactly one stat on Unix, zero on Windows; only followed symlinks pay the extra target stat.
- **P03 — FIXED.** `list_buckets` (`crates/tinio-fs/src/backend/buckets.rs:141-166`) streams `for_each_bucket_name` into `UnorderedPager` — no per-page collection/sort (the sorting `bucket_names` remains only for scanner/cleanup). Stats are gated by `entry_is_bucket_dir` (`backend/mod.rs:576-639`): plain dirs/files cost zero stats. The `max_buckets=0` short-circuit (S03) precedes any root sweep.
- **P04 — FIXED.** `scanner.rs:476-511`: probes collect confirmed orphans, then a single `meta.remove_many(name, orphans)` write txn under the bucket lock (new `remove_many` at `crates/tinio-fs/src/meta.rs:511`).
- **P05 — FIXED.** `crates/tinio-mem/src/bucket.rs:104-111`: the range starts at `params.prefix` and `take_while` exits at the first non-matching name. (Residual: the `filter_map` re-checks `starts_with` at :115 — redundant, harmless.)
- **P06 — FIXED.** `crates/tinio-mem/src/multipart.rs:369-434`: a lazy `range/take_while/filter_map` feeds `group_and_paginate_ordered` directly — no `Vec` materialization. `params.bucket.clone()` happens once per *emitted* row at page build (:430).
- **P07 — FIXED (rollup half).** `crates/tinio-mem/src/object.rs:256-327`: a `last_cp` mirror drops delimiter-rollup-collapsed rows before the key copy, validation, and `etag.parse()`. No redb re-seek, same cost cut. Per-surviving-row validation stays as deliberate defense-in-depth (documented at :280-283).
- **P08 — FIXED (documented).** No `spawn_blocking`; each scan carries an explicit inline-by-design comment (`bucket.rs:98-101`, `object.rs:246-248`, `multipart.rs:164-167, 363-367`) — matches the review's "document or fix".

Micro-optimizations: all fs items fixed (alloc-after-prefix-match `backend/mod.rs:550-558`; cached `entry.path()` `listing.rs:679`; record-only `get_or_insert` on miss `bucket.rs:90`; assembly consumes `page` `listing.rs:469` — `etag.clone()` per entry remains, trivial). Core/server items fixed (`last_emitted` recomputed from the page `listing.rs:84-88`; `with_capacity(min(max,1024))` on the grouped engines and `UnorderedPager`; `String::from(o.key)` moves; V2 token clones only the winner `listing.rs:146`; base64 decode reuses the `Vec<u8>`). Two negligible leftovers: the double `cp.to_string()` per new prefix (`core/listing.rs:66-68, 222-224`) and `paginate_ordered`'s uncapacitied page Vec (`:152`). `list_page`'s param clones are inherent (values echoed in the response; borrowed-params contract change not worth it).

### Summary table update

| Axis | Findings | Status (2026-08-30) |
|------|----------|---------------------|
| Performance | 8 bottlenecks + 9 micro-optimizations | P01–P08 FIXED (P08 documented-resolution); micro-opts fixed except 2 negligible leftovers |

## Third pass — broader sweep (2026-08-30)

After the P-series fixes landed, a wider sweep of the grown diff (58 files) covered the new `UnorderedPager` engine, the mem rework, the fs subsystem changes, and the server/config/e2e surface — three parallel review agents plus targeted parent verification. `crates/tinio-fs/src/multipart.rs` (256 changed lines) turned out to be test-only and clean. Nine new findings, all **OPEN**:

### T01 — F15 guard can silently truncate a listing that still has live keys — **OPEN** · medium (correctness, data visibility)
- **File**: `crates/tinio-fs/src/listing.rs:501-505`
- **Finding**: when every walked page entry vanishes between the walk and the hash, the F15 guard rewrites `(truncated=true, next=Some(m))` to `(false, None)`. But `truncated` was set by a *probe* entry beyond the page, which may still exist. Scenario: bucket holds `a1..a1001`, `max_keys=1000` → page `a1..a1000`, probe `a1001`, `next="a1000"`. A concurrent `rm` deletes exactly `a1..a1000` mid-listing (all computes return NotFound → skipped) → client sees `IsTruncated=false` and stops; the live `a1001` is never listed. The guard's premise is wrong: resume markers are exclusive-after and do not require the marker key to exist — without the guard the client resumes with `start_after="a1000"` and gets `a1001` (or an empty untruncated page if everything is gone). Either way it terminates; no "dead ranges forever" loop exists. Found independently by two review agents; confirmed against source by the parent. Note: the guard cannot misfire via compute failures — a non-NotFound compute error fails the whole listing, so an empty page after a truncated walk does mean "all vanished"; the bug is purely the suppression of the resume signal.
- **Fix**: delete the guard and propagate the pager's `(truncated, next)` unconditionally (a re-walk-from-marker alternative is strictly more complex for no semantic gain).

### T02 — mem `list_multipart_uploads` range ignores prefix and both markers — **OPEN** · medium (perf)
- **File**: `crates/tinio-mem/src/multipart.rs:369`
- **Finding**: the scan starts at `bucket_prefix..`; the `take_while` bounds only the bucket band, and the marker skip happens inside the engine — *after* `rsplit_once` + `object::key()` validation per row. A deep resume (large `key-marker`) or a sparse prefix re-scans and re-validates O(marker position) — or the whole bucket — per page. Uploads per bucket are uncapped.
- **Fix**: start the range at `max(bucket_prefix + params.prefix, bucket_prefix + composite-marker)` with `Bound::Excluded` for the marker, and extend `take_while` to the key-prefix band — mirroring `object.rs:233-240`, which already seeks to `Excluded(after_key)`.

### T03 — `e2e/interop/lib.sh` `stop_server` pgrep fallback can kill strangers and leak our own server — **OPEN** · low/medium (dev tooling)
- **File**: `e2e/interop/lib.sh:50-62`
- **Finding**: the fallback runs `pgrep -f 'debug/examples/serve'` and, if *anything* matches, kills those PIDs and **skips `kill "$SERVER_PID"` entirely**. With `--server-binary` pointing at a non-matching path (release/copied binary) while an unrelated debug `serve` runs, the stranger's server is killed and the script's own server leaks holding its redb lock — a later restart on the same root can hit `DatabaseAlreadyOpen`. The pattern also isn't scoped to `$REPO`.
- **Fix**: always kill `$SERVER_PID` first; use pgrep only as an additional sweep, anchored to the resolved `$SERVER_BIN` path.

### T04 — mem `adjust_total` reports the wrong size on limit breach — **OPEN** · low (correctness, diagnostic)
- **File**: `crates/tinio-mem/src/storage.rs:158-168`
- **Finding**: `map_err(|total| entity_too_large(total, limit))` reports the *current* total; the old Mutex code reported the would-be `new_total`. The `EntityTooLarge.size` payload is now wrong, and racy under concurrency.
- **Fix**: carry the projected value out of the `fetch_update` closure for the error.

### T05 — `UnorderedPager::offer` allocates the order `String` before heap admission — **OPEN** · low (perf)
- **Files**: `crates/tinio-core/src/storage/listing.rs:372-376`; call site `crates/tinio-fs/src/backend/buckets.rs:150-161`
- **Finding**: every offered item pays one `String` alloc even when the heap rejects it. `offer_keyed` exists for exactly this; fs `list_buckets` (`order_of = |name| name.as_ref().to_string()`) can use it verbatim (the bucket name IS its order), dropping O(matches) allocations per page to O(page). Composite uploads orders genuinely can't use it.
- **Fix**: switch the fs `list_buckets` call site to `offer_keyed`.

### T06 — mem `list_buckets` never seeks to `start_after` — **OPEN** · low (perf)
- **File**: `crates/tinio-mem/src/bucket.rs:105`
- **Finding**: the range starts at `params.prefix` only; deep resumes re-read skipped rows (no allocs — the skip precedes `Bucket` construction — just cursor steps). Capped by the ~1k-bucket ceiling.
- **Fix**: `Bound::Excluded(start_after)` when it exceeds the prefix, mirroring `object.rs`.

### T07 — Blocking lstat on the executor thread for DT_UNKNOWN filesystems — **OPEN** · low (perf regression, narrow)
- **Files**: `crates/tinio-fs/src/listing.rs:697`, `crates/tinio-fs/src/backend/mod.rs:586`
- **Finding**: tokio's `DirEntry::file_type()` is synchronous; on filesystems returning DT_UNKNOWN (some NFS/FUSE/older XFS mounts) std does a blocking lstat inline on the async worker — the old code's `fs::symlink_metadata().await` went through the blocking pool. Free on mainstream filesystems.
- **Fix**: if DT_UNKNOWN matters, fall back to `fs::symlink_metadata().await` explicitly instead of `file_type()`; otherwise document the platform assumption.

### T08 — Continuation token decoded with no length bound — **OPEN** · low (robustness)
- **File**: `crates/tinio-server/src/backend/buckets.rs:102`
- **Finding**: `URL_SAFE_NO_PAD.decode(token.as_bytes())` allocates ~¾ of the input before validation; token length is bounded only by the HTTP head buffer (hundreds of KB), so each request can force a largish allocation en route to `InvalidArgument`. Bucket names are ≤63 ASCII chars, so legitimate tokens are tiny.
- **Fix**: reject tokens longer than a small multiple of the max bucket-name length (e.g. >256 bytes → `InvalidArgument`) before decoding.

### T09 — `UnorderedPager` equivalence matrix only feeds sorted input — **OPEN** · low (test gap)
- **File**: `crates/tinio-core/src/storage/listing.rs:978-1011`
- **Finding**: the eviction/re-offer path (prefix evicted, later row of its group re-offers it) is exercised by exactly one 4-item test (`:1048`). The heap invariants deserve shuffled-input coverage.
- **Fix**: add a randomized shuffled-input equivalence matrix pinning the bounded-k invariant (page = k smallest distinct offers, marker respected, no double-emitted prefixes).

### Informational (no action)

- The new `[s3]` knobs (`max_buckets`, `max_keys`, `allow_zero_page_size`) are config-file-only — no env/CLI overlays. Consistent with the pre-existing bool toggles; note only if env configurability was intended. Negative TOML values correctly fail the serde `u32` parse at load.

### Verified clean this pass (recorded to prevent re-litigation)

- **`UnorderedPager` heap semantics are sound.** Marker filter runs before insertion, so ≤-marker entries never enter the heap; eviction pops the max and the heap provably holds the `max+1` smallest distinct offers (heap entries can only shrink after an eviction, so an evicted entry can never belong to the final page — the re-offer path is harmless). Rollup dedup via `heap_prefixes` with evict/re-offer keeps one entry either way; a prefix is never double-emitted and never swallows a pre-marker key (marker check precedes dedup, matching the ordered engine). `finish()`'s probe pop + resume marker correct. `max=0/1`, exact fill, empty input, all-filtered, duplicate keys: all correct.
- **mem `object.rs` `last_cp` mirror correct** — marker-inside-rollup, prefix-resume (`start_after="dir/"`), the `Excluded(after_key)` lower bound, and empty-prefix cases all walked; group state updates precede the marker skip exactly as the engine's do.
- **mem multipart lazy path preserves key/upload-id marker positioning** — composite order matches redb key order within the bucket band; malformed/tampered rows skip without panic. `take_while` Err passthrough correct (bucket.rs continues after an error where object.rs aborts — inconsistent, harmless).
- **`entry_is_bucket_dir`** (`backend/mod.rs:576-639`) correct on both platforms — unix d_type classifies link-vs-dir without following; Windows find-data catches junctions (`file_type()` misreports them as plain dirs). Classification-vs-use TOCTOU yields at worst a stale page entry — snapshot semantics, identical to the old lstat path.
- **`remove_many`** (`meta.rs:511`): single atomic write txn under the existing per-bucket mutation lock (scope unchanged); idempotent; empty-batch short-circuits; no interaction hazard with `create_multipart_upload`/`upload_part` (same lock).
- **`get_or_insert` record-only path** (`bucket.rs:90`): read inside the single-writer txn — first writer wins, creation-time semantics preserved.
- **Server boundary casts**: `cap as usize` lossless on 32-bit; `max_buckets as usize` pre-validated 1..=10,000; `normalize_page_size` casts only after `requested >= 1`; config u32::MAX safe; 10,000 accepted / 10,001 rejected inclusive-correct and test-pinned.
- **Escape hatch**: `allow_zero_page_size` does not leak into ListBuckets (its 1..=10,000 validation is inline); V1/V2/parts/uploads share `normalize_page_size` consistently.
- **Tokens**: encode/decode/`from_utf8` round-trip correct; empty token no-op; stale decodable token degrades to a marker. (Length bound is T08.)
- **`pipeline.rs` diff is test-only** (kind-panic survival, drain idempotency, lane labels) plus one stale-comment removal — no backpressure/error-propagation behavior change.
- **`serve.rs` `capabilities_for`** seam with two CI-gated tests; fixes the silently dropped bool toggles.
- **`coverage_gaps.rs`**: fresh tempdir per server, ephemeral ports, no sleeps — not flaky (the exact metrics content-type pin is brittle across crate upgrades, not timing).
- **`testing.rs` conformance additions**: `{prefix}-{counter}-{pid}` names can't collide; paged-union check robust to heap-ordered pages.
- **`boto3.sh` / `e2e/mod.rs` / `error_codes.rs`**: array-based arg passing correct, EXIT trap covers the second server.
- **fs `multipart.rs` diff is test-only** — 8 tests verified against the production paths they pin (zero-page/mismatched-key `list_parts`, §5.6 completion behavior incl. the verify-then-copy race closed by the assembly re-hash, publish-failure cleanup); assertions are error-kind-agnostic, portable across Unix/Windows.

### Priority

**T01** (real data-visibility bug — a client can permanently miss live objects; fix is a deletion) > **T02** (uncapped rescan on deep multipart resume) > **T03** (dev-tooling footgun with a lock-leak consequence). T04–T09 are small, independent fixes.

## Fourth pass — `/code-review max` (2026-08-31)

A `max`-effort `/code-review` run over the grown diff (58 files, +6,388/−474 — the same scope as the third pass). Ten parallel finder agents were dispatched but their reports were lost when the parent's turn ended (all outputs empty); the final report was rebuilt from the review agent's own line-level verification plus the one finder report that survived, re-verified against source. **F01 (build break) and F02 (lock split) were additionally re-verified by the parent session** against the working tree; all other line references below were verified by the review agent. One claim (a cleanup.rs false-success report) was dropped — already fixed in the current tree. Findings F01–F10, all **OPEN**. Note: the P/T-series fixes are all in place at this point — the finding IDs below start from the fourth pass only, not from earlier passes.

### F01 — Unix build break: `tokio::fs::DirEntry::file_type()` called without `.await` — **OPEN** · critical (build, 2/3 CI platforms)
- **File**: `crates/tinio-fs/src/listing.rs:698` (P02's Unix classification)
- **Finding**: The `#[cfg(unix)]` branch calls `entry.file_type()` with no `.await`. The entries come from `tokio::fs::read_dir` (`entries.next_entry().await`, `:673`), and tokio's `DirEntry::file_type` is `async fn` — so `match entry.file_type() { Ok(..) }` pattern-matches a `Future`, not a `Result`, and fails with E0308 on Linux/macOS. Windows passes because the branch is cfg'd out. Two correct forms exist in the same diff: the Windows half of the same block awaits (`entry.metadata().await`, `:706`), and the sibling `entry_is_bucket_dir` uses `entry.file_type().await` (`backend/mod.rs:582`). The inline comment's d_type rationale is right — only the await is missing. (Distinct from T07: T07 flags the *internal* blocking lstat std performs on DT_UNKNOWN filesystems — fixing F01 leaves T07 open.)
- **Fix**: `entry.file_type().await` at `:698`; the three match arms stay unchanged. Then a Unix `cargo check` (ubuntu/macos CI) is green again.

### F02 — lockmap hot path can lock an orphaned slot: per-key exclusion splits — **OPEN** · high (correctness, delete-vs-PUT race)
- **File**: `crates/tinio-util/src/lockmap.rs:78-83` (hot path), `:128-130` (eviction predicate)
- **Finding**: The hot path accepts a slot when `Arc::strong_count(&slot) >= 2` after cloning. Interleaving that breaks it:
  1. Waiter B pins the map and borrows the entry (`pinned.get(&key)` — a borrow, no strong ref), then is preempted.
  2. Holder A's `Guard::drop` runs `remove_if` — predicate `Arc::ptr_eq(live, slot) && strong_count == 2` (table + A's own `OwnedMutexGuard` ref) — and evicts the slot; A is then preempted before its guard ref drops.
  3. B resumes and clones: count = 2 (A's mid-drop ref + B's clone) → the `>= 2` check passes → B breaks and locks the **orphaned** mutex.
  4. C's `get_or_insert_with` inserts a fresh slot for the same key and acquires it immediately.
  B and C now hold two different mutexes for one key — the exact delete_bucket-vs-PUT-commit race the per-bucket mutation lock (and the RFC 7232 conditional-put lock) exists to prevent: the PUT's rename can land while the delete's emptiness check + unpublish rename are in flight, losing the object with a 200. The code's own comment assumes the evictor's ref is gone before the clone lands ("our clone then holds the only reference (`strong_count == 1`)") — the interleaving above breaks that assumption, and nothing in the code prevents it (B's pin→clone→check has no `.await`; A's drop is sync; on a multi-threaded runtime the two genuinely run concurrently). The `> 2` variant narrows the window — it fixes the single-waiter case — but two waiters cloning after the eviction still split.
- **Fix**: after the clone, re-verify identity against the live table entry before breaking — `pinned.get(&key).is_some_and(|live| Arc::ptr_eq(live, &slot))` — the same check the cold path already uses (`:92-97`). This is airtight: once B holds a clone, the eviction predicate (`strong_count == 2`) can never fire for that slot (count ≥ 3), so a slot that passes the identity check cannot be evicted before B's `lock_owned`; a slot that fails it is retried. Add a regression test that reproduces the interleaving with two concurrent waiters.

### F03 — Scanner counts channel sends as reclamation — **OPEN** · medium (diagnostics)
- **File**: `crates/tinio-fs/src/scanner.rs:248-249`; `tombstone.rs:109-110` (`RemoveTask::run`)
- **Finding**: `summary.reclaimed += 1` fires on `tombstone::enqueue_one` success — a channel send — while `RemoveTask::run` swallows `remove_tree` failures with a `warn!` and returns Ok. A delete-bucket tombstone whose tree cannot be removed (Windows share-mode-0 file, permission error) is therefore re-enqueued every scan cycle: `ScanSummary.reclaimed` climbs indefinitely, the bytes persist under `.tinio/deleting/`, and no error ever reaches the summary — an operator watching the summary sees steady reclamation activity and never learns the tree is stuck. The pre-P04 code incremented only after a removal actually succeeded.
- **Fix**: count from the removal side — `RemoveTask::run` returns a per-tree outcome (removed / skipped / failed); the summary increments only on actual removal, and a stuck tree logs `tracing::error` once (not per pass) with its path. Zero progress then reads as zero reclamation instead of a climbing counter. (Also feeds F07's visibility ask.)

### F04 — `[s3] max_buckets` above 10,000 is dead configuration — **OPEN** · low (config)
- **File**: `crates/tinio-server/src/backend/buckets.rs:85-92` (wire validation), `:111` (clamp)
- **Finding**: The wire rejects any `max-buckets` outside 1..=DEFAULT_MAX_BUCKETS (10,000) with InvalidArgument *before* `clamp_page_size` can act, so a configured cap above 10,000 never clamps anything: requests above 10k are rejected, requests ≤ 10k pass unclamped. `docs/superpowers/specs/001-s3-local-server/contracts/config.md` documents `max_buckets` as "0 = unlimited, no range validation" — the config space above 10k silently does nothing, with no startup warning.
- **Fix**: validate at config load (`crates/tinio-config/src/schema/s3.rs`): reject or clamp `max_buckets > 10_000` at startup with a clear error/warning — the wire ceiling is AWS-documented, so the cap is meaningful only within 1..=10,000. Update `config.md` to state the effective range.

### F05 — mem ListBuckets lazy scan hides redb error rows past the page — **OPEN** · low (robustness parity)
- **File**: `crates/tinio-mem/src/bucket.rs:90-110` (P05 scan); same pattern in `multipart.rs` `list_multipart_uploads` (P06)
- **Finding**: The P05 lazy `range`/`take_while` (with the `unwrap_or(true)` Err passthrough) + `paginate_ordered` scan only touches the rows the engine visits (page + one probe); a redb `Err` row beyond the page is never reached, so a corrupt row past the first page silently yields successful pages and only fails listings whose scan happens to reach it. The old eager `.collect::<Result<Vec<_>,_>>()?` failed the whole listing on any error row in the bucket band. mem is dev-grade, but this is a silent error-visibility regression versus the eager path.
- **Fix**: document the shift inline (like P08's accepted "document or fix" disposition) — mem is dev-grade and the cost of a full-band validation pass is exactly what P05/P06 removed; revisit if mem gains production use.

### F06 — `max-keys`/`max-parts`/`max-uploads` < 1 flip to InvalidArgument without a migration signal — **OPEN** (deliberate per plan; upgrade cost unannounced) — cross-ref S01
- **File**: `crates/tinio-server/src/backend/listing.rs:55` (S01); `[s3] allow_zero_page_size` in `config.md`
- **Finding**: S01's unified policy turned `< 1` from the legacy empty page (`.max(0)`) into InvalidArgument by default, and the `allow_zero_page_size` escape hatch defaults to `false` — so any pre-existing client that has always sent 0 (an interop tool, a scripted sweep) breaks with 400 after an upgrade, and the operator has no way to know the knob exists until requests start failing. Deliberate per the plan docs, but the break ships with no announcement.
- **Fix**: (a) document the breaking change in the upgrade note and `config.md` (the knob exists; the failure mode is discoverability); (b) optionally ship one release with `allow_zero_page_size = true` as the default (legacy empty page restored) and flip to strict (`false`) in the following release — the migration signal is the documented default change.

### F07 — DeleteBucket answers 204 before the tree is removed; removal failures are invisible and shutdown drops the lane — **OPEN** · medium (design cost)
- **File**: `crates/tinio-fs/src/backend/buckets.rs:118-126` (fire-and-forget `tombstone::reclaim`); `tombstone.rs` `RemoveTask`
- **Finding**: commit a78bbda moved tree deletion onto the REMOVAL lane: the rename to `.tinio/deleting/<uuid>` unpublishes the name, the request answers 204, and `RemoveTask` removes the tree later. Three costs versus the old sync `remove_dir_all` (which failed the delete on failure): (1) removal failures are a `warn!` the client never sees — data persists under `deleting/` until a scanner pass (scanner disableable via `TINIO_SCANNER`) or the startup repair; (2) `Pipelines::drain` deliberately does not await the removal lane, so a shutdown mid-removal drops the work with the 204 already sent; (3) nothing reports the stuck tree (ties into F03).
- **Fix**: (a) await the removal lane on shutdown with a bounded timeout so a graceful stop does not orphan trees; (b) surface stuck trees — `tracing::error` with the path on `remove_tree` failure, and count `deleting/` residue in the scanner summary (F03's outcome plumbing); (c) document the 204-before-gone contract (delete = unpublish + async purge) in the API docs so clients know a recreating bucket may briefly collide with the purge.

### F08 — `stop_server` POSIX fallback kills strangers; boto3.sh pagination-server teardown is trap-only — **OPEN** · low/medium (dev tooling)
- **File**: `e2e/interop/lib.sh:46-64`; `e2e/interop/boto3.sh:58-63`
- **Finding**: Two parts. (1) Kill-strangers — same root as T03, confirmed: the POSIX fallback `pgrep -f 'debug/examples/serve'` kills every matching process on the machine (other checkouts, a manual test session), and when anything matches, `kill "$SERVER_PID"` is skipped entirely (`:60-62`) — so with `--server-binary` at a non-matching path, the script's own server leaks holding the redb lock (a later restart on the same root hits `DatabaseAlreadyOpen`). (2) Teardown asymmetry — new detail: boto3.sh stops the journey server explicitly (`:45`) but the pagination server (`:58`) has no explicit stop; cleanup relies on the EXIT trap's pattern kill, which only catches binaries whose path contains `debug/examples/serve` — a custom-path binary leaks (only the last `$SERVER_PID` is killed, and only in the pgrep-no-match branch).
- **Fix**: in `stop_server`, always `kill "$SERVER_PID"` first, then use pgrep only as an additional sweep anchored to the resolved `$SERVER_BIN` path rather than a fixed pattern; in `boto3.sh`, add an explicit `stop_server` after the pagination run, mirroring `:45`.

### F09 — New pipeline test uses the prohibited `block_on` wrapper — **OPEN** · low (conventions)
- **File**: `crates/tinio-core/src/pipeline.rs:554-565`
- **Finding**: `not_sync_task_runs_through_the_inline_runner` wraps its body in `Runtime::new().unwrap().block_on(...)`; CLAUDE.md → Tests: "Async: `#[tokio::test]` / `async fn` directly — no `Runtime::block_on` / `rt(...)` wrappers." The diff adds a new instance of the exact prohibited pattern (sibling tests in the file share it pre-existing, which does not excuse the addition).
- **Fix**: convert to `#[tokio::test] async fn` — the body's async shape needs no bespoke runtime (the inline runner is runtime-agnostic).

### F10 — `max_keys == 0` short-circuit drops an unused pinned Stream — must_use warning on every build — **OPEN** · low (build hygiene)
- **File**: `crates/tinio-fs/src/listing.rs:344-352`
- **Finding**: The short-circuit's `self.walk_files_streaming(...).await?` constructs the walk stream purely for its side effect (the bucket-existence check, which happens at construction) and drops it un-polled; the boxed `Stream` is `#[must_use]` → "unused pinned boxed Stream trait object that must be used" on every build (confirmed via `cargo check -p tinio-fs`). Behavior is correct, but the warning is new noise and a `-D warnings` liability.
- **Fix**: `let _ = ...;` on the construction, or extract the probe into a small `ensure_bucket_exists` helper that owns the call.

### Priority

**F01** (2/3 CI platforms cannot build) > **F02** (delete-vs-PUT lock split — object loss with a 200) > **F03** (false reclamation signal hides stuck trees; feeds F07) > **F08** (tooling kill/leak footguns) > **F07** (204-before-gone with invisible failures and no shutdown drain) > **F06** (silent wire-compat flip). F04, F05, F09, F10 are small, independent fixes.

## Fifth pass — fixing T01–T09 / F01–F10 (2026-08-31)

All nineteen findings implemented in the working tree, each verified by the relevant tests (the workspace suite is green; F01 additionally verified by a Unix-target `cargo check` since the branch is cfg'd out on Windows). Per-finding outcome:

| ID | Severity | Fix landed |
|----|----------|-----------|
| F01 | critical (build) | `entry.file_type().await` (listing.rs) — verified compiling on `x86_64-unknown-linux-gnu`. The actual Unix check surfaced THREE further pre-existing breakages in the same class (the review's "cargo check is green again" had never been run): path.rs `of_async` missing `#[cfg(unix)] use tokio::fs`; fs `put_part_copy` typed `tokio::fs::File` against the unix `stage_copy(StdFile)` chain (now `StdFile`, matching the `into_std` conversion at its only caller); `file_identity_handle` dead-code warning on Unix (now `#[cfg(windows)]` — its only caller is `file_identity_async`'s Windows branch). Unix lib check now warning-free |
| F02 | high (lock split) | hot path re-verifies the cloned slot by IDENTITY against the live table entry (`Arc::ptr_eq`), not by the count; 8×20k-cycle multi-thread stress net added. Note: a deterministic interleaving reproduction is impossible through the public API (the window is ~10 ns of OS preemption inside a pin→clone gap — measured: 20M cycles never fired the old bug), so the regression test is documented as a probabilistic invariant net; the fix's airtightness is the reasoning that a clone pins `strong_count ≥ 3`, disabling the eviction predicate |
| F03 | medium (diagnostics) | `RemoveTask::run` propagates the failure; the scanner awaits leftover completions (`FuturesUnordered`) and counts only actual removals; `ScanSummary.removal_failures` added; a stuck tree is error-logged once (known-stuck set in `Scanner`); fire-and-forget paths (`enqueue_one`, `reclaim`, cleanup stage) log the failure from a detached awaiter; new Windows test `stuck_tombstone_counts_as_a_failure_not_as_reclaimed` (two passes) |
| F04 | low (config) | `#[garde(range(min = 0, max = MAX_BUCKETS))]` on `Capabilities.max_buckets` — >10,000 rejected at parse; test `max_buckets_above_the_aws_ceiling_is_rejected_at_parse`; `config.md` states the effective range |
| F05 | low (robustness) | inline doc in mem `list_buckets` (the lazy scan never reaches error rows beyond the page — the documented shift vs the eager `collect`) |
| F06 | low (compat) | documented only (author decision): `config.md` carries the breaking-change note (`< 1` → `InvalidArgument` since 2026-08) and the `allow_zero_page_size` escape hatch; no default flip (no release history) |
| F07 | medium (design) | (a) `Pipelines::drain` awaits the removal lane with a bounded 10 s timeout overlapping the IO drain (constant + comment; pipeline-spec.md revision note updated); (b) stuck trees: error log + `removal_failures` summary count (F03 plumbing); (c) s3-surface.md documents the 204-before-gone contract |
| F08 | low/medium (tooling) | `stop_server` always kills `$SERVER_PID` first; the pgrep fallback is anchored to the resolved `$SERVER_BIN` path, never a bare pattern; boto3.sh adds an explicit `stop_server` after the pagination run |
| F09 | low (conventions) | `not_sync_task_runs_through_the_inline_runner` converted to `#[tokio::test] async fn` (no `block_on` wrapper) |
| F10 | low (build hygiene) | `let _ =` on the dropped walk stream; `cargo check -p tinio-fs` warning-free |
| T01 | medium (correctness) | F15 guard deleted; `(truncated, next)` propagate unconditionally (exclusive-after markers need no live key — the sweep terminates either way); the pinning test rewritten as `page_whose_entries_all_vanish_keeps_the_resume_marker` |
| T02 | medium (perf) | mem `list_multipart_uploads` seeks the range to `max(prefix-band, composite marker)` with `Bound::Excluded` for the marker; `take_while` bounds the key-prefix band within the bucket; all multipart listing tests green |
| T03 | low/medium (tooling) | same fix as F08 (one fix for both) |
| T04 | low (correctness) | `adjust_total` carries the projected size out of `fetch_update` (`Cell`); new test `limit_breach_reports_the_projected_size` (14, not 4, with limit 8) |
| T05 | low (perf) | fs `list_buckets` offers via `offer_keyed` (order String only for heap-admitted entries; the `order_of` closure is documented as never invoked on the delimiter-less path) |
| T06 | low (perf) | mem `list_buckets` seeks to `Bound::Excluded(start_after)` when it exceeds the prefix (mirroring object.rs); the now-dead filter_map marker check removed |
| T07 | low (perf) | documented as a platform assumption at both classification sites (listing.rs walk + `entry_is_bucket_dir`) — tokio exposes no raw d_type, so the blocking-pool fallback is unobservable and would cost a stat per entry on every filesystem |
| T08 | low (robustness) | tokens > 256 bytes rejected as `InvalidArgument` BEFORE the decode (which would allocate ~¾ of the input); test covers the rejection and the 256-byte boundary (decodes to NULs — valid UTF-8 — a stale marker) |
| T09 | low (test gap) | `unordered_matches_ordered_on_shuffled_input` — 300 deterministic xorshift Fisher–Yates shuffles × 4 prefixes × 5 markers × 5 maxima, compared through the sorted multiset (the ordered engine assumes sorted input) |

One finding-adjacent correction: the F02 fix comment in the code documents why the identity check is airtight (a cloned slot pins `strong_count ≥ 3`, so the eviction predicate can never fire for it), and the F02 stress test's comment records the measured probabilistic limitation honestly rather than overclaiming determinism.
