# Code Review — Multipart checksum validation diff vs. the 2026-08-31 design spec

**Date**: 2026-08-31
**Scope**: `git diff HEAD` (uncommitted, staged + unstaged) — 30 files, +2,357/−216 — plus the untracked new files `crates/tinio-core/src/checksum.rs` and `crates/tinio-server/src/backend/checksum.rs`. Implements `docs/superpowers/specs/2026-08-31-multipart-checksum-validation-design.md` per `docs/superpowers/plans/2026-08-31-multipart-checksum-validation.md`.
**Method**: two-axis review (Standards / Spec) run as parallel sub-agents, per the `code-review` skill. Standards sources: `CLAUDE.md`, `docs/style.md`, `docs/cargo.md`, plus the Fowler smell baseline. Spec axis quotes the design doc for every finding. Line numbers refer to the current working tree. The parent session re-verified W01–W04 and C01 line-by-line against the tree.

## Verdict

The implementation is broadly faithful to the spec: VerifyStream single-pass hashing (md5 + requested algorithm in one `s3s::checksum::ChecksumHasher`), trailer expectation read at stream end (R4), Content-MD5 coexistence (R6), CRC64NVME default FULL_OBJECT (R7), the algorithm×type validity table, type-conflict → `BadDigest` (R3), D2 skip-with-warn, D5 copy split, toggle-off passthrough in all five ops, publish-txn stale-row clearing, additive redb tables with ensure-on-open (R10), and zero hashing in the backends all check out. The composition/linearization math carries the spec-mandated self-validating randomized test plus known check values.

However, **two locked decisions are not implemented**: the `set_part_checksum` etag guard (R9 — the contract lost the `etag` parameter entirely, leaving the lost-update race the spec designed against wide open) and the R8 lock-ordering change (paging/validation still run before `lock_object`, against an explicit locked decision, justified only by a new comment). Two further validation-logic bugs (W03, W04) let requests through that the spec's error table rejects.

Findings below: 3 hard standards violations (H01–H03), 8 baseline smells (M01–M08), 4 implemented-but-wrong spec items (W01–W04), 2 scope-creep items (C01–C02), 4 minor items (N01–N04). **Status at round 2 (verified 2026-08-31 against the tree): all resolved** — see [Round-2 resolution](#round-2-resolution-verified-2026-08-31); the design doc's review log (rounds 1–2) records each resolution.

---

## Round-2 resolution (verified 2026-08-31)

Every finding below was re-verified against the working tree on 2026-08-31 and is resolved as tagged; the design doc's [review log (rounds 1–2)](../specs/2026-08-31-multipart-checksum-validation-design.md) records the rationale. Highlights: W01 was resolved by **redesign**, not patch — `set_part_checksum`/CAS is gone, replaced by the `upload_part` tee-slot atomic commit (design round-1 log R9); W02–W04 and C01 per the design round-2 log; H01 verified clean (`cargo +nightly fmt --check`); H03 via `parse_display`. The one deliberate keep is M07 (the `""` sentinel, no schema change) and M06 (partial — the tee pair tuple remains). Full suite green: `cargo test -p tinio-core -p tinio-config -p tinio-fs -p tinio-mem -p tinio-util -p tinio-server`. The boto3/journey e2e tests with `[s3] checksum = true` are added (`tests/boto3.rs`, `tests/journey.rs`, `#[ignore]`d — they need real boto3/aws-cli).

## Standards — hard violations

### H01 — Working tree fails `cargo +nightly fmt --check` — **[RESOLVED — `cargo +nightly fmt --check` exits clean]**
- **Files**: ~80 spots across 12 files (incl. `crates/tinio-core/src/checksum.rs:129`, `crates/tinio-core/src/lib.rs:27`, `crates/tinio-server/src/backend/checksum.rs`, `crates/tinio-server/src/backend/multipart.rs`, `crates/tinio-fs/`, `crates/tinio-mem/`, `crates/tinio-util/src/testing.rs`) · **Standard**: repo `rustfmt.toml` (nightly unstable features) — the format gate.
- **Finding**: The sub-agent ran `cargo +nightly fmt --check` and reports failures across the change (parent did not re-run it). Tooling catches this, so no per-hunk list — but the tree cannot be committed as-is.
- **Fix**: `cargo +nightly fmt --all`, then re-check.

### H02 — Inline 3+-segment paths — **[RESOLVED — top-level `use` only (e.g. `use http::header::HeaderName`, `STANDARD` import); no inline 3+-segment paths remain]**
- **Standard**: `docs/style.md` → Imports ("3+ (`a::b::c`): `use` then short form — code, tests, benches"; "Top `use`; never inline").
- **Findings**:
  - `base64::engine::general_purpose::STANDARD` spelled out inline 8+ times: `crates/tinio-server/src/backend/checksum.rs:417,454,472,556,572,641-644` and the composite test in `crates/tinio-server/src/backend/multipart.rs` (~:1515). Fix: `use base64::engine::general_purpose::STANDARD;` once per module.
  - `http::header::HeaderName::from_static` inline at `checksum.rs:174,305`. Fix: `use http::header::HeaderName;`.
  - `s3s::dto::ETag` in test signatures at `multipart.rs:1813,1839` — `dto` is already imported; use `dto::ETag`.
  - `use http::HeaderMap; use s3s::dto::UploadPartInput;` inside a test fn body at `checksum.rs:650-651`. Fix: move to the `mod tests` top.

### H03 — Hand-rolled wire (de)serialization vs. the `parse-display` convention — **[RESOLVED — `parse_display` adopted: `#[derive(Display, FromStr)]` with `#[display(style = "UPPERCASE")]`/`SNAKE_CASE`; `as_wire`/`from_wire` dropped]**
- **File**: `crates/tinio-core/src/checksum.rs:39-84` · **Standard**: `docs/style.md` → Types & defaults ("`parse-display` enums"; repo precedent `tinio-config/src/schema/log.rs:18`).
- **Finding**: `ChecksumAlgorithm`/`ChecksumType` hand-roll `as_wire`/`from_wire`. Justifiable — `from_wire` deliberately returns `Option` (corrupt-row skip on read) and `tinio-core` is dependency-free by design (spec §Contract changes: "no new dependencies — plain types only"). The spec arguably overrides the convention here.
- **Fix**: Either adopt `parse_display::{Display, FromStr}` with `#[display("CRC32")]` etc., or record the exception in the module doc of `checksum.rs` so the deviation is deliberate and visible.

## Standards — baseline smells (judgement calls)

### M01 — Duplicated Code: hasher-slot switch ×4 — **[RESOLVED — shared `enable_algo(hasher, algo)` helper in `backend/checksum.rs`]**
- `match algo { Crc32 => hasher.crc32 = Some(Crc32::new()), … }` appears at `checksum.rs:271` (`VerifyStream::wrap`), `checksum.rs:394` (`hash_bytes`), `checksum.rs:563` (test `crc_raw`), `multipart.rs:981` (test `client_checksum`).
- **Fix**: one `fn enable_slot(hasher: &mut ChecksumHasher, algo: ChecksumAlgorithm)` in `checksum.rs`, reused everywhere (tests included).

### M02 — Duplicated Code: six-field algorithm/value array ×3 — **[RESOLVED — `single_checksum_value` + `ValueFields` trait (at-most-one check, `InvalidRequest` on a second field)]**
- The `[(Crc32, x.checksum_crc32.as_deref()), …]` literal recurs at `checksum.rs:90-97`, `multipart.rs:64-72` (`part_checksum`), `multipart.rs:80-88` (`full_object_value`).
- **Fix**: a shared constructor taking the six `Option<&str>`s (or one "at most one checksum value" helper parameterized by the field list), called from all three sites.

### M03 — Duplicated Code: `op_complete_multipart_upload` value-present / value-absent tails — **[RESOLVED — `stored_parts` / `derive_full_checksum` / `resolve_checksum_type` extract both branches]**
- `crates/tinio-server/src/backend/multipart.rs`: the `stored_checksums` collection (:634-637 ≡ :696-699), the `sizes_list` build (:656-662 ≡ :712-718), and the `Composite`/`FullObject` compute+echo match (:645-678 ≡ :721-728) are near-verbatim repeats across the two branches.
- **Fix**: extract `gather_parts(stored, parts) -> (Vec<PartChecksum>, Vec<u64>)` and one `compute_full_object(algo, checksum_type, parts) -> Option<ChecksumValue>`; call both from each branch. Also resolves part of M08.

### M04 — Duplicated Code (minor): `.map(|a| a.as_wire().parse().unwrap())` ×5 — **[RESOLVED — `wire_algo`/`wire_type` helpers]**
- `multipart.rs:163,166,849,852,896-897`.
- **Fix**: one small `fn wire_dto(...)` helper next to `set_output_checksum`.

### M05 — Middle Man: `set_output_checksum` — **[RESOLVED — helper deleted; `HasFields::set_checksum` called directly]**
- `checksum.rs:194-199`: a one-line forwarder to `HasChecksumFields::set_checksum`.
- **Fix**: delete it; call `output.set_checksum(algo, value)` directly at the call sites.

### M06 — Data Clumps / Primitive Obsession: algorithm/value tuples restating `PartChecksum` — **[ACCEPTED — partial: the `echo_checksum` tuple collapsed into `derive_full_checksum`'s `Option<(Algorithm, Value)>`; the `(Arc<VerifyState>, Spec)` tee pair remains, minor]**
- `(ChecksumAlgorithm, String)` / `(ChecksumAlgorithm, &str)` at `multipart.rs:63,79`, `echo_checksum: Option<(ChecksumAlgorithm, ChecksumValue)>` at :529, and the `tee: Option<(Arc<VerifyState>, ChecksumSpec)>` pair destructured at five sites.
- **Fix**: return/pass `PartChecksum` (the type exists for exactly this); bundle the tee pair in a small struct (e.g. `Tee { state, spec }`).

### M07 — Primitive Obsession: `""` sentinel for `Option<ChecksumType>` in persistence — **[RESOLVED as decided — sentinel kept; additive tables, no schema change (design round-2 M07)]**
- `crates/tinio-fs/src/database/tables.rs:505`, `crates/tinio-fs/src/multipart.rs:296`, `crates/tinio-mem/src/storage.rs:65`: a magic empty string encodes "no checksum type" in the `UPLOAD_CHECKSUMS` value.
- **Fix**: store the type in its own table/second value column, or encode `Option<&str>` explicitly (e.g. an enum-tagged byte) instead of a sentinel.

### M08 — Divergent Change (mild): checksum policy swelling `backend/multipart.rs` — **[RESOLVED — validation helpers shared (`stored_parts`/`derive_full_checksum`); full move into `backend/checksum.rs` not pursued (design round-2 M08)]**
- The file grows +1445 to ~2900 lines; the ~200-line pre-commit validation block in `op_complete_multipart_upload` is checksum policy that belongs next to `checksum.rs`'s derivations.
- **Fix**: move the validation body into `checksum.rs` as one function, e.g. `validate_complete(input, upload, stored) -> S3Result<Option<(ChecksumAlgorithm, ChecksumValue)>>`, leaving the op as orchestration. Do together with M03.

No Feature Envy / Speculative Generality findings worth reporting; the `Cargo.toml` changes conform to `docs/cargo.md`.

---

## Spec — implemented but wrong

### W01 — R9 CAS dropped: `set_part_checksum` lost its etag guard — **[RESOLVED via redesign — R9 revised: `upload_part` gains the tee-slot param `Option<Arc<PartChecksum>>`; the checksum row commits atomically with the part row (write-or-clear), so the lost-update race cannot exist; no CAS, no second call (design round-1 log R9)]**
- **Spec**: §Contract changes — "`set_part_checksum(…, checksum: PartChecksum, etag: &str)` — conditional upsert of the part's checksum row, applied only when the currently stored part's etag equals `etag` (CAS — prevents a stale tee result from overwriting a newer re-uploaded part's row, R9)"; Decisions — "`set_part_checksum` as the uniform persistence path … conditionally on the returned etag (CAS, R9)".
- **Finding**: The contract method has **no `etag` param** and its doc says plain "upsert" (`crates/tinio-core/src/storage/multipart.rs:171-183`). The fs (`crates/tinio-fs/src/multipart.rs:346`, `Store::set_part_checksum`) and mem (`crates/tinio-mem/src/multipart.rs:157`) implementations write unconditionally, checking only upload existence — they do not even verify the part row exists. Both server call sites (`crates/tinio-server/src/backend/multipart.rs:258-270`, `:368-380`) call it without any guard. The exact R9 race is live: a re-upload of part N (whose publish txn clears the stale row) can be followed by the older tee's `set_part_checksum`, restoring a stale digest → false `BadDigest` at Complete. The spec-mandated tests ("`set_part_checksum` stale-etag no-op (R9)"; conformance "etag guard") are correspondingly absent.
- **Fix**: Restore `etag: &str` on the contract signature. fs: inside the same write txn, compare the current `PARTS` row's etag against the argument; mismatch (or missing part) → no-op `Ok(())`. mem: compare against `PART_META` likewise. Server: pass `part.etag` (the etag `upload_part` just returned) from both call sites. Add the two spec-mandated tests.

### W02 — R8 lock ordering not done: paging + validation still run before the write lock — **[RESOLVED — the `list_parts` paging, the 5 MiB rule, and the pre-commit validation run under `lock_object` (design round-2 W02)]**
- **Spec**: Architecture — "validation requires **moving the paging inside the lock** — an explicit reordering of existing code (R8)"; "pre-commit, per-object write lock held".
- **Finding**: `op_complete_multipart_upload` pages `list_parts` and runs the whole validation block *before* `lock_object` (`crates/tinio-server/src/backend/multipart.rs:478-534,530-734` vs the lock at `:738`), with a new comment (:470-477) re-arguing the old order via the backend's in-txn ETag re-verification. That argument mirrors the spec's own pre-commit race argument and is plausibly sound — but it contradicts a **locked decision**; a code comment cannot rescind R8.
- **Fix**: Either move the paging + validation under `_guard` as specified, or formally amend the spec (review log round 2) recording that R8 is rescinded in favor of the etag-reverification argument, with the fs assembly-time re-hash and mem write-txn re-verification cited as the mechanism. One of the two must happen — code and spec must not disagree.

### W03 — CompletedPart cross-check gated on the create-algorithm — **[RESOLVED — the value-vs-stored comparison runs whenever the client sent an entry and a stored value exists; only the algorithm-consistency `InvalidRequest` is gated on the upload's algorithm; missing stored value skips (D2) (design round-2 W03)]**
- **Spec**: Architecture, Complete step 1 — "cross-check `CompletedPart.checksum_*` vs stored → `BadDigest`" (unconditional; the algorithm-vs-create `InvalidRequest` is step 2's concern). Error table — "CompletedPart entry vs stored mismatch → `BadDigest` 400".
- **Finding**: The entire cross-check loop sits inside `if let Some(upload_algo) = upload.checksum_algorithm` (`crates/tinio-server/src/backend/multipart.rs:536-563`). Stored part checksums exist without a create-algorithm — `op_upload_part` persists a value whenever the request carried one (:254-272) — so a client sending `CompletedPart` checksum entries on a non-algorithm upload gets **no cross-check at all**: a wrong client value commits silently. Related defect at :555: `stored_value.is_none() → BadDigest` punishes D2's own scenario (part stored without a checksum, e.g. uploaded with the toggle off) — a client entry that cannot be checked should skip-with-warn, not fail.
- **Fix**: Run the value-vs-stored comparison whenever the client sent an entry and a stored value exists, independent of `upload_algo`; gate only the algorithm-consistency `InvalidRequest` on `upload_algo`. Treat a missing stored value as skip (D2 semantics), not `BadDigest`.

### W04 — FULL_OBJECT size check shadowed by the D2 skip — **[RESOLVED — `x-amz-mp-object-size` presence + sum checks run before the D2 completeness gate in `derive_full_checksum`; only the digest comparison stays under the skip (design round-2 W04)]**
- **Spec**: Error table — "FULL_OBJECT value without `x-amz-mp-object-size`, or size vs Σ parts → `InvalidRequest` 400".
- **Finding**: The D2 early-warn at `crates/tinio-server/src/backend/multipart.rs:642-644` (`stored_checksums.len() != parts.len()` → skip) runs *before* the `mpu_object_size` presence and sum checks at :650-671. A FULL_OBJECT value sent **without** the size header is silently accepted whenever any listed part lacks a stored checksum — the spec-mandated `InvalidRequest` never fires.
- **Fix**: Hoist the `mpu_object_size` presence check (and the size-sum check, which depends only on stored sizes, not stored checksums) above the stored-checksum completeness gate, so request-shape validation always runs; only the digest comparison stays under the D2 skip.

## Spec — scope creep

### C01 — Create rejects `checksum_type` without an algorithm — **[RESOLVED — accepted and dropped with a `warn!` (design round-2 C01)]**
- **Finding**: `op_create_multipart_upload` returns `InvalidRequest` for `checksum_type` without `checksum_algorithm` (`crates/tinio-server/src/backend/multipart.rs:141-146`). Nothing in the spec's error table, architecture, or deviations mandates this rejection — it is an invented request-shape rule. (AWS's actual behavior here is also undocumented in the spec.)
- **Fix**: Drop the rejection (accept and persist/drop per the toggle), or record it as a new deviation in the spec's Deviations section and in the compatibility docs.

### C02 — Dangling spec references — **[RESOLVED — no `spec Q*` references remain in `backend/multipart.rs` or `tinio-util/testing.rs`]**
- **Finding**: Comments cite "spec Q3" (`crates/tinio-server/src/backend/multipart.rs:694`) and "spec Q7" (`crates/tinio-util/src/testing.rs`, re-upload comment) — the design doc defines R1–R11 and D1–D6; no Q-items exist. Readers cannot resolve the references.
- **Fix**: Point the comments at the real spec items (the echo-only computation is Architecture Complete step 3; the re-upload row-clear is §Persistence) or drop the qualifiers.

## Spec — minor

### N01 — `part_checksum` silently takes the first of several `CompletedPart` checksum fields — **[RESOLVED — `part_checksum` routes through `single_checksum_value`; a second field answers `InvalidRequest`]**
- `crates/tinio-server/src/backend/multipart.rs:63-74` uses `find_map`, while `full_object_value` (:78-101) rejects two values with `InvalidRequest`. Spec: "More than one `checksum_<algo>` value in one request → `InvalidRequest`". A single `CompletedPart` carrying two checksum fields is the same shape violation.
- **Fix**: Mirror the error in `part_checksum` (make it fallible, reject a second field per entry).

### N02 — CHK019 says "D1–D5"; the spec defines D1–D6 — **[RESOLVED — design doc Deviations lists D1–D6 (D6 = ten-variant enum); CHK019 updated to "D1–D6" with the D6 description]**
- `specs/001-s3-local-server/checklists/compatibility.md:40`.
- **Fix**: Update to "D1–D6".

### N03 — Broken doc link to the design spec — **[RESOLVED — `s3-surface.md` links `../../../docs/superpowers/specs/2026-08-31-multipart-checksum-validation-design.md`]**
- `specs/001-s3-local-server/contracts/s3-surface.md:21` links `../superpowers/specs/2026-08-31-…`, which resolves to a nonexistent `specs/001-s3-local-server/superpowers/…`.
- **Fix**: `../../../docs/superpowers/specs/2026-08-31-multipart-checksum-validation-design.md`.

### N04 — Contract signature drift from the spec, undocumented — **[RESOLVED — design doc §Contract changes records the `key` param on `get_multipart_upload` and the tee-slot `checksum` param on `upload_part`]**
- `get_multipart_upload`/`set_part_checksum` gained a `key` parameter the spec's signatures don't have. Harmless in itself (enables the key-match check), but the spec's §Contract changes no longer describe the contract.
- **Fix**: Note the `key` param (and, per W01, the restored `etag` param) in the design doc's contract section.

## Verified correct (no action)

VerifyStream mechanics (md5 + algorithm in one pass; trailer read at stream end; a missing declared trailer counts as mismatch); `BadDigest` mapping on `state.mismatched()`; two checksum-value sources → `InvalidRequest`; algorithm-without-value → `InvalidRequest`; composition = hash of concatenated raw part digests; CRC linearization (self-validating randomized test + pinned check values); CRC64NVME defaulting to FULL_OBJECT (R7); type-conflict → `BadDigest` (R3); D5 copy split with `copy_part_fast` retained for non-algorithm uploads; toggle off ⇒ today's path in all five ops; re-upload clears the stale checksum row in the fs publish txn and in mem; `consume`/`abort`/`drain_bucket` drain both new tables; the R11 effective-coverage matrix in `s3-surface.md`; e2e journey + boto3 + `error_codes.rs` coverage.

## Suggested fix order

All items below are **complete** (verified 2026-08-31; see [Round-2 resolution](#round-2-resolution-verified-2026-08-31) and the design doc's review log):

1. ~~**W01** (R9 CAS) — correctness race; touches the contract, both backends, and the server call sites.~~ → redesign (tee-slot atomic commit, design round-1 R9)
2. ~~**W03 + W04** (validation-logic bugs in `op_complete_multipart_upload`)~~ → fixed; shared helpers (`stored_parts`/`derive_full_checksum`)
3. ~~**W02** (R8)~~ → paging + validation moved under `lock_object`
4. ~~**C01, N01**~~ → accept-and-drop with warn; second `CompletedPart` field rejects
5. ~~**H01–H03, M01–M08**~~ → fmt clean; `parse_display`; shared helpers (`enable_algo`, `single_checksum_value`, `wire_algo`/`wire_type`, `HasFields`); M06/M07 accepted as decided
6. ~~**C02, N02–N04**~~ → comments fixed; CHK019 D1–D6; link corrected; design doc contract section updated
