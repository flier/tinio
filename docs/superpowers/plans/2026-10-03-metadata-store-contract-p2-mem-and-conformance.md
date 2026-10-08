# Metadata store contract, plan 2 — `tinio-mem` migration, the second consumer's conformance run, and the close-out: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `tinio-mem` onto the four persistence contracts (metadata, version rows, content, blocks), preserve its TOCTOU-free bucket delete through `put_guarded`/`remove_if_empty`, run the conformance suite from the second consumer's tests, and close the work out — `cargo test --workspace`, clippy, docs and both cucumber passes green.

**Architecture:** mem keeps its ops modules and its byte accounting; metadata and content go through `tinio-store-redb`'s implementations held as **concrete handles** (`Arc<RedbMetadata>`, `Arc<CountingContent<RedbContent>>` — round 40: no `dyn`). Content is written before the metadata row that describes it, and a failed guarded write removes what it staged.

**Tech Stack:** Rust (edition 2024), `async-trait`, `redb` (via `tinio-store-redb`), `tokio`, `tinio-util`'s `testing` harness.

**Spec:** `docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md` — this plan implements its **§4 `tinio-mem`** and the second consumer's half of **§6 Testing and acceptance**.

**Depends on:** `docs/superpowers/plans/2026-10-03-metadata-store-contract-p1-contract-and-fs.md` — the contracts, `tinio-store-redb`'s implementations and the conformance harness must exist with the signatures P1's Task 1 pins.

## Global Constraints

- English only in repo artifacts; no hard-wrapped prose (`docs/compress.md`); the doc gate is `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- Per-crate `Error` with `impl From<Error> for tinio_core::storage::Error`; the mem `DatabaseError` alias and the `Error::Database` field survive, and the contract errors convert into them (never the reverse).
- redb schema: `FORMAT_VERSION` stays `1`; mem's state is in-memory, so a content-table re-key is free — no migration.
- Every multi-table operation is one write transaction; content is published before the metadata row is written (spec §1 property 6).
- Unit tests inline (`#[cfg(test)] mod tests`); conformance legs live in `tinio-util`'s `testing` module and are called from mem's tests.
- mem's byte limits and its `total_bytes` accounting live in **one** place, the `CountingContent<C>` `ObjectContent` decorator; no op body counts bytes itself (round 29) — the one carve-out is the versioned paths' own byte moves (a version write/delete, a completion's history bytes), which never pass through the decorator and move the total through `MemoryStorage`'s locked forms (see the close-out note).
- P2 migrates mem's **access layer** only — function boundaries, branches and error variants stay as they are (the operation bodies move into `tinio-ops` in P4, round 27's rule).
- mem's version **row** access goes through `VersionMeta` here; the ten `VersionOps` method bodies stay for P4's version group (round 29).
- Git: every commit step asks the operator first (`docs/git.md`); no AI/agent trailers.
- Gates: `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, the `@mem` and `@fs` cucumber passes.

## Review Focus

1. **A rejected write leaving counted bytes behind.** `MemoryOptions` accounting must be unchanged after a `put_object` that failed its guarded write: the staged content is removed and the counter corrected.
2. **A concurrent `put_object` racing `delete_bucket`.** The outcome is either `BucketNotEmpty` or a complete object — never an object row (or counted bytes) in a removed bucket.
3. **A drain or batch that became a loop.** `delete_bucket`, `abort`, `consume`, `put_batch` and `remove_many` keep one transaction per operation.
4. **A mem-only behaviour drift.** The `Storage` surface, the error paths and the byte limits are unchanged; `assert_conformance` passes with no assertion edits.
5. **The bytes-first order being reversed for parts.** A part's bytes are published before its `PARTS` row; a completion that reads part rows must never find a row whose bytes are absent.

---

### Task 1: mem metadata onto the contracts, and the counting content

**Files:**
- Create: `crates/tinio-mem/src/counting.rs` (`CountingContent<C>`: an `ObjectContent` decorator enforcing `MemoryOptions`)
- Modify: `crates/tinio-mem/src/storage.rs` (`MemoryStorage` holds the shared `Arc<RedbMetadata>` + `Arc<CountingContent<RedbContent>>`; the byte-accounting helpers move into the decorator), `crates/tinio-mem/src/bucket.rs` (`BucketOps` bodies through `BucketMeta`), `crates/tinio-mem/src/object.rs` (`ObjectOps` bodies through `ObjectMeta`), `crates/tinio-mem/src/error.rs` (the conversions)
- Test: the `#[cfg(test)] mod tests` in those files

**Interfaces:**
- Consumes: P1's `RedbMetadata`, `RedbContent`, `assert_metadata_conformance`.
- Produces: `MemoryStorage::metadata(&self) -> &Arc<RedbMetadata>` (test-visible), `MemoryStorage::content(&self) -> &Arc<CountingContent<RedbContent>>`, and the migrated `BucketOps`/`ObjectOps` bodies.

- [x] **Step 1: Write the failing tests** — a test that calls `tinio_util::testing::assert_metadata_conformance(storage.metadata())`; one that rejects a `put_object` into a missing bucket and asserts the `MemoryOptions` total is unchanged afterwards; and the decorator's own tests — `max_object_bytes` refused during `stage` (the total untouched), `max_total_bytes` enforced atomically at `publish` (a concurrent publish cannot overshoot the cap), **an overwrite asserting the total after two writes to one key (the delta rule)**, and `remove`/`rename` subtracting the byte count.
- [x] **Step 2: Run to verify they fail** — `cargo test -p tinio-mem`. Expected: no metadata accessor, no `CountingContent`.
- [x] **Step 3: Implement `CountingContent<C>`** — stage counts the chunks it accepts and refuses `max_object_bytes`; `publish` reads the key's **current** length inside the same critical section and adds only the **difference** with `AtomicU64::fetch_update` (refusing without writing when the total would overshoot); `remove` and `rename` subtract what is stored; nothing else in mem counts bytes (round 29; the audit's overwrite finding — an add-only rule bills ten 1 MiB overwrites of one key as 10 MiB).
- [x] **Step 4: Migrate the metadata access** — bucket ops through `BucketMeta` (`delete_bucket` becomes `remove_if_empty` plus the existing not-empty mapping); object ops through `ObjectMeta` with content published first and `put_guarded` last, compensating content removal (through the decorator, so the accounting follows) on rejection; delete `check_bucket`/`check_upload`'s table-level helpers in favour of the contract's guarded forms. **Only the metadata-access layer changes**: function boundaries, branches and error variants stay as they are — moving the operation bodies into `tinio-ops` is P4's (round 27's rule, applied to mem).
- [x] **Step 5: Run to verify they pass** — `cargo test -p tinio-mem`, including `assert_conformance`.
- [ ] **Step 6: Gate and commit** — ask the operator, then commit.

---

### Task 2: mem content and multipart onto the contracts

**Files:**
- Modify: `crates/tinio-mem/src/storage.rs` (the content tables move behind `RedbContent`), `crates/tinio-mem/src/multipart.rs` (`MultipartOps` bodies through `UploadMeta` + `ObjectContent` for the assembled object and its parts), `crates/tinio-mem/src/version.rs` (its version-row access through `VersionMeta` — the ten `VersionOps` bodies stay as they are for P4), `crates/tinio-store-redb/src/content.rs` if P1's Task 8 left an object-byte shape that only mem needs
- Test: the part-order and accounting tests below

**Interfaces:**
- Consumes: `UploadMeta`, `ObjectContent`, `assert_upload_meta_conformance`, `assert_object_content_conformance`.
- Produces: mem's multipart paths over the contracts; the two conformance legs called from mem's tests.

- [x] **Step 1: Write the failing tests** — `assert_upload_meta_conformance` and `assert_object_content_conformance` from mem's tests (the content legs include the D23 surface: a part key's round-trip, `rename`, `copy`, `scan` with the prefix bound, `stat`, `is_empty` with a directory-only bucket, and the **8 MiB staged leg**), plus a test that a completed multipart object's part rows exist exactly when their bytes are readable (the bytes-first order).
- [x] **Step 2: Run to verify they fail.**
- [x] **Step 3: Migrate** — `create`/`put_part`/`list_parts`/`complete`/`abort`/`consume`/`list_uploads*`/`walk_uploads`/`remove_bucket` through `UploadMeta`; **object and part bytes through the shared `ObjectContent`** behind mem's `CountingContent` (stage → publish → row, with `ContentKey::Part` for a part — spec D23's amendment); and mem's version-row access through the shared `VersionMeta` (`crates/tinio-mem/src/version.rs`), leaving the ten `VersionOps` bodies to P4 (round 29). Only the access layer changes here too.
- [x] **Step 4: Run to verify they pass** — `cargo test -p tinio-mem` and the `@mem` cucumber pass.
- [ ] **Step 5: Gate and commit.**

---

### Task 3: the close-out — seams, docs and the workspace gates

**Files:**
- Modify: `CLAUDE.md` (the two operator-requested rules: Chinese dialogue, plan-progress ticks), `CONTEXT.md` (the glossary gains the four contract terms and the visibility-point rule's statement), `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md` (its "Out of scope" list gains the pointer to this design; its record is not rewritten), `docs/style.md` only if the alias/error rules changed, `docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md` (its Status line becomes "landed" with the date)
- Modify: any dead code the migrations left behind

**Interfaces:**
- Consumes: every earlier task.
- Produces: the closed seam — four contracts, one redb implementation, two consumers, no duplicated metadata logic.

- [ ] **Step 1: The seam invariants** — **[not ticked: the privatization half is deliberately not taken]** `objects`/`part_data`/`part_meta` stay `pub`: mem's versioned single-transaction paths open `objects`, and `crates/tinio-store-redb/tests/lifecycle.rs` opens `part_data` inside the read transaction it pins, so `pub(crate)` would break the versioned atomicity or that test — the seam census in the close-out note is the compensating evidence. `rg -n 'redb::' crates/tinio-core/src` (empty), `rg -n '_store_redb::' crates/tinio-fs/src crates/tinio-mem/src` outside the implementations (enumerate the crate's modules — the prefix `version` misses `bucket_versioning`, and the byte tables are `objects`/`part_data`/`part_meta`), `cargo tree -p tinio-version` (**`tinio-core` and `tinio-blob`** — the block plane is the engine's storage — and no `redb`/`tinio-fs`/`tinio-store-redb`), and **privatize the three byte-table modules** (`objects`/`part_data`/`part_meta` become `pub(crate)` in `tinio-store-redb`, their only remaining consumer being `RedbContent`).
- [x] **Step 2: The format guard** — `cargo test -p tinio-store-redb` (the schema pins **and the committed pre-change fixture test**, P1 Task 4 Step 5 — the `--test layout` target the earlier draft cited no longer exists), and the fs suite green.
- [x] **Step 3: The workspace gates** — `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `cargo hack check` per feature-bearing crate, both cucumber passes.
- [ ] **Step 4: Doc sync and commit** — the four documents above, nothing restated; ask the operator, then commit.

---

## Plan-set ownership

- **P2 (this plan) owns**: the `tinio-mem` migration, its conformance run, the close-out invariants and the doc sync.
- **P1 (`…-metadata-store-contract-p1-contract-and-fs.md`) owns**: the contracts, the `tinio-store-redb` rename and implementations, the harness, the engine re-point and the fs migration.
- **The spec (`…-metadata-store-contract-design.md`) owns**: every decision, the properties and the topology.
- **`2026-09-03-shared-store-table-layer-design.md` owns**: the table set, arities, codecs and the on-disk format contract.

## Notes for the executor

- mem's write order is the contract's, not a mem choice: publish the bytes, then the row. A reversal is Review Focus 5's defect and the tests above are what catch it.
- Do not add a mem-local metadata helper to avoid a contract call: the second consumer is what proves the contract is implementable outside redb's call sites.
- If a method the migration needs is missing, add it to the contract **and** its leg to the shared suite in the same task, then use it from both consumers.

**Carried into P4.** Two contract decisions were raised while closing P1 out and deliberately not decided there. They belong to the ops wave, and are recorded in `plans/2026-10-03-tinio-ops-p4.md` under "Carried into P4".
---

## Executor's close-out note (2026-10-08)

Deviations from the step text, each deliberate:

- **Step 1's privatization is not taken** — `objects`/`part_data`/`part_meta` stay `pub`. The premise ("their only remaining consumer is `RedbContent`") does not hold: mem's versioned write path still opens `objects` in its one transaction (the exception review focus 4 requires), mem's part-stat plane is `part_meta` (no contract carries a part's size/mtime; D13 forbids reading the part whole to fabricate it), and `crates/tinio-store-redb/tests/lifecycle.rs` opens `part_data` inside the same read transaction as the rows it pins. `pub(crate)` breaks the versioned atomicity or that test, so the census below is the compensating evidence.
- **`UploadMeta::create_guarded` is a defaulted method**, not required — `tinio-version`'s `Fake` keeps the default (plain upsert, opt-in per backend); `tinio-fs`'s `Rows` forwards it and is genuinely guarded.
- **Task 2's `assert_object_content_conformance` is `assert_content_conformance`** in the suite; the leg is called, the suite not renamed.
- **Known deviations, commented in code**: (a) a versioned `complete` spans transactions, so a post-`prepare_publish` failure can duplicate a history entry (in-memory — no crash surface); (b) an object publish's transient double-count can refuse a `complete` under a tight `max_total_bytes` that the baseline net delta allowed; (c) closed by `create_guarded`, which re-reads the bucket row in the upload's write transaction; (d) a domain-invalid stored part etag self-heals past `list_parts` and leaves its bytes uncleaned (external tampering only).
- **Open suggestions from the review passes, deliberately not taken**: a `scan::for_each_triple` helper (bucket.rs's `part_data` sweep follows the same boundary rule `part_data::Table::total_len` implements, so it is a duplication of a shape, not a new seek — a helper would be the tidy-up); bundling `object.rs`'s three `#[allow(clippy::too_many_arguments)]` signatures around the payload struct; and `tinio-version`'s fs engine keeping the unguarded `set_bucket_state` (fs serializes the flip with its per-bucket mutation lock, and a guarded call was measured to refuse two legitimate fs states — the reason is commented at the call site).
- **Contract growth beyond the Files list, each sanctioned by the plan's "if a method the migration needs is missing" rule**: `UploadMeta::create_guarded`, `VersionMeta::set_bucket_state_guarded`, `ObjectContent::remove_many`, `ObjectContent::remove_if_matches` (all defaulted, each with a shared-suite leg), and the two `store_handle` accessors (`Driver::store_handle`, `RedbMetadata::store_handle`) that let mem share one database between its metadata and content planes.
- **Scope beyond the Files list**: `CLAUDE.md` gained the two rules the operator asked for (dialogue in Chinese, tick the owning plan's checkbox in the round the step finishes) — an explicit operator instruction, not an executor addition. The 2026-09-03 design record is left as written (the plan says its record is not rewritten), so its `OBJECT_META` 5-/6-tuple paragraph stays a dated decision rather than a description of the landed schema; the correction landed as a dated bullet beside that paragraph in this doc sync (2026-10-08), not as a rewrite.
- **Feature-off dead code**: `MemoryStorage::release_total` and `version::records_versions` carry `#[cfg_attr(not(feature = "versioning"), allow(dead_code))]` — the version paths are their only callers.

Seam census — `_store_redb::` in `crates/tinio-mem/src`, production code: the contract handles (`RedbMetadata`/`RedbContent`), `Error`/`store`/`Driver`, the versioned plane's own rows (`objects`/`object_meta`/`object_part`/`bucket_versioning`, read and written inside its one transaction, in-transaction bucket probe included), the part-stat plane `part_meta`, and the two `Table::ensure` calls at construction. `part_data` no longer appears; the versioned plane's in-transaction probes do reach the store's `bucket` table and its `table` module (the probes at `version.rs`'s `check_bucket_read`/`check_bucket_write` and `object.rs`'s two raw closures), and the part plane reaches `part_meta`. `rg 'redb::' crates/tinio-core/src` is empty and `cargo tree -p tinio-version` shows no `redb`/`tinio-fs`/`tinio-store-redb`.

## Independent audit round (2026-10-08, after the gates)

A read-only audit of the whole diff ran before the close-out was claimed; its findings and their dispositions:

- **Dead reverse conversion** — `impl From<Error> for DatabaseError` existed only to feed a projection closure nothing called, against the constraint that contract errors convert into mem's error and never the reverse. Deleted: `ProjectError`, `project_through`, `project`, the `project` field, the `C::Error: From<Error>` bound and the reverse impl.
- **`has_bucket` read the bucket table directly** — now `BucketMeta::created_at`, so the ordering pre-check is a contract call and mem's raw table set shrinks again.
- **`bucket_records_versions` read `bucket_versioning` directly** — now `VersionMeta::bucket_state` (the same shared table; `tinio-store-redb`'s own alias of it proves the equivalence), six call sites awaited.
- **The version write paths lost their in-transaction bucket check** when `check_bucket` became `ensure_bucket` — restored: an in-transaction probe is the first statement of the nine raw read/write closures in `version.rs`, and `object.rs`'s two versioned raw closures already had theirs. Red evidence: removing the probe makes `a_versioned_write_racing_a_teardown_leaves_nothing_behind` fail with an object row outliving its bucket.
- **The bucket-state write could not be guarded from mem** — the contract gained `VersionMeta::set_bucket_state_guarded` (defaulted, like `create_guarded`), redb's override probes the bucket row in the same write transaction, a shared-suite leg pins the refusal and the absent state row, mem's `set_bucket_versioning` uses it, and fs's `Rows` forwards it. The read half stays unguarded and is documented in both implementations: a racing teardown can answer `Unversioned` instead of `NoSuchBucket`, harmless because the read writes nothing. fs's own flip (`tinio-version`'s engine) keeps the unguarded call — fs serializes it with a per-bucket mutation lock, so it has no such window.
- **`upload_part`'s compensation destroyed the previous part's bytes** — the publish overwrote them and a refused row write then removed the key, leaving a `PARTS` row whose bytes were gone. Now the old bytes are read before the publish and restored on a refusal (or the bytes are dropped when the row is already gone). The same round restored the `part_meta` drain that `complete`/`abort` had lost in the migration, a regression the audit surfaced.
- **Same-key concurrent publishes billed the full length twice** — the delta read and the reservation now share one critical section (`futures::lock::Mutex`, no new dependency), pinned red-then-green by `racing_publishes_to_one_key_bill_only_the_difference`. Residual (closed in the second round): the version paths and `delete_bucket` still moved the total through `MemoryStorage::adjust_total`/`release_total`, outside that lock — the lock now covers them (`lock_accounting` + the `*_unlocked` forms), and `delete_bucket` commits its row work and its release in one transaction.
- **Focus 3 (one transaction per operation)** — verified rather than asserted: no row-batch operation became a per-row loop (mem has no `put_batch`, and `complete`'s row work is one call per contract method). The residual was the byte plane, which the contract exposed one key at a time — `ObjectContent::remove_many` now batches it — plus `delete_bucket`'s `remove_if_empty` + version drain + blob drop, which the second round folded into one transaction by ruling.

The version-path op bodies still adjust the total themselves (`object.rs`'s versioned write/delete, `multipart.rs`'s history bytes): those bytes never pass through the decorator, because the versioned write is the one path that must read the outgoing bytes and write the history row in a single transaction.

## Second independent audit round (2026-10-08, four axes)

A second, adversarial re-review ran against the tree the first round left. Confirmed-true findings and their dispositions:

- **The guarded legs were unusable by any backend that takes the default** — `assert_version_conformance` carried the `set_bucket_state_guarded` leg (and the aggregate the `create_guarded` leg) while both methods stay defaulted, so the out-of-workspace pg spike — which overrides neither — would panic in its own gate. Both legs moved into a new opt-in entry point `assert_guarded_write_conformance` (`S: Metadata + UploadMeta + VersionMeta`), called by redb's `tests/conformance.rs`; the aggregate suites now assert only what a default can satisfy. `assert_version_conformance` also plants the bucket row its slot writer probes: that writer is guarded, and the suite had been driving `advance_slot` against a bucket with no row.
- **Three writers materialized rows for a bucket with no row** — `put_with_parts`, `put_batch` (found by the fixer while auditing its own file) and `advance_slot` now open with the same in-transaction probe `put_guarded` uses; red-then-green pins (`*_refuses_a_bucket_with_no_row`).
- **The unversioned `complete` could commit history bytes off the books** — `prepare_publish` committed its supersede, then a separate `adjust_total` could refuse under a tight limit, leaving a history blob billed as zero (reachable by racing the bucket-state flip). The charge moved into that same transaction (`adjust_total_with_credit_unlocked(history_bytes, 0)` beside `prepare_publish`), so a refusal rolls the supersede back; moving the charge back out turns the new test red.
- **A compensation could delete a later writer's bytes** — `remove` by key cannot tell two publishes apart. The contract gained `ObjectContent::remove_if_matches` (default reads back and compares; redb compares and removes in one commit), the object and part compensations call it, and the shared suite pins match, mismatch and absence.
- **A failed compensation was swallowed and its bytes stayed billed** — `remove_counted`/`remove_many` release the bytes, drop the ledger entry and report before re-raising; the compensation sites log instead of `let _ =`.
- **`delete_bucket` was not atomic** (ruling: mem's own path goes single-transaction) — one raw `db.write` probes the bucket row, probes occupancy, drops the bucket row and drains the version plane, releasing the bytes under the accounting guard. The contract's `remove_if_empty`/`drain_bucket` pair is elided here on purpose and commented as such: it is the one place P2 trades a contract call for atomicity, and the reason is recorded beside it.
- **The version plane's state read sits outside the write transaction** (ruling: re-check after the write) — the unversioned path re-reads `bucket_records_versions` once `put_guarded` returns; when the flip landed it adopts the write through `version::record_current_as_version`, which in one commit mints a fresh identity and sequence onto the object row and advances the slot (no version row: the current version *is* the object row, so an index row would make a listing read it twice). Commented residual: the overwritten predecessor's content is not recoverable, and the window needs a concurrent `PutBucketVersioning`.
- **Accepted residuals, documented rather than fixed** (rulings): `upload_part` keeps its four write transactions, its before-publish read of the previous part (peak ≈ two parts) and its `part_meta`-failure strand (`part_meta` is mem's own stat plane and the bytes stay reachable by key); the underflow clamp in `CountingContent` stays but is now loud (`debug_assert!` plus a report) instead of silent.
- **A third read-only pass on the frozen tree** confirmed fixes 1–7 and found three more things, two fixed and one recorded. Fixed: `put_preserving` and `put_version` materialized rows with no bucket row, and now carry the same in-transaction probe as their siblings (auditor-verified: no other probe-free row materializer remains — `move_`, the tag/ACL writers and `put_part` all read an existing row first, and `put`/`create`/`set_bucket_state` keep their D40 lazy shape on purpose); and `delete_bucket`'s drain now sweeps the byte plane's own rows for the bucket — `objects`, `part_data` (their lengths join `released`) and `part_meta` — with the test asserting every table, `version_index` included. Recorded: `remove_if_matches`'s default answers `false` for a failed `open` **and** for a failed stream read, because it cannot tell a backend's "no such key" error from a real fault without widening every implementation's error; the shared store's override and the decorator's keep the distinction, and the default's doc says so.
- **`delete_bucket`'s metadata drains sat inside the `versioning` cfg** — the six row-plane drains ran under `#[cfg(feature = "versioning")]`, so a build without that feature deleted the bucket row and drained nothing (the dev-dependency self-reference keeps the feature on in tests, so only `cargo check -p tinio-mem --lib` sees it). The drains are unconditional now; only the version plane's own drain is gated.
- **Naming and structure** — the ledger helpers read like state and are renamed (`part_lengths`/`record_part_length`/`part_length`), the `\u{1}`-joined string key became a structured `PartKey` (it carries the bucket dimension `ContentKey::Part` does not), `CountingContent::new` no longer hands out its test-only counter, and the dead `check_object_bytes_locked` is gone.

## Third independent audit round (2026-10-08, the ruling close-outs)

A further four-axis re-verification ran against the frozen tree and put its remaining items to the operator; every ruling landed here.

- **`remove_if_matches`'s equal-body blind spot** — two publishes of byte-identical content (an empty body matching an empty body) let a stale compensation delete a later writer's content, on both the object and the part path. Ruling: the contract carries the publish's identity. Added `content::PublishId` and the defaulted `ObjectContent::publish_tagged`/`remove_if_tagged` — a plane that keeps no identity drops the tag and falls back to the byte compare, documented per method, so fs and the pg spike keep working unchanged. `CountingContent` overrides both: a monotonic, never-reused id per publish plus a process-local per-key tag map, ordered by the existing accounting lock, so the tag check, the byte compare, the removal and the release are one critical section — no schema change, no new table, `FORMAT_VERSION` stays 1. The shared suite pins the live id's round-trip on every plane; mem pins the stale id's refusal **with identical bodies** on both paths (`the_staged_bytes_refund_leaves_a_newer_writers_content_alone`, `a_refused_part_write_leaves_another_writers_bytes_alone`).
- **`upload_part` double-released a part's bytes** — the caller released what the guarded removal's failure arm had already released. The caller's release is deleted (with the now-unused `release_total_locked` wrappers), and `a_failed_guarded_removal_releases_a_parts_bytes_exactly_once` pins the failure arm's single release (the `FlakyRemove` double now gates `remove_if_matches` too). No pin is possible at the caller itself: `MemoryStorage`'s plane is concrete and the failure cannot be injected through it.
- **A failed refund masked the refusal** — `object.rs` reports the compensation's failure and returns the row write's error, as `multipart.rs` already did.
- **Two raw write closures gained the in-transaction bucket probe** every sibling opens with: `record_current_as_version`, and the `part_meta` put (inlined there — `check_bucket_write` lives behind the versioning feature).
- **The window repair neither escalates nor over-claims** — a failed repair is reported and the write stands (a landed `put_object` must not fail for the repair), and a row that already carries an identity answers `None`: a racing versioned writer's id is never reported as this write's own (pinned in `the_window_repair_gives_an_unversioned_row_its_identity`).
- **Observability** — the operator's ruling lands in mem: `tracing` is a dependency now, and every book-keeping report (`report`), compensation failure and refusal refund is a `tracing` event (`warn` for anomalies, `info` for the two expected-race outcomes) instead of `eprintln!`; the test watchdog keeps its `eprintln!`-and-exit.
- **Doc sync corrections** — the 2026-09-03 record gained its dated arity correction (landed 5/5/11: bucket, upload, `object_meta`); P1's forwarding line names 5/5/11 now; the Global Constraint carries the versioned paths' carve-out; Task 3's Files list names `CLAUDE.md`; the earlier audit bullet's stale "mem has no `put_batch`/`remove_many`" clause is corrected; and `assert_version_conformance`'s comment no longer calls its planted bucket row absent.
- **Unchanged by ruling**: `upload_part`'s four transactions, its before-publish read and its `part_meta`-failure strand; the loud underflow clamp; `delete_bucket`'s sum-based release (the clamp plus the report are the accepted guard).

A cleanup pass followed the rulings (2026-10-08), deepening the identity mechanism rather than widening it: **every** publishing path in the decorator records an identity now — plain `publish` (through `publish_locked`), `publish_tagged`, `publish_replacing`, and the `rename`/`copy` targets — so the guard's premise (the key's latest publish) holds for every write through the decorator, and the residual shrinks to a raw write that bypasses it (the versioned plane). A compensated publish retires its tag once its row write lands (`retire_publish_tag`), `release_counted` forgets a key's tag when its bytes leave, and a removal's guard takes the tag it matches (`take_publish_tag`) so one publish is claimed once. The two bucket probes moved out of the versioning gate (`version::check_bucket_read`/`check_bucket_write`; the read half stays feature-gated for its gated callers), the object path's two inline re-spellings call the shared probe, the duplicated roll-back helpers collapsed into `MemoryStorage::roll_back_publish`, and every book-keeping `warn` goes through `counting::report`. Skipped by judgement: the tag registry stays mem-side (no shared `TaggedContent` yet), the map keeps its `(bucket, key)` tuple key (two small clones are noise beside a redb commit), tagged publishes still take the lock without a total limit (redb serializes writers anyway), and `upload_part`'s accepted residuals stand.

The three commit steps (Task 1 Step 6, Task 2 Step 5, Task 3 Step 4) stay unticked: `docs/git.md` — ask the operator first, so the commits await their go. Step 4's doc sync is otherwise landed.
