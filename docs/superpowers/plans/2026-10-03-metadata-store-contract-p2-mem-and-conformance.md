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
- mem's byte limits and its `total_bytes` accounting live in **one** place, the `CountingContent<C>` `ObjectContent` decorator; no op body counts bytes itself (round 29).
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

- [ ] **Step 1: Write the failing tests** — a test that calls `tinio_util::testing::assert_metadata_conformance(storage.metadata())`; one that rejects a `put_object` into a missing bucket and asserts the `MemoryOptions` total is unchanged afterwards; and the decorator's own tests — `max_object_bytes` refused during `stage` (the total untouched), `max_total_bytes` enforced atomically at `publish` (a concurrent publish cannot overshoot the cap), **an overwrite asserting the total after two writes to one key (the delta rule)**, and `remove`/`rename` subtracting the byte count.
- [ ] **Step 2: Run to verify they fail** — `cargo test -p tinio-mem`. Expected: no metadata accessor, no `CountingContent`.
- [ ] **Step 3: Implement `CountingContent<C>`** — stage counts the chunks it accepts and refuses `max_object_bytes`; `publish` reads the key's **current** length inside the same critical section and adds only the **difference** with `AtomicU64::fetch_update` (refusing without writing when the total would overshoot); `remove` and `rename` subtract what is stored; nothing else in mem counts bytes (round 29; the audit's overwrite finding — an add-only rule bills ten 1 MiB overwrites of one key as 10 MiB).
- [ ] **Step 4: Migrate the metadata access** — bucket ops through `BucketMeta` (`delete_bucket` becomes `remove_if_empty` plus the existing not-empty mapping); object ops through `ObjectMeta` with content published first and `put_guarded` last, compensating content removal (through the decorator, so the accounting follows) on rejection; delete `check_bucket`/`check_upload`'s table-level helpers in favour of the contract's guarded forms. **Only the metadata-access layer changes**: function boundaries, branches and error variants stay as they are — moving the operation bodies into `tinio-ops` is P4's (round 27's rule, applied to mem).
- [ ] **Step 5: Run to verify they pass** — `cargo test -p tinio-mem`, including `assert_conformance`.
- [ ] **Step 6: Gate and commit** — ask the operator, then commit.

---

### Task 2: mem content and multipart onto the contracts

**Files:**
- Modify: `crates/tinio-mem/src/storage.rs` (the content tables move behind `RedbContent`), `crates/tinio-mem/src/multipart.rs` (`MultipartOps` bodies through `UploadMeta` + `ObjectContent` for the assembled object and its parts), `crates/tinio-mem/src/version.rs` (its version-row access through `VersionMeta` — the ten `VersionOps` bodies stay as they are for P4), `crates/tinio-store-redb/src/content.rs` if P1's Task 8 left an object-byte shape that only mem needs
- Test: the part-order and accounting tests below

**Interfaces:**
- Consumes: `UploadMeta`, `ObjectContent`, `assert_upload_meta_conformance`, `assert_object_content_conformance`.
- Produces: mem's multipart paths over the contracts; the two conformance legs called from mem's tests.

- [ ] **Step 1: Write the failing tests** — `assert_upload_meta_conformance` and `assert_object_content_conformance` from mem's tests (the content legs include the D23 surface: a part key's round-trip, `rename`, `copy`, `scan` with the prefix bound, `stat`, `is_empty` with a directory-only bucket, and the **8 MiB staged leg**), plus a test that a completed multipart object's part rows exist exactly when their bytes are readable (the bytes-first order).
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Migrate** — `create`/`put_part`/`list_parts`/`complete`/`abort`/`consume`/`list_uploads*`/`walk_uploads`/`remove_bucket` through `UploadMeta`; **object and part bytes through the shared `ObjectContent`** behind mem's `CountingContent` (stage → publish → row, with `ContentKey::Part` for a part — spec D23's amendment); and mem's version-row access through the shared `VersionMeta` (`crates/tinio-mem/src/version.rs`), leaving the ten `VersionOps` bodies to P4 (round 29). Only the access layer changes here too.
- [ ] **Step 4: Run to verify they pass** — `cargo test -p tinio-mem` and the `@mem` cucumber pass.
- [ ] **Step 5: Gate and commit.**

---

### Task 3: the close-out — seams, docs and the workspace gates

**Files:**
- Modify: `CONTEXT.md` (the glossary gains the four contract terms and the visibility-point rule's statement), `docs/superpowers/specs/2026-09-03-shared-store-table-layer-design.md` (its "Out of scope" list gains the pointer to this design; its record is not rewritten), `docs/style.md` only if the alias/error rules changed, `docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md` (its Status line becomes "landed" with the date)
- Modify: any dead code the migrations left behind

**Interfaces:**
- Consumes: every earlier task.
- Produces: the closed seam — four contracts, one redb implementation, two consumers, no duplicated metadata logic.

- [ ] **Step 1: The seam invariants** — `rg -n 'redb::' crates/tinio-core/src` (empty), `rg -n '_store_redb::' crates/tinio-fs/src crates/tinio-mem/src` outside the implementations (enumerate the crate's modules — the prefix `version` misses `bucket_versioning`, and the byte tables are `objects`/`part_data`/`part_meta`), `cargo tree -p tinio-version` (**`tinio-core` and `tinio-blob`** — the block plane is the engine's storage — and no `redb`/`tinio-fs`/`tinio-store-redb`), and **privatize the three byte-table modules** (`objects`/`part_data`/`part_meta` become `pub(crate)` in `tinio-store-redb`, their only remaining consumer being `RedbContent`).
- [ ] **Step 2: The format guard** — `cargo test -p tinio-store-redb` (the schema pins **and the committed pre-change fixture test**, P1 Task 4 Step 5 — the `--test layout` target the earlier draft cited no longer exists), and the fs suite green.
- [ ] **Step 3: The workspace gates** — `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `cargo hack check` per feature-bearing crate, both cucumber passes.
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
