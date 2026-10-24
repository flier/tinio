# S3 Object Lock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The AWS Object Lock surface on tinio's version model: bucket configuration and default retention, per-version GOVERNANCE/COMPLIANCE retention and legal holds, write-time lock headers, the governance bypass, delete protection inherited by S3, bulk delete, lifecycle and the sweeper, the versioning-state rules, the S3 wire and error shapes, and an mc-aligned local CLI over a management plane — proven per backend by the conformance harness and end to end by cucumber, the CLI and boto3.

**Architecture:** The model, validation and pure rules live in `tinio-core::object_lock`. The bucket config, per-version lock and multipart intent live in three additive tables in the shared redb layer (fs and mem) and in pg's `0001_init.sql`; no version-row or bucket-row format changes. The lock is committed through `VersionMeta::advance_slot_with` and enforced in `expire_with` **and in each backend's current-version delete arm** (fs `version_adapter`/`backend/version.rs`, mem `drop_current`, pg ops). The write path resolves the effective lock in the backend where `BucketMeta` is in hand and passes it through the actual seams (`commit_object`, `copy_object`, `publish_versioned`, `write_object_recording_versions`, `create_multipart_upload`). `tinio-server` maps the six S3 operations; `tinio-auth` resolves the addressed version's authority; `tinio-lifecycle` counts a skip; `tinio-api` carries the two plane routes; `tinio-cli` carries the verbs and the `MetricsStorage` forwards; `tinio-util` owns the conformance legs and the metric.

**Tech Stack:** Rust edition 2024, `s3s` 0.17 DTO/XML, `serde`/`serde_json`, `time`, `redb`, `sqlx`, `prometheus`, `axum`/`utoipa`, cucumber.

**Spec:** `docs/superpowers/specs/2026-10-20-s3-object-lock-design.md` — OL1–OL31 are binding; where this plan and the spec disagree, the spec wins and the plan is corrected, never the code. Citations: `docs/superpowers/object-lock-prior-art-research.md`.

## Global Constraints

### Binding rules

- Decisions: OL1–OL31 bind. 2026-10-20 rulings in the spec **Status** line; audit fixes: OL2/OL4/OL5/OL7/OL8/OL15/OL21/OL23/OL27/OL28/OL31.
- No new crate.
- Additive contract: `expire_with`, `expire_many_report`, `advance_slot_with`, `set_retention`, `set_legal_hold`.
- Additive contract: `get_object_retention`, `get_object_legal_hold`, `create_with_lock`; old methods delegate.
- New methods are required, no default body; never default a security read to "unlocked".
- `set_version_lock` compiles only under `cfg(any(test, feature = "testing"))`.

### Delivery and environment

- Delivery: one plan, dependency order; each task compiles, named suites green. Agents never `git add`/commit; one operator commit at end.
- Tests first: failing test, smallest impl, `cargo test -p <crate> --features object-lock` (gated code needs it), clippy, fmt.
- PG: `TINIO_PG_DSN`, `--test-threads=4`; `query!` for one-liners, `query_file!`/`sql/<group>/<name>.sql` for longer ones.
- PG: `cargo xtask sqlx update --dsn …` and `cargo xtask sqlx check` clean.
- PG: `0001_init.sql` edited; `SCHEMA_VERSION` 1; no old-database migration — new databases only (operator ruling).
- Features: Task 6 `tinio-server`, Task 9 `tinio-api`, Task 10 `tinio-cli`; Task 11 adds `tinio`'s `full` and the config key.
- Features: matrix `cargo hack check --package tinio-server --each-feature --skip otel`; `tinio-core` and the stores are not gated.

### Wires and docs

- Wires: S3 XML; plane JSON = the stored model; CLI `--json` = `mc`'s field names. No fourth spelling.
- Docs prose: English, no hard wraps. Records: tick `- [ ]` and append one `Evidence` line per task.

## Review Focus

### Contract and stores

1a. **A contract implementor is missed.** In-tree stores: `RedbMetadata`, `PgStore`, fs `version_adapter::Rows`.
1b. **Implementors (cont.).** Also `tinio-version::Fake`, `tinio-ops::TestMeta`, two `MetricsStorage` wrappers (`write_metrics.rs`).
1c. **Implementor update.** Required methods; Task 2 updates all with `--all-targets` and the feature sets.
2. **Current delete bypasses `expire`.** fs current/marker arms, mem `drop_current` check/remove lock row; a leg covers current by id/null.
3. **`advance_slot_with(None)` must never clear;** only removals clear. A null in-place replacement of a locked identity is refused.

### Plane and defaults

4. **Per-object writes need config.** `set_retention`/`set_legal_hold` check config/versioning atomically; locked-null suspension closed.
5. **Plane must not disable or collapse current/null.** `enabled:false` refused; address explicit; retention set/clear/leave ≠ legal-hold.
6. **The addressed version's ACL is ignored.** `may_bypass` uses `version_acl` for version-addressed requests; ACL-off/no identity is false.
7. **Security defaults fail open.** Lock reads/writes are required (no default body); corrupt/partial rows fail closed.

### Surfaces and cache

8. **Read-only over a pre-feature root fails.** The three redb tables are `MAY_BE_ABSENT`.
9. **The metric registers twice.** One family, one site in `tinio-util::metrics`.
10. **Gated code must compile.** Every gated task runs `--features object-lock`; a stripped build has a runtime refusal test.

### Ownership and delivery

11a. **Ownership — corpus and metadata.** Task 0 corpus; Task 3 redb; Task 4 pg.
11b. **Ownership — write paths.** Task 5a write seams; Task 5b retention/delete/multipart.
11c. **Ownership — surface.** Task 6 server; Task 7 auth; Task 8 lifecycle/metric.
11d. **Ownership — remaining.** Task 8b doctor/repair; Task 9 plane; Task 10 commands; Task 11 facade.
11e. **Shared edits.** A shared file edit names the function it owns.
12. **The GET/HEAD cache goes stale.** `put_bucket_object_lock` and bucket delete invalidate it; a stale `false` must not hide a new config.
13. **fs create residue is completed.** Startup repair/doctor completes or clears a partial lock-enabled bucket; the test plants one.

---

### Task 0: The corpus rows

#### Files
- Modify: `specs/001-s3-local-server/spec.md` (FR-043 after FR-042; amend FR-042/cli.md for the second FR-007 exception)
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (new `## Object Lock`)
- Modify: `specs/001-s3-local-server/contracts/config.md` (`[s3] object_lock`)
- Modify: `specs/001-s3-local-server/contracts/management-api.md` (the two plane routes and their status mapping)

#### Files (continued)
- Modify: `specs/001-s3-local-server/contracts/cli.md` (the three groups; FR-007's second exception)
- Modify: `specs/001-s3-local-server/data-model.md` (three tables + the metric)
- Modify: `specs/001-s3-local-server/tasks.md` (T176–T189 mapping)
- Modify: `crates/tinio-e2e/tests/traceability.rs` (temporary allow-list entry with its `Home::Task("T…")` ID)

#### Interfaces
- Consumes: OL1, OL17, OL22–OL25, OL30.
- Produces: the normative wire/status/verb lists Tasks 6, 9, 10 implement; the allow-list entry Task 12 removes.

#### Steps
- [x] **Step 1: s3-surface section** — six operations/wires.
- Step 1 wires: `CreateBucket` header; three write headers; delete/retention bypass; three GET/HEAD headers.
- Step 1 rules: statuses 403/404/409/400; marker rules; versioning rules; checksum; Complete refusal.
- [x] **Step 2: config, management-api, CLI and data-model sections** — `[s3] object_lock` default true.
- Step 2 sections: two routes + distinguishing 404 body; three groups + FR-007's second exception.
- Step 2 corpus: three tables, metric, pg new-DB note.

#### Steps (continued)
- [x] **Step 3: Cite FR-043 and pin T176–T189 + the allow-list `Home::Task` ID.**
- [x] **Step 4: Verify** — `cargo test -p tinio-e2e --test traceability`.
- [x] **Step 5: Evidence.**

#### Evidence
`cargo test -p tinio-e2e --test traceability` → `test result: ok. 20 passed; 0 failed` (2026-10-20); allow-list entry `Home::Task("T188")`; review clean after fix round 1 (FR-043 reduced to requirement-level pointers, `[s3] object_lock` versioning-state wording corrected, R13 `CreateBucket`-with-capability-off ruling written into `contracts/config.md`).

### Task 1: The model, the wire and the pure rules

#### Files
- Create: `crates/tinio-core/src/object_lock.rs`
- Modify: `crates/tinio-core/src/lib.rs`
- Modify: `crates/tinio-core/Cargo.toml` only if a dependency edge is needed

#### Interfaces
- Consumes: OL1, OL4, OL10–OL13, OL17.
- Produces: `object_lock::{Config, Rule, DefaultRetention, Period, Mode}`.
- Produces: `object_lock::{Retention, LegalHold, Lock, Bypass, Refusal, RefusalReason}`.
- Produces: `Config::validate`, `Config::rule_wire`/`from_rule_wire` (empty = none), `Retention::validate`, `LegalHold::parse`.
- Produces: `write_lock`, `apply_retention`, `may_delete`, `default_retention_at`.
- `Bypass` is a newtype; any invalid element or partial pair is a hard `Refusal`.
- A `Years` default is calendar years with Feb-29 → Feb-28.

#### Steps
- [x] **Step 1: Validation** — `Enabled` only; mode + 1 period; bounds; past date; legal hold; empty rule; hold default; override; Feb-29.
- [x] **Step 2: Protection matrix failing** — governance extend/shorten ± bypass; compliance; legal hold; expiry; `RefusalReason`.
- [x] **Step 3: Implement the types and pure functions; the rule wire is empty for `None`.**
- [x] **Step 4: Gate** — `cargo test -p tinio-core object_lock`, clippy, fmt; **Evidence**.

#### Evidence
`cargo test -p tinio-core object_lock` → 42 unit tests + 22 doctests pass; `cargo clippy -p tinio-core --all-targets -- -D warnings` clean; `cargo +nightly fmt -p tinio-core` clean (2026-10-20). Review: spec ✅, no Critical/Important; five deferred minors ledgered.

### Task 2: The contract and the conformance legs

#### Files
- Modify: `crates/tinio-core/src/storage/error.rs` (`Locked(RefusalReason)`, `InvalidBucketState`, the invariant/corrupt class)
- Modify: `crates/tinio-core/src/meta/bucket.rs` (`object_lock`, `set_object_lock`, `clear_object_lock`)
- Modify: `crates/tinio-core/src/meta/version.rs` (`version_lock`, `get_object_retention`).
- Modify: `crates/tinio-core/src/meta/version.rs` (`get_object_legal_hold`, `set_retention`, `set_legal_hold`).
- Modify: `crates/tinio-core/src/meta/version.rs` (`expire_with`, `expire_many_report`, `advance_slot_with`).
- Modify: `crates/tinio-core/src/meta/version.rs` (old methods delegate; `set_version_lock` test-only).
- Modify: `crates/tinio-core/src/meta/upload.rs` (`upload_lock`, `create_with_lock`/`create_guarded_with_lock`, `clear_upload_lock`)

#### Files (continued)
- Modify: `crates/tinio-core/src/storage/{bucket,object,multipart,version}.rs` (the `BucketOps`/`VersionOps` surface).
- Add to the storage surface: `get_object_retention`/`get_object_legal_hold` and `create_bucket_with_lock`.
- Modify: `crates/tinio-util/src/testing/metadata.rs` (the shared legs; corrupt rows stay per-store)
- Modify: `crates/tinio-version/src/testutil.rs` (`Fake`), `crates/tinio-ops/src/testutil.rs` (`TestMeta`).
- Modify: `crates/tinio-fs/src/version_adapter.rs` (the adapter).
- Modify: `crates/tinio-cli/src/commands/start/write_metrics.rs` (both wrappers).
- Modify: `crates/tinio-core/src/lib.rs` re-exports

#### Interfaces
- Consumes: OL2, OL4–OL8, OL21, OL27, OL28.
- Produces: the additive contract; `storage::Error::{Locked, InvalidBucketState, …}`.
- Produces: `ExpireReport { removed, protected: Vec<(u64, RefusalReason)> }`; the read methods the server/plane use.
- Produces: the `MetricsStorage` forwards; the conformance entry points Tasks 3/4 call.
- The new methods are **required, no default body**; `set_version_lock` is `cfg(any(test, feature = "testing"))`.
- `set_retention`/`set_legal_hold` assert the identity, bucket config and versioning; `create_with_lock` is atomic.
- Old `expire`/`expire_many`/`advance_slot` delegate (no bypass/discard/no lock) and never clear.

#### Steps
- [x] **Step 1: Write the failing shared legs** — bucket config, current/history/null lock.
- Step 1 legs: current-delete-by-id/null; markers; protected delete; bypass; compliance; expiry.
- Step 1 contract: `expire_with`, `expire_many_report`; drain (protected refusal + orphan removal).
- Step 1 intent: upload intent; read methods.
- [x] **Step 2: Add required methods (no default body); update all implementors and decorators; build `--all-targets` and feature sets.**
- [x] **Step 3: Gate** — `cargo test --workspace --all-targets` (contract ungated; gated crates in 6/9/10/11/12), clippy, fmt; **Evidence**.

#### Evidence
`cargo test --workspace --all-targets` → 66 suites green, no failures (pg legs skip without `TINIO_PG_DSN`); `cargo check -p tinio-store-redb --features testing`, `-p tinio-store-pg --features testing`, `-p tinio-fs --features versioning,testing` pass; `cargo tree -p tinio-fs -e features -i tinio-store-redb` shows redb/testing; clippy `-D warnings` and `cargo +nightly fmt --all -- --check` clean (2026-10-21). Review clean after fix round 1 (store `testing` features forward `tinio-core/testing`; the `From<storage::Error>` bound is method-local). R18 waiver recorded on `set_version_lock` (fail-closed default body, test-only).

### Task 3: The redb tables and the atomic primitives

#### Files
- Create: `crates/tinio-store-redb/src/bucket_object_lock.rs`, `version_object_lock.rs`, `upload_object_lock.rs`
- Modify: `crates/tinio-store-redb/src/lib.rs` (table list/pins), `table.rs` (`MAY_BE_ABSENT` for the three)
- Modify: `crates/tinio-store-redb/src/meta/{bucket,version,upload}.rs` (the contract impls)
- Modify: `crates/tinio-store-redb/src/version.rs`, `object_meta.rs` (slot/expire transactions)
- Modify: `crates/tinio-fs/src/version_adapter.rs`, `crates/tinio-fs/src/backend/version.rs` (adapter forwards; current/marker delete check)
- Modify: `crates/tinio-mem/src/version.rs` (drop_current check)
- Modify: `crates/tinio-store-redb/tests/conformance.rs`

#### Interfaces
- Consumes: OL4, OL5, OL7, OL8, OL21.
- Produces: the tables; `MAY_BE_ABSENT`; read-only pre-feature test; crash-ordering tests.
- Produces: `advance_slot_with` writes only on `Some` and never clears.
- Produces: `expire_with` resolves the slot when no row exists and removes row/index/lock.
- Produces: `expire_many_report` skips and reports; `set_retention`/`set_legal_hold` atomic.
- Produces: current/marker delete checks; bucket-writer clears.
- Produces: `drain_bucket` refuses a protected version and removes orphan locks.

#### Steps
- [x] **Step 1: Write the table pins and conformance callers failing.**
- [x] **Step 2: Implement tables/transactions** — no advance clear; current-slot id; drain refuses protected, drops orphans; writers clear.
- [x] **Step 3: crash/read-only/current-delete/corrupt-row/drain** — corrupt legs use raw access; drain refuses protected, drops orphans.
- [x] **Step 4: Gate** — `tinio-store-redb`, `tinio-fs`, `tinio-mem` (the stores are ungated), clippy, fmt; **Evidence**.

#### Evidence
redb 169+4+1+2+28, fs 390+18, mem 106+4, tinio-version 26, tinio-util 58+36 (2 ignored) pass; per-crate clippy `-D warnings` and `cargo +nightly fmt --all -- --check` clean; workspace check clean (2026-10-21). Three tables `MAY_BE_ABSENT`, `ensure_all` at fifteen, `FORMAT_VERSION` 1; the fs adapter and redb conformance callers green. Review clean after fix round 1 (the drain now resolves a lock against the current slot, and mem's teardown probes include version/lock rows).

### Task 4: The pg tables and the transactions

#### Files
- Modify: `crates/tinio-store-pg/sql/migrations/0001_init.sql` (three tables; `REFERENCES buckets(name) ON DELETE CASCADE`)
- Modify: `crates/tinio-store-pg/src/meta/{bucket,version,upload}.rs`, `sql/meta/...` (the statements; `query_file!` for multi-line)
- Modify: `crates/tinio-store-pg/src/teardown.rs`, `store.rs`/`migrations.rs` only if the schema gate/version needs a note
- Modify: `crates/tinio-store-pg/src/tests/conformance.rs`
- Modify: `crates/tinio-pg/src/{storage,version,cleanup}.rs` (backend forwards)
- Regenerate: `.sqlx`

#### Interfaces
- Consumes: OL4, OL5, OL7, OL8, OL21, OL26.
- Produces: the SQL contract.
- Produces: `expire_with`, `set_retention`, `set_legal_hold` lock the version row / `current_slots` row `FOR UPDATE`.
- Produces: the versioning/lock cross-check first `SELECT … FOR UPDATE` the bucket row; the bucket FK cascades.
- Produces: `expire_many_report` is one set-based transaction; **no old-database migration** — new databases only.
- `SCHEMA_VERSION` stays 1; `cargo xtask sqlx update --dsn …`.

#### Steps
- [x] **Step 1: Write the DDL and failing conformance caller.**
- [x] **Step 2: Implement statements/transactions and the common-row lock; add corrupt-row/drain legs (raw access is this store's).**
- [x] **Step 3: Regenerate the cache and run the pg legs.**
- [x] **Step 4: Gate** — `TINIO_PG_DSN=… cargo test -p tinio-store-pg -p tinio-pg -- --test-threads=4`, clippy, fmt; **Evidence**.

#### Evidence
`TINIO_PG_DSN=postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable cargo test -p tinio-store-pg -p tinio-pg --features tinio-pg/versioning -- --test-threads=4` → 20+6+6+2+2+4+1+2+197+10+39 passed, 0 failed; `cargo xtask sqlx check` → 185 entries up to date; clippy `-D warnings`, sqruff and fmt clean (2026-10-21). The base dev database was recreated (no old-DB migration). Review clean after fix round 1 (bucket-first lock order across the delete family; R21 guarded versioning write refuses `Suspended` over an enabled config on pg and redb; teardown docs corrected).

### Task 5a: The write seams and the resolved lock

#### Files
- Modify: `crates/tinio-ops/src/bucket.rs` (`get/put_bucket_object_lock`, `create_bucket_with_lock`)
- Modify: `crates/tinio-ops/src/object.rs` (`commit_object`/`copy_object` lock parameter)
- Modify: `crates/tinio-ops/src/version.rs` (`put_object_versioned` lock parameter)
- Modify: `crates/tinio-core/src/storage/{bucket,object,version}.rs` (the declarations)
- Modify: `crates/tinio-fs/src/backend/{objects,version}.rs` (resolve the lock where `BucketMeta` is in hand).
- Modify: the fs `publish_versioned` fast path, `crates/tinio-mem/src/{object,version}.rs`, `crates/tinio-pg/src/{storage,version}.rs`.
- Modify: `crates/tinio-cli/src/commands/start/write_metrics.rs` (forwards for the write methods)

#### Interfaces
- Consumes: OL1, OL6–OL8, OL10, OL12, OL13, OL16.
- Produces: the bucket config bodies; `create_bucket_with_lock`; `advance_slot_with`.
- Produces: the effective lock resolved once in the backend and passed into the actual seams.
- Produces: the checksum decision uses the same lock; the decorator forwards.
- `create_bucket_with_lock` default compensates; redb/pg override atomically.
- fs: three commits + compensation; Task 8b repair completes a crash residue.

#### Steps
- [x] **Step 1: Engine tests** — default/explicit; hold-only default; copy no inherit; lock rides commit; atomic create; checksum agrees.
- [x] **Step 2: Implement the bodies and the seams.**
- [x] **Step 3: Gate** — `tinio-ops`, fs, mem, pg and `tinio-cli` (the contract and the forwards are ungated); clippy, fmt; **Evidence**.

#### Evidence
ops 396, fs 111+79+18+4+9, mem 112, pg DSN suites all pass; `cargo check -p tinio-cli` and `cargo check --workspace --all-targets` clean; feature-off and feature-on clippy `-D warnings` clean; nightly fmt clean; WSL `cargo test -p tinio-fs --features versioning,testing` → 423 passed + 18 doctests (2026-10-21). Review clean after three fix rounds: the unix fast-copy refusal moved before the rename (destination content/ETag proven unchanged), mem's atomic redb create, and the feature-off import gates. The effective lock resolves once in each backend and rides every write seam; CopyObject never inherits.

### Task 5b: Retention, legal hold, multipart and the delete path

#### Files
- Modify: `crates/tinio-ops/src/version.rs` (`set_retention`/`set_legal_hold` bodies; `delete_object_version` forwards `Bypass`)
- Modify: `crates/tinio-ops/src/multipart.rs` (`create_multipart_upload` calls `create_with_lock`; `complete` applies; abort/expiry clears)
- Modify: `crates/tinio-fs/src/backend/{multipart,version}.rs`, `crates/tinio-mem/src/{multipart,version}.rs`.
- Modify: `crates/tinio-pg/src/{storage,version}.rs` (the retention/delete/multipart forwards).

#### Interfaces
- Consumes: OL5–OL9, OL19, OL27.
- Produces: retention/legal-hold bodies over atomic store methods; multipart intent.
- Produces: delete bypass; fs/mem current/marker enforcement; backend tests.
- `set_retention`/`set_legal_hold` preserve each other; a marker is 405; an unconfigured bucket is 400; a compliance shortening is refused.

#### Steps
- [x] **Step 1: Write the failing engine tests** — multipart default fixed at create; abort clears.
- Step 1 retention: retention/legal-hold preserve; protected delete; governance bypass.
- Step 1 refusals: compliance refusal; current-version delete by id/null on fs/mem.
- [x] **Step 2: Implement the bodies and forwards.**
- [x] **Step 3: Gate** — fs, mem, pg and `tinio-ops` (ungated); clippy, fmt; **Evidence**.

#### Evidence
fs 404, mem 117, ops 84 lib tests (18/4/9 doctests), pg DSN suites 20+6+6+2+2+4+6+2 + 10 doctests, WSL fs 431 + 18 doctests pass; workspace check, feature-off/on clippy `-D warnings`, nightly fmt clean (2026-10-21). `VersionOps::delete_object_version` carries `Bypass` (R20) through every caller; the multipart intent is written at create, read from the upload row at completion on fs/mem/pg, and cleared by the drains; retention/legal-hold preserve each other and compliance refuses even with the bypass. Review clean after fix round 1 (the fs current-delete pre-check judges the recorded identities before the unlink under the mutation lock, and fs routes versioning through the guarded write).

### Task 6: The S3 surface

#### Files
- Create: `crates/tinio-server/src/backend/object_lock.rs`; add `mod object_lock;` in `crates/tinio-server/src/backend/mod.rs`
- Modify: `crates/tinio-server/src/backend/mod.rs` (the `S3Backend` `has_lock_config` cache field and its eviction)
- Modify: `crates/tinio-server/src/backend/ops.rs` (six rows), `s3.rs` (dispatch).
- Modify: `crates/tinio-server/src/s3.rs` (`object_lock`, `DEFAULT_OBJECT_LOCK`), `crates/tinio-server/Cargo.toml` (`object-lock`).

#### Files (continued)
- Modify: `crates/tinio-server/src/backend/objects.rs` (write headers, checksum once, GET/HEAD echo keyed by wire).
- Modify: `crates/tinio-server/src/backend/objects.rs` (cache lookup, delete bypass).
- Modify: `multipart.rs` (create intent, Complete refusal), `buckets.rs` (header, cache eviction).
- Modify: `versioning.rs` (409), `errors.rs` (Locked/409/404).
- Modify: `crates/tinio-server/src/{capability.rs,readonly.rs,metrics.rs}` only through the table

#### Interfaces
- Consumes: OL1–OL3, OL9, OL11, OL13–OL18, OL20, OL29.
- Produces: six handlers; capability; `CreateBucket` s3s parsing.
- Produces: write headers + checksum once; GET/HEAD echo including null; bypass; Complete refusal; 409.
- Produces: `PostObject` input checksum; `Address` resolution; `MetricsStorage` reads.
- Produces: a process-local `has_lock_config` cache in `S3Backend`, consulted before the version-lock read.
- Produces: the cache is invalidated by `op_put_bucket_object_lock`/`op_create_bucket`/`op_delete_bucket`.
- Error mapping: 403/404/409/400 as OL17; the marker mix as OL9.

#### Steps
- [x] **Step 1: Write the handler tests failing** — six round trips; header; suspension.
- Step 1 writes: write headers; checksum; protected 403; bulk per-key.
- Step 1 echo/gates: echo/null; Complete refusal; capability off; marker statuses.
- Step 1 cache: cache invalidation (a no-config bucket skips the lock table; enabling flips it).
- [x] **Step 2: Implement the handlers/rows; wire the feature in the gate.**
- [x] **Step 3: Gate** — `cargo test -p tinio-server --features object-lock --all-targets`, both feature sets; clippy, fmt; **Evidence**.

#### Evidence
`cargo test -p tinio-server --features object-lock --all-targets` → lib 341 (29 object-lock) + integration suites; feature-off → lib 312 + integration; both clippy sets `-D warnings`, nightly fmt and `cargo check --workspace --all-targets` clean (2026-10-21). Six gated operations, the CreateBucket header (R13), one resolved lock feeding the checksum gate and all three write seams, GET/HEAD echo keyed by the identity wire behind the generation-checked `has_lock_config` cache, the fail-closed delete bypass, the Complete refusal, and R22's `VersionOps` writes with fs/mem/pg/`MetricsStorage` forwards. Review clean after two fix rounds (the cache cannot hide a new config nor resurrect an evicted bucket; the validation and capability-off enforcement-stays matrices are pinned).

### Task 7: The auth authority

#### Files
- Modify: `crates/tinio-auth/src/matrix.rs` (six rows), `access.rs` (`may_bypass` from the addressed version's ACL; ACL-off false)
- Modify: `crates/tinio-server/src/backend/{objects.rs,object_lock.rs}` (read the extension)
- Modify: `crates/tinio-auth/Cargo.toml` only if needed

#### Interfaces
- Consumes: OL15, OL29.
- Produces: the matrix rows; `version_acl` for addressed requests; current-object ACL only for unaddressed.
- Produces: ACL-off/no identity false; the extension bit.
- Produces: the handler passes `Bypass::Governance` only when header && may_bypass.

#### Steps
- [x] **Step 1: Write the failing auth tests** — addressed/current owners; non-owner; ACL-off; compliance; the six base requirements.
- [x] **Step 2: Implement the resolution.**
- [x] **Step 3: Gate** — `tinio-auth`, `tinio-server --features object-lock`; clippy, fmt; **Evidence**.

#### Evidence
auth 66 unit + 4 doctests, server object-lock 350 (38 object-lock, 9 new) and feature-off 312 pass; clippy both feature sets, nightly fmt and the stripped object-lock build clean (2026-10-21). The six matrix rows are `OwnerOnly` for the configuration operations and `ObjectOrBucketOwner` for the per-version operations; `may_bypass` resolves `version_acl` for addressed requests (the current ACL only unaddressed), bucket-owner parity, FullControl, ACL-off/no identity false; the extension bit is inserted only by the access layer and the header is still required; both delete paths (per-entry bulk) and `PutObjectRetention` compute it; compliance refuses with the bit. Review clean after fix round 1 (the config rows' owner-only authority was the escalation). R23: the base operation gate remains current-object based, parked for the final review.

### Task 8: Lifecycle, sweeper and the metric

#### Files
- Modify: `crates/tinio-lifecycle/src/runner.rs` (Locked = skip; count)
- Modify: `crates/tinio-fs/src/sweep.rs`, `crates/tinio-pg/src/cleanup.rs` (`expire_many_report`)
- Modify: `crates/tinio-util/src/metrics.rs` (the one family/helper)
- Modify: the S3/lifecycle/sweeper call sites for the counter

#### Interfaces
- Consumes: OL19, OL25.
- Produces: `tinio_object_lock_protected_deletions_total{operation, reason}`; one registration; lifecycle summary count; sweeper report.

#### Steps
- [x] **Step 1: Write the failing metric/lifecycle tests.**
- [x] **Step 2: Implement the helper and call sites.**
- [x] **Step 3: Gate** — `tinio-lifecycle`, `tinio-fs`, `tinio-pg` ungated, `tinio-server --features object-lock`; clippy/fmt; **Evidence**.

#### Evidence
lifecycle+fs+pg gate and the pg DSN suite pass (405/11/22/20/7/2/2/4/6/2 + doctests; pg 20/7/6/2/2/4/6/2 + 10 doctests); server `--features object-lock --all-targets` 352+2+1+3; clippy `-D warnings`, nightly fmt and workspace check clean (2026-10-21). One registered `tinio_object_lock_protected_deletions_total{operation, reason}` family with one producer taking `object_lock::RefusalReason` directly (R24, no `LockReason`); lifecycle counts a protected noncurrent version as a skip; fs/pg sweepers consume `expire_many_report` and record each protected reason; the two S3 delete sites record `delete_object`/`delete_objects`; retention records nothing. Review: spec ✅, no Critical/Important; five deferred minors.

### Task 8b: doctor and the fs startup repair

#### Files
- Modify: `crates/tinio-cli/src/commands/doctor/mod.rs` (the lock report)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (the startup repair; the state/open path)
- Modify: the doctor report fixtures/tests

#### Interfaces
- Consumes: OL31.
- Produces: `tinio doctor` reports a corrupt bucket rule, a corrupt per-version mode/retain-until/partial pair and an orphan lock row.
- Produces: doctor is dry-run-aware and **report-only** (no repair).
- Produces: the fs startup repair completes or clears a partial lock-enabled bucket (bucket row + versioning + config).
- Produces: redb/pg create atomically.
- The doctor check is report-only; the startup repair is the only writer.

#### Steps
- [x] **Step 1: Doctor/repair tests** — corrupt rule, corrupt mode/partial pair, orphan lock row, partial fs bucket from a simulated crash.
- [x] **Step 2: Implement the report and the startup repair.**
- [x] **Step 3: Gate** — `tinio-cli`, `tinio-fs` (with the feature where gated), clippy, fmt; **Evidence**.

#### Evidence
cli 234+1+111, fs 408+18 (Windows) and 435+18 (WSL), store-redb 170+30 doctests pass; feature-off and versioning fs clippy `-D warnings`, workspace check and nightly fmt clean (2026-10-21). `tinio doctor` reports the corrupt bucket rule (enabled bit survives), the corrupt per-version mode/date/partial rows, a corrupt upload intent and orphan lock rows through a read-only `lock_inspect::scan`, dry-run-aware and report-only on every leg; the fs writable `new` completes the R25 partial shape by enabling versioning once, idempotently, while `new_from_db` (doctor) and read-only opens never repair. Review clean after two fix rounds (report completeness on every store arm; the provenance docs corrected across all three fs sites).

### Task 9: The management plane

#### Files
- Create: `crates/tinio-api/src/object_lock.rs`
- Modify: `crates/tinio-api/src/{lib.rs,router.rs,state.rs,openapi.rs,client.rs,error.rs}`, `Cargo.toml` (`object-lock`)
- Modify: `crates/tinio-cli/src/commands/start/run.rs` (the admin impl/injection; only when the feature and capability are on)

#### Interfaces
- Consumes: OL3, OL23, OL28, OL29.
- Produces: `ObjectLockAdmin` (`bucket_config`, `put_bucket_config`, `object_lock`, `put_retention`, `put_legal_hold`).
- Produces: two routes; explicit `VersionAddress`; the 404 body; openapi.
- Produces: `enabled:false` refused; set/clear/leave; 403/404/409; read-only.
- Produces: the enable path (`put_bucket_config` with or without a rule) enables versioning first when unversioned (OL24).
- Produces: an update on a disabled bucket is refused; the admin impl in `run.rs` holds the storage handle.

#### Steps
- [x] **Step 1: Failing route tests** — JSON, address, `enabled:false`, set/clear/leave, error mapping, 404 body, admin gate, read-only.
- [x] **Step 2: Implement the trait/routes/client/admin.**
- [x] **Step 3: Gate** — `cargo test -p tinio-api --features object-lock`, clippy, fmt; **Evidence**.

#### Evidence
tinio-api 80 lib + 1 spans + 80 doctests, tinio-cli 239+1+111 with `object-lock`, server 353/354 pass; clippy `-D warnings` both feature sets and nightly fmt clean (2026-10-21). `ObjectLockAdmin` with the two routes and the explicit `current`/`null`/id address; `enabled:false` refused; set/clear/leave and legal-hold set/clear distinct; 403/404/409 with the distinguishing 404 body; the admin gate/read-only; the enable-versioning-first path; the plane-written config invalidates the shared `ObjectLockCache` (`PlaneConfig` handle, `Arc::ptr_eq` pinned). Review clean after fix round 1 (the `bypass` member written into `contracts/management-api.md`; the request DTO serializes "leave" as an omitted member).

### Task 10: The CLI

#### Files
- Create: `crates/tinio-cli/src/commands/{retention,legalhold,object_lock}/…`
- Modify: `crates/tinio-cli/src/commands/mod.rs`, `Cargo.toml` (`object-lock = ["api", "tinio-api/object-lock"]`)

#### Interfaces
- Consumes: OL24, OL30.
- Produces: the seven commands; `bucket[/key]`; `--version-id`/`--bypass`/`--default`/`--json`; `Nd`/`Ny`.
- Produces: auto-enable versioning for `enable`/`set --default`; `clear --default` no-op on disabled.
- `retention info <bucket>` fallback; `run.rs` is Task 9's.
- The CLI drives the plane, never a store or an S3 client.

#### Steps
- [x] **Step 1: Write the failing CLI tests** — the commands, no-server, auto-enable, clear no-op, `--json`, `Nd`/`Ny`.
- [x] **Step 2: Implement the commands.**
- [x] **Step 3: Gate** — `cargo test -p tinio-cli --features object-lock`, clippy, fmt; **Evidence**.

#### Evidence
`cargo test -p tinio-cli --features object-lock` → 260 lib + 1 span + 136 doctests, 0 failed; clippy `--all-targets -- -D warnings` (both feature sets), default/no-default checks and nightly fmt clean (2026-10-21). Seven verbs (`object-lock enable`; `retention set|info|clear`; `legalhold set|info|clear`) with `bucket[/key]`, `--version-id`/`--bypass`/`--default`/`--json`, `Nd`/`Ny`; usage checks run before the server probe (exit 2 on a stopped server); auto-enable versioning delegates to the plane's enable path; `clear --default` is a no-op on a disabled bucket; `--json` mirrors mc's field names; the CLI drives the plane only. Review clean after fix round 1 (pre-connect planning; the API's `UpdateObjectLock` is the single request serializer).

### Task 11: The configuration, facade and feature matrix

#### Files
- Modify: `crates/tinio/Cargo.toml` (`full`, forward)
- Modify: `crates/tinio-cli/src/config/s3.rs`, `config/tests.rs` (the key; the field is declared by Task 6 on `Capabilities`)
- Modify: `crates/tinio-cli/Cargo.toml` if the forward needs a dependency edge
- Verify only: the per-task feature declarations

#### Interfaces
- Consumes: OL2, OL20, OL26.
- Produces: `[s3] object_lock = true`; `object-lock` in `full`.
- Produces: feature matrix; stripped-build runtime refusal test; `cargo hack … --skip otel`.

#### Steps
- [x] **Step 1: Write the failing config/feature tests** — default, every feature set, stripped runtime refusal.
- [x] **Step 2: Wire the facade and verify the matrix.**
- [x] **Step 3: Gate** — the matrix + stripped test; clippy, fmt; **Evidence**.

#### Evidence
`[s3] object_lock` defaults true and maps to `Capabilities::object_lock` (config tests 100/100); `object-lock` is in the facade `full` and forwarded; `cargo hack check -p tinio-server --each-feature --skip otel` → 16/16 and `-p tinio-cli --each-feature` → 12/12; the stripped-build runtime refusal (`backend/object_lock_stripped.rs`, inside `#[cfg(test)] mod tests` per R27) proves the six operations `NotImplemented` and — after the review fix — that a stripped PUT/COPY/multipart into a lock-enabled bucket still applies the bucket default retention and refuses a permanent delete (403); 5 stripped tests default, 1 with `--no-default-features` (2026-10-21). Clippy both server postures, facade checks and fmt clean.

### Task 12: The e2e and interop corpora

#### Files
- Create: `crates/tinio-e2e/tests/features/object_lock.feature`, `steps/object_lock.rs`.
- Create: `features/cli_object_lock.feature`, `crates/tinio-server/tests/boto3_object_lock.py`.
- Modify: `crates/tinio-e2e/tests/steps/mod.rs`, `Cargo.toml` (the feature), `traceability.rs` (drop the allow-list)

#### Interfaces
- Consumes: all; OL1–OL31.
- Produces: AWS/CLI/boto3 journeys; current/null addresses; marker mix.
- Produces: PostObject lock fields; no-config bucket cache; capability off; read-only.

#### Steps
- [x] **Step 1: Write the features/steps.**
- [x] **Step 2: Run fs/mem/pg with the feature; the boto3 tier.**
- [x] **Step 3: Drop the allow-list and run traceability.**
- [x] **Step 4: Gate** — `cargo test -p tinio-e2e --features object-lock`, traceability, clippy, fmt; **Evidence**.

#### Evidence
`cargo test -p tinio-e2e --features object-lock` → 328 scenarios/3037 steps passed; object-lock-only legs fs 16/16, mem 15/15, pg 15/15; the full pg leg 267/267; the boto3 tier passes (including a deliberately divergent shell credential pair); `--test traceability` → 21 passed after dropping the Task 0 allow-list (2026-10-21). The AWS journey covers the six round trips, CreateBucket header, suspension, write headers, checksum, protected 403, bulk per-key, echo incl. null, the marker mix, Complete refusal, PostObject form fields, no-config cache, capability off and read-only; the CLI journey covers the seven verbs, `--json`, the auto-enable, the clear no-op, version-addressed/bypass legs and the no-server refusal. The review-driven pg `NoSuchBucket` fix (`PgStorage::ensure_bucket` on the six object reads) was made here; clippy/fmt clean.

### Task 13: The final verification and docs

#### Files
- Modify: the crate READMEs; `tasks.md` ticks; this plan's evidence only

#### Steps
- [x] **Step 1: Full gate** — workspace tests with the feature; pg legs on a **fresh database** (no old-DB migration).
- Step 1 gate: e2e; clippy; fmt; doc; sqlx check; feature matrix (`--skip otel`); traceability; doctor/startup-repair tests.
- [x] **Step 2: Run the spec/plan ownership check** — one owner per fact; no duplicate definitions.
- [x] **Step 3: Evidence** — commands and tails; leave the commit to the operator.

#### Evidence
`cargo test --workspace --all-targets` exit 0 (405 server lib, 197 store-pg, 20 pg, 303 e2e scenarios, 21 traceability); feature-on suites (server 357, cli 261+1+136, api 80, e2e 328 scenarios/3037 steps) and the stripped postures (321 / 5+1) pass; pg store gate and pg e2e 279/279 pass on a fresh DB; clippy (workspace + object-lock variants + feature-off libs), fmt, `cargo hack` (server 16/16, fs 5/5, facade), doctor 32 and startup repair 2, `cargo xtask sqlx check` 185/185 pass (2026-10-22). Two pre-existing red legs reproduce byte-identically at base `31d91f10` and are not introduced here: `cargo doc --workspace` (`tinio-util`'s unchanged `itoa::Buffer` link) and `clippy -p tinio-server --no-default-features --lib` (four dead content-md5 helpers + `absolute_paths`). The ownership check named each family's owner and fixed three restatements and two stale FR-043 allow-list statements; the READMEs and T176–T189 are updated. The final whole-branch review's seven findings were fixed in one wave (fs sweeper block survival, config-write versioning check inside the transaction, stripped checksum/Complete, missing-bucket parity, R28 corrupt-row 500, R29 S3 clear, `enable` preserving the rule) and re-reviewed clean; the test-only metric flake was serialized (R30). The commit is the operator's.
