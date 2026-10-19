# S3 Object Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One lifecycle engine, generic over the persistence contracts, that every backend gets by storing a bucket-rule row: the three locally meaningful AWS actions enforced in the background, managed through the S3 subresource, the management plane and the CLI, seeded from the configuration file, double-gated at compile time and at runtime.

**Architecture:** The model, its JSON wire, its validation and its matcher live in `tinio-core` (the `cors.rs` precedent); the pass lives in a new `tinio-lifecycle` crate that depends on `tinio-core` only and drives the existing `Storage`/`VersionOps` deletions; the rules live in a new per-bucket table in each store (redb table, pg table inside `0001_init.sql`, mem free through the shared redb layer). The S3 surface is three rows in the generated operation table; the management plane is three routes fed by an injected trait; the CLI is four verbs over that plane.

**Tech Stack:** Rust edition 2024, `serde`/`serde_json` for the wire, `time` for UTC midnight arithmetic, `s3s` 0.17 dtos for the XML, `axum`/`utoipa` for the plane, `sqlx` for pg, `redb` for fs/mem, cucumber for the e2e pass.

**Spec:** `docs/superpowers/specs/2026-10-18-s3-object-lifecycle-design.md` — the binding decisions are LC1–LC22; executors read both. Citations the decisions rest on: `docs/superpowers/lifecycle-prior-art-research.md` (AWS + MinIO §1–§9, RustFS §10). There is no translation mirror (operator ruling 2026-10-18).

## Global Constraints

- Decisions: LC1–LC22 are binding; where this plan and the spec disagree, the spec wins and the plan is corrected, never the code.
- Delivery: implement tasks in order; each task leaves the tree compiling and the named suites green. No parallel tasks — Task 3a changes a trait every other task's files implement.
- Delivery (operator ruling 2026-10-18): **subagent-driven per-task review, one commit at the end, operator-owned** — agents never stage or commit, so the index stays the operator's review progress.
- Git: agents never `git add` or commit; the operator owns the index and the single commit.
- Tests: test first in every task, then the smallest implementation, then the task's own gate: `cargo test -p <crate>`, `cargo clippy -p <crate> --all-targets -- -D warnings`, `cargo +nightly fmt -p <crate>`.
- PG: every pg leg needs `TINIO_PG_DSN` and `--test-threads=4`; every new statement needs `cargo xtask sqlx update` against the fixture, and `cargo xtask sqlx check` must be clean before the task is ticked.
- Features: `lifecycle` on `tinio-server`, `tinio-api` and `tinio-cli`; `tinio`'s `full` gains it. `tinio-core`'s model is always compiled (LC15).
- Wires: S3 surface = the framework's XML on `/?lifecycle`; plane and CLI = the model's JSON. Never a third spelling (LC17).
- Docs prose: English, no translation mirror — the operator deleted the reading copy and ruled it stays deleted (their own edit; docs/compress.md's English-only rule is the law).
- Records: tick `- [x]` and append one `Evidence` line (command + result) in the task that ran it. Never rewrite another task's record.

## Review Focus

The spec's §Review Focus owns the failure catalogue; each line there is pinned by a task's tests. This plan adds the ones only the wiring can break:

1. A trait change leaves an implementor unimplemented — `BucketMeta` has **four** implementations in-tree (`RedbMetadata`, `PgStore`, `tinio-fs`'s `Rows` adapter, `tinio-version`'s `Fake`); a missed one is a compile error only in some feature matrix, so Task 3a builds the workspace `--all-targets` explicitly.
2. A table-name pin goes stale — redb's two table-list tests and the bucket-row 5-tuple pin must be updated, not "fixed" by loosening the assertion (Task 3a).
3. The operation table's counts drift — `WRITE_OPS` is a fixed-size array and the read/write lists are counted in tests; adding three rows without touching both is a silent capability hole (Task 6).
4. A plane route exists where the build cannot serve it — a stripped build must answer `404` and never offer a `PUT` that would replace a stored configuration (Task 8).
5. The pass is spawned but never told its options, or the options replace another pass's — the `ServingBackend::background` signature is shared by both arms (Task 10).
6. `cargo xtask sqlx check` is skipped after an inline `query!` edit — the database-free build then fails on a cache miss (Tasks 3, 6).

---

### Task 0: The contract rows

#### Files
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (new `## Lifecycle` section)
- Modify: `specs/001-s3-local-server/contracts/config.md` (`[lifecycle]`, `[[lifecycle.rules]]`, `[s3] lifecycle`)
- Modify: `specs/001-s3-local-server/contracts/cli.md` (the `lifecycle` verb group + FR-007's one exception)
- Modify: `crates/tinio-e2e/tests/traceability.rs` (temporary allow-list entry for FR-042)

#### Interfaces

- Consumes: LC7, LC8, LC9, LC13, LC14, LC20, LC21, LC22, LC23.
- Produces: the normative element/key/verb lists Tasks 6, 8 and 10 implement; the temporary allow-list entry Task 11 removes.

#### Steps

- [x] **Step 1: Write the three contract sections** — s3-surface: the three operations, the `/?lifecycle` wire, `200`/`404 NoSuchLifecycleConfiguration`/`204`, the accepted element set, the two error classes, the checksum requirement, transitions refused. config: `[s3] lifecycle` (default true), `[lifecycle] delay` (default `10s`) / `cycle` (default `1h`), `[[lifecycle.rules]]` with `bucket`, `id` (required), `prefix`, `expire_days`, `noncurrent_expire_days`, `abort_incomplete_days`. cli: `tinio lifecycle ls|add|rm|put` and the sentence that this is FR-007's one exception.
- [x] **Step 2: Cite `FR-042` in the new sections, and add the temporary allow-list entry** — `FR-037`–`FR-041` belong to the versioning family (`contracts/s3-surface.md` §Object versioning) and are not yours to touch. The corpus check runs in both directions: once `FR-042` is defined in a contract and nothing tags it, `spec_ids_and_feature_tags_are_consistent` fails without an entry (measured — ledger R7), so the entry goes in now and Task 11 drops it.
- [x] **Step 3: Verify** — `cargo test -p tinio-e2e --test traceability` passes with the entry present.
- [x] **Step 4: Evidence** — paste the command and its tail.
#### Evidence

`cargo test -p tinio-e2e --test traceability` → 19 passed, 0 failed (Task 0, fix round 1); review 1 = spec ✅ / changes requested (2 Important, 3 Minor); re-review = all 3 findings ADDRESSED, no new breakage.

### Task 1: The model, the wire and the validation

#### Files
- Create: `crates/tinio-core/src/lifecycle.rs`
- Modify: `crates/tinio-core/src/lib.rs` (add `pub mod lifecycle;` beside `cors`)
- Modify: `crates/tinio-core/Cargo.toml` (`serde_json.workspace = true` unconditionally; the `sqlx` feature becomes `["dep:sqlx"]`)

#### Interfaces

- Consumes: LC3, LC8, LC17.
- Produces: `lifecycle::{Config, Rule, Filter, Tag, Status, Expiration, NoncurrentVersionExpiration, AbortIncompleteMultipartUpload, Invalid, InvalidKind}`; `Config::validate(&self) -> Result<(), Invalid>`; `Config::to_wire(&self) -> String`; `Config::from_wire(wire: &str) -> Option<Config>`; `Rule { id: Option<String>, status: Status, filter: Filter, expiration: Option<Expiration>, noncurrent_version_expiration: Option<NoncurrentVersionExpiration>, abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload> }`; `Filter { prefix: Option<String>, tag: Option<Tag>, size_greater_than: Option<u64>, size_less_than: Option<u64>, and: Option<And> }`; `Invalid { rule: Option<String>, reason: &'static str, kind: InvalidKind }` implementing `Display`, with `InvalidKind::{Shape, Semantic}` — the two error classes LC8 splits into `MalformedXML` and `InvalidRequest`, carried by the model so Task 6 maps one field instead of re-deriving the class from a string.

#### Steps

- [x] **Step 1: Write the failing validation matrix** — one test per refusal, each asserting `validate()`'s `reason`: `two_top_level_predicates_are_malformed`, `an_and_needs_two_predicates`, `a_size_range_needs_and`, `the_greater_bound_must_be_below_the_less`, `an_empty_filter_is_everything`, `a_rule_needs_at_least_one_action`, `several_actions_in_one_rule_are_legal`, `id_is_at_most_255_and_unique`, `days_is_at_least_one`, `a_date_must_be_midnight_utc`, `days_and_date_are_exclusive`, `expired_delete_marker_excludes_days_and_date`, `a_tag_filter_excludes_abort_and_delete_marker`, `newer_noncurrent_versions_needs_a_filter`, `a_transition_is_refused`.
- [x] **Step 2: Run them and watch them fail to compile** — `cargo test -p tinio-core lifecycle` → the types do not exist.
- [x] **Step 3: Implement the types and `validate`** — serde derives on the model (the wire and the plane JSON are one shape, LC17); `Invalid` carries the offending rule's id when there is one.
- [x] **Step 4: Run the matrix green.**
- [x] **Step 5: Write the wire tests** — `the_wire_round_trips`, `a_corrupt_wire_decodes_to_none`.
- [x] **Step 6: Implement `to_wire`/`from_wire`** over `serde_json`, returning `None` on a decode error.
- [x] **Step 7: Gate** — `cargo test -p tinio-core`, `cargo clippy -p tinio-core --all-targets -- -D warnings`, `cargo +nightly fmt -p tinio-core`; **Evidence** line.
#### Evidence

24 lifecycle tests + 215 unit + 8 validation + 88 doctests green; clippy both feature sets, fmt, `cargo doc` clean; review 1 = changes requested (1 Important + 3 ruled-in) → fix round 1 → re-review all ADDRESSED, no new breakage, verdict OK.

### Task 2: The matcher and the day arithmetic

**Files:**
- Modify: `crates/tinio-core/src/lifecycle.rs`
- Modify: `crates/tinio-core/Cargo.toml` (`time.workspace = true` — the model stores a `Date` as Unix seconds (Task 1), and the UTC-midnight arithmetic needs a calendar; std has none, so the matcher brings `time` into the crate)

**Interfaces:**
- Consumes: Task 1's types; `object::{Key, Tags}`.
- Produces: `Facts<'a> { key: &'a object::Key, size: u64, last_modified: SystemTime, tags: &'a object::Tags }`; `Config::rules_for<'a>(&'a self, facts: &'a Facts<'_>) -> impl Iterator<Item = &'a Rule> + 'a`; `Config::expiry_for(&self, facts: &Facts<'_>) -> Option<SystemTime>`; **`Config::noncurrent_plans<'a>(&'a self, facts: &'a Facts<'_>) -> impl Iterator<Item = NoncurrentPlan> + 'a`** with `NoncurrentPlan { days: u32, keep: Option<u32> }` — one plan per matching rule, because the two thresholds are evaluated **per rule** and a merged pair would authorize a deletion no single rule permits (review F1); `Config::abort_after(&self, facts: &Facts<'_>) -> Option<u32>`; `pub fn midnight_utc(anchor: SystemTime, days: u32) -> SystemTime`.

- [x] **Step 1: Write the failing matcher tests** — `prefix_matches_by_prefix`, `a_tag_predicate_needs_the_tag`, `an_untagged_fact_never_matches_a_tag_rule`, `and_requires_every_predicate`, `size_bounds_are_strict`, `a_disabled_rule_never_matches`, `the_earliest_expiry_wins`, `days_round_up_to_the_next_midnight`, `a_count_only_plan_is_zero_days`, `both_thresholds_are_required`.
- [x] **Step 2: Run them and watch them fail.**
- [x] **Step 3: Implement the matcher and `midnight_utc`** — `time::OffsetDateTime` for the UTC arithmetic; the two-threshold rule is `days >= plan.days && keep.map_or(true, |k| rank >= k)` (LC10 — both, not either).
- [x] **Step 4: Run green; gate as in Task 1; Evidence line.**
**Evidence:** 38 lifecycle tests, 229 lib + 8 validation + 88 doctests, `tinio-api --features lifecycle` 60+75, clippy and fmt clean; review 1 = one semantic defect (the merged noncurrent thresholds) + 3 ruled-in → fix 1 (lazy iterator) → review 2 (F1–F4) → fix 2 (per-rule `noncurrent_plans`) → review 3 → fix 3 (`Option` direction) → review 3 (addressed) → fix 4 (test strength) → review 4 clean.

### Task 3a: The rule row — interfaces and the two stores

#### Files: core and redb

- Modify: `crates/tinio-core/src/meta/bucket.rs` (the four trait methods)
- Create: `crates/tinio-store-redb/src/bucket_lifecycle.rs`
- Modify: `crates/tinio-store-redb/src/lib.rs` (module, `ensure_all`, both table-list pins)
- Modify: `crates/tinio-store-redb/src/meta/bucket.rs`

#### Files: pg, fs and the fake

- Modify: `crates/tinio-store-pg/sql/migrations/0001_init.sql` (the table joins the initial schema — no second migration; `bucket` references `buckets(name) ON DELETE CASCADE`)
- Modify: `crates/tinio-store-pg/src/meta/bucket.rs`
- Modify: `crates/tinio-fs/src/version_adapter.rs` (`impl BucketMeta for Rows`)
- Modify: `crates/tinio-version/src/testutil.rs` (`impl BucketMeta for Fake`)

#### Interfaces
- Consumes: Task 1's `Config`; LC5, LC6.
- Produces: `BucketMeta::lifecycle(&self, bucket: &bucket::Name) -> Result<Option<lifecycle::Config>, <Self as Metadata>::Error>`; `lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, <Self as Metadata>::Error>`; `set_lifecycle(&self, bucket, &Config) -> Result<bool, …>`; `set_lifecycle_or_create(&self, bucket, &Config, now) -> Result<(), …>` (the lazy first-sight twin of `set_tags_or_create`: a row-less bucket gains its row and its configuration in one transaction); `clear_lifecycle(&self, bucket) -> Result<bool, …>`. **The `bool` is bucket presence, not change** — the `set_if`/`recorded` tail the CORS setters already use (`crates/tinio-store-redb/src/meta/bucket.rs:40-70`) — so re-`PUT`ting an identical configuration, and `DELETE`ing one on a bucket that stores none, both answer `true`; only a missing bucket answers `false`. `tinio_ops`' `put_bucket_lifecycle` calls the or-create form on a lazy store and the bool form on a guaranteed one; `delete_bucket_lifecycle` treats a lazy store's row-less clear as idempotent. `lifecycles` parses each stored name, discarding an unparsable one with one warning. redb: `bucket_lifecycle::{Def, Table, get, set, drain_bucket}` with `MAY_BE_ABSENT = true`. pg: `bucket_lifecycle(bucket text COLLATE "C" PRIMARY KEY REFERENCES buckets(name) ON DELETE CASCADE, config text NOT NULL, updated_at timestamptz NOT NULL)`.

#### Steps

- [x] **Step 1: Declare the five trait methods, then implement all five in every implementor in the same change** — `RedbMetadata` (new table), `PgStore` (new table), `tinio-fs`'s `Rows` adapter (delegate to the same table), `tinio-version`'s `Fake` (in-memory map). A missed implementor is a compile error only in some feature sets, so the gate below builds everything.
- [x] **Step 2: The redb table** — `bucket_lifecycle.rs` mirrors `bucket_versioning.rs`: `Def` with `DEF = TableDefinition::new("bucket_lifecycle")` and `MAY_BE_ABSENT = true`, a `Table` alias, `get`, `set`, `drain_bucket`; register it in `lib.rs`'s module list and `ensure_all`; update the eleven-table pins in `lib.rs` and `open.rs` to twelve (`"bucket_lifecycle"` sorts between `"buckets"` and `"object_meta"`); the buckets 5-tuple pin stays untouched and must stay untouched. `set_lifecycle_or_create` inserts `BucketRow::at(now)` and the configuration in one write transaction.
- [x] **Step 3: The pg table** — the DDL joins `0001_init.sql` after `bucket_versioning`'s block; the five methods use inline `query!` (`SELECT config FROM bucket_lifecycle WHERE bucket = $1`; `INSERT INTO bucket_lifecycle (bucket, config, updated_at) VALUES ($1, $2, now()) ON CONFLICT (bucket) DO UPDATE SET config = $2, updated_at = now()`; `DELETE FROM bucket_lifecycle WHERE bucket = $1` returning `rows_affected`; `SELECT bucket, config FROM bucket_lifecycle ORDER BY bucket`), with the or-create form reusing the cached `record_if_absent.sql` inside the same transaction; then `cargo xtask sqlx update` against the fixture and `cargo xtask sqlx check`.
- [x] **Step 4: Gate** — `cargo build --workspace --all-targets` (the four-implementor check); `cargo test -p tinio-store-redb -p tinio-store-pg -p tinio-fs -p tinio-mem -p tinio-version -p tinio-util -p tinio-ops` green with the existing legs only; `cargo clippy -p tinio-store-redb -p tinio-store-pg -p tinio-fs -p tinio-version --all-targets -- -D warnings`.
- [x] **Step 5: Evidence** — paste the commands and tails.
#### Evidence

`cargo build --workspace --all-targets` clean; the seven-crate suite green (redb 146, store-pg 181 live, fs 386, lifecycle 5+16+11, ops 78, version 23, util green); `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo xtask sqlx check` up to date (167 entries). The 2026-10-18 review amended the contract: `set_lifecycle_or_create` (the lazy first-sight twin) joined the four methods, and `lifecycles()` now returns `Vec<(bucket::Name, Config)>`; the conformance leg `set_lifecycle_or_create` proves the first-sight row and its configuration.

### Task 3b: Rules die with the bucket (LC19) and the conformance legs

**Files:**
- Modify: `crates/tinio-core/src/meta/bucket.rs` (`create`/`record_full`/`remove`/`remove_if_empty` clear the row)
- Modify: `crates/tinio-store-redb/src/meta/bucket.rs` (pg's cascade needs no Rust clear)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (the teardown's per-table transaction)
- Modify: `crates/tinio-util/src/testing/metadata.rs` (`bucket_legs`)

**Interfaces:**
- Consumes: Task 3a's five methods.
- Produces: the deletion coupling LC19 names, and the legs that prove it.

- [x] **Step 1: Write the legs** — `a_lifecycle_row_round_trips`, `lifecycles_lists_only_configured_buckets`, `clear_lifecycle_is_idempotent_and_false_without_a_row` (these three should pass on first run against 3a — say so in their comment; they are characterization legs), and the two that must fail: `create_clears_a_stale_lifecycle_row`, `deleting_a_bucket_clears_its_rules`.
- [x] **Step 2: Run them and watch exactly the two coupling legs fail** — `cargo test -p tinio-util`.
- [x] **Step 3: Implement the coupling** — pg's `bucket_lifecycle.bucket` references `buckets(name) ON DELETE CASCADE`, so no Rust clear is needed there; fs/mem `create`/`record_full` drain the row in the same write transaction as the bucket row, and `remove`/`remove_if_empty` drain it too. **fs's bucket delete never reaches `remove`**, so its teardown transaction (`crates/tinio-fs/src/backend/mod.rs`, where `bucket_versioning` is drained today) gains a `bucket_lifecycle::Table::open(txn)?.drain_bucket(&bucket)?` block; `Cleanup`'s stale-bucket stage gains the same one-row deletion for a bucket that vanished out of band (fs/mem only; pg's FK makes it impossible).
- [x] **Step 4: Run green** — `cargo test -p tinio-util -p tinio-store-redb -p tinio-fs -p tinio-mem -p tinio-version`; then the pg legs with `TINIO_PG_DSN` and `--test-threads=4`; clippy; **Evidence**.
**Evidence:** RED A `create_clears_a_stale_lifecycle_row` + RED B `deleting_a_bucket_clears_its_rules` failed first, then green; fs 378, mem 100, redb 146+2+1+2, util 48+27, version 23, store-pg 181+39, tinio-pg 20/6/6/2/2/2/9 — 0 failed; clippy, fmt, `cargo xtask sqlx check` 165. Review = nothing blocking; fix 1 pinned pg's two creation drains by mutation (deleting either drain fails its own assertion) → re-review ADDRESSED.

### Task 4: The three bodies and the three forwards

**Files:**
- Modify: `crates/tinio-core/src/storage/bucket.rs` (three `BucketOps` methods, beside the CORS block)
- Modify: `crates/tinio-ops/src/bucket.rs` (the bodies)
- Modify: `crates/tinio-fs/src/backend/buckets.rs`, `crates/tinio-mem/src/bucket.rs`, `crates/tinio-pg/src/storage.rs` (thin forwards)

**Interfaces:**
- Consumes: Task 3a's `BucketMeta`; `tinio_ops::bucket::BucketOps::row_or`; `storage::no_such_bucket`.
- Produces: `BucketOps::{get,put,delete}_bucket_lifecycle(&self, name: &bucket::Name[, config: &lifecycle::Config]) -> Result<…, <Self as Storage>::Error>`; `tinio_ops` bodies `get_bucket_lifecycle` (via `row_or(name, None, |row| …)`), `put_bucket_lifecycle` (a `BucketRow::Guaranteed` store's `set_lifecycle` `false` → `no_such_bucket`; a `BucketRow::Lazy` store's `set_lifecycle_or_create` records first sight), `delete_bucket_lifecycle` (guaranteed: `clear_lifecycle`'s `false` → `no_such_bucket`; lazy: the row-less clear is idempotent). `false` is **no bucket row**, never "no configuration": the surface's rule is that a `DELETE` on an existing bucket that stores none answers `204`, which is why Task 3a's bool is presence and not change.

- [x] **Step 1: Write the failing fs leg** — `crates/tinio-fs/src/backend/buckets.rs`'s test module: `lifecycle_round_trips_and_needs_a_bucket` (put on a ghost bucket → `NoSuchBucket`; put/get/delete on a real one).
- [x] **Step 2: Run it and watch it fail.**
- [x] **Step 3: Declare the three `Storage` methods, write the three `tinio-ops` bodies, forward from fs/mem/pg.**
- [x] **Step 4: Run green** — `cargo test -p tinio-fs -p tinio-mem -p tinio-pg`; clippy; **Evidence**.
**Evidence:** fs 378+18, mem 99+4, pg green offline and live, ops 78+doctests, `build --workspace --all-targets`, clippy on five crates, fmt; review = OK with notes (no code change; the report mis-attributed Task 3b's LC19 teardown that a whole-worktree diff carried into this package).

### Task 5: The `tinio-lifecycle` crate and the pass

**Files:**
- Create: `crates/tinio-lifecycle/{Cargo.toml,src/lib.rs,src/clock.rs,src/runner.rs}`
- Create: `crates/tinio-lifecycle/tests/engine.rs`
- Modify: `Cargo.toml` (workspace member + `tinio-lifecycle = { path = "crates/tinio-lifecycle", default-features = false }` pin)

**Interfaces:**
- Consumes: Tasks 1–4; `tinio_core::storage::{Storage, VersionOps}`, `meta::{BucketMeta, VersionMeta, UploadMeta}`, `object::Info`, `version::Entry`, `multipart::MultipartUpload`.
- Produces: `Clock` (`fn now(&self) -> SystemTime`), `SystemClock`, `FixedClock`; `Options { delay: Duration, cycle: Duration }` with `Default` = 10 s / 1 h; `Summary { buckets_with_rules: usize, expired_objects: u64, expired_versions: u64, aborted_uploads: u64, errors: u64 }`; `Runner<S: Storage>::new(Arc<S>, Options, Arc<dyn Clock>, watch::Receiver<bool>)`, `Runner::seed(&self, seeds: &[Seed])`, `Runner::pass(&self) -> Summary`, `Runner::run(&self)`; `Seed { bucket: bucket::Name, rule: lifecycle::Rule }`. **Amended in the 2026-10-18 simplify round**: the pass reads every rule in one `storage::LifecycleRules::lifecycles()` scan and intersects it with `list_buckets` (LC19) instead of one `BucketMeta::lifecycle` read per bucket.

- [x] **Step 1: Write the failing engine tests** (`tests/engine.rs`, over `tinio-mem` as a dev-dependency, `FixedClock`): `an_expired_object_is_deleted`, `a_versioned_object_gains_a_marker_not_a_deletion`, `a_due_key_already_marked_is_skipped`, `a_noncurrent_version_past_both_thresholds_is_deleted`, `the_count_only_form_keeps_the_count`, `an_only_version_marker_is_removed`, `an_upload_past_the_clock_is_aborted`, `a_key_that_vanished_is_counted_not_failed`, `a_suspended_bucket_writes_a_null_marker`.
- [x] **Step 2: Run and watch them fail** — the crate does not exist.
- [x] **Step 3: Implement `pass`** — buckets via `list_buckets`; **one `LifecycleRules::lifecycles()` scan, intersected with the listed buckets** (LC19: a stale row is never looked up); the rules map is consumed per bucket; objects via `list_objects`; a due key's current slot via `VersionOps::current_version` (skip a marker); versions via `list_object_versions` newest-first with the running rank, and a version is due when **some single rule's own pair** is exceeded — `noncurrent_plans(..).any(|p| age_days >= p.days && p.keep.is_none_or(|k| rank >= k))`; uploads via `list_multipart_uploads`; every action wrapped so `NoSuchKey` is counted, not returned; `delay` sleeps and the stop check between items; one `tracing::info!` summary per pass with the five counters (LC16) and nothing per object at info.
- [x] **Step 4: Run green; clippy `-p tinio-lifecycle --all-targets`; `cargo hack check --package tinio-lifecycle --each-feature`; Evidence.**
**Evidence:** `tinio-lifecycle` 4 unit + 11 engine + 10 doctests, `tinio-core` 231+8+89, hack each-feature, clippy both, fmt, `cargo doc`, workspace build; review 1 = OK with notes (1 Important + 3 Minor) → fix 1 (`uses_tags` one home) → fix 2 (the four items) → re-review all ADDRESSED, verdict ready to close. Simplify round (2026-10-18): the pass reads every rule in one `LifecycleRules::lifecycles()` scan and intersects it with `list_buckets` (LC19), instead of one read per bucket; the engine suite and doctests re-ran green.

### Task 6: The S3 surface and the capability

**Files:**
- Create: `crates/tinio-server/src/backend/lifecycle.rs`
- Modify: `crates/tinio-server/src/backend/ops.rs` (three rows; `WRITE_OPS` 21 → 23)
- Modify: `crates/tinio-server/src/backend/mod.rs` (module declaration)
- Modify: `crates/tinio-server/src/s3.rs` (`Capabilities::lifecycle`, default true, `serde(default = "lifecycle")`)
- Modify: `crates/tinio-server/Cargo.toml` (`lifecycle` feature)

**Interfaces:**
- Consumes: Task 4's `BucketOps` methods; Task 1's `validate`/`Invalid`; LC7, LC8, LC9, LC18.
- Produces: `op_get_bucket_lifecycle`, `op_put_bucket_lifecycle`, `op_delete_bucket_lifecycle` on `impl<S: Storage> S3Backend<S>`.

- [x] **Step 1: Write the failing handler tests** — in `lifecycle.rs`'s test module, shaped like `cors.rs`'s: `a_missing_configuration_answers_no_such_lifecycle_configuration`, `a_put_without_a_checksum_is_bad_request`, `a_mismatched_content_md5_is_bad_request`, `two_top_level_predicates_are_malformed_xml`, `a_duplicate_id_is_invalid_request`, `a_transition_is_invalid_request_naming_the_tiers`, `a_put_then_get_round_trips`.
- [x] **Step 2: Run and watch them fail.**
- [x] **Step 3: Implement the dto mapping and the three handlers** — map `s3s::dto::{LifecycleRule, LifecycleRuleFilter, LifecycleExpiration, NoncurrentVersionExpiration, AbortIncompleteMultipartUpload}` onto the model; pick the checksum from `Content-MD5` or any `x-amz-checksum-*`; `Invalid` reason → `MalformedXML` or `InvalidRequest` by the model's own class.
- [x] **Step 4: Add the three op rows and the capability field** — `#[gated(lifecycle: "lifecycle")]`, `WRITE_OPS` gains the two write names; the read/write count tests in `ops.rs` are updated with it. Also wire `caps.lifecycle = false` into `@minimal-caps`'s arm in `crates/tinio-e2e/tests/steps/mod.rs`'s `config_from_tags`, so the corpus's capability-off scenario stops passing vacuously (Task 11's concern 1).
- [x] **Step 5: Run the matrix** — `cargo test -p tinio-server`; `cargo test -p tinio-server --features lifecycle`; clippy both; **Evidence**.
**Evidence:** server 301 lib without the feature / 312 with it + 20 doctests, read_only 1, sandbox 3; e2e traceability 20 and the full run 293/2445; clippy both feature sets; fmt. Review = OK with notes → fix 1 (two `ops.rs` counts, the module doc order, the shadowed transition message) → re-review all three ADDRESSED. R43: the checksum is form-checked only (mismatch unverifiable — the body is consumed before the handler), documented next to CORS/ACL.

### Task 7: The sweeper stands down for a governed bucket (LC21)

**Files:**
- Modify: `crates/tinio-fs/src/sweep.rs` (the noncurrent and multipart stages)
- Modify: `crates/tinio-pg/src/cleanup.rs` (the same two stages, including the abandoned-upload stage the pg pass did not have at HEAD)
- Modify: `crates/tinio-store-pg/src/cleanup.rs`, `crates/tinio-store-pg/sql/cleanup/{stale_uploads,abort_stale_uploads}.sql` (the store half of the pg abandoned-upload stage)
- Modify: `crates/tinio-pg/tests/multipart_sweep.rs` (the stage's legs)

**Interfaces:**
- Consumes: Task 3a's `BucketMeta::lifecycles()`.
- Produces: a per-pass set of `(bucket, governs_noncurrent, governs_abort)`, read once by each sweeper from its **own storage handle** at the top of the pass (no cord through the CLI's `sweep::Options`); stale entries are harmless, and a rule that is `Disabled` never makes the sweeper stand down — only an `Enabled` rule that carries the action does. **The pg abandoned-upload stage is scope amended during the 2026-10-18 code review**: design LC21's "already aborts uploads" claim described the fs sweeper only, and the pg pass gains the stage here as backend parity, at the same `[s3] multipart_expire_days` clock and with the same per-bucket governed skip. It runs only when the operator wrote that key, so a default pg configuration keeps its pre-lifecycle behavior (LC21).

- [x] **Step 1: Write the failing sweep tests** — `a_bucket_with_a_noncurrent_rule_keeps_its_versions_past_the_server_knob`, `a_bucket_with_an_abort_rule_is_not_swept_by_the_server_knob`, `a_bucket_with_an_expiration_rule_only_keeps_the_server_noncurrent_behaviour`; the pg half is `crates/tinio-pg/tests/multipart_sweep.rs`'s abandonment and governed-skip legs.
- [x] **Step 2: Run and watch them fail.**
- [x] **Step 3: Implement the read and the per-action skip** — one `lifecycles()` call at the top of a pass; the skip is per action, never per bucket.
- [x] **Step 4: Run green** — `cargo test -p tinio-fs -p tinio-pg`; clippy; **Evidence**.
**Evidence:** fs 382+18, pg legs 20/6/6/2/2/2/9, core 40 lifecycle, clippy `--all-targets` with and without default features, fmt, workspace build; review = OK with notes → fix 1 (`governs_*` folded into `tinio-core`, R48) → fix 2 (`#[cfg]` on the fixture, module-header clause, report correction) → re-review all three ADDRESSED, verdict ready to close. Simplify round: `PgCleanup::pass` reads the rules once for the retention and abandoned-upload stages; `repair`/`abort_stale_uploads` remain for the doctor and single-stage callers.

### Task 8: The management plane

**Files:**
- Create: `crates/tinio-api/src/lifecycle.rs` (the trait)
- Modify: `crates/tinio-api/src/{lib.rs,router.rs,openapi.rs}`
- Modify: `crates/tinio-api/Cargo.toml` (`async-trait`, `lifecycle` feature)
- Modify: `specs/001-s3-local-server/contracts/management-api.md` (the three `/buckets/{bucket}/lifecycle` rows and their status mapping — Task 0's concern 1)

**Interfaces:**
- Consumes: Task 4's `BucketOps` methods (through the CLI's implementation); LC14, LC15, LC18.
- Produces: `#[async_trait] pub trait LifecycleAdmin: Send + Sync { async fn get(&self, bucket: &str) -> Result<Option<lifecycle::Config>, storage::Error>; async fn put(&self, bucket: &str, config: &lifecycle::Config) -> Result<(), storage::Error>; async fn delete(&self, bucket: &str) -> Result<(), storage::Error>; }`; `Context::with_lifecycle(admin: Option<Arc<dyn LifecycleAdmin>>) -> Self`; routes `GET|PUT|DELETE /buckets/{bucket}/lifecycle`.

- [x] **Step 1: Write the failing router tests** — `the_lifecycle_routes_need_the_token`, `a_get_without_a_configuration_is_404`, `a_read_only_refusal_is_403`, `no_injected_admin_answers_404`, `the_openapi_document_lists_the_three_paths`.
- [x] **Step 2: Run and watch them fail.**
- [x] **Step 3: Implement the trait, the injection and the routes** — registered before `route_layer`, `#[cfg(feature = "lifecycle")]`, JSON body = the model; `storage::Error::AccessDenied` → `403`, `NoSuchBucket` → `404`, anything else → `500` with the detail only in the log. `Context::lifecycle()` answers `None` when no admin was injected — one mechanism for both causes (LC15's stripped build, LC23's disabled capability) — and every handler answers `404` in that case.
- [x] **Step 4: Run green** — `cargo test -p tinio-api`; `cargo test -p tinio-api --features lifecycle`; `cargo test -p tinio-api --no-default-features`; clippy each; **Evidence**.
**Evidence:** 59+74 default / 59+74 `--features lifecycle` / 46+67 `--no-default-features`, clippy on each, fmt clean; review = Approved with 3 Minor → fix rounds 1–3 (validation-before-store, 404-before-body, contract `PUT` statuses, auth matrix, bare-`Context::new` leg, stub string, `debug`-verbosity clause, `401` bullet) → re-review clean, verdict OK.

### Task 9: The CLI verbs

**Files:**
- Create: `crates/tinio-cli/src/commands/lifecycle/{mod.rs,ls.rs,add.rs,rm.rs,put.rs}`
- Modify: `crates/tinio-cli/src/cli.rs` (`Command::Lifecycle(lifecycle::Args)`, `#[cfg(feature = "lifecycle")]`)
- Modify: `crates/tinio-cli/src/commands/mod.rs` (module list)
- Modify: `crates/tinio-cli/src/run.rs` (dispatch arm)
- Modify: `crates/tinio-cli/Cargo.toml` (`lifecycle = ["dep:tinio-lifecycle", "tinio-api/lifecycle"]`)
- Modify: `crates/tinio-api/src/client.rs` (the existing `Call`/`exchange` can send neither a body nor a `content-type` — Task 8's concern 1 and R19; add both on the same client rather than building a second one. Without `application/json` every `PUT` answers `415`)
- Modify: `specs/001-s3-local-server/contracts/cli.md` (one line: the verbs exist only in a build carrying the `lifecycle` feature — Task 0's concern 2)

**Interfaces:**
- Consumes: Task 8's routes through `tinio_api::client`; `tinio_cli::config::discover` for the root.
- Produces: `tinio lifecycle ls <BUCKET>`, `add <BUCKET> --id <ID> [--prefix <P>] [--expire-days <N>] [--noncurrent-days <N>] [--abort-days <N>]`, `rm <BUCKET>`, `put <BUCKET> <FILE>`; JSON on stdout; `no server for that root` reuses the existing refusal.

- [x] **Step 1: Write the failing parse tests** — `the_four_verbs_parse`, `add_requires_an_id`, `put_requires_a_path`.
- [x] **Step 2: Run and watch them fail.**
- [x] **Step 3: Implement the verb group and the client calls** — `add` reads, merges by id, writes (LC14); `put` replaces from a file; `rm` deletes; `ls` prints.
- [x] **Step 4: Run green** — `cargo test -p tinio-cli --features lifecycle`; `cargo test -p tinio-cli` (the verbs absent without the feature); clippy; **Evidence**.
**Evidence:** cli 228+109 `--features lifecycle` / 223+108 default (verbs absent), api 60+75 and 46+68 stripped, clippy across feature sets, fmt; review = OK with notes → fix 1 (guard recursion, `DirArgs` clause, `add`-replaces clause) → re-review → fix 2 (self-guarding descent) → re-review clean, verdict OK.

### Task 10: The configuration section, the seed and the spawn

**Files:**
- Create: `crates/tinio-cli/src/config/lifecycle.rs`
- Modify: `crates/tinio-cli/src/config/mod.rs` (root field `lifecycle`, module)
- Modify: `crates/tinio-cli/src/commands/start/serving.rs` (`ServingBackend::background` gains a `lifecycle: LifecycleOptions` parameter; both arms join the pass; the seed runs once, before the first pass)
- Modify: `crates/tinio-cli/src/commands/start/serve.rs` (the call site builds the options and the seeds)
- Modify: `crates/tinio/src/lib.rs` and `crates/tinio/Cargo.toml` (`full` gains `lifecycle`; the facade forwards it)

**Interfaces:**
- Consumes: Task 5's `Runner`/`Options`/`Seed`; LC11, LC13, LC22.
- Produces: `config::lifecycle::Config { delay: Duration = 10s, cycle: Duration = 1h, rules: Vec<Rule> }` with `Rule { bucket: String, id: String, prefix: Option<String>, expire_days: Option<u32>, noncurrent_expire_days: Option<u32>, abort_incomplete_days: Option<u32> }`; `lifecycle_options(&Resolution) -> (tinio_lifecycle::Options, Vec<Seed>)`.

- [x] **Step 1: Write the failing tests** — `the_section_defaults_to_ten_seconds_and_one_hour`, `an_idless_file_rule_is_a_config_error`, `an_invalid_bucket_name_is_a_config_error`, `the_seeds_convert_per_bucket`, `a_read_only_run_builds_no_pass`, `a_disabled_capability_builds_no_pass_and_no_seed`.
- [x] **Step 2: Run and watch them fail.**
- [x] **Step 3: Implement the section, the conversion, the signature change and the arms** — fs joins `lifecycle` with `scanner_and_sweeper`; pg joins it with `pg_pass`; both under the existing stop channel; a read-only run returns before constructing it; a run whose `[s3] lifecycle` is false builds neither the runner nor the seeds, and does not inject the plane's admin (LC23).
- [x] **Step 4: Run green** — `cargo test -p tinio-cli --features lifecycle`; `cargo test -p tinio`; clippy; **Evidence**.
**Evidence:** attempt 2 (attempt 1 was killed by a hung shell read, leaving `config/lifecycle.rs` unwired): the section + root field, the conversion (`config::lifecycle::options`, crate-visible), the one gate (`serving::lifecycle_pass` — `None` for a read-only run and for `[s3] lifecycle = false`), `LifecyclePass` on all three `background` arms with `serving::seeded` (seed once, then `run`), the spawn moved below readiness (LC13), `tinio_api::Options::lifecycle` → `Context::with_lifecycle` in `assemble` (the CLI never holds a `Context`), `run::StorageAdmin` + `lifecycle_admin` injection gated on `capabilities_for`, and the three feature forwards (`tinio-cli` `dep:tinio-lifecycle`, `tinio` `full`, `tinio-e2e` default). Gates: cli 236+112 `--features lifecycle` / 226+111 default, api 60+75 and 46+68 stripped, facade `full` green, traceability 20, clippy on the four crates (both feature sets where they differ), fmt; workspace build and runtime smoke ran green before the clock cap and are Task 12's to re-run. RED by mutation: the two gate legs, the admin leg and both validation legs each fail with their gate/rule removed.
**Evidence:** cli 236+112 `--features lifecycle` / 226+111 default, facade, traceability 20, clippy on four crates, fmt; workspace build and the runtime smoke ran green before the run cap and were not re-run after. Review = OK with notes, no code change; the seed ordering and the `serve.rs` wiring are read-verified and get their first executable proof from Task 12a.

### Task 11: The e2e corpus and the traceability row

**Files:**
- Create: `crates/tinio-e2e/tests/features/lifecycle.feature`
- Create: `crates/tinio-e2e/tests/features/lifecycle_cli.feature`
- Create: `crates/tinio-e2e/tests/steps/lifecycle.rs`
- Modify: `crates/tinio-e2e/tests/steps/mod.rs` (module)
- Modify: `crates/tinio-e2e/tests/traceability.rs` (drop the Task 0 allow-list entry)
- Modify: `crates/tinio-e2e/Cargo.toml` (`lifecycle = ["tinio-server/lifecycle", "tinio-pg?/lifecycle"]`, in the crate's default features so the in-process `@lifecycle` scenarios run in a plain `cargo test -p tinio-e2e`)

**Interfaces:**
- Consumes: Tasks 6, 8, 9, 10; `config_from_tags`.
- Produces: `@FR-042` scenarios tagged `@lifecycle`; the CLI legs tagged `@cli`.

- [x] **Step 1: Write the feature files** — round trip, `DELETE` clears, `NoSuchLifecycleConfiguration`, transitions refused, read-only `AccessDenied`, capability off `NotImplemented`, and one real expiry: `Expiration.Date` in the past, then a bounded poll for the key to vanish.
- [x] **Step 2: Write the steps** — reuse the existing S3 request steps; add only the lifecycle-specific ones (`I put this lifecycle configuration`, `the lifecycle configuration has a rule with ID`, `the key is gone within`).
- [x] **Step 3: Run the default pass and the pg pass** — `cargo test -p tinio-e2e`; `TINIO_E2E_BACKEND=pg TINIO_PG_DSN=… cargo test -p tinio-e2e -- --test-threads=1`; `cargo test -p tinio-e2e --test traceability` with the allow-list entry removed; **Evidence**.
**Evidence:** default run 293 scenarios / 2445 steps green with the corpus excluded; traceability 20 passed with the FR-042 entry removed; `--tags "@lifecycle"` probe 9 scenarios / 42 steps, 0 skipped or undefined (1 passes, 8 fail on the absent surface); clippy and fmt clean. Review = changes requested → fix 1 (the transition document made single-violation, `not @lifecycle` in both CI literals and three README commands, README three fixes, `docstring` de-duplicated, probe arithmetic) → re-review OK.

### Task 12: The verification wave and the docs sync

**Files:**
- Modify: `docs/superpowers/specs/2026-10-18-s3-object-lifecycle-design.md` (status → implemented, if the operator wants it recorded)
- Modify: `specs/001-s3-local-server/spec.md` only if a decision changed a clause (it should not)
- Record (operator ruling 2026-10-18, code review): the out-of-plan work retained beside this feature — the serde-default conformance migration under `crates/tinio-cli/src/config/` and `crates/tinio-server/src/`, the `docs/style.md` compression, `docs/agents/execution.md`, and the `CLAUDE.md` pointer. It is reviewed and kept, not a plan task; this line is its record.

**Interfaces:**
- Consumes: everything above.
- Produces: the gate record.

- [x] **Step 1: Run the workspace gates** — `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --check`; the doc gate; `cargo xtask sqlx check`.
- [x] **Step 2: Run the feature matrix** — `cargo hack check --package tinio-cli --each-feature`; `cargo hack check --package tinio-server --each-feature`; `cargo hack check --package tinio-api --each-feature`; `cargo build -p tinio-cli --no-default-features` (the stripped build must still compile and must not know the verbs).
- [x] **Step 3: Run the database legs** — `cargo test -p tinio-store-pg -p tinio-pg -- --test-threads=4` with `TINIO_PG_DSN`.
- [x] **Step 4: Run the e2e passes** — the default, `TINIO_E2E_BACKEND=mem`, and `@pg`.
- [x] **Step 5: Reconcile the design with what landed** — the module and statement names, the `lifecycles` key type, the pin lines (R31); no translation mirror exists (operator ruling 2026-10-18).
- [x] **Step 6: Evidence** — every command with its tail, one line each.
**Evidence:** `cargo test --workspace` green (3495 passed, 0 failed, 29 ignored on the gate run; re-run after the simplify round green); `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo +nightly fmt --all -- --check` clean; `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps` clean; `cargo xtask sqlx check --dsn …` up to date (167 entries). Feature matrix: `cargo hack check --each-feature` for tinio-lifecycle, tinio-cli, tinio-server, tinio-api and the facade/backends clean; `cargo build -p tinio-cli --no-default-features` clean. Database legs with `TINIO_PG_DSN='postgres://…/tinio?sslmode=disable'`: tinio-pg 20 lib + 6/6/2/2/4/2 integration + 10 doctests, tinio-store-pg 181 lib + 39 integration, 0 failed. E2E: default 302 scenarios / 2535 steps; mem 251 / 2167; pg 254 / 2214; traceability 20 — all passed. Note: the literal DSN without `sslmode=disable` hits the fixture's TLS refusal; the CI command uses the disable form. Second fix round (simplify, 2026-10-18): shared Content-MD5 gate, `tinio-core` default/day constants, `tinio_util::testing::lifecycle_config`, `drain_upload.sql` `ANY($2)`, `Rule::days`, `Config::upsert_rule`, and the one-scan pg pass — the workspace, pg and e2e gates re-ran green.

## Self-review against the spec

- LC1–LC4: Tasks 1–5 (core model, matcher, crate, contract-only engine).
- LC5/LC6: Task 3a. LC19: Task 3b. LC7/LC8/LC9/LC18: Task 6. LC10: Tasks 2 and 5. LC11: Task 10. LC12: Task 5 (stateless pass). LC13/LC22/LC23: Task 10. LC14/LC15: Tasks 8 and 9. LC16: Task 5's summary line. LC17: Tasks 1 and 6. LC20: Task 0. LC21: Task 7.
- The spec's §Testing rows map to Task 3b (contract), Task 5 (engine), Task 6 (S3 surface), Task 11 (e2e/CLI), Task 10 (configuration), Task 7 (sweeper).
- Open: nothing. Anything discovered mid-implementation that contradicts a decision goes back to the spec first, in the same round.
