# Tiered Storage and ILM Transitions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve one root with a mem cache, fs as the warm/durable tier and pg as the cold tier, moving objects by heat, fs capacity and lifecycle rules, with third-party clients and e2e coverage, while existing backends stay unchanged outside tier mode.

**Architecture:** A new `tinio-tier` crate implements `Storage` as a decorator over `Arc<FsStorage>`. `ColdInfo` in `tier.redb` is the read authority for pg-placed objects; the fs row is a mirror reached through one row-only `tier::Rows` seam. Cold objects have no fs file. Listings merge a key-ordered fs row scan with the cold index. Moves copy outside the locks and revalidate under them; promotion publishes through the fs content plane (ownership/mode included).

**Tech Stack:** Rust 2024, redb, tokio, async-trait, s3s dto, tinio-fs (tier + versioning), tinio-pg, tinio-store-pg, tinio-core, tinio-ops, tinio-lifecycle, tinio-util metrics, cucumber, aws-cli/rclone/mc/boto3, the bench-ab harness.

**Spec:** `docs/superpowers/specs/2026-10-24-tiered-storage-design.md` — T1–T28 bind; where this plan and the spec disagree, the spec wins and the plan is corrected, never the code.

## Global Constraints

- English only in code, docs, commits, PRs; chat with the operator is Chinese.
- No AI attribution in commits or PRs; ask before every git write; leave the index untouched.
- Agents never run `git add` or commit. Each task's Checkpoint is a file inventory, not a command. The operator makes one final commit after Task 28.
- `Cargo.toml` carries no comments. Build/scratch dirs live under `target/`; one-off scratch lives outside the repo.
- Business code never panics; errors use the crate's `Error` and thiserror.
- Async tests stay async; DB legs run with `--test-threads=4` and the pg fixture; `TINIO_PG_REQUIRED=1` fails closed. One cargo test filter per invocation.
- Docs are compressed prose, no hard wraps; one fact, one home.
- Plan checkboxes are ticked by the dispatcher in the same round; subagents never edit this plan.
- Phase gates: each phase ends with its suites green and an operator review.
- Existing-crate changes are limited to T26: core additive, one fs seam behind `tinio-fs/tier`, one scanner field, three server changes, lifecycle additive, CLI wiring. pg/store-pg/mem unchanged.
- Fault injection uses a `testing` feature; `from_parts` and other helpers behind it.

## Review Focus

- **A remembered body outlives a mutation:** every commit, overwrite, delete, copy/rename, MPU complete, transition and bucket delete invalidates the mem and buffer entry. Pinned in Tasks 6, 10 and 11.
- **A copy finishes over a newer write:** the copy is snapshotted and revalidated under the per-key write lock; a mismatch discards it. Pinned in Task 12.
- **A cold metadata mirror diverges:** promote/repair restore the **full row** from `ColdInfo` (tags/ACL/checksum/owner), not facts only. Pinned in Tasks 2 and 12.
- **A sync PUT materialises the whole body:** sync streams through the fs stage; only bounded batch writes use mem and spill past the cap. Pinned in Task 9.
- **A non-tier backend changes behavior:** `STANDARD_IA` is ignored and lifecycle transitions keep today's refusal. Pinned in Tasks 17 and 19.
- **A versioned bucket loses expiration:** only transitions skip versioned/suspended buckets. Pinned in Task 18.

---

## Phase 1 — Contracts and the fs seam

### Task 1: Core class, tier capability and metrics scan

**Files:**
- Create: `crates/tinio-core/src/storage/class.rs`
- Modify: `crates/tinio-core/src/storage/mod.rs`
- Modify: `crates/tinio-core/src/storage/error.rs`
- Modify: `crates/tinio-server/src/backend/errors.rs`
- Modify: `crates/tinio-util/src/metrics.rs` (`Backend::Tier` and the label match)
- Test: `crates/tinio-mem/tests/tier_defaults.rs` (new); `crates/tinio-util` metric tests

**Interfaces:**
- Produces: `Class { Standard, StandardIa }` with `STANDARD`/`STANDARD_IA` wire spellings (`#[serde(rename_all = "SCREAMING_SNAKE_CASE")]`) and `FromStr`.
- Produces: `#[async_trait] Storage` with defaults `supports_tiers() -> bool` (false), `storage_class(&bucket, &key) -> Result<Class, Self::Error>` (`Standard`), `transition(&bucket, &key, Class, SystemTime)` (`Err(Error::NoTiers.into())`), and `metrics_scan(&self, ListObjectsParams) -> Result<ObjectListing, Self::Error>` (default `self.list_objects(params)`).
- Produces: `storage::Error::NoTiers`; `tinio_util::metrics::Backend::Tier`.

- [x] **Step 1: Write failing tests**

`a_non_tier_backend_defaults_to_standard_and_refuses_transition`, `metrics_scan_defaults_to_list_objects`, `the_tier_backend_label_is_available`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-mem --test tier_defaults`
Expected: FAIL.

- [x] **Step 3: Implement**

Add `Class`; annotate `Storage` with `#[async_trait]` and the four defaults; add `NoTiers`; map it in `backend/errors.rs` to `NotImplemented`; add `Backend::Tier` and update every exhaustive match/`as_str`. Record the additive `NoTiers` semver exemption in the plan's evidence.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-mem --test tier_defaults`
Run: `cargo test -p tinio-util metrics`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-core/src/storage/{class,mod,error}.rs`, `crates/tinio-server/src/backend/errors.rs`, `crates/tinio-util/src/metrics.rs`, `crates/tinio-mem/tests/tier_defaults.rs`.

Evidence: `cargo test -p tinio-mem --test tier_defaults` 3 passed; `cargo test -p tinio-util metrics` 2 passed. `storage::Error::NoTiers` is an additive variant on the facade-re-exported enum; the only exhaustive match is `tinio-server/src/backend/errors.rs` and it was updated — semver exemption recorded per spec T24. Plus `crates/tinio-core/src/lib.rs` re-exports `Class`.

### Task 2: fs row-only tier seam

**Files:**
- Create: `crates/tinio-fs/src/tier.rs`
- Modify: `crates/tinio-fs/Cargo.toml` (a `tier` feature, no default)
- Modify: `crates/tinio-fs/src/lib.rs`
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`tier_rows()`)
- Test: `crates/tinio-fs/src/tier.rs` inline tests

**Interfaces:**
- Produces: `tier::Facts { etag, size, mtime }`.
- Produces: `tier::Rows` with `info(bucket, key) -> Option<object::Info>`, `facts(bucket, key) -> Option<tier::Facts>`, `set_row(&object::Info, mtime)`, `parts(bucket, key) -> Vec<ObjectPart>`, `scan(bucket, prefix, after, limit) -> Vec<object::Info>`.
- `set_row` restores ETag, size, tags, checksum, owner and ACL, stats the file for the identity when one exists, and preserves the version slot, part rows and lock rows.

- [x] **Step 1: Write failing tests**

`info_and_facts_read_without_content`, `set_row_restores_tags_acl_and_checksum`, `set_row_preserves_parts_and_the_version_slot`, `scan_is_prefix_after_limit_bounded`, `scan_skips_delete_markers`, `a_delete_marker_row_round_trips`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-fs --features tier tier::tests`
Expected: FAIL.

- [x] **Step 3: Implement**

A cloneable handle over the fs metadata store. `scan` uses the key-ordered row table with prefix/after/limit; it never walks files. No file I/O lives in this module.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-fs --features tier tier::tests`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-fs/src/tier.rs`, `crates/tinio-fs/Cargo.toml`, `crates/tinio-fs/src/lib.rs`, `crates/tinio-fs/src/backend/mod.rs`.

Evidence: 6 passed. Shipped signature is `set_row(&Name, &Info, SystemTime)` — `object::Info` carries no bucket; downstream tasks code against that. `info`/`facts` return `None` for a delete-marker row while `set_row` preserves the marker flag and version slot (pinned by `a_delete_marker_row_round_trips`); downstream tasks rely on that semantic.

### Task 3: Scanner reconciliation switch

**Files:**
- Modify: `crates/tinio-fs/src/scanner.rs`
- Modify: `crates/tinio-cli/src/commands/start/serving.rs`
- Modify: `crates/tinio-cli/src/commands/start/serve.rs` (the call site)
- Modify: `crates/tinio-cli/src/commands/start/serve_tests.rs`
- Modify: `crates/tinio-fs/src/version/tests.rs` (the complete literal)
- Modify: `crates/tinio-e2e/tests/steps/common.rs` (the complete literal)
- Test: `crates/tinio-fs/src/scanner.rs` inline tests

**Interfaces:**
- Produces: `ScannerOptions.reconcile_objects: bool`; `scanner_options(section, access, reconcile_objects)` and `scanner_options_for(resolution, access, reconcile_objects)`. No `Default` impl (the `[scanner]` defaults stay owned by `config::scanner`).

- [x] **Step 1: Write failing tests**

`reconcile_off_leaves_a_stale_row_alone`, `reconcile_off_still_sweeps_temp_and_uploads`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-fs scanner`
Expected: FAIL.

- [x] **Step 3: Implement**

Add the field and thread it; every existing call site passes `true`, except the tier arm in Task 21. Update all complete literals.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-fs scanner`
Run: `cargo test -p tinio-cli start`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-fs/src/scanner.rs`, `crates/tinio-cli/src/commands/start/{serving,serve,serve_tests}.rs`, `crates/tinio-fs/src/version/tests.rs`, `crates/tinio-e2e/tests/steps/common.rs`.

Evidence: `cargo test -p tinio-fs scanner` 22 passed; `cargo test -p tinio-cli start` 30 passed; `cargo check -p tinio-e2e --tests` clean. RED evidence unavailable: the first implementation predates a session shutdown; the GREEN run and the pre-existing scanner suite are the regression control. Task 21 watch item: the stale-bucket stage drains rows for a bucket whose directory vanished, so tier mode must keep a bucket directory alive while only its cold rows remain.

---

## Phase 2 — The tier crate

### Task 4: `tinio-tier` crate and runtime config

**Files:**
- Create: `crates/tinio-tier/Cargo.toml`, `src/lib.rs`, `src/config.rs`, `src/error.rs`
- Modify: `Cargo.toml`
- Test: inline config/error tests

**Interfaces:**
- Produces: `Config`, `Mem`, `Fs`, `FlushMode`; `Config::validate()`; `pub use tinio_store_pg::PgStore as ColdStore`; the crate defines `[features] testing = []`, which gates `from_parts`, `FailPoint` and the e2e `tier` feature.
- Dependencies: `tinio-core`, `tinio-fs` (`features = ["tier", "versioning"]`), `tinio-pg`, `tinio-store-pg`, `tinio-util`, `redb`, `tokio`, `async-trait`, `futures`, `bytes`, `thiserror`, `tracing`, `serde`, `humantime-serde`, `garde`, target-gated `rustix`/`windows-sys`.
- Config keys: `capacity_bytes`, `max_object_bytes`, `flush_mode`, `batch_bytes`, `batch_interval`, `heat_window`, `promote_reads`, `stats_flush`, `high_water_percent`, `low_water_percent`, `demote_idle`, `dwell`, `scan_interval`, `move_concurrency`, `index_compact_percent`, `gc_grace` (the runtime fields; no defaults live here).
- The TOML schema and defaults are owned by the CLI section in Task 20; this runtime config is built from explicit values.

- [x] **Step 1: Write failing tests**

`low_watermark_below_high`, `caps_do_not_exceed_capacity`, `move_concurrency_at_least_one`, `error_projects_to_storage`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier config`
Expected: FAIL.

- [x] **Step 3: Implement**

Create the crate with the dependency list and the runtime types; the TOML schema lives in the CLI (Task 20). `Error` wraps fs/storage/index/cold/config errors and projects to `storage::Error`.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier config`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`Cargo.toml`, `crates/tinio-tier/{Cargo.toml,src/lib.rs,src/config.rs,src/error.rs}`.

Evidence: `cargo test -p tinio-tier config` 3 passed; error test 1 passed; clippy `-D warnings` and fmt clean. Review fix round 1 addressed two Important findings: `capacity_bytes` is the cache+write-buffer budget (T9), and the crate's lint is `unsafe_code = "deny"` so Task 13 can use a scoped `#[expect(unsafe_code)]` for the Windows capacity FFI (T1).

### Task 5: Index: placement, `ColdInfo`, warm and heat

**Files:**
- Create: `crates/tinio-tier/src/index.rs`
- Test: inline

**Interfaces:**
- Produces: `ColdInfo` and `Placement { tier, state: Live|Copying|Deleting, cold, mtime }`.
- Produces: `Index::{open, open_in_memory, root_id, set_root_id, placement, set_placement, clear, scan_placement, warm_insert, warm_remove, scan_warm, record_read, flush_heat, scan_heat, expire_heat, remove_bucket, compact}`.
- `scan_placement`/`scan_warm`/`scan_heat` are keyed range scans; the index is created `0600` with a format gate; compaction runs at construction.

- [x] **Step 1: Write failing tests**

`placement_round_trips_and_is_durable`, `placement_carries_cold_info`, `scan_placement_is_prefix_after_limit`, `warm_scans_oldest_first`, `warm_removes_on_demote`, `heat_rows_expire`, `the_root_id_round_trips`, `a_format_mismatch_is_refused`, `the_file_is_owner_only`, `compaction_reopens_clean`, `read_only_open_refuses_writes`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier index`
Expected: FAIL.

- [x] **Step 3: Implement**

Tables: `placement` `(bucket,key)`, `warm` `(mtime,bucket,key)`, `heat` `(bucket,key)`, `meta`. Placement uses immediate durability; the file is created with `0600` and a format gate; `compact` runs with exclusive access at construction.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier index`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/index.rs`.

Evidence: `cargo test -p tinio-tier index` 11 passed (`the_file_is_owner_only` is Unix-gated per the 0600 ruling); crate 15 unit + 36 doctests; `RUSTDOCFLAGS="-D warnings" cargo doc -p tinio-tier --no-deps` clean; clippy `-D warnings` clean. Fix round 1 added the constitution's runnable examples on 17 public items and the read-only `record_read` no-op. Deferred: nightly-rustfmt violations in some doc examples (CI gate; Task 28 must run `cargo +nightly fmt --check`), heat sharding, pending-heat invalidation, `scan_heat` pagination, format-2 gate ordering.

### Task 6: Mem cache, shared budget, heat queue

**Files:**
- Create: `crates/tinio-tier/src/cache.rs`, `src/heat.rs`
- Test: inline

**Interfaces:**
- Produces: `Cache::new(budget, max_object_bytes)`, `get`, `insert`, `remove`, `take`, `len_bytes`.
- Produces: `Heat::record_read` on every GET (mem hits included), `score`, `drain`, `promote_queue() -> Vec<(bucket,key)>`, `expire`.
- One budget partitioned between the cache and the write buffer.

- [x] **Step 1: Write failing tests**

`cache_evicts_lru_under_the_budget`, `an_over_cap_object_is_not_inserted`, `a_mem_hit_records_heat`, `the_promotion_queue_fires_at_threshold_once`, `expired_heat_is_dropped`, `cache_and_buffer_share_one_budget`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier cache`
Expected: FAIL.

- [x] **Step 3: Implement**

LRU with a byte budget; sharded heat counters; a bounded queue fed at threshold crossing; a shared budget object.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier cache`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{cache,heat}.rs`.

Evidence: 21 unit tests (15 prior + 6 new) and 57 doctests pass; clippy `-D warnings`, rustdoc `-D warnings`, nightly rustfmt clean. The `cache` filter does not select `heat::tests`; the report ran `cargo test -p tinio-tier heat` as the second filter. The incidental `index.rs` change is doc-comment formatting only (0 non-`///` lines). Task 7 owns the cache→heat wiring on a mem hit.

### Task 7: Read path, reconciliation and cold elements

**Files:**
- Create: `crates/tinio-tier/src/storage.rs`
- Modify: `crates/tinio-tier/src/lib.rs`
- Test: `crates/tinio-tier/tests/read_path.rs`

**Interfaces:**
- Produces: `#[derive(Clone)] TierStorage<C: ObjectContent>` with `Arc<FsStorage>`, `tier::Rows`, cold, `Arc<Index>`, cache, config, locks.
- Produces: `open`, `from_parts` (`#[cfg(feature = "testing")]`), `fs`, `index`, `cache`.
- Intercepts: reads, staged writes, commits, deletes, copy/rename, `copy_part`, complete, versioning, the tag/ACL/parts reads, listings, bucket delete, version-addressed null.
- Overrides `supports_tiers`, `storage_class`, `transition`; `metrics_scan` is implemented with the listing merge in Task 8.

- [x] **Step 1: Write failing tests**

`a_warm_get_delegates`, `a_cold_get_streams_pg`, `cold_info_comes_from_cold_info`, `a_missing_row_means_deleted`, `a_file_with_a_different_etag_means_fs_won`, `a_file_with_the_same_etag_is_removed_later`, `a_missing_pg_copy_errors`, `a_length_mismatch_errors`, `a_full_get_fills_mem`, `a_range_get_does_not_fill_mem`, `a_mem_hit_records_heat`, `cache_is_invalidated_on_every_mutation`, `a_null_version_read_matches_the_placement`, `cold_tags_acl_and_parts_work_without_content`, `storage_class_is_ia_for_pg`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier --test read_path`
Expected: FAIL.

- [x] **Step 3: Implement**

Read under the per-key read lock; cold `Info` from `ColdInfo`; reconcile `facts` + file stat (missing row → deleted; different ETag → fs; same ETag → crash-before-removal); verify pg presence/length in `open`; fill mem only on a full GET; null version must match; cold tags/ACL/parts from `ColdInfo`/`tier::Rows.parts`; invalidate mem/buffer on mutations. Forward the rest.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier --test read_path`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{storage,lib}.rs`, `crates/tinio-tier/tests/read_path.rs`.

Evidence: `cargo test -p tinio-tier --test read_path` 15 passed; crate 21 unit + 60 doctests; clippy/rustdoc/nightly rustfmt clean. The per-key read guard is moved into the body stream (`stream::unfold` state holds it), so T14's read side spans the stream. Fix round 1 ruled that fs-won invalidates mem only and leaves the placement and pg copy to Task 10/12; the old `abandon` is removed.

### Task 8: Bounded listing merge

**Files:**
- Create: `crates/tinio-tier/src/listing.rs`
- Modify: `crates/tinio-tier/src/storage.rs`
- Test: `crates/tinio-tier/tests/listing.rs`

**Interfaces:**
- Produces: `list_objects_merged`, `list_object_versions_merged`, `metrics_scan` override.
- The merge scans `tier::Rows.scan` and `Index.scan_placement` with prefix/after/limit, prefers the index for a duplicate key, groups delimiters before cutting, and never repeats a key. Cold keys appear as null versions in `ListObjectVersions`.

- [x] **Step 1: Write failing tests**

`warm_and_cold_keys_merge_in_order`, `prefix_and_delimiter_cross_the_boundary`, `pagination_spans_pages`, `a_duplicate_key_resolves_to_cold`, `cold_keys_appear_in_list_object_versions`, `the_metrics_scan_does_not_flush`, `an_empty_bucket_is_empty`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier --test listing`
Expected: FAIL.

- [x] **Step 3: Implement**

Take the bucket read gate; page the row scan and the cold range; merge/dedupe; group/cut. `metrics_scan` counts without flushing.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier --test listing`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/listing.rs`, `crates/tinio-tier/src/storage.rs`, `crates/tinio-tier/tests/listing.rs`.

Evidence: `cargo test -p tinio-tier --test listing` 8 passed (the brief's seven plus `a_rolled_prefix_is_never_split_across_pages` added in fix round 1, which drives a delimiter cut across both planes and pins the `dir/` roll-up marker). The merge emits a plane's front row only after the other plane is read through its key; `metrics_scan` bypasses the single no-op `before_client_listing` hook Task 11 fills.

### Task 9: Streaming stage and sync commit

**Files:**
- Create: `crates/tinio-tier/src/stage.rs`
- Modify: `crates/tinio-tier/src/storage.rs`
- Test: `crates/tinio-tier/tests/stage.rs`

**Interfaces:**
- Produces: `TierStaged { Mem { bytes, info }, Fs(FsStaged) }`.
- `stage_body`: sync/non-batch delegates to `FsStorage::stage_body` (streaming); batch-eligible reads a bounded buffer (≤ `max_object_bytes`) and spills to the fs stage past the cap, forwarding the checksum tee.
- `commit_object`: sync writes through the fs commit; batch decides buffering (Task 11).

- [x] **Step 1: Write failing tests**

`a_sync_put_streams_and_does_not_buffer`, `a_large_batch_put_spills_to_the_fs_stage`, `a_checksum_survives_the_mem_path`, `an_aborted_stage_leaves_nothing`, `a_classed_put_is_never_batch_eligible`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier --test stage`
Expected: FAIL.

- [x] **Step 3: Implement**

The bounded/spill staging and the commit split; the sync commit uses `fs.commit_object` and resets the destination.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier --test stage`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{stage,storage}.rs`, `crates/tinio-tier/tests/stage.rs`.

Evidence: `cargo test -p tinio-tier --test stage` 5 passed; read_path 15, listing 8, lib 21, docs 63 passed; clippy, rustdoc `-D warnings`, nightly fmt clean. `TierStaged::Fs` holds `<FsStorage as ObjectOps>::StagedBody` because that type's constructor is `pub(crate)` in tinio-ops (ruled acceptable). The destination reset stays with Task 10; Task 9 invalidates the mem cache only. Review evidence was regenerated from the pre-Task-9 tree after the first package's base was found to include this task's files.

### Task 10: Write operations, placement reset and invalidation

**Files:**
- Modify: `crates/tinio-tier/src/storage.rs`, `src/mover.rs`
- Test: `crates/tinio-tier/tests/write_path.rs`

**Interfaces:**
- Overrides: `delete_object`, `delete_object_version` (the null version routes to `delete_object`), `copy_object`, `rename_object`, `copy_part`, `complete_multipart_upload`, `set_bucket_versioning`, tag/ACL writes, `delete_bucket`.
- Every mutation resets the destination, removes its pg copy, and invalidates mem/buffer. `copy_part` uses a promoted source. Versioning enable/suspend holds the bucket write gate, flushes, promotes all and rechecks. Delete is tombstoned under the write lock with rollback; bucket delete checks the index and removes the pg namespace.

- [x] **Step 1: Write failing tests**

`overwrite_returns_to_fs_and_invalidates_cache`, `delete_removes_row_index_pg_and_cache`, `a_null_version_delete_clears_placement_and_pg`, `copy_of_a_cold_source_leaves_it_warm`, `rename_resets_both_keys`, `upload_part_copy_from_cold_copies_real_bytes`, `complete_over_cold_resets_placement`, `versioning_gate_promotes_all_and_rechecks`, `delete_bucket_flushes_and_refuses_cold`, `a_failed_delete_rolls_the_tombstone_back`, `a_tag_write_updates_cold_info_and_the_row`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier --test write_path`
Expected: FAIL.

- [x] **Step 3: Implement**

The overrides with the reset/invalidate rules, the tombstone lock/rollback, `copy_part` via a promoted source, and the versioning gate. `delete_object_version` with `version: None` routes to `delete_object`; any `Some(id)` delegates. Controller ruling: `transition` is deferred to Task 12, which owns `demote`; this task does not define it.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier --test write_path`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{storage,mover}.rs`, `crates/tinio-tier/tests/write_path.rs`.

Evidence: `cargo test -p tinio-tier --test write_path` 14 passed (the brief's eleven plus three fix-round tests); read_path 15, listing 8, stage 5, lib 21, docs 67 and the rustdoc gate green. `mover.rs` was not created: the mutation helpers are private `TierStorage` methods that Task 12 extends. Fix round 1: `copy_part` now promotes a cold destination, then resets it to fs and removes its pg copy (test `copy_part_from_a_cold_source_resets_a_cold_destination`); a failed delete whose row is already gone keeps a `Deleting` tombstone for GC (test `a_failed_delete_after_the_row_leaves_a_completable_tombstone`); the `Suspended` gate leg is pinned. Deferred minor: a row-read `Err` in `settle_failed_delete` is folded into the absent-row branch.

### Task 11: Batch buffer

**Files:**
- Create: `crates/tinio-tier/src/buffer.rs`
- Modify: `crates/tinio-tier/src/storage.rs`
- Test: `crates/tinio-tier/tests/batch.rs`

**Interfaces:**
- `Buffer` with the shared budget; `commit` inserts, `flush` writes, `remove`; the hard cap and single-flight.
- Flush: fs commit, set the file mtime to the buffered time, `set_row` with the buffered `Info`, clear the destination placement/pg.

- [x] **Step 1: Write failing tests**

`a_plain_commit_answers_before_the_fs_write`, `flush_preserves_last_modified`, `flush_resets_placement`, `list_flushes_first`, `list_object_versions_flushes_first`, `a_buffered_key_is_flushed_before_element_or_byte_ops`, `deleting_a_buffered_key_delegates`, `the_buffer_cap_stops_acknowledging`, `shutdown_drains`, `the_metrics_scan_does_not_flush`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier --test batch`
Expected: FAIL.

- [x] **Step 3: Implement**

The buffer and flush as above; every element/byte operation flushes the key first; a classed PUT is never buffered.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier --test batch`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{buffer,storage}.rs`, `crates/tinio-tier/tests/batch.rs`.

Evidence: `cargo test -p tinio-tier --test batch` 13 passed (ten named plus the folder-marker and two admission tests); read_path 15, listing 8, stage 5, write_path 14, moves 18 and 87 doctests green; clippy/fmt clean. Review found one Critical: a folder-marker PUT was buffered, acknowledged and permanently unflushable (the flush tried to set the mtime of a directory), wedging every client listing and the shutdown drain. Fix round 1 makes `plain_buffered` refuse folder markers so a marker commits synchronously through the fs marker branch (`ETag::EMPTY`, directory, no row), and pins the two `Admission::Refused` fallbacks. Deferred: drain has no failure isolation; `Buffer::commit`/`remove` public via `tier.buffer()`; `batch_interval` unused and `shutdown` only called by tests until Task 25, which needs a public flush entry point; `Buffer::commit(bucket, entry)` signature.

### Task 12: Demote, promote, delete and crash order

**Files:**
- Create: `crates/tinio-tier/src/mover.rs`
- Modify: `crates/tinio-tier/src/storage.rs`
- Test: `crates/tinio-tier/tests/moves.rs`

**Interfaces:**
- `MoveSummary`, `GcSummary`; `demote`, `promote`, `delete_tombstone`, `gc`.
- Demote: snapshot → placement `copying` → copy → under the lock revalidate → `pg`; the fs file is left to GC.
- Promote: snapshot → copy to the fs stage → revalidate placement/ETag under the lock → `publish` (ownership/mode) → set the file mtime to `ColdInfo.last_modified` → `set_row` → fs placement → remove pg.
- Delete: tombstone under the lock → fs delete → index clear → pg remove; rollback on failure.
- GC: finish tombstones with an absent row after `gc_grace`; remove a pg-placed file after the grace with no readers; never auto-delete unplaced pg content.

- [x] **Step 1: Write failing tests**

`demote_revalidates_and_finishes`, `a_concurrent_overwrite_aborts_the_demote`, `promote_preserves_composed_etag_and_last_modified`, `a_concurrent_overwrite_aborts_the_promote`, `a_deleted_key_is_not_resurrected`, `gc_finishes_a_tombstone_after_the_grace`, `gc_finishes_a_complete_copying_placement_after_the_grace`, `gc_reverts_a_partial_copying_placement`, `verify_reports_an_unresolved_copying`, `gc_removes_a_pg_placed_file_with_no_readers`, `gc_never_auto_deletes_unplaced_pg`, `an_in_flight_read_does_not_see_a_torn_file`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier --test moves`
Expected: FAIL.

- [x] **Step 3: Implement**

The move state machine, snapshots/revalidation, fs publish promotion, `set_row` full restore, tombstone rollback, and GC grace/readers. After the grace, a `copying` placement is completed when the pg content is complete and the fs snapshot matches; otherwise the partial copy is discarded and the placement reverts to warm; unresolved cases are reported by `verify`.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier --test moves`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{mover,storage}.rs`, `crates/tinio-tier/tests/moves.rs`.

Evidence: `cargo test -p tinio-tier --test moves` 20 passed (the brief's twelve plus the fix-round tests); mover 11, write_path 14, batch 13, read_path 15, listing 8, stage 5, pg_plane 8, lib 21, 100 doctests; clippy/workspace/nightly fmt clean. Fix round 1: promote delegates to `promote_locked` (T5 fs-won revalidation, stale row never restored) with a crash-state test; the demote stages outside the locks and publishes+settles inside the per-key write lock after revalidating placement/row/versioning, with a losing-demote test; `transition` flushes before reading the row. The bucket gate's shared side is taken at finalize (the write side serialized distinct keys and broke Task 13's overlap test); same-key exclusion is the per-key `try_write`. `InvalidRequest` is a second additive core-error semver exemption, now recorded in Task 28's evidence with `NoTiers` and the `Placement.mtime` overload. Remaining Minors are parked in the SDD ledger.

### Task 13: Mover pass, capacity, heat and GC cadence

**Files:**
- Modify: `crates/tinio-tier/src/mover.rs`
- Create: `crates/tinio-tier/src/capacity.rs`
- Test: `crates/tinio-tier/tests/mover.rs`

**Interfaces:**
- `Mover::new(storage, Arc<dyn Capacity>, concurrency)`; `pass`, `gc`; `Capacity { used_percent }` with `VolumeCapacity` and a fake.
- Capacity pops `scan_warm` ascending; heat pops the promotion queue; dwell; concurrency; folder markers skipped; GC on its own cadence. A `warm_ready` flag gates capacity candidate selection until the post-bind rebuild finishes; reads and heat keep working meanwhile.

- [x] **Step 1: Write failing tests**

`capacity_demotes_the_oldest_warm_objects`, `a_never_read_object_is_demoted`, `dwell_blocks_immediate_re_promotion`, `heat_promotes_after_dwell`, `the_warm_table_is_rebuilt_after_restart`, `the_heat_queue_is_restored_after_restart`, `a_versioned_or_suspended_bucket_is_never_moved`, `a_folder_marker_is_never_moved`, `concurrency_two_moves_distinct_keys`, `gc_runs_on_the_configured_cadence`, `a_failed_move_leaves_placement_unchanged`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier mover`
Expected: FAIL.

- [x] **Step 3: Implement**

Warm-candidate capacity, heat queue promotion, dwell, skip rules, concurrency, GC cadence. After bind, rebuild the warm table from the fs row scan and restore the promotion queue from heat rows inside the window.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier mover`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{mover,capacity}.rs`, `crates/tinio-tier/tests/mover.rs`.

Evidence: `cargo test -p tinio-tier --test mover` 14 passed (the brief's eleven plus three tests); moves 20, write_path 14, batch 13, lib 22, 105 doctests; clippy/check/fmt clean on the host (the Unix `VolumeCapacity` arm needs the WSL/Linux CI leg). Fix rounds 1–3: dropped the redundant Unix casts; made the warm-candidate table maintained on every `Fs/Live` establishment via a keyed `warm_replace` (reverse `WARM_KEY` map, one-time backfill+dedupe at writable open, `warm_insert` delegates), so post-startup writes are capacity candidates and rewrites cannot leave stale rows; GC due/stamp at-most-once; `finish_tombstone` retires the warm row; `row_survives` propagates a read error. New `WARM_KEY` table under FORMAT_VERSION 1 with a pre-release backfill documented. Deferred: `demote_idle` has no consumer in this plan (follow-up); `flush_stats` draining can starve threshold crossing; dwell-deferred promotions expire when `dwell > heat_window`; same-pass promote→demote thrash; `next_candidates` head rescans; some coverage gaps; doc nits.

### Task 14: pg cold plane, ownership, root identity

**Files:**
- Create: `crates/tinio-tier/src/pg.rs`
- Modify: `crates/tinio-tier/src/lib.rs`
- Test: `crates/tinio-tier/tests/pg_plane.rs`

**Interfaces:**
- `pg::connect(&tinio_pg::Config, AccessMode) -> Result<ColdStore, Error>` mapping `AccessMode`→`tinio_pg::Access`; `TierStorage::open_pg`; the reserved `tinio-tier-root` namespace.
- Every S3 operation on the reserved name is refused; conversion refuses a root already holding it; GC/repair exclude it; `--adopt` matches the root ID; `from_parts` (testing) skips ownership and the root check.

- [x] **Step 1: Write failing tests**

`a_cold_round_trip_survives_restart`, `a_bucket_namespace_is_created_on_first_demote`, `ownership_refuses_a_second_instance`, `a_fresh_index_over_non_empty_pg_refuses`, `adopt_matches_the_root_id`, `adopt_refuses_a_foreign_root_id`, `the_reserved_namespace_is_hidden_and_protected`, `pg_unreachable_errors`.

- [x] **Step 2: Run and confirm the failure**

Run with the fixture: `docker compose -f tools/pg/docker-compose.yml up -d`, `export TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable'`, `TINIO_PG_REQUIRED=1 cargo test -p tinio-tier --test pg_plane -- --test-threads=4`.
Expected: FAIL.

- [x] **Step 3: Implement**

Connect/own, lazy namespaces, the reserved namespace and its refusals, the root ID, the testing constructor.

- [x] **Step 4: Run the tests**

Run: `TINIO_PG_REQUIRED=1 cargo test -p tinio-tier --test pg_plane -- --test-threads=4`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/{pg,lib}.rs`, `crates/tinio-tier/tests/pg_plane.rs`.

Evidence: all 8 pg_plane legs pass against the live fixture; the full tinio-tier suite is 71 tests + 67 doctests; clippy `-D warnings` and fmt clean; no scratch DB leaked. Review Approved with no fixes. Extra files: `storage.rs`, `error.rs`, `Cargo.toml` (secrecy, uuid, `tinio-store-pg/testing`) — minimal and disclosed. Read-only index/pg opening is Task 15's (this task's `open_pg` still opens the index read-write); `pg::is_reserved` + `ROOT_NAMESPACE` are the seams GC/repair (Tasks 12/23) consume.

### Task 15: Construction, marker, read-only

**Files:**
- Modify: `crates/tinio-tier/src/storage.rs`
- Test: `crates/tinio-tier/tests/open.rs`

**Interfaces:**
- Index `<root>/.tinio/tier.redb` and marker `<root>/.tinio/tier.mode`, both owner-only; the marker is created and fsynced first; both record the root ID.
- Read-only opens the index/pg read-only, starts no tasks, performs no repair/refresh; a missing index refuses.

- [x] **Step 1: Write failing tests**

`the_marker_precedes_the_index`, `a_marker_or_index_present_refuses_a_non_tier_start`, `read_only_writes_nothing_and_serves_cold`, `read_only_without_an_index_refuses`, `compaction_runs_at_construction`, `a_marker_read_error_fails_closed`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-tier --test open`
Expected: FAIL.

- [x] **Step 3: Implement**

Marker/index ordering and checks, read-only behavior, construction compaction, fail-closed reads.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-tier --test open`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/src/storage.rs`, `crates/tinio-tier/tests/open.rs`.

Evidence: all six named tests pass; tier suites and `pg_plane` 8/8 green; clippy `-D warnings` and rustfmt clean. The marker is 16 raw UUID bytes, written (fsynced + parent sync) before the index; writable construction reconciles marker/index identity and refuses a mismatch; `open_pg` writes the marker first and resyncs after bind; read-only opens index/pg/fs read-only, creates and starts nothing, refuses a missing index, and serves cold GET/HEAD/listing from `ColdInfo`. Seam exported: `is_tier_root`, `refuse_non_tier_root`, `marker_path`, `index_path`. Four new `Error` variants. Deferred minors: the marker is written twice on a fresh pg construction (mint before bind + resync), so a crash in that window needs marker removal; a swallowed index probe error can leave a guessed identity; `.tinio` duplicates `tinio_fs::path::STATE_DIR_NAME` and skips its containment proof; `CorruptMarker` duplicates `CorruptRootId`; the marker is truncated in place; the `open_pg` read-only leg and pg marker are untested.

---

## Phase 3 — S3, lifecycle, CLI and acceptance

### Task 16: Lifecycle transition model

**Files:**
- Modify: `crates/tinio-core/src/lifecycle/model.rs`, `eval.rs`
- Modify every `Rule` literal the compiler names
- Test: `crates/tinio-core/src/lifecycle/{shape_tests.rs,model.rs}`

**Interfaces:**
- `Transition { days | date, storage_class }`; `Rule.transitions` with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`; a transition-only rule is valid; `STANDARD_IA` only; `Days ≥ 30`; past dates accepted; noncurrent refused; `Class` serialises `STANDARD_IA`.

- [x] **Step 1: Write failing tests**

`a_transition_only_rule_is_valid`, `a_standard_target_is_refused`, `a_transition_day_is_at_least_thirty`, `a_past_date_is_accepted`, `an_empty_transitions_list_is_omitted`, `the_class_wire_spelling_is_screaming_snake`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-core lifecycle`
Expected: FAIL.

- [x] **Step 3: Implement and update the literals**

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-core lifecycle`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-core/src/lifecycle/{model,eval,shape_tests}.rs`.

Evidence: `cargo test -p tinio-core lifecycle` 47 passed (six new, 41 pre-existing untouched); lifecycle doctests 35 passed; clippy/nightly rustfmt clean on touched files. Sixteen `Rule` literals across the workspace gained `transitions: Vec::new()` only — no Task 17/18 feature work; the server's `rule_to_dto` still hard-codes `transitions: None` (Task 17 owns the dto mapping) and the runner ignores transitions (Task 18).

### Task 17: Lifecycle gates on every surface

**Files:**
- Modify: `crates/tinio-server/src/backend/lifecycle.rs`
- Modify: `crates/tinio-api/src/lifecycle.rs`
- Modify: `crates/tinio-cli/src/commands/lifecycle/{put,add}.rs`
- Modify: `crates/tinio-lifecycle/src/runner.rs` (seed)
- Test: the corresponding inline tests

**Interfaces:**
- Every write path (S3, management plane, CLI `put`, seed) refuses transitions when `!supports_tiers()` with today's message; on a tier backend the S3/plane/CLI round-trip `STANDARD_IA`.

- [x] **Step 1: Write failing tests**

`a_non_tier_s3_put_refuses_transitions`, `a_non_tier_plane_write_refuses_transitions`, `a_non_tier_cli_put_refuses_transitions`, `a_non_tier_seed_refuses_transitions`, `a_tier_surface_round_trips`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-server lifecycle`
Run: `cargo test -p tinio-api lifecycle`
Expected: FAIL.

- [x] **Step 3: Implement**

The four gates with one message source.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server lifecycle`
Run: `cargo test -p tinio-api lifecycle`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-server/src/backend/lifecycle.rs`, `crates/tinio-api/src/lifecycle.rs`, `crates/tinio-cli/src/commands/lifecycle/{put,add}.rs`, `crates/tinio-lifecycle/src/runner.rs`.

Evidence: server 14, api 10, cli lifecycle 13, seed runner 11 passed; `cargo check -p tinio-cli` with default features is warning-free; broader suites green. Fix round 1 added the `date` leg to the tier round-trip test, the tier-backend `NoncurrentVersionTransition` assertion, and made the CLI gate wiring testable; the ungated `storage::Backend` import was cfg-gated. Extra files: `tinio-cli/src/commands/mod.rs`, `tinio-cli/src/commands/start/run.rs` (a 4-line `StorageAdmin::supports_tiers` override Task 21 must keep), and `tinio-core/src/lifecycle/model.rs` (the shared `TRANSITIONS_NEED_TIERS` constant). Handler-level S3 tier behaviour and CLI tier acceptance are Task 25's `@tier` e2e obligation.

### Task 18: Lifecycle runner

**Files:**
- Modify: `crates/tinio-lifecycle/src/runner.rs`
- Test: `runner_tests.rs`

**Interfaces:**
- Transitions run only for unversioned buckets; versioned/suspended buckets still expire; expiration first; markers skipped; `Options.concurrency`.

- [x] **Step 1: Write failing tests**

`a_due_transition_demotes`, `a_transition_to_the_current_class_is_a_noop`, `a_versioned_bucket_still_expires`, `a_versioned_bucket_skips_only_the_transition`, `expiration_wins_when_both_are_due`, `a_delete_marker_is_never_transitioned`, `transitions_run_with_two_workers`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-lifecycle runner`
Expected: FAIL.

- [x] **Step 3: Implement**

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-lifecycle runner`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-lifecycle/src/{runner,runner_tests}.rs`.

Evidence: `cargo test -p tinio-lifecycle runner` 19 passed; full crate 20 + engine 22 + 11 doctests; clippy/fmt clean. Fix round 1 recorded the `ok` action metric on the success and already-gone arms (`a_transition_records_its_outcome` pins ok +2.0 / error +1.0), corrected two docs, and made `a_versioned_bucket_still_expires` carry a due transition so the gate actually runs. `Options.concurrency` is fed by `[lifecycle] concurrency` (a `usize`, default 1, zero refused at parse; `crates/tinio-cli/src/config/lifecycle.rs`, T22). Deferred: `State::Suspended` gate leg untested; the stop path can drop in-flight `Storage::transition` futures, so Task 12 must make `transition` cancel-safe; clock helpers duplicated from `eval.rs`; no behavioral assertion that the default concurrency stays serial.

### Task 19: S3 storage class

**Files:**
- Modify: `crates/tinio-server/src/backend/{objects,versioning}.rs`
- Test: inline

**Interfaces:**
- `GetObjectAttributes` reports the class; PUT class: non-tier ignores the field (no 501); tier + unknown → `InvalidStorageClass`; tier + versioned/suspended + IA → `InvalidRequest`; tier + plain unversioned IA → sync commit then demote; Copy/MPU ignore.

- [x] **Step 1: Write failing tests**

`attributes_report_ia_for_cold`, `a_non_tier_backend_ignores_standard_ia`, `tier_standard_ia_demotes_before_answering`, `tier_ia_on_a_versioned_or_suspended_bucket_is_refused`, `an_unknown_class_is_invalid`, `copy_and_mpu_ignore_the_class`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-server objects`
Expected: FAIL.

- [x] **Step 3: Implement**

Gate on `supports_tiers()` before any class logic.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server objects`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-server/src/backend/{objects,versioning}.rs`.

Evidence: `cargo test -p tinio-server objects` 50 passed (RED was 46/4); `--lib` 334 passed; nightly rustfmt clean. Only `objects.rs` changed — the version-addressed attributes path shares the handler, and the pre-existing `versioning.rs` test still pins `STANDARD` on a non-tier backend. PostObject does honour the class (s3s populates `PutObjectInput.storage_class`), which matches AWS and is intended. Deferred minors: the write guard is released before the awaited transition, so a concurrent overwrite can leave `GetObjectAttributes` reporting IA while the last write asked STANDARD (class is already mover-unstable; data stays consistent); the versioning gate is a time-of-check read outside the per-key lock (bucket state cannot be fenced there); the inline tier double's doc overstates its delegation; four small coverage gaps.

### Task 20: CLI config for tier

**Files:**
- Modify: `crates/tinio-cli/src/config/storage.rs`, `config/mod.rs`, `config/tests.rs`
- Modify: `specs/001-s3-local-server/contracts/config.md` (the tier key table and the `[storage.pg]` relation; this task owns the config-contract edit)
- Test: config tests

**Interfaces:**
- `Backend::Tier`; `storage::Tier` carrying the sixteen T22 keys as the CLI schema (the plan's `{ mem: tier::Mem, fs: tier::Fs }` shape was superseded: plane views carry no schema, and T22 forbids schema/defaults in the crate); `Tier::runtime()` / `Config::tier_runtime()` builds `tinio_tier::Config`; `[storage.tier]` absent from generated configs; `humantime` durations; the CLI is the only schema/defaults owner; the contract docs carry the key table and `[storage.pg]` relation. `tinio-tier` is a non-optional `tinio-cli` dependency so the named test builds without `--features tier`.
- Defaults: 256 MiB capacity, 4 MiB max object, `sync`, 8 MiB/1s batch, 1h heat window, 3 reads, 10s stats flush, 90/80 watermarks, 1h dwell, 60s scan, concurrency 1, compact 30%, gc grace 5m.

- [x] **Step 1: Write failing tests**

`tier_parses`, `tier_defaults`, `tier_requires_pg`, `tier_durations`, `generated_config_omits_an_absent_section`, `tier_converts_to_runtime`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-cli config`
Expected: FAIL.

- [x] **Step 3: Implement**

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-cli config`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-cli/src/config/*`, `specs/001-s3-local-server/contracts/config.md`.

Evidence: `cargo test -p tinio-cli config` 106 passed; `cargo test -p tinio-cli --doc config` 35 passed; clippy clean. Extra files: `crates/tinio-cli/{Cargo.toml,src/lib.rs}`. Ruling recorded in the SDD ledger: flat `Tier` + `tier_runtime()` (T22 wins), and `tinio-tier` non-optional (the brief's test command builds `tinio_tier::Config` without the feature). Consequence for Task 21: the `tier` feature is wiring-only (`dep:tinio-tier` cannot be used), and the `run.rs` catch-all that currently serves fs for `type = "tier"` must be replaced by the tier arm plus the feature-off refusal.

### Task 21: Start wiring, resolve, ServingBackend

**Files:**
- Modify: `crates/tinio-cli/src/commands/start/{mod,run,serve,serving,reader}.rs`
- Modify: `crates/tinio-cli/src/resolve/{locate,mod}.rs`
- Modify: `crates/tinio-cli/src/sandbox/confine.rs`
- Modify: `crates/tinio-cli/Cargo.toml`, `crates/tinio/Cargo.toml`
- Test: `serve_tests.rs`

**Interfaces:**
- `Served::Tier`; `prepare_tier`; `ServingBackend for TierStorage<ColdStore>`; background joins scanner/sweeper (reconcile off), flush, mover, heat, GC and lifecycle; `Storage::metrics_scan` used by `reader.rs`; the Tier arm in `resolve`; confinement refuses Tier; fs/pg refuse marker **or** index; read-only starts no tasks; feature graph `tier` is wiring-only because `tinio-tier` is a non-optional CLI dependency (Task 20 ruling; `dep:tinio-tier` cannot be used) — it still gates the tier start arm, and `tinio-fs/tier`/`versioning`/`pg` arrive through the tier crate; facade `full`. The `run.rs` catch-all must no longer serve fs for `type = "tier"`.

- [x] **Step 1: Write failing tests**

`a_tier_config_selects_the_tier_arm`, `tier_requires_the_feature`, `resolve_keeps_a_tier_run_out_of_the_pg_arm`, `a_marker_or_index_root_is_refused_by_fs_pg`, `scanner_off_in_tier_mode`, `read_only_starts_nothing`, `tier_under_confinement_is_refused`, `the_metrics_reader_uses_metrics_scan`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-cli --features tier start`
Expected: FAIL.

- [x] **Step 3: Implement**

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-cli --features tier start`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-cli/src/commands/start/*`, `resolve/*`, `sandbox/confine.rs`, manifests.

Evidence: `cargo test -p tinio-cli --features tier start` 43 passed; no-feature `tier_requires_the_feature` 1; resolve and confinement legs 1 each; tier+lifecycle 46; clippy/doc/nightly rustfmt clean. Feature graph needed no manifest edit (Task 20's non-optional `tinio-tier`; `tier = ["pg","versioning"]`), facade `full` includes it. `Served::Tier`, `prepare_tier`, `ServingBackend` (metrics storage, `reconcile_objects() == false`, warm-plane repair, background joins scanner/sweeper/flush/mover/GC/lifecycle, drain after joins), the Tier arm in `resolve`, confinement refusal, marker-or-index refusal for fs/pg, read-only early return, and the metrics reader's `metrics_scan` are all wired; the wrapper's `#[expect(dead_code)]` is gone. Known gap recorded: tier metric families are still not emitted by the tier hot paths (follow-up task). Minors parked: gated test imports, stale confinement comment, an unreachable `tier_selection` fallback, `[s3] max_concurrent_uploads` silently ignored in tier mode (out of file scope), and helper-level tests that do not pin the `run` wiring.

### Task 22: Metrics wrapper and tier families

**Files:**
- Modify: `crates/tinio-cli/src/commands/start/write_metrics.rs`
- Modify: `crates/tinio-util/src/metrics.rs`
- Test: inline

**Interfaces:**
- Concrete `MetricsStorage<TierStorage<ColdStore>>` for all six contracts; `WriteFailureKindOf` for the tier error; the tier families from T23; `metrics_scan` forwarding.

- [x] **Step 1: Write failing tests**

`the_tier_wrapper_forwards_a_read`, `the_tier_wrapper_records_a_tier_failure`, `the_tier_families_register_once`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-cli --features tier write_metrics`
Expected: FAIL.

- [x] **Step 3: Implement**

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-cli --features tier write_metrics`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-cli/src/commands/start/write_metrics.rs`, `crates/tinio-util/src/metrics.rs`.

Evidence: `cargo test -p tinio-cli write_metrics` 4 passed (default regime: the `tier` feature does not exist yet), `cargo test -p tinio-util metrics` 3 passed, `tinio-cli --lib` 245 passed; clippy/fmt clean. The wrapper forwards all four tier hooks (verified 63/63 methods against the pg wrapper); eight T23 families in one `LazyLock`; the tier error classifies ReadOnly/Cap/Io. Fix round 1 changed the forwarding test to compare wrapper vs inner outcomes (survives Task 12), made the failure-counter series test-exclusive, and fixed two docs. Handoff: Task 21 removes the `#[expect(dead_code)]` on the production constructor, keeps `tinio-tier` non-optional, and reconciles family names with Tasks 12/13.

### Task 23: Tier offline commands

**Files:**
- Create: `crates/tinio-cli/src/commands/tier/{mod,verify,repair,downgrade}.rs`
- Modify: `crates/tinio-cli/src/cli.rs`, `commands/mod.rs`, `commands/doctor/mod.rs`
- Test: inline

**Interfaces:**
- `verify` (report-only; exit 1 on findings; `--deep` hashes pg for cold/fs for warm; composed ETags unverified unless recomposed).
- `repair` (`--adopt` root-ID, `--delete-orphans`; rebuild by pg-key scan + row match; no synthetic rows).
- `prepare-downgrade` (promote all, remove index/marker, verify fs-only).
- Doctor refuses marker **or** index; spans for all three.

- [x] **Step 1: Write failing tests**

`verify_reports_a_missing_cold_copy`, `verify_deep_hashes_pg_not_the_fs_file`, `repair_rebuilds_from_rows`, `repair_adopt_requires_the_root_id`, `repair_delete_orphans_is_opt_in`, `prepare_downgrade_leaves_fs_only`, `doctor_refuses_a_marker_or_index_root`.

- [x] **Step 2: Run and confirm the failure**

Run: `cargo test -p tinio-cli --features tier tier`
Expected: FAIL.

- [x] **Step 3: Implement**

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-cli --features tier tier`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-cli/src/commands/tier/*`, `cli.rs`, `commands/mod.rs`, `commands/doctor/mod.rs`.

Evidence: `cargo test -p tinio-cli --features tier tier` 22 passed (the seven named plus five legs added in the fix round); doctor 33, parser 5, tier doctests 10; clippy clean in these files and nightly rustfmt clean. Fix round 1: writable offline verbs now require `is_tier_root(root) || adopt` before any marker/index/pg write (a plain root is refused with a usage error; `--adopt` still recovers a both-records-lost root); `prepare-downgrade` also walks fs rows and refuses an `UNSETTLED` no-file row that the index does not place `Tier::Pg`, leaving index+marker intact. Extra files: `commands/doctor/{checks,tests}.rs`, `run.rs` (Task 21's), `error.rs`. `--deep` reports composed multipart ETags as unverified (T12 limit); tier spans carry booleans only.

### Task 24: Conformance, crash matrix, bench

**Files:**
- Create: `crates/tinio-tier/tests/{conformance,repair}.rs`, `benches/tier.rs` (screen), `src/bin/tier-leg.rs` (the bench-ab leg)
- Modify: `crates/tinio-tier/Cargo.toml` (`[[bin]]`, `[[bench]]`)
- Test: both suites and the bench

**Interfaces:**
- Conformance over fs + pg; crash matrix (copy, copying, placement, file removal, publish, index clear, tombstone) including an in-flight read.
- The bench-ab leg exposes `warm_get`, `cold_get` and `put`, one named op per process; the criterion bench is only a screen and never a verdict.

- [x] **Step 1: Write the tests/bench**

- [x] **Step 2: Run and confirm the failure**

Run: `TINIO_PG_REQUIRED=1 cargo test -p tinio-tier --features testing --test conformance -- --test-threads=4`
Expected: FAIL.

- [x] **Step 3: Implement the fixtures and bench**

- [x] **Step 4: Run**

Run: `TINIO_PG_REQUIRED=1 cargo test -p tinio-tier --features testing --test repair -- --test-threads=4`
Run: `cargo bench -p tinio-tier --bench tier`
Expected: PASS; the criterion readings are a screen only. The verdict comes from `cargo xtask bench-ab` with the two revision leg binaries, one named op (`warm_get`, `cold_get`, `put`) at a time, following `docs/benches.md`.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-tier/tests/*`, `benches/tier.rs`, `src/bin/tier-leg.rs`, `Cargo.toml`.

Evidence: conformance 17/17 and the repair legs 4/4 ran with `TINIO_PG_DSN` + `TINIO_PG_REQUIRED=1` (the report shows the DSN; the pg legs do not silently skip); mem/fs/pg conformance re-validated; `cargo check --profile ci --workspace --all-targets` exit 0; the criterion screen ran (warm_get 6.01 ms, cold_get 2.68 ms, put 18.44 ms — screen only, not a verdict). The shared `assert_conformance` is restored over the tier after draining three metadata-only bodies in `tinio-util`'s harness (the deadlock was harness/T14, not a tier defect), regaining multipart/checksum/ACL coverage; select/conditional checks do not exist in the shared harness, so that part of the review's parenthetical could not be regained. The repair suite lives at `crates/tinio-cli/tests/tier_repair.rs` behind `#![cfg(feature = "pg")]` (the controller ruling: do not move production code). The bench leg requires exactly one `--op` (0 or 2 → exit 2) and prints one `ab-result` line per process; `--features bench` is required. The crash matrix's seven states are the base `moves.rs` fake-cold matrix, credited as such.

### Task 25: In-process e2e

**Files:**
- Modify: `crates/tinio-e2e/Cargo.toml` (`tier` = `pg`, `tinio-tier/testing`)
- Modify: `crates/tinio-e2e/tests/steps/{mod,tag_filter,common}.rs`
- Create: `crates/tinio-e2e/tests/features/tier.feature`
- Test: the cucumber run

**Interfaces:**
- `@tier` backend tag, `TINIO_E2E_BACKEND=tier`, default exclusion; `Server::tier` takes the shared pg reset permit, truncates, uses `from_parts`, and starts the mover/flush/GC/lifecycle tasks; scenarios for CRUD, cold read, transition, heat, batch, restart, copy/rename/delete, versioned refusal, tags/ACL, range, attributes, pagination, large object, read-only.

- [x] **Step 1: Write the feature**

- [x] **Step 2: Run and confirm the failure**

Run: `TINIO_E2E_BACKEND=tier cargo test -p tinio-e2e --features tier --test cucumber -- --tags '@tier and not @parquet and not @object-lock and not @interop and not @boto3 and not @mc and not @confinement and not @pg'`
Expected: FAIL.

- [x] **Step 3: Implement the World/tasks/tag filter**

- [x] **Step 4: Run**

Run: `TINIO_E2E_BACKEND=tier cargo test -p tinio-e2e --features tier --test cucumber -- --tags '@tier and not @parquet and not @object-lock and not @interop and not @boto3 and not @mc and not @confinement and not @pg'`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-e2e/Cargo.toml`, `tests/steps/{mod,tag_filter,common}.rs`, `tests/features/tier.feature`.

Evidence: tier pass 16 scenarios / 208 steps passed; default suite 303 / 2540 with neither the feature nor the tag selected; the env grammar lives in `tests/steps/backend.rs` (tier build parses, non-tier build refuses loudly, tag precedence preserved). `Server::tier` takes and moves the shared pg permit, resets after the cold plane connects, uses `from_parts`, and joins mover/flush/GC/lifecycle with the hidden cadences disabled for deterministic scenarios. Extra files: `tests/cucumber.rs` (seven axes), `tests/traceability.rs` (path-include for `backend.rs`), `tests/steps/backend.rs`. Fix round 1 addressed the runner doc, the env arm, the dead `tier_read_only`, the batch title and the report. The CI explicit tag lists (`and not @tier`) and the tier CI jobs are Task 26's, which owns `.github/workflows/ci.yml`.

### Task 26: External client suite

**Files:**
- Create: `crates/tinio-e2e/tests/features/interop/tier.feature`
- Modify: `crates/tinio-e2e/tests/steps/{mod,clients,common}.rs`
- Modify: `crates/tinio-e2e/README.md`
- Modify: `specs/001-s3-local-server/spec.md` (amend FR-042; add FR-044–046)
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (the single owner of this edit)
- Test: the external run and traceability

**Interfaces:**
- `@tier @interop`/`@boto3`/`@mc`; `TINIO_E2E_EXTERNAL=1`; a spawned tier server config (root, `[storage] type="tier"`, `[storage.pg]`, `[storage.tier.*]`).
- All four clients are required in CI: the job installs aws-cli, rclone, mc and the boto3 venv, and the leg fails when one is missing.
- aws-cli: IA PUT, cold GET, attributes class, copy, UploadPartCopy, list, restart.
- rclone: IA copy, ls, cat.
- mc: cp, stat, ls, lifecycle round trip.
- boto3: IA PUT, get, attributes, copy, UploadPartCopy, lifecycle (30-day IA), versioned refusal, batch LIST.
- The class assertions use `GetObjectAttributes`; HEAD/list class is not asserted.

- [x] **Step 1: Write the feature**

- [x] **Step 2: Run and confirm the failure**

Run: `cargo build -p tinio --features tier`
Run: `TINIO_E2E_EXTERNAL=1 TINIO_E2E_BACKEND=tier cargo test -p tinio-e2e --features tier --test cucumber -- --tags '@tier and (@interop or @boto3 or @mc)' --retry 1`
Expected: FAIL.

- [x] **Step 3: Implement the spawn/config and spec text**

- [x] **Step 4: Run**

Run the command above.
Run: `cargo test -p tinio-e2e --test traceability`
Expected: PASS.

- [x] **Step 5: Checkpoint inventory**

`crates/tinio-e2e/tests/features/interop/tier.feature`, `tests/steps/*`, `README.md`, `specs/001-s3-local-server/*`.

Evidence: external tier 4 scenarios / 63 steps passed with aws 2.38.5, rclone 1.75.2, mc 2025-08-13 and boto3 1.43.118 (boto3 installed in a `target/` venv); traceability 23; default suite 303/2540; in-process tier 16/208; e2e clippy `-D warnings` clean. The spawned server is a real `tinio` start with `type="tier"`, `[storage.pg]` and `[storage.tier]`; restart is a graceful `tinio stop` with a bounded wait and hard-kill backstop; a copy promotes its cold source, so scenarios assert the class before copying and use a second object for the restart leg. CI: `and not @tier` on the mem, pg and shared interop lists; a `tier` job (in-process pass) and a `tier-external` job installing rclone/mc/boto3, verifying all four clients fail-closed, with a pg service and `TINIO_PG_REQUIRED=1`. Fix round 1: FR-044 restored as its own spec line, FR-042/FR-045 defer to their single owners (FR-046, `contracts/s3-surface.md`), the tier CI comment corrected, `not @confinement` added to the external list, `TINIO_PG_REQUIRED=1` on both jobs, and `tinio stop` clears the same env vars as `spawn_child`.

### Task 27: Docs and ownership check

**Files:**
- Modify: `CONTEXT.md`, `docs/tests.md`, `specs/001-s3-local-server/data-model.md` (the state-dir closed set; this task owns it), `docs/superpowers/specs/2026-10-18-s3-object-lifecycle-design.md` (every live-prose no-tier statement), `README.md`
- Test: traceability, doc gate

**Interfaces:**
- Glossary `tier`, `absent`, `ColdInfo`, `tier::Rows`, `placement`, `warm candidate`, `copying`, `tier marker`, `prepare-downgrade`.
- `tier.redb`/`tier.mode` in the state-dir layout; the config contract's tier keys; all LC9 live prose points to T11; the accepted limits.

- [x] **Step 1: Update the docs**

- [x] **Step 2: Run the gates**

Run: `cargo test -p tinio-e2e --test traceability`
Run: `cargo doc --workspace --no-deps`
Expected: PASS.

- [x] **Step 3: Run the one-definer check**

Search every other document for a second statement of each tier fact; fix hits before closing.

- [x] **Step 4: Checkpoint inventory**

The docs listed above.

Evidence: traceability 23 passed; `cargo doc --workspace --no-deps` exit 0 (one pre-existing `tinio-api/src/transport.rs:1389` warning at the base commit — the file is unmodified in this branch; CI's `-D warnings` doc job is red at base for the same reason). Documents updated: `CONTEXT.md` (nine glossary terms, shipped names `tier::Rows`/`tier::Facts`), `docs/tests.md` (six tier layers, harness drain rule, `TINIO_PG_REQUIRED` scoped to Rust DB legs), `docs/cargo.md` (the `tier` feature graph matches the manifest: non-optional `tinio-tier`, `tier = ["pg","versioning"]`), `README.md` (tier section reduced to feature + pointers; crate-README line softened), `specs/001-s3-local-server/{data-model.md,contracts/config.md}` (closed-set rows for `tier.redb`/`tier.mode`; metric families), the lifecycle design (live-prose no-tier statements point at the tier design), the tier design, the tagging/conditionals designs and the prior-art research. The one-definer check fixed the README restatements and the e2e README duplicates/counts. Parked minor: `README.md` "the `tier` feature compiles the backend" could read "enables the backend" (`tinio-tier` is non-optional).

### Task 28: Final acceptance gate

**Files:** none.

- [x] **Step 1: Workspace tests**

Run: `cargo test --workspace`
Run: `cargo test --workspace --all-features`
Expected: PASS.

- [x] **Step 2: pg and e2e**

```bash
docker compose -f tools/pg/docker-compose.yml up -d
export TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable'
export TINIO_PG_REQUIRED=1
cargo test -p tinio-tier --features testing --test conformance -- --test-threads=4
cargo test -p tinio-tier --features testing --test repair -- --test-threads=4
cargo test -p tinio-pg -p tinio-store-pg -- --test-threads=4
TINIO_E2E_BACKEND=tier cargo test -p tinio-e2e --features tier --test cucumber -- --tags '@tier and not @parquet and not @object-lock and not @interop and not @boto3 and not @mc and not @confinement and not @pg'
docker compose -f tools/pg/docker-compose.yml down
```
Expected: PASS.

- [x] **Step 3: External clients (required)**

Prerequisite: aws-cli, rclone, mc and the boto3 venv are installed; the leg fails when one is missing. The CI job installs them.

```bash
docker compose -f tools/pg/docker-compose.yml up -d
export TINIO_PG_DSN='postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable'
cargo build -p tinio --features tier
TINIO_E2E_EXTERNAL=1 TINIO_E2E_BACKEND=tier cargo test -p tinio-e2e --features tier --test cucumber -- --tags '@tier and (@interop or @boto3 or @mc)' --retry 1
docker compose -f tools/pg/docker-compose.yml down
```
Expected: PASS.

- [x] **Step 4: Feature, platform, lint and doc gates**

Run: `cargo check -p tinio --no-default-features`
Run: `cargo check -p tinio-cli --no-default-features`
Run: `cargo check -p tinio-fs --no-default-features`
Run: `cargo check -p tinio-fs --features tier`
Run: `cargo hack check --package tinio-server --each-feature --skip otel`
Run: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
Run: `cargo doc --workspace --no-deps`
Expected: PASS. The Linux cfg leg follows `docs/tests.md` on WSL.

- [x] **Step 5: Evidence and the operator's single commit**

Record the commands and results in this task's Evidence line, including the two additive core-error semver exemptions — `NoTiers` (Task 1) and `InvalidRequest` (Task 12, required for the S3 `InvalidRequest` when a transition targets `STANDARD`) — and the `Placement.mtime` overload (state-start for `Copying`/`Deleting`, completion for `Pg/Live`, object mtime for `Fs/Live`); tick the boxes in the same round. The operator stages the branch and makes one commit for the whole plan; no AI attribution. Agents do not run git.

Evidence (2026-10-25, D:\Github\tinio\.claude\worktrees\tiered-storage):
- `cargo test --workspace` (DSN + REQUIRED): PASS except one pre-existing sandbox sweep load flake, green in isolation.
- `cargo test --workspace --all-features` (DSN + REQUIRED): PASS except one pre-existing sandbox e2e load flake, green in isolation.
- Step 2: tier conformance 17, CLI `tier_repair` 4, pg/store-pg and the `@tier` cucumber 16/16 all PASS.
- Step 3: the external tier leg's 4 scenarios PASS (aws-cli, rclone, mc, boto3).
- Step 4: check, hack, clippy, doc and fmt gates and the WSL tier leg PASS; 25 CLI-test and 2 pre-existing lints fixed.
- Compatibility: `NoTiers` and `InvalidRequest` are additive variants; the one exhaustive match updated; semver exemptions recorded.
- `Placement.mtime`: state-start for `Copying`/`Deleting`, completion for `Pg/Live`, object mtime for `Fs/Live`.
- Controller-added: Task 22b metrics, the `delete_object_version(None)` suspended-bucket fix, the clippy/format rounds.
- Final review: ready to merge with fixes; 2 Critical, 7 Important, 11 Minor; all closed; deferred minors in the SDD ledger.
- The operator stages the branch and makes one commit for the whole plan; no AI attribution. Agents did not run git.
