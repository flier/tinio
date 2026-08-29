# Code Review: Dual Pipelines Implementation (uncommitted, against pipeline-spec.md)

> Date: 2026-08-29
> Scope: all staged (uncommitted) changes implementing `pipeline-spec.md` — tinio-core pipeline contract, tinio-server dual-pipeline runtime, tinio-fs task implementations + list/scanner integration, meta/DB layer (set_batch, gated load, write-lock histogram), config, metrics.
> Method: spec-vs-code conformance review + data-path performance review (5 parallel review tracks); `cargo check --workspace --all-targets` clean (only the pre-existing tinio-util warning).

## Summary

The implementation matches the spec's contract decisions (P1–P7, R1–R8, Q1–Q10) closely: backpressure is a real bounded channel, pagination-first is preserved, the P6 matches gate happens before enqueue, composed-ETag preservation is a faithful reproduction of `ensure_etag`, and histogram overhead is negligible.

One **major** performance issue (scanner holds a redb read transaction across the entire bucket walk), several **minor** data-path redundancies (avoidable batch/key clones, a gauge drift race), and a set of nits (spec-wording mismatches, stale test names, unused runtime drivers) were found. Suggested priority: fix the major finding before merge; #2/#3 are low-risk pure wins; #4 protects observability accuracy.

## Resolution status (2026-08-29, latest working tree)

All findings in this review are resolved in the current working tree; the fixes are cited by F-number in code comments:

- **#1 (scanner read-transaction pinning) — resolved**: `reconcile_bucket` materializes the gating snapshot up front via `meta::Store::load_bucket` (scanner.rs, one short-lived read transaction) and gates each walked file against the in-memory map; the pinned-window class is gone.
- **#2 (`set_batch` clone) — resolved**: `meta::Store::set_batch_owned` takes the batch by value; `MetaWriteBatchTask::run` moves it (`std::mem::take`), no per-batch copy.
- **#3 (list hot-path key clones) — resolved**: `load_entries` borrows the page keys and returns index-aligned `Vec<Option<StoredMeta>>`; the page's own key order is the assembly order.
- **#4 (queue-depth gauge drift) — resolved**: `stats()` derives the depth from the channel itself (`max_capacity() - capacity()`); the counted increment/decrement race class is deleted.
- **Nits — resolved**: serve.rs stops the scanner/sweeper before the pipelines and awaits their handles (shutdown ordering); the runtimes drop `enable_all()`; the metrics refresh is wired to the reserved data-plane `GET /metrics` (F10) with the families registered at startup; `for_bucket_gated` has one home in tables.rs; the stale channel-era test names are gone; `md5_of_file` re-panics on a join error like `database::Handle` (F25, deliberate). The R7 escalation fires at exactly the 10th failure once per streak — kept per the spec's own 2026-08-29 reading ("at the 10th").

## Findings

### 1. MAJOR — Scanner pins a redb read transaction for the whole bucket walk

`crates/tinio-fs/src/scanner.rs:263` opens `meta.gate_bucket(name)` before the walk; the gate's `redb::ReadTransaction` is only released inside the `spawn_blocking` closure at `scanner.rs:342-357`. While it is held, the loop (`scanner.rs:288-332`) performs per-file async walk IO, `enqueue().await` (blocks on queue-full backpressure), and `yield_now + sleep(delay)` every `BATCH_SIZE` enqueued tasks (R2). Meanwhile the DB pipeline keeps committing `set_batch` write transactions — **no page freed during the scan can be recycled while this snapshot lives**, so the `.redb` file grows monotonically for the duration of a long cold scan; the slower the scan (larger delay, more backpressure), the worse the growth.

This also deviates from the spec's own settled trade-off: §3.7 accepts "whole-bucket meta **loaded into memory**" (materialize-then-drop), and `meta::Store::load_bucket` (`meta.rs:443`) is exactly that primitive — yet it has **no production callers** (its own doc admits this). The code comments (`scanner.rs:260-262`) acknowledge the pinning as a trade-off, but it contradicts R1/§3.7. Note that `walked: HashSet` (`scanner.rs:272`) is already O(bucket) memory, so materializing the gate does not worsen the memory class.

**Fix**: load once up front via `load_bucket` into a `HashMap<Key, Option<StoredMeta>>` (short-lived read transaction, released immediately); gate each walked file against the map in memory; derive orphan candidates as snapshot-minus-walked from the same map. Identical snapshot semantics, zero pinning during the walk, and `MetaGate`/`begin_gate` can be deleted.

### 2. MINOR — `set_batch` re-clones a batch the task already owns

`crates/tinio-fs/src/meta.rs:371` (`entries.to_vec()`), called from `crates/tinio-fs/src/write_task.rs:41`. `MetaWriteBatchTask` owns `entries: Vec<BatchEntry>` and is dropped after `run()`, but the slice signature forces a full copy into the `'static` write closure — ~128 `String` key + `ETag` clones per batch commit, on the write pipeline's only data path.

**Fix**: add a by-value variant (`set_batch_owned(Vec<BatchEntry>)` or change the signature to take `Vec`); `run()` passes `std::mem::take(&mut self.entries)`. Slice-based test callers can `to_vec()` themselves.

### 3. MINOR — list hot path clones every page key twice for the gating load

`crates/tinio-fs/src/listing.rs:302` builds `keys: Vec<Key>` (clone #1); `meta::Store::load_entries` (`meta.rs:419`) clones each key again into `GatedMeta.key` (clone #2), even though point-read rows are contractually index-aligned with the page (asserted at `listing.rs:325`) and `row.key` is never read on the hit path. Up to ~2000 avoidable `String` allocations per hot `max_keys=1000` page.

**Fix**: have `load_entries` return index-aligned `Vec<Option<StoredMeta>>` (keep `GatedMeta { key, .. }` only for the traversal form, where the key is new information), and pass keys by reference without materializing the intermediate `Vec<Key>`.

### 4. MINOR — `queue_depth` gauge drifts permanently upward on a send/receive race

`crates/tinio-server/src/pipeline.rs:308-315` increments `queue_depth` after `send()` returns; the worker decrements (saturating, `:429`) on dequeue. A fast worker can dequeue and decrement (saturating at 0) before the producer's `fetch_add(1)` lands — the decrement is consumed, the increment then leaves the gauge at +1 with an empty queue, permanently, until the shutdown reset. The comment at `:301-307` claims the drift is "bounded ... per racing sender"; that holds only for a single producer — concurrent producers can accumulate beyond +1 per race event, so the gauge slowly misreports over the process lifetime. This gauge feeds the `/metrics` acceptance surface (C2/C5).

**Fix**: stop counting; read depth from the channel in `stats()` via `sender.max_capacity() - sender.capacity()` (keep a `Sender` clone in `PipelineInner` for post-shutdown stats). This deletes `queue_depth_dec`, the shutdown `store(0)`, and the whole race class.

### 5. NIT — redundant syscalls in the ETag compute path

- Composed-replacement path calls `file.metadata()` (and on Windows `GetFileInformationByHandle`) twice: once for the identity probe (`etag_task.rs:123-135`) and again inside `md5_of_handle` (`etag_task.rs:191`). Rare path; reuse the post-hash metadata for both.
- `open_nofollow_std` (`fsutil.rs:90`) performs a post-open `metadata()` check on unix, where `O_NOFOLLOW` already guarantees the opened file is not a symlink — one dead fstat per hashed file. `#[cfg(windows)]`-gate the post-open check (the check is only meaningful on Windows, where `FILE_FLAG_OPEN_REPARSE_POINT` succeeds on a link).

### 6. NIT — observability wiring gap to close when T075 lands

`refresh_write_lock_histograms` / `refresh_pipeline_gauges` (`crates/tinio-server/src/metrics.rs:216,234`) currently have **test-only callers**; no production `/metrics` endpoint exists yet (the spec defers endpoint exposure to T075, so this is not a deviation of this changeset). However, the histogram families register lazily on first refresh — if the T075 wiring forgets the refresh calls, `tinio_write_lock_*` will be silently absent or permanently zero, guarded only by the explicit refresh inside `registers_all_families`.

**Fix**: touch the histogram registration once unconditionally at server startup, and record the refresh call sites in the T075 task when it is scheduled.

### 7. NIT — spec-wording and consistency items

- **R7 escalation fires only at exactly `== 10`** (`pipeline.rs:541-543`); the spec says "≥10 → escalate". Tests (`escalation_fires_once_per_failure_streak`) deliberately pin once-per-streak as anti-log-spam — confirm whether this is the intended reading; if "escalated while systemic" is meant, change to `>=` and update tests.
- **`serve.rs:131-136` shutdown-ordering comment overpromises**: the scanner checks the watch only between passes, so a mid-flight pass keeps enqueueing after `pipelines.shutdown()` and hits `Error::ShutDown`, producing exactly the spurious "scanner pass failed" warn the comment claims the ordering prevents. Fix: keep the scanner/sweeper JoinHandles, send the watch signals, await the handles, then shut down the pipelines.
- **Scanner orphan-candidate scan re-panics on JoinError** (`scanner.rs:355`); the scanner loop has no `catch_unwind` and nothing awaits its JoinHandle, so a panic there would silently kill the scanner. Map the JoinError to `Error` and propagate (consistent with R4's warn-and-retry-next-round).
- **Stale test names** still describe the removed channel design (P7): `etag_task.rs:456`, `:472`, `write_task.rs:123`, `:164`, `:189/205`.
- `meta::md5_of_file` converts a hashing-task panic into an `io::Error`, inconsistent with `database/handle.rs`'s deliberate re-panic on join errors.
- `for_bucket_gated` is duplicated verbatim in `tables.rs:228` and `:309` — the corrupt-row self-healing rule should live in one private helper.
- Both pipeline runtimes use `enable_all()` (`pipeline.rs:177-184`) although workers only poll channels — a timer and IO driver per runtime that nothing uses.
- `busy_workers` is structurally identical to `in_flight` (incremented/decremented as a pair) — contract-mandated redundancy, noted so nobody later "fixes" a divergence that cannot exist.
- Cold-list hash concurrency default drops 16 → 2 (old `buffer_unordered(ETAG_CONCURRENCY=16)` replaced by `[pipeline.io] workers` default 2). Benchmark-backed and intentional (§3.3), but worth a changelog/docs note for operators.

## Verified clean (checked against the code, not assumed)

- **Backpressure**: real bounded `mpsc` + `send().await`; no unbounded channels, no busy-loop/polling in non-test code; capacity range validation prevents `channel(0)`.
- **No head-of-line blocking / no lock-step**: list enqueues the whole page then drains via `FuturesUnordered` in completion order; scanner drains with non-blocking `now_or_never` between enqueues; producers only ever wait on queue slots.
- **Transaction counts**: `load_entries` = 1 read transaction per page (not per key); `set_batch` = 1 `begin_write`/`commit` per batch (table opened once, empty batch short-circuits before the write lock); hot path has zero write transactions and zero IO tasks (tested).
- **P1 fidelity**: the in-task composed-ETag preservation decision matches `ensure_etag` clause-for-clause, including the identity-unavailable (`0`) fallback to the mtime jitter window.
- **P6**: the matches gate runs in the producer before enqueue; no short-circuit inside the task.
- **R3**: nofollow open, Windows identity from the already-open handle (no second open-by-path), ELOOP normalized to `PermissionDenied` on both platforms.
- **Histogram overhead**: 3 `Instant::now` + 7 relaxed atomics per write transaction — noise next to an `Immediate`-commit fsync; counters are cache-line padded; `evaluate_compact` goes through the same timed path (P5).
- **Config**: defaults/ranges match the spec exactly and share the tinio-core `DEFAULT_*` constants; presence-gating and "[pipeline.*] not emitted" tested (the `toml_edit` write side was dropped per F47 after this review ran).
- **Thread priority mapping**: `low = Crossplatform(0)`, `high = Crossplatform(99)`, matching the §3.4 Windows probe; `set_for_current` failure warns and degrades.
- **Q2/Q3b/R2/R4/R6/R8**: final drain before list responds; scanner drops batch completions with `Outcome` warn fallback; pacing after every `BATCH_SIZE` enqueued tasks; ≥100 consecutive non-NotFound failures abort the bucket; panicking tasks leave the worker alive and resolve `Error::Dropped`; failures are logged even when the completion is dropped.
