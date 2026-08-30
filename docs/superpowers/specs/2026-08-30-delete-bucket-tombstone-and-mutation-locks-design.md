# Design: Delete-bucket unpublish, per-bucket mutation locks, IO-pipeline outputs

**Date**: 2026-08-30
**Status**: implemented in the working tree (uncommitted); decisions locked after Thermos review + grilling (2026-08-30); second-pass amendments D-A..D-F (2026-08-30); third-pass type split (2026-08-30, addendum at the bottom)
**Scope**: `tinio-util` lock map (`lockmap.rs`, workspace `papaya`), `tinio-fs` bucket delete (`backend/buckets.rs`, `tombstone.rs`, `cleanup.rs`, `scanner.rs`, `etag.rs`, `listing.rs`), the removal lane (D-A: `tinio-core` pipeline constants, `tinio-config` `schema/pipeline.rs`, `tinio-server` `pipeline.rs` / `benches/pipeline.rs` / `examples/serve.rs`), startup repair split (D-B: `tinio-core` `cleanup.rs` options).

## Goal

1. Make `delete_bucket` return as soon as the live name is unpublished. Tree `remove_dir_all` is slow IO: enqueue it off the request on the removal lane (D-A); leftovers are removal-lane / doctor / scanner work.
2. Serialize directory mutations **per bucket name**, not process-wide, so delete/create/PUT of A never stall B.
3. Keep the mutation-lock invariant: a PUT waiting on the lock of a name that is being deleted must not land in a **recreated** directory of the same name.
4. Keep IO-pipeline jobs as object ETag compute (`etag::Result`). Tombstone reclaim is a different task type on the removal lane (`Result<(), Error>`), not a dummy ETag or a shared `Etag | Done` sum.

## Non-goals

- No lock **generation/epoch**. Recreate of a deleted name may wait on doomed waiters of the old mutex; they fail `ensure_bucket` with `NoSuchBucket` and then the recreate proceeds. An epoch that unblocks recreate immediately is a later change if latency of that wait becomes a problem.
- No `Runner::enqueue_forget`. Dropping `Completion` is already fire-and-forget after the queue slot is taken. Tombstone additionally `tokio::spawn`s the **enqueue** so delete does not wait on queue backpressure. That spawn stays local to `tombstone::reclaim`.
- No revert of `papaya`. The table stays lock-free; `Map::remove` is **not** part of the API.
- No change to followed-symlink policy: unpublish `rename`s the lexical root **entry** (the link), not the canonical target.
- No relocated-`state_dir` tombstone: `<root>/.tinio/deleting/` stays on the data volume (FR-023 `EXDEV`).

## Problems this round closed

### 1. Process-wide bucket mutation lock

`FsStorage` held `Arc<Mutex<()>>` across every bucket create/delete/PUT-commit/complete/folder-marker. Unrelated buckets serialized. The table is now `lockmap::Map<bucket::Name>`; `lock_bucket_mutations(&self, name)` takes that name only.

### 2. Delete blocked on `remove_dir_all`

A successful emptiness check used to `remove_dir_all` the live directory on the request task. Large empty-looking trees (folder markers, leftover dirs) made delete latency the tree walk. The live name now leaves via `rename` onto `<root>/.tinio/deleting/<uuid>` under the per-bucket lock; the request returns; `RemoveTask` walks the unpublished tree on a removal-lane worker (D-A). The emptiness check itself is a **shallow early-exit** — a `read_dir` of the bucket root where the first entry other than `.tinio` (and no in-progress uploads) means content — never a tree walk.

### 3. `Map::remove` split the mutex (Thermos High)

After unpublish, delete **yanked** the lockmap slot so recreate would not queue behind doomed waiters. Waiters that had already cloned the old `Arc<Mutex<()>>` kept that mutex. Recreate’s `lock(name)` inserted a **fresh** slot. A PUT parked on the old mutex could then `ensure_bucket` the new directory and commit — the invariant delete was written to protect.

Chosen fix: **do not yank**. After delete drops the guard, the same slot still serializes parked PUT vs recreate. The PUT runs, sees the unpublished name, fails `NoSuchBucket`. Recreate waits, then succeeds.

Rejected: generation/epoch (unblocks recreate, more API); keeping `remove()` (generation split).

### 4. `etag::Outcome::discarded()` type leak (Thermos quality)

Tombstone reclaim shared `Runner<etag::Result>` by synthesizing a dummy keep (`key = "tombstone"`, `ETag::EMPTY`). The type claimed every IO job was an object ETag. A later producer that persisted IO completions would write a fake meta row.

First fix: `tinio_fs::io::Output { Etag(etag::Outcome), Done }` so tombstone reclaim could share `Runner<io::Result>` with compute. Deleted `Outcome::discarded()`.

Third pass: D-A already gave removal its own runner. The shared sum type and listing/scanner `Done` no-ops were leftover. Deleted `io.rs`. IO is `etag::Result`; remove is `Result<(), Error>`; `Pipelines<IoResult, RemoveResult, DbResult>`.

Rejected: dummy ETag keep; `spawn_blocking` (the dedicated removal lane already provides the physical isolation, D-A); `enqueue_forget` on the pipeline trait.

### 5. Tombstone repair piled into `cleanup.rs`

Walk/remove/count lived in a file already past 1.5k lines, plus a thin `reclaim_delete_tombstones` that counted `Ok(RepairAction)`. Two unrelated multipart-orphan tests landed in the same file.

Chosen fix: `tombstone::{leftovers, clear_one, clear_leftovers}` own the leftover tree; `FsCleanup::repair` only `record_repair`s; scanner calls `clear_leftovers`. Unrelated tests were split into `cleanup_edges.rs`, later folded back into `cleanup.rs`'s test module when the split proved redundant (the file is gone).

## Architecture

```
delete_bucket(name)
  │  lock_bucket_mutations(name)     // lockmap::Map<bucket::Name>
  │  ensure_bucket + bucket_is_empty // shallow early-exit read_dir, not a tree walk
  │  rename lexical root → <root>/.tinio/deleting/<uuid>
  │  remove_bucket_state (warn on failure; delete already succeeded)
  │  drop guard                       // same slot remains for waiters
  │  tombstone::reclaim(remove_pipeline, dest)  // spawn; do not await enqueue
  ▼  Ok(())                           // live name is gone

RemoveTask (removal pipeline, Q4 blocking `remove_tree_blocking`)
  → Ok(()) (warn on IO error; never fail a pipeline streak)

leftover / crash:
  doctor  FsCleanup::repair → tombstone::leftovers + record_repair(clear_one)
  startup FsCleanup::repair(Startup) with with_remove_runner(remove_pipeline) (D-B)
          → repair_delete_tombstones enqueues one RemoveTask per leftover
  scanner scan_once         → tombstone::clear_leftovers(root)
```

### Per-bucket mutation lock — `tinio-util::lockmap`

- Table: `Arc<papaya::HashMap<K, Arc<tokio::sync::Mutex<()>>>>`.
- `lock(key)`: `get_or_insert_with` + retry if `Guard::drop`’s `remove_if` evicted the slot between insert and clone. **Do not hold a papaya pin across `.await`.** `lock_owned().await` runs after the pin is dropped.
- `Guard::drop`: release the tokio mutex first, then `remove_if` only when `Arc::ptr_eq` and `strong_count == 2` (table + this guard; no waiter clone).
- **No `Map::remove`.** Yanking a live key while waiters exist is a generation split. Eviction is only last-guard `Drop`.
- Call sites that held the old process-wide lock now pass `name`: `create_bucket`, `delete_bucket`, PUT phase-2, complete, folder markers, scanner F05 orphan probe, cleanup F02 stale-bucket prune, `testutil::retarget_bucket_during_commit`. F02's two halves differ (D-B): the **scanner** path (`reclaim_stale_buckets`) takes the per-bucket lock (a recreate could interleave a probe + wipe mid-serve), while the **startup** repair's `repair_buckets` runs synchronously pre-serving — no request can race it yet, so it stays lock-free; **doctor** is offline and needs no lock.
- Lock-wait observability (D-E, fs layer only — `tinio-util` stays generic): `lock_bucket_mutations` measures the wait around `map.lock(name).await`; past a `const` threshold of 1 second it `tracing::warn!`s with the bucket name and the waited ms.

### Unpublish — `tinio-fs::tombstone`

| Item | Rule |
|------|------|
| Location | `<root>/.tinio/deleting/<uuid>` (`STATE_DIR_NAME` + `"deleting"`), **not** relocated `FsOptions.state_dir` |
| Prepare | `create_dir_all` parent, then unique uuid path |
| Unpublish | `rename` lexical `root/<name>` → dest (same volume as the name) |
| Followed symlink | Moves the link under root; canonical target is untouched |
| Lane | The **removal pipeline** (D-A): `FsOptions.remove_pipeline` / `Pipelines.remove`, built from `[pipeline.remove]` (workers default 1, capacity 1024) — physically isolated from ETag compute on the IO pipeline |
| Request return | After rename + state wipe + `reclaim` spawn; does **not** wait for `remove_dir_all` or enqueue backpressure |
| IO errors | `tracing::warn`; leftover stays for doctor/scanner |
| Shutdown | Enqueue `ShutDown`/`Dropped` → warn; leftover stays |

Public API (`tinio_fs::tombstone`; the module is public — the cleanup stage calls `enqueue_one`):

```rust
pub(crate) fn dir(root: &Path) -> PathBuf;
pub(crate) async fn prepare(root: &Path) -> Result<PathBuf, Error>;
pub(crate) async fn leftovers(root: &Path) -> Result<Vec<(String, PathBuf)>, Error>; // missing dir → empty
pub(crate) async fn clear_one(path: &Path) -> Result<(), Error>; // dir or stray file; missing ok
pub(crate) async fn clear_leftovers(root: &Path) -> Result<usize, Error>; // warn-and-skip per entry
pub(crate) fn reclaim(pipeline: Arc<dyn Runner<Result<(), Error>>>, path: PathBuf);
pub(crate) async fn enqueue_one(path: PathBuf, pipeline: &Arc<dyn Runner<Result<(), Error>>>) -> bool; // fire-and-forget
```

`leftovers`: a `read_dir` error other than `NotFound` is returned (not silently truncated).

### Pipeline outputs

- `FsOptions.io_pipeline`: `Arc<dyn Runner<etag::Result>>` — `ComputeTask::run` returns `hash()` as `etag::Result`.
- `FsOptions.remove_pipeline` (D-A): `Arc<dyn Runner<Result<(), Error>>>` — `RemoveTask` warns on IO error and returns `Ok(())`. `delete_bucket` passes this (not `io_pipeline`) to `tombstone::reclaim`.
- List `fold_compute` / scanner `fold_outcome`: `Ok(Outcome)` is a page/meta entry; `NotFound` still skips; other `Err` still fails the list / counts toward scanner R4. There is no `Done` arm.
- Server `Pipelines<IoResult, RemoveResult, DbResult>`: IO = `etag::Result`, remove and DB = `Result<(), tinio_fs::Error>`. The removal field is constructed from `[pipeline.remove]` (`track_consecutive_failures: false`, workers default 1, capacity default 1024); serve.rs and the bench construct it, and serve.rs injects it as `FsOptions.remove_pipeline`.
- `pipeline::Outcome` remains the blanket `Result<T, E>` impl. The IO and removal lanes do **not** track consecutive failures (`track_consecutive_failures: false`) — consecutive-failure escalation (R7, "likely systemic") is the DB write pipeline's mechanism. Tombstone IO errors are logged inside `RemoveTask` and still return `Ok(())` so a leftover tree is not a removal-lane failure.

### Cleanup / scanner

- `FsCleanup::repair` always calls `repair_delete_tombstones`; the stage enumerates via `tombstone::leftovers` and reports via `record_repair` (dry-run supported). With a removal-lane runner injected (`with_remove_runner`, D-B) each leftover is enqueued as a `RemoveTask` (`tombstone::enqueue_one`, fire-and-forget); without one (doctor) it is cleared inline via `clear_one`.
- Server startup (D-B, before readiness): `FsCleanup::new(&storage, CleanupOptions::default()).with_remove_runner(pipelines.remove())`, then drive `repair(Startup)` **synchronously** — best-effort: a failed stage is warned and readiness proceeds (the scanner covers residue). The tombstone stage enqueues one `RemoveTask` per leftover on the removal lane from inside `repair_delete_tombstones` (fire-and-forget; an enqueue failure is warned inside and the leftover stays for the scanner) — no second enumeration outside the repair. The stale-bucket prune (`repair_buckets`) therefore runs pre-serving — lock-free is correct, do **not** take the mutation lock there. `RemoveTask` uses the same tree-or-file removal primitive as `clear_one` (`fsutil::remove_tree_blocking`), so a stray file under the tombstone dir is removed, not left for the scanner. **With the scanner OFF, tombstones are still cleared by the startup enqueue.**
- Scanner `scan_once` calls `tombstone::clear_leftovers(storage.root())` after stale-bucket prune. It does **not** go through cleanup’s action vec.

## Data flow

1. Client `DeleteBucket`. Emptiness + unpublish hold `name`’s mutation lock. Concurrent PUT/complete of **this** name wait; other names do not.
2. Lexical rename unpublished the name. `remove_bucket_state` best-effort. Guard drops. Request returns 200.
3. Spawned task enqueues `RemoveTask`. Worker `remove_dir_all`. Failure → warn.
4. Crash between 2 and 3, or enqueue rejected: `<root>/.tinio/deleting/<id>` remains. The startup repair (with the tombstone stage deferred, D-B) plus the removal-lane enqueue — and each scanner pass — clear it; with the scanner OFF the startup enqueue alone still covers it.
5. Recreate of the same name: waits on any waiter still holding the old slot, then `create_dir`. A parked PUT of the **previous** generation fails `ensure_bucket` before that create, or runs after create only if it acquired the **same** mutex after recreate — in which case it is a write into the new generation **after** recreate succeeded, which is ordinary PUT-vs-create ordering on one lock, not a split.

## Error handling

| Event | Request | Residue |
|-------|---------|---------|
| Bucket not empty | `NotEmpty` (lock still held, then dropped; **no** unpublish) | live name unchanged |
| Rename / prepare IO | `Error::Io` | live name unchanged |
| `remove_bucket_state` fails | still `Ok(())` + warn | leaked rows; startup stale-bucket prune |
| Enqueue shutdown / drop | still `Ok(())` + warn | tombstone dir |
| `remove_dir_all` fails | already returned | tombstone dir; warn |
| Doctor `clear_one` fails | repair action `Err` | leftover kept |
| Sync startup repair step fails (D-B) | warn; **readiness proceeds** | residue covered by the scanner |
| Startup tombstone enqueue fails (D-B) | warn | leftover stays for the scanner |

## Testing

- **lockmap**: same-key serialize, distinct keys independent, last drop evicts, waiter pins the slot. **No** test that a later `lock` of a yanked key must not wait (that was the split).
- **fs buckets**: per-bucket lock does not stall another name; same name serializes; unpublish moves live name under `.tinio/deleting` (wait for leftover empty — reclaim is async); tombstone stays on the data volume when `state_dir` is relocated; recreate **times out** while a doomed waiter holds the mutex (`delete_bucket_does_not_split_the_mutation_lock`).
- **fs buckets (D-F split-invariant stress)**: `delete_create_put_hammer_keeps_successful_puts_in_the_live_generation` — 25 bounded rounds (per-round timeout) of concurrent `delete_bucket` / `create_bucket` / PUT phase-2 commits on ONE name; every PUT that reports success must be readable from the **live** bucket afterward (never only in a tombstoned tree); the tombstone dir must drain empty after reclaim (waited like the unpublish test). Benign outcomes tolerated: delete `NotEmpty`/`Io`; PUT `NoSuchBucket`/`NoSuchKey` (it never committed). The forbidden outcome — a successful PUT whose object is not in the current live generation — fails the test. Runs in seconds.
- **tombstone**: `clear_leftovers` removes unpublished trees; `RemoveTask` completion is `Ok(())`.
- **cleanup**: `repair(Startup)` reports a tombstone action and deletes the leftover (doctor wiring — no runner); with a runner injected (`with_remove_runner`) the stage enqueues and the inline runner clears the leftover while the other stages still run (D-B). Multipart invalid-name / unreadable-upload-dir tests live in `cleanup.rs`'s test module.
- **scanner**: `scan_once` increments `reclaimed` and removes a planted leftover.
- **etag**: inline-runner tests unwrap `etag::Result`.
- **server**: `Pipelines` inject into `FsOptions` with `etag::Result` (IO) and `Result<(), Error>` (remove + DB, D-A); probe task returns a dummy `etag::Outcome`; 12 consecutive IO **or** removal failures stay ordinary warns (only the DB lane escalates).

## Call sites / compatibility

All `io_pipeline`/`remove_pipeline` constructors are in-workspace: fs tests and fixtures plus `tinio-fs::testing` pass an inline runner for both (D-A); the server serve/example and the pipeline bench construct the removal lane from `[pipeline.remove]` (default 1 worker). `etag::Result` is the IO runner output and the compute-core return of `ComputeTask::hash`.

## Decisions (locked 2026-08-30)

- **Per-bucket `lockmap::Map<bucket::Name>`**, not a process-wide mutex.
- **Unpublish then async reclaim** on the removal pipeline (D-A; Q4 blocking task + spawn around enqueue).
- **Tombstones on the data volume** (`<root>/.tinio/deleting/`), never relocated `state_dir`.
- **Do not `Map::remove` after delete.** Same mutex serializes doomed waiters and recreate. Epoch deferred.
- **Keep papaya.** Retry/`remove_if` stay; they exist to make lock-free eviction safe, not to support yank.
- **No dummy ETag keep**, not `spawn_blocking`, not `enqueue_forget`. (`io::Output` was an interim shared sum type; third pass deleted it.)
- **Leftover walk/remove in `tombstone.rs`.** Cleanup only reports. Scanner calls `clear_leftovers`.
- **English-only docs**; no auto git commit (project git rule).

### Second pass (locked 2026-08-30)

- **D-A — dedicated removal lane.** New `[pipeline.remove]` config section (workers default 1, capacity default 1024); `FsOptions.remove_pipeline`; `Pipelines.remove`; bench/serve wiring. `delete_bucket` passes `remove_pipeline` to `tombstone::reclaim`. The removal lane provides the **physical isolation** — a large tombstone tree walk can never occupy the IO workers' capacity — which supersedes the original `spawn_blocking` non-goal rationale. (Output types were later split: IO `etag::Result`, remove `Result<(), Error>`.)
- **D-B — startup split.** The server drives `repair(Startup)` **synchronously** before readiness — best-effort, warn and continue (readiness proceeds; the scanner covers residue); `repair_buckets` therefore runs pre-serving lock-free (no mutation lock there — the scanner's `reclaim_stale_buckets` stays the locked path). The tombstone stage is routed to the removal lane by injecting the runner into `FsCleanup` (`with_remove_runner(pipelines.remove())` — no `CleanupOptions` flag): `repair_delete_tombstones` enqueues one `RemoveTask` per leftover (`tombstone::enqueue_one`, fire-and-forget; warn on enqueue failure) instead of clearing inline; doctor keeps the default and clears inline. With the scanner OFF, tombstones are still cleared by the startup enqueue.
- **D-E — lock-wait observability.** `lock_bucket_mutations` (fs layer only; `tinio-util` stays generic) times the wait around `map.lock(name).await` and warns past a 1-second `const` threshold (bucket name + waited ms).
- **D-F — split-invariant stress test.** Bounded rounds (25, per-round timeout) of concurrent delete/create/PUT on one name; a successful PUT must be readable from the live bucket afterward; the tombstone dir drains empty after reclaim. Benign failures tolerated (`NotEmpty`/`Io` delete, `NoSuchBucket`/`NoSuchKey` PUT); the forbidden outcome — a successful PUT not in the current live generation — fails the test.

### Third pass (locked 2026-08-30)

- **Split pipeline outputs.** Deleted `tinio_fs::io`. IO pipeline = `etag::Result`; removal pipeline = `Result<(), Error>`; `Pipelines<IoResult, RemoveResult, DbResult>`. List/scanner no longer fold a `Done` no-op. `RemoveTask` already used `fsutil::remove_tree_blocking` (same tree-or-file fallback as `clear_one`). No extra lock for scanner vs `RemoveTask` on the same unpublished tree; no change to papaya `strong_count` eviction.
