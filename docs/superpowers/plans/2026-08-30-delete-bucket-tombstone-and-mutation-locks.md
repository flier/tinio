# Delete-bucket unpublish and per-bucket mutation locks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]` / `- [ ]`) syntax for tracking.
>
> **This round is already in the working tree (uncommitted).** Checkboxes below are marked done. Re-running a task means verifying against the spec, not inventing a second design. Git writes: ask the user per operation (`CLAUDE.md`); never auto-commit.
>
> **Third pass (2026-08-30):** deleted `tinio_fs::io`. IO pipeline is `etag::Result`; removal pipeline is `Result<(), Error>`; `Pipelines<IoResult, RemoveResult, DbResult>`. Task 2's `io.rs` / `Output::{Etag, Done}` steps are superseded.

**Goal:** Unpublish `delete_bucket` onto `<root>/.tinio/deleting/`, reclaim the tree off the request on the **removal** pipeline, serialize directory mutations per bucket name without splitting the mutex across generations, and keep IO jobs as `etag::Result` (not dummy ETags, not a shared `Etag | Done` sum).

**Architecture:** `lockmap::Map<bucket::Name>` (papaya table, last-guard eviction only — no `remove`). Delete holds that lock for emptiness + lexical `rename`, then `tombstone::reclaim` `tokio::spawn`s enqueue of a Q4 `RemoveTask`. Leftovers: `tombstone::leftovers` / `clear_one` / `clear_leftovers`; doctor `record_repair`s; scanner counts `clear_leftovers`. IO runner is `Runner<etag::Result>`; removal runner is `Runner<Result<(), Error>>`.

**Tech Stack:** Rust 2024 workspace, `tokio`, `papaya` 0.2 (workspace pin), `tinio-core::pipeline`, `async-trait`.

**Spec:** `docs/superpowers/specs/2026-08-30-delete-bucket-tombstone-and-mutation-locks-design.md`

## Global Constraints

- English only: docs, comments, commits, PRs (`CLAUDE.md`).
- Never auto-commit / push / merge / rebase / stash — ask first, per operation (`CLAUDE.md`).
- Import module not type; `bucket::Name` qualified; no prefixed type names (`docs/style.md`).
- `unsafe_code = forbid` on every crate (`docs/cargo.md`).
- Pin `papaya` once in root `[workspace.dependencies]`; crates use `papaya.workspace = true`.
- Tombstones live under `<root>/.tinio/deleting/`, never relocated `state_dir` (FR-023).
- No `Map::remove`. No `etag::Outcome::discarded`. No `Runner::enqueue_forget`.
- Do not hold a papaya `pin()` across `.await`.
- Tests: `cargo test -p tinio-util --lib lockmap`; `cargo test -p tinio-fs --lib`; `cargo test -p tinio-server --lib pipeline`.

## File map

| File | Responsibility |
|------|----------------|
| `Cargo.toml` | Workspace `papaya = "0.2"`. |
| `crates/tinio-util/Cargo.toml` | `papaya.workspace = true`. |
| `crates/tinio-util/src/lockmap.rs` | Per-key mutex table; papaya; **no yank**. |
| `crates/tinio-fs/src/tombstone.rs` | Unpublish paths, leftover GC, `RemoveTask`, `reclaim`. |
| `crates/tinio-fs/src/lib.rs` | `mod tombstone`. |
| `crates/tinio-fs/src/backend/mod.rs` | `bucket_mutation_locks`; `lock_bucket_mutations(&name)`; `io_pipeline: Runner<etag::Result>`; `remove_pipeline: Runner<Result<(), Error>>`. |
| `crates/tinio-fs/src/backend/buckets.rs` | Unpublish `delete_bucket`; per-name lock on create. |
| `crates/tinio-fs/src/backend/objects.rs` | PUT / folder-marker take `lock_bucket_mutations(bucket)`. |
| `crates/tinio-fs/src/backend/multipart.rs` | Complete takes `lock_bucket_mutations(bucket)`. |
| `crates/tinio-fs/src/etag.rs` | `ComputeTask::Output = etag::Result`. |
| `crates/tinio-fs/src/listing.rs` | Runner type `etag::Result`. |
| `crates/tinio-fs/src/scanner.rs` | Runner type `etag::Result`; `clear_leftovers` on `scan_once`. |
| `crates/tinio-fs/src/cleanup.rs` | `repair_delete_tombstones` = leftovers + `record_repair(clear_one)`. |
| `crates/tinio-fs/src/cleanup_edges.rs` | Multipart-orphan edge tests (not tombstone). |
| `crates/tinio-fs/src/testutil.rs` | `retarget_bucket_during_commit(..., bucket)`; `FailingTaskRunner<etag::Result>`. |
| `crates/tinio-server/src/pipeline.rs` | `Pipelines<IoResult, RemoveResult, DbResult>`; inject probe = `etag::Result`. |
| `crates/tinio-server/benches/pipeline.rs` | Bench tasks return `etag::Outcome`. |
| `crates/tinio-core/src/pipeline.rs` | Doc: IO task error type is `etag::Result`. |

---

### Task 1: Per-bucket lock map (papaya, no yank)

**Files:**
- Modify: `Cargo.toml` (workspace dep)
- Modify: `crates/tinio-util/Cargo.toml`
- Modify: `crates/tinio-util/src/lockmap.rs`
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`bucket_mutation_locks`, `lock_bucket_mutations(&name)`)
- Modify: `crates/tinio-fs/src/backend/{buckets,objects,multipart}.rs` (pass `name`)
- Modify: `crates/tinio-fs/src/scanner.rs` (F05)
- Modify: `crates/tinio-fs/src/cleanup.rs` (F02 `reclaim_stale_buckets`)
- Modify: `crates/tinio-fs/src/testutil.rs` (`retarget_bucket_during_commit` takes `&bucket::Name`)

**Interfaces:**
- Consumes: existing `lockmap::Map` / `Guard` contract (`lock`, last-guard eviction).
- Produces: `FsStorage::lock_bucket_mutations(&self, name: &bucket::Name) -> lockmap::Guard<bucket::Name>`. **No** `Map::remove`. **No** process-wide `Mutex<()>`.

- [x] **Step 1: Write the failing tests** (tinio-fs, `backend/buckets.rs` tests)

```rust
#[test]
fn mutation_lock_is_per_bucket() {
    rt(async {
        let (_root, storage) = storage();
        let a = bucket::name("alpha").unwrap();
        let b = bucket::name("beta").unwrap();
        storage.create_bucket(&a).await.unwrap();
        let _guard = storage.lock_bucket_mutations(&a).await;
        let storage2 = storage.clone();
        let create_b = tokio::spawn(async move { storage2.create_bucket(&b).await });
        let created = tokio::time::timeout(std::time::Duration::from_millis(500), create_b)
            .await
            .expect("create of a different bucket must not wait on another bucket's lock")
            .unwrap();
        created.unwrap();
    });
}

#[test]
fn mutation_lock_serializes_same_bucket() {
    rt(async {
        let (_root, storage) = storage();
        let a = bucket::name("alpha").unwrap();
        let _guard = storage.lock_bucket_mutations(&a).await;
        let storage2 = storage.clone();
        let create_a = tokio::spawn(async move { storage2.create_bucket(&a).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(80), create_a)
                .await
                .is_err(),
            "create of the same bucket must wait on the mutation lock"
        );
    });
}
```

- [x] **Step 2: Run tests to verify they fail** (before the field change they do not compile: `lock_bucket_mutations` takes no name)

Run: `cargo test -p tinio-fs --lib mutation_lock_is_per_bucket -- --nocapture`

Expected: compile error on `lock_bucket_mutations(&a)` if the old `()` signature remains.

- [x] **Step 3: Minimal implementation**

Workspace `Cargo.toml`: `papaya = "0.2"` under `[workspace.dependencies]`.

`lockmap::Map`: `Arc<papaya::HashMap<K, Arc<Mutex<()>>>>`. `lock` retries `get_or_insert_with` if pin/`get` after clone is not `ptr_eq`. `Drop`: `remove_if` with `ptr_eq && strong_count == 2`. Do **not** add `remove()`. Keep `contains` / `len` / `is_empty`.

`FsStorage`: replace `bucket_mutation_lock: Arc<Mutex<()>>` with `bucket_mutation_locks: lockmap::Map<bucket::Name>`. Every former `.lock().await` becomes `.lock_bucket_mutations(name).await`.

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tinio-util --lib lockmap`

Run: `cargo test -p tinio-fs --lib mutation_lock`

Expected: PASS.

- [x] **Step 5: Commit** — skip unless the user asks (`CLAUDE.md`).

---

### Task 2: IO pipeline output type

**Files:**
- Create: `crates/tinio-fs/src/io.rs`
- Modify: `crates/tinio-fs/src/lib.rs` (`pub mod io`)
- Modify: `crates/tinio-fs/src/etag.rs` (`ComputeTask::Output = io::Result`; delete `Outcome::discarded`)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`io_pipeline: Arc<dyn Runner<io::Result>>`)
- Modify: `crates/tinio-fs/src/listing.rs` (runner type; `fold_compute` matches `Output::etag()`)
- Modify: `crates/tinio-fs/src/scanner.rs` (same for `fold_outcome` / `compute_outcome` / test runners)
- Modify: `crates/tinio-fs/src/testutil.rs` (`FailingTaskRunner` over `io::Result`)
- Modify: `crates/tinio-server/src/pipeline.rs` (inject probe)
- Modify: `crates/tinio-server/benches/pipeline.rs`
- Modify: `crates/tinio-core/src/pipeline.rs` (module doc: `io::Result` not `etag::Result`)

**Interfaces:**
- Consumes: `etag::Outcome`, `crate::Error`.
- Produces:

```rust
// crates/tinio-fs/src/io.rs
pub enum Output {
    Etag(etag::Outcome),
    Done,
}
impl Output {
    pub fn etag(self) -> Option<etag::Outcome>;
}
pub type Result = std::result::Result<Output, crate::Error>;
```

`ComputeTask::run` → `self.hash().map(crate::io::Output::Etag)`.

List/scanner: `Ok(output) => match output.etag() { Some(o) => o, None => return skip }`.

- [x] **Step 1: Write the failing test** (etag helper must unwrap `Etag`, not a raw `Outcome`)

In `etag.rs` tests, `run(task)` becomes:

```rust
fn run(task: ComputeTask) -> Result {
    let runner = InlineRunner::default();
    crate::testutil::rt(async move {
        match runner.enqueue(Box::new(task)).await.unwrap().await.unwrap() {
            Ok(crate::io::Output::Etag(outcome)) => Ok(outcome),
            Ok(crate::io::Output::Done) => panic!("etag compute returned Done"),
            Err(err) => Err(err),
        }
    })
}
```

Existing etag tests fail to compile until `ComputeTask::Output` is `io::Result`.

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-fs --lib etag::tests::kind_is_etag`

Expected: type mismatch `etag::Result` vs `io::Result` until Task impl is updated.

- [x] **Step 3: Write minimal implementation**

Add `io.rs` as in **Produces**. Retype every `Runner<etag::Result>` that is the **IO** pipeline (not the DB `Runner<Result<(), Error>>`). Delete `Outcome::discarded`. Bench `BenchEtagTask` wraps the hashed `Outcome` in `io::Output::Etag`. Server inject probe may return `Ok(io::Output::Done)`.

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tinio-fs --lib etag::`

Run: `cargo test -p tinio-server --lib pipelines_inject_into_fs_options`

Expected: PASS.

- [x] **Step 5: Commit** — skip unless the user asks.

---

### Task 3: Tombstone module (paths, leftover GC, RemoveTask)

**Files:**
- Create: `crates/tinio-fs/src/tombstone.rs`
- Modify: `crates/tinio-fs/src/lib.rs` (`mod tombstone`)

**Interfaces:**
- Consumes: `crate::io::Result`, `pipeline::Runner`, `fsutil::ok_if_missing`, `path::STATE_DIR_NAME`.
- Produces: `dir`, `prepare`, `leftovers`, `clear_one`, `clear_leftovers`, `reclaim` — signatures exactly as in the spec “Unpublish” table.

- [x] **Step 1: Write the failing tests** (in `tombstone.rs`)

```rust
#[test]
fn clear_leftovers_removes_unpublished_trees() {
    rt(async {
        let root = tempfile::tempdir().unwrap();
        let leftover = dir(root.path()).join("dead-bucket");
        std::fs::create_dir_all(&leftover).unwrap();
        std::fs::write(leftover.join("leftover.bin"), b"was-a-bucket").unwrap();
        assert_eq!(clear_leftovers(root.path()).await.unwrap(), 1);
        assert!(!leftover.exists());
    });
}

#[test]
fn remove_task_completes_as_done() {
    use tinio_core::pipeline::{InlineRunner, Runner};
    rt(async {
        let root = tempfile::tempdir().unwrap();
        let path = dir(root.path()).join("gone");
        std::fs::create_dir_all(&path).unwrap();
        let runner = InlineRunner::default();
        let done = runner
            .enqueue(Box::new(RemoveTask { path: path.clone() }))
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(done, crate::io::Output::Done));
        assert!(!path.exists());
    });
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tinio-fs --lib tombstone::`

Expected: unresolved `tombstone` / `RemoveTask` until the module exists.

- [x] **Step 3: Write minimal implementation**

`DIR_NAME = "deleting"`. `prepare` = `dir(root)/uuid` + `create_dir_all` parent. `leftovers`: `NotFound` → `Ok(vec![])`; other `read_dir`/`next_entry` errors propagate. `clear_one`: `remove_dir_all`, else `NotADirectory` → `remove_file`; missing ok. `clear_leftovers`: warn-and-skip per entry, return count. `reclaim`: `let _ = tokio::spawn(async move { pipeline.enqueue(RemoveTask).await ... })`. `RemoveTask::run`: blocking `std::fs::remove_dir_all`; NotFound ok; other errors warn; always `Ok(io::Output::Done)`.

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tinio-fs --lib tombstone::`

Expected: PASS.

- [x] **Step 5: Commit** — skip unless the user asks.

---

### Task 4: `delete_bucket` unpublish (no lock yank)

**Files:**
- Modify: `crates/tinio-fs/src/backend/buckets.rs`

**Interfaces:**
- Consumes: `tombstone::prepare`, `tombstone::reclaim`, `lock_bucket_mutations(name)`, `bucket_path_lexical`.
- Produces: `delete_bucket` returns after unpublish + `reclaim` spawn. Does **not** call `bucket_mutation_locks.remove`.

- [x] **Step 1: Write the failing test** (generation split)

Replace any test that asserted recreate must **not** wait after delete while a waiter holds the old mutex. The required test:

```rust
#[test]
fn delete_bucket_does_not_split_the_mutation_lock() {
    // A waiter queued behind delete still holds the same mutex after
    // unpublish. Recreate must wait — yanking the slot would let a
    // parked PUT land in the new directory (generation split).
    rt(async {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let held = storage.lock_bucket_mutations(&b).await;
        let storage_d = storage.clone();
        let bd = b.clone();
        let delete = tokio::spawn(async move { storage_d.delete_bucket(&bd).await });
        wait_for_lock_waiter().await;
        let storage_w = storage.clone();
        let bw = b.clone();
        let waiter = tokio::spawn(async move {
            let _guard = storage_w.lock_bucket_mutations(&bw).await;
            std::future::pending::<()>().await;
        });
        wait_for_lock_waiter().await;
        drop(held);
        delete.await.unwrap().unwrap();
        let storage_c = storage.clone();
        let bc = b.clone();
        let created = tokio::time::timeout(
            std::time::Duration::from_millis(80),
            tokio::spawn(async move { storage_c.create_bucket(&bc).await }),
        )
        .await;
        waiter.abort();
        assert!(
            created.is_err(),
            "recreate must wait on the doomed waiter of the deleted name"
        );
    });
}
```

Also: `delete_bucket_unpublishes_into_deleting_dir` (live name gone, `tombstone::dir` exists, `wait_for` empty leftover). `delete_bucket_tombstone_stays_on_the_data_volume` (relocated `state_dir` has no `deleting/`).

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-fs --lib delete_bucket_does_not_split_the_mutation_lock`

Expected: FAIL with `"recreate must wait..."` if `Map::remove` still runs after delete (recreate succeeds immediately).

- [x] **Step 3: Write minimal implementation**

Inside the per-name lock: `ensure_bucket`, `bucket_is_empty` → `NotEmpty` **without** unpublish; `prepare`; `rename(live, dest)`; `remove_bucket_state` warn-only. Drop guard. `tombstone::reclaim(io_pipeline.clone(), dest)`. `Ok(())`. **Do not** `bucket_mutation_locks.remove(name)`.

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tinio-fs --lib delete_bucket`

Expected: PASS (4 tests: unpublish, data volume, lock split, uploads not empty).

- [x] **Step 5: Commit** — skip unless the user asks.

---

### Task 5: Doctor + scanner leftover reclaim; split cleanup edges

**Files:**
- Modify: `crates/tinio-fs/src/cleanup.rs` (`repair_delete_tombstones` uses `tombstone::leftovers` + `record_repair(clear_one)`; delete `reclaim_delete_tombstones`)
- Create: `crates/tinio-fs/src/cleanup_edges.rs` (`#[cfg(test)] #[path = "cleanup_edges.rs"] mod edges` at the end of `cleanup.rs`)
- Modify: `crates/tinio-fs/src/scanner.rs` (`scan_once` → `tombstone::clear_leftovers(self.storage.root())`)

**Interfaces:**
- Consumes: `tombstone::{leftovers, clear_one, clear_leftovers}`.
- Produces: doctor actions `"would/cleared leftover bucket tombstone {name}"`; scanner `ScanSummary.reclaimed` includes leftover count. **No** `FsCleanup::reclaim_delete_tombstones`.

- [x] **Step 1: Write the failing tests**

Keep `cleanup::tests::startup_repair_clears_delete_tombstones` (plant `<root>/.tinio/deleting/<id>/leftover.bin`, `repair(Startup)`, description contains `"tombstone"`, path gone).

Scanner:

```rust
#[test]
fn reclaims_delete_tombstones() {
    rt(async {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let leftover = crate::tombstone::dir(root.path()).join("dead-bucket");
        std::fs::create_dir_all(&leftover).unwrap();
        std::fs::write(leftover.join("leftover.bin"), b"was-a-bucket").unwrap();
        let scanner = Scanner::new(storage, options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.reclaimed, 1);
        assert!(!leftover.exists());
    });
}
```

Move `invalid_bucket_names_skip_the_drain_but_not_the_removal` and `orphan_stage_reports_an_unreadable_upload_dir` to `cleanup_edges.rs` (unix-only for the latter).

- [x] **Step 2: Run tests to verify they fail** (scanner test fails if `scan_once` never calls `clear_leftovers`)

Run: `cargo test -p tinio-fs --lib reclaims_delete_tombstones`

Expected: `reclaimed == 0` or leftover still exists until `scan_once` is wired.

- [x] **Step 3: Write minimal implementation**

```rust
async fn repair_delete_tombstones(&self, actions: &mut Vec<Result<RepairAction, Error>>) {
    let entries = match tombstone::leftovers(&self.root).await {
        Ok(entries) => entries,
        Err(err) => {
            actions.push(Err(err));
            return;
        }
    };
    for (name, path) in entries {
        record_repair(
            actions,
            self.dry_run,
            format!("would clear leftover bucket tombstone {name}"),
            format!("cleared leftover bucket tombstone {name}"),
            tombstone::clear_one(&path),
        )
        .await;
    }
}
```

`scan_once`: after `reclaim_stale_buckets`, `tombstone::clear_leftovers(self.storage.root())`, warn on `Err`, add `Ok(n)` to `summary.reclaimed`.

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tinio-fs --lib tombstone -- --test-threads=1`

Run: `cargo test -p tinio-fs --lib cleanup::`

Run: `cargo test -p tinio-fs --lib reclaims_delete_tombstones`

Run: `cargo test -p tinio-server --lib`

Expected: PASS.

- [x] **Step 5: Commit** — skip unless the user asks.

---

## Spec coverage (self-review)

| Spec requirement | Task |
|------------------|------|
| Per-bucket lockmap, papaya, no `remove` | 1 |
| `io::Output` / delete `discarded` | 2 |
| Tombstone dir/prepare/leftovers/clear/reclaim | 3 |
| Unpublish `delete_bucket`, data-volume tombstone, no yank | 4 |
| Doctor `record_repair` + scanner `clear_leftovers` + edges file | 5 |
| FR-023 / followed-symlink rename of the link | 4 (comments + volume test) |
| Spawn around enqueue, no `enqueue_forget` | 3 (`reclaim`) |
| English docs, no auto-commit | Global constraints |

No placeholders. Signatures match the spec (`clear_leftovers` → `Result<usize, Error>`, `reclaim` → `Runner<io::Result>`).

## Handoff

Work is already in the working tree. Do **not** re-implement unless a file diverged from this plan.

Verification (optional): `cargo test -p tinio-util --lib lockmap` && `cargo test -p tinio-fs --lib` && `cargo test -p tinio-server --lib`.

Git: leave changes uncommitted until the user asks.
