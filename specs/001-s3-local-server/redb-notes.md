# redb Understanding & Troubleshooting Notes (redb 4.2.0)

> Purpose: capture the verified findings, pitfalls, and learning/on-call entry points about redb from this session (tinio-fs metadata migration to redb). Every fact below was verified against the redb 4.2.0 source / docs.rs / README. Version: `redb = "4.2"` (workspace dependency, `Cargo.lock` pins 4.2.0; tinio-mem already uses its `InMemoryBackend`, tinio-fs plans the file backend).

---

## 1. What redb is / core mechanics (understanding)

- **Embedded KV library**: single-file, BTreeMap-style API, ACID transactions, MVCC concurrency, crash-safe by default. Features: zero-copy, thread-safe, savepoints/rollbacks (README "Fully ACID-compliant transactions, MVCC support for concurrent readers & writer, without blocking, Crash-safe by default").
- **Storage engine: copy-on-write B-tree, no WAL**. Writes never mutate pages in place; they copy new pages. A commit atomically switches the root-page reference + fsyncs (Immediate durability). No WAL ⇒ no log replay ⇒ the class of "replay corruption" problems does not exist; a commit is either complete or not applied.
- **Automatic crash recovery**: `check_integrity()` docs verbatim: "redb will automatically detect and recover from crashes, power loss, and other unclean shutdowns" — no operator intervention needed.
- **Concurrency model**: single writer (`begin_write()` docs: "Only a single write may be in progress at a time. If a write is in progress, this function will block"); MVCC readers never block the writer; `WriteTransaction` is not tied to the `Database` lifetime (transactions remain usable after the `Database` is dropped).
- **Durability**: 4.2 has only `None` / `Immediate` (`#[non_exhaustive]`, more variants may come later). `None` = nothing is flushed until a later `Immediate` commit carries it out.
- **Stats**: `WriteTransaction::stats() -> Result<DatabaseStats>` (note: on the transaction, and returns a `Result` — not on `Database`) → `DatabaseStats` (private fields + getters): `tree_height` / `allocated_pages` / `leaf_pages` / `branch_pages` / `stored_bytes()` (getter name; the underlying field is actually `stored_leaf_bytes`) / `metadata_bytes` / `fragmented_bytes` (data trees + system-tree fragmentation + free pages × page_size) / `page_size`.
- **Compaction**: `Database::compact(&mut self)` — needs exclusive `&mut`, returns `Ok(true)` (compacted) / `Ok(false)` (nothing left to compact); COW makes the file grow-only, compaction reclaims free pages.
- **`Database` is not `Clone`**: `db.rs` has `pub struct Database { mem: Arc<TransactionalMemory>, transaction_tracker: Arc<TransactionTracker> }` — the internals are `Arc`s, but the type itself has **no `impl Clone`**. Multi-component sharing must wrap `Arc<Database>` yourself (this project: `Arc<DbHandle>`). This directly conflicts with `compact(&mut self)`: once shared you can never obtain `&mut` (`Arc::get_mut` returns `None` while live clones exist) — see pit 13 in §2.
- **Integrity**: `Database::check_integrity(&mut self)` → `Ok(true)` intact / `Ok(false)` repaired / `Err(Corrupted)` unrecoverable; docs explicitly say "quite slow, only use when you suspect the file was modified outside redb or a redb bug".
- **Key/value types**: `Key`/`Value` are implemented for tuples up to **12 elements** (`tuple_types.rs`); wide elements (`&str`/`&[u8]`) may appear at **any position** — a non-last wide element is written with a varint length prefix (lengths are front-loaded, data concatenated after); comparison is per-element lexicographic; `&str` compares by bytes. **Correction** (found in review): tinio-mem's `TableDefinition<&str, (&str, u64, u64)>` tuple sits in the **value** position, so it is not evidence for tuple-key feasibility — tuple-key feasibility is established by the `tuple_types.rs` source (verified: `(&str, &str)` and `(&str, &str, u32)` are both legal). Tuple keys sort by first component then second, so entries sharing a first element are contiguous in the B+ tree, which gives prefix range scans for free (boundary construction in pit 14 of §2).
- **Tables**: `TableDefinition<const>` static definitions; **read transactions refuse to open a table that does not exist** — table creation must be concentrated in one write transaction at open time (tinio-mem pattern). Also available: `MultimapTable`, `UntypedTable`, `ReadOnlyDatabase`, `StorageBackend` (no_std capable, needs `experimental-api-5`).
- **Error types**: `DatabaseError` (open class), `TransactionError`, `TableError`, `StorageError`, `CommitError` + the aggregate `Error`. tinio-mem wraps each kind with an explicit `From` (tinio-fs plans the same: five variants, projecting to contract `storage::Error::Io`).
- **Ecosystem position**: README benchmarks vs lmdb/rocksdb/fjall (random reads in 4.2 faster than rocksdb; write throughput same order); license MIT OR Apache-2.0; file format stable ("reasonable effort upgrade path").

## 2. Issues / pitfalls found (design must account for these)

1. **`stats()` is on `WriteTransaction`, not `Database`** — even "read-only stats" needs `begin_write()` (takes the write lock!) + `abort()`. Frequent stats contention with writers; evaluation (e.g. compact decisions) must be low-frequency (on the order of once per sweep cycle).
2. **`DatabaseStats` fields are private**: only getters; there is no direct "free pages" counter — the fragmentation ratio must be computed yourself (`fragmented_bytes / (allocated_pages × page_size)`).
3. **`compact()` / `check_integrity()` need `&mut self`**: cannot be called while serving (conflicts with live transactions; `check_integrity` returns `TransactionInProgress` when a transaction is active). ⇒ compact must be offline (before readiness at startup / doctor --fix).
4. **Savepoint deadlock trap**: the `compact()` source comment warns — the caller may legitimately hold an uncommitted `WriteTransaction`; if that transaction created a savepoint, a blocking `begin_write()` would deadlock. Any pending write transaction must be resolved before compacting (this project: the compact phase is exclusive, no active transactions).
5. **`Durability::None` semantics are subtle**: a `None` commit is not durable until a later `Immediate` commit; `check_integrity` on a non-durable `None` commit either persists it (if it passes) or rolls it back (if repair is needed). Defaulting to `Immediate` is fine; leave `None` until a benchmark proves it necessary.
6. **Single writer = write-throughput bottleneck**: concurrent write transactions serialize; each commit under `Immediate` is one fsync. ⇒ batch writes (`set_batch`: N entries in one transaction) both reduce lock contention and amortize fsync.
7. **COW files only grow**: no automatic compaction; needs an explicit strategy (this project: fragmentation-ratio evaluation + offline compact at startup/doctor + the `compact_needed` marker).
8. **Multi-process**: redb is designed single-process (tinio relies on SC-005 single-instance binding; redb itself has a file-lock fallback — cross-process writes fail).
9. **Read-transaction guard lifetime**: zero-copy read values are bound to the read transaction's lifetime; values must be copied outside the guard (tinio-mem pattern: "zero-copy inside the guard, copy outside").
10. **Missing table is an error**: opening a non-existent table in a read transaction fails — create all tables in one write transaction at open (idempotent).
11. **`Durability` is `#[non_exhaustive]`**: future versions may add variants (e.g. Eventual); matches need a wildcard arm.
12. **Version / source mirror**: the version is whatever `Cargo.lock` pins; the registry source-dir prefix varies with `CARGO_HOME` and the mirror — do not hardcode paths; locate per the approach in §3.
13. **`Database` not `Clone` ⇒ compact timing is structurally locked** (found in review; harder than pit 3): pit 3 says "cannot compact while serving" because of live transactions; but even with no live transactions, once the `Database` has been distributed to the stores via `Arc`, `&mut` is permanently unavailable. ⇒ **the only viable compact window is after open and before wrapping in `Arc<DbHandle>`/constructing `FsStorage`**. The startup orchestration order is therefore: open → compact (marker/evaluation) → construct stores/FsStorage → startup repair → readiness (the old "repair → compact" order is infeasible: repair needs stores, stores need the shared handle). doctor --fix opens exclusively offline on its own, so it is unconstrained.
14. **Tuple-key range-scan boundaries (correction, verified 2026-08-24)**: comparison is **per-element by bytes**, the first element dominates. There is **no legal exclusive `&str` upper bound** for a `(bucket, ..)` prefix: `bucket + '\u{10FFFF}'` encodes to `F4 8F BF BF`, and any ASCII continuation character (e.g. `'x'`, 0x2D/0x78) compares smaller — `"data-x" < "data\u{10FFFF}"`, so bucket names with longer prefixes leak into the range (unit test `bucket_scan_boundaries_exclude_other_buckets` failed and proved it). `(bucket, "\u{10FFFF}")` as an upper bound is equally wrong (the second element never participates once the first element decides). **Correct approach**: scan from the lower bound `(bucket, "")` and stop when the first element stops matching (this project's `drain_pair`/`collect_pairs` predicate boundary, O(range) + 1 lookahead).

## 3. Where to look next time you need redb facts (learning path)

1. **Local source (preferred; grep-verifiable line by line)** — the version comes from `Cargo.lock`; no need to memorize the source root — derive it from the `manifest_path` that `cargo metadata` reports (the parent dir is the source root; the registry prefix varies with `CARGO_HOME`/mirror), or search the registry source dir by crate name, or `cargo doc -p redb` for local docs. Read by file role: `src/db.rs` (`Database`: open/create/begin_write/compact/check_integrity), `src/transactions.rs` (`WriteTransaction`/`ReadTransaction`/`stats()`/ `Durability`/savepoints), `src/tuple_types.rs` (`Key`/`Value` tuple impls), `src/tree_store/` (B-tree internals, only when digging into the engine), `src/error.rs`/`src/multimap_table.rs`/`src/table.rs`.
2. **docs.rs**: `docs.rs/redb/latest/redb/` (`Database` / `DatabaseStats` / `Durability` / `TableDefinition` / `Key` / `Value`) — API docs are faster than source.
3. **GitHub README**: cberner/redb — feature list, benchmark comparisons (lmdb/rocksdb/fjall), no_std notes. Note: the repo's `DESIGN.md` is 404 on master (the raw path does not exist); for engine design details read the `tree_store/` source directly.
4. **This workspace's reference implementation**: `crates/tinio-mem/src/` (storage.rs, error.rs) — existing redb API usage patterns (table creation, transactions, error wrapping, guard-copy pattern), directly relevant to this project's integration.
5. **Upgrade path**: the version in `Cargo.lock` + the crates.io page (4.2.0, updated 2026-08); README claims a stable file format and an upgrade-path commitment.
6. **Comparisons**: README's benchmark table (write throughput, volume before/after compaction) — look here first when you need "redb vs other libraries" conclusions.

## 4. Troubleshooting path

Locate the source file by symptom (all paths relative to `src/`):

| Symptom | Look at first | Focus |
|---|---|---|
| Cannot open the DB / open fails | `db.rs` `open`/`create` + `error.rs` `DatabaseError` | file lock (multi-process), corruption, version/upgrade, path permissions |
| Commit fails | `transactions.rs` commit path + `error.rs` `CommitError` | fsync failure, disk full, transaction already aborted |
| Write blocking / high latency | this project's `DbHandle` write-lock histogram (wait vs total) → who holds the write transaction | single-writer contention; batch size big enough; `stats()` fragmentation already high (triggers compact) |
| Suspected data corruption | `db.rs` `check_integrity` | external modification / bit rot → `Ok(false)` repaired / `Err(Corrupted)` unrecoverable → delete and rebuild (meta is derivable; recompute fallback) |
| File only grows, never shrinks | `transactions.rs` `stats()` (fragmented_bytes/allocated_pages) | fragmentation ratio over threshold → offline `compact()` (before `FsStorage` construction at startup / doctor --fix, see pit 13) |
| Read errors | read-transaction guard lifetime (value used after drop) | zero-copy values bound to the transaction lifetime; copy before leaving the guard |
| Table won't open | `table.rs` + open-time table creation | table not created (read transactions refuse non-existent tables); `Key`/`Value` type changes make the schema incompatible |
| Hang / deadlock | `db.rs` `compact`/`begin_write` comments + savepoints | a blocking operation called while holding a write transaction + savepoint deadlocks |
| Data "lost" | `transactions.rs` `Durability::None` | a `None` commit is not durable until a later `Immediate`; default to `Immediate` |
| Concurrent write throughput | single-writer model | add batching (many entries per transaction); >1 write-pipeline worker yields nothing |

**This project's integration troubleshooting entry**: tinio-fs `state.rs` (`DbHandle` wrapper + `stats`/`needs_compact`/`compact_if_needed`; the timing histogram belongs to the pipeline phase, see `specs/001-s3-local-server/pipeline-spec.md`) → tinio-mem `storage.rs` (reference implementation) → error mapping (five variants → `storage::Error::Io`) → /metrics (write-lock histogram + the two pipeline gauges, pipeline phase).

## 5. This project's (tinio-fs migration) integration essentials at a glance

- Single file `<state-dir>/meta.redb`, five tables: `OBJECT_META` / `BUCKETS` / `UPLOADS` / `PARTS` / `STATE` (version + `compact_needed` marker); composite keys `(bucket, key)` etc., tuple `Key`s (range boundary construction in pit 14).
- Always `Durability::Immediate`; single writer → remove the in-process Mutex.
- `Database` is not `Clone`: stores hold `Arc<DbHandle>`; compact can only run before sharing (startup orchestration: open → compact → construct `FsStorage` → startup repair → readiness, see pit 13).
- Stats-based evaluation (fragmentation ≥ `[storage.fs] compact_threshold_percent`, default 20; floor constant 64 MiB) + `STATE.compact_needed` marker + offline compact at startup/doctor --fix.
- **Two-phase split (review decision 2026-08-23)**: the redb migration (`specs/001-s3-local-server/meta-redb-spec.md`) goes first, list/scanner stay on inline recompute; the dual pipeline (ETag computation + `set_batch` batched writes, write-lock histogram, thread priorities) is a separate later phase (`specs/001-s3-local-server/pipeline-spec.md`) — `set_batch` is a pipeline-phase task. Offline/test construction for pipeline-required scenarios uses tinio-core's public `pipeline::InlineRunner` (same-thread synchronous execution, reference implementation), not testutil (doctor is not a test; grilling Q1 decision).
- Detailed design: `specs/001-s3-local-server/meta-redb-spec.md` and `specs/001-s3-local-server/pipeline-spec.md`.

