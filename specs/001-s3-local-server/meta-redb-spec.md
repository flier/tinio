# Design & Implementation Plan: tinio-fs metadata migration to redb

> Status: reviewed + grilling-decided (2026-08-23: G1–G6 all settled — two-phase construction (state::open → compact_if_needed → FsStorage::new_from_db), closure-based DbHandle read/write, direct blocking calls, error-variant mapping (Json/InvalidMetaEntry/ CorruptStateFile removed), read-only-compatible fs-layer tests; plus review revisions: fact corrections, crash/race semantics pinned, dual pipeline split out); **implementation revision (2026-08-24, written back after tasks 1–8 landed)**: range scans do not use an exclusive upper bound (`bucket + '\u{10FFFF}'` does not hold under byte order; changed to scan from the lower bound + stop when the first element stops matching, see §5.2 and redb-notes pit 14 correction); the error shape nests as `Database(DatabaseError)` with five sub-variants per style.md (`Storage(StorageError)` collides with the contract passthrough variant, see §5.4 note); `InvalidMetaEntry` does not actually exist in the current error.rs; redb 4.2 has no separate .lock file; **implementation revision (2026-08-25, review write-back)**: the error model follows thiserror-derived `#[from]` (§5.4 and G4 revised — sub-variants include `Compaction`/`Io`/`UnsupportedVersion`/ `CorruptMeta`, `UnsupportedVersion` nests inside `database::Error`, the crate-level `From` lifts it to `Error::Database(..)`, docs/style.md revised in sync); `follow_symlinks` default flips from true to false as the key moves into `[storage.fs]` (Q10); the complete consumption path is implemented as `complete_object_state` (meta write + UPLOADS/PARTS deletion in one transaction) + `remove_part_dir`, `consume` stays as a store-level API (§5.3/§5.6); task 7 lock-free acceptance whitelist `bucket_mutation_lock` and `PartLocks` Scope: migrate tinio-fs derived metadata storage from JSON files to a redb database file Constraints: new-project scenario, no legacy data, no migration/rollback/legacy-cleanup design Related docs: the **dual pipeline** for batched ETag computation and batched DB writes is a separate later phase, see `specs/001-s3-local-server/pipeline-spec.md`; redb verified notes see `specs/001-s3-local-server/redb-notes.md`

---

# Part 1 — Spec (requirements)

## 1. Background & goals

tinio-fs currently stores derived metadata as files:

- ETag entries: `meta/objects/<bucket>/<2hex>/<sha1>.json` (git-style fan-out)
- bucket creation times: `buckets.json`
- multipart upload records: `upload.json`; part ETags: `part-<n>.etag` sidecars

Pain points of the file approach:

1. **Small-file tax**: at scale this is millions of small JSON files + fan-out directory bloat (inode pressure, large-directory enumeration, Windows path-length limits).
2. **Atomicity is hand-assembled**: temp+rename+in-process lock; part content and the ETag sidecar are two files, leaving a crash window.
3. **Multi-entry operations are not atomic**: `remove_bucket` (recursive directory deletion can fail halfway), meta-orphan reclamation, and multipart complete/abort are not single transactions.

**Goal**: move all derived metadata into a redb database file (`meta.redb`), gaining crash-safe transactional writes, single-transaction multi-entry operations, ordered range scans, and eliminating the small-file tax. redb 4.2 is already a workspace dependency (tinio-mem uses it); no new external dependency.

**Non-goals (this phase)**: pipelining ETag computation / batched DB writes, write-lock latency stats — these are independent optimizations of the "cold-scan recompute" path, split into `specs/001-s3-local-server/pipeline-spec.md`. This phase keeps the current inline recompute cadence for list/scanner ETags (`BATCH_SIZE=32` yield) unchanged.

## 2. Scope

**In**:

- ETag metadata → redb table `OBJECT_META`
- bucket creation times → redb table `BUCKETS`
- multipart upload records → redb table `UPLOADS`; part ETags → redb table `PARTS`
- error model, FsCleanup adaptation, compact mechanism (evaluate/mark/execute), test rewrites, dependency cleanup

**Out / explicitly retained**:

- **The filesystem keeps only multipart part-content files** (`multipart/<bucket>/<upload_id>/part-<n>`) and the write staging directory `tmp/` — these are transient data files that must stay on the filesystem (streaming writes, atomic rename, streaming assembly reads are all bound to the file model).
- Object content is always files (mirror semantics SC-006).
- No migration/rollback/legacy-cleanup mechanism (new project, no legacy data).
- No dual pipeline / write-lock histogram / thread priorities (→ `specs/001-s3-local-server/pipeline-spec.md`).
- read-only mode's home resolution and data-plane AccessDenied rejection belong to T076/T058 (this phase only has fs-layer compatibility tests, G5/G6).

**The old layout disappears entirely**: `meta/objects/`, `buckets.json`, `upload.json`, `*.etag` sidecars **no longer exist** in new projects; no cleanup code is written.

## 3. Prerequisites & constraints

- Single-instance binding (SC-005) guarantees one process per storage root; redb itself carries a file lock, so cross-process protection is unchanged or stronger.
- **read-only mode (FR-023) compatibility** (grilling G5/G6 decision): all private state (meta.redb/tmp/multipart) resolves via `FsOptions.state_dir` — in read-only mode the CLI points state_dir at `~/.tinio/roots/<sha1(root)>/`; the root stays write-free. This phase provides **fs-layer tests** (construct FsStorage with an explicit home state_dir → reads + internal recompute writes work, root has no `.tinio/` and no new files); home path resolution and data-plane AccessDenied rejection belong to T076/T058.
- External behavior unchanged: the `Storage` contract, `MetaStore`/ `BucketStore`/`MultipartStore` public API signatures, and the conformance suite must stay green.
- Metadata is **derived data**: in the worst case, deleting `meta.redb` and recomputing on demand is an acceptable fallback (the scanner already has this capability).
- Style constraints (docs/style.md): redb errors nest as `Database(database::Error)`; redb/io sub-variants use thiserror-derived `#[from]` (2026-08-25 revision: the draft's "explicit From, no derive" contradicted the implementation; derive is authoritative), struct variants use explicit constructors; extra mappings project to `storage::Error::Io`.

## 4. redb 4.2 key facts (decision basis; all verified)

| Fact | Detail | Impact |
|---|---|---|
| **No WAL** | copy-on-write B-tree: writes copy new pages; a commit atomically switches the root-page reference + fsync | no log replay, no replay-corruption class; a commit is either complete or not applied |
| **Crash-safe by default** | README "Crash-safe by default"; `check_integrity()` docs: "automatically detect and recover from crashes, power loss, and other unclean shutdowns" | power loss/crash auto-recovers, no manual intervention |
| **Integrity check** | `check_integrity()`: `Ok(true)` intact; `Ok(false)` repaired; `Err(Corrupted)` unrecoverable (external tampering / hardware bit rot) | doctor can offer "check DB integrity"; when unrecoverable, delete the file and rebuild (metadata is derivable) |
| **Single-writer model** | `begin_write()` docs: "Only a single write may be in progress at a time. If a write is in progress, this function will block" | the in-process Mutex can go; write transactions serialize naturally, MVCC readers never block |
| **`Database` is not `Clone`** | source `db.rs`: `pub struct Database { mem: Arc<TransactionalMemory>, ... }`, **no `impl Clone`** | shared handles must wrap `Arc` yourself (`DbHandle`); `compact(&mut self)` can only run before shared distribution (see §5.9) |
| **Durability** | 4.2 has only `None`/`Immediate` (`#[non_exhaustive]`); `None` = not flushed until a later Immediate commit | use default Immediate (crash-safe); matches need a wildcard arm |
| **Grow-only + `compact()`** | COW makes the file only grow; `Database::compact(&mut self)` compacts | needs stats-based evaluation + offline trigger (startup / doctor --fix), and must run before the `Database` is shared via `Arc` |
| **Stats interface** | `WriteTransaction::stats() -> Result<DatabaseStats>` (note: on the transaction and returns a `Result`); getters: `allocated_pages` / `leaf_pages` / `branch_pages` / `stored_bytes()` (field is actually `stored_leaf_bytes`) / `metadata_bytes` / `fragmented_bytes` / `page_size` | fragmentation-ratio evaluation drives compact decisions; even read-only stats take the write lock (`begin_write`+`abort`), so they must be low-frequency |
| **Tuple key/value** | `Key`/`Value` are implemented for tuples up to 12 elements (source `tuple_types.rs`): wide elements (e.g. `&str`) may appear at **any position** — a non-last wide element is written with a varint length prefix; comparison is per-element lexicographic; `&str` compares by bytes | composite keys `(bucket, key)` are feasible, entries of one bucket are contiguous in the B+ tree, prefix range scans come for free. **Note**: tinio-mem's `TableDefinition<&str, (&str, u64, u64)>` is a tuple **value**, not evidence about keys — key feasibility rests on the redb source |

## 5. Design

### 5.1 Storage layout

```
<state-dir>/
├── meta.redb                # all derived metadata (8 tables, see 5.2); redb 4.2 has no separate lock file (verified)
├── tmp/                     # unchanged: staging area for object writes and multipart assembly (transient data files)
└── multipart/<bucket>/<upload_id>/part-<n>   # part content files only; no upload.json, no sidecars
```

`meta.redb` lives under `.tinio/` → naturally excluded from listing/serving (FR-020); sweep only cleans `tmp/`, never touches it.

### 5.2 Table design

| Table | key | value | Notes |
|---|---|---|---|
| `OBJECT_META` | `(bucket: &str, key: &str)` | `(etag_hex: &str, size: u64, mtime_nanos: u64, file_identity: u64, tags_wire: &str, checksum_wire: &str)` | composite key makes per-bucket range scans free (walk, remove_bucket); identity is used for composed-ETag touch/replace discrimination (§5.5.7). The tags and checksum elements are **empty strings when the object has none** (spec 2026-08-31); the checksum wire is `<algorithm wire>:<base64 value>:<kind>` — e.g. `CRC32:NhCmhg==:FULL_OBJECT` — with the kind (`FULL_OBJECT`/`COMPOSITE`) recorded at write time so read paths never derive it; garbage elements self-heal to empty/`None` on read (like the etag) |
| `BUCKETS` | `name: &str` | `(created_at_nanos: u64, tags_wire: &str)` | aligns with the existing buckets.json semantics; the tags element is empty when the bucket has none |
| `UPLOADS` | `(bucket: &str, upload_id: &str)` | `(key: &str, initiated_at_nanos: u64, tags_wire: &str)` | upload existence = record existence (replaces upload.json); the tags element is the create-time object tag set, applied to the object at completion |
| `PARTS` | `(bucket: &str, upload_id: &str, part_number: u32)` | `etag_hex: &str` | replaces the sidecars; size/mtime come from the part-file stat (real file state) |
| `UPLOAD_CHECKSUMS` | `(bucket: &str, upload_id: &str)` | `(algorithm_wire: &str, checksum_type_wire: &str)` | the upload's create-time checksum spec; `""` for a checksum type that was never fixed |
| `PART_CHECKSUMS` | `(bucket: &str, upload_id: &str, part_number: u32)` | `(algorithm_wire: &str, base64_value: &str)` | one uploaded part's computed checksum |
| `OBJECT_PARTS` | `(bucket: &str, key: &str, part_number: u32)` | `(size: u64, algorithm_wire: &str, base64_value: &str)` | the completed object's retained part list (GetObjectAttributes ObjectParts): the parts composed at the object's last multipart completion, in part order, with their stored checksums (`""` = part stored without one). Key shape mirrors `PARTS`. Drained on overwrite/delete/bucket removal, migrated on rename, never inherited by copy |
| `STATE` | `"version"` | `1` | validated at open: absent → write it (new DB); present but mismatched → error (mirrors buckets.json's version behavior); also holds the `compact_needed` marker row (see §5.8). **Version stays 1 across additive schema changes** — `UPLOAD_CHECKSUMS`/`PART_CHECKSUMS` and the tags/checksum elements + `OBJECT_PARTS` carried no bump (user ruling 2026-09-02: additive changes carry no version; a same-version DB from an older format may fail at row decode — delete and rebuild by hand) |

Encoding conventions: etags are stored as `ETag::as_str()` hex strings, validated into the domain type via `ETag::new` on read — consistent with the existing "storage layer uses plain strings, validation on read" convention; `mtime_nanos` reuses the existing `mtime_nanos()` helper. Tags persist as the canonical wire string (sorted `k=v&k2=v2`, RFC-3986 percent-encoded, shared with the `x-amz-tagging` header codec); read paths parse with the tolerant `tags_from_wire` mirror — the 10-cap core `parse_wire` cannot serve the 50-tag bucket cap.

**Range-scan boundary construction**: tuple comparison is per-element by byte order, the first element dominates; one bucket's entries are contiguous from the lower bound `(bucket, "")`. **No exclusive upper bound** — `(bucket + '\u{10FFFF}', "")` does not hold under byte order (`"data-x"`'s first element < `"data\u{10FFFF}"` because `'x'` (0x2D) < 0xF4, so bucket names with longer prefixes leak into the range; verified empirically). Correct approach: scan from `(bucket, "")` and stop when the first element stops matching (`drain_pair`/`collect_pairs` predicate boundary, O(range) + 1 lookahead). No assumptions about the key charset.

### 5.3 Transactions & concurrency

- **Handle sharing & construction seam (grilling G1 decision)**: `redb::Database` is **not `Clone`** (§4), compact needs exclusive `&mut` before sharing. **Two-phase construction**: `state::open(state_dir) -> Database` (task 1) → `compact_if_needed(&mut Database, threshold)` (task 6) → **`FsStorage::new_from_db(root, options, db)`** (takes the already-open Database, wraps it in `DbHandle`, stores hold `Arc<DbHandle>` clones); `FsStorage::new(root, options)` stays as the **convenience path** (internally open → marker/threshold evaluation → compact → delegate to from_db, for tests/examples). Orchestration (server startup / doctor --fix) explicitly uses the two phases; doctor --dry-run only opens + evaluates + reports, never constructs FsStorage. Independent constructors such as `MetaStore::new(state_dir)` remain (tests/examples; each with its own state_dir, no double-open of the same file).
- **DbHandle API shape (grilling G2 decision)**: closure-based `read(|tx| T)`/`write(|tx| T)` — the transaction lifetime is sealed inside the closure (guard escape is unexpressible); multi-table-one- transaction (remove_bucket's three-table range deletes) is one write closure; **the pipeline phase's write-lock histogram timing wrapper (pipeline-spec §4) lands on the write closure** (record wait before entry, total at closure end); this phase implements this shape already.
- **Single entries**: get/set/remove each take their own read/write transaction (no streaming dependency, open short transactions directly). **Synchronous call model (grilling G3 decision)**: async store methods **call redb directly and block** (no spawn_blocking) — short transactions + Immediate fsync are millisecond-scale; concurrency under a multi_thread runtime comes from multiple threads; consistent with the pipeline phase's "batched writes block on the write-pipeline thread" style (aligned with the tinio-mem precedent). **G3 REVISED by the data-path review (2026-08-27)**: write transactions move OFF the async request threads — `Handle::write`/`evaluate_compact` are now `async fn` that execute the closure + commit inside `tokio::task::spawn_blocking` (the per-commit `Immediate` fsync is millisecond-scale, so the inline block on a runtime worker was a real latency hazard); the redb single-writer lock still serializes commits, and the write-lock histogram timing (pipeline-spec.md §4/P5) spans the spawn_blocking hop — the blocking-pool queue delay counts as wait. Reads stay inline (no lock, no fsync).
- **Range operations** (all single-transaction atomic): `remove_bucket` (delete the OBJECT_META/UPLOADS/PARTS ranges per bucket), `walk` (read-transaction range scan, zero-copy inside the guard, copy outside), `load_all` (naturally ordered by key).
- **multipart**:
  - `create`: one write transaction inserts `UPLOADS` (no directory created anymore — the directory is created by the first `put_part`).
  - `put_part`: write the part file (tmp+rename) → one write transaction upserts `PARTS`.
  - `complete`: read-transaction validation (PARTS vs client manifest, strictly increasing + ETag match) → assemble into tmp (**copy-is-hash** — re-hash copied bytes per part, closing the verify-then-copy race: if a concurrent put_part overwrites a part, the copied bytes disagree with the composed ETag and it fails, with no second pass after assembly) → return the temp path; **does not consume**. The caller (backend) renames to the object target under the mutation lock, then calls `complete_object_state` (**one write transaction**: delete UPLOADS+PARTS and write the object's OBJECT_META entry, all-or-nothing — on failure it rolls back and the client retries idempotently), then best-effort `remove_part_dir` (failure is only logged; leftovers are reclaimed by the §5.7 orphan phase). **Implementation revision (2026-08-25)**: the production path does not go through `Store::consume`; `consume` (one write transaction deleting UPLOADS+PARTS → best-effort directory deletion, idempotent — no-op if already consumed) stays as a store-level API for tests and independent store-construction paths.
  - `abort` / `remove_bucket`: one write transaction deletes the range → best-effort directory deletion.
- **The complete read/write-transaction gap**: between validation (read transaction) and consumption (write transaction), a concurrent `put_part` can insert new part records — the client should not put_part while complete is in progress anyway (protocol-level); if it happens, the per-part re-hash during assembly catches it (InvalidPart), and leftover part files/directories are reclaimed by the FsCleanup orphan phase (§5.7).
- **complete idempotency (crash after rename)**: records survive until after the rename (`complete_object_state` is executed by the caller after the rename) — a crash between rename and the state transaction leaves the upload in UPLOADS; the client retries complete: revalidate + reassemble + rename (atomic over the same path) + state transaction, returning the same composed ETag.
- **remove_bucket vs in-flight uploads**: after remove_bucket deletes the range, a late put_part can resurrect that upload's PARTS rows and directory (bucket already deleted). The old implementation (upload.json) had the same race; semantics are preserved, not a regression; resurrected subtrees are reclaimed by the FsCleanup orphan phase (bucket-missing determination, §5.7).
- **Remove the in-process lock**: all three stores' `Arc<Mutex<()>>` are deleted (redb's single writer serializes writers).

### 5.4 Error model

`Error` gains a `Database(database::Error)` variant (aligned with tinio-mem); `database::Error` sub-variants are `Open(redb::DatabaseError)`/ `Transaction(TransactionError)`/`Table(TableError)`/`Storage(StorageError)`/ `Compaction(CompactionError)`/`Commit(CommitError)`/`Io(io::Error)` — **thiserror-derived `#[from]` per variant** (2026-08-25 revision: the draft's "explicit `From` per variant, no derived `#[from]`" contradicted the implementation; derive is authoritative, docs/style.md synced) — plus struct variants `UnsupportedVersion { path, found, expected }` and `CorruptMeta { key, source }` (explicit constructors `database::error::unsupported_version`/`corrupt_meta`); redb failures project to the contract `storage::Error::Io`. Note: the spec review draft once listed five top-level variants, of which `Storage(StorageError)` collided with the existing contract passthrough variant `Error::Storage(storage::Error)`; settled by nesting per style.md "redb nests in `Database(database::Error)`". **Existing-variant disposition (grilling G4 decision + 2026-08-25 revision)**: `Json` + `CorruptStateFile` removed (JSON-specific, disappear with the old layout; `InvalidMetaEntry` does not actually exist in the current error.rs, so there is nothing to remove); integrity is now `check_integrity`'s job, corrupt state is expressed as `Database(database::Error::Open)`; **no top-level `UnsupportedStateVersion`** — a STATE-table version mismatch is the nested `database::Error::UnsupportedVersion`, lifted to `Error::Database(..)` by the crate-level `From` (`database::Error::Io` is unwrapped separately to `Error::Io`); `InvalidPath`/`RootNotDirectory` stay.

**Dependency cleanup** (after all stores are migrated, in the wrap-up task, see task 7): remove the `Json`/`CorruptStateFile` error variants (G4 decision; version-check failures are the nested `database::Error::UnsupportedVersion`, see above); remove `serde`/ `serde_json` (no users once StoredEntry/BucketsFile/UploadFile are gone) and `sha1` (the fan-out location hash is gone) from tinio-fs dependencies (after confirming no dev-dependency leftovers). **Must not be removed during the skeleton phase** — the old implementation still uses them while it runs in parallel.

### 5.5 Semantics preserved (invariants, each must hold)

1. The `matches(size, mtime)` gate is unchanged — entries still store size+mtime, compared against the object-file stat on read; mismatch → stream-recompute-rewrite.
2. `get` missing → `None`; database-level failure → error (equivalent to today's "corrupt entry errors", and with redb's table-level consistency a single "bad entry" no longer exists).
3. `walk` yields all valid entries; `load_all` sorted by name.
4. `get_or_record`: records on first sight, stable afterwards.
5. `etag_for_file` / `etag_matching` signatures and semantics unchanged.
6. Public APIs and doc examples unchanged (rerun after the internal implementation swap).
7. multipart: part existence, `part_etag` point-query db-or-recompute fallback (**point-query path only** — when complete validation fetches etags per number from the client manifest, a missing PARTS record → recompute from the part file), complete validation (strictly increasing
   + ETag match), composed `MD5-of-MD5s-N` all unchanged. **`list_parts` is purely DB-driven**: parts without PARTS records do not appear in the list (no ghost parts, see §5.6 — such parts are necessarily retransmitted by the client, so a list fallback has neither a trigger point nor would it be correct).

### 5.6 Crash windows & recovery (multipart)

| Window | Consequence | Recovery |
|---|---|---|
| put_part: file written, PARTS not committed | the part has no DB record. **List is DB-driven → absent from list_parts**; the client never got a response so it retransmits, and the new write overwrites the old file via temp+rename | complete's point query finds no record → recompute the etag from the file (fallback stays on the point-query path); if the client gives up, the leftover file goes with its directory via abort/FsCleanup |
| a part is overwritten by a concurrent put_part during complete assembly | copied bytes disagree with the composed ETag | **copy-is-hash**: per-part re-hash during assembly; any file content ≠ the validated etag → InvalidPart failure, client retries (§5.3) |
| sweep/abort consumes the upload during complete assembly | part files gone, upload deleted | assembly (per-part re-hash) hits NotFound → re-check the upload; if already consumed, return NoSuchUpload (no bare I/O error); client retry → NoSuchUpload |
| crash after object rename, before the state transaction (`complete_object_state`) | upload still in UPLOADS (object correctly renamed; user-visible result is correct) | client retries complete idempotently (§5.3); leftover directory cleaned by `remove_part_dir`/orphan phase |
| after the state transaction, directory not deleted (`remove_part_dir` failed) | leftover upload directory with no UPLOADS record | FsCleanup cleans "multipart directories without DB records" (§5.7, draining their residual rows) |
| abort: same | same | same |

`idle_since` (for sweep) keeps scanning the part files' mtimes in the directory (files are real evidence of activity); upload existence and `initiated_at` come from UPLOADS.

### 5.7 FsCleanup adaptation

- Meta-orphan reclamation: `repair_meta_orphans` becomes an OBJECT_META range scan (entry → object-file existence check → per-entry reclaim); `RepairAction` stream and dry-run semantics unchanged.
- Bucket-orphan prune: BUCKETS-table entries whose bucket directory is missing → delete (table scan).
- **New phase**: clean up "multipart directories without UPLOADS records" (covers directories left after complete/abort commits, and subtrees whose bucket is gone).
  - **TOCTOU order pinned**: **enumerate the `multipart/` directory tree first, then open a single read transaction to read UPLOADS**, then decide. Safety basis: `create` commits UPLOADS first and `put_part` creates the directory (task-4 decision), so directory exists ⇒ the corresponding UPLOADS commit happened before directory creation ⇒ a read transaction opened after enumeration necessarily sees the record. The reverse order (read UPLOADS then enumerate) would misjudge fresh uploads and is forbidden.
  - **Activity grace**: a directory judged orphaned must still satisfy the same idle grace as sweep (by part-file mtime; falling back to the directory mtime when there are no part files — implementation revision 2026-08-25, so part-less orphan directories still have an idle age to judge) before deletion, to avoid racing a slow put_part.
  - `multipart/<bucket>/<upload_id>/` where the bucket directory is missing or upload_id is not in UPLOADS (and past the grace) → delete the directory, and **drain that directory's residual UPLOADS/PARTS rows** (`drain_upload_rows` — directories whose upload_id is not a UUID are handled the same way; row deletion does not depend on UUID validation).
- Never touching user data (bucket directories and objects) is unchanged.

### 5.8 Observability & operations

- **doctor** gains two items: `check_integrity()` integrity check (report `intact / repaired / unrecoverable`; when unrecoverable, suggest delete-and-rebuild — metadata is derivable, recompute fallback); a fragmentation-ratio evaluation report (dry-run only reports, `--fix` executes compact).
- Cost accepted: a single file is not grep/jq/per-entry-`du`-able; doctor provides equivalent checks. The blast radius goes from "single entry" to "the whole DB" — but metadata is derivable and self-heals.

### 5.9 compact evaluation, marking, and execution

**The `&mut self` hard constraint**: `Database::compact(&mut self)` needs an exclusive mutable reference. `Database` is not `Clone` (§4); once wrapped in `Arc<DbHandle>` and distributed to the stores, `&mut` is permanently unavailable at runtime (`Arc::get_mut` returns `None` while live clones exist). **Therefore compact has exactly one viable window: after the `Database` is opened and before it is wrapped in `DbHandle`/`FsStorage` is constructed.** This fixes the execution order below.

**Evaluation (stats-driven)**:

- `DbStats` snapshot: `begin_write().stats()?` + `abort()` (read-only use, the write transaction is not committed — note that `stats()` takes the write lock, so only low-frequency calls), exposing `allocated_bytes` (`allocated_pages × page_size`), `fragmented_bytes`, and the fragmentation ratio.
- `needs_compact(threshold) -> bool`: fragmentation ratio = `fragmented_bytes / allocated_bytes`; **the threshold is configurable**: `[storage.fs] compact_threshold_percent` (default 20, garde-validated 5..=90, compact needed when fragmentation ≥ threshold%); **absolute floor constant** `COMPACT_MIN_ALLOCATED = 64 MiB` (below it nothing triggers, avoiding pointless compacts of small DBs; Q1 decision).

**Marking (runtime → executed at next startup)**:

- The `STATE` table gains a `compact_needed` (0/1) row: `mark_compact_needed()` writes the marker, carrying "evaluated at runtime, executed at next startup".
- Evaluation timing: the end of every sweep cycle (one write-transaction stats call; low-frequency and acceptable) → set the marker if over threshold; doctor --fix evaluation sets/clears the marker too.

**Execution (offline; never compact at runtime)**:

- **At startup** (server startup orchestration, before readiness): `state::open` opens the `Database` (not yet shared) → check the marker + evaluate once directly (double insurance) → `compact()` if needed → clear the marker → **only then** construct `DbHandle`/`FsStorage`/the stores via `FsStorage::new_from_db` → startup repair (FsCleanup) → readiness. I.e. **compact strictly precedes FsStorage construction and startup repair** (grilling G1 two-phase construction; revision Q4 — the old "repair → compact" order is infeasible under the `&mut` constraint: repair needs stores, stores need the shared handle, and after sharing `&mut` is unreachable).
- **doctor --fix** (offline, service stopped): doctor opens the `Database` exclusively on its own (not shared), evaluates → reports → `compact()` → clears the marker; `--dry-run` only reports the evaluation. No sharing constraint.
- Fragmentation produced after compact (e.g. startup-repair writes) waits for the next mark-trigger cycle; this mechanism does not aim for zeroing in the same run.
- Note (implementation revision 2026-08-25): `compact_if_needed` clears the marker with a separate write transaction after `db.compact()`; if that commit fails, the DB is already compacted but the marker remains — the next startup repeats compact, harmless.

**Ownership**: the mechanism lives in tinio-fs (`state.rs`/`DbHandle` provides `open()`, `stats()`, `needs_compact()`, marker read/write, `compact_if_needed()`, `FsStorage::new_from_db` — compact_if_needed operates on the exclusive pre-share `Database`); the triggers live in tinio-server/tinio-cli (startup orchestration T068, doctor T073/T074 — these don't exist yet; trigger points land with the features; the CLI contract already has `tinio doctor [DIR] [--json] [--dry-run] [--fix]`, cli.md:72-75).

## 6. Acceptance criteria

1. `cargo test -p tinio-fs` fully green (unit + proptest + conformance); `cargo clippy --workspace` clean.
2. New layout holds: `<state-dir>/` contains only `meta.redb`, `tmp/`, `multipart/` (part content files); no `meta/objects/`, `buckets.json`, `upload.json`, `*.etag`.
3. Cold-start first list/HEAD is correct (entries computed on demand), second pass has zero recompute (`ScanSummary.recomputed == 0`).
4. Entries stay intact after concurrent puts (proptest retained; holds naturally under redb's single writer).
5. Full multipart lifecycle (create→parts→complete/abort→continue after restart) conformance green; crash-window tests pass (point-query recomputes from the file when the DB record is missing; list_parts never shows record-less parts; orphan directories reclaimed by FsCleanup).
6. doctor (FsCleanup): meta orphans + multipart directories without DB records — dry-run reports, fix clears, live data untouched (including unit tests for the TOCTOU order and the activity grace).
7. Soundness: delete `meta.redb`, restart → on-demand recompute self-heals (new-project scenario, not a committed rollback mechanism).
8. compact mechanism: create heavy write/delete churn (fragmentation over threshold) → startup path (before `Database` sharing) or doctor --fix triggers → file shrinks, data intact (round-trip check), the `compact_needed` marker is cleared; doctor dry-run reports the evaluation correctly; a shared `DbHandle` cannot trigger compact (compile-time/assert guarantee).
9. read-only compatibility (grilling G5/G6): construct FsStorage with an explicit home state_dir → reads + internal recompute writes work, meta.redb/tmp/multipart all in state_dir, root zero-write (no `.tinio/`, no new files); home resolution/rejection logic is verified end-to-end by T076/T058.

## 7. Risks & mitigations

| Risk | Mitigation |
|---|---|
| DB file corruption (hardware bit rot, external tampering) | `check_integrity` detects/repairs; unrecoverable → delete and rebuild (metadata derivable, recompute fallback) |
| Single-writer throughput ceiling | meta writes are all short transactions; this phase keeps the current recompute cadence; the batched-write pipeline is a separate later phase (`specs/001-s3-local-server/pipeline-spec.md`), optional benchmark gate |
| Cold-scan hashing load | this phase keeps the status quo (inline recompute + `BATCH_SIZE=32` yield); pipelined rate-limiting is the later phase |
| COW file only grows | stats evaluation (fragmentation threshold) + compact triggered at startup (pre-share) or doctor --fix (offline); runtime compact is structurally impossible (`&mut` unreachable) |
| put_part file-database window | semantics unchanged: point-query db-or-recompute fallback + client retransmit + FsCleanup orphan cleanup; list_parts is DB-driven so no ghost parts |
| Part listing switches from directory-driven to DB-driven | the only behavioral difference is "parts of a crashed request that never got a response" — the client necessarily retransmits; DB-driven is more correct (no ghost parts) |
| complete/remove_bucket racing a concurrent put_part | last-write-wins + the FsCleanup orphan phase reclaims resurrected subtrees (§5.3, §5.7); the old implementation had the same race, not a regression |

## 8. Decision log (review-decided)

| # | Decision | Settled |
|---|---|---|
| Q1 | compact minimum trigger floor | constant 64 MiB (nothing below triggers) |
| Q2 | compact threshold configurable | `[storage.fs] compact_threshold_percent` (default 20, 5..=90) |
| Q4 | startup orchestration order | **revised (2026-08-23)**: open `Database` → compact (marker/evaluation, exclusive `&mut`) → construct `FsStorage`/stores (shared handle) → startup repair (FsCleanup) → readiness. The old "repair → compact" order conflicts with `compact(&mut self)` + `Database` not `Clone`, infeasible |
| Q8 | redb `Durability` | always `Immediate`, not configurable; `Durability` is `#[non_exhaustive]`, matches have a wildcard |
| Q10 | section for fs-specific keys | `[storage.fs]` (nested sub-section, `[api.*]` precedent): `follow_symlinks` moves in from `[storage]` + `compact_threshold_percent`; `[storage]` keeps a future `type` selection key; `meta_batch_*` keys land with the pipeline phase (`specs/001-s3-local-server/pipeline-spec.md`). **Implementation revision (2026-08-25)**: as `follow_symlinks` moves into `[storage.fs]` its default flips from true to **false** (reject symlinks by default; access never resolves through links, listings exclude link entries) |

| G1 | FsStorage construction seam | **decided (grilling 2026-08-23)**: two phases — `state::open` → `compact_if_needed(&mut Database)` → `FsStorage::new_from_db(root, options, db)`; `FsStorage::new` is the convenience path (internally open → evaluate/compact → delegate to from_db); orchestration (server/doctor) explicitly uses the two phases, doctor --dry-run only evaluates, never constructs |
| G2 | DbHandle API shape | **decided (grilling)**: closure-based `read(\|tx\| T)`/`write(\|tx\| T)` — transaction lifetime sealed, guard escape unexpressible; multi-table-one-transaction is one write closure; pipeline-phase write-lock histogram timing wraps the write closure (wait before entry, total at end) |
| G3 | synchronous call model | **decided (grilling)**: async store methods **call redb directly and block** (no spawn_blocking) — short transactions + Immediate fsync are millisecond-scale, multi_thread handles concurrency; aligned with the tinio-mem precedent and the pipeline phase's batched-write style. **G3 REVISED by the data-path review (2026-08-27)**: writes move off the async threads — `Handle::write` is async and executes the closure + commit inside `spawn_blocking` (fsync-per-commit is millisecond-scale, not microseconds); single-writer lock semantics unchanged; the histogram wait/total timing spans the spawn_blocking hop (queue delay included in wait) |
| G4 | error variant disposition | **decided (grilling)**: remove `Json`/`InvalidMetaEntry`/`CorruptStateFile` (integrity is `check_integrity`'s job; corrupt state expressed as `Database(database::Error::Open)`); `InvalidPath`/`RootNotDirectory` stay. **Revised (2026-08-25)**: no top-level `UnsupportedStateVersion` — STATE version-check failure is the nested `database::Error::UnsupportedVersion`, lifted by the crate-level `From` to `Error::Database(..)`; redb/io sub-variants use thiserror-derived `#[from]` (the original "explicit From, no derive" contradicted the implementation; docs/style.md synced) |
| G5 | read-only mode scope | **decided (grilling)**: private state resolves via `state_dir` (home in read-only mode), root stays write-free; this phase has fs-layer tests, T076/T058 handle home resolution and data-plane rejection |
| G6 | read-only test shape | **decided (grilling)**: construct FsStorage with an explicit home state_dir (no need for T076 early — the state_dir override is enough) → read + recompute-write + root-zero-write assertion |

(G1–G6 are this phase's grilling decisions; Q1/Q2/Q3/Q3b/Q5/Q7/Q8/Q10 are pipeline-phase decisions, see `specs/001-s3-local-server/pipeline-spec.md`.)

---

# Part 2 — Plan (implementation)

## Phases & dependencies

```
1 tinio-fs skeleton ──▶ 2/3/4 three stores ──▶ 5 FsCleanup ──┐
6 compact + config (depends on 1) ───────────────────────────┴──▶ 7 tests/wrap-up ──▶ 8 integration acceptance
```

Every task is **test-first** (red → green).

## Task list

### 1. tinio-fs redb skeleton ✅ (done and verified)
- `Cargo.toml`: add `redb.workspace = true`. **Do not remove** `serde`/`serde_json`/`sha1` — the old implementation runs in parallel until task 7.
- `error.rs`: add the nested `database::Error` variants (redb/io sub-variants thiserror-derived `#[from]` — 2026-08-25 revision, was "explicit From"; version mismatch is the nested `UnsupportedVersion`, lifted by the crate-level From; redb I/O projects to `storage::Error::Io`); the `Json`/`InvalidMetaEntry`/`CorruptStateFile` variants are removed together in task 7 (G4 — after the 2026-08-25 revision these are already landed; the three variants no longer exist in error.rs).
- New `database/open.rs` (2026-08-25 revision: originally planned as `state.rs`; the implementation renamed): `open(state_dir) -> Open` — create/open `meta.redb`, create the five tables (idempotent; one write transaction at open creates all tables — read transactions refuse non-existent tables), STATE version validation (absent → write, mismatch → error → nested `database::Error::UnsupportedVersion`, G4).
- `database::Handle` (`handle.rs`, 2026-08-25 revision: originally planned as `DbHandle`): holds the shared `Database` handle (`Arc<Handle>` distributed to the stores); **closure-based API (G2)**: `read(|tx| T)`/`write(|tx| T)` (transaction lifetime sealed, guard escape unexpressible, multi-table-one-transaction is one closure); thin wrapper this phase, reserving the stats/evaluation extension point and the pipeline-phase write-lock timing wrapper slot.

**Acceptance**: error-conversion unit tests green; double-open does not error; STATE version round-trip / mismatch-error unit tests green; the skeleton compiles alongside the old implementation (the old layout files are still produced by the old implementation — layout acceptance is task 8).

### 2. tinio-fs MetaStore → OBJECT_META ✅ (done and verified)
- `get`/`set`/`remove`: single-entry read/write transactions.
- `walk`: read-transaction range scan over `(bucket, ..)` (boundary construction in §5.2), copy outside the guard, ordered by key.
- `remove_bucket`: delete the `(bucket, ..)` range in one transaction.
- `etag_matching`/`etag_for_file`: interface and semantics unchanged.
- Delete the fan-out path code (`entry_path`, `Sha1` use sites); the `sha1` dependency removal stays in task 7.

**Acceptance**: rewritten unit tests green (round-trip, missing→None, gate, remove idempotency, overwrite, unicode, remove_bucket, range boundaries with special-character bucket/key).

### 3. tinio-fs BucketStore → BUCKETS ✅ (done and verified)
- `created_at`/`get_or_record`/`record`/`remove`/`load_all` interfaces unchanged.
- The `file_format_has_version` test becomes a STATE version test.

**Acceptance**: rewritten unit tests green; the records-first-time/ stable-afterwards test retained.

### 4. tinio-fs MultipartStore → UPLOADS + PARTS ✅ (done and verified)
- `create`: one write transaction inserts UPLOADS; no directory created, no upload.json written (the directory is created by the first put_part — §5.7's TOCTOU order depends on this invariant).
- `put_part`: write the part file → one write transaction upserts PARTS (no sidecars).
- `list_parts`: read-transaction PARTS range scan, **purely DB-driven** (record-less parts don't appear); size/mtime stat the part files.
- `part_etag` point query: DB record missing → recompute from the part file (this path only).
- `complete`: validate (strictly increasing + existence + ETag match) → assemble (copy-is-hash, closing the verify-then-copy race; no second pass after assembly) → return the temp path without consuming; the caller renames then calls `complete_object_state` (one transaction deleting UPLOADS+PARTS + writing the OBJECT_META entry) then best-effort `remove_part_dir` (2026-08-25 revision: the production path does not go through `Store::consume`; `consume` stays as the store-level idempotent API, used by tests/independent-construction paths).
- `abort`/`remove_bucket`: one transaction deletes the range → best-effort directory deletion.
- `list_uploads`/`walk_uploads`/`has_uploads`: DB range scans.
- `idle_since`: keeps scanning the part files' mtimes.

**Acceptance**: multipart unit tests + proptest (assembly against an independent reference, part-overwrite last-write-wins, numbering boundaries) all green; crash-window tests (point-query recomputes from the file when the DB record is missing; list_parts never shows record-less parts) pass; conformance full lifecycle green.

### 5. tinio-fs FsCleanup adaptation ✅ (done and verified; doctor CLI wiring lands with T073/T074, out of scope per spec §5.9)
- `repair_meta_orphans`: OBJECT_META range scan.
- bucket-orphan prune: BUCKETS table scan.
- New "multipart directories without UPLOADS records" cleanup phase: **enumerate the directory tree first, then open one read transaction to read UPLOADS** (§5.7 order), judge, then pass the idle grace before deleting.
- New doctor integrity check (`check_integrity`, lands with the doctor feature).

**Acceptance**: cleanup unit tests green (dry-run/fix, live data untouched, orphan-directory cleanup, TOCTOU order + grace race unit tests).

### 6. tinio-fs compact evaluation, marking, execution + `[storage.fs]` config ✅ (done and verified; startup orchestration / doctor --fix trigger points land with T068/T073/T074, out of scope per spec §5.9)
- `state.rs`: `DbStats` snapshot (`begin_write().stats()?` + abort), `needs_compact(threshold)` pure function (fragmentation ratio = fragmented_bytes / allocated_bytes; threshold from config `[storage.fs] compact_threshold_percent` (default 20, 5..=90); floor constant 64 MiB), STATE-table `compact_needed` marker read/write, `compact_if_needed(&mut Database)` (operates on the exclusive pre-share handle: evaluate → compact → clear the marker).
- **`FsStorage::new_from_db(root, options, db)` (G1)**: takes the already-open `Database` (post-compact) and constructs the stores; `FsStorage::new(root, options)` becomes the convenience path (internally open → marker/threshold evaluation → compact → delegate to from_db).
- tinio-fs exposes the mechanism; trigger points (startup orchestration, doctor --fix) are marked as landing with tinio-server/tinio-cli T068/T073/T074; this task implements the fs-side mechanism + unit tests.
- Startup orchestration order (Q4 revision + G1): **state::open → compact (pre-share) → FsStorage::new_from_db → startup repair → readiness**.
- Config wiring: tinio-config `StorageConfig` gains an `fs` sub-structure (`[storage.fs]`: `follow_symlinks` moved in from `[storage]` + `compact_threshold_percent`, schema + garde) → `FsOptions`; contracts/config.md updated in sync (`[storage] follow_symlinks` → `[storage.fs] follow_symlinks`).
- Never compact at runtime (`&mut` structurally unreachable).

**Acceptance**: `needs_compact` threshold-logic unit tests green (including the config threshold and the floor constant); marker round-trip; compact integration test (write/delete many entries → fragmentation over threshold → exclusive-handle compact → file shrinks, data intact, marker cleared); config unit tests green (`[storage.fs]` parse/validate/defaults, `follow_symlinks` behavior unchanged after the move).

### 7. Tests & wrap-up ✅ (done and verified, incl. optional T030-style meta-hit benchmark `benches/meta.rs`)
- Rewrite `proptest_meta.rs` (round-trip, composed etag, no torn writes under concurrency).
- Crash-window tests consolidated (point-query recompute fallback, orphan-directory cleanup).
- **Dependency cleanup**: after confirming all three stores are migrated — remove the `Json`/`CorruptStateFile` error variants (G4; version-check failures are the nested `database::Error::UnsupportedVersion`), `serde`/`serde_json`/`sha1` dependencies (full sweep incl. dev-dependencies and doc examples; after write.rs tests switch to string comparison, serde_json is removed too), the three stores' `Arc<Mutex<()>>` (the whitelist keeps `bucket_mutation_lock` and multipart `PartLocks` — the former serializes bucket-directory changes, the latter serializes same-upload part rename+record transactions; both are semantically required, 2026-08-25 revision), fan-out leftover code.
- **read-only-compatible fs-layer tests (G5/G6)**: construct FsStorage with an explicit home state_dir → reads + internal recompute writes work, meta.redb/tmp/multipart all in state_dir, root zero-write (no `.tinio/`, no new files).
- Optional: T030-style meta-hit benchmark (guard against B+ tree lookup regression).

**Benchmark reference data** (`cargo bench -p tinio-fs --bench meta`, 2026-08-24, Windows 11 / release build; measured after seeding 100k entries into a single-bucket `OBJECT_META`; T088 uses this as the Phase 6 baseline-gate start point):

| Benchmark | Measured | Notes |
|---|---|---|
| `meta_hits/get_hit_100k` | ~1.8 µs | single-entry B+ tree point-query hit |
| `meta_hits/etag_matching_hit_100k` | ~1.8 µs | FR-022 full gate hit (size+mtime compare) |
| `meta_walk/walk_100k` | ~28 ms | full-bucket range scan (list/remove_bucket path), ~3.5M entries/sec |

**Acceptance**: proptest fully green; `cargo tree -p tinio-fs` shows no serde/serde_json/sha1; lock-free compile (`grep -r "Mutex" crates/tinio-fs/src` shows only the whitelisted leftovers: `bucket_mutation_lock` and multipart `PartLocks` — the former serializes bucket-directory changes, the latter serializes same-upload part rename+record transactions; both semantically required, 2026-08-25 revision); read-only compatibility test green (root-zero-write assertion); clippy clean.

### 8. Integration acceptance ✅ (tinio-fs + affected crates full test + clippy green; acceptance criteria 1–9 all have fs-layer evidence; server end-to-end verification lands with T068/T073/T074)
- Full `cargo test` (tinio-fs and affected crates) + `cargo clippy --workspace`.
- Verify acceptance criteria 1–9: new layout, cold-start/second-hit, concurrent integrity, multipart lifecycle, doctor checks, delete-and- self-heal, compact, read-only compatibility.

**Acceptance**: all criteria in Part 1 §6 pass.

## Verification approach

- Per task: that task's tests go red then green; task 8 runs the full test suite + clippy + server-layer verification.
- Optional manual: start the server → upload objects / multipart → observe `meta.redb` being produced, the old layout files absent → doctor integrity check, fragmentation report, and orphan cleanup.

