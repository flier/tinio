# Design: tinio-fs Backend (Filesystem Backend)

**Branch**: `001-s3-local-server` | **Date**: 2026-08-22

Design details for the filesystem backend (`tinio-fs`, v1) — the v1 implementation of the `tinio-core` storage contract (`Storage` + `Cleanup` traits, task T012). This document is the single home for fs-specific behavior and algorithms: path mapping, atomic writes, the ETag meta store, bucket creation times, listing, multipart, the sweep, the `Cleanup` impl, and the scanner walk. **Future backends (tinio-s3, tinio-webdav) define their own behavior, documented in their own backend documents; nothing in this file is assumed to carry over.**

Related: [failure-handling.md](failure-handling.md) (abnormal conditions, reclamation division of labor), [scanner.md](scanner.md) (scanner design), [data-model.md](data-model.md) (state layout — `tmp/`, `multipart/`, `meta.redb` schemas), [contracts/s3-surface.md](contracts/s3-surface.md) (protocol behaviors: folder markers, symlink policy, charset, Range/conditional requests, error codes).

## 1. Path mapping (`crates/tinio-fs/src/path.rs`, T037)

- Bucket → `<root>/<bucket>`; key → path relative to the bucket; nested keys map to nested directories.
- Containment is proven by `strict-path` (`PathBoundary::try_new` + `strict_join` — a canonicalize + boundary check) on the existing prefix; tinio only **supplements** rules the crate does not provide (or that conflict with its Windows reserved-name policy). The returned path is the **lexical** join — the proof is a gate, not a rewrite — so the I/O-time symlink policy in `objects.rs` (a link inside a bucket is refused even when contained) stays authoritative, and listing/write consistency is unchanged.
- Traversal (`..`), absolute paths, and control characters are rejected **before any FS access** (FR-006; `tinio-core` `object::key` / `bucket::name` validation is reused). Supplements run before the boundary proof (zero FS side effects on bad keys).
- **Empty interior segments** (`a//b`, `a\\b`, mixed `a/\b`) are rejected at the contract boundary (`tinio-core` `object::key` → `InvalidKey`, all backends agree): the mirror cannot represent distinct keys that map to one OS path — consecutive `/` or `\` separators alias the single-separator form. The trailing empty segment of a folder marker (`dir/`) is the one legal empty segment. A single `\` (`a\b`) stays a legal key (Unix filename; Windows path mapping still joins it as a separator).
- `.tinio` is a reserved segment at ANY depth: writes rejected (`AccessDenied`), reads return `NoSuchKey`, listings skip (FR-020) — this also protects nested roots (an outer server never serves an inner root's state).
- Platform charset: universal rules apply everywhere; Windows-invalid characters are rejected on Windows only. Windows also refuses **8.3 short-name shapes** (`PROGRA~1`, `FILE~1.TXT`): `strict-path` only expands 8.3 names of *existing* components, so a *new* 8.3-shaped key could alias a later out-of-band sibling (SC-006 serves out-of-band files) — the shape is refused outright (future backends define their own).
- Case sensitivity follows the host filesystem (no artificial enforcement on case-insensitive hosts).
- **`follow_symlinks` ↔ `enforce_boundary`**: object operations pass `enforce_boundary = !follow_symlinks` — the default refuses symlink/junction escape at the path layer (an earlier gate than the I/O-time checks); opt-in `true` skips the proof and returns the plain join (escape stays an I/O-time concern). Cleanup/scan paths always enforce (they must never address outside the bucket). `state_dir` always proves containment — a pre-existing `<root>/.tinio` resolving outside the root is refused. Bucket directories: under `follow_symlinks = false` a symlinked/junction bucket directory is refused by the containment proof and answers `NoSuchBucket` everywhere (invisible to listings, discovery, scanner, cleanup). Under `follow_symlinks = true` the bucket directory is **resolved to its canonical target** — the bucket *is* the target (a legit way to put a bucket on another volume); all proofs run against the target, and `list_buckets`/scanner/cleanup discover it like any other bucket.
- Boundary proofs are cached per `FsStorage` (bounded, identity-checked via dev+inode on Unix — a replaced directory rebuilds). Windows has no stable *directory* identity for the cache (the creation FILETIME does not change on recreation), so the cache never hits there and every call rebuilds the proof (correct, one extra canonicalize per mapping). Object **file** identity on Windows uses `volume_serial_number` + `file_index` (`MetadataExt`, stable), so the composed-ETag touch-vs-replace distinction is exact there too; filesystems without a file ID report `0` and fall back to the mtime jitter window (meta.rs).
- `state_dir` is fallible: it refuses a pre-existing `<root>/.tinio` symlink/junction instead of following it (a `database::open` through a linked state dir would write outside the root). The FR-023 relocation override bypasses this mapping (admin/CLI-provided, config-validated).
- Folder markers: keys ending in `/` are never objects — PUT creates the directory (idempotent), GET/HEAD return `NoSuchKey`, DELETE always returns 204 and removes the directory only when empty (s3-surface.md).
- **Known mirror limitation**: a key `dir` (an object file) and the folder marker `dir/` (a directory) cannot coexist on the filesystem — PUT `dir/` over an existing `dir` file fails with an I/O error. S3 allows both as distinct keys; the fs mirror (SC-006) cannot. (tinio-mem keeps them distinct.)
- Root identity is the canonical path: renaming/re-linking the root yields a new derived home state dir and regenerated credentials (documented behavior).

## 2. Atomic streaming writes (`crates/tinio-fs/src/write.rs`, T038)

- Every object write streams into a temp file under `<state-dir>/tmp/`, then `fs::rename` to the final path — atomic on the same volume (in normal mode the state dir is inside the root, so tmp and target share a volume). A cross-volume state dir (`FsOptions.state_dir` on another volume, FR-023 relocation) makes the rename fail with `CrossesDevices`: the fallback copies the temp through a unique staging file inside the target directory's `.tinio/` reserved segment (FR-020 — invisible to the data plane), then renames it (atomic on the target volume); a crash between copy and rename leaves invisible residue, not a served stray. The source temp is removed on the success path too (the copy does not consume it). Residue in a bucket's `.tinio/` staging directory is reclaimed at startup (§8.1) and never counts toward bucket emptiness (the delete walk skips `.tinio` at any depth).
- Bounded buffers (constitution V): no per-object allocation; the ETag MD5 is computed while streaming (FR-010/022).
- Last-write-wins: the last completed rename wins; a GET during an upload sees the previous object or not-found — never a torn mix (FR-011).
- An interrupted upload leaves only a temp file: invisible to listings, reclaimed at startup (full clear, §8.1) or by the sweep after 24 h mtime (§7).

## 3. ETag meta store (`crates/tinio-fs/src/meta.rs`, T039; redb since meta-redb-spec)

- Layout: `OBJECT_META` table of `<state-dir>/meta.redb` — key `(bucket, key)`, value `(etag hex, size, mtime)` (entry schema in data-model.md). The composite key keeps one bucket's entries contiguous: walks and bucket deletions are prefix range scans in one transaction.
- Validation: an entry is served only when size+mtime match the object file; otherwise the ETag is recomputed streaming and the entry rewritten (FR-022).
- Atomic writes: redb transactions (single-writer, crash-safe by default — commit is atomic, no torn state).
- Known granularity limit: an out-of-band edit preserving both size and mtime tick may serve a stale ETag (accepted trade-off, FR-022). A multipart `MD5-of-MD5s-N` ETag survives a same-file touch (antivirus/indexer): the `OBJECT_META` value stores the file identity (unix dev+inode) and a touch keeps the composed form while a same-size replacement (new inode) re-hashes — no clock threshold. On platforms without an identity (Windows), the 60 s mtime jitter window is the fallback.
- Last-Modified is always read from the FS mtime (actual file state).
- Orphaned entries (entry whose object file is gone) are reclaimed by the scanner (§9) through `FsCleanup` (§8.3).

## 4. Bucket creation times (`crates/tinio-fs/src/buckets.rs`, T040; redb since meta-redb-spec)

- `BUCKETS` table of `<state-dir>/meta.redb`: `name` → created-at unix nanos.
- Lazy recording on first sight (pre-existing directories get their creation time recorded on first list/head; one atomic upsert).
- Orphaned entries (bucket directory gone) are pruned on bucket delete and at startup repair (§8.1).

## 5. Listing (`crates/tinio-fs/src/listing.rs`, T043)

- Prefix filtering, delimiter-based grouping (common-prefix roll-up), pagination per S3 semantics (FR-004).
- ETags included: missing/stale entries are recomputed synchronously during the listing — the documented one-time full-content pass over externally-added files (the accepted cost of SC-006 mirror semantics), mitigated by the background scanner (§9).
- `.tinio` entries are always skipped.
- No listing latency bound is promised; listings remain correct and complete at all times.

## 6. Multipart (`crates/tinio-fs/src/multipart.rs`, T044; redb since meta-redb-spec)

- Part content files at `<state-dir>/multipart/<bucket>/<uploadId>/part-<n>` (layout in data-model.md); part numbers 1..=10000. Upload records live in the `UPLOADS` table (`(bucket, upload_id)` → `(key, initiated_at)`), part ETags in `PARTS` (`(bucket, upload_id, part_number)` → etag) of `meta.redb` — no `upload.json`, no `.etag` sidecars.
- `create` commits the `UPLOADS` record only; the upload directory is created by the first `put_part` (the orphan-cleanup TOCTOU order depends on this, §8.1).
- Assembly streams all parts into a temp file, hashing each part's copied bytes in the same pass, then atomic rename; the composed ETag `MD5-of-MD5s-N` matches the AWS reference composition. `complete` closes the verify-then-copy race by hashing the copied bytes — a concurrent `put_part` overwriting a part mid-assembly yields copied bytes that disagree with the verified ETag and fails the completion (no post-assembly re-read of the parts), then the records are deleted in one transaction and the directory removed best-effort. `complete` does not consume the upload — the backend renames first, then commits `complete_object_state` — so a crash between rename and the state transaction leaves the upload listed and a client retry completes idempotently.
- `list_parts` is DB-driven (a part whose `PARTS` record never committed is invisible — the client retransmits, and the point query recomputes from the file as a crash-window fallback).
- Abort removes the parts subtree; parts survive restarts, so cross-restart completion/abort is legal (quickstart §7).
- No 5 MB minimum (FR-014).
- Idle uploads (no part writes and not completed) expire via the sweep after the configured TTL (§7).
- In read-only mode no part files are ever created (all multipart operations are rejected, FR-023).

## 7. Sweep (`crates/tinio-fs/src/sweep.rs`, T046)

- Time-driven, mtime-based cleanup that runs while the server is live: temp files older than 24 h (`[s3] temp_ttl_hours`), multipart uploads idle more than 7 days (`[s3] multipart_expire_days`, idle = max(initiated_at, latest part mtime)).
- Non-blocking and yields to request traffic (FR-014).
- Complements, does not replace, the event-driven `FsCleanup` (§8.4 comparison).

## 8. FsCleanup — `Cleanup` trait impl (`crates/tinio-fs/src/cleanup.rs`, T070)

The fs implementation of the `Cleanup` trait (tinio-core, T012). Three callers: the start orchestration (startup repair, T068), `doctor` (diagnostics + `--fix`, T073/T074), and the scanner (meta-orphan reclamation, T045). All modes share one code path with a `dry_run` flag; every action is logged to the operational log; **user data (bucket directories and objects) is never touched**.

### 8.1 Startup repair

Runs after single-instance binding, before readiness (SC-005). Fast, deterministic items only — nothing that requires a full-tree walk:

- **`tmp/` full clear**: at startup there are no active writers, so every file under `<state-dir>/tmp/` is a crash leftover — the whole directory is emptied unconditionally (unlike the sweep, which is mtime-driven because it runs while the server is live).
- **Bucket-orphaned multipart subtrees**: `multipart/<bucket>/<uploadId>/` whose `<bucket>` directory no longer exists at `<root>/<bucket>` is removed. Cross-restart uploads (bucket still exists) are **never** touched — completing or aborting them after a restart is legal (quickstart §7).
- **Upload directories without a `UPLOADS` record** (complete/abort committed but the directory removal failed; revived subtrees from a `put_part` racing a bucket removal): the multipart tree is **enumerated first, then `UPLOADS` is read in one transaction** (the TOCTOU order — a directory exists only after its record commit, so the read sees every live upload); a judged orphan is deleted only after its parts have been idle past the sweep's `multipart_ttl` grace (a slow `put_part` must not be interrupted).
- **Stale bucket records**: `BUCKETS` entries whose bucket directory is gone are pruned.

### 8.2 Doctor diagnostics and fix (dry-run aware)

- `check` mode (doctor, T073) reports what a fix would change without touching anything: the same items as startup repair, plus meta orphans (§8.3), the `meta.redb` integrity check and fragmentation report, and stale home root-state dirs.
- `fix` mode (doctor `--fix`, T074) applies them through the same code path — the startup repair items plus meta orphans (§8.3), `check_integrity` (redb's automatic repair; an unfixable database is reported for deletion and rebuild — the metadata is derivable) and offline compact (fragmentation ≥ `[storage.fs] compact_threshold_percent`, §10), and home root-state-dir GC. Requires the server for the target root to be stopped (live control-channel probe → error).

### 8.3 Meta-orphan reclamation (scanner path)

During a scan, a meta entry whose object file no longer exists is deleted (the object may have been removed out-of-band). Reclamation and recomputation share the same walk — there is no separate pass.

### 8.4 Relationship to the sweep

| | Sweep (§7) | FsCleanup |
|--|-----------|-----------|
| Trigger | Time-driven (mtime TTLs: temps 24 h, multipart idle 7 d) | Event-driven (startup / doctor / scan) |
| Runs while server is live | Yes (yields to traffic) | Startup: before readiness; doctor: offline; scanner: background |
| `tmp/` | Only files older than TTL | Full clear at startup |

Neither replaces the other: the sweep bounds disk leakage from mid-run interruptions; FsCleanup repairs crash leftovers immediately.

## 9. Fs scanner walk (`crates/tinio-fs/src/scanner.rs`, T045)

Implementation of the scanner design in [scanner.md](scanner.md) for the filesystem backend:

- **Walk model**: directory-tree walk over the storage root, one bucket directory at a time, skipping the reserved `.tinio/` (any depth) and symlinks when `follow_symlinks` is disabled.
- **Reconciliation**: per [scanner.md §2](scanner.md) — missing → compute; size/mtime mismatch → recompute; object gone → reclaim via §8.3; match → skip. Reads/writes meta via `crates/tinio-fs/src/meta.rs` (redb transactions on `OBJECT_META`).
- **Yield**: after a bounded batch of entries, yield to the runtime so in-flight S3 requests preempt scanning (tuned by the T093 cold/warm benchmark).
- **Pacing**: `[scanner]` delay/max_wait/cycle per contracts/config.md; `TINIO_SCANNER` env override.
- **Read-only mode**: the walk runs identically, but meta writes land in the home state dir (`~/.tinio/roots/<sha1>/`, FR-023).

## 10. State-database compaction and integrity (`crates/tinio-fs/src/state.rs`)

redb's copy-on-write file only grows; `Database::compact(&mut self)` needs an exclusive `&mut`, which is structurally impossible once the handle is shared (`Database` is not `Clone`). Compaction therefore runs **offline only**, in the two-stage construction: open → compact (marker + fragmentation evaluation) → construct `FsStorage` over the shared handle. The runtime never compacts.

- Evaluation: fragmentation ratio `fragmented / allocated` ≥ `[storage.fs] compact_threshold_percent` (5..=90, default 20) and allocated ≥ 64 MiB (below the floor a rewrite gains nothing).
- Marker: the `STATE` table's `compact_needed` row carries the runtime evaluation to the next startup (the sweep evaluates once per round; over threshold → marker set).
- Triggers: startup (before the stores are constructed) and `doctor --fix` (offline, exclusive open); `--dry-run` only reports the evaluation.
- `check_integrity()`: doctor's integrity check — `Ok(true)` healthy / `Ok(false)` repaired / `Err(Corrupted)` unfixable (delete and rebuild; metadata is derivable).

## 11. Scope and reference boundaries

To keep this document the single home for fs design without duplicating others:

- **Layouts and schemas** (`tmp/`, `meta.redb`, `multipart/`): [data-model.md](data-model.md) — referenced, not duplicated.
- **Protocol behaviors** (folder markers, symlink policy, key charset, Range/conditional requests, error codes): [contracts/s3-surface.md](contracts/s3-surface.md).
- **Abnormal-condition taxonomy and reclamation division of labor**: [failure-handling.md](failure-handling.md).
- **Scanner pacing, lifecycle, and concurrency correctness**: [scanner.md](scanner.md).
