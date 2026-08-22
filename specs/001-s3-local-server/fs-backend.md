# Design: tinio-fs Backend (Filesystem Backend)

**Branch**: `001-s3-local-server` | **Date**: 2026-08-22

Design details for the filesystem backend (`tinio-fs`, v1) — the v1 implementation of the `tinio-core` storage contract (`Storage` + `Cleanup` traits, task T012). This document is the single home for fs-specific behavior and algorithms: path mapping, atomic writes, the ETag meta store, bucket creation times, listing, multipart, the sweep, the `Cleanup` impl, and the scanner walk. **Future backends (tinio-s3, tinio-webdav) define their own behavior, documented in their own backend documents; nothing in this file is assumed to carry over.**

Related: [failure-handling.md](failure-handling.md) (abnormal conditions, reclamation division of labor), [scanner.md](scanner.md) (scanner design), [data-model.md](data-model.md) (state layout — `tmp/`, `meta/`, `multipart/`, `buckets.json` schemas), [contracts/s3-surface.md](contracts/s3-surface.md) (protocol behaviors: folder markers, symlink policy, charset, Range/conditional requests, error codes).

## 1. Path mapping (`crates/tinio-fs/src/path.rs`, T037)

- Bucket → `<root>/<bucket>`; key → path relative to the bucket; nested keys map to nested directories.
- Traversal (`..`), absolute paths, and control characters are rejected **before any FS access** (FR-006; `tinio-core` `object::key` / `bucket::name` validation is reused).
- `.tinio` is a reserved segment at ANY depth: writes rejected (`AccessDenied`), reads return `NoSuchKey`, listings skip (FR-020) — this also protects nested roots (an outer server never serves an inner root's state).
- Platform charset: universal rules apply everywhere; Windows-invalid characters are rejected on Windows only (future backends define their own).
- Case sensitivity follows the host filesystem (no artificial enforcement on case-insensitive hosts).
- Folder markers: keys ending in `/` are never objects — PUT creates the directory (idempotent), GET/HEAD return `NoSuchKey`, DELETE always returns 204 and removes the directory only when empty (s3-surface.md).
- Root identity is the canonical path: renaming/re-linking the root yields a new derived home state dir and regenerated credentials (documented behavior).

## 2. Atomic streaming writes (`crates/tinio-fs/src/write.rs`, T038)

- Every object write streams into a temp file under `<state-dir>/tmp/`, then `fs::rename` to the final path — atomic on the same volume (in normal mode the state dir is inside the root, so tmp and target share a volume; cross-device rename failures fail the request, see failure-handling.md §2C).
- Bounded buffers (constitution V): no per-object allocation; the ETag MD5 is computed while streaming (FR-010/022).
- Last-write-wins: the last completed rename wins; a GET during an upload sees the previous object or not-found — never a torn mix (FR-011).
- An interrupted upload leaves only a temp file: invisible to listings, reclaimed at startup (full clear, §8.1) or by the sweep after 24 h mtime (§7).

## 3. ETag meta store (`crates/tinio-fs/src/meta.rs`, T039)

- Layout: git-style 2-hex fan-out `meta/objects/<bucket>/<2hex>/<sha1>.json` (entry schema in data-model.md). The fan-out avoids huge flat directories and Windows path-length limits; bucket deletion is a subtree removal.
- Validation: an entry is served only when size+mtime match the object file; otherwise the ETag is recomputed streaming and the entry rewritten (FR-022).
- Atomic writes (temp + rename) under an in-process lock — concurrent writers never produce torn JSON.
- Known granularity limit: an out-of-band edit preserving both size and mtime tick may serve a stale ETag (accepted trade-off, FR-022).
- Last-Modified is always read from the FS mtime (actual file state).
- Orphaned entries (entry whose object file is gone) are reclaimed by the scanner (§9) through `FsCleanup` (§8.3).

## 4. Bucket creation times (`crates/tinio-fs/src/buckets.rs`, T040)

- `buckets.json` = `{"version": 1, "buckets": {name: created_at}}`; written atomically (temp + rename) under an in-process lock.
- Lazy recording on first sight (pre-existing directories get their creation time recorded on first list/head).
- Orphaned entries (bucket directory gone) are pruned on bucket delete and at startup repair (§8.1).

## 5. Listing (`crates/tinio-fs/src/listing.rs`, T043)

- Prefix filtering, delimiter-based grouping (common-prefix roll-up), pagination per S3 semantics (FR-004).
- ETags included: missing/stale entries are recomputed synchronously during the listing — the documented one-time full-content pass over externally-added files (the accepted cost of SC-006 mirror semantics), mitigated by the background scanner (§9).
- `.tinio` entries are always skipped.
- No listing latency bound is promised; listings remain correct and complete at all times.

## 6. Multipart (`crates/tinio-fs/src/multipart.rs`, T044)

- Parts at `<state-dir>/multipart/<bucket>/<uploadId>/part-<n>` (layout in data-model.md); part numbers 1..=10000.
- Assembly streams all parts into a temp file, then atomic rename; the composed ETag `MD5-of-MD5s-N` matches the AWS reference composition.
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
- **Stale `buckets.json` entries**: entries whose bucket directory is gone are pruned.

### 8.2 Doctor diagnostics and fix (dry-run aware)

- `check` mode (doctor, T073) reports what a fix would change without touching anything: the same items as startup repair, plus meta orphans (§8.3) and stale home root-state dirs.
- `fix` mode (doctor `--fix`, T074) applies them through the same code path — the startup repair items plus meta orphans (§8.3) and home root-state-dir GC. Requires the server for the target root to be stopped (live control-channel probe → error).

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
- **Reconciliation**: per [scanner.md §2](scanner.md) — missing → compute; size/mtime mismatch → recompute; object gone → reclaim via §8.3; match → skip. Reads/writes meta via `crates/tinio-fs/src/meta.rs` (atomic temp+rename, in-process lock).
- **Yield**: after a bounded batch of entries, yield to the runtime so in-flight S3 requests preempt scanning (tuned by the T093 cold/warm benchmark).
- **Pacing**: `[scanner]` delay/max_wait/cycle per contracts/config.md; `TINIO_SCANNER` env override.
- **Read-only mode**: the walk runs identically, but meta writes land in the home state dir (`~/.tinio/roots/<sha1>/`, FR-023).

## 10. Scope and reference boundaries

To keep this document the single home for fs design without duplicating others:

- **Layouts and schemas** (`tmp/`, `meta/` fan-out, `multipart/`, `buckets.json`): [data-model.md](data-model.md) — referenced, not duplicated.
- **Protocol behaviors** (folder markers, symlink policy, key charset, Range/conditional requests, error codes): [contracts/s3-surface.md](contracts/s3-surface.md).
- **Abnormal-condition taxonomy and reclamation division of labor**: [failure-handling.md](failure-handling.md).
- **Scanner pacing, lifecycle, and concurrency correctness**: [scanner.md](scanner.md).
