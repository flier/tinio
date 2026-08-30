# Design: Failure & Abnormal-Condition Handling

**Branch**: `001-s3-local-server` | **Date**: 2026-08-22

Systematic design for how tinio detects and handles abnormal conditions. This document consolidates the spec Edge Cases, FR-011/014/020/022/023, the crash-recovery behavior, and the startup-repair/doctor design into one implementation reference. Related: [scanner.md](scanner.md) (meta-orphan reclamation), [fs-backend.md](fs-backend.md) (fs backend design), [data-model.md](data-model.md) (state layout), [contracts/cli.md](contracts/cli.md) (command behavior).

## 1. Invariants

These hold under every abnormal condition in this document:

- **No torn objects**: an object is either absent, the previous complete version, or the new complete version — never a mix (temp file + atomic `fs::rename`, FR-011).
- **No partial object visible**: an interrupted upload never surfaces as a completed object (temp files live under `<state-dir>/tmp/`, invisible to listings).
- **User data is never cleaned**: live bucket directories and object files are never removed by automatic repair. Only tinio-private state (`tmp/`, `meta.redb`, `multipart/`, `state`, socket, logs — including unpublished delete tombstones under `<root>/.tinio/deleting/`, residue of a user-initiated DeleteBucket) is eligible.
- **Atomic state writes**: `state`/socket are written temp+rename (no torn JSON, FR-020/022); derived metadata lives in `meta.redb` — redb transactions are crash-safe by default (commit is atomic; no torn state, no replay corruption).
- **Fail-request, keep-running**: a mid-operation environment failure (permissions, disk, unlink) fails that request with a meaningful error while the server keeps running.
- **State/socket lifecycle**: removed on graceful stop; probed and reclaimed at next start after a crash.

## 2. Classification and Handling

### A. Startup failures (exit before readiness, exit code 1, message to stderr)

| Condition | Detection | Handling |
|-----------|-----------|----------|
| Port already in use | bind error | Clear startup error, exit 1 |
| Second instance on same root | control-channel bind (unix socket / `FILE_FLAG_FIRST_PIPE_INSTANCE`) | Single-instance error, exit 1 |
| Stale socket from crashed instance | probe-then-unlink before bind (connect refused → unlink + rebind; connect succeeds → genuine second instance) | Reclaim, start normally |
| Root unreadable, or unwritable without `--read-only` | startup check | Clear startup error, exit 1 |
| Invalid config (unknown keys, bad format variables, port rules, missing https cert/key, http/https same port) | fail-fast validation (T017) | Startup error, exit 1 |
| `api` feature compiled, no transport enabled after resolution | config check | Startup error, exit 1 |
| Non-loopback bind | bind address check | Warning on stderr; escalated prominent warning with anonymous mode (layered trust) |

### B. Request-phase protocol/input errors (standard S3 error codes, FR-005)

| Condition | Error code | Notes |
|-----------|-----------|-------|
| Missing bucket / object | NoSuchBucket / NoSuchKey | Never a generic failure |
| Invalid bucket name | InvalidBucketName | s3s `check-bucket-name` authoritative (FR-012); backend re-validates on create |
| Create existing / delete non-empty bucket | BucketAlreadyExists / BucketNotEmpty | |
| Traversal (`..`), absolute path, control characters | Rejected outright | Before any FS access (FR-006, T013/T037) |
| `.tinio` segment at ANY depth | Write → AccessDenied; read → NoSuchKey; listings skip | FR-020, incl. nested-root protection |
| Missing/invalid signature | InvalidAccessKeyId / SignatureDoesNotMatch | s3s SigV4/SigV2 verification; no operation performed |
| Invalid part number | InvalidPart | 1..=10000 (data-model) |
| Disabled capability (runtime toggle / stripped feature) | NotImplemented | `[s3]` toggles + compile-time features (FR-021) |
| Unknown config keys | Startup error | Fail-fast (FR-016/021) |

### C. Storage-layer runtime conditions

| Condition | Behavior |
|-----------|----------|
| Path becomes inaccessible mid-operation (permissions, unlink, disk full) | Fail that request with a meaningful error; server keeps running |
| Cross-device rename | Not expected in normal mode: `tmp/` and target buckets are both under the root (`.tinio/` inside the root) → same volume. In read-only mode no data writes occur. If rename still fails (e.g. network FS), the error fails the request and the temp file is left for the sweep or the startup repair |
| Symlink in the tree | Followed by default (documented: may point outside the root); with `follow_symlinks` disabled → access rejected, entries excluded from listings |
| Case collisions on case-insensitive hosts | Host FS semantics; no artificial enforcement |
| Out-of-band file modification | ETag served only when meta size/mtime matches, else recomputed streaming and rewritten (FR-022); Last-Modified always from FS mtime |
| Same-size, same-mtime-tick edit | Stale ETag may be served — documented granularity limit (FR-022); a same-size edit with a changed file identity (new inode) is re-hashed (60 s mtime-window fallback where no identity exists, e.g. Windows) |
| Empty/read-only root in read-only mode | Supported: all state under `~/.tinio/roots/<sha1>/` (FR-023); root never written |

### D. Interruption and crash

| Condition | Leftover | Reclamation path |
|-----------|----------|------------------|
| Upload interrupted (client drop, crash) | Temp file in `tmp/` | Startup: full `tmp/` clear (T070); runtime: sweep after 24 h mtime (T046) |
| Multipart upload interrupted | Parts under `multipart/<bucket>/<uploadId>/` | **Kept** — cross-restart completion/abort is legal (quickstart §7); idle > 7 days swept (T046); bucket deleted → subtree removed at startup (T070) or by `doctor --fix` (T074) |
| Forced kill (`kill -9`) | Stale `state` + socket; orphaned meta entries; stale bucket records; upload directories without a `UPLOADS` record; unpublished delete tombstones | Startup repair (T070): probe-then-reclaim socket/state, prune stale bucket records, clear `tmp/` + tombstone residue, remove bucket-orphaned multipart + no-record upload dirs (idle past the grace); meta orphans reclaimed by the background scanner (T045) |
| Concurrent writes to one object | — | Last completed atomic rename wins (FR-011) |
| Crash during state/meta write | No torn state — redb commits are atomic (crash-safe by default) | Partial temp files swept as above |

### E. Shutdown phase

| Condition | Handling |
|-----------|----------|
| Graceful stop (`POST /stop`, SIGINT/SIGTERM, Ctrl+C, console-close) | Cease accepting, drain ≤ 10 s, remove `state` + socket, exit 0 (FR-018) |
| Drain timeout (10 s) | In-flight requests cut, process exits |
| Second signal during drain | Immediate exit without draining (standard UX) |
| `stop` CLI wait | Polls control channel until probe failure / `state` removal (bounded ~15 s); reports unconfirmed exit on timeout |
| SIGHUP | Ignored and logged (no config reload in v1) |

## 3. Reclamation-path division of labor

Cleanup is a backend contract: the `Cleanup` trait in `tinio-core` defines startup repair, orphan reclamation, and doctor diagnostics/fix, and each backend implements its own semantics. The matrix below is the **tinio-fs implementation** (`FsCleanup`) — future backends define their own (documented in their own backend docs).

| Item | Startup repair | Scanner | Sweep | `doctor --fix` |
|------|----------------|---------|-------|----------------|
| `tmp/` leftovers | Full clear (no active writers) | — | mtime > 24 h | Remove (shared impl) |
| Multipart uploads: idle (bucket exists) | Kept — cross-restart completion/abort legal (quickstart §7) | — | Remove after idle > 7 d | — |
| Bucket-orphaned multipart subtrees | Remove (cross-restart uploads kept) | — | idle > 7 d | Remove (shared impl) |
| Stale bucket records (`BUCKETS`, dir gone) | Prune | — | — | Remove (shared impl) |
| Upload dirs without a `UPLOADS` record | Remove (enumerate first, then read `UPLOADS` — the pinned TOCTOU order, fs-backend.md §8.1) | — | parts idle past the grace | Remove (shared impl) |
| Delete-bucket tombstones (`<root>/.tinio/deleting/`) | Remove | Clear per pass (count-only, `tombstone::clear_leftovers`) | — | Remove (shared impl) |
| Stale `state`/socket | Probe-then-reclaim | — | — | Remove (shared impl) |
| Orphaned meta entries (object gone) | — | Delete during scan | — | Remove (shared impl) |
| Stale home root-state dirs (root gone) | — | — | — | Remove (shared impl) |

Fs implementation details: [fs-backend.md §8](fs-backend.md). Startup repair, `doctor --fix`, and scanner reclamation share the `FsCleanup` implementation (the scanner's tombstone clear goes through `tombstone::clear_leftovers`) so behavior stays consistent by construction. Every repair action is logged to the operational log.

## 4. Ordering and constraints

- Startup repair runs **after** single-instance binding and **before** readiness (SC-005): only fast, deterministic items. Meta-orphan reclamation is deferred to the scanner (would require a full meta-tree walk at scale).
- Startup repair is skipped in read-only mode for anything that would write the root (state lives in the home dir there; the home state dir is repaired by the same rules).
- No automatic repair ever blocks readiness; the scanner and sweep may continue after readiness.
- Crash recovery verification: quickstart §7 (manual) and the lifecycle integration test (T055 crash-recovery case).
