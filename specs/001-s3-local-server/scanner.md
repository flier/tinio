# Design: Background ETag Scanner (FR-024)

**Branch**: `001-s3-local-server` | **Date**: 2026-08-22

Implementation design for the background ETag scanner (FR-024), as implemented by the tinio-fs backend in `crates/tinio-fs/src/scanner.rs` (task T045). Consolidates FR-024, research.md §22, the `[scanner]` config contract, and the meta-store semantics from data-model.md. The walk is fs-specific — future backends decide whether and how they scan; fs walk details are in [fs-backend.md §9](fs-backend.md). Related: [failure-handling.md](failure-handling.md) (reclamation division of labor), [data-model.md](data-model.md) (meta store).

## 1. Purpose

Eliminate the cold-listing cliff: the first listing over an externally-populated tree pays a synchronous full-content MD5 pass for missing/stale meta entries (FR-022). The scanner converts cold files into meta-store hits in the background, so repeated listings become cheap. Listings remain correct with the scanner disabled (synchronous recompute fallback).

## 2. Meta-store interaction

The scanner walks the object tree and reconciles it against the meta store (entry schema in data-model.md):

| Entry state | Action |
|-------------|--------|
| Object exists, meta missing | Compute MD5 streaming (bounded buffers), write meta entry atomically |
| Object exists, meta size/mtime mismatch | Recompute streaming, rewrite entry (out-of-band modification, FR-022) |
| Object gone, meta exists | **Orphan reclamation**: delete the meta entry (invalid-file recovery; FR-024) |
| Entry matches | No-op (cheap skip: stat + entry read) |

All meta writes are redb transactions on `OBJECT_META` (single-writer, crash-safe by default — no torn state). Reclamation and recomputation share the same walk; there is no separate pass.

## 3. Scheduling and pacing

Config (presence-gated `[scanner]` section — present = on, absent = off; Minio-aligned keys, contracts/config.md):

| Key | Default | Meaning |
|-----|---------|---------|
| `delay` | 10.0 s | Pacing between scan iterations (throttle) |
| `max_wait` | 15 s | Max time to wait for a scan slot when throttled |
| `cycle` | 24 h | Full-tree re-scan cadence (catches out-of-band changes over time) |

Independent override: `TINIO_SCANNER` env (`0`/`1`).

Loop shape:

1. Wait for a scan slot (bounded by `max_wait`; if no slot, back off and retry).
2. Walk the tree, processing entries per §2, yielding to request traffic (see §4).
3. Sleep `delay`, then repeat until `cycle` elapsed → full-tree re-scan restarts.
4. On shutdown signal, abort quietly (no partial-write cleanup needed — writes are atomic).

## 4. Yielding and priority

- The scanner is the lowest-priority background task: it must not measurably delay request handling.
- Yield strategy: after a bounded batch of entries (e.g. a configurable-in-impl constant, tuned by the T093 cold/warm benchmark), the scanner yields to the runtime (e.g. `tokio::task::yield_now()` or a small sleep), so in-flight S3 requests preempt scanning.
- Never blocks startup: the scanner launches after readiness (SC-005); startup only performs the fast deterministic repairs (failure-handling.md §3).
- Never blocks shutdown: aborts quietly on the shared shutdown channel.

## 5. Lifecycle and modes

- **Default on**: the auto-created config includes the `[scanner]` section. Section omitted → scanner off; listings fall back to synchronous recompute (correct, possibly slow on cold trees).
- **Read-only mode**: runs too — meta writes land in the home state dir (`~/.tinio/roots/<sha1>/`), never in the root (FR-023).
- **Concurrency correctness**: object deletions/additions during a scan are safe by design — an entry whose object vanishes between stat and read is treated as an orphan and deleted; a new object appears in the next cycle (or the current one if reached later in the walk). Meta writes are atomic; a concurrent recompute and delete of the same entry resolve to either outcome, both consistent.

## 6. Boundaries with other modules

| Module | Boundary |
|--------|----------|
| `listing.rs` (T043) | Synchronous recompute fallback for missing/stale entries during a listing — the scanner only makes this rare; never a correctness dependency |
| `sweep.rs` (T046) | Sweep owns time-based cleanup of `tmp/` (24 h) and idle multipart (7 d). Scanner owns meta reconciliation only |
| `Cleanup` trait (T012) / `FsCleanup` (fs-backend.md §8) | Startup repair owns fast deterministic items (tmp clear, bucket-orphaned multipart, no-record upload dirs, stale bucket records). Meta-orphan reclamation belongs to the scanner (cost: full meta-tree walk). Both go through the `Cleanup` trait (fs details: fs-backend.md §8) |
| `doctor` (T073/T074) | Offline checks share the same reclamation semantics via cleanup.rs; the scanner is the runtime counterpart |

## 7. Testing

- Cold-vs-warm listing benchmark (T093): generated externally-populated tree (thousands of objects) — first listing (synchronous recompute) vs warm listing (scanner-completed meta store); documents the FR-022 cold cost and the scanner benefit.
- Interop cold-listing with and without scanner (T033).
- Unit tests: pacing/yield behavior, orphan reclamation, size/mtime mismatch recompute, atomic writes under concurrency (proptest T028 covers meta-store validation).
