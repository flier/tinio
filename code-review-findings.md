# Code Review Findings — uncommitted working-tree diff (branch `dev`)

Review scope: `git diff HEAD` — 57 files, ~9,500 insertions (redb metadata migration, scanner/etag/write background tasks, S3 request pipeline, listing pagination, multipart, strict path mapping, metrics).

Method: 10 finder angles (5 correctness + 5 cleanup/altitude/conventions) over 5 diff chunks, each candidate verified by an independent verifier (CONFIRMED / PLAUSIBLE / REFUTED), plus a final sweep. Line numbers refer to the current working-tree files.

> Status (2026-08-29, latest working tree): this document is a snapshot of the earlier tree — the current uncommitted diff is 72 files / ~12,032 insertions. All 49 findings were addressed in the current tree; the fixes are cited by F-number in code comments (e.g. `entry_matches` consults file identity — F01; scanner `load_bucket` snapshot + bucket-mutation lock — F02/F05; `set_batch_owned`; channel-derived queue depth; reserved data-plane `GET /metrics` — F10/F49; plain-`toml` write path with the root pin removed — F47). Line numbers in the findings refer to the snapshot tree, not the current one.

## Verified findings — 49 total (24 correctness, 25 cleanup)

### Correctness (ranked by severity)

#### F01 — Stale ETag served permanently after mtime-preserving replacement
- **File**: `crates/tinio-fs/src/scanner.rs` (line 483) · **Severity**: High · **Category**: correctness (data integrity)
- **Summary**: Persisted rows pair the hash-time ETag with walk-time size/mtime, and the gate never consults file identity, so a same-size mtime-preserving replacement after the hash is served with the stale ETag forever.
- **Failure scenario**: `cp -p` / `rsync -a` replaces object A with B (same size, mtime restored) after the etag task hashed A. The batch commits `(etag_of_A, S, M0)`; `entry_matches(S, M0)` gate-hits on every later walk, so HEAD/GET/List serve `etag_of_A` for B's bytes permanently — If-Match/If-None-Match and client caches misbehave until the file changes again.
- **Fix direction**: Include file identity in the gate (`entry_matches`) when identity is available, or carry task-time size/mtime in the batch so the stored row matches what was actually hashed.

#### F02 — reclaim_stale_buckets race destroys recreated bucket's fresh state
- **File**: `crates/tinio-fs/src/cleanup.rs` (line 461) · **Severity**: High · **Category**: correctness (data loss)
- **Summary**: `reclaim_stale_buckets` probes `try_exists` then wipes the bucket's whole derived state without holding the bucket-mutation lock, and runs on every scanner pass concurrently with live requests — an out-of-band delete + recreate in the window destroys the fresh bucket's state.
- **Failure scenario**: Bucket dir removed out-of-band; probe returns false; `create_bucket('data')` + `create_multipart_upload` + `upload_part` commit fresh UPLOADS/PARTS/OBJECT_META rows; `remove_bucket_state` then drains the reborn bucket's rows in one write txn → next UploadPart answers NoSuchUpload and part files become orphans swept later.
- **Fix direction**: Hold `bucket_mutation_lock` across probe → remove, or add a generation/version check (e.g. compare a BUCKETS-row created-at/epoch) before draining state.

#### F03 — D1 dir-sync fsyncs only the leaf parent, not the new ancestor chain
- **File**: `crates/tinio-fs/src/write.rs` (line 218) · **Severity**: High · **Category**: correctness (durability)
- **Summary**: `commit()`'s D1 dir-sync fsyncs only the leaf parent, never the newly created ancestor chain, so the durability promise ("no durable meta row without durable bytes") is inverted for the first PUT into a new prefix.
- **Failure scenario**: Empty bucket; first PUT `a/b/c.txt`: `create_dir_all` creates `a/` and `a/b`, rename lands, `sync_parent_dir(a/b)` fsyncs only `c.txt`'s entry — the `a/` and `a/b` directory entries are never fsynced. Power loss right after the 200 (OBJECT_META row committed with `Durability::Immediate`) can lose the whole chain and the object, leaving a durable meta row for a file that no longer exists.
- **Fix direction**: Track the directories `create_dir_all` actually created and fsync each newly created ancestor.

#### F04 — Stale composed ETag kept after in-place same-size rewrite
- **File**: `crates/tinio-fs/src/etag_task.rs` (line 130) · **Severity**: High · **Category**: correctness (data integrity)
- **Summary**: The composed-ETag keep decision trusts file identity alone (with a 60 s mtime-jitter fallback when identity is 0), so an in-place same-size rewrite of a multipart object keeps the stale MD5-of-MD5s ETag and re-stores it with the fresh mtime — served forever.
- **Failure scenario**: `mp.bin` has composed etag X-2, inode I, mtime M0; content overwritten in place with different same-size bytes: mtime → M1, inode stays I. `hash_into` sees `current == stored.file_identity` → keeps X-2 and writes `(X-2, S, M1)`; the next pass gate-hits, so the wrong ETag is served indefinitely, breaking If-Match/If-None-Match. On identity-less filesystems (identity == 0) any replacement within 60 s of the stored mtime is kept the same way.
- **Fix direction**: Require both identity AND a compatible mtime (or re-hash whenever mtime moved within the jitter window unless size also changed).

#### F05 — Orphan reclamation TOCTOU against a concurrent PUT
- **File**: `crates/tinio-fs/src/scanner.rs` (line 411) · **Severity**: High · **Category**: correctness (race)
- **Summary**: `try_exists`-probe-then-remove is a TOCTOU against a concurrent PUT: a false probe just before the PUT's rename lands lets `remove()` delete the meta row the PUT just committed.
- **Failure scenario**: Key K's file deleted out-of-band → candidate. Probe `try_exists` false at T1; concurrent PUT K renames at T2 and commits its row at T3; scanner `remove(K)` runs at T4 > T3 → the fresh row is deleted: object exists on disk with no row until the next pass, and the PUT's last-write-wins guarantee is silently lost if another mutation lands in the gap.
- **Fix direction**: Re-probe inside the same redb write transaction as the remove (or use a generation check on the row).

#### F06 — CompleteMultipartUpload 500 after the rename already landed
- **File**: `crates/tinio-fs/src/backend/multipart.rs` (line 129) · **Severity**: High · **Category**: correctness (consistency)
- **Summary**: Phase-2 `AtomicWriter::commit` can return Err after the rename already landed (parent-dir fsync failure — D1), leaving the object fully visible and served while the client receives a 500 and the upload records survive.
- **Failure scenario**: CompleteMultipartUpload on a bucket dir whose parent fsync fails after rename (e.g. mode 0o300 — write+execute, no read): phase 2 returns Err → 500, but the assembled object is on disk under the target and GET serves it (self-healed ETag recompute); UPLOADS/PARTS rows survive, so a client retry re-assembles and overwrites — the object is externally visible before the client ever got a success.
- **Fix direction**: Treat the rename as the commit point (return success once the rename landed and report post-rename fsync failure as a warning), or make the parent-dir fsync failure happen before the rename is visible.

#### F07 — `[pipeline]` config section parsed but never consumed
- **File**: `crates/tinio-config/src/schema/config.rs` (line 93) · **Severity**: High · **Category**: correctness (silently broken feature)
- **Summary**: The new `[pipeline]` section is parsed and garde-validated but nothing in the workspace consumes it — every `[pipeline.*]` value the operator sets is silently dropped.
- **Failure scenario**: Config with `[pipeline.io] workers = 64` and `[pipeline.db] priority = "high"` passes `Config::parse`; the only production `Pipelines::build` call site (`examples/serve.rs:76`) passes `pipeline::Config::default()` with a comment deferring to a "US2 CLI" that never reads the parsed section. The server runs 2/1 workers at Normal priority, capacity 1024, ignoring the config with no warning.
- **Fix direction**: Wire serve.rs (and the CLI) to pass `config.pipeline` into `Pipelines::build`; add a test asserting the consumed values.

#### F08 — enqueue racing shutdown returns Ok(done) for a task that never runs
- **File**: `crates/tinio-server/src/pipeline.rs` (line 298) · **Severity**: Medium · **Category**: correctness (contract violation, Q3)
- **Summary**: `enqueue` racing `shutdown` can return Ok(done) for a task that is then dropped without ever running, violating Q3 ("after shutdown, enqueue returns Err"): the retained `inner.sender` keeps the channel open after `shutdown_inner`'s `queue.take()`, and enqueue never re-checks shutdown after the send.
- **Failure scenario**: A list-batch enqueue passes the pre-send shut-down check; `shutdown()` runs concurrently and takes the queue sender while the retained clone keeps the channel open; the send succeeds → caller gets Ok(Completion), but the workers' biased select breaks on the watch signal, the task is dropped unrun, and `done.await` yields Err(Dropped) — a spurious mid-shutdown list failure instead of the contract's Err(ShutDown).
- **Fix direction**: Re-check the shutdown watch after the send; on shutdown, return Err(ShutDown) and drop the task.

#### F09 — catch_unwind covers only task.run(): panic in kind()/failure()/reply kills the worker permanently
- **File**: `crates/tinio-server/src/pipeline.rs` (line 468) · **Severity**: Medium · **Category**: correctness (availability)
- **Summary**: `catch_unwind` wraps only `task.run()`; a panic in `task.kind()` (before the fetch_adds) or in `output.failure()`/`reply.send` (after them) escapes `run_one` and kills the worker task permanently — no respawn, `drain()` swallows the JoinError, and in_flight/busy_workers stay stuck at their incremented value.
- **Failure scenario**: A task implementation whose `kind()` panics is enqueued on the DB pipeline (default workers=1): the sole worker dies, `tinio_pipeline_in_flight` stays 1 forever, and every subsequent DB enqueue fails or blocks (the retained sender keeps the channel open), hanging scanner/list DB batches until restart — the "worker stays alive (R6)" guarantee does not cover this path.
- **Fix direction**: Move `task.kind()`/`output.failure()`/`reply.send` inside the `catch_unwind` (or wrap the whole per-task step), and/or respawn the worker loop with backoff; decrement counters via a guard.

#### F10 — Five new metric families never refreshed or registered in production
- **File**: `crates/tinio-server/src/metrics.rs` (line 216) · **Severity**: Medium · **Category**: correctness (dead observability)
- **Summary**: `refresh_pipeline_gauges` and `refresh_write_lock_histograms` have no production call site (only `#[cfg(test)]` tests), and registration is lazy_static-lazy, so a running server's registry never contains `tinio_pipeline_queue_depth/in_flight/busy_workers` or `tinio_write_lock_wait/total_duration_seconds`.
- **Failure scenario**: Start serve.rs (which builds Pipelines but nothing calls `refresh_*`), scrape the registry: the three pipeline gauges and the two write-lock histogram families are absent from the output entirely (lazy_static never initialized) — pipeline-spec §4 observability ships as dead code.
- **Fix direction**: Call `refresh_*` from a scrape/management endpoint or a background interval task; register the families explicitly at server startup.

#### F11 — try_exists(...).unwrap_or(false) treats IO errors as "file does not exist"
- **File**: `crates/tinio-fs/src/scanner.rs` (line 411) · **Severity**: Medium · **Category**: correctness (data integrity)
- **Summary**: Any IO error (EACCES, EIO) from the orphan probe is treated as absent, so a live object whose path is temporarily unreadable has its meta row removed — repeated on every pass while the error persists.
- **Failure scenario**: A bucket directory's permissions are tightened (0o000) while the scanner runs: the probe on a candidate errors → `unwrap_or(false)` → `remove()` deletes the row of an existing object; every subsequent pass repeats the removal, so the object's ETag is recomputed per request (full re-hash per GET/HEAD) with no cache benefit.
- **Fix direction**: Match on `ErrorKind::NotFound` only; propagate other probe errors (skip the candidate, don't delete).

#### F12 — One bucket's reconcile error aborts scan_once, starving the rest of the pass
- **File**: `crates/tinio-fs/src/scanner.rs` (line 193) · **Severity**: Medium · **Category**: correctness (availability)
- **Summary**: One bucket's reconcile error (unreadable bucket dir, concurrent bucket-dir deletion) aborts `scan_once` via `?`, skipping all remaining buckets AND the `reclaim_stale_buckets` stage — a permanently failing bucket starves the rest of the pass forever.
- **Failure scenario**: Bucket "bad" has an unreadable directory: `reconcile_bucket` returns Err every pass and `scan_once` propagates it (run() only warns and retries the whole pass), so the loop never reaches the other buckets nor `reclaim_stale_buckets` — a bucket dir removed out-of-band keeps its BUCKETS/OBJECT_META/UPLOADS/PARTS rows (ghost bucket in ListBuckets/ListMultipartUploads) as long as "bad" keeps failing.
- **Fix direction**: Per-bucket error handling — warn and continue to the remaining buckets; only abort the pass on fatal errors; make `reclaim_stale_buckets` not depend on all buckets reconciling.

#### F13 — Dangling bucket symlink returns empty 200 instead of NoSuchBucket (follow=false)
- **File**: `crates/tinio-fs/src/listing.rs` (line 485) · **Severity**: Medium · **Category**: correctness (inconsistent S3 semantics)
- **Summary**: With `follow_symlinks=false` and a DANGLING bucket symlink, `list()` returns an empty, untruncated 200 instead of the NoSuchBucket error the follow=true path returns for the same bucket.
- **Failure scenario**: `follow_symlinks=false` + dangling bucket symlink: `symlink_metadata` succeeds (stats the link itself), the guard `!(is_symlink_or_reparse && !follow)` leaves the worklist unseeded → `list()` returns 200 with zero keys; with follow=true the same bucket canonicalizes to NotFound → NoSuchBucket. Same bucket answers differently depending on a server-wide flag, and the scanner's orphan reclaim then treats every snapshot row of that bucket as a candidate.
- **Fix direction**: Treat a dangling bucket symlink as NoSuchBucket regardless of `follow_symlinks` (canonicalize/probe the link target before the symlink guard). (The guard itself is also written as a double negation — an early return of the empty seed would be clearer.)

#### F14 — Composed-keep successes reset the failure streak, defeating the R4 abort
- **File**: `crates/tinio-fs/src/scanner.rs` (line 477) · **Severity**: Medium · **Category**: correctness (robustness)
- **Summary**: `fold_outcome` resets the consecutive-failure streak on every success, and a composed-ETag keep (P1, no re-hash) counts as a success, so alternating keeps and failures never reach `MAX_CONSECUTIVE_FAILURES` and the R4 abort never fires.
- **Failure scenario**: On an identity-less filesystem, a bucket where files fail the nofollow open inside the 60 s jitter window interleaves with composed-keep successes: each keep resets `*consecutive_failures` (scanner.rs:477) to 0, so the `>= MAX_CONSECUTIVE_FAILURES` abort (scanner.rs:471) is never reached and the failing files re-fail on every pass forever, defeating the R4 abort protection.
- **Fix direction**: Only reset the streak on a genuine re-hash success, or treat keeps as neutral (do not reset).

#### F15 — NotFound skips leave truncated marker over a dead range — empty-page amplification
- **File**: `crates/tinio-fs/src/listing.rs` (line 364) · **Severity**: Medium-Low · **Category**: correctness (pagination)
- **Summary**: A NotFound compute result skips the entry, but `truncated`/`next_start_after` were computed over the pre-resolution walked keys, so a page whose later entries all vanished returns far fewer than max_keys while advertising a resume marker.
- **Failure scenario**: Bucket with 1000 objects; a concurrent delete removes the last 999 after the walk but before their hashes run: each returns NotFound → `continue`; the response has 1 object but `truncated=true` with `next_start_after` = the last walked key. The client issues the next ListObjects, which re-walks the entire bucket and re-hashes page 2 (999 fresh enqueues) to get nothing — a pathological empty-page loop on every listing under concurrent deletion.
- **Fix direction**: Recompute `truncated`/`next_start_after` from the resolved entries, or return an empty untruncated page when all entries vanish.

#### F16 — Non-monotonic write-lock histogram exposition under load
- **File**: `crates/tinio-server/src/metrics.rs` (line 304) · **Severity**: Medium-Low · **Category**: correctness (observability)
- **Summary**: `WriteLockHistograms::collect` copies a snapshot of independent Relaxed atomics (tinio-fs `handle.rs` `write_lock_stats`), and `record()` increments buckets BEFORE count, so a scrape interleaved between a writer's bucket add and count add yields cumulative `le=0.1 > le="+Inf"` — a non-monotonic histogram exposition.
- **Failure scenario**: Under continuous write load, a scrape reads `wait_buckets` after the bucket fetch_adds but `count` before its fetch_add (e.g. 100 transactions, 99 bucketed, 1 racing): `le="0.1"` shows 100 while `le="+Inf"` shows 99 — `prometheus::histogram_quantile` over that scrape returns NaN/garbage and promtool flags the series invalid.
- **Fix direction**: Publish a consistent snapshot (seq-lock/epoch, or increment `count` first), or compute the buckets from a single atomic total.

#### F17 — List page enqueues everything before draining — head-of-line block
- **File**: `crates/tinio-fs/src/listing.rs` (line 352) · **Severity**: Medium-Low · **Category**: correctness (latency)
- **Summary**: All page compute tasks are enqueued (awaiting io-pipeline capacity) before any completion is drained, so a real concurrent runner serializes the page's enqueue phase behind the slowest in-flight hash — contradicting the no-HOL claim in the doc comment.
- **Failure scenario**: With a bounded concurrent io pipeline (default 2 workers, capacity 1024) and a page near max_keys: when the queue is full, the enqueue of task N blocks until a worker dequeues, and the drain that would observe already-finished hashes is not reached until all N enqueues complete. A single multi-GB object on the first worker stalls the enqueue of the remaining page entries (and thus the whole ListObjects response).
- **Fix direction**: Interleave drain with enqueue (e.g. `buffer_unordered`-style: poll completions while enqueueing).

#### F18 — list_uploads_page holds one redb read txn across the whole UPLOADS scan
- **File**: `crates/tinio-fs/src/multipart.rs` (line 813) · **Severity**: Medium-Low · **Category**: efficiency (storage growth)
- **Summary**: One redb read transaction is held open across the entire UPLOADS-table scan (the `read_blocking` closure spans the full `for_bucket` walk), pinning old pages against recycling while concurrent put_part/complete commits run.
- **Failure scenario**: Bucket with many in-progress uploads and sustained UploadPart traffic: ListMultipartUploads pins pages freed by concurrent writes for the scan's duration and `meta.redb` grows monotonically per listing — the exact held-open-window pattern the scanner was changed to eliminate (scanner.rs:231-236 documents the rule).
- **Fix direction**: Materialize a bounded snapshot per page (like the scanner's gating snapshot) instead of one long read txn.

#### F19 — Torn-file hash: truncate mid-hash persists a truncated-prefix ETag
- **File**: `crates/tinio-fs/src/etag_task.rs` (line 174) · **Severity**: Low · **Category**: correctness (transient)
- **Summary**: `md5_of_path` fetches `file.metadata()` AFTER the streaming hash completes, so a file truncated by a concurrent writer mid-hash yields an ETag of the truncated prefix, persisted with the walk-time size.
- **Failure scenario**: A concurrent truncate during the IO-pipeline hash of a large object: the worker hashes the truncated prefix and the batch records that ETag with the walk-time size; GET/HEAD serve the (now full) file with a content MD5 of only its prefix until the next scan's `entry_matches` detects the size change and recomputes.
- **Fix direction**: Stat before and after hashing; if size changed, discard the result and retry (or re-verify before persisting).

#### F20 — md5_of_file re-panics on JoinError on the request path
- **File**: `crates/tinio-fs/src/meta.rs` (line 79) · **Severity**: Low · **Category**: correctness (resilience)
- **Summary**: `md5_of_file` re-panics on a blocking-pool JoinError inside `ensure_etag`'s async request path, so a panic in the hash closure (or pool shutdown mid-hash) crashes the request task with an invisible 500 instead of `Error::Io`.
- **Failure scenario**: `ensure_etag`/`etag_for_file` runs on the GET/HEAD request path (backend/objects.rs:275); `spawn_blocking(...).await.unwrap_or_else(|join| panic!(...))` converts a transient blocking-pool shutdown or a hash-closure bug into a panic on the request task — the self-healing recompute path is bypassed and the real cause is invisible to the error pipeline.
- **Fix direction**: Return `Error::Io` for JoinError (or centralize one documented panic policy per F25) so a transient pool failure degrades gracefully.

#### F21 — Windows-reserved bucket names pass validation, fail with opaque errors
- **File**: `crates/tinio-fs/src/path.rs` (line 327) · **Severity**: Low (Windows-only) · **Category**: correctness (platform)
- **Summary**: `bucket_path`/`map_bucket_path` never apply the Windows charset/aliasing refusal that `map_key_path_lexical` (line 414) applies to keys, so Windows-reserved device names pass `bucket::name` validation and fail materialization with an opaque error.
- **Failure scenario**: On Windows, `bucket::name("con")` succeeds (3 lowercase letters pass `validate_bucket_name`), then `create_bucket("con")` reaches `map_bucket_path`: canonicalizing `root\con` resolves the console device outside the boundary (InvalidPath) or errors — an opaque 400/500 instead of the clean InvalidBucketName refusal the key-side `windows_aliasing` gives for a `con` key. Same for `nul`, `aux`, `prn`, `com1..com9`, `lpt1..lpt9`.
- **Fix direction**: Apply the reserved-name/charset refusal to bucket names too (map to a clean InvalidBucketName).

#### F22 — missing_bucket_boundary runs a sync stat on the async request thread
- **File**: `crates/tinio-fs/src/path.rs` (line 198) · **Severity**: Low · **Category**: correctness (latency, error path)
- **Summary**: `missing_bucket_boundary` calls the synchronous `bucket_dir.exists()` stat on the async request thread, violating the chunk's own invariant of no sync filesystem calls on request threads.
- **Failure scenario**: Every failed containment proof (an escaping key attempt, or a racing delete_bucket between ensure and proof) runs a blocking std::fs stat on the tokio request thread — on a slow/networked root this stalls the request thread on the error path.
- **Fix direction**: Use `tokio::fs::try_exists` (or `spawn_blocking`) here, mirroring the async conversion done for `boundary_for`/`prove_key_contained`.

#### F23 — InlineRunner discards task failures (fire-and-forget errors invisible)
- **File**: `crates/tinio-core/src/pipeline.rs` (line 405) · **Severity**: Low · **Category**: correctness (observability, offline contexts)
- **Summary**: `InlineRunner::enqueue` discards `run()`'s Err with `let _ = reply.send(...)` when the Completion is dropped, so fire-and-forget task failures are invisible under the reference runner — the R8 logging the module doc promises exists only in the tinio-server runtime.
- **Failure scenario**: A scanner pass enqueues an EtagComputeTask that fails (e.g. ELOOP-mapped PermissionDenied or a read error); the scanner drops the Completion and the InlineRunner's send returns Err into `let _`, losing the error — the same failure is silent under InlineRunner (the default in all offline contexts: tests, benches, doctor).
- **Fix direction**: Log the discarded Err in InlineRunner (at least `tracing::warn`), matching R8.

#### F24 — ELOOP → PermissionDenied normalization swallows intermediate-chain loops
- **File**: `crates/tinio-fs/src/fsutil.rs` (line 85) · **Severity**: Low (PLAUSIBLE) · **Category**: correctness (error semantics)
- **Summary**: The unix ELOOP → PermissionDenied normalization also swallows ELOOP from deep intermediate symlink chains (ENOTRECURSIVE-class path loops), not just a leaf O_NOFOLLOW rejection, conflating two distinct failure modes into one error.
- **Failure scenario**: With follow_symlinks=false, a path component chain exceeding the kernel symlink resolution limit (an out-of-band symlink loop between the policy walk and the open) returns `PermissionDenied: symlink` — identical to a leaf-link TOCTOU rejection — so an operator cannot distinguish a looped path from a denied link, and the original ELOOP is dropped from the error chain. Would confirm if a distinct response/diagnostic is documented for intermediate-loop ELOOP.
- **Fix direction**: Preserve the original error kind/context for chain loops vs. leaf links (e.g. keep ELOOP distinct, or attach the original error as source).

### Cleanup / reuse / efficiency / altitude / conventions

#### F25 — JoinError policy contradiction: same panic class, two opposite treatments
- **File**: `crates/tinio-fs/src/scanner.rs` (line 288) · **Severity**: Medium · **Category**: correctness-adjacent (error semantics)
- **Summary**: The snapshot load converts a panicking `spawn_blocking` closure to `Error::Io(io::Error::other(join))`, while `meta.rs:79` and `database/handle.rs` re-panic for the same class (documented at meta.rs:75-78: "re-panics the caller... converting it to io::Error would mask it as a self-healable recompute").
- **Failure scenario**: A panicking `load_bucket` closure becomes a self-healable IO error that the run layer warns on and retries next pass, while a panicking hash closure on the same blocking pool re-panics — error semantics depend on which module the closure lives in.
- **Fix direction**: Pick one policy (re-panic per meta.rs) and apply it everywhere, or extract a shared join-error mapper.

#### F26 — write_temp adds a second fsync to every multipart put_part
- **File**: `crates/tinio-fs/src/write.rs` (line 270) · **Severity**: Low · **Category**: efficiency
- **Summary**: The new D1 `file.sync_all()` in `AtomicWriter::write_temp` adds a second fsync to every multipart put_part — the DB commit fsync (`Durability::Immediate`, handle.rs counts "10 000 flushes by design") already exists per part.
- **Failure scenario**: A 10,000-part upload pays 2 fsyncs per part (redb Immediate commit + part-file sync), then 2 more at complete (assemble `out.sync_all()` + `sync_parent_dir` after rename) — doubling the documented per-part flush cost on the hot path.
- **Fix direction**: Document the tradeoff, or drop the per-part file sync (part temp files are re-uploadable; assemble-time sync covers durability at complete).

#### F27 — Composed-ETag keep decision implemented twice and already drifted
- **File**: `crates/tinio-fs/src/meta.rs` (line 282; also `etag_task.rs:126`) · **Severity**: Low · **Category**: reuse
- **Summary**: `Store::ensure_etag` and `EtagComputeTask::hash_into` both implement the composed-keep rule (Composed + size match → identity-or-mtime-jitter same-file check), but the copies already diverge: ensure_etag probes identity path-based (`file_identity`), the task handle-based (`file_identity_handle`) — meta.rs:53's doc claims "one home for the rule", but only the const is shared.
- **Fix direction**: Extract `meta::composed_keep(stored, size, mtime, identity) -> bool` and call it from both.

#### F28 — busy_workers gauge is redundant with in_flight by construction
- **File**: `crates/tinio-server/src/pipeline.rs` (line 469) · **Severity**: Low · **Category**: efficiency
- **Summary**: `run_one` increments/decrements `busy_workers` in strict lockstep with `in_flight` (nothing else touches either), so the gauge always equals `in_flight` — plus 4 atomics and the `AlignedCounter`/false-sharing machinery per task for a metric carrying zero information.
- **Fix direction**: Drop `busy_workers`; report both Stats fields from the single remaining counter.

#### F29 — Hand-rolled panic_message reimplements std::panic::panic_message
- **File**: `crates/tinio-server/src/pipeline.rs` (line 575) · **Severity**: Low · **Category**: reuse
- **Summary**: The 8-line downcast-to-&str/else-fallback helper reimplements `std::panic::panic_message` (stable since Rust 1.81; toolchain is 1.98).
- **Fix direction**: `panic = %std::panic::panic_message(&payload)`.

#### F30 — wait_for test helper duplicated verbatim
- **File**: `crates/tinio-server/src/pipeline.rs` (line 804) · **Severity**: Low · **Category**: reuse
- **Summary**: `wait_for` is a verbatim copy of `tinio_fs::testutil::wait_for` (same 10 s deadline, 2 ms poll, same assert message).
- **Fix direction**: Hoist `wait_for` into `tinio_util::testing` (already a dev-dependency of tinio-server).

#### F31 — Production worker core duplicates the testutil multi-worker queue pattern
- **File**: `crates/tinio-server/src/pipeline.rs` (line 451) · **Severity**: Low · **Category**: reuse/altitude
- **Summary**: The production worker loop re-implements `tinio_fs::testutil`'s `PacedRunner`/`GatedRunner` pattern (identical `Job<O>` alias at pipeline.rs:50 vs testutil.rs:148, and `receive` vs `recv_job` body-identical) — the subtle shutdown/backpressure semantics now live in two copies whose divergence silently weakens the test harness.
- **Fix direction**: Extract the generic worker-loop core into tinio-core (next to `Runner`/`Stats`) or tinio-util.

#### F32 — SharedBuf owned-Write sink exists in three copies
- **File**: `crates/tinio-server/src/pipeline.rs` (line 765) · **Severity**: Low · **Category**: reuse
- **Summary**: The test `SharedBuf` sink is the third copy in the workspace — log.rs defines it twice (lines 423 and 484) with identical write/flush impls.
- **Fix direction**: One definition in log.rs (or `tinio_util::testing`), reused by pipeline.rs tests.

#### F33 — fs_options() constructor copy-pasted in 6 files after removing FsOptions::Default
- **File**: `crates/tinio-fs/src/backend/mod.rs` (line 87) · **Severity**: Low · **Category**: reuse
- **Summary**: Removing `SmartDefault`/`Default` from `FsOptions` (mandatory pipeline fields) forces every offline construction site to spell out all 6 fields; the same ~14-line `fs_options()` helper is now copy-pasted in testutil.rs:46, benches/streaming.rs:19, tests/layout.rs:26, tinio-server/benches/listing.rs:21, benches/multipart_assembly.rs:23, tests/common/mod.rs:38.
- **Fix direction**: Keep a documented offline `Default` (InlineRunner) or export one `tinio_fs::testing::fs_options` gated like `tinio_util::testing`.

#### F34 — EtagResult re-exported at crate root from a private module
- **File**: `crates/tinio-fs/src/lib.rs` (line 36) · **Severity**: Low · **Category**: conventions (style.md)
- **Summary**: `pub use self::etag_task::EtagResult;` re-exports a brand-new public type from a module that stays private (`mod etag_task;`), making the crate-root re-export the only way to name the type — style.md prescribes "Expose via module path, not crate-root re-exports".
- **Fix direction**: Make `etag_task` public (or expose the type from a public concern module).

#### F35 — list_uploads_page re-implements the tinio-core pagination engine as a custom heap
- **File**: `crates/tinio-fs/src/multipart.rs` (line 795) · **Severity**: Low · **Category**: altitude
- **Summary**: `list_uploads_page` re-implements `group_and_paginate_ordered`'s rollup-dedup / exclusive-after marker / max+1 truncation semantics as a custom max-heap (`UploadsPageEntry`, `heap_insert`, `heap_prefixes`, hand-written Ord) — the S3 ListMultipartUploads page contract now has two homes, pinned only by the 80-combo equivalence test.
- **Fix direction**: An iterator-based variant of the engine in tinio-core serving both paths.

#### F36 — Dead identity slot in listing compute results
- **File**: `crates/tinio-fs/src/listing.rs` (line 337) · **Severity**: Low · **Category**: dead code
- **Summary**: The `results: Vec<Option<(ETag, u64)>>` identity is never read: gate hits write a hardcoded 0, assembly destructures `Some((etag, _))` and ignores the identity.
- **Fix direction**: `Vec<Option<ETag>>`, identity only in the BatchEntry.

#### F37 — WalkState::next_file repeats the fatal two-liner eight times
- **File**: `crates/tinio-fs/src/listing.rs` (line 558) · **Severity**: Low · **Category**: simplification
- **Summary**: `self.done = true; return Some(Err(...))` is repeated 8 times (lines 558, 573, 604, 650, 658, 666, 687, 693) inside up to 4 levels of nesting.
- **Fix direction**: A `fn fatal(&mut self, err) -> Option<Result<T, Error>>` helper (or small macro).

#### F38 — Compute-result fold duplicated with scanner's fold_outcome
- **File**: `crates/tinio-fs/src/listing.rs` (line 361) · **Severity**: Low · **Category**: reuse
- **Summary**: The NotFound-skip / BatchEntry-construct fold is duplicated with scanner.rs's `fold_outcome` (scanner.rs:453-495), including the identical `BatchEntry { key, etag, size, mtime, identity }` construction.
- **Fix direction**: A shared `MetaBatchAccumulator::push_result`-style method that both callers invoke with their own failure policy.

#### F39 — Test fixture duplicated between listing.rs and scanner.rs
- **File**: `crates/tinio-fs/src/listing.rs` (line 1280) · **Severity**: Low · **Category**: reuse (tests)
- **Summary**: `files_fixture`/`file_etag` duplicate scanner.rs's `files` fixture (same `f{00..n}.txt` tree with `payload {i}` content), and listing's `files_under` re-implements the walk rules — the fixtures have already diverged (listing's creates the bucket dir + state store, scanner's only the dir).
- **Fix direction**: One shared fixture in testutil.rs.

#### F40 — Scanner producer loop duplicated with the list producer
- **File**: `crates/tinio-fs/src/scanner.rs` (line 314) · **Severity**: Low · **Category**: altitude
- **Summary**: `reconcile_bucket` and `FsListing::list` each hand-roll gating → FuturesUnordered enqueue → drain → accumulator push → flush, with different failure policies already (listing fails the page on first non-NotFound error; scanner warns and tolerates 99).
- **Fix direction**: Factor the shared drain/fold (the accumulator is already shared); policies become a parameter.

#### F41 — Dead key slot in compute_outcome
- **File**: `crates/tinio-fs/src/scanner.rs` (line 456) · **Severity**: Low · **Category**: dead code
- **Summary**: The outer `key` paired through every completion is dead on the Ok path — shadowed by the key from the result; only the Err-warn arm (line 467) uses it.
- **Fix direction**: Pair `(size, mtime, result)` and take the key from the result.

#### F42 — Walk-loop pacing is an inline copy of pace_write_batches
- **File**: `crates/tinio-fs/src/scanner.rs` (line 354) · **Severity**: Low · **Category**: reuse
- **Summary**: The `yield_now + sleep` pacing in the walk loop duplicates `Self::pace_write_batches` (scanner.rs:436-439), which exists for exactly this.
- **Fix direction**: Replace scanner.rs:355-356 with `Self::pace_write_batches(self.options.delay).await`.

#### F43 — Three hand-rolled streaming MD5 read loops
- **File**: `crates/tinio-fs/src/etag_task.rs` (line 193) · **Severity**: Low · **Category**: reuse
- **Summary**: `md5_of_handle` (etag_task.rs:187-197), `md5_of_file` (write.rs:100-113, still allocating a fresh 64 KiB vec per call) and the part-assembly hasher loop (multipart.rs:611-619) are three near-identical read-into-buf/hasher.update loops.
- **Fix direction**: One `md5_stream(reader, buf)` helper (fsutil or a hash module) sharing the pooled-buffer win.

#### F44 — key_path duplicates prove_key_contained's body
- **File**: `crates/tinio-fs/src/path.rs` (line 383) · **Severity**: Low · **Category**: reuse
- **Summary**: The public sync `key_path` hand-inlines `prove_key_contained`'s logic (boundary match, prove_contained, `missing_bucket_boundary` wrap), so the racing-delete policy ("NoSuchKey when the bucket directory is gone") lives in two copies that must be updated in lockstep.
- **Fix direction**: A sync `prove_key_contained_sync(bucket_dir, key)` used by both.

#### F45 — Io and Db config structs are near-identical duplicates
- **File**: `crates/tinio-config/src/schema/pipeline.rs` (line 73) · **Severity**: Low · **Category**: simplification
- **Summary**: The `workers`/`priority`/`capacity` field triple is duplicated verbatim (48 lines of identical serde/SmartDefault/garde attributes), differing only in the worker bound.
- **Fix direction**: A generic `Queue<const DEFAULT_WORKERS, const MIN, const MAX>` struct with type aliases.

#### F46 — serde default fns re-derive values via Io::default()/Db::default()
- **File**: `crates/tinio-config/src/schema/pipeline.rs` (line 123) · **Severity**: Low · **Category**: simplification
- **Summary**: `io_workers()`/`db_workers()`/`default_capacity()` go through a SmartDefault round-trip instead of returning the module's imported constants (`DEFAULT_IO_WORKERS`, etc.), so the serde defaults can silently diverge from the constants they exist to mirror.
- **Fix direction**: `fn io_workers() -> u8 { DEFAULT_IO_WORKERS }` — use the constants directly.

#### F47 — toml_edit write-path swap is unmotivated
- **File**: `crates/tinio-config/src/schema/config.rs` (line 148) · **Severity**: Low · **Category**: altitude
- **Summary**: `to_toml` swaps `toml::to_string` for a second TOML serializer (`toml_edit::ser::to_document`) without naming any defect in the old write path — the presence-gated attributes the comment cites worked with plain `toml` too; a new dependency for the same format with no stated behavior change.
- **Fix direction**: Revert to `toml::to_string`, or document the actual reason (formatting/ordering) in the comment.

#### F48 — Four near-identical warn blocks in run_one
- **File**: `crates/tinio-server/src/pipeline.rs` (line 480) · **Severity**: Low · **Category**: simplification
- **Summary**: Four `tracing::warn!` blocks (failure+escalation, failure, panic+escalation, panic) differ only in the `failures` field and message suffix — and the messages have already drifted ("the pipeline keeps consuming" vs "the worker stays alive").
- **Fix direction**: One warn per outcome arm with `Option<u32>` failures / `Option<&str>` panic fields.

#### F49 — Scrape path is two ad-hoc refresh entry points that must be called in the right order
- **File**: `crates/tinio-server/src/metrics.rs` (line 234) · **Severity**: Low · **Category**: altitude
- **Summary**: `refresh_pipeline_gauges` and `refresh_write_lock_histograms` are separate entry points (the write-lock one doubling as the registration path), and callers must remember both — a caller that forgets one silently serves stale gauges.
- **Fix direction**: One `refresh(io: Stats, db: Stats, write_lock: WriteLockSnapshot)` scrape hook behind which the special cases sit.

---

## REFUTED candidates (verified and rejected)

1. **listing.rs ~370 — compute result dropped when binary search misses**: REFUTED — miss is unreachable by construction: task keys are cloned from the page itself (listing.rs:343), the task returns `self.key` unchanged, `walk_files` sorts, and `group_and_paginate` pushes only original walked items (rollups go to common_prefixes). The `Err(_) => continue` is dead defensive code.
2. **listing.rs ~305 — gating-load length checked only by debug_assert**: REFUTED — `gated.len()` can never differ from `page.len()`: `load_entries` maps one slot per input key and any row error fails the whole read via `?`.
3. **listing.rs ~224 — FsListing::new has 8 positional args**: REFUTED — it has 7, with only 3 call sites total.
4. **scanner.rs ~339 — pending FuturesUnordered grows to O(bucket) memory**: REFUTED — enqueue is bounded backpressure ("waits while the queue is full"), so outstanding futures are capped at queue capacity + workers.
5. **tinio-server pipeline.rs ~473 — blocking hash stalls the server runtime**: REFUTED — each Pipeline builds its OWN runtime on which only worker_loop runs (blocking-task model by design, Q4); request tasks and the scanner run on the server's runtime and are never blocked by a hash.
6. **tinio-core pipeline.rs ~93 — Error::shut_down()/dropped() miss style.md's #[inline]**: REFUTED — both constructors already carry `#[inline]`.
7. **tinio-server pipeline.rs ~309 — idle workers parked in receive() hang at shutdown**: REFUTED (sweep candidate) — the worker loop is one biased `select!` with the watch branch and `receive` as sibling arms (the lock is acquired inside the receive future), so a parked worker's `changed()` waker fires on shutdown; `shutdown_inner`'s sender drop also resolves parked `recv()` to None → break. `drain()` cannot hang.
8. **write.rs ~209 — EXDEV fallback failure leaves both temp files behind**: REFUTED (sweep candidate) — `fallback = true` is set BEFORE the copy, so the source temp is removed in every EXDEV-failure case, and `copy_across_volumes` removes the staging file best-effort on failure.
9. **scanner.rs ~408 — symlink-to-in-bucket target keeps a phantom row forever**: REFUTED (sweep candidate) — the behavior is deliberate and pinned by test `symlinked_objects_are_not_reclaimed` (scanner.rs:685-723): the probe seeing the link itself and keeping the row is by design.

Also withdrawn by the sweep finder itself (no concrete divergence found): the listing accumulator-flush ordering item, and the multipart heap marker/rollup truncation item.

## Notes

- 49 verified findings total: 24 correctness (1 PLAUSIBLE — F24), 25 cleanup/reuse/efficiency/altitude/conventions.
- A handful of finder-phase candidates were not individually verified and are excluded from this list (e.g. listing shutdown mid-page task-drop, load_bucket sync-signature enforcement, docs-style nits, bench fixture duplication, DirId::of dead-code gating). Treat the 49 above as the verified set.
- Line numbers refer to the current working tree at review time (2026-08-29, branch `dev`).

---

## Fix status (2026-08-29, working tree on `dev` — NOT committed)

All 49 verified findings were processed. 45 fixed, 4 documented as deliberately kept / deferred (F29, F31, F35, F40). Full workspace: 602 tests pass, `cargo check --workspace --all-targets` clean.

- **F01** — fixed: `meta::entry_matches` consults the file identity (both sides nonzero); the walk yields identity (unix free from the stat; Windows one extra open per file); GET/HEAD derive it from the open handle (path-based on Windows — winapi-util is std-File-only and the crate forbids unsafe); scanner/list gates + `ensure_etag`/`etag_matching`/`etag_for_file` take identity.
- **F02** — fixed: `reclaim_stale_buckets` probes + wipes under the bucket-mutation lock (create/put hold the same lock).
- **F03** — fixed: `AtomicWriter::commit` takes `sync_root`; the first commit into a new prefix syncs every created ancestor up to the bucket root.
- **F04** — fixed (per decision): `meta::composed_keep` — identity platforms require identity match AND zero mtime drift (any drift re-hashes); identity-less platforms keep the 60 s jitter fallback (documented risk; Windows ~16 ms FILETIME granularity documented as a bounded limitation).
- **F05** — fixed: the scanner's orphan probe + remove run under the bucket-mutation lock.
- **F06** — fixed: the rename is the commit point; post-rename fsync failures warn instead of failing the write (test updated to pin the new semantics).
- **F07** — fixed (per decision): `serve --config <file>` consumes `config.pipeline` into `Pipelines::build`.
- **F08** — fixed: `enqueue` re-checks the shutdown flag after the send → `Err(ShutDown)`.
- **F09** — fixed: `run_one` catches the whole per-task step (`kind()` separately, `run()`/`failure()`/`send` in one catch); a counter guard decrements on every exit path.
- **F10** — fixed (per decision): a `/metrics` endpoint on the data-plane listener (GET only, before the S3 service) refreshes through a `MetricsRefresh` hook and serves the registry text.
- **F11** — fixed: `fsutil::is_absent` (NotFound = gone, any other error propagates); applied in the scanner and all four cleanup probe sites.
- **F12** — fixed: per-bucket reconcile errors warn and continue; the stale-bucket stage always runs (R4 tests updated).
- **F13** — fixed: a dangling bucket symlink with follow=false canonicalizes to NotFound → NoSuchBucket.
- **F14** — fixed: composed-ETag keeps are neutral to the failure streak (only genuine re-hashes reset).
- **F15** — fixed: a page whose entries ALL vanish answers empty + untruncated (no resume marker over a dead range).
- **F16** — fixed: `WriteHistogram::record` increments `count` first — monotonic exposition.
- **F17** — fixed: the list producer drains resolved completions while still enqueueing (no head-of-line block).
- **F18** — fixed: `list_uploads_page` materializes the bucket's rows in one SHORT read transaction; the heap pagination runs after the txn.
- **F19** — fixed: `md5_of_path` verifies the size before/after the hash (retry once); every outcome carries hash-time size/mtime/identity (`EtagOutcome`), so a row never pairs a hash-time ETag with walk-time metadata.
- **F20** — resolved via F25: the documented policy is re-panic on JoinError (a panic is a bug, not a self-healable IO error); the scanner's snapshot load now follows it.
- **F21** — fixed: `map_bucket_path`/`bucket_path` refuse Windows reserved names/aliases with a clean `InvalidBucketName`.
- **F22** — fixed: `missing_bucket_boundary_async` probes through `tokio::fs`.
- **F23** — fixed: `InlineRunner` logs a warn when a task result is dropped before delivery.
- **F24** — fixed: the unix ELOOP → PermissionDenied normalization preserves the original error as the source.
- **F25** — fixed: one policy — re-panic on JoinError everywhere (scanner aligned with meta/handle).
- **F26** — documented: the per-part fsync is a deliberate tradeoff (parts are re-uploadable; assemble-time sync covers the completed object).
- **F27** — fixed: `meta::composed_keep` is the single home of the keep rule (task + ensure_etag).
- **F28** — fixed: `busy_workers` counter removed; both `Stats` fields come from `in_flight`.
- **F29** — **kept**: `std::panic::panic_message` does NOT exist on the 1.98 toolchain (the finding's "stable since 1.81" premise is wrong); the helper stays with a note.
- **F30** — fixed: `wait_for` hoisted to `tinio_util::testing`.
- **F31** — **deferred**: extracting the generic worker-loop core risks the subtle shutdown/backpressure semantics (the reviewer's own divergence concern); the accumulator/producer sharing (F38/F17) already removed most duplication.
- **F32** — fixed: `SharedBuf` single definition in `tinio_util::testing` (three copies removed).
- **F33** — fixed: `tinio_fs::testing::fs_options` is the single home (six copies removed).
- **F34** — fixed: `etag_task` is a public module; the crate-root re-export is gone.
- **F35** — **kept**: the max-heap IS the bounded-memory implementation of the engine over an upload-id-keyed table (item 7e deliberately removed the full-bucket materialization an engine variant would need); the equivalence test pins the semantics; documented in `list_uploads_page`.
- **F36** — fixed: dead identity slot removed (`Vec<Option<ETag>>`).
- **F37** — fixed: `WalkState::fatal` helper.
- **F38** — fixed: `MetaBatchAccumulator::push_outcome` shared by both producers.
- **F39** — fixed: shared `testutil::files` fixture.
- **F40** — **deferred**: the two producers' enqueue/drain scaffolding differs deliberately in failure policy (list fails the page; scanner tolerates 99); the shared accumulator (F38) and the interleaved drain (F17) already converged them.
- **F41** — fixed: `compute_outcome` pairs only the key (the outcome carries the rest).
- **F42** — fixed: walk-loop pacing reuses `pace_write_batches`.
- **F43** — fixed: `fsutil::md5_stream` / `md5_stream_async` shared by the etag task and the write path (the multipart assembly's copy+hash loop is fused by design).
- **F44** — fixed: the racing-delete policy lives in `missing_bucket_boundary_{sync,async}` (one home, two probe forms).
- **F45** — fixed: `Queue<const DEFAULT_WORKERS, const MIN, const MAX>` with `Io`/`Db` type aliases.
- **F46** — fixed: serde defaults return the constants directly.
- **F47** — fixed: `to_toml` back on plain `toml`; `toml_edit` dependency removed.
- **F48** — fixed: one warn per outcome arm with optional `failures`/`panic` fields; the "likely systemic" signal kept on escalation.
- **F49** — fixed: one `metrics::refresh(io, db, write_lock)` entry point behind the `/metrics` endpoint.

Smoke-tested end-to-end: `serve --config` starts with the configured pipelines; `GET /metrics` serves the pipeline gauges + write-lock histograms; S3 paths are not hijacked.
