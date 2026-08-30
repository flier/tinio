//! The background ETag scanner (task T045, FR-024; design per scanner.md).
//!
//! A low-priority task that converts cold files into meta-store hits in the
//! background: missing entries are computed (streaming MD5, bounded
//! buffers), stale entries recomputed, and orphaned entries (object gone)
//! reclaimed through the [`Cleanup`] trait — so repeated listings become
//! cheap. Listings stay correct with the scanner disabled (synchronous
//! recompute fallback). The bucket walk streams — files come out of the
//! directory walk one at a time, gated against a **materialized**
//! snapshot of the bucket's meta (one short read transaction at bucket
//! start, released before the walk — pipeline-spec.md §3.7
//! whole-bucket-in-memory, R1).
//!
//! Pacing per contracts/config.md (Minio-aligned): `delay` between entry
//! batches (throttle), `max_wait` bounds a single sleep so shutdown is
//! always prompt, `cycle` is the minimum interval between full-tree passes.
//! `TINIO_SCANNER` (`0`/`1`) overrides the `[scanner]` presence gate at
//! construction. The scanner never blocks startup (it launches after
//! readiness) and aborts quietly on the shutdown channel.

use std::{
    collections::{HashMap, HashSet},
    env,
    io::ErrorKind,
    time::{Duration, Instant},
};

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use tinio_core::{bucket, cleanup::CleanupOptions, object, pipeline::Completion};
use tokio::{
    sync::watch,
    task::{spawn_blocking, yield_now},
    time::sleep,
};

use crate::{
    FsCleanup, backend::FsStorage, database, error::Error, etag, fsutil,
    listing::MetaBatchAccumulator, meta, pacing, tombstone,
};

/// Entries per batch: after each batch of **enqueued compute tasks** the
/// scanner yields and sleeps `delay`, so in-flight S3 requests preempt
/// scanning (pipeline-spec.md R2 — `BATCH_SIZE` is the enqueued-task
/// count, tuned by the T093 cold/warm benchmark).
const BATCH_SIZE: usize = 32;

/// The same-bucket consecutive task-failure threshold at which the
/// scanner aborts the bucket reconcile and propagates (pipeline-spec.md
/// R4). `NotFound` failures are excluded — a vanished file is a normal
/// skip, not a systematic failure.
const MAX_CONSECUTIVE_FAILURES: u32 = 100;

/// Scanner construction options (contracts/config.md `[scanner]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannerOptions {
    /// Presence gate: `[scanner]` section present = on, absent = off.
    pub enabled: bool,
    /// Seconds between scan batches (pacing/throttle).
    pub delay: Duration,
    /// Max time to wait for a scan slot when throttled (bounds a single
    /// sleep so shutdown stays prompt).
    pub max_wait: Duration,
    /// Full-tree re-scan cadence (catches out-of-band changes over time).
    pub cycle: Duration,
}

/// The background ETag scanner of a storage backend.
///
/// # Examples
///
/// ```rust
/// use std::{sync::Arc, time::Duration};
///
/// use tinio_core::{
///     pipeline::InlineRunner,
///     storage::{
///         BucketOps, DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES,
///         DEFAULT_META_BATCH_SIZE, ObjectOps,
///     },
/// };
/// use tinio_fs::{FsOptions, FsStorage, Scanner, ScannerOptions};
/// use tinio_util::testing::body;
/// use tokio::runtime::Runtime;
///
/// let root = tempfile::tempdir().unwrap();
/// let options = FsOptions {
///     follow_symlinks: false,
///     state_dir: None,
///     compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///     meta_batch_size: DEFAULT_META_BATCH_SIZE,
///     meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
///     io_pipeline: Arc::new(InlineRunner::default()),
///     remove_pipeline: Arc::new(InlineRunner::default()),
///     db_pipeline: Arc::new(InlineRunner::default()),
/// };
/// let storage = FsStorage::new(root.path(), options).unwrap();
/// // A hand-dropped file with no meta entry.
/// fs::create_dir(root.path().join("data")).unwrap();
/// fs::write(root.path().join("data/dropped.txt"), b"out-of-band").unwrap();
/// let options = ScannerOptions {
///     enabled: true,
///     delay: Duration::from_millis(1),
///     max_wait: Duration::from_millis(10),
///     cycle: Duration::from_millis(50),
/// };
/// let scanner = Scanner::new(storage.clone(), options);
/// Runtime::new().unwrap().block_on(async {
///     scanner.scan_once().await.unwrap();
///     let head = storage
///         .head_object(&"data".into(), &"dropped.txt".into())
///         .await
///         .unwrap();
///     assert_eq!(head.size, 11);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Scanner {
    storage: FsStorage,
    options: ScannerOptions,
}

/// Resolve the enabled gate: `TINIO_SCANNER` is a strict `0`/`1` toggle
/// that overrides the `[scanner]` section presence independently
/// (contracts/config.md, FR-024); any other value (or a non-Unicode one)
/// is ignored, falling back to the section gate.
fn scanner_enabled(env: Option<&str>, section_present: bool) -> bool {
    match env {
        Some("1") => true,
        Some("0") => false,
        _ => section_present,
    }
}

impl Scanner {
    /// Construct the scanner. The `TINIO_SCANNER` env toggle (`0`/`1`)
    /// overrides the `[scanner]` presence gate independently (FR-024); any
    /// other value is ignored.
    pub fn new(storage: FsStorage, options: ScannerOptions) -> Self {
        let enabled = scanner_enabled(env::var("TINIO_SCANNER").ok().as_deref(), options.enabled);
        Self {
            storage,
            options: ScannerOptions { enabled, ..options },
        }
    }

    /// Run the scan loop until `shutdown` turns true (aborts quietly — no
    /// partial-write cleanup needed; writes are atomic). Does nothing when
    /// disabled.
    pub async fn run(self, shutdown: watch::Receiver<bool>) {
        if !self.options.enabled {
            return;
        }
        loop {
            let pass_start = Instant::now();
            let walked = self.scan_once().await;
            match walked {
                Ok(summary) => {
                    tracing::debug!(
                        reconciled = summary.reconciled,
                        recomputed = summary.recomputed,
                        reclaimed = summary.reclaimed,
                        "scanner pass complete"
                    );
                }
                Err(err) => tracing::warn!(error = %err, "scanner pass failed"),
            }
            if *shutdown.borrow() {
                return;
            }
            // One full pass per cycle at most; passes longer than the
            // cycle (large trees) restart immediately.
            let elapsed = pass_start.elapsed();
            let budget = self.options.cycle.max(self.options.delay);
            if elapsed < budget {
                pacing::sleep_checked(
                    budget - elapsed,
                    self.options.max_wait.max(Duration::from_millis(100)),
                    &shutdown,
                )
                .await;
                if *shutdown.borrow() {
                    return;
                }
            }
        }
    }

    /// One full-tree pass: reconcile every bucket's files against the meta
    /// store (missing → compute, stale → recompute), reclaiming each
    /// bucket's meta orphans from the pass's own data (item 2 — entries
    /// of the gating snapshot the walk never emitted, probed against the
    /// filesystem), then prune stale bucket records. Yields to request
    /// traffic between buckets (and after each batch of entries within a
    /// bucket); returns the pass summary.
    ///
    /// Per-bucket isolation (F12): one bucket's reconcile error (an
    /// unreadable bucket dir, a concurrent bucket-dir deletion, an R4
    /// abort) is warned and skipped — the remaining buckets and the
    /// stale-bucket stage still run, so a permanently failing bucket
    /// cannot starve the rest of the pass (or leave ghost bucket records)
    /// forever. Only a root-level failure (the `bucket_names` walk)
    /// aborts the pass.
    pub async fn scan_once(&self) -> Result<ScanSummary, Error> {
        let mut summary = ScanSummary::default();
        for name in self.storage.bucket_names().await? {
            match self.reconcile_bucket(&name).await {
                Ok((reconciled, recomputed, reclaimed)) => {
                    summary.reconciled += reconciled;
                    summary.recomputed += recomputed;
                    summary.reclaimed += reclaimed;
                }
                Err(err) => tracing::warn!(
                    bucket = %name,
                    error = %err,
                    "bucket reconcile failed; skipping the bucket (the next pass retries)"
                ),
            }
            yield_now().await;
        }
        // Stale bucket records (item 2): a bucket directory removed
        // out-of-band is invisible to the reconcile loop (it walks live
        // directories), so its BUCKETS/OBJECT_META/UPLOADS/PARTS rows are
        // pruned here — one `BUCKETS` read plus an existence probe per
        // row. The meta-orphan half of the old `reclaim_meta_orphans`
        // full pass is derived per bucket above — no second full scan
        // per round.
        let cleanup = FsCleanup::new(&self.storage, CleanupOptions::default());
        match cleanup.reclaim_stale_buckets().await {
            Ok(reclaimed) => summary.reclaimed += reclaimed,
            Err(err) => tracing::warn!(error = %err, "stale-bucket reclamation failed"),
        }
        // Unpublished delete-bucket trees (a crash after the unpublish
        // rename, or a failed fire-and-forget `remove_dir_all`) live
        // under `<root>/.tinio/deleting/` — not a live bucket name, so
        // the reconcile loop never sees them. They are enqueued on the
        // removal lane (D-A), never removed inline — a huge tree must
        // not block the scan cycle, and the lane is the storage's own
        // (an inline runner offline, so the enqueue still clears).
        let leftover_runner = self.storage.remove_pipeline();
        match tombstone::leftovers(self.storage.root()).await {
            Ok(leftovers) => {
                for (path, _) in leftovers {
                    if tombstone::enqueue_one(path, &leftover_runner).await {
                        summary.reclaimed += 1;
                    }
                }
            }
            Err(err) => tracing::warn!(error = %err, "delete-tombstone reclamation failed"),
        }
        Ok(summary)
    }

    /// Reconcile one bucket: for every object file, ensure a matching meta
    /// entry exists (compute or recompute the MD5 streaming through the
    /// pipelines, pipeline-spec.md §3.2), and reclaim the bucket's meta
    /// orphans from the pass's own data (item 2). Returns
    /// `(files, recomputed, reclaimed)`.
    ///
    /// The walk streams: files come out of the directory walk one at a
    /// time, in walk order (no full-bucket `Vec`, no sort — the scanner
    /// needs no order, §3.7 constant memory). The walk's size + mtime
    /// double as the staleness check — no second stat per file. Gating
    /// runs against a **materialized snapshot** (data-path review
    /// 2026-08-29, finding 1): [`Store::load_bucket`] loads the
    /// whole bucket's meta into memory BEFORE the walk — one short-lived
    /// read transaction on the blocking pool (P3), released as soon as
    /// the load returns — and each file is gated against the in-memory
    /// map (identical snapshot semantics to the old held-open window: a
    /// row committed mid-walk is invisible until the next pass; either
    /// side self-heals through the write batches). The walk also records
    /// every walked key — the pass's disk truth. The snapshot's memory
    /// is O(bucket) — the same class as the `walked` set, accepted by
    /// §3.7 — and no read transaction is ever pinned during the walk:
    /// the DB pipeline's commits recycle pages freely, so a long cold
    /// scan cannot stall meta.redb's file growth. The orphan candidates
    /// (the snapshot minus the walked keys) are derived from the map in
    /// memory after the walk — no second transaction, no blocking-pool
    /// hop.
    ///
    /// Matching entries are left untouched in memory (P6 — no worker for
    /// a cache hit); missing/stale entries are enqueued into the IO
    /// pipeline, and their results stream back in COMPLETION order — each
    /// arrival folds straight into the write-pipeline batch accumulator
    /// (Q5), so the DB pipeline works while the walk is still enqueuing
    /// and results never accumulate past the in-flight window (§3.7
    /// constant memory). Batch completions are **dropped** (Q3b
    /// fire-and-forget — batch failures surface through the runtime's
    /// `Outcome` warn, R8).
    ///
    /// A single compute-task failure is skipped with a warn and the scan
    /// continues (Q10); [`MAX_CONSECUTIVE_FAILURES`] consecutive failures
    /// (NotFound excluded) abort this bucket and propagate (R4) — as soon
    /// as the threshold ARRIVES, not after the whole bucket is hashed.
    /// Yields and sleeps `delay` after each batch of [`BATCH_SIZE`]
    /// enqueued tasks (R2), and after every enqueued write batch (the
    /// same discipline extended to the DB pipeline — item 1), so
    /// in-flight S3 requests preempt scanning. A pass in progress at
    /// shutdown completes, then the loop exits.
    async fn reconcile_bucket(&self, name: &bucket::Name) -> Result<(usize, usize, usize), Error> {
        let meta = self.storage.meta_store();
        let listing = self.storage.listing();
        // The bucket-existence check happens here; the stream then emits
        // files one at a time (no full-bucket materialization).
        let mut walk = listing.walk_files_streaming(name, "").await?;
        // R1/§3.7: the gating snapshot is MATERIALIZED up front — one
        // short-lived read transaction on the blocking pool (P3),
        // released as soon as the load returns. The walk gates each file
        // against the in-memory map (identical snapshot semantics to the
        // old held-open window — a row committed mid-walk is invisible
        // until the next pass; either side self-heals through the write
        // batches), and the DB pipeline's commits never collide with a
        // pinned read transaction (data-path review 2026-08-29, finding
        // 1: a long cold scan no longer stalls page recycling in
        // meta.redb). O(bucket) memory — the same class as the `walked`
        // set below, accepted by §3.7. A JoinError (a panicking load
        // closure) re-panics the caller — the one documented policy
        // (F25): a panic is a bug, not a recoverable IO error; converting
        // it to `io::Error` would mask it as a self-healable recompute
        // (consistent with `database::Handle` and `meta::md5_of_file`).
        let snapshot: HashMap<object::Key, Option<database::StoredMeta>> = {
            let meta = meta.clone();
            let name = name.clone();
            spawn_blocking(move || {
                Ok::<_, Error>(
                    meta.load_bucket(&name)?
                        .into_iter()
                        .map(|row| (row.key, row.stored))
                        .collect(),
                )
            })
            .await
            .unwrap_or_else(|join| panic!("the snapshot-load task panicked: {join}"))?
        };
        let mut file_count = 0usize;
        let mut recomputed = 0usize;
        let mut reclaimed = 0usize;
        let mut enqueued = 0usize;
        let mut consecutive_failures: u32 = 0;
        // The walked keys of this round — the pass's disk truth (item 2:
        // entries of the gating snapshot never walked are the orphan
        // candidates; O(bucket) memory for the duration of the bucket).
        let mut walked: HashSet<object::Key> = HashSet::new();
        let mut accumulator = MetaBatchAccumulator::new(
            name,
            meta.clone(),
            listing.db_pipeline(),
            listing.meta_batch_size(),
            listing.meta_batch_bytes(),
        );
        // The in-flight compute completions — drained as they resolve,
        // never collected for the whole bucket.
        let mut pending = FuturesUnordered::new();

        // P6: the in-memory matches gate — a matching entry never
        // enqueues (no worker for a cache hit). Missing/stale entries go
        // through the IO pipeline (concurrency = its workers, Q4). The
        // gate answers against the materialized snapshot (R1) and
        // consults the walked file identity (F01) — a same-size
        // mtime-preserving replacement is a gate miss, never a stale
        // serve.
        while let Some(file) = walk.next().await {
            let file = file?;
            file_count += 1;
            // The identity is lazy (WalkedFile): the scanner gates EVERY
            // file, so it pays the Windows open once per file — the same
            // cost as the eager walk.
            let identity = file.identity();
            let stored = snapshot.get(&file.key).and_then(Option::as_ref);
            if let Some(stored) = stored
                && meta::entry_matches(
                    stored.size,
                    stored.mtime,
                    stored.file_identity,
                    file.size,
                    file.mtime,
                    identity,
                )
            {
                walked.insert(file.key);
                continue;
            }
            let task = etag::ComputeTask {
                key: file.key.clone(),
                path: file.path,
                size: file.size,
                stored: stored.cloned(),
                follow_symlinks: *self.storage.follow_symlinks(),
            };
            walked.insert(file.key.clone());
            let done = listing.io_pipeline().enqueue(Box::new(task)).await?;
            pending.push(compute_outcome(done, file.key));
            enqueued += 1;
            // Drain whatever has already resolved (never blocking — the
            // walk keeps enqueuing): arrivals fold into the batch
            // accumulator while the hash phase is still running (§3.2).
            while let Some(Some(outcome)) = pending.next().now_or_never() {
                if Self::fold_outcome(
                    name,
                    &mut accumulator,
                    outcome,
                    &mut recomputed,
                    &mut consecutive_failures,
                )
                .await?
                {
                    Self::pace_write_batches(self.options.delay).await;
                }
            }
            // R2: after each batch of enqueued tasks, yield and sleep
            // `delay` so in-flight S3 requests preempt scanning.
            if enqueued.is_multiple_of(BATCH_SIZE) {
                Self::pace_write_batches(self.options.delay).await;
            }
        }
        // The walk is done. The orphan candidates come from the pass's
        // OWN snapshot (item 2): the candidates are the snapshot entries
        // the walk never emitted — derived from the in-memory map (the
        // materialized snapshot minus the walked set), no second
        // transaction and no blocking-pool hop. The snapshot drops here,
        // BEFORE the drain, so the pending completions resolve and the
        // accumulator flushes outside any read window (P3).
        let candidates: Vec<object::Key> = snapshot
            .into_iter()
            .filter(|(key, _)| !walked.contains(key))
            .map(|(key, _)| key)
            .collect();
        // The walk is done — drain the in-flight rest.
        while let Some(outcome) = pending.next().await {
            if Self::fold_outcome(
                name,
                &mut accumulator,
                outcome,
                &mut recomputed,
                &mut consecutive_failures,
            )
            .await?
            {
                Self::pace_write_batches(self.options.delay).await;
            }
        }
        if let Some(done) = accumulator.flush().await? {
            drop(done);
            Self::pace_write_batches(self.options.delay).await;
        }
        // Orphan reclamation (item 2): the candidates are probed against
        // the filesystem — the exact "object file no longer exists" test
        // of the old full-bucket reclaim, now per-candidate (zero in
        // steady state). A row committed mid-pass by a concurrent PUT
        // (its key was never walked) exists on disk and is skipped; a
        // vanished file is reclaimed.
        if !candidates.is_empty() {
            // A bucket directory that cannot resolve (a symlinked bucket
            // with following disabled — the walk yielded nothing, so
            // every row is a candidate) skips the reclamation entirely,
            // like the old reclaim (the containment proof cannot address
            // such buckets).
            if let Ok(bucket_dir) = self.storage.bucket_dir(name).await {
                // The probes + removes of the whole candidate pass run
                // under this bucket's mutation lock (F05): a concurrent
                // PUT's rename and meta-row commit hold the same
                // per-bucket lock, so a fresh row can never be removed
                // by a stale probe — the PUT either completes before the
                // probe (the object exists — skipped) or after the
                // remove (its row re-lands). Mutations of other buckets
                // do not wait; the pass is bounded by the candidate list.
                let _guard = self.storage.lock_bucket_mutations(name).await;
                for key in candidates {
                    // The object path through the crate's own mapping
                    // (one source of truth — the same key_path the
                    // old reclaim used; an unaddressable key, e.g. a
                    // link inside the bucket with following disabled,
                    // is skipped like the old reclaim skipped it).
                    let Ok(path) = self.storage.key_path(&bucket_dir, &key, true).await else {
                        continue;
                    };
                    match fsutil::is_absent(&path).await {
                        Ok(true) => {}         // gone — reclaim below
                        Ok(false) => continue, // the object exists — not an orphan
                        Err(err) => {
                            // F11: an IO error must never be treated as
                            // "gone" — a live object whose path is
                            // temporarily unreadable keeps its row (it is
                            // re-probed on the next pass).
                            tracing::warn!(
                                bucket = %name,
                                key = %key,
                                error = %err,
                                "orphan probe failed; the entry is kept"
                            );
                            continue;
                        }
                    }
                    if let Err(err) = self.storage.meta_store().remove(name, &key).await {
                        tracing::warn!(
                            bucket = %name,
                            key = %key,
                            error = %err,
                            "orphaned meta entry not reclaimed"
                        );
                        continue;
                    }
                    reclaimed += 1;
                }
            }
        }
        Ok((file_count, recomputed, reclaimed))
    }

    /// R2 extended to the write batches (item 1, data-path review
    /// 2026-08-27): after every enqueued write batch the scanner yields
    /// and sleeps `delay`, exactly like the compute batches — a cold
    /// reconcile cannot keep the DB pipeline's queue full and amplify
    /// list tail latency by the queue depth (the queue drains while the
    /// scanner sleeps).
    async fn pace_write_batches(delay: Duration) {
        yield_now().await;
        sleep(delay).await;
    }

    /// Fold one resolved compute outcome into the reconcile state: a
    /// vanished file (NotFound between the walk and the hash — concurrent
    /// delete) is a normal skip, never a failure (R4 excludes it), and is
    /// neutral to the streak — it neither increments nor resets
    /// `consecutive_failures` (the spec's literal "excluding NotFound",
    /// protecting the abort threshold against concurrent churn); any
    /// other failure warns and counts toward the
    /// [`MAX_CONSECUTIVE_FAILURES`] abort threshold (Q10/R4); a genuine
    /// re-hash success resets the counter; a composed-ETag keep (P1 — no
    /// hash ran) is **neutral**, like NotFound — counting it a success
    /// would let alternating keeps and failures defeat the R4 abort
    /// forever (F14). Every success feeds the streaming batch
    /// accumulator (Q5 — the batch completion is dropped, Q3b
    /// fire-and-forget). Returns whether a write batch was enqueued (the
    /// caller's write-batch pacing, item 1).
    async fn fold_outcome(
        name: &bucket::Name,
        accumulator: &mut MetaBatchAccumulator<'_>,
        (key, result): (object::Key, etag::Result),
        recomputed: &mut usize,
        consecutive_failures: &mut u32,
    ) -> Result<bool, Error> {
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(Error::Io(err)) if err.kind() == ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                *consecutive_failures += 1;
                tracing::warn!(
                    bucket = %name,
                    key = %key,
                    error = %err,
                    "scanner etag task failed; skipping the entry (Q10)"
                );
                if *consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    return Err(err); // R4: abort this bucket
                }
                return Ok(false);
            }
        };
        if !outcome.kept {
            *consecutive_failures = 0;
        }
        *recomputed += 1;
        if let Some(done) = accumulator.push_outcome(outcome).await? {
            // Q3b: fire-and-forget — batch failures surface via the
            // runtime's `Outcome` warn (R8), never here.
            drop(done);
            return Ok(true);
        }
        Ok(false)
    }
}

/// Await one compute completion, pairing the [`etag::Result`] with the key
/// the failure warn needs (F41 — the outcome carries its own key; the
/// paired key is only the Err-arm fallback). A dropped completion (task
/// panic, R6) is a failure like any other — the original pipeline error
/// is kept (P7).
async fn compute_outcome(
    done: Completion<etag::Result>,
    key: object::Key,
) -> (object::Key, etag::Result) {
    let result = match done.await {
        Ok(result) => result,
        Err(err) => Err(Error::from(err)),
    };
    (key, result)
}

/// Result of one scanner pass (for logs and tests).
///
/// # Examples
///
/// ```rust
/// use tinio_fs::ScanSummary;
///
/// let summary = ScanSummary {
///     reconciled: 3,
///     recomputed: 1,
///     reclaimed: 0,
/// };
/// assert_eq!(summary.reconciled, 3);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanSummary {
    /// Object files reconciled (entries checked).
    pub reconciled: usize,
    /// Entries computed or recomputed (missing/stale).
    pub recomputed: usize,
    /// Orphaned meta entries reclaimed.
    pub reclaimed: usize,
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{
        fs::File,
        path::Path,
        sync::{Arc, atomic::Ordering},
    };

    use tinio_core::{
        ETag, object,
        pipeline::{InlineRunner, Runner},
        storage::{BucketOps, ObjectOps},
    };
    use tinio_util::testing::body;
    use tokio::{fs, time::sleep};

    use super::*;
    use crate::{
        FsOptions, testutil,
        testutil::{
            FailingBatchRunner, FailingTaskRunner, GatedRunner, PacedRunner, fs_options, wait_for,
        },
        tombstone,
    };

    fn options() -> ScannerOptions {
        ScannerOptions {
            enabled: true,
            delay: Duration::from_millis(1),
            max_wait: Duration::from_millis(10),
            cycle: Duration::from_millis(50),
        }
    }

    #[test]
    fn scanner_env_toggle_is_strict_zero_one() {
        // `1` forces on, `0` forces off; anything else is ignored and the
        // config-section gate decides (contracts/config.md).
        assert!(scanner_enabled(Some("1"), false));
        assert!(!scanner_enabled(Some("0"), true));
        for value in ["false", "true", "yes", "2", ""] {
            assert!(scanner_enabled(Some(value), true), "{value}");
            assert!(!scanner_enabled(Some(value), false), "{value}");
        }
        assert!(scanner_enabled(None, true));
        assert!(!scanner_enabled(None, false));
    }

    #[tokio::test]
    async fn computes_missing_entries() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        fs::write(root.path().join("data/dropped.txt"), b"out-of-band")
            .await
            .unwrap();
        let scanner = Scanner::new(storage.clone(), options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.reconciled, 1);
        assert_eq!(summary.recomputed, 1);
        let head = storage
            .head_object(
                &bucket::name("data").unwrap(),
                &object::key("dropped.txt").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.size, 11);
        // A second pass is a no-op (entries match).
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.recomputed, 0);
    }

    #[tokio::test]
    async fn recomputes_stale_entries() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        storage
            .put_object(&b, &"a.txt".into(), body(b"first"))
            .await
            .unwrap();
        // Out-of-band edit: new content, new size.
        fs::write(root.path().join("data/a.txt"), b"second")
            .await
            .unwrap();
        let scanner = Scanner::new(storage.clone(), options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.recomputed, 1);
        let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
        assert_eq!(head.size, 6);
        assert_eq!(head.etag, ETag::from_content(b"second"));
    }

    #[tokio::test]
    async fn recomputes_after_mtime_preserving_replacement() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let file = root.path().join("data/a.txt");
        storage
            .put_object(&b, &"a.txt".into(), body(b"first!"))
            .await
            .unwrap();
        assert_eq!(
            storage.head_object(&b, &"a.txt".into()).await.unwrap().etag,
            ETag::from_content(b"first!")
        );
        // Replace with a NEW file, same size, mtime restored.
        let metadata = fs::metadata(&file).await.unwrap();
        let replacement = root.path().join("data/replacement.txt");
        fs::write(&replacement, b"second").await.unwrap();
        let handle = File::options().write(true).open(&replacement).unwrap();
        handle.set_modified(metadata.modified().unwrap()).unwrap();
        drop(handle);
        fs::rename(&replacement, &file).await.unwrap();
        let scanner = Scanner::new(storage.clone(), options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.recomputed, 1, "the replacement must be recomputed");
        let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
        assert_eq!(head.etag, ETag::from_content(b"second"));
    }

    #[tokio::test]
    async fn reclaims_orphaned_meta_entries() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let gone = object::key("gone.txt").unwrap();
        let alive = object::key("alive.txt").unwrap();
        storage.put_object(&b, &gone, body(b"x")).await.unwrap();
        storage.put_object(&b, &alive, body(b"y")).await.unwrap();
        fs::remove_file(root.path().join("data/gone.txt"))
            .await
            .unwrap();
        let scanner = Scanner::new(storage.clone(), options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.reclaimed, 1);
        assert_eq!(storage.meta_store().walk(&b).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reclaims_delete_tombstones() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let leftover = tombstone::dir(root.path()).join("dead-bucket");
        fs::create_dir_all(&leftover).await.unwrap();
        fs::write(leftover.join("leftover.bin"), b"was-a-bucket")
            .await
            .unwrap();
        let scanner = Scanner::new(storage, options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.reclaimed, 1);
        assert!(!leftover.exists());
    }

    #[tokio::test]
    async fn write_batch_enqueues_pace_like_compute_batches() {
        let root = tempfile::tempdir().unwrap();
        files(root.path(), 3);
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                meta_batch_size: 1,
                io_pipeline: Arc::new(InlineRunner::default()),
                db_pipeline: Arc::new(InlineRunner::default()),
                ..fs_options()
            },
        )
        .unwrap();
        let scanner = Scanner::new(
            storage,
            ScannerOptions {
                enabled: true,
                delay: Duration::from_millis(200),
                max_wait: Duration::from_millis(10),
                cycle: Duration::from_millis(50),
            },
        );
        let started = Instant::now();
        let summary = scanner.scan_once().await.unwrap();
        let elapsed = started.elapsed();
        assert_eq!(summary.recomputed, 3);
        assert!(
            elapsed >= Duration::from_millis(500),
            "three write batches must pace (3 × 200 ms sleeps): {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_objects_are_not_reclaimed() {
        let root = tempfile::tempdir().unwrap();
        files(root.path(), 2); // f00.txt, f01.txt
        fs::write(root.path().join("data/target.txt"), b"t")
            .await
            .unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let scanner = Scanner::new(storage.clone(), options());
        scanner.scan_once().await.unwrap(); // all entries computed
        // Swap the objects for links: f00 → dangling, f01 → the
        // in-bucket target.
        for (name, target) in [
            ("f00.txt", root.path().join("gone")),
            ("f01.txt", root.path().join("data/target.txt")),
        ] {
            let path = root.path().join("data").join(name);
            fs::remove_file(&path).await.unwrap();
            symlink(target, &path).unwrap();
        }
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.reclaimed, 0, "the links exist — not orphans");
        assert_eq!(
            storage
                .meta_store()
                .walk(&bucket::name("data").unwrap())
                .await
                .unwrap()
                .len(),
            3,
            "all rows must survive (f00, f01, target)"
        );
    }

    #[tokio::test]
    async fn run_stops_on_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        fs::write(root.path().join("data/a.txt"), b"x")
            .await
            .unwrap();
        let scanner = Scanner::new(storage.clone(), options());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            scanner.run(rx).await;
        });
        // Give the loop a moment to make progress, then stop it.
        sleep(Duration::from_millis(200)).await;
        tx.send(true).unwrap();
        task.await.unwrap();
        // The pass completed at least once.
        assert!(
            !storage
                .meta_store()
                .walk(&bucket::name("data").unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn run_does_nothing_when_disabled() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        fs::write(root.path().join("data/a.txt"), b"x")
            .await
            .unwrap();
        let mut options = options();
        options.enabled = false;
        let scanner = Scanner::new(storage.clone(), options);
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            scanner.run(rx).await;
        });
        sleep(Duration::from_millis(50)).await;
        // Disabled scanners return immediately, dropping the receiver.
        let _ = tx.send(true);
        task.await.unwrap();
        assert!(
            storage
                .meta_store()
                .walk(&bucket::name("data").unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn yields_between_entry_batches() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        for i in 0..40 {
            fs::write(
                root.path().join(format!("data/f{i:02}.txt")),
                format!("payload {i}"),
            )
            .await
            .unwrap();
        }
        let scanner = Scanner::new(storage.clone(), options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.reconciled, 40);
        assert_eq!(summary.recomputed, 40);
        assert_eq!(
            storage
                .meta_store()
                .walk(&bucket::name("data").unwrap())
                .await
                .unwrap()
                .len(),
            40
        );
    }

    #[tokio::test]
    async fn run_restarts_immediately_when_pass_exceeds_cycle() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        for i in 0..40 {
            fs::write(
                root.path().join(format!("data/f{i:02}.txt")),
                format!("payload {i}"),
            )
            .await
            .unwrap();
        }
        let scanner = Scanner::new(
            storage.clone(),
            ScannerOptions {
                enabled: true,
                delay: Duration::from_millis(1),
                max_wait: Duration::from_millis(10),
                cycle: Duration::from_millis(1),
            },
        );
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            scanner.run(rx).await;
        });
        sleep(Duration::from_millis(200)).await;
        tx.send(true).unwrap();
        task.await.unwrap();
        // At least one full pass completed before shutdown.
        assert_eq!(
            storage
                .meta_store()
                .walk(&bucket::name("data").unwrap())
                .await
                .unwrap()
                .len(),
            40
        );
    }

    // --- the pipeline producers (pipeline-spec.md task 4) ---

    /// A bucket with `n` files `f00.txt..`, each with distinct content —
    /// the shared producer fixture (F39; the listing tests' store-owning
    /// `files_fixture` builds on it).
    fn files(root: &Path, n: usize) {
        testutil::files(root, n);
    }

    /// Swap every `data/f*.txt` file for a symlink to a missing target:
    /// the compute task's nofollow open then rejects each with
    /// `PermissionDenied` (R3).
    #[cfg(unix)]
    fn swap_all_for_symlinks(root: &Path, n: usize) {
        for i in 0..n {
            let path = root.join("data").join(format!("f{i:02}.txt"));
            remove_file(&path).unwrap();
            symlink(root.join("gone"), &path).unwrap();
        }
    }

    /// A scanner over a storage with the given pipelines.
    fn scanner_with(
        root: &Path,
        io: Arc<dyn Runner<etag::Result>>,
        db: Arc<dyn Runner<Result<(), Error>>>,
    ) -> Scanner {
        let storage = FsStorage::new(
            root,
            FsOptions {
                io_pipeline: io,
                db_pipeline: db,
                ..fs_options()
            },
        )
        .unwrap();
        Scanner::new(storage, options())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn single_compute_failure_is_skipped_and_the_scan_continues() {
        let root = tempfile::tempdir().unwrap();
        files(root.path(), 3);
        let io = GatedRunner::<etag::Result>::new(1, 8);
        let scanner = scanner_with(root.path(), io.clone(), Arc::new(InlineRunner::default()));
        let scanner2 = scanner.clone();
        let scan = tokio::spawn(async move { scanner2.scan_once().await });
        wait_for(|| io.enqueued() == 3).await;
        // The walk is done; break only the first file in the
        // walk-to-hash window.
        let path = root.path().join("data/f00.txt");
        fs::remove_file(&path).await.unwrap();
        symlink(root.path().join("gone"), &path).unwrap();
        io.open_gate();
        let summary = scan.await.unwrap().unwrap();
        assert_eq!(summary.reconciled, 3);
        assert_eq!(summary.recomputed, 2, "the healthy entries were recomputed");
        // The broken entry is skipped, not persisted.
        assert_eq!(
            scanner
                .storage
                .meta_store()
                .walk(&bucket::name("data").unwrap())
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn consecutive_compute_failures_abort_the_bucket() {
        // R4: the whole bucket failing systematically (≥ 100 consecutive
        // non-NotFound task failures) aborts the reconcile. F12: the
        // abort stays at the bucket boundary — scan_once succeeds and
        // the failing bucket contributes nothing to the summary (the run
        // layer warns at the bucket, and the next pass retries).

        let root = tempfile::tempdir().unwrap();
        let n = 105;
        files(root.path(), n);
        let io = GatedRunner::<etag::Result>::new(4, 256);
        let scanner = scanner_with(root.path(), io.clone(), Arc::new(InlineRunner::default()));
        let scanner2 = scanner.clone();
        let scan = tokio::spawn(async move { scanner2.scan_once().await });
        wait_for(|| io.enqueued() == n).await;
        swap_all_for_symlinks(root.path(), n);
        io.open_gate();
        let summary = scan.await.unwrap().unwrap();
        assert_eq!(
            summary.reconciled, 0,
            "the aborted bucket must count nothing (F12)"
        );
        assert_eq!(summary.recomputed, 0);
    }

    #[tokio::test]
    async fn systematic_failures_abort_the_walk_early() {
        let root = tempfile::tempdir().unwrap();
        files(root.path(), 105);
        let io = FailingTaskRunner::new();
        let scanner = scanner_with(root.path(), io.clone(), Arc::new(InlineRunner::default()));
        scanner.scan_once().await.unwrap();
        assert_eq!(
            io.enqueued(),
            MAX_CONSECUTIVE_FAILURES as usize,
            "the abort must stop the walk at the threshold"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn one_failing_bucket_does_not_starve_the_pass() {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};

        use meta::Store;
        use tinio_core::ETag;

        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let bad = bucket::name("bad").unwrap();
        let good = bucket::name("good").unwrap();
        storage.create_bucket(&bad).await.unwrap();
        storage.create_bucket(&good).await.unwrap();
        fs::write(root.path().join("good/a.txt"), b"x")
            .await
            .unwrap();
        // The bad bucket's dir becomes unreadable — its walk fails
        // with PermissionDenied on every pass.
        fs::set_permissions(root.path().join("bad"), Permissions::from_mode(0o000))
            .await
            .unwrap();
        let scanner = Scanner::new(storage.clone(), options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(
            summary.reconciled, 1,
            "the good bucket reconciles despite the bad one (F12)"
        );
        let head = storage.head_object(&good, &"a.txt".into()).await.unwrap();
        assert_eq!(head.etag, ETag::from_content(b"x"));
    }

    #[tokio::test]
    async fn vanished_files_do_not_abort_the_bucket() {
        let root = tempfile::tempdir().unwrap();
        let n = 105;
        files(root.path(), n);
        let io = GatedRunner::<etag::Result>::new(4, 256);
        let scanner = scanner_with(root.path(), io.clone(), Arc::new(InlineRunner::default()));
        let scanner2 = scanner.clone();
        let scan = tokio::spawn(async move { scanner2.scan_once().await });
        wait_for(|| io.enqueued() == n).await;
        for i in 0..n {
            fs::remove_file(root.path().join("data").join(format!("f{i:02}.txt")))
                .await
                .unwrap();
        }
        io.open_gate();
        let summary = scan.await.unwrap().unwrap();
        assert_eq!(summary.reconciled, n);
        assert_eq!(summary.recomputed, 0);
    }

    #[tokio::test]
    async fn write_batch_failures_are_observed_and_the_scan_continues() {
        let root = tempfile::tempdir().unwrap();
        files(root.path(), 3);
        let (db, batches, failures) = FailingBatchRunner::new();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                meta_batch_size: 1, // one batch per entry (Q5)
                io_pipeline: Arc::new(InlineRunner::default()),
                db_pipeline: db,
                ..fs_options()
            },
        )
        .unwrap();
        let scanner = Scanner::new(storage, options());
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.recomputed, 3);
        assert_eq!(batches.load(Ordering::Relaxed), 3);
        assert_eq!(
            failures.load(Ordering::Relaxed),
            3,
            "every dropped batch's failure must be observed (R8)"
        );
        // The real writes landed despite the reported failures.
        assert_eq!(
            scanner
                .storage
                .meta_store()
                .walk(&bucket::name("data").unwrap())
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn hot_scan_enqueues_no_tasks() {
        let root = tempfile::tempdir().unwrap();
        files(root.path(), 3);
        let io = PacedRunner::<etag::Result>::new(1, 8, Duration::ZERO);
        let scanner = scanner_with(root.path(), io.clone(), Arc::new(InlineRunner::default()));
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.recomputed, 3);
        assert_eq!(io.enqueued(), 3);
        let summary = scanner.scan_once().await.unwrap();
        assert_eq!(summary.recomputed, 0);
        assert_eq!(io.enqueued(), 3, "hot pass: no compute tasks (P6)");
    }
}
