//! Shared database access handle.

use std::{
    array::from_fn,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use redb::{Database, ReadTransaction, ReadableDatabase, WriteTransaction};
use tinio_core::storage::{WRITE_LOCK_BUCKET_BOUNDS_US, WRITE_LOCK_BUCKETS};
use tokio::task::spawn_blocking;

use super::{
    compact::{needs_compact, snapshot},
    error::Error,
    open::open,
    tables::StateTable,
};

/// The histogram bucket of a duration in microseconds.
pub(crate) fn write_lock_bucket(duration_us: u64) -> usize {
    WRITE_LOCK_BUCKET_BOUNDS_US.partition_point(|bound| *bound <= duration_us)
}

/// A read-only snapshot of the write-lock histograms (pipeline-spec.md
/// §4). Two distributions are recorded per write transaction — the
/// **wait** (the `begin_write` return ≈ the lock wait, incl. a little
/// transaction initialization) and the **total** (the commit/abort return
/// — the whole transaction incl. fsync). Per-distribution `count`,
/// `sum`, and `max` let the metrics layer derive p50/p90/p99
/// approximations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteLockSnapshot {
    /// Wait-duration counts per bucket (index per
    /// [`WRITE_LOCK_BUCKET_BOUNDS_US`]).
    pub wait_buckets: [u64; WRITE_LOCK_BUCKETS],
    /// Total-duration counts per bucket.
    pub total_buckets: [u64; WRITE_LOCK_BUCKETS],
    /// Write transactions recorded.
    pub count: u64,
    /// Sum of wait durations, microseconds.
    pub wait_sum_us: u64,
    /// Maximum wait duration, microseconds.
    pub wait_max_us: u64,
    /// Sum of total durations, microseconds.
    pub total_sum_us: u64,
    /// Maximum total duration, microseconds.
    pub total_max_us: u64,
}

/// The fixed-bucket write-lock histogram behind [`Handle`] — plain
/// atomic counters, no third-party dependencies (pipeline-spec.md §4).
/// Cache-line aligned (item 6b, data-path review 2026-08-27): the
/// blocking-pool writers hammer the counters while the /metrics scrape
/// reads them from a request thread — the alignment keeps the two
/// groups off one shared line (the histogram sits next to the `Database`
/// handle in [`Handle`]).
#[derive(Debug, Default)]
#[repr(align(64))]
struct WriteHistogram {
    wait_buckets: [AtomicU64; WRITE_LOCK_BUCKETS],
    total_buckets: [AtomicU64; WRITE_LOCK_BUCKETS],
    count: AtomicU64,
    wait_sum_us: AtomicU64,
    wait_max_us: AtomicU64,
    total_sum_us: AtomicU64,
    total_max_us: AtomicU64,
}

impl WriteHistogram {
    /// Record one timed write transaction (wait and total, µs).
    ///
    /// `count` is incremented FIRST (F16): a scrape reading the snapshot
    /// between the count add and the bucket adds sees `count` ≥ the
    /// cumulative buckets — monotonic exposition (the newest sample may
    /// briefly sit in `count` without its bucket, an accepted
    /// approximation). The reverse order could expose
    /// `le=0.1` > `le="+Inf"` and break `histogram_quantile`.
    fn record(&self, waited: Duration, total: Duration) {
        let wait_us = waited.as_micros() as u64;
        let total_us = total.as_micros() as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.wait_buckets[write_lock_bucket(wait_us)].fetch_add(1, Ordering::Relaxed);
        self.total_buckets[write_lock_bucket(total_us)].fetch_add(1, Ordering::Relaxed);
        self.wait_sum_us.fetch_add(wait_us, Ordering::Relaxed);
        self.total_sum_us.fetch_add(total_us, Ordering::Relaxed);
        self.wait_max_us.fetch_max(wait_us, Ordering::Relaxed);
        self.total_max_us.fetch_max(total_us, Ordering::Relaxed);
    }
}

/// The shared access handle to a state database.
///
/// Every store holds an `Arc<Handle>` clone; the single redb writer
/// serializes writes (the per-store in-process locks are gone). `compact`
/// cannot run through this handle (`&mut` is structurally unavailable once
/// shared) — it runs before `FsStorage` construction (meta-redb-spec §5.9).
///
/// **Write transactions run on the tokio blocking pool** (G3, revised by
/// the data-path review 2026-08-27): every commit is a
/// `Durability::Immediate` fsync — millisecond-scale, not microseconds —
/// so the async request threads must not execute it inline.
/// `Self::write` / `Self::evaluate_compact` are `async fn` that hop
/// the closure + commit into `spawn_blocking`; the redb single-writer
/// lock still serializes commits (semantics unchanged). Point reads
/// stay inline — a read transaction takes no lock and no fsync;
/// full-bucket traversals (thousands of rows of validation +
/// allocation) go through `read_blocking` (P3, data-path review
/// 2026-08-27) — a scan must never execute inline on a runtime worker.
/// Do not add large full-table transactions to a hot path without
/// revisiting this.
/// Durability is redb's default `Immediate` (every commit flushes to
/// disk): safe, at one flush per write transaction — a 10 000-part
/// multipart upload costs 10 000 flushes by design (a `Durability::None`
/// would trade crash/outage safety for speed; the meta state is derivable
/// and self-healing, but this trade-off was deliberately not made).
#[derive(Debug)]
pub struct Handle {
    db: Database,
    /// The write-lock histograms (pipeline-spec.md §4).
    hist: WriteHistogram,
}

impl Handle {
    /// Open the state database at `state_dir` and wrap it (the single
    /// construction path of the standalone store constructors).
    pub fn open(state_dir: &Path) -> Result<Arc<Self>, Error> {
        Ok(Self::new(open(state_dir)?.db))
    }

    /// Wrap an opened database (after the pre-sharing compact window —
    /// the orchestration path: `open` → `compact_if_needed` → `Handle::new`,
    /// meta-redb-spec §5.9/G1).
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            hist: WriteHistogram::default(),
        })
    }

    /// Run `f` against a read transaction.
    ///
    /// Zero-copy guards live only inside `f`; return owned copies (guards
    /// are invalid once the transaction ends).
    pub(crate) fn read<T>(
        &self,
        f: impl FnOnce(&ReadTransaction) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let txn = self.db.begin_read()?;
        f(&txn)
    }

    /// Run `f` against a read transaction on the tokio blocking pool —
    /// the read-side counterpart of [`Self::write`]'s hop (P3, data-path
    /// review 2026-08-27). A full-bucket traversal (row-by-row validation
    /// plus allocation — hundreds of ms at 1M rows) must not execute
    /// inline on the runtime workers: it would block the same worker's
    /// request tasks. The closure + `begin_read` run on the blocking
    /// pool via `spawn_blocking`; the closure must be `'static` — call
    /// sites clone their captured data into it (one small alloc per
    /// scan, noise next to the scan itself). Point reads stay on the
    /// inline [`Self::read`] (no lock, no fsync, O(1) cost). A panic
    /// inside the closure re-panics the caller via the join error (like
    /// [`Self::write`]).
    pub(crate) async fn read_blocking<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&ReadTransaction) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error>
    where
        T: Send + 'static,
    {
        let this = Arc::clone(self);
        spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            f(&txn)
        })
        .await
        .unwrap_or_else(|join| panic!("the read-transaction task panicked: {join}"))
    }

    /// Run `f` against a write transaction — committed on success, aborted
    /// on error. Multi-table operations run as one write closure. Timed:
    /// the entry-to-`begin_write` return is the approximate lock wait and
    /// the commit/abort return is the total duration, both recorded into
    /// the write-lock histograms (pipeline-spec.md §4).
    ///
    /// **Async (G3, revised by the data-path review 2026-08-27)**: the
    /// closure + commit run on the tokio blocking pool via
    /// `spawn_blocking`, so the caller's runtime worker is never occupied
    /// by the commit's fsync. The closure must be `'static` — call sites
    /// clone their captured data into it (one small alloc per write,
    /// noise next to the fsync).
    pub(crate) async fn write<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut WriteTransaction) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error>
    where
        T: Send + 'static,
    {
        self.timed_write(move |mut txn| {
            let result = f(&mut txn);
            match result {
                Ok(value) => txn.commit().map(|()| value).map_err(Into::into),
                Err(err) => {
                    let _ = txn.abort();
                    Err(err)
                }
            }
        })
        .await
    }

    /// The timing write-transaction wrapper (pipeline-spec.md §4, P5):
    /// the entry-to-`begin_write` return ≈ the lock wait, the
    /// entry-to-return = the total duration (incl. fsync) — both recorded
    /// into the histograms. The single home of the timing path: `write`
    /// and `evaluate_compact` share it, so **every** write transaction is
    /// covered (the compact stats call is the only direct `begin_write`
    /// besides this wrapper). The closure receives the transaction by
    /// value — it owns the commit/abort (redb's `commit`/`abort` consume
    /// the transaction).
    ///
    /// The body runs on the blocking pool (see [`Self::write`]). The
    /// `start` instant is captured **before** the `spawn_blocking` hop,
    /// so the recorded wait/total span the hop: the blocking-pool queue
    /// delay counts as wait (a queued write is waiting for the lock *and*
    /// for a pool slot; the redb single-writer lock dominates once in
    /// flight — the queue is short in practice). The histogram record
    /// happens on the blocking thread (plain atomic counters). Dropping
    /// the returned future (task abort) does not cancel the transaction —
    /// the closure still runs to its commit/abort on the pool, so the
    /// database never sees a torn transaction. A panic inside the closure
    /// re-panics the caller via the join error (the old inline call
    /// propagated panics too).
    async fn timed_write<T>(
        self: &Arc<Self>,
        f: impl FnOnce(WriteTransaction) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error>
    where
        T: Send + 'static,
    {
        // The wrapper entry — the recorded wait/total span the
        // spawn_blocking hop (queue delay included, see the doc above).
        let start = Instant::now();
        let this = Arc::clone(self);
        spawn_blocking(move || this.timed_write_blocking(start, f))
            .await
            .unwrap_or_else(|join| panic!("the write-transaction task panicked: {join}"))
    }

    /// The synchronous timing body — runs on the blocking pool
    /// ([`Self::timed_write`]). `start` was captured at the async wrapper
    /// entry, before the hop.
    fn timed_write_blocking<T>(
        self: Arc<Self>,
        start: Instant,
        f: impl FnOnce(WriteTransaction) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let txn = match self.db.begin_write() {
            Ok(txn) => txn,
            Err(err) => {
                // No transaction opened: wait == total (no commit phase).
                // A failed begin_write is not a write transaction — this
                // records the no-transaction path, unreachable in practice
                // (Handle owns the Database).
                let duration = start.elapsed();
                self.hist.record(duration, duration);
                return Err(err.into());
            }
        };
        let waited = start.elapsed();
        let result = f(txn);
        let total = start.elapsed();
        self.hist.record(waited, total);
        result
    }

    /// A read-only snapshot of the write-lock histograms
    /// (pipeline-spec.md §4): per-bucket counts plus count/sum/max per
    /// distribution. The tinio-server metrics layer converts it to
    /// prometheus cumulative buckets on scrape (a cheap atomic snapshot —
    /// no 30 s TTL cache; the TTL pattern belongs to the storage gauges
    /// only).
    pub fn write_lock_stats(&self) -> WriteLockSnapshot {
        WriteLockSnapshot {
            wait_buckets: from_fn(|i| self.hist.wait_buckets[i].load(Ordering::Relaxed)),
            total_buckets: from_fn(|i| self.hist.total_buckets[i].load(Ordering::Relaxed)),
            count: self.hist.count.load(Ordering::Relaxed),
            wait_sum_us: self.hist.wait_sum_us.load(Ordering::Relaxed),
            wait_max_us: self.hist.wait_max_us.load(Ordering::Relaxed),
            total_sum_us: self.hist.total_sum_us.load(Ordering::Relaxed),
            total_max_us: self.hist.total_max_us.load(Ordering::Relaxed),
        }
    }

    /// Evaluate fragmentation against `threshold_percent` and update the
    /// `compact_needed` marker — ONE write transaction (the stats call
    /// takes the write lock; the marker write is skipped when unchanged,
    /// so an unchanged evaluation costs a single aborted transaction).
    /// Returns whether the marker ended up set. The sweep calls this once
    /// per round; the marker is consumed at startup by
    /// [`super::compact::compact_if_needed`] (meta-redb-spec §5.9). The
    /// transaction goes through the same timing wrapper as [`Self::write`]
    /// (P5 — all write transactions are covered) and runs on the blocking
    /// pool like it (G3 revision — the stats call is a write transaction).
    pub(crate) async fn evaluate_compact(
        self: &Arc<Self>,
        threshold_percent: u8,
    ) -> Result<bool, Error> {
        self.timed_write(move |mut txn| {
            let stats = txn.stats()?;
            let needed = needs_compact(&snapshot(&stats), threshold_percent);
            let changed = {
                let mut state = StateTable::open(&mut txn)?;
                let current = state.compact_marker()?;
                if current == needed {
                    false
                } else {
                    state.set_compact_marker_value(needed)?;
                    true
                }
            };
            if changed {
                txn.commit()?;
            } else {
                txn.abort()?;
            }
            Ok(needed)
        })
        .await
    }

    /// Whether the `compact_needed` marker is set — the marker protocol's
    /// read half (meta-redb-spec §5.9; the startup orchestration and
    /// doctor report it, the runtime evaluation path is
    /// `Self::evaluate_compact`).
    pub fn compact_needed(&self) -> Result<bool, Error> {
        self.read(|txn| StateTable::open_readonly(txn)?.compact_marker())
    }

    /// Set or clear the `compact_needed` marker — the marker protocol's
    /// write half (meta-redb-spec §5.9; doctor `--fix` sets/clears it, the
    /// runtime path goes through `Self::evaluate_compact`). Async like
    /// every write transaction (G3 revision).
    pub async fn mark_compact_needed(self: &Arc<Self>, needed: bool) -> Result<(), Error> {
        self.write(move |txn| StateTable::open(txn)?.set_compact_marker_value(needed))
            .await
    }
}
