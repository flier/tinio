//! The shared redb driver: the async transaction handle the backends run
//! their metadata against.
//!
//! Relocated from tinio-fs's `database/handle.rs` (plan 2026-10-03 Task 4):
//! the database arm, the transaction entry, and the closure-passing
//! `read`/`write` pattern are the shared store handle's
//! ([`crate::store::Handle`]); this type adds the tokio blocking-pool hop
//! every transaction takes and the write-lock histograms the server's
//! `tinio_storage_write_lock_*` metrics are scraped from.
//!
//! **The closure error is generic** (`E: From<Error>`), exactly as the
//! wrapped store handle's is: a backend accumulates its own error through a
//! transaction, so tinio-fs's closures keep returning the fs database error
//! while `tinio-store-redb`'s own implementations use [`crate::Error`]
//! directly.

use std::{
    array::from_fn,
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use redb::{Database, ReadTransaction, WriteTransaction};
use tokio::task::{JoinError, spawn_blocking};

use crate::{
    _core::storage::{
        WRITE_LOCK_BUCKET_BOUNDS_US, WRITE_LOCK_BUCKETS, WriteLockSnapshot,
    },
    compact::{needs_compact, snapshot},
    error::Error,
    open::{open, open_read_only},
    state, store,
};

/// The histogram bucket of a duration in microseconds.
pub(crate) fn write_lock_bucket(duration_us: u64) -> usize {
    WRITE_LOCK_BUCKET_BOUNDS_US.partition_point(|bound| *bound <= duration_us)
}

/// The failure a blocking-pool join reports: the closure panicked, or the task
/// was cancelled before it ran, so no transaction answered. The value is the
/// store's own error, never a panic re-raised on the caller's side (style.md
/// §Panics — business code returns); the join's message names the task and the
/// panic payload, so the detail rides the error's chain to the log.
fn blocking_task_failed(what: &str, join: JoinError) -> Error {
    Error::Io(io::Error::other(format!("the {what} task failed: {join}")))
}

/// The fixed-bucket write-lock histogram behind [`Driver`] — plain
/// atomic counters, no third-party dependencies (pipeline-spec.md §4).
/// Cache-line aligned (item 6b, data-path review 2026-08-27): the
/// blocking-pool writers hammer the counters while the /metrics scrape
/// reads them from a request thread — the alignment keeps the two
/// groups off one shared line (the histogram sits next to the `Database`
/// handle in [`Driver`]).
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

/// The access handle to a state database.
///
/// Every store holds an `Arc<Driver>` clone; the single redb writer
/// serializes writes (the per-store in-process locks are gone). `compact`
/// cannot run through this handle (`&mut` is structurally unavailable once
/// shared) — it runs before storage construction (meta-redb-spec §5.9).
/// The database arm — writable, read-only, or the degraded empty view, and
/// with it the write refusal — is the wrapped store handle's
/// ([`crate::store::Handle`], FR-023 amended).
///
/// **Write transactions run on the tokio blocking pool** (G3, revised by
/// the data-path review 2026-08-27): every commit is a
/// `Durability::Immediate` fsync — millisecond-scale, not microseconds —
/// so the async request threads must not execute it inline.
/// [`Self::write`] / [`Self::evaluate_compact`] are `async fn` that hop
/// the closure + commit into `spawn_blocking`; the redb single-writer
/// lock still serializes commits (semantics unchanged). Point reads
/// stay inline — a read transaction takes no lock and no fsync;
/// full-bucket traversals (thousands of rows of validation +
/// allocation) go through [`Self::read_blocking`] (P3, data-path review
/// 2026-08-27) — a scan must never execute inline on a runtime worker.
/// Do not add large full-table transactions to a hot path without
/// revisiting this.
/// Durability is redb's default `Immediate` (every commit flushes to
/// disk): safe, at one flush per write transaction — a 10 000-part
/// multipart upload costs 10 000 flushes by design (a `Durability::None`
/// would trade crash/outage safety for speed; the meta state is derivable
/// and self-healing, but this trade-off was deliberately not made).
#[derive(Debug)]
pub struct Driver {
    /// The shared store handle: the database arm, its transactions and
    /// the closure-passing read/write pattern. Shared behind an `Arc` so a
    /// backend can hold the same handle beside the driver
    /// ([`Driver::store_handle`]).
    db: Arc<store::Handle>,
    /// The write-lock histograms (pipeline-spec.md §4).
    hist: WriteHistogram,
}

impl Driver {
    /// Open the state database at `state_dir` and wrap it (the single
    /// construction path of the standalone store constructors).
    pub fn open(state_dir: &Path) -> Result<Arc<Self>, Error> {
        Ok(Self::wrap(store::Handle::new(open(state_dir)?.db)))
    }

    /// Open `state_dir`'s database **read-only** and wrap it — the
    /// read-only run's metadata source (FR-023 amended). The open
    /// creates nothing; its failure is the caller's to report (the
    /// degraded read-only view is [`Driver::empty`]).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::Driver;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// // A read-write run created the root's database...
    /// drop(Driver::open(dir.path()).unwrap());
    /// // ...and the read-only run opens the same file, creating nothing.
    /// let driver = Driver::open_read_only(dir.path()).unwrap();
    /// assert!(!driver.compact_needed().unwrap());
    /// ```
    pub fn open_read_only(state_dir: &Path) -> Result<Arc<Self>, Error> {
        Ok(Self::wrap(store::Handle::new_read_only(open_read_only(
            state_dir,
        )?)))
    }

    /// Wrap an empty in-memory database — the degraded read-only view:
    /// every table reads as empty, every write is refused.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_store_redb::Driver;
    ///
    /// // The view a read-only run degrades to when the root database
    /// // cannot be opened: reads answer the empty state, writes are
    /// // refused with the read-only refusal (`Error::is_read_only`).
    /// let driver = Driver::empty().unwrap();
    /// assert!(!driver.compact_needed().unwrap());
    /// ```
    #[tracing::instrument(
        target = "tinio::storage",
        level = "info",
        skip_all,
        fields(kind = "degraded", reason = "state database unavailable")
    )]
    pub fn empty() -> Result<Arc<Self>, Error> {
        Ok(Self::wrap(store::Handle::empty()?))
    }

    /// Wrap an opened database (after the pre-sharing compact window —
    /// the orchestration path: `open` → `compact_if_needed` → `Driver::new`,
    /// meta-redb-spec §5.9/G1).
    pub fn new(db: Database) -> Arc<Self> {
        Self::wrap(store::Handle::new(db))
    }

    fn wrap(db: store::Handle) -> Arc<Self> {
        Arc::new(Self {
            db: Arc::new(db),
            hist: WriteHistogram::default(),
        })
    }

    /// The shared store handle this driver runs its transactions through.
    ///
    /// A backend whose **own** tables live in the same database (tinio-mem's
    /// version blobs and part-stat rows) shares this handle with the
    /// contract implementations instead of opening a second database — one
    /// database, so a mem-local table's write and the contract row's write
    /// run against the same state. The fs backend opens its own handle (its
    /// blocking-pool hop and histograms live there).
    pub fn store_handle(&self) -> Arc<store::Handle> {
        Arc::clone(&self.db)
    }

    /// Run `f` against a read transaction — every arm reads.
    ///
    /// Zero-copy guards live only inside `f`; return owned copies (guards
    /// are invalid once the transaction ends).
    pub fn read<T, E>(&self, f: impl FnOnce(&ReadTransaction) -> Result<T, E>) -> Result<T, E>
    where
        E: From<Error>,
    {
        self.db.read(f)
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
    /// inside the closure is answered as an error — the join failure
    /// lifts into the store's `Error` (the private `blocking_task_failed`)
    /// rather than re-raising on the caller's side (style.md §Panics) —
    /// like [`Self::write`].
    pub async fn read_blocking<T, E>(
        self: &Arc<Self>,
        f: impl FnOnce(&ReadTransaction) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<Error> + Send + 'static,
    {
        let this = Arc::clone(self);
        spawn_blocking(move || this.read(f))
            .await
            .map_err(|join| E::from(blocking_task_failed("read-transaction", join)))?
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
    ///
    /// **The redb 4.2.0 page-manager ordering rule (Task 6, durability
    /// finding I1).** A write closure that both removes from and inserts
    /// into the *same* table must run **every removal before every
    /// insert**. The other order frees a page of the table and then
    /// reuses or re-reads it, tripping the debug assertion
    /// `!read_page_ref_counts...contains_key(&page)`
    /// (`tree_store/page_store/page_manager.rs`); the failing write
    /// surfaces one frame up as the write transaction's error (the
    /// `spawn_blocking` join in `timed_write` and in
    /// [`Self::read_blocking`]). A range iterator that is still alive
    /// while point removes run on the same table is the same hazard:
    /// collect the keys in an inner scope so the iterator is dropped
    /// first (the `drain_impl` macro in `crate::scan`). `put_with_parts`,
    /// `move_` and the drains are the worked examples.
    #[tracing::instrument(
        target = "tinio::storage",
        level = "trace",
        skip_all,
        fields(kind = "write")
    )]
    pub async fn write<T, E>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut WriteTransaction) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<Error> + Send + 'static,
    {
        self.timed_write(move |mut txn| {
            let result = f(&mut txn);
            match result {
                Ok(value) => txn
                    .commit()
                    .map(|()| value)
                    .map_err(|err| E::from(Error::from(err))),
                Err(err) => {
                    let _ = txn.abort();
                    Err(err)
                }
            }
        })
        .await
    }

    /// The no-op-aware sibling of [`Self::write`]: the closure reports
    /// whether it changed the database (`None` = nothing to commit), and
    /// a clean no-op drops the write transaction without the commit
    /// fsync — the idempotent tag clears and the row-missing probes of
    /// the tag ops cost nothing. The `Err` arm aborts like `write`, and
    /// the same redb page-manager ordering rule applies (see
    /// [`Self::write`]: removals before inserts within one table, and no
    /// live range iterator across the removals).
    #[tracing::instrument(
        target = "tinio::storage",
        level = "trace",
        skip_all,
        fields(kind = "write_if")
    )]
    pub async fn write_if<T, E>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut WriteTransaction) -> Result<Option<T>, E> + Send + 'static,
    ) -> Result<Option<T>, E>
    where
        T: Send + 'static,
        E: From<Error> + Send + 'static,
    {
        self.timed_write(move |mut txn| {
            let result = f(&mut txn);
            match result {
                Ok(Some(value)) => txn
                    .commit()
                    .map(|()| Some(value))
                    .map_err(|err| E::from(Error::from(err))),
                Ok(None) => {
                    let _ = txn.abort();
                    Ok(None)
                }
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
    /// is answered as an error (the join failure lifts into the store's
    /// `Error`); the old inline call propagated the panic instead.
    async fn timed_write<T, E>(
        self: &Arc<Self>,
        f: impl FnOnce(WriteTransaction) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<Error> + Send + 'static,
    {
        // A non-writable arm refuses before the hop: no transaction exists
        // to open, and the refusal is the arm's answer (the wrapped store
        // handle's), not a pool failure.
        if !self.db.is_writable() {
            return Err(E::from(Error::ReadOnly));
        }
        // The wrapper entry — the recorded wait/total span the
        // spawn_blocking hop (queue delay included, see the doc above).
        let start = Instant::now();
        let this = Arc::clone(self);
        spawn_blocking(move || this.timed_write_blocking(start, f))
            .await
            .map_err(|join| E::from(blocking_task_failed("write-transaction", join)))?
    }

    /// The synchronous timing body — runs on the blocking pool
    /// ([`Self::timed_write`]). `start` was captured at the async wrapper
    /// entry, before the hop.
    fn timed_write_blocking<T, E>(
        self: Arc<Self>,
        start: Instant,
        f: impl FnOnce(WriteTransaction) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Error>,
    {
        // The write transaction's entry is the store handle's, which owns
        // the arm (and with it the refusal): a non-writable arm answered it
        // at the async wrapper, before the hop.
        let txn = match self.db.begin_write() {
            Ok(txn) => txn,
            Err(err) => {
                // No transaction opened: wait == total (no commit phase).
                // A failed begin_write is not a write transaction — this
                // records the no-transaction path, unreachable in practice
                // (the handle owns the Database).
                let duration = start.elapsed();
                self.hist.record(duration, duration);
                return Err(E::from(err));
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
    /// [`crate::compact::compact_if_needed`] (meta-redb-spec §5.9). The
    /// transaction goes through the same timing wrapper as [`Self::write`]
    /// (P5 — all write transactions are covered) and runs on the blocking
    /// pool like it (G3 revision — the stats call is a write transaction).
    #[tracing::instrument(
        target = "tinio::storage",
        level = "info",
        skip_all,
        fields(threshold_percent = threshold_percent)
    )]
    pub async fn evaluate_compact(self: &Arc<Self>, threshold_percent: u8) -> Result<bool, Error> {
        self.timed_write::<bool, Error>(move |mut txn| {
            let stats = txn.stats()?;
            let needed = needs_compact(&snapshot(&stats), threshold_percent);
            let changed = {
                let mut state_table = state::Table::open(&mut txn)?;
                let current = state_table.compact_marker()?;
                if current == needed {
                    false
                } else {
                    state_table.set_compact_marker(needed)?;
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
        self.read(|txn| state::Table::open_readonly(txn)?.compact_marker())
    }

    /// Set or clear the `compact_needed` marker — the marker protocol's
    /// write half (meta-redb-spec §5.9; doctor `--fix` sets/clears it, the
    /// runtime path goes through `Self::evaluate_compact`). Async like
    /// every write transaction (G3 revision).
    pub async fn mark_compact_needed(self: &Arc<Self>, needed: bool) -> Result<(), Error> {
        self.write::<(), Error>(move |txn| {
            let mut state_table = state::Table::open(txn)?;
            state_table.set_compact_marker(needed)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::Arc,
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use io::Error as IoError;
    use redb::Database;
    use tokio::{runtime::Builder, sync::oneshot, time::timeout};
    use tracing::Level;

    use super::*;
    use tinio_util::testing::spans::one_named;
    use crate::{
        _core::{bucket::name, object},
        bucket, object_meta, open, state,
        testutil::capture_spans,
    };

    fn open_db() -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().unwrap();
        let opened = open::open(dir.path()).unwrap();
        (dir, opened.db)
    }

    #[tokio::test]
    async fn handle_read_write_round_trip() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let name = name("alpha").unwrap();
        let created = UNIX_EPOCH + Duration::from_nanos(42);
        let write_name = name.clone();
        driver
            .write(move |txn| -> Result<(), Error> {
                bucket::Table::open(txn)?.put(&write_name, created)?;
                Ok(())
            })
            .await
            .unwrap();
        let got = driver
            .read(|txn| -> Result<Option<SystemTime>, Error> {
                bucket::Table::open_readonly(txn)?.get(&name)
            })
            .unwrap();
        assert_eq!(got, Some(created));
        drop(dir);
    }

    #[tokio::test]
    async fn write_closure_aborts_on_error() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let name = name("beta").unwrap();
        let write_name = name.clone();
        let err = driver
            .write(move |txn| -> Result<(), Error> {
                bucket::Table::open(txn)?.put(&write_name, UNIX_EPOCH)?;
                Err(Error::Io(IoError::other("boom")))
            })
            .await;
        assert!(err.is_err());
        // The aborted transaction persisted nothing.
        let got = driver
            .read(|txn| -> Result<bool, Error> {
                Ok(bucket::Table::open_readonly(txn)?.get(&name)?.is_some())
            })
            .unwrap();
        assert!(!got);
        drop(dir);
    }

    #[tokio::test]
    async fn compact_marker_round_trip() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        assert!(!driver.compact_needed().unwrap());
        driver.mark_compact_needed(true).await.unwrap();
        assert!(driver.compact_needed().unwrap());
        driver.mark_compact_needed(false).await.unwrap();
        assert!(!driver.compact_needed().unwrap());
        drop(dir);
    }

    // --- write-lock histograms (pipeline-spec.md §4) ---

    /// The bucket boundaries are pinned by the shared constants: a duration
    /// `d` lands in bucket `i` where `bounds[i-1] <= d < bounds[i]` (bucket
    /// 0: `d < 10 µs`), and the open last bucket holds `d >= 100 000 µs`.
    #[test]
    fn write_lock_bucket_boundaries() {
        // Just below each bound → the previous bucket; exactly on a bound →
        // the bound's own bucket (partition_point on `<=`).
        let cases: &[(u64, usize)] = &[
            (0, 0),
            (9, 0),
            (10, 1),
            (99, 1),
            (100, 2),
            (999, 2),
            (1_000, 3),
            (4_999, 3),
            (5_000, 4),
            (19_999, 4),
            (20_000, 5),
            (99_999, 5),
            (100_000, 6),
            (u64::MAX, 6),
        ];
        for (duration, expected) in cases {
            assert_eq!(
                write_lock_bucket(*duration),
                *expected,
                "duration {duration} µs"
            );
        }
    }

    /// The recorded snapshot of a fresh driver: zero write transactions must
    /// produce a fully zero histogram — no spurious empty buckets.
    #[tokio::test]
    async fn write_lock_stats_start_all_zero() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let snapshot = driver.write_lock_stats();
        assert_eq!(snapshot.count, 0);
        assert_eq!(snapshot.wait_sum_us, 0);
        assert_eq!(snapshot.total_sum_us, 0);
        assert_eq!(snapshot.wait_max_us, 0);
        assert_eq!(snapshot.total_max_us, 0);
        assert!(snapshot.wait_buckets.iter().all(|n| *n == 0));
        assert!(snapshot.total_buckets.iter().all(|n| *n == 0));
        drop(dir);
    }

    /// One write transaction records exactly one wait + one total sample
    /// (wait ≤ total — the lock wait is a prefix of the transaction).
    #[tokio::test]
    async fn write_transactions_are_timed() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let name = name("alpha").unwrap();
        driver
            .write(move |txn| -> Result<(), Error> {
                bucket::Table::open(txn)?.put(&name, UNIX_EPOCH)?;
                Ok(())
            })
            .await
            .unwrap();
        let snapshot = driver.write_lock_stats();
        assert_eq!(snapshot.count, 1);
        assert_eq!(snapshot.wait_buckets.iter().sum::<u64>(), 1);
        assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 1);
        assert!(
            snapshot.wait_sum_us <= snapshot.total_sum_us,
            "the lock wait must be a prefix of the total: {} > {}",
            snapshot.wait_sum_us,
            snapshot.total_sum_us
        );
        assert_eq!(snapshot.total_max_us, snapshot.total_sum_us);
        // The max sample's bucket carries the single count.
        let bucket = write_lock_bucket(snapshot.total_max_us);
        assert_eq!(snapshot.total_buckets[bucket], 1);
        drop(dir);
    }

    /// A slow write closure lands in the overflow bucket (>100k µs)
    /// deterministically — the histogram records real durations, not just
    /// counts.
    #[tokio::test]
    async fn slow_write_lands_in_the_last_bucket() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let name = name("gamma").unwrap();
        driver
            .write(move |txn| -> Result<(), Error> {
                bucket::Table::open(txn)?.put(&name, UNIX_EPOCH)?;
                thread::sleep(Duration::from_millis(150));
                Ok(())
            })
            .await
            .unwrap();
        let snapshot = driver.write_lock_stats();
        assert_eq!(snapshot.count, 1);
        assert_eq!(snapshot.total_buckets.len(), 7);
        assert_eq!(
            snapshot.total_buckets[6], 1,
            "a 150 ms transaction must land in the >100k µs bucket"
        );
        assert!(
            snapshot.total_max_us >= 100_000,
            "150 ms must exceed the 100 ms overflow bound: {}",
            snapshot.total_max_us
        );
        drop(dir);
    }

    /// An aborted write closure is still one timed transaction (the total
    /// covers the abort) and persists nothing.
    #[tokio::test]
    async fn aborted_write_is_recorded() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let name = name("delta").unwrap();
        let write_name = name.clone();
        let err = driver
            .write(move |txn| -> Result<(), Error> {
                bucket::Table::open(txn)?.put(&write_name, UNIX_EPOCH)?;
                Err(Error::Io(IoError::other("boom")))
            })
            .await;
        assert!(err.is_err());
        let snapshot = driver.write_lock_stats();
        assert_eq!(snapshot.count, 1);
        assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 1);
        drop(dir);
    }

    /// Read transactions are never timed — the histograms cover write
    /// transactions only.
    #[tokio::test]
    async fn reads_do_not_record() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let b = name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        driver
            .read(|txn| -> Result<bool, Error> {
                Ok(object_meta::Table::open_readonly(txn)?
                    .get(&b, &k)?
                    .is_some())
            })
            .unwrap();
        assert_eq!(driver.write_lock_stats().count, 0);
        drop(dir);
    }

    /// P5: `evaluate_compact` is the only direct `begin_write` path besides
    /// the `write` wrapper — its stats transaction goes through the SAME
    /// timing helper, so "all write transactions are covered" holds.
    #[tokio::test]
    async fn evaluate_compact_is_timed() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        assert_eq!(driver.write_lock_stats().count, 0);
        driver.evaluate_compact(20).await.unwrap();
        let snapshot = driver.write_lock_stats();
        assert_eq!(
            snapshot.count, 1,
            "the stats transaction must be timed (P5)"
        );
        assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 1);
        // A second evaluation with the marker unchanged is still a write
        // transaction (an aborted one) — still recorded.
        driver.evaluate_compact(20).await.unwrap();
        assert_eq!(driver.write_lock_stats().count, 2);
        drop(dir);
    }

    /// Multiple transactions accumulate per-bucket counts and the sum/max.
    #[tokio::test]
    async fn snapshots_accumulate_across_transactions() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        for i in 0..5u64 {
            let name = name(format!("bucket{i}")).unwrap();
            driver
                .write(move |txn| -> Result<(), Error> {
                    bucket::Table::open(txn)?.put(&name, UNIX_EPOCH)?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        let snapshot = driver.write_lock_stats();
        assert_eq!(snapshot.count, 5);
        assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 5);
        assert_eq!(snapshot.wait_buckets.iter().sum::<u64>(), 5);
        assert!(snapshot.total_max_us >= snapshot.total_sum_us / 5);
        drop(dir);
    }

    /// P1 (G3 revision): a write transaction executes on the tokio blocking
    /// pool, NOT on the runtime worker. Deterministic proof: on a
    /// single-thread runtime, a write parked inside its closure (the write
    /// transaction is open — the redb write lock is held) must not stop the
    /// worker — a read transaction completes while the write is still
    /// parked, and the write task is still in flight when the read returns.
    /// (Pre-revision the inline write would hang the runtime forever — the
    /// worker is occupied, so the timeout is never polled.)
    #[test]
    fn async_writes_run_on_the_blocking_pool() {
        let runtime = Builder::new_current_thread().enable_time().build().unwrap();
        runtime.block_on(async {
            let (dir, db) = open_db();
            let driver = Driver::new(db);
            let name = name("data").unwrap();
            let (started_tx, started_rx) = oneshot::channel();
            let (gate_tx, gate_rx) = oneshot::channel::<()>();
            let writer = {
                let driver = Arc::clone(&driver);
                let name = name.clone();
                tokio::spawn(async move {
                    driver
                        .write(move |txn| -> Result<(), Error> {
                            bucket::Table::open(txn)?.put(&name, UNIX_EPOCH)?;
                            let _ = started_tx.send(());
                            // Park with the write transaction OPEN — the
                            // redb write lock is held from here on.
                            let _ = gate_rx.blocking_recv();
                            Ok(())
                        })
                        .await
                        .unwrap()
                })
            };
            // The write is running on the blocking pool and parked (not
            // committed). A failure to reach the park within 5 s means the
            // worker was occupied — the pre-revision hang.
            timeout(Duration::from_secs(5), started_rx)
                .await
                .expect("the write never reached the blocking pool")
                .unwrap();
            // The single runtime worker is free: a read transaction
            // completes here while the write is still parked. The parked
            // write is uncommitted, so the row is invisible (redb MVCC)...
            let got = driver
                .read(|txn| -> Result<Option<SystemTime>, Error> {
                    bucket::Table::open_readonly(txn)?.get(&name)
                })
                .unwrap();
            assert_eq!(got, None, "the parked write must not be committed yet");
            // ...and the parked write is still in flight (it holds the
            // write lock — the single-writer serialization is unchanged).
            assert!(
                !writer.is_finished(),
                "the parked write must still be in flight"
            );
            drop(gate_tx);
            writer.await.unwrap();
            // The write committed when the closure returned: the row is
            // now visible.
            let got = driver
                .read(|txn| -> Result<Option<SystemTime>, Error> {
                    bucket::Table::open_readonly(txn)?.get(&name)
                })
                .unwrap();
            assert_eq!(got, Some(UNIX_EPOCH));
            drop(dir);
        });
    }

    /// A panic inside the write closure is reported to the caller as an error:
    /// the blocking task's panic surfaces as a `JoinError`, which the wrapper
    /// lifts into the store's `Error` — never a panic re-raised on the awaiting
    /// side (style.md §Panics; the old inline call propagated it instead). The
    /// open transaction is dropped, i.e. aborted, so the database stays
    /// consistent: the failing call reports the failure and the next one still
    /// runs.
    #[tokio::test]
    async fn a_panicking_write_closure_is_reported_as_an_error() {
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let err = driver
            .write(move |_txn| -> Result<(), Error> { panic!("boom") })
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Io(_)),
            "the join failure lifts into the store's error: {err:?}"
        );
        assert!(
            err.to_string()
                .contains("the write-transaction task failed"),
            "the failure names the transaction that died: {err}"
        );
        // The aborted transaction persisted nothing and the driver still
        // serves transactions: the panic cost one call, not the database.
        let name = name("after-panic").unwrap();
        let write_name = name.clone();
        driver
            .write(move |txn| -> Result<(), Error> {
                bucket::Table::open(txn)?.put(&write_name, UNIX_EPOCH)?;
                Ok(())
            })
            .await
            .unwrap();
        let got = driver
            .read(|txn| -> Result<Option<SystemTime>, Error> {
                bucket::Table::open_readonly(txn)?.get(&name)
            })
            .unwrap();
        assert_eq!(got, Some(UNIX_EPOCH));
        drop(dir);
    }

    /// FR-023 amended: a read-only driver reads and refuses every write. The
    /// read-only policy refuses the mutating operations upstream and a
    /// read-only run runs neither the scanner nor the sweeper, so a write
    /// through the driver is a programming error — and the refusal is loud
    /// (the read-only refusal, `Error::is_read_only`), on both non-writable
    /// arms and on the degraded empty view, without opening a transaction.
    #[tokio::test]
    async fn a_read_only_driver_refuses_writes_and_serves_reads() {
        let (dir, db) = open_db();
        {
            let mut txn = db.begin_write().unwrap();
            state::Table::open(&mut txn)
                .unwrap()
                .set_compact_marker(true)
                .unwrap();
            txn.commit().unwrap();
        }
        drop(db);

        let driver = Driver::open_read_only(dir.path()).unwrap();
        // Reads work, over the database's own rows.
        assert!(driver.compact_needed().unwrap());

        // Both write arms answer the refusal, before any transaction exists.
        let err = driver
            .write(|_txn| -> Result<(), Error> { Ok(()) })
            .await
            .unwrap_err();
        assert!(err.is_read_only(), "{err:?}");
        assert_eq!(err.to_string(), "the state database is read-only");
        let err = driver
            .write_if(|_txn| -> Result<Option<()>, Error> { Ok(Some(())) })
            .await
            .unwrap_err();
        assert!(err.is_read_only(), "{err:?}");

        // The degraded empty view reads as empty and refuses writes alike.
        let empty = Driver::empty().unwrap();
        assert!(!empty.compact_needed().unwrap());
        let err = empty
            .write(move |_txn| -> Result<(), Error> { panic!("no transaction exists") })
            .await
            .unwrap_err();
        assert!(err.is_read_only(), "{err:?}");
        drop(dir);
    }

    /// The transaction choke points open one span at their tier on the
    /// storage target: trace for the unlabelled write pair, info for the
    /// once-per-cycle compaction evaluation. The driver cannot know the
    /// caller's op, so the span carries the transaction kind only.
    #[tokio::test]
    async fn transaction_spans_carry_their_tier_and_kind() {
        let capture = capture_spans();
        let watermark = capture.watermark();
        let (dir, db) = open_db();
        let driver = Driver::new(db);
        let name = name("traced").unwrap();
        driver
            .write(move |txn| -> Result<(), Error> {
                bucket::Table::open(txn)?.put(&name, UNIX_EPOCH)?;
                Ok(())
            })
            .await
            .unwrap();
        driver
            .write_if(|_txn| -> Result<Option<()>, Error> { Ok(None) })
            .await
            .unwrap();
        driver.evaluate_compact(20).await.unwrap();

        let spans = capture.spans_since(watermark);
        let one = |name: &str| one_named(&spans, name);
        let write = one("write");
        assert_eq!(write.target, "tinio::storage");
        assert_eq!(write.level, Level::TRACE);
        assert_eq!(write.field("kind"), Some("write"));
        let write_if = one("write_if");
        assert_eq!(write_if.target, "tinio::storage");
        assert_eq!(write_if.level, Level::TRACE);
        assert_eq!(write_if.field("kind"), Some("write_if"));
        let compact = one("evaluate_compact");
        assert_eq!(compact.target, "tinio::storage");
        assert_eq!(compact.level, Level::INFO);
        assert_eq!(compact.field("threshold_percent"), Some("20"));
        drop(dir);
    }

    /// The degraded empty view opens one info span and names the reason: a
    /// read-only run that falls back to it must not look like an empty store.
    #[test]
    fn the_degraded_empty_view_records_the_reason() {
        let capture = capture_spans();
        let watermark = capture.watermark();
        let _driver = Driver::empty().unwrap();

        let spans = capture.spans_since(watermark);
        let empty = spans
            .iter()
            .find(|span| span.name == "empty")
            .unwrap_or_else(|| panic!("no empty span in {spans:?}"));
        assert_eq!(empty.target, "tinio::storage");
        assert_eq!(empty.level, Level::INFO);
        assert_eq!(empty.field("kind"), Some("degraded"));
        assert_eq!(empty.field("reason"), Some("state database unavailable"));
    }

    /// A driver whose database was written out-of-band reports the stored
    /// rows (the read arm of [`Driver::open`]'s own state dir).
    #[tokio::test]
    async fn open_serves_rows_written_by_a_previous_run() {
        let dir = tempfile::tempdir().unwrap();
        {
            let opened = open::open(dir.path()).unwrap();
            let driver = Driver::new(opened.db);
            let name = name("carried").unwrap();
            driver
                .write(move |txn| -> Result<(), Error> {
                    bucket::Table::open(txn)?.put(&name, UNIX_EPOCH)?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        let driver = Driver::open(dir.path()).unwrap();
        let name = name("carried").unwrap();
        let got = driver
            .read(|txn| -> Result<Option<SystemTime>, Error> {
                bucket::Table::open_readonly(txn)?.get(&name)
            })
            .unwrap();
        assert_eq!(got, Some(UNIX_EPOCH));
        drop(dir);
    }
}
