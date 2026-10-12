//! Storage-scan metrics adapter injected into the management plane (`/metrics`).
//!
//! Kept out of [`super`] so the start sequence stays orchestration-only (decision 23).

use std::sync::Arc;

use tokio::runtime::Handle;

#[cfg(feature = "pg")]
use crate::_pg::PgStorage;
use crate::{
    _api::metrics::MetricsSource,
    _core::{
        bucket::Name,
        pipeline::Runner,
        storage::{ListBucketsParams, ListObjectsParams, Storage},
    },
    _fs::{FsStorage, database::WriteLockSnapshot},
    _server::{
        MetricsRefresh,
        metrics::{self, STORAGE_BUCKETS, STORAGE_BYTES, STORAGE_OBJECTS, WriteLockStats},
    },
    resolve::Pipelines,
};

/// The write-lock snapshot a backend publishes to `/metrics`, when it has one.
///
/// **The one backend-specific bit of this adapter**, and the reason it is a trait rather than a
/// second `ServerMetrics`: the scan ([`count_storage`]) is the contract's generic walk and is
/// written once, while the write-lock histograms are the filesystem's own lane — a backend whose
/// writes carry no such lane answers `None`, and the gauges publish zeros rather than a fiction.
/// The fs arm is `Some` because `FsStorage` records a write lock per transaction; pg's is `None`
/// because its writes are the store's rows and hand out no wait-duration distribution.
pub(super) trait WriteLockStatsSource {
    /// The snapshot, or `None` for a backend with no filesystem write lane.
    fn write_lock_stats(&self) -> Option<WriteLockStats>;
}

impl WriteLockStatsSource for FsStorage {
    fn write_lock_stats(&self) -> Option<WriteLockStats> {
        Some(write_lock_snapshot(FsStorage::write_lock_stats(self)))
    }
}

#[cfg(feature = "pg")]
impl WriteLockStatsSource for PgStorage {
    fn write_lock_stats(&self) -> Option<WriteLockStats> {
        None
    }
}

/// The CLI's `tinio_api::metrics::MetricsSource` — the registry side of a `/metrics` scrape.
///
/// `tinio-api` must not depend on `tinio-server` (design decision 23: the edge is the reverse
/// of `tinio-server`'s optional `tinio-api` dependency, and cargo rejects the cycle at
/// resolution time), so the source is **injected** and this crate — the only one that already
/// depends on both — is what injects it.
///
/// **Two methods, and the cadence is the plane's.** `refresh` is the cheap families, called on
/// every scrape through `Gauges::refresh`; `counts` is the storage scan and the plane's reader
/// is its only caller, which is what retires the TTL cache decision 23 describes: there is no
/// staged copy in this crate and nothing to keep in step, because the reader's own schedule
/// *is* the schedule.
///
/// **Generic over the served backend** (the backend-generic serving seam): the scan is the
/// contract's own walk, so this adapter reads any `Storage`, and the one backend-specific family
/// — the write-lock histograms — travels through [`WriteLockStatsSource`].
pub(super) struct ServerMetrics<S> {
    /// The cheap families — pipeline gauges and write-lock histograms — as the type
    /// `tinio_server::MetricsRefresh` (`Arc<dyn Fn() + Send + Sync>`), re-exported out of
    /// `tinio-server` because its `data` module is private.
    refresh: MetricsRefresh,
    /// The scan's subject, held as the **concrete** type: `tinio_core`'s `storage::Storage` is
    /// deliberately dyn-incompatible (its associated error type and its `where Self: Storage`
    /// bounds), so `dyn Storage` is never spelled in this crate — [`counts`](ServerMetrics)
    /// is what reads it, and this field is why the read is possible at all.
    storage: S,
}

impl<S> ServerMetrics<S>
where
    S: Storage + WriteLockStatsSource + Clone + Send + Sync + 'static,
{
    pub(super) fn new(pipelines: &Pipelines, storage: &S) -> Self {
        Self {
            refresh: refresh_handle(pipelines, storage),
            storage: storage.clone(),
        }
    }
}

/// The cheap families' closure, built once in the pre-runtime half and shared with the adapter.
///
/// The two pipelines' `.stats()` snapshots are taken **at call time**, not at start-up, and
/// the storage's write-lock snapshot (when the backend has one) is converted to
/// `WriteLockStats` by [`write_lock_snapshot`].
pub(super) fn refresh_handle<S>(pipelines: &Pipelines, storage: &S) -> MetricsRefresh
where
    S: WriteLockStatsSource + Clone + Send + Sync + 'static,
{
    let (io, db, storage) = (pipelines.io(), pipelines.db(), storage.clone());
    Arc::new(move || {
        metrics::refresh(
            io.stats(),
            db.stats(),
            storage.write_lock_stats().unwrap_or_default(),
        )
    })
}

/// The write-lock snapshot of the fs storage into the metric layer's plain-data form — the
/// wiring-point conversion that keeps `tinio-server`'s metrics decoupled from backend
/// snapshot types.
fn write_lock_snapshot(snapshot: WriteLockSnapshot) -> WriteLockStats {
    WriteLockStats {
        wait_buckets: snapshot.wait_buckets,
        total_buckets: snapshot.total_buckets,
        count: snapshot.count,
        wait_sum_us: snapshot.wait_sum_us,
        wait_max_us: snapshot.wait_max_us,
        total_sum_us: snapshot.total_sum_us,
        total_max_us: snapshot.total_max_us,
    }
}

/// The three numbers one scan publishes.
#[derive(Debug, Default)]
struct Counts {
    /// Buckets the listing pass saw.
    buckets: u64,
    /// Objects across every bucket.
    objects: u64,
    /// Their total size in bytes.
    bytes: u64,
}

/// One page's request size, for both listings: the walk is paged rather than unbounded, so a
/// large root costs a bounded amount of memory at a time.
const SCAN_PAGE: usize = 1000;

/// The full listing pass — the one expensive thing in this module — as plain data.
///
/// `S: Storage` is the contract's generic form, and it is the only form that exists:
/// `dyn Storage` cannot be written (an associated error type plus the `where Self: Storage`
/// category bounds). The walk never crosses a crate boundary — the numbers travel from here
/// into `tinio-server`'s three public gauge statics, which are the only place they can go,
/// because `tinio-api` cannot name that crate (decision 23).
///
/// The trait method this serves is synchronous, and its only caller is the plane's reader task,
/// which awaits it on the **blocking pool** (`Gauges::spawn_reader`). The contract's listing is
/// async, so it is driven here by the runtime handle that is in scope on that thread — and a
/// run with no runtime handle (no plausible caller today) leaves the gauges at their last
/// values and says so, rather than panicking in a library.
fn count_storage<S: Storage>(storage: &S) -> Counts {
    let Ok(runtime) = Handle::try_current() else {
        tracing::warn!(
            "the storage scan needs the serving runtime; the gauges keep their last values"
        );
        return Counts::default();
    };
    runtime.block_on(scan(storage))
}

/// The walk itself: every bucket, then every page of its objects.
///
/// A failed page is warned and ends the walk with what it has counted so far — a scrape's
/// gauges are an observation, and a half-counted root is a better answer than a panic or a
/// stale one. A truncated page with no continuation marker ends the walk too, rather than
/// looping on the same marker forever.
async fn scan<S: Storage>(storage: &S) -> Counts {
    let mut counts = Counts::default();
    let mut start_after = None;
    loop {
        let page = match storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after,
                    max_buckets: SCAN_PAGE,
                },
                None,
                None,
            )
            .await
        {
            Ok(page) => page,
            Err(err) => {
                tracing::warn!(error = %err, "the storage scan could not list buckets");
                return counts;
            }
        };
        for entry in &page.buckets {
            counts.buckets += 1;
            scan_objects(storage, &entry.name, &mut counts).await;
        }
        if !page.truncated {
            return counts;
        }
        match page.next_start_after {
            Some(marker) => start_after = Some(marker),
            None => return counts,
        }
    }
}

/// One bucket's objects, page by page.
async fn scan_objects<S: Storage>(storage: &S, name: &Name, counts: &mut Counts) {
    let mut start_after = None;
    loop {
        let page = match storage
            .list_objects(ListObjectsParams {
                bucket: name.clone(),
                prefix: String::new(),
                delimiter: None,
                start_after,
                max_keys: SCAN_PAGE,
            })
            .await
        {
            Ok(page) => page,
            Err(err) => {
                tracing::warn!(error = %err, bucket = %name, "the storage scan could not list a bucket");
                return;
            }
        };
        for object in &page.objects {
            counts.objects += 1;
            counts.bytes += object.size;
        }
        if !page.truncated {
            return;
        }
        match page.next_start_after {
            Some(marker) => start_after = Some(marker),
            None => return,
        }
    }
}

impl<S> MetricsSource for ServerMetrics<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// The cheap families — the call `Gauges::refresh` makes on every scrape, and the same arc
    /// the data plane's retired hook held, so the two cannot drift.
    fn refresh(&self) {
        (self.refresh)();
    }

    /// The storage scan, and the only place these numbers exist.
    ///
    /// `refresh` is called from a request; this is called by the reader task and by no request,
    /// so its cost — a full listing pass — is paid on the blocking pool once per ttl rather
    /// than by whoever scrapes.
    fn counts(&self) {
        let counts = count_storage(&self.storage);
        STORAGE_BUCKETS.set(i64::try_from(counts.buckets).unwrap_or(i64::MAX));
        STORAGE_OBJECTS.set(i64::try_from(counts.objects).unwrap_or(i64::MAX));
        STORAGE_BYTES.set(i64::try_from(counts.bytes).unwrap_or(i64::MAX));
    }
}
