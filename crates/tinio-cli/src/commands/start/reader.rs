//! The metrics reader the serving half owns: one task that refreshes the cheap families and runs
//! the storage scan on one `ttl`, off every request's path.
//!
//! Kept out of [`super`] so the start sequence stays orchestration-only (decision 23).

use std::{sync::Arc, time::Duration};

use tokio::{
    runtime::Handle,
    sync::watch,
    task::{JoinHandle, spawn_blocking},
    time::sleep,
};

use crate::{
    _api::metrics::DEFAULT_TTL,
    _core::{
        bucket::Name,
        pipeline::Runner,
        storage::{ListBucketsParams, ListObjectsParams, Storage, WriteLockSnapshot},
    },
    _server::{
        MetricsRefresh,
        metrics::{self, STORAGE_BUCKETS, STORAGE_BYTES, STORAGE_OBJECTS},
    },
    resolve::Pipelines,
};

/// The CLI's registry side: the one task that keeps the families a `/metrics` scrape gathers
/// fresh, and the only thing in the process that touches the served backend for them.
///
/// `tinio-api` cannot read the storage and must not depend on `tinio-server` (design decision 23:
/// the edge is the reverse of `tinio-server`'s optional `tinio-api` dependency, and cargo rejects
/// the cycle at resolution time), so `/metrics` is a gather of the process registry and this task
/// is what fills it — here, in the only crate that already depends on both.
///
/// **One cycle, both halves.** A tick republishes the cheap families ([`refresh_handle`]: the two
/// pipelines' gauges and the write-lock histograms) and then runs the storage scan
/// ([`ServerMetrics::counts`]), so a scrape's numbers are all as old as the same tick: the
/// freshness change from per-scrape to per-`ttl` (`research.md` §9). The cadence is
/// [`DEFAULT_TTL`] and the task is single-flight by construction — one task, scanning
/// sequentially — so a scrape flood cannot multiply scans, and the serving path never waits for
/// one.
///
/// **Generic over the served backend** (the backend-generic serving seam): the scan is the
/// contract's own walk, so this adapter reads any `Storage`, and the one backend-specific family
/// — the write-lock histograms — reaches it through the provider the caller supplies.
pub(super) struct ServerMetrics<S> {
    /// The cheap families — pipeline gauges and write-lock histograms — as the type
    /// `tinio_server::MetricsRefresh` (`Arc<dyn Fn() + Send + Sync>`), re-exported out of
    /// `tinio-server` because its `data` module is private.
    refresh: MetricsRefresh,
    /// The scan's subject, held as the **concrete** type: `tinio_core`'s `storage::Storage` is
    /// deliberately dyn-incompatible (its associated error type and its `where Self: Storage`
    /// bounds), so `dyn Storage` is never spelled in this crate — [`ServerMetrics::counts`]
    /// is what reads it, and this field is why the read is possible at all.
    storage: Arc<S>,
}

impl<S> ServerMetrics<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub(super) fn new<W>(pipelines: &Pipelines, storage: S, write_lock: W) -> Self
    where
        W: Fn() -> Option<WriteLockSnapshot> + Send + Sync + 'static,
    {
        let storage = Arc::new(storage);
        Self {
            refresh: refresh_handle(pipelines, write_lock),
            storage,
        }
    }

    /// Starts the one task that runs both halves: once immediately (so the first scrape is not
    /// empty), then every `ttl`, until `stop` flips.
    ///
    /// **Off the serving path, and never per scrape.** The scan is synchronous and can take
    /// seconds on a large root, so both halves run on the **blocking pool**
    /// ([`spawn_blocking`]): called inline they would occupy a serving runtime worker for the
    /// scan's whole duration, once per `ttl` and therefore permanently, while `/stop`,
    /// `/status` and the data plane share that pool. The plane's handler reads only what the last
    /// tick published, so a stalled scan cannot fail a request. A blocked task that does not
    /// complete (a panic inside the scan) is logged and the loop keeps its cadence.
    ///
    /// **Holding the returned handle is the serving half's choice**; a dropped `JoinHandle` only
    /// detaches the task, so what ends this loop is the channel, not the handle's drop.
    pub(super) fn spawn_reader(
        self: &Arc<Self>,
        ttl: Duration,
        mut stop: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let metrics = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let tick = Arc::clone(&metrics);
                if spawn_blocking(move || {
                    (tick.refresh)();
                    tick.counts();
                })
                .await
                .is_err()
                {
                    tracing::warn!("the metrics refresh task did not complete");
                }
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = sleep(ttl) => {}
                }
            }
        })
    }

    /// The storage scan, and the only place these numbers exist.
    ///
    /// Called on [`ServerMetrics::spawn_reader`]'s tick and by no request, so its cost — a full
    /// listing pass — is paid on the blocking pool once per ttl rather than by whoever scrapes.
    fn counts(&self) {
        let counts = count_storage(self.storage.as_ref());
        STORAGE_BUCKETS.set(i64::try_from(counts.buckets).unwrap_or(i64::MAX));
        STORAGE_OBJECTS.set(i64::try_from(counts.objects).unwrap_or(i64::MAX));
        STORAGE_BYTES.set(i64::try_from(counts.bytes).unwrap_or(i64::MAX));
    }
}

/// Starts the reader over `storage` at [`DEFAULT_TTL`] — **the serving half's one call**, so the
/// construction, the cadence and the task's shape stay in this module rather than at the
/// composition root.
pub(super) fn spawn<S, W>(
    pipelines: &Pipelines,
    storage: S,
    write_lock: W,
    stop: watch::Receiver<bool>,
) -> JoinHandle<()>
where
    S: Storage + Send + Sync + 'static,
    W: Fn() -> Option<WriteLockSnapshot> + Send + Sync + 'static,
{
    Arc::new(ServerMetrics::new(pipelines, storage, write_lock)).spawn_reader(DEFAULT_TTL, stop)
}

/// The cheap families' closure, built once with the adapter.
///
/// The two pipelines' `.stats()` snapshots are taken **at call time**, not at start-up, and
/// the storage's write-lock snapshot (when the backend has one) is taken through the caller's
/// provider.
pub(super) fn refresh_handle<W>(pipelines: &Pipelines, write_lock: W) -> MetricsRefresh
where
    W: Fn() -> Option<WriteLockSnapshot> + Send + Sync + 'static,
{
    let (io, remove, db) = (pipelines.io(), pipelines.remove(), pipelines.db());
    Arc::new(move || {
        metrics::refresh(
            io.stats(),
            remove.stats(),
            db.stats(),
            write_lock().unwrap_or_default(),
        )
    })
}

/// The three numbers one scan publishes.
#[derive(Debug, Default)]
pub(super) struct Counts {
    /// Buckets the listing pass saw.
    buckets: u64,
    /// Objects across every bucket.
    pub(super) objects: u64,
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
/// This function is synchronous, and its only caller is the reader task, which runs it on the
/// **blocking pool** (`ServerMetrics::spawn_reader`). The contract's listing is async, so it is
/// driven here by the runtime handle that is in scope on that thread — and a run with no runtime
/// handle (no plausible caller today) leaves the gauges at their last values and says so, rather
/// than panicking in a library.
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
/// **Objects are read through [`Storage::metrics_scan`], never the client listing** (T26). A
/// client listing is allowed to flush the tier layer's batch buffer first — a reader would then
/// publish a key that is still buffered and write the buffer at every tick — while the metrics
/// scan is the contract's "count what is stored without side effects" entry: the tier layer takes
/// its merge directly and flushes nothing. On the filesystem and pg backends the default is the
/// ordinary listing, so this one call site serves every backend.
///
/// A failed page is warned and ends the walk with what it has counted so far — a scrape's
/// gauges are an observation, and a half-counted root is a better answer than a panic or a
/// stale one. A truncated page with no continuation marker ends the walk too, rather than
/// looping on the same marker forever.
pub(super) async fn scan<S: Storage>(storage: &S) -> Counts {
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
            .metrics_scan(ListObjectsParams {
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

#[cfg(test)]
mod tests {
    use tinio_mem::MemoryStorage;
    use tinio_server::pipeline::Config;
    use tokio::time::{Instant, sleep, timeout};

    use super::*;
    use crate::_core::{acl::Acl, bucket, storage::BucketOps};

    #[tokio::test]
    async fn the_reader_ticks_the_scan_and_stops_on_the_channel() {
        let pipelines = Pipelines::build(&Config::default()).expect("default pipeline config");
        let storage = MemoryStorage::new().expect("mem storage");
        storage
            .create_bucket(
                &bucket::name("reader-tick").expect("bucket name"),
                None,
                &Acl::default_private(None),
            )
            .await
            .expect("bucket");
        let metrics = Arc::new(ServerMetrics::new(&pipelines, storage, || None));
        let (stop, stopped) = watch::channel(false);
        let reader = metrics.spawn_reader(Duration::from_millis(20), stopped);

        let deadline = Instant::now() + Duration::from_secs(2);
        while STORAGE_BUCKETS.get() == 0 {
            assert!(
                Instant::now() < deadline,
                "the reader never ticked the storage scan"
            );
            sleep(Duration::from_millis(10)).await;
        }

        stop.send(true).expect("the reader is listening");
        timeout(Duration::from_secs(2), reader)
            .await
            .expect("the reader stops on the channel")
            .expect("the reader does not panic");
    }
}
