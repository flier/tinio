//! `GET /metrics`: the injected metric source, the registry gather, and the reader task that
//! runs the expensive half off the serving path.
//!
//! The HTTP and S3-operation families are recorded by the data plane as requests happen. The
//! rest live in the registry `tinio-server` owns, and `tinio-api` must not depend on that
//! crate — the edge would be a **dependency cycle** (cargo rejects it at resolution time, even
//! with `tinio-server`'s optional `tinio-api` feature off). So the registry side is *injected*:
//! [`MetricsSource`] is what the caller implements — the CLI, which already depends on both
//! crates — and this module only drives it, splitting the work the way its cost splits.
//!
//! [`MetricsSource::refresh`] (the pipeline gauges and the write-lock histograms) is cheap and
//! runs on every scrape, from the handler. [`MetricsSource::counts`] (the storage scan and the
//! three gauges it sets) is a full listing pass over the served root, so **no request ever runs
//! it**: [`Gauges::spawn_reader`] is a single background task that scans once at start and then
//! once per `ttl` — single-flight by construction, because there is one task and it scans
//! sequentially — and the handler only gathers the registry the scan has already filled. That
//! is what bounds the scrape's cost at the design ceiling regardless of how large the root is
//! or how often the endpoint is hit, and why `/metrics` cannot park a runtime worker the way an
//! inline scan would (`FR-019`, `SC-008`; a scrape-triggered scan would also be a small denial
//! of service on every runtime worker, since `/metrics` is unauthenticated on the local channel
//! and on a loopback bind). **No number crosses this boundary**: the three counts would have to
//! be written into `tinio-server`'s statics, which only the caller can name, and they cannot be
//! computed in this crate either — it has no storage to scan. This supersedes decision 23's
//! "gating `counts()` behind the TTL" and §3's "30 s TTL cache" phrasing: the TTL is the
//! reader's cadence, there is no cache, and the design's two sentences are corrected in the
//! same commit.

use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::Arc,
    time::Duration,
};

use prometheus::{Encoder, TextEncoder, default_registry};
use tokio::{
    sync::watch,
    task::{JoinHandle, spawn_blocking},
    time::sleep,
};

use crate::Error;

/// The registry side of a scrape, supplied by whoever builds the plane (design decision 23).
///
/// The CLI implements it, and it is the only crate that can: `refresh` calls
/// `tinio_server::metrics::refresh(io, db, write_lock)` — two `tinio_core::pipeline::Stats`
/// snapshots and one `tinio_server::metrics::WriteLockStats` — and `counts` scans the caller's
/// own backend and publishes the result with `IntGauge::set` on `STORAGE_BUCKETS`,
/// `STORAGE_OBJECTS` and `STORAGE_BYTES`, the three public statics only `tinio-server` can
/// name. Nothing in this crate names `tinio-server`, and adding that dependency is
/// **forbidden** — it is the reverse edge of `tinio-server`'s optional `tinio-api` dependency
/// and cargo refuses the cycle.
///
/// Neither method returns anything, and that is the shape of the seam rather than an
/// oversight: every number a scrape needs is written into the registry the caller already
/// holds, and a copy handed back here could not be used by anything on this side.
///
/// **`refresh` is called from a request; `counts` never is.** The reader task owns the second
/// one, so an implementation may block for as long as the scan takes without any request
/// paying for it — and an implementation that needs blocking or async I/O is still free to
/// schedule it (`tokio::task::spawn_blocking`, or the caller's own machinery), because nothing
/// on a request's path waits for the answer.
///
/// # Examples
///
/// ```rust
/// use std::sync::{
///     Arc,
///     atomic::{AtomicUsize, Ordering},
/// };
///
/// use tinio_api::metrics::MetricsSource;
///
/// /// The caller's adapter in miniature: `refresh` republishes the cheap families, `counts`
/// /// runs the storage scan and sets the three storage gauges.
/// #[derive(Default)]
/// struct Adapter {
///     refreshes: Arc<AtomicUsize>,
///     counts: Arc<AtomicUsize>,
/// }
///
/// impl MetricsSource for Adapter {
///     fn refresh(&self) {
///         self.refreshes.fetch_add(1, Ordering::SeqCst);
///     }
///
///     fn counts(&self) {
///         self.counts.fetch_add(1, Ordering::SeqCst);
///     }
/// }
///
/// let adapter = Adapter::default();
/// adapter.refresh();
/// adapter.counts();
/// assert_eq!(adapter.refreshes.load(Ordering::SeqCst), 1);
/// assert_eq!(adapter.counts.load(Ordering::SeqCst), 1);
/// ```
pub trait MetricsSource: Send + Sync {
    /// Cheap, and runs on every scrape: the pipeline gauges and the write-lock histograms.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_api::metrics::MetricsSource;
    ///
    /// struct Source;
    ///
    /// impl MetricsSource for Source {
    ///     fn refresh(&self) {}
    ///
    ///     fn counts(&self) {}
    /// }
    ///
    /// // What a scrape calls, once per request: the cheap half alone.
    /// Source.refresh();
    /// ```
    fn refresh(&self);
    /// Expensive, and run by [`Gauges`]'s reader task once per ttl — never from a scrape: the
    /// storage scan and the three gauges it sets.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::{
    ///     Arc,
    ///     atomic::{AtomicUsize, Ordering},
    /// };
    ///
    /// use tinio_api::metrics::MetricsSource;
    ///
    /// struct Source(Arc<AtomicUsize>);
    ///
    /// impl MetricsSource for Source {
    ///     fn refresh(&self) {}
    ///
    ///     fn counts(&self) {
    ///         self.0.fetch_add(1, Ordering::SeqCst);
    ///     }
    /// }
    ///
    /// let scans = Arc::new(AtomicUsize::new(0));
    /// // Called by the reader task, never by a request.
    /// Source(scans.clone()).counts();
    /// assert_eq!(scans.load(Ordering::SeqCst), 1);
    /// ```
    fn counts(&self);
}

/// The default ttl of the storage-layer scan: how often the reader task rescans.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_api::metrics::DEFAULT_TTL;
///
/// assert_eq!(DEFAULT_TTL, Duration::from_secs(30));
/// ```
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

/// The registry's injected side, and the reader task that keeps it fresh.
///
/// There is no cache behind [`Gauges::refresh`]: the cadence lives in one place — the reader's
/// `ttl` — and the reader is a single task, so "one scan at a time" is a property of the shape
/// rather than of a lock or a timestamp comparison.
///
/// # Examples
///
/// ```rust
/// use std::{sync::Arc, time::Duration};
///
/// use tinio_api::metrics::{Gauges, MetricsSource};
///
/// struct Source;
///
/// impl MetricsSource for Source {
///     fn refresh(&self) {}
///
///     fn counts(&self) {}
/// }
///
/// let gauges = Gauges::new(Arc::new(Source), Duration::from_secs(30));
/// // The source is opaque here on purpose: a scrape drives it, it never reads it back.
/// let printed = format!("{gauges:?}");
/// assert!(printed.contains("<injected>"), "{printed}");
/// assert!(printed.contains("30s"), "{printed}");
/// ```
#[derive(Clone)]
pub struct Gauges {
    source: Arc<dyn MetricsSource>,
    ttl: Duration,
}

impl Debug for Gauges {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Gauges")
            .field("source", &"<injected>")
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl Gauges {
    /// Builds the scrape-time source over the caller's adapter; `ttl` is how often the reader
    /// task runs that adapter's expensive half — the storage scan.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_api::metrics::{DEFAULT_TTL, Gauges, MetricsSource};
    ///
    /// struct Source;
    ///
    /// impl MetricsSource for Source {
    ///     fn refresh(&self) {}
    ///
    ///     fn counts(&self) {}
    /// }
    ///
    /// let gauges = Gauges::new(Arc::new(Source), DEFAULT_TTL);
    /// assert!(format!("{gauges:?}").contains("<injected>"));
    /// ```
    pub fn new(source: Arc<dyn MetricsSource>, ttl: Duration) -> Self {
        Self { source, ttl }
    }

    /// Refreshes the cheap half. **This is the only method a scrape calls**, and it never
    /// scans: the storage gauges are whatever the reader task last published.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::{
    ///     Arc,
    ///     atomic::{AtomicUsize, Ordering},
    /// };
    ///
    /// use tinio_api::metrics::{DEFAULT_TTL, Gauges, MetricsSource};
    ///
    /// struct Source(Arc<AtomicUsize>);
    ///
    /// impl MetricsSource for Source {
    ///     fn refresh(&self) {
    ///         self.0.fetch_add(1, Ordering::SeqCst);
    ///     }
    ///
    ///     fn counts(&self) {}
    /// }
    ///
    /// let refreshes = Arc::new(AtomicUsize::new(0));
    /// let gauges = Gauges::new(Arc::new(Source(refreshes.clone())), DEFAULT_TTL);
    /// gauges.refresh();
    /// assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    /// ```
    pub fn refresh(&self) {
        self.source.refresh();
    }

    /// Starts the one task that runs the expensive half: once immediately (so the first scrape
    /// is not empty), then every `ttl`, until `stop` flips. Single-flight by construction —
    /// one task, scanning sequentially — so a scrape flood cannot multiply scans, and the task
    /// exits with the plane's own shutdown channel rather than being aborted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_api::metrics::{Gauges, MetricsSource};
    /// use tokio::sync::watch;
    ///
    /// struct Source;
    ///
    /// impl MetricsSource for Source {
    ///     fn refresh(&self) {}
    ///
    ///     fn counts(&self) {}
    /// }
    ///
    /// # async fn run() {
    /// let (tx, rx) = watch::channel(false);
    /// let gauges = Arc::new(Gauges::new(Arc::new(Source), Duration::from_millis(20)));
    /// // One task, scanning once at start and then once per ttl, until the plane stops.
    /// let reader = gauges.spawn_reader(rx);
    /// let _ = tx.send(true);
    /// reader.await.expect("the reader exits on the channel");
    /// # }
    /// ```
    pub fn spawn_reader(self: &Arc<Self>, mut stop: watch::Receiver<bool>) -> JoinHandle<()> {
        let gauges = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                // The scan is synchronous and can take seconds on a large root, so it runs on
                // the **blocking pool**: called inline it would occupy one worker of the serving
                // runtime for the scan's whole duration, once per `ttl` and therefore
                // permanently, while `/stop`, `/status` and the data plane share that pool. A
                // blocking task that fails to complete (a panic inside the scan) is logged and
                // the loop keeps its cadence rather than exiting with it.
                let scan = Arc::clone(&gauges);
                if spawn_blocking(move || scan.source.counts()).await.is_err() {
                    tracing::warn!("the metrics scan task did not complete");
                }
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = sleep(gauges.ttl) => {}
                }
            }
        })
    }

    /// Encodes the registry in the Prometheus text format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_api::metrics::{Gauges, MetricsSource};
    ///
    /// struct Source;
    ///
    /// impl MetricsSource for Source {
    ///     fn refresh(&self) {}
    ///
    ///     fn counts(&self) {}
    /// }
    ///
    /// let gauges = Gauges::new(Arc::new(Source), Duration::from_secs(30));
    /// // The default registry encodes; a registry with no samples yet is an empty body.
    /// let text = gauges.render().expect("the registry encodes");
    /// assert!(text.is_empty() || text.ends_with('\n'), "{text}");
    /// ```
    pub fn render(&self) -> Result<String, Error> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let families = default_registry().gather();
        encoder
            .encode(&families, &mut buffer)
            .map_err(|err| Error::Internal(format!("metrics encoding: {err}")))?;
        String::from_utf8(buffer).map_err(|err| Error::Internal(format!("metrics encoding: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    use super::*;
    use crate::test_support::Counting;

    #[tokio::test]
    async fn the_scan_runs_once_per_ttl_on_the_reader_task_and_never_from_a_scrape() {
        // The *instance's* ttl, not `DEFAULT_TTL`: construct the gauges with 20 ms and the
        // cadence is observable in a test rather than in half a minute. The reader scans once
        // at start so the first scrape is not empty, then once per ttl. (The first assertion is
        // after the sleep rather than immediately after `spawn_reader`: a spawned task is not
        // guaranteed to have been polled yet, and a test that assumes otherwise is flaky.)
        let source = Arc::new(Counting::default());
        let gauges = Arc::new(Gauges::new(source.clone(), Duration::from_millis(20)));
        let (tx, rx) = watch::channel(false);
        let reader = gauges.spawn_reader(rx);

        sleep(Duration::from_millis(60)).await;
        let scanned = source.scans();
        assert!(
            scanned >= 2,
            "the reader scanned at start and kept scanning on its ttl: {scanned}"
        );

        // The regression this task exists for: any number of scrapes, and the scan does not
        // run at all — a `/metrics` flood cannot park a runtime worker on a storage scan.
        for _ in 0..8 {
            gauges.refresh();
        }
        assert_eq!(source.scans(), scanned, "no scrape scans");

        tx.send(true).expect("the reader is listening");
        reader.await.expect("the reader exits on the channel");
    }

    #[tokio::test]
    async fn the_cheap_half_refreshes_on_every_scrape() {
        let source = Arc::new(Counting::default());
        let gauges = Gauges::new(source.clone(), Duration::from_secs(30));

        gauges.refresh();
        gauges.refresh();
        assert_eq!(
            source.refreshes(),
            2,
            "cheap families refresh on every scrape"
        );
    }
}
