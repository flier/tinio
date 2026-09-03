//! Prometheus registry and metric families (task T023).
//!
//! The metric layers of the data model (data-model.md Metrics): HTTP
//! (`tinio_http_*`), S3 operations (`tinio_s3_*`), storage
//! (`tinio_storage_*`), the pipeline gauges (`tinio_pipeline_*`), and the
//! write-lock duration histograms (`tinio_write_lock_*`, pipeline-spec.md
//! §4). Families are process-wide globals, registered once on the default
//! registry via `register_*!`. The storage-layer full-scan gauges are
//! computed (with a 30 s TTL cache) by the management plane later (T075);
//! the pipeline gauges are refreshed from the runtimes' [`Stats`]
//! snapshots on scrape, and the write-lock histograms are converted from
//! a plain-data [`WriteLockStats`] (the backend snapshot conversion lives
//! at the wiring point — `tinio-server` never imports a backend crate) —
//! cheap atomic snapshots, no TTL cache (that pattern belongs to the
//! storage full-scan gauges only).
//!
//! # Examples
//!
//! ```rust
//! use std::time::Duration;
//!
//! use tinio_server::metrics::{STORAGE_BUCKETS, record_http_request, record_s3_operation};
//!
//! record_http_request("GET", 200, Duration::from_millis(3));
//! record_s3_operation("GetObject", 200, Duration::from_millis(5));
//! STORAGE_BUCKETS.set(2);
//! assert!(!prometheus::default_registry().gather().is_empty());
//! ```

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use lazy_static::lazy_static;
use prometheus::{
    HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Result as PromResult,
    core::{Collector, Desc},
    proto::{Bucket, Histogram, Metric, MetricFamily, MetricType},
    register, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec,
};
use s3s::{S3, S3Request, S3Response, S3Result, dto};

use crate::_core::{
    pipeline::Stats,
    storage::{WRITE_LOCK_BUCKET_BOUNDS_US, WRITE_LOCK_BUCKETS},
};

/// The write-lock distribution snapshot as **plain data** — the metric
/// layer is decoupled from any backend's snapshot type: the wiring point
/// (serve.rs) converts the tinio-fs `WriteLockSnapshot` into this
/// value, so `tinio-server` never imports a backend crate. The bucket
/// bounds are the shared tinio-core constants (positional with the
/// fs backend's bucketing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteLockStats {
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

lazy_static! {
    /// HTTP requests served by the management plane, by method and status.
    pub static ref HTTP_REQUESTS: IntCounterVec = register_int_counter_vec!(
        "tinio_http_requests_total",
        "HTTP requests served by the management plane, by method and status",
        &["method", "status"]
    )
    .expect("register tinio_http_requests_total");
    /// HTTP request durations, by method.
    pub static ref HTTP_DURATION: HistogramVec = register_histogram_vec!(
        "tinio_http_request_duration_seconds",
        "HTTP request durations, by method",
        &["method"]
    )
    .expect("register tinio_http_request_duration_seconds");
    /// HTTP requests currently being served.
    pub static ref HTTP_IN_FLIGHT: IntGauge = register_int_gauge!(
        "tinio_http_in_flight",
        "HTTP requests currently being served"
    )
    .expect("register tinio_http_in_flight");
    /// S3 data-plane operations, by operation and status.
    pub static ref S3_OPERATIONS: IntCounterVec = register_int_counter_vec!(
        "tinio_s3_operations_total",
        "S3 data-plane operations, by operation and status",
        &["op", "status"]
    )
    .expect("register tinio_s3_operations_total");
    /// S3 operation durations, by operation.
    pub static ref S3_DURATION: HistogramVec = register_histogram_vec!(
        "tinio_s3_operation_duration_seconds",
        "S3 operation durations, by operation",
        &["op"]
    )
    .expect("register tinio_s3_operation_duration_seconds");
    /// Bucket count (full-scan, TTL-cached 30 s; management plane, T075).
    pub static ref STORAGE_BUCKETS: IntGauge = register_int_gauge!(
        "tinio_storage_buckets_total",
        "Number of buckets (full-scan, TTL-cached 30 s)"
    )
    .expect("register tinio_storage_buckets_total");
    /// Object count (full-scan, TTL-cached 30 s; management plane, T075).
    pub static ref STORAGE_OBJECTS: IntGauge = register_int_gauge!(
        "tinio_storage_objects_total",
        "Number of objects (full-scan, TTL-cached 30 s)"
    )
    .expect("register tinio_storage_objects_total");
    /// Total object bytes (full-scan, TTL-cached 30 s; management plane, T075).
    pub static ref STORAGE_BYTES: IntGauge = register_int_gauge!(
        "tinio_storage_bytes_total",
        "Total object bytes (full-scan, TTL-cached 30 s)"
    )
    .expect("register tinio_storage_bytes_total");
    /// Bytes streamed into storage.
    pub static ref STORAGE_UPLOAD_BYTES: IntCounter = register_int_counter!(
        "tinio_storage_upload_bytes_total",
        "Bytes streamed into storage"
    )
    .expect("register tinio_storage_upload_bytes_total");
    /// Bytes streamed out of storage.
    pub static ref STORAGE_DOWNLOAD_BYTES: IntCounter = register_int_counter!(
        "tinio_storage_download_bytes_total",
        "Bytes streamed out of storage"
    )
    .expect("register tinio_storage_download_bytes_total");
    /// Objects written, by operation (put, copy, multipart-complete).
    pub static ref STORAGE_OBJECTS_UPLOADED: IntCounterVec = register_int_counter_vec!(
        "tinio_storage_objects_uploaded_total",
        "Objects written to storage, by operation",
        &["op"]
    )
    .expect("register tinio_storage_objects_uploaded_total");
    /// Objects deleted from storage.
    pub static ref STORAGE_OBJECTS_DELETED: IntCounter = register_int_counter!(
        "tinio_storage_objects_deleted_total",
        "Objects deleted from storage"
    )
    .expect("register tinio_storage_objects_deleted_total");
    /// Multipart uploads currently in progress.
    pub static ref STORAGE_MULTIPART_IN_PROGRESS: IntGauge = register_int_gauge!(
        "tinio_storage_multipart_in_progress",
        "Multipart uploads currently in progress"
    )
    .expect("register tinio_storage_multipart_in_progress");
    /// Tasks currently queued in a pipeline, by pipeline (io/db).
    pub static ref PIPELINE_QUEUE_DEPTH: IntGaugeVec = register_int_gauge_vec!(
        "tinio_pipeline_queue_depth",
        "Tasks currently queued in a pipeline, by pipeline",
        &["pipeline"]
    )
    .expect("register tinio_pipeline_queue_depth");
    /// Tasks currently executing in a pipeline, by pipeline (io/db).
    pub static ref PIPELINE_IN_FLIGHT: IntGaugeVec = register_int_gauge_vec!(
        "tinio_pipeline_in_flight",
        "Tasks currently executing in a pipeline, by pipeline",
        &["pipeline"]
    )
    .expect("register tinio_pipeline_in_flight");
    /// Workers currently busy in a pipeline, by pipeline (io/db).
    pub static ref PIPELINE_BUSY_WORKERS: IntGaugeVec = register_int_gauge_vec!(
        "tinio_pipeline_busy_workers",
        "Workers currently busy in a pipeline, by pipeline",
        &["pipeline"]
    )
    .expect("register tinio_pipeline_busy_workers");
    /// The write-lock duration histograms (pipeline-spec.md §4): the
    /// `wait` and `total` distributions of every write transaction,
    /// converted from the tinio-fs snapshot at scrape time (registered
    /// with the default registry — the registry holds its own copy of
    /// the descriptors).
    pub static ref WRITE_LOCK_HISTOGRAMS: WriteLockHistograms = {
        let histograms = WriteLockHistograms::new()
            .expect("build the write-lock histogram descriptors");
        register(Box::new(histograms.clone()))
            .expect("register the write-lock histogram families");
        histograms
    };
}

/// The metric statics are process-global (the prometheus default
/// registry): a test binary runs hundreds of tests in parallel threads,
/// every one of which records into the same statics through the data
/// plane, so an exact-value/family assertion races the other tests'
/// writes (observed flakes: the write-lock snapshot, the family set,
/// `STORAGE_DOWNLOAD_BYTES`, `HTTP_IN_FLIGHT`). The serialization is a
/// cfg(test)-only writer lock: every metric WRITE takes it, and the
/// exact-assert tests take it for their whole assert window. Production
/// call sites compile the lock away. Reentrant: an exact test holds the
/// window and does its setup THROUGH the public record/refresh fns, so a
/// writer on the same thread (one test = one thread) must not deadlock
/// on its own lock — the window marks the thread and writers skip
/// acquisition while it is held.
#[cfg(test)]
pub(crate) mod test_lock {
    use std::{
        cell::Cell,
        sync::{Mutex, MutexGuard},
    };

    static LOCK: Mutex<()> = Mutex::new(());

    thread_local! {
        static HELD: Cell<bool> = const { Cell::new(false) };
    }

    /// Writer-side guard: `Some` blocks while an exact-assert test on
    /// another thread holds the window; `None` on the window's own
    /// thread (reentrant setup).
    pub(crate) fn writer() -> Option<MutexGuard<'static, ()>> {
        if HELD.with(Cell::get) {
            None
        } else {
            Some(LOCK.lock().unwrap())
        }
    }

    /// The exact-assert window: exclusive against every other-thread
    /// writer; reentrant on this thread. The thread mark is cleared on
    /// drop — the harness reuses test threads, so a stale mark would
    /// silently disable the lock for a later test.
    pub(crate) struct Window {
        _lock: MutexGuard<'static, ()>,
    }

    impl Drop for Window {
        fn drop(&mut self) {
            HELD.with(|held| held.set(false));
        }
    }

    /// Open the window (the assert-exclusive region of an exact test).
    pub(crate) fn window() -> Window {
        HELD.with(|held| held.set(true));
        Window {
            _lock: LOCK.lock().unwrap(),
        }
    }
}

/// Take the in-flight gauge for a request (the data plane's per-request
/// inc/dec pair).
pub(crate) fn http_in_flight_inc() {
    #[cfg(test)]
    let _g = test_lock::writer();
    HTTP_IN_FLIGHT.inc();
}

/// Release the in-flight gauge (request completion or cancellation).
pub(crate) fn http_in_flight_dec() {
    #[cfg(test)]
    let _g = test_lock::writer();
    HTTP_IN_FLIGHT.dec();
}

/// Record the uploaded bytes of a completed request.
pub(crate) fn record_upload_bytes(n: u64) {
    #[cfg(test)]
    let _g = test_lock::writer();
    STORAGE_UPLOAD_BYTES.inc_by(n);
}

/// Record the downloaded bytes of a completed response body.
pub(crate) fn record_download_bytes(n: u64) {
    #[cfg(test)]
    let _g = test_lock::writer();
    STORAGE_DOWNLOAD_BYTES.inc_by(n);
}

/// Decrement the in-progress-multipart gauge, saturating at zero: after a
/// restart the persisted uploads are not counted, so completing or
/// aborting one must not drive the gauge negative.
pub(crate) fn multipart_in_progress_dec() {
    #[cfg(test)]
    let _g = test_lock::writer();
    if STORAGE_MULTIPART_IN_PROGRESS.get() > 0 {
        STORAGE_MULTIPART_IN_PROGRESS.dec();
    }
}

/// Record a completed operation against a counter/histogram family pair.
fn record(
    counter: &IntCounterVec,
    counter_labels: &[&str],
    histogram: &HistogramVec,
    histogram_label: &str,
    duration: Duration,
) {
    counter.with_label_values(counter_labels).inc();
    histogram
        .with_label_values(&[histogram_label])
        .observe(duration.as_secs_f64());
}

/// Record a completed HTTP request (management plane).
pub fn record_http_request(method: &str, status: u16, duration: Duration) {
    #[cfg(test)]
    let _g = test_lock::writer();
    record(
        &HTTP_REQUESTS,
        &[method, &status.to_string()],
        &HTTP_DURATION,
        method,
        duration,
    );
}

/// Record a completed S3 data-plane operation.
pub fn record_s3_operation(op: &str, status: u16, duration: Duration) {
    #[cfg(test)]
    let _g = test_lock::writer();
    record(
        &S3_OPERATIONS,
        &[op, &status.to_string()],
        &S3_DURATION,
        op,
        duration,
    );
}

/// Refresh every scrape-computed family from one call (F49 — the single
/// refresh entry point behind the `/metrics` endpoint, pipeline-spec.md
/// §4): the pipeline gauges from two [`Stats`] snapshots and the
/// write-lock histograms from a plain-data [`WriteLockStats`] (the
/// conversion happens at gather, never on the write path; the backend
/// snapshot → [`WriteLockStats`] conversion lives at the wiring point).
/// Touching the statics registers the families with the default registry
/// (F10 — the server's `/metrics` endpoint calls this on every scrape,
/// so a running server's registry always contains them).
pub fn refresh(io: Stats, db: Stats, write_lock: WriteLockStats) {
    #[cfg(test)]
    let _g = test_lock::writer();
    for (label, stats) in [("io", io), ("db", db)] {
        PIPELINE_QUEUE_DEPTH
            .with_label_values(&[label])
            .set(i64::try_from(stats.queue_depth).unwrap_or(i64::MAX));
        PIPELINE_IN_FLIGHT
            .with_label_values(&[label])
            .set(i64::try_from(stats.in_flight).unwrap_or(i64::MAX));
        PIPELINE_BUSY_WORKERS
            .with_label_values(&[label])
            .set(i64::try_from(stats.busy_workers).unwrap_or(i64::MAX));
    }
    WRITE_LOCK_HISTOGRAMS.refresh(write_lock);
}

/// The scrape-time conversion of the write-lock histograms
/// (pipeline-spec.md §4): two cumulative histogram families — the `wait`
/// and `total` distributions of every write transaction — filled from
/// the latest [`WriteLockStats`] on gather. Prometheus histograms
/// cannot be set from external counts, so the families are a small
/// custom [`Collector`] over the [`proto`] message types: the shared
/// tinio-core bucket bounds become cumulative `le=` buckets (µs →
/// seconds), with `_sum`/`_count` from the snapshot's count/sum (the
/// text encoder appends the `le="+Inf"` bucket from `_count`).
#[derive(Clone)]
pub struct WriteLockHistograms {
    wait_desc: Desc,
    total_desc: Desc,
    /// The latest snapshot, shared with the registered clone (the
    /// lazy_static registers a clone — the snapshot must be one Arc, not
    /// per-copy state). A cheap atomic read on the write path — no 30 s
    /// TTL cache; that pattern belongs to the storage full-scan gauges
    /// only.
    snapshot: Arc<Mutex<WriteLockStats>>,
}

impl WriteLockHistograms {
    /// Build the two family descriptors. The bucket bounds are the
    /// shared tinio-core constants — the conversion is positional by
    /// index (the metrics layer reads the buckets by index, not name).
    fn new() -> PromResult<Self> {
        let wait_desc = Desc::new(
            "tinio_write_lock_wait_duration_seconds".to_string(),
            "Write-lock wait duration of write transactions (the entry-to-begin_write interval, approximating the single-writer lock wait), cumulative histogram"
                .to_string(),
            Vec::new(),
            HashMap::new(),
        )?;
        let total_desc = Desc::new(
            "tinio_write_lock_total_duration_seconds".to_string(),
            "Total duration of write transactions (entry to commit/abort return, incl. fsync), cumulative histogram"
                .to_string(),
            Vec::new(),
            HashMap::new(),
        )?;
        Ok(Self {
            wait_desc,
            total_desc,
            snapshot: Arc::new(Mutex::new(WriteLockStats::default())),
        })
    }

    /// Store the latest snapshot for the scrape-time conversion.
    fn refresh(&self, snapshot: WriteLockStats) {
        *self.snapshot.lock().unwrap() = snapshot;
    }
}

impl Collector for WriteLockHistograms {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.wait_desc, &self.total_desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let snapshot = *self.snapshot.lock().unwrap();
        vec![
            write_lock_family(
                &self.wait_desc,
                &snapshot.wait_buckets,
                snapshot.count,
                snapshot.wait_sum_us,
            ),
            write_lock_family(
                &self.total_desc,
                &snapshot.total_buckets,
                snapshot.count,
                snapshot.total_sum_us,
            ),
        ]
    }
}

/// One write-lock distribution as a prometheus cumulative histogram:
/// per-bound cumulative buckets (µs → seconds). The open `>100k µs`
/// bucket's counts stay implicit in `_count` (the standard +Inf
/// semantics — `histogram_quantile` extrapolates past the last bound).
/// A duration exactly at a bucket bound counts in the NEXT `le=` bucket
/// (the strict `bounds[i-1] <= d < bounds[i]` bucketing of tinio-fs's
/// `write_lock_bucket`, documented with `WRITE_LOCK_BUCKET_BOUNDS_US`) —
/// correct as-is, not a fencepost to fix.
fn write_lock_family(
    desc: &Desc,
    buckets: &[u64; WRITE_LOCK_BUCKETS],
    count: u64,
    sum_us: u64,
) -> MetricFamily {
    let mut histogram = Histogram::default();
    histogram.set_sample_count(count);
    histogram.set_sample_sum(sum_us as f64 / 1_000_000.0);
    let mut cumulative = 0u64;
    let mut proto_buckets = Vec::with_capacity(WRITE_LOCK_BUCKET_BOUNDS_US.len());
    for (bucket, bound_us) in buckets.iter().zip(WRITE_LOCK_BUCKET_BOUNDS_US) {
        cumulative += bucket;
        let mut proto_bucket = Bucket::default();
        proto_bucket.set_cumulative_count(cumulative);
        proto_bucket.set_upper_bound(bound_us as f64 / 1_000_000.0);
        proto_buckets.push(proto_bucket);
    }
    histogram.set_bucket(proto_buckets);

    let mut metric = Metric::default();
    metric.set_histogram(histogram);

    let mut family = MetricFamily::default();
    family.set_name(desc.fq_name.clone());
    family.set_help(desc.help.clone());
    family.set_field_type(MetricType::HISTOGRAM);
    family.set_metric(vec![metric]);
    family
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Mutex};

    use http::{Extensions, HeaderMap, Method, Uri};
    use prometheus::{Encoder, TextEncoder, default_registry};
    #[cfg(feature = "copy")]
    use s3s::dto::CopySource;
    #[cfg(feature = "multipart")]
    use s3s::dto::{
        AbortMultipartUploadInput, CompleteMultipartUploadInput, CreateMultipartUploadInput,
    };
    use s3s::{
        dto::{
            AbortMultipartUploadOutput, CompleteMultipartUploadOutput, CreateBucketInput,
            CreateBucketOutput, CreateMultipartUploadOutput, Delete,
        },
        s3_error,
    };
    use tokio::runtime::Runtime;

    use super::*;

    /// Serializes the tests that mutate the shared, label-less
    /// `STORAGE_MULTIPART_IN_PROGRESS` gauge — parallel interleaving would
    /// clobber the exact-value asserts.
    static MULTIPART_GAUGE: Mutex<()> = Mutex::new(());
    /// Serializes the tests that refresh the shared write-lock snapshot —
    /// a parallel refresh between the refresh and the encode would clobber
    /// the exact bucket counts.
    static WRITE_LOCK_SNAPSHOT_TEST: Mutex<()> = Mutex::new(());

    #[test]
    fn registers_all_families() {
        let _window = test_lock::window();
        let _guard = MULTIPART_GAUGE.lock().unwrap();
        // gather() only emits families with samples — record one of each
        // label-bearing family first.
        record_http_request("FAM", 200, Duration::from_millis(1));
        record_s3_operation("FamGetObject", 200, Duration::from_millis(1));
        HTTP_IN_FLIGHT.set(0);
        STORAGE_BUCKETS.set(0);
        STORAGE_OBJECTS.set(0);
        STORAGE_BYTES.set(0);
        STORAGE_UPLOAD_BYTES.inc_by(0);
        STORAGE_DOWNLOAD_BYTES.inc_by(0);
        STORAGE_OBJECTS_UPLOADED.with_label_values(&["put"]).inc();
        STORAGE_OBJECTS_DELETED.inc_by(0);
        STORAGE_MULTIPART_IN_PROGRESS.set(0);
        // The write-lock histograms are always emitted (zero counts
        // before any write transaction); the refresh is also the
        // registration path (the server's `/metrics` endpoint calls it on
        // every scrape, F10). The refresh happens under the snapshot
        // guard — it writes the shared snapshot, so it must not race the
        // other snapshot test's refresh+encode (a clobbered snapshot
        // fails its exact-value asserts and poisons the guard).
        let _snapshot_guard = WRITE_LOCK_SNAPSHOT_TEST.lock().unwrap();
        // The pipeline gauges (io/db labels) sample the inline runners —
        // stats are all zeros, the family set is what matters here.
        refresh(
            Stats::default(),
            Stats::default(),
            WriteLockStats::default(),
        );
        let names: Vec<String> = default_registry()
            .gather()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        // The tinio_* family set must be exactly the 18 spec'd names
        // (data-model.md Metrics + the pipeline gauges + the write-lock
        // histograms) — a 19th family would fail this equality.
        let expected: HashSet<&str> = [
            "tinio_http_requests_total",
            "tinio_http_request_duration_seconds",
            "tinio_http_in_flight",
            "tinio_s3_operations_total",
            "tinio_s3_operation_duration_seconds",
            "tinio_storage_buckets_total",
            "tinio_storage_objects_total",
            "tinio_storage_bytes_total",
            "tinio_storage_upload_bytes_total",
            "tinio_storage_download_bytes_total",
            "tinio_storage_objects_uploaded_total",
            "tinio_storage_objects_deleted_total",
            "tinio_storage_multipart_in_progress",
            "tinio_pipeline_queue_depth",
            "tinio_pipeline_in_flight",
            "tinio_pipeline_busy_workers",
            "tinio_write_lock_wait_duration_seconds",
            "tinio_write_lock_total_duration_seconds",
        ]
        .into_iter()
        .collect();
        let actual: HashSet<&str> = names
            .iter()
            .filter(|n| n.starts_with("tinio_"))
            .map(|n| n.as_str())
            .collect();
        assert_eq!(actual, expected, "tinio_* family set");
    }

    #[test]
    fn write_lock_histograms_reflect_the_snapshot() {
        let _window = test_lock::window();
        // The conversion is positional: snapshot bucket i maps to the
        // i-th cumulative `le=` bound (µs → seconds), `_sum`/`_count`
        // carry the snapshot's count/sum, and the text encoder appends
        // the `le="+Inf"` bucket from `_count` (the open >100k µs
        // bucket's counts are implicit there).
        let _snapshot_guard = WRITE_LOCK_SNAPSHOT_TEST.lock().unwrap();
        let snapshot = WriteLockStats {
            wait_buckets: [10, 5, 3, 2, 1, 0, 0],
            total_buckets: [0, 1, 2, 3, 4, 5, 6],
            count: 21,
            wait_sum_us: 21_000,
            wait_max_us: 9_999,
            total_sum_us: 210_000,
            total_max_us: 99_999,
        };
        refresh(Stats::default(), Stats::default(), snapshot);

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&default_registry().gather(), &mut buf)
            .unwrap();
        let text = String::from_utf8(buf).unwrap();
        // Wait distribution: cumulative 10/15/18/20/21/21, sum 0.021 s.
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_bucket{le="0.00001"} 10"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_bucket{le="0.0001"} 15"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_bucket{le="0.001"} 18"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_bucket{le="0.005"} 20"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_bucket{le="0.02"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_bucket{le="0.1"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_bucket{le="+Inf"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_sum 0.021"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_wait_duration_seconds_count 21"#),
            "{text}"
        );
        // Total distribution: cumulative 0/1/3/6/10/15, +Inf 21, sum 0.21 s.
        assert!(
            text.contains(r#"tinio_write_lock_total_duration_seconds_bucket{le="0.005"} 6"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_total_duration_seconds_bucket{le="+Inf"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_total_duration_seconds_sum 0.21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_write_lock_total_duration_seconds_count 21"#),
            "{text}"
        );
    }

    #[test]
    fn recording_increments_counters() {
        let _window = test_lock::window();
        record_http_request("INC", 200, Duration::from_millis(2));
        record_http_request("INC", 200, Duration::from_millis(1));
        record_s3_operation("IncGetObject", 200, Duration::from_millis(3));

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&default_registry().gather(), &mut buf)
            .unwrap();
        let text = String::from_utf8(buf).unwrap();
        // Label-bearing counters are asserted with unique labels so parallel
        // tests cannot clobber each other (the shared default registry);
        // label-less gauges are covered by registers_all_families instead.
        assert!(
            text.contains(r#"tinio_http_requests_total{method="INC",status="200"} 2"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_s3_operations_total{op="IncGetObject",status="200"} 1"#),
            "{text}"
        );
    }

    #[test]
    fn multipart_in_progress_dec_saturates_at_zero() {
        let _window = test_lock::window();
        let _guard = MULTIPART_GAUGE.lock().unwrap();
        // After a restart the persisted uploads are not counted — a dec
        // on an empty gauge must not drive it negative.
        STORAGE_MULTIPART_IN_PROGRESS.set(0);
        multipart_in_progress_dec();
        assert_eq!(STORAGE_MULTIPART_IN_PROGRESS.get(), 0);
        STORAGE_MULTIPART_IN_PROGRESS.set(3);
        multipart_in_progress_dec();
        assert_eq!(STORAGE_MULTIPART_IN_PROGRESS.get(), 2);
    }

    /// A minimal `S3` whose bucket/multipart operations answer per mode —
    /// every other operation inherits the trait default (NotImplemented).
    #[derive(Clone, Copy)]
    struct FakeS3 {
        mode: FakeMode,
    }

    #[derive(Clone, Copy)]
    enum FakeMode {
        Ok,
        NoSuchBucket,
        InternalError,
        NotImplemented,
    }

    #[async_trait]
    impl S3 for FakeS3 {
        async fn create_bucket(
            &self,
            _req: S3Request<dto::CreateBucketInput>,
        ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
            match self.mode {
                FakeMode::Ok => Ok(S3Response::new(CreateBucketOutput::default())),
                FakeMode::NoSuchBucket => Err(s3_error!(NoSuchBucket, "no such bucket")),
                FakeMode::InternalError => Err(s3_error!(InternalError, "boom")),
                FakeMode::NotImplemented => Err(s3_error!(NotImplemented, "nope")),
            }
        }

        async fn create_multipart_upload(
            &self,
            _req: S3Request<dto::CreateMultipartUploadInput>,
        ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
            Ok(S3Response::new(CreateMultipartUploadOutput::default()))
        }

        async fn complete_multipart_upload(
            &self,
            _req: S3Request<dto::CompleteMultipartUploadInput>,
        ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
            Ok(S3Response::new(CompleteMultipartUploadOutput::default()))
        }

        async fn abort_multipart_upload(
            &self,
            _req: S3Request<dto::AbortMultipartUploadInput>,
        ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
            Ok(S3Response::new(AbortMultipartUploadOutput::default()))
        }
    }

    fn request<T>(input: T) -> S3Request<T> {
        S3Request {
            input,
            method: Method::PUT,
            uri: Uri::default(),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn s3_counter(op: &str, status: u16) -> u64 {
        S3_OPERATIONS
            .with_label_values(&[op, &status.to_string()])
            .get()
    }

    #[test]
    fn metric_s3_records_status_classes() {
        let _window = test_lock::window();
        // Ok → 200; a generic client error → 400; InternalError → 500;
        // the trait default (NotImplemented) → 501.
        for (mode, _status) in [
            (FakeMode::Ok, 200),
            (FakeMode::NoSuchBucket, 400),
            (FakeMode::InternalError, 500),
            (FakeMode::NotImplemented, 501),
        ] {
            let backend = MetricS3::new(FakeS3 { mode });
            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(backend.create_bucket(request(CreateBucketInput::default())));
        }
        assert_eq!(s3_counter("CreateBucket", 200), 1);
        assert_eq!(s3_counter("CreateBucket", 400), 1);
        assert_eq!(s3_counter("CreateBucket", 500), 1);
        assert_eq!(s3_counter("CreateBucket", 501), 1);
    }

    #[test]
    #[cfg(feature = "multipart")]
    fn metric_s3_maintains_multipart_gauge() {
        let _window = test_lock::window();
        let _guard = MULTIPART_GAUGE.lock().unwrap();
        STORAGE_MULTIPART_IN_PROGRESS.set(0);
        let backend = MetricS3::new(FakeS3 { mode: FakeMode::Ok });
        let rt = Runtime::new().unwrap();
        rt.block_on(
            backend.create_multipart_upload(request(CreateMultipartUploadInput::default())),
        )
        .unwrap();
        assert_eq!(STORAGE_MULTIPART_IN_PROGRESS.get(), 1);
        rt.block_on(
            backend.complete_multipart_upload(request(CompleteMultipartUploadInput::default())),
        )
        .unwrap();
        assert_eq!(STORAGE_MULTIPART_IN_PROGRESS.get(), 0);
        // An abort on an empty gauge must not go negative (saturating).
        rt.block_on(backend.abort_multipart_upload(request(AbortMultipartUploadInput::default())))
            .unwrap();
        assert_eq!(STORAGE_MULTIPART_IN_PROGRESS.get(), 0);
    }

    #[test]
    fn metric_s3_records_every_delegated_operation() {
        let _window = test_lock::window();
        // Every thin wrapper must route through `record` — the fake
        // answers the trait default (NotImplemented → 501) for every op
        // it does not override, so each delegation line is exercised.
        let backend = MetricS3::new(FakeS3 {
            mode: FakeMode::NotImplemented,
        });
        let rt = Runtime::new().unwrap();
        macro_rules! call {
            ($op:ident, $input:ty) => {
                let _ = rt.block_on(backend.$op(request(<$input>::default())));
            };
        }
        call!(delete_bucket, dto::DeleteBucketInput);
        call!(head_bucket, dto::HeadBucketInput);
        call!(list_buckets, dto::ListBucketsInput);
        call!(get_bucket_location, dto::GetBucketLocationInput);
        call!(get_bucket_tagging, dto::GetBucketTaggingInput);
        call!(delete_bucket_tagging, dto::DeleteBucketTaggingInput);
        // `PutBucketTaggingInput` has a required `tagging` field — no
        // Default derive — built explicitly like the delete_objects
        // input above.
        let _ = rt.block_on(
            backend.put_bucket_tagging(request(dto::PutBucketTaggingInput {
                bucket: "b".into(),
                tagging: dto::Tagging { tag_set: vec![] },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
            })),
        );
        call!(put_object, dto::PutObjectInput);
        call!(get_object, dto::GetObjectInput);
        call!(head_object, dto::HeadObjectInput);
        call!(get_object_attributes, dto::GetObjectAttributesInput);
        call!(delete_object, dto::DeleteObjectInput);
        call!(get_object_tagging, dto::GetObjectTaggingInput);
        call!(delete_object_tagging, dto::DeleteObjectTaggingInput);
        #[cfg(feature = "multipart")]
        call!(upload_part, dto::UploadPartInput);
        #[cfg(feature = "list-v1")]
        call!(list_objects, dto::ListObjectsInput);
        #[cfg(feature = "list-v2")]
        call!(list_objects_v2, dto::ListObjectsV2Input);
        #[cfg(feature = "multipart")]
        call!(list_parts, dto::ListPartsInput);
        #[cfg(feature = "multipart")]
        call!(list_multipart_uploads, dto::ListMultipartUploadsInput);
        // These three inputs have required fields (no Default) — built
        // explicitly with every field.
        #[cfg(feature = "copy")]
        {
            let copy_source = CopySource::parse("src/key").unwrap();
            let _ = rt.block_on(backend.copy_object(request(dto::CopyObjectInput {
                bucket: "b".into(),
                key: "k".into(),
                copy_source,
                acl: None,
                bucket_key_enabled: None,
                cache_control: None,
                checksum_algorithm: None,
                content_disposition: None,
                content_encoding: None,
                content_language: None,
                content_type: None,
                copy_source_if_match: None,
                copy_source_if_modified_since: None,
                copy_source_if_none_match: None,
                copy_source_if_unmodified_since: None,
                copy_source_sse_customer_algorithm: None,
                copy_source_sse_customer_key: None,
                copy_source_sse_customer_key_md5: None,
                expected_bucket_owner: None,
                expected_source_bucket_owner: None,
                expires: None,
                grant_full_control: None,
                grant_read: None,
                grant_read_acp: None,
                grant_write_acp: None,
                metadata: None,
                metadata_directive: None,
                object_lock_legal_hold_status: None,
                object_lock_mode: None,
                object_lock_retain_until_date: None,
                request_payer: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                ssekms_encryption_context: None,
                ssekms_key_id: None,
                server_side_encryption: None,
                storage_class: None,
                tagging: None,
                tagging_directive: None,
                website_redirect_location: None,
            })));
            let _ = rt.block_on(backend.upload_part_copy(request(dto::UploadPartCopyInput {
                bucket: "b".into(),
                key: "k".into(),
                copy_source: CopySource::parse("src/key").unwrap(),
                part_number: 1,
                upload_id: "u".into(),
                copy_source_if_match: None,
                copy_source_if_modified_since: None,
                copy_source_if_none_match: None,
                copy_source_if_unmodified_since: None,
                copy_source_range: None,
                copy_source_sse_customer_algorithm: None,
                copy_source_sse_customer_key: None,
                copy_source_sse_customer_key_md5: None,
                expected_bucket_owner: None,
                expected_source_bucket_owner: None,
                request_payer: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            })));
            // `RenameObjectInput` derives Default — the plain `call!`
            // form (the fake answers NotImplemented on every op it does
            // not override).
            call!(rename_object, dto::RenameObjectInput);
        }
        let _ = rt.block_on(backend.delete_objects(request(dto::DeleteObjectsInput {
            bucket: "b".into(),
            delete: Delete::default(),
            bypass_governance_retention: None,
            checksum_algorithm: None,
            expected_bucket_owner: None,
            mfa: None,
            request_payer: None,
        })));
        // `PutObjectTaggingInput` has a required `tagging` field — no
        // Default derive — built explicitly like the delete_objects
        // input above.
        let _ = rt.block_on(
            backend.put_object_tagging(request(dto::PutObjectTaggingInput {
                bucket: "b".into(),
                key: "k".into(),
                tagging: dto::Tagging { tag_set: vec![] },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                request_payer: None,
                version_id: None,
            })),
        );
        let mut expected: Vec<&str> = vec![
            "DeleteBucket",
            "HeadBucket",
            "ListBuckets",
            "GetBucketLocation",
            "GetBucketTagging",
            "PutBucketTagging",
            "DeleteBucketTagging",
            "PutObject",
            "GetObject",
            "HeadObject",
            "GetObjectAttributes",
            "DeleteObject",
            "DeleteObjects",
            "GetObjectTagging",
            "PutObjectTagging",
            "DeleteObjectTagging",
        ];
        #[cfg(feature = "multipart")]
        expected.extend(["UploadPart", "ListParts", "ListMultipartUploads"]);
        let recorded: Vec<&str> = expected
            .iter()
            .copied()
            .filter(|op| s3_counter(op, 501) == 1)
            .collect();
        // Every unconditional + feature-enabled op recorded exactly one
        // 501 (the copy/list-v1/list-v2 ops are asserted under cfg below
        // to keep the baseline feature-independent).
        assert_eq!(recorded, expected, "{recorded:?}");
        #[cfg(feature = "copy")]
        {
            assert_eq!(s3_counter("CopyObject", 501), 1);
            assert_eq!(s3_counter("UploadPartCopy", 501), 1);
            assert_eq!(s3_counter("RenameObject", 501), 1);
        }
        #[cfg(feature = "list-v1")]
        assert_eq!(s3_counter("ListObjects", 501), 1);
        #[cfg(feature = "list-v2")]
        assert_eq!(s3_counter("ListObjectsV2", 501), 1);
    }
}

/// A delegation wrapper recording `tinio_s3_operations_total{op,status}`
/// plus duration for every implemented operation, and the in-progress
/// multipart gauge (task T054). The status label is `200` on success,
/// `400` for client errors, and `500`/`501` for internal/not-implemented.
///
/// # Examples
///
/// ```rust
/// use http::{Extensions, HeaderMap, Method, Uri};
/// use s3s::{S3, S3Request, dto::ListBucketsInput};
/// use tinio_mem::MemoryStorage;
/// use tinio_server::{backend::S3Backend, metrics::MetricS3};
/// use tokio::runtime::Runtime;
///
/// fn request<T>(input: T) -> S3Request<T> {
///     S3Request {
///         input,
///         method: Method::GET,
///         uri: Uri::default(),
///         headers: HeaderMap::new(),
///         extensions: Extensions::new(),
///         credentials: None,
///         region: None,
///         service: None,
///         trailing_headers: None,
///     }
/// }
///
/// let inner = S3Backend::new(MemoryStorage::new().unwrap(), Default::default());
/// let backend = MetricS3::new(inner);
/// let out = Runtime::new().unwrap().block_on(async {
///     backend
///         .list_buckets(request(ListBucketsInput::default()))
///         .await
///         .unwrap()
/// });
/// assert_eq!(out.output.buckets.as_ref().unwrap().len(), 0);
/// ```
#[derive(Debug, Clone)]
pub struct MetricS3<T> {
    inner: T,
}

impl<T> MetricS3<T> {
    /// Wrap any `S3` implementation.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    /// The inner mapping.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Record one operation: `op` label, status class, duration.
    async fn record<R>(
        &self,
        op: &'static str,
        fut: impl Future<Output = S3Result<R>> + Send,
    ) -> S3Result<R> {
        let start = Instant::now();
        let result = fut.await;
        let status = match &result {
            Ok(_) => 200u16,
            Err(err) => match err.code().as_str() {
                "InternalError" => 500,
                "NotImplemented" => 501,
                _ => 400,
            },
        };
        record_s3_operation(op, status, start.elapsed());
        result
    }
}

#[async_trait]
impl<T: S3 + Send + Sync> S3 for MetricS3<T> {
    // --- buckets ---
    async fn create_bucket(
        &self,
        req: S3Request<dto::CreateBucketInput>,
    ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
        self.record("CreateBucket", self.inner.create_bucket(req))
            .await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketOutput>> {
        self.record("DeleteBucket", self.inner.delete_bucket(req))
            .await
    }

    async fn head_bucket(
        &self,
        req: S3Request<dto::HeadBucketInput>,
    ) -> S3Result<S3Response<dto::HeadBucketOutput>> {
        self.record("HeadBucket", self.inner.head_bucket(req)).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<dto::ListBucketsInput>,
    ) -> S3Result<S3Response<dto::ListBucketsOutput>> {
        self.record("ListBuckets", self.inner.list_buckets(req))
            .await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<dto::GetBucketLocationOutput>> {
        self.record("GetBucketLocation", self.inner.get_bucket_location(req))
            .await
    }

    async fn get_bucket_tagging(
        &self,
        req: S3Request<dto::GetBucketTaggingInput>,
    ) -> S3Result<S3Response<dto::GetBucketTaggingOutput>> {
        self.record("GetBucketTagging", self.inner.get_bucket_tagging(req))
            .await
    }

    async fn put_bucket_tagging(
        &self,
        req: S3Request<dto::PutBucketTaggingInput>,
    ) -> S3Result<S3Response<dto::PutBucketTaggingOutput>> {
        self.record("PutBucketTagging", self.inner.put_bucket_tagging(req))
            .await
    }

    async fn delete_bucket_tagging(
        &self,
        req: S3Request<dto::DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<dto::DeleteBucketTaggingOutput>> {
        self.record("DeleteBucketTagging", self.inner.delete_bucket_tagging(req))
            .await
    }

    // --- objects ---
    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.record("PutObject", self.inner.put_object(req)).await
    }

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.record("GetObject", self.inner.get_object(req)).await
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        self.record("HeadObject", self.inner.head_object(req)).await
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<dto::GetObjectAttributesInput>,
    ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
        self.record("GetObjectAttributes", self.inner.get_object_attributes(req))
            .await
    }

    async fn delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        self.record("DeleteObject", self.inner.delete_object(req))
            .await
    }

    async fn delete_objects(
        &self,
        req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        self.record("DeleteObjects", self.inner.delete_objects(req))
            .await
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<dto::GetObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::GetObjectTaggingOutput>> {
        self.record("GetObjectTagging", self.inner.get_object_tagging(req))
            .await
    }

    async fn put_object_tagging(
        &self,
        req: S3Request<dto::PutObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::PutObjectTaggingOutput>> {
        self.record("PutObjectTagging", self.inner.put_object_tagging(req))
            .await
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<dto::DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectTaggingOutput>> {
        self.record("DeleteObjectTagging", self.inner.delete_object_tagging(req))
            .await
    }

    #[cfg(feature = "copy")]
    async fn copy_object(
        &self,
        req: S3Request<dto::CopyObjectInput>,
    ) -> S3Result<S3Response<dto::CopyObjectOutput>> {
        self.record("CopyObject", self.inner.copy_object(req)).await
    }

    #[cfg(feature = "copy")]
    async fn rename_object(
        &self,
        req: S3Request<dto::RenameObjectInput>,
    ) -> S3Result<S3Response<dto::RenameObjectOutput>> {
        self.record("RenameObject", self.inner.rename_object(req))
            .await
    }

    // --- listing ---
    #[cfg(feature = "list-v1")]
    async fn list_objects(
        &self,
        req: S3Request<dto::ListObjectsInput>,
    ) -> S3Result<S3Response<dto::ListObjectsOutput>> {
        self.record("ListObjects", self.inner.list_objects(req))
            .await
    }

    #[cfg(feature = "list-v2")]
    async fn list_objects_v2(
        &self,
        req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        self.record("ListObjectsV2", self.inner.list_objects_v2(req))
            .await
    }

    // --- multipart (in-progress gauge maintained here) ---
    #[cfg(feature = "multipart")]
    async fn create_multipart_upload(
        &self,
        req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        let out = self
            .record(
                "CreateMultipartUpload",
                self.inner.create_multipart_upload(req),
            )
            .await;
        if out.is_ok() {
            STORAGE_MULTIPART_IN_PROGRESS.inc();
        }
        out
    }

    #[cfg(feature = "multipart")]
    async fn upload_part(
        &self,
        req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        self.record("UploadPart", self.inner.upload_part(req)).await
    }

    #[cfg(feature = "multipart")]
    #[cfg(feature = "copy")]
    async fn upload_part_copy(
        &self,
        req: S3Request<dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<dto::UploadPartCopyOutput>> {
        self.record("UploadPartCopy", self.inner.upload_part_copy(req))
            .await
    }

    #[cfg(feature = "multipart")]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        let out = self
            .record(
                "CompleteMultipartUpload",
                self.inner.complete_multipart_upload(req),
            )
            .await;
        if out.is_ok() {
            multipart_in_progress_dec();
        }
        out
    }

    #[cfg(feature = "multipart")]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        let out = self
            .record(
                "AbortMultipartUpload",
                self.inner.abort_multipart_upload(req),
            )
            .await;
        if out.is_ok() {
            multipart_in_progress_dec();
        }
        out
    }

    #[cfg(feature = "multipart")]
    async fn list_parts(
        &self,
        req: S3Request<dto::ListPartsInput>,
    ) -> S3Result<S3Response<dto::ListPartsOutput>> {
        self.record("ListParts", self.inner.list_parts(req)).await
    }

    #[cfg(feature = "multipart")]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<dto::ListMultipartUploadsOutput>> {
        self.record(
            "ListMultipartUploads",
            self.inner.list_multipart_uploads(req),
        )
        .await
    }
}
