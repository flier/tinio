//! Prometheus registry and metric families (task T023).
//!
//! The metric layers of the data model (data-model.md Metrics): S3 operations
//! (`tinio_s3_*`), storage (`tinio_storage_*`), the pipeline gauges
//! (`tinio_pipeline_*`), and the write-lock duration histograms
//! (`tinio_storage_write_lock_*`, pipeline-spec.md §4). The HTTP families
//! (`tinio_http_*`) are declared and emitted in `tinio-api`, the crate that
//! renders `/metrics` and serves the management plane they observe. Families
//! are process-wide globals, registered once on the default registry via
//! `register_*!`. The storage-layer full-scan gauges are computed by the
//! CLI-owned reader task (T075 — one scan per ttl, no cache; the cadence is
//! `tinio_api::DEFAULT_TTL`); the pipeline gauges are refreshed from the
//! runtimes' [`Stats`] snapshots on that same tick, and the write-lock
//! histograms are filled from the shared plain-data [`WriteLockSnapshot`]
//! (`tinio-core` owns the type, so `tinio-server` never imports a backend
//! crate) — cheap atomic snapshots, no TTL cache (that pattern belongs to
//! the storage full-scan gauges only).
//!
//! # Examples
//!
//! ```rust
//! use std::time::Duration;
//!
//! use tinio_server::metrics::{STORAGE_BUCKETS, record_s3_operation};
//!
//! record_s3_operation("GetObject", 200, Duration::from_millis(5));
//! STORAGE_BUCKETS.set(2);
//! assert!(!prometheus::default_registry().gather().is_empty());
//! ```

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use prometheus::{
    HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Result as PromResult,
    core::{Collector, Desc},
    proto::{Bucket, Histogram, Metric, MetricFamily, MetricType},
    register, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec,
};
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result, dto};
use tinio_util::{
    metrics::{
        LockOperation, RegisteredExt, record_protected_deletion as record_util_protected_deletion,
    },
    status_label,
};
use tracing::{field, instrument};

use crate::{
    _core::{
        object_lock::RefusalReason,
        pipeline::Stats,
        storage::{WRITE_LOCK_BUCKET_BOUNDS_US, WRITE_LOCK_BUCKETS, WriteLockSnapshot},
    },
    backend::ops::tinio_s3_ops,
};

/// S3 data-plane operations, by operation and status.
pub static S3_OPERATIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_s3_operations_total",
        "S3 data-plane operations, by operation and status",
        &["op", "status"]
    )
    .registered("tinio_s3_operations_total")
});
/// S3 operation durations, by operation.
pub static S3_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "tinio_s3_operation_duration_seconds",
        "S3 operation durations, by operation",
        &["op"]
    )
    .registered("tinio_s3_operation_duration_seconds")
});
/// S3 wire errors, by operation and error code.
pub static S3_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_s3_errors_total",
        "S3 wire errors, by operation and error code",
        &["op", "code"]
    )
    .registered("tinio_s3_errors_total")
});
/// Bucket count (full-scan, TTL-cached 30 s; management plane, T075).
pub static STORAGE_BUCKETS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_buckets",
        "Number of buckets (full-scan, TTL-cached 30 s)"
    )
    .registered("tinio_storage_buckets")
});
/// Object count (full-scan, TTL-cached 30 s; management plane, T075).
pub static STORAGE_OBJECTS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_objects",
        "Number of objects (full-scan, TTL-cached 30 s)"
    )
    .registered("tinio_storage_objects")
});
/// Total object bytes (full-scan, TTL-cached 30 s; management plane, T075).
pub static STORAGE_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_bytes",
        "Total object bytes (full-scan, TTL-cached 30 s)"
    )
    .registered("tinio_storage_bytes")
});
/// Bytes streamed into storage.
pub static STORAGE_UPLOAD_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_upload_bytes_total",
        "Bytes streamed into storage"
    )
    .registered("tinio_storage_upload_bytes_total")
});
/// Bytes streamed out of storage.
pub static STORAGE_DOWNLOAD_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_download_bytes_total",
        "Bytes streamed out of storage"
    )
    .registered("tinio_storage_download_bytes_total")
});
/// Objects written, by operation (put, copy, multipart-complete).
pub static STORAGE_OBJECTS_UPLOADED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_storage_objects_uploaded_total",
        "Objects written to storage, by operation",
        &["op"]
    )
    .registered("tinio_storage_objects_uploaded_total")
});
/// Objects deleted from storage.
pub static STORAGE_OBJECTS_DELETED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "tinio_storage_objects_deleted_total",
        "Objects deleted from storage"
    )
    .registered("tinio_storage_objects_deleted_total")
});
/// Multipart uploads currently in progress.
pub static STORAGE_MULTIPART_IN_PROGRESS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_storage_multipart_in_progress",
        "Multipart uploads currently in progress"
    )
    .registered("tinio_storage_multipart_in_progress")
});
/// Tasks currently queued in a pipeline, by pipeline (io/db).
pub static PIPELINE_QUEUE_DEPTH: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "tinio_pipeline_queue_depth",
        "Tasks currently queued in a pipeline, by pipeline",
        &["pipeline"]
    )
    .registered("tinio_pipeline_queue_depth")
});
/// Tasks currently executing in a pipeline, by pipeline (io/db).
pub static PIPELINE_IN_FLIGHT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "tinio_pipeline_in_flight",
        "Tasks currently executing in a pipeline, by pipeline",
        &["pipeline"]
    )
    .registered("tinio_pipeline_in_flight")
});
/// The pipeline families' bucket bounds: one scale for task duration and enqueue wait.
const TASK_DURATION_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0];

/// Pipeline task durations, by pipeline.
pub static PIPELINE_TASK_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "tinio_pipeline_task_duration_seconds",
        "Pipeline task durations, by pipeline",
        &["pipeline"],
        TASK_DURATION_BUCKETS.to_vec(),
    )
    .registered("tinio_pipeline_task_duration_seconds")
});
/// Pipeline task outcomes, by pipeline and outcome (`ok` | `error` | `panic`).
pub static PIPELINE_TASKS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_pipeline_tasks_total",
        "Pipeline task outcomes, by pipeline and outcome",
        &["pipeline", "outcome"]
    )
    .registered("tinio_pipeline_tasks_total")
});
/// Time a task waited between enqueue and the worker picking it up, by pipeline.
pub static PIPELINE_ENQUEUE_WAIT: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "tinio_pipeline_enqueue_wait_seconds",
        "Time a task waited before a worker picked it up, by pipeline",
        &["pipeline"],
        TASK_DURATION_BUCKETS.to_vec(),
    )
    .registered("tinio_pipeline_enqueue_wait_seconds")
});
/// The write-lock duration histograms (pipeline-spec.md §4): the
/// `wait` and `total` distributions of every write transaction,
/// converted from the tinio-fs snapshot at scrape time (registered
/// with the default registry — the registry holds its own copy of
/// the descriptors).
pub static WRITE_LOCK_HISTOGRAMS: LazyLock<WriteLockHistograms> = LazyLock::new(|| {
    let histograms = WriteLockHistograms::new().registered("the write-lock histogram descriptors");
    register(Box::new(histograms.clone())).registered("the write-lock histogram families");
    histograms
});

/// The metric statics are process-global (the prometheus default
/// registry): a test binary runs hundreds of tests in parallel threads,
/// every one of which records into the same statics through the data
/// plane, so an exact-value/family assertion races the other tests'
/// writes (observed flakes: the write-lock snapshot, the family set,
/// `STORAGE_DOWNLOAD_BYTES`). The serialization is a
/// cfg(test)-only writer lock: every metric WRITE takes it, and the
/// exact-assert tests take it for their whole assert window. Production
/// call sites compile the lock away. Reentrant: an exact test holds the
/// window and does its setup THROUGH the public record/refresh fns, so a
/// writer on the same thread (one test = one thread) must not deadlock
/// on its own lock — the window marks the thread and writers skip
/// acquisition while it is held.
#[cfg(test)]
pub(crate) use tinio_util::testing::metrics::test_lock;

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

/// Increment the in-progress-multipart gauge. Not test-lock guarded: the
/// inline increment it replaces was not either (the exact-assert test holds
/// `MULTIPART_GAUGE` across the three multipart operations).
#[cfg(feature = "multipart")]
pub(crate) fn multipart_in_progress_inc() {
    STORAGE_MULTIPART_IN_PROGRESS.inc();
}

/// Decrement the in-progress-multipart gauge, saturating at zero: after a
/// restart the persisted uploads are not counted, so completing or
/// aborting one must not drive the gauge negative.
#[cfg(feature = "multipart")]
pub(crate) fn multipart_in_progress_dec() {
    #[cfg(test)]
    let _g = test_lock::writer();
    if STORAGE_MULTIPART_IN_PROGRESS.get() > 0 {
        STORAGE_MULTIPART_IN_PROGRESS.dec();
    }
}

/// Count one successful upload-shaped operation under its wire name.
///
/// The shared operation table carries the call beside the row, so a new upload-shaped operation
/// declares its count where the operation itself is declared.
pub(crate) fn object_uploaded(op: &'static str) {
    STORAGE_OBJECTS_UPLOADED.with_label_values(&[op]).inc();
}

/// Count one successful object deletion.
pub(crate) fn object_deleted() {
    STORAGE_OBJECTS_DELETED.inc();
}

/// Record one permanent delete a stored Object Lock refused (OL25).
///
/// The family lives in `tinio-util` because the lifecycle pass and both
/// backends' sweepers feed it too; this is the S3 surface's one call, made
/// where the `Locked` error is still in hand so the operation names the wire
/// call (`delete_object` / `delete_objects`). The test writer gate is the
/// crate's, so an exact-value test's window excludes every other test's
/// refusals of the same family.
pub(crate) fn record_protected_deletion(operation: LockOperation, reason: RefusalReason) {
    #[cfg(test)]
    let _g = test_lock::writer();
    record_util_protected_deletion(operation, reason);
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

/// Record a completed S3 data-plane operation.
pub fn record_s3_operation(op: &'static str, status: u16, duration: Duration) {
    #[cfg(test)]
    let _g = test_lock::writer();
    let mut label = itoa::Buffer::new();
    record(
        &S3_OPERATIONS,
        &[op, status_label!(status, label)],
        &S3_DURATION,
        op,
        duration,
    );
}

/// Record one S3 wire error: the operation and the S3 error code the client receives.
pub fn record_s3_error(op: &'static str, code: &S3ErrorCode) {
    #[cfg(test)]
    let _g = test_lock::writer();
    S3_ERRORS.with_label_values(&[op, code.as_str()]).inc();
}

/// Refresh every scrape-computed family from one call (F49 — the single
/// refresh entry point behind the management plane's `/metrics`,
/// pipeline-spec.md §4): the pipeline gauges from two [`Stats`]
/// snapshots and the write-lock histograms from the shared plain-data
/// [`WriteLockSnapshot`] (the snapshot is taken at gather, never on the
/// write path). Touching the statics registers the
/// families with the default registry (F10 — the CLI's reader tick calls
/// this every ttl, so a running server's registry always contains them; the
/// data-plane listener serves no `/metrics`).
pub fn refresh(io: Stats, remove: Stats, db: Stats, write_lock: WriteLockSnapshot) {
    #[cfg(test)]
    let _g = test_lock::writer();
    for (label, stats) in [("io", io), ("remove", remove), ("db", db)] {
        PIPELINE_QUEUE_DEPTH
            .with_label_values(&[label])
            .set(i64::try_from(stats.queue_depth).unwrap_or(i64::MAX));
        PIPELINE_IN_FLIGHT
            .with_label_values(&[label])
            .set(i64::try_from(stats.in_flight).unwrap_or(i64::MAX));
    }
    WRITE_LOCK_HISTOGRAMS.refresh(write_lock);
}

/// The scrape-time conversion of the write-lock histograms
/// (pipeline-spec.md §4): two cumulative histogram families — the `wait`
/// and `hold` distributions of every write transaction — filled from
/// the latest [`WriteLockSnapshot`] on gather. Prometheus histograms
/// cannot be set from external counts, so the families are a small
/// custom [`Collector`] over the [`prometheus::proto`] message types: the shared
/// tinio-core bucket bounds become cumulative `le=` buckets (µs →
/// seconds), with `_sum`/`_count` from the snapshot's count/sum (the
/// text encoder appends the `le="+Inf"` bucket from `_count`).
#[derive(Clone)]
pub struct WriteLockHistograms {
    wait_desc: Desc,
    total_desc: Desc,
    /// The latest snapshot, shared with the registered clone (the
    /// `LazyLock` registers a clone — the snapshot must be one Arc, not
    /// per-copy state). A cheap atomic read on the write path — no 30 s
    /// TTL cache; that pattern belongs to the storage full-scan gauges
    /// only.
    snapshot: Arc<Mutex<WriteLockSnapshot>>,
}

impl WriteLockHistograms {
    /// Build the two family descriptors. The bucket bounds are the
    /// shared tinio-core constants — the conversion is positional by
    /// index (the metrics layer reads the buckets by index, not name).
    fn new() -> PromResult<Self> {
        let wait_desc = Desc::new(
            "tinio_storage_write_lock_wait_seconds".to_string(),
            "Write-lock wait duration of write transactions (the entry-to-begin_write interval, approximating the single-writer lock wait), cumulative histogram"
                .to_string(),
            Vec::new(),
            HashMap::new(),
        )?;
        let total_desc = Desc::new(
            "tinio_storage_write_lock_hold_seconds".to_string(),
            "Write-lock hold duration of write transactions (entry to commit/abort return, incl. fsync), cumulative histogram"
                .to_string(),
            Vec::new(),
            HashMap::new(),
        )?;
        Ok(Self {
            wait_desc,
            total_desc,
            snapshot: Arc::new(Mutex::new(WriteLockSnapshot::default())),
        })
    }

    /// The snapshot's guard — the one spelling of the lock's D3 recovery,
    /// never a panic: both critical sections are one whole-struct write or
    /// read, invariant-preserving, so a poisoned lock still holds a complete
    /// snapshot.
    fn snapshot_guard(&self) -> MutexGuard<'_, WriteLockSnapshot> {
        self.snapshot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Store the latest snapshot for the scrape-time conversion.
    fn refresh(&self, snapshot: WriteLockSnapshot) {
        *self.snapshot_guard() = snapshot;
    }
}

impl Collector for WriteLockHistograms {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.wait_desc, &self.total_desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // A poisoned lock still holds a complete snapshot (one struct,
        // assigned whole) — see `snapshot_guard`.
        let snapshot = *self.snapshot_guard();
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
    use prometheus::default_registry;
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
    use tinio_util::testing::{metrics::exposition, spans};
    use tokio::runtime::Runtime;
    use tracing::{Level, callsite::rebuild_interest_cache, subscriber::with_default};
    use tracing_subscriber::{prelude::*, registry};

    use super::*;
    #[cfg(any(feature = "acl", feature = "copy"))]
    use crate::_auth::metrics as auth_metrics;
    #[cfg(feature = "select")]
    use crate::_select::metrics as select_metrics;
    use crate::{_mem::MemoryStorage, _util::metrics as util_metrics, backend::S3Backend};

    /// The `tinio_*` names the process registers today, grouped by the crate
    /// that declares them. A task that adds a family appends it here in the
    /// same change: the gate is exact, so an unlisted family fails this test.
    const SPEC_FAMILIES: &[&str] = &[
        // `tinio-server`
        "tinio_s3_operations_total",
        "tinio_s3_operation_duration_seconds",
        "tinio_s3_errors_total",
        "tinio_storage_buckets",
        "tinio_storage_objects",
        "tinio_storage_bytes",
        "tinio_storage_upload_bytes_total",
        "tinio_storage_download_bytes_total",
        "tinio_storage_objects_uploaded_total",
        "tinio_storage_objects_deleted_total",
        "tinio_storage_multipart_in_progress",
        "tinio_pipeline_queue_depth",
        "tinio_pipeline_in_flight",
        "tinio_pipeline_task_duration_seconds",
        "tinio_pipeline_tasks_total",
        "tinio_pipeline_enqueue_wait_seconds",
        "tinio_storage_write_lock_wait_seconds",
        "tinio_storage_write_lock_hold_seconds",
        // `tinio-util` — the cross-backend families.
        "tinio_storage_write_failures_total",
        "tinio_storage_pass_duration_seconds",
        "tinio_storage_pass_failures_total",
        "tinio_storage_read_only",
        "tinio_storage_corrupt_rows_total",
        "tinio_object_lock_protected_deletions_total",
    ];

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
        record_s3_operation("FamGetObject", 200, Duration::from_millis(1));
        record_s3_error("FamGetObject", &S3ErrorCode::NoSuchKey);
        STORAGE_BUCKETS.set(0);
        STORAGE_OBJECTS.set(0);
        STORAGE_BYTES.set(0);
        STORAGE_UPLOAD_BYTES.inc_by(0);
        STORAGE_DOWNLOAD_BYTES.inc_by(0);
        STORAGE_OBJECTS_UPLOADED.with_label_values(&["put"]).inc();
        STORAGE_OBJECTS_DELETED.inc_by(0);
        STORAGE_MULTIPART_IN_PROGRESS.set(0);
        // The five cross-backend families are declared in `tinio-util`; touch
        // each so this gate also sees them in the shared registry.
        util_metrics::record_write_failure(
            util_metrics::Backend::Fs,
            util_metrics::WriteOp::Put,
            util_metrics::WriteFailureKind::Io,
        );
        util_metrics::record_pass_duration(util_metrics::Pass::FsSweep, Duration::ZERO);
        util_metrics::record_fs_pass_failure(util_metrics::FsSection::Tmp);
        util_metrics::record_read_only(util_metrics::Backend::Fs, false);
        util_metrics::record_corrupt_rows(util_metrics::Backend::Fs, 0);
        // The Object Lock refusal family: touch it so this gate sees it in
        // isolation and in a full run alike.
        util_metrics::record_protected_deletion(
            util_metrics::LockOperation::Sweeper,
            RefusalReason::Governance,
        );
        // The pipeline families: the histograms need one sample each to be gathered.
        PIPELINE_TASK_DURATION
            .with_label_values(&["gate"])
            .observe(0.0);
        PIPELINE_ENQUEUE_WAIT
            .with_label_values(&["gate"])
            .observe(0.0);
        PIPELINE_TASKS.with_label_values(&["gate", "ok"]).inc();
        // The decision counter is declared in `tinio-auth`, which this binary
        // links for the ACL tests; touch it so the gate sees it in isolation
        // and in a full run alike.
        #[cfg(any(feature = "acl", feature = "copy"))]
        {
            auth_metrics::record_authorization(false);
            auth_metrics::record_credential_lookup(false);
        }
        // The Select families are declared in `tinio-select`; touch them so the
        // gate sees them in isolation and in a full run alike.
        #[cfg(feature = "select")]
        select_metrics::record_stream(select_metrics::Format::Csv, 0, 0);
        // The write-lock histograms are always emitted (zero counts
        // before any write transaction); the refresh is also the
        // registration path (the management plane's `/metrics` calls it on
        // every scrape, through the CLI's adapter, F10). The refresh happens under the snapshot
        // guard — it writes the shared snapshot, so it must not race the
        // other snapshot test's refresh+encode (a clobbered snapshot
        // fails its exact-value asserts and poisons the guard).
        let _snapshot_guard = WRITE_LOCK_SNAPSHOT_TEST.lock().unwrap();
        // The pipeline gauges (io/remove/db labels) sample the inline
        // runners — stats are all zeros, the family set is what matters.
        refresh(
            Stats::default(),
            Stats::default(),
            Stats::default(),
            WriteLockSnapshot::default(),
        );
        let names: Vec<String> = default_registry()
            .gather()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        // The HTTP families belong to `tinio-api` (its middleware records
        // them; a process that runs that plane carries them in the same
        // registry), so this crate's set is the gate below.
        let mut expected: HashSet<&str> = SPEC_FAMILIES.iter().copied().collect();
        // Families another crate declares on this binary's path; the spec table
        // is their home, and this gate sees them only when the feature is on.
        // `tinio-auth`'s two families: the crate is on this binary's path only
        // with `acl` or `copy`, and the recorders above are gated the same way.
        #[cfg(any(feature = "acl", feature = "copy"))]
        {
            expected.insert("tinio_auth_authorizations_total");
            expected.insert("tinio_auth_credential_lookups_total");
        }
        #[cfg(feature = "select")]
        {
            expected.insert("tinio_s3_select_scanned_bytes_total");
            expected.insert("tinio_s3_select_rows_returned_total");
        }
        let actual: HashSet<&str> = names
            .iter()
            .filter(|n| n.starts_with("tinio_"))
            .map(|n| n.as_str())
            .collect();
        // Exact in both directions: a declared family that never registered, and a
        // registered family the spec does not list, both fail. The spec table in
        // `data-model.md` §Metrics is the one enumeration; this list is its code-side
        // projection for this binary's feature set.
        let mut missing: Vec<&str> = expected.difference(&actual).copied().collect();
        missing.sort_unstable();
        let mut unexpected: Vec<&str> = actual.difference(&expected).copied().collect();
        unexpected.sort_unstable();
        assert!(
            missing.is_empty() && unexpected.is_empty(),
            "family gate mismatch — unregistered: {missing:?}; undeclared: {unexpected:?}"
        );
    }

    #[test]
    fn s3_errors_total_records_the_wire_code() {
        let _window = test_lock::window();
        record_s3_error("ErrGetObject", &S3ErrorCode::NoSuchKey);
        let text = exposition();
        assert!(
            text.lines().any(|line| {
                line.starts_with("tinio_s3_errors_total{")
                    && line.contains(r#"op="ErrGetObject""#)
                    && line.contains(r#"code="NoSuchKey""#)
            }),
            "{text}"
        );
    }

    /// The decorator's `Err` arm is `record_s3_error`'s only production
    /// caller: the wire code the client receives is booked under the
    /// operation. The delta is exact under the window (every other writer is
    /// locked out), and the labels are this test's own — the status-class
    /// test records the status family, never the error family.
    #[test]
    fn metric_s3_records_the_wire_code_on_a_failed_operation() {
        let _window = test_lock::window();
        let counter = || {
            S3_ERRORS
                .with_label_values(&["CreateBucket", "NoSuchBucket"])
                .get()
        };
        let before = counter();
        let backend = MetricS3::new(FakeS3 {
            mode: FakeMode::NoSuchBucket,
        });
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(backend.create_bucket(request(CreateBucketInput::default())));
        let err = result.expect_err("the fake refuses the operation");
        assert_eq!(err.code().as_str(), "NoSuchBucket");
        assert_eq!(
            counter() - before,
            1,
            "the Err arm books one error under its op and code"
        );
    }

    /// `refresh` publishes both pipeline gauges for all three lanes (`io`,
    /// `remove`, `db`) — the three-lane loop is the branch, not a single
    /// `io` sample. Distinct values per lane make a swapped or missing lane
    /// observable; the window keeps parallel writers out of the window.
    #[test]
    fn refresh_publishes_every_pipeline_lane() {
        let _window = test_lock::window();
        let stats = |queue_depth, in_flight| Stats {
            queue_depth,
            in_flight,
            busy_workers: in_flight,
        };
        refresh(
            stats(11, 12),
            stats(21, 22),
            stats(31, 32),
            WriteLockSnapshot::default(),
        );
        let text = exposition();
        for (lane, depth, flight) in [("io", 11, 12), ("remove", 21, 22), ("db", 31, 32)] {
            let queue = format!(r#"tinio_pipeline_queue_depth{{pipeline="{lane}"}} {depth}"#);
            let in_flight = format!(r#"tinio_pipeline_in_flight{{pipeline="{lane}"}} {flight}"#);
            assert!(text.contains(&queue), "missing {queue}: {text}");
            assert!(text.contains(&in_flight), "missing {in_flight}: {text}");
        }
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
        let snapshot = WriteLockSnapshot {
            wait_buckets: [10, 5, 3, 2, 1, 0, 0],
            total_buckets: [0, 1, 2, 3, 4, 5, 6],
            count: 21,
            wait_sum_us: 21_000,
            total_sum_us: 210_000,
            ..WriteLockSnapshot::default()
        };
        refresh(
            Stats::default(),
            Stats::default(),
            Stats::default(),
            snapshot,
        );

        let text = exposition();
        // Wait distribution: cumulative 10/15/18/20/21/21, sum 0.021 s.
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_bucket{le="0.00001"} 10"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_bucket{le="0.0001"} 15"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_bucket{le="0.001"} 18"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_bucket{le="0.005"} 20"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_bucket{le="0.02"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_bucket{le="0.1"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_bucket{le="+Inf"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_sum 0.021"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_wait_seconds_count 21"#),
            "{text}"
        );
        // Total distribution: cumulative 0/1/3/6/10/15, +Inf 21, sum 0.21 s.
        assert!(
            text.contains(r#"tinio_storage_write_lock_hold_seconds_bucket{le="0.005"} 6"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_hold_seconds_bucket{le="+Inf"} 21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_hold_seconds_sum 0.21"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_storage_write_lock_hold_seconds_count 21"#),
            "{text}"
        );
    }

    #[test]
    fn recording_increments_counters() {
        let _window = test_lock::window();
        record_s3_operation("IncGetObject", 200, Duration::from_millis(3));

        let text = exposition();
        // Label-bearing counters are asserted with unique labels so parallel
        // tests cannot clobber each other (the shared default registry);
        // label-less gauges are covered by registers_all_families instead.
        assert!(
            text.contains(r#"tinio_s3_operations_total{op="IncGetObject",status="200"} 1"#),
            "{text}"
        );
    }

    #[test]
    #[cfg(feature = "multipart")]
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
        let mut buffer = itoa::Buffer::new();
        let status = status_label!(status, buffer);
        S3_OPERATIONS.with_label_values(&[op, status]).get()
    }

    #[test]
    fn metric_s3_records_status_classes() {
        let _window = test_lock::window();
        // The registry is process-global and a parallel probe may already have
        // recorded CreateBucket, so the assertions are deltas.
        let before_200 = s3_counter("CreateBucket", 200);
        let before_404 = s3_counter("CreateBucket", 404);
        let before_500 = s3_counter("CreateBucket", 500);
        let before_501 = s3_counter("CreateBucket", 501);
        // Ok → 200; a client error → its own status (NoSuchBucket → 404);
        // InternalError → 500; the trait default (NotImplemented) → 501.
        for (mode, _status) in [
            (FakeMode::Ok, 200),
            (FakeMode::NoSuchBucket, 404),
            (FakeMode::InternalError, 500),
            (FakeMode::NotImplemented, 501),
        ] {
            let backend = MetricS3::new(FakeS3 { mode });
            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(backend.create_bucket(request(CreateBucketInput::default())));
        }
        assert_eq!(s3_counter("CreateBucket", 200) - before_200, 1);
        assert_eq!(s3_counter("CreateBucket", 404) - before_404, 1);
        assert_eq!(s3_counter("CreateBucket", 500) - before_500, 1);
        assert_eq!(s3_counter("CreateBucket", 501) - before_501, 1);
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
        // The trio's counters are pinned here: this fake is the only one
        // answering `Ok` for them (the delegation test's answers
        // NotImplemented → 501), so the three 200 labels are this test's
        // own — parallel tests cannot clobber them.
        assert_eq!(s3_counter("CreateMultipartUpload", 200), 1);
        assert_eq!(s3_counter("CompleteMultipartUpload", 200), 1);
        assert_eq!(s3_counter("AbortMultipartUpload", 200), 1);
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
        // `SelectObjectContentInput` has no Default (its request carries
        // required fields) — built explicitly like the tagging input above.
        #[cfg(feature = "select")]
        {
            let _ = rt.block_on(backend.select_object_content(request(
                dto::SelectObjectContentInput {
                    bucket: "b".into(),
                    key: "k".into(),
                    expected_bucket_owner: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    request: dto::SelectObjectContentRequest {
                        expression: "SELECT * FROM S3Object s".into(),
                        expression_type: dto::ExpressionType::from("SQL".to_string()),
                        input_serialization: dto::InputSerialization {
                            csv: Some(dto::CSVInput::default()),
                            json: None,
                            parquet: None,
                            compression_type: None,
                        },
                        output_serialization: dto::OutputSerialization {
                            csv: Some(dto::CSVOutput::default()),
                            json: None,
                        },
                        request_progress: None,
                        scan_range: None,
                    },
                },
            )));
        }
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
                annotation_directive: None,
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
                if_match: None,
                if_none_match: None,
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
        // The CORS trio: `PutBucketCorsInput` has a required config — no
        // Default derive — built explicitly like the other tag inputs.
        #[cfg(feature = "cors")]
        {
            let _ = rt.block_on(backend.get_bucket_cors(request(dto::GetBucketCorsInput {
                bucket: "b".into(),
                expected_bucket_owner: None,
            })));
            let _ = rt.block_on(backend.put_bucket_cors(request(dto::PutBucketCorsInput {
                bucket: "b".into(),
                cors_configuration: dto::CORSConfiguration { cors_rules: vec![] },
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
            })));
            let _ = rt.block_on(
                backend.delete_bucket_cors(request(dto::DeleteBucketCorsInput {
                    bucket: "b".into(),
                    expected_bucket_owner: None,
                })),
            );
        }
        #[cfg(feature = "acl")]
        {
            call!(get_bucket_acl, dto::GetBucketAclInput);
            call!(put_bucket_acl, dto::PutBucketAclInput);
            call!(get_object_acl, dto::GetObjectAclInput);
            call!(put_object_acl, dto::PutObjectAclInput);
        }
        #[cfg(feature = "versioning")]
        {
            call!(get_bucket_versioning, dto::GetBucketVersioningInput);
            call!(list_object_versions, dto::ListObjectVersionsInput);
            // `PutBucketVersioningInput` has a required configuration — no
            // Default derive — built explicitly like the tagging inputs.
            let _ = rt.block_on(backend.put_bucket_versioning(request(
                dto::PutBucketVersioningInput {
                    bucket: "b".into(),
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                    mfa: None,
                    versioning_configuration: dto::VersioningConfiguration {
                        status: None,
                        mfa_delete: None,
                    },
                },
            )));
        }
        let expected: Vec<&str> = {
            let base = [
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
            ]
            .into_iter();
            #[cfg(feature = "acl")]
            let base = base.chain([
                "GetBucketAcl",
                "PutBucketAcl",
                "GetObjectAcl",
                "PutObjectAcl",
            ]);
            #[cfg(feature = "multipart")]
            let base = base.chain(["UploadPart", "ListParts", "ListMultipartUploads"]);
            #[cfg(feature = "cors")]
            let base = base.chain(["GetBucketCors", "PutBucketCors", "DeleteBucketCors"]);
            #[cfg(feature = "versioning")]
            let base = base.chain([
                "GetBucketVersioning",
                "PutBucketVersioning",
                "ListObjectVersions",
            ]);
            base.collect()
        };
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
        #[cfg(feature = "select")]
        assert_eq!(s3_counter("SelectObjectContent", 501), 1);
    }

    /// A probe input for one table row.
    ///
    /// Most inputs derive `Default`; the ones that do not — their required
    /// fields are non-optional — are built through s3s's own builders with
    /// empty required values. Any other input without `Default` fails to
    /// compile here instead of going quietly undriven.
    macro_rules! probe_input {
        (CopyObjectInput) => {
            dto::CopyObjectInput::builder()
                .bucket(String::new())
                .key(String::new())
                .copy_source(dto::CopySource::parse("src/key").expect("a copy source"))
                .build()
                .expect("a probe input")
        };
        (UploadPartCopyInput) => {
            dto::UploadPartCopyInput::builder()
                .bucket(String::new())
                .key(String::new())
                .copy_source(dto::CopySource::parse("src/key").expect("a copy source"))
                .part_number(1)
                .upload_id(String::new())
                .build()
                .expect("a probe input")
        };
        (SelectObjectContentInput) => {
            dto::SelectObjectContentInput::builder()
                .bucket(String::new())
                .key(String::new())
                .request(dto::SelectObjectContentRequest {
                    expression: String::new(),
                    expression_type: dto::ExpressionType::from(String::new()),
                    input_serialization: Default::default(),
                    output_serialization: Default::default(),
                    request_progress: None,
                    scan_range: None,
                })
                .build()
                .expect("a probe input")
        };
        (PutBucketTaggingInput) => {
            dto::PutBucketTaggingInput::builder()
                .bucket(String::new())
                .tagging(Default::default())
                .build()
                .expect("a probe input")
        };
        (PutBucketCorsInput) => {
            dto::PutBucketCorsInput::builder()
                .bucket(String::new())
                .cors_configuration(Default::default())
                .build()
                .expect("a probe input")
        };
        (DeleteObjectsInput) => {
            dto::DeleteObjectsInput::builder()
                .bucket(String::new())
                .delete(Default::default())
                .build()
                .expect("a probe input")
        };
        (PutObjectTaggingInput) => {
            dto::PutObjectTaggingInput::builder()
                .bucket(String::new())
                .key(String::new())
                .tagging(Default::default())
                .build()
                .expect("a probe input")
        };
        (PutBucketVersioningInput) => {
            dto::PutBucketVersioningInput::builder()
                .bucket(String::new())
                .versioning_configuration(Default::default())
                .build()
                .expect("a probe input")
        };
        ($other:ident) => {
            dto::$other::default()
        };
    }

    /// The probe inner backend: every row answers the same 400, so the span
    /// is the subject and the status label stays off the families the
    /// exact-value tests pin.
    #[derive(Debug, Clone, Copy)]
    struct ProbeFake;

    macro_rules! probe_fake {
        (
            read { $( $(#[handler($rop:ident)])? $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
            write { $( $(#[handler($wop:ident)])? $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
        ) => {
            #[async_trait]
            impl S3 for ProbeFake {
                $(
                    $( $( $(#[cfg(feature = $rf)])? )+ )?
                    async fn $r(
                        &self,
                        _req: S3Request<dto::$ri>,
                    ) -> S3Result<S3Response<dto::$ro>> {
                        Err(s3_error!(InvalidArgument, "span probe"))
                    }
                )*
                $(
                    $( $( $(#[cfg(feature = $wf)])? )+ )?
                    async fn $w(
                        &self,
                        _req: S3Request<dto::$wi>,
                    ) -> S3Result<S3Response<dto::$wo>> {
                        Err(s3_error!(InvalidArgument, "span probe"))
                    }
                )*
            }
        };
    }
    tinio_s3_ops!(probe_fake);

    macro_rules! span_probe {
        (
            read { $( $(#[handler($rop:ident)])? $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
            write { $( $(#[handler($wop:ident)])? $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
        ) => {
            /// Every row this build compiles, as (table name, wire name).
            fn probe_rows() -> Vec<(&'static str, &'static str)> {
                vec![
                    $(
                        $( $( $(#[cfg(feature = $rf)])? )+ )?
                        (stringify!($r), wire_name!($ri)),
                    )*
                    $(
                        $( $( $(#[cfg(feature = $wf)])? )+ )?
                        (stringify!($w), wire_name!($wi)),
                    )*
                ]
            }

            /// Drive one row through `backend`, by its table name.
            async fn call_probe<T: S3>(backend: &T, op: &str) -> S3Result<()> {
                match op {
                    $(
                        $( $( $(#[cfg(feature = $rf)])? )+ )?
                        stringify!($r) => {
                            backend.$r(request(probe_input!($ri))).await.map(|_| ())
                        }
                    )*
                    $(
                        $( $( $(#[cfg(feature = $wf)])? )+ )?
                        stringify!($w) => {
                            backend.$w(request(probe_input!($wi))).await.map(|_| ())
                        }
                    )*
                    other => panic!("no table row named {other}"),
                }
            }
        };
    }
    tinio_s3_ops!(span_probe);

    /// The dispatch opens one span per operation, named and targeted before
    /// the handler runs; the table is the operation list, so a row added
    /// without its span fails here.
    #[test]
    fn every_operation_opens_exactly_one_s3_span() {
        let capture = spans::Capture::new();
        with_default(registry().with(capture.clone()), || {
            // Interest is process-wide: another test may have cached it with
            // no recorder installed, so rebuild it under this thread's.
            rebuild_interest_cache();
            let runtime = Runtime::new().unwrap();
            let backend = MetricS3::new(ProbeFake);
            for (row, wire) in probe_rows() {
                capture.clear();
                let result = runtime.block_on(call_probe(&backend, row));
                assert!(result.is_err(), "{row} is a probe, not an answer");
                let spans = capture.named("tinio.s3.operation");
                assert_eq!(spans.len(), 1, "{row}: {spans:?}");
                assert_eq!(spans[0].target, "tinio::s3", "{row}");
                assert_eq!(spans[0].level, Level::INFO, "{row}");
                assert_eq!(spans[0].field("op"), Some(wire), "{row}");
            }
        });
    }

    /// A T0 handler fills the identity the decorator left empty: one span,
    /// carrying the bucket and key the request named.
    #[test]
    fn put_object_records_its_identity_on_the_operation_span() {
        let capture = spans::Capture::new();
        with_default(registry().with(capture.clone()), || {
            rebuild_interest_cache();
            let runtime = Runtime::new().unwrap();
            let backend = MetricS3::new(S3Backend::new(
                MemoryStorage::new().unwrap(),
                Default::default(),
            ));
            let _ = runtime.block_on(backend.put_object(request(dto::PutObjectInput {
                bucket: "probe-bucket".into(),
                key: "probe/key.txt".into(),
                ..Default::default()
            })));
            let spans = capture.named("tinio.s3.operation");
            assert_eq!(spans.len(), 1, "{spans:?}");
            assert_eq!(spans[0].field("op"), Some("PutObject"));
            assert_eq!(spans[0].field("bucket"), Some("probe-bucket"));
            assert_eq!(spans[0].field("key"), Some("probe/key.txt"));
        });
    }
}

/// A delegation wrapper recording `tinio_s3_operations_total{op,status}`
/// plus duration for every implemented operation, and the in-progress
/// multipart gauge (task T054). The status label is the real HTTP status
/// s3s puts on the wire, `err.status_code().unwrap_or(500)`, with one gap:
/// the Ok arm books `200`, because s3s's generated ops discard
/// `S3Response.status` and re-serialize from the output, so a ranged
/// `GetObject` is 206 on the wire and unreachable at this layer.
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

    /// Record one operation: `op` label, HTTP status, duration — and the one span every
    /// operation shares, because this is the only place all 42 of them pass through.
    ///
    /// `bucket` and `key` are declared empty here and filled by the handler that knows them:
    /// a T0 operation records its object identity without a second span per handler.
    async fn record<R>(
        &self,
        op: &'static str,
        fut: impl Future<Output = S3Result<R>> + Send,
    ) -> S3Result<R> {
        let start = Instant::now();
        let result = s3_operation(op, fut).await;
        let status = match &result {
            Ok(_) => 200u16,
            Err(err) => {
                record_s3_error(op, err.code());
                err.status_code().map_or(500, |code| code.as_u16())
            }
        };
        record_s3_operation(op, status, start.elapsed());
        result
    }
}

#[instrument(
    level = "info",
    target = "tinio::s3",
    name = "tinio.s3.operation",
    skip(work),
    fields(op, bucket = field::Empty, key = field::Empty)
)]
async fn s3_operation<R>(
    op: &'static str,
    work: impl Future<Output = S3Result<R>> + Send,
) -> S3Result<R> {
    tracing::Span::current().record("op", op);
    work.await
}

/// The input dto's name minus `Input` — the AWS operation name s3s's
/// codegen gives the pair, and the metric label.
macro_rules! wire_name {
    ($i:ident) => {
        &stringify!($i)[..stringify!($i).len() - "Input".len()]
    };
}
pub(crate) use wire_name;

/// The metrics decorator's `S3` impl, expanded from the shared table
/// ([`crate::backend::ops`]): every operation routes through
/// [`MetricS3::record`] under its derived wire name, and the three multipart
/// operations carry their in-progress gauge effect in the table's
/// `#[on_ok(...)]` clause.
///
/// The label is the input dto's name minus `Input` — s3s's codegen names
/// each dto pair after the operation, so `CreateMultipartUploadInput` labels
/// `CreateMultipartUpload`. Every one of the 39 labels is pinned by a
/// counter: `metric_s3_records_every_delegated_operation` asserts 35 at 501
/// (its feature-gated rows under their own `#[cfg]`),
/// `metric_s3_records_status_classes` pins `CreateBucket`, and
/// `metric_s3_maintains_multipart_gauge` pins the multipart trio's 200s
/// beside the gauge — no label is recorded-but-unasserted.
macro_rules! metric_s3_impl {
    (
        read { $( $(#[handler($rop:ident)])? $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[handler($wop:ident)])? $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        #[async_trait]
        impl<T: S3 + Send + Sync> S3 for MetricS3<T> {
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                async fn $r(&self, req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                    let out = self.record(wire_name!($ri), self.inner.$r(req)).await;
                    $( if out.is_ok() { $rh } )?
                    out
                }
            )*
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                async fn $w(&self, req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                    let out = self.record(wire_name!($wi), self.inner.$w(req)).await;
                    $( if out.is_ok() { $wh } )?
                    out
                }
            )*
        }
    };
}

tinio_s3_ops!(metric_s3_impl);
