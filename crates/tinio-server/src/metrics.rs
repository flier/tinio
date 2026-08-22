//! Prometheus registry and metric families (task T023).
//!
//! The three metric layers of the data model (data-model.md Metrics):
//! HTTP (`tinio_http_*`), S3 operations (`tinio_s3_*`), and storage
//! (`tinio_storage_*`). Families are process-wide globals, registered
//! once on the default registry via `register_*!`. The storage-layer
//! full-scan gauges are computed (with a 30 s TTL cache) by the
//! management plane later (T075).
//!
//! # Examples
//!
//! ```rust
//! use std::time::Duration;
//! use tinio_server::metrics::{
//!     record_http_request, record_s3_operation, STORAGE_BUCKETS,
//! };
//!
//! record_http_request("GET", 200, Duration::from_millis(3));
//! record_s3_operation("GetObject", 200, Duration::from_millis(5));
//! STORAGE_BUCKETS.set(2);
//! assert!(!prometheus::default_registry().gather().is_empty());
//! ```

use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::{
    HistogramVec, IntCounter, IntCounterVec, IntGauge, register_histogram_vec,
    register_int_counter, register_int_counter_vec, register_int_gauge,
};

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
    record(
        &S3_OPERATIONS,
        &[op, &status.to_string()],
        &S3_DURATION,
        op,
        duration,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Encoder;

    #[test]
    fn registers_all_families() {
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
        let names: Vec<String> = prometheus::default_registry()
            .gather()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        // The tinio_* family set must be exactly the 13 spec'd names
        // (data-model.md Metrics) — a 14th family would fail this equality.
        let expected: std::collections::HashSet<&str> = [
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
        ]
        .into_iter()
        .collect();
        let actual: std::collections::HashSet<&str> = names
            .iter()
            .filter(|n| n.starts_with("tinio_"))
            .map(|n| n.as_str())
            .collect();
        assert_eq!(actual, expected, "tinio_* family set");
    }

    #[test]
    fn recording_increments_counters() {
        record_http_request("INC", 200, Duration::from_millis(2));
        record_http_request("INC", 200, Duration::from_millis(1));
        record_s3_operation("IncGetObject", 200, Duration::from_millis(3));

        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&prometheus::default_registry().gather(), &mut buf)
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
}
