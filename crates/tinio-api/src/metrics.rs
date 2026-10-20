//! `GET /metrics`: the process registry, gathered and encoded.
//!
//! The registry is process-global (`prometheus::default_registry`): every crate that declares a
//! family registers it there, and the HTTP families are the only ones this module declares. A
//! scrape is therefore a gather and an encode — no storage, no injected source, and nothing on a
//! request's path beyond the registry's own size. The data-plane families are declared by the
//! crates that record them, which this crate must not depend on (the edge would be a dependency
//! cycle against `tinio-server`'s optional `tinio-api`); a global registry is what lets the two
//! sides meet without one.
//!
//! **The refresh is not here.** The cheap half (the pipeline gauges and the write-lock
//! histograms) and the expensive half (the storage scan and the three gauges it sets) refresh on
//! one reader task the composition root owns — `tinio-cli`, the only crate that can read the
//! served backend and name `tinio-server`'s statics — on the same `ttl` ([`DEFAULT_TTL`]), off
//! every request's path. A scrape only gathers what the last tick published, so its cost is
//! independent of the root's size and of how often the endpoint is hit (`FR-019`, `SC-008`;
//! an unauthenticated route that scanned would be a small denial of service on every runtime
//! worker).
//!
//! **Freshness is per-`ttl`, not per-scrape**: the numbers a scrape shows are as old as the
//! reader's last tick, and the design's freshness change is noted in `research.md` §9. The
//! reader stays single-flight by construction — one task, scanning sequentially — so a scrape
//! flood cannot multiply scans, and a stalled scan cannot fail a request.

use std::{sync::LazyLock, time::Duration};

use prometheus::{
    Encoder, HistogramVec, IntCounterVec, IntGauge, TextEncoder, default_registry,
    register_histogram_vec, register_int_counter_vec, register_int_gauge,
};
use tinio_util::{metrics::RegisteredExt, status_label};

use crate::Error;

/// HTTP requests served by the management plane, by method and status.
pub(crate) static HTTP_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_http_requests_total",
        "HTTP requests served by the management plane, by method and status",
        &["method", "status"]
    )
    .registered("tinio_http_requests_total")
});
/// HTTP request durations, by method.
pub(crate) static HTTP_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "tinio_http_request_duration_seconds",
        "HTTP request durations, by method",
        &["method"]
    )
    .registered("tinio_http_request_duration_seconds")
});
/// HTTP requests currently being served.
pub(crate) static HTTP_IN_FLIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_http_in_flight",
        "HTTP requests currently being served"
    )
    .registered("tinio_http_in_flight")
});

/// The six verbs a management transport serves; any other token collapses to
/// one label so a client cannot grow the families.
fn method_label(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "PUT" => "PUT",
        "POST" => "POST",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    }
}

/// Record a completed management request. The method is collapsed by
/// [`method_label`] before it becomes a label, so an arbitrary request token
/// books `OTHER` rather than a new series.
pub(crate) fn record_http_request(method: &str, status: u16, duration: Duration) {
    let method = method_label(method);
    let mut buffer = itoa::Buffer::new();
    let status = status_label!(status, buffer);
    HTTP_REQUESTS.with_label_values(&[method, status]).inc();
    HTTP_DURATION
        .with_label_values(&[method])
        .observe(duration.as_secs_f64());
}

/// Holds one in-flight request, and releases the gauge on drop — normal
/// completion and cancellation (a dropped response future) both decrement,
/// so a client disconnect cannot leak the count.
pub(crate) struct InFlightGuard;

impl InFlightGuard {
    /// Take the in-flight gauge for one request.
    pub(crate) fn new() -> Self {
        HTTP_IN_FLIGHT.inc();
        Self
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        HTTP_IN_FLIGHT.dec();
    }
}

/// The ttl between the reader's refresh cycles: one cadence for the pipeline gauges, the
/// write-lock histograms and the storage scan, and therefore how old a scrape's numbers may be.
///
/// It lives here because this is the metrics surface's module and the reader's caller reads the
/// constant from it; the task itself is the composition root's.
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

/// Encodes the default registry in the Prometheus text format — the whole of what a scrape runs.
///
/// Every family every crate registered is gathered and encoded; nothing is read from storage and
/// nothing is awaited, so the cost is the registry's size and a stalled reader cannot reach the
/// request at all. A registry with no samples yet is an empty body, not an error.
///
/// # Examples
///
/// ```rust
/// use tinio_api::metrics::render;
///
/// let text = render().expect("the registry encodes");
/// assert!(text.is_empty() || text.ends_with('\n'), "{text}");
/// ```
pub fn render() -> Result<String, Error> {
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&default_registry().gather(), &mut buffer)
        .map_err(|err| Error::Internal(format!("metrics encoding: {err}")))?;
    String::from_utf8(buffer).map_err(|err| Error::Internal(format!("metrics encoding: {err}")))
}

#[cfg(test)]
mod tests {
    use tinio_util::testing::metrics::{assert_registered, exposition};

    use super::*;

    /// The per-crate gate: every family this module declares is registered once.
    #[test]
    fn the_three_http_families_register_once() {
        let _in_flight = InFlightGuard::new();
        record_http_request("GET", 596, Duration::from_millis(1));
        assert_registered(&[
            "tinio_http_requests_total",
            "tinio_http_request_duration_seconds",
            "tinio_http_in_flight",
        ]);
    }

    #[test]
    fn unknown_methods_share_one_label() {
        assert_eq!(method_label("FAM"), "OTHER");
        assert_eq!(method_label("PATCH"), "OTHER");
        assert_eq!(method_label("GET"), "GET");
        assert_eq!(method_label("OPTIONS"), "OPTIONS");
    }

    #[test]
    fn record_http_request_books_the_families() {
        // The status 598 is this test's alone, so the exact count cannot race
        // the other tests writing the shared default registry.
        record_http_request("GET", 598, Duration::from_millis(1));
        let text = exposition();
        assert!(
            text.contains(r#"tinio_http_requests_total{method="GET",status="598"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"tinio_http_request_duration_seconds_count{method="GET"}"#),
            "{text}"
        );
    }

    #[test]
    fn render_encodes_the_families_a_scrape_serves() {
        // The status 597 is this test's alone, for the same reason as above.
        record_http_request("GET", 597, Duration::from_millis(1));
        let text = render().expect("the registry encodes");
        assert!(
            text.contains(r#"tinio_http_requests_total{method="GET",status="597"} 1"#),
            "{text}"
        );
    }
}
