//! The lifecycle pass families: one pass histogram, one per-action counter.
//!
//! `specs/001-s3-local-server/data-model.md` §Metrics owns the names; this
//! module is their one registration site. Labels are closed sets — `kind` is
//! the action (`object`, `version`, `marker`, `upload`) and `outcome` is
//! `ok`/`error` — so no bucket, key or upload id can become a series.
//!
//! The registry is process-global: the crate that renders `/metrics` gathers
//! these families without this crate knowing it. `Summary::errors` is what a
//! pass prints; the counter is what an operator pages on.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use prometheus::{Histogram, IntCounterVec, register_histogram, register_int_counter_vec};
use tinio_util::metrics::{PASS_DURATION_BUCKETS, RegisteredExt};
#[cfg(test)]
use tinio_util::testing::metrics::{counter, test_lock};

/// One pass's wall-clock cost.
static PASS_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "tinio_lifecycle_pass_duration_seconds",
        "Lifecycle pass durations",
        PASS_DURATION_BUCKETS.to_vec()
    )
    .registered("tinio_lifecycle_pass_duration_seconds")
});

/// One action's outcome, by action kind.
static ACTIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_lifecycle_actions_total",
        "Lifecycle actions by kind and outcome",
        &["kind", "outcome"]
    )
    .registered("tinio_lifecycle_actions_total")
});

/// The action a sample belongs to — a closed label domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A current version expired: an unlink, or a delete marker in a
    /// versioned bucket.
    Object,
    /// A noncurrent version removed permanently.
    Version,
    /// A delete marker that was its key's only version.
    Marker,
    /// An incomplete multipart upload aborted.
    Upload,
}

impl Kind {
    /// The label value, without allocating.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Version => "version",
            Self::Marker => "marker",
            Self::Upload => "upload",
        }
    }
}

/// Record one completed pass's duration.
pub(crate) fn record_pass_duration(duration: Duration) {
    #[cfg(test)]
    let _g = test_lock::writer();
    PASS_DURATION.observe(duration.as_secs_f64());
}

/// A guard that records one pass's duration on drop, so every exit path — including an early
/// return — books exactly one sample.
#[must_use = "the sample is recorded on drop"]
pub(crate) struct PassTimer(Instant);

impl PassTimer {
    /// Start timing one pass.
    pub(crate) fn new() -> Self {
        Self(Instant::now())
    }
}

impl Drop for PassTimer {
    fn drop(&mut self) {
        record_pass_duration(self.0.elapsed());
    }
}

/// Record one action's outcome: `ok` when it did what it wanted (an item
/// already gone included), `error` when the store refused or failed it.
pub(crate) fn record_action(kind: Kind, ok: bool) {
    #[cfg(test)]
    let _g = test_lock::writer();
    ACTIONS
        .with_label_values(&[kind.as_str(), if ok { "ok" } else { "error" }])
        .inc();
}

/// One action series' current value, read back out of the default registry;
/// zero before the first sample created the series.
#[cfg(test)]
pub(crate) fn action_count(kind: Kind, outcome: &str) -> f64 {
    counter(
        "tinio_lifecycle_actions_total",
        &[("kind", kind.as_str()), ("outcome", outcome)],
    )
}

#[cfg(test)]
mod tests {

    use tinio_util::testing::metrics::{assert_registered, exposition};

    use super::*;

    #[test]
    fn the_lifecycle_families_render_their_recorded_values() {
        let _guard = test_lock::window();
        record_pass_duration(Duration::from_millis(250));
        for kind in [Kind::Object, Kind::Version, Kind::Marker, Kind::Upload] {
            record_action(kind, true);
            record_action(kind, false);
        }

        assert_registered(&[
            "tinio_lifecycle_pass_duration_seconds",
            "tinio_lifecycle_actions_total",
        ]);
        let text = exposition();
        assert!(
            text.contains("tinio_lifecycle_pass_duration_seconds_count"),
            "the pass histogram is declared and records a sample: {text}"
        );
        assert!(
            text.contains("tinio_lifecycle_pass_duration_seconds_sum"),
            "the pass histogram records its seconds: {text}"
        );
        for kind in ["object", "version", "marker", "upload"] {
            for outcome in ["ok", "error"] {
                let series = format!(
                    r#"tinio_lifecycle_actions_total{{kind="{kind}",outcome="{outcome}"}}"#
                );
                assert!(text.contains(&series), "missing {series}: {text}");
            }
        }
    }
}
