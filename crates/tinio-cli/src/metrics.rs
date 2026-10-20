//! The CLI's one metric family: how long each startup phase takes.
//!
//! **Startup cost is the one thing a run cannot be diagnosed from afterwards.** A start that
//! takes eleven seconds and one that takes eighty milliseconds leave the same server behind, and
//! the operator's next question — was it the resolution, the configuration, the bind, or the
//! repair? — has no answer in the log. The histogram is that answer, one series per phase.
//!
//! **Only the serving path records.** `stop`, `status`, `doctor` and the lifecycle verbs are
//! one-shot clients with no `/metrics` endpoint to scrape, and the shutdown is deliberately
//! deferred (`specs/001-s3-local-server/data-model.md` §Metrics): the plane serving the scrape
//! is the thing being drained, so the last increment may never be read.
//!
//! **The label is a closed set** — [`Phase`]'s five variants, the `phase` domain
//! `data-model.md` §Metrics names. No path, configuration text, token or flag has a route into
//! it: [`Phase`] carries no field, and the family carries nothing else.
//!
//! The registry is process-global, so the management plane renders this family without this
//! module knowing it; [`record`] is the only write path.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use prometheus::{HistogramVec, register_histogram_vec};
use tinio_util::metrics::RegisteredExt;

/// The startup phases `tinio_cli_startup_seconds` labels — the `phase` domain's whole set.
///
/// **The variants are the chain's own steps, in the order a run reaches them**: the resolution
/// (root, mode, state dir and configuration document), the configuration-derived static
/// determinations, the control channel's bind, the startup repair, and readiness. The serving
/// path's data-plane bind is a `Bind` sample too — a start binds two channels — and a phase
/// that a one-shot verb never reaches is still a value the family may hold, because the domain
/// is the run's and not one command's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// `resolve::server`/`resolve::server_confined`: root, mode, state dir, configuration load
    /// and the flag fold.
    Resolve,
    /// The configuration-derived determinations `start::run` makes before it creates anything:
    /// the sandbox plan, the pg-under-confinement refusal and the credential policy.
    Config,
    /// A control or data channel's bind.
    Bind,
    /// The startup repair, post-bind.
    Repair,
    /// The window between the data-plane bind and the readiness report.
    Ready,
}

impl Phase {
    /// The `phase` label's value — the closed domain `data-model.md` §Metrics names.
    const fn label(self) -> &'static str {
        match self {
            Self::Resolve => "resolve",
            Self::Config => "config",
            Self::Bind => "bind",
            Self::Repair => "repair",
            Self::Ready => "ready",
        }
    }
}

/// One phase's duration bounds, in seconds: a resolution on a warm cache at one end and the
/// ten-to-thirty-second wait a first start can pay while a state database or a pg pool comes up
/// at the other.
const STARTUP_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// The startup cost by phase — `data-model.md` §Metrics' family 24.
static STARTUP_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "tinio_cli_startup_seconds",
        "Startup duration by phase",
        &["phase"],
        STARTUP_BUCKETS.to_vec(),
    )
    .registered("tinio_cli_startup_seconds")
});

/// A phase's clock: one sample on drop.
///
/// **The guard is what makes a refused phase visible.** Every phase in [`Phase`] ends on a
/// fallible call, and an early `?` would otherwise leave the family silent about the cost the
/// refusal had already paid — exactly the run an operator most wants the number for. The timer
/// therefore records in `Drop`, whether the scope ended in `Ok`, in `Err` or by an early return.
pub(crate) struct Timer {
    phase: Phase,
    started: Instant,
}

impl Timer {
    /// Starts a phase's clock.
    pub(crate) fn new(phase: Phase) -> Self {
        Self {
            phase,
            started: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        record(self.phase, self.started.elapsed());
    }
}

/// Records one phase's duration: the family's only write path.
pub(crate) fn record(phase: Phase, elapsed: Duration) {
    STARTUP_SECONDS
        .with_label_values(&[phase.label()])
        .observe(elapsed.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use prometheus::{default_registry, proto::MetricType};
    use tinio_util::testing::metrics::{assert_registered, exposition, histogram_count, test_lock};

    use super::{Phase, Timer, record};

    /// One phase's sample count, read back out of the default registry; zero before the first
    /// sample created the series.
    fn count(phase: Phase) -> f64 {
        histogram_count("tinio_cli_startup_seconds", &[("phase", phase.label())])
    }

    /// The family is registered on the default registry, is a histogram, and its only label is
    /// the closed `phase` set: one series per value, never a second label.
    #[test]
    fn the_startup_family_renders_every_phase_as_its_own_series() {
        let _window = test_lock::window();
        let before = count(Phase::Resolve);
        record(Phase::Resolve, Duration::from_millis(3));
        assert_registered(&["tinio_cli_startup_seconds"]);
        assert_eq!(
            count(Phase::Resolve) - before,
            1.0,
            "one record is one sample"
        );

        for phase in [
            Phase::Resolve,
            Phase::Config,
            Phase::Bind,
            Phase::Repair,
            Phase::Ready,
        ] {
            record(phase, Duration::ZERO);
            let series = format!(
                r#"tinio_cli_startup_seconds_count{{phase="{}"}}"#,
                phase.label()
            );
            assert!(
                exposition().contains(&series),
                "missing {series} in the gathered exposition"
            );
        }

        let family = default_registry()
            .gather()
            .into_iter()
            .find(|family| family.name() == "tinio_cli_startup_seconds")
            .expect("the startup family is registered");
        assert_eq!(family.get_field_type(), MetricType::HISTOGRAM);
        assert!(
            family.get_metric().iter().all(|metric| {
                metric.get_label().len() == 1 && metric.get_label()[0].name() == "phase"
            }),
            "phase is the histogram's only label"
        );
    }

    /// The timer writes its phase on drop, so a scope that returns early (an operator's refusal)
    /// still leaves one sample behind.
    #[test]
    fn the_timer_records_the_scope_it_was_dropped_in() {
        let _window = test_lock::window();
        let before = count(Phase::Repair);
        {
            let _timer = Timer::new(Phase::Repair);
        }
        assert_eq!(
            count(Phase::Repair) - before,
            1.0,
            "dropping the guard records one sample"
        );
    }
}
