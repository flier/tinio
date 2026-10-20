//! The confinement families: how often a launch confined, and how long it took.
//!
//! **The count and the duration are one decision's two halves.** A confinement is a security
//! decision this process takes about itself (Linux) or about a child it starts (macOS, Windows),
//! and it is the first thing a run does — before the logging stack is fully in place — so the
//! metric is the carrier that does not depend on where in the startup ordering a span lands.
//! Both families are recorded at the one applicator, `serve`'s `apply_platform`, where the
//! mechanism and the outcome are both in hand.
//!
//! **The labels are closed sets.** `mechanism` is a variant of [`Mechanism`], one per mechanism
//! this build's target can apply; `outcome` is `applied` or `refused`. No exposure, path,
//! token or configuration value has a route into either: neither type has a field that can hold
//! one, and the families carry nothing else.
//!
//! The registry is process-global, so the management plane renders these without this crate
//! knowing it; the record call is the only write path.

#![expect(
    clippy::expect_used,
    reason = "argued: every error a registration can answer is about this family's own name (already registered, or registered with a different label set or help), from one `LazyLock` site a process runs once, against the family list `specs/001-s3-local-server/data-model.md` owns; a collision is a source-level mistake the registry refuses here, not a runtime condition, and `LazyLock::new` has no `Result` to return it through (design 2026-10-09 D2d). The module's tests read both names back out of the gathered exposition, so a registration that did not land fails a test."
)]

use std::{sync::LazyLock, time::Duration};

use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};

#[cfg(target_os = "linux")]
use crate::_linux::Mechanism as LinuxMechanism;
#[cfg(target_os = "macos")]
use crate::_macos::Mechanism as MacosMechanism;

/// The mechanism a confinement attempted or applied — the `mechanism` label's whole domain.
///
/// **One variant per value the build can produce, and the `#[cfg]` is the platform fact**:
/// Linux resolves a model before it confines, macOS runs one of two mechanisms, and Windows'
/// AppContainer is the only one it has. A target with no backend answers [`Self::None`], which
/// is the refusal rather than a mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mechanism {
    /// Linux's `Auto` request, recorded on a refusal taken before resolution chose a model.
    #[cfg(target_os = "linux")]
    Auto,
    /// Linux's unprivileged user namespace.
    #[cfg(target_os = "linux")]
    UserNamespace,
    /// Linux's real root's `CAP_SYS_ADMIN`.
    #[cfg(target_os = "linux")]
    Root,
    /// macOS's `sandbox-exec` helper.
    #[cfg(target_os = "macos")]
    SandboxExec,
    /// macOS's in-process `sandbox_init` apply.
    #[cfg(target_os = "macos")]
    SandboxInit,
    /// Windows' AppContainer profile.
    #[cfg(target_os = "windows")]
    AppContainer,
    /// A target this build wires no backend for: the run is refused and nothing confined it.
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    None,
}

impl Mechanism {
    /// The `mechanism` label's value.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            #[cfg(target_os = "linux")]
            Self::Auto => "auto",
            #[cfg(target_os = "linux")]
            Self::UserNamespace => "user-namespace",
            #[cfg(target_os = "linux")]
            Self::Root => "root",
            #[cfg(target_os = "macos")]
            Self::SandboxExec => "sandbox-exec",
            #[cfg(target_os = "macos")]
            Self::SandboxInit => "sandbox-init",
            #[cfg(target_os = "windows")]
            Self::AppContainer => "appcontainer",
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Self::None => "none",
        }
    }
}

/// What the applicator's call to the backend ended on — the `outcome` label's whole domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The backend confined the process or launched the child.
    Applied,
    /// The backend refused: nothing was confined, and the caller's `?` carries the reason.
    Refused,
}

impl Outcome {
    /// The `outcome` label's value.
    const fn label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Refused => "refused",
        }
    }
}

/// The Linux request or resolved model, as the label's value.
///
/// `Auto` is the one request that is not a mechanism: it is recorded only where the run was
/// refused before `enter` resolved it, which is why the family's domain carries it.
#[cfg(target_os = "linux")]
impl From<LinuxMechanism> for Mechanism {
    fn from(model: LinuxMechanism) -> Self {
        match model {
            LinuxMechanism::Auto => Self::Auto,
            LinuxMechanism::UserNamespace => Self::UserNamespace,
            LinuxMechanism::Root => Self::Root,
        }
    }
}

/// The macOS mechanism, as the label's value: the platform has no `Auto`, so the request and
/// the answer are one value.
#[cfg(target_os = "macos")]
impl From<MacosMechanism> for Mechanism {
    fn from(mechanism: MacosMechanism) -> Self {
        match mechanism {
            MacosMechanism::SandboxExec => Self::SandboxExec,
            MacosMechanism::SandboxInit => Self::SandboxInit,
        }
    }
}

/// Confinements by mechanism and outcome.
static CONFINEMENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_sandbox_confinement_total",
        "Confinements by mechanism and outcome",
        &["mechanism", "outcome"]
    )
    .expect("register tinio_sandbox_confinement_total")
});

/// The confinement's own duration bounds: a millisecond-scale in-place jail at one end and the
/// ten-to-twenty-second ACL grant a first Windows launch pays at the other.
const CONFINEMENT_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0];

/// The confinement's own duration, by mechanism — the startup budget a refusal or a slow grant
/// spends, which the process's uptime cannot recover once the run is served.
static CONFINEMENT_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "tinio_sandbox_confinement_seconds",
        "Confinement duration by mechanism",
        &["mechanism"],
        CONFINEMENT_BUCKETS.to_vec(),
    )
    .expect("register tinio_sandbox_confinement_seconds")
});

/// Record one confinement: the mechanism attempted (or applied), how it ended, and how long the
/// one backend call took.
pub(crate) fn record(mechanism: Mechanism, outcome: Outcome, elapsed: Duration) {
    #[cfg(test)]
    let _writer = test_lock::writer();
    let mechanism = mechanism.label();
    CONFINEMENTS
        .with_label_values(&[mechanism, outcome.label()])
        .inc();
    CONFINEMENT_SECONDS
        .with_label_values(&[mechanism])
        .observe(elapsed.as_secs_f64());
}

/// The metric statics are process-global (the prometheus default registry), so an exact-value
/// test races every other test's writes. The serialization is a cfg(test)-only writer lock, the
/// shape `tinio-server`'s metrics module uses: every write takes it, and an exact-assert test
/// takes the window for its whole assert region. Production call sites compile the lock away.
#[cfg(test)]
pub(crate) use tinio_util::testing::metrics::test_lock;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use prometheus::{default_registry, proto::MetricType};

    use super::{Mechanism, Outcome, record, test_lock};

    /// A mechanism this target can construct: the family's set is per-platform, and each arm is
    /// the one the platform's confinement resolves to.
    const fn sample() -> Mechanism {
        #[cfg(target_os = "linux")]
        {
            Mechanism::UserNamespace
        }
        #[cfg(target_os = "macos")]
        {
            Mechanism::SandboxExec
        }
        #[cfg(target_os = "windows")]
        {
            Mechanism::AppContainer
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            Mechanism::None
        }
    }

    /// The exposition the management plane serves, as the CLI's adapter produces it.
    use tinio_util::testing::metrics::{assert_registered, counter, exposition};

    /// The counter's current value for one label pair, read out of the default registry.
    fn count(mechanism: Mechanism, outcome: Outcome) -> f64 {
        counter(
            "tinio_sandbox_confinement_total",
            &[
                ("mechanism", mechanism.label()),
                ("outcome", outcome.label()),
            ],
        )
    }

    /// A recorded refusal moves the one series it names, and the duration family carries the same
    /// mechanism as its only label.
    #[test]
    fn a_refusal_is_one_counter_sample_and_one_duration() {
        let _window = test_lock::window();
        let before = count(sample(), Outcome::Refused);
        record(sample(), Outcome::Refused, Duration::from_millis(3));
        assert_eq!(count(sample(), Outcome::Refused) - before, 1.0);

        assert_registered(&[
            "tinio_sandbox_confinement_total",
            "tinio_sandbox_confinement_seconds",
        ]);
        let text = exposition();
        assert!(
            text.contains(r#"tinio_sandbox_confinement_total{mechanism="#)
                && text.contains(r#"outcome="refused"}"#),
            "the counter carries its mechanism and outcome: {text}"
        );
        assert!(
            text.contains("tinio_sandbox_confinement_seconds_count"),
            "the histogram carries the sample: {text}"
        );
        let family = default_registry()
            .gather()
            .into_iter()
            .find(|family| family.name() == "tinio_sandbox_confinement_seconds")
            .expect("the duration family is registered");
        assert_eq!(family.get_field_type(), MetricType::HISTOGRAM);
        assert!(
            family.get_metric().iter().all(|metric| {
                metric.get_label().len() == 1 && metric.get_label()[0].name() == "mechanism"
            }),
            "mechanism is the only duration label"
        );
    }

    /// The applied answer is its own series, so a run that confined and one that was refused are
    /// two values rather than one sum.
    #[test]
    fn applied_and_refused_are_one_series_each() {
        let _window = test_lock::window();
        let applied = count(sample(), Outcome::Applied);
        let refused = count(sample(), Outcome::Refused);
        record(sample(), Outcome::Applied, Duration::ZERO);
        assert_eq!(count(sample(), Outcome::Applied) - applied, 1.0);
        assert_eq!(count(sample(), Outcome::Refused), refused);
    }
}
