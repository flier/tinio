//! The launcher's two families: what a readiness wait answered, and how a child's exit ended.
//!
//! **Both are the launcher's own facts and both are invisible as a span under `info`** — the
//! readiness wait answers while the child is still running, and the exit is the value the CLI
//! forwards. The counters are the aggregate an operator pages on: a child that never reported
//! (the readiness `gone`) and a child that died instead of serving (the exit `failure`).
//!
//! **The labels are closed sets and carry no subject**: `result` is the three
//! [`Readiness`](crate::Readiness) answers and `outcome` is [`Exit`]'s four. No pid, exit code,
//! address, path or payload is a label, and neither family has another label.
//!
//! The registry is process-global, so the management plane renders these without this crate
//! knowing it; the record calls are the only write paths.

use std::{process::ExitStatus, sync::LazyLock};

use prometheus::{IntCounterVec, register_int_counter_vec};
use tinio_util::metrics::RegisteredExt;

/// What a readiness wait answered — the `result` label's whole domain, one variant per
/// [`Readiness`](crate::Readiness) answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ready {
    /// The child reported a terminal fact before the bound elapsed.
    Reported,
    /// The relay ended without one: the child is gone, and it never reported.
    Gone,
    /// The bound elapsed with the child still there.
    Elapsed,
}

impl Ready {
    /// The label value.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Reported => "reported",
            Self::Gone => "gone",
            Self::Elapsed => "elapsed",
        }
    }
}

impl From<&crate::Readiness> for Ready {
    fn from(readiness: &crate::Readiness) -> Self {
        match readiness {
            crate::Readiness::Reported(_) => Self::Reported,
            crate::Readiness::Gone => Self::Gone,
            crate::Readiness::Elapsed => Self::Elapsed,
        }
    }
}

/// How a child's wait ended — the `outcome` label's whole domain.
///
/// **The export code is not a label** (it is unbounded), so a non-zero exit is one value: a
/// child that refuses after readiness and one that fails for any other reason are the same fact
/// at this boundary — the core's `Attribution::Ran` states why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Exit {
    /// The child exited zero.
    Success,
    /// The child exited non-zero, with an exit code to report.
    Failure,
    /// The child was terminated by a signal, so there is no exit code.
    Signalled,
    /// The platform's wait itself failed: no exit was observed.
    Error,
}

impl Exit {
    /// The label value.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Signalled => "signalled",
            Self::Error => "error",
        }
    }

    /// The outcome one exit status carries.
    pub(crate) fn of(status: &ExitStatus) -> Self {
        if status.success() {
            Self::Success
        } else if status.code().is_some() {
            Self::Failure
        } else {
            // A termination by signal: the status has no exit code to report.
            Self::Signalled
        }
    }
}

/// Readiness waits by the answer they ended on.
static READINESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_sandbox_readiness_total",
        "Readiness waits by the answer they ended on",
        &["result"]
    )
    .registered("tinio_sandbox_readiness_total")
});

/// Child exits by how the wait ended.
static CHILD_EXIT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_sandbox_child_exit_total",
        "Confined child exits by how the wait ended",
        &["outcome"]
    )
    .registered("tinio_sandbox_child_exit_total")
});

/// Record one readiness wait's answer — exactly one sample per `wait_for_ready` call.
pub(crate) fn record_readiness(result: Ready) {
    #[cfg(test)]
    let _writer = test_lock::writer();
    READINESS.with_label_values(&[result.as_str()]).inc();
}

/// Record one child's wait outcome — exactly one sample per `wait` call that reached a verdict.
pub(crate) fn record_exit(outcome: Exit) {
    #[cfg(test)]
    let _writer = test_lock::writer();
    CHILD_EXIT.with_label_values(&[outcome.as_str()]).inc();
}

/// The metric statics are process-global (the prometheus default registry), so an exact-value
/// test races every other test's writes. The serialization is a cfg(test)-only writer lock, the
/// shape `tinio-version`'s metrics module uses: every write takes it, and an exact-assert test
/// takes the window for its whole assert region. Production call sites compile the lock away.
#[cfg(test)]
pub(crate) use tinio_util::testing::metrics::test_lock;

#[cfg(test)]
mod tests {
    use prometheus::{default_registry, proto::MetricType};
    use tinio_util::testing::metrics::{assert_registered, counter};

    use super::{Exit, Ready, record_exit, record_readiness, test_lock};

    /// The readiness family is a counter with one `result` label, and one record moves the one
    /// series it names.
    #[test]
    fn the_readiness_family_counts_one_answer() {
        let _window = test_lock::window();
        let before = counter("tinio_sandbox_readiness_total", &[("result", "gone")]);
        record_readiness(Ready::Gone);
        assert_registered(&["tinio_sandbox_readiness_total"]);
        assert_eq!(
            counter("tinio_sandbox_readiness_total", &[("result", "gone")]) - before,
            1.0
        );

        let family = default_registry()
            .gather()
            .into_iter()
            .find(|family| family.name() == "tinio_sandbox_readiness_total")
            .expect("the readiness family is registered");
        assert_eq!(family.get_field_type(), MetricType::COUNTER);
        assert!(
            family.get_metric().iter().all(|metric| {
                metric.get_label().len() == 1 && metric.get_label()[0].name() == "result"
            }),
            "result is the only readiness label"
        );
    }

    /// The exit family counts each outcome as its own series: a successful exit is not the same
    /// value as a failure, and an error from the wait is its own third fact.
    #[test]
    fn the_exit_family_keeps_each_outcome_apart() {
        let _window = test_lock::window();
        let before = counter("tinio_sandbox_child_exit_total", &[("outcome", "failure")]);
        record_exit(Exit::Failure);
        assert_registered(&["tinio_sandbox_child_exit_total"]);
        assert_eq!(
            counter("tinio_sandbox_child_exit_total", &[("outcome", "failure")]) - before,
            1.0
        );

        let family = default_registry()
            .gather()
            .into_iter()
            .find(|family| family.name() == "tinio_sandbox_child_exit_total")
            .expect("the exit family is registered");
        assert_eq!(family.get_field_type(), MetricType::COUNTER);
        assert!(
            family.get_metric().iter().all(|metric| {
                metric.get_label().len() == 1 && metric.get_label()[0].name() == "outcome"
            }),
            "outcome is the only exit label"
        );
    }
}
