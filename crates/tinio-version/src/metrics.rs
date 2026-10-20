//! The supersede outcome family: the engine's contention signal.
//!
//! `result` is a closed set — `ok` for a supersede that committed (or found
//! nothing current) on the first resolve-read-verify attempt, `retried` for one
//! that committed after the design's re-resolve, `race` for one that answered
//! [`Error::SupersedeRace`](crate::Error::SupersedeRace) and wrote no row. A
//! bucket, key, version, slot value or blob never becomes a label.
//!
//! The registry is process-global, so the management plane renders this family
//! without this crate knowing it; the record call is the only write path.

#![expect(
    clippy::expect_used,
    reason = "argued: every error this registration can answer is about this family's own name (already registered, or registered with a different label set or help), from one `LazyLock` site a process runs once, against the family list `specs/001-s3-local-server/data-model.md` owns; the refusal is the collision detection, and it has nowhere to return a `Result` to (design 2026-10-09 D2d)"
)]

use std::sync::LazyLock;

use parse_display::Display;
use prometheus::{IntCounterVec, register_int_counter_vec};

/// One supersede outcome — the family's closed `result` label set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display(style = "lowercase")]
pub(crate) enum Outcome {
    /// Committed on the first attempt, or nothing was current to move.
    Ok,
    /// Committed after the design's one re-resolve.
    Retried,
    /// Answered `SupersedeRace`: no row was written.
    Race,
}

impl Outcome {
    /// The label value, without allocating.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Retried => "retried",
            Self::Race => "race",
        }
    }
}

/// Supersedes by result.
static SUPERSEDES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_storage_supersede_total",
        "Version supersedes by result",
        &["result"]
    )
    .expect("register tinio_storage_supersede_total")
});

/// Record one supersede outcome: exactly one sample per `supersede_with` call
/// that reaches a verdict (a storage failure records nothing).
pub(crate) fn record_supersede(outcome: Outcome) {
    #[cfg(test)]
    let _writer = test_lock::writer();
    SUPERSEDES.with_label_values(&[outcome.as_str()]).inc();
}

/// The metric statics are process-global (the prometheus default registry), so
/// an exact-value test races every other test's writes. The serialization is a
/// cfg(test)-only writer lock, the shape `tinio-server`'s metrics module uses:
/// every write takes it, and an exact-assert test takes the window for its
/// whole assert region. Production call sites compile the lock away.
#[cfg(test)]
pub(crate) use tinio_util::testing::metrics::{counter, test_lock};

/// The counter's current value for one outcome, read out of the default
/// registry (test support only).
#[cfg(test)]
pub(crate) fn count(outcome: Outcome) -> f64 {
    counter(
        "tinio_storage_supersede_total",
        &[("result", outcome.as_str())],
    )
}

#[cfg(test)]
mod tests {
    use prometheus::{default_registry, proto::MetricType};
    use tinio_util::testing::metrics::assert_registered;

    use super::{Outcome, count, record_supersede, test_lock};

    #[test]
    fn the_family_registers_once_as_a_counter_with_the_result_label() {
        let _window = test_lock::window();
        let before = count(Outcome::Race);
        record_supersede(Outcome::Race);
        assert_registered(&["tinio_storage_supersede_total"]);
        assert_eq!(count(Outcome::Race) - before, 1.0);

        let family = default_registry()
            .gather()
            .into_iter()
            .find(|family| family.name() == "tinio_storage_supersede_total")
            .expect("the family is registered");
        assert_eq!(family.get_field_type(), MetricType::COUNTER);
        assert!(
            family.get_metric().iter().all(|metric| {
                metric.get_label().len() == 1 && metric.get_label()[0].name() == "result"
            }),
            "result is the only label"
        );
    }
}
