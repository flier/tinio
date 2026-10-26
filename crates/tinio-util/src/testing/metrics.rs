//! Shared helpers for the metrics modules' exact-value tests.
//!
//! Every `metrics` module asserts against the process-global registry, so its
//! test needs two things: one exposition string, and a gate that serializes
//! exact assertions against the writers other tests keep running. Both used to
//! be copied per crate; they live here now.
//!
//! ```rust
//! use tinio_util::testing::metrics::{exposition, test_lock};
//!
//! let _window = test_lock::window();
//! let _text = exposition();
//! ```

use prometheus::{Encoder, TextEncoder, default_registry, proto::Metric};

/// Assert every `tinio_*` name is registered on the default registry.
///
/// This is the per-crate gate: a family task adds its name here in the same
/// change, so a registration that does not land fails that crate's test.
pub fn assert_registered(names: &[&str]) {
    let gathered = default_registry().gather();
    for name in names {
        assert!(
            gathered.iter().any(|family| family.name() == *name),
            "unregistered tinio_* family: {name}"
        );
    }
}

/// The one series matching every label, or `None` when the family or series
/// has no sample yet (prometheus omits a series it has never seen).
fn series(family: &str, labels: &[(&str, &str)]) -> Option<Metric> {
    default_registry()
        .gather()
        .into_iter()
        .find(|candidate| candidate.name() == family)
        .and_then(|family| {
            family
                .get_metric()
                .iter()
                .find(|metric| {
                    labels.iter().all(|(name, value)| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == *name && label.value() == *value)
                    })
                })
                .cloned()
        })
}

/// One counter series' current value, or zero when the family or series has
/// no sample.
pub fn counter(family: &str, labels: &[(&str, &str)]) -> f64 {
    series(family, labels).map_or(0.0, |metric| metric.get_counter().value())
}

/// One gauge series' current value, or zero when the family or series has no
/// sample.
pub fn gauge(family: &str, labels: &[(&str, &str)]) -> f64 {
    series(family, labels).map_or(0.0, |metric| metric.get_gauge().value())
}

/// One histogram series' sample count, or zero when the family or series has
/// no sample.
pub fn histogram_count(family: &str, labels: &[(&str, &str)]) -> f64 {
    series(family, labels).map_or(0.0, |metric| {
        metric.get_histogram().get_sample_count() as f64
    })
}

/// The `tinio_object_lock_protected_deletions_total` value for one
/// operation/reason pair — the family's delta fixture.
///
/// A test reads it before and after the code under test and asserts the
/// difference, so a parallel writer of another label pair cannot skew the
/// answer. The label values are the family's closed domains
/// (`delete_object`, `delete_objects`, `lifecycle`, `sweeper`; `legal_hold`,
/// `compliance`, `governance`).
pub fn protected_deletions(operation: &str, reason: &str) -> f64 {
    counter(
        "tinio_object_lock_protected_deletions_total",
        &[("operation", operation), ("reason", reason)],
    )
}

/// The whole default registry rendered in the text exposition format.
pub fn exposition() -> String {
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&default_registry().gather(), &mut buffer)
        .expect("the text encoder writes into a Vec");
    String::from_utf8(buffer).expect("the text encoder emits UTF-8")
}

/// The writers'/window gate the exact-value tests share.
pub mod test_lock {
    use std::{
        cell::Cell,
        sync::{Mutex, MutexGuard, PoisonError},
    };

    static LOCK: Mutex<()> = Mutex::new(());

    thread_local! {
        static HELD: Cell<bool> = const { Cell::new(false) };
    }

    /// Writer-side guard: `Some` blocks while an exact-assert test on another
    /// thread holds the window; `None` on the window's own thread, so the exact
    /// test's setup writes do not deadlock against its own lock.
    pub fn writer() -> Option<MutexGuard<'static, ()>> {
        if HELD.with(Cell::get) {
            None
        } else {
            Some(LOCK.lock().unwrap_or_else(PoisonError::into_inner))
        }
    }

    /// The exact-assert window: exclusive against every other-thread writer,
    /// reentrant on this thread. The thread mark is cleared on drop — the
    /// harness reuses test threads, so a stale mark would disable the lock.
    pub struct Window {
        _lock: MutexGuard<'static, ()>,
    }

    impl Drop for Window {
        fn drop(&mut self) {
            HELD.with(|held| held.set(false));
        }
    }

    /// Open the window.
    pub fn window() -> Window {
        HELD.with(|held| held.set(true));
        Window {
            _lock: LOCK.lock().unwrap_or_else(PoisonError::into_inner),
        }
    }
}
