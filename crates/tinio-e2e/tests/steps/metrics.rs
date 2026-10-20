//! Process-global metric assertions for the observability scenarios.
//!
//! The in-process plane wraps its backend in `MetricS3`, so a failed S3
//! request records `tinio_s3_errors_total` in the same registry this test
//! binary links. The assertion is a before/after delta, never an absolute
//! value: the registry is process-global and cucumber runs scenarios
//! concurrently, so only the sample this scenario took is a safe baseline.

use cucumber::{given, then};
use tinio_util::testing::metrics::counter;

/// Take the baseline the failed request's assertion compares against.
#[given(expr = "I sample the {string} counter for op {string} and code {string}")]
async fn sample_counter(world: &mut super::World, family: String, op: String, code: String) {
    world.metric_sample = Some(counter(&family, &[("op", &op), ("code", &code)]));
}

/// The series must have grown since the sample: the request that ran between
/// the two steps recorded it.
#[then(expr = "the {string} counter for op {string} and code {string} has increased")]
async fn counter_increased(world: &mut super::World, family: String, op: String, code: String) {
    let before = world
        .metric_sample
        .expect("a preceding sample step took the baseline");
    let after = counter(&family, &[("op", &op), ("code", &code)]);
    assert!(
        after > before,
        "{family}{{op=\"{op}\",code=\"{code}\"}} must increase: {before} -> {after}"
    );
}
