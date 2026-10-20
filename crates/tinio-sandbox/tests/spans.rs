//! The confinement span, asserted from a dedicated binary.
//!
//! **A separate target rather than a module of the lib test binary**, for the reason
//! `tinio-blob`'s span test carries: the recorder is the process default, so a capture opened
//! beside another test would read its spans. This binary has one test and nothing else races the
//! `apply` callsite.
//!
//! **The span is driven through a refusal, deliberately.** An empty exposure set is refused by
//! every backend before anything irreversible, so the legs are the same on a host that can
//! confine and one that cannot — and the refusal arm is the one whose `outcome` field the plan
//! asks a test to prove.

use tinio_sandbox::serve;
use tinio_util::testing::{metrics::counter, spans::recording};
use tracing::Level;

/// The refusal series the mechanism this host names.
fn refusals() -> f64 {
    counter(
        "tinio_sandbox_confinement_total",
        &[("mechanism", mechanism()), ("outcome", "refused")],
    )
}

/// The mechanism each platform's refusal names, from the facade's closed label set.
fn mechanism() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        // The empty set is refused by `validate_and_plan` before the model resolves, so the
        // request is the value the run had.
        "auto"
    }
    #[cfg(target_os = "macos")]
    {
        "sandbox-exec"
    }
    #[cfg(target_os = "windows")]
    {
        "appcontainer"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        "none"
    }
}

#[test]
fn apply_opens_one_info_span_with_the_mechanism_and_the_outcome() {
    let (capture, _gate) = recording();
    let refusals_before = refusals();
    // An empty exposure set is refused on every host; the call is the span's refusal arm.
    let refused = serve::Spec::new()
        .apply()
        .expect_err("an empty exposure set is refused");
    assert!(!refused.to_string().is_empty(), "{refused}");

    let spans = capture.named("tinio.sandbox.apply");
    assert_eq!(spans.len(), 1, "one T0 span per apply: {spans:#?}");
    let span = &spans[0];
    assert_eq!(span.target, "tinio::sandbox");
    assert_eq!(span.level, Level::INFO);
    assert_eq!(span.field("count"), Some("0"));
    assert_eq!(span.field("mechanism"), Some(mechanism()));
    assert_eq!(span.field("outcome"), Some("refused"));
    assert_eq!(
        refusals() - refusals_before,
        1.0,
        "a refused confinement adds one counter sample"
    );
}
