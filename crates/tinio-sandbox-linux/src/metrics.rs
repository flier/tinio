//! The two sandbox metric families, declared in the crate that observes them.
//!
//! **Each family is a fact about the confinement this process applied to itself, and both facts
//! are invisible today** — which is why they are metrics rather than lines. The read-only arm an
//! exposure took exists only as a `println!` on stdout from the test-only `sandbox-child` binary,
//! and the seccomp skip is a `warn!` emitted during the confinement, where this platform has no
//! subscriber yet (`tinio-cli`'s in-place arm builds its subscriber *after* the confinement, so
//! every event the confinement emits is discarded). A metric is the carrier that survives both:
//! it outlives the window and it is read from the servo's own surface.
//!
//! **Declared here rather than in `tinio-server`, and the direction of the dependency is the
//! reason.** The observing code is [`crate::enter`]'s read-only arm and [`crate::seccomp`]'s
//! skip, both of them this crate's; a sandbox crate depending on `tinio-server` to declare a
//! family would invert the layering, and the family would still be recorded from here. What
//! makes that work is the registry: these are the `prometheus` crate's default registry, which
//! is process-global, so a family registered in this crate is gathered by the management plane's
//! `/metrics` on the same process — and on this platform the process that confines itself in
//! place is the one that serves (design decision 6).
//!
//! **Nothing here is registered eagerly.** A family appears in an exposition once it has a
//! sample, so a run that took no read-only exposure publishes no `tinio_sandbox_read_only_arm`
//! line at all — which is the honest answer and the prometheus behaviour `tinio-server`'s own
//! registry tests rely on. The seccomp family is the other shape: `install` records it on both
//! of its answers, so a confined run always carries the `0` that says the filter landed.
//!
//! `specs/001-s3-local-server/data-model.md`'s `## Metrics` owns the name list, and
//! `contracts/management-api.md` owns the exposition.

use std::sync::LazyLock;

use prometheus::{IntGauge, IntGaugeVec, register_int_gauge, register_int_gauge_vec};

use crate::Seccomp;

/// The read-only arm an exposure actually took (design §4's two implementations).
///
/// **A closed vocabulary, and that is the point of the enum rather than a `&str` argument**: the
/// label's value can only ever be one of the two spellings below, so no path, configuration
/// value or credential has a route into the exposition through this family. What the label
/// carries is the *choice*, never the exposure it was made for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadOnlyArm {
    /// `MS_REMOUNT|MS_BIND` with the source's own locked flags re-asserted — the primary
    /// mechanism, and the one that leaves the host's mount table unshared.
    Remount,
    /// The `MS_RDONLY` overlay, taken when a locked per-mount flag refused the remount
    /// (`refused_remount_needs_the_overlay`). Design §4 calls it the fallback, and which of the
    /// two a host reaches is the kernel's answer rather than the plan's.
    Overlay,
}

impl ReadOnlyArm {
    /// The `arm` label's value.
    fn label(self) -> &'static str {
        match self {
            Self::Remount => "remount",
            Self::Overlay => "overlay",
        }
    }
}

/// The read-only mechanism each read-only exposure landed, by arm — `1` for the arm taken.
///
/// A gauge rather than a counter: the fact is *which* arm this run used, not how many times an
/// exposure used it, and two exposures that took different arms set two labels rather than
/// summing into one unreadable number.
static READ_ONLY_ARM: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "tinio_sandbox_read_only_arm",
        "The read-only mechanism a run's exposures landed (1 for the arm taken, by arm)",
        &["arm"]
    )
    .expect("register tinio_sandbox_read_only_arm")
});

/// Whether the seccomp filter was skipped because this architecture has no syscall table
/// (`1`) or installed (`0`) — the one documented degradation of this platform's confinement.
///
/// **Both answers are recorded, so the family is not merely present but readable in either
/// state**: a `0` is the assertion that the network cut landed, which an absence could not make.
static SECCOMP_SKIPPED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "tinio_sandbox_seccomp_skipped",
        "Whether the seccomp filter was skipped: 1 when this architecture has no syscall table, 0 when it landed"
    )
    .expect("register tinio_sandbox_seccomp_skipped")
});

/// Record the read-only arm `arm` landed, for the exposure the caller just enforced.
///
/// **Both call sites sit where the arm is *decided*, and that is why each sits immediately before
/// the step that follows it.** The remount arm is recorded with the `Ok` in hand and
/// *before* `refuse_writable_nested_mounts` — the kernel granted the arm, and the check after it
/// refuses the **run** if the remount left a writable submount reachable inside the exposure,
/// which is a verdict on the jail rather than on the arm. The overlay arm is recorded at the
/// substitution that chooses the fallback and before `apply_mount` runs it, because that branch
/// is the only way into `MountKind::OverlayReadOnly` and the choice is the fact this family
/// reports. Neither ordering can show an observer an arm the run then did not have: a refusal
/// after either record ends the run (design §11's fail-closed), and a run that ended serves no
/// `/metrics` for a scrape to read.
pub(crate) fn record_read_only_arm(arm: ReadOnlyArm) {
    READ_ONLY_ARM.with_label_values(&[arm.label()]).set(1);
}

/// Record what the seccomp install decided — the state `Seccomp` is returned as, recorded where
/// it is decided so the value a caller reads and the value `/metrics` reports are one answer.
pub(crate) fn record_seccomp(state: Seccomp) {
    SECCOMP_SKIPPED.set(i64::from(matches!(state, Seccomp::SkippedUnsupportedArch)));
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Mutex};

    use prometheus::{Encoder, TextEncoder, default_registry};
    use tempfile::tempdir;

    use super::*;

    /// Serializes the tests that assert an exact value of either family — the statics are
    /// process-global (the prometheus default registry), so a parallel test that records the
    /// other answer between this one's write and its gather would clobber the assertion. Two
    /// tests disagree about `tinio_sandbox_seccomp_skipped` by construction (`1` skipped, `0`
    /// installed), and both are worth having.
    static FAMILIES: Mutex<()> = Mutex::new(());

    /// The exposition the management plane serves, as the CLI's adapter produces it: the default
    /// registry, text-encoded.
    fn exposition() -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&default_registry().gather(), &mut buf)
            .expect("encode the default registry");
        String::from_utf8(buf).expect("the text exposition is UTF-8")
    }

    /// Both families render their values, and **neither carries a value the run holds as a
    /// secret** (the sandbox review's STD-01: a control token reaching CI logs).
    ///
    /// **The two halves are asserted against the state directory the run's token lives in**,
    /// because that is what a leaking exposition would carry: the directory's path and its
    /// token-bearing `state` file are in scope, and neither may appear. The structural half is
    /// the enum above — the label's value is drawn from `ReadOnlyArm` and the seccomp family has
    /// no label at all — and this is the rendered half that a future free-text field would fail.
    #[test]
    fn the_families_render_their_values_and_no_token() {
        let _guard = FAMILIES.lock().expect("the family lock");
        // The state directory as a confined run leaves it: `state` holds the control token
        // (`tinio-api`'s `State::write`), beside the access log and the metadata store.
        let state = tempdir().expect("a state directory");
        // A literal, not a minted token: 16 hex characters repeated 4× make 64, exactly a real
        // token's shape (`tinio-api`'s `TOKEN_BYTES = 32`), and reaching that crate's
        // `generate_token` from here would add a dependency edge inverting the layering this
        // module's doc reasons about.
        let token = "3f2a9c1d4b6e8f0a".repeat(4);
        fs::write(state.path().join("state"), format!("token: {token}")).expect("write `state`");
        let state_path = state.path().to_str().expect("a UTF-8 temp path").to_owned();

        record_read_only_arm(ReadOnlyArm::Remount);
        record_seccomp(Seccomp::SkippedUnsupportedArch);
        let text = exposition();

        // The sample lines are asserted whole, which is the label half of the same rule: a
        // family that grew a second label — a path, a configuration value — would not contain
        // these strings any more.
        assert!(
            text.contains("tinio_sandbox_read_only_arm{arm=\"remount\"} 1"),
            "{text}"
        );
        assert!(text.contains("tinio_sandbox_seccomp_skipped 1"), "{text}");

        for secret in [token.as_str(), state_path.as_str()] {
            assert!(
                !text.contains(secret),
                "the exposition carries {secret:?}:\n{text}"
            );
        }
    }

    /// The two arms are two label values of one family, and the second one lands beside the
    /// first rather than replacing it — a run whose exposures took different mechanisms reports
    /// both, which is what makes the family readable for such a run instead of a race.
    #[test]
    fn each_arm_is_its_own_sample() {
        let _guard = FAMILIES.lock().expect("the family lock");
        record_read_only_arm(ReadOnlyArm::Remount);
        record_read_only_arm(ReadOnlyArm::Overlay);
        let text = exposition();
        assert!(
            text.contains("tinio_sandbox_read_only_arm{arm=\"remount\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("tinio_sandbox_read_only_arm{arm=\"overlay\"} 1"),
            "{text}"
        );
    }

    /// The seccomp family's other answer: an installed filter reports `0`, so a scraper reads
    /// "the network cut landed" rather than an absence it has to interpret.
    #[test]
    fn an_installed_filter_reports_zero() {
        let _guard = FAMILIES.lock().expect("the family lock");
        record_seccomp(Seccomp::Installed);
        assert!(exposition().contains("tinio_sandbox_seccomp_skipped 0"));
    }
}
