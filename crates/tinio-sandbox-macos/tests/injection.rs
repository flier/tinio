//! **A hostile path through the real `sandbox-exec`**: the profile's `-D` substitution is data,
//! and this is the leg that would notice if it ever became text.
//!
//! The exposure set reaches the kernel as `-D NAME=VALUE` argv pairs that the profile text
//! refers to by name (`(param "E0")`), so nothing in the text is ever a path — which is what
//! removes escaping and injection from the generator entirely (design decision 24). The unit
//! legs assert that as text (`profile.rs`'s `the_profile_text_carries_no_path`, whose value is
//! `HOSTILE` below), and those assertions cannot distinguish the *property* from the
//! *argument*: a future `sandbox-exec` that substituted `-D` values into the profile text
//! before parsing it would leave every one of them green, because the text still carries
//! `(param "E0")`.
//!
//! So this leg runs the hostile path through the real binary instead. The path **closes the
//! literal and opens a rule of its own** —
//! `/srv/x") (allow network-outbound) (subpath "/` — and the child then attempts an outbound
//! `connect` to a target the test proved reachable from *outside* the confinement first. A
//! value that stayed data grants nothing and the connect is refused `permission denied`; a
//! value that became text would have granted `network-outbound` unconditionally and the connect
//! would succeed.
//!
//! **The negative control is measured, and it is the leg's own assertion that flips.** A
//! profile that really grants the rule the value names — the same fixture with
//! `(allow network-outbound)` written into it — is answered `outbound=CONNECTED`, because the
//! parent accepts the connection it proved reachable; the profile this leg runs against answers
//! `outbound=refused(permission denied)`. Both were run on macOS 15.8, 2026-09-25, against this
//! repository's own compiled probe binary, under a `(deny default)` base carrying the exposure
//! rule and the same four baseline allowances the real profile renders:
//!
//! ```text
//! === plain       exit=0   report: payload=none | outbound=refused(permission denied) |
//! === explicit    exit=0   report: payload=none | outbound=CONNECTED |
//! ```
//!
//! So `assert_eq!(report.value("outbound"), Some("refused(permission denied)"))` below is not a
//! vacuous assertion: it fails on the value it names the moment an outbound grant reaches the
//! compiled profile.
//!
//! **What a purely *textual* substitution of this particular value does is fail earlier, and
//! that is worth stating rather than hiding.** Writing `HOSTILE` straight into the rule text —
//! `(allow file-read* file-write* (subpath "<value>"))`, which is exactly what a substitution
//! that had stopped using `-D` would produce — makes `sandbox-exec` refuse the profile with
//! `illegal argument` (exit 65) before the child exists, because the value's own `"` closes the
//! argument and what follows is not SBPL. Measured on the same host:
//!
//! ```text
//! === interpolated  exit=65  sandbox-exec: illegal argument:  #t
//!                            <input string>:3:99:  (subpath "/")
//! ```
//!
//! A compiler refusal is a third state beside "granted" and "denied", and this leg treats it as
//! a failure too: `report.expect("payload", "ok")` and the `outbound=` presence check below
//! reject a child that never ran, so no substitution that changes what the kernel compiles can
//! pass this leg quietly.
//!
//! The shape of the guard is the neighbour's (`network.rs`): a `harness::launch()` no-op in the
//! runner, a probe selected by the launcher copy's argv, and a report read back from inside the
//! confinement.

#![cfg(target_os = "macos")]
// **A macOS-only test target, and a stated exemption because clippy's test keys do not reach
// it** (docs/style.md §Panics, design 2026-10-09 D7): `allow-*-in-tests` covers `#[test]`
// functions and `#[cfg(test)]` modules, while this target's helpers and `harness/` — declared
// below, inheriting this level — are plain `fn`s.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

mod harness;

use std::{fs, path::PathBuf, process};

use tinio_sandbox_core::{AccessMode, Sandbox};
use tinio_sandbox_macos::profile::profile;

/// The path that closes the rule it is written into — the value `profile.rs`'s
/// `the_profile_text_carries_no_path` also uses, and the reason `validate_path` deliberately
/// admits `"`, `\`, `(`, `)` and `;`.
const HOSTILE: &str = "/srv/x\") (allow network-outbound) (subpath \"/";

/// [`HOSTILE`] without its leading separator, for building a path *under* the scratch root.
///
/// **`Path::join` replaces the base when the argument is absolute** — so joining the constant
/// above onto the scratch directory would name `/srv/…` and the fixture would try to create the
/// hostile path at the filesystem root, where it is refused `ReadOnlyFilesystem` (measured on
/// this VM, macOS 15.8, 2026-09-25: `Os { code: 30, kind: ReadOnlyFilesystem }`). The relative
/// spelling is the same path under `/tmp`, which is what this leg wants; the absolute constant
/// stays the one the profile assertions are written against.
const HOSTILE_RELATIVE: &str = "srv/x\") (allow network-outbound) (subpath \"/";

/// Runs inside the confinement: try to reach out to the address the launcher proved reachable.
#[test]
fn child_injection() {
    if !harness::is_child_process() {
        harness::launch();
        return;
    }
    let Some(child) = harness::child() else {
        return;
    };
    let mut log = String::new();

    // The shared probe (`harness::probe_outbound`), which `network.rs`'s child
    // drives too: the assertion this leg makes about its answer is its own, and it
    // is the two rendered tokens — `refused(permission denied)` against
    // `CONNECTED` — that the two files have in common.
    log.push_str(&harness::probe_outbound(&child));
    harness::finish(&child, &log);
}

/// **A path that carries a whole SBPL rule stays a value**: the profile renders `(param "E0")`,
/// the launch hands the value over as a `-D` pair, and the rule it tries to open grants
/// nothing.
///
/// The hostile path is the *writable* exposure here, and that is deliberate: it is the state
/// directory's own shape (a read-write entry beside a read-only root), so this is the exposure
/// set the read-only product path builds, with the hostile spelling substituted for the state
/// directory's path. Were the value to reach the text unquoted, the injected
/// `(allow network-outbound)` would be part of the profile the kernel compiles.
///
/// **The scratch root is `/tmp`, and on this platform that is a fact rather than tidiness.**
/// `tempfile::tempdir()` puts its directory under `$TMPDIR` = `/private/var/folders/…`, where
/// the first version of this leg died creating its exposure — `Os { code: 30, kind:
/// ReadOnlyFilesystem, message: "Read-only file system" }` (measured on this VM, macOS 15.8,
/// 2026-09-25) — and the e2e leg's own comment names the same constraint from the other
/// direction (a root under `$TMPDIR` is past `sun_path`'s 104 bytes). `/tmp` is the platform's
/// short scratch area, so it is this fixture's home.
#[test]
fn a_hostile_exposure_path_cannot_open_an_outbound_rule() {
    // The child is launched by the launcher copy, whose argv selects `child_injection`.
    if harness::is_child_process() {
        return;
    }
    let dir = PathBuf::from(format!("/tmp/tinio-injection-{}", process::id()));
    // A pid can be reused across runs, and a stale tree would carry an earlier leg's exposure.
    let _ = fs::remove_dir_all(&dir);
    let root = dir.join("root");
    fs::create_dir_all(&root).expect("create root");
    // The hostile path is a directory like any other: a path that does not exist has no rule to
    // match against and would make the leg vacuous for a reason that is not the substitution's.
    // It is written relative to the scratch root — `HOSTILE_RELATIVE`'s comment carries what an
    // absolute join did instead.
    let hostile = dir.join(HOSTILE_RELATIVE);
    fs::create_dir_all(&hostile).expect("create the hostile exposure");
    assert!(
        hostile.is_dir() && hostile.to_str().is_some_and(|path| path.ends_with(HOSTILE)),
        "the fixture's hostile directory is the path the assertions below name: {}",
        hostile.display()
    );

    // The baseline first (design §12, and `network.rs`'s reason): the target is reachable from
    // *outside* the confinement, so the assertion below cannot pass vacuously on a machine with
    // no route. `harness::reachable_target` makes the proof; the listener it hands back stays
    // alive for the whole leg, which is what the `_target` binding is for.
    let (_target, addr) = harness::reachable_target();

    let sandbox = Sandbox::new()
        .expose(&hostile, AccessMode::ReadWrite)
        .expose(&root, AccessMode::ReadOnly);
    let report = harness::run_confined(
        &sandbox,
        &[("outbound", addr.to_string())],
        "child_injection",
    );

    report.expect("payload", "ok");
    // **The assertion the injection would flip.** `refused(permission denied)` is the profile's
    // own refusal — nothing in this text grants a network operation — while a substitution that
    // put the value's rule into the text answers `CONNECTED`.
    assert_eq!(
        report.value("outbound"),
        Some("refused(permission denied)"),
        "a hostile exposure path reached the profile as SBPL text rather than as a parameter \
         value; the full report is:\n{}",
        report.text()
    );

    // **And the branch this file exists for is the branch that ran.** A profile whose hostile
    // value landed in the text would be refused by `sandbox-exec`'s compiler for most spellings
    // (the first `(allow` closes the parameter reference and the rest is a syntax error), so a
    // leg that only read `outbound` could pass for a child that never started. `payload=ok` above
    // is written by the launcher's own handover, and the report's presence proves the child ran
    // under a compiled profile and wrote inside its writable exposure.
    assert!(
        report.text().contains("outbound="),
        "the child never reported, so nothing was confined: {}",
        report.text()
    );

    // The parameter list is the other half, and it is checked in-process: every exposure
    // reaches argv verbatim, the hostile one included, so the value that travels beside the
    // text is the operator's path rather than a sanitized form of it. The profile is rendered
    // from the set as it stands: the launch's own `profile(&with_canonical_paths(…))` would add
    // a second parameter for `/tmp`'s `/private` spelling, which is not what this assertion is
    // about.
    let rendered = profile(&sandbox, tinio_sandbox_macos::Mechanism::SandboxExec).expect("profile");
    assert!(
        !rendered.text.contains(HOSTILE),
        "the hostile path is written into the profile text:\n{}",
        rendered.text
    );
    assert!(
        !rendered.text.contains("network-outbound"),
        "an outbound rule is in the profile text:\n{}",
        rendered.text
    );
    assert!(
        rendered
            .params
            .iter()
            .any(|(_, value)| value.to_str().is_some_and(|path| path.ends_with(HOSTILE))),
        "the hostile path must travel as a `-D` value verbatim: {:?}",
        rendered.params
    );

    // The leg's own root goes with it: nothing else drops it, and a shared runner should not
    // accumulate one per run (the e2e leg's `cleanup`, for the same reason).
    let _ = fs::remove_dir_all(&dir);
}
