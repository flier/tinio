//! Test devices that **more than one crate's tests** need.
//!
//! **This module exists because a `tests/` directory is not importable across
//! crates.** `crates/tinio-server/tests/sandbox_e2e_windows.rs` drives the same
//! launcher this crate's own suites do, and the harness it needs — reaping a
//! spawned process under a deadline, reading a DACL, planting a junction —
//! lived in `crates/tinio-sandbox-windows/tests/common/mod.rs`, which that crate
//! cannot reach, so it was written a second time. `docs/tests.md` names the
//! convention: shared test support lives on the crate that owns the subject
//! rather than in a copy per suite.
//!
//! **It is a `testing` feature and not a plain module.** A feature is the usual
//! spelling of that convention (`tinio-util`'s, and this crate's `testing` is
//! non-default for the same reason), and it is what keeps the devices out of a
//! build that is not a test build: the module compiles only when the feature is
//! on, and no production edge turns it on. **A `#[cfg(test)]` module could not do
//! this**, because `#[cfg(test)]` is invisible to `tests/` — the harness has to
//! be lib code to cross that boundary at all. So the feature is asked for on this
//! crate's own test builds by a dev-dependency on this crate itself
//! (`tinio-sandbox-windows = { path = ".", features = ["testing"] }` in
//! `Cargo.toml`, which is not the cycle it reads as: Cargo unifies that edge with
//! the lib under test, so both are one build), and `tinio-server`'s Windows e2e
//! targets ask for it the same way from their own dev-dependency. Either way the
//! module is reachable from `tests/` without a per-suite copy. The cost is one
//! small module in a library `publish = false` keeps inside this workspace.
//!
//! **Nothing here asserts anything about the confinement.** These are devices a
//! test composes: what a test proves is still in the test. The two rules they do
//! carry are the ones a suite cannot be trusted to spell for itself, and they are
//! the reason these are shared rather than copied — [`wait_for_exit`] kills the
//! process it gives up on, so a suite that wrote its own copy could assert on the
//! deadline without killing the child first (`tests/common/mod.rs`'s doc records
//! the suite where it happened), and [`host_cannot`] is the one place a host
//! precondition may be turned into a skip, so a leg cannot opt itself out of an
//! assertion in a second spelling of [`ALLOW_SKIP`].
//!
//! **[`wait_for_exit`], [`host_cannot`], the [`ALLOW_SKIP`] pair, the [`capture`]
//! device and the [`Capture`]/[`Recording`]/[`recorder`] names beside it are
//! defined in [`tinio_util::testing`] and re-exported here.** This module used to
//! hold them, and this crate's root is `#![cfg(target_os = "windows")]` — so the
//! macOS and Linux legs that need the same rules, or record their own events,
//! could not import them at all, and each spelled its own copy. The definers moved
//! to the crate every platform builds; the re-exports are what keep every caller
//! of this path (this crate's own suites,
//! `tinio-server/tests/support/windows_e2e_harness.rs`'s wrapper) working
//! unchanged.
//!
//! Behind the crate's own `#![cfg(target_os = "windows")]` for the crate root's
//! reason: `crates/*` has no per-target member form, so this compiles on hosts
//! it does not serve.

use std::{fs, path::Path, process::Command};

/// Wait for `child` to exit, or kill it and fail the test rather than block on
/// it.
///
/// **The definition is [`tinio_util::testing::wait_for_exit`]'s, re-exported
/// rather than written again**: this crate's root is
/// `#![cfg(target_os = "windows")]`, so the rule's other callers — the server's
/// macOS e2e leg and the macOS crate's own suites — cannot reach a definer that
/// lives here, and each re-spelled the deadline/poll/kill rule for itself. The
/// name stays on this path because that is what this crate's own suites
/// (`tests/common/mod.rs`) and the server harness's wrapper
/// (`tinio-server/tests/support/windows_e2e_harness.rs`) already import; the
/// rule, its bounds and the kill-before-panic are documented there.
#[doc(inline)]
pub use tinio_util::testing::wait_for_exit;
/// The one way past a precondition this host cannot meet, the deliberate opt-in
/// `TINIO_SANDBOX_ALLOW_SKIP`, and the arbitration a suite takes it through.
///
/// **Defined in [`tinio_util::testing`] and re-exported here**, for
/// [`wait_for_exit`]'s reason: this crate's root is
/// `#![cfg(target_os = "windows")]`, so the Linux jail suite and the server's
/// Linux e2e legs — both of which skip under the same variable — could not import
/// a definer that lives here and each spelled the rule again. The name stays on
/// this path because that is what this crate's own suites
/// (`tests/network.rs`, `src/backend/tests.rs`) and the server harness
/// (`tinio-server/tests/support/windows_e2e_harness.rs`) already import; the
/// rule, the assert-before-print order and the remedy's spelling are documented
/// at the definer.
#[doc(inline)]
pub use tinio_util::testing::{ALLOW_SKIP, host_cannot, skip_allowed};

use crate::acl;

/// `icacls` on `path`, with `args` appended, and its exit status checked.
///
/// The oracle for what is on the operator's directory, and it has to refuse a
/// non-zero exit: an `icacls` that could not read the DACL would otherwise hand
/// back empty text, and "no package ACE is left" would be asserted about a
/// directory nothing had looked at. That is the reason every suite that asserts
/// on the DACL text shares this function rather than spelling the check again.
/// `args` is `icacls`'s own (`&[]` reads the DACL; `"/grant"`, `"/remove:g"` and
/// `"/q"` write it), and the path comes first as its syntax requires.
///
/// An AppContainer package SID does not resolve to an account name, so a
/// package-SID ACE renders as the `S-1-15-2-…` string itself; **a well-known SID
/// does not come back that way** (`icacls` prints `S-1-5-32-545` as
/// `BUILTIN\Users`, localised to the host's display language), which is why the
/// callers compare rendered ACE lines rather than searching the text for a
/// foreign SID.
///
/// **The binary is [`acl::icacls_binary()`] and not `"icacls"`**, for the reason
/// that function exists: the confinement's own enforcement resolves the tool
/// through the system directory rather than the environment, and a test naming
/// it twice would be two ways to spell one tool in one phase — with the second
/// selectable by whoever set `PATH`.
pub fn icacls(path: &Path, args: &[&str]) -> String {
    let output = Command::new(acl::icacls_binary())
        .arg(path)
        .args(args)
        .output()
        .expect("run icacls");
    assert!(
        output.status.success(),
        "icacls {} {}: {}",
        path.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// `<link>` as a directory junction onto `target`, through `cmd`'s `mklink /J`.
///
/// **A junction and not a symlink.** `mklink /D` needs
/// `SeCreateSymbolicLinkPrivilege`, which an ordinary process does not hold;
/// `/J` needs no privilege at all, and the two are the same thing to every layer
/// the sandbox's tests are about — `std` reports both `is_symlink() == true`, and
/// the canonicalizer resolves both by reading the link (`read_link`) rather than
/// by opening the target.
///
/// **The fixture asserts itself, and that is load-bearing rather than tidy.**
/// Every assertion a caller makes after it is an *absence* — "the grant did not
/// follow the link", "the escaping bucket is refused" — and an `mklink` that
/// silently failed would make each of them pass with nothing planted. The
/// `is_symlink` check is the same argument one step further in: a directory that
/// was created but is not a reparse point would make the leg assert nothing.
///
/// **A host that refuses `mklink /J` fails the test rather than skipping it**,
/// which is the plans' own rule: a run that never made the link proves nothing
/// about the case the link exists for.
pub fn junction(link: &Path, target: &Path) {
    let output = Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(link)
        .arg(target)
        .output()
        .expect("run mklink");
    assert!(
        output.status.success(),
        "mklink /J {} -> {}: {}{}",
        link.display(),
        target.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata = fs::symlink_metadata(link).expect("the junction exists");
    assert!(
        metadata.file_type().is_symlink(),
        "{} was created but is not a reparse point, so the leg would assert nothing",
        link.display()
    );
}

/// The capture a test binary records through — **defined in [`tinio_util::testing`] and re-exported
/// here**, for [`wait_for_exit`]'s reason: this crate's root is `#![cfg(target_os = "windows")]`, so
/// the macOS and Linux legs that record their own events could not take a definer that lives here,
/// and each of them spelled its own copy of it. The device's doc carries the install, the level it
/// records at, the second-global-installer hazard and which threads each entry point owns; the two
/// callers of this path (`src/backend/tests.rs`, `tests/profile.rs`) are unchanged.
#[doc(inline)]
pub use tinio_util::testing::{Capture, Recording, capture, recorder};
