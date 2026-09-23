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
//! **Why it is a plain module and not a `testing` feature.** A feature is the
//! usual spelling of that convention (`tinio-util`'s), and it is what this
//! crate's own tests cannot use: `#[cfg(test)]` is invisible to `tests/`, and a
//! crate cannot enable a feature of its own for them — a dev-dependency on
//! itself is a cycle. A gated module would therefore be reachable by
//! `tinio-server`'s suite and not by this crate's, and the copy would survive in
//! exactly the file this module was extracted from. The cost is one small
//! module in a library `publish = false` keeps inside this workspace.
//!
//! **Nothing here asserts anything about the confinement.** These are devices a
//! test composes: what a test proves is still in the test. The one rule they do
//! carry is the harness's, and it is the reason they are shared rather than
//! copied — [`wait_for_exit`] kills the process it gives up on, so a suite that
//! wrote its own copy could assert on the deadline without killing the child
//! first (`tests/common/mod.rs`'s doc records the suite where it happened).
//!
//! Behind the crate's own `#![cfg(target_os = "windows")]` for the crate root's
//! reason: `crates/*` has no per-target member form, so this compiles on hosts
//! it does not serve.

use std::{
    fs,
    path::Path,
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};

use crate::acl;

/// Wait for `child` to exit, or kill it and fail the test rather than block on
/// it.
///
/// **Every spawned process is waited for or killed on every path, red ones
/// included**, and this is the one function that rule is spelled in — which is
/// why it is here rather than in each suite. The failing path kills before it
/// panics: a child left running keeps `target\debug\deps\<suite>-<hash>.exe`
/// open for the rest of the run, which breaks a later build with `LNK1104`, and
/// it keeps writing its own libtest output into the parent's.
///
/// `try_wait` polls rather than `wait_with_output` waiting, which is the price
/// of the bound: nothing here needs the child's captured output, since a child
/// that inherits this process's streams has already written it where the reader
/// of a red log will see it. `what` names the process whose exit is being waited
/// for, because a suite has more than one.
pub fn wait_for_exit(child: &mut Child, within: Duration, what: &str) -> ExitStatus {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "{what} did not exit within {within:?} — whatever it did is in the output above; \
                 it has been killed"
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

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
