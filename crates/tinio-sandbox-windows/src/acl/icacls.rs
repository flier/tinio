//! **The tool and its text: where `icacls` is, which spellings a SID may be rendered under,
//! and the three helpers that run it.** The launcher no longer grants through the tool — the
//! write is a handle's — so what this module serves now is the operator's `revoke` and every
//! suite and fixture that uses the tool as an independent oracle, including the one read that
//! still goes through text: [`mentions_sid`].

use std::{
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use tinio_util::win32::system_directory;

use crate::_core::{Result, launch};
#[cfg(any(test, feature = "testing"))]
use crate::sid::ALL_APPLICATION_PACKAGES;

/// `<system directory>\icacls.exe` — the kernel's own answer to where the system
/// directory is, so no part of this path is read from the environment.
///
/// **Robustness rather than defence, and the distinction is deliberate**: an
/// actor who can set this launcher's environment is a local actor, and local
/// actors are outside this feature's threat model, so nothing here is a defence
/// being claimed. What the rule buys is that the tool this crate shells out to is
/// the operating system's own even when `%SystemRoot%` is unset, stale or wrong.
/// The module's rule — the process that decides this run's DACLs is not selectable
/// through the environment — is what this function makes true of the code rather
/// than an intention stated beside it.
///
/// **The grant no longer goes through the tool** (`Object` writes on a handle);
/// what still does is [`revoke`](super::grant::revoke), the operator's own primitive, and every suite
/// and fixture that uses the tool as an independent oracle — which is why this
/// path is still the system directory's rather than `PATH`'s.
///
/// # Examples
///
/// ```
/// let binary = tinio_sandbox_windows::acl::icacls_binary();
///
/// assert!(binary.is_absolute(), "{}", binary.display());
/// assert!(binary.to_string_lossy().to_lowercase().contains("system32"));
/// ```
pub fn icacls_binary() -> PathBuf {
    system_directory().join("icacls.exe")
}

/// The spellings `icacls` may render `sid` under in its listing.
///
/// **A well-known SID is listed by name, not by its literal.** A grant to
/// [`ALL_APPLICATION_PACKAGES`] (`S-1-15-2-1`) comes back as
/// `APPLICATION PACKAGE AUTHORITY\ALL APPLICATION PACKAGES:(RX)`, so a text
/// oracle that looks only for the literal never sees the ACE that is there.
/// Measured on this host 2026-09-25: that is what made the sibling batch's skip
/// never fire, and what
/// `the_sibling_batch_grants_every_dll_for_all_application_packages`'s
/// post-condition could not see either — which is why the *read* no longer goes
/// through text at all ([`has_ace`](super::grant::has_ace)).
///
/// A package SID (`S-1-15-2-<8 groups>`) has no name to render, so it comes back
/// literally and needs no second spelling.
#[cfg(any(test, feature = "testing"))]
pub(super) fn sid_spellings(sid: &str) -> Vec<&str> {
    if sid == ALL_APPLICATION_PACKAGES {
        vec![sid, "ALL APPLICATION PACKAGES"]
    } else {
        vec![sid]
    }
}

/// Whether `text` — an `icacls` listing — carries an entry for `sid`, in any
/// spelling `sid_spellings` allows.
///
/// **It is a test oracle now and no longer the production read.** [`has_ace`](super::grant::has_ace) and
/// the skip ask the kernel's DACL through `walk_dacl`, which compares trustees
/// with `EqualSid`, so nothing this crate *decides* rides on how a SID is
/// rendered; what still does is every suite that asserts against the tool's own
/// output — `the_sibling_batch_grants_every_dll_for_all_application_packages`
/// pins [`ALL_APPLICATION_PACKAGES`] on the sibling batch through it, and
/// `tests/confine.rs` asks whether the program directory carries the standing SID
/// at all. Keeping one implementation of that question is what
/// stops the oracle and the tool from drifting apart, and the rendering it handles
/// is the reason the predicate exists: `S-1-15-2-1` is printed as
/// `ALL APPLICATION PACKAGES`.
///
/// Public because those suites ask it and the text they hold is the tool's.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::acl::mentions_sid;
///
/// // The literal spelling, which is what a package SID comes back as.
/// let listing = r"D:\srv\tinio:\S-1-15-2-1:(OI)(CI)(RX)";
/// assert!(mentions_sid(listing, "S-1-15-2-1"));
/// // And the name `icacls` renders a well-known SID under, which is the reason
/// // this predicate exists at all.
/// let named = r"D:\srv APPLICATION PACKAGE AUTHORITY\ALL APPLICATION PACKAGES:(RX)";
/// assert!(mentions_sid(named, "S-1-15-2-1"));
/// ```
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn mentions_sid(text: &str, sid: &str) -> bool {
    text.lines().any(|line| {
        sid_spellings(sid)
            .into_iter()
            .any(|spelling| line.contains(spelling))
    })
}

/// Run `icacls` on `path` with `flags`, refusing a non-zero exit.
pub(super) fn run(operation: &'static str, path: &Path, flags: &[&OsStr]) -> Result<()> {
    checked(operation, path, flags).map(drop)
}

pub(super) fn checked(operation: &'static str, path: &Path, flags: &[&OsStr]) -> Result<Output> {
    let output = invoke(path, flags)?;
    if output.status.success() {
        return Ok(output);
    }
    let diagnostic = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let status = output.status.code().unwrap_or(-1);
    Err(launch(
        operation,
        path.to_path_buf(),
        io::Error::other(format!("icacls exited {status}: {diagnostic}")),
    ))
}

/// Run `icacls` on `path`, with the path first as icacls' own syntax requires,
/// and no shell in between.
///
/// The tool's write is [`revoke`](super::grant::revoke)'s now — the grant does not go through it — and
/// the one production caller is what this function and the two above are kept for.
pub(super) fn invoke(path: &Path, flags: &[&OsStr]) -> Result<Output> {
    let mut command = Command::new(icacls_binary());
    command.arg(path).args(flags);
    command
        .output()
        .map_err(|source| launch("run icacls", path.to_path_buf(), source))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;
    use crate::acl::{
        descriptor::{OwnedSid, acl_names},
        test_support::text_mentions_sid_among_the_entries,
    };

    #[test]
    fn the_icacls_binary_comes_from_the_system_directory_not_the_environment() {
        // The process that decides this run's DACLs must not be selectable
        // through the environment. Robustness rather than defence — a local
        // actor is out of the threat model — but the rule in `icacls_binary`'s
        // doc is only true, and this test only meaningful, if nothing on the
        // path is read from the environment.
        let binary = icacls_binary();
        assert!(binary.is_absolute(), "{}", binary.display());
        assert_eq!(
            binary.file_name().and_then(|name| name.to_str()),
            Some("icacls.exe")
        );
        assert!(binary.to_string_lossy().to_lowercase().contains("system32"));
    }

    /// **The defect this module's read was rewritten for, as a test.**
    ///
    /// The old read asked `icacls`'s text whether the literal SID was in the
    /// listing, and the tool prints a well-known SID by **name** — so the entry
    /// line for the ACE it had just written carried no `S-1-15-2-1`, the check
    /// answered "not granted", and the standing skip missed on every run. A claim
    /// about behaviour that was never observed is what shipped this the first
    /// time, so the failure is pinned beside the predicate that fixes it.
    #[test]
    fn a_well_known_sid_is_invisible_to_the_text_and_visible_to_the_read() {
        let dir = tempfile::tempdir().expect("temp dir");
        let granted = Command::new(icacls_binary())
            .arg(dir.path())
            .args([
                "/grant:r",
                &format!("*{ALL_APPLICATION_PACKAGES}:(OI)(CI)RX"),
                "/q",
            ])
            .output()
            .expect("run icacls");
        assert!(
            granted.status.success(),
            "{}",
            String::from_utf8_lossy(&granted.stderr)
        );
        let listing = checked("icacls (a test's own listing)", dir.path(), &[]).expect("read");
        let text = String::from_utf8_lossy(&listing.stdout);
        assert!(
            text.contains("ALL APPLICATION PACKAGES"),
            "the tool did not render the SID by name after all, so this test is \
             not the measurement it claims to be:\n{text}"
        );
        assert!(
            !text_mentions_sid_among_the_entries(&text, ALL_APPLICATION_PACKAGES),
            "the literal SID is in the entry lines, so the defect this pins was \
             not reproduced on this host:\n{text}"
        );
        // **And the read that replaced it sees the ACE**: the same question, with
        // the SID compared as a SID rather than searched for as a string.
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");
        assert!(
            acl_names(dir.path(), sid.raw()).expect("read the DACL"),
            "the kernel's DACL names the SID the tool rendered by name:\n{text}"
        );

        let taken_back = Command::new(icacls_binary())
            .arg(dir.path())
            .args(["/remove:g", "*S-1-15-2-1", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            taken_back.status.success(),
            "{}",
            String::from_utf8_lossy(&taken_back.stderr)
        );
    }

    /// The read's own comparison, against a SID that is not a package SID at
    /// all — the case the text oracle could never answer for this crate.
    #[test]
    fn the_read_recognizes_a_well_known_sid_the_text_cannot() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        let root = || {
            let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");
            acl_names(path, sid.raw()).expect("read the DACL")
        };
        assert!(!root(), "nothing granted yet");

        let granted = Command::new(icacls_binary())
            .arg(path)
            .args(["/grant:r", "*S-1-15-2-1:RX", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            granted.status.success(),
            "{}",
            String::from_utf8_lossy(&granted.stderr)
        );
        // A file-shaped grant, so the flags question does not enter it: this is
        // the plain "is this SID in the DACL" question.
        assert!(
            root(),
            "the API read sees the ACE the tool rendered by name"
        );

        let taken_back = Command::new(icacls_binary())
            .arg(path)
            .args(["/remove:g", "*S-1-15-2-1", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            taken_back.status.success(),
            "{}",
            String::from_utf8_lossy(&taken_back.stderr)
        );
        assert!(!root(), "the removed ACE is gone");
    }
}
