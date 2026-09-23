//! ACL whitelist: grant the package SID an access mode on each exposed path,
//! and take those grants back on the way out (decisions 15, 21, §6).
//!
//! Edits go through `icacls.exe` resolved from the system directory, not
//! `PATH` and not `SetNamedSecurityInfo`. `/grant:r` replaces this SID's ACE
//! only; `/remove:g *<sid>` removes it. [`Error::Launch`] carries the tool's
//! status and text. This module has no `unsafe`; the kernel call is
//! [`crate::handles`].
//!
//! [`Target`] is the grant shape. `(OI)(CI)` on a file is a silent no-op
//! (exit 0, nothing written). Caller directories stay inherited: one ACE
//! reaches pre-existing descendants, with no `/t` and one `/remove:g`. The
//! program directory does not inherit. Sibling DLLs are one `*.dll` pattern;
//! a second path on that command line is refused. A caller's file never
//! reaches a grant: `canonical_caller` refuses it. The filesystem propagates
//! an inherited ACE, so the tool's "1 files" is not the cost; `backend` times
//! the phase.
//!
//! [`Error::Launch`]: crate::_core::Error::Launch

use std::{
    ffi::OsStr,
    io, mem,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Arc,
};

use crate::{
    _core::{AccessMode, Error, Result, launch},
    backend::SiblingScan,
    handles::system_directory,
    sid::PackageSid,
};

/// The shape a grant takes.
///
/// **The first variant is the ordinary one, and that is a correction rather than
/// an ordering.** This crate shipped a `Directory`/`File` pair whose `Directory`
/// meant "a directory, non-inherited", so every pre-existing call site that spelled
/// `Target::Directory` silently changed meaning when decision 22's entry stopped
/// propagating — and a confined child then could not read a pre-existing file under
/// an exposed directory. The rule the variants now encode is: **the obvious
/// spelling is the caller's**, and the exception is named after the one decision
/// that makes it.
///
/// **The two dimensions behind it are independent.** *What the path is* decides
/// whether inheritance is meaningful at all: `(OI)(CI)` is meaningful on a
/// directory only, so an inheritance-flagged grant on a **file** is a **silent
/// no-op** — `icacls` exits 0 and writes nothing — while the file shape sent to a
/// directory leaves every descendant ungranted. *Whether it propagates* is a policy:
/// a caller's exposed directory must reach the descendants it already holds (design
/// §6's whitelist, one ACE per exposed path instead of a `/t` walk and a per-file
/// rollback), and decision 22's program-directory entry deliberately does not.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::acl::{Target, ace_spec};
///
/// // The ordinary directory: the caller's exposure, which propagates.
/// assert_eq!(
///     ace_spec(Target::Directory, AccessMode::ReadWrite),
///     "(OI)(CI)M"
/// );
/// // The exception, and the plain file shape.
/// assert_eq!(
///     ace_spec(Target::ProgramDirectory, AccessMode::ReadOnly),
///     "RX"
/// );
/// assert_eq!(ace_spec(Target::File, AccessMode::ReadOnly), "RX");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// **An exposed directory, and the ordinary case**: the caller exposed this
    /// path, so the ACE carries `(OI)(CI)` and reaches the descendants the
    /// directory already holds (design §6's whitelist). A caller with a directory
    /// in hand means this one.
    Directory,
    /// **A file**: it takes the explicit non-inherited form and nothing else,
    /// because the flags are a no-op on one. This variant is passed by its two
    /// callers rather than read off the filesystem to pick it — `backend::confine`
    /// filters the file-shaped entries out before asking `shape_for`, and
    /// [`Grants::apply_siblings`] passes the literal for the tool's `*.dll`
    /// pattern.
    File,
    /// **The program's own directory, and decision 22's exception**: a directory
    /// whose ACE deliberately stops at it, because what the confined child may
    /// reach there is the files its loader reads rather than everything a build
    /// directory holds (Ruling 78).
    ProgramDirectory,
}

/// The permission specification `icacls` grants for a target and an access mode.
///
/// `M` is `FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE |
/// DELETE` (`0x001301BF`) and `RX` is the read and execute halves
/// (`0x001200A9`). `(OI)(CI)` means object-and-container inherit, **is meaningful
/// on a directory only**, and is what carries an exposed directory's ACE to the
/// descendants that were already there — and what makes the grant and its rollback
/// cost a pass over the whole tree (design §6 and its Risks row).
///
/// [`Target::Directory`] is therefore the only variant that carries the flags: a
/// file cannot take them (the no-op above) and the program's directory must not
/// (decision 22).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::acl::{Target, ace_spec};
///
/// // The caller's exposed directories propagate to what they already hold.
/// assert_eq!(
///     ace_spec(Target::Directory, AccessMode::ReadOnly),
///     "(OI)(CI)RX"
/// );
/// assert_eq!(
///     ace_spec(Target::Directory, AccessMode::ReadWrite),
///     "(OI)(CI)M"
/// );
/// // The program's directory and every file do not.
/// assert_eq!(
///     ace_spec(Target::ProgramDirectory, AccessMode::ReadOnly),
///     "RX"
/// );
/// assert_eq!(ace_spec(Target::File, AccessMode::ReadOnly), "RX");
/// ```
pub const fn ace_spec(target: Target, access: AccessMode) -> &'static str {
    match (target, access) {
        (Target::Directory, AccessMode::ReadOnly) => "(OI)(CI)RX",
        (Target::Directory, AccessMode::ReadWrite) => "(OI)(CI)M",
        (Target::ProgramDirectory | Target::File, AccessMode::ReadOnly) => "RX",
        (Target::ProgramDirectory | Target::File, AccessMode::ReadWrite) => "M",
    }
}

/// The `*<sid>:<perm>` argument `icacls` is given for `target`.
///
/// The leading `*` is what stops the tool resolving the string as an account
/// name: an AppContainer package SID has no account behind it, and a name form
/// would be looked up in the local accounts and fail — or, worse, be answered by
/// an account that happens to carry the name.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::acl::{Target, grant_spec};
///
/// assert_eq!(
///     grant_spec("S-1-15-2-1-2-3", Target::Directory, AccessMode::ReadOnly),
///     "*S-1-15-2-1-2-3:(OI)(CI)RX"
/// );
/// assert_eq!(
///     grant_spec("S-1-15-2-1-2-3", Target::File, AccessMode::ReadWrite),
///     "*S-1-15-2-1-2-3:M"
/// );
/// ```
pub fn grant_spec(sid: &str, target: Target, access: AccessMode) -> String {
    format!("*{sid}:{}", ace_spec(target, access))
}

/// `<system directory>\icacls.exe` — the kernel's own answer to where the system
/// directory is, so no part of this path is read from the environment.
///
/// **Robustness rather than defence, and the distinction is deliberate**: an
/// actor who can set this launcher's environment is a local actor, and local
/// actors are outside this feature's threat model, so nothing here is a defence
/// being claimed. What the rule buys is that the tool which edits the DACL is the
/// operating system's own even when `%SystemRoot%` is unset, stale or wrong. The
/// module's rule — the process that decides this run's DACLs is not selectable
/// through the environment — is what this function makes true of the code rather
/// than an intention stated beside it.
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

/// Grant `access` to `sid` on `path` in `target`'s shape, replacing any entry this
/// SID already has there and touching nothing else.
///
/// **Grant-only, never revoke-then-grant** (design decision 15): `/grant:r`
/// rewrites this one SID's entry, so a directory's other ACEs, its permission
/// bits and its owner are left as they were found — a sandbox run cannot lock an
/// operator out of their own data. It is also what makes decision 21's takeover
/// work: an ACE a crashed run left behind is *replaced* rather than duplicated.
///
/// **`target` is the caller's, and it is the caller's for each of the two
/// questions `Target` encodes** (its own doc): `backend::confine` asks
/// `shape_for` for every entry whose host is not a file, while
/// `Grants::apply_siblings` passes a literal `Target::File` because the
/// wildcard it grants through names only `*.dll` matches — neither site probes the path to
/// answer either question. A shape that disagrees with the path is a grant the
/// tool reports as successful and does not write (the module doc states the
/// measurement).
///
/// # Errors
///
/// [`Error::Launch`] naming the path and carrying `icacls`'s own diagnostic when
/// it exits non-zero.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::{
///     acl,
///     acl::Target,
///     sid::{PackageSid, package_name},
/// };
///
/// let root = Path::new(r"E:\srv\tinio");
/// let sid = PackageSid::derive(&package_name(root)).unwrap();
/// acl::grant(root, Target::Directory, AccessMode::ReadOnly, &sid).unwrap();
/// ```
pub fn grant(path: &Path, target: Target, access: AccessMode, sid: &PackageSid) -> Result<()> {
    let spec = grant_spec(sid.as_str(), target, access);
    run(
        "icacls /grant:r",
        path,
        &[OsStr::new("/grant:r"), OsStr::new(&spec), OsStr::new("/q")],
    )?;
    // Shared §7's per-operation event, with the three fields that matter here:
    // which path, which access, which SID.
    tracing::debug!(
        step = "icacls /grant:r",
        path = %path.display(),
        access = ?access,
        sid = sid.as_str()
    );
    Ok(())
}

/// Remove this SID's entries from `path`, leaving every other ACE alone.
///
/// Idempotent, and measured rather than assumed: `icacls <dir> /remove:g
/// "*<sid>" /q` for a SID with **no entry anywhere under the path** exits `0`
/// with `Successfully processed 1 files; Failed processing 0 files` (2026-09-21,
/// this host). That is what lets the rollback run over paths whose grant never
/// landed, and over a crashed run's residue that was already taken back.
///
/// # Errors
///
/// [`Error::Launch`], as [`grant`].
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_windows::{
///     acl,
///     sid::{PackageSid, package_name},
/// };
///
/// let root = Path::new(r"E:\srv\tinio");
/// let sid = PackageSid::derive(&package_name(root)).unwrap();
/// acl::revoke(root, &sid).unwrap();
/// ```
pub fn revoke(path: &Path, sid: &PackageSid) -> Result<()> {
    let spec = format!("*{}", sid.as_str());
    run(
        "icacls /remove:g",
        path,
        &[OsStr::new("/remove:g"), OsStr::new(&spec), OsStr::new("/q")],
    )?;
    tracing::debug!(step = "icacls /remove:g", path = %path.display(), sid = sid.as_str());
    Ok(())
}

/// Whether `path`'s DACL carries an entry for `sid`.
///
/// The oracle is `icacls`'s own rendering — the same tool the design's evidence
/// table was produced with. An AppContainer package SID does not resolve to an
/// account name, so the rendered form is the `S-1-15-2-…` string itself, which is
/// what a caller may match on. **A well-known SID does not come back that way**:
/// `icacls` renders `S-1-5-32-545` as `BUILTIN\Users` (localised to the host's
/// display language), so this predicate is for this crate's own SIDs and a test
/// asserting anything about a foreign ACE has to compare the DACL text rather
/// than search it for that SID (measured 2026-09-21).
///
/// **A non-zero exit is an error and not `Ok(false)`**, which is the difference
/// between an oracle and a guess: `invoke` reports a failed *spawn* and nothing
/// else, so a call that exited non-zero would hand back its empty stdout as "no
/// entry for this SID" — and every assertion that a rolled-back directory
/// carries no ACE would then pass because `icacls` refused to read the path
/// rather than because the ACE was gone.
///
/// # Errors
///
/// [`Error::Launch`] when `icacls` cannot read the DACL at all, carrying its own
/// exit status and text.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_windows::{
///     acl,
///     sid::{PackageSid, package_name},
/// };
///
/// let root = Path::new(r"E:\srv\tinio");
/// let sid = PackageSid::derive(&package_name(root)).unwrap();
/// assert!(!acl::has_ace(root, &sid).unwrap(), "nothing granted yet");
/// ```
pub fn has_ace(path: &Path, sid: &PackageSid) -> Result<bool> {
    let output = checked("icacls (read the DACL)", path, &[])?;
    Ok(String::from_utf8_lossy(&output.stdout).contains(sid.as_str()))
}

/// The grants this run applied, so that every exit path can take exactly those
/// back (design decision 15 — including the path where the launch itself failed,
/// not only the clean one).
///
/// # Examples
///
/// ```no_run
/// use std::{path::Path, sync::Arc};
///
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::{
///     acl,
///     sid::{PackageSid, package_name},
/// };
///
/// let root = Path::new(r"E:\srv\tinio");
/// let sid = PackageSid::derive(&package_name(root)).unwrap();
/// let mut grants = acl::Grants::new(Arc::new(sid));
/// grants
///     .apply(root, acl::Target::Directory, AccessMode::ReadOnly)
///     .unwrap();
/// grants.rollback().unwrap();
/// ```
pub struct Grants {
    sid: Arc<PackageSid>,
    applied: Vec<PathBuf>,
}

impl Grants {
    /// An empty grant set for `sid`.
    ///
    /// The SID is held by [`Arc`] because the launcher needs it after the grants
    /// have been taken back: the same value goes to the launch, and this set is
    /// one owner among several rather than the owner.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{path::Path, sync::Arc};
    ///
    /// use tinio_sandbox_windows::{
    ///     acl,
    ///     sid::{PackageSid, package_name},
    /// };
    ///
    /// let root = Path::new(r"E:\srv\tinio");
    /// let sid = PackageSid::derive(&package_name(root)).unwrap();
    /// let grants = acl::Grants::new(Arc::new(sid));
    /// assert!(grants.is_empty());
    /// ```
    pub fn new(sid: Arc<PackageSid>) -> Self {
        Self {
            sid,
            applied: Vec::new(),
        }
    }

    /// Grant `access` on `path` in `target`'s shape and remember it.
    ///
    /// The path is recorded only once its grant has succeeded, so a failure here
    /// never records a path whose ACE was not written. A startup that dies on its
    /// third exposure therefore takes back the two grants that landed and claims
    /// nothing for the one that did not — the rollback never goes looking for an
    /// entry it did not write, which is what keeps its failures meaningful.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] from [`grant`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{path::Path, sync::Arc};
    ///
    /// use tinio_sandbox_core::AccessMode;
    /// use tinio_sandbox_windows::{
    ///     acl::{self, Target},
    ///     sid::{PackageSid, package_name},
    /// };
    ///
    /// let root = Path::new(r"E:\srv\tinio");
    /// let sid = PackageSid::derive(&package_name(root)).unwrap();
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// grants
    ///     .apply(root, Target::Directory, AccessMode::ReadOnly)
    ///     .unwrap();
    /// assert!(!grants.is_empty());
    /// ```
    pub fn apply(&mut self, path: &Path, target: Target, access: AccessMode) -> Result<()> {
        grant(path, target, access, &self.sid)?;
        self.applied.push(path.to_path_buf());
        Ok(())
    }

    /// Grant `access` on [`scan`](SiblingScan)'s files in **one `icacls`
    /// invocation** and remember the one path that was granted, so the rollback
    /// removes it in one call too.
    ///
    /// **This is decision 22's sibling-DLL half (Ruling 78), and the batch is the
    /// tool's wildcard rather than a list of paths** — `icacls` accepts a single
    /// `name` pattern and no second path (measured 2026-09-21 on this host; the
    /// measurement and its refused forms are on [`SiblingScan::pattern`]). The
    /// recorded path is the pattern the grant actually went through, so the
    /// rollback removes exactly what was granted rather than re-deriving it.
    ///
    /// **The scan is a parameter and not a `&Path`**, which is the whole reason
    /// [`SiblingScan`] exists: this call and the caller's own exposure set must
    /// be about one file set, and a `&Path` here would re-enumerate the directory
    /// — a second answer to a question already answered, and a second chance for
    /// the grant to name a DLL the exposure set does not.
    ///
    /// The `bool` is whether a sibling existed at all: `false` means nothing was
    /// granted, nothing was recorded, and an unmatched pattern was never handed to
    /// the tool.
    ///
    /// **This is the sibling rule's one implementation.** It had a free-function
    /// twin (`grant_siblings`) whose body this repeats, and a matching
    /// `revoke_siblings` the rollback never called — [`Self::rollback`] removes the
    /// recorded pattern itself. Both are gone: two spellings of the one decision
    /// Ruling 78 turned on is how the grant and its undo drift apart, and neither
    /// free copy had a caller outside its own doctest.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] from [`grant`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{path::Path, sync::Arc};
    ///
    /// use tinio_sandbox_core::AccessMode;
    /// use tinio_sandbox_windows::{
    ///     acl,
    ///     backend::SiblingScan,
    ///     sid::{PackageSid, package_name},
    /// };
    ///
    /// let dir = Path::new(r"E:\build");
    /// let sid = PackageSid::derive(&package_name(dir)).unwrap();
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// // `false` when the directory holds no DLL: nothing granted, nothing recorded.
    /// let granted = grants
    ///     .apply_siblings(&SiblingScan::scan(dir), AccessMode::ReadOnly)
    ///     .unwrap();
    /// # let _ = granted;
    /// ```
    pub fn apply_siblings(&mut self, scan: &SiblingScan, access: AccessMode) -> Result<bool> {
        match scan.pattern() {
            Some(pattern) => {
                grant(&pattern, Target::File, access, &self.sid)?;
                self.applied.push(pattern);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Whether anything is still granted.
    ///
    /// **`true` does not mean the disk is clean, and reading it that way is the
    /// mistake this note exists to prevent.** [`Self::rollback`] pops every path it
    /// attempts, so a rollback that *failed* also leaves this set empty: the paths
    /// it could not fix are named by the [`Error`] the rollback returned —
    /// and by the log line [`launch`] built it at — rather than by this predicate.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{path::Path, sync::Arc};
    ///
    /// use tinio_sandbox_windows::{
    ///     acl,
    ///     sid::{PackageSid, package_name},
    /// };
    ///
    /// let root = Path::new(r"E:\srv\tinio");
    /// let sid = PackageSid::derive(&package_name(root)).unwrap();
    /// assert!(acl::Grants::new(Arc::new(sid)).is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }

    /// Remove every grant this run applied, in reverse order, and forget them.
    ///
    /// The sibling set goes back in **one invocation** (Ruling 78):
    /// [`Self::apply_siblings`] recorded the one pattern it granted through, so the
    /// rollback costs what the grant cost instead of one process per sibling, and it
    /// removes exactly what was granted rather than re-deriving it.
    ///
    /// A failure does not stop the loop: it is kept and returned after every
    /// other path has been attempted, because a rollback that stopped at the
    /// first error would leave the rest of the residue behind. It is not printed
    /// here either — [`launch`] logged it where the `Error` was built, with the
    /// same `step` this method's caller will see (shared §7).
    ///
    /// # Errors
    ///
    /// The first failure, after attempting every other path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{path::Path, sync::Arc};
    ///
    /// use tinio_sandbox_core::AccessMode;
    /// use tinio_sandbox_windows::{
    ///     acl,
    ///     sid::{PackageSid, package_name},
    /// };
    ///
    /// let root = Path::new(r"E:\srv\tinio");
    /// let sid = PackageSid::derive(&package_name(root)).unwrap();
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// grants
    ///     .apply(root, acl::Target::Directory, AccessMode::ReadOnly)
    ///     .unwrap();
    /// grants.rollback().unwrap();
    /// assert!(grants.is_empty());
    /// ```
    pub fn rollback(&mut self) -> Result<()> {
        // `mem::take` first, so the loop below borrows the set's own SID rather
        // than the field it just drained.
        let applied = mem::take(&mut self.applied);
        let mut first: Option<Error> = None;
        for path in applied.into_iter().rev() {
            if let Err(err) = revoke(&path, &self.sid) {
                first.get_or_insert(err);
            }
        }
        match first {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl Drop for Grants {
    fn drop(&mut self) {
        // Every exit path, including a panic: design §11 requires the rollback
        // after a failed launch too, and a guard is the only shape that cannot be
        // forgotten on one of them. The error is dropped on purpose — `Drop` has
        // no caller to report to, and the failure already reached the log where
        // it was built (shared §7, `launch`'s own event).
        if !self.applied.is_empty() {
            let _ = self.rollback();
        }
    }
}

/// Run `icacls` on `path` with `flags`, refusing a non-zero exit.
fn run(operation: &'static str, path: &Path, flags: &[&OsStr]) -> Result<()> {
    checked(operation, path, flags).map(drop)
}

fn checked(operation: &'static str, path: &Path, flags: &[&OsStr]) -> Result<Output> {
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
fn invoke(path: &Path, flags: &[&OsStr]) -> Result<Output> {
    let mut command = Command::new(icacls_binary());
    command.arg(path).args(flags);
    command
        .output()
        .map_err(|source| launch("run icacls", path.to_path_buf(), source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::AccessMode;

    #[test]
    fn only_an_exposed_directory_propagates_its_ace() {
        // Three shapes, and the two dimensions are why: the caller's exposed
        // directories keep `(OI)(CI)` — design §6's whitelist, and the reason a
        // pre-existing descendant is reachable without a `/t` walk — while the
        // program's directory (decision 22) and every file take the plain form. An
        // inheritance-flagged grant on a file is a silent no-op (design §6's
        // evidence row), which `tests/acl.rs` pins.
        assert_eq!(
            ace_spec(Target::Directory, AccessMode::ReadOnly),
            "(OI)(CI)RX"
        );
        assert_eq!(
            ace_spec(Target::Directory, AccessMode::ReadWrite),
            "(OI)(CI)M"
        );
        assert_eq!(
            ace_spec(Target::ProgramDirectory, AccessMode::ReadOnly),
            "RX"
        );
        assert_eq!(
            ace_spec(Target::ProgramDirectory, AccessMode::ReadWrite),
            "M"
        );
        assert_eq!(ace_spec(Target::File, AccessMode::ReadOnly), "RX");
        assert_eq!(ace_spec(Target::File, AccessMode::ReadWrite), "M");
    }

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

    #[test]
    fn the_grant_argument_names_the_sid_literally() {
        // The `*` prefix is what stops icacls resolving the SID as a name, and the
        // spec it prefixes is the target's.
        assert_eq!(
            grant_spec("S-1-15-2-1-2-3", Target::Directory, AccessMode::ReadOnly),
            "*S-1-15-2-1-2-3:(OI)(CI)RX"
        );
        assert_eq!(
            grant_spec(
                "S-1-15-2-1-2-3",
                Target::ProgramDirectory,
                AccessMode::ReadOnly
            ),
            "*S-1-15-2-1-2-3:RX"
        );
        assert_eq!(
            grant_spec("S-1-15-2-1-2-3", Target::File, AccessMode::ReadWrite),
            "*S-1-15-2-1-2-3:M"
        );
    }
}
