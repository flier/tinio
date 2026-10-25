//! **The grant itself: the entry points (`grant`, `revoke`, `has_ace`), the SID a grant travels
//! as ([`GrantSid`]), and the standing batch that keeps this root's and [`ALL_APPLICATION_PACKAGES`]'
//! ACEs in place.** The only module here that may depend on all its siblings: a grant holds the
//! per-path lock, opens the object, asks the descriptor read whether the grant is already
//! satisfied, and opens a second time to write only when it is not.
//!
//! [`Target`] is the grant shape, and the shape is checked against the object the
//! handle opened rather than trusted: an inheritance-flagged ACE on a **file** is
//! a silent no-op when `icacls` writes it (measured: exit 0, nothing written) and
//! an inheritable ACE that nothing inherits from when the API does, so a shape that
//! disagrees with the path is refused instead of granted. Caller directories stay
//! inherited: one ACE reaches pre-existing descendants, with no `/t` and one
//! `/remove:g`. The program directory does not inherit. Sibling DLLs are granted
//! **one file at a time, each opened by handle** — a `*.dll` wildcard names objects
//! no handle can be opened on, and the pattern is what let a linked `x.dll` be
//! granted through the link. A caller's file never reaches a grant:
//! `canonical_whitelist` refuses it. The filesystem propagates an inherited ACE, so
//! one descriptor write is a pass over the whole subtree; `backend` times the phase.

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::Arc,
};

use windows_sys::Win32::Security::PSID;

#[cfg(any(test, feature = "testing"))]
use super::descriptor::acl_names;
#[cfg(test)]
use super::test_support::overlap;
use super::{
    descriptor::{Object, OwnedSid},
    icacls::run,
    lock::AclLock,
    spec::Target,
};
use crate::{
    _core::{AccessMode, Result},
    sid::{ALL_APPLICATION_PACKAGES, PackageSid},
};

/// Grant `access` to `sid` on `path` in `target`'s shape, replacing any entry this
/// SID already has there and touching nothing else.
///
/// **Grant-only, never revoke-then-grant** (design decision 15): the entry is
/// merged with `SET_ACCESS`, which replaces this one SID's ACE, so a directory's
/// other ACEs, its permission bits and its owner are left as they were found — a
/// sandbox run cannot lock an operator out of their own data. It is also what makes
/// decision 21's takeover work: an ACE a crashed run left behind is *replaced*
/// rather than duplicated (measured 2026-09-27: a second write with a different
/// mask **and** different inheritance flags leaves one entry for the SID, not two).
///
/// **The path is opened, not resolved**: see `Object` for the handle the read and
/// the write share and for the two refusals — a reparse point, and an object whose
/// kind disagrees with `target` — that `icacls` could not report (its
/// inheritance-flagged grant on a file exits 0 and writes nothing).
///
/// **`target` is the caller's, and it is the caller's for each of the two
/// questions `Target` encodes** (its own doc): `backend::confine` asks
/// `shape_for` for every entry, while `Grants::apply_siblings` passes a literal
/// `Target::File` for each enumerated `*.dll` — neither site probes the path to
/// answer either question, and a shape that disagrees with the object it reaches is
/// refused rather than approximated.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) naming the path and carrying the
/// Win32 status when the object cannot be opened or the descriptor cannot be
/// written, and [`Error::BadPath`](crate::_core::Error::BadPath) for a path that is
/// a reparse point or an object of the wrong kind.
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
#[cfg(any(test, feature = "testing"))]
pub fn grant(path: &Path, target: Target, access: AccessMode, sid: &PackageSid) -> Result<()> {
    grant_to(path, target, access, GrantSid::of(sid)).map(drop)
}

/// One SID as the two readings a grant needs: **the resolved value the kernel
/// compares and writes, and the `S-1-15-2-…` text an operator's log shows.**
///
/// **They travel as one value because they are two readings of one SID, and
/// nothing may derive either from the other.** [`has_ace`]'s doc states the rule:
/// a [`PackageSid`] already *is* the SID its profile was created under, so a
/// `ConvertStringSidToSidW` on `as_str()` would resolve the same SID a second
/// time — an allocation and a `LocalFree` per granted path, which is what made a
/// launch with a dozen exposures do a dozen needless conversions. The text is no
/// longer an argument to anything (`SetEntriesInAclW` takes the pointer), so what
/// it is for is the per-path event: an operator greps the `S-1-15-2-…` their own
/// root derives, not a pointer.
///
/// [`Self::of`] is the route for a SID this crate already owns; the standing
/// [`ALL_APPLICATION_PACKAGES`] grant arrives as text and goes through
/// [`OwnedSid::parse`] first, which is the one place a parse is still owed.
#[derive(Clone, Copy)]
pub(super) struct GrantSid<'a> {
    /// The resolved SID, as every Win32 call takes it.
    pub(super) raw: PSID,
    /// The rendered form, for the event a run leaves behind.
    pub(super) text: &'a str,
}

impl<'a> GrantSid<'a> {
    /// The pair for a SID that is already resolved.
    fn of(sid: &'a PackageSid) -> Self {
        Self {
            raw: sid.raw(),
            text: sid.as_str(),
        }
    }
}

/// Grant `access` to `sid` on the object `path` denotes, in `target`'s shape.
///
/// **Open, verify, read, and — only when the read says the grant is due — open
/// again for the write.** A satisfied path costs one `READ_CONTROL` open and one
/// descriptor read and never asks for `WRITE_DAC`; a path whose ACE is missing or
/// too narrow is opened a second time with the write access and written through
/// that handle, after [`Object::verify`] has answered the same two refusals for it.
/// The `bool` says whether the descriptor was written, which is what the per-path
/// event and [`Grants::apply_standing`] report.
///
/// **The whole of it is held under one [`AclLock`], taken before the first read
/// and released when this function returns** — the lock file's open and its
/// exclusive hold are the per-call constant on top of the opens above, and
/// [`Grants::apply`], [`Grants::apply_standing`] and [`Grants::apply_siblings`]
/// all reach it through here, so no caller can write a DACL outside it.
///
/// **The event is the phase's own step name**, because that is what this is: one
/// ACE of the `grant the ACLs` phase, at `debug!` with `written` telling the skip
/// from the write (shared §7's per-operation event).
///
/// **The SID arrives as a [`GrantSid`] and is never re-parsed from its text**: the
/// read and the write both compare and store the value the caller already holds,
/// and the derived text only ever reaches a log line.
pub(super) fn grant_to(
    path: &Path,
    target: Target,
    access: AccessMode,
    sid: GrantSid<'_>,
) -> Result<bool> {
    let _lock = AclLock::acquire(path);
    let read = Object::open(path)?;
    read.verify(target)?;
    if read.granted(sid.raw, target, access)? {
        tracing::debug!(
            step = "grant the ACLs",
            path = %path.display(),
            access = ?access,
            sid = sid.text,
            written = false
        );
        return Ok(false);
    }
    // **The gap the whole lock exists for, widened on request by the test that
    // proves it is a real gap**: a `#[cfg(test)]` sleep a concurrency test arms
    // in two threads, so that both reads are certainly before either write when
    // the lock is absent — which is what makes the lost ACE reproducible rather
    // than a matter of scheduling luck (`acl`'s `two_grants_to_one_path_from_two_
    // threads_keep_both_aces`). Production builds compile this to nothing.
    #[cfg(test)]
    overlap::settle();
    // The read handle is given up before the second open, so at most one handle to
    // this object is live at a time and the write never depends on the read's.
    drop(read);
    let object = Object::open_for_write(path)?;
    object.verify(target)?;
    object.write(sid.raw, target, access)?;
    tracing::debug!(
        step = "grant the ACLs",
        path = %path.display(),
        access = ?access,
        sid = sid.text,
        written = true
    );
    Ok(true)
}

/// Remove this SID's entries from `path`, leaving every other ACE alone.
///
/// Idempotent, and measured rather than assumed: `icacls <dir> /remove:g
/// "*<sid>" /q` for a SID with **no entry anywhere under the path** exits `0`
/// with `Successfully processed 1 files; Failed processing 0 files` (2026-09-21,
/// this host). That is what lets a `revoke` run over a path whose grant never
/// landed, and over a crash's residue that was already taken back — and it is
/// the operator's primitive rather than the launcher's: nothing on the exit path
/// calls it (`Grants`'s doc states the standing contract).
///
/// **It takes the same per-path `AclLock` a grant does, because it writes the
/// same DACL.** A revoke and a grant are separate operations on one descriptor,
/// so a `remove` that lands between a peer grant's read and its write is undone
/// by that write and a `remove` that reads nothing is not a merge at all — the
/// lock is what makes the two shapes of one directory's DACL a sequence rather
/// than an interleaving. The launcher itself never revokes (decision 15), so what
/// this costs is one file open and one lock per operator invocation.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch), as `grant`.
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
    let _lock = AclLock::acquire(path);
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
/// **The oracle is the kernel's DACL and not the tool's rendering of it**
/// (`GetNamedSecurityInfoW`, then `walk_dacl`'s ACE-by-ACE comparison, which
/// asks `EqualSid` and no rendering),
/// and this predicate used to be wrong for the reason the module doc opens with:
/// `icacls` renders a well-known SID by **name**. `S-1-5-32-545` comes back as
/// `BUILTIN\Users` — localised to the host's display language — and
/// [`ALL_APPLICATION_PACKAGES`] comes back as `APPLICATION PACKAGE AUTHORITY\ALL
/// APPLICATION PACKAGES:(OI)(CI)(RX)`, so a search of that text is a search for
/// the *system's* name for a SID and not for the SID. The comparison below has no
/// such step: two SIDs are equal or they are not, and no account database, locale
/// or unresolvable SID can change the answer.
///
/// **A failed read is an error and not `Ok(false)`**, which is the difference
/// between an oracle and a guess. It always was the rule; what changed is where
/// the failure comes from — `GetNamedSecurityInfoW` reports it directly, with the
/// Win32 code the descriptor read failed under, and a path that does not exist
/// answers `ERROR_FILE_NOT_FOUND` (measured 2026-09-25) rather than the tool's
/// exit 2. Answering "no entry for this SID" for a path that could not be read
/// would make every assertion that a revoked directory carries no ACE pass for
/// the wrong reason, and would skip a needed grant in `already_granted`.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) naming `path` when the DACL
/// cannot be read at all, with the Win32 error as its source.
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
#[cfg(any(test, feature = "testing"))]
pub fn has_ace(path: &Path, sid: &PackageSid) -> Result<bool> {
    // The `PackageSid` already *is* the SID this asserts against — it was
    // converted once when the profile was created — so nothing is derived from
    // `sid.as_str()` here and no second spelling of the SID exists to disagree
    // with the first.
    acl_names(path, sid.raw())
}

/// **The root's package SID, and the grants it keeps alive — standing, never
/// taken back.**
///
/// The value is the SID and a lifetime, not a set of applied paths: every grant
/// this crate makes is *standing*, so there is nothing to undo. What it still
/// owns is the [`Arc<PackageSid>`] — the profile has to outlive the child, because
/// `PackageSid`'s `Drop` deletes the profile and a confined process cannot run
/// under a profile that is gone.
///
/// **Why grants are standing.** Undoing them cost one `icacls /remove:g` per path
/// on every exit, and a descriptor write propagates eagerly over a directory's
/// whole subtree (10–20 s for this workspace's `target\debug`, 71 757 objects,
/// measured 2026-09-24; `docs/tests.md`'s Windows section). The ACE a run leaves
/// is also the ACE the next run on the same root reuses — the SID is root-derived
/// and deterministic — and a crashed run's residue was already the designed
/// takeover case, so taking it back bought nothing and cost seconds per launch.
///
/// **Measured on this host 2026-09-25, reproduced on three binary revisions:
/// readiness on a root that has already been served falls from 11 287.9 ms to
/// 65.2 ms (about 170x)** once the skip finds the standing ACE, where the first
/// run on that root pays one descriptor write over 82 184 objects at 0.136 ms
/// per object.
///
/// **What that gives up is stated rather than implied: a served root keeps its
/// package SID's ACE until an operator takes it back**, with `icacls <dir>
/// /remove:g "*<sid>"`. The launcher removes nothing on the way out — not on the
/// clean path and not on the failure path — and what it leaves is the ACE the
/// next run on that root reuses. **This crate's design says exactly that**; the
/// sentence that used to stand here cited the design for the opposite contract,
/// and the citation was false rather than stale.
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
/// let grants = acl::Grants::new(Arc::new(sid));
/// grants
///     .apply(root, acl::Target::Directory, AccessMode::ReadOnly)
///     .unwrap();
/// ```
pub struct Grants {
    sid: Arc<PackageSid>,
}

impl Grants {
    /// The grant guard for `sid`.
    ///
    /// The SID is held by [`Arc`] because this value is one owner among several:
    /// the launch holds another, and the profile has to outlive the child.
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
    /// # let _ = grants;
    /// ```
    pub fn new(sid: Arc<PackageSid>) -> Self {
        Self { sid }
    }

    /// Authorize `path` for **this root's** package SID, in `target`'s shape.
    ///
    /// Idempotent, and free once satisfied: `grant_to` reads the DACL of the object
    /// it opened and writes only what is missing, so the second run on the same root
    /// costs one open and one read instead of a descriptor write. What is left is
    /// the per-call constant `AclLock` adds — one lock file's open and one
    /// exclusive hold on it, per path.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](crate::_core::Error::Launch) from `grant_to` when the path
    /// cannot be opened or written, and
    /// [`Error::BadPath`](crate::_core::Error::BadPath) when it is a reparse point or
    /// an object of the wrong kind.
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
    /// let grants = acl::Grants::new(Arc::new(sid));
    /// grants
    ///     .apply(root, Target::Directory, AccessMode::ReadOnly)
    ///     .unwrap();
    /// ```
    pub fn apply(&self, path: &Path, target: Target, access: AccessMode) -> Result<()> {
        grant_to(path, target, access, GrantSid::of(&self.sid)).map(drop)
    }

    /// Authorize `path` for [`ALL_APPLICATION_PACKAGES`] — **the one standing,
    /// machine-wide grant**, for decision 22's program directory and, one object at
    /// a time, the `*.dll` files directly beside it ([`Self::apply_siblings`]).
    ///
    /// Every AppContainer token already carries that SID, so this grant outlives
    /// every root: a new root never rewrites the program directory's descriptor,
    /// which is the per-launch cost this exists to remove. The event says whether
    /// this run wrote it or found it there (`written`), so the standing fact — and
    /// a directory that already carried it for another reason — is observable
    /// rather than inferred. **It is `debug!`, one event per ACE**: the launch's
    /// `info!` is [`grant_whitelist`](mod@crate::backend)'s phase line, and the level
    /// split is what keeps a per-entry event out of an operator's log.
    ///
    /// **It is for the read-only program directory and its sibling DLLs and for
    /// nothing else.** [`ALL_APPLICATION_PACKAGES`] carries the price; a served
    /// root or a state directory granted it would let a process confined to one
    /// root write another's.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](crate::_core::Error::Launch) from `grant_to`.
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
    /// let dir = Path::new(r"E:\build");
    /// let sid = PackageSid::derive(&package_name(dir)).unwrap();
    /// let grants = acl::Grants::new(Arc::new(sid));
    /// // One ACE for `ALL APPLICATION PACKAGES`, which every container already
    /// // holds, so a new root never rewrites this directory.
    /// grants
    ///     .apply_standing(dir, Target::ProgramDirectory, AccessMode::ReadOnly)
    ///     .unwrap();
    /// ```
    pub fn apply_standing(&self, path: &Path, target: Target, access: AccessMode) -> Result<()> {
        // **The one route that owes a parse, and it pays it once per call**: this
        // SID is a constant string rather than a value the caller holds, and
        // decision 22's program directory is the one remaining call site (the
        // sibling batch resolves it once for the whole set — [`Self::apply_siblings`]).
        // The `OwnedSid` keeps the allocation alive until the grant using it has
        // returned, which is the only thing it is for.
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path)?;
        self.standing_grant(
            path,
            target,
            access,
            GrantSid {
                raw: sid.raw(),
                text: ALL_APPLICATION_PACKAGES,
            },
        )
    }

    /// Authorize [`files`](crate::backend::SiblingScan::files) for [`ALL_APPLICATION_PACKAGES`],
    /// **one object at a time** — decision 22's sibling-DLL half (Ruling 78), and a
    /// standing grant like the directory those files sit in.
    ///
    /// **The wildcard is gone because a wildcard is not an object.** The batch was
    /// one `icacls <dir>\*.dll /grant:r …` invocation, and what it matched was the
    /// tool's own reading of the *names* in the directory — a reading the
    /// enumeration had already answered differently, because `is_file` follows a
    /// link: a file symlink named `x.dll` had the machine-wide ACE written on its
    /// **target**, outside the program's directory, and a junction named `x.dll`
    /// left the enumeration's set entirely and still took the ACE on the link's own
    /// object. Both measured on this host 2026-09-27. So the grant is per file and
    /// by handle: each path the scan enumerated is opened, is refused if it is a
    /// reparse point or is not a file, and is written through that handle — the two
    /// sets are the same objects by construction, and a link named `*.dll` fails the
    /// launch rather than being skipped or followed.
    ///
    /// **The file list is the caller's scan's, not a `&Path`**, which is the whole reason
    /// [`SiblingScan`](crate::backend::SiblingScan) exists: this call and the caller's own
    /// exposure set must be about one file set, and a `&Path` here would re-enumerate the
    /// directory — a second answer to a question already answered, and a second chance for
    /// the grant to name a DLL the exposure set does not.
    ///
    /// The `bool` is whether a sibling existed at all: `false` means no object was
    /// opened.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](crate::_core::Error::Launch) from `grant_to` for a DLL
    /// that cannot be opened or written, and
    /// [`Error::BadPath`](crate::_core::Error::BadPath) for one that is a reparse
    /// point or is not a file.
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
    /// let grants = acl::Grants::new(Arc::new(sid));
    /// let scans = SiblingScan::scan(dir);
    /// // `false` when the directory holds no DLL: nothing granted.
    /// let granted = grants
    ///     .apply_siblings(scans.files(), AccessMode::ReadOnly)
    ///     .unwrap();
    /// # let _ = granted;
    /// ```
    pub fn apply_siblings(&self, files: &[PathBuf], access: AccessMode) -> Result<bool> {
        let Some(first) = files.first() else {
            return Ok(false);
        };
        // One parse for the whole set: the SID is the same constant for every file,
        // and the allocation lives until the last grant of this batch.
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, first)?;
        for file in files {
            self.standing_grant(
                file,
                Target::File,
                access,
                GrantSid {
                    raw: sid.raw(),
                    text: ALL_APPLICATION_PACKAGES,
                },
            )?;
        }
        Ok(true)
    }

    /// One standing grant, with the SID already resolved and the event emitted.
    ///
    /// **`debug!`, because a grant is a per-ACE event and the phase is the
    /// `info!`** (P2's logging split): the phase's own line is
    /// `grant_whitelist`'s, once per launch, and this one would otherwise print the
    /// program directory and then every sibling DLL into the operator's log on
    /// every start. `written` still travels with it, so a reader at `debug` sees the
    /// skip as plainly as the write.
    fn standing_grant(
        &self,
        path: &Path,
        target: Target,
        access: AccessMode,
        sid: GrantSid<'_>,
    ) -> Result<()> {
        let written = grant_to(path, target, access, sid)?;
        tracing::debug!(
            step = "grant the ACLs",
            path = %path.display(),
            sid = sid.text,
            standing = true,
            written
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // `Grants::new` takes an `Arc<PackageSid>` — this module's interface, and the
    // shape the launcher itself holds: one SID shared with the launch rather than a
    // copy per user. `PackageSid` owns a raw `PSID`, so it is deliberately neither
    // `Send` nor `Sync`, and clippy's lint about that is about an `Arc` meant to
    // cross a thread boundary. The grants below are shared ownership inside one
    // test, which is what the code under test is given.
    #![allow(clippy::arc_with_non_send_sync)]

    use std::{fs, path::PathBuf, process::Command, sync::Arc};

    use super::*;
    use crate::{
        acl::{
            Grants,
            icacls::{icacls_binary, mentions_sid},
            test_support::writes,
        },
        backend::SiblingScan,
        sid::{PackageSid, package_name},
        testing::{icacls, sid_for},
    };

    /// **The sibling set is granted one object at a time, and the skip is what
    /// makes a second launch free.**
    ///
    /// The count is descriptor writes rather than `icacls` invocations, because
    /// the tool is no longer in this path at all: the first pass writes one ACE per
    /// DLL — two, for the two files — and the second writes nothing, which is the
    /// standing grant's whole point. Removing one DLL's ACE puts exactly that one
    /// write back, which is what tells a skip from a write that happened to leave
    /// the same ACE.
    #[test]
    #[expect(
        clippy::arc_with_non_send_sync,
        reason = "test context: `PackageSid` owns a raw `PSID` and is deliberately neither `Send` nor `Sync`, and the `Arc` is the shared ownership inside one thread that the code under test is given"
    )]
    fn a_second_sibling_pass_writes_only_the_dll_that_lost_the_ace() {
        let dir = tempfile::tempdir().expect("temp dir");
        for name in ["one.dll", "two.dll"] {
            fs::write(dir.path().join(name), b"not a library").expect("dll");
        }
        let scan = SiblingScan::scan(dir.path());
        let sid = PackageSid::derive(&package_name(dir.path())).expect("package SID");
        let grants = Grants::new(Arc::new(sid));

        let _ = writes::take();
        assert!(
            grants
                .apply_siblings(scan.files(), AccessMode::ReadOnly)
                .expect("first pass")
        );
        assert_eq!(
            writes::take(),
            2,
            "the first pass is one ACE per DLL, each on its own handle"
        );

        assert!(
            grants
                .apply_siblings(scan.files(), AccessMode::ReadOnly)
                .expect("second pass")
        );
        assert_eq!(
            writes::take(),
            0,
            "every DLL already carries the ACE, so nothing is written again"
        );

        let removed = Command::new(icacls_binary())
            .arg(dir.path().join("two.dll"))
            .args(["/remove:g", "*S-1-15-2-1", "/q"])
            .output()
            .expect("remove one ACE");
        assert!(
            removed.status.success(),
            "{}",
            String::from_utf8_lossy(&removed.stderr)
        );
        let _ = writes::take();
        assert!(
            grants
                .apply_siblings(scan.files(), AccessMode::ReadOnly)
                .expect("the pass runs again")
        );
        assert_eq!(
            writes::take(),
            1,
            "one DLL without the ACE is one write: the other is skipped"
        );
    }

    /// The answer design §6 settled on 2026-09-19, pinned as a regression test.
    ///
    /// The question it settles was whether an inheritable ACE on a directory reaches
    /// children that were already there when it was set: every probe behind the old
    /// evidence table used `icacls … /t`, which applies the ACE to each child
    /// explicitly, so inheritance alone was never isolated. Measured on NTFS (`C:`)
    /// and ReFS (`E:`) alike, a directory-only `/grant:r` — **no `/t`** — does reach
    /// them, as an inherited `(I)` entry. The grant list is therefore one ACE per
    /// exposed directory, there is no `/t` walk, and this test is what fails if that
    /// stops being true on the filesystem this crate is serving from.
    ///
    /// **What the tool counts and what the filesystem does are two different
    /// counts.** `icacls` was handed one path and says so; the propagation is the
    /// filesystem's, and it is also the cost — one descriptor write walks every
    /// descendant the served tree holds, which is why the grants are standing and why
    /// the launcher reads before it writes (`acl::grant_to`'s skip).
    #[test]
    fn the_directory_ace_alone_reaches_a_file_that_was_already_there() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("root");
        let depth_one = root.join("direct.txt");
        let nested = root.join("bucket").join("nested.txt");
        fs::create_dir_all(nested.parent().expect("the bucket")).expect("create the tree");
        fs::write(&depth_one, b"before the grant").expect("seed");
        fs::write(&nested, b"before the grant").expect("seed");
        // One value, alive for the whole test: it owns the profile the grants are for.
        let sid = Arc::new(sid_for(&root));
        let sid_text = sid.as_str().to_string();

        assert!(
            !has_ace(&depth_one, &sid).expect("read the DACL"),
            "no package ACE yet"
        );

        {
            let grants = Grants::new(Arc::clone(&sid));
            grants
                .apply(&root, Target::Directory, AccessMode::ReadWrite)
                .expect("grant");
        }

        for file in [&depth_one, &nested] {
            let after = icacls(file, &[]);
            assert!(
                after.contains(&sid_text),
                "an inheritable ACE on the directory did not reach {}, which was there before the\n\
                 grant — so the grant list would need the `/t` walk design §6 rules out. File DACL:\n{after}",
                file.display()
            );
            assert!(
                after.to_lowercase().contains("inherited") || after.contains("(I)"),
                "the entry on the pre-existing file should be the *inherited* one, not a second \
                 explicit ACE:\n{after}"
            );
        }

        // A file created afterwards is the case the design's rows already covered; it
        // must keep working either way.
        let created_later = root.join("bucket").join("after.txt");
        fs::write(&created_later, b"after the grant").expect("write");
        assert!(has_ace(&created_later, &sid).expect("read the DACL"));

        // **The guard is gone and the ACE is not**, which is the standing contract:
        // `Grants` owns the profile's lifetime and nothing else. Read the DACL text
        // itself rather than only asking our own predicate: an assertion that rests on
        // `has_ace` alone would also pass for a path whose DACL could not be read, and
        // the point is the operator's directory, not this crate's view of it.
        for path in [&root, &depth_one, &nested, &created_later] {
            assert!(
                icacls(path, &[]).contains(&sid_text),
                "the package ACE is still on the whole tree after the guard dropped: {}",
                path.display()
            );
        }
        // And the predicate agrees with the oracle, which is what lets the suites that
        // have no oracle of their own rely on it.
        assert!(has_ace(&root, &sid).expect("read the DACL"));

        // `revoke` is still the primitive that takes one back, and the only one: an
        // operator who wants the ACE gone runs it, not a rollback this crate no longer
        // performs.
        revoke(&root, &sid).expect("take the grant back by hand");
        assert!(!has_ace(&root, &sid).expect("read the DACL"));
    }

    #[test]
    fn a_grant_leaves_a_foreign_ace_alone_and_a_revoke_removes_exactly_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("root");
        fs::create_dir_all(&root).expect("create the root");
        // **The outsider is asked about by SID, not by a count of `icacls` lines.** A
        // listing's line count is the host's business: `icacls` copies an inherited entry
        // into an explicit one on some hosts and not on others, which is what made the
        // count-based comparison fail on CI for a grant that touched nothing. `acl_names`
        // is the crate's own read (`EqualSid` over the DACL), so "the outsider's entry is
        // still there" is the property and the tool's rendering is not.
        let users = OwnedSid::parse("S-1-5-32-545", &root).expect("a SID");

        // A foreign ACE the run must not touch, in the shape the verified row
        // describes: `Users` granted on the directory. Named by SID so this does not
        // depend on how the host spells that account's name.
        let granted = Command::new(icacls_binary())
            .arg(&root)
            .args(["/grant", "*S-1-5-32-545:(OI)(CI)R", "/q"])
            .output()
            .expect("grant Users");
        assert!(
            granted.status.success(),
            "{}",
            String::from_utf8_lossy(&granted.stderr)
        );
        assert!(
            acl_names(&root, users.raw()).expect("read the DACL"),
            "the foreign ACE was written, so the assertions below are about a real one"
        );

        let sid = Arc::new(sid_for(&root));
        let grants = Grants::new(Arc::clone(&sid));
        grants
            .apply(&root, Target::Directory, AccessMode::ReadWrite)
            .expect("grant");
        assert!(
            has_ace(&root, &sid).expect("read the DACL"),
            "the grant landed"
        );
        assert!(
            acl_names(&root, users.raw()).expect("read the DACL"),
            "the grant left the foreign ACE alone"
        );

        revoke(&root, &sid).expect("take the grant back by hand");

        assert!(
            !has_ace(&root, &sid).expect("read the DACL"),
            "the package ACE is gone"
        );
        assert!(
            acl_names(&root, users.raw()).expect("read the DACL"),
            "the revoke removed exactly the package ACE and left the outsider's entry"
        );
    }

    #[test]
    fn revoking_an_absent_ace_succeeds() {
        // Idempotence is what makes a crashed run's residue removable twice, and what
        // lets a test clean up a directory whose grant it cannot be sure landed.
        // Measured on this host 2026-09-21: `/remove:g` for a SID with no entry
        // anywhere under the path exits 0 — "Successfully processed 1 files; Failed
        // processing 0 files" — so this is the tool's behaviour rather than a kindness
        // this crate adds.
        let dir = tempfile::tempdir().expect("temp dir");
        let sid = sid_for(dir.path());
        revoke(dir.path(), &sid).expect("first revoke");
        revoke(dir.path(), &sid).expect("second revoke");
    }

    #[test]
    fn dropping_the_grants_leaves_them_standing() {
        // **The inversion of what this test asserted before the grants became
        // standing** (design decision 22's cost follow-up): `Grants` owns the profile's
        // lifetime, so dropping it deletes the *profile* and nothing else — a guard
        // that quietly took an ACE back would be the per-launch descriptor write the
        // whole change exists to remove, and it would also mean a crash and a clean
        // exit left different trees.
        let dir = tempfile::tempdir().expect("temp dir");
        let sid = Arc::new(sid_for(dir.path()));
        {
            let grants = Grants::new(Arc::clone(&sid));
            grants
                .apply(dir.path(), Target::Directory, AccessMode::ReadOnly)
                .expect("grant");
            assert!(has_ace(dir.path(), &sid).expect("read the DACL"));
        }
        // The oracle, not the predicate: this assertion is about the operator's
        // directory, which is the thing the contract is about.
        assert!(
            icacls(dir.path(), &[]).contains(sid.as_str()),
            "the grant is standing: dropping the guard removed nothing"
        );
    }

    #[test]
    fn the_predicate_refuses_a_path_it_cannot_read() {
        // `has_ace` is the oracle the rest of this phase's suites consult, and an
        // oracle that answered "no entry for this SID" for a path that could not be
        // read would make every "the ACE is gone" assertion pass for the wrong reason.
        // Measured on this host 2026-09-21: a path that does not exist fails the read
        // with `ERROR_FILE_NOT_FOUND`. The read asks the DACL through
        // `GetNamedSecurityInfoW` and no longer through `icacls`, so the step the
        // failure names is the read's own.
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("never-created");
        let sid = sid_for(dir.path());

        let err = has_ace(&missing, &sid).expect_err("a path the read cannot see is an error");
        let text = err.to_string();
        assert!(text.contains("read the DACL"), "{text}");
        assert!(
            text.contains("never-created"),
            "the failure names the path it was about: {text}"
        );
    }

    /// **The file-shaped spec is what actually lands on a file, and the inherited
    /// form is the silent no-op the design's evidence row records** — decision 22's
    /// rule is the first time this crate puts *files* into an exposure set, so the
    /// two halves of this test are one measurement: what the grant has to be, and
    /// what it must not be.
    ///
    /// The inherited form is run first and asserted to write **nothing**, because
    /// that is the failure the counter-shape exists to prevent and it is silent:
    /// `icacls` exits 0 and reports success, so a rule that shipped the directory
    /// spec for the sibling DLLs would grant nothing and show up as a confined child
    /// that dies `0xC0000135` with no diagnostic (design decision 22).
    #[test]
    fn the_file_shaped_spec_is_what_lands_on_a_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let sibling = dir.path().join("sibling.dll");
        fs::write(&sibling, b"not really a library").expect("write the sibling");
        let sid = Arc::new(sid_for(dir.path()));

        assert!(
            !has_ace(&sibling, &sid).expect("read the DACL"),
            "no package ACE yet"
        );

        // The directory shape, on a file. The invocation itself succeeds — the tool
        // exits 0 and reports one file processed — and the ACE is nowhere: this is
        // the no-op, not an error. A well-known SID is used rather than a derived
        // package SID because this half is a fact about the tool and the filesystem
        // rather than about this crate's SID, and it needs no AppContainer profile.
        let written = dir.path().join("written.txt");
        fs::write(&written, b"a plain file").expect("write the control");
        let users = "S-1-5-32-545";
        let inherited = Command::new(icacls_binary())
            .arg(&written)
            .args(["/grant:r", &format!("*{users}:(OI)(CI)M"), "/q"])
            .output()
            .expect("run icacls");
        assert!(
            inherited.status.success(),
            "the inherited shape is accepted on a file: {}",
            String::from_utf8_lossy(&inherited.stderr)
        );
        assert!(
            !icacls(&written, &[]).contains(users),
            "an inheritance-flagged grant on a file wrote an ACE after all, so the \
             file-shaped spec below is not what makes a sibling grant land and this \
             crate's reason for it needs re-measuring (design §6's evidence row): {}",
            icacls(&written, &[])
        );

        // The file shape: no `(OI)`/`(CI)`, so the entry is explicit and stays. The
        // path probe is the crate's own, so this half is the shape this crate
        // actually writes for the sibling DLLs.
        grant(&sibling, Target::File, AccessMode::ReadOnly, &sid)
            .expect("the file shape is accepted on a file");
        assert!(
            has_ace(&sibling, &sid).expect("read the DACL"),
            "the file-shaped grant is what puts an ACE on a file: {}",
            icacls(&sibling, &[])
        );

        revoke(&sibling, &sid).expect("take the grant back");
        assert!(!has_ace(&sibling, &sid).expect("read the DACL"));
    }

    /// **The sibling set goes on through one invocation, for the SID every container
    /// already holds** (Ruling 78, decision 22, and the standing follow-up). The
    /// directory's pattern is the grant the launcher actually makes, and every sibling
    /// it covers is asserted through the oracle rather than through one path: a batch
    /// that granted only the first match would leave a confined child that dies
    /// `0xC0000135` with no diagnostic, which is the failure the counter-shape exists
    /// to prevent.
    ///
    /// The SID is [`ALL_APPLICATION_PACKAGES`] rather than this root's, which is the
    /// half that makes a new root cost nothing: the program directory does not change
    /// between roots, so its grant cannot either. The ACE is never taken back, so the
    /// assertion is that it is still there afterwards.
    #[test]
    fn the_sibling_batch_grants_every_dll_for_all_application_packages() {
        let dir = tempfile::tempdir().expect("temp dir");
        let siblings: Vec<PathBuf> = ["one.dll", "two.dll", "three.dll"]
            .iter()
            .map(|name| {
                let path = dir.path().join(name);
                fs::write(&path, b"not really a library").expect("write the sibling");
                path
            })
            .collect();
        let sid = Arc::new(sid_for(dir.path()));

        // The precondition that makes the assertion below mean the batch ran: this
        // temp tree does not already carry the standing SID, so its presence after the
        // call is the call's doing. A host whose temp parent chain already inherits
        // ALL APPLICATION PACKAGES fails here — which is the honest reading, because
        // the grant would then have been skipped as already satisfied.
        for sibling in &siblings {
            assert!(
                !mentions_sid(&icacls(sibling, &[]), ALL_APPLICATION_PACKAGES),
                "the fixture already carries the standing SID on {}: {}",
                sibling.display(),
                icacls(sibling, &[])
            );
        }

        let grants = Grants::new(Arc::clone(&sid));
        assert!(
            grants
                .apply_siblings(SiblingScan::scan(dir.path()).files(), AccessMode::ReadOnly)
                .expect("one batched grant"),
            "the directory holds siblings, so the grant ran"
        );
        for sibling in &siblings {
            assert!(
                mentions_sid(&icacls(sibling, &[]), ALL_APPLICATION_PACKAGES),
                "the batch granted {} for {ALL_APPLICATION_PACKAGES}: {}",
                sibling.display(),
                icacls(sibling, &[])
            );
        }

        // Standing: dropping the guard takes nothing back.
        drop(grants);
        for sibling in &siblings {
            assert!(
                mentions_sid(&icacls(sibling, &[]), ALL_APPLICATION_PACKAGES),
                "the batch grant is standing on {}: {}",
                sibling.display(),
                icacls(sibling, &[])
            );
        }
    }

    /// A directory with no sibling is a grant of nothing: no `icacls` invocation and
    /// no pattern handed to the tool. **An unmatched pattern is an error rather than a
    /// no-op** (measured 2026-09-21), so this is the negative that keeps a grant on a
    /// DLL-less directory from reporting success.
    #[test]
    fn a_directory_with_no_sibling_grants_nothing() {
        let dir = tempfile::tempdir().expect("temp dir");
        fs::write(dir.path().join("notes.txt"), b"not a library").expect("write a non-DLL");
        let sid = Arc::new(sid_for(dir.path()));

        let grants = Grants::new(Arc::clone(&sid));
        assert!(
            !grants
                .apply_siblings(SiblingScan::scan(dir.path()).files(), AccessMode::ReadOnly)
                .expect("nothing to grant"),
            "no sibling means no grant"
        );
    }

    // The DACL whitelist against a real filesystem, with `icacls` as the
    // independent oracle for the grant, the standing skip and the inheritance
    // answer design §6 settled on 2026-09-19.
    //
    // **Every test in this block writes real ACEs**, and only ever onto a
    // temporary directory it created itself: the operator's tree is not this
    // suite's to edit, and a failure must not be able to leave a residue outside
    // the process's own temp tree.
    //
    // **Nothing the launcher does takes an ACE back any more.** Every grant is
    // standing (`Grants`'s doc states why and what it gives up), so the tests that
    // used to assert a rollback now assert the opposite — the ACE is still there
    // after the guard drops — and `revoke` survives as the primitive an operator
    // (or a test) removes one with.
    //
    // **The SID these tests grant to is [`crate::testing::sid_for`]'s** — imported
    // above, and the same derivation the launcher asks — and each test derives it
    // **once and holds the value**: `PackageSid` owns the profile behind the SID and
    // deletes it when the value drops, so two live values for one name would have the
    // second delete take the profile the first is still using (the measurements are
    // at the definer's doc).
}
