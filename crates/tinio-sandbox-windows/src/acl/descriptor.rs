//! **A grant is written on a handle this module opened itself, so the object the
//! check approved is the object that is granted.** `icacls /grant:r` wrote until
//! 2026-09-27, and the defect that ended it is the check-then-use between the
//! whitelist's reparse refusal and the tool's own re-resolution of the name it was
//! handed: a writer who replaced an exposed directory with a junction inside
//! the grant window got the ACE written somewhere the check never saw. `Object`
//! opens the spelled path with `FILE_FLAG_OPEN_REPARSE_POINT`, so the *object
//! named* is opened rather than followed; the opened object is asked what it is (a
//! reparse point is refused, and so is an object of the wrong kind), and both the
//! DACL read that decides the skip and the `SetSecurityInfo` write that follows
//! happen **on that handle**. The ACE is what the tool's alias produced, and that
//! is measured rather than argued: `(OI)(CI)RX` on a directory is
//! `0x0012_00A9` with object-and-container inherit, and the descriptor
//! `SetEntriesInAclW` + `SetSecurityInfo` leaves is the one `icacls /grant:r`
//! left — entry for entry, control word included (`0x8404`, auto-inherited and
//! **not** protected), with the same propagation to a directory's pre-existing
//! children, which this module's own tests assert against the tool as an
//! independent oracle. [`Error::Launch`] carries the API's own status for a write
//! or an open that failed, and a failed read carries it too.
//!
//! **The read half is the Win32 API because the tool's *text* cannot answer the
//! question the read exists for.** The read decides whether the write may be
//! skipped, and `icacls` renders a well-known SID by **name**: an ACE written for
//! [`ALL_APPLICATION_PACKAGES`](crate::sid::ALL_APPLICATION_PACKAGES) comes back as
//! `APPLICATION PACKAGE AUTHORITY\ALL APPLICATION PACKAGES:(RX)`, so a check for
//! the literal `S-1-15-2-1` never saw the ACE it had just written and the skip
//! never fired — which is not a cosmetic failure, because the write it failed to
//! skip rewrites the descriptor and propagates eagerly over the whole subtree
//! (10–30 s for this workspace's `target\debug`; measured 2026-09-25). Two
//! spellings patched that symptom; [`has_ace`](super::grant::has_ace) ends the class, because the
//! entries are compared with `EqualSid` — as the kernel compares them, in the
//! walk [`walk_dacl`] runs — and no rendering takes part. [`mentions_sid`](super::icacls::mentions_sid)
//! survives as the *test* oracle over the tool's own text, where a rendering is
//! the thing being asserted rather than the thing being trusted.
//!
//! **The skip is a read of the same handle the write would use**, not of the name
//! again: a name-based read through a link answers about whatever the link
//! resolves to, and a "already granted" it reported for another object would skip
//! the grant the launch needs while the child reaches the link's target. The
//! oracle is unchanged — the predicate `access_matches` and the SID comparison
//! [`walk_dacl`] makes — only the object it is asked of.
//!
//! The `unsafe` here is the DACL's own: the handle open, the descriptor read (by
//! handle and by name), the merge and the descriptor's release. **The ACE walk is
//! not here** — the ACL's own count, `GetAce`, the ACE-type test and `EqualSid`
//! are [`walk::walk_dacl`](super::walk::walk_dacl)'s, which is this workspace's one traversal and which
//! `walk` re-exports here; what this crate hands it is a predicate and the SID to
//! compare against. Every allocation is `LocalFree`d
//! once, by the function that made it; the ACL inside a descriptor belongs to the
//! descriptor and may **not** be freed on its own. Decision 14 enumerates the
//! sites.
//!
//! [`Error::Launch`]: crate::_core::Error::Launch

// The manifest denies `unsafe_code` and the exception sits on the modules that
// need it, as decision 14 asks: every Win32 call below is the DACL layer's own —
// the descriptor read, the handle the grant acts on, the merge and the
// descriptor's release — and a crate-level allow would lift the lint for every
// module instead. **The ACE walk is not among the sites this permit covers**:
// it is [`walk_dacl`]'s, whose own module carries its exemption and states
// its reason.
#![allow(unsafe_code)]

use std::{
    io,
    ops::ControlFlow,
    path::{Path, PathBuf},
    ptr::{from_mut, null, null_mut},
};

use tinio_util::wide::to_wide;
#[cfg(any(test, feature = "testing"))]
use windows_sys::Win32::Security::Authorization::GetNamedSecurityInfoW;
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, LocalFree},
    Security::{
        ACCESS_ALLOWED_ACE, ACL,
        Authorization::{
            ConvertStringSidToSidW, EXPLICIT_ACCESS_W, GetSecurityInfo, SE_FILE_OBJECT, SET_ACCESS,
            SetEntriesInAclW, SetSecurityInfo, TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
        },
        DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
    },
    Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle,
        OPEN_EXISTING, READ_CONTROL, WRITE_DAC,
    },
};

#[cfg(test)]
use super::test_support::{opens, writes};
use super::{
    refusal::{kind_refusal, reparse_refusal},
    spec::{Target, access_matches, ace_bits},
    walk::{Ace, walk_dacl},
};
use crate::{
    _core::{AccessMode, Result, launch},
    handles,
};

/// **The object a grant acts on: the one the caller spelled, opened rather than
/// followed, and asked what it is before anything is written to it.**
///
/// This type exists because a path is not an object. `icacls /grant:r` — and any
/// name-based API, `SetNamedSecurityInfoW` included — resolves the name again when
/// it writes, so between the whitelist's reparse refusal and the write a local
/// writer who swapped an exposed directory for a junction redirected the ACE: the
/// check approved one object and the write reached another, with no re-check
/// between them — and the window is the whole grant phase, which
/// `backend/grant.rs` measures against a 30 s readiness budget.
/// `CreateFileW` with `FILE_FLAG_OPEN_REPARSE_POINT` opens the *object the name
/// denotes* — the link itself, if the last component is one — and every later call
/// takes that handle, so the object verified is the object written: there is no
/// second resolution to lose a race to.
///
/// **Each handle is opened for the access its half needs and no more**:
/// [`Self::open`] takes `READ_CONTROL` alone, which is what `GetSecurityInfo`
/// needs to answer the skip, and [`Self::open_for_write`] takes
/// `READ_CONTROL | WRITE_DAC`, which is what `SetSecurityInfo` needs to replace
/// the entry. That split is not tidiness: a grant whose ACE is already there — the
/// steady state of a standing grant, and every launch after the first on a root —
/// must not fail on a path this process may read but may not re-ACL, and a single
/// `READ_CONTROL | WRITE_DAC` open would refuse it before the read could answer
/// "nothing to do". The write pays the stronger access only when a descriptor
/// write is actually due.
///
/// **`FILE_FLAG_BACKUP_SEMANTICS` is passed for every target, directory or file**,
/// because the kind question has to be the *handle's*: Windows refuses any handle
/// to a directory without that flag, so a file-shaped grant on a directory would
/// fail the open with `ERROR_ACCESS_DENIED` and never reach [`Self::verify`] —
/// leaving the one check this type exists for unreachable in exactly the direction
/// `icacls` used to report as success. The flag is what a directory needs and costs
/// a file nothing (measured on this host 2026-09-27: a plain file opens, reads and
/// re-ACLs through it).
///
/// **Two refusals, and both are the handle's own answer.** The attributes come from
/// `GetFileInformationByHandle`, which reports on the object behind the handle
/// rather than on the name: `FILE_ATTRIBUTE_REPARSE_POINT` is a link (refused with
/// [`REPARSE_POINT`]'s sentence, the same rule the whitelist applies to a spelled
/// path) and `FILE_ATTRIBUTE_DIRECTORY` decides whether the object is the kind
/// `target` addresses (refused with [`NOT_THE_GRANTED_KIND`]'s). Both are
/// `Error::BadPath` — a path that cannot be expressed inside the confinement, which
/// is what "this is not the object the grant was about" is.
///
/// The `unsafe` blocks are this module's: the open, the attribute read, the
/// descriptor read, the merge and the write, each with its own safety argument, and
/// the handle is closed exactly once by `Drop`.
pub(super) struct Object {
    /// The spelling the caller gave, which every error and event names: the handle
    /// has no path of its own to report, and a `\\?\`-prefixed canonical spelling
    /// is not what an operator typed.
    path: PathBuf,
    /// The handle to the object itself, live until `Drop`.
    handle: HANDLE,
}

impl Object {
    /// Open the object `path` denotes **for the read that decides the skip**,
    /// without following a final reparse point.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](crate::_core::Error::Launch) naming the path when
    /// `CreateFileW` refuses — a path that does not exist, or an object this
    /// process may not read.
    pub(super) fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, READ_CONTROL)
    }

    /// Open the same object again **for the write**, with `WRITE_DAC` beside the
    /// read access the first open had.
    ///
    /// **It is a second open of the same *name*, not a second class of object**:
    /// the flags are [`Self::open`]'s, so a link that appeared between the two
    /// calls is not followed — and the caller verifies this handle too, which is
    /// what makes the write act on an object that passed the same two refusals the
    /// read did. A failure here is the grant's failure: nothing has been written
    /// (the write is the first thing this handle is used for), and nothing is taken
    /// back on the way out, because every grant this crate makes is standing.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](crate::_core::Error::Launch) naming the path when
    /// `CreateFileW` refuses — including a path this process may read but may not
    /// re-ACL, which is exactly the case the two-open split exists to keep off the
    /// skip's path.
    pub(super) fn open_for_write(path: &Path) -> Result<Self> {
        Self::open_with(path, READ_CONTROL | WRITE_DAC)
    }

    /// One open, with the access the caller's half needs.
    fn open_with(path: &Path, access: u32) -> Result<Self> {
        #[cfg(test)]
        opens::note(access);
        let name = to_wide(path.as_os_str());
        // SAFETY: `name` is NUL-terminated and alive for the call, the share mode
        // and creation disposition are the constants above, no security attributes
        // are passed (`null`, so the object's own descriptor is untouched by the
        // open), no template file is named, and `access` is a file access mask the
        // caller derived from the half it is opening for. A successful call returns
        // a handle this value owns.
        let handle = unsafe {
            CreateFileW(
                name.as_ptr(),
                access,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                null(),
                OPEN_EXISTING,
                FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
                null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE || handle.is_null() {
            return Err(launch("open the granted path", path, handles::last_error()));
        }
        Ok(Self {
            path: path.to_path_buf(),
            handle,
        })
    }

    /// What the opened object is, against what the grant's shape addresses.
    ///
    /// # Errors
    ///
    /// [`Error::BadPath`] with [`REPARSE_POINT`] for a link and with
    /// [`NOT_THE_GRANTED_KIND`] for an object whose directory-ness is not the kind
    /// `target` speaks about; [`Error::Launch`] when the attributes cannot be read.
    pub(super) fn verify(&self, target: Target) -> Result<()> {
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: `self.handle` is a live handle this value owns, and `info` is a
        // correctly sized, writable out-parameter that stays alive for the call.
        let read = unsafe { GetFileInformationByHandle(self.handle, &raw mut info) };
        if read == 0 {
            return Err(launch(
                "verify the granted path",
                &self.path,
                handles::last_error(),
            ));
        }
        if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(reparse_refusal(&self.path, "verify the granted path"));
        }
        let is_directory = info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0;
        if is_directory != (target != Target::File) {
            return Err(kind_refusal(&self.path));
        }
        Ok(())
    }

    /// Whether this object's DACL already carries the grant [`Self::write`] would
    /// write.
    ///
    /// **The same oracle the name-based read used, asked of the handle**: the walk
    /// is [`walk_dacl`]'s, which compares each entry's SID with `EqualSid`, and the
    /// predicate is [`access_matches`], which asks the entry about its mask and its
    /// inheritance flags — so "already granted" means exactly "the write would
    /// change nothing", which is what makes the skip sound. Asking it of the name
    /// instead would answer about whatever the name resolves to *now*, which is the
    /// object this handle is not.
    pub(super) fn granted(&self, sid: PSID, target: Target, access: AccessMode) -> Result<bool> {
        let wanted = |ace: &ACCESS_ALLOWED_ACE| access_matches(ace, access, target);
        let Some((dacl, descriptor)) = self.read_dacl()? else {
            return Ok(false);
        };
        let held = carries(&self.path, dacl, sid, &wanted);
        // The descriptor is this read's own allocation, released once here — after the
        // walk that borrows the ACL inside it has returned, and on the failure path too.
        free_descriptor(descriptor);
        held
    }

    /// Write `sid`'s entry — `(mask, inheritance)` for the shape, in place of
    /// whatever this SID carries now — into the object's DACL.
    ///
    /// **The read-merge-write shape is `icacls /grant:r`'s own**: the old DACL is
    /// read off the handle, `SetEntriesInAclW` merges one `SET_ACCESS` entry into
    /// it (leaving every other trustee's entries where they were), and the merged
    /// ACL and nothing else goes back through `SetSecurityInfo` with
    /// `DACL_SECURITY_INFORMATION` — which is what leaves the owner, the group, the
    /// SACL and the control bits alone, and what lets the filesystem propagate an
    /// inheritable ACE to a directory's pre-existing children exactly as it did
    /// when the tool wrote the ACE.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](crate::_core::Error::Launch) naming the path, with the
    /// Win32 status of whichever of the three calls refused. **The status is the
    /// call's own return value and not `GetLastError`'s**: this family reports its
    /// failure as a `WIN32_ERROR` and does not set the thread's last error, so
    /// [`handles::last_error`] would answer with a stale code here.
    pub(super) fn write(&self, sid: PSID, target: Target, access: AccessMode) -> Result<()> {
        let Some((old, descriptor)) = self.read_dacl()? else {
            // A null DACL is every access to every trustee; there is no ACL to merge
            // into, and `SetEntriesInAclW`'s `oldacl` argument may be null for
            // exactly that case ("no existing ACL"), so the merge starts from empty.
            return self.merge_and_set(sid, target, access, null());
        };
        let result = self.merge_and_set(sid, target, access, old);
        free_descriptor(descriptor);
        result
    }

    /// `SetEntriesInAclW` + `SetSecurityInfo` over `old`, which is either the
    /// object's own ACL or `null`.
    fn merge_and_set(
        &self,
        sid: PSID,
        target: Target,
        access: AccessMode,
        old: *const ACL,
    ) -> Result<()> {
        let (mask, inheritance) = ace_bits(target, access);
        let entry = EXPLICIT_ACCESS_W {
            grfAccessPermissions: mask,
            // `SET_ACCESS` is `/grant:r`: this SID's entry is replaced, every other
            // trustee's is left alone. `GRANT_ACCESS` would merge the masks instead,
            // which is the behaviour decision 15 deliberately does not want.
            grfAccessMode: SET_ACCESS,
            grfInheritance: inheritance,
            Trustee: TRUSTEE_W {
                // A SID and not a name: an AppContainer package SID has no account
                // behind it, and a name would be looked up — the same rule
                // `grant_spec`'s `*` prefix enforces for the tool's argument. The
                // pointer is borrowed for this call alone: `SetEntriesInAclW` copies
                // the SID into the ACL it builds.
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: sid.cast::<u16>(),
                // `pMultipleTrustee` and `MultipleTrusteeOperation`, both defaulted:
                // no trustee chain.
                ..Default::default()
            },
        };
        let mut merged: *mut ACL = null_mut();
        // SAFETY: `entry` is one initialized EXPLICIT_ACCESS_W alive for the call
        // and its trustee points at the caller's own SID; `old` is either null or an
        // ACL the caller holds (the descriptor it borrows stays alive until the
        // caller's free); `merged` is a valid out-parameter. On success the new ACL
        // is this call's allocation to free.
        let code = unsafe { SetEntriesInAclW(1, &entry, old, &raw mut merged) };
        if code != 0 {
            return Err(launch(
                "merge the ACL grant",
                &self.path,
                io::Error::from_raw_os_error(code as i32),
            ));
        }
        // SAFETY: `self.handle` is live for this value's lifetime; the ACL passed is
        // the one just built and stays alive until the free below; the owner, group
        // and SACL arguments are null, which is "not asked for", so only the DACL of
        // the object changes.
        let code = unsafe {
            SetSecurityInfo(
                self.handle,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                merged,
                null_mut(),
            )
        };
        // SAFETY: `merged` is `SetEntriesInAclW`'s own allocation and this is its
        // only free; nothing reads it afterwards.
        unsafe { LocalFree(merged.cast()) };
        if code != 0 {
            return Err(launch(
                "write the DACL",
                &self.path,
                io::Error::from_raw_os_error(code as i32),
            ));
        }
        #[cfg(test)]
        writes::note();
        Ok(())
    }

    /// This object's DACL, with the descriptor that owns it — read off the handle.
    ///
    /// [`read_dacl`]'s handle-shaped twin, and the reason it exists rather than a
    /// name form: a name can be re-resolved, a handle cannot.
    fn read_dacl(&self) -> Result<Option<(*mut ACL, PSECURITY_DESCRIPTOR)>> {
        let mut dacl: *mut ACL = null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
        // SAFETY: `self.handle` is a live handle this value owns with `READ_CONTROL`
        // on it, `dacl` and `descriptor` are valid out-parameters, and a null
        // owner/group/SACL out-parameter is the documented "not asked for". The
        // descriptor it returns is the one `LocalFree` releases, exactly once, in
        // the caller.
        let code = unsafe {
            GetSecurityInfo(
                self.handle,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &raw mut dacl,
                null_mut(),
                &raw mut descriptor,
            )
        };
        if code != 0 {
            return Err(launch(
                "read the DACL",
                &self.path,
                io::Error::from_raw_os_error(code as i32),
            ));
        }
        if dacl.is_null() {
            // The descriptor came back without a DACL: nothing to walk, and its own
            // allocation is still this function's to release.
            free_descriptor(descriptor);
            return Ok(None);
        }
        Ok(Some((dacl, descriptor)))
    }
}

impl Drop for Object {
    fn drop(&mut self) {
        // SAFETY: the handle is this value's own — `open` is the only constructor
        // and nothing else holds a copy — so it is closed exactly once, here.
        unsafe { CloseHandle(self.handle) };
    }
}

/// Whether `dacl` — one live ACL, with the descriptor that owns it still in the
/// caller's hands — holds an access-allowed entry for `sid` that `wanted` accepts.
///
/// **This crate's one question about a DACL, and its two reads differ in nothing
/// else**: [`Object::granted`] asks it of the handle the write would act on, and
/// [`any_ace_in`] of a path the test oracle named. The walk itself is
/// [`walk_dacl`]'s — which owns the ACE sequence, from the ACL's own
/// count through the type test to the SID comparison, so this crate never asks the
/// kernel for an ACE — and what the walk hands over is [`Ace`]'s three cases;
/// **the verdict is here**: a `Named` entry that `wanted` accepts ends the walk,
/// and every other entry is passed over rather than answered. A DACL may carry
/// entries this crate never wrote — an inherited `(OI)(CI)(RX)` for
/// `BUILTIN\Users`, a deny another tool set — and the question is whether *this*
/// grant is already there, not whether the DACL is anything in particular.
///
/// The caller keeps the descriptor: it read the ACL through it and releases it by
/// the route that produced it, so nothing here frees.
///
/// **A walk that fails is a failed launch and not a skip**: [`walk_dacl`]'s error
/// travels out as this call's own, attributed to the step `"walk the DACL"`, so an
/// entry nothing could read never becomes a satisfied grant and no descriptor is
/// rewritten on the strength of one. That is [`walk_dacl`]'s decision and its
/// reason is written beside it there — including the note that no test covers the
/// failing `GetAce`, because reaching it needs a DACL the security APIs cannot hand
/// out.
pub(super) fn carries(
    path: &Path,
    dacl: *const ACL,
    sid: PSID,
    wanted: &dyn Fn(&ACCESS_ALLOWED_ACE) -> bool,
) -> Result<bool> {
    // SAFETY: `dacl` is the caller's own read — the ACL inside the descriptor its
    // `GetSecurityInfo` or `GetNamedSecurityInfoW` returned, alive until the
    // caller's `free_descriptor` — and `sid` is the caller's live SID (a
    // `PackageSid`'s or an `OwnedSid`'s), both live for this call.
    let verdict = unsafe {
        walk_dacl(dacl, sid, &mut |ace| match ace {
            Ace::Named(ace) if wanted(ace) => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        })
    }
    .map_err(|source| launch("walk the DACL", path, source))?;
    Ok(verdict.is_break())
}

/// Whether `path`'s DACL carries an entry for `sid` whatever its mask.
#[cfg(any(test, feature = "testing"))]
pub(super) fn acl_names(path: &Path, sid: PSID) -> Result<bool> {
    any_ace_in(path, &|_| true, sid)
}

/// Run `wanted` over `path`'s access-allowed ACEs and answer whether any names
/// `sid`.
///
/// The path-shaped read, for [`has_ace`](super::grant::has_ace) — the test oracle that asks about a path
/// rather than about an opened object. The grant's own read is
/// [`Object::granted`]'s, which asks the same question of the same DACL through a
/// handle the writer holds.
#[cfg(any(test, feature = "testing"))]
pub(super) fn any_ace_in(
    path: &Path,
    wanted: &dyn Fn(&ACCESS_ALLOWED_ACE) -> bool,
    sid: PSID,
) -> Result<bool> {
    let Some((dacl, descriptor)) = read_dacl(path)? else {
        return Ok(false);
    };
    let held = carries(path, dacl, sid, wanted);
    // The descriptor is this read's own allocation, released once here — after the
    // walk that borrows the ACL inside it has returned, and on the failure path too.
    free_descriptor(descriptor);
    held
}

/// Release a security descriptor this module read — **the one place its
/// allocation is given back**.
///
/// Every producer here (`GetNamedSecurityInfoW`, `GetSecurityInfo`) documents
/// `LocalFree` as the release and hands back one allocation, and the ACL pointer it
/// filled is a borrow *into* that allocation: freeing the ACL on its own is the
/// documented mistake, which is why no caller does it.
pub(super) fn free_descriptor(descriptor: PSECURITY_DESCRIPTOR) {
    if descriptor.is_null() {
        return;
    }
    // SAFETY: the descriptor is this module's own read, it is not kept, and
    // nothing else refers to it — the callers hand it over exactly once.
    unsafe { LocalFree(descriptor) };
}

/// `path`'s DACL, with the descriptor that owns it — or `None` when the kernel
/// reports no DACL at all.
///
/// **A null DACL means "no ACE can be named", not "the path could not be read"**:
/// `GetNamedSecurityInfoW` reports success and leaves the pointer null for an
/// object carrying no discretionary ACL, and answering `None` there is the same
/// "nothing granted" a listing of it would produce. A failure to *read* is an
/// error and travels that way, exactly as it did through the tool.
#[cfg(any(test, feature = "testing"))]
pub(super) fn read_dacl(path: &Path) -> Result<Option<(*mut ACL, PSECURITY_DESCRIPTOR)>> {
    let name = to_wide(path.as_os_str());
    let mut dacl: *mut ACL = null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
    // SAFETY: `name` is NUL-terminated and alive for the call, `dacl` and
    // `descriptor` are valid out-parameters, and a null owner/group/SACL
    // out-parameter is the documented "not asked for". The descriptor it returns
    // is the one `LocalFree` releases, exactly once, in the caller.
    let code = unsafe {
        GetNamedSecurityInfoW(
            name.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            &raw mut dacl,
            null_mut(),
            &raw mut descriptor,
        )
    };
    if code != 0 {
        return Err(launch(
            // The step names the read, which is a `GetNamedSecurityInfoW` and no
            // longer the tool: the same variant and the same step a failed handle
            // read reports, so a caller sees one read failure rather than two
            // shapes of one.
            "read the DACL",
            path.to_path_buf(),
            io::Error::from_raw_os_error(code as i32),
        ));
    }
    if dacl.is_null() {
        // The descriptor came back without a DACL: nothing to walk, and its own
        // allocation is still this function's to release.
        free_descriptor(descriptor);
        return Ok(None);
    }
    Ok(Some((dacl, descriptor)))
}

/// A SID this module allocated and therefore has to release.
///
/// **The value is the ownership**: `ConvertStringSidToSidW` allocates, `LocalFree`
/// is its documented release, and `Drop` is where that happens — so an early
/// return cannot leak one, and no call site derives the raw SID a second time.
/// It exists only for the [`ALL_APPLICATION_PACKAGES`](crate::sid::ALL_APPLICATION_PACKAGES) route, where the SID
/// arrives as text; a [`PackageSid`](crate::sid::PackageSid) already owns its own, which is why
/// [`Grants::apply`](super::grant::Grants::apply) parses nothing and this type stays off the per-path hot
/// path. Its caller parses once per standing *set* — the program directory, or one
/// batch of sibling DLLs — rather than once per granted file.
pub(super) struct OwnedSid(PSID);

impl OwnedSid {
    /// `text` as a SID, or [`Error::Launch`] naming `path` when it is not one.
    ///
    /// **A malformed SID is an error rather than a skip**: a grant made for a
    /// zero SID is an ACE for nobody that still rewrites the descriptor, and there
    /// is no SID to compare against, so the failure has to be loud. The spelling
    /// that reaches here is one of this crate's own two constants, so a failure is
    /// the API's contract breaking rather than operator input.
    pub(super) fn parse(text: &str, path: &Path) -> Result<Self> {
        let wide = to_wide(text);
        let mut raw: PSID = null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, and `raw` is a
        // valid out-parameter. On success the SID is this value's own allocation,
        // freed once in `Drop`; on failure the API promises nothing was
        // allocated, so there is nothing to free.
        let ok = unsafe { ConvertStringSidToSidW(wide.as_ptr(), from_mut(&mut raw)) };
        if ok == 0 || raw.is_null() {
            return Err(launch(
                "ConvertStringSidToSidW",
                path.to_path_buf(),
                io::Error::other(format!("`{text}` is not a SID")),
            ));
        }
        Ok(Self(raw))
    }

    /// The SID itself, as every Win32 call takes it.
    pub(super) fn raw(&self) -> PSID {
        self.0
    }
}

impl Drop for OwnedSid {
    fn drop(&mut self) {
        // SAFETY: `self.0` is this value's own allocation — `ConvertStringSidToSidW`
        // documents `LocalFree` as its release — and it is freed exactly once.
        unsafe { LocalFree(self.0.cast()) };
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process::Command,
    };

    // The names only a measurement needs: an `(F)` mask and the access bits the alias
    // table is pinned against (they are imported here rather than above because the
    // production read never names them; the bit that marks an inherited entry and the
    // generic bit the read must *not* treat as one are imported where their own readings
    // live — `test_support.rs` and `spec.rs` — and the ACE type a fixture builds is
    // imported by name beside that fixture, as the walk's own module publishes it).
    use windows_sys::Win32::{
        Security::{CONTAINER_INHERIT_ACE, NO_INHERITANCE, OBJECT_INHERIT_ACE},
        Storage::FileSystem::{
            FILE_ALL_ACCESS, FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_GENERIC_WRITE,
        },
    };

    use super::*;
    use crate::{
        acl::{
            grant::{GrantSid, grant_to},
            icacls::icacls_binary,
            test_support::{dacl_entries, descriptor_shape, masks, satisfied, standing},
        },
        sid::{ALL_APPLICATION_PACKAGES, PackageSid, SALT_BYTES, Salt, package_name},
    };

    /// **The route changed and the descriptor did not, and this is the
    /// measurement that says so.**
    ///
    /// Two trees of the same shape under one parent, so every entry they inherit
    /// from it is the same and the writer is the only difference: one is granted
    /// through [`grant_to`] — the handle route — and the other through
    /// `icacls /grant:r` — the route this crate used until 2026-09-27. The
    /// comparison is over the directory, a file and a subdirectory that were
    /// **seeded before either grant**, so it covers the three things a route change
    /// could quietly alter: the entry's mask and inheritance flags, the control
    /// word (auto-inherited and *not* protected), and the propagation of an
    /// inheritable ACE to the descendants that were already there.
    ///
    /// The tool is the oracle rather than a written-down constant, which is the
    /// same rule the DACL tests below follow for the whole layer: what the crate has to
    /// keep is the behaviour `icacls` had, and only the tool can say what that was.
    #[test]
    fn the_descriptor_the_handle_route_writes_is_the_one_icacls_writes() {
        const USERS: &str = "S-1-5-32-545";
        let dir = tempfile::tempdir().expect("temp dir");
        let mut trees = Vec::new();
        for name in ["by-handle", "by-tool"] {
            let root = dir.path().join(name);
            fs::create_dir_all(root.join("sub")).expect("create the tree");
            fs::write(root.join("pre.txt"), b"before the grant").expect("seed");
            trees.push(root);
        }
        let sid = OwnedSid::parse(USERS, dir.path()).expect("a SID");

        assert!(
            grant_to(
                &trees[0],
                Target::Directory,
                AccessMode::ReadOnly,
                GrantSid {
                    raw: sid.raw(),
                    text: USERS,
                },
            )
            .expect("the handle route writes"),
            "the handle route writes the first tree"
        );
        let granted = Command::new(icacls_binary())
            .arg(&trees[1])
            .args(["/grant:r", &format!("*{USERS}:(OI)(CI)RX"), "/q"])
            .output()
            .expect("run icacls");
        assert!(
            granted.status.success(),
            "{}",
            String::from_utf8_lossy(&granted.stderr)
        );

        for relative in [Path::new(""), Path::new("pre.txt"), Path::new("sub")] {
            assert_eq!(
                descriptor_shape(&trees[0].join(relative), sid.raw()),
                descriptor_shape(&trees[1].join(relative), sid.raw()),
                "the two routes disagree about `{}`",
                relative.display()
            );
        }
    }

    /// The access bits `icacls`'s aliases *stored*, read back out of a DACL the
    /// tool wrote — and the constants below are the same numbers derived from the
    /// generic rights.
    ///
    /// **This is the whole "API predicate agrees with the tool" argument, and it
    /// is a measurement rather than a transcription.** `icacls` accepts `(M)`,
    /// `(RX)` and `(F)` as aliases and stores the mask behind the alias, so the
    /// read has to compare those bits: measured on this host 2026-09-25,
    /// `:(OI)(CI)RX` stored `0x0012_00A9` and `:(OI)(CI)M` stored `0x0013_01BF`,
    /// and `:(OI)(CI)F` — the third iteration, added with the mask the `F` alias
    /// stores — is what makes the containment test's `F` claim a measurement. If
    /// a future tool or filesystem stores something else, this test fails before
    /// the skip can start rewriting descriptors it did not need to.
    #[test]
    fn the_aliases_icacls_writes_are_the_masks_this_read_compares() {
        const RX: u32 = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE;
        assert_eq!(RX, 0x0012_00A9);
        let modify = RX | FILE_GENERIC_WRITE | 0x0001_0000;
        assert_eq!(modify, 0x0013_01BF);
        // **And the one table the write and the read share is that same pair**:
        // [`ace_bits`] is what a grant is written with, so pinning the table here
        // pins the masks a granted ACE carries against the aliases the tool stores
        // — one definer behind the write, the read's containment and the listing
        // below, rather than three that could drift apart.
        let both = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
        assert_eq!(
            ace_bits(Target::Directory, AccessMode::ReadOnly),
            (RX, both),
            "the directory's read shape"
        );
        assert_eq!(
            ace_bits(Target::Directory, AccessMode::ReadWrite),
            (modify, both),
            "the directory's write shape"
        );
        assert_eq!(
            ace_bits(Target::File, AccessMode::ReadOnly),
            (RX, NO_INHERITANCE),
            "a file's shape propagates to nothing"
        );
        assert_eq!(
            ace_bits(Target::ProgramDirectory, AccessMode::ReadWrite),
            (modify, NO_INHERITANCE),
            "the program directory's shape propagates to nothing"
        );

        let dir = tempfile::tempdir().expect("temp dir");
        // **The oracle's control, and the false pass it removes.** Nothing has
        // been granted to this directory, yet a reading that counts every
        // access-allowed entry answers "RX is there": the entry is the
        // *inherited* `(OI)(CI)(RX)` ACE the temp volume puts on every directory
        // it creates, and it names `BUILTIN\Users`, the very SID the grants below
        // name. An oracle over every entry therefore passed for a tool that wrote
        // nothing (SPEC-P2-03), and what the assertions from here on read is the
        // explicit set — the entries without `INHERITED_ACE`, which is what
        // "`icacls` stored this" means.
        //
        // The two assertions are one demonstration: the inherited entry is
        // there, and it is not what the oracle counts.
        // The SID every reading below is about, resolved once: the walk compares
        // trustees with `EqualSid`, and this test names the one its grants name.
        let users = "S-1-5-32-545";
        let sid = OwnedSid::parse(users, dir.path()).expect("a SID");
        let untouched = dir.path().join("untouched");
        fs::create_dir_all(&untouched).expect("create");
        assert!(
            masks(&untouched, false, sid.raw()).contains(&RX),
            "this host's temp volume puts no inherited (RX) entry on a new \
directory, so the false pass this control is about is not exercised here: {:08X?}",
            masks(&untouched, false, sid.raw())
        );
        assert!(
            !masks(&untouched, true, sid.raw()).contains(&RX),
            "nothing has been granted here, so an oracle that answered RX from \
an inherited entry is answering for the wrong reason: {:08X?}",
            masks(&untouched, true, sid.raw())
        );

        for (spec, wanted) in [
            ("(OI)(CI)RX", RX),
            ("(OI)(CI)M", modify),
            ("(OI)(CI)F", FILE_ALL_ACCESS),
        ] {
            let path = dir.path().join(spec.replace(['(', ')'], ""));
            fs::create_dir_all(&path).expect("create");
            let granted = Command::new(icacls_binary())
                .arg(&path)
                .args(["/grant:r", &format!("*{users}:{spec}"), "/q"])
                .output()
                .expect("run icacls");
            assert!(
                granted.status.success(),
                "{}",
                String::from_utf8_lossy(&granted.stderr)
            );
            assert!(
                masks(&path, true, sid.raw()).contains(&wanted),
                "icacls stored something other than 0x{wanted:08X} for {spec}: {:08X?}",
                masks(&path, true, sid.raw())
            );
        }
    }

    /// **The skip the whole read exists for, against a real DACL**: the same call
    /// twice writes the first time and skips the second, and both halves are
    /// asserted against the kernel's DACL rather than against the tool's text.
    ///
    /// The second call is the one that matters. A read that answered "not
    /// granted" would rewrite the descriptor every run, which on a directory is a
    /// descriptor write propagating over the whole subtree — the cost the skip
    /// exists to remove, and the cost the text oracle silently paid for a
    /// well-known SID.
    #[test]
    fn a_satisfied_grant_is_not_written_twice() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        // The standing SID, resolved once — the pair every `grant_to` below takes,
        // which is the resolve-once shape the production route uses.
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");

        assert!(
            !satisfied(path, Target::Directory, AccessMode::ReadOnly),
            "nothing granted yet, so the write is due"
        );
        assert!(
            grant_to(
                path,
                Target::Directory,
                AccessMode::ReadOnly,
                standing(&sid)
            )
            .expect("grant"),
            "the first call writes"
        );
        assert!(
            satisfied(path, Target::Directory, AccessMode::ReadOnly),
            "the ACE is in the DACL now, found through the API and not through a name"
        );
        assert!(
            !grant_to(
                path,
                Target::Directory,
                AccessMode::ReadOnly,
                standing(&sid)
            )
            .expect("grant"),
            "the second call is a read and nothing else"
        );

        // **And the mask is read as well as the SID**: `RX` does not satisfy a
        // read-write request, while the `M` the next call writes satisfies both,
        // which is what `Object::granted`'s predicate states and what the old text
        // comparison spelled `(RX)`/`(M)`/`(F)`.
        assert!(
            !satisfied(path, Target::Directory, AccessMode::ReadWrite),
            "a read-only ACE is not the read-write grant"
        );
        assert!(
            grant_to(
                path,
                Target::Directory,
                AccessMode::ReadWrite,
                standing(&sid)
            )
            .expect("grant"),
            "the wider grant is written"
        );
        assert!(
            satisfied(path, Target::Directory, AccessMode::ReadOnly),
            "`M` satisfies a read-only request"
        );
        // **A shape that disagrees with the object is refused, and that is the
        // check the tool could not make**: a file-shaped grant on a directory
        // carries no `(OI)(CI)`, so it would leave every descendant ungranted
        // while `icacls` reported success. The handle answers what the object is,
        // and the answer stops the grant before a descriptor is touched —
        // `a_directory_shaped_grant_on_a_file_is_refused` pins the other
        // direction, and `the_flags_are_the_difference_between_the_two_directory_
        // questions` the read's own half of the same rule.
        let refused = grant_to(path, Target::File, AccessMode::ReadOnly, standing(&sid))
            .expect_err("a file-shaped grant on a directory is refused");
        assert!(
            refused.to_string().contains("not the kind of object"),
            "{refused}"
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
    }

    /// **A wildcard is refused rather than expanded, and that is the fix rather
    /// than a limitation.**
    ///
    /// A `*.dll` pattern was the sibling batch's grant: `icacls` matched it against
    /// the directory and wrote an ACE on every file it named — including one the
    /// enumeration had counted as a file because `is_file` followed the link. No
    /// handle can be opened on a wildcard, so the batch is gone and this is what a
    /// caller that still hands this crate a pattern gets: an open failure naming the
    /// path, not a grant on whatever the pattern happened to match (measured on this
    /// host 2026-09-27: `CreateFileW` on a literal `…\*.dll` fails
    /// `ERROR_INVALID_NAME`, the same `123` the name-based DACL read reported).
    #[test]
    fn a_wildcard_path_is_refused_rather_than_expanded() {
        let dir = tempfile::tempdir().expect("temp dir");
        for name in ["one.dll", "two.dll"] {
            fs::write(dir.path().join(name), b"not a library").expect("dll");
        }
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");
        let pattern = dir.path().join("*.dll");

        let error = grant_to(&pattern, Target::File, AccessMode::ReadOnly, standing(&sid))
            .expect_err("a wildcard names no object a handle can be opened on");
        let text = error.to_string();
        assert!(text.contains("open the granted path"), "{text}");
        assert!(text.contains("*.dll"), "the failure names the path: {text}");
        // And nothing was written through the pattern: both files are untouched.
        for name in ["one.dll", "two.dll"] {
            assert!(
                !acl_names(&dir.path().join(name), sid.raw()).expect("read the DACL"),
                "{name} took an ACE through a pattern"
            );
        }
    }

    /// **The flags are read as well as the mask, and they only ever narrow a
    /// directory's question.**
    ///
    /// A directory grant is the inheritable one: an entry that lost `(OI)(CI)`
    /// reaches no descendant and is not the grant the caller asked for, while the
    /// file-shaped question asks for no flags at all. Both halves are read off a
    /// real DACL rather than off a synthetic ACE, because the flags travel through
    /// the filesystem and the tool on the way in — and the read is
    /// [`Object::granted`]'s, the same one the skip makes against the handle the
    /// write would use.
    #[test]
    fn the_flags_are_the_difference_between_the_two_directory_questions() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");
        let asked = |target| {
            Object::open(path)
                .expect("open")
                .granted(sid.raw(), target, AccessMode::ReadOnly)
        };

        // The plain directory ACE `icacls` writes for the file shape: the mask is
        // there and the flags are not, which is exactly the entry a grant that
        // "lost its flags" leaves.
        let plain = Command::new(icacls_binary())
            .arg(path)
            .args(["/grant:r", "*S-1-15-2-1:M", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            plain.status.success(),
            "{}",
            String::from_utf8_lossy(&plain.stderr)
        );
        assert!(
            asked(Target::File).expect("read the DACL"),
            "the SID is in the DACL with a wide enough mask, so the flag-free question is met"
        );
        assert!(
            !asked(Target::Directory).expect("read the DACL"),
            "an entry without `(OI)(CI)` is not what a directory grant was asked for"
        );

        // The inheritable form satisfies both questions, which is the relation the
        // caller's own grant relies on: `Grants::apply` writes this one and
        // `Grants::apply_siblings` writes the plain one.
        let inherited = Command::new(icacls_binary())
            .arg(path)
            .args(["/grant:r", "*S-1-15-2-1:(OI)(CI)M", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            inherited.status.success(),
            "{}",
            String::from_utf8_lossy(&inherited.stderr)
        );
        assert!(asked(Target::Directory).expect("read the DACL"));
        assert!(asked(Target::File).expect("read the DACL"));

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
        assert!(!asked(Target::Directory).expect("read the DACL"));
    }

    /// **The two opens ask for what their half needs and nothing more**, which is
    /// the fix for a regression this crate's first handle-based grant introduced:
    /// one `READ_CONTROL | WRITE_DAC` open for every path turned a satisfied
    /// grant — the steady state of a standing ACE, and every launch after the first
    /// on a root — into a startup failure wherever the launcher may read a root but
    /// may not re-ACL it.
    ///
    /// **It is pinned at the access mask rather than at a DACL, and the reason is
    /// measured** (this host, 2026-09-27): an ACL experiment cannot show it, because
    /// a `(WD)` **deny** for the account this process runs as does not take
    /// `WRITE_DAC` away — Windows grants an object's owner `READ_CONTROL` and
    /// `WRITE_DAC` implicitly, so the open succeeds with the deny ACE in place. What
    /// the split is about is the *request*, and that is what this reads: a due grant
    /// reads with `READ_CONTROL` and then asks for `WRITE_DAC` to write; a satisfied
    /// grant asks once, for `READ_CONTROL`, and never mentions the write access.
    #[test]
    fn a_satisfied_grant_asks_for_no_write_access() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");

        let _ = opens::take();
        assert!(
            grant_to(
                path,
                Target::Directory,
                AccessMode::ReadOnly,
                standing(&sid)
            )
            .expect("the first grant is due and is written"),
            "the first call writes"
        );
        assert_eq!(
            opens::take(),
            vec![READ_CONTROL, READ_CONTROL | WRITE_DAC],
            "a due grant reads, then opens again for the write"
        );

        assert!(
            !grant_to(
                path,
                Target::Directory,
                AccessMode::ReadOnly,
                standing(&sid)
            )
            .expect("the second call is a read"),
            "the second call skips"
        );
        assert_eq!(
            opens::take(),
            vec![READ_CONTROL],
            "a satisfied grant never asks for WRITE_DAC"
        );
    }

    /// **A path the kernel cannot open is an error and not a satisfied grant** —
    /// the rule [`has_ace`](super::grant::has_ace)'s callers rest on, one layer earlier: the grant opens
    /// the object before it reads anything, so a path that does not exist fails
    /// there, with the step named and the path attached.
    ///
    /// **And the grant that landed before the failure is still there**, which is
    /// the half `tests/confine.rs`'s `a_grant_that_fails_leaves_the_earlier_ones_standing`
    /// carried until the 2026-10-01 placement cleanup folded it here
    /// (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`): a failed
    /// grant takes nothing back, so the state a later run on the same root finds is
    /// the ACE the first grant wrote — the reason a second launch on a served root
    /// is the cheap one (`grant_to`'s skip). The guard's own half of the same rule
    /// (a dropped `Grants` removes nothing) is
    /// `dropping_the_grants_leaves_them_standing`'s, which asks it with a derived
    /// package SID.
    ///
    /// **The two grants are the standing `ALL_APPLICATION_PACKAGES` SID the rest of
    /// this module's tests grant with**, deliberately: a derived `PackageSid` needs
    /// `CreateAppContainerProfile`, which a host can refuse (`hr=0x80070005`,
    /// measured), and this leg's subject — the failure names the path, and the ACE
    /// that landed before it is not taken back — is about the DACL, not about the
    /// profile.
    #[test]
    fn the_grant_refuses_a_path_it_cannot_open() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("root");
        fs::create_dir_all(&root).expect("create the served root");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, &root).expect("a SID");

        assert!(
            grant_to(
                &root,
                Target::Directory,
                AccessMode::ReadWrite,
                standing(&sid)
            )
            .expect("the first grant lands"),
            "the first call writes"
        );
        let missing = root.join("never-created");
        let error = grant_to(
            &missing,
            Target::Directory,
            AccessMode::ReadOnly,
            standing(&sid),
        )
        .expect_err("a path that does not exist is an error, not a satisfied grant");
        let text = error.to_string();
        assert!(text.contains("never-created"), "{text}");
        assert!(
            text.contains("open the granted path"),
            "the failure names the step it happened at: {text}"
        );
        // The oracle is this module's own DACL read — the predicate `has_ace`
        // wraps — asked of the object the first call wrote and the failure did not
        // touch.
        assert!(
            acl_names(&root, sid.raw()).expect("read the DACL"),
            "the grant that landed before the failure is still standing: nothing is taken back"
        );

        let taken_back = Command::new(icacls_binary())
            .arg(&root)
            .args(["/remove:g", "*S-1-15-2-1", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            taken_back.status.success(),
            "{}",
            String::from_utf8_lossy(&taken_back.stderr)
        );
    }

    /// **A deny ACE is not the grant it resembles, and the skip is the judgement
    /// this pins.**
    ///
    /// `icacls /deny` stores an `ACCESS_DENIED_ACE` whose mask, flags and trustee are
    /// the *same shape* an access-allowed entry has: `(OI)(CI)F` is
    /// `FILE_ALL_ACCESS`, which contains every bit [`ace_bits`] asks for, and the
    /// SID is the very one [`Object::granted`] compares — so a read that took the
    /// entry's type for granted would call this DACL a satisfied grant, skip the
    /// write, and leave a confined child with an ACE that denies it everything.
    /// That is the fail-open [`Ace::Other`]'s `Continue` arm exists to refuse.
    ///
    /// The deny names [`ALL_APPLICATION_PACKAGES`](crate::sid::ALL_APPLICATION_PACKAGES) rather than another trustee on
    /// purpose: for any other SID the verdict would be `false` even from a read
    /// that ignored the type, so the entry would not discriminate. It is also the
    /// one SID a deny can name without touching this process's own access — the
    /// package SID is in no interactive token, which is what lets the test keep
    /// opening and reading the object afterwards.
    ///
    /// The grant that follows is the second half of the same judgement: the read
    /// answers "not granted", so the write is due, and the entry it writes is
    /// found again with the deny still in the DACL — the walk passes the deny over
    /// rather than stopping at it.
    #[test]
    fn a_deny_for_the_package_sid_is_not_the_grant_it_resembles() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");

        let denied = Command::new(icacls_binary())
            .arg(path)
            .args(["/deny", "*S-1-15-2-1:(OI)(CI)F", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            denied.status.success(),
            "{}",
            String::from_utf8_lossy(&denied.stderr)
        );

        assert!(
            !satisfied(path, Target::Directory, AccessMode::ReadOnly),
            "a deny entry carries the mask and the flags a grant is recognized by, so a \
             read that did not ask the entry's type would call this grant satisfied and \
             skip the write the child needs"
        );
        assert!(
            grant_to(
                path,
                Target::Directory,
                AccessMode::ReadOnly,
                standing(&sid)
            )
            .expect("the write is due and lands"),
            "the read answered `not granted`, so the grant is written"
        );
        assert!(
            satisfied(path, Target::Directory, AccessMode::ReadOnly),
            "the walk passes the deny over and still reaches the entry the grant wrote"
        );

        let taken_back = Command::new(icacls_binary())
            .arg(path)
            .args(["/remove:d", "*S-1-15-2-1", "/q"])
            .output()
            .expect("run icacls");
        assert!(
            taken_back.status.success(),
            "{}",
            String::from_utf8_lossy(&taken_back.stderr)
        );
    }

    /// **The AppContainer profile's own directory carries exactly its parent's
    /// descriptor — and leaves with the profile.**
    ///
    /// The accumulation half of that item was answered when it was measured that
    /// `DeleteAppContainerProfile` removes `%LOCALAPPDATA%\Packages\<name>`; what
    /// had never been measured is what that directory *says* while it exists, which
    /// is the question this test asks and pins. **Measured on this host,
    /// 2026-09-28**:
    ///
    /// - the directory exists as soon as `CreateAppContainerProfile` returns, and
    ///   it is gone once the [`PackageSid`](crate::sid::PackageSid) drops — the delete's own half, asked of
    ///   the same directory rather than inferred;
    /// - its DACL is the parent `%LOCALAPPDATA%\Packages`' DACL **entry for
    ///   entry**, with the same control word — `0x8404`, that is `SE_DACL_PRESENT |
    ///   SE_DACL_AUTO_INHERITED | SE_SELF_RELATIVE` — and five access-allowed
    ///   entries, every one of them inherited (`flags=0x13`, `INHERITED_ACE` plus
    ///   `(OI)(CI)`): on this host `FLIER-WIN11\CodexSandboxUsers` and one SID this
    ///   host cannot resolve, both `(I)(OI)(CI)(M)` (`mask=0x001301BF`), then
    ///   `NT AUTHORITY\SYSTEM`, `BUILTIN\Administrators` and the operator, all
    ///   `(I)(OI)(CI)(F)` (`mask=0x001F01FF`);
    /// - **no package SID and no capability SID appears in it**, so the directory
    ///   gives the confined child nothing beyond what the parent directory of every
    ///   package on the machine already gives it;
    /// - a second run for the same name produces the same descriptor, so the
    ///   per-run directory is not a per-run widening either.
    ///
    /// **The answer is therefore "nothing to do"**, which the design's residue row
    /// records: no cleanup step is added beside [`PackageSid`](crate::sid::PackageSid)'s `Drop` — the delete
    /// already takes the directory with it — and no ACE of this crate's own is
    /// written here, because an entry this crate added would be the only thing in
    /// the tree that widens the directory.
    ///
    /// The reading is the module's own ([`dacl_entries`]: `GetNamedSecurityInfoW`,
    /// the control word, and [`walk_dacl`]'s entries with their SIDs rendered), and
    /// the SID handed to the walk is the profile's own — which is what makes an
    /// entry naming the container arrive as `(this SID)` rather than as a string to
    /// search for.
    #[test]
    fn the_profile_directory_inherits_its_descriptor_and_leaves_with_the_profile() {
        let dir = tempfile::tempdir().expect("temp dir");
        // Lexical, like every other derivation here: the served root does not have
        // to exist for its package name to.
        let name = package_name(&dir.path().join("served"));
        let packages =
            PathBuf::from(env::var_os("LOCALAPPDATA").expect("LOCALAPPDATA")).join("Packages");
        let target = packages.join(&name);

        let mut runs = Vec::new();
        for run in 0..2 {
            let sid = PackageSid::derive(&name).expect("a profile");
            assert!(
                target.is_dir(),
                "run {run}: the profile creation made {} — the directory this \
                 measurement is about",
                target.display()
            );
            let reading = dacl_entries(&target, sid.raw());
            assert!(
                !reading.1.is_empty(),
                "run {run}: an empty DACL would make every comparison below vacuous"
            );
            // The parent's own descriptor, read with the same SID so the two
            // readings are one question asked of two directories.
            assert_eq!(
                reading,
                dacl_entries(&packages, sid.raw()),
                "run {run}: the profile directory's DACL is not the one it inherits \
                 from {}",
                packages.display()
            );
            assert!(
                reading.1.iter().all(|entry| entry.contains("flags=0x1")),
                "run {run}: every entry is inherited rather than explicit: {:?}",
                reading.1
            );
            assert!(
                !reading.1.iter().any(|entry| entry.contains("S-1-15-")),
                "run {run}: the directory names a container SID, so it is reachable \
                 by a confined child that was granted nothing: {:?}",
                reading.1
            );
            runs.push(reading);
            drop(sid);
            assert!(
                !target.is_dir(),
                "run {run}: the profile is gone and {} is still there",
                target.display()
            );
        }
        assert_eq!(
            runs[0], runs[1],
            "a second run for the same name made a directory with a different \
             descriptor"
        );
    }

    /// **The salt file's portable property, read with this module's own ACE walk** — the
    /// leg that replaces the descriptor assertion the crate's create call used to earn:
    /// the file exists, **this process can read it and write it**, and its descriptor is
    /// **not** the single-entry protected shape that call installed.
    ///
    /// **What is read is the file's shape, and what is asserted is the absence of one.**
    /// The file now takes what the state directory gives it, and the state directory takes
    /// what `%LOCALAPPDATA%` gave *it* — the host's own answer — so a leg naming `SYSTEM`
    /// or a logon-session SID would redden on a machine configured differently (the
    /// design spec's portable-property rule). The old shape is unambiguous beside it: one
    /// access-allowed entry, explicit rather than inherited, with the descriptor's
    /// `SE_DACL_PROTECTED` bit set — which is what the create-time descriptor this crate
    /// used to install asked
    /// for, and which this leg would have failed on before the removal.
    ///
    /// **The exposure the removal leaves is measured elsewhere and stated in prose**: a
    /// file in this directory is reachable by whatever the directory's descriptor reaches,
    /// and `docs/sandbox.md` plus the design spec carry that fact rather than this leg.
    ///
    /// The SID handed to the walk is not in the file, so every entry renders as its own
    /// trustee rather than as "(this SID)".
    #[test]
    fn the_salt_file_is_a_plain_file_this_process_can_use() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file = dir.path().join("acl-salt");
        // The load is what the leg is about: it creates the file this reading is then made
        // of, and nothing needs the value itself.
        Salt::load_at(&file).expect("the salt is created");

        assert!(file.is_file(), "the salt's first use creates its file");
        // The property a reader cares about, and the one the deleted descriptor leg stood
        // in for: the run that published the value can use it.
        let bytes = fs::read(&file).expect("this process reads the salt file it published");
        assert_eq!(bytes.len(), SALT_BYTES, "the whole value is on the disk");
        fs::write(&file, &bytes).expect("this process writes the salt file it published");

        // A SID certainly absent from the file: the well-known ALL APPLICATION
        // PACKAGES text, parsed rather than derived (nothing here needs a profile).
        let absent = OwnedSid::parse(ALL_APPLICATION_PACKAGES, &file).expect("the constant");
        let reading = dacl_entries(&file, absent.raw());

        // **The measurement, printed so a reader can see what the host gave the file.**
        println!(
            "measured: the salt file's DACL control=0x{:04X} entries={:?}",
            reading.0, reading.1
        );
        assert!(
            !(reading.1.len() == 1
                && !reading.1[0].contains("flags=0x1")
                && reading.0 & 0x1000 != 0),
            "the salt file carries the protected single-entry descriptor the old create \
             installed — one explicit entry and `SE_DACL_PROTECTED` set: control=0x{:04X} \
             entries={:?}",
            reading.0,
            reading.1
        );
    }
}
