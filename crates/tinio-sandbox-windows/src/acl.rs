//! ACL whitelist: grant the package SID an access mode on each exposed path, and
//! leave those grants standing — the launcher takes nothing back on the way out
//! (decisions 15, 21, §6).
//!
//! **Writes go through `icacls.exe` resolved from the system directory, not
//! `PATH` and not `SetNamedSecurityInfo`; reads go through
//! `GetNamedSecurityInfoW`.** `/grant:r` replaces this SID's ACE only;
//! `/remove:g *<sid>` removes it. [`Error::Launch`] carries the tool's status
//! and text, and a failed read carries the Win32 code instead.
//!
//! **The read half is the Win32 API because the tool's *text* cannot answer the
//! question the read exists for.** The read decides whether the write may be
//! skipped (`already_granted`), and `icacls` renders a well-known SID by
//! **name**: an ACE written for [`ALL_APPLICATION_PACKAGES`] comes back as
//! `APPLICATION PACKAGE AUTHORITY\ALL APPLICATION PACKAGES:(RX)`, so a check for
//! the literal `S-1-15-2-1` never saw the ACE it had just written and the skip
//! never fired — which is not a cosmetic failure, because the write it failed to
//! skip is `SetNamedSecurityInfoW` propagating eagerly over the whole subtree
//! (10–30 s for this workspace's `target\debug`; measured 2026-09-25). Two
//! spellings patched that symptom; [`has_ace`] ends the class, because
//! `EqualSid` compares the two SIDs as the kernel compares them and no rendering
//! takes part. [`mentions_sid`] survives as the *test* oracle over the tool's own
//! text, where a rendering is the thing being asserted rather than the thing
//! being trusted.
//!
//! The read is this module's one `unsafe` site, and it is narrower than the
//! writes: a security descriptor and nothing else, released once with
//! `LocalFree` (the ACL belongs to the descriptor and may **not** be freed on its
//! own). Decision 14 enumerates the sites.
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

// The manifest denies `unsafe_code` and the exception sits on the module that
// needs it, as decision 14 asks: the descriptor read is the only block below,
// and a crate-level allow would lift the lint for every module instead.
#![allow(unsafe_code)]

use std::{
    ffi::{OsStr, c_void},
    io,
    mem::size_of,
    path::{Path, PathBuf},
    process::{Command, Output},
    ptr::{addr_of, from_mut, null_mut},
    sync::Arc,
};

use windows_sys::Win32::{
    Foundation::LocalFree,
    Security::{
        ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
        Authorization::{ConvertStringSidToSidW, GetNamedSecurityInfoW, SE_FILE_OBJECT},
        CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
        INHERIT_ONLY_ACE, OBJECT_INHERIT_ACE, PSECURITY_DESCRIPTOR, PSID,
    },
    Storage::FileSystem::{FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_GENERIC_WRITE},
};

/// `ACCESS_ALLOWED_ACE_TYPE` — the `AceType` of the only ACE this module reads.
/// `windows-sys` exports the structure and not the type code for it, so the one
/// value is named here rather than the ACE being assumed to be one.
const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

use crate::{
    _core::{AccessMode, Result, launch},
    backend::SiblingScan,
    handles::system_directory,
    sid::{ALL_APPLICATION_PACKAGES, PackageSid},
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
/// §6's whitelist, one ACE per exposed path instead of a `/t` walk over
/// every file in the tree), and decision 22's program-directory entry deliberately
/// does not.
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
/// descendants that were already there — and what makes a grant on a directory
/// cost a pass over the whole tree (design §6 and its Risks row).
///
/// [`Target::Directory`] is therefore the only variant that carries the flags: a
/// file cannot take them (the no-op above) and the program's directory must not
/// (decision 22).
///
/// **`Target::File` and `Target::ProgramDirectory` come out of this function
/// byte-identical, and that is a coincidence rather than one shape with two
/// names.** Both are non-inherited, so both are `RX`/`M`; what separates them is a
/// *policy* — the program directory is decision 22's "this ACE stops at the
/// directory" — which a file cannot carry and which a directory must not lose
/// silently. The variants are therefore deliberately **not** merged: the type is
/// the only thing that keeps a call site from handing a file the directory-shaped
/// intent or the other way round, and `(OI)(CI)` is the single token in this table
/// that distinguishes a shape at all — it is what separates [`Target::Directory`]
/// from both of the others. Which variant a call site holds is checked where it is
/// chosen: `backend::grant_whitelist` refuses a program-directory shape whose path
/// is not a directory, rather than handing `icacls` a grant it would report as
/// successful and write nothing for.
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
/// [`Error::Launch`](crate::_core::Error::Launch) naming the path and carrying
/// `icacls`'s own diagnostic when it exits non-zero.
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
    grant_to(path, target, access, sid.as_str()).map(drop)
}

/// [`grant`] for a SID that is not a [`PackageSid`] — the standing
/// [`ALL_APPLICATION_PACKAGES`] grant — and the shape every grant in this crate
/// goes through.
///
/// **The write is skipped when the DACL already carries what it would write**,
/// and that skip is what makes a standing grant free. `icacls /grant:r` rewrites
/// the security descriptor even when the ACE it names is already there, and a
/// descriptor write on a directory propagates eagerly over its whole subtree
/// (`SetNamedSecurityInfoW`; measured ≈0.04 ms per object for this shape on ReFS,
/// 10–20 s for this workspace's `target\debug`). So the steady state is a read:
/// [`already_granted`] asks the kernel's DACL — `GetNamedSecurityInfoW` and
/// `EqualSid` — and a satisfied path never reaches the tool's write at all. The
/// `bool` says whether the descriptor was written, which is what
/// [`Grants::apply_standing`] logs.
///
/// **[`GrantOutcome::Unanswerable`] writes**, and that is the only one of the
/// three answers that does not come from a DACL: a wildcard path (see
/// [`already_granted`]) is one nothing can read, and a grant that is not written
/// at all would leave a confined child without the loader's own DLLs.
fn grant_to(path: &Path, target: Target, access: AccessMode, sid: &str) -> Result<bool> {
    if already_granted(path, sid, target, access)? == GrantOutcome::AlreadyGranted {
        // Shared §7's per-operation event, with `written = false` so a log shows
        // the skip rather than a silent absence of the line above it.
        tracing::debug!(
            step = "icacls /grant:r",
            path = %path.display(),
            access = ?access,
            sid,
            written = false
        );
        return Ok(false);
    }
    let spec = grant_spec(sid, target, access);
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
        sid,
        written = true
    );
    Ok(true)
}

/// The spellings `icacls` may render `sid` under in its listing.
///
/// **A well-known SID is listed by name, not by its literal.** A grant to
/// [`ALL_APPLICATION_PACKAGES`] (`S-1-15-2-1`) comes back as
/// `APPLICATION PACKAGE AUTHORITY\ALL APPLICATION PACKAGES:(RX)`, so a text
/// oracle that looks only for the literal never sees the ACE that is there.
/// Measured on this host 2026-09-25: that is what made the sibling batch's skip
/// never fire, and what `tests/acl.rs`'s post-condition could not see either —
/// which is why the *read* no longer goes through text at all ([`has_ace`]).
///
/// A package SID (`S-1-15-2-<8 groups>`) has no name to render, so it comes back
/// literally and needs no second spelling.
fn sid_spellings(sid: &str) -> Vec<&str> {
    if sid == ALL_APPLICATION_PACKAGES {
        vec![sid, "ALL APPLICATION PACKAGES"]
    } else {
        vec![sid]
    }
}

/// Whether `text` — an `icacls` listing — carries an entry for `sid`, in any
/// spelling `sid_spellings` allows.
///
/// **It is a test oracle now and no longer the production read.** [`has_ace`] and
/// `already_granted` ask the kernel's DACL with [`EqualSid`], so nothing this
/// crate *decides* rides on how a SID is rendered; what still does is every suite
/// that asserts against the tool's own output — `tests/acl.rs` pins
/// [`ALL_APPLICATION_PACKAGES`] on the sibling batch through it, and
/// `tests/confine.rs` asks whether the program directory carries the standing SID
/// at all. Keeping one implementation of that question is what stops the oracle
/// and the tool from drifting apart, and the rendering it handles is the reason
/// the predicate exists: `S-1-15-2-1` is printed as `ALL APPLICATION PACKAGES`.
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
#[must_use]
pub fn mentions_sid(text: &str, sid: &str) -> bool {
    text.lines().any(|line| {
        sid_spellings(sid)
            .into_iter()
            .any(|spelling| line.contains(spelling))
    })
}

/// The SID an access-allowed ACE names.
///
/// The ACE has already been established as an `ACCESS_ALLOWED_ACE` — the only
/// `AceType` [`any_ace_in`] reads an ACE as — which is what makes the `SidStart`
/// field meaningful rather than a guess at some other ACE's layout. It has a known
/// offset by the structure's definition (a header, a mask and then the SID
/// inline), so this borrows the SID inside the descriptor and copies nothing:
/// `EqualSid` reads it for the length the SID's own header carries, inside the ACL
/// the descriptor owns.
fn ace_sid(ace: *mut c_void) -> PSID {
    // SAFETY: the caller's contract (above) is that `ace` points at an
    // `ACCESS_ALLOWED_ACE` inside a descriptor that is alive for the call, so
    // reading its `SidStart` field is in bounds and aligned, and the field is
    // the first byte of that ACE's SID.
    unsafe { addr_of!((*(ace as *const ACCESS_ALLOWED_ACE)).SidStart) as PSID }
}

/// Whether `ace` — an access-allowed entry — carries `mask` or more, in the
/// flags `target` and `access` require.
///
/// **The masks are the API spelling of `icacls`'s own aliases, derived from the
/// generic rights rather than transcribed**, and that is the whole alignment
/// argument: the tool writes what its alias expands to, so comparing those bits
/// byte for byte decides the read exactly as the tool's own rendering did — `M`
/// is `FILE_GENERIC_READ | WRITE | EXECUTE | DELETE` and `RX` is the read and
/// execute halves plus `SYNCHRONIZE`. `ACCESS_ALLOWED_ACE::Mask` is what
/// `GetNamedSecurityInfoW` reports for an entry, the alias is not stored, and the
/// tests below pin both numbers against `icacls`.
///
/// **Containment is the test, not equality.** A DACL may carry `R`, `F`, a wider
/// mask or one written by another tool, and every one of those that *includes*
/// the request satisfies it — an equality would report a path that already has
/// more than enough as unsatisfied and rewrite its descriptor, which is the cost
/// this read exists to avoid.
///
/// **`F` needs no branch of its own**: what `icacls` stores for `(F)` is
/// `FILE_ALL_ACCESS` (`0x001F_01FF`), which carries every bit the other two masks
/// have, so the same containment test answers it — and the generic bit is not a
/// shortcut to that: an ACE whose mask is `GENERIC_ALL` and nothing else grants
/// no specific right at all, which is the false skip this test used to take.
/// **An `INHERIT_ONLY` ACE fails every target**: such an entry reaches nothing on
/// this object, so it is not the grant a caller asked for however wide its mask.
fn access_matches(ace: &ACCESS_ALLOWED_ACE, access: AccessMode, target: Target) -> bool {
    /// `DELETE` (`0x0001_0000`), which `icacls` folds into `(M)` and not into
    /// `(RX)`; `windows-sys` exports it as a generic-mapping bit only.
    const DELETE: u32 = 0x0001_0000;
    /// `icacls`'s `(M)` — the alias [`ace_spec`] writes.
    const MODIFY: u32 = FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE;
    /// `icacls`'s `(RX)` — the alias [`ace_spec`] writes.
    const READ_AND_EXECUTE: u32 = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE;

    let flags = u32::from(ace.Header.AceFlags);
    // An inherit-only ACE is not effective on this object at all, so it cannot
    // satisfy a grant whatever its mask says.
    if flags & INHERIT_ONLY_ACE != 0 {
        return false;
    }
    // **`F` is not a branch of its own, and the branch that stood here was
    // wrong.** `icacls`'s `(F)` stores `FILE_ALL_ACCESS` (`0x001F_01FF`), which
    // contains every bit both masks above are built from, so this containment
    // test answers it; the `GENERIC_ALL` disjunct that used to sit beside it
    // fired only for a mask literally carrying bit 28 — which an access check
    // expands into *no specific right at all* — and a skip that fired there left
    // a confined child without the access it was launched for (`(F)`'s own
    // measurement, and the false skip it pins, are in `a_mask_carrying_only_the_
    // generic_bit_does_not_satisfy_a_grant`).
    let mask_ok = match access {
        AccessMode::ReadWrite => ace.Mask & MODIFY == MODIFY,
        AccessMode::ReadOnly => ace.Mask & READ_AND_EXECUTE == READ_AND_EXECUTE,
    };
    let flags_ok = target != Target::Directory
        || (flags & (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE)
            == OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE);
    mask_ok && flags_ok
}

/// Whether any entry in `path`'s DACL names `sid` with at least the access
/// [`grant_to`] would write.
///
/// This is [`already_granted`] and [`has_ace`] behind one traversal, because they
/// are one question asked twice: a SID in the DACL is a grant, and whether its
/// mask is enough is what decides the skip.
fn acl_grants(path: &Path, sid: PSID, target: Target, access: AccessMode) -> Result<bool> {
    // `false` for a target that needs no flags (the `Target::File` and
    // `Target::ProgramDirectory` shapes), where `icacls` too makes no such
    // demand; `Target::Directory` is asked for `(OI)(CI)`.
    let wanted = |ace: &ACCESS_ALLOWED_ACE| {
        ace.Header.AceType == ACCESS_ALLOWED_ACE_TYPE && access_matches(ace, access, target)
    };
    any_ace_in(path, &wanted, sid)
}

/// Whether `path`'s DACL carries an entry for `sid` whatever its mask.
fn acl_names(path: &Path, sid: PSID) -> Result<bool> {
    any_ace_in(
        path,
        &|ace: &ACCESS_ALLOWED_ACE| ace.Header.AceType == ACCESS_ALLOWED_ACE_TYPE,
        sid,
    )
}

/// Run `wanted` over `path`'s access-allowed ACEs and answer whether any names
/// `sid`.
///
/// **The DACL is the boundary of the `unsafe`**: `descriptor` is one
/// `LocalFree`-owned allocation, released once here, and the ACL pointer is a
/// borrow *into* it and is **never** freed on its own. `AceCount` is what this
/// asks; the SIDs are recognized by [`EqualSid`] rather than by any rendering of
/// them.
fn any_ace_in(
    path: &Path,
    wanted: &dyn Fn(&ACCESS_ALLOWED_ACE) -> bool,
    sid: PSID,
) -> Result<bool> {
    let Some((dacl, descriptor)) = read_dacl(path)? else {
        return Ok(false);
    };
    let mut info = ACL_SIZE_INFORMATION::default();
    // SAFETY: `dacl` is the ACL the descriptor above owns and stays valid for
    // this call; `info` is the out-parameter the class calls for.
    let counted = unsafe {
        GetAclInformation(
            dacl,
            (&raw mut info).cast(),
            size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    };
    if counted == 0 {
        // SAFETY: the descriptor is freed once, and this is the last use of the
        // ACL: "a DACL whose size could not be read has no entries to report".
        unsafe { LocalFree(descriptor) };
        return Ok(false);
    }
    let mut found = false;
    for index in 0..info.AceCount {
        let mut ace: *mut c_void = null_mut();
        // SAFETY: `dacl` is alive and `index` is below the count just read;
        // `ace` is a valid out-parameter.
        let read = unsafe { GetAce(dacl, index, &mut ace) };
        if read == 0 || ace.is_null() {
            continue;
        }
        // SAFETY: `GetAce` just returned a pointer to this ACE, which the ACL
        // owns, so its header is readable and the pointer is aligned.
        let header = unsafe { &*ace.cast::<ACE_HEADER>() };
        // The type test comes first, and the ACE is only ever read as an
        // `ACCESS_ALLOWED_ACE` once it passed: the two structures share their
        // first eight bytes (header and mask), and a predicate that read `SidStart`
        // off an ACE of any other type would be reading a different layout.
        if header.AceType != ACCESS_ALLOWED_ACE_TYPE {
            continue;
        }
        // SAFETY: the type above is this structure's, and the pointer is the
        // ACE `GetAce` handed back.
        let allowed = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
        // SAFETY: `ace_sid`'s contract — an access-allowed ACE in a live
        // descriptor — is met by the two lines above, and `sid` is the caller's
        // own initialized SID.
        if wanted(allowed) && unsafe { EqualSid(ace_sid(ace), sid) } != 0 {
            found = true;
            break;
        }
    }
    // SAFETY: the descriptor is released exactly once, here, and the ACL pointer
    // borrowed from it is not freed separately.
    unsafe { LocalFree(descriptor) };
    Ok(found)
}

/// `path`'s DACL, with the descriptor that owns it — or `None` when the kernel
/// reports no DACL at all.
///
/// **A null DACL means "no ACE can be named", not "the path could not be read"**:
/// `GetNamedSecurityInfoW` reports success and leaves the pointer null for an
/// object carrying no discretionary ACL, and answering `None` there is the same
/// "nothing granted" a listing of it would produce. A failure to *read* is an
/// error and travels that way, exactly as it did through the tool.
fn read_dacl(path: &Path) -> Result<Option<(*mut ACL, PSECURITY_DESCRIPTOR)>> {
    let name = crate::wide_os(path.as_os_str());
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
            // **The string names the tool the skip's history is written in, and
            // the test suite asserts on it** (`tests/acl.rs`'s "a path `icacls`
            // cannot read is an error"): a Win32 failure is reported through the
            // same variant and the same step name as the tool's non-zero exit, so
            // a caller sees one read failure rather than two shapes of one.
            "icacls (read the DACL)",
            path.to_path_buf(),
            io::Error::from_raw_os_error(code as i32),
        ));
    }
    if dacl.is_null() {
        // The descriptor came back without a DACL: nothing to walk, and its own
        // allocation is still this function's to release.
        if !descriptor.is_null() {
            // SAFETY: the descriptor is this call's own allocation, it is not
            // kept, and nothing else refers to it.
            unsafe { LocalFree(descriptor) };
        }
        return Ok(None);
    }
    Ok(Some((dacl, descriptor)))
}

/// Whether `path`'s DACL already carries the grant [`grant_to`] would write.
///
/// The oracle is [`acl_grants`]'s — `GetNamedSecurityInfoW` and `EqualSid` — read
/// for the **mask** rather than for the SID alone, because that is what decides
/// whether the write is needed: an entry that already carries `M` satisfies a
/// read-only request and `F` satisfies both, while an entry left by an earlier
/// read-only grant does **not** satisfy a read-write one. A `Target::Directory`
/// entry must still carry `(OI)(CI)`, because an inherited grant that lost its
/// flags reaches no descendant and is not the grant the caller asked for.
///
/// **The comparison is binary and that is the point of it**: a well-known SID is
/// rendered by name wherever the tool prints it and by nothing the kernel
/// compares, so the question this answers — "is this SID in the DACL with enough
/// access" — has an answer that no locale, no display name and no unresolvable
/// SID can change. [`has_ace`]'s doc carries the measurement that made the text
/// route untenable.
///
/// **A wildcard path has no answer here, and that is a limitation of this read
/// rather than of its caller.** [`Grants::apply_siblings`] grants through
/// `<program directory>\*.dll` — the one `name` pattern `icacls` takes — and
/// `GetNamedSecurityInfoW` resolves no pattern: measured on this host 2026-09-25,
/// the literal path with the `*` in it fails `123` `ERROR_INVALID_NAME`, which is
/// where `tests/acl.rs`'s sibling batch caught this. Answering `false` there
/// would be a guess dressed as a decision, because the ACE lives on each matched
/// file and not on the pattern; the caller is told instead that this path has no
/// answer and writes. **That is also what the pre-API route did on this path in
/// practice**: its literal-SID search never matched `S-1-15-2-1` (see
/// [`sid_spellings`]), so the sibling batch has always written.
///
/// # Errors
///
/// [`Error::Launch`] when the DACL cannot be read, exactly as [`has_ace`]:
/// answering "nothing granted" for a path that could not be read would skip a
/// grant that is needed. A pattern is not one of those failures — it is
/// [`GrantOutcome::Unanswerable`], which is an answer.
fn already_granted(
    path: &Path,
    sid: &str,
    target: Target,
    access: AccessMode,
) -> Result<GrantOutcome> {
    if names_a_pattern(path) {
        return Ok(GrantOutcome::Unanswerable);
    }
    let sid = OwnedSid::parse(sid, path)?;
    Ok(if acl_grants(path, sid.raw(), target, access)? {
        GrantOutcome::AlreadyGranted
    } else {
        GrantOutcome::Missing
    })
}

/// What a read of a path's DACL says about the grant [`grant_to`] would write.
///
/// **Three answers rather than two, because a wildcard path is a third state and
/// not a guess.** [`Self::Unanswerable`] is [`SiblingScan::pattern`]'s `*.dll`,
/// which no DACL read can decide; it makes the caller write, and that is the safe
/// direction — a write that was not needed costs the descriptor propagation, while
/// a skip that was not due leaves a confined child without the access it was
/// launched for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrantOutcome {
    /// The DACL carries the grant.
    AlreadyGranted,
    /// The DACL was read and does not carry it.
    Missing,
    /// The path is a tool pattern, which no DACL read can answer.
    Unanswerable,
}

/// Whether `path`'s final component carries a wildcard — what
/// [`SiblingScan::pattern`] builds, and what `GetNamedSecurityInfoW` has no
/// equivalent for.
///
/// **Both pattern characters, because both of them are patterns to this tool.**
/// `icacls` expands `*` and `?` alike — measured on this host 2026-09-25:
/// `icacls <dir>\?.dll` lists `a.dll`, one character matched, exactly as the `*`
/// form does — and neither character can be part of a real Win32 name, so the
/// two together are the whole test rather than the `*` alone. The final component
/// is what is tested because that is where a pattern is spelled; a path whose
/// *directories* carry a wildcard is not a form this crate builds and would fail
/// the read on its own.
fn names_a_pattern(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().contains(['*', '?']))
}

/// A SID this module allocated and therefore has to release.
///
/// **The value is the ownership**: `ConvertStringSidToSidW` allocates, `LocalFree`
/// is its documented release, and `Drop` is where that happens — so an early
/// return cannot leak one, and no call site derives the raw SID a second time.
/// It exists only for the [`ALL_APPLICATION_PACKAGES`] route, where the SID
/// arrives as text; a [`PackageSid`] already owns its own.
struct OwnedSid(PSID);

impl OwnedSid {
    /// `text` as a SID, or [`Error::Launch`] naming `path` when it is not one.
    ///
    /// **A malformed SID is an error rather than `Ok(false)`**, the same rule the
    /// read itself answers under: `already_granted` would otherwise skip a grant
    /// it never checked. The spelling that reaches here is one of this crate's
    /// own two constants, so a failure is the API's contract breaking rather than
    /// operator input.
    fn parse(text: &str, path: &Path) -> Result<Self> {
        let wide: Vec<u16> = text.encode_utf16().chain([0]).collect();
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
    fn raw(&self) -> PSID {
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
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch), as [`grant`].
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
/// **The oracle is the kernel's DACL and not the tool's rendering of it**
/// (`GetNamedSecurityInfoW`, then `EqualSid` against every access-allowed ACE),
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
/// let mut grants = acl::Grants::new(Arc::new(sid));
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
    /// Idempotent, and free once satisfied: `grant_to` reads the DACL first and
    /// writes only what is missing, so the second run on the same root costs a
    /// read instead of a descriptor write.
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
    /// let root = Path::new(r"E:\srv\tinio");
    /// let sid = PackageSid::derive(&package_name(root)).unwrap();
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// grants
    ///     .apply(root, Target::Directory, AccessMode::ReadOnly)
    ///     .unwrap();
    /// ```
    pub fn apply(&mut self, path: &Path, target: Target, access: AccessMode) -> Result<()> {
        grant_to(path, target, access, self.sid.as_str()).map(drop)
    }

    /// Authorize `path` for [`ALL_APPLICATION_PACKAGES`] — **the one standing,
    /// machine-wide grant**, for decision 22's program directory and the `*.dll`
    /// files directly beside it.
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
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// // One ACE for `ALL APPLICATION PACKAGES`, which every container already
    /// // holds, so a new root never rewrites this directory.
    /// grants
    ///     .apply_standing(dir, Target::ProgramDirectory, AccessMode::ReadOnly)
    ///     .unwrap();
    /// ```
    pub fn apply_standing(
        &mut self,
        path: &Path,
        target: Target,
        access: AccessMode,
    ) -> Result<()> {
        let written = grant_to(path, target, access, ALL_APPLICATION_PACKAGES)?;
        // **`debug!`, because a grant is a per-ACE event and the phase is the
        // `info!`** (P2's logging split): the phase's own line is
        // `grant_whitelist`'s, once per launch, and this one would otherwise
        // print the program directory and then every `*.dll` batch into the
        // operator's log on every start. `written` still travels with it, so a
        // reader at `debug` sees the skip as plainly as the write.
        tracing::debug!(
            step = "grant the ACLs",
            path = %path.display(),
            sid = ALL_APPLICATION_PACKAGES,
            standing = true,
            written
        );
        Ok(())
    }

    /// Authorize [`scan`](SiblingScan)'s files for [`ALL_APPLICATION_PACKAGES`] in
    /// **one `icacls` invocation** — decision 22's sibling-DLL half (Ruling 78),
    /// and a standing grant like the directory those files sit in.
    ///
    /// **The batch is the tool's wildcard rather than a list of paths**: `icacls`
    /// accepts a single `name` pattern and no second path (measured 2026-09-21 on
    /// this host; the measurement and its refused forms are on
    /// [`SiblingScan::pattern`]). It reaches [`Self::apply_standing`], so the SID
    /// is the one every container already holds and the write is skipped once the
    /// pattern's ACE is there — which, for a program directory that does not
    /// change between roots, is every run after the first.
    ///
    /// **The scan is a parameter and not a `&Path`**, which is the whole reason
    /// [`SiblingScan`] exists: this call and the caller's own exposure set must
    /// be about one file set, and a `&Path` here would re-enumerate the directory
    /// — a second answer to a question already answered, and a second chance for
    /// the grant to name a DLL the exposure set does not.
    ///
    /// The `bool` is whether a sibling existed at all: `false` means the pattern
    /// was never handed to the tool.
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
    ///     acl,
    ///     backend::SiblingScan,
    ///     sid::{PackageSid, package_name},
    /// };
    ///
    /// let dir = Path::new(r"E:\build");
    /// let sid = PackageSid::derive(&package_name(dir)).unwrap();
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// // `false` when the directory holds no DLL: nothing granted.
    /// let granted = grants
    ///     .apply_siblings(&SiblingScan::scan(dir), AccessMode::ReadOnly)
    ///     .unwrap();
    /// # let _ = granted;
    /// ```
    pub fn apply_siblings(&mut self, scan: &SiblingScan, access: AccessMode) -> Result<bool> {
        match scan.pattern() {
            Some(pattern) => {
                self.apply_standing(&pattern, Target::File, access)?;
                Ok(true)
            }
            None => Ok(false),
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
    use std::{fs, path::Path, process::Command};

    // The three names only a measurement needs: `(F)`'s stored mask, the bit
    // that marks an inherited entry, and the generic bit the read must *not*
    // treat as one (they are imported here rather than above because the
    // production read never names them).
    use windows_sys::Win32::{
        Foundation::GENERIC_ALL, Security::INHERITED_ACE, Storage::FileSystem::FILE_ALL_ACCESS,
    };

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

    /// **`GENERIC_ALL` on its own is not the container**, and the disjunct that
    /// treated it as one was a false skip: bit 28 is a *request* bit that an
    /// access check expands into nothing, so an ACE whose mask carries only it
    /// grants no specific right to anyone. Every ACE `icacls` writes carries the
    /// specific bits as well — `(M)` is `0x0013_01BF` and `(RX)` is `0x0012_00A9`
    /// — which is why the disjunct was redundant where it was not wrong, and why
    /// removing it costs the skip nothing.
    #[test]
    fn a_mask_carrying_only_the_generic_bit_does_not_satisfy_a_grant() {
        // The shape `any_ace_in` reads an ACE as: a header, a mask, the SID.
        let aced = |flags: u8, mask: u32| ACCESS_ALLOWED_ACE {
            Header: ACE_HEADER {
                AceType: ACCESS_ALLOWED_ACE_TYPE,
                AceFlags: flags,
                AceSize: 0,
            },
            Mask: mask,
            SidStart: 0,
        };
        let both = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
        for access in [AccessMode::ReadOnly, AccessMode::ReadWrite] {
            assert!(
                !access_matches(&aced(both, GENERIC_ALL), access, Target::Directory),
                "`GENERIC_ALL` alone carries no specific right, so it satisfies \
                 no grant however it is asked ({access:?})"
            );
        }
        // The control: the mask the generic bit *expands to* is the container,
        // and containment answers it for both accesses.
        assert!(access_matches(
            &aced(both, FILE_ALL_ACCESS),
            AccessMode::ReadOnly,
            Target::Directory
        ));
        assert!(access_matches(
            &aced(both, FILE_ALL_ACCESS),
            AccessMode::ReadWrite,
            Target::Directory
        ));
    }

    /// The masks of a path's access-allowed ACEs, in the DACL's own order.
    ///
    /// **`explicit` decides whether the entries the tool did not write are
    /// counted, and the mask measurement below needs "no".** A `%TEMP%`
    /// directory inherits `(OI)(CI)(RX)` for `BUILTIN\Users` from the volume's
    /// own DACL — the same SID those grants name — so a reading of every entry
    /// answers "RX is there" for a directory where nothing was granted at all:
    /// the false pass this parameter removes. `INHERITED_ACE` is the flag such
    /// an entry carries and the one the tool's own entry does not.
    ///
    /// A test read and not the production one: `acl_grants` asks the kernel the
    /// same question through `any_ace_in`, and this exists so a measurement of
    /// what `icacls` *stored* can be asserted without a second `PackageSid`.
    fn masks(path: &Path, explicit: bool) -> Vec<u32> {
        let mut found = Vec::new();
        let (dacl, descriptor) = read_dacl(path).expect("read").expect("a DACL");
        let mut info = ACL_SIZE_INFORMATION::default();
        // SAFETY: `dacl` is the ACL `read_dacl` returned and is alive until the
        // free below; `info` is its out-parameter.
        let counted = unsafe {
            GetAclInformation(
                dacl,
                (&raw mut info).cast(),
                size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        };
        assert_ne!(counted, 0);
        for index in 0..info.AceCount {
            let mut ace: *mut c_void = null_mut();
            // SAFETY: `dacl` is alive and `index` is below its own count.
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
                continue;
            }
            // SAFETY: `GetAce` returned this ACE inside the live ACL, and its
            // type is tested before the mask is read off it.
            let header = unsafe { &*ace.cast::<ACE_HEADER>() };
            if header.AceType == ACCESS_ALLOWED_ACE_TYPE
                && !(explicit && u32::from(header.AceFlags) & INHERITED_ACE != 0)
            {
                // SAFETY: the type above is this structure's.
                found.push(unsafe { (&*ace.cast::<ACCESS_ALLOWED_ACE>()).Mask });
            }
        }
        // SAFETY: the descriptor is this test's own read, freed once here; the
        // ACL inside it is not freed on its own.
        unsafe { LocalFree(descriptor) };
        found
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
        let untouched = dir.path().join("untouched");
        fs::create_dir_all(&untouched).expect("create");
        assert!(
            masks(&untouched, false).contains(&RX),
            "this host's temp volume puts no inherited (RX) entry on a new \
directory, so the false pass this control is about is not exercised here: {:08X?}",
            masks(&untouched, false)
        );
        assert!(
            !masks(&untouched, true).contains(&RX),
            "nothing has been granted here, so an oracle that answered RX from \
an inherited entry is answering for the wrong reason: {:08X?}",
            masks(&untouched, true)
        );

        let users = "S-1-5-32-545";
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
                masks(&path, true).contains(&wanted),
                "icacls stored something other than 0x{wanted:08X} for {spec}: {:08X?}",
                masks(&path, true)
            );
        }
    }

    /// The entry lines of an `icacls` listing, with the path the tool prefixes
    /// the first one with and its own count line dropped.
    ///
    /// **The path has to come off, and a test that forgot it is how this helper
    /// exists**: a temporary directory's own name can contain something that looks
    /// like a SID or a permission, so a search of the whole listing for a SID can
    /// match the *path* rather than an ACE.
    fn ace_lines(listing: &str) -> Vec<&str> {
        listing
            .lines()
            .filter(|line| line.contains(':') && !line.contains("Successfully processed"))
            .collect()
    }

    /// The old read's question, asked the way the old read asked it: does an
    /// *entry* line of the listing carry the literal SID?
    fn text_mentions_sid_among_the_entries(listing: &str, sid: &str) -> bool {
        ace_lines(listing).iter().any(|line| line.contains(sid))
    }

    /// [`already_granted`] as the yes/no question the skip asks it.
    fn satisfied(path: &Path, target: Target, access: AccessMode) -> bool {
        already_granted(path, ALL_APPLICATION_PACKAGES, target, access).expect("read the DACL")
            == GrantOutcome::AlreadyGranted
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
        let listing = checked("icacls (read the DACL)", dir.path(), &[]).expect("read");
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

    /// **The skip the whole read exists for, against a real DACL**: the same call
    /// twice writes the first time and skips the second, and both halves are
    /// asserted against the kernel's DACL rather than against the tool's text.
    ///
    /// The second call is the one that matters. A read that answered "not
    /// granted" would rewrite the descriptor every run, which on a directory is
    /// `SetNamedSecurityInfoW` propagating over the whole subtree — the cost the
    /// skip exists to remove, and the cost the text oracle silently paid for a
    /// well-known SID.
    #[test]
    fn a_satisfied_grant_is_not_written_twice() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();

        assert!(
            !satisfied(path, Target::Directory, AccessMode::ReadOnly),
            "nothing granted yet, so the write is due"
        );
        assert!(
            grant_to(
                path,
                Target::Directory,
                AccessMode::ReadOnly,
                ALL_APPLICATION_PACKAGES
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
                ALL_APPLICATION_PACKAGES
            )
            .expect("grant"),
            "the second call is a read and nothing else"
        );

        // **And the mask is read as well as the SID**: `RX` does not satisfy a
        // read-write request, while the `M` the next call writes satisfies both,
        // which is what `already_granted`'s doc states and what the old text
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
                ALL_APPLICATION_PACKAGES
            )
            .expect("grant"),
            "the wider grant is written"
        );
        assert!(
            satisfied(path, Target::Directory, AccessMode::ReadOnly),
            "`M` satisfies a read-only request"
        );
        // **The flags are read as well as the mask**: a file-shaped question asks
        // for none, so the same `(OI)(CI)M` entry answers it too — which is what
        // `Target` being the caller's means, and what
        // `the_flags_are_the_difference_between_the_two_directory_questions` pins
        // the other way round.
        assert!(
            satisfied(path, Target::File, AccessMode::ReadOnly),
            "the file-shaped question asks for no flags, and this entry carries a wide mask"
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

    /// **The sibling batch's path has no DACL to read, and the answer says so
    /// rather than guessing.**
    ///
    /// `GetNamedSecurityInfoW` resolves no wildcard — the literal `…\*.dll` fails
    /// `123 ERROR_INVALID_NAME` on this host (measured 2026-09-25) — so the read
    /// the batch's skip rests on cannot decide it. What must *not* happen is the
    /// failure travelling: `apply_siblings` is a grant, and a batch that reported
    /// an error would leave a confined child without the DLLs its own loader
    /// reads. The skip's answer is therefore [`GrantOutcome::Unanswerable`] and
    /// the write follows, which is what the text route did on this path in
    /// practice.
    #[test]
    fn a_wildcard_path_is_unanswerable_rather_than_an_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let pattern = dir.path().join("*.dll");
        assert!(names_a_pattern(&pattern));
        assert!(
            already_granted(
                &pattern,
                ALL_APPLICATION_PACKAGES,
                Target::File,
                AccessMode::ReadOnly
            )
            .expect("a pattern is an answer and not a failure")
                == GrantOutcome::Unanswerable,
            "the read has no DACL to ask about a wildcard path"
        );
        // **The guard is a wildcard in the file name and nothing else.** `?` is a
        // pattern to this tool exactly as `*` is (measured 2026-09-25:
        // `icacls <dir>\?.dll` lists `a.dll`), so a name carrying one has no DACL
        // to read either; a missing concrete path stays the error it always was
        // rather than becoming "unanswerable".
        assert!(!names_a_pattern(dir.path()));
        assert!(names_a_pattern(&dir.path().join("what?.txt")));
        assert!(
            already_granted(
                &dir.path().join("never-created.dll"),
                ALL_APPLICATION_PACKAGES,
                Target::File,
                AccessMode::ReadOnly
            )
            .is_err(),
            "a concrete path that does not exist is still a failed read"
        );
    }

    /// **The orphan shape**: an ACE that carries `(OI)(CI)` and `INHERIT_ONLY` at
    /// once reaches the children and **not** the object it sits on, so it is not
    /// the grant a directory's own entry is — the reason `icacls`'s listing writes
    /// it `(IO)` beside the flags.
    ///
    /// The mask and the flags this tests are built rather than read, because the
    /// filesystem is what removes `(IO)` on the way down: a child's inherited copy
    /// is effective there, so a real DACL cannot hold the combination on the
    /// directory the question is asked about.
    #[test]
    fn an_inherit_only_entry_is_not_the_grant_it_resembles() {
        let ace = |flags: u8| ACCESS_ALLOWED_ACE {
            Header: ACE_HEADER {
                AceType: ACCESS_ALLOWED_ACE_TYPE,
                AceFlags: flags,
                AceSize: size_of::<ACCESS_ALLOWED_ACE>() as u16,
            },
            Mask: FILE_GENERIC_READ | FILE_GENERIC_EXECUTE,
            SidStart: 0,
        };
        let both = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
        assert!(
            access_matches(&ace(both), AccessMode::ReadOnly, Target::Directory),
            "`(OI)(CI)RX` on a directory is the grant"
        );
        assert!(
            !access_matches(
                &ace(both | INHERIT_ONLY_ACE as u8),
                AccessMode::ReadOnly,
                Target::Directory
            ),
            "the same ACE with `(IO)` is not effective on this object, so it is not its grant"
        );
        assert!(
            !access_matches(&ace(0), AccessMode::ReadOnly, Target::Directory),
            "and an effective entry without the flags does not reach the descendants"
        );
        assert!(
            access_matches(&ace(0), AccessMode::ReadOnly, Target::File),
            "the file-shaped question asks for no flags"
        );
    }

    /// **The flags are read as well as the mask, and they only ever narrow a
    /// directory's question.**
    ///
    /// A directory grant is the inheritable one: an entry that lost `(OI)(CI)`
    /// reaches no descendant and is not the grant the caller asked for, while the
    /// file-shaped question asks for no flags at all. Both halves are read off a
    /// real DACL rather than off a synthetic ACE, because the flags travel through
    /// the filesystem and the tool on the way in.
    #[test]
    fn the_flags_are_the_difference_between_the_two_directory_questions() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");
        let asked = |target| acl_grants(path, sid.raw(), target, AccessMode::ReadOnly);

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

    /// A path the kernel cannot read is an error and not "nothing granted", the
    /// rule the suites that consult this as an oracle rest on.
    #[test]
    fn the_read_refuses_a_path_it_cannot_see() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("never-created");
        let error = already_granted(
            &missing,
            ALL_APPLICATION_PACKAGES,
            Target::Directory,
            AccessMode::ReadOnly,
        )
        .expect_err("a path that does not exist is an error, not a satisfied grant");
        let text = error.to_string();
        assert!(text.contains("never-created"), "{text}");
        assert!(
            text.contains("icacls"),
            "the read failure names the step the skip's history is written under: {text}"
        );
    }
}
