//! ACL whitelist: grant the package SID an access mode on each exposed path, and
//! leave those grants standing — the launcher takes nothing back on the way out
//! (decisions 15, 21, §6).
//!
//! **A grant is written on a handle this module opened itself, so the object the
//! check approved is the object that is granted.** `icacls /grant:r` wrote until
//! 2026-09-27, and the defect that ended it is the check-then-use between the
//! whitelist's reparse refusal and the tool's own re-resolution of the name it was
//! handed: a writer who replaced an exposed directory with a junction inside
//! the grant window got the ACE written somewhere the check never saw. [`Object`]
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
//! children, which is what `tests/acl.rs` asserts against the tool as an
//! independent oracle. [`Error::Launch`] carries the API's own status for a write
//! or an open that failed, and a failed read carries it too.
//!
//! **The read half is the Win32 API because the tool's *text* cannot answer the
//! question the read exists for.** The read decides whether the write may be
//! skipped, and `icacls` renders a well-known SID by **name**: an ACE written for
//! [`ALL_APPLICATION_PACKAGES`] comes back as
//! `APPLICATION PACKAGE AUTHORITY\ALL APPLICATION PACKAGES:(RX)`, so a check for
//! the literal `S-1-15-2-1` never saw the ACE it had just written and the skip
//! never fired — which is not a cosmetic failure, because the write it failed to
//! skip rewrites the descriptor and propagates eagerly over the whole subtree
//! (10–30 s for this workspace's `target\debug`; measured 2026-09-25). Two
//! spellings patched that symptom; [`has_ace`] ends the class, because the
//! entries are compared with `EqualSid` — as the kernel compares them, in the
//! walk [`tinio_winsec`] owns — and no rendering takes part. [`mentions_sid`]
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
//! are [`tinio_winsec::walk_dacl`]'s, which is where this crate's reads and that
//! crate's current-user verdict share one traversal; what this crate hands it is a
//! predicate and the SID to compare against. Every allocation is `LocalFree`d
//! once, by the function that made it; the ACL inside a descriptor belongs to the
//! descriptor and may **not** be freed on its own. Decision 14 enumerates the
//! sites.
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
//!
//! [`Error::Launch`]: crate::_core::Error::Launch

// The manifest denies `unsafe_code` and the exception sits on the module that
// needs it, as decision 14 asks: every Win32 call below is the DACL layer's own —
// the descriptor read, the handle the grant acts on, the merge and the
// descriptor's release — and a crate-level allow would lift the lint for every
// module instead. **The ACE walk is not among the sites this permit covers any
// more**: it is `tinio-winsec`'s [`walk_dacl`], whose own module carries that
// crate's exemption and states its reason.
#![allow(unsafe_code)]

use std::{
    ffi::OsStr,
    io,
    ops::ControlFlow,
    path::{Path, PathBuf},
    process::{Command, Output},
    ptr::{from_mut, null, null_mut},
    sync::Arc,
};

use tinio_winsec::{Ace, to_wide, walk_dacl};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, LocalFree},
    Security::{
        ACCESS_ALLOWED_ACE, ACL,
        Authorization::{
            ConvertStringSidToSidW, EXPLICIT_ACCESS_W, GetNamedSecurityInfoW, GetSecurityInfo,
            SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW, SetSecurityInfo, TRUSTEE_IS_SID,
            TRUSTEE_IS_USER, TRUSTEE_W,
        },
        CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, INHERIT_ONLY_ACE, NO_INHERITANCE,
        OBJECT_INHERIT_ACE, PSECURITY_DESCRIPTOR, PSID,
    },
    Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle, OPEN_EXISTING, READ_CONTROL,
        WRITE_DAC,
    },
};

use crate::{
    _core::{AccessMode, Error, Result, launch},
    backend::SiblingScan,
    handles::{self, system_directory},
    sid::{ALL_APPLICATION_PACKAGES, PackageSid},
};

/// `DELETE` (`0x0001_0000`), which `icacls` folds into `(M)` and not into `(RX)`;
/// `windows-sys` exports it as a generic-mapping bit only.
const DELETE: u32 = 0x0001_0000;

/// `icacls`'s `(M)` — the alias [`ace_spec`] spells and the mask
/// [`access_matches`] compares: `FILE_GENERIC_READ | FILE_GENERIC_WRITE |
/// FILE_GENERIC_EXECUTE | DELETE` (`0x0013_01BF`).
const MODIFY: u32 = FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE;

/// `icacls`'s `(RX)` — the alias [`ace_spec`] spells and the mask
/// [`access_matches`] compares: the read and execute halves (`0x0012_00A9`).
const READ_AND_EXECUTE: u32 = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE;

/// **The sentence a reparse point is refused with, and the one place it is
/// spelled.** Three sites meet one: the whitelist's `refuse_reparse_points` — a
/// link anywhere in a *spelled* path, which is where the ACE would land on the
/// link's target — its `refuse_a_reparse_sibling`, and [`Object::verify`], the
/// handle check every grant now passes.
///
/// **What it states is what a grant does through a link, and that is not one
/// thing — the wording says so because the measurement did** (this host,
/// 2026-09-27): `icacls /grant:r` on a path with a junction **above** the last
/// component resolves it and writes the ACE on the target, outside the exposed
/// tree (`…\hop\tinio` wrote `…\real\tinio`'s descriptor, and the pre-existing
/// file beneath it inherited the ACE); on a path whose **last** component is a
/// junction the ACE lands on the link's own object rather than on the target; on a
/// **file symlink** it lands on the target — the sibling-DLL escape the name-based
/// enumeration closes. None of the three is the ACE the caller asked for, and two
/// of them can be outside the exposure.
pub(crate) const REPARSE_POINT: &str = "is a reparse point (a junction or a symlink), and a grant through it lands on the link's own object or on whatever it resolves to — never on the path the caller named, and not necessarily inside the exposed tree";

/// **The refusal a grant's shape earns on an object of the wrong kind** — the
/// opened handle's own answer, and the replacement for the one failure `icacls`
/// reported as success: an inheritance-flagged ACE on a file is a silent no-op
/// (design §6's evidence row), and a plain ACE aimed at a directory leaves every
/// descendant ungranted, so a shape that disagrees with the object is refused
/// rather than written.
const NOT_THE_GRANTED_KIND: &str = "is not the kind of object this grant's shape addresses: an inherited (OI)(CI) grant and the program directory's plain grant need a directory, and a file grant needs one that is not a directory — on the wrong kind the ACE is a silent no-op when a tool writes it and reaches nothing when this crate does";

/// The refusal a reparse point earns: **the event and the error, built in one
/// place** because the sites that meet one are three — the whitelist's walk over
/// a spelled path's components, its sibling check, and the grant's own handle
/// check — and one operator rule should read one way.
///
/// **The step is the caller's**, because the sites fail at different stages of a
/// launch while the *finding* is one: `step` names where in the launch the refusal
/// happened, and [`REPARSE_POINT`] is what happened.
///
/// **Hand-written for the same reason the caller's-file refusal is**: the
/// `Error::BadPath` below carries the path as its field, and `fail`/`refused`
/// cannot emit that — the error has to exist before `refused` could be handed it,
/// and `fail` would either drop the path or turn this into a `Launch`. The event
/// carries the core's fields — `step`, `path`, `error` — and is a security
/// decision, so an operator has to see it in the log rather than infer it from a
/// startup failure (shared §7's rule).
pub(crate) fn reparse_refusal(path: &Path, step: &'static str) -> Error {
    let error = Error::BadPath {
        path: path.to_path_buf(),
        reason: REPARSE_POINT,
    };
    tracing::error!(
        step,
        path = %path.display(),
        error = %error,
        "sandbox step failed"
    );
    error
}

/// The refusal a shape that disagrees with the object earns, in
/// [`reparse_refusal`]'s shape and for its reason: **an `Error::BadPath` with the
/// path as its field**, emitted where it is built.
fn kind_refusal(path: &Path) -> Error {
    let error = Error::BadPath {
        path: path.to_path_buf(),
        reason: NOT_THE_GRANTED_KIND,
    };
    tracing::error!(
        step = "verify the granted path",
        path = %path.display(),
        error = %error,
        "sandbox step failed"
    );
    error
}

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
/// no-op** when `icacls` writes it (exit 0, nothing written) and an ACE nothing
/// inherits from when the API does, while the file shape sent to a directory
/// leaves every descendant ungranted. *Whether it propagates* is a policy:
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
    /// [`Grants::apply_siblings`] passes the literal for each enumerated `*.dll`.
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
/// from both of the others.
///
/// **The string and the numbers a grant is actually written with are one table
/// read twice**: [`ace_bits`] is the mask and the inheritance flags
/// `SetSecurityInfo` takes, this is the tool's spelling of the same pair, and
/// `the_aliases_icacls_writes_are_the_masks_this_read_compares` pins the two
/// against each other on a real DACL.
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

/// The same table as [`ace_spec`], in the two numbers the write takes: **the
/// access mask and the inheritance flags an `EXPLICIT_ACCESS_W` carries**, which
/// is what `grfAccessPermissions` and `grfInheritance` are set from.
///
/// **The numbers are the aliases' own, not a second opinion about them**: `M` is
/// `MODIFY` (`0x0013_01BF`) and `RX` is `READ_AND_EXECUTE` (`0x0012_00A9`), the
/// two constants the read compares against, and the flags are
/// `OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE` for the one shape that
/// propagates — `NO_INHERITANCE` for the two that do not, which is why a file
/// grant never carries `(OI)(CI)` even though passing it would compile.
///
/// **The pair is `const` and private, and the table is small on purpose**: it is
/// the whole of what `SET_ACCESS` is told, and every other part of the ACE — the
/// SID, the type, the order the entry lands in — is the kernel's
/// (`SetEntriesInAclW` inserts a new allow ACE ahead of the existing allows, which
/// is the order `icacls` produced and the reason its alias list reads the same).
const fn ace_bits(target: Target, access: AccessMode) -> (u32, u32) {
    let mask = match access {
        AccessMode::ReadOnly => READ_AND_EXECUTE,
        AccessMode::ReadWrite => MODIFY,
    };
    let inheritance = match target {
        Target::Directory => OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
        Target::ProgramDirectory | Target::File => NO_INHERITANCE,
    };
    (mask, inheritance)
}

/// The `*<sid>:<perm>` argument `icacls` is given for `target`.
///
/// The leading `*` is what stops the tool resolving the string as an account
/// name: an AppContainer package SID has no account behind it, and a name form
/// would be looked up in the local accounts and fail — or, worse, be answered by
/// an account that happens to carry the name.
///
/// **The launcher no longer hands this to the tool — the tool is what an
/// operator, a fixture and [`revoke`] still use** — so what the function is for
/// now is that one table describes both writers: [`ace_bits`] is what
/// `SetSecurityInfo` is told and this is the same pair in the tool's own spelling,
/// which is what `tests/acl.rs` and `tinio-server`'s Windows fixtures need to
/// write an ACE the launcher's read will recognize.
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
/// being claimed. What the rule buys is that the tool this crate shells out to is
/// the operating system's own even when `%SystemRoot%` is unset, stale or wrong.
/// The module's rule — the process that decides this run's DACLs is not selectable
/// through the environment — is what this function makes true of the code rather
/// than an intention stated beside it.
///
/// **The grant no longer goes through the tool** ([`Object`] writes on a handle);
/// what still does is [`revoke`], the operator's own primitive, and every suite
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
/// **The path is opened, not resolved**: see [`Object`] for the handle the read and
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
struct GrantSid<'a> {
    /// The resolved SID, as every Win32 call takes it.
    raw: PSID,
    /// The rendered form, for the event a run leaves behind.
    text: &'a str,
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
struct Object {
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
    fn open(path: &Path) -> Result<Self> {
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
    fn open_for_write(path: &Path) -> Result<Self> {
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
    fn verify(&self, target: Target) -> Result<()> {
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
    fn granted(&self, sid: PSID, target: Target, access: AccessMode) -> Result<bool> {
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
    fn write(&self, sid: PSID, target: Target, access: AccessMode) -> Result<()> {
        let Some((old, descriptor)) = self.read_dacl()? else {
            // A null DACL is every access to every trustee; there is no ACL to merge
            // into, and `SetEntriesInAclW`'s `oldacl` argument may be null for
            // exactly that case ("no existing ACL"), so the merge starts from empty.
            return self.merge_and_set(sid, target, access, null());
        };
        let result = self.merge_and_set(sid, target, access, old);
        // SAFETY: the descriptor is this call's own read, released once here; the
        // ACL pointer borrowed from it is not freed separately.
        unsafe { LocalFree(descriptor) };
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
/// **The event is the phase's own step name**, because that is what this is: one
/// ACE of the `grant the ACLs` phase, at `debug!` with `written` telling the skip
/// from the write (shared §7's per-operation event).
///
/// **The SID arrives as a [`GrantSid`] and is never re-parsed from its text**: the
/// read and the write both compare and store the value the caller already holds,
/// and the derived text only ever reaches a log line.
fn grant_to(path: &Path, target: Target, access: AccessMode, sid: GrantSid<'_>) -> Result<bool> {
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
/// the skip ask the kernel's DACL through [`walk_dacl`], which compares trustees
/// with `EqualSid`, so nothing this crate *decides* rides on how a SID is
/// rendered; what still does is every suite that asserts against the tool's own
/// output — `tests/acl.rs` pins [`ALL_APPLICATION_PACKAGES`] on the sibling batch
/// through it, and `tests/confine.rs` asks whether the program directory carries
/// the standing SID at all. Keeping one implementation of that question is what
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
#[must_use]
pub fn mentions_sid(text: &str, sid: &str) -> bool {
    text.lines().any(|line| {
        sid_spellings(sid)
            .into_iter()
            .any(|spelling| line.contains(spelling))
    })
}

/// Whether `ace` — an access-allowed entry, as [`walk_dacl`] hands one over, so
/// the type test that makes this structure its layout has already been made —
/// carries what a grant for `target`/`access` would write.
///
/// **It asks [`ace_bits`], the one table the write is made from**, which is what
/// keeps the read and the write from disagreeing: the *rule* differs — the write
/// sets exactly those bits, the read accepts any entry that **contains** them —
/// and that difference is this function's, not a second copy of the numbers. A
/// DACL may carry `R`, `F`, a wider mask or one written by another tool, and every
/// one of those that includes the request satisfies it; an equality would report a
/// path that already has more than enough as unsatisfied and rewrite its
/// descriptor, which is the cost this read exists to avoid.
///
/// **The masks' provenance is the table's** (`icacls`'s own aliases, derived from
/// the generic rights rather than transcribed) and so is the flags': a
/// `Target::Directory` entry must carry `(OI)(CI)`, because an inherited grant that
/// lost its flags reaches no descendant and is not the grant the caller asked for,
/// while the two non-propagating shapes ask for no flags at all — at least the
/// requested flags, and therefore any flags when none are requested.
///
/// **`F` needs no branch of its own**: what `icacls` stores for `(F)` is
/// `FILE_ALL_ACCESS` (`0x001F_01FF`), which carries every bit both masks have, so
/// the same containment test answers it — and the generic bit is not a shortcut to
/// that: an ACE whose mask is `GENERIC_ALL` and nothing else grants no specific
/// right at all, which is the false skip this test used to take.
/// **An `INHERIT_ONLY` ACE fails every target**: such an entry reaches nothing on
/// this object, so it is not the grant a caller asked for however wide its mask.
fn access_matches(ace: &ACCESS_ALLOWED_ACE, access: AccessMode, target: Target) -> bool {
    let flags = u32::from(ace.Header.AceFlags);
    // An inherit-only ACE is not effective on this object at all, so it cannot
    // satisfy a grant whatever its mask or flags say.
    if flags & INHERIT_ONLY_ACE != 0 {
        return false;
    }
    // **`F` is not a branch of its own, and the branch that stood here was
    // wrong.** `icacls`'s `(F)` stores `FILE_ALL_ACCESS` (`0x001F_01FF`), which
    // contains every bit the table's masks are built from, so the containment
    // below answers it; the `GENERIC_ALL` disjunct that used to sit beside it
    // fired only for a mask literally carrying bit 28 — which an access check
    // expands into *no specific right at all* — and a skip that fired there left
    // a confined child without the access it was launched for (`(F)`'s own
    // measurement, and the false skip it pins, are in `a_mask_carrying_only_the_
    // generic_bit_does_not_satisfy_a_grant`).
    let (mask, wanted) = ace_bits(target, access);
    let mask_ok = ace.Mask & mask == mask;
    let flags_ok = wanted == NO_INHERITANCE || flags & wanted == wanted;
    mask_ok && flags_ok
}

/// Whether `dacl` — one live ACL, with the descriptor that owns it still in the
/// caller's hands — holds an access-allowed entry for `sid` that `wanted` accepts.
///
/// **This crate's one question about a DACL, and its two reads differ in nothing
/// else**: [`Object::granted`] asks it of the handle the write would act on, and
/// [`any_ace_in`] of a path the test oracle named. The walk itself is
/// [`walk_dacl`]'s — [`tinio_winsec`] owns the ACE sequence, from the ACL's own
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
/// rewritten on the strength of one. That is `tinio_winsec`'s decision and its
/// reason is written beside it there — including the note that no test covers the
/// failing `GetAce`, because reaching it needs a DACL the security APIs cannot hand
/// out.
fn carries(
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
        walk_dacl(path, dacl, sid, &mut |ace| match ace {
            Ace::Named(ace) if wanted(ace) => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        })
    }
    .map_err(|source| launch("walk the DACL", path, source))?;
    Ok(verdict.is_break())
}

/// Whether `path`'s DACL carries an entry for `sid` whatever its mask.
fn acl_names(path: &Path, sid: PSID) -> Result<bool> {
    any_ace_in(path, &|_| true, sid)
}

/// Run `wanted` over `path`'s access-allowed ACEs and answer whether any names
/// `sid`.
///
/// The path-shaped read, for [`has_ace`] — the test oracle that asks about a path
/// rather than about an opened object. The grant's own read is
/// [`Object::granted`]'s, which asks the same question of the same DACL through a
/// handle the writer holds.
fn any_ace_in(
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
fn free_descriptor(descriptor: PSECURITY_DESCRIPTOR) {
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
fn read_dacl(path: &Path) -> Result<Option<(*mut ACL, PSECURITY_DESCRIPTOR)>> {
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
/// It exists only for the [`ALL_APPLICATION_PACKAGES`] route, where the SID
/// arrives as text; a [`PackageSid`] already owns its own, which is why
/// [`Grants::apply`] parses nothing and this type stays off the per-path hot
/// path. Its caller parses once per standing *set* — the program directory, or one
/// batch of sibling DLLs — rather than once per granted file.
struct OwnedSid(PSID);

impl OwnedSid {
    /// `text` as a SID, or [`Error::Launch`] naming `path` when it is not one.
    ///
    /// **A malformed SID is an error rather than a skip**: a grant made for a
    /// zero SID is an ACE for nobody that still rewrites the descriptor, and there
    /// is no SID to compare against, so the failure has to be loud. The spelling
    /// that reaches here is one of this crate's own two constants, so a failure is
    /// the API's contract breaking rather than operator input.
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
/// (`GetNamedSecurityInfoW`, then [`walk_dacl`]'s ACE-by-ACE comparison, which
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
    /// Idempotent, and free once satisfied: `grant_to` reads the DACL of the object
    /// it opened and writes only what is missing, so the second run on the same root
    /// costs one open and one read instead of a descriptor write.
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
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// grants
    ///     .apply(root, Target::Directory, AccessMode::ReadOnly)
    ///     .unwrap();
    /// ```
    pub fn apply(&mut self, path: &Path, target: Target, access: AccessMode) -> Result<()> {
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

    /// Authorize [`scan`](SiblingScan)'s files for [`ALL_APPLICATION_PACKAGES`],
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
    /// **The scan is a parameter and not a `&Path`**, which is the whole reason
    /// [`SiblingScan`] exists: this call and the caller's own exposure set must
    /// be about one file set, and a `&Path` here would re-enumerate the directory
    /// — a second answer to a question already answered, and a second chance for
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
    /// let mut grants = acl::Grants::new(Arc::new(sid));
    /// // `false` when the directory holds no DLL: nothing granted.
    /// let granted = grants
    ///     .apply_siblings(&SiblingScan::scan(dir), AccessMode::ReadOnly)
    ///     .unwrap();
    /// # let _ = granted;
    /// ```
    pub fn apply_siblings(&mut self, scan: &SiblingScan, access: AccessMode) -> Result<bool> {
        let Some(first) = scan.files().first() else {
            return Ok(false);
        };
        // One parse for the whole set: the SID is the same constant for every file,
        // and the allocation lives until the last grant of this batch.
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, first)?;
        for file in scan.files() {
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
///
/// The tool's write is [`revoke`]'s now — the grant does not go through it — and
/// the one production caller is what this function and the two above are kept for.
fn invoke(path: &Path, flags: &[&OsStr]) -> Result<Output> {
    let mut command = Command::new(icacls_binary());
    command.arg(path).args(flags);
    command
        .output()
        .map_err(|source| launch("run icacls", path.to_path_buf(), source))
}

/// The access masks this thread's opens have asked for, in order, for the test that
/// pins the grant's two-open split.
#[cfg(test)]
mod opens {
    use std::{cell::RefCell, mem};

    thread_local! {
        static OPENS: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    }

    /// Record one open's requested access.
    pub(super) fn note(access: u32) {
        OPENS.with(|opens| opens.borrow_mut().push(access));
    }

    /// The masks since the last call, and empty from here on.
    pub(super) fn take() -> Vec<u32> {
        OPENS.with(|opens| mem::take(&mut *opens.borrow_mut()))
    }
}

/// The descriptor writes this thread has made, for the tests that tell a skip from
/// a write that happened to leave the same ACE.
///
/// **A thread-local in a `cfg(test)` module of its own**, because it exists for
/// one test's question and because the counter's `Cell` is a path this module has
/// no other use for: the crate lints `clippy::absolute_paths`, so a `std::cell::Cell`
/// spelled inline in a `thread_local!` is a warning rather than a device.
#[cfg(test)]
mod writes {
    use std::cell::Cell;

    thread_local! {
        static DACL_WRITES: Cell<u32> = const { Cell::new(0) };
    }

    /// Count one `SetSecurityInfo`.
    pub(super) fn note() {
        DACL_WRITES.with(|writes| writes.set(writes.get().saturating_add(1)));
    }

    /// The count since the last call, and zero from here on.
    pub(super) fn take() -> u32 {
        DACL_WRITES.with(|writes| writes.replace(0))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, mem::size_of, path::Path, process::Command, sync::Arc};

    // The names only a measurement needs: the ACE type a fixture builds, a `(F)`
    // mask, the bit that marks an inherited entry, and the generic bit the read
    // must *not* treat as one (they are imported here rather than above because
    // the production read never names them).
    use tinio_winsec::ACCESS_ALLOWED_ACE_TYPE;
    use windows_sys::Win32::{
        Foundation::GENERIC_ALL,
        Security::{ACE_HEADER, GetSecurityDescriptorControl, INHERITED_ACE},
        Storage::FileSystem::FILE_ALL_ACCESS,
    };

    use super::*;
    use crate::{
        _core::AccessMode,
        backend::SiblingScan,
        sid::{PackageSid, package_name},
    };

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

    /// **The two renderings of the one table agree**: [`ace_spec`]'s string is the
    /// tool's spelling of the pair [`ace_bits`] writes, and this walks all six
    /// shapes rather than the two a caller happens to use.
    ///
    /// The string is read structurally — the `(OI)(CI)` token iff the shape
    /// propagates, and the mask's own alias — because a comparison of two
    /// hand-written tables would be a third copy of them. What can still drift is
    /// only the *spelling*, and that is what
    /// `the_aliases_icacls_writes_are_the_masks_this_read_compares` measures against
    /// the tool.
    #[test]
    fn the_spec_string_and_the_bits_are_one_table() {
        for target in [Target::Directory, Target::ProgramDirectory, Target::File] {
            for access in [AccessMode::ReadOnly, AccessMode::ReadWrite] {
                let (mask, flags) = ace_bits(target, access);
                let spec = ace_spec(target, access);
                assert_eq!(
                    spec.contains("(OI)(CI)"),
                    flags != NO_INHERITANCE,
                    "{target:?}/{access:?}: the string and the flags disagree"
                );
                assert_eq!(
                    spec.ends_with("RX"),
                    mask == READ_AND_EXECUTE,
                    "{target:?}/{access:?}: the string and the mask disagree"
                );
                assert_eq!(
                    spec.ends_with('M'),
                    mask == MODIFY,
                    "{target:?}/{access:?}: the string and the mask disagree"
                );
            }
        }
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
        // The shape the walk hands a predicate: a header, a mask, the SID.
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
    /// **One walk, two readings**: this is [`descriptor_shape`]'s entry list with
    /// the masks kept, so a second traversal of the same ACL — and the second
    /// `unsafe` block that came with it — has no reason to exist.
    fn masks(path: &Path, explicit: bool, sid: PSID) -> Vec<u32> {
        let (_, entries) = descriptor_shape(path, sid);
        entries
            .into_iter()
            .filter(|(flags, _)| !(explicit && u32::from(*flags) & INHERITED_ACE != 0))
            .map(|(_, mask)| mask)
            .collect()
    }

    /// A descriptor as the comparison below needs it: **the control word and the
    /// sequence of access-allowed entries, each as its flags and its mask.**
    ///
    /// This is the whole of what "the same descriptor" means here — owner, group
    /// and SACL are not asked for by either writer, and a byte comparison would
    /// include an ACL revision that is not a behaviour.
    ///
    /// **The ACE walk is [`walk_dacl`]'s, like every other read in this module**,
    /// and this measurement puts the two access-allowed cases it hands back
    /// together again: the entry list it compares is every such entry whatever
    /// trustee it names, so the SID handed to the walk only decides which case an
    /// entry arrives as, never whether it is kept.
    fn descriptor_shape(path: &Path, sid: PSID) -> (u16, Vec<(u8, u32)>) {
        let (dacl, descriptor) = read_dacl(path).expect("read").expect("a DACL");
        let mut control = 0u16;
        let mut revision = 0u32;
        // SAFETY: `descriptor` is live until the free below, and both
        // out-parameters are writable.
        let read = unsafe {
            GetSecurityDescriptorControl(descriptor, &raw mut control, &raw mut revision)
        };
        assert_ne!(read, 0, "the control word of {}", path.display());
        let mut entries = Vec::new();
        // SAFETY: `dacl` is the ACL inside the descriptor `read_dacl` returned and
        // `sid` is the caller's live SID; both outlive this walk, and `descriptor` is
        // released once below, after it.
        let walked = unsafe {
            walk_dacl(path, dacl, sid, &mut |entry| {
                if let Ace::Named(ace) | Ace::OtherTrustee(ace) = entry {
                    entries.push((ace.Header.AceFlags, ace.Mask));
                }
                ControlFlow::Continue(())
            })
        };
        free_descriptor(descriptor);
        assert!(walked.is_ok(), "walk the DACL of {}", path.display());
        (control, entries)
    }

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
    /// same rule `tests/acl.rs` follows for the whole layer: what the crate has to
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

    /// What the skip asks of an object: whether its DACL already carries the
    /// grant, read the way `grant_to` reads it — **[`Object::granted`] on an
    /// opened handle**, so the answer is about the object the write would reach.
    fn satisfied(path: &Path, target: Target, access: AccessMode) -> bool {
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");
        let object = Object::open(path).expect("open the object");
        object.verify(target).expect("a plain object");
        object
            .granted(sid.raw(), target, access)
            .expect("read the DACL")
    }

    /// The standing SID as the [`GrantSid`] pair a test hands `grant_to` — resolved
    /// once here, exactly as the production route resolves it, rather than per path.
    fn standing(sid: &OwnedSid) -> GrantSid<'_> {
        GrantSid {
            raw: sid.raw(),
            text: ALL_APPLICATION_PACKAGES,
        }
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

    /// **The other direction of the kind check**: a directory-shaped grant on a
    /// file is refused rather than written.
    ///
    /// Both directory shapes are wrong on a file and wrong differently:
    /// `(OI)(CI)` is inherited by nothing, so the ACE reaches no one, and the
    /// program-directory form is the plain one, which the child's loader would read
    /// as an ACE on a file that is not the loader's. `icacls` writes neither (exit 0,
    /// nothing stored — design §6's evidence row); this is the same rule answered by
    /// the handle, and it is the check `backend::grant_whitelist` used to make with
    /// `is_dir` for decision 22's entry alone — the handle makes it for every shape,
    /// against the object the write would reach.
    #[test]
    fn a_directory_shaped_grant_on_a_file_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file = dir.path().join("plain.txt");
        fs::write(&file, b"x").expect("write");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");

        for target in [Target::Directory, Target::ProgramDirectory] {
            let refused = grant_to(&file, target, AccessMode::ReadOnly, standing(&sid))
                .expect_err("a directory-shaped grant needs a directory");
            assert!(
                refused.to_string().contains("not the kind of object"),
                "{target:?}: {refused}"
            );
            assert!(
                !acl_names(&file, sid.raw()).expect("read the DACL"),
                "{target:?} wrote an ACE on a file it refused"
            );
        }
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
    #[allow(clippy::arc_with_non_send_sync)]
    fn a_second_sibling_pass_writes_only_the_dll_that_lost_the_ace() {
        let dir = tempfile::tempdir().expect("temp dir");
        for name in ["one.dll", "two.dll"] {
            fs::write(dir.path().join(name), b"not a library").expect("dll");
        }
        let scan = SiblingScan::scan(dir.path());
        let sid = PackageSid::derive(&package_name(dir.path())).expect("package SID");
        let mut grants = Grants::new(Arc::new(sid));

        let _ = writes::take();
        assert!(
            grants
                .apply_siblings(&scan, AccessMode::ReadOnly)
                .expect("first pass")
        );
        assert_eq!(
            writes::take(),
            2,
            "the first pass is one ACE per DLL, each on its own handle"
        );

        assert!(
            grants
                .apply_siblings(&scan, AccessMode::ReadOnly)
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
                .apply_siblings(&scan, AccessMode::ReadOnly)
                .expect("the pass runs again")
        );
        assert_eq!(
            writes::take(),
            1,
            "one DLL without the ACE is one write: the other is skipped"
        );
    }

    /// **A path the kernel cannot open is an error and not a satisfied grant** —
    /// the rule [`has_ace`]'s callers rest on, one layer earlier: the grant opens
    /// the object before it reads anything, so a path that does not exist fails
    /// there, with the step named and the path attached.
    #[test]
    fn the_grant_refuses_a_path_it_cannot_open() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("never-created");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, &missing).expect("a SID");
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
    /// The deny names [`ALL_APPLICATION_PACKAGES`] rather than another trustee on
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
}
