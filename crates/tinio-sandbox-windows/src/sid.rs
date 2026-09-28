//! The AppContainer package SID, and the capability SIDs a container is created
//! with.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14): the SID derivation and the two SID/string conversions are the crate's
//! only security-identifier FFI.

#![allow(unsafe_code)]

use std::{
    io,
    os::windows::ffi::OsStrExt,
    path::{Path, PathBuf},
    ptr,
};

use sha2::{Digest, Sha256};
use tinio_util::path::plain_spelling;
use tinio_winsec::to_wide;
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, LocalFree, WAIT_ABANDONED, WAIT_OBJECT_0},
    Security::{
        Authorization::{ConvertSidToStringSidW, ConvertStringSidToSidW},
        FreeSid,
        Isolation::{
            CreateAppContainerProfile, DeleteAppContainerProfile,
            DeriveAppContainerSidFromAppContainerName,
        },
        PSID, SID_AND_ATTRIBUTES,
    },
    System::{
        SystemServices::SE_GROUP_ENABLED,
        Threading::{CreateMutexW, INFINITE, ReleaseMutex, WaitForSingleObject},
    },
};

use crate::{
    _core::{Result, launch},
    backend::Capability,
    handles::last_error,
};

/// `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` — what `CreateAppContainerProfile`
/// returns for a profile that is already there. A residue from a crashed run is
/// success here rather than an error (design decision 21).
///
/// **The collision comes back with the SID out-parameter left null**, which this
/// module measured on 2026-09-21 rather than assumed: the Windows SDK's own
/// sample answers the same code by falling back to
/// `DeriveAppContainerSidFromAppContainerName`, and the takeover below is that
/// pattern. An earlier draft of the plan asserted the opposite — that the
/// creation fills the SID in anyway — and its test failed for it.
///
/// **It is `windows-sys`'s one gap for this crate, and the reason is that
/// `HRESULT_FROM_WIN32` is a macro rather than a constant** — `winnt.h` expands
/// it, so there is no item to name. This is the arithmetic for the one error
/// code the crate tests: `FACILITY_WIN32` (`0x07`) in the high half and
/// `ERROR_ALREADY_EXISTS` (183, which `windows_sys::Win32::Foundation` *does*
/// export) in the low one — `0x8007_0000 | 183`.
const HRESULT_ALREADY_EXISTS: i32 = 0x8007_00B7u32 as i32;

/// `S-1-15-2-1` — **ALL APPLICATION PACKAGES**, the one package SID every
/// AppContainer token carries whatever package the process belongs to.
///
/// **The program directory is granted this SID, once per machine, and that is a
/// cost decision whose security price is stated rather than hidden.** Every other
/// grant this crate makes is for the package SID of one *root* ([`package_name`]),
/// so a new root is a new SID: rewriting the program directory's ACE per root
/// would rewrite that directory's security descriptor on every launch — and a
/// descriptor write propagates eagerly over the whole subtree, measured at
/// ≈0.04 ms per object for this shape on ReFS and 10–20 s for this workspace's
/// `target\debug` (71 757 objects, `docs/tests.md`'s Windows section). The program
/// directory does not depend on the root, so authorizing it once for a SID every
/// container already holds removes that per-root cost entirely.
///
/// **The price is that the program directory becomes readable and executable by
/// any AppContainer application on the machine**, and that the ACE is never taken
/// back. It is confined to the directory itself and the `*.dll` files directly
/// beside the program — the same narrow shapes decision 22 grants — and a build
/// output directory is usually world-readable already; a program directory that
/// holds configuration or keys beside the binary would leak them. **This SID is
/// for that read-only directory alone**: a served root or a state directory
/// granted it would let a process confined to root A write root B, which is the
/// cross-root hole the per-root derivation exists to prevent.
pub const ALL_APPLICATION_PACKAGES: &str = "S-1-15-2-1";

/// The AppContainer package name for `root`.
///
/// Derived, so a hard kill's residue is the *same* SID on the next run and the
/// launcher takes its own residue over instead of accumulating new ACEs (design
/// decision 21). Root-derived rather than a constant so that two servers serving
/// different roots do not share a package SID — a shared SID would let either
/// one read the other's granted tree — and bounded so that the residue stays one
/// directory per root, the same reasoning as decision 10's jail name.
///
/// **The normalisation is what makes the takeover work**, and it is lexical
/// rather than a filesystem call. A `\\?\` or `\\?\UNC\` prefix is stripped
/// first — [`plain_spelling`], whose own doc carries the unit-level rule that
/// strip is written under — because `Path::components` keeps the prefix, so
/// `\\?\E:\data` would otherwise hash apart from `E:\data`. `Path::components`
/// then drops a trailing separator, drops `.` components and collapses repeated
/// separators, so `E:\data`, `E:\data\`, `E:\data\.` and `\\?\E:\data` are one
/// name. Case is folded
/// for the same reason (`E:\Data` is that directory on Windows) and separators
/// are normalised because `/` and `\` are interchangeable to the filesystem and
/// to nothing else. `..` is deliberately left alone: resolving it lexically
/// would let one spelling derive another directory's name without that directory
/// being the one exposed.
///
/// **The digest is taken over two readings of that one normalisation, because only
/// one of them can be a string.** The folded text below is the readable half, and
/// it is lossy by construction: `to_string_lossy` replaces every unit of a path
/// that is not valid UTF-8 with U+FFFD, so two roots that differ only in such a
/// unit — `E:\data\<U+D800>` and `E:\data\<U+DC00>`, both legal directory names
/// and neither of them text — fold to one string, one package name, one SID and so
/// one AppContainer profile. One profile means one ACE tree shared by two roots,
/// and the first `Drop` deletes the profile the other is still launching under.
/// The UTF-16 units of the same normalised path are the second reading, they go
/// into the digest losslessly, and those two roots are two names again.
///
/// **The units are the *normalised* path's and not those of the spelling that
/// arrived**, which is the one thing this could get wrong in the other direction:
/// hashing the raw spelling would split `E:\Data` from `e:/data` and
/// `\\?\E:\data` from `E:\data` — one directory each — and a second name is a
/// second profile, a second ACE and a residue no run ever takes over (decision 21).
/// What those units are folded by is `fold_unit`'s business, and it is not
/// `str::to_lowercase`: that doc carries the many-to-one mapping which is the
/// reason the lossy half could not be the whole digest.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::sid::package_name;
///
/// let name = package_name(Path::new(r"E:\srv\tinio"));
/// assert!(name.starts_with("tinio.sandbox."));
/// // One directory, three spellings, one name — and so one SID.
/// assert_eq!(name, package_name(Path::new(r"e:/srv/tinio/.")));
/// ```
pub fn package_name(root: &Path) -> String {
    // One lexical normalisation, read twice: as the folded text below and as the
    // UTF-16 units under it. `components` does the lexical work the doc above
    // describes — it drops `.` components and a trailing separator, collapses
    // repeated separators, and the `PathBuf` re-joins with this volume's own.
    let normalised = plain_spelling(root).components().collect::<PathBuf>();
    let folded = normalised
        .to_string_lossy()
        .replace('/', r"\")
        .to_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(folded.as_bytes());
    for unit in normalised.as_os_str().encode_wide() {
        hasher.update(fold_unit(unit).to_le_bytes());
    }
    let digest = hex::encode(hasher.finalize());
    format!("tinio.sandbox.{}", &digest[..16])
}

/// One UTF-16 unit with the case fold the name's identity is derived under.
///
/// **The fold is Unicode's *uppercase* mapping applied one unit at a time, and both
/// halves of that are measured on this host (NTFS, 2026-09-24).** Case has to be
/// folded at all because `E:\Data` is `E:\data` here; it may not be folded by
/// `to_lowercase`, because that mapping is many-to-one across code points this
/// volume holds **apart**: U+212A KELVIN SIGN lowercases to U+006B, so
/// `E:\data\K` and `E:\data\k` would share a name and a profile while the probe
/// creates **two** directories. Uppercase is the direction that keeps them apart —
/// U+212A is already uppercase and maps to itself, `k` maps to `K` — and it is also
/// the direction that keeps the merges the volume does have: U+00E4 `ä` and
/// U+00C4 `Ä` both uppercase to U+00C4 and the probe creates **one** directory,
/// where folding ASCII alone would have split them into two names and two profiles.
///
/// **A unit with no one-unit uppercase mapping is left alone**, which mirrors the
/// per-unit table NTFS itself carries and errs the safe way: `ß` and an unpaired
/// surrogate have no single unit to fold to, so a pair that might be one directory
/// gets two names rather than a pair that *is* two directories getting one.
fn fold_unit(unit: u16) -> u16 {
    let Some(character) = char::from_u32(u32::from(unit)) else {
        return unit;
    };
    let mut upper = character.to_uppercase();
    match (upper.next(), upper.next()) {
        (Some(folded), None) if folded.len_utf16() == 1 => {
            u16::try_from(u32::from(folded)).unwrap_or(unit)
        }
        _ => unit,
    }
}

/// The lock one root's launch window and one root's profile deletion exclude each
/// other through — **one kernel object per served root, held across processes**.
///
/// **Why a profile needs one.** One root is one `package_name` is one SID is one
/// profile: a second launcher's `CreateAppContainerProfile` answers
/// `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` with a null SID out-parameter and
/// takes the first launcher's profile over, and `PackageSid`'s `Drop` deletes it
/// when that value goes, because nothing distinguishes "I created it" from "I
/// took it over" (measured on this host, 2026-09-26). The damage that costs is
/// exactly one interleaving — a `CreateProcessW` that lands between another
/// process's delete and its own completion fails `err=2
/// ERROR_FILE_NOT_FOUND` for a program that exists, three ways measured — so the
/// window a launch needs is `[derive the profile … CreateProcessW]`, and both
/// ends of it are taken through this lock: the launcher holds it for the whole
/// window, and [`PackageSid`]'s `Drop` holds it around the delete.
///
/// **Held, not owned for the child's life.** The lock is released at
/// `CreateProcessW`, so two runs on one root still run at once — a running
/// confined child is unharmed by a peer's delete (measured: same token, same
/// package SID, still writes through its ACE), which is why nothing here tries
/// to keep the profile alive for the child. The only case it serialises is a peer
/// whose launch overlaps the same root's window, and its bound is that window:
/// the ACL phase, seconds on a large tree.
///
/// **The name is the package name, so the object is per root and machine-wide**:
/// `Global\tinio.profile.<package_name>`. `Global\` rather than `Local\` because
/// two launchers on one root need not share a session — a service serving a root
/// and an operator's shell on it are one root — and measured on this host
/// 2026-09-27: a non-elevated process creates the object, a peer process opens it
/// by name, and a peer's wait blocks while it is held.
///
/// **Acquisition never fails a launch, and the abandonment rule is why it can
/// be waited on without a bound.** A holder killed inside its window leaves the
/// kernel to release the object, and the waiter's `WaitForSingleObject` answers
/// `WAIT_ABANDONED` rather than hanging or failing: the lock is the waiter's, the
/// invariant it protects holds vacuously (the holder is gone, so no
/// `CreateProcessW` is in flight), and the event says so — a crash is worth an
/// operator's eye, not a refusal. A host that refuses the object outright
/// (`CreateMutexW` failing, which is the machine-wide namespace's one real
/// exposure) degrades to the pre-lock behaviour — warned, not silent — rather
/// than refusing to confine the server at all.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::sid::{ProfileLock, package_name};
///
/// let name = package_name(std::path::Path::new(r"E:\srv\tinio"));
/// // `None` when the kernel refuses the object: a launch continues without the
/// // exclusion rather than failing, which the warning beside it records.
/// let window = ProfileLock::acquire(&name);
/// drop(window);
/// ```
pub struct ProfileLock {
    handle: HANDLE,
}

impl ProfileLock {
    /// Take this root's lock, waiting until the peer that holds it is done.
    ///
    /// `None` when no lock could be created, which the crate treats as "no
    /// exclusion" rather than as a failure: what is being serialised is a
    /// handover between two launchers, and a launcher that refused to start over
    /// it would trade a rare misdiagnosis for a total failure. Both reasons are
    /// logged, because a guarantee that is not in force has to be visible.
    #[must_use]
    pub fn acquire(package: &str) -> Option<Self> {
        // **`Global\` and no fallback to the session namespace.** Measured on
        // this host 2026-09-27: a non-elevated process creates and shares it. A
        // host that refuses it is a host where the exclusion is absent, which is
        // the behaviour every launcher had before this lock existed — stated in
        // a warning rather than papered over with a second scope that would
        // exclude only part of the window's peers.
        let wide = to_wide(format!(r"Global\tinio.profile.{package}"));
        // SAFETY: `wide` is NUL-terminated and alive for the call, a null
        // security descriptor gives the object this process's default DACL, and
        // `binitialowner = 0` leaves the ownership to the wait below — so the
        // create itself never owns the mutex and never needs a release of its
        // own. The handle is closed by `Drop` on every path below.
        let handle = unsafe { CreateMutexW(ptr::null(), 0, wide.as_ptr()) };
        if handle.is_null() {
            let error = last_error();
            tracing::warn!(
                step = "CreateMutexW",
                package = package,
                error = %error,
                "no per-root launch lock, so a peer's exit may delete this \
                 root's profile while a launch is in flight"
            );
            return None;
        }
        // SAFETY: `handle` is the mutex the call above returned and is live for
        // the call; the wait is INFINITE and not alertable, so it returns only
        // when the mutex is this thread's — or when the kernel releases an
        // abandoned one, which is the case below.
        let waited = unsafe { WaitForSingleObject(handle, INFINITE) };
        match waited {
            WAIT_OBJECT_0 => Some(Self { handle }),
            // **The abandonment rule**: the holder died without releasing
            // (it was killed inside its window), so the kernel hands the mutex
            // over rather than gating on a process that no longer exists. The
            // lock is this thread's, the window it would have been protecting is
            // gone with the holder, and the warning is what makes a crashed
            // launcher visible in a log.
            WAIT_ABANDONED => {
                tracing::warn!(
                    step = "WaitForSingleObject",
                    package = package,
                    "the per-root launch lock was abandoned by a launcher that \
                     died holding it; taken over"
                );
                Some(Self { handle })
            }
            other => {
                let error =
                    io::Error::other(format!("WaitForSingleObject: WAIT_EVENT 0x{other:08X}"));
                // SAFETY: `handle` is this call's own allocation and nothing has
                // taken it.
                unsafe { CloseHandle(handle) };
                tracing::warn!(
                    step = "WaitForSingleObject",
                    package = package,
                    error = %error,
                    "no per-root launch lock, so a peer's exit may delete this \
                     root's profile while a launch is in flight"
                );
                None
            }
        }
    }
}

impl Drop for ProfileLock {
    fn drop(&mut self) {
        // SAFETY: `handle` is this value's own mutex, taken by the wait in
        // `acquire` on this thread, so the release is this thread's to make —
        // and the handle is closed exactly once, here.
        if unsafe { ReleaseMutex(self.handle) } == 0 {
            // Only reachable if the guard were dropped on another thread than
            // the one that took it, which the type's own `!Send` shape (a raw
            // `HANDLE`) makes a compile error rather than a state to handle.
            tracing::warn!(
                step = "ReleaseMutex",
                error = %last_error(),
                "the per-root launch lock was not released by its owner"
            );
        }
        // SAFETY: the same handle, closed once, as the mutex is no longer this
        // thread's.
        unsafe { CloseHandle(self.handle) };
    }
}

/// A created package SID **and the profile behind it**, alive for as long as
/// anything needs them.
///
/// The two are one value deliberately. A SID whose profile does not exist cannot
/// launch, so a value that carried one without the other would be a value the
/// launcher could not use; the raw SID is freed with `FreeSid` when this value
/// drops, the string form with `LocalFree`, and the profile with
/// `DeleteAppContainerProfile` — so the profile's lifetime is the value's, which
/// is the launcher's, which is the child's.
///
/// **One profile has several such values, in several processes**, because the
/// profile's name is derived from the served root and nothing else: a second
/// launcher on a root *takes the first's profile over* rather than creating a
/// second one, and the first launcher's `Drop` then deletes it. That is the state
/// this doc used to deny ("there is no state in which this crate holds one
/// without the other"): the takeover's value holds a SID whose profile its peer
/// has removed, and it cannot launch until something creates the profile again
/// (measured 2026-09-26). The damage that costs is one launch in flight, which is
/// why the delete and the launcher's window are both taken under
/// [`ProfileLock`].
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::sid::PackageSid;
///
/// // Creating the profile here means deleting it when `sid` drops.
/// let sid = PackageSid::derive("tinio.sandbox.0123456789abcdef").unwrap();
/// assert!(sid.as_str().starts_with("S-1-15-2-"));
/// ```
pub struct PackageSid {
    raw: PSID,
    text: String,
    name: String,
}

impl PackageSid {
    /// Create the profile for `name` and take its package SID.
    ///
    /// The name is both the profile's identity and its display name, so a
    /// profile left behind by a hard kill is recognisable in the shell's own
    /// list of packages as well as by a derivation (decision 21).
    ///
    /// **A profile that already exists is taken over rather than refused**, and
    /// the SID of that takeover is the derivation's: the creation reports the
    /// collision with no SID, so the name — which is all the derivation needs —
    /// is read instead. The value the caller gets is the same either way, and
    /// nothing is created twice (design decisions 15 and 21).
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the profile
    /// cannot be created or the SID cannot be rendered — a name that is too long
    /// or malformed surfaces here, before anything has been granted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_windows::sid::{PackageSid, package_name};
    ///
    /// let name = package_name(Path::new(r"E:\srv\tinio"));
    /// let sid = PackageSid::derive(&name).unwrap();
    /// assert!(sid.as_str().starts_with("S-1-15-2-"));
    /// ```
    pub fn derive(name: &str) -> Result<Self> {
        let wide = to_wide(name);
        let mut raw: PSID = ptr::null_mut();
        // SAFETY: `wide` is a NUL-terminated UTF-16 buffer alive for the call and
        // `raw` is a valid out-parameter; the display name and the description
        // are the same buffer so the profile is identifiable, and a null
        // capability array with a count of zero is the documented "no
        // capabilities". The returned SID is freed in `Drop` with FreeSid, as the
        // API documents.
        let hr = unsafe {
            CreateAppContainerProfile(
                wide.as_ptr(),
                wide.as_ptr(),
                wide.as_ptr(),
                ptr::null(),
                0,
                &mut raw,
            )
        };
        // `ERROR_ALREADY_EXISTS` is success and not a special case: it is exactly
        // what the run after a hard kill sees, and treating it as a failure would
        // make the residue unrecoverable rather than reusable (design decision
        // 21). It is *not* the whole story, though — the creation reports the
        // collision and leaves `raw` null (measured 2026-09-21, and the Windows
        // SDK's sample falls back the same way), so the SID comes from the
        // derivation the name always yields.
        let taken_over = hr == HRESULT_ALREADY_EXISTS;
        if taken_over {
            // SAFETY: `wide` is NUL-terminated and alive for the call and `raw`
            // is a valid out-parameter; the SID is freed in `Drop` with FreeSid,
            // as the API documents.
            let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut raw) };
            if hr != 0 {
                return Err(launch(
                    "DeriveAppContainerSidFromAppContainerName",
                    name,
                    io::Error::other(format!(
                        "DeriveAppContainerSidFromAppContainerName: hr=0x{hr:08X}"
                    )),
                ));
            }
        } else if hr != 0 {
            return Err(launch(
                "CreateAppContainerProfile",
                name,
                io::Error::other(format!("CreateAppContainerProfile: hr=0x{hr:08X}")),
            ));
        }
        if raw.is_null() {
            // Neither call leaves the out-parameter null on the success it
            // reports, so this is the API's contract breaking rather than a
            // condition the caller can act on — and the operation named is the
            // one that actually ran.
            let operation = if taken_over {
                "DeriveAppContainerSidFromAppContainerName"
            } else {
                "CreateAppContainerProfile"
            };
            return Err(launch(
                operation,
                name,
                io::Error::other("the package SID was not filled in"),
            ));
        }
        let mut text: *mut u16 = ptr::null_mut();
        // SAFETY: `raw` is the SID `CreateAppContainerProfile` just returned, and
        // `text` is a valid out-parameter; the string is freed below.
        let ok = unsafe { ConvertSidToStringSidW(raw, &mut text) };
        if ok == 0 || text.is_null() {
            // SAFETY: `raw` is this call's own allocation and nothing else has
            // taken it.
            unsafe { FreeSid(raw) };
            return Err(launch("render the package SID", name, last_error()));
        }
        // SAFETY: `text` is a NUL-terminated UTF-16 string the conversion just
        // allocated.
        let rendered = unsafe { crate::from_wide(text) };
        // SAFETY: `text` is the buffer the conversion allocated for us, and the
        // copy above was its last use.
        unsafe { LocalFree(text.cast()) };
        // `info!`, and it is the event an operator's "was this process confined,
        // and to what" question is answered from: the name the profile was created
        // under and the SID it returned, as fields rather than a sentence. When a
        // run left ACEs behind, this is the line that says whose they are (shared
        // §7's split, and decision 21).
        tracing::info!(
            step = "CreateAppContainerProfile",
            name = name,
            sid = %rendered,
            taken_over = taken_over
        );
        Ok(Self {
            raw,
            text: rendered,
            name: name.to_owned(),
        })
    }

    /// The `S-1-15-2-…` form: what a log line prints, what `icacls` is given
    /// where a test or an operator runs it, and what a test compares against. One
    /// accessor, not two — an earlier draft carried `sid_text` as a second name for
    /// exactly this string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::sid::PackageSid;
    ///
    /// let sid = PackageSid::derive("tinio.sandbox.0123456789abcdef").unwrap();
    /// assert!(sid.as_str().starts_with("S-1-15-2-"), "{}", sid.as_str());
    /// ```
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The raw SID, for the `SECURITY_CAPABILITIES` value the launch builds.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::sid::PackageSid;
    ///
    /// let sid = PackageSid::derive("tinio.sandbox.0123456789abcdef").unwrap();
    /// assert!(!sid.raw().is_null());
    /// ```
    pub fn raw(&self) -> PSID {
        self.raw
    }
}

impl Drop for PackageSid {
    fn drop(&mut self) {
        // SAFETY: `raw` is this value's own allocation, produced by the profile
        // creation and freed exactly once here.
        unsafe { FreeSid(self.raw) };
        // **The delete is taken under this root's lock, against every launch on
        // it.** A peer holding the lock is between its derive and its
        // `CreateProcessW`, and that is the one interval where this delete
        // damages anything: the peer's own launch then fails `err=2
        // ERROR_FILE_NOT_FOUND` for a program that exists (measured 2026-09-26).
        // A running child is unharmed either way, which is why the lock is taken
        // around the delete rather than held for the child's life.
        let _lock = ProfileLock::acquire(&self.name);
        let wide = to_wide(&self.name);
        // SAFETY: `wide` is NUL-terminated and alive for the call, and the name
        // is the one this value created the profile under.
        let hr = unsafe { DeleteAppContainerProfile(wide.as_ptr()) };
        if hr != 0 {
            // **The delete failed, and that is all this branch means.** It is
            // *not* "someone else deleted it": the API does not report that as a
            // failure at all — it answers `S_OK` for a name that is not there
            // (measured 2026-09-26) — so a non-zero `hr` is this call being
            // refused, and what is left behind is a profile this run could not
            // take back. A residue rather than a failure, and `Drop` has no
            // caller to report one to: the next run derives the same name and
            // takes this profile over (decision 21). Warned, because shared §7's
            // "a documented degradation is logged" is what makes an undeleted
            // profile visible in a log rather than inferable from a package
            // listing.
            tracing::warn!(
                step = "DeleteAppContainerProfile",
                name = %self.name,
                hr = hr,
                absent = false,
                "the profile could not be deleted and may still be on this \
                 machine; the next run on this root takes it over (decision 21)"
            );
        } else {
            // The other half of the pair the creation emits, so a log answers
            // "was this process confined, and to what" from both ends of the
            // profile's life (shared §7).
            //
            // **`hr == 0` is "no profile with this name is left", which is not
            // the same statement as "this call deleted one"**, and the field
            // says the first because that is what the call knows: measured on
            // this host 2026-09-26, `DeleteAppContainerProfile` answers `S_OK`
            // for a name that was never created just as it does for one it
            // removed, so an event that claimed the removal would be reporting
            // an action this value cannot observe — and a run whose profile a
            // peer had already taken is exactly that case.
            tracing::info!(
                step = "DeleteAppContainerProfile",
                name = %self.name,
                sid = %self.text,
                absent = true
            );
        }
    }
}

/// The capability SIDs a container is created with.
///
/// Kept alive until the attribute list is deleted, because
/// `UpdateProcThreadAttribute` requires every value it is given to outlive the
/// list, and the kernel reads it again at `CreateProcessW` time rather than
/// copying it.
///
/// **Every entry carries [`SE_GROUP_ENABLED`], and a container's capability is
/// worth nothing without it.** The SID alone is placed in the child's token and
/// then honoured by nothing — not by the WFP AppContainer filters that enforce
/// the network policy, and not by an access check against a resource whose ACL
/// names the capability SID. [`CapabilitySids::resolve`] carries the
/// measurement.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::sid::CapabilitySids;
///
/// // The default: no capability at all, which is what the outbound guarantee
/// // rests on (design §7).
/// let caps = CapabilitySids::resolve(&[]).unwrap();
/// assert!(caps.is_empty());
/// ```
pub struct CapabilitySids {
    /// **The one owner of every SID this value resolved.** The entries carry each
    /// SID as the kernel takes it, and `Drop` releases the same pointer rather
    /// than a second vector kept index-aligned with this one.
    entries: Vec<SID_AND_ATTRIBUTES>,
}

impl CapabilitySids {
    /// Resolve the capabilities a container may hold into entries.
    ///
    /// **The argument is [`Capability`], and that is a security boundary rather
    /// than a convenience for the caller.** Every entry below is stamped
    /// [`SE_GROUP_ENABLED`], and an enabled group SID is what the kernel honours
    /// — so every SID this function resolves is a group the container really
    /// holds. While the argument was `&[&str]` any text reached it, and a caller
    /// inside this workspace passing `S-1-5-32-544` (`BUILTIN\Administrators`)
    /// would have handed the container that group. The three-variant enum makes
    /// every SID outside the AppContainer capability range unspellable, because
    /// [`Capability::sid`] is the only producer of the text below.
    ///
    /// **There is no string-taking form beside this one, private or `pub(crate)`**,
    /// because nothing needs one: the enum is the crate's only source of
    /// capability SIDs, and the reverse direction does not exist either — a
    /// capability arrives as the name an operator wrote
    /// ([`Capability::from_name`]), never as a SID to parse back
    /// (`backend`'s `capability_names_round_trip_through_from_name` states why).
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when
    /// `ConvertStringSidToSidW` refuses to resolve a SID. **The refusal the
    /// string-taking signature used to document — "not a SID" — is unreachable
    /// through this one**: the three inputs are compile-time constants of the
    /// `S-1-15-3-N` form the conversion accepts, which
    /// `every_capability_the_enum_names_resolves_to_its_own_enabled_sid` pins by
    /// resolving all three, and which
    /// `tests/confine.rs`'s `a_capability_from_the_options_reaches_the_container`
    /// drives end to end. What is left is the FFI call failing for its own reason
    /// (its allocation refused), and that is still reported rather than
    /// unwrapped: the null check cannot be dropped, because a null `Sid` in the
    /// list is a failed `CreateProcessW` rather than a capability.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// let caps = CapabilitySids::resolve(&[Capability::InternetClient]).unwrap();
    /// assert_eq!(caps.len(), 1);
    ///
    /// // Empty is the default, and the configuration the outbound guarantee
    /// // rests on (design §7).
    /// assert!(CapabilitySids::resolve(&[]).unwrap().is_empty());
    /// ```
    pub fn resolve(capabilities: &[Capability]) -> Result<Self> {
        // Built incrementally as a `Self`, so the failure below drops the SIDs
        // already resolved instead of leaking them: the value is the only thing
        // that owns them, and it exists from the first iteration onwards.
        let mut this = Self {
            entries: Vec::with_capacity(capabilities.len()),
        };
        for text in capabilities.iter().map(|capability| capability.sid()) {
            let wide = to_wide(text);
            let mut raw: PSID = ptr::null_mut();
            // SAFETY: `wide` is NUL-terminated and alive for the call; the
            // returned SID is freed in `Drop` with LocalFree, as documented.
            let ok = unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut raw) };
            if ok == 0 || raw.is_null() {
                return Err(launch("resolve a capability SID", text, last_error()));
            }
            // **The attribute is not decoration, and leaving it zero made every
            // capability this crate granted inert.** Measured 2026-09-22 on this
            // host: a container launched with `internetClient` and
            // `Attributes: 0` carries `S-1-15-3-1` in its token — the list
            // reaches the kernel — and the SID is nevertheless honoured by
            // nothing. The confined child was refused `1.1.1.1:53` with
            // `permission denied` however many capabilities it was given, and a
            // directory carrying an ACE for the capability SID stayed unreadable
            // to it. With `SE_GROUP_ENABLED` the same child **reaches** that
            // address, and that ACE grants the read. So the SID being present is
            // not what the check is against; an enabled SID is, and there is no
            // second place this crate builds one.
            this.entries.push(SID_AND_ATTRIBUTES {
                Sid: raw,
                Attributes: SE_GROUP_ENABLED as u32,
            });
        }
        Ok(this)
    }

    /// The contiguous array the `SECURITY_CAPABILITIES` value points at.
    ///
    /// `*const` rather than the `*mut` that structure's field is declared with:
    /// nothing writes through it, and the launch is where the two meet.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// let caps = CapabilitySids::resolve(&[Capability::InternetClient]).unwrap();
    /// // One pointer, one count, and `len` is the count that goes with it.
    /// let _entries = caps.as_ptr();
    /// assert_eq!(caps.len(), 1);
    /// ```
    pub fn as_ptr(&self) -> *const SID_AND_ATTRIBUTES {
        self.entries.as_ptr()
    }

    /// How many capabilities are present.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// assert_eq!(CapabilitySids::resolve(&[]).unwrap().len(), 0);
    /// assert_eq!(
    ///     CapabilitySids::resolve(&[Capability::InternetClient])
    ///         .unwrap()
    ///         .len(),
    ///     1
    /// );
    /// ```
    pub fn len(&self) -> u32 {
        self.entries.len() as u32
    }

    /// Whether the container gets no capability at all — the default, and the
    /// configuration the outbound guarantee rests on (design §7).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// assert!(CapabilitySids::resolve(&[]).unwrap().is_empty());
    /// assert!(
    ///     !CapabilitySids::resolve(&[Capability::InternetClient])
    ///         .unwrap()
    ///         .is_empty()
    /// );
    /// ```
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Drop for CapabilitySids {
    fn drop(&mut self) {
        for entry in &self.entries {
            // SAFETY: each `entry.Sid` came from ConvertStringSidToSidW, which
            // documents LocalFree as the release, and each is freed once.
            unsafe { LocalFree(entry.Sid.cast()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        os::windows::ffi::OsStringExt,
        path::{Path, PathBuf},
    };

    use super::*;

    /// A SID's `S-1-…` text, for the tests that compare a raw SID against the
    /// spelling this module or [`Capability`] carries.
    fn render_sid(sid: PSID) -> String {
        let mut text: *mut u16 = ptr::null_mut();
        // SAFETY: `sid` is a live SID (the caller's own) and `text` is a valid
        // out-parameter; the string is freed below with LocalFree.
        let ok = unsafe { ConvertSidToStringSidW(sid, &mut text) };
        assert_ne!(ok, 0, "a live SID renders");
        // SAFETY: `text` is a NUL-terminated UTF-16 string the conversion just
        // allocated, and the copy above was its last use.
        let rendered = unsafe { crate::from_wide(text) };
        // SAFETY: `text` came from the conversion, whose documented release is
        // LocalFree, and it is freed exactly once here.
        unsafe { LocalFree(text.cast()) };
        rendered
    }

    #[test]
    fn the_package_name_is_root_derived_and_stable() {
        let a = package_name(Path::new(r"E:\data"));
        assert_eq!(a, package_name(Path::new(r"E:\data")));
        assert_ne!(a, package_name(Path::new(r"E:\other")));
        assert!(a.starts_with("tinio.sandbox."), "{a}");
        assert_eq!(a.len(), "tinio.sandbox.".len() + 16, "{a}");
        // AppContainer profile names are limited in length; the digest keeps
        // this short.
        assert!(a.len() < 64, "{a}");
    }

    #[test]
    fn one_directory_has_one_package_name_however_it_is_spelled() {
        // A second name for the same directory would mean a second SID, a second
        // ACE on the same tree, and a residue neither run would ever take over
        // (decision 21). Case, separator, a trailing separator and a `.` are all
        // the same directory.
        let expected = package_name(Path::new(r"E:\Data\Root"));
        assert_eq!(expected, package_name(Path::new(r"e:/data/root")));
        assert_eq!(expected, package_name(Path::new("E:\\Data\\Root\\")));
        assert_eq!(expected, package_name(Path::new(r"E:\Data\.\Root")));
        // `\\?\` and `\\?\UNC\` are the same directory as the plain spelling.
        // Leaving the prefix in the hash splits decision 21's one-SID takeover.
        assert_eq!(expected, package_name(Path::new(r"\\?\E:\Data\Root")));
        assert_eq!(expected, package_name(Path::new(r"\\?\E:\data\root\.")));
        assert_eq!(
            package_name(Path::new(r"\\server\share\data")),
            package_name(Path::new(r"\\?\UNC\server\share\data"))
        );
        // `..` stays lexical: resolving it would name a different directory.
        assert_ne!(
            package_name(Path::new(r"E:\other")),
            package_name(Path::new(r"\\?\E:\data\..\other"))
        );
    }

    /// One root's units: `E:\data\` followed by one code unit that is not text.
    fn root_with(tail: u16) -> Vec<u16> {
        let mut units: Vec<u16> = r"E:\data\".encode_utf16().collect();
        units.push(tail);
        units
    }

    #[test]
    fn two_roots_that_differ_only_outside_utf8_get_different_package_names() {
        // **This is the collapse the lossy half alone could not see.** A Windows
        // path is arbitrary UTF-16 and an unpaired surrogate is a legal unit in
        // one, so `E:\data\<U+D800>` and `E:\data\<U+DC00>` are two directories —
        // and `to_string_lossy` reads both as `e:\data\ufffd`. One package name
        // for the two would be one SID, one AppContainer profile and so one ACE
        // tree between them, and the first `Drop` would delete the profile the
        // other was still launching under (decision 21). No file has to exist for
        // the derivation, which is lexical.
        let high = PathBuf::from(OsString::from_wide(&root_with(0xD800)));
        let low = PathBuf::from(OsString::from_wide(&root_with(0xDC00)));
        // **The two are one root to the lossy reading, and two to the name** —
        // which is the whole fix, asserted on the two halves rather than inferred
        // from the code: the fold the old derivation hashed is identical here, so
        // a name that differed only because the text differed would be
        // indistinguishable from the collapse.
        let folded = |path: &Path| {
            plain_spelling(path)
                .components()
                .collect::<PathBuf>()
                .to_string_lossy()
                .replace('/', r"\")
                .to_lowercase()
        };
        assert_eq!(
            folded(&high),
            folded(&low),
            "one string to the lossy reading"
        );
        assert_ne!(package_name(&high), package_name(&low));
        // Each is still stable, which is what the takeover needs.
        assert_eq!(package_name(&high), package_name(&high));
        // And neither is the name the lossy spelling used to give both of them:
        // the text half is still hashed, but it can no longer decide the name.
        let replacement = PathBuf::from(format!("E:\\data\\{}", '\u{fffd}'));
        assert_ne!(package_name(&high), package_name(&replacement));
        assert_ne!(package_name(&low), package_name(&replacement));
        // The rest of the name's shape is unchanged by the second reading.
        assert!(package_name(&high).starts_with("tinio.sandbox."));
        assert_eq!(package_name(&high).len(), "tinio.sandbox.".len() + 16);
    }

    #[test]
    fn a_kelvin_sign_and_an_ascii_k_are_two_roots() {
        // **Measured on this host, NTFS, 2026-09-24: `…\K` (U+212A) and `…\k`
        // create two directories, and `…\Ä` and `…\ä` create one.** `str::to_lowercase`
        // is many-to-one in the first case (U+212A → U+006B), so the text half
        // alone merges two directories; [`fold_unit`] folds uppercase, which keeps
        // them apart and still merges the second case.
        assert_ne!(
            package_name(Path::new("E:\\data\\\u{212a}")),
            package_name(Path::new(r"E:\data\k"))
        );
        assert_eq!(
            package_name(Path::new("E:\\data\\\u{00e4}")),
            package_name(Path::new("E:\\data\\\u{00c4}"))
        );
        // The fold this module has always promised still holds, because the case
        // fold the units take is the ASCII one the filesystem itself applies.
        assert_eq!(
            package_name(Path::new(r"E:\Data")),
            package_name(Path::new(r"e:\data"))
        );
    }

    #[test]
    fn a_verbatim_prefix_is_stripped_off_a_path_that_is_not_text() {
        // The prefix strip used to need `to_str`, so it was skipped for exactly
        // the paths this module may not be lossy about: `\\?\E:\data\<U+D800>`
        // and `E:\data\<U+D800>` are one directory and must be one name, or the
        // residue of the verbatim spelling is never taken over.
        let mut verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
        verbatim.extend_from_slice(&root_with(0xD800));
        let prefixed = PathBuf::from(OsString::from_wide(&verbatim));
        let plain = PathBuf::from(OsString::from_wide(&root_with(0xD800)));
        assert_eq!(package_name(&prefixed), package_name(&plain));
        // The same for the UNC form, unit by unit.
        let mut unc: Vec<u16> = r"\\?\UNC\server\share\".encode_utf16().collect();
        unc.push(0xDC00);
        let mut plain_unc: Vec<u16> = r"\\server\share\".encode_utf16().collect();
        plain_unc.push(0xDC00);
        assert_eq!(
            package_name(&PathBuf::from(OsString::from_wide(&unc))),
            package_name(&PathBuf::from(OsString::from_wide(&plain_unc)))
        );
        // The three legal spellings the existing test pins are untouched by it.
        assert_eq!(
            package_name(Path::new(r"\\?\E:\Data\Root")),
            package_name(Path::new(r"E:\data\root"))
        );
        assert_eq!(
            package_name(Path::new(r"\\?\UNC\server\share\data")),
            package_name(Path::new(r"\\server\share\data"))
        );
    }

    #[test]
    fn creating_the_profile_yields_the_derived_sid_and_a_second_run_takes_it_over() {
        // Measured on 2026-09-19: a SID that is *only* derived cannot launch —
        // handing it to PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES makes
        // CreateProcessW fail with err=2 ERROR_FILE_NOT_FOUND for our own binary
        // and for cmd.exe alike, ACE already granted or not — and the same call
        // succeeds once `CreateAppContainerProfile` has run. Task 8's launch test
        // is where that is asserted end to end; this one pins the two facts the
        // rest of the plan rests on: the profile's SID is the derivation's SID,
        // and creating an existing profile is a takeover rather than an error.
        let name = package_name(Path::new(r"E:\data"));
        let sid = PackageSid::derive(&name).unwrap();
        assert!(sid.as_str().starts_with("S-1-15-2-"), "{}", sid.as_str());

        let wide = to_wide(&name);
        let mut derived: PSID = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, and `derived`
        // is a valid out-parameter; the SID is freed below with FreeSid.
        let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut derived) };
        assert_eq!(hr, 0, "hr=0x{hr:08X}");
        let rendered = render_sid(derived);
        // SAFETY: `derived` came from the derivation above, whose documented
        // release is FreeSid, and it is freed exactly once here.
        unsafe { FreeSid(derived) };
        assert_eq!(
            rendered,
            sid.as_str(),
            "the profile's SID is the derived SID"
        );

        // The second creation is what a run after a hard kill does, and it is
        // success: `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)`, reported **without**
        // the SID out-parameter filled in — measured on this host 2026-09-21, and
        // the Windows SDK's sample answers that code the same way. What the
        // takeover rests on is therefore not the collision's output but the name,
        // which the derivation below turns into the same SID (decision 21).
        let mut again: PSID = ptr::null_mut();
        // SAFETY: as above: a null capability array with a count of zero is the
        // documented "no capabilities", and whatever the out-parameter holds is
        // freed below.
        let hr = unsafe {
            CreateAppContainerProfile(
                wide.as_ptr(),
                wide.as_ptr(),
                wide.as_ptr(),
                ptr::null(),
                0,
                &mut again,
            )
        };
        assert_eq!(hr, HRESULT_ALREADY_EXISTS, "hr=0x{hr:08X}");
        // SAFETY: `again` is this call's own allocation when it handed one back at
        // all, and the collision promises nothing about that — so the free is
        // guarded rather than assumed.
        if !again.is_null() {
            unsafe { FreeSid(again) };
        }

        // The residue is *reusable*, which is the property rather than the
        // collision's return value: the same name derives the same SID the first
        // creation returned, so a takeover has something to launch under.
        let mut reused: PSID = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, and `reused` is
        // a valid out-parameter; the SID is freed below with FreeSid.
        let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut reused) };
        assert_eq!(hr, 0, "hr=0x{hr:08X}");
        let reused_rendered = render_sid(reused);
        // SAFETY: `reused` came from the derivation above, whose documented
        // release is FreeSid, and it is freed exactly once here.
        unsafe { FreeSid(reused) };
        assert_eq!(
            reused_rendered,
            sid.as_str(),
            "the residue is reusable: one name, one SID"
        );

        // And the module's own takeover, which is what the launcher calls: the
        // same name while the first value still holds the profile alive.
        let taken_over = PackageSid::derive(&name).unwrap();
        assert_eq!(
            taken_over.as_str(),
            sid.as_str(),
            "the takeover keeps the SID"
        );
    }

    #[test]
    fn no_capabilities_is_the_default_and_is_free() {
        let caps = CapabilitySids::resolve(&[]).unwrap();
        assert!(caps.is_empty());
        assert_eq!(caps.len(), 0);
    }

    /// **The property the string-taking signature could not state**: what this
    /// value hands `CreateProcessW` is one of the enum's own SIDs — no caller
    /// spells one — and every entry is enabled, since an inert `Attributes: 0` is
    /// what made each capability this crate granted do nothing (the measurement
    /// is on [`CapabilitySids::resolve`]).
    ///
    /// **That an enabled SID is honoured is not assertable here**, because it
    /// takes a container: `tests/network.rs` reads it out of the child's token
    /// through an ACL and a socket, and that is where the measurement lives.
    #[test]
    fn every_capability_the_enum_names_resolves_to_its_own_enabled_sid() {
        let all = [
            Capability::InternetClient,
            Capability::InternetClientServer,
            Capability::PrivateNetworkClientServer,
        ];
        let caps = CapabilitySids::resolve(&all).unwrap();
        assert_eq!(caps.len(), all.len() as u32);
        for (index, capability) in all.iter().enumerate() {
            // SAFETY: `as_ptr` points at `len` initialized entries and `index` is
            // below that count, because it enumerates the list they were resolved
            // from; the reference borrows the array for the loop body, which ends
            // before the value drops.
            let entry = unsafe { &*caps.as_ptr().add(index) };
            assert_eq!(entry.Attributes, SE_GROUP_ENABLED as u32, "{capability:?}");
            assert_eq!(render_sid(entry.Sid), capability.sid(), "{capability:?}");
        }
    }
}
