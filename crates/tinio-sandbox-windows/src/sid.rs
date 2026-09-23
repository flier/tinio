//! The AppContainer package SID, and the capability SIDs a container is created
//! with.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14): the SID derivation and the two SID/string conversions are the crate's
//! only security-identifier FFI.

#![allow(unsafe_code)]

use std::{
    borrow::Cow,
    io,
    path::{Path, PathBuf},
    ptr,
};

use sha2::{Digest, Sha256};
use windows_sys::Win32::{
    Foundation::LocalFree,
    Security::{
        Authorization::{ConvertSidToStringSidW, ConvertStringSidToSidW},
        FreeSid,
        Isolation::{
            CreateAppContainerProfile, DeleteAppContainerProfile,
            DeriveAppContainerSidFromAppContainerName,
        },
        PSID, SID_AND_ATTRIBUTES,
    },
    System::SystemServices::SE_GROUP_ENABLED,
};

use crate::{
    _core::{Result, launch},
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
/// first — `Path::components` keeps it, so `\\?\E:\data` would otherwise hash
/// apart from `E:\data`. `Path::components` then drops a trailing separator,
/// drops `.` components and collapses repeated separators, so `E:\data`,
/// `E:\data\`, `E:\data\.` and `\\?\E:\data` are one name. Case is folded
/// for the same reason (`E:\Data` is that directory on Windows) and separators
/// are normalised because `/` and `\` are interchangeable to the filesystem and
/// to nothing else. `..` is deliberately left alone: resolving it lexically
/// would let one spelling derive another directory's name without that directory
/// being the one exposed.
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
    let normalised = plain_root(root)
        .components()
        .collect::<PathBuf>()
        .to_string_lossy()
        .replace('/', r"\")
        .to_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(normalised.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("tinio.sandbox.{}", &digest[..16])
}

/// The plain spelling of `root`.
///
/// `\\?\E:\data` and `\\?\UNC\server\share\data` become `E:\data` and
/// `\\server\share\data`. A path with no verbatim prefix, or one [`Path::to_str`]
/// cannot read, is returned unchanged.
fn plain_root(root: &Path) -> Cow<'_, Path> {
    let Some(text) = root.to_str() else {
        return Cow::Borrowed(root);
    };
    let Some(rest) = text.strip_prefix(r"\\?\") else {
        return Cow::Borrowed(root);
    };
    Cow::Owned(match rest.strip_prefix(r"UNC\") {
        Some(unc) => PathBuf::from(format!(r"\\{unc}")),
        None => PathBuf::from(rest),
    })
}

/// A created package SID **and the profile behind it**, alive for as long as
/// anything needs them.
///
/// The two are one value deliberately. A SID whose profile does not exist cannot
/// launch, so there is no state in which this crate holds one without the other;
/// the raw SID is freed with `FreeSid` when this value drops, the string form
/// with `LocalFree`, and the profile with `DeleteAppContainerProfile` — so the
/// profile's lifetime is the value's, which is the launcher's, which is the
/// child's.
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
        let wide = crate::wide(name);
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

    /// The `S-1-15-2-…` form: what `icacls` is given, what a log line prints,
    /// and what a test compares against. One accessor, not two — an earlier
    /// draft carried `sid_text` as a second name for exactly this string.
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
        let wide = crate::wide(&self.name);
        // SAFETY: `wide` is NUL-terminated and alive for the call, and the name
        // is the one this value created the profile under.
        let hr = unsafe { DeleteAppContainerProfile(wide.as_ptr()) };
        if hr != 0 {
            // A residue rather than a failure, and `Drop` has no caller to report
            // one to: the next run derives the same name and takes this profile
            // over (decision 21). Warned, because shared §7's "a documented
            // degradation is logged" is what makes an undeleted profile visible
            // in a log rather than inferable from a package listing.
            tracing::warn!(step = "DeleteAppContainerProfile", name = %self.name, hr = hr);
        } else {
            // The other half of the pair the creation emits, so a log answers
            // "was this process confined, and to what" from both ends of the
            // profile's life (shared §7).
            tracing::info!(step = "DeleteAppContainerProfile", name = %self.name, sid = %self.text);
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
    entries: Vec<SID_AND_ATTRIBUTES>,
    owned: Vec<PSID>,
}

impl CapabilitySids {
    /// Resolve capability SID strings (`S-1-15-3-3`, …) into entries.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) for a string that is
    /// not a SID.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::sid::CapabilitySids;
    ///
    /// let caps = CapabilitySids::resolve(&["S-1-15-3-1"]).unwrap();
    /// assert_eq!(caps.len(), 1);
    ///
    /// assert!(CapabilitySids::resolve(&["not-a-sid"]).is_err());
    /// ```
    pub fn resolve(sids: &[&str]) -> Result<Self> {
        // Built incrementally as a `Self`, so the failure below drops the SIDs
        // already resolved instead of leaking them: the value is the only thing
        // that owns them, and it exists from the first iteration onwards.
        let mut this = Self {
            entries: Vec::with_capacity(sids.len()),
            owned: Vec::with_capacity(sids.len()),
        };
        for text in sids {
            let wide = crate::wide(text);
            let mut raw: PSID = ptr::null_mut();
            // SAFETY: `wide` is NUL-terminated and alive for the call; the
            // returned SID is freed in `Drop` with LocalFree, as documented.
            let ok = unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut raw) };
            if ok == 0 || raw.is_null() {
                return Err(launch("resolve a capability SID", *text, last_error()));
            }
            this.owned.push(raw);
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
    /// use tinio_sandbox_windows::sid::CapabilitySids;
    ///
    /// let caps = CapabilitySids::resolve(&["S-1-15-3-1"]).unwrap();
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
    /// use tinio_sandbox_windows::sid::CapabilitySids;
    ///
    /// assert_eq!(CapabilitySids::resolve(&[]).unwrap().len(), 0);
    /// assert_eq!(CapabilitySids::resolve(&["S-1-15-3-1"]).unwrap().len(), 1);
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
    /// use tinio_sandbox_windows::sid::CapabilitySids;
    ///
    /// assert!(CapabilitySids::resolve(&[]).unwrap().is_empty());
    /// assert!(!CapabilitySids::resolve(&["S-1-15-3-1"]).unwrap().is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Drop for CapabilitySids {
    fn drop(&mut self) {
        for sid in self.owned.drain(..) {
            // SAFETY: each `sid` came from ConvertStringSidToSidW, which
            // documents LocalFree as the release, and each is freed once.
            unsafe { LocalFree(sid.cast()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

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

        let wide = crate::wide(&name);
        let mut derived: PSID = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, and `derived`
        // is a valid out-parameter; the SID is freed below with FreeSid.
        let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut derived) };
        assert_eq!(hr, 0, "hr=0x{hr:08X}");
        let mut text: *mut u16 = ptr::null_mut();
        // SAFETY: `derived` is the SID just returned and `text` is a valid
        // out-parameter; the string is freed below with LocalFree.
        let ok = unsafe { ConvertSidToStringSidW(derived, &mut text) };
        assert_ne!(ok, 0);
        // SAFETY: `text` is a NUL-terminated UTF-16 string the conversion just
        // allocated, and it is the last use of the buffer.
        let rendered = unsafe { crate::from_wide(text) };
        // SAFETY: `text` came from the conversion, whose documented release is
        // LocalFree, and `derived` from the derivation, whose release is FreeSid.
        unsafe {
            LocalFree(text.cast());
            FreeSid(derived);
        }
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
        let mut reused_text: *mut u16 = ptr::null_mut();
        // SAFETY: `reused` is the SID just returned and `reused_text` is a valid
        // out-parameter; the string is freed below with LocalFree.
        let ok = unsafe { ConvertSidToStringSidW(reused, &mut reused_text) };
        assert_ne!(ok, 0);
        // SAFETY: `reused_text` is a NUL-terminated UTF-16 string the conversion
        // just allocated, and it is the last use of the buffer.
        let reused_rendered = unsafe { crate::from_wide(reused_text) };
        // SAFETY: `reused_text` came from the conversion, whose documented release
        // is LocalFree, and `reused` from the derivation, whose release is FreeSid.
        unsafe {
            LocalFree(reused_text.cast());
            FreeSid(reused);
        }
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

    #[test]
    fn a_capability_string_that_is_not_a_sid_is_an_error() {
        assert!(CapabilitySids::resolve(&["not-a-sid"]).is_err());
    }
}
