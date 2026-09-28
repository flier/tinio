//! Exposure policy: the caller's set, the loader entries beside the program,
//! and the spelling the ACEs are written in.
//!
//! `Backend::confine` resolves through [`canonical_whitelist`], which walks the
//! caller's paths **once** and hands back the two things built from that single
//! walk — the [`whitelist::CallerSet`] the shape question is asked of, and the entries the
//! grant loop walks. Decision 22's program-directory rule (Ruling 78) is
//! [`exposures`], [`with_siblings`], and [`SiblingScan`]; the sibling `*.dll`
//! entries are enumerated by name, checked on the object itself and granted one by
//! one by handle, so they are listed and counted as their own entries.
//! [`canonical_exposures`] and
//! [`canonical_whitelist`] refuse a reparse point and a caller's file before any
//! grant. [`shape_for`] picks the ACE shape. [`merge`] is the core's rule. Which
//! files must be reachable is an exposure decision; granting them is `acl`'s.
//!
//! [`canonical_whitelist`], [`shape_for`], [`Whitelist`], and [`whitelist::CallerSet`] are
//! `pub(in crate::backend)`. `CallerSet` stays narrow so a raw spelling cannot be mistaken
//! for the resolved one. Nothing else here is `pub` that was not `pub` before.

mod reparse;
mod siblings;
mod whitelist;

use std::path::{Path, PathBuf};

pub use siblings::{SiblingScan, sibling_dlls};
#[cfg(test)]
pub(in crate::backend) use whitelist::CallerSet;
pub use whitelist::canonical_exposures;
pub(in crate::backend) use whitelist::{Whitelist, canonical_whitelist, shape_for};

use crate::_core::{Exposure, Result, Sandbox, describe_exposures};

/// The full whitelist: what the caller exposed, plus what the executable's own
/// loader needs — the directory holding it, and the `*.dll` files directly
/// beside it.
///
/// **The rule is decision 22's, as Ruling 78 (2026-09-22) reshaped it, and the
/// shape follows what the confined child's loader actually reads** (the evidence
/// row behind the decision): `CreateProcessW` opens the image with the
/// *launcher's* access, so a child whose dependencies all live in `System32`
/// needs no grant at all — what needs one is what the child loads *after*
/// creation, and for a binary that imports a DLL beside it the failure is a
/// silent `0xC0000135 (STATUS_DLL_NOT_FOUND)` rather than a launch error.
///
/// **The program-directory entry is granted non-inherited and the DLLs are named
/// individually, which is the measured trade the operator took**: a grant on that
/// directory that propagates to every descendant is 5 129 objects in this
/// workspace's own `target/debug/deps` — of which 68 are DLLs — and about
/// 1.6–1.9 s per launch walked twice, so the rule is one non-inherited `RX` ACE on
/// the program's directory plus one **file-shaped `RX` ACE per DLL**, each written
/// through a handle opened on the object the enumeration named. The batch that
/// spelled `*.dll` for the tool is gone: a wildcard is not an object, and its two
/// measured failures are a junction and a file symlink named `x.dll` — the first
/// took the ACE on the link's own object and the second on its **target**, outside
/// the program's directory (`acl`'s module doc carries both). **The
/// caller's own exposures are not narrowed by this rule**: they keep the inherited
/// form (§6's whitelist), because a file already inside one of them has to stay
/// reachable by the child. What this **deliberately does not reach** is a DLL in a
/// *subdirectory* of the program's directory, or one `LoadLibrary`'d from elsewhere
/// at runtime; that boundary is documented in the design rather than assumed away.
///
/// **A directory that cannot be read contributes no DLL entry and is not an
/// error**: the entry for the directory itself is produced either way, and the
/// resolution that follows (the launcher's own `canonical_whitelist` on the launch
/// path, [`canonical_exposures`] for a caller that hands the set in) is what
/// reports a path that does not exist. That is the difference between this function and a
/// filesystem walk with a failure mode of its own: a program directory that is
/// not there is reported by the resolution, before a single ACE is granted —
/// which is the shape `tests/confine.rs`'s failed-launch test has to work around,
/// because the state it is about is a failure *with the ACLs already on*.
///
/// **This is the set as spelled, and the launch resolves it through its own
/// `canonical_whitelist`** — the same rule read once, with the sibling `*.dll`
/// files kept aside for their one pattern grant instead of being walked as
/// entries of their own.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_windows::backend::exposures;
///
/// let sandbox = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadWrite);
/// let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
/// // The caller's root and the program's own directory, whatever that directory
/// // holds: the `*.dll` files beside the program are entries of their own, so how
/// // many there are is the filesystem's answer rather than this call's.
/// assert!(
///     all.iter()
///         .any(|exposure| exposure.host == Path::new(r"E:\srv\tinio"))
/// );
/// assert!(
///     all.iter()
///         .any(|exposure| exposure.host == Path::new(r"E:\build"))
/// );
/// ```
pub fn exposures(sandbox: &Sandbox, program: &Path) -> Vec<Exposure> {
    // A bare file name has no directory of its own to expose; the resolution that
    // follows reports it (`canonical_whitelist`), which is the same rule the
    // directory half states below.
    let siblings = program.parent().map(SiblingScan::scan);
    merge(build_exposures(sandbox, siblings.as_ref()).into_all())
}

/// [`exposures`] for a caller that already enumerated the program's siblings.
///
/// **The scan travels in and the directory is not re-read**, which is the
/// interface that makes one enumeration the answer for every reader:
/// the launcher's own `canonical_whitelist` and [`confine`]'s per-file grant
/// ([`Grants::apply_siblings`]) are both handed this scan, and that is what keeps
/// the exposure set, the banner and the grants about one file set rather than
/// three readings of a directory that a build may be changing under them.
///
/// [`confine`]: crate::_core::Backend::confine
/// [`Grants::apply_siblings`]: crate::acl::Grants::apply_siblings
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_windows::backend::{SiblingScan, with_siblings};
///
/// let sandbox = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadWrite);
/// // One scan, handed to both readers: the exposure set and the per-DLL grants
/// // are then about one directory listing.
/// let siblings = SiblingScan::scan(Path::new(r"E:\build"));
/// let all = with_siblings(&sandbox, &siblings);
/// assert!(
///     all.iter()
///         .any(|exposure| exposure.host == Path::new(r"E:\build"))
/// );
/// ```
pub fn with_siblings(sandbox: &Sandbox, siblings: &SiblingScan) -> Vec<Exposure> {
    merge(build_exposures(sandbox, Some(siblings)).into_all())
}

/// The exposure set, split where the grant splits it.
///
/// **The split is the grant's own shape rather than the caller's bookkeeping.**
/// The caller's entries and the program's directory are resolved, merged and
/// checked like any other exposure, and their ACE is the shape [`shape_for`]
/// picks; the sibling `*.dll` entries are kept as spelled, because the grant opens
/// each one by handle and the resolution's `fs::canonicalize` plus ancestor walk
/// per DLL bought the launch nothing — the one component the resolution covered
/// for them is asked of the object itself by `refuse_a_reparse_sibling` and again
/// by the handle check.
///
/// **The three parts are kept apart here and joined by [`Self::into_all`]**, so
/// the public set and the launch's whitelist are one rule read two ways rather
/// than two assemblies of one rule: the order is the caller's entries, the
/// caller's order preserved, then the program's directory, then the DLLs.
struct WhitelistParts {
    /// The caller's entries, in the caller's order, as spelled.
    caller: Vec<Exposure>,
    /// Decision 22's entry for the program's own directory, when the program has
    /// a directory to expose.
    program: Option<Exposure>,
    /// The sibling `*.dll` entries the scan named, as it spelled them.
    dlls: Vec<Exposure>,
}

impl WhitelistParts {
    /// Every part as one vector, in the order the banner lists them.
    fn into_all(self) -> Vec<Exposure> {
        let mut all = self.caller;
        all.extend(self.program);
        all.extend(self.dlls);
        all
    }
}

/// The exposure set as spelled, with `siblings`' directory and files appended when
/// the program has one.
fn build_exposures(sandbox: &Sandbox, siblings: Option<&SiblingScan>) -> WhitelistParts {
    let caller = sandbox.exposures().to_vec();
    match siblings {
        Some(siblings) => WhitelistParts {
            caller,
            program: Some(siblings::program_directory(siblings)),
            dlls: siblings::sibling_exposures(siblings),
        },
        None => WhitelistParts {
            caller,
            program: None,
            dlls: Vec::new(),
        },
    }
}

/// Collapse duplicate hosts, keeping the widest access.
///
/// A path may be exposed twice — the caller's list, the state directory, the
/// executable's directory — and two entries for one path would be two ACEs
/// for the same SID on one directory, one of which the grant-only read would
/// report as already granted for ever after.
///
/// **The rule is the core's, and this calls into it rather than restating it.**
/// [`Sandbox::from_exposures`] applies `Sandbox::expose`'s most-permissive rule
/// to the vector it is given: the first entry for a path stays where it was and a
/// later, wider duplicate merges into it. The copy that used to live here had
/// already drifted from it — it assigned an [`crate::_core::AccessMode::ReadWrite`] literal
/// where the core assigns the incoming `access` — which is the same answer only
/// while the enum has two variants, and the drift is exactly what the core's own
/// doc on that function is about.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Exposure};
/// use tinio_sandbox_windows::backend::merge;
///
/// let merged = merge(vec![
///     Exposure {
///         host: r"E:\data".into(),
///         access: AccessMode::ReadOnly,
///     },
///     Exposure {
///         host: r"E:\data".into(),
///         access: AccessMode::ReadWrite,
///     },
///     Exposure {
///         host: r"E:\state".into(),
///         access: AccessMode::ReadWrite,
///     },
/// ]);
/// // One entry per path, and the widest access survives — in either order.
/// assert_eq!(merged.len(), 2);
/// assert_eq!(merged[0].access, AccessMode::ReadWrite);
/// ```
pub fn merge(exposures: Vec<Exposure>) -> Vec<Exposure> {
    Sandbox::from_exposures(exposures).exposures().to_vec()
}

/// The whitelist as one line, for the launcher's own log.
///
/// **The launcher logs what it is about to grant**, and this string is the
/// `granting` field of the `grant the ACLs` event `backend` emits beside the
/// package SID and the exposure count: an operator whose server cannot read a
/// path needs to see which paths were on the list, and the design's residue story
/// needs the package SID next to it. Nothing prints this line itself — it is a
/// field of a `tracing` event, so `[log] verbosity` and the subscriber decide
/// whether it appears.
///
/// **The spelling is [`describe_exposures`]'s and not this crate's**, which is
/// the point of the delegate rather than a stop on the way: that function is the
/// workspace's one renderer for an exposure set, and its human words —
/// `read-only` / `read-write` — are what a Linux jail and a macOS profile print
/// too. An operator greps one vocabulary whichever backend's log they are reading,
/// and the `ReadOnly` a `Debug` used to print here went with the local renderer
/// that printed it.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Exposure};
/// use tinio_sandbox_windows::backend::describe;
///
/// let one = [Exposure {
///     host: r"E:\srv\tinio".into(),
///     access: AccessMode::ReadOnly,
/// }];
/// // The human spelling, shared with the other platforms, and the empty string
/// // for an empty set.
/// assert_eq!(describe(&one), r"E:\srv\tinio (read-only)");
/// assert_eq!(describe(&[]), "");
/// ```
pub fn describe(exposures: &[Exposure]) -> String {
    describe_exposures(exposures)
}

/// The served root: the first exposure the caller gave.
///
/// The core's builder keeps the caller's order and `serve` exposes the root
/// before anything else, so the first entry is the root by construction.
///
/// **The convention is the core's now** ([`Sandbox::served_root`]), and this is
/// the platform's old spelling of it delegating to that one: the same first entry,
/// and the same refusal — `Error::Step` with the step `confine` and the message
/// `nothing exposed to confine to` — built once in the crate that owns the
/// convention instead of once per backend. The operator-facing text is therefore
/// unchanged, and the variant is still `Step` rather than `Unsupported` because an
/// empty set is the caller's mistake rather than this platform's shortcoming
/// (design §11).
///
/// The `PathBuf` is this signature's own and the reason it stays: its two users —
/// [`package_name`](crate::sid::package_name) and the launch's own diagnostics —
/// want the root alone, not the [`Exposure`] carrying it.
///
/// # Errors
///
/// [`Error::Step`] with the step `confine` when the sandbox exposes nothing.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_windows::backend::served_root;
///
/// let sandbox = Sandbox::new()
///     .expose(r"E:\srv\tinio", AccessMode::ReadOnly)
///     .expose(r"E:\state", AccessMode::ReadWrite);
/// assert_eq!(served_root(&sandbox)?, Path::new(r"E:\srv\tinio"));
/// assert!(served_root(&Sandbox::new()).is_err());
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
///
/// [`Error::Step`]: crate::_core::Error::Step
pub fn served_root(sandbox: &Sandbox) -> Result<PathBuf> {
    Ok(sandbox.served_root()?.host.clone())
}
