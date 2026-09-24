//! Exposure policy: the caller's set, the loader entries beside the program,
//! and the spelling the ACEs are written in.
//!
//! `Backend::confine` calls these in order. Decision 22's program-directory
//! rule (Ruling 78) is [`exposures`], [`with_siblings`], and [`SiblingScan`].
//! [`canonical_exposures`] and [`canonical_caller`] refuse a reparse point and
//! a caller's file before any grant. [`shape_for`] picks the ACE shape.
//! [`merge`] is the core's rule. Which files must be reachable is an exposure
//! decision; granting them is `acl`'s.
//!
//! [`canonical_caller`], [`shape_for`], and [`CallerSet`] are `pub(super)`.
//! `CallerSet` stays narrow so a raw spelling cannot be mistaken for the
//! resolved one. Nothing else here is `pub` that was not `pub` before.

use std::{
    fs, io,
    os::windows::fs::MetadataExt,
    path::{Path, PathBuf},
};

use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

use crate::{
    _core::{AccessMode, Error, Exposure, Result, Sandbox, fail, launch},
    acl::Target,
};

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
/// the program's directory plus one **batched** file-shaped `RX` ACE on the DLLs —
/// one `icacls` invocation through [`SiblingScan::pattern`], because the tool takes a
/// single `name` pattern and no list of paths (measured 2026-09-21). **The
/// caller's own exposures are not narrowed by this rule**: they keep the inherited
/// form (§6's whitelist), because a file already inside one of them has to stay
/// reachable by the child. What this **deliberately does not reach** is a DLL in a
/// *subdirectory* of the program's directory, or one `LoadLibrary`'d from elsewhere
/// at runtime; that boundary is documented in the design rather than assumed away.
///
/// **A directory that cannot be read contributes no DLL entry and is not an
/// error**: the entry for the directory itself is produced either way, and the
/// resolution that follows ([`canonical_exposures`]) is what reports a path that
/// does not exist. That is the difference between this function and a filesystem
/// walk with a failure mode of its own — its one caller needs a program directory
/// that does not exist to reach the *grant*, which is what `tests/confine.rs`'s
/// failed-launch rollback test relies on.
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
/// // A program directory that does not exist contributes no DLL entry.
/// let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
/// assert_eq!(all.len(), 2);
/// ```
pub fn exposures(sandbox: &Sandbox, program: &Path) -> Vec<Exposure> {
    // A bare file name has no directory of its own to expose; the resolution that
    // follows reports it (`canonical_exposures`), which is the same rule the
    // directory half states below.
    let siblings = program.parent().map(SiblingScan::scan);
    build_exposures(sandbox, siblings.as_ref())
}

/// [`exposures`] for a caller that already enumerated the program's siblings.
///
/// **The scan travels in and the directory is not re-read**, which is the
/// interface that makes one enumeration the answer for every reader:
/// [`confine`] needs the same scan for the grant it makes through
/// [`SiblingScan::pattern`], and passing it here is what keeps the exposure set,
/// the banner and the grant about one file set rather than three readings of a
/// directory that a build may be changing under them.
///
/// [`confine`]: crate::_core::Backend::confine
pub fn with_siblings(sandbox: &Sandbox, siblings: &SiblingScan) -> Vec<Exposure> {
    build_exposures(sandbox, Some(siblings))
}

/// The exposure set, with `siblings`' directory and files appended when the
/// program has one.
fn build_exposures(sandbox: &Sandbox, siblings: Option<&SiblingScan>) -> Vec<Exposure> {
    let mut all = sandbox.exposures().to_vec();
    if let Some(siblings) = siblings {
        all.push(Exposure {
            host: siblings.dir().to_path_buf(),
            access: AccessMode::ReadOnly,
        });
        all.extend(sibling_exposures(siblings));
    }
    // The caller's order, with each path's widest access — the core's rule, taken
    // from the core rather than restated here. Two callers of it exist in this
    // module and this is the one that transforms an already-built set, which is
    // why `device_files_enabled()` travels with it (the core's own doc says why
    // the flag is a parameter).
    Sandbox::from_exposures(all, sandbox.device_files_enabled())
        .exposures()
        .to_vec()
}

/// **The caller's own exposure set, in the volume's canonical spelling — and the
/// only spelling anything downstream can be handed.**
///
/// This type exists to make one mistake unrepresentable rather than testable: the
/// answer `shape_for` asks — "did the *caller* expose this path?" — is asked
/// against [`canonical_caller`]'s resolved set, and a call site that passed the raw
/// set instead would compile and pass every existing test while shaping a caller's
/// exposed directory as decision 22's non-inherited [`Target::ProgramDirectory`],
/// which leaves the pre-existing descendants of that directory unreachable from
/// the confined child (Ruling 78's regression, P1 of the deferred list). The raw
/// set is not a `CallerSet`, so that call site is a **compile error**, not a test
/// failure — and the tests that reach this distinction need an AppContainer
/// profile, which is exactly how the mistake stayed invisible (Ruling 85).
///
/// **It is deliberately not a `Deref<Target = [Exposure]>`.** A `Deref` would
/// re-open the door this type closes: every reader back on a `&[Exposure]` could
/// be handed `sandbox.exposures()` by one auto-deref, with the type left as
/// decoration. What it offers instead is a named question — does the caller expose
/// this path — so production readers state what they are asking, and never work on
/// a slice at all (the test-only `entries` accessor hands back this set's own
/// entries, which is a different value from the raw set's).
///
/// **A caller's file is refused here, and this is the site that knows it is the
/// caller's** (P7, Ruling 95): `confine`'s per-path loop grants the
/// `(OI)(CI)`/plain directory forms, and the only shape that lands on a file is
/// [`Grants::apply_siblings`]' batched `*.dll` pattern — its own entries, added
/// by [`exposures`] and never handed to this type. A caller's file would
/// therefore be granted nothing at all while the launch continued, so the set
/// refuses the entry instead ([`Error::BadPath`]; design §6 owns the rule): a
/// loud startup failure rather than a confined server that cannot read a path the
/// operator exposed.
///
/// **And the check is not "is this path a file?" — it is "is this path not a
/// directory?"**: a caller's exposure names a tree, the grant is specified per
/// path as a whole, and anything that is not a directory is a path this launcher
/// cannot grant the way the caller asked.
///
/// [`Grants::apply_siblings`]: crate::acl::Grants::apply_siblings
#[derive(Debug)]
pub(super) struct CallerSet {
    /// The caller's entries, resolved by [`canonical_caller`] and checked to be
    /// directories. Private, which is what makes the raw set unreachable: the
    /// accessor below can only hand back a slice of *this* set, so no reader can
    /// end up holding `sandbox.exposures()` and believing it is this.
    entries: Box<[Exposure]>,
}

impl CallerSet {
    /// **The one constructor, and it is total over what it accepts**: [`canonical_caller`]
    /// has already resolved and checked the entries, so this holds the invariant
    /// rather than re-establishing it.
    fn from_exposures(exposures: Vec<Exposure>) -> Self {
        Self {
            entries: exposures.into_boxed_slice(),
        }
    }

    /// The resolved entries, in the caller's order.
    ///
    /// A slice of *this* set rather than the set it was built from: a reader that
    /// walks it cannot mistake it for `sandbox.exposures()`, because it never sees
    /// the value that spelling produces. **Test-only, and `cfg(test)` is where that
    /// is stated** (the tests in `backend::tests` are the only readers):
    /// production asks the membership question instead, so the slice a reader could
    /// mistake for the raw set is not compiled into the library at all.
    #[cfg(test)]
    pub(super) fn entries(&self) -> &[Exposure] {
        &self.entries
    }

    /// The caller's set with no entries.
    ///
    /// **Test-only, and the state it names is a real one**: a caller that exposes
    /// nothing reaches `confine` only after [`served_root`] has refused it, but the
    /// question this rule asks — "is the program's directory the caller's?" — has an
    /// answer for the empty set too, and the test in `backend::tests` is what asks
    /// it.
    #[cfg(test)]
    pub(super) fn empty() -> Self {
        Self {
            entries: Box::default(),
        }
    }

    /// Whether the caller exposed `path` itself, in the spelled form the volume
    /// resolves it to.
    ///
    /// `pub(super)` rather than private because `backend::tests` asserts on the
    /// answer directly — the resolved spelling's membership is what the raw set
    /// could never give — while production asks it through `shape_for`.
    pub(super) fn exposed(&self, path: &Path) -> bool {
        self.entries.iter().any(|exposure| exposure.host == path)
    }
}

/// Which grant shape an exposed directory takes.
///
/// **Two of the three shapes meet here, and the caller's precedence is the rule
/// that decides between them** (`caller_set` is the caller's exposure set in the
/// canonical spelling — [`canonical_caller`] — because a comparison against the
/// caller's raw spellings can never match the resolved ones: the verbatim form
/// [`canonical_exposures`] resolves to is not what `Sandbox` holds). Every
/// exposed directory takes the inherited form — design §6's whitelist, one ACE per
/// path reaching the descendants that were already there — except the entry
/// decision 22 adds for the program's own directory, which takes the non-inherited
/// [`Target::ProgramDirectory`].
///
/// **The exception applies only while the entry is this launcher's to shape.** A
/// caller that exposed the program's directory itself keeps its entry and its
/// inherited shape, because [`Sandbox`]'s merge rule gives the caller's access
/// priority (a read-only entry can never narrow it) and an automatic entry that
/// silently narrowed it would be the same class of quiet descent as the Ruling 78
/// regression. **The recognition is a membership test against `caller_set` and
/// nothing else**: `all` is the caller's set plus the one entry this rule adds, so
/// the two sets differ by exactly the entry whose ownership is in question.
///
/// **`caller_set` is the [`CallerSet`] and not a slice, and that is the whole of
/// P1's closure**: a call site that reached for `sandbox.exposures()` — or for any
/// other `&[Exposure]` — does not compile here, so the canonical spelling the
/// membership test depends on cannot be left out by accident.
///
/// `program_dir` is the canonical program directory, or `None` when it could not
/// be resolved — in which case nothing is recognized as this rule's and every
/// directory keeps the inherited form, which is the safe direction: an extra
/// descendant is reachable, rather than a file the child needs.
///
/// **`pub(super)`, for its two callers and no further**: `Backend::confine`
/// asks this question once per exposed directory, and `backend::tests` is where
/// the precedence rule above is pinned without a DACL in the loop. A `pub` here
/// would hand a caller outside the crate both the question and the raw
/// spelling it must not be asked with.
pub(super) fn shape_for(
    caller_set: &CallerSet,
    exposure: &Exposure,
    program_dir: Option<&Path>,
) -> Target {
    let is_program_dir = program_dir == Some(exposure.host.as_path());
    match is_program_dir && !caller_set.exposed(&exposure.host) {
        true => Target::ProgramDirectory,
        false => Target::Directory,
    }
}

/// The `*.dll` files a [`SiblingScan`] found, as read-only exposure entries.
///
/// The mapping from the ACL layer's enumeration into exposures, and it is a
/// function of the scan rather than of a `&Path` so that there is no way to spell
/// this that reads the directory a second time: the set below and the `*.dll`
/// grant are about one enumeration, which is what keeps them from disagreeing
/// about which files the program has beside it.
fn sibling_exposures(scan: &SiblingScan) -> Vec<Exposure> {
    scan.files()
        .iter()
        .map(|host| Exposure {
            host: host.clone(),
            access: AccessMode::ReadOnly,
        })
        .collect()
}

/// Collapse duplicate hosts, keeping the widest access.
///
/// A path may be exposed twice — the caller's list, the state directory, the
/// executable's directory — and granting the same SID twice on the same
/// directory would leave an entry rollback has to remove twice.
///
/// **The rule is the core's, and this calls into it rather than restating it.**
/// [`Sandbox::from_exposures`] applies `Sandbox::expose`'s most-permissive rule
/// to the vector it is given: the first entry for a path stays where it was and a
/// later, wider duplicate merges into it. The copy that used to live here had
/// already drifted from it — it assigned an [`AccessMode::ReadWrite`] literal
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
    Sandbox::from_exposures(exposures, false)
        .exposures()
        .to_vec()
}

/// **The refusal's sentence, shared by the event and the error it becomes** — an
/// [`Error::BadPath`] reason is a `&'static str`, so one copy serves both. What it
/// says is what was tested (*not a directory*) rather than the commonest instance:
/// a device path or anything else the volume answers without a directory takes the
/// same branch.
const NOT_A_DIRECTORY: &str = "is not a directory, and this launcher grants the caller's paths as directories (the inherited (OI)(CI) form, or the plain form on the program's own directory), which a file cannot take; expose the directory holding it";

/// The caller's exposure set as the volume names it, for the precedence question
/// [`shape_for`] asks.
///
/// **The set it returns is deliberately not the set `all` ends up with**: this is
/// the caller's own half alone, in the same canonical spelling
/// [`canonical_exposures`] grants in, so membership in it answers "did the caller
/// expose this path?" without comparing a resolved path to a raw one — which is
/// the comparison that could never be true, because [`Sandbox`] holds the
/// spellings the caller wrote and nothing canonicalizes them.
///
/// **This is where the caller's set becomes a [`CallerSet`], and where a
/// caller's file is refused** (P7). The work is the type's rather than the call
/// site's because the two facts are one decision: the set that must be canonical
/// is also the only one whose entries are the caller's, and a file among them is
/// the case nobody would notice — the per-path loop drops it and the launch
/// continues with the path granted nothing.
///
/// # Errors
///
/// The same resolutions and refusals as [`canonical_exposures`], plus
/// [`Error::BadPath`] for a caller's path that is not a directory: the grant this
/// launcher writes is the directory form, and a path that cannot take it is a
/// silent no-op, so the entry is refused loudly instead of being granted nothing.
///
/// **`pub(super)` because it is the only way for another module to hold a
/// [`CallerSet`]** — [`CallerSet::from_exposures`] is this module's own — and
/// the two readers outside this file are `confine`, which holds the precedence
/// question against the type, and `backend::tests`, which asserts the canonical
/// spelling on it. It is not `pub`: a caller outside the crate never needs the
/// set, and the spelling the type withholds is put back by a constructor rather
/// than by an accessor.
pub(super) fn canonical_caller(sandbox: &Sandbox) -> Result<CallerSet> {
    let mut resolutions = Vec::with_capacity(sandbox.exposures().len());
    for exposure in sandbox.exposures() {
        let resolved = canonical_entry(exposure)?;
        if !resolved.host.is_dir() {
            // **This event is built here rather than through the core's
            // `refused`/`fail`, and it has to be**: what it becomes is an
            // `Error::BadPath`, which carries a `path` beside its `reason`, and
            // neither builder can carry one — `fail` would drop the path and
            // `refused` would need the error built first. §7 names `path` as a
            // field an event carries, so the field is worth the hand-written
            // event.
            //
            // **The event is emitted over the values the error is then built from**,
            // which is the core's own `launch` shape: the path is borrowed for its
            // field and moved into the error afterwards, and both name
            // `NOT_A_DIRECTORY`, so one path and one sentence cross the whole refusal
            // with no copy of either. Reporting the *built* error instead —
            // `refuse_reparse_points`' shape — cannot also name the path: the error
            // owns its `PathBuf`, so that event would need a second copy of it.
            tracing::error!(
                step = "resolve an exposed path",
                path = %resolved.host.display(),
                error = %NOT_A_DIRECTORY,
                "sandbox step failed"
            );
            return Err(Error::BadPath {
                path: resolved.host,
                reason: NOT_A_DIRECTORY,
            });
        }
        resolutions.push(resolved);
    }
    Ok(CallerSet::from_exposures(resolutions))
}

/// The exposure set as the volume names it, with every reparse point refused.
///
/// **Why this is a step of its own rather than part of `exposures`.** `icacls`
/// resolves the path it is given and `(OI)(CI)` is inheritable, so a junction or
/// a directory symlink anywhere in an exposed path puts the ACE on whatever the
/// link resolves to — a directory outside the exposure — and inheritance then
/// carries the same ACE into that directory's own children. Following the link
/// *is* the grant the whitelist exists to prevent, and `std::fs::canonicalize`
/// by itself is the following: it resolves the link and returns a path that is no
/// longer a reparse point. The refusal therefore has to be checked on the path as
/// it is spelled, before the resolution, and the resolution has to happen as well
/// — a grant written on a spelling the volume does not use is a grant the child's
/// own view of the path may not match.
///
/// **This is the Windows half of the requirement macOS meets with
/// `with_canonical_paths`** (P3), and the two are separate functions because the
/// failure they prevent is not the same shape: there a profile naming an
/// unresolved spelling simply matches nothing, so macOS *adds* the canonical form
/// beside the original; here the resolved spelling is what the grant lands on, so
/// Windows *requires* the canonical form and refuses the link that produced it.
/// What the two share is the rule, not the remedy: a path is granted in the form
/// the kernel resolves, never in the form it happened to be written in.
///
/// **The caller's entries come first in the vector, and that is a rule rather
/// than an accident of the caller's code**: when a caller's spelled path and an
/// automatic entry the launcher added name one directory, the resolved vector
/// keeps the caller's spelling and its access, because `merge` keeps the first
/// entry for a host and the caller's exposure is the one that is not this
/// launcher's to reshape (`shape_for`). The launcher's own callers put the
/// caller's set first; a caller that reordered them would be taking the
/// precedence back.
///
/// # Errors
///
/// [`Error::BadPath`] for a path that is, or passes through, a reparse point —
/// the variant whose own definition is "a path handed to `Sandbox::expose`
/// cannot be expressed inside the confinement", which is exactly what a link
/// whose target lies outside the exposure is. [`Error::Launch`] when a path
/// cannot be resolved at all, carrying the OS error.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Exposure};
/// use tinio_sandbox_windows::backend::canonical_exposures;
///
/// let all = canonical_exposures(vec![Exposure {
///     host: Path::new(r"E:\srv\tinio").to_path_buf(),
///     access: AccessMode::ReadWrite,
/// }])?;
/// assert_eq!(all.len(), 1);
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn canonical_exposures(exposures: Vec<Exposure>) -> Result<Vec<Exposure>> {
    let canonical = exposures
        .iter()
        .map(canonical_entry)
        .collect::<Result<Vec<_>>>()?;
    // Merged again, and this is the merge that matters: two spellings of one
    // directory are two entries until they are resolved, and one directory with
    // two ACEs is a rollback that has to remove the same grant twice. The core's
    // rule, with no device files behind it — what this function is handed is a
    // resolved exposure set and nothing else.
    Ok(merge(canonical))
}

/// One path in the form the volume names it, with the reparse-point refusal in
/// front of the resolution.
///
/// Refusing first is the order that matters, and it is the reason this is a
/// function rather than a line in [`canonical_exposures`]: `std::fs::canonicalize`
/// *follows* a link, so a check after it would be a check on the target rather
/// than on the spelling that resolved to it.
///
/// # Errors
///
/// [`Error::BadPath`] for a path that is, or passes through, a reparse point, and
/// [`Error::Launch`] when it cannot be resolved at all.
fn canonical_host(path: &Path, access: AccessMode) -> Result<PathBuf> {
    refuse_reparse_points(path)?;
    // The `\\?\`-prefixed verbatim form is what `std::fs::canonicalize`
    // returns on Windows, and `icacls` accepts it (measured on this host,
    // 2026-09-19) — which is what lets the granted spelling *be* the resolved
    // one instead of a second spelling of it.
    let host =
        fs::canonicalize(path).map_err(|source| launch("resolve an exposed path", path, source))?;
    // `info!`: this set is the "to what" half of shared §7's question. An operator
    // whose server cannot reach a directory reads those lines to see which
    // paths the run was actually about to grant, in the spelling the volume
    // resolves them to.
    tracing::info!(
        step = "resolve an exposed path",
        path = %host.display(),
        access = ?access
    );
    Ok(host)
}

/// One exposure re-spelled in the form the volume names it.
///
/// **The one place an [`Exposure`] is rebuilt**, and the reason it is a function:
/// the caller's set and the granted set are both built from it, and a field added
/// to the type has to reach both. Copied into each loop, a field the second forgot
/// would leave the two sets describing different entries.
///
/// The directory question is **not** asked here — [`canonical_caller`] asks it
/// because a caller's file is a refusal and an exposure's is not, so it stays at
/// the call site that has an answer for it.
fn canonical_entry(exposure: &Exposure) -> Result<Exposure> {
    Ok(Exposure {
        host: canonical_host(&exposure.host, exposure.access)?,
        access: exposure.access,
    })
}

/// Refuse `path` when it, or any directory above it, is a reparse point.
///
/// The whole chain rather than the last component alone: `E:\link\tinio` names a
/// directory whose *parent* is a junction, and a grant on it lands wherever that
/// junction points. `symlink_metadata` is the call that reports on the link
/// itself rather than on its target, which is the property this check needs and
/// the reason a plain `metadata` would not do.
///
/// **The refusal emits its own `error!`, because it is the only site that builds
/// this `Error`** (shared §7's rule, and the phase-basis correction that moved
/// the builder this used to share with the other launcher failures into the
/// core): a refused reparse point is a security decision, and an operator has to
/// be able to see it in a log rather than infer it from a startup failure. The
/// fields are the core's — `step`, `path`, `error` — and the event is not routed
/// through `launch`, which builds [`Error::Launch`] and would put a second line in
/// the log for one failure.
///
/// # Errors
///
/// [`Error::BadPath`] naming the component that is a reparse point.
fn refuse_reparse_points(path: &Path) -> Result<()> {
    for component in path.ancestors() {
        // A component that cannot be read is not this check's business: the
        // resolution that follows reports it, with the OS error attached.
        let Ok(metadata) = fs::symlink_metadata(component) else {
            continue;
        };
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            // **Hand-written for the same reason the caller's-file refusal is**:
            // the `Error::BadPath` below carries the component as its `path`, and
            // `fail`/`refused` cannot emit that field — the error has to exist
            // before `refused` could be handed it, and `fail` would either drop
            // the path or turn this into a `Launch`.
            let error = Error::BadPath {
                path: component.to_path_buf(),
                reason: "is a reparse point (a junction or a symlink), and a grant through it would land on its target rather than inside the exposed tree",
            };
            tracing::error!(
                step = "resolve an exposed path",
                path = %component.display(),
                error = %error,
                "sandbox step failed"
            );
            return Err(error);
        }
    }
    Ok(())
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
/// The access is rendered with `Debug` rather than `Display`, because
/// [`AccessMode`] has no `Display` impl: it is a two-variant policy, not a
/// user-facing string, and nothing reads this line as a value.
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
/// assert!(describe(&one).contains("srv"), "{}", describe(&one));
/// assert!(describe(&one).contains("ReadOnly"), "{}", describe(&one));
/// assert_eq!(describe(&[]), "");
/// ```
pub fn describe(exposures: &[Exposure]) -> String {
    exposures
        .iter()
        .map(|exposure| format!("{} ({:?})", exposure.host.display(), exposure.access))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The served root: the first exposure the caller gave.
///
/// The core's builder keeps the caller's order and `serve` exposes the root
/// before anything else, so the first entry is the root by construction.
///
/// **The refusal is the core's [`fail`]**, which is [`refused`] over
/// [`Error::Step`]: one `error!` event with the core's fields and one error, so the
/// `step` in the log and the `step` in the error cannot drift. An empty exposure
/// set is a caller's mistake rather than a platform's shortcoming, which is why the
/// variant is `Step` and the step is `confine` rather than `Unsupported` (design
/// §11) — and why this site, unlike the two above it, has no event of its own to
/// write.
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
/// [`refused`]: crate::_core::refused
/// [`Error::Step`]: crate::_core::Error::Step
pub fn served_root(sandbox: &Sandbox) -> Result<PathBuf> {
    sandbox
        .exposures()
        .first()
        .map(|exposure| exposure.host.clone())
        .ok_or_else(|| fail("confine", io::Error::other("nothing exposed to confine to")))
}

/// One enumeration of a program directory's sibling DLLs, and the three answers
/// every caller derives from it.
///
/// **The type exists because three consumers must agree about one file set** —
/// the exposure vector `backend::exposures` builds, the banner the launcher logs,
/// and the `*.dll` grant — and each of them used to read the directory for
/// itself. The cost of that was measurable rather than theoretical: the measured
/// program directory in this workspace is `target/debug/deps`, **5,129 entries**,
/// and `confine` enumerated it twice on every launch (once through `exposures`,
/// once through the other reader's pattern) *plus* a `symlink_metadata` on each row
/// of that directory. Two enumerations are also two chances to disagree, which is
/// the failure the shared batch exists to prevent.
///
/// **One scan, then the pattern and the files are the same set by construction**:
/// [`Self::pattern`] asks this enumeration whether the directory holds a DLL
/// rather than re-reading it, so the grant that names `*.dll` and the exposure
/// entries that name each file cannot describe two different directories.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::backend::SiblingScan;
///
/// // A directory that does not exist holds no sibling, so there is no pattern —
/// // and the grant knows to write nothing rather than invoke the tool.
/// let scan = SiblingScan::scan(Path::new(r"E:\build"));
/// assert!(scan.is_empty());
/// assert!(scan.pattern().is_none());
/// ```
pub struct SiblingScan {
    dir: PathBuf,
    files: Vec<PathBuf>,
}

impl SiblingScan {
    /// Enumerate `dir` once.
    pub fn scan(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            files: sibling_dlls(dir),
        }
    }

    /// Whether the directory holds no sibling at all.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// The one path a batch of these DLLs is granted through, or `None` when
    /// there is nothing to grant.
    ///
    /// **The pattern is the batch mechanism, and it is the only one the tool
    /// has.** `icacls` takes a single `name` pattern and no second path —
    /// measured 2026-09-21 on this host: a two-path invocation exits `87 Invalid
    /// parameter` naming the second path and writes nothing, a comma-joined pair
    /// exits `123`, and the flags-first order is refused outright. The pattern is
    /// matched by the *tool* against the directory, so one invocation still
    /// covers every DLL directly inside it, and it is also what makes the
    /// rollback one invocation rather than a list.
    ///
    /// **An unmatched pattern is an error and not a no-op** (measured the same
    /// day: `icacls <dir>\*.dllx /grant:r …` exits `2` `The system cannot find
    /// the file specified.`), which is why this answers `None` for a directory
    /// with no sibling rather than handing the caller a pattern that grants
    /// nothing: [`crate::acl::Grants`] would then record a grant that never landed, and its
    /// rollback would report a failure for a path it never wrote.
    pub fn pattern(&self) -> Option<PathBuf> {
        if self.is_empty() {
            return None;
        }
        // **The base is `self.dir` itself, and the parent is never an answer.**
        // The tool matches the wildcard against the directory the pattern names —
        // it resolves no base of its own — so a pattern built one level up grants
        // the parent's DLLs and misses the program's own, which is the silent
        // `0xC0000135` this rule exists to close. A path whose final component is
        // a real name always has a parent, verbatim `\\?\` prefix or not, so
        // there is nothing to fall back for.
        Some(self.dir.join("*.dll"))
    }

    /// The files this scan found, in the order every caller sees them.
    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    /// The directory the scan is of, for the callers that name it in a log line.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// The `*.dll` files directly inside `dir`, in a stable order.
///
/// **Directly inside, and no deeper**, because that is the boundary the decision
/// drew rather than the one a loader has: a DLL in a subdirectory of the program is
/// not a sibling and is not granted, and a binary that needs one is a case the
/// design records as a consequence of the rule.
///
/// The suffix test is on the file name and is case-insensitive, because Windows
/// does not distinguish `.dll` from `.DLL` and a `dll` the loader finds is a DLL
/// that has to be granted. **The name is tested before the entry is resolved or
/// stat'd**, so `is_file` runs on the survivors rather than on the whole
/// directory. A directory that `read_dir` cannot open holds no entries
/// this function can name, which is why the error is not propagated: the caller's
/// next step is the resolution that reports a path it cannot reach, and a second
/// error here would replace it.
///
/// The order is sorted so that what the caller derives from it — the exposure
/// vector, the log line, the batched `icacls` invocation — does not depend on the
/// filesystem's enumeration order.
///
/// **A caller that needs both a list and a pattern wants [`SiblingScan`]**, which
/// reads the directory once for both; this is the enumeration alone, and it is
/// public for the tests that assert the boundary rule against a real directory.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::backend::sibling_dlls;
///
/// assert!(sibling_dlls(Path::new(r"E:\build")).is_empty());
/// ```
pub fn sibling_dlls(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .filter(|entry| {
            Path::new(&entry.file_name())
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("dll"))
        })
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();
    files.sort();
    files
}
