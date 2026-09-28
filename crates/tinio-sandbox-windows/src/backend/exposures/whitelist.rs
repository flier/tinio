//! The caller's resolved set, the launch whitelist, and the spelling the ACEs use.

use std::{
    fs,
    path::{Path, PathBuf},
};

use super::siblings::SiblingScan;
use crate::{
    _core::{AccessMode, Error, Exposure, Result, Sandbox, describe_exposures_iter, launch},
    acl::Target,
};

/// **The caller's own exposure set, in the volume's canonical spelling — and the
/// only spelling anything downstream can be handed.**
///
/// This type exists to make one mistake unrepresentable rather than testable: the
/// answer `shape_for` asks — "did the *caller* expose this path?" — is asked
/// against the set [`canonical_whitelist`] resolved — the [`Whitelist::caller`]
/// half, which is the whole of what `backend::tests` reaches for — and a call
/// site that passed the raw set instead would compile and pass every existing
/// test while shaping a caller's
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
/// **A caller's file is refused where this set is built, and that is the site
/// that knows the entries are the caller's** (P7, Ruling 95): `confine`'s per-path
/// loop grants the `(OI)(CI)`/plain directory forms, and the only shape that lands
/// on a file is [`Grants::apply_siblings`]' batched `*.dll` pattern — the
/// launcher's own entries, added by [`exposures`] and never handed to this type.
/// A caller's file would therefore be granted nothing at all while the launch
/// continued, so [`canonical_whitelist`] refuses the entry instead
/// ([`Error::BadPath`]; design §6 owns the rule): a loud startup failure rather
/// than a confined server that cannot read a path the operator exposed.
///
/// **And the check is not "is this path a file?" — it is "is this path not a
/// directory?"**: a caller's exposure names a tree, the grant is specified per
/// path as a whole, and anything that is not a directory is a path this launcher
/// cannot grant the way the caller asked.
///
/// [`Grants::apply_siblings`]: crate::acl::Grants::apply_siblings
#[derive(Debug)]
pub(in crate::backend) struct CallerSet {
    /// The caller's entries, resolved by [`canonical_whitelist`] and checked to be
    /// directories. Private, which is what makes the raw set unreachable: the
    /// accessor below can only hand back a slice of *this* set, so no reader can
    /// end up holding `sandbox.exposures()` and believing it is this.
    entries: Box<[Exposure]>,
}

impl CallerSet {
    /// **The one constructor, and it is total over what it accepts**:
    /// [`canonical_whitelist`] has already resolved and checked the entries, so
    /// this holds the invariant rather than re-establishing it.
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
    pub(in crate::backend) fn entries(&self) -> &[Exposure] {
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
    pub(in crate::backend) fn empty() -> Self {
        Self {
            entries: Box::default(),
        }
    }

    /// Whether the caller exposed `path` itself, in the spelled form the volume
    /// resolves it to.
    ///
    /// `pub(in crate::backend)` rather than private because `backend::tests` asserts on the
    /// answer directly — the resolved spelling's membership is what the raw set
    /// could never give — while production asks it through `shape_for`.
    pub(in crate::backend) fn exposed(&self, path: &Path) -> bool {
        self.entries.iter().any(|exposure| exposure.host == path)
    }
}

/// Which grant shape an exposed directory takes.
///
/// **Two of the three shapes meet here, and the caller's precedence is the rule
/// that decides between them** (`caller_set` is the caller's exposure set in the
/// canonical spelling — [`canonical_whitelist`] — because a comparison against the
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
/// **`pub(in crate::backend)`, for its two callers and no further**: `Backend::confine`
/// asks this question once per exposed directory, and `backend::tests` is where
/// the precedence rule above is pinned without a DACL in the loop. A `pub` here
/// would hand a caller outside the crate both the question and the raw
/// spelling it must not be asked with.
pub(in crate::backend) fn shape_for(
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

/// **The refusal's sentence, shared by the event and the error it becomes** — an
/// [`Error::BadPath`] reason is a `&'static str`, so one copy serves both. What it
/// says is what was tested (*not a directory*) rather than the commonest instance:
/// a device path or anything else the volume answers without a directory takes the
/// same branch.
const NOT_A_DIRECTORY: &str = "is not a directory, and this launcher grants the caller's paths as directories (the inherited (OI)(CI) form, or the plain form on the program's own directory), which a file cannot take; expose the directory holding it";

/// **The launch's whitelist, in the two shapes its grant takes** — the entries
/// granted one by one, and the sibling DLLs granted through a single pattern.
///
/// **One resolution, two readers**: the caller's paths are walked, refused and
/// logged exactly once, and both things that need them are built from that one
/// pass — the [`CallerSet`] [`shape_for`] asks its membership question of, and the
/// resolved entries the per-path grant loop walks. An earlier shape resolved the
/// same caller paths twice, once inside the granted set's own canonicalisation and
/// once more for the caller's set alone, which cost a second ancestor walk, a
/// second `fs::canonicalize` and a second identical `info!` line per path.
///
/// **The program's directory is resolved on that same pass, and that is what
/// removes the third walk**: the launch used to canonicalise `program.parent()`
/// again for [`shape_for`]'s comparison, although the resolved form was already an
/// entry of the granted set.
///
/// **The sibling `*.dll` entries are the third part and are deliberately
/// unresolved**: [`Grants::apply_siblings`] opens each one by the name the scan
/// enumerated, so the resolution's `fs::canonicalize` and ancestor walk per DLL
/// bought nothing — while the banner and the exposure count name them, and both
/// have to stay truthful, so they are carried as entries of their own. **What is
/// not given up is the check**: the one component the resolution would have covered
/// for them — the DLL itself — is asked its own attribute before this value is
/// returned ([`refuse_a_reparse_sibling`]), which is an attribute read per DLL
/// rather than the walk and canonicalisation the pipeline used to cost, and the
/// grant asks the same question again of the handle it opens.
///
/// [`Grants::apply_siblings`]: crate::acl::Grants::apply_siblings
#[derive(Debug)]
pub(in crate::backend) struct Whitelist {
    /// The caller's entries and the program's directory, resolved and merged —
    /// what the per-path grant loop walks, in the caller's order.
    granted: Vec<Exposure>,
    /// The program directory in the volume's spelling, or `None` when the program
    /// has no directory to expose: the value [`shape_for`] compares an entry
    /// against, resolved once by [`canonical_whitelist`].
    program_dir: Option<PathBuf>,
    /// The sibling `*.dll` entries, as the scan named them: listed and counted by
    /// the banner, and each granted by handle.
    dlls: Vec<Exposure>,
    /// The caller's own half of `granted`, in the same spelling.
    caller: CallerSet,
}

impl Whitelist {
    /// The entries granted one by one: the caller's, then the program's directory.
    pub(in crate::backend) fn granted(&self) -> &[Exposure] {
        &self.granted
    }

    /// The program directory in the volume's spelling, when the program has one.
    pub(in crate::backend) fn program_dir(&self) -> Option<&Path> {
        self.program_dir.as_deref()
    }

    /// The caller's own entries, for the precedence question [`shape_for`] asks.
    pub(in crate::backend) fn caller(&self) -> &CallerSet {
        &self.caller
    }

    /// How many entries the run is about — **one per path [`Self::describe`]
    /// lists, the program's directory and the sibling DLLs included**, which is
    /// what this count has always meant and what keeps the launch's banner and its
    /// phase report about the same set.
    pub(in crate::backend) fn listed_len(&self) -> usize {
        self.granted.len() + self.dlls.len()
    }

    /// The whitelist as one line, for the launcher's own log.
    ///
    /// [`describe`]'s spelling over both halves, so the DLLs are rendered with the
    /// same words as every other entry: they are part of what the run is about, and
    /// each of them takes one ACE of its own.
    pub(in crate::backend) fn describe(&self) -> String {
        // The two halves chained rather than cloned into a third set: every entry here is
        // borrowed, and the renderer takes the chain as it stands.
        describe_exposures_iter(self.granted.iter().chain(&self.dlls))
    }
}

/// **The launch's whitelist, from one resolution of every caller path and the
/// program's directory.**
///
/// It is the production reading of [`build_exposures`]' rule — the same parts in
/// the same order — with the resolution the grant needs and the checks the
/// refusal rules require, which is why nothing here re-canonicalises a path a
/// second time. The caller's entries are checked one by one as they are resolved:
/// that is the only place that knows an entry is the caller's, and it is where a
/// caller's file is refused (P7). The program's directory is resolved on the same
/// pass and its resolved form is kept for [`shape_for`], so it is walked once
/// rather than twice.
///
/// # Errors
///
/// The same resolutions and refusals as [`canonical_exposures`], plus
/// [`Error::BadPath`] for a caller's path that is not a directory — the grant this
/// launcher writes is the directory form, and a path that cannot take it is a
/// silent no-op, so the entry is refused loudly instead of being granted nothing —
/// and for a sibling DLL that is itself a reparse point
/// ([`refuse_a_reparse_sibling`]).
pub(in crate::backend) fn canonical_whitelist(
    sandbox: &Sandbox,
    siblings: Option<&SiblingScan>,
) -> Result<Whitelist> {
    let parts = super::build_exposures(sandbox, siblings);
    let mut granted = Vec::with_capacity(parts.caller.len() + 1);
    for exposure in &parts.caller {
        granted.push(checked_caller_entry(canonical_entry(exposure)?)?);
    }
    // The program's directory, resolved on this same pass — and its resolved
    // spelling kept, which is what `shape_for` compares against: the entry the
    // loop sees and the value the comparison uses are one resolution, not two.
    let program_dir = match &parts.program {
        Some(program) => {
            let resolved = canonical_entry(program)?;
            let dir = resolved.host.clone();
            granted.push(resolved);
            Some(dir)
        }
        None => None,
    };
    // **A sibling that is itself a reparse point is refused, on this same pass**
    // (see [`refuse_a_reparse_sibling`]): the walk the caller's paths and the
    // program's directory take covers the DLLs' shared ancestors, so the one
    // component it cannot cover is asked here — one attribute read per DLL, and
    // the DLLs still never enter the resolution or the merge.
    for dll in &parts.dlls {
        super::reparse::refuse_a_reparse_sibling(&dll.host)?;
    }
    // The caller's half is copied once — a handful of `PathBuf`s against the walks
    // and syscalls the single resolution saves — because [`CallerSet`] owns its own
    // entries and deliberately hands out no slice a reader could mistake for the
    // raw set, while the grant vector needs the same entries in the same order.
    let caller = CallerSet::from_exposures(granted[..parts.caller.len()].to_vec());
    Ok(Whitelist {
        caller,
        granted: super::merge(granted),
        program_dir,
        dlls: parts.dlls,
    })
}

/// The caller's resolved entry, or the refusal a path that is not a directory
/// earns.
///
/// **This is where a caller's file is refused** (P7, Ruling 95), and it is a
/// function because the two facts are one decision: the entry has just been
/// resolved by [`canonical_entry`], and the only thing that knows it is the
/// caller's is the loop in [`canonical_whitelist`] that walks the caller's own
/// half. A file among them is the case nobody would notice — the per-path grant
/// loop writes the directory forms only, so the path would be granted nothing
/// while the launch carried on — so it is refused loudly instead.
///
/// # Errors
///
/// [`Error::BadPath`] with [`NOT_A_DIRECTORY`] for an entry whose resolved form is
/// not a directory.
fn checked_caller_entry(resolved: Exposure) -> Result<Exposure> {
    if resolved.host.is_dir() {
        return Ok(resolved);
    }
    // **This event is built here rather than through the core's
    // `refused`/`fail`, and it has to be**: what it becomes is an `Error::BadPath`,
    // which carries a `path` beside its `reason`, and neither builder can carry
    // one — `fail` would drop the path and `refused` would need the error built
    // first. §7 names `path` as a field an event carries, so the field is worth
    // the hand-written event.
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
    Err(Error::BadPath {
        path: resolved.host,
        reason: NOT_A_DIRECTORY,
    })
}

/// The exposure set as the volume names it, with every reparse point refused.
///
/// **Why this is a step of its own rather than part of `exposures`.** Every
/// writer resolves a link that is not the last component — measured on this host
/// 2026-09-27 with `icacls /grant:r` on `…\hop\tinio`, which wrote the ACE on
/// `…\real\tinio` — so a junction or a directory symlink anywhere above a
/// path's last component puts the ACE on whatever the link resolves to, a
/// directory outside the exposure, and inheritance then carries the same ACE into
/// that directory's own children. Following the link
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
    // two ACEs for one SID is one grant the read reports as already there for
    // ever after. The core's rule, over a set that is resolved and nothing else.
    Ok(super::merge(canonical))
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
    super::reparse::refuse_reparse_points(path)?;
    // The `\\?\`-prefixed verbatim form is what `std::fs::canonicalize`
    // returns on Windows, and every writer accepts it: `icacls` was measured
    // doing so on this host 2026-09-19, and the grant's own
    // `CreateFileW(…, FILE_FLAG_OPEN_REPARSE_POINT, …)` on a verbatim directory
    // read and re-ACLed one on 2026-09-27 — which is what lets the granted
    // spelling *be* the resolved one instead of a second spelling of it.
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
/// The directory question is **not** asked here — [`checked_caller_entry`] asks it
/// because a caller's file is a refusal and a launcher's own entry is not, so it
/// stays at the call site that has an answer for it.
fn canonical_entry(exposure: &Exposure) -> Result<Exposure> {
    Ok(Exposure {
        host: canonical_host(&exposure.host, exposure.access)?,
        access: exposure.access,
    })
}
