//! The one containment rule: does this path lie inside that root, as the
//! volume that will serve it resolves the two?
//!
//! [`path_contains`] is the *definer* of the question, and it is the only
//! one. It has four readers, and they are the places a wrong answer is
//! expensive:
//!
//! - the filesystem backend's read-only state-dir guard (`FR-023`,
//!   decision 17a) — an in-root state directory silently writes the very
//!   tree the mode promises to leave alone;
//! - the server launcher's exposure set (design §9/§10) — a state
//!   directory judged outside the root is granted a read-write entry, and
//!   one judged outside a **read-only** root is a hole in the exposure
//!   set rather than a storage error;
//! - `tinio-config`'s discovery guard, one process earlier — the same
//!   read-only refusal at the config path, whose leaf a run has not
//!   created yet;
//! - the `xtask`'s `sqlx check`/`update` guard — a cache directory inside
//!   the scratch directory (or the reverse) would have the run regenerate
//!   and then compare against the same tree.
//!
//! **The copies this module replaced are why it exists.** While each
//! reader carried its own comparison, the Windows case folding below lived
//! in one of them only, and the other compared components literally — so on
//! NTFS a state directory spelled in another case (`E:\data\.tinio`
//! against a root given as `E:\Data`, and both of those spellings of one
//! directory) read as *outside*, and the guard that exists to catch
//! exactly that mistake let it through. A rule with two owners is a rule
//! with one owner's bugs; the fix is one definer, called by every reader,
//! and no reader keeps a copy of it now.
//!
//! The resolution step is the other half of the same defect: a guard comparing the spelling a path
//! was *written* with answers `false` for a state directory spelled through a symlink, so the
//! resolved spelling is compared beside it. **The pairings are not the caller's decision any more**
//! — the four-pairing rule is [`contains_under_any_spelling`], which every guard calls, and the
//! resolution behind it is private: a public half-step was an invitation to compose a fifth
//! pairing set, which is the divergence this module exists to end. `tinio-config` was the last call
//! site to stop composing its own, on 2026-09-26, when the `tinio-util` edge landed and its private
//! copy went with it.

#[cfg(windows)]
use std::{
    borrow::Cow,
    ffi::OsString,
    os::windows::ffi::{OsStrExt, OsStringExt},
};
use std::{
    fs::canonicalize,
    path::{Component, Path, PathBuf},
};

/// Whether `candidate` lies inside `root`, the way the serviced volume
/// sees it.
///
/// The comparison is **component-wise, never a string prefix**: `…\data2`
/// is a sibling of `…\data`, not inside it, and a rule that searched the
/// text would say otherwise.
///
/// **`Path::starts_with` is not that rule, on either arm**, and the
/// distinction is worth stating because the name invites the mistake:
/// it iterates [`std::path::Component`]s, so `Path::new("data2/x")`
/// does *not* start with `Path::new("data")`. The unix arm below is
/// therefore this function's own semantics rather than a weaker
/// approximation of them — which was verified by running it rather than
/// by reading it (WSL2, 2026-09-23: the `not(windows)` arm had never
/// executed on the development host, and `data2` is the case both arms
/// assert).
///
/// On Windows it is **case-insensitive**: NTFS resolves `D:\Temp\…` and
/// `d:\temp\…` to the same directory while [`Path::starts_with`] compares
/// components literally, so a differently-cased in-root path is exactly
/// the mistake a containment guard exists to catch. (The literal
/// comparison folds a `Prefix` — a drive letter differing in case alone
/// is `true` either way — so what it gets wrong is a case difference past
/// the prefix, and a verbatim root against a plain candidate: both are
/// measured `false`.) Everywhere else it is case-sensitive, exactly as
/// [`Path::starts_with`] compares: unix filesystems distinguish
/// `…/Data` from `…/data`.
///
/// **On Windows either spelling of one path compares equal, on both
/// sides**, because the two producers of these paths disagree about the
/// shape: `std::fs::canonicalize` answers a verbatim path (`\\?\D:\data`,
/// or `\\?\UNC\srv\share\data` for a UNC root) while
/// `std::path::absolute` answers the plain form (`D:\data`,
/// `\\srv\share\data`). **The respelling is the Windows-only
/// `plain_spelling` below, and it reads the UTF-16 units rather than
/// `to_str`**, so a path whose bytes are not valid Unicode is respelled too
/// — the pairing is for *either spelling of one path*, and a path that is
/// not text has two spellings as much as any other. The `to_str` version
/// this replaced answered "no plain form" for exactly such a path, which
/// left its two spellings unpaired — the pairing below is where that showed
/// up.
///
/// **Respell both, and that is a correction rather than symmetry.** The
/// rule first respelled only the root, on the argument that only the root
/// has those two producers. The argument is wrong about the input that
/// matters most here: the *candidate* is an operator's `TINIO_STATE_DIR`
/// (FR-023) as they wrote it, and an operator who pastes a verbatim path
/// into that variable — the very form `canonicalize` answers, so the one
/// a shell completion or a log line hands back — produces a candidate with
/// no plain form on the other side of the comparison. Measured: `\\?\E:\data`
/// against a plain root `E:\data` answered `false`, which is a read-write
/// state directory granted inside a read-only root — the same defect this
/// rule exists to close, one spelling further along.
///
/// Widening is the safe direction for a containment test: a `true` here
/// refuses (or skips an extra exposure), and a `false` is what grants the
/// hole. So all four pairings are tried, and the cost is bounded at four
/// component lists of a handful of entries each.
///
/// A path that starts with itself is contained (the comparison includes
/// equality), so a state directory equal to the root is an in-root one.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_util::path::path_contains;
///
/// assert!(path_contains(Path::new("data/state"), Path::new("data")));
/// assert!(
///     path_contains(Path::new("data"), Path::new("data")),
///     "a path starts with itself, so the root is its own in-root case"
/// );
/// // Component-wise, never a string prefix: `data2` merely starts with the root's
/// // name, so it is a sibling rather than a child.
/// assert!(!path_contains(Path::new("data2/state"), Path::new("data")));
/// ```
pub fn path_contains(candidate: &Path, root: &Path) -> bool {
    #[cfg(not(windows))]
    {
        components(candidate).is_within(&components(root))
    }
    #[cfg(windows)]
    {
        // Bound rather than inlined: each plain spelling is a borrow of its `Cow` (owned
        // only for the paths the strip changed), and both pairing sets borrow them.
        let candidate_plain = plain_spelling(candidate);
        let root_plain = plain_spelling(root);
        // A path with no verbatim prefix *is* its plain spelling, and the `Cow` hands that
        // path back unchanged, so that side contributes one spelling rather than the same
        // one twice. The `Option` this replaced deduplicated the same way, and answered
        // "no plain spelling" for a path whose bytes are not valid Unicode as well — which
        // is the pairing that was missing.
        let candidates = [
            Some(candidate),
            (candidate_plain.as_ref() != candidate).then_some(candidate_plain.as_ref()),
        ];
        let roots = [
            Some(root),
            (root_plain.as_ref() != root).then_some(root_plain.as_ref()),
        ];
        // The root's components are folded once rather than per candidate: the pairings below
        // cross two spellings of each side, so a root rebuilt inside the inner closure is the
        // same list computed up to twice over.
        let roots: Vec<_> = roots
            .iter()
            .flatten()
            .map(|root| lower_components(root))
            .collect();
        candidates.into_iter().flatten().any(|candidate| {
            let candidate = lower_components(candidate);
            roots.iter().any(|root| candidate.is_within(root))
        })
    }
}

/// `path`'s resolved spelling: its nearest existing ancestor canonicalized,
/// with the components below that ancestor joined back on. `None` when
/// nothing along the path resolves at all.
///
/// **The ancestor walk is what makes this reachable, and the leaf is where
/// the first spelling of it failed.** The paths that need a second spelling
/// are leaves a run has not created yet — a state directory, the config file
/// inside one — so [`canonicalize`] on the path itself answers `ENOENT` for
/// exactly the shape the guards see, and a version that resolved the leaf
/// answered `false` for the exploit it was written to catch (measured on
/// macOS 15.8, 2026-09-25, in the launcher's copy of this rule). The
/// components below the nearest existing ancestor cannot be links — a
/// symlink is a component of a path that exists — so the joined answer names
/// the same directory the leaf would.
///
/// **This is the resolution and not the comparison.** The rule a resolved
/// spelling feeds is [`path_contains`]'s, and comparing it widens which
/// *spellings* of one path a guard accepts, never which paths one contains:
/// `None` here contributes nothing, so the literal answer the caller already
/// had stands, and a path that does not exist cannot be inside an existing
/// root through a symlink.
///
/// **Which pairings get compared is the caller's, because the callers
/// differ.** The read-only guards compare two — the candidate as written and
/// this resolution of it — while the launcher, which receives both paths as
/// written, compares four, substituting each side's resolved spelling in
/// turn. That difference is why this step is public on its own rather than
/// wrapped in one containment function: a shared wrapper would have to pick
/// one pairing set and serve it to a caller that needs the other.
///
/// **The rejoin is in the written order, and that is a correction this
/// module carries rather than a detail.** Each private copy of this rule
/// this module replaced collected the components below the ancestor and
/// *reversed* them before pushing, so `…/state/absent/meta.redb` came back
/// as `…/state/meta.redb/absent` (measured on this host, 2026-09-26, by
/// `an_absent_leaf_joins_onto_its_nearest_existing_ancestor` below).
/// Containment survives a permutation while the suffix holds only names —
/// every spelling still sits under the same existing ancestor — and stops
/// surviving it as soon as the suffix holds a `..`: with the root spelled
/// through a symlink, a state directory written
/// `…/link/x/y/../../.tinio` resolves to `<root>/.tinio` in the written
/// order (in-root, refused) and to a directory beside the root in the
/// reversed one, so the caller that compares with [`path_contains`] — whose
/// lexical resolution is what pops the `..` — answers **served**: the
/// writable subtree inside a read-only root this module exists to close.
/// The `..`-bearing leg is
/// `a_rejoin_in_the_written_order_keeps_an_in_root_state_directory`.
///
/// **A prefix comparison does not see that permutation, and saying so keeps
/// the claim exact.** `Path::starts_with`, which `tinio-config`'s guard
/// compared with before it was widened to the shared rule, never lets a `..`
/// escape the prefix it matched, so that form answered in-root for every
/// `..`-bearing spelling probed on macOS 15.8, 2026-09-26 — and the refusal a
/// full `tinio server --read-only` run prints for those spellings still comes
/// from the config guard before either `path_contains` guard is reached,
/// because discovery runs first.
///
/// The walk is bounded by the path's own depth, and the suffix is iterated
/// rather than collected: `ancestors` yields the path first and then its
/// prefixes, so the first ancestor that canonicalizes is the nearest one,
/// and the components below it are pushed in the order they are written.
fn resolved_path(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        // The path itself is tried first, and fails for the absent-leaf case
        // above; the first `Ok` is the nearest existing ancestor.
        let Ok(base) = canonicalize(ancestor) else {
            continue;
        };
        // `Some` by `ancestors`' own contract; a shape that broke it would
        // skip this ancestor rather than answer `None` for the whole walk.
        let Some(below) = path.strip_prefix(ancestor).ok() else {
            continue;
        };
        let mut resolved = base;
        for component in below.components() {
            resolved.push(component.as_os_str());
        }
        return Some(resolved);
    }
    None
}

/// Whether `candidate` is at or below `root` **under any spelling of either side** — the one
/// containment rule every guard in this workspace asks — the one public spelling of it, with the
/// resolution behind it private so nobody composes a different pairing set again.
///
/// **Four pairings, and the widening is the safe direction.** The literal pair; the resolved
/// candidate against the literal root; the literal candidate against the resolved root; both
/// resolved. A `true` refuses (or skips an exposure) and a `false` is what grants a read-write
/// state directory inside a read-only root, so a spelling this misses is a hole rather than a
/// nuisance: measured on macOS 15.8, 2026-09-25, a state directory spelled `/tmp/<root>/.tinio`
/// under the canonical root `/private/tmp/<root>` compared as unrelated and the run served a
/// writable subtree inside a read-only root.
///
/// **A root that is already canonical is answered by the two-pairing form**, and the body returns
/// that answer early rather than computing the other two: on an existing canonical path the
/// resolution is the identity, so the last two pairings repeat the first two, and two of the three
/// callers hand in exactly that root. The extra pairings — and the extra walk — are what a caller
/// handed **both** paths as written needs.
///
/// **The resolved arm is load-bearing for the shapes the guards see.** The paths that need a
/// second spelling are leaves a run has not created yet (a state directory, the config file inside
/// one), which is why `resolved_path` walks to the nearest existing ancestor instead of
/// canonicalizing the leaf, and why it rejoins the suffix in written order: a `..` below that
/// ancestor resolves to a directory beside the root under a reversed rejoin, which was measured as
/// the served writable subtree this rule exists to close. `None` from either resolution
/// contributes nothing, so the literal answer stands and a path that does not exist cannot leak
/// through a symlink either.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_util::path::contains_under_any_spelling;
///
/// // The literal pair, and an operator's own spelling of the same directory: both
/// // name a state directory inside the root.
/// assert!(contains_under_any_spelling(
///     Path::new("data/state"),
///     Path::new("data")
/// ));
/// assert!(contains_under_any_spelling(
///     Path::new("./data/.tinio"),
///     Path::new("data")
/// ));
/// // The widening is over spellings of one path, never over which paths are inside:
/// // a sibling stays outside.
/// assert!(!contains_under_any_spelling(
///     Path::new("data2/state"),
///     Path::new("data")
/// ));
/// ```
pub fn contains_under_any_spelling(candidate: &Path, root: &Path) -> bool {
    // Bound rather than inlined: both resolved forms are owned, and every comparison below borrows
    // a side. `None` is "nothing along this path resolves" rather than "resolve it again" — a path
    // with no resolvable ancestor keeps the literal answer it already had.
    let resolved_candidate = resolved_path(candidate);
    let resolved_root = resolved_path(root);
    contained(
        candidate,
        root,
        resolved_candidate.as_deref(),
        resolved_root.as_deref(),
    )
}

/// **Both directions over one pair of resolutions** — the form for a caller that has to ask the
/// question both ways about one pair of paths.
///
/// The containment question is symmetric in its arguments but the resolution is not: asking it
/// through [`contains_under_any_spelling`] twice resolves each path twice, so a guard that
/// refuses "either inside the other" pays four ancestor walks where two decide both directions.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_util::path::contains_under_any_spelling_either_way;
///
/// assert_eq!(
///     contains_under_any_spelling_either_way(Path::new("data/state"), Path::new("data")),
///     (true, false)
/// );
/// ```
pub fn contains_under_any_spelling_either_way(a: &Path, b: &Path) -> (bool, bool) {
    let resolved_a = resolved_path(a);
    let resolved_b = resolved_path(b);
    (
        contained(a, b, resolved_a.as_deref(), resolved_b.as_deref()),
        contained(b, a, resolved_b.as_deref(), resolved_a.as_deref()),
    )
}

/// The pairings, over resolutions the caller already holds — the body both public forms share.
fn contained(
    candidate: &Path,
    root: &Path,
    resolved_candidate: Option<&Path>,
    resolved_root: Option<&Path>,
) -> bool {
    // The two pairings that never involve the root's own resolution: the candidate as the caller
    // spelled it, and the candidate resolved, each against the root the caller named.
    let literal = path_contains(candidate, root)
        || resolved_candidate.is_some_and(|candidate| path_contains(candidate, root));
    // The canonical-root case, because it is the common one and `literal` is already the whole
    // answer: `None` means the root has no resolvable ancestor (so the last two pairings
    // contribute nothing), and `Some(root)` means the resolution was the identity (so they repeat
    // `literal`). Either way the walks below are skipped.
    if resolved_root.is_none_or(|resolved| resolved == root) {
        return literal;
    }
    literal
        || resolved_root.is_some_and(|root| path_contains(candidate, root))
        || resolved_candidate
            .zip(resolved_root)
            .is_some_and(|(candidate, root)| path_contains(candidate, root))
}

/// The plain (non-verbatim) spelling of `path` — **the strip runs on the
/// UTF-16 units, never on a `&str`.**
///
/// `\\?\E:\data` becomes `E:\data`, and `\\?\UNC\srv\share\data` becomes
/// `\\srv\share\data`. The two spellings are one directory, and the
/// producers of these paths disagree about which one names it:
/// [`canonicalize`] answers the verbatim form while
/// `std::path::absolute`, an operator writing `TINIO_STATE_DIR` by hand,
/// and any log line or shell completion that hands a path back answer the
/// plain one. A path with no verbatim prefix is borrowed back unchanged,
/// which is the whole of the "already plain" case — [`Cow::Borrowed`] is
/// the caller's own path rather than a copy of it.
///
/// **A failed [`Path::to_str`] is not a reason to skip the strip.** A
/// Windows path is arbitrary UTF-16, so the path behind a verbatim prefix
/// need not be text at all: `\\?\E:\data\<U+D800>` and `E:\data\<U+D800>`
/// are one directory, and a strip that read only what `to_str` could read
/// left them two spellings — one directory under two names. Both prefixes
/// are ASCII, which is what makes a comparison on a unit stream meaningful
/// in the first place.
///
/// **This is the rule's one home.** It was written twice — the strip here
/// read `to_str` and answered "no plain form" for a path that is not text,
/// so [`path_contains`] never paired the two spellings of such a path,
/// while the Windows sandbox crate's private copy stripped them on the
/// units — and the two policies disagreed exactly where it costs most,
/// because the sandbox derives one root's AppContainer profile name from
/// the answer: two names for one directory is two profiles (its decision
/// 21). A rule with two implementations has two policies, which is why the
/// copy lives here now.
///
/// # Examples
///
/// ```
/// use std::{
///     borrow::Cow,
///     ffi::OsString,
///     os::windows::ffi::OsStringExt,
///     path::{Path, PathBuf},
/// };
///
/// use tinio_util::path::plain_spelling;
///
/// assert_eq!(
///     plain_spelling(Path::new(r"\\?\E:\data")).as_ref(),
///     Path::new(r"E:\data"),
/// );
/// assert_eq!(
///     plain_spelling(Path::new(r"\\?\UNC\srv\share\data")).as_ref(),
///     Path::new(r"\\srv\share\data"),
/// );
/// // A path that is already plain comes back borrowed rather than copied.
/// assert!(matches!(
///     plain_spelling(Path::new(r"E:\data")),
///     Cow::Borrowed(_)
/// ));
///
/// // The units, and not `to_str`: this path is a directory like any other, and the
/// // strip has to see it — one directory spelled two ways is two names for whoever
/// // derives a name from it.
/// let mut plain: Vec<u16> = r"E:\data\".encode_utf16().collect();
/// plain.push(0xD800);
/// let mut verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
/// verbatim.extend_from_slice(&plain);
/// assert_eq!(
///     plain_spelling(&PathBuf::from(OsString::from_wide(&verbatim))).as_ref(),
///     Path::new(&OsString::from_wide(&plain)),
/// );
/// ```
#[cfg(windows)]
pub fn plain_spelling(path: &Path) -> Cow<'_, Path> {
    // Both prefixes are ASCII, so comparing them as units says what comparing
    // them as text would — and the units are all a path that is not text offers.
    let verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
    let units: Vec<u16> = path.as_os_str().encode_wide().collect();
    let Some(rest) = units.strip_prefix(verbatim.as_slice()) else {
        return Cow::Borrowed(path);
    };
    let unc: Vec<u16> = r"UNC\".encode_utf16().collect();
    let stripped = match rest.strip_prefix(unc.as_slice()) {
        Some(share) => {
            let mut joined: Vec<u16> = r"\\".encode_utf16().collect();
            joined.extend_from_slice(share);
            joined
        }
        None => rest.to_vec(),
    };
    Cow::Owned(PathBuf::from(OsString::from_wide(&stripped)))
}

/// A path reduced to the names that decide containment, and whether it was
/// rooted to begin with.
///
/// **The flag is not decoration; it is what keeps an empty name list from
/// meaning everything.** Dropping separators is what makes `E:\Data` and
/// `E:\data` line up, but it also makes `E:\` and `.` both reduce to nothing —
/// and an empty list is a prefix of every list, so `.` would read as containing
/// `../other` and `E:\` would read as contained in any relative path. A rooted
/// path and a relative one are therefore never compared: the pair has to agree
/// on the flag before the names are looked at.
#[derive(Debug, PartialEq, Eq)]
struct Reduced {
    rooted: bool,
    names: Vec<String>,
}

impl Reduced {
    /// Whether `self` is at or below `root`.
    ///
    /// **Three conjuncts, and the third is the one a bare prefix test does not
    /// give you.** `self.names.starts_with(&root.names)` is `true` for *any*
    /// empty `root.names` — an empty prefix is a prefix of everything — and one
    /// shape that reduces to no names would then swallow the world: a **relative**
    /// root like `.`, which would contain `../other` and every other path the
    /// operator did not write relative to it. That shape is reachable here, and
    /// it is the reason the `rooted` conjunct cannot be left out: an absolute
    /// path is never compared with a relative one.
    ///
    /// **A rooted root is the opposite case, and the first version of this
    /// predicate got it wrong in that direction.** `E:\` and `/` also reduce to
    /// no names, and they genuinely do contain everything below them — that is
    /// what a volume root is. An earlier spelling demanded a name on the root
    /// unconditionally, which made `/` hold nothing at all (caught by the WSL2
    /// run, where `/` is the root that exists). So the emptiness test applies
    /// **only to an unrooted root**: a nameless relative root holds only a
    /// nameless path — itself, which is the equality case the callers rely on
    /// (`a state directory equal to the root is an in-root one`) — while a
    /// nameless rooted one is the root of its volume.
    fn is_within(&self, root: &Self) -> bool {
        self.rooted == root.rooted
            && self.names.starts_with(&root.names)
            && (root.rooted || !root.names.is_empty() || self.names.is_empty())
    }
}

/// `path`'s components as comparison strings, **with `.` and `..` resolved the
/// way a lexical reading allows**.
///
/// **This is the widening an operator's own spelling needs, and the hole it
/// closes is measured rather than supposed.** `TINIO_STATE_DIR=./data/.tinio`
/// against a root of `data` compares as *outside* under a raw
/// [`Path::components`] walk — `Component::CurDir` is kept rather than dropped —
/// so `serve`'s exposure set takes its `else` branch and grants that state
/// directory **read-write inside a read-only root**, which is the same defect the
/// case fold and the verbatim respelling each closed from their own direction
/// (WSL2 verification, 2026-09-23; `sandbox_request`'s and the in-root guard's
/// docs both name the rule this feeds). A `..` that stays inside the path is the
/// same class: `data/../data/.tinio` names `data/.tinio`.
///
/// **The two components are handled differently, and the difference is what
/// keeps this honest rather than merely eager:**
///
/// - `.` is dropped, because it never changes which directory a path names —
///   that is a fact about the syntax, and dropping it cannot widen the answer
///   past "these two spellings are one path".
/// - `..` pops, **but only while there is something to pop**. A `..` that would
///   climb past the path's own start is kept verbatim along with everything after
///   it, because resolving it would be guessing at a prefix this function cannot
///   see: `../data` under a root of `..` is in-root and `../data` under a root of
///   `data` is not, and only the caller knows which root it passed. Keeping it
///   means the two sides are still compared as written once the path leaves what
///   it can describe itself.
/// - A `Prefix` is never popped, so the `C:` of a Windows path survives any
///   number of leading `..`s.
///
/// The cost is bounded by the number of components, and the result is what both
/// arms already compared — this changes the words, not the rule.
fn components(path: &Path) -> Reduced {
    let rooted = path.is_absolute();
    let mut out: Vec<String> = Vec::new();
    // The `Prefix`'s own text, so the pop guard below can ask whether the last
    // element *is* the prefix rather than guessing from its spelling — the first
    // version tested `ends_with(':')` and a `..` ends with a dot, not a colon,
    // so `../other` against a root of `..` answered `true` (caught by the test
    // that exists for exactly that).
    let mut prefix: Option<String> = None;
    for component in path.components() {
        let part = component.as_os_str().to_string_lossy();
        match component {
            // **Dropped, and these two are what the first version of this
            // function got wrong.** A rooted path yields `Prefix`, `RootDir`,
            // `Normal`, … — `E:\Data` is `["E:", "\\", "Data"]` — and `RootDir`
            // is a separator, not a name: it is dropped from *both* sides, so a
            // candidate and a root that share a drive line up. Keeping it put a
            // `\` between the drive and the first real name, and every comparison
            // against a rooted root answered `false` (three tests). `CurDir` is
            // dropped for the same reason: it names the directory the path is
            // already in, so it can never change which one that is.
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                // `pop` on an empty vector is a silent `None`, which would
                // *drop* the `..` instead of keeping it; and a `Prefix` must
                // never be popped, because a path that lost its `C:` is not the
                // path it started as. A `..` that survives here stays in the
                // comparison as the `..` it is — `../other` under a root of `..`
                // is in-root, and under `data` is not, and only the caller knows
                // which root it passed.
                let last = out.last();
                // **The guard is `last is Some and is not the prefix`, spelled
                // out.** Two earlier spellings were wrong in opposite
                // directions, and both produced a real hole: `!out.is_empty() &&
                // !last.ends_with(':')` popped a `..` that *was* the only
                // component (nothing left to pop — the `..` vanished, and
                // `../../data` came out as `data`), and inverting the prefix test
                // with `is_some_and` protected a leading `..` as though it were a
                // `C:`. `Option` is where "nothing to pop" has to be decided,
                // once.
                let is_prefix = match (last, prefix.as_deref()) {
                    (Some(last), Some(prefix)) => last == prefix,
                    _ => false,
                };
                if last.is_some() && !is_prefix {
                    out.pop();
                } else {
                    out.push(part.to_string());
                }
            }
            Component::Prefix(_) => {
                prefix = Some(part.to_string());
                out.push(part.to_string());
            }
            _ => out.push(part.to_string()),
        }
    }
    Reduced { rooted, names: out }
}

/// The case fold the Windows arm compares with — the case-insensitive form of
/// the containment rule (NTFS resolves case-different names to the same
/// directory), applied to [`components`]' resolved output rather than to the
/// path directly.
///
/// A path whose bytes are not valid Unicode is folded lossily inside
/// [`components`]; NTFS cannot express two such names as distinct either, so the
/// approximation cannot hide a real in-root state dir. **The loss is a merge
/// rather than a skip, which is what makes it safe and what separates it from
/// the strip beside it**: folding two such names onto one string can only answer
/// "contained" for a path that is not, and that is the refusing direction, while
/// [`plain_spelling`] may not skip a spelling at all — a skipped spelling is one
/// directory under two names, which is the direction that grants.
#[cfg(windows)]
fn lower_components(path: &Path) -> Reduced {
    let reduced = components(path);
    Reduced {
        rooted: reduced.rooted,
        names: reduced
            .names
            .into_iter()
            .map(|part| part.to_lowercase())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, process};

    use super::*;

    /// A scratch directory this test owns and removes.
    ///
    /// `tinio-util` carries no `tempfile` dev-dependency, so the name is the
    /// mechanism: the process id keeps concurrent runs apart, the caller's
    /// suffix keeps the tests apart, and the removal makes the run repeatable
    /// (`tinio-winsec`'s unit tests hand-rolled theirs the same way, before that crate dissolved).
    fn scratch(name: &str) -> PathBuf {
        let dir = env::temp_dir().join(format!("tinio-util-path-{}-{name}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("scratch directory");
        dir
    }

    /// An existing path resolves to its canonical spelling — the prefix
    /// included, which is the whole reason the resolved spelling exists: a
    /// host's temp directory is spelled through a link or a verbatim prefix
    /// (`/var` → `/private/var` on macOS, `\\?\C:\…` on Windows), so the
    /// literal and resolved spellings of one directory agree on no prefix at
    /// all.
    #[test]
    fn an_existing_path_resolves_to_its_canonical_spelling() {
        let dir = scratch("existing");

        assert_eq!(
            resolved_path(&dir).expect("an existing directory resolves"),
            canonicalize(&dir).expect("canonicalize"),
        );

        fs::remove_dir_all(&dir).expect("clean");
    }

    /// **The `ENOENT`-leaf case, which is the bug the walk was written for.**
    /// A leaf a run has not created — `<state>/absent/meta.redb` — has no
    /// canonical spelling of its own, and the first version of this operation
    /// resolved the leaf, failed, and let the guard through. So the assertion
    /// that `canonicalize` refuses the leaf is half this test: without it the
    /// leg could pass through the leaf branch rather than the ancestor walk.
    /// The nearest existing ancestor's canonical spelling with the absent
    /// components joined back on is the answer.
    #[test]
    fn an_absent_leaf_joins_onto_its_nearest_existing_ancestor() {
        let dir = scratch("absent-leaf");
        let present = dir.join("state");
        fs::create_dir_all(&present).expect("the ancestor that exists");

        // Two components below the ancestor, so the rejoin is not a single push.
        let leaf = present.join("absent").join("meta.redb");
        assert!(
            canonicalize(&leaf).is_err(),
            "the leaf is the absent half: {leaf:?}"
        );
        assert_eq!(
            resolved_path(&leaf),
            Some(
                canonicalize(&present)
                    .expect("canonicalize the ancestor")
                    .join("absent")
                    .join("meta.redb")
            ),
        );

        // And a leaf directly inside the scratch directory, which is the shape
        // a state directory beside a served root has: the walk stops at the
        // first existing ancestor and rejoins one name.
        let shallow = dir.join("config.toml");
        assert!(canonicalize(&shallow).is_err());
        assert_eq!(
            resolved_path(&shallow),
            Some(
                canonicalize(&dir)
                    .expect("canonicalize")
                    .join("config.toml")
            ),
        );

        fs::remove_dir_all(&dir).expect("clean");
    }

    /// **Nothing to resolve is answered rather than guessed at.** The empty
    /// path is the portable shape: it is its own only ancestor and
    /// `canonicalize` refuses it on both hosts, so the walk ends with no `Ok`
    /// at all rather than with a spelling that was never checked.
    ///
    /// **No absolute path reaches this branch**, because every one of them has
    /// a resolvable ancestor wherever its volume exists — which is why the
    /// product call sites, whose paths are absolute by the time a guard sees
    /// them, had no leg for it, and why the operation had to move here to be
    /// tested at all. A path on a volume that does *not* exist would reach it
    /// too, and that shape is deliberately not asserted: naming a free drive
    /// letter is not something a hermetic test can do.
    #[test]
    fn a_path_with_nothing_to_resolve_answers_none() {
        assert_eq!(resolved_path(Path::new("")), None);
    }

    /// **The rejoin's order decides a containment answer, and getting it
    /// wrong is a fail-open.** This is the leg the reversed rejoin would lose:
    /// the root is spelled through a symlink (what macOS's `/tmp` is, and what
    /// an operator's `TINIO_STATE_DIR` carries), the suffix below the nearest
    /// existing ancestor holds absent names and a `..` that climbs back over
    /// them, and the state directory the run would create is *inside* the
    /// root. The written order resolves it there and answers in-root; a
    /// reversed rejoin walks the `..`s into the wrong names and answers
    /// outside, which is the read-write subtree inside a read-only root this
    /// module exists to refuse.
    ///
    /// The literal clause is asserted `false` first, because that is the whole
    /// reason the resolved spelling is consulted: without it the leg would pass
    /// on the first clause and prove nothing about this operation.
    #[cfg(unix)]
    #[test]
    fn a_rejoin_in_the_written_order_keeps_an_in_root_state_directory() {
        use std::os::unix::fs::symlink;

        let dir = scratch("rejoin-order");
        let real = dir.join("real");
        fs::create_dir_all(&real).expect("the served root");
        let root = canonicalize(&real).expect("canonicalize the served root");
        let link = dir.join("link");
        symlink(&real, &link).expect("symlink");

        // The exploit's shape: the root through the link, then two absent
        // names and the `..`s that climb back over them.
        let spelled = link
            .join("x")
            .join("y")
            .join("..")
            .join("..")
            .join(".tinio");
        assert!(
            !path_contains(&spelled, &root),
            "the leg is vacuous unless the literal clause answers `false`: {spelled:?} vs {root:?}"
        );
        let resolved = resolved_path(&spelled).expect("an existing ancestor resolves");
        assert!(
            path_contains(&resolved, &root),
            "the written order resolves to the in-root state directory the run would create: \
             {resolved:?} vs {root:?}"
        );

        fs::remove_dir_all(&dir).expect("clean");
    }

    /// The platform-neutral half: a candidate under the root is inside it
    /// (the root itself included), a sibling that merely shares the root's
    /// name is not, and an unrelated path is not.
    #[test]
    fn containment_is_component_wise() {
        assert!(path_contains(Path::new("data/state"), Path::new("data")));
        assert!(path_contains(
            Path::new("data/state/meta.redb"),
            Path::new("data")
        ));
        assert!(
            path_contains(Path::new("data"), Path::new("data")),
            "a path starts with itself, so the root is its own in-root case"
        );
        assert!(
            !path_contains(Path::new("data2/state"), Path::new("data")),
            "a name that merely starts with the root's is a sibling, not a child"
        );
        assert!(!path_contains(Path::new("other/state"), Path::new("data")));
        assert!(
            !path_contains(Path::new("data"), Path::new("data/state")),
            "containment is one-way: the root is not inside its own child"
        );
    }

    /// An operator's own spelling of the same directory, which is the input the
    /// rule's widest-reaching caller actually receives: `TINIO_STATE_DIR` is
    /// whatever was typed, and `./data/.tinio` and `data/../data/.tinio` both
    /// name the in-root directory `data/.tinio`. Before the resolution, each
    /// compared as *outside* — and a state directory judged outside a read-only
    /// root is granted **read-write inside it**, which is the hole the rule
    /// exists to close (WSL2 verification, 2026-09-23).
    #[test]
    fn a_lexical_spelling_is_the_same_directory() {
        assert!(path_contains(Path::new("./data/.tinio"), Path::new("data")));
        assert!(path_contains(
            Path::new("data/../data/.tinio"),
            Path::new("data")
        ));
        assert!(path_contains(Path::new("a/b/../c"), Path::new("a")));
        // The root is resolved the same way, so a `.`-prefixed root is not a
        // second, weaker answer.
        assert!(path_contains(Path::new("data/state"), Path::new("./data")));
        assert!(path_contains(
            Path::new("data/state"),
            Path::new("a/../data")
        ));
    }

    /// **The resolution has a floor, and this is where it is.** A `..` with
    /// nothing left to pop is *kept*, because resolving it would mean guessing
    /// at a prefix this function cannot see: `../data` is inside a root of `..`
    /// and outside a root of `data`, and only the caller knows which it passed.
    /// Keeping it also means the two sides are still compared as written for
    /// everything below it — `../other`'s first component really is the `..` the
    /// caller named as a root.
    ///
    /// **What the floor cannot do is resolve a climb that leaves the root**, and
    /// the assertion that says so is the point of this test rather than a
    /// limitation worked around: `../../data` against a root of `..` is `["..",
    /// "..", "data"]` against `[".."]`, and answering `true` would be claiming
    /// that `../other` is inside `other/`. Deciding it needs the real prefix —
    /// which is the canonical comparison the design records as its residual, not
    /// something a lexical rule can supply.
    #[test]
    fn a_parent_that_leaves_the_path_is_kept_rather_than_resolved() {
        assert!(
            !path_contains(Path::new("../data"), Path::new("data")),
            "a leading `..` must not be silently dropped into the root's own name"
        );
        assert!(
            !path_contains(Path::new("../../data"), Path::new("..")),
            "a second `..` climbs out of the root, and no lexical rule can say \
             where it landed"
        );
        // Kept as written, so it is still a component that can be compared.
        assert!(path_contains(Path::new("../data"), Path::new("..")));
        assert!(path_contains(Path::new("../other"), Path::new("..")));
        assert!(!path_contains(Path::new("../other"), Path::new(".")));
        // And a `..` that stays inside still resolves.
        assert!(path_contains(Path::new("a/b/../state"), Path::new("a")));
    }

    /// **A root that reduces to no names holds only itself.** An empty list is a
    /// prefix of every list, so the plain prefix test says `.` contains
    /// `../other` and that a drive root `E:\` contains every path on the drive —
    /// both reachable here, because `serve` compares an operator's root against
    /// an operator's state directory and neither is guaranteed to have a name.
    /// The equality case is kept, because the guard it feeds refuses a state
    /// directory equal to the root and a path contains itself.
    #[test]
    fn a_root_with_no_names_holds_only_itself() {
        assert!(path_contains(Path::new("."), Path::new(".")));
        assert!(!path_contains(Path::new("../other"), Path::new(".")));
        assert!(!path_contains(Path::new("data"), Path::new(".")));
    }

    /// **A rooted root that holds no names still holds what is below it**, and
    /// the platform is what decides which spelling that is: `E:\` on Windows
    /// (a prefix, and no name after it) and `/` on unix (a bare root). The
    /// distinction from the test above is the `rooted` conjunct — a name-less
    /// *relative* root holds only itself, while a name-less rooted one is the
    /// root of its volume and holds everything on it. Both are asserted per
    /// platform because the other spelling is not even a rooted path there
    /// (WSL2 caught this: `E:\` is a relative path on Linux, so an ungated
    /// assertion about it failed for the wrong reason).
    #[test]
    fn a_rooted_root_with_no_names_holds_what_is_below_it() {
        #[cfg(windows)]
        let (root, below) = (Path::new(r"E:\"), Path::new(r"E:\Data"));
        #[cfg(not(windows))]
        let (root, below) = (Path::new("/"), Path::new("/data"));

        assert!(path_contains(root, root));
        assert!(
            path_contains(below, root),
            "a volume root holds what is under it: {below:?} vs {root:?}"
        );
    }

    /// The unix half: `starts_with` semantics, case included, because unix
    /// filesystems distinguish `…/Data` from `…/data`.
    #[cfg(not(windows))]
    #[test]
    fn unix_comparison_is_case_sensitive() {
        assert!(path_contains(Path::new("/data/state"), Path::new("/data")));
        assert!(!path_contains(Path::new("/Data/state"), Path::new("/data")));
        assert!(!path_contains(
            Path::new("/data2/state"),
            Path::new("/data")
        ));
    }

    /// The Windows regression this rule exists for: NTFS resolves the two
    /// spellings to one directory, so the differently-cased state dir is
    /// in-root — [`Path::starts_with`] answers `false` on a case
    /// difference in a **normal** component (measured: `E:\Data\.tinio`
    /// against `\\?\E:\data`, and `E:\data\.tinio` against `E:\Data`, are
    /// both `false`), and that `false` is the bug (a read-write entry
    /// inside a read-only root). The drive-letter case is asserted too,
    /// though [`std::path::Component`]'s own `PartialEq` already folds it
    /// (so that leg passes with or without this rule): the rule folds
    /// every component, and the agreement is worth pinning.
    #[cfg(windows)]
    #[test]
    fn windows_folds_case() {
        assert!(path_contains(
            Path::new(r"E:\data\.tinio"),
            Path::new(r"E:\Data")
        ));
        assert!(path_contains(
            Path::new(r"E:\DATA\state"),
            Path::new(r"e:\data")
        ));
        assert!(path_contains(
            Path::new(r"e:\Data\.tinio"),
            Path::new(r"E:\Data")
        ));
        assert!(path_contains(
            Path::new(r"c:\DATA\state"),
            Path::new(r"C:\data")
        ));
        // The fold must not invent containment: a sibling still differs,
        // and the direction still matters.
        assert!(
            !path_contains(Path::new(r"D:\Data2\state"), Path::new(r"d:\data")),
            "the case fold widens which spellings match, not which paths"
        );
        assert!(!path_contains(
            Path::new(r"D:\OTHER"),
            Path::new(r"d:\data")
        ));
        assert!(!path_contains(
            Path::new(r"D:\data"),
            Path::new(r"d:\DATA\state")
        ));
    }

    /// The Windows verbatim leg: a plain candidate against a verbatim root
    /// is in-root, because `canonicalize` answers the verbatim shape while
    /// `absolute` (and the operator) answer the plain one — on both the
    /// drive and the UNC spelling. The literal comparison answers `false`
    /// for this pair (measured: `E:\Data\.tinio` against `\\?\E:\Data`),
    /// so the leg is a second defect and not a restatement of the case
    /// fold. The non-containment cases run against the same verbatim root,
    /// which keeps the leg a comparison rather than a blanket `true`.
    #[cfg(windows)]
    #[test]
    fn windows_matches_both_spellings_of_the_root() {
        assert!(path_contains(
            Path::new(r"D:\data\state"),
            Path::new(r"\\?\D:\data")
        ));
        assert!(path_contains(
            Path::new(r"d:\DATA\state"),
            Path::new(r"\\?\D:\data")
        ));
        assert!(!path_contains(
            Path::new(r"D:\data2\state"),
            Path::new(r"\\?\D:\data")
        ));
        assert!(!path_contains(
            Path::new(r"D:\other"),
            Path::new(r"\\?\D:\data")
        ));
        assert!(path_contains(
            Path::new(r"\\srv\share\data\state"),
            Path::new(r"\\?\UNC\srv\share\data")
        ));
        assert!(!path_contains(
            Path::new(r"\\srv\share\other"),
            Path::new(r"\\?\UNC\srv\share\data")
        ));
        // A root that is already plain needs no respelling and keeps
        // working (the verbatim arm is tried beside it, not instead of it).
        assert!(path_contains(
            Path::new(r"c:\DATA\state"),
            Path::new(r"C:\data")
        ));
    }

    /// The other half of the same defect, and the one the two-producer
    /// argument missed: a **verbatim candidate against a plain root**.
    /// `TINIO_STATE_DIR` is the operator's to write, and the verbatim form
    /// is what `canonicalize` answers and what a shell completion or a log
    /// line hands back — so this pairing is reachable, and answering
    /// `false` here grants a read-write state directory inside a read-only
    /// root exactly as the case-fold defect did.
    #[cfg(windows)]
    #[test]
    fn windows_matches_both_spellings_of_the_candidate() {
        assert!(path_contains(
            Path::new(r"\\?\E:\data\.tinio"),
            Path::new(r"E:\Data")
        ));
        assert!(path_contains(
            Path::new(r"\\?\UNC\srv\share\data\state"),
            Path::new(r"\\srv\share\data")
        ));
        // Widening the spelling does not widen containment: the sibling and
        // the unrelated path still differ, in either spelling.
        assert!(!path_contains(
            Path::new(r"\\?\E:\data2\state"),
            Path::new(r"E:\Data")
        ));
        assert!(!path_contains(
            Path::new(r"\\?\E:\other"),
            Path::new(r"E:\Data")
        ));
        // And the direction is still one-way for a verbatim candidate
        // against a root inside it.
        assert!(!path_contains(
            Path::new(r"\\?\E:\data"),
            Path::new(r"E:\data\state")
        ));
    }

    /// The strip itself, on the two shapes `canonicalize` produces and on the
    /// identity case: `\\?\D:\data` is `D:\data`, `\\?\UNC\srv\share\data` is
    /// `\\srv\share\data`, and a path with no verbatim prefix is returned
    /// **borrowed** rather than copied. `UNC\` is the share form only
    /// immediately after the prefix — a directory *called* `UNC` under a
    /// verbatim drive is a plain directory, not a share.
    #[cfg(windows)]
    #[test]
    fn windows_strips_the_verbatim_prefix() {
        assert_eq!(
            plain_spelling(Path::new(r"\\?\D:\data")).as_ref(),
            Path::new(r"D:\data")
        );
        assert_eq!(
            plain_spelling(Path::new(r"\\?\UNC\srv\share\data")).as_ref(),
            Path::new(r"\\srv\share\data")
        );
        assert_eq!(
            plain_spelling(Path::new(r"\\?\D:\UNC\data")).as_ref(),
            Path::new(r"D:\UNC\data")
        );
        for plain in [r"D:\data", r"\\srv\share\data", r"relative\data"] {
            assert!(
                matches!(plain_spelling(Path::new(plain)), Cow::Borrowed(_)),
                "{plain} carries no verbatim prefix, so it is its own plain spelling"
            );
        }
    }

    /// **The pairing the `to_str` strip skipped, and the reason the strip reads
    /// the units.** A Windows path is arbitrary UTF-16, so the root behind a
    /// verbatim prefix need not be text: `\\?\D:\data\<U+D800>` and
    /// `D:\data\<U+D800>` are one directory. The version this replaced answered
    /// "no plain form" for such a path — its `to_str` failed — so the two
    /// spellings were never paired and [`path_contains`] answered `false`, which
    /// on the sandbox side is two names and so two profiles for one root (its
    /// decision 21).
    ///
    /// The sibling beside it is asserted outside, so the leg is a comparison
    /// rather than a blanket `true`, and both directions are probed because
    /// *either* side of the comparison can arrive verbatim: the root from
    /// `canonicalize`, the candidate from an operator who pasted one.
    #[cfg(windows)]
    #[test]
    fn windows_pairs_the_two_spellings_of_a_root_that_is_not_text() {
        let mut units: Vec<u16> = r"D:\data\".encode_utf16().collect();
        units.push(0xD800);
        let plain = PathBuf::from(OsString::from_wide(&units));
        let mut verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
        verbatim.extend_from_slice(&units);
        let verbatim = PathBuf::from(OsString::from_wide(&verbatim));

        assert!(
            plain.to_str().is_none(),
            "the leg is vacuous unless the path really is not text: {plain:?}"
        );
        assert_eq!(plain_spelling(&verbatim).as_ref(), plain.as_path());

        // The plain spelling of the verbatim root is the root, and the other way
        // round; the candidate is the verbatim spelling an operator can paste.
        assert!(
            path_contains(&plain, &verbatim),
            "one directory's two spellings must compare as one: {plain:?} vs {verbatim:?}"
        );
        assert!(path_contains(&verbatim.join("state"), &plain));

        // Still component-wise: a sibling past the drive is outside in either
        // spelling of the root.
        let mut sibling: Vec<u16> = r"D:\data2\".encode_utf16().collect();
        sibling.push(0xD800);
        let sibling = PathBuf::from(OsString::from_wide(&sibling));
        assert!(!path_contains(&sibling, &verbatim));
        assert!(!path_contains(&sibling, &plain));

        // The UNC form, unit by unit: the other prefix the strip reads.
        let mut units: Vec<u16> = r"\\?\UNC\srv\share\".encode_utf16().collect();
        units.push(0xDC00);
        let share = PathBuf::from(OsString::from_wide(&units));
        let mut units: Vec<u16> = r"\\srv\share\".encode_utf16().collect();
        units.push(0xDC00);
        let plain_share = PathBuf::from(OsString::from_wide(&units));
        assert_eq!(plain_spelling(&share).as_ref(), plain_share.as_path());
        assert!(path_contains(&plain_share, &share));
    }

    /// A candidate whose bytes are not valid Unicode is folded lossily
    /// (the unpaired surrogate here), which cannot hide a real in-root
    /// path: NTFS cannot express two such names as distinct either. The
    /// ill-formed candidate is therefore not contained in a well-formed
    /// root, and the well-formed root is still found.
    #[cfg(windows)]
    #[test]
    fn windows_folds_ill_formed_names_lossily() {
        use std::{ffi::OsString, os::windows::ffi::OsStringExt};

        let ill_formed = PathBuf::from(OsString::from_wide(&[0xD800]));
        assert!(!path_contains(&ill_formed, Path::new(r"D:\data")));
        assert!(path_contains(
            &Path::new(r"D:\data").join(&ill_formed),
            Path::new(r"D:\data")
        ));
    }
}
