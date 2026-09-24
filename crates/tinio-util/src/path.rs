//! The one containment rule: does this path lie inside that root, as the
//! volume that will serve it resolves the two?
//!
//! [`path_contains`] is the *definer* of the question, and it is the only
//! one. It has two readers, and they are the two places a wrong answer is
//! expensive:
//!
//! - the filesystem backend's read-only state-dir guard (`FR-023`,
//!   decision 17a) — an in-root state directory silently writes the very
//!   tree the mode promises to leave alone;
//! - the server launcher's exposure set (design §9/§10) — a state
//!   directory judged outside the root is granted a read-write entry, and
//!   one judged outside a **read-only** root is a hole in the exposure
//!   set rather than a storage error.
//!
//! **The two copies are what this module is for.** While each reader
//! carried its own comparison, the Windows case folding below lived in
//! one of them only, and the other compared components literally — so on
//! NTFS a state directory spelled in another case (`E:\data\.tinio`
//! against a root given as `E:\Data`, and both of those spellings of one
//! directory) read as *outside*, and the guard that exists to catch
//! exactly that mistake let it through. A rule with two owners is a rule
//! with one owner's bugs; the fix is one definer, called twice.

#[cfg(windows)]
use std::path::PathBuf;
use std::path::{Component, Path};

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
/// `\\srv\share\data`).
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
pub fn path_contains(candidate: &Path, root: &Path) -> bool {
    #[cfg(not(windows))]
    {
        components(candidate).is_within(&components(root))
    }
    #[cfg(windows)]
    {
        // Bound rather than inlined: `plain` answers an owned path per side, and
        // the comparison below borrows both.
        let plain_candidate = plain(candidate);
        let plain_root = plain(root);
        let candidates = [Some(candidate), plain_candidate.as_deref()];
        let roots = [Some(root), plain_root.as_deref()];
        candidates.into_iter().flatten().any(|candidate| {
            let candidate = lower_components(candidate);
            roots
                .iter()
                .flatten()
                .any(|root| candidate.is_within(&lower_components(root)))
        })
    }
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
/// approximation cannot hide a real in-root state dir.
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

/// The plain (non-verbatim) form of a canonical root:
/// `std::fs::canonicalize` answers a verbatim path (`\\?\C:\data`, or
/// `\\?\UNC\srv\share\data` for a UNC root) while
/// `std::path::absolute` answers the plain form (`C:\data`,
/// `\\srv\share\data`). `None` when the path carries no verbatim prefix —
/// including a path [`Path::to_str`] cannot read (an unpaired surrogate,
/// say), which has no plain form to hand back rather than one made up.
#[cfg(windows)]
fn plain(root: &Path) -> Option<PathBuf> {
    let text = root.to_str()?.strip_prefix(r"\\?\")?;
    Some(match text.strip_prefix("UNC\\") {
        Some(unc) => PathBuf::from(format!(r"\\{unc}")),
        None => PathBuf::from(text),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
