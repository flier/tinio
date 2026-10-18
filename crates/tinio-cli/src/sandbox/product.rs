#[cfg(feature = "sandbox")]
use std::{
    error::Error as StdError,
    fs,
    path::Path,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "sandbox")]
use super::plan::StateDirSource;
#[cfg(feature = "sandbox")]
use crate::{
    _core::mode::AccessMode,
    _sandbox::Sandbox,
    _util::path::{contains_under_any_spelling_either_way, path_contains},
    Error,
    resolve::Resolution,
};

/// The confinement request the arms make, built from the resolution the run produced and the mode
/// that was resolved for it — **the in-root refusal and the exposure set it stands in front of, as
/// one call**.
///
/// **The exposure rule lives here and nowhere else** (design §9): the served root carries the
/// access the mode dictates, the state directory is always read-write — the access log, the redb
/// metadata and the staging areas live there — and an in-root state directory needs no entry of
/// its own, because the served root's exposure already covers it. `Sandbox::expose` keeps the
/// most permissive access for a path given twice, so two entries for one path could not fight
/// even if the rule above ever let them meet.
///
/// **The guard is *in* the construction rather than a sentence in front of it.** A state directory
/// that is neither inside the root nor disjoint from it — the root's own ancestor — is judged
/// *outside* by [`exposures`], so the exposure set gains a **read-write entry naming that
/// ancestor**. The root is not the whole of what that entry covers: every sibling of the root
/// under the ancestor is exposed read-write too, which no part of the run asked for, and the entry
/// is wider than the root's own on a read-write run as well. On a read-only run the served root
/// itself is the other half — its read-only entry is covered by the ancestor's read-write one —
/// and [`refuse_in_root_state_dir`]'s mirror clause refuses the pair in **both** modes. That
/// refusal used to be a line each arm spelled for itself, with this function's doc as the only
/// thing holding the two together; now the only way to a `Sandbox` is through it, so no arm — and
/// no channel added later — can reach the exposure set without the refusal having been asked, and
/// [`exposures`] is private to this module for exactly that reason.
///
/// **"In-root" is [`path_contains`]'s answer and not a `Path::starts_with`'s**, and on Windows
/// that is the difference between an exposure set that holds and one that leaks: NTFS resolves
/// `e:\Data\.tinio` and `E:\Data\` to one directory while a literal component comparison calls
/// them unrelated, so a state directory the operator spelled in another case would be judged
/// outside the root and granted a **read-write** entry inside it — the hole this rule exists to
/// close, left open by the comparison the rule was once spelled with. The containment question
/// has one definer, which is what keeps this call site and the read-only refusal below from
/// answering it differently.
///
/// **One name for the construction, and the device-file grant is no longer part of it.** The
/// construction used to carry the flag so that a rebuilt set could not forget it — a test building
/// its own `Sandbox` stayed green while the jail silently lost `/dev/null` and friends, which is
/// design decision 12's point. `Sandbox` has dropped the flag altogether, so what this function
/// builds is paths and access and nothing else, and the four device nodes are the Linux backend's
/// own `Options` knob — asked for by `serve::Spec::device_files` on the Linux arm.
///
/// # Errors
///
/// [`refuse_in_root_state_dir`]'s — a read-only root that holds the state directory, or one the
/// state directory holds — as an operational failure (exit 1): the run is refused before any
/// exposure set exists and before the fork, because the entry the refusal stands in front of
/// would be a hole in the set rather than a storage error.
#[cfg(feature = "sandbox")]
pub(crate) fn sandbox_request(
    resolution: &Resolution,
    access: AccessMode,
) -> Result<Sandbox, Error> {
    refuse_in_root_state_dir(
        access.is_read_only(),
        &resolution.root,
        &resolution.state_dir,
    )
    .map_err(|err| Error::Operational(err.to_string()))?;
    Ok(exposures(resolution, access))
}

/// [`sandbox_request`]'s exposure set with no refusal of its own — the half the guard stands in
/// front of, private so that the pair cannot be pulled apart at a call site.
///
/// **The set is paths and access, and nothing else since `Sandbox` lost its device-file flag.**
/// That grant was always a Linux concept with nothing to render on the other two arms — the
/// exposures that crate's backend computes are the whole whitelist, and the macOS crate reads no
/// such flag — so it moved to the platform that reads it: the Linux arm asks its own backend's
/// `Options` for the four nodes, and the request every arm builds is the exposure set alone.
///
/// A test drives it to read **the entry [`refuse_in_root_state_dir`] exists to stand in front
/// of** — the ancestor's read-write exposure — which the guarded constructor can no longer hand
/// back, and a guard whose hole cannot be exhibited is a guard nobody can check.
#[cfg(feature = "sandbox")]
pub(super) fn exposures(resolution: &Resolution, access: AccessMode) -> Sandbox {
    let sandbox = Sandbox::new().expose(&resolution.root, access);
    if path_contains(&resolution.state_dir, &resolution.root) {
        sandbox
    } else {
        sandbox.expose(&resolution.state_dir, AccessMode::ReadWrite)
    }
}

/// Design §9's refused combination, refused in the launcher rather than inside the confinement:
/// a read-only root **and a state directory nested with it** cannot be served, and `FsStorage`
/// would reject the in-root half a process too late. **The nesting has two directions, and only
/// one of them is read-only's**: a state directory *inside* the root is refused for a read-only
/// run alone (the in-root clause), while one that *holds* the root is refused on either run (the
/// mirror clause) — the mode's own paragraphs below say why each is what it is.
///
/// **Refused here because the launcher is what would have granted the wider access** — a
/// read-write entry inside a read-only root is a hole in the exposure set, not a storage error —
/// and it is the same containment question [`exposures`] answers, through the same
/// definer: a state directory judged "outside" a root it is actually inside is exactly the case
/// this guard exists for, and on Windows two spellings of one directory are how that mistake
/// arrives (`TINIO_STATE_DIR=e:\Data\state` under a read-only `E:\Data`).
///
/// **The mirror is the same hole from the other side, and it is refused in both modes.** A state
/// directory that **holds** the served root — a strict ancestor of it — is outside the root by
/// [`path_contains`], so [`exposures`]'s `else` branch exposes it **read-write**, and that
/// entry covers the served root *and everything beside it*: on a read-only run the operator's root
/// silently gains a writable subtree, which is the promise `--read-only` makes and the reason the
/// root is exposed at all, while on a read-write run the root's **siblings** under that ancestor
/// gain a write no part of the run asked for — the root's own entry, the wider one, reaches none
/// of them. One direction of the containment question is therefore not an answer at this seam —
/// the exposure set's judgement is deliberately one-directional, the ancestor is neither contained
/// nor disjoint, and this guard is the refusal that runs in front of the entry rather than a
/// second judgement inside [`exposures`].
///
/// **The mode narrows the in-root clause and nothing else, and that asymmetry is the whole
/// design.** A state directory *inside* the root is the run's normal one: in read-write mode the
/// resolution's answer **is** `<root>/.tinio`, and refusing it would refuse every read-write run
/// this project serves, so the early return sits **after** the in-root clause rather than in front
/// of both. The mirror clause is asked on either run, because the entry it stands in front of
/// names a path *above* the root and reaches paths no mode asked to expose. The earlier shape
/// returned `Ok` for the whole read-write mode, which left the mirror clause read-write-exempt
/// while [`exposures`] went on adding the ancestor's entry — the two sides of this seam
/// disagreeing.
///
/// **Every platform reaches the covered root by its own rule, and which half is measured is
/// stated rather than implied.** macOS: the exposure renders one `(allow file-read* file-write*
/// (subpath …))` rule per exposed path over a `(deny default)` base, Seatbelt's allow rules
/// union, and the read-write rule names a directory above the root's read-only one — the same
/// nesting as the measured `TINIO_STATE_DIR=/tmp/rw-hole/.tinio` case above, one level the other
/// way. Linux: the mount table is one recursive bind per exposure, in `Sandbox::exposures()`
/// order, and the root's read-only remount is performed before the ancestor's bind exists; a path
/// lookup then resolves to the topmost mount whose point contains it, so the later read-write
/// bind at the ancestor is what serves the nested path and the read-only bind under it is
/// shadowed (**read from `crates/tinio-sandbox-linux/src/{plan,enter}.rs`, not measured: no
/// Linux host was run for this pair**). Windows: an exposed directory is granted the inheritable
/// `(OI)(CI)M` form, and inheritance reaches descendants that already existed (§6's own
/// measurement), so the ancestor's ACE lands on the root and on everything under it beside the
/// root's own `(OI)(CI)RX` — access is the union, so `M` answers a write (**read from
/// `crates/tinio-sandbox-windows/src/{acl,backend/mod.rs}`, not measured for this pair**).
///
/// **The equality case is decided once, and by the clause that already owned it.** The two
/// conditions are asked in order and [`path_contains`] counts a path as contained in itself, so
/// a state directory equal to the root never reaches the mirror clause: on a read-only run it is
/// the in-root clause's refusal with the sentence that pair already had, and on a read-write run
/// the in-root clause's early return accepts it — the pair is inert there, because a state
/// directory contained in the root adds no exposure entry at all. The mirror clause below
/// therefore sees strict ancestors alone, on either run.
///
/// **Both spellings of both paths are compared, and the narrower comparison is what failed to
/// guard.** The root arrives canonical — `resolve::resolve_root` canonicalises it before any
/// guard sees it — while the state directory is the operator's own spelling (`TINIO_STATE_DIR`
/// as typed) or a join onto the canonical root, so on macOS, where `/tmp` resolves to
/// `/private/tmp`, the pair `/tmp/<root>/.tinio` against `/private/tmp/<root>` has no literal
/// relationship at all: [`exposures`] below takes its `else` branch, the exposure set gains a
/// **read-write** entry, and the macOS arm's `with_canonical_paths` adds that entry's canonical
/// spelling while carrying the access flag over — so the rendered profile holds a read-write
/// `subpath` rule naming a directory inside a read-only root, and the served root silently
/// acquires a writable subtree. That is not
/// hypothetical and not Windows-only — measured on macOS 15.8, 2026-09-25, with
/// `tinio server /tmp/rw-hole --sandbox --read-only` and `TINIO_STATE_DIR=/tmp/rw-hole/.tinio`:
/// the run started, served, and grew `/private/tmp/rw-hole/.tinio/access.log` from 0 to 105
/// bytes with no `file-write` denial in the kernel log. `exposures`'s own comparison stays
/// the literal `path_contains(state_dir, root)`, and what bounds it is this guard rather than the
/// branch: its `else` only ever *adds* an exposure for a path it judges outside the root, and that
/// judgement is one-directional — the root's own ancestor is judged outside, which is the mirror
/// case above. This guard is the read-only promise for the in-root clause and the mirror refusal
/// for both modes, and it decides on the directory rather than on the spelling.
///
/// **The pairings compared are all four** — the literal state directory against the literal
/// root (what the narrow form did, and what still satisfies the Windows case-folding and
/// `./`-spelling legs `path_contains` owns), the canonical state directory against that root,
/// and each of those with the state directory's canonical form substituted for the root. Both
/// sides are canonicalised here rather than respelled once, because at this call site either one
/// can carry the symlink: the root is canonical in the product because `resolve_root` resolved
/// it, and the state directory carries the spelling the shell handed over.
///
/// **A path that does not exist yet never widens the answer, and it still resolves.** The state
/// directory is created by the serving chain before this guard runs, and its *leaf* — the config
/// file, the access log — is not, so a whole-path `canonicalize` answers `ENOENT` on exactly the
/// pair this guard is handed (measured on macOS 15.8, 2026-09-25: a first version that resolved
/// the leaf answered `false` for the exploit's own pair). The resolution therefore walks up to
/// the nearest existing ancestor and canonicalizes **that**, joining the components below it back
/// on — they do not exist, so they cannot be links, and the joined answer names the same
/// directory the leaf would. With no ancestor resolvable at all the literal spelling stands, which
/// is the narrow form's answer rather than a wider one.
///
/// **The rule has one owner, and every guard calls it.** The four pairings, the resolution behind
/// them and the per-pair comparison are
/// `tinio_util::path::contains_under_any_spelling`'s — the literal pair, the resolved candidate
/// against the literal root, the literal candidate against the resolved root, and both resolved —
/// so this guard names the question in both directions and nothing more. The two read-only guards
/// (`tinio-fs`'s and the CLI config root's, whose call sites reach the same function directly)
/// ask it of a root that is already canonical (`resolve_root`), and there the last two pairings
/// repeat the first two: the four-pairing set is exact for them rather than wider, which is why
/// one function serves all three call sites. **The config layer → `tinio-util` edge is what let
/// its copy go**: measured 2026-09-26, neither the config layer's crate nor `tinio-util` had
/// named the other in its manifest in either direction, and the edge that landed is the config
/// layer → `tinio-util`.
///
/// **Nothing here is Windows-specific, and the gate says so** — it is the arms' gate widened by
/// `test` and narrowed by the `sandbox` feature. Three pure values in, a decision out.
#[cfg(feature = "sandbox")]
pub(crate) fn refuse_in_root_state_dir(
    read_only: bool,
    root: &Path,
    state: &Path,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    // **The in-root clause, and the only one the mode narrows.** A state directory inside the
    // served root is the run's own `<root>/.tinio` on a read-write run — the resolution's answer,
    // which must be served — so this clause refuses for read-only alone, and its early return is
    // what decides the **equal** pair too: `path_contains` counts a path as contained in itself,
    // so `state == root` is in-root here and never reaches the mirror clause. That pair adds no
    // exposure entry in either mode (the constructor's `if` skips it), which is why accepting it
    // read-write is inert rather than a hole.
    // Both directions off one pair of resolutions: the two clauses below ask the same question
    // about the same two paths, and asking each separately resolves both paths twice.
    let (state_in_root, root_in_state) = contains_under_any_spelling_either_way(state, root);
    if state_in_root {
        if !read_only {
            return Ok(());
        }
        return Err(
            "read-only mode cannot serve a root that holds the state directory; set \
             TINIO_STATE_DIR outside the served root"
                .into(),
        );
    }
    // The mirror: the state directory's read-write entry names a path *above* the root, so it
    // covers the served root and the root's siblings under that ancestor — a widening the operator
    // asked for in neither mode — and this clause therefore refuses on both. It sees strict
    // ancestors alone: equality was answered above. A strict ancestor reaches here on a read-write
    // run as well, so the mode is named in the sentence rather than hardcoded read-only, and the
    // two clauses stay tellable apart by a test.
    if root_in_state {
        let mode = if read_only { "read-only" } else { "read-write" };
        return Err(format!(
            "{mode} mode cannot serve a root the state directory holds; set TINIO_STATE_DIR \
             outside the served root"
        )
        .into());
    }
    Ok(())
}

/// The state directory a read-only confinement **cannot grant a parent for**, refused in the
/// launcher rather than inside the confinement: with no explicit `TINIO_STATE_DIR` the state
/// directory is the home-derived `<home>/.tinio/roots/<hash>/`, whose creation needs
/// `file-write-mode` on `<home>/.tinio` — above the one entry [`sandbox_request`] exposes.
///
/// **Refused here because the launcher is what would have granted the access** (the neighbour's
/// reason, one level up): macOS exposes the *state directory itself* read-write and no rule spends
/// on a parent, so the only grant that would cover `<home>/.tinio` is `$HOME` — the grant this
/// project deliberately deleted (`docs/superpowers/specs/2026-09-18-sandbox-design.md` §5, its
/// `home` paragraph). The remedy is the
/// operator's `TINIO_STATE_DIR`, which names a directory the profile exposes whole; the refusal
/// names it rather than letting the child die on its own `EPERM`, which arrives as
/// `state error: failed to resolve the state directory at …: Operation not permitted` with
/// no way for the operator to tell that a grant was the missing thing.
///
/// **What decides is whether the state directory was named, not where it lands** — and *named*
/// rather than *exists*, because by the time this runs the launcher's own resolution has already
/// created the derived tree (measured 2026-09-25: a refused run leaves
/// `<home>/.tinio/roots/<hash>/.tinio.toml` behind). An explicit `TINIO_STATE_DIR` is exposed
/// read-write wherever it points, so it is served; deriving it is the case whose parent no entry
/// covers. [`refuse_in_root_state_dir`] answers the different question that pairs with this one — a
/// *named* state directory that happens to lie under the served root — and the two never fire
/// together: that guard needs the directory inside the root and this one needs it unnamed.
///
/// **macOS-only, and the platform fact is [`tinio_sandbox::platform_facts`] at the call site
/// rather than a test in this body.** The mechanism is Seatbelt's: Linux confines in place
/// *after* `start::run` has created the state tree, and on Windows the launcher creates it as the
/// user before the container child ever runs, so a refusal there would reject a run whose own
/// platform guide documents it working — and [`plan`] reaches this function only when
/// `refuses_derived_state_dir` is set, so no shared line has to re-test the platform. **The gate here
/// (`any(test, …)`, the shape [`refuse_unwritable_state_dir`] carries) is the other half**: the
/// rule stays compiled, and therefore drivable, on a host whose kernel has no Seatbelt, so its
/// decision and the sentence an operator reads are asserted everywhere rather than only on a Mac.
/// What is inside is the rule alone — four values in, a decision out.
#[cfg(feature = "sandbox")]
pub(crate) fn refuse_underived_state_dir(
    read_only: bool,
    root: &Path,
    state: &Path,
    state_dir_source: StateDirSource,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    if read_only && state_dir_source.is_derived() {
        return Err(format!(
            "read-only mode confines `{root}` with the state directory derived from the home \
             directory (`{state}`), whose parent Seatbelt refuses `file-write-mode` on: the \
             confinement exposes the state directory itself and grants nothing for its parent. \
             Set an explicit, writable `TINIO_STATE_DIR` outside the served root",
            root = root.display(),
            state = state.display(),
        )
        .into());
    }
    Ok(())
}

/// The `state` publish's precondition, refused in the launcher rather than discovered after a
/// confined child is already serving: **the state directory must be able to take one new entry**,
/// proved by creating one and removing it before anything is launched.
///
/// **The failure this stands in front of leaves no serving process, and the probe is still the
/// cheaper half of the answer.** The launcher publishes `state` once the child reports it is
/// listening — that file is how `tinio status` and `tinio stop` find the run — and that publish is
/// the last fallible step with a child already running. **Since 2026-09-26 the launcher ends and
/// reaps that child before returning the publish error** (`Child::abort`, the core's own
/// kill-and-reap, called by `confine`), so the failure no longer leaves the Seatbelt-confined
/// server listening with nothing naming it — the platform's own accepted gap is a launcher killed
/// outright, which runs none of that code. What the probe still buys is the realistic arrival:
/// refusing a read-only, full or missing directory **here** costs a probe and a clear message while
/// no child exists, instead of a launched child that is then killed.
///
/// **Only a run that will publish asks, and the caller decides that.** A local management channel
/// and the token that guards it are what make a `state` file, so `--api http://…`,
/// `--no-api-unix` and every `api`-less build must never be refused for a directory they never
/// write. The control bind sits where that pair is produced — the channel it binds and
/// the token it mints — which is the same pair the publish at the end of that function reads, so
/// the run that never publishes cannot reach this probe at all.
///
/// **The probe is one `create_new` and one removal, and it creates no directory.** The state
/// directory is already there when this runs — `start::run`'s step h creates it before any
/// confinement, on every target — so a missing directory is a genuine refusal rather than
/// something to mint: creating it here would change what a refused run leaves behind, and it is
/// also what keeps the caller's *bind* from minting one, since a unix socket's parent is created
/// by the bind that this probe stands in front of. **Who makes the directory is that same step's**
/// — it hardens a derived read-only leaf with `tinio_util::state::prepare_home_derived` just
/// before the `create_dir_all` — and this probe only asks whether the directory that step produced
/// will take an entry; it neither creates nor hardens one. `create_new` rather than a plain create
/// is deliberate: a name already taken is refused instead of opened, so a symlink planted at the
/// probe's path is not followed and nothing this launcher did not create is truncated or removed.
/// The name carries the pid and the clock, so two probes — or one probe and a stale entry from a
/// process that is gone — cannot collide into a refusal of a directory that is in fact writable.
///
/// **What this does not close is a window, and it is named rather than claimed shut.** Between the
/// probe and the publish — which happens after the child reports `Listening`, seconds later — the
/// directory can fill, lose its writability, or be replaced. That publish is still the fallible
/// step with a child behind it, but it is no longer an unguarded one: the launcher kills and reaps
/// that child before it returns the publish error. What the
/// probe removes is the realistic arrival — a read-only, full or absent state directory at startup,
/// refused while nothing has been confined — rather than the race.
///
/// # Errors
///
/// The create, which is the refusal the caller wants; or the removal of the entry the create made,
/// which is reported rather than swallowed because that branch is the one that leaves something
/// behind, and the message names it. The removal is attempted on every path that made an entry,
/// and no other failure can occur between the two calls.
#[cfg(feature = "sandbox")]
pub(crate) fn refuse_unwritable_state_dir(
    state: &Path,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let probe = state.join(format!(
        "state-probe-{pid}-{stamp}",
        pid = process::id(),
        stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos()),
    ));
    if let Err(err) = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        return Err(format!(
            "the state directory `{state}` cannot take a new entry ({err}): the `state` file this \
             launcher publishes once the confined child is listening is written there, and a \
             publish that failed after the child exists would leave it serving undiscoverable — \
             this platform has no job object to end it. Set TINIO_STATE_DIR to a directory this \
             process can write",
            state = state.display(),
        )
        .into());
    }
    fs::remove_file(&probe).map_err(|err| {
        format!(
            "the state directory `{state}` would not give the probe entry `{probe}` back \
             ({err}): the entry is left there, and a `state` file that could not be removed is a \
             run `tinio status` would keep reporting. Remove it and set TINIO_STATE_DIR to a \
             directory this process can write",
            state = state.display(),
            probe = probe.display(),
        )
    })?;
    Ok(())
}
