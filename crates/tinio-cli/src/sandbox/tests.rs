// `OS` is read only by the `sandbox`-feature leg of the refusal test below, so it is imported
// only where that leg exists: a feature-off test build has no reader for it.
#[cfg(feature = "sandbox")]
use std::env::consts::OS;
// **`fs` is every host's now; the other three are the unix legs'.** The probe test's residue
// check reads a directory on any host, while the symlink and mode legs are unix's alone —
// Windows has no directory mode for the permission leg to set, and `symlink` needs the unix
// path — so gating all four together (as an earlier round did) is what would keep `fs` out of
// this host's test build.
#[cfg(feature = "sandbox")]
use std::fs;
#[cfg(all(feature = "sandbox", unix))]
use std::{
    fs::{OpenOptions, Permissions},
    os::unix::fs::{PermissionsExt, symlink},
};
use std::{io, path::Path};

use clap::parser::ValueSource;
// `Level` is the re-emit's level assertion; imported rather than spelled in the call for the
// same reason `api_state` below is named — `clippy::absolute_paths` counts a three-segment
// expression path.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use tracing::Level;

#[cfg(all(feature = "sandbox", feature = "api"))]
use super::confine::Handoff;
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use super::confine::announce_in_place;
#[cfg(feature = "sandbox")]
use super::confine::local_uid_configured;
#[cfg(feature = "sandbox")]
use super::product::exposures;
use super::*;
// The rule that mints a control token — what a leaking line would carry — read by the re-emit's
// leg alone.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use crate::_api::state as api_state;
#[cfg(feature = "sandbox")]
use crate::_core::mode::AccessMode;
#[cfg(feature = "sandbox")]
use crate::_sandbox;
// The enforcement vocabulary the legs below drive: the strength a backend answers for and the
// target question the answer is written against.
// Unconditional, like the seam they come through — this module is compiled in every build, so
// its tests are too.
use crate::_sandbox::{Enforcement, backend_available};
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use crate::_sandbox::{
    linux::{Mechanism, Seccomp},
    serve::Confined,
};
#[cfg(all(feature = "sandbox", unix))]
use crate::_util::path::path_contains;
// The shared structural reader — the boundary rule the one structural test below slices `plan.rs`'s
// function with. **Unconditional, unlike its neighbour below, and deliberately so**: that test runs
// in every build's test graph, so a `cfg` beside this import would leave it uncompiled where the
// test is.
use crate::_util::testing::function_body;
// The shared recording device (`tinio_util::testing`), which this crate's dev edge turns on.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use crate::_util::testing::{assert_recorded_at, assert_step, record_events};
use crate::{_config::Config, cli::Provenance, commands::start::Args, resolve::Resolution};

/// A resolution whose config asked for the confinement, with `root` and `state_dir` as given —
/// and the state directory **named**, which is `TINIO_STATE_DIR`'s case.
fn requested(root: &Path, state_dir: &Path) -> Resolution {
    let config_text = "version = 1\n[server]\nsandbox = true\n";
    let mut config = Config::parse(config_text).expect("config");
    config.server.sandbox = true;
    Resolution {
        root: root.to_path_buf(),
        config,
        config_text: config_text.to_owned(),
        config_path: state_dir.join(".tinio.toml"),
        read_only: false,
        state_dir: state_dir.to_path_buf(),
        state_dir_override: Some(state_dir.to_path_buf()),
        config_error: None,
    }
}

/// [`requested`] with **no** `TINIO_STATE_DIR` at all: the resolution derived the directory
/// from the home — *underived* is the guard's own word for it — which is the case macOS has no
/// grant for and the one [`refuse_underived_state_dir`] refuses on a read-only run.
///
/// Gated with its only caller below, which is `sandbox`'s: a feature-off build never reaches
/// `plan` with a request to refuse (decision 5 refuses them all with the build named), so the
/// helper would be dead code in exactly the graph that has no use for it — which is what
/// `cargo test --workspace --no-default-features` reports.
#[cfg(feature = "sandbox")]
fn underived(root: &Path, state_dir: &Path) -> Resolution {
    Resolution {
        state_dir_override: None,
        ..requested(root, state_dir)
    }
}

#[test]
fn every_channel_collapses_to_one_answer() {
    // The first argument is clap's answer's *source* — the flag, the variable (`.env`'s
    // value arrives as this one too: dotenvy puts it in the process environment before the
    // authoritative parse) or nothing at all — the second is that answer's value, and the
    // third is `config.server.sandbox`.
    assert!(
        !resolve_flag(ValueSource::DefaultValue, false, false),
        "nothing asked for it"
    );
    assert!(
        resolve_flag(ValueSource::CommandLine, true, false),
        "the flag"
    );
    assert!(
        resolve_flag(ValueSource::EnvVariable, true, false),
        "the variable"
    );
    assert!(
        resolve_flag(ValueSource::DefaultValue, false, true),
        "the config key"
    );
    assert!(
        !resolve_flag(ValueSource::EnvVariable, false, true),
        "the variable beats the config file"
    );
    assert!(
        !resolve_flag(ValueSource::CommandLine, false, true),
        "and so does a typed `false`"
    );
}

#[test]
fn no_channel_is_a_disabled_request() {
    // The answer half: nothing asked for the confinement, so nothing is refused and
    // nothing is exposed. The config is the **default** one rather than [`requested`]'s,
    // which says `sandbox = true` — this leg is the one that has no channel at all.
    let resolution = Resolution {
        root: Path::new("/srv/data").to_path_buf(),
        config: Config::default(),
        config_text: String::new(),
        config_path: Path::new("/srv/data/.tinio/.tinio.toml").to_path_buf(),
        read_only: false,
        state_dir: Path::new("/srv/data/.tinio").to_path_buf(),
        state_dir_override: None,
        config_error: None,
    };
    let request = plan(
        &resolution,
        &Args::for_test(Path::new("/srv/data")),
        &Provenance::default(),
    )
    .expect("nothing to refuse");
    assert!(!request.enabled());
}

/// The value half of the ruling is the leg above; this is the *discrimination* half, and it
/// is what decision 10 means by "the request names the channel it came from".
///
/// **Two shapes, one rule.** Where no backend can honour the channel the request is refused,
/// and the refusal's first words are the channel's own spelling — a typed flag names itself,
/// the variable names itself, and a request nothing above the file made names the config key.
/// Where this target's backend **is** wired (`tinio_sandbox::backend_available` answers
/// `true`) the request is honoured instead, and the discrimination is carried by the request's
/// own [`Channel`] — which is what FR-036's warning names on the run that loses its export, and
/// why the field exists rather than the channel being recomputed where the warning is emitted.
#[test]
fn the_request_names_the_channel_it_came_from() {
    let root = Path::new("/srv/data");
    let state_dir = Path::new("/srv/data/.tinio");
    let bare = Args::for_test(root);
    let typed = Args {
        sandbox: true,
        ..Args::for_test(root)
    };
    for (args, source, named, channel) in [
        (
            &typed,
            ValueSource::CommandLine,
            "`--sandbox`",
            Channel::Flag,
        ),
        (
            &typed,
            ValueSource::EnvVariable,
            "`TINIO_SANDBOX`",
            Channel::Env,
        ),
        (
            &bare,
            ValueSource::DefaultValue,
            "`[server] sandbox`",
            Channel::Config,
        ),
    ] {
        let provenance = Provenance {
            sandbox: source,
            ..Provenance::default()
        };
        let answer = plan(&requested(root, state_dir), args, &provenance);
        #[cfg(not(feature = "sandbox"))]
        {
            // The channel is the *honoured* request's, and a build with no `sandbox` feature
            // honours none: every channel is refused with the build named.
            let _ = channel;
            let err =
                answer.expect_err("a build with no `sandbox` feature never ignores a request");
            assert!(
                format!("{err}").starts_with(named),
                "the refusal names `{named}`: {err}"
            );
        }
        #[cfg(feature = "sandbox")]
        if !_sandbox::backend_available() {
            let err = answer.expect_err("a stub backend never ignores a request");
            assert!(
                format!("{err}").starts_with(named),
                "the refusal names `{named}`: {err}"
            );
        } else {
            let request =
                answer.expect("this target's backend is wired, so the request is honoured");
            assert!(request.enabled(), "`{named}` asked for a confinement");
            assert_eq!(
                request.channel,
                Some(channel),
                "`{named}` is the channel the honoured request carries"
            );
        }
    }

    // An explicit `false` from either clap channel is a request **not made**, so the
    // config key's `true` is never reached and nothing is refused.
    let request = plan(
        &requested(root, state_dir),
        &bare,
        &Provenance {
            sandbox: ValueSource::EnvVariable,
            ..Provenance::default()
        },
    )
    .expect("an explicit `false` is not a request for a confinement");
    assert!(!request.enabled());
}

/// **The Linux arm is wired, and this leg is what keeps it from drifting back.**
///
/// `commands::start::run` calls `sandbox::confine` on every target: in place on Linux, where
/// the answer is `None` and the process carries on to serve. The Linux arm of the facade's
/// answer (`tinio_sandbox::backend_available`) is therefore backed by an arm that acts on the
/// request, and a future edit that gated that call away again would leave every Linux run
/// that asks for a confinement starting normally and serving **unconfined** — the failure the
/// whole feature exists to prevent.
///
/// **The question is the facade's, and this leg is the CLI's half of it**: the target list
/// lives beside the `platform()` arms, in the crate whose arms those are, so an answer that
/// flipped without its backend is caught there — what is left for the CLI to keep is that a
/// `true` answer really is met by a `confine` call on this target.
///
/// `clippy::assertions_on_constants` is allowed **deliberately, and this is the one place the
/// lint's premise does not hold**: it reads a constant assertion as one that can never fail,
/// while here the answer is the subject and a future edit is exactly what makes this leg
/// fail. Nothing else is allowed for it.
#[cfg(feature = "sandbox")]
#[allow(clippy::assertions_on_constants, unreachable_code)]
#[test]
fn the_linux_arm_is_wired_because_a_confine_call_exists_on_this_target() {
    if _sandbox::platform_facts().launches_child {
        return;
    }
    assert!(
        _sandbox::backend_available(),
        "the facade answers `false` for this target, so `plan` refuses every confinement \
         request on Linux even though this module's in-place arm is wired and \
         `commands::start::run` reaches it: the answer is what promises `confine` handles \
         the target, and the two are wired in one edit"
    );
}

/// A requested confinement is never silently ignored: it is **refused with its reason named**
/// where nothing can honour it, and **honoured** where this target's backend is wired.
///
/// The discrimination is the point — a build that answered with the wrong shape would be
/// either silently permissive or misleading about what to fix — so the three shapes are
/// asserted separately: a feature-off build names the build, a stub backend names the
/// platform, and a wired backend carries the request through enabled.
#[test]
fn a_requested_confinement_is_refused_or_honoured_by_name() {
    // The channel here is the config key (`requested`'s fixture says `sandbox = true`, and
    // the provenance is every clap-backed key at its own default).
    let resolution = requested(Path::new("/srv/data"), Path::new("/srv/data/.tinio"));
    let answer = plan(
        &resolution,
        &Args::for_test(Path::new("/srv/data")),
        &Provenance::default(),
    );
    #[cfg(not(feature = "sandbox"))]
    {
        let err = answer.expect_err("a build with no `sandbox` feature never ignores a request");
        let text = format!("{err}");
        assert!(text.contains("sandbox"), "names the feature: {text}");
        assert!(
            text.contains("build"),
            "the feature-off refusal names the build: {text}"
        );
        assert!(
            !text.contains("platform"),
            "without the feature the platform is moot: {text}"
        );
        assert_eq!(err.exit_code(), 2, "a usage error naming the build");
    }
    #[cfg(feature = "sandbox")]
    if !_sandbox::backend_available() {
        let err = answer.expect_err("a stub backend refuses rather than serving unconfined");
        let text = format!("{err}");
        assert!(text.contains("sandbox"), "names the feature: {text}");
        assert!(text.contains("platform"), "the stub backend: {text}");
        assert!(
            text.contains(OS),
            "names the platform this build is on: {text}"
        );
        assert!(
            !text.contains("build"),
            "the feature is compiled in: {text}"
        );
        assert_eq!(err.exit_code(), 1, "a startup error, not a bad argument");
    } else {
        let request = answer.expect("this target's backend is wired, so the request is honoured");
        assert!(
            request.enabled(),
            "`[server] sandbox` asked for a confinement"
        );
    }
}

/// A read-only confinement over a root with no explicit `TINIO_STATE_DIR`: the state directory
/// the run would derive is the one macOS has no grant for, so the launcher refuses it and names
/// the remedy — while the same run **with** a named directory, and a read-only run that asks
/// for no confinement at all, are honoured.
///
/// **Read-only is spelled into the fixture rather than taken from [`requested`]**, whose
/// config is a plain `sandbox = true`: the mode is the refused combination's first half, and a
/// leg that left it out would assert the refusal on a read-write run — a run this guard must
/// never touch, because its state directory is `<root>/.tinio`, which the served root's own
/// exposure already covers. **The directory's channel is the fixture's other half**:
/// [`underived`] is the resolution whose directory was derived, which is the fact the guard
/// reads off the resolution — no parameter carries it here any more.
///
/// **The refusal reaches the seam on macOS alone, and the rule itself answers on every
/// host.** The platform half is [`plan`]'s: the guard's call site is a `macos`-gated block, so
/// only a Mac sees the refusal come out of the seam — Linux creates the state tree before it
/// enters the jail and Windows creates it as the user before the container child runs, so
/// refusing there would reject a run that works, and the accepted shapes are the legs that
/// would catch it. The rule's own decision and sentence are asserted **below, on any host**,
/// because the guard's own gate is the arms' widened by `test`
/// ([`refuse_unwritable_state_dir`]'s shape): what is macOS's is *where the guard is called*,
/// and not what it decides.
///
/// **Feature-gated with the guard, because a feature-off build never reaches it**: `plan`
/// refuses *every* requested confinement with the build named (decision 5) before the guard's
/// own gate is consulted, so this leg would read that refusal where it expects an enabled
/// request. The `sandbox`-feature leg above is where a request is driven through `plan` on such
/// a build.
#[cfg(feature = "sandbox")]
#[test]
fn a_read_only_confinement_refuses_a_state_directory_it_must_derive() {
    let root = Path::new("/srv/data");
    let derived = Path::new("/home/svc/.tinio/roots/0123456789abcdef");
    let named = Path::new("/srv/state");
    // The run the task refuses, and the pair it is refused for: the root is served and the
    // state directory is *not* named, which is what makes `plan` the place the decision can be
    // taken at all. The mode is `resolution.read_only` — the value both channels of the chain
    // produced — and `args`/`provenance` carry only the confinement's own channels, which is
    // why nothing here sets a read-only flag.
    let (resolution, args, provenance) = {
        let mut resolution = underived(root, derived);
        resolution.read_only = true;
        (resolution, Args::for_test(root), Provenance::default())
    };
    let answer = plan(&resolution, &args, &provenance);
    if _sandbox::platform_facts().refuses_derived_state_dir {
        let err = answer.expect_err("macOS cannot grant the derived directory's parent");
        let text = format!("{err}");
        for part in [
            "read-only",
            "TINIO_STATE_DIR",
            "/srv/data",
            "/home/svc/.tinio/roots/0123456789abcdef",
        ] {
            assert!(
                text.contains(part),
                "the refusal must name `{part}`: {text}"
            );
        }
        assert_eq!(err.exit_code(), 1, "a startup error, not a bad argument");
    } else {
        let request = answer.expect("a read-only confinement this target can grant is honoured");
        assert!(
            request.enabled(),
            "the refusal is macOS's mechanism and must not fire on {}",
            OS
        );
    }
    // Named, it is served: the state directory is always exposed read-write, so the operator
    // has a remedy that costs the confinement nothing.
    let mut named_resolution = requested(root, named);
    named_resolution.read_only = true;
    let request = plan(&named_resolution, &args, &provenance)
        .expect("an explicit writable TINIO_STATE_DIR is the remedy, not another refusal");
    assert!(request.enabled(), "the confinement is still applied");
    // Unconfined, the same read-only run and the same derived directory are honoured: the CLI
    // is not always sandboxed, and this refusal is the launcher's.
    let mut unconfined = underived(root, derived);
    unconfined.read_only = true;
    unconfined.config.server.sandbox = false;
    let request = plan(&unconfined, &args, &provenance)
        .expect("a run that asks for no confinement is refused nothing");
    assert!(!request.enabled(), "no channel asked for a confinement");

    // **The rule's own two answers, driven directly and therefore on this host.** Everything
    // above is the seam's half — which platform reaches the guard at all — and the refusal's
    // sentence would otherwise be unasserted on a host whose kernel has no Seatbelt. Four
    // values in, a decision out, with no platform test inside the rule: the refusal and the
    // two shapes it must not fire on.
    let refused = refuse_underived_state_dir(true, root, derived, StateDirSource::Derived)
        .expect_err("read-only cannot serve a state directory it must derive");
    let text = format!("{refused}");
    for part in [
        "read-only",
        "TINIO_STATE_DIR",
        "/srv/data",
        "/home/svc/.tinio/roots/0123456789abcdef",
    ] {
        assert!(
            text.contains(part),
            "the refusal must name `{part}`: {text}"
        );
    }
    refuse_underived_state_dir(true, root, named, StateDirSource::Named)
        .expect("an explicit TINIO_STATE_DIR is the remedy, not another refusal");
    refuse_underived_state_dir(false, root, derived, StateDirSource::Derived)
        .expect("a read-write run derives the state directory and is served");
}

/// **The served root cannot silently acquire a writable subtree: the state directory is
/// refused under either spelling of either path.** `root` reaches this guard canonical —
/// `resolve::resolve_root` canonicalises it — while `state` is the operator's
/// `TINIO_STATE_DIR` as their shell spelled it, so `/tmp/<root>/.tinio` against
/// `/private/tmp/<root>` (macOS's `/tmp`) has no literal relationship, and the narrow
/// `path_contains(state, root)` answered `false` for a directory that *is* inside the root.
/// Measured on macOS 15.8, 2026-09-25, through the real binary: that run started, served,
/// and wrote 105 bytes into `<canonical root>/.tinio/access.log` with no `file-write` denial
/// in the kernel log.
///
/// The explicit symlink is what makes the leg deterministic rather than dependent on a host
/// whose `/tmp` happens to be one — and the two spellings of the *root* are asserted
/// separately from the two of the state directory, because either side can be the symlinked
/// one at this call site.
///
/// **Feature-gated with the helpers it drives**, because a feature-off build compiles neither
/// the guard nor the constructor: the module is in every build, but `refuse_in_root_state_dir`
/// and its `exposures` half are the arms' own and exist only where an arm or a test can reach
/// them — the same conjunct its newer siblings carry.
#[cfg(all(feature = "sandbox", unix))]
#[test]
fn a_read_only_root_refuses_its_state_directory_under_either_spelling() {
    // The tempdir is held: dropping it would take the real tree with it.
    let dir = tempfile::tempdir().expect("tempdir");
    let real = dir.path().join("real");
    fs::create_dir_all(real.join(".tinio")).expect("the served root");
    let root = real.canonicalize().expect("canonicalize the served root");
    let link = dir.path().join("link");
    symlink(&real, &link).expect("symlink");

    // The exploit's pair, exactly: the canonical root the guard is handed, and the same
    // directory spelled through the link as `TINIO_STATE_DIR` is. The literal comparison is
    // asserted `false` first, so the leg cannot pass by accident on a host where the two
    // spellings coincide.
    let through_link = link.join(".tinio");
    assert!(
        !path_contains(&through_link, &root),
        "the leg is vacuous unless the narrow comparison answers `false`: {through_link:?} \
         vs {root:?}"
    );
    let err = refuse_in_root_state_dir(true, &root, &through_link)
        .expect_err("read-only cannot serve a root holding its state directory");
    assert!(
        format!("{err}").contains("read-only mode cannot serve a root"),
        "the refusal is the in-root guard's own: {err}"
    );

    // The mirror: a state directory spelled canonically under a root spelled through the
    // link. The same directory, so the same refusal — the guard decides on the directory
    // rather than on which side carries the link.
    let resolved_state = root.join(".tinio");
    assert!(!path_contains(&resolved_state, &link));
    refuse_in_root_state_dir(true, &link, &resolved_state)
        .expect_err("the root's spelling cannot decide the answer either");

    // And the guard still refuses what it refused before, through `path_contains`'s own
    // lexical rules: a `..` that stays inside the root names an in-root directory.
    refuse_in_root_state_dir(true, &root, &root.join("sub/../.tinio"))
        .expect_err("a lexically in-root spelling is still in-root");

    // **The other direction is the one that must stay open**, because widening a containment
    // guard past the question it asks refuses runs that work: a state directory beside the
    // root — created, so the canonical arm has something to resolve — is outside it.
    let beside = dir.path().join("state");
    fs::create_dir_all(&beside).expect("the state directory");
    refuse_in_root_state_dir(true, &root, &beside)
        .expect("a state directory beside the root is served, not refused");
    // The in-root pair stays accepted on a read-write run under **either** spelling, which is
    // the clause's early return rather than the mode's: `<root>/.tinio` is the state directory
    // every read-write run serves with.
    refuse_in_root_state_dir(false, &root, &through_link)
        .expect("a read-write run serves its own in-root state directory, whatever the spelling");
}

/// **The mirror of the guard above: a state directory that holds the served root is refused,
/// in either mode, and the exposure constructor is why.** `path_contains(state, root)` is
/// `false` for an ancestor, so [`exposures`] judges it *outside* the root, takes its
/// `else` branch and adds a **read-write** entry naming the ancestor — measured on macOS 15.8,
/// 2026-09-26, through the real binary and re-run for this change: `TINIO_STATE_DIR=/tmp/knl1
/// tinio server /tmp/knl1/root --read-only --sandbox` served, and re-applying the profile the
/// launcher rendered, with the launcher's own `-D` values
/// (`E0=/private/tmp/knl1/root`, `E1=/tmp/knl1`, `E2=/private/tmp/knl1`), made
/// `touch /private/tmp/knl1/root/knl-anc2.txt` answer `rc=0` with the file there afterwards.
/// The disjoint control renders the same three exposure rules and the same write is refused
/// `Operation not permitted`, which is what isolates the ancestor relationship as the cause.
///
/// **The width that entry has is the reason the clause is not read-only's alone**: it names a
/// directory *above* the root, so it reaches the root's **siblings** as well — a write no part
/// of a read-write run asked for either — and this test asserts the refusal on both modes. What
/// genuinely is read-only's is the in-root clause, because `<root>/.tinio` is the state
/// directory a read-write run serves with; the accepted shapes below therefore keep the
/// descendant (and the equal pair) on a read-write run and move only the ancestor.
///
/// **Feature-gated with the helpers it drives**, because a feature-off build compiles neither
/// the guard nor the constructor: the module is in every build, but `refuse_in_root_state_dir`
/// and its `exposures` half are the arms' own and exist only where an arm or a test can reach
/// them.
#[cfg(feature = "sandbox")]
#[test]
fn a_state_directory_that_holds_the_root_is_refused_in_either_mode() {
    let root = Path::new("/srv/data");

    // The ancestor: neither inside the root nor disjoint from it, and the pair the mirror
    // clause exists for. The refusal names the remedy as the in-root one does, and the mode it
    // names is the one that ran.
    let err = refuse_in_root_state_dir(true, root, Path::new("/srv"))
        .expect_err("read-only cannot serve a root the state directory holds");
    let text = format!("{err}");
    assert!(
        text.contains("read-only mode cannot serve a root the state directory holds"),
        "the mirror clause's own sentence: {text}"
    );
    assert!(
        text.contains("set TINIO_STATE_DIR outside the served root"),
        "the refusal names the remedy: {text}"
    );

    // **The same pair on a read-write run is refused too, with the same remedy.** The entry the
    // constructor would add is wider than the root's own — it reaches every sibling of the root
    // under the ancestor — so the root's wider access is not what makes it safe.
    let read_write = format!(
        "{}",
        refuse_in_root_state_dir(false, root, Path::new("/srv"))
            .expect_err("a read-write run may not expose the root's siblings either")
    );
    assert!(
        read_write.contains("read-write mode cannot serve a root the state directory holds"),
        "the mirror clause names the mode it fired on: {read_write}"
    );
    assert!(
        read_write.contains("set TINIO_STATE_DIR outside the served root"),
        "and the same remedy the read-only legs use: {read_write}"
    );

    // **Equality is decided once, and by the clause that already owned it.** Both conditions
    // would answer `true` for the equal pair, and the in-root clause is asked first, so what
    // comes back is the sentence that pair has always carried rather than a second refusal.
    let equal = format!(
        "{}",
        refuse_in_root_state_dir(true, root, root)
            .expect_err("a state directory equal to the root is in-root")
    );
    assert!(
        equal.contains("a root that holds the state directory"),
        "equality fires the in-root clause once: {equal}"
    );

    // The earlier round's refusal stays, unchanged.
    let inside = format!(
        "{}",
        refuse_in_root_state_dir(true, root, Path::new("/srv/data/.tinio"))
            .expect_err("a state directory under the root is still refused")
    );
    assert!(
        inside.contains("a root that holds the state directory"),
        "a descendant is the in-root clause's, not the mirror's: {inside}"
    );

    // The accepted shapes, and they are the in-root clause's alone: a sibling, the equal pair on
    // a read-write run (the in-root clause's early return — the pair adds no entry at all), and
    // the descendant a read-write run really serves with.
    refuse_in_root_state_dir(true, root, Path::new("/srv/state"))
        .expect("a state directory beside the root is served");
    refuse_in_root_state_dir(false, root, root)
        .expect("an equal pair is in-root, so a read-write run accepts it and adds nothing");
    refuse_in_root_state_dir(false, root, Path::new("/srv/data/.tinio"))
        .expect("and a descendant is the run's own state directory");

    // **The refusal runs in front of a construction that would really carry the entry**, and
    // both halves are read rather than argued: the guarded constructor refuses the pair, and
    // the exposure set it refuses to build is the one that names the ancestor read-write — the
    // entry whose `subpath` covers the read-only root. The set is [`exposures`]'s because the
    // guarded call can no longer hand it back, which is exactly what folding the guard into
    // that call means; the ancestor pair is the one no arm can reach a `Sandbox` with.
    let ancestor = Path::new("/srv");
    sandbox_request(&requested(root, ancestor), AccessMode::ReadOnly)
        .expect_err("the guarded construction refuses the pair the entry would come from");
    let would_carry = exposures(&requested(root, ancestor), AccessMode::ReadOnly);
    let carried = would_carry.exposures();
    assert_eq!(carried.len(), 2, "the root and its ancestor: {carried:?}");
    assert_eq!(carried[0].host, root.to_path_buf());
    assert_eq!(carried[0].access, AccessMode::ReadOnly);
    assert_eq!(carried[1].host, Path::new("/srv").to_path_buf());
    assert_eq!(
        carried[1].access,
        AccessMode::ReadWrite,
        "the entry that covers the read-only root"
    );

    // **And the same construction on a read-write run carries the same ancestor entry**, which
    // is the half that used to be reached: the root's own exposure is read-write here, so the
    // entry above is not wider *for the root* — it is wider for every sibling of the root under
    // that ancestor, and that is the widening the refusal in front of it now answers.
    let read_write_carry = exposures(&requested(root, ancestor), AccessMode::ReadWrite);
    let read_write_exposures = read_write_carry.exposures();
    assert_eq!(
        read_write_exposures.len(),
        2,
        "the same two paths: {read_write_exposures:?}"
    );
    assert_eq!(
        read_write_exposures[1].host,
        Path::new("/srv").to_path_buf()
    );
    assert_eq!(
        read_write_exposures[1].access,
        AccessMode::ReadWrite,
        "the ancestor entry a read-write run would carry too"
    );
}

/// The mirror under the two spellings, which is the half a literal comparison cannot see:
/// the state directory is the **ancestor** spelled through a link while the root is
/// canonical, so neither literal containment answer is `true` and only the resolved pairings
/// refuse it.
#[cfg(all(feature = "sandbox", unix))]
#[test]
fn a_state_directory_spelled_through_a_link_still_holds_the_root() {
    let dir = tempfile::tempdir().expect("tempdir");
    let real = dir.path().join("real");
    fs::create_dir_all(real.join("root")).expect("the served root");
    let root = real
        .join("root")
        .canonicalize()
        .expect("canonicalize the served root");
    let link = dir.path().join("link");
    symlink(&real, &link).expect("symlink");

    // One directory, two spellings, and the literal pairing answers `false` in both
    // directions — so a `true` from the guard can only come from a resolved pairing.
    assert!(
        !path_contains(&root, &link),
        "the literal clause is vacuous here unless it answers `false`: {root:?} vs {link:?}"
    );
    assert!(
        !path_contains(&link, &root),
        "and the other literal direction too: {link:?} vs {root:?}"
    );
    let err = refuse_in_root_state_dir(true, &root, &link)
        .expect_err("the link names the directory that holds the root");
    assert!(
        format!("{err}").contains("the state directory holds"),
        "the mirror clause is the one that fires: {err}"
    );

    // **The mirror clause is asked on a read-write run too, and the resolved pairing is what
    // answers it there as well** — this is the leg the read-write exemption used to accept,
    // asserted on the spelling that only the canonical arm can see.
    let read_write = format!(
        "{}",
        refuse_in_root_state_dir(false, &root, &link)
            .expect_err("read-write mode's ancestor entry reaches the root's siblings too")
    );
    assert!(
        read_write.contains("read-write mode cannot serve a root the state directory holds"),
        "and the read-write sentence is the mirror clause's: {read_write}"
    );

    // A sibling of the served root, in the same scratch tree: outside it under either
    // spelling, so the widened guard is not refusing everything under the state directory.
    let beside = dir.path().join("state");
    fs::create_dir_all(&beside).expect("the state directory");
    refuse_in_root_state_dir(true, &root, &beside)
        .expect("a state directory beside the root is served, not refused");
}

/// **The state directory is proved able to take the `state` entry before any child that needs
/// it exists.** This is the half of the macOS launcher's publish guard a host without Seatbelt
/// can hold: `refuse_unwritable_state_dir` is the whole decision, and the control bind is its
/// call site, placed ahead of `serve::Spec::apply` when `platform_facts` say the state
/// directory must be writable. What the
/// *call site* adds and this host cannot check is that placement, and the window the helper's
/// doc names: a directory writable here and unwritable seconds later is the TOCTOU case no
/// probe closes.
///
/// Three legs, on the hosts each is real on: a writable directory takes the probe and keeps no
/// residue; a path that is not a directory refuses; and — unix only, because Windows has no
/// directory mode a create honours — a directory the process cannot write refuses. The last is
/// asserted **only where the host enforces the mode it set**, because a container running
/// these tests as root would otherwise fail the leg for the wrong reason and pass it for none.
#[cfg(feature = "sandbox")]
#[test]
fn a_state_directory_is_proved_writable_before_a_child_can_need_it() {
    let dir = tempfile::tempdir().expect("tempdir");

    // The success leg, on every host: the entry is made and taken back out.
    refuse_unwritable_state_dir(dir.path()).expect("a writable state directory is served");
    let residue: Vec<_> = fs::read_dir(dir.path())
        .expect("read the state directory")
        .map(|entry| entry.expect("an entry").file_name())
        .collect();
    assert!(
        residue.is_empty(),
        "the probe must leave no entry behind: {residue:?}"
    );

    // The refusal leg, on every host: a state directory that is not there. `start::run` creates
    // it before any confinement, so this is the setup that never ran rather than a directory to
    // mint — and the probe must not mint it.
    let absent = dir.path().join("absent");
    let err = refuse_unwritable_state_dir(&absent).expect_err("an absent directory refuses");
    let text = format!("{err}");
    assert!(
        text.contains("cannot take a new entry"),
        "the refusal says what failed: {text}"
    );
    assert!(
        text.contains("Set TINIO_STATE_DIR"),
        "and names the remedy: {text}"
    );
    assert!(
        !absent.exists(),
        "the probe creates no directory on the refusal path either"
    );

    // The realistic leg, where a host can hold it: a directory whose mode refuses a create.
    #[cfg(unix)]
    {
        let locked = dir.path().join("locked");
        fs::create_dir(&locked).expect("the locked directory");
        fs::set_permissions(&locked, Permissions::from_mode(0o555)).expect("the mode");
        // Whether the host enforces the mode is asked of the filesystem rather than assumed.
        let enforced = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(locked.join("enforced"))
        {
            // A host that does not enforce it — a root container, or a filesystem that ignores
            // the bits — has no refusal for this leg to assert.
            Ok(file) => {
                drop(file);
                fs::remove_file(locked.join("enforced")).expect("the check's own entry");
                false
            }
            Err(_) => true,
        };
        if enforced {
            let err = refuse_unwritable_state_dir(&locked)
                .expect_err("a state directory this process cannot write refuses");
            let text = format!("{err}");
            assert!(
                text.contains("cannot take a new entry") && text.contains("Set TINIO_STATE_DIR"),
                "the unix refusal is the other legs' shape: {text}"
            );
        }
        fs::set_permissions(&locked, Permissions::from_mode(0o755)).expect("restore the mode");
    }
}

/// A launch with no local channel owns no `state` file; a token without a channel is refused.
#[cfg(all(feature = "sandbox", feature = "api"))]
#[test]
fn a_handoff_is_absent_or_refused_and_never_half() {
    assert!(
        matches!(Handoff::of(None, None), Ok(None)),
        "a launch with no local channel owns no `state` file"
    );
    assert!(
        Handoff::of(Some(&"00".repeat(32)), None).is_err(),
        "a token no bound channel carries is refused, not published without a name"
    );
}

/// The Linux capability knob follows `[owner] local_uid` the same way storage does.
#[cfg(feature = "sandbox")]
#[test]
fn the_mapping_follows_the_config() {
    assert!(
        !local_uid_configured(&Config::default()),
        "a config with no `local_uid` asks for no capability"
    );
    // The key does not parse on Windows, so the mapped half is the unix config's.
    #[cfg(unix)]
    {
        let mapped = Config::parse("version = 1\n[owner]\nlocal_uid = 1000\n").expect("config");
        assert!(
            local_uid_configured(&mapped),
            "a configured `local_uid` mapping is what the Linux jail keeps `CAP_CHOWN` for"
        );
    }
}

/// **A permission refusal a confined child meets is the confinement's own answer, and the
/// sentence names what was refused** — the classification follow-up §4 asks for, driven at the
/// one function that makes it.
///
/// **Both directions are asserted, because the gate is the whole rule**: outside a confinement
/// the same syscall failure is the filesystem's own answer, and reclassifying it would put a
/// sentence about a mechanism on a failure that mechanism never saw. The gate is
/// `adopted.is_some()` at the call sites (`commands::start::run`), which is the same
/// discriminator the process entry uses to identify a confined child at all.
///
/// **The three kinds are asserted separately**: `PermissionDenied` is what all three mechanisms
/// answer with, `ReadOnlyFilesystem` is the same fact from a filesystem that spells it `EROFS`
/// (a read-only mount), and anything else — a missing path under a refused parent is the case
/// the design's macOS rows measured — must stay the io error it was, because an `ENOENT` is
/// indistinguishable from a genuine absence at the syscall boundary.
#[test]
fn a_confined_childs_permission_refusal_is_the_confinements_own_answer() {
    let path = Path::new("/srv/data/.tinio");
    let refused = || Refused::Path(path.to_path_buf());
    let denied = || io::Error::from(io::ErrorKind::PermissionDenied);

    // Inside the confinement: the mechanism is what refused, and the path is in the sentence.
    let err = classify_io(denied(), refused(), true);
    let text = format!("{err}");
    assert!(
        matches!(&err, Error::SandboxDenied { platform, refused } if *platform == OS && refused.contains("/srv/data/.tinio")),
        "a confined child's refusal names the path: {text}"
    );
    assert_eq!(err.exit_code(), 1, "a startup error, not a bad argument");

    // Outside it, the identical failure is an ordinary io error.
    let unconfined = classify_io(denied(), refused(), false);
    assert!(
        matches!(unconfined, Error::Io(_)),
        "an unconfined process's filesystem said no: {unconfined}"
    );

    // The read-only spelling of the same refusal, inside the confinement.
    let read_only = classify_io(
        io::Error::from(io::ErrorKind::ReadOnlyFilesystem),
        refused(),
        true,
    );
    assert!(
        matches!(read_only, Error::SandboxDenied { .. }),
        "a read-only mount's `EROFS` is the same denial: {read_only}"
    );

    // And a kind that is not a refusal stays what it was, confinement or not.
    let missing = classify_io(io::Error::from(io::ErrorKind::NotFound), refused(), true);
    assert!(
        matches!(missing, Error::Io(_)),
        "an absent path is not a denial, even inside a confinement: {missing}"
    );

    // The fourth site's subject is a channel rather than a path, and it reads as one.
    let channel = classify_io(denied(), Refused::Channel("control"), true);
    let text = format!("{channel}");
    assert!(
        text.contains("the `control` channel"),
        "the control channel's refusal names the channel: {text}"
    );
}

/// **The core's usage class maps to the CLI's own, whichever platform's selector raised it** —
/// one operator mistake, one exit code. The core carries the class as its `Usage` variant now, so
/// this drives the mapping the way a caller reads it: a `Usage` error built from the Linux
/// sentence answers the CLI's `Usage` (exit 2) with the sentence intact, and its neighbour — the
/// same `detail` shape, but the platform's own refusal — is not promoted to usage.
#[test]
fn a_selector_typo_is_the_usage_class_whichever_platform_raised_it() {
    let typo = sandbox_err(_sandbox::Error::Usage {
        detail: _sandbox::LINUX_MODEL_USAGE,
    });
    assert!(
        matches!(&typo, Error::Usage(text) if text.contains("TINIO_SANDBOX_MODEL")),
        "a selector typo is the usage class, sentence intact: {typo}"
    );
    assert_eq!(typo.exit_code(), 2, "and the CLI answers exit 2 for it");

    let platform = sandbox_err(_sandbox::Error::Unsupported {
        detail: "no user namespaces",
    });
    assert!(
        matches!(platform, Error::Sandbox(_)),
        "a platform that cannot confine is operational, not usage: {platform}"
    );
    assert_eq!(
        platform.exit_code(),
        1,
        "exit 1 for the platform's own refusal"
    );
}

#[test]
fn a_host_refusal_keeps_its_classification_rather_than_flattening() {
    // The `Unsupported` leg beside this one pins the opposite outcome on purpose: a
    // platform fact flattens into `Sandbox`, a host fact does not, because the remedy is
    // the operator's and the caller may want to know which it was.
    let mapped = sandbox_err(_sandbox::Error::HostCannotConfine {
        cause: "this token cannot create an AppContainer profile (hr=0x80070005)".to_string(),
        remedy: "run in an ordinary user session",
        hr: 0x8007_0005u32 as i32,
    });
    assert!(
        matches!(mapped, Error::SandboxHostCannotConfine(_)),
        "{mapped}"
    );
    assert_eq!(mapped.exit_code(), 1);
}

#[test]
fn the_hosts_own_answer_is_a_third_refusal_and_not_a_fourth_path() {
    // **Layer one, and it is the whole of this seam's proof** (the plan-grilling ruling).
    // The workspace's own precedent for a structural test is
    // `crates/tinio-server/src/backend/ops.rs:287-310`.
    //
    // The guarantee is that the three facts are asked in ONE function, so a request that
    // reaches `confine` is one this build, this target and this host all committed to
    // applying. **No seam exists to force the facade's answer** — `backend_available` is a
    // `pub const fn` over `cfg!`, and `tinio-cli` has no dev-dependency on the Windows crate
    // — so the predicate's behaviour is proven where it is real (Task 11's Low-integrity
    // doctor scenario) and this test binds the call site. A text search is not a behaviour,
    // and the plan says so rather than implying otherwise.
    let body = function_body(include_str!("plan.rs"), "fn refuse_unhonourable");
    assert_eq!(
        body.matches("can_confine()").count(),
        1,
        "the host fact is asked once, beside the build and target facts"
    );
    assert!(
        body.contains("backend_available()"),
        "and the target fact stays"
    );
}

/// **The launcher's classification is a value, and the sentence is its rendering** —
/// [`describe`]'s two arms, driven directly, because the run site's own legs
/// (`commands::start::run`'s `launcher_exit`) assert the sentences and nothing here needs a
/// process to prove the classification behind them.
#[test]
fn an_attribution_renders_as_the_sentence_it_is() {
    let broken = Attribution::Broken {
        detail: "the confined child exited (exit status: 3) without reporting readiness".to_owned(),
    };
    assert_eq!(
        describe(&broken),
        "the confined child exited (exit status: 3) without reporting readiness",
        "`Broken`'s text is the detail the refusing site already emits, verbatim"
    );

    // **The other arm is the one no frame is built from, and it still renders a fact**: the
    // launcher withholds it by matching `Ran` before it renders (`launcher_exit`), so this is
    // what a caller that renders one anyway reads — the exit named, and no refusal invented
    // over a child whose stream this side never had (decision 19).
    let ran = describe(&Attribution::Ran);
    assert!(
        ran.contains("reported readiness"),
        "the arm that classifies nothing still says what happened: {ran}"
    );
}

/// The strength this build's confinement applies is the backend's own answer, and the one value
/// that is not a claim is `NotReported` — a backend that has not answered says so rather than
/// claiming a mechanism applies nothing.
#[test]
fn the_enforcement_is_the_target_backends_own_answer() {
    let expected = if backend_available() {
        Enforcement::Full
    } else {
        Enforcement::NotReported
    };
    // **The facade's own answer, read where the CLI reads every other platform fact** —
    // `_sandbox::platform_facts()` above in this file: the crate that owns the mechanism is the
    // one that answers, and a wrapper here would be a second name for a value with one home.
    assert_eq!(_sandbox::diagnostics().enforcement, expected);
}

/// **FR-036's trade names both halves or says nothing** — the four combinations, driven directly,
/// because the warning's own emission is `commands::start::run`'s (a `warn!` needs a subscriber,
/// and this crate's tests have none).
///
/// The rule is one function and the answer is its two facts: the channel that asked — spelled the
/// way every refusal spells it, which is what `contracts/cli.md` means by "naming the channel" —
/// and the endpoint the confinement costs. Anything else is not the trade: an unconfined run keeps
/// the export it configured, and a confined run that configured none has nothing to lose.
#[test]
fn the_otlp_trade_is_named_together_or_not_at_all() {
    let endpoint = "http://127.0.0.1:4317";
    let asked = Request {
        channel: Some(Channel::Flag),
    };
    assert_eq!(
        lost_export(&asked, Some(endpoint)),
        Some(("`--sandbox`", endpoint)),
        "the channel that asked and the endpoint it costs, in one answer"
    );
    let unasked = Request { channel: None };
    assert_eq!(
        lost_export(&unasked, Some(endpoint)),
        None,
        "an unconfined run keeps the export it configured"
    );
    assert_eq!(
        lost_export(&asked, None),
        None,
        "a confined run that named no endpoint has nothing to lose"
    );
    assert_eq!(
        lost_export(&unasked, None),
        None,
        "and neither half is a trade"
    );
}

/// The in-place re-emit **names §7's facts and carries nothing else** — the resolved model, the
/// exposure facts (the set's size, the jail's mount count and the rendered path list) and the
/// filter's outcome, at the level the serving subscriber enables, with no value the run holds as a
/// secret among them.
///
/// **Two assertions of deliberately different kinds.** The values say what the events report; the
/// per-line field sets say what each event *can* report, which is the half a later edit would
/// break — a field able to carry a string is the only route a token could take into these lines.
/// The emitter's input ([`serve::Confined`]) is the other end of the same argument: its one string
/// is the core's `describe_exposures` rendering of the exposure set — the set's own host paths and
/// access, not a value the spec held — which is why the token below cannot ride it. The sandbox
/// review's STD-01 — a control token printed into CI logs — is the precedent, so the token is
/// minted by the rule that mints them rather than spelled here.
///
/// **Driven directly rather than through `confine`**, the call site running behind a real
/// confinement on a Linux host — and the answer is built here the way `Spec::apply` builds one
/// (the confinement's own facts), so what the emission carries is the shape a confinement hands
/// it. The recording is the shared device (`tinio_util::testing`), which installs the process's
/// default subscriber with `try_init` — this crate's test binary installs none of its own, which
/// is why the capture is this test's to read.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
#[test]
fn the_in_place_re_emit_names_its_facts_and_carries_no_token() {
    let token = api_state::generate_token().expect("a control token");
    let rendered = "/srv/data (read-only), /srv/state (read-write), /srv/cache (read-only)";
    let confined = Confined {
        child: None,
        exposures: 3,
        // The jail root plus the three exposures plus the four device files — the plan the
        // backend applied, which is what `Confined::mounts` carries.
        mounts: 8,
        rendered_exposures: rendered.to_string(),
        model: Mechanism::UserNamespace,
        seccomp: Some(Seccomp::Installed),
    };
    let ((), logged) = record_events(|| announce_in_place(&confined));

    // §7's three events — the record the in-place ordering would otherwise drop — each at the
    // level `serve`'s subscriber enables, which is why they are `info!` rather than the per-item
    // `debug!` half.
    assert_step(&logged, "model");
    assert_step(&logged, "exposures");
    assert_step(&logged, "seccomp");
    assert_recorded_at(
        &logged,
        "the model this process confined itself",
        Level::INFO,
    );
    // The exposure line carries the same three fields the backend's own event carries — the count,
    // the mount count and the rendered set — all read off the confinement's answer. The rendered
    // set is recorded through `%` (as `enter`'s own event records it), which the text formatter
    // writes **bare** rather than quoted, spaces and all.
    assert!(
        logged.contains("model=UserNamespace")
            && logged.contains("count=3")
            && logged.contains("mounts=8")
            && logged.contains(&format!("exposures={rendered}"))
            && logged.contains("installed=true"),
        "the facts the confinement answered with: {logged}"
    );
    // Every `name=value` token on each event's own line, in the order it declares them: the
    // message precedes them and carries no `=`, so this is exactly that event's field set — §7's
    // names (`model`, `count`, `mounts`, `exposures`, `installed`), never the replaced line's own
    // vocabulary (`requested`, `local_uid`).
    let fields = |needle: &str| -> Vec<String> {
        logged
            .lines()
            .find(|line| line.contains(needle))
            .unwrap_or_else(|| panic!("no recorded line carries {needle:?} in:\n{logged}"))
            .split_whitespace()
            .filter_map(|token| token.split_once('='))
            .map(|(name, _)| name.to_string())
            .collect()
    };
    assert_eq!(fields("step=\"model\""), ["step", "model"]);
    assert_eq!(
        fields("step=\"exposures\""),
        ["step", "count", "mounts", "exposures"]
    );
    assert_eq!(fields("step=\"seccomp\""), ["step", "installed"]);
    assert!(
        !logged.contains(&token),
        "the line carries a control token: {logged}"
    );
}
