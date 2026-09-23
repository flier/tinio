use std::path::Path;

#[cfg(all(feature = "sandbox", target_os = "linux"))]
use tinio_config::Config;
use tinio_core::mode::AccessMode;
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use tinio_sandbox::linux;

#[cfg(all(feature = "sandbox", target_os = "linux"))]
use super::linux::sandbox_options;
use super::{refuse_in_root_state_dir, refuse_unhonoured_channels, sandbox_request};

#[test]
fn the_unimplemented_sandbox_channels_are_refused_by_name() {
    refuse_unhonoured_channels(false, false).expect("no channel is no refusal");

    let from_key = refuse_unhonoured_channels(true, false)
        .expect_err("`[server] sandbox` is a channel this phase does not implement");
    assert!(
        from_key.to_string().contains("[server] sandbox"),
        "the refusal must name the channel that was set: {from_key}"
    );

    let from_env = refuse_unhonoured_channels(false, true)
        .expect_err("`TINIO_SANDBOX` is a channel this phase does not implement");
    assert!(
        from_env.to_string().contains("TINIO_SANDBOX"),
        "the refusal must name the channel that was set: {from_env}"
    );
    // Whichever channel answers, the operator is told which one *is*
    // honoured, which is the design's own requirement for this row (§11).
    for message in [from_key.to_string(), from_env.to_string()] {
        assert!(
            message.contains("--sandbox"),
            "the refusal must name the channel that works today: {message}"
        );
    }

    // Both set is one refusal, and it names the key: the harder setting to
    // miss is the file the run was started with.
    let both = refuse_unhonoured_channels(true, true).expect_err("both channels are set");
    assert!(
        both.to_string().contains("[server] sandbox"),
        "the key answers first when both are set: {both}"
    );
}

#[test]
fn exposures_carry_the_mode_and_skip_an_in_root_state_dir() {
    let root = Path::new("/srv/data");
    let inside = Path::new("/srv/data/.tinio");
    let outside = Path::new("/home/u/.tinio/roots/h");

    let sandbox = sandbox_request(root, inside, AccessMode::ReadWrite);
    let exposures = sandbox.exposures();
    assert_eq!(
        exposures.len(),
        1,
        "an in-root state dir is already covered by the root"
    );
    assert_eq!(exposures[0].host, root);
    assert_eq!(exposures[0].access, AccessMode::ReadWrite);

    let sandbox = sandbox_request(root, outside, AccessMode::ReadOnly);
    let exposures = sandbox.exposures();
    assert_eq!(
        exposures.len(),
        2,
        "an out-of-root state dir needs its own entry"
    );
    assert_eq!(exposures[0].access, AccessMode::ReadOnly);
    assert_eq!(
        exposures[1].access,
        AccessMode::ReadWrite,
        "the state dir is always writable"
    );
}

/// **The read-write entry the case fold exists to prevent, at the site that
/// grants it.** NTFS resolves `E:\Data` and `e:\data` to one directory, so a
/// state directory the operator spelled in the other case is *inside* the
/// served root — and a literal component comparison calls it an unrelated
/// path outside, which buys it a second, **read-write** exposure inside a
/// root the caller asked to expose read-only. The assertion is one
/// exposure, and reverting `sandbox_request` to `Path::starts_with` fails it
/// on this platform.
///
/// Windows-gated because the fold is: on unix the two spellings are two
/// directories, and the literal answer is the right one there.
#[cfg(all(windows, feature = "sandbox"))]
#[test]
fn a_state_dir_inside_the_root_is_in_root_whatever_its_case() {
    let root = Path::new(r"E:\Data");
    let in_root = Path::new(r"e:\data\.tinio");

    let sandbox = sandbox_request(root, in_root, AccessMode::ReadOnly);
    let exposures = sandbox.exposures();
    assert_eq!(
        exposures.len(),
        1,
        "a differently-cased in-root state dir must not be granted its own entry: \
         {exposures:?}"
    );
    assert_eq!(
        exposures[0].access,
        AccessMode::ReadOnly,
        "and the one entry it has must carry the mode that was asked for"
    );
}

/// The same containment question at the refusal that closes the hole: a
/// state directory under a read-only root is refused even when the two
/// paths disagree about case. Without the fold this answers `Ok(())`, the
/// launcher proceeds, and the exposure set grants the state directory
/// read-write inside a root the operator asked to be read-only.
#[cfg(all(windows, feature = "sandbox"))]
#[test]
fn an_in_root_state_dir_is_refused_in_read_only_mode_whatever_its_case() {
    let root = Path::new(r"E:\Data");
    let err = refuse_in_root_state_dir(true, root, Path::new(r"e:\Data\state"))
        .expect_err("a state dir inside a read-only root must be refused");
    assert!(
        err.to_string().contains("TINIO_STATE_DIR"),
        "the refusal must name the way out: {err}"
    );

    // The guard is about the read-only mode and about containment, so the
    // two neighbouring cases pass: a read-write run may keep its state in
    // the root, and an out-of-root state directory is what read-only mode
    // asks for.
    refuse_in_root_state_dir(false, root, Path::new(r"e:\Data\state"))
        .expect("a read-write root may hold its own state directory");
    refuse_in_root_state_dir(true, root, Path::new(r"E:\state"))
        .expect("a state directory outside the root is the read-only layout");
}

/// **The guard's rule as a rule: three values in, a decision out.** No
/// sandbox feature, no read-only run, no spawned process — which is what the
/// `test`-widened gate above buys — and neither path below exists on this
/// host, so what is exercised is the comparison over the operator's **own
/// spellings** rather than anything the filesystem resolves (the function's
/// doc: the pair the grant would be built from, not their canonical forms).
///
/// Both halves of the conjunction have a case here, because dropping either
/// leaves a guard that refuses runs it should not: a read-write root may hold
/// its own state directory — there is no read-only exposure for the second
/// entry to be a hole in — and a state directory outside the root is exactly
/// the layout read-only mode asks the operator for.
///
/// **The phrase is pinned and not the whole sentence**, because it is what
/// the integration leg reads for (`serve_read_only`'s
/// `an_in_root_state_dir_is_refused_whatever_its_case` greps the child's
/// stderr for "holds the state directory"): a rewording here would be a green
/// unit test and a red leg, and the leg is the one that pays for a spawned
/// process to find out.
#[test]
fn a_read_only_root_refuses_a_state_dir_it_holds() {
    let root = Path::new("/srv/data");
    let state = Path::new("/srv/data/.tinio");

    let err = refuse_in_root_state_dir(true, root, state)
        .expect_err("a read-only root that holds the state directory cannot be served");
    let message = err.to_string();
    assert!(
        message.contains("holds the state directory"),
        "the refusal must name the situation the integration leg reads for: {message}"
    );
    assert!(
        message.contains("TINIO_STATE_DIR"),
        "and it must name the way out of it: {message}"
    );

    refuse_in_root_state_dir(false, root, state)
        .expect("a read-write root may hold its own state directory");
    refuse_in_root_state_dir(true, root, Path::new("/home/u/.tinio/roots/h"))
        .expect("a state directory outside the root is the read-only layout");
}

/// **Equality is containment, and containment stops at a component.** A state
/// directory *equal to* the root is refused — the read-write entry the guard
/// exists to prevent is then the whole root — while `…/data2` is a sibling of
/// `…/data` and not inside it.
///
/// The sibling is the case a rule spelled over text gets wrong: a prefix
/// comparison over the two strings answers "inside" for `…/data2` under
/// `…/data`, and the refusal that answer produces is one the operator cannot
/// act on — their state directory really is outside the root they named.
/// Neither answer is re-implemented here; both are `tinio_util::path`'s,
/// through the call under test, which is the one definer the guard's own doc
/// names.
#[test]
fn the_guard_holds_the_root_itself_and_stops_at_a_component() {
    let root = Path::new("/srv/data");
    refuse_in_root_state_dir(true, root, root)
        .expect_err("the root as its own state directory is in-root: a path contains itself");

    refuse_in_root_state_dir(true, root, Path::new("/srv/data2"))
        .expect("a sibling that shares the root's name prefix is not inside it");
}

/// **The same refusal on the platform where two spellings of one directory
/// are how the mistake actually arrives — and this is the twin the
/// feature-gated test above cannot be**: it runs in a plain `cargo test`, on
/// a build with no sandbox feature and no backend at all, because the fold
/// belongs to the shared definer rather than to any platform's arm.
///
/// NTFS resolves `E:\Data` and `e:\data` to one directory while a literal
/// component comparison calls them unrelated, so the second spelling is
/// inside the root and must be refused: it is the read-write entry inside a
/// read-only root, which is the hole this guard exists to close. The
/// neighbouring case is here too, so the fold is not "answer yes to
/// anything" — a differently-cased state directory *outside* the root is
/// still outside it.
///
/// Windows-gated because the fold is: on unix the two spellings are two
/// directories, and the literal answer is the right one there.
#[cfg(windows)]
#[test]
fn the_case_fold_alone_refuses_a_differently_cased_in_root_state_dir() {
    let root = Path::new(r"E:\Data");
    refuse_in_root_state_dir(true, root, Path::new(r"e:\data\state"))
        .expect_err("one directory under two spellings: the lower-cased in-root state dir");
    refuse_in_root_state_dir(true, root, Path::new(r"e:\state"))
        .expect("a differently-cased state directory outside the root is still outside");
}

#[test]
fn the_sandbox_asks_for_the_device_files() {
    // **Through `sandbox_request`, which is what the arms call.** The flag is
    // an argument of the construction rather than something the set
    // remembers, so a test that built its own `Sandbox` would stay green
    // while the shipped one lost its device files: flipping the argument to
    // `false` has to fail *this* test, and it does.
    let sandbox = sandbox_request(
        Path::new("/srv/data"),
        Path::new("/srv/data"),
        AccessMode::ReadWrite,
    );
    assert!(
        sandbox.device_files_enabled(),
        "the Linux jail needs /dev/null and friends (design decision 12)"
    );
    assert_eq!(sandbox.exposures().len(), 1);
}

#[test]
fn the_executable_directory_is_not_exposed_on_linux() {
    // Linux confines the process in place, so its image is already mapped and
    // needs no exposure (design decision 22). Read off the request the arms
    // build, for the same reason the flag is: an exposure only the tests
    // add is an exposure the server does not have.
    let exe_dir = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let sandbox = sandbox_request(
        Path::new("/srv/data"),
        Path::new("/srv/data"),
        AccessMode::ReadWrite,
    );
    assert!(!sandbox.exposures().iter().any(|e| e.host == exe_dir));
}

// The executable's directory is **not** tested here. It is this crate's own
// backend that merges it into the whitelist (`backend::exposures`, Task 3,
// decision 22) and Task 3 is where its tests live; the helper above is P1's,
// so a test of it here would be a second copy of both.

/// **The two `Options` fields `enter` decides the confinement from, and the
/// test the phase was missing.** Nine reviews passed over the shipped
/// `linux::Options::from_env()` — which sets neither — because every test
/// in this module pinned the `Sandbox` and nothing pinned the options: the
/// device-file flag is a fact about the request, the mode and the
/// `local_uid` mapping are facts about what the process is allowed to do
/// once it is inside. Both assertions below are the production call, not a
/// rebuilt `Options`, for the same reason `sandbox_request` is called
/// rather than a hand-built `Sandbox`: flipping either argument has to fail
/// *this* test.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
#[test]
fn the_sandbox_options_carry_the_resolved_mode_and_the_uid_mapping() {
    // Read-only with a mapping: the run that needs both, and the pair whose
    // defaults are wrong in opposite directions — a read-write exposure
    // where FR-023 asked for a read-only one, and no `CAP_CHOWN` where
    // design §8 promises one.
    let config =
        Config::parse("version = 1\n[owner]\nlocal_uid = 1005").expect("a config with a map");
    let options = sandbox_options(
        linux::Options::default(),
        &Some(config),
        AccessMode::ReadOnly,
    );
    assert_eq!(
        options.served_root_access(),
        AccessMode::ReadOnly,
        "a `--read-only` run must expose the served root read-only inside the jail"
    );
    assert!(
        options.local_uid_configured(),
        "a configured `local_uid` mapping must retain CAP_CHOWN (design §8)"
    );

    // And the no-mapping default, so the assertion above is about the
    // config rather than about the setter being stuck on.
    let options = sandbox_options(linux::Options::default(), &None, AccessMode::ReadWrite);
    assert_eq!(options.served_root_access(), AccessMode::ReadWrite);
    assert!(!options.local_uid_configured());
}
