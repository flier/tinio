//! The backend's unit tests, kept at the one path that can ask the policy's
//! private questions: `shape_for`'s precedence rule and `CallerSet`'s membership
//! answer are `pub(in crate::backend)` in the `exposures` module, and this module — a child of
//! `backend` — is the reader that visibility was widened for.
//!
//! **They live here rather than inside `exposures`**, which is the move that
//! would have let every item stay private: a `mod tests` inside that file would
//! also have renamed every test from `backend::tests::…` to
//! `backend::exposures::tests::…`, and a rename is not a move. What the split
//! had to widen, and why it stopped at `pub(in crate::backend)`, is stated once in `mod.rs`'s
//! module doc.
//!
//! **The two suites that drive `Backend::confine` end to end moved in here on
//! 2026-10-01** (`tests/network.rs`'s five legs and `tests/profile.rs`'s two;
//! `docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`): the subject of
//! each is this backend's composition — the ACLs it grants, the profile window it
//! holds, the capability list it builds — and `docs/tests.md`'s placement rule
//! puts a unit test beside the function it exercises. Their harness is
//! [`crate::testing`]'s, which became lib code for the same reason, and the child
//! they launch is this binary's `testing::tests::child_role`.
//!
//! **The profile window's legs are the measured answer to one interleaving.** A
//! second `CreateAppContainerProfile` for one name answers
//! `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` with a null SID out-parameter, so
//! the second launcher takes the first's profile over rather than creating a
//! second one — and the first launcher's exit deletes it, because nothing
//! distinguishes "I created it" from "I took it over". What that costs is exactly
//! one interleaving: a `CreateProcessW` that lands between a peer's delete and
//! its own completion fails `err=2 ERROR_FILE_NOT_FOUND` for a program that
//! exists, which is a misdiagnosis an operator chases as a missing binary (the
//! delete's own half, `a_peer_delete_waits_for_the_window_a_launch_holds`, is
//! inline in `tinio_sandbox_windows::sid`). The two legs below are the launch's
//! half, and the first of them is the strongest assertion of the pair.
//!
//! **Why those legs scope their reads to this run.** The recording device is
//! process-wide and this binary now holds several launching legs, so
//! `step="grant the ACLs"` or `step="CreateProcessW"` alone is also what a
//! *neighbouring* leg's launch writes: a wait satisfied by a foreign launch would
//! release the peer's delete before this launcher is inside its window, and a
//! search for the first `CreateProcessW` in the buffer could read a foreign
//! process's. Both reads are therefore scoped — the phase marker by this root's
//! SID, the launch by coming after that marker — which is stricter than the
//! unscoped originals, never looser.

use std::{
    env, fs,
    io::Read as _,
    net::{IpAddr, SocketAddr, TcpStream, UdpSocket},
    os::windows::fs::symlink_file,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use tempfile::tempdir;
use tinio_sandbox_core::{AccessMode, Confinement, Error, Exposure, Sandbox, supervise};
use tinio_util::testing::{assert_recorded_at, assert_recorded_below, assert_step};

// `CallerSet` is the one policy item `super::*` does not carry — the parent
// imports `shape_for` and `canonical_whitelist` by name because `confine` calls
// them, and the type is named here for the same reason these tests ask it the
// question: it is `pub(super)` for this module alone (`exposures.rs`'s module
// doc).
use super::{
    exposures::CallerSet,
    grant::{GRANT_PHASE_WARN_AFTER, report_grant_phase},
    *,
};
// **The recording device is `testing`'s rather than this module's**: `capture` and
// `recorder` used to be a copy here and a second, wider one in the file the
// profile legs arrived from (`tests/profile.rs`, retired 2026-10-01), and the
// definer's own doc states what the union folded together.
use crate::{
    acl::{self, Target},
    sid::{PackageSid, ProfileLock, package_name},
    testing::{
        self, Capture, capture, launch_confined, one_volume, recorder, run_confined,
        run_confined_with, sid_for, tree,
    },
};

/// A junction at `dir/name` pointing at `target`, or a failed test.
///
/// The link is [`testing::junction`]'s — this module builds the path and names
/// it, and the fixture's own self-assertion comes with it.
fn junction(dir: &Path, name: &str, target: &Path) -> PathBuf {
    let link = dir.join(name);
    testing::junction(&link, target);
    link
}

/// Run `body` with the capture recording this thread's events.
fn recorded<T>(body: impl FnOnce() -> T) -> (T, String) {
    let capture = capture();
    let value = {
        let _recording = capture.record();
        body()
    };
    let logged = capture.text();
    (value, logged)
}

/// Whether `path` names `leaf` directly under `dir`.
///
/// The directory is compared by equality and the leaf by name, because the two
/// halves of the assertion come from different places: the directory is the one
/// `exposures` produces, while the leaf is a spelling the rule adds.
fn is_the_file(dir: &Path, leaf: &str, path: &Path) -> bool {
    path.parent() == Some(dir) && path.file_name().and_then(|name| name.to_str()) == Some(leaf)
}

/// Whether `path`'s final component is `leaf` — the comparison the tests that
/// never touch the disk use, because `exposures` does not resolve anything and
/// their paths are spelled rather than resolved.
fn ends_with(path: &Path, leaf: &str) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some(leaf)
}

/// Shared §7's rule — `error!` where a returned [`Error`] is built, with the
/// operation as a field — is a property this module can lose, and it is the
/// only module that can lose it for these three failures: the core's own
/// recorder test covers the shared builders, while the refused reparse point,
/// the refused caller's file and the empty exposure set are `Error`s this
/// module builds by hand.
///
/// The assertions are on the **rendered fields and on the level each line
/// carries**, not on the values as substrings, for the reason the core's test
/// states at length: `step` appears inside the rendered error too, so a substring
/// check would pass for an event whose field said something else. `with_ansi(false)`
/// is what makes the field syntax visible at all (measured in that same test).
/// **The level is read off the line** ([`assert_recorded_at`]) rather than selected
/// by the recording, which records every level at `DEBUG`: a failure quietly moved
/// to `debug!` is no longer dropped by a filter, so it is caught by that assertion
/// instead — and a promotion away from `error!`, which the filter could never see,
/// fails it too.
///
/// The event count is the other half of the ruling: one failure emits one
/// event, and the refusal is deliberately not routed through the core's
/// `launch`, which builds [`Error::Launch`] and logs a line of its own.
///
/// **The capture is [`capture`]'s, which is global on purpose** — a
/// thread-scoped subscriber loses the event outright when a parallel test is
/// the first to hit the callsite, which is the one-run-in-thirty flake this
/// harness was fixed for. That doc carries the mechanism.
///
/// The two links below carry their targets, because `Error` is a name
/// `exposures.rs` builds its refusals with and not one this file names in code.
///
/// [`Error`]: crate::_core::Error
/// [`Error::Launch`]: crate::_core::Error::Launch
#[test]
fn the_failures_this_module_builds_are_logged_where_they_are_built() {
    let _recorder = recorder();
    let dir = tempdir().expect("temp dir");
    let real = dir.path().join("real");
    fs::create_dir_all(&real).expect("create the target");
    let link = junction(dir.path(), "link", &real);

    let (error, logged) = recorded(|| {
        canonical_exposures(vec![Exposure {
            host: link.clone(),
            access: AccessMode::ReadOnly,
        }])
        .expect_err("a junction is refused")
    });
    assert!(error.to_string().contains("reparse point"), "{error}");
    assert_step(&logged, "resolve an exposed path");
    // **The needle is the refusal's own message, not its step.** `assert_recorded_at` answers about the
    // first line carrying what it is given, and a step is not unique to a refusal: the caller's file
    // below is refused on a path where an `info!` line carries the same `step` — the per-exposure
    // resolution that logs *what* the run was about to grant — so the message that only a refusal
    // writes is what names the line. The count assertion at the end of each recording is what makes it
    // unique there.
    assert_recorded_at(&logged, "sandbox step failed", tracing::Level::ERROR);
    assert!(
        logged.contains(&format!("path={}", link.display())),
        "{logged}"
    );
    assert_eq!(logged.matches("sandbox step failed").count(), 1, "{logged}");

    // P7's refusal is the third of them, and it is the one an operator meets
    // as a bare startup failure unless it logs where it is built.
    let exposed_file = dir.path().join("exposed.txt");
    fs::write(&exposed_file, b"x").expect("write the caller's file");
    let (error, logged) = recorded(|| {
        canonical_whitelist(
            &Sandbox::new().expose(&exposed_file, AccessMode::ReadOnly),
            None,
        )
        .expect_err("a caller's file is refused")
    });
    assert!(error.to_string().contains("is not a directory"), "{error}");
    assert_step(&logged, "resolve an exposed path");
    assert_recorded_at(&logged, "sandbox step failed", tracing::Level::ERROR);
    assert!(
        logged.contains(&format!(
            "path={}",
            fs::canonicalize(&exposed_file)
                .expect("the file resolves")
                .display()
        )),
        "{logged}"
    );
    assert_eq!(logged.matches("sandbox step failed").count(), 1, "{logged}");

    let (error, logged) =
        recorded(|| served_root(&Sandbox::new()).expect_err("nothing exposed to confine to"));
    assert!(error.to_string().contains("nothing exposed"), "{error}");
    assert_step(&logged, "confine");
    assert_recorded_at(&logged, "sandbox step failed", tracing::Level::ERROR);
    assert_eq!(logged.matches("sandbox step failed").count(), 1, "{logged}");
}

/// **The grant phase's two halves, pinned without a launch**: the measurement is
/// `debug!` and the warning past the threshold is `warn!` — and the threshold is
/// asserted *as a boundary* rather than as a comparison, one millisecond on
/// either side of it.
///
/// **The quiet half is the assertion that matters to an operator, and the
/// capture is why it can be made at all.** The device records every level at
/// `DEBUG`, so a phase that stayed under the threshold still leaves its line in
/// the buffer — what makes it quiet for an operator is that the line is *below*
/// `INFO`, the level `serve` builds its subscriber with, and that is what the
/// first half asserts ([`assert_recorded_below`]); the `debug!` beside the
/// warning is invisible to such a subscriber on purpose. An event moved from
/// `warn!` to `debug!`, or a threshold moved above the measured case, would
/// leave this test's first half green and its second red, which is the pair of
/// mistakes one warning can make.
///
/// The remedy is asserted on too, because it is the half of the warning that
/// cannot be derived from the code: the line has to point at the design's own
/// guidance — narrow the exposures — rather than read as something the launcher
/// will do about the cost (the function's doc states what the warning can and
/// cannot promise).
#[test]
fn the_grant_phase_warns_only_past_a_share_of_the_readiness_budget() {
    let _recorder = recorder();
    let just_under = GRANT_PHASE_WARN_AFTER - Duration::from_millis(1);
    let ((), quiet) = recorded(|| report_grant_phase(just_under, 3));
    assert_recorded_below(&quiet, tracing::Level::INFO);

    let ((), loud) = recorded(|| report_grant_phase(GRANT_PHASE_WARN_AFTER, 3));
    assert!(loud.contains("WARN"), "{loud}");
    assert!(
        loud.contains(&format!(
            "elapsed_ms={}",
            GRANT_PHASE_WARN_AFTER.as_millis()
        )),
        "{loud}"
    );
    assert!(loud.contains("exposures=3"), "{loud}");
    assert!(
        loud.contains(&format!(
            "threshold_ms={}",
            GRANT_PHASE_WARN_AFTER.as_millis()
        )),
        "{loud}"
    );
    assert!(loud.contains("narrowest directory"), "{loud}");
    assert!(loud.contains("does not shorten it"), "{loud}");
}

/// **A standing grant is `debug!`, and the phase's own event is the `info!`.**
///
/// P2's logging split puts each ACE grant at `debug!` and the phase at `info!`,
/// and this is the half an operator's log level decides: `serve`'s subscriber
/// prints at `INFO`, so a per-ACE line would put one entry in the log for every
/// standing grant on every launch — the program directory and then the `*.dll`
/// batch beside it — where what an operator reads is the phase's one line.
///
/// **The device records every level at `DEBUG`, so the assertion is on the level
/// of the recorded lines rather than on their absence** ([`assert_recorded_below`]):
/// a per-ACE grant left in the buffer is fine as long as a subscriber at `INFO`
/// prints none of it, which is the property the split exists for — and what a
/// `warn!` or an `info!` slipped into that path still fails.
#[test]
// `Grants::new` takes an `Arc<PackageSid>` over a type that owns a raw `PSID` and
// is deliberately neither `Send` nor `Sync` — the same `expect`, for the same
// reason, as `src/acl.rs`'s tests module: this is shared ownership inside one
// thread, which is what the code under test is given.
#[expect(
    clippy::arc_with_non_send_sync,
    reason = "test context: `Grants::new` takes an `Arc<PackageSid>` over a type that owns a raw `PSID` and is deliberately neither `Send` nor `Sync`; this is shared ownership inside one thread, which is what the code under test is given"
)]
fn a_standing_grant_is_not_an_info_event() {
    let _recorder = recorder();
    let dir = tempdir().expect("temp dir");
    let sid = Arc::new(PackageSid::derive(&package_name(dir.path())).expect("the profile"));
    let mut grants = Grants::new(sid);
    let ((), logged) = recorded(|| {
        grants
            .apply_standing(dir.path(), Target::ProgramDirectory, AccessMode::ReadOnly)
            .expect("grant the standing SID");
    });
    assert_recorded_below(&logged, tracing::Level::INFO);
}

/// **Decision 22's rule, as Ruling 78 reshaped it**: the program's own directory
/// read-only, **and** the `*.dll` files directly beside the program read-only —
/// not the other files, and not a subdirectory.
///
/// The files that must *not* appear are the rule rather than an afterthought: a
/// directory holding a text file, and a subdirectory holding a DLL, is exactly the
/// exposure whole-directory inheritance used to grant and this rule deliberately
/// does not (Ruling 78 refused option D).
#[test]
fn the_executables_directory_is_exposed_read_only() {
    // Measured 2026-09-21: the image itself is opened with the launcher's
    // access and loads without an ACE, but a binary that imports a DLL beside
    // it starts and then dies `0xC0000135` without one — so the grant is for
    // the child's own loader (design decision 22).
    let sandbox = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadWrite);
    let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
    let exe_dir = all
        .iter()
        .find(|e| e.host == Path::new(r"E:\build"))
        .expect("the executable's directory is exposed");
    assert_eq!(exe_dir.access, AccessMode::ReadOnly);

    // The same rule on a real tree, where what lies beside the program decides
    // what the exposure set holds: the program's directory, the sibling DLL, and
    // neither a non-DLL sibling nor a DLL one level down.
    let dir = tempfile::tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    let nested = program_dir.join("nested");
    fs::create_dir_all(&nested).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    fs::write(program_dir.join("app-local.dll"), b"x").expect("write a sibling");
    fs::write(program_dir.join("notes.txt"), b"x").expect("write a non-DLL sibling");
    fs::write(nested.join("nested.dll"), b"x").expect("write a nested DLL");
    let program = fs::canonicalize(program_dir.join("serve.exe")).expect("canonical program");
    let canonical_dir = fs::canonicalize(&program_dir).expect("canonical directory");

    let sandbox = Sandbox::new().expose(dir.path().join("root"), AccessMode::ReadWrite);
    let all = exposures(&sandbox, &program);

    let entry = all
        .iter()
        .find(|exposure| exposure.host == canonical_dir)
        .expect("the program's directory is exposed");
    assert_eq!(
        entry.access,
        AccessMode::ReadOnly,
        "the program's directory is exposed read-only: {all:?}"
    );

    let dll = all
        .iter()
        .find(|exposure| is_the_file(&canonical_dir, "app-local.dll", &exposure.host))
        .expect("the sibling DLL is exposed");
    assert_eq!(
        dll.access,
        AccessMode::ReadOnly,
        "the sibling DLL is exposed read-only: {all:?}"
    );
    assert_eq!(
        all.iter()
            .filter(|exposure| is_the_file(&canonical_dir, "app-local.dll", &exposure.host))
            .count(),
        1,
        "the sibling DLL is exposed once: {all:?}"
    );

    assert!(
        !all.iter()
            .any(|exposure| is_the_file(&canonical_dir, "notes.txt", &exposure.host)),
        "a non-DLL sibling is not exposed: {all:?}"
    );
    assert!(
        !all.iter()
            .any(|exposure| is_the_file(&nested, "nested.dll", &exposure.host)),
        "a DLL below the program's directory is not exposed: {all:?}"
    );
}

/// The program's directory is exposed even when it holds no DLL at all — the
/// product's own `target/debug/examples` is 539 files and 0 DLLs — and a program
/// whose directory does not exist is not a failure of the exposure set: the
/// directory entry is still produced and the resolution that follows reports the
/// missing path, which is what `confine`'s failed-launch test relies on
/// (a directory that did not exist would fail before a single ACE was granted, and
/// that test needs the ACLs on).
#[test]
fn a_program_directory_with_no_dll_adds_no_file_entry() {
    // **A tree this test owns, so the count below is a fact about the fixture
    // rather than about `E:\build`.** The count is the assertion here ("the
    // served root and the program's directory, and no DLL entry"), and the only
    // way it can be one is for the program's directory to be one this test
    // filled — a fixed path would make the test depend on what happens to be
    // beside `E:\build\serve.exe` on the host running it.
    let dir = tempfile::tempdir().expect("temp dir");
    let program_dir = dir.path().join("build");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    let sandbox = Sandbox::new().expose(dir.path().join("srv"), AccessMode::ReadOnly);
    let all = exposures(&sandbox, &program_dir.join("serve.exe"));
    assert_eq!(
        all.len(),
        2,
        "the served root and the program's directory, and no DLL entry: {all:?}"
    );
    assert!(
        all.iter()
            .any(|exposure| ends_with(&exposure.host, "build")),
        "the program's directory is exposed whatever it holds: {all:?}"
    );

    let missing = exposures(&sandbox, Path::new("E:/nowhere/serve.exe"));
    assert!(
        missing
            .iter()
            .any(|exposure| ends_with(&exposure.host, "nowhere")),
        "a program directory that does not exist is still exposed: {missing:?}"
    );
}

/// The exposure set keeps every entry it produced before the rule changed — the
/// caller's exposures with their own access mode, and the program's directory —
/// and adds nothing but the DLLs beside it.
#[test]
fn every_entry_the_callers_set_carried_is_still_there() {
    let sandbox = Sandbox::new()
        .expose("E:/srv/tinio", AccessMode::ReadWrite)
        .expose("E:/state", AccessMode::ReadWrite);
    let all = exposures(&sandbox, Path::new("E:/build/serve.exe"));
    assert!(
        all.iter().any(|exposure| ends_with(&exposure.host, "tinio")
            && exposure.access == AccessMode::ReadWrite),
        "{all:?}"
    );
    assert!(
        all.iter()
            .any(|exposure| ends_with(&exposure.host, "state")),
        "{all:?}"
    );
    let program_dir = all
        .iter()
        .find(|exposure| ends_with(&exposure.host, "build"))
        .expect("the program's directory is exposed");
    assert_eq!(program_dir.access, AccessMode::ReadOnly);
}

/// The public half of the production combination: a caller exposes the launcher's
/// own directory through a **raw** path, and `canonical_exposures` keeps the
/// caller's entry and its access, so the merge cannot narrow a path the caller
/// wrote.
///
/// **The shape decision that follows the merge is asked of the private surface
/// elsewhere in this module** (`a_caller_who_exposed_the_launchers_own_directory_keeps_it_inherited`
/// carries `canonical_whitelist` and `shape_for`); what this test keeps is what it
/// can reach from the two public pure helpers (`exposures`, `canonical_exposures`)
/// on a real tree and a raw spelling, which is the input that private test's
/// production combination starts from.
#[test]
fn the_callers_access_survives_the_executable_exposure() {
    // The program directory's entry is read-only and must never widen a path
    // the caller exposed read-write — the merge rule, applied to the one entry
    // this rule adds.
    let sandbox = Sandbox::new().expose(r"E:\build", AccessMode::ReadWrite);
    let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].access, AccessMode::ReadWrite);

    // The same rule on a real tree, where the raw spelling is a path the volume
    // resolves rather than one that names nothing.
    let dir = tempfile::tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    // The raw path is what the caller writes; a canonical one would hide the
    // spelling question this test exists to keep in view.
    let caller_exposed = program_dir.clone();
    assert!(
        !caller_exposed
            .as_os_str()
            .to_string_lossy()
            .starts_with(r"\\?\"),
        "the caller's spelling is raw: {caller_exposed:?}"
    );
    let sandbox = Sandbox::new().expose(&caller_exposed, AccessMode::ReadWrite);
    let program = program_dir.join("serve.exe");

    let canonical_dir = fs::canonicalize(&program_dir).expect("the program's directory resolves");
    let all = canonical_exposures(exposures(&sandbox, &program)).expect("the entries resolve");
    assert_eq!(
        all.len(),
        1,
        "the caller's directory and the launcher's entry are one path: {all:?}"
    );
    assert_eq!(
        all[0].host, canonical_dir,
        "the granted spelling is the volume's: {all:?}"
    );
    assert_eq!(
        all[0].access,
        AccessMode::ReadWrite,
        "the caller's access survives the merge: {all:?}"
    );
}

/// **The sibling DLLs are listed and counted, and they are not resolved**: the
/// banner names them and the count includes them, because an operator reads both
/// and neither may quietly change meaning — while the entries the per-path grant
/// loop walks are the caller's and the program's directory alone, which is the
/// shape the one `*.dll` batch needs.
///
/// The spelling is the assertion that the resolution really was skipped: the
/// listed DLL is the path [`SiblingScan`] found, not the verbatim form
/// `fs::canonicalize` would have produced for it, while the program's directory
/// beside it *is* the resolved spelling.
#[test]
fn the_sibling_dlls_are_listed_and_counted_without_being_resolved() {
    let dir = tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    fs::write(program_dir.join("app-local.dll"), b"x").expect("write a sibling");
    let served = dir.path().join("srv");
    fs::create_dir_all(&served).expect("create the served root");

    // **The pattern's own contract, ahead of the whitelist that reads it**: a
    // directory with no DLL has no pattern to grant, and the one that has a DLL
    // spells the pattern in its own directory — never the parent's, whose same
    // `*.dll` leaf and suffix would name a set the grant never covers.
    let bare = dir.path().join("bare");
    fs::create_dir_all(&bare).expect("create a directory with no DLL");
    fs::write(bare.join("serve.exe"), b"not really an image").expect("write the program");
    fs::write(bare.join("notes.txt"), b"x").expect("write a non-DLL");
    assert_eq!(
        SiblingScan::scan(&bare).pattern(),
        None,
        "a directory with no DLL has no pattern to grant"
    );

    let scan = SiblingScan::scan(&program_dir);
    let pattern = scan.pattern().expect("a sibling makes a pattern");
    assert_eq!(
        pattern.file_name().and_then(|name| name.to_str()),
        Some("*.dll")
    );
    // **The base is the assertion that matters**, and it is the one the first
    // draft of this test missed: a pattern built in the *parent* has the same
    // `*.dll` leaf and the same suffix, so only the base distinguishes the rule
    // that grants the program's own siblings from one that grants its parent's
    // (and misses the program's).
    assert_eq!(
        pattern.parent(),
        Some(program_dir.as_path()),
        "the pattern is built in the program's directory, not its parent: {pattern:?}"
    );
    assert_eq!(pattern, program_dir.join("*.dll"), "{pattern:?}");
    assert_eq!(
        SiblingScan::scan(&program_dir.join("never-created")).pattern(),
        None,
        "a directory that cannot be read has no pattern either"
    );

    let sandbox = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    let whitelist = canonical_whitelist(&sandbox, Some(&scan)).expect("the whitelist resolves");

    // The grant's own set: the caller's root and the program's directory, both in
    // the volume's spelling — and no DLL, because the batch is not the loop's.
    assert_eq!(
        whitelist.granted().len(),
        2,
        "the per-path loop walks the caller's and the program's directories: {:?}",
        whitelist.granted()
    );
    let resolved = fs::canonicalize(&program_dir).expect("the program's directory resolves");
    assert_eq!(
        whitelist.granted()[1].host,
        resolved,
        "the program's directory is resolved on the one pass"
    );
    // **The same resolution is the value `shape_for` compares against**, so a
    // third `fs::canonicalize` of it would be a second answer to a question this
    // one has already answered.
    assert_eq!(
        whitelist.program_dir(),
        Some(resolved.as_path()),
        "the comparison value is the resolved entry, not a second resolution of it"
    );

    // The banner's list and its count: the same entries plus one per sibling DLL.
    assert_eq!(whitelist.listed_len(), 3, "{}", whitelist.describe());
    let listed = whitelist.describe();
    // The DLL carries the scan's own spelling rather than the volume's, and the
    // two are asserted to differ first, so the leg cannot pass vacuously on a
    // host where the resolution is the identity.
    let dll = &scan.files()[0];
    let resolved_dll = fs::canonicalize(dll).expect("the sibling DLL resolves");
    assert_ne!(*dll, resolved_dll, "the two spellings differ: {dll:?}");
    assert!(
        listed.contains(&format!("{} (read-only)", dll.display())),
        "the listed DLL carries the scan's own spelling, because nothing resolved it: {listed}"
    );
    assert!(
        !listed.contains(&format!("{} (read-only)", resolved_dll.display())),
        "and not the resolved spelling, which is what resolving it would have produced: {listed}"
    );

    // The enumerate side the pattern must agree with: the exposure set names
    // exactly the DLL directly inside the directory, and the pattern is that same
    // directory's — one level up would name a set the grant never covers.
    let all = exposures(&sandbox, &program_dir.join("serve.exe"));
    let enumerated = all
        .iter()
        .filter(|exposure| exposure.host.is_file())
        .count();
    assert_eq!(enumerated, 1, "one sibling DLL is exposed: {all:?}");
    let base = pattern.parent().expect("the pattern has a directory");
    assert!(
        all.iter()
            .all(|exposure| !exposure.host.is_file() || exposure.host.parent() == Some(base)),
        "every enumerated sibling lies in the pattern's directory: {all:?}"
    );
    assert_eq!(
        SiblingScan::scan(base).pattern(),
        Some(pattern),
        "the pattern is derived from that same directory"
    );
}

/// **A sibling DLL that is itself a reparse point is refused**, and the refusal is
/// the one a caller's linked path earns: `icacls` followed a file symlink named
/// `x.dll` and wrote the ACE on its **target** instead of beside the program — the
/// defect `refuse_reparse_points` exists for, one component further in, and the
/// defect the name-based enumeration closes. The sibling half is checked on the
/// whitelist's own pass
/// (`refuse_a_reparse_sibling`, one attribute read per DLL and no second walk), so
/// this is the site that has to answer.
///
/// **A file symlink, and a junction cannot stand in for one**: a junction targets a
/// directory, and the enumeration keeps only the entries `is_file` answers for —
/// so the leg would assert nothing. Creating one needs
/// `SeCreateSymbolicLinkPrivilege` or Developer Mode, and a host without it skips
/// this leg rather than passing it on a whitelist the host never built
/// (`testing::host_cannot`).
#[test]
fn a_sibling_dll_that_is_a_reparse_point_is_refused() {
    let dir = tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    let real = program_dir.join("app-local.dll");
    fs::write(&real, b"x").expect("write a sibling");
    let link = program_dir.join("linked.dll");
    if symlink_file(&real, &link).is_err() {
        testing::host_cannot(
            "a sibling DLL that is a reparse point",
            "creating a file symlink needs SeCreateSymbolicLinkPrivilege or Developer Mode",
            "; enable Developer Mode or run this suite with the privilege",
        );
        return;
    }
    let served = dir.path().join("srv");
    fs::create_dir_all(&served).expect("create the served root");

    let sandbox = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    let scan = SiblingScan::scan(&program_dir);
    // **The link is enumerated, or the refusal below would be about a file set the
    // launcher never saw**: `is_file` follows the link, so a linked sibling is one
    // of the entries the `*.dll` pattern covers.
    assert!(
        scan.files().iter().any(|file| file == &link),
        "the linked sibling is enumerated: {:?}",
        scan.files()
    );

    let (error, logged) = recorded(|| {
        canonical_whitelist(&sandbox, Some(&scan)).expect_err("a linked sibling is refused")
    });
    match &error {
        Error::BadPath { path, reason } => {
            assert_eq!(path, &link, "the refusal names the sibling: {error}");
            assert!(reason.contains("is a reparse point"), "{reason}");
        }
        other => panic!("a reparse point is refused with a `BadPath`: {other}"),
    }
    // The step and the path are the ones a linked caller's path is refused with:
    // one rule, and one place an operator finds it in a log.
    assert_step(&logged, "resolve an exposed path");
    assert!(
        logged.contains(&format!("path={}", link.display())),
        "{logged}"
    );
    assert_eq!(logged.matches("sandbox step failed").count(), 1, "{logged}");
}

/// **A junction named like a sibling DLL is enumerated and refused**, which is the
/// half the old enumeration could not see.
///
/// `sibling_dlls` filtered with `path.is_file()`, which follows a link and keeps
/// only regular files — so a junction to a directory answered "not a file" and was
/// **dropped from the set**, while the grant that followed it was the tool's own
/// `*.dll` wildcard, which matched the name and wrote through the link. A *file*
/// symlink was worse: `is_file` answered true, the entry stayed, and `icacls`
/// followed it and put the machine-wide ACE on the symlink's **target**, outside
/// the program's directory (measured on this host 2026-09-27 — the file-symlink
/// leg above pins the same enumeration, and the grant now opens every match by
/// handle so no link is followed either way).
///
/// **A junction rather than a symlink, because it needs no privilege to create**:
/// `mklink /J` is unprivileged, and a junction to a *directory* is exactly the
/// entry the old filter dropped. A directory named `x.dll` is listed by name too
/// and now fails the launch at the grant's kind check — the same fail-closed
/// direction, for an entry that used to be silently skipped.
#[test]
fn a_junction_named_like_a_sibling_dll_is_refused() {
    let dir = tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    let outside = dir.path().join("outside");
    fs::create_dir_all(&outside).expect("create the junction's target");
    let link = junction(&program_dir, "linked.dll", &outside);
    let served = dir.path().join("srv");
    fs::create_dir_all(&served).expect("create the served root");

    let sandbox = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    let scan = SiblingScan::scan(&program_dir);
    // **The link is enumerated, which is the fix**: by name and with no
    // follow-the-link filter in front of it.
    assert!(
        scan.files().iter().any(|file| file == &link),
        "the junction is enumerated by name: {:?}",
        scan.files()
    );

    let (error, logged) =
        recorded(|| canonical_whitelist(&sandbox, Some(&scan)).expect_err("a junction is refused"));
    match &error {
        Error::BadPath { path, reason } => {
            assert_eq!(path, &link, "the refusal names the sibling: {error}");
            assert!(reason.contains("is a reparse point"), "{reason}");
        }
        other => panic!("a reparse point is refused with a `BadPath`: {other}"),
    }
    assert_step(&logged, "resolve an exposed path");
    assert_eq!(logged.matches("sandbox step failed").count(), 1, "{logged}");
}

/// **The shape the launcher gives each exposed directory, and the precedence
/// that decides it** — the half of the Ruling 78 regression that no DACL check
/// can see, because a grant written in the wrong shape still reports success.
#[test]
fn the_program_directory_is_the_one_directory_that_does_not_propagate() {
    // **A real tree, because the caller's set is the type's now** (P1): the
    // only way to build one is [`canonical_whitelist`], which resolves and checks
    // every entry, so a literal spelling that names nothing would be a failed
    // `fs::canonicalize` rather than a test of the precedence rule. What the
    // two halves still separate is the question the test asks: the served root
    // is the caller's, the program's directory is not.
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("srv");
    let program_dir = dir.path().join("build");
    fs::create_dir_all(&root).expect("create the served root");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    let program = program_dir.join("serve.exe");
    fs::write(&program, b"not really an image").expect("write the program");
    let canonical_root = fs::canonicalize(&root).expect("the served root resolves");
    let canonical_program_dir =
        fs::canonicalize(&program_dir).expect("the program's directory resolves");

    let sandbox = Sandbox::new().expose(&root, AccessMode::ReadWrite);
    let all = canonical_exposures(exposures(&sandbox, &program)).expect("the entries resolve");
    // **The caller's set is [`canonical_whitelist`]'s answer and nothing else**
    // (P1): [`shape_for`] takes the type, so the raw set does not compile here.
    // No sibling scan is behind it, because this test asks the precedence
    // question and not what the launcher adds beside it.
    let whitelist = canonical_whitelist(&sandbox, None).expect("the caller's root resolves");
    let caller_set = whitelist.caller();

    // The caller's own exposure keeps the inherited form, which is what makes
    // a file already inside it readable by the child without a `/t` walk.
    let caller = all
        .iter()
        .find(|exposure| exposure.host == canonical_root)
        .expect("the caller's root is exposed");
    assert_eq!(
        shape_for(caller_set, caller, Some(&canonical_program_dir)),
        Target::Directory
    );

    // The entry decision 22 adds does not.
    let own = all
        .iter()
        .find(|exposure| exposure.host == canonical_program_dir)
        .expect("the program's directory is exposed");
    assert_eq!(
        shape_for(caller_set, own, Some(&canonical_program_dir)),
        Target::ProgramDirectory
    );

    // A program directory the caller exposed itself keeps the caller's shape:
    // the merge rule gives the caller's access priority, and this rule may not
    // quietly narrow it.
    let caller_owns_it = Sandbox::new().expose(&program_dir, AccessMode::ReadWrite);
    let all =
        canonical_exposures(exposures(&caller_owns_it, &program)).expect("the entries resolve");
    let merged = all
        .iter()
        .find(|exposure| exposure.host == canonical_program_dir)
        .expect("the program's directory is exposed");
    assert_eq!(merged.access, AccessMode::ReadWrite);
    let owns_whitelist =
        canonical_whitelist(&caller_owns_it, None).expect("the caller's directory resolves");
    assert_eq!(
        shape_for(
            owns_whitelist.caller(),
            merged,
            Some(&canonical_program_dir)
        ),
        Target::Directory,
        "the caller's own exposure keeps the inherited shape"
    );

    // A program directory that could not be resolved is not recognized at all,
    // which errs towards granting more rather than towards a file the child
    // cannot read. **The no-caller branch needs the type too**, so the one
    // input it can be asked with is an empty canonical set.
    assert_eq!(shape_for(&CallerSet::empty(), own, None), Target::Directory);
}

/// **The three spec strings, pinned where no DACL is needed** (Ruling 85's
/// division of labour): the caller's exposed directories keep `(OI)(CI)` — the
/// inherited form whose whole purpose is that a file already inside them stays
/// reachable — while the program's own directory and every file take the plain
/// form. A launcher that sent the plain form to the caller's directories is the
/// regression this pins, and it is invisible to any check that only reads whether
/// a grant reported success.
#[test]
fn the_three_shapes_are_the_ones_the_launcher_grants() {
    let sid = "S-1-15-2-1-2-3";

    // The caller's exposures: inherited, so a pre-existing descendant of the
    // served root or the report directory is reachable by the child (design §6).
    assert_eq!(
        acl::grant_spec(sid, Target::Directory, AccessMode::ReadWrite),
        "*S-1-15-2-1-2-3:(OI)(CI)M"
    );
    assert_eq!(
        acl::grant_spec(sid, Target::Directory, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:(OI)(CI)RX"
    );

    // The program's own directory: a directory, and deliberately no propagation.
    assert_eq!(
        acl::grant_spec(sid, Target::ProgramDirectory, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:RX"
    );

    // A file: the plain form, because the inheritance flags are a no-op on one.
    assert_eq!(
        acl::grant_spec(sid, Target::File, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:RX"
    );
    assert_eq!(
        acl::grant_spec(sid, Target::File, AccessMode::ReadWrite),
        "*S-1-15-2-1-2-3:M"
    );
}

/// **The ordinary spelling is the caller's, and this is the assertion that keeps
/// it so.** The variant a call site reaches for when it has a directory in hand
/// must be the inherited one; decision 22's entry is the only site in this crate
/// that spells the exception, and it is named after the decision. This pins the
/// direction rather than the mechanism: a future rename that made the plain form
/// the obvious spelling would fail here before it could silently narrow a caller's
/// exposure.
#[test]
fn the_obvious_directory_spelling_is_the_one_that_propagates() {
    assert_eq!(
        acl::grant_spec("S-1-15-2-1-2-3", Target::Directory, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:(OI)(CI)RX",
        "a caller with a directory in hand means the inherited shape"
    );
    assert_eq!(
        acl::grant_spec(
            "S-1-15-2-1-2-3",
            Target::ProgramDirectory,
            AccessMode::ReadOnly
        ),
        "*S-1-15-2-1-2-3:RX",
        "and the exception is spelled out"
    );
}

/// **A caller's exposed file is refused at launch, and this is the site that
/// refuses it** (P7, Ruling 95): the caller's set is the only one that knows
/// which entries are the caller's, so the refusal lives with the type that
/// holds it rather than in the loop that would silently drop it.
///
/// **The launcher's own file entries are the branch beside it and they are
/// *not* refused**, because they never enter this set: [`exposures`] adds them
/// to the spelled set, which the public path resolves through
/// [`canonical_exposures`] and which [`canonical_whitelist`] keeps aside as the
/// sibling batch rather than checking. The refusal exists because a caller's file
/// would otherwise be granted nothing and the server would run on with a quiet
/// fail-closed (the defect P7 records).
#[test]
fn a_caller_exposed_file_is_refused_rather_than_silently_dropped() {
    let dir = tempdir().expect("temp dir");
    let file = dir.path().join("exposed.txt");
    fs::write(&file, b"x").expect("write the caller's file");
    let canonical = fs::canonicalize(&file).expect("the file resolves");

    // The path that is wrong is the one the error has to name, and the one the
    // grant loop would have been handed if the refusal had not dropped it.
    let err = canonical_whitelist(&Sandbox::new().expose(&file, AccessMode::ReadOnly), None)
        .expect_err("an exposed file is refused");
    assert!(
        err.to_string().contains(&canonical.display().to_string()),
        "{err}"
    );
    assert!(err.to_string().contains("is not a directory"), "{err}");

    // The launcher's own entries for the very same shape are not what this is
    // about — the entry `exposures` adds for a sibling `*.dll` is a file too,
    // and it never enters the caller's set. **The two halves run against one
    // tree**, so what separates them is the set each is asked about and not the
    // directory each happens to be built in.
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    fs::write(program_dir.join("app-local.dll"), b"x").expect("write a sibling");
    let served = dir.path().join("srv");
    fs::create_dir_all(&served).expect("create the served root");
    let sandbox = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    // The granted set carries the launcher's file entry, and the whole of the
    // public path resolves it — no refusal fires for the launcher's own file.
    // **Every entry `exposures` adds for a sibling `*.dll` is a file, and it
    // never enters the caller's set**: `confine` grants it through
    // `acl::Grants::apply_siblings`' one `*.dll` invocation, not through the
    // per-path loop the caller's entries take — so a refusal that read the
    // *granted* set would refuse the launcher's own launch, and the assertions
    // below are the half of P7's distinction that says so.
    let all = exposures(&sandbox, &program_dir.join("serve.exe"));
    let launcher_file = all
        .iter()
        .find(|exposure| ends_with(&exposure.host, "app-local.dll"))
        .expect("the sibling DLL is exposed");
    assert!(
        launcher_file.host.is_file(),
        "the launcher's sibling entry really is a file: {launcher_file:?}"
    );
    let canonical_dll = fs::canonicalize(&launcher_file.host).expect("the sibling DLL resolves");
    let granted = canonical_exposures(all).expect("the launcher's own file entries resolve");
    assert!(
        granted
            .iter()
            .any(|exposure| exposure.host == canonical_dll),
        "the launcher's file entry survives into the granted set: {granted:?}"
    );
    assert!(
        granted.iter().any(|exposure| exposure.host.is_file()),
        "the granted set carries the launcher's own file entry: {granted:?}"
    );
    // The caller's set, built from the same tree, is the caller's entries alone.
    let whitelist = canonical_whitelist(&sandbox, None).expect("the served root is a directory");
    let caller = whitelist.caller();
    assert!(
        caller.exposed(&fs::canonicalize(&served).expect("the served root resolves")),
        "the caller's own directory is in its set: {caller:?}"
    );
    assert!(
        !caller.exposed(&fs::canonicalize(&program_dir).expect("the directory resolves")),
        "the launcher's own entries are not in the caller's set: {caller:?}"
    );
}

/// **The combination production actually builds, and it is this module's test
/// because [`shape_for`] and [`canonical_whitelist`] are private**: a caller
/// exposes the launcher's own directory through a **raw** path, and the entry
/// must still be the caller's — so it keeps [`Target::Directory`], the
/// inherited form, with the caller's access.
///
/// **It calls [`canonical_whitelist`] rather than reproducing it**, which is the
/// whole point of moving the assertion in here: the integration test that asked
/// this question first re-implemented the function with `fs::canonicalize` (a
/// private one is out of its reach), so it stayed green if the caller's set was
/// ever built another way. `tempfile` is a dev-dependency, so the caller's
/// spelling is a real directory and the production call is what is under test.
///
/// **The other no-caller branch is asked of the same type**: `shape_for` with
/// no resolved program directory and an empty caller set is the state a caller
/// reaches `confine` in only after [`served_root`] has refused the truly empty
/// exposure set.
#[test]
fn a_caller_who_exposed_the_launchers_own_directory_keeps_it_inherited() {
    let dir = tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    // The raw path is what the caller writes — the harness's own idiom, and the
    // spelling `Sandbox::expose` is handed in production (the server's launcher
    // arm builds the set from the operator's configuration, not from resolved
    // paths).
    let caller_exposed = program_dir.clone();
    assert!(
        !caller_exposed
            .as_os_str()
            .to_string_lossy()
            .starts_with(r"\\?\"),
        "the caller's spelling is raw: {caller_exposed:?}"
    );
    let sandbox = Sandbox::new().expose(&caller_exposed, AccessMode::ReadWrite);
    let program = program_dir.join("serve.exe");

    // `confine`'s own two lines, called rather than reproduced: the caller's
    // set from `canonical_whitelist` and the public spelled set from
    // `canonical_exposures(exposures(…))`.
    let canonical_dir = fs::canonicalize(&program_dir).expect("the program's directory resolves");
    let whitelist = canonical_whitelist(&sandbox, None).expect("the caller's path resolves");
    let caller = whitelist.caller();
    assert_eq!(
        caller.entries().len(),
        1,
        "the caller's own set is the one entry: {caller:?}"
    );
    assert_eq!(
        caller.entries()[0].host,
        canonical_dir,
        "the caller's set resolves to the volume's spelling: {caller:?}"
    );
    assert_eq!(caller.entries()[0].access, AccessMode::ReadWrite);
    assert_ne!(
        sandbox.exposures()[0].host,
        canonical_dir,
        "the caller's raw spelling differs from the resolved one, which is what \
             makes the membership assertions below mean something"
    );
    assert!(
        caller.exposed(&canonical_dir),
        "the type's membership answer is the resolved spelling's: {caller:?}"
    );
    assert!(
        !caller.exposed(&sandbox.exposures()[0].host),
        "and not the raw caller's, which is the spelling the type exists to keep out"
    );
    let all = canonical_exposures(exposures(&sandbox, &program)).expect("the entries resolve");
    let entry = all
        .iter()
        .find(|exposure| exposure.host == canonical_dir)
        .expect("the program's directory is in the granted set");
    assert_eq!(
        entry.access,
        AccessMode::ReadWrite,
        "the caller's access survives the merge: {all:?}"
    );
    assert_eq!(
        shape_for(caller, entry, Some(&canonical_dir)),
        Target::Directory,
        "the caller's own entry keeps the inherited shape, not decision 22's: {all:?}"
    );

    // **The branch the canonical spelling is there to avoid is no longer
    // expressible**: the assertion used to hand `shape_for` the caller's *raw*
    // set and watch it answer decision 22's variant, and that argument does not
    // compile now — `shape_for` takes the `CallerSet`, and the raw set is not
    // one (P1). What replaces it is the membership answer above, on both
    // spellings at once: the type's set answers for the resolved path, and the
    // caller's raw spelling is precisely what it cannot be asked about.

    // The other half is the production expectation rather than a formality: a
    // caller set that does not name the program's directory goes through
    // `canonical_whitelist` too, and answers decision 22's variant.
    let served = dir.path().join("srv");
    fs::create_dir_all(&served).expect("create the served root");
    let launchers_own = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    let served_whitelist =
        canonical_whitelist(&launchers_own, None).expect("the served root resolves");
    let launchers_set =
        canonical_exposures(exposures(&launchers_own, &program)).expect("the entries resolve");
    let own = launchers_set
        .iter()
        .find(|exposure| exposure.host == canonical_dir)
        .expect("the program's directory is in the granted set");
    assert_eq!(
        shape_for(served_whitelist.caller(), own, Some(&canonical_dir)),
        Target::ProgramDirectory,
        "the entry decision 22 adds is the launcher's own: {launchers_set:?}"
    );
}

#[test]
fn a_duplicate_host_keeps_the_widest_access() {
    let merged = merge(vec![
        Exposure {
            host: r"E:\data".into(),
            access: AccessMode::ReadWrite,
        },
        Exposure {
            host: r"E:\data".into(),
            access: AccessMode::ReadOnly,
        },
    ]);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].access, AccessMode::ReadWrite);
}

#[test]
fn nothing_outside_the_exposures_is_exposed() {
    let sandbox = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadOnly);
    let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
    assert!(!all.iter().any(|e| e.host == Path::new(r"E:\srv\other")));
    assert!(!all.iter().any(|e| e.host == Path::new(r"E:\")));
}

#[test]
fn capabilities_parse_by_name() {
    assert_eq!(
        Capability::from_name("internetClient").unwrap(),
        Capability::InternetClient
    );
    assert_eq!(
        Capability::from_name("privateNetworkClientServer").unwrap(),
        Capability::PrivateNetworkClientServer
    );
    assert!(Capability::from_name("bogus").is_err());
}

/// **`from_name`'s half of the round trip, which is the half the product
/// takes**: a capability arrives as the name an operator wrote — in
/// `TINIO_SANDBOX_CAPABILITIES`, or in the `Options` a library caller builds —
/// and is turned into the SID the container is created with by
/// [`Capability::sid`]. Nothing produces a SID for this crate to parse back,
/// which is why there is no `from_sid` to round-trip through.
#[test]
fn capability_names_round_trip_through_from_name() {
    for cap in [
        Capability::InternetClient,
        Capability::InternetClientServer,
        Capability::PrivateNetworkClientServer,
    ] {
        assert_eq!(Capability::from_name(cap.name()).unwrap(), cap);
    }
}

#[test]
fn the_default_container_has_no_capability() {
    // Design §7: the outbound guarantee is the *absence* of a network
    // capability, so an empty list is the default and stays empty unless an
    // operator asks otherwise.
    assert!(Options::default().capabilities.is_empty());
    assert!(Capability::parse_list(None).unwrap().is_empty());
    assert!(Capability::parse_list(Some("")).unwrap().is_empty());
    assert_eq!(
        Capability::parse_list(Some("privateNetworkClientServer")).unwrap(),
        vec![Capability::PrivateNetworkClientServer]
    );
    assert!(Capability::parse_list(Some("bogus")).is_err());
}

#[test]
fn the_served_root_is_the_first_exposure() {
    let sandbox = Sandbox::new()
        .expose(r"E:\srv\tinio", AccessMode::ReadOnly)
        .expose(r"E:\state", AccessMode::ReadWrite);
    assert_eq!(served_root(&sandbox).unwrap(), Path::new(r"E:\srv\tinio"));
    assert!(served_root(&Sandbox::new()).is_err());
}

/// **Every exposed path a launch's ledger records besides its root, as one leg** —
/// finding 3 of the 2026-09-28 review, which is that a `TINIO_STATE_DIR` outside
/// the root was granted read-write and never reached the file.
///
/// It would fail if the selection named the root by position alone, or compared
/// spellings literally: a second spelling of the root is one directory, one ACE and
/// one grant, and a line for it would claim a standing ACE the run did not write
/// twice. What the product's set is — the served root, then the state directory
/// when it lies outside the root — is the shape the first assertion pins, and the
/// empty cases are the run whose state directory needs no entry of its own.
#[test]
fn the_paths_a_run_records_besides_its_root_are_its_other_exposures() {
    let sandbox = Sandbox::new()
        .expose(r"E:\srv\tinio", AccessMode::ReadOnly)
        .expose(r"E:\state", AccessMode::ReadWrite);
    assert_eq!(
        other_exposed_paths(&sandbox, Path::new(r"E:\srv\tinio")),
        vec![PathBuf::from(r"E:\state")],
        "the state directory is the ACE the root's own line does not name"
    );
    // **The root's own second spelling is not a second path.** `E:\Data` and
    // `e:/data` are one directory to this volume, so they are one canonical
    // resolution, one grant and one line — the digest is the crate's identity rule
    // and not the spelling (`sid::root_digest`).
    let respelled = Sandbox::new()
        .expose(r"E:\Data", AccessMode::ReadOnly)
        .expose(r"e:/data", AccessMode::ReadWrite);
    assert!(
        other_exposed_paths(&respelled, Path::new(r"E:\Data")).is_empty(),
        "a second spelling of the root earned a line of its own"
    );
    // A root with nothing beside it records nothing extra at all, which is the
    // in-root state directory's case.
    let alone = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadWrite);
    assert!(other_exposed_paths(&alone, Path::new(r"E:\srv\tinio")).is_empty());
}

#[test]
fn a_path_spelled_through_a_junction_is_refused_rather_than_followed() {
    // Measured on this host, 2026-09-27: a junction **above** the last component
    // is resolved by every writer, so `…\link\tinio` put the ACE on
    // `…\real\tinio` — outside the exposure, with that target's own children
    // inheriting it — and a junction as the last component takes an ACE on the
    // link's own object, which is an ACE on nothing the child can reach. Neither
    // is the grant the caller asked for, so a spelled path that carries one
    // anywhere is refused. A junction rather than a symlink because `mklink /J`
    // needs no elevation and a symlink does.
    let dir = tempdir().expect("temp dir");
    let real = dir.path().join("real");
    fs::create_dir_all(&real).expect("create the target");
    let link = junction(dir.path(), "link", &real);

    let exposure = |host: &Path| Exposure {
        host: host.to_path_buf(),
        access: AccessMode::ReadOnly,
    };
    let err = canonical_exposures(vec![exposure(&link)]).expect_err("a junction is refused");
    assert!(err.to_string().contains("reparse point"), "{err}");
    // The target it points at is a directory like any other, and is granted:
    // what is refused is the spelling that would have granted somewhere else.
    let all = canonical_exposures(vec![exposure(&real)]).expect("a real directory resolves");
    assert_eq!(all.len(), 1);
}

#[test]
fn two_spellings_of_one_directory_collapse_to_one_exposure() {
    // The merge *after* the resolution is the one that matters: two spellings
    // that survive the first merge as two entries are one directory once they
    // are resolved, and an unresolved pair would write two ACEs for one SID on
    // one directory — the second of which the grant-only read then reports
    // as already granted.
    let dir = tempdir().expect("temp dir");
    let real = dir.path().join("data");
    fs::create_dir_all(&real).expect("create the directory");
    let all = canonical_exposures(vec![
        Exposure {
            host: real.clone(),
            access: AccessMode::ReadOnly,
        },
        Exposure {
            host: real.join("."),
            access: AccessMode::ReadWrite,
        },
    ])
    .expect("both spellings resolve");
    assert_eq!(all.len(), 1, "{}", describe(&all));
    assert_eq!(
        all[0].access,
        AccessMode::ReadWrite,
        "the widest access survives"
    );
    assert_eq!(all[0].host, fs::canonicalize(&real).expect("canonical"));
}

/// The capability SID `Capability::InternetClient` resolves to.
///
/// Spelled here rather than read from the enum so the ACL this suite writes is
/// an independent statement of which SID it expects the container to carry: a
/// test that asked `Capability::sid()` for the value to put in the `icacls`
/// argument would agree with a wrong constant.
const INTERNET_CLIENT_SID: &str = "S-1-15-3-1";

/// The address the baseline and both confined attempts use.
///
/// `1.1.1.1:53` is the default, and it was chosen for what a refusal would mean:
/// it is a public resolver, so a successful TCP handshake is a real round trip
/// to the internet rather than an artefact of a closed port on a reachable host.
/// `TINIO_SANDBOX_TEST_TARGET` points the suite at whatever address a given host
/// actually reaches.
fn target() -> SocketAddr {
    env::var("TINIO_SANDBOX_TEST_TARGET")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_else(|| "1.1.1.1:53".parse().expect("a literal"))
}

/// Whether an **unconfined** process on this host reaches `address`.
///
/// The baseline, and the only thing this suite knows about the machine rather
/// than about the container. Five seconds is the confined child's own budget for
/// the same attempt (the harness's `child_role_body`), so the two halves are
/// comparable rather than one being more patient than the other.
fn baseline(address: SocketAddr) -> bool {
    TcpStream::connect_timeout(&address, Duration::from_secs(5)).is_ok()
}

/// **The host's precondition, probed before anything is spawned — and the only
/// place this suite's opt-out is decided.** Returns `false` when this machine
/// cannot do what the measurement needs *and* the operator has deliberately said
/// so; the test then returns without asserting anything about the product.
///
/// One capability of the **machine** is asked about: whether an ordinary process
/// here can reach [`target`] at all. A host that cannot reach it unconfined
/// cannot be told apart from a container that refused it, so every leg of this
/// suite would assert nothing — which is why this is a precondition and not a
/// printed note.
///
/// **The control is not reachable from here.** Whether `internetClient` makes
/// the container reach the target is not a property of this machine; it is the
/// finding, and it lives in the test below where it can fail.
///
/// **The assert-before-print order, the variable's name and the remedy in the
/// message are [`testing::host_cannot`]'s**, so this suite, `tinio-server`'s
/// Windows harness and P1's jail test cannot drift apart on any of the three.
fn host_ready(target: SocketAddr) -> bool {
    if baseline(target) {
        return true;
    }
    let reason = format!("an unconfined process on this host cannot reach {target}");
    testing::host_cannot(
        "the network suite",
        &reason,
        ", or set TINIO_SANDBOX_TEST_TARGET to an address this host reaches (design §7)",
    );
    false
}

/// What the confined child reports about reaching out.
///
/// The target travels in the probe file the child already reads, so nothing about
/// this run goes through the environment — the rule every confined role in this
/// crate follows (design decision 17), and the reason `Tree::target` exists.
///
/// **A launch that fails panics inside the harness with the backend's own
/// error**, which is Task 9's rule: a test whose subject never started has
/// nothing to report, and a value it could return `None` on would make "the
/// backend refused to confine anything" look green.
fn confined_outbound(volume: &Path, options: Options, target: SocketAddr) -> String {
    let (_dir, mut tree) = tree(volume);
    tree.target = Some(target);
    // The harness's run, and it is the one whose body this function used to
    // spell: the child probes and reports before it accepts, so the harness's
    // `answer` both waits for it and proves inbound still works — with the
    // connect and the read bounded by `crate::testing::TIMEOUT`, which a block
    // spelling the same four lines without the timeout is the hazard Task 6 paid
    // for. Its `supervise` joins the relay and reaps the child, so a caller's
    // assertion that fails still leaves no process behind.
    run_confined_with(&tree, AccessMode::ReadWrite, options).report
}

/// The outbound guarantee: an address this host reaches is refused inside.
///
/// **The baseline passed or this test did not run**, so the refusal read out of
/// the child's own report is the container's policy rather than a missing route,
/// and the loopback round trip the harness's `answer` performed proves the
/// confinement left inbound alone — the other half of §7's rule, and the reason a
/// refusal cannot be explained by a child that never started.
///
/// **A failure here is a design finding, not a broken test.** An AppContainer
/// with an empty capability set that reaches an address this host reaches
/// unconfined falsifies §7's Windows row, and the row is what would change.
#[test]
fn an_address_that_works_outside_is_refused_inside() {
    let target = target();
    if !host_ready(target) {
        return;
    }
    let volume = one_volume();
    let report = confined_outbound(&volume, Options::default(), target);
    assert!(
        report.contains("outbound: refused"),
        "the container reached {target} with no capability, which §7's Windows row says it \
         cannot: {report}"
    );
}

/// The positive control: the same address, with `internetClient` granted.
///
/// **This asserts, and it is the assertion that found the capability defect.** It
/// is the only measurement here that tells "the container's capability list was
/// read from `Options` and honoured" from "it was silently emptied" — the two are
/// indistinguishable from every other observable in the suite, because a
/// container with no capabilities at all behaves exactly like one whose
/// configured list was dropped. The pair of tests either side of it is what makes
/// each of them mean something: same host, same address, same child, one
/// capability between them.
///
/// **A failure here is a finding, never a flake.** It says the container did not
/// reach an address this host reaches unconfined with `internetClient` granted.
/// On 2026-09-22 it failed for exactly that reason and the cause was in this
/// crate rather than on the host: the capability SIDs were handed to
/// `CreateProcessW` with no attribute set, so the capability sat in the child's
/// token and was honoured by nothing. If it fails again, the two candidates are
/// that regression and a host whose own policy refuses the container's traffic —
/// and [`a_granted_capability_is_enabled_in_the_container`] is what tells the two
/// apart, because it does not touch the network.
#[test]
fn the_same_address_is_reached_with_internet_client() {
    let target = target();
    if !host_ready(target) {
        return;
    }
    let volume = one_volume();
    let options = Options {
        capabilities: vec![Capability::InternetClient],
        ..Default::default()
    };
    let report = confined_outbound(&volume, options, target);
    eprintln!(
        "measured: with internetClient the confined child reported `{}`",
        report.lines().last().unwrap_or_default()
    );
    assert!(
        report.contains("outbound: SUCCEEDED"),
        "the container did not reach {target} with internetClient granted, while this host \
         reaches it unconfined: {report}"
    );
}

/// A capability that is not `internetClient` does not open the internet.
///
/// The third arm, and the one that makes the control's reading a *policy* rather
/// than a coincidence: `privateNetworkClientServer` is the capability §7 says a
/// non-loopback bind needs, so it is a non-empty list that must not reach a
/// public resolver. Together with the test above it separates three outcomes a
/// single capability would confound: a list that is read and honoured per
/// capability, a list that is emptied, and a container that opens the network
/// whenever any capability is present.
#[test]
fn a_capability_that_is_not_internet_does_not_open_the_network() {
    let target = target();
    if !host_ready(target) {
        return;
    }
    let volume = one_volume();
    let options = Options {
        capabilities: vec![Capability::PrivateNetworkClientServer],
        ..Default::default()
    };
    let report = confined_outbound(&volume, options, target);
    assert!(
        report.contains("outbound: refused"),
        "the container reached {target} with privateNetworkClientServer and no internetClient, \
         so a non-empty capability list is what granted it rather than the name of the \
         capability: {report}"
    );
}

/// The same question asked of an ACL instead of a socket.
///
/// **This is Task 9's carry, and the reason it is asked twice.** Its report
/// records that `a_capability_from_the_options_reaches_the_container` — deleted
/// by the 2026-10-01 cleanup, its capability arm now
/// `tests/confine.rs`'s `the_whitelist_refuses_a_path_outside_the_exposures` —
/// could not distinguish "the container's list was read from `Options`" from "it
/// was silently emptied", that no observable in its own suite could, and that the
/// network probe is the only measurement that can. That is true of the *network*
/// arm above — and a network arm is exactly what a locked-down host takes away,
/// since a host that refuses the container every destination reports the same
/// refusal whatever its capabilities are.
///
/// A capability SID is also a token SID an access check consults, so granting one
/// read on a directory and asking the child to read through it is the same
/// question with no network in the way: the child reads the file if and only if
/// the SID is in its token **and enabled**. Measured 2026-09-22, the second half
/// of that is not free — with `Attributes: 0` the ACE granted nothing, and this
/// leg is what makes a regression there fail on every host rather than only on
/// the ones whose outbound happens to work.
#[test]
fn a_granted_capability_is_enabled_in_the_container() {
    let volume = one_volume();
    let granted = capability_is_visible(&volume, vec![Capability::InternetClient]);
    let absent = capability_is_visible(&volume, Vec::new());
    eprintln!(
        "measured: the {INTERNET_CLIENT_SID} ACE was readable with internetClient={granted}, \
         with no capability={absent}"
    );
    assert!(
        granted,
        "a directory whose ACL grants {INTERNET_CLIENT_SID} read was not readable by a container \
         the operator granted internetClient, so either the list did not reach the container's \
         token or it reached it disabled (a capability whose entry is not SE_GROUP_ENABLED is \
         honoured by nothing)"
    );
    assert!(
        !absent,
        "a directory whose ACL grants {INTERNET_CLIENT_SID} read was readable by a container with \
         no capability at all, so this ACE says nothing about the list"
    );
}

/// Whether the confined child reads the `denied` file when `denied` carries an
/// ACE for the `internetClient` capability SID.
///
/// The harness's own tree is reused rather than a new one added: `denied` is the
/// directory the child already tries to read and reports on, it already carries
/// `Users` (so a pass cannot be an inherited ACE nobody meant), and the grant is
/// the suite's own `icacls` call — the launcher never writes a capability ACE,
/// which is the same rule that keeps the confined child out of `icacls` (design
/// decisions 15 and 17).
fn capability_is_visible(volume: &Path, capabilities: Vec<Capability>) -> bool {
    let (_dir, tree) = tree(volume);
    let granted = Command::new(acl::icacls_binary())
        .arg(&tree.denied)
        .args(["/grant", &format!("*{INTERNET_CLIENT_SID}:(OI)(CI)R"), "/q"])
        .output()
        .expect("grant the capability SID");
    assert!(
        granted.status.success(),
        "icacls could not grant {INTERNET_CLIENT_SID} on {}: {}",
        tree.denied.display(),
        String::from_utf8_lossy(&granted.stderr).trim()
    );
    run_confined_with(
        &tree,
        AccessMode::ReadWrite,
        Options {
            capabilities,
            ..Default::default()
        },
    )
    .report
    .contains("denied-read: READ")
}

/// The address this host would use to reach the network, when nothing says
/// otherwise.
///
/// **Discovered rather than configured, so the LAN leg below cannot pass by
/// doing nothing.** A `UdpSocket` connected to the suite's own target performs a
/// route lookup and nothing else — no packet is sent, no name is resolved — and
/// the address it reports is this host's own source address on the interface that
/// route selects. On a host with no route there is no answer, and the leg reports
/// that rather than a pass.
///
/// `TINIO_SANDBOX_TEST_LAN_ADDR` overrides the **interface only**; the port comes
/// from the listener the suite binds, so two runs cannot collide on a fixed one.
fn lan_ip() -> Option<IpAddr> {
    if let Ok(raw) = env::var("TINIO_SANDBOX_TEST_LAN_ADDR") {
        if let Ok(address) = raw.parse::<SocketAddr>() {
            return Some(address.ip());
        }
        if let Ok(ip) = raw.parse::<IpAddr>() {
            return Some(ip);
        }
    }
    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0))).ok()?;
    socket.connect(target()).ok()?;
    let local = socket.local_addr().ok()?;
    let ip = local.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}

/// The LAN question decision 16 leaves open, recorded and never asserted.
///
/// **What is measured is narrower than "the LAN", and saying so is the point.**
/// The connection below is made by this same host to this host's own interface
/// address; it travels the local stack rather than a cable. It establishes
/// whether the container's policy gates a connection addressed to a non-loopback
/// interface **at all** — and it established on 2026-09-22 that it does not: a
/// container with no capability at all answered on `192.168.1.10`. What it
/// cannot establish is whether a client on another machine gets through, because
/// that is a different packet arriving on the same interface from off the host,
/// and a second machine is what would settle it. Nothing in the design depends on
/// the answer (decision 16, §7's Windows note), which is why this leg records
/// rather than asserts.
///
/// **It is run for real rather than behind an unset variable.** A leg that
/// returned early when nobody configured it would report nothing at all, which is
/// the shape this suite exists to avoid; the interface is discovered when nothing
/// overrides it, and a host with no route fails with that reason named.
///
/// The two arms differ only in `privateNetworkClientServer`, which §7 says a
/// non-loopback bind needs: if the answer is ever "refused without it, answered
/// with it", that sentence is confirmed rather than asserted.
#[test]
fn the_lan_answer_is_recorded() {
    let Some(ip) = lan_ip() else {
        let reason = "this host has no non-loopback route to the suite's target, so there is no \
                      LAN interface to measure against";
        testing::host_cannot(
            "the LAN leg",
            reason,
            ", or set TINIO_SANDBOX_TEST_LAN_ADDR=<ip> to name the interface",
        );
        return;
    };
    let volume = one_volume();
    for (label, capabilities) in [
        ("no capability", Vec::new()),
        (
            "privateNetworkClientServer",
            vec![Capability::PrivateNetworkClientServer],
        ),
    ] {
        let (_dir, tree) = tree(&volume);
        // The port is the wildcard bind's, so the address that reaches it is this
        // host's own interface on the same port.
        let bind = SocketAddr::from(([0, 0, 0, 0], 0));
        let (confinement, bound, _slot) = launch_confined(
            &tree,
            AccessMode::ReadWrite,
            Options {
                capabilities,
                ..Default::default()
            },
            Some(bind),
        );
        let Confinement::Supervise(child) = confinement else {
            panic!("the Windows backend must launch a child");
        };
        let lan = SocketAddr::new(ip, bound.port());
        let reached = match TcpStream::connect_timeout(&lan, Duration::from_secs(5)) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("bound the read");
                let mut pong = [0u8; 4];
                Some(stream.read_exact(&mut pong).is_ok() && &pong == b"pong")
            }
            Err(_) => None,
        };
        eprintln!(
            "recorded: [{label}] the container bound {bound}; a same-host connection to {lan} {}",
            match reached {
                Some(true) => "was answered by the confined child",
                Some(false) => "connected but the confined child said something else",
                None => "was refused, so the container did not answer on that interface",
            }
        );
        // **Every spawn is waited for or killed on every path** (Task 11's rule).
        // A connection that was answered was also the child's `accept`, so it is
        // on its way out and can be waited for; one that was not leaves it blocked
        // in `accept` for a caller that is never coming, and dropping the
        // supervision is what ends it — the `Child`'s closure drops with it,
        // closing the job handle and taking the child with it (decision 20), and
        // the `Grants` guard leaves the ACE standing on the same path (design
        // decision 15 — the leg that asserted both halves, `tests/confine.rs`'s
        // `a_dropped_supervision_leaves_the_grant_standing_and_takes_the_child`,
        // was deleted by the 2026-10-01 cleanup: a standing grant after a failed
        // launch is now `tests/confine.rs`'s
        // `a_failed_launch_leaves_the_standing_grants_and_drops_the_profile`, and
        // the child's fate on a launcher's death is `confinement.feature`'s "The
        // launcher's death takes the confined child"). `_slot` is the harness's
        // process-wide launch slot, held for as long as this iteration's child
        // lives.
        if reached == Some(true) {
            let mut child = child;
            supervise(&mut child, || ()).expect("supervise the confined child");
        } else {
            drop(child);
        }
    }
}

/// **The launch's half of the window, asserted for a delete aimed at the ACL
/// phase** — the interval the window exists for, and the one the deferral leg
/// (now inline in `tinio_sandbox_windows::sid`) cannot see because that test
/// holds the lock by hand.
///
/// The property is the ordering such a delete is forced into: the launcher's
/// `CreateProcessW` for the child is written **before** the peer's
/// `DeleteAppContainerProfile`, because the delete waited for the window.
///
/// **What this covers, and what it does not, in this file's own words.**
/// Covered: the window never taken (`window: None`) or acquired after the ACL
/// phase — the delete aimed at the phase lands between the launcher's derive and
/// its `CreateProcessW`, the launch fails `err=2 ERROR_FILE_NOT_FOUND` for a
/// program that exists (the measurement this whole finding is about), and the
/// harness's `expect("the Windows backend must confine the sandbox it was
/// given")` panics on the launch before the ordering is ever read. **Not
/// covered: the interval after the ACL phase ends and before `CreateProcessW`
/// is called** — a window given up *there* leaves this test green, because
/// nothing remains in the launcher's path but a handle list and an attribute
/// buffer, which is faster than the peer's own delete round trip (measured on
/// this host 2026-09-27: releasing the window at the top of
/// `spawn_supervised_child` leaves both launch-side tests green,
/// load-dependently). Closing that interval needs the lock's state observable
/// from outside — a non-blocking probe, i.e. new public API for a test — or an
/// event at the release, which is log noise on every launch; so it is stated
/// here rather than implied by this test's name.
///
/// **Both reads are scoped to this run**, which the move into this binary made
/// necessary and which is stricter than the unscoped original: the phase wait is
/// the marker carrying *this* root's SID (a neighbouring leg's launch writes the
/// same marker), and the launch is the first `CreateProcessW` *after* that
/// marker. The harness's process-wide launch slot is what makes the second read
/// sound — while this launcher is inside its window no other launch can be — and
/// the first is what keeps a foreign window from releasing the peer early.
///
/// The served root carries [`SEEDED_FILES`] small files so the ACL phase — the
/// window's width, and the only thing standing between the peer's signal and
/// `CreateProcessW` — is wide enough for a delete that is *not* deferred to land
/// inside it.
#[test]
fn a_confined_launch_holds_the_window_across_its_acl_phase() {
    /// Enough objects under the served root that the grant takes tens of
    /// milliseconds after the phase announces itself.
    const SEEDED_FILES: usize = 500;

    let _recorder = recorder();
    let volume = one_volume();
    let (_dir, tree) = tree(&volume);
    let name = package_name(&tree.root);
    for index in 0..SEEDED_FILES {
        fs::write(tree.root.join(format!("seed-{index}.txt")), b"seed").expect("seed the root");
    }

    let capture = capture();
    let _recording = capture.record_every_thread();
    let (derived_tx, derived_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let peer_name = name.clone();
    let peer = thread::spawn(move || {
        // A second launcher's value for the same root, held until this root's
        // launch is inside its window and then dropped: that drop is the exit
        // whose delete the window exists to keep out of the launch.
        //
        // **Derived before the launcher starts, and that ordering is the
        // test's, not the lock's.** `PackageSid::derive` is a bare
        // `CreateAppContainerProfile`: two *concurrent* creations of one name
        // race inside userenv, and a run of this test caught it —
        // `CreateAppContainerProfile: hr=0x80070003` (`ERROR_PATH_NOT_FOUND`,
        // the profile's `Packages` directory being made and unmade at once).
        // Production cannot hit it, because every launcher's derive is inside
        // the window and a peer's delete is too; this test's peer derives
        // outside the window on purpose, so it waits for the value to exist
        // before the launcher that shares the name takes it over.
        let sid = PackageSid::derive(&peer_name).expect("the peer's value");
        // **The rendered SID travels back, and it is the phase wait's scope**:
        // the profile is a function of this root's name, so the SID every
        // `grant the ACLs` line of *this* tree's launch carries is this one, and
        // a neighbouring leg's launch carries another. The value itself stays
        // held here — sending the text does not drop the profile.
        derived_tx
            .send(sid.as_str().to_owned())
            .expect("the peer's value exists");
        go_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the launcher's window opens");
        drop(sid);
        dropped_tx.send(()).expect("the peer reports its exit");
    });
    let sid_text = derived_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the peer derived its value");

    // This run's own window, and not a neighbour's: see the test's doc. **The
    // quotes belong to the rendering, not to this marker**: `grant_whitelist`
    // records the SID from a `&str`, so the subscriber writes
    // `sid="S-1-15-2-…"` — the measured rule `windows_e2e_harness::field`
    // trims for (2026-09-24), and a marker without them matches no line at all.
    let phase = format!("{PHASE_MARKER} sid=\"{sid_text}\"");
    let run = thread::scope(|scope| {
        let launcher = scope.spawn(|| run_confined(&tree, AccessMode::ReadWrite));
        // The event the phase announces itself with is emitted *inside* the
        // window — after the derive, before the first grant — so waiting for
        // it is what turns the launcher's window into a signal rather than a
        // guess.
        wait_for(&capture, &phase, Duration::from_secs(60));
        go_tx.send(()).expect("the peer may drop");
        launcher.join().expect("the launcher thread")
    });

    assert_eq!(
        run.status, 0,
        "the confined child did not finish: {}",
        run.report
    );
    dropped_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the peer's delete ran");
    peer.join().expect("the peer thread");

    let logged = capture.text();
    // **Two deletes for this name and not one, which is what makes the ordering
    // below about the peer's rather than the launcher's own.** `Grants` keeps the
    // `Arc<PackageSid>` alive until `confine` returns — after the child — so this
    // root's launcher deletes the same name once the run is over, and a search
    // for *a* delete event would be satisfied by that one even if the peer's had
    // never happened. The first of the two is the peer's (it drops right after
    // the window opens; the launcher's comes after the child exits), and both
    // must be present for the ordering to mean anything.
    let deletes: Vec<usize> = logged
        .match_indices(&format!("step=\"DeleteAppContainerProfile\" name={name}"))
        .map(|(at, _)| at)
        .collect();
    assert_eq!(
        deletes.len(),
        2,
        "this root's two deletes — the peer's and the launcher's own, after the \
         child — are both in the buffer, and a test with only one of them would \
         assert an ordering the missing delete never entered: {deletes:?}\n{logged}"
    );
    // **The launch's own `CreateProcessW` and not the buffer's first one**: the
    // recording is process-wide and this binary holds several launching legs, so
    // an earlier line could belong to a neighbour. This run's launch is the first
    // one after *this run's* phase, which the harness's launch slot keeps
    // unambiguous — no other launch can be in flight while this one holds it.
    let opened = logged
        .find(&phase)
        .expect("this run's grant phase is in the buffer it was waited on");
    let launched = opened
        + logged[opened..]
            .find("step=\"CreateProcessW\"")
            .expect("the launch was logged");
    assert!(
        launched < deletes[0],
        "the peer's delete must wait for this root's launch, so the launcher's \
         CreateProcessW precedes it: launched at {launched}, first delete at \
         {}\n{logged}",
        deletes[0]
    );
}

/// **The window opens before the launch touches the tree**, which is the half a
/// revert inside `prepare_launch` would lose: while a peer holds this root's
/// lock, a `confine` may not have written a single ACE.
///
/// The observation is the served root's DACL, and that is what makes this
/// deterministic where a timing test would not be: the ACL phase is the first
/// thing `confine` does to the operator's tree after the derive, so "the ACE is
/// still absent after the launcher has started" is exactly "the launcher is
/// parked at the window" — a revert that takes the lock later, or not at all,
/// writes that ACE within milliseconds of starting and fails the assertion. The
/// window is then released and the run is asserted to finish, so a launcher that
/// never acquires is a failure here rather than a hang.
#[test]
fn a_confine_waits_for_the_window_before_it_touches_the_tree() {
    let volume = one_volume();
    let (_dir, tree) = tree(&volume);
    let sid = sid_for(&tree.root);
    let window = ProfileLock::acquire(&package_name(&tree.root)).expect("the per-root lock");
    let (started_tx, started_rx) = mpsc::channel();
    let run = thread::scope(|scope| {
        let launcher = scope.spawn(|| {
            started_tx.send(()).expect("the launcher starts");
            run_confined(&tree, AccessMode::ReadWrite)
        });
        started_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the launcher started");
        // Long enough for a launcher that is *not* parked at the window to have
        // reached the ACL phase — milliseconds away once nothing is in its way.
        thread::sleep(Duration::from_millis(500));
        assert!(
            !acl::has_ace(&tree.root, &sid).expect("read the DACL"),
            "the launcher granted the served root while a peer held this root's \
             window: the lock is taken before the ACL phase, or it is not taken \
             at all — and the window's whole purpose is that phase"
        );
        drop(window);
        launcher.join().expect("the launcher thread")
    });
    assert_eq!(
        run.status, 0,
        "the confined child did not finish: {}",
        run.report
    );
}

/// The phase's own `info!`, which `grant_whitelist` emits once per launch
/// *before* the first grant: the mark of the window being open, since the derive
/// has happened by then and `CreateProcessW` has not.
///
/// **Matched together with this root's own `sid=`, never alone**: the recording
/// is process-wide and this binary holds several launching legs, so the bare step
/// is also what a neighbouring launch writes — see
/// [`a_confined_launch_holds_the_window_across_its_acl_phase`].
const PHASE_MARKER: &str = "step=\"grant the ACLs\"";

/// Wait until the recording carries `marker`, so that an event emitted inside a
/// window becomes something a test can act on.
///
/// Polled rather than channelled because the writer is part of the subscriber and
/// has one job — copy what it is given — and the buffer is already the ordered
/// record the assertions read afterwards.
fn wait_for(capture: &Capture, marker: &str, bound: Duration) {
    let deadline = Instant::now() + bound;
    while !capture.peek().contains(marker) {
        assert!(
            Instant::now() < deadline,
            "the recording never carried {marker:?}:\n{}",
            capture.peek()
        );
        thread::sleep(Duration::from_millis(1));
    }
}

// **The host gate's order has no test here any more, and the reason is that it stopped being a
// text property.** `prepare_launch` takes a `capability::Admission` that only `capability::admit`
// can construct, so a run whose gate moved below the crate's first side effect would not compile
// — a stronger check than this file's source-reading assertion was, and one that cannot drift
// from the code it is about.
