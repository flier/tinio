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

use std::{
    fs,
    os::windows::fs::symlink_file,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tempfile::tempdir;
use tinio_sandbox_core::{AccessMode, Error, Exposure, Sandbox};
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
// `recorder` used to be a copy here and a second, wider one in `tests/profile.rs`,
// and that module's own doc states what the union folded together.
use crate::{
    acl::Target,
    testing::{self, capture, recorder},
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
// is deliberately neither `Send` nor `Sync` — the same allow, for the same
// reason, as `tests/acl.rs`'s: this is shared ownership inside one thread, which
// is what the code under test is given.
#[allow(clippy::arc_with_non_send_sync)]
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
}

#[test]
fn the_callers_access_survives_the_executable_exposure() {
    // The program directory's entry is read-only and must never widen a path
    // the caller exposed read-write — the merge rule, applied to the one entry
    // this rule adds.
    let sandbox = Sandbox::new().expose(r"E:\build", AccessMode::ReadWrite);
    let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].access, AccessMode::ReadWrite);
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

    let sandbox = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    let scan = SiblingScan::scan(&program_dir);
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
    let granted = canonical_exposures(exposures(&sandbox, &program_dir.join("serve.exe")))
        .expect("the launcher's own file entries resolve");
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
/// `TINIO_SANDBOX_CAPABILITIES` or in the configuration file — and is turned
/// into the SID the container is created with by [`Capability::sid`]. Nothing
/// produces a SID for this crate to parse back, which is why there is no
/// `from_sid` to round-trip through.
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
