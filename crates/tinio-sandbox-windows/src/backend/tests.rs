//! The backend's unit tests, kept at the one path that can ask the policy's
//! private questions: `shape_for`'s precedence rule and `CallerSet`'s membership
//! answer are `pub(super)` in `exposures.rs`, and this module — a child of
//! `backend` — is the reader that visibility was widened for.
//!
//! **They live here rather than beside `exposures.rs`**, which is the move that
//! would have let every item stay private: a `mod tests` inside that file would
//! also have renamed every test from `backend::tests::…` to
//! `backend::exposures::tests::…`, and a rename is not a move. What the split
//! had to widen, and why it stopped at `pub(super)`, is stated once in `mod.rs`'s
//! module doc.

use std::{
    fs,
    io::{self, Write},
    mem,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    thread::{self, ThreadId},
    time::Duration,
};

use tempfile::tempdir;
use tinio_sandbox_core::{AccessMode, Exposure, Sandbox};
use tracing::{Level, callsite::rebuild_interest_cache};
use tracing_subscriber::fmt::MakeWriter;

// `CallerSet` is the one policy item `super::*` does not carry — the parent
// imports `shape_for` and `canonical_caller` by name because `confine` calls
// them, and the type is named here for the same reason these tests ask it the
// question: it is `pub(super)` for this module alone (`exposures.rs`'s module
// doc).
use super::{
    exposures::CallerSet,
    grant::{GRANT_PHASE_WARN_AFTER, report_grant_phase},
    *,
};
use crate::{acl::Target, testing};

/// A junction at `dir/name` pointing at `target`, or a failed test.
///
/// The link is [`testing::junction`]'s — this module builds the path and names
/// it, and the fixture's own self-assertion comes with it.
fn junction(dir: &Path, name: &str, target: &Path) -> PathBuf {
    let link = dir.join(name);
    testing::junction(&link, target);
    link
}

/// The capture this binary records through, installed once.
///
/// **One subscriber for the whole process, as the global default, installed
/// with `try_init` and never `init` (shared §7).** That is the fix for the
/// flake this harness caused, and not a preference, because of a `tracing`
/// detail worth writing down: `tracing` caches each callsite's `Interest` in a
/// **process-global** cell, written both when a callsite is hit for the first
/// time and when any dispatch is created — and while exactly one dispatch is
/// registered, which is the whole of a `with_default` recording,
/// `tracing-core`'s `Rebuilder::JustOne` consults *the calling thread's*
/// default rather than the registry. A subscriber scoped to one thread is
/// therefore invisible to every other thread, so a parallel test that happens
/// to be the first to hit the same callsite writes `Interest::never` into the
/// shared cell and the event this test exists to observe is dropped in
/// silence: one run in thirty, and only when the other thread wins that race.
/// A **global** default is visible from every thread, so the cell can only
/// ever say "interested".
///
/// The capture is still this test's own: the writer records only while a
/// recording owns the calling thread, which keeps the event count exact while
/// other tests emit the same failures concurrently.
fn capture() -> Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let capture = Capture::default();
            // A subscriber the caller already installed wins, and this
            // crate's own test binary installs none; if one ever does, the
            // capture stays empty and the test below fails loudly rather
            // than passing on a subscriber that recorded nothing.
            let _ = tracing_subscriber::fmt()
                .with_writer(capture.clone())
                .with_ansi(false)
                .with_max_level(Level::INFO)
                .try_init();
            // **`set_global_default` stores the dispatch without
            // re-evaluating the callsite cache** — only `Dispatch::new` does
            // that, and it runs before the default is visible — so a callsite
            // another test already hit stays cached `never`. Rebuilding here,
            // with the global default in place, is what turns those cells
            // back to "interested".
            rebuild_interest_cache();
            capture
        })
        .clone()
}

/// The recorder: what was written, and the thread a recording owns, under one
/// lock so that "clear, record, take" is a single state.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    text: Vec<u8>,
    recording: Option<ThreadId>,
}

impl Capture {
    /// Own this thread's events until the guard drops.
    fn record(&self) -> Recording {
        let mut state = self.0.lock().expect("the capture mutex");
        assert!(
            state.recording.is_none(),
            "one recording at a time, and {:?} has one",
            state.recording
        );
        state.text.clear();
        state.recording = Some(thread::current().id());
        Recording(self.clone())
    }

    /// Give up this thread's ownership of the capture.
    fn release(&self) {
        self.0.lock().expect("the capture mutex").recording = None;
    }

    /// What the recording wrote, as UTF-8.
    fn text(&self) -> String {
        let text = mem::take(&mut self.0.lock().expect("the capture mutex").text);
        String::from_utf8(text).expect("the capture is UTF-8")
    }
}

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self.0.lock().expect("the capture mutex");
        // Another test's event is dispatched here too — the subscriber is
        // global — and the recording thread is what keeps it out.
        if state.recording == Some(thread::current().id()) {
            state.text.extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// One recording's lifetime: [`Capture::record`]'s guard.
///
/// A guard rather than a matching call at the end, so that a body which
/// panics releases the capture instead of leaving a thread id behind that a
/// later test's events could be attributed to.
struct Recording(Capture);

impl Drop for Recording {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// **One recording at a time across this binary's tests.**
///
/// The capture is process-wide and holds one buffer, so two tests that assert on
/// it must not overlap: `Capture::record`'s "one recording at a time" is an
/// *assertion*, and an assertion is the wrong shape for a rule two tests have to
/// keep — the loser fails for the other test's timing rather than for its own
/// subject, and the second recording clears the first's buffer with its own
/// events. This is the same device `tests/profile.rs` uses for the same reason.
/// It is taken once per test rather than once per `recorded` call, so a nested
/// `recorded` inside one test is still what the assertion catches.
fn recorder() -> MutexGuard<'static, ()> {
    static RECORDER: Mutex<()> = Mutex::new(());
    RECORDER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
/// The assertions are on the **rendered fields**, not on the values as
/// substrings, for the reason the core's test states at length: `step` appears
/// inside the rendered error too, so a substring check would pass for an event
/// whose field said something else. `with_ansi(false)` is what makes the field
/// syntax visible at all (measured in that same test). The level is the one
/// `serve`'s subscriber enables, so an event moved to `debug!` would stop being
/// recorded and this test would lose the property rather than keep it.
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
    assert!(
        logged.contains(r#"step="resolve an exposed path""#),
        "{logged}"
    );
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
        canonical_caller(&Sandbox::new().expose(&exposed_file, AccessMode::ReadOnly))
            .expect_err("a caller's file is refused")
    });
    assert!(error.to_string().contains("is not a directory"), "{error}");
    assert!(
        logged.contains(r#"step="resolve an exposed path""#),
        "{logged}"
    );
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
    assert!(logged.contains(r#"step="confine""#), "{logged}");
    assert_eq!(logged.matches("sandbox step failed").count(), 1, "{logged}");
}

/// **The grant phase's two halves, pinned without a launch**: the measurement is
/// `debug!` and the warning past the threshold is `warn!` — and the threshold is
/// asserted *as a boundary* rather than as a comparison, one millisecond on
/// either side of it.
///
/// **The quiet half is the assertion that matters to an operator, and the
/// capture is why it can be made at all.** This subscriber's max level is
/// `INFO` — the level `serve` builds its own with — so a phase that stayed under
/// the threshold emits **nothing**, and the `debug!` beside the warning is
/// invisible to it on purpose. An event moved from `warn!` to `debug!`, or a
/// threshold moved above the measured case, would leave this test's first half
/// green and its second red, which is the pair of mistakes one warning can make.
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
    assert!(
        quiet.is_empty(),
        "a grant phase under the threshold is silent at INFO: {quiet}"
    );

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
    assert!(
        logged.is_empty(),
        "a per-ACE grant belongs at the level a log shows the phase at, not \
         above it: {logged}"
    );
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

/// **The shape the launcher gives each exposed directory, and the precedence
/// that decides it** — the half of the Ruling 78 regression that no DACL check
/// can see, because a grant written in the wrong shape still reports success.
#[test]
fn the_program_directory_is_the_one_directory_that_does_not_propagate() {
    // **A real tree, because the caller's set is the type's now** (P1): the
    // only way to build one is [`canonical_caller`], which resolves and checks
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
    // **The caller's set is [`canonical_caller`]'s answer and nothing else**
    // (P1): [`shape_for`] takes the type, so the raw set does not compile here.
    let caller_set = canonical_caller(&sandbox).expect("the caller's root resolves");

    // The caller's own exposure keeps the inherited form, which is what makes
    // a file already inside it readable by the child without a `/t` walk.
    let caller = all
        .iter()
        .find(|exposure| exposure.host == canonical_root)
        .expect("the caller's root is exposed");
    assert_eq!(
        shape_for(&caller_set, caller, Some(&canonical_program_dir)),
        Target::Directory
    );

    // The entry decision 22 adds does not.
    let own = all
        .iter()
        .find(|exposure| exposure.host == canonical_program_dir)
        .expect("the program's directory is exposed");
    assert_eq!(
        shape_for(&caller_set, own, Some(&canonical_program_dir)),
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
    assert_eq!(
        shape_for(
            &canonical_caller(&caller_owns_it).expect("the caller's directory resolves"),
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
/// to the granted set, which `confine` resolves through
/// [`canonical_exposures`] and never hands to [`canonical_caller`]. The refusal
/// exists because a caller's file would otherwise be granted nothing and the
/// server would run on with a quiet fail-closed (the defect P7 records).
#[test]
fn a_caller_exposed_file_is_refused_rather_than_silently_dropped() {
    let dir = tempdir().expect("temp dir");
    let file = dir.path().join("exposed.txt");
    fs::write(&file, b"x").expect("write the caller's file");
    let canonical = fs::canonicalize(&file).expect("the file resolves");

    // The path that is wrong is the one the error has to name, and the one
    // `icacls` would have been handed if the loop had not dropped it.
    let err = canonical_caller(&Sandbox::new().expose(&file, AccessMode::ReadOnly))
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
    let caller = canonical_caller(&sandbox).expect("the served root is a directory");
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
/// because [`shape_for`] and [`canonical_caller`] are private**: a caller
/// exposes the launcher's own directory through a **raw** path, and the entry
/// must still be the caller's — so it keeps [`Target::Directory`], the
/// inherited form, with the caller's access.
///
/// **It calls [`canonical_caller`] rather than reproducing it**, which is the
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
    // set from `canonical_caller` (`:363`) and the granted set from
    // `canonical_exposures(exposures(…))`.
    let canonical_dir = fs::canonicalize(&program_dir).expect("the program's directory resolves");
    let caller = canonical_caller(&sandbox).expect("the caller's path resolves");
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
        shape_for(&caller, entry, Some(&canonical_dir)),
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
    // `canonical_caller` too, and answers decision 22's variant.
    let served = dir.path().join("srv");
    fs::create_dir_all(&served).expect("create the served root");
    let launchers_own = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    let served_caller = canonical_caller(&launchers_own).expect("the served root resolves");
    let launchers_set =
        canonical_exposures(exposures(&launchers_own, &program)).expect("the entries resolve");
    let own = launchers_set
        .iter()
        .find(|exposure| exposure.host == canonical_dir)
        .expect("the program's directory is in the granted set");
    assert_eq!(
        shape_for(&served_caller, own, Some(&canonical_dir)),
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
    // Measured on this host, 2026-09-19: `icacls` resolves the path it is
    // given, and `(OI)(CI)` is inheritable — so granting through a junction
    // puts the ACE on the junction's *target*, outside the exposure, and that
    // target's own children inherit it. A junction rather than a symlink
    // because `mklink /J` needs no elevation and a symlink does.
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
