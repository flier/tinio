//! The AppContainer profile's shared life: one root is one profile, so two
//! launchers on it have to agree about when it may be deleted.
//!
//! **The window this file is about was measured on this host (2026-09-26).** A
//! second `CreateAppContainerProfile` for one name answers
//! `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` with a null SID out-parameter, so
//! the second launcher takes the first's profile over rather than creating a
//! second one — and the first launcher's exit deletes it, because nothing
//! distinguishes "I created it" from "I took it over". What that costs is
//! exactly one interleaving: a `CreateProcessW` that lands between a peer's
//! delete and its own completion fails `err=2 ERROR_FILE_NOT_FOUND` for a
//! program that exists, which is a misdiagnosis an operator chases as a missing
//! binary. A running child is unharmed (measured), and a later launch heals
//! itself; the window is the whole reason [`ProfileLock`] exists.
//!
//! **Both ends of the window are asserted here, and the launch's half is
//! asserted twice over.** The delete's half: a peer's delete is deferred while
//! the lock is held by hand
//! ([`a_peer_delete_waits_for_the_window_a_launch_holds`]). The launch's half:
//! **the strongest assertion in this file** is
//! [`a_confine_waits_for_the_window_before_it_touches_the_tree`], which holds
//! the lock, runs a real `confine`, and requires that the served root has no
//! package-SID ACE at all while a peer owns the window — i.e. that `confine` is
//! parked at the window *before* its ACL phase, which is the interval the window
//! exists to cover. [`a_confined_launch_holds_the_window_across_its_acl_phase`]
//! is the other side of the same fact: a delete aimed at that phase lands after
//! the launcher's `CreateProcessW`, read out of the order the two events were
//! written in. Neither test covers the interval between the end of the ACL phase
//! and `CreateProcessW`, and that gap is stated in the second test's doc rather
//! than left to be inferred.

#![cfg(target_os = "windows")]

mod common;

use std::{
    fs,
    io::{self, Write},
    mem,
    sync::{Arc, Mutex, MutexGuard, OnceLock, mpsc},
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

use tinio_sandbox_core::AccessMode;
use tinio_sandbox_windows::{
    acl,
    sid::{PackageSid, ProfileLock, package_name},
};
use tracing::{Level, callsite::rebuild_interest_cache};
use tracing_subscriber::fmt::MakeWriter;

/// The confined child. The body lives in the harness because every suite that
/// launches a child runs the same role, and this binary is one of them since
/// [`a_confined_launch_holds_the_window_across_its_acl_phase`].
#[test]
fn child_role() {
    common::child_role();
}

/// **The window's whole purpose**: a peer's delete waits for the launch that is
/// holding the lock, so it can never land between the derive and
/// `CreateProcessW`.
///
/// The shape is a second holder's delete deferred while the first holds the
/// lock, which is deterministic — a raced pair of real launchers would not be,
/// because the window is a race by construction and a test that won it would
/// pass for the wrong reason. The negative assertion below is still a bound, so
/// what makes it mean anything is the **progress signal**: the peer says it is
/// at its drop before the wait starts, because a `derive` is a userenv profile
/// creation whose duration no bound controls and a peer that had not got there
/// yet would satisfy the same `is_err()`.
#[test]
fn a_peer_delete_waits_for_the_window_a_launch_holds() {
    let dir = tempfile::tempdir().expect("temp dir");
    let name = package_name(dir.path());
    // The launch's window, held by hand for as long as this test needs it: in
    // the launcher it is `prepare_launch`'s derive through `CreateProcessW`.
    let window = ProfileLock::acquire(&name).expect("the per-root lock");
    let (derived_tx, derived_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let peer = thread::spawn(move || {
        // A peer process's value for the same root: it takes the profile over,
        // and dropping it is that peer's exit.
        let sid = PackageSid::derive(&name).expect("the takeover");
        derived_tx.send(()).expect("the peer reached its drop");
        drop(sid);
        done_tx.send(()).expect("the peer reports its exit");
    });
    derived_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the peer derived its value and is at its drop");
    assert!(
        done_rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "the peer's delete ran inside the launch window, so it could land \
         between the derive and CreateProcessW and turn a launch of a program \
         that exists into err=2 ERROR_FILE_NOT_FOUND"
    );
    drop(window);
    done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the delete lands once the window closes");
    peer.join().expect("the peer thread");
}

/// **The launch's half of the window, asserted for a delete aimed at the ACL
/// phase** — the interval the window exists for, and the one the deferral test
/// above cannot see because that test holds the lock by hand.
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
    let volume = common::one_volume();
    let (_dir, tree) = common::tree(&volume);
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
        derived_tx.send(()).expect("the peer's value exists");
        go_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the launcher's window opens");
        drop(sid);
        dropped_tx.send(()).expect("the peer reports its exit");
    });
    derived_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the peer derived its value");

    let run = thread::scope(|scope| {
        let launcher = scope.spawn(|| common::run_confined(&tree, AccessMode::ReadWrite));
        // The event the phase announces itself with is emitted *inside* the
        // window — after the derive, before the first `icacls` — so waiting for
        // it is what turns the launcher's window into a signal rather than a
        // guess.
        wait_for(&capture, PHASE_MARKER, Duration::from_secs(60));
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
    let launched = logged
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
    let volume = common::one_volume();
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);
    let window = ProfileLock::acquire(&package_name(&tree.root)).expect("the per-root lock");
    let (started_tx, started_rx) = mpsc::channel();
    let run = thread::scope(|scope| {
        let launcher = scope.spawn(|| {
            started_tx.send(()).expect("the launcher starts");
            common::run_confined(&tree, AccessMode::ReadWrite)
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

/// The phase's own `info!`, which [`grant_whitelist`](tinio_sandbox_windows::backend) emits
/// once per launch *before* the first `icacls`: the mark of the window being
/// open, since the derive has happened by then and `CreateProcessW` has not.
const PHASE_MARKER: &str = "step=\"grant the ACLs\"";

/// **`S_OK` from `DeleteAppContainerProfile` is not "a deletion happened"**, and
/// the value whose profile a peer already deleted is the case that proves it:
/// measured on this host 2026-09-26, the call answers `S_OK` for a name that was
/// never created, for one deleted a moment ago and for a name never used at all
/// — so a `Drop` that logged a deletion on `hr == 0` logged one that did not
/// happen, and an operator reading the stream would believe the profile was taken
/// back when it may never have been there.
///
/// The other half of the same defect is the warning: `hr != 0` is not "someone
/// else deleted it" (there is no such answer — see above), it is the delete
/// having failed, so what is left behind is a profile this run could not remove.
/// That branch has no test: the API refuses a delete for a name it will not
/// resolve, and this crate cannot build one of those without failing its own
/// derivation first.
#[test]
fn a_delete_that_found_no_profile_is_not_reported_as_a_deletion() {
    let _recorder = recorder();
    let dir = tempfile::tempdir().expect("temp dir");
    let name = package_name(dir.path());
    // The takeover, then the creator's exit: `second` is a live `PackageSid`
    // whose profile the delete above has already removed — the exact state the
    // measured `S_OK` answers for.
    let first = PackageSid::derive(&name).expect("create the profile");
    let second = PackageSid::derive(&name).expect("take it over");
    drop(first);

    let capture = capture();
    let logged = {
        let _recording = capture.record();
        drop(second);
        capture.text()
    };
    assert!(
        logged.contains(r#"step="DeleteAppContainerProfile""#),
        "the event the profile's removal is read from: {logged}"
    );
    assert!(
        logged.contains(&format!("name={name}")) && logged.contains("absent=true"),
        "the event must report what the call leaves behind rather than a \
         deletion it cannot claim: {logged}"
    );
    // The delete succeeded here, so the warning the refused-delete branch writes
    // must not be what this recorded: a regression that took `hr != 0` for this
    // case would say the profile may still be on the machine, and `absent=false`
    // is the field that carries it.
    assert!(
        !logged.contains("absent=false") && !logged.contains("WARN"),
        "this delete answered `S_OK`, so the event is the `INFO` one and not \
         the warning a refused delete writes: {logged}"
    );
}

/// The lock is **per served root**, which is the same key the profile is named
/// from: two roots are two profiles and must not wait for one another.
///
/// The consequence of getting this wrong is a lock that serialises unrelated
/// roots — a launch blocked behind another root's ACL phase, which is minutes on
/// a large tree — so the assertion is that a second root's acquisition completes
/// while the first root's lock is held.
#[test]
fn a_different_root_is_a_different_lock() {
    let held_dir = tempfile::tempdir().expect("temp dir");
    let other_dir = tempfile::tempdir().expect("temp dir");
    let held = ProfileLock::acquire(&package_name(held_dir.path())).expect("the per-root lock");
    let other_name = package_name(other_dir.path());
    assert_ne!(other_name, package_name(held_dir.path()));
    let (done_tx, done_rx) = mpsc::channel();
    let peer = thread::spawn(move || {
        let lock = ProfileLock::acquire(&other_name).expect("the other root's lock");
        done_tx.send(()).expect("the peer reports the acquisition");
        drop(lock);
    });
    done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("another root's launch is not behind this root's window");
    peer.join().expect("the peer thread");
    drop(held);
}

/// **The one recording at a time**: two tests in this binary assert on a
/// captured window, and the capture holds one buffer, so each takes it to
/// itself. What the lock cannot cover is the *other* tests' events, which is why
/// the assertions name the package they are about.
fn recorder() -> MutexGuard<'static, ()> {
    static RECORDER: Mutex<()> = Mutex::new(());
    // A test that panicked while recording poisons this, and the suite is
    // already failing: the next reader takes the buffer rather than reporting a
    // second failure that hides the first.
    RECORDER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

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

/// What a recording saw, for the tests that assert on an event's fields or on
/// the order of two events — the shape `src/backend/tests.rs`'s capture
/// established, and here for the same two reasons it states: the subscriber is
/// **global** (a `with_default` dispatch is invisible to `tracing`'s
/// process-wide interest cache, which is the one-run-in-thirty flake that
/// harness was fixed for), and what keeps an assertion exact is the filter — the
/// **thread** for the field assertions, and every thread for the ordering one,
/// because the two events it compares are written by two different threads.
fn capture() -> Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let capture = Capture::default();
            let _ = tracing_subscriber::fmt()
                .with_writer(capture.clone())
                .with_ansi(false)
                .with_max_level(Level::INFO)
                .try_init();
            rebuild_interest_cache();
            capture
        })
        .clone()
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    text: Vec<u8>,
    recording: Option<ThreadId>,
    every_thread: bool,
}

impl Capture {
    /// Own this thread's events until the guard drops.
    fn record(&self) -> Recording {
        let mut state = self.0.lock().expect("the capture mutex");
        assert!(!state.every_thread, "another recording owns the capture");
        state.text.clear();
        state.recording = Some(thread::current().id());
        Recording(self.clone())
    }

    /// Own **every** thread's events until the guard drops: the mode the
    /// ordering assertion needs, and the reason the callers take [`recorder`]
    /// first.
    fn record_every_thread(&self) -> Recording {
        let mut state = self.0.lock().expect("the capture mutex");
        assert!(
            state.recording.is_none() && !state.every_thread,
            "another recording owns the capture"
        );
        state.text.clear();
        state.every_thread = true;
        Recording(self.clone())
    }

    fn release(&self) {
        let mut state = self.0.lock().expect("the capture mutex");
        state.recording = None;
        state.every_thread = false;
    }

    /// What has been recorded so far, without taking it: the reader
    /// [`wait_for`] polls with.
    fn peek(&self) -> String {
        let state = self.0.lock().expect("the capture mutex");
        String::from_utf8_lossy(&state.text).into_owned()
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
        if state.every_thread || state.recording == Some(thread::current().id()) {
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
struct Recording(Capture);

impl Drop for Recording {
    fn drop(&mut self) {
        self.0.release();
    }
}
