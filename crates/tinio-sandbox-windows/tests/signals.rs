//! The signal relay, measured: a stop delivered to the launcher's console
//! process group reaches the confined child only because the launcher forwards
//! it (design §3 step 9, decision 20's second half).
//!
//! **What the evidence is, and what it is not.** The child arms the hook a
//! forwarded stop lands on ([`launch::on_stop`]) and *that hook* is what this
//! suite asserts on: it writes a file, and the child then ends with the status
//! that arrival announces — a stopped server's shape, and the status the
//! launcher forwards to its own caller. A heartbeat exists, but only as
//! scaffolding for the negative case: a child whose hook never fired can only be
//! said to have been *missed* if it was alive and writing for the whole window,
//! and no assertion here reads a heartbeat as evidence of a stop. Nor is
//! `GenerateConsoleCtrlEvent`'s return value evidence of anything — it returns 1
//! with `GetLastError` 0 for a call that delivers nothing at all (the design's
//! evidence table has the rows).
//!
//! **Three processes, three addresses.** The launcher runs this binary's
//! `launcher_role` in its own process group, which is where the stop goes; the
//! confined child is a grandchild in a group of its own, which is the gap the
//! relay exists to close; and the premise test's children are spawned directly
//! in groups of their own, so a stop can be addressed at one of them and the
//! other watched for a delivery it must not receive.

#![cfg(target_os = "windows")]

// **The harness is the library's now** (`tinio_sandbox_windows::testing`), and
// this binary keeps its own top-level `child_role` and `launcher_role` shims —
// the names the harness spawns here, because the lib linked into an integration
// target is compiled without `cfg(test)`
// (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`).
use std::{
    fs,
    path::Path,
    process::{Child, Stdio},
    thread,
    time::{Duration, Instant},
};

use tinio_sandbox_windows::{control, testing as common};
use tinio_util::testing::{kill_and_reap, wait_for_path};

/// How often this suite looks at a file: the resolution of every window below.
const POLL: Duration = Duration::from_millis(50);

/// What a stop is given to arrive — and what a child that must stay silent is
/// given to stay silent.
///
/// Two orders of magnitude wider than [`POLL`] and fifty times the child's own
/// beat (200 ms), because every assertion here is about the child's hook and
/// never about timing: a window that could be confused with a poll period would
/// fail a test for being early rather than for being wrong.
const WINDOW: Duration = Duration::from_secs(10);

/// The body is [`common::launcher_role`]'s, which documents why the role is a
/// process of its own and why it is the harness's and not this suite's; `false`
/// is this suite's difference from the events one, which needs the bound address
/// published because it reaches the child through the listener.
#[test]
fn launcher_role() {
    common::launcher_role(false);
}

/// The child role: the stop hook, and a beat while it waits.
///
/// Two ways in, one body — the dispatch is [`common::child_role_entry`]'s, shared
/// with the library's own entry point so the two cannot drift. The confined child
/// reads the probe file its launcher wrote (the payload, design decision 17); the
/// unrelayed control case — a process this suite spawns directly, with nothing
/// relaying to it — is handed the same file's path in the environment. A process
/// with neither is the launcher, and plays no role at all. The half installed
/// here is the stop-hook variant, which is this suite's subject.
#[test]
fn child_role() {
    common::child_role_entry(common::child_role_with_stop_hook);
}

/// A process this suite started, and owns.
///
/// Every path out of a test above reaps it: the clean path waits for it, and the
/// failing path kills and waits in `Drop`, which runs during unwinding too. No
/// path leaves a confined child or a beating test binary behind.
struct Owned(Option<Child>);

impl Owned {
    /// Take ownership of a process this suite spawned.
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    /// The process's id, which is also its process-group id because it was
    /// started with `CREATE_NEW_PROCESS_GROUP` — the address a stop goes to.
    fn group(&self) -> u32 {
        self.child().id()
    }

    fn child(&self) -> &Child {
        self.0.as_ref().expect("the process is still owned")
    }

    /// The child, for the harness's own wait.
    ///
    /// **Waiting goes through [`common::wait_for_exit`] rather than a method
    /// here**, because that is the one function the harness rule — every spawned
    /// process is waited for or killed on every path, red ones included — is
    /// spelled in, and a suite that wrote its own copy could assert on the
    /// deadline without killing the child first. What stays here is the
    /// ownership `Drop` enforces.
    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("the process is still owned")
    }

    /// Whether it is still running.
    fn running(&mut self) -> bool {
        self.0
            .as_mut()
            .expect("the process is still owned")
            .try_wait()
            .expect("poll the process")
            .is_none()
    }

    /// Kill it and reap it.
    fn stop(&mut self) {
        if let Some(mut child) = self.0.take() {
            kill_and_reap(&mut child);
        }
    }
}

impl Drop for Owned {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Watch `stopped` for `within` while the child beats, failing the moment it
/// appears, and hand back how much the child wrote in the meantime.
///
/// The window is what makes the negative mean something: a child that was dead
/// for it cannot be said to have been missed.
fn missed_while_beating(beat: &Path, stopped: &Path, within: Duration) -> u64 {
    let before = length(beat);
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        assert!(
            !stopped.exists(),
            "the hook of a child in another process group fired: a stop addressed at one group \
             reached a process in another, so the premise design §3 step 9 rests on does not hold \
             here and the relay forwards nothing this platform would not have delivered anyway"
        );
        thread::sleep(POLL);
    }
    length(beat) - before
}

/// A file's length, or zero when it is not there yet.
fn length(path: &Path) -> u64 {
    fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

/// The relay, end to end: a stop addressed at the launcher's process group
/// reaches the confined child, which is in a group of its own.
///
/// **A host that cannot deliver a console control event fails this test rather
/// than skipping it.** There is nothing to fall back to — the relay is the
/// mechanism this platform has, and a run that quietly asserts nothing is the
/// shape of green this phase refuses.
#[test]
fn a_stop_addressed_at_the_launcher_reaches_the_confined_child() {
    let (_dir, tree) = common::tree(&common::one_volume());
    let armed = tree.report.join(common::ARMED);
    let stopped = tree.report.join(common::STOPPED);
    let mut launcher = Owned::new(common::spawn_launcher(&tree.path, Stdio::null(), false));

    // The child's hook is installed: from here a forwarded stop has somewhere to
    // land instead of taking the default disposition.
    assert!(
        wait_for_path(&armed, common::TIMEOUT, POLL),
        "the confined child never armed its stop hook: nothing would have caught a forwarded stop, \
         and this run proves nothing about the relay"
    );
    // The stop a container sends: addressed at the launcher's own process group,
    // which the child is not in.
    control::send_stop(launcher.group()).expect("a console to deliver a stop on");

    // The evidence: the child's own hook, which nothing but the launcher's
    // forward can have reached. `send_stop` returning `Ok` above says only that
    // the call was accepted.
    assert!(
        wait_for_path(&stopped, WINDOW, POLL),
        "the confined child's stop hook never fired within {WINDOW:?}: the launcher did not forward \
         the stop to the child's process group (design §3 step 9)"
    );
    // The hook ends the child, so the launcher's wait returns and it exits with
    // the status it was handed (design §3 step 8). The grants it wrote are
    // standing, so nothing happens on the way out but the exit.
    let status = common::wait_for_exit(launcher.child_mut(), common::TIMEOUT, "the launcher");
    assert_eq!(
        status.code(),
        Some(common::STOPPED_EXIT),
        "the launcher did not report the child's own status: the stop reached the child, and the \
         exit-status forwarding did not carry the answer back"
    );
}

/// The premise the relay rests on: a console control event addressed at one
/// process group is delivered to that group, and to no other.
///
/// Without this, "the confined child's hook fired" would be consistent with the
/// child having been reached directly — a console-wide event would need no relay
/// at all, and `CREATE_NEW_PROCESS_GROUP` (the flag the launch itself passes,
/// §3 step 9) is exactly the mechanism that makes one addressed stop stop short
/// of a child in another group. Both children here have the same hook the
/// confined child arms, so the addressed one doubles as the hook's own positive
/// case in an unconfined process.
#[test]
fn the_stop_reaches_the_group_it_addresses_and_reaches_no_other() {
    let (_addressed_dir, addressed_tree) = common::tree(&common::one_volume());
    let (_other_dir, other_tree) = common::tree(&common::one_volume());
    let mut addressed = Owned::new(common::spawn_unrelayed_child_in_own_group(
        &addressed_tree.probe(),
    ));
    let mut other = Owned::new(common::spawn_unrelayed_child_in_own_group(
        &other_tree.probe(),
    ));

    let addressed_stopped = addressed_tree.report.join(common::STOPPED);
    let other_stopped = other_tree.report.join(common::STOPPED);
    for (tree, what) in [
        (&addressed_tree, "the addressed"),
        (&other_tree, "the other"),
    ] {
        assert!(
            wait_for_path(&tree.report.join(common::ARMED), common::TIMEOUT, POLL),
            "the {what} child never armed its stop hook"
        );
    }

    control::send_stop(addressed.group()).expect("a console to deliver a stop on");

    assert!(
        wait_for_path(&addressed_stopped, WINDOW, POLL),
        "the hook of the process group the stop was addressed to never fired: the event did not \
         reach the group it named"
    );
    assert_eq!(
        common::wait_for_exit(
            addressed.child_mut(),
            common::TIMEOUT,
            "the addressed child"
        )
        .code(),
        Some(common::STOPPED_EXIT),
        "the addressed child's hook fired, and the process did not end the way the hook ends it"
    );

    // The other group's child is untouched: it beats for the whole window and its
    // hook never fires. The beat is not the evidence — the hook's silence is —
    // and it is what makes that silence a measurement rather than an absence.
    let written = missed_while_beating(
        &other_tree.report.join(common::BEAT),
        &other_stopped,
        WINDOW,
    );
    assert!(
        written > 0,
        "the other group's child wrote nothing for {WINDOW:?}: it was not alive to be missed, so \
         its silent hook proves nothing"
    );
    assert!(
        other.running(),
        "the other group's child is gone: it was reached after all, or it died for a reason this \
         test has not accounted for"
    );
    // Spawned by this suite, reaped by it (the beat never ends on its own).
    other.stop();
}
