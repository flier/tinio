//! The relay, measured: a signal sent to the launcher reaches the child, and
//! the launcher survives it (design §3 step 9, decision 20).
//!
//! This is deliberately *not* part of the confined-server test. A launcher with
//! no relay passes every exit-status assertion there; only a test that signals
//! the launcher itself can tell the relay apart from the forwarding.

#![cfg(target_os = "macos")]

use std::{
    os::unix::process::ExitStatusExt as _,
    process::{Command, Stdio},
    sync::Mutex,
    thread::sleep,
    time::Duration,
};

use rustix::process::{Signal, getpid, kill_process};
use tinio_sandbox_macos::relay::{disarm, on_stop};
use tinio_util::testing::poll_exit;

/// The two tests below share this module's process-wide state — the armed pid
/// and the installed handlers — and **libtest runs tests in parallel by
/// default**, so without this lock they race: the disarmed test's `disarm` can
/// land between the first test's `on_stop` and its signal, and the first test
/// then fails for a reason that has nothing to do with the relay.
static SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn a_signal_to_the_launcher_reaches_the_child_and_not_the_launcher() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // A stand-in child that outlives the test unless it is signalled. The real
    // one is `sandbox-exec` and is covered by Task 9's end-to-end run.
    let mut child = Command::new("/bin/sh")
        .args(["-c", "sleep 30"])
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn");
    let pid = child.id() as i32;

    on_stop(pid).expect("install the stop-signal handling");

    // Signal *this* process, the way a supervisor signals the launcher. The
    // handler `on_stop` installed is what keeps this process alive: before the
    // implementation exists, this line kills the test binary, and that is the
    // failure this test is written to show.
    kill_process(getpid(), Signal::TERM).expect("signal the launcher");

    // The shared bounded poll, and this leg's own ending: the child is killed before the panic so
    // a red run leaves nothing behind (`tinio_util::testing::poll_exit`).
    let status = match poll_exit(&mut child, Duration::from_secs(10)) {
        Some(status) => status,
        None => {
            let _ = child.kill();
            panic!("the child outlived the relayed signal");
        }
    };

    // It died of the relayed signal, not of something else.
    assert_eq!(status.signal(), Some(Signal::TERM.as_raw()), "{status:?}");

    disarm();
}

#[test]
fn a_disarmed_relay_leaves_the_child_alone() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let mut child = Command::new("/bin/sh")
        .args(["-c", "sleep 30"])
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn");
    on_stop(child.id() as i32).expect("install");
    disarm();

    // The handler stays installed after the disarm (that is `disarm`'s documented
    // residue), so this line does not kill the test binary — which is what makes
    // the assertion below about the *relay* and not about this process surviving.
    kill_process(getpid(), Signal::TERM).expect("signal the launcher");

    sleep(Duration::from_millis(200));
    assert!(
        child.try_wait().expect("try_wait").is_none(),
        "a disarmed relay must not forward"
    );
    let _ = child.kill();
    let _ = child.wait();
}
