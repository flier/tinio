//! Stop-hook roles and the process-group spawn the signal suite drives.

use std::{
    env, fs, io,
    os::windows::process::CommandExt,
    path::Path,
    process::{self, Child, Command, Stdio},
    thread,
    time::Duration,
};

use tinio_sandbox_windows::launch;

use super::roles::{append_to_report, child_role_body, probe_dir};

/// The file a child writes once the stop hook a forwarded stop lands on is
/// installed.
///
/// Its existence is the parent's licence to send a stop: before it, the event
/// would arrive with nothing to land on and take the default disposition.
pub const ARMED: &str = "armed";

/// The file the stop hook writes when it fires.
///
/// **This is the evidence the signal relay exists**, and the reason it is a file
/// the hook itself writes rather than a liveness signal the parent samples: a
/// child that was never reached cannot produce it.
pub const STOPPED: &str = "stopped";

/// The file a child appends to while it is alive.
///
/// Scaffolding, not evidence: a heartbeat is the same shape whether the child is
/// working or starved, so what it is for is the *negative* case — a child whose
/// hook stayed silent can only be said to have been missed if it was alive and
/// writing for the whole window.
pub const BEAT: &str = "beat";

/// The status the stop hook leaves behind.
///
/// The child's hook ends the process with it, which is a stopped server's shape
/// (it drains and leaves) and the way the parent learns the hook ran from the
/// event protocol's other side: the launcher forwards this status to its own
/// caller (design §3 step 8).
pub const STOPPED_EXIT: i32 = 41;

/// The flag a child and a launcher are both started with, so each is its own
/// process **group** — the unit a console control event is addressed to, and the
/// flag the launch itself passes (`crate::process`, design §3 step 9).
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

/// Beat once every 200 ms into `<report>/beat`, for as long as the process lives.
///
/// The child's own liveness, and nothing else — see [`BEAT`] for what it is for
/// and what it is not.
pub fn beat(report: &Path) -> ! {
    let beat = report.join(BEAT);
    loop {
        append_to_report(&beat, ".");
        thread::sleep(Duration::from_millis(200));
    }
}

/// Arm the hook a forwarded stop lands on, and announce it (Task 10).
///
/// The child-side half of the signal relay: without it a console control event
/// takes the default disposition and the process dies where it stands, instead
/// of draining — which is the whole reason the launcher forwards one at all
/// (design §3 step 9). [`ARMED`] is written after the hook is installed, so the
/// parent never sends a stop into a window where nothing would catch it.
///
/// **The hook writes [`STOPPED`] and then ends the process with
/// [`STOPPED_EXIT`].** The file is the evidence; the exit is what unwinds the
/// tree — the launcher's wait returns, its ACL rollback runs and it forwards the
/// status — so a test that reached its hook also leaves no process behind.
fn stop_hook(report: &Path) {
    let stopped = report.join(STOPPED);
    launch::on_stop(move || {
        append_to_report(&stopped, "stopped");
        process::exit(STOPPED_EXIT);
    })
    .expect("arm the stop hook");
    append_to_report(&report.join(ARMED), "armed");
}

/// The confined child, with the stop hook armed before it does anything else.
///
/// The payload's configuration is the probe file's path ([`child_role_body`]'s
/// rule), so the report directory the hook writes into is the launcher's own
/// choice and not this process's.
pub fn child_role_with_stop_hook() {
    let Some(adopted) =
        launch::adopt(env::args_os(), io::stdin().lock()).expect("adopt the payload")
    else {
        return;
    };
    let listener = launch::listener(&adopted).expect("the harness bound a listener");
    let probe = fs::read_to_string(adopted.config()).expect("read the probe file");
    let report = probe_dir(&probe);
    stop_hook(&report);
    let beating = report.clone();
    thread::spawn(move || beat(&beating));
    child_role_body(adopted.config(), listener);
}

/// The unrelayed child's role: the same hook and the same beat, with no payload
/// and no listener — never returns, so a stop that never arrives leaves it
/// beating until the test that started it ends it.
///
/// The probe text arrives in the **environment** here, and this is the harness's
/// one use of that channel: the child is spawned directly rather than launched,
/// so there is no payload for it to inherit, and the test is measuring a console
/// control event rather than a payload. Legitimate precisely because this child
/// is **not** confined (design decision 17 keeps the environment out of the
/// confined path, where a marker would be forgeable).
pub fn unrelayed_child_role(probe: &str) -> ! {
    let report = probe_dir(probe);
    stop_hook(&report);
    beat(&report)
}

/// A child in its own process group with nothing supervising it: the group a
/// stop can be addressed to directly, which is what the relay's own address
/// space looks like from the outside.
///
/// The caller owns the returned process and must reap it — the signal suite's
/// guard does, on every path. `stdout` is null because a test binary's banner is
/// not this suite's evidence and the event protocol's channel is not a place to
/// put it (design decision 19); `stderr` is inherited because that is the log
/// channel and a child's panic has to be readable.
pub fn spawn_unrelayed_child_in_own_group(probe: &Path) -> Child {
    Command::new(env::current_exe().expect("the test executable"))
        .args(["child_role", "--exact"])
        .env("TINIO_SANDBOX_TEST_PROBE", probe)
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the unrelayed child")
}

/// A launcher of this test binary, in its own process group — **which is the
/// address a stop goes to, and the reason this is one function rather than a
/// `Command` at each call site**.
///
/// The launcher role is a separate process because a console control event is
/// addressed at a *process group*: one aimed at the test process would test
/// nothing. `TINIO_SANDBOX_TEST_TREE` is the tree it works in — the role cannot
/// receive a [`Tree`], it is a fresh process.
///
/// The two arguments are the events suite's, and it is the only caller that
/// needs either: `stdout` is [the relay it asserts on](spawn_launcher) rather
/// than libtest's banner, so it pipes it where the signals suite, whose launcher
/// is addressed but never read, discards it; `quiet` passes libtest's `--quiet`,
/// which is what narrows the banner that suite has to filter. **A call site that
/// re-spelled this `Command` would drop the creation flag as well as the
/// environment**, and the flag is the whole mechanism: the copy in the events
/// suite did exactly that, which is the defect this signature closes.
pub fn spawn_launcher(tree: &Path, stdout: Stdio, quiet: bool) -> Child {
    Command::new(env::current_exe().expect("the test executable"))
        .args(["launcher_role", "--exact"])
        .args(quiet.then_some("--quiet"))
        .env("TINIO_SANDBOX_TEST_LAUNCHER", "1")
        .env("TINIO_SANDBOX_TEST_TREE", tree)
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(stdout)
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the launcher")
}
