//! The descriptor handover, read off a **spawned** process's table.
//!
//! `launch::make_inheritable` clears `FD_CLOEXEC` on the plan's listener and on the plan's
//! `ControlHandle` duplicate after the profile pre-flight and before the real spawn, so both
//! descriptors cross `execve` — and the launcher's own copy of the control socket, whose flag
//! nothing touches, does not. Whether a descriptor crosses `execve` is a property of a *child*
//! process: the unit leg beside the code (`the_clear_reaches_the_plans_handles_and_not_the_launchers`
//! in `src/launch.rs`) reads `fcntl_getfd` on the process that performs the clear, so it cannot
//! observe it; the measurement that does is the hand-run
//! `scripts/verify-descriptor-handover.py`. This file is that observation as a standing test: the
//! fixture descriptors are the product's own — `ControlHandle::duplicate` on the plan, cleared by
//! the product's `make_inheritable` inside `confine` — and the parent reads the child's table with
//! `/usr/sbin/lsof -p <pid>`.
//!
//! **Shape (a): a long-lived child that execs, observed from the parent.** The confined child is a
//! process `sandbox-exec` execs, which is exactly the `execve` `FD_CLOEXEC` is about, and its
//! table is read from outside rather than reported by the child itself — nothing here asserts on a
//! claim the measured process made about itself. What is asserted is a *path*, the unix control
//! socket's, which only this handover can put in the child's table. Descriptor numbers are checked
//! too, against [`harness::BASELINE`], because a process inherits numbers from the runtime and
//! libtest and a membership test on a low one can be satisfied by something that has nothing to do
//! with this handoff.
//!
//! **`lsof` missing is a failure, not a skip.** The observation is the whole of what this leg
//! owns, so a leg that quietly passed without it would certify nothing (`docs/tests.md`:
//! confinement tests fail, never skip). `lsof` ships with macOS, and the assertion names the
//! missing tool when it does not.
//!
//! **The roles are the harness's existing ones.** `harness::spawn_confined` re-executes this
//! binary as the **launcher copy** whose argv carries this test's name — the harness's documented
//! route to `confine`, which is why this file adds no env-marked helper of its own — and the
//! launcher copy spawns the **confined child**, which runs this same test with
//! `harness::is_child_process()` true. That is true because the product put the listener's
//! descriptor in the child's argv (decision 17), not because this test marked it. The parent
//! measures the child, releases it, and reaps the launcher copy; every other exit path kills both.

#![cfg(target_os = "macos")]

mod harness;

use std::{
    env, fs, io,
    net::{SocketAddr, TcpStream},
    path::Path,
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};

use tinio_sandbox_core::{AccessMode, Sandbox};
use tinio_util::testing::poll_exit;

/// This test's name, which is also the filter the confined child runs under.
///
/// `confine` derives the child's argv from the launcher copy's own, and the launcher copy's argv
/// is what [`harness::spawn_confined`] passes below, so this one spelling selects this test in
/// both roles — and it is how [`pgrep_child`] tells the confined child apart from the pre-flight
/// `sandbox-exec` that is briefly the launcher copy's other child.
const TEST_NAME: &str = "the_clear_reaches_the_spawned_childs_table_and_not_the_launchers";

/// The child's parked marker: it writes this path once it is past the exec, and the parent takes
/// its measurement only after it appears.
const READY: &str = "ready";

/// The parent's release: writing this path lets the child finish cleanly instead of being killed.
const RELEASE: &str = "release";

/// The parent's bound on the child coming up (profile render, pre-flight, spawn, libtest start).
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The parent's bound on the launcher copy after the release.
const REAP_TIMEOUT: Duration = Duration::from_secs(30);

/// The child's own bound on the park, so a parent that died leaves no process behind.
const PARK: Duration = Duration::from_secs(60);

/// `lsof(8)`, which ships with macOS.
const LSOF: &str = "/usr/sbin/lsof";

/// `pgrep(1)`, which ships with macOS.
const PGREP: &str = "/usr/bin/pgrep";

/// `kill(1)`, which ships with macOS.
const KILL: &str = "/bin/kill";

/// **The product's own clear, observed in a process that execs.**
///
/// The three roles of this one function, in the order each process reaches them: the **confined
/// child** takes the first branch, because the listener's descriptor in its argv is the product's
/// discriminator and not a mark this test added; the **launcher copy** falls through to
/// [`harness::launch`], which is the whole of that role's work, and returns; the ordinary **test
/// runner** falls through both and measures.
#[test]
fn the_clear_reaches_the_spawned_childs_table_and_not_the_launchers() {
    if harness::is_child_process() {
        child_role();
    }
    // No-op unless this process is the launcher copy, which is the only one the harness marked.
    harness::launch();
    if env::var_os(harness::LAUNCH_ENV).is_some() {
        return;
    }
    parent_role();
}

/// The confined child: park until the parent has read this process's table, then report.
///
/// **It never launches anything, and that is what keeps the roles from recursing.** A child that
/// called [`parent_role`] would spawn a launcher copy of its own, and the process table would
/// grow without bound — the failure `docs/tests.md` names. This half only waits.
fn child_role() -> ! {
    let child = harness::child().expect("the confined child carries a payload");
    // Written after `child()` returned, so its existence means this process is past the exec
    // *and* past the payload handover — the state the parent's `lsof` has to observe. Reading
    // the payload is a blocking read of stdin, so this also proves `hand_over` completed.
    fs::write(child.param(READY), "ready\n").expect("write the ready marker");

    let release = child.param(RELEASE);
    let deadline = Instant::now() + PARK;
    while !Path::new(&release).exists() {
        assert!(
            Instant::now() < deadline,
            "the parent did not release this child within {PARK:?}; the descriptor table it was \
             parked for was never read"
        );
        thread::sleep(Duration::from_millis(20));
    }
    harness::finish(&child, &harness::line("parked", "released"));
}

/// The parent: spawn the launcher copy, read the confined child's table, and assert both
/// directions of the handover.
fn parent_role() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("create the served root");
    // The control socket is the fixture `make_inheritable` clears, and the one path in the
    // child's table that only this handover can put there.
    let control = root.join("control.sock");
    let ready = root.join(READY);
    let release = root.join(RELEASE);

    let sandbox = Sandbox::new()
        .expose(&root, AccessMode::ReadWrite)
        .device_files();
    let extra = [
        (harness::CONTROL_PARAM, control.display().to_string()),
        (READY, ready.display().to_string()),
        (RELEASE, release.display().to_string()),
    ];

    let (launcher, report) = harness::spawn_confined(&sandbox, &extra, TEST_NAME);
    let mut children = Children::new(launcher);

    // 1. The child is up and parked. `lsof` cannot be trusted to see a table *before* the exec
    //    that applies `FD_CLOEXEC`, so the observation waits for the child's own marker rather
    //    than polling the pid: a table read while `sandbox-exec` still held the image would show
    //    the descriptor even if the clear had never happened.
    assert!(
        await_path(&ready, READY_TIMEOUT),
        "the confined child never wrote its ready marker ({}) within {READY_TIMEOUT:?}; the \
         launcher copy's stderr carries its side of the failure",
        ready.display()
    );

    // 2. The spawned process, found as the child of the launcher copy this test owns.
    let launcher_pid = children.launcher.id();
    let pid = pgrep_child(launcher_pid, TEST_NAME, READY_TIMEOUT);
    children.confined = Some(pid);

    // 3. The measurement, taken from outside the process it describes.
    let table = descriptor_table(pid);
    let control_name = control.display().to_string();
    let baseline = i64::try_from(harness::BASELINE).expect("the baseline fits an i64");

    // **Both directions in one count.** The plan's duplicate is the descriptor `make_inheritable`
    // clears, so it crosses the exec; the launcher's own socket names the same path and keeps the
    // close-on-exec flag it was bound with, so it does not — and the child's table therefore holds
    // the path exactly once. A second entry would be the launcher's copy leaking across `execve`,
    // which is the handover failing in the direction the unit leg cannot see; no entry at all is
    // the clear never having happened.
    let entries: Vec<&str> = table
        .lines()
        .filter(|line| line.contains(&control_name))
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "the plan's control handle ({control_name}) must be the child's one descriptor at that \
         path: `make_inheritable` clears the duplicate it put on the plan, and the launcher's own \
         socket — the same path, a different descriptor, still close-on-exec — must not cross the \
         exec. `lsof -p {pid}`:\n{table}"
    );
    let control_fd = field_fd(entries[0])
        .unwrap_or_else(|| panic!("no descriptor number in `{}`:\n{table}", entries[0]));
    assert!(
        control_fd > baseline,
        "the cleared control descriptor must land past the baseline (fd {control_fd}, baseline \
         {baseline}), so its number cannot be one this process inherited from libtest. \
         `lsof -p {pid}`:\n{table}"
    );

    // The listener is the second descriptor the same call clears, so it must be in the table too,
    // at a number past the baseline and actually accepting: the port is whatever the launcher copy
    // bound (`127.0.0.1:0`), which is why it is read off the table rather than known here.
    let listener = listen_line(&table).unwrap_or_else(|| {
        panic!("the child holds no listening TCP socket, so the listener `make_inheritable` clears did not cross the exec. `lsof -p {pid}`:\n{table}")
    });
    let listener_fd = field_fd(listener)
        .unwrap_or_else(|| panic!("no descriptor number in `{listener}`:\n{table}"));
    assert!(
        listener_fd > baseline,
        "the cleared listener must land past the baseline (fd {listener_fd}, baseline {baseline}): \
         `{listener}`\n{table}"
    );
    let port = listen_port(listener).unwrap_or_else(|| panic!("no port in `{listener}`:\n{table}"));
    TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port))).unwrap_or_else(|err| {
        panic!("the child's table names a listener on 127.0.0.1:{port} but connecting to it failed ({err}); the descriptor that crossed the exec is not the bound socket. `lsof -p {pid}`:\n{table}")
    });
    // What the run measured, said once: a green test carries this under `--nocapture`, and libtest
    // prints a failing test's captured output beside its assertion.
    eprintln!(
        "spawned child {pid}: {control_name} at fd {control_fd}, listener at fd {listener_fd} on \
         127.0.0.1:{port}; the launcher's own copy of the same socket stayed close-on-exec"
    );

    // 4. Release the child, reap the launcher copy, and read the report it left behind — the
    //    child's half of `harness::finish` writing `payload=ok` is the same handover's other
    //    channel (decision 17), so a green table with no report would mean the child never came
    //    through the payload path at all.
    fs::write(&release, "release\n").expect("release the confined child");
    let status = children.reap(REAP_TIMEOUT);
    assert!(
        status.success(),
        "the launcher copy exited with {status:?}; the confined child's exit is its own"
    );
    let reported = fs::read_to_string(&report).unwrap_or_else(|err| {
        panic!("the confined child wrote no report ({err}); the launcher exited with {status:?}")
    });
    assert!(
        reported.contains("payload=ok"),
        "the child's report does not carry the payload handover's discriminator:\n{reported}"
    );
}

/// The launcher copy this test spawned and, once found, the confined child under it.
///
/// **Every exit path kills both, panicking assertions included.** The parent owns the launcher
/// copy and the launcher copy owns the child, so leaving either behind is a process this test
/// started and did not clean up. The child is killed by pid because the launcher's own handle on
/// it is hidden behind the core's `Child` (the harness's `launch` says so) and this process is not
/// its parent; the children are looked for *before* the launcher copy is killed, because the pid
/// of an orphan is not something this process can find afterwards.
struct Children {
    /// The launcher copy, which the parent reaps.
    launcher: Child,
    /// The confined child's pid, once the observation has found it.
    confined: Option<u32>,
    /// Whether the launcher copy has been reaped, so [`Drop`] never signals a reused pid.
    reaped: bool,
}

impl Children {
    fn new(launcher: Child) -> Self {
        Self {
            launcher,
            confined: None,
            reaped: false,
        }
    }

    /// Wait for the launcher copy within `within`, and return its exit status.
    ///
    /// **The poll is `tinio_util::testing::poll_exit`'s, and the ending here is this fixture's.**
    /// The shared definer kills and panics on a deadline; this one has to leave the launcher for
    /// the fixture's `Drop` to end (its confined child is what the reap is observed against), so it
    /// takes the `None` and panics with its own sentence instead. The shared cadence (50 ms) is
    /// coarser than the 20 ms this used to poll at, which only moves a stuck fixture's failure a
    /// few tens of milliseconds later.
    fn reap(&mut self, within: Duration) -> ExitStatus {
        match poll_exit(&mut self.launcher, within) {
            Some(status) => {
                self.reaped = true;
                status
            }
            None => panic!("the launcher copy did not exit within {within:?}"),
        }
    }
}

impl Drop for Children {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        // **Never panic here.** This runs while a failed assertion unwinds, and a second panic
        // aborts the process; a `pgrep` that cannot run leaves nothing to kill by pid, and the
        // launcher copy's own handle is still killed below.
        let mut targets = pgrep(self.launcher.id(), None).unwrap_or_default();
        targets.extend(self.confined);
        // The pid the observation found is usually also one `pgrep` answers, and a second `kill`
        // of a pid already gone is a stderr line from `kill(1)` on a failing test's output.
        targets.sort_unstable();
        targets.dedup();
        for pid in targets {
            let pid_arg = pid.to_string();
            let _ = Command::new(KILL).args(["-9", pid_arg.as_str()]).status();
        }
        let _ = self.launcher.kill();
        let _ = self.launcher.wait();
    }
}

/// The confined child under `launcher`, which must carry `marker` in its command line.
///
/// The marker is what separates it from the pre-flight's `sandbox-exec` — [`pgrep`] carries the
/// argument — and the poll is what separates it from the moment before the exec: the child is
/// asked for only after it wrote its ready marker, so the pid this answers is one whose table the
/// kernel has already built.
fn pgrep_child(launcher: u32, marker: &str, within: Duration) -> u32 {
    let deadline = Instant::now() + within;
    loop {
        match pgrep(launcher, Some(marker)) {
            Ok(pids) => {
                if let Some(pid) = pids.first() {
                    return *pid;
                }
            }
            Err(err) => panic!(
                "`{PGREP}` could not run ({err}); the spawned process cannot be found, and this \
                 leg fails rather than skipping"
            ),
        }
        assert!(
            Instant::now() < deadline,
            "no child of the launcher copy ({launcher}) carrying `{marker}` appeared within \
             {within:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// `pgrep`'s answer for the children of `parent`, filtered by `marker` when one is given.
///
/// `marker` is what separates the confined child from the pre-flight's `sandbox-exec`, which is
/// briefly the launcher copy's other child and must never be measured: the pre-flight's command
/// line names the profile and `/bin/echo`, and the profile is rendered from the exposure paths,
/// so the test's own name appears only in the real child's argv.
fn pgrep(parent: u32, marker: Option<&str>) -> io::Result<Vec<u32>> {
    let parent = parent.to_string();
    let mut command = Command::new(PGREP);
    command.args(["-P", parent.as_str()]);
    if let Some(marker) = marker {
        command.args(["-f", marker]);
    }
    let out = command.output()?;
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect())
}

/// The descriptor table `lsof -p pid` reports, as text.
fn descriptor_table(pid: u32) -> String {
    let pid_arg = pid.to_string();
    let out = Command::new(LSOF)
        .args(["-p", pid_arg.as_str()])
        .output()
        .unwrap_or_else(|err| {
            panic!(
                "`{LSOF} -p {pid}` could not run ({err}); the spawned process's descriptor table \
                 is the whole observation, and this leg fails rather than skipping"
            )
        });
    assert!(
        out.status.success(),
        "`{LSOF} -p {pid}` exited {}: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The descriptor number `lsof` printed in its `FD` column, when the line has one.
///
/// The column carries a mode suffix (`6u` for read-write, `6r`, `6w`), so the number is the
/// leading run of digits — and the `cwd`/`txt` rows have none, which is what makes this `Option`.
fn field_fd(line: &str) -> Option<i64> {
    let field = line.split_whitespace().nth(3)?;
    let digits: String = field.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// The child's listening TCP socket line, when its table has one.
fn listen_line(table: &str) -> Option<&str> {
    table
        .lines()
        .find(|line| line.contains("TCP") && line.contains("(LISTEN)"))
}

/// The port of an `lsof` listening-socket line.
fn listen_port(line: &str) -> Option<u16> {
    line.split_whitespace().find_map(|field| {
        field
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse().ok())
    })
}

/// Whether `path` appears within `within`.
fn await_path(path: &Path, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    path.exists()
}
