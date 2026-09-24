//! CLI step definitions (T055): the scenarios that spawn the real `tinio` binary.
//!
//! These are the process-level legs `crates/tinio/tests/lifecycle.rs` used to hold. What stayed
//! there is the console-handler leg, which needs `GenerateConsoleCtrlEvent` under
//! `#[cfg(windows)]` and one of the workspace's five enumerated `#[allow(unsafe_code)]` sites;
//! its module doc says so, and `docs/tests.md`'s "CLI (tinio)" section carries the leg-by-leg
//! mapping.
//!
//! One harness per scenario ([`Cli`]) with one scratch directory under it: the served root, the
//! state dir every child is handed in `TINIO_STATE_DIR`, the systemd datagram socket and each
//! run's two stream files. No child reads the operator's home or a real root, and nothing here
//! mutates this process's environment — a variable rides on the child, never on the test runner
//! (Global Constraints).
//!
//! **Streams are files, not pipes.** A piped capture can wait forever on EOF when a descendant
//! holds the write end — measured on Windows for a detached daemon whose parent had already
//! exited — so each run writes to two files and the steps read them back. Readiness is a poll of
//! the stdout file, which is also what lets a run that *refuses* before it serves be captured
//! without a reader thread: the poll ends at the child's exit, and the file holds whatever it
//! said on the way out.

#[cfg(unix)]
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::net::UnixDatagram;
use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use assert_cmd::cargo::cargo_bin;
use cucumber::{gherkin::Step, given, then, when};
use tempfile::TempDir;
use tinio_sandbox::Event;
use tokio::time::sleep;

use super::World;

/// How long a run may go without exiting and without reporting readiness before the harness
/// calls it hung.
///
/// A failure bound, not a latency assertion: a `--daemon` parent spends its own 10 s readiness
/// bound before it refuses, and every scenario that cares about latency asserts it itself.
const RUN_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a **confined** run may go the same way.
///
/// The launcher's own bound, measured rather than guessed: before `CreateProcessW` it grants the
/// `(OI)(CI)` ACE on the served root and `RX` on the `*.dll` beside the binary (`docs/tests.md`'s
/// Windows "Grant cost": 32.3–37.8 s on NTFS against a 30 s readiness budget), and this harness
/// measured 4.6 s one run and past 120 s the next on one host. A bound sized for an unconfined
/// run would read that grant as a hang.
const CONFINED_RUN_TIMEOUT: Duration = Duration::from_secs(180);

/// How long the teardown's own client waits for the daemon it is ending — it exists to end a
/// process, not to assert on it, so it is bounded rather than polled forever.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// One poll's wait: a run's exit and its readiness event are both watched by polling, and no
/// scenario asserts on the poll's own cadence.
const POLL: Duration = Duration::from_millis(20);

/// How long a datagram a healthy lifecycle always sends is given before a step fails. Generous
/// on purpose: the subject is the *sequence* of notifications, not how fast a host starts a
/// server, and a tighter bound would fail a loaded machine rather than a defect. The steps that
/// assert a cadence carry their own, tighter bounds.
#[cfg(unix)]
const NOTIFY_DEADLINE: Duration = Duration::from_secs(20);

/// The datagram socket's read timeout — one poll's wait ([`POLL`]'s unix half).
#[cfg(unix)]
const NOTIFY_POLL: Duration = Duration::from_millis(100);

/// A scenario's CLI harness: the scratch directory everything lives under, the servers a run
/// left serving, and the last run's captured outcome.
#[derive(Debug)]
pub struct Cli {
    /// The scenario's scratch directory. The served root, the override state dir, the notify
    /// socket and every run's two stream files live under it, so no scenario touches a real
    /// root or the operator's home.
    dir: TempDir,
    /// The served root (the scratch directory's `root`).
    root: PathBuf,
    /// The `TINIO_STATE_DIR` every child is handed: the relocated state dir a read-only run
    /// resolves *first*, and the candidate a read-write client probes second (decision 19).
    state: PathBuf,
    /// The variables the scenario asks for, applied to every child it spawns.
    vars: Vec<(String, String)>,
    /// The entries the root held when the scenario claimed it: what "exactly as it found it" is
    /// measured against.
    entries: usize,
    /// The servers a run left serving, in spawn order. Killed and reaped by [`Cli::teardown`].
    children: Vec<Child>,
    /// The root a `--daemon` run left serving.
    ///
    /// A daemon is a detached grandchild this process holds no handle to (`daemon.rs`: a double
    /// fork on unix, a detached child on Windows), so the only way to end it is the client the
    /// scenario drives — [`Cli::teardown`] does that on every path out, so a failing assertion
    /// cannot leave one holding a temporary root. `None` while no run detached one.
    daemon: Option<PathBuf>,
    /// Whether this binary's parser knows `--sandbox` — the probe the sandbox scenario's
    /// discrimination rests on ([`Cli::knows_flag`]).
    sandbox_flag: Option<bool>,
    /// The endpoint the last readiness event reported, kept across runs so a later `status` run
    /// can be held to the address the server actually bound.
    endpoint: Option<String>,
    /// The last run; every run replaces it.
    last: Option<Run>,
    /// How many runs this scenario has made: each writes its own two stream files.
    runs: usize,
    /// systemd's end of the notify protocol, when the scenario bound one (unix only: off Linux
    /// there is no protocol to speak — `sd-notify` cannot even link on Windows).
    #[cfg(unix)]
    notify: Option<Notify>,
}

/// One run of the binary, as the operator would have seen it: the exit status (absent while it
/// still serves), both streams, and the timings the scenarios assert.
#[derive(Debug)]
struct Run {
    /// `None` for a run that is still serving when the poll loop ends.
    status: Option<ExitStatus>,
    /// Everything the run wrote to stdout.
    stdout: String,
    /// Everything the run wrote to stderr.
    stderr: String,
    /// The endpoint the run reported as its `listening` event, when it got that far.
    readiness: Option<SocketAddr>,
    /// Spawn → readiness event, for the runs that reported one.
    ready_in: Option<Duration>,
    /// Spawn → exit, or spawn → readiness for a run that never exited.
    took: Duration,
}

impl Cli {
    /// Claims a scratch directory and makes the root the scenario serves under it.
    fn new() -> Self {
        let dir = TempDir::new().expect("a scratch directory");
        let root = dir.path().join("root");
        fs::create_dir_all(&root).expect("the served root");
        let entries = fs::read_dir(&root).expect("read the root").count();
        Self {
            state: dir.path().join("state"),
            dir,
            root,
            vars: Vec::new(),
            entries,
            children: Vec::new(),
            daemon: None,
            sandbox_flag: None,
            endpoint: None,
            last: None,
            runs: 0,
            #[cfg(unix)]
            notify: None,
        }
    }

    /// The scratch directory: what a child's working directory and `HOME`/`USERPROFILE` are.
    fn scratch(&self) -> &Path {
        self.dir.path()
    }

    /// The served root.
    fn root(&self) -> &Path {
        &self.root
    }

    /// The root as the CLI resolves it: canonical, which is the identity the state dir, the
    /// control channel's name and `state`'s own record are all derived from
    /// (`resolve::resolve_root`) — and on Windows the `\\?\` form `Path::canonicalize` answers,
    /// which is what every command prints.
    fn canonical_root(&self) -> PathBuf {
        self.root
            .canonicalize()
            .expect("the served root exists while its scenario runs")
    }

    /// The last run's outcome; every `Then` step here reads it, and none may run before a `When`
    /// step made one.
    fn last(&self) -> &Run {
        self.last
            .as_ref()
            .expect("a scenario's assertions follow the `I run` step that captures them")
    }

    /// A child of this harness: the binary, the scratch working directory, and the one
    /// environment every leg is held to.
    ///
    /// **The child's environment is built, never inherited.** A `TINIO_*` the operator's shell
    /// exports — `TINIO_PORT`, `TINIO_READ_ONLY`, `TINIO_SANDBOX`, the credential pair — would
    /// change what these scenarios serve, and nothing here may `set_var` in this process (Global
    /// Constraints), so every `TINIO_*` is removed from the child and the scenario names the
    /// ones it wants back. `HOME`/`USERPROFILE` point at the scratch dir for the home
    /// derivation's fallback, and `TINIO_STATE_DIR` is the candidate that keeps a client out of
    /// a real home on either platform — on Windows the only mechanism that can, since neither
    /// variable is what `dirs::home_dir()` reads.
    fn command(&self) -> Command {
        let mut cmd = Command::new(cargo_bin("tinio"));
        for (key, _) in env::vars_os() {
            if key.to_string_lossy().starts_with("TINIO_") {
                cmd.env_remove(&key);
            }
        }
        cmd.env_remove("NOTIFY_SOCKET")
            .env_remove("WATCHDOG_USEC")
            .env_remove("WATCHDOG_PID")
            .current_dir(self.scratch())
            .env("TINIO_STATE_DIR", &self.state)
            .env("HOME", self.scratch())
            .env("USERPROFILE", self.scratch());
        for (key, value) in &self.vars {
            cmd.env(key, value);
        }
        cmd
    }

    /// The operator's command line as argv: whitespace-separated words, with the `{root}` and
    /// `{state}` placeholders expanded **per word** — so a scratch path with a space in it stays
    /// one argument without the scenario needing to quote it.
    fn argv(&self, command: &str) -> Vec<String> {
        let words: Vec<String> = command
            .split_whitespace()
            .map(|word| {
                word.replace("{root}", &self.root.display().to_string())
                    .replace("{state}", &self.state.display().to_string())
            })
            .collect();
        assert_eq!(
            words.first().map(String::as_str),
            Some("tinio"),
            "a CLI command line starts with `tinio`: {command:?}"
        );
        words
    }

    /// Whether this run asks for a confinement — the flag on the command line, or the variable a
    /// scenario put on the child ([`Cli::vars`]). Only the *budget* rides on this: which
    /// assertion the run owes is the step's own dispatch.
    fn confined(&self, argv: &[String]) -> bool {
        let flagged = argv.iter().any(|arg| arg == "--sandbox");
        let variable = self.vars.iter().any(|(name, value)| {
            name == "TINIO_SANDBOX" && !matches!(value.as_str(), "0" | "false")
        });
        flagged || variable
    }

    /// Runs the operator's command line — `tinio` and all — the way the scenario's environment
    /// asks, and captures what it did.
    ///
    /// **The loop ends at the first of three things**: the child's exit, a readiness event, or
    /// the deadline. Which of the two events counts as the end depends on the spelling: a
    /// `--daemon` run's readiness event is the *parent*'s, and the parent exits once it has
    /// proved the daemon ready, so for that command the exit is what the scenario asserts and
    /// the event is only read afterwards. Every other run that reports it *is* the server,
    /// and is kept — [`Cli::children`] kills and reaps it on the way out.
    async fn run(&mut self, command: &str) {
        let argv = self.argv(command);
        let detached = argv.iter().any(|arg| arg == "--daemon");
        let (out_path, err_path) = self.run_paths();
        let mut cmd = self.command();
        cmd.args(&argv[1..])
            .stdin(Stdio::null())
            .stdout(Stdio::from(
                fs::File::create(&out_path).expect("the run's stdout file"),
            ))
            .stderr(Stdio::from(
                fs::File::create(&err_path).expect("the run's stderr file"),
            ));
        let started = Instant::now();
        let mut child = cmd.spawn().expect("spawn the tinio binary");
        let budget = if self.confined(&argv) {
            CONFINED_RUN_TIMEOUT
        } else {
            RUN_TIMEOUT
        };
        let deadline = started + budget;
        let mut status = None;
        let mut ready_in = None;
        loop {
            if let Some(exit) = child.try_wait().expect("wait on the child") {
                status = Some(exit);
                break;
            }
            if !detached && readiness(&read(&out_path)).is_some() {
                ready_in = Some(started.elapsed());
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "`{command}` neither exited nor became ready within {}s\nstdout:\n{}\nstderr:\n{}",
                    budget.as_secs(),
                    read(&out_path),
                    read(&err_path)
                );
            }
            sleep(POLL).await;
        }
        let took = started.elapsed();
        let (stdout, stderr) = (read(&out_path), read(&err_path));
        let address = readiness(&stdout);
        if let Some(endpoint) = address {
            self.endpoint = Some(endpoint.to_string());
        }
        // A successful `--daemon` run leaves a process nothing here holds a handle to: the
        // `state` file it published is the record, and it is what the teardown stops the daemon
        // through.
        if detached && status.is_some_and(|exit| exit.success()) && state_file(&self.root).exists()
        {
            self.daemon = Some(self.root.clone());
        }
        if status.is_none() {
            self.children.push(child);
        }
        self.last = Some(Run {
            status,
            stdout,
            stderr,
            readiness: address,
            ready_in,
            took,
        });
    }

    /// The two files this run's streams land in, under the scratch dir so a failed scenario can
    /// be dissected after the fact. Each run gets its own pair, so a scenario's earlier runs
    /// stay readable beside the last one's.
    fn run_paths(&mut self) -> (PathBuf, PathBuf) {
        let n = self.runs;
        self.runs += 1;
        (
            self.scratch().join(format!("run-{n}.stdout")),
            self.scratch().join(format!("run-{n}.stderr")),
        )
    }

    /// Starts a server on the root and requires it to be serving — the `Given` half of every
    /// scenario that needs one.
    async fn serve(&mut self) {
        self.run("tinio server {root} --port 0").await;
        if self.last().readiness.is_none() {
            let run = self.last();
            panic!(
                "the server never reported readiness (status {:?})\nstdout:\n{}\nstderr:\n{}",
                run.status, run.stdout, run.stderr
            );
        }
    }

    /// Kills and reaps every server a run left serving.
    ///
    /// A killed server leaves its `state` file behind — no drain, no removal — which is exactly
    /// what the stale-state scenario needs.
    fn kill_servers(&mut self) {
        for mut child in self.children.drain(..) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Whether this binary's parser knows `flag`.
    ///
    /// **Clap is the fact, never `cfg!`.** The `tinio` package forwards `tinio-cli/sandbox` and
    /// the workspace pins `tinio-cli` with `default-features = false`, so a
    /// `cfg!(feature = "sandbox")` written here would answer about *tinio-e2e* and not about the
    /// binary it spawns — the two disagree in exactly the graph the sandbox scenario has to
    /// discriminate. A flag the parser does not know is clap's `error: unexpected argument`; a
    /// flag it knows and cannot honour answers the platform's own refusal.
    ///
    /// **`--help` is what keeps the probe bounded, and it is load-bearing.** A flag this parser
    /// knows is an honourable confinement on Linux and Windows, so the bare command line would
    /// *serve* — and on Windows the launcher parent supervises its child and does not exit either
    /// — leaving an unbounded `output()` waiting on a server rather than on a parse. `--help` is
    /// reached only once every argument before it parsed, and it exits without serving anything
    /// (no state dir, no `state` file). The discriminator stays clap's own words: the help rides
    /// stderr in this two-pass CLI, so the phrase searched for is the *error's*, not the help's.
    fn knows_flag(&self, flag: &str) -> bool {
        let mut cmd = self.command();
        cmd.arg("server")
            .arg(&self.root)
            .args(["--port", "0", flag, "--help"]);
        let out = cmd.output().expect("probe the flag");
        !String::from_utf8_lossy(&out.stderr).contains("error: unexpected argument")
    }

    /// Ends everything the scenario started.
    ///
    /// The `#[after]` hook calls this — hence `pub`, since the hook lives in the parent module
    /// — so no path out of a scenario, a failed assertion included, leaves a server holding a
    /// temporary root.
    pub fn teardown(&mut self) {
        self.kill_servers();
        if let Some(root) = self.daemon.take() {
            self.stop_daemon(&root);
        }
    }

    /// Asks a daemon the scenario detached to stop, and waits for the artifact that says it is
    /// gone.
    ///
    /// The client the scenario drives is the only way to reach it (a daemon is a detached
    /// grandchild — see [`Cli::daemon`]), so this is best-effort by design: the scenario may
    /// well have stopped it already, and a `stop` with nothing running is exit 1 by contract.
    /// Both waits are bounded — the client's exit, then the removal of the `state` file, which
    /// is what makes "it is gone" an observation rather than a hope.
    fn stop_daemon(&self, root: &Path) {
        let mut cmd = self.command();
        cmd.arg("stop")
            .arg(root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Ok(mut child) = cmd.spawn() {
            let deadline = Instant::now() + TEARDOWN_TIMEOUT;
            while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                thread::sleep(POLL);
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        let file = state_file(root);
        let deadline = Instant::now() + TEARDOWN_TIMEOUT;
        while file.exists() && Instant::now() < deadline {
            thread::sleep(POLL);
        }
    }
}

impl Drop for Cli {
    fn drop(&mut self) {
        // The backstop behind `teardown`; a daemon is deliberately not in a `Drop`'s reach (no
        // handle from here would touch it), which is why `teardown` asks it to stop instead.
        self.kill_servers();
    }
}

/// systemd's end of the notify protocol (decision 21): a datagram socket the child is pointed at
/// through `NOTIFY_SOCKET`, and every datagram read from it so far.
#[cfg(unix)]
#[derive(Debug)]
struct Notify {
    socket: UnixDatagram,
    /// The datagrams, in arrival order — so a step asserts on the sequence it actually received
    /// rather than only on the line it was waiting for.
    seen: Vec<String>,
}

#[cfg(unix)]
impl Notify {
    /// Binds `path` as systemd's own end would, and hands the child the variable that points at
    /// it.
    fn bind(path: &Path, vars: &mut Vec<(String, String)>) -> Self {
        let socket = UnixDatagram::bind(path).expect("systemd's end of the protocol");
        socket
            .set_read_timeout(Some(NOTIFY_POLL))
            .expect("a read timeout, so a datagram that never arrives costs the deadline");
        vars.push(("NOTIFY_SOCKET".to_string(), path.display().to_string()));
        Self {
            socket,
            seen: Vec::new(),
        }
    }

    /// Reads datagrams until `tag` has arrived `count` times or `within` expires, and answers
    /// how many it has seen of that tag — so the caller's `assert!` is the failure rather than a
    /// hang.
    async fn receive(&mut self, tag: &str, count: usize, within: Duration) -> usize {
        let deadline = Instant::now() + within;
        let mut buffer = [0u8; 4096];
        loop {
            let arrived = self.seen.iter().filter(|d| d.starts_with(tag)).count();
            if arrived >= count || Instant::now() >= deadline {
                return arrived;
            }
            match self.socket.recv(&mut buffer) {
                Ok(len) => self
                    .seen
                    .push(String::from_utf8_lossy(&buffer[..len]).into_owned()),
                // The read timeout did the waiting; the deadline owns the loop.
                Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                // Anything else is a socket error no poll can fix; the sleep keeps the loop
                // from spinning on it while the deadline runs out.
                Err(_) => sleep(POLL).await,
            }
        }
    }
}

/// The scenario's harness — the `Given a storage root` step claims it, and every step here needs
/// it.
fn cli(world: &mut World) -> &mut Cli {
    world.cli.as_mut().expect("the scenario has a storage root")
}

/// The endpoint `text` reports as its readiness event, if it holds one.
///
/// **The event and not a scraped line** (decision 19): the serving path's whole stdout is event
/// frames, so the readiness point is the `listening` frame's typed address rather than text
/// this side parses. `decode` rejects anything that is not one of our frames, which is what
/// keeps a stray write to stdout from reading as readiness.
fn readiness(text: &str) -> Option<SocketAddr> {
    match Event::decode(text.trim_end()) {
        Some(Event::Listening { addr }) => Some(addr),
        _ => None,
    }
}

/// The `state` file the management plane publishes for `root` (`api::state::path`: the reserved
/// directory's own file).
fn state_file(root: &Path) -> PathBuf {
    root.join(".tinio").join("state")
}

/// A file's text, or nothing when it has not been written yet — a run that has not spoken is
/// not an error here, it is a poll that found nothing.
fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// A step's docstring (`"""`-delimited in the feature), trimmed of the newline the block adds.
fn docstring(step: &Step) -> String {
    step.docstring()
        .map(|text| text.trim_matches('\n'))
        .unwrap_or_default()
        .to_string()
}

// --- Given --------------------------------------------------------------

/// The scenario's scratch root: the `Given` that claims the harness. A second call in the same
/// scenario is the same root, never a new one — a scenario serves one root.
#[given("a storage root")]
async fn a_storage_root(world: &mut World) {
    if world.cli.is_none() {
        world.cli = Some(Cli::new());
    }
}

/// A server already serving the root — the precondition of every leg about what a *second*
/// process (or a client) does.
#[given("a server is serving the root")]
async fn a_server_is_serving(world: &mut World) {
    cli(world).serve().await;
}

/// One variable the scenario wants on every child, in the operator's own spelling
/// (`TINIO_PORT=0` is what a shell would export).
#[given(expr = "the child's environment holds {string} = {string}")]
async fn environment_holds(world: &mut World, name: String, value: String) {
    cli(world).vars.push((name, value));
}

/// A `.env` file where the resolution reads it: `<state dir>/.env` — the reserved directory in
/// read-write mode, which is the file that makes the second parse observable at all (the child
/// starts after the file is written, so discovery cannot have seen it).
#[given(expr = "the root's state dir holds a {string} file")]
async fn state_dir_file(world: &mut World, name: String, step: &Step) {
    let state_dir = cli(world).root().join(".tinio");
    fs::create_dir_all(&state_dir).expect("the reserved directory");
    fs::write(state_dir.join(name), docstring(step)).expect("the file");
}

/// systemd's end of the notify socket, bound before the child starts so nothing it sends is
/// missed.
#[cfg(unix)]
#[given("the systemd notify socket is bound")]
async fn notify_socket(world: &mut World) {
    let cli = cli(world);
    let path = cli.scratch().join("systemd.sock");
    cli.notify = Some(Notify::bind(&path, &mut cli.vars));
}

/// The watchdog systemd asks for, in its own unit's spelling: microseconds. The child halves it
/// (`notify::watchdog_interval`), so the beat a scenario should see is half this value — and the
/// value is the *scenario's* rather than a policy: `packaging/tinio.service` carries the
/// operator's, and editing that line changes the cadence with no code change.
#[cfg(unix)]
#[given(expr = "systemd's watchdog interval is {int} microseconds")]
async fn watchdog_interval(world: &mut World, microseconds: u64) {
    cli(world)
        .vars
        .push(("WATCHDOG_USEC".into(), microseconds.to_string()));
}

/// Whether this binary's parser knows the flag, kept for the discrimination step below.
#[given(expr = "the binary's {string} flag is probed")]
async fn flag_is_probed(world: &mut World, flag: String) {
    let built_in = cli(world).knows_flag(&flag);
    cli(world).sandbox_flag = Some(built_in);
}

// --- When ---------------------------------------------------------------

/// The operator's command line, run to whatever end it reaches: an exit, or a server that keeps
/// serving.
#[when(expr = "I run {string}")]
async fn run_command(world: &mut World, command: String) {
    cli(world).run(&command).await;
}

/// Ends the server without letting it drain — the only way to leave a `state` file behind that
/// nothing answers for.
#[when("I kill the server")]
async fn kill_the_server(world: &mut World) {
    cli(world).kill_servers();
}

// --- Then ---------------------------------------------------------------

/// The run exited 0.
#[then("the run succeeds")]
async fn run_succeeds(world: &mut World) {
    let run = cli(world).last();
    let status = run
        .status
        .expect("the run has exited — a run that is still serving has no exit status");
    assert!(
        status.success(),
        "the run exited {status}\nstdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr
    );
}

/// The run exited non-zero — for the legs whose contract is "some error", where the *message*
/// is the assertion that matters.
#[then("the run fails")]
async fn run_fails(world: &mut World) {
    let run = cli(world).last();
    let status = run
        .status
        .expect("the run has exited — a run that is still serving has no exit status");
    assert!(
        !status.success(),
        "the run exited 0\nstdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr
    );
}

/// The run exited with `code` — the class the contract fixes (2 for usage errors, 1 for
/// operational ones).
#[then(expr = "the run fails with exit code {int}")]
async fn run_fails_with_code(world: &mut World, code: u64) {
    let run = cli(world).last();
    let status = run
        .status
        .expect("the run has exited — a run that is still serving has no exit status");
    assert_eq!(
        status.code(),
        Some(i32::try_from(code).expect("a plausible exit code")),
        "stdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr
    );
}

/// The endpoint a server reports when it starts, and the port it actually bound.
///
/// **One assertion per fact, and the address is the typed one the event carried**: the port is
/// not `0` — the literal a run asks for and cannot have bound — and it is not `--port`'s 9000
/// default, so the port is the one the *run asked for*, whichever channel supplied it (a flag,
/// `TINIO_PORT` or the `.env` file). An unmodified default would pass a leg that only looked for
/// a readiness event.
#[then("the readiness line reports a bound port")]
async fn ready_line_reports_port(world: &mut World) {
    let run = cli(world).last();
    let address = run.readiness.expect("the run reported its readiness event");
    assert_ne!(
        address.port(),
        0,
        "`--port 0` is ephemeral and reported bound: {address}"
    );
    assert_ne!(
        address.port(),
        9000,
        "the port is the one the run was given, never the default: {address}"
    );
}

/// SC-005: the spawn-to-readiness time of the run that just reported one. The parent of a
/// `--daemon` run is the one that waited, which is why the daemon scenario asserts this on the
/// parent's own line.
#[then(expr = "the server became ready within {int} milliseconds")]
async fn ready_within(world: &mut World, milliseconds: u64) {
    let run = cli(world).last();
    let ready_in = run
        .ready_in
        .expect("the run reported a readiness event to time");
    assert!(
        ready_in < Duration::from_millis(milliseconds),
        "ready within {milliseconds} ms, took {ready_in:?}"
    );
}

/// SC-007's shape, and the daemon parent's bound: how long the run itself took, spawn to exit.
#[then(expr = "the run returned within {int} milliseconds")]
async fn returned_within(world: &mut World, milliseconds: u64) {
    let took = cli(world).last().took;
    assert!(
        took < Duration::from_millis(milliseconds),
        "the run returned within {milliseconds} ms, took {took:?}"
    );
}

/// The run's stdout carries `text` — a state the command *reports* (`running`, `stopped`).
#[then(expr = "its stdout contains {string}")]
async fn stdout_contains(world: &mut World, text: String) {
    let stdout = &cli(world).last().stdout;
    assert!(stdout.contains(&text), "stdout lacks {text:?}:\n{stdout}");
}

/// The run's stderr carries `text` — the message an operator reads.
#[then(expr = "its stderr contains {string}")]
async fn stderr_contains(world: &mut World, text: String) {
    let stderr = &cli(world).last().stderr;
    assert!(stderr.contains(&text), "stderr lacks {text:?}:\n{stderr}");
}

/// A usage error says what to do, so its stderr is never empty.
#[then("its stderr is not empty")]
async fn stderr_not_empty(world: &mut World) {
    let stderr = &cli(world).last().stderr;
    assert!(!stderr.is_empty(), "a usage error explains itself");
}

/// `status` reports the endpoint the server actually bound — the address its readiness event
/// named, not a re-derivation.
#[then("its stdout names the bound endpoint")]
async fn stdout_names_endpoint(world: &mut World) {
    let cli = cli(world);
    let endpoint = cli
        .endpoint
        .clone()
        .expect("a server run reported an endpoint");
    let stdout = &cli.last().stdout;
    assert!(
        stdout.contains(&endpoint),
        "status prints the endpoint `{endpoint}`:\n{stdout}"
    );
}

/// The answer names the state directory that *answered* — the resolved path, because both
/// candidates contain `.tinio` and only the full path says which one it was.
#[then("its stdout names the root's state dir")]
async fn stdout_names_root_state_dir(world: &mut World) {
    let cli = cli(world);
    let named = cli.canonical_root().join(".tinio").display().to_string();
    let stdout = &cli.last().stdout;
    assert!(
        stdout.contains(&named),
        "the answer names the state dir that answered (`{named}`):\n{stdout}"
    );
}

/// The other half of decision 19: the relocated candidate held no server, so it is not the one
/// the answer names.
#[then("its stdout does not name the relocated state dir")]
async fn stdout_omits_relocated_state_dir(world: &mut World) {
    let cli = cli(world);
    let relocated = cli.state.display().to_string();
    let stdout = &cli.last().stdout;
    assert!(
        !stdout.contains(&relocated),
        "the first candidate held no server, so it is not the one named (`{relocated}`):\n{stdout}"
    );
}

/// The daemon published `state` before the parent returned — the artifact that makes "ready"
/// twice-provable rather than a message.
#[then("the daemon published its state file")]
async fn daemon_published_state(world: &mut World) {
    let file = state_file(cli(world).root());
    assert!(file.is_file(), "the daemon published `{}`", file.display());
}

/// The same artifact, read the other way round: a killed server wrote `state` and never removed
/// it, and nothing answers for it.
#[then("the root still holds its state file")]
async fn root_holds_state(world: &mut World) {
    let file = state_file(cli(world).root());
    assert!(
        file.is_file(),
        "a killed server leaves `{}` behind",
        file.display()
    );
}

/// Nothing served, so nothing published: a requested confinement that cannot be honoured must
/// not leave a `state` file, or the refusal would be indistinguishable from a run.
#[then("the root publishes no state file")]
async fn root_publishes_no_state(world: &mut World) {
    let file = state_file(cli(world).root());
    assert!(
        !file.exists(),
        "nothing served, so nothing published `{}`",
        file.display()
    );
}

/// A graceful stop removes `state` — waited for rather than raced, because `stop` returns the
/// moment the control channel stops answering, milliseconds before the plane removes the file.
#[then(expr = "the root's state file is gone within {int} milliseconds")]
async fn state_file_gone(world: &mut World, milliseconds: u64) {
    let file = state_file(cli(world).root());
    let deadline = Instant::now() + Duration::from_millis(milliseconds);
    while file.exists() && Instant::now() < deadline {
        sleep(POLL).await;
    }
    assert!(
        !file.exists(),
        "a graceful stop removes `{}`",
        file.display()
    );
}

/// The create-on-missing arm writes `<root>/.tinio/.tinio.toml` — with a generated credential
/// pair in it — and it is the *server's* first start and nothing else's (`contracts/cli.md:45`).
/// A command asked to report on a root must leave that root exactly as it found it.
#[then("the root holds no state directory")]
async fn root_holds_no_state_dir(world: &mut World) {
    let dir = cli(world).root().join(".tinio");
    assert!(
        !dir.exists(),
        "reporting a root never creates `{}` — nor the credential-bearing config inside it",
        dir.display()
    );
}

/// The count half of the same rule: the root holds exactly what it held before, so no command
/// added a file under a name this suite does not know to look for.
#[then("the root holds exactly what it held before")]
async fn root_unchanged(world: &mut World) {
    let cli = cli(world);
    let root = cli.root().to_path_buf();
    let now = fs::read_dir(&root).expect("read the root").count();
    assert_eq!(
        now,
        cli.entries,
        "the root holds exactly what it held before ({})",
        root.display()
    );
}

/// The channel a scenario asked a confinement through, and the spelling a refusal must carry.
#[derive(Clone, Copy, Debug)]
enum SandboxChannel {
    /// `--sandbox`: the flag, which only a build carrying the `sandbox` feature has at all.
    Flag,
    /// `TINIO_SANDBOX`: the variable, which cannot be compiled away.
    Env,
}

impl SandboxChannel {
    /// The channel a step's own spelling names — the feature's two, and only these.
    fn parse(spelling: &str) -> Self {
        match spelling {
            "--sandbox" => Self::Flag,
            "TINIO_SANDBOX" => Self::Env,
            other => {
                panic!("a confinement channel is `--sandbox` or `TINIO_SANDBOX`, not {other:?}")
            }
        }
    }

    /// The spelling the operator used, which every refusal the crate raises carries.
    fn spelling(self) -> &'static str {
        match self {
            Self::Flag => "--sandbox",
            Self::Env => "TINIO_SANDBOX",
        }
    }
}

/// Which arm a requested confinement must take on this host — **predicted from the two facts a
/// scenario cannot change**, so a run that took another one is a failure rather than an accepted
/// alternative.
#[derive(Clone, Copy, Debug)]
enum Arm {
    /// No `sandbox` feature in that binary, so nothing could be confined: clap's usage error for
    /// the flag (not in that parser at all) and this crate's for the variable, naming the build.
    BuildRefused,
    /// Feature in, and this target's backend is still the stub (macOS): refused, naming the
    /// platform.
    PlatformRefused,
    /// Feature in and this target's backend confines (Linux, Windows): honoured, the run serves.
    Applied,
}

/// The arm owed here. [`Cli::sandbox_flag`] is the probe step's answer, which is the feature's
/// own presence in the parsed binary; every scenario that runs the assertion probes first, and
/// that is what makes the arm a prediction rather than whichever one happened.
fn owed_arm(cli: &Cli) -> Arm {
    if !cli
        .sandbox_flag
        .expect("the scenario probes `--sandbox` before it asserts the arm")
    {
        Arm::BuildRefused
    } else if cfg!(target_os = "macos") {
        // The one stub (`tinio_cli::sandbox::BACKEND_IS_STUB`: Linux P1 and Windows P2 are
        // wired), and the spawned binary is built for this test binary's own target.
        Arm::PlatformRefused
    } else {
        Arm::Applied
    }
}

/// **The invariant every sandbox channel is held to, on every build: a requested confinement is
/// never silently ignored.**
///
/// Where nothing can honour the channel the run refuses, names the channel that asked for it, and
/// — because the two refusals are a matched pair, written to be told apart — names the *build*
/// where `--sandbox` is not in this parser and the *platform* whose backend cannot honour it, and
/// never the other one. Where this target's backend is wired the request is honoured instead, and
/// the assertion is the readiness event **plus the absence of every refusal's words**: "confined"
/// is the platform legs' subject (`sandbox_e2e*`), not something a reader of stdout can certify.
///
/// **The arm is dispatched, not accepted either way.** [`owed_arm`] says which one this host owes
/// and the run is held to that arm's own words, so the spelling this replaces — "either refused
/// with the channel named or applied" — can no longer pass on a run that failed for some other
/// reason and merely mentioned `sandbox`, nor on an honour arm that never reached readiness.
#[then(expr = "the {string} request is answered, never ignored")]
async fn request_answered(world: &mut World, channel: String) {
    let channel = SandboxChannel::parse(&channel);
    let cli = cli(world);
    let run = cli.last();
    match owed_arm(cli) {
        Arm::BuildRefused => refused_by_build(run, channel),
        Arm::PlatformRefused => refused_by_platform(run, channel),
        Arm::Applied => applied(run),
    }
}

/// Every refusal, whichever arm raised it: the channel's own spelling, and no service.
fn refused(run: &Run, channel: SandboxChannel) {
    let status = run
        .status
        .expect("a refused run has exited — a run still serving was not refused");
    assert!(
        !status.success(),
        "a requested confinement is refused, never served unconfined:\nstdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr
    );
    assert!(
        run.readiness.is_none(),
        "a refusal that then served is not a refusal:\nstdout:\n{}",
        run.stdout
    );
    assert!(
        run.stderr.contains(channel.spelling()),
        "the refusal names the channel that asked for it (`{}`):\n{}",
        channel.spelling(),
        run.stderr
    );
}

/// The build's refusal: the feature is not in that binary, so the run cannot confine and says so
/// rather than serving unconfined. The flag is not in that parser at all — clap answers it, in
/// clap's words — while the variable, which cannot be compiled away, is refused by the crate
/// itself: a usage error (2) naming the **build**, never the platform, which is the other arm's
/// half of the pair.
fn refused_by_build(run: &Run, channel: SandboxChannel) {
    refused(run, channel);
    assert_eq!(
        run.status.and_then(|status| status.code()),
        Some(2),
        "a capability this build does not carry is a usage error:\n{}",
        run.stderr
    );
    match channel {
        SandboxChannel::Flag => assert!(
            run.stderr.contains("unexpected argument"),
            "with no `sandbox` feature the flag is not in the parser, so clap is what refuses \
             it:\n{}",
            run.stderr
        ),
        SandboxChannel::Env => {
            assert!(
                run.stderr.contains("no `sandbox` feature"),
                "the variable cannot be compiled away, so the crate names the build:\n{}",
                run.stderr
            );
            assert!(
                !run.stderr.contains("platform"),
                "and not the platform — this is the build's half of the pair:\n{}",
                run.stderr
            );
        }
    }
}

/// The platform's refusal: the feature is in and this target's backend is the stub. A startup
/// error (1) naming `std::env::consts::OS`, and never the build — telling the two apart is what
/// the pair exists for.
fn refused_by_platform(run: &Run, channel: SandboxChannel) {
    refused(run, channel);
    assert_eq!(
        run.status.and_then(|status| status.code()),
        Some(1),
        "a stub backend is a startup error:\n{}",
        run.stderr
    );
    assert!(
        run.stderr.contains("platform"),
        "the refusal names the platform whose backend cannot honour it:\n{}",
        run.stderr
    );
    assert!(
        !run.stderr.contains("build"),
        "and not the build — the feature is compiled in:\n{}",
        run.stderr
    );
}

/// The honour arm: the request reached a run that serves, and **no refusal's words are anywhere
/// in its stream** — without that absence a run that refused and somehow still reported readiness
/// would pass as the honoured one.
fn applied(run: &Run) {
    assert!(
        run.readiness.is_some(),
        "an honoured confinement is a run that serves: nothing was refused and no readiness \
         event arrived\nstdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr
    );
    assert!(
        run.status.is_none(),
        "the run that serves is the one the scenario drives, not an exit: {:?}\nstderr:\n{}",
        run.status,
        run.stderr
    );
    for refusal in ["asked for a confinement", "unexpected argument"] {
        assert!(
            !run.stderr.contains(refusal),
            "an honoured confinement carries no refusal ({refusal:?}):\n{}",
            run.stderr
        );
    }
}

/// The publication half, per shape: a refusal leaves no `state` file, and a run that serves has
/// one — so the file tracks *serving* rather than the channel, and a refusal stays
/// distinguishable from a run.
#[then("the root publishes a state file only when the run is serving")]
async fn state_file_tracks_serving(world: &mut World) {
    let cli = cli(world);
    let file = state_file(cli.root());
    let published = file.exists();
    let serving = cli.last().readiness.is_some();
    assert_eq!(
        published,
        serving,
        "`state` is the serving process's record: published={published}, serving={serving}\n\
         stderr:\n{}",
        cli.last().stderr
    );
}

/// A datagram of this tag arrived — bounded by [`NOTIFY_DEADLINE`], which is generous on
/// purpose: the subject is the sequence, not the latency.
#[cfg(unix)]
#[then(expr = "systemd received {string}")]
async fn systemd_received(world: &mut World, tag: String) {
    systemd_received_within(world, tag, 1, NOTIFY_DEADLINE).await;
}

/// The same within the interval systemd's own configuration asks for.
#[cfg(unix)]
#[then(expr = "systemd received {string} within {int} milliseconds")]
async fn systemd_received_in(world: &mut World, tag: String, milliseconds: u64) {
    systemd_received_within(world, tag, 1, Duration::from_millis(milliseconds)).await;
}

/// **A second beat is the assertion that has content.** `tokio::time::interval`'s first tick is
/// immediate, so the first datagram would arrive even if the task only ever sent once — it is
/// the repeat that shows the cadence came from `WATCHDOG_USEC` rather than from the spawn.
#[cfg(unix)]
#[then(expr = "systemd received a second {string} within {int} milliseconds")]
async fn systemd_received_again(world: &mut World, tag: String, milliseconds: u64) {
    systemd_received_within(world, tag, 2, Duration::from_millis(milliseconds)).await;
}

#[cfg(unix)]
async fn systemd_received_within(world: &mut World, tag: String, count: usize, within: Duration) {
    let notify = cli(world)
        .notify
        .as_mut()
        .expect("the scenario bound a notify socket");
    let arrived = notify.receive(&tag, count, within).await;
    assert!(
        arrived >= count,
        "{count} `{tag}` datagram(s) within {within:?}, saw {arrived} of: {:?}",
        notify.seen
    );
}
