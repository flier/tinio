//! CLI step definitions (T055): the scenarios that spawn the real `tinio` binary.
//!
//! These are the process-level legs `crates/tinio/tests/lifecycle.rs` used to hold. What stayed
//! there is the console-handler leg, which needs `GenerateConsoleCtrlEvent` under
//! `#[cfg(windows)]` and one of the workspace's two console-related `#[expect(unsafe_code)]` sites
//! (the workspace enumerates its exception sites one by one rather than counting them — design
//! decision 20); its module doc says so, and `docs/tests.md`'s "CLI (tinio)" section carries the
//! leg-by-leg mapping.
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

// The Windows-only import, for the low-integrity step below: `OsStr` is the type
// `Command::get_envs` answers in.
#[cfg(target_os = "windows")]
use std::ffi::OsStr;
// The Linux arms' own import: [`outside_probe`] answers with `io::Result`, and the module's other
// `io` name is the `ErrorKind` the shared import below brings in on every platform.
#[cfg(target_os = "linux")]
use std::io;
#[cfg(unix)]
use std::mem::take;
#[cfg(unix)]
use std::os::unix::net::UnixDatagram;
use std::{
    env,
    ffi::OsString,
    fs,
    io::ErrorKind,
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant, SystemTime},
};

use assert_cmd::cargo::cargo_bin;
use cucumber::{gherkin::Step, given, then, when};
use http::Method;
#[cfg(target_os = "linux")]
use rustix::process::{Pid, Signal, kill_process};
use secrecy::ExposeSecret as _;
use tempfile::TempDir;
use tinio_api::{
    client::{Call, RawResponse, dialable, get_http, get_local},
    router::Auth,
    state::{STATE_FILE, State},
};
use tinio_sandbox::Event;
#[cfg(target_os = "windows")]
use tinio_sandbox::windows::testing;
use tinio_util::testing::{frame_line, job, kill_and_reap, poll_exit, wait_for_path};
#[cfg(windows)]
use tokio::net::windows::named_pipe::ClientOptions;
#[cfg(windows)]
use tokio::task::spawn_blocking;
use tokio::time::sleep;

#[cfg(target_os = "macos")]
use super::common::sweep_confined_grandchild;
use super::{World, common::ACL_ROOT};
// The configuration root's own schema and the file name discovery looks for
// (`steps/clients.rs` reaches the same pair through `crate::_cli::config`): the config-channel
// fixture is written through the types the run reads it back with, so a renamed key is a compile
// error here rather than a section a run silently takes as unset.
#[cfg(feature = "pg")]
use crate::_cli::config::storage;
#[cfg(feature = "pg")]
use crate::_pg::_store_pg::test_support::TestDb;
#[cfg(feature = "pg")]
use crate::_server::identity::auth;
use crate::{
    _cli::config::{Config, discover, server},
    _util::env::{READ_ONLY_VAR, SANDBOX_VAR, STATE_DIR_VAR},
};

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

/// How long one attempt at a run's reported endpoint may take before it is retried — the
/// connect's own bound, never the step's deadline.
///
/// **Sized from the host's own refusal latency, measured rather than guessed.** A connect to a
/// *closed* loopback port is refused **after ~2.0 s** on this Windows machine (python and
/// PowerShell alike, two never-bound ports, 2.017 s and 2.054 s, 2026-09-30), so a bound tighter
/// than that answers `TimedOut` for a port that is already gone — the first draft's 20 ms
/// ([`POLL`]) and its 500 ms successor both did, and the second read that timeout as "gone"
/// until the "only a refusal is the finding" rule below was honoured. The bound costs a passing
/// run 2 s and nothing else; [`ENDPOINT_GONE_WINDOW`] is the step's own clock.
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the endpoint a run reported has to stop accepting once the run has ended.
///
/// **An ordering rather than a latency**: the child's listener is meant to go with the run, so
/// the assertion is about *whether* it is gone, not how fast — and the window is two of
/// [`PROBE_CONNECT_TIMEOUT`]'s attempts, so a host that loses the first one still answers inside
/// it. The cost is asymmetric in the right direction: a gone endpoint returns at the first
/// refusal (~2 s on the measured host), and only a run that left something listening waits the
/// window out.
const ENDPOINT_GONE_WINDOW: Duration = Duration::from_secs(10);

/// How long a **serving** run's `state` file is given to appear after its readiness frame.
///
/// **The two artifacts are written by different processes on macOS, and the frame can win the
/// race.** The confined child emits `listening`, the launcher relays that frame to this harness's
/// stdout and only then writes `state` (`crates/tinio-cli/src/sandbox/confine.rs`'s `publish_state`
/// waits for the child's own terminal event), so a reading taken the instant the frame arrives can
/// find the file not yet there — measured on macOS 15.8, 2026-09-25, once in two runs, while the
/// second run showed the file already present. The window is the launcher's few syscalls, so the
/// bound is far above it and still fails fast rather than hiding a run that never publishes;
/// `crates/tinio-server/tests/sandbox_macos_e2e.rs` polls for the same artifact for the same
/// reason. The refusal half needs no allowance at all: a run that refused has already exited.
const PUBLISH_SETTLE: Duration = Duration::from_secs(2);

/// One poll's wait: a run's exit and its readiness event are both watched by polling, and no
/// scenario asserts on the poll's own cadence.
const POLL: Duration = Duration::from_millis(20);

/// The dotenv file `resolve::apply_env_file` loads: `<state dir>/.env`
/// (`tinio-cli/src/config/sources.rs`, which spells the name once).
const ENV_FILE: &str = ".env";

/// The credential pair's two variables, `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` — the
/// **environment-only** arguments `resolve::fold_credentials` folds onto `[auth]`
/// (`crates/tinio-cli/src/cli.rs` owns the two names and `contracts/config.md` lists them beside
/// `TINIO_STATE_DIR`). Spelled here for the one fixture that needs them
/// ([`harness_credentials`]): a signed journey against the spawned binary, whose served pair a
/// first start otherwise generates at random.
const ACCESS_KEY_VAR: &str = "TINIO_ACCESS_KEY";
const SECRET_KEY_VAR: &str = "TINIO_SECRET_KEY";

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
    pub(super) state: PathBuf,
    /// The path a missing-root scenario hands the run, and the one a run must never create —
    /// a sibling of the served root, so nothing else in the scenario's fixtures is near it
    /// ([`Cli::missing`]).
    missing: PathBuf,
    /// The variables the scenario asks for, applied to every child it spawns.
    vars: Vec<(String, String)>,
    /// The entries the root held when the scenario claimed it: what "exactly as it found it" is
    /// measured against.
    entries: usize,
    /// The served root's entries at the moment `the served root's entries are recorded` ran — the
    /// names, lengths and mtimes a later write would move. Recorded rather than counted
    /// ([`Cli::entries`]) because a run can write a tree without adding an entry to it, which is
    /// exactly what a state database under the root is.
    recorded: Option<Vec<RootEntry>>,
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
    /// Whether a run in this scenario actually asked for a confinement — the one state the macOS
    /// sweep in [`Cli::kill_servers`] is gated on, because only such a run can leave the
    /// grandchild that sweep exists to reach. Every other scenario would pay two process-table
    /// scans for a process it never started.
    confined_spawned: bool,
    /// Whether a step planted one of the confinement's **file** channels — the configuration key
    /// or the `.env` spelling.
    ///
    /// Nothing about a spawn says so: the ask is a file the run will read, not an argument or a
    /// variable, so the step that writes it is the only thing that knows. Kept for the *budget*
    /// alone ([`Cli::confined`]) — a run asked for through a file is confined the same way and
    /// owes the same timeout the launcher's grant needs, which is why the flag rides on the
    /// harness rather than being re-derived from the file's contents here.
    fixture_asked: bool,
    /// The endpoint the last readiness event reported, kept across runs so a later `status` run
    /// can be held to the address the server actually bound.
    endpoint: Option<String>,
    /// The last run; every run replaces it.
    last: Option<Run>,
    /// How many runs this scenario has made: each writes its own two stream files.
    runs: usize,
    /// The exposition the `I read the metrics from the control channel` step fetched — the
    /// management plane's own text, kept so the assertion step beside it reads the same bytes the
    /// scrape did rather than fetching a second sample.
    metrics: Option<String>,
    /// Whether the children are handed [`Cli::state`] as `TINIO_STATE_DIR` at all.
    ///
    /// **The variable is the channel that decides `StateDirSource`** — `Named` when it is there,
    /// `Derived` when the resolution has to derive `<home>/.tinio/roots/<hash>/` instead
    /// (`resolve::resolve_state_dir`) — so the one scenario about the derived directory has to
    /// take the variable away rather than point it somewhere else. `true` for every other
    /// scenario: the harness names the state dir so no child reaches a real home, and a scenario
    /// that does not care about the channel should not have to say so.
    state_dir_named: bool,
    /// The directories a step left **unwritable**, with the mode they had, for
    /// [`Cli::restore_modes`] to give back.
    ///
    /// A directory that refuses new entries refuses `remove_dir_all` too, so the scratch tree
    /// would outlive the scenario that made it unwritable. Unix only: the plant is a permission
    /// bit, and the platform without one has no such fixture.
    #[cfg(unix)]
    unwritable: Vec<(PathBuf, u32)>,
    /// systemd's end of the notify protocol, when the scenario bound one (unix only: off Linux
    /// there is no protocol to speak — `sd-notify` cannot even link on Windows).
    #[cfg(unix)]
    notify: Option<Notify>,
    /// The handles a fixture opened and must keep open for the rest of the scenario.
    ///
    /// **One call site, and the handle *is* the fixture**
    /// ([`doctor_leftover_refuses_removal`]): the doctor's failed-repair induction holds the
    /// stale temp file open with no sharing, and Windows refuses to unlink a file opened that
    /// way. Dropping the handle early would put the repair's stage back on the success path and
    /// the scenario would assert nothing — which is why it is parked here rather than left to a
    /// local, and why [`Cli::teardown`] does not need to touch it (the drop at the end of the
    /// scenario closes it).
    #[cfg(windows)]
    pub(super) held: Vec<fs::File>,
    /// The scratch PostgreSQL database a `@pg` scenario created — the handle
    /// [`Cli::drop_pg_scratch`] needs to remove it again.
    ///
    /// **One per scenario, dropped on every path out**: the `#[after]` hook calls
    /// [`Cli::drop_pg_scratch`] after [`Cli::teardown`] has ended the servers, so a failing
    /// assertion cannot leave a database holding PG12's process-lifetime ownership lock (which
    /// would starve the next scenario's connect, and the shared `tinio` fixture belongs to every
    /// other leg). `None` for every scenario that created none.
    #[cfg(feature = "pg")]
    pg_scratch: Option<TestDb>,
}

/// One run of the binary, as the operator would have seen it: the exit status (absent while it
/// still serves), both streams, and the timings the scenarios assert.
#[derive(Debug)]
pub(super) struct Run {
    /// `None` for a run that is still serving when the poll loop ends.
    status: Option<ExitStatus>,
    /// Everything the run wrote to stdout.
    pub(super) stdout: String,
    /// The file this run's stderr was redirected to ([`Cli::run_paths`]) — what [`Run::stderr`]
    /// re-reads at assertion time.
    stderr_path: PathBuf,
    /// Everything the run had written to stderr when the poll loop ended.
    ///
    /// **Not what an assertion reads**, because a run kept at its readiness frame goes on writing
    /// (the OTLP warning lands after the `state` publish, milliseconds after the frame) — see
    /// [`Run::stderr`], of which this is only the unreadable-file fallback.
    stderr_snapshot: String,
    /// The endpoint the run reported as its `listening` event, when it got that far.
    readiness: Option<SocketAddr>,
    /// Spawn → readiness event, for the runs that reported one.
    ready_in: Option<Duration>,
    /// Spawn → exit, or spawn → readiness for a run that never exited.
    took: Duration,
    /// The pid this harness spawned — the process itself, which is what an in-place confinement
    /// confines and what a launcher publishes `state` as. Kept on the run rather than read off
    /// [`Cli::children`], because a step about *this* run's identity must not have to know which
    /// of the scenario's runs is still serving.
    spawned: u32,
    /// Whether this run asked for read-only mode — which is where its `state` file is, not how it
    /// serves (FR-023 relocates the state directory, `resolve::resolve_state_dir`).
    read_only: bool,
}

impl Run {
    /// The run's stderr, re-read from the file it was redirected to.
    ///
    /// **Re-read rather than the snapshot [`Cli::run`] froze when its poll ended**, because a run
    /// kept at its readiness frame goes on writing: the launcher's OTLP warning is emitted after
    /// its `state` publish, milliseconds after the frame, so an assertion made on the snapshot
    /// races that write. The snapshot remains the fallback for a file that cannot be read back,
    /// so a captured stream is never lost to a failed re-read.
    pub(super) fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr_path).unwrap_or_else(|_| self.stderr_snapshot.clone())
    }
}

impl Cli {
    /// Claims a scratch directory, makes the root the scenario serves under it, and makes the
    /// relocated state directory the children are pointed at.
    ///
    /// **The state directory is created here rather than left to a run**, because a **read-only**
    /// run resolves it (`resolve::resolve_state_dir`) and the confinement then has to expose it:
    /// an exposure whose path does not exist is refused before anything is served, so a
    /// read-only scenario would fail about the fixture rather than about its subject. Nothing
    /// else writes it — a read-write run uses `<root>/.tinio` and never looks at this one.
    fn new() -> Self {
        let dir = scratch_dir();
        let root = dir.path().join("root");
        fs::create_dir_all(&root).expect("the served root");
        let entries = fs::read_dir(&root).expect("read the root").count();
        let state = dir.path().join("state");
        fs::create_dir_all(&state).expect("the relocated state directory");
        // `{missing}` — a path beside the root, never created: the leg's subject is the root a
        // run refuses to serve *and* refuses to create.
        let missing = dir.path().join("missing");
        Self {
            state,
            missing,
            dir,
            root,
            vars: Vec::new(),
            entries,
            recorded: None,
            children: Vec::new(),
            daemon: None,
            sandbox_flag: None,
            confined_spawned: false,
            fixture_asked: false,
            endpoint: None,
            last: None,
            runs: 0,
            metrics: None,
            state_dir_named: true,
            #[cfg(unix)]
            unwritable: Vec::new(),
            #[cfg(unix)]
            notify: None,
            #[cfg(windows)]
            held: Vec::new(),
            #[cfg(feature = "pg")]
            pg_scratch: None,
        }
    }

    /// The scratch directory: what a child's working directory and `HOME`/`USERPROFILE` are.
    pub(super) fn scratch(&self) -> &Path {
        self.dir.path()
    }

    /// The served root.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// The root a scenario hands a run **that must fail**, spelled `{missing}` on its command
    /// line: a path beside the served root that nothing creates, so a run that made it would be
    /// observable rather than harmless.
    ///
    /// **A sibling rather than a child of the root**: the root itself is created by [`Cli::new`],
    /// and a path under it would be a fixture inside a served tree — a run that wrote it would
    /// still be writing *inside* the root this leg is about leaving alone.
    fn missing(&self) -> &Path {
        &self.missing
    }

    /// The state directory a **read-write** run resolves: `<root>/.tinio`
    /// (`resolve::resolve_state_dir`'s first branch), and the directory the configuration file,
    /// the `.env` file and the `state` file all live in.
    ///
    /// **Not the harness's `state` field, which is one word away and answers a different
    /// question.** That one is the *relocated* directory — the candidate a read-only run resolves
    /// first, handed to every child as `TINIO_STATE_DIR` — and a read-write run never uses it, so
    /// a step that read `state` out of it would look for a file nothing writes. Every scenario
    /// these steps serve runs read-write; a **read-only** run's directory is
    /// [`Cli::state_dir_of`]'s answer, and the same rule is what makes [`state_file`] take a root.
    pub(super) fn state_dir(&self) -> PathBuf {
        self.root.join(".tinio")
    }

    /// The state directory the run itself resolved — the one its `state` file is in.
    ///
    /// FR-023's relocation is why this exists rather than a single path: a read-write run resolves
    /// `<root>/.tinio` whatever `TINIO_STATE_DIR` says, while a read-only run resolves the
    /// variable's directory (this harness always sets it, so the answer is the scenario's own
    /// `state`). Reading the wrong one finds no file and would report a serving run as
    /// unpublished, which is a failure about the harness rather than about the product.
    ///
    /// **The mode is read off the run the way [`Cli::confined`] reads the confinement**: the flag
    /// and the variable a scenario put on the child (`resolve::resolve_mode`'s two channels). A
    /// checklist that named the mode in the *configuration file* would need the resolution's own
    /// answer, and no scenario here makes a read-only run that way — the limit is stated rather
    /// than papered over, and the failure it would cause is [`published_state`]'s own loud one.
    fn state_dir_of(&self, run: &Run) -> PathBuf {
        if run.read_only {
            self.state.clone()
        } else {
            self.state_dir()
        }
    }

    /// The root as the CLI resolves it: canonical, which is the identity the state dir, the
    /// control channel's name and `state`'s own record are all derived from
    /// (`resolve::resolve_root`) — and on Windows the `\\?\` form `Path::canonicalize` answers,
    /// which is what every command prints.
    pub(super) fn canonical_root(&self) -> PathBuf {
        self.root
            .canonicalize()
            .expect("the served root exists while its scenario runs")
    }

    /// The last run's outcome; every `Then` step here reads it, and none may run before a `When`
    /// step made one.
    pub(super) fn last(&self) -> &Run {
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
    /// variable is what `dirs::home_dir()` reads. It is the one variable a scenario can **take
    /// away** rather than replace ([`Cli::state_dir_named`]): the variable is the resolution's
    /// `Named` answer, and a run that never saw it derives its directory from the home instead.
    ///
    /// **`NO_COLOR` is set here rather than by the step that greps a line.** Every stream this
    /// harness captures is a file, and the text layer styles a field's *name* whenever ANSI is on
    /// (`\x1b[3mstep\x1b[0m\x1b[2m=\x1b[0m…`), so a leg that reads a captured line for a `step="…"`
    /// event or one of its fields would match none of them. The variable is the text layer's own
    /// switch and `sandbox-shared.md` §7 owns the rule; setting it on every child — including the
    /// confined ones, which inherit the launcher's environment — is what makes the captured text
    /// data for all of them alike, rather than a property of whichever scenario happened to
    /// remember.
    fn command(&self) -> Command {
        let mut cmd = Command::new(cargo_bin("tinio"));
        for key in tinio_vars() {
            cmd.env_remove(key);
        }
        cmd.env_remove("NOTIFY_SOCKET")
            .env_remove("WATCHDOG_USEC")
            .env_remove("WATCHDOG_PID")
            .current_dir(self.scratch())
            .env("HOME", self.scratch())
            .env("USERPROFILE", self.scratch())
            .env("NO_COLOR", "1");
        // The named state directory, unless a scenario took the channel away
        // ([`Cli::state_dir_named`]): the variable is the resolution's `Named` answer, and its
        // absence is what makes a run derive one from the home.
        if self.state_dir_named {
            cmd.env(STATE_DIR_VAR, &self.state);
        }
        for (key, value) in &self.vars {
            cmd.env(key, value);
        }
        cmd
    }

    /// Gives back the modes a scenario took away ([`Cli::unwritable`]).
    ///
    /// Called from [`Cli::teardown`] and again from `Drop`, and it **empties the list** rather
    /// than walking it, so the second caller is a no-op rather than a second `set_permissions`
    /// on a directory the first one restored. The restore has to happen before `TempDir`'s own
    /// removal, which is the reason it is not left to the fixture: a directory that refuses
    /// entries refuses `remove_dir_all` too, and the scenario's scratch tree would survive it.
    #[cfg(unix)]
    fn restore_modes(&mut self) {
        use std::os::unix::fs::PermissionsExt as _;

        for (path, mode) in take(&mut self.unwritable) {
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(mode));
        }
    }

    /// The operator's command line as argv: whitespace-separated words, with the `{root}`,
    /// `{state}` and `{missing}` placeholders expanded **per word** — so a scratch path with a
    /// space in it stays one argument without the scenario needing to quote it.
    fn argv(&self, command: &str) -> Vec<String> {
        let words: Vec<String> = command
            .split_whitespace()
            .map(|word| {
                word.replace("{root}", &self.root.display().to_string())
                    .replace("{state}", &self.state.display().to_string())
                    .replace("{missing}", &self.missing.display().to_string())
            })
            .collect();
        assert_eq!(
            words.first().map(String::as_str),
            Some("tinio"),
            "a CLI command line starts with `tinio`: {command:?}"
        );
        words
    }

    /// Whether this run asks for a confinement — the flag on the command line, the variable a
    /// scenario put on the child ([`Cli::vars`]), or a file channel a step planted
    /// ([`Cli::fixture_asked`]). Only the *budget* rides on this: which assertion the run owes is
    /// the step's own dispatch.
    fn confined(&self, argv: &[String]) -> bool {
        let flagged = argv.iter().any(|arg| arg == "--sandbox");
        let variable = self
            .vars
            .iter()
            .any(|(name, value)| name == SANDBOX_VAR && !matches!(value.as_str(), "0" | "false"));
        flagged || variable || self.fixture_asked
    }

    /// Whether this run asks for read-only mode — the flag on the command line or the variable a
    /// scenario put on the child ([`Cli::vars`]). **One spelling for the two sites that need it**:
    /// `run` records the answer into the [`Run`] the `Then` steps read, and the low-integrity step
    /// must record the same one, because a run may be read-only through `TINIO_READ_ONLY` with no
    /// flag at all and a step that read argv alone would disagree with every other run about the
    /// mode it served.
    fn read_only(&self, argv: &[String]) -> bool {
        argv.iter().any(|arg| arg == "--read-only")
            || self.vars.iter().any(|(name, value)| {
                name == READ_ONLY_VAR && !matches!(value.as_str(), "0" | "false")
            })
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
    ///
    /// **The spawn goes through the job guard** (`tinio_util::testing::job::spawn`), and the
    /// `--daemon` scenario is the one that shows what it buys: the parent exits and the daemon
    /// survives it — a grandchild no handle here may ever name — while the job makes that daemon a
    /// member of this test process's job, so a scenario killed from outside (the abort or the hard
    /// kill that runs no `Drop` and no `kill_servers`) takes it too. The reaping is unchanged: the
    /// guard is what covers the paths that reach no teardown at all.
    async fn run(&mut self, command: &str) {
        let argv = self.argv(command);
        let detached = argv.iter().any(|arg| arg == "--daemon");
        let confined = self.confined(&argv);
        // The budget rides on the ask, and the ask is read once: the flag, the variable and the
        // file channel are all things this run may set, so a second evaluation could only answer
        // what the first one did.
        let budget = if confined {
            CONFINED_RUN_TIMEOUT
        } else {
            RUN_TIMEOUT
        };
        let read_only = self.read_only(&argv);
        self.confined_spawned |= confined;
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
        let mut child = job::spawn(&mut cmd).expect("spawn the tinio binary");
        let spawned = child.id();
        let deadline = started + budget;
        let mut status = None;
        let mut ready_in = None;
        // **The readiness poll reads only what it has not read.** The loop runs every [`POLL`] for
        // up to a whole budget, and a run's stream is a file that only grows: the length it had at
        // the last look is what decides whether there is anything new to decode. Without it, every
        // tick of a confined run's minutes-long window re-reads and re-parses the file, frames
        // [`readiness`] has already rejected included.
        let mut scanned = 0;
        loop {
            if let Some(exit) = child.try_wait().expect("wait on the child") {
                status = Some(exit);
                break;
            }
            if !detached {
                let grown = fs::metadata(&out_path).map_or(0, |meta| meta.len());
                if grown != scanned {
                    scanned = grown;
                    if readiness(&read(&out_path)).is_some() {
                        ready_in = Some(started.elapsed());
                        break;
                    }
                }
            }
            if Instant::now() >= deadline {
                kill_and_reap(&mut child);
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
        self.record(
            (out_path, err_path),
            status,
            ready_in,
            took,
            spawned,
            read_only,
        );
    }

    /// Records a finished run: read back the two stream files [`Cli::run_paths`] handed out and
    /// build the [`Run`] every `Then` step reads.
    ///
    /// **Both spawn paths end here** — this harness's own `Command::spawn` and the low-integrity
    /// device's `CreateProcessAsUserW` — so the stream → file → snapshot rule has one
    /// implementation. That relation is load-bearing: [`Run::stderr`] re-reads the file, and the
    /// snapshot beside it is only the unreadable-file fallback, which two hand-written literals
    /// would have to keep in step by hand.
    ///
    /// **Readiness is derived here rather than passed in**, as the poll loop derived it: the
    /// frame is a property of the run's stdout, so a caller that computed it would have to read
    /// the file this function is about to read.
    fn record(
        &mut self,
        (out_path, err_path): (PathBuf, PathBuf),
        status: Option<ExitStatus>,
        ready_in: Option<Duration>,
        took: Duration,
        spawned: u32,
        read_only: bool,
    ) {
        let stdout = read(&out_path);
        let stderr = read(&err_path);
        let address = readiness(&stdout);
        if let Some(endpoint) = address {
            self.endpoint = Some(endpoint.to_string());
        }
        self.last = Some(Run {
            status,
            stdout,
            stderr_path: err_path,
            stderr_snapshot: stderr,
            readiness: address,
            ready_in,
            took,
            spawned,
            read_only,
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
                run.status,
                run.stdout,
                run.stderr()
            );
        }
    }

    /// Waits for the process the last run spawned to exit, and records its status on the run.
    ///
    /// **`Cli::run`'s poll ends at the first of two things** — the child's exit or its readiness
    /// frame — so a run whose subject is what happens *after* it reports readiness can be
    /// captured either way. G12's launcher is the case: it relays the confined child's
    /// `listening` frame to this harness's stdout and *then* fails its `state` publish a few
    /// milliseconds later, which is a window the poll may or may not land in. Both captures are
    /// the same run, so this answers the ending for both: a run already recorded as exited is
    /// returned as it stands, and one captured while serving is waited for.
    ///
    /// **The handle waited on is the one [`Cli::run`] kept for a still-serving run**, which is
    /// what makes this a wait on *this* run rather than a poll of the process table: `children`
    /// is in spawn order, so its last element is the last run's process. Waiting reaps it, which
    /// is also why nothing is pushed back — the teardown has no handle left to kill.
    ///
    /// **Bounded by [`RUN_TIMEOUT`], through `poll_exit`** — the workspace's one bounded wait
    /// (`tinio_util::process`), whose `None` this caller answers for itself: an ending that never
    /// comes is a product finding this suite reports (with the run's two streams), never a hang it
    /// waits out. The kill lands before the report, as every failing wait here does.
    fn run_ends(&mut self) -> ExitStatus {
        if let Some(status) = self.last().status {
            return status;
        }
        let mut child = self
            .children
            .pop()
            .expect("a run captured while it was still serving keeps the process it spawned");
        let status = poll_exit(&mut child, RUN_TIMEOUT).unwrap_or_else(|| {
            kill_and_reap(&mut child);
            panic!(
                "the run did not end within {}s: it reported readiness and kept serving, so \
                 this scenario's subject — an ending — never happened, and the server was \
                 killed\nstdout:\n{}\nstderr:\n{}",
                RUN_TIMEOUT.as_secs(),
                self.last().stdout,
                self.last().stderr()
            );
        });
        // The run's own record, so every step beside this one reads the ending the way it reads
        // the ending of any other run (`the run fails`, `the run fails with exit code {int}`).
        self.last
            .as_mut()
            .expect("the scenario ran a command before its ending was waited for")
            .status = Some(status);
        status
    }

    /// Kills and reaps every server a run left serving.
    ///
    /// A killed server leaves its `state` file behind — no drain, no removal — which is exactly
    /// what the stale-state scenario needs.
    ///
    /// **On macOS the kill reaches the launcher and not the server, so the child is reaped by
    /// name.** Seatbelt has no job object — a `SIGKILL`ed launcher leaves its confined child
    /// running, an accepted gap the launcher's own module records
    /// (`crates/tinio-cli/src/sandbox/macos.rs`) — and this harness holds no handle to that
    /// grandchild. The sweep is [`sweep_confined_grandchild`]'s, which owns the pattern (the
    /// scenario's own root, unique to it) and the precondition below; every other platform needs
    /// no such step, because the process this kill ends *is* the server.
    fn kill_servers(&mut self) {
        for mut child in self.children.drain(..) {
            kill_and_reap(&mut child);
        }
        // **The gate is the precondition the sweep's doc states**: only a run that asked for a
        // confinement can have left the grandchild, and the sweep is a `pkill -f` over the whole
        // process table.
        #[cfg(target_os = "macos")]
        if self.confined_spawned {
            sweep_confined_grandchild(&self.root);
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
    /// — leaving an unbounded wait on a server rather than on a parse. `--help` is
    /// reached only once every argument before it parsed, and it exits without serving anything
    /// (no state dir, no `state` file). The discriminator stays clap's own words: the help rides
    /// stderr in this two-pass CLI, so the phrase searched for is the *error's*, not the help's.
    ///
    /// **The probe spawns through the job guard like every other site in this harness.** It used
    /// to reach the binary through `Command::output()`, the same spawn-and-wait without the guard:
    /// `output()` spawns as surely as `spawn()` does, and a suite that reaches the binary through
    /// it never holds the handle the job membership is about. `job::output` is the guard's own
    /// spelling of that recipe, so the child is a member of the test process's job and the three
    /// stdio modes are the helper's rather than three lines written here (a `--help` probe exits
    /// on its own, so what the guard buys here is the property rather than the reaping).
    fn knows_flag(&self, flag: &str) -> bool {
        let mut cmd = self.command();
        cmd.arg("server")
            .arg(&self.root)
            .args(["--port", "0", flag, "--help"]);
        let out = job::output(&mut cmd).expect("probe the flag");
        !String::from_utf8_lossy(&out.stderr).contains("error: unexpected argument")
    }

    /// Ends everything the scenario started.
    ///
    /// The `#[after]` hook calls this — hence `pub`, since the hook lives in the parent module
    /// — so no path out of a scenario, a failed assertion included, leaves a server holding a
    /// temporary root. The sticky publish-failure fixture goes with it
    /// ([`remove_sticky_state_tmp`]): the plant defeats the product's own removal, so this is what
    /// reaches it.
    pub fn teardown(&mut self) {
        self.kill_servers();
        if let Some(root) = self.daemon.take() {
            self.stop_daemon(&root);
        }
        remove_sticky_state_tmp(self);
        // Before `TempDir` drops: the scratch tree cannot be removed while a directory inside
        // it still refuses entries (see [`Cli::unwritable`]).
        #[cfg(unix)]
        self.restore_modes();
    }

    /// Drop the scratch PostgreSQL database this scenario created — the `#[after]` hook's second
    /// teardown step, after [`Cli::teardown`] has ended the servers that held it.
    ///
    /// **A scratch database, never the shared `tinio` one** (PG8/PG12): PG12's writable-ownership
    /// lock is process-lifetime, so a run that took it against the shared fixture would starve
    /// every other leg of the pass — and the pass's own backend-neutral scenarios reset that
    /// fixture for themselves. The store harness terminates lingering sessions and then drops;
    /// the servers are already gone ([`Cli::teardown`] runs first).
    ///
    /// **Best-effort by design**: a scenario that failed before it created the database has
    /// nothing to take, and a drop that cannot reach the server is not a finding this suite
    /// reports — the name is unique per process and nanosecond, so a leak cannot collide with
    /// anything.
    #[cfg(feature = "pg")]
    pub(super) async fn drop_pg_scratch(&mut self) {
        let Some(scratch) = self.pg_scratch.take() else {
            return;
        };
        scratch.drop_db().await;
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
        if let Ok(mut child) = job::spawn(&mut cmd) {
            let deadline = Instant::now() + TEARDOWN_TIMEOUT;
            while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                thread::sleep(POLL);
            }
            kill_and_reap(&mut child);
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
        #[cfg(unix)]
        self.restore_modes();
    }
}

/// The scenario's scratch directory: `$TMPDIR` everywhere except macOS, where it is `/tmp`.
///
/// **macOS is short for `sun_path`'s sake, not for tidiness** (`docs/tests.md`,
/// "Platform-gated verification (macOS)"). A read-write run's control socket is
/// `<root>/.tinio/tinio-<16 hex>.sock`, `sun_path` is **104 bytes**, and `$TMPDIR` there is
/// `/private/var/folders/<2 chars>/<30 chars>/T/` — 57 bytes before `tempfile`'s own ten, so the
/// socket name pushes past the limit and the server refuses to start. `/tmp` is the platform's
/// short scratch area (canonical `/private/tmp`, 12 bytes), and the root under it measures ~62
/// bytes for the same socket. `TINIO_STATE_DIR` is not the remedy: a read-write run's state
/// directory is `<root>/.tinio` whatever that variable says (`resolve::resolve_state_dir`), and
/// these scenarios serve read-write.
fn scratch_dir() -> TempDir {
    #[cfg(target_os = "macos")]
    {
        TempDir::new_in("/tmp").expect("a scratch directory")
    }
    #[cfg(not(target_os = "macos"))]
    {
        TempDir::new().expect("a scratch directory")
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
pub(super) fn cli(world: &mut World) -> &mut Cli {
    world.cli.as_mut().expect("the scenario has a storage root")
}

/// The same harness, **claiming one when the scenario has not claimed it yet** — the body of
/// `Given a storage root`, shared with the fixtures whose own `Given` must also have a root (the
/// doctor's, which plant their whole fixture in one step). One definition, so a second claim can
/// only ever be the first one.
pub(super) fn harness(world: &mut World) -> &mut Cli {
    if world.cli.is_none() {
        world.cli = Some(Cli::new());
    }
    cli(world)
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

/// Every frame `text` carries, decoded, in stream order.
///
/// **A frame is not a line** (decision 19): the framing is an `event:` line, a `data:` line and a
/// blank line, so this accumulates until the blank line and decodes the block — the rule
/// [`frame_line`] owns, and the same one `steps/clients.rs`'s readiness reader is written on. It
/// is also the reason [`readiness`] can take the whole stream: the runs it serves write one frame.
///
/// **A block that does not decode is not a frame and is skipped**, which is the property the
/// framing buys: a stray line the child or a dependency wrote cannot count as an event. A caller
/// asserting a *count* therefore asserts the events the protocol defines and not the lines the
/// file happens to hold.
///
/// The trailing overflow is deliberate rather than unreachable: a stream that ended without its
/// closing blank line still carries the frame it was writing, and dropping it would let a run
/// that died mid-frame read as a run that wrote nothing.
fn frames(text: &str) -> Vec<Event> {
    let mut frames = Vec::new();
    let mut frame = String::new();
    for line in text.lines() {
        if let Some(event) = frame_line(&mut frame, line, Event::decode) {
            frames.push(event);
        }
    }
    if let Some(event) = Event::decode(&frame) {
        frames.push(event);
    }
    frames
}

/// The `state` file the management plane publishes for `root` (`api::state::path`: the reserved
/// directory's own file, whose name `tinio_api::state::STATE_FILE` owns).
fn state_file(root: &Path) -> PathBuf {
    root.join(".tinio").join(STATE_FILE)
}

/// Takes away the sticky fixture G12 plants — the failed-publish scenario's induction: a
/// **directory** at `<root>/.tinio/state.tmp`, the path `State::write` clears with a tolerant
/// `remove_file` before it creates its own temp file.
///
/// **G12 plants it on every Windows and macOS run** — the scenario writes exactly that directory —
/// and this teardown is what removes it: a live call site ([`Cli::teardown`]) whose `NotFound` arm
/// covers every other scenario's path.
///
/// **A directory is what makes the plant sticky.** `remove_file` cannot take one, so the publish
/// fails — the induction — and the plant is still there afterwards: the product's own removal is
/// the very thing the plant defeats, so only the scenario that put it there can take it away.
///
/// `<root>/.tinio` is the state directory a **read-write** run resolves
/// (`resolve::resolve_state_dir`) and the one a launcher publishes into; the relocated
/// `TINIO_STATE_DIR` this harness hands every child is a read-only run's first candidate, never
/// this path. [`Cli::teardown`] calls this on every path out, so the plant cannot outlive its
/// scenario, and a scenario that needs the state directory clean *between* two runs calls the same
/// function — one home for the fixture, so no step spells its own `fs::remove_dir_all`.
fn remove_sticky_state_tmp(cli: &Cli) {
    let path = cli.root().join(".tinio").join("state.tmp");
    // Only what is there is removed, and both shapes count: the plant is a directory, while a run
    // killed in the window between the temp create and the rename leaves the file.
    let removed = if path.is_dir() {
        fs::remove_dir_all(&path)
    } else {
        fs::remove_file(&path)
    };
    match removed {
        Ok(()) => {}
        // Nothing planted: every scenario but the publish-failure one.
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => panic!(
            "the sticky fixture at `{}` is not removable: {err}",
            path.display()
        ),
    }
}

/// A file's text, or nothing when it has not been written yet — a run that has not spoken is
/// not an error here, it is a poll that found nothing.
fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// The `TINIO_*` variables this process's environment holds, enumerated once.
///
/// **Every child is stripped of them** ([`Cli::command`]), and the enumeration walks the whole
/// process environment — a spawn happens several times per scenario, so it is read once per test
/// process instead of once per spawn. Nothing here mutates this process's environment (Global
/// Constraints), which is what makes the list a constant for the run.
fn tinio_vars() -> &'static [OsString] {
    static VARS: OnceLock<Vec<OsString>> = OnceLock::new();
    VARS.get_or_init(|| {
        env::vars_os()
            .map(|(key, _)| key)
            .filter(|key| key.to_string_lossy().starts_with("TINIO_"))
            .collect()
    })
}

/// The environment a child of [`Cli::command`] is given, as the `name=value` block a spawn that
/// is not a `std::process::Command` needs.
///
/// **The rule is [`Cli::command`]'s, read back rather than restated.** `Command::get_envs`
/// answers exactly the modifications that method made — a key with no value is an `env_remove`,
/// a key with one is an `env` — so this starts from this process's own environment, drops the
/// removals and overlays the sets. [`tinio_vars`] therefore has one reader, and a child spawned
/// through `CreateProcessAsUserW` is scrubbed by the same definition as every other child.
///
/// **Windows-gated because its one caller is.** `CreateProcessAsUserW` is the only spawn in this
/// harness that cannot take a `std::process::Command`, so the step below is the only caller and
/// it exists on Windows alone; ungated, this function is dead code on every other target and the
/// ubuntu and macOS `-D warnings` gates reject it. The gate is the caller's own `cfg`, so the
/// two cannot drift apart silently.
#[cfg(target_os = "windows")]
fn child_environment(cli: &Cli) -> Vec<(OsString, OsString)> {
    let command = cli.command();
    let overrides: Vec<(&OsStr, Option<&OsStr>)> = command.get_envs().collect();
    let mut environment: Vec<(OsString, OsString)> = env::vars_os()
        .filter(|(key, _)| {
            !overrides
                .iter()
                .any(|(name, value)| value.is_none() && name.eq_ignore_ascii_case(key))
        })
        .collect();
    for (name, value) in overrides {
        if let Some(value) = value {
            if let Some(entry) = environment
                .iter_mut()
                .find(|entry| entry.0.eq_ignore_ascii_case(name))
            {
                entry.1 = value.to_os_string();
            } else {
                environment.push((name.to_os_string(), value.to_os_string()));
            }
        }
    }
    environment
}

/// A step's docstring (`"""`-delimited in the feature), trimmed of the newline the block adds.
///
/// `pub(super)` for the modules that spell a body as a docstring — the lifecycle steps' XML
/// document — so the trimming rule has one home rather than a copy per module.
pub(super) fn docstring(step: &Step) -> String {
    step.docstring()
        .map(|text| text.trim_matches('\n'))
        .unwrap_or_default()
        .to_string()
}

/// The non-empty lines of a step's docstring — one entry per line the scenario wrote.
fn docstring_lines(step: &Step) -> Vec<String> {
    docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect()
}

/// The one line of `text` carrying the `step="<step>"` event named by `step`, or `None` when the
/// process reported no such event.
///
/// **The event's own `step` field is the contract and the line's shape is not** — the rule
/// `crates/tinio-server/tests/support/windows_e2e_harness.rs` states for the launcher's log, and
/// the same one here: the timestamp, the level, the module target and the field order belong to
/// whatever subscriber the process installed, so a leg matching a prefix of the whole line would
/// pin the wrong thing. The `step` name is the only part a caller may depend on.
fn step_line(text: &str, step: &str) -> Option<String> {
    step_lines(text, step).into_iter().next()
}

/// **Every** line of `text` carrying the `step="<step>"` event named by `step`, in stream order.
///
/// One step name is not one line: Windows' `grant the ACLs` is the launcher's banner *and*, past
/// the 15 s threshold, the grant phase's own warning (`tinio-sandbox-windows/src/backend/
/// grant.rs`), emitted under the same step because they are the same operation's two reports. A
/// caller that needs a specific one of them has to say which fields it is looking for —
/// `stderr_carries_step_line` does that below.
fn step_lines(text: &str, step: &str) -> Vec<String> {
    let needle = format!("step=\"{step}\"");
    text.lines()
        .filter(|line| line.contains(&needle))
        .map(str::to_owned)
        .collect()
}

/// The one line of `text` carrying the `step="<step>"` event named by `step` **and** every field
/// named in `fields`, or a panic saying what was searched and what the record holds instead.
///
/// **The line is the first one carrying every field, and not the first one carrying the step.**
/// One step name is emitted more than once with different fields — Windows' `grant the ACLs` is
/// the launcher's banner, the per-ACE `debug!` lines and, past the 15 s threshold, the grant
/// phase's own `warn!` — so a first-match reader takes whichever of them the subscriber wrote
/// first and asserts the fields that line happens to carry. The fields are what select, so a
/// second event under an existing step name cannot take a witness over silently.
///
/// **Two callers and one rule**: [`witness_record`] asks it for a platform's confinement table,
/// and the `its stderr carries a … step line naming` step for the fields a scenario's docstring
/// names — so a scenario reads the same failure sentence either way.
fn step_line_with_fields(text: &str, step: &str, fields: &[&str]) -> String {
    let lines = step_lines(text, step);
    assert!(
        !lines.is_empty(),
        "the run's own record carries no `step=\"{step}\"` event:\n{text}"
    );
    lines
        .iter()
        .find(|line| fields.iter().all(|field| field_of(line, field).is_some()))
        .unwrap_or_else(|| {
            panic!(
                "no `step=\"{step}\"` event carries every field this leg asked for ({fields:?}); \
                 the lines carrying that step are:\n{}",
                lines.join("\n")
            )
        })
        .clone()
}

/// A docstring requirement, split into the field's name and the value it pins when it pins one:
/// `exposures` asks for the field, `code=0` asks for that value.
fn requirement(line: &str) -> (&str, Option<&str>) {
    match line.split_once('=') {
        Some((name, value)) => (name.trim(), Some(value.trim())),
        None => (line.trim(), None),
    }
}

/// One `name=value` field of a line [`step_line`] returned, with a quoted value's quotes removed.
///
/// **A quoted value is trimmed, and that is the format rather than defensiveness**: a field
/// recorded from a string arrives quoted (`sid="S-1-15-2-…"`) while a numeric or `Display` one
/// arrives bare (`exposures=2`), so the quotes cannot be assumed away and a reader that kept them
/// would hand `icacls` a SID with a `"` in it.
///
/// **The line is split on whitespace, so a message that itself contains `name=` is
/// indistinguishable from a field.** That is accepted rather than parsed around: the callers ask
/// for a field of a line they already identified by its `step`, and a subscriber's message text is
/// not a fact this suite may depend on.
fn field_of(line: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(&prefix))
        .map(|value| value.trim_matches('"').to_string())
}

/// Every event a run's **own record** has to carry for a confinement to be witnessed, and the
/// fields that make each one that event rather than a sentence that mentions it.
///
/// One table per platform because the arm differs, not the shape: an in-place backend confines the
/// process itself and can only speak *after* the confinement (the ordering `sandbox-shared.md` §7
/// and design decision 26 own), while a launcher's record is the launch it performed. Which of the
/// two a run is, is `tinio_sandbox::platform_facts`'s `launches_child` — the same fact
/// `crate::sandbox::confine` dispatches on, and the reason these three tables are the *only* place
/// in this file that names a platform's events rather than a platform's mechanism.
#[cfg(target_os = "linux")]
const CONFINED_STEPS: &[(&str, &[&str])] = &[
    // The §7 events the Linux backend emits while it confines in place: the *resolved* model
    // (`model`), the exposure event's whole field set — the set's size (`count`), the mounts the
    // jail applied (`mounts`) and the rendered path list (`exposures`) — and the network filter's
    // outcome (`installed` — `true` on an architecture with a syscall table, `false` where the
    // filter was skipped and the network guarantee is not delivered). They are live because the
    // CLI installs a fmt-only bootstrap before parsing, so the in-place ordering no longer drops
    // them (design decision 26); the step and field names are the backend's own.
    ("model", &["model"]),
    ("exposures", &["count", "mounts", "exposures"]),
    ("seccomp", &["installed"]),
];
#[cfg(target_os = "macos")]
const CONFINED_STEPS: &[(&str, &[&str])] = &[
    // The mechanism the environment resolved, before anything acts on it (`serve::apply`).
    ("mechanism", &["mechanism"]),
    // The launch's own record of the render it handed the child, and the spawn with the pid the
    // cross-check below reads back out of the kernel.
    ("confine", &["mechanism", "rules"]),
    ("spawn", &["pid", "mechanism"]),
];
#[cfg(target_os = "windows")]
const CONFINED_STEPS: &[(&str, &[&str])] = &[(
    // The grant the launcher reports *before* it makes it — the package SID and the exposure
    // count are the two facts the cross-check reads its SID out of.
    "grant the ACLs",
    &["sid", "exposures", "granting"],
)];
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
const CONFINED_STEPS: &[(&str, &[&str])] = &[];

/// The `state` file of the run under test, parsed by its own type.
///
/// **Parsed, never quoted: it is the one document that carries the bearer token.** `State`'s
/// `Debug` is the hand-written redaction this reads through (`crates/tinio-api/src/state.rs`), and
/// a step that grepped the file as text would bypass it into a CI log —
/// `crates/tinio-server/tests/sandbox_macos_e2e.rs` states the rule and this follows it.
///
/// The directory is the **run's own** ([`Cli::state_dir_of`]), which is the relocated one for a
/// read-only run. The wait is [`PUBLISH_SETTLE`]'s: on the launcher arms the launcher writes this
/// file *after* the readiness frame this harness saw, so a reading taken the instant the scenario
/// continues can find it not yet there — the same window, and the same answer.
fn published_state(cli: &Cli, run: &Run) -> State {
    let dir = cli.state_dir_of(run);
    let deadline = Instant::now() + PUBLISH_SETTLE;
    loop {
        match State::read(&dir) {
            Ok(Some(state)) => return state,
            Ok(None) if Instant::now() < deadline => thread::sleep(POLL),
            Ok(None) => panic!(
                "no server published `{}`: the run this scenario is asserting about is not the \
                 one the state directory records",
                dir.join(STATE_FILE).display()
            ),
            Err(err) => panic!("`{}` is unreadable: {err}", dir.display()),
        }
    }
}

/// A step's path argument with the harness's placeholders expanded: `{root}`, `{state}` and
/// `{scratch}`, the directories a scenario's fixtures live in — the same per-word expansion
/// [`Cli::argv`] does for a command line, and the same three names.
///
/// A path that is still relative after the expansion is the scenario's own scratch directory,
/// because a fixture outside the served root has nowhere else to be: the harness's working
/// directory is the scratch dir for every child, and a step that quietly wrote into the test
/// runner's own cwd would be a fixture nothing cleans up.
///
/// **Not windows-only any more.** Its first caller was the Windows junction fixture, and the
/// doctor's file assertions (`the file … holds/exists/does not exist`) reach the same placeholder
/// expansion on every platform — the paths they name are the harness's own scratch tree, which no
/// scenario can spell.
pub(super) fn expand(cli: &Cli, text: &str) -> PathBuf {
    let expanded = PathBuf::from(
        text.replace("{root}", &cli.root().display().to_string())
            .replace("{state}", &cli.state.display().to_string())
            .replace("{scratch}", &cli.scratch().display().to_string()),
    );
    if expanded.is_absolute() {
        expanded
    } else {
        cli.scratch().join(expanded)
    }
}

/// `path` as the process `pid` sees it: resolved through that process's own root
/// (`/proc/<pid>/root`), so the answer comes from the mount namespace it is confined in rather
/// than from this one.
///
/// **This is the one probe that can see a jail from outside it** —
/// `crates/tinio-server/tests/sandbox_e2e.rs` uses it for the read-only seam and says why. The
/// leading separator is stripped before the join because `Path::join` with an absolute path
/// *replaces* the base instead of extending it, which would silently probe the host's own path.
#[cfg(target_os = "linux")]
fn through_root(pid: u32, path: &Path) -> PathBuf {
    PathBuf::from(format!("/proc/{pid}/root")).join(path.strip_prefix("/").unwrap_or(path))
}

/// The metrics exposition, read from the control channel the run's `state` file names.
///
/// **The control channel is the management plane's, not the S3 port.** It is the unix socket
/// (Linux/macOS) or named pipe (Windows) `state.control_name` records — the *serving* process
/// listens there, and on the launcher arms the launcher binds it and hands it to the child — so
/// the endpoint is read out of the file rather than derived, exactly as `tinio status` reads it.
///
/// **The request is the product's own client** (`tinio_api::client`, the same dialers and the same
/// hyper HTTP/1 exchange `tinio status` and `stop` speak through), so this suite cannot drift from
/// the plane it asserts about: the framing rules a hand-rolled reader would have to restate — a
/// chunked transfer coding, a `Content-Length`, and a named pipe that disarms the instance once it
/// has answered — are the client's, and the token rule is [`metrics_token`]'s.
///
/// The retry is [`PUBLISH_SETTLE`]'s window: the channel and the file are written in the same few
/// milliseconds, and a scrape that lost that race would be a flake rather than a finding.
async fn scrape(cli: &Cli) -> String {
    let state = published_state(cli, cli.last());
    let control = state.control_name.as_str();
    let call = Call {
        target: "/metrics",
        method: Method::GET,
        token: metrics_token(control, &state),
        body: None,
    };
    let deadline = Instant::now() + PUBLISH_SETTLE;
    let response = loop {
        match control_exchange(control, call.clone()).await {
            Ok(response) => break response,
            // A dial that lost the race between the channel and the file it is named in.
            Err(_) if Instant::now() < deadline => sleep(POLL).await,
            Err(err) => panic!("the control channel `{control}` answered nothing: {err}"),
        }
    };
    assert!(
        response.status == 200,
        "`/metrics` over `{control}` was answered `{}`",
        response.status
    );
    response.body
}

/// The token `management-api.md`'s matrix requires on `/metrics` over the channel `control` names,
/// if any.
///
/// **The rule is `tinio_api::router::Auth`'s and this asks it rather than restating it**: the local
/// channel serves `/metrics` openly (a mode-0600 socket is not reachable by another user, and a
/// scrape cannot send a custom header anyway), a loopback TCP bind serves it openly for that same
/// reason, and every other bind requires the token on every endpoint. A `state` file records the
/// local channel in this suite — a run that binds TCP publishes none, because the launcher hands a
/// child only what it bound itself (`Handoff` in `crates/tinio-cli/src/sandbox/confine.rs`) — so
/// the token arm is here to keep this a rule and not a shortcut.
fn metrics_token<'a>(control: &str, state: &'a State) -> Option<&'a str> {
    let auth = match control.parse::<SocketAddr>() {
        Ok(addr) => Auth::for_bind(addr),
        Err(_) => Auth::Local,
    };
    auth.guards("/metrics")
        .then(|| state.token.expose_secret().as_str())
}

/// One management request over the control channel `control` names, as the three shapes the
/// contract lists: a Windows named pipe (the full `\\.\pipe\…` form the bind normalises), a TCP
/// address, or a unix socket path.
///
/// **The dialers and the exchange are the product's client's**, which is the point: `get_http` and
/// `get_local` dial the two transports it dials itself, and the pipe — the one stream it does not
/// dial here — goes through its `exchange` over tokio's own dialer. **That dialer is a Windows-fact
/// rather than a taste**: `CreateFile` on a pipe name succeeds against an instance the server has
/// not accepted yet, and the first write then answers `ERROR_PIPE_NOT_CONNECTED (233)` — measured
/// on this host, with `std::fs`'s own `OpenOptions` — while the dial the product's client uses
/// (`tokio::net::windows::named_pipe::ClientOptions`) connects.
async fn control_exchange(control: &str, call: Call<'_>) -> Result<RawResponse, tinio_api::Error> {
    if let Ok(addr) = control.parse::<SocketAddr>() {
        return get_http(dialable(addr), call).await;
    }
    #[cfg(windows)]
    if control.starts_with(r"\\.\pipe\") {
        use hyper_util::rt::TokioIo;
        use tinio_api::client::exchange;

        let pipe = ClientOptions::new().open(control)?;
        return exchange(TokioIo::new(pipe), call).await;
    }
    get_local(Path::new(control), call).await
}

// --- Given --------------------------------------------------------------

/// The scenario's scratch root: the `Given` that claims the harness. A second call in the same
/// scenario is the same root, never a new one — a scenario serves one root.
#[given("a storage root")]
async fn a_storage_root(world: &mut World) {
    harness(world);
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

/// The induction G12 rests on: **a directory** where the run's `state` publish clears its temp
/// file — `<root>/.tinio/state.tmp`, the path `State::write` removes before it creates its own
/// (`crates/tinio-api/src/state.rs`).
///
/// **A directory is what makes the plant defeat the removal**, and the defeat is the point:
/// `fs::remove_file` cannot take one (linux `EISDIR`, Windows `ERROR_ACCESS_DENIED`), so the
/// removal fails, the publish fails with it, and — because the product's own removal is exactly
/// what the plant is standing on — the plant is **sticky**: nothing but this scenario's teardown
/// removes it ([`remove_sticky_state_tmp`], which [`Cli::teardown`] calls on every path out).
///
/// **The pre-flight probe is immune to it, which is what makes the induction land where the
/// scenario says it does**: `refuse_unwritable_state_dir` only ever creates and removes its own
/// `state-probe-<pid>-<nanos>` entry, so a `state.tmp` it never names cannot make the run refuse
/// before it launches.
///
/// The directory is the **read-write** state dir ([`Cli::state_dir`]) — where a launcher
/// publishes, and the path [`remove_sticky_state_tmp`] reaches.
#[given(expr = "the root's state dir holds a {string} directory")]
async fn state_dir_holds_a_directory(world: &mut World, name: String) {
    let state_dir = cli(world).state_dir();
    fs::create_dir_all(&state_dir).expect("the reserved directory");
    let path = state_dir.join(&name);
    fs::create_dir(&path)
        .unwrap_or_else(|err| panic!("the fixture directory `{}`: {err}", path.display()));
}

/// The induction P3 rests on: **`<root>/.tinio/.env` is a symlink to an existing, parseable
/// dotenv file outside the served root** — the fixture `steps/cli.rs`'s docstring-writing
/// [`state_dir_file`] cannot express, because the entry has to be a *link* and its target has to
/// be reachable without being in the served root.
///
/// **Two writes, and both are load-bearing.** The target file is written first and is a valid
/// dotenv document, so what the confined child meets is the *read through the link* and not a
/// file that is missing or malformed; the link is the entry a run's `.env` load finds at
/// `<state dir>/.env` (`tinio_cli::config::sources::load_env_file`). The state directory is the
/// **read-write** one ([`Cli::state_dir`]): this is the mode whose state dir is lexically in-root,
/// which is what keeps the profile from ever naming the link's resolution — the mechanism the
/// scenario's own comment states in full.
///
/// **The target lives in the scenario's scratch directory** ([`Cli::scratch`], the root's parent),
/// which is neither the served root nor a path any rule names, so the fixture needs no fixture
/// path of its own and a scenario never spells a temp path.
///
/// **Unix only**: the plant is a symlink (`std::os::unix::fs::symlink`), and the scenario that
/// uses it is `@macos`. The recorded errno is deliberately not asserted anywhere — see the
/// scenario's comment.
#[cfg(unix)]
#[given(expr = "the root's state dir holds a {string} link to a file outside the root")]
async fn state_dir_holds_a_link_outside(world: &mut World, name: String) {
    use std::os::unix::fs::symlink;

    let cli = cli(world);
    let target = cli.scratch().join("outside.env");
    fs::write(&target, "# a dotenv file outside the served root\n")
        .unwrap_or_else(|err| panic!("the dotenv file `{}`: {err}", target.display()));
    let state_dir = cli.state_dir();
    fs::create_dir_all(&state_dir).expect("the reserved directory");
    let link = state_dir.join(&name);
    symlink(&target, &link).unwrap_or_else(|err| panic!("the link `{}`: {err}", link.display()));
}

/// The fixture one of the confinement's channels needs, planted before the run that asks through
/// it — the `Given` half of every scenario that drives a channel other than the flag, with
/// `SandboxChannel::ask` owning what each arm writes.
#[given(expr = "the {string} channel asks for the confinement")]
async fn channel_asks(world: &mut World, channel: String) {
    SandboxChannel::parse(&channel).ask(cli(world));
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

/// **The second state the predicate has to tell apart — the profile left behind, where the
/// scenario above has an absent name — as a state fixture with a regression role, not the
/// reproduction of an observed defect.** Measured, the residue cannot defeat the predicate on
/// this host: `CreateAppContainerProfile`'s permission check precedes its existence check, so a
/// refused token answers `E_ACCESSDENIED` (`0x80070005`) with the name present and never
/// reaches the colliding-name takeover (`ALREADY_EXISTS`, `0x800700B7`) that the
/// clear-before-create guards against on a host whose check order differs.
///
/// **The residue is manufactured by the crate that owns the name.**
/// [`testing::leave_probe_residue`] creates the probe's own profile and forgets the value:
/// `PackageSid`'s lifetime *is* create-then-delete, so forgetting it is exactly the state a
/// hard kill leaves — a profile that exists and that no value will remove. The fixture lives
/// there rather than here because the name is `capability::PROBE_NAME`, which is `pub(crate)`:
/// a step spelling the literal for itself would be a second definition of the name the
/// predicate probes, which is what the fixture's move retires.
///
/// **Measured, the residue is this run's rather than the next run's.** The seed's own
/// `CreateAppContainerProfile` line reports `taken_over=false` in every run measured (the
/// wave-E review's three and this round's), so the name is absent each time this step runs and the
/// sibling scenario above still starts from an absent name. Nothing in the fixture removes the
/// profile — the `forget` is its point — and nothing waits for it: the launch-path sweep
/// (`tinio_sandbox_windows::sweep`, design decision 21) reaps this class of orphaned profile,
/// and the suite's measured runs show the name gone by the next seed rather than carried to it.
///
/// **The seed retries, because the profile name is the predicate's fixed one and the scenario
/// beside this one is probing it at the same time.** `userenv` answers
/// `0x80070020` (`ERROR_SHARING_VIOLATION`) or `0x80070003` (`ERROR_PATH_NOT_FOUND`) to a
/// create that overlaps another process's create-then-delete of the same name — the transient
/// window `capability.rs` records — and a scenario that failed on one would be reporting its
/// own scheduling rather than the predicate. Only a definitive answer is taken: a refusal of
/// the `E_ACCESSDENIED` kind cannot arrive here, because this process's token is the ordinary
/// Medium one the runner has, so any other failure is a real host problem and panics. The retry
/// loop is the fixture's own ([`testing::leave_probe_residue`]); this step only gives it a
/// thread that is not the one driving every scenario in this binary.
#[cfg(target_os = "windows")]
#[given("an earlier probe left its profile behind")]
async fn an_earlier_probe_left_its_profile_behind(_world: &mut World) {
    spawn_blocking(testing::leave_probe_residue)
        .await
        .unwrap_or_else(|join| panic!("the residue fixture panicked: {join}"));
}

/// `TINIO_STATE_DIR` **is** the served root — the one input that makes a pg run's synthetic root
/// coincide with the scenario's scratch root.
///
/// A pg run has no served directory, so `resolve::pg::resolve_root`'s override arm takes the
/// variable verbatim as the **root**, not the state directory (`an_explicit_state_dir_moves_the_synthetic_root`
/// pins the rule): the state directory is then that root's own `.tinio`, so the `state` file, the
/// access log and the control channel land under it exactly as a filesystem run's do. Every other
/// `@cli` scenario's children get the harness's *other* state directory (a candidate beside the
/// root); this is the one step that points the variable at the root itself.
#[given("the child's state directory override is the served root")]
async fn state_dir_override_is_the_root(world: &mut World) {
    let cli = cli(world);
    cli.vars
        .push((STATE_DIR_VAR.to_owned(), cli.root().display().to_string()));
}

/// **The fixture every `@pg` scenario owns: a scratch PostgreSQL database and the
/// `[storage] type = "pg"` configuration that names it.**
///
/// **A scratch database, never the shared `tinio` one** (PG8/PG12): the writable-ownership lock is
/// process-lifetime, so a run that took it against the shared fixture would starve every other leg
/// of the pass — and the pass's own backend-neutral scenarios reset that fixture for themselves.
/// The name is the harness's, unique per process, and [`Cli::drop_pg_scratch`] removes the
/// database when the scenario ends.
///
/// **The configuration is written where the run reads it** — `<root>/pg.toml`, named on the
/// command line with `--config` (the one document a pg run reads without a root, PG13) — and
/// carries the `[auth]` pair this suite signs with ([`ACL_ROOT`]), so the journey's requests
/// authenticate against the run's own configuration chain rather than a harness-side switch. The
/// run opens the scratch dsn, never the base one, so no other leg's database is touched.
#[cfg(feature = "pg")]
#[given("a scratch pg database and a pg configuration")]
async fn scratch_pg_database(world: &mut World) {
    let db = TestDb::create()
        .await
        .expect("the @pg pass needs TINIO_PG_DSN (the compose fixture's dsn, never spelled here)");
    let dsn = db.dsn.clone();

    let cli = cli(world);
    // Through the schema, never as text (the same rule `config_asks_for_the_confinement`
    // follows): the section, the key and its value are `tinio_cli::config`'s own types, so a
    // renamed key is a compile error here rather than a document the run silently reads as
    // an unset default.
    let config = Config {
        storage: Some(storage::Config {
            backend: storage::Backend::Pg,
            pg: Some(storage::pg::Config {
                dsn: Some(auth::Secret::from(auth::SecretKey::from(dsn))),
                ..Default::default()
            }),
            ..Default::default()
        }),
        auth: Some(auth::Config {
            access_key: ACL_ROOT.0.to_owned(),
            secret_key: auth::Secret::from(auth::SecretKey::from(ACL_ROOT.1.to_owned())),
        }),
        ..Default::default()
    };
    let text = toml::to_string(&config).expect("the pg configuration schema serializes");
    let path = cli.root().join("pg.toml");
    fs::write(&path, text)
        .unwrap_or_else(|err| panic!("the pg configuration `{}`: {err}", path.display()));
    cli.pg_scratch = Some(db);
}

/// **The same scratch database and configuration, but with no `dsn` in the document.** The run
/// must take its dsn from `TINIO_PG_DSN` — the file the step below writes — so this is the
/// env-only half of `resolve::pg::resolve_dsn` (PG5) that the unit legs could only reach without
/// the process surface. The `[storage.pg]` section is present (so the backend is `pg` and the
/// configuration names one) but its `dsn` key is omitted, which the schema's own
/// `skip_serializing_if` makes an absent key rather than an empty one.
#[cfg(feature = "pg")]
#[given("a scratch pg database and a pg configuration with no dsn")]
async fn scratch_pg_database_env_only(world: &mut World) {
    let db = TestDb::create()
        .await
        .expect("the @pg pass needs TINIO_PG_DSN (the compose fixture's dsn, never spelled here)");

    let cli = cli(world);
    let config = Config {
        storage: Some(storage::Config {
            backend: storage::Backend::Pg,
            pg: Some(storage::pg::Config {
                dsn: None,
                ..Default::default()
            }),
            ..Default::default()
        }),
        auth: Some(auth::Config {
            access_key: ACL_ROOT.0.to_owned(),
            secret_key: auth::Secret::from(auth::SecretKey::from(ACL_ROOT.1.to_owned())),
        }),
        ..Default::default()
    };
    let text = toml::to_string(&config).expect("the pg configuration schema serializes");
    let path = cli.root().join("pg.toml");
    fs::write(&path, text)
        .unwrap_or_else(|err| panic!("the pg configuration `{}`: {err}", path.display()));
    cli.pg_scratch = Some(db);
}

/// **`TINIO_PG_DSN` written where the run reads it: `<state dir>/.env`** (FR-016). The file is the
/// one channel that supplies a dsn the process environment does not carry — `Cli::command` scrubs
/// every `TINIO_*` from a child, so the variable reaches this run only through the file — and the
/// resolution loads it *before* it derives the pg root (`resolve::pg::env_file_state_dir`), which
/// is what makes an env-only run reach `resolve_dsn` at all. The state directory is the run's own
/// read-write one (`<root>/.tinio`), where `resolve::apply_env_file` looks.
///
/// **The dsn is the scenario's own scratch database**, not the shared `tinio` fixture, so the
/// file names a database this scenario created and drops.
#[cfg(feature = "pg")]
#[given("the pg dsn is written to the state directory's environment file")]
async fn pg_dsn_in_env_file(world: &mut World) {
    let cli = cli(world);
    let dsn = cli
        .pg_scratch
        .as_ref()
        .expect("the pg configuration step created a scratch database")
        .dsn
        .clone();
    let state_dir = cli.state_dir();
    fs::create_dir_all(&state_dir).expect("the reserved directory");
    let path = state_dir.join(ENV_FILE);
    fs::write(&path, format!("TINIO_PG_DSN={dsn}\n"))
        .unwrap_or_else(|err| panic!("the dotenv file `{}`: {err}", path.display()));
}

// --- When ---------------------------------------------------------------

/// The operator's command line, run to whatever end it reaches: an exit, or a server that keeps
/// serving.
///
/// **The command text takes the same substitutions the raw wire steps do** (`errors.rs`'s
/// `subst`): a scenario may capture a value from the wire — a version id, an upload id — and
/// name it on the operator's command line, which is the only way the process surface can address
/// that identity. The two harness placeholders (`{root}`, `{state}`, `{missing}`) are `Cli::argv`'s
/// and are left alone here.
#[when(expr = "I run {string}")]
async fn run_command(world: &mut World, command: String) {
    let command = super::errors::subst(world, &command);
    cli(world).run(&command).await;
}

/// **The one host state a scenario has to create rather than ask about.** The runner's own token
/// is Medium, so a plain [`run_command`] makes `doctor` see a host that can confine and the `warn`
/// row this leg asserts on could never be witnessed. The child is spawned under a Low integrity
/// token (`tinio_sandbox_windows::testing::low_integrity`), which is the state a would-be
/// confiner is refused in, and it is recorded into [`Cli::last`] exactly as [`Cli::run`] records
/// an ordinary one: both streams into this run's own pair of files ([`Cli::run_paths`]), which
/// the device is handed as the child's destinations, so the `Then` steps read what they read for
/// every other run.
///
/// **The environment is [`Cli::command`]'s, and [`child_environment`] is what keeps it so.**
/// `CreateProcessAsUserW` cannot be handed a `std::process::Command`, so the block is built from
/// the modifications that method made and this process's environment — the `TINIO_*` scrub has
/// one definition rather than a second one here.
///
/// **The launch runs on the runtime's blocking pool.** The device is synchronous by design — it
/// waits with `WaitForSingleObject` under its own `CHILD_DEADLINE_MS` — and the whole suite is
/// polled from one thread (the runtime's `block_on`, `tests/cucumber.rs`'s runner), so calling
/// it inline would hold every scenario in this binary for the child's whole life, where each
/// sibling run here polls with `sleep(POLL).await`. The hop is the step's, which is what keeps
/// the device's sync entry — the one its own `#[test]` leg calls — and its deadline unchanged.
///
/// **A refused launch fails the scenario rather than skipping.** `low_integrity` panics when the
/// token or the process creation is refused; the blocking task surfaces that panic as its join's
/// error, which this step re-panics. A host that cannot make the state this leg asserts on has
/// not produced the state, and must be red rather than quietly fall back to the runner's own
/// Medium token.
#[cfg(target_os = "windows")]
#[when(expr = "I run {string} under a low-integrity token")]
async fn run_under_low_integrity(world: &mut World, command: String) {
    let cli = cli(world);
    let argv = cli.argv(&command);
    // **`Cli::read_only`'s answer, and not the flag alone**: this step records it into the same
    // [`Run`] the `Then` steps read, so the two sites must agree that a run may be read-only
    // through `TINIO_READ_ONLY` with no flag at all — reading argv only would make a scenario
    // that set the variable silently disagree with every other run about the mode it served.
    let read_only = cli.read_only(&argv);
    let environment = child_environment(cli);
    let program = cargo_bin("tinio");
    let args: Vec<String> = argv[1..].to_vec();
    let scratch = cli.scratch().to_path_buf();
    let (out_path, err_path) = cli.run_paths();
    let started = Instant::now();
    let out = out_path.clone();
    let err = err_path.clone();
    let finished = spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        testing::low_integrity::run_with(&program, &args, &scratch, Some(&environment), &out, &err)
    })
    .await
    .unwrap_or_else(|join| panic!("the Low-integrity launch task panicked: {join}"));
    let took = started.elapsed();
    cli.record(
        (out_path, err_path),
        Some(finished.status),
        None,
        took,
        finished.pid,
        read_only,
    );
}

/// Ends the server without letting it drain — the only way to leave a `state` file behind that
/// nothing answers for.
#[when("I kill the server")]
async fn kill_the_server(world: &mut World) {
    cli(world).kill_servers();
}

/// The run has ended — the wait [`Cli::run_ends`] owns, for the scenarios whose subject begins
/// where the readiness frame leaves off (G12's launcher reports readiness and then fails).
///
/// **A `When` rather than a `Then`** because it acts: it waits for the ending instead of
/// asserting a value the capture already holds, and the ordinary `the run fails` / `the run
/// fails with exit code {int}` steps beside it are what assert the status it recorded. A run
/// that never ends is that call's own loud failure.
#[when("the run ends")]
async fn the_run_ends(world: &mut World) {
    cli(world).run_ends();
}

/// Ends the run the way an operator does — the control channel's `stop`, through the client the
/// contract names (`tinio stop <root>`, `contracts/cli.md`) — so the child **drains** and the
/// process this scenario spawned leaves a status behind.
///
/// **The ending has to be a drain for a launcher arm's scenario to have a subject.** `I kill the
/// server` ends the process with nothing said: on a launcher arm the confined child is killed
/// without draining, which leaves the launcher no status to carry, so a scenario about what the
/// launcher forwards cannot use it. Here the client asks the server to stop, the child's control
/// plane shuts the run down, and the wait the launcher owns returns the child's own code.
///
/// **The client is deliberately not a `Cli::run`.** [`Cli::last`] has to stay the *server*: the
/// steps these scenarios pair with this one wait on the run under test and assert the status it
/// ended with, and `Cli::run` would replace that record with the client's own — a run that exits
/// 0 whether or not the server ever drained, which is exactly the fact such a scenario is
/// claiming. The client is spawned the way every site in this harness spawns (`job::spawn`),
/// writes its two streams to files ([`Cli::run_paths`], so its run number cannot collide with a
/// `Cli::run`'s), and is bounded by [`RUN_TIMEOUT`] rather than waited on forever — a client that
/// never exits is a finding this step reports with both streams, and one that did not reach the
/// server is reported here rather than by the wait that would have followed it.
///
/// **The `state` file is waited for before the client runs, and it asserts nothing.** The client
/// discovers the server through that file, and on the launcher platforms the file is published
/// *after* the readiness frame this harness sees (`sandbox/confine.rs`'s `publish_state` waits
/// for the child's own terminal event), so the instant the frame lands it can be absent —
/// measured on macOS 15.8, 2026-09-25, once in two runs, which is why [`published_state`] allows
/// the same window for the same reason. A file that never arrives is not this step's finding: the
/// client's own refusal is quoted below. The path is [`state_file`]'s, which is the read-write
/// answer a run under this step resolves (`<root>/.tinio`); a read-only run's file is elsewhere
/// and the wait is then simply spent.
#[when("I stop the server through its control channel")]
async fn stop_through_control(world: &mut World) {
    let cli = cli(world);
    // `wait_for_path`'s wait, and its verdict is deliberately dropped: a file that never arrives
    // is not this step's finding, and the client's own refusal is quoted below.
    wait_for_path(&state_file(cli.root()), PUBLISH_SETTLE, POLL);
    let (out_path, err_path) = cli.run_paths();
    let mut cmd = cli.command();
    cmd.arg("stop")
        .arg(cli.root())
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            fs::File::create(&out_path).expect("the stop client's stdout file"),
        ))
        .stderr(Stdio::from(
            fs::File::create(&err_path).expect("the stop client's stderr file"),
        ));
    let mut child = job::spawn(&mut cmd).expect("spawn the stop client");
    let status = poll_exit(&mut child, RUN_TIMEOUT).unwrap_or_else(|| {
        kill_and_reap(&mut child);
        panic!(
            "the stop client did not exit within {}s\nstdout:\n{}\nstderr:\n{}",
            RUN_TIMEOUT.as_secs(),
            read(&out_path),
            read(&err_path)
        );
    });
    assert!(
        status.success(),
        "the stop client failed ({status}), so the run this scenario is about never got the stop \
         and its ending says nothing about the launcher\nstdout:\n{}\nstderr:\n{}",
        read(&out_path),
        read(&err_path)
    );
}

/// Ends the server the way an operator's `kill` does — a **real `SIGTERM`**, sent to the pid the
/// run's own `state` file records.
///
/// **The pid is the run's own record and never this harness's handle**, which is the whole of why
/// the step reads the file: on the in-place arm that pid *is* the server (G10 asserts it,
/// `state_names_the_spawned_process`), while the handle this harness holds would answer with
/// `Child::kill` — a `SIGKILL`, the killer's ending rather than the one under test. The file is
/// parsed by its own type ([`published_state`]), so the token it carries never reaches a log.
///
/// **Why the signal is sent from outside the process.** The unit leg for the same channel
/// (`crates/tinio-cli/src/signal.rs`'s `a_signal_flips_the_shutdown_channel`) raises `SIGTERM` in
/// the *unconfined* libtest process, where the wake's syscall is permitted and the defect this
/// step's scenario exists for cannot happen; only a confined process is asked to wake itself under
/// a filter, and that intersection is what this step opens.
#[cfg(target_os = "linux")]
#[when("I send the server SIGTERM")]
async fn send_sigterm(world: &mut World) {
    let cli = cli(world);
    let state = published_state(cli, cli.last());
    let pid = Pid::from_raw(i32::try_from(state.pid).expect("a pid fits an i32"))
        .expect("a serving server's pid is not zero");
    kill_process(pid, Signal::TERM).expect("the run's own server takes the signal");
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
        run.stderr()
    );
}

/// The signal's ending: the run is gone, on its own, with the drain's status — the first
/// termination signal shuts a server down gracefully, so the process exits 0.
///
/// **The wait is [`Cli::run_ends`]'s**, the harness's wait for a run captured while it was still
/// serving, and **that wait's bound is the assertion's other half**: a server that ignored the
/// signal is still serving when the bound expires, and that call kills the process and fails
/// loudly with both of its streams — which is exactly what a confined run did while the wake
/// belonged to `signal-hook` (the seccomp trap is `crates/tinio-sandbox-linux/src/seccomp.rs`'s
/// module doc). A `SIGKILL` is therefore never mistaken for this ending: the harness would have
/// had to send one.
#[cfg(target_os = "linux")]
#[then("the run stops on the signal")]
async fn run_stops_on_signal(world: &mut World) {
    let cli = cli(world);
    let status = cli.run_ends();
    assert!(
        status.success(),
        "the first termination signal drains and exits 0; the run left {status}\nstdout:\n{}\n\
         stderr:\n{}",
        cli.last().stdout,
        cli.last().stderr()
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
        run.stderr()
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
        run.stderr()
    );
}

/// The refusal's other half: **a run that served always said so**, so a refusal that somehow came
/// up fails here instead of passing on its exit status alone.
///
/// **Why a refusal row cannot state this by naming a `state` file's path.** Which directory a
/// run publishes into is the resolution's answer, not the harness's: a **read-only** run resolves
/// `TINIO_STATE_DIR` (or the home-derived directory) rather than `<root>/.tinio`
/// (`resolve::resolve_state_dir`), and a scenario that overrode the variable moves it again — so
/// the one path a step could name is a path a serving run would not write, and the assertion
/// could not fail. The readiness frame has no such problem: it is the run's own record of having
/// come up, written to the stdout file this harness polls ([`Cli::run`] ends its loop on it), so
/// `None` is exactly "this run never served".
#[then("the run never reported readiness")]
async fn run_never_reported_readiness(world: &mut World) {
    let run = cli(world).last();
    assert!(
        run.readiness.is_none(),
        "a run that refused never comes up, and this one reported readiness \
         ({:?}):\nstdout:\n{}\nstderr:\n{}",
        run.readiness,
        run.stdout,
        run.stderr()
    );
}

/// **The launcher's own frame, and the only frame its stdout may carry** — the assertion P3 rests
/// on, and the one [`run_never_reported_readiness`] cannot make on its own.
///
/// `run.readiness` is `None` for the run this step is written for, and that is *not* the fact the
/// leg is about: [`readiness`] decodes the **first** frame (`Event::decode` dispatches at the
/// first blank line), so a stdout carrying `error` and then `listening` leaves it `None` — a step
/// asking only for the absence of readiness would pass over a run that announced a server it then
/// lost. What is asserted here is the **count** as well as the event: exactly one frame, it is the
/// `error` frame, and its message is the sentence the scenario names.
///
/// **The sentence is the launcher's own** and the caller quotes it rather than this step deriving
/// it: on this path it is `Attribution::Broken`'s detail rendered by `crate::sandbox::describe`
/// (`crates/tinio-cli/src/commands/start/run.rs`'s `launcher_exit`), so the status in it is the
/// child's — the one fact the run's exit code cannot carry, `1` being the launcher's fallback for
/// a child killed by a signal as well.
///
/// **A `listening` frame fails both halves**, which is what makes the step discriminating rather
/// than decorative: it would be the wrong event if it were the only frame, and a second frame if
/// it stood beside the error one.
#[then(expr = "its stdout is exactly one error frame saying {string}")]
async fn stdout_is_one_error_frame(world: &mut World, message: String) {
    let run = cli(world).last();
    let events = frames(&run.stdout);
    assert_eq!(
        events.len(),
        1,
        "the launcher's stdout is one frame and this run wrote {}:\nstdout:\n{}\nstderr:\n{}",
        events.len(),
        run.stdout,
        run.stderr()
    );
    match &events[0] {
        Event::Error { message: framed } => assert_eq!(
            framed,
            &message,
            "the launcher's `error` frame carries its own sentence:\nstdout:\n{}\nstderr:\n{}",
            run.stdout,
            run.stderr()
        ),
        Event::Listening { addr } => panic!(
            "the launcher framed `listening` ({addr}) instead of its `error` frame: this process \
             never announces a server — only the child it supervises knows one is serving \
             (`launcher_exit`'s doc, design §3 step 8):\nstderr:\n{}",
            run.stderr()
        ),
    }
}

/// A refusal's stdout shape, with no sentence asserted: **exactly one frame and it is the `error`
/// frame, exactly three raw lines and no fourth** — an `event:` line, a `data:` line and the blank
/// line that closes them (decision 19).
///
/// **The in-process home this shape came from is retired**:
/// `crates/tinio-server/tests/serve_sandbox_channels.rs`'s `refused_or_honoured` pinned it against
/// the spawned child, and the placement cleanup
/// (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`) moved the shape here with the
/// scenarios that owed it. The sentence a caller may want inside the frame is the return value.
///
/// **Both halves, because neither implies the other.** [`stdout_is_one_error_frame`] asks for the
/// *exact* sentence, which a wrapped one cannot satisfy — a `Config` refusal renders as
/// `configuration error: …` and the macOS refusals interpolate the two paths they name — so a
/// caller asserting a sentence matches it by containment; what either form cannot see is a line
/// that never reached a frame, because [`frames`] skips a block that does not decode, so a stray
/// write beside the frame would leave the decoded count at one. The raw line count closes that, and
/// the frame's own kind closes the mirror case: a run that came up wrote `listening`, whatever its
/// status.
fn refusal_frame_only(run: &Run) -> String {
    let events = frames(&run.stdout);
    assert_eq!(
        events.len(),
        1,
        "a refusal frames its cause once and this run wrote {} frames:\nstdout:\n{}\nstderr:\n{}",
        events.len(),
        run.stdout,
        run.stderr()
    );
    let framed = match &events[0] {
        Event::Error { message } => message.clone(),
        Event::Listening { addr } => panic!(
            "the run framed `listening` ({addr}) instead of the refusal it exited with — a run \
             that came up served, whatever its status:\nstdout:\n{}\nstderr:\n{}",
            run.stdout,
            run.stderr()
        ),
    };
    let lines = run.stdout.lines().count();
    assert_eq!(
        lines, 3,
        "stdout must carry the refusal frame and nothing else — an `event:` line, a `data:` line \
         and the blank line that closes it; got {lines} lines:\nstdout:\n{}",
        run.stdout
    );
    framed
}

/// **A refused run's stdout is the refusal frame and nothing else** — [`refusal_frame_only`]'s
/// shape, with the sentence the scenario named asserted inside the frame by containment (the same
/// relation the `stderr contains` step beside it uses).
#[then(expr = "its stdout is the refusal frame and nothing else, carrying {string}")]
async fn stdout_is_the_refusal_frame_alone(world: &mut World, message: String) {
    let run = cli(world).last();
    let framed = refusal_frame_only(run);
    assert!(
        framed.contains(message.as_str()),
        "the refusal frame carries the sentence the run refused with:\nstdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr()
    );
}

/// **A serving run's stdout is the readiness frame and nothing else** — the mirror of
/// [`stdout_is_the_refusal_frame_alone`] for the run that came up, and the shape decision 19
/// fixed for the stream: an `event:` line, a `data:` line and the blank line that closes them.
///
/// **Both halves, because neither implies the other.** A run that wrote a second frame (or a
/// plain line that is not a frame) *is* ready — [`Cli::run`] ends its poll at the first
/// `listening` frame — so `the readiness line reports a bound port` passes over it; and a decoded
/// count of one says nothing about a stray un-decodable line beside the frame. The count of raw
/// lines is what closes that, which is why the shape is asserted rather than inferred.
#[then("its stdout is the readiness frame alone")]
async fn stdout_is_the_readiness_frame_alone(world: &mut World) {
    let run = cli(world).last();
    let events = frames(&run.stdout);
    assert_eq!(
        events.len(),
        1,
        "a serving run frames its readiness once and this run wrote {} frames:\nstdout:\n{}\n\
         stderr:\n{}",
        events.len(),
        run.stdout,
        run.stderr()
    );
    match &events[0] {
        Event::Listening { .. } => {}
        Event::Error { message } => panic!(
            "the run framed `error` ({message}) for a run that served — a refusal and a ready \
             server are not the same run:\nstdout:\n{}\nstderr:\n{}",
            run.stdout,
            run.stderr()
        ),
    }
    let lines = run.stdout.lines().count();
    assert_eq!(
        lines, 3,
        "stdout must carry the readiness frame and nothing else — an `event:` line, a `data:` \
         line and the blank line that closes it; got {lines} lines:\nstdout:\n{}",
        run.stdout
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
    let stderr = cli(world).last().stderr();
    assert!(stderr.contains(&text), "stderr lacks {text:?}:\n{stderr}");
}

/// A usage error says what to do, so its stderr is never empty.
#[then("its stderr is not empty")]
async fn stderr_not_empty(world: &mut World) {
    let stderr = cli(world).last().stderr();
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
///
/// **Four arms for three channels, because `.env` is the variable one level lower** (FR-016): the
/// file is loaded into the child's environment between the two parses
/// (`resolve::apply_env_file`), so what the authoritative parse answers is an *environment*
/// answer — which is why the refusal names the variable and never the file, the half G3 asserts.
#[derive(Clone, Copy, Debug)]
enum SandboxChannel {
    /// `--sandbox`: the flag, which only a build carrying the `sandbox` feature has at all.
    Flag,
    /// `TINIO_SANDBOX`: the variable, which cannot be compiled away.
    Env,
    /// `[server] sandbox = true`: the configuration key, the third channel of design decision 10,
    /// read only when clap answered nothing (`sandbox::plan`'s `resolve_flag`).
    Config,
    /// `TINIO_SANDBOX` in `<state dir>/.env`: the variable as the file spells it.
    DotEnv,
}

impl SandboxChannel {
    /// The channel a step's own spelling names — the feature's four, and only these.
    fn parse(spelling: &str) -> Self {
        match spelling {
            "--sandbox" => Self::Flag,
            SANDBOX_VAR => Self::Env,
            "[server] sandbox" => Self::Config,
            ".env" => Self::DotEnv,
            other => panic!(
                "a confinement channel is `--sandbox`, `TINIO_SANDBOX`, `[server] sandbox` or \
                 `.env`, not {other:?}"
            ),
        }
    }

    /// The spelling the operator used, which every refusal the crate raises carries
    /// (`sandbox::plan`'s `Channel::name` is the one home of the three).
    ///
    /// **The `.env` arm's is the variable's**, and that is the whole of what makes it a fourth arm
    /// rather than a second name for [`Self::Env`]: the arm is a different *fixture*, not a
    /// different spelling, and a refusal that named the file would be naming a channel the
    /// resolution never had.
    fn spelling(self) -> &'static str {
        match self {
            Self::Flag => "--sandbox",
            Self::Env | Self::DotEnv => SANDBOX_VAR,
            Self::Config => "[server] sandbox",
        }
    }

    /// The fixture this channel needs before a run can be asked through it — what
    /// `the {string} channel asks for the confinement` plants.
    ///
    /// **The flag has nothing to plant**, and that is not an omission: the channel *is* the
    /// scenario's own command line, which is why a `Scenario Outline` over the three asking
    /// spellings carries this step for all three rows and only two of them write anything.
    fn ask(self, cli: &mut Cli) {
        match self {
            Self::Flag => {}
            Self::Env => {
                // Replaced rather than pushed: a scenario that named the variable itself
                // (`the child's environment holds …`, for a precedence case) owns the value, and
                // a second entry would make the last one to run the child's answer.
                cli.vars.retain(|(name, _)| name != SANDBOX_VAR);
                cli.vars.push((SANDBOX_VAR.to_owned(), "1".to_owned()));
            }
            Self::Config => {
                config_asks_for_the_confinement(cli);
                cli.fixture_asked = true;
            }
            Self::DotEnv => {
                env_file_asks_for_the_confinement(cli);
                cli.fixture_asked = true;
            }
        }
    }
}

/// The configuration channel, planted where the run reads it: `<root>/.tinio/.tinio.toml`, the
/// document `discover::resolve_path` finds first for a read-write run, carrying
/// `[server] sandbox = true`.
///
/// **Through the schema, never as text**: the section, the key and its value are `tinio_cli::config`'s
/// own types, so a renamed key is a compile error here rather than a document the run silently
/// reads as an unset default. That is the rule `steps/clients.rs`'s `config_for` follows for the
/// capability overrides, and this is the same call on a different section.
///
/// The run that finds this file takes it as its configuration — the first start's generated
/// credential pair is what the file's *absence* triggers — so `[auth]` is left at its default and
/// the data plane falls back to the pair `tinio-server`'s identity already uses for an absent
/// section, which is what these scenarios, which make no signed request, need.
fn config_asks_for_the_confinement(cli: &Cli) {
    let config = Config {
        server: server::Config {
            sandbox: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let text = toml::to_string(&config).expect("the configuration schema serializes");
    let state_dir = cli.state_dir();
    fs::create_dir_all(&state_dir).expect("the reserved directory");
    fs::write(state_dir.join(discover::CONFIG_FILE), text).expect("the configuration file");
}

/// The variable one level lower: `<state dir>/.env`, the file `resolve::apply_env_file` loads
/// between the two parses (FR-016's level 3; `tinio-cli/src/config/sources.rs` spells the name).
///
/// **The process environment stays empty**, which is the whole of what this arm adds: the file is
/// the channel, and the run's command line carries neither the flag nor a variable — the state
/// the scenario asserts a refusal against.
fn env_file_asks_for_the_confinement(cli: &Cli) {
    let state_dir = cli.state_dir();
    fs::create_dir_all(&state_dir).expect("the reserved directory");
    fs::write(state_dir.join(ENV_FILE), format!("{SANDBOX_VAR}=1\n")).expect("the dotenv file");
}

/// **The invariant every sandbox channel is held to, on every build: a requested confinement is
/// never silently ignored.**
///
/// Where nothing can honour the channel the run refuses, names the channel that asked for it, and
/// names the *build* whose parser has no `--sandbox` — never the platform, because every target
/// this workspace builds has a backend that would honour one. Where this target's backend is
/// wired the request is honoured instead, and the assertion is the readiness event **plus the
/// absence of every refusal's words**: "confined" is the platform legs' subject (`sandbox_e2e*`),
/// not something a reader of stdout can certify.
///
/// **The arm is dispatched, not accepted either way.** The probe says which one this host owes —
/// the feature is in that parser or it is not — and the run is held to that arm's own words, so
/// the spelling this replaces — "either refused with the channel named or applied" — can no
/// longer pass on a run that failed for some other reason and merely mentioned `sandbox`, nor on
/// an honour arm that never reached readiness.
///
/// **The refusal arm is also where the refusal's own shape lives now** ([`refused_by_build`]): the
/// stdout it wrote and the residue it may not leave, per channel. That shape was
/// `crates/tinio-server/tests/serve_sandbox_channels.rs`'s in-process subject until the placement
/// cleanup retired the file, and it is asserted here because the arm is this step's dispatch.
#[then(expr = "the {string} request is answered, never ignored")]
async fn request_answered(world: &mut World, channel: String) {
    let channel = SandboxChannel::parse(&channel);
    let cli = cli(world);
    let run = cli.last();
    if cli
        .sandbox_flag
        .expect("the scenario probes `--sandbox` before it asserts the arm")
    {
        applied(run);
    } else {
        refused_by_build(cli, run, channel);
    }
}

/// Every refusal: the channel's own spelling, and no service.
fn refused(run: &Run, channel: SandboxChannel) {
    let status = run
        .status
        .expect("a refused run has exited — a run still serving was not refused");
    assert!(
        !status.success(),
        "a requested confinement is refused, never served unconfined:\nstdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr()
    );
    assert!(
        run.readiness.is_none(),
        "a refusal that then served is not a refusal:\nstdout:\n{}",
        run.stdout
    );
    assert!(
        run.stderr().contains(channel.spelling()),
        "the refusal names the channel that asked for it (`{}`):\n{}",
        channel.spelling(),
        run.stderr()
    );
    // **The `.env` arm's other half**: the file the value was read from is not a channel the
    // operator used, and a refusal that named it would send the reader after the wrong thing
    // (`resolve::apply_env_file` puts the value in the environment; the parse reports that).
    if let SandboxChannel::DotEnv = channel {
        assert!(
            !run.stderr().contains(ENV_FILE),
            "the refusal names the variable, never the file it was read from (`{ENV_FILE}`):\n{}",
            run.stderr()
        );
    }
}

/// The build's refusal: the feature is not in that binary, so the run cannot confine and says so
/// rather than serving unconfined. The flag is not in that parser at all — clap answers it, in
/// clap's words — while a channel that cannot be compiled away (the variable, its `.env` spelling,
/// and the configuration key) is refused by the crate itself: a usage error (2) naming the
/// **build**, and never the platform, because no target this suite runs on owes that refusal any
/// more.
///
/// **And the refusal's own shape, which is the half a reader of the wording alone cannot see**
/// (`design §12` bullet 4's wire contract; the subject
/// `crates/tinio-server/tests/serve_sandbox_channels.rs`'s `refused_or_honoured` held in-process
/// until the placement cleanup retired that file). The two arms' shapes differ because the layers
/// that answer do, and each is asserted where it is measured:
///
/// - **The parse refusal wrote nothing at all.** clap answers a flag its parser does not know
///   before any command is dispatched, so the serving path — stdout's only frame writer (decision
///   19) — is never reached, and neither is the resolution that would have minted
///   `<root>/.tinio/.tinio.toml`. What is asserted is both halves of that order: stdout empty, and
///   the root's reserved directory absent, because a run that had created *anything* would not be
///   a run the parser refused.
/// - **The crate refusal is framed, and stops short of the store.** The channel cannot be compiled
///   away, so the run dispatches and the serving arm renders the error it returns as one `error`
///   frame — [`refusal_frame_only`]'s three stdout lines and no fourth. The resolution has already
///   minted `.tinio.toml` on its way here (the document every first run creates), so the absence
///   that says "nothing served" is the store, `<root>/.tinio/meta.redb`, which is the serving
///   path's first write under the root.
fn refused_by_build(cli: &Cli, run: &Run, channel: SandboxChannel) {
    refused(run, channel);
    assert_eq!(
        run.status.and_then(|status| status.code()),
        Some(2),
        "a capability this build does not carry is a usage error:\n{}",
        run.stderr()
    );
    match channel {
        SandboxChannel::Flag => {
            assert!(
                run.stderr().contains("unexpected argument"),
                "with no `sandbox` feature the flag is not in the parser, so clap is what refuses \
                 it:\n{}",
                run.stderr()
            );
            assert!(
                run.stdout.is_empty(),
                "stdout is the serving path's protocol and no command has been dispatched, so a \
                 refusal answered by the parser writes no frame; got {:?}",
                run.stdout
            );
            let state_dir = cli.state_dir();
            assert!(
                !state_dir.exists(),
                "a parse refusal lands before the run creates anything it serves: {} exists",
                state_dir.display()
            );
        }
        SandboxChannel::Env | SandboxChannel::DotEnv | SandboxChannel::Config => {
            assert!(
                run.stderr().contains("no `sandbox` feature"),
                "the channel cannot be compiled away, so the crate names the build:\n{}",
                run.stderr()
            );
            assert!(
                !run.stderr().contains("platform"),
                "and not the platform — this is the build's half of the pair:\n{}",
                run.stderr()
            );
            refusal_frame_only(run);
            let store = cli.state_dir().join("meta.redb");
            assert!(
                !store.exists(),
                "a refusal lands before the run serves, and the store is what a serving run \
                 opens: {} exists",
                store.display()
            );
        }
    }
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
        run.stderr()
    );
    assert!(
        run.status.is_none(),
        "the run that serves is the one the scenario drives, not an exit: {:?}\nstderr:\n{}",
        run.status,
        run.stderr()
    );
    for refusal in ["asked for a confinement", "unexpected argument"] {
        assert!(
            !run.stderr().contains(refusal),
            "an honoured confinement carries no refusal ({refusal:?}):\n{}",
            run.stderr()
        );
    }
}

/// The publication half, per shape: a refusal leaves no `state` file, and a run that serves has
/// one — so the file tracks *serving* rather than the channel, and a refusal stays
/// distinguishable from a run.
///
/// **A serving run is given [`PUBLISH_SETTLE`] to produce the file and a refused one is given
/// nothing**, which is the whole of the allowance: the readiness frame and the file are written
/// by two processes on macOS (see that constant), while a refused run has exited and its absence
/// is final.
#[then("the root publishes a state file only when the run is serving")]
async fn state_file_tracks_serving(world: &mut World) {
    let cli = cli(world);
    let file = state_file(cli.root());
    let serving = cli.last().readiness.is_some();
    let settle = if serving {
        PUBLISH_SETTLE
    } else {
        Duration::ZERO
    };
    let deadline = Instant::now() + settle;
    let published = loop {
        if file.is_file() {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        sleep(POLL).await;
    };
    assert_eq!(
        published,
        serving,
        "`state` is the serving process's record: published={published}, serving={serving}\n\
         stderr:\n{}",
        cli.last().stderr()
    );
}

/// Nothing accepts on the endpoint the run reported — the half that says the *process* which
/// served on it is gone, and not merely that the run ended.
///
/// **The listener belongs to the serving child on a launcher arm**, which is what makes this an
/// answer the run's own exit cannot give: the launcher binds it and hands the descriptor over
/// (design decision 16), so a confined child that outlived its launcher would still accept on
/// its own copy. With the run ended, a refused connection is that copy being closed with the
/// child. On an in-place arm the same sentence is the direct one: the process that accepted *is*
/// the process the scenario spawned.
///
/// **The child's socket is already closed when a scenario asks**: `Child::abort` kills and reaps
/// in one call (`crates/tinio-sandbox-core/src/launcher/supervise.rs`), so the run's ending and
/// the child's are the same instant. What the window is for is the *probe*, not the product —
/// [`ENDPOINT_GONE_WINDOW`] and [`PROBE_CONNECT_TIMEOUT`] carry the measurement.
///
/// **Only a refusal is the finding, and the difference matters**: `connect_timeout` rather than
/// `connect`, because a probe that waited on the OS's own connect bound would turn a firewall
/// into a slow pass, and a connect that times out or fails some other way says nothing about the
/// listener — so it is retried inside the same window ([`PROBE_CONNECT_TIMEOUT`] bounds one
/// attempt) and quoted when the window closes, never read as "gone".
#[then("the endpoint the run reported stops accepting")]
async fn endpoint_stops_accepting(world: &mut World) {
    let endpoint = cli(world)
        .endpoint
        .clone()
        .expect("the run reported an endpoint for this step to probe");
    let address: SocketAddr = endpoint
        .parse()
        .unwrap_or_else(|err| panic!("the run's endpoint `{endpoint}` is an address: {err}"));
    let deadline = Instant::now() + ENDPOINT_GONE_WINDOW;
    loop {
        let seen = match TcpStream::connect_timeout(&address, PROBE_CONNECT_TIMEOUT) {
            // The listener is gone: nothing completes the handshake, so the kernel refuses.
            Err(err) if err.kind() == ErrorKind::ConnectionRefused => return,
            Ok(_) => "a connection was accepted".to_owned(),
            Err(err) => err.to_string(),
        };
        assert!(
            Instant::now() < deadline,
            "probing the endpoint the run reported ({endpoint}) {ENDPOINT_GONE_WINDOW:?} after \
             the run ended did not find it refused: {seen}"
        );
        sleep(POLL).await;
    }
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

// --- The confinement's witness, identity and contracts -------------------

/// **Was this run really confined?** — the one question a reader of a stream cannot answer, asked
/// of two carriers because either alone answers something weaker.
///
/// **The first carrier is the product's own record** ([`CONFINED_STEPS`]): the events the
/// confinement emits about itself, one per platform, with the fields that make each one that event
/// rather than a sentence that mentions it. On the launcher arms this is the only record of the
/// launch; on the in-place arm the fmt-only bootstrap the CLI installs before parsing is what
/// makes the backend's own events reach stderr (design decision 26).
///
/// **The second carrier is independent of that record, one per platform, and it is the half that
/// keeps this step from being a self-report.** A run that served without confining anything would
/// still print the first carrier's lines if the branch that emits them ran, which is exactly what
/// `request_answered`'s own comment warns about; what the cross-check adds is an answer from the
/// kernel or the file system instead — Linux: the process's own root, plus a path outside every
/// exposure that is *not* there when seen through it; Windows: the package SID's ACE read back off
/// the served root by `icacls`; macOS: the kernel's process table for the pid the launcher reported
/// spawning, which is identity rather than enforcement — [`macos_witness`] states that waiver in
/// full, and the enforcement itself is the crate suites' and the gate script's. The step's text
/// stays platform-neutral because the scenario is; which arm runs is
/// `tinio_sandbox::platform_facts`'s answer, and it lives here.
#[then("the run was confined")]
async fn run_was_confined(world: &mut World) {
    let cli = cli(world);
    let run = cli.last();
    witness_record(run);
    witness_cross_check(cli);
}

/// The first carrier: every event the platform's confinement owes, each one line, each carrying
/// its own fields.
///
/// **`step` is the contract and the line is not** — the rule `step_line` states: a leg that
/// matched the whole line would pin a timestamp, a level and a field order that belong to whatever
/// subscriber the process installed.
///
/// **The line is [`step_line_with_fields`]'s choice, and its doc carries the argument**: the first
/// line carrying every field this platform's table names, which is the same selection the
/// scenario-driven `its stderr carries a … step line naming` step makes. A first-match reader
/// would be correct today only because the banner is emitted first and carries the three this
/// table names, while the warning that shares the name is emitted last and is one it could never
/// reach (G17's 15 s fact).
fn witness_record(run: &Run) {
    for (step, fields) in CONFINED_STEPS {
        step_line_with_fields(&run.stderr(), step, fields);
    }
}

/// The second carrier, per platform: the kernel's or the file system's answer about the process
/// this scenario spawned, never the product's own sentence about itself.
fn witness_cross_check(cli: &Cli) {
    #[cfg(target_os = "linux")]
    linux_witness(cli);
    #[cfg(target_os = "macos")]
    macos_witness(cli);
    #[cfg(target_os = "windows")]
    windows_witness(cli);
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = cli;
        panic!("no confinement backend is wired for this target, so no run can be witnessed");
    }
}

/// The outside probe both Linux arms are written on: the control, the fixture and the read-back.
///
/// **The control is what keeps either polarity from passing vacuously**: the served root has to
/// resolve through the process's own root, or the pid is gone (or that root is unreadable) and
/// "the path outside is not visible" would be a statement about a process that can see nothing at
/// all. The fixture is `{tag}-{pid}.txt` under the scenario's own scratch directory — a sibling of
/// the served root, which the exposure set (the served root and the state directory,
/// `crate::sandbox::product`'s `exposures`) does not cover.
///
/// **`/proc/<pid>/root` is the one probe that can see a jail from outside it**
/// (`crates/tinio-server/tests/sandbox_e2e.rs` uses it for the read-only seam and says why): a path
/// resolved through it is resolved in the *target's* mount namespace, which is exactly the
/// difference a jail makes. **The root's own path is not the probe, and that is measured rather
/// than argued.** `readlink /proc/<pid>/root` answers `/` for a confined process here (WSL2,
/// 2026-09-30): the jail is a mount inside the process's own mount namespace, and the kernel
/// renders a root the *reader* cannot reach in its own namespace as the reader's own `/`. The path
/// resolved *through* the link has no such trouble — the walk happens in the target's namespace —
/// which is why this reads files rather than a link.
///
/// The path it probed travels back beside what reading it produced, because each arm's assertion
/// is its own: a confined run must not see the file and an unconfined one must, and the two differ
/// in their message and in the fixture body as well as in polarity.
#[cfg(target_os = "linux")]
fn outside_probe(cli: &Cli, tag: &str, body: &[u8]) -> (PathBuf, io::Result<String>) {
    let pid = cli.last().spawned;
    let served = through_root(pid, cli.root());
    assert!(
        served.is_dir(),
        "the served root is not reachable through the process's own root at {}: the pid this \
         scenario spawned ({pid}) is gone, or its root is unreadable, so this probe cannot answer",
        served.display()
    );
    let outside = cli.scratch().join(format!("{tag}-{pid}.txt"));
    fs::write(&outside, body).expect("the outside fixture");
    let path = through_root(pid, &outside);
    let seen = fs::read_to_string(&path);
    (path, seen)
}

/// The Linux cross-check: **the confined process's own root**, probed by resolving a path through
/// it — the served root has to be there, and a path no exposure covers must not be.
///
/// The probe is [`outside_probe`]'s, control and fixture included; what stays here is the polarity
/// a confined run owes: the path outside every exposure is **not** visible through the process's
/// own root. A run that confined nothing has the host's root, and this harness's own scratch file
/// is then right where it was planted.
#[cfg(target_os = "linux")]
fn linux_witness(cli: &Cli) {
    let (path, seen) = outside_probe(cli, "witness", b"a path outside every exposure\n");
    assert!(
        matches!(&seen, Err(err) if err.kind() == ErrorKind::NotFound),
        "a path the run never exposed is visible through the process's own root: {} answered {:?}",
        path.display(),
        seen.map(|text| text.len())
    );
}

/// The Windows cross-check: **the package SID's ACE, read back off the served root by `icacls`** —
/// the grant the launcher made, verified by an oracle that is not the launcher.
///
/// The SID comes from the run's `grant the ACLs` event ([`granted_sid`]); the read is the shared
/// device (`tinio_sandbox::windows::testing`, the one `crates/tinio-sandbox-windows/src/testing.rs`
/// keeps as "the suites' independent oracle"), because the API that *wrote* the ACE must not be the
/// thing that reports it — and the shared `icacls` is also what refuses a read that failed, so a
/// DACL nothing could look at cannot read as a missing ACE. What is witnessed is that the ACE is on
/// the served root *now* — the standing grant the confined child opens its root through — with a
/// SID only this launch's profile could have produced.
#[cfg(target_os = "windows")]
fn windows_witness(cli: &Cli) {
    let sid = granted_sid(cli.last());
    let listing = testing::icacls(cli.root(), &[]);
    assert!(
        listing.contains(&sid),
        "the package SID `{sid}` the launcher reported granting is on no ACE of the served root \
         `{}`; `icacls` lists:\n{listing}",
        cli.root().display()
    );
}

/// The package SID the launcher reported granting — read for the two steps that need it, the
/// witness's `icacls` cross-check and the fixture grant P5 makes, so the event, the field and the
/// failure text have one home.
///
/// **From the launcher's own report, never re-derived here**: a `PackageSid` built in *this*
/// process would create — and, on its `Drop`, delete — the very profile the launcher is using,
/// which is the trap `crates/tinio-server/tests/support/windows_e2e_harness.rs` records.
#[cfg(windows)]
fn granted_sid(run: &Run) -> String {
    let line = step_line(&run.stderr(), "grant the ACLs")
        .expect("the launcher logged no `grant the ACLs` event, so no SID was reported");
    field_of(&line, "sid")
        .unwrap_or_else(|| panic!("the launcher's grant event carries no `sid=` field: {line}"))
}

/// The macOS cross-check: **the kernel's process table**, for the pid the launcher reported
/// spawning — the launcher's own record checked against the only other thing that knows a process
/// exists.
///
/// **Why the pid is the fact here.** macOS confines by launching: the launcher renders a profile,
/// proves it, and spawns a child that lives in it, so what this arm can establish is that a
/// *distinct* process really is running under that launch — the pid from the `spawn` event, and
/// whose child it is, read from the kernel rather than from the sentence that reported it.
///
/// **What this arm does *not* prove, stated rather than implied: the enforcement.** A live child
/// whose parent is the launcher exists whether or not the profile it was spawned under refuses
/// anything, so **the macOS witness in this suite is mechanism-plus-identity by construction** —
/// carrier 1's `step="mechanism"`/`step="confine"`/`step="spawn"` events plus this identity check —
/// and it is deliberately not a refusal probe. Two facts make that the right shape rather than a
/// gap to close here: `plans/2026-09-29-sandbox-bdd-e2e.md` §1 assigns "Seatbelt refusals and
/// profile text" to the Rust-side crate suites, which is where the enforcement half lives
/// (`crates/tinio-server/tests/sandbox_macos_e2e.rs`'s four `step=` assertions and the profile
/// text it reads, plus the gate script's zero-unexpected-refusal assertion), and the existing
/// macOS leg's own evidence for a confinement is exactly this shape. A scenario that needs the
/// enforcement asserted on macOS should extend that leg, not this step.
#[cfg(target_os = "macos")]
fn macos_witness(cli: &Cli) {
    let run = cli.last();
    let line = step_line(&run.stderr(), "spawn")
        .expect("the launcher logged no `spawn` event, so no pid was reported");
    let pid: u32 = field_of(&line, "pid")
        .unwrap_or_else(|| panic!("the launcher's `spawn` event carries no `pid=` field: {line}"))
        .parse()
        .unwrap_or_else(|err| panic!("the launcher's `spawn` pid is not a number: {err}"));
    let mut command = Command::new("/bin/ps");
    command.args(["-o", "ppid=", "-p", &pid.to_string()]);
    let output = job::output(&mut command)
        .unwrap_or_else(|err| panic!("could not read the process table for {pid}: {err}"));
    let ppid: u32 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or_else(|err| {
            panic!(
                "the pid the launcher reported spawning ({pid}) is not in the process table: {err}"
            )
        });
    assert_eq!(
        ppid, run.spawned,
        "the process the launcher reported spawning ({pid}) is not a child of the launcher this \
         scenario spawned ({}): the launch is not the one this run made",
        run.spawned
    );
}

/// **Was this run left unconfined?** — [`run_was_confined`]'s mirror, asked of the same two
/// carriers because either alone answers something weaker, and *this* direction is the one a
/// self-report cannot answer at all: "nothing says it confined" is exactly what a silent failure
/// looks like, so the platform's own cross-check is asked the other way round beside it.
///
/// **The first carrier is the product's own record, absent**: every event the platform's
/// confinement emits about itself ([`CONFINED_STEPS`]) must be missing from the run's stderr, and a
/// line that *is* there fails the step — a run that confined carries it whatever the scenario
/// asked for. Read alone this is a self-report's absence, which is why it is never the whole
/// answer here.
///
/// **The second carrier is the platform's own device, read negative**, one per platform: Linux —
/// the process's own root is the *host's*, so a path outside every exposure is readable through
/// `/proc/<pid>/root` where the positive arm's probe answers `NotFound`; Windows —
/// [`testing::icacls`] on the served root lists no package SID's ACE, which is what a confined
/// launch's grant creates (and the shared device refuses a read that failed, so "no ACE" cannot be
/// read off a DACL nothing looked at);
/// macOS — the kernel's process table holds no child of this run, where a confined run *is* a
/// launcher with one. Each arm keeps the positive step's control, so a process that is gone cannot
/// pass by being unreadable.
///
/// **The pair is what makes the two halves fail either way**: a run that confined fails the record
/// check, a run whose jail is in place fails the cross-check, and only a run that really served
/// unconfined passes both.
#[then("the run was not confined")]
async fn run_was_not_confined(world: &mut World) {
    let cli = cli(world);
    let run = cli.last();
    no_confinement_record(run);
    unconfined_cross_check(cli);
}

/// The first carrier, absent: no event of this platform's confinement on the run's record.
fn no_confinement_record(run: &Run) {
    for (step, _fields) in CONFINED_STEPS {
        if let Some(line) = step_line(&run.stderr(), step) {
            panic!(
                "the run's own record carries a `step=\"{step}\"` event, so this run confined \
                 rather than serving unconfined: {line}"
            );
        }
    }
}

/// The second carrier, per platform: the device [`witness_cross_check`] asks, for the run that
/// did not confine.
fn unconfined_cross_check(cli: &Cli) {
    #[cfg(target_os = "linux")]
    linux_unconfined(cli);
    #[cfg(target_os = "macos")]
    macos_unconfined(cli);
    #[cfg(target_os = "windows")]
    windows_unconfined(cli);
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = cli;
        panic!("no confinement backend is wired for this target, so no run can be witnessed");
    }
}

/// The Linux cross-check for the unconfined arm: **the process's own root is the host's**, so a
/// path no exposure covers is readable through `/proc/<pid>/root` — the positive arm's negative
/// probe, answered the other way.
///
/// The probe is [`outside_probe`]'s — the control included, which is what keeps this from passing
/// vacuously: the served root resolves through the link, so a pid that is gone (or a root that
/// cannot be read at all) fails there rather than looking like "no jail". What stays here is the
/// polarity an unconfined run owes, and it is the positive arm's negative probe answered the other
/// way.
#[cfg(target_os = "linux")]
fn linux_unconfined(cli: &Cli) {
    let (path, seen) = outside_probe(cli, "unconfined", b"a path this run never exposed\n");
    assert!(
        seen.is_ok(),
        "a path outside every exposure is not readable through the process's own root ({} \
         answered {:?}) while the served root resolves through it: the process cannot see the \
         host's tree, so a jail is in place and this run confined",
        path.display(),
        seen.as_ref().err().map(|err| err.kind())
    );
}

/// The Windows cross-check for the unconfined arm: **no package SID's ACE on the served root** —
/// the launcher's own device (`icacls`, this suite's independent oracle) asked the other way
/// round, because that ACE is what a confined launch's grant creates and nothing else on this
/// oracle is.
#[cfg(target_os = "windows")]
fn windows_unconfined(cli: &Cli) {
    let listing = testing::icacls(cli.root(), &[]);
    assert!(
        !listing.contains("S-1-15-2-"),
        "the served root carries an ACE for a package SID, which only a confined launch's grant \
         creates: this run confined rather than serving unconfined; `icacls` lists:\n{listing}"
    );
}

/// The macOS cross-check for the unconfined arm: **the kernel's process table holds no child of
/// this run** — the positive arm's identity question, mirrored, since a confinement here *is* a
/// launch and the launcher's child is the process that serves.
#[cfg(target_os = "macos")]
fn macos_unconfined(cli: &Cli) {
    let pid = cli.last().spawned;
    let children = children_of(pid);
    assert!(
        children.is_empty(),
        "the process this scenario spawned ({pid}) has children ({children:?}): a confinement on \
         this platform is a launcher with a confined child, so this run confined rather than \
         serving unconfined"
    );
}

/// Every process whose parent is `pid`, from the kernel's own table — `ps -axo pid=,ppid=`, one
/// line per process with no header and both columns bare.
#[cfg(target_os = "macos")]
fn children_of(pid: u32) -> Vec<u32> {
    let mut command = Command::new("/bin/ps");
    command.args(["-axo", "pid=,ppid="]);
    let output = job::output(&mut command)
        .unwrap_or_else(|err| panic!("could not read the process table: {err}"));
    let table = String::from_utf8_lossy(&output.stdout).into_owned();
    table
        .lines()
        .filter_map(|line| {
            let mut columns = line.split_whitespace();
            let child: u32 = columns.next()?.parse().ok()?;
            let parent: u32 = columns.next()?.parse().ok()?;
            (parent == pid).then_some(child)
        })
        .collect()
}

/// The pid the `state` file records, held to the process this scenario spawned.
///
/// **Which process that is differs by arm, and the assertion does not.** An in-place backend
/// confines *this* process, so the file names the server itself; a launcher backend publishes the
/// file from the launcher, and `State::with_token`'s pid is the publishing process's — the
/// launcher, which is what this harness spawned either way. The two arms therefore answer the same
/// pid, and the difference is the *shape* the file records, not the value.
///
/// **The launcher/child split is the launcher arms' alone.** On the in-place arm the spawned
/// process has no descendant at all — there is no second process to be the server — which is the
/// half P1 states as "the confined process is the spawned one"; it is asked only where
/// `platform_facts().launches_child` is false, because on the other arm a descendant is exactly
/// what a confinement is.
#[then("the state file names the spawned process")]
async fn state_names_the_spawned_process(world: &mut World) {
    let cli = cli(world);
    let run = cli.last();
    let state = published_state(cli, run);
    assert_eq!(
        state.pid, run.spawned,
        "`state` names the process that published it, which is the one this scenario spawned"
    );
    #[cfg(target_os = "linux")]
    linux_has_no_descendants(run.spawned);
}

/// The in-place arm's shape: the spawned process **is** the server, so the process table holds no
/// child of it.
///
/// The scan is `/proc/<n>/stat`'s parent field over every process, which is the whole table rather
/// than the one file some kernels keep a children list in: a *process* is what is being looked
/// for, and a runtime's threads share their pid's entry, so they are not candidates. A child here
/// would mean something else was launched to do the serving, which is the launcher arm's shape and
/// not this one's.
#[cfg(target_os = "linux")]
fn linux_has_no_descendants(pid: u32) {
    let mut children = Vec::new();
    for entry in fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(other) = name.parse::<u32>() else {
            continue;
        };
        if other == pid {
            continue;
        }
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        // `comm` may contain spaces and parentheses, so what follows the *last* `)` is what is
        // read: `state` is the first field there and `ppid` the second (`proc(5)`).
        let Some((_, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        if rest.split_whitespace().nth(1).and_then(|f| f.parse().ok()) == Some(pid) {
            children.push(other);
        }
    }
    assert!(
        children.is_empty(),
        "the process this scenario spawned has descendants ({children:?}): the confined process \
         must be the spawned one, with no child doing the serving"
    );
}

/// **A5's secret-leak rule, asked of the streams a real run writes**: the control token the run
/// published reaches neither the frame stream (stdout) nor the operational log (stderr).
///
/// **The in-process leg this replaces could not ask it, which is why the scenario spawns.** The
/// re-emit's own unit leg (`crates/tinio-cli/src/sandbox/tests.rs`) drives the emitter against a
/// recording, because fd 1 and fd 2 belong to a test harness from the inside — so the token it
/// asserts against is one it invented, and nothing could falsify that. Here the needle is the
/// **run's own published `state`**, read through [`published_state`] (whose doc carries the
/// redaction rule the parse exists for): `tinio-api`'s `generate_token` minted that value inside
/// the run and nowhere else, so a failure reports "the run printed its own token" and not "the run
/// printed something that happens to be this test's string".
///
/// **Both carriers, because A5 names both, and both are asserted present before the token is
/// looked for.** stdout is the frame stream (the readiness frame, and every refusal) and stderr is
/// where this platform's subscriber writes the log — the in-place re-emit line among it, the line
/// A5 added, which is what makes the negative probe a statement about a stream that carried the
/// line rather than about an empty buffer. The scenario asserts the same two facts with its own
/// steps (`the readiness line reports a bound port`, `the run was confined`); they are repeated
/// here because an absence assertion is only as good as the buffer it is made against, and a step
/// stands on its own. `NO_COLOR`, which makes the re-emit line greppable, is the harness's own
/// ([`Cli::command`]).
///
/// **The two failures do not quote the stream they are about.** A run that leaked its token would
/// otherwise put the value in the CI log on the way to reporting the leak — the shape STD-01 is
/// the precedent for — so the offset is named instead of the bytes around it.
///
/// `@linux` is the scenario's tag and this step's gate, the rule every platform-bound step in this
/// module follows (`I send the server SIGTERM`): the in-place arm is the only one whose own
/// process writes these streams — a launcher arm's log is the launcher's relay.
#[cfg(target_os = "linux")]
#[then("neither stream carries the run's published control token")]
async fn neither_stream_carries_the_token(world: &mut World) {
    let cli = cli(world);
    let run = cli.last();
    let token = published_state(cli, run)
        .token
        .expose_secret()
        .as_str()
        .to_owned();
    let stdout = &run.stdout;
    let stderr = run.stderr();
    assert!(
        stdout.contains("event: listening"),
        "the frame stream must carry the readiness frame this step reads it for:\n{stdout}"
    );
    assert!(
        stderr.contains("step=\"model\""),
        "the log must carry the in-place re-emit's §7 events, or the token check beside this one \
         would be about a stream that never carried the record A5 is about:\n{stderr}"
    );
    assert!(
        !stdout.contains(&token),
        "the frame stream carries the run's own control token, at byte {} of {}: the stream is not \
         quoted because the value is the credential this step exists to keep out of a log",
        stdout.find(&token).expect("the assertion above found it"),
        stdout.len()
    );
    assert!(
        !stderr.contains(&token),
        "the operational log carries the run's own control token, at byte {} of {}: the log is not \
         quoted, for the reason the frame stream's failure is not",
        stderr.find(&token).expect("the assertion above found it"),
        stderr.len()
    );
}

/// One named `step="…"` line of the captured stderr, and the fields the caller requires of it.
///
/// **The docstring is the contract for that event**, one requirement per line: `field` asserts the
/// event carries it, and `field=value` pins its value. That is what lets one step serve every fact
/// of a platform's operator contract: the scenario names the event and the fields it is owed, and
/// the step does not have to know which event it is looking at.
///
/// **The line chosen is [`step_line_with_fields`]'s**, the first one carrying *every* field the
/// scenario named — the selection `witness_record` makes for a platform's table, and the one that
/// makes G17's "the 15 s grant-phase warning" expressible at all: a docstring naming `elapsed_ms`
/// and `threshold_ms` lands on the warning, because the banner does not carry them.
///
/// **`NO_COLOR` is what makes the line readable and the harness sets it** (`Cli::command`): the
/// text layer styles a field's *name* whenever ANSI is on, so a run with colour logs the same
/// events yet matches none of them — the measured defect `sandbox-shared.md` §7's ANSI bullet
/// records, and that section owns the rule.
#[then(expr = "its stderr carries a {string} step line naming")]
async fn stderr_carries_step_line(world: &mut World, event: String, step: &Step) {
    let run = cli(world).last();
    let wanted: Vec<(String, Option<String>)> = docstring_lines(step)
        .iter()
        .map(|line| {
            let (name, value) = requirement(line);
            (name.to_owned(), value.map(str::to_owned))
        })
        .collect();
    let names: Vec<&str> = wanted.iter().map(|(name, _)| name.as_str()).collect();
    let line = step_line_with_fields(&run.stderr(), &event, &names);
    for (name, value) in &wanted {
        let found = field_of(&line, name).expect("the chosen line carries every wanted field");
        if let Some(value) = value {
            assert_eq!(&found, value, "the `{event}` line's `{name}` field: {line}");
        }
    }
}

/// The SID the launcher just granted, re-applied to a fixture **outside** the served root — the
/// fixture P5's escaping junction needs, whose whole premise is that the container can reach the
/// out-of-root target before the run that reads it.
///
/// The SID is [`granted_sid`]'s, the grant itself is `acl::grant_spec`'s — the launcher's own
/// spelling of the string — so the fixture is granted what the launcher granted and not a mask
/// this step invented.
///
/// A relative path is the scenario's own scratch directory ([`expand`]); a directory that is not
/// there is made, because a target outside the root has no product to create it.
#[cfg(windows)]
#[when(expr = "the launcher's package SID is granted to {string}")]
async fn package_sid_granted(world: &mut World, target: String) {
    use tinio_sandbox::{AccessMode, windows::acl};

    let cli = cli(world);
    let path = expand(cli, &target);
    if !path.is_dir() {
        fs::create_dir_all(&path).expect("the fixture outside the served root");
    }
    let sid = granted_sid(cli.last());
    let spec = acl::grant_spec(&sid, acl::Target::Directory, AccessMode::ReadOnly);
    // The grant goes through the shared device, which is the one place the exit status is checked
    // — a refused write is a failure here rather than a step that quietly asserted nothing.
    testing::icacls(&path, &["/grant:r", &spec, "/q"]);
}

/// The lines a file **under the served root** holds, counted by the text they carry — the
/// assertion a scenario makes about a file the run wrote inside its own root.
///
/// **The path stays the scenario's to spell**: this step joins the relative path onto the run's
/// root and reads it, so what it asserts about is the file the run wrote rather than anything of
/// this suite's own.
///
/// **A count of lines and not a substring of the file, because that is the fact being asserted**:
/// "one line containing `…`" and "no second line containing it" are one assertion — a count that
/// grew is a write the scenario did not ask for.
#[then(expr = "the file {string} under the served root has {int} line(s) containing {string}")]
async fn rooted_file_has_lines(world: &mut World, relative: String, count: usize, text: String) {
    let path = cli(world).root().join(&relative);
    let contents = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading `{}`: {err}", path.display()));
    let found = contents.lines().filter(|line| line.contains(&text)).count();
    assert_eq!(
        found,
        count,
        "`{}` holds {found} line(s) containing {text:?}, not {count}:\n{contents}",
        path.display()
    );
}

/// `GET /metrics` over the control channel the running server's `state` file names.
///
/// **The control channel, not the S3 port**: the management plane serves `/metrics`, and the
/// endpoint is the one `state.control_name` records — read, never re-derived, exactly as
/// `tinio status` reads it. The exposition is kept on the harness so the assertion step beside
/// this one reads the same bytes this scrape did.
#[when("I read the metrics from the control channel")]
async fn read_the_metrics(world: &mut World) {
    let cli = cli(world);
    let exposition = scrape(cli).await;
    cli.metrics = Some(exposition);
}

/// The sample lines the last scrape must carry, one per docstring line, whole — so a family that
/// grew a label, lost its value or stopped being exposed fails here rather than being read out of
/// a prefix.
#[then(expr = "the server's metrics carry")]
async fn metrics_carry(world: &mut World, step: &Step) {
    let cli = cli(world);
    let exposition = cli
        .metrics
        .as_deref()
        .expect("the scenario read the metrics before it asserted one");
    for wanted in docstring_lines(step) {
        assert!(
            exposition.contains(&wanted),
            "the exposition carries no `{wanted}` line:\n{exposition}"
        );
    }
}

// --- The confined journey's fixtures (G8/G9) -----------------------------

/// The credential pair **this suite signs with**, on the child — the fixture a signed journey
/// against the spawned binary cannot do without.
///
/// **Why a fixture at all.** The run serves with what `[auth]` says, and the pair the serving
/// chain resolves for an absent file is one `discover::load_or_create` **generates** at random
/// (`Config::generated` + `credentials::generate`) — so a request this harness signed with its own
/// pair would be answered `InvalidAccessKeyId`/`SignatureDoesNotMatch` about a server the scenario
/// had just started, and nothing the harness can read before the run exists would fix that.
/// `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` are the chain that closes the gap: they are the
/// environment's level of the credential fold (`resolve::fold_credentials`), and the fold runs in
/// the **confined child** too — the child inherits the launcher's environment and folds it onto
/// the payload's text (`resolve::server_confined` is `server`'s own body), so the pair reaches the
/// process that serves on all three arms. `steps/clients.rs` pins the same two variables on the
/// server the external clients are pointed at, for the same reason and with the same problem
/// behind it.
///
/// **The value is [`ACL_ROOT`]'s, and that is a decision rather than a coincidence.** It is the
/// one root credential pair this suite configures, and the signed-request steps in `steps/acl.rs`
/// know it under the user name `"root"` — "the configured `[auth]` pair presenting the `[owner]`
/// element". Pinning it here is what lets the journey drive the spawned binary with `signed as
/// "root"` and **without a second set of signed-request steps**: the pair is supplied by the
/// harness rather than re-derived by the step layer, exactly the way the harness supplies
/// `TINIO_STATE_DIR` and `NO_COLOR`.
///
/// Replaced rather than pushed, like [`SandboxChannel::ask`]'s variable arm: a scenario that named
/// either variable itself owns the value, and a second entry would make the last one to run the
/// child's answer.
#[given("the child holds the harness's credentials")]
async fn harness_credentials(world: &mut World) {
    let cli = cli(world);
    cli.vars
        .retain(|(name, _)| name != ACCESS_KEY_VAR && name != SECRET_KEY_VAR);
    cli.vars
        .push((ACCESS_KEY_VAR.to_owned(), ACL_ROOT.0.to_owned()));
    cli.vars
        .push((SECRET_KEY_VAR.to_owned(), ACL_ROOT.1.to_owned()));
}

/// The harness's client, bound to the endpoint the run reported — the step that makes a `@cli`
/// run drivable by the wire steps every other feature uses.
///
/// **The address is the readiness event's typed one and never a re-derivation**: `--port 0` binds
/// an ephemeral port, and `Run::readiness` (the `listening` frame's `SocketAddr`) is the only
/// record of which one. The `#[before]` hook cannot do this for a `@cli` scenario the way it does
/// for the in-process ones — the server here is a separate process, and its address is unknown
/// until it says so — which is why the binding is the scenario's own step.
///
/// A run that never reported readiness is a refusal or a hang, and both are reported with the
/// streams rather than as a missing field: the panic quotes what the run said on the way out,
/// which is the operator-facing half of "no endpoint".
#[given("the client is bound to the run's endpoint")]
#[when("the client is bound to the run's endpoint")]
async fn client_is_bound(world: &mut World) {
    let address = {
        let run = cli(world).last();
        run.readiness.unwrap_or_else(|| {
            panic!(
                "the run reported no endpoint to bind the client to — a run that refused or never \
                 became ready has none\nstdout:\n{}\nstderr:\n{}",
                run.stdout,
                run.stderr()
            )
        })
    };
    world.client.bind(address);
}

/// A bucket and one object planted **on disk** in the run's served root, before the run starts —
/// what a read-only journey needs, because the mode refuses the very writes that would build the
/// fixture over the wire (`read_only.feature` plants its fixtures for the same reason).
///
/// **Not the in-process `the served root holds bucket …` step**, which writes into the tempdir a
/// `#[before]`-spawned server serves and therefore has nothing to write into here: a `@cli`
/// scenario's root belongs to [`Cli`], and no server of this process serves it. The two sentences
/// differ by the one word that names which root they mean.
#[given(expr = "the root holds bucket {string} with object {string} and body {string}")]
async fn root_holds(world: &mut World, bucket: String, key: String, body: String) {
    let root = cli(world).root().to_path_buf();
    let path = root.join(&bucket).join(&key);
    fs::create_dir_all(path.parent().expect("a key has a parent")).expect("the planted bucket");
    fs::write(&path, body.as_bytes()).expect("the planted object");
}

/// **The disk half of a refusal**: the path the refused write named holds no file.
///
/// The status a client reads and the bytes the served root holds are two answers, and only the
/// first is what a client sees — a server that answered `AccessDenied` and wrote anyway would pass
/// on the status alone. The second is the assertion
/// `crates/tinio-sandbox-windows/tests/confine.rs`'s
/// `a_read_only_root_refuses_writes_and_still_reads` carried until the 2026-10-01 placement cleanup
/// moved the journey here (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`); the
/// path is the served root's own, because a `@cli` scenario's root belongs to [`Cli`].
#[then(expr = "the served root holds no file {string}")]
async fn root_holds_no_file(world: &mut World, rel: String) {
    let path = cli(world).root().join(&rel);
    assert!(
        !path.exists(),
        "the refused write left `{}` on the disk",
        path.display()
    );
}

// --- The exposure-set and state-dir refusals (G15/G16) -------------------

/// A file inside the served root, for the row that **serves it as a root**.
///
/// **`resolve_root` cannot be the refusal this fixture is for**: it canonicalises whatever it is
/// handed, and a file canonicalises like a directory (`resolve::resolve_root`'s contract), so the
/// run reaches the layers that have to *expose* the path — the Windows launcher's whitelist
/// (`crates/tinio-sandbox-windows/src/backend/exposures/whitelist.rs`, `NOT_A_DIRECTORY`) and the
/// shared storage open (`storage root is not a directory`) — both of which refuse it loudly.
/// Read-only is the mode the row runs in, because a read-write run resolves `<file>/.tinio` first
/// and the configuration layer's own error is what lands there.
#[given(expr = "the root holds a file {string}")]
async fn root_holds_a_file(world: &mut World, name: String) {
    let root = cli(world).root().to_path_buf();
    let path = root.join(&name);
    fs::write(&path, b"a file, not a directory\n")
        .unwrap_or_else(|err| panic!("the file fixture `{}`: {err}", path.display()));
}

/// The state directory that **holds** the served root — the mirror half of
/// `crates/tinio-cli/src/sandbox/product.rs`'s containment rule, which is the direction the served root's own
/// parent is: the run's state directory, one level above the root.
///
/// **Named rather than spelled by the scenario**, because the harness's scratch path is not
/// something a `.feature` file can spell: the value is [`Cli::scratch`], which *is*
/// [`Cli::root`]'s parent by construction.
#[given("the child's state directory holds the served root")]
async fn state_dir_holds_the_root(world: &mut World) {
    let cli = cli(world);
    let parent = cli
        .root()
        .parent()
        .expect("the scratch directory is the served root's parent")
        .to_path_buf();
    cli.vars
        .push((STATE_DIR_VAR.to_owned(), parent.display().to_string()));
}

/// The state directory **inside** the served root — the same rule's other direction, and the pair
/// the guard refuses for a **read-only** run alone (a read-write run's state directory *is*
/// `<root>/.tinio`, which must be served rather than refused).
///
/// The directory the value names is the reserved one, so the run that would write it is the run
/// whose configuration file lands under the root — which is the layer that answers first when the
/// file is not there yet (`tinio_cli::config::discover::load_or_create`).
#[given("the child's state directory is inside the served root")]
async fn state_dir_inside_the_root(world: &mut World) {
    let cli = cli(world);
    let inside = cli.root().join(".tinio");
    cli.vars
        .push((STATE_DIR_VAR.to_owned(), inside.display().to_string()));
}

/// The same in-root state directory, **spelled the root's other way round**: `<scratch>/ROOT/state`
/// against `<scratch>/root` — one directory on a volume that folds case, two on one that does not.
///
/// **This is the spelling the shared containment rule exists for.** A state directory spelled in the
/// other case once slipped past both guards and got exactly the read-write entry the read-only mode
/// refuses; `tinio_util::path::contains_under_any_spelling` (the configuration layer's guard and the
/// launcher's both call it) is what closed that, and this fixture is the only place a scenario asks
/// the rule about a *spelling* rather than about a path: the directory it names is the root's own
/// parent's `ROOT` child, so on Windows the run's state directory **is** `<root>/state` — inside the
/// served root — while on a case-sensitive volume it is a separate tree beside the root.
///
/// **`@windows` is the scenario's tag, not this step's**: the fold is the volume's, and off Windows
/// the row would assert a containment refusal about a path that is not contained.
#[given("the child's state directory is inside the served root under its other case")]
async fn state_dir_inside_the_root_other_case(world: &mut World) {
    let cli = cli(world);
    let folded = cli
        .root()
        .parent()
        .expect("the scratch directory is the served root's parent")
        .join("ROOT")
        .join("state");
    cli.vars
        .push((STATE_DIR_VAR.to_owned(), folded.display().to_string()));
}

/// The channel an operator **has not named**: no `TINIO_STATE_DIR` on the child, so the
/// resolution derives `<home>/.tinio/roots/<hash>/` instead
/// (`StateDirSource`'s `Derived`, the half macOS's read-only guard refuses — the enum and the
/// guard are `crates/tinio-cli/src/sandbox/plan.rs`'s and `product.rs`'s).
///
/// `HOME`/`USERPROFILE` are the scratch directory for every child ([`Cli::command`]), so the
/// derived tree lands there rather than under the operator's real home.
#[given("the child names no state directory")]
async fn state_dir_is_derived(world: &mut World) {
    cli(world).state_dir_named = false;
}

/// An unwritable state directory — the fixture the launcher's write probe refuses
/// (`crates/tinio-cli/src/sandbox/product.rs`'s `refuse_unwritable_state_dir`).
///
/// **Three facts, and each one is what keeps an earlier writer from being the refusal.** The
/// directory is made to refuse new entries (mode `0o555`), and it already holds the two files the
/// launcher would otherwise *create* in it — because on a launcher arm the subscriber is installed
/// before the probe runs (`crates/tinio-cli/src/commands/start/run.rs`'s `install_subscriber`,
/// called from `crate::sandbox::confine`'s `install()?` in front of `attach_control`), and:
///
/// - `access.log` is the **observer's own sink**: `build_subscriber`
///   (`crates/tinio-server/src/log.rs`) opens it with `create(true).append(true)`, so an *empty*
///   `0o555` directory fails there first — measured on macOS 15.8, 2026-09-30, where the run
///   answers `logging error: Permission denied (os error 13)` and the probe is never reached.
///   Planted, the open only needs the file's own write bit, and the sink is no longer in the way.
/// - `.tinio.toml` is the configuration the run would otherwise write
///   (`tinio_cli::config::discover::load_or_create`, reached from `start::run`'s own step), for the same
///   reason: a directory that cannot take that file refuses the run with the configuration
///   layer's error. The planted document is the smallest one the schema accepts.
///
/// What is left is exactly the probe: `create_new` of its `state-probe-<pid>-<nanos>` entry, which
/// a `0o555` directory cannot satisfy. **Measured on the same guest with the sink planted**:
/// `the state directory … cannot take a new entry (Permission denied (os error 13))`.
///
/// The mode is given back by [`Cli::restore_modes`]; unix only, because a mode bit is what the
/// fixture is.
#[cfg(unix)]
#[given("the child's state directory refuses new entries")]
async fn state_dir_refuses_new_entries(world: &mut World) {
    use std::os::unix::fs::PermissionsExt as _;

    let cli = cli(world);
    let path = cli.state.clone();
    fs::write(path.join(".tinio.toml"), b"version = 1\n").expect("the state dir's configuration");
    // The observer's sink, empty and writable: the probe's refusal is the sentence this row is
    // about, and a sink that had to *create* its file would answer first.
    fs::write(path.join("access.log"), b"").expect("the state dir's access log");
    let mode = fs::metadata(&path)
        .expect("the relocated state directory exists")
        .permissions()
        .mode()
        & 0o777;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o555))
        .unwrap_or_else(|err| panic!("make `{}` refuse entries: {err}", path.display()));
    cli.unwritable.push((path, mode));
}

/// The state directory **is** a junction: the one spelling the Windows launcher's reparse-point
/// refusal is reachable through, because it is the only exposure a run does not canonicalise on
/// the operator's behalf.
///
/// **The canonicalisation is the whole reason this row cannot use the served root.** A junction
/// above the root, or the root itself, is resolved by `resolve_root`
/// (`crates/tinio-cli/src/resolve/locate.rs`) before any arm sees it — measured on this host
/// 2026-09-30: such a run *serves* rather than refusing — while `TINIO_STATE_DIR` reaches the
/// exposure set as the operator spelled it (`crates/tinio-cli/src/sandbox/product.rs`'s
/// `exposures`), and the launcher then refuses the link rather than granting through it
/// (`crates/tinio-sandbox-windows/src/backend/exposures/reparse.rs`).
///
/// Read-only is the mode the row needs for the same reason the other state-directory fixtures do:
/// a read-write run resolves `<root>/.tinio` whatever the variable says.
///
/// **The plant fails rather than skipping** when `mklink` cannot make the link — the rule
/// `crates/tinio-sandbox-windows/src/testing.rs`'s `junction` states (a leg asserting an absence
/// after a silently failed `mklink` asserts nothing) — so the fixture also checks the entry it
/// created is really a reparse point.
#[cfg(windows)]
#[given("the child's state directory is a junction")]
async fn state_dir_is_a_junction(world: &mut World) {
    let cli = cli(world);
    let target = cli.state.clone();
    let link = cli.scratch().join("state-link");
    testing::junction(&link, &target);
    cli.vars
        .push((STATE_DIR_VAR.to_owned(), link.display().to_string()));
}

/// The escaping bucket P5's scenario reads: **a junction inside the served root that lands
/// outside it**, planted before the run that has to refuse it.
///
/// **The shape is the Rust leg's own** —
/// `crates/tinio-server/tests/sandbox_e2e_windows.rs`'s
/// `a_confined_server_refuses_an_escaping_bucket_junction_as_nonexistent`, whose CLI-observable
/// half the plan's §5 migrates here and deletes: the target is a directory **outside the served
/// root** (a sibling under the scenario's scratch directory, so `TempDir`'s drop takes both
/// trees) holding `outside.txt`, the object the scenario reads through the link. Outside the root
/// is what makes the link an escape: the exposure set covers the served root and the program
/// directory and nothing else (`crates/tinio-cli/src/sandbox/product.rs`'s `exposures`), so the
/// target is a path no grant of this run's covers — which is why the container needs the grant
/// applied afterwards, and why that grant is [`package_sid_granted`]'s: the SID is the launcher's
/// own report and cannot be derived here.
///
/// **The link is made by [`testing::junction`], and the plant fails rather than skipping when
/// `mklink` did not produce a reparse point** — the rule that device owns: a leg asserting a
/// refusal after a silently failed `mklink` asserts nothing.
///
/// The object's name is fixed rather than a parameter, because the fixture and the request
/// through it are one induction and this is the Rust leg's own spelling of it.
#[cfg(windows)]
#[given(expr = "the root holds an escaping junction {string} to {string}")]
async fn root_holds_an_escaping_junction(world: &mut World, name: String, target: String) {
    let (link, target) = {
        let cli = cli(world);
        (cli.root().join(&name), expand(cli, &target))
    };
    fs::create_dir_all(&target).expect("the out-of-root target");
    fs::write(target.join("outside.txt"), b"outside").expect("the out-of-root object");
    testing::junction(&link, &target);
}

// --- The read-only mode's process legs (FR-023) --------------------------

/// The stderr summary says which mode the run serves in, and the address it names is the one
/// its readiness event carried — the tail ` on <addr> (read-only; credentials ` that only the
/// serving path prints (`crates/tinio-cli/src/commands/start/`).
///
/// **The needle is built from the run's own report rather than the scenario's spelling of a
/// port**, because `--port 0` binds an ephemeral one: a literal in the feature could only name
/// a port no run would have. What the assertion is about is not the text but the *agreement*
/// between the two carriers of one fact — the frame the harness decoded readiness from and the
/// line an operator reads — so the address half is what makes it discriminating, and the mode
/// half is what makes it a claim about read-only mode.
#[then("the run reports serving read-only on the endpoint it bound")]
async fn stderr_reports_read_only(world: &mut World) {
    let run = cli(world).last();
    let endpoint = run
        .readiness
        .expect("the run reported its readiness event, which carries the address this asserts");
    let diagnostic = format!(" on {endpoint} (read-only; credentials ");
    let stderr = run.stderr();
    assert!(
        stderr.contains(&diagnostic),
        "stderr must carry `{diagnostic}`; got:\n{stderr}"
    );
}

/// The relocation's amended half (FR-023 amended, 2026-10-03), and the one no wire response can
/// carry: the **relocated** state dir the harness handed the run ([`Cli::state`] — the first
/// candidate `resolve::resolve_state_dir` reads in this mode) holds **no database**. The storage
/// reads the root's own `<root>/.tinio`, opened read-only; the relocated directory is tool state
/// only (the socket, `state` file and logs a run writes there), which is what keeps a read-only
/// run from writing anything under the served root.
///
/// **`meta.redb` and not "the directory is empty"**: the run legitimately writes its `state`
/// file there, so the claim is about the database — the one file whose presence would mean a
/// second metadata store (the old design's shape). The assertion beside it is
/// `the root holds no state directory`.
#[then("the relocated state dir holds no state database")]
async fn relocated_state_dir_holds_no_state_database(world: &mut World) {
    let path = cli(world).state.join("meta.redb");
    assert!(
        !path.exists(),
        "read-only mode keeps no second database: `{}` exists",
        path.display()
    );
}

/// One entry of a served-root snapshot: its path relative to the root and, for a file, its length
/// and mtime (directories contribute their name alone).
type RootEntry = (PathBuf, Option<(u64, SystemTime)>);

/// Every entry under `root`, sorted by relative path.
///
/// A write would move one of these — a new name, a file's length or mtime — so two snapshots are
/// the portable "the run never wrote this tree" evidence a count alone cannot give: a run that
/// rewrote the root's own `meta.redb` in place would leave the count unchanged.
fn snapshot_root(root: &Path) -> Vec<RootEntry> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<RootEntry>) {
        let entries = fs::read_dir(dir).unwrap_or_else(|err| {
            panic!("the root `{}` is readable: {err}", root.display());
        });
        for entry in entries {
            let entry = entry.expect("a readable entry");
            let path = entry.path();
            let meta = entry.metadata().expect("entry metadata");
            let file = (!meta.is_dir()).then(|| (meta.len(), meta.modified().expect("mtime")));
            out.push((
                path.strip_prefix(root)
                    .expect("under the root")
                    .to_path_buf(),
                file,
            ));
            if meta.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut entries = Vec::new();
    walk(root, root, &mut entries);
    entries.sort();
    entries
}

/// Record the served root's entries — the state the read-only run that follows must leave
/// untouched (FR-023 amended: the run **reads** the root's own database, so "it did not write the
/// root" is a claim about a run that opened it, not about one that never looked).
#[when("the served root's entries are recorded")]
async fn record_root_entries(world: &mut World) {
    let cli = cli(world);
    let snapshot = snapshot_root(&cli.root);
    cli.recorded = Some(snapshot);
}

/// The served root holds exactly the entries recorded before the run under test: every name,
/// every file's length and mtime.
#[then("the served root holds the entries it recorded")]
async fn root_holds_the_recorded_entries(world: &mut World) {
    let cli = cli(world);
    let recorded = cli
        .recorded
        .as_ref()
        .expect("`the served root's entries are recorded` ran before this assertion");
    let now = snapshot_root(&cli.root);
    assert_eq!(
        &now, recorded,
        "the run wrote the served root: recorded {recorded:?}, found {now:?}"
    );
}

/// The failure names the root it could not serve — the path `{missing}` expanded to, in either
/// form a render can print it: `Path::display` writes it plain, while a `Debug` of the error
/// escapes the separators on Windows, and both spellings are accepted because which one lands
/// is a property of the error's own rendering and not of the failure.
///
/// **Named rather than spelled by the scenario**, because the path is the harness's scratch tree
/// ([`Cli::missing`]) — a scenario names it `{missing}` on the command line and asserts it here.
#[then("its stderr names the missing root")]
async fn stderr_names_missing_root(world: &mut World) {
    let cli = cli(world);
    let missing = cli.missing().display().to_string();
    let escaped = missing.replace('\\', "\\\\");
    let stderr = cli.last().stderr();
    assert!(
        stderr.contains(&missing) || stderr.contains(&escaped),
        "the failure must name the root it could not serve (`{missing}`); stderr:\n{stderr}"
    );
}

/// A run that refused a missing root left it missing: the run ends during resolution, so nothing
/// created the directory it was told to serve — and read-only mode in particular must never
/// create the root rather than serve it (`contracts/cli.md`'s "an existing DIR").
#[then("the missing root was not created")]
async fn missing_root_not_created(world: &mut World) {
    let missing = cli(world).missing().to_path_buf();
    assert!(
        !missing.exists(),
        "the run created the root it could not serve: `{}`",
        missing.display()
    );
}
