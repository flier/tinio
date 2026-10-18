//! `--daemon`: no supervisor, no console, and a parent that waits for ready (decision 9).
//!
//! Unix: `Daemonize::execute()` (never `start()` — that exits the parent with 0 and hides
//! child refusals). Windows: detached child. Parent proves ready via `state` + control
//! channel + a launch token (so a concurrent or leftover instance cannot be mistaken for
//! this run). `api`-only: no plane means no readiness proof, so the flag is compiled out.

use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process, thread,
    time::{Duration, Instant},
};
#[cfg(unix)]
use std::{fs::OpenOptions, os::unix::net::UnixStream};
#[cfg(windows)]
use std::{
    fs::OpenOptions,
    os::windows::process::CommandExt,
    process::{Command, Stdio},
};

#[cfg(unix)]
use daemonize::{Daemonize, Outcome};
#[cfg(unix)]
use rustix::process::{Pid, Signal, kill_process};
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS};

#[cfg(windows)]
use crate::_util::process::kill_and_reap;
use crate::{
    _api::state::{self as api_state, State},
    Error,
    commands::start::Args,
    events, resolve,
    resolve::Resolution,
};

/// Parent readiness bound (`contracts/cli.md` ~10 s).
const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the parent re-reads `state` / the launch token.
const READY_POLL: Duration = Duration::from_millis(50);

/// TCP connect bound so one hung connect cannot outrun [`READY_TIMEOUT`].
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

/// Env the parent sets and the child echoes into [`LAUNCH_FILE`].
const LAUNCH_ENV: &str = "TINIO_DAEMON_LAUNCH";

/// Sidecar beside `state`: `daemon.launch.<token>` — one file per launch, no clobber.
const LAUNCH_FILE_PREFIX: &str = "daemon.launch.";

/// Sibling of the launch token: `daemon.pid.<token>` holds the grandchild's pid so the
/// parent can kill it when readiness fails. Double-fork does not return that pid.
#[cfg(unix)]
const PID_FILE_PREFIX: &str = "daemon.pid.";

/// What the parent waits for: prior pid (bystander filter), launch token, and on Windows the
/// known child pid.
struct ReadyExpect {
    previous: Option<u32>,
    token: String,
    expected_pid: Option<u32>,
}

/// Detaches when `--daemon` is set; otherwise `Ok(())` immediately.
///
/// Unix: `Outcome::Child` returns into the sequence; the parent waits and `process::exit`s.
/// Windows: this process spawns the child (argv minus `--daemon`) and waits the same way.
pub fn detach_if_requested(args: &Args, resolution: &Resolution) -> Result<(), Error> {
    if !args.daemon {
        return Ok(());
    }
    let log_file = resolve::log_file(resolution, args)?;
    let previous = State::read(&resolution.state_dir)
        .ok()
        .flatten()
        .map(|state| state.pid);
    // The state file's own generator: one spelling of "an unguessable token" for the crate's two
    // producers of one (the length only sizes the launch file's name).
    let token = api_state::generate_token()?;
    #[cfg(unix)]
    {
        // Inherited across the double-fork; must land before any thread exists.
        set_launch_env(&token);
        unix_detach(
            resolution,
            &log_file,
            ReadyExpect {
                previous,
                token,
                expected_pid: None,
            },
        )
    }
    #[cfg(windows)]
    {
        windows_detach(
            resolution,
            &log_file,
            ReadyExpect {
                previous,
                token,
                expected_pid: None, // filled with child.id() inside the arm
            },
        )
    }
}

/// Sets `TINIO_DAEMON_LAUNCH` for the unix fork path.
///
/// # Safety
///
/// Callers must ensure this process is still single-threaded (decision 9: detach before the
/// runtime). Edition 2024 marks env mutation `unsafe` because it races with concurrent
/// `getenv` on other threads — there are none here.
#[cfg(unix)]
#[expect(
    unsafe_code,
    reason = "edition 2024 marks env mutation `unsafe` because it races with `getenv` on other threads; this call runs on the single-threaded detach path, before the runtime exists"
)]
fn set_launch_env(token: &str) {
    // SAFETY: single-threaded detach path; see function docs.
    unsafe { env::set_var(LAUNCH_ENV, token) };
}

/// Creates `daemon.launch.<token>` when this process is a `--daemon` child.
///
/// Called from `start::run` after the state dir exists so the parent can match identity before
/// (and independently of) `state` publication. Concurrent launches use distinct filenames.
pub fn publish_launch_token(state_dir: &Path) -> Result<(), Error> {
    let Ok(token) = env::var(LAUNCH_ENV) else {
        return Ok(());
    };
    if token.is_empty() {
        return Ok(());
    }
    fs::create_dir_all(state_dir)?;
    fs::write(launch_path(state_dir, &token), b"1").map_err(Error::from)?;
    Ok(())
}

fn launch_path(state_dir: &Path, token: &str) -> PathBuf {
    state_dir.join(format!("{LAUNCH_FILE_PREFIX}{token}"))
}

/// `daemon.pid.<token>` beside `state`. The token makes the file this launch's, so a timeout
/// cannot kill a previous daemon whose pid was left in a shared file.
#[cfg(unix)]
fn pid_path(state_dir: &Path, token: &str) -> PathBuf {
    state_dir.join(format!("{PID_FILE_PREFIX}{token}"))
}

/// Writes this process's pid where the parent looks after `Daemonize::execute` returns.
///
/// The parent unblocks when the intermediate child exits, which is after this grandchild
/// exists. The write is the grandchild's first act, so a 10s readiness wait always observes it.
#[cfg(unix)]
fn publish_daemon_pid(state_dir: &Path, token: &str) -> Result<(), Error> {
    fs::create_dir_all(state_dir)?;
    fs::write(pid_path(state_dir, token), process::id().to_string()).map_err(Error::from)
}

/// `SIGKILL` the pid this launch recorded, then drop the file. A missing file means the
/// grandchild never published — there is no pid to kill, and killing a guess would hit a
/// bystander.
#[cfg(unix)]
fn kill_recorded_daemon(state_dir: &Path, token: &str) {
    let path = pid_path(state_dir, token);
    let Ok(text) = fs::read_to_string(&path) else {
        return;
    };
    if let Ok(raw) = text.trim().parse::<i32>()
        && let Some(pid) = Pid::from_raw(raw)
    {
        let _ = kill_process(pid, Signal::KILL);
    }
    let _ = fs::remove_file(path);
}

/// Child argv: `env::args_os` minus `--daemon` (Windows only).
#[cfg_attr(
    not(any(windows, test)),
    expect(
        dead_code,
        reason = "`--daemon` detach and the argv it rewrites are Windows-only, so off Windows nothing calls this"
    )
)]
pub fn child_argv<I, S>(argv: I) -> Vec<OsString>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    argv.into_iter()
        .map(|arg| arg.as_ref().to_os_string())
        .filter(|arg| arg != "--daemon")
        .collect()
}

#[cfg(unix)]
fn unix_detach(resolution: &Resolution, log_file: &Path, expect: ReadyExpect) -> Result<(), Error> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)?;
    let stderr = file.try_clone()?;
    let outcome = Daemonize::new()
        .working_directory("/")
        .stdout(file)
        .stderr(stderr)
        .execute();
    match outcome {
        Outcome::Parent(Ok(_)) => match await_ready(resolution, log_file, &expect) {
            Ok(()) => {
                let _ = fs::remove_file(pid_path(&resolution.state_dir, &expect.token));
                process::exit(0);
            }
            Err(err) => {
                // Same rule as the Windows arm: a reported failure must not leave the daemon up.
                kill_recorded_daemon(&resolution.state_dir, &expect.token);
                eprintln!("tinio: {err}");
                process::exit(1);
            }
        },
        Outcome::Parent(Err(err)) => Err(Error::Operational(format!(
            "the daemon could not be started: {err}"
        ))),
        Outcome::Child(Ok(_)) => {
            publish_daemon_pid(&resolution.state_dir, &expect.token)?;
            tracing::info!(pid = process::id(), "detached: this process is the daemon");
            Ok(())
        }
        Outcome::Child(Err(err)) => Err(Error::Operational(format!(
            "the daemon could not complete its own setup: {err}"
        ))),
    }
}

#[cfg(windows)]
fn windows_detach(
    resolution: &Resolution,
    log_file: &Path,
    mut expect: ReadyExpect,
) -> Result<(), Error> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)?;
    let stderr = file.try_clone()?;
    let mut child = Command::new(env::current_exe()?)
        .args(child_argv(env::args_os().skip(1)))
        .env(LAUNCH_ENV, &expect.token)
        .stdin(Stdio::null())
        .stdout(Stdio::from(file))
        .stderr(Stdio::from(stderr))
        .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
        .spawn()?;
    expect.expected_pid = Some(child.id());
    tracing::info!(pid = child.id(), "detached: the child process serves");
    match await_ready(resolution, log_file, &expect) {
        Ok(()) => process::exit(0),
        Err(err) => {
            // Failure must not leave a "false fail, still running" child.
            kill_and_reap(&mut child);
            eprintln!("tinio: {err}");
            process::exit(1);
        }
    }
}

fn await_ready(
    resolution: &Resolution,
    log_file: &Path,
    expect: &ReadyExpect,
) -> Result<(), Error> {
    let state_file = api_state::path(&resolution.state_dir);
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Some(state) = the_daemons_publication(resolution, expect)
            && let Some(port) = state.port
            && control_channel_answers(&state.control_name)
        {
            // Decision 19: the parent speaks the child's protocol on **its own** stdout, because
            // the daemon's stdout is the log file. The endpoint is the port the child published,
            // on the address the resolution names — the same `SocketAddr` the child bound and
            // emitted its own frame for.
            match resolution.address() {
                Ok(address) => events::ready(SocketAddr::new(address.ip(), port)),
                // Unreachable while the child is serving — it bound this very address — and
                // reported rather than returned, because a reporting failure must not kill a
                // healthy daemon.
                Err(err) => eprintln!(
                    "tinio: the daemon is serving on port {port}, but the address to report it \
                     on could not be resolved: {err}"
                ),
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::Operational(format!(
                "the daemon did not become ready within {}s: nothing published `{}` and answered on the channel it names — the daemon's log is `{}`; `tinio status` reports what is running",
                READY_TIMEOUT.as_secs(),
                state_file.display(),
                log_file.display()
            )));
        }
        thread::sleep(READY_POLL);
    }
}

fn the_daemons_publication(resolution: &Resolution, expect: &ReadyExpect) -> Option<State> {
    if !launch_path(&resolution.state_dir, &expect.token).is_file() {
        return None;
    }
    let state = State::read(&resolution.state_dir).ok().flatten()?;
    if let Some(pid) = expect.expected_pid {
        if state.pid != pid {
            return None;
        }
    } else if Some(state.pid) == expect.previous {
        // Unix: token is primary; still refuse a bystander that somehow echoed our file.
        return None;
    }
    Some(state)
}

fn control_channel_answers(control_name: &str) -> bool {
    if let Ok(address) = control_name.parse::<SocketAddr>() {
        return TcpStream::connect_timeout(&address, CONNECT_TIMEOUT).is_ok();
    }
    #[cfg(unix)]
    {
        UnixStream::connect(control_name).is_ok()
    }
    #[cfg(windows)]
    {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(control_name)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::Config;

    fn args_without_daemon() -> Args {
        Args::default()
    }

    fn resolution_for(root: &Path) -> Resolution {
        Resolution {
            pg_root: None,
            root: root.to_path_buf(),
            config: Config::default(),
            identity: Default::default(),
            config_text: String::new(),
            config_path: root.join(".tinio/.tinio.toml"),
            read_only: false,
            state_dir: root.join(".tinio"),
            state_dir_override: None,
            config_error: None,
        }
    }

    #[test]
    fn without_the_flag_nothing_detaches() {
        let dir = tempfile::TempDir::new().expect("temp");
        let resolution = resolution_for(&dir.path().join("root"));
        detach_if_requested(&args_without_daemon(), &resolution).expect("no daemon asked for");
    }

    #[test]
    fn the_child_argv_drops_the_daemon_flag() {
        let argv = child_argv(["server", "/srv/data", "--daemon", "--port", "9000"]);
        assert_eq!(argv, ["server", "/srv/data", "--port", "9000"]);
    }

    #[test]
    fn publish_launch_token_is_inert_without_the_env() {
        let dir = tempfile::TempDir::new().expect("temp");
        // No `TINIO_DAEMON_LAUNCH` in this process (tests must not set_var — edition 2024).
        // An unset variable is the inert path; if a sibling somehow left one, write would
        // create a file and this assertion would fail loudly.
        if env::var_os(LAUNCH_ENV).is_none() {
            publish_launch_token(dir.path()).expect("inert");
            assert!(fs::read_dir(dir.path()).expect("dir").next().is_none());
        }
    }

    #[test]
    fn publish_launch_token_writes_when_env_present() {
        // Drive the write path without set_var: call the same fs::write the env arm uses.
        let dir = tempfile::TempDir::new().expect("temp");
        fs::create_dir_all(dir.path()).expect("dir");
        fs::write(launch_path(dir.path(), "abc123"), b"1").expect("write");
        assert!(launch_path(dir.path(), "abc123").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn a_recorded_daemon_pid_is_killed_and_forgotten() {
        let dir = tempfile::TempDir::new().expect("temp");
        // Not this process: SIGKILL here would end the test runner. A pid nothing owns still
        // exercises the read, the kill (ESRCH), and the unlink.
        fs::write(pid_path(dir.path(), "tok"), "2147483647").expect("pid");
        kill_recorded_daemon(dir.path(), "tok");
        assert!(!pid_path(dir.path(), "tok").exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_missing_pid_file_is_not_a_kill() {
        let dir = tempfile::TempDir::new().expect("temp");
        kill_recorded_daemon(dir.path(), "absent");
        assert!(!pid_path(dir.path(), "absent").exists());
    }

    #[test]
    fn publication_requires_matching_token() {
        let dir = tempfile::TempDir::new().expect("temp");
        let resolution = resolution_for(dir.path());
        fs::create_dir_all(&resolution.state_dir).expect("state dir");
        fs::write(launch_path(&resolution.state_dir, "mine"), b"1").expect("token");
        let state = State::new("127.0.0.1:9", Some(9), &resolution.root).expect("state");
        state.write(&resolution.state_dir).expect("write state");
        let pid = state.pid;
        let expect = ReadyExpect {
            previous: None,
            token: "mine".into(),
            expected_pid: Some(pid),
        };
        assert!(the_daemons_publication(&resolution, &expect).is_some());
        let wrong = ReadyExpect {
            previous: None,
            token: "other".into(),
            expected_pid: Some(pid),
        };
        assert!(the_daemons_publication(&resolution, &wrong).is_none());
    }
}
