//! Process, DACL and console helpers for the Windows confined-server legs.

#![allow(dead_code)]

use std::{
    env, fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    os::windows::process::CommandExt as _,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use tinio_sandbox::{
    AccessMode,
    windows::{
        acl::{self, Target},
        control, sid, testing,
    },
};

use super::common::{serve_bin, wait_for_listening};

pub const ALLOW_SKIP: &str = "TINIO_SANDBOX_ALLOW_SKIP";

/// `CREATE_NEW_PROCESS_GROUP` — the launcher is spawned as the leader of its own
/// process group, which is what makes a console control event addressed at *it*
/// reach it and nothing else (Task 10's suite spawns the same way, for the same
/// reason).
pub const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

/// How long the confined server may take to die once its launcher is gone, and
/// how long the launcher may take to exit once a stop has been forwarded to it.
///
/// Both are failures rather than waits: a child that outlives its launcher is a
/// job object that did not do its work, and a launcher that does not exit after a
/// stop is a relay that did not deliver.
pub const EXIT_TIMEOUT: Duration = Duration::from_secs(15);

/// One `serve --sandbox` run, from the test's side.
pub struct Run {
    pub(super) launcher: Child,
    /// The launcher's stderr: its banner, every log the confined child writes, and
    /// shared §7's `info!` events — the launcher reaches `main`'s subscriber, which
    /// is installed by the time its sandbox arm runs (Task 11's arm sits after it).
    pub(super) log: PathBuf,
    pub(super) address: SocketAddr,
}

/// **Every spawned process is waited for or killed on every path, red ones
/// included.** A leg that panics between `spawn` and its own kill — [`answer`]
/// does exactly that against a server that accepts and then says nothing — would
/// otherwise leave the launcher *and* the confined child alive for the rest of
/// the run: they hold the listener, the job and the ACEs, and they hold
/// `target\debug\examples\serve.exe` open, which breaks the next build of the
/// example with `LNK1104` (measured, twice, before this impl existed).
///
/// Cheap when the launcher is already gone: `kill` and `wait` on a reaped child
/// both answer errors, and both are ignored here. Killing the launcher is also
/// what ends the confined child — `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` is the
/// only thing that can (decision 20).
impl Drop for Run {
    fn drop(&mut self) {
        let _ = self.launcher.kill();
        let _ = self.launcher.wait();
    }
}

impl Run {
    /// Spawn `serve --sandbox` on `root` and wait for the endpoint.
    ///
    /// **stderr goes to a file and not to a pipe.** Everything the launcher and
    /// the confined child have to say is on stderr, and a pipe nobody drains
    /// becomes a child blocked on a full buffer — which reads as a hung server
    /// rather than as a full pipe. The file is rewritten by a second run on the
    /// same root, so read what you need from a run before starting the next.
    ///
    /// **The file sits beside the served root, never inside it.** The launcher is
    /// what creates the root, so a log written before the spawn has no directory
    /// to live in; and the served tree is the server's, where a diagnostic of the
    /// harness would be an object of the store under test. The confined child still
    /// reaches the file, because its stderr is an inherited handle: the access
    /// decision was made at the open, and no path is involved afterwards.
    pub(super) fn spawn(root: &Path, port: u16) -> Self {
        let log = root.with_file_name("launcher.log");
        let stderr = fs::File::create(&log).expect("create the launcher's log");
        let mut launcher = Command::new(serve_bin())
            .args([
                root.to_str().expect("a UTF-8 root"),
                "--port",
                &port.to_string(),
                "--sandbox",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .creation_flags(CREATE_NEW_PROCESS_GROUP)
            .spawn()
            .expect("spawn serve");
        // The handshake is the harness's; the failure is this leg's, because a
        // launcher's log survives the kill that a child's stderr pipe does not.
        let address = match wait_for_listening(&mut launcher) {
            Ok(address) => address,
            Err(message) => fail(&mut launcher, &log, &message),
        };
        Self {
            launcher,
            log,
            address,
        }
    }

    /// The package SID this run's launcher printed.
    ///
    /// Read from the banner rather than re-derived here: a `PackageSid` built in
    /// *this* process would create the very profile the assertion is about, and
    /// its `Drop` would delete that profile while the launcher still needed it
    /// (Task 2).
    pub(super) fn package_sid(&self) -> String {
        self.log_text()
            .lines()
            .find_map(|line| line.strip_prefix("sandbox: package SID "))
            .expect("the launcher printed its package SID")
            .trim()
            .to_string()
    }

    /// Whether this run found the root's profile already there — design
    /// decision 21's second artifact, read the way an operator reads it.
    ///
    /// **This is the only half of decision 21 an ACE count cannot see.** The
    /// profile and the ACEs are two artifacts a clean run takes back, and a
    /// launcher that rolled the ACEs up and left the profile behind passes
    /// every assertion on the DACL: that is exactly the state this phase's own
    /// diagnosis found (eight leaked profiles from runs that exited cleanly,
    /// because a `process::exit` had landed above the `Drop` that deletes
    /// them), and the fix for it is the kind a later `process::exit` can
    /// reintroduce. A fresh run on the same root answering `false` says the
    /// profile was not there to take over — the claim in the form a second run
    /// can falsify, rather than in the launcher's own assurance that it called
    /// the deletion API.
    pub(super) fn took_the_profile_over(&self) -> bool {
        let log = without_ansi(&self.log_text());
        let field = log
            .lines()
            .find(|line| line.contains("CreateAppContainerProfile"))
            .and_then(|line| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix("taken_over="))
            })
            .expect("the launcher reported its profile creation");
        match field {
            "false" => false,
            "true" => true,
            other => panic!("`taken_over` is not a boolean: {other:?}"),
        }
    }

    pub(super) fn log_text(&self) -> String {
        fs::read_to_string(&self.log).expect("the launcher's log")
    }

    /// Send this run's launcher a stop and wait for it, or fail naming `what`.
    ///
    /// **One spelling for the sequence three legs run**, because a refusal and a
    /// launcher that outlives its stop are the same two failures whoever is
    /// stopping whom, and each leg had its own rendering of them — the third
    /// copy had also lost the `status` its siblings assert on.
    ///
    /// **A refusal is fatal rather than swallowed, and the host's half of the
    /// question was settled before this leg spawned anything.** `send_stop` has
    /// two causes for an `Err` — no console, or a group that does not exist — and
    /// only the first is a property of the machine; [`host_ready`] probed it. A
    /// launcher that announced readiness from *this* run and then refused a stop
    /// died in between, and an earlier revision let the operator's opt-out
    /// swallow exactly that, turning a dead launcher into a pass — the same
    /// defect class as a readiness frame nobody waited for.
    pub(super) fn stop(&mut self, what: &str) -> ExitStatus {
        if let Err(err) = control::send_stop(self.launcher.id()) {
            fail(
                &mut self.launcher,
                &self.log,
                &format!("the stop could not be delivered to {what}'s group ({err})"),
            );
        }
        wait_for_exit(&mut self.launcher, &self.log, what)
    }
}

/// **The host's preconditions, probed before anything is spawned — and the only
/// place this suite's opt-outs are decided.** Returns `false` when this machine
/// cannot do something a leg needs *and* the operator has deliberately said so;
/// the leg then returns without asserting anything about the product.
///
/// Two capabilities of the **machine** are asked about, both before the first
/// spawn, and the reason names whichever answered no:
///
/// - **can it confine at all** — [`host_can_confine`] performs the launcher's own
///   setup acts against a throwaway directory and rolls them back;
/// - **can it deliver a console control event** — [`console_device`] opens
///   `CONOUT$`, which is the console a `GenerateConsoleCtrlEvent` needs to have
///   anywhere to go. The rollback half of the second leg depends on that event,
///   and a host without a console cannot exercise that half at all.
///
/// That is the precondition Ruling 28 makes an opt-out, and the shape is P1's
/// suite's: fail by default, name what is missing *and the remedy*, and let
/// [`ALLOW_SKIP`] pass deliberately. P1 has one opt-out site
/// (`crates/tinio-sandbox-linux/tests/jail.rs:118`) and the Linux e2e one
/// (`crates/tinio-server/tests/sandbox_e2e.rs:170`); this file has two, and both
/// go through this function.
///
/// **It is a probe of the host and not a check on the run, and that distinction
/// is the whole of this function.** Earlier drafts asked these same questions of
/// things the product controls — [`wait_for_listening`]'s `Event::Error` frame,
/// which is also how the confined child's own startup failure arrives, and the
/// `send_stop` call made after the second launcher has already been running — so
/// a tree whose server could not start, or a launcher that died mid-run, could
/// report a pass under the variable. Nothing the product does is reachable from
/// here: this runs before the first spawn, and **every failure afterwards is
/// fatal**, with no opt-out left to reach for.
pub fn host_ready() -> bool {
    let Some(reason) = host_missing() else {
        return true;
    };
    assert!(
        skip_allowed(),
        "this host cannot run the confined server suite ({reason}), so this leg would assert \
         nothing. Set {ALLOW_SKIP}=1 to pass deliberately on a host that genuinely cannot \
         (design §12)"
    );
    eprintln!(
        "skipping: this host cannot run the confined server suite ({reason}) and {ALLOW_SKIP} is \
         set (design §12)"
    );
    false
}

/// Which host capability is missing, if either — the one reason [`host_ready`]
/// reports and the one it asserts on.
pub fn host_missing() -> Option<String> {
    host_can_confine().err().or_else(|| console_device().err())
}

/// Whether this process has a console for a control event to be delivered on.
///
/// **`CONOUT$` is the proxy, and it is the honest one for this question.** A
/// console is a property of the process's environment rather than of the
/// product; `GenerateConsoleCtrlEvent` addresses processes attached to the
/// caller's console — `control::send_stop`'s own `# Errors` names "there is no
/// console to deliver on" as one of its two causes — and opening the console
/// device fails exactly when there is none. A CI runner that spawns this suite
/// without a console cannot exercise the rollback half of the second leg, which
/// is why that is a precondition here rather than a failure there.
///
/// It is deliberately **not** a probe of the launcher's group: whether a
/// particular group exists is a fact about the run, and the leg reports it.
pub fn console_device() -> Result<(), String> {
    fs::OpenOptions::new()
        .write(true)
        .open("CONOUT$")
        .map(|_| ())
        .map_err(|err| format!("open the console device (CONOUT$): {err}"))
}

/// Whether this machine can do what a confined run's setup needs — the launcher's
/// own acts, in its own order: resolve the exposure set's paths, create the
/// AppContainer profile (`resolve an exposed path` precedes
/// `CreateAppContainerProfile` in every launcher log this suite writes), and
/// write the ACEs with `icacls`.
///
/// A throwaway directory under `tempfile`, so a host that passes leaves nothing
/// behind: the ACE is rolled back, and the profile is deleted by `PackageSid`'s
/// own `Drop` when the value goes out of scope at the end of the call.
pub fn host_can_confine() -> Result<(), String> {
    let scratch = tempfile::tempdir().map_err(|err| format!("make a scratch directory: {err}"))?;
    let profile = sid::package_name(scratch.path());
    let sid = sid::PackageSid::derive(&profile)
        .map_err(|err| format!("create an AppContainer profile: {err}"))?;
    #[allow(clippy::arc_with_non_send_sync)]
    let mut grants = acl::Grants::new(Arc::new(sid));
    grants
        .apply(scratch.path(), Target::Directory, AccessMode::ReadWrite)
        .map_err(|err| format!("grant an ACE with icacls: {err}"))?;
    grants
        .rollback()
        .map_err(|err| format!("roll an ACE back with icacls: {err}"))?;
    Ok(())
}

/// Kill and reap the launcher, then fail with `message` and the log it left.
///
/// **Killing first is not tidiness.** A confined server left alive holds its
/// listener, its job and its ACEs for the life of the test process, and the leg
/// that failed is the one whose launcher is least likely to exit on its own.
pub fn fail(launcher: &mut Child, log: &Path, message: &str) -> ! {
    let _ = launcher.kill();
    let _ = launcher.wait();
    panic!(
        "{message}; the launcher's log:\n{}",
        fs::read_to_string(log).unwrap_or_default()
    );
}

/// Wait for the launcher to exit, or kill it and fail.
///
/// **Bounded, and that is the whole reason it is not a bare `wait`**: a stop the
/// child never acted on leaves the launcher waiting on a child that is not going
/// to exit, and an unbounded wait would take the leg with it instead of reporting
/// the relay's failure (Task 6 paid for this rule).
///
/// **The wait is [`testing::wait_for_exit`]'s and not a copy of it**, which is
/// what this file and the sandbox crate's suites share: the deadline, the poll,
/// and the kill-before-panic are one rule, and a second spelling of it is a
/// second place it can be given up on without the kill. This wrapper exists for
/// the one argument the shared form does not take — the `EXIT_TIMEOUT` that is
/// this suite's rather than the harness's 30 s — and the log it once rendered
/// into the panic message is read from the same red output either way, since
/// [`fail`] and this file's other diagnostics all name the leg's own files.
pub fn wait_for_exit(launcher: &mut Child, _log: &Path, what: &str) -> ExitStatus {
    testing::wait_for_exit(launcher, EXIT_TIMEOUT, what)
}

/// `icacls` on `root`, with no extra arguments.
///
/// The oracle for what is on the operator's directory. **The definition is
/// `tinio-sandbox-windows`'s, re-exported through the facade**: this suite reads
/// the same DACLs the crates' own ACL tests do, and its copy was the third
/// spelling of one command — the binary's resolution, the checked exit status
/// and the text all identical. Two arguments are what this suite's callers all
/// want, and the shared function takes the rest as a slice for the callers that
/// pass `["/T"]`.
pub fn icacls(root: &Path) -> String {
    testing::icacls(root, &[])
}

/// How many ACEs on `root` name `sid`.
///
/// The count and not a presence check: the grant is `/grant:r`, so a takeover that
/// added a second entry instead of replacing the first would leave two — and
/// "the residue is reusable" is exactly the claim that no second entry appears.
pub fn ace_count(listing: &str, sid: &str) -> usize {
    listing.matches(sid).count()
}

/// `<link>` as a directory junction onto `target`.
///
/// **The definition is [`testing::junction`]'s**, and this leg is why it is
/// shared: the same fixture is planted by `tinio-sandbox-windows`'s own
/// `backend` tests, and the self-assertion that makes a broken `mklink` fail the
/// leg rather than read as a refusal — every assertion after this one is an
/// *absence* — is the half that must not be re-derived per suite.
pub fn junction(link: &Path, target: &Path) {
    testing::junction(link, target);
}

/// `text` with the ANSI SGR sequences `tracing`'s text formatter writes removed.
///
/// **The launcher's stderr is a file and still carries them.** `serve` builds its
/// subscriber with `fmt::layer()` and does not turn colour off, so the default
/// is `with_ansi(true)` and a *field* arrives with its name and its `=` separated
/// by escapes — `taken_over` is written as
/// `\x1b[3mtaken_over\x1b[0m\x1b[2m=\x1b[0mfalse` in the log a run leaves behind.
/// Every other assertion in this file is on a value (`sandbox: package SID`,
/// `CreateProcessW`, `step="…"`), and a value survives the interleaving; a field
/// does not, which is why this exists rather than a substring test on the raw
/// text. `tinio-sandbox-core`'s recorder test solves the same problem the other
/// way — `with_ansi(false)` on a subscriber it builds itself — which a test
/// reading another process's log cannot do.
pub fn without_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        // Everything that is not the start of an escape is copied through.
        if ch != '\u{1b}' {
            plain.push(ch);
            continue;
        }
        // An SGR sequence is `ESC [`, parameter bytes, then one final byte in
        // `@`–`~` — the shape libtest's and `tracing`'s writers both emit. A
        // stray `ESC` with no `[` after it is dropped rather than copied,
        // because copying it would leave the assertion comparing escape text.
        if chars.next() != Some('[') {
            continue;
        }
        for byte in chars.by_ref() {
            if ('@'..='~').contains(&byte) {
                break;
            }
        }
    }
    plain
}

/// Whether the operator has deliberately opted out of a precondition this host
/// cannot meet (see [`ALLOW_SKIP`]).
pub fn skip_allowed() -> bool {
    env::var(ALLOW_SKIP).is_ok_and(|value| !value.is_empty())
}

/// Connect to the confined server, ask for the root, and require an HTTP answer.
///
/// A plain TCP exchange proves the listener is reachable from outside the
/// confinement. Anything more would re-test S3, which the other suites cover.
pub fn answer(address: SocketAddr) {
    let mut stream =
        TcpStream::connect_timeout(&address, EXIT_TIMEOUT).expect("connect to the confined server");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("write");
    stream
        .set_read_timeout(Some(EXIT_TIMEOUT))
        .expect("bound the read");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    assert!(response.starts_with("HTTP/1.1 "), "{response}");
}
