//! Spawn harness shared by the read-only and sandbox-channel `serve` legs.
//!
//! Two integration binaries include this file. A helper only one of them calls
//! is still part of the harness, so dead-code is allowed here and nowhere else.

#![allow(dead_code)]

use std::{
    fs,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tinio_config::{
    sources::{READ_ONLY_VAR, SANDBOX_VAR},
    state::STATE_DIR_VAR,
};
use tinio_sandbox::Event;
use tinio_util::testing::PipedChild;
use tokio::time::sleep;

use super::common::{frame_line, signed, tinio_bin, wait_timeout};

/// A spawned `serve` child with both pipes drained. Killed and reaped when
/// it drops (see the module doc for the ordering rationale).
///
/// **The child, its two buffers, their drains and the kill are [`PipedChild`]'s**, because the
/// read-only suite beside this one holds the same harness and two `tests/` trees cannot import
/// from each other — the shared device is where that copy went. What stays here is the part both
/// of them add on top and neither shares: reading the **readiness event** off stdout, which is
/// `tinio_sandbox::Event`'s type. Its methods arrive through `Deref`, so a leg still spells
/// `server.stop()`, `server.exited()` and `server.stderr_text()`.
pub struct Serve(PipedChild);

impl Deref for Serve {
    type Target = PipedChild;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Serve {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Serve {
    /// Spawn `tinio server` on `root`, with `args` after it.
    ///
    /// The child inherits this process's environment, so the four
    /// variables that would silently change what it serves are removed
    /// first — `TINIO_READ_ONLY` (which would put every spawned server in
    /// read-only mode), `TINIO_SANDBOX` (which the sandbox-channel legs below
    /// deliberately set and the rest must not inherit), `TINIO_STATE_DIR`
    /// (which would relocate its state) and `TINIO_SCANNER` (which would
    /// change what it scans) — and only then is the caller's `env` applied.
    /// The rationale is `clients.rs`'s `spawn_child`; each leg here sets
    /// exactly what it means to set.
    ///
    /// **The spawn is guarded** — [`PipedChild::spawn`] goes through
    /// `tinio_util::job::spawn` — so a leg that dies without unwinding takes
    /// its servers with it. What the guard covers on each target, and the
    /// thread it has to be taken on, are that module's doc.
    pub(super) fn spawn(root: &Path, args: &[&str], env: &[(&str, &str)]) -> Self {
        let bin = tinio_bin();
        let mut cmd = Command::new(&bin);
        cmd.arg("server")
            .arg(root)
            .args(args)
            .env_remove(READ_ONLY_VAR)
            .env_remove(SANDBOX_VAR)
            .env_remove(STATE_DIR_VAR)
            .env_remove("TINIO_SCANNER");
        for (name, value) in env {
            cmd.env(name, value);
        }
        Self(PipedChild::spawn(&mut cmd))
    }

    /// Wait for the `listening` event and return the address the child
    /// actually bound (`--port 0` makes it ephemeral).
    ///
    /// The example speaks the sandbox's event stream (design decision 19):
    /// one SSE frame per event — an `event:` line, a `data:` line, a blank
    /// line — so **a frame is not a line**, and [`Serve::stdout_event`] is
    /// what accumulates until `Event::decode` sees a whole one. The `error`
    /// frame a refusal sends is read as the refusal it is rather than as a
    /// timeout, which is what the frame buys: a consumer of stdout gets a
    /// cause where the old text scrape found only an absence.
    ///
    /// Bounded: a child that never gets there fails this wait with its
    /// captured stderr instead of hanging the suite, and one that dies
    /// first fails immediately rather than burning the whole timeout.
    pub(super) async fn ready(&mut self) -> SocketAddr {
        let timeout = wait_timeout();
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(event) = self.stdout_event() {
                match event {
                    Event::Listening { addr } => return addr,
                    Event::Error { message } => {
                        self.stop();
                        panic!("the server refused to start: {message}");
                    }
                }
            }
            if let Some(status) = self.child_mut().try_wait().expect("the child's status") {
                // Stop first: the message is only as good as the pipes it
                // quotes, and a live child's are not at EOF yet.
                self.stop();
                panic!(
                    "the server exited ({status}) without reporting `listening`; stderr:\n{}",
                    self.stderr_text()
                );
            }
            if Instant::now() >= deadline {
                self.stop();
                panic!(
                    "the server reported no event within {} s; stderr:\n{}",
                    timeout.as_secs(),
                    self.stderr_text()
                );
            }
            sleep(Duration::from_millis(2)).await;
        }
    }

    /// The first complete event frame among the stdout lines so far, or
    /// `None` while they hold none. The buffer is read from its start through
    /// [`frame_line`], which owns the framing rule: a frame is
    /// `Event::encode`'s two field lines closed by a blank line, so scanning
    /// line by line would hand `Event::decode` half a frame.
    ///
    /// **Read as a snapshot** ([`PipedChild::stdout_lines`]), so the buffer's lock is never held
    /// across a caller's failure path: those call `stop`, and the drain it joins takes the same
    /// lock.
    pub(super) fn stdout_event(&self) -> Option<Event> {
        let mut frame = String::new();
        self.stdout_lines()
            .iter()
            .find_map(|line| frame_line(&mut frame, line))
    }
}

/// The mode the plane serves in, observed from outside the process.
#[derive(Debug, PartialEq, Eq)]
pub enum Mode {
    ReadWrite,
    ReadOnly,
}

/// Observe the served plane's mode through the discriminator the module doc
/// describes: a signed `PUT` to a bucket that does not exist. Both answers
/// are pinned, so a third answer fails loudly instead of reading as either
/// mode.
pub async fn observed_mode(addr: SocketAddr) -> Mode {
    let answer = signed(addr, "PUT", "/ghost/a.txt", b"x").await;
    let code = answer.code();
    match (answer.status, code.as_str()) {
        (404, "NoSuchBucket") => Mode::ReadWrite,
        (403, "AccessDenied") => Mode::ReadOnly,
        _ => panic!(
            "a signed PUT /ghost/a.txt must answer 404 NoSuchBucket read-write or 403 \
             AccessDenied read-only; got {} `{code}`: {}",
            answer.status,
            answer.text()
        ),
    }
}

/// A scratch layout: one tempdir holding a leg's served root, its state dir
/// and its config file. Removed when the fixture drops — each leg stops its
/// server first, so nothing is ever deleted under a live one.
pub struct Scratch {
    dir: TempDir,
}

impl Scratch {
    pub(super) fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("a scratch dir"),
        }
    }

    /// The served root, created: a read-write server would create it itself,
    /// and a read-only one requires it to exist already.
    pub(super) fn root(&self) -> PathBuf {
        let root = self.dir.path().join("root");
        fs::create_dir_all(&root).expect("the served root");
        root
    }

    /// The relocated state dir the read-only legs pass as
    /// `TINIO_STATE_DIR` — never the real home, which a read-only leg would
    /// otherwise derive (`<home>/.tinio/roots/<hash>/`) and write into.
    pub(super) fn state(&self) -> PathBuf {
        self.dir.path().join("state")
    }

    /// A root path that does not exist, and must stay that way.
    pub(super) fn missing_root(&self) -> PathBuf {
        self.dir.path().join("missing")
    }

    /// Write a `[server] read_only = <on>` config file and return its path.
    pub(super) fn config_file(&self, read_only: bool) -> PathBuf {
        let path = self.dir.path().join("config.toml");
        fs::write(
            &path,
            format!("version = 1\n\n[server]\nread_only = {read_only}\n"),
        )
        .expect("the config file");
        path
    }

    /// Write a `[server] sandbox = <on>` config file and return its path — the
    /// key decision 1 names and no phase implements.
    pub(super) fn sandbox_config_file(&self, sandbox: bool) -> PathBuf {
        let path = self.dir.path().join("sandbox.toml");
        fs::write(
            &path,
            format!("version = 1\n\n[server]\nsandbox = {sandbox}\n"),
        )
        .expect("the config file");
        path
    }
}

/// `TINIO_STATE_DIR` for a leg, as the `&str` the child's environment takes.
pub fn state_var(state: &Path) -> (&'static str, &str) {
    (STATE_DIR_VAR, state.to_str().expect("a UTF-8 scratch path"))
}

/// Whether `haystack` names `path` in either form a failure can print it:
/// `Path::display` writes it plain, while a derived `Debug` (which is how
/// `main`'s returned error reaches stderr) escapes the separators on
/// Windows.
pub fn names_path(haystack: &str, path: &Path) -> bool {
    let plain = path.display().to_string();
    haystack.contains(&plain) || haystack.contains(&plain.replace('\\', "\\\\"))
}
