//! External-client harness and step definitions for the `@external`
//! scenarios (`@interop`/`@boto3`/`@mc`), ported from
//! `tinio-server/tests/e2e/mod.rs` plus the client-leg steps that drive
//! `journey.rs`/`advanced.rs`/`boto3.rs`/`mc.rs`/`edge.rs` and the CI
//! baseline `e2e/interop/*.sh`.
//!
//! The `#[before]` hook (steps/mod.rs) spawns the real `tinio` binary as a
//! subprocess — real redb database, scanner, sweep — bound to `127.0.0.1`
//! on an ephemeral port with no client-side addressing overrides (SC-002),
//! then drives third-party S3 clients (aws cli v2, rclone, mc, boto3)
//! against it. Process lifecycle is native Rust: `Child::kill` + `wait`
//! terminate the server synchronously (TerminateProcess on Windows), so the
//! redb lock is released before a sibling server starts.
//!
//! Config passthrough (grilling Q4): the spawned binary receives the same
//! tag→config mapping as the in-process hook (`config_from_tags` in
//! steps/mod.rs) — `Capabilities` overrides become the `--config <file>`
//! the old harness used (`[s3] checksum = true`, `[s3] max_buckets = 3`),
//! and the fs-scanner variant becomes `TINIO_SCANNER` (`@cold-listing` →
//! on, plain → off).
use std::{
    env, fs,
    fs::File,
    io::{BufRead, BufReader, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use assert_cmd::{Command as AssertCmd, cargo::cargo_bin};
use cucumber::{given, then, when};
use tinio_sandbox::Event;
use tinio_util::testing::{job, kill_and_reap};

use super::{FsKind, World, common::deterministic_bytes, has_tag};
use crate::_server::{
    _config::{Config, s3},
    Capabilities,
};

/// The fixed MinIO-convention credential pair the harness signs every
/// request with, handed to the child in the two variables the resolution
/// reads (`contracts/config.md:110`): the CLI serves `[auth]`, and a first
/// start *generates* a random pair into the config it creates, so without
/// these every scenario would be refused with `SignatureDoesNotMatch`.
pub const ACCESS_KEY: &str = "minioadmin";
pub const SECRET_KEY: &str = "minioadmin";

/// How long a spawned child gets to report the address it bound before the
/// scenario fails rather than hanging on a pipe that will never carry it.
const START_TIMEOUT: Duration = Duration::from_secs(30);

/// The workspace target dir (`CARGO_TARGET_DIR` override honored).
fn target_dir() -> PathBuf {
    if let Ok(dir) = env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(dir);
    }
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target");
    p
}

/// @external scenarios only: a spawned `tinio server` + one client
/// session. The child is killed on drop (synchronously, so the redb lock
/// is released before a sibling server starts).
#[derive(Debug)]
pub struct External {
    /// The `tinio server` subprocess (killed on drop).
    pub child: Child,
    /// `http://127.0.0.1:<port>` — the client-facing base URL.
    pub base_url: String,
    /// The endpoint the aws steps dial (`--endpoint-url`): [`Self::base_url`] until a scenario
    /// points the client at the ephemeral second server (`the client targets the {word} server`),
    /// which is the only observation point that server has.
    target_url: String,
    /// Scratch for client files (uploads, downloads, configs).
    pub workdir: tempfile::TempDir,
    /// The served storage root (`workdir/root`) — out-of-band file drops
    /// (cold listing, pagination truncation) write under it.
    pub root: PathBuf,
}

impl External {
    /// Spawn the binary per `caps`/`fs_kind` — the tag→config
    /// mapping from `config_from_tags` (steps/mod.rs), translated into the
    /// old harness's mechanisms: `Capabilities` overrides become a
    /// `--config <file>` (`[s3] checksum = true`, `[s3] max_buckets = 3`),
    /// the fs-scanner variant becomes `TINIO_SCANNER` (`@cold-listing` →
    /// on, plain → off).
    pub fn start(caps: &Capabilities, fs_kind: FsKind) -> Self {
        let workdir = tempfile::tempdir().unwrap();
        let root = workdir.path().join("root");
        fs::create_dir_all(&root).unwrap();
        let config = config_for(caps).map(|text| {
            let path = workdir.path().join("config.toml");
            fs::write(&path, text).unwrap();
            path
        });
        let scanner = Some(matches!(fs_kind, FsKind::ColdListing(_)));
        let (child, endpoint) = spawn_child(&root, scanner, config.as_deref());
        let base_url = format!("http://{endpoint}");
        Self {
            child,
            base_url: base_url.clone(),
            target_url: base_url,
            workdir,
            root,
        }
    }
}

impl Drop for External {
    fn drop(&mut self) {
        terminate(&mut self.child);
    }
}

/// Synchronously kill + reap a spawned serve child (TerminateProcess on
/// Windows): the redb lock is released before a sibling server can start
/// on the same root. One home for the teardown ordering rationale — the
/// Drop impls of [`External`]/[`SpawnedServer`] and the not-ready panic
/// path all call it.
///
/// The sequence itself is [`kill_and_reap`]'s, spelled once for the whole
/// workspace: this name stays because the ordering rule above is this
/// harness's and not the shared helper's.
fn terminate(child: &mut Child) {
    kill_and_reap(child);
}

/// A running serve subprocess on an ephemeral loopback port (killed on
/// drop — the redb lock is released before a sibling server can start on
/// the same root).
#[derive(Debug)]
pub struct SpawnedServer {
    child: Child,
    endpoint: SocketAddr,
}

/// Spawn `tinio server <root> --port 0` and wait for its readiness event;
/// returns the child and the bound endpoint (`127.0.0.1:PORT`). `scanner`
/// sets `TINIO_SCANNER` per Some (None leaves it unset), `config` is passed
/// through `--config`.
///
/// The child inherits this process's environment, so the three variables
/// that would change what it serves are cleared first: a shell exporting
/// `TINIO_SCANNER` (the harness decides the scanner, not the caller
/// shell), `TINIO_READ_ONLY` (FR-023, which would otherwise put every
/// spawned server in read-only mode), and `TINIO_STATE_DIR` (which would
/// relocate its state). The `scanner` argument sets its variable back
/// afterwards.
///
/// **The credential pair is one thing the spawned server needs and the
/// retired example did not.** Every client below signs with the fixed
/// `minioadmin` pair, which the example accepted because it hard-coded it —
/// while the CLI reads `[auth]`, and a first start *generates* a random
/// pair into the config it creates, so a scenario would fail with
/// `SignatureDoesNotMatch` the moment the server under test changed.
/// `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` are the two names
/// `contracts/config.md:110` lists (not the `MINIO_*` names, which are the
/// data plane's fallback and not this chain's), and the resolution applies
/// them to `[auth]` — env, then `.env`, then the config file.
///
/// **The child is spawned through the job guard** (`tinio_util::testing::job::spawn`): on Windows
/// it is put in this test process's job, so a leg that dies **without unwinding** — the abort or
/// the killed run that no `Drop` reaches — takes the server with it instead of leaving an orphan
/// holding the redb lock and `target\<profile>\tinio.exe`. [`terminate`] and the two `Drop` impls
/// stay what they were: the guard is the backstop for the path that runs no `Drop`, never the
/// reaping.
fn spawn_child(root: &Path, scanner: Option<bool>, config: Option<&Path>) -> (Child, SocketAddr) {
    let mut cmd = Command::new(cargo_bin("tinio"));
    cmd.arg("server").arg(root).args(["--port", "0"]);
    if let Some(config) = config {
        cmd.arg("--config").arg(config);
    }
    cmd.env_remove("TINIO_SCANNER")
        .env_remove("TINIO_READ_ONLY")
        .env_remove("TINIO_STATE_DIR")
        .env_remove("TINIO_SANDBOX")
        .env("TINIO_ACCESS_KEY", ACCESS_KEY)
        .env("TINIO_SECRET_KEY", SECRET_KEY);
    if let Some(s) = scanner {
        cmd.env("TINIO_SCANNER", if s { "1" } else { "0" });
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child =
        job::spawn(&mut cmd).unwrap_or_else(|e| panic!("failed to spawn `tinio server`: {e}"));
    let stdout = child.stdout.take().expect("piped stdout");
    let endpoint = match wait_for_ready(stdout) {
        // The address travels as the value the child bound: it comes out of the event's typed
        // field and goes to a URL as it stands, so nothing here can disagree with what
        // `local_addr` reported.
        Some(Event::Listening { addr }) => addr,
        // **A refusal is reported as the refusal it is.** Discarding the event and reporting the
        // missing-frame case below made a server that refused to start read as the stale-binary
        // guess, which sends the reader after the wrong thing.
        Some(Event::Error { message }) => {
            terminate(&mut child);
            panic!("the server refused to start: {message}");
        }
        None => {
            terminate(&mut child);
            panic!(
                "the server reported no `listening` event — is a stale `tinio.exe` holding this \
                 root? ({} server --port 0)",
                cargo_bin("tinio").display()
            );
        }
    };
    (child, endpoint)
}

impl SpawnedServer {
    /// Serve `root` (caller keeps it) on `--port 0`; see [`spawn_child`].
    fn start(root: &Path, scanner: Option<bool>, config: Option<&Path>) -> Self {
        let (child, endpoint) = spawn_child(root, scanner, config);
        Self { child, endpoint }
    }

    /// The bound address, `127.0.0.1:PORT`.
    fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        // See [`terminate`] for the redb-lock ordering rationale.
        terminate(&mut self.child);
    }
}

/// Read the child's stdout until it reports its first event.
///
/// The child speaks the sandbox's event stream (design decision 19): one SSE frame per event —
/// an `event:` line, a `data:` line, closed by a blank line. **A frame is not a line**, so this
/// accumulates until the blank line and decodes the block; decoding line by line would hand
/// [`Event::decode`] half a frame. Text that is not a frame — a log a dependency wrote before
/// the subscriber was installed — does not decode and is skipped rather than fatal, which is
/// the property the framing buys. A reader thread keeps the pipe drained so the child never
/// blocks on a full buffer; the thread exits when the child dies.
fn wait_for_ready(stdout: impl Read + Send + 'static) -> Option<Event> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut frame = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            if line.trim_end().is_empty() {
                if let Some(event) = Event::decode(&frame) {
                    let _ = tx.send(event);
                    return;
                }
                frame.clear();
            } else {
                frame.push_str(line.trim_end());
                frame.push('\n');
            }
            line.clear();
        }
    });
    rx.recv_timeout(START_TIMEOUT).ok()
}

/// The `[s3]` config text for capability overrides (grilling Q4): the
/// spawned server receives the same tag→config mapping as the
/// in-process hook, serialized through the config schema's own serde
/// types — one home for the knob list (a knob the schema gains is a
/// knob the scenarios can set). None when every knob sits at its
/// default (no file).
fn config_for(caps: &Capabilities) -> Option<String> {
    if *caps == Capabilities::default() {
        return None;
    }
    let config = Config {
        s3: Some(s3::Config {
            capabilities: *caps,
            ..Default::default()
        }),
        ..Default::default()
    };
    Some(toml::to_string(&config).expect("capabilities serialize to toml"))
}

/// aws cli v2, pre-wired for path-style SigV4 against `base_url`.
fn aws(base_url: &str, args: &[&str]) -> AssertCmd {
    let mut c = AssertCmd::new("aws");
    c.env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
        .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .arg("--endpoint-url")
        .arg(base_url)
        .arg("--region")
        .arg("us-east-1")
        .args(args);
    c
}

/// (Re)create the `tinio` rclone remote pointing at `base_url`.
///
/// rclone config is isolated to `config` per scenario: scenarios run in
/// parallel under cargo, and the bash harness used to race the user's
/// `~/.config/rclone/rclone.conf` (and clobber its `tinio` remote).
#[derive(Debug)]
struct Rclone {
    config: PathBuf,
}

impl Rclone {
    /// Use `config` (a path inside the scenario's scratch dir) as the
    /// config file for every rclone invocation.
    fn new(config: PathBuf) -> Self {
        Self { config }
    }

    /// `rclone --config <config> <args>`.
    fn cmd(&self, args: &[&str]) -> AssertCmd {
        let mut c = AssertCmd::new("rclone");
        c.arg("--config").arg(&self.config).args(args);
        c
    }

    /// (Re)create the `tinio` remote pointing at `base_url`.
    fn remote(&self, base_url: &str) -> AssertCmd {
        let mut c = self.cmd(&[
            "config",
            "create",
            "tinio",
            "s3",
            "provider",
            "Minio",
            "access_key_id",
            ACCESS_KEY,
            "secret_access_key",
            SECRET_KEY,
        ]);
        c.arg("endpoint").arg(base_url);
        c
    }
}

/// `mc` with an isolated config dir (same rationale as [`Rclone`]).
#[derive(Debug)]
struct Mc {
    config_dir: PathBuf,
}

impl Mc {
    fn new(config_dir: PathBuf) -> Self {
        Self { config_dir }
    }

    /// `mc --config-dir <dir> <args>`.
    fn cmd(&self, args: &[&str]) -> AssertCmd {
        let mut c = AssertCmd::new("mc");
        c.arg("--config-dir").arg(&self.config_dir).args(args);
        c
    }

    /// (Re)set the `tinio` alias pointing at `base_url`.
    fn alias(&self, base_url: &str) -> AssertCmd {
        let mut c = self.cmd(&["alias", "set", "tinio"]);
        c.arg(base_url).args([ACCESS_KEY, SECRET_KEY]);
        c
    }
}

/// The venv python for the boto3 scenario. boto3 must run inside an
/// isolated venv (never the system python): `TINIO_BOTO3_PYTHON`
/// overrides the conventional `<target>/tinio-e2e-venv` venv.
fn boto3_python() -> PathBuf {
    if let Some(p) = env::var_os("TINIO_BOTO3_PYTHON") {
        return PathBuf::from(p);
    }
    let mut p = target_dir().join("tinio-e2e-venv");
    p.push(if cfg!(windows) { "Scripts" } else { "bin" });
    p.push(if cfg!(windows) {
        "python.exe"
    } else {
        "python3"
    });
    p
}

/// Run a boto3 scenario script against `endpoint` (best-effort client:
/// needs the venv from [`boto3_python`]).
fn boto3(endpoint: &str, script: &Path) -> AssertCmd {
    let mut c = AssertCmd::new(boto3_python());
    c.arg(script).arg(endpoint);
    c
}

/// The checked-in boto3 scenario scripts stay in `tinio-server/tests/`
/// (one copy, shared with the old tests until they are deleted).
fn boto3_script(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../tinio-server/tests");
    p.push(name);
    p
}

/// Assert two files are byte-identical (upload/download round-trips): a
/// size compare first, then a chunked stream compare — a full read of
/// both multi-MB files never happens when the sizes differ.
fn files_equal(a: &Path, b: &Path) {
    let ma = fs::metadata(a).unwrap();
    let mb = fs::metadata(b).unwrap();
    assert_eq!(
        ma.len(),
        mb.len(),
        "files differ: {} vs {} (sizes {} vs {})",
        a.display(),
        b.display(),
        ma.len(),
        mb.len()
    );
    let mut fa = File::open(a).unwrap();
    let mut fb = File::open(b).unwrap();
    let (mut buf_a, mut buf_b) = ([0u8; 64 * 1024], [0u8; 64 * 1024]);
    loop {
        let na = fa.read(&mut buf_a).unwrap();
        let nb = fb.read(&mut buf_b).unwrap();
        if na != nb || buf_a[..na] != buf_b[..nb] {
            panic!("files differ: {} vs {}", a.display(), b.display());
        }
        if na == 0 {
            return;
        }
    }
}

/// Whether `name` (an executable, e.g. `aws`) is on PATH.
fn on_path(name: &str) -> bool {
    let Some(path) = env::var_os("PATH") else {
        return false;
    };
    let names: Vec<String> = if cfg!(windows) {
        vec![
            format!("{name}.exe"),
            format!("{name}.cmd"),
            format!("{name}.bat"),
            name.to_string(),
        ]
    } else {
        vec![name.to_string()]
    };
    env::split_paths(&path).any(|dir| names.iter().any(|n| dir.join(n).is_file()))
}

/// The `#[before]` hook's presence checks for the @external scenario
/// families (a missing client panics with a filter/setup hint, mirroring
/// the old `#[ignore]` semantics: `@interop` is CI-gated, `@boto3`/`@mc`
/// are manual).
pub fn check_presence(tags: &[String]) {
    let tagged = |t: &str| has_tag(tags, t);
    if tagged("interop") {
        for tool in ["aws", "rclone"] {
            assert!(
                on_path(tool),
                "`{tool}` not found on PATH — the @interop scenarios need aws cli v2 and \
                 rclone; run them in WSL2 or filter them out (`--tags 'not @interop'`)"
            );
        }
    }
    if tagged("boto3") {
        let python = boto3_python();
        assert!(
            python.exists(),
            "boto3 venv python not found at {} — create it and install boto3:\n\
             python3 -m venv <target>/tinio-e2e-venv && <venv>/pip install boto3\n\
             (or point TINIO_BOTO3_PYTHON at your own venv python)",
            python.display()
        );
    }
    if tagged("mc") {
        assert!(
            on_path("mc"),
            "`mc` not found on PATH — filter it out (`--tags 'not @mc'`)"
        );
    }
}

/// The scenario's scratch path for `name` (may contain `/` for subdirs).
fn scratch(world: &World, name: &str) -> PathBuf {
    let ext = world.ext.as_ref().expect("external server running");
    ext.workdir.path().join(name)
}

/// Split a client command line into argv: whitespace-separated words;
/// double-quoted segments stay one word (quotes stripped — the shell
/// convention the old bash scripts used). `{work}` expands to the scratch
/// workdir, `{captured}` to the last captured client output.
fn tokenize(world: &World, command: &str) -> Vec<String> {
    let work = world
        .ext
        .as_ref()
        .expect("external server running")
        .workdir
        .path()
        .to_string_lossy()
        .into_owned();
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in command.chars() {
        match c {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !cur.is_empty() {
                    args.push(
                        cur.replace("{work}", &work)
                            .replace("{captured}", &world.ext_captured),
                    );
                    cur.clear();
                }
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        args.push(
            cur.replace("{work}", &work)
                .replace("{captured}", &world.ext_captured),
        );
    }
    args
}

/// The tokenized command as `&str` args (the wrapper signatures).
fn arg_refs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

/// Store the last external client run's stdout/stderr for the Then steps.
fn save_output(world: &mut World, out: &Output) {
    world.ext_output = String::from_utf8_lossy(&out.stdout).into_owned();
    world.ext_error = String::from_utf8_lossy(&out.stderr).into_owned();
}

// --- run steps (registered under every keyword a feature uses) ----------

/// Run `cmd`, assert its exit status matches `expect_success`, and store
/// the output for the Then steps — the tail every client-run step shares
/// (the run steps differ only in the wrapper they build).
fn run_external(world: &mut World, cmd: &mut AssertCmd, expect_success: bool) {
    let assert = cmd.assert();
    let out = if expect_success {
        assert.success()
    } else {
        assert.failure()
    }
    .get_output()
    .clone();
    save_output(world, &out);
}

/// The configured wrapper for a `{work}`-expanded aws command line, dialled at the scenario's
/// **current** target — the first server unless a step switched it.
fn aws_cmd(world: &World, command: &str) -> AssertCmd {
    let ext = world.ext.as_ref().expect("external server running");
    aws(&ext.target_url, &arg_refs(&tokenize(world, command)))
}

#[given(regex = r"I run aws (.*)")]
#[when(regex = r"I run aws (.*)")]
#[then(regex = r"I run aws (.*)")]
async fn run_aws(world: &mut World, command: String) {
    let mut cmd = aws_cmd(world, &command);
    run_external(world, &mut cmd, true);
}

/// A run that must fail (the error-path legs): the client's stderr lands
/// in `world.ext_error` for `the external client error contains …`.
/// Constrained to `s3…` subcommands so the wrong-credentials variant
/// below stays unambiguous (cucumber-rs panics on overlapping matches;
/// the regex crate has no lookahead). Extend the class for a future
/// non-s3 command instead of widening to `.*`.
#[given(regex = r"I try aws (s3[a-z]*(?: .*)?)")]
#[when(regex = r"I try aws (s3[a-z]*(?: .*)?)")]
#[then(regex = r"I try aws (s3[a-z]*(?: .*)?)")]
async fn try_aws(world: &mut World, command: String) {
    let mut cmd = aws_cmd(world, &command);
    run_external(world, &mut cmd, false);
}

/// A failing run signed with wrong credentials (US3-AS2): the server must
/// reject the request with the framework's auth error and perform no
/// operation — aws prints `InvalidAccessKeyId` / `SignatureDoesNotMatch`
/// to stderr for a 403.
#[given(regex = r"I try aws with wrong credentials (.*)")]
#[when(regex = r"I try aws with wrong credentials (.*)")]
#[then(regex = r"I try aws with wrong credentials (.*)")]
async fn try_aws_wrong_credentials(world: &mut World, command: String) {
    let mut cmd = aws_cmd(world, &command);
    cmd.env("AWS_ACCESS_KEY_ID", "wrong-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "wrong-secret-key");
    run_external(world, &mut cmd, false);
}

#[given(regex = r"I run rclone (.*)")]
#[when(regex = r"I run rclone (.*)")]
#[then(regex = r"I run rclone (.*)")]
async fn run_rclone(world: &mut World, command: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Rclone::new(ext.workdir.path().join("rclone.conf"))
        .cmd(&arg_refs(&tokenize(world, &command)));
    run_external(world, &mut cmd, true);
}

#[given(expr = "I configure the rclone remote")]
#[when(expr = "I configure the rclone remote")]
#[then(expr = "I configure the rclone remote")]
async fn configure_rclone(world: &mut World) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Rclone::new(ext.workdir.path().join("rclone.conf")).remote(&ext.base_url);
    run_external(world, &mut cmd, true);
}

#[given(regex = r"I run mc (.*)")]
#[when(regex = r"I run mc (.*)")]
#[then(regex = r"I run mc (.*)")]
async fn run_mc(world: &mut World, command: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Mc::new(ext.workdir.path().join("mc")).cmd(&arg_refs(&tokenize(world, &command)));
    run_external(world, &mut cmd, true);
}

#[given(expr = "I configure the mc alias")]
#[when(expr = "I configure the mc alias")]
#[then(expr = "I configure the mc alias")]
async fn configure_mc(world: &mut World) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Mc::new(ext.workdir.path().join("mc")).alias(&ext.base_url);
    run_external(world, &mut cmd, true);
}

/// Run one of the checked-in boto3 scripts against the server (the venv
/// python; the script asserts its own journey, the feature asserts the
/// OK marker in the output).
#[given(expr = "I run the boto3 script {string}")]
#[when(expr = "I run the boto3 script {string}")]
#[then(expr = "I run the boto3 script {string}")]
async fn run_boto3(world: &mut World, name: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let script = boto3_script(&name);
    assert!(
        script.exists(),
        "boto3 script not found at {}",
        script.display()
    );
    let endpoint = ext
        .base_url
        .strip_prefix("http://")
        .expect("base url has the http:// prefix");
    let mut cmd = boto3(endpoint, &script);
    run_external(world, &mut cmd, true);
}

/// The ephemeral `--port 0` leg (journey): a second server starts while
/// the first keeps serving.
#[given(expr = "I start a second server")]
#[when(expr = "I start a second server")]
#[then(expr = "I start a second server")]
async fn start_second_server(world: &mut World) {
    let ext = world.ext.as_ref().expect("external server running");
    let root = ext.workdir.path().join("root2");
    fs::create_dir_all(&root).unwrap();
    let spawned = SpawnedServer::start(&root, None, None);
    assert!(
        spawned.endpoint().port() != 0,
        "the second server must report the address it actually bound, not the `:0` it asked for"
    );
    world.ext_second = Some(spawned);
}

/// Point the aws steps at one of the scenario's two servers.
///
/// **The second server's only observation point.** `I start a second server` proves it bound an
/// ephemeral port and nothing more, so `journey.feature`'s "serves alongside the first server"
/// needs an op that dials *it* — and the state it then holds is what tells the two apart. An
/// explicit target rather than "the second one from now on", because the same scenario also has
/// to show the first still answering.
#[given(expr = "the client targets the {word} server")]
#[when(expr = "the client targets the {word} server")]
#[then(expr = "the client targets the {word} server")]
async fn client_targets(world: &mut World, which: String) {
    let url = match which.as_str() {
        "first" => world
            .ext
            .as_ref()
            .expect("external server running")
            .base_url
            .clone(),
        "second" => format!(
            "http://{}",
            world
                .ext_second
                .as_ref()
                .expect("the scenario started a second server")
                .endpoint()
        ),
        other => panic!("a client target is `first` or `second`, not {other:?}"),
    };
    let ext = world.ext.as_mut().expect("external server running");
    ext.target_url = url;
}

// --- file-preparation steps ----------------------------------------------

#[given(expr = "I write {string} to the scratch file {string}")]
#[when(expr = "I write {string} to the scratch file {string}")]
#[then(expr = "I write {string} to the scratch file {string}")]
async fn write_text(world: &mut World, text: String, name: String) {
    fs::write(scratch(world, &name), text).unwrap();
}

#[given(expr = "I write {int} deterministic bytes to the scratch file {string}")]
#[when(expr = "I write {int} deterministic bytes to the scratch file {string}")]
#[then(expr = "I write {int} deterministic bytes to the scratch file {string}")]
async fn write_deterministic(world: &mut World, n: u64, name: String) {
    // The ONE byte generator the steps share (F14 — see common.rs).
    fs::write(scratch(world, &name), deterministic_bytes(n)).unwrap();
}

/// Out-of-band files dropped by hand under the served root (cold listing,
/// pagination truncation): `{stem}1.txt` … `{stem}{count}.txt`.
#[given(expr = "the served root contains a bucket {string} with {int} files {string}")]
#[when(expr = "the served root contains a bucket {string} with {int} files {string}")]
#[then(expr = "the served root contains a bucket {string} with {int} files {string}")]
async fn served_root_files(world: &mut World, bucket: String, count: u64, stem: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let dir = ext.root.join(&bucket);
    fs::create_dir_all(&dir).unwrap();
    for i in 1..=count {
        fs::write(dir.join(format!("{stem}{i}.txt")), "x").unwrap();
    }
}

// --- Then steps -----------------------------------------------------------

#[then(expr = "the external client output contains {string}")]
async fn output_contains(world: &mut World, text: String) {
    assert!(
        world.ext_output.contains(&text),
        "client output missing {text:?}: {}",
        world.ext_output
    );
}

/// The mc stat ETag check: the bash baseline greps `etag` case-insensitively
/// (mc prints `ETag` in newer releases, `Etag` in older ones).
#[then(expr = "the external client output contains {string} ignoring case")]
async fn output_contains_ignore_case(world: &mut World, text: String) {
    let lower = text.to_lowercase();
    assert!(
        world.ext_output.to_lowercase().contains(&lower),
        "client output missing {text:?} (case-insensitive): {}",
        world.ext_output
    );
}

#[then(expr = "the external client output does not contain {string}")]
async fn output_not_contains(world: &mut World, text: String) {
    assert!(
        !world.ext_output.contains(&text),
        "client output unexpectedly contains {text:?}: {}",
        world.ext_output
    );
}

/// Trimmed equality — for `--output text` values like `KeyCount`.
#[then(expr = "the external client output equals {string}")]
async fn output_equals(world: &mut World, text: String) {
    assert_eq!(
        world.ext_output.trim(),
        text.trim(),
        "client output mismatch"
    );
}

#[then("the external client output is not empty")]
async fn output_not_empty(world: &mut World) {
    assert!(
        !world.ext_output.trim().is_empty(),
        "client output is empty"
    );
}

#[then(expr = "the external client error contains {string}")]
async fn error_contains(world: &mut World, text: String) {
    assert!(
        world.ext_error.contains(&text),
        "client error missing {text:?}: {}",
        world.ext_error
    );
}

#[then(expr = "I capture the client output")]
async fn capture_output(world: &mut World) {
    world.ext_captured = world.ext_output.trim().to_string();
}

#[then(expr = "the scratch file {string} equals the scratch file {string}")]
async fn scratch_files_equal(world: &mut World, a: String, b: String) {
    files_equal(&scratch(world, &a), &scratch(world, &b));
}

#[then(expr = "the scratch file {string} is {int} bytes")]
async fn scratch_file_len(world: &mut World, name: String, n: u64) {
    let len = fs::metadata(scratch(world, &name)).unwrap().len();
    assert_eq!(len, n, "size mismatch for {name}");
}

#[then(expr = "the scratch file {string} matches the prefix of the scratch file {string}")]
async fn scratch_file_prefix(world: &mut World, part: String, whole: String) {
    let part = fs::read(scratch(world, &part)).unwrap();
    let whole = fs::read(scratch(world, &whole)).unwrap();
    assert!(
        whole.starts_with(&part),
        "{} is not a prefix of {}",
        part.len(),
        whole.len()
    );
}
