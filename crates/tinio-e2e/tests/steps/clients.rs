//! External-client harness and step definitions for the `@external`
//! scenarios (`@interop`/`@boto3`/`@mc`), ported from
//! `tinio-server/tests/e2e/mod.rs` plus the client-leg steps that drive
//! `journey.rs`/`advanced.rs`/`boto3.rs`/`mc.rs`/`edge.rs` and the CI
//! baseline `e2e/interop/*.sh`.
//!
//! The `#[before]` hook (steps/mod.rs) spawns the real `serve` example
//! binary as a subprocess — real redb database, scanner, sweep — bound to
//! `127.0.0.1` on an ephemeral port with no client-side addressing
//! overrides (SC-002), then drives third-party S3 clients (aws cli v2,
//! rclone, mc, boto3) against it. Process lifecycle is native Rust:
//! `Child::kill` + `wait` terminate the server synchronously
//! (TerminateProcess on Windows), so the redb lock is released before a
//! sibling server starts.
//!
//! Config passthrough (grilling Q4): the spawned binary receives the same
//! tag→config mapping as the in-process hook (`config_from_tags` in
//! steps/mod.rs) — `Capabilities` overrides become the `--config <file>`
//! the old harness used (`[s3] checksum = true`, `[s3] max_buckets = 3`),
//! and the fs-scanner variant becomes `TINIO_SCANNER` (`@cold-listing` →
//! on, plain → off).
use std::{
    env, fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use assert_cmd::Command as AssertCmd;
use cucumber::{given, then, when};

use super::{FsKind, World, common::deterministic_bytes, has_tag};
use crate::_server::_config::{Config, s3};

/// The fixed MinIO-convention credential pair the serve example accepts.
pub const ACCESS_KEY: &str = "minioadmin";
pub const SECRET_KEY: &str = "minioadmin";

const READY: &str = "listening on ";
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

/// The serve example binary. Cargo sets `CARGO_BIN_EXE_*` for bins only —
/// not examples — so resolve it relative to this test binary's own
/// location: `<target>/<profile>/deps/cucumber-<hash>` means the example
/// sits in the sibling `examples/` dir of the same profile dir. The
/// example must be built with the SAME profile as the test run (CI builds
/// both with `--profile ci`): a profile mismatch looks in the wrong dir —
/// or worse, finds a stale binary restored from a cache.
fn serve_bin() -> PathBuf {
    let name = format!("serve{}", if cfg!(windows) { ".exe" } else { "" });
    let exe = env::current_exe().expect("current exe path");
    let profile_dir = exe
        .parent() // .../deps
        .and_then(Path::parent) // .../<profile>
        .expect("test binary lives in <target>/<profile>/deps");
    let p = profile_dir.join("examples").join(name);
    assert!(
        p.exists(),
        "serve example binary not found at {} — build it with the same profile as the tests \
         (`cargo build -p tinio-server --example serve`; CI adds `--profile ci`)",
        p.display()
    );
    p
}

/// @external scenarios only: a spawned `serve` binary + one client
/// session. The child is killed on drop (synchronously, so the redb lock
/// is released before a sibling server starts).
#[derive(Debug)]
pub struct External {
    /// The serve binary subprocess (killed on drop).
    pub child: Child,
    /// `http://127.0.0.1:<port>` — the client-facing base URL.
    pub base_url: String,
    /// Scratch for client files (uploads, downloads, configs).
    pub workdir: tempfile::TempDir,
    /// The served storage root (`workdir/root`) — out-of-band file drops
    /// (cold listing, pagination truncation) write under it.
    pub root: PathBuf,
}

impl External {
    /// Spawn the serve binary per `caps`/`fs_kind` — the tag→config
    /// mapping from `config_from_tags` (steps/mod.rs), translated into the
    /// old harness's mechanisms: `Capabilities` overrides become a
    /// `--config <file>` (`[s3] checksum = true`, `[s3] max_buckets = 3`),
    /// the fs-scanner variant becomes `TINIO_SCANNER` (`@cold-listing` →
    /// on, plain → off).
    pub fn start(caps: &crate::_server::Capabilities, fs_kind: FsKind) -> Self {
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
        Self {
            child,
            base_url: format!("http://{endpoint}"),
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
fn terminate(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// A running serve subprocess on an ephemeral loopback port (killed on
/// drop — the redb lock is released before a sibling server can start on
/// the same root).
#[derive(Debug)]
pub struct SpawnedServer {
    child: Child,
    endpoint: String,
}

/// Spawn the serve binary serving `root` on `--port 0` and wait for the
/// readiness line; returns the child and the bound endpoint
/// (`127.0.0.1:PORT`). `scanner` sets `TINIO_SCANNER` per Some (None
/// leaves it unset), `config` is passed through `--config`.
fn spawn_child(root: &Path, scanner: Option<bool>, config: Option<&Path>) -> (Child, String) {
    let mut cmd = Command::new(serve_bin());
    cmd.arg(root).arg("--port").arg("0");
    if let Some(config) = config {
        cmd.arg("--config").arg(config);
    }
    if let Some(s) = scanner {
        cmd.env("TINIO_SCANNER", if s { "1" } else { "0" });
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn {}: {e}", serve_bin().display()));
    let stdout = child.stdout.take().expect("piped stdout");
    let endpoint = match wait_for_ready(stdout) {
        Some(line) => line
            .trim()
            .strip_prefix(READY)
            .expect("ready line prefix")
            .to_string(),
        None => {
            terminate(&mut child);
            panic!(
                "server did not print `{READY}` — is a stale serve.exe holding this root? \
                 ({} `--port 0`)",
                serve_bin().display()
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
    fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        // See [`terminate`] for the redb-lock ordering rationale.
        terminate(&mut self.child);
    }
}

/// Read the child's stdout until the `listening on` readiness line (the
/// serve example prints exactly one line to stdout). A reader thread keeps
/// the pipe drained so the child never blocks on a full buffer; the thread
/// exits when the child dies.
fn wait_for_ready(stdout: impl Read + Send + 'static) -> Option<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            if line.contains(READY) {
                let _ = tx.send(line);
                return;
            }
            line.clear();
        }
    });
    rx.recv_timeout(START_TIMEOUT).ok()
}

/// The `[s3]` config text for capability overrides (grilling Q4): the
/// spawned serve binary receives the same tag→config mapping as the
/// in-process hook, serialized through the config schema's own serde
/// types — one home for the knob list (a knob the schema gains is a
/// knob the scenarios can set). None when every knob sits at its
/// default (no file).
fn config_for(caps: &crate::_server::Capabilities) -> Option<String> {
    if *caps == crate::_server::Capabilities::default() {
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
    let mut fa = fs::File::open(a).unwrap();
    let mut fb = fs::File::open(b).unwrap();
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

/// The configured wrapper for a `{work}`-expanded aws command line.
fn aws_cmd(world: &World, command: &str) -> AssertCmd {
    let ext = world.ext.as_ref().expect("external server running");
    aws(&ext.base_url, &arg_refs(&tokenize(world, command)))
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
/// the first keeps serving; the bucket ops stay on the first server.
#[given(expr = "I start a second server")]
#[when(expr = "I start a second server")]
#[then(expr = "I start a second server")]
async fn start_second_server(world: &mut World) {
    let ext = world.ext.as_ref().expect("external server running");
    let root = ext.workdir.path().join("root2");
    fs::create_dir_all(&root).unwrap();
    let spawned = SpawnedServer::start(&root, None, None);
    assert!(
        !spawned.endpoint().is_empty(),
        "second server must bind an ephemeral port"
    );
    world.ext_second = Some(spawned);
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
