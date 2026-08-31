//! Shared harness for the third-party-client e2e tests (replaces the
//! `e2e/interop/*.sh` bash scenarios; T032–T036).
//!
//! Spawns the real `serve` example binary as a subprocess — real redb
//! database, scanner, sweep — bound to `127.0.0.1` on an ephemeral port
//! with no client-side addressing overrides (SC-002), then drives
//! third-party S3 clients (aws cli v2, rclone, mc, boto3) against it.
//! Scenario tests are `#[ignore]`d: the CI interop stage and targeted
//! manual runs enable them explicitly (`cargo test ... -- --ignored`).
//!
//! Process lifecycle is native Rust: `Child::kill` + `wait` terminate the
//! server synchronously (TerminateProcess on Windows), so the redb lock is
//! released before a sibling server starts — the bash harness leaked
//! servers here (e2e/interop/TROUBLESHOOTING.md §4).
//!
//! Each test binary compiles its own copy of this module.
#![allow(dead_code)]

use std::{
    env, fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use assert_cmd::Command as AssertCmd;

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
/// not examples — so resolve it relative to the workspace target dir.
pub fn serve_bin() -> PathBuf {
    let name = format!("serve{}", if cfg!(windows) { ".exe" } else { "" });
    let p = target_dir().join("debug/examples").join(name);
    assert!(
        p.exists(),
        "serve example binary not found at {} — run `cargo build -p tinio-server --example serve`",
        p.display()
    );
    p
}

/// A running serve subprocess on an ephemeral loopback port.
pub struct Server {
    child: Child,
    root: PathBuf,
    dir: Option<tempfile::TempDir>,
    endpoint: String,
}

impl Server {
    /// Fresh tempdir root, scanner left at its config default.
    pub fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        Self::start_inner(&root, None, Some(dir), None)
    }

    /// Serve `root` (caller keeps it) with `TINIO_SCANNER` set per
    /// `scanner` (None leaves it unset) — advanced.rs reuses one root for
    /// the scanner-on / scanner-off pair.
    pub fn start_at(root: &Path, scanner: Option<bool>) -> Self {
        Self::start_inner(root, scanner, None, None)
    }

    /// Serve `root` (caller keeps it) with an additional
    /// `--config <path>` — the serve-wiring proof: a configured
    /// `[s3] max_buckets` must reach the running plane.
    pub fn start_with_config(root: &Path, config: &Path) -> Self {
        Self::start_inner(root, None, None, Some(config))
    }

    fn start_inner(
        root: &Path,
        scanner: Option<bool>,
        dir: Option<tempfile::TempDir>,
        config: Option<&Path>,
    ) -> Self {
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
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "server did not print `{READY}` — is a stale serve.exe holding this root? \
                     ({} `--port 0`)",
                    serve_bin().display()
                );
            }
        };
        Self {
            child,
            root: root.to_path_buf(),
            dir,
            endpoint,
        }
    }

    /// The bound address, `127.0.0.1:PORT`.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The storage root.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Synchronous terminate + reap: the redb lock is released before a
        // sibling server can start on the same root.
        let _ = self.child.kill();
        let _ = self.child.wait();
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

/// aws cli v2, pre-wired for path-style SigV4 against `endpoint`.
pub fn aws(endpoint: &str, args: &[&str]) -> AssertCmd {
    let mut c = AssertCmd::new("aws");
    c.env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
        .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .arg("--endpoint-url")
        .arg(format!("http://{endpoint}"))
        .arg("--region")
        .arg("us-east-1")
        .args(args);
    c
}

/// `aws s3 <op> <args>`.
pub fn aws_s3(endpoint: &str, op: &str, args: &[&str]) -> AssertCmd {
    let mut c = aws(endpoint, &["s3", op]);
    c.args(args);
    c
}

/// (Re)create the `tinio` rclone remote pointing at `endpoint`.
///
/// rclone config is isolated to `config` per test: test binaries run in
/// parallel under cargo, and the bash harness used to race the user's
/// `~/.config/rclone/rclone.conf` (and clobber its `tinio` remote).
pub struct Rclone {
    config: PathBuf,
}

impl Rclone {
    /// Use `config` (a path inside the test's scratch dir) as the config
    /// file for every rclone invocation.
    pub fn new(config: PathBuf) -> Self {
        Self { config }
    }

    /// `rclone --config <config> <args>`.
    pub fn cmd(&self, args: &[&str]) -> AssertCmd {
        let mut c = AssertCmd::new("rclone");
        c.arg("--config").arg(&self.config).args(args);
        c
    }

    /// (Re)create the `tinio` remote pointing at `endpoint`.
    pub fn remote(&self, endpoint: &str) -> AssertCmd {
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
        c.arg("endpoint").arg(format!("http://{endpoint}"));
        c
    }
}

/// `mc` with an isolated config dir (same rationale as [`Rclone`]).
pub struct Mc {
    config_dir: PathBuf,
}

impl Mc {
    pub fn new(config_dir: PathBuf) -> Self {
        Self { config_dir }
    }

    /// `mc --config-dir <dir> <args>`.
    pub fn cmd(&self, args: &[&str]) -> AssertCmd {
        let mut c = AssertCmd::new("mc");
        c.arg("--config-dir").arg(&self.config_dir).args(args);
        c
    }

    /// (Re)set the `tinio` alias pointing at `endpoint`.
    pub fn alias(&self, endpoint: &str) -> AssertCmd {
        let mut c = self.cmd(&["alias", "set", "tinio"]);
        c.arg(format!("http://{endpoint}"))
            .args([ACCESS_KEY, SECRET_KEY]);
        c
    }
}

/// The venv python for the boto3 scenario. boto3 must run inside an
/// isolated venv (never the system python): `TINIO_BOTO3_PYTHON`
/// overrides the conventional `<target>/tinio-e2e-venv` venv.
pub fn boto3_python() -> PathBuf {
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

/// Run the boto3 journey script against `endpoint` (best-effort client:
/// needs the venv from [`boto3_python`]).
pub fn boto3(endpoint: &str, script: &Path) -> AssertCmd {
    let mut c = AssertCmd::new(boto3_python());
    c.arg(script).arg(endpoint);
    c
}

/// Write `n` deterministic pseudo-random bytes (same content every run).
pub fn write_bytes(dest: &Path, n: usize) {
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let mut buf = Vec::with_capacity(n);
    for _ in 0..n {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        buf.push(state as u8);
    }
    fs::write(dest, buf).unwrap();
}

/// Assert two files are byte-identical (upload/download round-trips).
pub fn files_equal(a: &Path, b: &Path) {
    assert_eq!(
        fs::read(a).unwrap(),
        fs::read(b).unwrap(),
        "files differ: {} vs {}",
        a.display(),
        b.display()
    );
}
