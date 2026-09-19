//! The `serve` example binary in read-only mode (FR-023).
//!
//! The coverage hole this closes: every other read-only test drives the
//! data plane in process (`tests/read_only.rs`), and the only thing that
//! spawns the example binary is the CI-only `@interop` cucumber harness
//! (it needs aws-cli/rclone, so no local run reaches it) — `serve.rs`'s
//! `main`, the mode resolution and the state-dir choice included, is
//! executed by no local test. These legs spawn the real binary and observe
//! it from outside: its stdout, its stderr, its exit status, and the S3
//! answers on the wire.
//!
//! Signing is mandatory. The example always configures the fixed
//! `minioadmin` pair (`plane_for`), so an unsigned request is refused with
//! `Signature is required` before it reaches the layer under test — an
//! unsigned probe would answer the same thing whether or not the resolved
//! mode was wired into the plane. Every request below carries a header-auth
//! SigV4 signature: the compact signer `data.rs`'s `acl` test module wrote
//! for exactly this situation, mirrored here.
//!
//! The mode discriminator needs no fixture: a signed `PUT` to a bucket that
//! does not exist answers `404 NoSuchBucket` read-write (the reference
//! answer `read_only.rs`'s leg 4 pins) and `403 AccessDenied` read-only. A
//! signed `GET` of a file planted in the served root shows reads still
//! serve.
//!
//! Each spawned child is killed and reaped before its leg ends, on the
//! assertion-failure path too: [`Serve`]'s `Drop` does it, so the redb lock
//! is released before a sibling server starts on the same root (the
//! ordering `crates/tinio-e2e/tests/steps/clients.rs`'s `terminate`
//! documents).

mod common;

use std::{
    env, fs,
    io::{BufRead, BufReader, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use common::{Wire, plant, send};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use time::OffsetDateTime;
use tinio_config::{sources::READ_ONLY_VAR, state::STATE_DIR_VAR};
use tinio_core::acl::DEFAULT_ROOT_ACCESS_KEY;
use tinio_util::testing::wait_timeout_secs;
use tokio::time::sleep;

/// The fixed credential pair the example always accepts: the access key is
/// the core convention constant, the secret the literal
/// `identity::root_auth_pair` falls back to with no `[auth]` section.
const SECRET_KEY: &str = "minioadmin";

/// The `Host` every request carries and signs. The example binds
/// `127.0.0.1`, and s3s verifies the signature over the `Host` header the
/// client sent — never against the bound address — so the value is the
/// signer's to choose (`data.rs`'s acl test picks the same one).
const HOST: &str = "localhost";

/// The one line the example writes to stdout (the harness parses it).
const READY: &str = "listening on ";

/// The serve example binary: cargo sets `CARGO_BIN_EXE_*` for bins only,
/// never for examples, so it is resolved relative to this test binary's own
/// location (`<target>/<profile>/deps/serve_read_only-<hash>` means the
/// example sits in the sibling `examples/` dir of the same profile dir).
/// The example must be built with the SAME profile as the test run — a
/// profile mismatch looks in the wrong dir, or worse, finds a stale binary.
fn serve_bin() -> PathBuf {
    let name = format!("serve{}", env::consts::EXE_SUFFIX);
    let exe = env::current_exe().expect("the test binary's own path");
    let profile_dir = exe
        .parent() // .../deps
        .and_then(Path::parent) // .../<profile>
        .expect("the test binary lives in <target>/<profile>/deps");
    let bin = profile_dir.join("examples").join(name);
    assert!(
        bin.is_file(),
        "the serve example binary is missing at {} — `cargo test --test serve_read_only` alone \
         does not build examples, so build it first with the same profile as the tests \
         (`cargo build -p tinio-server --example serve`; CI adds `--profile ci`)",
        bin.display()
    );
    bin
}

/// How long a spawned child gets to reach readiness, or to exit — the knob
/// and default [`tinio_util::testing::wait_for`] honours, read through its
/// own accessor: one policy for a starved runner, not a second default here.
fn wait_timeout() -> Duration {
    Duration::from_secs(wait_timeout_secs())
}

/// The lines a child has written to one pipe so far, shared with that
/// pipe's draining thread (the only writer).
type Lines = Arc<Mutex<Vec<String>>>;

/// Drain `pipe` line by line into `lines` until EOF (the child exits). A
/// pipe nobody reads fills up and blocks the child, so both pipes are
/// always drained, whether or not the leg looks at them.
fn drain(pipe: impl Read + Send + 'static, lines: Lines) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(pipe);
        let mut raw = Vec::new();
        while reader.read_until(b'\n', &mut raw).is_ok_and(|n| n > 0) {
            // Lossy: a diagnostic that is not UTF-8 drops bytes from the
            // record, never the rest of the pipe.
            let line = String::from_utf8_lossy(&raw);
            lines
                .lock()
                .expect("the pipe buffer")
                .push(line.trim_end().to_string());
            raw.clear();
        }
    })
}

/// A spawned `serve` child with both pipes drained. Killed and reaped when
/// it drops (see the module doc for the ordering rationale).
struct Serve {
    child: Child,
    stdout: Lines,
    stderr: Lines,
    /// The draining threads, taken by `stop`.
    drains: Vec<JoinHandle<()>>,
}

impl Serve {
    /// Spawn the example serving `root` (always the first argument) with
    /// `args` after it.
    ///
    /// The child inherits this process's environment, so the three
    /// variables that would silently change what it serves are removed
    /// first — `TINIO_READ_ONLY` (which would put every spawned server in
    /// read-only mode), `TINIO_STATE_DIR` (which would relocate its state)
    /// and `TINIO_SCANNER` (which would change what it scans) — and only
    /// then is the caller's `env` applied. The rationale is `clients.rs`'s
    /// `spawn_child`; each leg here sets exactly what it means to set.
    fn spawn(root: &Path, args: &[&str], env: &[(&str, &str)]) -> Self {
        let bin = serve_bin();
        let mut cmd = Command::new(&bin);
        cmd.arg(root)
            .args(args)
            .env_remove(READ_ONLY_VAR)
            .env_remove(STATE_DIR_VAR)
            .env_remove("TINIO_SCANNER");
        for (name, value) in env {
            cmd.env(name, value);
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("cannot spawn {}: {e}", bin.display()));
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let drains = vec![
            drain(
                child.stdout.take().expect("stdout is piped"),
                Arc::clone(&stdout),
            ),
            drain(
                child.stderr.take().expect("stderr is piped"),
                Arc::clone(&stderr),
            ),
        ];
        Self {
            child,
            stdout,
            stderr,
            drains,
        }
    }

    /// Wait for the `listening on <addr>` readiness line and return the
    /// address the child actually bound (`--port 0` makes it ephemeral).
    ///
    /// Bounded: a child that never gets there fails this wait with its
    /// captured stderr instead of hanging the suite, and one that dies
    /// first fails immediately rather than burning the whole timeout.
    async fn ready(&mut self) -> SocketAddr {
        let timeout = wait_timeout();
        let deadline = Instant::now() + timeout;
        loop {
            for line in self.stdout.lock().expect("the stdout buffer").iter() {
                if let Some(raw) = line.strip_prefix(READY) {
                    return raw.parse().unwrap_or_else(|e| {
                        panic!("cannot parse the readiness line `{line}`: {e}")
                    });
                }
            }
            if let Some(status) = self.child.try_wait().expect("the child's status") {
                // Stop first: the message is only as good as the pipes it
                // quotes, and a live child's are not at EOF yet.
                self.stop();
                panic!(
                    "the server exited ({status}) before printing `{READY}`; stderr:\n{}",
                    self.stderr_text()
                );
            }
            if Instant::now() >= deadline {
                self.stop();
                panic!(
                    "the server printed no `{READY}` line within {} s; stderr:\n{}",
                    timeout.as_secs(),
                    self.stderr_text()
                );
            }
            sleep(Duration::from_millis(2)).await;
        }
    }

    /// Wait for the child to exit on its own — the legs that expect a
    /// failure, never a server — and return its status. Bounded the same
    /// way as [`Serve::ready`].
    async fn exited(&mut self) -> ExitStatus {
        let timeout = wait_timeout();
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("the child's status") {
                // Complete the pipes before the caller reads them: the child
                // is gone, so its ends are closed and the drains cannot block.
                self.join_drains();
                return status;
            }
            if Instant::now() >= deadline {
                self.stop();
                panic!(
                    "the server did not exit within {} s (this leg expects a failure); stderr:\n{}",
                    timeout.as_secs(),
                    self.stderr_text()
                );
            }
            sleep(Duration::from_millis(2)).await;
        }
    }

    /// Kill and reap the child now, then join the draining threads (their
    /// pipes reach EOF at the child's death). `Drop` runs the same path, so
    /// an assertion failure later still reaps — an explicit call just gets
    /// the complete pipes before the leg's final assertions.
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.join_drains();
    }

    /// Join the draining threads, taking the handles so a second `stop`
    /// cannot join them twice.
    fn join_drains(&mut self) {
        for handle in self.drains.drain(..) {
            let _ = handle.join();
        }
    }

    /// Everything the child has written to stdout so far.
    fn stdout_lines(&self) -> Vec<String> {
        self.stdout.lock().expect("the stdout buffer").clone()
    }

    /// Everything the child has written to stderr so far, line-joined.
    fn stderr_text(&self) -> String {
        self.stderr.lock().expect("the stderr buffer").join("\n")
    }

    /// The child's stderr once it carries `needle`, waiting (bounded) while
    /// it does not. A separate thread drains the pipe, so a line the child
    /// wrote is not necessarily recorded the instant this thread looks — a
    /// live child's stderr has no EOF to synchronize on.
    async fn stderr_containing(&self, needle: &str) -> String {
        let deadline = Instant::now() + wait_timeout();
        loop {
            let text = self.stderr_text();
            if text.contains(needle) || Instant::now() >= deadline {
                return text;
            }
            sleep(Duration::from_millis(2)).await;
        }
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        // See [`Serve::stop`] and the module doc.
        self.stop();
    }
}

/// RFC 2104 HMAC-SHA256 (SigV4's derivation needs it; the test signer skips
/// dedicating the `hmac` crate, like `data.rs`'s acl test).
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut key_vec = key.to_vec();
    if key_vec.len() > 64 {
        key_vec = Sha256::digest(&key_vec).to_vec();
    }
    key_vec.resize(64, 0);
    let mut inner = Vec::with_capacity(64);
    let mut outer = Vec::with_capacity(64);
    for byte in &key_vec {
        inner.push(byte ^ 0x36);
        outer.push(byte ^ 0x5c);
    }
    let inner_hash = Sha256::digest([inner.as_slice(), data].concat());
    Sha256::digest([outer.as_slice(), inner_hash.as_slice()].concat()).into()
}

/// An `AWS4-HMAC-SHA256` Authorization header over the signed headers
/// `host;x-amz-content-sha256;x-amz-date` — the canonicalization s3s
/// verifies in `ops/signature.rs`, mirrored from `data.rs`'s acl test
/// (`x-amz-content-sha256` is S3-specific and covers the payload).
#[allow(clippy::too_many_arguments)]
fn authorization(
    access_key: &str,
    secret: &str,
    method: &str,
    path: &str,
    host: &str,
    date8: &str,
    amz_date: &str,
    payload_hash: &str,
) -> String {
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "{method}\n{path}\n\nhost:{host}\n\
         x-amz-content-sha256:{payload_hash}\n\
         x-amz-date:{amz_date}\n\n\
         {signed_headers}\n\
         {payload_hash}"
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date8}/us-east-1/s3/aws4_request\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let mut key = b"AWS4".to_vec();
    key.extend_from_slice(secret.as_bytes());
    for part in [date8.as_bytes(), b"us-east-1", b"s3", b"aws4_request"] {
        key = hmac_sha256(&key, part).to_vec();
    }
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{date8}/us-east-1/s3/aws4_request, \
         SignedHeaders={signed_headers}, Signature={signature}"
    )
}

/// One signed request with the example's fixed credential pair — no
/// unsigned request reaches the layer under test, see the module doc.
async fn signed(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> Wire {
    let now = OffsetDateTime::now_utc();
    let date8 = format!(
        "{:04}{:02}{:02}",
        now.year(),
        u8::from(now.month()),
        now.day()
    );
    let amz_date = format!(
        "{date8}T{:02}{:02}{:02}Z",
        now.hour(),
        now.minute(),
        now.second()
    );
    let payload_hash = hex::encode(Sha256::digest(body));
    let auth = authorization(
        DEFAULT_ROOT_ACCESS_KEY,
        SECRET_KEY,
        method,
        path,
        HOST,
        &date8,
        &amz_date,
        &payload_hash,
    );
    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {HOST}\r\n\
         x-amz-date: {amz_date}\r\n\
         x-amz-content-sha256: {payload_hash}\r\n\
         Authorization: {auth}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    send(addr, &head, body).await
}

/// The mode the plane serves in, observed from outside the process.
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    ReadWrite,
    ReadOnly,
}

/// Observe the served plane's mode through the discriminator the module doc
/// describes: a signed `PUT` to a bucket that does not exist. Both answers
/// are pinned, so a third answer fails loudly instead of reading as either
/// mode.
async fn observed_mode(addr: SocketAddr) -> Mode {
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
struct Scratch {
    dir: TempDir,
}

impl Scratch {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("a scratch dir"),
        }
    }

    /// The served root, created: a read-write server would create it itself,
    /// and a read-only one requires it to exist already.
    fn root(&self) -> PathBuf {
        let root = self.dir.path().join("root");
        fs::create_dir_all(&root).expect("the served root");
        root
    }

    /// The relocated state dir the read-only legs pass as
    /// `TINIO_STATE_DIR` — never the real home, which a read-only leg would
    /// otherwise derive (`<home>/.tinio/roots/<hash>/`) and write into.
    fn state(&self) -> PathBuf {
        self.dir.path().join("state")
    }

    /// A root path that does not exist, and must stay that way.
    fn missing_root(&self) -> PathBuf {
        self.dir.path().join("missing")
    }

    /// Write a `[server] read_only = <on>` config file and return its path.
    fn config_file(&self, read_only: bool) -> PathBuf {
        let path = self.dir.path().join("config.toml");
        fs::write(
            &path,
            format!("version = 1\n\n[server]\nread_only = {read_only}\n"),
        )
        .expect("the config file");
        path
    }
}

/// `TINIO_STATE_DIR` for a leg, as the `&str` the child's environment takes.
fn state_var(state: &Path) -> (&'static str, &str) {
    (STATE_DIR_VAR, state.to_str().expect("a UTF-8 scratch path"))
}

/// Whether `haystack` names `path` in either form a failure can print it:
/// `Path::display` writes it plain, while a derived `Debug` (which is how
/// `main`'s returned error reaches stderr) escapes the separators on
/// Windows.
fn names_path(haystack: &str, path: &Path) -> bool {
    let plain = path.display().to_string();
    haystack.contains(&plain) || haystack.contains(&plain.replace('\\', "\\\\"))
}

/// Case 1: `--read-only` with `TINIO_STATE_DIR` serves read-only, keeps the
/// state out of the served root, and writes exactly one line to stdout.
#[tokio::test]
async fn the_read_only_flag_relocates_the_state_and_refuses_writes() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();
    plant(&root, "data", "a.txt", "hello");

    let mut server = Serve::spawn(&root, &["--port", "0", "--read-only"], &[state_var(&state)]);
    let addr = server.ready().await;

    // The flag reached the plane: the write path is refused where the
    // read-write plane answers NoSuchBucket from the bucket check itself.
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "`--read-only` must refuse the signed write with AccessDenied"
    );
    // Reads still serve, with the planted body: a refusal decorator that
    // broke reads would still pass the discriminator above.
    let object = signed(addr, "GET", "/data/a.txt", b"").await;
    assert_eq!(
        object.status,
        200,
        "the planted object must serve on a read-only plane: {}",
        object.text()
    );
    assert_eq!(
        object.text(),
        "hello",
        "the planted object's body must arrive intact"
    );
    // The state really moved out of the root, into the dir the diagnostic
    // names on stderr (stdout is the harness's readiness line alone).
    let diagnostic = format!("read-only mode: state dir {}", state.display());
    let stderr = server.stderr_containing(&diagnostic).await;
    assert!(
        stderr.contains(&diagnostic),
        "stderr must carry `{diagnostic}`; got:\n{stderr}"
    );
    assert!(
        state.join("meta.redb").is_file(),
        "the state database must land in {} (TINIO_STATE_DIR)",
        state.display()
    );
    assert!(
        !root.join(".tinio").exists(),
        "the read-only server wrote a `.tinio` into the served root it promises to leave alone"
    );

    server.stop();
    // Complete now that the child is gone: stdout carried the readiness
    // line and nothing else.
    let stdout = server.stdout_lines();
    assert_eq!(
        stdout.len(),
        1,
        "stdout must carry the readiness line alone; got {stdout:?}"
    );
    assert!(
        stdout[0].starts_with(READY),
        "the single stdout line must be the readiness line; got {:?}",
        stdout[0]
    );
}

/// Case 2: a config file's `[server] read_only = true` puts the plane in
/// read-only mode with no flag and no environment variable.
#[tokio::test]
async fn a_config_file_turns_read_only_on() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();
    let config = scratch.config_file(true);

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--config", config.to_str().unwrap()],
        &[state_var(&state)],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "`[server] read_only = true` must put the plane in read-only mode without a flag or \
         TINIO_READ_ONLY"
    );
    server.stop();
}

/// Case 3: `TINIO_READ_ONLY=1` puts the plane in read-only mode with no
/// flag and no config file.
#[tokio::test]
async fn the_read_only_env_var_turns_read_only_on() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();

    let mut server = Serve::spawn(
        &root,
        &["--port", "0"],
        &[state_var(&state), (READ_ONLY_VAR, "1")],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "TINIO_READ_ONLY=1 must put the plane in read-only mode without a flag or a config file"
    );
    server.stop();
}

/// Case 4a: the environment beats the config file — `read_only = true` in
/// the file with `TINIO_READ_ONLY=0` serves READ-WRITE.
#[tokio::test]
async fn the_env_var_beats_the_config_file() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();
    let config = scratch.config_file(true);

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--config", config.to_str().unwrap()],
        &[state_var(&state), (READ_ONLY_VAR, "0")],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadWrite,
        "TINIO_READ_ONLY=0 must override the config file's `read_only = true`"
    );
    // The stronger half of "read-write": a write really lands. The
    // discriminator's 404 could come from a plane that refuses writes some
    // other way; the create and upload below are the plain proof it does not.
    let created = signed(addr, "PUT", "/fresh", b"").await;
    assert_eq!(
        created.status,
        200,
        "CreateBucket must succeed on the read-write plane: {}",
        created.text()
    );
    let uploaded = signed(addr, "PUT", "/fresh/a.txt", b"body").await;
    assert_eq!(
        uploaded.status,
        200,
        "a signed upload must succeed on the read-write plane: {}",
        uploaded.text()
    );
    assert_eq!(
        fs::read_to_string(root.join("fresh").join("a.txt")).expect("the uploaded object"),
        "body",
        "the uploaded object must land in the served root"
    );
    // And the state stayed in the root: read-write mode never consults
    // TINIO_STATE_DIR (the read-only escape hatch), so the exported value is
    // ignored rather than obeyed.
    assert!(
        root.join(".tinio").is_dir(),
        "read-write mode must keep its state in <root>/.tinio, not in TINIO_STATE_DIR"
    );
    server.stop();
}

/// Case 4b: the flag beats the environment — `TINIO_READ_ONLY=0` with
/// `--read-only` serves READ-ONLY.
#[tokio::test]
async fn the_read_only_flag_beats_the_env_var() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--read-only"],
        &[state_var(&state), (READ_ONLY_VAR, "0")],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "`--read-only` must win over TINIO_READ_ONLY=0"
    );
    server.stop();
}

/// Case 5: `--read-only` never creates the root it was told to serve — a
/// missing root fails the process instead. Both routes the example
/// documents are driven: with no `TINIO_STATE_DIR` the derived
/// `<home>/.tinio/roots/<hash>/` resolution fails (it canonicalizes the
/// root before it touches the home, which is why that route cannot leave
/// anything in the real home either), and with `TINIO_STATE_DIR` set the
/// backend's own canonicalization fails.
#[tokio::test]
async fn a_missing_root_is_not_created_in_read_only_mode() {
    let scratch = Scratch::new();
    let missing = scratch.missing_root();
    let state = scratch.state();

    // Route 1: the derived state dir. Its failure names the root it could
    // not canonicalize.
    let stderr = missing_root_fails("the derived state dir", &missing, &[]).await;
    assert!(
        names_path(&stderr, &missing),
        "the derived-layout failure must name the missing root {}; stderr:\n{stderr}",
        missing.display()
    );

    // Route 2: an explicit `TINIO_STATE_DIR`. The resolution succeeds — its
    // diagnostic names the scratch dir — and the backend's canonicalization
    // of the root fails instead, as a bare `Io` error that carries no path,
    // so the diagnostic is the evidence that this route got that far.
    let stderr = missing_root_fails("TINIO_STATE_DIR", &missing, &[state_var(&state)]).await;
    let diagnostic = format!("read-only mode: state dir {}", state.display());
    assert!(
        stderr.contains(&diagnostic),
        "the explicit state dir is resolved before the root is opened, so stderr must carry \
         `{diagnostic}`; got:\n{stderr}"
    );
}

/// Spawn `--read-only` over a root that does not exist and assert what every
/// route shares — a non-zero exit, and the root still absent afterwards —
/// returning the child's stderr for the route's own evidence.
async fn missing_root_fails(label: &str, missing: &Path, env: &[(&str, &str)]) -> String {
    let mut server = Serve::spawn(missing, &["--read-only"], env);
    let status = server.exited().await;
    let stderr = server.stderr_text();
    assert!(
        !status.success(),
        "{label}: a missing root must fail startup, not be served ({status}); stderr:\n{stderr}"
    );
    assert!(
        !missing.exists(),
        "{label}: read-only mode created the root it was told to serve"
    );
    stderr
}

/// Case 6: an unknown argument exits with status 2, the `usage()` path.
#[tokio::test]
async fn an_unknown_argument_exits_with_the_usage_status() {
    let scratch = Scratch::new();
    let root = scratch.root();

    let mut server = Serve::spawn(&root, &["--not-a-flag"], &[]);
    let status = server.exited().await;
    let stderr = server.stderr_text();
    assert_eq!(
        status.code(),
        Some(2),
        "an unknown argument must exit 2 (usage); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("usage: serve"),
        "the usage text must name the binary; stderr:\n{stderr}"
    );
}
