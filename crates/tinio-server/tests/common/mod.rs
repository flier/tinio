//! The raw-HTTP wire helpers the read-only integration targets share, and the
//! spawn harness the targets that run the `tinio` binary share.
//!
//! `read_only.rs` drives the plane in process; the spawned-`tinio server`
//! half of the read-only family is `crates/tinio-e2e`'s `@cli` scenarios
//! since 2026-10-01 (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`,
//! which retired `serve_read_only.rs`). What stays here speaks raw HTTP/1.1 to
//! a loopback port and reads S3's XML answers. The response parser, the de-chunker, the
//! connect-with-retry client and the on-disk fixture writer therefore live
//! here once, instead of in two copies nothing forces together — a fix to a
//! chunked-body edge case used to land in one and silently not the other.
//!
//! The targets that spawn the binary — `sandbox_e2e.rs`,
//! `sandbox_e2e_windows.rs` (through `support/windows_e2e_harness.rs`) and
//! `sandbox_macos_e2e.rs` — share the spawn helpers for
//! the same reason: each carried its own copy of how the binary is resolved
//! ([`tinio_bin`]), of how long a spawned child gets to report itself
//! ([`wait_timeout`]) and of the framing rule a child's event stream is decoded
//! with ([`frame_line`] — what [`wait_for_listening_in`] and
//! `sandbox_macos_e2e.rs`'s own walk read their buffered lines with, and
//! what [`stdout_lines`] pumps a pipe into for the sandbox legs). **A fix to the
//! resolution landing in one copy and silently not the others** is the failure
//! those names exist to remove.
//!
//! The sandbox targets — `sandbox_e2e.rs`, `sandbox_e2e_windows.rs` and
//! `sandbox_macos_e2e.rs` — also carried the readiness handshake three times
//! ([`wait_for_listening`] for the first two, and the macOS leg's own raw-line
//! copy), which is the same read under the same framing rule in all three. What
//! genuinely differs between them is the failure: a launcher's log survives the
//! kill that a child's stderr pipe does not, so each leg keeps its own `fail` and
//! this hands it the message. The macOS leg is also why the read is split —
//! [`wait_for_listening_in`] takes the raw lines so that a caller can keep them,
//! and [`wait_for_listening`] is the child-taking wrapper for the other two.
//!
//! Two more names belong to those targets and are here for the same reason:
//! [`answer`], with the [`PROBE`] bytes it sends, is the "the listener answers"
//! exchange the Linux leg and the Windows harness had each spelled out, and
//! [`sweep_confined_grandchild`] is the macOS sweep for the confined grandchild a
//! killed launcher leaves behind (design decision 20's accepted gap), which a
//! failing leg needs before it joins the stderr drain the grandchild holds open.
//!
//! Each target declares `mod common;` and takes what it uses, so none of them
//! uses everything: hence the module-wide `dead_code` allow. Nothing here
//! asserts about the product — these are the helpers and the legs own the
//! expectations; the one guard is [`tinio_bin`]'s precondition on the
//! environment (the CLI is built), which is the check the copies had
//! drifted on.

#![allow(dead_code)]
// **A test-support module inside a test target, and a stated exemption because
// clippy's test keys do not reach it** (docs/style.md §Panics, design 2026-10-09
// D7): `allow-*-in-tests` covers `#[test]` functions and `#[cfg(test)]` modules,
// while this fixture's sites are plain helper `fn`s (the wire helpers, the spawn
// harness) that no `#[test]` encloses.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

use std::{
    env, fs,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream as StdTcpStream},
    path::{Path, PathBuf},
    process::Child,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tinio_core::{
    BucketOps,
    acl::{Acl, DEFAULT_ROOT_ACCESS_KEY},
    bucket,
};
use tinio_mem::MemoryStorage;
use tinio_sandbox::Event;
use tinio_server::{Capabilities, DataPlane};
use tinio_util::testing::{frame_line, wait_timeout_secs};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::sleep,
};

/// The fixed credential pair the `tinio` binary always accepts: the access key is the
/// core convention constant, the secret the literal `identity::root_auth_pair`
/// falls back to with no `[auth]` section.
pub const SECRET_KEY: &str = "minioadmin";

/// The `Host` every signed request carries. The server binds `127.0.0.1`, and
/// s3s verifies the signature over the `Host` header the client sent — never
/// against the bound address — so the value is the signer's to choose
/// (`data.rs`'s acl test picks the same one).
pub const HOST: &str = "localhost";

/// An in-process data plane over a fresh mem backend, bound to an ephemeral loopback port.
///
/// The shape the span/OTel legs share: mem storage with one `data` bucket, a bound listener, the
/// plane spawned on this runtime, and a [`Plane`] handle that owns the stop channel and the join.
pub async fn spawn_plane() -> Plane {
    let plane = DataPlane::new(mem_storage().await, Capabilities::default());
    spawn_plane_with(plane).await
}

/// [`spawn_plane`] with the static credential pair the auth leg drives an unknown key through.
pub async fn spawn_plane_with_auth(access_key: &'static str, secret: &'static str) -> Plane {
    let plane = DataPlane::new_with_auth(
        mem_storage().await,
        Capabilities::default(),
        access_key,
        secret,
    );
    spawn_plane_with(plane).await
}

/// A mem backend with the `data` bucket every fixture request addresses.
async fn mem_storage() -> MemoryStorage {
    let storage = MemoryStorage::new().expect("mem storage");
    let name = bucket::name("data").expect("bucket name");
    storage
        .create_bucket(&name, None, &Acl::default_private(None))
        .await
        .expect("bucket");
    storage
}

/// The running plane: its address, the shutdown sender and the serve task's join handle.
pub struct Plane {
    /// The loopback address the listener bound.
    pub addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl Plane {
    /// Flip the shutdown channel and wait for the accept loop to end.
    pub async fn stop(self) {
        let _ = self.shutdown.send(true);
        self.handle.await.expect("the accept loop ends on the stop");
    }
}

/// Bind and spawn the plane, turning a serve failure into the same panic the legs asserted.
async fn spawn_plane_with(plane: DataPlane) -> Plane {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown, rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        plane.serve(listener, rx).await.expect("the plane serves");
    });
    Plane {
        addr,
        shutdown,
        handle,
    }
}

/// A raw HTTP response: the status and the de-chunked body.
pub struct Wire {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Wire {
    /// Parse `status line CRLF headers CRLF CRLF body` off the wire.
    pub fn parse(raw: &[u8]) -> Self {
        let split = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("the response header terminator");
        let head = String::from_utf8_lossy(&raw[..split]).into_owned();
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .expect("the status line");
        let chunked = lines.any(|line| {
            let line = line.to_ascii_lowercase();
            line.starts_with("transfer-encoding:") && line.contains("chunked")
        });
        let body = raw[split + 4..].to_vec();
        Self {
            status,
            body: if chunked { dechunk(&body) } else { body },
        }
    }

    /// The body as text (lossy: an error or XML body is UTF-8).
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The `<Code>` element of an S3 error body.
    pub fn code(&self) -> String {
        extract(&self.text(), "<Code>", "</Code>")
    }
}

/// Reassemble a chunked transfer-coded body (chunk extensions and
/// trailers ignored).
pub fn dechunk(mut rest: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(pos) = rest.windows(2).position(|w| w == b"\r\n") {
        let line = String::from_utf8_lossy(&rest[..pos]).into_owned();
        let size = line
            .split(';')
            .next()
            .and_then(|hex| usize::from_str_radix(hex.trim(), 16).ok())
            .unwrap_or(0);
        rest = rest.get(pos + 2..).unwrap_or_default();
        if size == 0 || rest.len() < size {
            break;
        }
        out.extend_from_slice(&rest[..size]);
        rest = rest.get(size + 2..).unwrap_or_default();
    }
    out
}

/// The text between the first `open`/`close` pair — empty when either is
/// absent, so a missing tag never yields a garbage slice. The rule is
/// [`tinio_util::testing::extract`]'s, re-exported rather than copied.
pub use tinio_util::testing::extract;

/// Whether a connect failure is the Windows loopback accept race (a
/// connection aborted in the accept queue) — the e2e harness retries these
/// too.
pub fn retryable(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionRefused
    )
}

/// One HTTP/1.1 request on a fresh connection (`Connection: close`, the
/// response read to EOF).
///
/// The request head failing before any byte is sent is retried — nothing was
/// transmitted, so a retry cannot duplicate the request. From the body on, the
/// request is sent once.
pub async fn send(addr: SocketAddr, head: &str, body: &[u8]) -> Wire {
    let mut attempt = 0;
    loop {
        let mut stream = match TcpStream::connect(addr).await {
            Ok(stream) => stream,
            Err(err) if retryable(&err) && attempt < 2 => {
                attempt += 1;
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => panic!("cannot connect to {addr}: {err}"),
        };
        match stream.write(head.as_bytes()).await {
            Ok(sent) if sent == head.len() => {}
            Ok(sent) => stream
                .write_all(&head.as_bytes()[sent..])
                .await
                .expect("the rest of the request head"),
            Err(err) if retryable(&err) && attempt < 2 => {
                attempt += 1;
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => panic!("cannot send the request head to {addr}: {err}"),
        }
        stream.write_all(body).await.expect("the request body");
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .expect("the response body");
        return Wire::parse(&raw);
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
/// `host;x-amz-content-sha256;x-amz-date` — the canonicalization s3s verifies
/// in `ops/signature.rs`, mirrored from `data.rs`'s acl test
/// (`x-amz-content-sha256` is S3-specific and covers the payload).
#[expect(
    clippy::too_many_arguments,
    reason = "the SigV4 canonical request's inputs — access key, secret, method, path, host, the two dates and the payload hash — taken positionally because the helper mirrors `data.rs`'s acl test signing"
)]
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

/// One signed request with the server's fixed credential pair — an unsigned
/// request is refused with `Signature is required` **before it reaches the
/// layer under test**, so every leg whose subject is storage behaviour signs.
///
/// **It lives here because one harness needs it, and that harness is kept compiled.** It was
/// written in the retired `serve_read_only.rs`, where the module doc explained why the plane
/// cannot be driven unsigned; its one caller now is `support/serve_spawn.rs`'s
/// `observed_mode`, which `sandbox_e2e.rs` includes without calling — the read-only and
/// sandbox-channel legs that needed the header are `crates/tinio-e2e`'s `@cli` scenarios
/// since 2026-10-01, and a second copy is the drift this module exists to prevent.
///
/// **No query string, and that is a limit rather than a rule**: the canonical
/// request below signs an empty query, so a caller that needs `?list-type=2`
/// extends this function rather than signing around it.
pub async fn signed(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> Wire {
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

/// Plant a bucket directory and one object file directly on disk — the only
/// way a read-only plane can have anything to read: every mutating request,
/// bucket create and object upload included, is refused.
pub fn plant(root: &Path, bucket: &str, key: &str, body: &str) {
    let path = root.join(bucket).join(key);
    fs::create_dir_all(path.parent().expect("the key has a parent")).expect("the bucket dir");
    fs::write(&path, body).expect("the planted object");
}

/// The `tinio` binary — the serving process every spawning target in this crate
/// drives — resolved from the test binary's own location: `<target>/<profile>/deps/<test>`
/// means the binary sits in the parent `<profile>` dir.
///
/// **This is the repository's one resolution of it, and the helper it replaced is
/// not a second.** `assert_cmd::cargo::cargo_bin` reads `CARGO_BIN_EXE_tinio` first —
/// cargo defines that for the **binary targets of the crate under test**, and `tinio`
/// is `tinio-cli`'s, reached through the facade crate, so nothing this package builds
/// exports it — then falls back to its `legacy_cargo_bin` (assert_cmd 2.2.2,
/// `src/cargo.rs`): `current_exe`, drop the `deps` component, join `tinio<EXE_SUFFIX>`,
/// which is this function's directory. The fallback is not used for two reasons: it
/// guards with `exists()`, so a **directory** named `tinio.exe` passes for a binary,
/// and with nothing there it panics `` `CARGO_BIN_EXE_tinio` is unset ``, naming a
/// variable this package never had instead of the build that is missing. The e2e steps
/// under `crates/tinio-e2e/tests/**` — a different crate's test tree, which cannot
/// import this module — still take that fallback; every target here goes through here.
///
/// The binary must also be built with the **same profile** as the test run: a
/// profile mismatch looks in the wrong dir, or worse, finds a stale binary.
///
/// The checked property is `is_file`, the strict of the two guards the copies
/// this replaced had drifted to — a directory named `tinio.exe` is a missing
/// binary, and saying so here is cheaper than a spawn that fails obscurely — and
/// the failure names the command that builds it rather than assert_cmd's
/// unset-variable text.
pub fn tinio_bin() -> PathBuf {
    let name = format!("tinio{}", env::consts::EXE_SUFFIX);
    let exe = env::current_exe().expect("the test binary's own path");
    let profile_dir = exe
        .parent() // .../deps
        .and_then(Path::parent) // .../<profile>
        .expect("the test binary lives in <target>/<profile>/deps");
    let bin = profile_dir.join(name);
    assert!(
        bin.is_file(),
        "the `tinio` binary is missing at {} — a filtered `cargo test --test …` builds test \
         targets only, so build it first, with the same profile as the tests \
         (`cargo build -p tinio`; CI adds `--profile ci`)",
        bin.display()
    );
    bin
}

/// How long a spawned child gets to reach readiness, or to exit — the knob and
/// default [`tinio_util::testing::wait_for`] honours, read through its own
/// accessor: one policy for a starved runner, not a second default here.
pub fn wait_timeout() -> Duration {
    Duration::from_secs(wait_timeout_secs())
}

/// Read a spawned child's event stream until `listening` arrives, and hand back
/// the endpoint it bound — or the message the caller's own `fail` renders.
///
/// **`Ok`/`Err` rather than a `fail` of its own, because the suites that call
/// this fail differently and both differences are real**: the Windows leg kills a
/// launcher whose log survives the kill and quotes that file, while the Linux and
/// macOS legs kill the child and read the stderr pipe behind it. What the three
/// do *not* differ in is this handshake — [`wait_for_listening_in`] is the read,
/// and this is its child-taking wrapper for the legs that do not keep the raw
/// lines.
///
/// **The read happens on a thread of its own and the wait is bounded here.** The
/// earlier shape read lines straight off the pipe with no deadline, so a startup
/// path that neither writes a frame nor exits — a `confine` that blocks, a config
/// read on a FIFO — hung the whole workspace leg until CI times it out, with no
/// test named: libtest has no per-test timeout. The thread is [`stdout_lines`]'s
/// and it keeps the pipe drained, so the child cannot block on a full buffer. The
/// budget is the crate's own readiness knob, read through its accessor
/// ([`wait_timeout`]), because a starved runner is one policy and not a second
/// default here.
///
/// **And it accumulates a frame rather than reading two lines.** A stray line
/// ahead of the frame is exactly the case the framing exists to survive —
/// `sse-core` ignores field lines it does not know — while a fixed two-line read
/// reported it as an unexpected frame. The rule itself is [`frame_line`]'s, which
/// is where every suite that reads this stream shares it. A refusal is reported
/// as the refusal it is: an `error` frame carries the confinement's own
/// rendering, which is worth more than any guess this side could make about why
/// no endpoint came, and the caller is told not to skip on it — a server that
/// cannot start is the subject rather than a precondition.
pub fn wait_for_listening(child: &mut Child) -> Result<SocketAddr, String> {
    let Some(stdout) = child.stdout.take() else {
        // Not a panic, because the caller's `fail` is what kills and reaps the
        // child: a helper that panicked here would leave the process it was
        // talking about running.
        return Err("the child's stdout was not piped, so no event stream can be read".to_string());
    };
    wait_for_listening_in(&stdout_lines(stdout))
}

/// The readiness read itself, over [`stdout_lines`]' raw stream: the lines are
/// walked until one closes a `listening` frame, and the endpoint that frame
/// carries comes back.
///
/// **A receiver rather than a child, because one caller keeps the lines.** The
/// macOS leg draws two facts out of the same stream: this handshake, and the
/// assertion that nothing outside a frame reached the consumer (design §12) —
/// which a pump that decoded in its reader thread could not make, since it drops
/// a stray line by construction. So the raw lines are the caller's, and both
/// readers go through [`frame_line`]'s one rule.
///
/// A refusal is the `Err` [`Event::Error`] renders, and everything else that ends
/// the read — the budget, or a pipe that closed without a frame — is the `Err`
/// the caller's `fail` reports. Neither is skipped: a server that cannot start is
/// the subject.
pub fn wait_for_listening_in(lines: &mpsc::Receiver<String>) -> Result<SocketAddr, String> {
    let timeout = wait_timeout();
    let deadline = Instant::now() + timeout;
    let mut frame = String::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match lines.recv_timeout(left) {
            Ok(line) => {
                if let Some(event) = frame_line(&mut frame, &line, Event::decode) {
                    return match event {
                        Event::Listening { addr } => Ok(addr),
                        Event::Error { message } => {
                            Err(format!("serve refused to start: {message}"))
                        }
                    };
                }
            }
            // The budget, or a pipe that closed with no frame in it: both are
            // "this side got no endpoint", and the error names which.
            Err(err) => {
                return Err(format!(
                    "serve reported no `listening` event within {} s ({err})",
                    timeout.as_secs()
                ));
            }
        }
    }
}

/// A spawned child's event stream, **one raw line at a time**, off `pipe` on a
/// thread of its own; the caller's wait on the returned channel is what stays
/// bounded.
///
/// The thread is what keeps the pipe drained: a child with more to say than the
/// pipe holds would otherwise block on it, which reads as a hung server rather
/// than as a full pipe. The budget is the caller's, and it must have one — a
/// confined child that neither reports nor exits would otherwise hang the leg
/// with no test named, libtest having no per-test timeout ([`wait_timeout`] is
/// what every caller here uses).
///
/// **Raw lines rather than decoded events.** [`wait_for_listening`] is the
/// decoded reader for the legs whose subject is the endpoint; macOS keeps this
/// stream and walks it after the child has exited, asserting that every line
/// that reached the consumer was part of a frame — which is the half a pump that
/// decoded in this thread could not report, since it drops a stray line by
/// construction.
pub fn stdout_lines(pipe: impl Read + Send + 'static) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(pipe);
        loop {
            // A fresh buffer per line: `mem::take` would be the other way to
            // recycle one, and its path is one the crates' `absolute_paths`
            // lint refuses inline.
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            // A send that fails means the caller gave up and killed the child;
            // there is nowhere left to report it.
            if tx.send(line).is_err() {
                return;
            }
        }
    });
    rx
}

/// The one plain probe request a leg sends to prove a listener answers: the
/// smallest well-formed HTTP/1.1 exchange, closed by `Connection: close` so the
/// read ends at EOF.
///
/// **The bytes and the read are one helper because three legs had written them
/// out.** The Windows harness's `answer`, the Linux e2e leg's inline exchange
/// (twice) and the macOS leg's `request` each spelled the same request, the same
/// read and the same accepted answer (`starts_with("HTTP/1.1 ")`), and nothing
/// forced the three together. A fourth spelling survives on purpose, and it is
/// not in a Rust test tree: `crates/tinio-sandbox-macos/scripts/verify-product-run.py`
/// speaks the same probe to a confined server from outside this workspace's test
/// targets, so it keeps its own copy rather than reaching in for one.
pub const PROBE: &str = "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";

/// Connect to `addr`, send [`PROBE`], and return the whole answer — or the
/// message the caller's own failure path renders.
///
/// **`Result` rather than a panic, because the callers fail differently and both
/// differences are real** (the rule [`wait_for_listening`] states): the Windows
/// harness panics, while the legs whose subject is a confined child hand the
/// message to a `fail` that kills the process before it reports. So the
/// expectation stays with the leg and this hands back the text.
///
/// **Both bounds are [`wait_timeout`]'s**, and they are not decoration: a server
/// that accepted the connection and then went quiet would otherwise hang the leg
/// with no test named, libtest having no per-test timeout.
pub fn answer(addr: SocketAddr) -> Result<String, String> {
    let timeout = wait_timeout();
    let mut stream = StdTcpStream::connect_timeout(&addr, timeout)
        .map_err(|err| format!("connect to the server at {addr}: {err}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|err| format!("bound the read of the answer: {err}"))?;
    stream
        .write_all(PROBE.as_bytes())
        .map_err(|err| format!("send the probe request: {err}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| format!("read the server's answer: {err}"))?;
    Ok(response)
}

/// Sweep the confined **grandchild** a killed launcher leaves behind, by the
/// served root its own argv names.
///
/// **macOS has no job object** (design decision 20's accepted gap): a kill that
/// reaches a launcher leaves the process `sandbox-exec` exec'd running, and that
/// grandchild inherits the launcher's stderr — the pipe a failing leg is about to
/// read to EOF — so the leg hangs instead of reporting (measured on macOS 15.8,
/// 2026-09-25). The pattern is the leg's own served root, which is unique to it
/// and is named in both processes' argv.
///
/// **A caller must have confined something before it sweeps.** This is a
/// `pkill -f` over the whole process table, so a leg that spawned no confined
/// server would be matching a pattern it never created; the leg this exists for,
/// the macOS e2e leg's `fail`, is reached only after its own launcher exists.
///
/// **The rule is [`tinio_util::testing::sweep_confined_grandchild`]'s, re-exported
/// rather than copied.** The e2e crate's CLI harness needs the same sweep, and a
/// `tests/` tree is not importable from another one — a crate both dev-depend on
/// is, which is what removes the copy this module used to keep.
// `unused_imports` is not right on this line: every target that declares `mod common;`
// compiles this re-export, and only `sandbox_macos_e2e.rs` calls it — the same
// "none of them uses everything" fact the module-wide `dead_code` allow above records,
// which a lint reading one target at a time cannot see.
#[cfg(target_os = "macos")]
#[allow(
    unused_imports,
    clippy::allow_attributes,
    reason = "test support: every target that declares `mod common;` compiles this re-export and only `sandbox_macos_e2e.rs` calls it, so a lint reading one target at a time cannot see the use — an `expect` here is unfulfilled in the targets that do call it"
)]
pub use tinio_util::testing::sweep_confined_grandchild;
