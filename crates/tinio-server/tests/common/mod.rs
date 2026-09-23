//! The raw-HTTP wire helpers the read-only integration targets share, and the
//! spawn harness the targets that run the `serve` example share.
//!
//! `read_only.rs` drives the plane in process, `serve_read_only.rs` drives the
//! spawned `serve` binary; both speak raw HTTP/1.1 to a loopback port and both
//! read S3's XML answers. The response parser, the de-chunker, the
//! connect-with-retry client and the on-disk fixture writer therefore live
//! here once, instead of in two copies nothing forces together — a fix to a
//! chunked-body edge case used to land in one and silently not the other.
//!
//! The three targets that spawn the example — `serve_read_only.rs`,
//! `sandbox_e2e.rs` and `sandbox_e2e_windows.rs` — share the spawn harness for
//! the same reason: each carried its own copy of how the example is resolved
//! ([`serve_bin`]) and how long a spawned child gets to report itself
//! ([`wait_timeout`]), and each decoded the child's event stream with the same
//! framing rule spelled out a third time ([`frame_line`], which [`events`]
//! pumps a pipe into for the two of them that read one). **A fix to the
//! resolution landing in one copy and silently not the others** is the failure
//! those names exist to remove.
//!
//! The two sandbox targets — `sandbox_e2e.rs` and `sandbox_e2e_windows.rs` —
//! also carried the readiness handshake twice ([`wait_for_listening`]), which is
//! the same read under the same framing rule in both. What genuinely differs
//! between them is the failure: a launcher's log survives the kill that a
//! child's stderr pipe does not, so each leg keeps its own `fail` and this hands
//! it the message.
//!
//! Each target declares `mod common;` and takes what it uses, so none of them
//! uses everything: hence the module-wide `dead_code` allow. Nothing here
//! asserts about the product — these are the helpers and the legs own the
//! expectations; the one guard is [`serve_bin`]'s precondition on the
//! environment (the example is built), which is the check the copies had
//! drifted on.

#![allow(dead_code)]

use std::{
    env, fs,
    io::{self, BufRead, BufReader, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Child,
    sync::mpsc,
    thread,
    time::Duration,
};

use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tinio_core::acl::DEFAULT_ROOT_ACCESS_KEY;
use tinio_sandbox::Event;
use tinio_util::testing::wait_timeout_secs;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::sleep,
};

/// The fixed credential pair the example always accepts: the access key is the
/// core convention constant, the secret the literal `identity::root_auth_pair`
/// falls back to with no `[auth]` section.
pub const SECRET_KEY: &str = "minioadmin";

/// The `Host` every signed request carries. The example binds `127.0.0.1`, and
/// s3s verifies the signature over the `Host` header the client sent — never
/// against the bound address — so the value is the signer's to choose
/// (`data.rs`'s acl test picks the same one).
pub const HOST: &str = "localhost";

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
/// absent, so a missing tag never yields a garbage slice.
pub fn extract(text: &str, open: &str, close: &str) -> String {
    let Some(start) = text.find(open).map(|i| i + open.len()) else {
        return String::new();
    };
    let Some(end) = text[start..].find(close).map(|i| start + i) else {
        return String::new();
    };
    text[start..end].to_string()
}

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

/// One signed request with the example's fixed credential pair — an unsigned
/// request is refused with `Signature is required` **before it reaches the
/// layer under test**, so every leg whose subject is storage behaviour signs.
///
/// **It lives here because two targets now need it.** It was written in
/// `serve_read_only.rs`, where the module doc explains why the plane cannot be
/// driven unsigned; the Windows confinement leg needs the same header for the
/// same reason, and a second copy is the drift this module exists to prevent.
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

/// The `serve` **example** binary, resolved from the test binary's own
/// location: `<target>/<profile>/deps/<test>` means the example sits in the
/// sibling `examples/` dir of the same profile dir.
///
/// **Not `assert_cmd::cargo::cargo_bin`, and not `env!("CARGO_BIN_EXE_serve")`.**
/// Cargo sets `CARGO_BIN_EXE_*` for binary targets only — naming an example in
/// `env!(…)` is a compile error — and `assert_cmd`'s helper resolves that same
/// set, so neither finds an example. The example must also be built with the
/// **same profile** as the test run: a profile mismatch looks in the wrong dir,
/// or worse, finds a stale binary.
///
/// The checked property is `is_file`, the strict of the two guards the copies
/// this replaced had drifted to — a directory named `serve.exe` is a missing
/// example, and saying so here is cheaper than a spawn that fails obscurely.
pub fn serve_bin() -> PathBuf {
    let name = format!("serve{}", env::consts::EXE_SUFFIX);
    let exe = env::current_exe().expect("the test binary's own path");
    let profile_dir = exe
        .parent() // .../deps
        .and_then(Path::parent) // .../<profile>
        .expect("the test binary lives in <target>/<profile>/deps");
    let bin = profile_dir.join("examples").join(name);
    assert!(
        bin.is_file(),
        "the serve example binary is missing at {} — a filtered `cargo test --test …` builds \
         test targets only and never examples, so build it first, with the same profile as the \
         tests (`cargo build -p tinio-server --example serve`; CI adds `--profile ci`)",
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

/// Feed one line of a child's event stream into `frame`, returning the event
/// when the line closes a frame, and clearing `frame` either way.
///
/// **A frame is not a line.** [`Event::encode`] writes an `event:` line, a
/// `data:` line and the blank line that closes them, so a decoder fed one line
/// at a time sees an event name with no payload and drops it; accumulating to
/// the blank line is what makes that line the frame's end rather than a line to
/// skip. Text that is not one of our frames is dropped rather than fatal, which
/// is what makes a stray write to stdout harmless (design §12).
///
/// The line's own terminator is not significant: it is trimmed and re-emitted
/// as `\n`, so a reader holding a pipe's raw CRLF and one holding an already
/// trimmed line build the same frame text, which `sse-core` dispatches alike.
pub fn frame_line(frame: &mut String, line: &str) -> Option<Event> {
    if line.trim_end().is_empty() {
        let event = Event::decode(frame);
        frame.clear();
        return event;
    }
    frame.push_str(line.trim_end());
    frame.push('\n');
    None
}

/// Read a spawned child's event stream until `listening` arrives, and hand back
/// the endpoint it bound — or the message the caller's own `fail` renders.
///
/// **`Ok`/`Err` rather than a `fail` of its own, because the two suites that call
/// this fail differently and both differences are real**: the Windows leg kills a
/// launcher whose log survives the kill and quotes that file, while the Linux leg
/// kills the child and reads the stderr pipe behind it. What the two do *not*
/// differ in is this handshake, which is why it is here and the failures stay
/// with the legs.
///
/// **The read happens on a thread of its own and the wait is bounded here.** The
/// earlier shape read lines straight off the pipe with no deadline, so a startup
/// path that neither writes a frame nor exits — a `confine` that blocks, a config
/// read on a FIFO — hung the whole workspace leg until CI times it out, with no
/// test named: libtest has no per-test timeout. The thread is [`events`]'s and it
/// keeps the pipe drained, so the child cannot block on a full buffer. The budget
/// is the crate's own readiness knob, read through its accessor
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
    let rx = events(stdout);
    let timeout = wait_timeout();
    match rx.recv_timeout(timeout) {
        Ok(Event::Listening { addr }) => Ok(addr),
        Ok(Event::Error { message }) => Err(format!("serve refused to start: {message}")),
        Err(err) => Err(format!(
            "serve reported no `listening` event within {} s ({err})",
            timeout.as_secs()
        )),
    }
}

/// Decode a spawned child's event stream off `pipe`, on a thread of its own,
/// handing back each event as it arrives; the caller's wait on the returned
/// channel is what stays bounded.
///
/// The thread is what keeps the pipe drained: a child with more to say than the
/// pipe holds would otherwise block on it, which reads as a hung server rather
/// than as a full pipe. The budget is the caller's, and it must have one — a
/// confined child that neither reports nor exits would otherwise hang the leg
/// with no test named, libtest having no per-test timeout ([`wait_timeout`] is
/// what every caller here uses).
pub fn events(pipe: impl Read + Send + 'static) -> mpsc::Receiver<Event> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(pipe);
        let mut line = String::new();
        let mut frame = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            if let Some(event) = frame_line(&mut frame, &line) {
                // A send that fails means the caller's wait already gave up and
                // killed the child; there is nowhere left to report it.
                if tx.send(event).is_err() {
                    return;
                }
            }
            line.clear();
        }
    });
    rx
}
