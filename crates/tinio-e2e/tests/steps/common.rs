//! Shared in-process harness for the cucumber scenarios (T024–T026),
//! ported from `tinio-server/tests/common/mod.rs`.
//!
//! Spins up the real data plane ([`DataPlane`]: hyper + hyper-util hosting
//! the s3s service) on `127.0.0.1:0` and drives it with a minimal raw
//! HTTP/1.1 client over `TcpStream` — one connection per request
//! (`Connection: close`, response read to EOF), so the full wire pipeline
//! (routing, XML, error codes, streaming bodies) is exercised without
//! pulling in an HTTP client dependency. The raw client also allows the
//! malformed/truncated requests the abort tests need.
//!
//! Every step goes through [`Client::request`] — one client bound to the
//! scenario's server by the `#[before]` hook. The free-function helpers
//! (`request`, `extract`, `eventually`, …) are kept for the steps that
//! need them directly. The generic response assertions (status, body
//! contains/omits/equals) live here too, so a new feature does not have
//! to hunt for them in a feature-specific step module.

use std::{io, net::SocketAddr, path::Path, str, time::Duration};

use cucumber::{given, then};
use md5::{Digest, Md5};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::sleep,
};

pub use crate::_fs::testing::fs_options;

/// Repeatable body of length `n`: `(i * 31 + 7) % 256` at position `i`.
/// The ONE byte generator the steps share (F14): the upload-vs-verify
/// pairing relies on the part and object uploads producing identical
/// bytes, so a copy that drifts would silently break the pairing.
pub fn deterministic_bytes(n: u64) -> Vec<u8> {
    (0..n).map(|i| ((i * 31 + 7) % 256) as u8).collect()
}

/// The lower-hex MD5 digest of `data` — an INDEPENDENT oracle of the
/// server's ETag (computed straight from the md5 crate, not through
/// `tinio_core::etag::ETag`: a test that verified the server's ETag
/// with the production helper would be circular). One home for the
/// steps (F14).
pub fn md5_hex(data: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher
        .finalize()
        .into_iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
use crate::{
    _core::storage::Storage,
    _fs::{FsStorage, Scanner, ScannerOptions},
    _mem::MemoryStorage,
    _server::{Capabilities, DataPlane},
};

/// Which storage backend the in-process server runs on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Backend {
    #[default]
    Fs,
    Mem,
}

/// Which fs-server variant a scenario's tags demand.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsKind {
    #[default]
    Plain,
    /// root = tempdir/"root" — traversal-proof scenarios.
    NestedRoot,
    /// Fast scanner interval (cold-listing scenarios).
    ColdListing(Duration),
}

/// A running in-process server bound to an ephemeral loopback port.
#[derive(Debug)]
pub struct Server {
    addr: SocketAddr,
    /// Kept alive for the server's lifetime (the fs root must not be
    /// deleted while the plane serves it) — never read otherwise.
    _root: Option<TempDir>,
    served: Option<std::path::PathBuf>,
    shutdown: watch::Sender<bool>,
}

impl Server {
    /// Serve a fresh filesystem-backed root (a temp dir).
    pub async fn fs(caps: Capabilities) -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        Self::spawn_with(storage, caps, Some(root), None).await
    }

    /// Serve the in-memory reference backend.
    pub async fn mem(caps: Capabilities) -> Self {
        Self::spawn_with(MemoryStorage::new().unwrap(), caps, None, None).await
    }

    /// Serve a filesystem backend over a nested root: the base tempdir
    /// contains only the served `root` subdir, so the traversal-proof
    /// scenarios can observe the parent to prove no file escaped.
    pub async fn fs_nested(caps: Capabilities) -> Self {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        tokio::fs::create_dir(&root).await.unwrap();
        let storage = FsStorage::new(&root, fs_options()).unwrap();
        let mut server = Self::spawn_with(storage, caps, Some(base), None).await;
        server.served = Some(root);
        server
    }

    /// Serve a filesystem-backed root with a background scanner on a fast
    /// cycle (`@cold-listing` scenarios): the scanner re-scans the tree
    /// every `interval`, so out-of-band changes surface promptly. The
    /// in-process data plane does not wire a scanner (the config-driven
    /// `serve` path does), so this constructor runs the real [`Scanner`]
    /// next to the plane on the same shutdown channel.
    pub async fn fs_with_scanner_interval(caps: Capabilities, interval: Duration) -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let scanner = Scanner::new(
            storage.clone(),
            ScannerOptions {
                enabled: true,
                delay: Duration::from_millis(1),
                max_wait: Duration::from_millis(10),
                cycle: interval,
            },
        );
        Self::spawn_with(storage, caps, Some(root), Some(scanner)).await
    }

    /// Bind the data plane — plus an optional background scanner on a
    /// cloned shutdown receiver (it aborts on shutdown) — and spawn its
    /// accept loop.
    async fn spawn_with<S: Storage>(
        storage: S,
        caps: Capabilities,
        root: Option<TempDir>,
        scanner: Option<Scanner>,
    ) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        if let Some(scanner) = scanner {
            tokio::spawn(scanner.run(rx.clone()));
        }
        let plane = DataPlane::new(storage, caps);
        tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });
        let served = root.as_ref().map(|r| r.path().to_path_buf());
        Self {
            addr,
            _root: root,
            served,
            shutdown,
        }
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The served root (fs backends only): the tempdir for [`Server::fs`]
    /// and [`Server::fs_with_scanner_interval`], the nested `base/root`
    /// for [`Server::fs_nested`].
    pub fn root(&self) -> Option<&Path> {
        self.served.as_deref()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

/// A raw HTTP response: status, headers (lower-cased names), body bytes.
#[derive(Debug, Clone, Default)]
pub struct LastResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl LastResponse {
    /// The first value of `name` (case-insensitive; the stored names
    /// are lower-cased at parse).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Raw HTTP client used by every step (one per scenario): bound to the
/// scenario's server by the `#[before]` hook, then every request goes
/// through [`Client::request`] (the ported `request` helper over the
/// bound address).
#[derive(Debug, Clone, Default)]
pub struct Client {
    addr: Option<SocketAddr>,
}

impl Client {
    /// Bind to the scenario's server.
    pub fn bind(&mut self, addr: SocketAddr) {
        self.addr = Some(addr);
    }

    /// One raw HTTP request on a fresh connection; see [`request`].
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> LastResponse {
        request(
            self.addr.expect("client bound to a server"),
            method,
            path,
            headers,
            body,
        )
        .await
    }
}

/// One HTTP/1.1 request on a fresh connection; the response is read to
/// EOF (`Connection: close`) and de-chunked when needed.
///
/// Retry policy — the connect stage only, up to 2 retries ~50 ms apart,
/// on `ConnectionAborted` / `ConnectionRefused` (the Windows loopback
/// accept race: with scenarios running in parallel, each binding its own
/// ephemeral-port listener, the stack sometimes aborts a connection
/// before the server accepts it — the flake root the CI `--retry 1`
/// papers over). Two manifestations are covered:
///
/// - `TcpStream::connect` itself fails, or
/// - the first write (the request head) fails after zero bytes were
///   transmitted — the connection died in the accept queue; the server
///   never received a byte, so the request cannot have executed.
///
/// Once ANY byte has been sent, no retry happens: the server may already
/// have started executing the request (e.g. a PUT that wrote the object),
/// and a retry could double-apply a non-idempotent write.
pub async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> LastResponse {
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if !headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-length"))
    {
        head += &format!("Content-Length: {}\r\n", body.len());
    }
    for (k, v) in headers {
        head += &format!("{k}: {v}\r\n");
    }
    head += "\r\n";
    let retryable = |e: &io::Error| {
        matches!(
            e.kind(),
            io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionRefused
        )
    };
    let mut attempt = 0;
    loop {
        // Phase 1 — connect + transmit the request head. Both may abort
        // before any byte reaches the server; both are retried (see the
        // policy above).
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) if retryable(&e) && attempt < 2 => {
                attempt += 1;
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => panic!("cannot connect to {addr}: {e}"),
        };
        match stream.write(head.as_bytes()).await {
            Ok(n) if n == head.len() => {}
            Ok(n) => {
                // A partial head (rare): finish it. Any failure from here
                // on is a genuine post-transmission failure — never
                // retried.
                stream
                    .write_all(&head.as_bytes()[n..])
                    .await
                    .expect("finish the request head");
            }
            Err(e) if retryable(&e) && attempt < 2 => {
                // Zero bytes were sent: the connection died in the accept
                // queue and the request never reached the server — the
                // same safety class as a connect failure.
                attempt += 1;
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => panic!("cannot send request to {addr}: {e}"),
        }
        // Phase 2 — body and response. Never retried: the head went out,
        // so the server may have executed the request.
        stream.write_all(body).await.expect("send the request body");
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .expect("read the response");
        return parse_response(&raw);
    }
}

/// A PUT whose body is cut off mid-stream: the headers declare
/// `declared_len` bytes, only `partial` are sent, then the connection is
/// dropped — an interrupted upload.
pub async fn abort_mid_upload(addr: SocketAddr, path: &str, declared_len: usize, partial: &[u8]) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head =
        format!("PUT {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {declared_len}\r\n\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(partial).await.unwrap();
    stream.shutdown().await.unwrap();
}

/// Poll `cond` (50 ms steps, ~5 s budget) until it holds — for effects
/// that land asynchronously after a connection drops.
pub async fn eventually(cond: impl FnMut() -> bool) -> bool {
    let mut cond = cond;
    for _ in 0..100 {
        if cond() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    cond()
}

/// The text between the first `open`/`close` tag pair — empty when the
/// opener (or the closer) is absent, so a missing tag never yields a
/// garbage slice of the body.
pub fn extract(text: &str, open: &str, close: &str) -> String {
    let Some(start) = text.find(open).map(|i| i + open.len()) else {
        return String::new();
    };
    let Some(end) = text[start..].find(close).map(|i| start + i) else {
        return String::new();
    };
    text[start..end].to_string()
}

/// The generic response assertions — one home, so a new feature does not
/// have to hunt for them in a feature-specific step module (the wire-XML
/// scenarios of tagging/listing/conditions use the body-contains form).

#[given(expr = "the response status is {int}")]
#[then(expr = "the response status is {int}")]
async fn status_is(world: &mut super::World, status: u16) {
    assert_eq!(world.last.status, status, "status mismatch");
}

/// The last response body contains `text` (a wire-XML fragment).
#[then(expr = "the response body contains {string}")]
async fn body_contains(world: &mut super::World, text: String) {
    let body = String::from_utf8_lossy(&world.last.body);
    assert!(body.contains(&text), "body missing {text:?}: {body}");
}

/// The last response body does not contain `text` — e.g. a quiet-mode
/// DeleteObjects response carries neither `<Deleted>` entries nor errors.
#[then(expr = "the response body does not contain {string}")]
async fn body_omits(world: &mut super::World, text: String) {
    let body = String::from_utf8_lossy(&world.last.body);
    assert!(
        !body.contains(&text),
        "body unexpectedly contains {text:?}: {body}"
    );
}

/// The last response body is exactly `text` (the nested-root scenario's
/// "non-reserved objects of the inner root are served normally"). The
/// `object body is` phrase (objects/multipart/conditions/reserved_paths
/// features) is the same exact-equality assertion under the features'
/// wording.
#[then(expr = "the response body is {string}")]
#[then(expr = "the object body is {string}")]
async fn body_is(world: &mut super::World, text: String) {
    assert_eq!(world.last.body, text.as_bytes(), "response body mismatch");
}

/// How many times `tag` (a wire-XML open tag like `<Key>`) occurs in the
/// response body — the "shows N keys/parts/uploads" assertions' one home
/// (a folder marker is never a key, so `<Key>` occurrences are counted,
/// not parsed).
pub(super) fn count_tag(body: &[u8], tag: &str) -> usize {
    String::from_utf8_lossy(body).matches(tag).count()
}

/// The sorted top-level entry names of `dir` — shared by the
/// traversal-proof and root-shape assertions.
pub(super) async fn sorted_entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    names
}

/// Split a raw HTTP response into status, headers, and (de-chunked) body.
fn parse_response(raw: &[u8]) -> LastResponse {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .expect("status code");
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_lowercase(), v.trim().to_string()))
        })
        .collect();
    let mut body = raw[split + 4..].to_vec();
    let chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.to_lowercase().contains("chunked"));
    if chunked {
        body = dechunk(&body);
    }
    LastResponse {
        status: code,
        headers,
        body,
    }
}

/// Reassemble a chunked transfer-coded body (trailers ignored).
fn dechunk(mut rest: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let pos = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("chunk size line");
        let size_text = str::from_utf8(&rest[..pos]).unwrap();
        let size = usize::from_str_radix(size_text.trim(), 16).expect("chunk size");
        rest = &rest[pos + 2..];
        if size == 0 {
            break;
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
    out
}
