//! Shared harness for the tinio-server integration tests (T024–T026).
//!
//! Spins up the real data plane ([`DataPlane`]: hyper + hyper-util hosting
//! the s3s service) on `127.0.0.1:0` and drives it with a minimal raw
//! HTTP/1.1 client over `TcpStream` — one connection per request
//! (`Connection: close`, response read to EOF), so the full wire pipeline
//! (routing, XML, error codes, streaming bodies) is exercised without
//! pulling in an HTTP client dependency. The raw client also allows the
//! malformed/truncated requests the abort tests need.

// Each test binary compiles its own copy of this module and uses a
// different subset of the helpers.
#![allow(dead_code)]

use std::{net::SocketAddr, path::Path, str, time::Duration};

use http::StatusCode;
use tempfile::TempDir;
use tinio_core::storage::Storage;
use tinio_fs::FsStorage;
pub use tinio_fs::testing::fs_options;
use tinio_mem::MemoryStorage;
use tinio_server::{Capabilities, DataPlane};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::sleep,
};

/// A running in-process server bound to an ephemeral loopback port.
pub struct Server {
    addr: SocketAddr,
    root: Option<TempDir>,
    shutdown: watch::Sender<bool>,
}

impl Server {
    /// Serve a fresh filesystem-backed root (a temp dir).
    pub async fn fs(caps: Capabilities) -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        Self::spawn(storage, caps, Some(root)).await
    }

    /// Serve a filesystem backend over `root` (caller keeps the dir).
    pub async fn fs_at(root: &Path, caps: Capabilities) -> Self {
        let storage = FsStorage::new(root, fs_options()).unwrap();
        Self::spawn(storage, caps, None).await
    }

    /// Serve the in-memory reference backend.
    pub async fn mem(caps: Capabilities) -> Self {
        Self::spawn(MemoryStorage::new().unwrap(), caps, None).await
    }

    /// Bind the data plane and spawn its accept loop.
    async fn spawn<S: Storage>(storage: S, caps: Capabilities, root: Option<TempDir>) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        let plane = DataPlane::new(storage, caps);
        tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });
        Self {
            addr,
            root,
            shutdown,
        }
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The served root (servers built by [`Server::fs`] only).
    pub fn root(&self) -> &Path {
        self.root.as_ref().unwrap().path()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

/// A raw HTTP response: status, headers (lower-cased names), body bytes.
pub struct Response {
    /// The status code.
    pub status: StatusCode,
    /// The headers, names lower-cased, in wire order.
    pub headers: Vec<(String, String)>,
    /// The (de-chunked) body.
    pub body: Vec<u8>,
}

impl Response {
    /// The first value of `name` (case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The body as UTF-8 text.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The S3 `<Code>` of an XML error body (empty when absent).
    pub fn error_code(&self) -> String {
        error_code(&self.body)
    }
}

/// One HTTP/1.1 request on a fresh connection; the response is read to
/// EOF (`Connection: close`) and de-chunked when needed.
pub async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Response {
    let mut stream = TcpStream::connect(addr).await.unwrap();
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
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    parse_response(&raw)
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

/// The text between the first `open`/`close` tag pair (empty when absent).
pub fn extract(text: &str, open: &str, close: &str) -> String {
    let start = text.find(open).map(|i| i + open.len()).unwrap_or(0);
    let end = text[start..]
        .find(close)
        .map(|i| start + i)
        .unwrap_or(text.len());
    text[start..end].to_string()
}

/// Parse an error-code XML body: the `<Code>` text (empty when absent).
pub fn error_code(body: &[u8]) -> String {
    extract(&String::from_utf8_lossy(body), "<Code>", "</Code>")
}

/// Split a raw HTTP response into status, headers, and (de-chunked) body.
fn parse_response(raw: &[u8]) -> Response {
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
    Response {
        status: StatusCode::from_u16(code).unwrap(),
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
