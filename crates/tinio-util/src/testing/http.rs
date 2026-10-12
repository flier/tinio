//! The raw HTTP/1.1 client two test harnesses share.
//!
//! `tinio-e2e`'s steps drive a spawned server and `tinio-cli`'s pg serving leg drives an
//! in-process one, and both do the same thing: write one request head and body on a fresh
//! connection, read the response to EOF, and split it. The framing is one text here rather than a
//! copy per harness, and it is test-only — nothing in the served path speaks HTTP by hand.

use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    str,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    time::sleep,
};

/// How long two consecutive connect attempts wait for each other.
const RETRY_DELAY: Duration = Duration::from_millis(50);

/// A raw HTTP response: status, headers (lower-cased names), body bytes.
#[derive(Debug, Clone, Default)]
pub struct Response {
    /// The status line's code.
    pub status: u16,
    /// The header lines, names lower-cased at the parse.
    pub headers: Vec<(String, String)>,
    /// The body, de-chunked when the response was transfer-coded.
    pub body: Vec<u8>,
}

impl Response {
    /// The first value of `name` (case-insensitive; the stored names
    /// are lower-cased at parse).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// One HTTP/1.1 request on a fresh connection; the response is read to
/// EOF (`Connection: close`) and de-chunked when needed.
///
/// `host` is what the `Host` header carries — the authority the caller signed, which is not
/// always the address it dialled.
///
/// Retry policy — the connect stage only, up to `retries` attempts ~50 ms apart, on
/// `ConnectionAborted` / `ConnectionRefused` (the Windows loopback
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
/// and a retry could double-apply a non-idempotent write. A caller that drives an in-process
/// server bounds the attempts at `0`: there is no queue to lose the connection in, and a refusal
/// is that server's own answer.
pub async fn exchange(
    host: &str,
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    retries: u32,
) -> Response {
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
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
            ErrorKind::ConnectionAborted | ErrorKind::ConnectionRefused
        )
    };
    let mut attempt = 0;
    loop {
        // Phase 1 — connect + transmit the request head. Both may abort
        // before any byte reaches the server; both are retried (see the
        // policy above).
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) if retryable(&e) && attempt < retries => {
                attempt += 1;
                sleep(RETRY_DELAY).await;
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
            Err(e) if retryable(&e) && attempt < retries => {
                // Zero bytes were sent: the connection died in the accept
                // queue and the request never reached the server — the
                // same safety class as a connect failure.
                attempt += 1;
                sleep(RETRY_DELAY).await;
                continue;
            }
            Err(e) => panic!("cannot send request to {addr}: {e}"),
        }
        // Phase 2 — body and response. The body write is never retried:
        // the head went out, so the server may already have executed the
        // request. The response read can still catch the same accept-queue
        // abort: if the connection dies BEFORE any response byte arrives
        // the server never executed the request (it was still queued), so
        // retrying is safe. A read that aborts mid-stream (partial bytes
        // already received) is never retried — the request ran.
        stream.write_all(body).await.expect("send the request body");
        let mut raw = Vec::new();
        match stream.read_to_end(&mut raw).await {
            Ok(_) => return parse_response(&raw),
            Err(e) if raw.is_empty() && retryable(&e) && attempt < retries => {
                attempt += 1;
                sleep(RETRY_DELAY).await;
                continue;
            }
            Err(e) => panic!("read the response from {addr}: {e}"),
        }
    }
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
