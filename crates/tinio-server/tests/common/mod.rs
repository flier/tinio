//! The raw-HTTP wire helpers both read-only integration targets share.
//!
//! `read_only.rs` drives the plane in process, `serve_read_only.rs` drives the
//! spawned `serve` binary; both speak raw HTTP/1.1 to a loopback port and both
//! read S3's XML answers. The response parser, the de-chunker, the
//! connect-with-retry client and the on-disk fixture writer therefore live
//! here once, instead of in two copies nothing forces together — a fix to a
//! chunked-body edge case used to land in one and silently not the other.
//!
//! Each target declares `mod common;` and takes what it uses, so neither uses
//! everything: hence the module-wide `dead_code` allow. Nothing here asserts —
//! these are the helpers; the legs own the expectations.

#![allow(dead_code)]

use std::{fs, io, net::SocketAddr, path::Path, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::sleep,
};

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

/// Plant a bucket directory and one object file directly on disk — the only
/// way a read-only plane can have anything to read: every mutating request,
/// bucket create and object upload included, is refused.
pub fn plant(root: &Path, bucket: &str, key: &str, body: &str) {
    let path = root.join(bucket).join(key);
    fs::create_dir_all(path.parent().expect("the key has a parent")).expect("the bucket dir");
    fs::write(&path, body).expect("the planted object");
}
