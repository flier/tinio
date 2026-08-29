//! Full data-plane round-trip integration tests over real HTTP (task
//! T025).
//!
//! create bucket / upload / download (byte-identical, ETag = content MD5)
//! / list (prefix + delimiter grouping, pagination) / delete; the
//! uploaded file physically appears in the served directory; zero-byte
//! objects; nested keys; concurrent writes last-write-wins with no torn
//! objects; an interrupted upload leaves no partial object; Range
//! requests (206/Content-Range); conditional requests (304/412); folder
//! markers (`dir/` never an object); out-of-band file changes served
//! immediately (SC-006). Reserved-path behavior lives in
//! `reserved_paths.rs` (T026).

mod common;

use http::StatusCode;

use tinio_server::Capabilities;

use common::{Server, abort_mid_upload, eventually, extract, request};

/// A fresh fs-backed server with default toggles.
async fn fs_server() -> Server {
    Server::fs(Capabilities::default()).await
}

#[tokio::test]
#[cfg(feature = "list-v2")]
async fn full_round_trip_with_listing_and_delete() {
    let server = fs_server().await;
    let addr = server.addr();

    // Create bucket.
    let resp = request(addr, "PUT", "/data", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);

    // Upload (byte-identical download; ETag is the content MD5).
    let resp = request(addr, "PUT", "/data/hello.txt", &[], b"hello world").await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(
        resp.header("etag"),
        Some("\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")
    );

    // The uploaded file physically appears in the local directory.
    assert_eq!(
        std::fs::read(server.root().join("data/hello.txt")).unwrap(),
        b"hello world"
    );

    let resp = request(addr, "GET", "/data/hello.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body, b"hello world");
    assert_eq!(
        resp.header("etag"),
        Some("\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")
    );
    // Content-Type is inferred from the key (FR-022).
    assert_eq!(resp.header("content-type"), Some("text/plain"));

    // Zero-byte object.
    let resp = request(addr, "PUT", "/data/empty", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    let resp = request(addr, "GET", "/data/empty", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body, b"");
    assert_eq!(resp.header("content-length"), Some("0"));
    assert_eq!(
        resp.header("etag"),
        Some("\"d41d8cd98f00b204e9800998ecf8427e\"")
    );

    // Nested keys.
    request(addr, "PUT", "/data/dir/sub/deep.txt", &[], b"deep").await;

    // List V2: everything.
    let resp = request(addr, "GET", "/data?list-type=2", &[], &[]).await;
    let text = resp.text();
    assert!(text.contains("<Key>hello.txt</Key>"));
    assert!(text.contains("<Key>empty</Key>"));
    assert!(text.contains("<Key>dir/sub/deep.txt</Key>"));

    // Delimiter grouping rolls `dir/` up into a common prefix.
    let resp = request(addr, "GET", "/data?list-type=2&delimiter=/", &[], &[]).await;
    let text = resp.text();
    assert!(text.contains("<Key>hello.txt</Key>"));
    assert!(text.contains("<Prefix>dir/</Prefix>"));
    assert!(!text.contains("<Key>dir/sub/deep.txt</Key>"));

    // Prefix filtering.
    let resp = request(addr, "GET", "/data?list-type=2&prefix=dir/", &[], &[]).await;
    let text = resp.text();
    assert!(text.contains("<Key>dir/sub/deep.txt</Key>"));
    assert!(!text.contains("<Key>hello.txt</Key>"));

    // Pagination: page 1 of 1 key, then follow the continuation token.
    let resp = request(addr, "GET", "/data?list-type=2&max-keys=1", &[], &[]).await;
    let mut pages = vec![resp.text()];
    assert!(pages[0].contains("<IsTruncated>true</IsTruncated>"));
    // Two follow-up pages: the second is still truncated, the third ends.
    for _ in 0..2 {
        let token = extract(
            pages.last().unwrap(),
            "<NextContinuationToken>",
            "</NextContinuationToken>",
        );
        let path = format!(
            "/data?list-type=2&max-keys=1&continuation-token={}",
            url_encode(&token)
        );
        let resp = request(addr, "GET", &path, &[], &[]).await;
        pages.push(resp.text());
    }
    assert!(pages[1].contains("<IsTruncated>true</IsTruncated>"));
    assert!(pages[2].contains("<IsTruncated>false</IsTruncated>"));
    // Three pages of one key cover all three objects, each exactly once.
    let keys: Vec<String> = pages
        .iter()
        .map(|p| extract(p, "<Key>", "</Key>"))
        .collect();
    assert_eq!(keys, ["dir/sub/deep.txt", "empty", "hello.txt"]);

    // Delete.
    let resp = request(addr, "DELETE", "/data/hello.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let resp = request(addr, "GET", "/data/hello.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchKey");
    assert!(!server.root().join("data/hello.txt").exists());
}

#[tokio::test]
async fn range_requests() {
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;
    request(addr, "PUT", "/data/digits", &[], b"0123456789").await;

    // bytes=2-5
    let resp = request(addr, "GET", "/data/digits", &[("Range", "bytes=2-5")], &[]).await;
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.header("content-range"), Some("bytes 2-5/10"));
    assert_eq!(resp.body, b"2345");

    // Suffix range bytes=-3.
    let resp = request(addr, "GET", "/data/digits", &[("Range", "bytes=-3")], &[]).await;
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.header("content-range"), Some("bytes 7-9/10"));
    assert_eq!(resp.body, b"789");

    // Unsatisfiable → 416 InvalidRange.
    let resp = request(addr, "GET", "/data/digits", &[("Range", "bytes=99-")], &[]).await;
    assert_eq!(resp.status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(resp.error_code(), "InvalidRange");
}

#[tokio::test]
async fn conditional_requests() {
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;
    let resp = request(addr, "PUT", "/data/hello.txt", &[], b"hello").await;
    let etag = resp.header("etag").unwrap().to_string();

    // If-None-Match matching → 304.
    let resp = request(
        addr,
        "GET",
        "/data/hello.txt",
        &[("If-None-Match", &etag)],
        &[],
    )
    .await;
    assert_eq!(resp.status, StatusCode::NOT_MODIFIED);

    // If-Match mismatching → 412.
    let stale = "\"deadbeefdeadbeefdeadbeefdeadbeef\"";
    let resp = request(addr, "GET", "/data/hello.txt", &[("If-Match", stale)], &[]).await;
    assert_eq!(resp.status, StatusCode::PRECONDITION_FAILED);

    // If-Match matching → 200.
    let resp = request(addr, "GET", "/data/hello.txt", &[("If-Match", &etag)], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body, b"hello");

    // Failed conditional Put: If-Match mismatch → 412, object untouched.
    let resp = request(
        addr,
        "PUT",
        "/data/hello.txt",
        &[("If-Match", stale)],
        b"changed",
    )
    .await;
    assert_eq!(resp.status, StatusCode::PRECONDITION_FAILED);
    let resp = request(addr, "GET", "/data/hello.txt", &[], &[]).await;
    assert_eq!(resp.body, b"hello");

    // Failed conditional Put: If-None-Match * on an existing key → 412.
    let resp = request(
        addr,
        "PUT",
        "/data/hello.txt",
        &[("If-None-Match", "*")],
        b"changed",
    )
    .await;
    assert_eq!(resp.status, StatusCode::PRECONDITION_FAILED);
    let resp = request(addr, "GET", "/data/hello.txt", &[], &[]).await;
    assert_eq!(resp.body, b"hello");

    // If-None-Match * on a fresh key → the write goes through.
    let resp = request(
        addr,
        "PUT",
        "/data/fresh.txt",
        &[("If-None-Match", "*")],
        b"new",
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn folder_markers_never_objects() {
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;

    // PUT dir/ → 200, creates the directory (no object).
    let resp = request(addr, "PUT", "/data/dir/", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(server.root().join("data/dir").is_dir());

    // GET / HEAD on dir/ → NoSuchKey.
    let resp = request(addr, "GET", "/data/dir/", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchKey");
    let resp = request(addr, "HEAD", "/data/dir/", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // DELETE dir/ on a non-empty directory → 204, the content stays.
    request(addr, "PUT", "/data/dir/file.txt", &[], b"kept").await;
    let resp = request(addr, "DELETE", "/data/dir/", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let resp = request(addr, "GET", "/data/dir/file.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);

    // DELETE dir/ on an empty directory → 204, the directory is removed.
    request(addr, "DELETE", "/data/dir/file.txt", &[], &[]).await;
    let resp = request(addr, "DELETE", "/data/dir/", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    assert!(!server.root().join("data/dir").exists());
}

#[tokio::test]
async fn concurrent_writes_last_write_wins_no_torn_objects() {
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;

    // Concurrent PUTs of different payloads on separate connections; the
    // final object is exactly one of them (never a torn mix).
    let mut handles = Vec::new();
    for i in 0..8u32 {
        handles.push(tokio::spawn(async move {
            let payload = vec![b'a' + (i % 26) as u8; 64 * 1024 + i as usize];
            let resp = request(addr, "PUT", "/data/shared.bin", &[], &payload).await;
            assert_eq!(resp.status, StatusCode::OK);
            payload
        }));
    }
    let mut payloads = Vec::new();
    for h in handles {
        payloads.push(h.await.unwrap());
    }

    // The final object — on disk and over the wire — is one complete
    // writer's payload.
    let on_disk = std::fs::read(server.root().join("data/shared.bin")).unwrap();
    assert!(
        payloads.iter().any(|p| p == &on_disk),
        "final object is not any single writer's payload"
    );
    let resp = request(addr, "GET", "/data/shared.bin", &[], &[]).await;
    assert_eq!(resp.body, on_disk);
}

#[tokio::test]
async fn interrupted_upload_leaves_no_partial_object() {
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;

    // Declare 1 MiB, send 1 KiB, drop the connection.
    abort_mid_upload(addr, "/data/aborted.bin", 1024 * 1024, &[b'x'; 1024]).await;

    // The key never appears …
    let resp = request(addr, "GET", "/data/aborted.bin", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchKey");

    // … and no temp file remains under the state dir (the writer removes
    // it best-effort once the aborted stream errors — poll briefly for
    // the server to notice the dropped connection).
    let tmp = server.root().join(".tinio/tmp");
    let clean =
        eventually(|| !tmp.exists() || std::fs::read_dir(&tmp).unwrap().next().is_none()).await;
    assert!(clean, "temp file left behind under {tmp:?}");
}

#[tokio::test]
async fn out_of_band_changes_served_immediately() {
    // SC-006: a file dropped into the directory by hand is immediately
    // retrievable through the S3 interface.
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;
    std::fs::write(server.root().join("data/dropped.txt"), b"out-of-band").unwrap();

    let resp = request(addr, "GET", "/data/dropped.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body, b"out-of-band");
    // The ETag is recomputed on the fly (content MD5).
    assert_eq!(
        resp.header("etag"),
        Some("\"ca65dbc792c101ed142ffe5c8656322b\"")
    );

    // And it shows up in listings without any restart/rescan.
    #[cfg(feature = "list-v2")]
    {
        let resp = request(addr, "GET", "/data?list-type=2", &[], &[]).await;
        assert!(resp.text().contains("<Key>dropped.txt</Key>"));
    }
}

/// Percent-encode a query-string value (unreserved + `/` pass through).
fn url_encode(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => out += &format!("%{b:02X}"),
        }
    }
    out
}
