//! Coverage-gap integration tests over real HTTP: branches the earlier
//! suites left uncovered — the `/metrics` endpoint through the data-plane
//! middleware, v1 listing (delimiter grouping, marker pagination),
//! GetObjectTagging, DeleteObjects quiet mode, a conditional PUT against
//! a missing key, multipart EntityTooSmall / part-number-marker
//! validation, and copy-source-range validation.

mod common;

use common::{Server, extract, request};
use http::StatusCode;
use tinio_server::Capabilities;

/// A fresh fs-backed server with default toggles.
async fn fs_server() -> Server {
    Server::fs(Capabilities::default()).await
}

#[tokio::test]
async fn metrics_endpoint_served_before_the_s3_service() {
    // F10: GET /metrics on the data-plane listener answers the
    // Prometheus text format — a management path never routed to the
    // storage plane. The HTTP families register on the first served
    // request, so one normal request precedes the scrape.
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;

    let resp = request(addr, "GET", "/metrics", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(
        resp.header("content-type"),
        Some("text/plain; version=0.0.4")
    );
    assert!(
        resp.text().contains("tinio_http_requests_total"),
        "the served families must include the HTTP counters: {}",
        resp.text()
    );
}

#[tokio::test]
#[cfg(feature = "list-v1")]
async fn list_v1_round_trip_with_delimiter_and_marker() {
    // The v1 ListObjects surface: Name/prefix/marker/max-keys fields,
    // delimiter common-prefix grouping, and marker pagination — the
    // path-style GET without `list-type` (only reachable with the
    // list-v1 feature).
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;
    request(addr, "PUT", "/data/a.txt", &[], b"a").await;
    request(addr, "PUT", "/data/b.txt", &[], b"b").await;
    request(addr, "PUT", "/data/dir/c.txt", &[], b"c").await;

    // Plain v1 list: every key, the bucket name echoed, not truncated.
    let resp = request(addr, "GET", "/data", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    let text = resp.text();
    assert!(text.contains("<Name>data</Name>"), "{text}");
    assert!(text.contains("<Key>a.txt</Key>"), "{text}");
    assert!(text.contains("<Key>b.txt</Key>"), "{text}");
    assert!(text.contains("<Key>dir/c.txt</Key>"), "{text}");
    assert!(text.contains("<IsTruncated>false</IsTruncated>"), "{text}");

    // Delimiter grouping rolls dir/ up into a common prefix.
    let resp = request(addr, "GET", "/data?delimiter=/", &[], &[]).await;
    let text = resp.text();
    assert!(text.contains("<Prefix>dir/</Prefix>"), "{text}");
    assert!(!text.contains("<Key>dir/c.txt</Key>"), "{text}");

    // Marker pagination: max-keys=1 after marker a.txt → b.txt, truncated.
    let resp = request(addr, "GET", "/data?marker=a.txt&max-keys=1", &[], &[]).await;
    let text = resp.text();
    assert!(text.contains("<Marker>a.txt</Marker>"), "{text}");
    assert!(text.contains("<Key>b.txt</Key>"), "{text}");
    assert!(text.contains("<IsTruncated>true</IsTruncated>"), "{text}");
    assert!(text.contains("<NextMarker>b.txt</NextMarker>"), "{text}");
}

#[tokio::test]
async fn get_object_tagging_answers_empty_set() {
    // GetObjectTagging: always an empty tag set (v1 stores no tags);
    // a missing object answers NoSuchKey.
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;
    request(addr, "PUT", "/data/tagged.txt", &[], b"x").await;

    let resp = request(addr, "GET", "/data/tagged.txt?tagging", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(resp.text().contains("<TagSet>"), "{}", resp.text());

    let resp = request(addr, "GET", "/data/missing.txt?tagging", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchKey");
}

#[tokio::test]
async fn delete_objects_quiet_mode_suppresses_deleted_entries() {
    // DeleteObjects: quiet=true drops the per-key <Deleted> entries (the
    // response carries neither a deleted list nor errors), while
    // quiet=false echoes them; the objects vanish either way.
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;
    request(addr, "PUT", "/data/a.txt", &[], b"a").await;
    request(addr, "PUT", "/data/b.txt", &[], b"b").await;

    let body = b"<Delete><Quiet>true</Quiet><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>";
    let resp = request(
        addr,
        "POST",
        "/data?delete",
        &[("Content-Type", "application/xml")],
        body,
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    let text = resp.text();
    assert!(!text.contains("<Deleted>"), "{text}");
    assert!(!text.contains("<Error>"), "{text}");
    let resp = request(addr, "GET", "/data/a.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    let resp = request(addr, "GET", "/data/b.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    // quiet=false echoes the deleted keys.
    request(addr, "PUT", "/data/c.txt", &[], b"c").await;
    let body = b"<Delete><Object><Key>c.txt</Key></Object></Delete>";
    let resp = request(
        addr,
        "POST",
        "/data?delete",
        &[("Content-Type", "application/xml")],
        body,
    )
    .await;
    let text = resp.text();
    assert!(text.contains("<Deleted>"), "{text}");
    assert!(text.contains("<Key>c.txt</Key>"), "{text}");
}

#[tokio::test]
async fn conditional_put_on_a_missing_key_precondition_fails() {
    // RFC 7232 + the shared destination protocol: an If-Match against a
    // key with no current version can never match — 412, and the write
    // is rejected. If-None-Match: * on the same missing key passes.
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;

    let resp = request(
        addr,
        "PUT",
        "/data/missing.txt",
        &[("If-Match", "\"deadbeefdeadbeefdeadbeefdeadbeef\"")],
        b"x",
    )
    .await;
    assert_eq!(resp.status, StatusCode::PRECONDITION_FAILED);
    let resp = request(addr, "GET", "/data/missing.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);

    let resp = request(
        addr,
        "PUT",
        "/data/missing.txt",
        &[("If-None-Match", "*")],
        b"x",
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
#[cfg(feature = "multipart")]
async fn multipart_non_final_part_too_small_answers_entity_too_small() {
    // CompleteMultipartUpload: every non-final part must be at least the
    // 5 MiB minimum (EntityTooSmall) — the size check runs before any
    // assembly; the final part has no minimum, so a single-part
    // completion of a tiny part succeeds.
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;

    // Single-part completion (the final part has no minimum) — the happy
    // wire path.
    let upload_id = create_upload(addr, "/data/small.bin").await;
    let etag = upload_part(addr, "/data/small.bin", 1, &upload_id, b"tiny").await;
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let resp = request(
        addr,
        "POST",
        &format!("/data/small.bin?uploadId={upload_id}"),
        &[("Content-Type", "application/xml")],
        body.as_bytes(),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    let resp = request(addr, "GET", "/data/small.bin", &[], &[]).await;
    assert_eq!(resp.body, b"tiny");

    // Two parts, the first below the minimum → EntityTooSmall.
    let upload_id = create_upload(addr, "/data/big.bin").await;
    let etag1 = upload_part(addr, "/data/big.bin", 1, &upload_id, b"small").await;
    let etag2 = upload_part(addr, "/data/big.bin", 2, &upload_id, b"tail").await;
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let resp = request(
        addr,
        "POST",
        &format!("/data/big.bin?uploadId={upload_id}"),
        &[("Content-Type", "application/xml")],
        body.as_bytes(),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert_eq!(resp.error_code(), "EntityTooSmall");
}

#[tokio::test]
#[cfg(feature = "multipart")]
async fn list_parts_rejects_a_negative_marker() {
    // A negative part-number-marker would wrap to a huge u32 and mask
    // every part as already listed — rejected up front with
    // InvalidArgument (before any storage call).
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/data", &[], &[]).await;
    let upload_id = create_upload(addr, "/data/parts.bin").await;

    let resp = request(
        addr,
        "GET",
        &format!("/data/parts.bin?uploadId={upload_id}&part-number-marker=-1"),
        &[],
        &[],
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert_eq!(resp.error_code(), "InvalidArgument");
}

#[tokio::test]
#[cfg(feature = "copy")]
#[cfg(feature = "multipart")]
async fn upload_part_copy_rejects_an_open_source_range() {
    // The copy source range accepts only the closed `bytes=first-last`
    // form (the suffix/open forms GET accepts answer InvalidArgument).
    // The parse runs before any storage call — a bogus upload id
    // suffices for the rejection.
    let server = fs_server().await;
    let addr = server.addr();
    request(addr, "PUT", "/src", &[], &[]).await;
    request(addr, "PUT", "/dst", &[], &[]).await;
    request(addr, "PUT", "/src/key.bin", &[], b"0123456789").await;

    let resp = request(
        addr,
        "PUT",
        "/dst/parts.bin?partNumber=1&uploadId=none",
        &[
            ("x-amz-copy-source", "/src/key.bin"),
            ("x-amz-copy-source-range", "bytes=0-"),
        ],
        &[],
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert_eq!(resp.error_code(), "InvalidArgument");

    // The closed form copies the range through a real upload.
    let upload_id = create_upload(addr, "/dst/parts.bin").await;
    let resp = request(
        addr,
        "PUT",
        &format!("/dst/parts.bin?partNumber=1&uploadId={upload_id}"),
        &[
            ("x-amz-copy-source", "/src/key.bin"),
            ("x-amz-copy-source-range", "bytes=2-5"),
        ],
        &[],
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(resp.text().contains("<CopyPartResult>"), "{}", resp.text());
    let part_etag = extract(&resp.text(), "<ETag>", "</ETag>");
    assert!(!part_etag.is_empty(), "{}", resp.text());
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let resp = request(
        addr,
        "POST",
        &format!("/dst/parts.bin?uploadId={upload_id}"),
        &[("Content-Type", "application/xml")],
        body.as_bytes(),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);
    let resp = request(addr, "GET", "/dst/parts.bin", &[], &[]).await;
    assert_eq!(resp.body, b"2345");
}

/// Create one multipart upload; the upload id.
async fn create_upload(addr: std::net::SocketAddr, path: &str) -> String {
    let resp = request(addr, "POST", &format!("{path}?uploads"), &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    extract(&resp.text(), "<UploadId>", "</UploadId>")
}

/// Upload one part; the part ETag (quoted, as the wire returns it).
async fn upload_part(
    addr: std::net::SocketAddr,
    path: &str,
    part_number: u32,
    upload_id: &str,
    data: &[u8],
) -> String {
    let resp = request(
        addr,
        "PUT",
        &format!("{path}?partNumber={part_number}&uploadId={upload_id}"),
        &[],
        data,
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK, "{}", resp.text());
    resp.header("etag").unwrap().to_string()
}
