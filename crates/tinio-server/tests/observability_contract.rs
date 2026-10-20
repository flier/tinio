//! The request spine's span contract, asserted from a dedicated binary: the lib test binary
//! runs the other server tests in parallel, and a callsite another thread registers while no
//! subscriber is installed is cached `never` for the process — a thread-scoped capture cannot
//! reliably turn it back on. A global recorder in a binary with nothing else to race is the
//! deterministic shape.
//!
//! The test drives the real plane: a mem backend behind `DataPlane`, bound to an ephemeral
//! loopback port and reached with raw unsigned HTTP/1.1 requests. It asserts the tree the plan
//! fixes — request span root, one `tinio.s3.operation` span below it, its storage span under
//! that — by name, target, level and the recorded `op`, never by log text.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: the plane, request and recording fixtures state their own invariants (docs/style.md §Panics)"
)]

mod common;

use tinio_util::testing::{http::exchange, spans};
use tracing::Level;

#[tokio::test]
async fn the_request_spine_nests_request_operation_and_storage_spans() {
    spans::install();
    let plane = common::spawn_plane().await;
    let addr = plane.addr;

    let put = exchange("localhost", addr, "PUT", "/data/key", &[], b"hello", 0)
        .await
        .status;
    assert_eq!(put, 200, "the PUT stores the object");
    let get = exchange("localhost", addr, "GET", "/data/key", &[], b"", 0)
        .await
        .status;
    assert_eq!(get, 200, "the GET reads it back");
    plane.stop().await;

    // The recorder is global; the spans of any other thread are not this request's.
    // `#[tokio::test]` runs a current-thread runtime, so the plane's spawned connection task
    // polls on this thread and the filter keeps the whole request.
    let recorded: Vec<spans::RecordedSpan> = spans::spans_on_this_thread();

    let requests: Vec<&spans::RecordedSpan> = recorded
        .iter()
        .filter(|span| span.name == "tinio.http.request")
        .collect();
    assert_eq!(
        requests.len(),
        2,
        "one request span per request: {recorded:#?}"
    );

    for (method, op) in [("PUT", "PutObject"), ("GET", "GetObject")] {
        let request = requests
            .iter()
            .copied()
            .find(|span| span.field("method") == Some(method))
            .unwrap_or_else(|| panic!("the {method} request span: {recorded:#?}"));

        // Layer one: the admission span is the root, at the request target and level.
        assert_eq!(request.target, "tinio::http");
        assert_eq!(request.level, Level::INFO);
        assert_eq!(request.parent, None, "the request span is the root");
        assert_eq!(request.field("path"), Some("/data/key"));
        assert!(request.field("request_id").is_some(), "{request:#?}");

        // Layer two: exactly one operation span in the request's subtree, with a non-empty
        // `op`. The s3s protocol layer opens spans of its own (`s3s::service` `call`), so
        // the operation sits one level below the request span, not directly under it.
        let operations: Vec<&spans::RecordedSpan> = spans::descendants(&recorded, request.id)
            .into_iter()
            .filter(|span| span.name == "tinio.s3.operation")
            .collect();
        assert_eq!(
            operations.len(),
            1,
            "one operation span per request, never a duplicate: {recorded:#?}"
        );
        let operation = operations[0];
        assert_eq!(operation.target, "tinio::s3");
        assert_eq!(operation.level, Level::INFO);
        assert_eq!(
            operation.field("op"),
            Some(op),
            "the op is recorded: {operation:#?}"
        );
        assert!(!op.is_empty());

        // Layer three: the T0 storage unit is the operation's own child, at `info` — the
        // plan's tier rule for the layer that owns the unit.
        let storage_roots: Vec<&spans::RecordedSpan> = spans::children(&recorded, operation.id)
            .into_iter()
            .filter(|span| span.target == "tinio::storage")
            .collect();
        assert!(
            !storage_roots.is_empty(),
            "the operation opens a storage span: {recorded:#?}"
        );
        for span in &storage_roots {
            assert_eq!(span.target, "tinio::storage");
            assert_eq!(span.level, Level::INFO, "a T0 storage unit: {span:#?}");
        }

        // Below it the storage layer keeps the tier ladder (T0/T1/T2), and a T1 child is
        // visible: nothing in the tree is raised above its criticality.
        let storage_below: Vec<&spans::RecordedSpan> = spans::descendants(&recorded, operation.id)
            .into_iter()
            .filter(|span| span.target == "tinio::storage")
            .collect();
        let mut t1_seen = false;
        for span in storage_below {
            match span.level {
                Level::INFO => {}
                Level::DEBUG => t1_seen = true,
                Level::TRACE => {}
                level => panic!("storage spans use the tier levels, got {level}: {span:#?}"),
            }
        }
        assert!(t1_seen, "a T1 storage child is visible: {recorded:#?}");
    }
}

#[tokio::test]
async fn an_unknown_access_key_opens_the_auth_lookup_span() {
    spans::install();
    let plane = common::spawn_plane_with_auth("AKID", "SECRET").await;
    let addr = plane.addr;

    // A well-formed SigV4 header naming an unknown key reaches the credential
    // lookup before signature verification, which is the security decision the
    // auth layer owns. The timestamp must be current: s3s rejects a skewed
    // request before the lookup.
    let now = time::OffsetDateTime::now_utc();
    let date = format!(
        "{:04}{:02}{:02}",
        now.year(),
        u8::from(now.month()),
        now.day()
    );
    let stamp = format!(
        "{date}T{:02}{:02}{:02}Z",
        now.hour(),
        now.minute(),
        now.second()
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential=WRONG/{date}/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0000000000000000000000000000000000000000000000000000000000000000"
    );
    let status = exchange(
        "localhost",
        addr,
        "GET",
        "/data/key",
        &[
            ("X-Amz-Date", stamp.as_str()),
            (
                "X-Amz-Content-Sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            ("Authorization", authorization.as_str()),
        ],
        b"",
        0,
    )
    .await
    .status;
    assert_eq!(status, 403, "an unknown key is denied");
    plane.stop().await;

    let recorded: Vec<spans::RecordedSpan> = spans::spans_on_this_thread();
    let lookups: Vec<&spans::RecordedSpan> = recorded
        .iter()
        .filter(|span| span.name == "get_secret_key")
        .collect();
    assert_eq!(
        lookups.len(),
        1,
        "one lookup span per request: {recorded:#?}"
    );
    let lookup = lookups[0];
    assert_eq!(lookup.target, "tinio::auth");
    assert_eq!(lookup.level, Level::INFO);
    assert_eq!(lookup.field("result"), Some("unknown"));
    assert!(
        lookup.field("access_key").is_none(),
        "the key itself never enters the span: {lookup:#?}"
    );
}
