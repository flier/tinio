//! S3 error-code behavior over real HTTP (task T024, SC-004).
//!
//! NoSuchBucket, NoSuchKey, InvalidBucketName, BucketAlreadyOwnedByYou,
//! BucketNotEmpty, NotImplemented (runtime capability toggles, FR-021),
//! and traversal rejection with no filesystem access outside the served
//! root (FR-006). Every case asserts the S3 XML `<Code>` in the response
//! body, not just the HTTP status. The in-memory backend is the
//! reference; the traversal case runs against the fs backend to prove no
//! FS access happens.

mod common;

use http::StatusCode;

use tinio_server::Capabilities;

use common::{Server, request};

/// The default toggles.
fn caps() -> Capabilities {
    Capabilities::default()
}

#[tokio::test]
async fn no_such_bucket() {
    let server = Server::mem(caps()).await;

    // GET on a missing bucket → NoSuchBucket.
    let resp = request(server.addr(), "GET", "/missing", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchBucket");

    // PUT into a missing bucket → NoSuchBucket.
    let resp = request(server.addr(), "PUT", "/missing/a.txt", &[], b"x").await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchBucket");
}

#[tokio::test]
async fn no_such_key() {
    let server = Server::mem(caps()).await;
    request(server.addr(), "PUT", "/data", &[], &[]).await;

    let resp = request(server.addr(), "GET", "/data/missing.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchKey");

    // HEAD on a missing key → 404 as well (no body on HEAD).
    let resp = request(server.addr(), "HEAD", "/data/missing.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invalid_bucket_name() {
    let server = Server::mem(caps()).await;
    let resp = request(server.addr(), "PUT", "/Bad_Name", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert_eq!(resp.error_code(), "InvalidBucketName");
}

#[tokio::test]
async fn bucket_already_exists_and_not_empty() {
    let server = Server::mem(caps()).await;
    request(server.addr(), "PUT", "/data", &[], &[]).await;

    // Duplicate create → BucketAlreadyOwnedByYou (AWS/MinIO semantics).
    let resp = request(server.addr(), "PUT", "/data", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::CONFLICT);
    assert_eq!(resp.error_code(), "BucketAlreadyOwnedByYou");

    // Populate, then delete → BucketNotEmpty.
    request(server.addr(), "PUT", "/data/a.txt", &[], b"x").await;
    let resp = request(server.addr(), "DELETE", "/data", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::CONFLICT);
    assert_eq!(resp.error_code(), "BucketNotEmpty");

    // Deleting the object frees the bucket.
    request(server.addr(), "DELETE", "/data/a.txt", &[], &[]).await;
    let resp = request(server.addr(), "DELETE", "/data", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn disabled_capabilities_answer_not_implemented() {
    let caps = Capabilities {
        multipart: false,
        copy_object: false,
        list_objects_v1: false,
        list_objects_v2: false,
        delete_objects: false,
    };
    let server = Server::mem(caps).await;
    request(server.addr(), "PUT", "/data", &[], &[]).await;

    let resp = request(server.addr(), "GET", "/data?list-type=2", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(resp.error_code(), "NotImplemented");

    let resp = request(server.addr(), "GET", "/data", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(resp.error_code(), "NotImplemented");

    let resp = request(server.addr(), "POST", "/data/big.bin?uploads", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(resp.error_code(), "NotImplemented");
}

#[tokio::test]
async fn unsupported_operations_answer_not_implemented() {
    let server = Server::mem(caps()).await;
    request(server.addr(), "PUT", "/data", &[], &[]).await;
    // GetBucketPolicy is outside the v1 surface.
    let resp = request(server.addr(), "GET", "/data?policy", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(resp.error_code(), "NotImplemented");
}

#[tokio::test]
async fn traversal_keys_rejected_without_fs_access() {
    // The served root sits inside a parent dir so writes escaping the root
    // are observable.
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let server = Server::fs_at(&root, caps()).await;
    request(server.addr(), "PUT", "/data", &[], &[]).await;

    for key in ["../evil.txt", "..%2Fevil2.txt", "a%2F..%2Fb"] {
        let resp = request(server.addr(), "PUT", &format!("/data/{key}"), &[], b"x").await;
        assert_eq!(resp.status, StatusCode::BAD_REQUEST, "key {key}");
        assert!(
            !resp.error_code().is_empty(),
            "key {key} must produce a coded error"
        );
    }
    // Absolute-path key: `/data//abs.txt` → key `/abs.txt`.
    let resp = request(server.addr(), "PUT", "/data//abs.txt", &[], b"x").await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert!(!resp.error_code().is_empty());

    // Proof: nothing was created outside the served root — the parent
    // still holds only the root, and the root only the state dir and the
    // bucket directory.
    let base_entries = sorted_entries(base.path());
    assert_eq!(base_entries, ["root"]);
    let root_entries = sorted_entries(&root);
    assert_eq!(root_entries, [".tinio", "data"]);
}

/// The sorted entry names of a directory.
fn sorted_entries(dir: &std::path::Path) -> Vec<String> {
    let mut entries: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    entries
}
