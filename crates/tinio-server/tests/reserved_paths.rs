//! Reserved-path behavior over the data plane (task T026, FR-020).
//!
//! Any-depth `.tinio` segments: write → AccessDenied, read → NoSuchKey,
//! listings skip. The nested-root scenario: an inner root's state is never
//! served by an outer server. A distinct file from T025 so the security
//! surface is independently reviewable.

mod common;

use http::StatusCode;

use tinio_core::storage::BucketOps;
use tinio_fs::{FsOptions, FsStorage};
use tinio_server::Capabilities;

use common::{Server, request};

#[tokio::test]
async fn tinio_writes_denied_reads_missing() {
    let server = Server::fs(Capabilities::default()).await;
    let addr = server.addr();
    // Create the bucket through the contract (the raw harness has no
    // bucket-create shortcut).
    let storage = FsStorage::new(server.root(), FsOptions::default()).unwrap();
    storage.create_bucket(&"data".into()).await.unwrap();

    for key in [".tinio", ".tinio/state", "a/.tinio/x", "a/b/.tinio/c"] {
        // Write → AccessDenied.
        let resp = request(addr, "PUT", &format!("/data/{key}"), &[], b"x").await;
        assert_eq!(resp.status, StatusCode::FORBIDDEN, "key {key}");
        assert_eq!(resp.error_code(), "AccessDenied", "key {key}");

        // Read → NoSuchKey.
        let resp = request(addr, "GET", &format!("/data/{key}"), &[], &[]).await;
        assert_eq!(resp.status, StatusCode::NOT_FOUND, "key {key}");
        assert_eq!(resp.error_code(), "NoSuchKey", "key {key}");
    }

    // Listings skip reserved entries.
    let resp = request(addr, "GET", "/data?list-type=2", &[], &[]).await;
    assert!(!resp.text().contains(".tinio"));
    assert_eq!(resp.text().matches("<Key>").count(), 0);
}

#[tokio::test]
async fn nested_root_state_never_served() {
    // An outer server's root contains an inner server's root as a bucket.
    // The outer server must never serve the inner root's state — the
    // `.tinio` segment is reserved at any depth.
    let outer_server = Server::fs(Capabilities::default()).await;
    let addr = outer_server.addr();
    let outer = FsStorage::new(outer_server.root(), FsOptions::default()).unwrap();
    outer.create_bucket(&"inner-root".into()).await.unwrap();

    // The inner root's reserved state (created by its own server).
    let inner_root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(inner_root.path().join(".tinio")).unwrap();
    std::fs::write(inner_root.path().join(".tinio/state"), b"secret").unwrap();
    // Symlink-free copy: place the inner root inside the outer bucket.
    let inner_bucket = outer_server.root().join("inner-root");
    copy_dir(inner_root.path(), &inner_bucket);

    // Reading the inner state through the outer server → NoSuchKey.
    let resp = request(addr, "GET", "/inner-root/.tinio/state", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    assert_eq!(resp.error_code(), "NoSuchKey");

    // Writing it → AccessDenied, and the inner state is left untouched.
    let resp = request(addr, "PUT", "/inner-root/.tinio/state", &[], b"x").await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
    assert_eq!(resp.error_code(), "AccessDenied");
    assert_eq!(
        std::fs::read(inner_bucket.join(".tinio/state")).unwrap(),
        b"secret"
    );

    // Listings never expose it.
    let resp = request(addr, "GET", "/inner-root?list-type=2", &[], &[]).await;
    assert!(!resp.text().contains(".tinio"));

    // Non-reserved objects of the inner root are served normally (it is a
    // regular bucket of the outer server).
    std::fs::write(inner_bucket.join("public.txt"), b"public").unwrap();
    let resp = request(addr, "GET", "/inner-root/public.txt", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body, b"public");
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
