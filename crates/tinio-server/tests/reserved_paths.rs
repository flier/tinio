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
    // The bucket is created before the server starts (redb's file lock
    // allows a single open per root — SC-005 — so the test handle must be
    // dropped before the server opens the same state database).
    let root = tempfile::tempdir().unwrap();
    {
        let storage = FsStorage::new(root.path(), FsOptions::default()).unwrap();
        storage.create_bucket(&"data".into()).await.unwrap();
    }
    let server = Server::fs_at(root.path(), Capabilities::default()).await;
    let addr = server.addr();

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
    // `.tinio` segment is reserved at any depth. (The outer bucket is
    // created before the server starts — redb's file lock allows a single
    // open per root, SC-005.)
    let outer_root = tempfile::tempdir().unwrap();
    {
        let storage = FsStorage::new(outer_root.path(), FsOptions::default()).unwrap();
        storage.create_bucket(&"inner-root".into()).await.unwrap();
    }
    let outer_server = Server::fs_at(outer_root.path(), Capabilities::default()).await;
    let addr = outer_server.addr();

    // The inner root's reserved state (created by its own server).
    let inner_root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(inner_root.path().join(".tinio")).unwrap();
    std::fs::write(inner_root.path().join(".tinio/state"), b"secret").unwrap();
    // Symlink-free copy: place the inner root inside the outer bucket.
    let inner_bucket = outer_root.path().join("inner-root");
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
