//! State-layout integration tests (meta-redb-spec acceptance 2 and 7).
//!
//! The state dir holds only `meta.redb`, `tmp/`, and `multipart/` part
//! files — no `buckets.json`, no `upload.json`, no `meta/objects/` fan-out,
//! and no separate lock file (redb 4.2 takes the file lock inside
//! `meta.redb` itself — meta-redb-spec §4). Deleting `meta.redb` is safe:
//! the metadata is derivable and recomputed on demand (self-healing
//! restart).

use std::{fs::read_dir, path::Path};

use tinio_core::{
    bucket,
    multipart::part_number,
    storage::{BucketOps, ListPartsParams, MultipartOps, ObjectOps},
};
use tinio_fs::{FsOptions, FsStorage, testing};
use tinio_util::testing::{body, read_body};
use tokio::fs;

/// The shared offline defaults plus the test's state-dir override (F33).
fn fs_options(state_dir: &Path) -> FsOptions {
    FsOptions {
        state_dir: Some(state_dir.to_path_buf()),
        ..testing::fs_options()
    }
}

/// The entries of `dir` as a sorted set.
fn entries(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    out.sort();
    out
}

/// One full lifecycle over an explicit state dir; then assert the layout.
#[tokio::test]
async fn state_dir_holds_only_redb_tmp_and_multipart() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options(state.path())).unwrap();

    let b = bucket_name("data");
    storage.create_bucket(&b).await.unwrap();
    storage
        .put_object(&b, &"hello.txt".into(), body(b"hello"))
        .await
        .unwrap();

    // Multipart lifecycle leaves part files only.
    let key = "big.bin".into();
    let upload = storage.create_multipart_upload(&b, &key).await.unwrap();
    storage
        .upload_part(
            &b,
            &key,
            &upload.upload_id,
            part_number(1).unwrap(),
            body(b"x"),
        )
        .await
        .unwrap();
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: key.clone(),
            upload_id: upload.upload_id.clone(),
            max_parts: 10,
            part_number_marker: None,
        })
        .await
        .unwrap();
    assert_eq!(listing.parts.len(), 1);

    // The state dir: meta.redb and the multipart tree only (tmp/ appears
    // on demand when the atomic writer stages a body).
    let mut allowed = vec!["meta.redb", "multipart"];
    if state.path().join("tmp").exists() {
        allowed.push("tmp");
    }
    allowed.sort();
    assert_eq!(entries(state.path()), allowed);

    // No legacy layout anywhere.
    assert!(!state.path().join("buckets.json").exists());
    assert!(!state.path().join("meta").exists());
    let upload_dir = state.path().join("multipart/data").join(&upload.upload_id);
    let upload_entries = entries(&upload_dir);
    assert_eq!(
        upload_entries,
        ["part-1"],
        "no sidecars: {upload_entries:?}"
    );
}

/// Deleting `meta.redb` must not break the store: entries are recomputed
/// on demand (the metadata is derivable — meta-redb-spec §3).
#[tokio::test]
async fn deleting_meta_redb_self_heals() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let state_dir = state.path().to_path_buf();
    let storage = FsStorage::new(root.path(), fs_options(&state_dir)).unwrap();
    let b = bucket_name("data");
    storage.create_bucket(&b).await.unwrap();
    storage
        .put_object(&b, &"a.txt".into(), body(b"hello"))
        .await
        .unwrap();
    let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
    assert_eq!(head.size, 5);
    drop(storage);

    // Wipe the database (simulating corruption beyond repair), then reopen.
    fs::remove_file(state_dir.join("meta.redb")).await.unwrap();
    let storage = FsStorage::new(root.path(), fs_options(&state_dir)).unwrap();
    // The object is still served — the ETag is recomputed on demand.
    let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
    assert_eq!(head.size, 5);
    let content = storage.get_object(&b, &"a.txt".into(), None).await.unwrap();
    let body = read_body(content.body).await.unwrap();
    assert_eq!(body, b"hello");
}

fn bucket_name(name: &str) -> bucket::Name {
    name.into()
}
