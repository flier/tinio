#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::{
    fs::{read_dir, write},
    time::Duration,
};

use redb::ReadableDatabase;
use tokio::fs;

use super::*;
#[cfg(windows)]
use crate::testutil::link_directory;
use crate::{
    _core::{
        object,
        storage::{BucketOps, Error as StorageError, ObjectOps},
    },
    _util::testing::body,
    database::{self, StateTable},
    testutil::fs_options,
};

#[test]
fn compact_threshold_percent_is_validated() {
    let root = tempfile::tempdir().unwrap();
    let err = FsStorage::new(
        root.path(),
        FsOptions {
            compact_threshold_percent: 4,
            ..fs_options()
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidValue(_)));
    assert!(
        err.to_string().contains("compact_threshold_percent"),
        "{err}"
    );
}

#[tokio::test]
async fn new_from_db_constructs_over_an_opened_database() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let db = database::open(state.path()).unwrap().db;
    let storage = FsStorage::new_from_db(
        root.path(),
        FsOptions {
            state_dir: Some(state.path().to_path_buf()),
            ..fs_options()
        },
        db,
    )
    .unwrap();
    assert_eq!(storage.state_dir(), state.path());
    let b = bucket::name("data").unwrap();
    storage.create_bucket(&b).await.unwrap();
    assert!(storage.bucket_names("").await.unwrap() == vec![b]);
}

#[tokio::test]
async fn read_only_state_relocation_keeps_root_clean() {
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("roots").join("abc");
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            state_dir: Some(state.clone()),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket::name("data").unwrap();
    storage.create_bucket(&b).await.unwrap();
    storage
        .put_object(&b, &"a.txt".into(), body(b"hello"))
        .await
        .unwrap();
    let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
    assert_eq!(head.size, 5);
    // All private state lives under the state dir.
    assert!(state.join("meta.redb").exists());
    // The root holds only the bucket directory — no `.tinio/`, no
    // new files.
    let root_entries: Vec<String> = read_dir(root.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        root_entries,
        ["data"],
        "root must have zero private state: {root_entries:?}"
    );
    assert!(!root.path().join(".tinio").exists());
}

#[tokio::test]
async fn new_consumes_the_compact_marker() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    {
        let db = database::open(state.path()).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        {
            let mut state = StateTable::open(&mut txn).unwrap();
            state.set_compact_marker_value(true).unwrap();
        }
        txn.commit().unwrap();
    }
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            state_dir: Some(state.path().to_path_buf()),
            ..fs_options()
        },
    )
    .unwrap();
    assert!(!storage.evaluate_compact(20).await.is_err());
    drop(storage);
    // The marker is cleared after the startup compact.
    let db = database::open(state.path()).unwrap().db;
    let txn = db.begin_read().unwrap();
    let state = StateTable::open_readonly(&txn).unwrap();
    assert!(!state.compact_marker().unwrap());
}

#[test]
fn new_refuses_a_file_root() {
    // The root must be a directory: a file root canonicalizes fine but
    // is not a bucket container — `RootNotDirectory` (startup fails
    // loudly, never halfway).
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("not-a-dir");
    write(&file, b"x").unwrap();
    let err = FsStorage::new(&file, fs_options()).unwrap_err();
    assert!(
        matches!(err, Error::RootNotDirectory(_)),
        "expected RootNotDirectory, got {err:?}"
    );
}

#[tokio::test]
async fn bucket_dir_of_a_missing_bucket_answers_no_such_bucket() {
    let root = tempfile::tempdir().unwrap();
    // The symlink-probing path (follow_symlinks) lstat-probes the
    // directory — a missing bucket answers the contract error there.
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket::name("missing-bucket").unwrap();
    let err = storage.bucket_dir(&b).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "expected NoSuchBucket, got {err:?}"
    );
}

#[tokio::test]
async fn write_lock_stats_reports_the_database_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let snapshot = storage.write_lock_stats();
    assert_eq!(snapshot.count, 0);
    assert_eq!(snapshot.wait_sum_us, 0);
    assert_eq!(snapshot.total_sum_us, 0);
}

#[tokio::test]
async fn set_max_concurrent_uploads_is_enforced() {
    let root = tempfile::tempdir().unwrap();
    let mut storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = bucket::name("data").unwrap();
    storage.create_bucket(&b).await.unwrap();
    let k = object::key("k").unwrap();
    storage.set_max_concurrent_uploads(1);
    storage
        .multipart_store()
        .create(&b, &k, None, object::Tags::empty())
        .await
        .unwrap();
    let err = storage
        .multipart_store()
        .create(&b, &k, None, object::Tags::empty())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("uploads"), "{err}");
}

#[tokio::test]
async fn bucket_names_skips_root_level_files() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = bucket::name("data").unwrap();
    storage.create_bucket(&b).await.unwrap();
    fs::write(root.path().join("notes.txt"), b"not a bucket")
        .await
        .unwrap();
    let names = storage.bucket_names("").await.unwrap();
    assert_eq!(names, vec![b], "{names:?}");
}

#[tokio::test]
async fn bucket_names_filters_by_prefix() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    for name in ["alpha-1", "alpha-2", "beta-1"] {
        storage
            .create_bucket(&bucket::name(name).unwrap())
            .await
            .unwrap();
    }
    let names = storage
        .bucket_names("alpha")
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.as_ref().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, ["alpha-1", "alpha-2"], "{names:?}");
    // The empty-prefix form keeps the scanner/cleanup behavior.
    assert_eq!(storage.bucket_names("").await.unwrap().len(), 3);
}

/// The follow-policy fixture of the two bucket-name tests (R3 — one
/// copy, used by both): a real bucket, a linked bucket directory, and a
/// broken link — returns `bucket_names("")` under the policy.
/// Platform-split inside: `symlink` on unix, `link_directory` (junction)
/// on Windows.
async fn bucket_names_with_links(follow_symlinks: bool) -> Vec<String> {
    let root = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks,
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket::name("real-bucket").unwrap();
    storage.create_bucket(&b).await.unwrap();
    #[cfg(unix)]
    symlink(target.path(), root.path().join("linked")).unwrap();
    #[cfg(windows)]
    link_directory(target.path(), &root.path().join("linked"));
    #[cfg(unix)]
    symlink(
        root.path().join("does-not-exist"),
        root.path().join("broken"),
    )
    .unwrap();
    #[cfg(windows)]
    link_directory(
        &root.path().join("does-not-exist"),
        &root.path().join("broken"),
    );
    storage
        .bucket_names("")
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.as_ref().to_string())
        .collect()
}

#[cfg(unix)]
#[tokio::test]
async fn bucket_names_follows_symlinked_bucket_dirs() {
    // A symlinked bucket directory is a bucket only when following is
    // enabled (the follow policy, one source of truth); a broken link is
    // never a bucket.
    assert_eq!(
        bucket_names_with_links(true).await,
        vec!["linked".to_string(), "real-bucket".to_string()]
    );
    assert_eq!(
        bucket_names_with_links(false).await,
        vec!["real-bucket".to_string()]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn bucket_dir_resolves_dangling_and_looping_links() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    symlink(
        root.path().join("no-such-target"),
        root.path().join("dangle"),
    )
    .unwrap();
    symlink(root.path().join("loop"), root.path().join("loop")).unwrap();

    let dangle = bucket::name("dangle").unwrap();
    let err = storage.bucket_dir(&dangle).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "dangling link: {err:?}"
    );

    let looped = bucket::name("loop").unwrap();
    let err = storage.bucket_dir(&looped).await.unwrap_err();
    assert!(matches!(err, Error::Io(_)), "link loop: {err:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn bucket_names_skips_non_utf8_directory_names() {
    // A top-level directory whose name is not valid UTF-8 cannot be a
    // bucket name (the contract is UTF-8) — skipped, never a panic.
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = bucket::name("data").unwrap();
    storage.create_bucket(&b).await.unwrap();
    let mut name = b"bad-".to_vec();
    name.push(0xff);
    fs::create_dir(root.path().join(OsStr::from_bytes(&name)))
        .await
        .unwrap();
    let names = storage.bucket_names("").await.unwrap();
    assert_eq!(names, vec![b], "{names:?}");
}

#[cfg(windows)]
#[tokio::test]
async fn bucket_dir_of_a_dangling_junction_is_no_such_bucket() {
    // A bucket dir that is a DANGLING junction resolves to no target:
    // `bucket_dir` answers NoSuchBucket (the bucket is the target, and
    // the target is gone — F13).
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    link_directory(
        &root.path().join("no-such-target"),
        &root.path().join("data"),
    );
    let b = bucket::name("data").unwrap();
    let err = storage.bucket_dir(&b).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "{err:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn bucket_names_follows_junction_bucket_dirs() {
    // A junction bucket directory is a bucket only when following is
    // enabled (the follow policy, one source of truth); a broken link
    // is never a bucket.
    assert_eq!(
        bucket_names_with_links(true).await,
        vec!["linked".to_string(), "real-bucket".to_string()]
    );
    assert_eq!(
        bucket_names_with_links(false).await,
        vec!["real-bucket".to_string()]
    );
}

#[tokio::test]
async fn lock_bucket_mutations_warns_after_the_threshold() {
    // A waiter past the warn threshold (1 s) still acquires the lock —
    // the wait is warned (D-E), never silently absorbed and never
    // failed.
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = bucket::name("data").unwrap();
    let held = storage.lock_bucket_mutations(&b).await;
    let task = tokio::spawn({
        let storage = storage.clone();
        let b = b.clone();
        async move {
            let guard = storage.lock_bucket_mutations(&b).await;
            drop(guard);
        }
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    drop(held);
    task.await.unwrap();
}

#[tokio::test]
async fn ensure_bucket_refuses_a_file_in_place_of_a_bucket() {
    // A bucket path that exists as a FILE is no bucket (NoSuchBucket,
    // never an IO error).
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = bucket::name("data").unwrap();
    fs::write(root.path().join("data"), b"not-a-dir")
        .await
        .unwrap();
    let err = storage.ensure_bucket(&b).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "{err:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn resolve_object_file_rejects_a_junction_leaf_when_following_is_disabled() {
    // A key whose path resolves through a junction is refused with
    // AccessDenied when following is disabled (the documented 403 — a
    // link inside the bucket never escapes the root, s3-surface.md).
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = bucket::name("data").unwrap();
    storage.create_bucket(&b).await.unwrap();
    fs::write(outside.path().join("x.txt"), b"s").await.unwrap();
    link_directory(outside.path(), &root.path().join("data/subdir"));
    let k = object::key("subdir/x.txt").unwrap();
    let err = storage.resolve_object_file(&b, &k).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::AccessDenied(_))),
        "{err:?}"
    );
}
