use super::*;
use crate::database::{self, StateTable};
use crate::testutil::{fs_options, rt};
use redb::ReadableDatabase;
use tinio_core::storage::{BucketOps, ObjectOps};
use tinio_util::testing::body;

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

#[test]
fn new_from_db_constructs_over_an_opened_database() {
    // The orchestration path (G1): open → (compact) → `new_from_db`.
    rt(async {
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
        assert!(storage.bucket_names().await.unwrap() == vec![b]);
    });
}

#[test]
fn read_only_state_relocation_keeps_root_clean() {
    // G5/G6: read-only mode relocates the private state to a home
    // state dir (the fs-level contract — `state_dir` override); the
    // storage root itself gets zero private writes.
    rt(async {
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
        let root_entries: Vec<String> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            root_entries,
            ["data"],
            "root must have zero private state: {root_entries:?}"
        );
        assert!(!root.path().join(".tinio").exists());
    });
}

#[test]
fn new_consumes_the_compact_marker() {
    // The runtime may set the marker; `FsStorage::new` (the startup
    // path) compacts while the handle is exclusive and clears it.
    rt(async {
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
    });
}

#[test]
fn new_refuses_a_file_root() {
    // The root must be a directory: a file root canonicalizes fine but
    // is not a bucket container — `RootNotDirectory` (startup fails
    // loudly, never halfway).
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    let err = FsStorage::new(&file, fs_options()).unwrap_err();
    assert!(
        matches!(err, Error::RootNotDirectory(_)),
        "expected RootNotDirectory, got {err:?}"
    );
}

#[test]
fn bucket_dir_of_a_missing_bucket_answers_no_such_bucket() {
    // `bucket_dir` is the single source of truth for a bucket's
    // directory; a missing bucket must answer the contract error, not a
    // raw IO error (callers map it to `NoSuchBucket` on the wire).
    rt(async {
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
            matches!(
                err,
                Error::Storage(tinio_core::storage::Error::NoSuchBucket(_))
            ),
            "expected NoSuchBucket, got {err:?}"
        );
    });
}

#[test]
fn write_lock_stats_reports_the_database_snapshot() {
    // The `/metrics` scrape path reads the write-transaction snapshot —
    // the accessor must never error and must answer the defaults of a
    // fresh database.
    rt(async {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let snapshot = storage.write_lock_stats();
        assert_eq!(snapshot.count, 0);
        assert_eq!(snapshot.wait_sum_us, 0);
        assert_eq!(snapshot.total_sum_us, 0);
    });
}

#[test]
fn set_max_concurrent_uploads_is_enforced() {
    // The `[s3] max_concurrent_uploads` cap is applied through the
    // backend: the first upload fits, the second is refused.
    rt(async {
        let root = tempfile::tempdir().unwrap();
        let mut storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("k").unwrap();
        storage.set_max_concurrent_uploads(1);
        storage.multipart_store().create(&b, &k).await.unwrap();
        let err = storage.multipart_store().create(&b, &k).await.unwrap_err();
        assert!(err.to_string().contains("uploads"), "{err}");
    });
}

#[test]
fn bucket_names_skips_root_level_files() {
    // The bucket walk lists top-level DIRECTORIES only — a stray file at
    // the root is not a bucket.
    rt(async {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        tokio::fs::write(root.path().join("notes.txt"), b"not a bucket")
            .await
            .unwrap();
        let names = storage.bucket_names().await.unwrap();
        assert_eq!(names, vec![b], "{names:?}");
    });
}

#[cfg(unix)]
#[test]
fn bucket_names_follows_symlinked_bucket_dirs() {
    // A symlinked bucket directory is a bucket only when following is
    // enabled (the follow policy, one source of truth); a broken link is
    // never a bucket.
    let list = |follow_symlinks: bool| {
        rt(async move {
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
            std::os::unix::fs::symlink(target.path(), root.path().join("linked")).unwrap();
            std::os::unix::fs::symlink(
                root.path().join("does-not-exist"),
                root.path().join("broken"),
            )
            .unwrap();
            storage
                .bucket_names()
                .await
                .unwrap()
                .into_iter()
                .map(|n| n.as_ref().to_string())
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(list(true), vec!["linked".to_string(), "real-bucket".to_string()]);
    assert_eq!(list(false), vec!["real-bucket".to_string()]);
}

#[cfg(unix)]
#[test]
fn bucket_dir_resolves_dangling_and_looping_links() {
    // A dangling bucket link has no target — NoSuchBucket; a looping link
    // fails canonicalization with a filesystem error (not a 500 from a
    // lexical fallback).
    rt(async {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                ..fs_options()
            },
        )
        .unwrap();
        std::os::unix::fs::symlink(
            root.path().join("no-such-target"),
            root.path().join("dangle"),
        )
        .unwrap();
        std::os::unix::fs::symlink(root.path().join("loop"), root.path().join("loop")).unwrap();

        let dangle = bucket::name("dangle").unwrap();
        let err = storage.bucket_dir(&dangle).await.unwrap_err();
        assert!(
            matches!(
                err,
                Error::Storage(tinio_core::storage::Error::NoSuchBucket(_))
            ),
            "dangling link: {err:?}"
        );

        let looped = bucket::name("loop").unwrap();
        let err = storage.bucket_dir(&looped).await.unwrap_err();
        assert!(matches!(err, Error::Io(_)), "link loop: {err:?}");
    });
}
