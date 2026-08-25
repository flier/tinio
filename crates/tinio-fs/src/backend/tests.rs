use super::*;
use crate::database::{self, StateTable};
use crate::testutil::rt;
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
            ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!storage.evaluate_compact(20).is_err());
        drop(storage);
        // The marker is cleared after the startup compact.
        let db = database::open(state.path()).unwrap().db;
        let txn = db.begin_read().unwrap();
        let state = StateTable::open_readonly(&txn).unwrap();
        assert!(!state.compact_marker().unwrap());
    });
}
