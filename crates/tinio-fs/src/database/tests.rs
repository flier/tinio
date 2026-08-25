use redb::{Database, ReadableDatabase};
use std::ops::DerefMut;

use crate::testutil::rt;
use tinio_core::bucket;
use tinio_util::testing::assert_send_sync;

use super::{
    compact::{Compaction, Stats, compact_if_needed, needs_compact},
    error::Error,
    handle::Handle,
    open::{Integrity, check_integrity, open},
    tables::{BucketsTable, ObjectMetaTable, PartsTable, StateTable, UploadsTable},
};

fn open_db() -> (tempfile::TempDir, Database, Stats) {
    let state = tempfile::tempdir().unwrap();
    let opened = open(state.path()).unwrap();
    (state, opened.db, opened.stats)
}

#[test]
fn redb_errors_wrap_per_kind() {
    let err = Error::from(redb::TableError::TableDoesNotExist("t".into()));
    assert!(
        matches!(err, Error::Table(redb::TableError::TableDoesNotExist(_))),
        "{err:?}"
    );
    let err = Error::from(redb::TransactionError::Storage(
        redb::StorageError::ValueTooLarge(1),
    ));
    assert!(matches!(err, Error::Transaction(_)), "{err:?}");
    assert!(
        Error::from(redb::StorageError::ValueTooLarge(1))
            .to_string()
            .contains("storage error")
    );
}

#[test]
fn errors_are_send_sync_and_static() {
    assert_send_sync::<Error>();
}

#[test]
fn open_creates_database_and_all_tables() {
    rt(async {
        let (state, db, _) = open_db();
        assert!(state.path().join("meta.redb").exists());
        // Every table is openable (read transactions refuse missing
        // tables — the open-time write transaction created them).
        let txn = db.begin_read().unwrap();
        ObjectMetaTable::open_readonly(&txn).unwrap();
        BucketsTable::open_readonly(&txn).unwrap();
        UploadsTable::open_readonly(&txn).unwrap();
        PartsTable::open_readonly(&txn).unwrap();
        StateTable::open_readonly(&txn).unwrap();
    });
}

#[test]
fn open_twice_is_idempotent() {
    rt(async {
        let (state, _, _) = open_db();
        // A second open on the same file must succeed.
        let db = open(state.path()).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        let version = StateTable::open(&mut txn)
            .unwrap()
            .ensure_version(state.path())
            .unwrap();
        assert_eq!(version, 1);
    });
}

#[test]
fn version_mismatch_is_unsupported_version() {
    rt(async {
        let (state, db, _) = open_db();
        // Bump the version out-of-band.
        let mut txn = db.begin_write().unwrap();
        StateTable::open(&mut txn)
            .unwrap()
            .insert("version", 9)
            .unwrap();
        txn.commit().unwrap();
        // The first handle holds the redb file lock — drop it before
        // re-opening.
        drop(db);
        let err = open(state.path()).unwrap_err();
        assert!(
            matches!(
                err,
                Error::UnsupportedVersion {
                    found: 9,
                    expected: 1,
                    ..
                }
            ),
            "{err:?}"
        );
    });
}

#[test]
fn open_fresh_state_dir_creates_dir() {
    rt(async {
        let base = tempfile::tempdir().unwrap();
        let nested = base.path().join("a/b");
        let db = open(&nested).unwrap().db;
        assert!(nested.join("meta.redb").exists());
        drop(db);
    });
}

#[test]
fn corrupt_file_is_an_error() {
    rt(async {
        let (state, _, _) = open_db();
        std::fs::write(state.path().join("meta.redb"), b"not a redb file").unwrap();
        let err = open(state.path()).unwrap_err();
        assert!(matches!(err, Error::Open(_)), "{err:?}");
    });
}

#[test]
fn check_integrity_reports_healthy() {
    rt(async {
        let (state, db, _) = open_db();
        // The integrity check opens the file exclusively — drop the
        // test handle first.
        drop(db);
        assert!(matches!(
            check_integrity(state.path()).unwrap(),
            Integrity::Healthy
        ));
    });
}

#[test]
fn needs_compact_threshold_and_floor() {
    // Below the 64 MiB floor: never compact, whatever the ratio.
    let floor = 64 * 1024 * 1024;
    let small = Stats {
        allocated_bytes: floor - 1,
        fragmented_bytes: floor - 1,
    };
    assert!(!needs_compact(&small, 5));
    // At/above the floor: the ratio decides ("≥ threshold%").
    let big = Stats {
        allocated_bytes: 100 * 1024 * 1024,
        fragmented_bytes: 25 * 1024 * 1024, // 25%
    };
    assert!(needs_compact(&big, 20));
    assert!(needs_compact(&big, 25));
    assert!(!needs_compact(&big, 26));
    let clean = Stats {
        allocated_bytes: 100 * 1024 * 1024,
        fragmented_bytes: 19 * 1024 * 1024, // 19%
    };
    assert!(!needs_compact(&clean, 20));
}

#[test]
fn compact_marker_round_trip() {
    rt(async {
        let (state, db, _) = open_db();
        let handle = Handle::new(db);
        assert!(!handle.compact_needed().unwrap());
        handle.mark_compact_needed(true).unwrap();
        assert!(handle.compact_needed().unwrap());
        handle.mark_compact_needed(false).unwrap();
        assert!(!handle.compact_needed().unwrap());
        drop(state);
    });
}

#[test]
fn compact_if_needed_shrinks_and_clears_marker() {
    rt(async {
        let state = tempfile::tempdir().unwrap();
        let mut db = open(state.path()).unwrap().db;
        // Churn: write then delete many entries — COW leaves the
        // file grown with dead pages.
        {
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = ObjectMetaTable::open(&mut txn).unwrap();
                for i in 0..50_000u32 {
                    let key = format!("key-{i}");
                    table
                        .insert(
                            ("data", key.as_str()),
                            (key.as_str(), u64::from(i), u64::from(i), 0),
                        )
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        {
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = ObjectMetaTable::open(&mut txn).unwrap();
                for i in 0..50_000u32 {
                    let key = format!("key-{i}");
                    // Raw remove: domain `remove` validates keys; this
                    // churn fixture only needs the B+tree delete path.
                    table.deref_mut().remove(("data", key.as_str())).unwrap();
                }
            }
            txn.commit().unwrap();
        }
        let grown = std::fs::metadata(state.path().join("meta.redb"))
            .unwrap()
            .len();
        // Set the marker (the marker path bypasses the 64 MiB floor —
        // the runtime evaluation would have set it once the ratio
        // crossed the threshold).
        {
            let mut txn = db.begin_write().unwrap();
            {
                let mut state_table = StateTable::open(&mut txn).unwrap();
                state_table.set_compact_marker_value(true).unwrap();
            }
            txn.commit().unwrap();
        }
        let report = compact_if_needed(
            &mut db,
            true,
            Stats {
                allocated_bytes: 0,
                fragmented_bytes: 0,
            },
            20,
        )
        .unwrap();
        assert!(matches!(report, Compaction::Compacted));
        let after = std::fs::metadata(state.path().join("meta.redb"))
            .unwrap()
            .len();
        assert!(
            after < grown,
            "compact must shrink the file: {after} >= {grown}"
        );
        // The marker is cleared.
        let txn = db.begin_read().unwrap();
        let state_table = StateTable::open_readonly(&txn).unwrap();
        assert!(!state_table.compact_marker().unwrap());
        drop(state);
    });
}

#[test]
fn compact_if_needed_skips_when_clean() {
    rt(async {
        let (state, mut db, stats) = open_db();
        let report = compact_if_needed(&mut db, false, stats, 20).unwrap();
        assert!(matches!(report, Compaction::Skipped));
        drop(state);
    });
}

#[test]
fn check_integrity_fails_on_garbage() {
    rt(async {
        let (state, db, _) = open_db();
        drop(db);
        std::fs::write(state.path().join("meta.redb"), b"junk").unwrap();
        // Unreadable file: the open itself fails (doctor reports the
        // database as unrecoverable — metadata is derivable).
        assert!(check_integrity(state.path()).is_err());
    });
}

#[test]
fn handle_read_write_round_trip() {
    rt(async {
        let (state, db, _) = open_db();
        let handle = Handle::new(db);
        let name = bucket::name("alpha").unwrap();
        let created = std::time::UNIX_EPOCH + std::time::Duration::from_nanos(42);
        handle
            .write(|txn| {
                BucketsTable::open(txn)?.put(&name, created)?;
                Ok(())
            })
            .unwrap();
        let got = handle
            .read(|txn| BucketsTable::open_readonly(txn)?.get(&name))
            .unwrap();
        assert_eq!(got, Some(created));
        drop(state);
    });
}

#[test]
fn write_closure_aborts_on_error() {
    rt(async {
        let (state, db, _) = open_db();
        let handle = Handle::new(db);
        let name = bucket::name("beta").unwrap();
        let err = handle.write(|txn| {
            BucketsTable::open(txn)?.put(&name, std::time::UNIX_EPOCH)?;
            Err::<(), _>(Error::Io(std::io::Error::other("boom")))
        });
        assert!(err.is_err());
        // The aborted transaction persisted nothing.
        let got = handle
            .read(|txn| Ok(BucketsTable::open_readonly(txn)?.get(&name)?.is_some()))
            .unwrap();
        assert!(!got);
        drop(state);
    });
}
