use std::{
    fs, io,
    ops::DerefMut,
    sync::Arc,
    thread,
    time::{Duration, UNIX_EPOCH},
};

use io::Error as IoError;
use redb::{
    Database, ReadableDatabase, StorageError::ValueTooLarge, TableError::TableDoesNotExist,
    TransactionError::Storage as TxnStorage,
};
use tokio::{runtime::Builder, sync::oneshot, time::timeout};

use super::{
    compact::{Compaction, Stats, compact_if_needed, needs_compact},
    error::Error,
    handle,
    handle::Handle,
    open::{Integrity, check_integrity, open},
    tables::{BucketsTable, ObjectMetaTable, PartsTable, StateTable, UploadsTable},
};
use crate::{
    _core::{bucket, object},
    _util::testing::{assert_send_sync, etag},
};

fn open_db() -> (tempfile::TempDir, Database, Stats) {
    let state = tempfile::tempdir().unwrap();
    let opened = open(state.path()).unwrap();
    (state, opened.db, opened.stats)
}

#[test]
fn redb_errors_wrap_per_kind() {
    let err = Error::from(TableDoesNotExist("t".into()));
    assert!(matches!(err, Error::Table(TableDoesNotExist(_))), "{err:?}");
    let err = Error::from(TxnStorage(ValueTooLarge(1)));
    assert!(matches!(err, Error::Transaction(_)), "{err:?}");
    assert!(
        Error::from(ValueTooLarge(1))
            .to_string()
            .contains("storage error")
    );
}

#[test]
fn errors_are_send_sync_and_static() {
    assert_send_sync::<Error>();
}

#[tokio::test]
async fn open_creates_database_and_all_tables() {
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
}

#[tokio::test]
async fn open_twice_is_idempotent() {
    let (state, _, _) = open_db();
    // A second open on the same file must succeed.
    let db = open(state.path()).unwrap().db;
    let mut txn = db.begin_write().unwrap();
    let version = StateTable::open(&mut txn)
        .unwrap()
        .ensure_version(state.path())
        .unwrap();
    assert_eq!(version, 1);
}

#[tokio::test]
async fn version_mismatch_is_unsupported_version() {
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
}

#[tokio::test]
async fn open_fresh_state_dir_creates_dir() {
    let base = tempfile::tempdir().unwrap();
    let nested = base.path().join("a/b");
    let db = open(&nested).unwrap().db;
    assert!(nested.join("meta.redb").exists());
    drop(db);
}

#[tokio::test]
async fn corrupt_file_is_an_error() {
    let (state, _, _) = open_db();
    fs::write(state.path().join("meta.redb"), b"not a redb file").unwrap();
    let err = open(state.path()).unwrap_err();
    assert!(matches!(err, Error::Open(_)), "{err:?}");
}

#[tokio::test]
async fn check_integrity_reports_healthy() {
    let (state, db, _) = open_db();
    // The integrity check opens the file exclusively — drop the
    // test handle first.
    drop(db);
    assert!(matches!(
        check_integrity(state.path()).unwrap(),
        Integrity::Healthy
    ));
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

#[tokio::test]
async fn compact_marker_round_trip() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    assert!(!handle.compact_needed().unwrap());
    handle.mark_compact_needed(true).await.unwrap();
    assert!(handle.compact_needed().unwrap());
    handle.mark_compact_needed(false).await.unwrap();
    assert!(!handle.compact_needed().unwrap());
    drop(state);
}

#[tokio::test]
async fn compact_if_needed_shrinks_and_clears_marker() {
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
    let grown = fs::metadata(state.path().join("meta.redb")).unwrap().len();
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
    let after = fs::metadata(state.path().join("meta.redb")).unwrap().len();
    assert!(
        after < grown,
        "compact must shrink the file: {after} >= {grown}"
    );
    // The marker is cleared.
    let txn = db.begin_read().unwrap();
    let state_table = StateTable::open_readonly(&txn).unwrap();
    assert!(!state_table.compact_marker().unwrap());
    drop(state);
}

#[tokio::test]
async fn compact_if_needed_skips_when_clean() {
    let (state, mut db, stats) = open_db();
    let report = compact_if_needed(&mut db, false, stats, 20).unwrap();
    assert!(matches!(report, Compaction::Skipped));
    drop(state);
}

#[tokio::test]
async fn check_integrity_fails_on_garbage() {
    let (state, db, _) = open_db();
    drop(db);
    fs::write(state.path().join("meta.redb"), b"junk").unwrap();
    // Unreadable file: the open itself fails (doctor reports the
    // database as unrecoverable — metadata is derivable).
    assert!(check_integrity(state.path()).is_err());
}

#[tokio::test]
async fn handle_read_write_round_trip() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("alpha").unwrap();
    let created = UNIX_EPOCH + Duration::from_nanos(42);
    let write_name = name.clone();
    handle
        .write(move |txn| {
            BucketsTable::open(txn)?.put(&write_name, created)?;
            Ok(())
        })
        .await
        .unwrap();
    let got = handle
        .read(|txn| BucketsTable::open_readonly(txn)?.get(&name))
        .unwrap();
    assert_eq!(got, Some(created));
    drop(state);
}

#[tokio::test]
async fn write_closure_aborts_on_error() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("beta").unwrap();
    let write_name = name.clone();
    let err = handle
        .write(move |txn| {
            BucketsTable::open(txn)?.put(&write_name, UNIX_EPOCH)?;
            Err::<(), _>(Error::Io(IoError::other("boom")))
        })
        .await;
    assert!(err.is_err());
    // The aborted transaction persisted nothing.
    let got = handle
        .read(|txn| Ok(BucketsTable::open_readonly(txn)?.get(&name)?.is_some()))
        .unwrap();
    assert!(!got);
    drop(state);
}

// --- write-lock histograms (pipeline-spec.md §4) ---

/// The recorded snapshot of a fresh handle: zero write transactions must
/// produce a fully zero histogram — no spurious empty buckets.
#[tokio::test]
async fn write_lock_stats_start_all_zero() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let snapshot = handle.write_lock_stats();
    assert_eq!(snapshot.count, 0);
    assert_eq!(snapshot.wait_sum_us, 0);
    assert_eq!(snapshot.total_sum_us, 0);
    assert_eq!(snapshot.wait_max_us, 0);
    assert_eq!(snapshot.total_max_us, 0);
    assert!(snapshot.wait_buckets.iter().all(|n| *n == 0));
    assert!(snapshot.total_buckets.iter().all(|n| *n == 0));
    drop(state);
}

/// One write transaction records exactly one wait + one total sample
/// (wait ≤ total — the lock wait is a prefix of the transaction).
#[tokio::test]
async fn write_transactions_are_timed() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("alpha").unwrap();
    handle
        .write(move |txn| {
            BucketsTable::open(txn)?.put(&name, UNIX_EPOCH)?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = handle.write_lock_stats();
    assert_eq!(snapshot.count, 1);
    assert_eq!(snapshot.wait_buckets.iter().sum::<u64>(), 1);
    assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 1);
    assert!(
        snapshot.wait_sum_us <= snapshot.total_sum_us,
        "the lock wait must be a prefix of the total: {} > {}",
        snapshot.wait_sum_us,
        snapshot.total_sum_us
    );
    assert_eq!(snapshot.total_max_us, snapshot.total_sum_us);
    // The max sample's bucket carries the single count.
    let bucket = handle::write_lock_bucket(snapshot.total_max_us);
    assert_eq!(snapshot.total_buckets[bucket], 1);
    drop(state);
}

/// A slow write closure lands in the overflow bucket (>100k µs)
/// deterministically — the histogram records real durations, not just
/// counts.
#[tokio::test]
async fn slow_write_lands_in_the_last_bucket() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("gamma").unwrap();
    handle
        .write(move |txn| {
            BucketsTable::open(txn)?.put(&name, UNIX_EPOCH)?;
            thread::sleep(Duration::from_millis(150));
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = handle.write_lock_stats();
    assert_eq!(snapshot.count, 1);
    assert_eq!(snapshot.total_buckets.len(), 7);
    assert_eq!(
        snapshot.total_buckets[6], 1,
        "a 150 ms transaction must land in the >100k µs bucket"
    );
    assert!(
        snapshot.total_max_us >= 100_000,
        "150 ms must exceed the 100 ms overflow bound: {}",
        snapshot.total_max_us
    );
    drop(state);
}

/// An aborted write closure is still one timed transaction (the total
/// covers the abort) and persists nothing.
#[tokio::test]
async fn aborted_write_is_recorded() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("delta").unwrap();
    let write_name = name.clone();
    let err = handle
        .write(move |txn| {
            BucketsTable::open(txn)?.put(&write_name, UNIX_EPOCH)?;
            Err::<(), _>(Error::Io(IoError::other("boom")))
        })
        .await;
    assert!(err.is_err());
    let snapshot = handle.write_lock_stats();
    assert_eq!(snapshot.count, 1);
    assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 1);
    drop(state);
}

/// Read transactions are never timed — the histograms cover write
/// transactions only.
#[tokio::test]
async fn reads_do_not_record() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let b = bucket::name("data").unwrap();
    let k = object::key("a.txt").unwrap();
    handle
        .read(|txn| ObjectMetaTable::open_readonly(txn)?.get(&b, &k))
        .unwrap();
    assert_eq!(handle.write_lock_stats().count, 0);
    drop(state);
}

/// P5: `evaluate_compact` is the only direct `begin_write` path besides
/// the `write` wrapper — its stats transaction goes through the SAME
/// timing helper, so "all write transactions are covered" holds.
#[tokio::test]
async fn evaluate_compact_is_timed() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    assert_eq!(handle.write_lock_stats().count, 0);
    handle.evaluate_compact(20).await.unwrap();
    let snapshot = handle.write_lock_stats();
    assert_eq!(
        snapshot.count, 1,
        "the stats transaction must be timed (P5)"
    );
    assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 1);
    // A second evaluation with the marker unchanged is still a write
    // transaction (an aborted one) — still recorded.
    handle.evaluate_compact(20).await.unwrap();
    assert_eq!(handle.write_lock_stats().count, 2);
    drop(state);
}

/// Multiple transactions accumulate per-bucket counts and the sum/max.
#[tokio::test]
async fn snapshots_accumulate_across_transactions() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    for i in 0..5u64 {
        let name = bucket::name(format!("bucket{i}")).unwrap();
        handle
            .write(move |txn| {
                BucketsTable::open(txn)?.put(&name, UNIX_EPOCH)?;
                Ok(())
            })
            .await
            .unwrap();
    }
    let snapshot = handle.write_lock_stats();
    assert_eq!(snapshot.count, 5);
    assert_eq!(snapshot.total_buckets.iter().sum::<u64>(), 5);
    assert_eq!(snapshot.wait_buckets.iter().sum::<u64>(), 5);
    assert!(snapshot.total_max_us >= snapshot.total_sum_us / 5);
    drop(state);
}

/// P1 (G3 revision): a write transaction executes on the tokio blocking
/// pool, NOT on the runtime worker. Deterministic proof: on a
/// single-thread runtime, a write parked inside its closure (the write
/// transaction is open — the redb write lock is held) must not stop the
/// worker — a read transaction completes while the write is still
/// parked, and the write task is still in flight when the read returns.
/// (Pre-revision the inline write would hang the runtime forever — the
/// worker is occupied, so the timeout is never polled.)
#[test]
fn async_writes_run_on_the_blocking_pool() {
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    runtime.block_on(async {
        let (state, db, _) = open_db();
        let handle = Handle::new(db);
        let name = bucket::name("data").unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let writer = {
            let handle = Arc::clone(&handle);
            let name = name.clone();
            tokio::spawn(async move {
                handle
                    .write(move |txn| {
                        BucketsTable::open(txn)?.put(&name, UNIX_EPOCH)?;
                        let _ = started_tx.send(());
                        // Park with the write transaction OPEN — the
                        // redb write lock is held from here on.
                        let _ = gate_rx.blocking_recv();
                        Ok(())
                    })
                    .await
                    .unwrap()
            })
        };
        // The write is running on the blocking pool and parked (not
        // committed). A failure to reach the park within 5 s means the
        // worker was occupied — the pre-revision hang.
        timeout(Duration::from_secs(5), started_rx)
            .await
            .expect("the write never reached the blocking pool")
            .unwrap();
        // The single runtime worker is free: a read transaction
        // completes here while the write is still parked. The parked
        // write is uncommitted, so the row is invisible (redb MVCC)...
        let got = handle
            .read(|txn| BucketsTable::open_readonly(txn)?.get(&name))
            .unwrap();
        assert_eq!(got, None, "the parked write must not be committed yet");
        // ...and the parked write is still in flight (it holds the
        // write lock — the single-writer serialization is unchanged).
        assert!(
            !writer.is_finished(),
            "the parked write must still be in flight"
        );
        drop(gate_tx);
        writer.await.unwrap();
        // The write committed when the closure returned: the row is
        // now visible.
        let got = handle
            .read(|txn| BucketsTable::open_readonly(txn)?.get(&name))
            .unwrap();
        assert_eq!(got, Some(UNIX_EPOCH));
        drop(state);
    });
}

/// A panic inside the write closure re-panics the caller: the blocking
/// task's panic surfaces as a `JoinError`, which the wrapper re-raises
/// on the awaiting side (the old inline call propagated panics the same
/// way — behavior unchanged by the blocking-pool move). The open
/// transaction is dropped, i.e. aborted, so the database stays
/// consistent; only the awaiting task panics.
#[tokio::test]
#[should_panic(expected = "the write-transaction task panicked")]
async fn a_panicking_write_closure_panics_the_caller() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    handle
        .write(move |_txn| -> Result<(), Error> { panic!("boom") })
        .await
        .unwrap();
    drop(state);
}

#[tokio::test]
async fn bucket_get_or_insert_returns_the_stored_creation_time() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("data").unwrap();
    let write_name = name.clone();
    let now = UNIX_EPOCH + Duration::from_nanos(42);
    handle
        .write(move |txn| {
            let mut table = BucketsTable::open(txn)?;
            // Insert-absent path, then the stored-value path: a later
            // call must return the first recorded time, not overwrite it.
            let first = table.get_or_insert(&write_name, now)?;
            let second = table.get_or_insert(&write_name, now + Duration::from_secs(1))?;
            assert_eq!(first, second);
            Ok(second)
        })
        .await
        .unwrap();
    drop(state);
}

#[tokio::test]
async fn object_meta_put_round_trips() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("data").unwrap();
    let write_name = name.clone();
    let key = object::key("dir/a.txt").unwrap();
    let write_key = key.clone();
    let written = etag("5eb63bbbe01eeed093cb22bb8f5acdc3");
    let write_etag = written.clone();
    handle
        .write(move |txn| {
            let mut table = ObjectMetaTable::open(txn)?;
            table.put(&write_name, &write_key, &write_etag, 2, UNIX_EPOCH, 7)
        })
        .await
        .unwrap();
    let got = handle
        .read(|txn| ObjectMetaTable::open_readonly(txn)?.get(&name, &key))
        .unwrap();
    let row = got.expect("the put row must be readable");
    assert_eq!(row.etag, written);
    assert_eq!(row.size, 2);
    assert_eq!(row.file_identity, 7);
    drop(state);
}

#[tokio::test]
async fn upload_and_part_rows_round_trip_and_list_from_stops_at_the_next_upload() {
    let (state, db, _) = open_db();
    let handle = Handle::new(db);
    let name = bucket::name("data").unwrap();
    let write_name = name.clone();
    let key = object::key("big.bin").unwrap();
    let write_key = key.clone();
    let part_etag = etag("d41d8cd98f00b204e9800998ecf8427e");
    let write_etag = part_etag.clone();
    handle
        .write(move |txn| {
            {
                let mut uploads = UploadsTable::open(txn)?;
                uploads.put(&write_name, "aaa", &write_key, UNIX_EPOCH)?;
                uploads.put(&write_name, "bbb", &write_key, UNIX_EPOCH)?;
            }
            let mut parts = PartsTable::open(txn)?;
            parts.put(&write_name, "aaa", 1, &write_etag)?;
            parts.put(&write_name, "bbb", 1, &write_etag)?;
            Ok(())
        })
        .await
        .unwrap();
    // Paging past the last part of `aaa` must stop at the `bbb` row —
    // the mismatch break, never a cross-upload bleed.
    let (page, truncated) = handle
        .read(|txn| PartsTable::open_readonly(txn)?.list_from(&name, "aaa", 0, 10))
        .unwrap();
    assert_eq!(page, vec![(1, part_etag.to_string())]);
    assert!(!truncated);
    drop(state);
}

#[tokio::test]
async fn compact_if_needed_unchanged_when_nothing_to_reclaim() {
    let (state, mut db, _) = open_db();
    // Reach the compact fixpoint first: a fresh database has initial
    // slack to reclaim, and `compact_if_needed`'s own marker-clear
    // write would leave COW residue for a following call (the first
    // `compact()` after any write reports progress). Only at the
    // fixpoint is "nothing to reclaim" true.
    while db.compact().unwrap() {}
    // Marker set (passed in, as `open` would) on the small, clean
    // database: `compact()` has nothing to reclaim — `Unchanged` is
    // reported and the marker is still cleared.
    let stats = Stats {
        allocated_bytes: 0,
        fragmented_bytes: 0,
    };
    let report = compact_if_needed(&mut db, true, stats, 20).unwrap();
    assert!(matches!(report, Compaction::Unchanged));
    let txn = db.begin_read().unwrap();
    let state_table = StateTable::open_readonly(&txn).unwrap();
    assert!(!state_table.compact_marker().unwrap());
    drop(state);
}
