//! Offline compaction evaluation and execution.
//!
//! Relocated from `tinio-fs`'s `database/compact.rs` (plan 2026-10-03 Task 4): the
//! evaluation is redb machinery, so it lives with the driver. The fs-facing
//! name and signature are unchanged (tinio-fs re-exports them through its
//! `database` shim); `compact_if_needed` still runs on a **not-yet-shared**
//! `Database`, the only moment `&mut` is available (meta-redb-spec §5.9).

use redb::Database;

use crate::{error::Error, state};

/// Compact never triggers below this allocated size — a small database
/// gains nothing from a rewrite (meta-redb-spec Q1).
const MIN_ALLOCATED: u64 = 64 * 1024 * 1024;

/// A fragmentation snapshot of the state database (compact evaluation,
/// meta-redb-spec §5.9). All values in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Allocated size (`allocated_pages × page_size`).
    pub allocated_bytes: u64,
    /// Fragmented bytes (unused space inside the allocated pages).
    pub fragmented_bytes: u64,
}

/// Whether the database should be compacted: the fragmentation ratio
/// `fragmented / allocated` reaches `threshold_percent` (%), but never
/// below the 64 MiB floor (a small database gains nothing from a rewrite).
pub fn needs_compact(stats: &Stats, threshold_percent: u8) -> bool {
    if stats.allocated_bytes < MIN_ALLOCATED {
        return false;
    }
    let threshold = threshold_percent as u64;
    stats.fragmented_bytes.saturating_mul(100) >= stats.allocated_bytes.saturating_mul(threshold)
}

/// Outcome of [`compact_if_needed`] (doctor reporting).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compaction {
    /// Neither the marker nor stats required compaction.
    Skipped,
    /// Compaction ran and the file shrank.
    Compacted,
    /// Compaction was needed but `compact()` reclaimed nothing (the marker
    /// was still cleared).
    Unchanged,
}

/// Evaluate and run compaction on a **not-yet-shared** `Database` — the
/// only moment `&mut` is available (meta-redb-spec §5.9: `Database` is not
/// `Clone`, and once wrapped in `Arc<Driver>` a mutable reference is
/// structurally impossible). Checks the `compact_needed` marker plus the
/// open-time stats snapshot (double insurance), then compacts and clears
/// the marker. The runtime never compacts: the server startup orchestration
/// and `doctor --fix` call this offline, before `FsStorage` construction.
/// `compact_needed` and `stats` come from [`crate::open::open`].
pub fn compact_if_needed(
    db: &mut Database,
    compact_needed: bool,
    stats: Stats,
    threshold_percent: u8,
) -> Result<Compaction, Error> {
    let was_needed = compact_needed || needs_compact(&stats, threshold_percent);
    if !was_needed {
        return Ok(Compaction::Skipped);
    }
    // No transactions are alive, so the savepoint deadlock the redb docs
    // warn about cannot occur.
    let compacted = db.compact()?;
    // Clear the marker whether or not compact actually shrank anything
    // (`Ok(false)` = nothing to reclaim — the need was addressed either
    // way).
    {
        let mut txn = db.begin_write()?;
        state::Table::open(&mut txn)?.set_compact_marker(false)?;
        txn.commit()?;
    }

    Ok(if compacted {
        Compaction::Compacted
    } else {
        Compaction::Unchanged
    })
}

/// The fragmentation snapshot of a redb `DatabaseStats`.
pub(crate) fn snapshot(stats: &redb::DatabaseStats) -> Stats {
    Stats {
        allocated_bytes: stats
            .allocated_pages()
            .saturating_mul(stats.page_size() as u64),
        fragmented_bytes: stats.fragmented_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, ops::DerefMut};

    use redb::{Database, ReadableDatabase};

    use super::*;
    use crate::{object_meta, open, state};

    fn open_db() -> (tempfile::TempDir, Database, Stats) {
        let dir = tempfile::tempdir().unwrap();
        let opened = open::open(dir.path()).unwrap();
        (dir, opened.db, opened.stats)
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
    async fn compact_if_needed_shrinks_and_clears_marker() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = open::open(dir.path()).unwrap().db;
        // Churn: write then delete many entries — COW leaves the
        // file grown with dead pages.
        {
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = object_meta::Table::open(&mut txn).unwrap();
                for i in 0..50_000u32 {
                    let key = format!("key-{i}");
                    table
                        .insert(
                            ("data", key.as_str()),
                            (
                                key.as_str(),
                                u64::from(i),
                                u64::from(i),
                                0,
                                "",
                                "",
                                "",
                                "",
                                "null",
                                0,
                                false,
                            ),
                        )
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        {
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = object_meta::Table::open(&mut txn).unwrap();
                for i in 0..50_000u32 {
                    let key = format!("key-{i}");
                    // Raw remove: domain `remove` validates keys; this
                    // churn fixture only needs the B+tree delete path.
                    table.deref_mut().remove(("data", key.as_str())).unwrap();
                }
            }
            txn.commit().unwrap();
        }
        let grown = fs::metadata(dir.path().join("meta.redb")).unwrap().len();
        // Set the marker (the marker path bypasses the 64 MiB floor —
        // the runtime evaluation would have set it once the ratio
        // crossed the threshold).
        {
            let mut txn = db.begin_write().unwrap();
            {
                let mut state_table = state::Table::open(&mut txn).unwrap();
                state_table.set_compact_marker(true).unwrap();
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
        let after = fs::metadata(dir.path().join("meta.redb")).unwrap().len();
        assert!(
            after < grown,
            "compact must shrink the file: {after} >= {grown}"
        );
        // The marker is cleared.
        let txn = db.begin_read().unwrap();
        let state_table = state::Table::open_readonly(&txn).unwrap();
        assert!(!state_table.compact_marker().unwrap());
        drop(dir);
    }

    #[tokio::test]
    async fn compact_if_needed_skips_when_clean() {
        let (dir, mut db, stats) = open_db();
        let report = compact_if_needed(&mut db, false, stats, 20).unwrap();
        assert!(matches!(report, Compaction::Skipped));
        drop(dir);
    }

    #[tokio::test]
    async fn compact_if_needed_unchanged_when_nothing_to_reclaim() {
        let (dir, mut db, _) = open_db();
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
        let state_table = state::Table::open_readonly(&txn).unwrap();
        assert!(!state_table.compact_marker().unwrap());
        drop(dir);
    }
}
