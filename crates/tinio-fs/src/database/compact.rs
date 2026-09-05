//! Offline compaction evaluation and execution.

use redb::Database;

use super::error::Error;
use crate::_store::state;

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
pub(crate) fn needs_compact(stats: &Stats, threshold_percent: u8) -> bool {
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
/// `Clone`, and once wrapped in `Arc<Handle>` a mutable reference is
/// structurally impossible). Checks the `compact_needed` marker plus the
/// open-time stats snapshot (double insurance), then compacts and clears
/// the marker. The runtime never compacts: the server startup orchestration
/// and `doctor --fix` call this offline, before `FsStorage` construction.
/// `compact_needed` and `stats` come from [`super::open`].
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
        let mut txn = db.begin_write().map_err(|e| Error::Redb(e.into()))?;
        state::Table::open(&mut txn)?.set_compact_marker(false)?;
        txn.commit().map_err(|e| Error::Redb(e.into()))?;
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
