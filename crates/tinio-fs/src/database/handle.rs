//! Shared database access handle.

use std::{path::Path, sync::Arc};

use redb::{Database, ReadTransaction, ReadableDatabase, WriteTransaction};

use super::{
    compact::{needs_compact, snapshot},
    error::Error,
    open::open,
    tables::StateTable,
};

/// The shared access handle to a state database.
///
/// Every store holds an `Arc<Handle>` clone; the single redb writer
/// serializes writes (the per-store in-process locks are gone). `compact`
/// cannot run through this handle (`&mut` is structurally unavailable once
/// shared) — it runs before `FsStorage` construction (meta-redb-spec §5.9).
///
/// **Synchronous by design**: transactions execute inline on the caller's
/// thread — they are small (single-row or bounded-range operations, in
/// memory) and blocking is on the order of microseconds, so the async
/// callers (sweep, meta, multipart) do not need `spawn_blocking`. Do not
/// add large full-table transactions to a hot path without revisiting
/// this. Durability is redb's default `Immediate` (every commit flushes
/// to disk): safe, at one flush per write transaction — a 10 000-part
/// multipart upload costs 10 000 flushes by design (a `Durability::None`
/// would trade crash/outage safety for speed; the meta state is derivable
/// and self-healing, but this trade-off was deliberately not made).
#[derive(Debug)]
pub struct Handle {
    db: Database,
}

impl Handle {
    /// Open the state database at `state_dir` and wrap it (the single
    /// construction path of the standalone store constructors).
    pub fn open(state_dir: &Path) -> Result<Arc<Self>, Error> {
        Ok(Self::new(open(state_dir)?.db))
    }

    /// Wrap an opened database (after the pre-sharing compact window —
    /// the orchestration path: `open` → `compact_if_needed` → `Handle::new`,
    /// meta-redb-spec §5.9/G1).
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }

    /// Run `f` against a read transaction.
    ///
    /// Zero-copy guards live only inside `f`; return owned copies (guards
    /// are invalid once the transaction ends).
    pub(crate) fn read<T>(
        &self,
        f: impl FnOnce(&ReadTransaction) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let txn = self.db.begin_read()?;
        f(&txn)
    }

    /// Run `f` against a write transaction — committed on success, aborted
    /// on error. Multi-table operations run as one write closure.
    pub(crate) fn write<T>(
        &self,
        f: impl FnOnce(&mut WriteTransaction) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut txn = self.db.begin_write()?;
        match f(&mut txn) {
            Ok(value) => {
                txn.commit()?;
                Ok(value)
            }
            Err(err) => {
                let _ = txn.abort();
                Err(err)
            }
        }
    }

    /// Evaluate fragmentation against `threshold_percent` and update the
    /// `compact_needed` marker — ONE write transaction (the stats call
    /// takes the write lock; the marker write is skipped when unchanged,
    /// so an unchanged evaluation costs a single aborted transaction).
    /// Returns whether the marker ended up set. The sweep calls this once
    /// per round; the marker is consumed at startup by
    /// [`super::compact::compact_if_needed`] (meta-redb-spec §5.9).
    pub(crate) fn evaluate_compact(&self, threshold_percent: u8) -> Result<bool, Error> {
        let mut txn = self.db.begin_write()?;
        let stats = txn.stats()?;
        let needed = needs_compact(&snapshot(&stats), threshold_percent);
        let changed = {
            let mut state = StateTable::open(&mut txn)?;
            let current = state.compact_marker()?;
            if current == needed {
                false
            } else {
                state.set_compact_marker_value(needed)?;
                true
            }
        };
        if changed {
            txn.commit()?;
        } else {
            txn.abort()?;
        }
        Ok(needed)
    }

    /// Whether the `compact_needed` marker is set — the marker protocol's
    /// read half (meta-redb-spec §5.9; the startup orchestration and
    /// doctor report it, the runtime evaluation path is
    /// [`Self::evaluate_compact`]).
    pub fn compact_needed(&self) -> Result<bool, Error> {
        self.read(|txn| StateTable::open_readonly(txn)?.compact_marker())
    }

    /// Set or clear the `compact_needed` marker — the marker protocol's
    /// write half (meta-redb-spec §5.9; doctor `--fix` sets/clears it, the
    /// runtime path goes through [`Self::evaluate_compact`]).
    pub fn mark_compact_needed(&self, needed: bool) -> Result<(), Error> {
        self.write(|txn| StateTable::open(txn)?.set_compact_marker_value(needed))
    }
}
