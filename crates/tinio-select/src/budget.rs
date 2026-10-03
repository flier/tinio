//! The per-request evaluation budget (P3): the cap, its one message, and the
//! [`Budget`] every priced step charges.
//!
//! It lives here rather than in `engine` because the row context every
//! evaluation path carries — `json`'s leaf lookup included — holds it: the
//! dependency points from the leaf utilities to the budget, never up into the
//! engine.

use std::cell::Cell;

use crate::error::Error;

/// The per-request evaluation budget (P3), mirroring the LIKE matcher's own
/// `LIKE_CELL_CAP` product. That cap bounds one pattern pair; this one bounds
/// the whole request, because the attacker picks the record count and the LIKE
/// pairs alike — neither is a per-call quantity. Only the possibly-superlinear
/// work is priced (see [`Budget::charge`]); a per-record constant is not, so an
/// ordinary wide `SELECT` streams instead of aborting. 10⁷ cells is high-end
/// single-digit milliseconds per the LIKE precedent.
pub(crate) const EVAL_CELL_CAP: u64 = 10_000_000;

/// This request's evaluation budget, charged by the work each step does (P3).
/// `Cell`, not `Mut`, because every evaluation path reads the record through a
/// shared `&RowCtx`.
#[derive(Debug)]
pub(crate) struct Budget {
    spent: Cell<u64>,
}

impl Budget {
    pub(crate) fn new() -> Self {
        Self {
            spent: Cell::new(0),
        }
    }

    /// Charge `cells` to the request and refuse once the running total is past
    /// `EVAL_CELL_CAP`. Every step that walks something attacker-sized charges
    /// the work it does — one per JSON entry scanned, one per headerless
    /// name-set walk, the LIKE cell product — so the spend tracks the actual
    /// cost rather than a static shape. An indexed reference lookup is O(1) in
    /// the header width and is deliberately not charged: the budget prices
    /// DP-shaped work, not a wide but legitimate projection.
    pub(crate) fn charge(&self, cells: u64) -> Result<(), Error> {
        let total = self.spent.get() + cells;
        self.spent.set(total);
        if total > EVAL_CELL_CAP {
            return Err(too_expensive());
        }
        Ok(())
    }
}

#[cfg(test)]
impl Budget {
    /// Test-only: seed the running total (the P3 pins start just under the cap).
    pub(crate) fn set_spent(&self, spent: u64) {
        self.spent.set(spent);
    }

    /// Test-only: the running total, to pin what a walk charged.
    pub(crate) fn spent(&self) -> u64 {
        self.spent.get()
    }
}

/// The budget's one message — the value error the server maps to an in-stream
/// `S3QueryError`, same channel as the LIKE cap.
fn too_expensive() -> Error {
    Error::Value("SELECT expression too expensive".into())
}
