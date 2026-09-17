//! Per-root byte accounting: the exact `data_pos` span check that is the
//! 1 MB cap authority, plus a charged `Read` backstop that aborts a grossly
//! over-cap value mid-read.
//!
//! Struson reports consumed bytes through `current_position(false).data_pos`;
//! that single sequence feeds the record-start offsets and the exact span
//! check. The `Read` backstop is a second, coarser counter: it charges every
//! byte struson is served since the last verified boundary — `begin`, or a
//! passing span check — and errors once that charge passes the cap plus a
//! fixed allowance, so a matched value that vastly exceeds the cap cannot be
//! materialized whole before the exact check runs (design spec, byte-supply
//! section; deviation ⑦).

use std::{
    cell::Cell,
    io::{self, Read},
    rc::Rc,
};

use crate::{
    error::Error,
    json::error::Error as JsonError,
    record::{CAP_ALLOWANCE, MAX_RECORD, cap_io_error},
};

/// Shared per-root account. `RootBudget` owns one and every `BudgetedRead`
/// clone shares it, so the exact span check and the charged backstop see the
/// same root.
struct Account {
    start: Cell<u64>,
    /// Bytes the backstop has served since the last verified boundary:
    /// `begin` or a passing `check`.
    charged: Cell<u64>,
    /// The span check or the backstop rejected this root. Test-only tripwire:
    /// nothing on the production path reads it, so the field and its writes
    /// compile out of a release build.
    #[cfg(test)]
    fired: Cell<bool>,
    cap: u64,
}

/// The 1 MB budget of one root value. The handler calls `begin` once per root
/// (right after the root value start is known), `begin_element` as each array
/// element opens, and `check` at every batch boundary; all three reset the
/// charged counter, so it always measures bytes since the last verified
/// boundary — a `begin` or a passing span check — while the `Read` backstop
/// charges the same account as struson reads.
///
/// The budget also owns the one question the document-level check turns on:
/// whether the current span is still the root's own. `begin_element` is what
/// says it is not, so no caller has to carry that as a flag.
pub(crate) struct RootBudget {
    account: Rc<Account>,
    /// The current span opened at an array element's start, superseding the
    /// root origin. Read only by `check_root_span`.
    element_origin: bool,
}

impl RootBudget {
    pub(crate) fn new(cap: u64) -> Self {
        Self {
            account: Rc::new(Account {
                start: Cell::new(0),
                charged: Cell::new(0),
                #[cfg(test)]
                fired: Cell::new(false),
                cap,
            }),
            element_origin: false,
        }
    }

    pub(crate) fn with_default_cap() -> Self {
        Self::new(MAX_RECORD)
    }

    /// Start a new root at `pos` (the byte offset of the root value start) and
    /// reset the backstop's charge. The root origin is the one the
    /// document-level span check measures.
    pub(crate) fn begin(&mut self, pos: u64) {
        self.start(pos);
        self.element_origin = false;
    }

    /// Start a new array element span at `pos`. It supersedes the root origin:
    /// from here on the cap measures the element's own span, checked at its
    /// `End` (document.rs), and the root's closing span is left alone.
    pub(crate) fn begin_element(&mut self, pos: u64) {
        self.start(pos);
        self.element_origin = true;
    }

    fn start(&mut self, pos: u64) {
        self.account.start.set(pos);
        self.account.charged.set(0);
        #[cfg(test)]
        self.account.fired.set(false);
    }

    /// Check the span so far; returns false once the cap is exceeded.
    ///
    /// A passing check also clears the charged backstop: the exact `data_pos`
    /// span check is the cap authority, so once it has verified the span since
    /// `begin`, the bytes charged so far are accounted for. From that boundary
    /// on, the backstop measures bytes since the last verified span — exactly
    /// its purpose (abort a runaway value before it is materialized whole).
    /// Bytes that belong to no span — the separator whitespace a `has_next()`
    /// consumes before the next `begin_element`, the document tail
    /// `consume_trailing_whitespace` consumes — would otherwise be charged to
    /// a root whose own span is legal, and an element of exactly 1 MB followed
    /// by more than `CAP_ALLOWANCE` of whitespace would be rejected even
    /// though its span check passed.
    ///
    /// Residual window, by construction: whitespace consumed by `has_next()`
    /// before `]` and the document tail can still push the charge past the
    /// allowance for a pathological input. The exact span check remains the
    /// authority and no allowance removes that window. `fired` is untouched:
    /// a passing check never clears an earlier verdict.
    pub(crate) fn check(&mut self, pos: u64) -> bool {
        if pos.saturating_sub(self.account.start.get()) > self.account.cap {
            #[cfg(test)]
            self.account.fired.set(true);
            return false;
        }
        self.account.charged.set(0);
        true
    }

    /// The one cap verdict the sample sites report: a failed `check` at `pos`
    /// is the pinned `Cap` error.
    pub(crate) fn check_or_cap(&mut self, pos: u64) -> Result<(), Error> {
        if self.check(pos) {
            Ok(())
        } else {
            Err(Error::Json(JsonError::Cap.into()))
        }
    }

    /// Measure the span of a closing root: the document-level check the
    /// handler runs before the trailing probe.
    ///
    /// A no-op once an element's `begin_element` took the origin — the
    /// document-level end reaches past that element, through the whitespace
    /// before `]` and the bracket itself, while the element's own span was
    /// checked at its `End`, so measuring here would charge bytes the element
    /// never owned and reject a legal one. The no-op resets nothing either: the
    /// element's passing check is already the last verified boundary. An empty
    /// root array and a single-value root keep the root origin, so both are
    /// measured.
    pub(crate) fn check_root_span(&mut self, pos: u64) -> Result<(), Error> {
        if self.element_origin {
            return Ok(());
        }
        self.check_or_cap(pos)
    }

    #[cfg(test)]
    pub(crate) fn fired(&self) -> bool {
        self.account.fired.get()
    }
}

/// `Read` wrapper charging every byte it serves to the shared per-root
/// account. The charge is a BACKSTOP, not the cap authority (see
/// `record::CAP_ALLOWANCE`): once the charged total passes `cap +
/// CAP_ALLOWANCE` it fails with the pinned `record::cap_io_error()`, which
/// `json::Error::from` maps back to `json::Error::Cap` so the crate-level
/// message matches the retired kernel's byte for byte.
pub(crate) struct BudgetedRead<R> {
    inner: R,
    account: Rc<Account>,
}

impl<R: Read> BudgetedRead<R> {
    pub(crate) fn new(inner: R, budget: &RootBudget) -> Self {
        Self {
            inner,
            account: Rc::clone(&budget.account),
        }
    }
}

impl<R: Read> Read for BudgetedRead<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        let charged = self.account.charged.get().saturating_add(n as u64);
        self.account.charged.set(charged);
        if charged > self.account.cap.saturating_add(CAP_ALLOWANCE) {
            #[cfg(test)]
            self.account.fired.set(true);
            return Err(cap_io_error());
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};

    use super::RootBudget;
    use crate::record::{CAP_ALLOWANCE, CAP_MESSAGE, MAX_RECORD};

    #[test]
    fn a_root_exactly_at_the_cap_is_accepted() {
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(10);
        assert!(budget.check(10 + 1_048_576));
        assert!(!budget.fired());
    }

    #[test]
    fn one_byte_past_the_cap_fires() {
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(0);
        assert!(!budget.check(1_048_577));
        assert!(budget.fired());
    }

    #[test]
    fn intermediate_batches_are_checked_against_the_same_origin() {
        // Q18=b: the span accumulates across batches of one root, so a later
        // batch can fire the cap even when each batch is small.
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(100);
        assert!(budget.check(100 + 600_000));
        assert!(!budget.check(100 + 1_048_577));
        assert!(budget.fired());
    }

    #[test]
    fn an_element_origin_suspends_the_document_level_check() {
        // The root origin measures the whole document; an element's `begin`
        // supersedes it, and the closing span then reaches past the element
        // (the whitespace before `]` plus the bracket), so measuring it there
        // would reject a legal element. The element's own span check stays the
        // authority, and the next root's `begin` restores the document check.
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(0);
        budget.begin_element(1);
        assert!(budget.check_root_span(1 + 1_048_576 + 8).is_ok());
        assert!(!budget.fired(), "the document-level check ran anyway");
        assert!(budget.check_or_cap(1 + 1_048_577).is_err());
        assert!(budget.fired());
        budget.begin(2_000_000);
        assert!(!budget.fired());
        assert!(budget.check_root_span(2_000_000 + 1_048_576).is_ok());
        assert!(budget.check_root_span(2_000_000 + 1_048_577).is_err());
    }

    #[test]
    fn a_new_root_resets_the_account() {
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(0);
        assert!(!budget.check(1_048_577));
        budget.begin(2_000_000);
        assert!(!budget.fired());
        assert!(budget.check(2_000_000 + 1_048_576));
    }

    #[test]
    fn the_backstop_aborts_once_the_charge_passes_the_allowance() {
        // The backstop is coarser than the exact cap: it must not fire at the
        // cap (exact-span verdicts pass through it) and must abort a runaway
        // reader shortly after the cap plus the allowance.
        let mut budget = RootBudget::new(MAX_RECORD);
        budget.begin(0);
        let mut reader = super::BudgetedRead::new(io::repeat(b'a'), &budget);
        let mut out = [0u8; 4096];
        let mut served = 0u64;
        loop {
            match reader.read(&mut out) {
                Ok(0) => panic!("the backstop never fired"),
                Ok(n) => served += n as u64,
                Err(e) => {
                    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                    assert_eq!(e.to_string(), CAP_MESSAGE);
                    break;
                }
            }
        }
        assert!(
            served > MAX_RECORD,
            "the backstop fired at the cap: {served}"
        );
        assert!(
            served <= MAX_RECORD + CAP_ALLOWANCE + out.len() as u64,
            "the backstop ran far past the allowance: {served}"
        );
        assert!(budget.fired());
    }
}
