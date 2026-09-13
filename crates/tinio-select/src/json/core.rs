//! State the LINES and DOCUMENT handlers share: the compiled FROM path, the
//! per-pull budget, the pending record queue, the record-start offset and the
//! terminal-error rule. Each handler owns its framing and its own `pull_*`;
//! everything that would otherwise be written twice lives here.

// Only the `pending_high` tripwire (test builds) needs a `Cell`.
#[cfg(test)]
use std::cell::Cell;
use std::{collections::VecDeque, io::Read, rc::Rc};

use serde::de::DeserializeSeed;
use serde_json::Value;

use crate::{
    error::Error,
    json::{
        framing::{Budget, BudgetedRead},
        matcher::{CompiledPath, Skip, child_into},
    },
    row::Record,
    sql::FromClause,
};

pub(crate) struct Core {
    pub(crate) budget: Rc<Budget>,
    pub(crate) path: CompiledPath,
    pub(crate) pending: VecDeque<Option<Value>>,
    pub(crate) failure: Option<Error>,
    pub(crate) last_record_start: u64,
    pub(crate) done: bool,
    /// Test-only high-water mark of `pending`, measured in **bytes** of the
    /// buffered `Value`s rather than in records: the memory tripwire that
    /// replaces the spec's counting allocator, which `unsafe_code = "forbid"`
    /// rules out. An item count would say nothing about memory — a single
    /// queued 1 MB value and 5 000 tiny ones are the same number and three
    /// orders of magnitude apart in size.
    #[cfg(test)]
    pub(crate) pending_high: Cell<usize>,
}

impl Core {
    pub(crate) fn new(from: &FromClause) -> Self {
        Self {
            budget: Rc::new(Budget::default()),
            path: CompiledPath::from_segments(&from.segments),
            pending: VecDeque::new(),
            failure: None,
            last_record_start: 0,
            done: false,
            #[cfg(test)]
            pending_high: Cell::new(0),
        }
    }

    /// One root pull: the seeds over `framing` with the budget reset around
    /// them, plus — when `probe` — the trailing-value probe on the **same**
    /// `Deserializer`. `IoRead` parks a peeked byte inside itself, already
    /// taken from the framing, so a fresh `Deserializer` for the probe would
    /// silently swallow one trailing byte (`5x` would be accepted as the
    /// record `5`). `Skip` validates the trailing value the way `from_slice` +
    /// `end()` used to.
    pub(crate) fn pull<F: Read>(
        &mut self,
        framing: &mut F,
        probe: bool,
    ) -> (serde_json::Result<()>, Option<serde_json::Result<()>>) {
        // Reset before serde sees the first byte: every byte this root
        // delivers is charged, and the next root starts from a full budget
        // (Global Constraints — charging per matched value would accumulate
        // across a root's matches and reject a document whose elements are
        // each well under the cap).
        self.budget.reset();
        let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
            framing,
            Rc::clone(&self.budget),
        ));
        let root = child_into(self.path.steps(), &mut self.failure, &mut self.pending)
            .deserialize(&mut de);
        let probe = (probe && root.is_ok()).then(|| Skip.deserialize(&mut de));
        (root, probe)
    }

    /// The `RecordReader::next` loop: drain `pending`, stop at `done`, and make
    /// a pull error terminal — the failing pull's rows are dropped, so a caller
    /// that keeps polling after `Err` sees a finished stream, not rows from a
    /// root that already errored. `pending` is drained before every pull, so
    /// anything queued when a pull fails belongs to that pull.
    pub(crate) fn advance(
        &mut self,
        mut pull: impl FnMut(&mut Self) -> Result<(), Error>,
    ) -> Result<Option<Record>, Error> {
        loop {
            if let Some(v) = self.pending.pop_front() {
                return Ok(Some(Record::Json(v)));
            }
            if self.done {
                return Ok(None);
            }
            if let Err(e) = pull(self) {
                self.pending.clear();
                self.done = true;
                return Err(e);
            }
            #[cfg(test)]
            self.touch_pending_high();
        }
    }

    #[cfg(test)]
    fn touch_pending_high(&self) {
        // Bytes, not records: `pending`'s memory, not its length.
        let n: usize = self
            .pending
            .iter()
            .flatten()
            .map(|v| v.to_string().len())
            .sum();
        if n > self.pending_high.get() {
            self.pending_high.set(n);
        }
    }
}
