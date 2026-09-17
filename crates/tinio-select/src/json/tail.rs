//! The record stream every handler shares: the record-start offset and the done
//! latch, the terminal-error rule, and the drain loop behind
//! `RecordReader::next`.
//!
//! Terminal-error rule (pinned by `document_error_is_terminal_for_later_polls`
//! and `lines_error_is_terminal_for_later_polls`): a failing pull drops the
//! rows it produced and latches `done`, so a caller that keeps polling sees a
//! finished stream rather than rows from a root that already errored.

use crate::{error::Error, json::matcher::Matcher, row::Record};

/// The per-handler record-stream state: where the last record started, and
/// whether the stream is done.
pub(crate) struct ReaderTail {
    pub(crate) last_record_start: u64,
    pub(crate) done: bool,
}

impl ReaderTail {
    pub(crate) fn new() -> Self {
        Self {
            last_record_start: 0,
            done: false,
        }
    }

    /// Drain the matcher first, then one pull; a pull error is terminal.
    pub(crate) fn advance(
        &mut self,
        matcher: &mut Matcher,
        mut pull: impl FnMut(&mut Self, &mut Matcher) -> Result<(), Error>,
    ) -> Result<Option<Record>, Error> {
        loop {
            if let Some(record) = matcher.take_record() {
                return Ok(Some(record));
            }
            if self.done {
                return Ok(None);
            }
            if let Err(e) = pull(self, matcher) {
                matcher.drop_pending();
                self.done = true;
                return Err(e);
            }
        }
    }
}
