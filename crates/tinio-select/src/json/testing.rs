//! Helpers shared by the `json` test modules: `run`/`run_err` (engine rows /
//! the engine's error), the `drain` loop, `error_of` (the first terminal
//! reader error), the `s`/`raw` field constructors and the source/error
//! doubles. Moved out of `mod tests` verbatim so the migrated cases keep their
//! original call shape.

use std::io::{self, Cursor, Read};

use struson::reader::{JsonReader, JsonReaderPosition, JsonStreamReader, ReaderError};

use super::{Params, Reader, Type};
use crate::{
    engine::{Engine, OutRow},
    record::RecordReader,
    row::{Field, Record, Value},
    sql::parse,
};

/// Records `reader` yields, panicking with `sql` in the message. Both handler
/// test modules wrap this with their own reader construction.
pub(crate) fn drain<R: RecordReader>(sql: &str, mut reader: R) -> Vec<Record> {
    let mut out = Vec::new();
    while let Some(rec) = reader.next().unwrap_or_else(|e| panic!("{sql}: {e}")) {
        out.push(rec);
    }
    out
}

/// The first terminal error `reader` reports, draining the records that
/// precede it — a format error is streamed, so a handler yields rows before it
/// (deviation ④). A clean end is a bug in the case, so it panics with `ctx`.
pub(crate) fn error_of<R: RecordReader>(reader: &mut R, ctx: &str) -> crate::Error {
    loop {
        match reader.next() {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("{ctx}: ended cleanly"),
            Err(e) => return e,
        }
    }
}

/// The plan's reader and engine over `input`, composed: `run` and `run_err`
/// differ only in what they ask of the pairs.
fn composed(sql: &str, ty: Type, input: &str) -> (Reader<Cursor<Vec<u8>>>, Engine) {
    let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let reader = Reader::new(
        Cursor::new(input.as_bytes().to_vec()),
        Params { ty },
        &plan.from,
    );
    (reader, Engine::new(plan))
}

/// Engine rows over a JSON input, reader and engine composed.
pub(crate) fn run(sql: &str, ty: Type, input: &str) -> Vec<OutRow> {
    let (mut r, mut engine) = composed(sql, ty, input);
    let mut rows = Vec::new();
    while let Some(rec) = r.next().unwrap_or_else(|e| panic!("{sql}: {e}")) {
        if let Some(row) = engine.next(rec).unwrap_or_else(|e| panic!("{sql}: {e}")) {
            rows.push(row);
        }
    }
    rows
}

/// The engine error for `sql` over `input`, panicking on success.
pub(crate) fn run_err(sql: &str, ty: Type, input: &str) -> crate::Error {
    let (mut r, mut engine) = composed(sql, ty, input);
    let mut result = None;
    while let Some(rec) = r.next().unwrap_or_else(|e| panic!("{sql}: {e}")) {
        if let Err(e) = engine.next(rec) {
            result = Some(e);
            break;
        }
    }
    result.unwrap_or_else(|| panic!("{sql}: expected an engine error"))
}

pub(crate) fn s(v: &str) -> Field {
    Field::Present(Value::String(v.to_string()))
}

pub(crate) fn raw(v: &str) -> Field {
    Field::Present(Value::RawNumber(v.to_string()))
}

/// A source that serves `head` and then fails every further read, so a test can
/// land a failure on a chosen read: DOCUMENT's first `peek` (`empty_or`) or its
/// terminal trailing probe (`finish_document`), and LINES' read between two
/// lines (`start_line`, the one source read it makes outside `LineFraming`).
/// struson fills its 1 KiB buffer with one `read`, so a `head` under that size
/// is never re-read until it is exhausted.
pub(crate) struct FailsAfter {
    pub(crate) head: &'static [u8],
    pub(crate) at: usize,
}

impl Read for FailsAfter {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.at >= self.head.len() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "source failed",
            ));
        }
        let n = (self.head.len() - self.at).min(out.len());
        out[..n].copy_from_slice(&self.head[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// A reader position over an empty document: the baseline the error tests tell
/// apart by `data_pos` (`error.rs`'s `at`).
pub(crate) fn location() -> JsonReaderPosition {
    JsonStreamReader::new(b"".as_slice()).current_position(false)
}

/// The one `ReaderError` shape a real reader failure arrives in.
pub(crate) fn io_error(kind: io::ErrorKind, message: &str) -> ReaderError {
    ReaderError::IoError {
        error: io::Error::new(kind, message.to_owned()),
        location: location(),
    }
}
