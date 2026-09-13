//! Helpers shared by the `json` test modules: `run`/`run_err` (engine rows /
//! the engine's error) and the `s`/`raw` field constructors. Moved out of
//! `mod tests` verbatim so the migrated cases keep their original call shape.

use std::io::Cursor;

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

/// Engine rows over a JSON input, reader and engine composed.
pub(crate) fn run(sql: &str, ty: Type, input: &str) -> Vec<OutRow> {
    let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut r = Reader::new(
        Cursor::new(input.as_bytes().to_vec()),
        Params { ty },
        &plan.from,
    );
    let mut engine = Engine::new(plan);
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
    let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut r = Reader::new(
        Cursor::new(input.as_bytes().to_vec()),
        Params { ty },
        &plan.from,
    );
    let mut engine = Engine::new(plan);
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
