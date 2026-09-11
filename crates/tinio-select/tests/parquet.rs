//! Parquet reader integration test (feature `parquet`): the committed
//! fixture `tests/fixtures/select.parquet` read back through the crate's
//! public API.
//!
//! `src/parquet.rs`'s unit tests write and read their fixture inside one
//! process; this one reads bytes that live in the repo, so it pins that a
//! file *this run did not produce* still maps per the documented table — and
//! it is the same file the server-side parquet test and the `@parquet`
//! cucumber scenario read (both `include_bytes!` it), so the three layers
//! cannot drift onto different fixtures.
//!
//! Regenerate after a deliberate mapping change:
//! `cargo test -p tinio-select --features parquet --test parquet -- --ignored`

use std::{fs, io::Cursor, path::Path, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    },
    datatypes::{DataType, Field as ArrowField, Schema},
};
use parquet::arrow::ArrowWriter;
use rust_decimal::Decimal;
use tinio_select::{
    parquet::ParquetReader,
    record::RecordReader,
    row::{Columns, Field, Record, Value},
};

/// The fixture, relative to the package root (where the generator writes).
const FIXTURE_PATH: &str = "tests/fixtures/select.parquet";

/// The fixture bytes, embedded at compile time: the test needs no filesystem
/// and no writer — only the file's existence in the repo.
const FIXTURE: &[u8] = include_bytes!("fixtures/select.parquet");

/// The fixture's schema: one column per mapped value family, `score`
/// nullable so the committed bytes pin a present null.
fn schema() -> Schema {
    Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("name", DataType::Utf8, false),
        ArrowField::new("score", DataType::Float64, true),
        ArrowField::new("ok", DataType::Boolean, false),
        ArrowField::new("price", DataType::Decimal128(10, 2), false),
    ])
}

/// The fixture's rows — the assertion tables below are its mirror.
fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema()),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(3.5f64), None, Some(1.25)])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![1234i128, 567, 999])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef,
        ],
    )
    .unwrap()
}

/// The reader is the only public entry point, so every test goes through
/// `ParquetReader::new` — the signature the callers in `tinio-server` use.
fn read(projection: Vec<String>) -> Vec<Record> {
    let mut reader =
        ParquetReader::new(Cursor::new(FIXTURE.to_vec()), projection, 1 << 20).unwrap();
    let mut rows = Vec::new();
    while let Some(record) = reader.next().unwrap() {
        rows.push(record);
    }
    rows
}

/// One record's fields, with the variant pinned (a `Json` record would mean
/// the reader picked the wrong path).
fn fields(record: &Record) -> &[Field] {
    let Record::Parquet(Columns { fields, .. }) = record else {
        panic!("expected a parquet record, got {record:?}")
    };
    fields
}

#[test]
fn committed_fixture_maps_every_row() {
    let rows = read(Vec::new());
    assert_eq!(rows.len(), 3, "the fixture is three rows");
    let Record::Parquet(Columns { names, .. }) = &rows[0] else {
        panic!("expected a parquet record")
    };
    assert_eq!(**names, vec!["id", "name", "score", "ok", "price"]);
    assert_eq!(
        fields(&rows[0]),
        [
            Field::Present(Value::Int(1)),
            Field::Present(Value::String("alice".into())),
            // A float rides the raw text carrier (R6), never an eager Decimal.
            Field::Present(Value::RawNumber("3.5".into())),
            Field::Present(Value::Bool(true)),
            Field::Present(Value::Decimal(Decimal::new(1234, 2))),
        ]
    );
    // Row 2's score is a present `Null` — not MISSING, which is an absent
    // column only (the JSON-null semantics the reader documents).
    assert_eq!(
        fields(&rows[1]),
        [
            Field::Present(Value::Int(2)),
            Field::Present(Value::String("bob".into())),
            Field::Present(Value::Null),
            Field::Present(Value::Bool(false)),
            Field::Present(Value::Decimal(Decimal::new(567, 2))),
        ]
    );
    assert_eq!(
        fields(&rows[2]),
        [
            Field::Present(Value::Int(3)),
            Field::Present(Value::String("carol".into())),
            Field::Present(Value::RawNumber("1.25".into())),
            Field::Present(Value::Bool(true)),
            Field::Present(Value::Decimal(Decimal::new(999, 2))),
        ]
    );
}

#[test]
fn committed_fixture_projection_keeps_schema_order() {
    // A projection prunes what is *read*, not the column order: the batch
    // keeps the file's schema order, so a reversed reference list still
    // reads back `name` before `price`.
    let rows = read(vec!["price".into(), "name".into()]);
    let Record::Parquet(Columns { names, .. }) = &rows[0] else {
        panic!("expected a parquet record")
    };
    assert_eq!(**names, vec!["name", "price"]);
    assert_eq!(
        fields(&rows[2]),
        [
            Field::Present(Value::String("carol".into())),
            Field::Present(Value::Decimal(Decimal::new(999, 2))),
        ]
    );
}

/// Regenerates the committed fixture. Ignored on purpose: the file in the
/// repo is the input of this test, and rewriting it on every run would hide
/// an accidental encoding change instead of failing on it. Arrow's writer is
/// the tree's only parquet producer, so regeneration necessarily runs it.
#[test]
#[ignore = "regenerates tests/fixtures/select.parquet"]
fn regenerate_fixture() {
    let batch = batch();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_PATH);
    fs::write(&path, &buf).unwrap();
    assert_eq!(fs::read(&path).unwrap(), buf, "fixture written");
}
