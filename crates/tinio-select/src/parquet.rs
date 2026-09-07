//! Parquet input reader (feature-gated): the buffered parquet object →
//! `Record::Parquet` rows with type mapping and projection pruning.
//!
//! The storage read path has no seek, so the server buffers the
//! bound-checked object first (the size check — `max_parquet_bytes` — is
//! its 400 before streaming); this reader still refuses an over-bound
//! buffer (`ParquetTooLarge`, defense in depth). `parquet` 59 implements
//! `ChunkReader` for `File`, `bytes::Bytes` (plus `PushBuffers`/
//! `ColumnChunkData` in 59.3) — an `AsRef<[u8]>` blanket does not exist
//! (verified against the 59.3.0 source), so the pinned `Cursor<Vec<u8>>`
//! input moves into a zero-copy `Bytes` rather than being read through.
//!
//! Type mapping per the design: int/bool/string direct; float rides a raw
//! text carrier (`RawNumber`, review 2026-09-06b R6): finite floats render
//! verbatim through SELECT * and parse lazily on the 28-digit spine only
//! when a numeric operator consumes them (the CSV/JSON lazy rule — NaN/inf
//! are row-build errors, never a silently wrong number); decimal through
//! `parse_number`; timestamp → RFC 3339 ISO string (UTC, `Z` suffix);
//! list/struct → `Json` (JSON output renders natively, CSV output is the
//! engine's nested error). Nulls map to present `Null` (JSON null
//! semantics); MISSING means the column itself is absent. Projection: an
//! empty set reads every schema column (`Wild`/reference-free plans);
//! otherwise only the referenced columns are read, matched
//! case-insensitively against the top-level column names (identifier rules)
//! — unknown names are dropped and their lookup resolves MISSING.

use std::{io::Cursor, rc::Rc};

use arrow::{
    array::{
        Array, BooleanArray, Decimal128Array, Decimal256Array, FixedSizeListArray, LargeListArray,
        LargeStringArray, ListArray, PrimitiveArray, RecordBatch, StringArray, StructArray,
    },
    datatypes::{
        ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
        Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type,
        UInt64Type,
    },
};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
};
use rust_decimal::Decimal;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    engine::positional,
    error::Error,
    record::RecordReader,
    row::{Columns, Field, Record, Value, parse_number},
};

/// One parquet record stream: `Record::Parquet(Columns)` per row — the
/// batch's schema names are `Rc`-shared (S7).
pub struct ParquetReader {
    inner: ParquetRecordBatchReader,
    /// Current batch + row index; the batch's schema field names are the
    /// record's names (post-projection order = schema order) — `Rc`-shared
    /// into every row of the batch (S7).
    batch: Option<RecordBatch>,
    names: Rc<Vec<String>>,
    row: usize,
    /// The decoded-batch budget (X2) — same limit as the file-bytes bound.
    max_bytes: u64,
}

impl ParquetReader {
    pub fn new(
        input: Cursor<Vec<u8>>,
        projection: Vec<String>,
        max_bytes: u64,
    ) -> Result<Self, Error> {
        let len = input.get_ref().len() as u64;
        if len > max_bytes {
            return Err(Error::ParquetTooLarge);
        }
        let bytes = bytes::Bytes::from(input.into_inner());
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(from_parquet)?;
        let mask = projection_mask(&builder, &projection);
        let inner = builder
            .with_projection(mask)
            .build()
            .map_err(from_parquet)?;
        Ok(Self {
            inner,
            batch: None,
            names: Rc::new(Vec::new()),
            row: 0,
            max_bytes,
        })
    }
}

/// Projection mask: empty ⇒ all columns; otherwise the referenced names
/// matched case-insensitively against top-level parquet columns (the
/// identifier rules are lenient unquoted / exact quoted, and an extra read
/// never changes a row's values). Unknown names drop out — their lookup
/// resolves MISSING at row time. Duplicate matches (schema `a`/`A`) keep
/// both, so the row lookup reports ambiguity.
fn projection_mask(
    builder: &ParquetRecordBatchReaderBuilder<bytes::Bytes>,
    projection: &[String],
) -> ProjectionMask {
    if projection.is_empty() {
        return ProjectionMask::all();
    }
    // The engine's positional spine (`_N`) indexes the full record: a
    // pruned record would index the projected subset — the wrong column
    // or a starved MISSING — so any `_N` reference reads every column.
    if projection.iter().any(|n| positional(n).is_some()) {
        return ProjectionMask::all();
    }
    let schema = builder.parquet_schema();
    let mut names = Vec::new();
    for target in projection {
        for field in schema.root_schema().get_fields() {
            if field.name().eq_ignore_ascii_case(target) {
                names.push(field.name());
            }
        }
    }
    ProjectionMask::columns(schema, names)
}

impl RecordReader for ParquetReader {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        loop {
            let Some(batch) = &self.batch else {
                match self.inner.next() {
                    None => return Ok(None),
                    // The batch reader yields `ArrowError` (not `ParquetError`,
                    // the `from_parquet` input) — same surface text, distinct
                    // source type.
                    Some(Err(e)) => return Err(Error::Format(format!("parquet input: {e}"))),
                    Some(Ok(batch)) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        // X2: the file-bytes bound says nothing about the
                        // DECODED batch — a high-compression object (RLE,
                        // same-value columns) can decode to far more resident
                        // memory than its stored size. One batch is the
                        // reader's resident unit, so it is budgeted against
                        // the same limit.
                        if batch.get_array_memory_size() as u64 > self.max_bytes {
                            return Err(Error::ParquetTooLarge);
                        }
                        self.names = Rc::new(
                            batch
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect(),
                        );
                        self.batch = Some(batch);
                        self.row = 0;
                        continue;
                    }
                }
            };
            if self.row >= batch.num_rows() {
                self.batch = None;
                continue;
            }
            let fields = (0..batch.num_columns())
                .map(|j| {
                    Ok(Field::Present(arrow_value(
                        batch.column(j).as_ref(),
                        self.row,
                    )?))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            self.row += 1;
            return Ok(Some(Record::Parquet(Columns::new(
                fields,
                self.names.clone(),
            ))));
        }
    }
}

/// One top-level parquet cell → `Value`. A null is present `Null` (JSON
/// null semantics); MISSING is only an absent column (pruned or unknown).
fn arrow_value(array: &dyn Array, i: usize) -> Result<Value, Error> {
    if array.is_null(i) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        DataType::Int8 => Value::Int(primitive::<Int8Type>(array, i) as i64),
        DataType::Int16 => Value::Int(primitive::<Int16Type>(array, i) as i64),
        DataType::Int32 => Value::Int(primitive::<Int32Type>(array, i) as i64),
        DataType::Int64 => Value::Int(primitive::<Int64Type>(array, i)),
        DataType::UInt8 => Value::Int(primitive::<UInt8Type>(array, i) as i64),
        DataType::UInt16 => Value::Int(primitive::<UInt16Type>(array, i) as i64),
        DataType::UInt32 => Value::Int(primitive::<UInt32Type>(array, i) as i64),
        DataType::UInt64 => Value::Int(
            i64::try_from(primitive::<UInt64Type>(array, i))
                .map_err(|_| Error::Value("parquet value exceeds 64-bit signed integer".into()))?,
        ),
        DataType::Float32 => {
            Value::RawNumber(float_text(primitive::<Float32Type>(array, i) as f64)?)
        }
        DataType::Float64 => Value::RawNumber(float_text(primitive::<Float64Type>(array, i))?),
        DataType::Boolean => Value::Bool(downcast::<BooleanArray>(array).value(i)),
        DataType::Utf8 => Value::String(downcast::<StringArray>(array).value(i).to_string()),
        DataType::LargeUtf8 => {
            Value::String(downcast::<LargeStringArray>(array).value(i).to_string())
        }
        DataType::Decimal128(precision, scale) => Value::Decimal(decimal_value(
            *precision,
            decimal_text(
                &downcast::<Decimal128Array>(array).value(i).to_string(),
                *scale,
            ),
        )?),
        DataType::Decimal256(precision, scale) => Value::Decimal(decimal_value(
            *precision,
            decimal_text(
                &downcast::<Decimal256Array>(array).value(i).to_string(),
                *scale,
            ),
        )?),
        DataType::Timestamp(unit, _) => {
            let at = match unit {
                TimeUnit::Second => primitive::<TimestampSecondType>(array, i),
                TimeUnit::Millisecond => primitive::<TimestampMillisecondType>(array, i),
                TimeUnit::Microsecond => primitive::<TimestampMicrosecondType>(array, i),
                TimeUnit::Nanosecond => primitive::<TimestampNanosecondType>(array, i),
            };
            Value::String(timestamp_string(unit, at)?)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_) => Value::Json(Box::new(arrow_json(array, i, 0)?)),
        other => {
            return Err(Error::Format(format!(
                "parquet type not supported: {other}"
            )));
        }
    })
}

/// The depth ceiling of our own nested recursion (X3): a stacked-overflow is
/// not catchable, so the recursion errors cleanly before the stack is close
/// to exhausted (the upstream footer recursion is handled by the enlarged
/// engine stack, §1.1).
const MAX_NESTING: usize = 512;

/// One nested cell → JSON: lists/structs recurse, scalar cells reuse the
/// top-level mapping (same NaN/decimal-precision rules).
fn arrow_json(array: &dyn Array, i: usize, depth: usize) -> Result<serde_json::Value, Error> {
    if depth > MAX_NESTING {
        return Err(Error::Format("parquet nesting exceeds 512 levels".into()));
    }
    if array.is_null(i) {
        return Ok(serde_json::Value::Null);
    }
    Ok(match array.data_type() {
        DataType::List(_) => serde_json::Value::Array(list_items(
            downcast::<ListArray>(array).value(i).as_ref(),
            depth + 1,
        )?),
        DataType::LargeList(_) => serde_json::Value::Array(list_items(
            downcast::<LargeListArray>(array).value(i).as_ref(),
            depth + 1,
        )?),
        DataType::FixedSizeList(_, _) => serde_json::Value::Array(list_items(
            downcast::<FixedSizeListArray>(array).value(i).as_ref(),
            depth + 1,
        )?),
        DataType::Struct(_) => {
            let s = downcast::<StructArray>(array);
            let mut map = serde_json::Map::new();
            for (field, column) in s.fields().iter().zip(s.columns()) {
                map.insert(
                    field.name().to_string(),
                    arrow_json(column.as_ref(), i, depth + 1)?,
                );
            }
            serde_json::Value::Object(map)
        }
        _ => value_to_json(&arrow_value(array, i)?),
    })
}

/// The values of one list slot (the slice's own length = element count).
fn list_items(values: &dyn Array, depth: usize) -> Result<Vec<serde_json::Value>, Error> {
    (0..values.len())
        .map(|k| arrow_json(values, k, depth))
        .collect()
}

/// crate `Value` → JSON: numeric text parses as a JSON number literal
/// (arbitrary precision keeps it a `Number`, not a string).
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Decimal(d) => json_number(&d.normalize().to_string()),
        Value::RawNumber(s) => json_number(s),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Json(j) => j.as_ref().clone(),
    }
}

fn json_number(s: &str) -> serde_json::Value {
    serde_json::from_str(s).expect("decimal canonical text is valid JSON")
}

/// Float canonical text as a raw carrier (review 2026-09-06b R6, decision):
/// finite floats ride `RawNumber` — SELECT * renders the text verbatim and
/// the 28-digit spine fires only when a numeric operator consumes the value
/// (the CSV/JSON lazy rule). NaN/inf have no faithful JSON representation
/// and stay a row-build error here (never a silently wrong number).
fn float_text(v: f64) -> Result<String, Error> {
    if v.is_finite() {
        Ok(v.to_string())
    } else {
        Err(Error::Value(format!("invalid numeric value: {v}")))
    }
}

/// Fixed-point decimal via `parse_number` (the 28-digit spine); an arrow
/// decimal declared over 28 digits is refused before the value is built.
fn decimal_value(precision: u8, text: String) -> Result<Decimal, Error> {
    if precision > 28 {
        return Err(Error::Value(
            "parquet decimal precision exceeds 28 digits".into(),
        ));
    }
    parse_number(&text)
}

/// Fixed-point text from the raw integer + scale: scale ≥ 0 inserts the
/// point; a negative scale (arrow-rs allows it) appends zeros.
fn decimal_text(value: &str, scale: i8) -> String {
    let (neg, magnitude) = match value.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, value),
    };
    let sign = if neg { "-" } else { "" };
    if scale >= 0 {
        let scale = scale as usize;
        if scale == 0 {
            format!("{sign}{magnitude}")
        } else if magnitude.len() > scale {
            let point = magnitude.len() - scale;
            format!("{sign}{}.{}", &magnitude[..point], &magnitude[point..])
        } else {
            format!(
                "{sign}0.{}{}",
                "0".repeat(scale - magnitude.len()),
                magnitude
            )
        }
    } else {
        format!("{sign}{magnitude}{}", "0".repeat(-scale as usize))
    }
}

/// Unix epoch → RFC 3339 ISO string in UTC (`Z` suffix). time 0.3.55 has
/// only the second and nanosecond epoch constructors — milli/micro convert
/// to nanoseconds.
fn timestamp_string(unit: &TimeUnit, at: i64) -> Result<String, Error> {
    let dt = match unit {
        TimeUnit::Second => OffsetDateTime::from_unix_timestamp(at),
        TimeUnit::Millisecond => OffsetDateTime::from_unix_timestamp_nanos(at as i128 * 1_000_000),
        TimeUnit::Microsecond => OffsetDateTime::from_unix_timestamp_nanos(at as i128 * 1_000),
        TimeUnit::Nanosecond => OffsetDateTime::from_unix_timestamp_nanos(at as i128),
    }
    .map_err(|_| Error::Value("parquet timestamp out of range".into()))?;
    Ok(dt
        .format(&Rfc3339)
        .expect("rfc3339 formatting is infallible"))
}

/// The typed value at `i` for the matched `data_type` arm.
fn primitive<T: ArrowPrimitiveType>(array: &dyn Array, i: usize) -> T::Native {
    downcast::<PrimitiveArray<T>>(array).value(i)
}

/// Typed downcast pinned by the matched `data_type` arm — an internal
/// invariant, never a user-facing failure.
fn downcast<A: Array + 'static>(array: &dyn Array) -> &A {
    array
        .as_any()
        .downcast_ref::<A>()
        .expect("typed downcast follows data_type")
}

fn from_parquet(e: parquet::errors::ParquetError) -> Error {
    Error::Format(format!("parquet input: {e}"))
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int32Array,
            Int64Array, ListArray, RecordBatch, StringArray, StructArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        },
        buffer::OffsetBuffer,
        datatypes::{DataType, Field as ArrowField, Fields, Schema, TimeUnit},
    };
    use parquet::arrow::ArrowWriter;
    use rust_decimal::Decimal;

    use super::*;
    use crate::row::Record;

    const MAX_BYTES: u64 = 1024 * 1024;

    /// All design-listed parquet types, 2 rows: int, float, string, bool,
    /// decimal(≤28), timestamp in every unit, list, struct.
    fn fixture() -> RecordBatch {
        let schema = Schema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("small", DataType::Int32, true),
            ArrowField::new("score", DataType::Float64, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("price", DataType::Decimal128(10, 2), false),
            ArrowField::new("at_s", DataType::Timestamp(TimeUnit::Second, None), false),
            ArrowField::new(
                "at_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            ArrowField::new(
                "at_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            ArrowField::new(
                "at_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            ArrowField::new(
                "tags",
                DataType::List(Arc::new(ArrowField::new("element", DataType::Utf8, true))),
                false,
            ),
            ArrowField::new(
                "meta",
                DataType::Struct(Fields::from(vec![
                    ArrowField::new("a", DataType::Int64, false),
                    ArrowField::new("b", DataType::Boolean, false),
                ])),
                false,
            ),
            ArrowField::new("ok", DataType::Boolean, false),
        ]);
        let timestamp = 1_700_000_000; // 2023-11-14T22:13:20Z
        let id = Arc::new(Int64Array::from(vec![42i64, 43])) as ArrayRef;
        let small = Arc::new(Int32Array::from(vec![Some(-7i32), None])) as ArrayRef;
        let score = Arc::new(Float64Array::from(vec![3.5f64, 2.5])) as ArrayRef;
        let name = Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef;
        let price = Arc::new(
            Decimal128Array::from(vec![1234i128, 150])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef;
        let at_s = Arc::new(TimestampSecondArray::from(vec![timestamp, timestamp + 1])) as ArrayRef;
        let at_ms = Arc::new(TimestampMillisecondArray::from(vec![
            timestamp * 1_000,
            (timestamp + 1) * 1_000,
        ])) as ArrayRef;
        let at_us = Arc::new(TimestampMicrosecondArray::from(vec![
            timestamp * 1_000_000,
            (timestamp + 1) * 1_000_000,
        ])) as ArrayRef;
        let at_ns = Arc::new(TimestampNanosecondArray::from(vec![
            timestamp * 1_000_000_000,
            (timestamp + 1) * 1_000_000_000,
        ])) as ArrayRef;
        let tags = Arc::new(ListArray::new(
            Arc::new(ArrowField::new("element", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0_i32, 2, 2].into()),
            Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
            None,
        )) as ArrayRef;
        let meta = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    ArrowField::new("a", DataType::Int64, false),
                    ArrowField::new("b", DataType::Boolean, false),
                ]),
                vec![
                    Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
                    Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                ],
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let ok = Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef;
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                id, small, score, name, price, at_s, at_ms, at_us, at_ns, tags, meta, ok,
            ],
        )
        .unwrap()
    }

    /// One-column fixture with a custom column descriptor.
    fn one_column(field: ArrowField, array: ArrayRef) -> Vec<u8> {
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();
        write_parquet(&batch)
    }

    fn write_parquet(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn reader(bytes: Vec<u8>) -> ParquetReader {
        ParquetReader::new(Cursor::new(bytes), Vec::new(), MAX_BYTES).unwrap()
    }

    fn record(reader: &mut ParquetReader) -> Record {
        reader.next().unwrap().unwrap()
    }

    #[test]
    fn reads_mapped_types() {
        let mut r = reader(write_parquet(&fixture()));
        let Record::Parquet(Columns { fields, names }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert_eq!(
            *names,
            vec![
                "id", "small", "score", "name", "price", "at_s", "at_ms", "at_us", "at_ns", "tags",
                "meta", "ok"
            ]
        );
        assert_eq!(fields.len(), 12);
        assert_eq!(fields[0], Field::Present(Value::Int(42)));
        assert_eq!(fields[1], Field::Present(Value::Int(-7)));
        // Score is a float: the raw carrier (R6), not an eager Decimal.
        let Field::Present(Value::RawNumber(score)) = &fields[2] else {
            panic!("expected raw-number score, got {:?}", fields[2])
        };
        assert_eq!(score, "3.5");
        assert_eq!(fields[3], Field::Present(Value::String("alice".into())));
        assert_eq!(
            fields[4],
            Field::Present(Value::Decimal(Decimal::new(1234, 2)))
        );
        for f in &fields[5..9] {
            let Field::Present(Value::String(s)) = f else {
                panic!("expected timestamp string, got {f:?}")
            };
            assert_eq!(s, "2023-11-14T22:13:20Z");
        }
        let Field::Present(Value::Json(tags)) = &fields[9] else {
            panic!("expected json list, got {:?}", fields[9])
        };
        assert_eq!(tags.as_ref(), &serde_json::json!(["x", "y"]));
        let Field::Present(Value::Json(meta)) = &fields[10] else {
            panic!("expected json struct, got {:?}", fields[10])
        };
        assert_eq!(meta.as_ref(), &serde_json::json!({"a": 1, "b": true}));
        assert_eq!(fields[11], Field::Present(Value::Bool(true)));
        // Row 2: integer, present null, decimal with a fractional zero.
        let Record::Parquet(Columns { fields, .. }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert_eq!(fields[0], Field::Present(Value::Int(43)));
        assert_eq!(fields[1], Field::Present(Value::Null));
        assert_eq!(
            fields[4],
            Field::Present(Value::Decimal(Decimal::new(150, 2)))
        );
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn projection_prunes_to_requested_columns() {
        let bytes = write_parquet(&fixture());
        let mut r = ParquetReader::new(
            Cursor::new(bytes),
            vec!["id".into(), "price".into()],
            MAX_BYTES,
        )
        .unwrap();
        let Record::Parquet(Columns { fields, names }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert_eq!(*names, vec!["id", "price"]);
        assert_eq!(fields[0], Field::Present(Value::Int(42)));
        assert_eq!(
            fields[1],
            Field::Present(Value::Decimal(Decimal::new(1234, 2)))
        );
    }

    #[test]
    fn projection_matches_case_insensitively() {
        let bytes = write_parquet(&fixture());
        let mut r = ParquetReader::new(Cursor::new(bytes), vec!["ID".into()], MAX_BYTES).unwrap();
        let Record::Parquet(Columns { fields, names }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert_eq!(*names, vec!["id"]);
        assert_eq!(fields[0], Field::Present(Value::Int(42)));
    }

    #[test]
    fn positional_projection_reads_all_columns() {
        // `_N` references index the full record (the engine's positional
        // spine) — a pruned record would index the projected subset, so
        // `_2` would starve to MISSING. Fall back to every column.
        let bytes = write_parquet(&fixture());
        let mut r = ParquetReader::new(Cursor::new(bytes), vec!["_2".into()], MAX_BYTES).unwrap();
        let Record::Parquet(Columns { fields, names }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert_eq!(names.len(), 12);
        assert_eq!(fields[0], Field::Present(Value::Int(42)));
        assert_eq!(fields[1], Field::Present(Value::Int(-7)));
    }

    #[test]
    fn unknown_projection_name_reads_zero_columns() {
        let bytes = write_parquet(&fixture());
        let mut r = ParquetReader::new(Cursor::new(bytes), vec!["nope".into()], MAX_BYTES).unwrap();
        let Record::Parquet(Columns { fields, names }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert!(fields.is_empty() && names.is_empty());
        let Record::Parquet(Columns { fields, names }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert!(fields.is_empty() && names.is_empty());
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn decimal_precision_29_errors() {
        let array = Arc::new(
            Decimal128Array::from(vec![1234i128])
                .with_precision_and_scale(29, 2)
                .unwrap(),
        ) as ArrayRef;
        let bytes = one_column(
            ArrowField::new("p", DataType::Decimal128(29, 2), false),
            array,
        );
        match reader(bytes).next() {
            Err(Error::Value(msg)) => {
                assert_eq!(msg, "parquet decimal precision exceeds 28 digits")
            }
            other => panic!("expected value error, got {other:?}"),
        }
    }

    #[test]
    fn float_large_is_raw_number_carrier() {
        // R6: a float past the 28-digit spine (1e30) rides `RawNumber` at
        // row build — bare SELECT * never fails; the lazy parse breaks only
        // when a numeric operator consumes the value (row::parse_number).
        let text = format!("{}", 1e30_f64);
        assert_eq!(text.len(), 31, "1e30 must be a 31-digit carrier text");
        let array = Arc::new(Float64Array::from(vec![1e30])) as ArrayRef;
        let bytes = one_column(ArrowField::new("f", DataType::Float64, false), array);
        let mut r = reader(bytes);
        let Record::Parquet(Columns { fields, .. }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        let Field::Present(Value::RawNumber(carrier)) = &fields[0] else {
            panic!("expected raw number carrier, got {:?}", fields[0])
        };
        assert_eq!(carrier, &text);
        assert!(
            crate::row::parse_number(carrier).is_err(),
            "the carrier stays lazy — parsing it is the consumer's fault"
        );
    }

    #[test]
    fn float_nan_and_inf_error() {
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let array = Arc::new(Float64Array::from(vec![v])) as ArrayRef;
            let bytes = one_column(ArrowField::new("f", DataType::Float64, true), array);
            match reader(bytes).next() {
                Err(Error::Value(msg)) => {
                    assert!(msg.starts_with("invalid numeric value:"), "got {msg}")
                }
                other => panic!("expected value error for {v}, got {other:?}"),
            }
        }
    }

    #[test]
    fn unsupported_type_errors() {
        let array = Arc::new(BinaryArray::from(vec![Some(vec![1u8].as_slice())])) as ArrayRef;
        let bytes = one_column(ArrowField::new("blob", DataType::Binary, true), array);
        match reader(bytes).next() {
            Err(Error::Format(msg)) => {
                assert_eq!(msg, "parquet type not supported: Binary")
            }
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn over_memory_bound_errors() {
        let bytes = write_parquet(&fixture());
        match ParquetReader::new(
            Cursor::new(bytes.clone()),
            Vec::new(),
            bytes.len() as u64 - 1,
        ) {
            Err(Error::ParquetTooLarge) => {}
            Ok(_) => panic!("expected ParquetTooLarge"),
            Err(e) => panic!("expected ParquetTooLarge, got {e}"),
        }
    }

    #[test]
    fn memory_bound_exact_size_allowed() {
        let bytes = write_parquet(&fixture());
        let mut r =
            ParquetReader::new(Cursor::new(bytes.clone()), Vec::new(), bytes.len() as u64).unwrap();
        assert!(r.next().unwrap().is_some());
    }

    #[test]
    fn deep_nesting_is_a_format_error() {
        // X3: our own nested recursion caps at 512 levels — a deeper arrow
        // value errors cleanly instead of relying on the (uncatchable) stack
        // overflow the enlarged engine thread is the backstop for. The
        // fixture is built in memory: writing 600 nested levels through the
        // arrow writer itself overflows the test-process stack, which is
        // exactly the upstream recursion problem.
        let mut dtype = DataType::Int32;
        let mut array: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        for _ in 0..600 {
            let field = Arc::new(ArrowField::new("element", dtype, true));
            array = Arc::new(ListArray::new(
                field.clone(),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                array,
                None,
            ));
            dtype = DataType::List(field);
        }
        // The recursion runs on an enlarged-stack thread exactly like the
        // engine's: 513 live frames times a large arrow match arm exceeds a
        // default 8 MiB test-process stack even at the cap depth.
        let got = std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(move || arrow_json(array.as_ref(), 0, 0))
            .unwrap()
            .join()
            .unwrap();
        match got {
            Err(Error::Format(m)) => assert!(m.contains("nesting exceeds 512"), "got {m}"),
            other => panic!("expected the depth-cap format error, got {other:?}"),
        }
    }

    #[test]
    fn decoded_batch_over_budget_is_parquet_too_large() {
        // X2: the file-bytes bound says nothing about the DECODED batch —
        // two rows of the same 512 KiB text compress small but decode to
        // ~1 MiB+ of arrow memory (over MAX_BYTES). The batch budget must
        // fire even though the file fits.
        let big = "a".repeat(512 * 1024);
        let array = Arc::new(StringArray::from(vec![big.as_str(), big.as_str()])) as ArrayRef;
        let bytes = one_column(ArrowField::new("s", DataType::Utf8, false), array);
        let mut r = ParquetReader::new(Cursor::new(bytes.clone()), Vec::new(), MAX_BYTES).unwrap();
        match r.next() {
            Err(Error::ParquetTooLarge) => {}
            other => panic!("expected ParquetTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn multiple_batches_stream_continuously() {
        // Two written batches → the reader yields rows across the batch
        // boundary without skipping or repeating.
        let schema = Schema::new(vec![ArrowField::new("id", DataType::Int64, false)]);
        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch1.schema(), None).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.close().unwrap();
        let mut r = reader(buf);
        let mut ids = Vec::new();
        while let Some(Record::Parquet(Columns { fields, .. })) = r.next().unwrap() {
            let Field::Present(Value::Int(i)) = fields[0] else {
                panic!("expected int field, got {:?}", fields[0])
            };
            ids.push(i);
        }
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }
}
