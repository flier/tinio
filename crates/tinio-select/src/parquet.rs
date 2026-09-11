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
//! `parse_number`; timestamp and date → RFC 3339 ISO string (UTC, `Z`
//! suffix — a date is midnight of that day); ENUM (a top-level
//! BYTE_ARRAY annotation only) → checked-UTF-8 string; list/struct → `Json`
//! (JSON output renders natively, CSV output is the engine's nested error).
//! Nulls map to present `Null` (JSON null semantics); MISSING means the
//! column itself is absent. Projection: an empty set reads every schema column
//! (`Wild`/reference-free plans); otherwise only the referenced columns are
//! read, matched case-insensitively against the top-level column names
//! (identifier rules) — unknown names are dropped and their lookup resolves
//! MISSING.

use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    rc::Rc,
    str::from_utf8,
};

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array, FixedSizeListArray,
        LargeListArray, LargeStringArray, ListArray, PrimitiveArray, RecordBatch, StringArray,
        StructArray,
    },
    datatypes::{
        ArrowPrimitiveType, DataType, Date32Type, Date64Type, Float32Type, Float64Type, Int8Type,
        Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
        UInt16Type, UInt32Type, UInt64Type,
    },
};
use bytes::Bytes;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    },
    basic::{ConvertedType, LogicalType},
    schema::types::SchemaDescriptor,
};
use rust_decimal::Decimal;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

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
    /// Top-level root field names carrying the parquet ENUM annotation,
    /// captured once in `new` (never per row or per batch): arrow erases the
    /// annotation into `DataType::Binary`, so reader construction is the last
    /// point that can still tell an ENUM from an unannotated BYTE_ARRAY.
    enum_columns: HashSet<String>,
    /// Per-batch ENUM hints, positionally aligned with the batch's columns —
    /// resolved once when the batch is adopted, in the same pass as `names`,
    /// so the per-row loop indexes instead of hashing a name per cell.
    enum_hints: Vec<EnumHint>,
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
        let bytes = Bytes::from(input.into_inner());
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(from_parquet)?;
        let mask = projection_mask(&builder, &projection);
        let enum_columns = enum_columns(builder.parquet_schema());
        let inner = builder
            .with_projection(mask)
            .build()
            .map_err(from_parquet)?;
        Ok(Self {
            inner,
            batch: None,
            names: Rc::new(Vec::new()),
            enum_columns,
            enum_hints: Vec::new(),
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
    builder: &ParquetRecordBatchReaderBuilder<Bytes>,
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

/// The top-level parquet root fields annotated ENUM, by name.
///
/// `parquet` 59.3.0 maps the ENUM annotation — `LogicalType::Enum` and
/// `ConvertedType::ENUM` alike — to arrow `DataType::Binary`
/// (`arrow/schema/primitive.rs` lines 281, 288), the very type it gives an
/// unannotated BYTE_ARRAY, BSON, Geometry/Geography and `_Unknown`: the arrow
/// type alone cannot tell them apart, and `arrow_value` sees only that. The
/// annotation is still legible here, off the parquet schema the builder holds,
/// so it is read once at construction and the per-row path only looks a name up.
///
/// A name is kept only when EVERY top-level root field of that name is
/// annotated: a duplicate name pairing an ENUM with an unannotated `Binary`
/// would otherwise silently re-type the latter as a string. Nested leaves (a
/// longer column path) are dropped — the top-level mapping never sees them, so
/// an ENUM inside a list or struct stays refused (see `arrow_json`).
fn enum_columns(schema: &SchemaDescriptor) -> HashSet<String> {
    let mut hints: HashMap<&str, bool> = HashMap::new();
    for column in schema.columns() {
        let parts = column.path().parts();
        if parts.len() != 1 {
            continue;
        }
        let annotated = column.converted_type() == ConvertedType::ENUM
            || matches!(column.logical_type_ref(), Some(LogicalType::Enum));
        hints
            .entry(parts[0].as_str())
            .and_modify(|all| *all &= annotated)
            .or_insert(annotated);
    }
    hints
        .into_iter()
        .filter(|&(_, annotated)| annotated)
        .map(|(name, _)| name.to_string())
        .collect()
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
                        // Resolve the ENUM hint here, in the same pass and the
                        // same order as `names`: it is fixed per batch, so the
                        // per-cell work collapses to an index.
                        let enum_hints: Vec<EnumHint> = batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| EnumHint::of(self.enum_columns.contains(f.name())))
                            .collect();
                        self.names = Rc::new(
                            batch
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect(),
                        );
                        self.enum_hints = enum_hints;
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
                    // The batch's field names ARE the parquet root field names
                    // (`complex.rs` `convert_field`: `Field::new(parquet_type
                    // .name(), ..)`, and an embedded `ARROW:schema` hint is
                    // rejected unless its names match), so `enum_hints` — built
                    // positionally from those names when the batch was adopted
                    // — lines up with `batch.column(j)`: order- and
                    // projection-independent, and a field the mask filtered out
                    // simply never asks.
                    Ok(Field::Present(arrow_value(
                        batch.column(j).as_ref(),
                        self.row,
                        self.enum_hints[j],
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
/// The hint rides in beside the array because it is not recoverable from it:
/// arrow has already flattened the parquet annotation into `DataType::Binary`.
/// It is [`EnumHint::Annotated`] only for a top-level ENUM-annotated column
/// (`enum_columns`) — every other `Binary` producer keeps the refusal.
fn arrow_value(array: &dyn Array, i: usize, enum_hint: EnumHint) -> Result<Value, Error> {
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
        // AWS reads an ENUM as a string. The guard is load-bearing: without the
        // annotation `Binary` means an unannotated BYTE_ARRAY, BSON, Geometry,
        // Geography or `_Unknown` (primitive.rs lines 281-289), all of which
        // must keep failing in the catch-all below — never silently become text.
        DataType::Binary if enum_hint == EnumHint::Annotated => {
            Value::String(enum_string(array, i)?)
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
        DataType::Date32 => Value::String(date_string_from_days(
            primitive::<Date32Type>(array, i) as i64,
        )?),
        DataType::Date64 => {
            Value::String(date_string_from_millis(primitive::<Date64Type>(array, i))?)
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

/// Whether the column a cell came from carries the parquet ENUM annotation
/// (`enum_columns`). A nested cell is always [`EnumHint::Unannotated`]: it has
/// no top-level root field to resolve a hint against, so an ENUM inside a
/// list or struct keeps the `Binary` catch-all (see `arrow_json`). Nested
/// ENUM support is deliberately out of scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnumHint {
    Annotated,
    Unannotated,
}

impl EnumHint {
    /// `Annotated` exactly when `enum_columns` holds the column's name.
    fn of(annotated: bool) -> Self {
        if annotated {
            Self::Annotated
        } else {
            Self::Unannotated
        }
    }
}

/// One ENUM cell as text. The annotation says these bytes ARE a string, so an
/// invalid sequence is a corrupt value and is refused — `from_utf8`, not
/// `from_utf8_lossy`, whose U+FFFD substitution would smuggle in exactly the
/// silently-wrong value `float_text` refuses for NaN/inf.
fn enum_string(array: &dyn Array, i: usize) -> Result<String, Error> {
    from_utf8(downcast::<BinaryArray>(array).value(i))
        .map(str::to_owned)
        .map_err(|e| Error::Format(format!("parquet enum is not valid UTF-8: {e}")))
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
        // A nested cell carries `EnumHint::Unannotated`: `enum_columns` keys
        // top-level root fields, and this recursion has no path back to one —
        // so an ENUM inside a list/struct keeps refusing rather than decoding
        // by accident.
        _ => value_to_json(&arrow_value(array, i, EnumHint::Unannotated)?),
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

/// Days since the Unix epoch → RFC 3339 midnight UTC. Chosen over a bare
/// `YYYY-MM-DD` so DATE and TIMESTAMP share one shape on the service's
/// string spine until a real timestamp type exists (spec Q4).
///
/// The offset is built in *seconds*, not nanoseconds: a nanosecond
/// intermediate is an i64, which would cap the range at ±292 years of the
/// epoch and fail a legal post-2262 DATE (a `Date32` day count is an i32, and
/// a `Date64` millisecond count divides to ~±10¹¹ days, so neither needs that
/// truncation). With seconds, `time`'s own date span is the only bound: past
/// it — or on a date RFC 3339 cannot spell at all, i.e. a negative year — the
/// value is a `Format` error, never a panic or a wrap.
///
/// `Duration::seconds` is total (i64 seconds is the representation, unlike
/// the panicking `days`), so the only arithmetic that can overflow is the day
/// → second conversion, which `checked_mul` catches.
fn date_string_from_days(days: i64) -> Result<String, Error> {
    let out_of_range = || Error::Format("parquet date out of range".into());
    let offset = Duration::seconds(days.checked_mul(86_400).ok_or_else(out_of_range)?);
    let dt = OffsetDateTime::UNIX_EPOCH
        .checked_add(offset)
        .ok_or_else(out_of_range)?;
    dt.format(&Rfc3339)
        .map_err(|e| Error::Format(format!("parquet date format: {e}")))
}

/// Milliseconds → that day's midnight UTC. `div_euclid` floors toward
/// negative infinity, so a pre-epoch value lands on the right day.
fn date_string_from_millis(ms: i64) -> Result<String, Error> {
    date_string_from_days(ms.div_euclid(86_400_000))
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
            ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
            DictionaryArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
            ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        },
        buffer::OffsetBuffer,
        datatypes::{DataType, Field as ArrowField, Fields, Schema, TimeUnit},
    };
    use bytes::Bytes;
    use parquet::{
        arrow::ArrowWriter,
        data_type::{ByteArray, ByteArrayType},
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        schema::parser::parse_message_type,
    };
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

    /// A parquet file from a raw message-type string plus its leaf columns'
    /// BYTE_ARRAY values, in schema order.
    ///
    /// `ArrowWriter` derives the parquet schema from an arrow `DataType`, and
    /// arrow has no ENUM data type — it cannot write the annotation under test
    /// at all. The low-level writer takes the parquet schema directly:
    /// `parse_message_type` → `SerializedFileWriter` → one `write_batch` per
    /// column. Every fixture here is `required` at each level, so there are no
    /// definition or repetition levels to supply.
    fn raw_file(message: &str, columns: &[&[&[u8]]]) -> Vec<u8> {
        let schema = parse_message_type(message).unwrap();
        let props = Arc::new(WriterProperties::builder().build());
        let mut buf = Vec::new();
        let mut writer = SerializedFileWriter::new(&mut buf, Arc::new(schema), props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        for values in columns {
            let mut column = row_group.next_column().unwrap().unwrap();
            let bytes: Vec<ByteArray> = values.iter().map(|v| ByteArray::from(*v)).collect();
            column
                .typed::<ByteArrayType>()
                .write_batch(&bytes, None, None)
                .unwrap();
            column.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();
        buf
    }

    /// One `(ENUM)`-annotated `required binary e` column. The unannotated
    /// BYTE_ARRAY it must not be confused with is a plain `raw_file` at the
    /// call site that wants one; an `annotation` parameter selecting between
    /// the two was dead, since both callers here always annotate.
    fn enum_file(values: &[&[u8]]) -> Vec<u8> {
        raw_file("message s { required binary e (ENUM); }", &[values])
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

        // The AWS-listed parquet type table, one case per type: INT32/INT64
        // are the fixture's `small`/`id`, and DECIMAL/LIST/STRING/TIMESTAMP
        // are `price`/`tags`/`name`/`at_*` — this list adds INT8/INT16
        // (mapped all along, untested until now) and DATE, so the AWS table
        // reads as one list. ENUM is the one listed type this loop cannot
        // carry: `ArrowWriter` needs an arrow data type to write from, and
        // arrow has none for ENUM — its fixture comes from the low-level
        // writer instead, in `enum_column_reads_as_string` below.
        let day_2024 = 19_723; // 2024-01-01, days since the Unix epoch
        let cases: Vec<(ArrowField, ArrayRef, Value)> = vec![
            (
                ArrowField::new("i8", DataType::Int8, false),
                Arc::new(Int8Array::from(vec![-8i8])) as ArrayRef,
                Value::Int(-8),
            ),
            (
                ArrowField::new("i16", DataType::Int16, false),
                Arc::new(Int16Array::from(vec![-16i16])) as ArrayRef,
                Value::Int(-16),
            ),
            (
                ArrowField::new("d32", DataType::Date32, false),
                Arc::new(Date32Array::from(vec![day_2024])) as ArrayRef,
                Value::String("2024-01-01T00:00:00Z".into()),
            ),
            (
                ArrowField::new("d64", DataType::Date64, false),
                Arc::new(Date64Array::from(vec![day_2024 as i64 * 86_400_000])) as ArrayRef,
                Value::String("2024-01-01T00:00:00Z".into()),
            ),
        ];
        for (field, array, want) in cases {
            let mut r = reader(one_column(field, array));
            let Record::Parquet(Columns { fields, .. }) = record(&mut r) else {
                panic!("expected parquet record")
            };
            assert_eq!(fields[0], Field::Present(want));
            assert_eq!(r.next().unwrap(), None);
        }
        // `Dictionary` is neither an AWS-listed type nor a Parquet ENUM: it
        // keeps the catch-all, a `Format` error rather than a silent value.
        let dict = DictionaryArray::new(
            Int32Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["x"])),
        );
        match reader(one_column(
            ArrowField::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Arc::new(dict) as ArrayRef,
        ))
        .next()
        {
            Err(Error::Format(msg)) => {
                assert_eq!(msg, "parquet type not supported: Dictionary(Int32, Utf8)")
            }
            other => panic!("expected format error, got {other:?}"),
        }
    }

    /// The single value of a one-column date fixture, read through the reader
    /// path — the fixture plumbing the `DATE` cases share.
    fn date_value(data_type: DataType, array: ArrayRef) -> Field {
        let bytes = one_column(ArrowField::new("d", data_type, true), array);
        let Record::Parquet(Columns { fields, .. }) = record(&mut reader(bytes)) else {
            panic!("expected parquet record")
        };
        fields.into_iter().next().unwrap()
    }

    #[test]
    fn date32_maps_to_midnight_utc() {
        // 19723 days since the Unix epoch = 2024-01-01.
        let array = Arc::new(Date32Array::from(vec![Some(19723)])) as ArrayRef;
        assert_eq!(
            date_value(DataType::Date32, array),
            Field::Present(Value::String("2024-01-01T00:00:00Z".into()))
        );
    }

    #[test]
    fn date32_pre_epoch_is_negative() {
        // -1 = 1969-12-31: a negative day count renders as the day before
        // the epoch. No division happens on this path (days convert to a
        // whole-day offset by multiplication), so it pins the sign, not the
        // floor rule — `Date64`'s, and the discriminating case for it, is
        // `date64_floors_to_the_day`.
        let array = Arc::new(Date32Array::from(vec![Some(-1)])) as ArrayRef;
        assert_eq!(
            date_value(DataType::Date32, array),
            Field::Present(Value::String("1969-12-31T00:00:00Z".into()))
        );
    }

    #[test]
    fn dates_past_2262_still_read() {
        // The offset is built in seconds. An i64 *nanosecond* intermediate
        // capped the range at ±292 years of the epoch, so a legal DATE past
        // 2262-04-11 failed the whole read; `time`'s own date span is now the
        // bound. The day counts come from `time`'s calendar, so the assertion
        // pins the day→date conversion rather than a hand-computed constant.
        let day_of = |year: i32| {
            time::Date::from_calendar_date(year, time::Month::January, 1)
                .expect("a valid year")
                .to_julian_day()
        };
        let epoch = day_of(1970);
        for (year, want) in [
            (2300, "2300-01-01T00:00:00Z"),
            (9999, "9999-01-01T00:00:00Z"),
        ] {
            let days = day_of(year) - epoch;
            assert_eq!(
                arrow_value(&Date32Array::from(vec![days]), 0, EnumHint::Unannotated).unwrap(),
                Value::String(want.into()),
                "year {year}"
            );
        }
    }

    #[test]
    fn date64_floors_to_the_day() {
        // A non-midnight millisecond value must yield that day's midnight: the
        // type disclaims time-of-day, so the arm enforces it rather than leaking
        // whatever the input happened to carry.
        let noon = 19723i64 * 86_400_000 + 12 * 3_600_000;
        let array = Arc::new(Date64Array::from(vec![Some(noon)])) as ArrayRef;
        assert_eq!(
            date_value(DataType::Date64, array),
            Field::Present(Value::String("2024-01-01T00:00:00Z".into()))
        );
        // The reader path cannot carry a non-midnight Date64: arrow-rs's
        // writer stores the type as whole days (`arrow_writer/mod.rs`:
        // `x / 86_400_000`) and its reader scales back (`array_reader/
        // primitive_array.rs`: `x as i64 * 86_400_000`), so what reaches the
        // arm is day-aligned whatever the input carried. The floor rule is
        // pinned on the arm itself instead, pre-epoch case included — that is
        // the one truncation would push to the next day.
        for (ms, want) in [
            (noon, "2024-01-01T00:00:00Z"),
            (-3_600_000, "1969-12-31T00:00:00Z"),
        ] {
            assert_eq!(
                arrow_value(&Date64Array::from(vec![ms]), 0, EnumHint::Unannotated).unwrap(),
                Value::String(want.into()),
                "millis {ms}"
            );
        }
    }

    #[test]
    fn out_of_range_dates_are_format_errors() {
        // Never a panic or a wrap. The message is pinned, not just the variant:
        // the catch-all is also `Error::Format`, so a variant-only check cannot
        // tell the date arm from a read that never reached it.
        let array = Arc::new(Date32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let bytes = one_column(ArrowField::new("d", DataType::Date32, true), array);
        // ParquetReader::next is `Result<Option<Record>, Error>` — the error is
        // the OUTER variant, not nested in an Option.
        match reader(bytes).next() {
            Err(Error::Format(m)) => assert_eq!(m, "parquet date out of range"),
            other => panic!("expected format error, got {other:?}"),
        }
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

    /// The one place a name lookup could mis-decode. A parquet schema may
    /// repeat a top-level name, and arrow's batch then carries two fields both
    /// named `e` — so a per-name `true` would hand the unannotated column the
    /// ENUM column's decode. The hint is dropped for the whole name instead,
    /// and the file refuses, which is the honest answer for a schema whose
    /// columns cannot be told apart by name.
    #[test]
    fn a_duplicate_name_mixing_enum_with_binary_refuses_both() {
        let bytes = raw_file(
            "message s { required binary e (ENUM); required binary e; }",
            &[&[b"alpha"], &[&[0x01]]],
        );
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone())).unwrap();
        assert_eq!(builder.schema().fields().len(), 2, "the duplicate survived");
        assert!(enum_columns(builder.parquet_schema()).is_empty());
        match reader(bytes).next() {
            Err(Error::Format(msg)) => assert_eq!(msg, "parquet type not supported: Binary"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    /// `(ENUM)` on a top-level BYTE_ARRAY: AWS reads it as a string, and this
    /// reader decodes the bytes as UTF-8 — arrow has already flattened the
    /// annotation into `DataType::Binary`, so the parquet schema is the only
    /// place the distinction survives.
    #[test]
    fn enum_column_reads_as_string() {
        let mut r = reader(enum_file(&[b"alpha", b"beta"]));
        for want in ["alpha", "beta"] {
            let Record::Parquet(Columns { fields, .. }) = record(&mut r) else {
                panic!("expected parquet record")
            };
            assert_eq!(fields[0], Field::Present(Value::String(want.into())));
        }
        assert_eq!(r.next().unwrap(), None);
    }

    /// A corrupt ENUM is refused, not repaired: `from_utf8_lossy` would answer
    /// `a\u{fffd}b`, a value the file never held.
    #[test]
    fn enum_invalid_utf8_is_a_format_error() {
        let bytes = enum_file(&[&[0x61, 0xff, 0x62]]);
        match reader(bytes).next() {
            Err(Error::Format(msg)) => {
                assert!(
                    msg.starts_with("parquet enum is not valid UTF-8:"),
                    "got {msg}"
                )
            }
            other => panic!("expected format error, got {other:?}"),
        }
    }

    /// The guard that keeps the ENUM arm from being a blanket `Binary` arm.
    /// Both columns reach arrow as `DataType::Binary` — `(Some(LogicalType::
    /// Enum))` and `(None, ConvertedType::NONE)` are adjacent lines of
    /// `primitive.rs` (288 and 281's neighbours) — so only the name lookup
    /// separates them: `e (ENUM)` reads as a string, the bare BYTE_ARRAY
    /// `blob` beside it keeps the catch-all, projected either way.
    #[test]
    fn enum_does_not_leak_onto_an_unannotated_binary() {
        let bytes = raw_file(
            "message s { required binary e (ENUM); required binary blob; }",
            &[&[b"alpha"], &[&[0x01]]],
        );

        // Projected to the ENUM column alone: a string.
        let mut r =
            ParquetReader::new(Cursor::new(bytes.clone()), vec!["e".into()], MAX_BYTES).unwrap();
        let Record::Parquet(Columns { fields, names }) = record(&mut r) else {
            panic!("expected parquet record")
        };
        assert_eq!(*names, vec!["e"]);
        assert_eq!(fields[0], Field::Present(Value::String("alpha".into())));
        assert_eq!(r.next().unwrap(), None);

        // Projected to the Binary column alone: still refused.
        let mut r =
            ParquetReader::new(Cursor::new(bytes.clone()), vec!["blob".into()], MAX_BYTES).unwrap();
        match r.next() {
            Err(Error::Format(msg)) => assert_eq!(msg, "parquet type not supported: Binary"),
            other => panic!("expected format error, got {other:?}"),
        }

        // Both in one batch: the ENUM still reads, the row still fails on the
        // Binary — the hint is per column, never per batch.
        match reader(bytes).next() {
            Err(Error::Format(msg)) => assert_eq!(msg, "parquet type not supported: Binary"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    /// Nested ENUM is out of scope, deliberately: `enum_columns` keys top-level
    /// root fields, so an ENUM under a group resolves no hint and keeps the
    /// refusal rather than being decoded by a guess.
    #[test]
    fn nested_enum_is_still_refused() {
        let bytes = raw_file(
            "message s { required group meta { required binary e (ENUM); } }",
            &[&[b"alpha"]],
        );
        match reader(bytes).next() {
            Err(Error::Format(msg)) => assert_eq!(msg, "parquet type not supported: Binary"),
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
