//! Output serializers: one `OutRow` to CSV or JSON bytes per record.
//!
//! CSV rows are value-only (no output header line) and pad every key column —
//! `Field::Missing` → empty string, and ragged rows (`vals` shorter than the
//! key width) pad to the keys' length. JSON records are `{"k": v, ...}` in
//! key order: MISSING omits its key, all-missing → `{}`, `Decimal` renders
//! its normalized text (still a valid JSON number), `RawNumber` its token
//! verbatim (arbitrary-precision passthrough), `String`/`Json` via serde_json.
//! A JSON value under CSV output is the SELECT * matrix cell: compact JSON
//! text in that cell (`display`) — CSV quoting/escaping applies to the cell.

use csv::{QuoteStyle, Terminator, WriterBuilder};

use crate::SelectError;
use crate::engine::OutRow;
use crate::row::{Field, Value, display};

/// Output serialization mode (S3 Select `OutputSerialization`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputMode {
    Csv(CsvOutputParams),
    Json(JsonOutputParams),
}

/// CSV output options (S3 Select `OutputSerialization.CSV`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvOutputParams {
    pub field_delimiter: u8,
    pub record_delimiter: u8,
    pub quote: u8,
    pub escape: u8,
    pub quote_fields: QuoteFields,
}

/// AWS defaults: `,`, `\n`, `"`, `"`, ASNEEDED.
impl Default for CsvOutputParams {
    fn default() -> Self {
        Self {
            field_delimiter: b',',
            record_delimiter: b'\n',
            quote: b'"',
            escape: b'"',
            quote_fields: QuoteFields::AsNeeded,
        }
    }
}

/// Field quoting policy (S3 Select `QuoteFields`, default ASNEEDED).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteFields {
    AsNeeded,
    Always,
}

/// JSON output options (S3 Select `OutputSerialization.JSON`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonOutputParams {
    pub record_delimiter: u8,
}

/// AWS default: `\n`.
impl Default for JsonOutputParams {
    fn default() -> Self {
        Self {
            record_delimiter: b'\n',
        }
    }
}

/// One `OutRow` in `mode`, ready to pack into an event.
pub fn serialize_row(mode: &OutputMode, row: &OutRow) -> Result<Vec<u8>, SelectError> {
    match mode {
        OutputMode::Csv(params) => serialize_csv(params, row),
        OutputMode::Json(params) => Ok(serialize_json(params, row)),
    }
}

/// CSV row via the `csv` writer: values in key order under the params'
/// quoting/escaping; `Json` values render as compact JSON text in the cell
/// (the SELECT * matrix: JSON→CSV nested values land in the cell as
/// `to_string`, `{"a":1}` — CSV-side quoting/escaping applies, so the
/// cell's own `"` are escaped per the params).
/// Double-quoting is off so the configured escape actually prefixes quote
/// chars — with the default escape==quote the bytes equal doubling.
fn serialize_csv(params: &CsvOutputParams, row: &OutRow) -> Result<Vec<u8>, SelectError> {
    let mut b = WriterBuilder::new();
    b.delimiter(params.field_delimiter)
        .terminator(Terminator::Any(params.record_delimiter))
        .quote(params.quote)
        .escape(params.escape)
        .double_quote(false)
        .quote_style(match params.quote_fields {
            QuoteFields::AsNeeded => QuoteStyle::Necessary,
            QuoteFields::Always => QuoteStyle::Always,
        });
    let mut wtr = b.from_writer(Vec::new());
    let fields = row.keys.iter().enumerate().map(|(i, _)| match row.vals.get(i) {
        Some(Field::Present(v)) => display(v),
        _ => String::new(),
    });
    wtr.write_record(fields).map_err(from_csv)?;
    wtr.into_inner()
        .map_err(|e| SelectError::Io(e.into_error()))
}

/// JSON row: `{"k": v, ...}`, MISSING omits the key (zip stops at the shorter
/// — trailing missing keys fall off, `vals` past `keys` have no name either).
fn serialize_json(params: &JsonOutputParams, row: &OutRow) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'{');
    let mut first = true;
    for (key, field) in row.keys.iter().zip(&row.vals) {
        let Field::Present(v) = field else {
            continue;
        };
        if !first {
            out.push(b',');
        }
        first = false;
        out.extend_from_slice(json_text(key).as_bytes());
        out.push(b':');
        json_value(&mut out, v);
    }
    out.push(b'}');
    out.push(params.record_delimiter);
    out
}

/// Quoted JSON text — the key form and the String value form.
fn json_text(s: &str) -> String {
    serde_json::to_string(s).expect("json string serialization is infallible")
}

/// One JSON value: `Null`/`Bool`/`Int`/`Decimal`/`RawNumber` unquoted
/// (Decimal via its normalized text, RawNumber verbatim), `String` quoted,
/// `Json` nested natively.
fn json_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::String(s) => out.extend_from_slice(json_text(s).as_bytes()),
        Value::Json(j) => out.extend_from_slice(
            serde_json::to_string(j)
                .expect("json value serialization is infallible")
                .as_bytes(),
        ),
        Value::Decimal(_) | Value::Int(_) | Value::RawNumber(_) => {
            out.extend_from_slice(display(v).as_bytes());
        }
    }
}

/// `csv` writer error mapping: I/O passes through; the rest is a format
/// error — practically unreachable for `&str` fields into a `Vec`.
fn from_csv(e: csv::Error) -> SelectError {
    let msg = format!("csv output: {e}");
    match e.into_kind() {
        csv::ErrorKind::Io(io) => SelectError::Io(io),
        _ => SelectError::Format(msg),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;

    fn row(keys: Vec<&str>, vals: Vec<Field>) -> OutRow {
        OutRow {
            keys: keys.into_iter().map(String::from).collect(),
            vals,
        }
    }

    fn present(v: Value) -> Field {
        Field::Present(v)
    }

    fn csv_mode() -> OutputMode {
        OutputMode::Csv(CsvOutputParams::default())
    }

    fn json_mode() -> OutputMode {
        OutputMode::Json(JsonOutputParams::default())
    }

    #[test]
    fn csv_quotes_embedded_delimiter_and_quote() {
        let row = row(
            vec!["a", "b"],
            vec![
                present(Value::String("a,b".into())),
                present(Value::String("c\"d".into())),
            ],
        );
        assert_eq!(serialize_row(&csv_mode(), &row).unwrap(), &b"\"a,b\",\"c\"\"d\"\n"[..]);
    }

    #[test]
    fn csv_quote_always_quotes_every_field() {
        let mode = OutputMode::Csv(CsvOutputParams {
            quote_fields: QuoteFields::Always,
            ..Default::default()
        });
        let row = row(
            vec!["a", "b"],
            vec![
                present(Value::String("x".into())),
                present(Value::String("y".into())),
            ],
        );
        assert_eq!(serialize_row(&mode, &row).unwrap(), &b"\"x\",\"y\"\n"[..]);
    }

    #[test]
    fn csv_missing_is_empty_field() {
        let row = row(
            vec!["a", "b"],
            vec![present(Value::String("x".into())), Field::Missing],
        );
        assert_eq!(serialize_row(&csv_mode(), &row).unwrap(), &b"x,\n"[..]);
    }

    #[test]
    fn csv_trailing_missing_pads_to_key_width() {
        let row = row(
            vec!["a", "b", "c"],
            vec![
                present(Value::String("x".into())),
                present(Value::String("y".into())),
            ],
        );
        assert_eq!(serialize_row(&csv_mode(), &row).unwrap(), &b"x,y,\n"[..]);
    }

    #[test]
    fn csv_custom_delimiters() {
        let mode = OutputMode::Csv(CsvOutputParams {
            field_delimiter: b'|',
            record_delimiter: b';',
            ..Default::default()
        });
        let row = row(
            vec!["a", "b"],
            vec![
                present(Value::String("x".into())),
                present(Value::String("y".into())),
            ],
        );
        assert_eq!(serialize_row(&mode, &row).unwrap(), &b"x|y;"[..]);
    }

    #[test]
    fn csv_custom_escape_escapes_embedded_quote() {
        // S3 QuoteEscapeCharacter other than `"` (review fix): the escape
        // byte prefixes the quote char inside a quoted field — doubling is
        // off, else the configured escape would be inert.
        let mode = OutputMode::Csv(CsvOutputParams {
            escape: b'\\',
            ..Default::default()
        });
        let row = row(vec!["a"], vec![present(Value::String("a\"b".into()))]);
        assert_eq!(
            serialize_row(&mode, &row).unwrap(),
            &b"\"a\\\"b\"\n"[..]
        );
    }

    #[test]
    fn json_exact_record() {
        let row = row(vec!["id"], vec![present(Value::String("1".into()))]);
        assert_eq!(
            serialize_row(&json_mode(), &row).unwrap(),
            &b"{\"id\":\"1\"}\n"[..]
        );
    }

    #[test]
    fn json_decimal_normalizes_before_emitting() {
        let row = row(vec!["k"], vec![present(Value::Decimal(Decimal::new(150, 2)))]);
        assert_eq!(
            serialize_row(&json_mode(), &row).unwrap(),
            &b"{\"k\":1.5}\n"[..]
        );
    }

    #[test]
    fn json_raw_number_verbatim_unquoted() {
        let row = row(vec!["k"], vec![present(Value::RawNumber("1e309".into()))]);
        assert_eq!(
            serialize_row(&json_mode(), &row).unwrap(),
            &b"{\"k\":1e309}\n"[..]
        );
    }

    #[test]
    fn json_all_missing_is_empty_object() {
        let row = row(vec!["a", "b"], vec![Field::Missing, Field::Missing]);
        assert_eq!(
            serialize_row(&json_mode(), &row).unwrap(),
            &b"{}\n"[..]
        );
    }

    #[test]
    fn json_missing_key_is_omitted() {
        let row = row(
            vec!["a", "b"],
            vec![present(Value::String("1".into())), Field::Missing],
        );
        assert_eq!(
            serialize_row(&json_mode(), &row).unwrap(),
            &b"{\"a\":\"1\"}\n"[..]
        );
    }

    #[test]
    fn json_null_is_null() {
        let row = row(vec!["k"], vec![present(Value::Null)]);
        assert_eq!(serialize_row(&json_mode(), &row).unwrap(), &b"{\"k\":null}\n"[..]);
    }

    #[test]
    fn json_bool_and_int_forms() {
        let row = row(
            vec!["b", "i"],
            vec![present(Value::Bool(true)), present(Value::Int(42))],
        );
        assert_eq!(
            serialize_row(&json_mode(), &row).unwrap(),
            &b"{\"b\":true,\"i\":42}\n"[..]
        );
    }

    #[test]
    fn json_nested_value_renders_natively() {
        let row = row(
            vec!["k"],
            vec![present(Value::Json(Box::new(json!({"a": [1, true]}))))],
        );
        assert_eq!(
            serialize_row(&json_mode(), &row).unwrap(),
            &b"{\"k\":{\"a\":[1,true]}}\n"[..]
        );
    }

    #[test]
    fn json_custom_record_delimiter() {
        let row = row(vec!["k"], vec![present(Value::Null)]);
        let mode = OutputMode::Json(JsonOutputParams {
            record_delimiter: b';',
        });
        assert_eq!(serialize_row(&mode, &row).unwrap(), &b"{\"k\":null};"[..]);
    }

    #[test]
    fn json_value_under_csv_output_is_compact_cell() {
        // SELECT * matrix: JSON→CSV nested values land in the cell as
        // compact JSON text (`{"a":1}`, no spaces); the cell's own `"` are
        // CSV-escaped per the params (default escape==quote: doubled), so
        // the encoded cell is `"{""a"":1}"`.
        let row = row(
            vec!["k"],
            vec![present(Value::Json(Box::new(json!({"a": 1}))))],
        );
        assert_eq!(
            serialize_row(&csv_mode(), &row).unwrap(),
            b"\"{\"\"a\"\":1}\"\n"
        );
    }
}
