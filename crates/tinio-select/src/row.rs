//! Row / value model: the engine's record unit and its scalar spine.
//!
//! `Value` mirrors S3 Select's types: `Decimal` (28-digit) for numbers,
//! `RawNumber` for verbatim JSON number tokens (SELECT * passthrough),
//! `Json` for nested structures. `Field` adds the `MISSING` sentinel;
//! `Record` frames one record per input format. Numbers parse lazily:
//! `parse_number` runs only when a numeric operator consumes a value.

use rust_decimal::Decimal;

use crate::SelectError;

/// One projected/loaded record value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Decimal(Decimal),
    /// JSON number token verbatim — SELECT * carries it through unparsed.
    RawNumber(String),
    String(String),
    /// Nested structure (JSON object/array or parquet list/struct).
    Json(Box<serde_json::Value>),
}

/// One record column: a present value or the MISSING sentinel.
#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    Present(Value),
    Missing,
}

/// One input record: columns plus the name map (`_N`/headers for CSV,
/// column names for parquet); JSON input carries the whole value.
#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Csv(Vec<Field>, Vec<String>),
    Json(serde_json::Value),
    Parquet(Vec<Field>, Vec<String>),
}

/// Lazily parse a value text into `Decimal` (the 28-digit numeric spine).
///
/// The digit count is checked before parsing, so an oversized number
/// errors with the precision message regardless of what the parser would
/// have produced; any other failure is an "invalid numeric value" error.
pub fn parse_number(s: &str) -> Result<Decimal, SelectError> {
    if s.chars().filter(char::is_ascii_digit).count() > 28 {
        return Err(SelectError::Value(format!(
            "numeric value exceeds 28-digit precision: {s}"
        )));
    }
    s.parse()
        .map_err(|_| SelectError::Value(format!("invalid numeric value: {s}")))
}

/// Serializer text form: normalized `Decimal` (trailing zeros stripped),
/// raw `RawNumber` token back out, compact JSON for `Json`. CSV fields and
/// JSON number values use this; the JSON serializer quotes `String`,
/// renders `Null`/`Bool`/`Int` itself, and maps `Json` natively.
pub fn display(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Decimal(d) => d.normalize().to_string(),
        Value::RawNumber(s) => s.clone(),
        Value::String(s) => s.clone(),
        Value::Json(j) => serde_json::to_string(j).expect("json value serialization is infallible"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parse_number_accepts_decimals() {
        assert_eq!(parse_number("1.50").unwrap(), Decimal::new(150, 2));
        assert_eq!(parse_number("-12").unwrap(), Decimal::new(-12, 0));
        assert_eq!(
            parse_number("3e2").unwrap().normalize(),
            Decimal::new(300, 0)
        );
    }

    #[test]
    fn parse_number_rejects_non_numbers() {
        for s in ["abc", "NaN"] {
            match parse_number(s) {
                Err(SelectError::Value(msg)) => {
                    assert_eq!(msg, format!("invalid numeric value: {s}"))
                }
                other => panic!("expected value error for {s:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_number_rejects_over_precision_before_parsing() {
        let s = "99999999999999999999999999999";
        match parse_number(s) {
            Err(SelectError::Value(msg)) => {
                assert_eq!(
                    msg,
                    format!("numeric value exceeds 28-digit precision: {s}")
                )
            }
            other => panic!("expected value error, got {other:?}"),
        }
    }

    #[test]
    fn display_normalizes_decimal() {
        assert_eq!(display(&Value::Decimal(Decimal::new(150, 2))), "1.5");
    }

    #[test]
    fn display_passes_raw_number_through() {
        assert_eq!(display(&Value::RawNumber("1e309".to_string())), "1e309");
    }

    #[test]
    fn display_json_via_serde() {
        assert_eq!(
            display(&Value::Json(Box::new(json!({"a": 1})))),
            r#"{"a":1}"#
        );
    }

    #[test]
    fn csv_record_construction() {
        let record = Record::Csv(
            vec![
                Field::Present(Value::String("a".into())),
                Field::Missing,
                Field::Present(Value::Int(42)),
            ],
            vec!["_1".into(), "_2".into(), "_3".into()],
        );
        let Record::Csv(fields, names) = record else {
            unreachable!()
        };
        assert_eq!(names, vec!["_1", "_2", "_3"]);
        assert_eq!(fields.len(), 3);
        assert!(matches!(fields[1], Field::Missing));
        let Field::Present(third) = &fields[2] else {
            unreachable!()
        };
        assert_eq!(display(third), "42");
    }
}
