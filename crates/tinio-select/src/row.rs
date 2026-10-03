//! Row / value model: the engine's record unit and its scalar spine.
//!
//! `Value` mirrors S3 Select's types: `Decimal` (28-digit) for numbers,
//! `RawNumber` for verbatim JSON number tokens (SELECT * passthrough),
//! `Json` for nested structures. `Field` adds the `MISSING` sentinel;
//! `Record` frames one record per input format. Numbers parse lazily:
//! `parse_number` runs only when a numeric operator consumes a value.

use std::{collections::HashMap, rc::Rc};

use rust_decimal::Decimal;

use crate::{budget::Budget, error::Error};

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

/// Identifier spelling — the S3 Select identifier rules (review 2026-09-06
/// S9): unquoted = case-insensitive, `"quoted"` = exact. Replaces a bare
/// `quoted: bool` threaded through the lookup signatures; Parsers produce
/// it from the identifier's `quote_style`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameStyle {
    Bare,
    Quoted,
}

impl NameStyle {
    /// The exact-match flag the lookups branch on.
    pub(crate) fn exact(self) -> bool {
        matches!(self, NameStyle::Quoted)
    }
}

/// One header lookup's outcome; `Ambiguous` is a value, not an error, so the
/// caller keeps its existing per-style rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameHit {
    One(usize),
    Ambiguous,
    None,
}

/// Header names resolved once per stream: one pass at build time, O(1) per
/// reference afterwards.
#[derive(Debug, Default, PartialEq)]
pub struct NameIndex {
    exact: HashMap<String, NameHit>,
    folded: HashMap<String, NameHit>,
}

impl NameIndex {
    /// The one builder: [`NameSet::new`] calls it with the very names the set
    /// carries, so an index can never be built for a different list. Kept
    /// `pub(crate)` — nothing outside the crate pairs a name list with an
    /// index itself (review F6).
    pub(crate) fn build(names: &[String]) -> Self {
        let mut exact = HashMap::new();
        let mut folded = HashMap::new();
        for (i, n) in names.iter().enumerate() {
            insert(&mut exact, n.clone(), i);
            insert(&mut folded, n.to_ascii_lowercase(), i);
        }
        Self { exact, folded }
    }

    /// The name's outcome under the reference's spelling: quoted is
    /// exact-case, bare is the ASCII case folding `eq_ignore_ascii_case`
    /// applied (the one comparison the scan used before this index).
    ///
    /// **Both arms look the reference up without allocating**, which is what
    /// this index is for (P3: one lookup per reference per record). The exact
    /// map borrows the `&str` (`String: Borrow<str>`), and the folded map is
    /// asked for the name verbatim first — it answers whenever the reference
    /// is already ASCII-lowercase, which is the shape the fold would leave
    /// unchanged — so only a reference the fold would actually rewrite pays
    /// for a lowered copy.
    pub fn get(&self, name: &str, style: NameStyle) -> NameHit {
        if style.exact() {
            return self.exact.get(name).copied().unwrap_or(NameHit::None);
        }
        if let Some(hit) = self.folded.get(name) {
            return *hit;
        }
        self.folded
            .get(&name.to_ascii_lowercase())
            .copied()
            .unwrap_or(NameHit::None)
    }
}

/// Insert one name's position; a second occupant of the key is the ambiguity
/// verdict (a third changes nothing).
fn insert(map: &mut HashMap<String, NameHit>, key: String, i: usize) {
    match map.get(&key) {
        Some(NameHit::One(_)) => {
            map.insert(key, NameHit::Ambiguous);
        }
        Some(_) => {}
        None => {
            map.insert(key, NameHit::One(i));
        }
    }
}

/// One stream's names and the index built from them — one value, so the two
/// can never disagree (review C4): when they travelled as separate fields,
/// four construction sites paired them by hand and `Columns::new` accepted
/// any pairing, resolving names against the wrong header. The fields are
/// private and [`NameSet::new`] the only constructor, so the invariant is the
/// type's rather than its callers' (review F6).
///
/// # Examples
///
/// ```
/// use tinio_select::row::{NameHit, NameSet, NameStyle};
///
/// let names = NameSet::new(vec!["Id".to_string(), "Name".to_string()]);
///
/// assert_eq!(names.names()[1], "Name");
/// // A lookup resolves against exactly those names: bare references fold
/// // ASCII case, quoted ones are exact.
/// assert_eq!(names.index().get("id", NameStyle::Bare), NameHit::One(0));
/// assert_eq!(names.index().get("id", NameStyle::Quoted), NameHit::None);
/// assert_eq!(names.index().get("Name", NameStyle::Bare), NameHit::One(1));
/// ```
#[derive(Debug, PartialEq)]
pub struct NameSet {
    names: Vec<String>,
    index: NameIndex,
}

impl NameSet {
    /// The one constructor: the index is built from these very names, so
    /// `NameIndex::build` stays the only builder and a mismatch cannot be
    /// constructed.
    pub fn new(names: Vec<String>) -> Self {
        let index = NameIndex::build(&names);
        Self { names, index }
    }

    /// The stream's names, in header order — the names [`NameSet::index`]
    /// was built over, handed out together so callers never pair the two by
    /// hand.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The index over those same names: the O(1) side of a named reference.
    pub fn index(&self) -> &NameIndex {
        &self.index
    }
}

/// One record's fields and its header name set — the payload shared by the
/// CSV and parquet arms (review 2026-09-06 S7: the two `(Vec<Field>,
/// Vec<String>)` payloads were a data clump). `header` is `Rc`-shared per
/// reader stream: header/schema names are identical across every record, so
/// per-record cloning was pure waste (X8) — and the index travels inside the
/// same value, so a named reference is one map lookup instead of a scan of
/// the whole header per record (P3).
#[derive(Debug, Clone, PartialEq)]
pub struct Columns {
    pub fields: Vec<Field>,
    pub header: Rc<NameSet>,
}

impl Columns {
    /// Fields plus the reader's shared header name set (the `Rc` flows
    /// straight into the record — no per-record clone).
    pub fn new(fields: Vec<Field>, header: Rc<NameSet>) -> Self {
        Self { fields, header }
    }

    /// Owned-name helper (tests and one-off construction): the Vec is boxed
    /// once and its index built once.
    pub fn from_names(fields: Vec<Field>, names: Vec<String>) -> Self {
        Self::new(fields, Rc::new(NameSet::new(names)))
    }
}

/// One input record: columns (CSV/parquet) plus the name map (`_N`/headers
/// for CSV, column names for parquet); JSON input carries the whole value —
/// `None` is a path-traversal MISSING row (a wildcard/path step that matched
/// nothing), distinct from a present `null` value.
#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Csv(Columns),
    Json(Option<serde_json::Value>),
    Parquet(Columns),
}

/// What an expression in flight sees: the record under test plus the FROM
/// alias (the JSON path/access arms need the alias; flat lookups do not),
/// plus this request's MISSING sentinel name (the per-request uuid name —
/// the dialect hook produced the sentinel call under it; derived once in
/// `Engine::new`, never per row) and the request's budget.
///
/// It lives beside [`Record`] rather than in `engine` because the JSON
/// lookup chain takes it too: the dependency points from the leaf utilities
/// (which already reach into `row`) down to the row/budget pair, never up
/// into the engine.
pub(crate) struct RowCtx<'a> {
    pub(crate) record: &'a Record,
    pub(crate) alias: &'a Option<String>,
    pub(crate) missing_name: &'a str,
    pub(crate) budget: &'a Budget,
}

impl<'a> RowCtx<'a> {
    /// The one constructor: `next` and `aggregate_next` both build their
    /// context here, so the two literals cannot drift (review C2).
    pub(crate) fn new(
        record: &'a Record,
        alias: &'a Option<String>,
        missing_name: &'a str,
        budget: &'a Budget,
    ) -> Self {
        Self {
            record,
            alias,
            missing_name,
            budget,
        }
    }

    /// The record's JSON payload, `None` for anything else — what the JSON
    /// lookup arms resolve against (`None` is a JSON MISSING row: a wildcard
    /// or path step that matched nothing).
    pub(crate) fn json_value(&self) -> Option<&serde_json::Value> {
        match self.record {
            Record::Json(v) => v.as_ref(),
            _ => None,
        }
    }
}

/// Lazily parse a value text into `Decimal` (the 28-digit numeric spine).
///
/// The digit count is checked before parsing, so an oversized number
/// errors with the precision message regardless of what the parser would
/// have produced; any other failure is an "invalid numeric value" error.
pub fn parse_number(s: &str) -> Result<Decimal, Error> {
    if s.chars().filter(char::is_ascii_digit).count() > 28 {
        return Err(Error::Value(format!(
            "numeric value exceeds 28-digit precision: {s}"
        )));
    }
    s.parse()
        .map_err(|_| Error::Value(format!("invalid numeric value: {s}")))
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
    use std::rc::Rc;

    use serde_json::json;

    use super::*;

    #[test]
    fn display_null_and_scalars() {
        // Null renders empty (the JSON serializer quotes strings and
        // renders Null itself; this is the CSV/row text form).
        assert_eq!(display(&Value::Null), "");
        assert_eq!(display(&Value::Bool(true)), "true");
        assert_eq!(display(&Value::Bool(false)), "false");
        assert_eq!(display(&Value::Int(-42)), "-42");
        assert_eq!(display(&Value::String("hi".into())), "hi");
    }

    #[test]
    fn name_style_exact_matches_quoted() {
        // S9: unquoted identifiers are case-insensitive (not exact), quoted
        // are exact — the flag the lookups branch on.
        assert!(!NameStyle::Bare.exact());
        assert!(NameStyle::Quoted.exact());
    }

    #[test]
    fn the_index_distinguishes_a_hit_a_duplicate_and_a_miss() {
        let names = vec!["a".to_string(), "A".to_string(), "b".to_string()];
        let ix = NameIndex::build(&names);
        assert!(matches!(ix.get("a", NameStyle::Bare), NameHit::Ambiguous));
        assert_eq!(ix.get("a", NameStyle::Quoted), NameHit::One(0));
        assert_eq!(ix.get("b", NameStyle::Bare), NameHit::One(2));
        assert_eq!(ix.get("c", NameStyle::Bare), NameHit::None);
    }

    #[test]
    fn columns_new_shares_name_rc() {
        // `new` takes the `Rc` and stores it verbatim (the reader stream
        // shares one name set across every record — no per-record clone);
        // `from_names` boxes the Vec and builds its index once at
        // construction, inside the `NameSet` that carries both.
        let names = Rc::new(NameSet::new(vec!["a".into(), "b".into()]));
        let c = Columns::new(vec![Field::Missing, Field::Missing], Rc::clone(&names));
        assert_eq!(c.header.names(), ["a", "b"]);
        assert!(Rc::ptr_eq(&c.header, &names));
        let c2 = Columns::from_names(Vec::new(), vec!["x".into()]);
        assert_eq!(c2.header.names(), ["x"]);
        assert_eq!(c2.header.index().get("x", NameStyle::Bare), NameHit::One(0));
    }

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
                Err(Error::Value(msg)) => {
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
            Err(Error::Value(msg)) => {
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
        let record = Record::Csv(Columns::from_names(
            vec![
                Field::Present(Value::String("a".into())),
                Field::Missing,
                Field::Present(Value::Int(42)),
            ],
            vec!["_1".into(), "_2".into(), "_3".into()],
        ));
        let Record::Csv(Columns { fields, header, .. }) = record else {
            unreachable!()
        };
        assert_eq!(header.names(), ["_1", "_2", "_3"]);
        assert_eq!(fields.len(), 3);
        assert!(matches!(fields[1], Field::Missing));
        let Field::Present(third) = &fields[2] else {
            unreachable!()
        };
        assert_eq!(display(third), "42");
    }
}
