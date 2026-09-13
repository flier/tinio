//! JSON record readers: LINES / DOCUMENT framings over one streaming
//! kernel — serde_json pulls one record root per call through a framing
//! filter, and the FROM path runs as a compiled state machine on serde's
//! event flow. Memory is bounded by one *pull*, never by the input: a LINES
//! line (≤ 1 MB span) or one DOCUMENT array element; a single-value DOCUMENT
//! root accumulates its whole match set in that one pull, and that pull is
//! charged like any other, so it is bounded by the cap rather than unbounded
//! (deviation ③, amended).
//!
//! Throughput: serde_json's `IoRead` reads one byte per call, so each handler
//! hands its framing a `BufReader` (a second layer never engages — refills
//! are capacity-sized and `BufReader::read` bypasses its buffer). The gap
//! against the whole-record `from_slice` parse this replaces is the price of
//! bounded memory, and it is real: `json_document_scan` in
//! `benches/select_scan.rs` is the baseline, and the measured old→new
//! criterion numbers live in the plan (Task 8).
//!
//! Recorded deviations (spec 2026-09-12, docs/superpowers/specs/):
//! ① DOCUMENT total size is unbounded — the 1 MB cap binds the root pull.
//! ② A non-final Name step's duplicate key errors mid-stream: rows already
//!   yielded by earlier pulls of that object are not retracted, and the
//!   failing pull's own rows are dropped — `next` clears `pending` and sets
//!   `done`, so a caller polling after the error sees a finished stream.
//! ③ A single-value root accumulates its match set per pull — bounded by the
//!   record cap that pull is charged against, not unbounded.
//! ④ An invalid LINES line may parse-error before the span cap fires.
//! ⑤ A DOCUMENT malformation surfaces after the records before it.
//! ⑥ DOCUMENT records report element-start offsets and ScanRange filters
//!   them (AWS: LINES-only — an extension).

pub(crate) mod core;
pub(crate) mod document;
pub(crate) mod framing;
pub(crate) mod lines;
pub(crate) mod matcher;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod value;

use std::io::{Error as IoError, ErrorKind, Read};

pub(crate) use value::{json_lookup, to_value};

use crate::{error::Error, record::RecordReader, row::Record, sql::FromClause};

/// The `serde_json::Error` → crate `Error` tail both handlers' `map_err` ends
/// on: an io error keeps its kind, anything else becomes a `Format` error
/// under the `json input` prefix. Each handler keeps only its own precedence
/// arms in front of this.
pub(crate) fn json_error(e: serde_json::Error) -> Error {
    if e.is_io() {
        let kind = e.io_error_kind().unwrap_or(ErrorKind::Other);
        return Error::from(IoError::new(kind, e.to_string()));
    }
    Error::Format(format!("json input: {e}"))
}

/// JSON input framing (S3 Select `InputSerialization.JSON.Type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Lines,
    Document,
}

/// JSON input options (S3 Select `InputSerialization.JSON`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    pub ty: Type,
}

/// LINES/DOCUMENT dispatching reader — the wire-`Params` entry point.
pub struct Reader<R: Read> {
    inner: Handler<R>,
}

enum Handler<R: Read> {
    Lines(lines::Lines<R>),
    Document(document::Document<R>),
}

impl<R: Read> Reader<R> {
    pub fn new(reader: R, params: Params, from: &FromClause) -> Self {
        let inner = match params.ty {
            Type::Lines => Handler::Lines(lines::Lines::new(reader, from)),
            Type::Document => Handler::Document(document::Document::new(reader, from)),
        };
        Self { inner }
    }
}

impl<R: Read> RecordReader for Reader<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        match &mut self.inner {
            Handler::Lines(h) => h.next(),
            Handler::Document(h) => h.next(),
        }
    }

    fn last_record_start(&self) -> u64 {
        match &self.inner {
            Handler::Lines(h) => h.last_record_start(),
            Handler::Document(h) => h.last_record_start(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;

    use super::{
        Params, Reader, Type,
        testing::{raw, run, run_err, s},
    };
    use crate::{
        engine::OutRow,
        record::RecordReader,
        row::{Field, Value},
        sql::parse,
    };

    // ------------------------------------------------------------------
    // Reader: traversal.
    // ------------------------------------------------------------------

    #[test]
    fn traversal_case_insensitive_duplicates_are_ambiguous() {
        // R10: a case-folded duplicate along the traversed path is
        // ambiguous — the same rule as the engine's select-side lookup.
        let plan = parse("SELECT * FROM S3Object[*].id").unwrap();
        let mut r = Reader::new(
            Cursor::new("{\"id\": 1, \"ID\": 2}\n".as_bytes().to_vec()),
            Params { ty: Type::Lines },
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Ambiguous(m)) => assert_eq!(m, "id"),
            other => panic!("expected ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn traversal_bracket_name_duplicates_are_ambiguous() {
        // Q13: case-folded duplicate keys behind the `['name']` spelling are
        // ambiguous — the same AmbiguousFieldName as `.name` (the R10 test
        // above); the bracket segment is never routed as quoted-exact.
        let plan = parse("SELECT * FROM S3Object[*].files['name']").unwrap();
        let mut r = Reader::new(
            Cursor::new(
                "{\"files\": {\"NAME\": \"a\", \"name\": \"b\"}}\n"
                    .as_bytes()
                    .to_vec(),
            ),
            Params { ty: Type::Lines },
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Ambiguous(m)) => assert_eq!(m, "name"),
            other => panic!("expected ambiguous, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Engine: traversal lookup semantics.
    // ------------------------------------------------------------------

    const AWS_EXAMPLE: &str = r#"{ "Rules": [ {"id": "1"}, {"expr": "y > x"}, {"id": "2", "expr": "z = DEBUG"} ]}
{ "created": "June 27", "modified": "July 6" }
"#;

    #[test]
    fn select_id_extra_columns_match_aws() {
        // `SELECT id` over the AWS traversal example: MISSING rows project
        // Field::Missing — serialized `{}`, never `{"id":null}`.
        let rows = run(
            "SELECT id FROM S3Object[*].Rules[*].id",
            Type::Lines,
            AWS_EXAMPLE,
        );
        assert_eq!(rows.len(), 4);
        let expected = |f: Field| OutRow {
            keys: vec!["id".into()],
            vals: vec![f],
        };
        assert_eq!(rows[0], expected(s("1")));
        assert_eq!(rows[1], expected(Field::Missing));
        assert_eq!(rows[2], expected(s("2")));
        assert_eq!(rows[3], expected(Field::Missing));
    }

    #[test]
    fn where_is_not_missing_omits_empty_records() {
        let rows = run(
            "SELECT id FROM S3Object[*].Rules[*].id WHERE id IS NOT MISSING",
            Type::Lines,
            AWS_EXAMPLE,
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].vals, vec![s("1")]);
        assert_eq!(rows[1].vals, vec![s("2")]);
    }

    #[test]
    fn json_missing_projection_is_missing_not_null() {
        // A missing attribute in the select list stays MISSING (key omitted,
        // `{}` in JSON output) — never a present NULL.
        let rows = run(
            "SELECT id FROM S3Object s",
            Type::Lines,
            "{\"expr\": \"y\"}\n",
        );
        assert_eq!(rows[0].vals, vec![Field::Missing]);
    }

    #[test]
    fn json_null_attribute_is_null_not_missing() {
        let rows = run("SELECT id FROM S3Object s", Type::Lines, "{\"id\": null}\n");
        assert_eq!(rows[0].vals, vec![Field::Present(Value::Null)]);
    }

    #[test]
    fn json_bool_attribute_is_bool_value() {
        // A JSON scalar bool maps to the Bool row value (the `to_value`
        // arm), not a string or a number.
        let rows = run(
            "SELECT flag FROM S3Object s",
            Type::Lines,
            "{\"flag\": true}\n",
        );
        assert_eq!(rows[0].keys, vec!["flag"]);
        assert_eq!(rows[0].vals, vec![Field::Present(Value::Bool(true))]);
        let rows = run(
            "SELECT flag FROM S3Object s",
            Type::Lines,
            "{\"flag\": false}\n",
        );
        assert_eq!(rows[0].vals, vec![Field::Present(Value::Bool(false))]);
    }

    #[test]
    fn select_star_wild_encounter_order() {
        let rows = run(
            "SELECT * FROM S3Object[*]",
            Type::Lines,
            "{\"b\": \"x\", \"a\": 1}\n",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].keys, vec!["b", "a"]);
        assert_eq!(rows[0].vals, vec![s("x"), raw("1")]);
    }

    #[test]
    fn index_projection_uses_last_path_element_key() {
        let rows = run(
            "SELECT s.projects[0].project_name FROM S3Object s",
            Type::Lines,
            "{\"projects\": [{\"project_name\": \"project1\", \"completed\": false}]}\n",
        );
        assert_eq!(rows.len(), 1);
        // AWS: the last path element names the output column.
        assert_eq!(rows[0].keys, vec!["project_name"]);
        assert_eq!(rows[0].vals, vec![s("project1")]);
    }

    #[test]
    fn bracket_string_subscript() {
        let rows = run(
            "SELECT s['name'] FROM S3Object s",
            Type::Lines,
            "{\"name\": \"x\"}\n",
        );
        assert_eq!(rows[0].keys, vec!["name"]);
        assert_eq!(rows[0].vals, vec![s("x")]);
    }

    #[test]
    fn whole_row_refs_via_underscore_one() {
        // AWS doc example #2: `_1` = the row; the rest of the path applies.
        let input = r#"{ "created": "936864000", "dir_name": "important_docs", "files": [ { "name": "." } ], "owner": "Amazon S3" }
{ "created": "936864000", "dir_name": "other_docs", "files": [ { "name": ".." } ], "owner": "User" }
"#;
        let rows = run(
            "SELECT _1.dir_name, _1.owner FROM S3Object[*]",
            Type::Lines,
            input,
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].keys, vec!["dir_name", "owner"]);
        assert_eq!(rows[0].vals, vec![s("important_docs"), s("Amazon S3")]);
        assert_eq!(rows[1].vals, vec![s("other_docs"), s("User")]);
    }

    #[test]
    fn whole_row_refs_via_alias() {
        let input = r#"{ "dir_name": "important_docs", "files": [ { "name": "." } ], "owner": "Amazon S3" }
"#;
        let rows = run(
            "SELECT d.dir_name, d.files FROM S3Object[*] d",
            Type::Lines,
            input,
        );
        assert_eq!(rows[0].keys, vec!["dir_name", "files"]);
        // Nested JSON stays a `Json` value — under CSV output it renders as a
        // compact cell (Decision A); only parquet nested errors `NestedCsv`.
        assert_eq!(
            rows[0].vals,
            vec![
                s("important_docs"),
                Field::Present(Value::Json(Box::new(json!([{"name": "."}]))))
            ]
        );
    }

    #[test]
    fn underscore_one_field_wins_over_whole_record() {
        let rows = run(
            "SELECT _1 FROM S3Object s",
            Type::Lines,
            "{\"_1\": \"x\"}\n",
        );
        assert_eq!(rows[0].vals, vec![s("x")]);
        let rows = run("SELECT _1 FROM S3Object s", Type::Lines, "{\"a\": 1}\n");
        assert_eq!(
            rows[0].vals,
            vec![Field::Present(Value::Json(Box::new(json!({"a": 1}))))]
        );
    }

    #[test]
    fn scalar_row_any_unqualified_ref_resolves() {
        let rows = run(
            "SELECT price FROM S3Object[*].books[*].price",
            Type::Lines,
            "{\"books\": [{\"price\": 10}, {\"price\": 20}]}\n",
        );
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.keys, vec!["price"]);
            assert!(matches!(row.vals[0], Field::Present(Value::RawNumber(_))));
        }
        assert_eq!(rows[0].vals, vec![raw("10")]);
        assert_eq!(rows[1].vals, vec![raw("20")]);
    }

    #[test]
    fn raw_number_passthrough_no_decimal_parse() {
        // Arbitrary precision: an exponential token never hits f64 (1e309
        // overflows f64) or the decimal spine — `RawNumber` carries it
        // through. Known serde_json normalization (verified 1.0.151): its
        // arbitrary-precision scanner rewrites a signless exponent as
        // `e+` — `1e309` becomes `1e+309` — so the token is preserved up
        // to that sign-injection, never re-parsed.
        let rows = run("SELECT x FROM S3Object s", Type::Lines, "{\"x\": 1e309}\n");
        assert_eq!(rows[0].keys, vec!["x"]);
        assert_eq!(rows[0].vals, vec![raw("1e+309")]);
        let rows = run("SELECT * FROM S3Object s", Type::Lines, "{\"x\": 1e309}\n");
        assert_eq!(rows[0].vals, vec![raw("1e+309")]);
        // A plain integer token is verbatim.
        let rows = run(
            "SELECT x FROM S3Object s",
            Type::Lines,
            "{\"x\": 123456789012345678901234567890}\n",
        );
        assert_eq!(rows[0].vals, vec![raw("123456789012345678901234567890")]);
    }

    #[test]
    fn document_root_array_select_star() {
        let rows = run(
            "SELECT * FROM S3Object[*]",
            Type::Document,
            "[{\"id\": 1}, {\"id\": 2}]",
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].keys, vec!["id"]);
        assert_eq!(rows[0].vals, vec![raw("1")]);
        assert_eq!(rows[1].vals, vec![raw("2")]);
    }

    #[test]
    fn json_attribute_lookup_case_sensitivity() {
        // Unquoted = case-insensitive; double-quoted = exact (AWS rule).
        let rows = run(
            "SELECT s.name FROM S3Object s",
            Type::Lines,
            "{\"NAME\": \"a\"}\n",
        );
        assert_eq!(rows[0].vals, vec![s("a")]);
        let rows = run(
            "SELECT s.\"name\" FROM S3Object s",
            Type::Lines,
            "{\"NAME\": \"a\"}\n",
        );
        assert_eq!(rows[0].vals, vec![Field::Missing]);
    }

    #[test]
    fn json_attribute_case_insensitive_duplicates_are_ambiguous() {
        // Two attrs differing only in case → AmbiguousFieldName (AWS
        // example #2, JSON branch), surfaced in-stream like the CSV arm.
        match run_err(
            "SELECT s.name FROM S3Object s",
            Type::Lines,
            "{\"NAME\": \"a\", \"name\": \"b\"}\n",
        ) {
            crate::Error::Ambiguous(m) => assert_eq!(m, "name"),
            other => panic!("expected ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn reserved_keyword_attribute_is_accepted() {
        // Recorded divergence (spec A2): AWS answers 400 for an unquoted
        // reserved keyword used as an attribute name and requires `s."CAST"`;
        // we accept it, and MinIO — the reference OSS implementation —
        // documents the same non-compliance ("AWS S3's reserved keywords list
        // is not yet respected"). Pinned at the EVALUATION layer, not at
        // parse: a parse-level `Ok` assertion would keep passing when the
        // scalar-function spec turns CAST into a real function, which is
        // exactly the change that should force a re-decision here. The
        // support predicate is form-based, so this is a compound identifier
        // whose text happens to be a keyword.
        let rows = run(
            "SELECT s.CAST FROM S3Object s",
            Type::Lines,
            "{\"CAST\": \"x\"}\n",
        );
        assert_eq!(rows[0].vals, vec![s("x")]);
    }

    #[test]
    fn traversal_and_where_filters() {
        let rows = run(
            "SELECT dir_name FROM S3Object[*] s WHERE s.dir_name = 'other_docs'",
            Type::Lines,
            r#"{ "dir_name": "important_docs", "owner": "Amazon S3" }
{ "dir_name": "other_docs", "owner": "User" }
"#,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].vals, vec![s("other_docs")]);
    }
}
