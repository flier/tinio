//! JSON record reader: LINES / DOCUMENT framing + `FROM S3Object[*]`
//! path-traversal expansion.
//!
//! `Reader` yields one `Record::Json` per reached element. Framing:
//! LINES keeps one framed line (with the 1 MB input-record cap) and
//! produces the element set per line; DOCUMENT parses the whole stream
//! once — the record is byte-0-anchored — and expands the document's root
//! values. Traversal starts from the document-as-root-values (`S3Object[*]`:
//! DOCUMENT object = one root, DOCUMENT array = its elements, LINES = one
//! root per line) and walks the remaining segments field/index/wildcard;
//! a MISSING step propagates and a zero-match wildcard yields exactly one
//! MISSING row (a `Record::Json(None)` under the row model).

use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Read},
};

use serde_json::Value;

use crate::{
    error::Error,
    record::{CAP_MESSAGE, MAX_RECORD, RecordReader},
    row::{NameStyle, Record, Value as RowValue},
    sql::{FromClause, PathSeg},
};

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

/// The JSON element of a row in the row model: numbers keep their verbatim
/// token (`RawNumber`, arbitrary precision), scalars map to their variants,
/// objects/arrays become `Json` values.
pub(crate) fn to_value(j: &Value) -> RowValue {
    match j {
        Value::Null => RowValue::Null,
        Value::Bool(b) => RowValue::Bool(*b),
        Value::Number(n) => RowValue::RawNumber(n.to_string()),
        Value::String(s) => RowValue::String(s.clone()),
        Value::Array(_) | Value::Object(_) => RowValue::Json(Box::new(j.clone())),
    }
}

/// Object key lookup: case-insensitive when unquoted, exact when quoted;
/// a case-folded duplicate is ambiguous (AWS: two attrs differing only in
/// case → `AmbiguousFieldName`). The single implementation shared by the
/// traversal path (`apply`) and the engine's select-side lookup — review
/// 2026-09-06b R10 exists precisely because the two copies used to drift.
pub(crate) fn json_lookup<'a>(
    map: &'a serde_json::Map<String, Value>,
    name: &str,
    style: NameStyle,
) -> Result<Option<&'a Value>, Error> {
    // Early-exit count (X8): two matches is the ambiguity verdict — a
    // Vec-collect per lookup was pure allocation on the hot path.
    let mut found: Option<&Value> = None;
    for (key, value) in map {
        let matches = if style.exact() {
            key == name
        } else {
            key.eq_ignore_ascii_case(name)
        };
        if matches {
            if found.is_some() {
                return Err(Error::Ambiguous(name.into()));
            }
            found = Some(value);
        }
    }
    Ok(found)
}

/// LINES/DOCUMENT reader: one `Record::Json` per reached element.
pub struct Reader<R: Read> {
    inner: BufReader<R>,
    ty: Type,
    from: FromClause,
    /// Elements of the most recently expanded root, waiting to be yielded.
    pending: VecDeque<Option<Value>>,
    /// Uncompressed stream offset of the next byte the reader will consume.
    offset: u64,
    last_record_start: u64,
    /// No more roots remain (LINE EOF, or the DOCUMENT pass ran).
    exhausted: bool,
}

impl<R: Read> Reader<R> {
    pub fn new(reader: R, params: Params, from: &FromClause) -> Self {
        Self {
            inner: BufReader::new(reader),
            ty: params.ty,
            from: from.clone(),
            pending: VecDeque::new(),
            offset: 0,
            last_record_start: 0,
            exhausted: false,
        }
    }

    fn cap() -> Error {
        Error::Format(CAP_MESSAGE.into())
    }

    /// The next framed LINES record (content without its terminator) and its
    /// stream offset; a line whose span (content + terminator) is over 1 MB
    /// errors the stream out — measured on the reader's own span accounting,
    /// exactly like the CSV reader.
    fn read_line(&mut self) -> Result<Option<(u64, Vec<u8>)>, Error> {
        let start = self.offset;
        let mut buf = Vec::new();
        loop {
            let avail = self.inner.fill_buf()?;
            if avail.is_empty() {
                // Clean EOF: a final line without a terminator spans as its
                // content alone.
                if buf.is_empty() {
                    return Ok(None);
                }
                if buf.len() as u64 > MAX_RECORD {
                    return Err(Self::cap());
                }
                self.offset += buf.len() as u64;
                return Ok(Some((start, buf)));
            }
            let terminator = avail.iter().position(|&b| b == b'\n');
            let take = terminator.unwrap_or(avail.len());
            buf.extend_from_slice(&avail[..take]);
            let consumed = take + usize::from(terminator.is_some());
            self.inner.consume(consumed);
            self.offset += consumed as u64;
            if terminator.is_some() {
                if buf.len() as u64 + 1 > MAX_RECORD {
                    return Err(Self::cap());
                }
                return Ok(Some((start, buf)));
            }
            if buf.len() as u64 > MAX_RECORD {
                return Err(Self::cap());
            }
        }
    }

    /// The whole DOCUMENT stream, bounded by the same 1 MB cap — capped
    /// while reading (`Read::take`), so memory stays bounded (review
    /// 2026-09-06 simplify: the own 64 KiB chunk loop re-implemented this).
    fn read_document(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let mut data = Vec::new();
        self.inner
            .by_ref()
            .take(MAX_RECORD + 1)
            .read_to_end(&mut data)?;
        if data.len() as u64 > MAX_RECORD {
            return Err(Self::cap());
        }
        Ok((!data.is_empty()).then_some(data))
    }

    /// One root's expansion: the input item becomes the document-as-root
    /// values (`S3Object[*]` — the segment list's first step, guaranteed
    /// `Wild` by the parse layer), then the rest of the segments walk
    /// field/index/wildcard.
    fn expand(&self, value: Value) -> Result<Vec<Option<Value>>, Error> {
        let mut cands: Vec<Option<Value>> = match self.ty {
            Type::Lines => vec![Some(value)],
            Type::Document => match value {
                Value::Array(elems) if elems.is_empty() => vec![None],
                Value::Array(elems) => elems.into_iter().map(Some).collect(),
                other => vec![Some(other)],
            },
        };
        // The parse layer always opens a traversed path with `S3Object[*]`
        // — exactly the root-value extraction above, so it is applied, never
        // walked; a non-traversed FROM has no segments to walk.
        let segments = self.from.segments.as_slice();
        let rest = if segments.first().is_some_and(|s| matches!(s, PathSeg::Wild)) {
            &segments[1..]
        } else {
            &[]
        };
        for seg in rest {
            cands = apply(seg, cands)?;
        }
        Ok(cands)
    }
}

/// One traversal segment over the candidate set; every candidate yields at
/// least one result (the zero-match rule), so each root emits ≥ 1 row.
fn apply(seg: &PathSeg, cands: Vec<Option<Value>>) -> Result<Vec<Option<Value>>, Error> {
    let mut out = Vec::with_capacity(cands.len());
    for cand in cands {
        match seg {
            PathSeg::Wild => match cand {
                None => out.push(None),
                Some(Value::Array(elems)) if elems.is_empty() => out.push(None),
                Some(Value::Array(elems)) => out.extend(elems.into_iter().map(Some)),
                Some(Value::Object(map)) if map.is_empty() => out.push(None),
                Some(Value::Object(map)) => out.extend(map.into_values().map(Some)),
                // Wildcard over a scalar: zero matches → one MISSING row.
                Some(_) => out.push(None),
            },
            PathSeg::Name(name) => match cand {
                None => out.push(None),
                Some(Value::Object(map)) => {
                    out.push(json_lookup(&map, name, NameStyle::Bare)?.cloned())
                }
                Some(_) => out.push(None),
            },
            PathSeg::Index(i) => match cand {
                None => out.push(None),
                Some(Value::Array(elems)) => out.push(elems.get(*i).cloned()),
                Some(_) => out.push(None),
            },
        }
    }
    Ok(out)
}

impl<R: Read> RecordReader for Reader<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        loop {
            if let Some(element) = self.pending.pop_front() {
                return Ok(Some(Record::Json(element)));
            }
            if self.exhausted {
                return Ok(None);
            }
            match self.ty {
                Type::Lines => match self.read_line()? {
                    None => self.exhausted = true,
                    Some((start, bytes)) => {
                        self.last_record_start = start;
                        self.pending = self.expand(parse(&bytes)?)?.into_iter().collect();
                    }
                },
                Type::Document => {
                    self.exhausted = true;
                    if let Some(bytes) = self.read_document()? {
                        self.last_record_start = 0;
                        self.pending = self.expand(parse(&bytes)?)?.into_iter().collect();
                    }
                }
            }
        }
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}

/// One root value out of the line/document bytes — parse errors surface as
/// in-stream `Format` (the JSON analog of the CSV reader's mapping).
fn parse(bytes: &[u8]) -> Result<Value, Error> {
    serde_json::from_slice(bytes).map_err(|e| Error::Format(format!("json input: {e}")))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;

    use super::*;
    use crate::{
        engine::{Engine, OutRow},
        record::RecordReader,
        row::{Field, Record, Value},
        sql::parse,
    };

    /// All records the reader yields for `sql`'s FROM clause over `input`.
    fn read(sql: &str, ty: Type, input: &str) -> Vec<Record> {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut r = Reader::new(Cursor::new(input.as_bytes().to_vec()), Params { ty }, &plan.from);
        let mut out = Vec::new();
        while let Some(rec) = r.next().unwrap_or_else(|e| panic!("{sql}: {e}")) {
            out.push(rec);
        }
        out
    }

    /// Engine rows over a JSON input, reader and engine composed.
    fn run(sql: &str, ty: Type, input: &str) -> Vec<OutRow> {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut r = Reader::new(Cursor::new(input.as_bytes().to_vec()), Params { ty }, &plan.from);
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
    fn run_err(sql: &str, ty: Type, input: &str) -> crate::Error {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut r = Reader::new(Cursor::new(input.as_bytes().to_vec()), Params { ty }, &plan.from);
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

    fn s(v: &str) -> Field {
        Field::Present(Value::String(v.to_string()))
    }

    fn raw(v: &str) -> Field {
        Field::Present(Value::RawNumber(v.to_string()))
    }

    // ------------------------------------------------------------------
    // Reader: framing.
    // ------------------------------------------------------------------

    #[test]
    fn lines_two_objects() {
        let recs = read(
            "SELECT * FROM S3Object",
            Type::Lines,
            "{\"a\": 1, \"b\": 2}\n{\"c\": \"x\"}\n",
        );
        assert_eq!(
            recs,
            vec![
                Record::Json(Some(json!({"a": 1, "b": 2}))),
                Record::Json(Some(json!({"c": "x"}))),
            ]
        );
    }

    #[test]
    fn document_single_multiline_object() {
        let recs = read(
            "SELECT * FROM S3Object",
            Type::Document,
            "{\n \"a\": 1,\n \"b\": \"x\"\n}",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!({"a": 1, "b": "x"})))]);
    }

    #[test]
    fn document_root_array_yields_elements() {
        let recs = read(
            "SELECT * FROM S3Object",
            Type::Document,
            "[{\"id\": 1}, {\"id\": 2}]",
        );
        assert_eq!(
            recs,
            vec![
                Record::Json(Some(json!({"id": 1}))),
                Record::Json(Some(json!({"id": 2}))),
            ]
        );
    }

    #[test]
    fn empty_document_and_array_yield_nothing() {
        assert_eq!(
            read("SELECT * FROM S3Object", Type::Document, ""),
            Vec::<Record>::new()
        );
        // Zero root values from a DOCUMENT array: the `S3Object[*]` wildcard
        // zero-match rule — exactly one MISSING row, not zero rows.
        assert_eq!(
            read("SELECT * FROM S3Object", Type::Document, "[]"),
            vec![Record::Json(None)]
        );
    }

    // ------------------------------------------------------------------
    // Reader: traversal.
    // ------------------------------------------------------------------

    #[test]
    fn traversal_rules_id_matches_aws() {
        // AWS doc example #1: 2 roots, 4 yielded elements (two MISSING).
        let recs = read(
            "SELECT id FROM S3Object[*].Rules[*].id",
            Type::Lines,
            r#"{ "Rules": [ {"id": "1"}, {"expr": "y > x"}, {"id": "2", "expr": "z = DEBUG"} ]}
{ "created": "June 27", "modified": "July 6" }
"#,
        );
        assert_eq!(
            recs,
            vec![
                Record::Json(Some(json!("1"))),
                Record::Json(None),
                Record::Json(Some(json!("2"))),
                Record::Json(None),
            ]
        );
    }

    #[test]
    fn traversal_zero_match_wildcard_emits_one_missing() {
        // Root lacks `Rules`: MISSING step, then `[*]` matches nothing → the
        // exactly-one-MISSING-row rule, for both wildcard spellings.
        for sql in [
            "SELECT * FROM S3Object[*].Rules[*]",
            "SELECT * FROM S3Object[*].Rules.*",
        ] {
            assert_eq!(
                read(sql, Type::Lines, "{\"other\": 1}\n"),
                vec![Record::Json(None)],
                "{sql}"
            );
        }
    }

    #[test]
    fn traversal_index_segment() {
        let recs = read(
            "SELECT * FROM S3Object[*].projects[0]",
            Type::Lines,
            "{\"projects\": [{\"name\": \"p0\"}, {\"name\": \"p1\"}]}\n",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!({"name": "p0"})))]);
    }

    #[test]
    fn traversal_index_out_of_range_is_missing() {
        let recs = read(
            "SELECT * FROM S3Object[*].projects[9]",
            Type::Lines,
            "{\"projects\": [{\"name\": \"p0\"}]}\n",
        );
        assert_eq!(recs, vec![Record::Json(None)]);
    }

    #[test]
    fn traversal_bracket_name_segment() {
        let recs = read(
            "SELECT * FROM S3Object[*].files['name']",
            Type::Lines,
            "{\"files\": {\"name\": \"x\"}}\n",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!("x")))]);
    }

    #[test]
    fn traversal_name_case_insensitive() {
        let recs = read(
            "SELECT * FROM S3Object[*].rules.id",
            Type::Lines,
            "{\"Rules\": {\"id\": 1}}\n",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!(1)))]);
    }

    #[test]
    fn traversal_wild_over_scalar_is_missing() {
        // `[*]` over a scalar (not array/object) is a zero-match step →
        // exactly one MISSING row, never a present null.
        assert_eq!(
            read("SELECT * FROM S3Object[*].x.*", Type::Lines, "{\"x\": 5}\n"),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn traversal_wild_over_empty_object_is_missing() {
        // Empty object wildcard: zero elements → one MISSING row.
        assert_eq!(
            read(
                "SELECT * FROM S3Object[*].x.*",
                Type::Lines,
                "{\"x\": {}}\n"
            ),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn traversal_wild_over_object_yields_values() {
        // Non-empty object wildcard expands to its values in document order.
        assert_eq!(
            read(
                "SELECT * FROM S3Object[*].x.*",
                Type::Lines,
                "{\"x\": {\"a\": 1, \"b\": 2}}\n"
            ),
            vec![
                Record::Json(Some(json!(1))),
                Record::Json(Some(json!(2))),
            ]
        );
    }

    #[test]
    fn traversal_name_over_non_object_is_missing() {
        // A name step over a scalar value (not an object) is zero matches.
        assert_eq!(
            read("SELECT * FROM S3Object[*].x.y", Type::Lines, "{\"x\": 5}\n"),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn traversal_index_over_non_array_is_missing() {
        // An index step over a scalar (not an array) is zero matches.
        assert_eq!(
            read("SELECT * FROM S3Object[*].x[0]", Type::Lines, "{\"x\": 5}\n"),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn lines_final_unterminated_line_over_cap() {
        // A final line with no newline whose span crosses the 1 MB cap is
        // errored out at EOF — the same `Format` frame as the terminator
        // path, distinct because there is no terminator to measure.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Reader::new(
            Cursor::new(vec![b'a'; 1024 * 1024 + 1]),
            Params { ty: Type::Lines },
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn traversal_scalar_price_rows() {
        let recs = read(
            "SELECT price FROM S3Object[*].books[*].price",
            Type::Lines,
            "{\"books\": [{\"price\": 10}, {\"price\": 20}]}\n",
        );
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!(10))), Record::Json(Some(json!(20)))]
        );
    }

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
    fn document_object_root_is_one_row_object() {
        // DOCUMENT object root = one root value (the object itself — the
        // `S3Object[*]` roots are the object, not its fields); only an
        // array root splits into its elements.
        let recs = read(
            "SELECT * FROM S3Object[*]",
            Type::Document,
            "{\"a\": 1, \"b\": \"x\", \"c\": [1, 2]}",
        );
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!({"a": 1, "b": "x", "c": [1, 2]})))]
        );
    }

    // ------------------------------------------------------------------
    // Reader: cap + offsets.
    // ------------------------------------------------------------------

    #[test]
    fn lines_input_record_cap() {
        // Content + terminator span over 1 MB errors the stream out.
        let mut input = vec![b'a'; 1024 * 1024];
        input.push(b'\n');
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Reader::new(Cursor::new(input), Params { ty: Type::Lines }, &plan.from);
        match r.next() {
            Err(crate::Error::Format(msg)) => {
                assert_eq!(msg, "input record exceeds 1 MB");
                assert_eq!(
                    crate::Error::Format(msg).to_string(),
                    "S3 select: input error: input record exceeds 1 MB"
                );
            }
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn lines_input_record_cap_exact_span_allowed() {
        // Span (content + terminator) exactly 1 MB passes, like the CSV
        // reader: `{"x":"` ... `"}` is 8 bytes of framing, so the filler
        // is 1 MB - 1 - 8 and the record parses.
        let filler = "a".repeat(1024 * 1024 - 9);
        let line = format!("{{\"x\":\"{filler}\"}}");
        assert_eq!(line.len() + 1, 1024 * 1024);
        let recs = read(
            "SELECT * FROM S3Object",
            Type::Lines,
            &format!("{line}\n"),
        );
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!({"x": filler})))],
            "the entire key value must survive"
        );
    }

    #[test]
    fn document_input_record_cap() {
        let doc = format!("\"{}", "a".repeat(1024 * 1024));
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Reader::new(Cursor::new(doc), Params { ty: Type::Document }, &plan.from);
        match r.next() {
            Err(crate::Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn document_input_record_cap_exact_allowed() {
        let doc = format!("\"{}\"", "a".repeat(1024 * 1024 - 2));
        assert_eq!(doc.len(), 1024 * 1024);
        let recs = read("SELECT * FROM S3Object", Type::Document, &doc);
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!("a".repeat(1024 * 1024 - 2))))]
        );
    }

    #[test]
    fn lines_last_record_start() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Reader::new(
            Cursor::new("{\"a\":1}\n{\"b\":2}\n".as_bytes().to_vec()),
            Params { ty: Type::Lines },
            &plan.from,
        );
        assert_eq!(r.last_record_start(), 0);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 0);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 8);
        assert_eq!(r.next().unwrap(), None);
        assert_eq!(r.last_record_start(), 8);
    }

    #[test]
    fn document_last_record_start_is_zero() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Reader::new(
            Cursor::new("{\n \"a\": 1\n}\n".as_bytes().to_vec()),
            Params { ty: Type::Document },
            &plan.from,
        );
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 0);
        assert_eq!(r.next().unwrap(), None);
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
        let rows = run(
            "SELECT id FROM S3Object s",
            Type::Lines,
            "{\"id\": null}\n",
        );
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
        let rows = run(
            "SELECT x FROM S3Object s",
            Type::Lines,
            "{\"x\": 1e309}\n",
        );
        assert_eq!(rows[0].keys, vec!["x"]);
        assert_eq!(rows[0].vals, vec![raw("1e+309")]);
        let rows = run(
            "SELECT * FROM S3Object s",
            Type::Lines,
            "{\"x\": 1e309}\n",
        );
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
    fn empty_line_is_a_format_error() {
        // A blank LINES record cannot parse as JSON — an in-stream Format
        // error (there is no blank-line skip rule in the AWS surface).
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Reader::new(
            Cursor::new(b"
".to_vec()),
            Params { ty: Type::Lines },
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "got {m}"),
            other => panic!("expected format error, got {other:?}"),
        }
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
