//! LINES handler: one streaming pull per line — the line is never
//! materialized. Framing lives in `LineFraming`; matching in `matcher`; the
//! state shared with the DOCUMENT handler in `core`.

use std::io::{BufReader, Read};

use crate::{
    error::Error,
    json::{
        core::Core,
        framing::{LineFraming, LineStart},
        json_error,
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

/// LINES record reader: each line is one root value.
pub struct Lines<R: Read> {
    core: Core,
    framing: LineFraming<R>,
}

impl<R: Read> Lines<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            core: Core::new(from),
            // One buffer layer is enough: serde_json's `IoRead` issues one
            // `read` per byte no matter what, and this `BufReader` keeps
            // those calls memory-cheap. A second layer underneath never
            // engages — refills are capacity-sized, and `BufReader::read`
            // bypasses its buffer for requests >= capacity.
            framing: LineFraming::new(BufReader::new(reader)),
        }
    }
}

/// `fired` means "this line delivered more than `MAX_RECORD`"; the framing's
/// own span check (content + terminator) is the stricter of the two and
/// usually fires first, so both are mapped to the same message.
fn map_err(core: &mut Core, framing: &LineFraming<impl Read>, e: serde_json::Error) -> Error {
    if core.budget.fired() || framing.capped {
        return Error::Format(CAP_MESSAGE.into());
    }
    if let Some(f) = core.failure.take() {
        return f;
    }
    json_error(e)
}

/// Parse the next line into `core.pending`; sets `core.done` at input EOF.
/// One line is one root, so every byte serde is served here (matched, skipped
/// or trailing) is charged, and the next line starts from a full budget.
fn pull_line<R: Read>(core: &mut Core, framing: &mut LineFraming<R>) -> Result<(), Error> {
    match framing.start_line() {
        Err(io) => return Err(Error::from(io)),
        Ok(LineStart::Eof) => {
            core.done = true;
            return Ok(());
        }
        Ok(LineStart::Line(start)) => core.last_record_start = start,
    }
    let (root, probe) = core.pull(framing, true);
    match root {
        Err(e) => Err(map_err(core, framing, e)),
        Ok(()) => match probe.expect("probe runs on a clean root pull") {
            Ok(()) => Err(Error::Format(
                "json input: trailing characters on line".into(),
            )),
            Err(e) if e.is_eof() => Ok(()),
            Err(e) => Err(map_err(core, framing, e)),
        },
    }
}

impl<R: Read> RecordReader for Lines<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        let (core, framing) = (&mut self.core, &mut self.framing);
        core.advance(|core| pull_line(core, framing))
    }

    fn last_record_start(&self) -> u64 {
        self.core.last_record_start
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;

    use super::Lines;
    use crate::{json::testing::drain, record::RecordReader, row::Record, sql::parse};

    /// All records `Lines` yields for `sql`'s FROM clause over `input`.
    fn read(sql: &str, input: &str) -> Vec<Record> {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        drain(
            sql,
            Lines::new(Cursor::new(input.as_bytes().to_vec()), &plan.from),
        )
    }

    // ------------------------------------------------------------------
    // Reader: framing, traversal, cap + offsets.
    // ------------------------------------------------------------------

    #[test]
    fn lines_two_objects() {
        let recs = read(
            "SELECT * FROM S3Object",
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
    fn traversal_rules_id_matches_aws() {
        // AWS doc example #1: 2 roots, 4 yielded elements (two MISSING).
        let recs = read(
            "SELECT id FROM S3Object[*].Rules[*].id",
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
                read(sql, "{\"other\": 1}\n"),
                vec![Record::Json(None)],
                "{sql}"
            );
        }
    }

    #[test]
    fn traversal_index_segment() {
        let recs = read(
            "SELECT * FROM S3Object[*].projects[0]",
            "{\"projects\": [{\"name\": \"p0\"}, {\"name\": \"p1\"}]}\n",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!({"name": "p0"})))]);
    }

    #[test]
    fn traversal_index_out_of_range_is_missing() {
        let recs = read(
            "SELECT * FROM S3Object[*].projects[9]",
            "{\"projects\": [{\"name\": \"p0\"}]}\n",
        );
        assert_eq!(recs, vec![Record::Json(None)]);
    }

    #[test]
    fn traversal_bracket_name_segment() {
        let recs = read(
            "SELECT * FROM S3Object[*].files['name']",
            "{\"files\": {\"name\": \"x\"}}\n",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!("x")))]);
    }

    #[test]
    fn traversal_name_case_insensitive() {
        let recs = read(
            "SELECT * FROM S3Object[*].rules.id",
            "{\"Rules\": {\"id\": 1}}\n",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!(1)))]);
    }

    #[test]
    fn traversal_bracket_name_case_insensitive() {
        // Q13: `['name']` is a by-name form, not a quoted identifier — the
        // exact arm is only for SELECT/WHERE quoted attribute access. The
        // traversal feeder's NameStyle::Bare call site never routes a
        // bracket segment as quoted, so the bracket spelling resolves
        // case-insensitively like `.name` above.
        let recs = read(
            "SELECT * FROM S3Object[*].files['name']",
            "{\"files\": {\"NAME\": \"x\"}}\n",
        );
        assert_eq!(recs, vec![Record::Json(Some(json!("x")))]);
    }

    #[test]
    fn traversal_wild_over_scalar_is_missing() {
        // `[*]` over a scalar (not array/object) is a zero-match step →
        // exactly one MISSING row, never a present null.
        assert_eq!(
            read("SELECT * FROM S3Object[*].x.*", "{\"x\": 5}\n"),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn traversal_wild_over_empty_object_is_missing() {
        // Empty object wildcard: zero elements → one MISSING row.
        assert_eq!(
            read("SELECT * FROM S3Object[*].x.*", "{\"x\": {}}\n"),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn traversal_wild_over_object_yields_values() {
        // Non-empty object wildcard expands to its values in document order.
        assert_eq!(
            read(
                "SELECT * FROM S3Object[*].x.*",
                "{\"x\": {\"a\": 1, \"b\": 2}}\n"
            ),
            vec![Record::Json(Some(json!(1))), Record::Json(Some(json!(2))),]
        );
    }

    #[test]
    fn traversal_name_over_non_object_is_missing() {
        // A name step over a scalar value (not an object) is zero matches.
        assert_eq!(
            read("SELECT * FROM S3Object[*].x.y", "{\"x\": 5}\n"),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn traversal_index_over_non_array_is_missing() {
        // An index step over a scalar (not an array) is zero matches.
        assert_eq!(
            read("SELECT * FROM S3Object[*].x[0]", "{\"x\": 5}\n"),
            vec![Record::Json(None)]
        );
    }

    #[test]
    fn lines_final_unterminated_line_over_cap() {
        let filler = "a".repeat(1024 * 1024 - 6);
        let input = format!("{{\"x\":\"{filler}\"}}").into_bytes(); // no terminator
        assert_eq!(input.len(), 1024 * 1024 + 2);
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(input), &plan.from);
        match r.next() {
            Err(crate::Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn lines_final_unterminated_line_under_cap_is_one_record() {
        // The over-cap sibling above exercises the failure side; this pins the
        // success side of the same `LineFraming` EOF path (`avail.is_empty() ->
        // Ok(0)`): a final line with no terminator under the cap is exactly one
        // record, then the stream ends.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(b"{\"a\":1}".to_vec()), &plan.from);
        assert_eq!(r.next().unwrap(), Some(Record::Json(Some(json!({"a": 1})))));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn traversal_scalar_price_rows() {
        let recs = read(
            "SELECT price FROM S3Object[*].books[*].price",
            "{\"books\": [{\"price\": 10}, {\"price\": 20}]}\n",
        );
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!(10))), Record::Json(Some(json!(20)))]
        );
    }

    #[test]
    fn lines_input_record_cap() {
        // Valid JSON, span (content + terminator) 1 MB + 1: the cap fires
        // mid-serve (deviation ④: invalid filler would now parse-error first).
        let filler = "a".repeat(1024 * 1024 - 8);
        let mut input = format!("{{\"x\":\"{filler}\"}}").into_bytes();
        assert_eq!(input.len() + 1, 1024 * 1024 + 1);
        input.push(b'\n');
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(input), &plan.from);
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
        let recs = read("SELECT * FROM S3Object", &format!("{line}\n"));
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!({"x": filler})))],
            "the entire key value must survive"
        );
    }

    #[test]
    fn lines_last_record_start() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(
            Cursor::new("{\"a\":1}\n{\"b\":2}\n".as_bytes().to_vec()),
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
    fn empty_line_is_a_format_error() {
        // A blank LINES record cannot parse as JSON — an in-stream Format
        // error (there is no blank-line skip rule in the AWS surface).
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(
            Cursor::new(
                b"
"
                .to_vec(),
            ),
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "got {m}"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Reader: framing and the trailing probe.
    // ------------------------------------------------------------------

    #[test]
    fn lines_trailing_garbage_errors() {
        for input in [
            "{\"a\":1} x\n",
            "{\"a\":1} {\"b\":2}\n",
            "123 456\n",
            "123x\n",
        ] {
            let plan = parse("SELECT * FROM S3Object").unwrap();
            let mut r = Lines::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
            match r.next() {
                Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{input}: {m}"),
                other => panic!("{input}: expected format error, got {other:?}"),
            }
        }
    }

    #[test]
    fn lines_error_is_terminal_for_later_polls() {
        // A root that parses but has trailing garbage errors on the line, and the
        // rows it queued must NOT surface on the next poll: the error is terminal.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(b"{\"a\":1} x\n".to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
            other => panic!("expected format error, got {other:?}"),
        }
        assert_eq!(
            r.next().unwrap(),
            None,
            "a terminal error must not yield queued rows"
        );
    }

    #[test]
    fn lines_crlf_records_parse() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(
            Cursor::new(b"{\"a\":1}\r\n{\"b\":2}\r\n".to_vec()),
            &plan.from,
        );
        assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
        assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn lines_indented_line_reports_the_line_start() {
        // "Line start" means the line's first byte, not its first *value* byte:
        // the framing records the offset before it skips leading whitespace, so
        // an indented line reports the indentation. `lines_last_record_start`
        // (`{"a":1}\n{"b":2}\n` → 0, then 8) pins the unindented case.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(b"  {\"a\":1}\n".to_vec()), &plan.from);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 0);
    }

    #[test]
    fn lines_whitespace_only_line_is_a_format_error() {
        // jsonlines.org rule (G8): whitespace *around* a value is ignored, but a
        // record with no value at all is an error — the same class as a blank line.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(b"  \t \n".to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn lines_skipped_subtree_still_validates_strings() {
        // The reader must skip with `Skip`, never `IgnoredAny`: a lone surrogate
        // escape in a subtree the path never reads is rejected today by the
        // whole-line `from_slice`, and must stay rejected.
        let plan = parse("SELECT * FROM S3Object[*].a").unwrap();
        let mut r = Lines::new(
            Cursor::new(b"{\"b\":\"\\uD800\",\"a\":1}\n".to_vec()),
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
            other => panic!("expected format error, got {other:?}"),
        }
    }
}
