//! LINES handler: one root value per line. The line is never buffered —
//! `LineFraming` serves its bytes one at a time and reports EOF at the
//! newline, so the only thing that grows is the token being resolved.
//!
//! Ordering rule (design spec, verified fact 4): after the top-level value is
//! consumed, `peek()` panics, so the trailing check is
//! `consume_trailing_whitespace` and nothing else.

use std::io::{BufReader, Read};

use crate::{
    error::Error,
    json::{
        line::{LineFraming, LineStart},
        matcher::{CompiledPath, Matcher, Progress},
        source::{Reader, ReaderSource, consume_trailing, settings},
        tail::ReaderTail,
    },
    record::RecordReader,
    row::Record,
    sql::FromClause,
};

/// LINES record reader: each line is one root value.
pub struct Lines<R: Read> {
    framing: LineFraming<BufReader<R>>,
    matcher: Matcher,
    stream: ReaderTail,
}

impl<R: Read> Lines<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            // One buffer layer is enough: struson has its own 1 KiB reader
            // buffer, and the framing serves one byte per call.
            framing: LineFraming::new(BufReader::new(reader)),
            matcher: Matcher::new(CompiledPath::from_segments(&from.segments)),
            stream: ReaderTail::new(),
        }
    }

    /// The `pending` high-water mark, in rendered bytes (test tripwire).
    #[cfg(test)]
    pub(crate) fn pending_high(&self) -> usize {
        self.matcher.pending_high()
    }
}

/// Parse one whole line into the matcher's pending batch. The reader is built
/// per line over a borrow of the framing, so a half-read root cannot survive
/// the pull: the loop runs to `Progress::End` and leaves the queued records
/// for `ReaderTail::advance`.
///
/// The bound for LINES is `LineFraming`'s width cap (content plus terminator
/// ≤ 1 MB), enforced mid-serve inside `Read::read`: the framing errors before
/// struson can be served an over-cap line, so the width cap is the whole of
/// LINES' cap.
///
/// A free function, not a method: `Lines::next` destructures itself so
/// `advance` can hold the handler parts by `&mut` while this runs.
fn pull_line<R: Read>(
    stream: &mut ReaderTail,
    matcher: &mut Matcher,
    framing: &mut LineFraming<BufReader<R>>,
) -> Result<(), Error> {
    let line_start = match framing.start_line() {
        Ok(LineStart::Line(start)) => start,
        Ok(LineStart::Eof) => {
            stream.done = true;
            return Ok(());
        }
        Err(io) => return Err(Error::from(io)),
    };
    // The record start for LINES is the LINE start, not the value start:
    // leading indentation belongs to the record.
    stream.last_record_start = line_start;

    let outcome = {
        let mut reader = Reader::new_custom(&mut *framing, settings());
        let parsed = {
            let mut source = ReaderSource::new(&mut reader);
            loop {
                match matcher.next_root(&mut source) {
                    // A queued record needs nothing here: this pull reads the
                    // whole line before handing any record over, so the batch
                    // stays for `ReaderTail::advance`.
                    Ok(Progress::StopRoot) => {}
                    // The root value is finished; the trailing probe is left here
                    // because it consumes the reader.
                    Ok(Progress::End) => break Ok(()),
                    Err(e) => break Err(e),
                }
            }
        };
        // The source's borrow of the reader ends with the block above, so the
        // reader is whole again for the trailing probe below.
        parsed.map(|()| reader)
    };
    let reader = outcome?;
    // Ordering (verified fact 4): never peek again once the top-level value is
    // consumed; consume_trailing_whitespace is the only remaining probe. A
    // trailing byte on the line carries the pinned verbatim message (deviation
    // ⑨), not struson's.
    consume_trailing(reader, "trailing characters on line")
}

impl<R: Read> RecordReader for Lines<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        let Self {
            framing,
            matcher,
            stream,
        } = self;
        stream.advance(matcher, |stream, matcher| {
            pull_line(stream, matcher, framing)
        })
    }

    fn last_record_start(&self) -> u64 {
        self.stream.last_record_start
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use serde_json::json;

    use super::Lines;
    use crate::{
        json::testing::{FailsAfter, drain, error_of},
        record::RecordReader,
        row::Record,
        sql::parse,
    };

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
        match error_of(&mut r, "the unterminated line") {
            crate::Error::Json(e) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
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
        // mid-serve (deviation ③: invalid filler would now parse-error first).
        let filler = "a".repeat(1024 * 1024 - 8);
        let mut input = format!("{{\"x\":\"{filler}\"}}").into_bytes();
        assert_eq!(input.len() + 1, 1024 * 1024 + 1);
        input.push(b'\n');
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(input), &plan.from);
        match error_of(&mut r, "the over-cap line") {
            crate::Error::Json(e) => {
                assert!(e.is_cap(), "{e:?}");
                assert_eq!(
                    crate::Error::Json(e).to_string(),
                    "S3 select: input error: input record exceeds 1 MB"
                );
            }
            other => panic!("expected the cap error, got {other:?}"),
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
        let e = error_of(&mut r, "a blank line");
        assert!(e.to_string().contains("json input"), "got {e}");
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
            let e = error_of(&mut r, input);
            assert!(e.to_string().contains("json input"), "{input}: {e}");
        }
    }

    #[test]
    fn lines_error_is_terminal_for_later_polls() {
        // A root that parses but has trailing garbage errors on the line, and the
        // rows it queued must NOT surface on the next poll: the error is terminal.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(b"{\"a\":1} x\n".to_vec()), &plan.from);
        let e = error_of(&mut r, "the trailing garbage");
        assert!(e.to_string().contains("json input"), "{e}");
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
        let e = error_of(&mut r, "a whitespace-only line");
        assert!(e.to_string().contains("json input"), "{e}");
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
        let e = error_of(&mut r, "the lone surrogate");
        assert!(e.to_string().contains("json input"), "{e}");
    }

    #[test]
    fn hostile_lines_never_panic() {
        // Probe fact 4 + the ordering rule above: a `peek` after the top-level
        // value panics inside struson, so a malformed LINE is as dangerous as a
        // malformed DOCUMENT (document.rs's `hostile_inputs_never_panic`).
        // Every hostile line must come back `Err`; a panic anywhere aborts this
        // test.
        let mut deep = Vec::from(&b"["[..]);
        deep.resize(201, b'[');
        deep.push(b'\n');
        let hostile: Vec<Vec<u8>> = vec![
            // Trailing data after the line's value.
            b"1 2\n".to_vec(),
            b"{} {}\n".to_vec(),
            b"123x\n".to_vec(),
            b"{} x\n".to_vec(),
            // Truncated values: the line ends where the newline is.
            b"[1\n".to_vec(),
            br#"["abc"#.to_vec(),
            b"[{\"a\":\n".to_vec(),
            // A truncated final line, with no terminator at all.
            b"{\"a\":1".to_vec(),
            // A lone surrogate on the skipped path.
            br#"{"b":"\uD800","a":1}"#.to_vec(),
            // Invalid UTF-8.
            b"{\"a\":\"\xff\xfe\"}".to_vec(),
            // Nesting past the reader's 128-level limit.
            deep,
            // Empty and whitespace-only lines.
            b"\n".to_vec(),
            b"  \t \n".to_vec(),
        ];
        for input in hostile {
            let plan = parse("SELECT * FROM S3Object[*].a").unwrap();
            let mut r = Lines::new(Cursor::new(input.clone()), &plan.from);
            loop {
                match r.next() {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("{input:?}: ended cleanly instead of erroring"),
                    Err(_) => break,
                }
            }
        }
    }

    #[test]
    fn lines_pending_high_stays_bounded() {
        // A LINES pull reads one whole line before handing any record over (the
        // reader borrows the framing, so a half-read root cannot survive the
        // pull), so the buffered total is the line's matches — bounded by the
        // 1 MB line cap, not by a single record.
        let mut line = String::from("{");
        for i in 0..2_000 {
            if i > 0 {
                line.push(',');
            }
            line.push_str(&format!("\"k{i}\":{i}"));
        }
        line.push_str("}\n");
        let plan = parse("SELECT * FROM S3Object[*].*").unwrap();
        let mut r = Lines::new(Cursor::new(line.into_bytes()), &plan.from);
        let mut count = 0usize;
        while r.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2_000);
        assert!(
            r.pending_high() > 0 && r.pending_high() <= 1024 * 1024,
            "pending_high = {}",
            r.pending_high()
        );
    }

    #[test]
    fn lines_line_width_cap_still_fires_mid_serve() {
        // The `LineFraming` width cap is raised inside `Read::read` while a
        // skipped subtree is being served, so struson surfaces it as an IO
        // error; the `InvalidData` + `CAP_MESSAGE` mapping in `json::Error::from`
        // turns it back into `Error::Json(Cap)` and never leaks `Error::Io`.
        let filler = "a".repeat(1024 * 1024);
        let line = format!("{{\"skip\":\"{filler}\",\"a\":1}}\n");
        let plan = parse("SELECT * FROM S3Object[*].a").unwrap();
        let mut r = Lines::new(Cursor::new(line.into_bytes()), &plan.from);
        match error_of(&mut r, "the line-width cap") {
            crate::Error::Json(e) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the line-width cap, got {other:?}"),
        }
    }

    #[test]
    fn a_source_failing_at_the_next_line_is_an_io_error() {
        // `start_line` only asks the source for bytes (`fill_buf`); the
        // line-width cap is raised inside `LineFraming::read`, which this path
        // never enters while no line is open. So `pull_line`'s
        // `Err(io) => Error::from(io)` has no cap to carry here: a source
        // failing between two lines is the crate-level `Io` with the source's
        // own kind, and can never surface as `Error::Json(Cap)` — the reason
        // the `cap_or` helper had nothing left to do once the framing owned
        // the cap.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(
            FailsAfter {
                head: b"{\"a\":1}\n",
                at: 0,
            },
            &plan.from,
        );
        assert_eq!(r.next().unwrap(), Some(Record::Json(Some(json!({"a": 1})))));
        match error_of(&mut r, "the source failure at the next line") {
            crate::Error::Io(io) => assert_eq!(io.kind(), io::ErrorKind::ConnectionReset),
            other => panic!("expected the crate-level IO error, got {other:?}"),
        }
        assert_eq!(r.next().unwrap(), None, "the error is terminal");
    }
}
