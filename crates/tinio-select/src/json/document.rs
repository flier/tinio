//! DOCUMENT handler: one streaming pull per root value (root-array element
//! or the whole single root). Framing lives in `DocFraming`; matching in
//! `matcher`; the state shared with the LINES handler in `core`. Record
//! starts are element offsets (deviation ⑥).
//!
//! The 1 MB cap is charged per **root pull** (`Core::pull` resets the budget
//! before the `Deserializer` block), so the invariant is "one root ≤ 1 MB":
//! an array element, or the whole document in single mode. Because the charge
//! covers every byte serde is served, an *unmatched* oversized element is
//! capped exactly like a matched one — there is no skip path that escapes the
//! cap. Single mode therefore bounds deviation ③ too: a document over 1 MB
//! errors out instead of accumulating its whole match set.
//!
//! Boundary shift from the whole-document rule this replaces: a `[...]`-rooted
//! document that used to be rejected for exceeding 1 MB now streams whenever
//! every element is ≤ 1 MB (deviation ①); a scalar or object root keeps the
//! old rule, since that root *is* the record.

use std::io::{BufReader, Read};

use crate::{
    error::Error,
    json::{
        core::Core,
        framing::{DocFraming, DocMode},
        json_error,
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

/// DOCUMENT record reader: a root array yields one record per element; any
/// other root is a single record.
pub struct Document<R: Read> {
    core: Core,
    framing: DocFraming<R>,
    /// Whether any root value has been produced yet — the only thing the
    /// count was ever read for.
    produced: bool,
}

impl<R: Read> Document<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            core: Core::new(from),
            // One buffer layer — see `Lines::new` for why a second never
            // engages.
            framing: DocFraming::new(BufReader::new(reader)),
            produced: false,
        }
    }
}

fn map_err(core: &mut Core, framing: &mut DocFraming<impl Read>, e: serde_json::Error) -> Error {
    if core.budget.fired() {
        return Error::Format(CAP_MESSAGE.into());
    }
    if let Some(f) = core.failure.take() {
        return f;
    }
    if let Some(msg) = framing.framing_msg.take() {
        return Error::Format(format!("json input: {msg}"));
    }
    json_error(e)
}

/// One root pull: the seeds over the framing and — in single mode — the
/// trailing probe on the **same** `Deserializer` (see `Core::pull`).
fn pull_root<R: Read>(
    core: &mut Core,
    framing: &mut DocFraming<R>,
    produced: &mut bool,
) -> Result<(), Error> {
    // Resolved *before* the `Deserializer` exists: whether the probe runs
    // depends on the mode, and afterwards `de` holds `&mut framing`, so the
    // mode can no longer be read.
    let mode = match framing.peek_mode() {
        Ok(mode) => mode,
        Err(io) => return Err(Error::from(io)),
    };
    let (root, probe) = core.pull(framing, mode == DocMode::Single);
    match root {
        Ok(()) => {
            *produced = true;
            core.last_record_start = framing.element_start;
            if let Some(probe) = probe {
                core.done = true;
                match probe {
                    Err(e) if e.is_eof() => {}
                    Ok(()) => {
                        return Err(Error::Format(
                            "json input: trailing characters after the JSON document".into(),
                        ));
                    }
                    Err(e) => return Err(map_err(core, framing, e)),
                }
            }
            Ok(())
        }
        Err(e) if e.is_eof() => {
            core.done = true;
            match framing.mode {
                // Empty input: zero records, no error (today's behaviour).
                DocMode::Empty => {}
                // A root array that closed cleanly: `[]` yields the
                // S3Object[*] zero-match MISSING row when nothing was
                // produced; a closed array after elements needs nothing.
                DocMode::Array if framing.closed() => {
                    if !*produced {
                        core.pending.push_back(None);
                        core.last_record_start = 0;
                    }
                }
                // EOF *inside* a value — a truncated element, a truncated
                // single root, or a whitespace-only document — is a malformed
                // document. Without this arm `["abc` and `[{"a":` would be
                // reported as a clean empty array (today `from_slice` rejects
                // both).
                _ => return Err(Error::Format(format!("json input: {e}"))),
            }
            Ok(())
        }
        Err(e) => Err(map_err(core, framing, e)),
    }
}

impl<R: Read> RecordReader for Document<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        let (core, framing, produced) = (&mut self.core, &mut self.framing, &mut self.produced);
        core.advance(|core| pull_root(core, framing, produced))
    }

    fn last_record_start(&self) -> u64 {
        self.core.last_record_start
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::{Value, json};

    use super::Document;
    use crate::{json::testing::drain, record::RecordReader, row::Record, sql::parse};

    /// All records `Document` yields for `sql`'s FROM clause over `input`.
    fn read(sql: &str, input: &str) -> Vec<Record> {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        drain(
            sql,
            Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from),
        )
    }

    // ------------------------------------------------------------------
    // Reader: framing.
    // ------------------------------------------------------------------

    #[test]
    fn document_single_multiline_object() {
        let recs = read("SELECT * FROM S3Object", "{\n \"a\": 1,\n \"b\": \"x\"\n}");
        assert_eq!(recs, vec![Record::Json(Some(json!({"a": 1, "b": "x"})))]);
    }

    #[test]
    fn document_root_array_yields_elements() {
        let recs = read("SELECT * FROM S3Object", "[{\"id\": 1}, {\"id\": 2}]");
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
        assert_eq!(read("SELECT * FROM S3Object", ""), Vec::<Record>::new());
        // Zero root values from a DOCUMENT array: the `S3Object[*]` wildcard
        // zero-match rule — exactly one MISSING row, not zero rows.
        assert_eq!(
            read("SELECT * FROM S3Object", "[]"),
            vec![Record::Json(None)]
        );
    }

    // ------------------------------------------------------------------
    // Reader: traversal.
    // ------------------------------------------------------------------

    #[test]
    fn document_object_root_is_one_row_object() {
        // DOCUMENT object root = one root value (the object itself — the
        // `S3Object[*]` roots are the object, not its fields); only an
        // array root splits into its elements.
        let recs = read(
            "SELECT * FROM S3Object[*]",
            "{\"a\": 1, \"b\": \"x\", \"c\": [1, 2]}",
        );
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!({"a": 1, "b": "x", "c": [1, 2]})))]
        );
    }

    #[test]
    fn document_wild_over_object_collapses_duplicate_keys() {
        // Flagship shape end to end: the old whole-document pipeline collapsed
        // byte-identical duplicate keys last-wins in first-occurrence position
        // (serde_json's preserve_order IndexMap), so `x` is 2 and keeps its
        // original slot ahead of `y`.
        assert_eq!(
            read(
                "SELECT * FROM S3Object[*].a.*",
                "{\"a\":{\"x\":1,\"y\":9,\"x\":2}}"
            ),
            vec![Record::Json(Some(json!(2))), Record::Json(Some(json!(9)))]
        );
    }

    // ------------------------------------------------------------------
    // Reader: cap + offsets.
    // ------------------------------------------------------------------

    #[test]
    fn document_input_record_cap() {
        let doc = format!("\"{}", "a".repeat(1024 * 1024));
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(doc), &plan.from);
        match r.next() {
            Err(crate::Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn document_input_record_cap_exact_allowed() {
        let doc = format!("\"{}\"", "a".repeat(1024 * 1024 - 2));
        assert_eq!(doc.len(), 1024 * 1024);
        let recs = read("SELECT * FROM S3Object", &doc);
        assert_eq!(
            recs,
            vec![Record::Json(Some(json!("a".repeat(1024 * 1024 - 2))))]
        );
    }

    #[test]
    fn document_last_record_start_is_zero() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(
            Cursor::new("{\n \"a\": 1\n}\n".as_bytes().to_vec()),
            &plan.from,
        );
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 0);
        assert_eq!(r.next().unwrap(), None);
    }

    // ------------------------------------------------------------------
    // Reader: streaming root pulls, element offsets and error paths.
    // ------------------------------------------------------------------

    #[test]
    fn document_array_larger_than_1mb_streams() {
        // Deviation 1's main pin: a document over 1 MB total, every element
        // under 1 MB, streams through.
        let elem = |tag: &str| format!("\"{tag}{}\"", "a".repeat(400 * 1024));
        let doc = format!(
            "[{}, {}, {}, {}]",
            elem("a"),
            elem("b"),
            elem("c"),
            elem("d")
        );
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(doc.into_bytes()), &plan.from);
        for expected in ["a", "b", "c", "d"] {
            match r.next().unwrap() {
                Some(Record::Json(Some(Value::String(s)))) => {
                    assert!(s.starts_with(expected), "record tag {expected}")
                }
                other => panic!("expected string record, got {other:?}"),
            }
        }
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn document_element_over_cap_errors_after_prior_records() {
        // Deviations 1 + 5: the first element is produced, the second is over
        // the cap and errors the stream in-stream.
        let big = "a".repeat(1024 * 1024);
        let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(doc.into_bytes()), &plan.from);
        assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
        match r.next() {
            Err(crate::Error::Format(msg)) => assert_eq!(msg, "input record exceeds 1 MB"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn document_record_starts_are_element_offsets_monotonic() {
        // Deviation 6: record starts are the elements' real offsets, MISSING
        // rows included (they report the start of the element they derive
        // from), and the sequence is monotonic non-decreasing.
        let plan = parse("SELECT * FROM S3Object[*].a").unwrap();
        let mut r = Document::new(
            Cursor::new(b"[{\"a\": 1}, {\"b\": 2}, {\"a\": 3}]".to_vec()),
            &plan.from,
        );
        let mut starts = Vec::new();
        while let Some(rec) = r.next().unwrap() {
            let _ = rec;
            starts.push(r.last_record_start());
        }
        assert_eq!(starts.len(), 3);
        assert_eq!(starts[0], 1);
        assert!(starts.windows(2).all(|w| w[0] <= w[1]), "{starts:?}");
    }

    #[test]
    fn document_empty_array_missing_start_is_zero() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(b"[]".to_vec()), &plan.from);
        assert_eq!(r.next().unwrap(), Some(Record::Json(None)));
        assert_eq!(r.last_record_start(), 0);
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn document_single_root_trailing_garbage_errors() {
        // `5x` / `123#` are the cases that expose a lost peeked byte: `IoRead`
        // parks the byte after a bare number inside itself, so a *fresh*
        // Deserializer for the probe would read past it and accept the record.
        // Our own trailing-character message is pinned verbatim; the rest carry
        // serde's wording, so they are only required to be a `json input` error
        // *other than* that message — pinning serde's text would break on a
        // serde_json upgrade and add no signal.
        for (input, ours) in [
            ("{} x", false),
            ("{} {}", true),
            ("5 {}", true),
            ("5x", false),
            ("123#", false),
        ] {
            let plan = parse("SELECT * FROM S3Object").unwrap();
            let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
            match r.next() {
                Err(crate::Error::Format(m)) => {
                    assert!(m.contains("json input"), "{input}: {m}");
                    assert_eq!(
                        m == "json input: trailing characters after the JSON document",
                        ours,
                        "{input}: {m}"
                    );
                }
                other => panic!("{input}: expected format error, got {other:?}"),
            }
        }
    }

    #[test]
    fn document_scalar_roots() {
        for (input, expected) in [
            ("5", Some(Record::Json(Some(serde_json::json!(5))))),
            ("\"x\"", Some(Record::Json(Some(serde_json::json!("x"))))),
            ("null", Some(Record::Json(Some(serde_json::Value::Null)))),
        ] {
            let plan = parse("SELECT * FROM S3Object").unwrap();
            let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
            assert_eq!(r.next().unwrap(), expected, "{input}");
            assert_eq!(r.next().unwrap(), None, "{input}");
        }
    }

    #[test]
    fn document_error_is_terminal_for_later_polls() {
        // Ruling 10: a terminal error drops the failing pull's rows, so a caller
        // that keeps polling sees a finished stream. Single-value mode is the
        // shape where the root is already materialised when the trailing probe
        // fails, so `pending` is non-empty at the error site.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(b"5x".to_vec()), &plan.from);
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
    fn document_whitespace_only_is_a_format_error() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(b"  \n".to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
            other => panic!("expected format error, got {other:?}"),
        }
    }

    #[test]
    fn document_row_order_table() {
        // Row-order table: wildcard fan-out interleaved with MISSING (the
        // enumeration-style complement to the apply-order equivalence).
        let cases: &[(&str, &str, Vec<Record>)] = &[
            (
                "SELECT * FROM S3Object[*].a[*]",
                "{\"a\": [1, 2]}\n",
                vec![
                    Record::Json(Some(serde_json::json!(1))),
                    Record::Json(Some(serde_json::json!(2))),
                ],
            ),
            (
                "SELECT * FROM S3Object[*].a[*].b",
                "{\"a\": [{\"b\": 1}, {}, {\"b\": 2}]}\n",
                vec![
                    Record::Json(Some(serde_json::json!(1))),
                    Record::Json(None),
                    Record::Json(Some(serde_json::json!(2))),
                ],
            ),
        ];
        for (sql, input, expected) in cases {
            let plan = parse(sql).unwrap();
            let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
            let mut got = Vec::new();
            while let Some(rec) = r.next().unwrap() {
                got.push(rec);
            }
            assert_eq!(&got, expected, "{sql}");
        }
    }

    #[test]
    fn document_unterminated_input_is_a_format_error() {
        // The spec's failure-path list requires this at the handler level (only
        // the framing-level `[1` case exists today). An EOF *inside* a value is a
        // malformed document, never a clean end: `["abc` and `[{"a":` are the cases
        // that would otherwise surface as a clean empty array, and `[1, "abc`
        // pins the deviation-5 shape (the first record stands, then the error).
        for (input, rows_before) in [
            ("[1", 1usize),
            ("[\"abc", 0),
            ("[{\"a\":", 0),
            ("[1, \"abc", 1),
            ("{\"a\":", 0),
        ] {
            let plan = parse("SELECT * FROM S3Object").unwrap();
            let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
            let mut seen = 0usize;
            loop {
                match r.next() {
                    Ok(Some(_)) => seen += 1,
                    Ok(None) => panic!("{input}: ended cleanly after {seen} records"),
                    Err(crate::Error::Format(m)) => {
                        assert!(m.contains("json input"), "{input}: {m}");
                        break;
                    }
                    Err(e) => panic!("{input}: expected a format error, got {e:?}"),
                }
            }
            assert_eq!(seen, rows_before, "{input}");
        }
    }

    #[test]
    fn document_array_root_keeps_pending_bounded() {
        // The architecture's promise, pinned portably (the spec's counting
        // allocator is ruled out by `unsafe_code = "forbid"`): one *root* is in
        // flight, so the buffered bytes never approach a document's worth. The
        // yardstick is element size, not record count — 20 000 tiny objects and
        // one 1 MB object are the same count and nothing alike in memory.
        let mut doc = Vec::from(&b"["[..]);
        for i in 0..20_000 {
            if i > 0 {
                doc.push(b',');
            }
            doc.extend_from_slice(format!("{{\"a\": {i}}}").as_bytes());
        }
        doc.push(b']');
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(doc), &plan.from);
        let mut count = 0u64;
        while r.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 20_000);
        assert!(
            r.core.pending_high.get() <= 64,
            "pending peaked at {} bytes — an array root buffers one element's matches",
            r.core.pending_high.get()
        );
    }

    #[test]
    fn document_single_root_over_cap_errors() {
        // Deviation 3's tier under the per-root cap: the tier is *not* unbounded.
        // One pull accumulates the document's whole match set, and that pull is
        // charged like any other — so an over-1 MB single-value root errors
        // instead of growing without limit. This replaces the older assertion
        // that the tier was unbounded (`pending_high == 5_000`).
        let plan = parse("SELECT * FROM S3Object[*].a[*]").unwrap();
        let mut doc = Vec::from(&b"{\"a\": ["[..]);
        for i in 0..200_000 {
            if i > 0 {
                doc.push(b',');
            }
            doc.extend_from_slice(i.to_string().as_bytes());
        }
        doc.extend_from_slice(b"]}");
        assert!(doc.len() > 1024 * 1024, "the fixture must exceed the cap");
        let mut r = Document::new(Cursor::new(doc), &plan.from);
        loop {
            match r.next() {
                Ok(Some(_)) => continue,
                Err(crate::Error::Format(msg)) => {
                    assert_eq!(msg, "input record exceeds 1 MB");
                    break;
                }
                other => panic!("expected the cap error, got {other:?}"),
            }
        }
    }

    #[test]
    fn document_single_root_under_cap_yields_all_matches() {
        // The other side of the same boundary: under the cap the tier behaves as
        // deviation 3 describes — one pull, every match, in order.
        let plan = parse("SELECT * FROM S3Object[*].a[*]").unwrap();
        let mut doc = Vec::from(&b"{\"a\": ["[..]);
        for i in 0..5_000 {
            if i > 0 {
                doc.push(b',');
            }
            doc.extend_from_slice(i.to_string().as_bytes());
        }
        doc.extend_from_slice(b"]}");
        let mut r = Document::new(Cursor::new(doc), &plan.from);
        let mut count = 0u64;
        while r.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5_000);
        assert!(
            r.core.pending_high.get() > 0 && r.core.pending_high.get() <= 1024 * 1024,
            "a single-value root accumulates its match set in one pull (deviation 3), \
             bounded by the record cap: peak {} bytes",
            r.core.pending_high.get()
        );
    }
}
