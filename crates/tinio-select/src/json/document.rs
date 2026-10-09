//! DOCUMENT handler: one record per root value, except that a root array
//! yields one record per element. The root shape is discovered from the event
//! flow — there is no framing layer and no document buffer.
//!
//! Cap sampling (design spec, byte-supply section): each element span starts
//! after its has_next() and its peek(), and ends after the value is read, so
//! the span excludes the separators and the closing bracket. A single-value
//! root spans the whole document.

use std::io::Read;

use crate::{
    error::{Error, JsonInputError},
    json::{
        feed::{BudgetedRead, RootBudget},
        matcher::{CompiledPath, Matcher, Progress, ValueKind, ValueSource},
        source::{Reader, ReaderSource, consume_trailing, position, settings},
        tail::ReaderTail,
    },
    record::RecordReader,
    row::Record,
    sql::FromClause,
};

/// DOCUMENT record reader: a root array yields one record per element; any
/// other root is a single record.
pub struct Document<R: Read> {
    /// Built over the charged `Read` backstop, so a matched value that vastly
    /// exceeds the cap cannot be materialized whole before the cap rejects it.
    reader: Option<Reader<BudgetedRead<R>>>,
    matcher: Matcher,
    budget: RootBudget,
    stream: ReaderTail,
    state: RootState,
    /// Did any root element open? Read only for the empty-root-array rule: a
    /// root array that closed with nothing in it still owes one MISSING row.
    produced: bool,
}

enum RootState {
    /// Nothing read yet: the reader is lazy, so the shape is decided before the
    /// first byte is consumed (verified fact 3).
    Undecided,
    /// The root is an array. in_element records whether an element is
    /// **half-read**: a previous poll delivered that element's one record and
    /// returned, so the next poll must resume it instead of calling has_next()
    /// again (calling it mid-element is incorrect usage).
    Array { in_element: bool },
    /// The root is a single value. Whether its value has been fully consumed is
    /// `read_element`'s return value, not state: the same pull that reads the
    /// value to its end runs the terminal checks.
    Single,
    /// The document is over: only the cap and trailing checks remain.
    Done,
}

impl<R: Read> Document<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        let budget = RootBudget::with_default_cap();
        Self {
            reader: Some(Reader::new_custom(
                BudgetedRead::new(reader, &budget),
                settings(),
            )),
            matcher: Matcher::new(CompiledPath::from_segments(&from.segments)),
            budget,
            stream: ReaderTail::new(),
            state: RootState::Undecided,
            produced: false,
        }
    }

    /// The pending high-water mark, in rendered bytes (test tripwire).
    #[cfg(test)]
    pub(crate) fn pending_high(&self) -> usize {
        self.matcher.pending_high()
    }
}

/// The reader-slot invariant, spelled once for its three readers: the reader
/// is present until `finish_document` takes it out at `Done`, so an empty slot
/// is never a state a pull can reach.
fn reader_absent() -> Error {
    Error::Invariant("the document reader is present until Done")
}

/// Borrow the document reader as a ValueSource. The reader lives for the whole
/// document, so its position and container stack survive each poll; the slot
/// empties only in `finish_document`, which latches `Done` in the same pull.
fn source<R: Read>(reader: &mut Option<Reader<R>>) -> Result<ReaderSource<'_, Reader<R>>, Error> {
    reader
        .as_mut()
        .map(ReaderSource::new)
        .ok_or_else(reader_absent)
}

/// The document reader itself, still in its slot: `source` lends it to the
/// matcher's adapter, while the free `position` samples the reader directly.
fn as_reader<R: Read>(reader: &Option<Reader<R>>) -> Result<&Reader<R>, Error> {
    reader.as_ref().ok_or_else(reader_absent)
}

/// One root pull: decide the root shape, then read one batch of the current
/// root value. Readable across calls — an array element may take several polls,
/// because a batch boundary is one completed record.
fn pull_root<R: Read>(
    reader: &mut Option<Reader<R>>,
    matcher: &mut Matcher,
    budget: &mut RootBudget,
    stream: &mut ReaderTail,
    state: &mut RootState,
    produced: &mut bool,
) -> Result<(), Error> {
    loop {
        match state {
            RootState::Undecided => {
                // The shape decision only fixes the state; the loop below then runs
                // the branch it selected.
                decide_shape(reader, budget, stream, state)?;
                continue;
            }
            RootState::Array { in_element: true } => {
                read_element(reader, matcher, budget, state)?;
                return Ok(());
            }
            RootState::Array { in_element: false } => {
                let has_next = source(reader)?.has_next()?;
                let start = position(as_reader(reader)?)?;
                if !has_next {
                    // `has_next()` skipped the whitespace before `]`, so this
                    // position is past it: the document-level end. The budget
                    // decides whether it is measured — once an element's
                    // `begin_element` moved the origin, that element's own span
                    // (checked at its `End`) is the authority, and this end
                    // reaches past it through the whitespace and the bracket.
                    let end = {
                        let end = position(as_reader(reader)?)?;
                        source(reader)?.end_array()?;
                        end
                    };
                    if !*produced {
                        // The root array closed with no element: the S3Object[*]
                        // zero-match rule owes exactly one MISSING row at offset 0.
                        stream.last_record_start = 0;
                        matcher.queue_missing();
                    }
                    return finish_document(reader, budget, stream, state, end);
                }
                // The element span opens after has_next, so it excludes the
                // separators and any whitespace between elements.
                stream.last_record_start = start;
                budget.begin_element(start);
                *state = RootState::Array { in_element: true };
                *produced = true;
                read_element(reader, matcher, budget, state)?;
                return Ok(());
            }
            RootState::Single => {
                // A single-value root is one record, so the cap and trailing checks
                // must run before that record is delivered: keep reading to the end
                // of the root in this same pull. `read_element` reports the root
                // value as ended, which exits the loop for the terminal checks; a
                // single-value pull never re-enters with the value already
                // consumed, because that same pull runs the checks.
                while !read_element(reader, matcher, budget, state)? {
                    // Still in flight: the next iteration continues the value.
                }
                // The single-value root's span is the whole document, so this
                // check stays as it was: the budget origin is the root start.
                let end = position(as_reader(reader)?)?;
                return finish_document(reader, budget, stream, state, end);
            }
            RootState::Done => {
                // A defensive no-op: the empty-document path re-enters here
                // with `stream.done` already latched by `decide_shape`.
                stream.done = true;
                return Ok(());
            }
        }
    }
}

/// The first read decides whether the root is an array or a single value.
fn decide_shape<R: Read>(
    reader: &mut Option<Reader<R>>,
    budget: &mut RootBudget,
    stream: &mut ReaderTail,
    state: &mut RootState,
) -> Result<(), Error> {
    let outcome = source(reader)?.peek();
    let (kind, start) = match outcome {
        Ok(kind) => (kind, position(as_reader(reader)?)?),
        // Empty input is zero records; whitespace-only input is a format error.
        // Both report IncompleteDocument at the first peek (fact 10) and only
        // the consumed-byte position tells them apart.
        Err(e) => {
            empty_or(reader, stream, e)?;
            *state = RootState::Done;
            return Ok(());
        }
    };
    // The root value start is the position right after peek (fact 3).
    stream.last_record_start = start;
    budget.begin(start);
    if kind == ValueKind::Array {
        source(reader)?.begin_array()?;
        *state = RootState::Array { in_element: false };
        return Ok(());
    }
    *state = RootState::Single;
    Ok(())
}

/// Read one batch of the current root value, and report whether the root value
/// finished: StopRoot means leave with a record queued; End means the element or
/// the single-value root is over.
///
/// The cap is checked on both paths. A record usually completes before its
/// element's remaining bytes have been read — a `SELECT a` over
/// `[{"a":1,"b":"..."}]` hands the record over at the end of `a` — so charging
/// only at `StopRoot` would leave the element's whole tail uncharged. At `End`
/// the position is right after the element's value, before the next `has_next`,
/// which is exactly the span the spec's byte-supply table gives an element.
///
/// A record handed over at `StopRoot` can therefore come from an element whose
/// cap verdict is still open: it IS delivered, and if that element then turns
/// out to be over the cap the verdict arrives at its own `End` and the error is
/// terminal for what follows (deviation ④, "records from before the failing
/// element are produced", extended to the failing element's own records). That
/// `End` sample is the authority for an element's span: the element's
/// `begin_element` moved the budget origin to its own start, so the
/// document-level check that closes the root no longer measures this span.
fn read_element<R: Read>(
    reader: &mut Option<Reader<R>>,
    matcher: &mut Matcher,
    budget: &mut RootBudget,
    state: &mut RootState,
) -> Result<bool, Error> {
    let progress = {
        let mut source = source(reader)?;
        matcher.next_root(&mut source)?
    };
    let end = position(as_reader(reader)?)?;
    match progress {
        // Cap-check this root's span so far; the rest of the value is read by
        // the next pull.
        Progress::StopRoot => {
            budget.check_or_cap(end)?;
            Ok(false)
        }
        // The root value is done: charge its full span, then close the state so
        // the next step runs the terminal checks (the next element, or the
        // document stream).
        Progress::End => {
            budget.check_or_cap(end)?;
            if matches!(state, RootState::Array { .. }) {
                *state = RootState::Array { in_element: false };
            }
            Ok(true)
        }
    }
}

/// Check the closing span of the root, then the terminal trailing probe, in
/// that order. A failure is terminal — `ReaderTail::advance` drops the queued
/// batch, which is the one place that rule lives; only a clean tail lets
/// `stream.done` deliver the batch.
///
/// `end` is the document-level end position. Whether it is measured is the
/// budget's call (`check_root_span`): it applies only while the root origin is
/// still the current one, so the handler states the position and nothing else.
fn finish_document<R: Read>(
    reader: &mut Option<Reader<R>>,
    budget: &mut RootBudget,
    stream: &mut ReaderTail,
    state: &mut RootState,
    end: u64,
) -> Result<(), Error> {
    budget.check_root_span(end)?;
    *state = RootState::Done;
    // The trailing probe consumes the reader, so it is taken out for good. The
    // message is bare text: `JsonError::Other` renders it as `json input: {0}`,
    // so the crate-level Display keeps the old verbatim wording (deviation ⑨).
    let Some(owned) = reader.take() else {
        return Err(reader_absent());
    };
    consume_trailing(owned, "trailing characters after the JSON document")?;
    stream.done = true;
    Ok(())
}

/// A failure at the very first peek(): IncompleteDocument with nothing consumed
/// is an empty document — zero records, no error. The same error after
/// whitespace was consumed is a whitespace-only document, a format error.
fn empty_or<R: Read>(
    reader: &Option<Reader<R>>,
    stream: &mut ReaderTail,
    err: Error,
) -> Result<(), Error> {
    // The payload is the public newtype; its `pub(crate)` field is what lets
    // the classifier inside be reached here. The reader is present by
    // construction: only `finish_document` takes it out, at `Done`.
    let Error::Json(JsonInputError(json)) = &err else {
        return Err(err);
    };
    if json.is_incomplete_document() && position(as_reader(reader)?)? == 0 {
        stream.done = true;
        return Ok(());
    }
    Err(err)
}

impl<R: Read> RecordReader for Document<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        let Self {
            reader,
            matcher,
            budget,
            stream,
            state,
            produced,
        } = self;
        stream.advance(matcher, |stream, matcher| {
            pull_root(reader, matcher, budget, stream, state, produced)
        })
    }

    fn last_record_start(&self) -> u64 {
        self.stream.last_record_start
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        io::{self, Cursor, Read},
        rc::Rc,
    };

    use serde_json::{Value, json};

    use super::{Document, RootState, as_reader, decide_shape, source};
    use crate::{
        json::{
            feed::RootBudget,
            matcher::ValueSource,
            source::{Reader, position, settings},
            tail::ReaderTail,
            testing::{self, FailsAfter, drain},
        },
        record::{CAP_ALLOWANCE, MAX_RECORD, RecordReader},
        row::Record,
        sql::parse,
    };

    /// All records `Document` yields for `sql`'s FROM clause over `input`.
    fn read(sql: &str, input: &str) -> Vec<Record> {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        drain(
            sql,
            Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from),
        )
    }

    /// The terminal error `Document` reports for `input`: the shared driver over
    /// a `Document` built from `sql`'s FROM clause.
    fn error_of(sql: &str, input: &str) -> crate::Error {
        let plan = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        testing::error_of(&mut r, &format!("{sql} over {input:?}"))
    }

    /// A reader counting the bytes it has served, so a test can prove the
    /// charged backstop aborted mid-read instead of draining the value.
    struct CountingReader<R> {
        inner: R,
        served: Rc<Cell<u64>>,
    }

    impl<R: Read> Read for CountingReader<R> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(out)?;
            self.served.set(self.served.get() + n as u64);
            Ok(n)
        }
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
        // The fixture must be well-formed JSON. A single-value root is charged
        // only at a batch boundary, so a truncated over-cap value reports its
        // parse error (IncompleteDocument) before the cap can fire. The span is
        // still exactly 1 MB + 1, now valid.
        let doc = format!("\"{}\"", "a".repeat(1024 * 1024 - 1));
        assert_eq!(doc.len(), 1024 * 1024 + 1);
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(doc), &plan.from);
        match r.next() {
            Err(crate::Error::Json(e)) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
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
            Err(crate::Error::Json(e)) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
        }
    }

    #[test]
    fn document_element_tail_is_charged_after_an_early_match() {
        // A record can complete long before its element does: here the second
        // element's match ends at `[2]` and the rest of the element is a large
        // unmatched member, so the record is delivered and the element then runs
        // to its End with the whole tail uncharged. Charging only at StopRoot
        // would let that tail sail past the 1 MB per-element cap — and because
        // the element is not the last one, the document tail never charges it
        // either.
        let filler = "x".repeat(1024 * 1024 + 64);
        let doc = format!("[{{\"a\":[1]}}, {{\"a\":[2], \"junk\":\"{filler}\"}}, {{\"a\":[3]}}]");
        let plan = parse("SELECT * FROM S3Object[*].a[*]").unwrap();
        let mut r = Document::new(Cursor::new(doc.into_bytes()), &plan.from);
        assert_eq!(r.next().unwrap(), Some(Record::Json(Some(json!(1)))));
        assert_eq!(r.next().unwrap(), Some(Record::Json(Some(json!(2)))));
        match r.next() {
            Err(crate::Error::Json(e)) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
        }
    }

    #[test]
    fn document_record_starts_are_element_offsets_monotonic() {
        // Deviation ⑤: record starts are the elements' real offsets, MISSING
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
        // Deviation ⑨ (extended): `5x` / `123#` fail during the value read
        // (struson's `TrailingDataAfterNumber`), so they never reach the
        // trailing probe and do not carry our synthesized message. Every value
        // followed by a non-whitespace byte — `{} {}`, `5 {}`, `{} x` — goes
        // through `consume_trailing_whitespace`, which reports ANY trailing
        // byte as `TrailingData`, so the handler synthesizes its message.
        // The retired kernel could tell a parseable second value from garbage
        // (`Skip` over a fresh `Deserializer`); struson's public API cannot,
        // so `{} x` now carries our message too (see
        // `tail_check_synthesizes_our_messages`).
        for (input, ours) in [
            ("{} x", true),
            ("{} {}", true),
            ("5 {}", true),
            ("5x", false),
            ("123#", false),
        ] {
            let plan = parse("SELECT * FROM S3Object").unwrap();
            let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
            match r.next() {
                Err(crate::Error::Json(e)) => {
                    let m = e.to_string();
                    assert!(m.contains("json input"), "{input}: {m}");
                    assert_eq!(
                        m == "json input: trailing characters after the JSON document",
                        ours,
                        "{input}: {m}"
                    );
                }
                other => panic!("{input}: expected a JSON error, got {other:?}"),
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
            Err(crate::Error::Json(e)) => assert!(e.to_string().contains("json input"), "{e}"),
            other => panic!("expected a JSON error, got {other:?}"),
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
            Err(crate::Error::Json(e)) => assert!(e.to_string().contains("json input"), "{e}"),
            other => panic!("expected a JSON error, got {other:?}"),
        }
    }

    #[test]
    fn a_source_failure_at_the_first_peek_is_not_an_empty_document() {
        // The first `peek` is where an empty document is recognized: nothing
        // consumed plus IncompleteDocument is zero records and NO error (fact
        // 10). `empty_or` may swallow only that one shape, so a source failing
        // before the first byte has to keep its own value — otherwise a broken
        // input would be reported as a clean, empty S3Object.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(FailsAfter { head: b"", at: 0 }, &plan.from);
        match r.next() {
            Err(crate::Error::Io(io)) => assert_eq!(io.kind(), io::ErrorKind::ConnectionReset),
            other => panic!("expected the crate-level IO error, got {other:?}"),
        }
        assert_eq!(r.next().unwrap(), None, "the error is terminal");
    }

    #[test]
    fn a_source_failure_at_the_trailing_probe_is_an_io_error() {
        // The document's last source read is `consume_trailing_whitespace`.
        // Trailing *bytes* there are the handler's own synthesized message
        // (deviation ⑨), but a source failing at that read is a real IO
        // failure and must come out as the crate-level `Io` — classified by
        // `consume_trailing`, not reported as a malformed document.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(
            FailsAfter {
                head: b"{\"a\": 1}",
                at: 0,
            },
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Io(io)) => assert_eq!(io.kind(), io::ErrorKind::ConnectionReset),
            other => panic!("expected the crate-level IO error, got {other:?}"),
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
        // pins the deviation-4 shape (the first record stands, then the error).
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
                    Err(crate::Error::Json(e)) => {
                        assert!(e.to_string().contains("json input"), "{input}: {e}");
                        break;
                    }
                    Err(e) => panic!("{input}: expected a JSON error, got {e:?}"),
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
            r.pending_high() <= 64,
            "pending peaked at {} bytes — an array root buffers one element's matches",
            r.pending_high()
        );
    }

    #[test]
    fn document_single_root_over_cap_errors() {
        // Deviation ⑦'s tier under the per-root cap: the tier is *not* unbounded.
        // A single-value root is read to the end of the root inside one pull, so
        // the cap and the trailing check run before any of its records are
        // delivered, and the cap is charged at every batch boundary against the
        // root's start — an over-1 MB single-value root therefore errors instead
        // of buffering its whole match set.
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
                Err(crate::Error::Json(e)) => {
                    assert!(e.is_cap(), "{e:?}");
                    break;
                }
                other => panic!("expected the cap error, got {other:?}"),
            }
        }
    }

    #[test]
    fn document_single_root_over_cap_aborts_mid_read() {
        // The exact-span check is the cap authority, but it can only run once
        // the matched value is fully materialized. An empty compiled path
        // (`SELECT * FROM S3Object`) reads a whole single-value root in one
        // `next_root`, so without the charged backstop a multi-megabyte root
        // would be materialized before the cap rejected it. The backstop must
        // abort at `MAX_RECORD + CAP_ALLOWANCE` and leave the input undrained.
        let total = 4 * 1024 * 1024;
        let doc = format!("\"{}\"", "a".repeat(total));
        let served = Rc::new(Cell::new(0u64));
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(
            CountingReader {
                inner: Cursor::new(doc.clone().into_bytes()),
                served: Rc::clone(&served),
            },
            &plan.from,
        );
        match r.next() {
            Err(crate::Error::Json(e)) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
        }
        // The abort is charged, not span-checked: the backstop fires within one
        // struson read of `MAX_RECORD + CAP_ALLOWANCE`, having served the whole
        // first megabyte. The 4 MB value is therefore never materialized.
        let read = served.get();
        assert!(
            read > MAX_RECORD,
            "the backstop fired before the cap: {read} bytes served"
        );
        assert!(
            read <= MAX_RECORD + CAP_ALLOWANCE + 4096,
            "the over-cap value was not aborted mid-read: {read} of {} bytes served",
            doc.len()
        );
    }

    #[test]
    fn document_single_root_under_cap_yields_all_matches() {
        // The other side of the same boundary: under the cap the single pull
        // buffers the root's whole match set — every match, in order — and the
        // per-root charge that makes the over-cap case error passes.
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
            r.pending_high() > 0 && r.pending_high() <= 1024 * 1024,
            "one pull buffers the single-value root's whole match set, bounded by the \
             per-root record cap: peak {} bytes",
            r.pending_high()
        );
    }

    // ------------------------------------------------------------------
    // DocFraming take-over (spec test plan Q22=a+c) and the new pins.
    // ------------------------------------------------------------------

    #[test]
    fn document_element_offsets_are_byte_exact() {
        // Probe fact 7: `[{"a": 1}, {"b": 2}]` — the elements start at byte 1
        // and byte 11; the `has_next` that skipped the comma skipped the space
        // too, so the `peek` that followed moved `data_pos` by zero.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(
            Cursor::new(b"[{\"a\": 1}, {\"b\": 2}]".to_vec()),
            &plan.from,
        );
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 1);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 11);
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn document_scalar_and_nested_elements() {
        // Every element is one record, and the `]` inside the string must not
        // be mistaken for the array end.
        assert_eq!(
            read(
                "SELECT * FROM S3Object",
                "[1, \"two\", true, null, [1, 2], {\"a\": \"x]y\"}]"
            ),
            vec![
                Record::Json(Some(json!(1))),
                Record::Json(Some(json!("two"))),
                Record::Json(Some(json!(true))),
                Record::Json(Some(Value::Null)),
                Record::Json(Some(json!([1, 2]))),
                Record::Json(Some(json!({"a": "x]y"}))),
            ]
        );
    }

    #[test]
    fn document_single_object_root_start_is_zero() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(b"{\"a\": 1}".to_vec()), &plan.from);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 0);
        assert_eq!(r.next().unwrap(), None);
    }

    #[test]
    fn document_array_separator_errors_are_format_errors() {
        // Probe facts 8 and 9: the first three are struson syntax errors at the
        // array level; `[1] x` is caught by the document's trailing probe.
        for input in ["[1,]", "[1 2]", "[1", "[1] x"] {
            assert!(
                matches!(
                    error_of("SELECT * FROM S3Object", input),
                    crate::Error::Json(_)
                ),
                "{input}"
            );
        }
    }

    #[test]
    fn document_empty_vs_whitespace_only() {
        // Probe fact 10: both report IncompleteDocument at the first peek; the
        // consumed-byte position tells an empty document (zero records) from a
        // whitespace-only one (a format error).
        assert_eq!(read("SELECT * FROM S3Object", ""), Vec::<Record>::new());
        assert!(matches!(
            error_of("SELECT * FROM S3Object", "  \n"),
            crate::Error::Json(_)
        ));
    }

    #[test]
    fn cap_is_measured_over_the_whole_root() {
        // Deviation ⑥: the cap is the root value's own span, so an array
        // element of exactly 1 MB and a single-value root of exactly 1 MB both
        // pass at the line, while one byte more on an element fires the cap.
        let exact = format!("\"{}\"", "a".repeat(1024 * 1024 - 2));
        assert_eq!(exact.len(), 1024 * 1024);
        let payload = json!("a".repeat(1024 * 1024 - 2));
        assert_eq!(
            read("SELECT * FROM S3Object", &format!("[{exact}]")),
            vec![Record::Json(Some(payload.clone()))]
        );
        assert_eq!(
            read("SELECT * FROM S3Object", &exact),
            vec![Record::Json(Some(payload))]
        );
        let over = format!("\"{}\"", "a".repeat(1024 * 1024 - 1));
        assert_eq!(over.len(), 1024 * 1024 + 1);
        match error_of("SELECT * FROM S3Object", &format!("[{over}]")) {
            crate::Error::Json(e) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
        }
    }

    #[test]
    fn cap_exact_last_element_ignores_whitespace_before_the_bracket() {
        // The last element's span ends at its value: `has_next()` skips the
        // whitespace before `]` (struson stream_reader.rs:2068 → 640 → 544), so
        // an element of exactly 1 MB followed by that whitespace must not be
        // charged for it. Pretty-printed JSON is the common shape.
        let exact = format!("\"{}\"", "a".repeat(1024 * 1024 - 2));
        assert_eq!(exact.len(), 1024 * 1024);
        let payload = json!("a".repeat(1024 * 1024 - 2));
        for (label, doc) in [
            ("space", format!("[{exact} ]")),
            ("newline", format!("[{exact}\n]")),
        ] {
            assert_eq!(
                read("SELECT * FROM S3Object", &doc),
                vec![Record::Json(Some(payload.clone()))],
                "{label}: a legal 1 MB element must survive the close"
            );
        }
    }

    #[test]
    fn cap_under_the_cap_element_ignores_whitespace_before_the_bracket() {
        // The same sampling rule one byte class lower: whitespace before `]`
        // that would push the MEASURED span past the cap is still not charged
        // to the element, so a slightly undersized element passes.
        let filler = "a".repeat(1024 * 1024 - 10);
        let element = format!("\"{filler}\"");
        assert_eq!(element.len(), 1024 * 1024 - 8);
        let doc = format!("[{element}  \n   \n  ]");
        // 9 whitespace bytes + `[` + `]`: the old end-at-the-bracket sample
        // would have measured 1 MB + 1.
        assert_eq!(doc.len(), element.len() + 11);
        assert_eq!(
            read("SELECT * FROM S3Object", &doc),
            vec![Record::Json(Some(json!(filler)))]
        );
    }

    #[test]
    fn cap_exact_element_survives_whitespace_past_the_backstop_slack() {
        // The charged backstop must not reject a root whose own span is legal:
        // after an element's `End` span check passed, the whitespace the next
        // `has_next()` consumes before `]` is served to a finished element, so
        // an exactly-1 MB element followed by more than CAP_ALLOWANCE of it
        // used to abort with `Cap`. A passing exact span check clears the
        // charge, which starts a fresh account at that verified boundary, and
        // the element parses.
        let exact = format!("\"{}\"", "a".repeat(1024 * 1024 - 2));
        assert_eq!(exact.len(), 1024 * 1024);
        let gap = " ".repeat(CAP_ALLOWANCE as usize + 1024);
        assert_eq!(
            read("SELECT * FROM S3Object", &format!("[{exact}{gap}]")),
            vec![Record::Json(Some(json!("a".repeat(1024 * 1024 - 2))))]
        );
    }

    #[test]
    fn cap_one_byte_over_still_fires_before_the_bracket() {
        // The fix must not weaken the cap: the element's own `End` is the
        // authority, and the whitespace before `]` never enters that verdict.
        let over = format!("\"{}\"", "a".repeat(1024 * 1024 - 1));
        assert_eq!(over.len(), 1024 * 1024 + 1);
        match error_of("SELECT * FROM S3Object", &format!("[{over} \n ]")) {
            crate::Error::Json(e) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
        }
    }

    #[test]
    fn cap_fires_after_the_prior_records() {
        // Deviations ④ + ⑥: the records before the over-cap element are
        // delivered, and the error is terminal — the element after it never
        // surfaces.
        let big = "a".repeat(1024 * 1024);
        let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}, {{\"id\": 3}}]");
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(doc.into_bytes()), &plan.from);
        assert_eq!(
            r.next().unwrap(),
            Some(Record::Json(Some(json!({"id": 1}))))
        );
        match r.next() {
            Err(crate::Error::Json(e)) => assert!(e.is_cap(), "{e:?}"),
            other => panic!("expected the cap error, got {other:?}"),
        }
        assert_eq!(r.next().unwrap(), None, "the cap error is terminal");
    }

    #[test]
    fn tail_check_synthesizes_our_messages() {
        // Deviation ⑨ + probe fact 9: after a top-level value the only trailing
        // probe is `consume_trailing_whitespace`, and it reports every
        // following non-whitespace byte as `TrailingData`, so the handler
        // synthesizes its own message. `5x` / `123#` fail during the value read
        // (struson's `TrailingDataAfterNumber`) and never reach the probe.
        for (input, ours) in [
            ("{} {}", true),
            ("5 {}", true),
            ("{} x", true),
            ("5x", false),
            ("123#", false),
        ] {
            let m = error_of("SELECT * FROM S3Object", input).to_string();
            assert!(m.contains("json input"), "{input}: {m}");
            assert_eq!(
                m.contains("json input: trailing characters after the JSON document"),
                ours,
                "{input}: {m}"
            );
        }
    }

    #[test]
    fn document_duplicate_keys_collapse_last_wins_first_position() {
        // Deviation ⑩: the three serde reserved-key tests are gone; the
        // duplicate-key collapse is pinned here with no dependency on engine
        // internals. A byte-identical duplicate replaces its member in place,
        // so it keeps the first occurrence's position and carries the last
        // value — the retired `preserve_order` IndexMap behaviour.
        assert_eq!(
            read(
                "SELECT * FROM S3Object[*].m.*",
                "{\"m\": {\"x\": 1, \"y\": 9, \"x\": 2}}"
            ),
            vec![Record::Json(Some(json!(2))), Record::Json(Some(json!(9)))]
        );
        // A duplicate whose value is itself a container replaces the whole
        // group, so the inner fan-out yields only the last container's items.
        assert_eq!(
            read(
                "SELECT * FROM S3Object[*].m.*.*",
                "{\"m\": {\"x\": [1], \"y\": [8], \"x\": [2, 3]}}"
            ),
            vec![
                Record::Json(Some(json!(2))),
                Record::Json(Some(json!(3))),
                Record::Json(Some(json!(8))),
            ]
        );
    }

    #[test]
    fn document_root_shape_decided_before_first_read() {
        // Probe fact 3: `peek` only skips leading whitespace and never consumes
        // the value's first byte, so the value start is `pos()` right after it
        // (no compensation), and the shape decision never crosses the root.
        for (input, start) in [("{\"a\": 1}", 0u64), ("   [1, 2, 3]", 3)] {
            let mut slot = Some(Reader::new_custom(
                Cursor::new(input.as_bytes().to_vec()),
                settings(),
            ));
            {
                assert_eq!(position(as_reader(&slot).unwrap()).unwrap(), 0, "{input}");
                source(&mut slot).unwrap().peek().unwrap();
                assert_eq!(
                    position(as_reader(&slot).unwrap()).unwrap(),
                    start,
                    "{input}: peek moved data_pos"
                );
            }
            let mut budget = RootBudget::with_default_cap();
            let mut stream = ReaderTail::new();
            let mut state = RootState::Undecided;
            decide_shape(&mut slot, &mut budget, &mut stream, &mut state).unwrap();
            assert_eq!(stream.last_record_start, start, "{input}");
            let pos = position(slot.as_ref().unwrap()).unwrap();
            assert!(
                pos <= start + 1,
                "{input}: the shape decision crossed the root"
            );
        }
    }

    #[test]
    fn matcher_name_step_duplicate_reports_ambiguous_at_object_close() {
        // The handler-level counterpart of the matcher's scripted
        // `final_name_holds_value_until_object_close`: the final Name step holds
        // its value until the object closes and the ambiguity verdict is final,
        // so nothing is ever delivered for the root (semantics 6, deviation ②).
        let plan = parse("SELECT * FROM S3Object[*].id").unwrap();
        let mut r = Document::new(Cursor::new(b"{\"id\": 1, \"ID\": 2}".to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Ambiguous(m)) => assert_eq!(m, "id"),
            other => panic!("expected ambiguous, got {other:?}"),
        }
        assert_eq!(r.next().unwrap(), None, "the ambiguity error is terminal");
    }

    #[test]
    fn hostile_inputs_never_panic() {
        // Probe fact 4: a `peek` after the top-level value panics inside
        // struson, so neither handler may ever do it. Every malformed input must
        // come back `Err`; a panic anywhere aborts this test.
        let mut deep = Vec::from(&b"["[..]);
        deep.resize(201, b'[');
        let invalid_utf8 = vec![b'{', b'"', b'a', b'"', b':', b'"', 0xFF, 0xFE, b'"', b'}'];
        let hostile: Vec<Vec<u8>> = vec![
            // Trailing data after a top-level value (where the old probe peeked).
            b"{} {}".to_vec(),
            b"5 {}".to_vec(),
            b"{} x".to_vec(),
            b"5x".to_vec(),
            b"123#".to_vec(),
            // Truncated values.
            b"[1".to_vec(),
            b"[\"abc".to_vec(),
            b"[{\"a\":".to_vec(),
            b"{\"a\":".to_vec(),
            // A lone surrogate on the skipped path.
            br#"{"b":"\uD800","a":1}"#.to_vec(),
            // Invalid UTF-8.
            invalid_utf8,
            // Nesting past the reader's 128-level limit.
            deep,
        ];
        for input in hostile {
            let plan = parse("SELECT * FROM S3Object[*].a").unwrap();
            let mut r = Document::new(Cursor::new(input.clone()), &plan.from);
            loop {
                match r.next() {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("{input:?}: ended cleanly instead of erroring"),
                    Err(_) => break,
                }
            }
        }
    }
}
