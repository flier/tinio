//! JSON record readers: LINES / DOCUMENT framings over the struson kernel.
//!
//! Kernel: one `struson::reader::JsonStreamReader` per root. Each handler
//! drives it through the single `ValueSource` seam, and each pull reads one
//! root value; the `Matcher` is the pausable stepper over the FROM path, and
//! it stops as soon as one record (MISSING included) is queued, so a record
//! reader charges the 1 MB cap at a batch boundary, with a charged `Read`
//! backstop (`json::feed`) aborting a grossly over-cap value mid-read. An
//! unmatched subtree goes through `skip_value`: validation without
//! materialization. Numbers arrive as the verbatim token from
//! `next_number_as_str`, the only position source is
//! `current_position(false).data_pos` (`peek()` never advances it), and the
//! trailing probe — a `peek()` after the top-level value panics — is
//! `consume_trailing_whitespace`, which consumes the reader. LINES pulls
//! exactly one whole line, DOCUMENT delivers exactly one record per delivery.
//!
//! Memory is bounded by one *pull*, never by the input: a LINES line (≤ 1 MB
//! span) or one DOCUMENT array element. A single-value DOCUMENT root
//! accumulates its whole match set in that one pull, where the exact span check
//! is the cap authority and a charged `Read` backstop (`feed::BudgetedRead`)
//! aborts a grossly over-cap value mid-read, so memory is O(cap) with a small
//! constant factor — the charge is in input and rendered-row bytes, so per-row
//! container overhead sits outside it — not the whole document (deviation ⑦).
//!
//! Throughput baseline: the serde_json streaming kernel this module replaces
//! measured json_document_scan at 47.080-52.227 ms / 16.393-18.185 MiB/s
//! on 2026-09-13, Intel Core i7-14700KF. The struson kernel must not be
//! slower than this (acceptance threshold, struson design spec, acceptance
//! criteria).
//!
//! Throughput after the kernel swap: the same `json_document_scan` measured
//! 42.209-44.780 ms / 19.119-20.284 MiB/s on 2026-09-13, Intel Core
//! i7-14700KF (rustc 1.98.1). The whole interval sits below the baseline's
//! 47.080-52.227 ms / 16.393-18.185 MiB/s, so the struson kernel is not
//! slower. criterion's `change:` block reads `time: [-25.073% -9.6399%
//! +10.886%] (p = 0.33 > 0.05)` and `thrpt: [-9.8176% +10.668% +33.462%]`
//! ("No change in performance detected"): at that p the point estimate is not
//! trustworthy, and the verdict rests on the disjoint absolute intervals, not
//! on the change block. `json_document_scan` in `benches/select_scan.rs` is the
//! baseline, and the gap against the whole-record `from_slice` parse is the
//! price of bounded memory. Buffer layers: struson carries its own fixed 1 KiB
//! read buffer, so DOCUMENT hands it the source directly; LINES wraps the
//! source in `LineFraming`, whose own `BufReader` exists because the framing
//! serves one byte per call and reports EOF at the newline.
//!
//! Recorded deviations from the serde_json kernel, in the numbering of the
//! binding spec `docs/superpowers/specs/2026-09-12-json-streaming-struson-design.md`
//! ("semantic preservation and deviations"):
//! ① DOCUMENT total size is unbounded — the 1 MB cap binds each root instead:
//!   an array document of elements each ≤ 1 MB streams at any size, while a
//!   single-object or scalar root over 1 MB still errors.
//! ② The timing of a non-final Name step's duplicate key is relaxed: the
//!   stream errors as soon as a duplicate key is seen, so rows already
//!   produced inside that object are not retracted. A final Name step stays
//!   strict, holding the value until the object closes.
//! ③ LINES cap versus parse-error priority: parsing is struson's and happens
//!   as bytes are served, so an invalid line may parse-error before the line
//!   width cap fires.
//! ④ DOCUMENT error timing is streamed: a document-level format error is
//!   exposed element by element instead of all-or-nothing, so records before
//!   the failing element have been produced. An unterminated document's
//!   `rows_before` follows the probes.
//! ⑤ DOCUMENT records report real element-level offsets (uncompressed
//!   absolute; a single-value root reports its value start, and an empty root
//!   array's zero-match MISSING reports 0), and ScanRange filters them.
//!   `last_record_start` is no longer always 0 under DOCUMENT, and
//!   `BytesProcessed` advances with the scan.
//! ⑥ The cap is charged over a root value's own span: element separators and
//!   whitespace belong to no span, and the `]` is not part of the last
//!   element, so an element, a single-value root, or a line of exactly 1 MB
//!   passes at the line.
//! ⑦ Cap timing and mid-root interruption: an array root is measured per
//!   element span at that element's end, while a single-value root is sampled
//!   and compared at batch boundaries. The exact span check is the cap
//!   authority; a charged `Read` backstop (`feed::BudgetedRead`, abort
//!   threshold `MAX_RECORD + 64 KiB`) additionally stops a grossly over-cap
//!   matched value mid-read, so memory is O(cap) with a small constant factor
//!   rather than the whole document: the bound is on the charged bytes (input
//!   bytes served and the rendered length of the queued rows), so the per-row
//!   `Value`/`Map`/`Vec` overhead and the member-key index sit outside the
//!   measurement. Unmatched subtrees still buffer nothing and struson's read
//!   buffer stays a fixed 1 KiB.
//! ⑧ Number tokens are no longer normalized: serde_json's arbitrary-precision
//!   mode rewrote a signless exponent (`1e309` → `1e+309`), while struson's
//!   `next_number_as_str()` is the input token verbatim, so `RawNumber`
//!   replays the input from now on.
//! ⑨ Error types and message sources changed: a JSON input error moved from
//!   `Error::Format(String)` to the structured `Error::Json(json::Error)`,
//!   and the wording comes from struson's `ReaderError` variants instead of
//!   serde_json. The blast radius is 16 sites; the cap's Display is verbatim
//!   unchanged and the trailing-data messages are synthesized by the handlers
//!   with the old wording. One distribution moved: the retired trailing probe
//!   deserialized a second value, so it could tell `{} {}` (our message) from
//!   `{} x` (serde's error). `consume_trailing_whitespace` reports ANY
//!   trailing byte as `TrailingData` and cannot tell those shapes apart, so
//!   `{} x` is now reported by the SAME synthesized trailing message as
//!   `{} {}` / `5 {}` — which supersedes the spec's narrative for that one
//!   row of `document_single_root_trailing_garbage_errors`; `5x` / `123#`
//!   still fail during the value read.
//! ⑩ Test deletion: three tests pinned serde_json's reserved-key mechanism
//!   (`$serde_json::private::Number`, the `NUMBER_TOKEN` constant with
//!   them), which does not exist under the new kernel; duplicate-key collapse
//!   is pinned again by a test that does not depend on engine internals.
//! ⑪ The depth limit moved implementation point: serde_json's recursion
//!   threshold 128 became `ReaderSettings::max_nesting_depth = 128` (struson's
//!   default), with zero behavioural change.
//! ⑫ The `pending_high` read points changed: the high-water pin moved into
//!   `Matcher`, the handlers expose a `#[cfg(test)] pending_high()`, and the
//!   two `document.rs` assertions were rewritten with unchanged semantics.
//! ⑬ A new JSON-side error module: `crate::Error` gained the `Json` variant
//!   (it keeps its derived `PartialEq`), and `json/error.rs` was added with
//!   `json::Error`, which derives `PartialEq` too: its `Io` payload is the
//!   crate's own `IoError` newtype — never a bare `io::Error` — so the
//!   hand-written impl that payload used to force is gone.

pub(crate) mod document;
pub(crate) mod error;
pub(crate) mod feed;
pub(crate) mod line;
pub(crate) mod lines;
pub(crate) mod matcher;
pub(crate) mod source;
#[cfg(test)]
pub(crate) mod struson_contract;
pub(crate) mod tail;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod value;

use std::io::Read;

pub(crate) use value::{json_lookup, to_value};

use crate::{error::Error, record::RecordReader, row::Record, sql::FromClause};

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
    // Both variants are boxed: each carries a reader plus a matcher frame
    // stack, and the two differ in size enough to trip clippy's variant lint.
    Lines(Box<lines::Lines<R>>),
    Document(Box<document::Document<R>>),
}

impl<R: Read> Reader<R> {
    #[tracing::instrument(target = "tinio::s3", level = "debug", name = "json.reader", skip_all)]
    pub fn new(reader: R, params: Params, from: &FromClause) -> Self {
        let inner = match params.ty {
            Type::Lines => Handler::Lines(Box::new(lines::Lines::new(reader, from))),
            Type::Document => Handler::Document(Box::new(document::Document::new(reader, from))),
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

/// The synthetic-element shape this module's tests and the bench share.
///
/// **Declared at file scope rather than inside `mod tests`, and that is the whole point.**
/// `benches/select_scan.rs` `#[path]`-includes this very file, so the two cannot drift onto
/// different element layouts. A `#[path]` resolves against the directory of the file declaring
/// it **plus the name of any enclosing inline module** — inside `mod tests` that base is
/// `src/json/tests/`, which does not exist, and while Windows folds the `..` away lexically,
/// Linux resolves a path component by component and refuses to open it: the lib-test target did
/// not compile there at all. At file scope the base is `src/json/`, which is real, and so is
/// every component below it on both platforms.
#[cfg(test)]
#[path = "../../tests/support/json_fixture.rs"]
mod json_fixture;

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{Cursor, Read, Write},
    };

    use flate2::write::GzEncoder;
    use serde_json::json;

    use super::{
        Params, Reader, Type, json_fixture,
        testing::{raw, run, run_err, s},
    };
    use crate::{
        Error,
        engine::OutRow,
        record::{CAP_MESSAGE, Compression, RecordReader, decompressed},
        row::{Field, Record, Value},
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
        // through. The retired serde_json arbitrary-precision scanner
        // rewrote a signless exponent as `e+` (`1e309` → `1e+309`); that
        // injection left with the kernel, and struson's
        // `next_number_as_str()` replays the input token verbatim
        // (deviation ⑧).
        let rows = run("SELECT x FROM S3Object s", Type::Lines, "{\"x\": 1e309}\n");
        assert_eq!(rows[0].keys, vec!["x"]);
        assert_eq!(rows[0].vals, vec![raw("1e309")]);
        let rows = run("SELECT * FROM S3Object s", Type::Lines, "{\"x\": 1e309}\n");
        assert_eq!(rows[0].vals, vec![raw("1e309")]);
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

    // ------------------------------------------------------------------
    // Streaming a large DOCUMENT end to end (spec 2026-09-12): row count,
    // completion, strictly advancing element-start offsets. The fixture is
    // the gzip-compressed synthetic document in git LFS, 11 577 783 bytes
    // decompressed (~11 MB); the read path is the production one
    // (`decompressed(Gzip)`).
    // ------------------------------------------------------------------

    /// Fixture size, shared with `generate_large_fixture` below.
    const ELEMENTS: u64 = 100_000;

    fn fixture_reader() -> Reader<Box<dyn Read + Send>> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/large/document.json.gz"
        );
        let file = File::open(path).expect("LFS fixture missing — run `git lfs pull`");
        let input = decompressed(Some(Compression::Gzip), Box::new(file));
        let plan = parse("SELECT * FROM S3Object").unwrap();
        Reader::new(input, Params { ty: Type::Document }, &plan.from)
    }

    #[test]
    fn large_document_streams_to_completion() {
        let mut r = fixture_reader();
        let mut count = 0u64;
        let mut last = 0u64;
        while let Some(rec) = r.next().unwrap() {
            assert!(matches!(rec, Record::Json(Some(_))));
            let start = r.last_record_start();
            // Strict, not `>=`: `last` starts at 0, so this also requires the very
            // first record to start past the `[`, and a reader returning a constant
            // offset (which `start >= last` accepted) now fails. This is the only
            // CI-level check of spec deviation ⑤ (element-level real offsets).
            assert!(
                start > last,
                "record starts must strictly advance (spec deviation ⑤): record \
                 {count} starts at {start}, previous at {last}"
            );
            last = start;
            count += 1;
        }
        assert_eq!(count, ELEMENTS);
    }

    #[test]
    fn over_cap_element_errors_after_prior_records() {
        // The spec's cap-behaviour pin on the CI path (inline data, not the LFS
        // fixture). The public accessor classifies the failure mode; the
        // rendering is built from `record::CAP_MESSAGE` itself — the constant
        // the reader raises — so the pin cannot drift from it.
        let big = "a".repeat(1024 * 1024);
        let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Reader::new(
            Box::new(Cursor::new(doc.into_bytes())) as Box<dyn Read + Send>,
            Params { ty: Type::Document },
            &plan.from,
        );
        assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
        match r.next() {
            // A separate crate can name and match the variant now: the public
            // `JsonInputError` payload carries the classifiers, so telling the cap
            // from another JSON failure needs no Display string.
            Err(Error::Json(json)) => {
                assert!(json.is_cap(), "{json:?}");
                let e = Error::Json(json);
                assert!(e.is_cap(), "{e:?}");
                assert!(e.is_json_input(), "{e:?}");
                // The crate-level Display — byte-identical to the old
                // `Error::Format(CAP_MESSAGE)` rendering — is what the S3 Select
                // layer surfaces to the client.
                assert_eq!(
                    e.to_string(),
                    format!("S3 select: input error: {CAP_MESSAGE}")
                );
            }
            other => panic!("expected the reader cap error, got {other:?}"),
        }
    }

    /// Regenerate the fixture: `cargo test -p tinio-select --lib -- --ignored`.
    /// Same provenance shape as `src/parquet.rs`'s ignored generator.
    #[test]
    #[ignore = "regenerates the LFS fixture"]
    fn generate_large_fixture() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/large/document.json.gz"
        );
        let file = File::create(path).expect("create fixture");
        let mut gz = GzEncoder::new(file, flate2::Compression::default());
        gz.write_all(b"[\n").unwrap();
        for i in 0..ELEMENTS {
            writeln!(
                gz,
                "{}{}",
                json_fixture::element_pretty(i),
                if i + 1 < ELEMENTS { "," } else { "" }
            )
            .unwrap();
        }
        gz.write_all(b"]\n").unwrap();
        gz.finish().unwrap();
    }
}
