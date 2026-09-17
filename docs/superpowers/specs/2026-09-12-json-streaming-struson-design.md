
# JSON Streaming Reader Design (struson core)

- Date: 2026-09-12
- Status: Reviewed (brainstorming + five rounds of grilling: Q1–Q24 all adjudicated) + revised per external review + facts measured by probes
- Relationship: This document **supersedes** `docs/superpowers/plans/2026-09-12-json-streaming-actson.md` (the actson-core version); the latter is retained as a selection record with a superseded annotation added.
- Scope: refactor of `crates/tinio-select/src/json/` + a new variant in `crates/tinio-select/src/error.rs`; one added dependency line each in the root and crate `Cargo.toml`
- Language: English. Originally written in Chinese under an explicit user exemption from the CLAUDE.md English-only rule; translated to English on 2026-09-15 at the user's request, which ends that exemption.

## Background and Problem

S3 Select's JSON input has two framings (`InputSerialization.JSON.Type`): LINES (one JSON value per line) and DOCUMENT (the whole stream is a single JSON document; when the root is an array, each element is a record). The implementation currently in the working tree is a serde_json streaming core + a hand-written validating `Skip`, with three most expensive costs:

- `IoRead` is driven byte by byte, holding down syscall cost with two layers of `BufReader`;
- Non-matching subtrees must go through the hand-written `Skip`, because `IgnoredAny` does not validate surrogate pairs and UTF-8;
- The match set of a single-value root accumulates per document within one pull (old deviation ③), so memory is tied to document size.

Goal: switch to **struson 0.7.2's `JsonStreamReader`** as the sole parsing core — consuming from `Read` on demand, native `skip_value()` (validates but does not materialize, does not buffer), `next_number_as_str()` handing back verbatim number tokens — while keeping the matcher hand-written and converging the memory upper bound to "one root / the current record". The `json::Reader::new` signature, the `RecordReader` contract, the `Record::Json(Option<Value>)` model, and the engine-side `to_value`/`json_lookup` all stay unchanged.

Non-goals: streaming the CSV / Parquet readers; making the `RecordReader` pull contract async (the crate keeps synchronous pulling, `unsafe_code = "forbid"` unchanged); any change on the output side (event packing); the engine select side's per-column cloning; replacing the hand-written matcher with struson's `json_path` / `seek_to` / `simple` APIs (see the next section).

## Selection record: why not rill-json, and not actson

| Candidate | Fatal constraint |
| --- | --- |
| rill-json 0.5 | `parse_streaming(&'a str)` requires **holding the whole** input, and the entry point hardcodes a 10 MB cap on the whole string; the tokenizer holds `&[u8]` + `cursor`, with no "come back later" state; `JsonNumber` has only I64/U64/F64 (losing the original token), and `JsonValue::Object` is a `BTreeMap` (losing key order). Using it would require vendoring and modifying the source. |
| actson 2.1 | Usable, and its design has been validated (that is the plan this document supersedes). Gaps relative to struson: when skipping a subtree it still pushes every token byte into `current_buffer` and then discards it; numbers have no native verbatim token API. |
| **struson 0.7.2** | Adopted. `JsonStreamReader<R: Read>` consumes on demand + a fixed 1 KiB internal buffer; `skip_value()` validates without materializing or buffering; `next_number_as_str()` gives a verbatim token; `current_position(false).data_pos` is the byte position in consumption terms. |

## Verified external facts (measured by probes + locally vendored source)

**Evidence rule: only locally vendored source counts.** docs.rs's `latest` and GitHub `main` are both 0.8-dev, with field names and behavior differing from 0.7.2 (for example, main has `allow_empty_document` while 0.7.2 does not; main's `ReaderSettings` is marked `#[non_exhaustive]` while 0.7.2's `ReaderSettings` is not — this sentence applies only to `ReaderSettings`: 0.7.2's `ReaderError` **is** `#[non_exhaustive]` (locally vendored `reader/mod.rs:513`), see §Error handling). This design once mis-changed a field name based on main; that is corrected here.

1. `ReaderSettings` (`stream_reader.rs:234-368`) has **six fields**, with defaults: `allow_comments=false`, `allow_trailing_comma=false`, `allow_multiple_top_level=false`, `track_path=true`, `max_nesting_depth=Some(128)`, `restrict_number_values=true`. There is **no** `allow_empty_document`. Probe 8 measured agreement.
2. `current_position(&self, include_path: bool)` returns `JsonReaderPosition { path, line_pos, data_pos: Some(byte_pos) }` (`:2386-2400`). `byte_pos` is the number of **consumed** bytes; the documentation states verbatim that it "might not be equal to the amount of data consumed from the underlying data source in case the JSON reader buffers data internally".
3. **`peek()` does not change `data_pos`**: `skip_peeked_byte()` only advances `buf_pos` (`:522-525`), and `byte_pos` is increased by `consume_peeked()` once, by the literal's length, only when the value is consumed (`:901-916`). Probe measurement: across all seven types object/array/string/number/true/false/null, the `data_pos` difference before and after `peek()` is **always 0**.

   **Exact semantics (important, to avoid this reading as contradictory to the pure-whitespace case in fact 10)**: `peek()` itself calls `skip_whitespace` first, so `data_pos` may already have advanced because of leading whitespace by the time `peek()` is entered (fact 10's `"   \n  "` → `data_pos` 0→6 is exactly this). What does not change is **within a single `peek()` call**: it consumes no bytes for the value being probed. Hence the rule — **the value's first byte = the `pos()` after that value's `peek()` returns**, and leading whitespace and separators are all charged to the previous call. Probe fact 7 anchors this with the two elements' starts at 1 and 11 (11 rather than 12, showing that the whitespace beyond the comma was also eaten by `has_next`, and `peek` did not move anything further).
4. Calling `peek()`/`has_next()` after the top-level value has been consumed **panics** (`:743-747`, "Incorrect reader usage"). Trailing data **can only** be detected by `consume_trailing_whitespace(mut self)` (`:2360-2384`): it consumes `self`, requires the top-level value to be fully consumed (otherwise it panics), and returns only `Ok(())` or `SyntaxError(TrailingData)`. All five cases of probe 3 measured PANICKED + `TrailingData`.
5. `skip_value()` is an iterative `depth` loop (`:2223-2358`); strings go through `skip_all_string_bytes()` and numbers through `SkippingNumberBytesCollector`, and **neither keeps bytes**. The validation strength has a source basis: `skip_all_string_bytes` and `read_string` share the same `read_all_string_bytes → read_string_bytes` path (`:1125-1148`), which is where UTF-8 validity and unicode escape/surrogate-pair decoding live. Probe 5 measured: an unpaired surrogate reports `SyntaxError/UnpairedSurrogatePairEscapeSequence` on both the **skip** and the **match** paths.
6. The error for invalid UTF-8 is `IoError { error, location }`, where `error.kind() == InvalidData` and the message is `invalid UTF-8 data` (`:1009`). Probe 6 measured three contrasts: invalid UTF-8 → `InvalidData`; a real read failure (`ConnectionReset`) → `ConnectionReset`.
7. Element/value spans (probe 2): for `[{"a": 1}, {"b": 2}]` (20 bytes) the two elements have spans of **8** each, with starts at 1 and 11 respectively; the end is taken before `end_array()`, and **does not include the `]`**; the top-level `[`, the commas between elements, and whitespace are all excluded from every span (an element's start is taken after `has_next()`, and `has_next` has already skipped the commas and whitespace).
8. **Output shape** for an unterminated array (probe A, 12 cases): value complete but structure not closed (`[1`, `[1,`, `[1, 2`, `["a"`, `[true`, `[null`, `[[]`, `[{}`) → the completed values are **produced first**, then `has_next()` reports `SyntaxError/IncompleteDocument`; the value **itself truncated** (`["abc`, `[{"a":`) → that value is not produced, and `IncompleteDocument` is reported while reading the value; separator errors `[1 2]` → `MissingComma`, `[1,]` → `TrailingCommaNotEnabled`. **Matches the `rows_before` of the existing `document_unterminated_input_is_a_format_error` case by case.**
9. Three shapes of trailing data (probe C): `{} x`, `5 {}`, `[1] x` → the value reads successfully, and `consume_trailing_whitespace` reports `TrailingData`; `truey`, `nullx` → `SyntaxError/TrailingDataAfterLiteral` is reported already in the **value-reading stage**, and the trailing check is never reached. **Matches the true/false distribution of the existing `document_single_root_trailing_garbage_errors` case by case.**
10. Empty input and whitespace-only input: `peek()` reports `SyntaxError/IncompleteDocument` for both; whitespace-only also consumes the whitespace bytes (probe 7: `data_pos` 0 → 6, because `peek()` calls `skip_whitespace` itself first, see the exact semantics in fact 3).
11. `next_number_as_str()` returns a verbatim token (probe: `12.0e5` as-is); with `restrict_number_values=true` it rejects numbers whose exponent |e|>99 or whose string length >100 (`UnsupportedNumberValue`), so it **must be turned off**. `max_nesting_depth = 128` is the default and is exactly the existing serde_json `from_slice` threshold.
12. struson 0.7.2 is the current latest version, MIT OR Apache-2.0, MSRV 1.85 (compatible with the workspace's edition 2024); **the dependency has been pulled successfully from the local mirror**.

## Overall architecture

The `json/` directory (`json.rs` has already been split; that is kept):

- `mod.rs` — module documentation, `Type`, `Params`, the dispatch entry `Reader` (`enum Handler { Lines, Document }`), the `pub(crate) use value::{json_lookup, to_value};` re-export, engine composition tests. The module documentation's deviation list is synced to ①–⑬.
- `value.rs` — `to_value`, `json_lookup` as-is (no behavior change).
- `error.rs` (json side, new) — `json::Error`, the payload of `crate::Error::Json`.
- `line.rs` (`LineFraming` moved in from `framing.rs`, implementation kept) — LINES' line-width cap and line-start accounting.
- `matcher.rs` — `Step`/`CompiledPath` + the `ValueSource` trait + `Matcher` (a pausable explicit-stack stepper) + `verbatim_number`.
- `feed.rs` (new) — per-root byte accounting: `pos()` sampling, span determination, cap determination.
- `lines.rs` / `document.rs` — the two handlers, each composing the parts above.
- `framing.rs` — **deleted** (`BudgetedRead` retired, `DocFraming` retired as a whole, `LineFraming` moved to `line.rs`).
- `core.rs` — **renamed by behavior and split by behavior, then composed** (grilling Q21): the "a bit of everything" name and structure of `Core` is no longer retained; it is split into single-responsibility parts that the handlers then compose.
- `crates/tinio-select/src/json/struson_contract.rs` (new) — contract tests for struson 0.7.2: driving `JsonStreamReader` directly (not through this crate's wrapper), pinning down item by item the upstream behaviors this design depends on (§Verified external facts 1–6, 9, 10), so that an upstream upgrade fails here first.

**Single position source: `data_pos`**:

| Use | How it is obtained |
| --- | --- |
| Absolute offset of a value's first byte | `current_position(false).data_pos` (after `peek()`, **no compensation term** — fact 3) |
| The 1 MB cap per root | The difference of the same `data_pos` sequence between the root's start and end (fact 7 gives the sampling points) |

LINES swaps in a new `JsonStreamReader` for each line, whose `data_pos` starts at 0, so a line's record start is taken from `LineFraming::start_line()`'s `LineStart::Line(start)` payload (uncompressed absolute bytes). **No second byte-counting scheme is introduced**. `events.rs`'s `bytes_processed` is derived from the difference between adjacent `last_record_start` values (`Tally::close`), so `last_record_start` must keep its uncompressed absolute semantics.

The interface stays unchanged: the `json::Reader::new(reader, params, from)` signature is unchanged, and `events.rs`'s `build_reader`, `record.rs`'s ScanRange tests, and `tinio-server`'s `backend/select.rs` parameter mapping are all zero-change.

## Parsing core

The sole parsers are `struson::reader::JsonStreamReader<R>` (DOCUMENT) and `JsonStreamReader<LineFraming<R>>` (LINES). **No outer `BufReader` is wrapped around them**: 0.7.2 carries its own fixed 1 KiB internal read buffer (`READER_BUF_SIZE = 1024`), and the documentation states verbatim that wrapping `std::io::BufReader` is usually unnecessary. **No separate buffer for matched values**: a value is assembled structurally by `matcher.rs`'s `read_value` directly from the reader, and apart from struson's fixed 1 KiB read buffer and `feed.rs`'s metered `Read` backstop there is no third byte buffer; the assembly size of a single matched value is bounded by the 1 MB cap of §Byte supply and accounting (exact span) plus the backstop margin.

- All six `ReaderSettings` fields are written out explicitly: four keep their defaults (`allow_comments: false`, `allow_trailing_comma: false`, `allow_multiple_top_level: false`, `max_nesting_depth: Some(128)`), with **two explicit deviations**: `track_path: false` (default true; saves a copy of the JSON path we never read, and does not conflict with "the hand-written Matcher is the sole matching implementation") and `restrict_number_values: false` (default true; must be off, otherwise `1e309` / 30-digit integers are rejected, see fact 11).
- Value reading always dispatches on `peek()`, and numbers always use `next_number_as_str()`; `next_number::<T>()` and `next_number_as_string()` are never used.
- **How verbatim numbers are assembled (2026-09-13 revision)**: `serde_json::from_str::<Value>(token)` can no longer be used — the locally vendored `serde_json 1.0.151` executes `_ => buf.push('+')` for an unsigned exponent at `de.rs:1027-1043` (`scan_exponent`), so `from_str("1e309")` yields `Number("1e+309")`, taking the same normalization path as the old core, and deviation ⑧ would fail. The replacement is `Value::Number(serde_json::Number::from_string_unchecked(token.to_owned()))` (`number.rs:341-347`, a **safe** public constructor under `arbitrary_precision`, annotated "Not public API"). Validity is guaranteed by struson (fact 11), so no further parse validation is needed; a round-trip pin guards against upstream change (`from_string_unchecked("1e309").to_string() == "1e309"`). If that constructor is removed in the future, the fallback is to build our own `Number` equivalent whose `Display` is the token, or to accept `e+` normalization and invert deviation ⑧ into "keep the old behavior".
- Non-matching subtrees always use `skip_value()` (fact 5).
- `current_position(false)` is the sole position source; `include_path = true` and `track_path` are never used.
- **Never `peek()` again after the top-level value has been consumed** (fact 4: it panics); the trailing check always goes through `consume_trailing_whitespace`.

## PathSeg compilation state machine

The compilation and runtime semantics **carry over item by item** from the existing implementation; the 47 facade tests pass as-is, and the matcher's existing semantic cases keep their assertions and get their driver rewritten.

- `Matcher` holds a `&mut dyn ValueSource` and pulls values actively (`peek()` to determine the type, `next_*` to consume, `skip_value()` to skip non-matching subtrees); the actson version's event enum `VEvent` is deleted.
- **`Matcher` must be a pausable stepper** (grilling Q18=b): the recursive descent becomes single-step advancement over an explicit stack; once `pending` accumulates to a batch limit (or the root completes) control returns, and the next call resumes from the breakpoint. This is the largest single refactor in this change; the frame-stack structure is unchanged, but the assumption that "one call reads a whole root" must be eliminated.
- `ValueSource` is the single minimal seam between the matcher and the byte source: `peek` (returning the local `enum ValueKind { Array, Object, String, Number, Bool, Null }`)/`has_next`/`name`/`text`/`number`/`boolean`/`null`/`begin_object`/`end_object`/`begin_array`/`end_array`/`skip_value`. The real implementation wraps `JsonStreamReader`; tests use a scripted stand-in. **Error mapping happens at this layer**: the methods always return `crate::Error` (internally folding `ReaderError` into `crate::Error::Json(json::Error::…)`), so the matcher and the two handlers see only the crate's own error type (docs/style.md).
- The matcher tests currently in the working tree are **serde_json-driven** (`Deserializer::from_slice` + `StepSeed`) and must be rewritten to be driven by a scripted `ValueSource`; what is kept is the **case corpus and the assertions**, not the implementation.

Non-negotiable semantics:

1. The step sequence drops the leading `Wild` (the implicit `S3Object[*]`).
2. A container closes with a hit count of 0 for that scope → enqueue **one** `None` (MISSING).
3. A step applied to a scalar → zero hits.
4. `Wild` fans out in document DFS order; for `Wild` over an object, each member's rows are first accumulated into a group, which is appended in member order when the object closes.
5. `Index(i)` recurses only into the i-th element; out of range → zero hits.
6. A hit on the final-step `Name` is staged, and is enqueued only after confirming at object close that there is no second case-folding hit; a duplicate → `Error::Ambiguous(name)`.
7. A duplicate on a non-final-step `Name` → immediate `Error::Ambiguous(name)` (rows already produced are not retracted).
8. A non-matching subtree is only depth-counted; not a single value is materialized.
9. When `peek()` meets a type that does not match the expectation, it is treated as "zero hits" and not escalated to an error; `has_next()`'s boundary semantics (false before an object's `}` and before an array's `]`) is passed through verbatim by `ValueSource`.
10. The case-folding comparison reuses the existing `key_matches` (sharing one implementation with the engine-side `json_lookup`, to prevent drift).

## Byte supply and accounting

- **The exact span has only one source**: `current_position(false).data_pos`. Define `pos()` = read that value; **a value's first byte = the `pos()` after `peek()`, with no compensation term** (fact 3). The **authoritative determination** of the 1 MB cap is the difference of this sequence between the root's start and end (sampling points in the table below).
- **Beyond the exact span there is additionally a metered `Read` backstop** (`feed.rs`'s `BudgetedRead`, restored in final review C1 on 2026-09-14): it meters every byte struson reads, and once the cumulative total exceeds `MAX_RECORD + 64 KiB` it aborts the read and returns the verbatim-unchanged `CAP_MESSAGE`. Why it is a backstop rather than the authority: struson read-aheads with an approximately 1 KiB read buffer and looks one byte further on delimiter bytes, so byte metering cannot express the exact-span semantics of "exactly 1 MB passes, one byte more fails"; hence the 64 KiB margin — every existing boundary-case verdict is unchanged, while a matched value far beyond the limit is cut off at around 1 MB of reading instead of being fully materialized and then rejected. The threshold constant `CAP_ALLOWANCE = 64 * 1024` (defined in `record.rs`, shared with the CSV reader's read-ahead wrapper) and the rationale are written there.
- `LineFraming<R>` (`line.rs`) keeps its existing implementation, with the line start carried by `start_line()`'s `LineStart::Line(start)` payload — **no `line_start()` accessor exists today, and none is added**; it continues to carry LINES' **line-width cap** (content + terminator ≤ 1 MB, reported while reading) and line-start accounting, and **it is LINES' authoritative upper bound and only cap route**: the framing errors mid-serve at `span > MAX_RECORD`, before struson can be served an over-cap line, so the LINES handler holds no `RootBudget` (its exact sampling could never fire there) and no charged `Read` backstop (the charge could not reach `cap + CAP_ALLOWANCE` either).

**The cap's three-part structure and sampling points** (grilling Q3 + probe facts 7, 8):

| Root shape | Start | End |
| --- | --- | --- |
| DOCUMENT array root · element i | The `pos()` after that `has_next()` returns true and after the value's `peek()` (leading whitespace and the separator already skipped) | The `pos()` after that value has been read and before the next round's `has_next()`/value `peek()` (so the span excludes the commas and whitespace between elements) |
| DOCUMENT array root · last element | Same as above | The `pos()` **after that value has been read**, the same sampling point as element i — that is, the `pos()` before `has_next()`; `has_next()` first skips the whitespace before the `]`, and that whitespace **is not merged into** the last element's span, nor does the span naturally include the `]` |
| DOCUMENT single-value root | The `pos()` after the first `peek()` | The `pos()` after the whole document has been parsed |
| LINES · each line | No span sample: the handler holds no budget; the LINES record start still comes from `start_line()`'s `LineStart::Line(start)` payload, i.e. that line's first byte | No span sample: LINES' cap is `LineFraming`'s own line width, enforced mid-serve inside `Read::read` (`json::line`), so there is nothing for a `pos()` difference to decide |

Every span judged `end - start > MAX_RECORD` returns `Error::Json(json::Error::Cap)`; **accumulation is per root across batches** (Q18=b): a single-value root samples `pos()` at batch boundaries and compares it against the root's start, aborting and reporting cap as soon as it is over the limit, without having to wait for the root to be read to the end. An array root is judged by **each element's own span**, and elements are never summed across. The top-level `[`/`]`/commas and top-level whitespace are naturally counted when they fall within some root's span, and the old version's "container depth gating" is gone (old deviation ⑦ retired). The metered `Read` backstop accumulates independently on the same root (per element for a DOCUMENT array root, per line for a single-value root), and a hit returns the same `Error::Json(json::Error::Cap)`.

**The authoritative upper bound for LINES is `LineFraming`'s line width** (content + terminator ≤ 1 MB, reported while reading), and it is LINES' **only** cap route: the LINES handler holds no `RootBudget` and no `Read` backstop, because the framing errors mid-serve before struson can be served an over-cap line — the width cap is enforced by `LineFraming` alone, and struson surfaces that read failure as an IO error which `json::Error::from` maps to `Error::Json(json::Error::Cap)`, with the verbatim-unchanged `CAP_MESSAGE`.

Invariant: **one root ≤ 1 MB (exact-span determination)** — array mode means one element, single-value mode means the whole document, LINES means one line; **the resident-memory upper bound is O(cap) with a small constant factor** for a DOCUMENT root, not the whole document (the exact span is authoritative, and the metered `Read` backstop guarantees that a matched value far beyond the limit is cut off during reading; the charge is on input bytes served and on the rendered length of the queued rows, so per-row `Value`/`Map`/`Vec` overhead and the member-key index sit outside it — the bound is proven, not measured against RSS); LINES stays inside the line cap itself, which `LineFraming` cuts mid-serve, so there is no backstop margin to add there.

## Handlers

Both handlers hold an `Option<JsonStreamReader<…>>` — because **the trailing check consumes the reader** (fact 4).

- **LINES**: `LineFraming` splits lines → a new `JsonStreamReader` per line → `peek()` → a single value → produce that line's match set → tail check.
  - Empty line / whitespace-only line: `peek()` reports `IncompleteDocument` → report `Format("json input: …")` (the shape of the two existing tests is unchanged).
  - A second top-level value on a line: **no more `peek()`** (it would panic); `consume_trailing_whitespace` is used instead; when it reports `TrailingData`, the handler synthesizes its own message `json input: trailing characters on line` (the shape of the existing `lines_trailing_garbage_errors`).
- **DOCUMENT**: holds `enum RootState { Undecided, ArrayRoot { done: bool }, Single { done: bool }, Done }`. `JsonStreamReader::new` is lazy and reads no bytes, so the root-shape decision must happen before the first read (`peek()` decides, then the root value's start is recorded immediately per fact 3).
  - Array root: `begin_array()` → per-element `has_next()` loop → `end_array()` → tail check (`[1] x` reports `TrailingData` this way).
  - Single-value root: the whole root is read in one go → `consume_trailing_whitespace`; `{} {}` and `5 {}` thus report `TrailingData` and synthesize the message `json input: trailing characters after the JSON document`; `{} x`, `5x`, `123#` error already in the **value-reading stage** (fact 9), so they do not carry that message — matching the true/false distribution of the existing tests case by case. (**2026-09-13 measurement correction**: `{} x` in fact also goes through `consume_trailing_whitespace` and reports `TrailingData`, and thus takes the **same** synthesized message as `{} {}` and `5 {}` — struson's trailing API cannot distinguish the two; the previous sentence's old account of `{} x` is retired, deviation ⑨ records this point, and the test expectations are updated per the measurement.)
  - `next()` hands out one batch of records at a time, and the next call resumes from the breakpoint. An array root's `pending` is bounded per element and independent of document size; a single-value root differs: the cap and the trailing check must run first, so one pull reads the whole root before delivering, and that root's match set resides in `pending` with the cap as its upper bound (deviation ⑦).
- Empty input → zero records (`peek()` reports `IncompleteDocument` and `data_pos == 0`, which is fact 10's distinguishing point); `[]` → exactly one MISSING (start 0); whitespace-only → `Format`; unterminated → `Format`, with `rows_before` determined by the shapes of fact 8 (completed values are produced first).
- `last_record_start()`: DOCUMENT takes the `pos()` of the root value's start; LINES takes `start_line()`'s `LineStart::Line(start)` payload.
- **The `pending_high` high-water-mark pin moves inside `Matcher`** (its home after the behavior-based split): the handler exposes `#[cfg(test)] pending_high()` for the two existing assertions to read; the read sites at `document.rs:499/555` are rewritten accordingly.

## Error handling

**Top-level shape**: `crate::Error` gains one variant whose payload is a JSON-specific error enum, using thiserror so every error has its own value and the original error is preserved:

`@rust
#[error("S3 select: input error: {0}")]
Json(#[from] JsonInputError),
`@

`JsonInputError` is a public newtype in `crate::error` over the `pub(crate)` `json::Error` (private field). It exists because `json::Error`'s variants carry `struson` types (`ValueType`, `JsonReaderPosition`, `UnexpectedStructureKind`), which would otherwise make `struson` a de-facto public dependency: downstream can name and match `Error::Json` without any struson type entering the public API, and `#[allow(private_interfaces)]` is not needed. Its public surface is the classifiers (`is_cap()`) plus a `Display` forwarded verbatim, so the rendered crate-level message is byte-identical; `crate::Error::is_cap()` delegates to the payload's.

`json::Error` corresponds one-to-one with struson's variants, all `#[from]`-wrapping the original value (preserving `location`, `expected`/`actual`, `number`, etc.), and adds two variants of its own on the JSON side:

`@rust
#[derive(Debug, Error, PartialEq)]
pub(crate) enum Error {
    #[error("json input: {0}")]
    Syntax(#[from] JsonSyntaxError),
    #[error("json input: unexpected value type: expected {expected:?}, got {actual:?} at {location}")]
    UnexpectedType { expected: ValueType, actual: ValueType, location: JsonReaderPosition },
    #[error("json input: unexpected structure: {kind} at {location}")]
    Structure { kind: UnexpectedStructureKind, location: JsonReaderPosition },
    #[error("json input: nesting exceeds {max_nesting_depth} levels at {location}")]
    NestingDepth { max_nesting_depth: u32, location: JsonReaderPosition },
    #[error("json input: unsupported number value: {number} at {location}")]
    Number { number: String, location: JsonReaderPosition },
    // Invalid UTF-8 in the input: an input-format error, not an IO failure.
    // Recognized by IoError.error.kind() == InvalidData AND the message
    // equal to `invalid UTF-8 data` (verified fact 6); the message half is
    // what keeps the cap's own InvalidData/CAP_MESSAGE pair distinct.
    #[error("json input: invalid UTF-8 data at {location}")]
    InvalidUtf8 { location: JsonReaderPosition },
    // A real failure of the underlying reader. The payload is the crate's
    // `IoError` newtype (not a bare `std::io::Error`, which is not
    // `PartialEq`), so this enum derives `PartialEq`; `IoError` compares by
    // `kind()` and its Display is transparent, so both the comparison and the
    // rendered message are what the hand-written impl gave.
    #[error("S3 select: io error: {error}")]
    Io { error: IoError, location: JsonReaderPosition },
    // The 1 MB per-root record cap (deviations 6/7). Only the message lives
    // here: the `S3 select: input error: ` prefix is supplied by the
    // crate-level `Error::Json` variant, so repeating it would double it.
    #[error("{CAP_MESSAGE}")]
    Cap,
}
`@

- **`Cap`'s Display is verbatim** `S3 select: input error: input record exceeds 1 MB` (rendering identically to the old `Error::Format(CAP_MESSAGE)`), so the exact-string assertion in `json/lines.rs` and the contains assertion at `tinio-server`'s `backend/select.rs:1261` both need no change; the two variant-matching sites (`json/lines.rs`, `tests/stream_large.rs:81`) change to matching `Error::Json`.
- **Trailing data** does not use struson's message; instead a message of our own is synthesized: DOCUMENT uses `json input: trailing characters after the JSON document` (carrying over the verbatim message of the old implementation at `document.rs:93`, synthesized when `consume_trailing_whitespace` reports `TrailingData`), and LINES uses `json input: trailing characters on line` (the same message the old implementation reported at `lines.rs:69` during its serde tail probe; under the new core this detection is borne by `consume_trailing_whitespace` with the handler synthesizing the message — this is a change in the **behavior's implementation path**, with the external message verbatim unchanged, recorded in deviation ⑨).
- `PartialEq` is **derived**: `json::Error` derives it (`derive_more`, as `crate::Error` does) because its `Io` payload is the crate's `IoError` newtype, and both enums compare by "variant key + payload fields". `ReaderError` has no `PartialEq` (its payload types all do), and 0.7.2's `ReaderError` **is** `#[non_exhaustive]` (locally vendored `reader/mod.rs:513`), so the catch-all arm of `From<ReaderError>` **is required for the `match` to compile** (without it, E0004), and at the same time it guarantees upgrade safety (landing on `Syntax` or adding `Other(String)`; pick one at implementation time and report back).
- Invalid UTF-8 → `json::Error::InvalidUtf8` (**Q20=b**: an input-format error discriminated by `IoError.error.kind() == InvalidData` **and** the message equal to `invalid UTF-8 data` (`json::error`'s `INVALID_UTF8_MESSAGE`, both halves checked at `json/error.rs:112`) — the message half is what lets the LINES line-width cap and the charged `Read` backstop share the `InvalidData` kind with the pinned `CAP_MESSAGE` and still map to `Cap` (`json/error.rs:114`); externally it still carries the `json input:` prefix and is not `Error::Io`); a real read failure → `json::Error::Io` → carries over to the existing `Error::Io`. `LineFraming`'s own IO failures still go through `Error::Io`.
- The `Error::Ambiguous` path is unchanged.

## Semantic preservation and deviations

Invariants: `[]` → exactly one MISSING, LINES empty input → zero lines; empty document → zero records, whitespace-only document → error; trailing garbage at line end / document end → error; empty line → error; LINES' 1 MB line-width cap and line-start accounting; scalar semantics; no gap in the validation of skipped regions; byte-level duplicate keys last-wins with the first occurrence's position retained; **apart from those named below**, all existing tests pass as-is — the named ones being `matcher.rs`'s existing semantic cases (driver rewritten, assertions kept), `framing.rs`'s 10, deviation ③'s three test fixtures (assertions kept), deviation ⑧'s two expectations, deviation ⑩'s 3 deletions, and the `pending_high` read sites (deviation ⑫).

Confirmed deviations (the numbering is this document's deviations ①–⑬):

1. **DOCUMENT total-size lifting**: the 1 MB limit changes from "the whole document" to "each root". An array document whose elements are each ≤ 1 MB can be streamed at any size; a single-object/scalar root > 1 MB still errors.
2. **Relaxed timing for ambiguity errors on non-final-step Name**: a duplicate key found mid-stream errors immediately, and some records within that object may already have been produced before that; the final-step Name remains strict (staged until the object closes).
3. **Priority between LINES' cap and parse errors**: parsing is handed over entirely to struson, and under streaming it parses while being fed; a line with invalid content may report a parse error before the line-width cap is reached. The padding input of the two tests `lines_input_record_cap` and `lines_final_unterminated_line_over_cap` keeps a valid JSON shape, and their assertions are unchanged. The same priority reaches DOCUMENT's single-value root: `document_input_record_cap`'s **fixture** changed from a truncated `"aaa…` (1 MB + 1, unterminated) to a valid JSON string of the same length, because a single-value root is charged only at a batch boundary and the truncated value reports `IncompleteDocument` first (the reasoning is written in the test's own comment); its assertion (the cap) is unchanged. The three fixtures are this deviation's whole footprint on the existing suite.
4. **DOCUMENT error timing made streaming**: document-level format errors change from "all-or-nothing" to exposure element by element; records before the failing element have already been produced — and, because a record is delivered at a batch boundary while the element is still half-read, a record **from the failing element itself** can be delivered before that element's own `End` verdict fires the cap (`document.rs`'s `document_element_tail_is_charged_after_an_early_match` pins exactly that). The `rows_before` of an unterminated document is pinned case by case by probe fact 8.
5. **DOCUMENT element-level real offsets and ScanRange filtering**: a record (including MISSING) reports the **uncompressed absolute** first-byte offset of the element it derives from (single-value root = the root value's first byte; empty array with zero hits MISSING = 0); `last_record_start` is no longer constant 0 under DOCUMENT; `BytesProcessed` advances with the scan (`[...]` root final value = `scanned − 1`, the first `[` belonging to no record).
6. **The cap's metering scope = each root value's own span** (sampling points in §Byte supply and accounting, measured by probe fact 7): separators and whitespace between elements are excluded from every span, and the `]` is excluded from the last element; an element of exactly 1 MB inside an array, a single-value root of exactly 1 MB, and a line with a span of exactly 1 MB all pass right at the line.
7. **When the cap is determined and mid-stream cutoff**: an array root is determined by element span when the element ends; a single-value root samples its accumulated span at **batch boundaries** and determines then, and can be aborted midway. The exact span is the authoritative determination of the cap; in addition a metered `Read` backstop (`feed.rs`'s `BudgetedRead`, threshold `MAX_RECORD + 64 KiB`) cuts off a matched value far beyond the limit during reading, so the **resident-memory upper bound of a single-value root / matched value is O(cap) with a small constant factor**, not the whole document. The factor is on top of the cap rather than equal to it, because the proven charge counts input bytes served and the **rendered** length of the queued rows: the per-row `serde_json::Value`/`Map`/`Vec` overhead and the matcher's member-key index are real resident bytes that the tripwire does not see. Non-matching subtrees still have zero buffering, and struson's read buffer is still a fixed 1 KiB. **The backstop's residual window (recorded honestly)**: the metering accumulates from each root's `begin` **all** bytes that have been served, and bytes that belong to no span — the separator whitespace swallowed by the next `has_next()`, and the document tail swallowed by `consume_trailing_whitespace` — are likewise charged to that root; hence a root whose own span is under the cap but which is immediately adjacent to whitespace exceeding `CAP_ALLOWANCE` (64 KiB) may still be aborted by the backstop. This is a deliberately retained trade-off: the exact-span determination is still authoritative, and **no value of slack can eliminate this window** (when whitespace is plentiful, any finite margin will be eaten through). Mitigation does exist in the code: the metering is zeroed at every `begin`, and each element's start in an array root immediately follows the previous element's passing exact-span determination, so the backstop measures bytes "since the last verified boundary" rather than the whole document.
8. **Number tokens are no longer normalized**: serde_json's `arbitrary_precision` rewrites an unsigned exponent to `e+` (`1e309` → `1e+309`), whereas struson's `next_number_as_str()` is the original bytes. `RawNumber` from now on replays the input token verbatim; the two expectations of `raw_number_passthrough_no_decimal_parse` change from `1e+309` to `1e309` (assertion structure unchanged).
9. **Error type and message-source change**: JSON input errors change from `Error::Format(String)` to the structured variant `Error::Json(json::Error)`; the message source changes from serde_json to struson's `ReaderError` variants. **The blast radius is 16 sites** (measured): 7 in `json/lines.rs`, 7 in `json/document.rs`, 1 in `tests/stream_large.rs`, 1 in `events.rs`; of these, the 7 that assert the cap by exact string change to matching `Error::Json(json::Error::Cap)` (Display verbatim unchanged, not one line of assertion content changed), and the rest change to matching `Error::Json(e)` and then asserting the message from `e.to_string()` (the prefix and the verbatim message being guaranteed by `json::Error`'s Display). The trailing-data message is synthesized by the handler and kept verbatim. `crate::Error` keeps `derive_more::PartialEq` and is **not hand-written** (`json::Error` derives it too, its `Io` payload being the crate's `IoError`, which compares by `kind()`), otherwise `_ => discriminant` would make `Parse("x") == Parse("y")` true and break the existing `partial_eq_same_and_cross`.
10. **Test deletions**: three tests pin serde_json's reserved-key mechanism `$serde_json::private::Number` (`matcher.rs`'s `number_token_first_key_object_errors_not_panics`, `number_token_single_entry_is_a_scalar`, `skipped_subtree_rejects_a_reserved_number_token_object`, with the `NUMBER_TOKEN` constant deleted along with them), and that mechanism does not exist under the new core; the duplicate-key collapsing semantics is re-pinned by a test that does not depend on engine internals.
11. **Depth-limit implementation point change**: serde_json's recursion threshold 128 → `ReaderSettings::max_nesting_depth = 128` (struson's default is 128). Zero change in behavior.
12. **`pending_high` read-site change**: the high-water-mark pin moves into `Matcher`, and the handler exposes `#[cfg(test)] pending_high()`; the two assertions in `document.rs` are rewritten (semantics unchanged: an array root is bounded, a single-value root is bounded by the cap backstop).
13. **New `json`-side error module**: `crates/tinio-select/src/error.rs` gains the `Json` variant, whose payload is the public `JsonInputError` newtype over the `pub(crate)` `json::Error` (both `PartialEq` by derive, so the crate's `derive_more::PartialEq` is untouched); `crates/tinio-select/src/json/error.rs` is added.

Also noted (not within the ①–⑬ numbering; both items are named here):

- **Two retired matcher budget tests (beyond deviation ⑩)**: `matcher.rs`'s `element_barely_under_cap_is_accepted` and `budget_is_charged_per_root_not_accumulated` are likewise retired — they pin the now-obsolete `Budget` mechanism, and under the new architecture the matcher does not touch budgets; their coverage is taken over by `feed.rs`'s `a_root_exactly_at_the_cap_is_accepted`, `intermediate_batches_are_checked_against_the_same_origin`, `a_new_root_resets_the_account`, and the end-to-end shape of "four elements totaling over 1 MB must not be falsely reported" is still pinned by `document.rs`'s `document_array_larger_than_1mb_streams` (deviation ⑩ recorded only the 3 serde reserved-key ones; these two are recorded additionally here, and this does not change the requirement that all other existing tests pass as-is).
- **Two implementation additions (not seen in the table above)**: one more position reader than the enumeration in §PathSeg compilation state machine — a `position()` free function beside `ValueSource` (not a member: the matcher never samples a position), used only by the two handlers to sample `data_pos`, never called by the matcher; and `document.rs` issues one extra cap sample at the `StopRoot` handoff (first counting in the span of the part already read at the handoff, so as to avoid the tail of that element being left unbilled; the authoritative sample of that element's span is still the one at its `End`, see deviation ⑦).

## Test plan

Migration:

- `framing.rs`'s **10**: the 3 for `LineFraming` move into `line.rs`; the 2 for `BudgetedRead` are retired (budgeting is now borne by `feed.rs`'s `data_pos` accounting); the **5** for `DocFraming` are disposed of per "Takeover" below.
- `matcher.rs`'s existing semantic cases are rewritten to be driven by a scripted `ValueSource` (assertions kept item by item); the 3 named in deviation ⑩ are deleted.
- The existing tests in `lines.rs` / `document.rs` / `mod.rs` are not touched, not one line, apart from those named in deviations ③, ⑧, ⑩, ⑫.
- `lines.rs`'s `lines_skipped_subtree_still_validates_strings` and the unpaired-surrogate case at `matcher.rs:846` must pass as-is (probe fact 5 has confirmed that struson reports `UnpairedSurrogatePairEscapeSequence` on that path).

**Takeover of `DocFraming`'s 5 tests** (grilling Q22=a+c):

- `doc_array_elements_pull_one_at_a_time_with_offsets` → moved to `document.rs`, with `starts[0] == 1` replaced by a byte-level assertion on `last_record_start()` (probe fact 7: starts 1 and 11).
- `doc_scalar_and_nested_elements` → moved to `document.rs`.
- `doc_single_object_pulls_once` → moved to `document.rs` (start 0).
- `doc_separator_errors_are_framing_errors` → rewritten as `document_array_separator_errors_are_format_errors`, with all four inputs `[1,]`/`[1 2]`/`[1`/`[1] x` asserting `Error::Json` (fact 8 gives the kind of the first three; `[1] x` gets `TrailingData` from the trailing check).
- `doc_empty_and_ws_only_modes` → rewritten as `document_empty_vs_whitespace_only` (fact 10: both report `IncompleteDocument`, the difference being `data_pos`).

Added:

- `cap_is_measured_over_the_whole_root`: both shapes — an element of exactly 1 MB placed in an array, and a single-value root of exactly 1 MB — pass right at the line; an element over by 1 byte reports cap (deviation ⑥'s main pin).
- `cap_fires_after_the_prior_records`: records before the over-limit element have been produced, and none after it (the over-limit element's **own** record is the deviation ④ case rather than this one: it surfaces only when its match completed before the element's `End`, as `document_element_tail_is_charged_after_an_early_match` pins).
- `tail_check_synthesizes_our_messages`: `{} {}`/`5 {}` contain `trailing characters after the JSON document`; `{} x`/`5x`/`123#` do not (deviation ⑨ + fact 9's case-by-case distribution). (**2026-09-13 measurement correction** (ruling D1): `{} x` in fact also goes through `consume_trailing_whitespace` and reports `TrailingData`, and thus takes the **same** synthesized message as `{} {}` and `5 {}` — struson's trailing API reports `TrailingData` for any non-whitespace trailing byte, and cannot distinguish the two; this line's old assertion that `{} x` "does not contain" it is retired, and only `5x` / `123#` still error in the value-reading stage and do not carry that message. `tail_check_synthesizes_our_messages` pins these two sets per the measurement.)
- `document_duplicate_keys_collapse_last_wins_first_position`: a duplicate-key collapsing semantics pin that does not depend on engine internals.
- `document_root_shape_decided_before_first_read`: `peek()`'s probe of `{`/`[` advances `pos()` by only 0 (fact 3), and the decision does not cross over the whole root.
- `peek_never_moves_data_pos` (`json/struson_contract.rs`): asserts for all seven value types, one by one, that `data_pos` is equal before and after `peek()` (the regression pin for fact 3). (**2026-09-18 correction**: this coverage lives in the contract module's guard tests, not in a handler-level addition; the name `value_start_equals_data_pos_after_peek` was never added, so a second case would duplicate it.)
- `lines_line_width_cap_still_fires_mid_serve`: `LineFraming`'s line-width cap still takes effect during reading.
- `matcher_name_step_duplicate_reports_ambiguous_at_object_close`: the stage-then-decide-at-close semantics of the final-step Name.
- **Adversarial-input no-panic table**: for a set of malicious/malformed inputs (including the shape that `peek`s again after the top-level value, deep nesting, truncated values, unpaired surrogates, invalid UTF-8), assert that only `Err` is returned and it never panics — fact 4 proves the panic risk is real, and this is the line of defense.

The CI large-document streaming tests and the `pending_high` high-water-mark pin keep their existing arrangement (`tests/stream_large.rs` + `document_array_root_keeps_pending_bounded`).

## Acceptance criteria

- Apart from deviation ③ (three test fixtures, assertions unchanged), deviation ⑧ (two expectations), deviation ⑩ (3 deletions), and deviation ⑫ (two read sites), the entire existing test suite passes as-is; all assertions are retained for `document_start_above_zero_yields_nothing`, `document_last_record_start_is_zero`, `document_unterminated_input_is_a_format_error` (five `rows_before`), and `document_single_root_trailing_garbage_errors` (five true/false). (**2026-09-13 measurement correction** (ruling D1): these five lines are **not all retained** — `{} x` flips from false to true and takes the same synthesized message as `{} {}` and `5 {}`, because struson's `consume_trailing_whitespace` reports `TrailingData` for any non-whitespace trailing byte, and the old core's true/false distinction for that line is **unrecoverable**; the other four lines are retained as-is, and the five assertions remain exact assertions. Deviation ⑨ records this point.)
- The acceptance legs of `docs/tests.md`: `cargo test -p tinio-select`, `cargo test --workspace --exclude tinio-e2e`, `cargo test -p tinio-select --features parquet`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo +nightly fmt --all -- --check`.
- **Throughput threshold (grilling Q23=a, carrying over actson Q4-D)**: `benches/select_scan.rs`'s `json_document_scan` must not be slower than **58.620 ms** (the measured time of the current working tree's serde streaming core) (**2026-09-13 execution terms**: 58.620 ms is a historical measurement of the actson plan on another machine and **does not constitute the threshold**; the actual threshold is taken from "the local-machine baseline on the day of Task 1, 47.080-52.227 ms / 16.393-18.185 MiB/s", see the note at the end of this line); three numbers are recorded side by side — the old whole-document implementation 37.023 ms (quoted from the superseded plan, for historical reference only), the current streaming core 58.620 ms, and the struson core — **2026-09-13 local measurement**: `time` 42.209-44.780 ms, `thrpt` 19.119-20.284 MiB/s (Intel Core i7-14700KF, rustc 1.98.1), below the streaming-core baseline of Task 1's day, 47.080-52.227 ms / 16.393-18.185 MiB/s; criterion's `change:` line reads `time: [−25.073% −9.6399% +10.886%] (p = 0.33 > 0.05)`, `thrpt: [−9.8176% +10.668% +33.462%]` ("No change in performance detected") — p > 0.05, so the change line's point estimate is not trustworthy, and direction is taken from the absolute intervals (the two intervals do not overlap; this run is faster overall). The threshold is the local-machine baseline on the day of measurement; 58.620 ms is a historical reference value of the actson plan on another machine and does not constitute the threshold. Being slower than the threshold means stop and evaluate, and do not enter acceptance carrying a regression.
- Manual verification of the memory commitment (written into the execution report): no whole-document/whole-line buffering; non-matching subtrees are validated only, not materialized; the exact span is the cap authority, and in addition the metered `Read` backstop cuts off a matched value far beyond the limit during reading, so the **resident upper bound is O(cap) with a small constant factor** rather than the whole document (the accounting counts input bytes served plus the rendered row bytes, so per-row container overhead and the member-key index make the factor exceed 1); the cap's determination timing and the scope of its weakening are recorded honestly.
- **Version coupling note**: this design depends on 0.7.2's `data_pos`/EOF/trailing-check behavior (§Verified external facts 1–12). The workspace writes `struson = "0.7"`; upgrading to 0.8+ requires re-running the probes and re-verifying these facts — 0.8-dev's `ReaderSettings` fields and buffering strategy are already different.

## Synchronized updates

- `docs/superpowers/plans/2026-09-12-json-streaming-actson.md`: add a superseded annotation line at the top of the file (keep the original text, do not silently rewrite).
- `docs/superpowers/specs/2026-09-04-select-object-content-design.md`: annotate the JsonReader line and the byte-0 pin note as **superseded 2026-09-12**.
- `docs/superpowers/plans/2026-09-04-select-object-content.md`: annotate the byte-0 notes of Task 9 / Task 10 the same way.
- `crates/tinio-select/src/json/mod.rs`: rewrite the module documentation's deviation list (currently ①–⑥) to the table above's ①–⑬.
- `crates/tinio-select/src/record.rs`: add a sentence to the `MAX_RECORD` comment — the memory defense is "non-matching subtrees are not materialized + one root ≤ 1 MB", the cap is a record-shape rule, and the sampling points are in the byte-supply section of this spec (**2026-09-18 correction**: `record.rs` has no `§Byte supply and accounting` section of its own; the comment cites this design spec, and the instruction above now says what the comment does).
- `crates/tinio-select/src/error.rs`: add the `Json` variant and its `JsonInputError` newtype (the `PartialEq` stays derived), and extend the `display_messages` test; sync the Error section of `docs/style.md` as needed.
- `crates/tinio-select/src/json/struson_contract.rs` (new): the upstream-behavior contract test module for struson 0.7.2 (the inventory is in §Overall architecture); re-run this first when upgrading upstream.
