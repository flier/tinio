# JSON streaming reader rewrite plan (actson kernel)

> **SUPERSEDED 2026-09-13**: this plan is **no longer the basis for implementation** — the kernel choice has been superseded by `docs/superpowers/specs/2026-09-12-json-streaming-struson-design.md` (struson 0.7.2). This file is retained as the kernel-selection and review record for actson: the kernel-independent conclusions in §Revisions (R11.1 budget scope, R11.3 io failure reporting, R11.4 key-comparison reuse, R11.7 adversarial input, R11.9 LINES basis, R11.2 import style) apply equally to the struson version. The original text is not silently rewritten.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the serde_json parsing kernel of `crates/tinio-select/src/json/` with `actson` 2.1.0 (a reactive / non-blocking event-style JSON parser): bytes are pulled on demand by the parser from a `Read`, and **the whole flow neither reads the document whole nor copies the document whole** — no whole-document buffer, no whole-line buffer, no root-value buffer; matching and materialization are both driven directly by the event stream, and at any moment the only things in memory are the "current token" and the "current record". The `json::Reader::new` signature, the `RecordReader` contract, the `Record::Json(Option<Value>)` record model, and the engine-side `to_value`/`json_lookup` all stay unchanged, with zero changes at the call sites.

**Architecture:** `actson::JsonParser<StreamFeeder<R>>` is the only parser. `StreamFeeder` implements `actson::feeder::JsonFeeder` (three methods: `has_input`/`is_done`/`next_input`), fetching bytes from the underlying `Read` on demand when the parser asks for them, and taking care of two things along the way: charging 1 MB of root budget per byte (the cap takes effect mid-way), and recording the absolute offset of bytes already served (framing bytes are refunded by the handler by offset; the container-depth handle has been removed wholesale, see §Revisions R3). The event sequence drives the explicit stack state machine in `matcher.rs` (no longer serde's `DeserializeSeed`/`Visitor`), and a hit subtree is assembled in place into a `serde_json::Value` (with no text slicing / escape decoding of any kind); a subtree that is not hit is skipped purely by event depth, materializing not a single byte. The only difference between LINES and DOCUMENT is "where the next root starts": LINES is served one line at a time by `LineFraming`, while DOCUMENT has its element boundaries delimited by actson's own top-level array syntax — the old `DocFraming` bracket/comma rewrite layer is deleted wholesale.

**Tech Stack:** Rust 2024, `actson 2.1.0` (MIT, default features; a new workspace dependency), `serde_json 1.0` (`arbitrary_precision` + `preserve_order`, serving only as the **record value model**, no longer doing the parsing), `serde` (still transitively required by `serde_json`; tinio-select no longer directly `use serde::*`).

**Spec:** `docs/superpowers/specs/2026-09-12-json-streaming-design.md` (the authoritative source for §parsing kernel/§framing filter/§PathSeg compiled state machine/§handler and deviations ①–⑫; Task 1 of this plan first rewrites it onto the actson kernel, to be read together with the spec during execution)

---

## Why actson and not rill-json (the selection record of this plan)

| Dimension | rill-json 0.5.0 | actson 2.1.0 |
| --- | --- | --- |
| Input model | `parse_streaming(&'a str)` — **must be an already fully-held `&str`**, and the entry point hard-codes `MAX_JSON_SIZE_BYTES = 10 MB` as a whole-string limit | `JsonParser<T: JsonFeeder>`, pulling byte-by-byte on demand via `next_input() -> Option<u8>`; `BufReaderJsonFeeder` is ready-made, and `Read` can be wired up oneself |
| Resumable across chunks | No (the tokenizer holds `&[u8]` + `cursor`, with no "come back later" state) | Yes (`NeedMoreInput` + `put_back` + a token buffer that spans calls) |
| Semantic fidelity | `JsonNumber` has only I64/U64/F64 (`1e309`/integers beyond u64 lose the original token); `JsonValue::Object` is a `BTreeMap` (key order lost) | Event stream + `current_str()`, a verbatim token buffer (value bytes enter the buffer exactly as they arrive, with no normalization); key order is up to the caller and is naturally preserved |
| Fit | Doing this design would require vendoring + rewriting the tokenizer/parser into an incremental form | Simply implement one `JsonFeeder`, with zero upstream changes |

Conclusion: **actson is the only off-the-shelf parser that both leaves upstream untouched and satisfies "does not read the document whole / does not copy the document"**. rill-json's "streaming" is "event-style", not "incremental"; the two read alike but their constraints are opposite.

## Behavioral differences from the old serde_json kernel (all settled one by one, written into the spec deviations)

1. **(old deviation ③ eliminated)** A single-value root no longer "accumulates the whole document's match set in one pull" — actson drives event by event, and hit values are enqueued **one by one** when the container closes, so `pending` is independent of the document's total size. Old deviation ③ (and Task 7's two cap-attribution tests) are retired along with it.
2. **(old deviations ④⑥ retained, wording unchanged)** The cap is still "one root ≤ 1 MB"; DOCUMENT's element-level true offsets and ScanRange filtering are unchanged.
3. **(new deviation ⑦: cap charging point; revised 2026-09-13)** In the old implementation the `[`/`]`/`,`/`line terminator `\n` swallowed by framing at the top level did not enter the budget; in the new implementation these bytes really do flow through `StreamFeeder`. Settled: **the cap charges the root value's own bytes ≤ 1 MB, and framing (DOCUMENT's top-level `[`/`]`/`,` and top-level whitespace) is not charged**. Implemented as `Budget` charging everything by default + the handler refunding framing by offset, with the criterion `charged − refunded > MAX_RECORD` (no slack in flight: the handler refunds immediately on every event, and disarms when the root completes; R9.3 has overturned R3's +2 slack — it let the MAX+1 byte input of `document_input_record_cap` slip through as `NoMoreInput`; for the charging window of >1 MB of consecutive top-level whitespace see deviation ⑫/R9.5). Consequently `document_input_record_cap_exact_allowed` (a scalar root of exactly 1 048 576 B), `lines_input_record_cap_exact_span_allowed` (a line spanning exactly 1 MB) and `[<an element of exactly 1 MB>]` all pass exactly at the limit. **The original text's "only bytes with `container_depth >= 1` are charged" is abolished** — it let scalar roots never be counted (the cap was entirely ineffective, `document_input_record_cap` went red while `_exact_allowed` was falsely green), and it also made `[<an element of exactly 1 MB>]` be misjudged as over the limit because of the closing `]`.
4. **(new deviation ⑧: number tokens are no longer normalized)** serde_json's `arbitrary_precision` scanner rewrites an unsigned exponent as `e+` (`1e309` → `1e+309`); actson's token buffer is verbatim bytes. Settled: **`RawNumber` replays the input token verbatim from now on**. The two expectations of `raw_number_passthrough_no_decimal_parse` change from `1e+309` to `1e309` (the assertion structure is unchanged).
5. **(test deletion; 2026-09-13 revision list)** Pinned to serde_json's reserved key mechanism `"$serde_json::private::Number"` are **three** tests: `matcher.rs`'s `number_token_first_key_object_errors_not_panics`, `number_token_single_entry_is_a_scalar`, **and the `skipped_subtree_rejects_a_reserved_number_token_object` the original text missed**; the `NUMBER_TOKEN` constant at the top of `matcher.rs` is deleted along with them (the `number_token_map_entry_is_a_scalar` named in the original text **does not exist in the tree**). That mechanism does not exist under the new kernel, so all three are deleted; the **semantics** of duplicate-key collapsing are re-pinned with a test that does not depend on engine internals (`duplicate_keys_collapse_last_wins_first_position`, Task 5 matcher layer), and `document_wild_over_object_collapses_duplicate_keys` has only its comment rationale changed with not one line of the test touched (Task 9 Step 3).
6. **(new deviation ⑨: error text)** Parse-error text changes from serde_json's `"json input: <serde msg>"` to `actson`'s `ParserError` (`IllegalInput`/`SyntaxError`/`NoMoreInput`) wrapped by the same prefix. The only message still pinned verbatim remains `CAP_MESSAGE = "input record exceeds 1 MB"` (verbatim unchanged). Existing tests only assert `Error::Format` and the message prefix, not serde's wording — **R9.4 correction**: `document_single_root_trailing_garbage_errors` pins verbatim tinio's own message `trailing characters after the JSON document` (not serde's wording), so "only asserts the prefix" does not hold for it; settled as synthesized by the handler (after the root closes the parser errors → synthesize that message), and one expectation for `{} x` flips to true.
7. **(new deviation ⑩: nesting depth limit; revised 2026-09-13)** serde_json's recursion depth limit of 128 (`from_slice`/`from_reader` default) → actson `JsonParserOptionsBuilder::default().with_max_depth(MAX_DEPTH).build()`. **To keep 128 levels you must pass `MAX_DEPTH = 129`**: actson's stack is bottomed with `MODE_DONE` and its criterion is `stack.len() >= depth`, so `with_max_depth(n)` actually allows only `n - 1` levels (actson's default 2048 → 2047 levels). Going over the limit reports `SyntaxError` → `Error::Format`. No existing test has a 128-level case, so there is zero change on the behavioral surface.

---

## Revisions (2026-09-13 grill resolutions)

> This section is produced by the two rounds of grilling on 2026-09-13 and the three-way fact-checking (the current actual state of the `json/` module, the actson 2.1.0 source + probe measurements, and the wording of the spec/old documents). **When it conflicts with any task main text below, this section prevails**; the parts this section does not overturn are executed as originally written. If during execution a new behavioral difference not covered by this section is found → **stop and ask** (Q9-A), and do not extend the deviation list on your own.

### R0 · Phase 0: spike (before Task 1 is executed)

This same module was already rewritten once today with the serde streaming version and measured: **whole document 37.023 ms → streaming 58.620 ms (thrpt 23.125 → 14.605 MiB/s, −30.6%, p=0.02, a "real regression")** (see `plans/2026-09-12-json-streaming.md`'s Task 8 measurement record). actson also pulls byte-by-byte, and the throughput of a second rewrite is the biggest unknown — quantify it first at minimum cost, then decide whether to rewrite wholesale.

1. **Probe** (Q3-A): `cargo fetch` to pull actson 2.1.0. On this machine `CARGO_HOME=E:\.cargo`, TUNA sparse mirror; already measured externally: direct connection to crates.io 200, sha256 consistent with the index, probe crate compiles and runs. Stop on failure — do not vendor, do not change the registry.
2. **Land the dependency**: that is, the two one-line changes of Task 2.
3. **Write only `feed.rs`**: the entire output of Task 3 (`StreamFeeder` + `Budget`), implemented with R3's semantics. Do not write `stream.rs`, matcher, or handlers.
4. **bench**: add `json_events_only` to `benches/select_scan.rs` — the 897 751 B array fixture, the same harness as `json_document_scan`, `Budget` **armed** (it must include the real path of byte-by-byte charging), driving `JsonParser::next_event()` bare to EOF, with no matching and no materializing. This bench is kept permanently.
5. **Criterion**: ≤20 ms → continue the wholesale rewrite; >35 ms → stop and go back to the user to reopen; 20–35 ms → stop and hand the number to the user for a decision. Bare parsing is the **lower bound** of the whole façade (Q4-D's yardstick is façade ≤ 58.620 ms).

### R1 · The final shape of the deviation list (Task 1 Step 4 is executed by this, not by the original text's "reorder as ①–⑩")

The spec's current state is a Markdown ordered list `1.`–`6.`, with each item already carrying an `(Amended 2026-09-13...)` block; `①–⑩` exists only in this plan. Therefore **do not reorder and do not rewrite**:

- Keep the original `1.`–`6.` and the existing Amended blocks, and append two more: **③** — under the actson kernel, a single-value root's hit values are enqueued **one by one** when the container closes, and `pending` is unrelated to the root's size (driven by `Progress::StopRoot`); `document_single_root_under_cap_yields_all_matches` / `document_single_root_over_cap_errors` still hold, with the former's rationale changed from "backstopped by the cap" to "driven event by event, naturally bounded". **⑥** — the offset contract is unchanged, and the derivation mechanism is redone per R4.
- Append `7.`–`12.`, each with its own `(Amended 2026-09-13: actson kernel)` block: **⑦** the cap charging point = **the root value's own bytes** (framing is not charged; the criterion `charged − refunded > MAX_RECORD` has no slack, see R3/R9.3); **⑧** number tokens are replayed verbatim (`1e309` is no longer rewritten to `1e+309`); **⑨** the parse-error text is now provided by `actson::ParserError` (what is pinned verbatim is `CAP_MESSAGE` and the trailing message — the latter synthesized by the handler, see R9.4); **⑩** the nesting depth limit `MAX_DEPTH = 129` (see R2.2); **⑪** actson's escape laxity (see R2.4) — recorded as a known-gap, with no code change; **⑫** >1 MB of consecutive top-level whitespace triggers the cap before a refund opportunity (R9.5) — a known-gap, with no code change.
- Inline references in the main text continue to use the circled numbers ①–⑫.

### R2 · actson fact corrections (the whole "verified external facts" block has been replaced in place; the execution points are as follows)

1. **`has_input()` is not part of the contract**: the parser never calls it; when `next_input() == None` it only branches on `is_done()` (`None + !is_done()` → `Ok(Some(NeedMoreInput))`, `parser.rs:343-358`). The plan's `is_done() = done && !has_input()` returns false when the cap fires (there are still bytes in the buffer) → `Session::drive`'s `continue` **loops forever**. **Fix**: `fn is_done(&self) -> bool { self.done }`.
2. **`MAX_DEPTH = 129`**: actson's stack is bottomed with `MODE_DONE` and its criterion is `stack.len() >= depth`, so `with_max_depth(128)` actually allows only **127** levels; to keep serde_json's 128-container threshold you must pass 129. Going over the limit reports `SyntaxError`.
3. **There is no peek**: `put_back` is used only when `streaming = true` to terminate a top-level scalar (this plan does not use streaming) → `parsed_bytes() == byte_offset()`. Task 3's `StreamFeeder::value_start`, `Session::drive`'s `parsed_before` parameter, and the `value_start(true)` assertion in `offset_advances_by_served_bytes` are **all deleted**; offsets are redone per R4.
4. **Escape laxity (an upstream defect)**: a lone low surrogate `"\uDD1E"` → `SyntaxError` ✓; a lone **high** surrogate `"\uD834"` → **silently passes** (leaving the literal 6 characters in the token); `"\uD834\u0041\uDD1E"` → **accepted and produces garbage**. Remedying it is not feasible (the legal input `"\\uD834"` decodes to the same 6 characters, so any heuristic will necessarily false-positive) → deviation ⑪ known-gap.
5. **UTF-8 is not validated during parsing**: a string with invalid UTF-8 will emit `ValueString` normally, and only fail at `current_str()`. → The handler must call `current_str()` on **every** `ValueString` and `FieldName` event and **propagate the error**; the `parser.current_str().ok()` in the Task 7 draft is a bug (it silently turns invalid UTF-8 into an empty string, breaking existing tests of the `skipped_subtree_still_validates_strings` kind).
6. **EOF flushes unterminated tokens**: an unterminated **number/keyword** is treated as a legal value (`12` → `ValueInt`, `tru` → `ValueTrue`); an unclosed container/string reports `NoMoreInput`. `[{"a":` still errors ✓, but every "truncated inside a token" case must be re-run.
7. `JsonEvent` is `Copy + PartialEq` and `ParserError` is `Copy`; `parser.feeder` is a **pub field** ✓ (`Session::feeder()`/`feeder_mut()` hold); `JsonParserOptions`'s fields are `pub(super)` (the `Default` impl is pub; customizing `max_depth` still must go through the builder, default 2048, `streaming = false`) ✓. **`reset()` clears `parsed_bytes`** (the original statement "not cleared across roots" was wrong; after R2.3 the offsets are all self-accounted by StreamFeeder, so it has no impact), and after `Ok(None)` you cannot keep feeding without `reset()` (the upstream `too_many_next_event` test has the same cause) — each LINES line must `reset_for_next_root()` (see Task 7 ⚠⑤).

### R3 · cap and budget (overturns the original wording of §behavioral differences item 3, and deletes the whole depth pipeline)

- **Semantics (Q6-B)**: the cap charges **the root value's own bytes ≤ 1 MB**; DOCUMENT's top-level `[`/`]`/`,` and top-level whitespace are framing and are not charged.
- **Implementation (settled by R9.3, overturning the original +2 slack)**: `Budget` **charges everything by default** (after `arm()` every served byte deducts 1, **no longer looking at container depth**), and the handler uses the **offset** it is already computing to refund immediately, **on every event**, the framing served since the previous event (**the baseline for the first event is the arm point**, not "the previous event" — otherwise the `[` byte would stay on the books and `[<an element of exactly 1 MB>]` would be misjudged as over the limit); the criterion is `charged − refunded > MAX_RECORD` — **no slack in flight** (the original +2 slack would let the MAX+1 byte input of `document_input_record_cap` be misjudged at EOF as `NoMoreInput` instead of CAP_MESSAGE). **The scope of arm/disarm is one "record root", not one poll** (2026-09-13 review revision): in array mode each element is a root, in single-value-root mode the whole root value is one root, and one LINES line is one root; `Progress::StopRoot` (R9.2's batch delivery) **does not disarm** — with a single-value root + deep wildcard the same root keeps being fed across multiple polls, and the budget must accumulate all the way; if it were reset per poll, each round would charge only a dozen bytes and the cap would never fire (the existing `document_single_root_over_cap_errors` would go red, and deviation ①'s "a single object/scalar root > 1 MB still errors" would fail at the same time). disarm happens only at the root's completion event (a scalar value event / the outermost container closing) — after that the served `]`/`,`/top-level whitespace are not charged anyway, and an element of exactly 1 MB passes exactly at the limit.
- **Deleted**: the `Rc<Cell<usize>>` depth handle, `Session::depth()`, `StreamFeeder::new`'s `depth` parameter, Task 4's `depth_is_tracked_through_containers` test, and all `depth.set/get` in Tasks 6/7. The matcher does not touch the budget.
- **`Budget` interface**: `arm` / `disarm` / `fired` / `refund(n)`; `charge()` takes no arguments; the cap still fires by going `next_input() → None`.
- **LINES**: with everything charged by default, line content really is charged too (the original text's hole of "LINES is never counted" disappears); `LineFraming`'s span cap (content + terminator ≤ 1 MB) is retained, and the two bases are consistent.
- **Known window (deviation ⑫, R9.5)**: >1 MB of consecutive top-level whitespace (at the document start, between elements) triggers the cap before the handler gets a refund opportunity — an inherent window of charge-then-refund (actson does not expose "whether we are inside a token", and the feeder cannot distinguish top-level whitespace from in-root whitespace, so pause-refund-resume is not feasible). Under the old kernel whitespace was free. known-gap, with no code change.

### R4 · Offset derivation (contract unchanged, mechanism redone)

The contract is still "a record (including MISSING) reports the first-byte offset of the root element it was dispatched from" (deviation ⑥). Container: `byte_offset() - 1` at the moment of the `StartObject`/`StartArray` event is the opening bracket position, which is exact. Scalar: there is no peek, so the token start must be derived by hand (token last byte = `byte_offset() - 1 - (trailing delimiter ? 1 : 0)`; the delimiter and the string's original length are determined from the "most recently served raw bytes" kept by `StreamFeeder`, taking the degenerate branch when a 64 KiB refill is crossed). **The concrete mechanism is settled at implementation time, and the criterion is three existing exact assertions + one new one added in Task 6 Step 1**: `document_leading_whitespace_shifts_the_first_element_offset_only` (3 / 11, **new**, not yet present in the tree), `lines_indented_line_reports_the_line_start`, `document_record_starts_are_element_offsets_monotonic`, `document_last_record_start_is_zero`. **If it cannot be reproduced → stop and ask, and do not relax the contract on your own.**

### R5 · Test disposition corrections

- **One more entry for the deletion list**: `skipped_subtree_rejects_a_reserved_number_token_object` (same cause as the two `number_token_*` tests: it pins serde_json's `$serde_json::private::Number` reserved key mechanism); the `NUMBER_TOKEN` constant at the top of `matcher.rs` is deleted along with it.
- **Coverage take-over (deleting without taking over is not allowed)**: `element_barely_under_cap_is_accepted` and `budget_is_charged_per_root_not_accumulated` are **matcher-layer** budget tests, and under the new architecture the matcher does not touch the budget (R3) → both are retired, and their coverage is taken over at the handler layer: `document_input_record_cap_exact_allowed` (a scalar root of exactly 1 MB), `document_element_exact_cap_inside_array_root_is_accepted` (new, `[<exactly 1 MB>]`), `document_array_larger_than_1mb_streams` (4 × 400 KB, no false positive).
- **Count corrections (re-verified on the frozen working tree on 2026-09-13; baseline = the committed `b20e7eb`, `git status` clean apart from this plan and one old spec)**: measured on that trunk, `framing.rs` has **10** tests (LineFraming 3 + BudgetedRead 2 + **DocFraming 5**, the last 5 retired along with the file), `matcher.rs` **987** lines, `document.rs` **21** tests, `lines.rs` **26** tests, `json/mod.rs` **20** tests, `core.rs` **122** lines — the original text's 936 and one version's "correction" of 1018 are both inaccurate; the tree was still moving during the review, so **if the working tree deviates from `b20e7eb` again before execution, freeze it first and then re-verify this group of counts**.
- `index_over_number_map_is_missing`'s input is an ordinary number `{"x": 1e309}`, its semantics are unchanged, and it is retained.
- The remaining rows of the Task 9 Step 4 disposition table are unchanged.

### R6 · Throughput baseline and diagnosis order

- Task 10's basis: compare **against 58.620 ms / 14.605 MiB/s** (the serde streaming façade), **not** against 37.023 ms; keep all three numbers in the comment (whole document 37.023 / serde streaming 58.620 / actson measured).
- **Acceptance threshold (Q4-D)**: actson's `json_document_scan` **must not be slower than 58.620 ms**, otherwise **stop and evaluate** — no automatic optimization, no threshold changes.
- If it falls just barely short, the diagnosis order: ① `verbatim_number` (one `serde_json::from_str` per number token, and the bench fixture has two numbers per element) ② byte-by-byte `Budget::charge` ③ refill granularity. Still over the threshold → stop.
- **No throughput threshold for LINES** (settled in the 2026-09-13 review): the threshold applies only to `json_document_scan` (the DOCUMENT façade) — the old plan likewise only recorded DOCUMENT's 58.620 ms, and there is no serde-version number on the LINES side to compare against, so setting a threshold off the top of one's head would only become a fake threshold. LINES's regression is instead checked as "no obvious regression" in the **manual checklist of Task 11 Step 2**, with the measured numbers written into the report and not into the `mod.rs` docs.

### R7 · Memory gate

- Keep the in-crate `#[cfg(test)]` `pending_high` high-water-mark pin (counting a `#[global_allocator]` is blocked dead by `[lints.rust] unsafe_code = "forbid"`, settled on 2026-09-13; see the Amendment in spec §test plan). **The pin currently lives in `core.rs` (the actual state at review time); when core.rs is deleted it moves to `Matcher`** (`rows` is its only data source), with `Document`/`Lines` each exposing `#[cfg(test)] fn pending_high(&self) -> usize`, and the two existing assertions changed to read through it (R9.6).
- Add one **large-document** case: it must be an **in-crate unit test** (`pending_high` is `#[cfg(test)]`, and `tests/*.rs` integration tests cannot read it), using a **generative `Read`** (producing 100k elements on demand, neither materializing nor using an LFS fixture) to feed a multi-MB document and assert that the high-water mark has a constant upper bound — **lands as Task 10's new Step 5** (R9.7).
- Task 11 Step 2's manual checklist is retained, with its role changed to "explain why the pin holds".

### R8 · Task 12: translate after everything is complete

- Once Task 11 is all green, translate this plan and `specs/2026-09-12-json-streaming-design.md` **in place into English** (overwriting the Chinese, leaving no copy; the git history is the copy); code identifiers, deviation numbers and test names are not translated as-is, and the `superseded`/`amended` annotations are translated along with the main text.
- Commit order: the user commits the code by hand first, and **the translation is a separate commit**.

### R9 · 2026-09-13 review fix round (plan review + actson source re-check + three user resolutions)

The plan was reviewed line by line and re-checked against the actson 2.1.0 source (docs.rs + GitHub tag 2.1.0), and the following revisions have been **written back into the main text in place**; this section is the summary and the record of resolutions. When it conflicts with the main text, this section prevails.

1. **Empty-input semantics (actson evidence)**: empty input and whitespace-only input are both `Err(NoMoreInput)` under actson, not `Ok(None)` (`Ok(None)` occurs only after a complete top-level value). Task 4's `empty_input_ends_immediately` has been rewritten as a session-layer Format error assertion; the DOCUMENT handler distinguishes by "`feeder().byte_offset() == 0` and zero events": 0 bytes → zero records and no error (`empty_document_and_array_yield_nothing`), >0 bytes → `Format("json input: nothing more to parse")` (`document_whitespace_only_is_a_format_error` only asserts the prefix, and actson's text passes directly). An empty LINES line takes the same `Err` path (Task 7 ⚠③ closes the loop: all of them assert only the prefix, and the draft's synthesis branch is deleted). `document_unterminated_input_is_a_format_error` is unaffected: the EOF flush lets `[1` emit a record first and then report `NoMoreInput`, consistent with its `rows_before` assertion.
2. **StopRoot = "a batch of deliverable records has been produced"** (not "the root is complete"): the matcher returns StopRoot when `rows` is non-empty, and after the handler delivers it, the next poll keeps feeding the same root — `document_array_root_keeps_pending_bounded` (≤64 B) and the memory discipline require exactly this batch semantics. A hit value for a final-step Name is, per semantics 6, deferred until the object closes before being enqueued, so its StopRoot also appears only at the close event (Task 5's `stop_root_is_reported_when_a_root_completes` has been rewritten accordingly and renamed `stop_root_is_reported_when_a_batch_is_ready`). The spec main-text wording of Task 1 Step 3 has been synchronized.
3. **cap criterion (user resolution Q1)**: `charged − refunded > MAX_RECORD`, with **no +2 slack**; the handler refunds framing (the non-root bytes served since the previous event) immediately on every event, and **disarms at the root's completion event (a scalar value event / container close)**. R3's +2 formula has been overturned: `document_input_record_cap` (input of MAX+1 bytes) would slip through as `NoMoreInput` under +2. (2026-09-13 review addition: the scope of arm/disarm is **one record root**, not one poll — `StopRoot` does not disarm, and for the criterion see R11.1.)
4. **Trailing garbage message (user resolution Q2)**: after the root value is fully closed (a single-root scalar/container close, or the root array closing), any further error from the parser → the handler synthesizes `json input: trailing characters after the JSON document` (DOCUMENT) / `json input: trailing characters on line` (LINES). The `{} x` expectation of `document_single_root_trailing_garbage_errors` flips from false to true (the other four are unchanged); the LINES tests only assert the prefix and have zero changes. **Why only `{} x` flips (verified in the actson 2.1.0 `src/parser.rs` source)**: at a legal terminator actson **emits the value/close event first and then reports the error**, so all three of `{} x`, `{} {}`, `5 {}` hit the synthesis branch (`{} {}`/`5 {}` were already expected true); whereas when a number is followed by an illegal terminating byte (`5x`, `123#`) `parse()` directly returns `Err(SyntaxError)`, **emitting no value event**, so the handler cannot see that "the root is complete" and the message flows out as-is → those two expectations stay false.
5. **Deviation ⑫ (new, user resolution Q3)**: >1 MB of consecutive top-level whitespace (at the document start, between elements) triggers the cap before the handler gets a refund opportunity — an inherent window of charge-then-refund (actson does not expose "whether we are inside a token", and the feeder cannot distinguish top-level whitespace from in-root whitespace, so the pause-refund-resume mechanism is not feasible). Under the old kernel whitespace was free. Recorded as a known-gap, with no code change.
6. **core.rs disposition**: delete it (`Core::pull` is serde-kernel machinery as a whole; the shared state is taken over by the handler/Matcher respectively). The `pending_high` high-water-mark pin moves to `Matcher` (`rows`'s new home), the two handlers each expose `#[cfg(test)] fn pending_high()`; the read path `r.core.pending_high.get()` → `r.pending_high()` for the two assertions (`document_array_root_keeps_pending_bounded` ≤64 B, `document_single_root_under_cap_yields_all_matches` 0< ≤1 MB), listed in the Task 9 disposition table (Global Constraints' "do not touch a single line" grants an exemption for these two lines).
7. **R7 large-document generative unit test**: lands as Task 10's new Step 5 (in-crate `#[cfg(test)]`, a generative `Read`, 100k elements and multiple MB, asserting `pending_high`'s constant upper bound).
8. **Three corrections in the facts section**: `reset()` **does** clear `parser.parsed_bytes` (no impact after R2.3, but each LINES line must reset — after `Ok(None)` you cannot keep feeding without reset, and Task 7 has added `reset_for_next_root()`); `current_float()` returns `Ok(inf)` for an overflowing token rather than erroring ("never used" unchanged); `JsonParserOptions`'s `Default` impl is pub (customizing max_depth still must go through the builder, conclusion unchanged).
9. **`lines_skipped_subtree_still_validates_strings`**: the input `"\uD800"` (a lone high surrogate) is exactly deviation ⑪'s silent-pass class, and under actson no error is produced. The input changes to `"\uDC00"` (a lone low surrogate, which actson reports as `SyntaxError`), and the test's intent is fully preserved; listed in the Task 9 disposition table and deviation ⑪'s Amended block.
10. **Signatures and interfaces settled**: `CompiledPath::from_segments(&[PathSeg])` (the useless `_ty: Type` parameter is dropped, consistent with the current code and spec :46 Amendment); `vevent() -> Result<VEvent<'a>, Error>` (R2.5 propagation); `StreamFeeder` gains `inner()/inner_mut()`, and `Session` gains `feeder_mut()` (LINES calls `start_line()` via `session.feeder_mut().inner_mut()`); LineFraming's `capped` changes from a field to a method (there is no `line_start()` method; the line start comes from the `LineStart::Line(u64)` payload); Task 5's test name `duplicate_keys_collapse_last_wins_first_position` (the `document_` prefix is dropped, as it is a matcher-layer test).
11. **spec new-test list alignment** (Task 1 Step 5): delete the unowned `container_framing_bytes_are_not_charged` and `matcher_name_step_duplicate_reports_ambiguous_at_object_close`; the new list follows the test names actually delivered by Tasks 5/6.
12. **Count and reference corrections**: matcher.rs is 987 lines (2026-09-13 re-check; both 936 and 1018 are inaccurate, and the tree was still moving during the review — freeze the working tree and re-verify the counts before execution); framing.rs's LineFraming tests number 3; R4's "four existing exact assertions" becomes "three existing + one new in Task 6"; the Task 9 Step 1 deletion list is corrected per R5; the Task 4 interface block gets `MAX_DEPTH = 129`; Self-Review §3 is updated to the signatures after R2.3; Task 11's deviation range is ①–⑫; R8's "Task 12" has been added as a main-text task section.

### R10 · 2026-09-13 R9 landing re-check (re-taking evidence against the frozen tree `b20e7eb`)

Every resolution of R9 has been written into the main text one by one; this round **re-takes evidence by comparing R9's claims with the main text against the committed frozen tree `b20e7eb`** (that tree was still moving at review time, which is why R5's counts were off), and fills in in place the residue R9 did not cover. When it conflicts with R9, R10 prevails — R10 only fills in landing details and changes none of R9's resolutions.

1. **Counts re-taken at the freeze point**: all six of R5's counts (matcher 987 / framing 10 = 3+2+5 / document 21 / lines 26 / mod 20 / core 122) are re-verified on this trunk ✓; Task 5's `17 items / 17 passed` → **18** (counting the Step 1 code block item by item gives 18; the original text was leftover from the old draft); the Goal's Spec line ①–⑩ → ①–⑫; Task 9's `LineFraming::line_start()` → the `LineStart::Line(u64)` payload returned by `start_line()`; Self-Review §1's "depth gating" → absolute offset accounting (R3 already deleted the whole depth pipeline).
2. **Task 4 Step 4's LineFraming migration note changed to a "shape sketch"**: naming the existing field set (`inner: BufReader<R>` / `offset` / `span` / `eol` / `capped`, framing.rs:99-105, with `new` at :108) and the **type parameterization** (`inner: R`, with `BufReader` passed in by the caller — this is the precondition for Task 7's `Session<LineFraming<BufReader<R>>>` to hold), plus Task 7's **construction point** (`Session::new(LineFraming::new(BufReader::new(reader)), budget)`).
3. **The `run_err` R9.10 assigned to Task 7 Step 3 does not hold** (a new defect introduced by the rewrite): `json::testing::run_err` drives only the **engine**, and the reader's `Err` will panic inside it at `r.next().unwrap_or_else(|e| panic!(...))` (testing.rs:53) — yet what this case wants to observe is exactly the reader's Format error. Switch to the module's existing shape (`Lines::new` + `match r.next()`, the same shape as `empty_line_is_a_format_error`).
4. **Task 7 ⚠③'s decision branch closes the loop**: verified on the tree that `empty_line_is_a_format_error` (lines.rs:352) and `lines_whitespace_only_line_is_a_format_error` (lines.rs:433) **both assert only the `"json input"` prefix** → take the "use actson's text directly" branch, **writing no synthesis code**; ⚠⑧ is merged with it into the same item.
5. **Task 1 Step 5 made executable**: the spec's "new" list **does not contain** `document_single_root_under_cap_yields_all_matches` / `document_single_root_over_cap_errors` (they appear only in the Amendment of the "memory high-water-mark gate" item), and per R1 deviation ③ they are an **amendment, not a retirement → both are retained**; what really needs retiring is `element_barely_under_cap_is_accepted` and `budget_is_charged_per_root_not_accumulated` (R5, with coverage taken over by the three handler-layer tests); §acceptance criteria's exemption list is completed; in Step 2 the spec main text's empty-input message switches to actson's text (`json input: nothing more to parse`).
6. **Two spots in the Task 6 draft scaffolding**: `from_segments(&from.segments, crate::json::Type::Document)` → one argument (and written into the signature clause of Task 6 ⚠, to prevent the implementer from "adding the parameter back"); `Document::new`'s struct literal gains `failed: None` (the field is declared, and the scaffolding missed its initialization).
7. **Task 8 Step 3 adds the item-by-item disposition of the module docs' ①–⑥ deviation list**: ①②④⑤⑥ are retained, and **③ must be rewritten** ("a single-value root accumulates the match set by pull, backstopped by the cap" is an old-kernel fact; under the new kernel hit values are enqueued one by one at container close and delivered in batches, and `pending` is independent of the root's size, R9.2).
8. **Task 3 ⚠⑧ new**: the draft's `Err(kind == Interrupted) => return None` **does not set `done`** — a transient EINTR is naturally retried by `NeedMoreInput → drive's continue`, but a persistently `Interrupted` lower layer just spins; at implementation time retry inside `next_input` following the `BufReader` precedent, and put the tradeoff into the report.
9. **Global Constraints' "do not touch a single line" exemption list completed to four categories**: deviation ⑧ (two expectations), deviation ⑤-test (three reserved-key tests + `NUMBER_TOKEN`), R5's two matcher budget tests + the whole `framing.rs` file, and the three R9 items (`{} x` flips to true / the two `pending_high` read paths / `\uD800`→`\uDC00`).
10. **actson facts re-taken a second time (tag 2.1.0's `src/parser.rs`, independently re-checked this round)**: `has_input` has no call sites at all ✓; the stack is bottomed with `MODE_DONE` and `push()` checks `stack.len() >= depth` (so `with_max_depth(129)` allows exactly 128 levels) ✓; `current_str()` = `from_utf8(current_buffer)` (UTF-8 validation deferred to here) ✓; `reset()` does contain `self.parsed_bytes = 0;` ✓; **the ordering of the two sides at a terminator** — a legal terminator **emits the value/close event first and then reports the error**, whereas a number followed by an illegal terminating byte **emits no value event and directly returns `Err(SyntaxError)`**: this is exactly the mechanism by which `{} x`'s expectation flips to true while `5x`/`123#` stay false, and it has been written into the facts section and R9.4.

### R11 · 2026-09-13 second review round (omissions / conflicts / performance / safety; three user resolutions)

Checked item by item against the frozen tree `b20e7eb` + the actson 2.1.0 source, and the following problems were found and fixed in place. **When it conflicts with R0–R10, R11 prevails** (R11.1 is a **supplement** to R9.3, not an overturn: the criterion and the disarm timing are unchanged, and only the scope is written down firmly).

1. **The budget's arm/disarm scope = one record root, not one poll** (must fix): R9.2's `StopRoot` batch semantics let **a single-value root span multiple polls** (`SELECT * FROM S3Object[*].a[*]` delivers a batch as soon as each scalar is hit). If `arm()/disarm()` were called per poll (the Task 6 scaffolding is exactly that), `charged/refunded/fired` would be cleared each round → the existing test **`document_single_root_over_cap_errors` would necessarily go red** (a 1.4 MB root with 200,000 hits, charging only a dozen bytes per poll, so the cap never fires → loops to `Ok(None)` → the test's `panic!`), and deviation ①'s "a single object/scalar root > 1 MB still errors" would fail at the same time. ⇒ R3/R9.3 have already stated the scope; **Task 6's per-poll `self.budget.arm()/disarm()` calls are a semantic error in the scaffolding** (not just a compile error), and the criterion is that test.
2. **Inline full paths would turn Task 11's clippy leg red** (must fix): this repo's `docs/style.md` says "Top `use`; **never inline**" plus `crates/tinio-select/Cargo.toml`'s `[lints.clippy] absolute_paths = "warn"`, while Task 11 runs `cargo clippy --workspace --all-targets -- -D warnings` (test targets are equally guilty). Instances in the plan's code blocks: `std::io::ErrorKind::Interrupted` (Task 3), `Session<R: std::io::Read>` (Task 4), `std::cell::Cell` (Task 5), `std::rc::Rc`/`std::mem::{take,replace,swap}` (Task 6/7), and `impl std::io::Read`/`std::io::Result`/`std::io::Error::other` in tests (Task 3). ⇒ Global Constraints has gained one hard requirement.
3. **`Read` errors are no longer swallowed into a clean EOF** (user resolution; a behavioral regression): the old kernel threw io errors as `Error::from(IoError)` via `json_error`, and the new kernel's `Err(_) => { done = true; None }` would downgrade them — in root-array/line mode this mostly shows up as a misleading `Format` message, while **for a DOCUMENT single-value root an io failure in the "root already closed, trailing probe" window shows up as success with silent truncation** (a data-plane fidelity problem). ⇒ `StreamFeeder` records `io_error`, and `Session::map_err` and both handlers' termination paths report it with priority (the cap decision takes priority over io errors).
4. **`key_matches` must be reused** (should fix): the current matcher uses `json::value::key_matches(.., NameStyle::Bare)` (matcher.rs:307), and value.rs's docs state that "this is the only comparison shared by the select side and the traversal side, and the very existence of review 2026-09-06b R10 is to prevent the two from drifting apart". Task 5's original text says only "case folding", and an implementer might inline `to_lowercase()` (Unicode folding) and fork from `json_lookup`'s ASCII folding. ⇒ Task 5's Interfaces and semantics now name that function.
5. **Add clamping to `Budget::refund`** (should fix, defensive): in `charge()`, `charged - refunded` is a bare subtraction, and `refund()` accumulates unconditionally; once the refund window covers outside this span due to an implementation deviation (bytes already refunded by the previous span), you get an integer underflow panic in debug and a huge value in release → an immediate false cap report. The review could not construct a reachable path (the arm point exactly equals the end of the previous event's token, so the window ⊆ this span), hence defensive treatment: clamp `refund` to `charged` + `debug_assert!`.
6. **Literal conflict between Global Constraints and Task 10 Step 3** (should fix): the original text says "`record.rs` zero changes", while Task 10 Step 3 changes exactly `record.rs`'s `MAX_RECORD` doc comment. ⇒ An exception note has been added.
7. **Adversarial input coverage** (user resolution: table-driven, no new dependencies): the rewrite hands parsing to a home-grown state machine and carries two panics (`verbatim_number`'s `expect`, `Cap::on`'s `unreachable!`), while S3 Select parses **untrusted object bytes**; the repo has zero adversarial coverage of the json reader (the e2e select scenarios are all well-formed small inputs). ⇒ Task 10's new Step 6: a fixed input table, asserting "it can only produce records or `Format`/io errors, and never panics".
8. **The Task 9 disposition table is missing `record.rs`'s two tests** (omission): `document_start_above_zero_yields_nothing` (record.rs:360) and `document_scan_range_filters_elements_by_start` (:382) directly pin the element-level offset (11), making them the third assertion of deviation ⑥ within the crate, and they are not among the `document_*` (21) tests. ⇒ A row has been added to the table.
9. **No throughput threshold for LINES** (user resolution): R6's threshold applies only to `json_document_scan`; the old plan likewise has only the DOCUMENT baseline, and LINES has no serde-version number to compare against. ⇒ R6 has stated this, and LINES is checked only as "no obvious regression" in Task 11's manual checklist.
10. **Deviation coupling to the actson version** (omission): ⑨⑩⑪ and "EOF flushes unterminated tokens" etc. are all behaviors "measured against 2.1.0", while the workspace writes `actson = "2.1"` (caret); `Cargo.lock` pins 2.1.0, and `cargo update` can drift. ⇒ A sentence has been added to the facts section: upgrading actson requires re-verifying this group of deviations.
11. **`StreamFeeder::capped` stickiness** (should fix): `restart()` clears only `done`, while `Budget::fired` is cleared by `arm()`, so the two are semantically inconsistent; for now this is caught by "an error is a terminal state", but any call site that "keeps reading after an error" would misreport a subsequent error as CAP_MESSAGE. ⇒ `restart()` clears `capped` and `io_error` along with it.

---

---

## Global Constraints

- **No git write operations whatsoever** (CLAUDE.md overrides the skill's commit step): every task closes with "Checkpoint: report the list of files pending commit", and **does not execute** `git add/commit/push`.
- Code, comments, tests, and commit messages are always English. This plan and its spec were originally written in Chinese under an explicit user exemption; both were translated to English on 2026-09-15 at the user's request, which ends that exemption.
- **The only permitted dependency change** is `crates/tinio-select/Cargo.toml` adding `actson = { workspace = true }` and the root `Cargo.toml` adding `actson = "2.1"` (workspace dependencies are declared centrally, see `docs/cargo.md`). `serde.workspace = true` is already in place in the working tree, keep it (after the rewrite `serde` has no direct usage site, left for later cleanup — no lint gate). **Forbidden**: vendoring any actson module into the repo, and patching crates.io.
- `unsafe_code = "forbid"` is a per-crate setting (`crates/tinio-select/Cargo.toml`): actson is pure safe Rust (no `unsafe`), and this crate adds none either.
- The signature of `json::Reader::new(reader, params, from)`, `RecordReader::{next,last_record_start}`, `Record::Json(Option<serde_json::Value>)`, and the paths and semantics of `json::{to_value, json_lookup}` are **all unchanged**; `engine.rs`/`events.rs`/`tinio-server` have zero changes; in `record.rs` only the doc comment on `MAX_RECORD` may be touched (Task 10 Step 3), with zero code changes.
- **Import style is a hard constraint, not a suggestion**: `docs/style.md` "Top `use`; **never inline**" + `crates/tinio-select/Cargo.toml`'s `[lints.clippy] absolute_paths = "warn"`, and Task 11 closes it out with `cargo clippy --workspace --all-targets -- -D warnings` (**test targets are equally at fault**). The inline fully-qualified paths written in this plan's code blocks to save space (`std::io::ErrorKind::Interrupted`, `Session<R: std::io::Read>`, `std::cell::Cell`, `std::rc::Rc`, `std::mem::{take,replace,swap}`, `impl std::io::Read`/`std::io::Result`/`std::io::Error::other` in tests) **must be lifted to a top-level `use` at implementation time**, otherwise the clippy leg goes red immediately (R11.2).
- Memory discipline: **at no point may a whole document or a whole line be held**. The only things allowed to be held are: `StreamFeeder`'s 64 KiB read buffer, actson's current token (`current_buffer`, the length of the string/number literal), the **single already-matched value** the matcher buffers for "final Name step deferred emission" (≤ 1 MB, bounded, released immediately when the object closes), the current batch of records pending delivery in `rows`/`pending` (StopRoot batch semantics, R9.2), and the materialized current record.
- Existing tests: apart from the following **exemptions**, **do not touch a single line** — ① "behavior differences" item 4 (deviation ⑧: the two expectations in `raw_number_passthrough_no_decimal_parse` change to `1e309`); ② item 5 (three serde reserved-key tests deleted + the `NUMBER_TOKEN` constant); ③ R5's two matcher-layer budget tests retired (`element_barely_under_cap_is_accepted`, `budget_is_charged_per_root_not_accumulated`) + the whole `framing.rs` file deleted along with them (its 3 `LineFraming` tests move to `stream.rs`); ④ what R9 names (one `{} x` expectation flips to true, two `pending_high` read paths change to `r.pending_high()`, one input `\uD800`→`\uDC00`). **An existing assertion that has gone red = an implementation bug, not a reason to change the test.**
- Documentation style (`docs/style.md`): compressed prose, imports de-qualified at 3+ segments, errors via a per-crate `Error`, newtype/derive conventions.
- **Implementation discipline**: interfaces, tests, and semantics follow this plan and the spec; inline implementation bodies may be adjusted under compiler constraints, but every adjustment must be reported item by item in the execution report.

### Verified external facts (2026-09-13 three-way verification + point-by-point review of actson 2.1.0 `src/parser.rs`; may be cited directly at execution time, no need to re-check)

- `actson 2.1.0` (MIT, upstream `github.com/michel-kraemer/actson-rs`, source 100% safe Rust): `JsonParser::next_event() -> Result<Option<JsonEvent>, ParserError>`; the `JsonEvent` variants `NeedMoreInput / StartObject / EndObject / StartArray / EndArray / FieldName / ValueString / ValueInt / ValueFloat / ValueTrue / ValueFalse / ValueNull` (`Copy + Clone + Debug + PartialEq + Eq`, no payload). Clean end = `Ok(None)` — **only after a complete top-level value**; empty input and whitespace-only input are `Err(NoMoreInput)` (R9.1, empirically confirmed); unclosed = `Err(NoMoreInput)`; needs more input = `Ok(Some(NeedMoreInput))`; feeding again after `Ok(None)` without `reset()` → `SyntaxError`/`NoMoreInput` (LINES must reset on every line).
- `JsonFeeder` (`actson::feeder`): `fn has_input(&self) -> bool`, `fn is_done(&self) -> bool`, `fn next_input(&mut self) -> Option<u8>`. **The parser never calls `has_input()`** (`parser.rs` has no call site); when `next_input() == None` it only branches on `is_done()` — `done` → wrap up or `Err(NoMoreInput)`, not done → `Ok(Some(NeedMoreInput))`. `BufReaderJsonFeeder`/`PushJsonFeeder`/`SliceJsonFeeder` are ready-made implementations (this plan **does not use** them, for the reasoning see Task 3).
- `JsonParser`: `feeder` is a **pub field**; `current_str(&self) -> Result<&str, InvalidStringValueError>` returns a UTF-8 view of the token buffer — for `ValueInt`/`ValueFloat` that buffer is **the verbatim input bytes** (`1e309`, integers beyond u64 are preserved as-is; `current_int()` fails on overflow, `current_float()` silently folds `1e309` into `inf` — none of them is used). `parsed_bytes()` is "the number of bytes already taken from the feeder", `put_back` only happens when `streaming = true` and will `-1` — this plan does not use streaming, so **`parsed_bytes() == StreamFeeder::byte_offset()`, and there is no peek**, offsets rely entirely on `StreamFeeder`'s own absolute accounting (R2.3).
- Strictness: an illegal byte outside a token → `IllegalInput(u8)`; a control character inside a string, an illegal `\uXXXX`, a **lone low** surrogate → `SyntaxError`; unclosed (stack non-empty at EOF) → `NoMoreInput`; all three map to `Error::Format("json input: …")`. **Timing on both sides at the terminator** (verified in `src/parser.rs`): a legal terminator emits the value/close event first and then errors (`{} x` can therefore have its trailing message synthesized by the handler, R9.4); a number followed by an illegal terminating byte (`5x`) **emits no value event** and goes straight to `SyntaxError`. **Escapes are decoded at parse time, but UTF-8 is not validated at parse time** — a string with illegal UTF-8 still emits `ValueString` normally, and fails only at `current_str()` (R2.5). **Two upstream leniencies (deviation ⑪)**: a lone **high** surrogate `"\uD834"` passes silently (leaving the literal 6 characters in the token); `"\uD834\u0041\uDD1E"` is accepted and yields garbage.
- **EOF flushes unterminated tokens**: an unterminated **number/keyword** is taken as a legal value at EOF (`12` → `ValueInt(12)`, `tru` → `ValueTrue`); an unclosed container/string reports `NoMoreInput` (`[{"a":` still errors ✓).
- `JsonParserOptionsBuilder::default().with_max_depth(n).build()` is the only construction path (`JsonParserOptions` fields are private); the defaults are `max_depth = 2048`, `streaming = false`. **The stack is bottomed out with `MODE_DONE` and the criterion is `stack.len() >= depth`, so `n` actually permits `n - 1` levels** — to preserve serde_json's 128-container threshold you must pass **129** (R2.2). `new_with_max_depth` has been deprecated since 1.1.0. `reset()` comes from the `Reset` trait and **zeroes `parsed_bytes`** (after R2.3 offsets rely entirely on StreamFeeder's own accounting, so no impact), and feeding cannot continue without `reset()` after `Ok(None)`.
- `actson` defaults to `streaming = false`: non-whitespace bytes appearing **after** a JSON text → `ParserError::SyntaxError` (top-level trailing garbage errors immediately, exactly what this crate needs); this plan **does not use** streaming mode (LINES's "one value per line" relies on framing to mark the lines, DOCUMENT relies on top-level array syntax).
- Pull: a direct connection to `https://static.crates.io/crates/actson/actson-2.1.0.crate` works (sha256 matches the index); locally `CARGO_HOME=E:\.cargo`, the TUNA sparse mirror works too. Dependencies `btoi 0.5`, `num-traits 0.2`, `thiserror 2.0` (the latter two are already in this crate's transitive closure).
- **License (the grounds for rejecting extraction, Q1 settled)**: MIT `(c) 2023 Michel Kraemer` **+** `LICENSE_JSON_checker`: `(c) 2005 JSON.org`, with **"The Software shall be used for Good, not Evil."** attached (non-OSI) — the state table and event parser are derived from `JSON_checker.c`, and that clause travels with the code. So this plan **depends on the crate; vendor / extraction / patch are forbidden** (extraction cost: 926 lines of core + 69 lines of a usable feeder + ~706 lines of upstream test migration and 283 fixtures).
- **Version coupling (added in the 2026-09-13 review)**: the behaviors listed above — EOF flushing unterminated tokens, escape leniency, non-streaming trailing-error timing (a legal terminator emits the event first / an illegal terminating byte does not), the `n - 1` levels caused by the `MODE_DONE` bottom, `reset()` zeroing `parsed_bytes` — are all **measured conclusions about actson 2.1.0**. The workspace writes a caret per in-repo style (`actson = "2.1"`), with the pin resting on `Cargo.lock`'s 2.1.0; **upgrading actson requires re-verifying this set of deviations and their corresponding tests**, do not assume behavior within 2.x stays unchanged (deviations ⑨⑩⑪ all rest on these details).
- `serde_json 1.0.151` (local vendored source `~\.cargo\registry\src\mirrors.tuna.tsinghua.edu.cn-4dc01642fd091eda\serde_json-1.0.151`): `Number::from_string_unchecked(String)` (`number.rs:341-347`) **exists and is `pub`**, but is `#[doc(hidden)]` + the comment "Not public API. Only tests use this."; under `preserve_order` objects are `IndexMap`, duplicate keys are last-wins and **keep the position of the first occurrence**. This plan **does not use** `from_string_unchecked` (hidden API, no guarantee across minor versions), and instead uses `serde_json::from_str::<Value>(token)` for an explicit transcription of "verbatim token → Number" (see Task 5 `verbatim_number`).

---

## File Structure

| File | Responsibility | Disposition |
| --- | --- | --- |
| `src/json/mod.rs` | module docs, `Type`/`Params`, the `Reader` façade (`Handler` dispatch), engine composition tests; the `pub(crate) use value::{json_lookup, to_value}` re-export | rewrite doc comments and handler construction, façade signature unchanged |
| `src/json/feed.rs` | `StreamFeeder<R>`: `JsonFeeder` implementation + 1 MB budget accounting + absolute byte offsets + **io failure retention** (`io_error`; no depth handle, deleted in R3) | **new** |
| `src/json/stream.rs` | `Session`: wraps `JsonParser<StreamFeeder<R>>`, `drive()` drives event by event and returns `Progress`, mapping `ParserError`/cap/io failures into `Error`; `LineFraming` (line width cap + line start) | **new** (`LineFraming` moves in from `framing.rs` and is simplified) |
| `src/json/matcher.rs` | `Step`/`CompiledPath` + `Matcher` (explicit-stack state machine: `Frame` + `Scope` + assembler), `VEvent` value events, MISSING/ambiguous/duplicate-key rules | **rewrite** (delete every serde seed/visitor) |
| `src/json/lines.rs` | the `Lines<R>` handler: line framing + one root per line | rewrite (structural simplification) |
| `src/json/document.rs` | the `Document<R>` handler: one parser session for the whole document, each top-level array element becomes a record | rewrite (everything `DocFraming`-related deleted) |
| `src/json/framing.rs` | — | **delete** (`BudgetedRead` merges into `feed.rs`; the whole `DocFraming` block is retired; `LineFraming` moves to `stream.rs`) |
| `src/json/core.rs` | — | **delete** (`Core::pull` is entirely serde kernel machinery; the shared state is taken over by handler/Matcher, `pending_high` moves to `Matcher`, R9.6) |
| `src/json/value.rs` | `to_value`/`json_lookup` | unchanged |
| `src/json/testing.rs` | test helpers `raw`/`run`/`run_err`/`s` | unchanged |
| `Cargo.toml` (root + tinio-select) | the `actson` dependency | one line added to each |
| `benches/select_scan.rs` | the `json_document_scan` baseline | doc comment updated for the new kernel |
| `docs/superpowers/specs/2026-09-12-json-streaming-design.md` | design authority | kernel section rewritten + deviations ①–⑫ |

---

### Task 1: Rewrite the design document onto the actson kernel (spec re-baseline)

**Files:**
- Modify: `docs/superpowers/specs/2026-09-12-json-streaming-design.md` (whole file)
- Test: none (a documentation task; acceptance rests on the tests of later tasks and the Task 11 cross-check)

**Interfaces:**
- Consumes: the full existing spec, and this plan's two sections "Why actson" and "Behavior differences".
- Produces: the spec section numbers and deviation numbers that all later tasks reference; §Parsing Kernel describes actson + `StreamFeeder`; §Framing Filter becomes `StreamFeeder`'s accounting rules; §PathSeg Compilation State Machine becomes the explicit-stack `Matcher`.

- [ ] **Step 1: Rewrite §Parsing Kernel**

Replace the whole section "everything goes through serde_json streaming deserialization…" with:

```markdown
## Parsing Kernel

The only parser is actson 2.1.0's JsonParser<StreamFeeder<R>>: reactive / non-blocking, pulling byte by byte.
StreamFeeder implements actson::feeder::JsonFeeder, taking from the underlying Read and serving it on demand when the parser asks for bytes,
so no whole-document or whole-record buffering exists: what the parser holds is only the current token (current_buffer),
and what the caller holds is only the current record.

Events are the matching input: StartObject / EndObject / StartArray / EndArray / FieldName / ValueString /
ValueInt / ValueFloat / ValueTrue / ValueFalse / ValueNull. The matcher is an explicit-stack state machine (no more
serde DeserializeSeed / Visitor / Deserializer), and an unmatched subtree is skipped whole by event depth (without materializing a value).

Number fidelity: current_str() returns the verbatim token for ValueInt / ValueFloat (actson's token buffer accumulates the arriving
bytes as-is, and only backslash-u form unicode escapes are rewritten in place). Integers beyond u64, and exponents like 1e309 that
overflow f64, are all preserved verbatim (current_int() fails on overflow, current_float() silently folds 1e309 into inf, so neither is used). The token is
transcribed into Value::Number via serde_json::from_str::<Value>, then to_value turns it into
RawNumber(n.to_string()) (under arbitrary_precision Display is the original token).

Depth cap: JsonParserOptionsBuilder::default().with_max_depth(MAX_DEPTH).build(),
MAX_DEPTH = 129 (actson's stack is bottomed out with MODE_DONE and the criterion is stack.len() >= depth, so n actually permits n - 1 levels;
to preserve serde_json from_slice's 128-container threshold you must pass 129; actson's default 2048 → 2047 levels).

Rejected alternatives: ① serde_json Deserializer::from_reader + DeserializeSeed (the old implementation) — IoRead
reads byte by byte and IgnoredAny's skip does not validate surrogate pairs/UTF-8, requiring a hand-written Skip, and the single-value-root match set accumulates per document;
② rill-json 0.5 — parse_streaming(&str) requires holding the entire input and hard-codes a 10 MB whole-string cap, the tokenizer has no
"come back later" state, JsonNumber drops the original token, JsonValue::Object is a BTreeMap that drops key order, and to use it in this design you would have to
vendor and modify the source; ③ serde SeqAccess + a worker thread + a channel — one thread per query, Send + 'static contagion, violating
the crate's synchronous pull model.
```

- [ ] **Step 2: Rewrite §Framing Filter as §Byte Serving and Accounting**

Replace with (keeping the old text's wording "invariant: one root ≤ 1 MB"):

```markdown
## Byte Serving and Accounting

StreamFeeder<R> is the only component that touches the underlying bytes, and every byte served does two things:

- Budget: the handler calls arm()/disarm() per **record root** — in array mode each element, in single-value-root mode the whole root value, and in LINES
  one line, each of which is a root; `Progress::StopRoot`'s batch delivery **does not disarm** (a single-value root + a deep wildcard makes the same root
  keep being fed across multiple polls, so the budget must accumulate the whole way, R11.1). Every byte served charges 1 (regardless of container depth),
  and at each event the handler uses the offset it computed itself to refund immediately the framing bytes served since the previous event (top-level [, ], , and
  top-level whitespace; **the basis for the first event is the arm point**); the criterion is "charged − refunded > MAX_RECORD"
  (no in-flight slack; disarm happens at the root's completion event — a scalar value event / the outermost container closing, R9.3). The invariant is still
  "one root ≤ 1 MB": [<exactly 1 MB element>] and a scalar root of exactly 1 MB both pass exactly at the limit (deviation ⑦);
  >1 MB of consecutive top-level whitespace triggers the cap before any refund opportunity (deviation ⑫, known-gap).
- Offset: bytes_served accumulates monotonically, and the record start is derived from the event + offset (containers take the opening bracket position, scalars see the handler) —
  in non-streaming mode actson has no put_back / peek, so there is no "parser looking back one byte".

LINES additionally has a line width cap: LineFraming serves only the current line's content bytes (excluding the terminator), end of line is EOF, and
after start_line() it serves the next line; the line width (content + terminator) is counted while serving, and exceeding 1 MB immediately reports
the lines_input_record_cap error. An empty line → the first byte is already EOF → no events at all → actson reports Err(NoMoreInput),
which via Session::map_err becomes Format("json input: nothing more to parse") (empty_line_is_a_format_error /
lines_whitespace_only_line_is_a_format_error both only assert the "json input" prefix, semantics unchanged — R9.1).
```

- [ ] **Step 3: Rewrite the runtime subsection of §PathSeg Compilation State Machine**

Replace the StepSeed-related paragraphs with:

```markdown
Runtime: the Matcher holds steps, frames (an explicit stack), failure. On each value event it calls
Matcher::on(ev, text, start) -> Result<Progress, Error>: a container whose hit count is 0 enqueues a single
None when it closes (MISSING short-circuit propagation); a step applied to a scalar means zero hits; the final Name step's matched value is buffered first, and is enqueued at object close only if there is no case-
folded duplicate, with a duplicate reporting Error::Ambiguous; a non-final Name reports an error immediately on discovering a duplicate key (tokens already produced before that
are not reclaimed — consistent with the existing contract that "data-related errors surface mid-stream"). Progress::StopRoot tells the handler "a batch of
deliverable records has been produced": the handler stops feeding events, hands this batch to RecordReader::next, and the next next() continues feeding the same root
— matching is incremental, and pending does not grow with the size of the root (the final Name step's row is enqueued only after the object closes, and its
StopRoot likewise appears only at the close event, see R9.2).
```

- [ ] **Step 4: Update §Semantic Preservation and Deviations**

**Execute per §Revisions R1 (not per the original text's "delete deviation ③ and renumber as ①–⑩")**: the spec's `1.`–`6.` original text and the existing Amended blocks are kept untouched, with one amendment appended to each of ③ and ⑥, and `7.`–`12.` appended at the end (⑦ the cap charge point, ⑧ number tokens not normalized, ⑨ error message, ⑩ depth cap, ⑪ actson escape leniency known-gap, ⑫ the cap window for >1 MB of consecutive top-level whitespace known-gap — R9.5).

- [ ] **Step 5: Update §Test Plan and §Acceptance Criteria**

- §Test Plan: **the "new" list does not contain** the two names `document_single_root_under_cap_yields_all_matches` / `document_single_root_over_cap_errors` — they appear only in the Amendment to the "memory peak gate" item, and per R1 deviation ③ it is an **appended amendment** (the two tests are **kept**, with their rationale changed from "backstopped by the cap" to "driven per event, inherently bounded"), so **the original text's "deviation ③ is retired so delete them" does not hold — do not delete them**. That Amendment adds two more points: the pin has moved to `Matcher` along with the deletion of `core.rs` (R7/R9.6); and a **generative large-document unit test** is added (Task 10 Step 5, R9.7).
- In the Amendment to §Test Plan's "matcher matrix" item, the two tests `element_barely_under_cap_is_accepted` and `budget_is_charged_per_root_not_accumulated` are **retired as matcher-layer budget tests** (the matcher does not touch the budget, R3/R5 — **no renaming**), and the coverage is taken over instead by three handler-layer tests: `document_input_record_cap_exact_allowed`, `document_element_exact_cap_inside_array_root_is_accepted`, `document_array_larger_than_1mb_streams`; the new names the original plan proposed, `container_framing_bytes_are_not_charged` and `matcher_name_step_duplicate_reports_ambiguous_at_object_close`, have no task delivering them, so they are deleted as well (R9.11).
- Append four entries to §Test Plan's "new" list (all of them test names actually delivered by Task 5/6): `duplicate_keys_collapse_last_wins_first_position`, `duplicate_key_at_final_name_step_is_ambiguous`, `duplicate_key_at_non_final_name_step_is_ambiguous`, `document_element_exact_cap_inside_array_root_is_accepted`.
- §Acceptance Criteria: change "all existing tests pass as-is except for the padding input of deviation ④'s two tests" to "all existing tests pass as-is except for deviation ④ (padding input), deviation ⑤-tests (three serde reserved-key tests deleted), deviation ⑧ (the two `1e309` expectations), and the exemptions R9 names (`{} x` expectation flips to true, the two `pending_high` read paths, `\uD800`→`\uDC00`, the two matcher budget tests retired)".
- Append one more entry to §Test Plan's "new" list: an **adversarial input table** (`document_adversarial_inputs_never_panic` / `lines_adversarial_inputs_never_panic`, Task 10 Step 7, R11.7) — after the rewrite the parser is a self-built state machine, and it needs a regression net asserting "any malformed input yields only records/`Format`/io errors, and never panics".
- §Error Handling is rewritten in full (the current text is still on the serde basis): parse errors are mapped uniformly by `Session::map_err`, with the decision order **cap (`feeder.capped()` → verbatim `CAP_MESSAGE`) → underlying `Read` failure (`StreamFeeder::io_error()` → `Error::from(io)`, the same shape as the old kernel `json_error`'s `is_io()` branch, R11.3) → the remaining actson `ParserError` → `Format("json input: {e}")`**; `Error::Ambiguous` and the engine-side `json_lookup` are unchanged.

- [ ] **Step 6: Checkpoint**

Report: `docs/superpowers/specs/2026-09-12-json-streaming-design.md` pending commit (along with the code changes from Task 2 onward). **Do not execute commit.**

---

### Task 2: Land the dependency (actson)

**Files:**
- Modify: `Cargo.toml:9-75` (the workspace.dependencies table, in alphabetical order)
- Modify: `crates/tinio-select/Cargo.toml:8-30` (the [dependencies] table, in alphabetical order)

**Interfaces:**
- Consumes: none.
- Produces: `actson` usable inside `crates/tinio-select`; from Task 3 on everything depends on it.

- [ ] **Step 1: Add the workspace dependency**

In the root `Cargo.toml`'s `[workspace.dependencies]` table, change the `arrow` line into two lines (that table is in alphabetical order, `actson` sorts before `arrow`):

```toml
actson = "2.1"
arrow = { version = "59", default-features = false }
```

- [ ] **Step 2: Add the crate dependency**

In `crates/tinio-select/Cargo.toml`'s `[dependencies]`, change the first line `arrow = { workspace = true, optional = true }` into two lines:

```toml
actson.workspace = true
arrow = { workspace = true, optional = true }
```

- [ ] **Step 3: Verify pull and compilation**

Run: `cargo fetch; cargo check -p tinio-select`
Expected: success; `Cargo.lock` gains `actson 2.1.0` and its dependencies `btoi`/`num-traits` (`thiserror` is already among this crate's dependencies).

If the local mirror cannot pull (SSL / SEC_E_NO_CREDENTIALS), **stop and report**, do not vendor, do not change the registry configuration — this is an environment problem, not part of this task.

- [ ] **Step 4: Checkpoint**

Report: `Cargo.toml`, `crates/tinio-select/Cargo.toml`, `Cargo.lock` pending commit. **Do not execute commit.**

---

### Task 3: `json/feed.rs` — `StreamFeeder` (byte supply + budget + offset)

> ⚠ **The code in this section has been corrected in place per §Revisions R2/R3/R9** (this task is also the only deliverable of R0 phase 0): **① no `depth` field/parameter** (the budget does not look at container depth, R3); **② no `value_start`** (actson has no peek, R2.3), see R4 for the offset mechanism; **③ the `Budget` interface is `arm/disarm/fired/refund(n)`, `charge()` takes no argument and charges everything by default, and the criterion `charged − refunded > MAX_RECORD` has no slack** (R9.3); **④ `is_done()` returns `self.done`** (`done && !has_input()` would livelock, R2.1); **⑤ the tests number 7** (R3/R5/R9: the 4 old tests for depth and peek semantics have been deleted); **⑥ the dead method `serve()` is deleted and `inner()/inner_mut()` accessors are added** (Task 7 reaches LineFraming through them, R9.10); **⑦ a cap trip still goes `next_input() → None` + the `done` latch → the parser reports `NoMoreInput` → the handler maps CAP_MESSAGE**; **⑧ `Interrupted` does not equal a clean end of input** — in the draft `Err(kind == Interrupted) => return None` **does not set `done`**, the parser immediately reports `NeedMoreInput`, and `Session::drive`'s `continue` feeds and reads again: a transient EINTR is naturally retried along this path (acceptable), but if the layer below keeps returning `Interrupted` it is a spin. When implementing, follow the `BufReader` precedent and `loop`-retry `Interrupted` inside `next_input` (or add a bounded retry in `drive`'s `NeedMoreInput` branch); record any trade-off in the execution report. **⑨ `io_error` must be recorded and reported** (R11.3, settled by the user): the `JsonFeeder` signature cannot hand out an `io::Error`, so the failure must **stay in the feeder** (`io_error: Option<IoError>`), and be reported as `Error::from(io)` by `Session::map_err` and the two handlers' end paths — otherwise a read failure inside the "root has closed, trailing probe" window of a doc single-value root masquerades as EOF and the client gets a silently truncated success result. **⑩ `refund` clamps to `charged` + `debug_assert`** (R11.5: when the refund window goes out of bounds a bare subtraction would underflow); **`restart()` clears `capped` and `io_error` together** (R11.11: these two latches align with the `Budget::fired` basis, so a stale latch cannot mislabel a later error).

**Files:**
- Create: `crates/tinio-select/src/json/feed.rs`
- Test: `#[cfg(test)] mod tests` at the tail of the same file

**Interfaces:**
- Consumes: `crate::record::{CAP_MESSAGE, MAX_RECORD}`; `actson::feeder::JsonFeeder`, `actson::reset::Reset`.
- Produces (Tasks 4–7 depend on these):
  - `pub(crate) struct Budget` + `Default`, methods `arm(&self)`, `disarm(&self)`, `fired(&self) -> bool`, `refund(&self, n: u64)`
  - `pub(crate) struct StreamFeeder<R>`, implementing `JsonFeeder` and `Reset`
  - `StreamFeeder::new(inner: R, budget: Rc<Budget>) -> Self`
  - `StreamFeeder::restart(&mut self)` (clears the EOF latch **and the `capped`/`io_error` latches**, keeps unread bytes)
  - `StreamFeeder::byte_offset(&self) -> u64` (the absolute offset of the next unserved byte)
  - `StreamFeeder::capped(&self) -> bool`
  - `StreamFeeder::io_error(&self) -> Option<&io::Error>` — the underlying `Read` failure (R11.3); `Session` and the two handlers use it to re-judge a "clean end" as `Error::from(io)`

- [ ] **Step 1: Write the failing test**

At the tail of `json/feed.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor, Read},
        rc::Rc,
    };

    use actson::feeder::JsonFeeder;

    use super::{Budget, StreamFeeder};
    use crate::record::MAX_RECORD;

    fn feeder(bytes: &[u8]) -> (StreamFeeder<Cursor<Vec<u8>>>, Rc<Budget>) {
        let budget = Rc::new(Budget::default());
        (
            StreamFeeder::new(Cursor::new(bytes.to_vec()), Rc::clone(&budget)),
            budget,
        )
    }

    #[test]
    fn serves_bytes_in_order_then_eof() {
        let (mut f, _b) = feeder(b"ab");
        assert_eq!(f.next_input(), Some(b'a'));
        assert_eq!(f.next_input(), Some(b'b'));
        assert_eq!(f.next_input(), None);
        assert!(f.is_done());
    }

    #[test]
    fn full_charge_up_to_cap_is_allowed() {
        // Every served byte is charged while armed; exactly MAX_RECORD fits.
        let (mut f, budget) = feeder(&vec![b'x'; MAX_RECORD as usize]);
        budget.arm();
        let mut served = 0u64;
        while f.next_input().is_some() {
            served += 1;
        }
        assert_eq!(served, MAX_RECORD);
        assert!(!budget.fired());
    }

    #[test]
    fn first_byte_past_the_cap_fires() {
        let (mut f, budget) = feeder(&vec![b'x'; MAX_RECORD as usize + 1]);
        budget.arm();
        for _ in 0..MAX_RECORD {
            assert!(f.next_input().is_some());
        }
        assert_eq!(f.next_input(), None, "the byte past the cap is refused");
        assert!(budget.fired());
        assert!(f.capped());
    }

    #[test]
    fn refund_pulls_the_meter_back() {
        // The handler refunds framing bytes at every event: the meter is
        // charged - refunded, so refunded bytes are free again.
        let (mut f, budget) = feeder(&vec![b'x'; MAX_RECORD as usize + 1]);
        budget.arm();
        for _ in 0..100 {
            assert!(f.next_input().is_some());
        }
        budget.refund(100);
        for _ in 0..(MAX_RECORD - 100) {
            assert!(f.next_input().is_some());
        }
        assert!(!budget.fired());
        assert_eq!(
            f.next_input(),
            Some(b'x'),
            "one more byte fits after the refund"
        );
    }

    #[test]
    fn disarm_resets_the_budget() {
        let (mut f, budget) = feeder(&vec![b'x'; MAX_RECORD as usize + 1]);
        budget.arm();
        while f.next_input().is_some() {}
        assert!(budget.fired());
        budget.disarm();
        budget.arm();
        assert!(!budget.fired());
        assert!(f.next_input().is_some());
    }

    #[test]
    fn restart_keeps_unread_bytes() {
        let (mut f, _b) = feeder(b"abc");
        assert_eq!(f.next_input(), Some(b'a'));
        f.restart();
        assert_eq!(f.next_input(), Some(b'b'));
        assert_eq!(f.byte_offset(), 2);
    }

    #[test]
    fn read_error_ends_the_stream_and_is_kept() {
        struct Boom;
        impl Read for Boom {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("boom"))
            }
        }
        let mut f = StreamFeeder::new(Boom, Rc::new(Budget::default()));
        assert_eq!(f.next_input(), None);
        assert!(f.is_done());
        // Kept, not swallowed: the handler turns this into `Error::from(io)`
        // instead of reporting a clean — silently truncated — stream (R11.3).
        assert_eq!(f.io_error().map(ToString::to_string).as_deref(), Some("boom"));
    }
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select feed::`
Expected: compilation failure (`StreamFeeder`/`Budget` do not exist).

- [ ] **Step 3: Implement**

```rust
//! Byte supply for the actson parser: a `JsonFeeder` that pulls from a
//! `Read` only when the parser asks for a byte, charges the 1 MB root budget
//! as it serves, and keeps the absolute byte offset every record start is
//! measured against. Nothing here holds more than one 64 KiB read buffer:
//! there is no whole-document, whole-line or whole-root buffer anywhere in
//! the reader.

use std::{
    cell::Cell,
    io::{self, Error as IoError, Read},
    rc::Rc,
};

use actson::{feeder::JsonFeeder, reset::Reset};

use crate::record::MAX_RECORD;

/// Bytes pulled from the inner reader per refill: one read per 64 KiB
/// instead of one per byte. The parser still consumes them one at a time.
const BUFSZ: usize = 64 * 1024;

/// Per-root budget, armed by the handler before driving a root and disarmed
/// at the root's completion event. Every served byte is charged while armed;
/// the handler refunds framing bytes (top-level `[` / `]` / `,` and
/// top-level whitespace) as it processes each event, so the meter —
/// `charged - refunded` — tracks root content only, and a root trips it the
/// byte past `MAX_RECORD` (deviation 7; no in-flight slack, R9.3).
#[derive(Default)]
pub(crate) struct Budget {
    armed: Cell<bool>,
    charged: Cell<u64>,
    refunded: Cell<u64>,
    fired: Cell<bool>,
}

impl Budget {
    pub(crate) fn arm(&self) {
        self.armed.set(true);
        self.fired.set(false);
        self.charged.set(0);
        self.refunded.set(0);
    }

    pub(crate) fn disarm(&self) {
        self.armed.set(false);
    }

    /// The meter ran past `MAX_RECORD` inside the last armed span.
    pub(crate) fn fired(&self) -> bool {
        self.fired.get()
    }

    /// Hand back `n` framing bytes (the handler knows the offsets). Clamped to
    /// what was charged in this span: a refund window that ever reached outside
    /// the armed span must not wrap the meter — underflow panics in debug and
    /// produces a huge `charged - refunded` in release, which would fire the cap
    /// spuriously (R11.5).
    pub(crate) fn refund(&self, n: u64) {
        let refunded = (self.refunded.get() + n).min(self.charged.get());
        self.refunded.set(refunded);
    }

    /// Charge one byte; `false` means the budget is spent, so the feeder
    /// hands the parser `None` — a clean end of input, which the parser
    /// reports as `NoMoreInput` and the handler maps to
    /// `Error::Format(CAP_MESSAGE)`.
    fn charge(&self) -> bool {
        if !self.armed.get() {
            return true;
        }
        let charged = self.charged.get() + 1;
        self.charged.set(charged);
        debug_assert!(
            self.refunded.get() <= charged,
            "refunds never exceed charges in one armed span"
        );
        if charged - self.refunded.get() > MAX_RECORD {
            self.fired.set(true);
            return false;
        }
        true
    }
}

/// The actson feeder over any `Read`. `budget` is armed by the handler once per
/// **record root** and stays armed across every poll that root spans — a
/// `Progress::StopRoot` batch does not disarm it (R11.1). The feeder's only
/// other jobs are the 64 KiB refill and the absolute offset accounting every
/// record start derives from (R4).
pub(crate) struct StreamFeeder<R> {
    inner: R,
    buf: Box<[u8; BUFSZ]>,
    /// Index of the next unread byte in `buf`; `len` is what the last read
    /// returned.
    pos: usize,
    len: usize,
    /// Bytes served to the parser: the absolute offset of `buf[pos]`.
    served: u64,
    done: bool,
    capped: bool,
    /// The inner reader failed. The `JsonFeeder` shape cannot hand an
    /// `io::Error` to the parser, so the failure is parked here and the stream
    /// is ended; the handler must surface it before treating the end as clean
    /// (R11.3) — otherwise a mid-stream read failure is indistinguishable from
    /// EOF and the response is silently truncated.
    io_error: Option<IoError>,
    budget: Rc<Budget>,
}

impl<R: Read> StreamFeeder<R> {
    pub(crate) fn new(inner: R, budget: Rc<Budget>) -> Self {
        Self {
            inner,
            buf: Box::new([0; BUFSZ]),
            pos: 0,
            len: 0,
            served: 0,
            done: false,
            capped: false,
            io_error: None,
            budget,
        }
    }

    /// The wrapped reader — LINES reaches its `LineFraming` through these to
    /// call `start_line()` and read `capped()` (Task 7, R9.10).
    pub(crate) fn inner(&self) -> &R {
        &self.inner
    }

    pub(crate) fn inner_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Absolute offset of the next byte the parser has not consumed yet.
    pub(crate) fn byte_offset(&self) -> u64 {
        self.served
    }

    /// Forget the end-of-input latch so the next line can be served. Bytes
    /// already read but not yet consumed stay buffered: `LineFraming`
    /// guarantees a refill never crosses a line boundary. The cap and io-error
    /// latches are cleared too, so they line up with `Budget::arm` (which clears
    /// `fired`) instead of leaking a stale verdict into a later line (R11.11).
    pub(crate) fn restart(&mut self) {
        self.done = false;
        self.capped = false;
        self.io_error = None;
    }

    /// Whether the budget ran out while serving.
    pub(crate) fn capped(&self) -> bool {
        self.capped
    }

    /// The inner reader failed; the parser was told the input ended, so this is
    /// the only place the failure survives. `Session::map_err` and both handlers
    /// check it before accepting a clean end (R11.3).
    pub(crate) fn io_error(&self) -> Option<&IoError> {
        self.io_error.as_ref()
    }
}

impl<R: Read> JsonFeeder for StreamFeeder<R> {
    /// The parser never consults `has_input`; it only asks `is_done` after a
    /// `None` from `next_input`, so `is_done` must be plain `self.done` — the
    /// `&& !has_input()` form livelocks whenever the cap fires while buffered
    /// bytes remain (R2.1).
    fn has_input(&self) -> bool {
        self.pos < self.len
    }

    fn is_done(&self) -> bool {
        self.done
    }

    fn next_input(&mut self) -> Option<u8> {
        if !self.has_input() && !self.done {
            match self.inner.read(&mut *self.buf) {
                Ok(0) => {
                    self.done = true;
                    return None;
                }
                Ok(n) => {
                    self.len = n;
                    self.pos = 0;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => return None,
                Err(e) => {
                    // A read failure ends the stream — the trait has no way to
                    // hand the parser an `io::Error` — but it is *kept*, not
                    // discarded: the handler surfaces it as `Error::from(io)`
                    // before accepting the end as clean (R11.3).
                    self.io_error = Some(e);
                    self.done = true;
                    return None;
                }
            }
        }
        if !self.has_input() {
            return None;
        }
        let byte = self.buf[self.pos];
        if !self.budget.charge() {
            self.capped = true;
            self.done = true;
            return None;
        }
        self.pos += 1;
        self.served += 1;
        Some(byte)
    }
}

impl<R> Reset for StreamFeeder<R> {
    /// Only the end-of-input latch is cleared. The source is a stream: bytes
    /// already served are gone, and the budget / offset accounting must not
    /// restart when a LINES line ends.
    fn reset(&mut self) {
        self.done = false;
    }
}
```

(`CAP_MESSAGE` appears in this file only inside doc comments — if clippy reports an unused import, delete that `use`; the actual message is produced by the handler's `map_err`.)

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select feed::`
Expected: 7 passed.

- [ ] **Step 5: Checkpoint**

Report: `crates/tinio-select/src/json/feed.rs` pending commit. **Do not commit.**

---

### Task 4: `json/stream.rs` — `Session` (parser session + error mapping) and `LineFraming`

> ⚠ **This section's code has already been corrected in place per §Revisions R2/R3/R9**: **① the depth handle is deleted wholesale** — `Session::new(reader, budget)` does not create an `Rc<Cell<usize>>`, there is no `Session::depth()`, and no `depth_is_tracked_through_containers` test; **② the `Session::drive` callback takes two parameters** (R2.3: actson has no peek, the offset is accounted for by the handler itself from `feeder().byte_offset()`, R4); **③ `MAX_DEPTH = 129`** (R2.2); **④ the `NeedMoreInput` branch is kept**, and together with the corrected `is_done()`, the cap-triggering path goes through `Err(NoMoreInput)` → `map_err`'s CAP_MESSAGE; **⑤ the empty-input test has been rewritten** — actson reports `Err(NoMoreInput)` for empty input / whitespace-only input rather than `Ok(None)` (R9.1), and DOCUMENT's empty/whitespace distinction lives in the handler layer (Task 6 ⚠⑥); **⑥ `Session` gains `feeder_mut()`, `StreamFeeder` gains `inner()/inner_mut()`** (Task 7 calls `start_line()` via `session.feeder_mut().inner_mut()`, R9.10); **⑦ stream.rs needs `use actson::reset::Reset`** — `parser.reset()` comes from that trait and **zeroes `parsed_bytes`** (no impact after R2.3), LINES must reset on every line (Task 7 ⚠⑤); **⑧ LineFraming migration: `capped` goes from a pub(crate) field to a method; the line start is carried by the `LineStart::Line(u64)` payload, with no `line_start()` method; the migrated existing tests number 3**; **⑨ an io failure must be reported** (R11.3): the order of judgment in `map_err` is **cap → `io_error` → parser message**, and both handlers must also check `session.io_error()` first when `drive` returns `Progress::End` (or `Ok(None)`) — this is where the old kernel `json_error`'s `is_io()` branch lands in the new kernel, and missing it is the same as downgrading a read failure into a silent truncation.

**Files:**
- Create: `crates/tinio-select/src/json/stream.rs`
- Modify: `crates/tinio-select/src/json/framing.rs` (**only extract** `LineFraming` into stream.rs; this task does not touch framing.rs, Task 8 deletes it)
- Test: the `#[cfg(test)] mod tests` at the tail of `stream.rs`

**Interfaces:**
- Consumes: `feed::{Budget, StreamFeeder}`; `actson::{JsonEvent, JsonParser, options::JsonParserOptionsBuilder}`.
- Produces (relied on by Task 5–7):
  - `pub(crate) const MAX_DEPTH: usize = 129`
  - `pub(crate) enum Progress { Continue, StopRoot, End }`
  - `pub(crate) struct Session<R> { ... }`, with methods:
    - `Session::new(reader: R, budget: Rc<Budget>) -> Self`
    - `Session::drive(&mut self, on_event: impl FnMut(JsonEvent, &JsonParser<StreamFeeder<R>>) -> Result<Progress, Error>) -> Result<Progress, Error>` — drives one event at a time; **no `parsed_before` parameter** (R2.3: actson has no peek, the offset is accounted for by the handler itself from `feeder.byte_offset()`, see R4)
    - (the final shape dictated by borrow-checker limits, see the Step 3 comment)
    - `Session::feeder(&self) -> &StreamFeeder<R>`, `Session::feeder_mut(&mut self) -> &mut StreamFeeder<R>`, `Session::reset_for_next_root(&mut self)`
    - `Session::io_error(&self) -> Option<Error>` — a failure of the underlying `Read` (the mapping of `feed::StreamFeeder::io_error`: `IoError::new(kind, msg)` → `Error::from`, the same shape as the old kernel `json_error`'s `e.is_io()` branch); both `map_err` and the two handlers' end paths must check it first (R11.3)
  - `pub(crate) struct LineFraming<R>` (`Read`): `new`, `start_line() -> io::Result<LineStart>` (the line start is carried by the `LineStart::Line(u64)` payload, with no `line_start()` method), `capped() -> bool` (changed from a field to a method)
  - `pub(crate) enum LineStart { Eof, Line(u64) }`

- [ ] **Step 1: Write the failing tests (session layer)**

```rust
#[cfg(test)]
mod tests {
    use std::{io::Cursor, rc::Rc};

    use actson::JsonEvent;

    use super::{Progress, Session};
    use crate::{error::Error, json::feed::Budget, record::MAX_RECORD};

    /// Drive a whole document, collecting every (event, token text) pair.
    fn events(input: &str) -> Result<Vec<(JsonEvent, String)>, Error> {
        let budget = Rc::new(Budget::default());
        budget.arm();
        let mut session = Session::new(Cursor::new(input.as_bytes().to_vec()), budget);
        let mut out = Vec::new();
        loop {
            match session.drive(|ev, parser| {
                let text = match ev {
                    JsonEvent::FieldName
                    | JsonEvent::ValueString
                    | JsonEvent::ValueInt
                    | JsonEvent::ValueFloat => {
                        parser.current_str().unwrap_or_default().to_string()
                    }
                    _ => String::new(),
                };
                out.push((ev, text));
                Ok(Progress::Continue)
            })? {
                Progress::End => break,
                _ => {}
            }
        }
        Ok(out)
    }

    #[test]
    fn events_are_emitted_in_document_order() {
        let evs = events(r#"{"a":[1,2.5,"x",true,null]}"#).unwrap();
        let kinds: Vec<JsonEvent> = evs.iter().map(|(e, _)| *e).collect();
        assert_eq!(
            kinds,
            vec![
                JsonEvent::StartObject,
                JsonEvent::FieldName,
                JsonEvent::StartArray,
                JsonEvent::ValueInt,
                JsonEvent::ValueFloat,
                JsonEvent::ValueString,
                JsonEvent::ValueTrue,
                JsonEvent::ValueNull,
                JsonEvent::EndArray,
                JsonEvent::EndObject,
            ]
        );
        assert_eq!(evs[1].1, "a");
        assert_eq!(evs[3].1, "1");
        assert_eq!(evs[4].1, "2.5");
        assert_eq!(evs[5].1, "x");
    }

    #[test]
    fn numbers_keep_their_verbatim_token() {
        // Deviation 8: actson's token buffer is the input bytes, so the
        // arbitrary-precision contract holds for every shape serde_json used
        // to normalize (1e309) or overflow (u64).
        let evs = events(r#"[1e309,123456789012345678901234567890,-0.5]"#).unwrap();
        let tokens: Vec<&str> = evs
            .iter()
            .filter(|(e, _)| matches!(e, JsonEvent::ValueInt | JsonEvent::ValueFloat))
            .map(|(_, t)| t.as_str())
            .collect();
        assert_eq!(tokens, vec!["1e309", "123456789012345678901234567890", "-0.5"]);
    }

    #[test]
    fn escaped_field_name_is_decoded() {
        let evs = events(r#"{"R\u0075les":1}"#).unwrap();
        assert_eq!(evs[1].0, JsonEvent::FieldName);
        assert_eq!(evs[1].1, "Rules");
    }

    #[test]
    fn trailing_garbage_is_a_format_error() {
        let err = events("{} x").unwrap_err();
        assert!(matches!(err, Error::Format(m) if m.starts_with("json input:")));
    }

    #[test]
    fn unterminated_document_is_a_format_error() {
        let err = events("[{\"a\":").unwrap_err();
        assert!(matches!(err, Error::Format(m) if m.starts_with("json input:")));
    }

    #[test]
    fn empty_input_is_a_session_level_format_error() {
        // actson reports empty (and whitespace-only) input as NoMoreInput —
        // `Ok(None)` only ever follows a complete top-level value (R9.1).
        // The DOCUMENT handler turns the zero-bytes case into "no records".
        for input in ["", "   \n  "] {
            let err = events(input).unwrap_err();
            assert!(
                matches!(err, Error::Format(m) if m.starts_with("json input:")),
                "{input:?}: {err:?}"
            );
        }
    }

    #[test]
    fn cap_exhaustion_reports_the_verbatim_message() {
        let budget = Rc::new(Budget::default());
        budget.arm();
        let doc = format!(r#"["{}"]"#, "a".repeat(MAX_RECORD as usize + 1));
        let mut session = Session::new(Cursor::new(doc.into_bytes()), budget);
        let err = loop {
            match session.drive(|_ev, _parser| Ok(Progress::Continue)) {
                Ok(Progress::End) => panic!("expected a cap error"),
                Ok(_) => {}
                Err(e) => break e,
            }
        };
        assert!(matches!(err, Error::Format(m) if m == "input record exceeds 1 MB"));
    }
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select stream::`
Expected: compilation failure (`Session`/`Progress` do not exist).

- [ ] **Step 3: Implement Session**

```rust
//! The parser session: one actson `JsonParser` over one `StreamFeeder`, with
//! the error mapping every handler shares. A `Session` is the only place
//! that ever sees a `ParserError` — everything above it sees `crate::Error`.

use std::{
    io::{Error as IoError, Read},
    rc::Rc,
};

use actson::{JsonEvent, JsonParser, options::JsonParserOptionsBuilder, reset::Reset};

use crate::{
    error::Error,
    json::feed::{Budget, StreamFeeder},
    record::CAP_MESSAGE,
};

/// Nesting depth ceiling, chosen so the parser accepts the same 128 containers
/// the previous kernel did: actson seeds the stack with `MODE_DONE` and tests
/// `stack.len() >= depth`, so `with_max_depth(n)` allows only `n - 1` levels
/// (its own default 2048 means 2047).
pub(crate) const MAX_DEPTH: usize = 129;

/// What the caller wants after seeing an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    /// Feed me the next event.
    Continue,
    /// A deliverable batch of records is queued: stop pulling, the caller
    /// will resume the same session on its next poll (R9.2).
    StopRoot,
    /// The input ended cleanly.
    End,
}

pub(crate) struct Session<R: Read> {
    parser: JsonParser<StreamFeeder<R>>,
}

impl<R: Read> Session<R> {
    pub(crate) fn new(reader: R, budget: Rc<Budget>) -> Self {
        let feeder = StreamFeeder::new(reader, budget);
        let options = JsonParserOptionsBuilder::default()
            .with_max_depth(MAX_DEPTH)
            .build();
        Self {
            parser: JsonParser::new_with_options(feeder, options),
        }
    }

    pub(crate) fn feeder(&self) -> &StreamFeeder<R> {
        &self.parser.feeder
    }

    pub(crate) fn feeder_mut(&mut self) -> &mut StreamFeeder<R> {
        &mut self.parser.feeder
    }

    /// Continue the current root: pull events until `on_event` says stop, or
    /// the input ends. Each `on_event` receives the parser so it can read
    /// `current_str()` / `feeder.byte_offset()` while the token is still
    /// current (there is no peek to compensate for — R2.3).
    pub(crate) fn drive<F>(&mut self, mut on_event: F) -> Result<Progress, Error>
    where
        F: FnMut(JsonEvent, &JsonParser<StreamFeeder<R>>) -> Result<Progress, Error>,
    {
        loop {
            let event = match self.parser.next_event() {
                Ok(Some(event)) => event,
                Ok(None) => return Ok(Progress::End),
                Err(e) => return Err(self.map_err(e)),
            };
            match event {
                JsonEvent::NeedMoreInput => {
                    // The feeder blocks on `Read`, so a need for more input
                    // with a non-empty read buffer is impossible; with an
                    // empty one it means end of input.
                    if self.parser.feeder.is_done() {
                        return Ok(Progress::End);
                    }
                    continue;
                }
                event => match on_event(event, &self.parser)? {
                    Progress::Continue => {}
                    other => return Ok(other),
                },
            }
        }
    }

    /// Restart the parser for the next root: actson cannot be fed again after
    /// `Ok(None)` without this (it also zeroes `parser.parsed_bytes`, which
    /// nothing reads after R2.3). The feeder keeps its buffer and its
    /// offsets; only the JSON state machine is cleared.
    pub(crate) fn reset_for_next_root(&mut self) {
        self.parser.reset();
    }

    /// The underlying reader's failure, if it failed — rebuilt as a crate
    /// `Error` the way the previous kernel's `json_error` did for `e.is_io()`.
    /// Both handlers call this before accepting `Progress::End` / `Ok(None)` as
    /// a clean end, so a mid-stream read failure can never masquerade as a
    /// silently truncated but successful stream (R11.3).
    pub(crate) fn io_error(&self) -> Option<Error> {
        self.parser
            .feeder
            .io_error()
            .map(|e| Error::from(IoError::new(e.kind(), e.to_string())))
    }

    fn map_err(&self, e: actson::parser::ParserError) -> Error {
        if self.parser.feeder.capped() {
            return Error::Format(CAP_MESSAGE.into());
        }
        // Cap first (a deliberate limit), then a reader failure, then the
        // parser's own verdict.
        if let Some(io) = self.io_error() {
            return io;
        }
        Error::Format(format!("json input: {e}"))
    }
}
```

**Borrowing note (may be adjusted during implementation, but report it back in the report)**: the reason `on_event` receives a `&JsonParser` rather than a field of `&mut self` is so that the callback can read `current_str()` while the token is still "current". **There is no longer a `parsed_before`/`self.seen`**: actson has no put_back / peek in non-streaming mode (R2.3), and the offset is accounted for by the handler itself at the event layer using `feeder.byte_offset()` (R4).

- [ ] **Step 4: Migrate and simplify LineFraming**

Move `LineFraming` / `LineStart` from `framing.rs` into `stream.rs`, with two simplifications (the rest of the logic is kept verbatim):

1. Delete the `Budget` references related to `BudgetedRead` (at this layer `Budget` belongs only to `StreamFeeder`);
2. `capped` goes from a `pub(crate)` field to a `pub(crate)` method (the `Lines` handler needs to read it); the line start is already carried by the `LineStart::Line(u64)` payload, so no `line_start()` method is added.

```rust
/// Serves exactly one line's content bytes (terminator excluded), then EOF.
/// The handler calls `start_line()` to advance; the span cap (content +
/// terminator ≤ 1 MB) is enforced here, mid-serve, so nothing buffers a line.
/// Moved from `framing.rs` verbatim except for two shape changes: `inner` is
/// the caller's reader (the `BufReader` is supplied by `Lines::new`, not fixed
/// inside — that is what makes `Session<LineFraming<BufReader<R>>>` nest), and
/// `capped` is a method instead of a field.
pub(crate) struct LineFraming<R: Read> {
    inner: R,
    /// Absolute byte offset of the line about to be served.
    offset: u64,
    /// Content + terminator bytes counted while serving the current line.
    span: u64,
    /// The previous line is fully drained; `start_line` may advance.
    eol: bool,
    capped: bool,
}
```

(**Field names are verbatim identical to the existing implementation**: framing.rs:99-105's `struct LineFraming<R: Read> { inner: BufReader<R>, offset, span, eol, pub(crate) capped }` undergoes only two transformations — `inner` is parameterized by type (`BufReader` is passed in by the caller), and the `capped` field becomes a method of the same name; `new`'s parameter accordingly changes from `BufReader<R>` to `R` (formerly :108), and the rest of the `start_line`/`Read` logic is unchanged down to the character. The existing **3** `LineFraming` tests move along with the implementation into `stream.rs`'s test module: `lines_serve_content_then_eof_until_next_line`, `lines_blank_line_serves_immediate_eof_but_exists`, `lines_cap_fires_mid_serve_and_exact_span_allowed`; the two `budgeted_read_*` ones are retired along with `BudgetedRead` — their coverage is replaced by Task 3's feed tests; the 5 `DocFraming` ones are retired along with `framing.rs`.)

- [ ] **Step 5: Confirm green**

Run: `cargo test -p tinio-select stream::`
Expected: all 7 session tests + 3 LineFraming tests green (at this point `framing.rs` is still present and `json/mod.rs` does not yet declare the new modules — first add the two lines `pub(crate) mod feed;` and `pub(crate) mod stream;` to `json/mod.rs` so it compiles).

- [ ] **Step 6: Checkpoint**

Report: `crates/tinio-select/src/json/stream.rs`, `crates/tinio-select/src/json/mod.rs` (the two module declarations) are pending commit. **Do not run commit.**

---

### Task 5: `json/matcher.rs` — the event-driven explicit-stack matcher (the matcher body + the assembler)

**Files:**
- Rewrite: `crates/tinio-select/src/json/matcher.rs` (the existing **987** lines are replaced wholesale, count baseline `b20e7eb`, R5/R9.12)
- Test: end of the same file (the facade tests from the old test module that still hold are carried over one by one, see Task 9)

**Interfaces:**
- Consumes: `crate::{error::Error, path::PathSeg}`; `json::value::key_matches` (+ `row::NameStyle`); `serde_json::Value`; `actson::JsonEvent`.
- Produces (Task 6/7 depend on it):
  - `pub(crate) enum Step { Name(String), Index(usize), Wild }`
  - `pub(crate) struct CompiledPath`: `from_segments(&[PathSeg]) -> Self` (no `Type` parameter — both the current code and spec :46 Amendment have already deleted it, R9.10), `steps(&self) -> &[Step]`
  - `Progress` (`pub(crate) use crate::json::stream::Progress;` re-exported, the handler takes it from `matcher`)
  - `pub(crate) enum VEvent<'a> { StartArray, EndArray, StartObject, EndObject, Name(&'a str), Text(&'a str), Int(&'a str), Float(&'a str), Bool(bool), Null }`
  - `pub(crate) struct Matcher`: `new(path: CompiledPath) -> Self`, `on(&mut self, ev: VEvent<'_>, start: u64) -> Result<Progress, Error>`, `take_rows(&mut self) -> Vec<Option<Value>>`, `failure(&self) -> Option<&Error>`
  - `pub(crate) fn verbatim_number(token: &str) -> Value`
  - `#[cfg(test)] Matcher::pending_high(&self) -> usize` — the new home of the high-water mark pin (moved in from `core.rs`, `rows` is its only data source; Document/Lines each expose an accessor of the same name, R9.6)

**Semantics (implemented point by point, all from the spec, equivalent to the old `StepSeed`)**:
1. The step sequence drops the leading `Wild` (the implicit `S3Object[*]`).
2. A container closes and that scope's hit count is 0 → enqueue **one** `None` (MISSING).
3. A step applies to a scalar → zero hits (nothing is produced; MISSING is filled in when the container closes).
4. Each child value fanned out by `Wild` becomes a record of its own, in document DFS order; when `Wild` is over an object, **each member's rows are first accumulated into a group**, and at object close they are appended in one shot in member order (this prevents a child scope's MISSING from interleaving out of place with the rows of later members).
5. `Index(i)` recurses only into the i-th element, with the index counted in the container's actual element order; out of range → zero hits.
6. After a last-step `Name` hits, it is **not** enqueued immediately: the value for that key in this object is stashed first, and only once the whole object has been scanned and no second case-folded hit is confirmed is it enqueued; a second one found → `Error::Ambiguous(name)`.
7. After a non-last-step `Name` hits, recursion continues; a second case-folded hit in the same object → `Error::Ambiguous(name)` immediately (rows produced earlier are not reclaimed).
8. At each container close the row group is appended to the parent scope, and the parent scope likewise executes item 2 at its own close.
9. A record's start = the first-byte offset of the value event that produced that record (passed in by Task 6/7).

**Key comparison must reuse `json::value::key_matches(.., NameStyle::Bare)`** (i.e. `eq_ignore_ascii_case`) — it is **the only comparison shared** by the select-side `json_lookup` and the traversal side, and the value.rs docs state that "review 2026-09-06b R10 exists to prevent the two from drifting apart"; the current matcher uses it the same way (matcher.rs:307). **Do not inline `to_lowercase()`/`to_ascii_lowercase()`**: Unicode folding would diverge from `json_lookup`'s ASCII folding, giving oddities like "the path hits but the projection cannot find it" that reproduce only on non-ASCII keys (R11.4). The `['name']` bracket segment goes the same way as `.name` (mod.rs:146 Q13: bracket segments are never routed as quoted-exact), so `Step::Name` does not need to carry `NameStyle`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{CompiledPath, Matcher, Progress, VEvent, verbatim_number};
    use crate::{error::Error, path::PathSeg};

    fn compiled(segs: &[PathSeg]) -> CompiledPath {
        CompiledPath::from_segments(segs)
    }

    /// Drive one root value as a scripted event flow. Test-only: the handler
    /// is the real event pump, so matcher semantics can be pinned here
    /// without any reader.
    fn rows(segs: &[PathSeg], script: &[VEvent<'_>]) -> Result<Vec<Option<Value>>, Error> {
        let mut m = Matcher::new(compiled(segs));
        for (i, ev) in script.iter().enumerate() {
            m.on(*ev, i as u64)?;
        }
        Ok(m.take_rows())
    }

    #[test]
    fn empty_steps_capture_the_root_value() {
        let got = rows(
            &[PathSeg::Wild],
            &[VEvent::StartObject, VEvent::Name("a"), VEvent::Int("1"), VEvent::EndObject],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!({"a": 1}))]);
    }

    #[test]
    fn wild_over_array_fans_out_in_order() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Wild],
            &[
                VEvent::StartArray,
                VEvent::StartObject,
                VEvent::Name("x"),
                VEvent::Int("1"),
                VEvent::EndObject,
                VEvent::StartObject,
                VEvent::Name("x"),
                VEvent::Int("2"),
                VEvent::EndObject,
                VEvent::EndArray,
            ],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!({"x": 1})), Some(json!({"x": 2}))]);
    }

    #[test]
    fn empty_container_is_one_missing() {
        let got =
            rows(&[PathSeg::Wild, PathSeg::Wild], &[VEvent::StartArray, VEvent::EndArray]).unwrap();
        assert_eq!(got, vec![None]);
    }

    #[test]
    fn step_over_scalar_is_missing() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("x"), PathSeg::Wild],
            &[VEvent::StartObject, VEvent::Name("x"), VEvent::Int("5"), VEvent::EndObject],
        )
        .unwrap();
        assert_eq!(got, vec![None]);
    }

    #[test]
    fn index_out_of_range_is_missing() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("x"), PathSeg::Index(3)],
            &[
                VEvent::StartObject,
                VEvent::Name("x"),
                VEvent::StartArray,
                VEvent::Int("1"),
                VEvent::EndArray,
                VEvent::EndObject,
            ],
        )
        .unwrap();
        assert_eq!(got, vec![None]);
    }

    #[test]
    fn index_picks_the_nth_element() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("x"), PathSeg::Index(1), PathSeg::Name("y")],
            &[
                VEvent::StartObject,
                VEvent::Name("x"),
                VEvent::StartArray,
                VEvent::StartObject,
                VEvent::Name("y"),
                VEvent::Int("1"),
                VEvent::EndObject,
                VEvent::StartObject,
                VEvent::Name("y"),
                VEvent::Int("2"),
                VEvent::EndObject,
                VEvent::EndArray,
                VEvent::EndObject,
            ],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!(2))]);
    }

    #[test]
    fn name_step_is_case_insensitive() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("rules")],
            &[VEvent::StartObject, VEvent::Name("RULES"), VEvent::Int("7"), VEvent::EndObject],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!(7))]);
    }

    #[test]
    fn name_step_can_take_a_value_event() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("x")],
            &[VEvent::StartObject, VEvent::Name("x"), VEvent::Int("5"), VEvent::EndObject],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!(5))]);
    }

    #[test]
    fn wild_over_object_is_document_order() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("a"), PathSeg::Wild],
            &[
                VEvent::StartObject,
                VEvent::Name("a"),
                VEvent::StartObject,
                VEvent::Name("y"),
                VEvent::Int("9"),
                VEvent::Name("x"),
                VEvent::Int("1"),
                VEvent::EndObject,
                VEvent::EndObject,
            ],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!(9)), Some(json!(1))]);
    }

    #[test]
    fn wild_group_keeps_a_missing_in_its_own_slot() {
        // Member `a` matches nothing (MISSING), member `b` matches: the
        // MISSING must stay in `a`'s slot, not interleave with `b`.
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("o"), PathSeg::Wild, PathSeg::Name("k")],
            &[
                VEvent::StartObject,
                VEvent::Name("o"),
                VEvent::StartObject,
                VEvent::Name("a"),
                VEvent::Int("1"),
                VEvent::Name("b"),
                VEvent::StartObject,
                VEvent::Name("k"),
                VEvent::Int("2"),
                VEvent::EndObject,
                VEvent::EndObject,
                VEvent::EndObject,
            ],
        )
        .unwrap();
        assert_eq!(got, vec![None, Some(json!(2))]);
    }

    #[test]
    fn duplicate_key_at_final_name_step_is_ambiguous() {
        let err = rows(
            &[PathSeg::Wild, PathSeg::Name("id")],
            &[
                VEvent::StartObject,
                VEvent::Name("id"),
                VEvent::Int("1"),
                VEvent::Name("ID"),
                VEvent::Int("2"),
                VEvent::EndObject,
            ],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Ambiguous(name) if name == "id"));
    }

    #[test]
    fn duplicate_key_at_non_final_name_step_is_ambiguous() {
        let err = rows(
            &[PathSeg::Wild, PathSeg::Name("files"), PathSeg::Wild],
            &[
                VEvent::StartObject,
                VEvent::Name("files"),
                VEvent::Int("1"),
                VEvent::Name("FILES"),
                VEvent::Int("2"),
                VEvent::EndObject,
            ],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Ambiguous(name) if name == "files"));
    }

    #[test]
    fn object_with_no_match_is_one_missing() {
        let got = rows(
            &[PathSeg::Wild, PathSeg::Name("nope")],
            &[VEvent::StartObject, VEvent::Name("a"), VEvent::Int("1"), VEvent::EndObject],
        )
        .unwrap();
        assert_eq!(got, vec![None]);
    }

    #[test]
    fn duplicate_keys_collapse_last_wins_first_position() {
        // The record model is serde_json's preserve_order map: a byte-equal
        // duplicate key overwrites the value and keeps its original slot.
        let got = rows(
            &[PathSeg::Wild, PathSeg::Wild],
            &[
                VEvent::StartObject,
                VEvent::Name("x"),
                VEvent::Int("1"),
                VEvent::Name("y"),
                VEvent::Int("9"),
                VEvent::Name("x"),
                VEvent::Int("2"),
                VEvent::EndObject,
            ],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!({"x": 2, "y": 9}))]);
        let Some(Value::Object(map)) = got[0].as_ref() else {
            panic!("expected an object");
        };
        let keys: Vec<&str> = map.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["x", "y"], "first-occurrence position is kept");
    }

    #[test]
    fn number_tokens_are_preserved_verbatim() {
        // Deviation 8: the token is the input bytes, so a signless exponent
        // stays signless and a huge integer keeps every digit.
        let got = rows(&[PathSeg::Wild], &[VEvent::Float("1e309")]).unwrap();
        assert_eq!(got, vec![Some(verbatim_number("1e309"))]);
        assert_eq!(
            verbatim_number("1e309").to_string(),
            "1e309",
            "no e+ normalization"
        );
        let big = "123456789012345678901234567890";
        let got = rows(&[PathSeg::Wild], &[VEvent::Int(big)]).unwrap();
        assert_eq!(got, vec![Some(verbatim_number(big))]);
    }

    #[test]
    fn nested_containers_are_assembled() {
        let got = rows(
            &[PathSeg::Wild],
            &[
                VEvent::StartObject,
                VEvent::Name("a"),
                VEvent::StartArray,
                VEvent::StartArray,
                VEvent::Int("1"),
                VEvent::EndArray,
                VEvent::Text("two"),
                VEvent::Bool(true),
                VEvent::Null,
                VEvent::EndArray,
                VEvent::EndObject,
            ],
        )
        .unwrap();
        assert_eq!(got, vec![Some(json!({"a": [[1], "two", true, null]}))]);
    }

    #[test]
    fn take_rows_drains_only_once() {
        let mut m = Matcher::new(compiled(&[PathSeg::Wild]));
        m.on(VEvent::Int("1"), 0).unwrap();
        assert_eq!(m.take_rows(), vec![Some(json!(1))]);
        assert_eq!(m.take_rows(), Vec::<Option<Value>>::new());
    }

    #[test]
    fn stop_root_is_reported_when_a_batch_is_ready() {
        // StopRoot means "a deliverable batch is queued", not "the root is
        // complete" (R9.2): the final-name value is held for the case-fold
        // duplicate check, so the batch (and StopRoot) only appears at
        // object close.
        let mut m = Matcher::new(compiled(&[PathSeg::Wild, PathSeg::Name("x")]));
        m.on(VEvent::StartObject, 0).unwrap();
        m.on(VEvent::Name("x"), 1).unwrap();
        assert_eq!(m.on(VEvent::Int("5"), 2).unwrap(), Progress::Continue);
        assert_eq!(m.on(VEvent::EndObject, 3).unwrap(), Progress::StopRoot);
        assert_eq!(m.take_rows(), vec![Some(json!(5))]);
    }
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select matcher::`
Expected: compile failure (`Matcher`/`VEvent`/`verbatim_number` do not exist).

- [ ] **Step 3: Implement — module header, `VEvent`, `verbatim_number`, `CompiledPath`**

```rust
//! `PathSeg` compiled to a state machine and run over the actson event flow.
//! The matcher owns an explicit stack (no serde visitors, no recursion):
//! every container is a frame, every step level is a scope, and the only
//! values that ever materialize are the ones a final step matches. Unmatched
//! subtrees are skipped by depth accounting — not one byte is copied for
//! them.

use std::{collections::VecDeque, mem};

use serde_json::Value;

use crate::{error::Error, path::PathSeg};

pub(crate) use crate::json::stream::Progress;

/// One traversal step after the root `S3Object[*]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Name(String),
    Index(usize),
    Wild,
}

/// The FROM path, compiled once per reader. The leading `Wild` (the
/// implicit `S3Object[*]` root step) is the handler's job and dropped here.
pub(crate) struct CompiledPath {
    steps: Vec<Step>,
}

impl CompiledPath {
    pub(crate) fn from_segments(segments: &[PathSeg]) -> Self {
        let rest = if segments.first().is_some_and(|s| matches!(s, PathSeg::Wild)) {
            &segments[1..]
        } else {
            &[]
        };
        let steps = rest
            .iter()
            .map(|s| match s {
                PathSeg::Name(n) => Step::Name(n.clone()),
                PathSeg::Index(i) => Step::Index(*i),
                PathSeg::Wild => Step::Wild,
            })
            .collect();
        Self { steps }
    }

    pub(crate) fn steps(&self) -> &[Step] {
        &self.steps
    }
}

/// One JSON event with its token text already resolved: actson's
/// `current_str` borrow ends at the next `next_event` call, so the text is
/// handed to the matcher borrowed for exactly one `Matcher::on` call.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum VEvent<'a> {
    StartArray,
    EndArray,
    StartObject,
    EndObject,
    Name(&'a str),
    Text(&'a str),
    Int(&'a str),
    Float(&'a str),
    Bool(bool),
    Null,
}

/// A number token straight from the input. Parsed exactly once, by
/// `serde_json`: with `arbitrary_precision` the resulting `Number` holds the
/// token verbatim, and `Display` gives it back unchanged (deviation 8:
/// no `e+` normalization).
pub(crate) fn verbatim_number(token: &str) -> Value {
    serde_json::from_str::<Value>(token).expect("actson emits only valid JSON number tokens")
}
```

- [ ] **Step 4: Implement — the value assembler `Cap`**

`Cap` is driven only when "a value has been fully matched"; an unmatched subtree only gets depth accounting and not a single value is materialized.

```rust
/// Builds one `serde_json::Value` from the event flow. Driven only for a value
/// a final step already matched, so its memory is bounded by that value.
struct Cap {
    stack: Vec<CapC>,
    /// A field name arrives before its value; stashed until the value lands.
    pending_key: String,
}

enum CapC {
    Array(Vec<Value>),
    Object(serde_json::Map<String, Value>),
}

impl Cap {
    fn new() -> Self {
        Self {
            stack: Vec::new(),
            pending_key: String::new(),
        }
    }

    fn name(&mut self, name: &str) {
        self.pending_key.clear();
        self.pending_key.push_str(name);
    }

    /// Feed one event. The return value is only meaningful for a value that
    /// completes without any container above it (a scalar capture).
    fn on(&mut self, ev: VEvent<'_>) -> Option<Value> {
        match ev {
            VEvent::Int(tok) | VEvent::Float(tok) => Some(self.leaf(verbatim_number(tok))),
            VEvent::Text(s) => Some(self.leaf(Value::String(s.to_owned()))),
            VEvent::Bool(b) => Some(self.leaf(Value::Bool(b))),
            VEvent::Null => Some(self.leaf(Value::Null)),
            VEvent::StartObject => {
                self.stack.push(CapC::Object(serde_json::Map::new()));
                None
            }
            VEvent::StartArray => {
                self.stack.push(CapC::Array(Vec::new()));
                None
            }
            VEvent::EndObject => {
                let Some(CapC::Object(map)) = self.stack.pop() else {
                    unreachable!("EndObject always closes an object frame");
                };
                Some(self.leaf(Value::Object(map)))
            }
            VEvent::EndArray => {
                let Some(CapC::Array(items)) = self.stack.pop() else {
                    unreachable!("EndArray always closes an array frame");
                };
                Some(self.leaf(Value::Array(items)))
            }
            VEvent::Name(_) => unreachable!("a name is routed to Cap::name"),
        }
    }

    /// Attach a completed value to the innermost open container; with nothing
    /// open the value is the capture itself.
    fn leaf(&mut self, value: Value) -> Value {
        match self.stack.last_mut() {
            None => value,
            Some(CapC::Array(items)) => {
                items.push(value);
                Value::Null
            }
            Some(CapC::Object(map)) => {
                map.insert(mem::take(&mut self.pending_key), value);
                Value::Null
            }
        }
    }
}
```

- [ ] **Step 5: Implement — `Scope` / `Frame` / `Matcher`**

```rust
/// What one step level is doing inside its container.
enum Scope {
    /// The container itself is the record (`SELECT * FROM S3Object`).
    Value(Option<Cap>),
    /// `Name`: rows of the matched key accumulate here until the object
    /// closes — that is what makes the final-name ambiguity rule possible.
    Named { group: VecDeque<Option<Value>>, matched: bool },
    /// `Wild`: one group per child, appended at close so a child's MISSING
    /// stays in its own slot.
    Wild { group: VecDeque<Option<Value>> },
    /// `Index(i)` over an array.
    Indexed { target: usize, ord: usize },
}

/// One open container.
struct Frame {
    scope: Scope,
    /// Matches registered at this level (0 on close means one MISSING row).
    matched: bool,
    /// Case-folded key names already resolved at this `Name` level.
    seen: Vec<String>,
    /// Depth of an unmatched subtree currently being skipped.
    skip: usize,
}

pub(crate) struct Matcher {
    steps: Vec<Step>,
    /// The open containers, outermost first; empty between root values.
    frames: Vec<Frame>,
    /// Records ready for the handler, drained one poll at a time.
    rows: VecDeque<Option<Value>>,
    failure: Option<Error>,
    /// Test-only high-water mark of `rows`, in serialized `Value` bytes
    /// (the tripwire replacing the spec's counting allocator, which
    /// `unsafe_code = "forbid"` rules out) — migrated from `core.rs` (R9.6).
    #[cfg(test)]
    pending_high: std::cell::Cell<usize>,
}

impl Matcher {
    pub(crate) fn new(path: CompiledPath) -> Self {
        Self {
            steps: path.steps().to_vec(),
            frames: Vec::new(),
            rows: VecDeque::new(),
            failure: None,
        }
    }

    pub(crate) fn take_rows(&mut self) -> Vec<Option<Value>> {
        self.rows.drain(..).collect()
    }

    pub(crate) fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }

    /// The record start offset of the value the parser is about to report.
    /// Filled in by the handler in Task 6/7; the matcher stores it against
    /// the row it produces.
    pub(crate) fn on(&mut self, ev: VEvent<'_>, _start: u64) -> Result<Progress, Error> {
        // 1. If a skip is in progress, only `Start`/`End` accounting runs.
        // 2. Otherwise dispatch on the innermost frame against
        //    `steps[frames.len()]`: `Wild` fans out per child, `Name`
        //    resolves per field, `Index` counts elements, an exhausted step
        //    list means "capture this value".
        // 3. A container that closes with `!matched` pushes one `None`.
        // 4. When the step level has queued a deliverable batch (`rows`
        //    non-empty), return `Progress::StopRoot` — the handler drains it
        //    and resumes feeding the same root on its next poll (R9.2). A
        //    final-name row is only queued at object close (rule 6), so its
        //    `StopRoot` only appears there.
        todo!("Task 5 Step 5: implement the dispatch above")
    }
}
```

(The structure above is the **agreed shape**: fields may be merged or renamed when implementing, but five things must be preserved — each level's own row group, the `matched` count at each level, `Wild`'s "one slot per group", `Name`'s enqueue-at-close, and the ability to keep feeding events for the same root after `StopRoot`. `#[cfg(test)] pending_high` is updated at every site that enqueues into `rows` and in `take_rows()`. **Get the 18 tests from Step 1 green first before touching the handler**.)

- [ ] **Step 6: Confirm green**

Run: `cargo test -p tinio-select matcher::`
Expected: 18 passed (counting the code block in Step 1 item by item gives 18; the 17 in the original text is a leftover from an old draft).

- [ ] **Step 7: Checkpoint**

Report: `crates/tinio-select/src/json/matcher.rs` pending commit. **Do not execute commit.**

---

### Task 6: `json/document.rs` — the DOCUMENT handler (top-level array yields one record per element)

> ⚠ **The Step 3/4 code in this section is draft scaffolding and is rewritten as a whole block** (the original itself marks it `todo!()`, but the leftover helper names are equally fictional): the `self.matcher` field does not exist; `path.canonical()`, `Matcher::new(CompiledPath::new())`, `begin_root_element()`, `css_done()`, `finish_root()`, `last_row_start()` are all undefined; the double `mem::swap` on `matcher` is dead code. **Closure borrows**: inside `self.session.drive(|…| …)` you must not borrow `&self.session` again (the original's `event_start(&self.session, …)` conflicts with the mutable borrow) — take the values you need out before `drive`, or take them directly from the `&JsonParser` the `drive` callback hands you. **Depth**: delete every `depth.set/get` (R3); **offsets**: implement per R4, with the `event_start` helper's signature settled along with R4; **budget scope**: the scaffolding's `self.budget.arm(); … drive(); self.budget.disarm();` is called **per poll**, which is a **semantic error** (R11.1, not merely a compile-level scaffolding problem) — `StopRoot` hands over only one batch and the root is not yet complete, so `disarm` lets the next poll's `arm()` zero out `charged/refunded/fired`; with a single-value root plus a deep wildcard (`SELECT * FROM S3Object[*].a[*]`) the cap never fires, the existing `document_single_root_over_cap_errors` goes red outright, and deviation ① fails at the same time. The right shape is **arm once per record root, and disarm only at the root's completion event (a scalar value event / the outermost container's close)**: in array mode the "root" is the element (one arm/disarm per element), in single-value root mode it is the whole root value (stays armed across polls, including the `StopRoot` rounds triggered by a non-empty `rows`); **signature**: the scaffolding's `CompiledPath::from_segments(&from.segments, crate::json::Type::Document)` is a two-argument leftover (already changed in place to one argument inside the Step 3 block), and the signature settled by R9.10 must not re-add the `Type` argument. **The signature evolution follows the suggestion at the end of Step 4 below**: `Matcher::take_rows -> Vec<(Option<Value>, u64)>` (the record start travels along with the row), with Task 5's assertion and Task 7's call site changed in step — this is the one explicitly named signature evolution in this plan. **⑤ `vevent` returns `Result<VEvent<'a>, Error>`** — the `current_str()` errors of `FieldName`/`ValueString` must propagate (R2.5; number tokens go through `?` as well, for a uniform interface, R9.10); **⑥ empty/whitespace distinction**: when `drive` ends with `Err(NoMoreInput)` or `Progress::End` and there are zero events, `session.feeder().byte_offset() == 0` → `done` + `Ok(None)` (`empty_document_and_array_yield_nothing`), otherwise report `Format` (`document_whitespace_only_is_a_format_error`, with the actson message passing a prefix assertion) (R9.1); **⑦ trailing garbage synthesis**: after the root value closes completely (a single root's scalar/container close, or the root array's close), if the parser then reports an error → synthesize `Error::Format("json input: trailing characters after the JSON document")` (R9.4, `{} x` expected to flip true, see Task 9); **⑧ `pending_high`**: `Document` exposes `#[cfg(test)] fn pending_high(&self) -> usize` (via `self.matcher`), and when rewriting the test module change the two `r.core.pending_high.get()` sites in `document_array_root_keeps_pending_bounded` and `document_single_root_under_cap_yields_all_matches` to this path (thresholds unchanged, R9.6). **⑨ the end path checks the io failure first** (R11.3): when `drive` returns `Progress::End` (or `Ok(None)`) you must not treat it directly as a clean end — first `if let Some(e) = self.session.io_error() { return Err(self.store(e)); }`; the decision order in the `Err` branch is **cap (`budget.fired()`) → io (`session.io_error()`) → parser message**. Miss this one and a doc single-value root whose read fails inside the "root already closed, trailing probe" window masquerades as success and is silently truncated.

**Files:**
- Rewrite: `crates/tinio-select/src/json/document.rs`
- Test: at the end of the same file (the existing 21 tests, except the exceptions named in Task 9, kept case by case in semantics)

**Interfaces:**
- Consumes: `json::feed::Budget`, `json::stream::{LineStart, Session}` (DOCUMENT does not use LineFraming), `json::matcher::{CompiledPath, Matcher, Progress, VEvent}`, `crate::record::{CAP_MESSAGE, RecordReader}`, `crate::row::Record`.
- Produces: `pub struct Document<R: Read>`, `new(reader: R, from: &FromClause)`, `impl RecordReader`.
- New shared helpers (`pub(crate)` in this module, reused by Task 7):
  - `pub(crate) fn vevent<'a, R: Read>(ev: JsonEvent, parser: &'a JsonParser<StreamFeeder<R>>) -> Result<VEvent<'a>, Error>` (R9.10: propagate the `current_str` error)
  - `pub(crate) fn event_start<R: Read>(session: &Session<R>) -> u64` (redone per R4: containers take the opening bracket's position; scalars are derived by R4's mechanism)
  - `#[cfg(test)] Document::pending_high(&self) -> usize` (R9.6)

- [ ] **Step 1: Write the failing tests (new in this module, the existing 21 kept)**

```rust
    #[test]
    fn document_element_exact_cap_inside_array_root_is_accepted() {
        // Deviation 7: the top-level brackets and commas are framing, not
        // root content, so an element of exactly 1 MB fits.
        let elem = "a".repeat(1024 * 1024 - 2);
        let doc = format!("[\"{elem}\"]");
        assert_eq!(
            read("SELECT * FROM S3Object", &doc),
            vec![Record::Json(Some(json!(elem)))],
        );
    }

    #[test]
    fn document_root_array_of_objects_streams_one_row_per_element() {
        // The root shape is discovered from the event flow (no framing
        // sniff): StartArray at depth 0 means "one record per element".
        let recs = read(
            "SELECT * FROM S3Object",
            "  [ {\"a\": 1}, {\"b\": 2} ]  ",
        );
        assert_eq!(
            recs,
            vec![
                Record::Json(Some(json!({"a": 1}))),
                Record::Json(Some(json!({"b": 2}))),
            ]
        );
    }

    #[test]
    fn document_leading_whitespace_shifts_the_first_element_offset_only() {
        // The first element's start offset is its real byte offset, because
        // nothing is stripped or buffered on the way in.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(br#"  [{"a":1},{"a":2}]"#.to_vec()), &plan.from);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 3);
        assert!(r.next().unwrap().is_some());
        assert_eq!(r.last_record_start(), 11);
    }
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select document::`
Expected: compile failure (the new structure is not implemented yet).

- [ ] **Step 3: Implement**

```rust
//! DOCUMENT handler: one root value per record, except that a **root array**
//! yields one record per element (the `S3Object[*]` rule). The root shape is
//! discovered from actson's event flow — there is no framing layer and no
//! document buffer: the same parser session drives the whole document, and
//! records are produced as each element completes.
//!
//! Deviations this handler owns (spec 2026-09-12): (1) document size is
//! unbounded, the 1 MB cap binds one root value; (5) a malformation surfaces
//! after the records before it; (6) record offsets are real element offsets
//! and single-mode root values keep their own offset; (7) root bytes are
//! charged as served and framing is refunded per event — no in-flight
//! slack (R9.3).

use std::io::{Read, Error as IoError, ErrorKind};

use actson::{JsonEvent, JsonParser};
use serde_json::Value;

use crate::{
    error::Error,
    json::{
        feed::{Budget, StreamFeeder},
        matcher::{CompiledPath, Matcher, Progress, VEvent},
        stream::Session,
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

pub struct Document<R: Read> {
    session: Session<R>,
    budget: std::rc::Rc<Budget>,
    path: CompiledPath,
    matcher: Matcher,
    pending: Vec<Option<Value>>,
    last_record_start: u64,
    done: bool,
    /// Set once an error was surfaced; every later poll repeats it
    /// (document_error_is_terminal_for_later_polls).
    failed: Option<Error>,
}

impl<R: Read> Document<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        let budget = std::rc::Rc::new(Budget::default());
        Self {
            session: Session::new(reader, std::rc::Rc::clone(&budget)),
            budget,
            path: CompiledPath::from_segments(&from.segments),
            pending: Vec::new(),
            last_record_start: 0,
            done: false,
            failed: None,
        }
    }

    fn emit(&mut self, rows: Vec<Option<Value>>, start: u64) -> Option<Record> {
        self.last_record_start = start;
        self.pending = rows;
        self.pending
            .drain(..)
            .next()
            .map(Record::Json)
    }
}

impl<R: Read> RecordReader for Document<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        if let Some(record) = self.emit(std::mem::take(&mut self.pending), self.last_record_start) {
            return Ok(Some(record));
        }
        if self.done {
            return Ok(None);
        }

        todo!("Task 6 Step 4: the pumping loop")
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}
```

- [ ] **Step 4: Implement — the pumping loop (replaces the entire block containing the `todo!` above)**

```rust
    fn next(&mut self) -> Result<Option<Record>, Error> {
        // A record the previous poll did not drain comes first.
        if let Some(record) = self.pending.pop() {
            return Ok(Some(record));
        }
        if self.done {
            return Ok(None);
        }

        let mut matcher = std::mem::replace(&mut self.matcher, Matcher::new(CompiledPath::new()));
        // (the matcher lives in `self` across polls; `mem::replace` only
        //  avoids a double borrow while `self.session` is used below.)

        let path = self.path.canonical();
        let mut matcher = Matcher::new(path);
        std::mem::swap(&mut matcher, &mut self.matcher);

        let depth = self.session.depth();
        let mut root_array: Option<bool> = None;
        let mut values: u64 = 0;
        let mut value_start: u64 = 0;
        let mut rows: Vec<Option<Value>> = Vec::new();
        let mut last_start: u64 = 0;
        let mut complete = false;
        let mut seen_any = false;

        self.budget.arm();
        let outcome = self.session.drive(|ev, parser, parsed_before| {
            let start = event_start(&self.session, parser, parsed_before);
            let vev = vevent(ev, parser);
            match vev {
                VEvent::StartArray if depth.get() == 0 => {
                    root_array = Some(true);
                    value_start = start;
                    depth.set(1);
                    Ok(Progress::Continue)
                }
                VEvent::StartObject if depth.get() == 0 => {
                    root_array = Some(false);
                    value_start = start;
                    depth.set(1);
                    let p = self.matcher.on(vev, start)?;
                    seen_any = true;
                    Ok(p)
                }
                VEvent::StartArray | VEvent::StartObject => {
                    depth.set(depth.get() + 1);
                    if depth.get() == 1 {
                        value_start = start;
                        values += 1;
                    }
                    let p = if root_array == Some(true) && depth.get() == 1 {
                        self.matcher.begin_root_element(start)
                    } else {
                        self.matcher.on(vev, start)?
                    };
                    Ok(p)
                }
                VEvent::EndArray | VEvent::EndObject => {
                    let p = if depth.get() == 1 {
                        seen_any = true;
                        let p = self.matcher.on(vev, start)?;
                        rows = self.matcher.take_rows();
                        if !rows.is_empty() || p == Progress::StopRoot {
                            last_start = self.matcher.last_row_start();
                        }
                        complete = p == Progress::StopRoot;
                        p
                    } else {
                        depth.set(depth.get() - 1);
                        self.matcher.on(vev, start)?
                    };
                    Ok(p)
                }
                scalar => {
                    if depth.get() == 0 {
                        // single-mode scalar root
                        root_array = Some(false);
                        value_start = start;
                        depth.set(1);
                    } else if root_array == Some(true) && depth.get() == 1 {
                        values += 1;
                        value_start = start;
                    }
                    let p = self.matcher.on(scalar, start)?;
                    seen_any = true;
                    if p == Progress::StopRoot {
                        rows = self.matcher.take_rows();
                        last_start = self.matcher.last_row_start();
                        complete = true;
                    }
                    Ok(p)
                }
            }
        });
        self.budget.disarm();

        let progress = match outcome {
            Ok(r) => r,
            Err(e) => {
                if self.budget.fired() {
                    return Err(Error::Format(CAP_MESSAGE.into()));
                }
                return Err(e);
            }
        };

        if !rows.is_empty() {
            self.pending = rows;
            self.pending.reverse();
            self.last_record_start = last_start;
            return Ok(self.pending.pop().map(Record::Json));
        }

        if progress == Progress::End || complete {
            self.done = true;
            self.matcher.finish_root();
            let zero_roots = values == 0 && !seen_any;
            if zero_roots {
                return Ok(None);
            }
            if !seen_any {
                return Ok(None);
            }
            let zero_match = values == 0 && rows.is_empty() && self.matcher.css_done();
            if zero_match {
                self.last_record_start = 0;
                return Ok(Some(Record::Json(None)));
            }
            return Ok(None);
        }

        Ok(None)
    }
```

todo!` `zero_roots`/`zero_match`/`canonical()`/`last_row_start()`/`begin_root_element()`/`finish_root()` these helpers are **placeholder names in the plan**: name them against the real data structures when implementing (for instance, judge "does this root have a record" directly from `rows.is_empty()` and `values`, with no extra method needed). **The only criterion is that Task 9's existing tests are all green**, especially:

- `empty_document_and_array_yield_nothing`: empty input yields zero records; `[]` yields exactly one `Record::Json(None)` and `last_record_start() == 0`;
- `document_error_is_terminal_for_later_polls`: after an error, further `next()` calls still return the same error (remembering it in `self.failed: Option<Error>` is enough);
- `document_single_root_trailing_garbage_errors` / `document_whitespace_only_is_a_format_error`: in whole-document single-root mode, both trailing garbage and pure whitespace report `Format("json input: …")`;
- `document_array_larger_than_1mb_streams`: 4 × 400 KB elements with no error (deviation ①).

**Implementation hint (avoid the long way around)**: make "does the current root have a record" and "is the root already finished" explicit state on `Matcher` (for example `Matcher::root_complete()` / `Matcher::take_rows()`), and make the "record start" a value carried along inside `Matcher` (`VecDeque<(Option<Value>, u64)>` instead of `VecDeque<Option<Value>>`), and you can delete every `last_row_start`-style read-back above. **Implementing in this direction is recommended**: Task 5's `take_rows` signature then becomes `take_rows(&mut self) -> Vec<(Option<Value>, u64)>` (Task 5's test assertions change accordingly to compare only the `value` part).

- [ ] **Step 5: Confirm green**

Run: `cargo test -p tinio-select document::`
Expected: the existing 21 + the 3 new ones (apart from the exceptions named in Task 9, which should still be red at this point).

- [ ] **Step 6: Checkpoint**

Report: `crates/tinio-select/src/json/document.rs` pending commit. **Do not run a commit.**

---

### Task 7: `json/lines.rs` — the LINES handler (one value per line, the line never lands in memory)

> ⚠ **The Step 2 code in this section is draft scaffolding; use it only after applying the §Revisions corrections**: **① `current_str()` for strings/key names must propagate errors** — the original's `parser.current_str().ok()` silently turns invalid UTF-8 into an empty string (`VEvent::Name("")`/`Text("")`), which directly breaks existing tests such as `skipped_subtree_still_validates_strings` (R2.5); both the `FieldName` and `ValueString` events must call it and `?`. **② Delete the `depth`-related code** (R3) and the `value_start(&self.session, parsed_before)` helper (actson has no peek, R2.3; offsets follow R4). **③ Blank lines / whitespace-only lines (verified closed, no synthesis)**: for empty input actson returns `Err(NoMoreInput)` directly (message `json input: nothing more to parse`), so it never reaches the original draft's `!seen_value && progress == Progress::End` synthesis branch. Verified on the frozen tree: `empty_line_is_a_format_error` (lines.rs:352, assertion at :365) and `lines_whitespace_only_line_is_a_format_error` (lines.rs:433, assertion at :439) **both assert only on `m.contains("json input")`** → the first branch of Q9-A holds, **let the actson message flow straight out, write no synthesis code**. **④** Write `lines_root_array_is_one_record` and `lines_multiline_document_per_line_is_a_format_error` (Step 3) as they are; for the latter, use the module's existing shape (`Lines::new` + `match r.next()`) per the note at the end of Step 3, **do not use `run_err`**. **⑤ The start of every line must call `self.session.reset_for_next_root()`** — after `Ok(None)` actson cannot be fed further without a reset (upstream's `too_many_next_event` has the same cause); `reset()` zeroes `parser.parsed_bytes`, which has no effect after R2.3 (R9.8). **⑥ Struct nesting correction**: `session: Session<LineFraming<BufReader<R>>>` (a single `LineFraming` instance — the original draft's `framing` field plus `Session<BufReader<LineFraming<BufReader<R>>>>` was double ownership, now deleted); call `start_line()` through `session.feeder_mut().inner_mut()`, and read the span flag through `session.feeder().inner().capped()` (R9.10). **⑦ Trailing-garbage synthesis**: the root closes and the parser then errors → synthesize `Error::Format("json input: trailing characters on line")` (its test asserts only the prefix, so the actson message would pass too; the synthesis is there to keep tinio's own message, R9.4). **⑧ Blank lines / whitespace-only lines**: the same item as ③ — verified closed, the original draft's `!seen_value && progress == Progress::End` synthesis branch is deleted (unreachable, R9.1). **⑨ The finishing path checks io failure first** (R11.3): the decision order of the `Err` branch is **cap (`feeder().inner().capped() || budget.fired()`) → io (`session.io_error()`) → trailing synthesis → parser message**; when `drive` ends normally, one more `session.io_error()` check must also be added — a `start_line()` read failure already returns directly through `Error::from(io)`, but a read failure **mid-line** can only be surfaced by this error the feeder remembers, otherwise that line is treated as "read cleanly to the end".

**Files:**
- Rewrite: `crates/tinio-select/src/json/lines.rs`
- Test: end of the same file (the 26 existing tests are kept one by one; only the padding input of `lines_input_record_cap` / `lines_final_unterminated_line_over_cap` switches to a legal JSON shape per deviation ④ — this was already done in the old plan, this task keeps it)

**Interfaces:**
- Consumes: `json::feed::Budget`, `json::stream::{LineFraming, LineStart, Session}`, `json::matcher::{CompiledPath, Matcher, Progress, VEvent}`.
- Produces: `pub struct Lines<R: Read>`, `new(reader: R, from: &FromClause)`, `impl RecordReader`.

- [ ] **Step 1: Implementation (run the existing tests first, then add new cases)**

```rust
//! LINES handler: each line is one root value. The line is never buffered —
//! LineFraming serves its bytes one at a time and reports EOF at the newline,
//! so the parser consumes the line as a stream and the only thing that grows
//! is the token it is currently resolving.

use std::io::{BufReader, Read};

use actson::JsonEvent;

use crate::{
    error::Error,
    json::{
        feed::Budget,
        matcher::{CompiledPath, Matcher, Progress, VEvent},
        stream::{LineFraming, LineStart, Session},
    },
    record::CAP_MESSAGE,
    row::Record,
    sql::FromClause,
};

pub struct Lines<R: Read> {
    /// One parser session over one `LineFraming` over one buffer layer; the
    /// framing is reached through `session.feeder_mut().inner_mut()` (no
    /// separate `framing` field — the draft's split ownership was a
    /// scaffolding bug, R9.10).
    session: Session<LineFraming<BufReader<R>>>,
    budget: std::rc::Rc<Budget>,
    matcher: Matcher,
    pending: Vec<Option<serde_json::Value>>,
    last_record_start: u64,
    done: bool,
    /// Set once an error was surfaced; every later poll repeats it
    /// (lines_error_is_terminal_for_later_polls).
    failed: Option<Error>,
}
```

The nesting order must be `Session(LineFraming(BufReader<R>))`: `StreamFeeder`'s own 64 KiB buffer already does `BufReader`'s job, and wrapping one more layer inside only makes `LineFraming::read`'s byte-by-byte calls cheap. **Construction point** (`Lines::new`, the same shape as `Document::new`): `let budget = Rc::new(Budget::default());` then `session: Session::new(LineFraming::new(BufReader::new(reader)), Rc::clone(&budget))`, with `budget` held by the handler itself — `LineFraming::new`'s parameter becomes `new(inner: R)` with Task 4's type parameterization (both the line start and `capped` are read through `session.feeder()….inner()…`, R9.10).

- [ ] **Step 2: Implement `pull_line`**

```rust
    /// Parse the next line into self.pending; sets self.done at input EOF.
    fn pull_line(&mut self) -> Result<(), Error> {
        match self.session.feeder_mut().inner_mut().start_line() {
            Err(io) => return Err(Error::from(io)),
            Ok(LineStart::Eof) => {
                self.done = true;
                return Ok(());
            }
            Ok(LineStart::Line(start)) => self.last_record_start = start,
        }
        self.session.feeder_mut().restart();
        // actson cannot be fed again after `Ok(None)` without a reset
        // (upstream's `too_many_next_event` is the same rule); the reset
        // zeroes `parser.parsed_bytes`, which nothing reads after R2.3 (R9.8).
        self.session.reset_for_next_root();
        self.budget.arm();
        let mut rows = Vec::new();
        // The handler's own depth counter: it sees every event, so it always
        // knows when the root value closes (the trailing-garbage rule, R9.4).
        let mut depth = 0usize;
        let mut root_complete = false;
        let outcome = self.session.drive(|ev, parser| {
            // R4: a scalar's start derives from the feeder's lookback of
            // recently served bytes; a container takes `byte_offset() - 1`.
            let start = parser.feeder.byte_offset();
            let vev = match ev {
                JsonEvent::StartObject => VEvent::StartObject,
                JsonEvent::EndObject => VEvent::EndObject,
                JsonEvent::StartArray => VEvent::StartArray,
                JsonEvent::EndArray => VEvent::EndArray,
                // `current_str` errors are propagated, never swallowed (R2.5):
                // invalid UTF-8 must surface even inside a skipped subtree.
                JsonEvent::FieldName => VEvent::Name(current_text(parser)?),
                JsonEvent::ValueString => VEvent::Text(current_text(parser)?),
                JsonEvent::ValueInt => VEvent::Int(current_text(parser)?),
                JsonEvent::ValueFloat => VEvent::Float(current_text(parser)?),
                JsonEvent::ValueTrue => VEvent::Bool(true),
                JsonEvent::ValueFalse => VEvent::Bool(false),
                JsonEvent::ValueNull => VEvent::Null,
                // `drive` intercepts NeedMoreInput and never forwards it.
                JsonEvent::NeedMoreInput => unreachable!("drive intercepts NeedMoreInput"),
            };
            match ev {
                JsonEvent::StartObject | JsonEvent::StartArray => depth += 1,
                JsonEvent::EndObject | JsonEvent::EndArray => {
                    depth -= 1;
                    if depth == 0 {
                        root_complete = true;
                    }
                }
                // A top-level scalar is a complete root by itself.
                _ if depth == 0 => root_complete = true,
                _ => {}
            }
            let p = self.matcher.on(vev, start)?;
            if p == Progress::StopRoot {
                rows.extend(self.matcher.take_rows());
            }
            Ok(p)
        });
        self.budget.disarm();

        let progress = match outcome {
            Ok(p) => p,
            Err(e) => {
                if self.session.feeder().inner().capped() || self.budget.fired() {
                    return Err(Error::Format(CAP_MESSAGE.into()));
                }
                // A reader failure outranks the parser's verdict: the parser
                // only ever saw an end of input (R11.3).
                if let Some(io) = self.session.io_error() {
                    return Err(self.store(io));
                }
                // The root closed cleanly and the parser still errored:
                // whatever follows is trailing garbage on the line (R9.4).
                if root_complete {
                    return Err(self.store(Error::Format(
                        "json input: trailing characters on line".into(),
                    )));
                }
                return Err(self.store(e));
            }
        };
        let _ = progress;
        // A clean drive is only clean if the reader did not fail underneath it
        // (R11.3) — otherwise the line is silently truncated.
        if let Some(io) = self.session.io_error() {
            return Err(self.store(io));
        }
        // A blank or whitespace-only line surfaces as `Err(NoMoreInput)`
        // ("json input: nothing more to parse") above — every related test
        // asserts the prefix only, so no message is synthesized here (R9.1).
        rows.extend(self.matcher.take_rows());
        if rows.is_empty() && self.matcher.root_matched_nothing() {
            rows.push(None);
        }
        self.pending = rows;
        Ok(())
    }
```

`current_text` is this module's small helper: it calls `parser.current_str()` and maps the error to `Error::Format(format!("json input: {e}"))` (R2.5's propagation requirement; Task 6's `vevent()` is the same thing). `self.store(e)`: stores the error in `self.failed` and returns it as-is (the terminal rule, `lines_error_is_terminal_for_later_polls`).

The criterion is that Task 9's existing 26 tests all pass green, of which the three most critical are:

- `lines_trailing_garbage_errors`: when there is more content after the root value on the same line, actson reports `SyntaxError` directly (non-streaming mode does not support a second top-level value) → `Format("json input: ...")`;
- `lines_last_record_start` / `lines_indented_line_reports_the_line_start`: the start comes from the `LineStart::Line(u64)` payload returned by `LineFraming::start_line()` (the line's first byte, indentation included; **there is no `line_start()` method**, the line offset is now carried by that enum payload, R9.10);
- `lines_error_is_terminal_for_later_polls`: store the error in `self.failed`; later `next()` calls return it as-is.

- [ ] **Step 3: Add two new cases**

```rust
    #[test]
    fn lines_root_array_is_one_record() {
        // LINES roots are lines, so a bracketed line is ONE record — the
        // root-array splitting belongs to DOCUMENT only.
        assert_eq!(
            read("SELECT * FROM S3Object", "[{\"a\":1},{\"a\":2}]\n"),
            vec![Record::Json(Some(json!([{"a": 1}, {"a": 2}])))],
        );
    }

    #[test]
    fn lines_multiline_document_per_line_is_a_format_error() {
        // Each line must be a complete value: a line that only opens a
        // container is a format error, not the start of a two-line record.
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(b"{\n  \"a\": 1\n}\n".to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{m}"),
            other => panic!("expected format error, got {other:?}"),
        }
    }
```

(The shape is word-for-word identical to the same module's `empty_line_is_a_format_error`; `Lines`/`parse`/`Cursor` are all in this test module's existing imports, and `read` uses the module's existing private helper. **Do not use `crate::json::testing::run_err`** — what it returns is an **engine** error, while the reader's `Err` panics directly at `testing.rs:53`'s `r.next().unwrap_or_else(|e| panic!(...))`, so the reader Format error this case wants to observe never reaches the assertion at all; R9.10's "switch to run_err" therefore does not hold.)

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select lines::`
Expected: all 26 + 2 pass green.

- [ ] **Step 5: Checkpoint**

Report: `crates/tinio-select/src/json/lines.rs` pending commit. **Do not run commit.**

---

### Task 8: `json/mod.rs` — façade closeout and `framing.rs` retirement

**Files:**
- Modify: `crates/tinio-select/src/json/mod.rs` (module documentation + module declarations)
- Delete: `crates/tinio-select/src/json/framing.rs`, `crates/tinio-select/src/json/core.rs`
- Test: `json/mod.rs`'s engine composition tests stay

**Interfaces:**
- Consumes: `lines::Lines`, `document::Document`.
- Produces: the unchanged `json::Reader::new` and `RecordReader` (`Handler` dispatch logic itself is unchanged).

- [ ] **Step 1: Swap the module declarations**

```rust
pub(crate) mod document;
pub(crate) mod feed;
pub(crate) mod lines;
pub(crate) mod matcher;
pub(crate) mod stream;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod value;
```

(The `framing` line is deleted; the order is alphabetical.)

- [ ] **Step 2: Delete `framing.rs` and `core.rs`**

Delete `crates/tinio-select/src/json/framing.rs` and `crates/tinio-select/src/json/core.rs` outright (`Core::pull` is altogether serde kernel machinery; the shared state has been taken over by the handler/Matcher, and `pending_high` has migrated to `Matcher`, R9.6). Do not touch the git index; the working tree state is for the user to decide.

- [ ] **Step 3: Rewrite the module documentation**

Change the first paragraph into a factual statement about actson + StreamFeeder, and replace the old Throughput note with a placeholder for the measured record that points at the bench (at the same time delete the `json_error` helper that served only the serde kernel — error mapping is now carried uniformly by `Session::map_err`). **Do not miss the ①–⑥ deviation list in that same documentation section** — go through it item by item against the table: ① kept (cap changes to binding "per root value"), ② kept (in-stream error for the ambiguous Name of a non-final step + the `failed` terminal-state rule unchanged), **③ must be rewritten** (the original's "a single-value root accumulates the match set by pulling, backstopped by cap" is a fact of the old kernel; under the new kernel hit values are enqueued one by one when the container closes and `StopRoot` delivers by batch, so `pending` is independent of root size, R9.2), ④ kept, ⑤ kept, ⑥ kept (element-level offset + ScanRange filtering):

```rust
//! Throughput note: this kernel feeds actson one byte at a time on demand, so
//! the cost model is fixed by actson's state machine rather than by a
//! byte-slice scanner. The json_document_scan bench over the ~900 KB fixture
//! is the regression baseline; record the measured
//! "old (serde_json whole-document) -> new (actson streaming)" numbers here
//! after Task 10 runs the bench.
```

- [ ] **Step 4: Confirm whole-package compilation and tests**

Run: `cargo test -p tinio-select`
Expected: all green except Task 9's 3 named tests pending deletion, deviation ⑧'s two expectations, and the exemptions R9 names (`{} x` expectation flips, `pending_high` read paths ×2, `\uD800`→`\uDC00`).

- [ ] **Step 5: Checkpoint**

Report: `crates/tinio-select/src/json/mod.rs`, plus the deleted `framing.rs` and `core.rs`, pending commit. **Do not run commit.**

---

### Task 9: Test migration and aligning with existing assertions

**Files:**
- Modify: `crates/tinio-select/src/json/matcher.rs` (test module: delete the 3 serde reserved-key tests)
- Modify: `crates/tinio-select/src/json/mod.rs` (the two expectations of deviation ⑧)
- Modify: `crates/tinio-select/src/json/document.rs` (rewrite the serde_json rationale in a comment)

**Interfaces:**
- Consumes: the implementation of Tasks 5–8.
- Produces: an all-green `cargo test -p tinio-select`.

- [ ] **Step 1: Delete the serde reserved-key tests**

Delete these three in `matcher.rs` (their whole rationale is serde_json's `$serde_json::private::Number` mechanism, which does not exist under the new kernel): `number_token_first_key_object_errors_not_panics`, `number_token_single_entry_is_a_scalar`, `skipped_subtree_rejects_a_reserved_number_token_object` (corrected per R5 — the originally named `number_token_map_entry_is_a_scalar` does not exist in the tree).

Also delete the `NUMBER_TOKEN` constant at the top of `matcher.rs` and every comment that references it. The semantics (that key is just an ordinary key) are covered by Task 5's `duplicate_keys_collapse_last_wins_first_position` and `nested_containers_are_assembled`.

- [ ] **Step 2: Align the numeric expectation of deviation ⑧**

Change `json/mod.rs`'s `raw_number_passthrough_no_decimal_parse` to:

```rust
    #[test]
    fn raw_number_passthrough_no_decimal_parse() {
        // Arbitrary precision: an exponential token never hits f64 (1e309
        // overflows f64) and never reaches the decimal spine — RawNumber
        // carries it through. Deviation 8: actson's token buffer is the input
        // bytes, so the token comes back verbatim; serde_json's old injection
        // of a plus sign into a signless exponent is gone.
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
```

- [ ] **Step 3: Rewrite the old rationale in the doc comment**

In `document.rs`'s `document_wild_over_object_collapses_duplicate_keys` comment, change the serde_json preserve_order IndexMap sentence to "the record model's preserve_order map (an IndexMap)"; the test itself is not changed by a single line — it pins the record model's collapse semantics, not the parse kernel.

- [ ] **Step 4: Check item by item against the disposition table for existing tests**

After the implementation is complete, check against this table (this table is this task's acceptance checklist):

| Test | Disposition |
| --- | --- |
| document_* (21 items, except those listed in the table below) | pass as-is |
| document_last_record_start_is_zero | keep; if the measured offset is not 0, report and ask, do not change the expectation on your own |
| document_input_record_cap / _exact_allowed | pass as-is (deviation ⑦ guarantees the exact value passes exactly at the limit) |
| document_single_root_over_cap_errors / _under_cap_yields_all_matches | keep (still holds after deviation ③ is retired; the rationale becomes per-event driven; the former requires the budget to **accumulate across polls**, see R11.1) |
| `record.rs`'s `document_start_above_zero_yields_nothing` (:360) / `document_scan_range_filters_elements_by_start` (:382) | pass as-is — both directly pin DOCUMENT's element-level real offset (the latter pins element 2 = 11 of `[{"a": 1}, {"b": 2}]`), the third assertion of deviation ⑥ inside the crate, and not among the `document_*` (21 items) (R11.8) |
| lines_* (26 items) | pass as-is |
| all of framing.rs (**10** items) | deleted along with the file; LineFraming's 3 items move into stream.rs, BudgetedRead's 2 items are replaced by new feed.rs cases, **DocFraming's 5 items are retired** (the original text says 9 / 4, which does not match the tree) |
| `document_single_root_trailing_garbage_errors` | the `{} x` case's expectation goes `false`→`true` (the handler synthesizes the trailing report, R9.4); the other four items stay untouched |
| the `pending_high` assertions of `document_array_root_keeps_pending_bounded` / `document_single_root_under_cap_yields_all_matches` | read path `r.core.pending_high.get()` → `r.pending_high()` (R9.6); thresholds untouched |
| `lines_skipped_subtree_still_validates_strings` | the input `"\uD800"` (lone high surrogate, actson passes it silently — deviation ⑪) changes to `"\uDC00"` (lone low surrogate, actson reports `SyntaxError`); the assertion structure is untouched (R9.9) |
| matcher.rs's 3 reserved-key tests | delete: `number_token_first_key_object_errors_not_panics`, `number_token_single_entry_is_a_scalar`, **`skipped_subtree_rejects_a_reserved_number_token_object`** (the originally named `number_token_map_entry_is_a_scalar` does not exist in the tree) + the top `NUMBER_TOKEN` constant |
| matcher.rs's `element_barely_under_cap_is_accepted` / `budget_is_charged_per_root_not_accumulated` | retired (matcher-layer budget tests; under the new architecture the matcher does not touch the budget); the coverage is taken over by the handler layer's `document_input_record_cap_exact_allowed`, `document_element_exact_cap_inside_array_root_is_accepted`, `document_array_larger_than_1mb_streams` (R5, deleting without taking over is not allowed) |
| raw_number_passthrough_no_decimal_parse | expectation changed to 1e309 (Step 2) |
| the remaining combination tests of json/mod.rs | pass as-is |

- [ ] **Step 5: Confirm green**

Run: `cargo test -p tinio-select`
Expected: all green.

- [ ] **Step 6: Checkpoint**

Report: `matcher.rs`, `mod.rs`, `document.rs` pending commit. **Do not run commit.**

---

### Task 10: bench, large-document streaming test, and doc sync

**Files:**
- Modify: `crates/tinio-select/benches/select_scan.rs`
- Modify: `crates/tinio-select/src/record.rs` (the `MAX_RECORD` comment)
- Modify: `crates/tinio-select/src/json/document.rs` (add the large-document generated high-water-mark test, R9.7; adversarial input table)
- Modify: `crates/tinio-select/src/json/lines.rs` (adversarial input table)
- Modify: `docs/superpowers/specs/2026-09-04-select-object-content-design.md`, `docs/superpowers/plans/2026-09-04-select-object-content.md`, `docs/superpowers/specs/2026-09-12-json-streaming-design.md`

**Interfaces:**
- Consumes: every artifact of Tasks 1–9.
- Produces: a reproducible throughput baseline + docs consistent with the implementation.

- [ ] **Step 1: Update the bench doc and run the baseline**

Change the description of `json_document_scan` at the top of `benches/select_scan.rs` to:

```text
json_document_scan drives the streaming json::Reader façade over the same
897_751-byte array fixture the pre-refactor baseline used. Three numbers, all
recorded here (R6): the serde_json whole-document façade 37.023 ms
(23.125 MiB/s), the serde_json streaming façade 58.620 ms (14.605 MiB/s,
change +44.1% / -30.6%, p = 0.02 — a real regression, recorded in
plans/2026-09-12-json-streaming.md), and the actson streaming façade measured
here. The actson number is compared against 58.620 ms — the streaming façade
it replaces — never against 37.023 ms. Acceptance bar (Q4-D): the actson
façade must not be slower than 58.620 ms; if it is, stop and escalate instead
of tuning thresholds. json_events_only (R0) records the raw parse cost of the
same fixture under the same harness with the budget armed, as the lower bound.
The fixture is under the 1 MB record cap, so the old path could read it too.
```

Run: `cargo bench -p tinio-select --bench select_scan -- json_document_scan --quick`
Expected: numbers produced (benchmark on this machine, no threshold set).

- [ ] **Step 2: Write the measured numbers back**

Replace Task 8 Step 3's placeholders with the measured old -> new numbers (the time and thrpt lines, including criterion's change row). Write only measured values, never estimates; if the change row has p > 0.05, state plainly that the direction is credible and the point estimate is not.

- [ ] **Step 3: Update record.rs's MAX_RECORD comment**

```rust
/// One record root. The memory defence is "unmatched subtrees are never
/// materialized + nothing buffers the document", so this cap is a record
/// shape rule (and the DoS guard) rather than a memory backstop: a DOCUMENT
/// over 1 MB streams fine as long as every root value fits.
pub(crate) const MAX_RECORD: u64 = 1024 * 1024;
```

- [ ] **Step 4: Sync the two 2026-09-04 documents**

- `specs/2026-09-04-select-object-content-design.md`: change the JsonReader row to "DOCUMENT (root array -> one record per element; the object itself is the record otherwise; each root value <= 1 MB)", and append `(superseded 2026-09-12: the streaming kernel is actson; see 2026-09-12-json-streaming-design.md)` at the end of the row; append one superseded/amended annotation each to ScanRange's byte-0 pinned note and Streaming's 1 MB wording, keeping the original text without rewriting it.
- `plans/2026-09-04-select-object-content.md`: append supersede annotations to Task 9 / Task 10's byte-0 notes the same way.

- [ ] **Step 5: Add the large-document generated high-water-mark unit test (R7/R9.7)**

In `document.rs`'s test module (in-crate `#[cfg(test)]` — `tests/*.rs` integration tests cannot read `pending_high`) add `document_large_generated_stream_keeps_pending_bounded`: a generated `Read` (producing 100 000 elements on demand, not materialized, no LFS fixture) feeds a multi-MB document, all records are taken out one by one, and `r.pending_high()` is asserted to have a constant upper bound (same basis as `document_array_root_keeps_pending_bounded`, scaled up to multi-MB).

Run: `cargo test -p tinio-select document_large_generated_stream_keeps_pending_bounded`
Expected: pass.

- [ ] **Step 6: Run the large-document streaming test once**

Run: `cargo test -p tinio-select --test stream_large`
Expected: pass (the fixture is already in the repo, git LFS is in place). If the fixture is missing, report and ask, do not regenerate it — the generator is an `--ignored` test, a step the user decides.

- [ ] **Step 7: Add the adversarial input cases (R11.7, settled by the user: table-driven, no new dependency)**

Add one table-driven case to each of the `document.rs` and `lines.rs` test modules (`document_adversarial_inputs_never_panic` / `lines_adversarial_inputs_never_panic`): for each input assert "**either it produces records, or it is `Error::Format` or an io error — never a panic**" (`Ok` records read are all counted, contents are not compared; `#[should_panic]` does not appear). Every entry of the input table targets one verified boundary mechanism:

| Input | Mechanism pinned |
| --- | --- |
| `5x` / `123#` | illegal terminating byte: actson **emits no value event**, a direct `SyntaxError` (R9.4) |
| `{} x` / `{} {}` / `5 {}` | a legal terminator emits the event first → the handler synthesizes the trailing report (R9.4) |
| `{"a":"\uD800"}` / `{"a":"\uDC00"}` | lone high/low surrogate: the high one passes silently (deviation ⑪), the low one is a `SyntaxError` (R9.9) |
| `1e` / `-` / `tru` | truncated inside a token: `1e`/`-` are non-accepting states → `NoMoreInput`; `tru` is flushed by EOF into `ValueTrue` (R2.6) |
| `[` / `{"a":` / `[[[[` | unclosed container → `NoMoreInput` |
| 128 / 129 levels of `[` nesting | both sides of the `MAX_DEPTH = 129` boundary (128 levels pass, 129 levels error, R2.2) |
| `{"a":"<0xFF>"}` (illegal UTF-8 bytes) | not validated at parse time, only `current_str()` fails (R2.5) → must be `Format`, not a panic |
| `{"a":"b\x01c"}` (control character inside a string) | `SyntaxError` at parse time |

Run: `cargo test -p tinio-select adversarial`
Expected: both pass. **This one must be in place before Task 8/9**, as the new kernel's regression net (in particular covering the two panic points: `verbatim_number`'s `expect` and `Cap::on`'s `unreachable!`).

- [ ] **Step 8: Checkpoint**

Report: `benches/select_scan.rs`, `src/record.rs`, `src/json/document.rs`, `src/json/lines.rs`, four docs pending commit. **Do not run commit.**

---

### Task 11: Full acceptance

**Files:** none (verification task)

**Interfaces:**
- Consumes: all changes.
- Produces: acceptance evidence.

- [ ] **Step 1: Standard acceptance legs (one by one, done only when all are green)**

```text
cargo test -p tinio-select
cargo test --workspace --exclude tinio-e2e
cargo test -p tinio-select --features parquet
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --all -- --check
```

(the first leg already includes Task 10 Step 7's two adversarial input cases; the `--all-targets` clippy leg also guards R11.2's import style.)

- [ ] **Step 2: Cross-check of the memory promises (manual, written into the report)**

Answer item by item and give the code location:

1. Does anything anywhere read the whole document into memory? Expected: no; StreamFeeder only has a 64 KiB read buffer.
2. Does anything anywhere read a whole line into memory? Expected: no; LineFraming serves byte by byte.
3. Does anything anywhere materialize unmatched subtrees? Expected: no; the matcher only drives Cap for matched values.
4. With a single-value root and a deep wildcard, does it still accumulate the whole document's match set at once? Expected: no; StopRoot hands each produced batch to the handler, and pending holds only the records not yet taken.
5. Is the document content copied more than once? Expected: no; actson's current_buffer is the current token, a matched value's string goes straight into the record via to_owned, everything else is zero-copy.
6. Is the budget's scope "one root"? Expected: yes — array mode charges in and out per element, single-value root mode **stays armed across polls** (`document_single_root_over_cap_errors` being green is the counter-proof), `StopRoot` does not disarm (R11.1).
7. Is an underlying `Read` failure taken for a clean EOF? Expected: no — `StreamFeeder::io_error` is reported as `Error::from(io)` by `Session::map_err` and by both handlers' finish paths (R11.3).
8. Does the LINES path show an obvious throughput regression? (**no bar set**, R6/R11.9) Expected: run the same fixture once by hand in its line-shaped form and put the numbers in the report; the same order of magnitude as DOCUMENT is enough.

- [ ] **Step 3: Spec coverage self-check**

Check item by item this plan's "behavioral differences from the old serde_json kernel" 1–7, the spec's deviations ①–⑫, **and §Revisions R11's 11 items**: each one must point to a task or a test. Any one without an owner → add a test or add a doc, do not leave it blank.

- [ ] **Step 4: Checkpoint (final report)**

Report: the full list of files pending commit (paste git status --short as-is) + the results of the three steps above + an item-by-item report of every "implementation-body adjustment that deviated from the plan during implementation". **Do not perform any git write operation.**

### Task 12: Translation after full completion (per R8)

**Files:**
- Modify: `docs/superpowers/plans/2026-09-12-json-streaming-actson.md`, `docs/superpowers/specs/2026-09-12-json-streaming-design.md` (translate in place into English)

**Interfaces:**
- Consumes: Task 11's all-green result.
- Produces: the English plan and spec.

- [ ] **Step 1: Translate**

After Task 11 is all green, **translate this plan and the spec in place into English** (overwriting the Chinese, keeping no copy — the git history is the copy); code identifiers, deviation numbers, and test names are not translated, `superseded`/`amended` annotations are translated along with the main text.

- [ ] **Step 2: Checkpoint**

Commit order: the user commits the code by hand first, **the translation is a separate commit**. Do not perform any git write operation.

---

## Self-Review

**1. Spec coverage**

| spec section/requirement | Implemented by |
| --- | --- |
| Parse kernel (single parser + number fidelity + depth cap) | Task 1 (docs), Task 3/4 (implementation) |
| Byte serving and accounting (cap per root, no swallowing bytes) | Task 1, Task 3 (Budget + absolute-offset accounting; **depth gating was deleted entirely with R3**), Task 6/7 (arm/disarm + refund by offset) |
| PathSeg compile state machine (order equivalence, ambiguous streaming handling, MISSING) | Task 5 (18 semantic tests), Task 9 (existing façade tests) |
| Handler (LINES one root per line, DOCUMENT array element by element, offsets, empty input/whitespace/trailing garbage) | Task 6, Task 7 |
| Error handling (Format prefix, cap verbatim, Ambiguous unchanged) | Task 4 (map_err), Task 6/7 (map_err) |
| Semantics preserved and deviations ①–⑫ | Task 1 (written into the spec), Task 9 (test alignment) |
| Test plan (migration + new + large document + memory gate + adversarial input) | inline tests of Task 3/4/5/6/7, Task 9, Task 10 Step 5/7 |
| Acceptance criteria (five legs + bench) | Task 10, Task 11 |
| Sync updates (three old docs + record.rs comment) | Task 10 Step 3/4 |

**2. Placeholder scan**

- `todo!()` appears only in Task 6 Step 3, and is explicitly marked as scaffolding, replaced wholesale in Step 4 — the final code must not leave a `todo!` behind.
- The "implementation body may be adjusted" annotation appears only in Task 4 Step 3 (borrow explanations, item-by-item reporting required), Task 5 Step 5 (structure may be merged/renamed, the criterion being Step 1's 18 tests) and Task 6 Step 4 (placeholder naming, the criterion being Task 9's existing tests) — all three give an explicit criterion, none says "use your judgment".
- No TBD / to-be-filled / "similar to Task N".

**3. Type consistency**

- `Progress` is defined in `stream.rs` and re-exported by `matcher.rs`; both handlers import it uniformly from `matcher`.
- `Matcher::on(ev: VEvent<'_>, start: u64) -> Result<Progress, Error>` is identical in the three places Task 5/6/7.
- `Session::drive`'s callback signature `FnMut(JsonEvent, &JsonParser<StreamFeeder<R>>) -> Result<Progress, Error>` (no offset parameter after R2.3) is identical in the three places Task 4/6/7.
- `StreamFeeder::restart/byte_offset/capped/io_error/inner/inner_mut` are defined in Task 3 and called in Task 4/7, with consistent names (`value_start` was deleted along with R2.3); `Session::io_error() -> Option<Error>` is defined in Task 4 and called by Task 6/7's finish paths (R11.3).
- **Known cross-task signature evolution (must be synced during execution)**: Task 6 Step 4 suggests upgrading `Matcher::take_rows` to `Vec<(Option<Value>, u64)>` (the record start carried along with the row); if adopted, Task 5's test assertions (taking .0) and Task 7's call sites must be changed in sync. This is the only explicitly named signature evolution in this plan. The remaining signatures settle per R9.10: `CompiledPath::from_segments(&[PathSeg])` (no `Type` parameter), `vevent() -> Result<VEvent<'a>, Error>`, `Session::feeder_mut` / `StreamFeeder::inner(_mut)`, `Matcher::pending_high` (`#[cfg(test)]`).

**4. Revisions take precedence**

When the text above (Goal / Architecture / Behavioral differences / Global Constraints / Verified external facts / each Task's main text) conflicts with `## Revisions (2026-09-13 grill decisions)`, **Revisions always prevails**: R0 phase 0 (spike + criterion), R1 the deviation-list form, R2 the 7 actson fact corrections, R3 cap and budget (depth pipeline deleted), R4 offset derivation and stop conditions, R5 test disposition corrections, R6 throughput baseline and bar, R7 memory gate, R8 Task 12 translation, **R9 the review-fix round (empty-input semantics, StopRoot batch semantics, cap no-slack criterion, trailing report synthesis, deviation ⑫, core.rs disposition, fact-section corrections, signature settling)**, **R10 the R9 landing re-check (re-taking counts and evidence against the frozen tree `b20e7eb`, Task 7's `run_err` fix, Task 1 Step 5 made executable, Task 8 deviation-list disposition, `Interrupted` idle-spin, second evidence-gathering on actson's terminator timing — where it conflicts with R9, R10 prevails)**, **R11 the second review round (budget arm/disarm scope = one record root, clippy `absolute_paths` hard constraint, `io_error` reporting on all paths, `key_matches` reuse, `refund` clamping, adversarial input table, no throughput bar for LINES, actson version coupling, `capped` stickiness — where it conflicts with R0–R10, R11 prevails)**. If a new behavioral difference not covered by Revisions is found during execution → stop and ask, do not extend the deviation list on your own.
