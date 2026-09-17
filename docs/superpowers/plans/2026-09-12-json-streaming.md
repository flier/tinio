# JSON Streaming Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `crates/tinio-select/src/json.rs` into two streaming handlers, LINES / DOCUMENT, sharing the serde_json streaming core and the `PathSeg` compiled state machine. Memory boundedness is tiered by framing (spec deviation ③): LINES ≤ the match set of a single line (line span ≤ 1 MB); DOCUMENT array root ≤ the match set of one element; DOCUMENT single-value root = the match set of the whole document accumulated over one pull, **but equally backstopped by the 1 MB record budget** (when the limit is exceeded, the cap is reported mid-pull, see deviation ③ and the Q15 revision). The `json::Reader::new` signature is unchanged, and call sites need zero changes.

**Architecture:** serde_json `Deserializer::from_reader` + `DeserializeSeed` pulling value by value; the framing filter (a `Read` adapter) makes each pull parse exactly one record root and enforces the cap mid-way; `PathSeg` is compiled into `CompiledPath`, matched against the event stream via the serde `MapAccess`/`SeqAccess` callbacks, and unmatched subtrees are consumed with the self-written validating skip `Skip` (no materialization, and validation identical point for point to today's `from_slice`); the `json::Reader::new` façade signature is unchanged, so call sites need zero changes. **The cap is charged per "root pull"**: the handler arms/disarms the `Budget` at every root boundary, and `BudgetedRead` charges **every byte** served during that pull (including unmatched and skipped subtrees and the injected virtual whitespace; the top-level `[`/`]`/`,` swallowed inside framing, the whitespace of `skip_ws` and the `\n` are accounted for by framing itself) — hence "one pull = one root ≤ 1 MB" is the only invariant that has to be maintained.

**Tech Stack:** Rust 2024, serde_json 1.0.151 (`arbitrary_precision` + `preserve_order`, crate-level features, `crates/tinio-select/Cargo.toml:22-25`), serde 1 (the only dependency change in this plan, already staged and uncommitted), flate2 (regular dependency, used by integration tests).

**Spec:** `docs/superpowers/specs/2026-09-12-json-streaming-design.md` (the authoritative source for the six confirmed deviations ①–⑥; read alongside this plan during execution)

## Global Constraints

- **Perform no git write operations whatsoever** (CLAUDE.md overrides the skill's commit step): every task closes with "checkpoint: report the list of files pending commit", and **does not run** `git add/commit/push`.
- Code, comments, tests and commit messages are always in English.
- Language: English. Originally written in Chinese under an explicit user exemption; translated to English on 2026-09-15 at the user's request, which ends that exemption.
- `unsafe_code = "forbid"` is a **per-crate** setting (`docs/cargo.md:10`, `crates/tinio-select/Cargo.toml:51`); there is no workspace-level setting; the **only permitted dependency change** is `serde.workspace = true` (already staged in `crates/tinio-select/Cargo.toml`, within spec scope and pre-approved).
- Existing tests: **not a single line is touched**, apart from the two padded inputs designated by deviation ④; an existing assertion that goes red = an implementation bug, not grounds for changing the test.
- Semantics follow spec deviations ①–⑥: ① DOCUMENT total volume unmuzzled; ② non-final-step Name ambiguity reported immediately in-stream; ③ single-value root match set accumulates across the document (backstopped by the 1 MB record budget, see Q15); ④ an illegal LINES line may report a parse error first; ⑤ DOCUMENT errors exposed in streaming fashion; ⑥ DOCUMENT element-level true offsets + ScanRange filtering.
- **The cap is charged per root pull** (2026-09-13 review revision, Q9(a)): `Budget::reset()` is called once by the **handler** at the start of every root pull (**Amended 2026-09-13**: originally an `arm()`/`disarm()` pair; `BudgetedRead` lives only inside the pull block, and the always-true `armed` flag was deleted), the matcher's two seeds do not hold a `Budget`; `BudgetedRead` charges every byte served during this one pull (the ones swallowed inside framing — the top-level `[`/`]`/`,`, the whitespace swallowed by `skip_ws`, the LINES `\n` — do not pass through it, and are backstopped by the framing's span/offset accounting). The cap's semantics are therefore "one root ≤ 1 MB" — an element in array mode, the whole document in single-value mode — and unmatched subtrees are equally constrained (there is no longer a "skipped region is exempt from the cap" gap).
- Document style (docs/style.md): compressed prose, imports of 3+ segments de-qualified, errors via the per-crate `Error`.
- **Implementation discipline**: interfaces, tests and semantics follow this plan and the spec; inline implementation bodies may be adjusted under compiler constraints, but every adjustment must be reported item by item in the execution report (two are already known, decided empirically by rustc 1.98.1 `--edition 2024`: the `serve`/`skip_ws` borrow refactor in Task 4, and the seed shape in Task 5).
- **Memory gate**: the spec's counting `#[global_allocator]` is unavailable (`unsafe_code = "forbid"` at `crates/tinio-select/Cargo.toml:51` covers every target of this crate, including `tests/`, and `forbid` cannot be overridden by `allow`), so it is replaced by a crate-internal `#[cfg(test)]` `pending_high` high-water pin (the two unit tests in Task 7: array root bounded + deviation ③ exception), and `tests/stream_memory.rs` is cancelled.
- Verified serde_json 1.0.151 facts: under `arbitrary_precision`, **non-integer** numbers (`1.5`, `1e309`, integers ≥ 2^64) reach the visitor as a single-key map whose key is `"$serde_json::private::Number"` (`src/number.rs:17-18`; `pub(crate)`, not importable — hard-coded and pinned by a test), while **ordinary integers go through `visit_u64/i64`** (`ParserNumber::visit`, `de.rs:117-129`); `serde_json::Error` has `is_eof()/is_io()/io_error_kind()` and does **not** have `into_io_error`; `impl Deserializer for &mut Deserializer` exists (`de.rs:1389`); `de::IoRead` reads **byte by byte** (`read.rs:200-207` uses std's `io::Bytes` — byte-at-a-time comes from std `Bytes`'s implementation, each `read` yields exactly 1 byte) and stores the byte it peeked in its own `ch` (`read.rs:286-298`) — **that byte has already been consumed from framing, so dropping this Deserializer loses it permanently**; `de::IgnoredAny` only performs structural validation, and its `ignore_str` (`read.rs:348-366`) validates neither surrogate pairs nor UTF-8 (hence every skip uses the self-written `Skip`, see Task 5).

---

### Task 1: Module skeleton split (json.rs → json/mod.rs + json/value.rs)

**Files:**
- Create: `crates/tinio-select/src/json/mod.rs` (moved wholesale from `json.rs`)
- Create: `crates/tinio-select/src/json/value.rs`
- Delete: `crates/tinio-select/src/json.rs` (replaced by the two preceding items)

**Interfaces:**
- Consumes: the entire current `json.rs` (`Type`/`Params`/`to_value`/`json_lookup`/`Reader`/tests).
- Produces: `crate::json::value::{to_value, json_lookup}`; `crate::json::{to_value, json_lookup}` re-exported via `pub(crate) use`, leaving `engine.rs` and the existing test paths unchanged.

- [ ] **Step 1: Pure-move split**

`json/value.rs` (cut `to_value` and `json_lookup` verbatim from the old `json.rs`, including all doc comments):

```rust
//! Row-value mapping and object key lookup — the engine's select side and
//! the traversal matcher share these (review 2026-09-06b R10: the two copies
//! used to drift).

use serde_json::Value;

use crate::{
    error::Error,
    row::{NameStyle, Value as RowValue},
};

pub(crate) fn to_value(j: &Value) -> RowValue { /* verbatim */ }
pub(crate) fn json_lookup<'a>(
    map: &'a serde_json::Map<String, Value>,
    name: &str,
    style: NameStyle,
) -> Result<Option<&'a Value>, Error> { /* verbatim */ }
```

`json/mod.rs` gains at the top:

```rust
pub(crate) mod value;
pub(crate) use value::{json_lookup, to_value};
```

And delete the two local function definitions from `mod.rs` (if `use crate::row::{NameStyle, Value as RowValue}` is left with only test uses, move it into the test module). `lib.rs`'s `pub mod json;` resolves automatically to `json/mod.rs`, so no change is needed.

- [ ] **Step 2: Run the suite to verify the pure move**

Run: `cargo test -p tinio-select`
Expected: all green (identical to before the split); `cargo check --workspace --all-targets` warning-free.

- [ ] **Step 3: Checkpoint**

Report: `crates/tinio-select/src/json.rs` (deleted), `json/mod.rs`, `json/value.rs`, `crates/tinio-select/Cargo.toml` (serde dependency) pending commit. **Do not run commit.**

---

### Task 2: framing.rs — Budget + BudgetedRead

**Files:**
- Create: `crates/tinio-select/src/json/framing.rs`

**Interfaces:**
- Consumes: `crate::record::{CAP_MESSAGE, MAX_RECORD}`.
- Produces: `pub(crate) struct Budget { reset, fired }` — **reset is called by the handler at the start of each root pull** (Tasks 6/7; **Amended 2026-09-13**: `arm`/`disarm` merged into `reset`), the matcher's seed does not hold it; `pub(crate) struct BudgetedRead<R>::new(inner, Rc<Budget>)` implementing `Read`; `fn ws(b: u8) -> bool`. Tasks 3–7 all depend on this.

- [ ] **Step 1: Write the failing test**

End of `json/framing.rs`, `#[cfg(test)] mod tests`:

```rust
#[test]
fn budgeted_read_fires_mid_stream() {
    use std::io::Read;
    let budget = std::rc::Rc::new(Budget::default());
    budget.arm();
    let mut r = BudgetedRead::new(
        std::io::Cursor::new(vec![b'a'; (MAX_RECORD + 1) as usize]),
        budget.clone(),
    );
    let mut sink = Vec::new();
    let err = r.read_to_end(&mut sink).unwrap_err();
    assert_eq!(err.to_string(), CAP_MESSAGE);
    assert!(budget.fired());
}

#[test]
fn budgeted_read_exact_cap_allowed() {
    use std::io::Read;
    let budget = std::rc::Rc::new(Budget::default());
    let mut r = BudgetedRead::new(
        std::io::Cursor::new(vec![b'a'; MAX_RECORD as usize]),
        budget.clone(),
    );
    budget.arm();
    let mut sink = Vec::new();
    r.read_to_end(&mut sink).unwrap();
    assert_eq!(sink.len() as u64, MAX_RECORD);
    assert!(!budget.fired());
    budget.disarm();
    assert!(!budget.fired());
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select framing`
Expected: compile failure (`Budget`/`BudgetedRead` do not exist).

- [ ] **Step 3: Implement**

```rust
//! Framing filters: `Read` adapters between serde_json and the input stream.
//! They reshape the byte stream so each serde pull parses exactly one record
//! root, enforce the 1 MB input-record cap mid-read, and account offsets in
//! uncompressed bytes.

use std::{
    cell::Cell,
    io::{self, BufRead, BufReader, Read},
    rc::Rc,
};

use crate::record::{CAP_MESSAGE, MAX_RECORD};

/// JSON whitespace per RFC 8259.
pub(crate) fn ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Per-root-pull budget: the handler arms it before a root pull and disarms
/// it after, so every byte serde is served during that pull is charged —
/// matched, skipped and injected-delimiter alike (bytes the framing consumes
/// internally — top-level `[` `]` `,`, whitespace skipped by `skip_ws`, and
/// the LINES `\n` — never reach this layer). One pull is exactly one record
/// root, so the cap this enforces is "one root ≤ `MAX_RECORD`" (an array
/// element in DOCUMENT array mode, the whole document in single mode). The
/// handler maps `fired` to `Error::Format(CAP_MESSAGE)`.
#[derive(Default)]
pub(crate) struct Budget {
    armed: Cell<bool>,
    remaining: Cell<u64>,
    fired: Cell<bool>,
}

impl Budget {
    pub(crate) fn arm(&self) {
        self.armed.set(true);
        self.fired.set(false);
        self.remaining.set(MAX_RECORD);
    }

    pub(crate) fn disarm(&self) {
        self.armed.set(false);
    }

    pub(crate) fn fired(&self) -> bool {
        self.fired.get()
    }

    fn charge(&self, n: usize) -> io::Result<()> {
        if !self.armed.get() || n == 0 {
            return Ok(());
        }
        let r = self.remaining.get();
        if n as u64 > r {
            self.remaining.set(0);
            self.fired.set(true);
            return Err(io::Error::new(io::ErrorKind::InvalidData, CAP_MESSAGE));
        }
        self.remaining.set(r - n as u64);
        Ok(())
    }
}

/// `Read` wrapper charging every served byte to the shared `Budget` — armed
/// by the handler around a whole root pull, so the charge covers everything
/// serde is served: matched values, skipped subtrees and the injected
/// delimiter alike. Bytes the framing consumes internally (top-level
/// `[` `]` `,`, whitespace skipped by `skip_ws`, the LINES `\n`) never pass
/// through here; they are bounded by the framing's own span accounting
/// instead.
pub(crate) struct BudgetedRead<R> {
    inner: R,
    budget: Rc<Budget>,
}

impl<R: Read> BudgetedRead<R> {
    pub(crate) fn new(inner: R, budget: Rc<Budget>) -> Self {
        Self { inner, budget }
    }
}

impl<R: Read> Read for BudgetedRead<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        self.budget.charge(n)?;
        Ok(n)
    }
}
```

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select framing`
Expected: both tests PASS.

- [ ] **Step 5: Checkpoint**

Report `crates/tinio-select/src/json/framing.rs` pending commit (and add `pub(crate) mod framing;` to `json/mod.rs`).

---

### Task 3: framing.rs — LineFraming

**Files:**
- Modify: `crates/tinio-select/src/json/framing.rs`

**Interfaces:**
- Consumes: `ws`/`MAX_RECORD`/`CAP_MESSAGE` from Task 2.
- Produces: `pub(crate) struct LineFraming<R: Read>`: `new(BufReader<R>)`, `start_line() -> io::Result<LineStart>`, the `capped: bool` field; `pub(crate) enum LineStart { Line(u64), Eof }`. Task 6 depends on it.

- [ ] **Step 1: Write the failing test**

```rust
fn framing(bytes: &[u8]) -> LineFraming<std::io::Cursor<Vec<u8>>> {
    LineFraming::new(BufReader::new(std::io::Cursor::new(bytes.to_vec())))
}

#[test]
fn lines_serve_content_then_eof_until_next_line() {
    use std::io::Read;
    let mut f = framing(b"{\"a\":1}\n{\"b\":2}\n");
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap(); // lines exhausted means EOF
    assert_eq!(s, "{\"a\":1}");
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(8)));
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    assert_eq!(s, "{\"b\":2}");
    assert!(matches!(f.start_line().unwrap(), LineStart::Eof));
}

#[test]
fn lines_blank_line_serves_immediate_eof_but_exists() {
    use std::io::Read;
    let mut f = framing(b"\n");
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut buf = [0u8; 4];
    assert_eq!(f.read(&mut buf).unwrap(), 0); // empty line = present but with no content
    assert!(matches!(f.start_line().unwrap(), LineStart::Eof));
}

#[test]
fn lines_cap_fires_mid_serve_and_exact_span_allowed() {
    use std::io::Read;
    let mut f = framing(&vec![b'a'; (MAX_RECORD + 1) as usize]); // no terminator
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut sink = Vec::new();
    assert_eq!(f.read_to_end(&mut sink).unwrap_err().to_string(), CAP_MESSAGE);
    assert!(f.capped);

    let mut f = framing(&{
        let mut v = vec![b'a'; (MAX_RECORD - 1) as usize];
        v.push(b'\n');
        v
    });
    assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
    let mut sink = Vec::new();
    f.read_to_end(&mut sink).unwrap();
    assert_eq!(sink.len() as u64, MAX_RECORD - 1);
    assert!(!f.capped);
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select lines_`
Expected: compile failure.

- [ ] **Step 3: Implement (append to framing.rs)**

```rust
/// What `start_line` found at the stream position.
pub(crate) enum LineStart {
    /// A line exists (blank included); carries its start offset.
    Line(u64),
    /// Clean end of input: no more lines.
    Eof,
}

/// Serves one LINES line at a time: the line's content bytes (without the
/// `\n`), then EOF until `start_line` advances. The span (content +
/// terminator) is counted while serving and errors mid-serve past
/// `MAX_RECORD` — a line is never buffered whole.
pub(crate) struct LineFraming<R: Read> {
    inner: BufReader<R>,
    offset: u64,
    span: u64,
    eol: bool,
    pub(crate) capped: bool,
}

impl<R: Read> LineFraming<R> {
    pub(crate) fn new(inner: BufReader<R>) -> Self {
        Self { inner, offset: 0, span: 0, eol: true, capped: false }
    }

    pub(crate) fn start_line(&mut self) -> io::Result<LineStart> {
        debug_assert!(self.eol, "start_line before the previous line drained");
        if self.inner.fill_buf()?.is_empty() {
            return Ok(LineStart::Eof);
        }
        self.eol = false;
        self.span = 0;
        Ok(LineStart::Line(self.offset))
    }

    fn cap(&mut self) -> io::Error {
        self.capped = true;
        io::Error::new(io::ErrorKind::InvalidData, CAP_MESSAGE)
    }
}

impl<R: Read> Read for LineFraming<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.eol || out.is_empty() {
            return Ok(0);
        }
        let avail = self.inner.fill_buf()?;
        if avail.is_empty() {
            // Unterminated final line: EOF ends its content.
            self.eol = true;
            return Ok(0);
        }
        let nl = avail.iter().position(|&b| b == b'\n');
        let content = nl.unwrap_or(avail.len());
        let n = content.min(out.len());
        out[..n].copy_from_slice(&avail[..n]);
        self.inner.consume(n);
        self.offset += n as u64;
        self.span += n as u64;
        if n == content && nl.is_some() {
            self.inner.consume(1);
            self.offset += 1;
            self.span += 1;
            self.eol = true;
        }
        if self.span > MAX_RECORD {
            return Err(self.cap());
        }
        Ok(n)
    }
}
```

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select lines_`
Expected: the three tests PASS; the existing suite stays green.

- [ ] **Step 5: Checkpoint**

Report `json/framing.rs` pending commit.

---

### Task 4: framing.rs — DocFraming

**Files:**
- Modify: `crates/tinio-select/src/json/framing.rs`

**Interfaces:**
- Consumes: `ws` from Task 2.
- Produces: `pub(crate) struct DocFraming<R: Read>`: `new(BufReader<R>)`, `peek_mode() -> io::Result<DocMode>` (idempotent, cached and does not consume the first byte; Task 7 uses it to obtain the mode before constructing the `Deserializer`), `closed() -> bool` (the top-level `]` has been consumed; Task 7 uses it to tell `[]` apart from "EOF in the middle of a value"), pub(crate) fields `mode: DocMode`, `element_start: u64`, `framing_msg: Option<String>`; `pub(crate) enum DocMode { Unknown, Empty, WsOnly, Array, Single }`. Task 7 depends on it. Behavioral contract: in array mode each serde pull parses exactly one element (the top-level `[`/`]`/`,` are swallowed and validated, and a virtual space is injected at the tail of a scalar element); single mode passes through; `element_start` = the byte offset of the current element's first byte.

- [ ] **Step 1: Write the failing test (verifying byte reshaping through real serde pulls)**

```rust
fn pull_values(bytes: &str) -> (Vec<serde_json::Value>, Vec<u64>, DocMode) {
    let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(bytes.as_bytes().to_vec())));
    let mut vals = Vec::new();
    let mut starts = Vec::new();
    loop {
        let r = {
            let mut de = serde_json::Deserializer::from_reader(&mut f);
            serde_json::Value::deserialize(&mut de)
        };
        match r {
            Ok(v) => {
                vals.push(v);
                starts.push(f.element_start);
            }
            Err(e) if e.is_eof() => break,
            Err(e) => panic!("{bytes}: {e}"),
        }
        if f.mode == DocMode::Single {
            break;
        }
    }
    (vals, starts, f.mode)
}

#[test]
fn doc_array_elements_pull_one_at_a_time_with_offsets() {
    let (vals, starts, mode) = pull_values("[{\"id\": 1}, {\"id\": 2}]");
    assert_eq!(vals, vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})]);
    assert_eq!(mode, DocMode::Array);
    assert_eq!(starts[0], 1);
    assert!(starts[1] > starts[0]);
}

#[test]
fn doc_scalar_and_nested_elements() {
    let (vals, ..) = pull_values("[1, \"two\", true, null, [1, 2], {\"a\": \"x]y\"}]");
    assert_eq!(
        vals,
        vec![
            serde_json::json!(1),
            serde_json::json!("two"),
            serde_json::json!(true),
            serde_json::Value::Null,
            serde_json::json!([1, 2]),
            serde_json::json!({"a": "x]y"}),
        ]
    );
}

#[test]
fn doc_single_object_pulls_once() {
    let (vals, starts, mode) = pull_values("{\n \"a\": 1\n}");
    assert_eq!(vals, vec![serde_json::json!({"a": 1})]);
    assert_eq!(mode, DocMode::Single);
    assert_eq!(starts, vec![0]);
}

#[test]
fn doc_separator_errors_are_framing_errors() {
    for (input, msg) in [
        ("[1,]", "trailing comma"),
        ("[1 2]", "expected `,` or `]`"),
        ("[1", "unexpected end of input"),
        ("[1] x", "trailing characters"),
    ] {
        let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(input.as_bytes().to_vec())));
        let mut errs = 0;
        loop {
            let r = {
                let mut de = serde_json::Deserializer::from_reader(&mut f);
                serde_json::Value::deserialize(&mut de)
            };
            match r {
                Ok(_) => {}
                Err(e) if e.is_eof() => break,
                Err(_) => {
                    errs += 1;
                    assert!(
                        f.framing_msg.as_deref().is_some_and(|m| m.contains(msg)),
                        "{input}: {:?}",
                        f.framing_msg
                    );
                    break;
                }
            }
        }
        assert_eq!(errs, 1, "{input}");
    }
}

#[test]
fn doc_empty_and_ws_only_modes() {
    let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(Vec::<u8>::new())));
    let r = {
        let mut de = serde_json::Deserializer::from_reader(&mut f);
        serde_json::Value::deserialize(&mut de)
    };
    assert!(r.unwrap_err().is_eof());
    assert_eq!(f.mode, DocMode::Empty);

    let mut f = DocFraming::new(BufReader::new(std::io::Cursor::new(b"  \n".to_vec())));
    let r = {
        let mut de = serde_json::Deserializer::from_reader(&mut f);
        serde_json::Value::deserialize(&mut de)
    };
    assert!(r.unwrap_err().is_eof());
    assert_eq!(f.mode, DocMode::WsOnly);
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select doc_`
Expected: compile failure.

- [ ] **Step 3: Implement (append to framing.rs)**

```rust
/// The document's root shape, resolved lazily on the first read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DocMode {
    Unknown,
    /// Zero bytes of input.
    Empty,
    /// Only whitespace before EOF.
    WsOnly,
    /// Root is an array: elements are served one per pull.
    Array,
    /// Root is any other value: served verbatim, one pull.
    Single,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Start,
    ExpectElement { after_comma: bool },
    ServingElement,
    InjectSpace,
    ExpectSep,
    AfterRoot,
    ServeSingle,
    Done,
}

/// Reshapes a DOCUMENT stream so each serde pull parses exactly one root
/// value. Array mode swallows (and validates) the top-level `[` `]` `,`,
/// injects one virtual space at each element end (a clean terminator for
/// number tokens), and records each element's first-byte offset. Single
/// mode passes the stream through verbatim.
pub(crate) struct DocFraming<R: Read> {
    inner: BufReader<R>,
    offset: u64,
    state: State,
    in_string: bool,
    escape: bool,
    depth: u64,
    pub(crate) mode: DocMode,
    pub(crate) element_start: u64,
    pub(crate) framing_msg: Option<String>,
}

impl<R: Read> DocFraming<R> {
    pub(crate) fn new(inner: BufReader<R>) -> Self {
        Self {
            inner,
            offset: 0,
            state: State::Start,
            in_string: false,
            escape: false,
            depth: 0,
            mode: DocMode::Unknown,
            element_start: 0,
            framing_msg: None,
        }
    }

    fn peek(&mut self) -> io::Result<Option<u8>> {
        Ok(self.inner.fill_buf()?.first().copied())
    }

    fn consume(&mut self, n: usize) {
        self.inner.consume(n);
        self.offset += n as u64;
    }

    fn skip_ws(&mut self) -> io::Result<()> {
        loop {
            // Borrow scope: `avail` borrows `self.inner`, so only scalars may
            // cross out of it — `self.consume` needs `&mut self`.
            let (n, done) = {
                let avail = self.inner.fill_buf()?;
                let n = avail.iter().position(|&b| !ws(b)).unwrap_or(avail.len());
                (n, avail.is_empty() || n < avail.len())
            };
            self.consume(n);
            if done {
                return Ok(());
            }
        }
    }

    fn framing_err(&mut self, msg: &str) -> io::Error {
        self.framing_msg = Some(msg.to_string());
        io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
    }

    /// Resolve the root shape and cache it without consuming the first
    /// significant byte. The handler asks *before* the pull — whether the
    /// trailing probe must run depends on the mode, and once the
    /// `Deserializer` exists it holds `&mut self.framing`, so the mode can no
    /// longer be read. Idempotent: the `Start` arm calls it too, so callers
    /// that never ask still get the mode resolved.
    pub(crate) fn peek_mode(&mut self) -> io::Result<DocMode> {
        if self.mode != DocMode::Unknown {
            return Ok(self.mode);
        }
        self.skip_ws()?;
        self.mode = match self.peek()? {
            None if self.offset == 0 => DocMode::Empty,
            None => DocMode::WsOnly,
            Some(b'[') => DocMode::Array,
            Some(_) => DocMode::Single,
        };
        Ok(self.mode)
    }

    /// Whether the root array has closed cleanly at the top level (the `]` was
    /// consumed). The handler needs this to tell `[]` — the one MISSING row of
    /// the `S3Object[*]` zero-match rule — from an EOF that struck *inside* a
    /// value, which is a malformed document and must error like today's
    /// whole-document `from_slice` did.
    pub(crate) fn closed(&self) -> bool {
        matches!(self.state, State::AfterRoot | State::Done)
    }

    /// Advance the machine after `n` served bytes have been copied out. The
    /// copy itself lives in the caller, inside the borrow scope of `avail`:
    /// `avail` borrows `self.inner`, so no `&mut self` method is callable
    /// while it is live (rustc 1.98.1 reports E0499 for the `serve(out, avail,
    /// …)` shape this replaces).
    fn advance(&mut self, n: usize, next: State) -> usize {
        self.consume(n);
        self.state = next;
        n
    }
}

impl<R: Read> Read for DocFraming<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            match self.state {
                State::Done => return Ok(0),
                State::InjectSpace => {
                    out[0] = b' ';
                    self.state = State::ExpectSep;
                    return Ok(1);
                }
                State::Start => match self.peek_mode()? {
                    DocMode::Array => {
                        self.consume(1);
                        self.state = State::ExpectElement { after_comma: false };
                    }
                    DocMode::Single => {
                        self.element_start = self.offset;
                        self.state = State::ServeSingle;
                    }
                    // Empty / WsOnly end the stream; `Unknown` cannot occur
                    // (peek_mode always resolves it).
                    _ => self.state = State::Done,
                },
                State::ExpectElement { after_comma } => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => {
                            return Err(self.framing_err("unexpected end of input inside the root array"))
                        }
                        Some(b']') if !after_comma => {
                            self.consume(1);
                            self.state = State::AfterRoot;
                        }
                        Some(b']') => return Err(self.framing_err("trailing comma in the root array")),
                        Some(b',') => return Err(self.framing_err("expected an array element")),
                        Some(_) => {
                            self.element_start = self.offset;
                            self.depth = 0;
                            self.state = State::ServingElement;
                        }
                    }
                }
                State::ServingElement => {
                    // Scan and copy live in one borrow scope: `avail` borrows
                    // `self.inner`, and only scalars may cross out of it so the
                    // `&mut self` advance stays callable (E0499 otherwise).
                    // Field writes (`depth`/`in_string`/`escape`/`state`) are
                    // disjoint from `self.inner` and therefore fine in scope.
                    let (n, next) = {
                        let avail = self.inner.fill_buf()?;
                        if avail.is_empty() {
                            // EOF: depth > 0 → serde reports the truncated
                            // value; depth == 0 → the scalar ends and ExpectSep
                            // flags the unterminated array.
                            if self.depth == 0 {
                                self.state = State::ExpectSep;
                            }
                            return Ok(0);
                        }
                        let mut n = 0;
                        let mut next = State::ServingElement;
                        while n < avail.len() && n < out.len() {
                            let b = avail[n];
                            if self.in_string {
                                if self.escape {
                                    self.escape = false;
                                } else if b == b'\\' {
                                    self.escape = true;
                                } else if b == b'"' {
                                    self.in_string = false;
                                }
                                n += 1;
                                continue;
                            }
                            match b {
                                b'"' => {
                                    self.in_string = true;
                                    n += 1;
                                }
                                b'{' | b'[' => {
                                    self.depth += 1;
                                    n += 1;
                                }
                                b'}' | b']' if self.depth > 0 => {
                                    self.depth -= 1;
                                    n += 1;
                                    if self.depth == 0 {
                                        next = State::InjectSpace;
                                        break;
                                    }
                                }
                                b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r' if self.depth == 0 => {
                                    // A scalar element ends BEFORE this byte
                                    // (delimiter left in the stream for
                                    // ExpectSep). n == 0 only for a malformed
                                    // empty element (`[}]`): serving nothing
                                    // lets serde report the truncated value —
                                    // no debug_assert: it would panic debug
                                    // builds on untrusted input.
                                    next = State::InjectSpace;
                                    break;
                                }
                                _ => n += 1,
                            }
                        }
                        out[..n].copy_from_slice(&avail[..n]);
                        (n, next)
                    };
                    return Ok(self.advance(n, next));
                }
                State::ExpectSep => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => {
                            return Err(self.framing_err("unexpected end of input inside the root array"))
                        }
                        Some(b',') => {
                            self.consume(1);
                            self.state = State::ExpectElement { after_comma: true };
                        }
                        Some(b']') => {
                            self.consume(1);
                            self.state = State::AfterRoot;
                        }
                        Some(_) => return Err(self.framing_err("expected `,` or `]` in the root array")),
                    }
                }
                State::AfterRoot => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => self.state = State::Done,
                        Some(_) => return Err(self.framing_err("trailing characters after the JSON document")),
                    }
                }
                State::ServeSingle => {
                    let n = self.inner.read(out)?;
                    self.offset += n as u64;
                    if n == 0 {
                        self.state = State::Done;
                    }
                    return Ok(n);
                }
            }
        }
    }
}
```

Note that in `State::Start` the `Some(b'[')` branch consumes `[` without setting `element_start` — the element offset is recorded when `ExpectElement` enters `ServingElement`.

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select doc_`
Expected: the five tests PASS; the existing suite stays green.

- [ ] **Step 5: Checkpoint**

Report `json/framing.rs` pending commit.

---

### Task 5: matcher.rs — CompiledPath + Skip + CaptureSeed/StepSeed

**Files:**
- Create: `crates/tinio-select/src/json/matcher.rs`
- Modify: `crates/tinio-select/src/json/mod.rs` (add `pub(crate) mod matcher;`)

**Interfaces:**
- Consumes: `crate::path::PathSeg`; `crate::error::Error`; `crate::json::Type`.
- Produces: `pub(crate) struct CompiledPath { from_segments(segments: &[PathSeg], _ty: Type) -> Self, steps() -> &[Step] }` (`ty` is the spec's signature parameter; the matcher does not branch on it, hence the underscore); `pub(crate) enum Step { Name(String), Index(usize), Wild }` (**Amended 2026-09-13**: `Step` has been deleted — it was a field-by-field clone of `PathSeg` with no added behaviour, so `CompiledPath.steps` uses `Vec<PathSeg>` directly and `steps()` returns `&[PathSeg]`); `pub(crate) struct CaptureSeed<'a> { pending: &'a mut VecDeque<Option<Value>> }` (**Amended 2026-09-13**: the original declaration carried a `failure: &'a mut Option<Error>` field; capture only runs after the steps are exhausted and can never record an ambiguity, that field was always unused in the implementation and has been deleted — the handler enters the seed through an unplanned `child_into` instead, see below), `pub(crate) struct StepSeed<'a> { steps: &'a [Step], pending, failure }` (**no `Sink` wrapper**, see the Task 5 note; **no `budget` field** — the cap is armed by the handler per root pull, see Global Constraints), both implementing `serde::de::DeserializeSeed<'de, Value = ()>`; `pub(crate) struct Skip` (validating skip, replacing `IgnoredAny`), implementing `DeserializeSeed` + `Visitor`. Tasks 6/7 depend on this. **(Amended 2026-09-13)** The `_ty: Type` parameter of `from_segments` has been deleted and the signature changed to `from_segments(segments: &[PathSeg])` — the root rule is carried by framing, the matcher does not branch on `Type`, and that parameter was always unused; the code block below and every `Type::Lines`/`Type::Document` argument in Tasks 6/7/8 are void by the same amendment, and the actual code is authoritative.

- [ ] **Step 1: Write the failing test (driven by `Deserializer::from_slice`, independent of framing)**

```rust
fn match_rows(segments: &[PathSeg], input: &str) -> Vec<Option<Value>> {
    let path = CompiledPath::from_segments(segments, Type::Lines);
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(input.as_bytes());
        if path.steps().is_empty() {
            CaptureSeed { pending: &mut out, failure: &mut failure }.deserialize(&mut de)
        } else {
            StepSeed {
                steps: path.steps(),
                pending: &mut out,
                failure: &mut failure,
            }
            .deserialize(&mut de)
        }
    };
    assert!(r.is_ok(), "{input}: {:?}", r.err());
    assert!(failure.is_none(), "{input}: {failure:?}");
    out.into_iter().collect()
}

fn segs(sql_path: &[PathSeg]) -> Vec<PathSeg> { sql_path.to_vec() }

#[test]
fn wild_over_int_scalar_is_missing() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild]),
            "{\"x\": 5}"
        ),
        vec![None]
    );
}

#[test]
fn wild_over_float_scalar_is_missing() {
    // arbitrary_precision: a float is delivered back as the single-key map NUMBER_TOKEN — it must be recognized as a scalar.
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild]),
            "{\"x\": 1.5}"
        ),
        vec![None]
    );
}

#[test]
fn index_over_number_map_is_missing() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Index(0)]),
            "{\"x\": 1e309}"
        ),
        vec![None]
    );
}

#[test]
fn number_token_first_key_object_errors_not_panics() {
    // A real object with the reserved first key is not a number — and must
    // never trip a debug assert (untrusted input). Today's
    // `from_slice::<Value>` rejects it (invalid number); so do we.
    let path =
        CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into())], Type::Lines);
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(
            br#"{"$serde_json::private::Number": 1, "a": 2}"#,
        );
        StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
            .deserialize(&mut de)
    };
    assert!(r.is_err());
    assert!(out.is_empty());
}

#[test]
fn number_token_single_entry_is_a_scalar() {
    // serde_json's documented `arbitrary_precision` collision: the key is
    // reserved, and today's `from_slice::<Value>` turns this object into a
    // number too (value/de.rs:125-129) — a step over it is a zero-match
    // either way.
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild]),
            "{\"x\": {\"$serde_json::private::Number\": \"1.5\"}}"
        ),
        vec![None]
    );
}

#[test]
fn escaped_key_matches_case_insensitively() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("rules".into()), PathSeg::Wild]),
            "{\"R\\u0075les\": [1]}"
        ),
        vec![Some(serde_json::json!(1))]
    );
}

#[test]
fn final_name_holds_value_until_object_close() {
    // Duplicate folded keys: a value deferred for emission must not be enqueued ahead of time, so pending is still empty when the error is reported.
    let path = CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("id".into())], Type::Lines);
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(b"{\"id\": 1, \"ID\": 2}");
        StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
            .deserialize(&mut de)
    };
    assert!(r.is_err());
    assert_eq!(failure, Some(Error::Ambiguous("id".into())));
    assert!(out.is_empty());
}

#[test]
fn non_final_name_duplicate_fails_mid_stream() {
    let path = CompiledPath::from_segments(
        &[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild],
        Type::Lines,
    );
    let mut out = VecDeque::new();
    let mut failure = None;
    let r = {
        let mut de = serde_json::Deserializer::from_slice(b"{\"a\": [1], \"A\": [2]}");
        StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
            .deserialize(&mut de)
    };
    assert!(r.is_err());
    assert_eq!(failure, Some(Error::Ambiguous("a".into())));
    // Deviation ②: lines already produced before the error are retained.
    assert_eq!(out.into_iter().collect::<Vec<_>>(), vec![Some(serde_json::json!(1))]);
}

#[test]
fn zero_match_rules_per_step() {
    // Name miss / Index out of range / Wild over empty / chained MISSING propagation.
    assert_eq!(
        match_rows(&segs(&[PathSeg::Wild, PathSeg::Name("a".into())]), "{\"b\": 1}"),
        vec![None]
    );
    assert_eq!(
        match_rows(&segs(&[PathSeg::Wild, PathSeg::Index(9)]), "[1]"),
        vec![None]
    );
    assert_eq!(
        match_rows(&segs(&[PathSeg::Wild, PathSeg::Wild]), "[]"),
        vec![None]
    );
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild, PathSeg::Name("b".into())]),
            "{\"c\": 1}"
        ),
        vec![None]
    );
}

#[test]
fn nested_wild_order_matches_dfs() {
    assert_eq!(
        match_rows(
            &segs(&[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild, PathSeg::Name("b".into())]),
            "{\"a\": [{\"b\": 1}, {}, {\"b\": 2}]}"
        ),
        vec![Some(serde_json::json!(1)), None, Some(serde_json::json!(2))]
    );
}

#[test]
fn skipped_subtree_still_validates_string_content() {
    // `Skip` replaces `IgnoredAny` precisely so a subtree the path never reads
    // is validated the way today's whole-line `from_slice` validates it: a lone
    // surrogate escape and a raw non-UTF-8 byte must both fail here, and both
    // would have been accepted under `IgnoredAny`.
    for input in [
        &b"{\"b\":\"\\uD800\",\"a\":1}"[..],
        &b"{\"b\":\"\xff\",\"a\":1}"[..],
    ] {
        let path =
            CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into())], Type::Lines);
        let mut out = VecDeque::new();
        let mut failure = None;
        let r = {
            let mut de = serde_json::Deserializer::from_slice(input);
            StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }
                .deserialize(&mut de)
        };
        assert!(r.is_err(), "{input:?} must not be accepted");
        assert!(out.is_empty(), "{input:?}");
    }
}

/// One root pull, with the budget armed and the framing wrapped in
/// `BudgetedRead` exactly as the handlers do it (Task 6/7) — the fix under
/// test is *where* `arm` is called, so the test has to reproduce the whole
/// charging path. An armed budget without `BudgetedRead` is never charged:
/// `fired` would be vacuously false and the boundary assertions below would
/// prove nothing.
fn pull_with_budget(framing: &mut DocFraming<std::io::Cursor<Vec<u8>>>) -> (Vec<Value>, bool) {
    use std::rc::Rc;

    use crate::json::framing::{Budget, BudgetedRead};
    let budget = Rc::new(Budget::default());
    let path = CompiledPath::from_segments(&[PathSeg::Wild], Type::Document);
    let mut out = VecDeque::new();
    let mut failure = None;
    budget.arm();
    {
        let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
            &mut *framing,
            Rc::clone(&budget),
        ));
        let r = if path.steps().is_empty() {
            CaptureSeed { pending: &mut out, failure: &mut failure }.deserialize(&mut de)
        } else {
            StepSeed { steps: path.steps(), pending: &mut out, failure: &mut failure }.deserialize(&mut de)
        };
        let _ = r;
    }
    budget.disarm();
    (out.into_iter().collect(), budget.fired())
}

#[test]
fn element_barely_under_cap_is_accepted() {
    // The boundary that decides where `arm` lives: delivered bytes stay one
    // byte under `MAX_RECORD`, so the record passes only when the budget is
    // armed once for the whole pull. One byte of margin, not zero, because
    // the assertion relies on a serde fact: a string is self-delimiting and
    // nothing past the closing quote is ever read — keep the margin so that
    // fact is not on the critical path. The exact 1 MB line is pinned at
    // the handler level by `document_input_record_cap_exact_allowed`
    // (json.rs:691).
    let content = "a".repeat(MAX_RECORD as usize - 3);
    let (vals, fired) = pull_with_budget(&mut DocFraming::new(BufReader::new(
        std::io::Cursor::new(format!("[\"{content}\"]").into_bytes()),
    )));
    assert!(!fired, "a record of exactly MAX_RECORD delivered bytes must pass");
    assert_eq!(vals.len(), 1);
}

#[test]
fn budget_is_charged_per_root_not_accumulated() {
    // Each root is one pull and one arm: four 400 KB elements total ~1.6 MB,
    // yet no single pull exceeds the cap. A cumulative budget would fire on
    // the second element and reject a document the framing accepts.
    // One array of four string elements — four concatenated arrays would be
    // trailing characters after the first root, not a document.
    let elem = |c: char| format!("\"{}\"", c.to_string().repeat(400 * 1024));
    let doc = format!("[{}, {}, {}, {}]", elem('a'), elem('b'), elem('c'), elem('d'));
    let mut framing = DocFraming::new(BufReader::new(std::io::Cursor::new(doc.into_bytes())));
    let mut total = 0usize;
    loop {
        let (vals, fired) = pull_with_budget(&mut framing);
        assert!(!fired, "the cap must count one root, not the document");
        total += vals.len();
        if vals.is_empty() {
            break;
        }
    }
    assert_eq!(total, 4);
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select matcher`
Expected: compile failure.

- [ ] **Step 3: Implement**

```rust
//! `PathSeg` compiled to a state machine, and the serde `DeserializeSeed`
//! visitors running it over the streaming event flow (`MapAccess` /
//! `SeqAccess` are the events). Unmatched subtrees go through `Skip`, a
//! validating skip that replaces `IgnoredAny`: nothing materializes, and
//! string contents are validated exactly as today's whole-line `from_slice`
//! validates them.

use std::collections::VecDeque;

use serde::{
    Deserialize,
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value;

use crate::{
    error::Error,
    json::Type,
    path::PathSeg,
};

/// serde_json 1.0.151 `src/number.rs:18` (pub(crate) there): with
/// `arbitrary_precision` a non-integer number reaches a custom visitor as a
/// one-key map under this key — detect it and treat the "map" as a scalar.
/// Pinned by `wild_over_float_scalar_is_missing`.
const NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// One traversal step after the root `S3Object[*]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Name(String),
    Index(usize),
    Wild,
}

/// The FROM path, compiled once per reader. The leading `Wild` (the
/// implicit `S3Object[*]` root step) is the framing's job and dropped here,
/// mirroring the old `expand`'s guard.
pub(crate) struct CompiledPath {
    steps: Vec<Step>,
}

impl CompiledPath {
    /// `ty` is the spec's second parameter (spec §PathSeg compiled state machine: `from_segments(segments, ty)`);
    /// the root rule it could encode lives in the framing, so the matcher never
    /// branches on it — hence the underscore.
    pub(crate) fn from_segments(segments: &[PathSeg], _ty: Type) -> Self {
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

/// Per-pull outputs, borrowed field-by-field by both seeds — no `Budget`
/// here: the cap is armed once per root pull by the handler (Global
/// Constraints), not once per matched value, so the seeds carry no
/// budget state at all.
///
/// There is deliberately no `Sink<'a>` wrapper: with a field of type
/// `&'a mut Sink<'a>` the `Child` enum becomes *invariant* over `'a`, so
/// `fn child(&mut self) -> Child<'_>` cannot return a reborrow of the shorter
/// `&mut self` lifetime (rustc 1.98.1: "lifetime may not live long enough");
/// `-> Child<'a>`, an explicit `'b: 'a` bound and the UFCS spelling all fail
/// as well — the bound variant merely moves the error to the caller. Inlining
/// the fields keeps `Child` covariant, which is what makes the recursion
/// compile.
pub(crate) struct CaptureSeed<'a> {
    pub(crate) pending: &'a mut VecDeque<Option<Value>>,
    pub(crate) failure: &'a mut Option<Error>,
}

/// The steps are exhausted: materialize the current value as the record.
impl<'de> DeserializeSeed<'de> for CaptureSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.pending.push_back(Some(Value::deserialize(d)?));
        Ok(())
    }
}

/// Zero-match rule: the container closed with no match — one MISSING row.
fn push_missing(pending: &mut VecDeque<Option<Value>>) {
    pending.push_back(None);
}

/// A second case-folded match inside one object: recorded for the handler and
/// surfaced as a serde error so the pull aborts; the handler's `map_err`
/// prefers the recorded `Error::Ambiguous`.
fn ambiguous<E: serde::de::Error>(failure: &mut Option<Error>, name: &str) -> E {
    *failure = Some(Error::Ambiguous(name.into()));
    E::custom("ambiguous field name")
}

/// A scope step (`steps[0]`) matched against the current value. A container
/// closing with zero matches emits exactly one MISSING row (the zero-match
/// rule); a step over a scalar is a zero-match.
pub(crate) struct StepSeed<'a> {
    pub(crate) steps: &'a [Step],
    pub(crate) pending: &'a mut VecDeque<Option<Value>>,
    pub(crate) failure: &'a mut Option<Error>,
}

enum Child<'a> {
    Capture(CaptureSeed<'a>),
    Step(StepSeed<'a>),
}

impl<'de> DeserializeSeed<'de> for Child<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match self {
            Child::Capture(c) => c.deserialize(d),
            Child::Step(s) => s.deserialize(d),
        }
    }
}

impl<'a> StepSeed<'a> {
    /// The seed for the value selected by the current step: one level
    /// deeper, or the capture once the steps run out.
    fn child(&mut self) -> Child<'_> {
        let rest = &self.steps[1..];
        if rest.is_empty() {
            Child::Capture(CaptureSeed {
                pending: &mut *self.pending,
                failure: &mut *self.failure,
            })
        } else {
            Child::Step(StepSeed {
                steps: rest,
                pending: &mut *self.pending,
                failure: &mut *self.failure,
            })
        }
    }
}

impl<'de> DeserializeSeed<'de> for StepSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for StepSeed<'_> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut matched = false;
        match self.steps.first() {
            None => unreachable!("CaptureSeed handles the exhausted steps"),
            Some(Step::Wild) => {
                while let Some(()) = seq.next_element_seed(self.child())? {
                    matched = true;
                }
            }
            Some(Step::Index(i)) => {
                let target = *i;
                let mut ord = 0usize;
                loop {
                    if ord == target {
                        match seq.next_element_seed(self.child())? {
                            Some(()) => {
                                matched = true;
                                ord += 1;
                            }
                            None => break,
                        }
                    } else {
                        match seq.next_element_seed(Skip)? {
                            Some(()) => ord += 1,
                            None => break,
                        }
                    }
                }
            }
            Some(Step::Name(_)) => {
                // A by-name step over an array is a zero-match — consume.
                while seq.next_element_seed(Skip)?.is_some() {}
            }
        }
        if !matched {
            push_missing(self.pending);
        }
        Ok(())
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // First key read doubles as the number-map check: with
        // arbitrary_precision a non-integer number is a scalar in disguise.
        let key0 = map.next_key::<String>()?;
        if key0.as_deref() == Some(NUMBER_TOKEN) {
            map.next_value_seed(Skip)?;
            // Genuine token maps have exactly one entry (`NumberDeserializer`
            // is synthetic, number.rs:653-677), but a *real* object with this
            // literal first key arrives through the raw `MapAccess` —
            // `deserialize_any` makes no token check at `{`
            // (de.rs:1444-1454) — and collides here. serde_json documents
            // that collision and today's `from_slice::<Value>` rejects such
            // objects (value/de.rs:125-129), so error like today instead of
            // asserting: the input is untrusted and a `debug_assert!` would
            // panic debug builds. A single-entry "token map" is still taken
            // as the number without re-validating the value (today's `Value`
            // re-parses and rejects non-numeric strings — an accepted
            // divergence on a key serde_json itself reserves).
            if map.next_key::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("invalid number-token object"));
            }
            push_missing(self.pending);
            return Ok(());
        }
        match self.steps.first() {
            None => unreachable!("CaptureSeed handles the exhausted steps"),
            Some(Step::Wild) => {
                let mut matched = false;
                let mut has_key = key0.is_some();
                while has_key {
                    map.next_value_seed(self.child())?;
                    matched = true;
                    has_key = map.next_key_seed(Skip)?.is_some();
                }
                if !matched {
                    push_missing(self.pending);
                }
                Ok(())
            }
            Some(Step::Index(_)) => {
                // An index step over an object is a zero-match — consume.
                let mut has_key = key0.is_some();
                while has_key {
                    map.next_value_seed(Skip)?;
                    has_key = map.next_key_seed(Skip)?.is_some();
                }
                push_missing(self.pending);
                Ok(())
            }
            Some(Step::Name(name)) => {
                let mut matched = false;
                let mut held: Option<Value> = None;
                let mut key = key0;
                while let Some(k) = key {
                    if k.eq_ignore_ascii_case(name) {
                        if matched {
                            return Err(ambiguous(self.failure, name));
                        }
                        matched = true;
                        if self.steps.len() == 1 {
                            // Final Name: hold the value until the object
                            // closes — the ambiguity verdict needs the full
                            // member scan (R10 parity, bounded by one value,
                            // which the pull's own budget already caps).
                            held = Some(map.next_value::<Value>()?);
                        } else {
                            map.next_value_seed(self.child())?;
                        }
                    } else {
                        map.next_value_seed(Skip)?;
                    }
                    key = map.next_key::<String>()?;
                }
                if !matched {
                    push_missing(self.pending);
                } else if let Some(v) = held {
                    self.pending.push_back(Some(v));
                }
                Ok(())
            }
        }
    }

    // A step over any scalar is a zero-match — exactly one MISSING row.
    fn visit_bool<E>(mut self, _: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_i64<E>(mut self, _: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_u64<E>(mut self, _: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_f64<E>(mut self, _: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_str<E>(mut self, _: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_string<E>(mut self, _: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_borrowed_str<E>(mut self, _: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_unit<E>(mut self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        push_missing(self.pending);
        Ok(())
    }
}

/// Validating skip for every unmatched subtree, replacing
/// `serde::de::IgnoredAny`.
///
/// `IgnoredAny` materializes nothing, but it also does not fully validate:
/// serde_json's `IoRead::ignore_str` (`read.rs:348-366`) checks escape *shape*
/// and the four hex digits of `\uXXXX`, yet never runs the surrogate-pairing
/// check (`ignore_escape` does not reach `parse_unicode_escape`) and never runs
/// `str::from_utf8` over the raw bytes. A string `from_slice` rejects today — a
/// lone `\uD800`, a raw non-UTF-8 byte — would therefore pass silently inside a
/// subtree the query never reads, which is precisely the validation gap that
/// ruled out the hand-written scanner (spec: "full serde validation first" is the ruling).
///
/// `Skip` takes `deserialize_any` — never `deserialize_ignored_any`, which
/// consumes the value before any visitor runs — so strings arrive as `&str`,
/// get validated, and are dropped. Memory stays O(1): scalars are dropped in
/// place, children recurse with `Skip`, and `from_reader` hands strings over in
/// serde's reusable scratch buffer. Numbers, literals, structure and control
/// characters were already validated; this closes the string-content half.
pub(crate) struct Skip;

impl<'de> DeserializeSeed<'de> for Skip {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Skip {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Keys go through `Skip` too: today's whole-line `from_slice` validates
        // every key, and a skipped key must not become the one exception.
        while map.next_entry_seed(Skip, Skip)?.is_some() {}
        Ok(())
    }

    // Scalars: the visit *is* the validation; the value itself is dropped.
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_borrowed_str<E>(self, _: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_string<E>(self, _: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(())
    }
}
```

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select matcher`
Expected: thirteen tests PASS (including `skipped_subtree_still_validates_string_content`, `element_barely_under_cap_is_accepted`, `budget_is_charged_per_root_not_accumulated`, `number_token_first_key_object_errors_not_panics`). **Do not run the full clippy in this step**: before Tasks 6/7 wire them up, framing and matcher are referenced only from `#[cfg(test)]`; under `--all-targets` the lib target without cfg(test) reports all these `pub(crate)` items as dead_code, so `-D warnings` is bound to go red — the full clippy gate is at Task 8 Step 3.

- [ ] **Step 5: Checkpoint**

Report `json/matcher.rs`, `json/mod.rs` pending commit.

---

### Task 6: lines.rs — Lines<R> handler + LINES test migration

**Files:**
- Create: `crates/tinio-select/src/json/lines.rs`
- Create: `crates/tinio-select/src/json/testing.rs` (test helpers shared across modules)
- Modify: `crates/tinio-select/src/json/mod.rs` (add `pub(crate) mod lines;`, `#[cfg(test)] pub(crate) mod testing;`; move the LINES cases out of its test module)

**Interfaces:**
- Consumes: Task 3 `LineFraming`/`LineStart`, Task 2 `BudgetedRead`/`Budget`, Task 5 `CompiledPath`/`Skip`/`StepSeed`/`CaptureSeed`, `crate::record::RecordReader`, `crate::row::Record`.
- Produces: `pub struct Lines<R: Read>`: `pub fn new(reader: R, from: &FromClause) -> Self`; `impl RecordReader for Lines<R>`. The Task 8 façade depends on it.

- [ ] **Step 0: Move the shared test helpers out of `mod tests` (green first)**

Of the 26 cases moved out (LINES 19 + DOCUMENT 7), 20 reference `read`; the engine composition cases left behind reference `run`/`run_err`/`s`/`raw` (`traversal_and_where_filters` uses `run`/`s`, and a further 16 engine cases use `run`), so the three modules must share them. Create `json/testing.rs` and move these four helpers **verbatim** out of `json/mod.rs`'s `mod tests` (signatures and bodies unchanged to the letter, only the `mod tests` scope dropped): **(Amended 2026-09-13)** the original text said "these five helpers"; four are actually moved in (`run`/`run_err`/`s`/`raw`), and the fifth, `read`, is instead given a thin local wrapper in each of the two handler test modules (`read(sql, input)`, constructing its own `Lines`/`Document` and delegating to `testing::drain`) — the call shape of the roughly 20 moved cases therefore changes from `read(sql, Ty, input)` to `read(sql, input)`, while inputs and assertions are unchanged byte for byte and no case is lost.

```rust
//! Helpers shared by the `json` test modules: `read` (façade-level records),
//! `run`/`run_err` (engine rows / the engine's error) and the `s`/`raw` field
//! constructors. Moved out of `mod tests` verbatim so the migrated cases keep
//! their original call shape.

use std::io::Cursor;

use crate::{
    engine::{Engine, OutRow},
    row::{Field, Record, Value},
    sql::parse,
};

use super::{Params, Reader, Type};

pub(crate) fn read(sql: &str, ty: Type, input: &str) -> Vec<Record> { /* verbatim */ }
pub(crate) fn run(sql: &str, ty: Type, input: &str) -> Vec<OutRow> { /* verbatim */ }
pub(crate) fn run_err(sql: &str, ty: Type, input: &str) -> crate::Error { /* verbatim */ }
pub(crate) fn s(v: &str) -> Field { /* verbatim */ }
pub(crate) fn raw(v: &str) -> Field { /* verbatim */ }
```

`json/mod.rs` gains `#[cfg(test)] pub(crate) mod testing;`; `mod tests` keeps `AWS_EXAMPLE` (only the cases left behind use it) and `use super::testing::{raw, run, run_err, s};` (all four are referenced by the cases left behind — 17 engine cases use `run`, several use `s`, so importing only a subset would fail to compile); the test modules of `json/lines.rs` and `json/document.rs` each `use crate::json::testing::{…};` as needed.

Structural adjustment declaration (spec §Overall Architecture records "shared test helpers" under mod.rs): the helpers are split out into `json/testing.rs` (`#[cfg(test)]`), because 20 moved cases across the two test modules lines/document reference the same set of helpers; an Amendment has been added on the spec side.

Run: `cargo test -p tinio-select`
Expected: all green — a pure move; the new modules do not exist yet.

- [ ] **Step 1: Migrate tests + deviation ④ padding adjustment + new line-tail case (red first)**

Move the following cases **word for word** out of `json/mod.rs`'s test module into `json/lines.rs`'s test module (two construction differences: `Reader::new` → `Lines::new(..., &plan.from)`, and importing the helpers from `crate::json::testing`): `lines_two_objects`, the read-based `traversal_*` (all 13), `lines_final_unterminated_line_over_cap`, `lines_input_record_cap`, `lines_input_record_cap_exact_span_allowed`, `lines_last_record_start`, `empty_line_is_a_format_error`.

Counting basis (16 tests with the `traversal_` prefix measured in json.rs): besides those 13 read-based cases, `traversal_case_insensitive_duplicates_are_ambiguous` and `traversal_bracket_name_duplicates_are_ambiguous` construct `Reader::new` directly and assert `Err(Error::Ambiguous)` — the façade signature is unchanged, so they **stay in mod.rs and pass verbatim**; `traversal_and_where_filters` is an engine composition case (using `run`/`s`) and likewise stays behind.

Deviation ④ changes only two input constructions (assertions verbatim):

```rust
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
```

New (framing and line-tail detection):

```rust
#[test]
fn lines_trailing_garbage_errors() {
    for input in ["{\"a\":1} x\n", "{\"a\":1} {\"b\":2}\n", "123 456\n", "123x\n"] {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Lines::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{input}: {m}"),
            other => panic!("{input}: expected format error, got {other:?}"),
        }
    }
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
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select`
Expected: compile failure (`json::lines` does not exist). At this point the old implementation is still in `mod.rs`, so apart from the cases that moved out the suite should still be green.

- [ ] **Step 3: Implement**

```rust
//! LINES handler: one streaming pull per line — the line is never
//! materialized. Framing lives in `LineFraming`; matching in `matcher`.

use std::{
    collections::VecDeque,
    io::{BufReader, Read},
    rc::Rc,
};

use serde::de::DeserializeSeed;
use serde_json::Value;

use crate::{
    error::Error,
    json::{
        Type,
        framing::{Budget, BudgetedRead, LineFraming, LineStart},
        matcher::{CaptureSeed, CompiledPath, Skip, StepSeed},
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

/// LINES record reader: each line is one root value.
pub struct Lines<R: Read> {
    framing: LineFraming<R>,
    budget: Rc<Budget>,
    path: CompiledPath,
    pending: VecDeque<Option<Value>>,
    failure: Option<Error>,
    last_record_start: u64,
    done: bool,
}

impl<R: Read> Lines<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            // One buffer layer is enough: serde_json's `IoRead` issues one
            // `read` per byte no matter what, and this `BufReader` keeps
            // those calls memory-cheap. A second layer underneath never
            // engages — refills are capacity-sized, and `BufReader::read`
            // bypasses its buffer for requests >= capacity.
            framing: LineFraming::new(BufReader::new(reader)),
            budget: Rc::new(Budget::default()),
            path: CompiledPath::from_segments(&from.segments, Type::Lines),
            pending: VecDeque::new(),
            failure: None,
            last_record_start: 0,
            done: false,
        }
    }

    /// The budget is armed around the whole root pull (see `pull_line`), so
    /// `fired` means "this line delivered more than `MAX_RECORD`"; the
    /// framing's own span check (content + terminator) is the stricter of the
    /// two and usually fires first, so both are mapped to the same message.
    fn map_err(&mut self, e: serde_json::Error) -> Error {
        if self.budget.fired() || self.framing.capped {
            return Error::Format(CAP_MESSAGE.into());
        }
        if let Some(f) = self.failure.take() {
            return f;
        }
        if e.is_io() {
            let kind = e.io_error_kind().unwrap_or(std::io::ErrorKind::Other);
            return Error::from(std::io::Error::new(kind, e.to_string()));
        }
        Error::Format(format!("json input: {e}"))
    }

    /// Parse the next line, filling `pending`; sets `done` at input EOF.
    fn pull_line(&mut self) -> Result<(), Error> {
        match self.framing.start_line() {
            Err(io) => return Err(Error::from(io)),
            Ok(LineStart::Eof) => {
                self.done = true;
                return Ok(());
            }
            Ok(LineStart::Line(start)) => self.last_record_start = start,
        }
        // The cap is armed around the whole pull — one line is one root, so
        // every byte serde is served here (matched, skipped or trailing) is
        // charged, and `disarm` starts the next line from full budget.
        // The Deserializer borrows the framing only inside this block, so
        // the handler inspects it again after the pull (cap flags, offsets).
        self.budget.arm();
        let (root, probe) = {
            let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
                &mut self.framing,
                Rc::clone(&self.budget),
            ));
            let root = if self.path.steps().is_empty() {
                CaptureSeed {
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            } else {
                StepSeed {
                    steps: self.path.steps(),
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            };
            // Trailing probe on the SAME Deserializer: `IoRead` parks a peeked
            // byte inside itself — already consumed from the framing — so a
            // fresh Deserializer would silently swallow it. `Skip` validates
            // the trailing value the way `from_slice` + `end()` used to.
            let probe = root.is_ok().then(|| Skip.deserialize(&mut de));
            (root, probe)
        };
        self.budget.disarm();
        match root {
            Err(e) => Err(self.map_err(e)),
            Ok(()) => match probe.expect("probe runs on a clean root pull") {
                Ok(()) => Err(Error::Format("json input: trailing characters on line".into())),
                Err(e) if e.is_eof() => Ok(()),
                Err(e) => Err(self.map_err(e)),
            },
        }
    }
}

impl<R: Read> RecordReader for Lines<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        loop {
            if let Some(v) = self.pending.pop_front() {
                return Ok(Some(Record::Json(v)));
            }
            if self.done {
                return Ok(None);
            }
            // An error is terminal: latch `done` so a caller that keeps
            // polling after `Err` sees a finished stream, not a retried pull
            // against a broken framing.
            if let Err(e) = self.pull_line() {
                self.done = true;
                return Err(e);
            }
        }
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}
```

Note: `deserialize(&mut de)` goes through `DeserializeSeed`, so `use serde::de::DeserializeSeed;` is required (`Skip` implements only that trait, while the `Value` side goes through `serde::Deserialize`; merge the import list per style.md).

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select`
Expected: all green (including the migrated and new cases).

- [ ] **Step 5: Checkpoint**

Report `json/lines.rs`, `json/testing.rs`, `json/mod.rs` pending commit.

---

### Task 7: document.rs — Document<R> handler + DOCUMENT test migration and new pins

**Files:**
- Create: `crates/tinio-select/src/json/document.rs`
- Modify: `crates/tinio-select/src/json/mod.rs` (add `pub(crate) mod document;`; move the DOCUMENT cases out of the test module), `crates/tinio-select/src/record.rs` (add one ScanRange case), `crates/tinio-select/src/events.rs` (add two event-layer DOCUMENT cases); **(Amended 2026-09-13)** events.rs carries one further **production-side** change not listed in the original plan: `SelectIter::pump` flushes the staged frame before `outbox.fail` — deviation ⑤ (the client receives the Records events before the error event) requires it, otherwise elements already successfully read out before the failure would be dropped. The review recorded this as scope creep, noted here for the record.

**Interfaces:**
- Consumes: Task 4 `DocFraming`/`DocMode`/`peek_mode`, Task 2 `Budget`/`BudgetedRead`, all of Task 5 (**the seed has no `budget` field**; the cap is armed/disarmed by this task at the head and tail of `pull_root`).
- Produces: `pub struct Document<R: Read>`: `pub fn new(reader: R, from: &FromClause) -> Self`; `impl RecordReader for Document<R>` (`last_record_start` = element-level offset, deviation ⑥); `#[cfg(test)] pending_high` (a **byte** high-water mark). The Task 8 façade depends on it.

- [ ] **Step 1: Migrate tests + new pins (red first)**

Move word for word into `json/document.rs`'s test module (construction differences as in Task 6: `Reader::new` → `Document::new(..., &plan.from)`, helpers imported from `crate::json::testing`): `document_single_multiline_object`, `document_root_array_yields_elements`, `empty_document_and_array_yield_nothing`, `document_object_root_is_one_row_object`, `document_input_record_cap`, `document_input_record_cap_exact_allowed`, `document_last_record_start_is_zero`.

`document_last_record_start_is_zero` is **unchanged to the letter** (spec: apart from the two padded inputs of deviation ④, no existing test is touched by a single line) — its input `"{\n \"a\": 1\n}\n"` is a single-value root starting at byte 0, and under the element-level offset rule the assertion is still `0` (checked word for word against `json.rs:719-729`); the pin for element offsets is carried by the new cases below. New:

```rust
#[test]
fn document_array_larger_than_1mb_streams() {
    // Deviation ① main pin: an array document whose total is > 1 MB with a single element < 1 MB streams through.
    let elem = |tag: &str| format!("\"{tag}{}\"", "a".repeat(400 * 1024));
    let doc = format!(
        "[{}, {}, {}, {}]",
        elem("a"), elem("b"), elem("c"), elem("d")
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
    // Deviations ①+⑤: the first element is produced, the second exceeds the cap and errors in-stream.
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
    // Deviation ⑥: true offsets of element starts, including MISSING rows (taking the start of the element they derive from), monotonically non-decreasing.
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
    for input in ["{} x", "{} {}", "5 {}", "5x", "123#"] {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let mut r = Document::new(Cursor::new(input.as_bytes().to_vec()), &plan.from);
        match r.next() {
            Err(crate::Error::Format(m)) => assert!(m.contains("json input"), "{input}: {m}"),
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
    // Table-driven line order: wildcard fan-out interleaved with MISSING (an enumerative reinforcement of apply-order equivalence).
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
```

```rust
#[test]
fn document_unterminated_input_is_a_format_error() {
    // The spec's failure-path list requires this at the handler level (only the
    // framing-level `[1` case exists today). An EOF *inside* a value is a
    // malformed document, never a clean end: `["abc` and `[{"a":` are the cases
    // that would otherwise surface as a clean empty array, and `[1, "abc`
    // pins the deviation-⑤ shape (the first record stands, then the error).
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
        r.pending_high <= 64,
        "pending peaked at {} bytes — an array root buffers one element's matches",
        r.pending_high
    );
}

#[test]
fn document_single_root_over_cap_errors() {
    // Deviation ③'s tier under the per-root cap: the tier is *not* unbounded.
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
    // deviation ③ describes — one pull, every match, in order.
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
        r.pending_high > 0 && r.pending_high <= 1024 * 1024,
        "a single-value root accumulates its match set in one pull (deviation ③), \
         bounded by the record cap: peak {} bytes",
        r.pending_high
    );
}
```

- `record.rs` test module additions (the ScanRange side of deviation ⑥):

```rust
#[test]
fn document_scan_range_filters_elements_by_start() {
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let reader = json::Reader::new(
        Cursor::new(b"[{\"a\": 1}, {\"b\": 2}]".to_vec()),
        json::Params { ty: json::Type::Document },
        &plan.from,
    );
    // Element 2 starts at 11: the window [11, ∞) keeps only it.
    let mut r = RangeFilter::new(reader, ScanRange { start: 11, end: None });
    assert_eq!(
        r.next().unwrap(),
        Some(crate::row::Record::Json(Some(serde_json::json!({"b": 2}))))
    );
    assert_eq!(r.next().unwrap(), None);
}
```

- `crates/tinio-select/src/events.rs` test module gains two cases (deviations ①/⑤ are essentially event-layer behaviour, and the event layer's DOCUMENT coverage was previously zero); reuse the module's existing `run(sql, config, input)` helper:

```rust
#[test]
fn json_document_streams_past_1mb_then_stats() {
    // Deviation ①: an over-1 MB DOCUMENT streams element by element.
    // Deviation ⑥: `bytes_processed` closes on the last element's start, so it
    // is `scanned - 1` for a `[...]`-rooted document — the leading `[` belongs
    // to no record. Pinned deliberately: no existing test covers DOCUMENT byte
    // counters at all, and this value moves with deviation ⑥.
    let elem = |tag: &str| format!("{{\"tag\":\"{tag}\",\"pad\":\"{}\"}}", "a".repeat(400 * 1024));
    let doc = format!("[{},{},{},{}]", elem("a"), elem("b"), elem("c"), elem("d"));
    let scanned = doc.len() as u64;
    assert!(scanned > 1024 * 1024, "the fixture must exceed the old whole-document cap");
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params { ty: json::Type::Document }),
        output: OutputMode::Json(JsonOutputParams::default()),
        ..Default::default()
    };
    let events = run("SELECT * FROM S3Object", config, doc.as_bytes());
    let stats = events
        .iter()
        .find_map(|e| match e {
            Ok(SelectEvent::Stats(c)) => Some(*c),
            _ => None,
        })
        .expect("Stats before End");
    assert_eq!(stats.bytes_scanned, scanned);
    assert_eq!(stats.bytes_processed, scanned - 1);
    let rows: Vec<u8> = events
        .iter()
        .filter_map(|e| match e {
            Ok(SelectEvent::Records(b)) => Some(b.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(rows.iter().filter(|&&b| b == b'\n').count(), 4);
    assert!(matches!(events.last(), Some(Ok(SelectEvent::End))));
}

#[test]
fn json_document_reports_the_cap_after_prior_records() {
    // Deviation ⑤: a DOCUMENT malformation surfaces in-stream, after the records
    // that preceded it — the first element's record stands, then the over-cap
    // element errors the stream out.
    let big = "a".repeat(1024 * 1024);
    let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params { ty: json::Type::Document }),
        output: OutputMode::Json(JsonOutputParams::default()),
        ..Default::default()
    };
    let events = run("SELECT * FROM S3Object", config, doc.as_bytes());
    let first_err = events.iter().position(Result::is_err).expect("a stream error");
    assert!(
        events[..first_err]
            .iter()
            .any(|e| matches!(e, Ok(SelectEvent::Records(_)))),
        "the first element's record must precede the error"
    );
    match &events[first_err] {
        Err(crate::Error::Format(m)) => assert_eq!(m, "input record exceeds 1 MB"),
        other => panic!("expected the reader cap error, got {other:?}"),
    }
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select`
Expected: compile failure (`json::document` does not exist).

(Also noting two assertions tied to the deleted old implementation that are **green** after the implementation — criteria, not red: `document_input_record_cap` (`json.rs:680`, `"` + 1 MB unclosed) and `document_input_record_cap_exact_allowed` (`json.rs:691`, an exactly-1 MB closed string) are both **single-value roots**, and the latter passes only when the budget is "armed exactly once per root pull", right at the 1 048 576/1 048 576 line — which is exactly why the `arm()` position in Step 3 must be outside the `Deserializer`.)

- [ ] **Step 3: Implement**

```rust
//! DOCUMENT handler: one streaming pull per root value (root-array element
//! or the whole single root). Framing lives in `DocFraming`; matching in
//! `matcher`. Record starts are element offsets (deviation ⑥).
//!
//! The 1 MB cap is charged per **root pull** (`Budget::arm` before the
//! `Deserializer` block, `disarm` after it), so the invariant is "one root
//! ≤ 1 MB": an array element, or the whole document in single mode. Because
//! the charge covers every byte serde is served, an *unmatched* oversized
//! element is capped exactly like a matched one — there is no skip path that
//! escapes the cap. Single mode therefore bounds deviation ③ too: a document
//! over 1 MB errors out instead of accumulating its whole match set.
//!
//! Boundary shift from the whole-document rule this replaces: a `[...]`-rooted
//! document that used to be rejected for exceeding 1 MB now streams whenever
//! every element is ≤ 1 MB (deviation ①); a scalar or object root keeps the
//! old rule, since that root *is* the record.

use std::{
    collections::VecDeque,
    io::{BufReader, Read},
    rc::Rc,
};

use serde::de::DeserializeSeed;
use serde_json::Value;

use crate::{
    error::Error,
    json::{
        Type,
        framing::{Budget, BudgetedRead, DocFraming, DocMode},
        matcher::{CaptureSeed, CompiledPath, Skip, StepSeed},
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

/// DOCUMENT record reader: a root array yields one record per element; any
/// other root is a single record.
pub struct Document<R: Read> {
    framing: DocFraming<R>,
    budget: Rc<Budget>,
    path: CompiledPath,
    pending: VecDeque<Option<Value>>,
    failure: Option<Error>,
    roots: u64,
    last_record_start: u64,
    done: bool,
    /// Test-only high-water mark of `pending`, measured in **bytes** of the
    /// buffered `Value`s rather than in records: it is the memory tripwire
    /// that replaces the spec's counting allocator, which
    /// `unsafe_code = "forbid"` rules out. An item count would say nothing
    /// about memory — a single queued 1 MB value and 5 000 tiny ones are the
    /// same number and three orders of magnitude apart in size.
    #[cfg(test)]
    pending_high: std::cell::Cell<usize>,
}

impl<R: Read> Document<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            // One buffer layer — see `Lines::new` for why a second never
            // engages.
            framing: DocFraming::new(BufReader::new(reader)),
            budget: Rc::new(Budget::default()),
            path: CompiledPath::from_segments(&from.segments, Type::Document),
            pending: VecDeque::new(),
            failure: None,
            roots: 0,
            last_record_start: 0,
            done: false,
            #[cfg(test)]
            pending_high: std::cell::Cell::new(0),
        }
    }

    fn map_err(&mut self, e: serde_json::Error) -> Error {
        if self.budget.fired() {
            return Error::Format(CAP_MESSAGE.into());
        }
        if let Some(f) = self.failure.take() {
            return f;
        }
        if let Some(msg) = self.framing.framing_msg.take() {
            return Error::Format(format!("json input: {msg}"));
        }
        if e.is_io() {
            let kind = e.io_error_kind().unwrap_or(std::io::ErrorKind::Other);
            return Error::from(std::io::Error::new(kind, e.to_string()));
        }
        Error::Format(format!("json input: {e}"))
    }

    /// One root pull: StepSeed/CaptureSeed over the framing and — in single
    /// mode — the trailing probe on the **same** `Deserializer`. `IoRead` parks
    /// a peeked byte inside itself, already taken from the framing, so a fresh
    /// `Deserializer` for the probe would silently swallow one trailing byte
    /// (`5x` would be accepted as the record `5`).
    fn pull_root(&mut self) -> Result<(), Error> {
        // Resolved *before* the `Deserializer` exists: whether the probe runs
        // depends on the mode, and afterwards `de` holds `&mut self.framing`,
        // so the mode can no longer be read.
        let mode = match self.framing.peek_mode() {
            Ok(mode) => mode,
            Err(io) => return Err(Error::from(io)),
        };
        // Armed before serde sees the first byte and disarmed after the pull:
        // every byte this root delivers is charged, and the next root starts
        // from a full budget (Global Constraints — charging per matched value
        // would accumulate across a root's matches and reject a document whose
        // elements are each well under the cap).
        self.budget.arm();
        let (root, probe) = {
            let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
                &mut self.framing,
                Rc::clone(&self.budget),
            ));
            let root = if self.path.steps().is_empty() {
                CaptureSeed {
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            } else {
                StepSeed {
                    steps: self.path.steps(),
                    pending: &mut self.pending,
                    failure: &mut self.failure,
                }
                .deserialize(&mut de)
            };
            let probe =
                (mode == DocMode::Single && root.is_ok()).then(|| Skip.deserialize(&mut de));
            (root, probe)
        };
        self.budget.disarm();
        match root {
            Ok(()) => {
                self.roots += 1;
                self.last_record_start = self.framing.element_start;
                if let Some(probe) = probe {
                    self.done = true;
                    match probe {
                        Err(e) if e.is_eof() => {}
                        Ok(()) => {
                            return Err(Error::Format(
                                "json input: trailing characters after the JSON document".into(),
                            ))
                        }
                        Err(e) => return Err(self.map_err(e)),
                    }
                }
                Ok(())
            }
            Err(e) if e.is_eof() => {
                self.done = true;
                match self.framing.mode {
                    // Empty input: zero records, no error (today's behaviour).
                    DocMode::Empty => {}
                    // A root array that closed cleanly: `[]` yields the
                    // S3Object[*] zero-match MISSING row when nothing was
                    // produced; a closed array after elements needs nothing.
                    DocMode::Array if self.framing.closed() => {
                        if self.roots == 0 {
                            self.pending.push_back(None);
                            self.last_record_start = 0;
                        }
                    }
                    // EOF *inside* a value — a truncated element, a truncated
                    // single root, or a whitespace-only document — is a
                    // malformed document. Without this arm `["abc` and
                    // `[{"a":` would be reported as a clean empty array (today
                    // `from_slice` rejects both).
                    _ => return Err(Error::Format(format!("json input: {e}"))),
                }
                Ok(())
            }
            Err(e) => Err(self.map_err(e)),
        }
    }
}

impl<R: Read> RecordReader for Document<R> {
    fn next(&mut self) -> Result<Option<Record>, Error> {
        loop {
            if let Some(v) = self.pending.pop_front() {
                return Ok(Some(Record::Json(v)));
            }
            if self.done {
                return Ok(None);
            }
            // An error is terminal: latch `done`, same as in `Lines::next`.
            if let Err(e) = self.pull_root() {
                self.done = true;
                return Err(e);
            }
            #[cfg(test)]
            {
                // Bytes, not records: `pending`'s memory, not its length.
                let n: usize = self
                    .pending
                    .iter()
                    .flatten()
                    .map(|v| v.to_string().len())
                    .sum();
                if n > self.pending_high.get() {
                    self.pending_high.set(n);
                }
            }
        }
    }

    fn last_record_start(&self) -> u64 {
        self.last_record_start
    }
}
```

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select && cargo test -p tinio-select record`
Expected: all green (including the new pins, the two `pending_high` byte-accounting cases, and the cap boundary of `document_single_root_over_cap_errors`; the new record.rs case runs too).

- [ ] **Step 5: Checkpoint**

Report `json/document.rs`, `json/mod.rs`, `record.rs`, `events.rs` pending commit.

---

### Task 11′: benches/select_scan.rs — JSON DOCUMENT throughput case (**before Task 8**)

**Files:**
- Modify: `crates/tinio-select/benches/select_scan.rs`

**Interfaces:**
- Consumes: the existing `drain`/`WARM_UP`/`MEASURE` (`benches/select_scan.rs:37-38,55`), `events::{InputFormat, SelectConfig}`, `json::{Params, Type}`.
- Produces: the `json_document_scan` bench group, **plus comparable numbers before and after the refactor**.

- **Why before Task 8**: once Task 8 deletes the old implementation, any "before the refactor" number can no longer be obtained. This task's Step 2 must be executed while the old implementation is still in the tree; after Task 8, run the same command once more and write both numbers into Task 8's module comment (that comment has an `<old> → <new>` placeholder waiting).

- [ ] **Step 1: Implement**

- Append inside the file (pattern aligned with the existing `scan_group`; for how `SelectConfig`'s `input_format` field is used, see the test construction at `events.rs:996`):

```rust
/// Benchmark fixture rows (JSON). ~800 KB, deliberately: the doc must stay
/// *under* the 1 MB whole-document cap so the pre-refactor implementation can
/// read it too — a bigger fixture would make the before/after incomparable
/// rather than merely slow.
const JSON_ELEMENTS: usize = 10_000;

/// JSON DOCUMENT scan: an array of small objects, streamed (2026-09-12).
fn json_document_scan(c: &mut Criterion) {
    let mut doc = String::from("[");
    for i in 0..JSON_ELEMENTS {
        doc.push_str(&format!(
            "{}{{\"id\": {i}, \"name\": \"record-{i:08}\", \"price\": {}, \"tags\": [\"alpha\", \"beta-{i:08}\"]}}",
            if i == 0 { "" } else { "," },
            i % 997,
        ));
    }
    doc.push(']');
    let doc = doc.into_bytes();
    let plan = parse("SELECT * FROM S3Object").unwrap();
    // Struct-update form, matching `events.rs:995-1001` — also what keeps
    // `clippy::field_reassign_with_default` quiet under `-D warnings`.
    let config = SelectConfig {
        input_format: InputFormat::Json(json::Params { ty: json::Type::Document }),
        ..Default::default()
    };
    let mut group = c.benchmark_group("json_document_scan");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(WARM_UP));
    group.measurement_time(Duration::from_secs(MEASURE));
    group.throughput(Throughput::Bytes(doc.len() as u64));
    group.bench_function("select_star", |b| {
        b.iter_batched(
            || Box::new(Cursor::new(doc.clone())) as Box<dyn Read + Send>,
            |input| drain(plan.clone(), config.clone(), input),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}
```

- At the imports, append `events::InputFormat`, `json` (merge per style.md); `criterion_group!(benches, full_scan, filtered_scan, json_document_scan);`.

- [ ] **Step 2: Baseline numbers for the old implementation (**must be run before Task 8**)**

Run: `cargo bench -p tinio-select -- json_document_scan`
- Expected: the bench compiles (the old `Reader::new` signature is unchanged, so this file needs no modification to run) and produces baseline numbers. **Write down the `time` and `thrpt` columns**; after Task 8 they are to be compared against the new implementation with the same command.

- [ ] **Step 3: Checkpoint**

- Report `benches/select_scan.rs` pending commit + **the baseline numbers** (for writing into Task 8's module comment).

---

### Task 8: mod.rs façade switch + old implementation deletion

**Files:**
- Modify: `crates/tinio-select/src/json/mod.rs`

**Interfaces:**
- Consumes: Task 6 `lines::Lines`, Task 7 `document::Document`.
- Produces: `pub struct Reader<R: Read>` (`pub fn new(reader: R, params: Params, from: &FromClause) -> Self`, signature unchanged); `impl RecordReader for Reader<R>` delegating variant. Zero changes to `events.rs`/`record.rs`/server call sites.

- [ ] **Step 1: Switch the façade and delete the old implementation**

- `json/mod.rs`: delete the old `Reader` struct and all its methods (`read_line`/`read_document`/`expand`/`apply`/`parse`) along with now-unused imports such as `VecDeque`/`BufRead`; replace with:

```rust
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

pub(crate) mod document;
pub(crate) mod framing;
pub(crate) mod lines;
pub(crate) mod matcher;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod value;

pub(crate) use value::{json_lookup, to_value};

use std::io::Read;

use crate::{
    record::RecordReader,
    row::Record,
    sql::FromClause,
};

// `Type`/`Params` kept verbatim (wire mapping unchanged).

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
    fn next(&mut self) -> Result<Option<Record>, crate::error::Error> {
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
```

- mod.rs test module: keep the engine composition cases (`run`/`run_err`/`s`/`raw`/`AWS_EXAMPLE` and all their tests) and the two ambiguity pins that construct `Reader::new` directly (`traversal_case_insensitive_duplicates_are_ambiguous`, `traversal_bracket_name_duplicates_are_ambiguous`) — the façade `Reader::new` signature is unchanged, so all of them need zero changes. `document_root_array_select_star` looks like a DOCUMENT migration case but is really engine composition (it uses `run`) — it likewise stays behind, so do not migrate it out by prefix.

- [ ] **Step 2: Full suite + post-change numbers for the new implementation**

Run: `cargo test -p tinio-select && cargo bench -p tinio-select -- json_document_scan`
- Expected: all green; the bench produces the **post-switch** numbers. Record them together with the Task 11′ baseline in the "Task 8 measured" block at the end of this section (the `time`/`thrpt` columns, the `select_star` group, the `JSON_ELEMENTS` row) — **do not write them into `mod.rs`'s module docs** (docs/style.md's compress-hard: module docs keep only directional conclusions; raw criterion numbers belong to the plan). The buffer layer is **single** (the second-round revision assumed a double layer; in the third round the std source confirmed the second layer to be pass-through — `BufReader::read` bypasses its own buffer wholesale for requests ≥ its capacity — so it was deleted, and the construction and fields are likewise `LineFraming<R>`/`DocFraming<R>`).

- [ ] **Step 3: clippy + fmt**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo +nightly fmt --all -- --check`
- Expected: clean.

- [ ] **Step 4: Checkpoint**

- Report `json/mod.rs` pending commit.

- **Task 8 measured (criterion, recorded 2026-09-13)**: `json_document_scan/select_star`, fixture 897 751 B (< 1 MB, accepted by the old whole-document path as well), old whole-document façade → new streaming façade: `time` 37.023 ms (CI 34.204–39.186 ms) → 58.620 ms (CI 49.368–65.262 ms); `thrpt` 23.125 MiB/s (CI 21.849–25.031 MiB/s) → 14.605 MiB/s (CI 13.119–17.342 MiB/s). Criterion change line: `time +44.064%` / `thrpt −30.586%` (p = 0.02) — a real regression, not the baseline's ±4.5% run-to-run fluctuation. A repeated run of the same implementation estimated 51.311 ms / 16.686 MiB/s (against the first run's `new` estimate p = 0.99, "no change"), so read direction rather than the point estimate. `mod.rs` module docs keep only the directional conclusion and a pointer to this block.

---

### Task 9: record.rs comments + 2026-09-04 document supersede annotations

**Files:**
- Modify: `crates/tinio-select/src/record.rs:100-106`, `crates/tinio-select/src/record.rs:116-124`, the `crates/tinio-select/src/record.rs` test `document_start_above_zero_yields_nothing`
- Modify: `docs/superpowers/specs/2026-09-04-select-object-content-design.md`
- Modify: `docs/superpowers/plans/2026-09-04-select-object-content.md`

**Interfaces:**
- Consumes: nothing.
- Produces: document consistency (every item of the spec's "synchronized update" section).

- [ ] **Step 1: record.rs comments**

- Replace the `MAX_RECORD` comment (`record.rs:100-104`) with:

```rust
/// AWS input-record cap (design §1, review 2026-09-05 #3): a record over
/// 1 MB errors the stream out — bounds memory, defuses decompression bombs.
/// Measured on the reader's own span accounting: the record's first byte
/// through its terminator (a span that may also include comment lines
/// immediately preceding the record, which the `csv` parser consumes inline).
/// Amended 2026-09-12 (json-streaming): for JSON the cap binds each root
/// (a DOCUMENT array's elements count separately; a single-value root is
/// one record). Amended 2026-09-13 (Q9(a)/Q15): the cap is charged per
/// root PULL — every served byte counts (matched values, skipped subtrees
/// and delimiters alike) — and deviation ③'s tier is NOT outside it: a
/// single-value root's pull is charged like any other, so an over-1 MB
/// document errors instead of accumulating without bound. The memory
/// backstop stays "skip without materializing + one root ≤ 1 MB".
```

- Replace the last sentence of the `RangeFilter` docs (`record.rs:121-124`), "JSON DOCUMENT's whole object is one byte-0 record, so `start > 0` yields nothing — correct, not a bug (spec §Background).", with:

```rust
/// JSON DOCUMENT records report their root element's start (2026-09-12
/// json-streaming, spec D6): the window filters them like any other
/// framing; a single-object root still starts at byte 0.
```

- Test `document_start_above_zero_yields_nothing`: the assertion is untouched; the comment becomes "the single object root starts at byte 0, so `start = 1` still drops it — element-level offsets keep this case (spec D6)".

- [ ] **Step 2: 2026-09-04 design document annotations (keep the original text, append an annotation line, no silent rewriting)**

- `docs/superpowers/specs/2026-09-04-select-object-content-design.md`, three places:

1. Append at the end of the line "JsonReader: LINES (serde_json per line) and DOCUMENT (whole object is one record)…": `(superseded 2026-09-12: DOCUMENT streams root-array elements one record at a time — docs/superpowers/specs/2026-09-12-json-streaming-design.md)`. (The literal text of spec synchronized-update item 1 is "update to streaming per-record … wording"; here, consistently with items 2/3, the original text is kept and an annotation appended, which is a declared deviation in form — an Amendment has been added on the spec side.)
2. Append after the sentence "**ScanRange**: …with JSON `Type=DOCUMENT` the whole object is one record starting at byte 0, so `start > 0` yields **zero records** — correct behavior…": `**Superseded 2026-09-12** (json-streaming deviation ⑥): DOCUMENT records report element-start offsets and the window filters them.`
3. Append after the sentence "**Both input and output records are capped at 1 MB**…": `(input side amended 2026-09-12: the cap binds each record root)`. **(Amended 2026-09-13)** the wording is changed from `each yielded JSON record` to `each record root` — consistent with deviation ①'s own amendment ("each root"): charging per match would miss unmatched over-cap elements.

- `docs/superpowers/plans/2026-09-04-select-object-content.md`: `grep -nE "byte[- ]0" docs/superpowers/plans/2026-09-04-select-object-content.md` (**must also match the hyphen-less spelling**: Task 9's "DOCUMENT … byte-0-anchored but still capped at 1 MB" (line 376) and Task 10's RangeFilter note (line 394, whose original wording is "byte 0", so grepping only "byte-0" would miss it)), and append `(superseded 2026-09-12 — see 2026-09-12-json-streaming-design.md)` to each of the two places.

- [ ] **Step 3: Verify**

Run: `cargo test -p tinio-select record && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- Expected: green; docs warning-free.

- [ ] **Step 4: Checkpoint**

- Report `record.rs` and the two 2026-09-04 documents pending commit.

---

### Task 10: git LFS + large-document fixture + CI streaming test

**Files:**
- Create: `.gitattributes`
- Modify: `.github/workflows/ci.yml:63-64` (the `&checkout` anchor)
- Create: `crates/tinio-select/tests/fixtures/large/document.json.gz` (generated, LFS)
- Create: `crates/tinio-select/tests/stream_large.rs` (the behavior pin + the `#[ignore]` generator in the same file, so `ELEMENTS` is defined in exactly one place)

**Interfaces:**
- Consumes: `crate::record::{decompressed, Compression}`, `json::{Reader, Params, Type}`, `sql::parse`, `flate2` (a regular dependency of tinio-select, usable from integration tests).
- Produces: large-document streaming behavior pins + cap behavior pins in CI (the spec's test-plan items require "assert line count, cap behavior, completion and offset monotonicity").

Background facts (verified): the repository previously had no LFS (no `.gitattributes`, `git lfs ls-files` empty, lfs not enabled in CI); git-lfs 3.7.1 is installed locally, but **the repository has not been repo-local initialized**. All 16 CI jobs share the `&checkout` anchor at `ci.yml:63-64` (only `actions/checkout@v7`, no `with:`), so one `lfs: true` takes effect on every leg — including the windows/macos `test-port` ones; no leg receives a pointer stub. **The memory gate is not part of this task**: the spec's counting `#[global_allocator]` is blocked by `unsafe_code = "forbid"` (covering every target of this crate, including `tests/`) and has been replaced by Task 7's crate-internal `pending_high` high-water pin.

**CI cost note (review addendum)**: `lfs: true` takes effect on all 16 jobs through the shared anchor — legs that do not run this test, such as fmt/lint/docs, pull the fixture too (~1 MB order when gzipped, acceptable; if LFS bandwidth/quota becomes a problem, the fallback is to give the test-family jobs their own checkout with lfs enabled). The behavior pin runs a full ~16 MB byte-by-byte parse on every platform leg (serde_json `IoRead` is `io::Bytes`): the throughput baseline comes first from the **moved-up Task 11′** (per-platform differences can be read from the CI logs, so no separate bench is needed), and **the default remains to run on all platforms** — `#[cfg(target_os = "linux")]` is enabled only if the numbers really prove unacceptable, and enabling that downgrade is a decision that must be explicitly recorded (the cap pin has a small data volume, so every platform keeps it under any circumstances). By user ruling the scale was lowered from 400k/~64 MB to 100k/~16 MB (2026-09-13): pinning streaming of a >1 MB total, the line count and offset monotonicity does not depend on a larger size, and the byte-by-byte parse cost drops to roughly 1/4.

- [ ] **Step 1: .gitattributes**

- Create `.gitattributes`:

```
crates/tinio-select/tests/fixtures/large/** filter=lfs diff=lfs merge=lfs -text
```

- [ ] **Step 2: git lfs install (**must precede the generator in Step 5**)**

```bash
git lfs install
git check-attr filter -- crates/tinio-select/tests/fixtures/large/document.json.gz
```

- Expected: hooks installed; `check-attr` prints `...: filter: lfs`. **This check is the whole of this gate**: while `git lfs install` has not been run, `.gitattributes` is inert, and any `git add` during that window writes the binary permanently into history. Both `git check-attr` and the clean filter of `git add` read the **working tree's** `.gitattributes` — independent of tracking state, effective for untracked files too, so **no early commit is needed** (consistent with "perform no git write operations"). If you would rather not have an agent run it, execute it yourself and paste the output back.

- [ ] **Step 3: Enable lfs in CI**

- Change the `&checkout` anchor in `.github/workflows/ci.yml` to:

```yaml
      - &checkout
        uses: actions/checkout@v7
        with:
          lfs: true
```

- [ ] **Step 4: Create the complete `tests/stream_large.rs` (create the file only, do not run the tests)**

- The complete file (the generator follows the `#[ignore]` precedent of `tests/parquet.rs`, in the same file as the behavior pin, so `ELEMENTS` is defined in exactly one place; the behavior pin cannot run yet — the fixture has not been generated):

```rust
//! Streaming a large DOCUMENT end to end (spec 2026-09-12): row count,
//! completion, monotonic element-start offsets. The fixture is the
//! gzip-compressed ~16 MB synthetic document in git LFS; the read path is
//! the production one (`decompressed(Gzip)`).

use std::{fs::File, io::Cursor};

use tinio_select::{
    json,
    record::{Compression, RecordReader, decompressed},
    row::Record,
    sql::parse,
};

/// Fixture size, shared with `generate_large_fixture` below.
const ELEMENTS: u64 = 100_000;

fn fixture_reader() -> json::Reader<Box<dyn std::io::Read + Send>> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/large/document.json.gz");
    let file = File::open(path).expect("LFS fixture missing — run `git lfs pull`");
    let input = decompressed(Some(Compression::Gzip), Box::new(file));
    let plan = parse("SELECT * FROM S3Object").unwrap();
    json::Reader::new(input, json::Params { ty: json::Type::Document }, &plan.from)
}

#[test]
fn large_document_streams_to_completion() {
    let mut r = fixture_reader();
    let mut count = 0u64;
    let mut last = 0u64;
    while let Some(rec) = r.next().unwrap() {
        assert!(matches!(rec, Record::Json(Some(_))));
        let start = r.last_record_start();
        assert!(start >= last, "record starts not monotonic: {start} < {last}");
        last = start;
        count += 1;
    }
    assert_eq!(count, ELEMENTS);
}

#[test]
fn over_cap_element_errors_after_prior_records() {
    // The spec's cap-behaviour pin on the CI path (inline data, not the LFS
    // fixture). `CAP_MESSAGE` is `pub(crate)`, so the literal is pinned here.
    let big = "a".repeat(1024 * 1024);
    let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = json::Reader::new(
        Box::new(Cursor::new(doc.into_bytes())) as Box<dyn std::io::Read + Send>,
        json::Params { ty: json::Type::Document },
        &plan.from,
    );
    assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
    match r.next() {
        Err(tinio_select::Error::Format(m)) => assert_eq!(m, "input record exceeds 1 MB"),
        other => panic!("expected the reader cap error, got {other:?}"),
    }
}

/// Regenerate the fixture:
/// `cargo test -p tinio-select --test stream_large -- --ignored`.
/// Same provenance shape as `tests/parquet.rs`'s ignored generator.
#[test]
#[ignore = "regenerates the LFS fixture"]
fn generate_large_fixture() {
    use std::io::Write;

    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/large/document.json.gz");
    let file = File::create(path).expect("create fixture");
    let mut gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    gz.write_all(b"[\n").unwrap();
    for i in 0..ELEMENTS {
        write!(
            gz,
            "  {{\n    \"id\": {i},\n    \"name\": \"record-{i:08}\",\n    \"price\": {}.5,\n    \"tags\": [\"alpha\", \"beta-{i:08}\"]\n  }}{}\n",
            i % 997,
            if i + 1 < ELEMENTS { "," } else { "" }
        )
        .unwrap();
    }
    gz.write_all(b"]\n").unwrap();
    gz.finish().unwrap();
}
```

- Note: `json::Reader<Box<dyn Read + Send>>` requires `R: Read` to be satisfied — `Box<dyn Read + Send>` implements `Read` ✓. The imports must include `RecordReader` (`next`/`last_record_start` are trait methods, so failing to import it will not compile); `Cursor` is used by the cap pin (an unused import would be reported under Task 12's clippy `-D warnings`).

- [ ] **Step 5: Run the generator (**only after Step 2's verification passes**)**

Run: `cargo test -p tinio-select --test stream_large -- --ignored`
- Expected: `document.json.gz` generated (~16 MB decompressed, ~1 MB order gzipped).

- [ ] **Step 6: Confirm the fixture is under LFS control**

```bash
git status --short crates/tinio-select/tests/fixtures/large/
git check-attr filter -- crates/tinio-select/tests/fixtures/large/document.json.gz
```

- Expected: the fixture shows as an untracked new file; `check-attr` still returns `filter: lfs`. **Do not add/commit** — the user decides when it enters the repository; confirm these two outputs before adding, otherwise the binary enters git history permanently.

- [ ] **Step 7: Run the tests**

Run: `cargo test -p tinio-select --test stream_large`
- Expected: all green (`generate_large_fixture` is skipped by `#[ignore]`). The memory gate is not in this file — it is Task 7's `document_array_root_keeps_pending_bounded` (byte accounting) / `document_single_root_under_cap_yields_all_matches` / `document_single_root_over_cap_errors` unit tests, which run together with `cargo test -p tinio-select`.

- [ ] **Step 8: Checkpoint**

- Report `.gitattributes`, `ci.yml`, the fixture (LFS), `tests/stream_large.rs` pending commit; remind the user that the first LFS push requires quota. **The order is fixed here in Step 1→2→5** (`.gitattributes` → `git lfs install` + `check-attr` verification → generator); the commit timing is the user's call, but `git check-attr filter -- <fixture>` must be re-run before `git add` to confirm `lfs`.

---

> **(Task 11 has been moved up per the 2026-09-13 review — see the task of the same name before Task 8.)**

---

### Task 12: Acceptance gate

**Files:** none (pure verification).

- [ ] **Step 1: Compilation graph**

Run: `cargo check --workspace --all-targets && cargo hack check --package tinio --each-feature --no-dev-deps && cargo hack check --package tinio-server --each-feature --no-dev-deps`
- Expected: clean.

- [ ] **Step 2: lint**

Run: `cargo +nightly fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings`
- Expected: clean.

- [ ] **Step 3: Unit tests (both graphs + doctest)**

Run: `cargo test --workspace --exclude tinio-e2e && cargo test --doc --workspace --exclude tinio-e2e && cargo test --workspace --exclude tinio-e2e --no-default-features && cargo test --doc --workspace --exclude tinio-e2e --no-default-features`
- Expected: all green.

- [ ] **Step 4: Feature legs**

Run: `cargo test -p tinio-select --features parquet && cargo test -p tinio-server --features select-parquet`
- Expected: all green.

- [ ] **Step 5: doc + bench smoke**

Run: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps && cargo bench -p tinio-select -- --test`
- Expected: clean (`json_document_scan` has already landed in the moved-up Task 11′ and been measured on both sides).

- [ ] **Step 6: e2e select-related legs (where the platform allows)**

Run: `cargo test -p tinio-e2e --test cucumber -- --retry 2` (or the platform-qualified variant in docs/tests.md)
- Expected: all green.

- [ ] **Step 7: Final checkpoint**

- Summarize the complete list of files pending commit in a report to the user: **perform no git write operations.**

---

## Self-Review record

- Spec coverage: module split (T1), parse core/framing (T2–T4), state machine (T5), the two handlers (T6/T7), façade (T8), deviation ① (T7 `document_array_larger_than_1mb_streams` + the events layer's `json_document_streams_past_1mb_then_stats`), ② (T5 `non_final_name_duplicate_fails_mid_stream`), ③ (T7 `document_single_root_under_cap_yields_all_matches` pins its observable semantics, **and per the Q15 revision** becomes "backstopped by the 1 MB record budget", plus `document_single_root_over_cap_errors` pinning the upper bound), ④ (T6's two paddings), ⑤ (T7 `document_element_over_cap_errors_after_prior_records` + the events layer's `json_document_reports_the_cap_after_prior_records`), ⑥ (T7's offset pins + the record.rs ScanRange case + T9), every item of the test plan (T6/T7 + T10, including the added pure-whitespace line, indented line and unclosed document), the memory gate (T7's two `pending_high` cases, **byte accounting**, replacing the spec's counting allocator), cap ownership (T5's two cases + T7's `arm()` position, see below), bench (T11′, **moved up**), synchronized update (T9), delivery and LFS (T10), acceptance commands (T12). No gaps.
- Placeholder scan: no TBD/TODO; every code step contains actual code (the five helpers of `json/testing.rs` are existing code moved verbatim, so only signatures and the module head are listed).
- Type consistency: `Budget::{reset, fired}` (**reset is called by the handler**; the matcher does not hold it; **Amended 2026-09-13**: originally `{arm, disarm, fired}`), `BudgetedRead::new(inner, Rc<Budget>)`, `LineFraming::{new, start_line, capped}`/`LineStart::{Line(u64), Eof}`, `DocFraming::{new, peek_mode, closed, mode, element_start, framing_msg}`/`DocMode::{Unknown, Empty, WsOnly, Array, Single}`, `CompiledPath::from_segments(segments, ty)/steps()`, `Skip`, `CaptureSeed`/`StepSeed` (**two/three fields inline, no `Sink`, no `budget`**), `Lines::new(reader, from)`/`Document::new(reader, from)`, `Document::pending_high` (`#[cfg(test)]`, **byte** accounting) — consistent across task references.
- Known implementation risks (to be checked against the Global Constraints serde_json facts during execution): the NUMBER_TOKEN hard-coding; **a Deserializer created fresh for each pull would lose the byte in `IoRead.ch` that has already been consumed from framing** (so single-value mode's probe must reuse the same one, see T7); `Skip`'s key path depends on `MapKey::deserialize_any` (if a string key does not go through `visit_str`, or a non-string key does not error, the fallback is to change the key side back to `next_key::<String>`); `is_eof`/`is_io`/`io_error_kind` are the only means of error classification.
- **Review revision (2026-09-13 plan review, three rounds of grilling, all implemented as document revisions)**: ① **cap ownership** — `Budget::arm()` was originally called per value inside `CaptureSeed`/the final-step Name (F1: accumulation across captures would falsely report the cap on the second element of a document of "400 KB per element, 1.6 MB in total"), and unmatched subtrees were not armed at all, leaving framing's mid-stream cap with no way to charge either; changed to **the handler calling it once at the head and tail of each root pull** (Task 6/7's `pull_line`/`pull_root`), so the cap semantics become "one root ≤ 1 MB", and the two seeds accordingly drop the `budget` field and all arm/disarm (Task 5). This at the same time lets `document_input_record_cap_exact_allowed` (a single-value root of exactly 1 MB, `json.rs:692`) pass right at the 1 048 576/1 048 576 line. ② **deviation ③ is no longer "unbounded"** (Q15) — the single pull of a single-value root is charged too, so exceeding 1 MB is a cap error; Task 8's module comment, Task 7's tests (`document_single_root_over_cap_errors` / `document_single_root_under_cap_yields_all_matches` replacing the original `document_single_root_fanout_yields_all_matches`) and **the body of spec deviation ③** are revised in step. ③ **`pending_high` switched to byte accounting** (Q2/Q16) — an entry count over 20 000 tiny objects is invariably small and unrelated to memory; changed to the cumulative serialized byte count of the enqueued `Value`s, asserting an array root on the order of ≤ 64 bytes. ④ **double BufReader** (Q3/Q17) — wrap one more layer on the serde side, removing the need to drive `IoRead` byte by byte; after the bench is moved up, the measured numbers decide whether it stays. ⑤ **Task 11 moved up to Task 11′** (Q3/Q17) — the bench must land and produce its baseline **before** Task 8 deletes the old implementation, with a dataset of ~800 KB (< 1 MB, which the old implementation can read too); Task 8's module comment keeps the `<old> → <new>` placeholder. ⑥ **LFS step order** (Q19) — Task 10 is reordered to `.gitattributes` → `git lfs install` + `git check-attr` verification → generator → commit, putting the one irreversible action ("the binary enters history permanently") behind the verification. ⑦ **fact corrections**: `serde` is staged and uncommitted (not "in place"), `unsafe_code = "forbid"` is per-crate (no workspace-level setting), the trailing check is performed by `from_slice` calling `end()` internally (no explicit call on the tinio side), and the large fixture runs on all platforms by default (`#[cfg(target_os = "linux")]` only as a fallback).
- Review revisions (2026-09-12 plan review, all implemented as document revisions): ① three places with the double BufReader wrapper (Task 3 helper return type, Task 6/7 handler fields → `LineFraming<R>`/`DocFraming<R>` — `new(BufReader<R>) -> Self` returning `…<R>` would otherwise be E0308); ② Task 5's full clippy gate moved to Task 8 (`--all-targets` includes the lib target without cfg(test), which necessarily reports dead_code before framing/matcher are wired up); ③ Task 6 Step 0 keeps imports {raw, run_err} → {raw, run, run_err, s}, migration count 29 → 26, traversal accounting spelled out (16 = 13 migrated + 2 ambiguity left behind + 1 engine left behind); ④ Task 10 adds the cap behavior pin required by the spec, plus the `RecordReader` import and the actual use site of `Cursor`; ⑤ Task 9's grep changed to `byte[- ]0` (line 394 is "byte 0" without a hyphen), item 1 declares the deviation in form; ⑥ ServingElement drops `debug_assert!(n > 0)` (malformed input such as `[}]` would panic in a debug build, where release already returns Ok(0) and takes the Format error path); ⑦ IoRead fact correction (read.rs:200-207 / 286-298; byte-at-a-time follows from std `io::Bytes` behavior, and "out.len() == 1" is not the serde_json source's wording); ⑧ Task 10 adds the CI cost note (all 16 jobs pull LFS, fallback for running the 64 MB pin on all platforms).
- Review revisions (2026-09-13 plan review, second round, all implemented as document revisions): ① Task 6/7 field declarations follow the double BufReader (`LineFraming<BufReader<R>>`/`DocFraming<BufReader<R>>` — revision ④ changed only the constructors, which would be E0308); Task 8's "unlayer" note gains the matching field-type sync. ② Task 5's `pull_with_budget` gains the `BudgetedRead` wrapper — as written, the armed budget charged nothing and the two `!fired` assertions held vacuously; the `budget_is_charged_per_root_not_accumulated` fixture changed to a single array of four elements (the original four concatenated arrays would necessarily trigger trailing characters, total == 1). ③ Task 9's `MAX_RECORD` comment synchronized with Q9(a)/Q15 (no longer "YIELDED record"; "One tier sits outside" deleted). ④ Task 10 order fixed: the complete `tests/stream_large.rs` (with `ELEMENTS`/imports) comes before running the generator (the original order ran the generator before the file existed); Step 2's wrong assertion that "`.gitattributes` must already be committed" is deleted (both `check-attr` and `git add` read working-tree attributes, independent of tracking state). ⑤ spec §PathSeg compiled state machine gains an Amendment: `StepSeed` fields, per-value arming and `IgnoredAny` aligned in three places with the 2026-09-13 amendments. ⑥ count and placeholder fixes: Task 5 test count 12→11; the Task 11′/Self-Review placeholder strings unified with Task 8's actual `<old> → <new>`. ⑦ `BudgetedRead::new` drops `impl Into<Rc<Budget>>` (the stated reason does not hold; call sites already use `Rc::clone`). ⑧ `Lines::new`'s buffer-layer comment changed to the correct mechanism (`IoRead` always calls read byte by byte; buffering only lowers the per-call cost; whether the underlying layer stays is decided by Task 8's bench). ⑨ checkpoint omissions filled in: Task 6 gains `json/testing.rs`, Task 7 gains `events.rs`; the Goal line's "reported as soon as the pull ends" becomes "reported mid-pull".
- Review revisions (2026-09-13 plan review, third round; external review record `.workbuddy/memory/2026-09-13.md`, all implemented as document revisions): ① **the buffer layer reverts to single** (overturning revision ④'s double-layer decision) — returning to the std source confirmed it: `BufReader::read` bypasses its own buffer wholesale for requests ≥ its capacity, and framing's own buffered refill is exactly the capacity size, so the second layer against the raw reader is pass-through and never takes effect; `Lines`/`Document`'s construction and fields return to a single layer (`LineFraming<R>`/`DocFraming<R>`), with Task 8 Step 2 and the module comments synchronized. (The three blockers the external review listed — E0308, the empty `pull_with_budget` assertions, the fixture concatenation — target the snapshot from before the second-round revision and were all fixed in the second round.) ② `element_barely_under_cap_is_accepted` margin 0 → 1 byte (content changed to `MAX_RECORD - 3`): the assertion relies on "the string self-terminates, nothing is read past the closing quote", and with the margin that fact leaves the critical path; the exactly-1 MB boundary belongs to the handler-level `document_input_record_cap_exact_allowed` (json.rs:691). ③ error-path closure: both `next()`s latch `done = true` when `pull_*` errors, so polling after the `Err` sees a terminated stream instead of retrying the pull. ④ the "every byte is charged" wording made precise (Architecture/Global Constraints/the two comments in Task 2/spec §Framing): the top-level `[`/`]`/`,` and the LINES `\n` swallowed inside framing do not pass through `BudgetedRead` and are accounted for by the framing's span/offset accounting. ⑤ spec gains two Amendments: §Parse core `next_key::<IgnoredAny>()` → `next_key_seed(Skip)`; test plan `document_element_over_cap_errors` renamed `…_after_prior_records` to match the implementation. ⑥ cited line numbers corrected: the json.rs:681/692 in Task 7's note → 680/691; the `docs/cargo.md:10 → :9` raised by the external review **does not hold** on inspection (the per-crate `unsafe_code` clause is indeed at cargo.md:10), so the original citation is kept. ⑦ the CI fixture scale is lowered by user ruling: 400k elements/~64 MB → 100k elements/~16 MB (`ELEMENTS`, Step 5 Expected, the CI cost note and the spec's test plan/delivery sections synchronized) — pinning line count, cap behavior, completion and offset monotonicity does not depend on a larger size, and the byte-by-byte parse cost drops to roughly 1/4.
- Review revisions (2026-09-13 plan review, fourth round, all implemented as document revisions): ① `visit_map`'s NUMBER_TOKEN branch drops `debug_assert!` — serde_json routes only genuine number tokens through `NumberDeserializer` (de.rs:127, 938-953), and `deserialize_any`'s `{` branch does no special-casing (de.rs:1444-1454), so a real object whose first key is the reserved key arrives through the raw `MapAccess` and, with multiple keys, the assert would panic in a debug build (untrusted input); changed to an explicit `Error::custom` — consistent with today's `from_slice::<Value>` semantics of erroring on that class of object (value/de.rs:125-129 runs `KeyClassifier` then `NumberFromString`, erroring on both non-string values and multiple keys); a single-key "token map" is still treated as a number (serde_json's documented collision for `arbitrary_precision`, which today misjudges likewise), and the value is not re-validated as a numeric string (today it would error, a recorded micro-deviation on the reserved key). Two tests added: multiple keys error without panicking, single key is treated as a scalar with zero matches; Task 5's test count 11 → 13. ② charging wording made precise again: the uncharged bytes include the whitespace swallowed by `skip_ws` (not only top-level delimiters and `\n`) — Architecture/Global Constraints/the two comments in Task 2/spec §Framing Amendment synchronized. ③ Task 8's deviation ② comment tightened: already-produced rows stay in `pending`, and the event layer stops at the first error without yielding further. ④ Task 8's left-behind list names `document_root_array_select_star` (its name looks like a DOCUMENT case but it is really engine composition).
- Review corrections (2026-09-13 after the review, fifth round; audit pin HEAD `5e7ee2a`, diff = all staged changes): ① **shared core** — the two handlers' twin structures (fields, `new()`, the `next()` loop, the pull skeleton, ~80 lines) are extracted into `json/core.rs`'s `Core`: holding `budget`/`path`/`pending`/`failure`/`last_record_start`/`done`, sharing `pull()` (reset the budget + build the `Deserializer` + `child_into` + same-source probing) and `advance()` (the `next()` loop and the terminal-error rules). `Lines`/`Document` keep only their own framing and `pull_line`/`pull_root` — the latter are free functions (`&mut Core` + `&mut DocFraming` + `&mut produced`), because a closure version and `&mut self` conflict over the borrow on `core.advance`. `pending_high` moves into `Core` along with `pending` (`#[cfg(test)]`), and test references become `r.core.pending_high`. ② **`Step` deleted** (see the Task 5 entry). ③ **ambiguity predicate shared** — a new `json::value::key_matches(key, name, style)`, used by both `json_lookup` and `StepSeed`'s Name branch (the latter passing `NameStyle::Bare`), delivering on R10's "the two must not drift". ④ **fixture generator shared** — a new `tests/support/json_fixture.rs` (`#![allow(dead_code)]`, each binary uses one of its items): `element_compact`/`element_pretty` produce the shape from one source (`record-{i:08}`, `beta-{i:08}`, `i % 997`), while the layout and the `price` decimal places are kept per consumer (the bench must be < 1 MB, the test pretty-prints). **Verified**: regenerating the fixture with `cargo test --test stream_large -- --ignored` is byte-for-byte identical (sha256) to the committed version. ⑤ **test pin loosened** — `document_single_root_trailing_garbage_errors` no longer pins serde's literal message (`expected value at line 1 column 4` etc.), asserting instead "it is a `json input` error, and whether it is our own trailing-characters message": this keeps the discriminative power the original comment wanted while removing the fragility that a serde_json upgrade would trip. ⑥ **stale comment cleanup** — the residual `Budget::arm`/`disarm`/`armed` wording in `document.rs`'s module docs and two `matcher.rs` comments is changed to `reset` (the protocol was simplified to a single `reset()` on 2026-09-13). ⑦ **authorization and record for unauthorized changes made during implementation** (confirmed with the user on 2026-09-13): (a) `Skip::visit_map`'s NUMBER_TOKEN probe (`TokenProbe`, comparing inside `visit_str`, no allocation) + one new test, filling in reserved-key validation for skipped subtrees so that Skip and the traversal path agree; (b) four unplanned tests `lines_error_is_terminal_for_later_polls`, `lines_final_unterminated_line_under_cap_is_one_record`, `document_error_is_terminal_for_later_polls`, `document_wild_over_object_collapses_duplicate_keys`; (c) `mod.rs`'s `pub(crate) fn json_error` (the original plan had this logic inlined in the two handlers' `map_err`); (d) `LineFraming::read`'s newline scan window narrowed from the whole `avail` to `out.len() + 1` — the original implementation scanned the entire buffer for every byte served, O(L × min(L, 8 KiB)) for a line of length L; after narrowing it is byte-for-byte equivalent (the `\n` at `out.len()` still terminates the line), and the tinio-select lib test time went 4.98 s → 0.61 s; (e) `ServingElement`'s EOF/`n == 0` path refactored relative to the plan's code block. ⑧ **authorization for Wild-over-object duplicate-key folding**: when a Wild step lands on an object, buffering is grouped by key, duplicate keys are last-wins and keep the position of the first occurrence — reproducing the folding semantics of the deleted old pipeline `from_slice::<Value>` (`preserve_order`'s IndexMap); this is fixing a deviation rather than adding a feature, pinned by three tests. ⑨ **deviation ③ wording** aligned (see the "synchronized update" section).
