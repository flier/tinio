# JSON Streaming Reader Implementation Plan (struson kernel)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the parsing kernel of `crates/tinio-select/src/json/` from serde_json with **struson 0.7.2's `JsonStreamReader`**: bytes are consumed by the parser from `Read` on demand, and the whole flow never reads anything in full and never copies the document wholesale; matching is still the hand-written explicit-stack stepper, and unmatched subtrees go through `skip_value()` (validating, not materializing, not buffering); the 1 MB cap is decided exactly by a `data_pos` difference over each root value's span. The `json::Reader::new` signature, the `RecordReader` contract, the `Record::Json(Option<Value>)` model, and the engine-side `to_value` and `json_lookup` all stay unchanged.

**Architecture:** `JsonStreamReader<LineFraming<R>>` (LINES) and `JsonStreamReader<R>` (DOCUMENT) are the only parsers; `matcher.rs` defines the `ValueSource` trait (the real implementation wraps `JsonStreamReader`, the tests use a scripted stand-in) and holds the pausable explicit-stack state machine; `feed.rs` does nothing but per-root byte accounting; the two handlers each compose them. `framing.rs` is deleted (`BudgetedRead` and `DocFraming` become void, `LineFraming` moves to `line.rs`), and `core.rs` is split by behaviour under new names.

**Tech Stack:** Rust 2024 (edition.workspace), `struson 0.7` (new), `serde_json 1.0` (`arbitrary_precision` and `preserve_order`, only as the record value model), `thiserror 2.0` (already present).

**Spec:** `docs/superpowers/specs/2026-09-12-json-streaming-struson-design.md` (12 verified external facts, deviations ① to ⑬, cap sampling-point table). **It must be read alongside this plan during execution**; every why in this document points at a section of that spec and is not restated.

## Global Constraints

- **No git write operations of any kind** (CLAUDE.md overrides the skill's commit step): each task ends with "Checkpoint: report the list of files pending commit", and **does not run** `git add` or `git commit`. The working tree is allowed to be dirty.
- Code, comments, tests, and commit messages are English throughout. This plan was originally written in Chinese under an explicit user exemption; it was translated to English on 2026-09-15 at the user's request, which ends that exemption.
- **The only permitted dependency change**: add `struson = "0.7"` to `[workspace.dependencies]` in the root `Cargo.toml`, and add `struson.workspace = true` to `[dependencies]` in `crates/tinio-select/Cargo.toml`. Vendoring, patching, and changing registry configuration are **forbidden**.
- `unsafe_code = "forbid"` is a per-crate setting; struson is entirely safe, and this crate adds no new `unsafe`.
- **Only the locally vendored source counts** as the basis for struson facts (`struson-0.7.2/` under `~/.cargo/registry/src/`). Both docs.rs `latest` and GitHub `main` are 0.8-dev (different field names and behaviour), and **must not** be used to change fields or assertions.
- **Never `peek()` again once the top-level value has been consumed**: struson panics (`stream_reader.rs:743-747`, spec fact 4). Every trailing check goes through `consume_trailing_whitespace`.
- `peek()` does not change `data_pos` (spec fact 3): **the value's first byte = the `pos()` after `peek()`, with no compensation term of any kind**. Any form of minus one is a bug.
- Memory discipline: the whole document or a whole line must never be held at any moment. The only things that may be held are: struson's fixed 1 KiB read buffer, the sentinel buffer of the current token, the single matched value the matcher holds back for the final-step Name, the current batch's match set in `pending`, and the current record being materialized.
- **A batch boundary = when one matched value completes** (the operative reading of Q18=b, revised 2026-09-13): `Matcher::on_value` returns `Progress::StopRoot` as soon as it has enqueued one record (MISSING included), and `next_root` passes that through unchanged; the handler then immediately does the cap check and delivers this one record. So **one delivery = one record**, the upper bound of `pending` is 1 record and independent of root size, and `pending_high` is naturally bounded (this is exactly what Task 5's new case pins). A large scalar token is the only exception, already recorded as spec deviation ⑦.
- **Root completion and "fully read" are two different things**: `next_root` returns `End` only on the **loop after the root completes**, and only because the in-flight `pending` is empty; when a single-value root completes within the same pull, the cap and trailing checks must be finished first and only then may it return — see the ordering rule in Task 7.
- Existing tests: apart from those this plan names (the matcher driver rewrite in Task 3, the deviation points in Task 9) **not one line is touched**; an existing assertion that turns red is an implementation bug, not a reason to change the test.
- Documentation style (`docs/style.md`): compressed prose, imports de-qualified at 3+ segments, errors through the per-crate `Error`.

### Verified interface shapes (copy them verbatim during execution, do not infer anything further)

```rust
// ---- matcher.rs ----
pub(crate) enum Step { Name(String), Index(usize), Wild }
pub(crate) struct CompiledPath { /* unchanged */ }
impl CompiledPath {
    pub(crate) fn from_segments(segments: &[PathSeg]) -> Self;  // the existing signature takes one argument, do not add Type back
    pub(crate) fn steps(&self) -> &[Step];                                 // unchanged
}

/// Value kinds (a local mirror of struson's ValueType, so the test stand-in does not depend on struson)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueKind { Array, Object, String, Number, Bool, Null }

/// The single minimal seam between the matcher and the byte source. Errors are folded into crate::Error at this layer.
pub(crate) trait ValueSource {
    fn peek(&mut self) -> Result<ValueKind, Error>;
    fn has_next(&mut self) -> Result<bool, Error>;
    fn name(&mut self) -> Result<Cow<'_, str>, Error>;   // next_name: borrowed, no per-member copy
    fn text(&mut self) -> Result<String, Error>;      // next_string
    fn number(&mut self) -> Result<String, Error>;    // next_number_as_str().to_owned()
    fn boolean(&mut self) -> Result<bool, Error>;
    fn null(&mut self) -> Result<(), Error>;
    fn begin_object(&mut self) -> Result<(), Error>;
    fn end_object(&mut self) -> Result<(), Error>;
    fn begin_array(&mut self) -> Result<(), Error>;
    fn end_array(&mut self) -> Result<(), Error>;
    fn skip_value(&mut self) -> Result<(), Error>;    // unmatched subtree: validate, do not materialize
    // No position member: `current_position(false).data_pos` is the handlers' own free function.
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress { StopRoot, End }   // the private step: None = still in flight

pub(crate) struct Matcher { /* steps, frames, pending, done */ }
impl Matcher {
    pub(crate) fn new(path: CompiledPath) -> Self;
    /// Advance one step: value events go to the state machine, unmatched subtrees are skipped by depth.
    /// `None` = still in flight, the one state a caller must never see as a `Progress`.
    pub(crate) fn on_value(&mut self, kind: ValueKind, source: &mut dyn ValueSource)
        -> Result<Option<Progress>, Error>;
    /// Advance one root: read values repeatedly until the root completes, the source ends, or a delivery is needed.
    pub(crate) fn next_root(&mut self, source: &mut dyn ValueSource)
        -> Result<Progress, Error>;
    pub(crate) fn pending_len(&self) -> usize;
    pub(crate) fn take_record(&mut self) -> Option<Record>;   // deliver one (MISSING included)
    #[cfg(test)] pub(crate) fn pending_high(&self) -> usize;  // deviation 12: high-water mark in bytes
}
```

```rust
// ---- feed.rs ----
/// Per-root byte accounting. Single source: `position()` (that is, data_pos).
pub(crate) struct RootBudget { start: u64, cap: u64, fired: bool }
impl RootBudget {
    pub(crate) fn new(cap: u64) -> Self;
    /// Record the root's start (call it right after peek).
    pub(crate) fn begin(&mut self, pos: u64);
    /// Batch-boundary check: a span exceeding the cap sets fired.
    pub(crate) fn check(&mut self, pos: u64) -> bool;   // true = within the cap
    pub(crate) fn fired(&self) -> bool;
}
```

> **(revised during implementation)**: `RootBudget` also carries `begin_element(pos)` (opens an array element's own span, superseding the root origin) and `check_root_span(pos) -> Result<(), Error>` (the document-level closing check, a no-op once an element origin is set), and every sample site reports its verdict through the `check_or_cap(pos) -> Result<(), Error>` wrapper; the fields are private internals shared with the charged `Read` backstop through an `Rc`, not `start`/`cap`/`fired`.

```rust
// ---- json/error.rs (new) ----
#[derive(Debug, Error)]
pub(crate) enum Error {
    Syntax(#[from] JsonSyntaxError),
    UnexpectedType { expected: ValueType, actual: ValueType, location: JsonReaderPosition },
    Structure { kind: UnexpectedStructureKind, location: JsonReaderPosition },
    NestingDepth { max_nesting_depth: u32, location: JsonReaderPosition },
    Number { number: String, location: JsonReaderPosition },
    InvalidUtf8 { location: JsonReaderPosition },
    Io { #[from] error: std::io::Error, location: JsonReaderPosition },
    Cap,
}
impl PartialEq for Error { /* hand-written: compare by variant */ }
```

---

### Task 1: Pre-refactor baseline (throughput of the serde streaming kernel)

**Files:**
- Modify: `crates/tinio-select/src/json/mod.rs` (append one paragraph of measured data to the module docs only)
- Test: none (a measurement task)

**Interfaces:**
- Consumes: the serde_json kernel currently runnable in the working tree, and `json_document_scan` from `benches/select_scan.rs`.
- Produces: the baseline numbers Task 11 compares against.

- [ ] **Step 1: Run the baseline**

Run: `cargo bench -p tinio-select --bench select_scan -- json_document_scan`
Expected: numbers come out. Record criterion's `time` and `thrpt` lines (with confidence intervals).

- [ ] **Step 2: Write it into the module docs**

Append one paragraph to the module docs of `json/mod.rs` (replace the numbers with the measured values from Step 1, do not copy the placeholders):

```rust
//! Throughput baseline: the serde_json streaming kernel this module replaces
//! measured json_document_scan at <time> (<lo>-<hi>) / <thrpt> (<lo>-<hi>)
//! on <date>, <machine>. The struson kernel must not be slower than this
//! (acceptance threshold, struson design spec, acceptance criteria).
```

- [ ] **Step 3: Confirm the starting state**

Run: `cargo test -p tinio-select`
Expected: all green. This is the starting state of the refactor; if it is already red, **stop and report** — do not start work on a red baseline.

- [ ] **Step 4: Checkpoint**

Report: `json/mod.rs` pending commit. **Do not run commit.**

---

### Task 2: Land the dependency + version guard tests

**Files:**
- Modify: `Cargo.toml` (the `[workspace.dependencies]` table, alphabetical order)
- Modify: `crates/tinio-select/Cargo.toml` (the `[dependencies]` table, alphabetical order)
- Create: `crates/tinio-select/src/json/struson_contract.rs` (version guard tests)
- Modify: `crates/tinio-select/src/json/mod.rs` (add one line `#[cfg(test)] pub(crate) mod struson_contract;`)

**Interfaces:**
- Consumes: nothing.
- Produces: a `struson` dependency usable across the whole crate; a guard that pins spec facts 1, 3, 4, 5, 6, 9 and 10 as tests.

- [ ] **Step 1: Add the two dependency lines**

In the root `Cargo.toml`'s `[workspace.dependencies]`, insert after the `sqlparser` line:

```toml
struson = "0.7"
```

In `crates/tinio-select/Cargo.toml`'s `[dependencies]`, insert after the `sqlparser` line:

```toml
struson.workspace = true
```

- [ ] **Step 2: First confirm it compiles**

Run: `cargo check -p tinio-select`
Expected: success; `Cargo.lock` gains `struson`, `duplicate`, `strum` (and its `strum_macros`); `thiserror 2` is already among this crate's dependencies and is not added. **Note**: `btoi` and `num-traits` are actson's dependencies and are not introduced by this kernel.

- [ ] **Step 3: Write the version guard tests**

`crates/tinio-select/src/json/struson_contract.rs`:

```rust
//! Contract tests pinning the struson 0.7.2 behaviours this crate depends on.
//! They drive JsonStreamReader directly, not our wrappers, so an upstream
//! upgrade fails here first with a clear name.
//!
//! Source of truth: struson 0.7.2 as vendored in the local cargo registry.
//! docs.rs latest and GitHub main are 0.8-dev and differ.
//! See the design spec, verified facts 1/2/3/4/5/6/9/10.

use std::io::Read;

use struson::reader::{JsonReader, JsonStreamReader, ReaderSettings, SyntaxErrorKind};

fn pos<R: Read>(reader: &JsonStreamReader<R>) -> u64 {
    reader
        .current_position(false)
        .data_pos
        .expect("JsonStreamReader always reports data_pos")
}

fn location() -> struson::reader::JsonReaderPosition {
    JsonStreamReader::new(b"".as_slice()).current_position(false)
}

/// Fact 1: the 0.7.2 field set and its defaults.
#[test]
fn settings_defaults_match_the_pinned_facts() {
    let settings = ReaderSettings::default();
    assert!(!settings.allow_comments);
    assert!(!settings.allow_trailing_comma);
    assert!(!settings.allow_multiple_top_level);
    assert!(settings.track_path, "0.7.2 default is true");
    assert_eq!(settings.max_nesting_depth, Some(128));
    assert!(settings.restrict_number_values, "0.7.2 default is true");
}

/// Fact 3: peek() never advances data_pos, whatever the value kind.
#[test]
fn peek_never_moves_data_pos() {
    for json in ["{\"a\":1}", "[1,2]", "\"hi\"", "12.0e5", "true", "false", "null"] {
        let mut reader = JsonStreamReader::new(json.as_bytes());
        let before = pos(&reader);
        reader.peek().expect("peek");
        assert_eq!(pos(&reader), before, "{json}: peek moved data_pos");
    }
}

/// Fact 4: consume_trailing_whitespace is the only safe trailing probe.
#[test]
fn trailing_data_is_reported_by_consume_trailing_whitespace() {
    for json in ["{} x", "5 {}", "[1] x"] {
        let mut reader = JsonStreamReader::new(json.as_bytes());
        reader.skip_value().expect("root value");
        match reader.consume_trailing_whitespace() {
            Err(struson::reader::ReaderError::SyntaxError(e)) => {
                assert_eq!(e.kind, SyntaxErrorKind::TrailingData, "{json}");
            }
            other => panic!("{json}: expected TrailingData, got {other:?}"),
        }
    }
    let mut reader = JsonStreamReader::new(b"{} ");
    reader.skip_value().expect("root value");
    reader
        .consume_trailing_whitespace()
        .expect("trailing whitespace is clean");
}

/// Fact 9: a literal followed by a stray byte fails during the value read.
#[test]
fn trailing_data_after_literal_fails_at_the_value() {
    let mut reader = JsonStreamReader::new(b"truey".as_slice());
    match reader.next_bool() {
        Err(struson::reader::ReaderError::SyntaxError(e)) => {
            assert_eq!(e.kind, SyntaxErrorKind::TrailingDataAfterLiteral);
        }
        other => panic!("expected TrailingDataAfterLiteral, got {other:?}"),
    }
    let mut reader = JsonStreamReader::new(b"nullx".as_slice());
    match reader.next_null() {
        Err(struson::reader::ReaderError::SyntaxError(e)) => {
            assert_eq!(e.kind, SyntaxErrorKind::TrailingDataAfterLiteral);
        }
        other => panic!("expected TrailingDataAfterLiteral, got {other:?}"),
    }
}

/// Fact 10: empty and whitespace-only inputs both fail at the first peek.
#[test]
fn empty_and_whitespace_only_fail_at_peek() {
    for json in ["", "   \n  "] {
        let mut reader = JsonStreamReader::new(json.as_bytes());
        match reader.peek() {
            Err(struson::reader::ReaderError::SyntaxError(e)) => {
                assert_eq!(e.kind, SyntaxErrorKind::IncompleteDocument, "{json:?}");
            }
            other => panic!("{json:?}: expected IncompleteDocument, got {other:?}"),
        }
    }
}

/// Fact 6: invalid UTF-8 arrives as an IoError whose kind is InvalidData.
#[test]
fn invalid_utf8_is_an_invalid_data_io_error() {
    let bytes = b"{\"a\":\"\xff\xfe\"}".to_vec();
    let mut reader = JsonStreamReader::new(&bytes[..]);
    reader.begin_object().expect("begin object");
    reader.next_name_owned().expect("name");
    match reader.next_string() {
        Err(struson::reader::ReaderError::IoError { error, .. }) => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), "invalid UTF-8 data");
        }
        other => panic!("expected IoError/InvalidData, got {other:?}"),
    }
}

/// Fact 5: a lone surrogate is rejected on the skipped path too.
#[test]
fn skipped_subtree_rejects_a_lone_surrogate() {
    let json = "{\"b\":\"\\uD800\",\"a\":1}";
    let mut reader = JsonStreamReader::new(json.as_bytes());
    reader.begin_object().expect("begin object");
    assert_eq!(reader.next_name_owned().expect("name"), "b");
    match reader.skip_value() {
        Err(struson::reader::ReaderError::SyntaxError(e)) => {
            assert_eq!(e.kind, SyntaxErrorKind::UnpairedSurrogatePairEscapeSequence);
        }
        other => panic!("expected an unpaired-surrogate error, got {other:?}"),
    }
}

/// Fact 2 (spot check): current_position(false) reports no path.
#[test]
fn current_position_without_path_is_available() {
    let reader = JsonStreamReader::new(b"{}".as_slice());
    let position = reader.current_position(false);
    assert!(position.path.is_none());
    assert!(position.line_pos.is_some());
    assert_eq!(position.data_pos, Some(0));
}
```

- [ ] **Step 4: Confirm red (module not mounted)**

Run: `cargo test -p tinio-select struson_contract::`
Expected: 0 tests run — the `mod` is not mounted yet.

- [ ] **Step 5: Mount the module**

In `crates/tinio-select/src/json/mod.rs`, add after the other `mod` declarations:

```rust
#[cfg(test)]
pub(crate) mod struson_contract;
```

- [ ] **Step 6: Confirm green**

Run: `cargo test -p tinio-select struson_contract::`
Expected: 8 passed. If any one of them fails, the dependency version disagrees with the facts recorded in the spec — **stop and report**; do not change the tests to accommodate it.

- [ ] **Step 7: Checkpoint**

Report: `Cargo.toml`, `crates/tinio-select/Cargo.toml`, `Cargo.lock`, `crates/tinio-select/src/lib.rs`, `crates/tinio-select/src/json/struson_contract.rs` pending commit. **Do not run commit.**
---

### Task 3: `json/error.rs` and `crate::Error::Json`

**Files:**
- Create: `crates/tinio-select/src/json/error.rs`
- Modify: `crates/tinio-select/src/error.rs` (add the Json variant, hand-write PartialEq, extend the display tests)
- Modify: `crates/tinio-select/src/json/mod.rs` (add `pub(crate) mod error;`)

**Interfaces:**
- Consumes: `struson::reader::{JsonReaderPosition, JsonSyntaxError, ReaderError, UnexpectedStructureKind, ValueType}`, `crate::record::CAP_MESSAGE`.
- Produces (relied on from Task 5): `pub(crate) enum json::Error` (eight variants), `impl From<ReaderError> for json::Error`, `impl PartialEq for json::Error`, the `crate::Error::Json(json::Error)` variant and `From<json::Error>`, and the hand-written PartialEq of `crate::Error`.

- [ ] **Step 1: Write the failing tests**

At the end of `crates/tinio-select/src/json/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::io;

    use struson::reader::{JsonStreamReader, ReaderError, SyntaxErrorKind};

    use super::Error;
    use crate::record::CAP_MESSAGE;

    fn location() -> struson::reader::JsonReaderPosition {
        JsonStreamReader::new(b"".as_slice()).current_position(false)
    }

    fn syntax(kind: SyntaxErrorKind) -> ReaderError {
        ReaderError::SyntaxError(struson::reader::JsonSyntaxError { kind, location: location() })
    }

    fn io_error(kind: io::ErrorKind, message: &str) -> ReaderError {
        ReaderError::IoError {
            error: io::Error::new(kind, message.to_owned()),
            location: location(),
        }
    }

    #[test]
    fn cap_renders_the_verbatim_crate_message() {
        assert_eq!(CAP_MESSAGE, "input record exceeds 1 MB");
        assert_eq!(
            Error::Cap.to_string(),
            format!("S3 select: input error: {CAP_MESSAGE}")
        );
    }

    #[test]
    fn syntax_renders_the_json_input_prefix() {
        let json = Error::from(syntax(SyntaxErrorKind::TrailingData));
        assert!(json.to_string().starts_with("json input:"), "{json}");
    }

    #[test]
    fn invalid_utf8_is_recognized_by_io_kind_and_message() {
        let json = Error::from(io_error(io::ErrorKind::InvalidData, "invalid UTF-8 data"));
        assert!(matches!(json, Error::InvalidUtf8 { .. }), "{json:?}");
    }

    #[test]
    fn a_real_read_failure_stays_an_io_error() {
        let json = Error::from(io_error(io::ErrorKind::ConnectionReset, "boom"));
        match json {
            Error::Io { error, .. } => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
            other => panic!("expected Io, got {other:?}"),
        }
    }

    #[test]
    fn invalid_data_from_the_reader_is_not_called_invalid_utf8() {
        // A reader that genuinely fails with InvalidData and a different
        // message must not be reported as a malformed document.
        let json = Error::from(io_error(io::ErrorKind::InvalidData, "disk on fire"));
        assert!(matches!(json, Error::Io { .. }), "{json:?}");
    }

    #[test]
    fn partial_eq_compares_the_variant() {
        assert_eq!(Error::Cap, Error::Cap);
        assert_ne!(Error::Cap, Error::from(syntax(SyntaxErrorKind::TrailingData)));
    }
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select json::error::`
Expected: compilation fails (`json/error.rs` is not mounted).

- [ ] **Step 3: Implement `json/error.rs`**

```rust
//! JSON input errors. Split out of the crate-level Error so every failure mode
//! keeps its own value and the original struson error (with its location)
//! survives instead of degrading into a formatted string.
//!
//! From<ReaderError> keeps a catch-all arm: 0.7.2 is not non-exhaustive, but an
//! upgrade must not silently drop a variant.

use std::io;

use struson::reader::{
    JsonReaderPosition, JsonSyntaxError, ReaderError, UnexpectedStructureKind, ValueType,
};
use thiserror::Error;

use crate::record::CAP_MESSAGE;

/// struson builds invalid UTF-8 as io::Error::new(ErrorKind::InvalidData,
/// "invalid UTF-8 data") (stream_reader.rs:1009). Both the kind and the message
/// are checked, so a reader failing with InvalidData for another reason stays a
/// plain IO error.
const INVALID_UTF8_MESSAGE: &str = "invalid UTF-8 data";

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("json input: {0}")]
    Syntax(#[from] JsonSyntaxError),
    #[error("json input: unexpected value type: expected {expected:?}, got {actual:?} at {location}")]
    UnexpectedType {
        expected: ValueType,
        actual: ValueType,
        location: JsonReaderPosition,
    },
    #[error("json input: unexpected structure: {kind} at {location}")]
    Structure {
        kind: UnexpectedStructureKind,
        location: JsonReaderPosition,
    },
    #[error("json input: nesting exceeds {max_nesting_depth} levels at {location}")]
    NestingDepth {
        max_nesting_depth: u32,
        location: JsonReaderPosition,
    },
    #[error("json input: unsupported number value: {number} at {location}")]
    Number {
        number: String,
        location: JsonReaderPosition,
    },
    /// Invalid UTF-8 in the input: an input-format error, not an IO failure.
    #[error("json input: invalid UTF-8 data at {location}")]
    InvalidUtf8 { location: JsonReaderPosition },
    /// A real failure of the underlying reader.
    #[error("S3 select: io error: {error}")]
    Io {
        error: io::Error,
        location: JsonReaderPosition,
    },
    /// The 1 MB per-root record cap (design deviations 6 and 7).
    #[error("S3 select: input error: {CAP_MESSAGE}")]
    Cap,
    /// Forward-compatibility arm: `ReaderError` IS `#[non_exhaustive]`
    /// (vendored `reader/mod.rs:513`), so a future 0.7.x variant must still
    /// compile and must still report something.
    #[error("json input: {0}")]
    Other(String),
}

impl From<ReaderError> for Error {
    fn from(err: ReaderError) -> Self {
        match err {
            ReaderError::SyntaxError(e) => Self::Syntax(e),
            ReaderError::UnexpectedValueType { expected, actual, location } => {
                Self::UnexpectedType { expected, actual, location }
            }
            ReaderError::UnexpectedStructure { kind, location } => Self::Structure { kind, location },
            ReaderError::MaxNestingDepthExceeded { max_nesting_depth, location } => {
                Self::NestingDepth { max_nesting_depth, location }
            }
            ReaderError::UnsupportedNumberValue { number, location } => {
                Self::Number { number, location }
            }
            ReaderError::IoError { error, location } => {
                if error.kind() == io::ErrorKind::InvalidData
                    && error.to_string() == INVALID_UTF8_MESSAGE
                {
                    Self::InvalidUtf8 { location }
                } else {
                    Self::Io { error, location }
                }
            }
            // `ReaderError` is `#[non_exhaustive]`: without this arm the match
            // does not compile downstream (E0004), and a future variant must
            // not be dropped silently.
            other => Self::Other(other.to_string()),
        }
    }
}

/// Compared by variant only: ReaderError has no PartialEq, and the crate's
/// event-sequence assertions only need to tell failure modes apart (the
/// crate-level IoError compares by kind() for the same reason).
```

- [ ] **Step 4: Mount it and add the crate-level variant**

Add `pub(crate) mod error;` at the top of `crates/tinio-select/src/json/mod.rs`.

In the `Error` enum of `crates/tinio-select/src/error.rs`, insert after `Format(String)`:

```rust
    /// JSON input failures: every mode keeps its own value (see json::error).
    #[error("S3 select: input error: {0}")]
    Json(#[from] crate::json::error::Error),
```

**Do not change the crate-level `PartialEq`**: `#[derive(Debug, Error, From, PartialEq)]` is kept as is. `derive_more::PartialEq` compares field by field: `Io` goes through the kind comparison `IoError` already has, and `Json` goes through the hand-written `PartialEq` of `json::Error` (by variant) — exactly the right semantics. A hand-written `_ => discriminant` catch-all would make `Parse("x") == Parse("y")` true and the existing `partial_eq_same_and_cross` would go red at once.

- [ ] **Step 5: Extend the crate-level display test**

Append one more entry to `display_messages` in `error.rs`:

```rust
        assert_eq!(
            Error::Json(crate::json::error::Error::Cap).to_string(),
            "S3 select: input error: input record exceeds 1 MB"
        );
```

- [ ] **Step 6: Confirm green**

Run: `cargo test -p tinio-select`
Expected: all green. Not one line of the existing tests is changed (the Json variant is not yet produced by any production path).

- [ ] **Step 7: Checkpoint**

Report: `src/error.rs`, `src/json/error.rs`, `src/json/mod.rs` pending commit. **Do not run commit.**
---

### Task 4: `line.rs` - LineFraming moves in and stays

**Files:**
- Create: `crates/tinio-select/src/json/line.rs`
- Modify: `crates/tinio-select/src/json/mod.rs` (add `pub(crate) mod line;`)
- Modify: `crates/tinio-select/src/json/framing.rs` (delete only the LineFraming definition; the rest stays until Task 8)
- Modify: `crates/tinio-select/src/json/lines.rs` (follow the use change)

**Interfaces:**
- Consumes: `crate::record::MAX_RECORD`.
- Produces (relied on by Task 7): `pub(crate) struct LineFraming<R>`, `pub(crate) enum LineStart { Eof, Line(u64) }`, and the methods `new(inner: R) -> Self`, `start_line(&mut self) -> io::Result<LineStart>`, `capped(&self) -> bool` — **(revised during implementation)**: `capped` was retired; the width cap is raised as an `InvalidData` IO error carrying `CAP_MESSAGE`, which `json::error`'s `From<ReaderError>` maps back to `Cap` (see Task 7's mapping note), so `start_line` is the type's only other method.

**The type's arity must change (this task's third undeclared change)**: as it stands there is `struct LineFraming<R: Read> { inner: BufReader<R> }` and `new(inner: BufReader<R>)` (`framing.rs:99-108`). Copied as is, Task 7 passing a `BufReader<R>` would become the double buffering of `BufReader<BufReader<R>>`. **Change both the field and the constructor to a single parameter**: `inner: R` and `new(inner: R)`, leaving the caller responsible for passing a BufReader (that is how `lines.rs` passed it in the old implementation; keep that division of labour).

Also: the line start is already carried by the `LineStart::Line(u64)` payload, so **no `line_start()` accessor exists today and none is added**.

- [ ] **Step 1: Migrate the implementation**

Move the `LineFraming` and `LineStart` blocks from `json/framing.rs` into `json/line.rs` wholesale, without changing one line of logic, making only two adjustments:

1. `line_start()` and `capped()` change from private field reads to `pub(crate)` methods (Task 7's handler needs to read them) — **(revised during implementation)**: neither survives, as the note above says — the line start rides `start_line()`'s `LineStart::Line(start)` payload, and `capped` is gone, the width cap being raised as an `InvalidData` IO error that `json::error`'s `From<ReaderError>` maps back to `Cap`;
2. delete any reference to `Budget` and `BudgetedRead` (this layer no longer has a budget concept; the budget is `feed.rs`'s `RootBudget`).

Replace the file-header comment with:

```rust
//! LINES framing: serves exactly one line content bytes (terminator
//! excluded) and reports EOF at the newline, so the parser consumes a line
//! as a stream. The span cap (content + terminator <= 1 MB) is enforced here,
//! mid-serve, and remains the authority on how wide a line is.
```

- [ ] **Step 2: Migrate its three tests**

Move `framing.rs`'s `lines_serve_content_then_eof_until_next_line`, `lines_blank_line_serves_immediate_eof_but_exists` and `lines_cap_fires_mid_serve_and_exact_span_allowed` into `line.rs`'s `#[cfg(test)] mod tests` verbatim, together with the `framing()` helper they share.

- [ ] **Step 3: Eliminate the duplicate name**

Two `LineFraming`s under the same name would clash, so this task's handling is: delete the `LineFraming` definition in `framing.rs`, change it to `use crate::json::line::LineFraming;`, and change `lines.rs`'s use in step. The rest of `framing.rs` (`BudgetedRead`, `DocFraming`) stays until Task 8.

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select`
Expected: all green. `lines.rs` still goes through the old kernel, but the `LineFraming` it references has changed files.

- [ ] **Step 5: Checkpoint**

Report: `src/json/line.rs`, `src/json/mod.rs`, `src/json/framing.rs`, `src/json/lines.rs` pending commit. **Do not run commit.**

---

### Task 5: `matcher.rs` - ValueSource and the pausable stepper

**Files:**
- Rewrite: `crates/tinio-select/src/json/matcher.rs`

**Interfaces:**
- Consumes: `crate::{error::Error, json::Type, path::PathSeg}`, `serde_json::Value`, `crate::row::Record`.
- Produces (relied on by Tasks 6 and 7): every matcher.rs entry in Global Constraints' "Verified interface shapes" (`Step`, `CompiledPath`, `ValueKind`, `ValueSource`, `Progress`, `Matcher`, plus `Matcher::drop_pending`, which `Tail` needs).

> **Atomicity (2026-09-13 review: P1-4)**: the moment Task 5 lands, the old `core.rs` and the two handlers can no longer reference `Skip`/`child_into`, so **the crate stops compiling from there on**, until Task 7 Step 4. Therefore **Tasks 5, 6 and 7 must be completed consecutively as one atomic change before any verification**: Task 6's `feed.rs` is purely additive and does not widen the broken window; Task 5's and Task 6's own `cargo test <mod>::` can only show that "the new module's own tests pass", and **the crate-level green gate is at Task 7 Step 4**. The executor must not declare "all green" anywhere other than where it is asked for.

**Semantics (implemented point by point, equivalent to the old StepSeed)**

1. the step sequence drops the leading `Wild` (the implicit `S3Object[*]`).
2. when a container closes and that scope's match count is 0, enqueue one `None` (MISSING).
3. a step applied to a scalar is zero matches.
4. `Wild` fans out in document DFS order; for `Wild` over an object, each member's rows are accumulated into one group first and appended in member order when the object closes.
5. `Index(i)` recurses only into the i-th element; out of range is zero matches.
6. after a final-step `Name` matches it is held back, and it is enqueued only when the object closes with no second case-folded match confirmed; a duplicate is `Error::Ambiguous(name)`.
7. a duplicate non-final-step `Name` is `Error::Ambiguous(name)` immediately, and rows already produced are not reclaimed.
8. an unmatched subtree is only depth-counted and not one value is materialized: when a container the current frame has no interest in is met, `source.skip_value()` is called and not one byte enters Cap.
9. a `peek()` that meets a type other than the expected one is treated as zero matches and is not escalated into an error.
10. case-folded comparison reuses the existing `key_matches` (shared with the engine-side `json_lookup`, to prevent drift).

**Shape conventions (names may change and fields may be merged, but these five things must be preserved)**: each level's own row group, each level's matched count, `Wild`'s one slot per group, `Name`'s enqueue-at-close, and the ability to keep feeding events of the same root after `StopRoot`.

- [ ] **Step 1: Write the failing tests (scripted stand-in, assertions kept one by one)**

The test module first replaces the old serde_json driver with a stand-in. The stand-in is an "instruction table" style `ValueSource`:

```rust
/// A scripted ValueSource: the instructions are the JSON a real reader would
/// have produced, so a test pins matcher semantics without any reader.
#[derive(Default)]
struct Script {
    steps: Vec<ScriptStep>,
    at: usize,
    pos: u64,
}

#[derive(Clone, Debug)]
enum ScriptStep {
    Kind(ValueKind),   // peek() reports this
    Name(String),      // name()
    Text(String),      // text()
    Number(String),    // number()
    Bool(bool),        // boolean()
    Null,              // null()
    Begin,             // begin_object / begin_array
    End,               // end_object / end_array
    HasNext(bool),     // has_next()
    Skip,              // skip_value()
}
```

Keep the assertions one by one; only the way they are driven changes.

| Real case in the working tree | Disposition | Semantics pinned (semantic numbers above) |
| --- | --- | --- |
| `wild_over_int_scalar_is_missing` | keep, change driver | 3 |
| `wild_over_float_scalar_is_missing` | keep, change driver | 3 |
| `index_over_number_map_is_missing` | keep, change driver | 5 |
| `number_token_first_key_object_errors_not_panics` | delete (deviation 10) | the mechanism does not exist |
| `number_token_single_entry_is_a_scalar` | delete (deviation 10) | the mechanism does not exist |
| `escaped_key_matches_case_insensitively` | keep, change driver | 10 (escaped keys take part in case folding too) |
| `final_name_holds_value_until_object_close` | keep, change driver | 6 (the final-step Name's hold-back and decide-at-close) |
| `non_final_name_duplicate_fails_mid_stream` | keep, change driver | 7 (a duplicate non-final step errors immediately) |
| `zero_match_rules_per_step` | keep, change driver | 2 and 3 (each step shape's zero match adds one MISSING row) |
| `nested_wild_order_matches_dfs` | keep, change driver | 4 |
| `wild_over_object_collapses_duplicate_keys_in_place` | keep, change driver | 4 and key collapsing (record model semantics) |
| `wild_over_object_collapse_replaces_the_whole_row_group` | keep, change driver | 4 (the whole group is replaced, not overridden row by row) |
| `wild_over_object_collapses_across_many_distinct_keys` | keep, change driver | 4 |
| `skipped_subtree_still_validates_string_content` | keep, change driver | 8 and the validation strength of the skip path |
| `skipped_subtree_rejects_a_reserved_number_token_object` | delete (deviation 10) | the mechanism does not exist |
| `element_barely_under_cap_is_accepted` | void (it pins the old Budget); taken over by Task 9 Step 5 item 1 | none |
| `budget_is_charged_per_root_not_accumulated` | void (it pins the old Budget); taken over by Task 9 Step 5 item 2 | none |

`pull_with_budget` (which depends on `DocFraming` and disappears together with `framing.rs`) is voided along with them.

Added: `next_root_stops_after_a_completed_root_value`, `pending_high_stays_bounded_for_a_large_fan_out` (given below), `verbatim_number_keeps_the_token` (already given in Step 4), `take_record_yields_values_not_offsets` (offsets belong to the handler; the matcher produces no offsets).
Delete three (deviation 10; serde's reserved-key mechanism does not exist under the new kernel): `number_token_first_key_object_errors_not_panics`, `number_token_single_entry_is_a_scalar`, `skipped_subtree_rejects_a_reserved_number_token_object`, and delete the `NUMBER_TOKEN` constant at the top along with every comment that references it.

Add two (the pins for Q18=b and deviation 12):

```rust
#[test]
fn next_root_stops_after_a_completed_root_value() {
    // One root value completes, so the batch is handed over immediately and
    // pending never grows with the root.
    let mut m = Matcher::new(compiled(&[PathSeg::Wild, PathSeg::Name("x".into())]));
    let mut src = Script::new([Begin, Name("x"), Number("1"), End, HasNext(false)]);
    assert_eq!(m.next_root(&mut src).unwrap(), Progress::StopRoot);
    assert_eq!(m.pending_len(), 1);
}

#[test]
fn pending_high_stays_bounded_for_a_large_fan_out() {
    // 10_000 tiny matches under one root: pending is drained per batch, so
    // the high-water mark stays at one batch worth of bytes.
    let mut m = Matcher::new(compiled(&[PathSeg::Wild, PathSeg::Wild]));
    // Drive the Script with 10_000 small objects, calling take_record() each
    // time next_root() returns StopRoot, then assert:
    assert!(m.pending_high() <= 1024, "pending_high = {}", m.pending_high());
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select matcher::`
Expected: compilation fails (`ValueSource`, `ValueKind`, `Matcher` are not implemented yet).

- [ ] **Step 3: Implement `ValueKind`, `ValueSource`, `Progress`, `CompiledPath`**

`CompiledPath` and `Step` are carried over verbatim from the existing implementation (semantics unchanged; drop the reference to `StepSeed` in the old comments). `ValueKind`, `ValueSource` and `Progress` are written to the interface shapes in Global Constraints. `ValueSource::position` is not a member: position sampling is the handlers' own free function (`json::source::position`), and the matcher never calls it.

- [ ] **Step 4: Implement the value assembler Cap**

It is driven only when "the value has been matched in full". A `Name` event first stages its key through `Cap::name`, and `map.insert` happens when the value arrives. Under `preserve_order`, `serde_json::Map` is an IndexMap where a duplicate key is last-wins and keeps the position of the first occurrence — exactly the semantics `document_wild_over_object_collapses_duplicate_keys` pins.

```rust
/// A number token straight from the input, kept verbatim (deviation 8).
///
/// Do NOT use `serde_json::from_str::<Value>` here: with `arbitrary_precision`
/// its scanner injects a `+` into a signless exponent (`de.rs:1027-1043`,
/// `scan_exponent`), so `from_str("1e309")` yields `Number("1e+309")` and
/// deviation 8 fails. `from_string_unchecked` is a safe constructor that wraps
/// the string as-is (`number.rs:341-347`); struson already validated the token
/// (verified fact 11), so no re-parse is needed.
fn verbatim_number(token: &str) -> Value {
    Value::Number(serde_json::Number::from_string_unchecked(token.to_owned()))
}
```

Add a round-trip pin, to stop an upstream change from turning this constructor into something that normalizes:

```rust
#[test]
fn verbatim_number_keeps_the_token() {
    assert_eq!(verbatim_number("1e309").to_string(), "1e309");
    assert_eq!(
        verbatim_number("123456789012345678901234567890").to_string(),
        "123456789012345678901234567890"
    );
    // and the path it replaces would NOT:
    assert_eq!(
        serde_json::from_str::<Value>("1e309").unwrap().to_string(),
        "1e+309",
        "serde_json still normalizes; that is why from_string_unchecked is used"
    );
}
```

- [ ] **Step 5: Implement Matcher**

```rust
/// What one step level is doing inside its container.
enum Scope {
    /// The container itself is the record (SELECT * FROM S3Object).
    Value(Option<Cap>),
    /// Name: rows of the matched key accumulate here until the object closes,
    /// which is what makes the final-name ambiguity rule possible.
    Named { group: VecDeque<Option<Value>>, matched: bool },
    /// Wild: one group per child, appended at close so a child MISSING stays
    /// in its own slot.
    Wild { group: VecDeque<Option<Value>> },
    /// Index(i) over an array.
    Indexed { target: usize, ord: usize },
}

/// One open container.
struct Frame {
    scope: Scope,
    /// Matches registered at this level (0 on close means one MISSING row).
    matched: bool,
    /// Case-folded key names already resolved at this Name level.
    seen: Vec<String>,
}
```

**The division of labour between the two return values (this is the only genuinely new mechanism in this plan and must be implemented verbatim)**:

- `on_value` returns `Progress::StopRoot` **after a matched value completes and one record has been enqueued** (MISSING counts as one too); in every other case it reports no progress (`None` on the private `step` that returns it); when the input ends it returns `End`.
- `next_root` passes `on_value`'s return value through **unchanged** and does not synthesize `StopRoot` itself; it returns `End` only when `done` (the root has completed and the in-flight `pending` is empty).

```rust
impl Matcher {
    pub(crate) fn next_root(&mut self, source: &mut dyn ValueSource) -> Result<Progress, Error> {
        loop {
            // The root is complete and nothing is in flight: this root is done.
            if self.done {
                return Ok(Progress::End);
            }
            let kind = source.peek()?;
            match self.on_value(kind, source)? {
                // One record is queued: hand it over immediately. This is what
                // keeps `pending` at one record regardless of the root size.
                Some(Progress::StopRoot) => return Ok(Progress::StopRoot),
                // The root value finished (its scope closed) with nothing queued
                // (for example a zero-match scalar step whose MISSING was already
                // delivered): latch done and let the next loop end the root.
                None | Some(Progress::End) if self.root_complete() => {
                    self.finish_root();
                }
                None => {}
                Some(Progress::End) => return Ok(Progress::End),
            }
        }
    }
}
```

`on_value` dispatches by kind: a container is `begin_*` followed by pushing a frame; a scalar is produced or skipped according to the current step. An unmatched subtree is always `source.skip_value()`, reporting no progress (`None`), with not one value materialized. `take_record` takes one entry from `pending` and wraps it as `Record::Json` (`None` means MISSING).

**The root-complete signal**: `root_complete()` = the top-level scope has closed (the frame stack is back to empty) and `done` is false. `finish_root()` sets `done` and does two closing jobs: adding the root's MISSING row (if the whole root had zero matches and never enqueued one) and clearing the frame stack.

`pending_high` is counted in bytes (carrying over the old `core.rs` algorithm):

```rust
#[cfg(test)]
fn touch_pending_high(&mut self) {
    // Bytes, not records: pending memory, not its length.
    let bytes: usize = self.pending.iter().flatten().map(|v| v.to_string().len()).sum();
    if bytes > self.pending_high {
        self.pending_high = bytes;
    }
}
```

- [ ] **Step 6: Confirm green**

Run: `cargo check -p tinio-select` + `cargo test -p tinio-select matcher::`
Expected: **after this task and before Task 7 is done, the crate does not compile as a whole** — the old `core.rs` still has `use matcher::{Skip, child_into}` (`core.rs:18`), and the two handlers are still using the old matcher. Therefore the achievable criterion for this step is: **the new matcher's own 17 tests pass**; crate-level compilation waits for Task 7 Step 4. To avoid a long broken window, do Tasks 5, 6 and 7 consecutively as **one atomic change** and verify afterwards (Task 6's `feed.rs` is purely additive and does not widen the broken window).

- [ ] **Step 7: Checkpoint**

Report: `crates/tinio-select/src/json/matcher.rs` pending commit. **Do not run commit.**
---

### Task 6: `feed.rs` - per-root byte accounting and the cap

**Files:**
- Create: `crates/tinio-select/src/json/feed.rs`
- Modify: `crates/tinio-select/src/json/mod.rs` (add `pub(crate) mod feed;`)

**Interfaces:**
- Consumes: `crate::record::MAX_RECORD`, `crate::error::Error`, `crate::json::error::Error as JsonError`.
- Produces (relied on by Task 7): `pub(crate) struct RootBudget`, with the methods `new(cap: u64) -> Self`, `begin(&mut self, pos: u64)`, `check(&mut self, pos: u64) -> bool`, `fired(&self) -> bool` — plus, added during implementation, `begin_element(&mut self, pos: u64)`, `check_or_cap(&mut self, pos: u64) -> Result<(), Error>` and `check_root_span(&mut self, pos: u64) -> Result<(), Error>` (see the note under Global Constraints' interface shapes).

**Sampling points (the spec's three-part cap table, measured by probe fact 7)**: for an array root, each element's span starts at the `pos()` after that `has_next()` returned true and after the value's `peek()`, and ends at the `pos()` after that value has been read and before the next round's `has_next()` or value `peek()` (so the span excludes the commas and whitespace between elements); the last element's end point is the `pos()` before `end_array()` (so it excludes the closing bracket); a single-value root runs from the `pos()` after the first `peek()` to the `pos()` at which the whole document has been read; LINES takes no span sample at all — **(revised during implementation)**: the LINES budget was removed, `LineFraming` alone owns the line-width cap (so the handler holds no `RootBudget`, and the spec's sampling table agrees), and the line's record start comes from `start_line()`'s `LineStart::Line(start)` payload rather than from any `pos()`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::RootBudget;

    #[test]
    fn a_root_exactly_at_the_cap_is_accepted() {
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(10);
        assert!(budget.check(10 + 1_048_576));
        assert!(!budget.fired());
    }

    #[test]
    fn one_byte_past_the_cap_fires() {
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(0);
        assert!(!budget.check(1_048_577));
        assert!(budget.fired());
    }

    #[test]
    fn intermediate_batches_are_checked_against_the_same_origin() {
        // Q18=b: the span accumulates across batches of one root, so a later
        // batch can fire the cap even when each batch is small.
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(100);
        assert!(budget.check(100 + 600_000));
        assert!(!budget.check(100 + 1_048_577));
        assert!(budget.fired());
    }

    #[test]
    fn a_new_root_resets_the_account() {
        let mut budget = RootBudget::new(1_048_576);
        budget.begin(0);
        assert!(!budget.check(1_048_577));
        budget.begin(2_000_000);
        assert!(!budget.fired());
        assert!(budget.check(2_000_000 + 1_048_576));
    }
}
```

- [ ] **Step 2: Confirm red**

Run: `cargo test -p tinio-select feed::`
Expected: compilation fails (`RootBudget` does not exist).

- [ ] **Step 3: Implement**

```rust
//! Per-root byte accounting. Struson reports consumed bytes through
//! `current_position(false).data_pos`; that single sequence feeds both the
//! record-start offsets and the 1 MB cap, so no second byte counter exists
//! anywhere in the reader (design spec, overall architecture).

use crate::record::MAX_RECORD;

/// The 1 MB budget of one root value. The handler calls `begin` once (right
/// after the root value start is known) and `check` at every batch boundary,
/// so a slow drip of batches can still fire the cap mid-root (deviation 7).
pub(crate) struct RootBudget {
    start: u64,
    cap: u64,
    fired: bool,
}

impl RootBudget {
    pub(crate) fn new(cap: u64) -> Self {
        Self { start: 0, cap, fired: false }
    }

    pub(crate) fn with_default_cap() -> Self {
        Self::new(MAX_RECORD)
    }

    /// Start a new root at `pos` (the byte offset of the root value start).
    pub(crate) fn begin(&mut self, pos: u64) {
        self.start = pos;
        self.fired = false;
    }

    /// Check the span so far; returns false once the cap is exceeded.
    pub(crate) fn check(&mut self, pos: u64) -> bool {
        if pos.saturating_sub(self.start) > self.cap {
            self.fired = true;
            return false;
        }
        true
    }

    pub(crate) fn fired(&self) -> bool {
        self.fired
    }
}
```

> **(revised during implementation)**: the block above is the planned shape, not the shipped one — `begin_element(pos)` (opens an array element's span, superseding the root origin), `check_root_span(pos) -> Result<(), Error>` (the document-level closing check, a no-op once an element origin is set), and the `check_or_cap(pos)` wrapper the sample sites report through were added, and the fields became private internals shared with the charged `Read` backstop rather than `start`/`cap`/`fired` (see the Global Constraints note).

- [ ] **Step 4: Confirm green**

Run: `cargo test -p tinio-select feed::`
Expected: 4 passed.

- [ ] **Step 5: Checkpoint**

Report: `src/json/feed.rs`, `src/json/mod.rs` pending commit. **Do not run commit.**

---

### Task 7: `lines.rs` and `document.rs` switch kernels

**Files:**
- Rewrite: `crates/tinio-select/src/json/lines.rs`
- Rewrite: `crates/tinio-select/src/json/document.rs`
- Modify: `crates/tinio-select/src/json/mod.rs` (the `Handler` dispatch is unchanged; just confirm the use points at the new types)
- Modify: `crates/tinio-select/src/json/core.rs` (split by behaviour: see Step 1)

**Interfaces:**
- Consumes: `json::error::Error` from Task 3, `LineFraming` from Task 4, `ValueSource`/`Matcher`/`ValueKind`/`Progress` from Task 5, `RootBudget` from Task 6, and `crate::record::CAP_MESSAGE`.
- Produces: an unchanged `json::Reader::new` and `RecordReader`; the two handlers each expose `#[cfg(test)] fn pending_high(&self) -> usize`.

- [ ] **Step 1: Split `core.rs` by behaviour (Q21=a + renaming)**

Today's `Core` in `core.rs` holds a bit of everything (budget / path / pending / failure / last_record_start / done / pending_high / pull / advance). Split by behaviour and recomposed:

- `feed.rs` (already created in Task 6) takes over budget;
- `matcher.rs` (Task 5) takes over path and pending_high;
- **create `crates/tinio-select/src/json/tail.rs`** to take over the "endgame" behaviour: `last_record_start`, `done`, `failure` (the terminal-error rule) and the `advance` loop.

```rust
//! The tail every handler shares: the record-start offset, the done latch, the
//! terminal-error rule, and the drain loop behind `RecordReader::next`.
//!
//! Terminal-error rule (pinned by `document_error_is_terminal_for_later_polls`
//! and `lines_error_is_terminal_for_later_polls`): a failing pull drops the
//! rows it produced and latches `done`, so a caller that keeps polling sees a
//! finished stream rather than rows from a root that already errored.
pub(crate) struct Tail {
    pub(crate) last_record_start: u64,
    pub(crate) done: bool,
    pub(crate) failure: Option<Error>,
}

impl Tail {
    pub(crate) fn new() -> Self { ... }

    /// Drain the matcher first, then one pull; a pull error is terminal.
    pub(crate) fn advance(
        &mut self,
        matcher: &mut Matcher,
        mut pull: impl FnMut(&mut Self, &mut Matcher) -> Result<(), Error>,
    ) -> Result<Option<Record>, Error> {
        loop {
            if let Some(record) = matcher.take_record() {
                return Ok(Some(record));
            }
            if self.done {
                return Ok(None);
            }
            if let Err(e) = pull(self, matcher) {
                matcher.drop_pending();
                self.done = true;
                return Err(e);
            }
        }
    }
}
```

Delete `core.rs` (its `pull` is replaced by each handler's own driving, its `budget` is replaced by `RootBudget`, and `pending_high` has moved into `Matcher`). In `mod.rs`, `pub(crate) mod core;` becomes `pub(crate) mod tail;`.

- [ ] **Step 2: Write `lines.rs`**

```rust
//! LINES handler: one root value per line. The line is never buffered —
//! `LineFraming` serves its bytes one at a time and reports EOF at the
//! newline, so the only thing that grows is the token being resolved.
//!
//! Ordering rule (design spec, verified fact 4): after the top-level value is
//! consumed, `peek()` panics, so the trailing check is
//! `consume_trailing_whitespace` and nothing else.

use std::io::{BufReader, Read};

use struson::reader::{JsonStreamReader, ReaderSettings, SyntaxErrorKind};

use crate::{
    error::Error,
    json::{
        error::Error as JsonError,
        feed::RootBudget,
        line::{LineFraming, LineStart},
        matcher::{CompiledPath, Matcher, Progress, ValueKind, ValueSource},
        tail::Tail,
        Type,
    },
    record::{CAP_MESSAGE, RecordReader},
    row::Record,
    sql::FromClause,
};

/// Map a struson error into the crate error.
///
/// A **real read failure** must keep the crate IO shape instead of nesting into
/// `Error::Json(json::Error::Io)`, which would render as
/// `S3 select: input error: S3 select: io error: ...` (double prefix). Invalid
/// UTF-8 is already split off inside `json::Error::from`, so what reaches the
/// `Io` arm here is a genuine reader failure.
fn map_json_err(err: struson::reader::ReaderError) -> Error {
    match JsonError::from(err) {
        JsonError::Io { error, .. } => Error::from(error),
        other => Error::Json(other),
    }
}

/// The `ValueSource` over a real reader. This is the only place that ever
/// sees a `struson::reader::ReaderError` — everything above sees `crate::Error`.
struct ReaderSource<R: Read> {
    reader: JsonStreamReader<R>,
}

impl<R: Read> ValueSource for ReaderSource<R> {
    fn peek(&mut self) -> Result<ValueKind, Error> {
        Ok(match self.reader.peek().map_err(JsonError::from)? {
            struson::reader::ValueType::Array => ValueKind::Array,
            struson::reader::ValueType::Object => ValueKind::Object,
            struson::reader::ValueType::String => ValueKind::String,
            struson::reader::ValueType::Number => ValueKind::Number,
            struson::reader::ValueType::Boolean => ValueKind::Bool,
            struson::reader::ValueType::Null => ValueKind::Null,
        })
    }

    fn has_next(&mut self) -> Result<bool, Error> {
        self.reader.has_next().map_err(|e| map_json_err(e))
    }

    fn name(&mut self) -> Result<Cow<'_, str>, Error> {
        // `next_name` borrows from the reader; the matcher only compares the name.
        self.reader
            .next_name()
            .map_err(|e| map_json_err(e))
            .map(Cow::Borrowed)
    }

    fn text(&mut self) -> Result<String, Error> {
        self.reader.next_string().map_err(|e| map_json_err(e))
    }

    fn number(&mut self) -> Result<String, Error> {
        // Verbatim token: `next_number_as_str` never round-trips through f64/u64.
        Ok(self
            .reader
            .next_number_as_str()
            .map_err(|e| map_json_err(e))?
            .to_owned())
    }

    fn boolean(&mut self) -> Result<bool, Error> {
        self.reader.next_bool().map_err(|e| map_json_err(e))
    }

    fn null(&mut self) -> Result<(), Error> {
        self.reader.next_null().map_err(|e| map_json_err(e))
    }

    fn begin_object(&mut self) -> Result<(), Error> {
        self.reader.begin_object().map_err(|e| map_json_err(e))
    }

    fn end_object(&mut self) -> Result<(), Error> {
        self.reader.end_object().map_err(|e| map_json_err(e))
    }

    fn begin_array(&mut self) -> Result<(), Error> {
        self.reader.begin_array().map_err(|e| map_json_err(e))
    }

    fn end_array(&mut self) -> Result<(), Error> {
        self.reader.end_array().map_err(|e| map_json_err(e))
    }

    fn skip_value(&mut self) -> Result<(), Error> {
        self.reader.skip_value().map_err(|e| map_json_err(e))
    }

    // No position member: it is the handlers' own free function, `position(&reader)`.
}
```

The skeleton of `Lines::next` (every endgame check goes through `consume_trailing_whitespace`):

```rust
pub struct Lines<R: Read> {
    framing: LineFraming<BufReader<R>>,
    matcher: Matcher,
    budget: RootBudget,
    tail: Tail,
}

impl<R: Read> Lines<R> {
    pub fn new(reader: R, from: &FromClause) -> Self {
        Self {
            framing: LineFraming::new(BufReader::new(reader)),
            matcher: Matcher::new(CompiledPath::from_segments(&from.segments, Type::Lines)),
            budget: RootBudget::with_default_cap(),
            tail: Tail::new(),
        }
    }

    fn pull_line(&mut self, tail: &mut Tail, matcher: &mut Matcher) -> Result<(), Error> {
        let line_start = match self.framing.start_line().map_err(|io| Error::from(io))? {
            LineStart::Eof => {
                tail.done = true;
                return Ok(());
            }
            // The record start for LINES is the LINE start, not the value start:
            // leading indentation belongs to the record (pinned by
            // `lines_indented_line_reports_the_line_start`).
            LineStart::Line(start) => start,
        };
        tail.last_record_start = line_start;

        let settings = ReaderSettings {
            track_path: false,
            restrict_number_values: false,
            ..ReaderSettings::default()
        };
        let reader = JsonStreamReader::new_custom(&mut self.framing, settings);
        let mut source = ReaderSource { reader };
        self.budget.begin(source.position());

        let mut seen_value = false;
        let mut progress = None;
        while progress.is_none() {
            let kind = match source.peek() {
                Ok(kind) => kind,
                Err(e) => return Err(self.cap_or(e)),
            };
            if !seen_value {
                // The root value start is the position after peek: peek never
                // moves `data_pos` (verified fact 3).
                self.budget.begin(source.position());
            }
            progress = matcher
                .on_value(kind, &mut source)
                .map_err(|e| self.cap_or(e))?;
            seen_value = true;
            if progress == Some(Progress::StopRoot) {
                // A value completed: cap-check this root's span, then hand the
                // one queued record over. The remaining values of the same line
                // are read by the next poll (`matcher` keeps its frame stack).
                if !self.budget.check(source.position()) {
                    return Err(Error::Json(JsonError::Cap));
                }
                return Ok(());
            }
        }

        // Ordering (verified fact 4): never peek again once the top-level value
        // is consumed; the only remaining probe is consume_trailing_whitespace,
        // and it must come before the first record is handed out.
        if !seen_value {
            return Err(self.cap_or(Error::Json(JsonError::from(
                struson::reader::ReaderError::SyntaxError(struson::reader::JsonSyntaxError {
                    kind: SyntaxErrorKind::IncompleteDocument,
                    location: source.reader.current_position(false),
                }),
            ))));
        }
        match source.reader.consume_trailing_whitespace() {
            Ok(()) => Ok(()),
            Err(struson::reader::ReaderError::SyntaxError(e))
                if e.kind == SyntaxErrorKind::TrailingData =>
            {
                // Ours, not struson's (deviation 9): pinned verbatim.
                Err(Error::Json(JsonError::Other(
                    "json input: trailing characters on line".into(),
                )))
            }
            Err(e) => Err(self.cap_or(map_json_err(e))),
        }
    }

    /// The line-width cap of `LineFraming` reaches us as an IO error (it is
    /// raised inside `Read::read`), so every failure path checks it FIRST and
    /// reports the pinned `Cap` variant — the equivalent of the old kernel
    /// checking `framing.capped` in `map_err`.
    fn cap_or(&self, err: Error) -> Error {
        if self.framing.capped() {
            return Error::Json(JsonError::Cap);
        }
        err
    }
}
```

> **(revised during implementation)**: the `Lines` skeleton above still holds `budget: RootBudget` and samples it; the shipped handler holds **no** budget and takes no span sample — `LineFraming` alone owns the line-width cap (so the cap arrives as an IO error, per note 1 below) and the record start is the line's own first byte, which is why the spec's LINES sampling row says "no span sample".

**Note (two rules that must be carried out)**:

1. **Mapping the LINES line-width cap** (**superseded 2026-09-15**: the skeleton's `cap_or` and `LineFraming::capped` above were retired in an earlier wave). `LineFraming`'s cap is still an IO error thrown inside `Read::read` — the `InvalidData` kind carrying the pinned `CAP_MESSAGE` — and after flowing out through struson it still lands on a `json::Error::IoError`, **not** on `json::Error::Cap`. The mapping now lives once, in `json::Error::from`: it recognizes that exact kind + message pair and returns `json::Error::Cap`, so every LINES error path inherits the pinned cap semantics and there is no `capped()` flag left to consult. Without that arm the existing `lines_input_record_cap` and `lines_final_unterminated_line_over_cap` would see `Error::Io` instead of the pinned cap semantics. This is the equivalent of the `budget.fired()` check inside `map_err` in the old implementation.
2. **Record start**: use the `LineStart::Line(start)` payload (the line's first byte, indentation included); do not try to derive it from `data_pos` — LINES swaps the reader on every line, so `data_pos` starts at 0 each time.

**The only criterion is that Task 9's existing tests are all green**, in particular:

- `lines_trailing_garbage_errors`: the message must be `json input: trailing characters on line`;
- `empty_line_is_a_format_error` and `lines_whitespace_only_line_is_a_format_error`: an empty line and a whitespace-only line report `Format` (fact 10);
- `lines_last_record_start` and `lines_indented_line_reports_the_line_start`: the start is the line's first byte (indentation included), that is `start_line()`'s `LineStart::Line(start)` payload;
- `lines_error_is_terminal_for_later_polls`: `Tail`'s endgame rule;
- `lines_crlf_records_parse`: CRLF lines;
- `lines_skipped_subtree_still_validates_strings`: the validation strength of `skip_value()` (fact 5).

- [ ] **Step 3: Write `document.rs`**

```rust
//! DOCUMENT handler: one record per root value, except that a root array
//! yields one record per element. The root shape is discovered from the event
//! flow — there is no framing layer and no document buffer.
//!
//! Cap sampling (design spec, byte-supply section): each element span starts
//! after its `has_next()` and its `peek()`, and ends after the value is read,
//! so the span excludes the separators and the closing bracket.

pub struct Document<R: Read> {
    reader: Option<JsonStreamReader<R>>,
    matcher: Matcher,
    budget: RootBudget,
    tail: Tail,
    state: RootState,
}

enum RootState {
    /// Nothing read yet: `JsonStreamReader::new` is lazy, so the shape is
    /// decided before the first byte is consumed (verified fact 3).
    Undecided,
    /// The root is an array. `in_element` records whether an element is
    /// **half-read**: a previous poll delivered that element's one record and
    /// returned, so the next poll must resume it instead of calling
    /// `has_next()` again (calling `has_next` mid-element is incorrect usage).
    Array { in_element: bool },
    /// The root is a single value. `done` means its value has been fully
    /// consumed, so only the cap and trailing checks are left.
    Single { done: bool },
    Done,
}
```

The branches of `pull_root` and its sampling:

`pull_root` must be **resumable across calls** (a batch boundary is when one matched value completes, so one element may span two polls):

```rust
    fn pull_root(&mut self, tail: &mut Tail, matcher: &mut Matcher) -> Result<(), Error> {
        let reader = self.reader.as_mut().expect("reader is present until Done");
        let mut source = ReaderSource { reader };
        match self.state {
            RootState::Undecided => {
                let kind = source.peek().map_err(map_json_err)?;
                // The root value start is the position right after peek: peek
                // never moves data_pos (verified fact 3).
                tail.last_record_start = source.position();
                self.budget.begin(source.position());
                if kind == ValueKind::Array {
                    source.begin_array().map_err(map_json_err)?;
                    self.state = RootState::Array { in_element: false };
                    return Ok(());
                }
                self.state = RootState::Single { done: false };
                self.read_one(tail, matcher, &mut source)
            }
            // Resuming an element that the previous poll left half-read.
            RootState::Array { in_element: true } => self.read_one(tail, matcher, &mut source),
            RootState::Array { in_element: false } => {
                if source.has_next().map_err(map_json_err)? {
                    // The element span opens here: after has_next and after peek,
                    // so it excludes the separators and any whitespace.
                    tail.last_record_start = source.position();
                    self.budget.begin(source.position());
                    self.state = RootState::Array { in_element: true };
                    self.read_one(tail, matcher, &mut source)
                } else {
                    // The last element ends before `]`, so the closing bracket
                    // is not charged to it.
                    let end = source.position();
                    source.end_array().map_err(map_json_err)?;
                    self.finish_document(tail, matcher, end)
                }
            }
            RootState::Single { done: false } => self.read_one(tail, matcher, &mut source),
            RootState::Single { done: true } => {
                let end = source.position();
                self.finish_document(tail, matcher, end)
            }
            RootState::Done => {
                tail.done = true;
                Ok(())
            }
        }
    }

    /// Read one batch of the current root value. `StopRoot` means leave with a
    /// record queued (the element may still be half-read); `End` means the root
    /// value finished, so the element/root is no longer in flight.
    fn read_one(
        &mut self,
        tail: &mut Tail,
        matcher: &mut Matcher,
        source: &mut ReaderSource<&mut JsonStreamReader<R>>,
    ) -> Result<(), Error> {
        let progress = matcher.next_root(source).map_err(map_json_err)?;
        if progress == Progress::End {
            self.state = match self.state {
                RootState::Array { .. } => RootState::Array { in_element: false },
                RootState::Single { .. } => RootState::Single { done: true },
                other => other,
            };
            let _ = tail;
        }
        Ok(())
    }
```

The order of `finish_document` is **counter-intuitive but has to be this way** (revised 2026-09-13):

1. do the last cap check with the span from the root start to `end` (exceeding it returns `Cap`);
2. **immediately** do the endgame check: `take()` the reader out of the `Option` and call `consume_trailing_whitespace`;
3. `TrailingData` → synthesize our own message `json input: trailing characters after the JSON document` and return an error (**discarding the batch of records just enqueued**);
4. only once the endgame check passes may the in-flight batch of records be delivered, along with `tail.done = true`.

**Why it cannot be delivered first**: the existing `document_single_root_trailing_garbage_errors` pins that `5 {}` **returns Err on the very first `next()`**, and `document_error_is_terminal_for_later_polls` pins that a terminal error discards the records that pull produced (its comment says outright "Single-value mode is the shape where the root is already materialised when the trailing probe fails, so `pending` is non-empty at the error site"). Therefore, when a single-value root completes within the same pull, the cap and trailing checks must come **before** any record delivery; intermediate batches (root not yet fully read) are unaffected and deliver normally — which is exactly the shape deviation ④ describes.

`consume_trailing_whitespace` consumes the reader, so `Document`'s reader must be an `Option`, and after the endgame check `self.reader` is `None` (the `Done` branch no longer needs a reader).

- [ ] **Step 4: Confirm green (except the exceptions Task 9 names)**

Run: `cargo test -p tinio-select`
Expected: all green except the deviation points Task 9 has to change. At this point the two handlers have switched kernels, and `framing.rs` is still there (`BudgetedRead`/`DocFraming` are already unused).

- [ ] **Step 5: Checkpoint**

Report: `src/json/lines.rs`, `src/json/document.rs`, `src/json/tail.rs`, `src/json/mod.rs`, and the deleted `src/json/core.rs` pending commit. **Do not run commit.**

---

### Task 8: Close the façade, retire `framing.rs`

**Files:**
- Delete: `crates/tinio-select/src/json/framing.rs`
- Modify: `crates/tinio-select/src/json/mod.rs` (module declarations and module docs)

**Interfaces:**
- Consumes: the two handlers (Task 7).
- Produces: an unchanged `json::Reader` façade.

- [ ] **Step 1: Delete `framing.rs`**

By now nothing should reference it (`LineFraming` moved away in Task 4, and `BudgetedRead`/`DocFraming` fell out of use with the old kernel). Delete the file and confirm `mod.rs` has no `pub(crate) mod framing;`.

- [ ] **Step 2: Converge the module declarations**

```rust
pub(crate) mod document;
pub(crate) mod error;
pub(crate) mod feed;
pub(crate) mod line;
pub(crate) mod lines;
pub(crate) mod matcher;
pub(crate) mod tail;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod value;
```

- [ ] **Step 3: Rewrite the deviation list in the module docs**

Replace the whole existing deviations ①–⑥ list with the spec's ①–⑬ (including the `pending_high` migration, the new `json::Error` module, the cap sampling points, and the panic constraint of the trailing check). Also change the "Kernel" paragraph from serde_json to struson, and keep the throughput baseline paragraph written in Task 1.

- [ ] **Step 4: Confirm the whole package compiles and tests**

Run: `cargo test -p tinio-select`
Expected: all green except Task 9's deviation points; `cargo check --workspace --all-targets` has no unused-import warnings.

- [ ] **Step 5: Checkpoint**

Report: the deleted `src/json/framing.rs` and `src/json/mod.rs` pending commit. **Do not run commit.**
---

### Task 9: Align the existing assertions (deviation-point list)

**Files:**
- Modify: `crates/tinio-select/src/json/mod.rs` (deviation ⑧'s two expectations)
- Modify: `crates/tinio-select/src/json/document.rs` (deviation ⑫'s read point + taking over DocFraming)
- Modify: `crates/tinio-select/tests/stream_large.rs` (deviation ⑨'s variant matching)
- Modify: `crates/tinio-select/src/json/lines.rs` (deviation ⑨'s variant matching)

**Interfaces:**
- Consumes: Task 7's two handlers.
- Produces: an all-green `cargo test -p tinio-select`.

- [ ] **Step 1: Deviation ⑧ (number tokens are no longer normalized)**

In `json/mod.rs`'s `raw_number_passthrough_no_decimal_parse`: change both occurrences of `raw("1e+309")` to `raw("1e309")`, and explain in a comment that serde_json's `e+` injection disappeared with the kernel replacement. The assertion structure is unchanged.

- [ ] **Step 2: Deviation ⑫ (the `pending_high` read point)**

In `document.rs`, change both occurrences of `r.core.pending_high.get()` to `r.pending_high()` (the `#[cfg(test)]` method the handler exposes, which forwards to `Matcher`). Semantics unchanged: bounded for an array root, backstopped by the cap for a single-value root.

- [ ] **Step 3: Deviation ⑨ (variant matching, 16 places in total)**

Once the JSON path's errors change from `Error::Format` to `Error::Json`, **every** `Err(Error::Format(...))` binding arm falls through into `other => panic!`. 16 places were measured; each is handled as follows (re-check with `grep -rn "Error::Format" crates/tinio-select/src/json crates/tinio-select/tests/stream_large.rs crates/tinio-select/src/events.rs` before touching anything):

| File | Places | Location (line numbers are pre-revision) | Change |
| --- | --- | --- | --- |
| `json/lines.rs` | 7 | 268, 308, 365, 385, 398, 439, 455 | see the two rules below |
| `json/document.rs` | 7 | 234, 301, 354, 390, 405, 465, 526 | same as above |
| `tests/stream_large.rs` | 1 | 81 | same as above |
| `events.rs` | 2 | 871, 1107 | the one at 871 asserts an `Error::Format(...)` value (not a binding arm), so change it to an equality assertion on `Error::Json(json::Error::Cap)`; 1107 follows the same rule |

The two rules:

1. **those asserting the cap by exact string** (`lines.rs:268`/`308`/the neighbourhood of `308`, `document.rs:234`/`301`/`526`, `stream_large.rs:81`, `events.rs:871`/`1107`) → match `Error::Json(e)` and then assert `e == json::Error::Cap`, **or** keep the string comparison `assert_eq!(e.to_string(), "S3 select: input error: input record exceeds 1 MB")`. The latter's message is pinned verbatim by Task 3's `cap_renders_the_verbatim_crate_message`, so the two forms are equivalent.
2. **those asserting only a prefix or `contains("json input")`** (all the remaining places) → match `Error::Json(e)` and then make the same assertion against `e.to_string()`; **not one line of the asserted string content is changed**.

The assertion in `lines.rs` that is exact down to the full Display (`assert_eq!(crate::Error::Format(msg).to_string(), "S3 select: input error: input record exceeds 1 MB")`) can simply keep the string comparison, because the new variant's Display is completely identical to the old one.

- [ ] **Step 4: Taking over DocFraming's five tests (Q22=a+c)**

| Original test | New home and new name |
| --- | --- |
| `doc_array_elements_pull_one_at_a_time_with_offsets` | `document_element_offsets_are_byte_exact` in `document.rs`: for `[{"a": 1}, {"b": 2}]` assert the first record's `last_record_start() == 1` and the second's `== 11` (probe fact 7) |
| `doc_scalar_and_nested_elements` | `document_scalar_and_nested_elements` in `document.rs`: assert that `[1, "two", true, null, [1, 2], {"a": "x]y"}]` yields six elements, including the string containing `]` |
| `doc_single_object_pulls_once` | `document_single_object_root_start_is_zero` in `document.rs`: start is 0 |
| `doc_separator_errors_are_framing_errors` | `document_array_separator_errors_are_format_errors` in `document.rs`: all four of `[1,]`, `[1 2]`, `[1` and `[1] x` assert `Error::Json` (the first three come from struson's syntax checks and `[1] x` from the trailing check; probe facts 8 and 9) |
| `doc_empty_and_ws_only_modes` | `document_empty_vs_whitespace_only` in `document.rs`: empty input yields zero records, whitespace-only reports `Format` (probe fact 10) |

- [ ] **Step 5: Add eight (the spec's nine-item addition list minus the one Task 2 already took over)**

1. `cap_is_measured_over_the_whole_root`: an element of exactly 1 MB placed into an array, and a single-value root of exactly 1 MB, both pass in either shape; an element one byte over reports cap (taking over the voided `element_barely_under_cap_is_accepted`).
2. `cap_fires_after_the_prior_records`: the records before the over-limit element have been produced and none is produced after it (taking over the voided `budget_is_charged_per_root_not_accumulated`).
3. `tail_check_synthesizes_our_messages`: `{} {}` and `5 {}` contain `json input: trailing characters after the JSON document`; `{} x`, `5x` and `123#` do not (probe fact 9).
4. `document_duplicate_keys_collapse_last_wins_first_position`: a pin on the duplicate-key collapsing semantics that does not depend on the engine's internal implementation (replacing the three deleted reserved-key tests).
5. `document_root_shape_decided_before_first_read`: `peek()`'s probe of `{` and `[` does not cross the whole root (fact 3); for input with leading whitespace, assert the exact semantics with `data_pos`.
6. `lines_line_width_cap_still_fires_mid_serve`: `LineFraming`'s line-width cap still takes effect mid-read, and is **mapped to `Error::Json(Cap)`** by the `InvalidData` + `CAP_MESSAGE` arm of `json::Error::from` (the same job the retired `cap_or` did) rather than `Error::Io` (the pin for those two rules in Task 7).
7. `matcher_name_step_duplicate_reports_ambiguous_at_object_close`: the hold-back-then-decide-at-close semantics of a final-step `Name` (the handler-level counterpart of `final_name_holds_value_until_object_close` in Task 5's table).
8. `hostile_inputs_never_panic`: for a set of malformed inputs (including the shape that `peek`s again after the top-level value, deep nesting, truncated values, a lone surrogate, invalid UTF-8) assert that they only return `Err` and never panic (fact 4 shows the risk is real).

(`value_start_equals_data_pos_after_peek` in the spec's addition list duplicates `peek_never_moves_data_pos` from Task 2's guard tests, so no separate case is created for it.)

- [ ] **Step 6: Confirm green**

Run: `cargo test -p tinio-select`
Expected: all green. If an existing assertion goes red and this task did not name it, that is an implementation bug — **stop and report**; do not change the assertion.

- [ ] **Step 7: Checkpoint**

Report: `src/json/mod.rs`, `src/json/document.rs`, `src/json/lines.rs`, `tests/stream_large.rs` pending commit. **Do not run commit.**

---

### Task 10: Throughput comparison and documentation sync

**Files:**
- Modify: `crates/tinio-select/src/json/mod.rs` (place Task 1's baseline paragraph alongside this round's measurements)
- Modify: `crates/tinio-select/src/record.rs` (the `MAX_RECORD` comment)
- Modify: `docs/superpowers/specs/2026-09-12-json-streaming-struson-design.md` (write the measured numbers back)
- Modify: `docs/superpowers/specs/2026-09-04-select-object-content-design.md` and `docs/superpowers/plans/2026-09-04-select-object-content.md` (supersede annotations)

**Interfaces:**
- Consumes: Task 1's baseline and Task 7's implementation.
- Produces: a comparable throughput record and consistent documentation.

- [ ] **Step 1: Run the comparison**

Run: `cargo bench -p tinio-select --bench select_scan -- json_document_scan`
Expected: numbers come out. Record them alongside Task 1's baseline.

- [ ] **Step 2: Judge the threshold (acceptance criterion Q23=a)**

If the local `time` is slower than **the baseline Task 1 measured on the day** (that machine on that day is authoritative; 58.620 ms is only a historical reference value from the actson plan on another machine and does not constitute a threshold): **stop and assess**, write the numbers and criterion's change line into the report, and do not move into acceptance carrying a regression. If it is level or faster, continue.

- [ ] **Step 3: Write the numbers back**

Write Task 1's baseline paragraph and this round's measurements side by side into `json/mod.rs`'s module docs; fill the threshold sentence in the spec's acceptance-criteria section with this round's measurements (`struson kernel <time> / <thrpt>`). Write only measured values, never estimated ones; when criterion's change line has `p > 0.05`, note that the direction is trustworthy but the point estimate is not.

- [ ] **Step 4: The `MAX_RECORD` comment in `record.rs`**

```rust
/// One record root. The memory defence is that unmatched subtrees are never
/// materialized and nothing buffers the document, so this cap is a record
/// shape rule (and a DoS guard) rather than a memory backstop: a DOCUMENT
/// over 1 MB streams fine as long as every root value fits. Sampling points
/// are in the struson design spec, byte-supply section.
pub(crate) const MAX_RECORD: u64 = 1024 * 1024;
```

- [ ] **Step 5: Sync the two 2026-09-04 documents**

- `specs/2026-09-04-select-object-content-design.md`: change the `JsonReader` line to `DOCUMENT (root array -> one record per element; the object itself is the record otherwise; each root value <= 1 MB)`, and append at the end of the line `(superseded 2026-09-12: the streaming kernel is struson; see 2026-09-12-json-streaming-struson-design.md)`; append one superseded or amended annotation each to ScanRange's byte-0 pin and Streaming's 1 MB wording, keeping the original text without rewriting it.
- `plans/2026-09-04-select-object-content.md`: append supersede annotations to Task 9's and Task 10's byte-0 notes in the same way.

- [ ] **Step 6: Checkpoint**

Report: `src/json/mod.rs`, `src/record.rs`, and the three docs pending commit. **Do not run commit.**

---

### Task 11: Full acceptance

**Files:** none (a verification task)

**Interfaces:**
- Consumes: all the changes.
- Produces: the acceptance evidence.

- [ ] **Step 1: The standard acceptance legs (one by one; it is only done when all are green)**

```text
cargo test -p tinio-select
cargo test --workspace --exclude tinio-e2e
cargo test -p tinio-select --features parquet
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --all -- --check
```

- [ ] **Step 2: The large-document streaming test**

Run: `cargo test -p tinio-select --test stream_large`
Expected: passes (the LFS fixture is already in the repository).

- [ ] **Step 3: Manual check of the memory promises (written into the report)**

Answer each one and give the code location:

1. Is there anywhere that reads the whole document into memory? Expected: no; struson only has a fixed 1 KiB read buffer.
2. Is there anywhere that reads a whole line into memory? Expected: no; `LineFraming` supplies bytes one at a time.
3. Is there anywhere that materializes an unmatched subtree? Expected: no; the matcher drives `Cap` only for matched values and everything else goes through `skip_value()`.
4. With a single-value root and a deep wildcard, can the match set of the whole document still accumulate at once? Expected: no; a batch boundary is when one matched value completes, one record is delivered at a time, and `pending` is independent of root size.
5. Is the document content copied more than once anywhere? Expected: no; struson's token buffer is the current token, and a matched value's string goes into the record as `to_owned` directly.

- [ ] **Step 4: Spec-coverage self-check**

Check the spec's deviations ① to ⑬ and its 12 verified facts one by one: each one must point at a task or a test. If any one has no owner, add a test or add documentation; do not leave it blank.

- [ ] **Step 5: Checkpoint (final report)**

Report: the complete list of files pending commit (paste `git status --short` verbatim), the results of the four steps above, and a point-by-point report of every "implementation adjustment that deviated from the plan during implementation". **Do not run any git write operation.**

---

## Self-Review

**1. Spec coverage**

| Spec section or requirement | Task that carries it out |
| --- | --- |
| Verified fact 1 (ReadSettings' six fields and their defaults) | Task 2 (guard tests) + Task 7 (explicit settings) |
| Verified fact 3 (peek does not move data_pos) | Task 2 (guard tests) + Task 7 (sampling) |
| Verified fact 4 (peek after the top level panics) | Task 2 (guard) + Task 7 (always through consume_trailing_whitespace) |
| Verified fact 5 (the skip path validates surrogate pairs too) | Task 2 (guard) + Task 9 (existing assertions kept) |
| Verified fact 6 (IoError/InvalidData for invalid UTF-8) | Task 2 (guard) + Task 3 (InvalidUtf8 discrimination) |
| Verified fact 7 (element spans and sampling points) | Task 6 (RootBudget) + Task 7 (sampling) + Task 9 (byte-exact assertions) |
| Verified fact 8 (what an unclosed array produces) | Task 7 (error timing) + Task 9 (rows_before assertions kept) |
| Verified fact 9 (the three shapes of trailing data) | Task 7 (endgame check) + Task 9 (true/false distribution) |
| Verified fact 10 (empty input and whitespace-only) | Task 7 (the zero-record and Format branches) |
| Verified fact 11 (verbatim tokens and number restrictions) | Task 7 (next_number_as_str + restrict_number_values: false) |
| The parsing kernel and the PathSeg state machine | Task 5 |
| Byte supply and the three-part cap | Task 6 + Task 7 |
| The handlers (LINES and DOCUMENT) | Task 7 |
| Error handling (the Json variant, hand-written PartialEq, verbatim messages) | Task 3 + Task 9 |
| Deviations ① to ⑬ | Task 7 (behaviour) + Task 9 (test alignment) + Task 10 (docs) |
| The test plan (migration and additions) | the inline tests of Tasks 2, 4, 5, 6 + Task 9 |
| Acceptance criteria (the five legs, the threshold, the memory check) | Task 1 + Task 10 + Task 11 |
| Sync updates (the three old documents and record.rs) | Task 8 (module docs) + Task 10 |

**2. Placeholder scan**

- Task 7's two skeletons have been rewritten per the 2026-09-13 review: `pull_line` and `pull_root` are both executable shapes, and `cap_or` and `map_json_err` are the two small helpers that must be implemented (the rules are written out). **No leftover placeholder names.**
- Task 5 Step 1's `pending_high` case gives the way to drive it and the assertion; the implementer needs to fill in the complete loop (the shape of the instruction-table-driven loop depends on their own `Script` implementation — this is the only place in this plan where the implementer has to write code themselves).
- There is no TBD, TODO, to-be-added, or "similar to Task N".

**2b. Three conventions that must be kept in sync at implementation time**

- **`Tail::advance`'s borrow conflict (E0499)**: its signature needs both a `&mut Matcher` and a closure that captures `&mut self`, so calling it directly as `self.tail.advance(&mut self.matcher, |s, m| ...)` hits E0499. Follow what today's `core.rs` does: **destructure the fields out first** (`let Self { tail, matcher, .. } = self;`) and then call `tail.advance(matcher, |tail, matcher| ...)`, so the closure only touches the two destructured references.
- **What `matcher` needs to expose**: `Tail` only uses `Matcher::{take_record, drop_pending}`; `drop_pending` must be added to Task 5's interface block (`take_record` is already there).
- **The producer of `Progress::End`**: it is returned by `Matcher::on_value` when the source returns EOF (`peek` reports `IncompleteDocument` and `data_pos` shows the input is exhausted); `next_root` only passes it through. The two handlers must **not** judge EOF themselves — otherwise this variant becomes dead code and Task 8's clippy leg will flag it as an unused variant.

**3. Type consistency**

- `ValueSource`'s 13 methods align one by one with Task 7's `ReaderSource` implementation.
- `Progress::{StopRoot, End}` is defined in Task 5 — the private `step` reports "still in flight" as `None`, so no caller ever sees a third variant — and consumed in two places in Task 7; consistent.
- `RootBudget::{new, with_default_cap, begin, begin_element, check, check_or_cap, check_root_span, fired}` is defined in Task 6 and consumed in Task 7; consistent.
- `Matcher::{new, on_value, next_root, pending_len, take_record, pending_high}` is defined in Task 5 (Global Constraints' interface table) and consumed by Tasks 7 and 9; consistent.
- `json::Error`'s eight variants are defined in Task 3; Task 7 uses `Cap`, and Task 9 asserts that `Cap`'s Display is verbatim unchanged.
- `Tail::{new, advance}` and the fields `last_record_start`/`done`/`failure` are defined in Task 7 Step 1 and used in both handlers.

**Known cross-task evolution (must be kept in sync)**: `json::Error` defines nine variants in Task 3 (eight plus `Other(String)`); Task 7's skeleton uses `Other` to carry the two messages of our own trailing checks (`json input: trailing characters on line` and `... after the JSON document`) and the catch-all parse error. If the implementer would rather give trailing data a separate variant with a field (for example `Trailing { in_line: bool }`), that is fine too — but they must go back to Task 3 and sync `PartialEq`, and guarantee that Task 9's message assertions are verbatim identical. Both routes are legal; **do not keep two sets side by side**.
