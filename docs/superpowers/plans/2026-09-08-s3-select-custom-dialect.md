# S3 Select custom sqlparser Dialect Implementation Plan

> **Execution status (2026-09-10): implemented in full.** All six tasks landed; every task review and the final whole-branch review are clean, plus four follow-up waves (/code-review two-axis, deferred-minor cleanup, /simplify, leftover pins). Test state: **284+2** unit/doc in tinio-select, **1290** workspace (excl. e2e), **228** e2e scenarios. Nothing is committed (user ruling 2026-09-09: the branch commit stays the user's call). This plan is retained as the historical execution record; the current authority is the spec — a few intermediates here were superseded during review and the spec carries the final wording (e.g. `RowCtx.missing` became the once-per-request `missing_name` derived in `Engine::new`; the (line,col)→byte converter became the monotonic `SpanCursor`; the FROM trigger token scan became the tokenize-once `candidate` in `S3SelectDialect::new`).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two text rewrites in `tinio-select/sql.rs` (the FROM-path preprocessor + the `IS [NOT] MISSING` rewrite) with a custom sqlparser Dialect, parse the path grammar with pest, and make the sentinel function name a per-request uuid carried to the engine on the QueryPlan (deleting `reject_reserved_functions`).

**Architecture:** The new `dialect.rs`'s `S3SelectDialect` delegates all 71 GenericDialect overrides in full, plus two hooks: `parse_statement` (token scan probing `S3Object[`/`S3Object.`; takes over SELECT-led statements only, everything else falls back to stock) and `parse_infix` (`IS [NOT] MISSING` → sentinel `Expr::Function`, name from the request's `SentinelNames`; `IS NOT MISSING` = `UnaryOp{Not}` wrapping the same sentinel). `src/grammar/object_path.pest` defines the 4 segment forms with `$` atomic rules; the dialect takes the consumed length from the pest prefix parse, then consumes tokens by byte offset (with the boundary-alignment and refused-continuation checks). Task boundaries split along two orthogonal feature axes — the IS MISSING axis lands atomically (engine matching and sentinel generation must be one task, or the intermediate state breaks every test outright), the FROM factor axis lands independently.

**Tech Stack:** sqlparser 0.62.0 (pinned), pest + pest_derive 2.7 (new workspace pin), uuid 1.x (already workspace-pinned; tinio-select enables v4).

**Spec:** `docs/superpowers/specs/2026-09-07-s3-select-custom-dialect-design.md` (required reading — the plan argues from the spec; the spec is already synced with the uuid ruling and this plan's error-surface stance).

## Global Constraints

- `unsafe_code = "forbid"` stays; pest/pest_derive generate no unsafe code — if verification finds an exception, `#[allow(unsafe_code)]` is scoped to the generated parser module only, with a justifying comment.
- English-only: code comments, commit messages, test names.
- Workspace dependency pin, once: `pest = "2.7"`, `pest_derive = "2.7"` into `[workspace.dependencies]`; crates use `X.workspace = true`; uuid is pinned already — tinio-select adds `uuid = { workspace = true, features = ["v4"] }`.
- **Never auto commit/push** — each task's closing commit is a request for user approval, executed after the user confirms.
- `sql::parse(&str) -> Result<QueryPlan, Error>` signature unchanged; `QueryPlan` gains `pub missing: SentinelNames` (user ruling 2026-09-08 — the single interface change); `FromClause`/`PathSeg`/`Projection` unchanged.
- Error messages at our discretion (user ruling 2026-09-08), old text not preserved; e2e asserts only 400 + `S3QueryParsingError`.
- Each task ends with an independently testable deliverable; no intermediate state may carry a known-failing test.

## File Structure

```
crates/tinio-select/
  Cargo.toml                     modify — pest, pest_derive (workspace), uuid v4
  src/dialect.rs                 new — S3SelectDialect: 71 method delegations + parse_statement/
                                      parse_infix hooks + SELECT skeleton + factor parsing + sentinel construction
  src/path.rs                    new — pest #[derive(Parser)] + Rule→PathSeg
  src/grammar/object_path.pest   new — pest grammar (4 segment forms)
  src/sql.rs                     modify — parse() rewiring, QueryPlan.missing, delete rewrite/reserved/
                                      preprocess, validate_from quoted-S3Object rejection, from dual source
  src/engine.rs                  modify — RowCtx.missing, sentinel match via ctx.missing, delete __s3_is_not_missing arm
  src/json.rs                    modify — tests only (Q13 alignment, code untouched)
  src/lib.rs                     modify — register dialect/path modules
Cargo.toml (workspace)           modify — [workspace.dependencies] add pest/pest_derive
```

Task dependencies: Task 1 (deps) and Task 2 (pest path layer) have no code dependency on each other, but Task 2's `cargo test path::` needs `path` registered in `lib.rs` — if the two run in parallel, Task 2 registers it itself (otherwise Task 1 Step 4 does); Task 3 (IS MISSING axis) depends on Task 1; Task 4 (FROM factor axis) depends on Task 1+2+3; Task 5 (json.rs tests) is independent; Task 6 (regression + cleanup) last.

---

### Task 1: Dependencies + type skeleton

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `crates/tinio-select/Cargo.toml`
- Modify: `crates/tinio-select/src/lib.rs`

**Interfaces:**
- Produces: `sql::SentinelNames { pub uuid: uuid::Uuid }` (`Debug + Clone + PartialEq`) and `SentinelNames::is_missing(&self) -> String` (= `format!("__s3_is_missing_{}", self.uuid.simple())` — **the crate's single name-construction point**); `QueryPlan.pub missing: SentinelNames`; `IS NOT MISSING` wraps the sentinel call in `Expr::UnaryOp{ op: Not }` (no second name — the engine's `UnaryOperator::Not` arm already exists, engine.rs:882-890).

- [ ] **Step 1: workspace pins**

In `Cargo.toml` `[workspace.dependencies]`, add in alphabetical order:

```toml
pest = "2.7"
pest_derive = "2.7"
```

- [ ] **Step 2: crate dependencies**

`crates/tinio-select/Cargo.toml` `[dependencies]`, add in alphabetical order:

```toml
pest.workspace = true
pest_derive.workspace = true
uuid = { workspace = true, features = ["v4"] }
```

- [ ] **Step 3: types**

`crates/tinio-select/src/sql.rs`, add before `QueryPlan`:

```rust
/// Per-request MISSING sentinel. Only the uuid is stored; the full function
/// name is `__s3_is_missing_<uuid>` (the prefix is a debug/tracing marker —
/// not a reserved-name attack surface; the unguessable part is the uuid).
/// `IS NOT MISSING` reuses the same name: `UnaryOp{Not}` wraps the call.
#[derive(Debug, Clone, PartialEq)]
pub struct SentinelNames {
    pub uuid: uuid::Uuid,
}

impl SentinelNames {
    /// This request's full sentinel function name — the crate's single
    /// construction point.
    pub fn is_missing(&self) -> String {
        format!("__s3_is_missing_{}", self.uuid.simple())
    }
}
```

`QueryPlan` gains the field:

```rust
pub struct QueryPlan {
    pub from: FromClause,
    pub projections: Vec<Projection>,
    pub where_expr: Option<Expr>,
    pub limit: Option<usize>,
    pub aggregates: bool,
    pub missing: SentinelNames,
}
```

- [ ] **Step 4: register modules**

`crates/tinio-select/src/lib.rs`, add:

```rust
pub mod dialect;
pub mod path;
```

(`dialect.rs`/`path.rs` start as empty shell files with module doc comments this task; once this step registers them in lib.rs, creating the two empty files compiles.)

- [ ] **Step 5: compile check**

Run: `cargo build -p tinio-select`
Expected: compile error — `sql.rs`'s `parse()` builds a `QueryPlan` missing the `missing` field. **Fix** (done in this same step):

The construction site in `parse()` (sql.rs:223):

```rust
    Ok(QueryPlan {
        from,
        projections,
        where_expr: select.selection.clone(),
        limit,
        aggregates,
        missing,
    })
```

`missing` is minted at the top of `parse()` (final state for this task; Task 3 wires it into the dialect and the engine):

```rust
pub fn parse(sql: &str) -> Result<QueryPlan, Error> {
    if sql.len() > MAX_EXPRESSION {
        return Err(Error::Parse("expression exceeds 256 KiB".into()));
    }
    let missing = SentinelNames {
        uuid: uuid::Uuid::new_v4(),
    };
    ...
```

- [ ] **Step 6: run tests**

Run: `cargo test -p tinio-select`
Expected: all green (`missing` only rides along on the plan, no consumer yet — sentinel-name assertions still use the old fixed name, unaffected).

- [ ] **Step 7: Commit (execute after user confirmation)**

```bash
git add Cargo.toml crates/tinio-select/Cargo.toml crates/tinio-select/src/sql.rs crates/tinio-select/src/lib.rs crates/tinio-select/src/dialect.rs crates/tinio-select/src/path.rs
git commit -m "feat(select): add SentinelNames, pest and uuid deps"
```

---

### Task 2: `path.rs` + pest grammar (parallelizable with Task 1; registers the `path` module itself)

**Files:**
- Create: `crates/tinio-select/src/grammar/object_path.pest`
- Modify: `crates/tinio-select/src/path.rs`
- Modify: `crates/tinio-select/src/lib.rs` (only if Task 1 has not landed first — registers `pub mod path;`)

**Interfaces:**
- Produces: `path::validate_full(&str) -> Result<Vec<PathSeg>, String>` (full-text `main` match); `path::parse_path(&str) -> Result<(Vec<PathSeg>, usize), String>` (prefix `path` rule, consumed byte count); `PathSeg` reuses `crate::sql::PathSeg`.

- [ ] **Step 1: grammar file `crates/tinio-select/src/grammar/object_path.pest`**

```pest
main = ${ SOI ~ path ~ EOI }
path = ${ "^S3Object" ~ (first_seg ~ seg*)? }
first_seg = { "[" ~ "*" ~ "]" }
seg = _{ dot_name | dot_wild | bracketed }
dot_name = { "." ~ name }
dot_wild = { "." ~ "*" }
wstar = { "*" }
bracketed = { "[" ~ (wstar | index | quoted) ~ "]" }
name = { (ASCII_ALPHANUMERIC | "_" | "$")+ }
index = { ASCII_DIGIT+ }
quoted = { "'" ~ (!"'" ~ ANY)* ~ "'" }
```

(`$` compound-atomic: suppresses implicit whitespace but keeps interior tokens — `@` would make interior rules silent, with no segment pairs to walk; no `WHITESPACE` rule is defined, zero-whitespace semantics; `^"S3Object"` is ASCII case-insensitive.)

- [ ] **Step 2: `crates/tinio-select/src/path.rs`**

```rust
//! pest grammar for FROM path segments (jsonpath-rust layout: grammar file +
//! #[derive(Parser)] + manual Pair→model). The dialect uses the prefix-parse
//! `path` rule for consumed length; the `main` rule (EOI) is the test entry.

use pest::iterators::Pair;
use pest::Parser;

use crate::sql::PathSeg;

#[derive(Parser)]
#[grammar = "grammar/object_path.pest"]
struct ObjectPathParser;

const ERR: &str = "invalid FROM path";

/// Full-text match (`main` rule, with EOI) — test entry.
pub fn validate_full(input: &str) -> Result<Vec<PathSeg>, String> {
    let mut pairs = ObjectPathParser::parse(Rule::main, input)
        .map_err(|e| format!("{ERR}: {e}"))?;
    let path = pairs
        .next()
        .ok_or_else(|| ERR.to_string())?
        .into_inner()
        .next()
        .ok_or_else(|| ERR.to_string())?;
    walk_path_pair(path)
}

/// Prefix match (`path` rule, no EOI) — used by the dialect.
/// Returns (segments, consumed byte length).
pub fn parse_path(input: &str) -> Result<(Vec<PathSeg>, usize), String> {
    let pairs = ObjectPathParser::parse(Rule::path, input).map_err(|e| format!("{ERR}: {e}"))?;
    let pair = pairs.into_iter().next().ok_or_else(|| ERR.to_string())?;
    let len = pair.as_span().end();
    let segs = walk_path_pair(pair)?;
    Ok((segs, len))
}

fn walk_path_pair(pair: Pair<'_, Rule>) -> Result<Vec<PathSeg>, String> {
    let mut out = Vec::new();
    for seg in pair.into_inner() {
        match seg.as_rule() {
            // Container rules: recurse AND merge — `first_seg` yields `wstar`,
            // `bracketed` yields `wstar`/`index`/`quoted`. Dropping the
            // recursion result silently drops segments.
            Rule::path | Rule::first_seg | Rule::bracketed => {
                out.extend(walk_path_pair(seg)?);
            }
            Rule::dot_name => {
                let p = seg.into_inner().next().unwrap(); // name
                out.push(PathSeg::Name(p.as_str().to_string()));
            }
            Rule::dot_wild => out.push(PathSeg::Wild),
            Rule::wstar => out.push(PathSeg::Wild),
            Rule::index => {
                let v: usize = seg.as_str().parse().map_err(|_| ERR.to_string())?;
                out.push(PathSeg::Index(v));
            }
            Rule::quoted => {
                // `['name']`: raw content (no escapes), outer quotes stripped.
                let s = seg.as_str();
                let name = s.trim_matches('\'').to_string();
                out.push(PathSeg::Name(name));
            }
            _ => {}
        }
    }
    Ok(out)
}
```

- [ ] **Step 3: tests (`#[cfg(test)]` inside `path.rs`)**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> Vec<PathSeg> {
        validate_full(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    fn rej(s: &str) {
        assert!(validate_full(s).is_err(), "{s}: expected rejection");
    }

    #[test]
    fn s3object_alone_is_valid() {
        assert_eq!(ok("S3Object"), vec![PathSeg::Wild; 0]);
    }

    #[test]
    fn wildcard_first_segment() {
        // Pins the walker merge: `first_seg` recurses to `wstar`.
        assert_eq!(ok("S3Object[*]"), vec![PathSeg::Wild]);
    }

    #[test]
    fn traversal_path_full() {
        assert_eq!(
            ok("S3Object[*].books[*].price"),
            vec![
                PathSeg::Wild,
                PathSeg::Name("books".into()),
                PathSeg::Wild,
                PathSeg::Name("price".into()),
            ]
        );
    }

    #[test]
    fn index_segment() {
        // Pins the walker merge: `bracketed` recurses to `index`.
        assert_eq!(
            ok("S3Object[*].books[0]"),
            vec![PathSeg::Wild, PathSeg::Name("books".into()), PathSeg::Index(0)]
        );
    }

    #[test]
    fn quoted_name_segment() {
        assert_eq!(
            ok("S3Object[*]['a b']"),
            vec![PathSeg::Wild, PathSeg::Name("a b".into())]
        );
    }

    #[test]
    fn dot_wild_segment() {
        assert_eq!(ok("S3Object[*].*"), vec![PathSeg::Wild, PathSeg::Wild]);
    }

    #[test]
    fn case_insensitive_root() {
        assert_eq!(
            ok("s3object[*].name"),
            vec![PathSeg::Wild, PathSeg::Name("name".into())]
        );
    }

    #[test]
    fn dollar_in_names() {
        assert_eq!(
            ok("S3Object[*].a$b"),
            vec![PathSeg::Wild, PathSeg::Name("a$b".into())]
        );
    }

    #[test]
    fn no_dot_first_segment() {
        rej("S3Object.name");
    }

    #[test]
    fn no_index_first_segment() {
        rej("S3Object[0]");
    }

    #[test]
    fn index_overflow_is_rejected() {
        rej("S3Object[*].books[99999999999999999999]");
    }

    #[test]
    fn unterminated_quoted_segment_is_rejected() {
        // pest-unit-level only: end-to-end this input dies in parse_sql's
        // upfront tokenization before any hook runs.
        rej("S3Object[*].'books");
    }

    #[test]
    fn quoted_content_with_quote_is_rejected() {
        // No-escape rule: raw up to the closing quote.
        rej("S3Object[*]['it''s']");
    }

    #[test]
    fn affix_parse_consumes_only_path() {
        let (segs, len) = parse_path("S3Object[*].books[*].price s WHERE x = 1").unwrap();
        assert_eq!(segs.len(), 4);
        assert_eq!(len, "S3Object[*].books[*].price".len());
    }

    #[test]
    fn prefix_parse_plain_factor() {
        let (segs, len) = parse_path("S3Object s").unwrap();
        assert!(segs.is_empty());
        assert_eq!(len, "S3Object".len());
    }

    #[test]
    fn non_ascii_is_rejected() {
        rej("S3Object[*].café");
    }

    #[test]
    fn whitespace_not_allowed() {
        rej("S3Object [*]");
    }
}
```

- [ ] **Step 4: run**

Run: `cargo test -p tinio-select path::`
Expected: all 17 tests green.

- [ ] **Step 5: Commit (on user confirmation)**

```bash
git add crates/tinio-select/src/grammar/object_path.pest crates/tinio-select/src/path.rs
git commit -m "feat(select): pest grammar and path parser"
```

---

### Task 3: IS MISSING axis lands atomically (dialect delegation + parse_infix + engine + delete rewrite/reserved)

> **Why one task**: engine uuid-name matching and sentinel generation must be one state. If the engine changes first (matching `ctx.missing`) while the old `rewrite_is_missing` still mints the fixed name `__s3_is_missing`, the two coexist and every IS MISSING test fails immediately. So this task deletes the rewrite + enables the hook + changes the engine + updates the test assertions in one pass.

**Files:**
- Create: `crates/tinio-select/src/dialect.rs` (delegation + parse_infix + sentinel construction + positions)
- Modify: `crates/tinio-select/src/sql.rs` (parse() wiring + drop the rewrite_is_missing call + delete reject_reserved_functions/SENTINEL_FUNCTIONS + test-assertion updates + delete the reserved test)
- Modify: `crates/tinio-select/src/engine.rs` (RowCtx.missing + matching swap + delete the `__s3_is_not_missing` arm)

**Interfaces:**
- Consumes: `SentinelNames`; produces `dialect::S3SelectDialect::new(missing: SentinelNames, sql: &str)`, `.take_from() -> Option<FromClause>` (the from slot stays unwritten this task, enabled in Task 4), `positions` moves into this file.

- [ ] **Step 1: `crates/tinio-select/src/dialect.rs`**

This task implements: the `S3SelectDialect` struct, the 71 method delegations, the `parse_infix` hook, `sentinel_function`, `positions`. The `parse_statement` hook is not implemented this task (the trait default returns None — stock parsing, behavior unchanged).

**Two structural facts (verified against the 0.62.0 source — do not route around them):**
1. `Dialect: Debug + Any` (dialect/mod.rs:204) — `Any` carries a `'static` bound, so the dialect **cannot borrow** an `&'a str` and must own a `String` (≤256 KiB, one copy per request — acceptable); and it must `#[derive(Debug)]`, or the impl does not hold.
2. `IS NOT MISSING` returns the `UnaryOp{Not}` wrapper from this task on — `SentinelNames` has one name only; `sentinel_function` takes only `&str` (single name). The engine's `UnaryOperator::Not` arm (engine.rs:882-890) is semantically == the old second sentinel (the sentinel arm always returns Bool, engine.rs:957-963).

```rust
//! Custom sqlparser Dialect: the `IS [NOT] MISSING` hook + the custom FROM
//! factor (Task 4 adds parse_statement). Every other method delegates to
//! GenericDialect — an incomplete delegation silently changes stock-path
//! behavior (e.g. supports_limit_comma).

use std::cell::RefCell;
use std::collections::HashMap;

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments,
    Ident, Keyword, ObjectName, ObjectNamePart, Statement, UnaryOperator,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithSpan};

use crate::sql::{FromClause, PathSeg, SentinelNames};

// Debug is required by the Dialect trait; String (not &str) because
// Dialect: Any imposes 'static.
#[derive(Debug)]
pub struct S3SelectDialect {
    inner: GenericDialect,
    missing: SentinelNames,
    /// FromClause captured by the custom FROM factor; parse() takes it
    /// before building the QueryPlan.
    captured: RefCell<Option<FromClause>>,
    sql: String,
}

impl S3SelectDialect {
    pub fn new(missing: SentinelNames, sql: &str) -> Self {
        Self {
            inner: GenericDialect,
            missing,
            captured: RefCell::new(None),
            sql: sql.to_string(),
        }
    }

    /// Take the FromClause captured this parse (custom-path statements only).
    pub fn take_from(&self) -> Option<FromClause> {
        self.captured.borrow_mut().take()
    }
}

/// (line, col) → byte offset: 0.62 spans carry only line/column, so slicing
/// needs this converter. Read-only; never splices text.
pub(crate) fn positions(sql: &str) -> HashMap<(u64, u64), usize> {
    let mut map = HashMap::new();
    let mut line = 1u64;
    let mut col = 1u64;
    let mut offset = 0usize;
    map.insert((line, col), offset);
    for c in sql.chars() {
        offset += c.len_utf8();
        if c == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
        map.insert((line, col), offset);
    }
    map
}

/// Sentinel call: single-part, unquoted name, one argument (the shape the
/// engine's sentinel_operand expects).
fn sentinel_function(name: &str, operand: &Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            clauses: Vec::new(),
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Box::new(
                operand.clone(),
            )))],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

impl sqlparser::dialect::Dialect for S3SelectDialect {
    // -----------------------------------------------------------------
    // GenericDialect delegation: one forwarding line per method.
    // The full list is generic.rs's impl (71 overrides). Let compile
    // errors drive completion. The behavior-critical ones (drift if
    // missed): is_identifier_start/is_identifier_part,
    // is_delimited_identifier_start, supports_limit_comma,
    // supports_limit_by, supports_group_by_expr,
    // supports_empty_projections, supports_parens_around_table_factor,
    // supports_select_wildcard_except, supports_projection_trailing_commas,
    // supports_filter_during_aggregation, supports_struct_literal,
    // supports_unicode_string_literal, ...
    // -----------------------------------------------------------------
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.inner.is_delimited_identifier_start(ch)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.inner.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.inner.is_identifier_part(ch)
    }

    // ...the remaining ~68 methods forward the same way; copy the
    //   signatures from generic.rs and replace each body with
    //   `self.inner.<method>(<args>)`...

    // -----------------------------------------------------------------
    // IS [NOT] MISSING
    // -----------------------------------------------------------------
    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        // The current token must be IS (not yet consumed).
        if !parser.peek_keyword(Keyword::IS) {
            return None;
        }
        // Look ahead (peek consumes nothing): optional NOT — compared by
        // KEYWORD, never by Token::make_keyword("NOT") equality (Word::value
        // keeps the user's casing, so equality would miss lowercase `not`
        // that the old byte scan accepted) — then a `missing` word.
        let mut n = 1;
        let negated = matches!(
            &parser.peek_nth_token(n).token,
            Token::Word(w) if w.quote_style.is_none() && w.keyword == Keyword::NOT
        );
        if negated {
            n += 1;
        }
        if !is_missing_word(&parser.peek_nth_token(n)) {
            return None; // IS NOT NULL / IS DISTINCT FROM etc. → stock
        }
        // Confirmed: consume IS [NOT] MISSING (next_token skips whitespace —
        // advance_token's loop, parser/mod.rs:4528).
        parser.parse_keyword(Keyword::IS);
        if negated {
            parser.parse_keyword(Keyword::NOT);
        }
        parser.next_token(); // the `missing` word

        // Chained rejection: in `X IS [NOT] MISSING IS MISSING` the operand
        // is already a sentinel — a bare Function, or our own UnaryOp{Not}
        // wrapper (the NOT is this hook's production for IS NOT MISSING,
        // not a user Nested). Both shapes are rejected.
        let sentinel_name = self.missing.is_missing();
        if is_sentinel_expr(expr, &sentinel_name) {
            return Some(Err(ParserError::ParserError(
                "invalid IS MISSING expression".into(),
            )));
        }

        // One name: IS NOT MISSING = UnaryOp{Not} around the sentinel call.
        let call = sentinel_function(&sentinel_name, expr);
        Some(Ok(if negated {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(call),
            }
        } else {
            call
        }))
    }
}

/// Unquoted, case-insensitive `missing` word (a NoKeyword token).
fn is_missing_word(t: &TokenWithSpan) -> bool {
    match &t.token {
        Token::Word(w) if w.quote_style.is_none() => w.value.eq_ignore_ascii_case("missing"),
        _ => false,
    }
}

/// Is the expression already a sentinel call: a bare `Function(sentinel)`,
/// or this hook's own `UnaryOp{Not}` wrapper around one (the shape of a
/// chained IS NOT MISSING IS MISSING).
fn is_sentinel_expr(expr: &Expr, sentinel: &str) -> bool {
    match expr {
        Expr::Function(f) => f.name.to_string() == sentinel,
        Expr::UnaryOp { op: UnaryOperator::Not, expr } => is_sentinel_expr(expr, sentinel),
        _ => false,
    }
}
```

**Implementation hint (delegation completeness)**: in the `sqlparser` source, `src/dialect/generic.rs`'s `impl Dialect for GenericDialect` runs from line 26 to end of file (315 lines), 71 `fn`s total. Copying that impl block into ours and replacing each body with a forwarding call is mechanical and leak-proof — **do not hand-transcribe a list**; let the compiler report missing methods and fill them in one by one, then audit against generic.rs once for reverse overrides (every method we override must forward).

- [ ] **Step 2: `sql.rs` parse() wiring**

```rust
pub fn parse(sql: &str) -> Result<QueryPlan, Error> {
    if sql.len() > MAX_EXPRESSION {
        return Err(Error::Parse("expression exceeds 256 KiB".into()));
    }
    let missing = SentinelNames {
        uuid: uuid::Uuid::new_v4(),
    };
    let (from, rewritten) = preprocess_from(sql)?; // deleted only in Task 4 — see below
    let dialect = S3SelectDialect::new(missing.clone(), &rewritten);
    let stmts = Parser::parse_sql(&dialect, &rewritten)
        .map_err(|e| Error::Parse(format!("syntax error: {e}")))?;
    // Single-statement check + Query-shape check + validators unchanged;
    // only the rewrite_is_missing call line is deleted from parse().
    // QueryPlan construction gains missing: missing,
}
```

Change list:
1. Delete the `reject_reserved_functions(sql)?;` line.
2. Delete the `let rewritten = rewrite_is_missing(&rewritten)?;` line.
3. **`preprocess_from` is retained (deleted in Task 4), and the intermediate state must feed its rewritten text**: this task has no `parse_statement` hook yet, and stock cannot parse `S3Object[*]` — feeding the original sql to `parse_sql` breaks every traversal test (violating "no known-failing test in an intermediate state"). So: `let (from, rewritten) = preprocess_from(sql)?;`, and both the dialect and `parse_sql` take `&rewritten`. preprocess leaves text outside FROM alone, so `IS MISSING` survives verbatim and is caught by the `parse_infix` hook (the rewrite is gone); the `sql` copy the dialect holds is read by nobody this task (factor parsing is enabled only in Task 4) — storing the rewritten text keeps it consistent with the parse input. Leave one test, `traversal_plus_missing_rewrite`, to verify the intermediate state.
4. Delete the `reject_reserved_functions` function and the `SENTINEL_FUNCTIONS` constant (uuid ruling: the name is unguessable, no replacement guard needed — behavior consequence in the spec's Error surface: a user-authored fixed-name `__s3_is_missing(x)` becomes an unknown-function error at engine eval time, on the in-stream channel).
5. Delete the `rewrite_is_missing` function and its private helpers (`is_word`, `skip_whitespace`, `token_start`/`token_end`).

- [ ] **Step 3: `engine.rs` matching swap**

```rust
struct RowCtx<'a> {
    record: &'a Record,
    alias: &'a Option<String>,
    missing: &'a SentinelNames,
}
```

The two construction sites (`next` and `aggregate_next`, engine.rs:141/185 — verified as the only two) gain `missing: &self.plan.missing,`.

`eval`'s Function arm (engine.rs:957 on):

```rust
            let m = ctx.missing.is_missing();
            if is_sentinel_call(&f.name, &m) {
```

**Delete** the `__s3_is_not_missing` arm (engine.rs:964-970) — IS NOT MISSING now arrives as `UnaryOp{Not}` and takes the Not arm (882-890). Add `use sql::{..., SentinelNames}` to the imports.

- [ ] **Step 4: test-assertion updates (sql.rs)**

1. `is_missing_after_non_ascii_text` (around line 1238):
```rust
        let s = plan.where_expr.unwrap().to_string();
        assert!(s.contains(&plan.missing.is_missing()), "{s}");
```
(name = `__s3_is_missing_` prefix + 32 hex uuid; take it from the plan, never hardcode.)

2. `is_missing_rewrite` (line 1353 on) — take the name from the plan, and assert the `UnaryOp{Not}` shape for IS NOT MISSING:
```rust
    #[test]
    fn is_missing_rewrite() {
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x IS MISSING");
        let m = q.missing.is_missing();
        match q.where_expr {
            Some(Expr::Function(f)) => assert_eq!(f.name.to_string(), m),
            other => panic!("expected missing sentinel, got {other:?}"),
        }
        // IS NOT MISSING = UnaryOp{Not} around the same sentinel (no second name).
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x IS NOT MISSING");
        let m = q.missing.is_missing();
        match q.where_expr {
            Some(Expr::UnaryOp { op: UnaryOperator::Not, expr }) => match expr.as_ref() {
                Expr::Function(f) => assert_eq!(f.name.to_string(), m),
                other => panic!("expected sentinel under NOT, got {other:?}"),
            },
            other => panic!("expected NOT-wrapped sentinel, got {other:?}"),
        }
        // Lowercase form: the hook compares NOT by keyword, not by text.
        let q = ok("SELECT s.x FROM S3Object s WHERE s.x is not missing");
        assert!(matches!(q.where_expr, Some(Expr::UnaryOp { .. })));
    }
```

3. `is_missing_chained_forms_are_parse_errors` (line 1377 on): the match after the trailing `ok(...)` likewise takes the name from `q.missing.is_missing()` (and asserts a chained `IS NOT MISSING IS MISSING` still reports invalid IS MISSING expression — the hook's chain check recognizes the `UnaryOp{Not}`-wrapped shape).

4. `traversal_plus_missing_rewrite` (line 1398 on): same — `q.missing.is_missing()`.

5. The `s.from IS MISSING` match in `keyword_named_fields_are_not_keywords` (line 1431 on): same — `q.missing.is_missing()`.

6. **Delete** the `reject_reserved_sentinel_functions` test (lines 1263-1278), replaced by:

```rust
    #[test]
    fn user_sentinel_like_call_is_an_unknown_function() {
        // R15 guard deleted: the sentinel name is a per-request uuid, user
        // text cannot collide. A fixed-name `__s3_is_missing(x)` call is an
        // ordinary unknown function — legal at parse time, rejected by the
        // engine during eval (in-stream error channel, not request-level 400).
        let q = ok("SELECT __s3_is_missing(s.a) FROM S3Object s");
        assert!(!q.aggregates);
    }
```

- [ ] **Step 5: run**

Run: `cargo test -p tinio-select`
Expected: all green.

- [ ] **Step 6: Commit (on user confirmation)**

```bash
git add crates/tinio-select/src/dialect.rs crates/tinio-select/src/sql.rs crates/tinio-select/src/engine.rs crates/tinio-select/src/lib.rs
git commit -m "refactor(select): uuid sentinels, infix hook, drop IS MISSING rewrite"
```

---

### Task 4: FROM factor axis (parse_statement hook + delete preprocess + validate_from adjustment)

**Files:**
- Modify: `crates/tinio-select/src/dialect.rs`
- Modify: `crates/tinio-select/src/sql.rs`
- Modify: `crates/tinio-select/src/path.rs` (if needed)

**Interfaces:**
- Consumes: `path::parse_path`; `dialect::positions`; `crate::sql::FromClause`.
- Produces: the full `parse_statement`; `take_from()` populated (custom statements); `validate_from` returns the alias (the non-custom-path `from` is built by it).

- [ ] **Step 1: add `parse_statement` to dialect.rs**

```rust
    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        // Probe: the statement contains `S3Object[` or `S3Object.` and
        // starts with SELECT.
        let first = parser.peek_token();
        let is_select = matches!(
            &first.token,
            Token::Word(w) if w.quote_style.is_none() && w.keyword == Keyword::SELECT
        );
        if !is_select {
            return None; // WITH/CREATE/INSERT/UNION-led → stock
        }
        // Token scan up to the first `;`/EOF (peek_nth_token skips
        // whitespace — verified parser/mod.rs:4451).
        let mut idx = 0usize;
        loop {
            let t = parser.peek_nth_token(idx);
            match &t.token {
                Token::EOF | Token::SemiColon => return None,
                Token::Word(w)
                    if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("S3Object") =>
                {
                    let next = parser.peek_nth_token(idx + 1);
                    if matches!(next.token, Token::LBracket | Token::Period) {
                        return Some(self.parse_custom_select(parser));
                    }
                }
                _ => {}
            }
            idx += 1;
        }
    }
```

> On no S3Object hit it `return None`, having consumed **no token at all**, and stock re-parses from the same position — correct (the dialect hook is called before the leading token is consumed, parser/mod.rs:589-596).

- [ ] **Step 2: custom SELECT skeleton**

```rust
impl S3SelectDialect {
    fn parse_custom_select(&self, parser: &mut Parser) -> Result<Statement, ParserError> {
        parser.next_token(); // SELECT (the probe guarantees it)
        let distinct = if parser.parse_keyword(Keyword::DISTINCT) {
            Some(Distinct::Distinct)
        } else {
            None
        };
        let projection = parser.parse_projection()?;
        if !parser.parse_keyword(Keyword::FROM) {
            return Err(parser_error("invalid FROM: expected S3Object"));
        }
        let (segs, alias) = self.parse_custom_factor(parser)?;
        let selection = if parser.parse_keyword(Keyword::WHERE) {
            Some(parser.parse_expr()?)
        } else {
            None
        };
        let group_by = parser.parse_optional_group_by()?;
        let having = if parser.parse_keyword(Keyword::HAVING) {
            Some(parser.parse_expr()?)
        } else {
            None
        };
        let order_by = parser.parse_optional_order_by()?;
        let limit_clause = self.parse_optional_limit_clause(parser)?;

        let select = Box::new(Select {
            select_token: AttachedToken::empty(),
            optimizer_hints: Vec::new(),
            distinct,
            select_modifiers: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                        "S3Object",
                    ))]),
                    alias: alias.clone(),
                    args: None,
                    with_hints: Vec::new(),
                    version: None,
                    with_ordinality: false,
                    partitions: Vec::new(),
                    json_path: None,
                    sample: None,
                    index_hints: Vec::new(),
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
            connect_by: Vec::new(),
            group_by: group_by
                .unwrap_or(GroupByExpr::Expressions(Vec::new(), Vec::new())),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        });
        let body = match parser.parse_set_operator(&parser.peek_token().token) {
            Some(op) => {
                // Precedence numbers mirror stock's Precedence values for the
                // set operators; they only need to let the right operand
                // parse — the validator rejects every SetOperation anyway.
                let precedence = if op == SetOperator::Intersect { 20 } else { 10 };
                parser.next_token(); // the set-op word
                let set_quantifier = parser.parse_set_quantifier(&Some(op));
                let right = parser.parse_query_body(precedence)?;
                SetExpr::SetOperation {
                    left: Box::new(SetExpr::Select(select)),
                    op,
                    set_quantifier,
                    right,
                }
            }
            None => SetExpr::Select(select),
        };
        *self.captured.borrow_mut() = Some(FromClause {
            segments: segs,
            alias: alias.map(|a| a.name.value),
        });
        Ok(Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(body),
            order_by,
            limit_clause,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: Vec::new(),
        })))
    }
}
```

The `parser_error(s)` helper: `ParserError::ParserError(s.to_string())` (confirm the error constructor's visibility in 0.62 at implementation — if `ParserError::ParserError`'s field is private, use `parser.expected_ref(...)` or store the text and `map_err`).

- [ ] **Step 3: factor parsing**

```rust
    /// `S3Object` + path segments (pest prefix parse) → (segments, alias).
    fn parse_custom_factor(
        &self,
        parser: &mut Parser,
    ) -> Result<(Vec<PathSeg>, Option<TableAlias>), ParserError> {
        let tok = parser.next_token(); // S3Object word
        let Token::Word(w) = &tok.token else {
            return Err(parser_error("invalid FROM: expected S3Object"));
        };
        if w.quote_style.is_some() || !w.value.eq_ignore_ascii_case("S3Object") {
            return Err(parser_error("invalid FROM: expected S3Object"));
        }
        // Slice from the S3Object token's START byte (the pest `path` rule
        // re-matches the `^S3Object` root itself — slicing from the token's
        // end would make every factor fail).
        let pos = positions(&self.sql);
        let start_byte = pos
            .get(&(tok.span.start.line, tok.span.start.column))
            .copied()
            .ok_or_else(|| parser_error("invalid FROM path"))?;
        let slice = &self.sql[start_byte..];
        let (segs, consumed) = crate::path::parse_path(slice)
            .map_err(|e| parser_error(&e))?;
        let path_end = start_byte + consumed;

        // Consume tokens while token start < path_end. Boundary-alignment
        // check (a): the last consumed token's END byte must equal path_end.
        // `last_end` is SEEDED with the S3Object token's own end so the
        // zero-segment case (`FROM S3Object s` via a false-positive trigger,
        // where the walk consumes nothing) passes instead of erroring.
        let mut last_end = (tok.span.end.line, tok.span.end.column);
        loop {
            let t = parser.peek_token();
            if matches!(t.token, Token::EOF) {
                break;
            }
            let start = pos
                .get(&(t.span.start.line, t.span.start.column))
                .copied()
                .ok_or_else(|| parser_error("invalid FROM path"))?;
            if start >= path_end {
                break;
            }
            parser.next_token();
            last_end = (t.span.end.line, t.span.end.column);
        }
        if pos.get(&last_end).copied() != Some(path_end) {
            // (a) pest stopped inside a token: the Word class is wider than
            // pest `name` (non-ASCII, @, #) — reject, never truncate-accept.
            return Err(parser_error("invalid FROM path"));
        }
        // Refused continuation (b): a path directly followed by Period /
        // LBracket is a segment the grammar refused (`S3Object.books`,
        // `[0x]`, `['it''s']`, `S3Object[0]`) — the pest prefix parse never
        // fails on its own (the segment group is optional).
        let next = parser.peek_token();
        if matches!(next.token, Token::Period | Token::LBracket) {
            return Err(parser_error("invalid FROM path"));
        }
        // Alias: hand-rolled (stock parse_optional_alias's after_as arm
        // accepts ANY keyword — parser/mod.rs:12953-12960 — which would
        // loosen the `AS where` rejection; keep the old
        // take_ident+clause_keyword rules).
        let alias = self.parse_custom_alias(parser)?;
        Ok((segs, alias))
    }

    /// `AS? ident`; a clause keyword is never an alias (bare: the clause
    /// starts; after AS: rejected, as the old pipeline did); any other
    /// keyword word is not an alias either (the old pipeline let the stock
    /// tail reject it — the skeleton must reject it itself).
    fn parse_custom_alias(
        &self,
        parser: &mut Parser,
    ) -> Result<Option<TableAlias>, ParserError> {
        let after_as = parser.parse_keyword(Keyword::AS);
        let t = parser.peek_token();
        let is_clause = |w: &sqlparser::tokenizer::Word| {
            CLAUSE_KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(&w.value))
        };
        let ident = match &t.token {
            Token::Word(w) if w.quote_style.is_none() && is_clause(w) => {
                if after_as {
                    // `AS where` — old rejection kept.
                    return Err(parser_error("invalid FROM path"));
                }
                None // WHERE/GROUP/... starts the next clause
            }
            Token::Word(w) if w.quote_style.is_some() || w.keyword == Keyword::NoKeyword => {
                let value = w.value.clone();
                let quote = w.quote_style;
                parser.next_token();
                Some(match quote {
                    Some(q) => Ident::with_quote(q, value),
                    None => Ident::new(value),
                })
            }
            // Any other keyword word (`select`, `and`, ...): not an alias.
            // Bare → leave it for the clause dispatch / trailing-token error;
            // after AS → reject outright. Both were rejections in the old
            // pipeline (via the stock tail).
            Token::Word(_) if after_as => return Err(parser_error("invalid FROM path")),
            _ if after_as => return Err(parser_error("invalid FROM path")),
            _ => None,
        };
        Ok(ident.map(|name| TableAlias {
            explicit: after_as,
            name,
            columns: Vec::new(),
            at: None,
        }))
    }
```

The `CLAUSE_KEYWORDS` constant **stays in sql.rs marked `pub(crate)`**, the dialect referencing `crate::sql::CLAUSE_KEYWORDS`; the `clause_keyword` function likewise (or inline the `is_clause` closure above — pick one, do not keep two copies). Note the old `take_ident` did not accept a single-quoted alias (`'alias'` was rejected by the old pipeline); this skeleton likewise does not (no `SingleQuotedString` arm).

- [ ] **Step 4: LIMIT clause (`parse_optional_limit_clause` is a private API)**

```rust
    fn parse_optional_limit_clause(
        &self,
        parser: &mut Parser,
    ) -> Result<Option<LimitClause>, ParserError> {
        if !parser.parse_keyword(Keyword::LIMIT) {
            return Ok(None);
        }
        let limit = Some(parser.parse_expr()?);
        let offset = if parser.parse_keyword(Keyword::OFFSET) {
            Some(Offset {
                value: parser.parse_expr()?,
                rows: OffsetRows::None,
            })
        } else {
            None
        };
        Ok(Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by: Vec::new(),
        }))
    }
```

(The MySQL comma form `LIMIT 5,2`: GenericDialect's `supports_limit_comma=true`, but this skeleton does not parse the comma — on the custom path the `,` is left over and fails closed as a stock trailing-token error; the stock path keeps `OffsetCommaLimit` → the validator's present-day behavior. The spec's skeleton section records this divergence.)

- [ ] **Step 5: delete preprocess + validate_from adjustment (sql.rs)**

1. Delete `preprocess_from` and every function used only by it: `parse_object_clause`, `find_from_keyword`, `from_clause_end`, `clause_has_join`, `word_at`, `ScanState`, `function_paren`, `is_ident_byte`, `is_ascii_ident_start`, `expect`, `take_ident` (the alias logic is rewritten by the dialect's `parse_custom_alias`).
2. **Keep** `clause_keyword`, `CLAUSE_KEYWORDS` (marked `pub(crate)`, referenced by the dialect).
3. In `parse()`, delete `let (from, rewritten) = preprocess_from(sql)?;`, and the dialect and `parse_sql` take the original `sql`; `from` becomes:
```rust
    let from = dialect.take_from().unwrap_or_else(|| {
        // Non-custom statement: alias from the validated factor, no segments.
        FromClause { segments: Vec::new(), alias: select_alias }
    });
```
`validate_from` changes to return `Option<String>` (the alias):
```rust
fn validate_from(select: &sqlparser::ast::Select) -> Result<Option<String>, Error> {
    let twj = match select.from.as_slice() {
        [t] => t,
        _ => return Err(Error::Parse("FROM must reference exactly one S3Object".into())),
    };
    if !twj.joins.is_empty() {
        return Err(Error::Unsupported("JOIN".into()));
    }
    let TableFactor::Table { name, alias, .. } = &twj.relation else {
        return Err(Error::Parse("FROM must reference exactly one S3Object".into()));
    };
    let [ObjectNamePart::Identifier(name_ident)] = name.0.as_slice() else {
        return Err(Error::Parse("FROM must reference exactly one S3Object".into()));
    };
    if name_ident.quote_style.is_some() {
        // Quoted "S3Object": the old scanner rejected it by accident, the
        // new grammar rejects it explicitly.
        return Err(Error::Parse("invalid FROM: expected S3Object".into()));
    }
    if !name_ident.value.eq_ignore_ascii_case(S3_OBJECT) {
        return Err(Error::Parse("invalid FROM: expected S3Object".into()));
    }
    Ok(alias.as_ref().map(|a| a.name.value.clone()))
}
```
The call site in `parse()`:
```rust
    let select_alias = validate_from(select)?;
```
Note `select` is borrowed through a `Box<Select>` deref — the existing code has `validate_from(select, &from)`; sync the call site with the new signature. `validate_from` runs for custom-path statements too — a custom statement's AST factor is likewise a plain S3Object + alias, so the name check passes; the alias cross-check (clause.alias == AST alias) is deleted (the FromClause is built directly by the skeleton, no longer two independent sources — on the non-custom path that check was always trivially true, see the spec's corresponding paragraph).

- [ ] **Step 6: test adjustments**

1. `reject_bare_path_first_segment` (`S3Object.name`): now takes the custom path → the pest prefix matches `S3Object` only, leaving `.name` behind after consumption → the refused-continuation check → `"invalid FROM path"` — **change the assertion to contains "invalid FROM path"**.
2. `missing_bracket_is_invalid_from_path` / `index_overflow_is_invalid_from_path` / `unterminated_quoted_segment_is_invalid_from_path`: all these paths land in the `"invalid FROM path"` family (index overflow is already tested in path.rs; an unterminated quote dies in tokenization end-to-end, so assert the tokenizer error channel, not pest text) — assertions kept.
3. `bare_column_from_before_from_is_a_parse_error` (`SELECT from FROM S3Object s`): **behavior stance corrected** — verified against the 0.62 source: stock treats `from` as an ordinary identifier (`RESERVED_FOR_IDENTIFIER` holds only EXISTS/INTERVAL/STRUCT/TRIM, keywords.rs:1304-1309; the prefix parser falls back to identifier for other keywords, parser/mod.rs:1793-1818) — under the new grammar this query is **accepted** (projection = a column named `from`). The old rejection was the byte scanner misfiring, not grammar; the honest grammar takes it (the spec's Error surface is synced). Rename the test to `bare_column_from_is_an_identifier`, asserting `ok(...)` with the projection `Identifier("from")`. **Superseded — this query is REJECTED** (corrected after implementation review 2026-09-09 — the spec carries the final wording): the empty-projection arm runs first (`supports_empty_projections() && peek_keyword(FROM)`, parser/mod.rs:14742-14747; GenericDialect delegates true), so `from` is consumed as the FROM keyword and the identifier fallback never runs — `rej(..., "end of statement")`, and the test name stays `bare_column_from_before_from_is_a_parse_error`.
4. The third case in `from_scan_is_comment_aware` (`FROM other -- join\nx`): no S3Object probe → stock parse → `validate_from` → `"invalid FROM: expected S3Object"` — assert that text. The existing `rej(..., "invalid FROM: expected S3Object")` already conforms.
5. `reject_two_from_factors`, `reject_join_*`: no custom path → stock — same as the old behavior (with the old `clause_has_join` path deleted, stock parsing + validate_from's joins check take over).
6. In `is_missing_after_non_ascii_text`'s `'ü'` case, `S3Object[*]` does not trigger — unchanged.
7. `traversal_path` (`SELECT price FROM S3Object[*].books[*].price`): skeleton → segments = [Wild, Name(books), Wild, Name(price)], projection `price` parsed by `parse_projection` — unchanged.
8. `bracket_subscript_access` (`s.projects[0].project_name FROM S3Object s`): no S3Object[ probe → stock — unchanged.
9. `traversal_plus_missing_rewrite`: custom path + WHERE x IS MISSING; the skeleton's `parse_expr` fires the `parse_infix` hook → sentinel name. The match takes `q.missing.is_missing()`.
10. New:
```rust
    #[test]
    fn false_positive_projection_trigger_keeps_stock_behavior() {
        // `s.S3Object.name` in the projection triggers the skeleton; a
        // pathless factor must behave identically to stock.
        let q = ok("SELECT s.S3Object.name FROM S3Object s");
        assert_eq!(q.from.segments, Vec::new());
        assert_eq!(q.from.alias.as_deref(), Some("s"));
    }

    #[test]
    fn multi_statement_captured_slot_not_read() {
        // The second statement would overwrite the slot; the single-statement
        // rejection fires first, the slot is never read.
        rej("SELECT * FROM S3Object[*].a; SELECT * FROM S3Object[*].b", "single statement");
    }

    #[test]
    fn from_path_boundary_alignment_rejects_wider_word_class() {
        // Tokenizer's Word class is wider than pest `name` (non-ASCII, @, #):
        // pest stops mid-token, the walk consumes the whole token, and the
        // end-byte mismatch must reject — never truncate-accept `Name("foo")`.
        rej("SELECT * FROM S3Object[*].foo#bar", "invalid FROM path");
        rej("SELECT * FROM S3Object[*].café", "invalid FROM path");
    }

    #[test]
    fn from_path_refused_continuation_family() {
        rej("SELECT * FROM S3Object.books", "invalid FROM path");
        rej("SELECT * FROM S3Object[0]", "invalid FROM path");
        rej("SELECT * FROM S3Object[*]['it''s']", "invalid FROM path");
        rej("SELECT * FROM S3Object[*].books[0x]", "invalid FROM path");
    }

    #[test]
    fn from_alias_keyword_rules() {
        // Clause keyword after AS is rejected (old behavior kept).
        rej("SELECT * FROM S3Object[*] AS where", "invalid FROM path");
        // A reserved keyword is never an alias (old pipeline rejected via
        // the stock tail; the skeleton rejects it itself).
        rej("SELECT * FROM S3Object[*] AS select", "invalid FROM path");
        // Quoted and bare aliases still work.
        let q = ok("SELECT * FROM S3Object[*] AS \"My Alias\"");
        assert_eq!(q.from.alias.as_deref(), Some("My Alias"));
        let q = ok("SELECT * FROM S3Object[*] s");
        assert_eq!(q.from.alias.as_deref(), Some("s"));
    }
```

- [ ] **Step 7: run**

Run: `cargo test -p tinio-select`
Expected: all green.

- [ ] **Step 8: Commit (on user confirmation)**

```bash
git add crates/tinio-select/src/dialect.rs crates/tinio-select/src/sql.rs crates/tinio-select/src/path.rs
git commit -m "refactor(select): custom FROM factor via parse_statement, drop preprocessor"
```

---

### Task 5: json.rs path-matching alignment tests (Q13)

**Files:**
- Modify: `crates/tinio-select/src/json.rs` (add tests under `#[cfg(test)]`)

**Interfaces:** none added.

- [ ] **Step 1: add tests**

Read the existing style of the `json.rs` test module (e.g. `json_attribute_lookup_case_sensitivity` at line 915), then add:

```rust
    #[test]
    fn traversal_quoted_segment_is_case_insensitive() {
        // Q13 ruling: `['name']` and `.name` are both by-name forms — match
        // case-insensitively + ambiguity error; the exact arm is only for
        // quoted attribute access in SELECT/WHERE. The implementation is
        // unchanged (the feeder always passes NameStyle::Bare, json.rs:215);
        // this pins the behavior.
        // ...build the reader the way the existing tests do (search the test
        // module for Reader::new) or call json_lookup directly...
    }
```

Implementation hint: first see how the existing `json.rs` tests build a reader (search the `#[cfg(test)]` module for `Reader::new`) and reuse the same shape. If `json_lookup` is directly callable (`pub(crate)`), assert once each with `assert_eq!(json_lookup(map, "name", NameStyle::Bare)...)` and the quoted name. **Q13 is explicit that the code does not change** — tests only; if existing behavior contradicts the ruling (the test fails), stop and report rather than changing the code.

- [ ] **Step 2: run**

Run: `cargo test -p tinio-select json::`
Expected: all green.

- [ ] **Step 3: Commit (on user confirmation)**

```bash
git add crates/tinio-select/src/json.rs
git commit -m "test(select): pin path segment case-insensitivity"
```

---

### Task 6: Regression + cleanup + whole-workspace verification

**Files:**
- Modify: `crates/tinio-select/src/sql.rs` (add the drift corpus to the tests)
- Nothing else

**Interfaces:** none.

- [ ] **Step 1: delegation-drift corpus (sql.rs tests)**

These queries take the **stock** path (no `S3Object[`/`S3Object.` trigger): `LIMIT 3,5` is parsed by stock → `OffsetCommaLimit` → the validator reports "LIMIT must be a positive integer"; `LIMIT 5 BY s.a` → `LimitOffset.limit_by` non-empty → the validator reports "unsupported: LIMIT BY"; `GROUP BY 1` → unsupported GROUP BY — **all error channels**, so assert errors, not plans:

```rust
    #[test]
    fn delegation_drift_surface() {
        // No custom-path trigger → stock parsing, which depends on the
        // GenericDialect overrides being delegated. A missed delegation
        // flips accept/reject behavior; assert channels, not message text.
        let _ = parse("SELECT * FROM S3Object s LIMIT 3, 5")
            .expect_err("LIMIT 3,5: OffsetCommaLimit rejects via validator");
        let _ = parse("SELECT * FROM S3Object s LIMIT 5 BY s.a")
            .expect_err("LIMIT BY: validator rejects");
        let _ = parse("SELECT s.a FROM S3Object s GROUP BY 1")
            .expect_err("GROUP BY: validator rejects");
        ok("SELECT * FROM S3Object s WHERE s.x = 1"); // baseline stock path
    }
```

- [ ] **Step 2: leftover scan**

Run: `rg -n "preprocess_from|rewrite_is_missing|reject_reserved_functions|SENTINEL_FUNCTIONS|ScanState|word_at|from_clause_end|clause_has_join|function_paren|OPERAND_BOUNDARY_KEYWORDS|is_ident_byte|is_ascii_ident_start|take_ident|parse_object_clause|find_from_keyword|__s3_is_missing\"|__s3_is_not_missing" crates/tinio-select/src/`
Expected: no matches (the `SentinelNames::is_missing` format string is `"__s3_is_missing_{}"` — the prefix is followed by `{`, so it does not match `__s3_is_missing\"`; comments may explain the prefix, but no hardcoded full name may serve a functional purpose).

- [ ] **Step 3: whole-workspace tests**

Run: `cargo test --workspace --exclude tinio-e2e`
Expected: all green.

- [ ] **Step 4: e2e (run with user assistance)**

Run: `cargo test -p tinio-e2e` (or `--test cucumber -- --tags select`)
Expected: all green (e2e pins only 400 + S3QueryParsingError; the `IS NOT MISSING` scenario select.feature:257-264 is a result-level assertion, unaffected by taking the `UnaryOp{Not}` shape).

- [ ] **Step 5: Commit (on user confirmation)**

```bash
git add crates/tinio-select/src/sql.rs
git commit -m "test(select): delegation drift corpus"
```

---

## Self-Review (complete; the 2026-09-08 review revisions are merged in)

**Spec coverage map:**
1. SentinelNames + QueryPlan field → Task 1
2. pest grammar + path.rs (including the walker's recursive merge) → Task 2
3. parse_infix hook + uuid sentinel + UnaryOp{Not} + chained rejection + engine matching + delete reserved/rewrite → Task 3
4. parse_statement + skeleton + factor (start-offset slice + alignment/continuation checks) + delete preprocess + validate_from quoted rejection + from dual path → Task 4
5. json.rs Q13 tests → Task 5
6. drift corpus + e2e → Task 6
7. Known boundaries (set-op right operand, DISTINCT ON, `FROM (` returns invalid FROM, LIMIT comma fail-closed) → Task 4 implementation + Task 6 verification
8. Error surface (`SELECT from` **rejected** — `end of statement`, corrected after implementation review 2026-09-09 — the spec carries the final wording; `FROM other` new message, fixed-name sentinel in-stream) → Task 3/4 test assertions
9. Risks (delegation, pest @/$, unsafe, uuid entropy, QueryPlan equality nondeterminism) → covered by Task 3/6

**Verified API points (against the 0.62.0 source, 2026-09-08):**
- `peek_nth_token` skips whitespace ✓ (parser/mod.rs:4451); `next_token`/`advance_token` skip whitespace ✓ (4506/4528) — parse_infix needs no manual whitespace-skip loop.
- `parse_set_operator`/`parse_set_quantifier` are both `pub` and their signatures match the skeleton's usage ✓ (14637/14648).
- The `Select` (24 fields, `optimizer_hints` included) / `Query` (10 fields) / `Offset{ value: Expr, rows }` field lists checked one by one ✓; when it lands in code, fill it in per compile errors.
- `Dialect: Debug + Any` (dialect/mod.rs:204) → the struct must `#[derive(Debug)]` and own a `String` (`Any`'s `'static` bound rules out borrowing).
- The `NOT` probe compares by `w.keyword == Keyword::NOT`; `Token::make_keyword` equality would miss a lowercase `not` because `Word::value` preserves the user's casing (the old byte scan was case-insensitive) — fixed.
- `SELECT from FROM S3Object s` is **REJECTED** under stock with `"end of statement"` — the empty-projection arm (`supports_empty_projections() && peek_keyword(FROM)`, parser/mod.rs:14742-14747) consumes `from` as the FROM keyword, so the identifier fallback never runs (corrected after implementation review 2026-09-09 — the spec carries the final wording; the earlier "accepted, `from` falls back to an identifier" claim here and in Task 4 Step 6.3 is superseded).
- `ParserError::ParserError(String)` constructor visibility, and the blast radius of the `validate_from` signature change (one call site) — confirm at implementation; the fallback is given.

**Plan-quality note**: the intermediate states of Task 3 and Task 4 are explicitly argued testable in the task titles and Step 2's accounting (Task 3 feeds the preprocess-rewritten text, so the IS MISSING hook still fires; Task 4 switches back to the original text and deletes preprocess).
