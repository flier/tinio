# S3 Select (tinio-select) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `SelectObjectContent` (S3 Select) over tinio objects — a streaming SQL filter pipeline in a new `tinio-select` crate, wired into `tinio-server` behind the `select` feature (default on) with `select-parquet` (default off).

**Architecture:** Pure byte-in/event-out pipeline (`sqlparse+validate → record readers → evaluator → serializers → event chunker`) in `tinio-select`, which depends on no tinio crate. `tinio-server` adds `op_select_object_content` (validation → `Storage` read → engine → s3s event mapping). Backends untouched.

**Tech Stack:** `sqlparser` (parse), `csv`, `serde_json` (`arbitrary_precision`), `rust_decimal` (numeric spine, 28-digit, overflow → error), `flate2` (gzip), `bzip2` 0.6 (pure-Rust), optional `parquet` (arrow-rs), `futures`.

**Spec:** `docs/superpowers/specs/2026-09-04-select-object-content-design.md` — read it first; every task below argues from it.

## Global Constraints

- `unsafe_code = "forbid"` per crate; English-only comments/messages; follow `docs/style.md` and `docs/tests.md`.
- `tinio-select` must not depend on `tinio-core`, `s3s`, or any backend crate; the event model is `SelectEvent` (s3s-free), mapped by the server. **The crate is synchronous** (`std::io::Read` in, `Iterator` out; no tokio/bytes/futures deps) — the async→sync bridge lives in tinio-server (review 2026-09-05, spec §1.1): bounded `tokio::sync::mpsc` (capacity 4) + `ChannelReader` (std `Read` over `blocking_recv`) + `spawn_blocking` engine + output mapped through a hand-rolled `futures::Stream` over `Receiver::poll_recv` (no `tokio-stream` dep — review 2026-09-05b); CPU-bound work never runs on a tokio worker; cancellation = channel drop.
- Numeric spine = `rust_decimal`; precision > 28 significant digits → error, **only when a numeric operator consumes the value** (grilling Q3): a `SELECT *` passes the raw token text through untouched. `Value::RawNumber(String)` carries JSON number tokens verbatim; CSV fields stay `Value::String`.
- **`SELECT *` matrix** (review 2026-09-05): CSV→CSV values untouched (framed by output config); CSV→JSON keys = USE headers else `_1.._N`; JSON→CSV columns = top-level keys in encounter order (`serde_json` `preserve_order`), nested → compact JSON cell; JSON→JSON re-emits the record in encounter order with `RawNumber` tokens verbatim.
- Request-level errors map: 400 `S3QueryParsingError` (via `Custom`), `InvalidRequestParameter` for config combos incl. Parquet size bound. **Header-dependent checks (`AmbiguousFieldName`/`MissingHeaderName`) are in-stream** (they need the CSV header row — cannot be 400 before any read; review 2026-09-05). Other in-stream errors: one code `Custom("S3QueryError")` with a detail message (grilling Q2). `Error` gains `Ambiguous(String)`/`MissingHeader(String)` variants for the mapping.
- **Parquet memory bound** (review 2026-09-05): the parquet reader buffers the whole object (no seek in the storage path); `SelectConfig.max_parquet_bytes` (default 256 MiB); server rejects over-bound objects **request-level 400** (the size is known from the read path's own `GetObjectResult.info.size` before any body byte streams — no `head_object` round trip; review 2026-09-05b). CSV/JSON/GZIP/BZIP2 fully streaming.
- **Alias rule scoped to JSON output** (review 2026-09-05): a bare-expression projection needs an alias only when `OutputSerialization = JSON`; CSV output allows it. `sql.rs` accepts; the server op enforces per output mode.
- **Input-record cap 1 MB** (review 2026-09-05 #3): every reader (CSV row, JSON line) tracks bytes consumed per record; exceeding → in-stream `Format("input record exceeds 1 MB")` — bounds memory and defuses decompression bombs. Output side keeps `TooLarge`.
- **Expression length ≤ 256 KiB** (review 2026-09-05 #5, AWS's documented expression limit) → request-level `S3QueryParsingError`.
- **ScanRange validation set** (review 2026-09-05 #6): non-negative `start`/`end`; not both absent; `start ≤ end`; `end < size` with **checked arithmetic** (no underflow/panic); Parquet ⇒ ScanRange rejected (`InvalidRequestParameter`); `start`-only and `end`-only forms as documented.
- **`AllowQuotedRecordDelimiter` known deviation** (review 2026-09-05 #7): the `csv` crate cannot distinguish a record delimiter inside quotes — both `true` and `false` behave as `true` (permissive); documented in the spec, never silently ignored.
- **Records are never split across events** (review 2026-09-05 #10): the adapter flushes at record boundaries; a record that would exceed 1 MB → `TooLarge`. No split test.
- **Request-level `Custom` errors must set HTTP 400 explicitly** (review 2026-09-05 #2; 2026-09-05b: `S3Error::new` takes only the code — the code+message constructor is `S3Error::with_message(code, msg)`): a `Custom`-coded error built as `S3Error::with_message(S3ErrorCode::Custom("...".into()), msg)` alone serializes as **500** (s3s `Custom.status_code() = None` → `unwrap_or(500)`); call `.set_status_code(StatusCode::BAD_REQUEST)`. `Custom`'s payload is a `bytestring::ByteString`, so pass `"...".into()` — a `b".."` byte literal does not coerce (review 2026-09-05b). Use the real `S3ErrorCode::AmbiguousFieldName` variant (exists, 400) instead of a Custom code for ambiguity (#11); only `S3QueryParsingError`/`MissingHeaderName`/`S3QueryError` use `Custom`.
- **Concurrency cap** (review 2026-09-05 #4): the op guards with a `tokio::sync::Semaphore` (default 4, documented constant; `acquire_owned` and drop-releases).
- **sqlparser 0.62 AST vocabulary** (review 2026-09-05 #9 — the plan's original names don't exist; pinned 0.62 + arrow/parquet 59 as of 2026-09-06, upgraded from 0.57/56): aggregates are `Expr::Function` (name in `count`/`sum`/`avg`/`min`/`max`; `count(*)` = `FunctionArg::Wildcard`), **not** `Expr::AggregateExpr`; `CompoundFieldAccess` is the struct variant `{ root: Box<Expr>, access_chain: Vec<AccessExpr> }` — chain elements are `AccessExpr::Subscript` (bracket `[i]`) or `AccessExpr::Ident` (dot access); a plain dotted name (`a.b.c`) stays `CompoundIdentifier` — none of these are `Expr::Subscript` (review 2026-09-05b: corrected tuple→struct shape); LIKE is `Expr::Like`/`ILike` (`escape_char` is a `ValueWithSpan` in 0.62 — the engine unwraps the single-quoted literal); `SelectItem::ExprWithAliases` (Spark `AS (a,b)`) and `ObjectNamePart::Function` are 0.62 additions, both refused request-level (review 2026-09-06b upgrade adaptation).
- Feature wiring: `select = ["dep:tinio-select"]`; `select-parquet = ["select", "tinio-select/parquet"]`; `default += "select"`. Runtime `Capabilities.select` (in `tinio-config/src/schema/s3.rs`, default true). The s3s output stream is a hand-rolled `futures::Stream` over `tokio::sync::mpsc::Receiver::poll_recv` — tinio-server keeps no `tokio-stream` dependency (review 2026-09-05b, spec §1.1).
- Non-goals rejected at parse time: JOIN, subquery, GROUP BY/HAVING, ORDER BY, DISTINCT, UNION.
- ScanRange only with `CompressionType::NONE`; Parquet only with `CompressionType::NONE`; output never Parquet; delimiters single-byte; **CSV output `QuoteFields` default = `ASNEEDED`** (grilling Q4); JSON output `RecordDelimiter` default `\n`; ScanRange+JSON DOCUMENT → start>0 yields zero records (documented, not a bug).
- Timing is pull-driven (known deviation, review 2026-09-05): Progress/Cont cadence checked at emit points; slow clients see less prompt events. Tests assert cadence only at event granularity, never wall-clock.
- **e2e traceability** (review 2026-09-05): `crates/tinio-e2e/tests/traceability.rs` enforces feature tags ↔ spec IDs — register **FR-034** (the assigned requirement ID; the concurrent bucket-CORS work took FR-033) in `specs/001-s3-local-server` contracts/checklists *before* `select.feature` lands, or the suite fails.
- **AWS defaults for unset DTO fields** (review 2026-09-05, previously unstated): FieldDelimiter `,`; RecordDelimiter `\n`; QuoteCharacter/QuoteEscapeCharacter `"`; Comments `#`; FileHeaderInfo `NONE`; JSONType `LINES` (the AWS default). The server's dto→`SelectConfig` builder applies them; a unit test pins each.
- Commit per task (grilling Q5): convention style messages; per repo CLAUDE.md, git writes need an explicit user approval — each task's commit step asks the user first.
- s3s DTO facts (verified against s3s 0.15.0 source): `InputSerialization { csv, compression_type, json, parquet }`; `CSVInput { allow_quoted_record_delimiter: Option<AllowQuotedRecordDelimiter> /* bool */, comments: Option<String>, field_delimiter: Option<String>, file_header_info: Option<FileHeaderInfo /* String */>, quote_character: Option<String>, quote_escape_character: Option<String>, record_delimiter: Option<String> }`; `JSONInput { type_: Option<JSONType /* String */> }`; `OutputSerialization { csv: Option<CSVOutput>, json: Option<JSONOutput> }`; `ScanRange { start: Option<Start /* i64 */>, end: Option<End /* i64 */> }`; `RequestProgress { enabled: Option<EnableRequestProgress /* bool */> }`; `CompressionType` consts `NONE`/`GZIP`/`BZIP2`; `JSONType` consts `DOCUMENT`/`LINES`; `FileHeaderInfo` consts `NONE`/`IGNORE`/`USE`. Use the `From<&'static str>`/const access style shown in `crates/tinio-server/src/backend/*.rs`.
- `S3Backend` fields (backend/mod.rs:188): `storage: Arc<S>`, `caps: Capabilities`; read path: `self.storage.get_object(&bucket, &key, range)`; runtime toggle idiom: `Self::require_cap(self.caps.list_objects_v1, "ListObjects")?` (listing.rs:110).
- Executor facts to confirm at implementation (not design): exact `testutil.rs` fixture helper names (`setup_name`-style), e2e step wiring location, `parquet` crate's current 59.x line, `dto::CSVInput`/`CSVOutput` `Default` impls.

---

## File Structure

```
crates/tinio-select/
  Cargo.toml            new — manifest; feature parquet (off); criterion dev-dep
  src/lib.rs            new — root, re-exports
  src/error.rs          new — Error (Parse/Unsupported/Value/Ambiguous/MissingHeader/Format/Io/TooLarge/NestedCsv/ParquetTooLarge)
  src/row.rs            new — Value, Field (MISSING), Record
  src/sql.rs            new — FROM preprocessor + parse/validate → QueryPlan
  src/record.rs         new — RecordReader trait + CsvReader + compression/scan-range wrappers
  src/json.rs           new — JsonReader (LINES/DOCUMENT + traversal), JSON lookup
  src/parquet.rs        new — ParquetReader (#[cfg(feature = "parquet")])
  src/engine.rs         new — evaluator: WHERE/projection/LIMIT/aggregates
  src/output.rs         new — CSV + JSON serializers
  src/events.rs         new — SelectEvent + select_iter assembly (1MB chunks, cont/progress/stats/end)
  benches/select_scan.rs new — criterion bench (100k-row CSV, full vs filtered)
crates/tinio-server/
  src/backend/select.rs new — op_select_object_content + validation + s3s event mapping
  src/backend/s3.rs     modify — select_object_content impl (cfg feature)
  src/backend/mod.rs    modify — mod select
  Cargo.toml            modify — features + dep
crates/tinio-config/src/schema/s3.rs  modify — Capabilities.select
crates/tinio-e2e/tests/features/select.feature  new
Cargo.toml (workspace)  modify — add arrow, bytes, bzip2, csv, flate2, parquet, rust_decimal, sqlparser workspace deps (arrow pinned once here per docs/cargo.md "pin once"; parquet gated via the crate's own optional dep — RFC 2906)
```

---

### Task 1: Workspace + `tinio-select` scaffold

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Create: `crates/tinio-select/Cargo.toml`, `crates/tinio-select/src/lib.rs`, `crates/tinio-select/src/error.rs`

**Interfaces:**
- Produces: `tinio_select::Error`; crate name `tinio_select`, library only.

- [x] **Step 1: Add workspace deps** (alphabetical, like the existing list):

```toml
arrow = { version = "56", default-features = false }
bytes = "1"
bzip2 = "0.6"
csv = "1.3"
flate2 = "1.0"
parquet = "56"
rust_decimal = "1"
sqlparser = "0.62"
```

(parquet: `optional` is forbidden in [workspace.dependencies] (RFC 2906) — the crate marks it optional, review 2026-09-05b; `arrow` is pinned here with `default-features = false` per docs/cargo.md "pin once", the crate re-declares it as `optional`.)

- [x] **Step 2: Create the crate manifest** `crates/tinio-select/Cargo.toml`:

```toml
[package]
name = "tinio-select"
version.workspace = true
edition.workspace = true
description = "Streaming SQL SELECT engine for S3 objects (S3 Select)"
publish = false

[dependencies]
arrow = { workspace = true, optional = true }
bzip2.workspace = true
bytes = { workspace = true, optional = true }
csv.workspace = true
flate2.workspace = true
parquet = { workspace = true, optional = true }
rust_decimal.workspace = true
serde_json = { workspace = true, features = ["arbitrary_precision", "preserve_order"] }
sqlparser.workspace = true
thiserror.workspace = true
time.workspace = true

[dev-dependencies]
criterion.workspace = true

[[bench]]
name = "select_scan"
harness = false

[features]
default = []
parquet = ["dep:parquet", "dep:arrow", "dep:bytes"]

[lints.rust]
unsafe_code = "forbid"
```

(`arrow`/`bytes` are optional, parquet-only — see Task 11's ChunkReader rationale; review 2026-09-05. No async deps — the crate is synchronous per the execution model; fixtures are in-memory `Cursor`s, no `tempfile`. No `Cargo.toml` comments per docs/cargo.md.)

- [x] **Step 3: Create `src/error.rs`**:

```rust
use thiserror::Error;

/// Errors raised by the select pipeline. `Parse`/`Unsupported` are
/// request-level (the server maps them to 400 before streaming);
/// the rest surface as error items inside the 200 event stream
/// (spec §3) under `Custom("S3QueryError")` (grilling Q2).
#[derive(Debug, Error)]
pub enum Error {
    #[error("S3 select: {0}")]
    Parse(String),
    #[error("S3 select: unsupported: {0}")]
    Unsupported(String),
    #[error("S3 select: value error: {0}")]
    Value(String),
    #[error("S3 select: ambiguous field: {0}")]
    Ambiguous(String),      // -> S3ErrorCode::AmbiguousFieldName (real s3s variant) in-stream
    #[error("S3 select: missing header: {0}")]
    MissingHeader(String),  // -> Custom("MissingHeaderName") in-stream
    #[error("S3 select: input error: {0}")]
    Format(String),
    #[error("S3 select: io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("S3 select: output record exceeds 1 MB limit")]
    TooLarge,
    #[error("S3 select: nested column not supported for CSV output")]
    NestedCsv,
    #[error("S3 select: parquet object exceeds the memory bound")]
    ParquetTooLarge,
}
```

- [x] **Step 4: `src/lib.rs`** — `mod error;` (private per `docs/style.md`) + root re-export `pub use self::error::Error;` + module doc comment pointing at the spec.

- [x] **Step 5: Verify** — `cargo check -p tinio-select` succeeds; `cargo test -p tinio-select` green (empty suite).

- [x] **Step 6: Commit** — ask the user, then `git add Cargo.toml Cargo.lock crates/tinio-select && git commit -m "feat(select): scaffold tinio-select crate"`

---

### Task 2: `row.rs` — Value / MISSING / Record

**Files:**
- Create: `crates/tinio-select/src/row.rs` (+ `#[cfg(test)]` module)

**Interfaces:**
- Produces:
  - `pub enum Value { Null, Bool(bool), Int(i64), Decimal(rust_decimal::Decimal), RawNumber(String), String(String), Json(Box<serde_json::Value>) }` — `RawNumber` carries a JSON number token verbatim (grilling Q3); `Json` carries nested parquet/JSON structures (populated from Task 10/11, rendered by the JSON serializer; under CSV output the two inputs differ — JSON nested renders as a compact cell, parquet nested is `NestedCsv`; decision 2026-09-05 review, grilling Q10).
  - `pub enum Field { Present(Value), Missing }`
  - `pub enum Record { Csv(Vec<Field>, Vec<String>), Json(Option<serde_json::Value>), Parquet(Vec<Field>, Vec<String>) }` — `Vec<String>` is the name map (`_N`/headers for CSV; column names for Parquet); `Json(None)` is a path-traversal MISSING row (a wildcard/path step that matched nothing), distinct from a present `null` value.
  - `pub fn parse_number(s: &str) -> Result<Decimal, Error>` — lazy parse; digit count > 28 → `Value("numeric value exceeds 28-digit precision: {s}")`, else any parse error → `Value("invalid numeric value: {s}")`.
  - `pub fn display(v: &Value) -> String` — serializer text form: `Normalize` the Decimal, raw `RawNumber` text back out, `Json` → `serde_json::to_string`.

Tests:
- `parse_number`: `"1.50"`, `"-12"`, `"3e2"` → Ok; `"abc"`, `"NaN"` → Err; 29-digit `"99999999999999999999999999999"` → Err with the precision message.
- `display`: `Decimal::new(150, 2)` → `"1.5"`; `RawNumber("1e309")` → `"1e309"`; `Json(serde_json::json!({"a":1}))` → `{"a":1}`.
- `Record::Csv` construction smoke test.

- [x] **Step 1: Write failing tests** (the cases above).
- [x] **Step 2: Run** — `cargo test -p tinio-select row` → FAIL (module missing).
- [x] **Step 3: Implement** `row.rs` per interfaces; `parse_number` counts digits (`s.chars().filter(char::is_ascii_digit).count() > 28`) before parsing.
- [x] **Step 4: Run tests** → PASS.
- [x] **Step 5: Commit** (ask user) — `feat(select): value model with lazy decimal parsing`

---

### Task 3: `sql.rs` — FROM preprocessor, parse + validate, `QueryPlan`

**Files:**
- Create: `crates/tinio-select/src/sql.rs` + tests

**Interfaces:**
- Produces:
  - `pub enum PathSeg { Name(String), Index(usize), Wild }`
  - `pub struct FromClause { pub segments: Vec<PathSeg>, pub alias: Option<String> }` — `segments` is the whole traversal state (an empty list = non-traversed; the derived `traversed` flag was removed in the 2026-09-06 cleanup).
  - `pub enum Projection { Wild, Item { expr: sqlparser::ast::Expr, alias: Option<String> } }`
  - `pub struct QueryPlan { pub from: FromClause, pub projections: Vec<Projection>, pub where_expr: Option<Expr>, pub limit: Option<usize>, pub aggregates: bool }`
  - `pub fn parse(sql: &str) -> Result<QueryPlan, Error>` — also refuses, request-level: expression-position subqueries (`IN (SELECT …)`, `EXISTS`, bare `(SELECT …)` — the top-level `SetExpr::Query` arm catches only the statement shape; review 2026-09-06b R4), and user-authored `__s3_is_missing`/`__s3_is_not_missing` calls (R15 — the names are the rewrite's MISSING sentinels; rejected on the pre-rewrite text, quote/comment-aware).

Preprocessor algorithm (spec §1 sql.rs):
1. `preprocess_from(sql)`: find `FROM` with a small byte scanner that toggles on `'` (string) and `"` (quoted identifier); read the object clause: `S3Object` followed by zero+ segments ((`.` name | `[` int `]` | `.` `*` | `[` `*` `]` | `[` `'` name `'` `]`)) and an optional `AS? alias`; record `(segments, alias)`; rewrite the clause's object text to `S3Object` (strip segments) for sqlparser.
2. **`IS [NOT] MISSING` rewrite** (review 2026-09-05b): sqlparser 0.62's `Expr` has no `IsMissing` variant, so `GenericDialect` cannot parse `IS [NOT] MISSING` at all (only IsNull/IsNotNull/IsTrue/IsFalse/IsDistinctFrom exist) — the same quote-aware pass rewrites each `X IS [NOT] MISSING` into the sentinel function calls `__s3_is_missing(X)` / `__s3_is_not_missing(X)`, which `eval` maps back to MISSING semantics (Task 5). A plain `IS [NOT] NULL` substitution would be wrong: a present-but-null field must not satisfy `IS MISSING`.
3. Parse with `sqlparser::Parser::parse_sql(&GenericDialect, &rewritten)`.
4. Validate AST: one `Statement::Query`; `set_expr` is `SetExpr::Select` (reject UNION/subquery); reject `GROUP BY`/`HAVING`/`ORDER BY`/`DISTINCT`; `LIMIT` must be a positive integer literal; any `join` → `Unsupported("JOIN")`; `FROM` = the single `S3Object` factor with the recorded alias.
5. SELECT list → projections: bare `*` → `Wild`; `SelectItem::UnnamedExpr(e)` → `Item { expr: e, alias: None }`; `SelectItem::ExprWithAlias` → alias. **No alias rejection here** (review 2026-09-05b): a bare-expression projection without an alias is *accepted* — `Projection::Item.alias` records whatever was written (or `None`); the alias rule (JSON output only) is enforced by the server op (Global Constraints; Task 13).
6. `aggregates` = any `Expr::Function` in the list whose name is `count`/`sum`/`avg`/`min`/`max` (**sqlparser 0.62 AST — there is no `Expr::AggregateExpr`; `count(*)` is a `Function` whose args contain `FunctionArg::Wildcard`**; review 2026-09-05 #9). Also reject an expression string longer than 256 KiB (`Parse("expression exceeds 256 KiB")`, review 2026-09-05 #5). **If `aggregates`, `Wild` → `Parse("aggregates require an explicit select list")`.**

Tests:
- Accept: `SELECT * FROM S3Object s WHERE s._3 > 100 LIMIT 5`; `SELECT s.Id, s.Name AS n FROM S3Object s`; `SELECT price FROM S3Object[*].books[*].price`; `SELECT s.projects[0].project_name FROM S3Object s`; `SELECT count(*) FROM S3Object s`; `SELECT s.x+1 FROM S3Object s` (bare expression, no alias — parse accepts; the alias rule is the server's, JSON output only, Task 13; review 2026-09-05b).
- Reject: `SELECT * FROM a JOIN b`; `SELECT * FROM S3Object GROUP BY 1`; `SELECT * FROM S3Object ORDER BY 1`; `SELECT DISTINCT x FROM S3Object`; `(SELECT 1) UNION (SELECT 2)`; `SELECT * FROM S3Object.name` (bare name without `[*]`); `SELECT count(*) FROM S3Object` … `SELECT * FROM S3Object GROUP BY…` covered; `SELECT * FROM S3Object s` + `COUNT(*)` in same list → rejected (`aggregates + Wild`).

- [x] **Step 1: Write failing tests** (the corpus above).
- [x] **Step 2: Run** → FAIL (sql.rs missing).
- [x] **Step 3: Implement** `sql.rs`:

```rust
fn preprocess_from(sql: &str) -> Result<(FromClause, String), Error> // (clause, rewritten)
fn rewrite_is_missing(sql: &str) -> String                                // quote-aware IS [NOT] MISSING -> sentinel form (review 2026-09-05b)
fn parse_object_clause(s: &str) -> Result<FromClause, Error>
```

`parse_object_clause` consumes `S3Object`, then segments:
- `.name` → `PathSeg::Name`
- `[*]` or `.*` → `PathSeg::Wild`
- `[N]` → `PathSeg::Index(N)`
- `['name']` → `PathSeg::Name`

A segment without a leading `[*]` on the first step (`S3Object.name`) → `Parse("invalid FROM path: must start with S3Object[*]")`. `segments.is_empty() ⇒ traversed=false`.

- [x] **Step 4: Run tests** → PASS.
- [x] **Step 5: Commit** (ask user) — `feat(select): sql parse + validate to query plan`

---

### Task 4: `record.rs` — RecordReader + CsvReader

**Files:**
- Create: `crates/tinio-select/src/record.rs` + tests

**Interfaces:**
- Produces:
  - `pub trait RecordReader { fn next(&mut self) -> Result<Option<Record>, Error>; }`
  - `pub struct CsvParams { pub field_delimiter: u8, pub record_delimiter: u8, pub quote: u8, pub escape: u8, pub comments: Option<u8>, pub header: CsvHeader, pub allow_quoted_record_delimiter: bool }`
  - `pub enum CsvHeader { Use, Ignore, None_ }`
  - `pub struct CsvReader<R: Read>` with `new(reader: R, params: CsvParams) -> Self` — the reader runs over a byte-counting `CappedRead` wrapper (review 2026-09-06b R5): the own 1 MB span check is post-hoc (the `csv` crate buffers a whole record first), so the wrapper bounds the in-flight memory — a gzip bomb expanding into one giant field errors mid-read at `MAX_RECORD` + a read-ahead allowance (the csv internal buffer prefetches across record boundaries; the span check stays the exact per-record authority).
- CSV → `Record::Csv(fields, names)`: `Use` → first line = header names (subsequent rows use them); `Ignore` → first line skipped, names `_1.._n`; `None_` → no skip, names `_1.._n` (per row width, ragged rows allowed via `flexible(true)`).

Builder:

```rust
let mut b = csv::ReaderBuilder::new();
b.delimiter(params.field_delimiter)
 .terminator(csv::Terminator::Any(params.record_delimiter))
 .quote(params.quote)
 .escape(params.escape)
 .has_headers(false) // header handling is ours
 .flexible(true);
if let Some(c) = params.comments { b.comment(Some(c)); }
```

**Input record cap** (review 2026-09-05 #3): the reader tracks bytes consumed per record and errors `Format("input record exceeds 1 MB")` past 1 MB — bounds memory and defuses decompression bombs.

`allow_quoted_record_delimiter`: the csv crate cannot distinguish a record delimiter inside quotes — both `true` and `false` behave as `true` (permissive). **Documented known deviation** (review 2026-09-05 #7): AWS's `false` strictness is not enforced; never silently ignored.

Tests:
- USE: `id,name\n1,alice\n2,bob` → record 1 names `["id","name"]`, values `["1","alice"]`.
- IGNORE: first line dropped, names `["_1","_2"]`.
- NONE: no skip, names `["_1","_2"]`.
- comments `#...` skipped; custom `|` delimiter; quoted field with embedded record delimiter (`"a\nb",c` → one record, two fields); ragged tail `a,b\n1` → row `["1"]`, names `["_1"]`.
- input cap: a 1 MB + 1-byte row → `Format("input record exceeds 1 MB")`.

- [x] **Step 1: Write failing tests**.
- [x] **Step 2: Run** → FAIL.
- [x] **Step 3: Implement** `record.rs`.
- [x] **Step 4: Run** → PASS.
- [x] **Step 5: Commit** (ask user) — `feat(select): csv record reader`

---

### Task 5: `engine.rs` — evaluator, WHERE / projections / LIMIT

**Files:**
- Create: `crates/tinio-select/src/engine.rs` + tests

**Interfaces:**
- Produces:
  - `pub struct Engine { plan: QueryPlan }` — `pub fn new(plan: QueryPlan) -> Self`, `pub fn next(&mut self, rec: Record) -> Result<Option<OutRow>, Error>` (None = row filtered), `pub fn finish(&mut self) -> Result<Option<OutRow>, Error>` (aggregate row, live in Task 7 — this task implements the non-aggregate paths and leaves `finish` returning Ok(None)).
  - `pub struct OutRow { pub keys: Vec<String>, pub vals: Vec<Field> }` — keys: alias > plain field name > `_1..` (Wild).
  - `fn column_value(rec: &Record, name: &str, alias: &Option<String>, quoted: bool) -> Result<Option<Field>, Error>` — CSV: `_N` → index N−1 (out-of-range → `Some(Field::Missing)`); header map (lowercased unless `quoted`); case-insensitive duplicate → `Ambiguous(name)`; USE-mode named ref that doesn't match any header → `MissingHeader(name)`; NONE/IGNORE named refs fall through to positional `_N` only. JSON/Parquet arms: `Ok(None)` → treated as Missing until Tasks 9/11 replace them (each task replaces only its arm — they never share code paths).
  - `fn eval(expr: &Expr, ctx: &RowCtx) -> Result<Value, Error>` — literals; Identifier/CompoundIdentifier/`CompoundFieldAccess` via `column_value`; BinaryOp (`Eq`, `NotEq`, `Lt`, `LtEq`, `Gt`, `GtEq`, `And`, `Or`, `Plus`, `Minus`, `Multiply`, `Divide`, `Modulo`); `InList`/`Between` honoring their `negated` flag (`NOT IN` / `NOT BETWEEN` — review 2026-09-05b); `UnaryOp::Not`; `UnaryOp::Minus`/`Plus` over numeric literals (sqlparser parses `-1` as `UnaryOp::Minus(Value(Number("1")))` — fold it, so `WHERE s._3 > -100` works — review 2026-09-05b); `IsNull`/`IsNotNull`/`IsTrue`/`IsNotTrue`/`IsFalse`/`IsNotFalse` (review 2026-09-05b) — the `IsNot*` forms are the pure negations of their cousins, so a MISSING operand answers `true` there (pinned, review 2026-09-06b R12); the `__s3_is_missing(X)`/`__s3_is_not_missing(X)` sentinels from Task 3's `IS [NOT] MISSING` rewrite (sqlparser 0.62 has no `IsMissing` variant) → `Value::Bool` of "the operand is `Field::Missing`" (a present-but-null value is NOT missing — review 2026-09-05b; recognized as single-part UNQUOTED identifiers only, review 2026-09-06b R15); `And`/`Or` short-circuit (`false AND x`, `true OR x` never evaluate x — the dead arm's errors must not fire; review 2026-09-06b R11); `Like` → `Unsupported("LIKE")` and `ILike` → `Unsupported("ILIKE")` (Task 6 implements `Like` only — AWS has no ILIKE); aggregate `Expr::Function` arms (beyond the two MISSING sentinels) are unreachable when `plan.aggregates` is false (guaranteed by sql.rs) — the arm returns `Value("internal: unexpected aggregate call")` as a guard (review 2026-09-05 #9: Function-based detection, not `Expr::AggregateExpr`).
- Comparison rules (spec §1 row.rs): Decimal↔Decimal, Int↔Int, Int↔Decimal promote; String↔String; String vs numeric → lazy `parse_number` (parse failure → comparison false); Bool only `=`/`!=`; `Null`/`Missing` in comparisons → false except under `IS NULL`/`IS [NOT] MISSING` (the MISSING tests reach eval as Task 3's sentinel functions), which distinguish a present Null from an absent field.
- Arithmetic: Int×Int stays Int with checked overflow → promote to Decimal; `/` → Decimal scale 10; `%` Int-only; division by zero → `Value("division by zero")`.
- `WHERE` = `eval(...) == Value::Bool(true)` only; `LIMIT` counts passing rows; `projections`: `Wild` → all record fields (keys: header names / `_1..`); `Item` → eval the expression (alias key) — JSON scalar-row semantics land in Task 9.

Tests (CSV fixtures):
- `SELECT * FROM S3Object s WHERE s._3 > 100 LIMIT 2` — filters then caps.
- `WHERE s._5 > 0` on 3-col CSV → zero rows, no error (Missing → false).
- string equality `WHERE s._1 = '1'`; `AND`/`OR`/`NOT`; arithmetic in WHERE (`s._1 + 1 > 10`, where `_1` = "10"); `IN ('a','b')`; `BETWEEN 1 AND 10` (string `_1` vs Int literal via lazy parse); `IS NULL` on a string → false (0 rows).
- `SELECT s.Id, s.Name AS n FROM S3Object s` → OutRow keys `["Id","n"]`.
- `WHERE s._2` (string "x" in boolean position) → false, no error.

- [x] **Step 1: Write failing tests**.
- [x] **Step 2: Run** → FAIL.
- [x] **Step 3: Implement** `engine.rs` per above.
- [x] **Step 4: Run** → PASS.
- [x] **Step 5: Commit** (ask user) — `feat(select): engine where/projection/limit`

---

### Task 6: LIKE + `finish` headroom (aggregates next task)

**Files:** `engine.rs` + tests (LIKE only; `finish` stays `Ok(None)`)

- Implement `Expr::Like` in `eval`: case-sensitive (grilling Q1), `%` any sequence, `_` one char, optional `ESCAPE 'c'`; the `negated` flag is honored (`NOT LIKE` — review 2026-09-05b); pattern and subject coerced via `display()`; non-string operands → false — **including under `NOT LIKE`** (review 2026-09-06b R13: the negation must not flip the non-string mismatch to true). `Expr::ILike` → `Unsupported("ILIKE")` (review 2026-09-05b): AWS S3 Select has no case-insensitive LIKE, so it must not silently behave as a case-sensitive LIKE. **The matcher is O(n·m) dynamic-programming** — no backtracking (review 2026-09-05 #5: naive recursion is exponential on `%`-heavy patterns; with the 256 KiB expression cap and the 1 MB record cap, DP keeps the worst case bounded) — **capped at 10⁷ DP cells** (pattern tokens × text chars; review 2026-09-06b R7: 256 KiB pattern × 1 MB record ≈ 2.6×10⁸ cells would pin a spawn_blocking worker for seconds-to-minutes — past the cap is a `Value("LIKE pattern too large")` error, not a run).
- Tests: `'hello' LIKE 'h%'` true; `'hello' LIKE 'h_llo'` true; `'hello' LIKE '%llo'` true; `'HELLO' LIKE 'h%'` false (case); `'hello' LIKE '%'` true; `_` matches exactly one char; `'a%b' LIKE 'a\%b' ESCAPE '\'` true; pattern with no `%`/`_` is exact equality; subject `Missing` → false; `NOT LIKE` negates (`'abc' NOT LIKE 'd%'` true); `'x' ILIKE 'X'` → `Unsupported("ILIKE")` (review 2026-09-05b).

- [x] **Step 1: Failing tests** → **Step 2: run fail** → **Step 3: implement** → **Step 4: pass** → **Step 5: commit** (ask user) — `feat(select): like matching`

---

### Task 7: Aggregates

**Files:** `engine.rs` + tests

**Interfaces:**
- Aggregate mode: when `plan.aggregates`, `next()` accumulates into `AggState { count: u64, per_col: Vec<AggCol> }` (`AggCol { count, sum: Option<Decimal>, min: Option<Value>, max: Option<Value> }` — the running extrema, typed by the first contributing value; review 2026-09-05b: the earlier `min: Max, max: Min` fields were swapped) and `finish()` produces the single row. Engine drives: caller loops `next()` to EOF, then calls `finish()`.
- `COUNT(*)` counts all rows; `COUNT(expr)` counts non-Missing/Null; `SUM`/`AVG` skip Missing/Null — AVG → Decimal scale 10; `MIN`/`MAX` over Decimal/Int/String (typed via the first non-Missing value); any aggregate with zero contributing values → Null; `COUNT` → `Int`.
- Aggregate AST forms: `CountStar`; `Count { expr, distinct: false }` — `distinct: true` → `Parse("distinct not supported")`; `Sum`/`Avg`/`Min`/`Max`.
- Aggregates + Wild rejected in sql.rs (Task 3) — the list is always explicit.

Tests: `count(*)` → 2 on 2 rows; `count(s._5)` (Missing) → 0; `sum(s._3)` per `_3` numeric strings; `avg(...)` exact (`1.5`); `min`/`max` incl. strings; zero-row object → `count=0`, `sum/avg/min/max = Null` (one all-Null row); `SUM` all-Missing → Null row.

- [x] **Step 1: Failing tests** → **Step 2: run fail** → **Step 3: implement** (accumulate + `finish`) → **Step 4: pass** → **Step 5: commit** (ask user) — `feat(select): scalar aggregates`

---

### Task 8: `output.rs` — CSV + JSON serializers

**Files:** Create `crates/tinio-select/src/output.rs` + tests

**Interfaces:**
- `pub enum OutputMode { Csv(CsvOutputParams), Json(JsonOutputParams) }`; `CsvOutputParams { field_delimiter: u8, record_delimiter: u8, quote: u8, escape: u8, quote_fields: QuoteFields }`; `pub enum QuoteFields { AsNeeded, Always }`; `JsonOutputParams { record_delimiter: u8 }`.
- `pub fn serialize_row(mode: &OutputMode, row: &OutRow) -> Result<Vec<u8>, Error>`
- CSV: csv `WriterBuilder` with `QuoteStyle::Necessary` (ASNEEDED, the **default** — grilling Q4) or `QuoteStyle::Always`; fields via `display()`; `Field::Missing` → empty string.
- JSON: `{"k": v, ...}` — key via `serde_json::to_string`; values: `Null`→`null`, `Bool`→bool, `Int`→int, `Decimal`→normalize-then-string via `row::display()` (`1.50` → `1.5`; still a valid JSON number — review 2026-09-05b), `RawNumber`→raw text verbatim, `String`→quoted, `Json`→nested JSON natively; `Field::Missing` → omit key; all keys missing → `{}` (spec: MISSING serializes as empty record). JSON record delimiter default `\n`.
- `Field::Present(Value::Json(..))` + `OutputMode::Csv` — scoped per input (decision 2026-09-05, grilling Q10): **parquet-input** nested (list/struct) is the `NestedCsv` error — checked by `events.rs::step` (the parquet row has no JSON matrix equivalent); **JSON-input** nested stays a cell: compact JSON text in the cell via `display()` (`{"a":1}`), the SELECT * matrix row (this serializer only; the `NestedCsv` error is never raised here).

Tests: CSV round (embedded delimiter/quote), `Always` quotes everything; JSON `{"id":"1"}` exact; `Decimal` `1.50` → `1.5`; `RawNumber("1e309")` → `1e309` unquoted; all-missing → `{}`; one missing key omitted; `Null` → `null`; JSON `\n` delimiter; JSON nested + CSV → compact cell; parquet nested + CSV → `NestedCsv` (the step-level check, Task 12).

- [x] **Step 1: Failing tests** → **Step 2: run fail** → **Step 3: implement** → **Step 4: pass** → **Step 5: commit** (ask user) — `feat(select): output serializers`

---

### Task 9: `json.rs` — LINES/DOCUMENT + path traversal + lookup

**Files:** Create `crates/tinio-select/src/json.rs` + tests; engine JSON arm replaced

**Interfaces:**
- `pub enum JsonType { Lines, Document }`
- `pub struct JsonReader<R: Read>` — `new(reader: R, ty: JsonType, from: &FromClause) -> Self`; yields `Record::Json(element)`. LINES buffers per line with the **1 MB input-record cap** (review 2026-09-05 #3: line length past 1 MB → `Format("input record exceeds 1 MB")`); DOCUMENT parses the whole stream once — the record is byte-0-anchored but still capped at 1 MB.
- Traversal (spec): `S3Object[*]` = document as an array of root values (DOCUMENT root = one element; LINES root = per line). Per segment: `.name` → field of each element (absent → MISSING step); `[i]` → index i; `.*`/`[*]` → iterate values (zero matches → emit exactly one MISSING row); `['name']` → field. Final segment value(s) become the yielded element (already-deep part of the path ends inside `segments`). A case-folded duplicate key in a `.name` segment is `Ambiguous` — the same rule as the engine's select-side lookup (review 2026-09-06b R10).
- Engine JSON lookup (replaces the CSV-only arm): scalar element (`Value` not an object) → any unqualified ref resolves to the scalar; object → case-insensitive field when unquoted, exact when quoted; `_1` → whole record (unless record has a `_1` key — field wins); `[i]`/`['k']` subscripts via `CompoundFieldAccess`/`AccessExpr::Subscript` and `JsonAccess` (sqlparser 0.62 AST — no `Expr::Subscript`; review 2026-09-05 #9); missing → `Field::Missing`.
- JSON number tokens → `Value::RawNumber(n.to_string())` (arbitrary_precision — verbatim token, grilling Q3); JSON `null` → `Value::Null`; strings/bools/ints → their variants.

Tests: LINES 2 objects; DOCUMENT 1 object (multi-line); traversal `S3Object[*].Rules[*].id` per the AWS doc example (2 roots → 4 rows incl. `{}` empty records; `WHERE id IS NOT MISSING` variant omits them); `[0]` index; `['name']`; `. *` zero-match → one empty record; DOCUMENT root array; `SELECT _1.dir_name, _1.owner FROM S3Object[*]` whole-row refs; `SELECT price FROM S3Object[*].books[*].price` scalar-row semantics; raw-number passthrough `SELECT x FROM S3Object s` with `{"x": 1e309}` → `{"x":1e309}` (no decimal parse).

- [x] **Step 1: Failing tests** → **Step 2: run fail** → **Step 3: implement** (reader + engine arm) → **Step 4: pass** → **Step 5: commit** (ask user) — `feat(select): json reader with traversal`

---

### Task 10: Compression + ScanRange

**Files:** `crates/tinio-select/src/record.rs` (decompression + range filter), `events.rs` (config), tests

**Interfaces:**
- `pub enum Compression { None_, Gzip, Bzip2 }`
- `pub fn decompressed(compression: Compression, r: Box<dyn std::io::Read + Send>) -> Box<dyn std::io::Read + Send>` — sync wrappers (review 2026-09-05: no async in the crate): `Box::new(flate2::read::MultiGzDecoder::new(r))` / `Box::new(bzip2::read::BzDecoder::new(r))` (bzip2: single-stream `BzDecoder`; multi-member gzip via `MultiGzDecoder`).
- `pub struct RangeFilter<R: RecordReader>` — wraps a reader, tracks the uncompressed byte position of each record's first byte; yields the record only when its start ∈ [start, end]; drops pre-range records; returns `Ok(None)` after a record whose start > end (documents the JSON DOCUMENT case: whole object = record at byte 0, so `start > 0` yields nothing — correct, not a bug). `end`-only: `start = size - end`; the *server* resolves the window against the fetched object size (Task 13; review 2026-09-05: the read path returns `info.size` on the same call, no `head_object`).
- `ScanRange { start: u64, end: Option<u64> }` is the named window type (review 2026-09-05, Data Clumps); `SelectConfig.scan_range: Option<ScanRange>` carries the *resolved* window (Task 12) — there is no `SelectConfig.size` (removed in the review: write-only, the server resolves against `info.size`).
- Validation (from spec; enforced by the server in Task 13): ScanRange ⇒ `Compression::None_`.

Tests: 4 records at known byte offsets — `start=10` drops the partial first record; both bounds; nothing in range → `Ok(None)` immediately; end-only via `size`; gzip round-trip (flate2 write→read via `std::io::Cursor`); bzip2 round-trip (`bzip2::write::BzEncoder`); DOCUMENT + `start>0` → no records.

- [x] **Step 1: Failing tests** → **Step 2: run fail** → **Step 3: implement** → **Step 4: pass** → **Step 5: commit** (ask user) — `feat(select): gzip/bzip2 streaming + scan range`

---

### Task 11: `parquet.rs` (feature-gated)

**Files:** Create `crates/tinio-select/src/parquet.rs` + tests; modify `src/lib.rs` (`#[cfg(feature = "parquet")] pub mod parquet;`), `record.rs` (arm), `output.rs`'s `Json` path (already supported), `row.rs` (already defined)

**Interfaces:**
- Memory bound (review 2026-09-05): the storage path has no seek, so the crate buffers the whole parquet object into a `Vec<u8>` first (events.rs `build_reader`) — `ParquetRecordBatchReaderBuilder::try_new` needs a `ChunkReader`, and a bare `R: Read` does not satisfy it. parquet 59 implements `ChunkReader` for `File` and `bytes::Bytes` **only** (no `AsRef<[u8]>` blanket exists — verified against the 59.3.0 source, review 2026-09-05), so the buffered `Cursor<Vec<u8>>` moves into a zero-copy `bytes::Bytes` via the crate's **optional** `bytes` dep (`arrow`/`bytes` are optional, parquet-only; the 2026-09-05b "no bytes" line is superseded by review 2026-09-05). The bound check is the server's 400 before streaming, `SelectConfig.max_parquet_bytes`, default 256 MiB; the reader errors `ParquetTooLarge` if the buffered input exceeds the bound (defense in depth).
- `pub struct ParquetReader` — `new(input: std::io::Cursor<Vec<u8>>, projection: Vec<String>, max_bytes: u64)` via `parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(input)` + `with_projection(ProjectionMask::columns(schema, projector))`; rows → `Record::Parquet(fields, names)`. (review 2026-09-05b: no generic `R: Read` — `try_new` takes a `ChunkReader`.)
- Type mapping: Int64/Int32→Int; Float/Double→`RawNumber` carrier (review 2026-09-06b R6: finite floats render verbatim through SELECT * and parse lazily on the 28-digit spine only when a numeric operator consumes them — the CSV/JSON lazy rule; NaN/inf stay a row-build `Value` error, never a silently wrong number); Boolean→Bool; Utf8→String; Decimal→Decimal (arrow precision > 28 → `Value("parquet decimal precision exceeds 28 digits")`); Timestamp→string ISO via `time::OffsetDateTime::from_unix_timestamp(...)` → `format(&Rfc3339)` (workspace `time` dep, add to manifest); List/Struct→`Value::Json` (arrow nested → `serde_json::Value`; render native in JSON output, `NestedCsv` for CSV output — the parquet arm of grilling Q10, checked by Task 12).
- Projection = the set of column names referenced by SELECT/WHERE (extracted in Task 3's plan; add a `pub fn referenced_columns(plan: &QueryPlan) -> Vec<String>` to sql.rs).

Tests (`#[cfg(feature = "parquet")]`, run with `cargo test -p tinio-select --features parquet`): write a fixture with the `parquet`'s arrow writer (int/float/string/decimal(≤28)/timestamp/nested), read back per mapping; decimal precision 29 → error; projection prunes (schema fields unselected read as 0 — assert via the wrong-name lookup → Missing).

- [x] **Step 1: Failing tests** → **Step 2: run fail** → **Step 3: implement** → **Step 4: pass** → **Step 5: commit** (ask user) — `feat(select): parquet reader`

---

### Task 12: `events.rs` — SelectEvent + `select_iter` (assembly after all readers)

**Files:** Create `crates/tinio-select/src/events.rs` + tests; modify `src/lib.rs` (export)

**Interfaces:**
- `pub enum SelectEvent { Records(Vec<u8>), Progress(ByteCounters), Stats(ByteCounters), Cont, End }` — `ByteCounters { bytes_scanned: u64, bytes_processed: u64, bytes_returned: u64 }` is the named counter triple (review 2026-09-05, Data Clumps refactor).
- `pub enum InputFormat { Csv(CsvParams), Json(JsonParams), Parquet(ParquetParams /* projection */) }`
- `pub struct SelectConfig { pub input_format: InputFormat, pub output: OutputMode, pub compression: Compression, pub scan_range: Option<ScanRange>, pub request_progress: bool, pub cont: ContPolicy, pub max_parquet_bytes: u64 }` — `scan_range` is the **resolved** window (`end`-only `start = size - end` is the server's job, Task 13); no `size` field (removed in the review 2026-09-05 — write-only, the op resolves against `info.size`); `max_parquet_bytes` default 256 MiB (review 2026-09-05).
- `pub struct ContPolicy { pub idle: Option<Duration>, pub every_n: Option<usize> }` — defaults `Some(5s)`, `Some(4096)` (grilling Q3 spec).
- `pub fn select_iter(plan: QueryPlan, config: SelectConfig, input: Box<dyn std::io::Read + Send>) -> impl Iterator<Item = Result<SelectEvent, Error>>` — a hand-written state-machine `Iterator`; **no async anywhere in the crate** (review 2026-09-05). Pipeline per `next()` pull: `decompressed` → `RangeFilter` → per-format reader → engine next/finish → serialize → buffer — the parquet arm's whole-object slurp checks `max_parquet_bytes` *while* reading (`Read::take(max+1)`, review 2026-09-06b R14: a plain `read_to_end` slurps the object before the check, defense in name only):
  - Flush `Records` when buffer ≥ 1 MB, or when appending a record would push past 1 MB (flush first, single record > 1 MB → `TooLarge`).
  - Counters at named call sites (spec Decisions, review 2026-09-05): `bytes_scanned` += bytes consumed from the reader; `bytes_processed` += raw record payload byte length handed to the engine; `bytes_returned` += serialized bytes. Progress when `request_progress` and ≥1s elapsed or ≥1MB scanned since the last Progress (grilling Q2) — checked at emit points only (pull-driven timing, documented deviation).
  - Cont: every `every_n` records, and if ≥`idle` (5s) since the last emitted event.
  - After EOF: `engine.finish()` row (aggregates) serialized into the buffer, then `Stats` + `End`.

Tests (sync — plain iterator `.collect::<Result<Vec<_>, _>>()`, no executor):
- 3 CSV rows → `[Records(..), Stats{..}, End]`; Stats `bytes_scanned` = input bytes.
- >1 MB single row → the iterator errors `TooLarge`.
- `cont: ContPolicy { idle: None, every_n: Some(2) }` → Cont between every 2nd Records.
- Progress counters advance across records.
- Empty input → `[Stats{zeros}, End]`.
- A record just under 1 MB is emitted whole after the boundary flush (records are never split — review 2026-09-05 #10; the old "1.5 MB row splits across events" test is **wrong**: flush-at-boundary + `TooLarge` make a split impossible — removed).

- [x] **Step 1: Failing tests** → **Step 2: run fail** → **Step 3: implement** → **Step 4: pass** → **Step 5: commit** (ask user) — `feat(select): event stream adapter`

---

### Task 13: tinio-server — features, Capabilities, op, s3s mapping

**Files:**
- Modify: `crates/tinio-server/Cargo.toml` (features + dep), `crates/tinio-server/src/backend/mod.rs` (`mod select;`), `crates/tinio-server/src/backend/s3.rs` (impl)
- Create: `crates/tinio-server/src/backend/select.rs`
- Modify: `crates/tinio-config/src/schema/s3.rs` (`Capabilities.select`)

**Interfaces:**
- `Capabilities.select: bool` — `#[serde(default = "select")] #[default = true]` + `fn select() -> bool { true }` (mirror `multipart`).
- `op_select_object_content(&self, req: S3Request<dto::SelectObjectContentInput>) -> S3Result<S3Response<dto::SelectObjectContentOutput>>`.

Steps:

1. **Failing integration test** in `backend/select.rs` (style of `objects.rs` tests — use the exact fixture helper from `testutil.rs`):

```rust
#[tokio::test]
async fn select_filters_by_column() {
    let (backend, bucket) = setup_name().await;
    backend
        .op_put_object(S3Request::new(dto::PutObjectInput {
            bucket: bucket.clone(),
            key: "data.csv".into(),
            body: Some(StreamingBlob::wrap(... /* "a,b\n1,x\n2,y\n" */)),
            ..Default::default()
        }))
        .await
        .unwrap();
    let req = S3Request::new(dto::SelectObjectContentInput {
        bucket,
        key: "data.csv".into(),
        request: dto::SelectObjectContentRequest {
            expression: "SELECT s._1, s._2 FROM S3Object s WHERE s._1 = '1'".into(),
            expression_type: dto::ExpressionType::from("SQL"),
            input_serialization: dto::InputSerialization { csv: Some(Default::default()), ..Default::default() },
            output_serialization: dto::OutputSerialization { csv: Some(Default::default()), ..Default::default() },
            request_progress: None,
            scan_range: None,
        },
    })
    .into();
    let resp = backend.op_select_object_content(req).await.unwrap();
    let records = collect_records(resp.payload.unwrap()).await; // drain s3s event stream; join Records payload bytes
    assert!(records.contains("1,x"));
}
```

(`dto::Expression::from("...")`-style conversions and exact `StreamingBlob`/`S3Request` constructor names: match existing `objects.rs` tests.)

2. **Validation unit tests** (each asserts 400 + code, review 2026-09-05): JOIN in expression → `S3QueryParsingError` (Custom, **must carry `set_status_code(BAD_REQUEST)`** — plain `Custom` serializes as 500, #2); expression > 256 KiB → same; `ExpressionType != SQL` → `InvalidRequestParameter`; parquet + GZIP; parquet + ScanRange (→ reject, #6); scan_range + GZIP; scan_range negative / empty / `start > end` / `end >= size` (checked arithmetic, #6); neither input serialization present; JSON output + `SELECT s.x+1 FROM S3Object s` (no alias) → `InvalidRequestParameter`; **CSV output + same query → allowed** (alias rule scoped); parquet object over `max_parquet_bytes` → `InvalidRequestParameter`; missing object → `NoSuchKey`; `Capabilities.select = false` → `NotImplemented`; concurrent select jobs beyond the semaphore admission → requests wait (semaphore test with a slow fixture, #4). **Stream-level error tests** (not 400): CSV `FileHeaderInfo=USE` with duplicate headers + named ref → the event stream error item carries `S3ErrorCode::AmbiguousFieldName` (**the real variant — exists in s3s, #11**); unknown named header → `Custom("MissingHeaderName")`; input record > 1 MB → `Custom("S3QueryError")` with the cap message (#3); other runtime errors → `Custom("S3QueryError")`.

3. Implement `backend/select.rs` (validation + **async→sync bridge**, spec §1.1):

```rust
#[cfg(feature = "select")]
pub(crate) async fn op_select_object_content(
    &self,
    req: S3Request<dto::SelectObjectContentInput>,
) -> S3Result<S3Response<dto::SelectObjectContentOutput>> {
    Self::require_cap(self.caps.select, "SelectObjectContent")?;
    // Concurrency cap (review 2026-09-05 #4): admission via a Semaphore (default 4; the permit is
    // held for the whole streaming response — acquire_owned, drop on stream end). Acquired AFTER
    // `build_config` (review 2026-09-06b R9): invalid requests must not momentarily hold slots.
    let permit = SELECT_SEMAPHORE.clone().acquire_owned().await.map_err(...)?;
    let bucket = self.bucket(req.input.bucket)?;
    let key = self.key(req.input.key)?;
    let r = &req.input.request;
    // 1. static validation first — nothing streamed yet (each failure ->
    //    S3Error::with_message(code, msg); review 2026-09-05, constructor form fixed 2026-09-05b):
    //    expression_type == SQL; expression <= 256 KiB; exactly one of csv/json/parquet input;
    //    exactly one of csv/json output; parquet ⇒ compression NONE and no ScanRange;
    //    scan_range ⇒ compression NONE; JSON output ⇒ bare-expression projections need aliases
    //    (CSV output allows them). Custom-coded request errors (S3QueryParsingError) MUST
    //    set_status_code(BAD_REQUEST) — s3s serializes a Custom with no status override as HTTP 500
    //    (findings #2/#11; the in-stream Custom codes in step 4 sit inside the 200 stream and need
    //    no status override).
    // 2. fetch = get_object(&bucket, &key, None).await — body stream AND GetObjectResult.info.size
    //    in one storage round trip (no head_object; review 2026-09-05b).
    // 2b. size-dependent checks against info.size, still before a body byte is consumed (spec §2,
    //     review 2026-09-05b): ScanRange window (non-negative, non-empty, start <= end, end < size,
    //     end-only = trailing [size-end, size-1]; checked arithmetic, no underflow); parquet ⇒
    //     object size <= max_parquet_bytes.
    // 3. bridge (spec §1.1, review 2026-09-05):
    //    let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(4);
    //    tokio::spawn(async move { pump body stream into tx (drop on stream end) });  // forwarder, backpressure
    //    let input = Box::new(ChannelReader { rx });           // std::io::Read over rx.blocking_recv() (blocking thread only)
    //    let engine = async move { ... };                       // see select_iter signature
    //    tokio::task::spawn_blocking(move || { catch_unwind(assert || select_iter(plan, config, input)
    //        blocking_send loop) ... })   // CPU off the workers; a panic maps to an in-stream
    //                                     // S3QueryError item, never a silent stream end (review 2026-09-06b R8)
    //    -> results flow into a second mpsc
    // 4. s3s stream = hand-rolled futures::Stream over Receiver::poll_recv (spec §1.1 — no
    //    tokio-stream; review 2026-09-05b), .map(event-mapper).map_err(error-mapper):
    //    SelectEvent::Records(b)      -> dto::SelectObjectContentEvent::Records(dto::RecordsEvent { payload: Some(Body::from(b)) })
    //    Progress/Stats               -> dto::{ProgressEvent, StatsEvent} from the counters
    //    Cont                         -> dto::SelectObjectContentEvent::Cont(dto::ContinuationEvent)
    //    End                          -> dto::SelectObjectContentEvent::End(dto::EndEvent)
    //    Err(Error::Ambiguous(m))     -> S3Error::with_message(S3ErrorCode::AmbiguousFieldName, m) (real variant, #11)
    //    Err(Error::MissingHeader(m)) -> S3Error::with_message(S3ErrorCode::Custom("MissingHeaderName".into()), m) (in-stream)
    //    Err(e)                             -> S3Error::with_message(S3ErrorCode::Custom("S3QueryError".into()), e.to_string())
    //    (review 2026-09-05b: S3Error::new takes only the code — the message form is with_message;
    //    Custom's payload is a ByteString, so "..".into() — a b".." byte literal does not coerce)
    //    then SelectObjectContentEventStream::new(...).
}
```

`ChannelReader` (in `select.rs`): `impl std::io::Read for ChannelReader { fn read(&mut self, buf) { match self.rx.blocking_recv() { ... EOF when sender dropped ... } } }` — used only on the blocking thread (documented: `blocking_recv` must not be called on a tokio worker).

4. `s3.rs`:

```rust
#[cfg(feature = "select")]
async fn select_object_content(&self, req: S3Request<dto::SelectObjectContentInput>) -> S3Result<S3Response<dto::SelectObjectContentOutput>> {
    self.op_select_object_content(req).await
}
```

5. `Cargo.toml` (tinio-server): `default = ["multipart", "copy", "list-v1", "list-v2", "select"]`; `select = ["dep:tinio-select"]`; `select-parquet = ["select", "tinio-select/parquet"]`; `tinio-select = { workspace = true, optional = true }`. No `tokio-stream` — the s3s output side is a hand-rolled `futures::Stream` over `Receiver::poll_recv` (review 2026-09-05b, spec §1.1).
6. Verify: `cargo test -p tinio-server --features select`; `cargo check -p tinio-server --features select-parquet`; `cargo check -p tinio-server --no-default-features`; `cargo test -p tinio-server` (defaults, other suites unchanged).
7. Commit — `feat(server): select_object_content op with s3s event mapping`

---

### Task 14: cucumber e2e `select.feature`

**Files:** Create `crates/tinio-e2e/tests/features/select.feature` (+ steps wiring per `docs/tests.md`; tags match an existing feature, e.g. `@fs @mem`)

```gherkin
@fs @mem
Feature: S3 Select over objects
  Scenario: filter CSV by column position
    Given a bucket
    And an object "data.csv" with content:
      """
      a,b
      1,x
      2,y
      """
    When I select over object "data.csv" with query "SELECT s._1, s._2 FROM S3Object s WHERE s._1 = '1'"
    Then the select results contain "1,x"

  Scenario: count(*) over JSON lines
    Given a bucket
    And an object "data.jsonl" with content:
      """
      {"name":"alice","age":30}
      {"name":"bob","age":40}
      """
    When I select over object "data.jsonl" with query "SELECT count(*) FROM S3Object s"
    Then the select results contain "2"

  Scenario: LIMIT caps results
    Given a bucket
    And an object "data.csv" with content:
      """
      1,2
      3,4
      5,6
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s LIMIT 2"
    Then the select results contain "1,2" and "3,4" but not "5,6"

  Scenario: GZIP compressed CSV input
    Given a bucket
    And a gzip-compressed object "data.csv.gz" with content:
      """
      1,x
      2,y
      """
    When I select over object "data.csv.gz" with query "SELECT * FROM S3Object s"
    Then the select results contain "1,x"
```

(Exact steps are wired to the e2e client per `docs/tests.md` and the existing world; GZIP fixture built by the step.)

- [x] **Step 0 (before the feature file): traceability registration** (review 2026-09-05) — `crates/tinio-e2e/tests/traceability.rs` cross-checks feature tags ↔ spec IDs; add the next requirement ID to `specs/001-s3-local-server` (contracts + checklists, following the existing convention and numbering), and register it in the traceability map, **before** `select.feature` lands. Without it the suite fails.
- [x] Write `select.feature` + wire steps; run `cargo test -p tinio-e2e` (per docs/tests.md) → green.
- Commit (ask user) — `test(e2e): select_object_content feature`

---

### Task 15: criterion bench

**Files:** Create `crates/tinio-select/benches/select_scan.rs`

- Generate a 100k-row CSV (4 cols) in the bench setup; two criterion groups: full-scan (`SELECT * FROM S3Object`) vs filtered (`SELECT * FROM S3Object s WHERE s._4 > 0`); measure with `iter_batched`; report MB/s.
- Run: `cargo bench -p tinio-select --bench select_scan -- --quick` (full run optional).
- Commit (ask user) — `bench(select): full vs filtered CSV scan`

---

## Self-Review

- **Spec coverage:** §1 crate/modules ↔ Tasks 1–12; §2 server ↔ Task 13; §3 errors ↔ Task 3 (parse) + 5–7 (runtime) + 13 (mapping); §4 tests ↔ per-task + 14/15; phases ↔ task order (readers → engine → serializers → assembly, respecting dependencies: `events` assembly sits after all reader tasks per grilling Q1).
- **Grilling (plan round):** Q1 reorder ✓ (Task 12 after all readers), Q2 in-stream unified `Custom("S3QueryError")` ✓ (Global Constraints + Task 13), Q3 `RawNumber` passthrough ✓ (Task 2 enum + Task 9 raw-number test + Task 8 render), Q4 ASNEEDED default ✓ (Task 8), Q5 per-task commits ✓.
- **External review 2026-09-05 applied (full report):** #1 sync/async bridge ✓ (§1.1 + Tasks 1/10/12/13, `ChannelReader`/`spawn_blocking`/bounded mpsc, cross-chunk regression in Task 13 stream tests), #2 Custom→500 ✓ (`set_status_code(BAD_REQUEST)`, Task 13), #3 input-record 1 MB cap ✓ (Tasks 4/9/12 + `Format` message), #4 worker starvation ✓ (`spawn_blocking` + semaphore cap, Task 13), #5 LIKE O(n·m) ✓ (Task 6 + 256 KiB expression cap, Task 3), #6 ScanRange validation set ✓ (Task 13 + parquet+scan rejection), #7 defaults + AllowQuoted deviation ✓ (Global Constraints/Tasks 4/13), #8 header errors in-stream ✓ (spec §3, Task 13 mapping), #9 sqlparser 0.62 AST vocabulary ✓ (Tasks 3/5/9 + Global Constraints), #10 split-record test contradiction ✓ (Task 12 tests), #11 real `AmbiguousFieldName` variant ✓ (Global Constraints + Task 13), #12 BigDecimal leftover ✓ (spec Grilling Q5), #13 `time` dep + Task 10 rewording ✓ (Task 1 manifest, Task 10), #14 `preserve_order` + JSON key order ✓ (Task 1 manifest, spec `SELECT *` matrix).
- **External review 2026-09-05b applied:** manifest/feature wiring (workspace-dep `optional` removed per RFC 2906; the crate declares `parquet = { workspace = true, optional = true }` — Task 1), alias-rule placement (parse accepts bare-expression projections; the server enforces for JSON output only — Tasks 3/13), `IS [NOT] MISSING` pre-processor rewrite + MISSING eval arms (Tasks 3/5/9), parquet `ChunkReader` signature (`Cursor<Vec<u8>>`; `bytes` optional, parquet-only — Task 11, superseded by review 2026-09-05), `S3Error` constructor (`with_message`, `ByteString` payloads — Tasks 1/13), sqlparser AST shape (`CompoundFieldAccess { root, access_chain }` — Global Constraints), size source (`GetObjectResult.info.size`, one round trip, no `head_object` — Global Constraints/Task 13), tokio-stream removal (hand-rolled `futures::Stream` — Global Constraints/Task 13), eval coverage nits (IsNot* arms, negated flags, numeric unary minus/plus, ILIKE unsupported, Decimal JSON rendering, `AggCol` field fix — Tasks 5–8).
- **Placeholders:** none — the soft spots flagged are executor facts ("confirm at implementation" in Global Constraints): exact `testutil` helper names, e2e step wiring, `parquet` 59.x pin, `dto` Default impls. They are lookup tasks, not design gaps.
- **Type consistency:** `Error`/`SelectEvent`/`SelectConfig`/`OutRow`/`Record`/`Value` names consistent across every task; engine API `next`/`finish` consistent with the events adapter; `Compression`/`InputFormat` enum names consistent between Tasks 10/12; `Value::Json` defined once (Task 2), populated (Task 11), rendered/errored (Task 8); `select_iter` name consistent between spec §1 and Tasks 12/13; `ChannelReader`/bridge terms consistent across spec §1.1 and Task 13.
