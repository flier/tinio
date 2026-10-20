# Code Style

## Imports

- Top `use`, never inline; nest groups; 3+/2-seg: drop qualifier unless generic/prefix/collision.
- `clippy::absolute_paths`; glob `Error::*` only in `error.rs` / tests.
- `tinio_*` → `_`+rest via hidden `extern crate` in `lib.rs` (`#[cfg(feature)]`; test `#[cfg(test)]`).
- In-crate `crate::{_…, path}`; else crate path.
- Prefer `tokio::fs` except sync / `spawn_blocking` / no-async-API / `Metadata`.

## Types, JSON, derives

- Untrusted → ctor; qualify the type, import the module; `From<&str>` literals only.
- Defaults: container `#[serde(default)]`; field attr only for a non-default (`skip_serializing_if`); flatten ok, const-generic panics.
- Else: `DEFAULT_<FIELD>` + `#[default(_code = …)]` + one-line helper; never `X::default().<field>`; `String` → `&str`+`.to_owned()`.
- Prod JSON: `Serialize` struct + `to_string()`, never `json!` (tests, `Value` shapes exempt); field order = declaration order.
- Derive: `Display`/`FromStr` → `parse-display`; rest → `derive_more`; `Default` → `SmartDefault`.
- Hand `impl` only thiserror `Error` and delegating wrappers; style attrs over per-variant formats; never `impl` in a backend.
- Contract newtype as a statement parameter: backend traits behind one backend-agnostic feature; foreign PG pin → `as _`.

## Validation & modules

- Garde: custom rule `fn(&T, &Context) -> garde::Result`; rules private; entry = constructor; the attribute spells it bare, imported.
- Thin `lib.rs`/`mod.rs`; path-expose; `{Name, name}`; module fns.
- Schema: drop prefix; `Config`+`Version`; table open/ensure via the crate's macro.
- Parse only the operator's file (`Config::load`) or launcher text, never fabricated; build the struct; tests exempt.
- ETag: `Single` / `Composed`; parse `FromStr`; emit `Display`; content/composed constructors.

## Instrumentation

- `#[instrument]` on the function; never `.instrument()` — a hand-built future gets an annotated helper.

## Error & async

- Per-crate `Error`; private `mod error`; qualify. Wrap storage; store-redb display verbatim; extras → `Io`.
- Refused arm unwraps before the database arm (D22).
- Error: reuse the fitting variant; else `Invariant(&'static str)`; detail to log/metrics only (D5).
- Fallback only where `Result` is impossible (`Display`, `Drop`, foreign trait): defined + `tracing` (D6).
- Async traits: `async-trait` + `async fn`; pinned stream aliases.
- Storage aggregates `*Ops`; `Error: Into<storage::Error>`; methods use `<Self as Storage>::Error`.

## Panics

- Business code returns, never panics: `unwrap`/`expect`/`panic!`/`assert!` family confined to tests, benches, doc examples.
- `unwrap_err`/`expect_err`/`unreachable!`/`todo!`/`unimplemented!` are in that family too (D1).
- `#[expect(<lint>, reason = "…")]`, prefix one of four (D2): `literal:` compile-time input; `infallible:` cannot fail.
- `guarded:` invariant visible in the same expression; `argued:` unreachable, argument + catching mechanism named.
- Runtime-failable never; eliminate over annotate; `#[expect(…, reason)]`, never an outer `#[allow(…)]` (D2).
- Lock: `.lock()` recovers — `.unwrap_or_else(PoisonError::into_inner)`; never unwrap, never propagate (D3).
- Out of rule: indexing/slicing, arithmetic overflow, `debug_assert!` (D4).

## Test-code exceptions

- Test code states `#[allow]`/`#[expect]` with one prefix (D7): `test support:` support module or bench; `test context:` test module/fn.
- `#![allow(…)]` boundaries sit outside `clippy::allow_attributes` — an `unsafe_code` module, a shared test module (D2, D7).
- Gate: eight lints warn per crate (CI `-D warnings`); assert family + reason prefixes are held by review (D7, D8).

## SQL

- Every `.sql` file: sqruff-formatted and lint-clean after edit (postgres, 80 cols); CI `sql` job.
- One-line statements inline in `query!`/`query_as!`/`query_scalar!`; longer → `sql/<group>/<name>.sql`.
- No runtime `sqlx::query(…)`/`query_scalar::<_, T>(…)` in production code: unchecked. Tests, const SQL, DDL exempt.
- No comments inside SQL (`--`): the *why* goes in Rust — a `//` above the query or the fn's doc. **Migrations keep theirs** (checksum).
- Statement files carry no comments; the reason lives at the Rust call site.
- Longer form via `query_file*!`; DDL, templated or macro-unexpressable array binds → `include_str!`.
- Migrations: `sqlx::migrate!` directory — no Rust list of versions.
- Returning columns decode into a row struct beside the call (field order = column order), never by position.

## SQL params & review

- Non-null / nullable via `AS "name!"` / `AS "name?"` when inference would lie.
- Array params by reference for `= ANY ($1)`.
- By review, not suite: every statement inline in a macro; every `.sql` file named by Rust or a `migrate!` dir.
- Reformatting is whitespace/case only.
- SQL edit = code change. Regenerating the `.sqlx` cache: `docs/cargo.md` §sqlx cache.

## Docs & examples

- Public item: doc comment + runnable rustdoc example. Exempt classes: constitution III.
- `no_run` only when a runnable example would mock >10 trait methods; MUST say what it would have shown.
- Small free functions have no such excuse.
- English bullets. Scripts go to the temp dir. Prose: `docs/compress.md`. Git: `docs/git.md`. Gherkin/e2e: `docs/tests.md`.
- `docs/*.md` prose lands compressed (≤140-char items, ≤7-item sections, no commands/paths/dates); check: `docs/compress.md`.
- Plans/specs/design: no hard wraps — break only on blank/table/fence/heading.
- CLAUDE.md = always-loaded pointer; each `docs/*.md` = SoT for its branch. No restating in both.
