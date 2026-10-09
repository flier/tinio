# Code Style

## Imports

- Top `use`; never inline. Nest groups. 3+/2-seg: drop qualifier unless generic/prefix/collision. `clippy::absolute_paths`. Glob `Error::*` only `error.rs`/tests.
- `tinio_*` → `_`+rest via hidden `extern crate` in `lib.rs` (`#[cfg(feature)]`; test `#[cfg(test)]`). In-crate `crate::{_…, path}`; else crate path. Prefer `tokio::fs` except sync / `spawn_blocking` / no-async-API / `Metadata`.

## Types, JSON, derives

- Untrusted → ctor. Qualify the type; import the module. `From<&str>`: literals (panic). Defaults: `SmartDefault` / `#[serde(default)]`.
- Prod JSON: `Serialize` struct + `to_string()`, never `json!` — `json!` → map → unstable key order. Struct: declaration order; drop/rename = compile error. `json!` OK: tests; `Value`-typed where untyped shape *is* value.
- Derive: `Display`/`FromStr` → `parse-display`; rest → `derive_more`; `Default` → `SmartDefault`. Hand `impl` only thiserror `Error` and a wrapper that delegates to what it wraps. Mechanism over literal: style attrs, not one format string per variant.
- Contract newtype as a statement parameter: backend traits behind an optional, backend-agnostic feature of the contract crate — never default, never `impl` in the backend (orphan). Derive cannot reach it → hand-impl behind that feature, wire string + read-path self-heal. Macro pins a foreign Postgres type → wildcard cast (`as _`).

## Validation, modules, Error, async, ETag

- Garde: custom rule `fn(&T, &Context) -> garde::Result`; rules private; entry = constructor.
- Thin `lib.rs`/`mod.rs`; path-expose; `{Name, name}`; module fns. Schema: drop prefix; `Config`+`Version`. Table open/ensure via the crate's macro.
- Per-crate `Error`; private `mod error`; qualify. Wrap storage; store-redb display verbatim; extras → `Io`. Refused arm unwraps before the database arm — a `#[from]` hop would degrade contract refusals to `Io` (design 2026-10-03 D22).
- Async traits: `async-trait` + `async fn`. Storage aggregates `*Ops`; `Error: Into<storage::Error>`; methods use `<Self as Storage>::Error`; pinned stream aliases.
- ETag: `Single` / `Composed`; parse `ETag::new`; emit `Display`; content/composed constructors.

## Doc examples

- Public item: doc comment + runnable rustdoc example. Exempt classes: constitution III.
- `no_run` only when a runnable example would mock more than ten required trait methods — below that the double stays readable; above it is a reimplementation. `no_run` MUST still say in prose what it would have shown. Small free functions have no such excuse.

## SQL

- Format: sqruff (postgres, 80 cols, upper keywords/literals, lower idents). Lint after edit; CI `sql` job. Ignore list holds dialect gaps; drop an entry once parseable.
- Short (one-line) statements may live inline in `query!` / `query_as!` / `query_scalar!`. Longer → `sql/<group>/<name>.sql` via `query_file*!`. DDL, templated, or macro-unexpressable array binds → `include_str!`. Migrations: `sqlx::migrate!` directory — no Rust list of versions.
- Returning columns decode into a row struct beside the call (field order = column order), never by position. Non-null / nullable via `AS "name!"` / `AS "name?"` when schema inference would lie. Array params by reference for `= ANY ($1)`.
- By review, not suite: no statement outside a single-line inline macro; every `.sql` file named by Rust (or under a `migrate!` dir). Reformatting is whitespace/case only.
- SQL edit = code change (macros `include_str!`). Regenerating the `.sqlx` cache: `docs/cargo.md` §sqlx cache.

## Docs & scripts

- English bullets. Scripts → `/tmp`. Prose: `docs/compress.md`. Git: `docs/git.md`. Gherkin/e2e: `docs/tests.md`.
- Plans/specs/design: no hard wraps — break only on blank/table/fence/heading.
- CLAUDE.md = always-loaded pointer; each `docs/*.md` = SoT for its branch. No restating in both.
