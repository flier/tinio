# Code Style

## Imports

- Top `use`; never inline. `super::server` → `server::Config`. Nest `foo::{Bar, dto::{self, Baz}}`. 3+/2-seg: drop qualifier unless generic/prefix/collision. `clippy::absolute_paths`. Glob `Error::*` only `error.rs`/tests.
- `tinio_*` → `_`+rest: `lib.rs` `#[doc(hidden)] pub extern crate tinio_store_redb as _store_redb;` (`#[cfg(feature)]`; test `#[cfg(test)] extern crate`). In-crate `crate::{_core::{...}, path}`; else `tinio_fs::`. `tokio::fs` except sync/`spawn_blocking`/no-async-API/`Metadata`.

## Types & defaults

- Untrusted → ctor (`bucket::name`, …). Qualify (`bucket::Name`); import module. `From<&str>`: literals (panic). `SmartDefault`/`#[serde(default)]`.

## JSON

- Prod: `#[derive(Serialize)]` struct (borrowed fields) + `to_string()`, never `json!`. `json!` → map → `serde_json` key order (sorted; graph `preserve_order` → same file, two byte orders). Struct: declaration order; drop/rename = compile error.
- `json!` OK: tests; `Value`-typed (`Value::Json`) where untyped shape *is* value.

## Derives

- derive `Display`/`FromStr` → `parse-display`; rest → `derive_more`; `Default` → `SmartDefault`; `impl` only thiserror `Error` (`#[error]`/`#[from]`; nest `#[from(forward)]`/ctor).

## Validation (garde)

- `#[garde(custom(...))]` `fn(&T, &Context) -> garde::Result`; rules private; entry = constructor.

## Modules & lib.rs

- Thin `lib.rs`/`mod.rs`; path-expose (`bucket::Store`); `{Name, name}`; module fns; prod `Store::from_handle`.
- `schema/`: drop prefix; `Config`+`Version`. `database::Error`; `From` (`Io` unwraps, rest `Database`). `table_impl!` → `open`/`ensure` (`no_ensure`: `STATE`).

## Error

- Per-crate `Error`; private `mod error`; qualify (`storage::Error`). Wrap `Storage(#[from])`; redb `tinio_store_redb::error::Error` (display verbatim; fs `Redb`/mem `DatabaseError`); extras → `Io`. **The refused arm unwraps before the database arm** (`Refused(err) => Storage(err)`, fs `map_contract_error`, mem a manual `From<DatabaseError>`) — a `#[from]` hop would degrade `AlreadyExists`/`NoSuchBucket`/`NotEmpty` to `Io` (design 2026-10-03 D22). ctor `already_exists(name)`.

## Async traits

- `async-trait` + `async fn`. `Storage` aggregates `*Ops`; `Error: Into<storage::Error>`; methods use `<Self as Storage>::Error`; pinned stream aliases (`BodyStream`, …).

## ETag

- `Single([u8; 16])`/`Composed([u8; 16], u32)`; parse `ETag::new`; emit `Display`; `from_content`/`composed_from_parts`.

## Doc examples

- Public item: doc comment + runnable rustdoc example; exempt classes are a closed list — owner constitution III (`.specify/memory/constitution.md`).
- `no_run` waives the runnable rule only when a runnable example would have to mock more than ten required trait methods: at or below ten the double stays a readable illustration, above it the example is a reimplementation (the `Versions` case needs 69, ~90 lines, and `format_code_in_doc_comments` would then have to keep every line rustfmt-exact). A `no_run` example MUST still say in prose what it would have demonstrated, and the bound is not a licence — a small entry point has no such excuse: `plan_write`, a free function over `Intent`/`State`/`Current`/`Seqs` with no trait to mock, is the in-repo counter-example.

## Docs & scripts

- English bullets. Scripts → `/tmp`. Prose: `docs/compress.md`. Git: `docs/git.md`. Gherkin/e2e: `docs/tests.md`.
- Plans/specs/design: no hard wraps — single-line paras/items/blockquotes; break only on blank/table/fence/heading.
- CLAUDE.md = always-loaded pointer (name branches → doc); each `docs/*.md` = SoT for its branch. No restating in both.
