# Code Style

## Imports

- Top `use`; never inline. `super::server` → `server::Config`. Nest `foo::{Bar, dto::{self, Baz}}`.
- 3+ (`a::b::c`): `use` then short form — code, tests, benches, docs. `Type::item`: `use` type then `Type::item` (not item, not module-qualify). Collide → alias (`IoError`); glob `Error::*` only `error.rs`/tests.
- Workspace `tinio_*` → `_`+rest: `lib.rs` `#[doc(hidden)] pub extern crate tinio_core as _core;` (`#[cfg(feature)]`; test `#[cfg(test)] extern crate`). In-crate: `crate::{_core::{...}, path}`; else `tinio_fs::`.
- `tokio::fs` over `std::fs` (sync / `spawn_blocking` / no-async-API / re-exports `Metadata` stay `std::fs`).

## Types & defaults

- Untrusted → ctor (`bucket::name`, …). Qualify (`bucket::Name`); import module. `derive_more` (`full`); `parse-display` enums. `From<&str>`: literals (panic). `SmartDefault`/`#[serde(default)]`.

## Validation (garde)

- `#[garde(custom(...))]` `fn(&T, &Context) -> garde::Result`; rules private; entry = constructor.

## Modules & lib.rs

- Thin `lib.rs`/`mod.rs`; path-expose (`bucket::Store`); `{Name, name}`; module fns; prod `Store::from_handle`.
- `schema/`: drop prefix; `Config`+`Version`. `database::Error`; `From` (`Io` unwraps, rest `Database`). `table_impl!` → `open`/`ensure` (`no_ensure`: `STATE`).

## Error

- Per-crate `Error`; private `mod error`; qualify (`storage::Error`). Wrap `Storage(#[from])`; redb `Database`; extras → `Io`. One `#[inline] pub(crate)` ctor/variant (`already_exists(name)`).

## Async traits

- `async-trait` + `async fn`. `Storage` aggregates `*Ops`; `Error: Into<storage::Error>`; methods use `<Self as Storage>::Error`; pinned stream aliases (`BodyStream`, …).

## ETag

- `Single([u8; 16])`/`Composed([u8; 16], u32)`; parse `ETag::new`; emit `Display`; `from_content`/`composed_from_parts`.

## Docs & scripts

- English bullets. Scripts → `/tmp`.
- Compress hard: no articles/filler/hedging; fragments; `condition (ex): action — scope`. 1 ex/pattern (extra = distinct branch). Merge overlap; positive > ban. Cut till drop changes behavior. Inline code/paths/commands exact.
- CLAUDE.md = always-loaded pointer (name branches, then this file); this file = source of truth. No restating a rule in both.
