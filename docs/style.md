# Code Style

## Imports

- `use` at module top, paired with usage (`garde::Validate`, `derive_more::{Display, Deref, …}`, `std::error::Error` bound, `async_trait`); nothing inline in signatures/bounds. `use super::server;` → `server::Config`, never inline.

## Types & defaults

- Untrusted input → checked constructors (`bucket::name`, `object::key`, `multipart::part_number`, `ETag::new`); never import `Name`/`Key` raw.
- In-module names short, cross-module qualified (`bucket::Name`); import module not type. Derives: `derive_more` (`full`) `Display`/`Deref`/`AsRef`/`Into`; `parse-display` enum `Display`/`FromStr`. `From<&str/String>`: trusted literals only (panic on invalid). Defaults: `SmartDefault` + `#[default = …]`, `#[serde(default)]` when needed.

## Validation (garde)

- Custom validators `fn(&T, &Context) -> garde::Result` via `#[garde(custom(...))]`; key/bucket rules private in `object`/`bucket`, public entry = checked constructor.

## Modules & lib.rs

- One module per concern; `lib.rs`/`mod.rs` thin (`mod`/`pub use`, primary type — no logic); tests/impls/helpers in sibling files. Expose via module path (`tinio_fs::bucket::Store`), not crate-root re-exports.
- Import module not type: `use crate::bucket;` → `bucket::Store`; bare `Store`/`Name` only in defining module; no prefixed names (`DatabaseError`, …). Re-export owned contract types from the concern module (`pub use tinio_core::bucket::{Name, name};`).
- Standalone stores: module-level `store(state_dir)` (not `Store::new`); constructors drop the prefix (`database::open`, `database::storage_error`); production shares one handle via `Store::from_handle` in `FsStorage`.
- Config `schema/`: one module per TOML section, prefix dropped (`log::Config`, `api::Http`); `pub mod` each, crate re-exports as `tinio_config::log`; no crate-root section re-exports (collide with `Config`); root = `tinio_config::Config` (+ `Version`).
- `database/`: fns return `database::Error` only; crate lifts via `From` (`Io` → `Error::Io`, rest incl. `UnsupportedVersion` nests under `Database`); per-kind `From`/constructors in `database/error.rs`; modules split by concern.
- `database/tables.rs`: all redb handles — private `TableDefinition` const + generic handle (`BucketsTable<'txn, T>(T, PhantomData)`); `table_impl!` generates `Deref`/`DerefMut`, `open`/`ensure`/`open_readonly` (`no_ensure` for `STATE`); domain methods on the specialization.

## Error

- `Error` per crate (`tinio_fs::Error`), qualified cross-crate (`storage::Error`); private `mod error`, re-export `Error` + `ErrorBody`; `storage::Error` on the contract module. Bare `Error` collides — glob `Error::*` only in `error.rs`/tests; op modules import `crate::error` constructors.
- Backends wrap `storage::Error` (`Storage(#[from])`) + own variants; redb nests `Database(database::Error)` — per-kind `From` derived (`thiserror #[from]`), struct variants (`UnsupportedVersion`, `CorruptMeta`) use constructor fns; `?` funnels via `From`; extras → `Io`. Payloads keep original types (`#[from] io::Error`, entities `bucket::Name`, `{range, size}`, `PathBuf`, `String`).
- Constructors: one `#[inline] pub(crate)` per variant (`no_such_bucket`, `database::storage_error`), one-line `///`, cloneable payloads take `&`, clone inside; call `already_exists(name)`, never the variant; crate wrappers only lift.

## Async traits

- Storage/cleanup: `async-trait` + `async fn`.
- Contract: `BucketOps`/`ObjectOps`/`MultipartOps` aggregated by `Storage`; `Error: Into<storage::Error>`; category methods use `<Self as Storage>::Error`; `BodyStream`/`ActionStream` pinned aliases — no re-aliasing.

## ETag

- Raw 16-byte MD5: `Single([u8; 16])`, `Composed([u8; 16], u32)` for `-N`; `Deref`/`AsRef` expose digest; `From<ETag> for Bytes` wire. Parse `ETag::new`/hand-written `FromStr`; emit `as_str`/`Display`; `from_content`; `composed_from_parts`.

## Docs & scripts

- Docs: terse, exact, bullets, English only. Scripts: temp helpers in `/tmp`, not the repo.
