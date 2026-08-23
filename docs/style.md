# Code Style

## Imports

- `use` at module top; nothing inline (`std::…` / crate paths) in signatures or bounds.
- `use` pairs with what is used: `garde::Validate` + `#[derive(Validate)]`; `derive_more::{Display, Deref, …}` on deriving structs; `std::error::Error` for the bound; `async_trait` for the contract.

## Types & defaults

- Untrusted input goes through checked constructors — `bucket::name`, `object::key`, `multipart::part_number`, `ETag::new`; never import `Name` / `Key` for raw input.
- Short names in-module (`Name`, `Key`, `Info`); qualified across modules (`bucket::Name`). Import the module, not the type.
- Derives: `derive_more` (`full`) for `Display`/`Deref`/`AsRef`/`Into`; `parse-display` for enum `Display`/`FromStr`.
- `From<&str>` / `From<String>`: trusted literals only (panic on invalid).
- Defaults: `SmartDefault` + `#[default = …]`; pair `#[serde(default)]` when serde needs it.

## Validation (garde)

- Custom validators: `fn(&T, &Context) -> garde::Result`; referenced by `#[garde(custom(...))]`.
- Key/bucket rules: private validators in `object` / `bucket`; the public entry is the checked constructor.

## Modules & lib.rs

- Top-level modules by concern — `bucket`, `object`, `etag`, `multipart`; `tinio-config`: `schema/`, `sources`, `error`.
- `lib.rs`: crate docs, `mod`, `pub use` — no logic.

## Error

- Naming & placement: `Error` per crate (`tinio_fs::Error`); qualified cross-crate (`tinio_config::Error`, `storage::Error`). Private `mod error`; re-export `Error` + extras (`ErrorBody`); `storage::Error` stays on the contract module.
- Usage: `use tinio_core::storage;` then `storage::Error` in signatures — bare `Error` collides. `Error::*` is globbed only in `error.rs` and tests; operation modules import the `crate::error` constructors.
- Payloads keep original types: `#[from]` `io::Error` / `etag::Error` / `ParseIntError`; entities `bucket::Name` / `object::Key`; ranges `{ range, size }`; paths `PathBuf`; rejected input `String`.
- Structure: backends wrap `storage::Error` (`Storage(#[from])`) + own variants (redb nests in `Database(DatabaseError)` — per-kind explicit `From`, never derived `#[from]`); `?` funnels through `From`; extras project to `Io`.
- Constructors: one `#[inline] pub(crate)` per variant (`no_such_bucket`, `database_storage`), one-line `///`, cloneable payloads take `&` and clone inside; call `already_exists(name)`, never the variant directly. The mapping is single-homed on `storage::Error`'s free-function constructors — backend wrappers only lift (`Error::Storage(storage::no_such_bucket(name))`).

## Async traits

- Storage/cleanup: `async-trait` + `async fn`.
- Contract: `BucketOps` / `ObjectOps` / `MultipartOps` aggregated by `Storage`; `Error: Into<storage::Error>`; category methods use `<Self as Storage>::Error`.
- `BodyStream` / `ActionStream` are the pinned stream aliases — no re-aliasing.

## ETag

- Stored as the raw 16-byte MD5: `Single([u8; 16])`, `Composed([u8; 16], u32)` for `-N`; `Deref`/`AsRef` expose the digest; `From<ETag> for Bytes` for wire bytes.
- Parse via `ETag::new` / hand-written `FromStr`; emit via `as_str` / `Display`. Content: `from_content`; multipart: `composed_from_parts`.

## Docs & scripts

- Docs: terse, exact, bullets, English only.
- Scripts: temp helpers in system temp (`/tmp`), not the repo.
