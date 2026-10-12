# Code Style

## Imports

- Top `use`, never inline; nest groups; 3+/2-seg: drop qualifier unless generic/prefix/collision.
- `clippy::absolute_paths`; glob `Error::*` only in `error.rs` / tests.
- `tinio_*` → `_`+rest via hidden `extern crate` in `lib.rs` (`#[cfg(feature)]`; test `#[cfg(test)]`).
- In-crate `crate::{_…, path}`; else crate path.
- Prefer `tokio::fs` except sync / `spawn_blocking` / no-async-API / `Metadata`.

## Types, JSON, derives

- Untrusted → ctor; qualify the type, import the module; `From<&str>` literals only.
- Defaults: `SmartDefault` / `#[serde(default)]`; prod JSON: `Serialize` struct + `to_string()`, never `json!`.
- Struct field order = declaration order; `json!` OK in tests and where a `Value` shape is the value.
- Derive: `Display`/`FromStr` → `parse-display`; rest → `derive_more`; `Default` → `SmartDefault`.
- Hand `impl` only thiserror `Error` and delegating wrappers; style attrs over per-variant format strings.
- Contract newtype as a statement parameter: backend traits behind an optional, backend-agnostic feature.
- Never default, never `impl` in the backend; hand-impl behind that feature; foreign PG type pin → `as _`.

## Validation & modules

- Garde: custom rule `fn(&T, &Context) -> garde::Result`; rules private; entry = constructor.
- Thin `lib.rs`/`mod.rs`; path-expose; `{Name, name}`; module fns.
- Schema: drop prefix; `Config`+`Version`; table open/ensure via the crate's macro.
- Production parses only the operator's file (`Config::load`) or launcher text, never a fabricated text; build the struct; tests exempt.
- ETag: `Single` / `Composed`; parse `ETag::new`; emit `Display`; content/composed constructors.

## Error & async

- Per-crate `Error`; private `mod error`; qualify. Wrap storage; store-redb display verbatim; extras → `Io`.
- Refused arm unwraps before the database arm (design D22).
- Error: reuse the fitting variant; else `Invariant(&'static str)`; detail to log/metrics only (D5).
- Fallback only where `Result` is impossible (`Display`, `Drop`, foreign trait): defined + `tracing` (D6).
- Async traits: `async-trait` + `async fn`; pinned stream aliases.
- Storage aggregates `*Ops`; `Error: Into<storage::Error>`; methods use `<Self as Storage>::Error`.

## Panics

- Business code returns, never panics: `.unwrap()` / `.expect()` / `panic!` / `assert!` family confined to tests, benches, doc examples.
- `unwrap_err` / `expect_err` / `unreachable!` / `todo!` / `unimplemented!` are in that family too (D1).
- `#[expect(<lint>, reason = "…")]`, prefix one of four (D2): `literal:` compile-time input; `infallible:` cannot fail by construction.
- `guarded:` invariant visible in the same expression; `argued:` unreachable, argument + catching mechanism in the reason.
- Runtime-failable never; eliminate over annotate; `#[expect(…, reason)]`, never `#[allow(…)]` on an outer attribute (D2).
- Lock: `.lock()` recovers — `.unwrap_or_else(PoisonError::into_inner)`; never unwrap, never propagate (D3).
- Out of rule: indexing/slicing, arithmetic overflow, `debug_assert!` (D4).

## Test-code exceptions

- Test code carries a stated `#[allow]`/`#[expect]`, one of two prefixes (D7):
- `test support:` a test-support module or non-test target (bench, `harness = false` binary, integration helpers).
- `test context:` a test-gated module or test fn.
- `#![allow(…)]` boundaries are outside `clippy::allow_attributes` — an `unsafe_code` module, a shared test module (D2, D7).
- Gate: eight lints warn per crate (CI `-D warnings`); assert family + reason prefixes are held by review (D7, D8).

## SQL

- Every `.sql` file: sqruff-formatted and lint-clean after edit (postgres, 80 cols, upper keywords/literals, lower idents); CI `sql` job.
- One-line statements inline in `query!` / `query_as!` / `query_scalar!`; only longer → `sql/<group>/<name>.sql`.
- Statement files (the `sql/<group>/<name>.sql` a macro names, not a migration) carry no comments; the reason lives at the Rust call site.
- Longer form via `query_file*!`; DDL, templated or macro-unexpressable array binds → `include_str!`.
- Migrations: `sqlx::migrate!` directory — no Rust list of versions.
- Returning columns decode into a row struct beside the call (field order = column order), never by position.

## SQL params & review

- Non-null / nullable via `AS "name!"` / `AS "name?"` when inference would lie.
- Array params by reference for `= ANY ($1)`.
- By review, not suite: every statement in a single-line inline macro; every `.sql` file named by Rust or a `migrate!` dir.
- Reformatting is whitespace/case only.
- SQL edit = code change. Regenerating the `.sqlx` cache: `docs/cargo.md` §sqlx cache.

## Docs & examples

- Public item: doc comment + runnable rustdoc example. Exempt classes: constitution III.
- `no_run` only when a runnable example would mock >10 trait methods; MUST say in prose what it would have shown.
- Small free functions have no such excuse.
- English bullets. Scripts go to the temp dir. Prose: `docs/compress.md`. Git: `docs/git.md`. Gherkin/e2e: `docs/tests.md`.
- `docs/*.md` prose lands compressed (≤140-char items, ≤7-item sections, no commands/paths/dates); check: `docs/compress.md`.
- Plans/specs/design: no hard wraps — break only on blank/table/fence/heading.
- CLAUDE.md = always-loaded pointer; each `docs/*.md` = SoT for its branch. No restating in both.
