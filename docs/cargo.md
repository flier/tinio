# Cargo Workspace

## Rules

- No comments in `Cargo.toml` — rationale in `docs/`.
- Pin once in root `[workspace.dependencies]`; crates use `X.workspace = true`.
- A patched dependency lives under `vendor/`, keeps the upstream version it patches, and carries a source header naming the change; the root names it in `[patch.crates-io]` and `exclude` (`docs/superpowers/specs/2026-09-18-sandbox-design.md`).
- Members (`tinio-*`): `path = "crates/…"`, no version.
- `version`/`edition` from `[workspace.package]`.
- `publish = false` except facade `tinio`.
- `[lints.rust] unsafe_code = "forbid"` by default; `"deny"` + enumerated `#[allow(unsafe_code)]` only where raw platform API forces it — the sandbox backends go `"deny"` **as each lands**, sites enumerated in the sandbox design's decision 14 (`docs/superpowers/specs/2026-09-18-sandbox-design.md`); a stub with no site yet stays `forbid`. Reasons live here, not in manifests.
- `[lints.clippy] absolute_paths = "warn"` on every crate — 3+ segment paths via `use` (`docs/style.md`).
- Optional/target deps at the crate.

## Versions

- `major.minor` only (`tokio = "1"`).

## Features

- No `features` in `[workspace.dependencies]`; exception: `tokio-rustls` / `utoipa` pin `default-features = false` at workspace so members cannot re-enable heavy native crypto into the graph; `clap` pins it so its default set (`color` included) is not inherited and each member lists what it actually uses (`tinio-cli`: `derive`/`env` plus the help and error renderer; `tinio-server`'s example: the refusal renderer only).
- Enable on the crate that uses them (`serde = { workspace = true, features = ["derive"] }`).

## Groups

- Order: `[workspace.dependencies]`, `[dependencies]`, `[dev-dependencies]`, `[target.'cfg(...)'.dependencies]` — external, blank line, `tinio-*`. Alpha within each group. No blank line when one group.

## Example

Root:

```toml
[workspace.package]
version = "0.1.0"
edition = "2024"

[workspace.dependencies]
serde = "1"
tokio = "1"

tinio-core = { path = "crates/tinio-core" }
tinio-mem = { path = "crates/tinio-mem" }
```

Crate:

```toml
[package]
name = "tinio-mem"
version.workspace = true
edition.workspace = true
publish = false

[dependencies]
serde = { workspace = true, features = ["derive"] }
utoipa = { workspace = true, optional = true, features = ["axum_extras"] }

tinio-core.workspace = true
tinio-mem = { workspace = true, optional = true }

[features]
default = ["mem"]
mem = ["dep:tinio-mem"]

[dev-dependencies]
tinio-util = { workspace = true, features = ["testing"] }

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "forbid"
```

- `tinio-cli` defaults `mem`, but nothing selects in-memory backend — no `DIR` discovers `.tinio/` or usage-errors (`contracts/cli.md:17`).
- Conformance `testing` feature, cucumber deps/targets/run: `docs/tests.md`.
