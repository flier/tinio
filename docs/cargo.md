# Cargo Workspace

## Rules

- No comments in `Cargo.toml` — rationale in `docs/`.
- Pin once in root `[workspace.dependencies]`; crates use `X.workspace = true`.
- Members (`tinio-*`): `path = "crates/…"`, no version.
- `version`/`edition` from `[workspace.package]`.
- `publish = false` except facade `tinio`.
- `[lints.rust] unsafe_code = "forbid"` on every crate.
- Optional/target deps at the crate.

## Versions

- `major.minor` only (`tokio = "1"`).

## Features

- No `features` in `[workspace.dependencies]`.
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

[lints.rust]
unsafe_code = "forbid"
```

- `tinio-cli` defaults `mem` so `tinio server` without a directory uses the in-memory backend.
- Enable `testing` in `[dev-dependencies]` for the conformance harness.
