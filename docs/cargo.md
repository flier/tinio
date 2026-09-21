# Cargo Workspace

## Rules

- No comments in `Cargo.toml` — rationale in `docs/`.
- Pin once in root `[workspace.dependencies]`; crates use `X.workspace = true`.
- Members (`tinio-*`): `path = "crates/…"`, no version.
- `version`/`edition` from `[workspace.package]`.
- `publish = false` except facade `tinio`.
- `[lints.rust] unsafe_code = "forbid"` on every crate, **except a platform backend**, which is `"deny"` with its `#[allow(unsafe_code)]` sites enumerated in design decision 14 — the lint follows the implementation, and `forbid` cannot be overridden at all, so `deny` is what makes the exception expressible where the kernel interface has to be met on its own terms and nowhere else. The rule covers the three platform backends (`tinio-sandbox-linux`, `tinio-sandbox-macos`, `tinio-sandbox-windows`) and `tinio-api`; of these, `tinio-sandbox-linux` and `tinio-api` are `deny` today — `tinio-api`'s one site is `src/dacl.rs`, its single raw-Windows module (design decision 20's pattern) — while the two sandbox backends that have not landed their raw bindings stay `forbid` until their own plans flip them.
- `[lints.clippy] absolute_paths = "warn"` on every crate — 3+ segment paths go through `use` (`docs/style.md`).
- Optional/target deps at the crate.

## Versions

- `major.minor` only (`tokio = "1"`).

## Features

- No `features` in `[workspace.dependencies]`; `tokio-rustls` and `utoipa` are the one deliberate exception — their `default-features = false` and feature list sit at the workspace because a member can disable default features but cannot stop another member re-enabling them, and only the workspace-level `default-features = false` keeps `aws-lc-rs` (a C/CMake/NASM build) out of every crate's graph.
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

- `tinio-cli` defaults `mem` so `tinio server` without a directory uses the in-memory backend.
- Conformance `testing` feature, cucumber deps/targets/run: `docs/tests.md`.
