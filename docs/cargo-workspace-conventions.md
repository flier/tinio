# Cargo Workspace Conventions

Dependency management rules for the tinio workspace. Referenced from `CLAUDE.md`.

## Rules

- Declare every dependency version once, in the workspace root `Cargo.toml` under `[workspace.dependencies]`.
- Crates reference the shared declaration with `X.workspace = true`.
- Optional and target-gated usage is declared at the crate: `optional = true` or a `[target.'cfg(...)'.dependencies]` section. Features may be declared at the workspace (applies to every user) or added at the usage site.
- Local `tinio-*` path dependencies are the only exception — never versioned.
- Keep the workspace dependency list sorted alphabetically.
- Versions are `major.minor` only — never patch (e.g. `s3s = "0.14"`, `tokio = "1.53"`).

## Examples

Workspace root `Cargo.toml`:

```toml
[workspace.dependencies]
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.53", features = ["full"] }
utoipa = "5.5"
```

Crate `Cargo.toml`:

```toml
[dependencies]
serde.workspace = true

# optional usage at the crate
utoipa = { workspace = true, optional = true, features = ["axum_extras"] }

[target.'cfg(windows)'.dependencies]
windows-sys = { workspace = true, features = ["Win32_System_Console"] }
```
