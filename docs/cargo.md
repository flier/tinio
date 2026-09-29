# Cargo Workspace

## Rules

- No comments in `Cargo.toml` — rationale in `docs/`.
- Pin once in root `[workspace.dependencies]`; crates use `X.workspace = true`.
- A patched dependency lives under `vendor/`, keeps the upstream version it patches, and carries a source header naming the change; the root names it in `[patch.crates-io]` and `exclude` (`docs/superpowers/specs/2026-09-18-sandbox-design.md`).
- Members (`tinio-*`): `path = "crates/…"`, no version.
- `version`/`edition` from `[workspace.package]`.
- `publish = false` except facade `tinio`.
- `[lints.rust] unsafe_code = "forbid"` by default; `"deny"` + enumerated `#[allow(unsafe_code)]` only where raw platform API forces it — the sandbox backends go `"deny"` **as each lands**, sites enumerated in the sandbox design's decision 14 (`docs/superpowers/specs/2026-09-18-sandbox-design.md`); a stub with no site yet stays `forbid`. **The Windows backend's newest site, and the reason it is one, is `sweep`** (2026-09-28): the launch path's reaper of the AppContainer profiles an unclean exit leaves behind (design decision 21) — a Toolhelp process walk for its liveness gate, and the registry listing and deletion for the `Storage` half — under the same file-level `#[allow(unsafe_code)]` as that crate's other six modules, enumerated with them in decision 14. **One crate outside that feature is `deny` for a reason of its own, and this is where the reason lives**: `tinio-util` takes `deny` for two modules. `job_object` is the Win32 job-object call sequence itself — `CreateJobObjectW`, `SetInformationJobObject`, `AssignProcessToJobObject`, `IsProcessInJob`, `QueryInformationJobObject`, `TerminateJobObject`, `CloseHandle`, and a `Send`/`Sync` wrapper on the handle — **and it is the one part of that crate a production build compiles**, because the per-launch job `tinio-sandbox-windows` creates is created by the launcher's own build: the module is `#[cfg(windows)]` and not feature-gated, and its `windows-sys` edge under `[target.'cfg(windows)'.dependencies]` is a plain target dependency rather than one `testing` turns on (2026-09-29; the sandbox crate's `job.rs` is expressed on that primitive since and lost its own allow with the calls). `job` is still the test harnesses' spawn guard, and it keeps the reason it had: `#[cfg(any(test, feature = "testing"))]`, so no production build compiles it, with the two calls that are its own — `GetCurrentProcess`, which adopts the test process, and, on Linux, the `#[cfg(target_os = "linux")]` `pre_exec` registration that arms the child with `PR_SET_PDEATHSIG` (`Command::pre_exec` is the `unsafe` call; the closure inside it runs `rustix`'s safe `set_parent_process_death_signal`) — and the only optional edge it needs left is `rustix` (feature `process`) under `[target.'cfg(target_os = "linux")'.dependencies]`, reached through `testing`. Both modules' allows are enumerated with the other sites in decision 14. Reasons live here, not in manifests.
- `[lints.clippy] absolute_paths = "warn"` on every crate — 3+ segment paths via `use` (`docs/style.md`).
- Optional/target deps at the crate.

## Versions

- `major.minor` only (`tokio = "1"`).

## Features

- No `features` in `[workspace.dependencies]`; exception: `tokio-rustls` / `utoipa` pin `default-features = false` at workspace so members cannot re-enable heavy native crypto into the graph; `clap` pins it so its default set (`color` included) is not inherited and each member lists what it actually uses (`tinio-cli`: `derive`/`env` plus the help and error renderer; `tinio-server`'s example: the refusal renderer only).
- Enable on the crate that uses them (`serde = { workspace = true, features = ["derive"] }`) — and that is where a member's extra features belong, the exception above included: the workspace line names the pin and `default-features = false`, the member's own line names the set (`tokio-rustls` / `utoipa` moved there out of `[workspace.dependencies]` on 2026-09-28, deliberately, since only `tinio-api` asks for them).

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
