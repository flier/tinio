# Tests

## Rust

- Async: `#[tokio::test]` / `async fn` — no `Runtime::block_on` / `rt(...)`.
- Sync: `#[test]`. Exempt: runtime-shape tests.
- Conformance: `tinio-util` `testing` in `[dev-dependencies]`. Cucumber: no repeat asserts.

## Standard acceptance (pre-push gate)

Gate = local cmds for every ci.yml leg. `[profile.ci]` = `dev`; omit `--profile ci`.
CI unit = nextest (retries 2, `.config/nextest.toml`); local = `cargo test`; doctest = `--doc`.
Unix/windows-gnu: [Platform-gated verification](#platform-gated-verification-wsl2).

```
# compile graphs — the default graph cannot see feature-gate breakage
cargo check --workspace --all-targets
cargo hack check --package tinio --each-feature --no-dev-deps
cargo hack check --package tinio-server --each-feature --no-dev-deps

# lints
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# unit tests — the default and no-default graphs; doctests are separate runs
cargo test --workspace --exclude tinio-e2e
cargo test --doc --workspace --exclude tinio-e2e
cargo test --workspace --exclude tinio-e2e --no-default-features
cargo test --doc --workspace --exclude tinio-e2e --no-default-features

# legs no default graph reaches
cargo test -p tinio-select --features parquet
cargo test -p tinio-server --features select-parquet
cargo bench -p tinio-fs -p tinio-server -- --test

# docs
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# e2e — scoping/env rules in Cucumber below
cargo test -p tinio-e2e --test cucumber -- --retry 2
TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e --test cucumber -- --tags 'not @fs and not @parquet and not @interop and not @boto3 and not @mc' --retry 2
cargo test -p tinio-e2e --features parquet --test cucumber -- --tags '@parquet'
cargo test -p tinio-e2e --test traceability
```

`--features` = `test-parquet` Rust; features job = cargo-hack check (`--no-dev-deps`).

### CI-only legs

Need extra tools/baseline; out of gate:

| Leg (ci.yml job) | Command | Needs |
|---|---|---|
| `interop`, `interop-port` | `cargo build --profile ci -p tinio-server --example serve`, then `cargo test --profile ci -p tinio-e2e --test cucumber -- --tags '@interop' --retry 1` | aws cli v2 + rclone on `PATH` |
| `audit` | `cargo audit` | cargo-audit, net; parse Cargo.lock |
| `semver` | `cargo semver-checks check-release --package tinio` | cargo-semver-checks + crates.io (no-op pre-release) |

Constitution VI: `audit` (dev+main), `semver` (API MUST check).

### rustdoc

- Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- Intra-doc: `-D rustdoc::broken-intra-doc-links`, `-D rustdoc::private-intra-doc-links`.
- **`cargo test --doc` ≠ this gate**; green `cargo test` ≠ green docs.
- Public `//!`/`pub`: no private `[`foo`]`.
- `foo` `pub(crate)` → `private-intra-doc-links`. Write `` `foo` ``.
- `[0]`/`parts[0]` in prose → `broken-intra-doc-links`; `` `parts[0]` `` or `\[0\]`.
- Re-run doc gate after fix — not just `cargo test`.

## CI legs

- `.github/actions/run-test-leg` `run:` = unit/doc cmds; gate mirrors.
- `features`: `--each-feature` on facade + tinio-server. No powerset.
- Catch `use` out of `#[cfg]` / leftover path.
- `test-parquet`: parquet-only (`parquet`/`select-parquet` off default).
- `tests/parquet.rs` needs `required-features = ["parquet"]` or `pub mod parquet`/`[[test]]` break.
- `test-port` / `e2e-port` / `interop-port`: same cmds, windows/macos.

## Cucumber (tinio-e2e)

Layout: `tests/features/`, `tests/steps/`. Tags/FR-025/WSL2: `crates/tinio-e2e/README.md`.

### Cargo

- `cucumber` `"0.23"` once; `output-json`/`tracing` in `[dev-dependencies]` only (never lib).
- Targets: `[[test]] cucumber` (`harness = false`) + `traceability`.
- `--test cucumber` only; `traceability` rejects `--tags`/`--retry`.
- Bare `cargo test -p tinio-e2e` OK.
- Filter (tests/cucumber.rs): `@parquet` iff `--features parquet`.
- `@interop`/`@boto3`/`@mc` iff `TINIO_E2E_EXTERNAL=1`.
- `--tags` replaces whole filter — re-state exclusions.
- Env: `TINIO_E2E_BACKEND` (`mem`; `@fs`/`@mem` win), `TINIO_E2E_EXTERNAL=1`.
- Also: `TINIO_E2E_REPORT=<path>`, `TINIO_BOTO3_PYTHON`.

```
cargo test -p tinio-e2e                     # unscoped: the default filter applies
cargo test -p tinio-e2e --test cucumber -- --tags @interop --retry 1
```

Gate e2e = fs/mem/`@parquet`/traceability. `@boto3`/`@mc` need `TINIO_E2E_EXTERNAL=1`.

### Gherkin

- English; features = `specs/001-s3-local-server/contracts/s3-surface.md`.
- `Given`/`When`=act, `Then`=assert; 1st-person; `And`; unanchored `{int}`/`{string}`/`{word}`.
- S3 mods in `tests/steps/mod.rs`: `buckets`, `clients`, `common`, `conditions`, `errors`.
- Also: `listing`, `multipart`, `objects`, `reserved_paths`.
- `metrics`/`tagging` → `common`; shared `World`.
- Data-driven: `Examples`; one behavior/scenario.
- Feature tags: `@FR-xxx`/`@SC-xxx`/`@Txxx` (filter, no hook).
- Config: `@fs`/`@mem`/`@nested-root`/`@checksum-on`.
- Also: `@minimal-caps`/`@cold-listing`/`@max-buckets-3`/`@tagging-off`.
- Gated: `@parquet` + `--features parquet`. Map: `config_from_tags`.
- Ext: `@interop`/`@aws`/`@rclone`/`@boto3`/`@mc`.
- Spec IDs: `cargo test -p tinio-e2e --test traceability`.

### Migration

- Migrate iff spec+S3 observable. No ID → Rust. Keep `tinio-util`.

## Platform-gated verification (WSL2)

- `#[cfg(unix)]` (copy/ETag) invisible on Win; unused-import may be CI-load-bearing (E0599).
- WSL2 if Win-clean/CI-red (`/mnt/e/GitHub/tinio`, ext4 `CARGO_TARGET_DIR`).
- crates/tinio-e2e/README.md "WSL2":
  `cargo clippy --workspace --all-targets -- -D warnings`
- Hammer tests (`wait_for`, 60s, `TINIO_TEST_WAIT_TIMEOUT_SECS`) starve on loaded CI.
- Repro WSL2 before flaky-vs-real.
- `lint` `windows-gnu`: ubuntu cross, no native Win. `cargo clippy --target x86_64-pc-windows-gnu
  --workspace --all-targets -- -D warnings` (`rustup target add
  x86_64-pc-windows-gnu` + mingw-w64).
