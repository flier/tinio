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

## Management plane (tinio-api)

- `crates/tinio-api/tests/management.rs` = T056's matrix. Drives the plane the way the CLI will — `Plane::bind(options)` → `publish(port)` → `serve(stop)` → `client::status`/`stop` → `shutdown()` — never a transport directly. Nothing outside a test starts a plane in phase A, so this suite is the acceptance, not a companion to a binary.
- Legs: `/status` with and without the token; `/stop`'s `202` + body, the bounded drain, and `shutdown`'s removal of the socket file and `state`; `/metrics` open on the local channel and on a loopback bind, `401` on a non-loopback bind, `404` with no gauges attached; `/openapi.json` under the feature; the reader task's wiring (advances with the plane, stops with it, no scrape scans); the pipe's two sequential connections and its drain; unix stale-socket reclaim and mode 0600.
- One local `MetricsSource` double, two counting no-ops: `refresh` → `refreshes`, `counts` → `scans`. Nothing a real adapter computes crosses the boundary (the counts go into `tinio-server`'s statics), and the scan cannot run here — `Storage` is dyn-incompatible.
- Windows-gated: first-pipe-instance refusal (a second `Plane::bind` on the same name) + `dacl::pipe_acl_is_current_user_only` and `dacl::acl_is_current_user_only` reading both DACLs back. Unix-gated: state file and socket mode 0600, stale socket reclaimed by the bind.
- **Pipe dials and the successor window.** The serve loop creates an instance's successor only when the runtime polls it again, and `ClientOptions::open` does not wait for a free instance (`ERROR_PIPE_BUSY`, measured): a dial that follows a synchronous one with no await between meets it. Legs that dial twice back-to-back end through the channel instead — and the consequence is not only a test rule: `client::status` maps any dial failure to `running: false`, so `tinio status` prints "stopped" for a **running** server that hit the window, and `client::stop` returns an error. **T147** owns the fix (`specs/001-s3-local-server/tasks.md`); do not paper over it in a leg.
- Facade features: `crates/tinio/Cargo.toml` forwards `openapi`/`tls` to `tinio-api` and carries both in `default`; `cargo test -p tinio-api --no-default-features` is the leg that exercises the original graph.

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
