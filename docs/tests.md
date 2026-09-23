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
cargo build -p tinio                            # the @cli scenarios spawn the binary
cargo test -p tinio-e2e --test cucumber -- --retry 2
TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e --test cucumber -- --tags 'not @fs and not @unix and not @windows and not @parquet and not @interop and not @boto3 and not @mc' --retry 2
cargo test -p tinio-e2e --features parquet --test cucumber -- --tags '@parquet'
cargo test -p tinio-e2e --test traceability
```

`--features` = `test-parquet` Rust; features job = cargo-hack check (`--no-dev-deps`).

**Otel alignment (fixed 2026-09-23)**: the `opentelemetry`/`tracing-opentelemetry` mismatch recorded here on 2026-09-20 is gone — the family is pinned together (`opentelemetry*` 0.32 with `tracing-opentelemetry` 0.33, whose own manifest requires it), so `cargo hack check --each-feature --no-dev-deps` passes for `tinio` and `tinio-server`. (`--each-feature --all-features` is not a cargo-hack combination.)

### CI-only legs

Out of gate:

| Leg (ci.yml job) | Command | Needs |
|---|---|---|
| `interop`, `interop-port` | `cargo build --profile ci -p tinio`, then `cargo test --profile ci -p tinio-e2e --test cucumber -- --tags '@interop' --retry 1` | aws cli v2 + rclone on `PATH` |
| `audit` | `cargo audit` | cargo-audit, net; parse Cargo.lock |
| `semver` | `cargo semver-checks check-release --package tinio` | cargo-semver-checks + crates.io (no-op pre-release) |

Constitution VI: `audit` (dev+main), `semver` (API MUST check).

### rustdoc

- Gate: `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- Intra-doc: `-D rustdoc::broken-intra-doc-links`, `-D rustdoc::private-intra-doc-links`.
- **`cargo test --doc` ≠ this gate**.
- Public docs: no private `[`foo`]`; `pub(crate)` → `` `foo` ``; prose `[0]` → `` `parts[0]` `` or `\[0\]`.
- Re-run doc gate after fix.

## CI legs

- `.github/actions/run-test-leg` mirrors gate unit/doc cmds.
- `features`: `--each-feature` on facade + tinio-server (no powerset). Catch cfg/`use` drift.
- `test-parquet`: parquet-only; `tests/parquet.rs` needs `required-features = ["parquet"]`.
- `test-port` / `e2e-port` / `interop-port`: same cmds, windows/macos.

## Cucumber (tinio-e2e)

Layout: `tests/features/`, `tests/steps/`. Tags/FR-025/WSL2: `crates/tinio-e2e/README.md`.

### Cargo

- `cucumber` `"0.23"` once; `output-json`/`tracing` in `[dev-dependencies]` only.
- Targets: `[[test]] cucumber` (`harness = false`) + `traceability` (rejects `--tags`/`--retry`).
- Filter: `@parquet` iff `--features parquet`; `@unix`/`@windows` skip off-platform (else `fail_on_skipped` fails); `@interop`/`@boto3`/`@mc` iff `TINIO_E2E_EXTERNAL=1`.
- `--tags` replaces filter — re-state every exclusion incl. `not @unix and not @windows`.
- Env: `TINIO_E2E_BACKEND`, `TINIO_E2E_EXTERNAL=1`, `TINIO_E2E_REPORT`, `TINIO_BOTO3_PYTHON`.

```
cargo test -p tinio-e2e                     # unscoped: the default filter applies
cargo test -p tinio-e2e --test cucumber -- --tags @interop --retry 1
```

Gate e2e = fs/mem/`@parquet`/traceability. `@boto3`/`@mc` need `TINIO_E2E_EXTERNAL=1`.

### Gherkin

- English; features = `specs/001-s3-local-server/contracts/s3-surface.md`.
- `Given`/`When`=act, `Then`=assert; 1st-person; unanchored `{int}`/`{string}`/`{word}`.
- Steps in `tests/steps/mod.rs`; `metrics`/`tagging` → `common`; shared `World`.
- One behavior/scenario; `Examples` for data-driven.
- Tags: `@FR-xxx`/`@SC-xxx`/`@Txxx`; config `@fs`/`@mem`/…; gated `@parquet`; platform `@unix`/`@windows`; CLI `@cli` (spawn own binary — hook skips); ext `@interop`/`@aws`/`@rclone`/`@boto3`/`@mc`. Map: `config_from_tags`.
- Spec IDs: `cargo test -p tinio-e2e --test traceability`.

### Migration

- Migrate iff spec+S3 observable. No ID → Rust. Keep `tinio-util`.

## Management plane (tinio-api)

- `crates/tinio-api/tests/management.rs` = T056. Drive via `Plane` API, never raw transport. Plane acceptance; CLI process legs → `cli_lifecycle.feature` (`@cli`).
- Cover status/stop/metrics/openapi + pipe/unix ACL + reader lifecycle. Local `MetricsSource` double only.
- **Pipe successor window** (`ERROR_PIPE_BUSY`): do not paper over — **T147** owns. Dial fail → false "stopped".
- Facade forwards `openapi`/`tls` in `default`; `--no-default-features` = original graph.

## CLI (tinio)

- Process legs = cucumber `@cli` (`cli_lifecycle.feature` / `steps/cli.rs`); need `cargo build -p tinio` first. Assert readiness, FR-016 L2/L3, daemon, notify, status/stop/state-dir, sandbox refuse.
- Stays Rust — `crates/tinio/tests/lifecycle.rs`: Windows console leg only (unsafe). Assert drain, not exit code.
- Doctor (T057) — `crates/tinio/tests/doctor.rs`, `cargo test -p tinio --test doctor`:
  - store-less root → 0, meta + private-state rows `ok`, no `meta.redb` created (plain and `--dry-run`).
  - stale `state` file (`Stopped`) and stale unix socket → warn; `--fix` reclaims both.
  - stale-socket reclaim follows recorded `control_name`, never re-derived `socket_path` (`[api.unix] path` shape).
  - `NoStateFile` still probes derived socket name; `Unreadable` → warn `not inspected`, `--fix` clears.
  - live server → state row `ok`; `--fix` refuses (error finding, parseable `--json`).
  - orphaned `OBJECT_META` row, bucket-orphaned multipart subtree, stale `BUCKETS` record → information plain, finding `--dry-run`, gone after `--fix`, `object.txt` intact.
  - `meta.redb` integrity + fragmentation; `compact_needed` → warn, `--fix` compacts; unopenable store → error finding with the report intact.
  - `--json` on each leg = `contracts/doctor-report.schema.json` key sets; `state` token in neither stream.
  - `--read-only` diagnoses the relocated state dir; `--fix` on a store-less root creates the store; `--fix --read-only` repairs the relocated dir and leaves the root alone.
  - `--fix` reports `home root-state dirs … not repaired … T144`; a plain run does not.
  - `tokio` dev-dep + `#[tokio::test]`: the store-seeding legs drive async `tinio_fs` store constructors; `Runtime::block_on` is banned.
- Elsewhere: startup repair; service pure halves; `serve_read_only.rs`.

## Service verbs (manual)

- Needs supervisor + elevation — no auto leg. Pure halves: `tinio-cli/tests/service.rs`. Below = verification gap.
- Linux `--user` / system: install → notify unit + state `WorkingDirectory` + query; stop → stopped; elevated also `is-enabled` + journal `READY`/`STOPPING`. Linger for reboot. No elevation → remedy, never raw `EACCES`.
- macOS: `install` does **not** start (`Disabled`); `start` → running; assert via `launchctl print`.
- Windows elevated: RUNNING + path; stop = drain not kill; `state`/socket gone. Not elevated → Administrator remedy (≠ missing-service 1060); `--user` = usage exit 2.
- Both: uninstall → not installed; second uninstall = backend error.

## Platform-gated verification (WSL2)

- `#[cfg(unix)]` invisible on Win; unused-import may be CI-load-bearing (E0599).
- WSL2 if Win-clean/CI-red (`/mnt/e/GitHub/tinio`, ext4 `CARGO_TARGET_DIR`).
- crates/tinio-e2e/README.md "WSL2":
  `cargo clippy --workspace --all-targets -- -D warnings`
- Hammer tests (`wait_for`, 60s, `TINIO_TEST_WAIT_TIMEOUT_SECS`) starve on loaded CI — repro WSL2 before flaky-vs-real.
- `lint` `windows-gnu`: ubuntu cross. `cargo clippy --target x86_64-pc-windows-gnu
  --workspace --all-targets -- -D warnings` (`rustup target add
  x86_64-pc-windows-gnu` + mingw-w64).
