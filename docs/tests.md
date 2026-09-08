# Tests

## Rust

- Async: `#[tokio::test]` / `async fn` — no `Runtime::block_on` / `rt(...)`. Sync: `#[test]`. Exception: deliberate runtime shape under test.
- Conformance: enable `tinio-util` `testing` in `[dev-dependencies]`. Cucumber never repeats its assertions.

## CI legs

- `.github/actions/run-test-leg` — its `run:` lines are the exact unit/doc commands (modes default / no-default / doc): nextest `--cargo-profile ci --profile ci` (`.config/nextest.toml` = runner profile: fail-fast false, retries 2), doctests via `cargo test --doc --profile ci` (nextest can't run them), rustdoc `-D warnings`.
- Local equivalents keep `cargo test` (canonical above; cucumber sections) — the CI nextest profile is for the runners.

## Standard acceptance (pre-push gate)

Mirror the CI legs locally before push; every leg must be green. The unix-only legs (the `#[cfg(unix)]` clippy/tests) run under WSL2 — see
[Platform-gated verification](#platform-gated-verification-wsl2).

```
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

### rustdoc

- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` is the doc gate; `-D rustdoc::broken-intra-doc-links` and `-D rustdoc::private-intra-doc-links` are on (CI's rustdoc leg). **`cargo test --doc` runs doctests but is not this gate** — a green `cargo test` does not guarantee green docs, so the doc build is a separate acceptance leg.
- Public docs (crate/module `//!` or a `pub` item) must not link a private item: `[`foo`]` where `foo` is `pub(crate)`/private → `private-intra-doc-links`. Link a public item only, or write the name as a plain code span `` `foo` `` (drop the brackets).
- A bare `[0]`/`parts[0]` (array or field index) in prose is parsed as a link → `broken-intra-doc-links`; wrap it in backticks (`` `parts[0]` ``) or escape `\[0\]`.
- Re-verify by re-running the doc gate after fixing — not just `cargo test`.

## Cucumber (tinio-e2e)

Layout: `tests/features/`, `tests/steps/`. Tag taxonomy / FR-025 / WSL2: `crates/tinio-e2e/README.md`.

### Cargo

- `cucumber` pinned once workspace (`"0.23"`, no features); tinio-e2e enables `output-json`/`tracing` in `[dev-dependencies]` only — never a lib dep.
- Targets: `[[test]] cucumber` (`harness = false`) + plain-harness `traceability`.
- Scoping: cucumber args → `--test cucumber` (`traceability` rejects `--tags`/`--retry`). No-arg `cargo test -p tinio-e2e` fine unscoped.
- Default filter excludes `@external` (`not @interop and not @boto3 and not @mc`); explicit `--tags` replaces it — re-state exclusion.
- Env: `TINIO_E2E_BACKEND` (`mem` CI; `@fs`/`@mem` tags win), `TINIO_E2E_EXTERNAL=1`, `TINIO_E2E_REPORT=<path>` (bare name → package root), `TINIO_BOTO3_PYTHON`.

```
cargo test -p tinio-e2e
cargo test -p tinio-e2e --test cucumber -- --tags @interop --retry 1
cargo test -p tinio-e2e --test cucumber -- --tags 'not @fs and not @interop and not @boto3 and not @mc'   # CI mem
cargo test -p tinio-e2e --test traceability
```

### Gherkin

- English; features = executable `specs/001-s3-local-server/contracts/s3-surface.md`.
- Steps: `Given`/`When` = actions, `Then` = assertions; first-person verbs; `And` chains; unanchored regex; `{int}`/`{string}`/`{word}`.
- One module per S3 family (`buckets`, `clients`, `common`, `conditions`, `errors`, `listing`, `multipart`, `objects`, `reserved_paths` — declared in `tests/steps/mod.rs`; the `metrics`/`tagging` scenarios ride the generic `common` request steps, no dedicated module); shared `World`.
- Data-driven: `Examples` tables; one behavior per scenario.
- Tags: feature `@FR-xxx`/`@SC-xxx`/`@Txxx` (filter-inherited, hook-invisible); scenario config (`@fs`/`@mem`/`@nested-root`/`@checksum-on`/`@minimal-caps`/`@cold-listing`/`@max-buckets-3`/`@tagging-off`) + external (`@interop`/`@aws`/`@rclone`/`@boto3`/`@mc`). One mapping: `config_from_tags`.
- Spec IDs: `cargo test -p tinio-e2e --test traceability`.

### Migration

- Migrate iff SC/FR/T spec semantic **and** S3-API observable. Unit tests without spec ID stay Rust; leave `tinio-util` harness untouched.

## Platform-gated verification (WSL2)

- `#[cfg(unix)]` tests (fs copy fast-path, multipart-ETag sources) are
  **invisible to the Windows toolchain** — an import Windows clippy reports
  as unused can still be load-bearing on the CI ubuntu/macos legs (E0599
  class: trait methods unresolved). Verify gate runs in WSL2 before the
  next push when a Windows-clean run goes red only upstream (repo at
  `/mnt/e/GitHub/tinio`, ext4 `CARGO_TARGET_DIR` — crates/tinio-e2e/README.md "WSL2"):
  `cargo clippy --workspace --all-targets -- -D warnings`
- Timing-sensitive unit tests (bucket delete/create/put hammer polls
  `wait_for`, tinio-util — 60 s default timeout, `TINIO_TEST_WAIT_TIMEOUT_SECS`
  overrides) can starve on loaded CI runners; reproduce under WSL2
  before classifying flaky vs real.
