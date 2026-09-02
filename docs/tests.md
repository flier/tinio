# Tests

## Rust

- Async: `#[tokio::test]` / `async fn` — no `Runtime::block_on` / `rt(...)`. Sync: `#[test]`. Exception: deliberate runtime shape under test.
- Conformance: enable `tinio-util` `testing` in `[dev-dependencies]`. Cucumber never repeats its assertions.

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
- One module per S3 family (`buckets`, `objects`, `multipart`, `listing`, `errors`, `conditions`, `reserved_paths`, `metrics`, `tagging`, `clients`); register in `tests/steps/mod.rs`; shared `World`.
- Data-driven: `Examples` tables; one behavior per scenario.
- Tags: feature `@FR-xxx`/`@SC-xxx`/`@Txxx` (filter-inherited, hook-invisible); scenario config (`@fs`/`@mem`/`@nested-root`/`@checksum-on`/`@minimal-caps`/`@cold-listing`/`@max-buckets-3`) + external (`@interop`/`@aws`/`@rclone`/`@boto3`/`@mc`). One mapping: `config_from_tags`.
- Spec IDs: `cargo test -p tinio-e2e --test traceability`.

### Migration

- Migrate iff SC/FR/T spec semantic **and** S3-API observable. Unit tests without spec ID stay Rust; leave `tinio-util` harness untouched.
