# tinio

[![CI](https://github.com/flier/tinio/actions/workflows/ci.yml/badge.svg)](https://github.com/flier/tinio/actions/workflows/ci.yml)

An S3-compatible object storage server in Rust. tinio hosts the [`s3s`](https://crates.io/crates/s3s) protocol layer over a pluggable storage contract: a filesystem backend and an in-memory reference backend, driven by real third-party clients (`aws` cli v2, `rclone`, `mc`, `boto3`) in CI.

## Layout

| Crate | Role |
|---|---|
| `tinio-core` | The `Storage` contract: buckets, objects, multipart, copy, tagging, checksums, pagination |
| `tinio-fs` | Filesystem backend — object files under `<root>/<bucket>/`, redb metadata, async pipelines |
| `tinio-mem` | In-memory reference backend (conformance oracle) |
| `tinio-server` | The s3s data plane: routing, SigV4 auth, XML, error mapping, metrics |
| `tinio-api` / `tinio-cli` / `tinio-config` | Facade passthroughs, CLI, TOML config |
| `tinio-e2e` | Cucumber BDD suites: in-process conformance, external-client interop |
| `tinio-util` | Shared test harness (`testing` feature) |

Workspace docs: [`docs/`](docs) — cargo conventions, code style, test guide.

## Quickstart

Run the `serve` example (defaults to the fs backend over one root directory, MinIO-convention credentials `minioadmin` / `minioadmin`):

```sh
cargo run -p tinio-server --example serve -- ./root --port 9000
```

Then point any S3 client at it:

```sh
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp file.bin s3://demo/
```

`serve` flags: `<root> [--port N] [--address HOST:PORT] [--config <config.toml>]`. The config toggles the `[s3]` capabilities (checksum, tagging, page-size caps, strippable feature groups).

## Testing

The canonical run commands (unit/doc, cucumber, external-client interop),
the full gate matrix, WSL2 verification notes, and the `TINIO_E2E_*` /
`TINIO_TEST_WAIT_TIMEOUT_SECS` knobs live in
[`docs/tests.md`](docs/tests.md) — its commands are the source of truth,
so the CI workflow and this README never restate them.
