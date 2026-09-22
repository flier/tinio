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
| `tinio-sandbox` / `-core` / `-linux` / `-macos` / `-windows` | Process confinement: the `Backend` contract, per-platform implementations, and a facade |
| `tinio-api` | The management plane: the `state` file and single-instance bind, the local channel / named pipe / TCP transports, `/status`, `/stop`, `/metrics`, `/openapi.json` |
| `tinio-cli` | The CLI: `server`/`start`, `status`, `stop`, `doctor`, and the `service` verbs that register the server with systemd, launchd or the SCM |
| `tinio-config` | TOML configuration schema, discovery and auto-create, credentials, `.env` and the environment chain |
| `tinio-e2e` | Cucumber BDD suites: in-process conformance, external-client interop |
| `tinio-util` | Shared test harness (`testing` feature) |

Workspace docs: [`docs/`](docs) — cargo conventions, code style, test guide.

## Quickstart

Start the server over a directory (the fs backend; the config is auto-created under `<root>/.tinio/` on first start with a generated credential pair):

```sh
cargo run -p tinio -- server ./root --port 9000
```

Then point any S3 client at it, reading the generated pair out of `<root>/.tinio/.tinio.toml`:

```sh
export AWS_ACCESS_KEY_ID=<access key> AWS_SECRET_ACCESS_KEY=<secret key>
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp file.bin s3://demo/
```

Lifecycle: `tinio status ./root`, `tinio stop ./root`, and `tinio service install ./root` to register the server with the platform's supervisor. Run `tinio --help` for the full surface; `[s3]`-section config toggles the capabilities (checksum, tagging, page-size caps, strippable feature groups).

## Testing

The canonical run commands (unit/doc, cucumber, external-client interop),
the full gate matrix, WSL2 verification notes, and the `TINIO_E2E_*` /
`TINIO_TEST_WAIT_TIMEOUT_SECS` knobs live in
[`docs/tests.md`](docs/tests.md) — its commands are the source of truth,
so the CI workflow and this README never restate them.
