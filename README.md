# tinio

[![CI](https://github.com/flier/tinio/actions/workflows/ci.yml/badge.svg)](https://github.com/flier/tinio/actions/workflows/ci.yml)

An S3-compatible object storage server in Rust. tinio hosts the [`s3s`](https://crates.io/crates/s3s) protocol layer over a pluggable storage contract: filesystem, in-memory and PostgreSQL backends, driven by real third-party clients (`aws` cli v2, `rclone`, `mc`, `boto3`) in CI.

## Layout

Every crate carries its own README; those are the detail, this is the index.

**Contracts and shared layers**

| Crate | Role |
|---|---|
| [`tinio-core`](crates/tinio-core/README.md) | The `Storage` contract: domain errors, the trait, the four persistence contracts, key validation |
| [`tinio-version`](crates/tinio-version/README.md) | The shared version state machine: supersede, materialize, expire |
| [`tinio-ops`](crates/tinio-ops/README.md) | The S3 operation bodies the backends share, written once over the persistence contracts |
| [`tinio-lifecycle`](crates/tinio-lifecycle/README.md) | The S3 object lifecycle pass: the stateless background sweep over the storage contract |
| [`tinio-blob`](crates/tinio-blob/README.md) | Content-addressed block engine: blobs by SHA-256, deduplicated, swept by live set |
| [`tinio-store-redb`](crates/tinio-store-redb/README.md) | The shared redb table layer and the state-database driver |
| [`tinio-store-pg`](crates/tinio-store-pg/README.md) | The four persistence contracts over PostgreSQL: migrations, the driver pool, the content and block planes |
| [`tinio-util`](crates/tinio-util/README.md) | Shared runtime utilities: the evicting per-key lock map, commit primitives, path/process/secret helpers, the state-directory resolver and the `TINIO_*` names, and the Windows wide-string writer |

**Backends**

| Crate | Role |
|---|---|
| [`tinio-fs`](crates/tinio-fs/README.md) | Filesystem backend — object files under `<root>/<bucket>/`, redb metadata, async pipelines |
| [`tinio-mem`](crates/tinio-mem/README.md) | In-memory reference backend — the conformance oracle |
| [`tinio-pg`](crates/tinio-pg/README.md) | The servable PostgreSQL backend: `PgStorage` and `PgCleanup` over the ops engine and `tinio-store-pg` |

**Serving, auth and the control plane**

| Crate | Role |
|---|---|
| [`tinio-server`](crates/tinio-server/README.md) | The s3s data plane: routing, XML, error mapping, metrics, and the `[s3]` capability switches it enforces |
| [`tinio-auth`](crates/tinio-auth/README.md) | The authn/authz engine: the secret lookup and the pre-route authorization pipeline |
| [`tinio-select`](crates/tinio-select/README.md) | The streaming SQL SELECT engine for S3 objects (S3 Select) |
| [`tinio-api`](crates/tinio-api/README.md) | The management plane: the `state` file and single-instance bind, the local channel / named pipe / TCP transports, `/status`, `/stop`, `/metrics`, `/openapi.json` |
| [`tinio-cli`](crates/tinio-cli/README.md) | The CLI: `server`/`start`, `status`, `stop`, `doctor`, the `service` verbs that register the server with systemd, launchd or the SCM, and the configuration composition root |
| [`tinio`](crates/tinio/README.md) | The facade: the `tinio` binary and the re-export library |

**Confinement**

| Crate | Role |
|---|---|
| [`tinio-sandbox`](crates/tinio-sandbox/README.md) | The facade over the per-platform backends — the one name a consumer writes |
| [`tinio-sandbox-core`](crates/tinio-sandbox-core/README.md) | The `Backend` contract, its vocabulary, and the launcher machinery |
| [`tinio-sandbox-linux`](crates/tinio-sandbox-linux/README.md) | Linux mount-namespace confinement |
| [`tinio-sandbox-macos`](crates/tinio-sandbox-macos/README.md) | macOS Seatbelt (SBPL) confinement |
| [`tinio-sandbox-windows`](crates/tinio-sandbox-windows/README.md) | Windows AppContainer confinement |

**Tests, benches and tooling**

| Crate | Role |
|---|---|
| [`tinio-e2e`](crates/tinio-e2e/README.md) | Cucumber BDD suites: in-process conformance, external-client interop |
| [`tinio-bench`](crates/tinio-bench/README.md) | The measurement harness behind the workspace's A/B bench protocol |
| `xtask` | The workspace's own cargo verbs (`sqlx`, `bench-ab`) |

Workspace docs: [`docs/`](docs) — cargo conventions, code style, test guide, [`--sandbox` operator notes](docs/sandbox.md).

## Quickstart

Start the server over a directory (the fs backend; the config is auto-created under `<root>/.tinio/` on first start with a generated credential pair):

```sh
cargo run -p tinio -- server ./root --port 9000
```

A config that carries no `[auth]` section serves the built-in `minioadmin`/`minioadmin` pair instead, which is a **loopback-only** development credential: a non-loopback bind refuses to start without an `[auth]` pair (or `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY`).

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

