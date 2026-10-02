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

## `--sandbox` on Windows

The launcher's own directory is handled for you, and it does not pay the cost below: it grants the program's directory one **non-inherited** read ACE, and the `*.dll` files **directly beside the program** their read ACEs in a single batched call through the tool's `*.dll` pattern — so a binary that imports a DLL from its own directory loads, while a DLL in a *subdirectory* of it, or one a program loads from elsewhere at runtime, is **not** granted. **The directories you expose are untouched by that rule**: they keep the inherited ACE below, which is what makes a file that was already inside them readable by the confined server. That boundary follows the loader's own search path rather than assuming it. The confined server reaches its data through a DACL grant, and the filesystem's own inheritance is what carries that grant to every object under each exposed directory — so the grant (before the child starts) costs a pass over the whole served tree — and it is the **only** pass, because every grant is standing (design decision 15): a root that already carries its ACE pays a read instead of a write, and nothing removes the grant when the server exits. On a 200,000-object tree that is **roughly 30–45 seconds per pass on NTFS and about nine seconds on ReFS** — enough that the grant alone outlasts the readiness budget the e2e harness allows. So expose the narrowest directory that serves the corpus — a parent holding far more than the server serves pays for all of it — and where the corpus is large enough for that pass to matter, measure the grant on a tree of the same object count before exposing it: `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1 -AclCost <dirs> <files>` (the method and its checks are in `docs/tests.md`, the numbers and their spread in the design's Risks table, which also lists every place a killed launcher can leave residue).

The run reports what that pass cost it, so an unfamiliar startup does not have to be guessed at: every launcher logs its grant phase's elapsed time on stderr as a `debug` event, and a run whose grant phase spent half or more of the readiness budget logs a `WARN` naming that elapsed time, the number of exposures the pass was paid for and the threshold it crossed — so a run that looked like a hang reads as `the ACL grant phase took 34.2s`, beside the `sandbox: granting …` banner that lists the directories whose trees the pass covered. That line is a diagnosis and not a remedy: nothing in the launcher shortens the pass, so the two choices above — expose the narrowest directory that serves the corpus, or measure the grant on a tree of the same object count and accept the cost — stay the operator's.

## Testing

The canonical run commands (unit/doc, cucumber, external-client interop),
the full gate matrix, WSL2 verification notes, and the `TINIO_E2E_*` /
`TINIO_TEST_WAIT_TIMEOUT_SECS` knobs live in
[`docs/tests.md`](docs/tests.md) — its commands are the source of truth,
so the CI workflow and this README never restate them.

