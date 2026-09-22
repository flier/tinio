# CONTEXT.md

Domain glossary for the tinio workspace. English only. This file is a glossary — it records the meaning of terms, not implementation details.

## BDD test suite (cucumber)

- **Feature file** — a Gherkin `.feature` document; the executable form of a contract in `specs/001-s3-local-server`.
- **Scenario** — one behavior example in a feature file, composed of Given/When/Then steps; the atomic test unit.
- **Scenario outline / Examples** — a parameterized scenario template with data rows.
- **Step / step definition** — one Given/When/Then line in a feature and the Rust function that executes it.
- **World** — the per-scenario shared state: the server under test and the last response.
- **Spec scenario** — a scenario that carries a spec ID (`@FR-xxx` / `@SC-xxx` / `@Txxx`) and documents a contract behavior.
- **Spec traceability tags** — the `@FR-xxx`/`@SC-xxx`/`@Txxx` tags linking a scenario to the spec; feature-level (inherited by tag filters, invisible to the hooks).
- **Scenario-level tags** — the tags the `#[before]` hook reads (`config_from_tags`, one mapping shared by the in-process server and the `@external` spawn): backend (`@fs`/`@mem`), `@nested-root`, `@checksum-on`, `@minimal-caps`, `@cold-listing`, `@max-buckets-3`, and the external-client tags `@interop`/`@boto3`/`@mc`; `@aws`/`@rclone` are declarative markers of which client an `@interop` scenario drives (not read by the hook).
- **Backend-neutral scenario** — no backend tag; runs on the default backend and in every backend pass.
- **Backend-tagged scenario** — carries `@fs` or `@mem`; runs only on that backend. An explicit backend tag wins over the environment.
- **Backend pass** — a full run of the backend-neutral suite on one backend; CI runs the fs pass (default) and the mem pass (`TINIO_E2E_BACKEND=mem`, excluding `@fs`-only scenarios).
- **@external umbrella** — the union of `@interop` ∪ `@boto3` ∪ `@mc`; scenarios that need external client binaries and are excluded from default runs.
- **fail_on_skipped** — the runner's always-on mode: undefined or otherwise skipped steps fail the run, so a feature that drifts out of sync with its step definitions never passes silently.
- **Traceability check** — `cargo test -p tinio-e2e --test traceability`: cross-references the spec corpus and the feature tags both ways (with the documented `NOT_COVERED_BY_CUCUMBER` allow-list).
- **Parity** — a migrated test's feature scenarios cover the old test 1:1; the old file is deleted only after parity holds.
- **Deletion rhythm** — port → run old and new side by side → verify 1:1 coverage → delete the old file in-tree.

## Testing concepts

- **Conformance harness** — the `tinio-util` `assert_conformance` suite, Rust-only; proves every `Storage` implementation meets the storage contract. Cucumber scenarios never repeat its assertions.
- **Migration rule** — a test migrates iff it carries a spec semantic AND its behavior is observable through the S3 API.
- **Interop** — scenarios that drive external clients (aws-cli, rclone, boto3, mc) against a spawned server; `@interop` is CI-gated, `@boto3`/`@mc` are manual.

## Storage & server

- **Served root** — the directory a server instance serves; the fs backend keeps its state and buckets inside it, and never writes outside it.
- **State dir** — the fs backend's metadata database and temp areas: `.tinio/` inside the served root in normal mode; read-only mode relocates it to the home state dir (see Read-only mode).
- **Scanner** — the fs backend's background sweeper (temp cleanup, interrupted-upload cleanup); "cold listing" exercises listing before the scanner has caught up.
- **Capability toggle** — a server configuration switch that enables/disables an S3 operation or feature (multipart, copy, list v1/v2, delete-objects, checksum validation).
- **Read-only mode** — the served plane's write policy (FR-023): every mutating S3 operation answers `AccessDenied` and the storage root is never written, so a genuinely read-only filesystem can be served. Tool state relocates from `<root>/.tinio/` to `~/.tinio/roots/<hash>/` (`TINIO_STATE_DIR` overrides the location). The root's own `.tinio/` is not read in this mode, so metadata written by an earlier read-write run — tags, ACLs, CORS, upload records — reads back empty while objects still serve.

## CLI and management plane

- **Management plane** — the control surface beside the S3 data plane (FR-018): one local channel (a unix socket or a Windows named pipe) or one configured TCP HTTP(S) listener, carrying `/status`, `/stop`, `/metrics` and `/openapi.json` under the token rule its bind address selects.
- **State file** — the plane's record of a running server, published in the state dir at readiness and removed by a graceful stop: version, PID, endpoint, token, root and the control channel's name. Clients read it instead of re-deriving any of those, which is what keeps `status`'s answer the server's rather than the client's reconstruction.
- **Root discovery** — how a command finds the served root: the positional `DIR` when one is given, else a walk up from the current directory to the nearest ancestor holding `.tinio/`. A root that cannot be found is a usage error naming the remedy, never an in-memory server.
- **Sandbox channel** — each of the three ways confinement is asked for: the `--sandbox` flag, the `TINIO_SANDBOX` variable and the `[server] sandbox` key, resolved through one precedence chain, so any one of them is enough and an explicit `false` at a higher level beats a `true` below it. A request the build or the platform cannot honour is refused — a silent unconfined serve is the failure the request exists to prevent.
