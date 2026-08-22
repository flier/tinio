# Implementation Plan: S3-Compatible Local Storage Server (tinio)

**Branch**: `001-s3-local-server` | **Date**: 2026-08-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/001-s3-local-server/spec.md`

## Summary

A compact Rust tool serving an S3-compatible interface over local directories: buckets map to top-level subdirectories, objects to files, with an S3 protocol layer provided by `s3s` over hyper/hyper-util and a filesystem backend implementing the `tinio-core` storage contract (mapped onto ~30 `S3` trait operations). All private state lives in a reserved `<root>/.tinio/` directory (configuration, state file, control socket, logs, multipart parts, git-style ETag metadata store, bucket creation times). A lifecycle CLI (`server`/`start`/`status`/`stop`/`doctor`, systemd-style with Minio-compatible invocation — positional directory argument, default ports 9000/9001) drives a management plane — axum + utoipa (OpenAPI) + Prometheus `/metrics` over a unix socket (Linux/macOS) or Windows named pipe — separate from the S3 data plane. Observability is tracing-based with optional OpenTelemetry export. Workspace of 7 crates: facade `tinio` (thin binary + curated re-export library), `tinio-core` (storage backend contract: trait + domain types + validation), `tinio-fs` (filesystem backend, v1), `tinio-config` (configuration/credentials), `tinio-server` (S3 compatibility layer: s3s operation mapping + data plane — always compiled; capability groups strippable via features `multipart`, `copy`, `list-v1`, `list-v2`), `tinio-api` (management plane — feature `api`, with crate-internal `openapi` and `tls` features), `tinio-cli` (commands). Facade default features: `api` + `openapi` + `tls` + `multipart` + `copy` + `list-v1` + `list-v2` (all individually strippable via `--no-default-features`); `otel` is opt-in. The `tinio-core` storage trait is the extension seam — planned backends (`tinio-s3`, `tinio-webdav`) implement the same contract, and future interface layers follow the same optional-feature pattern. CI runs unit/integration/property tests, criterion benchmarks (smoke), and aws cli v2 + rclone interop tests on Windows, Linux, and macOS.

## Technical Context

**Language/Version**: Rust, edition 2024; tracks the latest stable toolchain (no pinned MSRV — s3s's declared MSRV floor is satisfied by any current stable). MSRV policy = current stable; if fixed-version pinning is ever needed, it will follow constitution §VI discipline (documented, CI-tested, raised only in MINOR/MAJOR).

**Primary Dependencies**: `s3s` (protocol/auth/XML/error codes); `hyper` + `hyper-util` (data plane); `axum` (management plane); `utoipa` with `axum_extras` (OpenAPI); `prometheus` (metrics); `tracing` + `tracing-subscriber` (logging); `tokio` (async, fs/io-util/net) + `tokio-util` (streams); `serde`/`serde_json`/`toml` (config); `dotenvy` (`.env`); `mime_guess` (Content-Type inference); `md-5` (ETag); `clap` (CLI); `time` (timestamps); `thiserror` (per-crate error types); `uuid` (multipart upload IDs); `dirs` (home-dir resolution for read-only-mode state); `windows-sys` (cfg(windows), console-close event handling); `tokio-rustls` + `rustls-pemfile` (optional HTTPS management listener). Optional, behind feature `otel`: `opentelemetry`, `opentelemetry-otlp`, `tracing-opentelemetry`. Dev: `criterion`, `proptest`, `tempfile`.

**Storage**: Local filesystem. Buckets = top-level directories of the storage root; objects = files; nested keys map to nested directories. The state dir is `<root>/.tinio/` normally and `~/.tinio/roots/<sha1(canonical root)16>/` in read-only mode (FR-023 — root never written, all S3 mutations rejected with `AccessDenied`). It holds the files specified in [data-model.md](data-model.md) (Reserved Directory table). Writes are streaming temp-file + atomic rename.

**Testing**: `cargo test` per crate (unit + doc + integration); `proptest` for path/traversal handling, meta-store validation, multipart assembly; `criterion` benchmarks (smoke in CI): streaming write, streaming read, multipart assembly, prefix/delimiter listing; regressions are detected by comparing recorded baselines in the PR (mean slowdown > 10 % counts as a regression, requiring a documented decision and reviewer approval — constitution V); SC-003 is verified by streaming a 1 GB upload and download while sampling peak RSS (flat memory = RSS does not grow with object size); interop tests with aws cli v2 + rclone in CI on Windows/Linux/macOS; best-effort clients boto3 and mc are exercised via targeted/manual checks, not CI-gated (FR-025); constitution quality gates (fmt/clippy `-D warnings`/doc/stable matrix/semver-checks/audit). Dedicated integration tests: addressing style (aws cli v2 against both `127.0.0.1` and `localhost` endpoints — no client-side overrides, SC-002); cold listing with and without the scanner (FR-022/024); read-only mode end-to-end (genuinely read-only FS on unix, flag-only on Windows, FR-023); `doctor --dry-run`/`--fix` incl. home-state-dir GC; any-depth `.tinio` hiding incl. a nested-root scenario (FR-020); stop-wait confirmation; signal-driven graceful shutdown (SIGINT/SIGTERM on unix, Ctrl+C / console-close on Windows, second-signal immediate exit, SIGHUP ignored); crash-recovery startup repair (stale state/socket, tmp/ leftovers, bucket-orphaned multipart subtrees, stale buckets.json entries — per failure-handling.md §3).

**Target Platform**: Windows 11 (primary development), Linux, macOS.

**Project Type**: CLI + local web service (binary tool: S3 data plane + management plane).

**Performance Goals**: Flat memory on streaming paths regardless of object size (SC-003); ready within 1 s of start (SC-005); `status`/`stop` round-trip within 1 s (SC-007); full-scan metric gauges bounded by a 30 s TTL cache; bounded-buffer streaming with no unbounded buffering (constitution V); the background ETag scanner never blocks startup and yields to request traffic (FR-024).

**Constraints**: No panics/`unwrap`/`expect` in library code (constitution II); no `unsafe` (all crates `forbid`); every dependency already justified in spec Technical Decisions; case-sensitivity follows host filesystem (no artificial enforcement); Linux unix-socket path-length limit (108 bytes) documented as a limitation; single-instance enforcement via control-channel bind (management plane only — a build without the `api` feature has no such enforcement); graceful stop with bounded (10 s) in-flight drain; cargo feature gates: `api` (management plane), `openapi` (OpenAPI endpoint), `tls` (HTTPS listener), and the S3 capability groups `multipart`/`copy`/`list-v1`/`list-v2` — all default on; `otel` (OpenTelemetry) opt-in. The S3 compatibility layer itself is always compiled; symlinks are followed by default and can be disabled via `[storage] follow_symlinks` or `--no-follow-symlinks`. Allocation discipline (constitution V): streaming hot paths (file↔socket copy loops, ETag MD5 computation, multipart assembly) MUST use bounded buffers with no per-object allocation; verified by benchmark profiles.

**Scale/Scope**: Design ceiling: thousands to hundreds of thousands of objects per bucket, hundreds of GB to a few TB per storage root; no performance promises beyond that.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Evidence |
|------|--------|----------|
| I. Tiny Core | PASS | All dependencies justified in spec Technical Decisions section; no speculative features; scope bounded by spec (incl. management plane and observability, which are explicit spec scope) |
| II. Safety & Correctness | PASS | Library code returns `Result`/`Option`; no `unwrap`/`expect`/`panic!` in lib paths; `unsafe_code = "forbid"` in all crates (s3s itself forbids unsafe) |
| III. Idiomatic Rust APIs | PASS | Public API = curated facade re-exports with rustdoc examples; semver-checks target the facade; `no_std` N/A — server feature inherently requires std I/O (constitution exemption) |
| IV. Test-First | PASS | Unit tests written before implementation; proptest for I/O edge cases (path traversal, meta store, multipart); red-green-refactor enforced in task sequence |
| V. Predictable Performance | PASS | Streaming with bounded buffers; criterion benchmarks for hot paths; no hidden allocations in streaming path |
| VI. Semver & MSRV | PASS | MSRV policy: latest stable toolchain, documented and CI-tested (constitution VI interpreted with MSRV := current stable — the dual matrix collapses to one stable row); `cargo-semver-checks` on facade public API; CHANGELOG.md maintained |
| Workflow (spec review → plan → tasks) | PASS | Spec reviewed in design-review and clarify sessions before planning; implementation lands via PR with reviewer approval |

**Result**: No violations. Complexity Tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/001-s3-local-server/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── README.md            # documentation map (document roles, reading order, layering)
├── failure-handling.md  # abnormal-condition handling design (error taxonomy, crash recovery, reclamation division of labor)
├── scanner.md           # background ETag scanner design (pacing, meta reclamation, lifecycle)
├── fs-backend.md        # tinio-fs backend design (path mapping, atomic writes, meta store, listing, multipart, cleanup, scanner)
├── contracts/           # Phase 1 output (/speckit-plan command)
│   ├── config.md
│   ├── cli.md
│   ├── management-api.md
│   ├── s3-surface.md
│   └── minio-compat.md  # Minio-alignment contract (invocation, ports, env, scanner keys)
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
Cargo.toml                  # workspace root (members: crates/*)
crates/
├── tinio/                  # facade: thin binary + public re-export library
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs         # thin entry → tinio_cli::run()
│   │   ├── lib.rs          # curated pub use of tinio_core/tinio_config/tinio_server/tinio_api/tinio_cli (api feature-gated)
│   │   └── error.rs        # facade error re-exports
│   └── tests/              # integration tests against the facade public API
├── tinio-core/             # storage backend contract — no HTTP deps, no backend impl
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── error.rs       # StorageError (thiserror): backend-agnostic domain errors
│   │   ├── storage.rs      # Storage trait: bucket/object/multipart ops (async, Send+Sync)
│   │   ├── cleanup.rs      # Cleanup trait: startup repair / orphan reclamation / doctor diagnostics (backend-specific)
│   │   ├── domain.rs       # Bucket, ObjectInfo, PartInfo, multipart state types
│   │   ├── keys.rs         # backend-agnostic key validation (traversal, control chars)
│   │   └── testing.rs      # conformance test harness for backend implementations (behind the `testing` feature, off by default)
│   └── tests/
├── tinio-fs/               # filesystem backend (implements tinio-core::Storage) — v1
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── error.rs       # FsError (thiserror): io + domain mapping
│   │   ├── backend/       # Storage impl over the local filesystem (mod.rs + buckets.rs + objects.rs)
│   │   ├── path.rs         # bucket/key → path mapping, traversal rejection, case rules
│   │   ├── write.rs        # streaming temp-file + atomic rename
│   │   ├── listing.rs      # prefix/delimiter listing, pagination
│   │   ├── meta.rs         # git-style ETag store (2-hex fan-out, size/mtime validation)
│   │   ├── multipart.rs    # parts layout, assembly, abort
│   │   ├── buckets.rs      # buckets.json (creation times)
│   │   ├── scanner.rs      # low-priority background ETag scanner, optional rate cap (FR-024; Minio-aligned name)
│   │   ├── sweep.rs        # async mtime-based cleanup (temps 24 h, multipart 7 d)
│   │   └── cleanup.rs      # Cleanup trait impl for the fs backend (startup repair, doctor, scanner reclamation)
│   └── benches/            # criterion: streaming write/read throughput
├── tinio-config/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs          # Config struct, [server]/[scanner]/[auth]/[log]/[s3]/[storage]/[api]/[telemetry], validation
│   │   ├── error.rs       # ConfigError (thiserror): parse/validation failures
│   │   ├── sources.rs      # flags > env > .env > file resolution
│   │   └── credentials.rs  # first-start generation (persisted) / session generation
│   └── tests/
├── tinio-server/           # S3 compatibility layer — always compiled; capability features multipart/copy/list-v1/list-v2 (default on)
│   ├── Cargo.toml          # optional dep: tinio-api (behind feature `api`)
│   ├── src/
│   │   ├── lib.rs
│   │   ├── error.rs       # ServerError (thiserror): startup + mapping failures
│   │   ├── backend/       # S3 trait impl (~30 ops) over tinio-core (mod.rs + buckets/objects/listing/multipart.rs)
│   │   ├── metrics.rs      # MetricS3 wrapper + registry + TTL-cached gauges (registry injected into tinio-api)
│   │   ├── auth.rs         # S3Auth impl from config (SigV4 verification by s3s)
│   │   ├── data.rs         # hyper-util + S3Service data plane wiring
│   │   └── log.rs          # tracing layers: access-log formatter (nginx-style), fmt layers
│   ├── benches/
│   └── examples/           # serve.rs: minimal server binary used by the interop harness during US1
├── tinio-api/              # management plane — optional crate, feature `api` (default on)
│   ├── Cargo.toml          # axum, prometheus; utoipa(axum_extras) behind feature `openapi`; tokio-rustls behind feature `tls`
│   ├── src/
│   │   ├── lib.rs
│   │   ├── error.rs       # ApiError (thiserror): maps to HTTP status + JSON error bodies
│   │   ├── router.rs       # axum router: status/stop/metrics/openapi + token auth
│   │   ├── transport.rs    # unix socket / Windows named pipe / TCP HTTP(S) listeners
│   │   ├── state.rs        # state file (pid/token/port), single-instance bind
│   │   ├── openapi.rs      # utoipa schema
│   │   └── client.rs       # status/stop CLI client (used by tinio-cli)
└── tinio-cli/
    ├── Cargo.toml          # optional dep: tinio-api (behind feature `api`)
    └── src/
        ├── lib.rs          # run() + clap arg parsing + directory discovery
        ├── error.rs        # CliError (thiserror): user-facing messages + exit codes
        └── commands/
            ├── start.rs    # server/start commands (Minio-style positional DIR), config auto-create, daemon, wiring
            ├── status.rs   # api client (subcommand absent without `api`)
            ├── stop.rs     # api client, graceful stop (subcommand absent without `api`)
            └── doctor.rs   # offline diagnostics: config validity, on-disk keys, .tinio/ integrity

e2e/
├── interop/                # third-party S3 client scenarios (aws cli v2 + rclone CI-gated; boto3/mc targeted — FR-025)
│   ├── journey.sh          # core journey + shared harness; multipart/copy/cold-listing scenarios
│   ├── boto3.sh, mc.sh     # best-effort client scenarios
│   └── README.md           # client coverage matrix (FR-025)
└── perf/                   # performance verification scripts (flat-memory, streaming-memory smoke)

docs/
├── user-manual.md          # user manual (markdown)
└── tutorial.md             # usage tutorial (markdown)

packaging/
└── tinio.service           # example systemd unit (Type=simple, foreground)

benches/baselines.json      # committed criterion baseline data (Phase 6 regression gate)

.github/workflows/ci.yml    # matrix: Windows/Linux/macOS on latest stable; quality gates + feature-matrix compile checks; interop stage; bench-comparison job
```

**Structure Decision**: Seven-crate workspace. The facade crate `tinio` is the only public API surface (semver-checks target, rustdoc-example contract) while keeping the binary entry point a few lines. `tinio-core` defines the storage contract (`Storage` trait + domain types + backend-agnostic key validation + a conformance test harness) with zero HTTP dependencies; `tinio-fs` is the v1 filesystem implementation, and planned backends (`tinio-s3`, `tinio-webdav`) implement the same trait — the protocol layer, CLI, and config do not change when a backend is added. `tinio-server` maps the s3s `S3` trait onto the storage contract (the s3s protocol layer itself stays replaceable per the MVP decision). `tinio-api` holds the entire management plane (axum router, transports, token auth, state file, single-instance bind, status/stop client) as an optional crate behind the default-on `api` cargo feature — builds with `--no-default-features` produce a bare S3 server without FR-018's management surface; when the feature is off, the `status`/`stop` subcommands and `--api` options are absent from the CLI (compiled out) and `[api]` config keys are silently ignored (they are schema-known keys, not unknown-key failures). Wiring: `tinio-cli` (start) builds the data plane (the S3 compatibility layer is always compiled — there is no `s3` feature), then the api plane around a shared shutdown channel; the Prometheus registry is owned by `tinio-server` (the data plane instruments it) and injected into `tinio-api` for `/metrics`, so any feature combination works (the api plane exposes the metrics and computes the storage-layer gauges via the storage contract). Feature-off behavior follows the contract for each disabled feature (config keys silently ignored, CLI options absent, `NotImplemented` for stripped groups — see `contracts/config.md`, `contracts/cli.md`, `contracts/management-api.md`, `contracts/s3-surface.md`). `tinio-config` isolates configuration/credential resolution so CLI, server, and tests share one source of truth. Conformance tests: every backend implementation runs the `tinio-core` harness, so `tinio-s3`/`tinio-webdav` inherit the same behavioral contract. Interop tests live outside the crate tree because they require installed S3 clients and only run in CI; unit/integration tests live per crate and run everywhere. Backend selection is not configurable in v1 (filesystem-only); a selection key will be added when the second backend lands. Cleanup follows the same seam: the `Cleanup` trait in `tinio-core` (startup repair, orphan reclamation, doctor diagnostics/fix) is implemented per backend — the fs implementation owns tmp/multipart/buckets.json/meta reconciliation — and the start orchestration and `doctor` call it through the trait, never through a backend implementation. Each crate defines its own typed error module (`error.rs`) built on `thiserror`, with `From`-conversion chains across crate boundaries (storage errors → S3 error codes → HTTP statuses → CLI exit codes), so no crate ever leaks another crate's error type.

## Complexity Tracking

> Not required — no Constitution Check violations. All added components (management plane, metrics, observability) are explicit spec scope with justified dependencies.

## Phase 0: Research

Output: [research.md](research.md) — protocol/framework decisions, storage semantics, observability design, and Windows/Unix transport verification. No unresolved `NEEDS CLARIFICATION` items remain (design review + clarify session resolved all; three framework facts verified during research).

## Phase 1: Design & Contracts

Outputs: [data-model.md](data-model.md), [contracts/](contracts/) (`config.md`, `cli.md`, `management-api.md`, `s3-surface.md`), [quickstart.md](quickstart.md).
