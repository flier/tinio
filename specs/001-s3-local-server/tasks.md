---

description: "Task list for the S3-compatible local storage server (tinio) feature implementation"
---

# Tasks: S3-Compatible Local Storage Server (tinio)

**Input**: Design documents from `/specs/001-s3-local-server/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/ (config.md, cli.md, management-api.md, s3-surface.md, minio-compat.md), quickstart.md

**Tests**: Test tasks ARE included — the project constitution (`.specify/memory/constitution.md`, Principle IV, NON-NEGOTIABLE) mandates test-first development, and plan.md §Testing specifies the test matrix (unit, doc, proptest, criterion, interop).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- **Unit tests**: every implementation task includes its unit tests, written FIRST and failing (red-green, constitution IV)
- Include exact file paths in descriptions

## Path Conventions

- **Workspace**: seven crates under `crates/` (`tinio`, `tinio-core`, `tinio-fs`, `tinio-config`, `tinio-server`, `tinio-api`, `tinio-cli`); workspace root `Cargo.toml` at the repository root
- **Interop tests**: `e2e/interop/` (third-party S3 client scenarios — mandated clients CI-gated, best-effort clients targeted/manual, per FR-025)
- **Performance scripts**: `e2e/perf/` (streaming-memory and flat-memory verification scripts)
- **User docs**: `docs/` (user manual + usage tutorial, markdown)
- **Packaging**: `packaging/tinio.service`
- **CI**: `.github/workflows/ci.yml`
- **Backend modules**: the S3 mapping and the filesystem `Storage` impl live in `backend/` module directories (`src/backend/mod.rs` + one file per operation group) — the module name `backend` is unchanged from plan.md

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Workspace initialization, crate skeletons with all dependencies and cargo features per plan.md §Project Structure

- [ ] T001 Create workspace root `Cargo.toml` (members `crates/*`, edition 2024, shared workspace dependencies: thiserror, serde, tokio, tracing, tracing-subscriber, time, uuid, md-5, clap, dotenvy, dirs, mime_guess, tempfile/proptest/criterion as dev deps) and `.gitignore` (target/, etc.)
- [ ] T002 [P] Create `crates/tinio-core` skeleton: Cargo.toml (deps: thiserror, tokio, serde; feature `testing`, off by default; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [ ] T003 [P] Create `crates/tinio-fs` skeleton: Cargo.toml (deps: tinio-core, md-5, time, uuid, tokio; dev-deps: tinio-core/testing, tempfile, proptest, criterion, dhat; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [ ] T004 [P] Create `crates/tinio-config` skeleton: Cargo.toml (deps: serde, serde_json, toml, dotenvy, dirs; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [ ] T005 [P] Create `crates/tinio-server` skeleton: Cargo.toml (deps: s3s, hyper, hyper-util, tokio-util, mime_guess, prometheus, tracing, tinio-core, tinio-config; optional dep tinio-api behind feature `api`; default-on features `multipart`, `copy`, `list-v1`, `list-v2`; opt-in feature `otel` = optional deps opentelemetry + opentelemetry-otlp + tracing-opentelemetry; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [ ] T006 [P] Create `crates/tinio-api` skeleton: Cargo.toml (deps: axum, prometheus, tinio-core; feature `openapi` = utoipa with `axum_extras`, feature `tls` = tokio-rustls + rustls-pemfile; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [ ] T007 [P] Create `crates/tinio-cli` skeleton: Cargo.toml (deps: clap, tinio-config, tinio-core, tinio-fs, tinio-server; optional dep tinio-api behind feature `api`; target-gated `windows-sys` (cfg(windows), `Win32_System_Console`) for console-close event handling (T069); `unsafe_code = "forbid"`) and empty `src/lib.rs` + `src/commands/` directory
- [ ] T008 [P] Create `crates/tinio` facade skeleton: Cargo.toml (deps: tinio-core, tinio-config, tinio-server, tinio-cli; optional tinio-api behind feature `api`; default features `api` + `openapi` + `tls` + `multipart` + `copy` + `list-v1` + `list-v2`; opt-in passthrough feature `otel` = `tinio-server/otel`; `unsafe_code = "forbid"`), thin `src/main.rs` delegating to `tinio_cli::run()`, `src/lib.rs` with curated re-exports (rustdoc examples per constitution III), `src/error.rs`
- [ ] T009 [P] Create CI workflow `.github/workflows/ci.yml`: Windows/Linux/macOS matrix on latest stable; quality gates (fmt --check, clippy `-D warnings`, `cargo test --workspace` incl. `--no-default-features`, `cargo doc` no warnings, semver-checks on facade, audit); feature-matrix compile checks (explicit feature-combination list or cargo-hack, `cargo check` level — catches feature-gate breakage early); interop stage (aws cli v2 + rclone)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story: the `tinio-core` storage contract, the configuration schema, per-crate error types, and the metrics registry. **No user story work can begin until this phase is complete.**

- [ ] T010 Implement `StorageError` (thiserror) in `crates/tinio-core/src/error.rs`: backend-agnostic domain errors (NotFound, AlreadyExists, NotEmpty, InvalidKey, InvalidBucketName, Unsupported, transparent Io), with rustdoc examples on public items (constitution III); unit tests written first
- [ ] T011 [P] Implement domain types in `crates/tinio-core/src/domain.rs`: Bucket, ObjectInfo, PartInfo, multipart state types (Send + Sync + 'static), with rustdoc examples on all public items (constitution III); unit tests written first
- [ ] T012 Implement the async `Storage` trait in `crates/tinio-core/src/storage.rs`: bucket/object/multipart operations per data-model.md, backend-agnostic, with rustdoc examples on all public items (constitution III); unit tests written first
- [ ] T013 [P] Implement backend-agnostic key validation in `crates/tinio-core/src/keys.rs`: traversal (`..`) / absolute-path / control-character rejection (FR-006), bucket-name rules (FR-012), `.tinio` reserved-segment rule at ANY depth (FR-020); unit tests written first
- [ ] T014 Implement conformance test harness in `crates/tinio-core/src/testing.rs` behind the `testing` feature: every backend implementation must pass it
- [ ] T015 Implement `ConfigError` in `crates/tinio-config/src/error.rs` (parse/validation failures), with rustdoc examples on public items (constitution III); unit tests written first
- [ ] T016 Implement the `Config` struct in `crates/tinio-config/src/lib.rs`: `version = 1` + `[server]` `[scanner]` `[auth]` `[log]` `[s3]` `[storage]` `[api]` (`unix`/`http`/`https` subsections) `[telemetry]` sections per contracts/config.md, with rustdoc examples on public items; unit tests written first
- [ ] T017 Implement fail-fast validation in `crates/tinio-config/src/lib.rs`: unknown keys/sections rejected; fixed access-log format variable set enforced (closed set — no Authorization/query/credentials, per FR-017); presence-gated sections (`[scanner]`, `[api.*]`); port rules (default 9000, 0 = ephemeral); `[api.https]` requires cert+key; http/https ports must differ; boolean key typing; credential presence rules; unit tests written first
- [ ] T018 [P] Implement source precedence resolution in `crates/tinio-config/src/sources.rs`: CLI flags > process env > `.env` (via dotenvy) > config file (FR-016); `.env` loaded from the state dir; unit tests written first
- [ ] T019 [P] Implement `FsError` in `crates/tinio-fs/src/error.rs`: io + domain mapping, `From`-conversion into the core error, with rustdoc examples on public items (constitution III); unit tests written first
- [ ] T020 [P] Implement `ServerError` in `crates/tinio-server/src/error.rs`: startup + S3-mapping failures, with rustdoc examples on public items (constitution III); unit tests written first
- [ ] T021 [P] Implement `ApiError` in `crates/tinio-api/src/error.rs`: maps to HTTP status + JSON error bodies (401/404/500 per management-api contract), with rustdoc examples on public items (constitution III); unit tests written first
- [ ] T022 [P] Implement `CliError` in `crates/tinio-cli/src/error.rs`: user-facing messages + exit codes (0 success / 1 operational / 2 usage), with rustdoc examples on public items (constitution III); unit tests written first
- [ ] T023 Implement the Prometheus registry and metric family definitions in `crates/tinio-server/src/metrics.rs`: `tinio_http_*`, `tinio_s3_*`, `tinio_storage_*` families per data-model.md Metrics section (names, labels, help strings). The TTL-cached (30 s) full-scan gauges are registered and computed later in T074, not scaffolded here; unit tests written first

**Checkpoint**: Foundation ready — storage contract, config schema, error chains (fs → core → S3 → HTTP → CLI), and metrics registry exist. User story implementation can now begin.

---

## Phase 3: User Story 1 - Serve Local Directories Over an S3-Compatible Interface (Priority: P1) — MVP CORE

**Goal**: The data plane serves an S3-compatible interface over the local filesystem: buckets map to top-level subdirectories, objects to files; create/delete buckets, upload/download/delete/list objects work with standard S3 clients (aws cli v2, rclone, boto3, mc) with no client-side workarounds. Includes streaming (FR-010), atomic writes (FR-011), ETag metadata (FR-022), multipart (FR-014), server-side copy (FR-015), listing semantics (FR-004), error codes (FR-005), key validation (FR-006), `.tinio` reservation (FR-020), background ETag scanner (FR-024), and the async sweep.

**Independent Test**: Start the data plane against an empty directory (via the tinio-server integration harness), then use a standard S3 client to create a bucket, upload a file, download it back (byte-identical), list objects with prefix/delimiter, and delete the object. Verify the uploaded file physically appears in the local directory and that a file dropped into the directory by hand is immediately retrievable through the client (SC-006). Files served with correct ETag (MD5), Content-Type, and error codes (SC-004). Multipart and copy operations complete without client passthrough. The basic scenario set is exercised through EACH third-party tool per the client matrix (T036).

### Tests for User Story 1 (written FIRST, must FAIL before implementation)

- [ ] T024 [P] [US1] Contract/integration test: S3 error-code behavior (SC-004) in `crates/tinio-server/tests/error_codes.rs`: NoSuchBucket, NoSuchKey, InvalidBucketName, BucketAlreadyExists, BucketNotEmpty, NotImplemented, traversal rejection with no FS access
- [ ] T025 [P] [US1] Integration test: full data-plane round-trip in `crates/tinio-server/tests/data_plane.rs`: create bucket / upload / download (byte-identical) / list (prefix + delimiter grouping, pagination) / delete; zero-byte objects; nested keys; concurrent writes last-write-wins with no torn objects; interrupted upload leaves no partial object; Range requests (206/Content-Range); conditional requests (304/412); folder markers (`dir/` never an object); out-of-band file changes served immediately (reserved-path behavior is tested in T026)
- [ ] T026 [P] [US1] Integration test: reserved-path behavior in `crates/tinio-server/tests/reserved_paths.rs`: any-depth `.tinio` segments — write → AccessDenied, read → NoSuchKey, listings skip; nested-root scenario (an inner root's state is never served by an outer server); distinct file from T025 so the security surface is independently reviewable
- [ ] T027 [P] [US1] Property tests for key validation and path mapping in `crates/tinio-core/tests/keys.rs` (proptest): traversal sequences, absolute paths, control characters, `.tinio` segments at any depth, bucket-name rules
- [ ] T028 [P] [US1] Property tests for the meta store in `crates/tinio-fs/tests/proptest_meta.rs`: ETag entry round-trips, size/mtime mismatch → recompute, atomic-write concurrency (no torn JSON)
- [ ] T029 [P] [US1] Property tests for multipart assembly in `crates/tinio-fs/tests/proptest_multipart.rs`: arbitrary part counts/sizes assemble to the exact concatenation, composed ETag `MD5-of-MD5s-N` matches reference implementation
- [ ] T030 [P] [US1] Criterion benchmarks for the fs backend in `crates/tinio-fs/benches/`: streaming write/read throughput on the bounded-buffer paths; smoke run in CI; baselines recorded and regression-gated in Phase 6 (T087)
- [ ] T031 [P] [US1] Criterion benchmarks for the S3 mapping layer in `crates/tinio-server/benches/`: multipart assembly, prefix/delimiter listing; smoke run in CI; baselines recorded and regression-gated in Phase 6 (T087)
- [ ] T032 [US1] Interop core-journey scenario and shared harness in `e2e/interop/journey.sh` (+ common runner): aws cli v2 + rclone full journey (mb/cp/ls/rm/rb) against `127.0.0.1` with no client-side addressing overrides (SC-002), plus `--port 0` ephemeral-port runs; CI-gated (mandated per FR-025). The harness spawns the server via the minimal example binary `crates/tinio-server/examples/serve.rs` during US1 and switches to the facade binary once the CLI lands in US2
- [ ] T033 [US1] Interop advanced scenarios in `e2e/interop/` (reusing the T032 harness): multipart upload (>8 MB file → composed ETag pattern), server-side copy, cold-listing with and without scanner — via aws cli v2 + rclone; CI-gated (mandated per FR-025)
- [ ] T034 [P] [US1] boto3 basic-journey scenario in `e2e/interop/boto3.sh`: the SC-001 basic scenario set via the boto3 SDK (best-effort client per FR-025 — targeted/manual, NOT CI-gated): create bucket, upload, download byte-identical, list with prefix/delimiter, delete object, zero-byte round-trip, multipart via `upload_file` (>8 MB → composed ETag pattern); documents known deviations (x-amz-checksum-* ignored, x-amz-meta-* dropped, Content-Type inferred at serve time); requires Python 3 + boto3 installed for targeted runs
- [ ] T035 [P] [US1] mc (MinIO Client) basic-journey scenario in `e2e/interop/mc.sh`: the SC-001 basic scenario set via `mc` (best-effort client per FR-025 — targeted/manual, NOT CI-gated): mb/cp/ls/rm/rb, large-file copy (multipart), `mc stat` ETag check, zero-byte object; documents known deviations; requires the mc binary installed for targeted runs
- [ ] T036 [P] [US1] Client coverage matrix in `e2e/interop/README.md`: map every third-party tool (aws cli v2, rclone — mandated/CI-gated; boto3, mc — best-effort/targeted-manual; unsupported list per FR-025) to the basic scenarios it exercises (bucket create/delete, upload, download, list, multipart, copy, auth), so FR-025 coverage is verifiable at a glance

### Implementation for User Story 1

- [ ] T037 [US1] Implement path mapping in `crates/tinio-fs/src/path.rs`: bucket/key → filesystem path, traversal rejection before any FS access, platform charset rules (Windows-invalid chars on Windows only), `.tinio` any-depth reservation, case sensitivity follows host FS; unit tests written first
- [ ] T038 [P] [US1] Implement streaming atomic writes in `crates/tinio-fs/src/write.rs`: temp file under `<state-dir>/tmp/` + `fs::rename`, bounded buffers, last-write-wins (FR-010/011); unit tests written first
- [ ] T039 [P] [US1] Implement the ETag meta store in `crates/tinio-fs/src/meta.rs`: git-style 2-hex fan-out (`meta/objects/<bucket>/<2hex>/<sha1>.json` = `{key, etag, size, mtime}`), atomic writes under an in-process lock, size/mtime validation with streaming recompute on mismatch (FR-022); unit tests written first
- [ ] T040 [P] [US1] Implement bucket creation times in `crates/tinio-fs/src/buckets.rs`: `buckets.json` (`{"version": 1, "buckets": {...}}`), atomic temp+rename under an in-process lock, lazy recording on first sight, orphan cleanup on bucket delete; unit tests written first
- [ ] T041 [P] [US1] Implement bucket operations of the `Storage` impl in `crates/tinio-fs/src/backend/buckets.rs`: create (with S3-name re-validation), delete-only-when-empty → BucketNotEmpty, head, list; MUST pass the tinio-core conformance harness; unit tests written first
- [ ] T042 [P] [US1] Implement object operations of the `Storage` impl in `crates/tinio-fs/src/backend/objects.rs`: put/get/head/delete with streaming, Range seek, folder-marker semantics — keys ending in `/` never become objects: PUT creates the directory, GET/HEAD → NoSuchKey, DELETE removes an empty directory and always returns 204; symlink policy — follow by default, exclude/reject when disabled; MUST pass the tinio-core conformance harness; unit tests written first
- [ ] T043 [US1] Implement listing in `crates/tinio-fs/src/listing.rs`: prefix filtering, delimiter-based grouping (common-prefix roll-up), pagination per S3 semantics (FR-004); ETags included — missing/stale entries recomputed synchronously during listing; `.tinio` entries always skipped; unit tests written first
- [ ] T044 [US1] Implement multipart storage in `crates/tinio-fs/src/multipart.rs`: parts at `<state-dir>/multipart/<bucket>/<uploadId>/part-<n>`, streaming assembly into temp + atomic rename, composed ETag, abort removes the parts subtree, no 5 MB minimum (FR-014); unit tests written first
- [ ] T045 [US1] Implement the background ETag scanner in `crates/tinio-fs/src/scanner.rs` (FR-024): low-priority task pre-computing missing/stale meta entries, paced by `[scanner]` delay/max_wait/cycle, presence-gated (section absent = off) with the `TINIO_SCANNER` env toggle (0/1) as an independent override, yields to request traffic, never blocks startup, aborts quietly on shutdown; unit tests written first
- [ ] T046 [US1] Implement the async sweep in `crates/tinio-fs/src/sweep.rs` (FR-014): mtime-based cleanup of stale temp files (default 24 h, `[s3] temp_ttl_hours`) and abandoned multipart uploads (default 7 days, `[s3] multipart_expire_days`); non-blocking, yields to request traffic; unit tests written first
- [ ] T047 [P] [US1] Implement the S3 buckets group in `crates/tinio-server/src/backend/buckets.rs`: CreateBucket/DeleteBucket/HeadBucket/ListBuckets/GetBucketLocation (CreationDate from buckets.json, GetBucketLocation → `us-east-1`); storage errors → S3 error codes (NoSuchBucket, BucketAlreadyExists, BucketNotEmpty, InvalidBucketName); unit tests written first
- [ ] T048 [P] [US1] Implement the S3 objects group + copy in `crates/tinio-server/src/backend/objects.rs`: PutObject/GetObject/HeadObject/DeleteObject/DeleteObjects/CopyObject; Range + conditional headers per s3-surface contract (206/Content-Range, 304/412); Content-Type inferred via mime_guess; `x-amz-meta-*` and `x-amz-checksum-*` accepted and dropped; CopyObject behind `copy` feature, runtime `[s3]` toggles (copy_object, delete_objects) → NotImplemented; unit tests written first
- [ ] T049 [P] [US1] Implement S3 listing V1/V2 in `crates/tinio-server/src/backend/listing.rs`: ListObjects (V1) + ListObjectsV2 (V2) over the tinio-fs listing — prefix filtering, delimiter grouping, pagination per S3 semantics (FR-004); shared listing core with separate XML surfaces; compile-time gates `list-v1`/`list-v2` and runtime `[s3]` toggles → NotImplemented; unit tests written first
- [ ] T050 [P] [US1] Implement the S3 multipart group in `crates/tinio-server/src/backend/multipart.rs`: CreateMultipartUpload/UploadPart/UploadPartCopy/CompleteMultipartUpload/AbortMultipartUpload/ListParts/ListMultipartUploads; composed ETag `MD5-of-MD5s-N`; UploadPartCopy additionally gated by `copy` feature; part numbers validated 1..=10000 per data-model (invalid → InvalidPart); compile-time `multipart` gate and runtime `[s3]` toggle → NotImplemented; unit tests written first
- [ ] T051 [US1] Wire the data plane in `crates/tinio-server/src/data.rs`: hyper + hyper-util hosting of `S3Service` (tower::Service), path-style addressing, streaming bodies with bounded buffers; unit tests written first
- [ ] T052 [US1] Implement logging layers in `crates/tinio-server/src/log.rs`: access-log tracing layer (nginx-style `combined`/`common`/custom format strings over the fixed variable set, target `tinio::access`) writing to `access.log` in the state dir; operational text/json fmt layers (errors always visible on stderr, FR-017); unit tests written first
- [ ] T053 [US1] Implement the OpenTelemetry export layer (FR-017 opt-in export) behind the `otel` feature in `crates/tinio-server/src/log.rs` + `crates/tinio-server/src/data.rs`: OTLP exporter construction from `[telemetry] otlp_endpoint` (fallback to `OTEL_EXPORTER_OTLP_ENDPOINT`), `tracing-opentelemetry` layer registration, shutdown/export hygiene on server exit; enabled via the facade `otel` passthrough (T008) or `-p tinio-server --features otel`; unit tests written first
- [ ] T054 [US1] Implement metrics instrumentation in `crates/tinio-server/src/metrics.rs`: `MetricS3` delegation wrapper recording `tinio_s3_operations_total{op,status}` + duration; HTTP middleware recording request count/duration/in-flight; upload/download byte counters maintained on streaming paths; in-progress multipart gauge; unit tests written first

**Checkpoint**: User Story 1 fully functional — the data plane serves the complete v1 S3 surface against aws cli v2 and rclone (CI-gated), with boto3 and mc basic journeys verified manually; error-code suite green; streaming flat-memory verification runs in Phase 6 (T088/T089); scanner + sweep operational.

---

## Phase 4: User Story 2 - Basic CLI Management (Priority: P2)

**Goal**: The user manages the server from the command line: `tinio server <dir>` / `start` (Minio-style positional directory, config auto-created on first start with generated credentials), `status`, `stop` (graceful, in-flight drain ≤ 10 s, stop-wait confirmation), `doctor` (offline diagnostics with `--json`/`--dry-run`/`--fix`); the management plane (unix socket / Windows named pipe / optional TCP HTTP(S), token-authenticated) exposes `/status`, `/stop`, `/metrics`, `/openapi.json`; single-instance enforcement via the control-channel bind; read-only mode (FR-023); correct handling of common signals in foreground server mode.

**Independent Test**: Start the server against a directory, run `status` (reports running, endpoint, root, PID — round-trip < 1 s, SC-007), create subdirectories and files by hand and verify they are served through the S3 interface, then `stop` and verify a clean shutdown with no partial files. A second `server` on the same root fails with the single-instance error. `GET /metrics` returns the three-layer metric set with TTL-cached gauges (SC-008). Pressing Ctrl+C (or sending SIGTERM) shuts the server down gracefully with state/socket removed and no partial files.

### Tests for User Story 2 (written FIRST, must FAIL before implementation)

- [ ] T055 [P] [US2] Integration test: lifecycle in `crates/tinio-cli/tests/lifecycle.rs`: start (custom port) → status → stop round-trips < 1 s (SC-007); startup readiness reached ≤ 1 s of the start command (SC-005); config auto-created with generated credentials; `--port 0` ephemeral port reported in logs/state; second instance → single-instance error exit 1; stop-wait confirmation and timeout path; `--daemon` detaches; feature-off builds omit `status`/`stop` subcommands; signal handling: SIGINT/SIGTERM (unix) / Ctrl+C (Windows) → graceful shutdown (state/socket removed, exit 0, no partial files); second signal → immediate exit; SIGHUP ignored (unix)
- [ ] T056 [P] [US2] Integration test: management plane in `crates/tinio-api/tests/management.rs`: `/status` (token) and `/stop` (202 draining, bounded 10 s drain); `/metrics` and `/openapi.json` open on the local channel but require the token over TCP (401 without); Windows named pipe transport with `FILE_FLAG_FIRST_PIPE_INSTANCE`; unix stale-socket probe-then-reclaim
- [ ] T057 [P] [US2] Integration test: doctor in `crates/tinio-cli/tests/doctor.rs`: clean root → exit 0; stale state/socket, orphaned meta and buckets.json entries, abandoned multipart, stale temps → exit 1; `--dry-run` lists repairs without touching; `--fix` applies them (server stopped required); `--json` output; home root-state-dir GC
- [ ] T058 [P] [US2] Integration test: read-only mode end-to-end in `crates/tinio-server/tests/read_only.rs` (FR-023): reads behave identically; every mutating op → AccessDenied; state lives under `~/.tinio/roots/<sha1(canonical root)16>/`; genuinely read-only FS on unix, flag-only on Windows; pre-existing root config still read but never written

### Implementation for User Story 2

- [ ] T059 [US2] Implement credential generation in `crates/tinio-config/src/credentials.rs`: CSPRNG generation (access key ≥ 16 bytes, secret key ≥ 32 bytes) for the first-start config auto-create; config written mode 0600 / ACL-restricted on Windows; unit tests written first
- [ ] T060 [US2] Implement the state file and single-instance bind in `crates/tinio-api/src/state.rs`: `state` = `{version, pid, token, port, started_at, control_name}` (token ≥ 32 bytes CSPRNG, mode 0600); stale unix socket probe-then-unlink; Windows pipe `FILE_FLAG_FIRST_PIPE_INSTANCE`; state removed on graceful stop; unit tests written first
- [ ] T061 [US2] Implement the local management channel in `crates/tinio-api/src/transport.rs`: unix socket (`tokio::net::UnixListener` via axum `Listener`) on Linux/macOS; Windows named pipe with manual `serve_connection` loop and `http1_keep_alive(false)` (half-close pitfall, research §14); unit tests written first
- [ ] T062 [US2] Implement TCP management listeners in `crates/tinio-api/src/transport.rs`: TCP HTTP via axum::serve; HTTPS via TLS listener wrapper (tokio-rustls, feature `tls`); the "at least one transport enabled" startup check (with the `api` feature compiled, no transports enabled is a startup error); unit tests written first
- [ ] T063 [US2] Implement the management router in `crates/tinio-api/src/router.rs`: axum router with `/status`, `/stop` (202 draining response), `/metrics`, `/openapi.json`; `X-Tinio-Token` auth on `/status`/`/stop` always, on ALL endpoints over TCP (the drain/shutdown mechanism itself lives in the start orchestration, T068; the single-instance bind lives in T060); unit tests written first
- [ ] T064 [P] [US2] Implement the OpenAPI schema in `crates/tinio-api/src/openapi.rs` (feature `openapi`): utoipa with `axum_extras` documenting all four endpoints and the token security scheme
- [ ] T065 [US2] Implement the status/stop client in `crates/tinio-api/src/client.rs`: reads `state` (channel + token), probes the control channel, calls `/status` / `/stop`; unit tests written first
- [ ] T066 [US2] Implement `tinio-cli` entry in `crates/tinio-cli/src/lib.rs`: `run()` + clap arg parsing for all commands per contracts/cli.md; storage-root walk-up discovery (nearest ancestor with `.tinio/`); global exit-code mapping; unit tests written first
- [ ] T067 [US2] Implement the `server`/`start` CLI surface in `crates/tinio-cli/src/commands/start.rs`: Minio-style positional DIR, `--address HOST:PORT` alias, `--host`/`--port` (default 9000, 0 = ephemeral), `--verbosity`/`--log-file`/`--log-format`, `--api <URL>` (repeatable, per-scheme override) / `--no-api-unix`, `--no-follow-symlinks`, `--read-only`; config auto-create with generated credentials; unit tests written first
- [ ] T068 [US2] Implement start-runtime orchestration in `crates/tinio-cli/src/commands/start.rs`: data plane + api plane wiring around a shared shutdown channel (cease accepting, graceful drain ≤ 10 s); `--daemon` detach (Windows: detached child; unix: detach with stderr → `server.log`/`server.json` per contracts/cli.md — the packaging systemd unit covers Linux service-style daemonization); readiness reporting (SC-005); non-loopback bind warnings (escalated with anonymous mode); unit tests written first
- [ ] T069 [US2] Implement signal handling in the start-runtime orchestration in `crates/tinio-cli/src/commands/start.rs`: SIGINT/SIGTERM (unix) and Ctrl+C / console-close events (Windows, via `SetConsoleCtrlHandler` — new `windows-sys` dependency, target-gated, user-approved, justification recorded per constitution I) trigger the same shared shutdown channel as `POST /stop` (cease accepting, drain ≤ 10 s, remove state/socket, exit 0); a second signal exits immediately without draining; SIGHUP is ignored and logged (no config reload in v1); SIGPIPE is already ignored by the Rust runtime; applies to `--daemon` children too (the systemd unit relies on SIGTERM); unit tests written first
- [ ] T070 [P] [US2] Implement `status` in `crates/tinio-cli/src/commands/status.rs`: reads `state` and probes the control channel via the T065 client; output (running|stopped, endpoint, root, PID, started time); unit tests written first
- [ ] T071 [P] [US2] Implement `stop` in `crates/tinio-cli/src/commands/stop.rs`: sends graceful stop via the T065 client, then polls the control channel until probe failure / `state` removal (bounded ~15 s) and reports unconfirmed exit on timeout; unit tests written first
- [ ] T072 [US2] Implement `doctor` diagnostics in `crates/tinio-cli/src/commands/doctor.rs`: root exists/readable/writable (skipped in read-only mode), config validity + resolvable credentials, `.tinio/` integrity (stale state/socket, orphaned meta + buckets.json entries, abandoned multipart, stale temps), on-disk bucket/object key validity incl. any-depth `.tinio`, symlinks present while disabled, low disk space warn; severity report (ok/warn/error) + `--json` output; unit tests written first
- [ ] T073 [US2] Implement `doctor --dry-run`/`--fix` in `crates/tinio-cli/src/commands/doctor.rs`: `--dry-run` lists exactly what a fix would change without touching anything; `--fix` applies the cleanups (stale state files/sockets, orphaned meta + buckets.json entries, abandoned multipart, stale temps, home root-state-dir GC) — server-stopped probe required (live server → error), never touches user data; unit tests written first
- [ ] T074 [US2] Expose `GET /metrics` in `crates/tinio-api/src/router.rs`: serve the registry injected from tinio-server; compute storage-layer gauges (bucket count; object count and total bytes) via the storage contract with the 30 s TTL cache — the full-scan gauges are registered and computed here (per the T023 boundary); Prometheus text format (SC-008); unit tests written first
- [ ] T075 [US2] Implement read-only state relocation (FR-023): state dir resolves to `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700, `dirs` crate) in read-only mode; root never written; pre-existing `<root>/.tinio/.tinio.toml` or `config.toml` read but never written (`.tinio.toml` wins); `.env` loaded from the state dir; affects tinio-fs (meta, buckets.json, multipart, tmp) and tinio-api (state, socket, logs); unit tests written first
- [ ] T076 [US2] Implement read-only enforcement across the S3 mapping groups in `crates/tinio-server/src/backend/`: every mutating operation (bucket create/delete, object put/delete/copy, all multipart ops) returns `AccessDenied`; read operations behave identically; unit tests written first
- [ ] T077 [P] [US2] Add the example systemd unit `packaging/tinio.service` (Type=simple, foreground) per contracts/cli.md

**Checkpoint**: User Stories 1 AND 2 both work — full SC-001 journey (start via CLI → S3 client operations → stop) completes in under 5 minutes; management plane + doctor + read-only mode verified; signal-driven shutdown (Ctrl+C / SIGINT / SIGTERM) behaves identically to `tinio stop`.

---

## Phase 5: User Story 3 - Authenticated Access (Priority: P3)

**Goal**: The server authenticates S3 requests with the standard SigV4 scheme using configured credentials (CLI flags > env > `.env` > config, with `MINIO_*` credential fallbacks); requests with missing or invalid signatures are rejected with the standard auth errors. An explicit anonymous mode (flag/env only — never a config key) skips auth entirely and wins over configured credentials with a warning. Session credentials are generated and printed once when the config exists without credentials and anonymous mode is off. `sig_v2` remains a deprecated opt-in runtime toggle.

**Independent Test**: Start the server with credentials, connect with a standard S3 client using those credentials and confirm operations succeed; connect with wrong credentials and confirm rejection (SignatureDoesNotMatch). Restart in anonymous mode and confirm operations succeed without credentials.

### Tests for User Story 3 (written FIRST, must FAIL before implementation)

- [ ] T078 [P] [US3] Integration test: SigV4 auth in `crates/tinio-server/tests/auth.rs`: correctly signed requests succeed; missing/invalid signatures → InvalidAccessKeyId / SignatureDoesNotMatch with no operation performed; credential rotation (edit config + restart → only new credentials accepted)
- [ ] T079 [P] [US3] Integration test: anonymous mode in `crates/tinio-server/tests/anonymous.rs`: explicit `--anonymous`/`TINIO_ANONYMOUS` wins over configured credentials with a warning; no-creds config without anonymous → session credentials generated and printed once; operations succeed without credentials in anonymous mode
- [ ] T080 [P] [US3] Interop scenario in `e2e/interop/auth.sh`: credentialed aws cli v2 + rclone flows against a server started with credentials; wrong-credentials rejection spot-check

### Implementation for User Story 3

- [ ] T081 [US3] Implement the auth provider in `crates/tinio-server/src/auth.rs`: `S3Auth` impl resolving the secret key from the resolved config (thin lookup; s3s performs SigV4/SigV2 verification); `check-bucket-name` framework feature retained (FR-012); unit tests written first
- [ ] T082 [US3] Wire auth into the data plane in `crates/tinio-server/src/data.rs`: build `S3Service` with the auth provider by default; anonymous mode builds it without (framework skips access checks); explicit anonymous switch overrides configured credentials with a logged warning (FR-009); unit tests written first
- [ ] T083 [US3] Extend `crates/tinio-config/src/sources.rs` with the `MINIO_*` credential fallback: `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` (legacy) and `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` (modern) accepted when the `TINIO_*` names are absent (minio-compat contract); unit tests written first
- [ ] T084 [US3] Implement the session-credential branch in `crates/tinio-config/src/credentials.rs`: config present without credentials and no anonymous mode → session credentials generated and printed once (daemon: into the log); unit tests written first
- [ ] T085 [US3] Implement the `sig_v2` runtime toggle in `crates/tinio-server/src/data.rs` (FR-021): `[s3] sig_v2 = false` default; enabling prints a startup warning (deprecated scheme, slated for removal in v2); unit tests written first
- [ ] T086 [US3] Wire the `--anonymous` flag through `crates/tinio-cli/src/commands/start.rs` and the env resolution in `crates/tinio-config/src/sources.rs` (`TINIO_ANONYMOUS`): flag/env-only semantics — an `anonymous` key in `[auth]` is rejected as unknown at startup; unit tests written first

**Checkpoint**: All user stories independently functional — authenticated and anonymous flows verified against aws cli v2 and rclone; error-code suite (SC-004) green including auth failures.

---

## Phase 6: Performance Testing & Verification

**Purpose**: Turn the performance properties promised in the spec into executable checks: the benchmark regression gate (constitution V), flat-memory streaming (SC-003), allocation discipline (constitution V), metric-recording overhead (FR-019), scanner efficiency on externally-populated trees (FR-022/024), and the timing criteria (SC-005/007). Cheap checks run in CI; expensive ones (1 GB transfers) run manually per quickstart §9. All measured values are recorded so trends are visible across releases.

- [ ] T087 [P] Record criterion benchmark baselines and enforce the regression gate: after US1 lands, run the full bench suite from T030/T031 with criterion `--save-baseline` and commit the recorded values as tracked data (e.g. `benches/baselines.json`); extend `.github/workflows/ci.yml` with a PR-time bench-comparison job; mean slowdown > 10 % vs baseline counts as a regression requiring a documented decision and reviewer approval (constitution V)
- [ ] T088 [P] Implement the SC-003 flat-memory verification script `e2e/perf/sc003-flat-memory.sh`: 1 GB upload + download against a running server with RSS sampling (ps on unix, Get-Process on Windows), asserting flat memory — RSS growth stays within a bounded delta regardless of object size; manual run, documented in quickstart §9
- [ ] T089 [P] Implement the CI streaming-memory smoke `e2e/perf/ci-streaming-memory.sh`: ~128 MB upload/download round-trip with RSS sampling and a generous bound (e.g. < 256 MB growth), wired into the CI interop stage on the 3-OS matrix; cheaply catches full-object buffering regressions
- [ ] T090 [P] Implement the allocation-discipline verification with dhat (the `dhat` crate, tinio-fs dev-dependency — user-approved; the dependency justification is recorded per constitution I): heap-profile a streaming put/get round-trip through the fs backend in `crates/tinio-fs/tests/allocations.rs` (dhat `Allocator`/`Profiler`), asserting bounded allocations — no per-object buffers on streaming hot paths (constitution V allocation discipline)
- [ ] T091 [P] Implement the metric-recording overhead benchmark `crates/tinio-server/benches/metrics_overhead.rs`: identical request workload with and without Prometheus recording; record the overhead baseline and subject it to the T087 regression gate (FR-019: metric recording MUST NOT measurably degrade request handling)
- [ ] T092 [P] Implement the cold-vs-warm listing benchmark `crates/tinio-server/benches/cold_warm_listing.rs`: generated externally-populated tree (thousands of objects) — first listing (synchronous ETag recompute) vs warm listing (scanner-completed meta store); documents the FR-022 one-time cold cost and the FR-024 scanner benefit
- [ ] T093 Implement timing-criteria measurement: record time-to-ready (SC-005) and `status`/`stop` round-trip (SC-007) on the CI matrix and a typical dev machine, captured into the benchmark report so timing trends are visible across releases (functional ≤ 1 s assertions already live in T056)

**Checkpoint**: Every promised performance property is verified and recorded — baselines committed and regression-gated, memory/allocation/overhead/cold-listing checks green, timing values captured.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories; release-readiness per the constitution

- [ ] T094 [P] Maintain `CHANGELOG.md` at the repository root (constitution VI: public API changes recorded; generated via git-cliff); record the MSRV policy (current stable — the constitution-VI interpretation from plan.md) at the first release
- [ ] T095 Create integration tests in `crates/tinio/tests/` against the facade public API: curated re-exports present and usable, `tinio_cli::run()` dispatch, facade error re-exports; acts as the local baseline for the semver-checks contract (the public API is final once US2 lands)
- [ ] T096 Run quickstart.md validation end-to-end: all 10 scenarios (build/test gate, aws cli v2 journey, multipart/copy, management plane, auth/error codes, rclone, crash recovery, doctor, zero-byte/large objects, read-only mode) against a scratch root
- [ ] T097 [P] Verify the feature-matrix behavior contracts: `--no-default-features` and all-default builds pass the full test suite (the remaining feature combinations are covered by the CI compile checks added in T009); feature-off behavior contracts hold — CLI options absent, `[api]`/`[s3]` keys silently ignored, stripped ops → NotImplemented
- [ ] T098 [P] Security hardening review: dependency audit (`cargo audit`), secret-bearing file permissions, layered-trust warning behavior, access-log variable-set security property re-check
- [ ] T099 [P] Constitution compliance review: no `unwrap`/`expect`/`panic!` in library paths, `unsafe_code = "forbid"` everywhere, rustdoc examples on all public items, semver-checks green, doc links valid
- [ ] T100 [P] Write the user manual `docs/user-manual.md` (markdown): installation, configuration reference (all `[server]`/`[scanner]`/`[auth]`/`[log]`/`[s3]`/`[storage]`/`[api]`/`[telemetry]` keys, env variables, precedence), CLI reference (all commands, flags, exit codes), read-only mode, security notes (layered trust, tokens, credentials), troubleshooting; English per the language policy
- [ ] T101 [P] Write the usage tutorial `docs/tutorial.md` (markdown): quick start (install → `tinio server <dir>` → aws cli v2 / rclone setup), typical workflows (bucket/object operations, multipart, server-side copy), management plane (`status`/`stop`/`doctor`, `/metrics`, `/openapi.json`), common scenarios (ephemeral ports, daemon mode, systemd unit, read-only serving); English per the language policy
- [ ] T102 [P] Language-policy cross-check: all docs (including the new `docs/` manual and tutorial), comments, and commit messages in English (per CLAUDE.md)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational completion
  - US1 (P1) → US2 (P2) → US3 (P3) in priority order (US2 needs the data plane from US1; US3 needs US1's data plane and US2's credential generation)
- **Performance (Phase 6)**: Depends on US1 (the bench suite, T030/T031) and US2 (the running server for timing checks); its baseline/regression tasks can start as soon as US1 lands, independently of US3
- **Polish (Final Phase)**: Depends on all desired user stories AND the Performance phase being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — no dependencies on other stories
- **User Story 2 (P2)**: Depends on US1 (the start command wires the data plane) — independently testable once US1 lands
- **User Story 3 (P3)**: Depends on US1 (auth guards the data plane) and US2 (credential generation) — independently testable once those land

### Within Each User Story

- Tests MUST be written and FAIL before implementation (constitution IV)
- Filesystem modules before services: path → write/meta/buckets → backend (bucket ops, object ops) → listing/multipart; scanner/sweep last within the story
- The four S3 mapping groups (T047–T050) can be implemented against the storage contract as soon as Phase 2 lands; they share the `backend/` module directory but are independent files
- Signal handling (T069) depends on the shared shutdown channel from the start orchestration (T068)
- Story complete and validated at its checkpoint before moving to the next priority

### Parallel Opportunities

- All Setup tasks marked [P] (crate skeletons) can run in parallel
- All Foundational tasks marked [P] (domain, keys, error types, sources) can run in parallel
- Once Foundational completes, US1's test tasks ([P]) can run in parallel — including the third-party client tasks (T034–T036)
- Within US1: write.rs / meta.rs / buckets.rs ([P]) after path.rs; the two fs backend groups ([P]); the four S3 mapping groups ([P], against the storage contract); the OTel task (T053) after log.rs (T052); scanner.rs and sweep.rs ([P]) can run in parallel
- Within US2: test tasks ([P]); openapi.rs ([P]) alongside the router; status (T070) and stop (T071) are [P] in different files; systemd unit ([P])
- Within US3: all test tasks ([P])
- Phase 6 (Performance): all tasks marked [P] can run in parallel once US1's bench suite (T030/T031) exists; T093 additionally needs US2
- Phase 7 (Polish): docs tasks (T100, T101) are [P] and independent of the other checks
- Different user stories can be worked on in parallel by different team members once their dependencies land

---

## Parallel Example: User Story 1

```bash
# Launch all US1 test suites together (must fail first):
Task: "Contract/integration test: S3 error-code behavior in crates/tinio-server/tests/error_codes.rs"
Task: "Integration test: full data-plane round-trip in crates/tinio-server/tests/data_plane.rs"
Task: "Integration test: reserved-path behavior in crates/tinio-server/tests/reserved_paths.rs"
Task: "Property tests for key validation in crates/tinio-core/tests/keys.rs"

# Launch the two bench suites together:
Task: "Criterion benchmarks for the fs backend in crates/tinio-fs/benches/"
Task: "Criterion benchmarks for the S3 mapping layer in crates/tinio-server/benches/"

# Launch the third-party client scenarios (journey first, then the rest):
Task: "Interop core-journey scenario and shared harness in e2e/interop/journey.sh"
Task: "boto3 basic-journey scenario in e2e/interop/boto3.sh"
Task: "mc (MinIO Client) basic-journey scenario in e2e/interop/mc.sh"
Task: "Client coverage matrix in e2e/interop/README.md"

# Launch the independent storage modules together (after path.rs):
Task: "Implement streaming atomic writes in crates/tinio-fs/src/write.rs"
Task: "Implement the ETag meta store in crates/tinio-fs/src/meta.rs"
Task: "Implement bucket creation times in crates/tinio-fs/src/buckets.rs"

# Launch the four S3 mapping groups together (against the storage contract):
Task: "Implement the S3 buckets group in crates/tinio-server/src/backend/buckets.rs"
Task: "Implement the S3 objects group + copy in crates/tinio-server/src/backend/objects.rs"
Task: "Implement S3 listing V1/V2 in crates/tinio-server/src/backend/listing.rs"
Task: "Implement the S3 multipart group in crates/tinio-server/src/backend/multipart.rs"

# Launch scanner and sweep together:
Task: "Implement the background ETag scanner in crates/tinio-fs/src/scanner.rs"
Task: "Implement the async sweep in crates/tinio-fs/src/sweep.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 + User Story 2 — the S3 data plane + CLI management)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 → STOP and VALIDATE independently (data-plane harness + interop)
4. Complete Phase 4: User Story 2 → STOP and VALIDATE
5. **MVP = US1 + US2 together**: the SC-001 full journey (`tinio server <dir>` → S3 client operations → `tinio stop`) completes in under 5 minutes. US1 alone has no user-facing entry point — the production CLI arrives with US2, so pair them for the first demoable milestone.
6. Add User Story 3 → Deploy/Demo (authenticated + anonymous flows)

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently (core S3 serving via integration harness)
3. Add User Story 2 → Test independently → Deploy/Demo (SC-001 full journey — the MVP!)
4. Add User Story 3 → Test independently → Deploy/Demo (authenticated + anonymous flows)
5. Run Phase 6 Performance Verification (baselines after US1, memory/allocation/overhead checks) before final polish
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 storage modules (tinio-fs)
   - Developer B: User Story 1 S3 mapping + wiring (tinio-server)
   - Developer C: User Story 2 CLI/management (after US1 data plane stabilizes)
3. Stories complete and integrate independently; US3 slots in last

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing (constitution IV)
- Commit after each task or logical group
- Stop at any checkpoint to validate the story independently
- Avoid: vague tasks, same-file conflicts, cross-story dependencies that break independence
- Test tasks are included because the project constitution (Principle IV) mandates test-first development; the plan.md §Testing matrix (unit/doc/proptest/criterion/interop) is the authoritative coverage contract
- The one cross-story exception is read-only mode (FR-023): its data-plane rejections live in US2 because the CLI flag, home state dir, and state relocation are management-plane infrastructure
- Phase 6 turns the spec's performance properties into executable checks; the allocation-discipline check uses dhat (tinio-fs dev-dependency, user-approved — constitution I justification recorded) and asserts constitution V allocation discipline
- The boto3/mc scenarios (T034/T035) are targeted/manual per FR-025's best-effort tier — promoting them into the CI interop gate is an FR-025 amendment (release-gating contract) requiring spec approval
- `backend.rs` from plan.md is implemented as a `backend/` module directory (mod.rs + one file per operation group) so the split S3 mapping tasks and fs backend tasks are genuinely parallelizable; the module name `backend` is unchanged
- OTel support (T053) is an explicit task behind the opt-in `otel` feature; the `[telemetry]` config key is validated in Phase 2 (T016/T017) and the exporter is consumed in US1
- The interop harness (T032) spawns the server through the tinio-server example binary (`examples/serve.rs`) during US1 and switches to the facade binary once US2 lands — resolving the US1-interop vs US2-binary ordering dependency
- The SigV4 clock-skew window (±15 min per AWS convention, spec §Assumptions) is verified and recorded during implementation (T081), per the spec's implementation-notes commitment
- Signal handling (T069) is part of the start-runtime orchestration: SIGINT/SIGTERM and Windows console-close events share the `POST /stop` shutdown path; the `windows-sys` dependency (cfg(windows), console events) is user-approved with the constitution I justification recorded in the spec
