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

- **Workspace**: eight crates under `crates/` (`tinio`, `tinio-core`, `tinio-mem`, `tinio-fs`, `tinio-config`, `tinio-server`, `tinio-api`, `tinio-cli`); workspace root `Cargo.toml` at the repository root
- **Interop tests**: `e2e/interop/` (third-party S3 client scenarios — mandated clients CI-gated, best-effort clients targeted/manual, per FR-025)
- **Performance scripts**: `e2e/perf/` (streaming-memory and flat-memory verification scripts)
- **User docs**: `docs/` (user manual + usage tutorial, markdown)
- **Packaging**: `packaging/tinio.service`
- **CI**: `.github/workflows/ci.yml`
- **Backend modules**: the S3 mapping and the filesystem `Storage` impl live in `backend/` module directories (`src/backend/mod.rs` + one file per operation group) — the module name `backend` is unchanged from plan.md

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Workspace initialization, crate skeletons with all dependencies and cargo features per plan.md §Project Structure

- [X] T001 Create workspace root `Cargo.toml` (members `crates/*`, edition 2024, shared workspace dependencies: thiserror, serde, tokio, tracing, tracing-subscriber, time, uuid, md-5, clap, dotenvy, dirs, mime_guess, tempfile/proptest/criterion as dev deps) and `.gitignore` (target/, etc.)
- [X] T002 [P] Create `crates/tinio-core` skeleton: Cargo.toml (deps: async-trait, bytes, derive_more, futures, garde, hex, md-5, thiserror, tokio; feature `testing`, off by default; `unsafe_code = "forbid"`) and `src/lib.rs` module layout
- [X] T003 [P] Create `crates/tinio-fs` skeleton: Cargo.toml (deps: tinio-core, thiserror, time, uuid, tokio; dev-deps: tinio-core/testing, tempfile, proptest, criterion, dhat; `unsafe_code = "forbid"`) and `src/lib.rs` + `src/error.rs` (`md-5` added in US1 when the meta store lands)
- [X] T003a [P] Create `crates/tinio-mem`: Cargo.toml (deps: tinio-core, async-trait, bytes, futures, redb, tokio; dev-deps: tinio-core/testing; `unsafe_code = "forbid"`) and full module layout (`storage`, `bucket`, `object`, `multipart`, `cleanup`, `error`) — `MemoryStorage` over redb's `InMemoryBackend`, conformance harness green
- [X] T004 [P] Create `crates/tinio-config` skeleton: Cargo.toml (deps: serde, serde_json, toml, dotenvy, dirs; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [X] T005 [P] Create `crates/tinio-server` skeleton: Cargo.toml (deps: s3s, hyper, hyper-util, tokio-util, mime_guess, prometheus, tracing, tinio-core, tinio-config; optional dep tinio-api behind feature `api`; default-on features `multipart`, `copy`, `list-v1`, `list-v2`; opt-in feature `otel` = optional deps opentelemetry + opentelemetry-otlp + tracing-opentelemetry; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [X] T006 [P] Create `crates/tinio-api` skeleton: Cargo.toml (deps: axum, prometheus, tinio-core; feature `openapi` = utoipa with `axum_extras`, feature `tls` = tokio-rustls + rustls-pemfile; `unsafe_code = "forbid"`) and empty `src/lib.rs`
- [X] T007 [P] Create `crates/tinio-cli` skeleton: Cargo.toml (deps: clap, tinio-config, tinio-core, tinio-fs, tinio-server; optional deps tinio-api (`api`) and tinio-mem (`mem`, default on); target-gated `windows-sys` (cfg(windows), `Win32_System_Console`) for console-close event handling (T069); `unsafe_code = "forbid"`) and empty `src/lib.rs` + `src/commands/` directory
- [X] T008 [P] Create `crates/tinio` facade skeleton: Cargo.toml (deps: tinio-core, tinio-config, tinio-server, tinio-cli; optional tinio-api behind feature `api`; default features `api` + `openapi` + `tls` + `multipart` + `copy` + `list-v1` + `list-v2`; opt-in passthrough feature `otel` = `tinio-server/otel`; `unsafe_code = "forbid"`), thin `src/main.rs` delegating to `tinio_cli::run()`, `src/lib.rs` with curated re-exports (rustdoc examples per constitution III), `src/error.rs`
- [X] T009 [P] Create CI workflow `.github/workflows/ci.yml`: Windows/Linux/macOS matrix on latest stable; quality gates (fmt --check, clippy `-D warnings`, `cargo test --workspace` incl. `--no-default-features`, `cargo doc` no warnings, semver-checks on facade, audit); feature-matrix compile checks (explicit feature-combination list or cargo-hack, `cargo check` level — catches feature-gate breakage early); interop stage (aws cli v2 + rclone)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story: the `tinio-core` storage contract, the configuration schema, per-crate error types, and the metrics registry. **No user story work can begin until this phase is complete.**

- [X] T010 Implement `storage::Error` (thiserror) in `crates/tinio-core/src/storage.rs`: backend-agnostic domain errors (NoSuchBucket/NoSuchKey/NoSuchUpload, AlreadyExists, NotEmpty, InvalidKey, InvalidBucketName, AccessDenied, Unsupported, transparent Io), with rustdoc examples on public items (constitution III); unit tests written first
- [X] T011 [P] Implement domain types in `crates/tinio-core/src/{bucket,object,etag,multipart}.rs`: `bucket::Name` + `Bucket`, `object::Key` + `object::Info`, `ETag`, `MultipartUpload` + `PartInfo` (Send + Sync + 'static), with rustdoc examples on all public items (constitution III); unit tests written first
- [X] T012 Implement the storage contract in `crates/tinio-core/src/storage.rs` + `crates/tinio-core/src/cleanup.rs`: the async `Storage` trait (bucket/object/multipart operations per data-model.md) and the `Cleanup` trait (startup repair, orphan reclamation, doctor diagnostics/fix — backend-specific cleanup behind the contract seam, per failure-handling.md); backend-agnostic, with rustdoc examples on all public items (constitution III); unit tests written first
- [X] T013 [P] Implement backend-agnostic validation in `crates/tinio-core/src/bucket.rs` + `crates/tinio-core/src/object.rs`: traversal (`..`) / absolute-path / control-character rejection (FR-006), bucket-name rules (FR-012), `.tinio` reserved-segment rule at ANY depth (FR-020) via `object::Key::is_reserved`; unit tests written first
- [X] T014 Implement conformance test harness in `crates/tinio-core/src/testing.rs` behind the `testing` feature: every backend implementation must pass it
- [X] T015 Implement `Error` in `crates/tinio-config/src/error.rs` (parse/validation failures), with rustdoc examples on public items (constitution III); unit tests written first
- [X] T016 Implement the `Config` struct in `crates/tinio-config/src/schema/mod.rs`: `version = 1` + `[server]` `[scanner]` `[auth]` `[log]` `[s3]` `[storage]` `[api]` (`unix`/`pipe`/`http`/`https` subsections) `[telemetry]` sections per contracts/config.md, with rustdoc examples on public items; unit tests in `schema/tests.rs`; unit tests written first
- [X] T017 Implement fail-fast validation in `crates/tinio-config/src/schema/mod.rs`: unknown keys/sections rejected; fixed access-log format variable set enforced (closed set — no Authorization/query/credentials, per FR-017); presence-gated sections (`[scanner]`, `[api.*]`); port rules (default 9000, 0 = ephemeral); `[api.https]` requires cert+key; the management transports are mutually exclusive (three-choose-one per contracts/config.md, making a ports-differ rule moot); boolean key typing; credential presence rules; unit tests written first
- [X] T018 [P] Implement source precedence resolution in `crates/tinio-config/src/sources.rs`: CLI flags > process env > `.env` (via dotenvy) > config file (FR-016); `.env` loaded from the state dir. This task lands the `.env` loading; the overlay itself needs no code here — dotenvy never overrides process env, and the CLI/env layers are clap `env` attributes applied over the config-file base by the CLI tasks (T066/T067); unit tests written first
- [X] T019 [P] Implement `Error` in `crates/tinio-fs/src/error.rs`: io + domain mapping, `From`-conversion into `tinio_core::storage::Error`, with rustdoc examples on public items (constitution III); unit tests written first
- [X] T020 [P] Implement `Error` in `crates/tinio-server/src/error.rs`: startup + S3-mapping failures, with rustdoc examples on public items (constitution III); unit tests written first
- [X] T021 [P] Implement `Error` in `crates/tinio-api/src/error.rs`: maps to HTTP status + JSON error bodies (401/404/500 per management-api contract), with rustdoc examples on public items (constitution III); unit tests written first
- [X] T022 [P] Implement `Error` in `crates/tinio-cli/src/error.rs`: user-facing messages + exit codes (0 success / 1 operational / 2 usage), with rustdoc examples on public items (constitution III); unit tests written first
- [X] T023 Implement the Prometheus registry and metric family definitions in `crates/tinio-server/src/metrics.rs`: `tinio_http_*`, `tinio_s3_*`, `tinio_storage_*` families per data-model.md Metrics section (names, labels, help strings). All families — including the full-scan gauges — are registered here; the gauges' TTL-cached (30 s) values are computed later in T075; unit tests written first

**Checkpoint**: **Foundation ready — Phase 2 complete (T010–T023).** Storage contract (`Storage` + `Cleanup`), config schema + `.env` loading, conformance harness, `tinio-mem::MemoryStorage` (conformance green), per-crate error types, and the Prometheus registry all exist. User story implementation (US1) may begin.

---

## Phase 3: User Story 1 - Serve Local Directories Over an S3-Compatible Interface (Priority: P1) — MVP CORE

**Goal**: The data plane serves an S3-compatible interface over the local filesystem: buckets map to top-level subdirectories, objects to files; create/delete buckets, upload/download/delete/list objects work with standard S3 clients (aws cli v2, rclone, boto3, mc) with no client-side workarounds. Includes streaming (FR-010), atomic writes (FR-011), ETag metadata (FR-022), multipart (FR-014), server-side copy (FR-015), listing semantics (FR-004), error codes (FR-005), key validation (FR-006), `.tinio` reservation (FR-020), background ETag scanner (FR-024), and the async sweep.

**Independent Test**: Start the data plane against an empty directory (via the tinio-server integration harness), then use a standard S3 client to create a bucket, upload a file, download it back (byte-identical), list objects with prefix/delimiter, and delete the object. Verify the uploaded file physically appears in the local directory and that a file dropped into the directory by hand is immediately retrievable through the client (SC-006). Files served with correct ETag (MD5), Content-Type, and error codes (SC-004). Multipart and copy operations complete without client passthrough. The basic scenario set is exercised through EACH third-party tool per the client matrix (T036).

### Tests for User Story 1 (written FIRST, must FAIL before implementation)

- [X] T024 [P] [US1] Contract/integration test: S3 error-code behavior (SC-004) in `crates/tinio-server/tests/error_codes.rs`: NoSuchBucket, NoSuchKey, InvalidBucketName, BucketAlreadyExists, BucketNotEmpty, NotImplemented, traversal rejection with no FS access
- [X] T025 [P] [US1] Integration test: full data-plane round-trip in `crates/tinio-server/tests/data_plane.rs`: create bucket / upload / download (byte-identical) / list (prefix + delimiter grouping, pagination) / delete; zero-byte objects; nested keys; concurrent writes last-write-wins with no torn objects; interrupted upload leaves no partial object; Range requests (206/Content-Range); conditional requests (304/412); folder markers (`dir/` never an object); out-of-band file changes served immediately (reserved-path behavior is tested in T026)
- [X] T026 [P] [US1] Integration test: reserved-path behavior in `crates/tinio-server/tests/reserved_paths.rs`: any-depth `.tinio` segments — write → AccessDenied, read → NoSuchKey, listings skip; nested-root scenario (an inner root's state is never served by an outer server); distinct file from T025 so the security surface is independently reviewable
- [X] T027 [P] [US1] Property tests for key validation and path mapping in `crates/tinio-core/tests/validation.rs` (proptest): traversal sequences, absolute paths, control characters, `.tinio` segments at any depth, bucket-name rules (exercise `bucket::name` / `object::key`)
- [X] T028 [P] [US1] Property tests for the meta store in `crates/tinio-fs/tests/proptest_meta.rs`: ETag entry round-trips, size/mtime mismatch → recompute, atomic-write concurrency (no torn JSON)
- [X] T029 [P] [US1] Property tests for multipart assembly in `crates/tinio-fs/tests/proptest_multipart.rs`: arbitrary part counts/sizes assemble to the exact concatenation, composed ETag `MD5-of-MD5s-N` matches reference implementation
- [X] T030 [P] [US1] Criterion benchmarks for the fs backend in `crates/tinio-fs/benches/`: streaming write/read throughput on the bounded-buffer paths; smoke run in CI; baselines recorded and regression-gated in Phase 6 (T088)
- [X] T031 [P] [US1] Criterion benchmarks for the S3 mapping layer in `crates/tinio-server/benches/`: multipart assembly, prefix/delimiter listing; smoke run in CI; baselines recorded and regression-gated in Phase 6 (T088)
- [X] T032 [US1] Interop core-journey scenario and shared harness in `e2e/interop/journey.sh` (+ common runner): aws cli v2 + rclone full journey (mb/cp/ls/rm/rb) against `127.0.0.1` with no client-side addressing overrides (SC-002), plus `--port 0` ephemeral-port runs; CI-gated (mandated per FR-025). The harness spawns the server via the minimal example binary `crates/tinio-server/examples/serve.rs` during US1 and switches to the facade binary once the CLI lands in US2
- [X] T033 [US1] Interop advanced scenarios in `e2e/interop/` (reusing the T032 harness): multipart upload (>8 MB file → composed ETag pattern), server-side copy, cold-listing with and without scanner — via aws cli v2 + rclone; CI-gated (mandated per FR-025)
- [X] T034 [P] [US1] boto3 basic-journey scenario in `e2e/interop/boto3.sh`: the SC-001 basic scenario set via the boto3 SDK (best-effort client per FR-025 — targeted/manual, NOT CI-gated): create bucket, upload, download byte-identical, list with prefix/delimiter, delete object, zero-byte round-trip, multipart via `upload_file` (>8 MB → composed ETag pattern); documents known deviations (x-amz-checksum-* ignored, x-amz-meta-* dropped, Content-Type inferred at serve time); requires Python 3 + boto3 installed for targeted runs
- [X] T035 [P] [US1] mc (MinIO Client) basic-journey scenario in `e2e/interop/mc.sh`: the SC-001 basic scenario set via `mc` (best-effort client per FR-025 — targeted/manual, NOT CI-gated): mb/cp/ls/rm/rb, large-file copy (multipart), `mc stat` ETag check, zero-byte object; documents known deviations; requires the mc binary installed for targeted runs
- [X] T036 [P] [US1] Client coverage matrix in `e2e/interop/README.md`: map every third-party tool (aws cli v2, rclone — mandated/CI-gated; boto3, mc — best-effort/targeted-manual; unsupported list per FR-025) to the basic scenarios it exercises (bucket create/delete, upload, download, list, multipart, copy, auth), so FR-025 coverage is verifiable at a glance

### Implementation for User Story 1

- [X] T037 [US1] Implement path mapping in `crates/tinio-fs/src/path.rs`: bucket/key → filesystem path, traversal rejection before any FS access, platform charset rules (Windows-invalid chars on Windows only), `.tinio` any-depth reservation, case sensitivity follows host FS; unit tests written first
- [X] T038 [P] [US1] Implement streaming atomic writes in `crates/tinio-fs/src/write.rs`: temp file under `<state-dir>/tmp/` + `fs::rename`, bounded buffers, last-write-wins (FR-010/011); unit tests written first
- [X] T039 [P] [US1] Implement the ETag meta store in `crates/tinio-fs/src/meta.rs`: git-style 2-hex fan-out (`meta/objects/<bucket>/<2hex>/<sha1>.json` = `{key, etag, size, mtime}`), atomic writes under an in-process lock, size/mtime validation with streaming recompute on mismatch (FR-022); unit tests written first
- [X] T040 [P] [US1] Implement bucket creation times in `crates/tinio-fs/src/buckets.rs`: `buckets.json` (`{"version": 1, "buckets": {...}}`), atomic temp+rename under an in-process lock, lazy recording on first sight, orphan cleanup on bucket delete; unit tests written first
- [X] T041 [P] [US1] Implement bucket operations of the `Storage` impl in `crates/tinio-fs/src/backend/buckets.rs`: create (with S3-name re-validation), delete-only-when-empty → BucketNotEmpty, head, list; MUST pass the tinio-core conformance harness; unit tests written first
- [X] T042 [P] [US1] Implement object operations of the `Storage` impl in `crates/tinio-fs/src/backend/objects.rs`: put/get/head/delete with streaming, Range seek, folder-marker semantics — keys ending in `/` never become objects: PUT creates the directory, GET/HEAD → NoSuchKey, DELETE removes an empty directory and always returns 204; symlink policy — follow by default, exclude/reject when disabled; MUST pass the tinio-core conformance harness; unit tests written first
- [X] T043 [US1] Implement listing in `crates/tinio-fs/src/listing.rs`: prefix filtering, delimiter-based grouping (common-prefix roll-up), pagination per S3 semantics (FR-004); ETags included — missing/stale entries recomputed synchronously during listing; `.tinio` entries always skipped; unit tests written first
- [X] T044 [US1] Implement multipart storage in `crates/tinio-fs/src/multipart.rs`: parts at `<state-dir>/multipart/<bucket>/<uploadId>/part-<n>`, streaming assembly into temp + atomic rename, composed ETag, abort removes the parts subtree, no 5 MB minimum (FR-014); unit tests written first
- [X] T100 [P] redb metadata migration (meta-redb-spec tasks 1–8; supersedes T039/T040/T044's file layout): all derived metadata → `<state-dir>/meta.redb` (OBJECT_META/BUCKETS/UPLOADS/PARTS/STATE tables, one shared `DbHandle`); per-store in-process locks removed; `FsCleanup` gains the no-`UPLOADS`-record upload-dir stage and `check_integrity`; offline compact (fragmentation threshold `[storage.fs] compact_threshold_percent` + 64 MiB floor + `compact_needed` marker); `serde`/`serde_json`/`sha1` deps removed; read-only state-relocation fs test (root zero writes); layout/self-heal integration tests
- [X] T045 [US1] Implement the background ETag scanner in `crates/tinio-fs/src/scanner.rs` (FR-024): low-priority task pre-computing missing/stale meta entries, paced by `[scanner]` delay/max_wait/cycle, presence-gated (section absent = off) with the `TINIO_SCANNER` env toggle (0/1) as an independent override, yields to request traffic, never blocks startup, aborts quietly on shutdown; additionally reclaims meta orphans — meta entries whose object file no longer exists are deleted during the scan through the `Cleanup` trait (T012; the startup repair T070 handles the fast, deterministic items); design per scanner.md; unit tests written first
- [X] T046 [US1] Implement the async sweep in `crates/tinio-fs/src/sweep.rs` (FR-014): mtime-based cleanup of stale temp files (default 24 h, `[s3] temp_ttl_hours`) and abandoned multipart uploads (default 7 days, `[s3] multipart_expire_days`); non-blocking, yields to request traffic; unit tests written first
- [X] T047 [P] [US1] Implement the S3 buckets group in `crates/tinio-server/src/backend/buckets.rs`: CreateBucket/DeleteBucket/HeadBucket/ListBuckets/GetBucketLocation (CreationDate from the fs backend's BUCKETS table, GetBucketLocation → `us-east-1`); storage errors → S3 error codes (NoSuchBucket, BucketAlreadyExists, BucketNotEmpty, InvalidBucketName); unit tests written first
- [X] T048 [P] [US1] Implement the S3 objects group + copy in `crates/tinio-server/src/backend/objects.rs`: PutObject/GetObject/HeadObject/DeleteObject/DeleteObjects/CopyObject; Range + conditional headers per s3-surface contract (206/Content-Range, 304/412); Content-Type inferred via mime_guess; `x-amz-meta-*` and `x-amz-checksum-*` accepted and dropped; CopyObject behind `copy` feature, runtime `[s3]` toggles (copy_object, delete_objects) → NotImplemented; unit tests written first
- [X] T049 [P] [US1] Implement S3 listing V1/V2 in `crates/tinio-server/src/backend/listing.rs`: ListObjects (V1) + ListObjectsV2 (V2) over the tinio-fs listing — prefix filtering, delimiter grouping, pagination per S3 semantics (FR-004); shared listing core with separate XML surfaces; compile-time gates `list-v1`/`list-v2` and runtime `[s3]` toggles → NotImplemented; unit tests written first
- [X] T050 [P] [US1] Implement the S3 multipart group in `crates/tinio-server/src/backend/multipart.rs`: CreateMultipartUpload/UploadPart/UploadPartCopy/CompleteMultipartUpload/AbortMultipartUpload/ListParts/ListMultipartUploads; composed ETag `MD5-of-MD5s-N`; UploadPartCopy additionally gated by `copy` feature; part numbers validated 1..=10000 per data-model (invalid → InvalidPart); compile-time `multipart` gate and runtime `[s3]` toggle → NotImplemented; unit tests written first
- [X] T051 [US1] Wire the data plane in `crates/tinio-server/src/data.rs`: hyper + hyper-util hosting of `S3Service` (tower::Service), path-style addressing, streaming bodies with bounded buffers; unit tests written first
- [X] T052 [US1] Implement logging layers in `crates/tinio-server/src/log.rs`: access-log tracing layer (nginx-style `combined`/`common`/custom format strings over the fixed variable set, target `tinio::access`) writing to `access.log` in the state dir; operational text/json fmt layers (errors always visible on stderr, FR-017); unit tests written first
- [X] T053 [US1] Implement the OpenTelemetry export layer (FR-017 opt-in export) behind the `otel` feature in `crates/tinio-server/src/log.rs` + `crates/tinio-server/src/data.rs`: OTLP exporter construction from `[telemetry] otlp_endpoint` (fallback to `OTEL_EXPORTER_OTLP_ENDPOINT`), `tracing-opentelemetry` layer registration, shutdown/export hygiene on server exit; enabled via the facade `otel` passthrough (T008) or `-p tinio-server --features otel`; unit tests written first
- [X] T054 [US1] Implement metrics instrumentation in `crates/tinio-server/src/metrics.rs`: `MetricS3` delegation wrapper recording `tinio_s3_operations_total{op,status}` + duration; HTTP middleware recording request count/duration/in-flight; upload/download byte counters maintained on streaming paths; in-progress multipart gauge; unit tests written first

**Checkpoint**: User Story 1 fully functional — the data plane serves the complete v1 S3 surface against aws cli v2 and rclone (CI-gated), with boto3 and mc basic journeys verified manually; error-code suite green; streaming flat-memory verification runs in Phase 6 (T089/T090); scanner + sweep operational.

---

## Phase 4: User Story 2 - Basic CLI Management (Priority: P2)

**Goal**: The user manages the server from the command line: `tinio server <dir>` / `start` (Minio-style positional directory, config auto-created on first start with generated credentials), `status`, `stop` (graceful, in-flight drain ≤ 10 s, stop-wait confirmation), `doctor` (offline diagnostics with `--json`/`--dry-run`/`--fix`); the management plane (unix socket / Windows named pipe / optional TCP HTTP(S), token-authenticated) exposes `/status`, `/stop`, `/metrics`, `/openapi.json`; single-instance enforcement via the control-channel bind; read-only mode (FR-023); correct handling of common signals in foreground server mode; automatic repair of orphaned state left by crashes or forced kills at startup, with the scanner reclaiming invalid files in the background and `doctor` checking the same problems.

**Independent Test**: Start the server against a directory, run `status` (reports running, endpoint, root, PID — round-trip < 1 s, SC-007), create subdirectories and files by hand and verify they are served through the S3 interface, then `stop` and verify a clean shutdown with no partial files. A second `server` on the same root fails with the single-instance error. `GET /metrics` returns the three-layer metric set with TTL-cached gauges (SC-008). Pressing Ctrl+C (or sending SIGTERM) shuts the server down gracefully with state/socket removed and no partial files. After a forced kill, restarting repairs the orphaned private state (tmp leftovers, stale bucket records, bucket-orphaned multipart, no-record upload dirs) before readiness.

### Tests for User Story 2 (written FIRST, must FAIL before implementation)

- [ ] T055 [P] [US2] Integration test: lifecycle in `crates/tinio-cli/tests/lifecycle.rs`: start (custom port) → status → stop round-trips < 1 s (SC-007); startup readiness reached ≤ 1 s of the start command (SC-005); config auto-created with generated credentials; `--port 0` ephemeral port reported in logs/state; second instance → single-instance error exit 1; stop-wait confirmation and timeout path; `--daemon` detaches; feature-off builds omit `status`/`stop` subcommands; signal handling: SIGINT/SIGTERM (unix) / Ctrl+C (Windows) → graceful shutdown (state/socket removed, exit 0, no partial files); second signal → immediate exit; SIGHUP ignored (unix); crash-recovery startup repair: with stale state/socket, stale bucket records, temp-file leftovers, a bucket-orphaned multipart subtree and a no-record upload dir on disk, a restart removes them before readiness and never touches user data; orphaned meta entries are reclaimed by the background scanner (failure-handling.md §3)
- [ ] T056 [P] [US2] Integration test: management plane in `crates/tinio-api/tests/management.rs`: `/status` (token) and `/stop` (202 draining, bounded 10 s drain); `/metrics` and `/openapi.json` open on the local channel but require the token over TCP (401 without); Windows named pipe transport with `FILE_FLAG_FIRST_PIPE_INSTANCE`; unix stale-socket probe-then-reclaim
- [ ] T057 [P] [US2] Integration test: doctor in `crates/tinio-cli/tests/doctor.rs`: clean root → exit 0; stale state/socket, orphaned meta and bucket records, abandoned multipart, stale temps, `meta.redb` integrity/fragmentation report → exit 1; `--dry-run` lists repairs without touching; `--fix` applies them (server stopped required); `--json` output; home root-state-dir GC
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
- [ ] T068 [US2] Implement start-runtime orchestration in `crates/tinio-cli/src/commands/start.rs`: data plane + api plane wiring around a shared shutdown channel (cease accepting, graceful drain ≤ 10 s); `--daemon` detach (Windows: detached child; unix: detach with stderr → `server.log`/`server.json` per contracts/cli.md — the packaging systemd unit covers Linux service-style daemonization); readiness reporting (SC-005); non-loopback bind warnings (escalated with anonymous mode); invokes the startup repair through the `Cleanup` trait (T012/T070) after single-instance binding and before readiness; unit tests written first
- [ ] T069 [US2] Implement signal handling in the start-runtime orchestration in `crates/tinio-cli/src/commands/start.rs`: SIGINT/SIGTERM (unix) and Ctrl+C / console-close events (Windows, via `SetConsoleCtrlHandler` — new `windows-sys` dependency, target-gated, user-approved, justification in research.md §24) trigger the same shared shutdown channel as `POST /stop` (cease accepting, drain ≤ 10 s, remove state/socket, exit 0); a second signal exits immediately without draining; SIGHUP is ignored and logged (no config reload in v1); SIGPIPE is already ignored by the Rust runtime; applies to `--daemon` children too (the systemd unit relies on SIGTERM); unit tests written first
- [X] T070 [US2] Implement the `Cleanup` trait (tinio-core, T012) for the fs backend in `crates/tinio-fs/src/cleanup.rs` (`FsCleanup`): startup repair (items per failure-handling.md §3, fs implementation per fs-backend.md §8.1 — after single-instance binding and before readiness), dry-run diagnostics + fix application used by `doctor` (T073/T074), and meta-orphan reclamation used by the scanner (T045) — one code path with a dry-run flag, called through the trait (never through the fs implementation); never touches user data (bucket dirs/objects); every action logged to the operational log; unit tests written first
- [ ] T071 [P] [US2] Implement `status` in `crates/tinio-cli/src/commands/status.rs`: reads `state` and probes the control channel via the T065 client; output (running|stopped, endpoint, root, PID, started time); unit tests written first
- [ ] T072 [P] [US2] Implement `stop` in `crates/tinio-cli/src/commands/stop.rs`: sends graceful stop via the T065 client, then polls the control channel until probe failure / `state` removal (bounded ~15 s) and reports unconfirmed exit on timeout; unit tests written first
- [ ] T073 [US2] Implement `doctor` diagnostics in `crates/tinio-cli/src/commands/doctor.rs`: root exists/readable/writable (skipped in read-only mode), config validity + resolvable credentials, `.tinio/` integrity per the reclamation matrix (failure-handling.md §3), on-disk bucket/object key validity incl. any-depth `.tinio`, symlinks present while disabled, low disk space warn; severity report (ok/warn/error) + `--json` output; diagnostics run through the `Cleanup` trait (T012/T070); unit tests written first
- [ ] T074 [US2] Implement `doctor --dry-run`/`--fix` in `crates/tinio-cli/src/commands/doctor.rs`: `--dry-run` lists exactly what a fix would change without touching anything; `--fix` applies the reclamation-matrix cleanups (failure-handling.md §3) plus home root-state-dir GC through the `Cleanup` trait implementation (T070) — server-stopped probe required (live server → error), never touches user data; unit tests written first
- [ ] T075 [US2] Expose `GET /metrics` in `crates/tinio-api/src/router.rs`: serve the registry injected from tinio-server; compute storage-layer gauges (bucket count; object count and total bytes) via the storage contract with the 30 s TTL cache — the full-scan gauges are registered and computed here (per the T023 boundary); Prometheus text format (SC-008); unit tests written first. **Wiring (pipeline-spec-review 2026-08-29, finding 6)**: call `metrics::refresh_pipeline_gauges` (from the runtimes' `Stats` snapshots) and `metrics::refresh_write_lock_histograms` (from `Handle::write_lock_stats`) on scrape, and touch `WRITE_LOCK_HISTOGRAMS` registration unconditionally at server startup — the families register lazily on first refresh, so a missing refresh call would silently omit `tinio_write_lock_*`
- [ ] T076 [US2] Implement read-only state relocation (FR-023): state dir resolves to `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700, `dirs` crate) in read-only mode; root never written; pre-existing `<root>/.tinio/.tinio.toml` or `config.toml` read but never written (`.tinio.toml` wins); `.env` loaded from the state dir; affects tinio-fs (meta.redb, multipart, tmp) and tinio-api (state, socket, logs); unit tests written first
- [ ] T077 [US2] Implement read-only enforcement across the S3 mapping groups in `crates/tinio-server/src/backend/`: every mutating operation (bucket create/delete, object put/delete/copy, all multipart ops) returns `AccessDenied`; read operations behave identically; unit tests written first
- [ ] T078 [P] [US2] Add the example systemd unit `packaging/tinio.service` (Type=simple, foreground) per contracts/cli.md

**Checkpoint**: User Stories 1 AND 2 both work — full SC-001 journey (start via CLI → S3 client operations → stop) completes in under 5 minutes; management plane + doctor + read-only mode verified; signal-driven shutdown (Ctrl+C / SIGINT / SIGTERM) behaves identically to `tinio stop`; crash recovery repairs orphaned private state automatically at startup, with the scanner and `doctor` reclaiming the same class of problems.

---

## Phase 5: User Story 3 - Authenticated Access (Priority: P3)

**Goal**: The server authenticates S3 requests with the standard SigV4 scheme using configured credentials (CLI flags > env > `.env` > config, with `MINIO_*` credential fallbacks); requests with missing or invalid signatures are rejected with the standard auth errors. An explicit anonymous mode (flag/env only — never a config key) skips auth entirely and wins over configured credentials with a warning. Session credentials are generated and printed once when the config exists without credentials and anonymous mode is off. `sig_v2` remains a deprecated opt-in runtime toggle.

**Independent Test**: Start the server with credentials, connect with a standard S3 client using those credentials and confirm operations succeed; connect with wrong credentials and confirm rejection (SignatureDoesNotMatch). Restart in anonymous mode and confirm operations succeed without credentials.

### Tests for User Story 3 (written FIRST, must FAIL before implementation)

- [ ] T079 [P] [US3] Integration test: SigV4 auth in `crates/tinio-server/tests/auth.rs`: correctly signed requests succeed; missing/invalid signatures → InvalidAccessKeyId / SignatureDoesNotMatch with no operation performed; credential rotation (edit config + restart → only new credentials accepted)
- [ ] T080 [P] [US3] Integration test: anonymous mode in `crates/tinio-server/tests/anonymous.rs`: explicit `--anonymous`/`TINIO_ANONYMOUS` wins over configured credentials with a warning; no-creds config without anonymous → session credentials generated and printed once; operations succeed without credentials in anonymous mode
- [ ] T081 [P] [US3] Interop scenario in `e2e/interop/auth.sh`: credentialed aws cli v2 + rclone flows against a server started with credentials; wrong-credentials rejection spot-check

### Implementation for User Story 3

- [ ] T082 [US3] Implement the auth provider in `crates/tinio-server/src/auth.rs`: `S3Auth` impl resolving the secret key from the resolved config (thin lookup; s3s performs SigV4/SigV2 verification); `check-bucket-name` framework feature retained (FR-012); unit tests written first
- [ ] T083 [US3] Wire auth into the data plane in `crates/tinio-server/src/data.rs`: build `S3Service` with the auth provider by default; anonymous mode builds it without (framework skips access checks); explicit anonymous switch overrides configured credentials with a logged warning (FR-009); unit tests written first
- [ ] T084 [US3] Extend `crates/tinio-config/src/sources.rs` with the `MINIO_*` credential fallback: `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` (legacy) and `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` (modern) accepted when the `TINIO_*` names are absent (minio-compat contract); unit tests written first
- [ ] T085 [US3] Implement the session-credential branch in `crates/tinio-config/src/credentials.rs`: config present without credentials and no anonymous mode → session credentials generated and printed once (daemon: into the log); unit tests written first
- [ ] T086 [US3] Implement the `sig_v2` runtime toggle in `crates/tinio-server/src/data.rs` (FR-021): `[s3] sig_v2 = false` default; enabling prints a startup warning (deprecated scheme, slated for removal in v2); unit tests written first
- [ ] T087 [US3] Wire the `--anonymous` flag through `crates/tinio-cli/src/commands/start.rs` and the env resolution in `crates/tinio-config/src/sources.rs` (`TINIO_ANONYMOUS`): flag/env-only semantics — an `anonymous` key in `[auth]` is rejected as unknown at startup; unit tests written first

**Checkpoint**: All user stories independently functional — authenticated and anonymous flows verified against aws cli v2 and rclone; error-code suite (SC-004) green including auth failures.

---

## Phase 6: Performance Testing & Verification

**Purpose**: Turn the performance properties promised in the spec into executable checks: the benchmark regression gate (constitution V), flat-memory streaming (SC-003), allocation discipline (constitution V), metric-recording overhead (FR-019), scanner efficiency on externally-populated trees (FR-022/024), and the timing criteria (SC-005/007). Cheap checks run in CI; expensive ones (1 GB transfers) run manually per quickstart §9. All measured values are recorded so trends are visible across releases.

- [ ] T088 [P] Record criterion benchmark baselines and enforce the regression gate: after US1 lands, run the full bench suite from T030/T031 with criterion `--save-baseline` and commit the recorded values as tracked data (e.g. `benches/baselines.json`); extend `.github/workflows/ci.yml` with a PR-time bench-comparison job; mean slowdown > 10 % vs baseline counts as a regression requiring a documented decision and reviewer approval (constitution V)
- [ ] T089 [P] Implement the SC-003 flat-memory verification script `e2e/perf/sc003-flat-memory.sh`: 1 GB upload + download against a running server with RSS sampling (ps on unix, Get-Process on Windows), asserting flat memory — RSS growth stays within a bounded delta regardless of object size; manual run, documented in quickstart §9
- [ ] T090 [P] Implement the CI streaming-memory smoke `e2e/perf/ci-streaming-memory.sh`: ~128 MB upload/download round-trip with RSS sampling and a generous bound (e.g. < 256 MB growth), wired into the CI interop stage on the 3-OS matrix; cheaply catches full-object buffering regressions
- [ ] T091 [P] Implement the allocation-discipline verification with dhat (the `dhat` crate, tinio-fs dev-dependency — user-approved; justification in research.md §24): heap-profile a streaming put/get round-trip through the fs backend in `crates/tinio-fs/tests/allocations.rs` (dhat `Allocator`/`Profiler`), asserting bounded allocations — no per-object buffers on streaming hot paths (constitution V allocation discipline)
- [ ] T092 [P] Implement the metric-recording overhead benchmark `crates/tinio-server/benches/metrics_overhead.rs`: identical request workload with and without Prometheus recording; record the overhead baseline and subject it to the T088 regression gate (FR-019: metric recording MUST NOT measurably degrade request handling)
- [ ] T093 [P] Implement the cold-vs-warm listing benchmark `crates/tinio-server/benches/cold_warm_listing.rs`: generated externally-populated tree (thousands of objects) — first listing (synchronous ETag recompute) vs warm listing (scanner-completed meta store); documents the FR-022 one-time cold cost and the FR-024 scanner benefit
- [ ] T094 Implement timing-criteria measurement: record time-to-ready (SC-005) and `status`/`stop` round-trip (SC-007) on the CI matrix and a typical dev machine, captured into the benchmark report so timing trends are visible across releases (functional ≤ 1 s assertions already live in T056)

**Checkpoint**: Every promised performance property is verified and recorded — baselines committed and regression-gated, memory/allocation/overhead/cold-listing checks green, timing values captured.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories; release-readiness per the constitution

- [ ] T095 [P] Maintain `CHANGELOG.md` at the repository root (constitution VI: public API changes recorded; generated via git-cliff); record the MSRV policy (current stable — the constitution-VI interpretation from plan.md) at the first release
- [ ] T096 Create integration tests in `crates/tinio/tests/` against the facade public API: curated re-exports present and usable, `tinio_cli::run()` dispatch, facade error re-exports; acts as the local baseline for the semver-checks contract (the public API is final once US2 lands). A binary smoke test (`smoke.rs`: exit 0) already exists from Phase 1.
- [ ] T097 Run quickstart.md validation end-to-end: all 10 scenarios (build/test gate, aws cli v2 journey, multipart/copy, management plane, auth/error codes, rclone, crash recovery, doctor, zero-byte/large objects, read-only mode) against a scratch root
- [ ] T098 [P] Verify the feature-matrix behavior contracts: `--no-default-features` and all-default builds pass the full test suite (the remaining feature combinations are covered by the CI compile checks added in T009); feature-off behavior contracts hold — CLI options absent, `[api]`/`[s3]` keys silently ignored, stripped ops → NotImplemented
- [ ] T099 [P] Security hardening review: dependency audit (`cargo audit`), secret-bearing file permissions, layered-trust warning behavior, access-log variable-set security property re-check
- [ ] T100 [P] Constitution compliance review: no `unwrap`/`expect`/`panic!` in library paths, `unsafe_code = "forbid"` everywhere, rustdoc examples on all public items, semver-checks green, doc links valid
- [ ] T101 [P] Write the user manual `docs/user-manual.md` (markdown): installation, configuration reference (all `[server]`/`[scanner]`/`[auth]`/`[log]`/`[s3]`/`[storage]`/`[api]`/`[telemetry]` keys, env variables, precedence), CLI reference (all commands, flags, exit codes), read-only mode, security notes (layered trust, tokens, credentials), troubleshooting; English per the language policy
- [ ] T102 [P] Write the usage tutorial `docs/tutorial.md` (markdown): quick start (install → `tinio server <dir>` → aws cli v2 / rclone setup), typical workflows (bucket/object operations, multipart, server-side copy), management plane (`status`/`stop`/`doctor`, `/metrics`, `/openapi.json`), common scenarios (ephemeral ports, daemon mode, systemd unit, read-only serving); English per the language policy
- [ ] T103 [P] Language-policy cross-check: all docs (including the new `docs/` manual and tutorial), comments, and commit messages in English (per CLAUDE.md)

### Addendum 2026-09-02 — conditional-headers surface (design `docs/superpowers/specs/2026-08-31-s3-conditionals-design.md`, plan `docs/superpowers/plans/2026-08-31-s3-conditionals-cleanup.md`)

Incremental surface work implemented on top of the original plan (FR-027/FR-028/FR-029, added to `contracts/s3-surface.md`); the `conditions.feature` cucumber scenarios each entry references landed with the BDD-migration follow-up (plan Task 7) and carry the `@FR-0xx` tags.

- [X] T104 [US1] Conditional delete: DeleteObject `If-Match` / `x-amz-if-match-last-modified-time` / `x-amz-if-match-size` trio bundled as `DeleteConditions` (AND; strong ETag; whole-second date equality; exact size) with the missing-object policy (missing → 204 under every conditional header per the AWS model text; negative size → 400 InvalidArgument validated up front, state-independent — revised 2026-09-02 on code review; `check_delete_conditions`/`any_delete_conditions` folded into the `DeleteConditions` struct with `absent()` + `check(&Info)` 2026-09-02), head-check + delete under the per-key lock — FR-027 (`conditions.feature` @FR-027)
- [X] T105 [US1] Conditional CompleteMultipartUpload: destination conditions against the object currently at the key (If-Match mismatch → 412, missing → 404 NoSuchKey; If-None-Match `*` only — existing → 412, missing → pass, specific value → 501 NotImplemented) via `check_complete_conditions`; shape errors (both-present 400, specific If-None-Match 501) rejected up front by `check_complete_shape` before the parts parse + lock, and the upload's existence validated before the destination head-check (a dead upload answers NoSuchUpload — revised 2026-09-02 on code review) — FR-028 (`conditions.feature` @FR-028)
- [X] T106 [US1] Conditional AbortMultipartUpload: `x-amz-if-match-initiated-time` (whole-second equality with the upload's Initiated timestamp → else 412; missing upload → NoSuchUpload) with the check + abort under the per-key lock, scoped to conditional aborts only (unconditional aborts stay lock-free — revised 2026-09-02 on code review) — FR-028 (`conditions.feature` @FR-028)
- [X] T107 [US1] If-Range on GET + read-path fix: head → condition check → body fetch (a failed precondition never pays the body); If-Range gates the Range (strong ETag or `last_modified` ≤ header date at whole-second precision; wildcard/garbage → header ignored, mismatch → full 200); response metadata from the fetch's own snapshot with a coherence re-check when a write races between the head and the fetch (revised 2026-09-02 on code review); unconditional GET fast path kept — FR-029 (`conditions.feature` @FR-029; a pure If-Range request — no RFC 7232 conditions — now fetches the Range head-less against one snapshot and evaluates the validator on it, discarding + refetching the full object when stale, and classifies an `InvalidRange` against a lazily-taken head — the head-first flow survives only under RFC 7232 conditions, revised 2026-09-02 #3)
- [X] T108 [US1] Write-path both-present 400 rule: PutObject/CopyObject destinations/CompleteMultipartUpload reject `If-Match` + `If-None-Match` → 400 `InvalidRequest` up front (before staging/parts parse/lock); the copy-source family keeps the RFC 9110 §13.2.2 order, no 400 — FR-027/FR-028 (`conditions.feature` @FR-027 copy-source-position scenario)
- [X] T109 [US1] Shared conditional machinery in `backend/conditions.rs`: `absent()`, `check_missing()`, `parse_if_range()`/`IfRange` (RFC 9110 §13.1.5), `to_whole_seconds()`, `strong_matches`, moved `parse_etag_condition_header`; destination checkers converge on the evaluator and the head-the-key preamble converges on `S3Backend::head_optional` (the `objects.rs` special case and the hand-rolled CopyObject destination parse are gone; `any()` renamed `absent()`, `check_missing`'s dead `write_path` param dropped, and the single-caller `reject_both_etag_headers()` folded into `check_write_shape` 2026-09-02 on code review) — FR-027/FR-028/FR-029 (unit tests in `conditions.rs`)

### Addendum 2026-09-02 #2 — concurrency code-review fixes (review of the T104-T109 diff; design header entry "review pass 2026-09-02 #2")

- [X] T110 [US1] Date-condition consolidation: one `whole_second_ordering(stored, header) -> Ordering` primitive under every date rule (If-Unmodified-Since fails on `Greater`, If-Modified-Since / date If-Range on `!= Greater`, delete/abort equality on `== Equal`); missing-object policy consolidated with the three deliberate per-op answers (put/copy 412, complete 404 NoSuchKey, delete 204) documented centrally in `conditions.rs` with the delete trio bundled as `DeleteConditions` (`absent()`/`check(&Info)` — the `any_delete_conditions()` free predicate folded in) and the destination-write sets built via the etag-only constructor `ConditionalHeaders::etag_only`; `check_complete_conditions` typed on `Option<&object::Info>`, the complete's destination-condition presence a plain OR over the request fields — FR-027/FR-028 (behavior-preserving; unit pins `whole_second_ordering_compares_at_second_precision`, `delete_conditions_absent_detects_the_trio`)
- [X] T111 [US1] Write-path If-None-Match unify: the complete's shape gate becomes the shared `check_write_shape` (both headers → 400, specific `If-None-Match` → 501 `NotImplemented`) over PutObject, CopyObject destination, and CompleteMultipartUpload — real AWS answers 501 on PutObject too, and a non-matching specific value must never fall through to a silent overwrite — FR-027 (unit pins: PUT/copy-destination specific INM → `NotImplemented` on existing and fresh keys; cucumber conditions.feature @FR-027 scenarios)
- [X] T112 [US1] Conditional abort lock removal: `op_abort_multipart_upload` no longer takes the per-key lock in any path — abort state is `(bucket, upload_id)`-scoped and both backends drain in one transaction with an in-txn existence + key match (parity with the always-lock-free unconditional abort; a concurrent complete surfaces `NoSuchUpload`; the fs backend's best-effort `remove_dir_all` never runs under a server lock) — FR-028
- [X] T113 [US1] GET single-snapshot read path: no-Range conditional GETs are a single fetch + post-check (304/412 with the fetched validators, race-free by construction); Range requests with conditions/If-Range keep head-first with the reconciliation upgraded — the generation gate fires on ETag OR mtime (same-content rewrites change only the mtime), the full refetch is served-range-guarded (never discards an already-fetched full body) and re-validates, and a ranged-fetch `InvalidRange` under a matched If-Range refetches when its size differs from the head's (shrink) — pure helpers `generation_changed` / `decide_fetch` / `decide_range_error`; conditional complete runs its upload-existence fetch and destination head concurrently under the lock (`tokio::join!`, NoSuchUpload precedence kept) — FR-029 (table-driven unit tests on the helpers; existing 206/416/304/412 pins stay green)
- [X] T114 [US1] EntityTooSmall authoritative in the storage commit: fs `Store::complete`'s verify loop and the mem complete's write txn re-read the current part state and answer `PartTooSmall` for non-final listed parts below `MIN_PART_BYTES` (the S3-layer pre-check on the lock-held listing snapshot stays as early validation) — storage contract doc, fs/mem store tests, proptest generator and regression seeds, `conformance_multipart` payloads + `EXPECTED_COMPOSED_ETAG`, and the assembly bench reworked to the 5 MiB minimum — FR-014/FR-028 (boundary pins on both backends: MIN passes, MIN−1 → `PartTooSmall`, single small part completes; rule single-sourced as `tinio_core::multipart::check_part_minimum` with all three enforcement sites — fs verify loop, mem write txn, S3-layer pre-check — calling it, and the failure leg asserted once in `conformance_multipart` 2026-09-02 #3)

### Addendum 2026-09-03 — tagging, RenameObject & GetObjectAttributes surface (design `docs/superpowers/specs/2026-08-31-s3-tagging-ops-design.md`, plan `docs/superpowers/plans/2026-08-31-s3-tagging-ops.md`)

Incremental surface work implemented on top of the original plan (FR-030/FR-031/FR-032, added to `contracts/s3-surface.md`; executed as the sdd plan's Tasks 1-12); the cucumber scenarios each entry references carry the new `@FR-0xx` scenario tags — the features host pre-existing keep-legs with older tags, so the tags are per-scenario, not feature-level. `STATE_VERSION` stays 1 — the tagging rows and `OBJECT_PARTS` are additive (user ruling 2026-09-02).

- [X] T115 [US1] `Tags` type + validation + wire codec in `crates/tinio-core/src/object.rs`: per-surface count caps (10 object / 50 bucket), UTF-16 key/value lengths, Unicode charset, duplicate rejection, canonical sorted `k=v&k2=v2` wire form — FR-030 (unit tests written first)
- [X] T116 [US1] Storage-contract additions in `crates/tinio-core/src/storage/`: object/bucket tag methods, write-path `tags` params (`commit_object`/`copy_object`/`create_multipart_upload`), `rename_object`, `list_object_parts`, `stage_body` checksum tee slot — FR-030/FR-031/FR-032 (T2-A/T2-B/T2-C rulings: delete idempotent on missing; `Info.tags` always populated; tag ops lock-free)
- [X] T117 [US1] fs backend (`crates/tinio-fs`): redb row extensions (object tags + recorded checksum, upload tags, bucket tags), the `OBJECT_PARTS` side table with its lifecycle (completion persist, overwrite/delete cleanup, rename migration, copy never inherits), `rename_object`, parts-listing, tee plumbing — FR-030/FR-031/FR-032; rename's parts migration lands with the delete-before-insert reorder (redb 4.2.0 debug-build defect, task-10 blocker)
- [X] T118 [US1] mem backend (`crates/tinio-mem`): the same plumbing over the in-memory redb, conformance green — FR-030/FR-031/FR-032
- [X] T119 [US1] `tagging` capability toggle: `[s3] tagging` schema key (default on), `Capabilities.tagging`, e2e `@tagging-off` config tag, `@minimal-caps` clears tagging too (six caps) — FR-021/FR-030
- [X] T120 [US1] Interface object tagging in `crates/tinio-server/src/backend/`: the `?tagging` ops (existence-head get, replace-all put, idempotent delete; `InvalidTag` on validation failure), write-path `x-amz-tagging` parsing on put/copy/multipart-create, `x-amz-tagging-count` echo on GET (dto field) and HEAD (hand-set header), `tags.rs` wire helpers, `s3.rs` overrides + `MetricS3` wrappers — FR-030 (`tagging.feature` @FR-030)
- [X] T121 [US1] Interface bucket tagging: the three bucket ops behind the same toggle — FR-030 (conformance + server unit pins; no cucumber scenarios)
- [X] T122 [US1] Interface RenameObject: `PUT ?renameObject` + `x-amz-rename-source`, source conditions on `x-amz-rename-source-if-*` (RFC order), destination conditions on the plain `If-*` headers through the shared destination policy + write-shape gate, sorted dual-lock move, degenerate same-key 412, response ETag echo — FR-031 (`conditions.feature` @FR-031)
- [X] T123 [US1] Interface GetObjectAttributes + write-time checksum recording: requested attribute subset (comma-joinable header), `ObjectParts` pagination (`<PartsCount>`), recorded-checksum member; PUT tee validation, completion composite record, copy carry, GET/HEAD echo with `x-amz-checksum-type` — FR-032/FR-026 (`objects.feature` @FR-032, `@checksum-on`)
- [X] T124 [US1] Conformance harness additions in `crates/tinio-util/src/testing.rs` (61 checks): tags both surfaces incl. caps, write-path tags, recorded checksums + kinds, parts retention/lifecycle, rename (with/without conditions, overwrite, parts migration) — FR-030/FR-031/FR-032
- [X] T125 [US1] Cucumber scenarios: `tagging.feature` +7, `conditions.feature` RenameObject block +5, `objects.feature` GetObjectAttributes block +3 (167 scenarios total) — FR-030/FR-031/FR-032 (scenarios left untagged until T126 assigns the spec IDs)
- [X] T126 [US1] Specs & docs: FR-030/FR-031/FR-032 written into `contracts/s3-surface.md`, task entries here, checklist items in `checklists/compatibility.md`, the feature scenarios tagged with the new IDs, the `steps/mod.rs` dangling "spec §Tagging" comment resolved — FR-030/FR-031/FR-032

### Addendum 2026-09-05 — bucket CORS surface (design `docs/superpowers/specs/2026-09-05-s3-cors-design.md`, plan `docs/superpowers/plans/2026-09-05-s3-bucket-cors.md`)

Incremental surface work closing gap-analysis Tier A#2 (FR-033, added to `contracts/s3-surface.md`; executed as the sdd plan's Tasks 1-11). `STATE_VERSION` stays 1 — the `cors_wire` BUCKETS element is appended last (user ruling 2026-09-02; corrected 2026-09-06 — "no bump" covers only the version number: redb 4.2 binds the value type at the `TableDefinition`, so a state dir written under the pre-CORS row arity refuses to open with `TableTypeMismatch` — loud failure, no migration, recovery = delete the state dir). The cucumber scenarios carry `@FR-033` per-scenario tags (the features host no pre-existing keep-legs); the `@cors-off` scenario also pins FR-021.

- [X] T127 [US1] CORS domain types + wire codec in `crates/tinio-core/src/cors.rs` (NEW): `CorsConfig`/`CorsRule` (order-preserving, first-match semantics), `preflight`/`rule_for` matching (single-`*` wildcard incl. apex exclusion, method/header validation within the winning rule — no fall-through), the canonical percent-encoded `cors_wire` string, validation constants (≤100 rules, 255-char ID, ≤1 `*`, five methods, 64-KB config cap) — FR-033 (unit tests written first)
- [X] T128 [US1] Storage-contract additions in `crates/tinio-core/src/storage/`: `get_bucket_cors`/`put_bucket_cors`/`delete_bucket_cors`, BUCKETS row `cors_wire` element (appended last; missing bucket → `NoSuchBucket`; `''` = no configuration; a zero-rule set is normalized to `''`) — FR-033
- [X] T129 [US1] fs backend (`crates/tinio-fs`): the BUCKETS row extension + the trio over the shared-store layer — FR-033
- [X] T130 [US1] mem backend (`crates/tinio-mem`): the trio mirroring the fs backend, conformance green — FR-033
- [X] T131 [US1] Conformance harness additions in `crates/tinio-util/src/testing.rs`: the CORS trio (round trip, replace-all, delete, no-config, both backends) — FR-033
- [X] T132 [US1] Double gate: `cors` cargo feature on tinio-server (default on, in `default`), `Capabilities.cors` + `[s3] cors` config key (default true), e2e `@cors-off` config tag, `@minimal-caps` clears `cors` too (seven caps) — FR-021/FR-033
- [X] T133 [US1] Interface bucket CORS ops in `crates/tinio-server/src/backend/cors.rs`: the trio behind the double gate (`require_cap` → `NotImplemented "{name} is disabled"`), put-layer validations (≥1 rule, ≤100 rules, ID ≤255, ≥1 method+origin per rule, five methods, ≤1 `*`, no `,`/control bytes, non-negative max-age, 64-KB cap), Content-MD5 three-state (missing → `InvalidRequest` + verbatim AWS message, malformed → `InvalidDigest`) — FR-033
- [X] T134 [US1] Preflight route in `crates/tinio-server/src/backend/cors.rs`: over s3s 0.15's `S3Route` seam (OPTIONS + `Origin` + `Access-Control-Request-Method`, anonymous `check_access`), the shared-handle `CorsConfigs` lookup, the AWS-verbatim 403 messages (no-config/existence-oracle-closed, evalution mismatch), the allow-list answer headers (`Access-Control-*`, `Vary` append, `Content-Length: 0`) — FR-033
- [X] T135 [US1] Response decoration in `crates/tinio-server/src/data.rs`: first-origin-matching-rule decoration of `Ok` responses (4xx/5xx included), bare-`*` ACAO/credentials split, `Vary` append, fallible header construction — FR-033
- [X] T136 [US1] Acceptance + docs: `cors.feature` (6 scenarios, `@FR-033`), e2e `@cors-off` step wiring, boto3-journey CORS legs (config trio + raw OPTIONS preflight — the Origin-bearing decoration is covered by the cucumber and data-plane suites, not the journey), FR-033 written into `contracts/s3-surface.md`, `[s3] cors` into `contracts/config.md`, task entries here, gap-analysis Tier A#2 status note — FR-033

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
- Signal handling (T069) and startup orphan cleanup (T070) depend on the shared shutdown channel and orchestration from T068
- Story complete and validated at its checkpoint before moving to the next priority

### Parallel Opportunities

- All Setup tasks marked [P] (crate skeletons) can run in parallel
- All Foundational tasks marked [P] (domain modules, validation, error types, sources) can run in parallel once T010 lands
- `tinio-core` domain types are split by concern (`bucket`, `object`, `etag`, `multipart`) — not a single `domain.rs` / `keys.rs` — per `docs/style.md`
- `tinio-mem` (`MemoryStorage`) is the conformance reference; finish it before relying on it in interop/CLI no-directory mode
- Once Foundational completes, US1's test tasks ([P]) can run in parallel — including the third-party client tasks (T034–T036)
- Within US1: write.rs / meta.rs / buckets.rs ([P]) after path.rs; the two fs backend groups ([P]); the four S3 mapping groups ([P], against the storage contract); the OTel task (T053) after log.rs (T052); scanner.rs and sweep.rs ([P]) can run in parallel
- Within US2: test tasks ([P]); openapi.rs ([P]) alongside the router; status (T071) and stop (T072) are [P] in different files; systemd unit ([P])
- Within US3: all test tasks ([P])
- Phase 6 (Performance): all tasks marked [P] can run in parallel once US1's bench suite (T030/T031) exists; T094 additionally needs US2
- Phase 7 (Polish): docs tasks (T101, T102) are [P] and independent of the other checks
- Different user stories can be worked on in parallel by different team members once their dependencies land

---

## Parallel Example: User Story 1

```bash
# Launch all US1 test suites together (must fail first):
Task: "Contract/integration test: S3 error-code behavior in crates/tinio-server/tests/error_codes.rs"
Task: "Integration test: full data-plane round-trip in crates/tinio-server/tests/data_plane.rs"
Task: "Integration test: reserved-path behavior in crates/tinio-server/tests/reserved_paths.rs"
Task: "Property tests for key validation in crates/tinio-core/tests/validation.rs"

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
- Phase 6 turns the spec's performance properties into executable checks; the allocation-discipline check uses dhat (tinio-fs dev-dependency, user-approved — constitution I justification in research.md §24) and asserts constitution V allocation discipline
- The boto3/mc scenarios (T034/T035) are targeted/manual per FR-025's best-effort tier — promoting them into the CI interop gate is an FR-025 amendment (release-gating contract) requiring spec approval
- `backend.rs` from plan.md is implemented as a `backend/` module directory (mod.rs + one file per operation group) so the split S3 mapping tasks and fs backend tasks are genuinely parallelizable; the module name `backend` is unchanged
- OTel support (T053) is an explicit task behind the opt-in `otel` feature; the `[telemetry]` config key is validated in Phase 2 (T016/T017) and the exporter is consumed in US1
- The interop harness (T032) spawns the server through the tinio-server example binary (`examples/serve.rs`) during US1 and switches to the facade binary once US2 lands — resolving the US1-interop vs US2-binary ordering dependency
- The SigV4 clock-skew window (±15 min per AWS convention, spec §Assumptions) is verified and recorded during implementation (T082), per the spec's implementation-notes commitment
- Signal handling (T069) is part of the start-runtime orchestration: SIGINT/SIGTERM and Windows console-close events share the `POST /stop` shutdown path; the `windows-sys` dependency (cfg(windows), console events) is user-approved with the constitution I justification in research.md §24
- Orphan cleanup is split by cost: fast, deterministic items run at startup before readiness (T070); meta-orphan reclamation runs in the background scanner (T045); `doctor` reuses the same implementation with a dry-run mode (T073/T074)
- The abnormal-condition handling and scanner designs are documented in failure-handling.md and scanner.md, with the fs backend design in fs-backend.md — implementation references for T045 (scanner), T070 (startup repair), and T073/T074 (doctor)
