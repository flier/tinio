# Research: S3-Compatible Local Storage Server

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

Consolidated findings from the design-review research (s3s ecosystem report), the design tree decisions, and the clarify session. Format per item: Decision / Rationale / Alternatives considered.

## 1. Protocol layer: s3s (MVP)

- **Decision**: Use the `s3s` framework (Apache-2.0, active maintenance — last commit 2026-08-19) for the entire S3 protocol layer: request routing, XML parsing/serialization, standard S3 error codes, SigV4/SigV2 verification, DTOs. Our storage backend implements the `S3` trait (~30 operations; the other 69 trait methods have default implementations returning standard NotImplemented errors).
- **Rationale**: The S3 protocol is a large, detail-dense surface (XML shapes, error semantics, signing); reimplementing it is the dominant correctness risk for a compact tool. s3s is the de-facto standard framework, `unsafe_code = "forbid"` (aligned with constitution II), declares MSRV 1.92 as a floor (satisfied by any current stable), edition 2024, actively maintained.
- **Alternatives considered**: Hand-rolled protocol (rejected: scope and correctness risk); `s3s-fs` reference backend (rejected: stores metadata dotfiles inside bucket directories, polluting the served data tree; 0% doc coverage; does not implement `list_multipart_uploads`; used only as a behavioral reference — its atomic temp+rename pattern and multipart ETag composition are reimplemented in `tinio-fs`).
- **Note**: Explicit MVP decision — the protocol layer is swappable later; the storage contract (`tinio-core::Storage` trait + domain types) is protocol-agnostic and backend-agnostic. s3s ships no HTTP body-size or rate limiting; that is intentional (unlimited object size, SC-003; trusted local clients).

## 2. HTTP runtime and wiring

- **Decision**: Data plane = `hyper` + `hyper-util` (s3s's native transport; `S3Service` implements `tower::Service<Request<Body>>`; the framework's own server binary uses this pattern). Management plane = `axum` (user decision) served over the control channel.
- **Rationale**: `s3s-axum` does not exist as a crate; axum would be pure glue on the data plane (a single `any_service` route), while hyper-util is the canonical hosting path. axum earns its place on the management plane (routing, extractors, OpenAPI integration).
- **Alternatives considered**: axum for both planes (rejected: extra dependency layer with no data-plane benefit); sync server (tiny-http, rejected: concurrent streaming and graceful drain are harder to express).

## 3. Authentication

- **Decision**: s3s `S3Auth` trait — implement `get_secret_key(access_key)` over the resolved config (a thin custom impl; the framework performs actual SigV4/SigV2 verification). Anonymous mode = build the `S3Service` without an auth provider (framework then skips access checks entirely). Explicit anonymous switch wins over configured credentials with a warning (spec FR-009).
- **Rationale**: Framework-owned verification minimizes hand-rolled crypto; the backend only does a secret lookup.
- **Alternatives considered**: `SimpleAuth::from_single` (docs explicitly warn "testing only" — rejected for the shipped tool, though it is a reference for the custom impl); hand-rolled SigV4 (rejected in protocol-layer decision).
- **Notes**: `check-bucket-name` default feature kept (FR-012 validation framework-side; the framework's rules are authoritative — FR-012 documents the common subset). SigV2 support available behind the `[s3] sig_v2` toggle, **off by default** (weaker scheme — HMAC-SHA1, no payload signing; aws cli v2 and rclone never use it).

## 4. Data plane details (framework gaps we own)

- **Decision**: (a) GET Range: s3s parses the `Range` header into `Range::Int/Suffix`; the backend seeks and emits `Content-Range`/`206` itself. (b) ETag: backend-provided — single uploads = content MD5 (`md-5` crate, computed while streaming); multipart = `MD5(concatenated part MD5s)-N` (same composition as s3s-fs's reference implementation). (c) Bodies flow as `StreamingBlob` (stream of `Bytes`); the backend streams to/from files with bounded buffers.
- **Rationale**: These are explicitly backend responsibilities in the s3s design; the ETag composition rule must match AWS for client integrity checks (interop-tested).
- **Alternatives considered**: Recomputing ETag on every GET (correct but CPU-costly for large objects) — mitigated by the persisted meta store with size/mtime validation (recompute only on mismatch, see §6).

## 5. Atomic writes and concurrency (FR-011)

- **Decision**: Every object write streams into a temp file, then `fs::rename` to the final path (atomic on same volume; Rust's `fs::rename` replaces existing targets on Windows via MoveFileEx semantics). Temp files live under `<root>/.tinio/tmp/` so in-flight writes never appear in listings. Last completed rename wins; a GET during an upload sees the previous object (or not-found).
- **Rationale**: Simple, correct last-write-wins without locking; matches the spec's torn-write prohibition and the s3s-fs reference approach.
- **Alternatives considered**: In-place writes (rejected: torn objects); per-key locks (rejected: unnecessary — rename is the synchronization point).

## 6. Private state layout (`.tinio/`) and ETag metadata store

- **Decision**: All tool-owned state lives in a reserved directory (`.tinio/` in the storage root; relocated to `~/.tinio/roots/<sha1(canonical root)16>/` in read-only mode, §21). The exact layout — config, state, socket, logs, buckets.json, multipart parts, tmp, and the git-style 2-hex fan-out ETag metadata store `{key, etag, size, mtime}` — is specified in [data-model.md](data-model.md) (Reserved Directory).
- **Rationale**: Root level stays 100% user data (bucket listing shows only top-level dirs; `.`-prefixed bucket names are already invalid per S3 rules, so `.tinio` can never be a bucket). The 2-hex fan-out mirrors git's object-store design: avoids huge flat directories, avoids Windows path-length limits, and keeps bucket-deletion cleanup to a subtree removal. ETag correctness after out-of-band file changes: serve the persisted ETag only when size+mtime match, else recompute streaming and rewrite.
- **Alternatives considered**: Sidecar dotfiles inside buckets (s3s-fs style — rejected: pollutes served data, violates SC-006 mirror semantics); flat mirrored meta tree (rejected: single-dir growth, Windows path limits); no persistence + recompute every GET (rejected: §4).

## 7. Multipart storage and crash cleanup

- **Decision**: Parts stored as files under `.tinio/multipart/<bucket>/<uploadId>/part-<n>` (survive restarts — a crashed server can still complete/abort them). Complete = stream-assemble parts into a temp file, compute composed ETag, atomic rename. Abort = delete the parts subtree. An asynchronous background sweep (does not block startup) removes: temp files with mtime > 24 h; multipart uploads idle > 7 days (no part writes and not completed — matches AWS default); both TTLs configurable.
- **Rationale**: Disk-backed parts give crash resilience for free; the sweep bounds disk leakage from interrupted clients; mtime-based (no journal needed).
- **Alternatives considered**: In-memory parts (rejected: restart orphans); startup-only sweep (rejected: blocks startup, misses long-running uploads); never clean (rejected: unbounded leakage).

## 8. Management plane transport and security

- **Decision**: Management plane = axum served over a local unix socket (Linux/macOS) or Windows named pipe, token-authenticated, with optional TCP HTTP/HTTPS exposure (defaults HTTP; the S3 data plane stays plain HTTP in v1). The complete transport schema — socket/pipe naming, stale-socket reclaim, first-instance pipe semantics, single-instance enforcement, token rules, per-transport config subsections, `--api <URL>` semantics, port defaults, TLS — is specified in [contracts/management-api.md](contracts/management-api.md) and [contracts/config.md](contracts/config.md).
- **Rationale**: Deterministic per-root pipe names allow multi-instance coexistence on Windows; canonicalize-before-hash removes case/trailing-slash/symlink variance. Named pipes are the unix-socket analogue (local-only, no TCP surface).
- **Alternatives considered**: Admin HTTP on the S3 port (rejected: pollutes the S3 surface, conflicts with s3s routing); TCP localhost admin port (rejected: port conflicts and discoverability); PID-file + OS signal (rejected: no reliable cross-process graceful signal on Windows).
- **Note**: Linux `sun_path` is 108 bytes — deep roots may fail to bind; documented limitation, no workaround (abstract namespace sockets are not portable to macOS).

## 9. Observability: tracing, access logs, OpenTelemetry, Prometheus

- **Decision**: All logging through `tracing`. One tower middleware (data plane) emits one access event per request (target `tinio::access`, fields: method/path/status/duration/remote addr/user agent/bytes) and updates the Prometheus registry; a custom `tracing::Layer` formats those events into the access log per `access_log_format` (presets `combined`/`common` or custom nginx-style strings over a fixed variable set; unknown variables rejected at startup) into `.tinio/access.log`. Operational logs via `tracing-subscriber` fmt layers: `text` (default, stderr) or `json` (default file name `server.json`); daemon mode redirects stderr to `server.log`. Errors always visible on stderr (FR-017). Each S3 operation is a span (target `tinio::s3`, fields op/bucket/key/upload_id) — the `MetricS3` delegation wrapper records Prometheus `tinio_s3_operations_total{op,status}` and `tinio_s3_operation_duration_seconds{op}` alongside the span. Optional OTel export behind the `otel` feature (`tracing-opentelemetry` + OTLP exporter, configured via `[telemetry] otlp_endpoint` or standard `OTEL_EXPORTER_OTLP_ENDPOINT`). Management-plane requests are not written to the access log (it is a data-plane log); they appear in the operational log at debug level.
- **Metrics** (`GET /metrics`, Prometheus text): three layers — HTTP (request count/duration/in-flight), S3 operations (op × status incl. error codes), storage (buckets; objects/bytes with a 30 s TTL cache; upload/download byte counters; multipart in progress). Exact metric family names and labels are specified in [data-model.md](data-model.md) (Metrics section). The registry is in-memory (resets on restart — documented).
- **Rationale**: A single instrumentation point (access middleware) feeds log + metrics; spans give end-to-end traces when OTel is enabled. TTL cache bounds scrape cost at scale (design ceiling §Scale).
- **Alternatives considered**: Direct file writes for access logs (rejected: no OTel path, duplicated instrumentation); always-fresh full scans (rejected: O(n) per scrape at scale); tower-http TraceLayer (rejected: emits tracing events, but the nginx-combined format and metric updates are custom anyway — a ~30-line custom layer is smaller).

## 10. Configuration and credentials

- **Decision**: Single TOML config file in the reserved directory (auto-created with generated credentials on first start; no `init` command), validated fail-fast, with `.env` support via `dotenvy`; precedence flags > process environment > `.env` > config. The exact schema — `version = 1`, sections and keys, env names with `MINIO_*` credential fallbacks, the anonymous-key rejection rule, session-credential generation — is specified in [contracts/config.md](contracts/config.md). Credential rotation = edit config + restart.
- **Rationale**: One file, TOML (standard `toml` crate), fail-fast validation, dotenvy for battle-tested `.env` parsing (quoting/comments/CRLF) instead of hand-rolling.
- **Alternatives considered**: No config file (rejected: credentials must persist); YAML/JSON (rejected: TOML is the Rust-native convention); hand-rolled `.env` parser (rejected: classic footgun).

## 11. Content-Type, user metadata, checksums

- **Decision**: Content-Type not persisted — inferred per request from the file extension via `mime_guess`, falling back to `application/octet-stream` (a client-supplied Content-Type on PUT is accepted and dropped; responses always infer from the extension; user `x-amz-meta-*` headers accepted and dropped). `x-amz-checksum-*` headers ignored in v1 (ETag remains the integrity mechanism; interop tests gate this).
- **Rationale**: No sidecar state for Content-Type keeps the meta store single-purpose (ETag); checksums add a hash dimension without a client-visible requirement in v1 (aws cli v2 tolerates their absence).
- **Alternatives considered**: Persisting Content-Type in the meta store (deferred — a one-field change if interop tests demand it); computing CRC32/CRC64NVME (deferred).

## 12. Case sensitivity

- **Decision**: Follow the host filesystem — no collision detection or enforcement on case-insensitive hosts (Windows/macOS); the spec's original "enforce case-sensitive semantics regardless of host" was amended to host semantics.
- **Rationale**: User decision during design review; artificial enforcement (case-insensitive existence checks before every create) costs I/O and diverges from OS truth.
- **Alternatives considered**: Explicit rejection of case-collisions (`BucketAlreadyExists`-style, rejected); forced lowercase-only buckets (rejected: over-restrictive).

## 13. Scale, testing, and CI

- **Decision**: Design ceiling: thousands to hundreds of thousands of objects per bucket, hundreds of GB to a few TB per root. No object-size limit (unlimited streaming). Testing: unit + doc tests per crate; proptest for path traversal, meta-store validation, multipart assembly; criterion benchmarks (streaming upload/download throughput — smoke in CI, full runs manual); interop tests with aws cli v2 and rclone covering the core journey (create/upload/download/list/delete/multipart/copy) plus error-code spot checks (bad signature, missing bucket/object, traversal attempt, invalid bucket name); CI matrix Windows + Linux + macOS on the latest stable toolchain. Interop runs also verify addressing-style behavior: path-style is the supported mode, and both clients must work against `127.0.0.1` endpoints with no client-side addressing override (SC-002's "no workarounds" clause). Cold-listing ETag cost (one-time full-content MD5 pass over externally-added files) is documented; a benchmark with a large externally-populated tree guards the behavior.
- **Rationale**: Scale statement makes listing/metrics design testable; two independent clients (AWS-official + independent implementation) cross-validate protocol correctness, including hand-verified ETag/Range behavior.
- **Alternatives considered**: Single-client interop (rejected: SC-002 requires two); Unix-only CI (rejected: FS differences are core bug sources on Windows).

## 14. Framework verification (management plane transport)

- **Decision**: Management plane = axum Router served over `tokio::net::UnixListener` (Linux/macOS) and `tokio::net::windows::named_pipe::NamedPipeServer` (Windows); OpenAPI via `utoipa` with its `axum_extras` feature (user-specified).
- **Rationale**: User decision; utoipa documents the management API (status/stop/metrics) as an OpenAPI layer.
- **Alternatives considered**: Hand-documented API (rejected: user requested utoipa).

**Verified facts (research agent, 2026-08-21)**:
- **Versions** (current stable): axum 0.8.9, tokio 1.53.1 (Windows named pipes live under tokio's `net` feature — no separate flag), hyper-util 0.1.20, utoipa 5.5.0, utoipa-swagger-ui 9.0.2.
- **utoipa**: the axum integration feature is exactly `axum_extras` (confirms the requirement); it makes `#[utoipa::path]` compatible with axum handler signatures. `utoipa-swagger-ui` is a separate crate (feature `axum`) — not needed in v1 (spec JSON only), available later if a UI is wanted.
- **UnixListener**: `tokio::net::UnixListener` implements axum's `Listener` trait directly — `axum::serve(unix_listener, router)` works with no adapter (axum defines the `Listener` trait itself in `axum::serve`).
- **Windows named pipe**: `NamedPipeServer` does NOT implement `Listener` — the serving loop must be manual: accept the pipe, wrap it in `hyper_util::rt::TokioIo`, then `hyper_util::server::conn::auto::Builder::serve_connection(io, router)`. `Router<()>` implements `tower::Service<http::Request<Body>>` (axum 0.8), so there is no body-type mismatch. Known pitfall: Windows named pipes lack TCP's half-close (SHUT_WR) semantics, which can break HTTP keep-alive/pipelining — plan to disable keep-alive on the pipe transport (or document the limitation).
- **TLS (HTTPS listener)**: verified — axum 0.8's `Listener` trait (public, `accept` + `local_addr`) is the canonical extension point: wrap `TcpListener` + `tokio_rustls::TlsAcceptor` (0.26.x) in a `Listener` impl and pass to `axum::serve`; `accept` must handle/retry `TlsAcceptor` errors per the trait contract. Alternative: `axum-server` 0.8.0 (supports axum 0.8; `tls-rustls` feature; `RustlsConfig::from_pem_file`). Same `Listener` mechanism could unify the named-pipe transport (implement `Listener` for a `NamedPipeServer` wrapper), but keep-alive is not configurable through `axum::serve` — so the named-pipe path keeps the manual `serve_connection` loop with `http1_keep_alive(false)` to dodge the half-close pitfall, while TCP/HTTPS use `axum::serve` with `Listener` impls. Current versions: tokio-rustls 0.26.4, rustls 0.23.43.

## 15. Backend abstraction (extension seam)

- **Decision**: `tinio-core` defines the storage contract — a `Storage` trait (async bucket/object/multipart operations, `Send + Sync + 'static`), domain types (`Bucket`, `ObjectInfo`, part/multipart state), backend-agnostic key validation, and a conformance test harness (behind the `testing` feature, off by default — backend crates enable it in their dev-dependencies) that every backend implementation must pass. `tinio-fs` is the v1 filesystem implementation (path mapping, atomic writes, listing, meta store, multipart parts, sweep, buckets.json). `tinio-server` maps the s3s `S3` trait operations onto `Storage`; the facade re-exports the contract for third-party backends. Planned follow-on backends: `tinio-s3` (S3-backed gateway) and `tinio-webdav` — same trait, no protocol/CLI/config changes. Backend selection is not a v1 config key (filesystem-only); the selection key lands with the second backend.
- **Rationale**: User-directed extensibility; the trait is a deep module — protocol layer, management plane, CLI, and config all speak the contract, so adding a backend is a new crate + one wiring point. The conformance harness makes every backend provably equivalent from the protocol layer's perspective.
- **Alternatives considered**: Filesystem implementation wired directly into the server (rejected: the planned backends would require a refactor of the mapping layer); contract hosted in `tinio-server` (rejected: backends would drag in the HTTP/s3s stack); traitless single-implementation (rejected: contradicts the extension plan).

## 16. Management plane as an optional crate (feature `api`)

- **Decision**: The entire management plane moves to a dedicated crate `tinio-api` (axum router with `/status` `/stop` `/metrics` `/openapi.json`, token auth, transports, state file, single-instance bind, status/stop client), gated behind the default-on cargo feature `api`. Builds with `--no-default-features` yield a bare S3 server (data plane only): no management surface, no state file, no single-instance enforcement beyond port conflicts. When the feature is off: `status`/`stop` subcommands and the `--api` options are absent from the CLI (compiled out); `[api]` config keys are schema-known and silently ignored (not unknown-key failures). Wiring: the start command builds the data plane, then the api plane around a shared shutdown channel; the Prometheus registry is owned by `tinio-server` (the data plane instruments it) and injected into `tinio-api` for `/metrics`.
- **Rationale**: User-directed — the management plane is the heaviest dependency block (axum, utoipa, rustls, prometheus exposure); making it an optional crate keeps the bare data-plane build light and keeps feature-unification semantics explicit (facade default `api`).
- **Alternatives considered**: Management plane inside `tinio-server` behind a feature flag (rejected: would still compile the axum/utoipa dependency graph unless the whole crate is feature-split; a separate crate gives clean dependency isolation and a re-export seam); always-on (rejected: no bare build possible).

## 17. Per-crate error types (thiserror)

- **Decision**: Every crate defines its own error module (`error.rs`) with `thiserror`-derived types, and each crate exposes exactly one public error type: `tinio-core::Error` (backend-agnostic domain errors: NotFound/AlreadyExists/NotEmpty/InvalidKey/InvalidBucketName/Unsupported/transparent Io), `tinio-fs::Error` (io + domain mapping, `From`-converts into the core error), `tinio-config::Error` (parse/validation), `tinio-server::Error` (startup + mapping), `tinio-api::Error` (maps to HTTP status + JSON error bodies), `tinio-cli::Error` (user-facing messages + exit codes), and the facade's `error.rs` re-exports the crate errors for third-party consumers. Conversion chains run one way: fs → core → s3s error codes (in `tinio-server`'s mapping layer) → HTTP statuses (in `tinio-api`) → CLI exit codes (in `tinio-cli`).
- **Rationale**: User-directed; typed errors are required by constitution II, and per-crate errors keep dependency boundaries honest — `tinio-core` must never leak a backend-specific type, and the mapping layers (S3 codes, HTTP statuses, exit codes) each translate in exactly one place.
- **Alternatives considered**: One workspace-wide error type (rejected: couples crates and would force `tinio-s3`/`tinio-webdav` to reuse filesystem error variants); `anyhow` in library crates (rejected: libraries need typed, nameable errors; `anyhow` is reserved for nothing here since the CLI also uses typed errors for exit-code mapping).

## 18. Optionality audit (feature matrix)

- **Decision**: Two-layer capability model for the S3 compatibility layer. Compile-time cargo features (default on, individually strippable): `api` (management plane crate), `openapi` (utoipa + `/openapi.json`, crate-internal to `tinio-api`), `tls` (rustls HTTPS listener, crate-internal to `tinio-api`), and the S3 capability groups `multipart` (7 ops + part storage + assembly), `copy` (`copy_object` + `upload_part_copy`), `list-v1`, `list-v2` (shared listing core, separate XML surfaces). Runtime `[s3]` config toggles remain for the same groups plus `delete_objects` and `sig_v2`. `otel` stays opt-in. The S3 compatibility layer itself is ALWAYS compiled — it is the tool's core interface; only its capability groups are strippable.
- **Rationale**: User-directed audit. Compile features strip real code weight (multipart assembly, part storage, listing surfaces) for minimal/embedding builds; runtime toggles keep operator choice without rebuilds; the two layers compose (feature off → config keys silently ignored, CLI options absent, operation → NotImplemented). `delete_objects` (~20 lines) and `sig_v2` (s3s-internal runtime option) are not worth compile-time gating.
- **Alternatives considered**: Whole-layer `s3` feature (rejected by the user — the compatibility layer is required); compile features for every `[s3]` key including `delete_objects`/`sig_v2` (rejected: negligible code to strip, s3s-internal behavior).

## 19. Plan-review round 3 decisions

- **Symlink policy**: followed by default — the storage root is user-owned and links may point outside it (documented, matches "serve what is in the directory", SC-006). Disable via `[storage] follow_symlinks = false` or `--no-follow-symlinks`: access resolving through a symlink is then rejected and symlink entries are excluded from listings. The `[storage]` config section now exists for backend behavior keys; the deferred `type` selection key will land with the second backend.
- **Windows local-channel addressing**: `--api pipe://<name>` is the pipe-analogous form of `unix://`; `[api.unix] path` holds the pipe name on Windows (empty = derived `tinio-<sha1(root)>`); platform-mismatched schemes are usage errors.
- **Stale unix socket recovery**: probe-then-unlink before bind (a live instance makes the probe succeed → single-instance error; a dead socket is removed → clean restart). No unconditional unlink (would allow double instances).
- **Multipart upload IDs**: UUID v4 via the `uuid` crate (added to the dependency list).
- **`tinio doctor`**: offline diagnostic subcommand (no server needed): config validity (exists/parses/validates/credentials resolvable), on-disk bucket/object key validity (universal + platform rules), `.tinio/` integrity (stale state, stale socket, orphaned meta/buckets.json entries, abandoned multipart, stale temps), symlinks present while disabled, low disk space warn; human-readable severity report, optional `--json`, exit 0 clean / 1 problems. Lives in `tinio-cli`, uses `tinio-config` + `tinio-core` + `tinio-fs`; not feature-gated (diagnoses the always-present layers).

## 20. S3 surface semantics (plan review)

The plan-review session confirmed the S3 surface semantics — port defaults (Minio-compatible 9000/9001, `--port 0` ephemeral), the uniform `--api <URL>` flag, conditional requests (If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since, 304/412), folder markers (keys ending in `/` are never objects), backend-defined key charset, and anonymous-first-start credential persistence — as specified in [contracts/s3-surface.md](contracts/s3-surface.md), [contracts/minio-compat.md](contracts/minio-compat.md), and [data-model.md](data-model.md). The charset rule amends the earlier "consistent rejection" draft: universal rules (traversal, absolute paths, control characters) apply everywhere, platform limits follow the backend.

## 21. Read-only mode and post-review hardening (2026-08-21)

- **Decision (read-only mode, FR-023)**: `--read-only` flag / `TINIO_READ_ONLY` env / `[server] read_only = true` — all S3 mutations rejected with `AccessDenied`, the storage root never written (may be a genuinely read-only filesystem), and all state relocated to `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700; home via the `dirs` crate). The config read/lookup rules and `.env` behavior are specified in [contracts/config.md](contracts/config.md).
- **Rationale**: makes the spec's "read-only storage root" edge case actually achievable (all state under an unwritable root was a contradiction), and serves pristine directories without `.tinio/` pollution. Per-root hash subdirectories mirror the Windows pipe-name derivation, so multiple read-only roots coexist.
- **Alternatives considered**: metadata-only redirection with data writes still landing in the root (rejected — half-read-only semantics confuse everyone; users who want a pristine root but writable data can bind-mount); single shared `~/.tinio/` state dir (rejected — no multi-root coexistence); failing startup on unwritable roots without a mode (rejected — serving read-only archives is a real use case).
- **Notes**: Windows `--daemon` spawns a detached child process (no service-manager integration in v1; systemd unit example covers Linux). ETag size/mtime validation has a documented granularity limit (same-size, same-tick edits may serve a stale ETag). `buckets.json` and meta files are written atomically (temp + rename) under an in-process lock.

## 22. Scanner, doctor fixes, nested roots (post-review round 2, 2026-08-21)

- **Decision (background scanner, FR-024; Minio-aligned name and keys)**: after startup, a low-priority background task pre-computes ETag metadata for missing/stale entries across the whole tree — default on (presence-gated `[scanner]` section in the auto-created config; omitted = off), lowest scheduling priority (yields to request traffic), paced by Minio-aligned keys (`delay` / `max_wait` / `cycle`; schema and defaults in contracts/config.md). Runs in read-only mode too (meta writes go to the home state dir). Never blocks startup; aborts quietly on shutdown; listings stay correct without it (synchronous recompute fallback).
- **Rationale**: removes the cold-listing cliff (first listing over an externally-populated tree no longer pays a full-content pass inline); the task eventually converts every cold file into a meta-store hit, so even the first client listing after warmup is cheap.
- **Alternatives considered**: unlimited inline recompute on listing (rejected — client-timeout risk on huge trees); scanner default-off (rejected — the common case is externally-populated roots); no rate-cap option (rejected — shared-disk scenarios want one).
- **Decision (`doctor --fix` / `--dry-run`)**: `--dry-run` reports what a fix would do; `--fix` applies the cleanups (stale state/sockets, orphaned meta and `buckets.json` entries, abandoned multipart, stale temps, stale home root-state dirs), requires the server stopped, and never touches user data — the command contract is in [contracts/cli.md](contracts/cli.md). This doubles as the GC story for `~/.tinio/roots/<hash>/`.
- **Decision (nested roots)**: `.tinio` is a reserved path segment at ANY depth — writes rejected (`AccessDenied`), reads `NoSuchKey`, listings skip. Nested roots are allowed; the reservation closes the state-leak channel (an outer server would otherwise serve an inner root's credentials). Deliberate deviation from pure S3 key semantics, documented.
- **Alternatives considered**: warn-only nesting (rejected — leak stays open); refusing nested starts (rejected — forbids legitimate layouts and doesn't close the leak for pre-existing nesting).
- **Decision (stop wait)**: after `POST /stop` 202, the CLI polls the control channel until probe failure / `state` removal (bounded ~15 s); timeout → report unconfirmed exit.
- **Decision (Windows secrets)**: state dir and secret-bearing files get a current-user-only ACL (0600/0700 equivalent).
- **Decision (root identity)**: a root's identity is its canonical path; renaming/re-linking the root means a new derived home state dir and regenerated credentials — documented behavior.
- **Integration test coverage** (see plan.md Testing): addressing style (aws cli v2 against `127.0.0.1` AND `localhost` endpoints — virtual-hosted fallback must not require client overrides, SC-002), cold listing with and without the scanner, read-only mode end-to-end (genuinely read-only FS on unix; flag-only on Windows), doctor `--dry-run`/`--fix` incl. home-dir GC, any-depth `.tinio` hiding (incl. nested-root scenario), stop-wait behavior.

## 23. Minio-style CLI and environment compatibility

- **Decision**: CLI invocation, port defaults, environment fallbacks, and scanner keys follow Minio conventions (`tinio server <dir>` positional, `--address` alias, 9000/9001 defaults, `TINIO_*`-first with `MINIO_*` credential fallback, Minio-aligned scanner keys). The complete user-facing surface — including the documented deviations (loopback-only default bind, `--anonymous`, not-adopted Minio flags) — is specified in [contracts/minio-compat.md](contracts/minio-compat.md).
- **Rationale**: User-directed; Minio is the reference local S3 tool, and its invocation shape (`minio server /data`, ports 9000/9001, root-user env vars) is what automation scripts already assume. The earlier ephemeral-port default was reversed (explicit `--port 0` for tests keeps the same testing value without surprising default behavior).
- **Alternatives considered**: Keeping `--root` (rejected: user-directed removal — the positional form is the Minio idiom); ephemeral default with 9000 only in config (rejected: diverges from Minio's out-of-the-box 9000); adopting Minio's all-interfaces `:9000` bind (rejected: a local tool should stay loopback-bound by default); `MINIO_*` fallback for every variable (rejected: only credentials have meaningful Minio equivalents; address/logging variables stay `TINIO_`-only).
