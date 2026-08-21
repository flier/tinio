# Feature Specification: S3-Compatible Local Storage Server

**Feature Branch**: `001-s3-local-server`

**Created**: 2026-08-21

**Revised**: 2026-08-21

**Status**: Draft (design review complete; pending plan)

**Input**: User description: "Build a compact tool providing a Minio-like S3-compatible layer over local directories with basic CLI management."

## Clarifications

### Session 2026-08-21 (initial)

- Q: Should the v1 operation set include multipart uploads and server-side copy, or stay strictly compact? → A: Include basic multipart (initiate / upload part / complete / abort) and CopyObject in v1.
- Q: How should users provide server credentials to the CLI? → A: Config file, environment variables, and CLI flags are all supported; precedence is flags > environment variables > config file.
- Q: What request logging should the server do by default? → A: Request logs (method, path, status, duration) go to stderr with a verbosity flag (error/warn/info/debug, default info); an optional log-file configuration redirects request logs to that file; errors always remain visible on stderr.

### Session 2026-08-21 (design review)

- Q: What language and project structure? → A: Rust workspace (edition 2024, MSRV 1.92). One facade crate `tinio` (thin binary + a re-export library exposing a curated public API for third-party extension) plus four library crates: `tinio-core` (filesystem storage semantics, no HTTP), `tinio-config` (configuration and credentials), `tinio-server` (S3 backend + data plane + management plane), `tinio-cli` (CLI commands). The project is renamed from tinyio to tinio.
- Q: How is the S3 protocol layer implemented? → A: The MVP uses the `s3s` framework (v0.14.1) for the S3 protocol — routing, XML parsing/serialization, standard error codes, SigV4/SigV2 verification — served over hyper/hyper-util. The storage backend implements the `S3` trait (~30 operations; all other operations return standard NotImplemented errors). This is an explicit MVP decision: the protocol layer is swappable later without touching the storage layer.
- Q: Where do server state, metadata, multipart parts, and logs live? → A: In a reserved `<root>/.tinio/` directory that is never served or listed through the S3 interface (bucket listings show only top-level directories, and bucket names starting with `.` are invalid per S3 naming rules). Multipart parts live under `.tinio/multipart/`; ETag metadata in a git-style, content-addressed store under `.tinio/meta/objects/<bucket>/<2-hex>/<hash>.json` (2-character fan-out avoids large flat directories and Windows path-length limits); bucket creation times in `.tinio/buckets.json`.
- Q: How is the server configured? → A: Configuration file `<root>/.tinio/.tinio.toml` with sections `[server]` (host, port), `[auth]` (access_key, secret_key), `[log]` (verbosity, access_log, access_log_format, server_log_format, server_log_file), `[s3]` (capability toggles), `[telemetry]` (otlp_endpoint). A `.env` file in the same directory is also loaded. Precedence: CLI flags > process environment > `.env` > config file. The configuration file is auto-created with generated credentials on first start; there is no `init` command. Credential rotation = edit the config file and restart.
- Q: What is the CLI surface? → A: `start` / `status` / `stop` only, systemd-style (foreground by default, `--daemon` to detach; example systemd unit file shipped in the repo). Bucket and object operations are performed by manipulating the directory directly — no CLI data commands. Storage-root discovery: `--root` flag, else walk up from the current directory to the nearest `.tinio/`.
- Q: How do `status` and `stop` reach the server? → A: Through a management plane separate from the S3 data plane: axum over a unix socket (`.tinio/control.sock`, mode 0600) or a Windows named pipe (`\\.\pipe\tinio-<sha1(root)16>`), authenticated by a token stored in `.tinio/state` (0600). Endpoints: `GET /status`, `POST /stop` (graceful shutdown: stop accepting, drain in-flight requests within a bounded timeout), `GET /metrics` (Prometheus), `GET /openapi.json` (utoipa with axum_extras). Binding the control channel enforces single-instance semantics. In `--daemon` mode stderr is redirected to `.tinio/server.log` (or `server.json` when JSON format is selected).
- Q: Logging? → A: All logging flows through `tracing`. Access log defaults to `.tinio/access.log` in nginx/apache combined style; the format is configurable (`combined` / `common` / custom nginx-style format strings over a fixed variable set; unknown variables are rejected at startup). Operational logs default to stderr, `text` or `json` format (JSON defaults the filename `server.json`). Errors always remain visible on stderr. Optional OpenTelemetry export behind the `otel` feature and `[telemetry] otlp_endpoint`.
- Q: Metrics? → A: Prometheus `GET /metrics` on the management plane, covering three layers: HTTP (request count/duration/in-flight), S3 operations (operation × status including error codes), storage (buckets; objects and bytes with TTL-cached full scans; upload/download byte counters maintained on streaming paths; in-progress multipart count).
- Q: Case sensitivity? → A: Follows the host filesystem; no artificial enforcement is added on case-insensitive hosts.
- Q: Which S3 capabilities are configurable? → A: The `[s3]` section toggles multipart, copy_object, list_objects_v1, list_objects_v2, delete_objects, and sig_v2; disabled capabilities return standard NotImplemented errors; unknown configuration keys are rejected at startup.
- Q: Interop testing? → A: aws cli v2 and rclone, in CI on Windows, Linux, and macOS.
- Q: Multipart part size? → A: No 5 MB minimum is enforced (permissive; standard clients comply with the rule themselves).
- Q: Checksum headers (x-amz-checksum-*)? → A: Ignored in v1; ETag is the integrity mechanism.
- Q: TLS? → A: Out of scope for v1 (local/private-network usage); plain HTTP only.

### Session 2026-08-21 (clarify)

- Q: Should single-object uploads and downloads have a size limit? → A: No limit; any size streams, consistent with FR-010 and SC-003. The absence of built-in body/rate protection in the protocol framework is an intentional choice for a local single-user tool.
- Q: When credentials are configured and anonymous mode is explicitly enabled, which takes effect? → A: The explicit anonymous switch wins over configured credentials, with a warning logged.
- Q: How are stale temp files and interrupted multipart uploads cleaned up after a crash? → A: An asynchronous background sweep removes files by file-date (mtime) timeout: temporary write files after 24 hours, abandoned multipart uploads (no part writes and not completed) after 7 days, matching AWS's default; both timeouts configurable, and the sweep never blocks startup.
- Q: What data scale is the design ceiling? → A: Medium — thousands to hundreds of thousands of objects per bucket, hundreds of GB to a few TB per storage root; no performance promises beyond that.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Serve Local Directories Over an S3-Compatible Interface (Priority: P1)

A user starts the tool pointing at a local directory (the storage root). The tool listens on a local HTTP port and speaks the S3 API: buckets map to top-level subdirectories and objects map to files inside them. The user points any standard S3 client at the endpoint and can create and delete buckets, upload objects, download objects, list objects, and remove objects — exactly as they would against a hosted S3 service, with no client-side configuration tricks.

**Why this priority**: This is the core value of the feature — an S3-compatible layer that makes local files usable from the entire S3 ecosystem of tools (backup scripts, upload tools, SDKs). Without it the feature does not exist.

**Independent Test**: Start the tool against an empty directory, then use a standard S3 client to create a bucket, upload a file, download it back, list objects, and delete the object. Verify the uploaded file physically appears in the local directory and that a file dropped into the directory by hand is immediately retrievable through the client.

**Acceptance Scenarios**:

1. **Given** a storage root with an existing directory `photos`, **When** the user lists buckets with an S3 client, **Then** `photos` appears as a bucket.
2. **Given** a running server, **When** the user uploads a file via an S3 client to a new bucket, **Then** the bucket and file appear in the local directory and a subsequent download returns byte-identical content.
3. **Given** a file placed directly in a bucket directory, **When** the user requests it via an S3 client, **Then** it is served without a restart or sync step.
4. **Given** a bucket with several objects, **When** the user lists objects with prefix and delimiter parameters, **Then** results are filtered and grouped per S3 semantics.
5. **Given** an existing object, **When** the user deletes it, **Then** it is removed from the directory and further requests return a not-found response.

---

### User Story 2 - Basic CLI Management (Priority: P2)

A user manages the server entirely from the command line: start the server with host/port/credentials options (configuration is auto-created on first start), check its status, and shut it down cleanly. Bucket and object management is done by operating on the directory directly — the on-disk layout is the single source of truth.

**Why this priority**: The CLI is the control surface of the tool; for a local filesystem-backed tool, direct directory manipulation is the natural interface for data, which is why no CLI data commands are provided.

**Independent Test**: Start the server against a directory, run status, create subdirectories and files by hand, verify they are served through the S3 interface, then stop the server and verify a clean shutdown with no partial files.

**Acceptance Scenarios**:

1. **Given** an unconfigured storage root, **When** the user runs the start command with a custom port, **Then** the server binds to that port, reports readiness, and auto-creates the configuration file with generated credentials.
2. **Given** a running server, **When** the user runs the status command, **Then** it reports whether the server is running, its endpoint, and the storage root.
3. **Given** a running server, **When** the user creates buckets and files directly in the storage root, **Then** they are immediately served through the S3 interface and the on-disk layout matches.
4. **Given** a running server, **When** the user runs the stop command, **Then** the server shuts down cleanly (in-flight requests drained within a bounded timeout) and no partial files remain.

---

### User Story 3 - Authenticated Access (Priority: P3)

A user configures a pair of credentials (access key and secret key) when starting the server. Standard S3 clients authenticate requests with those credentials using the standard S3 request-signing scheme; requests without valid credentials are rejected. An anonymous mode (no credentials required) is available as an explicit configuration choice for local-only use.

**Why this priority**: Authentication is required for drop-in compatibility with standard S3 clients and to protect the served files from arbitrary local network access. It ranks below the core serving and CLI flows because the tool targets single-user local use, where an anonymous mode remains a safe fallback.

**Independent Test**: Start the server with credentials, connect with a standard S3 client using those credentials, and confirm operations succeed; connect with wrong credentials and confirm operations are rejected. Restart in anonymous mode and confirm operations succeed without credentials.

**Acceptance Scenarios**:

1. **Given** a server started with credentials, **When** an S3 client sends correctly signed requests, **Then** operations succeed.
2. **Given** a server started with credentials, **When** a client sends requests with missing or invalid signatures, **Then** the server responds with an authentication error and performs no operation.
3. **Given** a server started in anonymous mode, **When** any client connects without credentials, **Then** operations succeed.
4. **Given** a running server, **When** the configured credentials are changed in the configuration file and the server restarted, **Then** only the new credentials are accepted.

---

### Edge Cases

- What happens when a request references a bucket that does not exist? The server MUST respond with the standard S3 "bucket not found" error, never a generic failure.
- What happens when a request references an object that does not exist? The server MUST respond with the standard S3 "key not found" error.
- How does the server handle object keys containing path traversal sequences (`..` or absolute paths)? Such requests MUST be rejected outright — no file outside the storage root may ever be read, written, or deleted.
- How does the system handle object keys containing `/`? Nested keys map to nested directories and MUST work for upload, download, list, and delete.
- What happens when a client sends a zero-length object? It MUST be stored and retrieved as a valid zero-byte file.
- What happens when two clients write the same object concurrently? The last completed write wins; the stored object MUST never be a torn/partial mix of both writes.
- What happens when an upload is interrupted mid-transfer? The server MUST NOT leave a partial object visible as a completed object.
- How does the server behave with very large objects? Transfers MUST stream without buffering the whole object in memory.
- What happens when an upload exceeds any size limit? There is no size limit; arbitrarily large objects MUST stream without buffering. The absence of body-size and rate limits is an intentional choice for a local single-user tool, not an omission.
- What happens when a bucket name violates S3 naming rules? The server MUST reject it with the standard S3 "invalid bucket name" error.
- What happens when a request references the reserved `.tinio` directory? It MUST never appear in bucket or object listings, and bucket names starting with `.` are rejected by S3 naming rules.
- What happens when files are modified directly on disk after ETag metadata was persisted? The served ETag MUST be revalidated against file size/mtime and recomputed when they differ; the served Last-Modified MUST reflect the file's actual state.
- What happens on case-insensitive filesystems (Windows/macOS) when two bucket or object names differ only in case? Host filesystem semantics apply; no artificial enforcement is added.
- What happens when the storage root is read-only or a path becomes inaccessible mid-operation? The request MUST fail with a meaningful error and the server MUST keep running.
- How does the server respond when the configured port is already in use? It MUST report a clear error at startup and exit cleanly.
- How does the server respond when a second instance is started on the same storage root? It MUST report a clear single-instance error at startup and exit cleanly.
- How are object metadata timestamps handled when a file is modified directly on disk? The served last-modified time MUST reflect the file's actual state.
- What happens when a multipart upload is aborted or never completed? Incomplete uploads MUST NOT appear as objects, and abort MUST remove any parts already uploaded; uploads that stay idle for the configured timeout (default 7 days) are removed by the asynchronous background sweep, as are temporary write files older than their timeout (default 24 hours).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The tool MUST serve an S3-compatible interface over HTTP for a user-chosen local storage root, mapping buckets to top-level subdirectories and objects to files.
- **FR-002**: Users MUST be able to create and delete buckets through the S3 interface, with corresponding directories created and removed on disk.
- **FR-003**: Users MUST be able to upload, download, and delete objects through the S3 interface; uploaded content MUST appear as a file in the storage root and downloads MUST return byte-identical content.
- **FR-004**: Users MUST be able to list buckets, and list objects within a bucket including prefix filtering and delimiter-based grouping per standard S3 semantics.
- **FR-005**: The server MUST respond to error conditions with the standard S3 error codes (bucket not found, key not found, invalid bucket name, authentication failure, and so on) so standard clients report failures correctly.
- **FR-006**: The server MUST reject any request whose object key could address a path outside the storage root (path traversal, absolute paths); such requests MUST fail without touching the filesystem.
- **FR-007**: The tool MUST provide CLI subcommands to start the server (with configurable host, port, storage root, credentials, logging settings, and daemon mode), report server status, and stop the server. On first start the tool MUST auto-create the configuration file with generated credentials; there is no separate init command, and no CLI data commands are provided (bucket/object operations are performed directly on the filesystem).
- **FR-008**: The server MUST support authenticated requests using the standard S3 request-signing scheme (SigV4) with user-configured credentials, and MUST reject requests with missing or invalid signatures with an authentication error; signature verification is provided by the S3 protocol framework against a configured secret-key lookup.
- **FR-009**: The server MUST support an anonymous mode (no credentials required), enabled only by explicit configuration (flag or environment variable); an explicit anonymous switch takes precedence over configured credentials, with a warning logged.
- **FR-010**: Object transfers MUST stream; serving or receiving an object MUST NOT require buffering the full object in memory regardless of object size.
- **FR-011**: Concurrent writes to the same object MUST resolve to a complete last-write-wins result; an interrupted upload MUST NOT leave a partial object visible as a completed object.
- **FR-012**: Bucket names MUST be validated against standard S3 naming rules (3-63 characters, lowercase letters, digits, dots, and hyphens, no leading/trailing dot or hyphen) and invalid names MUST be rejected.
- **FR-013**: Files placed in the storage root outside the S3 interface MUST become immediately visible and served without restart; objects written through the interface MUST be immediately visible on disk.
- **FR-014**: Users MUST be able to upload objects via multipart (initiate, upload part, complete, abort, list parts, list uploads, server-side part copy); an incomplete or aborted multipart upload MUST NOT be visible as a completed object. Part size minimums are not enforced. Stale temporary write files and abandoned multipart uploads MUST be removed by an asynchronous background sweep based on file-date (mtime) timeouts — temporary files after 24 hours, uploads idle for 7 days (matching AWS defaults); both timeouts configurable, and the sweep MUST NOT block startup.
- **FR-015**: Users MUST be able to copy an object within a bucket or between buckets without the content passing through the client.
- **FR-016**: Credentials and other settings MUST be configurable via CLI flags, environment variables, a `.env` file in the storage root, or the configuration file, with precedence flags > environment variables > `.env` > configuration file; the configuration file MUST be `<root>/.tinio/.tinio.toml`.
- **FR-017**: All logging MUST flow through `tracing`. Each request (method, path, status, duration) MUST be written to the access log (default `<root>/.tinio/access.log`) in an nginx/apache-compatible format configurable in the configuration file (`combined` / `common` / custom nginx-style format strings over a fixed variable set; unknown variables MUST be rejected at startup). Operational logs MUST default to stderr with `text` or `json` format (JSON defaults the filename `server.json`). Errors MUST remain visible on stderr regardless of configured destinations. Optional OpenTelemetry export MUST be available behind the `otel` feature.
- **FR-018**: The server MUST provide a management plane separate from the S3 data plane: a unix socket (`.tinio/control.sock`, mode 0600) or a Windows named pipe, token-authenticated, exposing `GET /status`, `POST /stop` (graceful shutdown with bounded in-flight drain), `GET /metrics`, and `GET /openapi.json`; binding the control channel MUST enforce single-instance semantics.
- **FR-019**: The server MUST expose Prometheus metrics at `GET /metrics` covering the HTTP layer (request count, duration, in-flight), the S3 operation layer (operation × status including error codes), and the storage layer (bucket count; object count and total bytes with TTL-cached full scans; upload/download byte counters maintained on streaming paths; in-progress multipart count).
- **FR-020**: The server MUST keep all private state in a reserved `<root>/.tinio/` directory: configuration, state file, control socket, logs, multipart parts, ETag metadata, bucket creation times. This directory MUST never be served or listed through the S3 interface, and names that could collide with it MUST be rejected.
- **FR-021**: S3 API capabilities MUST be configurable in the configuration file (`[s3]` section: multipart, copy_object, list_objects_v1, list_objects_v2, delete_objects, sig_v2); disabled capabilities MUST return standard NotImplemented errors, and unknown configuration keys MUST be rejected at startup.
- **FR-022**: Object responses MUST include ETags per S3 semantics (MD5 of content for single uploads; MD5-of-part-MD5s-`N` for multipart), persisted in the private metadata store and revalidated against file size/mtime so out-of-band modifications are detected; Content-Type is inferred from the file extension at serve time and user-supplied metadata is accepted but not persisted.

### Key Entities *(include if feature involves data)*

- **Storage Root**: The local directory configured as the storage backend; the single source of truth for all data. Contains bucket directories and the reserved `.tinio/` directory.
- **Bucket**: A top-level subdirectory of the storage root. Attributes: name (validated per S3 rules), creation time (persisted in `.tinio/buckets.json`, lazily recorded on first sight).
- **Object**: A file within a bucket directory. Attributes: key (path relative to the bucket, may contain `/`), size, last-modified time, content, ETag (persisted in the metadata store with size/mtime validation); Content-Type is inferred at serve time.
- **Credentials**: A configured access key / secret key pair used to authenticate S3 requests. Auto-generated into the configuration file on first start; generated per-session and printed once when the configuration file exists without credentials and anonymous mode is not enabled. Applies to the whole server instance.
- **Reserved Directory**: `<root>/.tinio/` — all private state (configuration, state, control socket, logs, multipart parts, metadata store, bucket creation times); never served or listed through the S3 interface.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user can complete the full journey — start server, create bucket, upload, download, list, delete — in under 5 minutes using a standard S3 client plus the provided CLI, without touching the filesystem manually.
- **SC-002**: Two widely used standard S3 clients — aws cli v2 and rclone — connect and perform all core operations with no client-side workarounds or custom configuration; interop tests run in CI on Windows, Linux, and macOS.
- **SC-003**: Objects up to 1 GB in size upload and download successfully while memory usage stays flat regardless of object size (streamed, not buffered).
- **SC-004**: 100% of automated checks covering S3 error-code behavior pass, including the edge cases listed above (missing bucket/object, traversal attempts, invalid bucket names, bad signatures).
- **SC-005**: The server is ready to serve requests within 1 second of the start command on a typical machine.
- **SC-006**: A file created or modified directly in the storage root is served to clients immediately with no refresh, restart, or sync step; the on-disk directory always mirrors what the S3 interface serves.
- **SC-007**: Management plane: `status`/`stop` round-trips complete within 1 second; starting a second instance on the same storage root fails with a clear error.
- **SC-008**: `GET /metrics` returns the three-layer metric set (HTTP, S3 operations, storage) in Prometheus text format; full-scan gauges respect the TTL cache.

## Technical Decisions & Dependencies *(constitution-mandated; dependency justification per Principle I)*

- **s3s 0.14.1** (Apache-2.0, active maintenance, `unsafe_code = "forbid"`): the S3 protocol layer — routing, XML, standard error codes, SigV4/SigV2 verification. Reimplementing the S3 protocol from scratch is out of scope for a compact tool and would be the dominant source of correctness risk; the `S3` trait keeps the storage layer swappable. MVP decision; the architecture permits replacing the protocol layer later. The framework ships no HTTP body-size or rate limiting; that is an intentional choice for a local single-user tool — object size is unlimited (see SC-003), and clients are expected to be trusted.
- **hyper / hyper-util / tokio / tokio-util**: async runtime and streaming HTTP for the data plane (hyper is s3s's native transport; the framework's own server uses the same pattern).
- **axum**: HTTP framework for the management plane (unix socket / named pipe).
- **utoipa (axum_extras)**: OpenAPI documentation for the management plane.
- **prometheus**: metrics registry and text exposition for `GET /metrics`.
- **tracing / tracing-subscriber**: all logging; format layers provide the text/JSON operational log formats.
- **opentelemetry / opentelemetry-otlp / tracing-opentelemetry** (behind the `otel` feature): optional OTLP export of tracing data.
- **serde / serde_json / toml**: configuration parsing and serialization.
- **dotenvy**: `.env` file loading (standard parsing instead of hand-rolled).
- **mime_guess**: Content-Type inference from file extensions.
- **md-5**: ETag computation (single-object MD5 and multipart composition).
- **clap**: CLI parsing.
- **time**: timestamp formatting (same ecosystem as s3s).
- **criterion** (dev): benchmarks for the streaming paths, per constitution Principle V.

## Assumptions

- **Scope**: "Compact" means a minimal S3 subset for v1: bucket CRUD, object upload/download/delete/head, listing, basic multipart uploads (initiate/upload part/complete/abort/list/part-copy), server-side copy, plus a management plane (status/stop, Prometheus metrics, OpenAPI) and observability (tracing, optional OpenTelemetry). Object versioning, bucket policies, lifecycle rules, replication, encryption at rest, checksum headers, TLS, log rotation, and a web UI dashboard are OUT OF SCOPE for v1.
- **Usage model**: Single-user or small-team local use on a developer machine or private network; not a multi-tenant production object store. Design ceiling: thousands to hundreds of thousands of objects per bucket and hundreds of GB to a few TB per storage root; no performance promises beyond that.
- **Network**: The server binds to localhost (127.0.0.1) by default; users may configure another host and port (default port 9000, matching common local S3 tooling). No TLS in v1.
- **Authentication**: Standard S3 request signing (SigV4) with configurable credentials is the default; anonymous mode exists but must be explicitly enabled, and an explicit anonymous switch overrides configured credentials with a warning logged. Credentials may come from CLI flags, environment variables, a `.env` file, or the configuration file (flags > env > `.env` > config). Without any configured credentials and without anonymous mode, session credentials are generated and printed once.
- **Mapping**: Buckets and objects map 1:1 to directories and files; all tool-owned state lives in the reserved `.tinio/` directory (never served or listed); ETag metadata is persisted in a private content-addressed store and validated against file size/mtime.
- **Data safety**: Standard filesystem semantics apply — the tool does not journal, replicate, or recover data; users back up the storage root with ordinary file tools.
- **Environment**: The tool runs on Windows, Linux, and macOS; case-sensitivity semantics follow the host filesystem (no artificial enforcement on case-insensitive hosts); the Linux unix-socket path-length limit (108 bytes) is documented as a limitation.
- **Project governance**: The feature follows the project constitution (`.specify/memory/constitution.md`): tiny core, test-first, and strict versioning discipline apply throughout; the project and all artifacts are named `tinio` (renamed from tinyio, constitution amendment 1.0.2 in progress).
