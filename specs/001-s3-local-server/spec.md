# Feature Specification: S3-Compatible Local Storage Server

**Feature Branch**: `001-s3-local-server`

**Created**: 2026-08-21

**Revised**: 2026-08-22

**Status**: Draft (design + plan review complete; Phase 2 foundational implementation complete — US1 next)

**Input**: User description: "Build a compact tool providing a Minio-like S3-compatible layer over local directories with basic CLI management."

## Clarifications

> **Note**: Implementation-level decisions made during design review (stack and workspace structure, protocol framework, private-state layout, configuration schema, logging/metrics mechanisms, management-plane transports, dependency justifications, and related hardening) are recorded in the design artifacts: [plan.md](plan.md) (structure, features, dependency list), [research.md](research.md) (decisions, alternatives, constitution Principle I justifications), [data-model.md](data-model.md) (entities), and [contracts/](contracts/) (exact schemas). This section records user-facing behavior and interface decisions; the exact schemas live in the contracts.

### Session 2026-08-21 (initial)

- Q: Should the v1 operation set include multipart uploads and server-side copy, or stay strictly compact? → A: Include basic multipart (initiate / upload part / complete / abort) and CopyObject in v1.
- Q: How should users provide server credentials to the CLI? → A: Config file, environment variables, and CLI flags are all supported; precedence is flags > environment variables > config file.
- Q: What request logging should the server do by default? → A: Each request (method, path, status, duration) is written to an access log; operational logs default to stderr; errors always remain visible on stderr. (Current logging details are specified in FR-017 and the configuration contract.)

### Session 2026-08-21 (clarify)

- Q: Should single-object uploads and downloads have a size limit? → A: No limit; any size streams, consistent with FR-010 and SC-003. The absence of built-in body/rate protection in the protocol framework is an intentional choice for a local single-user tool.
- Q: When credentials are configured and anonymous mode is explicitly enabled, which takes effect? → A: The explicit anonymous switch wins over configured credentials, with a warning logged.
- Q: How are stale temp files and interrupted multipart uploads cleaned up after a crash? → A: An asynchronous background sweep removes files by file-date (mtime) timeout: temporary write files after 24 hours, abandoned multipart uploads (no part writes and not completed) after 7 days, matching AWS's default; both timeouts configurable, and the sweep never blocks startup.
- Q: What data scale is the design ceiling? → A: Medium — thousands to hundreds of thousands of objects per bucket, hundreds of GB to a few TB per storage root; no performance promises beyond that.

### Session 2026-08-21 (plan review)

- Q: Is a fixed default port required? → A: The default port is 9000 (Minio-compatible); `--port 0` explicitly selects an OS-assigned ephemeral port (for tests), reported in logs and status.
- Q: Are conditional request headers supported? → A: Yes — If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since on Get/Head (304) and Put/Copy (412), with the Copy source evaluated per S3 semantics.
- Q: How are keys ending in `/` (folder markers) handled? → A: They are never objects — PUT creates the directory, GET/HEAD return NoSuchKey, DELETE removes an empty directory and always returns 204 (idempotent, mirroring AWS DeleteObject marker semantics — a non-empty directory is left in place).
- Q: Are key charset restrictions platform-consistent? → A: No — universal rules (traversal, absolute paths, control characters) apply everywhere; platform charset limits follow the backend (Windows-invalid characters rejected on Windows only; future backends define their own).
- Q: Does anonymous mode on first start skip credential generation? → A: No — the auto-created config still persists generated credentials; anonymous mode affects only the running session, so a later start without `--anonymous` is never silently unauthenticated.

### Session 2026-08-21 (post-review hardening)

- Q: Is a read-only mode supported? → A: Yes — `--read-only` flag / `TINIO_READ_ONLY` env / `[server] read_only = true`. In read-only mode all S3 mutating operations (bucket create/delete, object put/delete/copy, all multipart operations) are rejected with `AccessDenied`, and the storage root may be a genuinely read-only filesystem. All tool state moves out of the root to `~/.tinio/roots/<sha1(canonical root)16>/` (state file, control socket, logs, ETag meta store, bucket creation times, and — when the root has no config — the auto-created config with generated credentials). A pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` is still read but never written (lookup order: `.tinio.toml` wins when both exist); other contents of the root's `.tinio/` are ignored in read-only mode. Root discovery by walking up looks for `.tinio/`, which a read-only root may lack — pass the directory positionally (or run from the root) in that case.

### Session 2026-08-21 (post-review round 2)

- Q: How does `doctor` fix what it finds? → A: `--dry-run` reports what a fix would do; `--fix` applies it: removes stale state files and sockets, orphaned meta and `buckets.json` entries, abandoned multipart uploads, stale temp files, and home root-state dirs whose storage root no longer exists. `--fix` requires the server for that root to be stopped (live control-channel probe → error). Exit codes: 0 clean / 1 problems found (dry-run) or remaining (fix) / 2 usage.
- Q: What about nested storage roots? → A: `.tinio` becomes a reserved path segment at ANY depth: a key containing a `.tinio` segment is rejected on write (`AccessDenied`), reads return `NoSuchKey`, and listings skip such entries. This both protects the server's own state and prevents an outer root from serving an inner root's `.tinio/` (which contains credentials). Nested roots are otherwise allowed.
- Q: How does `stop` wait for exit? → A: After the 202 response, the CLI polls the control channel until the probe fails or `state` disappears (bounded ~15 s), then reports success; on timeout it reports that the server did not confirm exit.
- Q: What happens when the canonical root path changes? → A: The root's identity IS its canonical path: renaming or re-linking the root yields a new home state dir (new derived hash), and generated credentials are recreated — a documented behavior, not an error.

### Session 2026-08-21 (plan review 2)

- Q: Are symlinks in the storage root followed? → A: Yes by default — links may point outside the root (user-owned directory, documented); configurable off via `[storage] follow_symlinks` or `--no-follow-symlinks`, in which case access resolving through a symlink is rejected and symlink entries are excluded from listings.
- Q: Is there a diagnostic CLI command? → A: Yes — `tinio doctor` (offline): inspects config validity, on-disk bucket/object key validity, and `.tinio/` integrity (stale state/socket, orphaned metadata, abandoned multipart, stale temps, symlinks when disabled, low disk space); exit 0 clean / 1 problems; optional `--json`.

### Session 2026-08-21 (Minio compatibility)

- Q: What is the default S3 port? → A: 9000 (matching Minio) when no port is given; `--port 0` explicitly selects an OS-assigned ephemeral port (for tests and multi-instance development).
- Q: What is the default management API address? → A: When exposed over TCP without an explicit port or scheme, the management API defaults to HTTP on 127.0.0.1:9001 (Minio's console-port convention); HTTPS requires an explicit `https://` scheme.
- Q: What is the CLI invocation style? → A: Minio-style — `tinio server <dir>` (with `start` as an alias) takes the storage root as a positional directory argument (`--root` removed); `status`/`stop`/`doctor` take the same positional directory; `--address HOST:PORT` is the Minio-style alias for host/port.
- Q: Which environment variables are supported? → A: `TINIO_*` names take precedence; credential variables additionally fall back to their `MINIO_*` equivalents (MINIO_ACCESS_KEY/MINIO_SECRET_KEY and MINIO_ROOT_USER/MINIO_ROOT_PASSWORD) so Minio-oriented environments work unchanged.
- Q: How is the scanner configured? → A: Minio-aligned keys under the presence-gated `[scanner]` section — `delay` / `max_wait` / `cycle` (matching `mc admin config set myminio scanner ...`). The complete Minio-alignment surface (invocation, ports, env fallback, deviations) is specified in the Minio-compatibility contract.

### Session 2026-08-22 (performance checklist review)

- Q: Does the streaming guarantee cover all transfer paths? → A: Yes — FR-010 now states that single-object get/put, ranged partial reads (a Range request reads and emits only the requested window), multipart part upload and assembly, and server-side copy all stream with bounded buffers; no path may buffer the full object.
- Q: What does "ready" mean for SC-005, and may background work delay it? → A: Ready = the data plane is bound and accepting requests (configuration loaded, single-instance check done, listeners up); the ETag scanner and sweep MAY continue after readiness and MUST NOT delay it (FR-024 cross-referenced).
- Q: How long is the in-flight drain bound on graceful stop? → A: 10 s — the value already present in the management API contract is now stated in FR-018.
- Q: How long is the full-scan metric TTL cache? → A: 30 s — the value already present in the management API contract is now stated in FR-019, bounding scrape cost at the design ceiling.
- Q: Is any listing latency bound promised? → A: No — the first listing of externally-added files MAY include a one-time full-content read; no listing latency bound is promised within the design ceiling; listings remain correct and complete at all times, and the background scanner makes repeated listings cheap.
- Q: Does the background sweep yield to request traffic like the scanner? → A: Yes — FR-014 now requires it.
- Q: What performance guarantees exist under concurrent clients, many-small-object workloads, or degraded hardware? → A: None — only correctness guarantees apply (last-write-wins with no torn objects; mid-operation failures fail the request while the server keeps running); documented as a non-goal in Assumptions.
- Q: How is the constitution-V benchmark obligation operationalized? → A: criterion benchmark set (streaming write/read, multipart assembly, listing) with CI smoke runs; a mean slowdown > 10 % against recorded baselines counts as a regression requiring a documented decision and reviewer approval; allocation discipline is scoped to streaming hot paths (plan.md).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Serve Local Directories Over an S3-Compatible Interface (Priority: P1)

A user starts the tool pointing at a local directory (the storage root). The tool listens on a local HTTP port and speaks the S3 API: buckets map to top-level subdirectories and objects map to files inside them. The user points any standard S3 client at the endpoint and can create and delete buckets, upload objects, download objects, list objects, and remove objects — exactly as they would against a hosted S3 service, with no client-side configuration tricks.

**Why this priority**: This is the core value of the feature — an S3-compatible layer that makes local files usable from mainstream standard S3 clients (backup scripts, upload tools, SDKs); the supported client sets are defined in FR-025. Without it the feature does not exist.

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
- What happens when an upload is interrupted mid-transfer? The server MUST NOT leave a partial object visible as a completed object. (FR-011)
- How does the server behave with very large objects? Transfers MUST stream without buffering the whole object in memory. (FR-010)
- What happens when an upload exceeds any size limit? There is no size limit; arbitrarily large objects MUST stream without buffering. The absence of body-size and rate limits is an intentional choice for a local single-user tool, not an omission. (FR-010)
- What happens when a bucket name violates S3 naming rules? The server MUST reject it with the standard S3 "invalid bucket name" error.
- What happens when a request references the reserved `.tinio` directory? It MUST never appear in bucket or object listings, and bucket names starting with `.` are rejected by S3 naming rules. `.tinio` is reserved at any depth: keys containing a `.tinio` segment are rejected on write, return `NoSuchKey` on read, and are skipped in listings (so a nested root's state is never served by an outer server).
- What happens when one storage root is nested inside another? Allowed — each server is independent; the any-depth `.tinio` reservation (FR-020) prevents state leakage from inner to outer.
- What happens when files are modified directly on disk after ETag metadata was persisted? The served ETag MUST be revalidated against file size/mtime and recomputed when they differ; the served Last-Modified MUST reflect the file's actual state.
- What happens on case-insensitive filesystems (Windows/macOS) when two bucket or object names differ only in case? Host filesystem semantics apply; no artificial enforcement is added.
- What happens when an object key resolves through a symlink? Symlinks are followed by default (the root is user-owned; links may point outside it — documented); when `follow_symlinks` is disabled, such requests are rejected and symlink entries are excluded from listings.
- What happens when the server binds a non-loopback address? Allowed and documented (layered trust model, Assumptions/Network): startup prints a stderr warning — basic for a non-loopback bind, escalated to a prominent warning when anonymous mode is also enabled.
- What happens when the storage root is read-only or a path becomes inaccessible mid-operation? A genuinely read-only storage root is supported via read-only mode (FR-023), in which the server runs normally with all state under the user's home directory; without read-only mode, startup fails with a clear error if the reserved directory cannot be created or written. A path that becomes inaccessible mid-operation fails that request with a meaningful error while the server keeps running.
- What happens when the server crashes and leaves the control socket behind? The next start probes the socket: if the connection is refused the stale file is removed and the bind retried; a successful probe means a live instance and the start fails with the single-instance error.
- How does the server respond when the configured port is already in use? It MUST report a clear error at startup and exit cleanly.
- How does the server respond when a second instance is started on the same storage root? It MUST report a clear single-instance error at startup and exit cleanly.
- How are object metadata timestamps handled when a file is modified directly on disk? The served last-modified time MUST reflect the file's actual state.
- What happens when a multipart upload is aborted or never completed? Incomplete uploads MUST NOT appear as objects, and abort MUST remove any parts already uploaded; uploads that stay idle for the configured timeout (default 7 days) are removed by the asynchronous background sweep, as are temporary write files older than their timeout (default 24 hours).
- What happens when the user presses Ctrl+C or sends SIGTERM to a foreground server? The server performs the same graceful shutdown as `POST /stop` (cease accepting, bounded (10 s) drain, state file and socket removed, exit 0); the full signal behavior — second-signal immediate exit, SIGHUP ignored, Windows console-close events — is specified in the CLI contract (contracts/cli.md, Signals).
- What happens when the server starts after a crash or a forced kill? Before readiness, startup performs a fast, deterministic repair of the private state (itemized in failure-handling.md §3): stale `state`/socket, a full clear of `tmp/`, multipart subtrees whose bucket directory no longer exists (cross-restart uploads stay intact, quickstart §7), and stale `buckets.json` entries. Orphaned ETag meta entries are reclaimed in the background by the scanner (FR-024) rather than delaying startup (SC-005). User data — bucket directories and objects — is never touched; every repair action is logged to the operational log. `tinio doctor` checks the same problems offline and repairs them with `--fix`. (FR-014)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The tool MUST serve an S3-compatible interface over HTTP for a user-chosen local storage root, mapping buckets to top-level subdirectories and objects to files.
- **FR-002**: Users MUST be able to create and delete buckets through the S3 interface, with corresponding directories created and removed on disk.
- **FR-003**: Users MUST be able to upload, download, and delete objects through the S3 interface; uploaded content MUST appear as a file in the storage root and downloads MUST return byte-identical content.
- **FR-004**: Users MUST be able to list buckets, and list objects within a bucket including prefix filtering and delimiter-based grouping per standard S3 semantics.
- **FR-005**: The server MUST respond to error conditions with the standard S3 error codes (bucket not found, key not found, invalid bucket name, authentication failure, and so on) so standard clients report failures correctly.
- **FR-006**: The server MUST reject any request whose object key could address a path outside the storage root (path traversal, absolute paths); such requests MUST fail without touching the filesystem.
- **FR-007**: The tool MUST provide CLI subcommands to start the server (with configurable host, port, storage root, credentials, logging settings, daemon mode, and symlink policy), report server status, stop the server, and diagnose the target directory (`doctor` — config validity, on-disk key validity, reserved-directory integrity; exit 0 clean / 1 problems; optional `--json`; `--dry-run`/`--fix` per the CLI contract). On first start the tool MUST auto-create the configuration file with generated credentials; there is no separate init command, and no CLI data commands are provided (bucket/object operations are performed directly on the filesystem).
- **FR-008**: The server MUST support authenticated requests using the standard S3 request-signing scheme (SigV4) with user-configured credentials, and MUST reject requests with missing or invalid signatures with an authentication error.
- **FR-009**: The server MUST support an anonymous mode (no credentials required), enabled only by explicit configuration (flag or environment variable); an explicit anonymous switch takes precedence over configured credentials, with a warning logged.
- **FR-010**: Object transfers MUST stream; serving or receiving an object MUST NOT require buffering the full object in memory regardless of object size. The streaming guarantee covers ALL transfer paths: single-object get/put, ranged partial reads (a Range request MUST read and emit only the requested window), multipart part upload and assembly (FR-014), and server-side copy (FR-015).
- **FR-011**: Concurrent writes to the same object MUST resolve to a complete last-write-wins result; an interrupted upload MUST NOT leave a partial object visible as a completed object.
- **FR-012**: Bucket names MUST be validated against standard S3 naming rules (3-63 characters, lowercase letters, digits, dots, and hyphens, no leading/trailing dot or hyphen) and invalid names MUST be rejected. The s3s framework's `check-bucket-name` validation is authoritative; this requirement documents the contract subset, and the backend re-validates on create without diverging from the framework rules.
- **FR-013**: Files placed in the storage root outside the S3 interface MUST become immediately visible and served without restart; objects written through the interface MUST be immediately visible on disk.
- **FR-014**: Users MUST be able to upload objects via multipart (initiate, upload part, complete, abort, list parts, list uploads, server-side part copy); an incomplete or aborted multipart upload MUST NOT be visible as a completed object. Part size minimums are not enforced. Stale temporary write files and abandoned multipart uploads MUST be removed by an asynchronous background sweep based on file-date (mtime) timeouts — temporary files after 24 hours, uploads idle for 7 days (matching AWS defaults); both timeouts configurable, and the sweep MUST NOT block startup; it MUST also yield to request traffic like the scanner (FR-024).
- **FR-015**: Users MUST be able to copy an object within a bucket or between buckets without the content passing through the client.
- **FR-016**: Credentials and other settings MUST be configurable via CLI flags, environment variables, a `.env` file in the reserved directory (`<root>/.tinio/.env`; read-only-mode placement per the configuration contract), or the configuration file, with precedence flags > environment variables > `.env` > configuration file; the configuration file MUST be `<root>/.tinio/.tinio.toml` (path details, the `config.toml` alias, and read-only-mode placement are specified in the configuration contract).
- **FR-017**: The server MUST log each request (method, path, status, duration) to an access log in an nginx/apache-compatible format, configurable in the configuration file (`combined` / `common` / custom format strings over a fixed variable set; unknown format variables MUST be rejected at startup); the access log's destination and format details are specified in the configuration contract. Operational logs MUST default to stderr with `text` or `json` format. Errors MUST remain visible on stderr regardless of configured destinations. An optional OpenTelemetry export MUST be available (opt-in). The fixed access-log variable set is a security property: it cannot reference the Authorization header, query strings, or credentials, so access logs cannot contain secrets — the set MUST NOT be extended without revisiting this guarantee.
- **FR-018**: The server MUST provide a management plane separate from the S3 data plane: token-authenticated, exposed by default over a local unix socket (or Windows named pipe), exposing `GET /status`, `POST /stop` (graceful shutdown with a bounded (10 s) in-flight drain), `GET /metrics`, and `GET /openapi.json`; binding the control channel MUST enforce single-instance semantics. The management API MUST additionally be exposable over TCP HTTP or HTTPS per configuration, in which case ALL endpoints require the token. (Optional in minimal builds; transport and endpoint details are specified in the management API contract. Signal-triggered shutdown is a CLI `server` behavior — specified in the CLI contract, contracts/cli.md.)
- **FR-019**: The server MUST expose Prometheus metrics at `GET /metrics` covering the HTTP layer (request count, duration, in-flight), the S3 operation layer (operation × status including error codes), and the storage layer (bucket count; object count and total bytes with TTL-cached full scans; upload/download byte counters maintained on streaming paths; in-progress multipart count). Full-scan gauges are TTL-cached (30 s) so scrape cost is bounded at the design ceiling; metric recording is per-request counter updates and MUST NOT measurably degrade request handling.
- **FR-020**: The server MUST keep all private state in a reserved directory (`.tinio/` in the storage root; under the user's home directory in read-only mode, FR-023): configuration, state file, control socket, logs, multipart parts, ETag metadata, bucket creation times. This directory MUST never be served or listed through the S3 interface, and names that could collide with it MUST be rejected. `.tinio` is a reserved path segment at ANY depth: object keys containing a `.tinio` segment MUST be rejected on write (`AccessDenied`), MUST return `NoSuchKey` on read, and MUST be skipped in listings — so an outer root can never serve a nested root's state (which contains credentials).
- **FR-021**: S3 API capabilities MUST be configurable (multipart, copy_object, list_objects_v1, list_objects_v2, delete_objects, sig_v2); disabled capabilities MUST return standard NotImplemented errors, and unknown configuration keys MUST be rejected at startup. (Capability groups are additionally strippable at compile time — see the implementation plan.) `sig_v2` is retained for Minio-era client compatibility but marked deprecated: off by default, enabling it prints a startup warning, and it is slated for removal in v2.
- **FR-022**: Object responses MUST include ETags per S3 semantics (MD5 of content for single uploads; MD5-of-part-MD5s-`N` for multipart), persisted in the private metadata store and revalidated against file size/mtime so out-of-band modifications are detected; Content-Type is inferred from the file extension at serve time and user-supplied metadata is accepted but not persisted. Listings MUST include ETags; missing or stale entries are recomputed during the listing and persisted — the one-time full-content pass over externally-added files is accepted as the cost of SC-006 mirror semantics, mitigated by the background scanner (FR-024). No listing latency bound is promised within the design ceiling; listings remain correct and complete at all times, and the background scanner makes repeated listings cheap. The size/mtime validation has a known granularity limit: an out-of-band edit that preserves both size and mtime tick may serve a stale ETag.
- **FR-023**: The server MUST support a read-only mode (CLI flag `--read-only`, `TINIO_READ_ONLY` env, or `[server] read_only = true`) in which all S3 mutating operations (bucket create/delete, object put/delete/copy, all multipart operations) MUST be rejected with the standard `AccessDenied` error and the storage root is never written to — it may be a genuinely read-only filesystem. In this mode all tool state (state file, control socket, logs, ETag meta store, bucket creation times, auto-created configuration with generated credentials) MUST live under `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700); a pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` MUST still be read but never written (`.tinio.toml` wins when both exist).
- **FR-024**: The server MUST avoid slow first listings over externally-populated directories: ETag metadata MUST be pre-computed in the background after startup (configurable and rate-limitable, never blocking startup (SC-005 readiness), yielding to request traffic, aborting quietly on shutdown), so externally-added files become cheap to list; listings MUST remain correct when the feature is disabled. The scanner MUST also reclaim orphaned meta entries (meta entries whose object file no longer exists) during its scan, keeping the meta store free of dead entries. (Implementation details in the research document.)
- **FR-025**: S3 client support follows a three-set compatibility contract: (1) *mandated* — aws cli v2 and rclone, which MUST complete all core operations with no client-side workarounds or custom configuration, verified in CI on Windows, Linux, and macOS (acceptance per SC-002); (2) *best-effort* — boto3 and mc (MinIO Client), expected to complete core bucket/object operations against a standard endpoint, with their known behavior deviations documented in the S3 surface contract (`x-amz-checksum-*` headers ignored in v1, user `x-amz-meta-*` headers accepted and dropped, Content-Type inferred at serve time) and NOT part of the CI interop gate; (3) *unsupported* — all other S3 clients and SDKs are not guaranteed to interoperate; compatibility issues with them are accepted as-is. Only the mandated set is release-gating.

### Key Entities *(include if feature involves data)*

- **Storage Root**: The local directory configured as the storage backend; the single source of truth for all data. Contains bucket directories and the reserved `.tinio/` directory.
- **Bucket**: A top-level subdirectory of the storage root. Attributes: name (validated per S3 rules), creation time (persisted in the tool's private state, lazily recorded on first sight).
- **Object**: A file within a bucket directory. Attributes: key (path relative to the bucket, may contain `/`), size, last-modified time, content, ETag (persisted with size/mtime validation); Content-Type is inferred at serve time.
- **Credentials**: A configured access key / secret key pair used to authenticate S3 requests. Auto-generated into the configuration file on first start; generated per-session and printed once when the configuration file exists without credentials and anonymous mode is not enabled. Applies to the whole server instance.
- **Reserved Directory**: The tool's private state directory (`.tinio/` in the storage root; under the user's home directory in read-only mode) — configuration, state, control socket, logs, multipart parts, metadata store, bucket creation times; never served or listed through the S3 interface.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user can complete the full journey — start server, create bucket, upload, download, list, delete — in under 5 minutes using a standard S3 client plus the provided CLI, without touching the filesystem manually.
- **SC-002**: Two widely used standard S3 clients — aws cli v2 and rclone — connect and perform all core operations with no client-side workarounds or custom configuration; interop tests run in CI on Windows, Linux, and macOS.
- **SC-003**: Objects up to 1 GB in size upload and download successfully while memory usage stays flat regardless of object size (streamed, not buffered).
- **SC-004**: 100% of automated checks covering S3 error-code behavior pass, including the edge cases listed above (missing bucket/object, traversal attempts, invalid bucket names, bad signatures).
- **SC-005**: The server is ready to serve requests within 1 second of the start command on a typical machine. Ready means the data plane is bound and accepting requests (configuration loaded, single-instance check done, listeners up); background work (ETag scanner, sweep) MAY continue after readiness and MUST NOT delay it. Measurement baseline: CI matrix machines and a typical development machine, with values recorded in the benchmark report.
- **SC-006**: A file created or modified directly in the storage root is served to clients immediately with no refresh, restart, or sync step; the on-disk directory always mirrors what the S3 interface serves.
- **SC-007**: Management plane: `status`/`stop` round-trips complete within 1 second; starting a second instance on the same storage root fails with a clear error.
- **SC-008**: `GET /metrics` returns the three-layer metric set (HTTP, S3 operations, storage) in Prometheus text format; full-scan gauges respect the TTL cache.

## Assumptions

- **Scope**: "Compact" means a minimal S3 subset for v1: bucket CRUD, object upload/download/delete/head, listing, basic multipart uploads (initiate/upload part/complete/abort/list/part-copy), server-side copy, plus a management plane (status/stop, Prometheus metrics, OpenAPI) and observability (logging with optional OpenTelemetry export). Object versioning, bucket policies, lifecycle rules, replication, encryption at rest, checksum headers, TLS, CORS headers, log rotation, and a web UI dashboard are OUT OF SCOPE for v1.
- **Usage model**: Single-user or small-team local use on a developer machine or private network; not a multi-tenant production object store. Design ceiling: thousands to hundreds of thousands of objects per bucket and hundreds of GB to a few TB per storage root; no performance promises beyond that. No throughput or latency guarantees are made for concurrent clients, many-small-object workloads, or degraded hardware (slow disks, limited memory); only correctness guarantees apply (last-write-wins with no torn objects; mid-operation failures fail the request while the server keeps running).
- **Network**: The server binds to localhost (127.0.0.1) by default and listens on port 9000 (matching Minio) unless configured; `--port 0` explicitly selects an OS-assigned ephemeral port (reported in logs and state). When the management API is exposed over TCP without an explicit port or scheme, it defaults to HTTP on port 9001 (Minio's console-port convention). No TLS on the S3 data plane in v1 — a documented v2 candidate, not a permanent exclusion. The trust model is layered: the default loopback binding trusts the local user; every configured exposure surface (non-loopback binding, anonymous mode, TCP management plane) carries its own mitigations and an explicit startup warning on stderr — a basic warning for a non-loopback bind, escalated to a prominent warning when combined with anonymous mode.
- **Authentication**: Standard S3 request signing (SigV4) with configurable credentials is the default; anonymous mode exists but must be explicitly enabled, and an explicit anonymous switch overrides configured credentials with a warning logged. Credentials may come from CLI flags, environment variables, a `.env` file, or the configuration file (flags > env > `.env` > config). Passing credentials via CLI flags or environment variables exposes them to process listings, shell history, and child-process environments — an accepted trade-off for a local single-user tool; the config file (mode 0600) is the recommended channel. Without any configured credentials and without anonymous mode, session credentials are generated and printed once. Generated credentials use a CSPRNG with at least 32 bytes for the secret key and 16 bytes for the access key. SigV4 timestamp validation (the `x-amz-date` clock-skew window) is delegated to the s3s protocol layer per AWS convention (±15 minutes); the actual window is verified during implementation and recorded in the implementation notes.
- **Mapping**: Buckets and objects map 1:1 to directories and files; all tool-owned state lives in the reserved directory (never served or listed); ETag metadata is kept in the tool's private state and validated against file size/mtime so out-of-band modifications are detected.
- **Data safety**: Standard filesystem semantics apply — the tool does not journal, replicate, or recover data; users back up the storage root with ordinary file tools.
- **Environment**: The tool runs on Windows, Linux, and macOS; case-sensitivity semantics follow the host filesystem (no artificial enforcement on case-insensitive hosts); the Linux unix-socket path-length limit (108 bytes) is documented as a limitation. A read-only mode serves genuinely read-only storage roots with all tool state under `~/.tinio/roots/<sha1(canonical root)16>/` (FR-023). S3 clients are expected to use path-style addressing against the endpoint (virtual-hosted addressing is not configured in v1); interop tests verify aws cli v2 and rclone work against `127.0.0.1` endpoints without client-side addressing overrides. A root's identity is its canonical path — renaming or re-linking the root yields a new derived home state dir and regenerated credentials (documented behavior). Secret-bearing state (state dir, `state`, config) is mode 0600/0700 on unix and ACL-restricted to the current user on Windows.
- **Project governance**: The feature follows the project constitution (`.specify/memory/constitution.md`): tiny core, test-first, and strict versioning discipline apply throughout; the project and all artifacts are named `tinio` (renamed from tinyio, constitution amendment 1.0.2 in progress).
