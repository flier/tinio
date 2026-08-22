# Data Model: S3-Compatible Local Storage Server

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

Entities derived from the feature spec (Key Entities + FR-001..024). The filesystem is the single source of truth; every entity below maps to concrete on-disk state. `<state-dir>` below means `<root>/.tinio/` in normal mode and `~/.tinio/roots/<sha1(canonical root)16>/` in read-only mode (FR-023).

**Backend seam**: `tinio-core` defines the storage contract (`Storage` + `Cleanup` traits, domain newtypes in `bucket`/`object`/`etag`/`multipart`, conformance harness); `tinio-fs` implements it over the local filesystem (v1); `tinio-mem` provides the in-memory reference backend (CLI default when no directory is given); planned backends (`tinio-s3`, `tinio-webdav`) implement the same trait. The FS-specific details in this document (meta store, sweep, buckets.json, tmp/) are `tinio-fs` internals behind that contract.

## Entities

### Storage Root

| Field | Type | Notes |
|-------|------|-------|
| path | `PathBuf` | User-chosen local directory; canonicalized at startup |
| buckets | `[Bucket]` | Top-level subdirectories (excluding reserved `.tinio/`) |
| reserved_dir | `PathBuf` | `<root>/.tinio/` — never served or listed (FR-020); in read-only mode (FR-023) the state dir is `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700) instead, and the root is never written |

Validation: canonicalization before use; unix-socket path limit (108 bytes) documented limitation. In read-only mode the root need not be writable at all.

### Bucket

| Field | Type | Notes |
|-------|------|-------|
| name | `String` | Validated per S3 rules: 3–63 chars, lowercase letters/digits/dots/hyphens, no leading/trailing dot or hyphen (FR-012; framework `check-bucket-name` feature enforces, backend re-validates on create) |
| creation_time | timestamp | Persisted in `<state-dir>/buckets.json`; lazily recorded on first sight (pre-existing dirs) |
| path | `PathBuf` | `<root>/<name>` |

Relationships: contains 0..n `Object`s. State: exists ⇔ directory exists. Delete: only when empty (standard S3 `BucketNotEmpty`); removes the directory and the meta-store subtree + buckets.json entry (lazy cleanup of orphans).

Case sensitivity follows the host filesystem (amended assumption): on case-insensitive hosts, names differing only in case collide at the FS level and no artificial enforcement is applied.

### Object

| Field | Type | Notes |
|-------|------|-------|
| key | `String` | Path relative to the bucket; may contain `/` (nested dirs); MUST NOT contain traversal (`..`, absolute) or control characters (FR-006, universal across backends); a `.tinio` path segment is reserved at ANY depth (FR-020): writes rejected with `AccessDenied`, reads return `NoSuchKey`, listings skip such entries; platform charset restrictions follow the backend (Windows-invalid characters rejected on Windows only); keys ending in `/` are folder markers, not objects; empty key invalid; zero-length content valid |
| size | `u64` | From filesystem metadata |
| last_modified | timestamp | Filesystem mtime (actual file state, FR edge case) |
| etag | `ETag` | Contract type in `tinio_core::etag` — raw 16-byte MD5 (`Single`) or composed multipart form (`Composed` + part count); wire format `"<md5hex>"` / `"<md5hex>-N"` via `Display`/`as_str` (FR-022); persisted in meta JSON as hex strings |
| content | file | Streamed, never buffered whole (FR-010) |
| content_type | inferred | From extension via `mime_guess`, fallback `application/octet-stream`; not persisted (FR-022) |
| user metadata | dropped | `x-amz-meta-*` accepted, not stored, not returned |

ETag persistence: meta entry `<state-dir>/meta/objects/<bucket>/<2hex>/<sha1hex>.json` = `{key, etag, size, mtime}`; served only when size+mtime match, else recomputed streaming and rewritten (out-of-band modification detection). Orphaned meta entries removed on object delete, lazily on bucket delete. Known granularity limit: an out-of-band edit preserving both size and mtime tick may serve a stale ETag (accepted trade-off, FR-022). Meta files are written atomically (temp file + rename) under an in-process lock, so concurrent writers never produce torn JSON. Listings include ETags: missing/stale entries are recomputed synchronously during the listing — a one-time full-content pass over externally-added files (documented cost of SC-006).

State: absent → exists → (overwritten atomically) → deleted. Concurrent writes: last completed atomic rename wins; never a torn mix (FR-011). Symlinks: followed by default (links may point outside the root — user-owned directory, documented); when `follow_symlinks` is disabled, access resolving through a symlink is rejected and symlink entries are excluded from listings.

### Multipart Upload

| Field | Type | Notes |
|-------|------|------|
| upload_id | `String` | UUID v4 (`uuid` crate); unique per upload |
| bucket, key | `String` | Target object |
| parts | `[(part_number, file)]` | `<state-dir>/multipart/<bucket>/<uploadId>/part-<n>`; part numbers 1..=10000 |
| initiated_at | timestamp | For the 7-day idle expiration |

State transitions: `created` → `uploading` (0..n part writes) → `completed` (assembly + atomic rename to object path; composed ETag written to meta store) | `aborted` (parts subtree removed) | `expired` (async sweep, idle > 7 days — idle means no part writes and not completed, measured as max(initiated_at, latest part mtime); configurable TTL). No 5 MB minimum enforced (FR-014). In read-only mode (FR-023) all multipart operations are rejected with `AccessDenied`, so no part files are ever created.

### Credentials

| Field | Type | Notes |
|-------|------|------|
| access_key, secret_key | `String` | Whole-server instance pair |
| source | enum | flags / env / `.env` / config file / generated |
| anonymous | `bool` | Explicit flag or env; wins over configured credentials with a warning (FR-009) |

Generation rules: first `start` without config → config auto-created with random credentials (persisted; in read-only mode, to the home state dir when the root has no config); config present without credentials and no anonymous → session credentials generated and printed once (daemon: into the log). Rotation = edit config + restart (amended P3.4). Anonymous mode is not settable via the config file — an `anonymous` key in `[auth]` is an unknown-key startup error (deliberate: anonymous must be an explicit per-invocation choice via flag/env). Environment: `TINIO_*` takes precedence, with a `MINIO_*` fallback for credentials (`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` legacy, `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` modern).

### Reserved Directory (`.tinio/`)

Normal mode: `<root>/.tinio/`. Read-only mode (FR-023): `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700) — same layout under the state dir; `<root>` is then never written, and a pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` is still read (but never modified; `.tinio.toml` wins when both exist; other root-`.tinio/` contents are ignored). The home base is resolved via the `dirs` crate.

| Entry | Purpose |
|-------|---------|
| `.tinio.toml` | Config file (auto-created first start; top-level `version = 1` for future format migration) |
| `.env` | Optional env file (loaded if present; in read-only mode only the state dir's `.env` is loaded) |
| `state` | `{version, pid, token, port, started_at, control_name}` JSON, mode 0600 |
| `tinio.sock` | Unix socket (Linux/macOS; stale file probed + unlinked before bind; configured via `[api.unix] path`); Windows: named pipe, name in state (configured via `[api.pipe] path` / `--api pipe://`) |
| `access.log` | Data-plane access log |
| `server.log` / `server.json` | Operational log (daemon mode; json = format-selected name) |
| `buckets.json` | Bucket name → creation time (`{"version": 1, "buckets": {...}}`; written atomically: temp + rename under an in-process lock) |
| `meta/objects/<bucket>/<2hex>/<hash>.json` | ETag metadata store |
| `multipart/<bucket>/<uploadId>/part-<n>` | Multipart parts (unused in read-only mode) |
| `tmp/` | In-flight write temp files (swept after 24 h; unused in read-only mode) |

Never served or listed; names that could collide (leading-dot buckets) rejected (FR-020).

### Server Instance

| Field | Type | Notes |
|-------|------|------|
| pid | `u32` | In state file |
| token | `String` | Random, per run; required on management `status`/`stop` |
| port | `u16` | Default 9000 (Minio-compatible); `0` = OS-assigned ephemeral (tests; actual port in logs/state); explicit value = fixed port |
| control channel | unix socket / named pipe | Bind failure = single-instance error. Stale unix socket: probe first — connect refused → unlink and rebind; connect succeeds → genuine second-instance error. Windows pipe created with `FILE_FLAG_FIRST_PIPE_INSTANCE`. With the `api` feature compiled in, at least one transport must be enabled (startup error otherwise) |
| management listeners | local channel (Linux/macOS: `[api.unix]`; Windows: `[api.pipe]`; on by default) + optional TCP HTTP/HTTPS (`[api.http]` / `[api.https]`, or `--api <URL>` flag) — transports are three-choose-one (exactly one enabled); crate `tinio-api`, feature `api` (default on) | TCP exposure requires token on all endpoints |
| scanner task | background | Low-priority ETag scanner (FR-024; Minio-aligned name): streams MD5 for missing/stale meta entries across the tree after startup; default on (`[scanner]` section present in the auto-created config), paced by `[scanner] delay` with `max_wait`/`cycle` (Minio-aligned keys); yields to request traffic; aborts on shutdown; runs in read-only mode too (meta → home state dir) |
| file permissions | — | unix: state dir 0700, `state`/config 0600; Windows: ACL restricted to the current user (0600 equivalent) |

State transitions: `stopped` → `starting` (bind port + control channel, write state) → `running` (scanner task launches) → `draining` (stop: cease accepting, ≤10 s in-flight drain, scanner aborts) → `stopped` (graceful stop removes the socket file and `state`). The `stop` CLI waits for exit by polling the control channel until the probe fails or `state` disappears (bounded ~15 s), then reports; on timeout it reports that exit was not confirmed. Crash recovery: state file and socket may be stale — `status` probes the control channel, `start` probes and reclaims a stale socket; temp files/multipart handled by the async sweep. Bare `--no-default-features` builds (no `api` feature) have no state file, no control channel, and no single-instance enforcement — a documented risk (ephemeral default ports remove the port-conflict fallback). A root's identity is its canonical path: renaming/re-linking the root yields a new derived home state dir and regenerated credentials (documented).

## Validation Rules (from requirements)

- FR-006: object key traversal/absolute-path rejection before any FS access.
- FR-012: bucket name rules; `.`-prefix implies reserved-name rejection.
- FR-009: anonymous explicit-only; precedence over creds with warning.
- FR-016: config sources precedence flags > env > `.env` > file; unknown config keys fail startup.
- FR-021: `[s3]` capability toggles; disabled ops return NotImplemented; unknown keys fail startup.
- FR-017: unknown access-log format variables fail startup.
- FR-022: ETag served only on size/mtime match; Content-Type inferred; user metadata dropped.
- FR-023: read-only mode rejects all S3 mutating ops with `AccessDenied`; state dir relocates to `~/.tinio/roots/<sha1(canonical root)16>/`; root never written.
- FR-024: background ETag scanner — low priority, never blocks startup, rate-limitable, disableable, aborts on shutdown.
- FR-020 (any-depth): `.tinio` is a reserved path segment at every level, not just the root.

## Metrics (read-only views, not persisted)

HTTP: `tinio_http_requests_total{method,status}`, `tinio_http_request_duration_seconds{method}`, `tinio_http_in_flight`. S3: `tinio_s3_operations_total{op,status}`, `tinio_s3_operation_duration_seconds{op}`. Storage: `tinio_storage_buckets_total`, `tinio_storage_objects_total` (TTL 30 s), `tinio_storage_bytes_total` (TTL 30 s), `tinio_storage_upload_bytes_total`, `tinio_storage_download_bytes_total`, `tinio_storage_objects_uploaded_total{op}`, `tinio_storage_objects_deleted_total`, `tinio_storage_multipart_in_progress`.
