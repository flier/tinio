# Data Model: S3-Compatible Local Storage Server

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

Entities derived from the feature spec (Key Entities + FR-001..024). The filesystem is the single source of truth; every entity below maps to concrete on-disk state. `<state-dir>` below means `<root>/.tinio/` — read-write in normal mode and opened **read-only** in read-only mode, where the **tool** state relocates to `~/.tinio/roots/<sha256(canonical root)16>/` (FR-023, amended 2026-10-03).

**Backend seam**: `tinio-core` defines the storage contract (`Storage` + `Cleanup` traits, domain newtypes in `bucket`/`object`/`etag`/`multipart`, conformance harness); `tinio-fs` implements it over the local filesystem (v1); `tinio-mem` provides the in-memory reference backend (**nothing selects it**: the CLI declares its `mem` feature and a run with no `DIR` walks up to the nearest ancestor containing `.tinio/`, so a root that cannot be found is a usage error naming the remedy — `contracts/cli.md:17` — never an in-memory server); planned backends (`tinio-s3`, `tinio-webdav`) implement the same trait. The FS-specific details in this document (meta store, sweep, `meta.redb`, tmp/) are `tinio-fs` internals behind that contract.

## Entities

### Storage Root

| Field | Type | Notes |
|-------|------|-------|
| path | `PathBuf` | User-chosen local directory; canonicalized at startup |
| buckets | `[Bucket]` | Top-level subdirectories (excluding reserved `.tinio/`) |
| reserved_dir | `PathBuf` | `<root>/.tinio/` — never served or listed (FR-020); in read-only mode (FR-023, amended 2026-10-03) it is still the storage's state dir, opened read-only, while the tool state relocates to `~/.tinio/roots/<sha256(canonical root)16>/` (mode 0700), and the root is never written |

Validation: canonicalization before use; unix-socket path limit (108 bytes) documented limitation. In read-only mode the root need not be writable at all.

### Bucket

| Field | Type | Notes |
|-------|------|-------|
| name | `String` | Validated per S3 rules: 3–63 chars, lowercase letters/digits/dots/hyphens, no leading/trailing dot or hyphen (FR-012; framework `check-bucket-name` feature enforces, backend re-validates on create) |
| creation_time | timestamp | Persisted in the `BUCKETS` table of `<state-dir>/meta.redb`; lazily recorded on first sight (pre-existing dirs) |
| owner | canonical account ID | The bucket's owning S3 principal (FR-033): a 64-hex canonical ID, empty wire when no-identity (the auth layer resolves the lazy default owner); the creator records it at create time; persisted as the `owner_wire` element of the `BUCKETS` row |
| acl | grant set | The bucket's ACL (FR-033): canonical grants wire (`acl_wire` row element; default private = the owner's own FULL_CONTROL); `PutBucketAcl` re-places the grant set, the owner element preserved by the store |
| path | `PathBuf` | `<root>/<name>` |

Relationships: contains 0..n `Object`s. State: exists ⇔ directory exists. Delete: only when empty (standard S3 `BucketNotEmpty`); the directory is **unpublished** first — renamed onto `<root>/.tinio/deleting/<id>` (same volume as the name, so a relocated state dir cannot force EXDEV) — then the bucket's metadata ranges (`OBJECT_META`/`UPLOADS`/`PARTS`/`BUCKETS`) are drained in one redb transaction (lazy cleanup of orphans). The tombstone tree is removed fire-and-forget on the **removal** pipeline (`[pipeline.remove]`, blocking `remove_dir_all` off the request path); a crash between the rename and the removal leaves the unpublished tree as private residue, reclaimed by the startup repair and the scanner.

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
| owner | canonical account ID | The object's owning S3 principal (FR-033): the uploader's ID (or the anonymous special ID in enforced mode; the empty wire under no-identity), recorded by the write path; an overwrite replaces it, a rename preserves it, a copy takes the requester as owner (never the source's); multipart applies the CMU-create-time owner (the creator) | 
| acl | grant set | The object's ACL (FR-033): canonical grants wire; `PutObjectAcl` re-places the grant set, the owner element preserved; new objects default to the private ACL (the requester's FULL_CONTROL grant) |

ETag persistence: `OBJECT_META` table of `<state-dir>/meta.redb` — key `(bucket, key)`, value `(etag, size, mtime, file identity)`; served only when size+mtime match, else recomputed streaming and rewritten (out-of-band modification detection). Orphaned meta entries removed on object delete, lazily on bucket delete. Known granularity limit: an out-of-band edit preserving both size and mtime tick may serve a stale ETag (accepted trade-off, FR-022). The stored file identity (unix dev+inode; 0 where unavailable, e.g. Windows) lets a same-file touch keep a multipart `MD5-of-MD5s-N` ETag while a same-size replacement (new inode) re-hashes — the 60 s mtime jitter window is the fallback where no identity exists. Meta writes are redb transactions (single-writer, crash-safe by default — commit is atomic; no torn state). The composite key keeps one bucket's entries contiguous, so bucket walks and deletions are cheap prefix range scans. Listings include ETags: missing/stale entries are recomputed synchronously during the listing — a one-time full-content pass over externally-added files (documented cost of SC-006).

**Versioning (FR-037–FR-041).** The file at the key path is always the **current** version: an unversioned bucket's object is its null version (its `OBJECT_META` row names no identity), a versioning-enabled bucket's current version is recorded on the row (the version id wire plus its sequence, the wire literally `null` when the current slot is the null version), and a suspended bucket's current version is the null version again. Superseding a current version writes its `VERSIONS` row and `VERSION_INDEX` entry **before** the key path is overwritten (the ordering `fs-backend.md` §11 states), so an interrupted write leaves the row describing the file still at the key path and recovery adopts it rather than minting a second identity. The row's elements are what a version *is*: the identity wire, the encoded block list of its content (the blocks under `blobs/`), the content length, its recorded ETag (copied from `OBJECT_META` at supersede time, never rehashed), the created and became-noncurrent stamps, the `object`/`marker` kind, and its own tags/checksum/owner/ACL — the per-version metadata a version-addressed read or a listing page serves without touching the content. Delete markers are rows too (kind `marker`, the empty blob, no etag), which is what lets the listing and the retention pass treat them uniformly, and the marker form of the current slot is an `OBJECT_META` row with the delete-marker flag set. Folder markers are **not** versioned (the design's non-goal): `dir/` keeps its idempotent directory semantics and never enters the version tables or the listing.

State: absent → exists → (overwritten atomically) → deleted. Concurrent writes: last completed atomic rename wins; never a torn mix (FR-011). Symlinks: rejected by default (`[storage.fs] follow_symlinks = false`) — access resolving through a symlink is refused and symlink entries are excluded from listings; opt-in `true` follows links (which may point outside the root — user-owned directory, documented).

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

Normal mode: `<root>/.tinio/`. Read-only mode (FR-023, amended 2026-10-03): the **storage's** state dir is still `<root>/.tinio/` — its own `meta.redb` is opened **read-only** and is the run's only metadata source, so objects, tags, ACLs, CORS and the version index of a root a read-write run wrote earlier are served rather than lost — while the **tool state** (socket, `state` file, logs), and the `.env` this run loads, live in `~/.tinio/roots/<sha256(canonical root)16>/` (mode 0700; the name is the first 16 hex characters of `sha2::Sha256` over the canonical root path) or under an explicit `TINIO_STATE_DIR`, used verbatim (see `contracts/config.md`). `<root>` is then never written: the read-only open creates nothing, the scanner and the sweeper do not run, and every mutating operation is refused. When `<root>/.tinio/meta.redb` cannot be opened read-only — absent, corrupt, a format this build refuses, or a read-write server holding the same root (redb refuses a read-only open concurrent with a writer) — the run **degrades**: current versions serve from their files and every metadata read comes back empty. A pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` is still read (but never modified; `.tinio.toml` wins when both exist). The home base is resolved via the `dirs` crate. The root-side config read and the `.env` load from the state dir land with the CLI (T143). **A pg run (`[storage] type = "pg"`) has no served root and no relocation**: its configuration and tool state live under the synthetic root's own `.tinio/` — the state-dir class `contracts/config.md`'s `[storage.pg]` bullet owns (PG5/PG13).

| Entry | Purpose |
|-------|---------|
| `.tinio.toml` | Config file (auto-created first start; top-level `version = 1` for future format migration) |
| `.env` | Optional env file (loaded if present; in read-only mode only the state dir's `.env` is loaded) |
| `state` | `{version, pid, token, port, root, started_at, control_name}` JSON, mode 0600 — `root` is the canonical storage root the run serves, written beside the control channel's own name so a reader can tell which root a `state` file answers for |
| `tinio-<sha1(canonical root)[..16]>.sock` | Unix socket (Linux/macOS), derived per root in the state dir — two roots sharing one state dir must not collide (design decision 24); stale file probed + unlinked before bind; overridden by `[api.unix] path` (relative to the state dir or absolute), and the resolved absolute path is what `state.control_name` records. Windows: named pipe, name in state (derived `tinio-<sha1(canonical root)[..16]>`, configured via `[api.pipe] path` / `--api pipe://`) |
| `access.log` | Data-plane access log |
| `server.log` / `server.json` | Operational log (daemon mode; json = format-selected name) |
| `meta.redb` | The state database (redb 4.2, copy-on-write, crash-safe by default): eleven tables — `OBJECT_META` (`(bucket, key)` → 11-tuple `(etag, size, mtime, file identity, tags, checksum, owner, acl, version id wire, version sequence, delete-marker flag)` — the version element spells the current version's identity, the literal `null` for the null version, with sequence `0` for a row that names none and the flag set when the current slot is a delete marker), `BUCKETS` (name → 4-tuple `(creation time, tags, owner, acl)`), `UPLOADS` (`(bucket, upload_id)` → 5-tuple `(key, initiated_at, tags, owner, acl)`), `PARTS` (`(bucket, upload_id, part_number)` → etag), `UPLOAD_CHECKSUMS`/`PART_CHECKSUMS`, `OBJECT_PARTS`, `STATE` (`version` + `compact_needed` marker), and the three version tables (FR-037–FR-041; `tinio_store::{version, version_index, bucket_versioning}`): `VERSIONS` (`(bucket, key, sequence)` → 11-tuple `(version id wire, encoded blob, content length, etag wire, created, became-noncurrent, kind, tags, checksum, owner, acl)` — one row per superseded version and delete marker; the sequence is the per-key monotonic mint (descending = newest first) and the row's only ordering source; `became-noncurrent` is `0` until the transaction that moves the current slot stamps it, so the supersede's crash-window row is not yet history; `kind` is `object` or `marker` and a marker's blob is the empty one, which is never stored; the etag is copied from `OBJECT_META` at supersede time, never recomputed), `VERSION_INDEX` (`(bucket, key, version id wire)` → `sequence` — the point lookup that keeps a version-addressed read from scanning the row table, written and removed in the same transaction as the row it names), `BUCKET_VERSIONING` (bucket → state wire `Enabled`/`Suspended` — a missing row is unversioned and a corrupt wire self-heals to it, so a row that cannot say its state never turns versioning on); the tag/checksum/owner/ACL wires are canonical strings — see `meta-redb-spec.md` §5.2 for the per-element rules |
| `multipart/<bucket>/<uploadId>/part-<n>` | Multipart part content files only (records + part ETags live in `UPLOADS`/`PARTS`; unused in read-only mode) |
| `blobs/<bucket>/<prefix>/<block-hash>` | The content-addressed blocks of a bucket's superseded object versions (one subtree per bucket, so a bucket deletion drops its blocks the way it drops `multipart/<bucket>/`; read in read-only mode, never written) |
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
| scanner task | background | Low-priority ETag scanner (FR-024; Minio-aligned name): streams MD5 for missing/stale meta entries across the tree after startup; default on (`[scanner]` section present in the auto-created config), paced by `[scanner] delay` with `max_wait`/`cycle` (Minio-aligned keys); yields to request traffic; aborts on shutdown; **not run in read-only mode** (its meta write-back is what it exists for, and that mode writes nothing — reads recompute missing/stale entries on demand without writing them back) |
| file permissions | — | unix: state dir 0700, `state`/config 0600; Windows: no descriptor is installed — `state` and the config take their directory's inherited ACEs, falling back to the creating token's default DACL only when it passes nothing down, so unix's mode is the stronger guarantee. **Payload modes (FR-033, §5a)**: on unix, new object files land `0600` and new bucket/prefix dirs `0700` (at creation; pre-existing files untouched) — the confidentiality asymmetry between the ACL data and the payload closes by mode alone; with an optional `[owner]`/`[[users]] local_uid` mapping and chown privilege (root/CAP_CHOWN — validated hard at startup), landed files additionally `chown` to the mapped owner's uid and bucket/prefix dirs to the bucket owner's mapped uid; unmapped owners keep server-user ownership (mapped users' 0600/0700 owner bits block cross-user local reads); Windows unchanged (`local_uid` rejected at config parse) |

State transitions: `stopped` → `starting` (bind port + control channel, write state) → `running` (scanner task launches) → `draining` (stop: cease accepting, ≤10 s in-flight drain, scanner aborts) → `stopped` (graceful stop removes the socket file and `state`). The `stop` CLI waits for exit by polling the control channel until the probe fails or `state` disappears (bounded ~15 s), then reports; on timeout it reports that exit was not confirmed. Crash recovery: state file and socket may be stale — `status` probes the control channel, `start` probes and reclaims a stale socket; temp files/multipart handled by the async sweep. Bare `--no-default-features` builds (no `api` feature) have no state file, no control channel, and no single-instance enforcement — a documented risk (ephemeral default ports remove the port-conflict fallback). A root's identity is its canonical path: renaming/re-linking the root yields a new derived home state dir and regenerated credentials (documented).

## Validation Rules (from requirements)

- FR-006: object key traversal/absolute-path rejection before any FS access.
- FR-012: bucket name rules; `.`-prefix implies reserved-name rejection.
- FR-009: anonymous explicit-only; precedence over creds with warning.
- FR-016: config sources precedence flags > env > `.env` > file; unknown config keys fail startup.
- FR-021: `[s3]` capability toggles; disabled ops return NotImplemented; unknown keys fail startup.
- FR-017: unknown access-log format variables fail startup.
- FR-022: ETag served only on size/mtime match; Content-Type inferred; user metadata dropped.
- FR-023: read-only mode rejects all S3 mutating ops with `AccessDenied`; the storage reads `<root>/.tinio/meta.redb` read-only (the root's metadata is served, not lost) while the tool state dir relocates to `~/.tinio/roots/<sha256(canonical root)16>/`; the root is never written; a root database that cannot be opened read-only degrades the run to an empty metadata view.
- FR-024: background ETag scanner — low priority, never blocks startup, rate-limitable, disableable, aborts on shutdown.
- FR-020 (any-depth): `.tinio` is a reserved path segment at every level, not just the root.
- FR-037–FR-041: versioning — the three bucket states with the closed status enum, the version tables above, the null-version and delete-marker rules (a matched version's metadata lives in its row), and the fail-open guard (a version-addressed request is refused, never answered from the current version). The protocol contract is `contracts/s3-surface.md` §Object versioning; the fs algorithms are `fs-backend.md` §11.
- FR-033: owner/ACL rows (shared redb elements, self-healing decode), the write-path owner-recording rules, and the unix payload modes + `local_uid` mapping rules above — `[owner]`/`[[users]]` config contract in `contracts/config.md`; the wire/enforcement contract in `contracts/s3-surface.md`.

## Metrics (read-only views, not persisted)

HTTP: `tinio_http_requests_total{method,status}`, `tinio_http_request_duration_seconds{method}`, `tinio_http_in_flight`. S3: `tinio_s3_operations_total{op,status}`, `tinio_s3_operation_duration_seconds{op}`. Storage: `tinio_storage_buckets_total`, `tinio_storage_objects_total` (reader-cadence 30 s), `tinio_storage_bytes_total` (reader-cadence 30 s), `tinio_storage_upload_bytes_total`, `tinio_storage_download_bytes_total`, `tinio_storage_objects_uploaded_total{op}`, `tinio_storage_objects_deleted_total`, `tinio_storage_multipart_in_progress`. Versioning (declared by `tinio-fs`, the crate that observes the sweep, and gathered by the same process-global registry): `tinio_storage_blocks_reclaimed_total` (block files reclaimed by the block sweep, cumulative), `tinio_storage_bytes_reclaimed_total` (bytes reclaimed, cumulative), `tinio_storage_noncurrent_versions_total` (noncurrent versions retained across versioned buckets, as of the last sweep pass), `tinio_storage_block_bytes` (total bytes held by the versioned buckets' block stores, as of the last sweep pass — the size signal the `[s3] block_bytes_warn` threshold compares against), `tinio_storage_blocks_corrupt_total` (stored blocks that failed verification against their own content hash, cumulative across the doctor's and the post-repair startup verify passes). Sandbox (Linux: the confinement the serving process applies to its own process, so each family is declared by `tinio-sandbox-linux`, the crate that observes it, and gathered by the same process-global registry `/metrics` renders): `tinio_sandbox_read_only_arm{arm}` (`remount` | `overlay` — the read-only mechanism the run's exposures actually took), `tinio_sandbox_seccomp_skipped` (1 when this architecture has no syscall table, so outbound connections are not blocked; 0 when the filter landed).
