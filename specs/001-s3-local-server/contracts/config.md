# Contract: Configuration

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

**Implementation**: `crates/tinio-config/src/schema/mod.rs` (`Config::parse`, garde validation, `SmartDefault` defaults); `.env` loading in `sources.rs` (T018). Credential generation (`credentials.rs`) lands in US2/US3.

## Config file: `<root>/.tinio/.tinio.toml`

Auto-created with generated credentials on first `start` (in read-only mode: created under the home state dir instead — see below). `<state-dir>/config.toml` is accepted as an alias when `.tinio.toml` is absent (auto-create always writes `.tinio.toml`). Sections and keys are validated; **unknown keys are rejected at startup** (fail-fast). All paths relative to the state dir (`<root>/.tinio/`, or `~/.tinio/roots/<sha1(canonical root)16>/` in read-only mode) unless absolute.

```toml
version = 1              # config format version (future migrations)

[server]
host = "127.0.0.1"       # default
port = 9000              # default (Minio-compatible); 0 = OS-assigned ephemeral port (tests; actual port in logs/state)
read_only = false        # true = read-only mode (FR-023): reject all S3 writes, state under ~/.tinio/roots/<hash>/

[scanner]
# presence = background ETag scanner on (auto-created config includes this section; omit to disable; FR-024)
# Minio-aligned keys (mc admin config set myminio scanner delay=... max_wait=... cycle=...)
delay = 10.0             # seconds between scan iterations (pacing/throttle)
max_wait = "15s"         # max time to wait for a scan slot when throttled
cycle = "24h"            # full-tree scan cycle (re-scan for out-of-band changes)

[auth]
access_key = "..."       # generated on first start (≥ 16 bytes, CSPRNG random)
secret_key = "..."       # generated on first start (≥ 32 bytes, CSPRNG random)
# NOTE: no `anonymous` key — anonymous mode is flag/env only (deliberate); the key is rejected as unknown.

[log]
verbosity = "info"                # error | warn | info | debug
access_log = "access.log"         # access-log file name (relative to the state dir, or absolute)
access_log_format = "combined"    # combined | common | custom nginx-style string
server_log_format = "text"        # text | json  (json defaults file name server.json)
server_log_file = ""              # empty = stderr; daemon mode defaults to server.log / server.json

[s3]
multipart = true          # multipart ops + upload_part_copy
copy_object = true
list_objects_v1 = true
list_objects_v2 = true
delete_objects = true
max_buckets = 10000   # ListBuckets page-size cap (0 = unlimited; larger max-buckets requests are clamped — the AWS documented ceiling; values above 10,000 are REJECTED at parse — the wire ceiling makes them dead config, F04)
max_keys = 0          # ListObjects page-size cap (0 = unlimited, the default — preserves current behavior)
allow_zero_page_size = false # escape hatch: true restores the legacy empty page for max-keys / max-parts / max-uploads = 0 (ListBuckets stays strict 1..=10,000)
checksum = false      # validate and echo x-amz-checksum-* on multipart uploads (default false = accepted and dropped; see the multipart checksum design 2026-08-31)
sig_v2 = false            # SigV2 off by default; DEPRECATED (weaker scheme; aws cli v2 / rclone never use it) — enabling prints a startup warning; slated for removal in v2
temp_ttl_hours = 24       # stale temp-write sweep timeout
multipart_expire_days = 7 # abandoned-upload sweep timeout

[storage.fs]              # filesystem backend keys
follow_symlinks = false   # reject access through symlinks + exclude from listings (default; true = follow — a link inside a bucket can then escape the storage root)
compact_threshold_percent = 20  # state-database fragmentation % triggering compact at startup (5..=90)
meta_batch_size = 128     # meta entries per write-pipeline batch (1..=4096; the cold list/scanner flush threshold — default from the set_batch benchmark knee)
meta_batch_bytes = 262144 # estimated bytes per write-pipeline batch (1024..=16 MiB; ≈ 56 B + key length per entry — the second flush trigger)

# [pipeline.io]          # IO task pipeline (ETag computation: bounded file reads + hashing)
# workers = 2            # worker-thread count (1..=64; each worker runs one blocking task); replaces the old fixed 16-way cold-list hash concurrency (buffer_unordered) — benchmark-backed, pipeline-spec.md §3.3
# priority = "normal"    # normal | low | high (normal = OS default thread priority; low/high = lowest/highest legal)
# capacity = 1024        # bounded queue capacity (1..=65536; the backpressure bound)

# [pipeline.db]          # DB write pipeline (batched meta writes)
# workers = 1            # worker-thread count (1..=4; redb is single-writer, so 1 is the write-throughput optimum)
# priority = "normal"
# capacity = 1024

[api.unix]              # local channel, unix form — Linux/macOS (part of three-choose-one)
# presence = local channel on (the auto-created config includes this section; omit it to disable)
# The transports are mutually exclusive — exactly one of unix/http/https may be enabled
# (three-choose-one). On Windows use [api.pipe] instead of [api.unix]. More than one is a
# startup error (US2 orchestration).
path = ""                 # socket path (relative to .tinio/, or absolute); default tinio.sock

# [api.pipe]              # local channel, Windows form — use this section on Windows instead of [api.unix]
# path = ""              # named-pipe name (empty = derived tinio-<sha1>)

# [api.http]              # presence of this section enables TCP HTTP exposure (mutually exclusive with unix/pipe/https; port default 9001)
# host = "127.0.0.1"
# port = 9001

# [api.https]             # presence of this section (or `--api https://`) enables TCP HTTPS; requires cert + key (mutually exclusive with unix/pipe/http)
# host = "127.0.0.1"
# port = 9001
# cert = ""               # PEM certificate path
# key = ""                # PEM private key path

[telemetry]
otlp_endpoint = "http://127.0.0.1:4317"   # required when the section is present (empty → startup error); omit [telemetry] to disable; requires the `otel` cargo feature
```

## Environment variables

`TINIO_ACCESS_KEY`, `TINIO_SECRET_KEY`, `TINIO_HOST`, `TINIO_PORT`, `TINIO_ANONYMOUS`, `TINIO_READ_ONLY`, `TINIO_SCANNER` (`0`/`1`), `TINIO_LOG_LEVEL`, `TINIO_ACCESS_LOG` (access-log file), `TINIO_LOG_FORMAT` (server log format), `TINIO_ACCESS_LOG_FORMAT`, `TINIO_API_UNIX` (`0`/`1`), `TINIO_API_HTTP` (`host:port`), `TINIO_API_HTTPS` (`host:port`), `TINIO_API_HTTPS_CERT`, `TINIO_API_HTTPS_KEY`.

`TINIO_*` takes precedence; when absent, the corresponding `MINIO_*` variable is accepted as a fallback for credentials: `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` (legacy names) and `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` (modern names) map to the access/secret key pair.

## Read-only mode (`[server] read_only = true` / `--read-only` / `TINIO_READ_ONLY=1`)

- All S3 mutating operations are rejected with `AccessDenied`; the storage root is never written (may be a genuinely read-only filesystem).
- State dir relocates from `<root>/.tinio/` to `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700; home resolved via the `dirs` crate). Everything else in this contract — state, socket, logs, meta store (`meta.redb`) — lives there with the same layout.
- Config read rule: a pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` is still read — never written (`.tinio.toml` wins when both exist; other contents of the root's `.tinio/` are ignored in read-only mode). When absent, the config is auto-created with generated credentials **in the home state dir** instead.
- `.env` is loaded only from the state dir in read-only mode.

## `.env` file: `<root>/.tinio/.env`

Loaded via `dotenvy` if present; standard dotenv syntax (KEY=VALUE lines, comments, quoting, CRLF tolerated). Contains the same `TINIO_*` names.

## Precedence (FR-016)

```
CLI flags > process environment > .env > config file
```

`--anonymous` (flag/env) overrides configured credentials with a warning logged (FR-009).

## Validation rules

- Unknown config keys / sections → startup error.
- Unknown `access_log_format` variables → startup error (fixed variable set: `$remote_addr`, `$remote_user`, `$time_local`, `$request`, `$status`, `$body_bytes_sent`, `$http_referer`, `$http_user_agent`, `$request_time`). The set is closed by design — it cannot reference the Authorization header, query strings, or credentials, a security property that keeps secrets out of access logs (spec §FR-017); extending the set requires revisiting this guarantee.
- `[server] port`: default 9000 (Minio-compatible); `0` = OS-assigned ephemeral port (for tests; reported in logs/state); explicit 1–65535 = fixed port. The auto-created config on first start writes `port = 9000`. Host any; verbosity in the four levels; boolean-typed keys must be booleans (`[server]` read_only, `[s3]`/`[storage.fs]`/`[telemetry]` toggles); `[scanner]` and `[api.*]` transports are presence-gated (section present = on, absent = off).
- Credential presence rules: no creds + no anonymous → generated session creds (printed once); first start → config auto-created with persisted creds.
- Backend selection is deferred: v1 is filesystem-only (`tinio-fs`); the `[storage]` section holds backend behavior keys (nested per backend — `[storage.fs]` for the filesystem), and a `type` selection key will be added when a second backend (`tinio-s3`, `tinio-webdav`) lands.
- `[s3]` capability groups are also strippable at compile time via default-on cargo features (`multipart`, `copy`, `list-v1`, `list-v2`); when a group is not compiled, its keys here are schema-known and silently ignored.
- `[s3] max_buckets` / `max_keys`: the ListBuckets / ListObjects page-size caps, `u32`. `max_buckets` defaults to 10,000 (the AWS documented maximum); `max_keys` defaults to 0 (unlimited, preserving current behavior). Multipart listings have no caps (AWS documents none). The wire also rejects `max-buckets` above 10,000 (`InvalidArgument`, never a silent clamp). Effective range for `max_buckets` is **0..=10,000** (F04): a value above 10,000 is rejected at config parse — the wire ceiling makes it dead configuration (it could never clamp a request the wire lets through).
- `[s3] allow_zero_page_size`: boolean, default **false** (strict). **Breaking-change signal** (F06): since 2026-08, the pre-existing listing surfaces (`max-keys` V1/V2, `max-parts`, `max-uploads`) answer `InvalidArgument` for values below 1 — where the pre-2026-08 server answered an empty page (`.max(0)`). A client that has always sent `0` breaks with 400 after an upgrade; set `allow_zero_page_size = true` to restore the legacy empty page (0 — and negatives, clamped to 0 — accepted on those surfaces). ListBuckets keeps the AWS-documented 1..=10,000 validation regardless.
- `[s3] checksum`: boolean, default **false** — validate and echo `x-amz-checksum-*` on multipart uploads (spec 2026-08-31). Off (the default) = the v1 behavior: checksums accepted and dropped. On: per-part values (headers or aws-chunked trailers) are validated in the streaming pass (`BadDigest` on mismatch, the part is never stored), the create-time algorithm is persisted and enforced across parts, and Complete validates the full-object value pre-commit (COMPOSITE composition / FULL_OBJECT CRC linearization) — see `contracts/s3-surface.md` for the effective coverage and the documented deviations.
- The `[api.https]` section (or `--api https://`) requires both `cert` and `key` (PEM paths); missing → startup error.
- `http` and `https` are mutually exclusive transports (three-choose-one with the local channel): at most one of `unix`/`pipe`/`http`/`https` may be enabled — the local channel is `unix` on Linux/macOS and `pipe` on Windows, so `unix` and `pipe` are mutually exclusive too; more than one is a startup error.
- The local channel has two platform forms: `[api.unix]` `path` (Linux/macOS — socket path, relative to `.tinio/` unless absolute; default `tinio.sock`) and `[api.pipe]` `path` (Windows — named-pipe name, empty = derived `tinio-<sha1(root)>`). Use the platform-appropriate section; both participate in the three-choose-one exclusivity.
- `[storage.fs] follow_symlinks`: boolean, default **false** (secure default: access never resolves through a symlink and link entries are excluded from listings, so a link inside a bucket cannot escape the storage root); `true` = follow symlinks (opt-in).
- `[storage.fs] compact_threshold_percent`: 5–90, default 20; the state-database fragmentation percentage that triggers compaction at startup (offline, before the store handles are shared; `doctor --fix` triggers the same).
- `[storage.fs] meta_batch_size` / `meta_batch_bytes`: the streaming meta-batch flush thresholds of the cold list/scanner producers (pipeline-spec.md §3.2, Q5). Computed entries accumulate into one batch, which is flushed through the DB write pipeline (`MetaWriteBatchTask` — one batch = one write transaction) once the entry count reaches `meta_batch_size` (1–4096, default 128 — the task-2.5 `set_batch` benchmark knee, Q6) **or** the estimated bytes reach `meta_batch_bytes` (1024–16 MiB, default 262144 = 256 KiB; the per-entry estimate is ≈ 56 B + key byte length). The defaults live in the tinio-core storage module and are shared by the config schema and `FsOptions`, so the two cannot drift.
- `[pipeline.io]` / `[pipeline.db]`: the task pipelines (IO: ETag computation — CPU/IO-bound; DB: batched meta writes). The section is presence-gated (Q8): an absent `[pipeline]` section resolves to the defaults, and the auto-created config never emits it. Keys per section: `workers` (io: 1–64, default 2 — the task-5 full-pipeline benchmark knee: 1→2 ≈ +2×, 2→4 regresses on the short-key axis, 4→8 flat, basis in task-5-report.md; db: 1–4, default 1 — redb is a single-writer store, so more than one worker adds no write throughput, verified at +0.5%/+1.7% vs 2), `priority` (`normal`/`low`/`high`, default `normal`; `normal` = no thread priority is set — the OS default; `low`/`high` = the lowest/highest legal thread-priority values — Windows `THREAD_PRIORITY_IDLE` / `THREAD_PRIORITY_TIME_CRITICAL`), `capacity` (1–65536, default 1024; the bounded-queue capacity, i.e. the backpressure bound). Each pipeline runs on its own tokio runtime with `workers` worker threads named `tinio-pipeline-io` / `tinio-pipeline-db`, and the configured priority is applied to exactly those threads.
- `[server] read_only`: boolean, default false; see the read-only-mode section above. In read-only mode, `[s3]` write-related toggles are moot (writes are rejected regardless).
- `[scanner]` section: presence = background ETag scanner on (FR-024; the auto-created config includes it; omitted = off); keys are Minio-aligned (`mc admin config set myminio scanner ...`): `delay` float seconds ≥ 0 (default 10.0 — pacing between scan iterations), `max_wait` duration string (default `15s` — max wait for a scan slot when throttled), `cycle` duration string (default `24h` — full-tree re-scan cadence). Runs in read-only mode as well (meta writes land in the home state dir). Env: `TINIO_SCANNER` (`0`/`1`).
- Any TCP exposure (http/https section present or `--api` flags) requires the token on ALL management endpoints (including `/metrics` and `/openapi.json`). Until the management plane lands (T075), the `serve` example additionally exposes a reserved `GET /metrics` on the data-plane listener without auth for local scraping (F10, pipeline-spec.md §4); the token rule above applies from T075.
- The `[api]` section requires the `api` cargo feature (default on). In builds without it, the keys are schema-known and silently ignored, not rejected as unknown. With the `api` feature compiled in, exactly one management transport must be enabled after resolution (three-choose-one: `unix`/`http`/`https`) — `--no-api-unix` with no TCP transport configured is a startup error.
- Repeatable `--api <URL>` flags select the transport per scheme: a flag replaces the matching transport's subsection (`unix`/`http`/`https`); the three-choose-one exclusivity applies after all flags and config are resolved.
