# Contract: Configuration

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

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
access_key = "..."       # generated on first start
secret_key = "..."
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
sig_v2 = false            # SigV2 off by default (weaker scheme; aws cli v2 / rclone never use it)
temp_ttl_hours = 24       # stale temp-write sweep timeout
multipart_expire_days = 7 # abandoned-upload sweep timeout

[storage]
follow_symlinks = true    # follow symlinks in the storage root (default); false = reject access through links + exclude from listings

[api.unix]
# presence = local channel on (the auto-created config includes this section; omit it to disable)
path = ""                 # unix: socket path (relative to .tinio/, or absolute); Windows: pipe name (empty = derived tinio-<sha1>); default tinio.sock on unix

# [api.http]              # presence of this section enables TCP HTTP exposure (default off: omit the section; port default 9001)
# host = "127.0.0.1"
# port = 9001

# [api.https]             # presence of this section (or `--api https://`) enables TCP HTTPS; requires cert + key
# host = "127.0.0.1"
# port = 9001             # must differ from http.port when both are enabled
# cert = ""               # PEM certificate path
# key = ""                # PEM private key path

[telemetry]
otlp_endpoint = ""        # empty = off; requires the `otel` cargo feature
```

## Environment variables

`TINIO_ACCESS_KEY`, `TINIO_SECRET_KEY`, `TINIO_HOST`, `TINIO_PORT`, `TINIO_ANONYMOUS`, `TINIO_READ_ONLY`, `TINIO_SCANNER` (`0`/`1`), `TINIO_LOG_LEVEL`, `TINIO_ACCESS_LOG` (access-log file), `TINIO_LOG_FORMAT` (server log format), `TINIO_ACCESS_LOG_FORMAT`, `TINIO_API_UNIX` (`0`/`1`), `TINIO_API_HTTP` (`host:port`), `TINIO_API_HTTPS` (`host:port`), `TINIO_API_HTTPS_CERT`, `TINIO_API_HTTPS_KEY`.

`TINIO_*` takes precedence; when absent, the corresponding `MINIO_*` variable is accepted as a fallback for credentials: `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` (legacy names) and `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` (modern names) map to the access/secret key pair.

## Read-only mode (`[server] read_only = true` / `--read-only` / `TINIO_READ_ONLY=1`)

- All S3 mutating operations are rejected with `AccessDenied`; the storage root is never written (may be a genuinely read-only filesystem).
- State dir relocates from `<root>/.tinio/` to `~/.tinio/roots/<sha1(canonical root)16>/` (mode 0700; home resolved via the `dirs` crate). Everything else in this contract — state, socket, logs, meta store, `buckets.json` — lives there with the same layout.
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
- Unknown `access_log_format` variables → startup error (fixed variable set: `$remote_addr`, `$remote_user`, `$time_local`, `$request`, `$status`, `$body_bytes_sent`, `$http_referer`, `$http_user_agent`, `$request_time`).
- `[server] port`: default 9000 (Minio-compatible); `0` = OS-assigned ephemeral port (for tests; reported in logs/state); explicit 1–65535 = fixed port. The auto-created config on first start writes `port = 9000`. Host any; verbosity in the four levels; boolean-typed keys must be booleans (`[server]` read_only, `[s3]`/`[storage]`/`[telemetry]` toggles); `[scanner]` and `[api.*]` transports are presence-gated (section present = on, absent = off).
- Credential presence rules: no creds + no anonymous → generated session creds (printed once); first start → config auto-created with persisted creds.
- Backend selection is deferred: v1 is filesystem-only (`tinio-fs`); the `[storage]` section holds backend behavior keys (e.g. `follow_symlinks`), and a `type` selection key will be added when a second backend (`tinio-s3`, `tinio-webdav`) lands.
- `[s3]` capability groups are also strippable at compile time via default-on cargo features (`multipart`, `copy`, `list-v1`, `list-v2`); when a group is not compiled, its keys here are schema-known and silently ignored.
- The `[api.https]` section (or `--api https://`) requires both `cert` and `key` (PEM paths); missing → startup error.
- `http` and `https` ports must differ when both are enabled (startup error otherwise); both may be on (two listeners on the same router).
- `[api.unix]` `path`: on unix it is a socket path, relative to `.tinio/` unless absolute; on Windows it is the named-pipe name (empty = derived `tinio-<sha1(root)>`).
- `[storage] follow_symlinks`: boolean, default true; false = reject access resolving through symlinks and exclude symlink entries from listings.
- `[server] read_only`: boolean, default false; see the read-only-mode section above. In read-only mode, `[s3]` write-related toggles are moot (writes are rejected regardless).
- `[scanner]` section: presence = background ETag scanner on (FR-024; the auto-created config includes it; omitted = off); keys are Minio-aligned (`mc admin config set myminio scanner ...`): `delay` float seconds ≥ 0 (default 10.0 — pacing between scan iterations), `max_wait` duration string (default `15s` — max wait for a scan slot when throttled), `cycle` duration string (default `24h` — full-tree re-scan cadence). Runs in read-only mode as well (meta writes land in the home state dir). Env: `TINIO_SCANNER` (`0`/`1`).
- Any TCP exposure (http/https section present or `--api` flags) requires the token on ALL management endpoints (including `/metrics` and `/openapi.json`).
- The `[api]` section requires the `api` cargo feature (default on). In builds without it, the keys are schema-known and silently ignored, not rejected as unknown. With the `api` feature compiled in, at least one management transport must be enabled after resolution — `--no-api-unix` with no TCP transport configured is a startup error.
- Repeatable `--api <URL>` flags override config per scheme: a flag replaces the matching transport's subsection (`unix`/`http`/`https`); transports not mentioned by any flag keep their configured values.
