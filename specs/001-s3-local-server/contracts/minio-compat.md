# Contract: Minio Compatibility

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

The user-facing surfaces aligned to Minio conventions so existing Minio workflows and automation work unchanged. This file is the canonical reference for the Minio-alignment decisions; [research.md §23](../research.md) records the rationale and alternatives.

## 1. CLI invocation (Minio-style)

- `tinio server <dir>` — the storage root is a **positional directory argument** (Minio: `minio server /data`); `start` is retained as an alias. `--root` is removed.
- `status` / `stop` / `doctor` take the same positional `[DIR]`; walk-up discovery from the current directory remains as the fallback when no directory is given.
- `--address HOST:PORT` is the Minio-style alias for `--host` + `--port`; the host part is optional (default `127.0.0.1`, so `--address :9000` works). `--host` / `--port` remain supported.

## 2. Port defaults

| Surface | Default | Notes |
|---------|---------|-------|
| S3 data plane | **9000** | Minio's default; no `--port` needed. `--port 0` explicitly selects an OS-assigned ephemeral port (tests/multi-instance; actual port in logs/state) |
| Management API over TCP | **9001** | Minio's console-port convention; a scheme-less `--api host:port` means HTTP. HTTPS requires an explicit `https://` scheme |

Convention: the data plane's fixed-port examples must never use 9001 (reserved for the API default), to avoid collision confusion.

## 3. Environment variables

`TINIO_*` names take precedence; when absent, the corresponding `MINIO_*` variable is accepted as a fallback for credentials:

| TINIO_ | MINIO_ fallbacks |
|--------|------------------|
| `TINIO_ACCESS_KEY` | `MINIO_ACCESS_KEY` (legacy) / `MINIO_ROOT_USER` (modern) |
| `TINIO_SECRET_KEY` | `MINIO_SECRET_KEY` (legacy) / `MINIO_ROOT_PASSWORD` (modern) |

All other variables (host, port, log, api transports) are `TINIO_`-only — Minio has no equivalents.

## 4. Scanner configuration (Minio-aligned keys)

The `[scanner]` section is presence-gated (section present = background ETag scanner on; the auto-created config includes it; omitted = off). Its keys match `mc admin config set myminio scanner ...` — schema and defaults are in [config.md](config.md); the mapping to Minio's semantics:

| key | meaning |
|-----|---------|
| `delay` | seconds between scan iterations (pacing/throttle) |
| `max_wait` | max time to wait for a scan slot when throttled |
| `cycle` | full-tree re-scan cadence (re-scan for out-of-band changes) |

Env: `TINIO_SCANNER` (`0`/`1`) toggles the scanner independently.

## 5. Presence-based config semantics

`[api.unix]` / `[api.http]` / `[api.https]` and `[scanner]` use **section presence = enabled** (no `enabled` booleans in the config): the section present means on, omitted means off. `--api` flags and `--no-api-unix` override per scheme as usual.

## 6. Documented deviations from Minio

- **Loopback-only default bind**: `--address :9000` (or no address) binds `127.0.0.1`, not all interfaces — a deliberate security default for a local tool; users who need exposure set the host explicitly.
- **`--anonymous` retained**: tinio supports an explicit anonymous mode (Minio always requires credentials).
- **Not adopted in v1** (listed for future alignment): `--console-address`, `--quiet`, `--json`, `--config-dir`, `--certs-dir`, `MINIO_REGION` and other `MINIO_*` environment variables beyond the credential fallback.
