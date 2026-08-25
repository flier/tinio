# Contract: CLI

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

Systemd-style lifecycle commands (spec FR-007, design review) with Minio-compatible invocation. No data commands — bucket/object operations are performed directly on the filesystem. Binary: `tinio` (thin `main` in the facade crate → `tinio_cli::run()`).

## Global

```
tinio <COMMAND> [DIR] [--help] [--version]
```

Exit codes: `0` success; `1` operational error (message to stderr); `2` usage error.

## Storage-root discovery

Positional `DIR` argument (Minio-style, e.g. `tinio server /data`); else walk up from the current directory to the nearest ancestor containing `.tinio/`. Read-only roots may legitimately lack `.tinio/` — pass the directory (or run from the root directory) in that case.

## Environment variables

`TINIO_*` names take precedence, with a `MINIO_*` credential fallback (`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` legacy, `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` modern) — the full env list is in [config.md](config.md), the mapping in [minio-compat.md](minio-compat.md).

## Commands

### `tinio server` / `tinio start`

```
tinio server [DIR] [--address HOST:PORT] [--host HOST] [--port N] [--anonymous] [--read-only]
             [--daemon] [--verbosity LEVEL] [--log-file PATH] [--log-format text|json]
             [--api unix://PATH] [--api pipe://NAME] [--api http://HOST:PORT] [--api https://HOST:PORT]
             [--api-cert PATH] [--api-key PATH] [--no-api-unix] [--no-follow-symlinks]
```

`server` is the Minio-style command name; `start` is an alias. `DIR` is the storage root, given positionally (`--root` is removed).

`--port` omitted → **default 9000** (Minio-compatible); `--port 0` → OS-assigned ephemeral port (for tests and multi-instance development; the actual port is printed to the operational log and recorded in `state`, visible via `status`). `--address HOST:PORT` is the Minio-style alias for `--host` + `--port` (host optional, defaults to `127.0.0.1`, e.g. `--address :9000`); `--host`/`--port` remain supported.

`--read-only` (config `[server] read_only`, env `TINIO_READ_ONLY`): read-only mode (FR-023) — all S3 mutating operations are rejected with `AccessDenied`, the storage root is never written, and all state moves to `~/.tinio/roots/<sha1(canonical root)16>/`. A pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` is still read (`.tinio.toml` wins when both exist).

Management transport flags: `--api <URL>` (repeatable) configures a management transport — the URL scheme selects it: `unix://PATH` (unix socket, path relative to the state dir unless absolute), `pipe://NAME` (Windows named pipe; the pipe-analogous form of the local channel), `http://HOST:PORT` or `https://HOST:PORT` (TCP); a scheme-less `--api HOST:PORT` defaults to HTTP. TCP port defaults to **9001** (Minio's console-port convention); defaults are HTTP — HTTPS requires an explicit `https://` scheme plus cert/key from flags (`--api-cert` / `--api-key`) or config. `--no-api-unix` disables the default local channel. Flags override config **per scheme**: a flag replaces the matching transport's config subsection; unmentioned transports keep their config. Platform gating: `unix://` is a usage error on Windows and `pipe://` on unix. `--no-follow-symlinks` disables symlink following in the storage root (config: `[storage] follow_symlinks`). Flags follow the global precedence (flags > env > `.env` > config). All `--api*` options exist only in builds with the `api` cargo feature (default on); without `tls`, the `https://` scheme is rejected as a usage error.

- Foreground by default; `--daemon` detaches (stderr → `server.log`/`server.json`). On Windows, `--daemon` spawns a detached child process (no service manager integration in v1); the systemd unit example below covers Linux.
- Signals: a foreground server handles SIGINT/SIGTERM (unix) and Ctrl+C / console-close events (Windows) as a graceful shutdown — identical to `POST /stop` (cease accepting, drain ≤ 10 s, remove `state` and the socket, exit 0); a second signal exits immediately without draining; `SIGHUP` is ignored and logged (no config reload in v1). `--daemon` children handle the same signals (the systemd unit relies on SIGTERM).
- Crash recovery: after the single-instance check succeeds and before readiness, the server runs a fast, deterministic repair of the private state (itemized in failure-handling.md §3): stale `state`/socket, a full clear of `tmp/`, multipart subtrees whose bucket directory no longer exists (cross-restart uploads stay intact, quickstart §7), upload directories without a `UPLOADS` record (idle past the grace), and stale bucket records. Orphaned ETag meta entries are reclaimed in the background by the scanner. User data (bucket directories and objects) is never touched; every repair action is logged to the operational log.
- First start: auto-creates `.tinio.toml` with generated credentials (in the state dir — `<root>/.tinio/` normally, the home state dir in read-only mode).
- On ready: prints endpoint, storage root, credentials status to stderr (operational log).
- Errors (exit 1): port in use; control channel bind failure after stale-socket reclaim (second live instance on same root); `api` feature present but every management transport disabled; unreadable storage root; storage root not writable without `--read-only`; invalid config.
- Flags override env overrides `.env` overrides config.
- Binding a non-loopback address prints a startup warning to stderr (basic); with anonymous mode also active the warning is escalated (prominent, per the layered trust model in spec §Assumptions/Network).

### `tinio status`

```
tinio status [DIR]
```

- Reads `state` from the state dir (`<root>/.tinio/` or the home state dir in read-only mode), probes the control channel with the token.
- Output: `running|stopped`, endpoint (host:port), storage root, PID, started time.
- Exit 1 with message if the root is uninitialized or the probe fails (server not running).
- Present only in builds with the `api` cargo feature (default on); builds without it do not expose this subcommand.

### `tinio stop`

```
tinio stop [DIR]
```

- Sends graceful stop over the control channel (cease accepting, drain in-flight ≤ 10 s), then waits for exit: polls the control channel until the probe fails or `state` disappears (bounded ~15 s) and reports success; on timeout it reports that exit was not confirmed.
- Exit 1 with message if no running server is found.
- Present only in builds with the `api` cargo feature (default on); builds without it do not expose this subcommand.

### `tinio doctor`

```
tinio doctor [DIR] [--json] [--dry-run] [--fix]
```

Offline diagnostics of the target directory (no server required). Checks:
- storage root exists, is readable and writable;
- configuration file exists, parses, passes schema validation (no unknown keys), and credentials or anonymous mode are resolvable;
- `.tinio/` integrity: stale state file (running server or crashed), stale unix socket (probed), orphaned meta-store entries, orphaned bucket records, abandoned multipart uploads, stale temp files, `meta.redb` integrity check and fragmentation report (state/socket checks apply when built with the `api` feature);
- stale home root-state dirs under `~/.tinio/roots/` whose storage root no longer exists;
- on-disk bucket names valid per S3 rules; on-disk object keys valid per universal + platform rules (including any-depth `.tinio` segments);
- symlink entries present while `follow_symlinks` is disabled;
- low free disk space (warn).

Output: human-readable report with per-check severity (ok / warn / error); `--json` emits machine-readable output. Exit codes: `0` = no problems, `1` = warnings/errors found (with `--dry-run`) or remain (with `--fix`), `2` = usage error.

`--dry-run` lists exactly what a fix would change without touching anything. `--fix` applies the same cleanups as the startup crash-recovery repair (failure-handling.md §3), plus meta-orphan reclamation and stale home root-state dirs (whose root no longer exists). `--fix` requires the server for the target root to be stopped — a live control-channel probe is an error (exit 1). Neither flag ever touches user data (bucket directories and objects are never modified).

With `--read-only` (or `[server] read_only = true`), the root-writability check is skipped (read-only roots are valid) and state-dir checks target the home state dir.

## Example systemd unit (packaging/tinio.service)

```ini
[Unit]
Description=tinio S3-compatible local storage server
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/tinio server /srv/tinio
Restart=on-failure

[Install]
WantedBy=multi-user.target
```
