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

**Every variable the CLI honours is one of its own arguments** (a clap argument with an `env` attribute, declared beside the flag it backs), so the environment is read in one place — the parser — and not re-derived beside it. `TINIO_STATE_DIR`, `TINIO_ACCESS_KEY` and `TINIO_SECRET_KEY` are environment-only: the flags that carry them (`--state-dir`, `--access-key`, `--secret-key`) are **hidden**, because the environment is the documented channel and a secret typed on a command line is visible in the process table. `<state dir>/.env` is loaded between the CLI's two parses, which is what makes it the chain's third level (see [config.md](config.md), Precedence).

## Commands

### `tinio server` / `tinio start`

```
tinio server [DIR] [--address HOST:PORT] [--host HOST] [--port N] [--anonymous] [--read-only]
             [--config PATH] [--sandbox]
             [--daemon] [--verbosity LEVEL] [--log-file PATH] [--log-format text|json]
             [--api unix://PATH] [--api pipe://NAME] [--api http://HOST:PORT] [--api https://HOST:PORT]
             [--api-cert PATH] [--api-key PATH] [--no-api-unix] [--no-follow-symlinks]
```

`server` is the Minio-style command name; `start` is an alias. `DIR` is the storage root, given positionally (`--root` is removed).

`--port` omitted → **default 9000** (Minio-compatible); `--port 0` → OS-assigned ephemeral port (for tests and multi-instance development; the actual port is printed to the operational log and recorded in `state`, visible via `status`). `--address HOST:PORT` is the Minio-style alias for `--host` + `--port` (host optional, defaults to `127.0.0.1`, e.g. `--address :9000`); `--host`/`--port` remain supported.

`--log-file PATH` names the operational log file a **detached** run redirects its stderr to (config `[log] server_log_file`; absent, the format-derived `server.log`/`server.json` in the state dir, following `--log-format`). The flag applies to `--daemon` and to nothing else: a foreground run writes its operational log to stderr, and the foreground file sink is `tinio-server`'s edit rather than this phase's — so `--log-file` on a foreground run is accepted and has no sink yet, a fact this contract states rather than leaves to be discovered.

`--read-only` (config `[server] read_only`, env `TINIO_READ_ONLY`): read-only mode (FR-023) — all S3 mutating operations are rejected with `AccessDenied`, the storage root is never written, and all state moves to `~/.tinio/roots/<sha256(canonical root)16>/` (or to the explicit state dir named by `TINIO_STATE_DIR` when that is set — see `contracts/config.md`). A pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` is still read (`.tinio.toml` wins when both exist). The flag carries its variable directly (`env = "TINIO_READ_ONLY"`), so `TINIO_READ_ONLY=1`/`0`/`true`/`false` is accepted; the bare flag means `true`, and the variable is an **answer** rather than a suggestion — an exported `TINIO_READ_ONLY=0` beats a config file that says `read_only = true`.

`--config PATH` names the configuration file explicitly, and it must name one that exists: a mistyped path is a usage error (exit 2) rather than a new config in a new place. Absent, the file is discovered under the root and then under the state dir, and a **launching** command auto-creates it on first start (below). The commands that launch nothing — `status`, `stop`, `doctor` — read the discovered file and never pass this flag, because a command asked to report on a root or to diagnose it must leave that root exactly as it found it.

`--sandbox` (config `[server] sandbox`, env `TINIO_SANDBOX`) confines the server process (FR-036). The three channels follow the global precedence and any one of them is enough: `--sandbox`, then `TINIO_SANDBOX` (`1`/`true`/`0`/`false`; `.env` in the state dir is loaded into the process environment between the CLI's two parses, so the file is the same channel one level lower), then `[server] sandbox`. The file reaches this switch but not the answers that place it: the read-only mode and the state dir are not reachable from `.env`, whose own location is a function of both (FR-016; [config.md](config.md), Precedence). The flag carries its variable directly (`env = "TINIO_SANDBOX"`, with the tolerant `1`/`true`/`0`/`false` parser the mode switch uses — clap's `SetTrue` action would reject `1`) and takes an optional value, so the bare flag means `true`; an exported `TINIO_SANDBOX=0` beats a config file that says `sandbox = true`. In a build **without** the `sandbox` cargo feature the flag is not in the parser at all while the variable keeps a hidden argument of its own, so the channel still exists and still refuses (see below) rather than being silently ignored.

A confinement request the build or the platform cannot honour is **refused, never ignored**: silently serving unconfined is the failure this flag exists to prevent (fail-closed). In a build without the `sandbox` cargo feature the flag is not in the parser at all and only the variable or the config key can ask, which is a usage error (exit 2) naming the build; with the feature on, a platform whose confinement backend has not landed yet is a startup error (exit 1) naming the platform. Neither refusal publishes `state`, binds a port or serves a request. The `sandbox` cargo feature is default-on in `tinio-cli`, and the facade names it as a **dependency** feature (`tinio-cli = { workspace = true, features = ["sandbox"] }`) rather than relying on its pass-through row, because the workspace pins `tinio-cli` with `default-features = false`: the shipped `tinio` binary therefore carries `--sandbox` in its parser in **every** build, `--no-default-features` included. That is deliberate — the flag is the operator's channel rather than a build-time nicety, and a product binary that lost it could still be asked for confinement through the variable or the config key while the flag silently did nothing. Only a direct `tinio-cli` build without the feature (and without the facade's dependency row) loses `--sandbox`, and there the refusal above still holds for the other two channels.

Under a confinement the process is a **launcher** on Windows and macOS — it binds the listener, binds the control channel, launches a confined child, hands both over and supervises it, and its own exit code is the child's — and the server itself on Linux, where the confinement applies in place, to this one process, before any runtime exists. **Two launcher platforms, one shape**: the steps both arms run in the same order — the in-root refusal, the exposure set, the configuration read, the listener bind and the `state` publish — are `tinio-cli/src/sandbox/confine.rs`'s (`confine_enabled`'s listener bind and `publish_state`), with the refusals and the exposure set in `tinio-cli/src/sandbox/{plan,product}.rs`, while each arm owns its own channel and backend: the Windows arm binds and hands over the inherited pipe, the macOS arm the socket for the profile's reason (a unix `bind()` is a network operation Seatbelt grants nothing for), and `tinio-sandbox-linux`'s in-place arm confines this process on Linux. **Every backend has landed**: `tinio_sandbox::backend_available()` is the one place the per-target answer lives — a facade-owned `pub const fn`, its `#[cfg]` arms beside the facade's per-target re-exports (`crates/tinio-sandbox/src/lib.rs`) — and it answers `true` on all three (the `--sandbox` bullet above), so no target refuses a request before either path is reached. A launcher serves no requests, and it is the launcher — not the child — that publishes `state` (`On ready`, below). What a confinement grants is the storage root at the resolved mode's access, the state directory read-write when it lives outside the root, the running executable's own directory read-only on Windows, and the platform's device files on Linux. A configured `[telemetry] otlp_endpoint` and a confinement cannot both be honoured — the confinement cuts the outbound connection the OTLP exporter is the only path to — so the combination is **warned about** through the operational log, naming both the channel and the endpoint, and the confinement proceeds: losing an observer is not grounds for refusing to serve. With confinement enabled, `tinio service`'s stop path does not drain through the launcher (FR-035); that configuration is refused by this build today, so the promise holds for every run this binary accepts.

Management transport flags: `--api <URL>` (repeatable) configures a management transport — the URL scheme selects it: `unix://PATH` (unix socket, path relative to the state dir unless absolute), `pipe://NAME` (Windows named pipe; the pipe-analogous form of the local channel), `http://HOST:PORT` or `https://HOST:PORT` (TCP); a scheme-less `--api HOST:PORT` defaults to HTTP. TCP port defaults to **9001** (Minio's console-port convention); defaults are HTTP — HTTPS requires an explicit `https://` scheme plus cert/key from flags (`--api-cert` / `--api-key`) or config. `--no-api-unix` disables the default local channel. **Any `--api` flag disables every transport the flags do not name** — the local channel is the tool's default and not the operator's choice — so `--api http://127.0.0.1:9001` on an auto-created root serves one transport and not two, and a flag replaces the matching transport's config subsection where there is one. With no `--api` flag the config file is what selects transports, and a config with **no `[api]` section at all** enables the derived local channel (`state.control_name` records the resolved name). Platform gating: `unix://` is a usage error on Windows and `pipe://` on unix. `--no-follow-symlinks` disables symlink following in the storage root (config: `[storage] follow_symlinks`). Flags follow the global precedence (flags > env > `.env` > config). All `--api*` options exist only in builds with the `api` cargo feature (default on); without `tls`, the `https://` scheme is rejected as a usage error.

- Foreground by default; `--daemon` detaches (stderr → `server.log`/`server.json`) and means "start in the background and **wait for ready**": the parent stays alive, waits (bounded at ~10 s) for the daemon's `state` file and an answer on the control channel that file names, prints the readiness event on stdout and exits 0 — or exits 1 with a message naming the daemon's log file and `tinio status`. On Windows, `--daemon` spawns a detached child process. `--daemon` is the no-elevation, no-supervisor route: it needs no unit, no root and no Administrator, and it does not survive a reboot. The supervised route is `tinio service` (below), which registers the server with the platform's own supervisor; on Windows the SCM has no unit file, so `tinio service install` is the only route there, while on Linux `packaging/tinio.service` remains the operator's own example beside the unit `tinio service install` renders. **`--daemon` exists only in builds with the `api` cargo feature** (default on): readiness is proved by the `state` file and by an answer on the control channel that file names, and both belong to the management plane — so a build without the feature does not carry the flag at all (an unknown argument, exit 2) rather than accepting one it would have to refuse. `--daemon` has no environment variable and no config key, so compiling it away cannot silently ignore a channel.
- Signals: a foreground server handles SIGINT/SIGTERM (unix) and Ctrl+C / console-close events (Windows) as a graceful shutdown — identical to `POST /stop` (cease accepting, drain ≤ 10 s, remove `state` and the socket, exit 0); a second signal exits immediately without draining; `SIGHUP` is ignored and logged (no config reload in v1). `--daemon` children handle the same signals (the systemd unit relies on SIGTERM).
- Crash recovery: after the single-instance check succeeds and before readiness, the server runs a fast, deterministic repair of the private state (itemized in failure-handling.md §3): stale `state`/socket, a full clear of `tmp/`, multipart subtrees whose bucket directory no longer exists (cross-restart uploads stay intact, quickstart §7), upload directories without a `UPLOADS` record (idle past the grace), and stale bucket records. Orphaned ETag meta entries are reclaimed in the background by the scanner. User data (bucket directories and objects) is never touched; every repair action is logged to the operational log.
- First start: auto-creates `.tinio.toml` with generated credentials (in the state dir — `<root>/.tinio/` normally, the home state dir in read-only mode).
- On ready: the readiness **event** — one SSE frame on stdout (`event: listening`, the bound address as a typed field, closed by a blank line) and nothing else, which is the one thing a harness decodes (design decision 19; `crates/tinio-cli/src/events.rs`) — then the operational summary (endpoint, storage root, credentials status) on **stderr**. The frame and the `state` file are **ordered, not simultaneous**: on the launcher platforms `state` is written by the launcher once it learns readiness through the relay, so the `listening` frame can precede the file by a few syscalls — measured on macOS 15.8, 2026-09-25, `published=false, serving=true` once in two runs — and a reader must poll for the artifact rather than assume it landed with the frame (`crates/tinio-e2e/tests/steps/cli.rs`'s `PUBLISH_SETTLE` is that bounded poll). `--port 0` reports the bound port in both. **The launcher's wait for that frame is bounded at 30 s — the design's readiness budget — and the bound is a refusal** (2026-09-27): a confined child still alive and silent when it expires is ended by the launcher, which exits non-zero without ever writing `state`, because a confined server left serving with no `state` file is the run `tinio status` reports as stopped while it works. A child that reports an `error` frame, or whose relay ends before the bound, is **not** that case: the ordinary supervision path reports that run and the launcher adds nothing.
- Errors (exit 1): port in use; control channel bind failure after stale-socket reclaim (second live instance on same root); `api` feature present but every management transport disabled; unreadable storage root; storage root not writable without `--read-only`; invalid config.
- Flags override env overrides `.env` overrides config.
- Binding a non-loopback address prints a startup warning to stderr (basic); with anonymous mode also active the warning is escalated (prominent, per the layered trust model in spec §Assumptions/Network).

### `tinio status`

```
tinio status [DIR] [--read-only]
```

- Reads `state` from the state dir (`<root>/.tinio/` or the home state dir in read-only mode), probes the control channel with the token.
- Output: `running|stopped`, endpoint (host:port), storage root, PID, started time, and the **state directory the answer came from**. That last line is what makes the answer auditable: a read-only client probes two candidates, so "running" without the directory it came from does not say which server was found.
- `--read-only` resolves the mode exactly as the server does (config `[server] read_only`, env `TINIO_READ_ONLY`), which is what selects the candidate pair: `<root>/.tinio` leads for a read-write run and the relocated state dir leads for a read-only one, with the other directory probed second.
- Exit 1 with message if the root is uninitialized or the probe fails (server not running).
- Present only in builds with the `api` cargo feature (default on); builds without it do not expose this subcommand.

### `tinio stop`

```
tinio stop [DIR] [--read-only]
```

- Sends graceful stop over the control channel (cease accepting, drain in-flight ≤ 10 s), then waits for exit: polls the control channel until the probe fails or `state` disappears (bounded ~15 s) and reports success; on timeout it reports that exit was not confirmed.
- `--read-only` resolves the mode exactly as the server does, and therefore the candidate pair this command searches (as `status` above).
- Exit 1 with message if no running server is found.
- Present only in builds with the `api` cargo feature (default on); builds without it do not expose this subcommand.

### `tinio doctor`

```
tinio doctor [DIR] [--read-only] [--json] [--dry-run] [--fix]
```

Offline diagnostics of the target directory (no server required). Checks:
- storage root exists, is readable and writable;
- configuration file exists, parses, passes schema validation (no unknown keys), and credentials or anonymous mode are resolvable;
- `.tinio/` integrity: stale state file (running server or crashed), stale unix socket (probed), orphaned meta-store entries, orphaned bucket records, abandoned multipart uploads, stale temp files, `meta.redb` integrity check and fragmentation report (state/socket checks apply when built with the `api` feature);
- stale home root-state dirs under `~/.tinio/roots/` whose storage root no longer exists (**T144**'s leg — not checked by this build);
- on-disk bucket names valid per S3 rules; on-disk object keys valid per universal + platform rules (including any-depth `.tinio` segments);
- symlink entries present while `follow_symlinks` is disabled;
- low free disk space (warn).

Output: human-readable report with per-check severity (ok / warn / error); `--json` emits machine-readable output. Exit codes: `0` when the report holds no warn and no error, `1` when it holds either, in every mode — a plain run counts defects, `--dry-run` counts the proposals a fix would apply (findings there, information without the flag), `--fix` counts what the repair left behind; `2` = usage error. **In this build `--fix` has a floor of `1`**: every `--fix` report carries the `home root-state dirs` row at `warn`, because the T144 stage is not this build's leg and a leg that cannot run is never silent — so a `--fix` that cleared everything still exits 1, and the `0` above is reachable for it only once T144 lands.

`--dry-run` lists exactly what a fix would change without touching anything, and the list is a *run*: a plain `doctor` reports defects and leaves those proposals as information, while `--dry-run` promotes every one of them to a finding — so a root whose only issue is a leftover temp file exits 1 under `--dry-run` and 0 in a plain run. The pair is exclusive: `--dry-run` and `--fix` given together are a usage error (exit 2), not a run in which one flag quietly wins. `--fix` applies the same cleanups as the startup crash-recovery repair (failure-handling.md §3), plus meta-orphan reclamation, the meta store's own offline maintenance (`fs-backend.md`: the `check_integrity` pass and `compact_if_needed`'s compaction); the stale home root-state dirs whose root no longer exists are **T144**'s leg, not this build's, and `--fix`'s report names them as not repaired — at `warn`, which is the row that floors every `--fix` exit code at 1 (above). `--fix` requires the server for the target root to be stopped — a live control-channel probe is an error (exit 1). A `state` file whose `version` is not this build's is that same probe when the channel the document records still answers: the refusal is the same error and it happens before any repair. **A repair stage that fails is a finding, never a bare failure of the command**: the stage's error is reported at `error` severity, the stages behind it still run, the `state`/socket reclaim is not skipped with it, and `--fix --json` emits a whole document on every path the command can reach — the same "the report is the product" rule the check list follows, and the reason `--fix` names what it could not do instead of exiting on stderr alone. A store another process holds is the diagnostics `not inspected` warn, not an error and not a note on the ok repairs summary. The removal of the `state` file is dialled **immediately before it happens**, not on the verdict the run chose its directory with: a server that started during the repair keeps its `state` file and the run notes that the channel answered. **That dial uses the name the document records, not the derived one, and it applies to a file this build cannot read**: a `state` file whose `version` is not this build's is still evidence — it is what a *newer* server writes — so the channel it names is probed before anything is cleared, on every platform. A file that does not decode at all names no channel and is reclaimed, which is the one case where "cannot read" stands for "nobody is behind it". Neither flag ever touches user data (bucket directories and objects are never modified).

With `--read-only` (or `[server] read_only = true`), the root-writability check is skipped (read-only roots are valid) and state-dir checks target the home state dir.

`tinio doctor` deliberately carries no api-only note, unlike `status` and `stop`: the command exists in every build, and only `--dry-run`, `--fix` and the state/socket check are compiled out without the `api` feature (the qualifier above, on that check). `contracts/doctor-report.schema.json` is the normative shape of `--json`.

### `tinio service`

```
tinio service VERB [DIR] [--name NAME] [--user]
```

`VERB` is `install`, `uninstall`, `start`, `stop` or `query` (FR-035). This is the supervisor's half of running a server: the verbs register a service with the platform's own supervisor — systemd on Linux, launchd on macOS, the Service Control Manager on Windows — so the server comes back after a reboot without an operator at a shell. The command talks to the OS and to no server, so it is never dispatched through a runtime, and the `api` cargo feature does not gate it.

| Argument | Meaning |
|---|---|
| `VERB` (positional) | `install` \| `uninstall` \| `start` \| `stop` \| `query` |
| `DIR` (positional) | The storage root, resolved exactly as `tinio server` resolves it (canonicalised); absent: walk up from the current directory to the nearest ancestor containing `.tinio/` |
| `--name NAME` | The service's label; default: derived from the canonical root (below) |
| `--user` | Use the user level — a systemd user unit or a launchd agent — instead of the system level (root/Administrator); no elevation. Not available on Windows |

The **label** is the service's identity to the supervisor, and it is derived from the root: `tinio-<sha256(canonical root)[..16]>`, the first 16 hex characters of the SHA-256 over the canonical storage root's path bytes — the same **shape** the derived control-channel name uses (`tinio-<sha1(canonical root)[..16]>`: shape shared, digest not — the socket and the pipe name are SHA-1 while this label is SHA-256, so the two must never be "unified"). The input is the canonical root, so `./data` and `/abs/data` name one service and not two, and several roots on one machine are several services. `--name` overrides the derivation verbatim.

The **registered command** is the server argv the operator would type: `server <DIR>`, plus `--read-only` when `TINIO_READ_ONLY` selected that mode in the installing environment, plus `--state-dir <resolved>` in that same read-only case. Read-only state is `TINIO_STATE_DIR` or the installing user's home; a supervisor starts the process with neither, and the server does not take its state dir from the working directory, so the argv records the path install resolved. The unit's `WorkingDirectory=` is that same **resolved state dir** (`<root>/.tinio`, or the relocated directory in read-only mode), never the served root — a unit pointing at the root would send a read-only deployment's relative paths into the tree that mode promises to leave alone. WorkingDirectory is for those relative paths; it is not a second copy of the state-dir rule. The unit text is the repository's own — the `Type=notify` rendering `packaging/tinio.service` shows, with its `WatchdogSec=` line, in place of the crate's default `Type=simple` template, under which `READY=1` and `WATCHDOG=1` are inert — and it is **systemd's alone**: the readiness-and-watchdog protocol it declares is systemd's, and launchd has no `Type=notify` equivalent to receive it. Each backend's *service file* is its own artifact — a systemd INI unit, a launchd plist, an OpenRC/rc.d shell script — so our INI unit handed to any of the others would be written verbatim into a file that supervisor cannot load (launchd writes `contents` straight to `<label>.plist` and `launchctl load`s it). Every non-systemd backend therefore takes the crate's own template for its platform. The SCM has no file form at all, so `tinio service install` is the only route on Windows and there is nothing to keep in step there.

The **level** is `--user`, and the default is the system level. `install`, `uninstall`, `start` and `stop` at the system level need root or Administrator, and a failure the supervisor reports as a **privilege denial** is answered with the remedy rather than the supervisor's own refusal text (exit 1): `tinio service install` needs root: pass `--user`, or re-run with elevation — on unix; `tinio service install` needs Administrator: run this from an elevated prompt — on Windows. A privilege denial is recognised by the error `kind` **or** by the refusal text the platform actually yields — the file-writing backends report `EACCES` as a `PermissionDenied` kind, while a supervisor that shells out (`sc.exe`, `systemctl`, `launchctl`) reports it as a command failure carrying `Access is denied` or `Permission denied`, and the SCM's is exit code **5** with `[SC] OpenSCManager FAILED 5: Access is denied.` — because that is what the three supervisors emit, not what a kind would ideally say. Everything else keeps the backend's own message, a service that is simply not installed included. At the user level the refusal names what a user unit still needs instead of a flag already in effect (ownership of the program and the state dir). `--user` on Windows is a **usage** error (exit 2): the SCM has no user-level services, so the flag cannot be honoured there.

`stop` is a graceful stop, never a kill: the supervisor's stop control reaches the same shutdown channel a termination signal and `POST /stop` use, so the service ceases accepting, drains in-flight requests (≤ 10 s), removes `state` and the control socket and exits 0 — the one shutdown path of the `server` section above, with the supervisor as its fourth entry point. While the drain runs the service reports itself as stopping, so a supervisor waits inside its own bound instead of assuming a hung process. **One configuration is outside this promise:** with confinement enabled (`--sandbox`, `TINIO_SANDBOX` or `[server] sandbox`), the supervised process is a *launcher* that forks, confines and supervises a child server, and no stop reaches the child through it — the stop path does not drain through a launcher in this phase, and the supervisor ends the process at its own bound. That configuration is refused by this build today — a confinement request fails closed, with a usage error naming the build when the `sandbox` cargo feature is off and a startup error naming the platform while the platform's backend has not landed (the `--sandbox` bullet above) — so the promise above holds for every run the binary accepts; carrying a stop into the launcher arrives with the confinement backends.

`query` prints `<label>: running`, `<label>: stopped` (with the supervisor's reason when it gives one) or `<label>: not installed` on stdout, and exits 0 only for `running` — the rule `tinio status` follows for a server. The four acting verbs print `<label>: installed|uninstalled|started|stopped` and exit 0. A backend failure that is not a permission denial keeps the backend's own message (exit 1).

The mode the verbs resolve is the environment's: `service` reads no configuration file — the unit it installs is what will read one — so `TINIO_READ_ONLY` selects the state dir and the argv above, and `TINIO_STATE_DIR` is honoured exactly where the server honours it. Both are carried by hidden arguments (`--read-only` is the server's spelling and this command's table has none) and both are answered **before** `<state dir>/.env` is loaded, which is deliberate: a command that reads no configuration has no `.env` in its chain, and a value from that file must not decide the argv a unit records. What the unit installs is therefore the argv a server resolves in the same invoking environment: read-only includes the resolved `--state-dir`, because the supervisor will not have that environment when it starts the process.

## Example systemd unit (packaging/tinio.service)

The unit is `Type=notify` because the server sends `READY=1` at readiness, `STOPPING=1` when a graceful shutdown begins and `WATCHDOG=1` on a timer at half of `WATCHDOG_USEC` (design decision 21). A `Type=simple` unit would make every one of those inert with no error anywhere, which is why this block is the rendering of the one template the `service` subcommand also installs rather than a second copy of it. `WatchdogSec=` is a default, not a policy: the cadence follows whatever the unit says, so it is the operator's to size.

```ini
[Unit]
Description=tinio S3-compatible local storage server
After=network.target

[Service]
Type=notify
WatchdogSec=30
WorkingDirectory=/srv/tinio/.tinio
ExecStart=/usr/local/bin/tinio server /srv/tinio
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```
