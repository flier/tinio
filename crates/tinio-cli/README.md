# `tinio-cli` — the command-line interface

`tinio-cli` is the CLI: the lifecycle verbs an operator types, the process sequence behind them, and the exit codes they answer with. It implements the CLI contract (`contracts/cli.md`) — a Minio-style invocation (a positional directory, default ports 9000/9001) and the exit-code mapping `0` success, `1` operational error, `2` usage error. It owns the **verb surface** — what each verb does at the operator's level — and the `service` verbs that register the server with the platform's supervisor. It owns no configuration schema (`tinio-config` owns the chain and the schema) and no control-plane mechanics (`tinio-api` owns the state file, the transports and the endpoints): what this crate does is reduce flags, environment and the config file to one resolution and hand each plane the inputs it expects.

## The verbs

```console
tinio server [DIR]          # `start` is the alias
tinio status [DIR]
tinio stop   [DIR]
tinio doctor [DIR] [--json] [--dry-run | --fix]
tinio service <install|uninstall|start|stop|query> [DIR] [--name NAME] [--user]
```

| Verb | What the operator gets |
|---|---|
| `server` (alias `start`) | serves the storage root; auto-creates the configuration on a first start; `--daemon` detaches with no supervisor and no console, and the parent waits for readiness before it returns |
| `status` | reports whether a server is running for a root — a read, so nothing is created and nothing is deleted |
| `stop` | asks the running server to stop and waits for its exit; the server's own plane removes the `state` file and the socket on the way out |
| `doctor` | offline diagnostics of a directory, server or not; `--json` renders the same report, `--dry-run` lists what a fix would change, `--fix` applies the repairs and requires the server for that root to be stopped |
| `service` | the supervisor's verbs, below |

`status` and `stop` are the two verbs compiled out of a build without the `api` feature, where they become an unknown subcommand; `doctor` exists in both builds, and only its plane-dependent half — `--dry-run` and `--fix` — is gated with them.

## The supervisor

`service install` registers the server with the platform's supervisor — systemd, launchd or the SCM — and `uninstall`, `start`, `stop` and `query` act on that registration. The label defaults to one derived from the root, so several roots are several services, and `--user` installs at the user level (a systemd user unit or a launchd agent) with no elevation. The unit text for the systemd case is this crate's own, because the default template cannot carry the readiness and watchdog notifications the server sends; on Windows the SCM's dispatcher is what lets the service start at all. Every verb here is synchronous and builds no runtime — installing, querying or stopping a service talks to the OS and to no server — and `service` reads no configuration file: the unit it installs is what will read one.

## Features

| Feature | Effect |
|---|---|
| `api` | the `status` and `stop` verbs, and the management plane they talk to (default) |
| `mem` | the in-memory backend becomes selectable (default) |
| `sandbox` | `--sandbox`: process confinement (default; implies `api`) |
| `versioning` | the version plane (default) |
| `pg` | the PostgreSQL backend becomes selectable |
| `tls` | the management plane's HTTPS transport (implies `api`) |

## Layout

| File | What it is |
|---|---|
| `src/cli.rs` | the command line: every env-backed argument, the two-pass parse, and the provenance |
| `src/run.rs` | the process entry point and the design's sequence |
| `src/resolve/mod.rs`, `src/resolve/locate.rs`, `src/resolve/pg.rs` | root, mode, state directory and config resolution |
| `src/commands/start/` | the serving path: its flags, the management-plane selection, the §1 serving half, the backend seam, and the metrics adapter |
| `src/commands/status.rs`, `src/commands/stop.rs` | the two client commands |
| `src/commands/doctor/` | the offline diagnosis, its check list and the audit walk |
| `src/service/mod.rs`, `src/service/windows.rs` | the supervisor verbs, and the SCM dispatcher |
| `src/sandbox/` | the `--sandbox` seam (operator notes: `docs/sandbox.md`) |
| `src/daemon.rs` | `--daemon`: no supervisor, no console, a parent that waits for ready |
| `src/signal.rs` | termination signals turned into the one shutdown channel |
| `src/notify.rs` | what systemd is told, and the unit text it is told it by |
| `src/events.rs`, `src/report.rs` | the serving path's stdout contract, and the doctor's report |
| `src/error.rs`, `src/lib.rs` | the exit-code mapping and the crate's entry points |

## Gates

```console
cargo test -p tinio-cli
```

The `pg` legs read `TINIO_PG_DSN` and skip when it is unset, so the default run stays database-free; what the `service` verbs' legs are is `docs/tests.md`'s. The process-level legs that spawn the built binary as an operator would are cucumber scenarios in `tinio-e2e`.
