# `tinio-config` — configuration and credential resolution

`tinio-config` is the single source of truth for the server's configuration: the TOML schema (`version = 1` with its `[server]`, `[scanner]`, `[auth]`, `[log]`, `[s3]`, `[storage]`, `[pipeline]`, `[api]`, `[telemetry]`, `[owner]` and `[[users]]` sections), the discovery that finds that file and creates it on a first start, and the credentials a first start generates. It implements the configuration contract (`contracts/config.md`) — the precedence chain, the schema's validation stance, and the loopback-only development credential rule. It does not own the flags or the variables themselves: every variable is a clap argument declared in `tinio-cli`, which is also where a value's *provenance* is read. It owns no server behaviour: the values it resolves are consumed by the backends and the data plane, not here.

## The precedence chain

`CLI flags > process environment > .env > config file`.

The bottom two levels are this crate's. `<state dir>/.env` is loaded with `dotenvy`, which never overrides a variable the process already carries — so the process environment sits above the file by construction rather than by a rule of its own. The config file is the base every other level overlays, and `schema::Config::parse` is its only reader. The chain's variables are `TINIO_HOST`, `TINIO_PORT`, `TINIO_LOG_LEVEL`, `TINIO_LOG_FORMAT`, `TINIO_READ_ONLY`, `TINIO_SANDBOX`, `TINIO_STATE_DIR`, `TINIO_ACCESS_KEY`, `TINIO_SECRET_KEY` and `TINIO_PG_DSN`.

## Discovery and auto-create

`discover::resolve_path` searches in order: an explicit `--config` path, returned verbatim and required to be a file already; the reserved directory `<root>/.tinio/`; the state directory; and, in each, `.tinio.toml` before its `config.toml` alias. In read-write mode the reserved directory **is** the state directory, so the two are one probe; in read-only mode they differ, and the state directory is exactly where an earlier read-only run left its file.

`discover::load_or_create` reads that path and creates it when it is absent. The only reachable input for the create branch is the state directory's own `.tinio.toml`, written with a generated `[auth]` pair, and the create call is the whole of the file's protection — unix `mode(0o600)`, so it never exists wider than that; on Windows the create installs no descriptor, and `contracts/config.md` holds both platforms' clause. Read-only mode refuses a path under the served root outright: nothing under the served root is ever written.

## The schema

One module per TOML section, the root document being `Config`; section types drop the section prefix. Sections are **presence-gated**: an absent optional section parses as `None` and is skipped when the config is re-serialized. Validation is fail-fast at parse — the schema does not reject unknown keys itself, `serde_ignored` collects them and they are refused, value rules come from `garde` attributes on every section, and both map onto one `Error`. A failure here is always a startup error: there is no partially-valid configuration.

## Credentials

A generated pair is a CSPRNG access key of at least 16 bytes and a secret key of at least 32. An absent `[auth]` section is the **loopback-only development pair** — not a credential at all — so a non-loopback bind must not serve it: `tinio-cli` refuses that start (or takes the pair from the environment) and warns on the loopback bind it does serve, while the rule itself is the schema's, stated on the section. There is deliberately no `anonymous` key: anonymous mode is a flag or an environment channel, so the key is refused as unknown.

## The state directory

`tinio-config` resolves the state directory the whole run hangs off — where it sits in each mode, and what read-only mode relocates, are `../../CONTEXT.md`'s "State dir" and "Read-only mode" terms. `TINIO_STATE_DIR` is the explicit override, and its parser refuses an empty value by name.

## Layout

| File | What it is |
|---|---|
| `src/schema/mod.rs`, `src/schema/<section>.rs` | the root document and one module per TOML section, `src/schema/storage/pg.rs` for `[storage.pg]` |
| `src/discover.rs` | finding the config file, and creating it on a first start |
| `src/credentials.rs` | the generated `[auth]` pair |
| `src/sources.rs` | the file level of the chain: `.env` loading |
| `src/state.rs` | state-directory resolution and the `TINIO_STATE_DIR` parser |
| `src/error.rs`, `src/lib.rs` | the crate's one error type, and its re-exports |

## Gates

```console
cargo test -p tinio-config
```

Database-free and server-free: every leg parses a document or writes a temp file. The created credential file's protection is a platform leg — the unix arm asserts the mode on the file itself, and the Windows arm asserts that a fresh reader and a fresh writer both reach it and that it does not carry the protected single-entry descriptor the create used to install.
