# Design: module-owned configuration with one composition root

**Date**: 2026-10-17
**Status**: shipped — acceptance evidence is [the plan's Task 9 Evidence line](../plans/2026-10-17-config-ownership-redesign.md) (the full gate's commands and their results).
**Scope**: delete `tinio-config`; redistribute its schema, loading, discovery, credential and state code to the crates that read it; make serde/garde optional on the receiving library; wire `[scanner]` to `ScannerOptions`; update every manifest, facade re-export, doc example, README, contract pointer and traceability home that names the crate. No configuration key, default, validation rule or source precedence changes; the one intentional wiring change is `[scanner]` (D16).

## Goal

Every configuration field has exactly one schema definition: in the crate that reads it at runtime, or, where the CLI converts it into another crate's runtime value, in the CLI composition root. The whole-document TOML type, unknown-key collection, cross-section validation and source loading live in `tinio-cli`, the only crate above every consumer. `tinio-config` is deleted. `tinio::Config` remains the public spelling by re-exporting the CLI's type.

## Non-goals

- No key, default, validation message, precedence or serialization-shape change: the TOML contract in `specs/001-s3-local-server/contracts/config.md` is unchanged.
- No new configuration key. `[scanner]` is the one key group that gains wiring (D16); every other key keeps its current readers and behavior.
- No cross-module aggregate configuration type. Composition happens at the call sites that already receive the individual values; `identity::Config` is one module's config (D11).
- No change to state-directory semantics: `state` moves with its behaviour, not its layout.
- No change to feature-gated backend behaviour. `[storage]` keys keep parsing in every build (PG5).
- No commit, CI or benchmark-regime change.

## Evidence base

Measured in the tree on 2026-10-17.

| Fact | Evidence |
|---|---|
| `tinio-config` size | 4,946 lines across 19 source files; schema 3,205 lines |
| external code references | 82 mentions in 33 files outside the crate |
| live code that names the crate | `tinio-server` (`_config` hidden extern, `identity`, `log`, `pipeline`, `backend`, `error`, tests, benches); `tinio-cli`; `tinio` facade; `tinio-e2e` through `tinio_server::_config` |
| server dependency profile | `tinio-server` has no `serde`, `garde`, `smart-default` or `parse-display` dependency today |
| existing runtime/schema split | `tinio-fs` has `FsOptions`, `tinio-mem` has `MemoryOptions`, `tinio-pg` has its own `Config`; `tinio-pg/src/config.rs` states the split explicitly |
| `[storage.pg]` parse rule | `schema/storage/pg.rs` and PG5: no `[storage]` key is feature-gated, so a pg document parses in a build without the pg backend |
| `[scanner]` wiring | the contract says section present = on, absent = off; `ScannerOptions` already carries `enabled` and the three pacing values; `tinio-cli`'s `start::serving` hardcodes `enabled: true` and the defaults; `Config::generated` writes the section; `Scanner::new` already applies `TINIO_SCANNER` |
| dead public helpers | `server::probe_server_keys`, `server::probe_read_only`, `sources::parse_read_only`, `sources::parse_sandbox` have no production caller |
| CLI duplicates | `READ_ONLY_VAR` and `SANDBOX_VAR` are defined in `tinio-cli::cli` and again in `tinio-config::sources` |
| test-only state users | `tinio-server/tests/read_only.rs` and `tinio-server/tests/support/serve_spawn.rs` read `tinio_config::state` and the env-name constants; no server production path does |
| traceability home | `tinio-e2e/tests/traceability.rs` names `crates/tinio-config/src/sources.rs` as T018's home |
| live prose naming the crate | root `README.md`, `crates/tinio/README.md`, `specs/001-s3-local-server/contracts/config.md`, `specs/001-s3-local-server/plan.md`; historical specs, plans and reviews are records |

## Target layout

```
tinio-server
  log::Config, Verbosity, Format, AccessFormat
  pipeline::Config, Io, Remove, Db, Priority
  s3::Capabilities                  (backend::Capabilities re-exports the definition)
  identity::Config, auth::Config, owner::Config, users::Config

tinio-util
  state                              (path derivation, hardening, errors)
  env                                (READ_ONLY_VAR, SANDBOX_VAR, STATE_DIR_VAR)

tinio-cli
  config::Config                     (root document, Version, parse/load)
  config::server, scanner, storage, api, s3, telemetry
  config::discover, sources, credentials, error, state

tinio
  Config, ConfigError                (re-exports of the CLI types)
```

## Section ownership

| TOML | Definition owner | Readers |
|---|---|---|
| `[log]` | `tinio_server::log` | server logging; CLI start flags read `Format`/`Verbosity` and fold them back into the section |
| `[pipeline]` | `tinio_server::pipeline` | `Pipelines::build` |
| `[s3]` | `tinio_server::s3::Capabilities` (flattened) plus `tinio_cli::config::s3` for the remaining keys | server capability checks; CLI maps sweep and multipart limits to the fs backend |
| `[telemetry]` | `tinio_cli::config::telemetry` | CLI reads the endpoint; server builds the export layer from that value |
| `[auth]`, `[owner]`, `[[users]]` | `tinio_server::identity` | identity assembly; per-field validation travels with the types |
| `[server]` | `tinio_cli::config::server` | CLI bind, read-only and sandbox decisions |
| `[scanner]` | `tinio_cli::config::scanner` | CLI builds `ScannerOptions` from it (D16) |
| `[storage]`, `[storage.pg]` | `tinio_cli::config::storage` | CLI constructs `FsOptions`, `MemoryOptions` and `tinio_pg::Config` |
| `[api]` | `tinio_cli::config::api` | CLI management-plane transports |
| `version` and the root | `tinio_cli::config` | the composition root |

## Decisions

- **D1 One definition per field** — each field is defined in the crate that reads it at runtime. Where the CLI converts a field into another crate's runtime value (storage, the `[s3]` sweep keys, scanner), the schema lives in the CLI and the runtime type stays with its consumer; no mirror schema. A TOML table whose keys have different readers is split with `#[serde(flatten)]`, so one table stays one table.
- **D2 The composition root is `tinio-cli`** — the root document, `Config::parse`/`load`, `serde_ignored` unknown-key collection, cross-section owner/users validation, `.env` loading, discovery and credential generation live in `tinio_cli::config`. `tinio-config` is deleted; its README and manifest go with it.
- **D3 `tinio-server` owns its sections and drops `tinio-config`** — `log`, `pipeline`, `s3::Capabilities` and `identity` schema types move in; `[telemetry]` does not. The crate gains a `config` feature that enables optional `serde` and `garde` (with `derive` and `regex`), and an unconditional `parse-display` for the log enums. `smart-default` stays unconditional because `Default` is runtime behaviour, not parsing behaviour. All serde and garde derives and field attributes are wrapped in `#[cfg_attr(feature = "config", ...)]`. `config` is not a default feature; the CLI and the facade enable it explicitly.
- **D4 CLI-owned sections** — `[server]`, `[scanner]`, `[storage]`, the `[s3]` non-capability keys, `[telemetry]`, `[api]`, `version` and the root document live in `tinio-cli`. `sig_v2` has no reader and stays in the CLI's `s3` schema, unwired. The CLI enables `tinio-server/config`; the facade enables it too so the parsing seam is visible in every graph that re-exports `Config`.
- **D5 `[storage]` stays backend-agnostic** — like the `[s3]` sweep keys and `[scanner]`, the section is defined once in the CLI and converted to `FsOptions`, `MemoryOptions` and `tinio_pg::Config`; it is never moved behind a backend feature (PG5). The pg runtime type keeps its no-default, no-environment semantics.
- **D6 Identity split** — `auth`, `owner` and `users` schema types move to `tinio_server::identity`, which also defines the identity module's own `Config { auth: Option<auth::Config>, owner: owner::Config, users: Vec<users::Config> }`. The CLI composes that value, applies the root's owner default, generates first-start credentials and runs the cross-section uniqueness and anonymous-ID validation. Per-field rules (`validate_canonical_id`, the Windows `local_uid` rejection) travel with the types. `Identity::from_config`, `root_auth_pair`, `uses_builtin_fallback` and `owner_uids_for` take `&identity::Config`, not the root document. The chown-privilege probe moves to the CLI's startup path, where its only caller runs; it returns the CLI configuration error and preserves the `InvalidValue { key: "local_uid", .. }` message. With no constructor left, `tinio_server::Error::Config` is deleted. `identity::Config` is assembled after parsing and does not derive serde.
- **D7 `state` and env names sink to `tinio-util`** — the path derivation, hardening and their error type become `tinio_util::state` behind a `state` feature (server tests and the CLI enable it). That error keeps the `Missing`, `InvalidValue` and `Root` variants and therefore their messages. `READ_ONLY_VAR`, `SANDBOX_VAR` and `STATE_DIR_VAR` become `tinio_util::env`, replacing the CLI's duplicate definitions. `parse_state_dir` stays in the CLI as `tinio_cli::config::state` because it is flag parsing; it returns `tinio_util::state::Error`, which the CLI's `State` variant carries.
- **D8 Source loading, discovery, credentials and errors move to the CLI** — `load_env_file`, `discover`, `credentials`, `parse_state_dir` and the configuration error type become `tinio_cli::config`. The configuration error loses `Root` (the state error owns it) and the CLI's top-level error gains a state variant wrapping `tinio_util::state::Error`; a state failure still names its path and cause, now through the state variant rather than `ConfigError`. `tinio::ConfigError` re-exports the CLI's configuration error.
- **D9 Dead helpers are deleted, not moved** — `server::probe_server_keys`, `server::probe_read_only`, `sources::parse_read_only` and `sources::parse_sandbox` have no production caller; deleting them keeps the move from carrying dead surface. `parse_switch` in `tinio-util` remains the one syntax definer.
- **D10 Public surface** — `tinio::Config` and `tinio::ConfigError` keep their paths and behaviour, now sourced from `tinio_cli::config`. The hidden `_config` re-exports on `tinio`, `tinio-server` and the e2e test crate are removed. Every crate is `publish = false`, so no compatibility shim is added (the crate precedent is the Windows DACL design's D5).
- **D11 No cross-module aggregate** — server modules expose their own config types and the CLI passes them to the constructors that consume them. `identity::Config` is one module's config (the three values its functions read), not a server-wide bundle; a `tinio_server::Config` spanning log, pipeline, s3, telemetry and identity, or a new `tinio-config`-shaped crate, is not introduced.
- **D12 e2e and facade follow the owner** — `tinio-e2e` adds `tinio-cli` as a dev-dependency and imports the root, discovery, server, auth, storage and identity types from `tinio_cli::config` and `tinio_util`; its `extern crate` seam gains `_cli`. `tinio` removes `tinio-config` from its dependency list and re-exports the CLI type.
- **D13 Migration order keeps every step compilable** — (a) add `tinio_util::state` and `tinio_util::env` and update their callers; (b) add the server-owned types with the `config` feature and switch server internals, examples, tests and benches, converting from the old types in the CLI at the seam until (c); (c) move the remaining schema and machinery into `tinio_cli::config` and switch the CLI; (d) switch the facade and e2e; (e) delete `tinio-config` last, when nothing names it. Each step runs the workspace check; steps may merge where a temporary conversion is not worth a separate diff, but the crate is deleted only after its last consumer has moved.
- **D14 Prose follows the record rule** — every live mention that asserts the crate exists or owns a fact is corrected in place: the root README crate index and every crate README that names it (including `tinio-cli`, `tinio-server`, `tinio-auth`, `tinio-fs`, `tinio-pg`, `tinio-store-pg`), doc comments that point at the crate (`tinio-core`, `tinio-util`, `tinio-sandbox-core`, `tinio-fs`, `tinio-pg`), the config contract's implementation pointer, the plan's structure decision and tree, the e2e feature-file comment, and the traceability home for T018. Historical specs, plans, reviews and completed task records stay as written; this design owns the superseding fact.
- **D15 Behaviour is pinned except D16** — moved attributes are copied verbatim except for the `cfg_attr` wrapper; `#[serde(flatten)]` on `[s3]` capabilities, the `type` rename on `[storage]`, `humantime` handling on `[scanner]`, skip-serializing fields and every default stay identical. Section tests travel with their types; root parsing, unknown-key and cross-section tests land beside the CLI root. The one intentional behavior change is the scanner gate in D16.
- **D16 `[scanner]` is wired** — the CLI builds `ScannerOptions` from the section: presence drives `enabled`, absence is off; `delay`, `max_wait` and `cycle` map to their fields. `TINIO_SCANNER`'s `1`/`0` override in `Scanner::new` and the read-only early return are unchanged. `Config::generated` already writes the section, so a fresh install still scans; a hand-written configuration without the section stops scanning. That is the contract's existing presence rule being implemented, not a new rule.

## Manifests

The table is indicative, not exhaustive: the builder takes every dependency the compiler proves necessary.

| Crate | Gains | Loses |
|---|---|---|
| `tinio-server` | `parse-display`; optional `serde` + `garde` (derive, regex) behind `config`; `smart-default`; `derive_more` with `eq` for `users::Config` | `tinio-config` dependency and `_config` extern |
| `tinio-util` | `dirs`, `thiserror`, `sha2` and `hex` behind `state`; `state` feature | — |
| `tinio-cli` | `dotenvy`, `garde` (derive, url), `getrandom`, `serde_ignored`, `toml`, `humantime-serde` | `tinio-config` dependency and `_config` extern |
| `tinio` | — | `tinio-config` dependency and `_config` extern |
| `tinio-e2e` | `tinio-cli` dev-dependency | reach into `tinio_server::_config` |
| `tinio-config` | — | the crate, its manifest and README |

The builder chooses exact feature spellings for `garde` and `serde`; the `config` feature name and the rule that serde and garde attributes ride it are fixed here.

## Ownership (one definer per fact)

- The TOML key set, defaults and validation semantics — `specs/001-s3-local-server/contracts/config.md`; this design changes only where the implementation lives.
- Each section's struct, its derives and its per-field rules — the module named in the ownership table.
- The root document, unknown-key collection and cross-section validation — `tinio_cli::config`.
- State layout, hardening and env names — `tinio_util`.
- The public configuration spelling — `tinio`'s re-export of the CLI type.
- The crate index and each crate's README — root `README.md` and the crate READMEs; `tinio-config`'s row and file go.
- The T018 documentation home — `tinio-e2e/tests/traceability.rs`, corrected to the CLI's `config/sources` path when the move lands.

## Testing & acceptance

- `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and the rustdoc gate are clean.
- `cargo check -p tinio-server --no-default-features` and `--features config` both build; the first proves the config types need no serde or garde, the second proves the parsing surface.
- The configuration suite moves with its assertions: unknown keys still fail, the garde rules still fire with the same messages, defaults are unchanged, and the TOML round-trip is byte-identical where it was before.
- The `[s3]` split is exercised through the root parser: capability keys, sweep keys and an unknown key in the same table, with the capability defaults unchanged.
- The scanner gate pins four combinations: section present with custom values, section absent (off), `TINIO_SCANNER=1` over an absent section (on), and `TINIO_SCANNER=0` over a present section (off). Read-only stays off regardless, and the auto-created document still contains `[scanner]`.
- The facade test that parses `version = 1` through `tinio::Config` stays green. `tinio::ConfigError` still names parse and validation failures; state-directory failures keep their `Missing`/`InvalidValue`/`Root` messages but surface through the CLI's state variant, and the acceptance test pins that chain.
- The e2e cucumber suite is green; the T018 traceability row resolves to the new path.
- A feature-off build is not enough on its own: the workspace unifies features, so the optionality is checked with `cargo tree -p tinio-server` or an isolated `cargo check` naming no `config` feature.

## Risks

- **Optional serde is invisible in the workspace build.** Cargo unifies the CLI's `config` feature into every workspace build; a stray ungated `serde` attribute or an accidental unconditional dependency would ship unnoticed unless the isolated check in Testing is run.
- **`cfg_attr` drift.** Twenty-plus fields in `[s3]` alone need conditional attributes; a missed wrapper breaks the feature-off build, and a wrongly wrapped default changes the schema. The moved config suite is the guard.
- **Unknown-key false positives.** If a CLI-only key is left out of the root document, a valid document fails as unknown. The contract's key list and the existing unknown-key tests are the check.
- **Public path and variant churn.** `tinio_server::_config`, `tinio::_config` and e2e's reach into it disappear, and `tinio_server::Error::Config` loses its only constructor and is deleted; the chown probe and its test move to the CLI. Every crate is `publish = false`; the facade keeps `Config`/`ConfigError`, so the only consumer is in-tree.
- **State error surface.** `ConfigError::Root` moves to `tinio_util::state::Error`, so a test matching the old variant must follow the new chain; the moved message text is the guard that the failure still reads the same.
- **e2e build graph.** Adding `tinio-cli` to `tinio-e2e` pulls the CLI's feature set into the test build; acceptable because the e2e crate already builds the server and backends, but it must be checked on the CI legs.
- **State feature on util.** A `state` feature adds the first `dirs` and `thiserror` edges to `tinio-util`; server tests must enable it explicitly, and a missing feature is a compile error rather than a silent fallback.
- **One intentional behaviour change.** A hand-written configuration without `[scanner]` stops scanning, while auto-created configurations keep the section and keep scanning. The contract already says absent = off; this change closes the implementation gap. The acceptance combinations in Testing are the guard.
- **`[s3]` split drift.** A missing `flatten` or a capability field duplicated in the CLI would either reject a valid key or create a second definition. The root parser tests and the ownership table are the check.
- **Doc drift.** Roughly 285 document mentions name the crate. The record rule keeps historical documents as records and corrects only live prose; the ownership check runs before the phase closes.
