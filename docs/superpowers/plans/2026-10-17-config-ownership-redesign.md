# Module-Owned Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Delete `tinio-config` by moving each TOML section to the crate that reads it, make `tinio-cli` the single composition root, and wire `[scanner]` to `ScannerOptions` on the way.

**Architecture:** Module-owned config types keep one definition per field. `tinio-server` owns `log`, `pipeline`, `s3::Capabilities` and `identity` behind an optional `config` feature; `tinio-cli` owns `[server]`, `[scanner]`, `[storage]`, the remaining `[s3]` keys, `[telemetry]`, `[api]`, the root document and all loading/discovery machinery; `tinio-util` owns state-path resolution and the env names. `tinio` re-exports the CLI's root type as `Config`.

**Tech Stack:** Rust 2024, cargo features, serde/garde/smart-default, parse-display, toml + serde_ignored, cucumber e2e.

**Spec:** `docs/superpowers/specs/2026-10-17-config-ownership-redesign.md`

## Global Constraints

- Code, comments, docs and commit messages are English only; chat stays Chinese.
- `Cargo.toml` files carry no comments; keep each crate's existing eight clippy lints and `unsafe_code` setting.
- No production panics: `unwrap`/`expect`/`panic!`/`assert!` stay in tests, benches and doc examples.
- Imports: top `use`, never inline; hidden `extern crate tinio_* as _*` seams stay in `lib.rs`.
- Per `docs/git.md`, the agent never stages or commits: leave every change in the tree, report the pending files and propose a commit message, and the user decides the commit. No AI trailers.
- Every task ends with the stated `cargo` command green and a pending-change report.
- `tinio-server/config` is not a default feature; `tinio-cli` and `tinio` enable it explicitly.
- `[storage]` keys parse in every build (PG5); never move them behind a backend feature.
- Preserve every TOML key, default, validation message, `serde(rename)`, `serde(flatten)`, `skip_serializing_if` and `humantime` handling verbatim.
- The only intentional behavior change is the `[scanner]` presence gate (Task 7).

## Execution

- Subagent-driven: a fresh implementer per task and a fresh reviewer per task; the user is interrupted only after Task 9.
- Each task runs its listed focused commands; Task 9 runs the full workspace gate.
- Work in the current working tree on `dev`; no worktree.
- Never stage or commit; every task ends by reporting the pending files and a proposed message.

## Review Focus

- **A key owned by the wrong crate.** A valid document must not fail as an unknown key after the split; the root parser tests in Task 6 must exercise every section, including capability keys, sweep keys, scanner keys and telemetry.
- **Feature-off drift.** A serde or garde attribute left ungated breaks `tinio-server` without `config`; a default wrapped in the wrong `cfg_attr` changes the schema. Task 4's feature-off check is the guard.
- **`[s3]` split drift.** A missing `flatten` or a duplicated capability field either rejects a valid key or creates a second definition; Task 6's document test covers both halves of the table.
- **Scanner behavior.** A hand-written document without `[scanner]` must now stop scanning, while an auto-created document must keep scanning; Task 7 pins both and the env override remains exercised by the existing `tinio-fs` tests.
- **State error chain.** A state-directory failure must still name its path and cause, now through `tinio_util::state::Error` and the CLI's `State` variant; Task 1 and Task 6 pin the messages.

---

### Task 1: Move state resolution and env names to `tinio-util`

**Files:**
- Create: `crates/tinio-util/src/state.rs` (moved from `crates/tinio-config/src/state.rs`, minus `parse_state_dir`)
- Create: `crates/tinio-util/src/env.rs`
- Modify: `crates/tinio-util/src/lib.rs`, `crates/tinio-util/Cargo.toml`
- Create: `crates/tinio-cli/src/config/mod.rs`, `crates/tinio-cli/src/config/state.rs`
- Modify: `crates/tinio-cli/src/lib.rs`, `error.rs`, `cli.rs`, `commands/mod.rs`, `commands/doctor/mod.rs`, `resolve/locate.rs`, `resolve/pg.rs`, `commands/start/run.rs`
- Modify: `crates/tinio-config/src/lib.rs`, `discover.rs`, `sources.rs`, `error.rs`; delete `crates/tinio-config/src/state.rs`
- Modify: `crates/tinio-server/tests/read_only.rs`, `tests/support/serve_spawn.rs`, `crates/tinio-server/Cargo.toml` (dev-dependency feature)

**Interfaces:**
- Produces: `tinio_util::state::{BASE_DIR_NAME, home_state_dir, default_home_state_dir, home_state_dir_path, default_home_state_dir_path, prepare_home_derived, prepare_synthetic_root, Error}`; `Error` has `Missing(String)`, `InvalidValue { key: String, reason: String }`, `Root { path: PathBuf, source: io::Error }` and `Error::invalid_value(key, reason) -> Self`.
- Produces: `tinio_util::env::{READ_ONLY_VAR, SANDBOX_VAR, STATE_DIR_VAR}` with values `"TINIO_READ_ONLY"`, `"TINIO_SANDBOX"`, `"TINIO_STATE_DIR"`.
- Produces: `tinio_cli::config::state::parse_state_dir(raw: &str) -> Result<PathBuf, tinio_config::Error>`; Task 6 changes the error type to `tinio_cli::config::Error`.
- Produces: `tinio_cli::Error::State(#[from] tinio_util::state::Error)`.

- [x] **Step 1: Move the state module and its tests**

Move `crates/tinio-config/src/state.rs` to `crates/tinio-util/src/state.rs`. Replace `crate::{_core::object::RESERVED_SEGMENT, Error, error}` with `crate::_core::object::RESERVED_SEGMENT` and a local `Error`. Remove `parse_state_dir` and its tests from the moved file; keep `BASE_DIR_NAME` as `pub`. Define the error exactly as the Interfaces block states, with the existing `#[error(...)]` strings and a private `fn root(path, source) -> Error`.

- [x] **Step 2: Add the util feature and module declarations**

In `crates/tinio-util/Cargo.toml` add optional `dirs`, `thiserror`, and reuse the optional `sha2` plus `hex` under a `state` feature: `state = ["dep:dirs", "dep:thiserror", "dep:sha2", "dep:hex"]`. Add `pub mod env;` and `#[cfg(feature = "state")] pub mod state;` to `crates/tinio-util/src/lib.rs`. Enable `state` on the normal `tinio-util` dependency in `crates/tinio-config/Cargo.toml` (for `discover`) and in `crates/tinio-cli/Cargo.toml` (`features = ["render", "state"]`). No comments in the manifests.

- [x] **Step 3: Add the env constants**

Create `crates/tinio-util/src/env.rs` with the three constants and a test asserting their exact strings. No parsing helpers move here.

- [x] **Step 4: Remove the old module and dead helpers from `tinio-config`**

Delete `crates/tinio-config/src/state.rs` and `pub mod state;`. In `discover.rs` replace `state::BASE_DIR_NAME` with `tinio_util::state::BASE_DIR_NAME`, and remove the `state` import. In `sources.rs` delete `READ_ONLY_VAR`, `SANDBOX_VAR`, `parse_read_only` and `parse_sandbox` with their tests; keep `load_env_file` and remove the now-unused `parse_switch` import. In `error.rs` delete `Error::Root`, `error::root` and the `Root` display test case; keep `Missing`, `InvalidValue`, `Io`, `Parse`, `UnknownKey`, `Env`.

- [x] **Step 5: Create the CLI state-value parser**

Create `crates/tinio-cli/src/config/mod.rs` with `pub mod state;` and `crates/tinio-cli/src/config/state.rs` holding `parse_state_dir` moved from the old module, still returning `tinio_config::Error` in this task. Add `pub mod config;` to `crates/tinio-cli/src/lib.rs`. Update `commands/mod.rs` to import `crate::config::state::parse_state_dir`.

- [x] **Step 6: Update the callers**

Replace `_config::state` with `_util::state` in `commands/doctor/mod.rs`, `resolve/locate.rs`, `resolve/pg.rs` and `commands/start/run.rs`. Replace `_config::state::STATE_DIR_VAR` with `_util::env::STATE_DIR_VAR` in `cli.rs` and `commands/mod.rs`. Replace the local `READ_ONLY_VAR`/`SANDBOX_VAR` definitions in `cli.rs` with `pub use crate::_util::env::{READ_ONLY_VAR, SANDBOX_VAR};`. Add `State(#[from] tinio_util::state::Error)` to the CLI error and a display/exit-code test beside the existing `Config` case. Update the doc comments that name `tinio_config::state`.

- [x] **Step 7: Update the server tests**

In `crates/tinio-server/tests/read_only.rs` import `tinio_util::state::home_state_dir`. In `tests/support/serve_spawn.rs` import `TINIO_READ_ONLY`, `TINIO_SANDBOX` and `TINIO_STATE_DIR` from `tinio_util::env`. Enable `tinio-util`'s `state` feature in the server's dev-dependencies.

- [x] **Step 8: Run the tests**

Run: `cargo test -p tinio-util --features state`
Expected: PASS
Run: `cargo test -p tinio-cli`
Expected: PASS
Run: `cargo test -p tinio-server --test read_only`
Expected: PASS

- [x] **Step 9: Report the pending change**

Report the changed files and propose: `refactor(config): move state resolution and env names into tinio-util`. Do not stage or commit.

---

### Task 2: Move `[log]` and `[pipeline]` into `tinio-server`

**Files:**
- Modify: `crates/tinio-server/src/log.rs`, `pipeline.rs`, `lib.rs`, `Cargo.toml`
- Modify: `crates/tinio-server/benches/pipeline.rs`
- Move from: `crates/tinio-config/src/schema/log.rs`, `schema/pipeline.rs`

**Interfaces:**
- Produces: `tinio_server::log::{Config, Verbosity, Format, AccessFormat}` with the existing fields, defaults and display behaviour.
- Produces: `tinio_server::pipeline::{Config, Io, Remove, Db, Priority, Queue<const DEFAULT_WORKERS: u8, const WORKERS_MIN: u8, const WORKERS_MAX: u8>}`.
- Produces: `tinio_server::log::build_subscriber(verbosity: Verbosity, format: Format, access_format: &AccessFormat, access_path: &Path, otel_endpoint: Option<&str>) -> Result<Box<dyn Subscriber + Send + Sync>, Box<dyn Error + Send + Sync>>`.
- Produces: `tinio_server::pipeline::Pipelines::build(config: &pipeline::Config) -> Result<Self, Error>` unchanged in name and parameters.

- [x] **Step 1: Add the server's optional schema dependencies**

In `crates/tinio-server/Cargo.toml` add `parse-display` and `smart-default` unconditionally; add optional `serde` (with `derive`) and `garde` (with `derive`, `regex`); add `config = ["dep:serde", "dep:garde"]`; add `toml` to dev-dependencies for section tests. No comments.

- [x] **Step 2: Define the log types in `log.rs`**

Move the four types and their doc examples from `schema/log.rs` into `crates/tinio-server/src/log.rs`. Derive `Serialize`, `Deserialize` and `Validate` through `#[cfg_attr(feature = "config", derive(...))]`. Wrap every `#[serde(...)]` and `#[garde(...)]` field attribute in `#[cfg_attr(feature = "config", ...)]`. Keep `parse-display` derives unconditional. Replace `use crate::_config::log::{self, Format, Verbosity};` with local imports of the moved types, rewrite `log::AccessFormat`/`log::Verbosity` paths to the local names, and rewrite every doc example's `tinio_config::log::...` path to `tinio_server::log::...`.

- [x] **Step 3: Define the pipeline types in `pipeline.rs`**

Move the types from `schema/pipeline.rs` into `crates/tinio-server/src/pipeline.rs` with the same `cfg_attr` treatment. Replace `use crate::_config::pipeline::{self as pipeline_config, Priority};` with local names, rewrite `pipeline_config::Config`, `pipeline_config::Io`, `pipeline_config::Db` to `Config`, `Io`, `Db`, and rewrite every doc example's `tinio_config::pipeline::...` path to `tinio_server::pipeline::...`.

- [x] **Step 4: Move the section tests**

Move the log and pipeline tests that parse only the section. Where a test parses the whole document through `tinio_config::Config`, move it to Task 6's CLI root suite instead. Section tests parse with `toml::from_str::<T>(...)` and call `garde::Validate::validate()` explicitly.

- [x] **Step 5: Update the benchmark**

Change `crates/tinio-server/benches/pipeline.rs` from `use tinio_config::pipeline::{Config, Db, Io};` to `use tinio_server::pipeline::{Config, Db, Io};`.

- [x] **Step 6: Run the tests**

Run: `cargo test -p tinio-server --features config`
Expected: PASS
Run: `cargo check -p tinio-server --no-default-features`
Expected: PASS
Run: `cargo bench -p tinio-server --no-run`
Expected: PASS

- [x] **Step 7: Report the pending change**

Report the changed files and propose: `refactor(server): own log and pipeline config sections`. Do not stage or commit.

---

### Task 3: Move `[s3]` capabilities and identity into `tinio-server`

**Files:**
- Create: `crates/tinio-server/src/s3.rs`
- Create: `crates/tinio-server/src/identity/auth.rs`, `owner.rs`, `users.rs`
- Modify: `crates/tinio-server/src/lib.rs`, `identity.rs`, `error.rs`, `backend/mod.rs`, `backend/buckets.rs`, `Cargo.toml`
- Create: `crates/tinio-cli/src/commands/start/chown.rs`
- Modify: `crates/tinio-cli/src/commands/start/mod.rs`, `commands/start/run.rs`
- Move from: `crates/tinio-config/src/schema/s3.rs`, `schema/auth.rs`, `schema/owner.rs`, `schema/users.rs`

**Interfaces:**
- Produces: `tinio_server::s3::{Capabilities, MAX_BUCKETS}`; `backend::Capabilities` re-exports `s3::Capabilities`.
- Produces: `tinio_server::identity::{Config, auth, owner, users}` with `Config { pub auth: Option<auth::Config>, pub owner: owner::Config, pub users: Vec<users::Config> }`.
- Produces: `Identity::from_config(config: &identity::Config) -> Arc<Identity>`, `root_auth_pair(config: &identity::Config) -> (String, String)`, `uses_builtin_fallback(config: &identity::Config) -> bool`, `owner_uids_for(config: &identity::Config) -> HashMap<OwnerId, u32>`.
- Produces: `tinio_cli::commands::start::chown::ensure_chown_privilege(config: &identity::Config, dir: &Path) -> Result<(), tinio_cli::Error>`.
- Removes: `tinio_server::Error::Config`.

- [x] **Step 1: Create the s3 module**

Move `Capabilities`, `MAX_BUCKETS` and the capability constants from `schema/s3.rs` into `crates/tinio-server/src/s3.rs`. Keep only the capability fields and their constants; the sweep and multipart limits stay for Task 5's CLI section. Apply the `cfg_attr` derives and attributes. Add `pub mod s3;` to `lib.rs` and change `backend/mod.rs` to `pub use crate::s3::Capabilities;`; change `backend/buckets.rs` from `_config::s3::MAX_BUCKETS` to `crate::s3::MAX_BUCKETS`.

- [x] **Step 2: Create the identity schema modules**

Create `crates/tinio-server/src/identity/auth.rs`, `owner.rs` and `users.rs` from the matching schema files. Add `pub mod auth; pub mod owner; pub mod users;` to `identity.rs`. Apply the `cfg_attr` derives and attributes, including `#[cfg_attr(all(windows, feature = "config"), garde(custom(validate_local_uid)))]`. Keep `pub use tinio_util::secret::{Secret, SecretKey};` in `auth.rs`. Move `reject_empty`, `validate_canonical_id`, `validate_canonical_id_opt` and `validate_local_uid` into `identity.rs` as private helpers. Update the `#[expect(clippy::expect_used, reason = "...")]` strings to name `identity.rs` instead of `schema/mod.rs`. Rewrite every doc example's `tinio_config::...` path to `tinio_server::identity::...`.

- [x] **Step 3: Define `identity::Config` and switch the functions**

Add the `Config` struct to `identity.rs`; it does not derive serde. Change the four public functions listed in Interfaces to take `&identity::Config`, reading `config.auth`, `config.owner` and `config.users` directly. Update the doc examples and the ACL test fixture in `identity.rs` to build the value.

- [x] **Step 4: Move the chown probe to the CLI**

Move `ensure_chown_privilege`, `probe_chown` and their tests from `identity.rs` into `crates/tinio-cli/src/commands/start/chown.rs`. The function takes `&tinio_server::identity::Config`, returns `tinio_cli::Error`, and builds `Error::Config(tinio_config::Error::invalid_value("local_uid", ...))` with the existing message; Task 6 changes that inner type to `config::Error`. Add `mod chown;` to `commands/start/mod.rs` and update the call in `run.rs`.

- [x] **Step 5: Remove the server's config error**

Delete `Error::Config` and its `#[from]` from `crates/tinio-server/src/error.rs`; update the display and send/sync tests. Check that no production code constructs it.

- [x] **Step 6: Add the server's derive_more feature**

Add `derive_more` with the `eq` feature to `crates/tinio-server/Cargo.toml` for `users::Config`'s `PartialEq`. No comments.

- [x] **Step 7: Move the section tests**

Move the s3, auth, owner and users section tests. Tests that call the root parser go to Task 6; section tests parse with `toml::from_str` and call `validate()`.

- [x] **Step 8: Run the tests**

Run: `cargo test -p tinio-server --features config`
Expected: PASS
Run: `cargo test -p tinio-cli`
Expected: PASS

- [x] **Step 9: Report the pending change**

Report the changed files and propose: `refactor(server): own s3 capabilities and identity config, move chown probe to cli`. Do not stage or commit.

---

### Task 4: Drop `tinio-config` from `tinio-server`

**Files:**
- Modify: `crates/tinio-server/Cargo.toml`, `src/lib.rs`, any remaining `_config` reference

**Interfaces:**
- Removes: `pub extern crate tinio_config as _config;` and the `tinio-config` dependency.

- [x] **Step 1: Remove the dependency and seam**

Delete `tinio-config.workspace = true` from `crates/tinio-server/Cargo.toml` and `#[doc(hidden)] pub extern crate tinio_config as _config;` from `src/lib.rs`.

- [x] **Step 2: Prove the feature-off build**

Run: `cargo check -p tinio-server --no-default-features`
Expected: PASS
Run: `cargo check -p tinio-server --no-default-features --features config`
Expected: PASS

- [x] **Step 3: Run the server suite**

Run: `cargo test -p tinio-server --features config`
Expected: PASS

- [x] **Step 4: Report the pending change**

Report the changed files and propose: `refactor(server): drop the tinio-config dependency`. Do not stage or commit.

---

### Task 5: Move the CLI-owned sections into `tinio-cli::config`

**Files:**
- Create: `crates/tinio-cli/src/config/server.rs`, `scanner.rs`, `storage.rs`, `storage/pg.rs`, `api.rs`, `s3.rs`, `telemetry.rs`
- Modify: `crates/tinio-cli/src/config/mod.rs`, `crates/tinio-cli/Cargo.toml`
- Move from: `crates/tinio-config/src/schema/server.rs`, `scanner.rs`, `storage.rs`, `storage/pg.rs`, `api.rs`, `telemetry.rs`, and the non-capability half of `schema/s3.rs`

**Interfaces:**
- Produces: `tinio_cli::config::{server, scanner, storage, api, s3, telemetry}` with their existing public types (`server::Config`, `scanner::Config`, `storage::{Config, Backend, Fs, Mem, pg::Config}`, `api::{Config, Unix, Pipe, Http, Https}`, `s3::Config`, `telemetry::Config`).
- Produces: `tinio_cli::config::s3::Config { #[serde(flatten)] pub capabilities: tinio_server::s3::Capabilities, pub sig_v2: bool, pub temp_ttl_hours: u64, pub multipart_expire_days: u64, pub noncurrent_keep: u64, pub noncurrent_expire_days: u64, pub block_grace_minutes: u64, pub block_bytes_warn: u64, pub max_concurrent_uploads: u32 }`.

- [x] **Step 1: Copy the CLI-owned section modules**

Create the six modules from their schema files with all attributes unchanged. `s3.rs` defines only the non-capability keys and flattens `tinio_server::s3::Capabilities`; `telemetry.rs` defines `otlp_endpoint`; `storage/pg.rs` keeps its own constants and `Secret`. Remove `probe_server_keys`, `probe_read_only` and their tests instead of moving them. `api.rs` also needs the old `schema/mod.rs` `reject_empty` helper; copy it as a private two-line function in `config/mod.rs` (the server's `identity.rs` keeps its own copy from Task 3). Rewrite every doc example's `tinio_config::...` path to `tinio_cli::config::...`.

- [x] **Step 2: Add the CLI's schema dependencies**

In `crates/tinio-cli/Cargo.toml` add `garde` (with `derive`, `url`) and `humantime-serde`, and enable `config` on the `tinio-server` dependency so the flattened `Capabilities` implements serde and validation. No comments.

- [x] **Step 3: Add the module declarations**

Extend `crates/tinio-cli/src/config/mod.rs` with `pub mod server; pub mod scanner; pub mod storage; pub mod api; pub mod s3; pub mod telemetry;`.

- [x] **Step 4: Move the section tests**

Move section-only tests; the root-parsing tests move in Task 6.

- [x] **Step 5: Run the tests**

Run: `cargo test -p tinio-cli`
Expected: PASS

- [x] **Step 6: Report the pending change**

Report the changed files and propose: `refactor(cli): own the server, scanner, storage, api, s3 and telemetry sections`. Do not stage or commit.

---

### Task 6: Move the root document and machinery into `tinio-cli::config`

**Files:**
- Create: `crates/tinio-cli/src/config/error.rs`, `discover.rs`, `sources.rs`, `credentials.rs`
- Modify: `crates/tinio-cli/src/config/mod.rs`, `config/state.rs`, `crates/tinio-cli/src/lib.rs`, `error.rs`, `cli.rs`, `commands/`, `resolve/`, `sandbox/`, `Cargo.toml`
- Move from: `crates/tinio-config/src/schema/config.rs`, `error.rs`, `discover.rs`, `sources.rs`, `credentials.rs`

**Interfaces:**
- Produces: `tinio_cli::config::Config { pub version: Version, pub server: server::Config, pub scanner: Option<scanner::Config>, pub auth: Option<tinio_server::identity::auth::Config>, pub log: Option<tinio_server::log::Config>, pub s3: Option<s3::Config>, pub storage: Option<storage::Config>, pub pipeline: Option<tinio_server::pipeline::Config>, pub api: Option<api::Config>, pub owner: Option<tinio_server::identity::owner::Config>, pub users: Option<Vec<tinio_server::identity::users::Config>>, pub telemetry: Option<telemetry::Config> }`.
- Produces: `Config::{parse, load, generated, to_toml, backend, pg_section, owner, users, identity}`; `identity(&self) -> tinio_server::identity::Config` composes the three identity values, applying `owner::Config::default()` when `[owner]` is absent.
- Produces: `tinio_cli::config::{Error, discover::{CONFIG_FILE, CONFIG_ALIAS, Outcome, resolve_path, load_or_create}, sources::load_env_file, credentials::{Credentials, generate}, state::parse_state_dir}`; `config::Error` has `Io`, `Parse`, `UnknownKey`, `InvalidValue`, `Missing`, `Env` and no `Root`.
- Produces: `tinio_cli::Error::{Config(#[from] config::Error), State(#[from] tinio_util::state::Error)}`.

- [x] **Step 1: Move the root document**

Move `Version`, `Config`, its parser (`parse`, `load`, `parse_at`, `sort_keys`), `to_toml`, `generated`, `backend`, `pg_section`, `owner`, `users` and `validate_owner_and_users` into `config/mod.rs`. Keep the section fields' serde/garde attributes verbatim. Add `identity()` as described in Interfaces. Rewrite every doc example's `tinio_config::...` path to `tinio_cli::config::...`.

- [x] **Step 2: Move the machinery**

Move the error enum and `from_report` into `config/error.rs` without `Root`; move `discover.rs`, `sources.rs` (only `load_env_file`) and `credentials.rs` into `config/`. `credentials.rs` imports `Secret`/`SecretKey` from `crate::_util::secret`; `discover.rs` imports the root and `crate::_util::state::BASE_DIR_NAME`; `sources.rs` drops its `parse_switch` import. Put `parse_state_dir` in `config/state.rs` and change its return type to `config::Error`.

- [x] **Step 3: Rewire the CLI**

Replace every `_config::` path with `crate::config::`; the root imports its server-owned sections from `tinio_server::{log, pipeline, identity::{auth, owner, users}}`. Delete `pub extern crate tinio_config as _config;` from `lib.rs` and `tinio-config` from `Cargo.toml`; add `dotenvy`, `getrandom`, `serde_ignored` and `toml`. Change `Error::Config` to wrap `config::Error`. Update `commands/start/chown.rs` to return `config::Error`. Update every `resolve` import and the `s3`, `telemetry`, `log`, `pipeline` section accesses to the new owners.

- [x] **Step 4: Move the root tests**

Bring the root-parsing tests from the moved schema files into `config/mod.rs`'s test module: unknown keys, defaults, `generated`, round-trip, owner/users cross-section validation, and the document-level `[s3]`/`[scanner]` cases. Include a document that carries capability keys, sweep keys, scanner keys and telemetry in one file.

- [x] **Step 5: Run the tests**

Run: `cargo test -p tinio-cli`
Expected: PASS
Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: PASS

- [x] **Step 6: Report the pending change**

Report the changed files and propose: `refactor(cli): make config the composition root`. Do not stage or commit.

---

### Task 7: Wire `[scanner]` to `ScannerOptions`

**Files:**
- Modify: `crates/tinio-cli/src/commands/start/serving.rs`, `commands/start/serve.rs`

**Interfaces:**
- Produces: `fn scanner_options(section: Option<&config::scanner::Config>, access: AccessMode) -> tinio_fs::ScannerOptions`.
- Produces: `fn scanner_options_for(resolution: &Resolution, access: AccessMode) -> tinio_fs::ScannerOptions`.
- Changes: `ServingBackend::background(&self, stop: watch::Receiver<bool>, access: AccessMode, sweep: sweep::Options, scanner: ScannerOptions) -> BoxFuture<'static, ()>`.

- [x] **Step 1: Add the mapping functions**

In `serving.rs` add `scanner_options` with `enabled: section.is_some()`, `delay: section.map_or(Duration::from_secs(10), |s| Duration::from_secs_f64(s.delay))`, `max_wait: section.map_or(Duration::from_secs(15), |s| s.max_wait)`, `cycle: section.map_or(Duration::from_secs(24 * 3600), |s| s.cycle)`, `access`. Add `scanner_options_for` reading `resolution.config.scanner.as_ref()`.

- [x] **Step 2: Thread the options through**

Add the `scanner: ScannerOptions` parameter to `ServingBackend::background`, its default body, the `FsStorage` impl and the `PgStorage` impl (which ignores it); pass it to `scanner_and_sweeper` and then to `Scanner::new`. In `serve.rs`, compute `scanner_options_for(&resolution, access)` and pass it at the `storage.background(...)` call.

- [x] **Step 3: Add the mapping tests**

Add a unit test for `scanner_options` covering: absent section (`enabled == false`, defaults `10s`/`15s`/`24h`), present default section (`enabled == true`, defaults), and a custom section (`delay = 2.5`, `max_wait = "1m"`, `cycle = "2h"`). Leave the `TINIO_SCANNER` `0`/`1` override to the existing `tinio-fs` `scanner_enabled` tests.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-cli`
Expected: PASS
Run: `cargo test -p tinio-fs scanner`
Expected: PASS

- [x] **Step 5: Report the pending change**

Report the changed files and propose: `feat(cli): wire the scanner section to ScannerOptions`. Do not stage or commit.

---

### Task 8: Update the facade and e2e

**Files:**
- Modify: `crates/tinio/src/lib.rs`, `crates/tinio/src/error.rs`, `crates/tinio/Cargo.toml`
- Modify: `crates/tinio-e2e/Cargo.toml`, `crates/tinio-e2e/tests/cucumber.rs`, `tests/steps/cli.rs`, `tests/steps/clients.rs`, `tests/steps/common.rs`

**Interfaces:**
- Produces: `tinio::Config` and `tinio::ConfigError` as re-exports of `tinio_cli::config::{Config, Error}`.
- Produces: `tinio_e2e`'s `_cli` extern seam over `tinio_cli`.

- [x] **Step 1: Update the facade**

In `crates/tinio/src/lib.rs` remove the `_config` extern and re-export `Config` from `_cli::config`; update the module doc that names `tinio-config`. In `crates/tinio/src/error.rs` re-export `ConfigError` from `_cli::config::Error`. In `Cargo.toml` remove `tinio-config` and add the `config` feature to the `tinio-server` dependency.

- [x] **Step 2: Update the e2e seam**

Add `tinio-cli` to `crates/tinio-e2e/Cargo.toml` dev-dependencies and `pub extern crate tinio_cli as _cli;` to `tests/cucumber.rs`. In `steps/cli.rs`, `clients.rs` and `common.rs`, take `Config`, `discover`, `server`, `storage` and `s3` from `crate::_cli::config`; take `auth`, `owner` and `users` from `crate::_server::identity`; take the env constants from `_util::env`. Remove the temporary direct `tinio-config` dev-dependency that Task 4 added, now that nothing imports it.

- [x] **Step 3: Run the tests**

Run: `cargo test -p tinio`
Expected: PASS
Run: `cargo test -p tinio-e2e --no-run`
Expected: PASS

- [x] **Step 4: Report the pending change**

Report the changed files and propose: `refactor: point the facade and e2e at the cli config root`. Do not stage or commit.

---

### Task 9: Delete `tinio-config` and correct the live docs

**Files:**
- Delete: `crates/tinio-config/` (manifest, README, `src/`)
- Modify: `README.md`, `crates/tinio/README.md`, `crates/tinio-cli/README.md`, `crates/tinio-server/README.md`, `crates/tinio-auth/README.md`, `crates/tinio-fs/README.md`, `crates/tinio-pg/README.md`, `crates/tinio-store-pg/README.md`
- Modify: `crates/tinio-core/src/acl.rs`, `crates/tinio-util/src/path.rs`, `crates/tinio-util/src/switch.rs`, `crates/tinio-sandbox-core/src/launcher/payload.rs`, `crates/tinio-fs/src/backend/mod.rs`, `crates/tinio-pg/src/config.rs`
- Modify: `specs/001-s3-local-server/contracts/config.md`, `specs/001-s3-local-server/plan.md`, `crates/tinio-e2e/tests/traceability.rs`, `crates/tinio-e2e/tests/features/` comments, `docs/superpowers/specs/2026-10-17-config-ownership-redesign.md`

**Interfaces:**
- Removes: the `tinio-config` crate and every live reference that says it owns configuration.

- [x] **Step 1: Delete the crate**

Remove `crates/tinio-config/` entirely. `crates/*` membership drops it automatically.

- [x] **Step 2: Correct the live prose**

Update the root README crate index and every crate README/doc comment listed above to name the new owner instead of `tinio-config`. In `contracts/config.md` correct the implementation pointer and the `tinio_config::state` sentence; leave the key contract itself untouched. In `plan.md` correct the eight-crate sentence and the tree. In `traceability.rs` change T018's home to `crates/tinio-cli/src/config/sources.rs` and update the reason text. Leave historical specs, reviews and completed task records as written.

- [x] **Step 3: Update the design's status**

Change the design doc's `**Status**` from `proposed` to `shipped`, recording that the acceptance evidence is this plan's Task 9 report (the commands and their output). Do not restate the design's decisions there.

- [x] **Step 4: Run the full gate**

Run: `cargo build -p tinio`
Expected: PASS (first: the `@cli` scenarios resolve this binary and build nothing themselves)
Run: `cargo test --workspace`
Expected: PASS
Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS
Run: `RUSTDOCFLAGS="-D warnings" cargo doc --profile ci --workspace --no-deps --all-features`
Expected: PASS
Run: `cargo check -p tinio-server --no-default-features`
Expected: PASS
Run: `cargo +nightly fmt --all -- --check`
Expected: PASS for every file this change touched; report any remaining pre-existing diff rather than reformatting unrelated files
Run: `cargo test -p tinio-e2e --test cucumber`
Expected: PASS under the default filter (`@pg`, external and confinement excluded per `docs/tests.md`)
Run: `cargo test -p tinio-e2e --test traceability`
Expected: PASS

- [x] **Step 5: Report the pending change**

Report the changed files and propose: `refactor: delete tinio-config and correct its live references`. Do not stage or commit.

**Evidence (2026-10-18, working tree; no commits)**: `cargo build -p tinio` PASS; `cargo test --workspace` PASS (0 failed); `cargo clippy --workspace --all-targets -- -D warnings` PASS; `RUSTDOCFLAGS="-D warnings" cargo doc --profile ci --workspace --no-deps --all-features` PASS; `cargo check -p tinio-server --no-default-features` PASS; `cargo +nightly fmt --all -- --check` PASS; `cargo test -p tinio-e2e --test cucumber` PASS (293 scenarios / 2445 steps); `cargo test -p tinio-e2e --test traceability` PASS (19). `crates/tinio-config/` is absent; `tinio::Config`/`ConfigError` re-export the CLI types. The final whole-branch review's one Important finding — flattened `[s3]`/`[api.https]` tables silently accepting unknown keys — was fixed with root tests, and its scoped re-review is clean. The `/simplify` pass re-ran the gate green: workspace tests, clippy, rustdoc, fmt, the no-default check, and `tinio-cli --features pg`.
