# CLI Lifecycle (`tinio server`/`start`, `status`, `stop`, `service`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `tinio <command> [DIR]` becomes the product's entry point — the process sequence of the sandbox design's §10, the clap surface and the four-level precedence chain of FR-016, config discovery with first-start credential generation, the `server`/`start` orchestration (readiness, signals, daemon mode, systemd notifications), `status` and `stop` as thin clients of plan A's management plane, the cross-platform `service` verb set, and the `--sandbox` seam resolved through all three of its channels.

**Architecture:** `run()` stays synchronous and owns the process: payload adoption before any parser sees argv, then clap, then the server's pre-runtime half (resolve, fork, confine), then — only at the end — one multi-threaded runtime, which `server`, `status`, `stop` and `doctor` all run on and none of them builds (decision 2). The §10 order (confine or launch *before* the runtime exists) is what makes `unshare` and a fork-based daemon legal, and it is why this crate, and not a library, owns the sequence. `examples/serve.rs` retires here, and the two harnesses that scraped its stdout move to the `tinio` binary unchanged, because the readiness line stays what it is until P1 replaces it with the event stream.

**Tech Stack:** Rust 2024, `clap` 4.6 (derive + `env`), `tokio` 1, `tinio-api` (plan A), `tinio-server`'s `DataPlane`/`PlaneConfig`/`log::build_subscriber`, `tinio-config` (discovery, credentials, `[server] sandbox`), `signal-hook` + `nix` (unix signals), `windows-sys` (the console handler), `daemonize` (unix daemon mode), `sd-notify` (systemd), `service-manager` 0.11 + `windows-service` 0.8.1 (the `service` verbs and the SCM dispatcher), `tinio-sandbox` (the confinement seam).

**Spec:** `docs/superpowers/specs/2026-09-20-cli-integration-design.md` — §1 (the sequence), §2 (this crate's shape), §4 (the commands), §5 (discovery/credentials), §6 (T143), §7 (the sandbox seam), §8 (the retirement), decisions 1–5, 9–16, 18–22. Plan A (`2026-09-20-cli-management-plane.md`) is its dependency and defines the plane's surface; read both.

## Global Constraints

- **Perform no git write operations whatsoever, and never touch the index** (`CLAUDE.md`): every task closes with a checkpoint step that reports the files pending commit and **does not run** `git add`/`git commit`/`git push`/`git restore`.
- Code, comments, tests, commit messages and documents are always in English. **Documents are not hard-wrapped.**
- **Test-first** (constitution IV): the failing test comes first and its red state is verified.
- No `unwrap()` / `expect()` / `panic!()` in library code; tests may use them (constitution II). Async tests use `#[tokio::test]`; never `Runtime::block_on` in a test, and **no command builds a runtime of its own** — the process creates exactly one (`new_multi_thread`, in `lib.rs`, design decision 2), and `server`, `status`, `stop` and `doctor` all run on it.
- **No test may mutate the process environment.** Everything this plan resolves from the environment (`.env`, `TINIO_*`, `NOTIFY_SOCKET`, `WATCHDOG_USEC`) is tested by **spawning the binary** with the variables set on the child, never by `set_var` in-process.
- Every public item carries a doc comment with at least one runnable rustdoc example; `no_run` where it launches a process or blocks. The exempt classes are constitution III's closed list (`.specify/memory/constitution.md`).
- Style (`docs/style.md`): compressed prose, 3+ segment imports de-qualified (`clippy::absolute_paths`, `-D warnings`), per-crate `Error`, `SmartDefault`, `parse-display`/`derive_more`.
- **The code blocks below are not swept for the imports rule.** Run `cargo clippy -p tinio-cli --all-targets -- -D warnings` as each task's lint step and fix what it reports in place (expect roughly 30 sites across this plan's blocks, mostly `std::`-qualified paths in the daemon, signal and service arms). Do not relax the rule.
- **The plan set's ownership rules**: this plan owns `crates/tinio-cli/**`, `crates/tinio-config/src/{discover,credentials,sources,schema/config,schema/server}.rs`, `crates/tinio-server/examples/serve.rs` (deletion), `crates/tinio-e2e/tests/steps/clients.rs`, `crates/tinio-server/tests/serve_read_only.rs`, `crates/tinio/tests/smoke.rs`, `crates/tinio/tests/lifecycle.rs`, `crates/tinio/Cargo.toml`'s `[dev-dependencies]` and its `sandbox` row, and `packaging/tinio.service`. Plan A owns `crates/tinio-api/**`, `crates/tinio-server/src/data.rs` and **`crates/tinio/Cargo.toml`'s `tinio-api` pass-through rows (`openapi`, `tls`) and that file's `default` list** — one owner per line, so no task here adds or edits an `openapi`/`tls`/`default` row, and plan A's task does not touch the `sandbox` row or the dev-dependencies. No task here edits a plan-A file.
- **The process-level tests live in the facade crate, not in `tinio-cli`.** `env!("CARGO_BIN_EXE_tinio")` is only defined for bins of the package under test, and `tinio` is the facade package's bin: a `crates/tinio-cli/tests/lifecycle.rs` would not compile. So every test that spawns the binary lives in `crates/tinio/tests/lifecycle.rs` (package `tinio`), where the macro is legal and cargo builds the binary first, and every `cargo test` command for it is `cargo test -p tinio --test lifecycle …`. `tinio-cli` keeps only its unit-level tests (`resolve::`, `cli::`, the `#[cfg(unix)]` `signal::` unit, `sandbox::`) and the pure `service.rs` units. Plan C's `tests/doctor.rs` moves the same way and is plan C's edit.
- **Execution**: subagent-driven — one fresh subagent per task, reviewed before the next starts; tick a task's step boxes only after its review is accepted.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `Cargo.toml` (workspace) | pins for `clap`, `daemonize`, `signal-hook`, `nix`, `sd-notify`, `service-manager`, `windows-service`, `windows-sys` | 1, 4–7 |
| `crates/tinio-config/src/discover.rs` (new) | the config path search order and load-or-create | 1 |
| `crates/tinio-config/src/credentials.rs` (new) | the generated pair (≥16 / ≥32 bytes, CSPRNG) | 1 |
| `crates/tinio-config/src/sources.rs` | the variable names and the pure grammar only — `READ_ONLY_VAR`/`SANDBOX_VAR` and `parse_read_only`/`parse_sandbox`: the `env_*` overlays this task added, and the `parse_var`/`from_var` scaffold they went through, were deleted 2026-09-24, and every variable is read by clap's `env` on the CLI's own flags | 1 |
| `crates/tinio-config/src/schema/server.rs` | `[server] sandbox` joins `host`/`port`/`read_only` | 1 |
| `crates/tinio-config/src/schema/config.rs` | `Config::generated` — the auto-created config, the local API channel (`[api.unix]`/`[api.pipe]`) included — beside the public `Config::server`/`Config::auth` fields | 1 |
| `crates/tinio-cli/Cargo.toml` | the dependency arms, the default-on `sandbox` feature, the tokio feature list (`time` and `macros` included), and `unsafe_code` moving from `forbid` to `deny` — **two** enumerated sites by the end (this plan's console handler; plan C's `windows-sys` free-space probe in `commands/doctor/`) | 1, 2, 4–9 |
| `crates/tinio/Cargo.toml` | this plan's rows only: the facade's `sandbox` pass-through row (10) and its `[dev-dependencies]` for the lifecycle tests — plan A owns the `openapi`/`tls` rows and the `default` list | 3, 5, 6, 8, 9, 10 |
| `crates/tinio-cli/src/cli.rs` (new) | the clap types, their `env` attributes (**non-boolean flags only** — the switches' variables are `sources`'), the `Provenance` the chain reads from `ArgMatches`, the global flags | 2 |
| `crates/tinio-cli/src/resolve.rs` (new) | root discovery, mode/state-dir resolution, `.env`, the four-level chain, the `[api]` selection, `for_doctor` | 2 |
| `crates/tinio-cli/src/error.rs` | the existing `Error` + `exit_code()`, plus this plan's variants: `Fs(tinio_fs::Error)`, `Server(tinio_server::Error)`, `Sandbox(tinio_sandbox::Error)` and `Log(String)` (the subscriber's boxed error, named rather than converted) | 2 |
| `crates/tinio-cli/src/lib.rs` | `run()`, the sequence, the dispatch, the module tree | 2, 3, 9 |
| `crates/tinio-cli/src/commands/{mod,start,status,stop}.rs` | the commands and their shared helpers (stubs in 2, real in 3 and 8) | 2, 3, 8 |
| `crates/tinio-cli/src/signal.rs` (new) | signal-hook + nix (unix), windows-sys (Windows), the second-signal exit (stub in 3) | 3, 4 |
| `crates/tinio-cli/src/daemon.rs` (new) | `daemonize` (unix) / the detached child (Windows), the log redirection (stub in 3) | 3, 5 |
| `crates/tinio-cli/src/notify.rs` (new) | `READY=1`/`STOPPING=1`/`WATCHDOG=1`, and the unit text's one author (stub in 3) | 3, 6 |
| `crates/tinio-cli/src/service.rs` (new) | the five verbs, the label derivation, the Windows dispatcher arm (stub in 2) | 2, 7 |
| `crates/tinio-cli/src/sandbox/{mod,linux,windows}.rs` (new) | the three channels, the exposure set, `confine`, the platform options (stub in 2) — `mod.rs` is the shared half and the dispatch, `linux.rs` / `windows.rs` are the platform arms | 2, 9 |
| `crates/tinio/tests/lifecycle.rs` (new) | T055's round trip and every leg that spawns the binary — daemon, notify socket, `status`/`stop`, sandbox refusals, the Windows console-handler leg; package `tinio`, because `CARGO_BIN_EXE_tinio` is only defined for the bin's own package | 3, 5, 6, 8, 9, 10 |
| `crates/tinio-cli/tests/service.rs` (new) | the service verbs' text/label units (pure, no spawn) | 7 |
| `crates/tinio-server/examples/serve.rs` | deleted; its consumers switched | 10 |
| `crates/tinio-e2e/tests/steps/clients.rs`, `crates/tinio-server/tests/serve_read_only.rs`, `crates/tinio/tests/smoke.rs` | the harness switch, credentials included | 10 |
| `.github/actions/run-test-leg/action.yml`, `.github/workflows/ci.yml` | the serve-example build steps become the `tinio` build the harnesses need | 10 |
| `packaging/tinio.service` (new) | the unit, rendered from the one template | 6 |
| `specs/001-s3-local-server/**`, `docs/tests.md`, `README.md`, `CONTEXT.md` | the FR, the contracts, the legs, the ticks — `contracts/config.md`'s socket default and the `--api` presence rule, `contracts/cli.md`'s readiness and `--daemon` sentences, the `service` section and its FR, `research.md`'s dependency rows, `plan.md:186`, `data-model.md`'s state-file list | 3, 5, 7, 9, 11 |

---

### Task 1: Config discovery, credentials, and the `[server] sandbox` key

**Files:**
- Create: `crates/tinio-config/src/discover.rs`, `crates/tinio-config/src/credentials.rs`
- Modify: `crates/tinio-config/src/lib.rs`, `crates/tinio-config/src/sources.rs`, `crates/tinio-config/src/schema/server.rs`, `crates/tinio-config/src/schema/config.rs` (where `Config::generated` lands, beside the public `server`/`auth` fields), `crates/tinio-config/Cargo.toml` (dep `getrandom`, plus `windows-sys` under `[target.'cfg(windows)'.dependencies]`, and `[lints.rust] unsafe_code` moves from `forbid` to `deny` for the enumerated ACL block), `Cargo.toml` (workspace: the `windows-sys` pin)

**Interfaces:**
- Consumes: `Config` (`schema::config`), `state::{home_state_dir, env_state_dir}`.
- Produces: `tinio_config::discover::{resolve_path, load_or_create, Outcome}` with `pub fn resolve_path(root: &Path, state_dir: &Path, explicit: Option<&Path>) -> Result<PathBuf, Error>` and `pub fn load_or_create(root: &Path, path: &Path, read_only: bool) -> Result<(Config, Outcome), Error>` where `enum Outcome { Loaded, Created }`; `tinio_config::credentials::{Credentials, generate}`; `tinio_config::sources::{READ_ONLY_VAR, SANDBOX_VAR, parse_read_only, parse_sandbox}` (the `env_sandbox`/`env_credentials` readers this task produced were deleted 2026-09-24; clap's `env` on the CLI's flags is the reader); `Config::generated(read_only, credentials) -> Result<Config, Error>`; and the existing public field `Config::server.sandbox: bool` (`Config::server` is a field, not a method — `schema/config.rs:76`; its own `host`/`port`/`read_only` are public, and `schema/server.rs`'s `host()`/`port()` at `:33`/`:37` are private serde-default fns, not accessors).

Two rules this task's signatures carry, and both are the reason for their shape: **auto-create belongs to discovery, never to the operator** — the create branch has exactly one reachable input, discovery's fallback `<writable state dir>/.tinio.toml`, because the CLI refuses a `--config` naming a missing file with exit 2 before discovery runs (Task 2's `resolve::config_path`); and **read-only mode never writes under the served root**, so `load_or_create` needs the root to refuse a path that lies inside it.

The search order `resolve_path` implements is design decision 5's, first hit wins and in this exact order: `<root>/.tinio/.tinio.toml` → `<root>/.tinio/config.toml` → the state dir's own `.tinio.toml` → the state dir's `config.toml` → auto-create at `<state dir>/.tinio.toml`. The third and fourth steps are what make read-only mode coherent rather than special-cased: the state dir *is* `<root>/.tinio` in read-write mode, so the four steps collapse to two there, and in read-only mode they are the relocated `<home>/.tinio/roots/<hash>/`, which is where an earlier read-only run auto-created the file. The tests below seed all four positions and assert the winner of each pair — the primary name beats the alias in every directory, and a root-side file beats a state-dir one.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-config/src/discover.rs`:

```rust
#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;
    use tempfile::TempDir;

    use super::*;

    /// Seeds `<dir>/<rel>`, parents included, and returns the path.
    fn seeded(dir: &Path, rel: &str) -> PathBuf {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("parent");
        }
        std::fs::write(&path, "version = 1\n").expect("seed");
        path
    }

    #[test]
    fn the_root_config_wins_over_the_state_dirs() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        // Decision 5's order, first hit wins: `<root>/.tinio/.tinio.toml` leads.
        let primary = seeded(root.path(), ".tinio/.tinio.toml");
        let _alias = seeded(root.path(), ".tinio/config.toml");
        let _state_copy = seeded(state.path(), ".tinio.toml");

        assert_eq!(resolve_path(root.path(), state.path(), None).expect("resolve"), primary);
    }

    #[test]
    fn the_alias_is_read_and_the_primary_name_beats_it() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        // Only the alias is there: it answers, in the root and in the state dir alike.
        let root_alias = seeded(root.path(), ".tinio/config.toml");
        assert_eq!(resolve_path(root.path(), state.path(), None).expect("resolve"), root_alias);

        std::fs::remove_file(&root_alias).expect("unseed");
        let state_alias = seeded(state.path(), "config.toml");
        assert_eq!(resolve_path(root.path(), state.path(), None).expect("resolve"), state_alias);

        // And within one directory the primary name wins over the alias.
        let state_primary = seeded(state.path(), ".tinio.toml");
        assert_eq!(resolve_path(root.path(), state.path(), None).expect("resolve"), state_primary);
    }

    #[test]
    fn the_state_dir_is_the_fallback() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let state_copy = seeded(state.path(), ".tinio.toml");
        assert_eq!(resolve_path(root.path(), state.path(), None).expect("resolve"), state_copy);
    }

    #[test]
    fn an_explicit_path_is_used_verbatim() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let explicit = seeded(root.path(), "elsewhere.toml");
        assert_eq!(
            resolve_path(root.path(), state.path(), Some(&explicit)).expect("resolve"),
            explicit
        );
    }

    #[test]
    fn a_first_start_creates_a_config_with_credentials_and_the_local_api_channel() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        let (config, outcome) = load_or_create(root.path(), &path, false).expect("create");

        assert!(matches!(outcome, Outcome::Created));
        assert!(path.exists());
        let auth = config.auth.as_ref().expect("generated credentials");
        assert!(auth.access_key.len() >= 16);
        assert!(auth.secret_key.expose_secret().len() >= 32);
        assert_eq!(config.server.port, 9000);
        assert!(!config.server.read_only);

        // The local management channel is written too, and it is written *explicitly*:
        // Task 2's `api_options` would enable the derived channel for a config with no
        // `[api]` section at all, so the section is not what makes `status` possible —
        // it is what makes the channel visible and editable in the file an operator
        // reads, with the derived path spelled out rather than implied.
        let text = std::fs::read_to_string(&path).expect("read back");
        let local = if cfg!(windows) { "[api.pipe]" } else { "[api.unix]" };
        assert!(text.contains(local), "the local channel is part of the created config:\n{text}");

        // The second call reads what the first wrote rather than generating again.
        let (again, outcome) = load_or_create(root.path(), &path, false).expect("load");
        assert!(matches!(outcome, Outcome::Loaded));
        assert_eq!(
            again.auth.as_ref().expect("credentials").access_key,
            auth.access_key
        );
    }

    #[test]
    fn a_read_only_root_creates_its_config_in_the_state_dir() {
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        let (config, outcome) = load_or_create(root.path(), &path, true).expect("create");
        assert!(matches!(outcome, Outcome::Created));
        assert!(path.starts_with(state.path()), "read-only never writes the served root");
        assert!(config.server.read_only, "the mode is recorded, so the next start keeps it");
    }

    #[test]
    fn a_read_only_run_refuses_to_create_under_the_served_root() {
        // `TINIO_STATE_DIR` inside the root is the reachable case: read-only mode relocated
        // everything, so a state dir under the root is a contradiction, and the guard makes
        // it a refusal rather than a write into the root.
        let root = TempDir::new().expect("root");
        let inside = root.path().join(".tinio");
        let err = load_or_create(root.path(), &inside.join(CONFIG_FILE), true)
            .expect_err("read-only never writes the root");
        assert!(format!("{err}").contains("read-only"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn the_created_config_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        load_or_create(root.path(), &path, false).expect("create");
        let mode = std::fs::metadata(&path).expect("metadata").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "it holds the credentials");
    }

    #[cfg(windows)]
    #[test]
    fn the_created_config_is_private_on_windows() {
        // The DACL's read-back lives beside its writer in `discover.rs` (`pub(crate) fn
        // acl_entries`, the same enumerated `unsafe` block): a second account's absence
        // cannot be tested from a single-account runner, so what is asserted is the shape —
        // an explicit DACL with one entry, the current user's, and no inherited rows.
        let root = TempDir::new().expect("root");
        let state = TempDir::new().expect("state");
        let path = resolve_path(root.path(), state.path(), None).expect("resolve");
        load_or_create(root.path(), &path, false).expect("create");
        let entries = acl_entries(&path).expect("DACL");
        assert_eq!(entries.len(), 1, "only the current user: {entries:?}");
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-config discover::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

`crates/tinio-config/src/discover.rs`:

```rust
//! Finding the configuration file, and creating it on a first start (FR-007, `config.md:9,120`).
//!
//! The search order is design decision 5. Its last two steps are what make read-only mode
//! ordinary: the state directory *is* `<root>/.tinio` in read-write mode, and in read-only
//! mode it is the relocated home directory, which is exactly where an earlier read-only run
//! auto-created the file.

use std::path::{Path, PathBuf};

use crate::{Config, Error, credentials, error};

/// The primary file's name inside a state directory.
pub const CONFIG_FILE: &str = ".tinio.toml";

/// The alias accepted when the primary name is absent.
pub const CONFIG_ALIAS: &str = "config.toml";

/// Whether the file was already there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// An existing file was parsed.
    Loaded,
    /// A new file was written with generated credentials.
    Created,
}

/// The configuration file for `root`, searched in decision 5's order.
///
/// An explicit path is returned verbatim and must already be a file: the caller refuses a
/// `--config` that names a missing one with exit 2 (Task 2's `resolve::config_path`), which is
/// what keeps the create branch below to discovery's single input.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_config::discover::resolve_path;
///
/// let path = resolve_path(Path::new("/srv/data"), Path::new("/srv/data/.tinio"), None)?;
/// # Ok::<(), tinio_config::Error>(())
/// ```
pub fn resolve_path(
    root: &Path,
    state_dir: &Path,
    explicit: Option<&Path>,
) -> Result<PathBuf, Error> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }
    for directory in [root.join(".tinio"), state_dir.to_path_buf()] {
        for name in [CONFIG_FILE, CONFIG_ALIAS] {
            let candidate = directory.join(name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    Ok(state_dir.join(CONFIG_FILE))
}

/// Reads `path`, or writes it with generated credentials when it is absent.
///
/// Auto-create is discovery's, and its one reachable input is `<state_dir>.tinio.toml`: an
/// operator-named path never arrives absent, and read-only mode refuses a path under `root`
/// outright, because FR-023's contract is that nothing under the served root is ever written.
pub fn load_or_create(root: &Path, path: &Path, read_only: bool) -> Result<(Config, Outcome), Error> {
    if path.is_file() {
        return Ok((Config::load(path)?, Outcome::Loaded));
    }
    if read_only && path.starts_with(root) {
        return Err(Error::InvalidValue {
            key: "server.read_only".into(),
            reason: format!(
                "read-only mode never writes under the served root, and {} is inside {}",
                path.display(),
                root.display()
            ),
        });
    }
    let config = Config::generated(read_only, credentials::generate()?)?;
    let text = config.to_toml()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| error::io(parent, err))?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|err| error::io(path, err))?;
    std::io::Write::write_all(&mut file, text.as_bytes())
        .map_err(|err| error::io(path, err))?;
    #[cfg(windows)]
    restrict_to_current_user(path)?;
    Ok((config, Outcome::Created))
}

/// Restricts `path` to the current user: a fresh DACL with one allow-ACE and no inheritance.
///
/// `std::fs` cannot express a security descriptor at creation, so this runs after the create
/// and before the file is announced: `SetEntriesInAclW` builds the ACL from one
/// `EXPLICIT_ACCESS_W` for the token user's SID, and `SetNamedSecurityInfoW` applies it with
/// `DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION` (protected = the parent's
/// inheritable ACEs are not copied in). The same protection as the unix `0o600` arm: the file
/// holds the credentials (design decision 15), and `tinio-api`'s `state` file takes its own
/// copy of this pattern in plan A.
///
/// This is `tinio-config`'s **one** enumerated exception — the crate moves from
/// `unsafe_code = "forbid"` to `"deny"`, and `#[allow(unsafe_code)]` appears on this block and
/// on the read-back below it and nowhere else — because `windows-sys` is raw bindings. It is
/// the pattern the sandbox design's decision 14 established and `tinio-cli`'s `signal.rs`
/// (Task 4) reuses. The block allocates and calls, and holds no invariant beyond the SID it
/// read from the process token; a failure here is the create's failure, never a silently
/// unprotected file.
#[cfg(windows)]
#[allow(unsafe_code)]
fn restrict_to_current_user(path: &Path) -> Result<(), Error> { /* windows-sys */ }

/// The read half, in the same enumerated block: the explicit ACE list `GetNamedSecurityInfoW`
/// reports, one string per row. It exists for the Windows test above and for nothing else.
#[cfg(windows)]
#[allow(unsafe_code)]
pub(crate) fn acl_entries(path: &Path) -> Result<Vec<String>, Error> { /* windows-sys */ }
```

`crates/tinio-config/src/credentials.rs`:

```rust
//! Generated credentials for a first start (`config.md:141`, FR-007).

use crate::Error;

/// The generated pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    /// At least 16 bytes of CSPRNG output, hex-encoded.
    pub access_key: String,
    /// At least 32 bytes, hex-encoded.
    pub secret_key: String,
}

/// The contract's floors, in bytes before hex encoding.
const ACCESS_BYTES: usize = 16;
const SECRET_BYTES: usize = 32;

/// Generates a pair from the CSPRNG.
pub fn generate() -> Result<Credentials, Error> {
    Ok(Credentials {
        access_key: hex(ACCESS_BYTES)?,
        secret_key: hex(SECRET_BYTES)?,
    })
}

fn hex(bytes: usize) -> Result<String, Error> {
    let mut buffer = vec![0u8; bytes];
    // There is no `Error::Utf8` variant in this crate: a CSPRNG failure is an
    // "environment fact the configuration cannot proceed without", which is
    // `Error::Missing`'s own doc — so it maps there rather than to a new variant
    // invented for one call site.
    getrandom::fill(&mut buffer).map_err(|err| {
        Error::Missing(format!("generated credentials: the CSPRNG is unavailable: {err}"))
    })?;
    Ok(hex::encode(buffer))
}
```

**The names this file and `discover.rs` must call are the ones that exist, not plausible ones.** The I/O mappings go through `error::io(path, err)` — the crate's `pub(crate)` constructor — because `Error::Io` is a **struct** variant (`{ path, source }`, `error.rs:30`), so `.map_err(Error::Io)` never compiled; and the CSPRNG failure is `Error::Missing` (see above), because the crate has no `Error::Utf8`.

`Config::generated(read_only, credentials) -> Result<Config, Error>` is an addition to `schema/config.rs`; `generated` fills `version`, `[auth]`, `[server]` (`host` `127.0.0.1`, `port` 9000, `read_only`), the **local management channel**, `[scanner]` and `[storage.fs]`, and leaves every defaulted key out — which is what `contracts/config.md:11-106`'s example documents. There is no `Config::auth()` to add beside it: `auth` is the existing public field `Option<auth::Config>` (`schema/config.rs:84`) and its own `access_key`/`secret_key` are public, so callers read `config.auth.as_ref()`.

The local channel is written by the created config: `[api.unix]` under `#[cfg(unix)]` and `[api.pipe]` under `#[cfg(windows)]`, each with an empty `path` (the derived default, decision 24). **It is written to be read and edited, not because its absence would leave the plane transportless**: Task 2's `api_options` enables the derived local channel when the config carries **no `[api]` section at all** (decision 7's rule that the local channel is the tool's default, and what keeps a hand-written `[s3]`-only file — the interop harness's own fixture — a working server), so a first start works either way. Recording the section makes the channel visible in the file an operator opens and lets `path` be edited in place; the derived name is `state::socket_path`'s either way. The section is the platform-appropriate one and never both: `contracts/config.md` rejects the wrong-platform section at parse.

`schema/server.rs` gains one field beside `read_only`:

```rust
    /// Process confinement (`--sandbox`, `TINIO_SANDBOX`; FR-0xx, design decision 10).
    pub sandbox: bool,
```

`sources.rs` gains two matching overlays, written exactly like the existing `env_read_only` — through the **`pub(crate)` `parse_var<T>` helper** (`sources.rs:87`), which is the crate's one scaffold for "unset / set / not-Unicode" and is what `env_read_only` itself calls; there is no `env_bool`. The first is the confinement channel:

```rust
/// The confinement variable's name.
pub const SANDBOX_VAR: &str = "TINIO_SANDBOX";

/// Parses `TINIO_SANDBOX` (`1`/`true`/`0`/`false`), `None` when unset.
pub fn env_sandbox() -> Result<Option<bool>, Error> {
    parse_var(SANDBOX_VAR, |raw| parse_switch(SANDBOX_VAR, raw))
}

/// A boolean switch's spelling rules, naming `var` in the refusal.
fn parse_switch(var: &str, raw: &str) -> Result<bool, Error> {
    match raw.trim() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(Error::invalid_value(
            var,
            format!("expected 0/1/true/false, got `{other}`"),
        )),
    }
}
```

The second is the credentials overlay FR-016 needs and nothing consumes yet — the keys `contracts/config.md:110` lists as `TINIO_ACCESS_KEY` and `TINIO_SECRET_KEY`:

```rust
/// The credential variables' names (`contracts/config.md:110`), the `[auth]` pair.
pub const ACCESS_KEY_VAR: &str = "TINIO_ACCESS_KEY";
pub const SECRET_KEY_VAR: &str = "TINIO_SECRET_KEY";

/// The `[auth]` overlay's two values: `None` when neither variable is set,
/// `Some((access, secret))` when both are, and a refusal when only one is —
/// half a credential pair is a configuration failure, never a run that keeps
/// the file's secret key and silently drops the operator's access key.
pub fn env_credentials() -> Result<Option<(String, String)>, Error> {
    let access = parse_var(ACCESS_KEY_VAR, |raw| Ok(raw.to_string()))?;
    let secret = parse_var(SECRET_KEY_VAR, |raw| Ok(raw.to_string()))?;
    match (access, secret) {
        (None, None) => Ok(None),
        (Some(access_key), Some(secret_key)) => Ok(Some((access_key, secret_key))),
        (access, secret) => Err(Error::invalid_value(
            if access.is_none() { ACCESS_KEY_VAR } else { SECRET_KEY_VAR },
            format!(
                "both {} and {} must be set together, or neither: on its own, the \
                 pair's other key silently falls back to the config file",
                ACCESS_KEY_VAR, SECRET_KEY_VAR
            ),
        )),
    }
}
```

Credentials are **not** clap arguments — no flag carries them — so this overlay is the whole of level 2 and 3 for `[auth]`, and Task 2's resolution applies it: `env` → `.env` → config, with the process environment winning over `.env` because `dotenvy` (level 3) never overrides an already-set variable.

**The `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` (and `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`) names are the data plane's fallback, not this chain's**, and this overlay reads neither: `contracts/config.md:110` documents them as the legacy/modern aliases the *data plane's* credential resolution accepts, `crates/tinio-config/src/sources.rs`'s module doc already parks them with T084, and folding them into the CLI's chain would give one environment variable two owners and two precedence positions. When T084 lands they land in the data plane's own resolution (§US3), where the config reference puts them.

*(2026-09-24: the two overlays and the `parse_var`/`from_var` scaffold above were deleted — `tinio_config::sources` keeps the variable names and the pure `parse_*` grammar, and every variable is read by clap's `env` on the CLI's flags (`crates/tinio-cli/src/cli.rs`'s `READ_ONLY_VAR`/`SANDBOX_VAR` beside `crates/tinio-cli/src/commands/start/args.rs`'s `value_parser`). The two blocks above, and the `from_var` leg Step 4 expects, are this step's record.)*

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-config`
Expected: PASS — the new tests plus every existing one (`[server] sandbox` defaults to `false`, so the schema change is additive; a test that constructs `tinio_config::server::Config` exhaustively needs `..Default::default()`, which is this task's edit). The two overlays' own tests follow the pattern `sources.rs` already carries: `from_var` is driven directly with the three lookup outcomes, because `env::set_var` is `unsafe` under edition 2024 and the crate is `unsafe_code = "forbid"` (`sources.rs:94-113`'s comment is the precedent), and `parse_switch` is asserted for `1`/`true`/`0`/`false`, for a rejected spelling, and for both halves of the credential pair (set together, one alone, neither).

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-config --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 2: The clap surface, root discovery, and the four-level chain

**Files:**
- Create: `crates/tinio-cli/src/cli.rs`, `crates/tinio-cli/src/resolve.rs`, and the stubs Step 3 lists (`crates/tinio-cli/src/commands/{mod,start,status,stop,doctor}.rs`, `crates/tinio-cli/src/service.rs`, `crates/tinio-cli/src/sandbox/{mod,linux,windows}.rs`)
- Modify: `crates/tinio-cli/src/lib.rs`, `crates/tinio-cli/src/error.rs` (the four variants the table above names — `docs/superpowers/specs/2026-09-20-cli-integration-design.md` §2 already gives this file "the variants the api plane and the sandbox surface need"), `crates/tinio-cli/Cargo.toml` (dep `clap` with the `env` feature, the crate's `tokio` features — `rt-multi-thread`, `net`, `fs`, `io-util`, **`time`** (the `stop` poll, `tokio::time::timeout`, the watchdog ticker) and **`macros`** (`#[tokio::test]`), none of which the design's §2 list spells out — and the crate's first `[dev-dependencies]` entry: `tempfile`, which `resolve.rs`'s tests build their `TempDir`s with)

**Interfaces:**
- Consumes: `tinio_config::discover`, `tinio_config::sources`, plan A's `tinio_api`, `tinio_fs::FsOptions` and `tinio_server::pipeline::Pipelines` (both for the `fs_options` mapping below — the crate already depends on `tinio-fs` and `tinio-server`).
- Produces: `tinio_cli::cli::{Cli, Command, ServerArgs, DirArgs, DoctorArgs, ApiFlags, Provenance, provenance}` (the last two are how FR-016's levels 1 and 2 are told apart — see the `ArgMatches` rule below); `tinio_cli::resolve::{Resolution, resolve_root, resolve_mode, resolve_state_dir, state_dir_candidates, config_path, apply_env_file, server, for_doctor, api_options}` with `pub struct Resolution { pub root: PathBuf, pub config: Config, pub config_path: PathBuf, pub read_only: bool, pub state_dir: PathBuf, pub config_error: Option<String> }`, `Resolution::address(&self) -> Result<SocketAddr, Error>` (the resolved bind address, read from `[server]` by **field**: `config.server.host` and `config.server.port` — the public fields at `crates/tinio-config/src/schema/server.rs:22,26`, whose `host()`/`port()` at `:33`/`:37` are private serde-default fns, not accessors, and `config.server` itself is a field of `Config`, not a method), and `pub fn log_file(resolution: &Resolution, args: &ServerArgs, provenance: &Provenance) -> Result<PathBuf, Error>` (the resolved operational log file, `--log-file` → `[log] server_log_file` → the format-derived default in the state dir; Task 5's daemon redirects to it). **`config_error: Option<String>` is the load outcome beside the parsed config, and plan C requires it by that name** (`2026-09-20-cli-doctor.md:308`, consumed in its Task 3): `None` for a config that loaded, `Some(message)` when `Config::load` failed — in which case `config` is `Config::default()` and the message is what the operator is shown. It exists because "the configuration does not parse" is one of `doctor`'s findings, and a `Resolution` that could only be built for a *valid* config would make that check unreachable; **`resolve::server` propagates the failure (a server must not run on a config it could not read) while `for_doctor` records it**, which is the one place the two chains differ in error handling rather than in inputs. `pub fn server(args: &ServerArgs, provenance: &Provenance) -> Result<Resolution, Error>` (the server's chain, Task 3's entry point, failing on an unreadable config); `pub fn for_doctor(args: &DoctorArgs) -> Result<Resolution, Error>` (the same chain minus the provenance — a client command carries no env-backed flag, which is why plan C's call is unchanged — and minus the config failure, which lands in `config_error` instead of propagating) `pub fn api_options(resolution: &Resolution, args: &ServerArgs) -> Result<Selection, Error>` (the `[api]` selection, Task 3's `Plane::bind` input; `#[cfg(feature = "api")]`, because `Selection` is plan A's type and `tinio-api` is an optional dependency), and `pub fn fs_options(resolution: &Resolution, pipelines: Option<&Pipelines>) -> FsOptions` — **the one `Resolution` → `FsOptions` mapping in this CLI**, so a resolved root becomes an `FsStorage` by the same code whichever command asks: `server` passes `Some(&pipelines)` (Task 3's serving half), plan C's `doctor` passes `None`, because an offline command builds no pipelines and the configured `[pipeline.*]` values apply as they stand. A second mapping anywhere is a defect this plan's ownership check is looking for.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-cli/src/resolve.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn an_explicit_directory_is_the_root() {
        let dir = tempfile::TempDir::new().expect("temp");
        let root = resolve_root(Some(dir.path()), dir.path()).expect("root");
        assert_eq!(root, dir.path().canonicalize().expect("canonical"));
    }

    #[test]
    fn a_missing_directory_and_no_ancestor_is_a_usage_error() {
        let dir = tempfile::TempDir::new().expect("temp");
        let err = resolve_root(None, dir.path()).expect_err("no root anywhere");
        assert!(matches!(err, crate::Error::Usage(_)), "exit 2, not exit 1: {err}");
        assert!(format!("{err}").contains("DIR"), "names the remedy: {err}");
    }

    #[test]
    fn the_walk_up_finds_the_nearest_ancestor_with_a_reserved_directory() {
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("root");
        let nested = root.join("a/b/c");
        std::fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
        std::fs::create_dir_all(&nested).expect("nested");
        assert_eq!(resolve_root(None, &nested).expect("root"), root.canonicalize().expect("c"));
    }

    #[test]
    fn the_four_levels_resolve_in_order() {
        // The chain's first two levels are clap's: a flag beats the process environment,
        // and `ValueSource` (read off the `ArgMatches` the parse hands back — see
        // `cli::provenance`) says which of the two produced the value. `.env` and the config
        // file are the levels below, and `pick` is the one place they are read in order —
        // so this test drives all four cases directly, with no parser in the way.
        assert_eq!(pick(ValueSource::CommandLine, Some(true), Some(false), Some(false)), Some(true), "the flag");
        assert_eq!(pick(ValueSource::EnvVariable, Some(true), Some(false), Some(false)), Some(true), "the process environment");
        assert_eq!(pick(ValueSource::DefaultValue, Some(false), Some(true), Some(false)), Some(true), "`.env` is level 3");
        assert_eq!(pick(ValueSource::DefaultValue, Some(false), None, Some(true)), Some(true), "the config file is level 4");
        // Nothing anywhere: clap's own default stands — `None` for a key that has none.
        assert_eq!(pick(ValueSource::DefaultValue, Some(false), None, None), Some(false));
        assert_eq!(pick(ValueSource::DefaultValue, None, None, None), None);
    }

    #[test]
    fn a_config_path_that_does_not_exist_is_a_usage_error() {
        // `--config` names a file that must be there; auto-create is discovery's behaviour,
        // and a mistyped path must not silently become a new config in a new place.
        let dir = tempfile::TempDir::new().expect("temp");
        let missing = dir.path().join("root/elsewhere.toml");
        let err = config_path(dir.path(), dir.path(), Some(&missing)).expect_err("missing file");
        assert!(matches!(err, crate::Error::Usage(_)), "exit 2, not exit 1: {err}");
        assert!(format!("{err}").contains("elsewhere.toml"), "names the path: {err}");
    }

    #[test]
    fn the_candidates_lead_with_the_state_dir_the_chain_resolved() {
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("root");
        let relocated = dir.path().join("home/.tinio/roots/h");

        // Two inputs arrive as parameters rather than being read here, and both have a
        // reason. `override_dir` is `TINIO_STATE_DIR` as the *caller* read it — this function
        // never touches the environment, because the crate's tests may not (`set_var` is
        // unsafe under edition 2024 and a unit test mutating one process-global table across
        // parallel threads is the rule's whole point). `derived` is the home-derived
        // candidate, *lazily*: the real one is `default_home_state_dir`, which **creates**
        // `<home>/.tinio/roots` while it computes (`crates/tinio-config/src/state.rs:37-42`),
        // so a unit test that derived it would seed the developer's real home — the rule
        // `home_state_dir`'s own doc states ("home is an explicit parameter, never
        // `dirs::home_dir()` inline: tests must resolve a scratch home instead of the real
        // one"). Laziness is load-bearing rather than tidy: when the override answers, the
        // derived closure is never called, which is what keeps a test on Windows out of
        // `%USERPROFILE%`.

        // Read-only: the resolved dir is the relocated one and the other candidate is the
        // root's own `.tinio` — which is the case the fallback exists for. Nothing derives a
        // home here at all: the other candidate is a join.
        let derived_ran = std::cell::Cell::new(false);
        let candidates = state_dir_candidates(&root, &relocated, true, None, || {
            derived_ran.set(true);
            Ok(dir.path().join("never"))
        })
        .expect("candidates");
        assert_eq!(candidates, vec![relocated, root.join(".tinio")]);
        assert!(!derived_ran.get(), "a read-only pair derives nothing");

        // Read-write: the root's `.tinio` leads, and the other candidate is the derived
        // home dir of the same root.
        let candidates =
            state_dir_candidates(&root, &root.join(".tinio"), false, None, || Ok(relocated.clone()))
                .expect("candidates");
        assert_eq!(candidates[0], root.join(".tinio"));
        assert_eq!(candidates.len(), 2, "two candidates, never more");
        assert_ne!(candidates[0], candidates[1]);

        // Read-write with the variable set: it replaces the **home-derived** candidate and
        // nothing derives.
        let derived_ran = std::cell::Cell::new(false);
        let candidates = state_dir_candidates(
            &root,
            &root.join(".tinio"),
            false,
            Some(relocated.clone()),
            || {
                derived_ran.set(true);
                Ok(dir.path().join("never"))
            },
        )
        .expect("candidates");
        assert_eq!(candidates, vec![root.join(".tinio"), relocated.clone()]);
        assert!(!derived_ran.get(), "an exported TINIO_STATE_DIR means no home is derived");

        // Read-only with the variable set: the resolved dir already *is* the variable, and the
        // pair must not collapse onto it — `<root>/.tinio` is what a reader probes second, or
        // the read-write server a read-only client is looking for is unreachable.
        let candidates =
            state_dir_candidates(&root, &relocated, true, Some(relocated.clone()), || {
                Ok(dir.path().join("never"))
            })
            .expect("candidates");
        assert_eq!(candidates, vec![relocated.clone(), root.join(".tinio")]);
        assert_ne!(candidates[0], candidates[1], "the pair never collapses onto one directory");
    }

    #[test]
    fn provenance_reads_the_parsers_own_answer() {
        // The one leg that proves the matches are kept: a flag reaches `CommandLine`, and a
        // key nothing supplied reaches `DefaultValue` with clap's own default as the value.
        let argv = ["tinio", "server", "/srv/data", "--port", "9001"];
        let matches = super::cli_matches(argv).expect("parse");
        let provenance = crate::cli::provenance(&matches);
        assert_eq!(provenance.port, ValueSource::CommandLine, "--port was typed");
        assert_eq!(pick(provenance.port, matches.get_one::<u16>("port").copied(), None, Some(9000)), Some(9001));

        let matches = super::cli_matches(["tinio", "server", "/srv/data"]).expect("parse");
        let provenance = crate::cli::provenance(&matches);
        assert_eq!(provenance.port, ValueSource::DefaultValue, "nothing supplied a port");
        assert_eq!(pick(provenance.port, matches.get_one::<u16>("port").copied(), None, Some(9000)), Some(9000));

        // The `EnvVariable` arm cannot be driven from here: it needs `TINIO_PORT` in the
        // process environment, and no test in this workspace may set one (a `set_var` in a
        // test is unsafe under edition 2024 and races every sibling test). It is covered where
        // the variable can be set on a *child* — Task 3's `spawn_with_env` leg, which starts
        // the server with `TINIO_PORT=0` and no `--port` flag and asserts the readiness line
        // reports a bound port, i.e. that level 2 reached the same flag.
    }

    #[test]
    fn doctor_records_a_config_that_does_not_parse() {
        // The field plan C consumes (`config_error`) and the reason it is not a `Result`:
        // `doctor` exists for exactly this root, and a `for_doctor` that propagated the
        // failure would put the "configuration valid" check out of reach.
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("root");
        std::fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
        std::fs::write(root.join(".tinio/.tinio.toml"), "version = \"not a number\"\n").expect("seed");

        let args = crate::cli::DoctorArgs::for_test(&root);
        let resolution = for_doctor(&args).expect("a broken config is a finding, not an error");
        assert!(resolution.config_error.is_some(), "the failure is recorded: {resolution:?}");
        assert_eq!(resolution.config.server.port, 9000, "the parsed config falls back to the default");

        // And the server's chain is the other half of the rule: the same root fails there.
        let server_args = crate::cli::ServerArgs::for_test(&root);
        let provenance = Provenance { port: ValueSource::DefaultValue, ..Default::default() };
        assert!(server(&server_args, &provenance).is_err(), "a server must not run on a config it could not read");
    }

    #[test]
    fn the_log_file_follows_flag_config_then_format() {
        // The one ordering of `--log-file`, `[log] server_log_file` and the format-derived
        // default (Task 5's daemon redirects to this answer and declares no rule of its own).
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).expect("root");

        // The default: no flag, no `[log]` section — and it lands in the state dir, because a
        // daemon has no console to write to (an empty key means "stderr" for a foreground run).
        let resolution = resolution_with(&root, "version = 1\n", &[]);
        let default = log_file(&resolution, &args_for(&root), &Provenance::default()).expect("log file");
        assert!(default.starts_with(&resolution.state_dir), "{default:?}");
        assert!(default.ends_with("server.log"), "text is the default format: {default:?}");
        assert!(
            log_file(&resolution, &args_for_log_format(&root, Format::Json), &Provenance::default())
                .expect("json")
                .ends_with("server.json"),
            "the JSON format's default is `server.json`"
        );

        // The config key answers when the flag does not, and the flag answers over it.
        let resolution = resolution_with(&root, "version = 1\n[log]\nserver_log_file = \"/var/log/tinio.log\"\n", &[]);
        assert_eq!(log_file(&resolution, &args_for(&root), &Provenance::default()).expect("config"), PathBuf::from("/var/log/tinio.log"));
        assert_eq!(
            log_file(&resolution, &args_for_log_file(&root, "/tmp/asked.log"), &Provenance::default()).expect("flag"),
            PathBuf::from("/tmp/asked.log"),
            "the flag wins over the key"
        );
    }

    /// `api` only: `api_options` returns plan A's `Selection`, and `tinio-api` is an optional
    /// dependency of this crate, so this leg does not exist in a feature-off build.
    #[cfg(feature = "api")]
    #[test]
    fn api_options_resolves_the_local_channel() {
        // Four assertions on the returned `Selection`: a relative `[api.unix] path` joins the
        // state dir, an absolute one is verbatim, an empty one is the derived
        // `state::socket_path`, and a config with **no `[api]` section at all** is that same
        // derived answer (Task 10's harness fixtures depend on the last one).
        let dir = tempfile::TempDir::new().expect("temp");
        let state = dir.path().join("state");
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).expect("root");

        let selection = api_options(&resolution_with(&root, &state, "[api.unix]\npath = \"ctrl.sock\"\n"), &args_for(&root)).expect("selection");
        assert_eq!(selection.local, Some(LocalForm::Unix(state.join("ctrl.sock"))));

        let absolute = dir.path().join("elsewhere/ctrl.sock");
        let selection = api_options(&resolution_with(&root, &state, &format!("[api.unix]\npath = \"{}\"\n", absolute.display())), &args_for(&root)).expect("selection");
        assert_eq!(selection.local, Some(LocalForm::Unix(absolute)));

        let derived = tinio_api::state::socket_path(&state, &root).expect("derived");
        let selection = api_options(&resolution_with(&root, &state, "[api.unix]\n"), &args_for(&root)).expect("selection");
        assert_eq!(selection.local, Some(LocalForm::Unix(derived.clone())));

        let selection = api_options(&resolution_with(&root, &state, ""), &args_for(&root)).expect("selection");
        assert_eq!(selection.local, Some(LocalForm::Unix(derived)), "no [api] section at all");

        // The `http`/`https` arms come from `--api`-shaped inputs in the second selection;
        // exclusivity is `select`'s and is asserted in plan A, not here.
    }
}
```

**The three legs above are what the prose promises** (`for_doctor`, `provenance`, `api_options`), and two of them need small test-only helpers the module owns: `cli_matches(argv) -> Result<ArgMatches, Error>` (the two-step parse, so a unit test reads the same provenance `try_run` does) and `DoctorArgs::for_test`/`ServerArgs::for_test`/`resolution_with`/`args_for` (`#[cfg(test)]` constructors, the shape plan C's `test_support` uses). They exist so the legs drive the real functions rather than re-spelling the chain — and `for_doctor`'s leg doubles as the `config_error` leg, which is the field plan C's Task 3 consumes.

`pick(source, parsed, dot_env, file)` is the pure helper this task extracts from the resolution (`resolve::pick`), and its `source` parameter is the whole point: **`clap::parser::ValueSource`** — `ArgMatches::value_source(id)` — is what tells the resolution whether a value came from the flag (`CommandLine`), from the process environment (`EnvVariable`, the `env = "TINIO_…"` attribute), or from nothing at all (`DefaultValue`). Levels 1 and 2 are therefore clap's and are never re-derived here: a `CommandLine` or `EnvVariable` answer is final, and `.env` (level 3) is read **only** when clap answered `DefaultValue`, with the config file (level 4) read only when `.env` supplied nothing either. A three-argument `pick(flag, env, file)` would not express that — it cannot tell a flag from the environment, and it would re-implement clap's precedence beside the parser instead of reading the parser's own provenance (design decision 3).

**The `ArgMatches` that carries that provenance has to be kept, and `Cli::try_parse_from` throws it away.** The parse is the two-step form, and `lib.rs`'s `try_run` is its one site:

```rust
let matches = Cli::command().try_get_matches_from(argv).map_err(clap_error)?;
let provenance = cli::provenance(&matches);      // one `ValueSource` per env-backed key
let cli = Cli::from_arg_matches(&matches).map_err(clap_error)?;
```

`cli::provenance(&ArgMatches) -> Provenance` is the one place `value_source` is called, and `Provenance` is a plain struct of `clap::parser::ValueSource` values (one per env-backed flag: `host`, `port`, `verbosity`, `log_format`) — it exists so `resolve` never names `ArgMatches` and so the provenance has a single spelling. **`resolve::server(args, &provenance)` is the only function that takes it**: the client commands' flags are `DIR` and `--read-only`, whose levels 1–2 are the flag itself and the variable behind it (`sources::env_read_only()` when this was written; clap's `env` on the flag since that reader was deleted 2026-09-24), so `resolve::for_doctor(&args)` and Task 8's client helper resolve without a parser in hand — which is exactly why plan C's call is unaffected.

**The boolean switches are clap's flags and `tinio-config::sources`' variables, never both** *(the one-owner rule)*. `--read-only` and `--sandbox` carry **no** `env` attribute: `#[arg(long, env = "TINIO_READ_ONLY")] read_only: bool` compiles, but clap's `SetTrue` action parses its values with `ValueParser::bool()`, whose only accepted spellings are the literals `true` and `false` — so `TINIO_READ_ONLY=1` (the spelling `contracts/config.md:110,117` documents, `crates/tinio-config/src/sources.rs`'s `parse_read_only` accepts, and `crates/tinio-server/tests/serve_read_only.rs` drives) would die in the parser as `invalid value '1' for '--read-only'`, exit 2, before any of this crate's code ran. So the variable's tolerant `1`/`true`/`0`/`false` parse stays where it already is — `sources::env_read_only()` and `sources::env_sandbox()`, one implementation each, through `parse_var` — and `resolve_mode(config, env_flag, cli_flag)` and `sandbox::resolve_flag` are their consumers. The variable's **name** stays visible to the operator through the flag's help text (`--read-only` "config `[server] read_only`, env `TINIO_READ_ONLY`", exactly `contracts/cli.md:38`'s wording), so the flag, its variable and its help text still cannot drift apart. `pick` and `Provenance` therefore cover the **non-boolean** env-backed flags only; that is not a gap but the rule, and the task's own test drives `sources`' overlays directly for the switches (Task 1's `parse_switch` legs, no parser involved).

*(2026-09-22, operator ruling — the paragraph above is **superseded**, and this note says what by.)* The switches moved onto clap's `env`: `--read-only` and `--sandbox` now declare `env = "TINIO_READ_ONLY"` / `"TINIO_SANDBOX"` with `num_args(0..=1)`, `default_missing_value("true")` and `action = ArgAction::Set` over a **custom `value_parser`** — the tolerant `1`/`true`/`0`/`false` spelling, trimmed, refused for anything else, is **the parser's now and not `sources`'**: `tinio_config::sources` reads no variable at all, and `READ_ONLY_VAR`/`SANDBOX_VAR`/`ACCESS_KEY_VAR`/`SECRET_KEY_VAR` live in `tinio_cli::cli` beside the flags whose `env` attributes spell them. `TINIO_STATE_DIR` and the credential pair became environment-only hidden arguments on the same terms, and `state::env_state_dir` is gone with them. `Provenance` therefore covers **all nine** env-backed keys — the boolean switches' `ValueSource` is what tells an explicit `TINIO_READ_ONLY=0`/`TINIO_SANDBOX=0` from silence, so `resolve_mode(config, source, read_only)` and `sandbox::resolve_flag(source, requested, file)` take it — and because clap reads the environment at parse time, the parse is **two-pass** (`lib.rs`'s `command_line`: discovery parse → `<state dir>/.env` → authoritative parse), which is what replaced the `.env` lookups this plan's `pick(.., dot_env, ..)` level performed ([`Design decision 3`'s amendment](../specs/2026-09-20-cli-integration-design.md) states the mechanism in full). Everything below in this task that names `sources::env_read_only()`, `sources::env_sandbox()`, `state::env_state_dir()` or a three-argument `resolve_flag` is the plan as it was written; the landed code is the documents' — `contracts/cli.md` and `contracts/config.md` carry the same ruling. *(2026-09-24: those readers were then deleted outright — `sources::{env_read_only, env_sandbox}` and the `parse_var`/`from_var` helpers they went through are gone; `READ_ONLY_VAR`/`SANDBOX_VAR` and the pure `parse_*` helpers stay, and the plan's own `enable`/`sources` references below are its record of how the phase ran.)*

`resolve::config_path(root, state_dir, explicit)` is the second helper: it owns the rule that an explicit `--config` must name an existing file (`Error::Usage`, exit 2), and delegates the discovery order to `tinio_config::discover::resolve_path`; `resolve::state_dir_candidates(root, resolved, read_only, override_dir, derived) -> Result<Vec<PathBuf>, Error>` is the third, the ordered pair Task 8's three client commands probe. **Its rule, stated once here and quoted by plan C's Global Constraints:** the resolved state dir leads and the **other** candidate follows; the other candidate is `<root>/.tinio` for a **read-only** run and the **home-derived** state dir (`<home>/.tinio/roots/<sha256(canonical root)16>/`) for a **read-write** one — and `TINIO_STATE_DIR`, when it is set, **replaces the home-derived candidate, in both modes**, for the "escape hatch for the environment with no writable home" reason `env_state_dir`'s own doc gives. Three consequences, each of which the shape of this function is built around:

- **The pair never collapses.** A read-only run's resolved dir is already the variable when it is set, so replacing the other candidate with it would give `[X, X]` and `<root>/.tinio` would never be probed — decision 19's whole case (a read-write server found by a reader whose mode says otherwise) would be unreachable, and Task 8's `a_client_finds_the_server_in_the_other_candidate_state_dir` leg asserts exactly that fallback. So the override is consulted **only** where the other candidate would be home-derived, which is the read-write case; a read-only pair is always `[the read-only resolution, <root>/.tinio]`. **One degenerate input sits outside that sentence and is harmless rather than a second rule** (Task 11): with `TINIO_STATE_DIR=<root>/.tinio`, in either mode, the resolved dir and the other candidate are the same path, so the pair is `[<root>/.tinio, <root>/.tinio]` and the second probe repeats the first. Nothing is lost — the fallback the rule protects *is* the first candidate here, because `<root>/.tinio` is where a read-write server for that root keeps its state — and in the read-only arm of the case a server is unreachable anyway, since a read-only run refuses an explicit state dir inside the served root (`contracts/config.md`'s hardening bullet), while a read-write run accepts the input and serves from the very directory both candidates name.
- **The variable moves the candidate list and nothing else.** In read-write mode the served state dir stays `<root>/.tinio`, because the reserved directory is what FR-020, the `meta.redb` layout and `doctor`'s walk all assume; `resolve_state_dir(root, read_only)` is unchanged by any of this.
- **`derived` is lazy so that no home is ever touched when the override answers** — and it is not called at all in a read-only run, where the other candidate is a join. That is what keeps a process-level test (and a container with no writable home) out of `dirs::home_dir()`.

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-cli resolve::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

`crates/tinio-cli/src/cli.rs` — the clap surface, one item per contract flag, with the `env` attributes carrying the names `contracts/config.md:110` lists — **for the non-boolean flags**, plus the `Provenance` reader:

```rust
//! The command line, as `contracts/cli.md` specifies it.
//!
//! Every **non-boolean** flag that has a config key also has an environment name, and both
//! live here so a flag, its variable and its help text cannot drift apart. The boolean
//! switches are the exception, and the exception is a rule rather than an omission: their
//! variables (`TINIO_READ_ONLY`, `TINIO_SANDBOX`) are read by `tinio_config::sources`, whose
//! parse accepts `1`/`true`/`0`/`false`, while a clap `SetTrue` argument parses its values
//! with `ValueParser::bool()` — `true`/`false` only — so an `env` attribute here would turn
//! `TINIO_READ_ONLY=1` into "invalid value '1' for '--read-only'". One owner per variable:
//! the name lives in the flag's help text (so the operator still sees it) and the value
//! lives in `sources`.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// An S3-compatible local storage server.
#[derive(Debug, Parser)]
#[command(name = "tinio", version, about)]
pub struct Cli {
    /// The subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Where an env-backed flag's value came from, one field per such key.
///
/// Read off the `ArgMatches` the parse hands back (`ArgMatches::value_source`), because
/// `Cli::try_parse_from` returns the struct and drops the matches — which is the one thing
/// FR-016's chain needs to tell a flag from `TINIO_*`. `cli::provenance` is the only reader.
#[derive(Debug, Clone, Copy)]
pub struct Provenance {
    /// `--host` / `TINIO_HOST`.
    pub host: clap::parser::ValueSource,
    /// `--port` / `TINIO_PORT`.
    pub port: clap::parser::ValueSource,
    /// `--verbosity` / `TINIO_LOG_LEVEL`.
    pub verbosity: clap::parser::ValueSource,
    /// `--log-format` / `TINIO_LOG_FORMAT`.
    pub log_format: clap::parser::ValueSource,
}

/// The provenance of every env-backed key, in one place.
///
/// `ValueSource` implements no `Default` (`clap_builder-4.6.7/src/parser/matches/value_source.rs:2`),
/// so an absent key — one the parser never saw at all — resolves to `DefaultValue`, which is
/// the same answer a key that fell back on clap's default gives, and is what `pick`'s
/// `_` arm already handles.
pub fn provenance(matches: &clap::ArgMatches) -> Provenance {
    let source = |id: &str| matches.value_source(id).unwrap_or(clap::parser::ValueSource::DefaultValue);
    Provenance {
        host: source("host"),
        port: source("port"),
        verbosity: source("verbosity"),
        log_format: source("log_format"),
    }
}

/// Every key at `DefaultValue` — the shape a caller with no parser in hand uses, and a manual
/// impl because `ValueSource` implements no `Default`. `resolve::server`'s own test is that
/// caller; `provenance` above is the one that reads a real parse.
impl Default for Provenance {
    fn default() -> Self {
        let default = clap::parser::ValueSource::DefaultValue;
        Self {
            host: default,
            port: default,
            verbosity: default,
            log_format: default,
        }
    }
}

/// What the user asked for.
///
/// **`Status` and `Stop` are compiled out with the `api` feature**
/// (`contracts/cli.md:60,70`: "present only in builds with the `api` cargo feature";
/// T055 asserts the same), so their variants sit behind `#[cfg(feature = "api")]`,
/// and without it a client command is an unknown subcommand — clap's own usage
/// error, exit 2.
///
/// **`Doctor` deliberately does not**, and `contracts/cli.md:72-91` is where the
/// omission is visible: it diagnoses the always-present layers (root, config,
/// names, symlinks, free space, the `Cleanup` trait's private-state legs, the meta
/// store), which need no plane, so the command exists in both builds — only
/// `--dry-run`, `--fix` and the stale-state/socket leg are `api`-gated, because
/// without a control channel the command cannot prove a server is stopped and must
/// not offer a repair it cannot guard. Without the feature the subcommand list is
/// `Server`, `Service` and `Doctor`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve a storage root (Minio-style: the directory is positional).
    #[command(alias = "start")]
    Server(ServerArgs),
    /// Report whether a server is running for a root.
    #[cfg(feature = "api")]
    Status(DirArgs),
    /// Ask a running server to stop.
    #[cfg(feature = "api")]
    Stop(DirArgs),
    /// Diagnose a directory without a server.
    Doctor(DoctorArgs),
    /// Manage the OS service that supervises a server.
    Service(ServiceArgs),
}
```

`ServerArgs` carries `dir: Option<PathBuf>`, `--address HOST:PORT`, `--host` (`env = "TINIO_HOST"`), `--port` (`env = "TINIO_PORT"`), `--read-only` (**no** `env` — see the one-owner rule above; its help text names `TINIO_READ_ONLY`), `--sandbox` (**no** `env` either, and the field behind `#[cfg(feature = "sandbox")]` — the `#[cfg]` is the whole of the gate, and the measured fact that a `#[cfg]`-gated clap field compiles out on the feature-off build is the one Task 9's Step 4 leg rides on), `--daemon`, `--verbosity` (`env = "TINIO_LOG_LEVEL"`), `--log-file` (**no** `env` — `contracts/config.md:110`'s list has no server-log-file variable, and `TINIO_ACCESS_LOG` names the *access* log, which `--log-file` is not; the earlier plan text invented `TINIO_SERVER_LOG_FILE` and this is the correction), `--log-format` (`env = "TINIO_LOG_FORMAT"`), `--config`, `--no-follow-symlinks`, `--api <URL>` (repeatable), `--no-api-unix`, `--api-cert`, `--api-key`. **`--anonymous` is not here**: the flag, its `TINIO_ANONYMOUS` wiring and anonymous semantics are US3's T087 (`tasks.md:181`), which is still unticked and stays that way (Task 11), so this plan declares none of them. Three of `contracts/config.md:110`'s names do remain unowned by this plan — `TINIO_API_UNIX`, `TINIO_API_HTTP`, `TINIO_API_HTTPS` (`_CERT`/`_KEY` too) and `TINIO_ACCESS_LOG`/`TINIO_ACCESS_LOG_FORMAT` — and Task 11 records them as a contract-side decision rather than leaving them as names a reader assumes this plan wires.

`DirArgs` is the shared client args struct — `dir: Option<PathBuf>` and `--read-only` (**no** `env` on this struct, for the same reason; the mode's levels 1–2 are the flag and the variable behind it — `sources::env_read_only()` when this was written, clap's `env` on the flag since that reader was deleted 2026-09-24) — which `Status`, `Stop` and plan C's `Doctor` all flatten, so all three resolve the mode the way the server does instead of each inventing one (decision 19); it is **not** gated, because `Doctor` flattens it in every build (the enum's doc above) and a struct a compiled-in variant mentions is never dead code. `DoctorArgs` adds `--json`, which stays in both builds with the report it selects, beside `--dry-run` and `--fix`: the two are one repair path one flag apart, and `doctor --fix` cannot prove a server is stopped without a control channel, so the safe half — the whole repair path, and with it the stale-state/socket leg — is what a feature-off build drops. **Those two flags, their `#[cfg(feature = "api")]` gate and the pair's `conflicts_with` exclusivity are plan C's edit in this file (its Task 1, the only one it makes here), declared once and never restated** — spelling them out here is the second copy this plan's own Step 5 forbids. `ServiceArgs` carries `verb: ServiceVerb`, `dir: Option<PathBuf>`, `--name`, `--user`, and is **not** gated: the service verbs exist in every build.

The sandbox feature gates **behaviour, not linkage**, and this file is where the half that can be compiled away lives: with `sandbox` off, `--sandbox` is not a flag at all (clap answers an unknown flag with its own usage error, exit 2 through the parse's mapping below), while `TINIO_SANDBOX` and `[server] sandbox` still exist — they cannot be compiled away — and reach `sandbox::plan`, which refuses them at runtime with `Error::Usage` naming the build (Task 9). A silently ignored channel would leave a server running without the confinement the operator asked for, which is the one outcome decision 10 forbids.

`crates/tinio-cli/src/resolve.rs` — the resolution, in the order §1's sequence fixes:

```rust
use clap::parser::ValueSource;

/// The four-level chain (FR-016), in the one place it is expressed.
///
/// Levels 1 and 2 are clap's — a flag beats the process environment — and
/// [`ValueSource`] (from `ArgMatches::value_source`) is what says which of the
/// two produced `parsed`. So an answer from either of them is final, and the
/// two levels below are read only when the parser had to fall back on its own
/// default: `.env` first, then the config file, and the parser's default last.
/// A key with no default and no value anywhere is `None`.
pub fn pick<T: Clone>(
    source: ValueSource,
    parsed: Option<T>,
    dot_env: Option<T>,
    file: Option<T>,
) -> Option<T> {
    match source {
        ValueSource::CommandLine | ValueSource::EnvVariable => parsed,
        _ => dot_env.or(file).or(parsed),
    }
}
```

plus `resolve_root` (explicit dir, else walk up from the current directory to the nearest ancestor containing `.tinio/`, else `Error::Usage`), `resolve_mode(config, env_flag, cli_flag) -> bool` implementing decision 4 (**the `.env` layer is not among its inputs**, and `env_flag` was `sources::env_read_only()`'s answer and is clap's `env` on the flag now — that reader was deleted 2026-09-24), `resolve_state_dir(root, read_only) -> Result<PathBuf, Error>` (reusing `tinio_config::state`), `config_path(root, state_dir, explicit) -> Result<PathBuf, Error>` (the `--config` usage rule, then `discover::resolve_path`), `state_dir_candidates(root, resolved, read_only, override_dir: Option<PathBuf>, derived: impl FnOnce() -> Result<PathBuf, Error>) -> Result<Vec<PathBuf>, Error>` (Task 8's ordered pair, and the one place `TINIO_STATE_DIR` is consulted for it — see the rule below), `log_file(resolution, args, provenance) -> Result<PathBuf, Error>` (the operational log file, `--log-file` → `[log] server_log_file` → the format-derived default in the state dir — Task 5's daemon redirects to it and declares no second rule), and `apply_env_file(state_dir) -> Result<(), Error>` calling the existing `sources::load_env_file`.

`resolve::server(&args, &provenance)` — the one the server's `run()` calls — is the composition of them in §1's order and nothing more: root, then mode, then state dir, then `.env`, then the config path, and finally `discover::load_or_create(root, &path, read_only)`. Two properties fall out of that order and are the reason it is spelled out here: an explicit `--config` is checked by `config_path` **before** `load_or_create` can see it, so a mistyped path is exit 2 and never a new file; and the only absent path that reaches the create branch is discovery's fallback `<writable state dir>/.tinio.toml`, which in read-only mode is the relocated dir and never the served root (Task 1's guard refuses the in-root case outright). The `Provenance` is the only reason it takes two arguments: it is what `pick` reads for `--host`/`--port`/`--verbosity`/`--log-format`, and no other key of the chain has a `ValueSource` to consult.

**The `[auth]` pair takes the same chain, minus the levels clap cannot supply.** Credentials are not flags, so there is no `ValueSource` to read for them: the pair resolves `env` → `.env` → the config file, with Task 1's `sources::env_credentials()` as the one lookup. One call answers both upper levels, because `apply_env_file` loads `<state dir>/.env` into the process environment (dotenvy, and it never overrides a variable that is already set — which is exactly why the process environment wins). So `resolve::server` reads `env_credentials()` **after** `apply_env_file` and before returning the `Resolution`, and the pair it answers replaces `config.auth` whenever it is `Some`. The names are `contracts/config.md:110`'s `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY`. Without this layer a child spawned with the two variables set would still serve the credentials the file holds, and the harness switch of Task 10 — whose scenario clients sign with the fixed `minioadmin` pair — would fail against every auto-created root with generated credentials.

`resolve::for_doctor(&args)` is that same chain for the one command that never launches a server: plan C's `doctor` calls it (`2026-09-20-cli-doctor.md`, its Task 3) rather than re-deriving root, mode and state dir, which is what makes decision 19's client-side resolution one implementation and not three. It differs from `resolve::server` in the args type it reads — `DoctorArgs` flattens the same `DirArgs`, so the mode chain is identical — in passing `None` to `config_path` (`doctor` diagnoses the discovered configuration, and a `--config` flag is the server's), and in taking **no `Provenance`**: a client command's flags are `DIR` and `--read-only`, whose levels 1–2 are the flag itself and the variable behind it (`sources::env_read_only()` then, clap's `env` on the flag since that reader was deleted 2026-09-24), so there is no `ValueSource` for it to consult and plan C's call site stays `resolve::for_doctor(&args)`.

**And it is the one chain that does not `?` on the config load, which is the whole reason `config_error` exists.** `discover::load_or_create`'s `Err` is a `Config` there and never a `Resolution`: it fills `config_error` with the failure's message, leaves `config` at `Config::default()`, and returns `Ok`, so `doctor` can report "the configuration does not parse" as an `error` **finding** — the check plan C's Task 3 exists for — instead of exiting 1 with a bare message and no report. The server's half keeps `?`: a run that could not read its configuration must not serve on the default one, and Task 3's `run` fails with the same exit code it always did. Everything downstream of the field treats it the same way: `#[cfg(feature = "api")]` `api_options` and `fs_options` read the (possibly default) `config`, and the report's first row explains why.

`resolve::api_options(resolution, args)` is the CLI's half of the plane's transport rule (design decision 7), and it is a **resolution**: it turns the resolved `[api]` configuration plus the `--api` / `--no-api-unix` / `--api-cert` / `--api-key` flags into plan A's `Selection` (`tinio_api::transport`, `{ local: Option<LocalForm>, http: Option<SocketAddr>, https: Option<HttpsOptions> }`), which Task 3 places in plan A's `Options.selection` and hands to `Plane::bind`. Two rules are its own. **The local form, and the absence rule that goes with it**: `[api.unix] path` (and `[api.pipe] path` on Windows) is a `PathBuf` a present section may leave empty; a relative one joins the resolved state dir — the config contract's "relative to `.tinio/`" — an absolute one is used verbatim, and when the section carries no path at all the answer is plan A's `state::socket_path(state_dir, root)`, the derived `tinio-<sha1(canonicalize(root))[..16]>.sock` of decision 24 and never a fixed `tinio.sock`; whichever of the three wins is what `state.control_name` records, and it is a `LocalForm::Unix`, so no caller re-derives the name. **A config with no `[api]` section at all enables that same derived local channel** — decision 7's ruling that "the local channel is the tool's default, not the operator's choice", and the reading that keeps a hand-written `[s3]`-only config (the interop harness's own fixture, `crates/tinio-e2e/tests/steps/clients.rs`'s `config_for`) a working server rather than a startup refusal. The presence-gating is therefore about *which* transports a config names once it names any: a config with no `[api]` section selects the local channel, a config with one subsection selects exactly that subsection, and `--no-api-unix` with no TCP transport stays the startup error `contracts/config.md:161` requires. This is the rule Task 10's harness switch depends on, and it is a leg of this task's own tests (a `Selection` built from a config with no `[api]` section equals the one built from `[api.unix]` with an empty `path`). **The flags**: one `--api <URL>` per scheme replaces that scheme's subsection (the local channel's path, `http`'s bind, `https`'s bind and pair), and the presence of *any* `--api` flag disables every transport the flags do not name — decision 7's ruling that the local channel is the tool's default and not the operator's choice, so `--api http://127.0.0.1:9001` on an auto-created root yields one transport and not two; `--no-api-unix` is the switch for the case no `--api` flag was given at all, where the config file is the only thing selecting transports; `--api-cert` and `--api-key` fill the `https` pair, and an `https` selection whose cert or key is still empty is refused here with a configuration error naming the missing key (`contracts/config.md:151`'s rule). The **exclusivity** is not decided here: `select` owns it inside `Plane::bind` (plan A), so two callers cannot enforce it differently.

Three of this task's public items are new and each needs its Step 1 leg before its implementation: `for_doctor`'s is the walk-up and mode chain the tests above already drive (one root, one config, one mode, reached through `DoctorArgs`); `provenance`'s is a parse of `--port 9001` and of `TINIO_PORT=9001` asserting `ValueSource::CommandLine` and `ValueSource::EnvVariable`, plus the flag-beats-env case (the one leg that proves the matches are kept); and `api_options`'s is the local-form rule as **four** assertions on the returned `Selection` — a relative `[api.unix] path` joined to the state dir, an absolute one verbatim, an empty one equal to plan A's `state::socket_path` for the same root, and a config with **no `[api]` section** equal to that same derived answer — with the `http`/`https` arms asserted through a second selection built from `--api`-shaped inputs, because the exclusivity belongs to `select` and not to this test.

`lib.rs`'s `run()` gains the shape decisions 2 and 3 fix. It also gains the module tree this task's dispatch needs — `mod cli;`, `mod commands;`, `mod resolve;`, `mod sandbox;` and `mod service;` beside the existing `mod error;` — and each of the last two is a **stub** in this task (below), replaced by Task 9 and Task 7 respectively:

```rust
// `CommandFactory` and `FromArgMatches` are **not** `Parser` methods, and neither comes in
// through `#[derive(Parser)]`: the two-step parse below names both traits explicitly, so the
// import is part of the block rather than something the implementer rediscovers as an E0599.
use clap::{CommandFactory, FromArgMatches};

/// Entry point for the `tinio` CLI.
///
/// # Examples
///
/// ```no_run
/// let _ = tinio_cli::run();
/// ```
pub fn run() -> ExitCode {
    match try_run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("tinio: {err}");
            ExitCode::from(err.exit_code())
        }
    }
}

fn try_run() -> Result<ExitCode, Error> {
    // Step 1 of the sequence: a confined child is identified before any parser sees argv,
    // because `tinio-sandbox-fd=<n>` is not a flag clap would accept.
    let (argv, adopted) = crate::sandbox::filter_argv(std::env::args_os().skip(1).collect())?;
    // The parse is two steps, not `Cli::try_parse_from`: the `ArgMatches` is what carries
    // FR-016's provenance (`ArgMatches::value_source`, read once by `cli::provenance`) and
    // `try_parse_from` returns the struct and throws the matches away. Nothing here exits:
    // clap's own `Error` maps to `Error::Usage`, so exit 2 goes through `exit_code`.
    let matches = Cli::command()
        .try_get_matches_from(std::iter::once(OsString::from("tinio")).chain(argv))
        .map_err(|err| Error::Usage(err.to_string()))?;
    let provenance = crate::cli::provenance(&matches);
    let cli = Cli::from_arg_matches(&matches).map_err(|err| Error::Usage(err.to_string()))?;
    match cli.command {
        // `start::run` is §1's pre-runtime half — resolve, fork, confine — so it returns
        // before a runtime exists, and the arm builds the process's one for the half it
        // leaves (decision 2). A launcher returns its child's status and never reaches it.
        Command::Server(args) => match crate::commands::start::run(args, adopted, &provenance)? {
            Server::Launched(code) => Ok(code),
            Server::Ready(ready) => on_runtime(crate::commands::start::serve(*ready)),
        },
        #[cfg(feature = "api")]
        Command::Status(args) => on_runtime(crate::commands::status::run(args)),
        #[cfg(feature = "api")]
        Command::Stop(args) => on_runtime(crate::commands::stop::run(args)),
        // Not feature-gated, and the enum's doc is why: `doctor` diagnoses the
        // always-present layers, so only the parts of it that need the plane are.
        Command::Doctor(args) => on_runtime(crate::commands::doctor::run(args)),
        Command::Service(args) => crate::service::run(args),
    }
}

/// The process's **one** runtime — the only construction site in the crate (decision 2,
/// and plan C's Global Constraints say the same).
///
/// Every command that needs a runtime is dispatched through this function, and none of
/// them — `server`, `status`, `stop` or `doctor` — builds one of its own. `status` and
/// `stop` are why "a small runtime of their own" is not a smaller version of the same
/// thing: they are network clients whose probe and poll run on this runtime, and a
/// `Builder::new_current_thread()` beside it would be exactly the second runtime decision
/// 2 exists to forbid. It is built **here**, at the end of the sequence, because §1 step k
/// must follow the fork and the confinement: creating a runtime and then forking is the
/// hazard the order avoids (decision 9).
/// `pub(crate)`, because Task 7's service arm — the one path the SCM dispatcher owns, where
/// `run()` itself has been taken over — is the only other caller.
pub(crate) fn on_runtime<F: Future<Output = Result<ExitCode, Error>>>(future: F) -> Result<ExitCode, Error> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::Operational(format!("the runtime could not start: {err}")))?
        .block_on(future)
}
```

Neither `try_get_matches_from` nor `from_arg_matches` exits the process: both `clap::Error`s map to `Error::Usage(err.to_string())`, so exit 2 goes through the one exit-code mapping (`crates/tinio-cli/src/error.rs`'s `exit_code`) rather than clap's own `exit` — `parse_from` would have panicked the process out from under that mapping. `from_arg_matches` cannot fail for a matches built by this command's own parser, and the `map_err` is still there because the alternative is a `panic!` in library code.

Three consequences of the dispatch, stated because they are what "one runtime" means in code. **`start::run` returns a `Server`, not an exit code** (Task 3): a launcher's arm carries the status it supervised and the serving arm carries the state §1's steps a–j fixed, so the runtime is built by `lib.rs` and only for the arm that serves. **`status`, `stop` and plan C's `doctor` are `async fn`s**, awaited by the arm that owns them — none of them constructs, enters or blocks on a runtime, and none may reach for `Builder::new_current_thread()` as the "smaller" alternative; the note in `on_runtime` is the reason. And **`service` is not dispatched through `on_runtime`**: it is a supervisor's errand (install, uninstall, start, stop, status) that talks to the OS and to no server, so it stays synchronous and its verbs never need a runtime at all.

**Stub-first: this task lands the whole dispatch, and every arm it does not own is a stub.** The dispatch above names eight modules and exactly one of them (`resolve`, and `cli`) belongs to this task, so the rest arrive as the smallest compiling shape and each later task replaces its own — no arm is left out, no `#[cfg]` apology is needed for the plan-C arm, and Step 4's "PASS" is reachable in this task's own tree:

| Stub this task creates | Shape | Replaced by |
|---|---|---|
| `commands/mod.rs` | `pub mod start; pub mod status; pub mod stop; pub mod doctor;` — only `status` and `stop` sit behind `#[cfg(feature = "api")]`; the doctor module compiles in both builds, and only the plane-dependent items inside it are gated | Tasks 3 and 8 (the modules), plan C's Task 1 (`doctor`) |
| `commands/start/{mod,args,run,serve}.rs` | `pub fn run(args: ServerArgs, adopted: Option<Adopted>, provenance: &Provenance) -> Result<Server, Error>` and `pub async fn serve(ready: Ready) -> Result<ExitCode, Error>` returning `Error::Usage("not implemented yet".into())`; `pub enum Server` and `pub struct Ready` declared as Task 3 declares them (with `Ready`'s plane-carrying fields behind `#[cfg(feature = "api")]`) | Task 3 |
| `commands/{status,stop}.rs` | `pub async fn run(args: DirArgs) -> Result<ExitCode, Error>` returning `Error::Usage("not implemented yet".into())` | Task 8 |
| `commands/doctor/` | the same shape over `DoctorArgs`, with **no `#[cfg(feature = "api")]` on the module** — `doctor` exists in both builds, and **which of its parts are gated is the contract's doctor section's statement** (plan C owns those edits; restating the gate here is the second copy this check forbids); the file is created here and, from Task 1 of plan C on, plan C's | plan C's Task 1 (`2026-09-20-cli-doctor.md`) |
| `service.rs` | `pub const SERVICE_NAME`, `pub enum ServiceVerb` (`clap::ValueEnum`), `pub fn run(args: ServiceArgs) -> Result<ExitCode, Error>` returning `Error::Usage("not implemented yet".into())` | Task 7 |
| `sandbox.rs` | `pub fn filter_argv(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<Adopted>), Error>` — a faithful pass-through, since no launcher puts the element in argv until a platform backend implements `confine` (the P0 crates are in this tree; their backends are the stubs) — plus `exposures`/`resolve_flag`/`plan`/`confine`/`adopted_listener`/`Request`/`BACKEND_IS_STUB` of the same "not implemented yet" shape, at the signatures Task 9's Produces block fixes | Task 9 |

`Error::Usage` is the right stub error rather than a panic: the global constraint forbids `panic!`/`unwrap` in library code, exit 2 is what an unfinished verb honestly is, and every stub disappears in the task named in the last column — the rule this task establishes is that **a task replaces its own stub and no one else's**, which is also why Task 3 stubs `daemon`, `notify`, `signal` and the sandbox calls it composes (its own table, below).

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-cli`
Expected: PASS. The placeholder `placeholder_run_returns_success` test in `lib.rs` is **deleted** by this task: `run()` without arguments is now a usage error naming the remedy (exit 2), which is what the smoke test asserts in Task 10.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 3: `tinio server` — the orchestration and the readiness contract

**Files:**
- Create: `crates/tinio-cli/src/commands/start/{mod,args,run,serve}.rs`, `crates/tinio/tests/lifecycle.rs` (the process-level test file — package `tinio`, the bin's own package, per the Global Constraints' `CARGO_BIN_EXE_tinio` rule)
- Modify: `crates/tinio-cli/src/commands/mod.rs`, `crates/tinio-cli/src/lib.rs` (the `mod daemon;`, `mod notify;` and `mod signal;` registration this task's stubs need), `crates/tinio-cli/src/{signal,daemon,notify,sandbox}.rs` (the stubs Step 3 lists), `crates/tinio/Cargo.toml` (`[dev-dependencies]`: `tempfile`)

**Interfaces:**
- Consumes: `Resolution` and `resolve::api_options` (Task 2), `tinio_api::{Plane, Options, Selection, Transport, select, State, client}`, `tinio_api::metrics::{MetricsSource, Gauges, DEFAULT_TTL}` (plan A's injection point), `tinio_server::{DataPlane, PlaneConfig, MetricsRefresh, pipeline::Pipelines, log}`, `tinio_fs::{FsStorage, FsOptions, Scanner, FsCleanup}`.
- Produces: `tinio_cli::commands::start::{run, serve, Server, Ready, ServerMetrics}`. `pub fn run(args: ServerArgs, adopted: Option<Adopted>, provenance: &Provenance) -> Result<Server, Error>` is §1's pre-runtime half and **constructs no runtime of its own** (decision 2 — the process's one is `lib.rs`'s); `pub enum Server { Launched(ExitCode), Ready(Box<Ready>) }` is what it returns — the status a launcher supervised, or the state the serving half needs — and `pub struct Ready { resolution, args, adopted, handles, storage, pipelines, plane }` is that state, built by `run` and by nothing else (the fields are Step 3's), with **the plane field behind `#[cfg(feature = "api")]`** — and no `metrics`/`gauges` field beside it: the adapter moved into the plane's registry in `run`, and the registry's reader belongs to plan A's `Plane::serve`, so this crate holds neither a second handle nor a cache — the api-off build has no `tinio-api` dependency at all and must still compile, which is what `docs/tests.md:17-19`'s `cargo check --workspace --all-targets` and `.github/actions/run-test-leg/action.yml:72`'s `--no-default-features` leg require. `pub async fn serve(ready: Ready) -> Result<ExitCode, Error>` is the serving half `lib.rs` dispatches on the one runtime, and it is where the sandbox plans' shared §4 signature lands (their `args`, `config: Option<Config>`, `storage: FsStorage`, `inherited: Option<TcpListener>`, `stop: Option<watch::Receiver<bool>>`, with `storage` **not** optional). `ServerMetrics` is the CLI's `tinio_api::metrics::MetricsSource` — the adapter this task owns because plan A's crate may not name `tinio-server` (design decision 23) — and it is specified in Step 3 below. **The test file this task writes belongs to `crates/tinio`, not `crates/tinio-cli`**: `env!("CARGO_BIN_EXE_tinio")` resolves only in the package that owns the `tinio` bin, so T055's round trip — and every other leg that spawns the binary — lives in `crates/tinio/tests/lifecycle.rs` and runs as `cargo test -p tinio --test lifecycle …`. What stays in `tinio-cli` is its unit-level surface only.

- [x] **Step 1: Write the failing test**

*Deviation (reviewed and accepted): the round-trip leg `a_server_starts_reports_and_stops` did not land here — it spawns `tinio status`/`tinio stop`, which are stubs until Task 8, so it moved into Task 8 beside the rest of the client legs. This task landed the two server-only legs and the shared `spawn`/`client_env` helpers.*

`crates/tinio/tests/lifecycle.rs` (package `tinio`) — T055's round trip, driven through the real binary. `tinio-cli` keeps no copy of this file: its own tests are `resolve::`'s, `cli::`'s, the `#[cfg(unix)]` `signal::` unit and `sandbox::`'s, none of which spawns anything.

```rust
//! T055: the lifecycle round trip through the `tinio` binary.

use std::{path::Path, process::{Child, Command, Stdio}, time::{Duration, Instant}};

fn tinio() -> std::path::PathBuf {
    // `CARGO_BIN_EXE_*` is defined for bins of the package under test, which is why this
    // file lives in `crates/tinio` — the package that owns the `tinio` bin — and not in
    // `crates/tinio-cli`, where the macro would not resolve at all.
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_tinio"))
}

struct Serving(Child);

impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(root: &Path) -> (Serving, String) {
    let mut child = Command::new(tinio())
        .arg("server")
        .arg(root)
        .args(["--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    std::io::BufRead::read_line(&mut reader, &mut line).expect("readiness");
    assert!(line.starts_with("listening on "), "readiness line: {line:?}");
    let endpoint = line.trim().trim_start_matches("listening on ").to_string();
    (Serving(child), endpoint)
}

/// A client spawn's environment, in one place: `TINIO_STATE_DIR` is what keeps the client's
/// second candidate out of `dirs::home_dir()` — which *creates* the directory it derives —
/// and `HOME`/`USERPROFILE` ride along as the unix fallback. Task 8's `client` helper is the
/// same block; a leg that spawned a client without it would seed the developer's real home.
fn client_env(mut cmd: Command, dir: &Path) -> Command {
    // `Command::env` returns `&mut Command`, so the chain is a statement and `cmd` is the
    // value — returning the chain would hand back a borrow of a local.
    cmd.env("TINIO_STATE_DIR", dir)
        .env("HOME", dir)
        .env("USERPROFILE", dir);
    cmd
}

#[test]
fn a_server_starts_reports_and_stops() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");

    let started = Instant::now();
    let (serving, endpoint) = spawn(&root);
    assert!(started.elapsed() < Duration::from_secs(1), "SC-005: ready within a second");
    assert!(!endpoint.is_empty(), "--port 0 reports the bound port");

    // status sees it. The client resolves both candidates, so it gets its own state dir.
    let out = client_env(Command::new(tinio()), dir.path())
        .arg("status")
        .arg(&root)
        .output()
        .expect("status");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("running"), "{text}");
    assert!(text.contains(&endpoint), "status prints the endpoint: {text}");

    // stop ends it, and the state file goes with it.
    let stopped = Instant::now();
    let out = client_env(Command::new(tinio()), dir.path())
        .arg("stop")
        .arg(&root)
        .output()
        .expect("stop");
    assert!(out.status.success());
    assert!(stopped.elapsed() < Duration::from_secs(2), "SC-007: the round trip is fast");
    drop(serving);
    assert!(!root.join(".tinio/state").exists(), "a graceful stop removes the state file");
}

/// The environment half of FR-016's chain, driven the only way this workspace allows it: the
/// variable is set on a **child**, never in-process (Global Constraints). `TINIO_PORT=0` with
/// no `--port` flag must reach the same flag — clap's `env` attribute is level 2 — and the
/// readiness line is where a bound ephemeral port becomes observable.
#[test]
fn the_environment_supplies_a_flag() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");

    let mut child = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .env("TINIO_PORT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    std::io::BufRead::read_line(&mut reader, &mut line).expect("readiness");
    assert!(line.starts_with("listening on "), "TINIO_PORT reached the flag: {line:?}");
    assert!(!line.trim().ends_with(":0"), "--port 0 is ephemeral and reported bound: {line:?}");

    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn a_second_instance_on_the_same_root_fails() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let (_serving, _endpoint) = spawn(&root);

    let out = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .args(["--port", "0"])
        .output()
        .expect("second");
    assert_eq!(out.status.code(), Some(1), "an operational error, per the contract");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("already"), "{text}");
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio --test lifecycle`
Expected: FAIL — `run()` returns success without serving (the test file is `crates/tinio/tests/lifecycle.rs`, hence `-p tinio`; `crates/tinio/Cargo.toml` gains `tempfile` in `[dev-dependencies]` for the `TempDir` above). Cargo builds the package's own binary for this run, so `env!("CARGO_BIN_EXE_tinio")` resolves without a separate `cargo build` — the explicit prerequisite is only needed where the harness lives in another crate (Task 10).

**Every client spawn in this file gets its own state dir, and `TINIO_STATE_DIR` is how.** The `status` and `stop` legs below run the client, and the client's second state-dir candidate is the home-derived `~/.tinio/roots/<hash>/` — *created* while it is computed, by `tinio_config::state::home_state_dir`'s `ensure_roots`. So each of those spawns sets **`TINIO_STATE_DIR`** to the test's `TempDir`, which is what replaces the home-derived candidate (Task 2's rule: the variable is the escape hatch for a machine with no writable home, it replaces that one candidate in either mode, and it never moves the *served* state dir — a read-write run still serves `<root>/.tinio`). `HOME` is set beside it as the unix fallback, and `USERPROFILE` too, but neither is what makes the isolation work: `dirs 7` resolves the Windows home through `SHGetKnownFolderPath(FOLDERID_Profile)` (`dirs-sys-0.5.0/src/lib.rs:150-173`), which never reads either variable, so a spawn that relied on them would still create `%USERPROFILE%\.tinio\roots\` — the real profile. Plan C's `tests/doctor.rs` legs follow the same rule, and its Global Constraints state it against `state_dir_candidates`; the two documents must read the same, so Task 2's rule paragraph is the sentence both quote.

- [x] **Step 3: Write the implementation**

`commands/start/{mod,args,run,serve}.rs` implements §1's sequence — **`commands/start.rs` landed as that directory; the same split took `crates/tinio-cli/src/sandbox.rs` to `crates/tinio-cli/src/sandbox/{mod,linux,windows}.rs`, `mod.rs` carrying the shared half and the dispatch** — and the two halves it must keep apart are the launcher's and the child's: `run` is the pre-runtime half — §1 steps a–j, resolve through the control-channel bind — and builds **no** runtime, `serve` is the half that runs on the process's one runtime, and the launcher arm returns from `run` with the supervised child's status without reaching either of them.

**The order inside `run` is itself the ruling, not a detail** (decision 12, `contracts/cli.md:44`): the control-channel bind is the plane's single-instance gate, so it happens before the startup repair — nothing repairs the state a live server is using — which is why the bind and the storage it takes its gauges from are built in `run` and the repair is the first thing `serve` does. The full order is: **create the state dir and, read-write, the root** (§1 step h, and it comes first because the exposures and the storage both need the paths to exist) → the sandbox's static half (§1 step f) → daemonize (§1 step g) → confine (§1 step i) → the signal handlers (§1 step j) → pipelines and storage → single-instance bind → startup repair → data plane bound → api plane bound → `state` published → readiness line → `READY=1`. **The directories precede `sandbox::plan`, not follow it**: `plan` canonicalises each exposure path (`sandbox.rs`'s own rule) and `tinio_config::state::home_state_dir` creates only `<home>/.tinio` and `…/roots`, never the leaf — so a `create_dir_all` placed after the confinement decision canonicalises a path that does not exist yet. §1's h → i → j order is exactly that, and this block keeps it.

**The first refusal a second instance meets is the store's, not the channel's, and the plan says so rather than implying otherwise.** `FsStorage::new` (`:974` in the block below) opens the redb meta store, which takes an exclusive lock, so a second server on the same root fails there with "Database already open. Cannot acquire lock." (`redb 4.2.0`, `src/error.rs:682-683`) before `Plane::bind` is reached; the control channel is the *plane's* gate and the one decision 12 names, and the acceptance assertion — exit 1 with a message containing "already" — is satisfied by either. Neither path reaches the repair, because both precede `serve`.

```rust
//! `tinio server` — the process sequence of design §1.
//!
//! Nothing here may construct an `FsStorage` before the confinement decision is made:
//! constructing one opens the state database, and under a launcher the *child* opens it
//! (sandbox design decision 18 and its shared plan's §4).
//!
//! This module builds no runtime either: `run` is steps a–j, `serve` is the half that runs
//! on the one `lib.rs` builds (decision 2).

/// What §1's pre-runtime half leaves for the process's one runtime.
pub enum Server {
    /// The macOS/Windows launcher: the confined child has been supervised, and its status
    /// is this process's answer. A launching process never builds a runtime at all.
    Launched(ExitCode),
    /// The serving half: steps a–j are fixed, and step k — the runtime — is `lib.rs`'s.
    /// Boxed because the serving half is by far the larger variant, and
    /// `clippy::large_enum_variant` is a `-D warnings` error here (Global Constraints).
    Ready(Box<Ready>),
}

/// The serving half's state, built by `run` and read by nothing but `serve`.
///
/// `storage`, `pipelines` and the plane's own fields are here rather than in `serve` for one
/// reason: `Plane::bind` is the plane's single-instance gate and belongs before the repair,
/// and the plane's `/metrics` route is fed by a `Gauges` built from this task's
/// `ServerMetrics` — which reads the storage. `Plane::bind` and `select` are synchronous by
/// design (plan A), so none of this needs a runtime, which is exactly why it can precede one.
///
/// **`plane` is the one `api`-gated field** (and `ServerMetrics`, `refresh_handle` and the
/// adapter's `MetricsSource` impl are gated with it), because `tinio-api` is an optional
/// dependency of this crate: in a `--no-default-features` build there is no plane, no state
/// file and no single-instance gate, which is the api-off build the design's Risk table
/// describes ("no single-instance enforcement (there is no control-channel bind without the
/// plane)"). The serving half still serves, and nothing else in this struct mentions the
/// plane's crate.
pub struct Ready {
    resolution: Resolution,
    args: ServerArgs,
    adopted: Option<Adopted>,
    handles: signal::Handles,
    /// The storage the pre-runtime half opened; `serve` binds it to the data plane.
    storage: FsStorage,
    /// The pipelines' handles — shut down and drained at the end of `serve`, IO first.
    pipelines: Pipelines,
    /// The bound management plane — the control channel, created before the repair runs, and
    /// the owner of the one `Gauges` the adapter was injected into. There is no `metrics` field
    /// beside it: the `Arc<ServerMetrics>` moved into that registry in `run`, and nothing in
    /// the serving half would have a use for a second handle — the scrape's cadence and the
    /// scan's schedule are the plane's reader's (plan A's `Plane::serve`), not this module's.
    #[cfg(feature = "api")]
    plane: tinio_api::Plane,
}

pub fn run(args: ServerArgs, adopted: Option<Adopted>, provenance: &Provenance) -> Result<Server, Error> {
    let resolution = resolve::server(&args, provenance)?;   // a–e: root, config, mode, state dir, .env

    // h, **before** the sandbox decision: the bind sources must exist before anything binds,
    // and `sandbox::plan` canonicalises the exposure paths (state dir included), so a missing
    // state dir is a refusal here rather than a canonicalisation failure inside `plan`.
    std::fs::create_dir_all(&resolution.state_dir)?;
    if !resolution.read_only {
        std::fs::create_dir_all(&resolution.root)?;
    }

    let request = sandbox::plan(&resolution, &args)?;    // f: the three static determinations,
                                                         // every unhonourable one refused here

    crate::daemon::detach_if_requested(&args, &resolution, provenance)?;   // g: forks before any
                                                               // thread; the parent waits and exits

    #[cfg(any(target_os = "macos", target_os = "windows"))]
    if let Some(mut child) = sandbox::confine(&resolution, &request)? {
        // The facade's **two-argument** `supervise(&mut Child, teardown)`: it waits for the
        // confined child, runs the teardown on every path, and answers the child's status,
        // which is this process's answer. It borrows the child rather than consuming it
        // (the caller may still read `Child::terminal`); `Child` is `tinio_sandbox::Child`,
        // not `std::process::Child`; and the CLI's own teardown is empty, because the
        // platform's launch owns the rollback and the callback cannot fail by contract.
        // The launcher stops here: it builds no runtime.
        let status = tinio_sandbox::supervise(&mut child, || {})?;
        return Server::Launched(ExitCode::from(u8::try_from(status.code().unwrap_or(1)).unwrap_or(1)));
    }

    let handles = crate::signal::install()?;             // j, before k: handlers before the
                                                         // runtime exists

    // `FsStorage::new` opens the state database, and a launcher has already returned by this
    // line, so this is the child (or the single non-launching process).
    let pipelines = Pipelines::build(&resolution.config.pipeline.clone().unwrap_or_default())?;
    let storage = FsStorage::new(&resolution.root, fs_options(&resolution, Some(&pipelines)))?;
    // The subscriber's error is `Box<dyn Error + Send + Sync>`, which no variant of this
    // crate's `Error` converts from — so it is named, not propagated with a bare `?`.
    let subscriber = log::build_subscriber(verbosity(&args, provenance), format(&args, provenance), &access_format(&args), &resolution.state_dir.join("access.log"), otlp_endpoint(&resolution.config))
        .map_err(|err| Error::Log(err.to_string()))?;
    tracing::subscriber::set_global_default(subscriber).ok();

    #[cfg(feature = "api")]
    let plane = {
        let metrics = Arc::new(ServerMetrics {
            refresh: refresh_handle(&pipelines, &storage),
            storage: storage.clone(),
        });
        // Plan A's `Options::gauges` is what feeds the plane's `/metrics`. The field is not
        // optional in the literal (the plane's `Options` has six fields, and a five-field one
        // does not compile), and omitting it would answer 404 on every scrape: the source is
        // this task's adapter, and plan A's `Plane::serve` is what drives it — the scrape's
        // cadence and the scan's schedule are the plane's, not this module's.
        let source: Arc<dyn tinio_api::metrics::MetricsSource> = metrics.clone();
        let gauges = Arc::new(tinio_api::metrics::Gauges::new(source, tinio_api::metrics::DEFAULT_TTL));
        let bind = resolution.address()?;                // `[server] host`/`port`, flags folded in
        let plane = tinio_api::Plane::bind(Options {
            selection: api_options(&resolution, &args)?,  // `[api]` + the `--api*` flags
            state_dir: resolution.state_dir.clone(),      // the plane's only filesystem anchor
            root: resolution.root.clone(),                // canonical: `state.root` too
            host: bind.ip().to_string(),                  // what `/status` reports
            stop: handles.stopper.clone(),                // `/stop` sends on the same channel
            gauges: Some(gauges),                         // 404 on `/metrics` without it
        })?;                                              // the plane's single-instance gate
        plane
    };

    Ok(Server::Ready(Box::new(Ready {
        resolution,
        args,
        adopted,
        handles,
        storage,
        pipelines,
        #[cfg(feature = "api")]
        plane,
    })))
    // k is `lib.rs`'s: no runtime is created anywhere in this module.
}

/// §1's serving half — the steps after the runtime exists — dispatched by `lib.rs` on the
/// process's **one** runtime (decision 2).
///
/// The repair is deliberately the first thing here, and the plane's single-instance gate has
/// already happened in `run`: decision 12's order is single-instance bind → repair → data
/// plane → api plane → `state` → readiness, so an instance that loses the bind never reaches
/// a repair that would rewrite the state a live server is using.
pub async fn serve(ready: Ready) -> Result<ExitCode, Error> {
    // One pattern, with the plane's field gated the same way the struct declares it.
    let Ready {
        resolution,
        args,
        adopted,
        handles,
        storage,
        pipelines,
        #[cfg(feature = "api")]
        plane,
    } = ready;

    repair(&storage, &resolution).await;                 // Cleanup::repair(Startup), post-bind

    let stop = handles.shutdown.clone();                 // one shutdown path, three entry points
    let bind = resolution.address()?;                    // `[server] host`/`[port]`, flags folded
    let listener = match adopted {
        Some(adopted) => sandbox::adopted_listener(&adopted)?,      // the handed-in listener
        None => tokio::net::TcpListener::bind(bind).await?,
    };
    let address = listener.local_addr()?;
    let caps = capabilities_for(&resolution);            // lifted from `serve.rs:91-103`
    let access = AccessMode::from(resolution.read_only);
    let (access_key, secret_key) = credentials_of(&resolution); // env → `.env` → `[auth]` (Task 2)
    // No `.with_metrics(..)`: plan A's Task 7 retires the data plane's interim `/metrics`
    // route (F10), so the hook has no reader left. The cheap families are still published —
    // the CLI's adapter calls them on plan A's reader, which is the same registry.
    let data = DataPlane::new_with_auth(storage.clone(), PlaneConfig { caps, access }, access_key, secret_key);

    // STOPPING=1 belongs to the **moment the shutdown begins**, not to the moment it ends:
    // watching the channel is what catches every entry point (a signal, `POST /stop`) at the
    // instant it fires, which is exactly the window decision 21 says systemd would otherwise
    // not see. Emitting it after the join would announce the drain only once it had finished.
    tokio::spawn({
        let mut watching = stop.clone();
        async move {
            let _ = watching.changed().await;
            crate::notify::stopping();
        }
    });

    // Both planes are up before `state` exists (decision 12), which is why the api plane is
    // spawned: its `serve` borrows the plane, and `state` is published after it. One `Arc`,
    // shared by the spawn, `publish` and `shutdown` — a second `Arc::new` would be a second
    // handle onto the same value and a moved-from one at that.
    #[cfg(feature = "api")]
    let plane = Arc::new(plane);
    #[cfg(feature = "api")]
    let api = tokio::spawn({
        let (plane, stop) = (plane.clone(), stop.clone());
        async move { plane.serve(stop).await }
    });
    let data = tokio::spawn(data.serve(listener, stop.clone()));
    let background = tokio::spawn(scanner_and_sweeper(storage.clone(), stop.clone()));

    #[cfg(feature = "api")]
    plane.publish(address.port())?;                      // `state` is written here
    println!("listening on {address}");                  // the readiness line, on stdout (§8)
    // The operational summary, on stderr: endpoint, root and credentials status — the line
    // `contracts/cli.md:46` promises, and the reason stdout stays a one-line channel.
    eprintln!(
        "serving {root} on {address} ({mode}; credentials {origin})",
        root = resolution.root.display(),
        mode = if resolution.read_only { "read-only" } else { "read-write" },
        origin = credential_origin(&resolution),
    );
    crate::notify::ready();                              // READY=1, inert off systemd
    crate::notify::watchdog();

    // **No metrics ticker here**, and that is deliberate rather than an omission. The
    // scrape's cadence and the storage scan's schedule belong to the plane: plan A's
    // `Plane::serve` spawns its reader (`gauges.spawn_reader(stop)`), `Gauges::refresh` is
    // the only method a scrape calls and it never scans, and `counts()` — this adapter's
    // expensive half — is called by that reader alone. A second ticker in this module would
    // be a cache beside a reader (design decision 23 retires the cache), and the two would
    // have to agree on a period neither of them owns.

    #[cfg(feature = "api")]
    let _ = tokio::join!(api, data, background);         // every arm ends on the same `stop`
    #[cfg(not(feature = "api"))]
    let _ = tokio::join!(data, background);

    #[cfg(feature = "api")]
    plane.shutdown().await?;                             // drain ≤ 10 s, remove socket and `state`
    pipelines.shutdown();                                // IO first, the DB write pipeline last
    pipelines.drain().await;
    Ok(ExitCode::SUCCESS)
}
```

**The api-off half is a real build, not a formality.** Every `tinio_api` name above sits behind `#[cfg(feature = "api")]` (the `Ready` fields, the bind, the spawn, `publish`, `shutdown`, the ticker), so `cargo check --workspace --all-targets` and the `--no-default-features` leg compile; what the api-off server loses is the state file, the control channel — and with it the single-instance gate — `status`/`stop` and `/metrics`, which is precisely the scope the design's Risk table gives that build. The readiness line, the stderr summary, the scanner and the shutdown path are unchanged, and `serve` still ends on the same `stop` channel the signal handlers flip.

**This task's own stubs, and the tasks that replace them.** The orchestration above composes four modules that do not exist yet, so it lands against the smallest compiling shape of each — the rule Task 2 establishes, applied to this task's dependencies. `lib.rs` registers them in this task (`mod daemon;`, `mod notify;`, `mod signal;` beside Task 2's `mod sandbox;`, which this task likewise stubs further):

| Stub this task creates | Shape | Replaced by |
|---|---|---|
| `signal.rs` | `pub struct Handles { pub shutdown: watch::Receiver<bool>, pub stopper: watch::Sender<bool> }`, `pub fn install() -> Result<Handles, Error>` returning a fresh pair and registering **no** handler, `fn second_signal_exits_immediately()` present and empty (private — Task 4's Interfaces says why) | Task 4 |
| `daemon.rs` | `pub fn detach_if_requested(args: &ServerArgs, resolution: &Resolution, provenance: &Provenance) -> Result<(), Error>` — `Ok(())` when `--daemon` was not asked for, `Error::Usage("not implemented yet".into())` when it was | Task 5 |
| `notify.rs` | `ready()`, `stopping()`, `watchdog()` — all three no-ops (which is what they already are without `NOTIFY_SOCKET`); no `unit_text` yet | Task 6 |
| `sandbox.rs` | Task 2's `filter_argv`, plus `plan(&resolution, &args) -> Result<Request, Error>` answering "disabled" (an empty exposure list) when no channel is set and refusing with `Error::Usage("not implemented yet".into())` when one is, `confine(&resolution, &request) -> Ok(None)`, and `adopted_listener(..)` → `Err(Error::Operational("not implemented yet".into()))` — the signatures Task 9's Produces block fixes | Task 9 |

None of these stubs may be *silently* permissive: a run that asks for confinement, or for a daemon, gets a refusal naming the build, never a plain server (decision 10's fail-closed rule). The only stub that is inert by design is `notify`'s, whose real implementation is inert in exactly the same way when the process is not under systemd.

`ServerMetrics` is the plan's one implementation of plan A's injection point, and its owner is this task because no other crate can be:

```rust
/// The CLI's `tinio_api::metrics::MetricsSource` — the registry side of a `/metrics` scrape.
///
/// `tinio-api` must not depend on `tinio-server` (design decision 23: the edge is the reverse
/// of `tinio-server`'s optional `tinio-api` dependency, and cargo rejects the cycle at
/// resolution time), so the source is *injected* and this crate — the only one that already
/// depends on both — is what injects it.
///
/// **Two methods, and the cadence is plan A's.** `refresh` is the cheap families, called on
/// every scrape through `Gauges::refresh`; `counts` is the storage scan — the one expensive
/// thing here — and plan A's **reader** is its only caller (`Plane::serve` spawns it,
/// `gauges.spawn_reader(stop)`), which is what retires the TTL cache decision 23 describes:
/// there is no staged copy in this crate and nothing to keep in step, because the reader's own
/// schedule *is* the schedule. A second ticker here would be that cache under another name.
///
/// **`counts` blocks for the length of a listing pass, and it runs on the blocking pool.** The
/// trait is synchronous by shape, and plan A's reader wraps the call in
/// `tokio::task::spawn_blocking` (`Gauges::spawn_reader`, plan A's `:2644-2660`), so the scan
/// never occupies a worker of the serving runtime: it runs once immediately and then once per
/// `DEFAULT_TTL`, on a blocking thread, and the adapter's work is stated here because a scan that
/// grew into minutes would show up as a busy blocking thread rather than as a stalled `/stop`,
/// `/status` or data plane. Sizing the scan itself stays plan A's `spawn_reader`'s call, not this
/// crate's.
#[cfg(feature = "api")]
struct ServerMetrics {
    /// The cheap families — pipeline gauges and write-lock histograms — as the type
    /// `tinio_server::MetricsRefresh` (`data.rs:75`, `Arc<dyn Fn() + Send + Sync>`), which
    /// plan A re-exports out of the crate because the `data` module is private. Name the
    /// re-export; never rely on the argument's position in any signature.
    refresh: tinio_server::MetricsRefresh,
    /// The scan's subject, held as the **concrete** type: `tinio_core`'s `storage::Storage`
    /// is deliberately dyn-incompatible (its associated error type and its
    /// `where Self: Storage` bounds), so `dyn Storage` is never spelled in this crate —
    /// `counts` below is what reads it, and this field is why the read is possible at all.
    storage: FsStorage,
}

/// The cheap families' closure, built once in `run` and shared with the adapter.
///
/// The same wiring `serve.rs` does today (`:337-347`): the two pipelines' `.stats()` snapshots
/// **taken at call time**, not at start-up, and the storage's `write_lock_stats()` converted to
/// `WriteLockStats` by the lifted `write_lock_stats` (`serve.rs:75`, which moves to this
/// module's helpers with the rest of the lifted code). Gated with the adapter, because the
/// adapter's construction is its only caller.
#[cfg(feature = "api")]
fn refresh_handle(pipelines: &Pipelines, storage: &FsStorage) -> tinio_server::MetricsRefresh {
    let (io, db, storage) = (pipelines.io(), pipelines.db(), storage.clone());
    Arc::new(move || {
        tinio_server::metrics::refresh(io.stats(), db.stats(), write_lock_stats(storage.write_lock_stats()))
    })
}

#[cfg(feature = "api")]
impl tinio_api::metrics::MetricsSource for ServerMetrics {
    /// The cheap families — the call `Gauges::refresh` makes on every scrape, and the same
    /// arc the data plane's retired hook held, so the two cannot drift.
    fn refresh(&self) {
        (self.refresh)();
    }

    /// The storage scan, and the only place these numbers exist.
    ///
    /// The trait is plan A's (its Task 7's `MetricsSource`: `refresh` and `counts`, both
    /// side-effecting and neither returning a value — no counts cross the crate boundary),
    /// and this is the one place plan B follows it. Nothing here computes a number for
    /// `tinio-api`: `STORAGE_BUCKETS`, `STORAGE_OBJECTS` and `STORAGE_BYTES` are
    /// `tinio-server`'s statics, `tinio-api` may not name that crate at all, and its gauges
    /// read the registry those three statics live in — so the numbers travel from the scan to
    /// the registry entirely inside this crate, and `tinio-api` stays storage-free. The walk
    /// goes through the contract's generic form (`S: Storage`) in this module's own
    /// `count_storage`, because `dyn Storage` does not exist.
    fn counts(&self) {
        let counts = count_storage(&self.storage);
        STORAGE_BUCKETS.set(i64::try_from(counts.buckets).unwrap_or(i64::MAX));
        STORAGE_OBJECTS.set(i64::try_from(counts.objects).unwrap_or(i64::MAX));
        STORAGE_BYTES.set(i64::try_from(counts.bytes).unwrap_or(i64::MAX));
    }
}
```

**Every item in that block is `api`-gated, and the type is no exception**: a feature-off build has no plane, so nothing would construct `ServerMetrics`, and an ungated private type with no constructor is dead code under `-D warnings` — which is why `ServerMetrics`, `refresh_handle`, the `impl MetricsSource` and the `Ready::metrics` field all carry the same gate, and why the `--no-default-features` leg compiles at all. The two halves of `MetricsSource`'s contract are the ones that matter here: **`refresh` is the cheap families with plan A's three real arguments** (`tinio_server::metrics::refresh(io: Stats, db: Stats, write_lock: WriteLockStats)`, `Stats` being `tinio_core::pipeline::Stats`), and **`counts` is the scan** — the counts are read over the concrete storage this type holds and settled through `IntGauge::set`, three `tinio-server` statics that `tinio-api` cannot name at all and has no storage to compute, which is why the scan is the CLI's. Neither method returns a value and neither takes one: no count, and no stand-in for one, crosses the crate boundary in either direction.

**The order inside `run()` and `serve()` is the ruling, and each move has a measured reason.**

- **The bind comes first, and the repair follows it.** `Plane::bind` is the control-channel bind and therefore the plane's single-instance gate (decision 12, `contracts/cli.md:44`), so a second plane fails there — exit 1, "already running" — and never reaches `Cleanup::repair(Startup)`, which would otherwise rewrite the state file and socket a live server is using. (The *first* refusal a second server meets is the meta store's own lock, in `FsStorage::new`; see the order paragraph above.) The order is bind → repair → data plane bound → api plane bound (`plane.serve` spawned) → `plane.publish(port)` → the readiness line → `READY=1`.
- **The bind happens in `run`, before the runtime.** `select`/`Plane::bind` are synchronous by design (plan A: the TCP arm binds with `std::net::TcpListener` and converts), so nothing about it needs a runtime — and putting it in `run` is what lets the repair, the data plane and the api plane all be built on the process's one runtime afterwards. `Ready` carries the bound plane and the storage its adapter reads, which is why those fields exist at all.
- **This module builds no runtime.** `run` is §1's pre-runtime half and ends by handing `lib.rs` a `Server`; `serve` is the half that runs *on* the runtime `lib.rs` built (decision 2). The split is not stylistic: a `Builder::new_multi_thread()` here would put a runtime on the wrong side of the daemon fork and the confinement, which is the hazard the sequence exists to avoid — and the launcher arm returns `Server::Launched(..)` without ever reaching §1 step k, so a process that launches never creates a runtime at all.
- **The three static sandbox determinations run before anything forks.** They are the feature-off build, the channel this build does not honour, and the platform whose backend is still a stub; `sandbox::plan` is where all three live (Task 9), so a fail-closed refusal reaches the shell as `Error::Usage` (exit 2, feature off) or as a startup error naming the channel or the platform — and the daemon bullet below is why that has to happen *before* the fork rather than inside it.
- **`--daemon` uses `Daemonize::execute()`, never `start()` — and the outcome it returns is `Outcome<T>`, not a `Result` of one.** Measured out of `daemonize 0.5.0`: `start()` exits the **original** process inside itself with the intermediate child's exit code (`src/lib.rs:343`), and the intermediate child's own exit (`:378`) is unconditional — so under `start()` the operator's `$?` is always 0 and a refusal raised inside the daemon is invisible. `execute(self) -> Outcome<T>` (`:351`) returns in the original process instead, with the two arms carrying results — `Outcome::Parent(Result<Parent, Error>)` and `Outcome::Child(Result<Child<T>, Error>)` (`:186`), both `#[non_exhaustive]` — so the parent branch is matched, not just called: `Outcome::Parent(Ok(..))` is where the readiness probe and the exit code live, `Outcome::Parent(Err(..))` is a daemonize failure reported as such, and `Outcome::Child(..)` is the daemon continuing into the sequence. That is what lets Task 5's parent probe the state dir it already resolved, print the readiness line on plain stdout and exit 0 — or exit 1 after the ~10 s bound with a message naming the daemon's log file and `tinio status`. This supersedes decision 9's "the parent exits 0 without waiting for readiness"; the design's own edit carries the change, and the plan must not restate the old sentence.
- **`confine` is reached in the daemon**, which is the point of the split: the child that will actually serve is the one that confines itself. With `--daemon` asked for on macOS or Windows, the launcher arm runs inside the daemon, so the supervised child is detached along with it and the parent's probe waits for the `state` whichever process publishes it.
- **The signal handlers are registered before the runtime is built** (§1 step j before step k), in `run` and not inside `serve`, because a signal that arrives during startup must reach the same shutdown path as one that arrives later (decision 20). `Handles::shutdown` is what crosses into `serve` and into the sandbox plans' shared `serve` signature — its `stop: Option<watch::Receiver<bool>>` parameter is this channel, unchanged — and `Handles::stopper` is the plane's `/stop` channel, so a signal, a `POST /stop` and the data plane's drain are one channel and not three (decision 12).
- **`state` and the control socket have one owner, the plane.** `plane.publish(port)` writes `state` at the readiness point and `plane.shutdown()` drains ≤ 10 s and then removes both; `Drop` on the plane is the best-effort backstop for a path that returns without a clean shutdown. `status` and `stop` never delete either file — the plane owns them (decision 12, `Plane::publish`/`Plane::shutdown`) — and `crates/tinio-cli` never joins the file names itself; the reason is a race: a client cannot know whether the server behind a file is still live, which is what keeps a read-only run, a launcher's child, a `/stop` and a second instance from disagreeing about who cleans up. The one exception is `doctor --fix`, which reclaims a stale socket through `tinio_api::bind::reclaim_stale` and a stale `state` through `tinio_api::State::remove`, only after a probe has proved no server answers for that root. This is the same story plan A implements.
- **`plane.shutdown()`'s `Result` is not discarded.** `let _ = plane.shutdown().await;` and a bare `plane.shutdown().await;` both trip this task's own `-D warnings` gate (`unused_must_use`), so the call is `plane.shutdown().await?` — a drain failure is a real operational failure and exits 1 through the one mapping.

The details this skeleton deliberately leaves to the implementer are all transcriptions of code that exists in `serve.rs` today, and they are lifted by **line range** rather than rewritten — with the ranges distributed between `run` and `serve` by the order the bullets above fix (`FsStorage`, the pipelines, the subscriber, the plane build and its `/metrics` wiring land in `run`; the repair, the data listener, the readiness point, the scanner/sweeper spawns and the shutdown/drain sequence land in `serve`). As it stands the example is one `async fn main` (`122`) plus four small items — `usage` (`65`), `write_lock_stats` (`75-84`, the `WriteLockSnapshot` → `WriteLockStats` conversion the adapter's handle above calls), `capabilities_for` (`91-103`) and `plane_for` (`105-121`) — and the ranges to lift are: argv handling `123-166` (replaced by clap, Task 2), config load `173-176` (replaced by `discover`, Task 1), read-only resolution `180-186` (replaced by `resolve::resolve_mode`), root preparation `195-197`, the state-dir resolution `215-237`, `FsStorage::new` with its `FsOptions` `243-263` and the pipelines `242`, the log subscriber `268-276`, the startup repair `285-307`, readiness `309-310`, the scanner and sweeper spawns `313-331`, the plane build `333-348` with the `/metrics` wiring inside it `334-347`, and the shutdown/pipe-drain sequence `349-368`. Keep their names. There is no `serve` in the example and no `Server` type: the entry points (`run`, `serve`, `Server`, `Ready`) are this plan's.

The stub the red state needs is the implementer's first act: the smallest `commands::start::run`/`serve` pair that compiles Task 2's `lib.rs` dispatch and returns success without serving, so Step 2 fails on the assertion rather than on a compile error. It is then replaced by the real `run`/`serve` above, in the same task.

**The small helpers this block uses, all pure and all this task's**: `capabilities_for(&resolution) -> Capabilities` (the lifted `serve.rs:91-103` shape, narrowed to the resolved `Config` rather than its `Option`, since `Resolution` always carries a parsed config); `credential_origin(&resolution) -> &'static str` (`"from TINIO_ACCESS_KEY/TINIO_SECRET_KEY"` when `sources::env_credentials()` answers `Some`, else `"from the config file"`), which is what the stderr summary line prints as the credentials status; and `verbosity(&args, provenance)` / `format(&args, provenance)`, the two readers that run `pick` over `--verbosity`/`--log-format` and their `TINIO_*` variables (and over the `.env` and `[log]` layers when the parser answered `DefaultValue`), which is why `run` takes the `Provenance` at all.

**A doc step belongs to this task, because the readiness line is its subject**: `contracts/cli.md` gains the sentence decision 11 fixes — readiness on **stdout** (the single line a harness scrapes) and the operational summary on **stderr** — replacing the "to stderr (operational log)" phrasing that left the machine-readable channel undocumented while this binary ships it.

- [x] **Step 4: Run the tests to verify they pass**

*Deviation (reviewed and accepted): three tests were written, not two — the two server-only legs plus `a_second_instance_on_the_same_root_fails`; the round trip is Task 8's. The run below passes with the two legs this task landed.*

Run: `cargo test -p tinio --test lifecycle`
Expected: PASS — both tests (the file is `crates/tinio/tests/lifecycle.rs`; `tinio-cli`'s own tests are its unit-level ones and are run by the next command's sibling, `cargo test -p tinio-cli`).
Run: `cargo test -p tinio-server --test serve_read_only`
Expected: PASS unchanged (the example still exists at this point; Task 10 retires it).

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings` and `cargo clippy -p tinio --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 4: The signal handlers

**Files:**
- Create: `crates/tinio-cli/src/signal.rs` (replacing the stub Task 3 landed — this task replaces its own stub and leaves the registration in place)
- Modify: `crates/tinio-cli/src/lib.rs` (`mod signal;` is already registered by Task 3's stub; what changes here is the module's content, and `lib.rs` itself only if the stub's `Handles` shape needs the real one), `crates/tinio-cli/Cargo.toml` (the `cfg(unix)`/`cfg(windows)` dependency arms, **and the manifest change below**)

**The manifest change this task owes, and the crate's two enumerated exceptions.** A console handler must be an `unsafe extern "system" fn` and `windows-sys` is raw bindings, so `crates/tinio-cli/Cargo.toml`'s `[lints.rust] unsafe_code` moves from `"forbid"` to `"deny"` — the crate currently forbids it outright. The crate has **one** `#[allow(unsafe_code)]` site when this phase closes, with one reason and nothing else allowed — and the phase has three sites in total, not two (a plan-audit correction: `tinio-config`'s ACL module in Task 1, this one, and Task 10's console-event test leg, which cannot be written without one because `forbid` — the facade manifest's level until Task 10 flips it to `deny` — cannot be locally allowed):

1. **this task's**: `signal.rs`'s Windows handler and its registration call, together in one block — design decision 20's enumerated exception (`docs/cargo.md`'s lint rule records the pattern, and `tinio-config` takes the same shape for its ACL block in Task 1), whose invariant is Windows's rather than this design's (the handler runs on a system thread, may not allocate or lock, and only `send`s on a channel created before it was registered);
2. **plan C's**, in `commands/doctor/`: the `windows-sys` `GetDiskFreeSpaceExW` call behind the free-space probe — a raw-binding call with no safe wrapper in `std`, enumerated in that plan's own task.

So "one site" is not the rule; **enumerated sites are**, and no task in this plan adds a third. Nothing else in the crate gains an `allow`.

**Interfaces:**
- Consumes: nothing from this plan beyond `Error`.
- Produces: `tinio_cli::signal::{install, Handles}` with `pub fn install() -> Result<Handles, Error>` where `Handles { shutdown: watch::Receiver<bool>, stopper: watch::Sender<bool> }`, plus the **private** fast-exit arm `fn second_signal_exits_immediately()`, called by the iterator thread on the second termination signal — private because nothing outside this module has a reason to reach it, and the stub Task 3 landed is the same private `fn` with an empty body.

- [x] **Step 1: Write the failing test**

Append to `crates/tinio-cli/src/signal.rs`:

```rust
#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_signal_flips_the_shutdown_channel() {
        let handles = install().expect("install");
        let mut shutdown = handles.shutdown.clone();
        // The test's own process is the only place a handler can be installed, so this test
        // asserts the wiring, and the process-level leg lives in
        // `crates/tinio/tests/lifecycle.rs` (the `tinio` package's file).
        nix::sys::signal::raise(nix::sys::signal::Signal::SIGTERM).expect("raise");
        tokio::time::timeout(Duration::from_secs(5), shutdown.changed())
            .await
            .expect("the channel flips")
            .expect("sender alive");
        assert!(*shutdown.borrow());
    }
}
```

`signal_hook`'s own `raise` is deliberately not used: the point of the test is the registration path, and `nix` is the crate this design already added for unix syscalls. The process-level behaviour (graceful exit, `state` removed, second signal exits without draining) is asserted in `crates/tinio/tests/lifecycle.rs` by spawning the binary and signalling it, because a handler installed by the test process is global to it — and that file belongs to the **`tinio` package**, not to `tinio-cli` (the Global Constraints' `CARGO_BIN_EXE_tinio` rule), so this task's own module keeps only the `#[cfg(all(test, unix))]` unit above.

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-cli signal::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

```rust
//! Termination signals, turned into the one shutdown channel (design decision 20).
//!
//! `signal-hook` registers and owns the self-pipe on unix; nothing in the handler allocates
//! or locks. On Windows `signal-hook`'s CRT-backed support cannot express the console-close
//! event the contract requires, so the console handler is `windows-sys`'s.

use tokio::sync::watch;

use crate::Error;

/// The shutdown channel, plus the ability to raise a signal from a test.
#[derive(Debug, Clone)]
pub struct Handles {
    /// The receiver the planes take. Flipping to `true` means "stop".
    pub shutdown: watch::Receiver<bool>,
    /// The sender `/stop` and the handlers both flip.
    pub stopper: watch::Sender<bool>,
}

/// Installs the handlers and returns the channel they flip.
pub fn install() -> Result<Handles, Error> {
    let (stopper, shutdown) = watch::channel(false);

    #[cfg(unix)]
    {
        use signal_hook::{consts::{SIGHUP, SIGINT, SIGTERM}, iterator::Signals};

        // **No `flag::register` here, and the reason is the counter below.** A
        // `flag::register(SIGTERM, Arc<AtomicBool>)` sets its atomic *from the handler*, and
        // the iterator only yields a signal after the handler for that signal has run — so a
        // thread that asked the same atomic "is this the second one?" would read `true` on the
        // **first** signal and exit before ever sending on the channel. The termination count
        // is therefore this thread's own, and the atomic is not registered at all.
        //
        // `stopper` is cloned **before** the `move` closure: the thread takes the clone and the
        // `Handles` returned below keeps the original. Without this line the sender is moved
        // into the thread and the return is an E0382 — the Windows arm never hits it because it
        // hands its own `stopper.clone()` to the registration call.
        let stopper = stopper.clone();
        let mut signals = Signals::new([SIGTERM, SIGINT, SIGHUP]).map_err(signal_error)?;
        std::thread::spawn(move || {
            let mut terminations = 0u32;
            for signal in signals.forever() {
                if signal == SIGHUP {
                    // The contract: SIGHUP is ignored and logged, never a shutdown. The
                    // iterator thread is where "logged" can happen — a handler cannot log.
                    tracing::info!(signal, "SIGHUP ignored (no config reload in v1)");
                    continue;
                }
                terminations += 1;
                if terminations > 1 {
                    second_signal_exits_immediately();
                    continue;
                }
                tracing::info!(signal, "shutting down");
                let _ = stopper.send(true);
            }
        });
    }

    #[cfg(windows)]
    {
        crate::signal::install_console_handler(stopper.clone())?;
    }

    Ok(Handles { shutdown, stopper })
}

/// The fast-exit arm: a second termination signal exits immediately, without draining
/// (`contracts/cli.md:43`). Called only from the iterator thread on its second count.
fn second_signal_exits_immediately() {
    tracing::warn!("second termination signal: exiting now");
    std::process::exit(0);
}

fn signal_error(err: std::io::Error) -> Error {
    Error::Operational(format!("the signal handlers could not be installed: {err}"))
}
```

**The channel is filled from ordinary code, which is what §1 promises**: `signal-hook`'s iterator owns the self-pipe, this thread is a real `std::thread`, and the only thing the handler body itself does is write to that pipe — no allocation, no lock, no `watch::Sender` inside a handler. The termination count is that thread's local state, never the registered atomic, and `SIGHUP` is in the same `Signals` set so "ignored and logged" is one arm of one loop.

The Windows arm declares `SetConsoleCtrlHandler` through `windows-sys` and handles `CTRL_C_EVENT`, `CTRL_BREAK_EVENT` and `CTRL_CLOSE_EVENT` by sending on the same channel; `CTRL_CLOSE_EVENT` gets a bounded wait because Windows terminates the process shortly after the handler returns, so the drain it can afford is shorter — record the bound in the handler's doc comment and assert it in the Windows leg of `crates/tinio/tests/lifecycle.rs` (Task 10 adds that leg). This function and its registration call are the crate's **first** `#[allow(unsafe_code)]` site (the manifest change above; the second is plan C's free-space probe), and the invariant it carries is Windows's rather than this design's: the handler runs on a system thread, may not allocate or lock, and does nothing but `send` on a channel created before it was registered.

- [x] **Step 4: Run the tests to verify they pass**

*Deviation (reviewed and accepted): run in WSL2, where the `#[cfg(all(test, unix))]` unit actually executes — 1 passed there, 0 matched on this Windows host. This task also added a `#[cfg(all(test, windows))]` unit covering the second-console-event rule, so the filter matches 1 test on Windows too.*

Run: `cargo test -p tinio-cli signal::`
Expected: PASS — **and on a Windows host this exact filter matches zero tests**. Both the unit above and its module are `#[cfg(all(test, unix))]`, so `cargo test -p tinio-cli signal::` exits 0 there having run nothing, which is not evidence of anything; say so when you report the step. The Windows arm is compiled by Step 5's `windows-gnu` clippy leg and is exercised end to end by the Windows-gated process legs in `crates/tinio/tests/lifecycle.rs` (Task 10's harness legs and the console-handler leg), never by this crate's unit tests.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings` on this host, and the second leg — `cargo clippy --target x86_64-pc-windows-gnu -p tinio-cli --all-targets -- -D warnings`, the one that compiles the `windows-sys` arm — **in WSL2**, which is where `docs/tests.md:13` routes it: its `Unix/windows-gnu` line points at the Platform-gated verification section (`docs/tests.md:121-131`), and this host does not have the `x86_64-pc-windows-gnu` target installed while WSL2's toolchain does (verified — `rustup target add x86_64-pc-windows-gnu` + mingw-w64). Give the Linux-side run its own `CARGO_TARGET_DIR` (e.g. `~/.cache/tinio-wsl-target`): `/mnt/e/GitHub/tinio/target` already holds this host's Windows `msvc` artifacts, and sharing the directory mixes the two toolchains' output.
Expected: both clean — and `unsafe_code = "deny"` with one enumerated `allow` is what the Windows leg is there to prove, since the `windows-sys` block only compiles there.
Report the files pending commit.

---

### Task 5: Daemon mode

**Files:**
- Create: `crates/tinio-cli/src/daemon.rs` (this task replaces Task 3's stub of it; `mod daemon;` is already registered in `lib.rs` by that stub)
- Modify: `crates/tinio-cli/src/lib.rs` (only if the stub's signature needs adjusting), `crates/tinio-cli/Cargo.toml` (dep `daemonize` under `cfg(unix)`, `windows-sys` under `cfg(windows)`), `crates/tinio/tests/lifecycle.rs` (the process-level legs, Step 5 — package `tinio`), `specs/001-s3-local-server/contracts/cli.md` (the `--daemon` bullet, below)

**Interfaces:**
- Consumes: `Resolution` (Task 2) — `daemonize`'s arms need `state_dir` for the log, the resolved `root`, and the `state` file the parent's probe watches; the config path needs no parameter because `Resolution.config_path` already carries it.
- Produces: `tinio_cli::daemon::{detach_if_requested, child_argv}` with `pub fn detach_if_requested(args: &ServerArgs, resolution: &Resolution, provenance: &Provenance) -> Result<(), Error>` — **returns only in the daemon** (`Outcome::Child`), or immediately when `--daemon` was not asked for; the parent (`Outcome::Parent`) prints the readiness line and exits from inside this function (0 when the probe succeeded, 1 when it timed out). The parent's probe is **synchronous** (`control_channel_answers`, Step 3): `tinio_api::client::probe` is an `async fn` and this runs before `lib.rs` builds the process's one runtime, so the parent cannot call it and must not build a runtime to reach it. The `Provenance` is for the log file: both arms redirect to `resolve::log_file(&resolution, &args, provenance)`, which is the *only* place `--log-file`/`[log] server_log_file`/the format-derived default are ordered — this module keeps no second copy of that rule.

**A doc step belongs to this task, because `--daemon`'s meaning is its subject**: `contracts/cli.md`'s `--daemon` bullet gains the sentence decision 9 rules — `--daemon` means "start in the background and **wait for ready**": the parent stays alive, waits (bounded ~10 s) for the daemon's `state` file and a control-channel answer, prints the readiness line on stdout and exits 0, or exits 1 with a message naming the daemon's log file and `tinio status`. The bullet keeps its existing "no service manager integration in v1" correction to Task 7's wording, and the `stderr → server.log`/`server.json` contract stays as it is.

- [x] **Step 1: Write the failing test**

Append to `crates/tinio-cli/src/daemon.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_the_flag_nothing_detaches() {
        // The one branch this module owns end to end and can test without forking: no
        // `--daemon` means no daemonize call at all, and the function returns in the caller's
        // process. (The log file's rule is Task 2's `resolve::log_file` — this module has no
        // second one, which is what keeps the flag, the key and the default in one place.)
        let dir = tempfile::TempDir::new().expect("temp");
        let resolution = resolution_for(&dir.path().join("root"));
        detach_if_requested(&args_without_daemon(), &resolution, &Provenance::default()).expect("no daemon asked for");
    }

    #[test]
    fn the_child_argv_drops_the_daemon_flag() {
        let argv = child_argv(&["server", "/srv/data", "--daemon", "--port", "9000"]);
        assert_eq!(argv, ["server", "/srv/data", "--port", "9000"]);
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-cli daemon::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

```rust
//! `--daemon`: no supervisor, no console, no readiness on stdout from the daemon (decision 9).
//!
//! On unix `daemonize` double-forks, calls `setsid`, sets the umask and redirects the three
//! standard streams; on Windows this process spawns a detached child. Both funnel the
//! operational log into the state directory, and both must run before any thread exists.
//!
//! This function does **not** use `Daemonize::start()`, and the reason is measured, not
//! stylistic: `start()` exits the original process inside itself with the intermediate
//! child's exit code (`daemonize 0.5.0`, `src/lib.rs:343`), and that child's own exit
//! (`:378`) is unconditional — so with `start()` every refusal raised inside the daemon is
//! invisible and `$?` is always 0.
//!
//! `execute()` returns in the original process instead, but its signature is **not**
//! `Result<Outcome, _>`: it is `pub fn execute(self) -> Outcome<T>` (`:351`), whose arms
//! carry the result — `Outcome::Parent(Result<Parent, Error>)` and
//! `Outcome::Child(Result<Child<T>, Error>)` (`:186`) — so the parent branch is *matched*,
//! never merely called:

/// The child's argv: this process's own command line without `--daemon`.
///
/// The source is `std::env::args_os().skip(1)` — the same list the parser saw — never a
/// re-serialization of the parsed flags, so every spelling the operator used (an `=`-joined
/// value, a repeated `--api`, the config path) survives into the child unchanged.
pub fn child_argv<I, S>(argv: I) -> Vec<OsString>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    argv.into_iter()
        .map(|arg| arg.as_ref().to_os_string())
        .filter(|arg| arg != "--daemon")
        .collect()
}

/// The parent's readiness wait, in one place for both arms.
///
/// Bounded at ~10 s: it polls `resolution.state_dir` for `state`, then asks the control
/// channel it names whether a server answers. Ready prints the same readiness line the
/// foreground server prints — `listening on <host>:<port>`, taken from the `port` the `state`
/// file recorded and the host the resolution carries, and written with plain `println!` rather
/// than through any listener, since the daemon's own stdout is the log file — and returns
/// `Ok(())`; the timeout is `Error::Operational` naming the daemon's log file (the `log_file`
/// the arms below redirected to, see `detach_if_requested`) and `tinio status`.
fn await_ready(resolution: &Resolution, log_file: &Path) -> Result<(), Error> { /* … */ }

/// Whether the channel `state.control_name` names answers — **synchronously**, and that is
/// the whole reason this function exists rather than a call to the client.
///
/// `tinio_api::client::probe` is `pub async fn probe(state: &State) -> bool` (plan A's Task
/// 9), and this runs in the parent **before `lib.rs` builds the process's one runtime**
/// (decision 2), so there is no executor to `.await` on and building one here would be the
/// second runtime decision 2 forbids. A connect is what readiness needs and all it needs:
/// `std::os::unix::net::UnixStream::connect` on the recorded socket path, `std::net::
/// TcpStream::connect_timeout` on a recorded `http`/`https` bind, and on Windows the pipe
/// opened as a file (`std::fs::OpenOptions::new().read(true).write(true).open(control_name)`
/// — the named-pipe path `\\.\pipe\…` opens like one). The async probe stays the client
/// commands' and `doctor`'s; this is liveness, not a `/status` answer, and the difference is
/// stated here rather than left for a reader to discover.
fn control_channel_answers(control_name: &str) -> bool { /* … */ }
```

**The unix arm matches the outcome.** It calls `daemonize::Daemonize::new().working_directory("/").stdout(file).stderr(file).execute()` with the file opened from `state_dir.join(log_target(format))`, and then:

- `Outcome::Parent(Ok(..))` — the original process, which is the one the operator is waiting on: run `await_ready`, print the readiness line, `std::process::exit(0)`; on its `Err` print the timeout message and `exit(1)`.
- `Outcome::Parent(Err(err))` — daemonization itself failed in the original process; that is an `Error::Operational` naming `err` and exit 1.
- `Outcome::Child(Ok(..))` — **the daemon**: fall through and return `Ok(())`, because `detach_if_requested` returns only in the daemon (the contract Task 3's sequence relies on).
- `Outcome::Child(Err(err))` — the daemon could not complete its own setup (the second fork, the `chdir`, the stdio redirection); report it as `Error::Operational`. Its stdout is already the log file, so the message is recorded rather than shown — the parent has already gone, which is the one place decision 9 accepts a logged-only failure.

The Windows arm spawns `current_exe()` with `child_argv(std::env::args_os().skip(1))`, `creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)` and the same file for stdout and stderr; the parent then runs the same `await_ready`/readiness-line/exit sequence, and the child is the one that returns from this function. Both arms log the resulting pid at `info!`. Only the daemon returns from this function on unix; on Windows only the child.

**"The same file" is `resolve::log_file(&resolution, &args, provenance)` — one rule, one owner.** `--log-file` wins, then `[log] server_log_file` (`crates/tinio-config/src/schema/log.rs:150`), then the format-derived default **in the state dir** (`server.log` / `server.json`). The last step is the daemon's own and not the config's: an empty `server_log_file` means "stderr" for a foreground run (`schema/log.rs:129`), and a daemon has no stderr to write to, so the default is what it falls back to rather than the empty string. That is why `detach_if_requested` takes the `Provenance` (the flag's level-2 answer lives beside the config's) and why this module declares no `log_target` of its own — a second spelling of "where the log goes" is exactly the drift this plan's ownership check looks for. One consequence, stated rather than hidden: **a foreground run keeps the operational log on stderr**, which is what the subscriber writes today (`crates/tinio-server/src/log.rs:305-306`); honouring `--log-file` there needs a file sink in `build_subscriber` and is that crate's edit, not this phase's.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-cli daemon::`
Expected: PASS.

- [x] **Step 5: Add the process-level leg and run it**

*Deviations (reviewed and accepted): the legs end the daemon through plan A's `client::stop` rather than `tinio stop` (Task 8's stub), and they capture the daemon's output to files rather than through `Command::output()` — on Windows a detached child inherits the parent's std handles, so the piped form never returns. Both legs pass; the second takes the probe's full ~10 s bound by design.*

Append to `crates/tinio/tests/lifecycle.rs` (the facade package's test file, where `env!("CARGO_BIN_EXE_tinio")` and the `spawn` helper from Task 3 already live):

```rust
#[test]
fn daemon_mode_returns_only_once_the_server_is_ready() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");

    let started = Instant::now();
    let out = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .args(["--port", "0", "--daemon"])
        .output()
        .expect("daemon");
    assert!(out.status.success());
    // The parent waits for the daemon's readiness and exits 0 — the bound is the probe's
    // ~10 s plus slack, and the assertion that matters is the one after it: no polling in
    // the test, because the parent is the one that waited.
    assert!(started.elapsed() < Duration::from_secs(15), "within the parent's readiness bound");
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("listening on "), "{out:?}");

    let state = root.join(".tinio/state");
    assert!(state.exists(), "the daemon published its state before the parent returned");

    let out = Command::new(tinio()).arg("stop").arg(&root).output().expect("stop");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn a_daemon_whose_startup_fails_exits_one_and_says_so() {
    // The other half of the ruling: a refusal that happens after the fork is still visible
    // in the operator's `$?`. A second daemon on the same root cannot publish `state`, so
    // the parent's probe times out instead of reporting success.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let (_serving, _endpoint) = spawn(&root);

    let out = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .args(["--port", "0", "--daemon"])
        .output()
        .expect("second daemon");
    assert_eq!(out.status.code(), Some(1), "the daemon's refusal is not swallowed: {out:?}");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("tinio status"), "the message names the way to look: {text}");
}
```

Run: `cargo test -p tinio --test lifecycle daemon`
Expected: PASS. The second test takes the probe's full ~10 s bound, which is the behaviour it asserts.

- [x] **Step 6: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 6: systemd notifications and the unit's one author

**Files:**
- Create: `crates/tinio-cli/src/notify.rs` (this task replaces Task 3's stub of it; `mod notify;` is already registered in `lib.rs` by that stub), `packaging/tinio.service`
- Modify: `crates/tinio-cli/src/lib.rs` (only if the stub's signatures need adjusting), `crates/tinio-cli/Cargo.toml` (dep `sd-notify` under `cfg(unix)`), `specs/001-s3-local-server/contracts/cli.md` (the example unit's `Type=` line), `crates/tinio/tests/lifecycle.rs` (Step 5's socket leg — package `tinio`)

**Interfaces:**
- Consumes: nothing beyond `Error`.
- Produces: `tinio_cli::notify::{ready, stopping, watchdog, unit_text}` with `pub fn unit_text(program: &Path, args: &[OsString], state_dir: &Path) -> String`. `SERVICE_NAME` is **not** here: it has one definition and Task 7's `service.rs` owns it, because that is the module that hands it to the SCM dispatcher.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-cli/src/notify.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn the_unit_notifies_and_watches() {
        // `unit_text` is ungated — it is a string — so this leg runs on every platform.
        let text = unit_text(Path::new("/usr/local/bin/tinio"), &["server".into(), "/srv/tinio".into()], Path::new("/srv/tinio/.tinio"));
        assert!(text.contains("Type=notify"), "otherwise READY=1 is inert:\n{text}");
        assert!(text.contains("WatchdogSec="), "otherwise WATCHDOG=1 is inert:\n{text}");
        assert!(text.contains("ExecStart=/usr/local/bin/tinio server /srv/tinio"), "{text}");
        assert!(text.contains("WantedBy=multi-user.target"), "{text}");
    }

    #[test]
    fn the_checked_in_unit_is_this_rendering() {
        let checked_in = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../packaging/tinio.service"))
            .expect("packaging/tinio.service");
        let rendered = unit_text(Path::new("/usr/local/bin/tinio"), &["server".into(), "/srv/tinio".into()], Path::new("/srv/tinio/.tinio"));
        assert_eq!(checked_in, rendered, "the file an operator reads and the file a machine installs must be the same text");
    }

    /// `unix` only: `notify` is the `sd_notify`-backed half, and that dependency cannot even
    /// link on Windows (`sd-notify-0.5.0/src/lib.rs:30-34` imports `std::os::unix::*`).
    #[cfg(unix)]
    #[test]
    fn notifications_are_inert_without_a_socket() {
        // No `NOTIFY_SOCKET` in this process, so nothing is sent and nothing is reported:
        // a run off systemd is the normal case and not a failure. The assertion is the
        // *outcome* — each call answers `false` (no socket, so nothing left the process)
        // rather than merely not panicking — which is why `notify` returns the boolean.
        //
        // The boolean is this module's, not the crate's: `sd_notify::notify` itself answers
        // `Ok(())` when `NOTIFY_SOCKET` is absent (`sd-notify-0.5.0/src/lib.rs:179-181`), so
        // a helper that returned `Ok`-ness would answer `true` here and the assertion below
        // would be testing nothing. The one-argument signature is that crate's real one.
        assert!(std::env::var_os("NOTIFY_SOCKET").is_none(), "the harness clears it");
        assert!(!notify(sd_notify::NotifyState::Ready), "no socket, nothing sent");
        assert!(!notify(sd_notify::NotifyState::Stopping));
        assert!(!notify(sd_notify::NotifyState::Watchdog));
        // And the public entry points stay callable and silent on the same path.
        ready();
        stopping();
        #[cfg(unix)]
        assert!(watchdog_interval().is_none(), "no WATCHDOG_USEC, no heartbeat");
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-cli notify::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

```rust
//! What systemd is told, and the unit text it is told it by (design decisions 21 and 22).
//!
//! Every call here is conditional on the environment systemd provides, and the crate does
//! most of that itself: `sd_notify::notify` answers `Ok(())` **without sending anything**
//! when `NOTIFY_SOCKET` is absent (`sd-notify-0.5.0/src/lib.rs:179-181`), and
//! `sd_notify::watchdog_enabled` answers `None` without `WATCHDOG_USEC` — and it consults
//! `WATCHDOG_PID` too, so a child that inherited the two variables does not beat for a
//! watchdog that belongs to its parent (`:489-499`). Neither case is a failure and neither is
//! surfaced.

use std::path::Path;

// **The whole notification surface is `cfg(unix)`-gated, not just its body.** `sd_notify` is a
// unix-only dependency of this crate (`notify.rs` and `daemon.rs` are where the design puts the
// two), and the crate cannot even link elsewhere — `sd-notify-0.5.0/src/lib.rs:30-34` imports
// `std::os::unix::*`. So every `sd_notify::` path lives behind the gate and each Windows build
// gets a stub with the same signature, which is what lets `serve` call `ready()`/`stopping()`/
// `watchdog()` unconditionally.

/// The `READY=1` that follows readiness.
#[cfg(unix)]
pub fn ready() {
    let _ = notify(sd_notify::NotifyState::Ready);
}

/// The `READY=1` that follows readiness. Inert on Windows, where there is no systemd.
#[cfg(not(unix))]
pub fn ready() {}

/// The `STOPPING=1` that opens the drain window — called by the watcher `serve` spawns, so it
/// fires when the shutdown *begins* rather than when it has finished.
#[cfg(unix)]
pub fn stopping() {
    let _ = notify(sd_notify::NotifyState::Stopping);
}

/// The `STOPPING=1` that opens the drain window. Inert on Windows, where there is no systemd.
#[cfg(not(unix))]
pub fn stopping() {}

/// Sends one notification, answering whether a socket was there to send to.
///
/// The boolean is **this module's**, not the crate's: `sd_notify::notify`'s one-argument form
/// answers `Ok(())` both when the datagram was sent and when `NOTIFY_SOCKET` was simply
/// absent, so `Ok`-ness cannot distinguish "sent" from "nothing to send" — and the unit test
/// below is exactly the case that has to be assertable. `NOTIFY_SOCKET` is the check that can
/// tell them apart; the crate call is the send.
#[cfg(unix)]
fn notify(state: sd_notify::NotifyState) -> bool {
    if std::env::var_os("NOTIFY_SOCKET").is_none() {
        return false;
    }
    match sd_notify::notify(&[state]) {
        Ok(()) => true,
        Err(err) => {
            // A socket that is there and still fails to take the datagram earns one line.
            tracing::debug!(error = %err, "the systemd notification could not be sent");
            false
        }
    }
}

/// Starts the watchdog task when the environment asks for one.
#[cfg(unix)]
pub fn watchdog() {
    let Some(interval) = watchdog_interval() else {
        return;
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let _ = notify(sd_notify::NotifyState::Watchdog);
        }
    });
}

/// Starts the watchdog task when the environment asks for one. There is none on Windows.
#[cfg(not(unix))]
pub fn watchdog() {}

/// Half of the watchdog interval systemd asked for.
///
/// `sd_notify::watchdog_enabled()` already parses `WATCHDOG_USEC` and gates it on
/// `WATCHDOG_PID`; halving it is this function's whole job, and hand-parsing the variable
/// instead would silently drop the pid gate.
///
/// `#[cfg(unix)]` because `sd_notify` is a unix-only dependency here — its only caller is
/// `watchdog`'s unix arm, and the not-unix arm of `notify` above is the whole Windows story.
#[cfg(unix)]
fn watchdog_interval() -> Option<std::time::Duration> {
    sd_notify::watchdog_enabled().map(|interval| interval / 2)
}

/// The unit, rendered from the one template (decision 21).
pub fn unit_text(program: &Path, args: &[std::ffi::OsString], state_dir: &Path) -> String {
    let args = args.iter().map(|a| a.to_string_lossy()).collect::<Vec<_>>().join(" ");
    format!(
        "[Unit]\n\
         Description=tinio S3-compatible local storage server\n\
         After=network.target\n\
         \n\
         [Service]\n\
         Type=notify\n\
         WatchdogSec=30\n\
         WorkingDirectory={work}\n\
         ExecStart={program} {args}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        work = state_dir.display(),
        program = program.display(),
    )
}
```

`packaging/tinio.service` is the rendering at the canonical values (`/usr/local/bin/tinio server /srv/tinio`, `WorkingDirectory=/srv/tinio/.tinio`), checked in verbatim — the test above is what keeps it equal to the template. `WatchdogSec=30` in that rendering is a **default, not a policy**: the design says the value is the operator's to size, and nothing in `notify.rs` hard-codes 30 — the heartbeat's cadence comes from `WATCHDOG_USEC`, which systemd derives from whatever the unit says, so an operator editing the line to `WatchdogSec=60` gets a 30-second cadence with no code change. Say that in the template's doc comment, next to the line.

One sentence in the spec set still describes a unit that sends nothing, and it is corrected here because this step is where the unit gets its one author: `contracts/cli.md`'s "Example systemd unit" block says `Type=simple`, which is exactly the two-copies defect decision 21 names — a unit that says `simple` beside a binary that sends `READY=1` makes every notification inert with no error anywhere. The block becomes this rendering, the same text `packaging/tinio.service` carries (`Type=notify` with `WatchdogSec=`, `ExecStart=/usr/local/bin/tinio server /srv/tinio`), so the file an operator reads and the file a machine installs cannot be told apart. (The other stale sentence — "no service manager integration in v1" — is Task 7's, because Task 7 is what makes it false.)

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-cli notify::`
Expected: PASS — three tests on unix (`the_unit_notifies_and_watches`, `the_checked_in_unit_is_this_rendering`, `notifications_are_inert_without_a_socket`); two on Windows, where the third is `#[cfg(unix)]` and the filter matches nothing for it — say so when you report the step rather than counting three.

- [x] **Step 5: The notification leg, driven through the real socket**

Append to `crates/tinio/tests/lifecycle.rs` (package `tinio`; `env!("CARGO_BIN_EXE_tinio")` and the `spawn` helper live there) a `#[cfg(unix)]` test that binds a unix datagram socket, sets `NOTIFY_SOCKET` and `WATCHDOG_USEC` on the child, starts `tinio server --port 0`, and asserts the datagrams it receives begin with `READY=1` and that a `WATCHDOG=1` arrives within the interval; then `POST`s stop through `tinio stop` and asserts a `STOPPING=1`. The `#[cfg(unix)]` gate is required rather than cosmetic: there is no `NOTIFY_SOCKET` protocol to test on Windows, where `notify` is the `false`-returning no-op arm.

Run: `cargo test -p tinio --test lifecycle notify`
Expected: PASS on unix — one test, and it is the only leg that observes the datagrams; on a Windows host the `#[cfg(unix)]` gate makes this filter match zero tests, so report it as unrun there rather than as a pass.

- [x] **Step 6: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 7: The `service` verbs

**Files:**
- Create: `crates/tinio-cli/src/service.rs` (this task replaces Task 2's stub of it; `mod service;` is already registered in `lib.rs`), `crates/tinio-cli/tests/service.rs`
- Modify: `crates/tinio-cli/src/lib.rs` (the Windows dispatcher arm at the top of `run()`), `crates/tinio-cli/Cargo.toml` (dep `service-manager` with **`default-features = false`** — its default `encoding` feature pulls `encoding_rs` + `encoding-utils`, which this plan does not use and the ledger leaves off; `windows-service` under `cfg(windows)`; **`sha2` and `hex`** — the label's digest and its rendering — which the crate does not depend on today), `crates/tinio-cli/src/cli.rs` (the `ServiceArgs` wiring), `specs/001-s3-local-server/contracts/cli.md` (the `service` section and flag table), `specs/001-s3-local-server/spec.md` (the `service` FR), `specs/001-s3-local-server/research.md` (the "no service manager integration in v1" sentences and the rejected Windows-service row at `:210`)

**Interfaces:**
- Consumes: `unit_text` (Task 6), `Resolution` (Task 2), `service_manager::{ServiceInstallCtx, ServiceLabel, ServiceManager, ServiceLevel, ServiceManagerKind, TypedServiceManager}`.
- Produces: `tinio_cli::service::{run, label_for, level_for, install_context, permission_remedy, ServiceVerb}` with `pub fn label_for(root: &Path, name: Option<&str>) -> String` (its `root` is the **canonical** root — `resolve::resolve_root`'s output, which is what makes `./data` and `/abs/data` one service and not two), `pub fn level_for(user: bool) -> ServiceLevel` (the only place `--user` becomes a `ServiceLevel`), `pub fn install_context(root: &Path, state_dir: &Path, program: &Path, args: Vec<OsString>, label: &str) -> Result<ServiceInstallCtx, Error>`, and `pub fn permission_remedy(kind: ServiceManagerKind, level: ServiceLevel) -> &'static str`; plus `pub const SERVICE_NAME: &str = "tinio"`, the service name's **one** definition — the SCM dispatcher's entry point takes it (below), and `notify.rs` declares no second copy (Task 6's Produces drops it for exactly this reason).

**The level is the manager's, not the install context's** — and that is the crate's shape, not a preference. `service-manager 0.11.0`'s `ServiceInstallCtx` (`src/lib.rs:298-341`) is `{ label: ServiceLabel, program: PathBuf, args: Vec<OsString>, contents: Option<String>, username: Option<String>, working_directory: Option<PathBuf>, environment: Option<Vec<(String, String)>>, autostart: bool, restart_policy: RestartPolicy }` — **there is no `level` field and no `ServiceInstallCtx::new` constructor** (its only inherent methods are `cmd_iter`/`args_iter`, `:343-353`, and it derives no `Default`), so the context is a struct literal naming all nine fields, and the level travels on the manager: `TypedServiceManager::target_or_native` / `native()` (`src/typed.rs:69,95`) then `manager.set_level(level_for(args.user))?` (`src/typed.rs:57`; `ServiceManager::set_level`) before the verb. `target_or_native(None)` does not compile either — the parameter is `impl Into<Option<ServiceManagerKind>>` and a bare `None` has no type to infer — so `native()` is the call.

- [x] **Step 1: Write the failing tests**

`crates/tinio-cli/tests/service.rs`:

```rust
//! The `service` verbs' pure halves: the label, the install context, and the unit it carries.

use std::path::Path;

use service_manager::{ServiceLevel, ServiceManagerKind};
use tinio_cli::service::{install_context, label_for, level_for, permission_remedy};

#[test]
fn the_label_is_derived_from_the_root_and_overridable() {
    // The derivation is over the **canonical** root, so the two spellings of one
    // directory are one service: `label_for` is called with what `resolve::resolve_root`
    // returned, never with the operator's spelling.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("data");
    std::fs::create_dir_all(&root).expect("root");
    let canonical = root.canonicalize().expect("canonical");
    let derived = label_for(&canonical, None);
    assert!(derived.starts_with("tinio-"), "{derived}");
    assert_eq!(derived.len(), "tinio-".len() + 16, "16 hex characters, as the control pipe's name is");
    assert_eq!(derived, label_for(&canonical, None), "the same root always derives the same label");
    assert_eq!(derived, label_for(&root, None), "`<dir>/data` and its canonical form agree");
    assert_ne!(derived, label_for(&dir.path().join("other"), None));
    assert_eq!(label_for(&canonical, Some("my-tinio")), "my-tinio");
}

#[test]
fn the_install_context_carries_our_unit_and_the_server_argv() {
    let context = install_context(
        Path::new("/srv/data"),
        Path::new("/srv/data/.tinio"),
        Path::new("/usr/local/bin/tinio"),
        vec!["server".into(), "/srv/data".into()],
        "tinio-abc",
    )
    .expect("the install context");
    assert_eq!(context.program, Path::new("/usr/local/bin/tinio"));
    assert_eq!(context.args, vec![std::ffi::OsString::from("server"), std::ffi::OsString::from("/srv/data")]);
    assert!(context.autostart, "a service that does not come back after a reboot is not a service");
    let contents = context.contents.expect("our unit, not the crate's Type=simple template");
    assert!(contents.contains("Type=notify"), "{contents}");
    // The working directory is the **state dir** the run resolved, not the root: read-only
    // mode relocates it (decision 6), and a unit that hard-codes `<root>/.tinio` would send
    // a read-only deployment's relative paths into the tree the mode promises not to write.
    assert_eq!(context.working_directory.as_deref(), Some(Path::new("/srv/data/.tinio")));
    assert!(contents.contains("WorkingDirectory=/srv/data/.tinio"), "{contents}");
}

#[test]
fn the_user_level_asks_for_no_elevation() {
    // The level is the manager's, not the context's: `ServiceInstallCtx` has no `level` field
    // (`service-manager 0.11.0/src/lib.rs:298-341`) — the crate carries it on the manager
    // (`ServiceManager::set_level`) — so what `--user` maps to is asserted here, on the one
    // pure function that maps it, rather than on a context field that does not exist.
    assert_eq!(level_for(false), ServiceLevel::System);
    assert_eq!(level_for(true), ServiceLevel::User);
}

#[test]
fn a_permission_failure_names_the_remedy_not_the_errno() {
    // The risk row decision 22 answers: three supervisors report the same failure as
    // `EACCES` or "Access is denied", and passing that through tells the operator nothing.
    let unix = permission_remedy(ServiceManagerKind::Systemd, ServiceLevel::System);
    assert!(unix.contains("--user"), "the unix remedy is the user level: {unix}");
    let windows = permission_remedy(ServiceManagerKind::Sc, ServiceLevel::System);
    assert!(windows.contains("Administrator"), "the Windows remedy is elevation: {windows}");
    // Already at the user level, the remedy is not "pass --user again".
    let user = permission_remedy(ServiceManagerKind::Systemd, ServiceLevel::User);
    assert!(!user.contains("--user"), "the flag is already in effect: {user}");
    assert!(!user.contains("EACCES"), "never the raw error: {user}");
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-cli --test service`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

```rust
//! `tinio service` — the supervisor's half of running a server (design decision 22).
//!
//! `service-manager` speaks systemd, launchd and the SCM; the one thing it does not do is
//! carry the unit *we* need, so the systemd unit travels in `ServiceInstallCtx::contents`
//! (its default template writes `Type=simple`, under which our readiness and watchdog
//! notifications are inert). The Windows arm additionally hands the process to
//! `windows-service`'s dispatcher, which is the only way the SCM can start us at all.

use std::{ffi::OsString, path::Path};

use service_manager::{ServiceInstallCtx, ServiceLevel, ServiceManagerKind, ServiceLabel};

/// The service's name, in one place: `windows-service`'s dispatcher entry point takes it, and
/// `notify.rs` does not declare a second copy (Task 6).
pub const SERVICE_NAME: &str = "tinio";

/// The five verbs the contract's users need.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum ServiceVerb {
    /// Register the service with the platform's supervisor.
    Install,
    /// Remove it.
    Uninstall,
    /// Start it.
    Start,
    /// Stop it.
    Stop,
    /// Report its state.
    Query,
}

/// The service's label: derived from the root so several roots can be several services.
///
/// The input is the **canonical** root — `resolve::resolve_root` canonicalises, and every
/// caller passes what it returned — so `tinio service install ./data` and
/// `tinio service install /abs/data` name one service rather than two. The digest is
/// `sha2::Sha256` over the canonical path's bytes, hex-encoded and truncated to 16
/// characters, the same shape the control channel's own name uses.
pub fn label_for(root: &Path, name: Option<&str>) -> String {
    if let Some(name) = name {
        return name.to_string();
    }
    let digest = Sha256::digest(root.as_os_str().as_encoded_bytes());
    format!("tinio-{}", hex::encode(&digest[..8]))
}

/// The level `--user` selects, in one place — the manager carries it, not the context.
pub fn level_for(user: bool) -> ServiceLevel {
    if user { ServiceLevel::User } else { ServiceLevel::System }
}

/// The context every backend takes, with our unit text attached on the backends that have one.
///
/// `state_dir` is the **resolved** state dir, not `<root>/.tinio`: read-only mode relocates
/// it (decision 6), and it is what `WorkingDirectory=` must name — a unit pointing at the
/// served root would send every relative path into the tree that mode promises to leave
/// alone. `resolve::resolve_state_dir` is where the value comes from.
///
/// **A struct literal, not `ServiceInstallCtx::new`**: the crate has no such constructor
/// (`service-manager 0.11.0/src/lib.rs:343-353` carries only `cmd_iter`/`args_iter`) and no
/// `Default` to spread, so all nine fields are named, `restart_policy` included (`RestartPolicy`
/// does implement `Default` — `OnFailure`, `:189-193`). The label arrives as a `&str` because
/// that is what the operator types and what `--name` carries, and `ServiceLabel` parses it
/// through `FromStr` (`:262`), which is fallible in type terms and therefore a `Result` here
/// rather than an `expect` in library code (Global Constraints).
pub fn install_context(
    root: &Path,
    state_dir: &Path,
    program: &Path,
    args: Vec<OsString>,
    label: &str,
) -> Result<ServiceInstallCtx, Error> {
    let _ = root; // the canonical root is what `label` was derived from; nothing else here needs it
    let contents = crate::notify::unit_text(program, &args, state_dir);
    Ok(ServiceInstallCtx {
        label: ServiceLabel::from_str(label).map_err(|err| Error::Operational(format!("the service label `{label}` is not usable: {err}")))?,
        program: program.to_path_buf(),
        args,
        contents: Some(contents),
        username: None,
        working_directory: Some(state_dir.to_path_buf()),
        environment: None,
        autostart: true,
        restart_policy: Default::default(),
    })
}

/// The remedy a permission failure names, instead of the raw `EACCES` / "Access is denied"
/// the three supervisors each spell differently (design decision 22's risk row, and §9's
/// one-refusal-shape rule: what was asked for, which channel it came from, what would work).
///
/// The SCM arm answers "run as Administrator"; the unix arms answer `--user`, except when the
/// call already carries `ServiceLevel::User` — then the remedy names the ownership of the
/// unit's paths, because repeating a flag the operator has already passed helps nobody.
pub fn permission_remedy(kind: ServiceManagerKind, level: ServiceLevel) -> &'static str {
    /* the four arms, and no I/O */
}
```

`run(args)` resolves the root (as `status` does — canonicalised), then the state dir by the same `resolve::resolve_state_dir`, builds the label from the canonical root, takes the manager with `TypedServiceManager::native()` (`src/typed.rs:95` — `target_or_native(None)` does not type-infer), sets the level on **the manager** with `manager.set_level(level_for(args.user))?` (`ServiceManager::set_level`, `src/typed.rs:57`), and dispatches the verb with the `install_context` from above; the systemd/launchd backends take the `contents`, and the SCM backend ignores it. `ServiceManagerKind::WinSw` is never selected — if `native()` would pick it, the command says so and refuses rather than shipping a wrapper.

Every verb's failure path goes through `permission_remedy`: an `Err` from the backend whose kind is a permission denial is mapped to `Error::Operational` carrying the remedy, so `install` at the system level says "…needs root: pass `--user`, or re-run with elevation" on unix and "…needs Administrator: run this from an elevated prompt" on Windows — the driver of the whole mapping is `permission_remedy`, which is why it is pure and tested. A failure that is *not* a permission denial keeps its own message, and the raw error is still logged at `debug!` so the detail is not lost, only demoted.

The Windows dispatcher arm goes at the very top of `run()`, before the CLI parse:

```rust
#[cfg(windows)]
fn try_run_as_service() -> bool {
    // The dispatcher answers `Err` immediately when the process was not started by the SCM,
    // so an ordinary `tinio doctor` pays nothing for this.
    windows_service::service_dispatcher::start(crate::service::SERVICE_NAME, ffi_service_main).is_ok()
}
```

with `ffi_service_main` → `service_main(args)` → register the control handler (STOP/SHUTDOWN/PRESHUTDOWN → the shutdown channel; INTERROGATE → the current status) → `StartPending` → run Task 3's orchestration with the service's arguments → `Running` at the readiness point → drain → `Stopped`, mapping the exit code into the status report.

**Where the callback's `ServerArgs` come from is the one non-obvious part of that chain.** The dispatcher runs *before* the parse — that is exactly what `try_run_as_service()`'s position at the top of `run()` buys — so the SCM's entry point cannot inherit a `Cli` from it. It re-parses instead, over the argument vector the SCM started the service with (`SERVICE_NAME`'s registered args, which `tinio service install` set to the `server` argv): `service_main` runs the same **two-step** parse `try_run` uses — `Cli::command().try_get_matches_from(["tinio", "server", <root>, …])`, then `cli::provenance(&matches)`, then `Cli::from_arg_matches(&matches)`, both errors mapped to `Error::Usage` exactly as `try_run` does — and hands the resulting `ServerArgs` **and** the `Provenance` to Task 3's `run`/`serve`. The provenance is not optional here: `start::run`'s signature takes it, and the service arm is the same entry point with a different argv source. One parser, one argv shape, and the dispatcher's only decision is who supplies the vector — never a second command-line dialect for the service arm.

**The service arm owns its runtime call, and that is a sentence rather than a gap.** `run()` is the function the dispatcher has taken over, so its `on_runtime(start::serve(ready))` arm is unreachable on this path: `service_main` builds the process's one runtime itself, exactly once, by calling the same `on_runtime` helper (`pub(crate)` in `lib.rs`) with `start::serve(ready)` — the runtime is still built at the end of the sequence, after the SCM handshake and after `run`'s pre-runtime half, and there is still exactly one per process.

**A doc step belongs to this task, because `service` is its subject** *(decision 22's round-2 ruling)*: `contracts/cli.md` gains a `service` section and flag table (the five verbs, `--name`, `--user`, the derived label `tinio-<sha256(canonical root)[..16]>`, the elevation refusals and their remedies), `specs/001-s3-local-server/spec.md` gains the corresponding FR — the command appears in no contract and no FR today, and `contracts/cli.md:42` with `research.md:143` actively say the opposite — and `research.md:210`'s rejected "a Windows service" row gets a **dated note** rather than a rewrite, because the ledger records what was decided and why. The two "no service-manager integration in v1" sentences are corrected in Step 5 below.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-cli --test service`
Expected: PASS.

- [x] **Step 5: Record what CI cannot verify, and correct the two sentences that say this verb set does not exist**

The verbs themselves need a supervisor and elevation. Add a `## Service verbs (manual)` block to `docs/tests.md`: the exact commands for a Linux container with systemd (`systemctl --user` for `--user`), a macOS host (`launchctl print`), and a Windows host (`sc query`), plus what to assert (`systemctl is-active`, the unit's `Type=notify` line, `sc query` reporting `RUNNING` and `STOP_PENDING` on stop). This is the phase's honest verification gap: the label, the context, the remedy text and the unit text are unit-tested, and the platform handshake is a documented manual leg.

Two sentences elsewhere still assert that this phase's subject does not exist, and both are corrected in this step rather than left as a second truth: `contracts/cli.md`'s `--daemon` bullet ends "(no service manager integration in v1)" and `research.md`'s §21 Notes carries the same claim ("no service-manager integration in v1; systemd unit example covers Linux"). `tinio service` **is** that integration, so both sentences now say what is true: `--daemon` remains the no-elevation, no-supervisor, does-not-survive-a-reboot route (decision 22's operator ruling keeps both routes), and the service verbs are the supervised one; on Windows the SCM has no file form, so `tinio service install` is the only route there, while `packaging/tinio.service` stays the operator's own example on Linux — the rendered `Type=notify` unit Task 6 checks in, not the crate's `Type=simple` template.

- [x] **Step 6: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 8: `status` and `stop`

**Files:**
- Create: `crates/tinio-cli/src/commands/status.rs`, `crates/tinio-cli/src/commands/stop.rs`
- Modify: `crates/tinio-cli/src/commands/mod.rs`

**Interfaces:**
- Consumes: `tinio_api::client::{status, stop, probe, Report}`, `Resolution` (Task 2) and `resolve::{resolve_mode, state_dir_candidates}`.
- Produces: `tinio_cli::commands::{status::run, stop::run}` — both `pub async fn(DirArgs) -> Result<ExitCode, Error>`, both dispatched by `lib.rs` on the process's **one** runtime and neither constructing one of its own (design decision 2, and plan C's `doctor` is the fourth command on that same runtime), and both working through one shared `client_target(args) -> Result<(Resolution, PathBuf), Error>` that resolves root, mode and the candidate state dirs once.

- [x] **Step 1: Write the failing tests**

These are asserted through the binary in `crates/tinio/tests/lifecycle.rs` — the `tinio` package's test file, not `tinio-cli`'s, because `env!("CARGO_BIN_EXE_tinio")` and Task 3's `spawn` helper live there — and this task extends that file rather than creating a second one. Task 3's round trip already covers the happy path; what this task adds is the three failure shapes the contract names plus the two mode/state-dir legs, each spawning the binary:

**Every leg here runs the child with a state dir of its own, and `TINIO_STATE_DIR` is how.** The other candidate would otherwise be the derived `~/.tinio/roots/<sha256(canonical root)[..16]>/`, which is **created while it is computed** (`tinio_config::state::home_state_dir`'s `ensure_roots`); `TINIO_STATE_DIR` **replaces that candidate** whenever it is set (Task 2's `state_dir_candidates` rule — the variable replaces the *home-derived* one, in either mode, and never `<root>/.tinio`). So each spawn sets **`TINIO_STATE_DIR`** to the test's own `TempDir`; that is the only form that works on Windows, where `dirs 7` resolves the home through `SHGetKnownFolderPath(FOLDERID_Profile)` (`dirs-sys-0.5.0/src/lib.rs:150-173`) and reads neither `HOME` nor `USERPROFILE` — a leg that relied on them would still seed `%USERPROFILE%\.tinio\roots\`. Both variables are set beside it as the unix fallback. Nothing under the real profile is read, written or derived by these tests.

```rust
/// A `status` child for these legs, built on Task 3's `client_env`: `TINIO_STATE_DIR` is the
/// temp dir, so the candidate the resolver would otherwise derive from the developer's real
/// home is never computed (and never created) at all — and on Windows that is the only
/// mechanism that can say so, because neither `HOME` nor `USERPROFILE` is what
/// `dirs::home_dir()` reads. One definition of the environment, in `lifecycle.rs`.
fn client(dir: &Path, root: &Path) -> Command {
    let mut cmd = client_env(Command::new(tinio()), dir);
    cmd.arg("status").arg(root);
    cmd
}

#[test]
fn status_on_an_uninitialised_root_exits_one_with_a_message() {
    let dir = tempfile::TempDir::new().expect("temp");
    let out = client(dir.path(), dir.path()).output().expect("status");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stdout).contains("stopped"));
}

#[test]
fn stop_with_no_server_exits_one_with_a_message() {
    let dir = tempfile::TempDir::new().expect("temp");
    let out = Command::new(tinio())
        .arg("stop")
        .arg(dir.path())
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .output()
        .expect("stop");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("no running server"));
}

#[test]
fn status_reports_a_stopped_server_whose_state_file_is_stale() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
    std::fs::write(root.join(".tinio/state"), br#"{"version":1,"pid":999999,"token":"x","control_name":"/nonexistent/tinio-0123456789abcdef.sock","port":9000,"root":"/nonexistent","started_at":"2026-09-20T00:00:00Z"}"#).expect("seed");
    let out = client(dir.path(), &root).output().expect("status");
    assert_eq!(out.status.code(), Some(1), "the probe failed, so the server is not running");
    assert!(String::from_utf8_lossy(&out.stdout).contains("stopped"));
}

#[test]
fn a_client_finds_the_server_in_the_other_candidate_state_dir() {
    // The server runs read-write, so `state` is in `<root>/.tinio`. The client resolves in
    // read-only terms, so `<root>/.tinio` is *not* its first candidate — the relocated dir
    // is, and `TINIO_STATE_DIR` names it inside the temp tree — and the fallback is what
    // finds the server, naming the dir that answered.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let (_serving, _endpoint) = spawn(&root);
    let relocated = dir.path().join("state");

    let out = Command::new(tinio())
        .arg("status")
        .arg(&root)
        .arg("--read-only")
        .env("TINIO_STATE_DIR", &relocated)
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .output()
        .expect("status");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("running"), "{text}");
    // The full path, not the substring ".tinio" — both candidates contain that, so only the
    // resolved path says *which* one answered.
    let answered = root.join(".tinio").display().to_string();
    assert!(text.contains(&answered), "it names the state dir that answered ({answered}): {text}");
    assert!(
        !text.contains(&relocated.display().to_string()),
        "the first candidate held no server, so it is not the one named: {text}"
    );

    let out = Command::new(tinio())
        .arg("stop")
        .arg(&root)
        .arg("--read-only")
        .env("TINIO_STATE_DIR", &relocated)
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .output()
        .expect("stop");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn a_client_without_a_mode_flag_takes_the_root_s_own_state_dir_first() {
    // The ordering half of the same rule: with the server read-write and the client
    // unreserved, the first candidate is `<root>/.tinio` and it answers there.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let (_serving, _endpoint) = spawn(&root);

    let out = client(dir.path(), &root).output().expect("status");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    let answered = root.join(".tinio").display().to_string();
    assert!(text.contains(&answered), "the resolved path, not just `.tinio`: {text}");

    let out = Command::new(tinio())
        .arg("stop")
        .arg(&root)
        .env("TINIO_STATE_DIR", dir.path())
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .output()
        .expect("stop");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}
```

Neither leg creates anything under the developer's real profile, and `TINIO_STATE_DIR` is doing the work rather than the two home variables: it replaces the home-derived candidate in either mode (Task 2's rule), so `dirs::home_dir()` is never consulted — which matters because the derived form *creates* the parents it returns. The two home variables stay set so a future leg that resolves a home for another reason is still steered, and `TINIO_STATE_DIR` is what a reviewer should hold the tests to. **Plan C's `tests/doctor.rs` follows the same rule and its Global Constraints state it against `state_dir_candidates`**, so the two process-level suites cannot diverge on how a client is isolated.

The mode every one of these runs with is Task 2's chain and nothing new: `--read-only` beats `TINIO_READ_ONLY` beats `[server] read_only` (decision 4, and `.env` is not an input to it), which is what makes the two legs above differ only in the flag. `doctor` reaches the same resolution through the same `DirArgs` when plan C lands (decision 19), so there is one implementation of this and not three.

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio --test lifecycle status`
Expected: FAIL — the commands do not exist (the tests live in `crates/tinio/tests/lifecycle.rs`, hence `-p tinio`).

- [x] **Step 3: Write the implementation**

`status` prints the contract's five fields on one line each (`running|stopped`, endpoint, root, pid, started) and returns `ExitCode::SUCCESS` when `report.running` else `ExitCode::from(1)` — but the *message* about an uninitialised root or a failed probe goes to stdout, because the contract says the command reports the state rather than erroring out of it; only an unreadable state file is an `Err`. `stop` sends the request, then polls `client::probe` every 100 ms to a ~15 s bound, reporting success when the probe fails or `state` disappears and an unconfirmed exit otherwise; `Err(Error::Operational(...))` for "no running server" gives exit 1 through the shared mapping.

Both are `async fn`s, and both are awaited by `lib.rs` on the runtime it built — the state read, the probe and `stop`'s 100 ms poll are async work on the process's **one** runtime, and neither command has a runtime of its own to build, enter or shut down (design decision 2; the Global Constraints' one-runtime rule, which names `status` and `stop` explicitly and forbids `Builder::new_current_thread()` as the smaller-looking alternative).

Both commands, and plan C's `doctor` with them, resolve the way the server does and then **probe both state-dir candidates**:

- The mode is `resolve_mode` (flag > `TINIO_READ_ONLY` > `[server] read_only`), so a read-only root is looked for in its own state dir exactly as the server leaves it, and the root-writability question never enters a client command.
- The candidates come from `resolve::state_dir_candidates(root, resolved, read_only, override_dir, derived)` (Task 2's five-argument rule): the dir the ordinary chain resolved comes first, and if it holds no live server (`State::read` finds no `state`, or `client::probe` fails), the other candidate is tried — `<root>/.tinio` when the mode relocated, and otherwise the home-derived `~/.tinio/roots/<sha256(canonical root)[..16]>/`, which `TINIO_STATE_DIR` replaces when it is set. The pair is `[X, <root>/.tinio]` whenever the resolved dir is not the root's own; the two candidates coincide only when `TINIO_STATE_DIR` names `<root>/.tinio` itself, which Task 2's rule covers as the harmless degenerate input. This is the client-side half of decision 19: a server started before an operator edited `read_only`, or started with `--read-only` and stopped from a plain shell, is still found, and the report names the state dir that answered (one line beside the contract's five, which Task 9's contract step adds to `status`'s output list) so the operator can see which of the two it was. `status` and `stop` never delete either file — the plane owns `state` and the socket (Task 3, decision 12, `Plane::publish`/`Plane::shutdown`) — and `crates/tinio-cli` never joins those file names itself, because a client that unlinks one cannot know whether the server behind it is still live; the one exception is `doctor --fix` (plan C), which reclaims a stale socket through `tinio_api::bind::reclaim_stale` and a stale `state` through `tinio_api::State::remove`, only after a probe has proved no server answers for that root.
- The consequence for discovery, stated because it is a contract paragraph and not a detail: a read-only root legitimately has no `.tinio/` of its own, so the walk-up from the current directory cannot find it — `status`, `stop`, `doctor` and `tinio server` all require an explicit `DIR` for a read-only root, which is what `contracts/cli.md:17` already tells the operator.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio --test lifecycle`
Expected: PASS.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings` and `cargo clippy -p tinio --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 9: The sandbox seam

**Prerequisite — the P0 crates must already be on this branch, and this task must not vendor them.** `tinio-sandbox`, `tinio-sandbox-core` and the three platform backends were staged-and-uncommitted in the `.claude/worktrees/sandbox` worktree (branch `feat/sandbox`) when this plan was written, so the prerequisite was a hard one: without them `cargo check -p tinio-cli` cannot resolve `tinio_sandbox`. **They are in this tree now** — the sandbox set's P0 landed on `dev` on 2026-09-20 (`a091cce`, the `feat/sandbox` fast-forward) — so the check that opens this task is `cargo metadata --no-deps`, which lists `tinio-sandbox` today. **Do not vendor or copy them** — no `[patch]`, no path dependency pointing into the worktree, no second copy of the facade's re-export list, and no local stand-in crate. The facade is the dependency, `tinio_sandbox::` is the only path this module names, and the check before starting is that `cargo metadata --no-deps` lists `tinio-sandbox` in this tree.

**Files:**
- Create: `crates/tinio-cli/src/sandbox/{mod,linux,windows}.rs` (this task replaces Task 3's stub of it; `mod sandbox;` is already registered in `lib.rs`) — `mod.rs` is the shared half and the dispatch, `linux.rs` / `windows.rs` are the platform arms
- Modify: `crates/tinio-cli/src/lib.rs` (only if the stub's signatures need adjusting), `crates/tinio-cli/src/commands/start/{mod,args,run,serve}.rs` (Task 3's `confine`/`supervise` call sites), `crates/tinio-cli/Cargo.toml` (the non-optional `tinio-sandbox` dependency and the default-on `sandbox` feature), `crates/tinio/tests/lifecycle.rs` (the refusal legs, package `tinio`)

**Interfaces:**
- Consumes: `Resolution` (Task 2), `tinio_sandbox::{Sandbox, AccessMode, Adopted, Backend, Confinement, Child, Exposure, LaunchPlan, LISTENER_FD_PREFIX, adopt, is_listener_fd_arg, listener_fd_from_arg, supervise}` — the launcher surface is the facade's, name for name (`is_listener_fd_arg` and `listener_fd_from_arg` for the element, `adopt(args, stdin)` for the payload, `LISTENER_FD_PREFIX` for its spelling, `supervise(&mut Child, teardown)` for the launcher's wait). **The third determination — "this platform's backend is still a stub" — is *not* a question put to `Backend`**, and the plan must not pretend it is: the trait has exactly one method, `fn confine(&mut self, sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement>` (`crates/tinio-sandbox-core/src/backend.rs:53-61`), and all three backends answer `Error::Unsupported` **from inside it** — which `plan` runs before by design (§1 step f precedes step i) and which a Linux run never reaches at all, because Task 3's launcher arm is `#[cfg(any(target_os = "macos", target_os = "windows"))]`. So the determination is this module's own table, and `plan` refuses on it.
- Produces: `tinio_cli::sandbox::{filter_argv, exposures, resolve_flag, plan, confine, adopted_listener, Request, Channel}` — **the signatures are these and Task 3's call sites are written against them** (they are the ones the earlier draft left loose, and the loose version could not have compiled): `pub fn filter_argv(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<Adopted>), Error>`; `pub fn exposures(root: &Path, state_dir: &Path, access: AccessMode) -> Vec<Exposure>`; `pub fn resolve_flag(flag: bool, env: Option<bool>, file: bool) -> bool`; `pub fn plan(resolution: &Resolution, args: &ServerArgs) -> Result<Request, Error>` — the three static determinations, answering a request whose `.enabled` is false and whose `.exposures` is empty when no channel is set, and building the canonicalised exposure set (device files on Linux, the executable's directory read-only on Windows) when one is; `pub fn confine(resolution: &Resolution, request: &Request) -> Result<Option<Child>, Error>` — `None` for `Confinement::InPlace`, `Some(child)` for `Confinement::Supervise(child)`; and `pub fn adopted_listener(adopted: &Adopted) -> Result<tokio::net::TcpListener, Error>`. `pub struct Request { pub enabled: bool, pub channel: Option<Channel>, pub exposures: Vec<Exposure>, pub device_files: bool }` with `pub enum Channel { Flag, Env, DotEnv, Config }` is what the refusals name and what `confine` consumes — and `device_files` is a **field rather than a re-derivation**, because `Sandbox::from_exposures(exposures, device_files)` takes the flag as its second argument precisely so it cannot be dropped (`crates/tinio-sandbox-core/src/sandbox.rs:124`); `plan` is what decides it (`.device_files()` on Linux, nothing elsewhere) and `confine` passes it on; **`confine` is where the facade's `LaunchPlan` is built** (`LaunchPlan::for_launch(config_text).with_listener(listener)`, `tinio-sandbox-core/src/launcher/mod.rs:37-99`), because the payload's configuration text and the inherited listener are both facts of this process and the platform's `Backend::confine(&mut self, &Sandbox, &LaunchPlan)` (`crates/tinio-sandbox-core/src/backend.rs:60`) takes them together — nothing else in this plan builds one. **None of those nine names — `filter_argv`, `exposures`, `resolve_flag`, `plan`, `confine`, `adopted_listener`, `Request`, `Channel` and `BACKEND_IS_STUB` — is behind `#[cfg(feature = "sandbox")]`**; the module compiles in every build and the feature only changes which answers `plan` gives (refusal naming the build vs. the platform). `Cargo.toml` gains `tinio-sandbox` as a **non-optional** dependency and a default-on `sandbox` feature that gates behaviour, not linkage; the facade `crates/tinio` forwards it (Task 10).
- **`Adopted::config` is carried but not yet consumed, and that is stated rather than implied.** `LaunchedPayload`/`Adopted` carry the configuration text the launcher forwards unparsed (`launcher/mod.rs:115-128`), and sandbox design decision 18 makes the *child* re-derive its answers from those bytes rather than from a file it may not be able to read. This plan's half is to carry it (`Ready.adopted`, untouched by the resolution) and to hand the payload to the retargeted P1 task, which owns the child's re-derivation in `resolution`; until that lands, a confined child re-reads the file its `--config` names, which is correct for every unconfined run and is exactly the gap P1 closes.

**The stub table, and what flips it.** `sandbox.rs` carries the third determination as one item with one owner:

```rust
/// Whether this build's platform backend can honour a confinement request.
///
/// **A table here rather than a call into the backend**, because the backend cannot answer it
/// where the answer is needed: `Backend` has one method, `confine`, and every platform says
/// `Error::Unsupported` from inside it — while `plan` runs *before* `confine` by design (§1 step
/// f before step i) and on Linux no `confine` call happens at all. So the state of the three
/// platform crates is recorded here, at the one place that refuses on it:
///
/// - P1 (`tinio-sandbox-linux`), P2 (`-macos`), P3 (`-windows`): **still stubs**, so this is
///   `true` for every target and `--sandbox` is refused with the platform named.
/// - A plan that lands a real backend flips **its own arm** — the mixed state (Linux real,
///   macOS still a stub) is the reason this is a per-target match rather than one boolean — and
///   its task is the one that changes `the_refusal_names_the_platform`'s expectation.
///
/// **`true` is the fail-closed answer, and that is why the constant is spelled this way.** A
/// `cfg!`-keyed expression would have to answer `false` on a target with no backend crate at
/// all — and `false` here means "the confinement is available", which is the one outcome
/// decision 10 forbids. When the platforms land unevenly the shape becomes a per-target
/// `match`, and **every arm that flips to `false` flips
/// `the_refusal_names_the_platform`'s expectation in the same commit**, because the two are one
/// fact: the plan that lands a backend owns both.
pub const BACKEND_IS_STUB: bool = true;
```

Read it as `true` **today**, for all three. The refusal names the platform from
`std::env::consts::OS`, so the message stays true whichever arm is current.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-cli/src/sandbox/{mod,linux,windows}.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn the_listener_element_is_taken_out_of_argv() {
        let argv = vec!["server".into(), "/srv/data".into(), "tinio-sandbox-fd=7".into()];
        let (filtered, _adopted) = filter_argv(argv).expect("filter");
        assert_eq!(filtered, vec![std::ffi::OsString::from("server"), std::ffi::OsString::from("/srv/data")], "clap must never see the element");
    }

    #[test]
    fn an_in_root_state_dir_needs_no_second_exposure() {
        let root = Path::new("/srv/data");
        let exposures = exposures(root, Path::new("/srv/data/.tinio"), AccessMode::ReadWrite);
        assert_eq!(exposures.len(), 1);
        let exposures = exposures(root, Path::new("/home/u/.tinio/roots/h"), AccessMode::ReadOnly);
        assert_eq!(exposures.len(), 2);
        assert_eq!(exposures[1].access, AccessMode::ReadWrite, "state is always writable");
    }

    #[test]
    fn every_channel_collapses_to_one_answer() {
        // The first argument is clap's answer — the `--sandbox` flag and nothing else, because
        // the flag carries no `env` (Task 2's one-owner rule: `TINIO_SANDBOX` is
        // `sources::env_sandbox()`'s, since clap's `SetTrue` parser would reject `1`). The
        // second is that variable's answer — the process environment with `.env` already
        // loaded into it — and the third is `config.server.sandbox`.
        assert!(!resolve_flag(false, None, false), "nothing asked for it");
        assert!(resolve_flag(true, None, false), "the flag");
        assert!(resolve_flag(false, Some(true), false), "the variable");
        assert!(resolve_flag(false, None, true), "the config key");
        assert!(!resolve_flag(false, Some(false), true), "the variable beats the config file");
    }
}
```

*(2026-09-22)* The `resolve_flag` leg above is superseded like Task 2's paragraph: the landed signature is `resolve_flag(source: ValueSource, requested: bool, file: bool)` — clap's answer and its source, then the config key — and the leg drives `ValueSource::CommandLine`/`EnvVariable`/`DefaultValue` for the three channels. The refusals' own words are unchanged, and a feature-off build keeps `TINIO_SANDBOX` readable through a hidden argument, which is what `the_sandbox_variable_is_never_silently_ignored` still asserts.

plus a `#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]` test asserting the facade-less build does not compile — a compile-fail assertion is not expressible in a unit test, so this task instead asserts the *runtime* refusal on the three supported platforms while their backends are stubs, as a process leg in `crates/tinio/tests/lifecycle.rs` (package `tinio`, where `tinio()` and `spawn` live):

```rust
#[test]
fn sandbox_on_a_stub_platform_refuses_and_never_serves() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let out = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .args(["--port", "0", "--sandbox"])
        .output()
        .expect("sandboxed");
    assert!(!out.status.success(), "an unhonourable flag must not run unconfined");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("sandbox"), "{text}");
    assert!(!root.join(".tinio/state").exists(), "nothing served, so nothing published");
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-cli sandbox::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

*Deviation (reviewed and accepted): canonicalisation and the reparse-point refusal are **not** done in `plan` — they are `tinio-sandbox-windows`' `canonical_exposures` and `tinio-sandbox-macos`' `with_canonical_paths` (P2/P3's owning definitions). A CLI that resolved the paths first would resolve away the very link that refusal exists to catch, and would be a second definer of macOS's "add, do not replace" rule. The `start.rs` comments that cited `plan` as the canonicaliser were corrected to match. The same correction applies to the paragraph above about canonicalisation.*

```rust
//! The `--sandbox` seam: three channels, one decision, every refusal named (decision 10).
//!
//! What this module does *not* do is confine anything: `tinio-sandbox-linux` and its two
//! siblings own that, and until they land every platform refuses here rather than serving
//! without the confinement the operator asked for (sandbox design decision 5).
//!
//! **Compiled in every build, and not behind `#[cfg(feature = "sandbox")]`.** `tinio-sandbox`
//! is a non-optional dependency of `tinio-cli`, and the feature gates behaviour, not linkage:
//! the only thing it compiles out is the `--sandbox` field in `cli.rs` (Task 2), because a
//! channel the operator can reach by variable or config key cannot be compiled away and must
//! be *refused* instead of ignored.

use std::{ffi::OsString, path::Path};

use tinio_sandbox::{
    AccessMode, Adopted, Exposure, Sandbox, adopt, is_listener_fd_arg,
};

/// Splits the listener element out of argv, reading the launch payload when it was there.
///
/// The element's spelling, its predicate and the adoption are all the **facade's**:
/// `LISTENER_FD_PREFIX` and `is_listener_fd_arg` are what name and recognise it, and
/// `adopt(args, stdin)` is what reads the payload off the reader — there is no second
/// `LISTENER_PREFIX` constant here and no hand-rolled scan of the prefix, because a second
/// spelling is exactly how a launcher and its child drift apart.
pub fn filter_argv(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<Adopted>), crate::Error> {
    // `adopt` reads the element out of the original list — it applies `is_listener_fd_arg`
    // itself, and touches stdin only when the element is present — so it is called before
    // the element is dropped from the argv clap will see.
    let adopted = adopt(argv.iter().cloned(), std::io::stdin())?;
    let filtered = argv.into_iter().filter(|arg| !is_listener_fd_arg(arg)).collect();
    Ok((filtered, adopted))
}

/// The paths the confined server must reach (design §7).
///
/// `access` is the run's own mode, and it is the **same type** the resolution produced:
/// `tinio_sandbox::AccessMode` is `tinio_core::mode::AccessMode` re-exported by the facade —
/// the sandbox core re-exports it "so a consumer of the sandbox never has to know which
/// crate the vocabulary came from, and so that a value the read-only work already produced
/// is the value an exposure takes". There is one access type here, imported once; a second
/// import of the same type under another name would only invite a reader to think two
/// enums exist.
pub fn exposures(root: &Path, state_dir: &Path, access: AccessMode) -> Vec<Exposure> {
    let mut sandbox = Sandbox::new().expose(root, access);
    if !state_dir.starts_with(root) {
        sandbox = sandbox.expose(state_dir, AccessMode::ReadWrite);
    }
    sandbox.exposures().to_vec()
}

/// The three channels collapsed to one answer (decision 10).
///
/// `flag` is what clap resolved and **only** that: `--sandbox` carries no `env` attribute
/// (Task 2's one-owner rule, and the reason is measured — clap's `SetTrue` arguments parse
/// their values with `ValueParser::bool()`, so `TINIO_SANDBOX=1` would be a usage error
/// instead of a channel). The variable arrives separately, through
/// `sources::env_sandbox()`, read after the `.env` file has been loaded into the process
/// environment — so the second input is the environment channel as a whole, and the third is
/// `config.server.sandbox`. A `true` from any of them is the answer; which channel produced
/// it is what the refusals name.
pub fn resolve_flag(flag: bool, env: Option<bool>, file: bool) -> bool {
    flag || env.unwrap_or(file)
}
```

`plan()` folds the three channels — landed as `resolve_flag(provenance.sandbox, args.sandbox_flag(), config.server.sandbox)` since 2026-09-22, where the source is what names the channel a refusal reports, `args.sandbox_flag()` is clap's flag-or-variable answer and `config.server` is the public field of `Config` (`config.server.sandbox`, never a `server()` method) — and resolves them in one place, and the split from `confine` is deliberate — the plan and the confinement call have different owners and different builds:

- **The three static determinations are `plan()`'s, and they run before daemonization** (Task 3's `run()` calls `plan` before `detach_if_requested`), so every one of them reaches the shell: with the `sandbox` feature **off**, a `true` answer from `TINIO_SANDBOX` or `[server] sandbox` is `Error::Usage` (exit 2) naming the build; a channel this build does not honour is a startup error naming the channel; and a platform whose backend is still a stub — `BACKEND_IS_STUB` above, which is the only place that fact is recorded — is a startup error naming `std::env::consts::OS`. Putting them here rather than in the confinement call is what makes them *static* and, more importantly, makes them reachable in every build: on a Linux build Task 3's `#[cfg(any(target_os = "macos", target_os = "windows"))]` launcher arm compiles out, and the same refusals must still come from this code path — which is exactly why the stub state cannot be read off `Backend::confine`'s answer, a call that never happens on Linux.
- With the feature **on** and a channel set, it builds the `Request`: the `exposures` set from `exposures(root, state_dir, access)`, `.device_files()` on Linux, the executable's directory read-only **on Windows only** (sandbox design decision 22), and the OTLP conflict warned about. *(Canonicalisation and the reparse-point refusal are **not** in that list — that clause is the deviation note's, not this step's: the rule belongs to P2/P3's `canonical_exposures`/`with_canonical_paths`, and no target implements it yet.)* `Request` is a plain value — the `Sandbox` is built from it inside `confine`, which is where the platform's own flags (`device_files`, the Windows exposure) are already folded in.
- **`confine()` stays the entry-point sequence's call** (Task 3, §1 step i) and contains no determination: it rebuilds the set the request carries — `Sandbox::from_exposures(request.exposures.clone(), request.device_files)` — and the `LaunchPlan` (the payload's configuration text plus the inherited listener), makes the facade's platform call — `tinio_sandbox::platform().confine(&sandbox, &plan)`, whose `Backend::confine(&mut self, …)` returns the `Confinement` — and answers `None` for `Confinement::InPlace` ("carry on in this process", the Linux case) and `Some(child)` for `Confinement::Supervise(child)` (macOS and Windows). Task 3's launcher arm then drives the facade's **two-argument** `tinio_sandbox::supervise(&mut child, teardown)`, which waits for the child, runs the teardown on every path, and returns the child's `ExitStatus` — the borrow rather than the by-value form is deliberate (the caller reads `Child::terminal` afterwards), and the CLI's own teardown is empty because the platform's launch owns everything it set up (the callback cannot fail by contract, and fallible teardown belongs in the platform's wait closure — for the Windows ACEs there is none to run, since every grant is standing, sandbox design decision 15). Because the stub-platform determination is `plan()`'s, the refusal `crates/tinio/tests/lifecycle.rs` asserts (a `--sandbox` run on a stub platform exits non-zero naming the platform and never publishes `state`) is raised before `confine` is ever called — and stays raised on a platform where the call is compiled out.

The Linux-only options (`LinuxOptions` with its model and `TINIO_SANDBOX_MODEL`) are reached inside `#[cfg(all(feature = "sandbox", target_os = "linux"))]`, exactly as the sandbox design's decision 2 draws the boundary; the facade exports no platform options — and that is the only `cfg(feature = "sandbox")` in this file: nothing else in the module is gated on the feature.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-cli`, `cargo test -p tinio-cli --no-default-features`, and `cargo test -p tinio --test lifecycle`
Expected: PASS in all three. Four corrections to what "differ in exactly one way" would suggest: `--no-default-features` drops **three** features — `api`, `mem`, and the default-on `sandbox` this plan's Task 9 adds (the crate's default list is `["api", "mem"]` today) — and `mem` changes nothing observable in this crate's code, so the sentence must not claim it does; `api` off is **not** only the enum change Task 2's doc fixes — `tinio-api` is an optional dependency, so the plane half of `run`/`serve`/`Ready` (the bind, the state file, `publish`, `shutdown`, the metrics ticker, `api_options`) is `#[cfg(feature = "api")]` and compiles out with it, which is what makes `cargo check --workspace --all-targets` and `.github/actions/run-test-leg/action.yml`'s `--no-default-features` leg pass; `status` and `stop` become unknown subcommands (clap's own usage error at exit 2), and `doctor` keeps its whole report while `--dry-run`, `--fix` and the stale-state/socket leg are compiled out; and `sandbox` off is the difference that matters for the **refusal text** — without it the refusals are the usage error naming the build, reached through `TINIO_SANDBOX` or `[server] sandbox` because `--sandbox` is not in the clap surface at all; with it they name the platform whose backend is still a stub.

The env-var channel's leg goes to `crates/tinio/tests/lifecycle.rs` (package `tinio`, where `tinio()` lives), and it **distinguishes the two refusals rather than accepting either**. One test, one channel, and the point is that the variable is *never* silently ignored — the discrimination is what keeps decision 10's fail-closed rule honest:

```rust
#[test]
fn the_sandbox_variable_is_never_silently_ignored() {
    // `TINIO_SANDBOX` cannot be compiled away, so a build that cannot honour it must
    // refuse — and the refusal must say *which* reason it is: without the feature the
    // message names the build, with it the platform. A test that accepted either one would
    // pass just as well if a feature-off build started answering like a feature-on one.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let out = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .args(["--port", "0"])
        .env("TINIO_SANDBOX", "1")
        .output()
        .expect("sandboxed");
    assert!(!out.status.success(), "the variable is never ignored");
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("sandbox"), "names the channel it came from: {text}");
    if cfg!(feature = "sandbox") {
        // The facade forwards `tinio-cli/sandbox`, so this cfg! reads the same feature the
        // refusal does: feature on, so the build is not the reason — the platform is.
        assert!(text.contains("platform"), "the stub backend, not the build: {text}");
        assert!(!text.contains("build"), "the feature is compiled in: {text}");
    } else {
        assert!(text.contains("build"), "the feature-off refusal names the build: {text}");
        assert!(!text.contains("platform"), "without the feature the platform is moot: {text}");
    }
    assert!(!root.join(".tinio/state").exists(), "nothing served, so nothing published");
}
```

The two strings the branches match are the ones `plan` produces in Step 3 — the test is written against them in the same task, and the wording is fixed there rather than guessed here.

- [x] **Step 5: Write the FR and the contract prose**

`specs/001-s3-local-server/spec.md` gains the FR this feature has never had (its three channels, FR-016 precedence, the fail-closed rule, the composition with FR-023); `contracts/cli.md` gains the `--sandbox` row and `--config PATH`, the `--read-only` row for `status`/`stop`/`doctor` (the doctor section already mentions the mode while the flag tables omit it), and the `status` output's state-dir line Task 8 adds; `contracts/config.md` gains `[server] sandbox`, the `.env`-never-selects-the-mode rule (decision 4), and the `/metrics`-on-the-management-plane note replacing F10's interim. Cross-check each against the code as written — the documents describe what exists, and a sentence without a task behind it is the defect this repository's plan conventions exist to catch.

**The rest of this plan's doc orphans land here, because `--sandbox`'s siblings are the CLI surface this step is already writing.** Six edits, each with the sentence that is wrong today:

- **`contracts/config.md:89` and `:153`** still say the unix socket defaults to a fixed `tinio.sock`; decision 24 makes it `tinio-<sha1(canonicalize(root))[..16]>.sock` in the state dir (`[api.unix] path` still overrides it, absolute or relative), which is the name `state.control_name` records — and the reason is stated there: two roots sharing one `TINIO_STATE_DIR` must not collide on one socket.
- **`contracts/config.md:162`** carries the `--api` rule as "a flag replaces the matching transport's subsection"; it gains decision 7's second half — the **presence of any `--api` flag disables every transport the flag does not name**, and a config with **no `[api]` section at all** selects the derived local channel (Task 2's `api_options`, which is what the interop harness's `[s3]`-only fixture relies on).
- **`contracts/cli.md:40`'s per-scheme sentence says the opposite of decision 7** ("Flags override config **per scheme** … unmentioned transports keep their config"); it is rewritten to the presence rule above, so the contract and `config.md` stop contradicting each other.
- **`contracts/config.md:118`'s `TINIO_STATE_DIR` row** gains one sentence beside the read-only paragraph it already carries: the variable selects the state dir for a **read-only** run *and* replaces the **home-derived** candidate a client probes in either mode — the same escape-hatch reasoning, so a machine with no usable home needs no second variable — while it never replaces `<root>/.tinio`, and a read-write run still serves `<root>/.tinio`, whose reserved directory FR-020 and the `meta.redb` layout assume. This is Task 2's `state_dir_candidates` rule in the contract's words, and it is the sentence plan C's Global Constraints quotes.
- **`research.md`**: §24's dependency table gains this phase's rows — `clap`, `daemonize`, `signal-hook`, `nix`, `sd-notify`, `windows-service`, `service-manager` — **six new rows** beyond `clap`, which the ledger already carries, and the same six the design's Bookkeeping now counts (its earlier "four" counted the unix-only arms and is no longer in the document), each with the reasoning that Bookkeeping records — and `:210`'s rejected "a Windows service" row gets a dated note rather than a rewrite (Task 7's step).
- **`plan.md:186`**'s sentence about an in-memory backend when no directory argument is given is corrected to the contract's rule (a missing root is a usage error naming the remedy, `contracts/cli.md:17`), and **`data-model.md`**'s state-file list gains the `root` field plan A writes.

- [x] **Step 6: Lint and checkpoint**

Run: `cargo clippy -p tinio-cli --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 10: `serve.rs`'s retirement and the harness switch

**Files:**
- Delete: `crates/tinio-server/examples/serve.rs`
- Modify: `crates/tinio-server/Cargo.toml`, `crates/tinio-e2e/tests/steps/clients.rs`, `crates/tinio-server/tests/serve_read_only.rs`, `crates/tinio/tests/smoke.rs`, `crates/tinio/tests/lifecycle.rs` (the Windows console-handler leg, Step 3), `crates/tinio/Cargo.toml`, **`.github/actions/run-test-leg/action.yml`** (its two serve-example build steps) and **`.github/workflows/ci.yml`** (the serve-example build the interop legs share)

**Interfaces:**
- Consumes: Task 3's binary.
- Produces: nothing the library uses; this is the phase's hand-over.

- [x] **Step 1: Switch the two harnesses, credentials included**

**Prerequisite: the `tinio` binary must be built before either harness runs.** Both harnesses locate a *built* binary rather than asking cargo for one: `assert_cmd::cargo::cargo_bin("tinio")` resolves the path under `target/<profile>/` and fails with its own message when the file is not there. So the leg's first command is `cargo build -p tinio` (CI adds `--profile ci`, matching the profile the tests run under). **Three CI artifacts build the example and would all break on the deletion**, and all three are this step's edit: `.github/actions/run-test-leg/action.yml`'s two `cargo build --profile ci -p tinio-server --example serve` steps (one per mode, the `--no-default-features` leg keeping its flag), and `.github/workflows/ci.yml`'s "build serve example (interop harness)" step at the top of the `interop`/`interop-port` legs (`ci.yml:707-714`), which becomes `cargo build --profile ci -p tinio`. The comment above the action's steps explains why they exist, and its reasoning is unchanged: nextest's target selection cannot be relied on to leave the artifact where the test derives it.

In `crates/tinio-e2e/tests/steps/clients.rs` and `crates/tinio-server/tests/serve_read_only.rs`, replace the example-binary locator with `assert_cmd::cargo::cargo_bin("tinio")` and the spawn arguments with `server <root> --port 0 [--config <path>]`; keep `READY = "listening on "`, the environment clearing (`TINIO_SCANNER`, `TINIO_READ_ONLY`, `TINIO_STATE_DIR`) and the 30 s timeout as they are. The readiness line is unchanged by design (§8).

**Three things beyond the locator and the argv do move, and pretending otherwise is how this switch would land red.** The spawn helper also sets `TINIO_ACCESS_KEY=minioadmin` and `TINIO_SECRET_KEY=minioadmin` (below), and two assertions in `serve_read_only.rs` are written against the *example's* own strings rather than against the contract:

- `serve_read_only.rs:511-516` asserts stderr carries `read-only mode: state dir <path>`. The example prints it (`serve.rs:221-231`); the CLI's equivalent is Task 3's stderr summary line, and this leg's needle becomes that line — the assertion stays, the string moves to the CLI's one author.
- `serve_read_only.rs:730-733` asserts stderr contains `usage: serve`. Under the CLI an unknown argument is clap's usage error, mapped to `Error::Usage` and printed as `tinio: <clap message>`, so the needle becomes the `Usage: tinio server` line clap renders and the exit status stays 2.

Both edits are assertion needles, not behavioural changes: the readiness line, the exit codes, the environment clearing and the `--port 0` contract are all held identical across the switch, which is what keeps the cucumber scenarios unchanged.

**On the fixtures: the `--config` files these harnesses pass carry no `[api]` section, and that is fine only because of Task 2's absence rule.** `serve_read_only.rs`'s `Scratch::config_file` writes `version = 1` + `[server]`, and `clients.rs`'s `config_for` writes `version = 1` + `[s3]` — neither names a transport. `api_options` answers the derived local channel for a config with no `[api]` section at all (decision 7's reading), so `tinio server <root> --port 0 --config <fixture>` starts and publishes `state` exactly as the example did; without that rule every one of these legs would die on plan A's "no management transport is enabled". This paragraph is here so the switch's reviewer checks that rule rather than rediscovering it as a red suite.

**The credentials are the one thing that must be added rather than preserved.** Both harnesses sign every request with the fixed `minioadmin`/`minioadmin` pair, which the example accepted because it hard-coded it; the CLI reads `[auth]`, and a first start *generates* a random pair, so every scenario would fail with `SignatureDoesNotMatch` the moment the switch lands. The spawn helper therefore sets `TINIO_ACCESS_KEY=minioadmin` and `TINIO_SECRET_KEY=minioadmin` on the child — the two names `contracts/config.md:110` lists, not the `MINIO_*` names, which are the data plane's fallback and not this chain's — and Task 2's resolution applies them to `[auth]` (env → `.env` → config). The variables the helper already *clears* stay as they are; these two are added to the set it *sets*.

**On the readiness-timeout tension, stated so it is not read as agreement:** the harnesses allow **30 s** to see the readiness line, while Task 3's round trip asserts SC-005's **≤ 1 s**. Both are right and they measure different things — 30 s is the bounds for a loaded CI runner starting a cold binary through cucumber, 1 s is the product requirement measured on an idle host — but a run where the CLI takes 5 s to become ready would pass the harness and fail T055. Neither bound is edited here.

- [x] **Step 2: Run both suites**

Run: `cargo build -p tinio` then `cargo test -p tinio-server --test serve_read_only`
Expected: PASS — the same legs, now against the CLI.
Run: `cargo test -p tinio-e2e --test cucumber -- --retry 2`
Expected: PASS. This is the run that proves the switch: ~all of the suite's scenarios start the server through the helper that just changed.

- [x] **Step 3: The Windows console-handler leg**

Task 4's handler is the crate's only Windows signal path and, until this step, the only one with no leg of its own: the `#[cfg(unix)]` unit asserts the registration on unix, `docs/tests.md:129-131`'s `windows-gnu` clippy leg proves the arm *compiles*, and nothing proves it *reaches the shutdown channel* — which is the thing the contract requires of Ctrl+C and the console-close event (`contracts/cli.md:43`, T069).

Append to `crates/tinio/tests/lifecycle.rs` a `#[cfg(windows)]` leg that spawns the real binary and signals it the only way a Windows test can:

```rust
#[cfg(windows)]
#[test]
fn the_console_handler_reaches_the_shutdown_path() {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};

    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");

    // CREATE_NEW_PROCESS_GROUP is what makes the child reachable by a console control
    // event: GenerateConsoleCtrlEvent targets a process *group*, and without the flag the
    // child shares this test runner's group — signalling it would signal the runner.
    let mut child = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .args(["--port", "0"])
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    std::io::BufRead::read_line(&mut reader, &mut line).expect("readiness");
    assert!(line.starts_with("listening on "), "readiness line: {line:?}");

    // CTRL_C_EVENT is the one the handler shares with CTRL_CLOSE_EVENT. The close event
    // itself cannot be generated — it arrives when the console window closes — so this leg
    // drives the arm the handler body shares, and the handler's own doc comment records
    // that the two are one path with one bounded wait.
    assert_ne!(unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, child.id()) }, 0, "the console event");

    // A graceful shutdown, not a kill: exit 0, and the plane's `state` file removed — the
    // same two assertions Task 3's round trip makes for a signal on unix.
    let status = child.wait().expect("wait");
    assert_eq!(status.code(), Some(0), "a console control event is a graceful shutdown");
    assert!(!root.join(".tinio/state").exists(), "the plane removed its state on the way out");
}
```

`windows-sys` is a dev-dependency of the facade package for this leg (with `Win32_System_Console`, the same feature the crate itself declares), and the leg is the reason Task 4's handler doc comment can say the Windows arm is exercised rather than only compiled.

- [x] **Step 4: Delete the example, and settle where the `sandbox` feature really lives**

Remove `crates/tinio-server/examples/serve.rs`. Unlike the `[[bench]]` blocks in that manifest, there is **no `[[example]]` block to remove**: `examples/serve.rs` is auto-discovered by cargo, and the `required-features` block the design's §8 describes does not exist in `crates/tinio-server/Cargo.toml` today — so the file's deletion is the whole of the edit there. **There is nothing to move either, and the design's §8 sentence that says otherwise is wrong about this tree**: `crates/tinio-server` has no `sandbox` feature and no `tinio-sandbox` dependency (`rg sandbox crates/tinio-server` finds no match; the manifest's optional dependency is `tinio-api`), so nothing is "dropped from that crate" — Task 9's *addition* to `tinio-cli` (the non-optional dependency and the default-on feature) is the whole of it, and the design-side sentence owes a correction that this plan records as debt in Task 11 rather than editing another plan's document. One more place names the deleted command and is **Task 11's** edit rather than this one's: `docs/tests.md`'s CI-only legs table lists `cargo build --profile ci -p tinio-server --example serve` as the interop legs' prerequisite, and the docs step there updates it to the `tinio` build.

**The facade's ownership is split, and this task holds only its half.** `crates/tinio/Cargo.toml` gains the `sandbox` pass-through row, `sandbox = ["tinio-cli/sandbox"]`, beside `api` and `mem`, plus this phase's `[dev-dependencies]` (`tempfile`, for `tests/lifecycle.rs`) — the facade forwards a feature it does not own and never names `tinio-sandbox` itself, while `tinio-cli` holds the dependency and its default-on `sandbox` feature (Task 9). **Plan A owns that file's other lines**: the `tinio-api` pass-through rows (`openapi`, `tls`) and the `default` list, which its own task edits; this task edits neither, and plan A's task edits neither the `sandbox` row nor the dev-dependencies. One owner per line is what keeps one `sandbox` row from becoming two.

- [ ] **Step 5: Update the binary smoke test** — *not run this way (2026-09-22): the file this step edits was deleted rather than updated. `crates/tinio/tests/smoke.rs` is gone; the bare-invocation leg is `cli_lifecycle.feature`'s "A bare invocation is a usage error" scenario, driven by `crates/tinio-e2e/tests/steps/cli.rs` against the real binary, which is what the assertion below makes. `specs/001-s3-local-server/tasks.md`'s T055/T096 notes and `docs/tests.md`'s "CLI (tinio)" section record the move.*

`crates/tinio/tests/smoke.rs` asserts the new contract — and the assertion **replaces** the file's existing `binary_exits_success` test rather than being added beside it: `tinio` with no arguments now exits **2**, so the old test would fail, and keeping both would leave two contradictory statements about the same invocation in one file. The file keeps its module doc and its `Command` import, drops the old body, and gains:

```rust
#[test]
fn no_subcommand_is_a_usage_error() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_tinio")).output().expect("tinio");
    assert_eq!(out.status.code(), Some(2), "usage errors exit 2 (contracts/cli.md:13)");
    assert!(!out.stderr.is_empty(), "a usage error says what to do");
}
```

- [x] **Step 6: Run the workspace gate**

Run the `docs/tests.md` Standard acceptance block in full.
Expected: PASS. Report each command's output.

- [x] **Step 7: Lint and checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 11: Bookkeeping and the phase's ownership check

**Files:**
- Modify: `specs/001-s3-local-server/tasks.md`, `docs/tests.md`, `README.md`, `CONTEXT.md`
- **Not** modified here: `docs/superpowers/specs/2026-09-20-cli-integration-design.md`. Its four stale sentences (Step 2 below) were corrected **on the design side**, so no task in this plan holds an edit to it — adding it to this list would claim a write this phase does not make.
- **Not** modified here, and deliberately: `docs/superpowers/plans/2026-09-18-sandbox-p1-linux.md` and `docs/superpowers/specs/2026-09-18-sandbox-design.md`. Both files are in this workspace now (the sandbox P0 merge, `a091cce`), but they are the sandbox workstream's own, and its worktree still holds uncommitted edits to `2026-09-18-sandbox-p1-linux.md` — so an earlier ruling keeps those two edits out of this plan, and **P1 must make both redirections before that work starts**: the P1 plan's Task 7 becomes "the CLI's `start` command consumes the sandbox backend" (this plan's Task 9 lands the seam), its Task 8's harness migration runs against the `tinio` binary, and the sandbox design's §10 gains one sentence naming `crates/tinio-cli/src/commands/start.rs` as the sequence's home, its risk row "`serve` is the only entry point" is marked closed by this phase, and its Bookkeeping's "`tinio-server` gains the `tinio-sandbox` dependency" sentence moves to `tinio-cli`. This task records the debt; it does not edit the sandbox workstream's documents to pay it.

- [x] **Step 1: Tick the spec's tasks**

Tick **T055, T059, T066–T069, T071, T072, T078 and T143** in `specs/001-s3-local-server/tasks.md`. **T060–T065 are not this plan's and are not ticked here**: they are plan A's (`2026-09-20-cli-management-plane.md` — the state file and single-instance bind, the local channel, the TCP listeners, the router, the OpenAPI schema and the client), and plan A ticks them; the same applies to T056 (its management-plane integration test) and T075 (`/metrics`), which are plan A's ticks. Ticking another plan's tasks here would state that this phase delivered code this phase never wrote. The leftover sentence "T075 was plan A's" is the same statement and carries no action. **T087 stays unticked**: `--anonymous` is not part of this plan — US3's task wires the flag through `crates/tinio-cli/src/commands/start.rs` and `TINIO_ANONYMOUS` through `crates/tinio-config/src/sources.rs`, and this plan declares neither (Task 2's `ServerArgs` has no `--anonymous` field, and no task here touches anonymous semantics).

**The home root-state-dir GC is T144's, and the sentence that used to point at T076 is corrected — landed, not owed.** `tasks.md:153` shows T076 ticked: the read-only state *relocation* landed, and the GC stage inside `RepairKind::Full`'s scope was never part of it. The operator ruled that a **new US2 task, T144**, owns the GC, and the two places that named the wrong task now name the right one: `tasks.md` carries T144's row (the coverage reviewer's edit), and `crates/tinio-fs/src/cleanup.rs:14-15`'s comment reads "lands with US2 (T144)" in this working tree — the re-pointing is done, so this plan records a fact rather than a debt. What follows from it here: nothing in this plan is the GC's, `RepairKind::Full`'s doc keeps promising it, `doctor --fix` inherits it the day T144 lands, and this plan neither ticks T144 nor claims the stage. A reader who followed the old sentence would have gone looking for the GC in T076's delivered work and found a relocation.

T143's tick is backed by the tasks, not by the tick — name each clause's owner in the tick comment, because that is the difference between ticking a deferred clause and assuming it landed:

- **Clause 1, the root-side config read** (a pre-existing `<root>/.tinio/.tinio.toml` or `.tinio/config.toml` is read in read-only mode and never written; `.tinio.toml` wins when both exist): Task 1's `resolve_path` search order and `load_or_create`'s read-only guard, plus Task 2's `resolve::config_path` (which refuses a missing explicit `--config` with exit 2 rather than creating it).
- **Clause 2, the `.env` load from the relocated state dir after the mode resolution** (never before it, decision 4): Task 2's `resolve::server` order — `resolve_state_dir` first, `apply_env_file` second — with `resolve_mode`'s signature carrying no `.env` input at all, which is what makes the cycle decision 4 rules out unrepresentable rather than merely avoided.
- **Clause 3, the api plane's `state`, control socket and logs joining the resolved state dir**: Task 3's `plane.publish`/`plane.shutdown` on `resolution.state_dir` (plan A made the plane take the dir as a parameter, so this is wiring and not a second resolver), and Task 8's clients reading the same resolved dir by the same chain.

- [x] **Step 2: Record the sandbox set's two redirections, without editing another worktree**

The two redirections are the Files note's: P1's Task 7 and Task 8, and the sandbox design's §10 and Bookkeeping. They are **recorded here, in `specs/001-s3-local-server/tasks.md`**, as a note under the T143 paragraph — the P1 plan and the sandbox design are the sandbox workstream's (it is still editing `2026-09-18-sandbox-p1-linux.md` in its own worktree), and an earlier ruling took those two edits out of this plan, so this task's obligation is the debt row and not the cross-workstream patch. P1 makes the two redirections in its own worktree before it starts; this plan's contribution is that the requirement is written down where the next reader will find it.

**Four sentences in the design document were wrong about this tree or about this phase, and all four are corrected on the design side** *(plan-audit round; this plan records them so the phase's history stays legible, and deliberately leaves no debt row pointing at a sentence somebody else has already fixed)*: §8's "the `sandbox` feature plus its `tinio-sandbox` dependency **move** to `tinio-cli`" — `crates/tinio-server` never had either (Task 10 Step 4's evidence), so it is an addition, not a move; decision 4's parenthetical asking the credentials chain for "the `MINIO_*` fallbacks" — the aliases belong to the data plane's resolution (T084), and Task 1's `env_credentials` deliberately reads only the `TINIO_*` pair; the Bookkeeping's "**four** rows" for the dependency ledger, which is six; and decision 11's sentence that the contracts "said only that the operational summary goes to stderr" — superseded by the readiness sentence this phase writes into `contracts/cli.md` (Task 3), which is an addition to that contract rather than a correction of the design.

- [x] **Step 3: The docs**

`README.md`'s crate table stops calling `tinio-api`/`tinio-cli` facade passthroughs and gains a line for the `service` verbs; `CONTEXT.md` gains glossary entries for the management plane's state file, the three sandbox channels and the CLI's root discovery; `docs/tests.md` gains the CLI section (T055's legs, the notify socket leg, the daemon leg) and the manual service-verbs block from Task 7.

**One contract question is recorded here rather than answered, because it is not this plan's to answer.** `contracts/config.md:110`'s environment list names `TINIO_API_UNIX`, `TINIO_API_HTTP`, `TINIO_API_HTTPS` (`_CERT`/`_KEY` included) and `TINIO_ACCESS_LOG`/`TINIO_ACCESS_LOG_FORMAT`; Task 2 wires none of them (the api transports resolve from `[api]` and the `--api*` flags, and the access log has no flag of its own), so either the contract's list loses the names no build consumes or a later task wires them. This plan's contribution is to say so in `tasks.md` beside T078's correction, so the next reader finds a decision rather than a silence.

Two corrections belong to this step as well, because both are places where a document would otherwise describe something the phase did not deliver:

- **`docs/tests.md`'s CI-only legs table** names `cargo build --profile ci -p tinio-server --example serve` as the `interop`/`interop-port` prerequisite. Task 10 deletes that example, so the row becomes the `tinio` build there (`cargo build --profile ci -p tinio`), matching what Task 10 Step 1 puts into `.github/actions/run-test-leg/action.yml` and `ci.yml`.
- **`specs/001-s3-local-server/tasks.md`'s T078** still reads "Add the example systemd unit `packaging/tinio.service` (`Type=simple`, foreground)". What this phase delivers is **`Type=notify` with `WatchdogSec=`** — the unit `notify.rs` renders and `packaging/tinio.service` checks in (Task 6) — so the parenthetical is corrected to say so, and the tick's comment names Task 6 as the owner. A `Type=simple` unit beside a binary that sends `READY=1` is precisely the two-copies defect decision 21 names, and leaving the stale text ticked would recreate it in the spec set.

- [x] **Step 4: The ownership check**

The check is the one `docs/superpowers/README.md` describes — the file is in this worktree now, having arrived with the sandbox P0 merge (`a091cce`), so it is cited as a local path. For each family below, name the single owner and search the other documents for a second statement of the same fact; a hit is a defect to fix **now**, not in a follow-up:

- the shared symbols: `Plane`/`Options` (plan A), `Resolution` (this plan's Task 2), the shared `serve` signature (the sandbox plans' §4), `unit_text` (Task 6);
- the public surfaces: `tinio-cli`'s module list and `tinio-config`'s two new modules, each declared in exactly one File Structure table;
- the lists that must have one home rather than one copy: the transport exclusivity rule (plan A's `select`), the **local-channel absence rule** (Task 2's `api_options` — decision 7's "the tool's default"; check no other document restates it), the **state-dir candidate rule** (Task 2's `state_dir_candidates`, which plan C's Global Constraints quotes word for word — the two texts must match), the exposure set (Task 9's `exposures`), the refusal messages (Task 9's `plan`), the service label derivation (Task 7's `label_for`), the environment-variable names (*2026-09-22: `tinio-cli`'s `cli.rs` owns all nine — every one of them is one of its arguments' `env` attributes, the boolean switches' through their custom parser, so the help text and the variable are the same declaration; `tinio-config` keeps only `state::STATE_DIR_VAR`, because `parse_state_dir` refuses an empty value by that name*) and the socket-name derivation (plan A's `state::socket_path`);
- the shared files: which task owns each edit — `crates/tinio/Cargo.toml` is split by line (**this plan**: the `sandbox` row and the dev-dependencies; **plan A**: the `tinio-api` pass-through rows `openapi`/`tls` and the `default` list), and `crates/tinio-cli/src/lib.rs`, `crates/tinio-cli/src/cli.rs`, `crates/tinio/tests/lifecycle.rs` and `docs/tests.md` each have exactly one owner per edit;
- the enumerated `#[allow(unsafe_code)]` sites this phase closes with — `tinio-winsec`'s file DACLs (Task 1's; they were `tinio-config`'s ACL module until the 2026-09-22 extraction, which is why the module is gone), `tinio-api`'s `dacl.rs`, `tinio-cli`'s console handler (Task 4) and its `daemon.rs` launch-env setter, and `crates/tinio/tests/lifecycle.rs`'s console-event leg (Task 10) — **five** in all, one reason each; plan C's free-space probe is the sixth when plan C lands;
- the type names that cross a document boundary: `MetricsSource`/`Gauges`/`DEFAULT_TTL`, `AccessMode` (one type, re-exported by the sandbox facade), `Child`/`Confinement`/`supervise`, `LaunchPlan` (Task 9's `confine` builds it), `Server`/`Ready`.

- [x] **Step 5: Final checkpoint**

Report every file pending commit across both plans, and the acceptance output. Do not run any git write.

---

## Correction, 2026-09-23 (code-review round): the non-creating home candidate, and who hardens it

`resolve::resolve_state_dir` no longer takes the creating `home_state_dir` on its read-only arm: it takes the **non-creating** `tinio_config::state::home_state_dir_path`, and the same split answers the second candidate `commands::client_target` probes. A client (`status`, `stop`, `doctor`) asks *where a server would be* and must not create the tree it is asking about — the creating form minted `~/.tinio/roots/<hash>/` merely because a probe asked, which is what `crates/tinio/tests/doctor.rs`'s `a_read_only_client_creates_no_home_state_tree` pins.

What the split costs is the hardening `ensure_roots` performs — `<home>/.tinio` and `roots` at 0700, `refuse_symlink`, the "exists and is not a directory" refusal — so a leg that goes on to **write** the leaf calls `tinio_config::state::prepare_home_derived(root, state_dir)` first, a no-op unless the directory is this root's home-derived leaf. Its two callers are `server`'s startup (`commands/start/run.rs`, immediately before `create_dir_all`) and `doctor --fix`'s repair (`commands/doctor/mod.rs`, before `open_store`'s `Pass::Fix`, which may create the store); each is one line before the write it guards, and a leaf the operator named explicitly (`TINIO_STATE_DIR`, `[api.unix] path`) returns early, because it is not the derived one.

This plan's documents did not carry that change; they do now. Without the call a bare `create_dir_all` inherits the umask and follows a planted symlink, which is exactly what `ensure_roots` exists to prevent.
