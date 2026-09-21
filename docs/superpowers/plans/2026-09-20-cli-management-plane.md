# The Management Plane (`tinio-api`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill `crates/tinio-api` — the state file with single-instance enforcement, the four management transports (unix socket, Windows named pipe, TCP HTTP, TCP HTTPS), the token-authenticated router with `/status`, `/stop`, `/metrics` and `/openapi.json`, and the thin status/stop client the CLI will drive.

**Architecture:** The plane is built in two halves with a hard seam between them: a **synchronous bind** (which is what enforces single-instance — the control channel *is* the lock) and an **async serve** (axum over whichever transport was selected). `DataPlane::serve` already takes a `watch::Receiver<bool>`, so `/stop` and the CLI's signal handlers flip the same channel; the plane sends on it, the CLI owns it. Everything the plane writes lives under a **state directory passed in by the caller** (design decision 6), which is what makes read-only mode's relocation structural rather than a second resolver.

**Tech Stack:** Rust 2024, `axum` 0.8 (`Listener` impls for the unix socket and TLS; a manual `hyper_util` connection loop for the named pipe, because `axum::serve` cannot configure keep-alive), `hyper` 1 + `hyper-util` 0.1 (the pipe's connection loop and `TokioIo`; the status/stop client is `hyper::client::conn::http1` over the dialed stream), `tokio` 1, `prometheus` 0.14 (the registry is `default_registry()` — the families are global statics in `tinio-server`), `serde`/`serde_json`, `time` (RFC3339 for `started_at`), `getrandom` (the token), and **std listeners for every bound transport**: `tokio`'s own `bind`/`from_std` panic outside a runtime, and the CLI binds before it builds one (design §1 steps a–j precede k), so the bind is synchronous and the conversion happens inside `serve_transport`.

**Spec:** `docs/superpowers/specs/2026-09-20-cli-integration-design.md` — §3 (the module map), decisions 6, 7, 8, 12, 13, 17. The contract it implements is `specs/001-s3-local-server/contracts/management-api.md` (endpoints, token rule, transports, responses) plus `contracts/config.md:151-162` (transport exclusivity, `--api` semantics, the HTTPS cert/key rule). Read the spec and those two contract sections alongside this plan; nothing here restates them.

## Global Constraints

- **Perform no git write operations whatsoever, and never touch the index** (`CLAUDE.md` overrides the skill's commit step): every task closes with a checkpoint step that reports the files pending commit, and **does not run** `git add`/`git commit`/`git push`/`git restore`. The index stays untouched — no `git add` of any form.
- Code, comments, tests, commit messages and documents are always in English.
- **Documents are not hard-wrapped** — every paragraph and list item is one unbroken line (`docs/style.md`).
- **Test-first**: every task's first step is a failing test, and the red state is verified before the implementation is written (constitution IV).
- No `unwrap()` / `expect()` / `panic!()` in library code; tests may use them (constitution II). Async tests use `#[tokio::test]` / `async fn`, **never** `Runtime::block_on` (`docs/tests.md`).
- **No test may mutate the process environment** — under edition 2024 `std::env::set_var` is `unsafe`, and every crate here is `unsafe_code = "forbid"` — `tinio-api` excepted, and only by Task 1: the Windows DACL and the named pipe's raw creation need raw bindings, so that crate's lint line becomes `deny` with exactly one `#[allow(unsafe_code)]` site carrying its reason (the design's decision 20 pattern), the `#[cfg(windows)] mod dacl` of Task 1 — and every later Windows raw call (Task 2's pipe creation and adoption, Task 10's pipe read-back) is an added use of **that same site**, never a second one. Tests that need an environment variable (the `sd-notify` leg in the CLI plan, not here) spawn a subprocess.
- Every public item carries a doc comment with at least one runnable rustdoc example; `no_run` where the example has side effects (`docs/style.md`, constitution III). **The code blocks below omit their `# Examples` sections** — they show the signatures, the bodies and the invariants, and the implementer adds one runnable example per public item as they write it (`cargo test --doc` is the leg that runs them).
- Style (`docs/style.md`): compressed prose, imports of 3+ segments de-qualified (`clippy::absolute_paths` is warn-level and the gate is `-D warnings`), per-crate `Error` built on `thiserror`, `SmartDefault` for defaults.
- **The code blocks below are not swept for the imports rule.** Run `cargo clippy -p tinio-api --all-targets -- -D warnings` as each task's lint step and fix what it reports in place; most of it will be `std::`-qualified paths in test setup and error plumbing (expect roughly 20 sites across this plan's blocks). Do not relax the rule.
- **Execution**: subagent-driven — one fresh subagent per task, reviewed before the next starts. When a task's steps pass and its review is accepted, tick that task's step boxes (`- [x]`) in this file; a task still under review stays unticked. A step deliberately not run keeps its box unticked with a one-line note saying why.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `crates/tinio-api/Cargo.toml` | `axum`, `tokio`, `serde`, `serde_json`, `prometheus`, `getrandom`, `hex`, `sha1`, `time`, `hyper`, `hyper-util`, `http`, `http-body-util`, `tower`, `tracing`; `cfg(windows)`: `windows-sys` (the DACL and the named pipe's raw creation, the crate's one `unsafe` site — lint `deny`, not `forbid`); features `openapi` (`utoipa`), `tls` (`tokio-rustls`, `rustls-pemfile`); dev-dep `tempfile` | 1, 5, 6, 7, 9 |
| `Cargo.toml` (workspace) | pins for `axum`, `sha1`, `utoipa`, `tokio-rustls` (`default-features = false`, `ring` — not `aws-lc-rs`), `rustls-pemfile`, `http-body-util`, `windows-sys`, and the `tests/fixtures` note | 1, 6 |
| `crates/tinio-api/src/state.rs` (new) | the `state` file: `State`, token generation, read/write/remove, 0600 on unix and a current-user-only DACL on Windows, and the root-derived control-socket name | 1 |
| `crates/tinio-api/src/dacl.rs` (new) | `#[cfg(windows)]` only, the crate's **one** raw-Windows module and its one `#[allow(unsafe_code)]`: the file DACL install and read-back (Task 1), the DACL-carrying named-pipe creation and the raw-handle adoption (Task 2, Task 4) and the pipe's DACL read-back (Task 10) | 1, 2, 4, 10 |
| `crates/tinio-api/src/error.rs` | `Error::State { path, message }`; `Error::Transport(String)`; the `IntoResponse` mapping onto the existing `Unauthorized`/`Internal` variants | 1, 5 |
| `crates/tinio-api/src/bind.rs` (new) | the single-instance bind, **std listeners only** so it runs before the runtime exists: the unix stale-probe-then-unlink as one public `reclaim_stale(path, dry_run)` that `bind_unix` and plan C's `doctor` both call, and the Windows first-pipe-instance claim | 2 |
| `crates/tinio-api/src/router.rs` (new) | the router, the token middleware (the bind-address rule and the bearer alias), `/status`, `/stop` | 3 |
| `crates/tinio-api/src/transport.rs` (new) | the four transports, the `Plane`/`Options` surface and the three-choose-one resolution | 4, 5, 6 |
| `crates/tinio-api/src/metrics.rs` (new) | `/metrics`: the injected `MetricsSource`, the registry gather, and the reader task that runs the storage scan once per `DEFAULT_TTL` — never on a scrape | 7 |
| `crates/tinio-server/src/data.rs` | the interim data-plane `/metrics` route removed; `with_metrics` and `MetricsRefresh` retained | 7 |
| `crates/tinio-server/src/{lib.rs, metrics.rs, backend/mod.rs}` | the `MetricsRefresh` re-export the CLI names, and the live F10 docs the route's removal falsifies | 7 |
| `specs/001-s3-local-server/{contracts/config.md, contracts/s3-surface.md, quickstart.md, pipeline-spec.md, pipeline-spec-review.md, checklists/security.md}`, `docs/superpowers/specs/2026-09-05-s3-acl-owner-design.md` | the live contract/spec/doc prose the data-plane route's removal makes false | 7 |
| `crates/tinio-server/examples/serve.rs`, `crates/tinio-server/src/{data.rs, metrics.rs, backend/mod.rs}` | the interim route's `with_metrics` wiring and its F10 comments — stale from this task until plan B retires the file | 7 |
| `crates/tinio-e2e/tests/features/metrics.feature` | the data-plane `/metrics` scrape retired; the control-channel form returns with plan B's harness migration | 7 |
| `crates/tinio-api/src/openapi.rs` (new) | feature `openapi`: the utoipa document | 8 |
| `crates/tinio-api/src/client.rs` (new) | the thin client: read `state`, probe, `GET /status`, `POST /stop` — created early with its `pub(crate)` test helpers in Task 4, promoted and completed in Task 9 | 4, 9 |
| `crates/tinio-api/src/lib.rs` | the module list, the re-exports, `Plane`/`Options` (both defined in Task 5) | 1–9 |
| `crates/tinio-api/tests/management.rs` (new) | T056's matrix | 10 |
| `crates/tinio-api/tests/fixtures/{cert,key}.pem` + `README` (new) | the HTTPS leg's committed self-signed test pair, and the note that it is test-only | 6 |
| `specs/001-s3-local-server/contracts/management-api.md` | the transport/auth matrix and its rationale (the bind-address rule, the bearer alias), the `/metrics` row (`:23`) and the derived socket name (`:5`) | 3 |
| `crates/tinio/Cargo.toml` | the facade's feature pass-through rows for `openapi` and `tls` and the two additions to its `default` list — **this plan owns those lines only**: plan B owns that crate's `[dev-dependencies]` and the `sandbox` row | 10 |
| `docs/tests.md`, `specs/001-s3-local-server/tasks.md` | the new legs; T056/T075 | 10 |

---

### Task 1: The state file

**Files:**
- Create: `crates/tinio-api/src/state.rs`
- Modify: `crates/tinio-api/src/lib.rs` (module list), `crates/tinio-api/src/error.rs`, `crates/tinio-api/Cargo.toml`, `Cargo.toml` (workspace pins)

**Interfaces:**
- Consumes: nothing.
- Produces: `tinio_api::state::{State, STATE_FILE, STATE_VERSION, generate_token, path, socket_path}`; `State { version: u32, pid: u32, token: String, control_name: String, port: Option<u16>, root: PathBuf, started_at: String }` with `State::new(control_name, port, root) -> Result<State, Error>`, `State::write(&self, state_dir: &Path) -> Result<(), Error>`, `State::read(state_dir: &Path) -> Result<Option<State>, Error>`, `State::remove(state_dir: &Path) -> Result<(), Error>`, and `socket_path(state_dir: &Path, root: &Path) -> Result<PathBuf, Error>` — the unix control socket's **default** path, `tinio-<sha1(canonicalize(root))[..16]>.sock` in the state dir, derived from the root exactly like the Windows pipe's name (design decision 24) so two roots sharing one state dir cannot collide. `[api.unix] path` overrides it (relative or absolute); whichever wins, its **resolved absolute path** is what goes into `state.control_name`, which is the source of truth `status`, `stop` and `doctor` read — no client re-derives the name, and no arm assumes a fixed `tinio.sock`.
- Produces, and **not** as prose the implementer may drop — the Windows DACL pair, `#[cfg(windows)]` and both in the crate's one raw-Windows module (`crates/tinio-api/src/dacl.rs`, registered as `#[cfg(windows)] pub mod dacl;` — `pub`, not `pub(crate)`: the module doc below says why), which is what makes Step 5's "seven on Windows" and Task 10's DACL legs true: `dacl::restrict_to_current_user(path: &Path) -> Result<(), Error>` (the install every `state` write runs, before a byte is written) and its read-back twin `dacl::acl_is_current_user_only(path: &Path) -> Result<bool, Error>`, which is this same task's so the DACL can be asserted rather than trusted — it reads the file's DACL back and answers whether every ACE's trustee is the current user's SID, and Task 10's Windows-gated leg calls it. **The module is the crate's single `#[allow(unsafe_code)]` site and Tasks 2, 4 and 10 add to it rather than opening a second one** — the named pipe's creation carries a DACL too (Task 2), so the pipe would otherwise have needed a second site; the design's decision 20 pattern is one enumerated site per crate, and this is it.

**Execution record (2026-09-20):** steps 1–6 ran and the review passed. Two amendments landed on `State::write`'s DACL path and the code in `crates/tinio-api/src/{state.rs,dacl.rs}` is the authority where it differs from the blocks below: the install passes `PROTECTED_DACL_SECURITY_INFORMATION` (without it Windows merges the parent directory's inheritable ACEs back in, so the file read back as *not* current-user-only), and the temp file is now created fresh under that DACL (`dacl::create_restricted`, `CREATE_NEW` after an unlink) with `restrict_to_current_user` re-asserted before the first byte, because `CreateFileW` with `CREATE_ALWAYS` applies `lpSecurityAttributes` only when it actually creates the file.

- [x] **Step 1: Write the failing tests**

Create `crates/tinio-api/src/state.rs` with only this module body:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;

    fn dir() -> TempDir {
        TempDir::new().expect("temp dir")
    }

    /// The name a run records: the derived absolute socket path (design decision 24) —
    /// there is no fixed `tinio.sock` default to fall back on.
    fn control(state_dir: &Path, root: &Path) -> String {
        socket_path(state_dir, root)
            .expect("socket path")
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn round_trips_through_the_file() {
        let (dir, root) = (dir(), dir());
        let name = control(dir.path(), root.path());
        let state = State::new(&name, Some(9000), root.path()).expect("new");
        state.write(dir.path()).expect("write");

        let read = State::read(dir.path()).expect("read").expect("present");
        assert_eq!(read.version, STATE_VERSION);
        assert_eq!(read.pid, std::process::id());
        assert_eq!(read.token, state.token);
        assert_eq!(read.control_name, name);
        assert_eq!(read.port, Some(9000));
        assert_eq!(read.root, root.path());
        assert_eq!(read.started_at, state.started_at);
    }

    #[test]
    fn the_socket_name_is_derived_from_the_root() {
        let state_dir = dir();
        let (a, b) = (dir(), dir());

        let first = socket_path(state_dir.path(), a.path()).expect("a");
        let second = socket_path(state_dir.path(), b.path()).expect("b");
        assert_ne!(first, second, "two roots sharing one state dir must not collide");
        assert_eq!(first.parent(), Some(state_dir.path()), "the socket lives in the state dir");
        let name = first.file_name().expect("name").to_string_lossy();
        assert!(name.starts_with("tinio-") && name.ends_with(".sock"), "{name}");
        assert_eq!(
            name.len(),
            "tinio-".len() + 16 + ".sock".len(),
            "the hash prefix is 16 hex characters: {name}"
        );
        assert!(first.is_absolute(), "the recorded control name is absolute: {}", first.display());

        // The name is a function of the *canonical* root: any spelling of the same root
        // derives the same socket, and a root that cannot be canonicalized is an error
        // rather than a silently different name.
        let leaf = a.path().file_name().expect("leaf");
        let respelled = a.path().join("..").join(leaf);
        assert_eq!(socket_path(state_dir.path(), &respelled).expect("respelled"), first);
        assert!(socket_path(state_dir.path(), Path::new("/no/such/root")).is_err());
    }

    #[test]
    fn an_absent_file_is_none_rather_than_an_error() {
        let dir = dir();
        assert!(State::read(dir.path()).expect("read").is_none());
    }

    #[test]
    fn a_malformed_file_is_an_error_naming_the_path() {
        let dir = dir();
        std::fs::write(crate::state::path(dir.path()), b"{not json").expect("seed");
        let err = State::read(dir.path()).expect_err("malformed");
        assert!(format!("{err}").contains("state"), "names the file: {err}");
    }

    #[test]
    fn an_unknown_version_is_refused_naming_both_versions() {
        let (dir, root) = (dir(), dir());
        let mut state = State::new(control(dir.path(), root.path()), None, root.path()).expect("new");
        state.version = STATE_VERSION + 1;
        state.write(dir.path()).expect("write");

        let err = State::read(dir.path()).expect_err("a future version is not ours to read");
        let text = format!("{err}");
        assert!(text.contains(&(STATE_VERSION + 1).to_string()), "names the file's version: {text}");
        assert!(text.contains(&STATE_VERSION.to_string()), "names this build's version: {text}");
        assert!(text.contains("newer"), "names the remedy for a newer file: {text}");
        assert!(text.contains("remove"), "names the way out when no server is running: {text}");
    }

    #[test]
    fn tokens_are_long_and_distinct() {
        let a = generate_token().expect("token");
        let b = generate_token().expect("token");
        assert!(a.len() >= 64, "64 hex characters carry the contract's 32 bytes: {a}");
        assert_ne!(a, b);
    }

    #[test]
    fn remove_is_idempotent() {
        let (dir, root) = (dir(), dir());
        let state = State::new(control(dir.path(), root.path()), None, root.path()).expect("new");
        state.write(dir.path()).expect("write");
        State::remove(dir.path()).expect("remove");
        assert!(State::read(dir.path()).expect("read").is_none());
        State::remove(dir.path()).expect("removing nothing is not an error");
    }

    #[cfg(unix)]
    #[test]
    fn the_file_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let (dir, root) = (dir(), dir());
        State::new(control(dir.path(), root.path()), None, root.path())
            .expect("new")
            .write(dir.path())
            .expect("write");
        let mode = std::fs::metadata(crate::state::path(dir.path()))
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the token is in this file");
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-api state::`
Expected: FAIL — `cannot find type State` / unresolved module `state`.

- [x] **Step 3: Add the dependencies and the error variants**

`Cargo.toml` (workspace) — add to `[workspace.dependencies]` in alphabetical position: `axum = "0.8"`, `http-body-util = "0.1"`, `rustls-pemfile = "2.2"`, `sha1 = "0.11"` (the socket name's hash — `sha1`, not the state dir's `sha256`; `Cargo.lock:3338` already carries 0.11.0), `tokio-rustls = { version = "0.26", default-features = false, features = ["ring", "tls12", "logging"] }`, `utoipa = { version = "5.5", default-features = false, features = ["axum_extras", "macros"] }`, and `windows-sys = "0.61"` for the DACL and the named pipe (`Cargo.lock:4868` already has 0.61.2; its features are chosen at the `tinio-api` dependency site below — and if plan B has already pinned it, keep the single pin). **`tokio-rustls`'s default features are off on purpose**: they select `aws-lc-rs`, whose `aws-lc-sys` is a C/CMake/NASM build on every developer machine and which drags a second `windows-sys 0.52` into the lock — the same duplicate the ledger rejected `windows-service` 0.8.0 over (design decision 22) — while `ring` is a pure-Rust build with no C toolchain and leaves exactly one crypto provider compiled in, which is also what keeps `ServerConfig::builder()` from panicking on an ambiguous process-level provider. (`hyper`, `hyper-util`, `http`, `tower`, `tokio`, `serde`, `serde_json`, `prometheus`, `hex`, `time`, `moka`, `tracing`, `tempfile` are already pinned; this plan uses none of `moka`.)

`crates/tinio-api/Cargo.toml` — `[dependencies]` gains `axum.workspace = true`, `hex.workspace = true`, `prometheus.workspace = true`, `serde_json.workspace = true`, `sha1.workspace = true`, `time = { workspace = true, features = ["formatting"] }`, `tokio = { workspace = true, features = ["rt", "net", "time"] }`, `tracing.workspace = true`, and the CSPRNG: `getrandom = "0.3"`. It also gains its first **dev-dependencies**: `tempfile.workspace = true` — this task's tests, and Tasks 2, 4, 5 and 9's, all build a `TempDir`, and the workspace already pins the crate. The Windows arm of the DACL, and Task 2's named-pipe creation that has to carry it, need raw bindings, so the manifest also gains:

```toml
[target.'cfg(windows)'.dependencies]
windows-sys = { workspace = true, features = [
    "Win32_Foundation",
    "Win32_Security",
    "Win32_Security_Authorization",
    "Win32_Storage_FileSystem",
    "Win32_System_Threading",
] }
```

**The crate's lint line changes in this task.** `windows-sys` is raw bindings, so `crates/tinio-api/Cargo.toml`'s `[lints.rust] unsafe_code = "forbid"` becomes `"deny"` with exactly **one** `#[allow(unsafe_code)]` site — the module-level attribute on `crates/tinio-api/src/dacl.rs` (`#[cfg(windows)] pub mod dacl;` in `lib.rs`), which carries the one reason and covers every helper in it: `dacl::restrict_to_current_user` (the file install) and `dacl::acl_is_current_user_only` (the file read-back) here, and Task 2's pipe creation, Task 4's raw-handle adoption and Task 10's pipe read-back later — which is the same pattern the design's decision 20 established for the CLI's console handler and `docs/cargo.md` records. **No second site is opened anywhere in this plan, and no other `unsafe` appears outside that module**: the crate's callers see safe wrappers, and each wrapper's invariant is the one Windows imposes — the calls are checked at every step and a failure is an `Error`, never a panic.

Features:

```toml
[features]
default = []
openapi = ["dep:utoipa"]
tls = ["dep:tokio-rustls", "dep:rustls-pemfile"]

[dependencies.utoipa]
workspace = true
optional = true

[dependencies.tokio-rustls]
workspace = true
optional = true

[dependencies.rustls-pemfile]
workspace = true
optional = true
```

`crates/tinio-api/src/error.rs` — the file already defines the two variants this plan's router leans on, with their contract mapping (`error.rs`, T021): `Unauthorized` (the `401 {"error":"unauthorized"}` the middleware answers with) and `Internal(String)` (a `500` whose detail stays in `Display` for the operational log and off the wire), beside `NotFound`, `Io` and `Storage`, all reachable through `Error::status()`/`body()`/`json_body()`. So this task adds only the state/transport pair, the two existing `match self` blocks that the new variants make non-exhaustive, and the one piece the router needs that does not exist yet, the axum response mapping. **The two matches are not optional and the compiler will not let them slide**: `status()` and `body()` (the file's existing `:52` and `:66`) both end in `Internal(_) | Io(_) | Storage(_) => …`, so a new variant stops the crate compiling until it is placed — both take the internal arm (`500`, `"internal server error"`), because a `state` file that will not parse and a transport that will not resolve are operational failures, never `4xx`:

```rust
    /// The state file could not be read, or its contents are not usable.
    #[error("state file {path}: {message}")]
    State { path: PathBuf, message: String },

    /// A management transport could not be set up or resolved.
    #[error("transport: {0}")]
    Transport(String),
```

The arms those two variants need in the existing matches (the `PathBuf` import at the top of `error.rs` comes with the first of them):

```rust
// in `Error::status`
Self::State { .. } | Self::Transport(_) => 500,

// in `Error::body`
Self::State { .. } | Self::Transport(_) => "internal server error".into(),
```

```rust
impl axum::response::IntoResponse for Error {
    /// The contract's status and body (`401 {"error":"unauthorized"}`, `404`,
    /// `500 {"error":"internal server error"}`); the detailed message is logged, not sent.
    fn into_response(self) -> axum::response::Response {
        tracing::debug!(error = %self, "management request failed");
        let status = axum::http::StatusCode::from_u16(self.status())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        (status, axum::Json(self.body())).into_response()
    }
}
```

- [x] **Step 4: Write the implementation**

Prepend to `crates/tinio-api/src/state.rs`:

```rust
//! The management plane's `state` file, and the token that guards `/status` and `/stop`.
//!
//! The file lives in the state directory the caller resolved (design decision 6), so
//! read-only mode's relocation needs no code here. Its presence means "a server claims
//! this root"; the effective single-instance lock is the control channel itself
//! ([`crate::bind`]), and this file is how `status` and `stop` find that channel —
//! `control_name` is the source of truth for it, so no reader re-derives a socket or pipe
//! name. The unix socket's default name is derived here too (design decision 24), from the
//! same root the Windows pipe's name comes from.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::Error;

/// The file's name inside the state directory.
pub const STATE_FILE: &str = "state";

/// The schema version this build writes and the only one it reads.
pub const STATE_VERSION: u32 = 1;

/// The token's length in bytes, before hex encoding (`management-api.md`: ≥ 32).
const TOKEN_BYTES: usize = 32;

/// The unix control socket's **default** path: `tinio-<sha1(canonicalize(root))[..16]>.sock`
/// in `state_dir` (design decision 24).
///
/// Derived from the root exactly like the Windows pipe's name, so two roots sharing one
/// state dir — the deployment `TINIO_STATE_DIR` exists to allow — cannot collide on the
/// socket path. `[api.unix] path` (relative or absolute) overrides it; whichever wins, the
/// caller records its **resolved absolute** path in [`State::control_name`], and a reader of
/// `state` never re-derives the name.
pub fn socket_path(state_dir: &Path, root: &Path) -> Result<PathBuf, Error> {
    let canonical = root.canonicalize().map_err(|err| Error::State {
        path: root.to_path_buf(),
        message: format!("the served root cannot be canonicalized: {err}"),
    })?;
    let digest = hex::encode(Sha1::digest(canonical.as_os_str().as_encoded_bytes()));
    Ok(state_dir.join(format!("tinio-{}.sock", &digest[..16])))
}

/// What a running server records about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    /// [`STATE_VERSION`] at the time of writing; a different value is refused on read.
    pub version: u32,
    /// The serving process's pid. Diagnostic only: nothing in this phase checks it for
    /// liveness — an absent control channel, not a stale pid, is what "stopped" means.
    pub pid: u32,
    /// The `X-Tinio-Token` value, hex-encoded [`TOKEN_BYTES`].
    pub token: String,
    /// The control channel's identity: the socket path (absolute — the derived default or
    /// the `[api.unix] path` override), the pipe name, or the TCP address. Clients read this
    /// and never re-derive it.
    pub control_name: String,
    /// The data plane's bound port (`None` before it is bound).
    pub port: Option<u16>,
    /// The served storage root, canonical.
    pub root: PathBuf,
    /// RFC3339, UTC.
    pub started_at: String,
}

/// The `state` file's path inside `state_dir`.
pub fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(STATE_FILE)
}

/// A fresh token: [`TOKEN_BYTES`] CSPRNG bytes, hex-encoded.
pub fn generate_token() -> Result<String, Error> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|err| Error::State {
        path: PathBuf::from("<csprng>"),
        message: format!("could not generate a token: {err}"),
    })?;
    Ok(hex::encode(bytes))
}

impl State {
    /// Records this process as the server for `root`, with a fresh token.
    pub fn new(
        control_name: impl Into<String>,
        port: Option<u16>,
        root: &Path,
    ) -> Result<Self, Error> {
        Ok(Self {
            version: STATE_VERSION,
            pid: std::process::id(),
            token: generate_token()?,
            control_name: control_name.into(),
            port,
            root: root.to_path_buf(),
            started_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .map_err(|err| Error::State {
                    path: PathBuf::from("<clock>"),
                    message: format!("could not format the start time: {err}"),
                })?,
        })
    }

    /// Writes the file atomically (a temp file beside it, then a rename), mode 0600 on unix
    /// and a current-user-only DACL on Windows.
    pub fn write(&self, state_dir: &Path) -> Result<(), Error> {
        fs::create_dir_all(state_dir).map_err(Error::Io)?;
        let text = serde_json::to_string_pretty(self).map_err(|err| Error::State {
            path: path(state_dir),
            message: format!("could not encode: {err}"),
        })?;

        let tmp = state_dir.join("state.tmp");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp).map_err(Error::Io)?;
        // The DACL is installed before a single byte is written, and the rename carries it:
        // the file that holds the token never exists with the directory's inherited ACL.
        #[cfg(windows)]
        crate::dacl::restrict_to_current_user(&tmp)?;
        file.write_all(text.as_bytes()).map_err(Error::Io)?;
        file.sync_all().map_err(Error::Io)?;
        fs::rename(&tmp, path(state_dir)).map_err(Error::Io)
    }

    /// Reads the file: `Ok(None)` when it is absent, an error when it is unreadable or was
    /// written by a schema version this build does not read (naming both versions).
    pub fn read(state_dir: &Path) -> Result<Option<Self>, Error> {
        let path = path(state_dir);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::Io(err)),
        };
        let state: Self = serde_json::from_str(&text).map_err(|err| Error::State {
            path: path.clone(),
            message: format!("could not decode: {err}"),
        })?;
        if state.version != STATE_VERSION {
            // The file's version and this build's, both by name, plus what the operator can
            // actually do about it: `version n is not the m this build reads` is true for an
            // older file and a newer one alike, so the remedy is named rather than guessed.
            return Err(Error::State {
                path,
                message: format!(
                    "version {} is not the {STATE_VERSION} this build reads: a newer tinio is \
                     needed, or remove this file if no server is running",
                    state.version
                ),
            });
        }
        Ok(Some(state))
    }

    /// Removes the file; absent is success.
    pub fn remove(state_dir: &Path) -> Result<(), Error> {
        match fs::remove_file(path(state_dir)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Error::Io(err)),
        }
    }
}
```

`crates/tinio-api/src/dacl.rs` — the crate's one raw-Windows module, and the two file helpers this task owns. **The `#[allow(unsafe_code)]` is on the module and nowhere else in the crate** (Step 3), and it is `pub` rather than `pub(crate)` for one reason that is worth writing down: Task 10's Windows leg is an *integration* test (`tests/management.rs` is a separate crate), so `acl_is_current_user_only` has to be reachable through the public path to be called at all — a `pub(crate)` helper would leave the DACL a claim the suite could not check. The module is `#[cfg(windows)]`, so nothing of it exists on any other platform:

```rust
//! The crate's one raw-Windows module: the DACL helpers, and — from Task 2 on — the named
//! pipe's DACL-carrying creation and adoption. Nothing here is reachable off Windows, and the
//! single `#[allow(unsafe_code)]` below is the crate's only `unsafe` site (design decision 20's
//! pattern, `docs/cargo.md`): every caller sees a safe wrapper whose checks are exhaustive and
//! whose failures are `Error`s, never panics, because the invariant is the one Windows imposes
//! rather than one this crate chooses.

#![allow(unsafe_code)]

use std::{
    os::windows::ffi::OsStrExt,
    path::Path,
    ptr,
};

use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, LocalFree},
    // The ACE's mask lives in the filesystem module (`FILE_ALL_ACCESS` is a file object's,
    // `0x1F_01_FF`), the four that build an ACE live in `Security`, its `Authorization`
    // submodule carries the trustee/ACL calls, and the split is why all three features are in
    // the list (`Win32_Security_Authorization` implies `Win32_Security`).
    Security::{
        ACL, DACL_SECURITY_INFORMATION, GetTokenInformation, NO_INHERITANCE, PSID,
        TOKEN_QUERY, TOKEN_USER, TokenUser,
        Authorization::{
            EXPLICIT_ACCESS_W, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW, SetNamedSecurityInfoW,
            TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
        },
    },
    Storage::FileSystem::FILE_ALL_ACCESS,
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

use crate::Error;

/// Installs a DACL that grants the current user full control and nothing to anyone else.
///
/// Every Windows `state` write runs this **before** the token is written, so the file never
/// exists with the directory's inherited ACL. A failure abandons the write.
pub fn restrict_to_current_user(path: &Path) -> Result<(), Error> {
    let user = current_user()?;                              // TOKEN_USER, freed on every path
    let mut entry = EXPLICIT_ACCESS_W {
        // `FILE_ALL_ACCESS` (`0x1F_01_FF`), never `GENERIC_ALL`: these bits are the file
        // object's, and the read-back below compares against this same constant.
        grfAccessPermissions: FILE_ALL_ACCESS,
        grfAccessMode: SET_ACCESS,
        grfInheritance: NO_INHERITANCE,                      // the whole list, not a delta
        Trustee: TRUSTEE_W {
            // A SID, not a name — and this is the pair of lines a reader gets wrong, because
            // `TRUSTEE_IS_NAME` is `1` and `TRUSTEE_IS_USER` is `1` too (as a `TRUSTEE_TYPE`),
            // so writing the type constant in the form field compiles and then makes
            // `SetEntriesInAclW` read the SID's bytes as a name string: `ERROR_NONE_MAPPED`,
            // and every Windows `state` write fails. `TRUSTEE_IS_SID` is `0`.
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: user.Sid as *mut u16,
            // `pMultipleTrustee` and `MultipleTrusteeOperation`, both defaulted: no trustee
            // chain (and `windows-sys` provides the `Default`).
            ..Default::default()
        },
    };
    let mut acl: *mut ACL = ptr::null_mut();
    let status = unsafe { SetEntriesInAclW(1, &mut entry, ptr::null_mut(), &mut acl) };
    if status != 0 {
        return Err(fail(path, "SetEntriesInAclW", status));
    }
    let wide = to_wide(path);
    let status = unsafe {
        SetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            acl,
            ptr::null_mut(),
        )
    };
    unsafe { LocalFree(acl as _) };                              // freed on both paths, before the branch
    if status != 0 {
        return Err(fail(path, "SetNamedSecurityInfoW", status));
    }
    Ok(())
}

/// Reads the file's DACL back and answers whether every ACE names the current user's SID with
/// full control — an absent or empty DACL is `false`, so "restricted" is never inferred from
/// silence. Task 10's Windows-gated leg is the caller: without it the DACL is a claim.
pub fn acl_is_current_user_only(path: &Path) -> Result<bool, Error> { /* GetNamedSecurityInfoW + ACE walk */ }
```

**The mechanism is the code, not a paragraph about it** — this is the file the implementer writes, and two things are named rather than pasted because they are small private helpers of the same module: `current_user() -> Result<CurrentUser, Error>`, the two-call `OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY)` + `GetTokenInformation(TokenUser)` pair (sized first, then read) that answers an owning guard — `CurrentUser { Sid: PSID, buffer: *mut c_void }` whose `Drop` `LocalFree`s the `TOKEN_USER` and `CloseHandle`s the token — which is why the code above can use `user.Sid` and why the SID cannot outlive the buffer; and `fail(path, which, status)` / `to_wide(path)`, the `Error::State { path, message }` constructor (the status in decimal plus its meaning) and the UTF-16 conversion every `*W` call needs. The read-back's body is the install's mirror image, elided for the same reason: `GetNamedSecurityInfoW(path, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, …)`, walk the returned ACL with `GetSecurityDescriptorDacl`/`GetAce`, answer `Ok(true)` only when every `ACCESS_ALLOWED_ACE`'s `grfAccessPermissions` is **`FILE_ALL_ACCESS`** (mask `0x1F_01_FF` — the same constant the install writes, so the round trip is a real round trip and a DACL written by another tool with `FILE_ALL_ACCESS` also passes; a read-back comparing against a different mask would answer `false` for exactly the DACLs it exists to recognise), no ACE is inherit-only, and every trustee `EqualSid`s the current user — `Ok(false)` for an empty or absent DACL, `Error::State { path, message }` for a status that is not `ERROR_SUCCESS`, and the returned descriptor `LocalFree`d on every path including the failures. Everything here is `unsafe` only inside this file, and every status is checked: a helper that cannot do its job answers an `Err`, never a panic. The import list above is the install's; the read-back adds its own (`GetNamedSecurityInfoW`, `GetSecurityDescriptorDacl`, `GetAce`, `ACCESS_ALLOWED_ACE`, `EqualSid`), and Task 2 adds `io` with `create_pipe`'s `io::Result` — each task keeps the list at exactly what its own code uses, since the crate's `-D warnings` gate fails on an unused import.

Add `pub mod state;` to `crates/tinio-api/src/lib.rs` in alphabetical position, the `#[cfg(windows)] pub mod dacl;` line beside it (the module of the block above — registered here because this is the task that creates it), and `pub use self::state::State;` beside the existing `pub use`.

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-api state::`
Expected: PASS — eight tests on unix, seven on Windows (the DACL arm's own assertion is Task 10's Windows-gated leg).

- [x] **Step 6: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets -- -D warnings` on this host, and the second leg — `cargo clippy --target x86_64-pc-windows-gnu -p tinio-api --all-targets -- -D warnings`, the one that compiles the DACL arm — **in WSL2**, which is where `docs/tests.md:13` routes it — its `Unix/windows-gnu` line points at the Platform-gated verification (WSL2) section, `docs/tests.md:121-131`: this host does not have the `x86_64-pc-windows-gnu` target installed, and WSL2's toolchain does (verified — `rustup target add x86_64-pc-windows-gnu` + mingw-w64, `docs/tests.md:129-131`). Give the Linux-side run its own `CARGO_TARGET_DIR` (e.g. `~/.cache/tinio-wsl-target`): `/mnt/e/GitHub/tinio/target` already holds this host's Windows `msvc` artifacts, and sharing the directory mixes the two toolchains' output.
Expected: both clean; the only lint-configuration change is `unsafe_code` going `forbid` → `deny` with the one `#[allow(unsafe_code)]` site.
Report the files pending commit. Do not run any git write.

---

### Task 2: The single-instance bind

**Files:**
- Create: `crates/tinio-api/src/bind.rs`
- Modify: `crates/tinio-api/src/lib.rs`, `crates/tinio-api/src/dacl.rs` (Task 1's module — this task adds the pipe creation and the DACL-carrying `SECURITY_ATTRIBUTES` builder to it), `crates/tinio-api/Cargo.toml` (`windows-sys` gains `Win32_System_Pipes`: `CreateNamedPipeW` and the `PIPE_*` mode/instance constants live there, while the `FILE_FLAG_*` open-mode bits and `FILE_ALL_ACCESS` come from `Win32_Storage_FileSystem`, which Task 1 already added)

**Interfaces:**
- Consumes: `crate::error::Error`, and (Windows only) `crate::dacl`'s pipe helper Task 1 opened the site for.
- Produces: `tinio_api::bind::{Channel, bind_unix, reclaim_stale, bind_pipe, SUN_PATH_MAX}` where `SUN_PATH_MAX` is the `#[cfg(unix)]` `pub const` plan C's socket-length check consumes (one owner for the platform constant, no second copy), and `Channel` is `#[non_exhaustive] pub enum Channel { Unix { listener: std::os::unix::net::UnixListener, path: PathBuf }, Pipe { name: String, handle: std::os::windows::io::OwnedHandle } }`, plus `Channel::identity(&self) -> String` (the bound socket path or the pipe name, for `State::control_name`) and `Channel::socket_path(&self) -> Option<PathBuf>`. **The listeners are `std` and the pipe is a raw handle on purpose, and it is the whole reason `Plane::bind` can be the single-instance lock before the runtime exists**: `tokio::net::UnixListener::bind` and `tokio::net::TcpListener::from_std` panic outside a runtime ("there is no reactor running, must be called from the context of a Tokio 1.x runtime"), `ServerOptions::create` reaches the same `PollEvented::new` through `NamedPipeServer::from_raw_handle`, and the CLI's sequence (§1 steps a–j, then k) binds before any runtime — so each transport is *created* with a runtime-free type here and *converted* inside `serve_transport`, where a runtime exists (Tasks 4, 5, 6). The unix arm carries its own path because the bind knows it: `identity` and `socket_path` read it rather than re-deriving one from `local_addr()`. The name is authored once, at the bind; `serve_local` (Task 4) reads it off the channel instead of taking it as a parameter. (**Amended 2026-09-21 by the operator's review of `bind.rs`: the `Channel` enum above is gone — each platform now has its own `Channel` struct selected by `#[cfg]`, because the two variants were mutually exclusive and every `match` therefore had exactly one reachable arm; `Channel::identity` and `Channel::socket_path` are unchanged, and the `## Post-plan review fixes (2026-09-21)` section at the end of this file carries the full record.**)

**Execution record (2026-09-20):** steps 1–5 ran and the review passed. One amendment: `create_pipe` sets `PIPE_REJECT_REMOTE_CLIENTS` in `dwPipeMode`, not in `dwOpenMode` as the block below shows — measured, an open-mode bit that is not a `FILE_FLAGS_AND_ATTRIBUTES` value makes `CreateNamedPipeW` fail with `ERROR_INVALID_PARAMETER` (87), and the flag is a `NAMED_PIPE_MODE` (windows-sys declares it so; tokio's own `ServerOptions` sets it in the pipe-mode argument).

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-api/src/bind.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;

    /// The derived default, not a fixed file name: the state dir doubles as the root here, so
    /// the path is exactly what a run would bind when `[api.unix] path` does not override it.
    fn sock(dir: &TempDir) -> PathBuf {
        crate::state::socket_path(dir.path(), dir.path()).expect("socket path")
    }

    #[cfg(unix)]
    #[test]
    fn a_second_bind_on_the_same_path_refuses() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let first = bind_unix(&path).expect("first bind");

        let err = bind_unix(&path).expect_err("second bind must refuse");
        assert!(format!("{err}").contains("already"), "names the reason: {err}");
        drop(first);
    }

    #[cfg(unix)]
    #[test]
    fn a_stale_socket_file_is_reclaimed() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        // A socket file with no listener behind it is what a crashed instance leaves.
        let stale = std::os::unix::net::UnixListener::bind(&path).expect("stale");
        drop(stale);

        let channel = bind_unix(&path).expect("a stale file must not block a restart");
        assert_eq!(channel.identity(), path.to_string_lossy());
        assert_eq!(channel.socket_path(), Some(path.as_path()));
    }

    #[cfg(unix)]
    #[test]
    fn a_dry_run_reports_a_stale_socket_and_leaves_it() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        // The same residue `a_stale_socket_file_is_reclaimed` removes, only observed:
        // the dry-run arm is what `doctor` reports with before `--fix` calls it for real.
        let stale = std::os::unix::net::UnixListener::bind(&path).expect("stale");
        drop(stale);

        assert!(reclaim_stale(&path, true).expect("probe"), "a stale file is what a fix reclaims");
        assert!(path.exists(), "a dry run unlinks nothing");

        // A live listener is answered the same way a real call answers it.
        let _channel = bind_unix(&path).expect("bind");
        assert!(reclaim_stale(&path, true).is_err(), "a live socket is not residue");
    }

    #[cfg(unix)]
    #[test]
    fn the_socket_file_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let _channel = bind_unix(&path).expect("bind");
        let mode = std::fs::metadata(&path).expect("metadata").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "only the owner may talk to the control channel");
    }

    #[cfg(unix)]
    #[test]
    fn an_over_long_path_names_the_remedy() {
        let dir = TempDir::new().expect("temp dir");
        let err = bind_unix(&dir.path().join("a".repeat(120)))
            .expect_err("`sun_path` is 108 bytes at most");
        let text = format!("{err}");
        assert!(text.contains("too long"), "names the reason: {text}");
        assert!(text.contains("[api.unix] path"), "names the remedy: {text}");
    }

    #[cfg(windows)]
    #[test]
    fn a_second_bind_on_the_same_pipe_refuses() {
        // A bare name, normalised by the bind: what `state` records is the full `\\.\pipe\…`
        // form the client dials, so the normalisation is asserted here rather than assumed.
        let name = format!("tinio-test-{}", std::process::id());
        let first = bind_pipe(&name).expect("first bind");
        assert!(first.identity().starts_with(r"\\.\pipe\"), "{}", first.identity());
        assert!(first.identity().ends_with(&name), "{}", first.identity());
        let err = bind_pipe(&name).expect_err("second bind must refuse");
        assert!(format!("{err}").contains("already"), "names the reason: {err}");
        drop(first);
    }
}
```

**Every test above is a plain `#[test]`, and that is the point of the shape rather than an oversight**: the bind path is `std` on unix (`std::os::unix::net::UnixListener`) and raw Windows on the pipe, so it runs with no runtime on the thread — which is exactly the property `Plane::bind` needs to be the single-instance lock before the CLI's runtime exists (§1 steps a–j precede k). If a test here ever needs `#[tokio::test]`, the bind has grown a reactor dependency again, and that is the regression to catch. (Task 4's and Task 5's tests *are* `#[tokio::test]`: they `serve`, and serving is where the conversion into tokio's types happens.)

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-api bind::`
Expected: FAIL — `cannot find function bind_unix`.

- [x] **Step 3: Write the implementation**

Prepend to `crates/tinio-api/src/bind.rs`:

```rust
//! Binding the control channel — which is also the single-instance lock.
//!
//! A second process on the same root fails here, before it can touch anything else: the
//! unix socket is unlinked only after a probe proves nobody is listening behind it, and the
//! Windows pipe is created with `FILE_FLAG_FIRST_PIPE_INSTANCE` (`management-api.md:5`).
//!
//! **Nothing here needs a runtime.** The listeners are `std` and the pipe is a raw handle
//! carrying a DACL, because the CLI binds *before* it builds its runtime (design §1: steps a–j,
//! then k) and tokio's own constructors cannot be called there — `tokio::net::UnixListener::bind`
//! panics ("there is no reactor running, must be called from the context of a Tokio 1.x
//! runtime"), and `NamedPipeServer::from_raw_handle`, which both `ServerOptions::create` and
//! `create_with_security_attributes_raw` end in, reaches the same `PollEvented::new`. The
//! conversion into tokio's types is `serve_transport`'s job, inside the runtime (Tasks 4–6).

use std::{io, path::PathBuf};

#[cfg(unix)]
use std::path::Path;

use crate::Error;

/// A bound control channel, before it is served.
#[derive(Debug)]
#[non_exhaustive]
pub enum Channel {
    /// The local channel's unix form: the bound `std` listener and the path it owns.
    #[cfg(unix)]
    Unix {
        /// The bound listener — `std`, so the bind needed no runtime.
        listener: std::os::unix::net::UnixListener,
        /// The path the bind created, authored once here and never re-derived.
        path: PathBuf,
    },
    /// The local channel's Windows form: the name plus the owned raw handle the runtime adopts.
    #[cfg(windows)]
    Pipe {
        /// The pipe name (`\\.\pipe\tinio-<sha1(root)[..16]>`, or `[api.pipe] path`).
        name: String,
        /// The bound first instance, created with its DACL and adopted inside the runtime
        /// (`dacl::adopt_pipe`, Task 4): an `OwnedHandle` rather than tokio's
        /// `NamedPipeServer`, because creating the latter needs the reactor the CLI does not
        /// have yet, and because it is `Send` where a raw pointer is not.
        handle: std::os::windows::io::OwnedHandle,
    },
}

impl Channel {
    /// The identity recorded in `state` and printed by `status`: the bound socket path or the
    /// pipe name. `state.control_name` carries that value — clients read it, never re-derive it.
    pub fn identity(&self) -> String {
        match self {
            #[cfg(unix)]
            Self::Unix { path, .. } => path.to_string_lossy().into_owned(),
            #[cfg(windows)]
            Self::Pipe { name, .. } => name.clone(),
        }
    }

    /// The socket path, when this is the unix form.
    pub fn socket_path(&self) -> Option<PathBuf> {
        match self {
            #[cfg(unix)]
            Self::Unix { path, .. } => Some(path.clone()),
            #[cfg(windows)]
            Self::Pipe { .. } => None,
        }
    }
}

/// The `sun_path` limit **including** the terminating NUL: 108 on Linux, 104 on macOS.
///
/// `pub` because plan C's `doctor` reports the resolved socket path *against* this limit rather
/// than spelling a number of its own — one owner for the platform constant, and the consumer
/// names it instead of a second copy.
#[cfg(unix)]
pub const SUN_PATH_MAX: usize = if cfg!(target_os = "macos") { 104 } else { 108 };

/// The refusal an over-long socket path gets. There is **no fallback** — abstract sockets are
/// not portable to macOS (`research.md:56`'s "documented limitation, no workaround" stands),
/// so the message names the remedy instead: `[api.unix] path` may be absolute (decision 24).
#[cfg(unix)]
fn over_long(path: &Path) -> Error {
    Error::Transport(format!(
        "the socket path {} is too long for `sun_path` ({SUN_PATH_MAX} bytes): set \
         `[api.unix] path` to an absolute path in a short directory",
        path.display()
    ))
}

/// Probes `path` and reclaims it when it is a stale socket file (one left by a
/// crashed instance), leaving a live listener alone. `dry_run` reports what a
/// real call would do without unlinking anything — the same one-code-path-plus-
/// a-flag shape `Cleanup::repair` uses, so `doctor` (plan C) never spells the
/// probe a second time.
///
/// `Ok(true)` — a stale file was removed (dry run: would be). `Ok(false)` —
/// there was nothing to reclaim. `Err` — a live server answered, or the file
/// could not be probed.
#[cfg(unix)]
pub fn reclaim_stale(path: &Path, dry_run: bool) -> Result<bool, Error> {
    if !path.exists() {
        return Ok(false);
    }

    // A listener behind the file means a live server; anything else is residue.
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => Err(Error::Transport(format!(
            "another server is already listening on {}",
            path.display()
        ))),
        Err(err) if err.kind() == io::ErrorKind::ConnectionRefused => {
            if !dry_run {
                std::fs::remove_file(path).map_err(Error::Io)?;
            }
            Ok(true)
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(Error::Transport(format!(
            "{} exists and cannot be probed: {err}",
            path.display()
        ))),
    }
}

/// Binds the unix socket at `path`, reclaiming a stale file left by a crashed instance —
/// [`reclaim_stale`] is that step, and it is the same implementation plan C's `doctor` probes
/// with, so there is one probe and not two.
///
/// The listener is `std` and stays non-blocking, because tokio's own bind cannot be called
/// before the runtime exists; `serve_local` (Task 4) converts it with `from_std` inside the
/// runtime, which is where tokio requires the non-blocking flag to already be set.
#[cfg(unix)]
pub fn bind_unix(path: &Path) -> Result<Channel, Error> {
    use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt};

    // Checked here as well as at the bind so the refusal names the remedy on every platform,
    // whatever its own limit is; the platform's check below is the authority.
    if path.as_os_str().as_bytes().len() >= SUN_PATH_MAX {
        return Err(over_long(path));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(Error::Io)?;
    }
    reclaim_stale(path, false)?;

    let listener = std::os::unix::net::UnixListener::bind(path).map_err(|err| {
        if err.kind() == io::ErrorKind::AddrInUse {
            Error::Transport(format!("another server is already listening on {}", path.display()))
        } else if err.kind() == io::ErrorKind::InvalidInput {
            over_long(path)
        } else {
            Error::Transport(format!("could not bind {}: {err}", path.display()))
        }
    })?;
    // Non-blocking, and private, both at the bind: `from_std` inside the runtime refuses a
    // blocking socket, and the mode has to be set here because the file exists only briefly.
    listener.set_nonblocking(true).map_err(Error::Io)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(Error::Io)?;
    Ok(Channel::Unix {
        listener,
        path: path.to_path_buf(),
    })
}

/// Binds the named pipe `name`, refusing when another instance already owns it, and keeps the
/// name on the channel (`NamedPipeServer` will not hand it back).
///
/// **The pipe is created raw, with a current-user-only DACL, and this is a deliberate
/// departure from `ServerOptions::create` on both counts.** The DACL is Task 1's reason
/// extended to the channel: `create` passes a null `SECURITY_ATTRIBUTES`, so the pipe gets the
/// creating token's default descriptor (the user, SYSTEM and Administrators full, Everyone and
/// ANONYMOUS LOGON read-only — measured on this host), while the channel is the one place this
/// crate claims is private (`Auth::Local` leaves `/metrics` and `/openapi.json` open on it,
/// decision 8). And the raw call is also what keeps the bind runtime-free: `ServerOptions`
/// ends in `NamedPipeServer::from_raw_handle`, which needs the reactor the CLI does not have
/// yet. `dacl` is the crate's one raw module, so this is an added use of that site, not a
/// second `unsafe` site (Task 1's lint note).
#[cfg(windows)]
pub fn bind_pipe(name: &str) -> Result<Channel, Error> {
    let name = pipe_name(name);
    let handle = crate::dacl::create_pipe(&name, true).map_err(|err| match err.kind() {
        // `FILE_FLAG_FIRST_PIPE_INSTANCE` answers ERROR_ACCESS_DENIED when the name is taken;
        // `AlreadyExists` is matched too so the refusal does not depend on one mapping.
        io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists => {
            Error::Transport(format!("another server is already listening on {name}"))
        }
        _ => Error::Transport(format!("could not create {name}: {err}")),
    })?;
    Ok(Channel::Pipe { name, handle })
}

/// The full form of a pipe name: `[api.pipe] path` may be written bare (`tinio-mine`), and the
/// client classifies `state.control_name` by its `\\.\pipe\` prefix (Task 9), while
/// `CreateNamedPipeW` wants the full form — so the bind owns the normalisation and every reader
/// downstream sees one shape.
#[cfg(windows)]
fn pipe_name(name: &str) -> String {
    const PREFIX: &str = r"\\.\pipe\";
    if name.starts_with(PREFIX) {
        name.to_string()
    } else {
        format!("{PREFIX}{name}")
    }
}
```

Add `pub mod bind;` to `crates/tinio-api/src/lib.rs` in alphabetical position (beside Task 1's `pub mod state;` and the `#[cfg(windows)] pub mod dacl;` line). `Channel` is named as `tinio_api::bind::Channel`, which is what this task's Interfaces line promises; no further re-export is added here.

`crate::dacl::create_pipe(name: &str, first_instance: bool) -> io::Result<std::os::windows::io::OwnedHandle>` is this task's addition to the module Task 1 created, and it is the one piece of the Windows arm the code above does not spell out, because the raw calls belong in that module and nowhere else:

```rust
/// Creates the named pipe `name` with a DACL that grants the current user full control and
/// nothing to anyone else, and returns it as an owned handle (adopted inside the runtime by
/// `adopt_pipe`, Task 4). `first_instance` is `FILE_FLAG_FIRST_PIPE_INSTANCE` — the
/// single-instance claim every later instance of the same name must fail on.
pub fn create_pipe(name: &str, first_instance: bool) -> io::Result<OwnedHandle> {
    let attributes = current_user_only_attributes()?;            // SECURITY_ATTRIBUTES + DACL
    let mode = FILE_FLAG_OVERLAPPED
        | PIPE_ACCESS_DUPLEX
        | PIPE_REJECT_REMOTE_CLIENTS                             // the local-only rule
        | if first_instance { FILE_FLAG_FIRST_PIPE_INSTANCE } else { 0 };
    let handle = unsafe {
        CreateNamedPipeW(
            to_wide_str(name).as_ptr(),
            mode,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE,
            PIPE_UNLIMITED_INSTANCES,
            65536, 65536, 0,
            &attributes.attributes,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // Ownership moves to the caller on success; the DACL buffer is dropped with `attributes`.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle as _) })
}

// and, in `serve_local`'s half of the same module (Task 4):
// `pub(crate) fn adopt_pipe(handle: OwnedHandle) -> io::Result<NamedPipeServer>` —
// `unsafe { NamedPipeServer::from_raw_handle(handle.into_raw_handle()) }`, called inside the
// runtime because that constructor is the one that needs the reactor.
```

**The imports `create_pipe` adds to `dacl.rs`**, since the module's list is kept at exactly what the code uses: `io` (its `io::Result`), `std::os::windows::io::{FromRawHandle as _, IntoRawHandle as _, OwnedHandle}` — **both traits, imported as `_`**: `OwnedHandle::from_raw_handle` and `OwnedHandle::into_raw_handle` are `FromRawHandle`/`IntoRawHandle` **trait** methods, not inherent ones, so a path-qualified call without the traits in scope is `error[E0599]` — and from `windows_sys::Win32::System::Pipes` the constants and call the body names (`CreateNamedPipeW`, `PIPE_ACCESS_DUPLEX`, `PIPE_REJECT_REMOTE_CLIENTS`, `PIPE_TYPE_BYTE`, `PIPE_READMODE_BYTE`, `PIPE_UNLIMITED_INSTANCES`, `INVALID_HANDLE_VALUE` is `Foundation`'s), plus `Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED` and `FILE_FLAG_FIRST_PIPE_INSTANCE` (already in the list for `FILE_ALL_ACCESS`).

Note for the Windows arm: `reject_remote_clients` is the named pipe's local-only rule and **it exists** — `ServerOptions::reject_remote_clients(&mut self, reject: bool)` in `tokio-1.53.1/src/net/windows/named_pipe.rs:2164` and the flag it sets at `:2318` (`PIPE_REJECT_REMOTE_CLIENTS`) — which is why `create_pipe` sets that flag explicitly: it is the same rule, asked of the same API, from the layer that can also carry the DACL. What `ServerOptions::create` cannot do is either of the two things this bind needs: it passes a null `SECURITY_ATTRIBUTES` (`named_pipe.rs:2275-2279`), and it ends in `NamedPipeServer::from_raw_handle` → `PollEvented::new` (`:128-133`), which needs the reactor the pre-runtime half does not have. Nothing is dropped, nothing is hedged.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-api bind::` (unix) and, on a Windows host, the same command there.
Expected: PASS — five `cfg(unix)` tests and one `cfg(windows)` test (`a_second_bind_on_the_same_pipe_refuses`), and **no `#[tokio::test]` among them**, which is the assertion that the bind grew no reactor dependency (the note under Step 1). The Windows arm is exercised by the `test-port` CI leg.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets -- -D warnings` on this host, and the `--target x86_64-pc-windows-gnu` leg in WSL2 (Task 1's Step 6 has the invocation and the `CARGO_TARGET_DIR` rule) — this is the second task whose Windows arm only that leg compiles.
Expected: both clean.
Report the files pending commit.

---

### Task 3: The router and the token rule

**Files:**
- Create: `crates/tinio-api/src/router.rs`
- Modify: `crates/tinio-api/src/lib.rs`, `crates/tinio-api/Cargo.toml` (dev-dep `tower` with `util`, `http-body-util`), `specs/001-s3-local-server/contracts/management-api.md` (the transport/auth matrix and its rationale sentence, the `/metrics` endpoint row at `:23`, and the derived socket name at `:5` — Step 3)

**Interfaces:**
- Consumes: `State` (Task 1), `Error`/`ErrorBody` (Task 1's `IntoResponse`).
- Produces: `tinio_api::router::{Auth, Shared, Context, router, StatusBody, StopBody}`; `pub type Shared = Arc<Context>` with `Context { state: State, host: String, auth: Auth, stop: tokio::sync::watch::Sender<bool>, port: Arc<OnceLock<u16>> }` built through `Context::new(state, host, auth, stop)`; `Auth { Local, Loopback, Tcp }` with `Auth::for_bind(addr) -> Auth` and `Auth::guards(self, path) -> bool`; `router(shared: Shared) -> axum::Router`. Task 7 adds a `gauges: Option<Arc<Gauges>>` field to `Context` (`None` from `Context::new`) and a route; Task 8 adds the `openapi` route behind its feature.

**Execution record (2026-09-20):** steps 1–5 ran and the review passed. One amendment: the middleware is attached with `.route_layer(...)`, not the `.layer(...)` the block below uses and the module doc's parenthetical explains — measured, `Router::layer` also wraps `fallback_router`, whose built-in catch-all serves every unmatched path, so an unrouted request reached the middleware and answered `401` instead of the contract's bare `404`. `route_layer` wraps only the routes registered before it, which is the invariant the task states; method-mismatched requests to registered paths stay guarded. Two of the brief's test loops also moved from `/metrics`/`/openapi.json` to registered paths (`/status`, `/stop`), with a comment naming Task 7/Task 8 as the owners of the deferred leg.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-api/src/router.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tokio::sync::watch;
    use tower::ServiceExt;

    use super::*;
    use crate::state::State;

    /// `state` records the control name and the router only passes it through — the value
    /// here is an arbitrary recorded name (the derived default is Task 1's, and no arm
    /// re-derives one).
    fn state() -> State {
        State::new("tinio-0123456789abcdef.sock", Some(9000), Path::new("/srv/data")).expect("state")
    }

    fn shared(auth: Auth) -> (Shared, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (Arc::new(Context::new(state(), "127.0.0.1", auth, tx)), rx)
    }

    fn get(path: &str, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri(path).method("GET");
        if let Some(token) = token {
            builder = builder.header("x-tinio-token", token);
        }
        builder.body(Body::empty()).expect("request")
    }

    #[tokio::test]
    async fn status_answers_with_the_state_on_the_local_channel() {
        let (shared, _rx) = shared(Auth::Local);
        let token = shared.state.token.clone();
        let response = router(shared)
            .oneshot(get("/status", Some(&token)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.expect("body").to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(text.contains("\"running\":true"), "{text}");
        assert!(text.contains("\"pid\""), "{text}");
        assert!(text.contains("\"host\":\"127.0.0.1\""), "the contract's body carries host: {text}");
    }

    #[tokio::test]
    async fn status_refuses_a_missing_or_wrong_token() {
        let (shared, _rx) = shared(Auth::Local);
        for request in [get("/status", None), get("/status", Some("wrong"))] {
            let response = router(shared.clone()).oneshot(request).await.expect("response");
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn a_token_that_differs_in_one_byte_is_refused() {
        // The comparison is fixed-length and branch-free past the length check (`tokens_match`
        // below), so the last byte is as expensive to guess as the first: a token that is
        // correct except for its final character is refused like any other.
        let (shared, _rx) = shared(Auth::Local);
        let mut token = shared.state.token.clone();
        token.pop();
        token.push(if token.ends_with('a') { 'b' } else { 'a' });
        assert!(!tokens_match(&token, &shared.state.token));
        assert!(tokens_match(&shared.state.token, &shared.state.token));
        let response = router(shared).oneshot(get("/status", Some(&token))).await.expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn the_local_channel_does_not_guard_metrics() {
        // The route itself arrives in Task 7. What is observable here is the middleware's
        // rule: a guarded path answers 401 before any routing, an open one does not.
        let (shared, _rx) = shared(Auth::Local);
        let response = router(shared).oneshot(get("/metrics", None)).await.expect("response");
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_loopback_tcp_plane_opens_metrics_and_nothing_else() {
        // Loopback opens `/metrics` for one stated reason — a Prometheus scrape cannot send a
        // custom header — and that reason does not cover `/openapi.json` or the two control
        // endpoints, which stay guarded exactly as they are on a remote bind (`/stop` by the
        // same path-only rule, asserted under `Auth::Tcp` below).
        let (shared, _rx) = shared(Auth::Loopback);
        let open = router(shared.clone()).oneshot(get("/metrics", None)).await.expect("response");
        assert_ne!(open.status(), StatusCode::UNAUTHORIZED);
        for path in ["/status", "/openapi.json"] {
            let guarded = router(shared.clone()).oneshot(get(path, None)).await.expect("response");
            assert_eq!(guarded.status(), StatusCode::UNAUTHORIZED, "{path}");
        }
    }

    #[tokio::test]
    async fn tcp_requires_the_token_on_every_endpoint() {
        let (shared, _rx) = shared(Auth::Tcp);
        for path in ["/status", "/metrics"] {
            let response = router(shared.clone()).oneshot(get(path, None)).await.expect("response");
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        }
    }

    #[tokio::test]
    async fn a_bearer_token_is_accepted_wherever_the_header_is() {
        // A Prometheus `scrape_config` can send `Authorization: Bearer` and never a custom
        // header (`management-api.md:15`), so the alias holds on every transport.
        for auth in [Auth::Local, Auth::Loopback, Auth::Tcp] {
            let (shared, _rx) = shared(auth);
            let token = shared.state.token.clone();
            let request = Request::builder()
                .uri("/status")
                .method("GET")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("request");
            let response = router(shared).oneshot(request).await.expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{auth:?}");
        }
    }

    #[tokio::test]
    async fn stop_flips_the_shutdown_channel_and_answers_202() {
        let (shared, mut rx) = shared(Auth::Local);
        let token = shared.state.token.clone();
        let response = router(shared)
            .oneshot(
                Request::builder()
                    .uri("/stop")
                    .method("POST")
                    .header("x-tinio-token", token)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        rx.changed().await.expect("the channel flips");
        assert!(*rx.borrow());
    }

    #[tokio::test]
    async fn an_unknown_path_is_404_with_or_without_the_token() {
        // An unmatched path never reaches the middleware: axum's `Router::layer` wraps the
        // routes registered before it and an explicit fallback, but **not** its built-in 404
        // (`axum-0.8.9/src/routing/mod.rs:311-317`), so both requests get the contract's plain
        // `404` (`management-api.md:50`) and nothing about the plane leaks. The invariant a new
        // route has to keep is the other half of this: register it **before** the `.layer(...)`
        // line, or it is served with no token check at all.
        let (shared, _rx) = shared(Auth::Local);
        let token = shared.state.token.clone();
        let with_token = router(shared.clone()).oneshot(get("/nope", Some(&token))).await.expect("response");
        assert_eq!(with_token.status(), StatusCode::NOT_FOUND);
        let without_token = router(shared).oneshot(get("/nope", None)).await.expect("response");
        assert_eq!(without_token.status(), StatusCode::NOT_FOUND);
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-api router::`
Expected: FAIL — `cannot find function router`.

- [x] **Step 3: Write the implementation**

Prepend to `crates/tinio-api/src/router.rs`:

```rust
//! The management router: `/status`, `/stop`, `/metrics`, `/openapi.json`, and the token rule.
//!
//! Which endpoints need the token follows the **bind address**, not the handler
//! (`management-api.md:9-15`, decision 8), and the rule is an **exception list on the open
//! side**: the open paths are named, every other *registered* path is guarded, and a request
//! that matches no route never reaches the middleware at all — axum's `Router::layer` wraps the
//! routes registered before it and an explicitly registered fallback, but not its built-in 404
//! (`axum-0.8.9/src/routing/mod.rs:311-317`), so an unmatched path answers the contract's plain
//! `404` (`management-api.md:50`) and leaks nothing. **Every route this crate serves is
//! therefore registered before the `.layer(...)` line** — that is the invariant a new endpoint
//! has to keep, and it is why Tasks 7 and 8 add their routes inside `router()` rather than
//! after the layer. On the local channel the open paths are exactly the two read endpoints,
//! `/metrics` and `/openapi.json`; a TCP transport bound to a loopback address keeps
//! **`/metrics`** open too — a scraper cannot send a custom header, which is a specific reason
//! and not a general trust in localhost — while `/status`, `/stop`, `/openapi.json` and every
//! other registered path still need the token there; a non-loopback bind requires it on every
//! endpoint, because a reachable host/port otherwise leaks operational data. Because a standard
//! scraper can send only `Authorization: Bearer` and never a custom header, that header is an
//! **alias** for `X-Tinio-Token` on every transport — which is what makes
//! `management-api.md:15`'s rationale true rather than aspirational.

use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
};

use axum::{
    Json, Router,
    extract::State as AxumState,
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::state::State;

/// The token rule that applies to this plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Auth {
    /// The local channel: every registered path is guarded **except** `/metrics` and
    /// `/openapi.json`. The two are the rule's open side, so `/status`, `/stop` and any other
    /// route registered before the layer are guarded by default.
    Local,
    /// A TCP transport bound to a loopback address: `/metrics` alone is open, because a
    /// scraper cannot send a custom header; everything else — `/status`, `/stop`,
    /// `/openapi.json` and any other registered path — is guarded. Deliberately narrower than
    /// [`Auth::Local`] — a loopback TCP port is reachable by every local user, which a
    /// mode-0600 unix socket is not.
    Loopback,
    /// A TCP transport bound to any other address: every endpoint is guarded.
    Tcp,
}

impl Auth {
    /// The rule for a TCP transport bound to `addr`. The rule follows the **bind** address:
    /// `0.0.0.0:9000` is not a loopback bind, whatever a client dials to reach it.
    pub fn for_bind(addr: SocketAddr) -> Self {
        if addr.ip().is_loopback() {
            Self::Loopback
        } else {
            Self::Tcp
        }
    }

    /// Whether `path` needs a token under this rule. Every arm is written as the **exception**
    /// list: a `false` is a deliberate opening, and every other registered path is guarded.
    /// An unmatched path never reaches this function — see the module doc.
    pub fn guards(self, path: &str) -> bool {
        match self {
            // The local channel: the two read endpoints are open, everything else that is
            // routed — `/status`, `/stop`, and any route registered before the layer — is
            // guarded (`management-api.md:11`).
            Self::Local => !matches!(path, "/metrics" | "/openapi.json"),
            // A loopback bind opens `/metrics` and nothing else: the scraper's inability to
            // send a custom header is the whole reason, and it does not extend to the API
            // schema, which a browser or a curl one-liner reads as easily as a scraper does.
            Self::Loopback => path != "/metrics",
            Self::Tcp => true,
        }
    }
}

/// What every handler shares.
#[derive(Debug)]
pub struct Context {
    /// The state this plane published.
    pub state: State,
    /// The data plane's host, as `/status` reports it (`state` carries its port).
    pub host: String,
    /// The token rule for the transport it is served over.
    pub auth: Auth,
    /// The shutdown channel `/stop` flips. The CLI owns the channel and its receiving ends.
    pub stop: watch::Sender<bool>,
    /// The data plane's port, set once at the readiness point by `Plane::publish` (Task 5) —
    /// the same value `state` records — and reported as `None` by `/status` until then.
    pub port: Arc<OnceLock<u16>>,
}

impl Context {
    /// The context a plane serves: `host` is the data plane's host, and the port is published
    /// later (`Plane::publish` is the only writer of either it or `state.port`).
    pub fn new(
        state: State,
        host: impl Into<String>,
        auth: Auth,
        stop: watch::Sender<bool>,
    ) -> Self {
        Self {
            state,
            host: host.into(),
            auth,
            stop,
            port: Arc::new(OnceLock::new()),
        }
    }
}

/// The shared context, behind an `Arc` because every handler clones it.
pub type Shared = Arc<Context>;

/// The `GET /status` body (`management-api.md:30-41`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusBody {
    /// Always true: a stopped server does not answer.
    pub running: bool,
    /// The serving pid.
    pub pid: u32,
    /// The data plane's host (`management-api.md:35`'s `"host": "127.0.0.1"`).
    pub host: String,
    /// The storage root, canonical.
    pub root: String,
    /// The data plane's port, when it is bound.
    pub port: Option<u16>,
    /// RFC3339, UTC.
    pub started_at: String,
    /// The serving build's version.
    pub version: String,
}

/// The `POST /stop` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopBody {
    /// Always `"draining"`.
    pub status: String,
}

/// Builds the router for `shared`.
///
/// **Every route goes before the `.layer(...)` line, and that ordering is the auth rule's other
/// half**: `Router::layer` wraps only the routes registered before it (`axum-0.8.9`'s
/// `routing/mod.rs:311-317`), so a route added after the layer would be served with no token
/// check at all. Tasks 7 and 8 add their routes to this function, above the layer.
pub fn router(shared: Shared) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/stop", post(stop))
        .layer(middleware::from_fn_with_state(shared.clone(), authorize))
        .with_state(shared)
}

async fn authorize(
    AxumState(shared): AxumState<Shared>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if shared.auth.guards(path) {
        // `X-Tinio-Token`, or `Authorization: Bearer <token>` as an alias — the alias is what
        // lets a Prometheus scrape reach `/metrics` over TCP (`management-api.md:15`).
        let presented = headers
            .get("x-tinio-token")
            .and_then(|value| value.to_str().ok())
            .or_else(|| bearer(&headers))
            .unwrap_or_default();
        if !tokens_match(presented, &shared.state.token) {
            return crate::Error::Unauthorized.into_response();
        }
    }
    next.run(request).await
}

/// Compares two tokens without leaking, through the comparison's own timing, *where* they
/// first differ — the length check is not a leak worth avoiding (the token is a fixed 64 hex
/// characters by construction, `state.rs`'s `TOKEN_BYTES`), and past it the loop touches every
/// byte of both sides whichever byte differs, so a token cannot be walked one byte at a time.
/// `==` on `&str` short-circuits at the first difference, which is the whole reason this helper
/// exists rather than a plain comparison: it is a few cycles on a path that runs once per
/// management request, and the alternative (`subtle`'s `ConstantTimeEq`, or comparing hashes
/// through it) buys the same property with a new dependency.
fn tokens_match(presented: &str, expected: &str) -> bool {
    let (presented, expected) = (presented.as_bytes(), expected.as_bytes());
    if presented.len() != expected.len() {
        return false;
    }
    presented
        .iter()
        .zip(expected)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// The token from an `Authorization: Bearer <token>` header, when there is one.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    let (scheme, token) = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())?
        .split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then_some(token)
}

async fn status(AxumState(shared): AxumState<Shared>) -> Json<StatusBody> {
    Json(StatusBody {
        running: true,
        pid: shared.state.pid,
        host: shared.host.clone(),
        root: shared.state.root.to_string_lossy().into_owned(),
        port: shared.port.get().copied(),
        started_at: shared.state.started_at.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

async fn stop(AxumState(shared): AxumState<Shared>) -> (StatusCode, Json<StopBody>) {
    // A send failure means every receiver is gone, which is only possible while shutting
    // down — the answer the caller needs is the same either way.
    let _ = shared.stop.send(true);
    (
        StatusCode::ACCEPTED,
        Json(StopBody {
            status: "draining".to_string(),
        }),
    )
}
```

Add `pub mod router;` to `crates/tinio-api/src/lib.rs` in alphabetical position; `Shared`, `Context`, `Auth`, `router`, `StatusBody` and `StopBody` are named as `tinio_api::router::{…}`, which is what this task's Interfaces line promises, and Task 5 adds the `Plane`/`Options` re-exports beside them.

`specs/001-s3-local-server/contracts/management-api.md` — **this task owns two of this feature's three contract edits**, and they are made here rather than deferred to the design's Bookkeeping:

- `:23`, the `/metrics` row of the endpoint table: its auth cell gains the bind-address dimension (open on the local channel and on a loopback bind, token-guarded otherwise — the rule `Auth::guards` implements), and its description records that the endpoint answers `404` when the plane carries no gauge source (Task 7's ruling: the absence is served, not an empty `200`).
- `:5`, the intro paragraph: the unix socket's default is the **derived** `tinio-<sha1(canonicalize(root))[..16]>.sock` in the state dir (design decision 24), not a fixed `tinio.sock`; `[api.unix] path` overrides it, and the resolved absolute path is what `state.control_name` records.

The transport/auth matrix's rationale sentence is rewritten in the same edit to say what the bearer alias makes true (decision 8: "the contract's token matrix gains the bind-address dimension and its rationale sentence is rewritten in the same task"). The **Errors** section (`:49-51`) needs no edit: its `404 — unknown path` row is exactly what the router now delivers for an unmatched path, and the module doc records why (the layer does not wrap axum's built-in 404).

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-api router::`
Expected: PASS — nine tests (the unmatched-path rule is one test with two assertions inside it: 404 with the token and 404 without).

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets -- -D warnings`
Expected: clean. (`middleware::from_fn_with_state` needs the `Router`'s state type to unify; if the compiler asks, `Router::new()…with_state(shared)` before `.layer(…)` is the alternative order — take whichever compiles and note it.)
Report the files pending commit.

---

### Task 4: The local-channel transports

**Files:**
- Create: `crates/tinio-api/src/transport.rs`, `crates/tinio-api/src/client.rs` (the early test helpers — Step 1's note; Task 9 promotes them to the public surface and adds the rest)
- Modify: `crates/tinio-api/src/lib.rs`, `crates/tinio-api/src/dacl.rs` (the `adopt_pipe` wrapper the pipe arm calls), `crates/tinio-api/Cargo.toml` (deps `hyper` — its default features carry `client` + `http1`, which the pipe's `auto::Builder` and Task 9's `hyper::client::conn::http1::handshake` both need — `http-body-util` (`BodyExt::collect`), and **`hyper-util = { workspace = true, features = ["server", "service", "tokio", "http1"] }`, declared rather than inherited**: today the sketch also compiles on axum's manifest alone (axum enables `hyper-util`'s `tokio`, `server` and `service`, and `hyper-util/http1` through its own default `http1` feature), but `hyper_util::server::conn::auto` is gated on `any(feature = "http1", feature = "http2")` and `TowerToHyperService` on `service` (`hyper-util-0.1.20/src/server/conn/mod.rs`, `src/service/mod.rs`), so a `-p tinio-api` build would break the day axum's feature set changes; `http2` and `server-auto` are deliberately **not** asked for — the pipe is HTTP/1 with keep-alive off — and this workspace's `tinio-server` turning them on does not unify into a `-p tinio-api` build)

**Interfaces:**
- Consumes: `Channel` (Task 2), `router`/`Shared` (Task 3).
- Produces: `tinio_api::transport::serve_local` with `pub async fn serve_local(channel: Channel, shared: Shared, stop: watch::Receiver<bool>) -> Result<(), Error>`. **This is where the bind's `std` listener becomes tokio's**: the conversion (`tokio::net::UnixListener::from_std`) happens here, inside the runtime, which is the only place it can happen at all (`from_std` panics without one) and the reason Task 2's bind could precede the runtime. The `stop` receiver is the same channel `/stop` and the signal handlers flip (decision 12); it is taken here rather than inside `shared` because serving — not the router — is what has to *stop*.

**Execution record (2026-09-20):** steps 1–5 ran and the review passed; the unix tests' evidence comes from WSL2, since this host's `transport::` filter matches zero tests and exits 0. Two amendments, both measured: hyper 1.x's default feature set is **empty**, so the `client`/`http1` features the note below attributes to its defaults are declared at the crate; and the crate declares its own tokio features (`sync` for `watch`, `macros` for the pipe arm's `select!`) rather than inheriting them from axum's feature set. `client.rs`'s staged helpers and `until_stopped` carry `dead_code` allows whose removal points are named in the report (Task 9, and Tasks 5/6).

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-api/src/transport.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;
    use tokio::sync::watch;

    use super::*;
    use crate::{
        bind::bind_unix,
        router::{Auth, Context},
        state::State,
    };

    /// The one channel a plane uses: the sender lives in the `Context` (`/stop` sends on it),
    /// the receiver is handed to `serve_local` — the real wiring, so the tests exercise the
    /// shape the CLI uses rather than a second one.
    fn shared() -> (Shared, watch::Receiver<bool>) {
        let state = State::new("tinio-0123456789abcdef.sock", Some(9000), Path::new("/srv/data"))
            .expect("state");
        let (tx, rx) = watch::channel(false);
        (Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx)), rx)
    }

    /// The derived default socket path (the state dir doubles as the root).
    fn sock(dir: &TempDir) -> PathBuf {
        crate::state::socket_path(dir.path(), dir.path()).expect("socket path")
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_unix_channel_serves_status() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let channel = bind_unix(&path).expect("bind");
        let (shared, stop) = shared();
        let token = shared.state.token.clone();

        let serving = tokio::spawn(serve_local(channel, shared, stop));
        let status = crate::client::get_local(&path, "/status", Some(&token)).await.expect("status");
        assert_eq!(status.status, 200);
        assert!(status.body.contains("\"running\":true"), "{}", status.body);

        serving.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_unix_channel_leaves_a_replaced_socket_alone() {
        // The bind's reclaim path must not touch a socket a live server is behind: this
        // test proves the pair — the second bind refuses rather than unlinking.
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let channel = bind_unix(&path).expect("bind");
        let (shared, stop) = shared();
        let serving = tokio::spawn(serve_local(channel, shared, stop));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(bind_unix(&path).is_err(), "a live socket is not residue");
        serving.abort();
    }
}
```

These tests use helpers that Task 9 publishes, and Task 5's TCP tests use a third. Create them **now**, in `crates/tinio-api/src/client.rs` — one definition each, no second set. **Real bodies, not signatures**: a module needs its items to have bodies, and these two dial and hand the stream to the `exchange` helper below, which is what makes the tests above (`crate::client::get_local`) compile and pass in this task rather than in Task 9:

```rust
use std::{net::SocketAddr, path::Path};

use http_body_util::BodyExt;
use hyper::Request;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawResponse {
    pub(crate) status: u16,
    pub(crate) body: String,
}

pub(crate) async fn get_local(path: &Path, target: &str, token: Option<&str>) -> Result<RawResponse, Error> {
    #[cfg(unix)]
    {
        let stream = tokio::net::UnixStream::connect(path).await.map_err(Error::Io)?;
        exchange(TokioIo::new(stream), target, "GET", token).await
    }
    #[cfg(not(unix))]
    {
        let _ = (path, target, token);
        Err(Error::Transport("a unix socket is not a Windows transport".to_string()))
    }
}

pub(crate) async fn get_http(addr: SocketAddr, target: &str, token: Option<&str>) -> Result<RawResponse, Error> {
    let stream = tokio::net::TcpStream::connect(addr).await.map_err(Error::Io)?;
    exchange(TokioIo::new(stream), target, "GET", token).await
}

/// One request over an already-dialed stream: the HTTP/1 client connection, the request, and
/// the response read back as text. **The single `exchange` for the whole crate** — Task 9's
/// `request` (its `Control` dialers and its `probe`/`status`/`stop`) and the tests above are all
/// its callers, so there is one definition and not two.
async fn exchange<I>(
    io: TokioIo<I>,
    target: &str,
    method: &str,
    token: Option<&str>,
) -> Result<RawResponse, Error>
where
    I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|err| Error::Transport(format!("handshake: {err}")))?;
    tokio::spawn(async move {
        // The connection future ends when either side is done; a failure here is the
        // caller's response error, reported there rather than twice.
        let _ = connection.await;
    });

    let mut builder = Request::builder().method(method).uri(target);
    if let Some(token) = token {
        builder = builder.header("x-tinio-token", token);
    }
    let request = builder
        .body(String::new())
        .map_err(|err| Error::Transport(format!("request: {err}")))?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|err| Error::Transport(format!("{target}: {err}")))?;
    let status = response.status().as_u16();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|err| Error::Transport(format!("{target}: {err}")))?
        .to_bytes();
    Ok(RawResponse {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}
```

Add `mod client;` to `crates/tinio-api/src/lib.rs` beside the other modules. Task 9 does the rest — promotes `RawResponse`, `get_local`, `get_http` and `exchange` to `pub` (or `pub(crate)` where no test outside the crate needs them), adds `Report`, `control`, `probe`, `status`, `stop` and `request`, and the crate keeps exactly one definition of each of the four. The alternative (a second HTTP helper in the tests) is the duplication this plan exists to avoid.

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-api transport::`
Expected: FAIL — `cannot find function serve_local`.

- [x] **Step 3: Write the implementation**

Prepend to `crates/tinio-api/src/transport.rs`:

```rust
//! Serving the router over the transports the config selected.
//!
//! **This module is where the bind's runtime-free listeners become tokio's.** Task 2 created
//! `std` ones (and, on Windows, a raw pipe handle) so `Plane::bind` could be the
//! single-instance lock before the CLI's runtime existed; `from_std` and
//! `NamedPipeServer::from_raw_handle` both need a reactor, and `serve_transport` runs inside
//! the runtime, so the conversions live here and nowhere else (`Channel` never enters tokio's
//! types).
//!
//! The unix socket is an axum `Listener` once converted, so `axum::serve` drives it directly,
//! with `with_graceful_shutdown` on the plane's channel — ceasing to accept is the plane's
//! job, and the ≤ 10 s bound on the drain is the CLI's (decision 12). The Windows named pipe is
//! not a `Listener` at all: `NamedPipeServer` has no impl, and the pipe's lack of TCP's
//! half-close makes keep-alive unsafe there, so it gets the manual connection loop
//! `research.md:99` prescribes. TCP and TLS (Task 5, Task 6) are `Listener`s again.

use std::sync::Arc;

use tokio::sync::watch;

use crate::{Error, bind::Channel, router::{Shared, router}};

/// Serves `shared` over a bound local channel until `stop` flips, then drains and returns.
///
/// The channel is self-describing: the pipe arm carries its own name (`Channel::Pipe { name,
/// .. }`), so no caller passes an identity in and no arm re-derives one — the same name
/// `Channel::identity` reports is what `state.control_name` records.
pub async fn serve_local(
    channel: Channel,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    match channel {
        #[cfg(unix)]
        Channel::Unix { listener, .. } => {
            // The one conversion: `std` (bound before the runtime) to tokio (served inside it).
            let listener = tokio::net::UnixListener::from_std(listener).map_err(Error::Io)?;
            axum::serve(listener, router(shared))
                .with_graceful_shutdown(until_stopped(stop))
                .await
                .map_err(|err| Error::Transport(format!("unix channel: {err}")))
        }
        #[cfg(windows)]
        Channel::Pipe { name, handle } => serve_pipe(handle, &name, shared, stop).await,
    }
}

/// The shape every `with_graceful_shutdown` call in this plan takes: resolve when the plane's
/// channel flips (or when its sender is gone, which is a shutdown too).
pub(crate) async fn until_stopped(mut stop: watch::Receiver<bool>) {
    let _ = stop.changed().await;
}

/// The named-pipe accept loop: one connection at a time, HTTP/1 keep-alive off, and the
/// shutdown channel watched while no client is connecting.
#[cfg(windows)]
async fn serve_pipe(
    handle: std::os::windows::io::OwnedHandle,
    identity: &str,
    shared: Shared,
    mut stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    use hyper_util::{rt::{TokioExecutor, TokioIo}, server::conn::auto, service::TowerToHyperService};

    // The raw handle becomes tokio's own type here, inside the runtime — the constructor needs
    // the reactor, and `dacl` is the crate's one raw-Windows module (Task 2's note).
    let mut pipe = crate::dacl::adopt_pipe(handle).map_err(Error::Io)?;
    // In-flight connections, so "cease accepting" is not "drop the requests already answered".
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            _ = stop.changed() => break,
            connected = pipe.connect() => {
                connected.map_err(|err| Error::Transport(format!("{identity}: pipe connect: {err}")))?;
            }
        }
        let connected = pipe;
        // The next instance must exist before this connection is served, or the pipe name
        // disappears between two connections. `first_pipe_instance` is false here: the
        // single-instance claim was made by the bind, and re-asserting it would refuse our
        // own successor.
        let next = crate::dacl::create_pipe(identity, false).map_err(Error::Io)?;
        pipe = crate::dacl::adopt_pipe(next).map_err(Error::Io)?;

        let router = router(shared.clone());
        connections.spawn(async move {
            // `http1` borrows the builder mutably (`hyper-util`'s
            // `auto::Builder::http1(&mut self)`), so the binding is `mut`.
            let mut builder = auto::Builder::new(TokioExecutor::new());
            // axum's `Router` is a tower service and this loop is hyper's own, so the bridge is
            // explicit — `serve_connection` wants `hyper::service::Service`, and axum wraps with
            // exactly this adapter in its own server (`axum-0.8.9/src/serve/mod.rs:388`).
            let service = TowerToHyperService::new(router);
            // The pipe has no half-close, so keep-alive would let a client wait forever for
            // a response delimitation TCP would have provided (`research.md:99`).
            if let Err(err) = builder
                .http1()
                .keep_alive(false)
                .serve_connection(TokioIo::new(connected), service)
                .await
            {
                tracing::debug!(error = %err, "management pipe connection ended");
            }
        });
    }

    // The drain: every connection already accepted runs to completion before this returns.
    while connections.join_next().await.is_some() {}
    Ok(())
}
```

Add `pub mod transport;` to `crates/tinio-api/src/lib.rs` in alphabetical position (beside `mod client;` from Step 1). `serve_local` is named as `tinio_api::transport::serve_local`, which is this task's Interfaces line; the `Plane`/`Options` re-exports arrive in Task 5.

Task 5's `Transport::Local { channel, identity, auth }` records the **channel's own identity** — the path or name the bind actually created — into `state.control_name`, and Task 9's client dials with that recorded name; the name is authored once, at the bind, carried by the channel, and no arm derives it a second way.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-api transport::`
Expected: PASS — **unix-only, and say so when you report it**: both tests are `#[cfg(unix)]`, so on a Windows host this exact filter matches **zero** tests and cargo exits `0` without running anything, which is not evidence. The Windows arm is not tested here; it compiles under the `windows-gnu` clippy leg (Step 5) and is exercised end to end by Task 10's pipe leg, which is Windows-gated.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets -- -D warnings` on this host, and the `--target x86_64-pc-windows-gnu` leg — the one that compiles the pipe arm — in **WSL2**, per `docs/tests.md:13`'s Platform-gated verification section (`docs/tests.md:121-131`; WSL2's toolchain has the target installed, this host's does not). Give the Linux-side run its own `CARGO_TARGET_DIR` (e.g. `~/.cache/tinio-wsl-target`), because `/mnt/e/GitHub/tinio/target` holds this host's Windows `msvc` artifacts.
Expected: both clean.
Report the files pending commit.

---

### Task 5: TCP HTTP and the three-choose-one resolution

**Files:**
- Modify: `crates/tinio-api/src/transport.rs`, `crates/tinio-api/src/lib.rs`, `crates/tinio-api/src/client.rs` (the `get_http` helper Task 4 created early — these tests are its first caller, and no second copy is added here)

**Interfaces:**
- Consumes: `Channel`, `serve_local`, `Shared`.
- Produces: `tinio_api::transport::{Transport, Selection, LocalForm, HttpsOptions, Tcp, serve_tcp, select}` where `Selection { local: Option<LocalForm>, http: Option<SocketAddr>, https: Option<HttpsOptions> }`, `LocalForm { Unix(PathBuf), Pipe(String) }`, `HttpsOptions { addr: SocketAddr, cert: PathBuf, key: PathBuf }`, `Transport { Local { channel: Channel, identity: String, auth: Auth }, Tcp { listener: std::net::TcpListener, addr: SocketAddr, auth: Auth }, Https { … } }`, `pub fn select(selection: Selection) -> Result<Transport, Error>`, and — re-exported from `lib.rs` — `tinio_api::{Plane, Options}` with `Plane::bind(options) -> Result<Plane, Error>`, `Plane::serve(&self, stop: watch::Receiver<bool>)`, `Plane::publish(&self, port: u16)`, `Plane::shutdown(&self)`, `Transport::identity(&self)`, `Transport::auth(&self)`. **The TCP and HTTPS listeners are `std` for the same reason Task 2's unix listener is** — `select` runs in `Plane::bind`, before the runtime, and `tokio::net::TcpListener::from_std` panics without a reactor, so `select` binds with `std`, keeps the std listener in the `Transport`, and `serve_tcp`/`serve_https` convert inside the runtime. **`Plane` and `Options` are defined here and nowhere else** — the File Structure table's `lib.rs` row and Task 10's ownership check both point at this task for them.

The CLI builds `Selection` from the resolved `[api]` config and the `--api*` flags (plan B) and hands the whole thing to `Plane::bind`; the exclusivity rule lives in `select` so that two callers cannot enforce it differently.

**Execution record (2026-09-20):** steps 1–5 ran and the review passed, after one fix round. Two amendments. The non-loopback test asserts the token rule on a **registered** path (`/status`) with a comment naming Task 7 as the owner of the `/metrics` leg, because an unrouted path answers the contract's bare `404` and never reaches the middleware. And `Plane::shutdown`/`Drop` now delete through one ownership-checked helper — the socket only while the file at that path still carries the `(dev, ino)` recorded at bind, the `state` file only when it reads back with this process's `pid` — because the unguarded deletion below could strip a successor that bound the reclaimed socket during this plane's shutdown window (the gate is released when the control listener closes, but `shutdown` runs after the data-plane drain). `publish` is a no-op on a second call, so the file and `/status` cannot disagree.

- [x] **Step 1: Write the failing tests**

Append to the `tests` module in `crates/tinio-api/src/transport.rs`:

```rust
    #[test]
    fn exactly_one_transport_may_be_enabled() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().expect("addr");
        let both = select(Selection {
            local: Some(LocalForm::Unix(PathBuf::from("/tmp/tinio-0123456789abcdef.sock"))),
            http: Some(addr),
            https: None,
        });
        assert!(both.is_err(), "the local channel and TCP are mutually exclusive");

        let http_and_https = select(Selection {
            local: None,
            http: Some(addr),
            https: Some(HttpsOptions {
                addr,
                cert: PathBuf::from("/tmp/cert.pem"),
                key: PathBuf::from("/tmp/key.pem"),
            }),
        });
        #[cfg(feature = "tls")]
        assert!(http_and_https.is_err(), "http and https are mutually exclusive");
        #[cfg(not(feature = "tls"))]
        assert!(http_and_https.is_err(), "https without the tls feature is a usage error");

        let none = select(Selection { local: None, http: None, https: None });
        assert!(none.is_err(), "no transport at all is a startup error");
    }

    #[tokio::test]
    async fn a_loopback_tcp_plane_does_not_guard_metrics() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");
        let state = State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data"))
            .expect("state");
        let (tx, stop) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.clone();

        let serving = tokio::spawn(serve_tcp(listener, shared, stop));
        // A loopback bind has the local channel's trust level, so `/metrics` is open there.
        // The route and its gauges arrive in Task 7: what is asserted here is that the
        // middleware does not answer 401, which is what a guarded path would answer, and
        // therefore that Task 7's 200 is reachable without a token.
        let open = crate::client::get_http(addr, "/metrics", None).await.expect("request");
        assert_ne!(open.status, 401, "a loopback bind keeps /metrics open");
        let guarded = crate::client::get_http(addr, "/status", None).await.expect("request");
        assert_eq!(guarded.status, 401, "/status still needs the token on a loopback bind");
        let allowed = crate::client::get_http(addr, "/status", Some(&token)).await.expect("request");
        assert_eq!(allowed.status, 200);

        serving.abort();
    }

    #[tokio::test]
    async fn a_non_loopback_tcp_plane_requires_the_token_on_metrics() {
        // The rule follows the *bind* address, so a wildcard bind is the strict one; the
        // client still dials 127.0.0.1, because a wildcard is not a connect address.
        let listener = std::net::TcpListener::bind("0.0.0.0:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let bound = listener.local_addr().expect("addr");
        let addr = SocketAddr::from(([127, 0, 0, 1], bound.port()));
        let state = State::new(bound.to_string(), Some(bound.port()), Path::new("/srv/data"))
            .expect("state");
        let (tx, stop) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "0.0.0.0", Auth::for_bind(bound), tx));
        let token = shared.state.token.clone();

        let serving = tokio::spawn(serve_tcp(listener, shared, stop));
        let open = crate::client::get_http(addr, "/metrics", None).await.expect("request");
        assert_eq!(open.status, 401, "a non-loopback bind guards every endpoint");
        let allowed = crate::client::get_http(addr, "/status", Some(&token)).await.expect("request");
        assert_eq!(allowed.status, 200);

        serving.abort();
    }
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-api transport::`
Expected: FAIL — `cannot find function select` / `serve_tcp`.

- [x] **Step 3: Write the implementation**

Add to `crates/tinio-api/src/transport.rs`:

```rust
/// The local channel's platform form, as the caller resolved it.
#[derive(Debug, Clone)]
pub enum LocalForm {
    /// A unix socket path.
    Unix(PathBuf),
    /// A Windows pipe name.
    Pipe(String),
}

/// The HTTPS settings, present only when the transport is HTTPS.
#[derive(Debug, Clone)]
pub struct HttpsOptions {
    /// The bind address.
    pub addr: SocketAddr,
    /// The PEM certificate chain.
    pub cert: PathBuf,
    /// The PEM private key.
    pub key: PathBuf,
}

/// What the caller asked for, before exclusivity is applied.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// The local channel, when enabled.
    pub local: Option<LocalForm>,
    /// TCP HTTP, when its section or a `--api http://…` flag enabled it.
    pub http: Option<SocketAddr>,
    /// TCP HTTPS, when its section plus cert/key or a `--api https://…` flag enabled it.
    pub https: Option<HttpsOptions>,
}

/// A bound transport, ready to serve.
#[derive(Debug)]
#[non_exhaustive]
pub enum Transport {
    /// The local channel.
    Local {
        /// The bound channel.
        channel: Channel,
        /// What `state` records as the control channel.
        identity: String,
        /// The token rule (always [`Auth::Local`]).
        auth: Auth,
    },
    /// TCP HTTP.
    Tcp {
        /// The bound listener — `std`, converted to tokio's inside `serve_tcp`, because
        /// `select` runs before the runtime exists.
        listener: std::net::TcpListener,
        /// The bound address.
        addr: SocketAddr,
        /// The token rule: [`Auth::Loopback`] for a loopback bind, [`Auth::Tcp`] otherwise
        /// (`Auth::for_bind`).
        auth: Auth,
    },
    /// TCP HTTPS (feature `tls`).
    #[cfg(feature = "tls")]
    Https {
        /// The bound listener — `std`, as in the `Tcp` arm.
        listener: std::net::TcpListener,
        /// The bound address.
        addr: SocketAddr,
        /// The PEM certificate chain.
        cert: PathBuf,
        /// The PEM private key.
        key: PathBuf,
        /// The token rule: [`Auth::Loopback`] for a loopback bind, [`Auth::Tcp`] otherwise
        /// (`Auth::for_bind`).
        auth: Auth,
    },
}

/// Applies the three-choose-one rule and binds the winner.
///
/// The local arms record the **bound** channel's identity in `Transport::Local { identity, .. }`
/// (`Channel::identity`), never the requested one: what the bind created is what `state`
/// records and what every client dials. `Plane::bind` is what guarantees that identity is
/// absolute.
pub fn select(selection: Selection) -> Result<Transport, Error> {
    let enabled = usize::from(selection.local.is_some())
        + usize::from(selection.http.is_some())
        + usize::from(selection.https.is_some());
    if enabled == 0 {
        return Err(Error::Transport(
            "no management transport is enabled: keep the local channel or configure exactly one of http/https"
                .to_string(),
        ));
    }
    if enabled > 1 {
        return Err(Error::Transport(
            "more than one management transport is enabled: the local channel, http and https are mutually exclusive"
                .to_string(),
        ));
    }

    match (selection.local, selection.http, selection.https) {
        (Some(LocalForm::Unix(path)), None, None) => {
            #[cfg(unix)]
            {
                let channel = crate::bind::bind_unix(&path)?;
                // The *bound* channel's identity, not the requested path: `state.control_name`
                // is absolute, because a relative one stops resolving the moment the daemon
                // `chdir("/")`s — which is exactly when `status` and `stop` dial it.
                Ok(Transport::Local {
                    identity: channel.identity(),
                    channel,
                    auth: Auth::Local,
                })
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(Error::Transport("unix:// is not a Windows transport".to_string()))
            }
        }
        (Some(LocalForm::Pipe(name)), None, None) => {
            #[cfg(windows)]
            {
                let channel = crate::bind::bind_pipe(&name)?;
                Ok(Transport::Local {
                    identity: channel.identity(),
                    channel,
                    auth: Auth::Local,
                })
            }
            #[cfg(not(windows))]
            {
                let _ = name;
                Err(Error::Transport("pipe:// is not a unix transport".to_string()))
            }
        }
        (None, Some(addr), None) => {
            // Bound with the runtime-free type and left that way: `serve_tcp` converts inside
            // the runtime, which is the only place `from_std` may be called at all.
            let listener = std::net::TcpListener::bind(addr).map_err(Error::Io)?;
            listener.set_nonblocking(true).map_err(Error::Io)?;
            // The bound address, not the requested one: `--port 0` and a wildcard bind both
            // change the answer, and the token rule follows what was actually bound.
            let addr = listener.local_addr().map_err(Error::Io)?;
            Ok(Transport::Tcp { listener, addr, auth: Auth::for_bind(addr) })
        }
        (None, None, Some(options)) => {
            #[cfg(feature = "tls")]
            {
                let listener = std::net::TcpListener::bind(options.addr).map_err(Error::Io)?;
                listener.set_nonblocking(true).map_err(Error::Io)?;
                let addr = listener.local_addr().map_err(Error::Io)?;
                Ok(Transport::Https {
                    listener,
                    addr,
                    cert: options.cert,
                    key: options.key,
                    auth: Auth::for_bind(addr),
                })
            }
            #[cfg(not(feature = "tls"))]
            {
                let _ = options;
                Err(Error::Transport(
                    "https:// needs a build with the `tls` feature".to_string(),
                ))
            }
        }
        // Unreachable by the count above — and written as an `Err` rather than
        // `unreachable!()` because this is library code, where the crate's constraints forbid
        // panics and a `clippy::unreachable` under `-D warnings` would have to be allowed
        // anyway. It is the same refusal the count already made, spelled where the compiler
        // can see it.
        _ => Err(Error::Transport(
            "more than one management transport is enabled: the local channel, http and https are mutually exclusive"
                .to_string(),
        )),
    }
}

/// The absolute form of the local channel's socket path: an absolute path is used verbatim, a
/// relative one joins `state_dir` (design decision 24). `state_dir` is absolute — the caller
/// resolved it — so the answer is too, which is what `state.control_name` needs.
fn resolve_socket_path(path: &Path, state_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        state_dir.join(path)
    }
}

/// Serves `shared` over a bound TCP listener until `stop` flips, then drains and returns.
///
/// The `std` listener `select` bound becomes tokio's here — inside the runtime, which is the
/// only place `from_std` may be called — and the graceful shutdown is what makes "cease
/// accepting, drain, then exit" one mechanism for the data plane and the control plane alike
/// (decision 12; the ≤ 10 s bound itself is the CLI's, which awaits this future).
pub async fn serve_tcp(
    listener: std::net::TcpListener,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let listener = tokio::net::TcpListener::from_std(listener).map_err(Error::Io)?;
    axum::serve(listener, router(shared))
        .with_graceful_shutdown(until_stopped(stop))
        .await
        .map_err(|err| Error::Transport(format!("tcp channel: {err}")))
}

impl Transport {
    /// What `state.control_name` records: the bound socket path, the pipe name, or the
    /// address. A client reads this value out of `state` and never re-derives it.
    pub fn identity(&self) -> String {
        match self {
            Self::Local { identity, .. } => identity.clone(),
            Self::Tcp { addr, .. } => addr.to_string(),
            #[cfg(feature = "tls")]
            Self::Https { addr, .. } => addr.to_string(),
        }
    }

    /// The token rule this transport carries (`management-api.md`'s matrix).
    pub fn auth(&self) -> Auth {
        match self {
            Self::Local { auth, .. } | Self::Tcp { auth, .. } => *auth,
            #[cfg(feature = "tls")]
            Self::Https { auth, .. } => *auth,
        }
    }
}

/// What the CLI hands the plane: the transport input, where the plane's files go, and the
/// shutdown channel it sends on.
#[derive(Debug)]
pub struct Options {
    /// The three-choose-one input, as `[api]` and the `--api*` flags resolved it.
    pub selection: Selection,
    /// The resolved state directory (design decision 6) — the plane's only filesystem anchor,
    /// and absolute: a **relative** `[api.unix] path` in `selection` joins this directory
    /// before anything binds ([`Plane::bind`]), so what the bind creates and what `state`
    /// records are the same absolute path.
    pub state_dir: PathBuf,
    /// The served root, canonical: `state.root`, and the input the socket name derives from.
    pub root: PathBuf,
    /// The data plane's host, as `/status` reports it.
    pub host: String,
    /// The shutdown channel `/stop` sends on — the same channel the CLI's signal handlers
    /// flip and `DataPlane::serve` reads (decision 12).
    pub stop: watch::Sender<bool>,
}

/// The management plane: the bound transport, the router it serves, and the two files it owns.
///
/// **One owner for both artifacts** (decision 12): the plane creates the socket at
/// [`Plane::bind`] and owns the `state` file, so it is the plane that writes and removes them —
/// through [`Plane::publish`] (the readiness point) and [`Plane::shutdown`]. The CLI never
/// joins those file names itself, and [`Drop`] is a best-effort backstop for the paths that do
/// not go through `shutdown`.
///
/// **The client never deletes, and the reason is a race.** A `status` or `stop` run removes
/// nothing, because a client that unlinks a socket or a `state` file cannot know whether the
/// server behind it is still live. The one exception is `doctor --fix` (plan C), and it is not
/// a client: it reclaims a stale socket through `crate::bind::reclaim_stale` (the dry-run arm
/// is what its report is built from) and a stale `state` through [`State::remove`], and only
/// after a probe has proved no server answers for that root — the proof is what the rule
/// exists to require, not a second way around it.
#[derive(Debug)]
pub struct Plane {
    /// Taken (once) by `serve`: a listener cannot be cloned, so serving consumes it.
    transport: Mutex<Option<Transport>>,
    shared: Shared,
    state: State,
    state_dir: PathBuf,
    socket: Option<PathBuf>,
}

impl Plane {
    /// Applies the three-choose-one rule and binds the winner — which **is** the
    /// single-instance lock (decision 12) — then builds the router around it.
    ///
    /// The local form is resolved to an **absolute** path *before* the bind (design decision 24,
    /// and the rule [`crate::state::socket_path`]'s doc states): an absolute `[api.unix] path`
    /// is used verbatim, a relative one joins `state_dir`, and the path the bind creates is
    /// therefore the identity `state.control_name` records. A relative name would stop
    /// resolving the moment the daemon `chdir("/")`s — which is exactly when `status` and
    /// `stop` need to dial it — so this is a correctness step, not a convenience.
    pub fn bind(options: Options) -> Result<Self, Error> {
        let mut selection = options.selection;
        if let Some(LocalForm::Unix(path)) = &mut selection.local {
            *path = resolve_socket_path(path, &options.state_dir);
        }
        let transport = select(selection)?;
        let state = State::new(transport.identity(), None, &options.root)?;
        let socket = match &transport {
            Transport::Local { channel, .. } => channel.socket_path(),
            _ => None,
        };
        let shared = Arc::new(Context::new(
            state.clone(),
            options.host,
            transport.auth(),
            options.stop,
        ));
        Ok(Self {
            transport: Mutex::new(Some(transport)),
            shared,
            state,
            state_dir: options.state_dir,
            socket,
        })
    }

    /// Serves until the shutdown channel flips — the plane's own `/stop` sends on that same
    /// channel — then stops accepting, drains the connections already accepted, and returns.
    /// The transport is taken out of the plane here, which makes this a one-shot call; the
    /// drain is `axum::serve`'s `with_graceful_shutdown` (and, on Windows, the pipe loop's
    /// `JoinSet`), and the ≤ 10 s bound on it is the CLI's — this future is what the CLI awaits
    /// with that bound (decision 12), so the plane never invents a second timeout.
    pub async fn serve(&self, stop: watch::Receiver<bool>) -> Result<(), Error> {
        let transport = self
            .transport
            .lock()
            .map_err(|_| Error::Internal("the plane's transport lock was poisoned".to_string()))?
            .take()
            .ok_or_else(|| Error::Internal("the plane is already serving".to_string()))?;

        serve_transport(transport, self.shared.clone(), stop).await
    }

    /// Writes `state` with the data plane's port — the readiness point (decision 12), after
    /// both planes are bound. `/status` reports the same port from here on.
    pub fn publish(&self, port: u16) -> Result<(), Error> {
        let mut state = self.state.clone();
        state.port = Some(port);
        state.write(&self.state_dir)?;
        // Published once: a second call keeps the first port, which is the one `state` holds.
        let _ = self.shared.port.set(port);
        Ok(())
    }

    /// The plane's last act, and the plane's only deletion site — `doctor --fix` reclaims the
    /// same two files, but only after proving no server answers, through `reclaim_stale` and
    /// `State::remove` — the
    /// serves have already ended on the shared channel (the CLI's bounded `≤ 10 s` wait is on
    /// their futures). Async so the CLI's shutdown path is one `await` in the sequence that
    /// also awaits the data plane's drain.
    pub async fn shutdown(&self) -> Result<(), Error> {
        if let Some(socket) = &self.socket {
            match fs::remove_file(socket) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(Error::Io(err)),
            }
        }
        State::remove(&self.state_dir)
    }
}

impl Drop for Plane {
    /// Best-effort backstop only — the paths that matter call [`Plane::shutdown`]. Errors are
    /// deliberately swallowed: nothing may panic on the way out of a failed startup.
    fn drop(&mut self) {
        if let Ok(mut transport) = self.transport.lock() {
            transport.take();
        }
        if let Some(socket) = &self.socket {
            let _ = fs::remove_file(socket);
        }
        let _ = State::remove(&self.state_dir);
    }
}

/// Dispatches to the transport's own serve. **The conversion into tokio's types happens in
/// the arms below, inside the runtime** — `serve_local` and `serve_tcp` take the `std`
/// listeners Task 2 and `select` bound before it existed (and `serve_https` converts too,
/// Task 6) — which is the whole reason `Plane::bind` can be the single-instance lock at a point
/// where no runtime exists.
async fn serve_transport(
    transport: Transport,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    match transport {
        Transport::Local { channel, .. } => serve_local(channel, shared, stop).await,
        Transport::Tcp { listener, .. } => serve_tcp(listener, shared, stop).await,
        #[cfg(feature = "tls")]
        Transport::Https { listener, cert, key, .. } => {
            serve_https(listener, &cert, &key, shared, stop).await
        }
    }
}
```

`pub use self::transport::{Options, Plane};` goes into `crates/tinio-api/src/lib.rs` beside the module list — the re-export this task's Interfaces line promises and the only place those two types are defined.

The `select` binding happens **before** the runtime exists in the CLI's sequence (design §1 steps a–j precede step k), and that ordering is not a preference: `tokio::net::TcpListener::from_std` panics outside a runtime (`# Panics: This function panics if it is not called from within a runtime with IO enabled`), so the TCP and HTTPS arms bind with `std`, keep the `std` listener in the `Transport`, and let `serve_tcp`/`serve_https` convert inside the runtime — and the same rule is why Task 2's unix listener is `std` and why the Windows pipe is created raw. **A `Transport` therefore never holds a tokio listener, and `Plane::bind` still needs no runtime**, which is what keeps it the single-instance lock before the repair (decision 12). The additions above want `std::sync::Mutex`, `std::fs`, `std::io`, `std::path::{Path, PathBuf}`, `tokio::sync::watch`, `crate::state::State` and `crate::router::{Auth, Context, Shared, router}` in `transport.rs`'s scope (Task 6's `serve_https` joins them, and `until_stopped` is Task 4's shared shutdown future).

One story for the plane's two files: the CLI hands the local form in as `LocalForm` (with neither `[api.unix] path` nor `[api.pipe] path` it is `state::socket_path`, and a relative `[api.unix] path` is resolved against the state dir by `Plane::bind` — the one place that guarantees the recorded name is absolute), and its sequence is `Plane::bind` → `Plane::publish(port)` → `Plane::serve(stop)` → `Plane::shutdown()` — it never creates, joins or deletes `state` or the socket itself. The contract's `[api.unix] path` line still reads "default `tinio.sock`" (`config.md:89`, `:153`): decision 24 is the ruling, the design's Bookkeeping edits those two lines of `config.md`, and the `management-api.md:5` copy of the same claim is **this plan's** — Task 3 makes that edit. An over-long socket path is refused by `bind_unix` with the remedy named (`[api.unix] path` may be absolute, so it can point at a short directory): there is **no** automatic fallback, because abstract sockets are not portable (`research.md:56`), and `doctor` reports the resolved path and its length.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-api transport::` and `cargo test -p tinio-api --no-default-features transport::`
Expected: PASS in both graphs (the `https` arms are feature-gated, and `select` without `tls` refuses an `https` selection).

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets -- -D warnings`
Expected: clean. (The `_` arm in `select` is an `Err`, not an `unreachable!()`, so `clippy::unreachable` has nothing to fire on and the crate keeps its no-panic rule: if the compiler can prove the arm dead after the count check, dropping it is also fine — but the count check stays, because it is what makes the refusal's *message* name the rule.)
Report the files pending commit.

---

### Task 6: HTTPS behind the `tls` feature

**Files:**
- Modify: `crates/tinio-api/src/transport.rs`, `crates/tinio-api/src/lib.rs`, `crates/tinio-api/Cargo.toml`
- Create: `crates/tinio-api/tests/fixtures/cert.pem`, `crates/tinio-api/tests/fixtures/key.pem`

**Interfaces:**
- Consumes: `Transport::Https`, `Shared`, `router`, Task 4's `until_stopped`.
- Produces: `pub async fn serve_https(listener: std::net::TcpListener, cert: &Path, key: &Path, shared: Shared, stop: watch::Receiver<bool>) -> Result<(), Error>` and `struct TlsListener` implementing `axum::serve::Listener`. The `std` listener arrives bound and is converted inside, exactly as in `serve_tcp` (`select` ran before the runtime, and `from_std` panics without one).

**Execution record (2026-09-20):** steps 1–6 ran and the review passed, after one fix round. Amendments and measurements worth keeping: the committed pair pins `CA:FALSE` (openssl's default `CA:TRUE` is rejected by webpki for an end-entity leaf, even self-signed and self-trusted); the test's certificate paths had to move into an `async move` block because `&cert`/`&key` cannot be `'static` for `tokio::spawn`; the brief's `transport::https` filter matches no test (the real path is `transport::tests::https_serves_status_over_tls`); `tokio-rustls` resolves to 0.26.5, not 0.26.4; and the fixture README's regeneration fence needed a fix (`$f` where the file is `$f.pem`) — the fence must be run as *text*, not as unrolled steps, or the bug survives. The stall regression measures the full `HANDSHAKE_TIMEOUT` (≈5.01 s) with a silent socket open, and its only assertion is that `/status` answered while the test never closed its end.

- [x] **Step 1: Create the test-only certificate pair**

Generate a self-signed pair for `localhost` with 3650-day validity and commit both PEM files under `crates/tinio-api/tests/fixtures/`. The pair is committed and it is **marked test-only** (design decision 25), in the files and beside them: each PEM carries a header comment saying what it is — self-signed test material, valid for `localhost` only, never to be used for a real deployment — and `crates/tinio-api/tests/fixtures/README` repeats that in prose together with the exact commands that produced the pair, so it can be regenerated rather than trusted. No test outside this crate reads the key, nothing ships it (`tests/` is not packaged), and a committed test key is a key with no trust attached to it — which is what keeps the TLS legs runnable on every platform without `rcgen` or a locally installed `openssl`.

- [x] **Step 2: Write the failing test**

Append to the `tests` module in `crates/tinio-api/src/transport.rs`:

```rust
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn https_serves_status_over_tls() {
        let (cert, key) = crate::test_support::fixture_pair();
        // Bound as `std` and handed over unconverted: that is the shape `select` produces and
        // `serve_https` consumes, and converting here would test a wiring that does not exist.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");

        let state = State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data"))
            .expect("state");
        let (tx, stop) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.clone();

        let serving = tokio::spawn(serve_https(listener, &cert, &key, shared, stop));
        let answer = crate::test_support::https_get(&addr, "/status", Some(&token))
            .await
            .expect("request");
        assert_eq!(answer.status, 200, "{}", answer.body);

        serving.abort();
    }
```

`crate::test_support` is a `#[cfg(test)] pub(crate) mod` in `lib.rs` holding the fixture paths and the rustls client; it is test-only and does not ship.

- [x] **Step 3: Run the test to verify it fails**

Run: `cargo test -p tinio-api --features tls transport::https`
Expected: FAIL — `cannot find function serve_https`.

- [x] **Step 4: Write the implementation**

Add to `crates/tinio-api/src/transport.rs`:

```rust
/// Serves `shared` over TLS with the PEM pair at `cert`/`key`, until `stop` flips.
#[cfg(feature = "tls")]
pub async fn serve_https(
    listener: std::net::TcpListener,
    cert: &Path,
    key: &Path,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let config = tls_config(cert, key)?;
    // The conversion, inside the runtime: `select` bound this before one existed.
    let listener = tokio::net::TcpListener::from_std(listener).map_err(Error::Io)?;
    axum::serve(TlsListener::new(listener, config), router(shared))
        .with_graceful_shutdown(until_stopped(stop))
        .await
        .map_err(|err| Error::Transport(format!("https channel: {err}")))
}

/// The rustls server config, from a PEM chain and key.
#[cfg(feature = "tls")]
fn tls_config(cert: &Path, key: &Path) -> Result<Arc<tokio_rustls::rustls::ServerConfig>, Error> {
    use std::io::BufReader;

    use tokio_rustls::rustls::ServerConfig;

    let certs = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(cert).map_err(Error::Io)?,
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|err| Error::Transport(format!("{}: {err}", cert.display())))?;
    if certs.is_empty() {
        return Err(Error::Transport(format!("{}: no certificate", cert.display())));
    }
    let key = rustls_pemfile::private_key(&mut BufReader::new(
        std::fs::File::open(key).map_err(Error::Io)?,
    ))
    .map_err(|err| Error::Transport(format!("{}: {err}", key.display())))?
    .ok_or_else(|| Error::Transport(format!("{}: no private key", key.display())))?;

    // No process-level provider installation needed: the workspace pin compiles exactly one
    // (`tokio-rustls` with `default-features = false` and `ring`, Task 1), so `builder()` cannot
    // hit the ambiguous-provider panic — and the same pin keeps `aws-lc-sys` (a C/CMake/NASM
    // build) and its second `windows-sys` out of every build, the duplicate the ledger refused
    // `windows-service` 0.8.0 over (decision 22).
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| Error::Transport(format!("TLS configuration: {err}")))?;
    Ok(Arc::new(config))
}

/// An axum `Listener` that terminates TLS on each accepted connection.
///
/// **No `Debug` derive, deliberately**: `tokio_rustls::TlsAcceptor` implements only `Clone`
/// (`tokio-rustls-0.26.4/src/server.rs:18-21`), so deriving `Debug` here does not compile, and
/// nothing needs it — `axum::serve` awaits a listener without printing it (only
/// `impl Debug for Serve` wants `L: Debug`, and nothing in this crate prints the `Serve`).
#[cfg(feature = "tls")]
pub struct TlsListener {
    inner: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
}

/// How long a connection may take to complete its TLS handshake before it is dropped.
///
/// **The handshake runs inside `accept()`, which is the whole reason this exists.**
/// `tokio-rustls`'s `TlsAcceptor::accept` has no timeout of its own, and axum only spawns the
/// per-connection task *after* `Listener::accept` returns (`axum-0.8.9/src/serve/mod.rs:189-191`),
/// so a client that connects and then sends nothing would park the accept loop — and with it
/// `/status`, `/stop` and the readiness probe — for as long as it stays connected, with no
/// authentication and one TCP connection. Five seconds is orders of magnitude more than a real
/// handshake needs on a management port.
#[cfg(feature = "tls")]
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(feature = "tls")]
impl TlsListener {
    /// Wraps a bound TCP listener with `config`.
    pub fn new(listener: tokio::net::TcpListener, config: Arc<tokio_rustls::rustls::ServerConfig>) -> Self {
        Self {
            inner: listener,
            acceptor: tokio_rustls::TlsAcceptor::from(config),
        }
    }
}

#[cfg(feature = "tls")]
impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = std::net::SocketAddr;

    /// The trait's method has **no** `Result` (`axum-0.8.9/src/serve/listener.rs:9-24`: "if the
    /// underlying accept call can return an error, this function must take care of logging and
    /// retrying"), so both failures below are absorbed here: a failed `accept` retries with
    /// axum's own back-off (`listener.rs:140-158` — silence for a client hang-up, one `error!`
    /// and a one-second sleep for anything else, **on every occurrence**, which is what keeps a
    /// persistent `EMFILE` a slow loop rather than a spin), and a failed handshake drops that
    /// one client.
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.inner.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    // Unconditional retry back-off, and outside the logging branch: a listener
                    // that is out of handles or out of addresses must not be retried in a tight
                    // loop (axum sleeps on every such error, and this keeps that behaviour).
                    if !is_connection_error(&err) {
                        tracing::error!(error = %err, "the TLS listener's accept failed");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    continue;
                }
            };
            let handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.acceptor.accept(stream));
            match handshake.await {
                Ok(Ok(stream)) => return (stream, addr),
                Ok(Err(err)) => {
                    tracing::debug!(%addr, error = %err, "TLS handshake failed");
                }
                Err(_elapsed) => {
                    // The socket is dropped with the timeout's future: a client that never
                    // spoke releases the loop instead of holding it.
                    tracing::debug!(%addr, "TLS handshake timed out");
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Whether an accept error is a client hanging up mid-handshake rather than a listener
/// problem: those are routine, and logging them would be noise, not information.
#[cfg(feature = "tls")]
fn is_connection_error(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(
        err.kind(),
        ErrorKind::ConnectionRefused | ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset
    )
}
```

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-api --features tls` and `cargo test -p tinio-api`
Expected: PASS in both; without `tls` the HTTPS code does not compile in, and `select` refuses an `https` selection (Task 5's test covers that).

**Add the stall regression to the same module, because it is the property the handshake timeout exists for and it is not visible in the happy path**: a second `#[cfg(feature = "tls")]` test connects a bare `std::net::TcpStream` to the bound address and sends nothing, then asserts that a real client's `/status` still answers (the plain `std` client from `test_support`, or a second `https_get`) — i.e. that the silent connection did not take the accept loop. Without the timeout that test hangs, which is exactly the failure a client with no ClientHello could produce in production; with it, the silent socket is dropped after `HANDSHAKE_TIMEOUT` and the assertion passes. Report the elapsed time in the task's report rather than asserting on it (a five-second bound is not a timing test).

- [x] **Step 6: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets --features tls -- -D warnings` and the same without the feature.
Expected: both clean.
Report the files pending commit.

---

### Task 7: `/metrics` — the injected metrics source, the reader task over its storage scan, and the interim route's removal

**Files:**
- Create: `crates/tinio-api/src/metrics.rs`
- Modify: `crates/tinio-api/src/router.rs` (a `gauges` field on `Context`, the `/metrics` route **above the `.layer(...)` line** and its tests), `crates/tinio-api/src/transport.rs` (Step 4 fills `Options::gauges` through `Plane::bind` and starts the reader in `Plane::serve`), `crates/tinio-api/src/lib.rs`, `crates/tinio-api/Cargo.toml` (dep `prometheus`; **no `moka`** — the TTL is the reader task's cadence, not a cache), `crates/tinio-server/src/data.rs` (the route removed) and its test module, `crates/tinio-server/src/lib.rs` (the `MetricsRefresh` re-export — one line, Step 4), `crates/tinio-server/src/metrics.rs` and `crates/tinio-server/src/backend/mod.rs` (the live F10 docs of Step 4), `crates/tinio-server/examples/serve.rs` (its `/metrics` wiring comment — stale from this task until plan B retires the file), `crates/tinio-e2e/tests/features/metrics.feature` (the data-plane scrape retired — Step 4), `specs/001-s3-local-server/contracts/s3-surface.md` (its data-plane `/metrics` section, `:309-311`, and the coverage-table row, `:391`), `specs/001-s3-local-server/contracts/config.md` (`:160`), `specs/001-s3-local-server/quickstart.md` (`:71-86`), `specs/001-s3-local-server/pipeline-spec.md` (`:7`, `:229`), `specs/001-s3-local-server/pipeline-spec-review.md` (its F10 sentence), `specs/001-s3-local-server/checklists/security.md` (`:84`), `docs/superpowers/specs/2026-09-05-s3-acl-owner-design.md` (`:65`)
- **Not** modified: `crates/tinio-api/Cargo.toml`'s dependency list gains no `tinio-server` entry (Step 3's note says why, and it is not negotiable); historical records — `code-review-findings.md`, completed plans and their reports — are **not** swept by the doc edits of Step 4, they record what was true when they were written.

**Interfaces:**
- Consumes: the router's `Context`, and the caller's `MetricsSource` — **not** `Storage`: the trait is dyn-incompatible by design (`tinio-core/src/storage/mod.rs:23-25`: an associated `Error` plus the `where Self: Storage` category bounds), so no `Arc<dyn Storage>` exists anywhere in this workspace and this crate never holds a backend.
- Produces: `tinio_api::metrics::{Gauges, MetricsSource, DEFAULT_TTL}` with `pub trait MetricsSource: Send + Sync { fn refresh(&self); fn counts(&self); }` — **the injection point**, defined here and implemented by the caller, split the way the cost is split: `refresh` is the cheap half (every scrape), `counts` the expensive one (one full storage scan, run by the reader task below and **never from a request**). **No number crosses this boundary** — the counts would have to be written into `tinio-server`'s three statics, which only the caller can name, and they cannot be computed here either (this crate has no storage). Plus `Gauges::new(source: Arc<dyn MetricsSource>, ttl: Duration) -> Gauges`; `pub fn refresh(&self)` (the cheap half only); `pub fn render(&self) -> Result<String, Error>`; `pub fn spawn_reader(self: &Arc<Self>, stop: watch::Receiver<bool>) -> tokio::task::JoinHandle<()>`.

**Execution record (2026-09-20):** steps 1–6 ran and the review passed. Notes worth keeping: the brief's cucumber command (`--tags 'not @interop'`) **replaces** the harness's whole default filter, so it selects `@boto3`/`@parquet` scenarios this host cannot run — the canonical default-filter run (236/236 scenarios) is the evidence; five imports (not three) went dead with the removed route; the two router loops Task 3 deliberately left incomplete now assert a concrete `200` rather than `assert_ne!(401)`; and `T146` joined `traceability.rs`'s `NOT_COVERED_BY_CUCUMBER` because the plan-audit commit's `config.md` row had left that leg red at HEAD (verified read-only against `c0…`'s parent).

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-api/src/metrics.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::watch;

    use super::*;

    /// The caller's side of the injection point: the CLI's adapter has this shape, and so does
    /// the one Task 10's integration leg supplies. Both methods are side-effecting in the real
    /// adapter — `refresh` republishes the cheap families, `counts` runs the storage scan and
    /// sets the three gauges — so both are counted here, and `counts` is the number the reader's
    /// cadence exists to keep down.
    #[derive(Default)]
    struct Counting {
        refreshes: Arc<AtomicUsize>,
        counts: Arc<AtomicUsize>,
    }

    impl MetricsSource for Counting {
        fn refresh(&self) {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
        }

        fn counts(&self) {
            self.counts.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn the_scan_runs_once_per_ttl_on_the_reader_task_and_never_from_a_scrape() {
        // The *instance's* ttl, not `DEFAULT_TTL`: construct the gauges with 20 ms and the
        // cadence is observable in a test rather than in half a minute. The reader scans once
        // at start so the first scrape is not empty, then once per ttl. (The first assertion is
        // after the sleep rather than immediately after `spawn_reader`: a spawned task is not
        // guaranteed to have been polled yet, and a test that assumes otherwise is flaky.)
        let source = Arc::new(Counting::default());
        let gauges = Arc::new(Gauges::new(source.clone(), Duration::from_millis(20)));
        let (tx, rx) = watch::channel(false);
        let reader = gauges.spawn_reader(rx);

        tokio::time::sleep(Duration::from_millis(60)).await;
        let scanned = source.counts.load(Ordering::SeqCst);
        assert!(scanned >= 2, "the reader scanned at start and kept scanning on its ttl: {scanned}");

        // The regression this task exists for: any number of scrapes, and the scan does not
        // run at all — a `/metrics` flood cannot park a runtime worker on a storage scan.
        for _ in 0..8 {
            gauges.refresh();
        }
        assert_eq!(source.counts.load(Ordering::SeqCst), scanned, "no scrape scans");

        tx.send(true).expect("the reader is listening");
        reader.await.expect("the reader exits on the channel");
    }

    #[tokio::test]
    async fn the_cheap_half_refreshes_on_every_scrape() {
        let source = Arc::new(Counting::default());
        let gauges = Gauges::new(source.clone(), Duration::from_secs(30));

        gauges.refresh();
        gauges.refresh();
        assert_eq!(
            source.refreshes.load(Ordering::SeqCst),
            2,
            "cheap families refresh on every scrape"
        );
    }
}
```

There is no storage-less constructor to fall back on: `Gauges::new` has one shape, and the whole of its input is the injected source. **The cadence is the reader task's, and there is no cache behind it** — a cache would be a second, competing mechanism for one rule ("how often may a full scan run"), and this repository's ownership checks hunt exactly that: the reader's `ttl` is the rule, the task is single-flight by construction (one task, one scan at a time), and `refresh()` — the only thing a scrape calls — touches the cheap half alone.

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-api metrics::`
Expected: FAIL — unresolved module `metrics` / `cannot find type Gauges`.

- [x] **Step 3: Write the implementation**

Prepend to `crates/tinio-api/src/metrics.rs`:

```rust
//! `GET /metrics`: the injected metric source, the registry gather, and the reader task that
//! runs the expensive half off the serving path.
//!
//! The HTTP and S3-operation families are recorded by the data plane as requests happen. The
//! rest live in the registry `tinio-server` owns, and `tinio-api` must not depend on that
//! crate — the edge would be a **dependency cycle** (cargo rejects it at resolution time, even
//! with `tinio-server`'s optional `tinio-api` feature off). So the registry side is *injected*:
//! [`MetricsSource`] is what the caller implements — the CLI, which already depends on both
//! crates — and this module only drives it, splitting the work the way its cost splits.
//!
//! [`MetricsSource::refresh`] (the pipeline gauges and the write-lock histograms) is cheap and
//! runs on every scrape, from the handler. [`MetricsSource::counts`] (the storage scan and the
//! three gauges it sets) is a full listing pass over the served root, so **no request ever runs
//! it**: [`Gauges::spawn_reader`] is a single background task that scans once at start and then
//! once per `ttl` — single-flight by construction, because there is one task and it scans
//! sequentially — and the handler only gathers the registry the scan has already filled. That
//! is what bounds the scrape's cost at the design ceiling regardless of how large the root is
//! or how often the endpoint is hit, and why `/metrics` cannot park a runtime worker the way an
//! inline scan would (`FR-019`, `SC-008`; a scrape-triggered scan would also be a small denial
//! of service on every runtime worker, since `/metrics` is unauthenticated on the local channel
//! and on a loopback bind). **No number crosses this boundary**: the three counts would have to
//! be written into `tinio-server`'s statics, which only the caller can name, and they cannot be
//! computed in this crate either — it has no storage to scan. This supersedes decision 23's
//! "gating `counts()` behind the TTL" and §3's "30 s TTL cache" phrasing: the TTL is the
//! reader's cadence, there is no cache, and the design's two sentences are corrected in the
//! same commit.

use std::{sync::Arc, time::Duration};

use prometheus::{Encoder, TextEncoder, default_registry};
use tokio::sync::watch;

use crate::Error;

/// The registry side of a scrape, supplied by whoever builds the plane (design decision 23).
///
/// The CLI implements it, and it is the only crate that can: `refresh` calls
/// `tinio_server::metrics::refresh(io, db, write_lock)` — two `tinio_core::pipeline::Stats`
/// snapshots and one `tinio_server::metrics::WriteLockStats` — and `counts` scans the caller's
/// own backend and publishes the result with `IntGauge::set` on `STORAGE_BUCKETS`,
/// `STORAGE_OBJECTS` and `STORAGE_BYTES`, the three public statics only `tinio-server` can
/// name. Nothing in this crate names `tinio-server`, and adding that dependency is
/// **forbidden** — it is the reverse edge of `tinio-server`'s optional `tinio-api` dependency
/// and cargo refuses the cycle.
///
/// Neither method returns anything, and that is the shape of the seam rather than an
/// oversight: every number a scrape needs is written into the registry the caller already
/// holds, and a copy handed back here could not be used by anything on this side.
///
/// **`refresh` is called from a request; `counts` never is.** The reader task owns the second
/// one, so an implementation may block for as long as the scan takes without any request
/// paying for it — and an implementation that needs blocking or async I/O is still free to
/// schedule it (`tokio::task::spawn_blocking`, or the caller's own machinery), because nothing
/// on a request's path waits for the answer.
pub trait MetricsSource: Send + Sync {
    /// Cheap, and runs on every scrape: the pipeline gauges and the write-lock histograms.
    fn refresh(&self);
    /// Expensive, and run by [`Gauges`]'s reader task once per ttl — never from a scrape: the
    /// storage scan and the three gauges it sets.
    fn counts(&self);
}

/// The default ttl of the storage-layer scan: how often the reader task rescans.
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

/// The registry's injected side, and the reader task that keeps it fresh.
///
/// There is no cache behind [`Gauges::refresh`]: the cadence lives in one place — the reader's
/// `ttl` — and the reader is a single task, so "one scan at a time" is a property of the shape
/// rather than of a lock or a timestamp comparison.
#[derive(Clone)]
pub struct Gauges {
    source: Arc<dyn MetricsSource>,
    ttl: Duration,
}

impl std::fmt::Debug for Gauges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gauges")
            .field("source", &"<injected>")
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl Gauges {
    /// Builds the scrape-time source over the caller's adapter; `ttl` is how often the reader
    /// task runs that adapter's expensive half — the storage scan.
    pub fn new(source: Arc<dyn MetricsSource>, ttl: Duration) -> Self {
        Self { source, ttl }
    }

    /// Refreshes the cheap half. **This is the only method a scrape calls**, and it never
    /// scans: the storage gauges are whatever the reader task last published.
    pub fn refresh(&self) {
        self.source.refresh();
    }

    /// Starts the one task that runs the expensive half: once immediately (so the first scrape
    /// is not empty), then every `ttl`, until `stop` flips. Single-flight by construction —
    /// one task, scanning sequentially — so a scrape flood cannot multiply scans, and the task
    /// exits with the plane's own shutdown channel rather than being aborted.
    pub fn spawn_reader(self: &Arc<Self>, mut stop: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        let gauges = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                // The scan is synchronous and can take seconds on a large root, so it runs on
                // the **blocking pool**: called inline it would occupy one worker of the serving
                // runtime for the scan's whole duration, once per `ttl` and therefore
                // permanently, while `/stop`, `/status` and the data plane share that pool. A
                // blocking task that fails to complete (a panic inside the scan) is logged and
                // the loop keeps its cadence rather than exiting with it.
                let scan = Arc::clone(&gauges);
                if tokio::task::spawn_blocking(move || scan.source.counts()).await.is_err() {
                    tracing::warn!("the metrics scan task did not complete");
                }
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep(gauges.ttl) => {}
                }
            }
        })
    }

    /// Encodes the registry in the Prometheus text format.
    pub fn render(&self) -> Result<String, Error> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let families = default_registry().gather();
        encoder
            .encode(&families, &mut buffer)
            .map_err(|err| Error::Internal(format!("metrics encoding: {err}")))?;
        String::from_utf8(buffer).map_err(|err| Error::Internal(format!("metrics encoding: {err}")))
    }
}
```

The injected source is the whole point of this shape. The three storage families (`STORAGE_BUCKETS`, `STORAGE_OBJECTS`, `STORAGE_BYTES`) and the cheap families are all registered globally by `tinio-server` (`metrics.rs:117-134`), and `tinio-api` reaches them through the caller's `MetricsSource` only.

**The adapter is the CLI's, in plan B's Task 3** — it is the only crate that depends on both — and it implements both methods from calls that actually exist. `refresh` **is** `tinio_server::metrics::refresh(io: Stats, db: Stats, write_lock: WriteLockStats)`, which takes exactly three arguments (`crates/tinio-server/src/metrics.rs:358`): the two `Stats` are `tinio_core::pipeline::Stats` snapshots (the IO and DB pipelines) and the third is `tinio_server::metrics::WriteLockStats`. `counts` is the storage scan **plus** the three gauges that scan feeds, because the numbers have no setters to wrap — the public statics *are* the mutation path, `IntGauge::set` on `STORAGE_BUCKETS` (`metrics.rs:117`), `STORAGE_OBJECTS` (`:123`) and `STORAGE_BYTES` (`:129`), and the CLI is the only crate that can name them. Nothing is returned by either call: the counts land in the registry as they are computed, and `Gauges` records only *when* the expensive half last ran — as the reader task's own position in its loop, not as a cache entry, because there is no cache (Step 1's note). **`counts` is off the request path**: the reader calls it once per `DEFAULT_TTL`, so plan B's adapter may block inside it and pick its own scheduling (`spawn_blocking`, or the storage's own machinery) without any request paying for the scan. Plan B's adapter also needs the `serve`-side hook to be nameable: `tinio_server::MetricsRefresh` (`crates/tinio-server/src/data.rs:75`, `Arc<dyn Fn() + Send + Sync>`) is **not reachable today** — `mod data` is private (`lib.rs:35`) and the re-export at `lib.rs:45-49` carries only `DataPlane`/`PlaneConfig` — so Step 4 adds it to that re-export (one line), which is why `crates/tinio-server/src/lib.rs` is in this task's file list.

**Adding a `tinio-api` → `tinio-server` dependency is forbidden, not merely discouraged**: `tinio-server` already carries `tinio-api = { optional = true }` (`crates/tinio-server/Cargo.toml:56,105`), and cargo rejects the cycle at **resolution** time — `error: cyclic package dependency` — even with that optional feature off, which was measured against a minimal two-crate workspace on 2026-09-20 (design decision 23). This task's diff must not contain that edge at all; a review that finds it has found a defect rather than a trade-off to weigh.

- [x] **Step 4: Add the route and remove the interim one**

`crates/tinio-api/src/router.rs` — add `pub gauges: Option<Arc<Gauges>>` to `Context` (`None` from `Context::new`, the route's default state), have `Plane::bind` fill it from a new `Options::gauges: Option<Arc<Gauges>>` field (`crates/tinio-api/src/transport.rs` — the field is added to `Options` there and threaded into the `Context` the plane builds), add `.route("/metrics", get(metrics))` **above the `.layer(...)` line** (Task 3's note: a route added after the layer is served with no token check at all), and:

```rust
async fn metrics(AxumState(shared): AxumState<Shared>) -> Response {
    // Deliberate and contractual: a plane whose source was never attached has no `/metrics`,
    // so a scraper sees the endpoint's absence rather than an empty 200 (decision 8's
    // round-2 ruling — keep the 404, change the contract).
    let Some(gauges) = &shared.gauges else {
        return (StatusCode::NOT_FOUND, "metrics are not attached").into_response();
    };
    // Cheap half only, and no `await`: the storage scan is the reader task's (`spawn_reader`),
    // so this handler never touches the backend and never scans — it refreshes the pipeline
    // families the caller's adapter republishes and gathers the registry the reader has
    // already filled. A scrape's cost is therefore independent of the root's size and of how
    // often the endpoint is hit.
    gauges.refresh();
    match gauges.render() {
        Ok(text) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            text,
        )
            .into_response(),
        Err(err) => err.into_response(),
    }
}
```

**The reader task starts with the plane, in `Plane::serve`,** which is the one place that has both the `Arc<Gauges>` and the runtime: `let reader = self.shared.gauges.as_ref().map(|gauges| gauges.spawn_reader(stop.clone()));` before `serve_transport(...)`, and the handle is dropped where the serve returns — the task watches the same channel `serve` waits on, so it exits with the plane rather than being aborted, and a plane with `gauges: None` starts nothing. **Plan B's `ServerMetrics` adapter changes with this**: its `counts` is no longer on a scrape path at all — the reader calls it once per `DEFAULT_TTL`, and the adapter's own docs say so — while `refresh` stays what the scrape calls (plan B's Task 3 owns that edit, and this plan's Task 10's ownership check names the reader as the scan's caller).

Both of the route's fixed points are tested here, in `router.rs`'s test module: with gauges attached, `/metrics` answers `200` without a token under `Auth::Local` and `Auth::Loopback` and `401` without one under `Auth::Tcp` (this is where Task 3's and Task 5's `assert_ne!(401)` legs become the `200` the ruling names); with `gauges: None`, it answers `404` — the test the 404 must not lose. Add a `gauged(auth)` helper beside Task 3's `shared(auth)` that builds a `Context` with `Some(Arc::new(Gauges::new(Arc::new(Counting::default()), DEFAULT_TTL)))` (a local `MetricsSource` double, as in Step 1; the reader is deliberately **not** started here — the route's tests assert the 200 and the 404, and Step 1's test is what asserts the cadence), and one test per point.

`crates/tinio-api/src/lib.rs` — add `pub mod metrics;` in alphabetical position, and re-export `Gauges`, `MetricsSource` and `DEFAULT_TTL` beside `Plane`/`Options`, so the CLI names them as `tinio_api::metrics::{…}` or `tinio_api::{Gauges, MetricsSource, DEFAULT_TTL}`.

`crates/tinio-server/src/data.rs` — **every deletion named, so none is missed**: `METRICS_PATH` (`:78`) and the function only it feeds, `is_metrics_request` (`:362`) — its two callers are the request branch inside `WithPeer::call` and the deleted test, so nothing is left dangling; the branch itself (`:353-355`); `metrics_response` (`:369`); the test `metrics_endpoint_refreshes_and_serves_the_registry` (`:988`) with its F10 comment; and three imports that go with them and turn into clippy errors under this task's `-D warnings` gate — `prometheus::{Encoder, TextEncoder, default_registry}` (`:45`), `metrics::WriteLockStats` (the test module's, `:750`), and `CONTENT_TYPE` (`:34`). Keep `with_metrics`/`MetricsRefresh`: the management plane's scrape still needs the server's cheap families, the CLI's `MetricsSource` adapter is what calls them, and `tinio_server::MetricsRefresh` is what plan B names.

`crates/tinio-server/src/lib.rs` — add `MetricsRefresh` to the existing re-export (`data::{DataPlane, MetricsRefresh, PlaneConfig}`, `lib.rs:45-49`). One line, and the reason is plan B's: the type lives in the private `mod data`, so without this line the CLI cannot name the hook it has to install.

The four **live** doc comments whose F10 wording this task falsifies, all in `crates/tinio-server/src/data.rs` — the module doc has no F10 sentence, so do not go looking for one there: `MetricsRefresh`'s (`:71-75`), `with_metrics`' (`:272-276`), `WithPeer::call`'s (`:350-355`) and `DataPlaneService.metrics`' (`:393-395`). Each says the `/metrics` endpoint is on the data-plane listener and that the hook fires there; each is rewritten to say the scrape lives on the management plane, that `with_metrics` is the hook the CLI's adapter drives, and that nothing in this file serves the path any more. (A fifth F10 comment, `metrics_response`'s at `:366-368`, goes with the function.)

`crates/tinio-e2e/tests/features/metrics.feature` — the `@T075 @SC-008` scenario scrapes the **data-plane** `/metrics` (its header comment names F10) and fails the moment the route is gone, so it moves in this task. It cannot be retargeted to the control channel here: the harness spawns `tinio-server`'s `serve` example (`steps/clients.rs`), which starts no management plane until plan B's harness migration moves the suite onto the `tinio` binary. Retire the data-plane scrape accordingly, and word the retirement as what it now is: after this task the data-plane `/metrics` is **no longer intercepted** — it is routed as the bucket named `metrics`, like any other first path segment. The scenario asserts exactly that, and its comment says why the fixture's answer is a consequence rather than a guarantee: the suite plants no bucket called `metrics`, so `GET /metrics` on the data-plane listener answers `404` **in this fixture**, and a root with a real bucket by that name would answer `200`. Asserting the interception's absence is the regression; pinning the `404` as if it were the contract's promise is not, and a future fixture that adds such a bucket must be free to pass. The comment also says that the three-layer assertion lives in Task 10's management-plane leg for now and that the control-channel form returns with plan B's harness. Keep its `@T075 @SC-008` tags so `crates/tinio-e2e/tests/traceability.rs` stays satisfied, and name the split in the task's report.

`specs/001-s3-local-server/contracts/s3-surface.md` — its `### /metrics` section (`:309-311`) documents the data-plane route's pre-route interception, and the coverage table's `GET /metrics` row (`:391`) points the scenario at that path. Both are rewritten: the endpoint lives on the management plane now, `management-api.md` carries its token rule, and the coverage row names the management plane's leg.

**The rest of the live prose this task makes false**, edited here rather than deferred — one sweep, no "the design's Bookkeeping owns it" left in this task:

- `specs/001-s3-local-server/contracts/config.md:160` — the interim sentence goes: the `serve` example's unauthenticated data-plane `GET /metrics` does not exist after this task, and the token rule above it governs the management plane's `/metrics` unconditionally.
- `specs/001-s3-local-server/quickstart.md:71-86` — it curls the **old fixed** `tinio.sock` and asserts that a token-less TCP `/metrics` answers `401`; both are wrong after this plan (the socket name is derived, and a loopback bind keeps `/metrics` open). The section is rewritten to the derived path and to the bind-address rule, leaving the non-loopback `401` as the only `401` it promises.
- `specs/001-s3-local-server/pipeline-spec.md:7` and `:229` — the F10 revision note and the metrics bullet both say the families are served by a reserved `GET /metrics` on the data-plane listener; they now say the families are registered once at startup and served by the management plane's token-gated `/metrics` (T075).
- `specs/001-s3-local-server/pipeline-spec-review.md` — its F10 sentence carries the same claim about the reserved data-plane path, and it is a live review of a spec that is still in force; it gets the same one-sentence correction rather than being classed with the completed plans. (If the reviewer judges it a historical record instead, leave it and say so in the task's report — but the choice is made explicitly, not by omission.)
- `crates/tinio-server/examples/serve.rs:335-347` — the example's `with_metrics` wiring comment names the `/metrics` scrape endpoint (F10) and is stale from this task until plan B deletes the file in the harness switch. Rewrite the comment (the hook is what the CLI's adapter drives now); do **not** remove the wiring, because the example still has to build and the data plane still installs its refresh hook.
- `specs/001-s3-local-server/checklists/security.md:84` — CHK045's `/metrics` clause ("management-plane interception, FR-018/FR-019") is restated for a route that is no longer on the data plane.
- `crates/tinio-server/src/backend/mod.rs:26-29` — the ACL module docs' "**`/metrics` stays outside the ACL check** … intercepted pre-route in `data.rs`" becomes a pointer to the management plane; the ACL posture is unchanged, only the path's owner is.
- `crates/tinio-server/src/metrics.rs:349-357` and `:542-543` — `refresh`'s doc and the snapshot test's comment both say the server's `/metrics` endpoint calls the refresh on every scrape; they now say the *management plane* does, through the CLI's adapter.
- `docs/superpowers/specs/2026-09-05-s3-acl-owner-design.md:65` — the grilling ruling that `/metrics` is pre-route-intercepted in `data.rs` is a live design decision this task falsifies; it is amended to the management plane rather than left as the record of a route that no longer exists. (Historical records — `code-review-findings.md`, completed plans and their reports — are not touched: they record what was true when they were written.)

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-api metrics::` and `cargo test -p tinio-server`
Expected: PASS. A tinio-server test that asserted the data-plane route is now gone rather than failing — if the compiler reports it, that is this task's edit, not a broken test.
Run: `cargo test -p tinio-e2e --test cucumber -- --tags 'not @interop'` and `cargo test -p tinio-e2e --test traceability`
Expected: PASS — **with** the scenario's data-plane scrape gone. This leg must not be reported green while the scenario still points at the data-plane path: the suite failing here means the scenario was not retired, and the task is not done until it is.

- [x] **Step 6: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets -- -D warnings` and `cargo clippy -p tinio-server --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 8: `/openapi.json` behind the `openapi` feature

**Files:**
- Create: `crates/tinio-api/src/openapi.rs`
- Modify: `crates/tinio-api/src/router.rs`, `crates/tinio-api/src/lib.rs`, `crates/tinio-api/Cargo.toml`

**Interfaces:**
- Consumes: `StatusBody`, `StopBody`, `Auth`.
- Produces: `pub fn document() -> utoipa::openapi::OpenApi` and a `/openapi.json` route registered only under the feature.

**Execution record (2026-09-20):** steps 1–5 ran and the review passed. Notes worth keeping, all four of them things the block below does not say: `StatusBody` and `StopBody` each need their own `#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]`, because `components(schemas(...))` cannot compile without it and `document()` is the only place those two names reach utoipa; the four `#[utoipa::path]` stubs each need `#[allow(dead_code)]`, because the macro re-emits the annotated item and never references it, so a stub without the allow is a dead-code error under `-D warnings`; the route is registered above the crate's **`.route_layer(...)`** line rather than the `.layer(...)` one the prose names — Task 3 chose `route_layer` deliberately, since `layer` also wraps the fallback router and would answer `401` for an unrouted path instead of leaking nothing with the contract's plain `404`; and the summaries the generated JSON carries come from the stubs' `///` doc comments, which is why those lines describe the endpoints rather than the stub functions they sit on.

- [x] **Step 1: Write the failing test**

Append to `crates/tinio-api/src/openapi.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_document_covers_every_endpoint_and_the_token_scheme() {
        let text = document().to_json().expect("json");
        for path in ["/status", "/stop", "/metrics", "/openapi.json"] {
            assert!(text.contains(path), "{path} is missing from the document");
        }
        assert!(text.contains("X-Tinio-Token"), "the token scheme is documented");
    }
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-api --features openapi openapi::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

```rust
//! The OpenAPI document (`management-api.md:59`), behind the `openapi` feature.
//!
//! `research.md:96-97` fixes the crate and the feature: `utoipa` with `axum_extras`, no
//! Swagger UI in v1 — the spec JSON only.

use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::router::{StatusBody, StopBody};

/// The document, with the token security scheme attached.
pub fn document() -> utoipa::openapi::OpenApi {
    #[derive(OpenApi)]
    #[openapi(
        paths(
            crate::router::status_doc,
            crate::router::stop_doc,
            crate::router::metrics_doc,
            crate::router::openapi_doc
        ),
        components(schemas(StatusBody, StopBody)),
        modifiers(&TokenScheme)
    )]
    struct Api;

    Api::openapi()
}

struct TokenScheme;

impl Modify for TokenScheme {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "token",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("X-Tinio-Token"))),
            );
        }
    }
}
```

The handler doc stubs (`status_doc`, `stop_doc`, `metrics_doc`, `openapi_doc`) are `#[utoipa::path]`-annotated functions in `router.rs` that delegate to the real handlers; `#[utoipa::path]` needs its own item, and keeping them beside the handlers is what stops the two from drifting. **All four** are registered, because the contract promises all four (`management-api.md:19-24`): the test above asserts the document carries `/status`, `/stop`, `/metrics` and `/openapi.json`, and the fourth stub is what makes that assertion true rather than aspirational — leave it out and the test fails on `/openapi.json`, which is the correct outcome. Register the route:

```rust
#[cfg(feature = "openapi")]
async fn openapi_json() -> axum::Json<utoipa::openapi::OpenApi> {
    axum::Json(crate::openapi::document())
}
```

`.route("/openapi.json", get(openapi_json))` under `#[cfg(feature = "openapi")]` — **registered inside `router()`, above the `.layer(...)` line**, like Task 7's (Task 3's note: a route added after the layer is served with no token check at all, and this one carries the API schema) — and no new guard entry: `Auth::guards` already carries the rule (open on the local channel, guarded on a loopback bind and everywhere else; `management-api.md:11`). The document says in prose that `Authorization: Bearer <token>` is accepted as an alias for the header on every transport, which is the one part of the token rule a generated scheme cannot express.

`crates/tinio-api/src/lib.rs` — `#[cfg(feature = "openapi")] pub mod openapi;` in alphabetical position, with `#[cfg(feature = "openapi")] pub use self::openapi::document;` beside it so a caller can name `tinio_api::document` without the module path. Both carry the same `cfg`, which is what keeps the default graph free of the module and of `utoipa`.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-api --features openapi` and `cargo test -p tinio-api`
Expected: PASS in both graphs.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets --features openapi -- -D warnings` and `cargo hack check -p tinio-api --each-feature --no-dev-deps`
Expected: clean; every feature combination compiles.
Report the files pending commit.

---

### Task 9: The thin client

**Files:**
- Modify: `crates/tinio-api/src/client.rs` (**created early by Task 4** with the `pub(crate)` test helpers — this task promotes them and adds the rest; there is no second file and no second definition), `crates/tinio-api/src/lib.rs`, `crates/tinio-api/Cargo.toml` (deps `hyper` — its default features carry `client` + `http1`, which is what `hyper::client::conn::http1::handshake` needs — `hyper-util` (only `rt::TokioIo`, which is unconditional), and `http-body-util` for `BodyExt::collect`; **no `client-legacy`**: this client dials the three local forms itself and never builds a connector pool, and `hyper-util`'s `client` features stay off)

**Interfaces:**
- Consumes: `State` (Task 1), `Router` (Task 3) — but only over the wire.
- Produces: `tinio_api::client::{RawResponse, get_local, get_http, probe, status, stop, Report}` with `Report { running: bool, state: Option<State>, status: Option<StatusBody> }`; `pub async fn status(state_dir: &Path) -> Result<Report, Error>`; `pub async fn stop(state_dir: &Path) -> Result<(), Error>`; `pub async fn probe(state: &State) -> bool`. `RawResponse`, `get_local` and `get_http` are Task 4's three `pub(crate)` helpers **promoted to `pub`** — one definition each, moved in place, not a second copy beside the old one.

Two boundaries this task owns. **The client takes one state dir**, passed in: resolving the candidates (the read-write one and the relocated read-only one, `TINIO_STATE_DIR` included) is the CLI's job — it resolves them, calls the client once per candidate, and reports the one that answered; the client never searches. And the client **never deletes anything**: `state` and the socket belong to the plane (`Plane::publish`/`Plane::shutdown`, Task 5), so `stop` is a request over the wire and the files are the plane's to remove. The rule's reason is a race — a client cannot know whether the server behind a file is still live, so it must never unlink one — and the one exception is not this client at all: the CLI's `doctor --fix` (plan C) reclaims a stale socket through `tinio_api::bind::reclaim_stale` and a stale `state` through `tinio_api::State::remove`, only after a probe has proved no server answers for that root.

Task 4's `mod client;` line becomes `pub mod client;`, and its three helpers are promoted in place: `RawResponse` and its two fields, `get_local` and `get_http` all go `pub(crate)` → `pub`, with the doc comments the workspace requires of public items added as they move. Everything else in this task is new here.

**Execution record (2026-09-20):** steps 1–5 ran and the review passed. Two rulings: a `200` whose body is not the contract's JSON means `running: false` rather than an error (the block below and the prose disagree; the prose, `contracts/cli.md`'s error model and the "probe fails ⇒ stopped" rule decide it), and `probe` stays body-blind — measured, it answers `true` for that same garbled server, so it and `status` differ by design and both docs say so. The block below also needs four things the crate's own gates force: `Arc` imported in the test, `rx` mutable, unix-only imports gated, and the fully-qualified paths de-qualified.

- [x] **Step 1: Write the failing tests**

Append to `crates/tinio-api/src/client.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;
    use tokio::sync::watch;

    use super::*;
    use crate::{bind::bind_unix, router::{Auth, Context}, state::State, transport::serve_local};

    #[cfg(unix)]
    #[tokio::test]
    async fn status_reports_a_running_server_and_stop_ends_it() {
        let dir = TempDir::new().expect("temp dir");
        // The derived default (the state dir doubles as the root) — no fixed `tinio.sock`.
        let path = crate::state::socket_path(dir.path(), dir.path()).expect("socket path");
        let channel = bind_unix(&path).expect("bind");
        let state = State::new(
            path.to_string_lossy().into_owned(),
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("state");
        state.write(dir.path()).expect("write");

        let (tx, rx) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx));
        // `Plane::publish` is what sets this in a real run (Task 5, the readiness point); the
        // client only reads it back through `/status`.
        shared.port.set(9000).expect("the port is published once");
        // The same channel twice, as the plane wires it: the sender is the router's (`/stop`),
        // the clone is what `serve_local` waits on — so the assertion below proves the pair.
        let serving = tokio::spawn(serve_local(channel, shared, rx.clone()));

        let report = status(dir.path()).await.expect("status");
        assert!(report.running);
        assert_eq!(report.status.expect("body").port, Some(9000));

        stop(dir.path()).await.expect("stop");
        rx.changed().await.expect("the channel flips");
        serving.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_stopped_server_is_reported_rather_than_failing() {
        let dir = TempDir::new().expect("temp dir");
        let report = status(dir.path()).await.expect("status");
        assert!(!report.running, "no state file means nothing is running here");
        assert!(stop(dir.path()).await.is_err(), "stop with nothing to stop is an error");
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-api client::`
Expected: FAIL — unresolved module.

- [x] **Step 3: Write the implementation**

Extend the file Task 4 created. The module doc and the three helpers are already there and keep their single definition — `RawResponse` and its two fields, `get_local` and `get_http` go `pub(crate)` → `pub` and gain the doc comments the workspace requires of public items — and `Report`, `Control`, `control`, `probe`, `status`, `stop`, `request` and `exchange` are added below them.

```rust
//! The `status`/`stop` client (`management-api.md:63`): read `state`, speak HTTP to the
//! channel it names, and answer in terms the CLI can print.
//!
//! One request path serves all three local forms: dial the stream (TCP, unix socket or
//! named pipe), then run hyper's HTTP/1 client connection over it. Only the dial differs,
//! which is why there is one `exchange` and three two-line dialers rather than three clients.
//!
//! The channel is read from `state.control_name`, never derived: the plane decided the
//! socket's path (or the pipe's name), resolved it to an absolute one before the bind, and
//! this side only dials what it recorded — which is what makes the client survive a daemon
//! that `chdir("/")`s after its startup. That also
//! means one state dir per call — a caller with several candidates (read-write and read-only)
//! calls these functions once per candidate and picks the one that answered; nothing here
//! searches, and nothing here deletes: `state` and the socket belong to the plane's
//! `shutdown`. A control name too long for `sun_path` cannot be dialed either, and the remedy
//! is the one the bind names (`[api.unix] path` may point at a short directory).

use std::{net::SocketAddr, path::{Path, PathBuf}};

use http_body_util::BodyExt;
use hyper::Request;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{Error, router::StatusBody, state::State};

/// A response as the client hands it over: status line plus body text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawResponse {
    /// The HTTP status code.
    pub status: u16,
    /// The body, decoded as UTF-8 (lossily: a body we did not write is not ours to reject).
    pub body: String,
}

/// What `status` reports: the state file, plus the server's own answer when it replied.
#[derive(Debug, Clone)]
pub struct Report {
    /// Whether a server answered the probe.
    pub running: bool,
    /// The state file, when there was one.
    pub state: Option<State>,
    /// The `/status` body, when the probe succeeded.
    pub status: Option<StatusBody>,
}

/// The channel a `state` file names.
///
/// The classification is by shape, and it is sound because the producer normalises: a Windows
/// pipe name is always the full `\\.\pipe\…` form, because `bind_pipe` (Task 2) prefixes a bare
/// `[api.pipe] path` before it creates anything and records what it created. A bare name would
/// otherwise be dialled as a unix path and fail with "a unix control channel on this platform"
/// — the reason the normalisation is the bind's job rather than this reader's.
#[derive(Debug, Clone)]
enum Control {
    Unix(PathBuf),
    Pipe(String),
    Tcp(SocketAddr),
}

fn control(state: &State) -> Result<Control, Error> {
    let name = state.control_name.as_str();
    if name.starts_with(r"\\.\pipe\") {
        return Ok(Control::Pipe(name.to_string()));
    }
    if let Ok(addr) = name.parse::<SocketAddr>() {
        return Ok(Control::Tcp(addr));
    }
    if name.is_empty() {
        return Err(Error::Transport("the state file names no control channel".to_string()));
    }
    Ok(Control::Unix(PathBuf::from(name)))
}

/// Whether a server answers behind `state`'s control channel.
pub async fn probe(state: &State) -> bool {
    request(state, "/status", "GET").await.is_ok()
}

/// Reads `state` and probes it: a missing file is "stopped", a failed probe is "stopped",
/// and only an unreadable file is an error (`contracts/cli.md:58`).
pub async fn status(state_dir: &Path) -> Result<Report, Error> {
    let Some(state) = State::read(state_dir)? else {
        return Ok(Report { running: false, state: None, status: None });
    };
    match request(&state, "/status", "GET").await {
        Ok(raw) if raw.status == 200 => {
            let status: StatusBody = serde_json::from_str(&raw.body).map_err(|err| {
                Error::Internal(format!("/status answered with something else: {err}"))
            })?;
            Ok(Report { running: true, state: Some(state), status: Some(status) })
        }
        _ => Ok(Report { running: false, state: Some(state), status: None }),
    }
}

/// Sends `POST /stop` and reports whether the server accepted it. The caller polls for the
/// exit; this function answers only what the control channel said (`contracts/cli.md:68`).
pub async fn stop(state_dir: &Path) -> Result<(), Error> {
    let Some(state) = State::read(state_dir)? else {
        return Err(Error::Transport("no running server for this root".to_string()));
    };
    let raw = request(&state, "/stop", "POST").await?;
    if raw.status == 202 {
        Ok(())
    } else {
        Err(Error::Transport(format!("/stop answered {}", raw.status)))
    }
}

/// One request over an explicit unix socket path — the seam the transport tests use.
pub async fn get_local(path: &Path, target: &str, token: Option<&str>) -> Result<RawResponse, Error> {
    #[cfg(unix)]
    {
        let stream = tokio::net::UnixStream::connect(path).await.map_err(Error::Io)?;
        exchange(TokioIo::new(stream), target, "GET", token).await
    }
    #[cfg(not(unix))]
    {
        let _ = (path, target, token);
        Err(Error::Transport("a unix socket is not a Windows transport".to_string()))
    }
}

/// One request over an explicit TCP address — the seam the transport tests use.
pub async fn get_http(addr: SocketAddr, target: &str, token: Option<&str>) -> Result<RawResponse, Error> {
    let stream = tokio::net::TcpStream::connect(addr).await.map_err(Error::Io)?;
    exchange(TokioIo::new(stream), target, "GET", token).await
}

async fn request(state: &State, target: &str, method: &str) -> Result<RawResponse, Error> {
    match control(state)? {
        Control::Tcp(addr) => {
            let stream = tokio::net::TcpStream::connect(addr).await.map_err(Error::Io)?;
            exchange(TokioIo::new(stream), target, method, Some(&state.token)).await
        }
        Control::Unix(path) => {
            #[cfg(unix)]
            {
                let stream = tokio::net::UnixStream::connect(&path).await.map_err(Error::Io)?;
                exchange(TokioIo::new(stream), target, method, Some(&state.token)).await
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(Error::Transport("a unix control channel on this platform".to_string()))
            }
        }
        Control::Pipe(name) => {
            #[cfg(windows)]
            {
                let client = tokio::net::windows::named_pipe::ClientOptions::new()
                    .open(&name)
                    .map_err(Error::Io)?;
                exchange(TokioIo::new(client), target, method, Some(&state.token)).await
            }
            #[cfg(not(windows))]
            {
                let _ = name;
                Err(Error::Transport("a named-pipe control channel on this platform".to_string()))
            }
        }
    }
}

async fn exchange<I>(
    io: TokioIo<I>,
    target: &str,
    method: &str,
    token: Option<&str>,
) -> Result<RawResponse, Error>
where
    I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|err| Error::Transport(format!("handshake: {err}")))?;
    tokio::spawn(async move {
        // The connection future ends when either side is done; a failure here is the
        // caller's response error, reported there rather than twice.
        let _ = connection.await;
    });

    let mut builder = Request::builder().method(method).uri(target);
    if let Some(token) = token {
        builder = builder.header("x-tinio-token", token);
    }
    let request = builder
        .body(String::new())
        .map_err(|err| Error::Transport(format!("request: {err}")))?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|err| Error::Transport(format!("{target}: {err}")))?;
    let status = response.status().as_u16();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|err| Error::Transport(format!("{target}: {err}")))?
        .to_bytes();
    Ok(RawResponse {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}
```

Two notes the implementer must not have to rediscover: `/metrics` is unauthenticated on the **local** channel and on a **loopback-bound** TCP transport (the contract's bind-address rule, decision 8 — `get_local`'s and `get_http`'s `token: None` is how Task 5's loopback leg exercises it), and **`probe` answers `false` rather than failing when nothing is listening** — a connection error is a normal answer for "is a server there?", not an error — while `status` maps both a missing file and a failed probe to `running: false` and keeps the state file it did read. The one case that is an `Err` is a state file that exists and cannot be read (`State::read`'s `Error::State`), which is what `contracts/cli.md:58` puts on the error path; a probe that reaches a server which then answers something that is not the contract's body is also `false` rather than an error, because `status` reports what answered, and a garbled answer is not a running server by the contract's definition.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-api client::`
Expected: PASS — **unix-only, and say so when you report it**: both tests are `#[cfg(unix)]`, so on a Windows host this filter matches zero tests and cargo exits `0` without running anything. The Windows legs — the pipe client and the DACL — are Task 10's, and they are Windows-gated there.

- [x] **Step 5: Lint and checkpoint**

Run: `cargo clippy -p tinio-api --all-targets -- -D warnings`
Expected: clean.
Report the files pending commit.

---

### Task 10: T056's matrix, the feature legs, and the bookkeeping

**Files:**
- Create: `crates/tinio-api/tests/management.rs`
- Modify: `crates/tinio-api/Cargo.toml` (dev-dep `assert_cmd`?), `crates/tinio-api/src/dacl.rs` (the pipe DACL read-back), `crates/tinio/Cargo.toml` (the facade's feature pass-through rows and its `default` list — and nothing else in that file, see Step 6), `docs/tests.md`, `specs/001-s3-local-server/tasks.md`

**Interfaces:**
- Consumes: every task above.
- Produces: nothing the library uses — this is the phase's acceptance.

**Execution record (2026-09-20/21):** steps 1–7 ran and the review passed, after two fix rounds (a hand-written `READ_CONTROL` whose justification was false — now imported from `Win32::Storage::FileSystem` — and a follow-up-row pointer that named the wrong ID). Notes: the red state is the pre-creation `no test target named 'management'` (the first test then passed on the spot, because modules 1–9 were already green, and the report says so rather than inventing a second red); the two `cargo hack --each-feature` rows are red for a **pre-existing** reason — `tinio-server`'s `otel` feature does not compile, which no file this plan touched is reachable from — so the facade matrix is green with `--skip otel`; and the pipe accept loop's successor-instance window (a dial in the gap before the successor exists meets `ERROR_PIPE_BUSY`, which makes `tinio status` report a running server as stopped) is recorded in `docs/tests.md` with its consequence and handed to a follow-up row, **T147**, rather than redesigned here.

- [x] **Step 1: Write the first failing test**

Create `crates/tinio-api/tests/management.rs` with **one** test: the CLI's own sequence over the local channel — `Plane::bind(options)` → `publish(port)` → `serve(stop)` → Task 9's `stop(state_dir)` → `shutdown()` — asserting `/status` answers `200` with the token and that `shutdown` leaves neither the socket file nor `state` behind. It is the first bullet of Step 3's list, written alone because the global constraint holds here too: this task's deliverable is the suite, so its red state has to be established before the suite exists rather than assumed after it passes.

- [x] **Step 2: Run it to verify it fails**

Run: `cargo test -p tinio-api --test management`
Expected: FAIL — `error: no test target named \`management\`` while the file holds no test (and, for the record, the same command with the whole suite is the phase's acceptance, Step 4).

- [x] **Step 3: Write the rest of the integration suite**

`crates/tinio-api/tests/management.rs` covers T056's list, one test per bullet, and it is the **only consumer of the plane in phase A**: nothing outside a test starts one until plan B wires the CLI, so this suite is T056's acceptance rather than a nice-to-have beside a running binary (and it is T075's phase-A verification too, since Task 7 retired the cucumber scrape). Drive the plane the way the CLI will — `Plane::bind(options)` → `publish(port)` → `serve(stop)` → `shutdown()` — rather than by calling the transports directly.

The bullets: `/status` with and without the token; `/stop`'s `202` and the bounded drain, asserting the shutdown channel flips and that **`Plane::shutdown` removes the socket file and `state`** — one story, the CLI's own path (Task 9's `stop` is what triggers it, and the test drives that client), with `Drop` left as the backstop it is documented to be; `/metrics` open on the local channel **and on a loopback bind**, `401` on a non-loopback bind, and `404` with no gauges attached; `/openapi.json` under the feature; the Windows pipe's first-instance refusal, the pipe's DACL and the state file's DACL (**Windows-gated**, the last two asserted through Task 1's `dacl::acl_is_current_user_only` and its pipe twin `dacl::pipe_acl_is_current_user_only` — the read-backs exist so this leg can check the results instead of trusting them, and the pipe's is the one that proves the channel `Auth::Local` opens `/metrics` and `/openapi.json` on is as private as the unix socket); the unix stale-socket probe-then-reclaim and the state file's mode 0600 (unix-gated). The gauges' half of the matrix uses **one local `MetricsSource` double** (Task 7's Step 1 shape): two counting no-ops, because nothing a real adapter computes crosses the boundary — the counts go straight into `tinio-server`'s three statics, which only the CLI (plan B's Task 3) can name, and the scan cannot run here either: `Storage` is dyn-incompatible, so there is no `dyn Storage` to hand the plane, and `tinio-api` takes no `tinio-mem` dependency for one. **The reader is asserted here too**, with that double: `Gauges::spawn_reader` on a short ttl advances `counts` with no scrape at all, and a burst of `/metrics` requests leaves it unmoved — the integration form of Task 7's Step 1 regression.

`dacl::pipe_acl_is_current_user_only(name: &str) -> Result<bool, Error>` is this task's addition to Task 1's module (the site is already open, and this is its third use): it opens the pipe for `READ_CONTROL`, reads the DACL back with `GetSecurityInfo(handle, SE_KERNEL_OBJECT, DACL_SECURITY_INFORMATION, …)` and walks it exactly as `acl_is_current_user_only` walks a file's — same trustee rule, same `Ok(false)` for an empty DACL, same `Error::State` mapping, same `LocalFree` on every path. It is public for the same reason the file twin is: this leg is an integration test and could not reach a `pub(crate)` helper.

- [x] **Step 4: Run the suite**

Run: `cargo test -p tinio-api --features openapi,tls` and `cargo test -p tinio-api`
Expected: PASS in both. The suite is the acceptance for this phase: with no runtime caller yet, a T056 bullet that only the CLI could exercise is out of scope rather than skipped, and every bullet above runs here.

- [x] **Step 5: Run the feature matrix and the workspace gate**

Run: `cargo hack check -p tinio-api --each-feature --no-dev-deps` and the `docs/tests.md` Standard acceptance block in full.
Expected: PASS. Report each command's output.

- [x] **Step 6: Bookkeeping**

- `docs/tests.md`: add the management-plane legs under a `## Management plane (tinio-api)` heading in the existing compressed style.
- `specs/001-s3-local-server/tasks.md`: tick **T056** and **T075**. The tick comments carry what each tick rests on: T056's acceptance is Step 3's suite (there is no runtime caller in this phase), and T075's phase-A verification is that same suite, because Task 7 retired the data-plane cucumber scrape and the control-channel e2e form returns with plan B's harness migration — a bare tick would read as if the e2e leg still covered it. The read-only follow-up T143 keeps its `tinio-api` half open until plan B wires the API's relocation (which this plan makes structural by taking the state dir as a parameter — note that in the same comment).
- `crates/tinio/Cargo.toml` (the facade): add `openapi` and `tls` to `tinio-api`'s feature pass-through (`openapi = ["tinio-api/openapi"]`, `tls = ["tinio-api/tls"]`, beside the existing `api = ["dep:tinio-api", …]` line) and to the facade's `default` list (design decision 17). **This plan owns those rows and the `default` list only**: plan B owns the same crate's `[dev-dependencies]` and the `sandbox` row, and neither plan may edit the other's lines — the ownership check below is what keeps that true.
- Run the ownership check of `docs/superpowers/README.md` against this plan and its two siblings before declaring the phase complete: name the single owner of `Plane`/`Options` (this plan, Task 5 — the same owner as `state` and the socket), of the state file's schema (`state.rs`), of the transport resolution (`transport::select`), of `/metrics`' gauge source (the injected `MetricsSource`; the family list itself stays `tinio-server`'s registry), **of the storage scan's cadence (`Gauges`'s reader task — one task, one scan at a time, and no request ever scans; the old phrasing "the TTL-gated scan" is corrected here as well as in the design)** and of the shutdown channel (the CLI creates it, the plane sends on it, both serves read it), and search the other two plans for a second statement of each.

- [x] **Step 7: Final checkpoint**

Report every file pending commit, and the full acceptance output. Do not run any git write.

## Post-plan review fixes (2026-09-21)

These seven landed after the plan's ten tasks closed, out of the whole-branch review's fix wave and the `/simplify` pass, except the last, which came from the operator's own review of `bind.rs` and is marked as such; where any task block above differs from what the code does, the code is the authority.

- **`client::dialable`** — a recorded control name whose IP is unspecified is dialled on loopback, because a wildcard bind's `0.0.0.0:port` is not a connect address (Windows refuses it with `WSAEADDRNOTAVAIL`, measured on this branch's host) and `status` maps every dial failure to `running: false`, so without the substitution a wildcard-bound server would report "stopped" deterministically; the contract carries the sentence (`management-api.md`'s wildcard-bind paragraph of the transport section) and the wildcard leg in `crates/tinio-api/tests/management.rs` pins it.
- **`/metrics`' empty-gauge answer** became `{"error": "metrics are not attached"}` rather than a plain-text body, so the absence keeps the crate's one error shape; the `404` itself is unchanged and is still what the contract's endpoint table says the absence is served as.
- **`get_local`/`get_http`/`exchange` take a `Call` value** (`target`, `method`, `token`) instead of three positional arguments, so `client::request`'s arms delegate to them instead of re-dialing — one carry type through all four, no duplicated argument lists, and no call site that can transpose the target and the method.
- **`Transport::Local`'s stored `identity` and `auth` fields are gone**: `Transport::identity()` asks the channel and `Transport::auth()` derives from `Auth::for_bind`, so a transport cannot carry a rule that disagrees with the address it bound.
- **`Context`'s `gauges` is private**, attached at construction (`Context::with_gauges`) instead of assigned afterwards — the field is readable only through the accessor the plane and the tests share.
- **`State` has a hand-written `Debug` that redacts the token** — the struct rides inside the CLI's `Report`, so a future `?report` in a log line cannot print the bearer token; every other field renders as the derive would.
- **The `Channel` enum is gone: each platform has its own `Channel` struct, selected by `#[cfg]`** — and this one came from the operator's review of `bind.rs` rather than from this branch's own reviews, which is why it is recorded here beside them. The enum's two variants were `#[cfg]`-selected and therefore mutually exclusive by construction, so every `match` on a channel had exactly one reachable arm per build and the `#[non_exhaustive]` attribute existed only to explain that; there is now a `#[cfg(unix)]` struct holding `listener`/`path` and a `#[cfg(windows)]` one holding `name`/`handle`, exactly one of which exists in any build. The accessors are unchanged — `identity()` and `socket_path()` are inherent on whichever struct the build has, unix answering the bound path and `Some(path)` and Windows the pipe name and `None` — the fields are crate-visible rather than public (`serve_local` destructures the channel it was handed, and nothing outside the crate reaches them), `bind_unix`/`bind_pipe` construct the struct where they built a variant before, and Task 2's Interfaces line above carries the same amendment.
