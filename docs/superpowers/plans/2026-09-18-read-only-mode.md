# Read-Only Mode (FR-023) + Shared S3 Operation Table — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve a storage root read-only — every mutating S3 operation answers `AccessDenied`, the root is never written — with all tool state relocated under `~/.tinio/roots/<hash>/`; and along the way collapse the hand-maintained S3 operation inventory into one machine-checked table that generates the metrics, capability and read-only decorators.

**Architecture:** One `macro_rules!` table in `backend/ops.rs` lists every operation tinio implements, grouped `read`/`write`, with optional `#[gated(...)]` and `#[on_ok(...)]` clauses. `#[gated]` carries the cargo feature alongside the capability field (`#[gated(copy_object: "copy")]`), so the compile-time strip is derived from it and the two can no longer disagree. Three decorators over the `S3` trait are generated from it and chained `MetricS3 → S3ReadonlyBackend → CapabilityS3 → S3Backend`. Read-only mode adds an `AccessMode` threaded through a new `PlaneConfig`, a state-dir resolver in `tinio-config`, and a `CleanupOptions.access` flag that keeps `tinio-fs`'s root-writing startup repair stages away from the root.

**Tech Stack:** Rust 2024, s3s 0.16 (`#[async_trait]` `S3` trait), `macro_rules!` (no proc-macro or `paste` dependency — `tinio-server` has none), `sha2` + `hex` + `dirs` in `tinio-config`.

**Spec:** `docs/superpowers/specs/2026-09-18-read-only-mode-design.md` — the authoritative design; read it alongside this plan. Task ordering follows its decisions 8 and 9.

## Global Constraints

- **Perform no git write operations whatsoever** (CLAUDE.md overrides the skill's commit step): every task closes with "checkpoint: report the list of files pending commit", and **does not run** `git add/commit/push`.
- Code, comments, tests, commit messages and documents are always in English.
- **Documents: no hard line wraps** — every paragraph and list item is one unbroken line (docs/style.md).
- Existing tests are **not edited** except where a task says so explicitly. A red existing test is an implementation bug, not grounds for changing the test.
- New tests follow docs/tests.md: async tests use `#[tokio::test]` / `async fn`, **never** `Runtime::block_on` or a hand-rolled `rt(...)`. Sync tests use `#[test]`.
- `unsafe_code = "forbid"` is per-crate (`docs/cargo.md:10`). Under edition 2024 `std::env::set_var` is `unsafe`, so **no test may mutate the process environment**.
- Style (docs/style.md): compressed prose, imports of 3+ segments de-qualified, per-crate `Error`, `SmartDefault` for defaults, `parse-display`/`derive_more` for derives.
- `clippy::absolute_paths` is a warn-level lint in `tinio-server` (`[lints.clippy] absolute_paths = "warn"`); the gate runs with `-D warnings`.
- The full local gate (docs/tests.md "Standard acceptance") is the pre-push bar; per-task steps run the subset named in that task.
- **Execution**: subagent-driven — one fresh subagent per task, and the task is reviewed before the next one starts. Once a task's steps pass and its review is accepted, tick that task's step checkboxes (`- [x]`) in this file; a task still under review stays unchecked.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `crates/tinio-core/src/mode.rs` (new) | `AccessMode` — the write policy of a served plane | 1 |
| `crates/tinio-core/src/lib.rs` | `pub mod mode;` + re-export | 1 |
| `crates/tinio-core/src/cleanup.rs` | `CleanupOptions.access` | 1 |
| `Cargo.toml` (workspace) | `dirs` entry | 2 |
| `crates/tinio-config/Cargo.toml` | `dirs`, `sha2`, `hex` | 2 |
| `crates/tinio-config/src/state.rs` (new) | `home_state_dir`, `default_home_state_dir`, the `TINIO_STATE_DIR` override, the symlinked-parent refusal | 2 |
| `crates/tinio-config/src/error.rs` | `Error::Root` + `error::root` | 2 |
| `crates/tinio-config/src/sources.rs` | `TINIO_READ_ONLY` overlay | 3 |
| `crates/tinio-server/src/backend/ops.rs` (new) | the operation table + the cross-check test | 4 |
| `crates/tinio-server/src/backend/mod.rs` | `mod ops;` | 4 |
| `crates/tinio-server/src/metrics.rs` | `metric_s3_impl!` replaces 36 hand-written methods; `wire_name!`; the real-status label | 5 |
| `crates/tinio-server/src/capability.rs` (new) | `CapabilityS3` — the FR-021 gates | 6 |
| 6 handler modules | the 26 `require_cap`/`require_multipart` call sites removed | 6 |
| `crates/tinio-server/src/backend/testutil.rs` | `setup_gated_with_caps` — the eight off-mode tests drive the decorator | 6 |
| `crates/tinio-server/src/readonly.rs` (new) | `S3ReadonlyBackend` — FR-023 write refusal | 7 |
| `crates/tinio-server/src/data.rs` | `PlaneConfig` + the decorator chain | 8 |
| `crates/tinio-server/src/lib.rs` | module declarations + re-exports | 6, 7, 8 |
| `crates/tinio-fs/src/cleanup.rs` | `access` field + root-writing stages skipped | 9 |
| `crates/tinio-fs/src/backend/mod.rs` | `FsOptions.access` + the construction guard | 9 |
| `crates/tinio-fs/src/scanner.rs` | `ScannerOptions.access` + tombstone purge skipped | 9 |
| `crates/tinio-fs/src/testing.rs` | `fs_options()` gains the new field | 9 |
| `crates/tinio-server/examples/serve.rs` | `--read-only`, env, state dir (`TINIO_STATE_DIR` included), cleanup, plane, the chown-probe skip | 10 |
| `crates/tinio-server/tests/read_only.rs` (new) | T058 end-to-end | 11 |
| `crates/tinio-e2e/tests/steps/common.rs` | `Server::read_only` | 12 |
| `crates/tinio-e2e/tests/features/read_only.feature` (new) | the scenarios | 12 |
| `crates/tinio-e2e/tests/traceability.rs`, `README.md` | FR-023 leaves the allow-list; the tag table | 12 |
| `specs/001-s3-local-server/*`, `CONTEXT.md` | bookkeeping | 13 |

---

### Task 1: `AccessMode` and `CleanupOptions.access`

**Files:**
- Create: `crates/tinio-core/src/mode.rs`
- Modify: `crates/tinio-core/src/lib.rs` (module list + `pub use` block)
- Modify: `crates/tinio-core/src/cleanup.rs:145-150` (`CleanupOptions`)

**Interfaces:**
- Consumes: nothing.
- Produces: `tinio_core::mode::AccessMode` (also re-exported at the crate root as `tinio_core::AccessMode`), with `Default` = `ReadWrite`, `From<bool>`, and `AccessMode::is_read_only(self) -> bool`; `CleanupOptions { dry_run: bool, access: AccessMode }`.

- [ ] **Step 1: Write the failing test**

Create `crates/tinio-core/src/mode.rs` with only the tests module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_read_write() {
        assert_eq!(AccessMode::default(), AccessMode::ReadWrite);
        assert!(!AccessMode::default().is_read_only());
    }

    #[test]
    fn bool_maps_to_mode() {
        assert_eq!(AccessMode::from(true), AccessMode::ReadOnly);
        assert_eq!(AccessMode::from(false), AccessMode::ReadWrite);
        assert!(AccessMode::from(true).is_read_only());
    }
}
```

Add `pub mod mode;` to `crates/tinio-core/src/lib.rs` **in alphabetical position** — the list is `acl, bucket, checksum, cleanup, cors, etag, multipart, object, percent, pipeline, storage`, and `mode` sorts between `etag` and `multipart`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-core mode::`
Expected: FAIL — `cannot find type AccessMode in this scope` / unresolved import.

- [ ] **Step 3: Write minimal implementation**

Prepend to `crates/tinio-core/src/mode.rs`:

```rust
//! The write policy of a served plane (FR-023).

/// The write policy of a served plane: [`ReadWrite`](Self::ReadWrite) serves
/// every operation, [`ReadOnly`](Self::ReadOnly) refuses the mutating ones
/// with `AccessDenied`.
///
/// # Examples
///
/// ```rust
/// use tinio_core::mode::AccessMode;
///
/// assert!(!AccessMode::default().is_read_only());
/// assert!(AccessMode::from(true).is_read_only());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AccessMode {
    /// Every operation is served.
    #[default]
    ReadWrite,
    /// Mutating operations answer `AccessDenied` (FR-023).
    ReadOnly,
}

impl AccessMode {
    /// Whether this mode refuses mutating operations.
    pub fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

impl From<bool> for AccessMode {
    fn from(read_only: bool) -> Self {
        if read_only { Self::ReadOnly } else { Self::ReadWrite }
    }
}
```

Add `mode::AccessMode` to the `pub use self::{…}` block in `crates/tinio-core/src/lib.rs` (alongside `bucket::Bucket`, `etag::ETag`) — rustfmt will place it.

- [ ] **Step 4: Add `access` to `CleanupOptions`**

In `crates/tinio-core/src/cleanup.rs`, extend the struct (keep the existing `dry_run` doc comment; extend the struct doc to mention the policy):

```rust
/// Construction options of a [`Cleanup`] implementation: whether the
/// pipeline runs in dry-run mode (report only, never touch anything), and
/// the plane's write policy — read-only mode forbids the repair stages that
/// write the storage root (FR-023).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupOptions {
    /// Report exactly what a real run would change without modifying
    /// anything.
    pub dry_run: bool,
    /// The served plane's write policy.
    pub access: AccessMode,
}
```

Add `use crate::mode::AccessMode;` to that file's imports.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p tinio-core`
Expected: PASS — including the crate's existing `CleanupOptions` doctest (it uses `..CleanupOptions::default()`, so the new field is absorbed).

- [ ] **Step 6: Checkpoint**

Report the files pending commit. Do not run git commands.

---

### Task 2: `tinio-config` state-directory resolution

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/tinio-config/Cargo.toml`
- Modify: `crates/tinio-config/src/error.rs`
- Create: `crates/tinio-config/src/state.rs`
- Modify: `crates/tinio-config/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `tinio_config::state::home_state_dir(root: &Path, home: &Path) -> Result<PathBuf, Error>`, `default_home_state_dir(root: &Path) -> Result<PathBuf, Error>`, the `STATE_DIR_VAR` constant with `env_state_dir() -> Result<Option<PathBuf>, Error>` / `parse_state_dir(&str) -> Result<PathBuf, Error>`; `Error::Root { path: PathBuf, source: io::Error }`.

- [ ] **Step 1: Add the dependencies**

In the root `Cargo.toml` `[workspace.dependencies]`, insert between `derive_more = "2"` and `dotenvy = "0.15"`:

```toml
dirs = "6"
```

In `crates/tinio-config/Cargo.toml` `[dependencies]`, insert `dirs.workspace = true` between `derive_more` and `dotenvy`, and `hex.workspace = true` + `sha2.workspace = true` in their alphabetical positions (after `garde`, before `humantime-serde` for both — `garde < hex < humantime-serde < parse-display` and `garde < humantime-serde < parse-display < secrecy < serde < serde_ignored < sha2 < smart-default`, so `hex` goes after `garde`, `sha2` after `serde_ignored`).

- [ ] **Step 2: Write the failing test**

Create `crates/tinio-config/src/state.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn layout_is_home_tinio_roots_hash() {
        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let state = home_state_dir(root.path(), home.path()).unwrap();
        assert_eq!(
            state.parent().unwrap(),
            home.path().join(".tinio").join("roots")
        );
        let leaf = state.file_name().unwrap().to_str().unwrap();
        assert_eq!(leaf.len(), 16, "16 hex characters: {leaf}");
        assert!(leaf.chars().all(|c| c.is_ascii_hexdigit()), "{leaf}");
        assert!(state.parent().unwrap().is_dir(), "parents are created");
    }

    #[test]
    fn hash_is_stable_and_root_specific() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        assert_eq!(
            home_state_dir(a.path(), home.path()).unwrap(),
            home_state_dir(a.path(), home.path()).unwrap()
        );
        assert_ne!(
            home_state_dir(a.path(), home.path()).unwrap(),
            home_state_dir(b.path(), home.path()).unwrap()
        );
    }

    #[test]
    fn missing_root_is_an_error() {
        let home = tempfile::tempdir().unwrap();
        let missing = home.path().join("nope");
        let err = home_state_dir(&missing, home.path()).unwrap_err();
        assert!(matches!(err, Error::Root { .. }), "{err:?}");
    }

    #[cfg(unix)]
    #[test]
    fn parents_are_mode_0700() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let state = home_state_dir(root.path(), home.path()).unwrap();
        for dir in [home.path().join(".tinio"), state.parent().unwrap().to_path_buf()] {
            let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", dir.display());
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_pre_existing_loose_parent_is_tightened() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let base = home.path().join(".tinio");
        fs::create_dir(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
        home_state_dir(root.path(), home.path()).unwrap();
        let mode = fs::metadata(&base).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "an inherited 0755 must be tightened");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_parent_is_refused() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        symlink(elsewhere.path(), home.path().join(".tinio")).unwrap();
        let err = home_state_dir(root.path(), home.path()).unwrap_err();
        assert!(err.to_string().contains("symlink"), "{err}");
    }

    #[test]
    fn state_dir_override_parses_strictly() {
        assert_eq!(
            parse_state_dir(" /srv/tinio-state ").unwrap(),
            PathBuf::from("/srv/tinio-state")
        );
        let err = parse_state_dir("   ").unwrap_err();
        assert!(err.to_string().contains(STATE_DIR_VAR), "{err}");
    }
}
```

`tempfile` is already a `[dev-dependencies]` entry of `tinio-config`.

Add `pub mod state;` to `crates/tinio-config/src/lib.rs` — alphabetically after `pub mod sources;`.

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p tinio-config state::`
Expected: FAIL — `cannot find function home_state_dir`.

- [ ] **Step 4: Write the implementation**

Prepend to `crates/tinio-config/src/state.rs`:

```rust
//! State-directory resolution (FR-023).
//!
//! Normal mode keeps the tool's private state inside the served root
//! (`<root>/.tinio/`, resolved by the backend). Read-only mode must not
//! write the root at all — it may be a genuinely read-only filesystem — so
//! every piece of tool state moves under the user's home:
//! `<home>/.tinio/roots/<hash>/` (contracts/config.md; data-model.md
//! "Reserved Directory").

use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{Error, error};

/// Length of the derived directory name, in hexadecimal characters.
const HASH_LEN: usize = 16;
/// The tool's home directory name.
const BASE_DIR_NAME: &str = ".tinio";
/// The per-root state directory parent.
const ROOTS_DIR_NAME: &str = "roots";

/// `<home>/.tinio/roots/<sha256(canonical root)[..16]>/` (FR-023).
///
/// `home` is an explicit parameter, never `dirs::home_dir()` inline: tests
/// must resolve a scratch home instead of the real one.
///
/// The root is canonicalized first — a root's identity is its canonical
/// path — so a root that does not exist fails here rather than silently
/// minting a new identity. The two parents are created mode 0700 on unix
/// (otherwise they inherit the umask, letting other users enumerate roots);
/// the leaf is left to the backend, which creates it 0700 already
/// (`tinio-fs`'s state-database open).
pub fn home_state_dir(root: &Path, home: &Path) -> Result<PathBuf, Error> {
    let canonical = root.canonicalize().map_err(|e| error::root(root, e))?;
    let digest = Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    let name = &hex::encode(digest)[..HASH_LEN];
    Ok(ensure_roots(home)?.join(name))
}

/// [`home_state_dir`] over the real home directory.
pub fn default_home_state_dir(root: &Path) -> Result<PathBuf, Error> {
    let home = dirs::home_dir().ok_or_else(|| Error::Missing("home directory".into()))?;
    home_state_dir(root, &home)
}

/// The environment variable that overrides the derived state dir (FR-023).
pub const STATE_DIR_VAR: &str = "TINIO_STATE_DIR";

/// The `TINIO_STATE_DIR` override, if set: an explicit state dir used
/// verbatim instead of the `~/.tinio/roots/<hash>/` derivation — the escape
/// hatch for the environments read-only mode is actually for (service
/// accounts, containers, immutable images) where the home may be absent or
/// unwritable, and where `default_home_state_dir` would otherwise fail
/// startup outright.
///
/// Split into a pure parse plus a two-line lookup, like the other env
/// overlay: `std::env::set_var` is `unsafe` under edition 2024 and every
/// crate forbids `unsafe_code`, so a test cannot mutate the process
/// environment.
pub fn env_state_dir() -> Result<Option<PathBuf>, Error> {
    match env::var(STATE_DIR_VAR) {
        Ok(raw) => parse_state_dir(&raw).map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(Error::invalid_value(
            STATE_DIR_VAR,
            "value is not valid Unicode",
        )),
    }
}

/// Parse a `TINIO_STATE_DIR` value.
///
/// An empty value is an error, never "the current directory". The path is
/// taken verbatim: its parents are **not** created mode 0700 and no symlink
/// check applies, because the caller chose the location explicitly — the
/// backend still creates the leaf itself and sets it 0700.
pub fn parse_state_dir(raw: &str) -> Result<PathBuf, Error> {
    let path = raw.trim();
    if path.is_empty() {
        return Err(Error::invalid_value(STATE_DIR_VAR, "must not be empty"));
    }
    Ok(PathBuf::from(path))
}

/// Refuse a symlinked `<home>/.tinio` or `<home>/.tinio/roots`.
///
/// Normal mode already refuses a symlinked `<root>/.tinio` (`prove_contained`),
/// and the same posture applies here: following the link would put the tool's
/// state — and with it the promise that the served root is untouched —
/// wherever the link points. `TINIO_STATE_DIR` is the escape hatch for a
/// deliberate layout.
fn refuse_symlink(dir: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(dir) {
        Ok(meta) if meta.file_type().is_symlink() => Err(Error::invalid_value(
            dir.display().to_string(),
            "must not be a symlink (normal mode refuses a symlinked `<root>/.tinio` \
             for the same reason; set TINIO_STATE_DIR for an explicit location)",
        )),
        _ => Ok(()),
    }
}

/// Create `<home>/.tinio` and `<home>/.tinio/roots` (mode 0700 on unix)
/// and return the latter.
fn ensure_roots(home: &Path) -> Result<PathBuf, Error> {
    let base = home.join(BASE_DIR_NAME);
    let roots = base.join(ROOTS_DIR_NAME);
    for dir in [&base, &roots] {
        refuse_symlink(dir)?;
        // A concurrent instance may create the same directory between the
        // probe and the create; AlreadyExists is the only tolerated error.
        if let Err(e) = create_dir_0700(dir) {
            if e.kind() != io::ErrorKind::AlreadyExists {
                return Err(error::root(dir, e));
            }
        }
        // The mode is enforced unconditionally, not only at creation: a
        // `<home>/.tinio` an earlier version created 0755 would otherwise
        // stay world-enumerable forever, and the whole read-only story
        // rests on other users not seeing this directory. Same shape as
        // `tinio-fs`'s state-db open, which always sets the leaf's mode.
        enforce_0700(dir)?;
    }
    Ok(roots)
}

#[cfg(unix)]
fn enforce_0700(dir: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(|e| error::root(dir, e))
}

#[cfg(not(unix))]
fn enforce_0700(_dir: &Path) -> Result<(), Error> {
    Ok(())
}

#[cfg(unix)]
fn create_dir_0700(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    fs::DirBuilder::new().mode(0o700).create(dir)
}

#[cfg(not(unix))]
fn create_dir_0700(dir: &Path) -> io::Result<()> {
    fs::create_dir(dir)
}
```

Add to `crates/tinio-config/src/error.rs`: a variant and its constructor, matching the file's existing shape (variant after `Env`, constructor with the other `pub(crate) fn`s):

```rust
    /// The storage root could not be resolved (missing, unreadable, or not
    /// a directory) — the state directory cannot be derived from it.
    #[error("failed to resolve the storage root `{path}`: {source}")]
    Root {
        /// The path that could not be resolved.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
```

```rust
/// Storage-root resolution failure.
#[inline]
pub(crate) fn root(path: impl Into<PathBuf>, source: io::Error) -> Error {
    Error::Root {
        path: path.into(),
        source,
    }
}
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p tinio-config`
Expected: PASS (6 new tests; the existing ones untouched).

Run: `cargo doc -p tinio-config --no-deps` with `RUSTDOCFLAGS="-D warnings"`
Expected: no doc warnings (the new module and items are documented).

- [ ] **Step 6: Checkpoint**

Report the files pending commit.

---

### Task 3: The `TINIO_READ_ONLY` overlay

**Files:**
- Modify: `crates/tinio-config/src/sources.rs`

**Interfaces:**
- Consumes: `crate::schema::Config` (field `server: server::Config` with `pub read_only: bool`).
- Produces: `tinio_config::sources::READ_ONLY_VAR: &str`; `tinio_config::sources::parse_read_only(raw: &str) -> Result<bool, Error>`; `tinio_config::sources::apply_read_only_env(config: &mut Config) -> Result<(), Error>`.

- [ ] **Step 1: Write the failing test**

Append to the `#[cfg(test)] mod tests` in `crates/tinio-config/src/sources.rs`:

```rust
    #[test]
    fn read_only_flag_parses_strictly() {
        for raw in ["1", "true"] {
            assert!(parse_read_only(raw).unwrap(), "{raw}");
        }
        for raw in ["0", "false"] {
            assert!(!parse_read_only(raw).unwrap(), "{raw}");
        }
        let err = parse_read_only("yes").unwrap_err();
        assert!(err.to_string().contains(READ_ONLY_VAR), "{err}");
    }

    #[test]
    fn read_only_overlay_sets_the_config_field() {
        let mut config = Config::parse("version = 1").unwrap();
        assert!(!config.server.read_only);
        apply_read_only(&mut config, true);
        assert!(config.server.read_only);
        apply_read_only(&mut config, false);
        assert!(!config.server.read_only);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-config sources::`
Expected: FAIL — `cannot find function parse_read_only`.

- [ ] **Step 3: Write the implementation**

Append to `crates/tinio-config/src/sources.rs` (imports: extend the existing `use std::path::Path;` to also bring in `env`; add `use crate::schema::Config;`):

```rust
/// The environment variable that selects read-only mode (FR-023).
pub const READ_ONLY_VAR: &str = "TINIO_READ_ONLY";

/// Parse a `TINIO_READ_ONLY` value: `1`/`true` and `0`/`false` only, the
/// same closed shape as `TINIO_SCANNER`. Anything else is a startup error —
/// a typo must not silently serve a writable root.
pub fn parse_read_only(raw: &str) -> Result<bool, Error> {
    match raw.trim() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(Error::invalid_value(
            READ_ONLY_VAR,
            format!("expected 0/1/true/false, got `{other}`"),
        )),
    }
}

/// Set the `[server] read_only` field.
///
/// Split out from [`apply_read_only_env`] because the process environment
/// cannot be mutated from a test: `std::env::set_var` is `unsafe` under
/// edition 2024 and every crate here forbids `unsafe_code`.
pub fn apply_read_only(config: &mut Config, read_only: bool) {
    config.server.read_only = read_only;
}

/// Apply the `TINIO_READ_ONLY` overlay (FR-023): an unset variable leaves
/// the config file's value standing; a set one must parse.
pub fn apply_read_only_env(config: &mut Config) -> Result<(), Error> {
    match env::var(READ_ONLY_VAR) {
        Ok(raw) => {
            apply_read_only(config, parse_read_only(&raw)?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(env::VarError::NotUnicode(_)) => Err(Error::invalid_value(
            READ_ONLY_VAR,
            "value is not valid Unicode",
        )),
    }
}
```

Update the module doc's third bullet, which currently says the config file "is overlayed by the CLI": add that `TINIO_READ_ONLY` is applied here in the interim, ahead of the clap `env` attributes the CLI will bring (T066/T067).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-config`
Expected: PASS.

- [ ] **Step 5: Checkpoint**

Report the files pending commit.

---

### Task 4: The shared operation table

**Files:**
- Create: `crates/tinio-server/src/backend/ops.rs`
- Modify: `crates/tinio-server/src/backend/mod.rs` (module list, near `mod s3;`)

**Interfaces:**
- Consumes: nothing.
- Produces: `crate::backend::ops::tinio_s3_ops` — a `macro_rules!` macro taking one identifier (the consumer macro) and invoking it with the table. Also (test-only) `crate::backend::ops::table_ops() -> Vec<&'static str>`.

- [ ] **Step 1: Write the failing test**

Create `crates/tinio-server/src/backend/ops.rs` with the table and the cross-check test. The table's rows are exactly the 36 operations of `backend/s3.rs`, grouped by whether they mutate, with the two optional clauses in the fixed order `#[gated]`, `#[on_ok]`. Every operation appears in **exactly one** group: the read group is all 16 non-mutating operations — `list_parts` and `list_multipart_uploads` included, they only read — and the read-only decorator serves them; the write group is the 20 that mutate. `table_has_no_duplicates` pins that, and a duplicated row would not compile anyway (two `async fn`s of the same name in one generated impl).

```rust
//! The S3 operation inventory.
//!
//! One row per operation tinio implements, grouped by whether it mutates.
//! The rows are facts about the operation — its s3s dto pair, its mutating
//! kind, the cargo feature that strips it, the `[s3]` capability that
//! refuses it, and any post-success side effect — and four consumers read
//! them: `MetricS3` (`metrics.rs`), `S3ReadonlyBackend` (`readonly.rs`),
//! `CapabilityS3` (`capability.rs`), and this module's own `include_str!`
//! cross-check against `backend/s3.rs`.
//!
//! # Row grammar
//!
//! ```text
//! $(#[gated(<field>[: "<feature>"], …)] )?   every listed `Capabilities`
//!     field must be on (the list is a conjunction), and each entry's cargo
//!     feature — when given — strips the row from builds without it.
//! $(#[on_ok(<expr>)]   )?     run after a successful call
//! fn <method> (<InputIdent>) -> <OutputIdent>;
//! ```
//!
//! The compile-time strip and the runtime refusal are one fact at two layers,
//! and the strip is a function of the capability — verified across all 36
//! operations against `backend/s3.rs`'s 19 `#[cfg]` attributes. So the row
//! states the feature once, and the consumers derive the attribute. A
//! capability with no cargo feature (`tagging`, `delete_objects`) is written
//! bare: `#[gated(tagging)]`.
//!
//! **The feature literal cannot be omitted.** `#[cfg]` takes a literal,
//! `macro_rules!` cannot make one from an ident, and the workaround — a
//! lookup macro matching the ident and emitting the attribute — breaks
//! `#[async_trait]` with `E0195`, the same expansion-order wall that rules
//! out the `delegate` crate: such a macro invocation sits *inside* the impl,
//! and `#[async_trait]` passes opaque items through untouched. That is also
//! why the capability field names are not renamed to match the features —
//! with the literal mandatory, `copy_object: "copy"` informs the reader,
//! while `copy: "copy"` would be a tautology.
//!
//! Two more constraints, each verified by compiling the alternative:
//!
//! - **A consumer cannot use `$(#[$m:meta])*`.** `#[gated(...)]` and
//!   `#[on_ok(...)]` are themselves valid `meta` productions, so a catch-all
//!   plus a named clause is a "local ambiguity" the parser refuses.
//! - **The dto pair is captured as `ident`, not `ty`.** A `ty` fragment
//!   cannot be used as a path segment (`dto::$in`), and `stringify!` of a
//!   multi-token path would break the derived wire name.
//!
//! Every consumer must spell out both clauses in order — `macro_rules!` has
//! no partial matching — so adding a clause here edits four matchers.

macro_rules! tinio_s3_ops {
    ($consumer:ident) => {
        $consumer! {
            read {
                fn head_bucket (HeadBucketInput) -> HeadBucketOutput;
                fn list_buckets (ListBucketsInput) -> ListBucketsOutput;
                fn get_bucket_location (GetBucketLocationInput) -> GetBucketLocationOutput;
                #[gated(tagging)]
                fn get_bucket_tagging (GetBucketTaggingInput) -> GetBucketTaggingOutput;
                #[gated(cors: "cors")]
                fn get_bucket_cors (GetBucketCorsInput) -> GetBucketCorsOutput;
                #[gated(acl: "acl")]
                fn get_bucket_acl (GetBucketAclInput) -> GetBucketAclOutput;
                fn get_object (GetObjectInput) -> GetObjectOutput;
                fn head_object (HeadObjectInput) -> HeadObjectOutput;
                fn get_object_attributes (GetObjectAttributesInput) -> GetObjectAttributesOutput;
                #[gated(tagging)]
                fn get_object_tagging (GetObjectTaggingInput) -> GetObjectTaggingOutput;
                #[gated(acl: "acl")]
                fn get_object_acl (GetObjectAclInput) -> GetObjectAclOutput;
                #[gated(list_objects_v1: "list-v1")]
                fn list_objects (ListObjectsInput) -> ListObjectsOutput;
                #[gated(list_objects_v2: "list-v2")]
                fn list_objects_v2 (ListObjectsV2Input) -> ListObjectsV2Output;
                #[gated(multipart: "multipart")]
                fn list_parts (ListPartsInput) -> ListPartsOutput;
                #[gated(multipart: "multipart")]
                fn list_multipart_uploads (ListMultipartUploadsInput) -> ListMultipartUploadsOutput;
                #[gated(select: "select")]
                fn select_object_content (SelectObjectContentInput) -> SelectObjectContentOutput;
            }
            write {
                fn create_bucket (CreateBucketInput) -> CreateBucketOutput;
                fn delete_bucket (DeleteBucketInput) -> DeleteBucketOutput;
                #[gated(tagging)]
                fn put_bucket_tagging (PutBucketTaggingInput) -> PutBucketTaggingOutput;
                #[gated(tagging)]
                fn delete_bucket_tagging (DeleteBucketTaggingInput) -> DeleteBucketTaggingOutput;
                #[gated(cors: "cors")]
                fn put_bucket_cors (PutBucketCorsInput) -> PutBucketCorsOutput;
                #[gated(cors: "cors")]
                fn delete_bucket_cors (DeleteBucketCorsInput) -> DeleteBucketCorsOutput;
                #[gated(acl: "acl")]
                fn put_bucket_acl (PutBucketAclInput) -> PutBucketAclOutput;
                #[gated(acl: "acl")]
                fn put_object_acl (PutObjectAclInput) -> PutObjectAclOutput;
                fn put_object (PutObjectInput) -> PutObjectOutput;
                fn delete_object (DeleteObjectInput) -> DeleteObjectOutput;
                #[gated(delete_objects)]
                fn delete_objects (DeleteObjectsInput) -> DeleteObjectsOutput;
                #[gated(tagging)]
                fn put_object_tagging (PutObjectTaggingInput) -> PutObjectTaggingOutput;
                #[gated(tagging)]
                fn delete_object_tagging (DeleteObjectTaggingInput) -> DeleteObjectTaggingOutput;
                #[gated(copy_object: "copy")]
                fn copy_object (CopyObjectInput) -> CopyObjectOutput;
                #[gated(copy_object: "copy")]
                fn rename_object (RenameObjectInput) -> RenameObjectOutput;
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_inc())]
                fn create_multipart_upload (CreateMultipartUploadInput) -> CreateMultipartUploadOutput;
                #[gated(multipart: "multipart")]
                fn upload_part (UploadPartInput) -> UploadPartOutput;
                #[gated(multipart: "multipart", copy_object: "copy")]
                fn upload_part_copy (UploadPartCopyInput) -> UploadPartCopyOutput;
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_dec())]
                fn complete_multipart_upload (CompleteMultipartUploadInput) -> CompleteMultipartUploadOutput;
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_dec())]
                fn abort_multipart_upload (AbortMultipartUploadInput) -> AbortMultipartUploadOutput;
            }
        }
    };
}
pub(crate) use tinio_s3_ops;

/// The table read as data, for the cross-check test.
///
/// Two properties, deliberately built two ways:
///
/// - [`table_features`] pushes **unconditionally**. It is the table's facts —
///   name plus stripping features — and must be comparable with `s3.rs`'s raw
///   text, which `include_str!` never filters by feature;
/// - [`table_ops`] carries the derived `#[cfg]`, so it is what *this* build
///   compiles (measured: an 11-row table yields 3 names with
///   `--no-default-features`, 11 with `--all-features`). It is a subset check
///   against the table, never an equality with the file.
///
/// The `allow`s cover this shape, not its content: a `Vec::new()` followed by
/// `push` trips `clippy::vec_init_then_push`, and a row without `#[gated]`
/// never pushes into `feats`, which makes that `mut` unused. Clippy **does**
/// lint local `macro_rules!` output (measured), and the repo gates on
/// `-D warnings`.
macro_rules! op_names {
    (
        read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        /// Every row this feature set compiles.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn table_ops() -> Vec<&'static str> {
            let mut ops = Vec::new();
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                ops.push(stringify!($r));
            )*
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                ops.push(stringify!($w));
            )*
            ops
        }

        /// Every row with the cargo features that strip it — not filtered by
        /// the build, so it can be compared with `s3.rs`'s text.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn table_features() -> Vec<(&'static str, Vec<&'static str>)> {
            let mut rows = Vec::new();
            $(
                #[allow(unused_mut)]
                let mut feats: Vec<&'static str> = Vec::new();
                $( $( $( feats.push($rf); )? )+ )?
                rows.push((stringify!($r), feats));
            )*
            $(
                #[allow(unused_mut)]
                let mut feats: Vec<&'static str> = Vec::new();
                $( $( $( feats.push($wf); )? )+ )?
                rows.push((stringify!($w), feats));
            )*
            rows
        }
    };
}

tinio_s3_ops!(op_names);

#[cfg(test)]
mod tests {
    use super::*;

    /// `(async fn name, cfg features)` for each method of `S3Backend`'s
    /// dispatch, in file order.
    ///
    /// Bounded to the dispatch impl block, and comment lines are skipped
    /// without clearing the pending features: a later test module, or a
    /// comment between an operation's `#[cfg]` and its `fn`, must not be
    /// mistaken for the operation set or silently drop the features.
    fn dispatch_rows(source: &str) -> Vec<(String, Vec<String>)> {
        let mut rows: Vec<(String, Vec<String>)> = Vec::new();
        let mut pending: Vec<String> = Vec::new();
        let mut in_impl = false;
        for line in source.lines() {
            let trimmed = line.trim();
            if !in_impl {
                in_impl = trimmed.starts_with("impl<") && trimmed.contains("S3 for S3Backend");
                continue;
            }
            if trimmed.starts_with("#[cfg(test)]") {
                break;
            }
            if trimmed.starts_with("//") {
                continue;
            }
            if trimmed.starts_with("#[cfg(") {
                // Every cfg in the dispatch is either one `feature = "x"` or
                // an `all(...)` of them.
                pending.extend(
                    trimmed
                        .split("feature = \"")
                        .skip(1)
                        .filter_map(|rest| rest.split('"').next())
                        .map(String::from),
                );
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("async fn ") {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                let mut features = std::mem::take(&mut pending);
                features.sort();
                rows.push((name, features));
                continue;
            }
            if !trimmed.is_empty() {
                pending.clear();
            }
        }
        rows
    }

    #[test]
    fn table_matches_the_s3_dispatch() {
        // The file side is raw text, so it is feature-independent: all 36
        // methods with their `#[cfg]` attributes, in file order.
        let mut expected = dispatch_rows(include_str!("s3.rs"));
        // The table side must not be filtered either — hence
        // `table_features`, which pushes unconditionally. Comparing the
        // `#[cfg]`-filtered `table_ops()` against the file could only ever
        // hold in an all-features build.
        let mut actual: Vec<(String, Vec<String>)> = table_features()
            .into_iter()
            .map(|(name, feats)| {
                let mut feats: Vec<String> = feats.into_iter().map(String::from).collect();
                feats.sort();
                (name.to_string(), feats)
            })
            .collect();
        expected.sort();
        actual.sort();
        // Names AND the cargo features that strip them, sorted on both
        // sides: the table's row order is free (reads first, then writes)
        // and is not `s3.rs`'s order.
        assert_eq!(actual, expected);
    }

    /// What this build compiles comes from the table — the decorators' own
    /// operation set, checked under every feature combination.
    #[test]
    fn compiled_ops_come_from_the_table() {
        let names: Vec<&str> = table_features().into_iter().map(|(name, _)| name).collect();
        for op in table_ops() {
            assert!(names.contains(&op), "{op} is not a table row");
        }
    }

    #[test]
    fn table_has_no_duplicates() {
        let ops = table_ops();
        let mut sorted = ops.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ops.len(), "{ops:?}");
    }
}
```

Add `mod ops;` to `crates/tinio-server/src/backend/mod.rs`'s module list (next to `mod s3;`).

- [ ] **Step 2: Run the tests**

Run: `cargo test -p tinio-server backend::ops`
Expected: PASS — `table_matches_the_s3_dispatch` (36 rows on both sides, names *and* features), `compiled_ops_come_from_the_table`, `table_has_no_duplicates`.

Run: `cargo test -p tinio-server --no-default-features backend::ops`, then `--no-default-features --features multipart`, then `--all-features`.
Expected: PASS in all three. Two properties are checked and they need different forms — which is exactly why `table_features` and `table_ops` are built two ways:

- **the table's facts equal the file's** (names and stripping features): feature-independent, because the file side is `include_str!` text and `table_features` pushes unconditionally;
- **what this build compiles is a subset of the table**: `table_ops` is `#[cfg]`-filtered by the build, so it shrinks as features come off (measured: 3 names with `--no-default-features` where the table has 11 rows). A plain `const` array could not do this, which is why the derived `#[cfg]` form exists.

- [ ] **Step 3: Checkpoint**

Report the files pending commit.

---

### Task 5: `MetricS3` from the table (behavior-preserving refactor #1)

**Files:**
- Modify: `crates/tinio-server/src/metrics.rs` (`impl S3 for MetricS3` at lines 1162–1496; the `multipart_in_progress_dec` area at ~286)
- Modify: `crates/tinio-server/src/lib.rs` (no change needed if `pub mod metrics;` already stands — verify)

**Interfaces:**
- Consumes: `crate::backend::ops::tinio_s3_ops`.
- Produces: `crate::metrics::multipart_in_progress_inc()` (new, `pub(crate)`, `#[cfg(feature = "multipart")]`), consumed by the table's `#[on_ok]` clauses; the additivity of `multipart_in_progress_dec()` to the same table.

- [ ] **Step 1: The `status` label becomes the real HTTP status (decision 11)**

`MetricS3::record`'s Err arm folds every client error into `400` today. Replace it — this is the only behavior change that rides with the refactor, and it makes the label equal what s3s puts on the wire (`s3s/src/ops/mod.rs:136-140` uses exactly `err.status_code().unwrap_or(500)`):

```rust
let status = match &result {
    Ok(_) => 200u16,
    Err(err) => err.status_code().map_or(500, |code| code.as_u16()),
};
```

`status_code()` is populated for every code the server raises except `Custom`, `InvalidAddressingHeader` and `MissingAttachment` (which fall back to 500 — the same fallback the wire uses). Update `record`'s own doc and the module doc's one-liner (`metrics.rs:1087-1090`) to say the label is the real HTTP status, and to record the one known gap: **the Ok arm books `200`** because s3s's generated ops discard `S3Response.status` and re-serialize from the output, so a ranged `GetObject` is 206 on the wire and unreachable at this layer.

Update the one assertion that pins the old class mapping — `metrics.rs:790`, `assert_eq!(s3_counter("CreateBucket", 400), 1)` becomes `404` (`NoSuchBucket.status_code()` is `NOT_FOUND`), and the comment above the loop at `:777-779` follows. Nothing else changes: every operation in `metric_s3_records_every_delegated_operation` answers the trait default `NotImplemented`, which stays 501.

Run: `cargo test -p tinio-server metrics::`
Expected: PASS — `metric_s3_records_status_classes` with the `404` assertion, everything else untouched.

- [ ] **Step 2: Add the missing increment helper**

The table needs `multipart_in_progress_inc()`, which does not exist — the increment is inline in `create_multipart_upload` (`metrics.rs:1409-1411`). Add it beside `multipart_in_progress_dec()` (which is at `metrics.rs:286`), **mirroring the current behavior exactly**: the existing inline increment takes no `test_lock` guard (only `dec` does), so the helper must not add one.

```rust
/// Increment the in-progress-multipart gauge. Not test-lock guarded: the
/// inline increment it replaces was not either (the exact-assert test holds
/// `MULTIPART_GAUGE` across the three multipart operations).
#[cfg(feature = "multipart")]
pub(crate) fn multipart_in_progress_inc() {
    STORAGE_MULTIPART_IN_PROGRESS.inc();
}
```

- [ ] **Step 3: Replace the hand-written delegation**

Delete the whole `#[async_trait] impl<T: S3 + Send + Sync> S3 for MetricS3<T> { … }` block (`metrics.rs:1162–1496`) and replace it with the macro definition plus its invocation:

```rust
/// The metrics decorator's `S3` impl, expanded from the shared table
/// ([`crate::backend::ops`]): every operation routes through
/// [`MetricS3::record`] under its derived wire name, and the three multipart
/// operations carry their in-progress gauge effect in the table's
/// `#[on_ok(...)]` clause.
///
/// The label is the input dto's name minus `Input` — s3s's codegen names
/// each dto pair after the operation, so `CreateMultipartUploadInput` labels
/// `CreateMultipartUpload`. Verified for all 36 by the test below.
macro_rules! metric_s3_impl {
    (
        read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        #[async_trait]
        impl<T: S3 + Send + Sync> S3 for MetricS3<T> {
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                async fn $r(&self, req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                    let out = self.record(wire_name!($ri), self.inner.$r(req)).await;
                    $( if out.is_ok() { $rh } )?
                    out
                }
            )*
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                async fn $w(&self, req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                    let out = self.record(wire_name!($wi), self.inner.$w(req)).await;
                    $( if out.is_ok() { $wh } )?
                    out
                }
            )*
        }
    };
}

tinio_s3_ops!(metric_s3_impl);
```

Add next to `MetricS3`:

```rust
/// The input dto's name minus `Input` — the AWS operation name s3s's
/// codegen gives the pair, and the metric label.
macro_rules! wire_name {
    ($i:ident) => {
        &stringify!($i)[..stringify!($i).len() - "Input".len()]
    };
}
pub(crate) use wire_name;
```
(The `pub(crate) use` is what makes it one copy for all three decorators: `capability.rs` and `readonly.rs` import `crate::metrics::wire_name` rather than defining their own.)

Add `use crate::backend::ops::tinio_s3_ops;` to the file's imports (`metrics.rs` currently imports nothing from `crate::backend`).

- [ ] **Step 4: Run the refactor's gate — the existing test, unedited**

Run: `cargo test -p tinio-server metrics::`
Expected: PASS, with `metric_s3_records_every_delegated_operation` and `metric_s3_maintains_multipart_gauge` **unmodified** and `metric_s3_records_status_classes` unmodified **apart from the one `400`→`404` assertion Step 1 requires**. Any other edit means the refactor changed behavior — fix the refactor, not the test.

Run: `cargo test -p tinio-e2e --test cucumber -- --tags '@SC-008 or @T075'`
Expected: PASS — `metrics.feature` untouched.

- [ ] **Step 5: Checkpoint**

Report the files pending commit, and state explicitly that the three metric tests and `metrics.feature` were not edited **except** `metric_s3_records_status_classes`'s `400`→`404` assertion, which decision 11 requires.

---

### Task 6: `CapabilityS3` — the FR-021 gates move onto the table (refactor #2)

**Files:**
- Create: `crates/tinio-server/src/capability.rs`
- Modify: `crates/tinio-server/src/lib.rs` (module declaration)
- Modify: `crates/tinio-server/src/data.rs` (all four constructors: wrap the backend)
- Modify: `crates/tinio-server/src/backend/mod.rs` (delete `require_cap`)
- Modify: `crates/tinio-server/src/backend/testutil.rs` (the gated setup helper)
- Delete the 26 gate call sites in `backend/{listing,select,cors,objects,buckets,multipart}.rs` and `require_multipart`; migrate the eight off-mode tests that asserted the gates through a bare `S3Backend` (`cors.rs:762`, `buckets.rs:1299/1660`, `multipart.rs:1375`, `objects.rs:2124/2878/3675`, `select.rs:924`)

**Interfaces:**
- Consumes: `crate::backend::ops::tinio_s3_ops`, `crate::backend::Capabilities`.
- Produces: `crate::capability::CapabilityS3<T>` with `pub fn new(inner: T, caps: Capabilities) -> Self` and `pub fn inner(&self) -> &T`.

- [ ] **Step 1: Write the failing test**

Create `crates/tinio-server/src/capability.rs`:

```rust
#[cfg(test)]
mod tests {
    use s3s::{S3Request, dto};

    use super::*;
    use crate::backend::{Capabilities, testutil::{s3_request, setup_with_caps}};

    /// Drive one gated operation through the decorator. The gate lives here
    /// now, so the test must wrap the backend — and by value: s3s implements
    /// `S3` for concrete types only (no blanket impl for `&T`, `Box<T>` or
    /// `Arc<T>`), so a `&S3Backend` would not satisfy `T: S3`.
    async fn refusal(op: &str, caps: Capabilities) -> S3Error {
        let (inner, _bucket) = setup_with_caps(caps).await;
        let wrapped = CapabilityS3::new(inner, caps);
        match op {
            "ListObjects" => wrapped
                .list_objects(s3_request(dto::ListObjectsInput::default()))
                .await
                .unwrap_err(),
            "PutBucketTagging" => wrapped
                .put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
                    bucket: "data".into(),
                    tagging: dto::Tagging { tag_set: vec![] },
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                }))
                .await
                .unwrap_err(),
            other => panic!("unhandled op {other}"),
        }
    }
}
```

Write the two behavior tests as `#[tokio::test]`s: a disabled toggle answers `NotImplemented` whose message is `"<Operation> is disabled"`, and the same operation with the toggle on is **not** `NotImplemented` (it reaches the handler and fails differently, or succeeds). Then run them.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server capability::`
Expected: FAIL — `cannot find type CapabilityS3`.

- [ ] **Step 3: Write the decorator**

Prepend to `crates/tinio-server/src/capability.rs`:

```rust
//! The `[s3]` capability gate as a decorator (FR-021).
//!
//! Refusals used to be the first statement of each handler
//! (`S3Backend::require_cap`, `require_multipart`); they now read the shared
//! operation table instead, so "which toggle refuses which operation" is one
//! column rather than a five-module grep. Disabled operations answer
//! `NotImplemented` with `"<Operation> is disabled"`.
//!
//! Only the *refusals* moved: the toggles' data-path effects (dropping tags
//! and ACL headers, the checksum tee, the CORS route and response
//! decoration) stay in the handlers.

use async_trait::async_trait;
use s3s::{S3, S3Request, S3Response, S3Result, dto, s3_error};

use crate::backend::{Capabilities, ops::tinio_s3_ops};

/// Wraps an `S3` implementation and refuses the operations its `[s3]`
/// capability disables.
#[derive(Debug, Clone)]
pub struct CapabilityS3<T> {
    inner: T,
    caps: Capabilities,
}

impl<T> CapabilityS3<T> {
    /// Wrap `inner`, gating it with `caps`.
    pub fn new(inner: T, caps: Capabilities) -> Self {
        Self { inner, caps }
    }

    /// The wrapped implementation.
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

macro_rules! capability_impl {
    (
        read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        #[async_trait]
        impl<T: S3 + Send + Sync> S3 for CapabilityS3<T> {
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                async fn $r(&self, req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                    if !(true $( $( && self.caps.$rg )+ )?) {
                        let name = wire_name!($ri);
                        return Err(s3_error!(NotImplemented, "{name} is disabled"));
                    }
                    self.inner.$r(req).await
                }
            )*
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                async fn $w(&self, req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                    if !(true $( $( && self.caps.$wg )+ )?) {
                        let name = wire_name!($wi);
                        return Err(s3_error!(NotImplemented, "{name} is disabled"));
                    }
                    self.inner.$w(req).await
                }
            )*
        }
    };
}

tinio_s3_ops!(capability_impl);
```

The matcher is verbatim Task 4's; the `#[on_ok(...)]` clause is matched and dropped, and the `#[cfg]` attributes are derived from the `#[gated]` entries. `wire_name!` is `crate::metrics::wire_name!`: `metrics.rs` re-exports it as `pub(crate) use wire_name;` (Task 5) and this module imports it — one copy of the label derivation, not three.

Add `pub mod capability;` to `crates/tinio-server/src/lib.rs` (alphabetically before `mod data;`).

- [ ] **Step 4: Wire it into the plane and delete the handler gates**

In `crates/tinio-server/src/data.rs`, every place that builds the chain (`new_shared` at 116, `new_with_auth` at 148, `new_with_acl` at 194-196) changes from

```rust
let backend = MetricS3::new(S3Backend::new_shared(Arc::clone(&storage), caps));
```

to

```rust
let backend = MetricS3::new(CapabilityS3::new(
    S3Backend::new_shared(Arc::clone(&storage), caps),
    caps,
));
```

with `CapabilityS3` added to the `use crate::{…}` list. The `new_with_acl` site keeps `.with_identity(…)` on the `S3Backend` before the wrap.

Then delete the 26 gates — each is the **first statement** of its handler:

```text
listing.rs:118/147   ListObjects, ListObjectsV2
select.rs:258        SelectObjectContent
cors.rs:34/56/71     Get/Put/DeleteBucketCors
objects.rs:745/870/894/912/932/961/1017/1197
                     DeleteObjects, Get/Put/DeleteObjectTagging,
                     Get/PutObjectAcl, CopyObject, RenameObject
buckets.rs:281/304/320/340/370
                     Get/Put/DeleteBucketTagging, Get/PutBucketAcl
multipart.rs:220/352/441/551/879/924/1016
                     CreateMultipartUpload, UploadPart, UploadPartCopy,
                     Complete/AbortMultipartUpload, ListParts,
                     ListMultipartUploads
```

Delete `S3Backend::require_cap` (`backend/mod.rs:335-341`) and `require_multipart` (`multipart.rs:206-212`), and drop the now-unused `S3Result`/`s3_error` imports from each touched module. Neither helper has a unit test of its own — what the deletion breaks is the **off-mode behaviour tests**, which assert the refusal by driving a bare `S3Backend` (`testutil.rs:34`'s `setup()` and `:40`'s `setup_with_caps` return `S3Backend<MemoryStorage>`; `buckets.rs:441`'s local `backend_with` and `multipart.rs:1375`'s inline `S3Backend::new` do the same). They stop asserting anything the moment the gate leaves the handler, so this task **authorizes editing them** (the global "existing tests are not edited" rule excepts exactly these eight):

| Test | Line | Why it breaks |
|---|---|---|
| `cors_toggle_off_gates_the_bucket_cors_ops` | `cors.rs:762` | asserts the three CORS gates through a bare backend |
| `tagging_toggle_off_gates_the_bucket_tagging_ops` | `buckets.rs:1299` | same, bucket tagging |
| `bucket_acl_toggle_off_gates_the_ops` | `buckets.rs:1660` | same, bucket ACL |
| `upload_part_copy_respects_copy_object_toggle` | `multipart.rs:1375` | the conjunction: after the move the copy would actually succeed |
| `delete_objects_toggle_off_keeps_no_per_key_checks` | `objects.rs:2124` | the gate half (the per-key check half is a data-path branch and stays) |
| `tagging_toggle_off_drops_the_headers_and_gates_the_ops` | `objects.rs:2878` | header-dropping stays in the handler; the refusal moves |
| `object_acl_toggle_off_gates_the_ops` | `objects.rs:3675` | same, object ACL |
| `select_capability_off_is_not_implemented` | `select.rs:924` | same, select |

Add one helper to `testutil.rs` and move those eight onto it:

```rust
/// Like [`setup_with_caps`], through the capability decorator: the `[s3]`
/// gates live there now (FR-021), so a test that asserts a refusal must
/// drive `CapabilityS3`, not a bare `S3Backend`. The wrapped backend is
/// reachable via [`CapabilityS3::inner`] for fixture setup.
pub(crate) async fn setup_gated_with_caps(
    caps: Capabilities,
) -> (CapabilityS3<S3Backend<MemoryStorage>>, String) {
    let (backend, bucket) = setup_with_caps(caps).await;
    (CapabilityS3::new(backend, caps), bucket)
}
```

The assertions themselves do not change — the code (`NotImplemented`), the CORS trio's `"is disabled"` message and `upload_part_copy`'s either-toggle-off conjunction are all preserved by construction; only the object under test moves from the handler to the decorator. Tests that build their backend inline (`multipart.rs:1375`) wrap it the same way, and `backend.storage()` becomes `backend.inner().storage()`.

Then run the crate and migrate anything else that fails for the same reason — the table above is the verified set, not a licence to skip the run.

- [ ] **Step 5: Normalize the six multipart messages**

The six multipart operations now answer `"<Operation> is disabled"` instead of `"multipart is disabled"`. Nothing asserts the old strings; grep to confirm before and after:

Run: `rg -n "multipart is disabled" crates/ specs/`
Expected before: only `multipart.rs:210` (the deleted helper). After: no hits.

- [ ] **Step 6: Run the refactor's gate**

Run: `cargo test -p tinio-server`
Expected: PASS — with the eight off-mode tests migrated onto `CapabilityS3` (Step 4) and their assertions unchanged.

Run: `cargo test -p tinio-e2e --test cucumber -- --retry 2`
Expected: PASS with the off-mode scenarios (`@cors-off`, `@tagging-off`, `@acl-off`, `@minimal-caps`) **unedited**: they drive a real plane, which now carries `CapabilityS3`, so the refusals still land. `cors.rs:774`'s `contains("is disabled")` assertion is preserved verbatim — it just runs against the decorator now. The only intended wire-visible change is the six normalized multipart messages (Step 5).

Run: `cargo clippy -p tinio-server --all-targets -- -D warnings`
Expected: clean — in particular no `unused_imports` left behind by the deleted gates.

- [ ] **Step 7: Checkpoint**

Report the files pending commit; state which eight off-mode unit tests were migrated onto `CapabilityS3` (with their assertions unchanged) and that the cucumber off-mode scenarios were not edited.

---

### Task 7: `S3ReadonlyBackend`

**Files:**
- Create: `crates/tinio-server/src/readonly.rs`
- Modify: `crates/tinio-server/src/lib.rs`

**Interfaces:**
- Consumes: `crate::backend::ops::tinio_s3_ops`.
- Produces: `crate::readonly::S3ReadonlyBackend<T>` with `pub fn new(inner: T) -> Self` and `pub fn inner(&self) -> &T`.

- [ ] **Step 1: Write the failing test**

Create `crates/tinio-server/src/readonly.rs` with a fake inner `S3` that records every call, and three `#[tokio::test]`s:

- every **read** in the table reaches the inner backend and is not `NotImplemented`;
- every **write** in the table answers `AccessDenied` **and the inner backend was never called**;
- `post_object` (the browser form upload, which s3s's trait default routes to `self.put_object`) answers `AccessDenied`.

Drive the read/write split from the same table the decorator uses — `tinio_s3_ops!` with a test-only consumer macro emitting the two name lists — so the test cannot drift from the implementation:

```rust
macro_rules! readonly_op_names {
    (
        read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        /// Every `read` row compiled into this feature set.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn read_ops() -> Vec<&'static str> {
            let mut ops = Vec::new();
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                ops.push(stringify!($r));
            )*
            ops
        }

        /// Every `write` row compiled into this feature set.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn write_ops() -> Vec<&'static str> {
            let mut ops = Vec::new();
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                ops.push(stringify!($w));
            )*
            ops
        }
    };
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server readonly::`
Expected: FAIL — `cannot find type S3ReadonlyBackend`.

- [ ] **Step 3: Write the decorator**

```rust
//! Read-only mode as a decorator (FR-023).
//!
//! [`S3ReadonlyBackend`] forwards every read to the wrapped implementation
//! and refuses every write with `AccessDenied`, without touching it. The
//! write set is the table's `write` group, so it cannot drift from the
//! protocol mapping.
//!
//! The refusal message is the same literal `tinio-auth`'s `denied()` uses
//! (AWS's default), so a read-only refusal is indistinguishable on the wire
//! from an ACL denial — see `contracts/s3-surface.md`'s 403-ambiguity note.

use async_trait::async_trait;
use s3s::{S3, S3Request, S3Response, S3Result, dto, s3_error};

use crate::backend::ops::tinio_s3_ops;

/// Wraps an `S3` implementation, refusing every mutating operation.
#[derive(Debug, Clone)]
pub struct S3ReadonlyBackend<T> {
    inner: T,
}

impl<T> S3ReadonlyBackend<T> {
    /// Wrap `inner` in read-only mode.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    /// The wrapped implementation.
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

macro_rules! readonly_impl {
    (
        read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        #[async_trait]
        impl<T: S3 + Send + Sync> S3 for S3ReadonlyBackend<T> {
            // reads: forward
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                async fn $r(&self, req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                    self.inner.$r(req).await
                }
            )*
            // writes: refuse, never touching the inner backend
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                async fn $w(&self, _req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                    Err(s3_error!(AccessDenied, "Access Denied"))
                }
            )*
        }
    };
}

tinio_s3_ops!(readonly_impl);
```

Add `pub mod readonly;` to `crates/tinio-server/src/lib.rs` (alphabetically after `pub mod pipeline;`).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-server readonly::`
Expected: PASS — every read forwards (not 501), every write is `AccessDenied` with the inner untouched, `post_object` is `AccessDenied`.

- [ ] **Step 5: Checkpoint**

Report the files pending commit.

---

### Task 8: `PlaneConfig` and the read-only wiring

**Files:**
- Modify: `crates/tinio-server/src/data.rs`
- Modify: `crates/tinio-server/src/lib.rs` (re-export `PlaneConfig`)

**Interfaces:**
- Consumes: `AccessMode`, `Capabilities`, `S3ReadonlyBackend`, `CapabilityS3`.
- Produces: `crate::data::PlaneConfig { pub caps: Capabilities, pub access: AccessMode }`, `impl From<Capabilities> for PlaneConfig`, `PlaneConfig::read_only(caps) -> Self`, `PlaneConfig::is_read_only(&self) -> bool`; the four `DataPlane` constructors take `plane: impl Into<PlaneConfig>` in place of `caps: Capabilities`.

- [ ] **Step 1: Write the failing test**

In `data.rs`'s test module:

```rust
    #[test]
    fn capabilities_convert_to_read_write_plane() {
        let plane: PlaneConfig = Capabilities::default().into();
        assert!(!plane.is_read_only());
        assert!(PlaneConfig::read_only(Capabilities::default()).is_read_only());
    }
```

The read/write behaviour of a read-only plane is **not** re-asserted here: `S3ReadonlyBackend`'s own table-driven test (Task 7) covers every operation, and the end-to-end assertion on a real plane is Task 11's. This step only pins the construction input.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server data::`
Expected: FAIL — `cannot find type PlaneConfig`.

- [ ] **Step 3: Implement `PlaneConfig` and thread it**

```rust
/// A data plane's construction inputs: the `[s3]` capability toggles and the
/// write policy. `From<Capabilities>` yields read-write, so existing call
/// sites pass a bare `Capabilities` unchanged and a read-only caller opts in
/// with [`PlaneConfig::read_only`].
#[derive(Debug, Clone, Copy)]
pub struct PlaneConfig {
    /// The `[s3]` capability toggles (FR-021).
    pub caps: Capabilities,
    /// The write policy (FR-023).
    pub access: AccessMode,
}

impl PlaneConfig {
    /// A read-only plane over `caps`.
    pub fn read_only(caps: Capabilities) -> Self {
        Self {
            caps,
            access: AccessMode::ReadOnly,
        }
    }

    /// Whether this plane refuses mutating operations.
    pub fn is_read_only(&self) -> bool {
        self.access.is_read_only()
    }
}

impl From<Capabilities> for PlaneConfig {
    fn from(caps: Capabilities) -> Self {
        Self {
            caps,
            access: AccessMode::ReadWrite,
        }
    }
}
```

Change the four constructors' `caps: Capabilities` parameter to `plane: impl Into<PlaneConfig>`, binding `let plane = plane.into();` at the top and using `plane.caps` where `caps` was used. The chain becomes:

```rust
// read-write
let backend = MetricS3::new(CapabilityS3::new(S3Backend::new_shared(storage, plane.caps), plane.caps));
// read-only
let backend = MetricS3::new(S3ReadonlyBackend::new(CapabilityS3::new(
    S3Backend::new_shared(storage, plane.caps),
    plane.caps,
)));
```

The two arms have different types, so each constructor forks on `plane.is_read_only()` — beside the `#[cfg(feature = "cors")]` fork the constructors already carry. Extract the fork into a small private helper per constructor *or* duplicate the two lines; do not try to unify them behind a single variable, the types differ.

Re-export `PlaneConfig` from `lib.rs` next to `DataPlane`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS — all 15 existing constructor call sites compile unchanged through `From<Capabilities>`.

Run: `cargo check --workspace --all-targets`
Expected: clean.

- [ ] **Step 5: Checkpoint**

Report the files pending commit.

---

### Task 9: `tinio-fs` — read-only startup gating

**Files:**
- Modify: `crates/tinio-fs/src/cleanup.rs` (`FsCleanup` struct + `new`, `repair_delete_tombstones` at 186, `repair_bucket_staging` at 229)
- Modify: `crates/tinio-fs/src/scanner.rs` (the tombstone-leftover stage at 264-280)

**Interfaces:**
- Consumes: `CleanupOptions.access: AccessMode` (Task 1).
- Produces: `FsCleanup` skips its two root-writing stages when constructed with `AccessMode::ReadOnly`; the scanner skips its tombstone purge the same way.

- [ ] **Step 1: Write the failing tests**

In `crates/tinio-fs/src/cleanup.rs`'s test module, add a `#[tokio::test]` that builds an fs storage over a temp root, plants residue in both places — a file under `<state-dir>/tmp/` and a tree under `<root>/.tinio/deleting/` — constructs `FsCleanup` with `CleanupOptions { access: AccessMode::ReadOnly, ..Default::default() }`, runs `repair(RepairKind::Startup)`, and asserts: the tombstone is **still there** and the root's `.tinio/deleting` was not enqueued, while the state-dir tmp residue **was** cleared.

A second `#[tokio::test]`, in `scanner.rs`'s test module, covers the scanner's own gate (nothing else exercises it — the e2e harness passes no scanner, so this is the only coverage): plant a tree under `<root>/.tinio/deleting/`, run one scan cycle with `ScannerOptions { access: AccessMode::ReadOnly, .. }`, and assert it is untouched and not enqueued; then repeat with `AccessMode::ReadWrite` and assert it **is** enqueued — the positive control, without which the test would also pass if the scan silently did nothing.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-fs cleanup:: scanner::`
Expected: FAIL — the tombstone is removed (the stage still runs) and the purge still enqueues.

- [ ] **Step 3: Implement the gate**

Add to `FsCleanup` (beside `dry_run`, which is the only `CleanupOptions` field stored today):

```rust
    /// The plane's write policy: read-only mode forbids the stages that
    /// write the storage root (FR-023).
    access: AccessMode,
```

In `FsCleanup::new`, set `access: options.access`. In `repair` (`cleanup.rs:620-632`), guard the two root-writing stages:

```rust
        if self.access.is_read_only() {
            tracing::debug!(
                "read-only mode: skipping the root-writing repair stages \
                 (delete tombstones, bucket staging residue)"
            );
        } else {
            self.repair_delete_tombstones(&mut actions).await;
            self.repair_bucket_staging(&mut actions).await;
        }
```

The state-dir stages (`repair_tmp`, `repair_multipart_orphans`, `repair_orphan_upload_dirs`) and the read-probe stage (`repair_buckets`) stay unconditional.

Read-only mode reaches the scanner through a new `ScannerOptions.access: AccessMode` field (default `ReadWrite`, so every existing construction compiles unchanged); the tombstone-leftover block (`scanner.rs:264-280`) is skipped when it is read-only.

- [ ] **Step 4: The `FsOptions` construction guard (decision 13)**

`FsOptions` gains `access: AccessMode` (default `ReadWrite`) and `FsStorage::new` rejects read-only with no state dir:

```rust
        if options.access.is_read_only() && options.state_dir.is_none() {
            return Err(invalid_value(
                "state_dir: required in read-only mode (the default `<root>/.tinio` would write the root)",
            ));
        }
```

Place it in `resolve` (`backend/mod.rs:370`), before `canonicalize`, so the failure does not depend on the root existing.

Then add the containment half (decision 17a) **after** `canonicalize`, in the same `resolve`: an explicit state dir inside the storage root is refused, because such a caller silently writes the very tree the mode promises to leave alone, and a writable root would never complain:

```rust
        if options.access.is_read_only() {
            if let Some(dir) = options.state_dir.as_deref() {
                // Lexical (see the design's residual): the state dir need not
                // exist yet, so it cannot be canonicalized here.
                let absolute = std::path::absolute(dir)
                    .map_err(|_| invalid_value("state_dir: could not be resolved in read-only mode"))?;
                if absolute.starts_with(&canonical) {
                    return Err(invalid_value(
                        "state_dir: must be outside the storage root in read-only mode",
                    ));
                }
            }
        }
```

`crates/tinio-fs/src/testing.rs`'s `fs_options()` — which constructs every field — gains `access: AccessMode::ReadWrite`.

Test it: `FsStorage::new(root, FsOptions { access: AccessMode::ReadOnly, ..fs_options() })` must return `Err`, and so must the same call with `state_dir: Some(root.join("state"))`.

- [ ] **Step 5: Run the whole fs crate**

Run: `cargo test -p tinio-fs`
Expected: PASS — the new read-only gating test and guard test, plus `read_only_state_relocation_keeps_root_clean` and `delete_bucket_tombstone_stays_on_the_data_volume` both **unedited**. The latter pins the tombstone to the root's volume; this task must not move it.

- [ ] **Step 6: Checkpoint**

Report the files pending commit.

---

### Task 10: `serve` wiring

**Files:**
- Modify: `crates/tinio-server/examples/serve.rs` (arg parsing at 115-145; wiring at 152-232)

**Interfaces:**
- Consumes: `tinio_config::state::home_state_dir`, `sources::apply_read_only_env`, `PlaneConfig::read_only`, `CleanupOptions.access`.
- Produces: a `--read-only` flag and the read-only wiring in the example.

- [ ] **Step 1: Add the flag and the resolution**

In the arg loop (115-145), add:

```rust
            "--read-only" => read_only_flag = true,
```

with `let mut read_only_flag = false;` beside `address`/`config_path`, and extend `usage()` to print it.

After the config is loaded (`serve.rs:158-161`), resolve the mode — lowest precedence first: the config file, then the environment overlay, then the flag:

```rust
    let mut config = match &config_path {
        Some(path) => Some(Config::load(path)?),
        None => None,
    };
    // FR-023 resolution, lowest precedence first: the config file, then the
    // `TINIO_READ_ONLY` overlay, then the flag. `.env` joins when the CLI
    // brings the dotenvy wiring (T066/T067).
    let mut read_only = false;
    if let Some(config) = config.as_mut() {
        tinio_config::sources::apply_read_only_env(config)?;
        read_only = config.server.read_only;
    } else {
        let mut probe = Config::parse("version = 1")?;
        tinio_config::sources::apply_read_only_env(&mut probe)?;
        read_only = probe.server.read_only;
    }
    read_only |= read_only_flag;
    let access = AccessMode::from(read_only);
```

(An absent `--config` still honours `TINIO_READ_ONLY`: the probe parses the minimal document so the overlay has somewhere to land.)

- [ ] **Step 2: Wire the read-only behavior**

- Skip the root creation when read-only — the root must already exist, and `FsStorage::new` canonicalizes it, so a missing root fails with a clear error rather than being created:

```rust
    if !read_only {
        fs::create_dir_all(&root)?;
    }
```

- Resolve the state dir before constructing the storage, and log it — it now lives outside the root, and which of the two it is matters to an operator:

```rust
    let state_dir = if read_only {
        // `TINIO_STATE_DIR` wins over the derived layout: the read-only
        // deployments where there is no writable home are exactly the ones
        // that need the escape hatch.
        let dir = match tinio_config::state::env_state_dir()? {
            Some(dir) => {
                println!(
                    "read-only mode: state dir {} (from {})",
                    dir.display(),
                    tinio_config::state::STATE_DIR_VAR
                );
                dir
            }
            None => {
                let dir = tinio_config::state::default_home_state_dir(Path::new(&root))?;
                println!("read-only mode: state dir {}", dir.display());
                dir
            }
        };
        Some(dir)
    } else {
        None
    };
```

The override is deliberately **not** part of the `flags > env > .env > file` chain: it selects the state dir, not a config value. It is recorded in `contracts/config.md` as a documented extension of FR-023's state-dir rule (Step 3 of Task 13).

- Pass both to `FsOptions`: `state_dir: state_dir.clone()` where `state_dir: None` stands at line 181, **and** `access` — the mode reaches the fs layer from the same value the plane gets (decision 13), so the guard's precondition and the plane's behaviour cannot drift.
- Skip the chown-privilege probe (decision 14): the `#[cfg(unix)] identity::ensure_chown_privilege(config…, Path::new(&root))?` call at `serve.rs:167-171` creates `<root>/.tinio-chown-probe-<nonce>/probe` — wrap it in `if !read_only { … }`.
- Pass the mode to the cleanup (`FsCleanup::new(&storage, CleanupOptions { access, ..Default::default() })` at line 216).
- Build the plane with it: `plane_for(storage, PlaneConfig { caps, access }, &config)` — `plane_for` takes `PlaneConfig` in place of `Capabilities` and forwards it to `new_with_acl`/`new_with_auth`, which now accept `impl Into<PlaneConfig>`. Keep `capabilities_for` and its two tests unchanged.
- Pass `access` to the scanner options (Task 9's new field).

- [ ] **Step 3: Verify by hand**

Run: `cargo run -p tinio-server --example serve -- ./scratch-root --read-only --port 0`
Expected: prints `read-only mode: state dir <home>/.tinio/roots/<hash>` and a listener line; `./scratch-root` contains no `.tinio/`.

Then, against the printed port:

```sh
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://127.0.0.1:<port> s3 mb s3://demo
```

Expected: fails with `AccessDenied`. And `aws s3 ls` succeeds (an empty bucket list, not a 403).

Run: `cargo test -p tinio-server --example serve` — the two `capabilities_for` tests, unedited.
Expected: PASS.

- [ ] **Step 4: Checkpoint**

Report the files pending commit.

---

### Task 11: The end-to-end integration test (T058)

**Files:**
- Create: `crates/tinio-server/tests/read_only.rs`

**Interfaces:**
- Consumes: `tinio_config::state::home_state_dir`, `PlaneConfig::read_only`, `tinio_fs::{FsOptions, FsStorage}`.
- Produces: nothing.

- [ ] **Step 1: Write the test**

The test builds an fs storage over a scratch root with a state dir resolved under a **scratch home** (never the real `$HOME`), wires a read-only plane, and drives it through the `S3` trait. Dev-dependencies already present: `tokio` (full), `tempfile`, `tinio-fs` (with the `testing` feature's `fs_options()`), `http`.

Cover:

1. **Reads behave identically** — a root pre-populated out-of-band with a bucket directory and an object file serves `HeadBucket`/`GetObject`/`ListObjects` exactly as a read-write plane does.
2. **The mutating classes answer `AccessDenied` on a real plane** — drive a representative subset (`PutObject`, `DeleteObject`, `CopyObject`, `CreateMultipartUpload`, `DeleteObjects`), the five classes that reach five different handlers, and assert `err.code().as_str() == "AccessDenied"`. All 20 write rows are covered by Task 7's table-driven decorator test, which additionally asserts the inner backend is never called — driving all 20 here would re-test the same refusal path while hand-building nine inputs that have no `Default` (`CopyObject` alone has ~40 fields).
3. **State lands under the injected home** — after the plane has served reads, `<home>/.tinio/roots/<hash>/meta.redb` exists and `<root>/.tinio/` does not.
4. **A write to a missing bucket answers `AccessDenied`, not `NoSuchBucket`** (decision 12): drive `PutObject` against a bucket that does not exist and assert 403 — the one wire-visible answer this mode is allowed to differ on, and the leg that would catch a regression to a 404.
5. **`#[cfg(unix)]` genuinely read-only root** — after the storage is opened, `chmod 0500` the root (`std::fs::set_permissions`, `PermissionsExt`), then serve reads and assert they still succeed. Restore the mode before the tempdir drops, or use a `Drop`-guarded fixture.
6. **The root's own state DB is not read (decision 15)** — the in-root meta view is empty by design, and this pins it rather than leaving it undefined. First build a *read-write* storage over the root (`state_dir: None`) and set a bucket tag through it, so `<root>/.tinio/meta.redb` holds a tag; drop it; then build the read-only plane over the same root with a scratch home. Assert: the planted object is still readable/listable, and `GetBucketTagging` answers **200 with an empty tag set** (this server never answers `NoSuchTagSet` — the handler returns `tag_set_from_tags(&[])`), i.e. the tag written through the read-write run is invisible.

- [ ] **Step 2: Run the test**

Run: `cargo test -p tinio-server --test read_only`
Expected: PASS on Windows (leg 5 absent via `#[cfg(unix)]`).

Run on WSL2 (docs/tests.md "Platform-gated verification"): `cargo test -p tinio-server --test read_only`
Expected: PASS with leg 5 active — a genuinely read-only root still serves.

- [ ] **Step 3: Checkpoint**

Report the files pending commit.

---

### Task 12: Cucumber scenarios

**Files:**
- Create: `crates/tinio-e2e/tests/features/read_only.feature`
- Modify: `crates/tinio-e2e/tests/steps/common.rs` (`Server::read_only`, `spawn_with` plane parameter)
- Modify: `crates/tinio-e2e/tests/steps/mod.rs` (the tag dispatch and the doc list)
- Modify: `crates/tinio-e2e/tests/traceability.rs` (drop the FR-023 allow-list entry)
- Modify: `crates/tinio-e2e/README.md` (the tag table)

**Interfaces:**
- Consumes: `_server::PlaneConfig` (the state dir is a scratch `TempDir`; no `tinio-config` dev-dependency is added — the resolved layout is asserted by Task 2's unit tests and Task 11's integration test, and the harness only needs *a* state dir outside the root).
- Produces: a `@read-only` scenario tag.

- [ ] **Step 1: Extend the harness**

In `common.rs`:

- Change `spawn_with(storage, caps, root, scanner)` to take `plane: PlaneConfig` and call `DataPlane::new(storage, plane)`; the six callers pass `caps.into()`.
- Add `_home: Option<TempDir>` to `Server` (both struct literals), `None` everywhere except the new constructor.
- Add the read-only constructor. Its state dir is a **plain scratch tempdir**, not the resolved hashed layout: the e2e suite never observes that path, and reaching the real resolver would add a `tinio-config` dependency plus a `_config` alias to the harness for nothing — the layout is asserted by `tinio-config`'s unit tests (Task 2) and by `crates/tinio-server/tests/read_only.rs` (Task 11). The state dir still sits outside the root, which is what the read-only guarantee needs.

```rust
    /// Serve a fresh filesystem root in read-only mode (FR-023), with the
    /// tool state in a scratch directory outside the root.
    pub async fn read_only(caps: Capabilities) -> Self {
        let root = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                access: AccessMode::ReadOnly,
                state_dir: Some(home.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let mut server =
            Self::spawn_with(storage, PlaneConfig::read_only(caps), Some(root), None).await;
        server._home = Some(home);
        server
    }
```

(`FsOptions.access` and the state-dir requirement are Task 9's guard; without the explicit state dir this construction would be rejected.)

- Recognise the tag with an `else if has_tag(&scenario.tags, "read-only")` branch in `before_hook`, before the `(backend, fs_kind)` match — `FsKind` drives an exhaustive match that would otherwise need an arm in three places. Both `@read-only` and `@fs` must be **scenario-level**: the hook reads `scenario.tags`, and feature-level tags are invisible to it.

- Add the out-of-band setup step in `common.rs`, beside the other `given` steps. It is the only way a read-only scenario can have anything to read — `Given I create bucket` and `Given I upload …` are themselves refused in this mode:

```rust
/// Plant a bucket directory and one object directly on disk (FR-023). The
/// read-only scenarios cannot build their fixture over the wire: every
/// mutating request — bucket create and object upload included — answers
/// `AccessDenied`.
#[given(expr = "the served root holds bucket {string} with object {string} and body {string}")]
async fn served_root_holds(world: &mut super::World, bucket: String, key: String, body: String) {
    let root = world
        .server
        .as_ref()
        .and_then(|server| server.root().map(Path::to_path_buf))
        .expect("a read-only scenario needs an fs-backed server");
    let path = root.join(&bucket).join(&key);
    tokio::fs::create_dir_all(path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&path, body.as_bytes()).await.unwrap();
}
```

`Server::root()` already exposes the served root for fs-backed servers, and `tokio::fs` is already imported in `common.rs`.

- [ ] **Step 2: Write the feature file**

`read_only.feature`. Fixtures are planted on disk by the step above; **reads are limited to what a planted root can express** — buckets and objects. Tagging, ACL and multipart reads depend on state in `meta.redb` that the filesystem cannot plant, so they stay with the Rust suites. Every scenario carries `@fs` (the constructor builds an `FsStorage`, so the tag must not claim backend neutrality) and `@read-only`.

```gherkin
# Read-only mode (FR-023): every mutating operation answers AccessDenied and
# reads are served unchanged. The other half of FR-023 — the relocated home
# state dir and a genuinely read-only root — is not observable through the
# wire and is asserted in crates/tinio-server/tests/read_only.rs.
#
# Fixtures are planted on disk: in this mode `Given I create bucket` and
# `Given I upload …` are themselves refused. Reads stop at what a planted
# root can express (buckets, objects); tagging / ACL / multipart read state
# lives in `meta.redb` and is covered by the Rust suites.
@FR-023
Feature: Read-only mode

  @fs @read-only
  Scenario: Reads are served unchanged
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "HEAD" request to "/data"
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 200
    And the response body contains "hello"
    When I send a "HEAD" request to "/data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/data?list-type=2"
    Then the response status is 200
    And the response body contains "a.txt"

  @fs @read-only
  Scenario: Bucket writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/other"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data"
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Object writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/data/b.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data/a.txt"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/data/b.txt" with headers
      | x-amz-copy-source | /data/a.txt |
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Batch delete is refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "POST" request to "/data?delete" with headers and body "<Delete><Object><Key>a.txt</Key></Object></Delete>"
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Multipart writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "POST" request to "/data/b.txt?uploads"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data/b.txt?uploadId=anything"
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Tagging and ACL writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/data?tagging" with headers and body "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/data/a.txt?acl" with headers
      | x-amz-acl | private |
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: A write to a missing bucket is refused too
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/ghost/a.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
```

The last scenario pins decision 12 — a `403`, not the `404 NoSuchBucket` the read-write server answers for a genuinely missing bucket. The step vocabulary is pre-existing (`the response status is`, `the error code is`, `the response body contains`, the raw-request steps); the only new step in this task is `served_root_holds`.

- [ ] **Step 3: Drop the allow-list entry and document the tag**

In `traceability.rs`, delete the `FR-023` entry and its comment (lines 82-84) — `@FR-023` in the new feature satisfies both directions of the cross-check.

In `crates/tinio-e2e/README.md`'s tag table, add a row beside `@cors-off`:

```markdown
| `@read-only` | scenario | `PlaneConfig::read_only` — every mutating operation answers `AccessDenied` (FR-023). |
```

- [ ] **Step 4: Run the suite**

Run: `cargo test -p tinio-e2e --test cucumber -- --retry 2`
Expected: PASS. This is the default (fs) leg and it is the leg that runs the `@read-only` scenarios: every scenario carries `@fs` because `Server::read_only` builds an `FsStorage`, so the mem leg's `not @fs` filter excludes them by construction.

Run: `TINIO_E2E_BACKEND=mem cargo test -p tinio-e2e --test cucumber -- --tags 'not @fs and not @parquet and not @interop and not @boto3 and not @mc' --retry 2`
Expected: PASS with the `@read-only` scenarios **skipped** (`not @fs`) — they are not backend-neutral, and a mem-backed read-only plane does not exist in this scope.

Run: `cargo test -p tinio-e2e --test traceability`
Expected: PASS.

- [ ] **Step 5: Checkpoint**

Report the files pending commit.

---

### Task 13: Bookkeeping

**Files:**
- Modify: `specs/001-s3-local-server/spec.md`, `data-model.md`, `contracts/config.md`, `contracts/s3-surface.md`, `research.md`, `tasks.md`
- Modify: `CONTEXT.md`

**Interfaces:** none.

- [ ] **Step 1: The spec's `sha1` wording**

FR-023 and the Reserved-Directory sections say `sha1(canonical root)16`. The implementation derives the name from `sha2::Sha256`'s first 16 hex characters. Update the wording in `spec.md` (FR-023), `data-model.md` (Reserved Directory), `contracts/config.md` (the read-only section), and `research.md` §21 (its dependency table names `dirs`; note the hashing choice beside it).

Same section of `contracts/config.md`, two additions this design introduces:

- **`TINIO_STATE_DIR`** — an explicit state dir used verbatim instead of the derived `~/.tinio/roots/<hash>/`, for the deployments with no writable home (service accounts, containers, immutable images). Record that it is not part of the `flags > env > .env > file` chain (it selects the state dir, not a config value), that an empty value is an error, and that its parents are not created mode 0700 and are not symlink-checked — the caller chose the location. Add it to the environment-variable list beside `TINIO_READ_ONLY` and `TINIO_SCANNER`.
- **Hardening (decision 17)** — a read-only storage refuses an explicit state dir that lies inside the served root, and the derived `<home>/.tinio` / `roots` are refused if they are symlinks (normal mode's `prove_contained` already refuses a symlinked `<root>/.tinio`).

- [ ] **Step 2: The capability-message note**

`contracts/s3-surface.md`'s FR-021 paragraph and `contracts/config.md`'s `[s3]` bullet: state that a disabled capability answers `NotImplemented "<Operation> is disabled"` uniformly — the six multipart operations included, whose message changed from `"multipart is disabled"`. Note the error *code* is unchanged.

- [ ] **Step 3: The read-only operation list**

`contracts/s3-surface.md`'s "Read-only mode (FR-023)" section currently lists only bucket/object/multipart operations. Replace it with the actual write set: the table's 20 `write` rows, naming the tagging, ACL and CORS writes that the parenthetical omitted, and add read-only mode as a fourth source of the 403 ambiguity. Record the missing-bucket deviation (decision 12) in the same section: a write to a bucket that does not exist answers `AccessDenied`, not `NoSuchBucket`, with the AWS/MinIO authorization-first citations, and a note that the B3 every-principal-404 pass-through is a read-write-mode rule.

The same section gains the two consequences this design makes explicit (decisions 15 and 17a):

- **The root's own `.tinio/` is not read.** `spec.md` already says other contents of the root's `.tinio/` are ignored in read-only mode; spell out what that means for a root previously served read-write: its tags, ACLs, CORS configuration and upload records live in `<root>/.tinio/meta.redb` (the only place they are stored — there are no in-root sidecars), so those reads answer empty while objects still serve from disk. The ACL plane is fail-closed: the owner identity still passes by the owner bypass, a non-owner identity loses its grants. Point at `TINIO_STATE_DIR` for an operator who wants the state dir somewhere specific.
- **An in-root state dir is refused in read-only mode** (construction error), because it would silently write the tree the mode promises to leave alone.

- [ ] **Step 4: `tasks.md`**

T058, T076 and T077 each carry a clause this plan does not implement (the root-side config read; the `.env` load from the state dir; tinio-api's state/socket/logs relocation). Split them rather than over-claim — in this file a tick means done:

1. **T077** (write enforcement across the S3 mapping groups) is delivered in full by Tasks 6–8 and 11: tick it.
2. **Append one new task** (next free ID, US2) carrying the deferred clauses: *read a pre-existing `<root>/.tinio/.tinio.toml` / `config.toml` in read-only mode without ever writing it (`.tinio.toml` wins when both exist), load `.env` from the state dir, and relocate tinio-api's state file, control socket and logs — lands with the CLI, T066/T067 (FR-023).*
3. **T058** — move its trailing clause (*"pre-existing root config still read but never written"*) into that new task, then tick it: the integration test itself is Task 11.
4. **T076** — move the tinio-api half (*"affects tinio-api (state, socket, logs)"*) into the new task, then tick the rest: the resolver, the mode-0700 parents, the fs-side relocation (`meta.redb`, multipart, tmp) and the root-never-written guarantee are this plan's Tasks 2, 9 and 10.

Notes on the ticked entries: the CLI flag (T067) reuses `tinio_config::state::{home_state_dir, env_state_dir}` and `sources::apply_read_only_env`; `TINIO_STATE_DIR` is this design's addition; and on T050/T133 the multipart denial messages are now per-operation.

- [ ] **Step 5: `CONTEXT.md`**

Add a "Read-only mode" glossary entry beside "Capability toggle":

```markdown
- **Read-only mode** — the served plane's write policy (FR-023): every mutating S3 operation answers `AccessDenied` and the storage root is never written, so a genuinely read-only filesystem can be served. Tool state relocates from `<root>/.tinio/` to `~/.tinio/roots/<hash>/` (`TINIO_STATE_DIR` overrides the location). The root's own `.tinio/` is not read in this mode, so metadata written by an earlier read-write run — tags, ACLs, CORS, upload records — reads back empty while objects still serve.
```

- [ ] **Step 6: Verify the doc gate**

Run (bash): `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
Expected: clean.

- [ ] **Step 7: Final gate**

Run every command in docs/tests.md "Standard acceptance" that the environment supports, and report the output of each. Do not run git commands.

---

## Self-Review

**Spec coverage** — every section of `2026-09-18-read-only-mode-design.md` maps to a task: §1 → T1, §2 → T2/T3, §3 → T9, §4 → T4, §5 → T5, §6 → T7, §7 → T6, §8 → T8, §9 → T10, §10 → T4/T5/T6/T7/T9/T11/T12, §11 → T13. The design's standing caveat (only half of a capability is a refusal) is recorded in the design, not implemented — T6's Step 4 explicitly leaves the data-path branches alone.

**Placeholder scan** — every consumer's matcher is spelled out in full in its own task (the merged `#[gated(field[: "feature"] …)]` form) and each transcriber shows the derived-`#[cfg]` emission, so no task defers to another. Where a test body would only restate boilerplate the step states the exact operations, inputs and assertions to drive rather than pasting the whole listing (Task 6's two behaviour tests, Task 7's three, Task 11's six legs); every other code step is complete as written.

**Measured, not assumed** — the shapes in Task 4 were compiled in a standalone crate before being written down, so they are not open to re-derivation by taste: the row matcher plus the three-level `$( $( $(#[cfg(feature = $f)])? )+ )?` emission compile under `#[async_trait]` against s3s 0.16 (both `--all-features` and `--no-default-features`); the two-level form of `table_features` fails with *this must repeat at least once*; clippy lints local `macro_rules!` output, so the `#[allow]`s are load-bearing (`clippy::vec_init_then_push` fires, `unused_mut` fires on the ten ungated rows) while `clippy::absolute_paths` is expansion-suppressed and does not; `$( if out.is_ok() { $rh } )?` is the shape that avoids an empty `if` on the 33 rows without `#[on_ok]`. Each operation appears in exactly one group: a duplicated row is a compile error in every generated `impl`, and `list_parts` / `list_multipart_uploads` are reads, so they appear once, in `read`.

**Type consistency** — `AccessMode` (T1) is the type of `CleanupOptions.access` (T1), `PlaneConfig.access` (T8) and `FsCleanup.access` (T9). `home_state_dir(root, home)` keeps its two-argument shape in T2, T10, T11 and T12; the state dir reaches `serve` (T10) either from `env_state_dir()` (T2) or from `default_home_state_dir(root)` (T2), and never from both at once. `PlaneConfig::read_only(caps)` (T8) is what T10, T11 and T12 call. `tinio_s3_ops!(consumer)` (T4) is invoked by `metric_s3_impl` (T5), `capability_impl` (T6), `readonly_impl` (T7) and `op_names` (T4) — four consumers, as the per-task notes say.
