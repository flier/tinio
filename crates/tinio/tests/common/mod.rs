//! The `doctor` residue's fixture: the root the schema leg diagnoses and the spawn shape it uses.
//!
//! Split out of `doctor.rs` (2026-09-23, code-review round) so the file holds the **legs** and this
//! one holds what they are built from; most of the fixture went with the legs when the doctor's
//! flows became `crates/tinio-e2e/tests/features/doctor.feature` (2026-10-01 test placement
//! cleanup, T6), and what is left is what the one remaining leg reads. It is `common/` because
//! that is this workspace's home for a package's shared test helpers
//! (`crates/tinio-server/tests/common/`), not because a second target reads it yet:
//! `lifecycle.rs`, the package's other process target, is `#[cfg(windows)]` throughout and folds
//! nothing in without dragging that gate through here.
//!
//! Nothing in here asserts — these are the helpers; the leg owns the expectations.

#![allow(clippy::absolute_paths, dead_code)]
// **A test-support module inside a test target, and a stated exemption because clippy's test
// keys do not reach it** (docs/style.md §Panics): `doctor.rs` declares `mod common;`, but
// `allow-*-in-tests` covers `#[test]` functions and `#[cfg(test)]` modules, while the fixture's
// sites are plain helper `fn`s (design 2026-10-09 D7).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

// The fixture spells `std::fs`/`std::process` at the call site rather than importing each one;
// this is a test crate and the absolute paths are the point (they name the module, not a local
// alias), the same shape `commands::doctor::tests` uses.
use std::{path::Path, process::Command};

/// The token the fixture writes into the `state` file: distinctive on purpose, so a rendering that
/// quoted the bytes it read would be caught by a string nothing else could produce. The scenario
/// that asserts it reaches neither stream is `doctor.feature`'s
/// (`crates/tinio-e2e/tests/features/doctor.feature`).
pub const TOKEN: &str = "doctor-must-never-print-this-token";

/// A configuration complete enough that nothing but the defect under test is wrong — the `[auth]`
/// pair included, because "no credentials at all" is a `warn` of its own and a fixture without it
/// could never reach a report with no problem in it.
pub const COMPLETE_CONFIG: &str = "version = 1\n\
     [auth]\n\
     access_key = \"tinio-access-key\"\n\
     secret_key = \"tinio-secret-key\"\n\
     [server]\n\
     port = 9000\n";

pub fn tinio() -> std::path::PathBuf {
    // `CARGO_BIN_EXE_*` exists for bins of the crate under test, and this file is in the facade
    // crate for exactly that reason.
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_tinio"))
}

/// Every spawn in this file goes through here (Global Constraints): the state location is
/// pinned to the test's own temp dir on every platform. `TINIO_STATE_DIR` is what decides
/// where the resolver may look (read-only mode's override), and `HOME`/`USERPROFILE` are the
/// unix arm of the home-derived candidate — `dirs` resolves the home through
/// `SHGetKnownFolderPath` on Windows, so those two alone would leave a Windows spawn reading
/// the developer's real profile and creating a state dir there.
pub fn spawn(dir: &Path) -> Command {
    let mut cmd = Command::new(tinio());
    cmd.env("TINIO_STATE_DIR", dir.join("home-state"))
        .env("HOME", dir)
        .env("USERPROFILE", dir);
    cmd
}

/// Runs the binary under test as `doctor <root> <flags...>` and answers its whole output: the
/// one place the process leg's spawn shape lives, so it cannot forget that the state location
/// is pinned ([`spawn`]) or spell the flag list out again.
///
/// **The child goes through the guard, and the capture recipe is the guard's own.**
/// `Command::output()` was this line's shape — the raw spawn and wait, and the site
/// `crates/tinio-util/tests/spawn_guard.rs` admits as a shape it must report — so
/// `job::output` is what this helper calls now: the spawn is the guard's, and the three stdio
/// modes a capture needs (`stdin` null, the other two piped) are the helper's rather than lines
/// written out here. What it replaces left the modes to the caller because `job::spawn` takes
/// the `Command` the builder left and sets no stdio of its own.
pub fn run_doctor(dir: &Path, root: &Path, flags: &[&str]) -> std::process::Output {
    let mut command = spawn(dir);
    command.arg("doctor").arg(root).args(flags);
    tinio_util::testing::job::output(&mut command).expect("doctor")
}

/// A root holding the complete configuration and nothing else: no store, no `state` file, no
/// `tmp/` residue.
pub fn configured_root(dir: &Path) -> std::path::PathBuf {
    let root = dir.join("root");
    std::fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
    std::fs::write(root.join(".tinio/.tinio.toml"), COMPLETE_CONFIG).expect("config");
    root
}

/// The `state` file a crashed run leaves, with `control_name` the channel name its writer
/// recorded. Built as a document, not as a string literal: a Windows path in a hand-written
/// literal would need its backslashes escaped to be JSON at all.
pub fn write_state_file(root: &Path, control_name: &str) {
    write_state_in(&root.join(".tinio"), root, control_name);
}

/// The same document written into an **arbitrary** state dir — the relocated one a read-only run
/// reads — with `root` recorded as the served root the document belongs to.
pub fn write_state_in(state_dir: &Path, root: &Path, control_name: &str) {
    let state = serde_json::json!({
        "version": 1,
        "pid": 999999,
        "token": TOKEN,
        "control_name": control_name,
        "port": 9000,
        "root": root.to_string_lossy(),
        "started_at": "2026-09-20T00:00:00Z",
    });
    std::fs::write(state_dir.join("state"), state.to_string()).expect("state file");
}

/// The control channel name the fixture's `state` file carries: the derivation itself where
/// the plane is compiled in — `tinio_api::state::socket_path` for this root, never a name
/// spelled out by hand — and a path under the fixture where it is not, because without `api`
/// nothing reads the document the leg writes.
#[cfg(feature = "api")]
pub fn control_name(root: &Path) -> String {
    tinio_api::state::socket_path(&root.join(".tinio"), root)
        .expect("socket path")
        .to_string_lossy()
        .into_owned()
}

#[cfg(not(feature = "api"))]
pub fn control_name(root: &Path) -> String {
    root.join(".tinio/no-such-socket.sock")
        .to_string_lossy()
        .into_owned()
}

/// A root with the private state a crashed run leaves behind: a store (a run that crashed had
/// opened one — and a valid redb file cannot be hand-written, so the fixture asks `tinio-fs`
/// for it rather than pretending), a stale `state` file, a leftover temp file, and a
/// configuration complete enough that nothing but the private state is wrong. The store is what
/// makes the report the schema leg reads a *populated* one.
pub fn crashed_root(dir: &Path) -> std::path::PathBuf {
    let root = configured_root(dir);
    std::fs::create_dir_all(root.join(".tinio/tmp")).expect("reserved dirs");
    std::fs::write(root.join(".tinio/tmp/leftover"), b"partial").expect("stale temp");
    // The store, created and immediately closed: `database::open` runs inside, which is the
    // same call a `--fix` run makes, and dropping the handle here is what leaves it openable
    // for the binary under test.
    drop(tinio_fs::FsStorage::new(&root, tinio_fs::testing::fs_options()).expect("store"));
    write_state_file(&root, &control_name(&root));
    root
}
