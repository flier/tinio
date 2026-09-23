//! The `doctor` process legs' fixture: the roots they diagnose, the spawn shape they share, and
//! the store seeds a redb file cannot be hand-written for.
//!
//! Split out of `doctor.rs` (2026-09-23, code-review round) so the file holds the **legs** and this
//! one holds what they are built from. It is `common/` because that is this workspace's home for a
//! package's shared test helpers (`crates/tinio-server/tests/common/`), not because a second target
//! reads it yet: `lifecycle.rs`, the package's other process target, is `#[cfg(windows)]`
//! throughout and folds nothing in without dragging that gate through here.
//!
//! Nothing in here asserts — these are the helpers; the legs own the expectations. Each target
//! declares `mod common;` and takes what it uses.

#![allow(clippy::absolute_paths, dead_code)]

// The fixture spells `std::fs`/`std::process`/`std::io` at the call site rather than importing
// each one; this is a test crate and the absolute paths are the point (they name the module, not a
// local alias), the same shape `commands::doctor::tests` uses.

#[cfg(all(feature = "api", unix))]
use std::os::unix::net::UnixListener;
use std::{path::Path, process::Command};

#[cfg(all(feature = "api", unix))]
use tinio_api::state::socket_path;

/// The token the fixture writes into the `state` file: distinctive on purpose, because the
/// redaction leg asserts this exact string reaches neither stdout nor stderr, in either rendering.
pub const TOKEN: &str = "doctor-must-never-print-this-token";

/// A configuration complete enough that nothing but the defect under test is wrong — the `[auth]`
/// pair included, because "no credentials at all" is a `warn` of its own and a fixture without it
/// could never reach an exit-0 leg.
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
/// one place the process legs' spawn shape lives, so a leg cannot forget that the state location
/// is pinned ([`spawn`]) or spell the flag list out again.
pub fn run_doctor(dir: &Path, root: &Path, flags: &[&str]) -> std::process::Output {
    spawn(dir)
        .arg("doctor")
        .arg(root)
        .args(flags)
        .output()
        .expect("doctor")
}

/// The report's rows that are not `ok`, **excluding `free space`** — the legs' answer to "was
/// this root reported clean?".
///
/// `free space` reads the **host's** volume, so a machine below doctor's own floor
/// (`max(1 GiB, 5%)` of the filesystem) answers `warn` on a root that has nothing wrong with it.
/// Asserting the whole report clean would make every "clean root" leg a function of the CI disk's
/// free space — which is exactly what happened on a 1.9 TB volume down to its last 3%. Nothing is
/// lost by leaving the host out here: the floor's two arms are pinned with explicit numbers in the
/// unit tests (`below_floor`'s rule and `free_space`'s own row), and this leg is about the root.
pub fn problems(out: &std::process::Output) -> Vec<String> {
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let (severity, rest) = line.split_once(' ')?;
            let check = rest.trim_start().split_once(':')?.0;
            (severity != "ok" && check != "free space").then(|| line.trim().to_string())
        })
        .collect()
}

/// A root holding the complete configuration and nothing else: no store, no `state` file, no
/// `tmp/` residue. The store-less leg's own fixture, because [`crashed_root`] has a store on
/// purpose (a run that crashed had opened one).
pub fn configured_root(dir: &Path) -> std::path::PathBuf {
    let root = dir.join("root");
    std::fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
    std::fs::write(root.join(".tinio/.tinio.toml"), COMPLETE_CONFIG).expect("config");
    root
}

/// [`configured_root`] plus a **valid** store: a root a run has already opened and closed. A
/// valid redb file cannot be hand-written, so the fixture asks `tinio-fs` for it — and drops the
/// handle, which is what leaves the file openable for the binary under test.
pub fn stored_root(dir: &Path) -> std::path::PathBuf {
    let root = configured_root(dir);
    drop(tinio_fs::FsStorage::new(&root, tinio_fs::testing::fs_options()).expect("store"));
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
/// spelled out by hand, the same call Task 4's sibling and the socket-length check make — and a
/// path under the fixture where it is not, because without `api` nothing reads it (the leg that
/// would is compiled out with it).
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
/// configuration complete enough that nothing but the private state is wrong — the `[auth]`
/// pair included, because "no credentials at all" is a `warn` of its own and a fixture without
/// it could never reach the exit-0 leg below. The token is distinctive on purpose: Task 4's
/// redaction leg asserts this exact string never reaches stdout or stderr.
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

/// Sets the store's `compact_needed` marker — the one input the fragmentation rule reads
/// besides the ratio, and the only way a small temp-dir database can be made to need compaction
/// (`needs_compact` never fires below the 64 MiB floor). Through [`Handle::mark_compact_needed`],
/// the marker protocol's own public write half, rather than the state table's: the fixture must
/// not be the second place the marker's storage shape is spelled out.
///
/// [`Handle::mark_compact_needed`]: tinio_fs::database::Handle::mark_compact_needed
pub async fn mark_compact_needed(state_dir: &Path) {
    let open = tinio_fs::database::open(state_dir).expect("open the store");
    tinio_fs::database::Handle::new(open.db)
        .mark_compact_needed(true)
        .await
        .expect("the marker");
}

/// Seeds an **orphaned** `OBJECT_META` row: a live bucket directory with a recorded entry whose
/// object file does not exist. Seeded through `tinio_fs::meta::store` rather than a hand-written
/// `meta.redb`, because a redb file is not a shape a test can write (failure-handling.md §3
/// documents the *file* shapes; this one is the store's).
#[cfg(feature = "api")]
pub async fn seed_orphaned_meta(root: &Path, bucket: &str, key: &str) {
    let store = tinio_fs::meta::store(&root.join(".tinio")).expect("meta store");
    store
        .set(
            &tinio::bucket::name(bucket).expect("bucket name"),
            &tinio::object::key(key).expect("object key"),
            &tinio::ETag::new("d41d8cd98f00b204e9800998ecf8427e").expect("etag"),
            4,
            std::time::SystemTime::UNIX_EPOCH,
            0,
        )
        .await
        .expect("meta row");
}

/// Seeds a **stale bucket record**: a `BUCKETS` row whose bucket directory does not exist.
#[cfg(feature = "api")]
pub async fn seed_stale_bucket_record(root: &Path, bucket: &str) {
    let store = tinio_fs::bucket::store(&root.join(".tinio")).expect("bucket store");
    store
        .record(
            &tinio::bucket::name(bucket).expect("bucket name"),
            std::time::SystemTime::now(),
        )
        .await
        .expect("bucket record");
}

/// Writes the live object every `--fix` leg asserts against: user data the repair must never
/// touch, and the read-back that proves it.
#[cfg(feature = "api")]
pub fn seed_user_data(root: &Path, bucket: &str) {
    let object = root.join(bucket).join("object.txt");
    std::fs::create_dir_all(object.parent().expect("bucket")).expect("bucket dir");
    std::fs::write(&object, b"user data").expect("object");
}
