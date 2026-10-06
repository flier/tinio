//! Read-only mode's residue (T058, FR-023 amended): the derived tool
//! state-dir layout, and nothing else.
//!
//! The rest of this file migrated to
//! `crates/tinio-e2e/tests/features/read_only.feature` (T5 of
//! `plans/2026-10-01-test-placement-cleanup.md`): reads behaving identically and
//! the mutating classes' refusals are that file's `@fs @read-only` scenarios, a
//! genuinely read-only root is its `@unix` one, and the mode's three channels,
//! the in-root meta database and a missing root are its `@cli` ones. Each of
//! them asserts the same rule against the same subject; the halves the wire
//! cannot show are asserted there through the surface that can reach them (the
//! CLI harness's streams and the real binary's own state layout).
//!
//! **This leg is the residue, and the plan above is why**: it asserts the
//! derived `<home>/.tinio/roots/<hash>/` layout the resolver hands a read-only
//! run, and no scenario reaches the derived path — every `@cli` child is handed
//! an explicit `TINIO_STATE_DIR` (the one step that takes it away drives the
//! missing-root leg, which fails before any state dir is resolved), and the
//! in-process read-only plane resolves a scratch home. That layout is FR-023's
//! contract, so it stays where the resolver is exercised directly — and the
//! amendment is asserted beside it: the derived directory is **tool** state,
//! holding no database, while the storage reads the root's own.
//!
//! The plane is the real one: an [`FsStorage`] in read-only mode whose tool
//! state dir is resolved under an injected scratch home (and ignored by the
//! storage), wired through [`PlaneConfig::read_only`] — the fork that stacks
//! the refusal decorator — bound to an ephemeral loopback port and driven with
//! raw unsigned HTTP/1.1 requests. No auth provider is configured, so s3s
//! serves unsigned requests, exactly as the cucumber harness drives its
//! servers.

mod common;

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::SystemTime,
};

use common::{Wire, plant, send};
use tempfile::TempDir;
use tinio_config::state::home_state_dir;
use tinio_core::{mode::AccessMode, storage::Storage};
use tinio_fs::{FsOptions, FsStorage, testing::fs_options};
use tinio_server::{Capabilities, DataPlane, PlaneConfig};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

/// A scratch served root and a scratch home — never the real `$HOME`.
/// Both are removed when the fixture drops.
struct Scratch {
    root: TempDir,
    home: TempDir,
}

impl Scratch {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
            home: tempfile::tempdir().unwrap(),
        }
    }

    /// `<home>/.tinio/roots/<hash>/` for this root (FR-023) — the layout
    /// the read-only plane must use and the only state dir the tests
    /// assert against.
    fn state_dir(&self) -> PathBuf {
        home_state_dir(self.root.path(), self.home.path()).expect("the root canonicalizes")
    }
}

/// A data plane accepting on an ephemeral loopback port, with the accept
/// loop's handle and the shutdown channel that stops it.
struct Plane {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Plane {
    /// Serve `storage` with `plane` — the caller's [`PlaneConfig`] picks
    /// the write policy, so the read-only fork is the one under test.
    async fn spawn<S: Storage>(storage: S, plane: impl Into<PlaneConfig>) -> Self {
        let plane = plane.into();
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            DataPlane::new(storage, plane)
                .serve(listener, rx)
                .await
                .expect("the plane serves");
        });
        Self {
            addr,
            shutdown,
            task,
        }
    }

    /// Stop accepting and wait for the accept loop to end — the plane,
    /// and with it the storage, is dropped by then.
    async fn close(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

/// The read-only plane over `scratch`, and the **tool** state dir this run
/// resolved under its injected home.
///
/// The storage's own state dir is not that one any more (FR-023 amended):
/// a read-only storage reads the root's own `<root>/.tinio`, and the
/// configured dir is the caller's tool state (the socket, `state` file and
/// logs a real run writes there). `scratch.state_dir()` is still the
/// derived `<home>/.tinio/roots/<hash>/` layout the resolution hands a
/// read-only run, and passing it in is exactly the real caller's input —
/// the storage ignores it.
async fn read_only_plane(scratch: &Scratch) -> (Plane, PathBuf) {
    let state_dir = scratch.state_dir();
    let storage = FsStorage::new(
        scratch.root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            state_dir: Some(state_dir.clone()),
            ..fs_options()
        },
    )
    .expect("the read-only storage constructs: it degrades over a root with no database");
    let plane = Plane::spawn(storage, PlaneConfig::read_only(Capabilities::default())).await;
    (plane, state_dir)
}

/// One entry of a root snapshot: its path relative to the root, and — for
/// a file — its length and mtime.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    path: PathBuf,
    file: Option<(u64, SystemTime)>,
}

/// Every entry under `root`, sorted by relative path.
///
/// A write would move one of these — a new name, a file's length or
/// mtime — so two snapshots are the portable "the root was never written"
/// evidence. Directories contribute their name alone.
fn snapshot(root: &Path) -> Vec<Entry> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<Entry>) {
        for entry in fs::read_dir(dir).expect("the root's entries are readable") {
            let entry = entry.expect("a readable entry");
            let path = entry.path();
            let meta = entry.metadata().expect("entry metadata");
            let file = (!meta.is_dir()).then(|| (meta.len(), meta.modified().expect("mtime")));
            out.push(Entry {
                path: path
                    .strip_prefix(root)
                    .expect("under the root")
                    .to_path_buf(),
                file,
            });
            if meta.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut entries = Vec::new();
    walk(root, root, &mut entries);
    entries.sort();
    entries
}

/// One raw unsigned HTTP/1.1 request on a fresh connection
/// (`Connection: close`, response read to EOF) — the head built here, the
/// send and the parse in [`common::send`].
async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Wire {
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\
         Content-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    send(addr, &head, body).await
}

/// Leg 3 (FR-023 amended): the read-only plane reads the **root's own**
/// state and leaves everything else alone.
///
/// The tool's state dir is still the derived `<home>/.tinio/roots/<hash>/`
/// the resolution hands a read-only run — asserted here because no
/// cucumber scenario reaches the derived path (every `@cli` child is
/// handed an explicit `TINIO_STATE_DIR`) — but it holds **no database**:
/// the amendment's storage is the root's own `<root>/.tinio`, opened
/// read-only, and this root has none, so the run serves the planted object
/// from its file with an empty metadata view and creates nothing, under
/// either directory. The root's entries are identical before and after.
#[tokio::test]
async fn the_tool_state_dir_holds_no_database_and_the_root_is_untouched() {
    const LEG: &str = "leg 3 (the tool state dir holds no database; the root is untouched)";
    let scratch = Scratch::new();
    plant(scratch.root.path(), "data", "a.txt", "hello");
    let before = snapshot(scratch.root.path());
    assert!(
        before.iter().any(|entry| entry.file.is_some()),
        "{LEG}: the snapshot must see the planted file: {before:?}"
    );
    let (plane, state_dir) = read_only_plane(&scratch).await;
    let roots = scratch.home.path().join(".tinio").join("roots");
    assert_eq!(
        state_dir.parent(),
        Some(roots.as_path()),
        "{LEG}: the tool state dir must be a root-hashed leaf under <home>/.tinio/roots"
    );
    let served = request(plane.addr, "GET", "/data/a.txt", &[], &[]).await;
    assert_eq!(served.status, 200, "{LEG}: the planted object serves");
    assert!(
        !state_dir.join("meta.redb").exists(),
        "{LEG}: the tool state dir must hold no database ({} holds one)",
        state_dir.display()
    );
    assert!(
        !state_dir.exists(),
        "{LEG}: nothing was written to the tool state dir at all"
    );
    assert!(
        !scratch.root.path().join(".tinio").exists(),
        "{LEG}: the read-only plane wrote the served root"
    );
    assert_eq!(
        snapshot(scratch.root.path()),
        before,
        "{LEG}: the served root's entries changed"
    );
    plane.close().await;
}
