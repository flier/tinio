//! Read-only mode end to end (T058, FR-023).
//!
//! The plane is the real one: an [`FsStorage`] in read-only mode with its
//! state dir resolved under an injected scratch home, wired through
//! [`PlaneConfig::read_only`] — the fork that stacks the refusal
//! decorator — bound to an ephemeral loopback port and driven with raw
//! unsigned HTTP/1.1 requests. No auth provider is configured, so s3s
//! serves unsigned requests, exactly as the cucumber harness drives its
//! servers.
//!
//! One `#[tokio::test]` per leg, so a leg fails on its own and its
//! message names it. What the wire cannot show is asserted here and
//! nowhere else: the state dir landing under the injected home, the
//! served root left without a `.tinio` of its own, a genuinely
//! read-only root still serving (unix), and the empty in-root meta view.

// The legs drive the plane over raw HTTP and name no feature-gated API,
// so this target is gated per request, never as a whole. Only three rows
// need a cargo feature: leg 2's `CopyObject` (`copy`) and
// `CreateMultipartUpload` (`multipart`), and the `?list-type=2` listing
// (`list-v2`) — a stripped build answers 501 for those from s3s's trait
// defaults, which is not the answer under test. Everything else,
// `PutObject`/`DeleteObject`/`DeleteObjects` included, runs in every
// configuration.

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

/// The placeholder `Content-MD5` the wire shapes use (16 zero bytes, the
/// feature file's own literal): nothing verifies the digest against the
/// body — a refusal precedes the handler, whose check is a shape check.
const CONTENT_MD5: &[(&str, &str)] = &[("Content-MD5", "AAAAAAAAAAAAAAAAAAAAAA==")];

/// The `PUT ?tagging` shape: an XML body with the digest AWS requires.
const TAGGING_HEADERS: &[(&str, &str)] = &[
    ("Content-Type", "application/xml"),
    ("Content-MD5", "AAAAAAAAAAAAAAAAAAAAAA=="),
];

/// A tag set the fixture plants through the read-write plane, whose keys
/// must be recognisable in — and absent from — a response body.
const PLANTED_TAG: &str = "<Tagging><TagSet><Tag><Key>planted-key</Key><Value>planted-value</Value></Tag></TagSet></Tagging>";

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

/// One wire row of leg 2: an operation's name and the request that must
/// reach it — one row per mutating class.
struct Refusal<'a> {
    operation: &'a str,
    method: &'a str,
    path: &'a str,
    headers: &'a [(&'a str, &'a str)],
    body: &'a [u8],
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

/// The read-only plane over `scratch`, and the state dir it must use.
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
    .expect("a read-only storage requires an out-of-root state dir");
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
/// evidence. The unix leg proves the same thing with `chmod 0500`, which
/// Windows has no equivalent of (design §10). Directories contribute
/// their name alone.
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

/// `body` with every `<LastModified>…</LastModified>` span masked: the
/// two fixtures are planted milliseconds apart, so a listing carries each
/// file's own mtime while every other byte must match. Each span is masked
/// (not every occurrence of one extracted value), so a second object with
/// a different mtime cannot slip through unmasked.
fn mask_last_modified(body: &str) -> String {
    const OPEN: &str = "<LastModified>";
    const CLOSE: &str = "</LastModified>";
    let mut masked = String::with_capacity(body.len());
    let mut rest = body;
    while let Some(start) = rest.find(OPEN) {
        let from = start + OPEN.len();
        let Some(end) = rest[from..].find(CLOSE) else {
            // An unclosed tag is left as it is.
            break;
        };
        masked.push_str(&rest[..from]);
        masked.push_str("<timestamp>");
        rest = &rest[from + end + CLOSE.len()..];
    }
    masked.push_str(rest);
    masked
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

/// Assert `wire` is the read-only refusal for `operation`: `403` carrying
/// `AccessDenied` — the same literal an ACL denial uses.
fn assert_refused(leg: &str, operation: &str, wire: &Wire) {
    assert_eq!(
        wire.status,
        403,
        "{leg}: {operation} must answer 403: {}",
        wire.text()
    );
    assert_eq!(
        wire.code(),
        "AccessDenied",
        "{leg}: {operation} must answer AccessDenied: {}",
        wire.text()
    );
}

/// The read requests legs 1 and 5 drive against both planes: the three
/// ungated reads always, plus the `list-v2` listing only when the build
/// carries the feature (a stripped build's s3s trait default answers
/// 501, which is not a read to compare).
fn read_requests() -> Vec<(&'static str, &'static str)> {
    // `mut` is needed only in the configurations where the last row
    // pushes; the same shape `backend/ops.rs`'s name tables use.
    #[allow(unused_mut)]
    let mut requests = vec![
        ("HEAD", "/data"),
        ("HEAD", "/data/a.txt"),
        ("GET", "/data/a.txt"),
    ];
    #[cfg(feature = "list-v2")]
    requests.push(("GET", "/data?list-type=2"));
    requests
}

/// Leg 1 (FR-023): reads behave identically. Two roots hold the same
/// planted bucket and object; every read is driven against a read-only
/// and a read-write plane and must answer the same thing — with the
/// read-only answer pinned to `200`, so two identical refusals cannot
/// pass as identical reads.
#[tokio::test]
async fn reads_behave_identically() {
    const LEG: &str = "leg 1 (reads behave identically)";
    let read_only_scratch = Scratch::new();
    let read_write_scratch = Scratch::new();
    for scratch in [&read_only_scratch, &read_write_scratch] {
        plant(scratch.root.path(), "data", "a.txt", "hello");
    }
    let (read_only, _) = read_only_plane(&read_only_scratch).await;
    let read_write = Plane::spawn(
        FsStorage::new(read_write_scratch.root.path(), fs_options()).unwrap(),
        Capabilities::default(),
    )
    .await;
    for (method, path) in read_requests() {
        let served = request(read_only.addr, method, path, &[], &[]).await;
        let reference = request(read_write.addr, method, path, &[], &[]).await;
        assert_eq!(
            served.status,
            200,
            "{LEG}: {method} {path} must serve on the read-only plane: {}",
            served.text()
        );
        assert_eq!(
            (served.status, mask_last_modified(&served.text())),
            (reference.status, mask_last_modified(&reference.text())),
            "{LEG}: {method} {path} differs from the read-write plane"
        );
    }
    // The comparisons above would also hold if both planes answered an
    // empty read, so the planted content is pinned directly.
    let object = request(read_only.addr, "GET", "/data/a.txt", &[], &[]).await;
    assert_eq!(object.text(), "hello", "{LEG}: the planted object's body");
    // The listing needs `list-v2`, like [`read_requests`]'.
    #[cfg(feature = "list-v2")]
    {
        let listing = request(read_only.addr, "GET", "/data?list-type=2", &[], &[]).await;
        assert!(
            listing.text().contains("a.txt"),
            "{LEG}: the listing must carry the planted key: {}",
            listing.text()
        );
    }
    read_write.close().await;
    read_only.close().await;
}

/// Leg 2 (FR-023): one operation from each mutating class answers
/// `AccessDenied` on a real plane, and the refused writes leave the tree
/// alone. All 20 write rows are covered by the decorator's own
/// table-driven test, which also proves the inner backend is never
/// reached.
#[tokio::test]
async fn mutating_classes_are_refused() {
    const LEG: &str = "leg 2 (mutating classes are refused)";
    let scratch = Scratch::new();
    plant(scratch.root.path(), "data", "a.txt", "hello");
    let (plane, _) = read_only_plane(&scratch).await;
    let rows = [
        Refusal {
            operation: "PutObject",
            method: "PUT",
            path: "/data/b.txt",
            headers: &[],
            body: b"x",
        },
        Refusal {
            operation: "DeleteObject",
            method: "DELETE",
            path: "/data/a.txt",
            headers: &[],
            body: b"",
        },
        // `copy`/`multipart` are these two rows' features: without them
        // s3s's trait default answers 501 instead of the refusal.
        #[cfg(feature = "copy")]
        Refusal {
            operation: "CopyObject",
            method: "PUT",
            path: "/data/b.txt",
            headers: &[("x-amz-copy-source", "/data/a.txt")],
            body: b"",
        },
        #[cfg(feature = "multipart")]
        Refusal {
            operation: "CreateMultipartUpload",
            method: "POST",
            path: "/data/b.txt?uploads",
            headers: &[],
            body: b"",
        },
        Refusal {
            operation: "DeleteObjects",
            method: "POST",
            path: "/data?delete",
            headers: CONTENT_MD5,
            body: b"<Delete><Object><Key>a.txt</Key></Object></Delete>",
        },
    ];
    for row in &rows {
        let refused = request(plane.addr, row.method, row.path, row.headers, row.body).await;
        assert_refused(
            LEG,
            &format!("{} ({} {})", row.operation, row.method, row.path),
            &refused,
        );
    }
    // The refusals are the decorator's, not the filesystem's: nothing was
    // created, nothing was removed.
    assert!(
        scratch.root.path().join("data").join("a.txt").is_file(),
        "{LEG}: the refused DELETE removed the planted object"
    );
    assert!(
        !scratch.root.path().join("data").join("b.txt").exists(),
        "{LEG}: a refused write landed on disk"
    );
    plane.close().await;
}

/// Leg 3 (FR-023): the tool's state lands under the injected home — the
/// state dir is the derived `<home>/.tinio/roots/<hash>/`, its database
/// exists once the plane has served reads, and the served root gained no
/// `.tinio` of its own — nor any other change: the root's entries are
/// identical before and after (the portable stand-in for the unix
/// `chmod 0500` proof, which Windows has no equivalent of).
#[tokio::test]
async fn state_lands_under_the_injected_home() {
    const LEG: &str = "leg 3 (state lands under the injected home)";
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
        "{LEG}: the state dir must be a root-hashed leaf under <home>/.tinio/roots"
    );
    let served = request(plane.addr, "GET", "/data/a.txt", &[], &[]).await;
    assert_eq!(served.status, 200, "{LEG}: the planted object serves");
    assert!(
        state_dir.join("meta.redb").is_file(),
        "{LEG}: {} must hold the state database",
        state_dir.display()
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

/// Leg 4 (decision 12): a write to a missing bucket answers
/// `AccessDenied`, not the `NoSuchBucket` the read-write plane answers —
/// the one wire-visible answer read-only mode is allowed to differ on.
#[tokio::test]
async fn a_write_to_a_missing_bucket_is_refused() {
    const LEG: &str = "leg 4 (a write to a missing bucket)";
    let read_only_scratch = Scratch::new();
    let read_write_scratch = Scratch::new();
    for scratch in [&read_only_scratch, &read_write_scratch] {
        plant(scratch.root.path(), "data", "a.txt", "hello");
    }
    // The reference answer: the read-write plane really answers 404 for
    // this request, so the read-only 403 is a difference, not a request
    // shape that never reaches the bucket check.
    let read_write = Plane::spawn(
        FsStorage::new(read_write_scratch.root.path(), fs_options()).unwrap(),
        Capabilities::default(),
    )
    .await;
    let reference = request(read_write.addr, "PUT", "/ghost/a.txt", &[], b"x").await;
    assert_eq!(
        (reference.status, reference.code().as_str()),
        (404, "NoSuchBucket"),
        "{LEG}: the read-write plane answers NoSuchBucket here: {}",
        reference.text()
    );
    read_write.close().await;
    let (read_only, _) = read_only_plane(&read_only_scratch).await;
    let refused = request(read_only.addr, "PUT", "/ghost/a.txt", &[], b"x").await;
    assert_refused(LEG, "PutObject /ghost/a.txt", &refused);
    read_only.close().await;
}

/// Leg 5 (FR-023, unix): a root that cannot be written at all still
/// serves reads once it is `chmod 0500`. Absent on Windows, which has no
/// mode bits; docs/tests.md's platform-gated verification is where it
/// runs.
#[cfg(unix)]
#[tokio::test]
async fn a_read_only_root_still_serves() {
    const LEG: &str = "leg 5 (a genuinely read-only root)";
    let scratch = Scratch::new();
    plant(scratch.root.path(), "data", "a.txt", "hello");
    let (plane, state_dir) = read_only_plane(&scratch).await;
    // Declared after the fixture and the plane so it drops first: the
    // tempdir cannot be removed at mode 0500.
    let _restricted = RestrictedRoot::restrict(scratch.root.path());
    for (method, path) in read_requests() {
        let served = request(plane.addr, method, path, &[], &[]).await;
        assert_eq!(
            served.status,
            200,
            "{LEG}: {method} {path} must serve off a 0500 root: {}",
            served.text()
        );
    }
    assert!(
        state_dir.join("meta.redb").is_file(),
        "{LEG}: state must land in the home, not the root"
    );
    plane.close().await;
}

/// A root restricted to owner read and execute for as long as this guard
/// lives; the original mode comes back on drop.
#[cfg(unix)]
struct RestrictedRoot {
    path: PathBuf,
    mode: u32,
}

#[cfg(unix)]
impl RestrictedRoot {
    /// `chmod 0500` `path`: its owner may read and traverse it, nobody
    /// may write it.
    fn restrict(path: &Path) -> Self {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(path).unwrap().permissions().mode();
        fs::set_permissions(path, fs::Permissions::from_mode(0o500)).unwrap();
        Self {
            path: path.to_path_buf(),
            mode,
        }
    }
}

#[cfg(unix)]
impl Drop for RestrictedRoot {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;

        let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(self.mode));
    }
}

/// Leg 6 (decision 15): the in-root meta database is not read. A tag
/// planted through a read-write run over the same root stays invisible —
/// the read-only plane answers `200` with an empty tag set (this server
/// never answers `NoSuchTagSet`) while the objects the tag belonged to
/// still serve.
#[tokio::test]
async fn the_in_root_meta_database_is_not_read() {
    const LEG: &str = "leg 6 (the in-root meta database is not read)";
    let scratch = Scratch::new();
    plant(scratch.root.path(), "data", "a.txt", "hello");
    // The read-write run: `state_dir: None` is the backend's own
    // `<root>/.tinio`, so the tag lands in the root's database.
    let read_write = Plane::spawn(
        FsStorage::new(scratch.root.path(), fs_options()).unwrap(),
        Capabilities::default(),
    )
    .await;
    let planted = request(
        read_write.addr,
        "PUT",
        "/data?tagging",
        TAGGING_HEADERS,
        PLANTED_TAG.as_bytes(),
    )
    .await;
    assert_eq!(
        planted.status,
        200,
        "{LEG}: planting the tag through the read-write plane: {}",
        planted.text()
    );
    let visible = request(read_write.addr, "GET", "/data?tagging", &[], &[]).await;
    assert!(
        visible.text().contains("planted-key"),
        "{LEG}: the read-write plane must see the tag it planted, else this leg is vacuous: {}",
        visible.text()
    );
    read_write.close().await;
    assert!(
        scratch
            .root
            .path()
            .join(".tinio")
            .join("meta.redb")
            .is_file(),
        "{LEG}: the tag must have landed in the in-root state database"
    );
    // The read-only run over the same root, with a scratch home.
    let (read_only, state_dir) = read_only_plane(&scratch).await;
    let object = request(read_only.addr, "GET", "/data/a.txt", &[], &[]).await;
    assert_eq!(
        object.text(),
        "hello",
        "{LEG}: the planted object still reads"
    );
    // The listing needs `list-v2`, like [`read_requests`]'.
    #[cfg(feature = "list-v2")]
    {
        let listing = request(read_only.addr, "GET", "/data?list-type=2", &[], &[]).await;
        assert!(
            listing.text().contains("a.txt"),
            "{LEG}: the planted object still lists: {}",
            listing.text()
        );
    }
    let tagging = request(read_only.addr, "GET", "/data?tagging", &[], &[]).await;
    assert_eq!(
        tagging.status,
        200,
        "{LEG}: GetBucketTagging is a read and answers 200, never NoSuchTagSet: {}",
        tagging.text()
    );
    // The tag set itself is empty, not merely without the planted key:
    // the read-only plane answers its own fresh meta view.
    assert!(
        !tagging.text().contains("<Tag>"),
        "{LEG}: the read-only tag set must be empty: {}",
        tagging.text()
    );
    assert!(
        state_dir.join("meta.redb").is_file(),
        "{LEG}: the read-only run's own state database"
    );
    read_only.close().await;
}
