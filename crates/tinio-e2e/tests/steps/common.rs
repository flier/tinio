//! Shared in-process harness for the cucumber scenarios (T024–T026),
//! ported from `tinio-server/tests/common/mod.rs`.
//!
//! Spins up the real data plane ([`DataPlane`]: hyper + hyper-util hosting
//! the s3s service) on `127.0.0.1:0` and drives it with a minimal raw
//! HTTP/1.1 client over `TcpStream` — one connection per request
//! (`Connection: close`, response read to EOF), so the full wire pipeline
//! (routing, XML, error codes, streaming bodies) is exercised without
//! pulling in an HTTP client dependency. The raw client also allows the
//! malformed/truncated requests the abort tests need.
//!
//! Every step goes through [`Client::request`] — one client bound to the
//! scenario's server by the `#[before]` hook. The free-function helpers
//! (`request`, `extract`, `eventually`, …) are kept for the steps that
//! need them directly. The generic response assertions (status, body
//! contains/omits/equals) live here too, so a new feature does not have
//! to hunt for them in a feature-specific step module.

use std::{
    io,
    io::ErrorKind,
    net::SocketAddr,
    path::{Path, PathBuf},
    str,
    sync::Arc,
    time::Duration,
};

use cucumber::{given, then};
use md5::{Digest, Md5};
use tempfile::TempDir;
use tinio_sandbox::Event;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::sleep,
};

use super::sigv4::Signer;
pub use crate::_fs::testing::fs_options;
use crate::{
    _core::{mode::AccessMode, storage::Storage},
    _fs::{FsOptions, FsStorage, Scanner, ScannerOptions},
    _mem::MemoryStorage,
    _server::{
        _config::Config, Capabilities, DataPlane, Identity, PlaneConfig,
        identity::IdentityFromConfig,
    },
};

/// Repeatable body of length `n`: `(i * 31 + 7) % 256` at position `i`.
/// The ONE byte generator the steps share (F14): the upload-vs-verify
/// pairing relies on the part and object uploads producing identical
/// bytes, so a copy that drifts would silently break the pairing.
pub fn deterministic_bytes(n: u64) -> Vec<u8> {
    (0..n).map(|i| ((i * 31 + 7) % 256) as u8).collect()
}

/// The lower-hex MD5 digest of `data` — an INDEPENDENT oracle of the
/// server's ETag (computed straight from the md5 crate, not through
/// `tinio_core::etag::ETag`: a test that verified the server's ETag
/// with the production helper would be circular). One home for the
/// steps (F14).
pub fn md5_hex(data: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher
        .finalize()
        .into_iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The @acl fixture users (grilling Q2 — the config-driven identity the
/// acl plane spawns with, mirroring `serve`): `[auth]` presents the
/// `[owner]` element as root, `[[users]]` adds user-b. The canonical
/// IDs are the constant values the feature files reference — the
/// root's configured one and user-b's derived one (hex SHA-256 of
/// `USERB`).
pub const ACL_ROOT: (&str, &str) = ("ROOTAK", "root-secret");
pub const ACL_USER_B: (&str, &str) = ("USERB", "user-b-secret");
pub const ACL_ROOT_ID: &str = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";
pub const ACL_USER_B_ID: &str = "8b3873bb48d62959c9316b8bc5a1c11dcb3e14dce9426cc76ce9c67e00523b6c";

/// The fixture config behind the @acl identity — the same
/// `[auth]`/`[owner]`/`[[users]]` assembly `serve` wires (spec §6), so
/// the e2e plane has real configured principals, not test-constructed
/// ones. Built from the access-key / canonical-ID constants above, so
/// the config and the Gherkin literals cannot drift.
fn acl_fixture_config() -> Config {
    let toml = format!(
        r#"version = 1
[auth]
access_key = "{root_ak}"
secret_key = "{root_sk}"
[owner]
canonical_id = "{ACL_ROOT_ID}"
display_name = "ops"
[[users]]
access_key = "{userb_ak}"
secret_key = "user-b-secret"
canonical_id = "{ACL_USER_B_ID}"
display_name = "bob"
"#,
        root_ak = ACL_ROOT.0,
        root_sk = ACL_ROOT.1,
        userb_ak = ACL_USER_B.0,
    );
    Config::parse(&toml).expect("the acl fixture config parses")
}

/// The @acl identity map assembled from the fixture config
/// ([`IdentityFromConfig`] — the config-driven path, never a direct
/// `Identity` construction).
pub fn acl_fixture_identity() -> Arc<Identity> {
    Identity::from_config(&acl_fixture_config())
}

/// Which storage backend the in-process server runs on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Backend {
    #[default]
    Fs,
    Mem,
}

/// Which fs-server variant a scenario's tags demand.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsKind {
    #[default]
    Plain,
    /// root = tempdir/"root" — traversal-proof scenarios.
    NestedRoot,
    /// Fast scanner interval (cold-listing scenarios).
    ColdListing(Duration),
}

/// A running in-process server bound to an ephemeral loopback port.
#[derive(Debug)]
pub struct Server {
    addr: SocketAddr,
    /// Kept alive for the server's lifetime (the fs root must not be
    /// deleted while the plane serves it) — never read otherwise.
    _root: Option<TempDir>,
    /// The read-only scenarios' scratch state dir (FR-023), kept outside
    /// the served root: alive for the server's lifetime — never read
    /// otherwise.
    _home: Option<TempDir>,
    served: Option<PathBuf>,
    shutdown: watch::Sender<bool>,
}

impl Server {
    /// Serve a fresh filesystem-backed root (a temp dir).
    pub async fn fs(caps: Capabilities) -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        Self::spawn_with(storage, caps.into(), Some(root), None).await
    }

    /// Serve the in-memory reference backend.
    pub async fn mem(caps: Capabilities) -> Self {
        Self::spawn_with(MemoryStorage::new().unwrap(), caps.into(), None, None).await
    }

    /// Serve a filesystem backend over a nested root: the base tempdir
    /// contains only the served `root` subdir, so the traversal-proof
    /// scenarios can observe the parent to prove no file escaped.
    pub async fn fs_nested(caps: Capabilities) -> Self {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        fs::create_dir(&root).await.unwrap();
        let storage = FsStorage::new(&root, fs_options()).unwrap();
        let mut server = Self::spawn_with(storage, caps.into(), Some(base), None).await;
        server.served = Some(root);
        server
    }

    /// Serve a filesystem-backed root with a background scanner on a fast
    /// cycle (`@cold-listing` scenarios): the scanner re-scans the tree
    /// every `interval`, so out-of-band changes surface promptly. The
    /// in-process data plane does not wire a scanner (the config-driven
    /// `serve` path does), so this constructor runs the real [`Scanner`]
    /// next to the plane on the same shutdown channel.
    pub async fn fs_with_scanner_interval(caps: Capabilities, interval: Duration) -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let scanner = Scanner::new(
            storage.clone(),
            ScannerOptions {
                enabled: true,
                delay: Duration::from_millis(1),
                max_wait: Duration::from_millis(10),
                cycle: interval,
                access: AccessMode::ReadWrite,
            },
        );
        Self::spawn_with(storage, caps.into(), Some(root), Some(scanner)).await
    }

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

    /// Bind the data plane — plus an optional background scanner on a
    /// cloned shutdown receiver (it aborts on shutdown) — and spawn its
    /// accept loop. `plane` carries both the capability toggles and the
    /// write policy, so a caller opts into read-only mode by handing in
    /// [`PlaneConfig::read_only`].
    async fn spawn_with<S: Storage>(
        storage: S,
        plane: PlaneConfig,
        root: Option<TempDir>,
        scanner: Option<Scanner>,
    ) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        if let Some(scanner) = scanner {
            tokio::spawn(scanner.run(rx.clone()));
        }
        let data_plane = DataPlane::new(storage, plane);
        tokio::spawn(async move {
            data_plane.serve(listener, rx).await.unwrap();
        });
        let served = root.as_ref().map(|r| r.path().to_path_buf());
        Self {
            addr,
            _root: root,
            _home: None,
            served,
            shutdown,
        }
    }

    /// Bind the identity-wired acl plane (`DataPlane::new_with_acl` —
    /// ConfigAuth + AclAccess, the @acl-scenario server) over the given
    /// identity: the same bind/serve/shutdown shape as `spawn_with`,
    /// which itself stays untouched so the legacy 12 feature files keep
    /// running unsigned.
    async fn spawn_with_acl<S: Storage>(
        storage: S,
        caps: Capabilities,
        root: Option<TempDir>,
        identity: Arc<Identity>,
    ) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = watch::channel(false);
        let plane = DataPlane::new_with_acl(storage, caps, identity);
        tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });
        let served = root.as_ref().map(|r| r.path().to_path_buf());
        Self {
            addr,
            _root: root,
            _home: None,
            served,
            shutdown,
        }
    }

    /// Serve a fresh filesystem-backed root on the identity-wired acl
    /// plane (the @acl scenarios' plain backend).
    pub async fn acl(caps: Capabilities) -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        Self::spawn_with_acl(storage, caps, Some(root), acl_fixture_identity()).await
    }

    /// Serve the in-memory reference backend on the identity-wired acl
    /// plane (the @acl @mem scenarios).
    pub async fn acl_mem(caps: Capabilities) -> Self {
        Self::spawn_with_acl(
            MemoryStorage::new().unwrap(),
            caps,
            None,
            acl_fixture_identity(),
        )
        .await
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The served root (fs backends only): the tempdir for [`Server::fs`]
    /// and [`Server::fs_with_scanner_interval`], the nested `base/root`
    /// for [`Server::fs_nested`].
    pub fn root(&self) -> Option<&Path> {
        self.served.as_deref()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

/// A raw HTTP response: status, headers (lower-cased names), body bytes.
#[derive(Debug, Clone, Default)]
pub struct LastResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl LastResponse {
    /// The first value of `name` (case-insensitive; the stored names
    /// are lower-cased at parse).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Raw HTTP client used by every step (one per scenario): bound to the
/// scenario's server by the `#[before]` hook, then every request goes
/// through [`Client::request`] (the ported `request` helper over the
/// bound address).
#[derive(Debug, Clone, Default)]
pub struct Client {
    addr: Option<SocketAddr>,
}

impl Client {
    /// Bind to the scenario's server.
    pub fn bind(&mut self, addr: SocketAddr) {
        self.addr = Some(addr);
    }

    /// One raw HTTP request on a fresh connection; see [`request`].
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> LastResponse {
        request(
            self.addr.expect("client bound to a server"),
            method,
            path,
            headers,
            body,
        )
        .await
    }

    /// One raw HTTP request signed (SigV4) as the configured fixture
    /// user (`access_key`, `secret`): the signer's three headers
    /// (`x-amz-date`, `x-amz-content-sha256`, `authorization`) are
    /// merged into `headers` and the request goes through
    /// [`Client::request`] unchanged — the step wrappers' only auth
    /// path (anonymous stays driver-level unsigned).
    pub async fn signed_request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        user: (&str, &str),
    ) -> LastResponse {
        let addr = self.addr.expect("client bound to a server");
        let signer = Signer::new(user.0, user.1);
        let extra = signer.sign(method, path, headers, body, &addr.to_string());
        let mut signed_headers: Vec<(&str, &str)> = Vec::with_capacity(headers.len() + extra.len());
        for (name, value) in &extra {
            signed_headers.push((name, value));
        }
        signed_headers.extend_from_slice(headers);
        request(addr, method, path, &signed_headers, body).await
    }
}

/// One HTTP/1.1 request on a fresh connection; the response is read to
/// EOF (`Connection: close`) and de-chunked when needed.
///
/// Retry policy — the connect stage only, up to 2 retries ~50 ms apart,
/// on `ConnectionAborted` / `ConnectionRefused` (the Windows loopback
/// accept race: with scenarios running in parallel, each binding its own
/// ephemeral-port listener, the stack sometimes aborts a connection
/// before the server accepts it — the flake root the CI `--retry 1`
/// papers over). Two manifestations are covered:
///
/// - `TcpStream::connect` itself fails, or
/// - the first write (the request head) fails after zero bytes were
///   transmitted — the connection died in the accept queue; the server
///   never received a byte, so the request cannot have executed.
///
/// Once ANY byte has been sent, no retry happens: the server may already
/// have started executing the request (e.g. a PUT that wrote the object),
/// and a retry could double-apply a non-idempotent write.
pub async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> LastResponse {
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if !headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-length"))
    {
        head += &format!("Content-Length: {}\r\n", body.len());
    }
    for (k, v) in headers {
        head += &format!("{k}: {v}\r\n");
    }
    head += "\r\n";
    let retryable = |e: &io::Error| {
        matches!(
            e.kind(),
            ErrorKind::ConnectionAborted | ErrorKind::ConnectionRefused
        )
    };
    let mut attempt = 0;
    loop {
        // Phase 1 — connect + transmit the request head. Both may abort
        // before any byte reaches the server; both are retried (see the
        // policy above).
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) if retryable(&e) && attempt < 2 => {
                attempt += 1;
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => panic!("cannot connect to {addr}: {e}"),
        };
        match stream.write(head.as_bytes()).await {
            Ok(n) if n == head.len() => {}
            Ok(n) => {
                // A partial head (rare): finish it. Any failure from here
                // on is a genuine post-transmission failure — never
                // retried.
                stream
                    .write_all(&head.as_bytes()[n..])
                    .await
                    .expect("finish the request head");
            }
            Err(e) if retryable(&e) && attempt < 2 => {
                // Zero bytes were sent: the connection died in the accept
                // queue and the request never reached the server — the
                // same safety class as a connect failure.
                attempt += 1;
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => panic!("cannot send request to {addr}: {e}"),
        }
        // Phase 2 — body and response. The body write is never retried:
        // the head went out, so the server may already have executed the
        // request. The response read can still catch the same accept-queue
        // abort: if the connection dies BEFORE any response byte arrives
        // the server never executed the request (it was still queued), so
        // retrying is safe. A read that aborts mid-stream (partial bytes
        // already received) is never retried — the request ran.
        stream.write_all(body).await.expect("send the request body");
        let mut raw = Vec::new();
        match stream.read_to_end(&mut raw).await {
            Ok(_) => return parse_response(&raw),
            Err(e) if raw.is_empty() && retryable(&e) && attempt < 2 => {
                attempt += 1;
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => panic!("read the response from {addr}: {e}"),
        }
    }
}

/// A PUT whose body is cut off mid-stream: the headers declare
/// `declared_len` bytes, only `partial` are sent, then the connection is
/// dropped — an interrupted upload.
pub async fn abort_mid_upload(addr: SocketAddr, path: &str, declared_len: usize, partial: &[u8]) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head =
        format!("PUT {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {declared_len}\r\n\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(partial).await.unwrap();
    stream.shutdown().await.unwrap();
}

/// Poll `cond` (50 ms steps, ~5 s budget) until it holds — for effects
/// that land asynchronously after a connection drops.
pub async fn eventually(cond: impl FnMut() -> bool) -> bool {
    let mut cond = cond;
    for _ in 0..100 {
        if cond() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    cond()
}

/// The text between the first `open`/`close` tag pair — empty when the
/// opener (or the closer) is absent, so a missing tag never yields a
/// garbage slice of the body.
pub fn extract(text: &str, open: &str, close: &str) -> String {
    let Some(start) = text.find(open).map(|i| i + open.len()) else {
        return String::new();
    };
    let Some(end) = text[start..].find(close).map(|i| start + i) else {
        return String::new();
    };
    text[start..end].to_string()
}

/// Plant a file at `rel`, relative to the served root, creating its
/// parents — the out-of-band fixture body shared by this module's `the
/// served root holds …` step and `reserved_paths`' `I write … in the
/// served root` step (their Gherkin sentences differ, the write does not).
pub(super) async fn plant_in_root(world: &super::World, rel: impl AsRef<Path>, body: &str) {
    let root = world
        .server
        .as_ref()
        .and_then(|server| server.root())
        .expect("a fixture step needs a running fs-backed server");
    let path = root.join(rel);
    fs::create_dir_all(path.parent().expect("a relative path has a parent"))
        .await
        .unwrap();
    fs::write(&path, body.as_bytes()).await.unwrap();
}

/// Plant a bucket directory and one object directly on disk (FR-023). The
/// read-only scenarios cannot build their fixture over the wire: every
/// mutating request — bucket create and object upload included — answers
/// `AccessDenied`.
#[given(expr = "the served root holds bucket {string} with object {string} and body {string}")]
async fn served_root_holds(world: &mut super::World, bucket: String, key: String, body: String) {
    plant_in_root(world, Path::new(&bucket).join(&key), &body).await;
}

/// A served root that **refuses new entries** for as long as this guard lives; the original mode
/// comes back on drop. Unix only: the plant is a permission bit (`chmod 0500` — owner read and
/// traverse, nobody may write), and a platform without mode bits has no equivalent (design §10).
///
/// **The guard is the step's, not the fixture's**, because the root is the harness's `TempDir`:
/// a directory at 0500 refuses `remove_dir_all`, so the mode has to be given back **before** the
/// server that owns it drops — which is why [`super::after_hook`] takes this field first.
#[cfg(unix)]
#[derive(Debug)]
pub struct RestrictedRoot {
    path: PathBuf,
    mode: u32,
}

#[cfg(unix)]
impl RestrictedRoot {
    /// `chmod 0500` `path` and remember the mode to give back.
    fn restrict(path: &Path) -> Self {
        use std::{
            fs::{Permissions, metadata, set_permissions},
            os::unix::fs::PermissionsExt as _,
        };

        let mode = metadata(path)
            .unwrap_or_else(|err| panic!("the served root `{}`: {err}", path.display()))
            .permissions()
            .mode();
        set_permissions(path, Permissions::from_mode(0o500))
            .unwrap_or_else(|err| panic!("chmod 0500 `{}`: {err}", path.display()));
        Self {
            path: path.to_path_buf(),
            mode,
        }
    }
}

#[cfg(unix)]
impl Drop for RestrictedRoot {
    fn drop(&mut self) {
        use std::{
            fs::{Permissions, set_permissions},
            os::unix::fs::PermissionsExt as _,
        };

        let _ = set_permissions(&self.path, Permissions::from_mode(self.mode));
    }
}

/// The served-root half of a genuinely read-only root: the root is made unwritable **after** the
/// scenario planted its objects, so every read the scenario drives is served off a tree that
/// cannot be written at all.
///
/// **What the wire cannot show is the point**: a plane that wrote the root — a state directory,
/// an object, anything — would fail on the request that drove it, where the portable half
/// (`the root holds exactly what it held before`) only counts the entries. The scenario carrying
/// this step is `@unix`, and the definition exists only there: an undefined step is a failure
/// under `fail_on_skipped()`, so it must not be reachable on a host whose mode bits do not exist.
#[cfg(unix)]
#[given("the served root refuses new entries")]
async fn served_root_refuses_new_entries(world: &mut super::World) {
    let root = world
        .server
        .as_ref()
        .and_then(|server| server.root())
        .expect("a fixture step needs a running fs-backed server")
        .to_path_buf();
    world.restricted = Some(RestrictedRoot::restrict(&root));
}

/// Sweep the confined grandchild a killed launcher leaves behind, by the scratch
/// root its own argv names.
///
/// **The precondition is the caller's and it is not optional.** Seatbelt has no
/// job object — a `SIGKILL`ed launcher leaves its confined child running, an
/// accepted gap the launcher's own module records
/// (`crates/tinio-cli/src/sandbox/macos.rs`) — and this harness holds no handle to
/// that grandchild. The pattern is the scenario's own root, which is unique to
/// the scenario and is named in the child's argv, so it reaches that one process
/// and nothing else. **A scenario that confined nothing must not sweep**: this is
/// a `pkill -f` over the whole process table, and a pattern no process of this
/// scenario's ever carried would be matching someone else's — hence
/// `Cli::confined_spawned`'s gate at the call site, where the fact is known.
///
/// **There is no non-macOS spelling, and that is why this is `cfg`'d rather than
/// stubbed.** Every other platform needs no sweep at all, because the process a
/// kill ends *is* the server; a no-op here would let a call site read as if it
/// swept.
///
/// **The rule is [`tinio_util::testing::sweep_confined_grandchild`]'s, re-exported
/// rather than copied.** The server crate's e2e module needs the same sweep, and a
/// `tests/` tree is not importable from another one — a crate both dev-depend on
/// is, which is what removes the copy this module used to keep.
#[cfg(target_os = "macos")]
pub use tinio_util::testing::sweep_confined_grandchild;

/// The generic response assertions — one home, so a new feature does not
/// have to hunt for them in a feature-specific step module (the wire-XML
/// scenarios of tagging/listing/conditions use the body-contains form).

#[given(expr = "the response status is {int}")]
#[then(expr = "the response status is {int}")]
async fn status_is(world: &mut super::World, status: u16) {
    assert_eq!(world.last.status, status, "status mismatch");
}

/// The last response body contains `text` (a wire-XML fragment).
#[then(expr = "the response body contains {string}")]
async fn body_contains(world: &mut super::World, text: String) {
    let body = String::from_utf8_lossy(&world.last.body);
    assert!(body.contains(&text), "body missing {text:?}: {body}");
}

/// The last response body does not contain `text` — e.g. a quiet-mode
/// DeleteObjects response carries neither `<Deleted>` entries nor errors.
#[then(expr = "the response body does not contain {string}")]
async fn body_omits(world: &mut super::World, text: String) {
    let body = String::from_utf8_lossy(&world.last.body);
    assert!(
        !body.contains(&text),
        "body unexpectedly contains {text:?}: {body}"
    );
}

/// The last response body is exactly `text` (the nested-root scenario's
/// "non-reserved objects of the inner root are served normally"). The
/// `object body is` phrase (objects/multipart/conditions/reserved_paths
/// features) is the same exact-equality assertion under the features'
/// wording.
#[then(expr = "the response body is {string}")]
#[then(expr = "the object body is {string}")]
async fn body_is(world: &mut super::World, text: String) {
    assert_eq!(world.last.body, text.as_bytes(), "response body mismatch");
}

/// The case-insensitive containment rule the `… ignoring case` step pair
/// shares: a client renders a header or a field as it read it, not as the
/// contract spells it (`mc`'s `ETag`/`Etag`; aws cli's
/// `X-Amz-Delete-Marker`), so both the stdout and the stderr assertion fold
/// case before comparing.
pub(super) fn contains_ignore_case(haystack: &str, needle: &str) -> bool {
    haystack.to_lowercase().contains(&needle.to_lowercase())
}

/// How many times `tag` (a wire-XML open tag like `<Key>`) occurs in the
/// response body — the "shows N keys/parts/uploads" assertions' one home
/// (a folder marker is never a key, so `<Key>` occurrences are counted,
/// not parsed).
pub(super) fn count_tag(body: &[u8], tag: &str) -> usize {
    String::from_utf8_lossy(body).matches(tag).count()
}

/// The sorted top-level entry names of `dir` — shared by the
/// traversal-proof and root-shape assertions.
pub(super) async fn sorted_entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut entries = fs::read_dir(dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    names
}

/// Feed one line of a child's event stream into `frame`, returning the event when the line closes
/// a frame, and clearing `frame` either way.
///
/// **A frame is not a line** (design decision 19): `Event::encode` writes an `event:` line, a
/// `data:` line and the blank line that closes them, so a decoder fed one line at a time sees an
/// event name with no payload and drops it; accumulating to the blank line is what makes that
/// line the frame's end rather than a line to skip. Text that is not one of our frames is dropped
/// rather than fatal, which is what makes a stray write to stdout harmless.
///
/// The line's own terminator is not significant: it is trimmed and re-emitted as `\n`, so a
/// reader holding a pipe's raw CRLF and one holding an already trimmed line build the same frame
/// text, which `sse-core` dispatches alike.
///
/// **The one home in this crate**: `steps/cli.rs`'s `frames` reads a whole captured stream with it
/// and `steps/clients.rs`'s readiness reader reads a live pipe with it, so a frame means the same
/// thing on both — a captured file and a stream still being written.
/// (`crates/tinio-server/tests/common/mod.rs`'s `frame_line` is the same rule for the legs that
/// live beside that crate, whose `tests/` tree this one cannot import.)
pub fn frame_line(frame: &mut String, line: &str) -> Option<Event> {
    if line.trim_end().is_empty() {
        let event = Event::decode(frame);
        frame.clear();
        return event;
    }
    frame.push_str(line.trim_end());
    frame.push('\n');
    None
}

/// Split a raw HTTP response into status, headers, and (de-chunked) body.
fn parse_response(raw: &[u8]) -> LastResponse {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .expect("status code");
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_lowercase(), v.trim().to_string()))
        })
        .collect();
    let mut body = raw[split + 4..].to_vec();
    let chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.to_lowercase().contains("chunked"));
    if chunked {
        body = dechunk(&body);
    }
    LastResponse {
        status: code,
        headers,
        body,
    }
}

/// Reassemble a chunked transfer-coded body (trailers ignored).
fn dechunk(mut rest: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let pos = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("chunk size line");
        let size_text = str::from_utf8(&rest[..pos]).unwrap();
        let size = usize::from_str_radix(size_text.trim(), 16).expect("chunk size");
        rest = &rest[pos + 2..];
        if size == 0 {
            break;
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
    out
}
