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

// The pg backend's configuration values — `TINIO_PG_DSN`, the secret pair the backend's `Config`
// takes (PG5), the crate's own constructor types and the reset connection. Only the `pg`
// feature's constructors below reach them.
#[cfg(feature = "tier")]
use std::fmt;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    str,
    sync::Arc,
    time::Duration,
};

use cucumber::{gherkin::Step, given, then};
use md5::{Digest, Md5};
#[cfg(feature = "pg")]
use sqlx::PgConnection;
use tempfile::TempDir;
#[cfg(feature = "tier")]
use tinio_lifecycle::{
    Options as LifecycleOptions, Runner, Summary as LifecycleSummary, SystemClock,
};
#[cfg(feature = "tier")]
use tinio_pg::_store_pg::pool::Pool;
#[cfg(feature = "pg")]
use tinio_pg::_store_pg::test_support::{base_dsn, truncate_all};
#[cfg(feature = "pg")]
use tinio_pg::{Access, Config as PgConfig, PgStorage};
#[cfg(feature = "tier")]
use tinio_tier::{
    ColdStore, Config as TierConfig, FlushMode, Index, Mover, TierStorage, VolumeCapacity,
    index_path,
};
#[cfg(feature = "pg")]
use tinio_util::secret::{Secret, SecretKey};
use tinio_util::testing::{http::exchange, sigv4::Signer};
#[cfg(feature = "pg")]
use tokio::sync::{OnceCell, Semaphore, SemaphorePermit};
use tokio::{
    fs,
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::sleep,
};

use super::cli::docstring;
pub use crate::_fs::testing::fs_options;
use crate::{
    _cli::config::Config,
    _core::{mode::AccessMode, storage::Storage},
    _fs::{FsOptions, FsStorage, Scanner, ScannerOptions},
    _mem::MemoryStorage,
    _server::{
        Capabilities, DataPlane, Identity, PlaneConfig,
        identity::{IdentityFromConfig, auth, owner, users},
    },
};

/// Repeatable body of length `n`: `(i * 31 + 7) % 256` at position `i`.
/// The ONE byte generator the steps share (F14): the upload-vs-verify
/// pairing relies on the part and object uploads producing identical
/// bytes, so a copy that drifts would silently break the pairing.
pub fn deterministic_bytes(n: u64) -> Vec<u8> {
    (0..n).map(|i| ((i * 31 + 7) % 256) as u8).collect()
}

/// The standard-alphabet, padded base64 of `bytes` — what a `Content-MD5`
/// header carries (RFC 4648 §4). Written out rather than adding `base64`
/// to this crate for one header on the XML writes (the lifecycle and
/// Object Lock documents share it).
pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let tail = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from(tail[0]) << 16 | u32::from(tail[1]) << 8 | u32::from(tail[2]);
        for i in 0..4u32 {
            out.push(if (i as usize) < chunk.len() + 1 {
                ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char
            } else {
                '='
            });
        }
    }
    out
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

/// `PUT path` with the step's docstring as the XML body and its own
/// `Content-MD5` — the one write shape the Object Lock and lifecycle XML
/// documents share.
pub async fn put_xml(world: &mut super::World, path: String, step: &Step) {
    let body = docstring(step);
    let digest = base64(&Md5::digest(body.as_bytes()));
    let headers = [
        ("Content-Type", "application/xml"),
        ("Content-MD5", digest.as_str()),
    ];
    world.last = world
        .client
        .request("PUT", &path, &headers, body.as_bytes())
        .await;
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
/// ones. Built from the schema's own section types (the way `clients.rs`'s
/// `config_for` builds its capability file), so a renamed key is a compile
/// error here rather than a document the run reads as an unset default.
fn acl_fixture_config() -> Config {
    let config = Config {
        auth: Some(auth::Config {
            access_key: ACL_ROOT.0.to_owned(),
            secret_key: auth::Secret::from(auth::SecretKey::from(ACL_ROOT.1.to_owned())),
        }),
        owner: Some(owner::Config {
            canonical_id: ACL_ROOT_ID.to_owned(),
            display_name: "ops".to_owned(),
            ..Default::default()
        }),
        users: Some(vec![users::Config {
            access_key: ACL_USER_B.0.to_owned(),
            secret_key: auth::Secret::from(auth::SecretKey::from(ACL_USER_B.1.to_owned())),
            canonical_id: Some(ACL_USER_B_ID.to_owned()),
            display_name: Some("bob".to_owned()),
            local_uid: None,
        }]),
        ..Default::default()
    };
    let toml = toml::to_string(&config).expect("the acl fixture config serializes");
    Config::parse(&toml).expect("the acl fixture config parses")
}

/// The @acl identity map assembled from the fixture config
/// ([`IdentityFromConfig`] — the config-driven path, never a direct
/// `Identity` construction).
pub fn acl_fixture_identity() -> Arc<Identity> {
    Identity::from_config(&acl_fixture_config().identity())
}

pub use super::backend::Backend;

/// The tier layer's write shape for a scenario: `@batch` exercises the buffered
/// write path, every other tier scenario the synchronous one.
#[cfg(feature = "tier")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TierKind {
    /// Every write reaches the filesystem before it is acknowledged.
    #[default]
    Sync,
    /// A plain small write waits in the buffer until the flush step drains it.
    Batch,
    /// The background idle pass's cadence (`@tier-idle`): the synchronous write path plus a
    /// three-second `demote_idle` and a 100 ms `scan_interval`, so a scenario can watch the
    /// product's own pass demote an object it never reads. The threshold leaves a loaded host
    /// more margin than the poll needs to observe it.
    Idle,
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
    /// The exclusive permit the shared pg database is served under (PG8/PG17),
    /// held for the server's lifetime and released when it drops — never read,
    /// because holding it IS the guarantee that no two pg scenarios reset and
    /// run at once.
    #[cfg(feature = "pg")]
    _pg_permit: Option<SemaphorePermit<'static>>,
    served: Option<PathBuf>,
    shutdown: watch::Sender<bool>,
    /// The serve loop's handle. [`Server::stop`] awaits it, so a tier restart can reopen the
    /// root — the index's file lock is exclusive — after every task that holds the storage has
    /// ended.
    serve_task: Option<JoinHandle<()>>,
    /// The tier layer's storage and mover (T25): the steps drive the moves and passes directly,
    /// and the background task owns the same two handles.
    #[cfg(feature = "tier")]
    tier: Option<Tier>,
    /// The tier background tasks' handle — the mover, flush and lifecycle loops (T25).
    #[cfg(feature = "tier")]
    tier_task: Option<JoinHandle<()>>,
}

/// The tier layer's two handles a scenario's steps drive: the storage (moves, collection, the
/// dirty-buffer drain) and the mover (the capacity-and-heat pass).
#[cfg(feature = "tier")]
pub struct Tier {
    storage: TierStorage<ColdStore>,
    mover: Arc<Mover<ColdStore>>,
}

#[cfg(feature = "tier")]
impl fmt::Debug for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tier").finish_non_exhaustive()
    }
}

#[cfg(feature = "tier")]
impl Tier {
    /// The layer the wire requests are served from.
    pub fn storage(&self) -> &TierStorage<ColdStore> {
        &self.storage
    }

    /// The mover the pass steps and the background loop share.
    pub fn mover(&self) -> &Mover<ColdStore> {
        &self.mover
    }
}

/// The one shared pg backend, constructed on the first `@pg` scenario and reused by every later
/// one. **Constructed once, never per scenario**: `PgStorage::connect` (writable) takes the
/// ownership advisory lock and holds it for the process's life (PG12), so a second construction
/// inside this process would be refused — the shared value, reset per scenario, is the shape
/// that lock forces (PG8's "one shared database, one scenario at a time").
#[cfg(feature = "pg")]
static PG_STORAGE: OnceCell<PgStorage> = OnceCell::const_new();

/// The dsn the pg pass runs against — `TINIO_PG_DSN`, its only source, so no credential is
/// spelled in this file (PG25).
///
/// **The variable's name and its normalization are the store harness's**, not a third spelling
/// of both: this pass already enables `tinio-store-pg`'s `testing` through `tinio-pg`, so
/// `base_dsn` is the same reader the store's own DB-backed legs and `tinio-pg`'s test targets
/// use. What is local is the *failure*: a `@pg` scenario cannot skip, so a missing variable is
/// this pass's panic rather than a leg's early return.
#[cfg(feature = "pg")]
pub(super) fn pg_dsn() -> String {
    base_dsn()
        .expect("the pg pass needs TINIO_PG_DSN (the compose fixture's dsn, never written here)")
}

/// The shared pg backend, migrated on first use (`PgStorage::connect` runs the migrations).
#[cfg(feature = "pg")]
async fn pg_storage() -> &'static PgStorage {
    PG_STORAGE
        .get_or_init(|| async {
            let config = PgConfig::new(Secret::from(SecretKey::from(pg_dsn())), 8);
            PgStorage::connect(&config, Access::ReadWrite)
                .await
                .expect("construct the pg backend against TINIO_PG_DSN")
        })
        .await
}

/// The one permit the shared pg database is served under (see [`pg_storage_reset`]).
#[cfg(feature = "pg")]
static PG_EXCLUSIVE: Semaphore = Semaphore::const_new(1);

/// The exclusive permit alone — for the tier pass, whose cold plane is constructed by the layer's
/// `testing` constructor rather than by [`pg_storage`], but which shares the one database and so
/// the same serialization (T25: "the shared pg permit/reset").
#[cfg(feature = "pg")]
pub(super) async fn pg_permit() -> SemaphorePermit<'static> {
    PG_EXCLUSIVE
        .acquire()
        .await
        .expect("the shared pg database's exclusive permit")
}

/// The shared pg backend, this scenario's scratch home, and the exclusive permit they are served
/// under — the body [`Server::pg`] and [`Server::acl_pg`] share, so the reset discipline is
/// written once.
///
/// **The permit is what makes the shared database safe at any concurrency.** The pass is correct
/// only one scenario at a time: one scenario's reset would truncate another's live rows, and the
/// CI invocation's `--concurrency 1` is not something the harness can check (PG8). So the permit
/// is taken here, next to the reset, and held until the server drops. A hand-run, or any future
/// concurrency change, therefore serializes the pg scenarios instead of failing them with no
/// cause in the message.
#[cfg(feature = "pg")]
async fn pg_storage_reset() -> (PgStorage, TempDir, SemaphorePermit<'static>) {
    let permit = pg_permit().await;
    let storage = pg_storage().await.clone();
    reset_pg_database().await;
    (storage, tempfile::tempdir().unwrap(), permit)
}

/// Reset the shared database for one scenario (PG17): truncate every table the schema owns except
/// the migration bookkeeping, so the next scenario starts on an empty database while the schema
/// stays. The storage is neither re-migrated nor re-constructed (its ownership lock forbids that —
/// see [`PG_STORAGE`]).
///
/// A fresh connection, not the storage's pool: the pool belongs to `PgStorage`, whose raw-SQL
/// surface is private to the store crate, and one connection per reset is what a serialized pass
/// costs. The statement itself is `tinio-store-pg`'s ([`truncate_all`]): the store owns which
/// tables the schema has and which one is the migration bookkeeping, so a migration that renames
/// or adds a table cannot leave rows behind for a harness that spelled the names itself.
#[cfg(feature = "pg")]
pub(super) async fn reset_pg_database() {
    use sqlx::Connection as _;

    let mut conn = PgConnection::connect(&pg_dsn())
        .await
        .expect("connect to TINIO_PG_DSN to reset the database for the next scenario");
    truncate_all(&mut conn)
        .await
        .expect("truncate the schema for the next scenario");
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

    /// Serve the shared PostgreSQL backend, its database reset for this scenario (PG17).
    #[cfg(feature = "pg")]
    pub async fn pg(caps: Capabilities) -> Self {
        Self::pg_server(caps, false).await
    }

    /// Serve the shared PostgreSQL backend on the identity-wired acl plane (the `@acl`
    /// scenarios), reset for this scenario exactly as [`Server::pg`] does.
    #[cfg(feature = "pg")]
    pub async fn acl_pg(caps: Capabilities) -> Self {
        Self::pg_server(caps, true).await
    }

    /// The shared pg server [`Server::pg`] and [`Server::acl_pg`] differ only by the plane over:
    /// one database reset, one scratch home, one permit, whatever the plane.
    ///
    /// The backend is constructed **once per process** ([`pg_storage`]) and the per-scenario
    /// reset is the truncate in [`reset_pg_database`] — the plan's "truncate + re-migrate +
    /// construct" is a truncate over one construct-once storage, because a writable
    /// `PgStorage::connect` takes the ownership advisory lock and holds it for the process's
    /// life (PG12), so a second construction inside this process is refused. The reset leaves
    /// the schema (and its `schema_version` row) in place, so the storage is already migrated;
    /// "construct" is the fresh [`DataPlane`] this spawns over a clone of that one storage. The
    /// exclusive permit [`pg_storage_reset`] takes is what makes the shared database safe at any
    /// concurrency (PG8), rather than the pass's `--concurrency` flag.
    ///
    /// The scenario also gets a scratch state dir of its own — the pg backend serves no root,
    /// but a scenario's temp state is kept alive for the server's lifetime, [`Server::read_only`]'s
    /// shape.
    #[cfg(feature = "pg")]
    async fn pg_server(caps: Capabilities, acl: bool) -> Self {
        let (storage, home, permit) = pg_storage_reset().await;
        let plane = if acl {
            DataPlane::new_with_acl(storage, caps, acl_fixture_identity())
        } else {
            DataPlane::new(storage, PlaneConfig::from(caps))
        };
        let mut server = Self::spawn_plane(plane, None, None).await;
        server._home = Some(home);
        server._pg_permit = Some(permit);
        server
    }

    /// Serve the tier layer over a fresh temp root, the shared pg database reset for this
    /// scenario (T25).
    #[cfg(feature = "tier")]
    pub async fn tier(caps: Capabilities, kind: TierKind) -> Self {
        Self::tier_server(
            caps,
            kind,
            false,
            false,
            TierRoot::Owned(fresh_root()),
            true,
            pg_permit().await,
        )
        .await
    }

    /// Serve the tier layer on the identity-wired acl plane (the `@acl @tier` scenarios), reset
    /// exactly as [`Server::tier`] is.
    #[cfg(feature = "tier")]
    pub async fn acl_tier(caps: Capabilities, kind: TierKind) -> Self {
        Self::tier_server(
            caps,
            kind,
            true,
            false,
            TierRoot::Owned(fresh_root()),
            true,
            pg_permit().await,
        )
        .await
    }

    /// Reopen a tier root a previous scenario server served: the restart and read-only legs. The
    /// database is **not** reset — the rows the first server wrote are the fixture — and the
    /// permit is the one the caller still holds from that server, so no other scenario can run
    /// (and truncate) between the two.
    #[cfg(feature = "tier")]
    pub async fn tier_on(root: &Path, read_only: bool, permit: SemaphorePermit<'static>) -> Self {
        Self::tier_server(
            Capabilities::default(),
            TierKind::Sync,
            false,
            read_only,
            TierRoot::Existing(root.to_path_buf()),
            false,
            permit,
        )
        .await
    }

    /// The tier arm's whole construction, in one body (T25).
    ///
    /// **`from_parts` instead of `open_pg`** is the spec's testing constructor: it skips the pg
    /// ownership lock and the root-identity check, which a per-scenario harness cannot satisfy —
    /// a writable `PgStorage` holds the ownership lock for the process's life (PG12), so a second
    /// construction would be refused, and every scenario would re-bind the reserved namespace.
    /// The shared pg database is therefore served under the [`pg_permit`] permit — passed in by
    /// the caller, so a reopen can move the one it already holds — and reset by
    /// [`reset_pg_database`] exactly like the `@pg` pass; the layer's tasks run over the one
    /// storage the plane serves.
    ///
    /// The writable run starts the mover, flush, GC and lifecycle loops; a read-only run starts
    /// none and opens the index, the cold plane and the filesystem read-only (T27).
    #[cfg(feature = "tier")]
    async fn tier_server(
        caps: Capabilities,
        kind: TierKind,
        acl: bool,
        read_only: bool,
        root: TierRoot,
        reset: bool,
        permit: SemaphorePermit<'static>,
    ) -> Self {
        let (root_guard, root_path) = match root {
            TierRoot::Owned(guard) => {
                let path = guard.path().to_path_buf();
                (Some(guard), path)
            }
            TierRoot::Existing(path) => (None, path),
        };
        let mut options = fs_options();
        if read_only {
            options.access = AccessMode::ReadOnly;
        }
        let fs = Arc::new(FsStorage::new(&root_path, options).unwrap());
        let index = if read_only {
            // T27: a read-only run creates no index. A root without one refuses here rather
            // than serving an empty placement map.
            Index::open_read_only(index_path(&root_path))
                .expect("a read-only tier run needs the root's index (T27)")
        } else {
            Index::open(index_path(&root_path)).expect("the tier index opens")
        };
        // The cold plane connects (and, writable, migrates) before the reset: the first scenario
        // of a pass runs against a schema the fixture already has, but the reset's truncate is
        // meaningful only once the tables exist.
        let cold = tier_cold_store(read_only).await;
        if reset {
            reset_pg_database().await;
        }
        let config = tier_config(kind);
        let storage = TierStorage::from_parts(fs, cold, Arc::new(index), config.clone());
        let mover = Arc::new(Mover::new(
            storage.clone(),
            Arc::new(VolumeCapacity::new(&root_path)),
            config.move_concurrency,
        ));
        let plane = if acl {
            DataPlane::new_with_acl(storage.clone(), caps, acl_fixture_identity())
        } else if read_only {
            DataPlane::new(storage.clone(), PlaneConfig::read_only(caps))
        } else {
            DataPlane::new(storage.clone(), PlaneConfig::from(caps))
        };
        let mut server = Self::spawn_plane(plane, root_guard, None).await;
        server.served = Some(root_path);
        if !read_only {
            let stop = server.shutdown.subscribe();
            server.tier_task = Some(tokio::spawn(tier_background(
                storage.clone(),
                Arc::clone(&mover),
                config,
                stop,
            )));
        }
        server.tier = Some(Tier { storage, mover });
        server._pg_permit = Some(permit);
        server
    }

    /// The tier layer this server runs, for the `@tier` steps (T25).
    #[cfg(feature = "tier")]
    pub fn tier_handle(&self) -> &Tier {
        self.tier.as_ref().expect("a tier scenario's server")
    }

    /// Run one lifecycle pass now, with no cadence wait — the transition scenario's deterministic
    /// driver. The background loop still runs on its own cadence, as the product does.
    #[cfg(feature = "tier")]
    pub async fn lifecycle_pass(&self) -> LifecycleSummary {
        let (_stop, stopped) = watch::channel(false);
        let runner = Runner::new(
            Arc::new(self.tier_handle().storage().clone()),
            LifecycleOptions {
                delay: Duration::ZERO,
                cycle: Duration::ZERO,
                concurrency: 1,
            },
            Arc::new(SystemClock),
            stopped,
        );
        runner.pass().await
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
                reconcile_objects: true,
            },
        );
        Self::spawn_with(storage, caps.into(), Some(root), Some(scanner)).await
    }

    /// Serve a fresh filesystem root in read-only mode (FR-023), with the
    /// tool state in a scratch directory outside the root.
    ///
    /// The tier corpus does not take this constructor: its read-only leg
    /// reopens a seeded tier root (`Server::tier_on`), and the hook refuses the
    /// `@tier @read-only` combination rather than serving a filesystem plane
    /// under a tier tag.
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
    /// accept loop. This owns the bind/watch/served bookkeeping; the
    /// callers below own which plane they hand in.
    async fn spawn_plane(
        plane: DataPlane,
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
        let serve_task = tokio::spawn(async move {
            plane.serve(listener, rx).await.unwrap();
        });
        let served = root.as_ref().map(|r| r.path().to_path_buf());
        Self {
            addr,
            _root: root,
            _home: None,
            #[cfg(feature = "pg")]
            _pg_permit: None,
            served,
            shutdown,
            serve_task: Some(serve_task),
            #[cfg(feature = "tier")]
            tier: None,
            #[cfg(feature = "tier")]
            tier_task: None,
        }
    }

    /// The legacy no-identity plane over `storage`, on the capability and write policy `plane`
    /// carries, so a caller opts into read-only mode by handing in [`PlaneConfig::read_only`].
    async fn spawn_with<S: Storage>(
        storage: S,
        plane: PlaneConfig,
        root: Option<TempDir>,
        scanner: Option<Scanner>,
    ) -> Self {
        Self::spawn_plane(DataPlane::new(storage, plane), root, scanner).await
    }

    /// The identity-wired acl plane (`DataPlane::new_with_acl` — ConfigAuth + AclAccess, the
    /// @acl-scenario server) over `identity`, so the legacy feature files keep running unsigned.
    async fn spawn_with_acl<S: Storage>(
        storage: S,
        caps: Capabilities,
        root: Option<TempDir>,
        identity: Arc<Identity>,
    ) -> Self {
        Self::spawn_plane(DataPlane::new_with_acl(storage, caps, identity), root, None).await
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

    /// Stop the serve loop and every background task on the one shutdown channel, and wait for
    /// them. A tier restart needs this: the index's file lock is exclusive, so the reopened root
    /// must be free of every clone of the storage the first server held.
    pub async fn stop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.serve_task.take() {
            let _ = task.await;
        }
        #[cfg(feature = "tier")]
        if let Some(task) = self.tier_task.take() {
            let _ = task.await;
        }
    }

    /// Move the served root's guard out of the server — a scenario that reopens the same tree
    /// keeps it alive across both servers (the `@tier` restart and read-only legs).
    #[cfg(feature = "tier")]
    pub fn take_root(&mut self) -> Option<TempDir> {
        self._root.take()
    }

    /// Move the shared database's permit out of the server. A restart drops it before the
    /// reopened server acquires it: a semaphore permit is not reentrant, and the second
    /// construction would otherwise wait for a release that only its own drop provides.
    #[cfg(feature = "pg")]
    pub fn take_permit(&mut self) -> Option<SemaphorePermit<'static>> {
        self._pg_permit.take()
    }

    /// The served root (fs backends only): the tempdir for [`Server::fs`]
    /// and [`Server::fs_with_scanner_interval`], the nested `base/root`
    /// for [`Server::fs_nested`].
    pub fn root(&self) -> Option<&Path> {
        self.served.as_deref()
    }

    /// The scenario's tag-driven facts — the backend, the fs variant, the capability set and
    /// whether the scenario asked for the identity-wired plane — as the running server.
    ///
    /// **One match, because the two axes cross**: `@acl` is independent of the backend axis, so
    /// a `@pg` acl scenario is [`Server::acl_pg`] and not a fourth branch. Each backend's own
    /// constructor stays the home of its setup; this only decides which one a scenario wants.
    /// The `@pg` arm is gated once: a build without the feature carries no `PgStorage` (PG15),
    /// so it fails loud here rather than silently serving another backend.
    pub async fn from_tags(
        backend: Backend,
        fs_kind: FsKind,
        caps: Capabilities,
        acl: bool,
        #[cfg(feature = "tier")] tier: TierKind,
    ) -> Self {
        match (backend, fs_kind, acl) {
            (Backend::Mem, _, true) => Self::acl_mem(caps).await,
            (Backend::Mem, _, false) => Self::mem(caps).await,
            (Backend::Fs, _, true) => Self::acl(caps).await,
            (Backend::Fs, FsKind::NestedRoot, false) => Self::fs_nested(caps).await,
            (Backend::Fs, FsKind::ColdListing(interval), false) => {
                Self::fs_with_scanner_interval(caps, interval).await
            }
            (Backend::Fs, FsKind::Plain, false) => Self::fs(caps).await,
            #[cfg(feature = "pg")]
            (Backend::Pg, _, true) => Self::acl_pg(caps).await,
            #[cfg(feature = "pg")]
            (Backend::Pg, _, false) => Self::pg(caps).await,
            #[cfg(not(feature = "pg"))]
            (Backend::Pg, _, _) => panic!("a @pg scenario needs a build with --features pg"),
            #[cfg(feature = "tier")]
            (Backend::Tier, _, true) => Self::acl_tier(caps, tier).await,
            #[cfg(feature = "tier")]
            (Backend::Tier, _, false) => Self::tier(caps, tier).await,
        }
    }
}

/// Where a tier server's root comes from: a fresh tempdir the server owns, or a tree a previous
/// server served and the scenario keeps alive across the restart.
#[cfg(feature = "tier")]
enum TierRoot {
    /// A fresh root; the guard travels into the server.
    Owned(TempDir),
    /// The same tree a previous server served (the restart and read-only legs).
    Existing(PathBuf),
}

#[cfg(feature = "tier")]
fn fresh_root() -> TempDir {
    tempfile::tempdir().unwrap()
}

/// The tier layer's runtime configuration for one scenario.
///
/// **Small, fast limits and no hidden cadence work, except `@tier-idle`**: the scenarios drive
/// the passes themselves so an assertion never races a background loop; the one idle scenario
/// asks for the opposite (a three-second `demote_idle` and a 100 ms `scan_interval`) because the
/// cadence IS what it asserts. `@batch` alone switches the write path to the
/// buffer, whose flush the scenario triggers explicitly (`batch_interval` therefore waits far
/// beyond the scenario's life). The watermarks sit above any volume this host fills, so the
/// capacity pass never demotes behind a scenario's back; `dwell` is zero so a hot key is
/// promotable at once; `gc_grace` is zero so the collection step can finish a demotion
/// immediately.
#[cfg(feature = "tier")]
fn tier_config(kind: TierKind) -> TierConfig {
    // `@tier-idle` is the one scenario class that must meet the mover's cadence: every other
    // configuration parks `demote_idle` and `scan_interval` far beyond the scenario's life, so
    // only an object a step demotes changes plane.
    let (flush_mode, batch_interval, demote_idle, scan_interval) = match kind {
        TierKind::Sync => (
            FlushMode::Sync,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(30),
        ),
        TierKind::Batch => (
            FlushMode::Batch,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(30),
        ),
        TierKind::Idle => (
            FlushMode::Sync,
            Duration::from_secs(3600),
            Duration::from_secs(3),
            Duration::from_millis(100),
        ),
    };
    TierConfig {
        capacity_bytes: 256 * 1024 * 1024,
        max_object_bytes: 1024 * 1024,
        flush_mode,
        batch_bytes: 8 * 1024 * 1024,
        batch_interval,
        heat_window: Duration::from_secs(3600),
        promote_reads: 2,
        stats_flush: Duration::from_secs(3600),
        high_water_percent: 99,
        low_water_percent: 98,
        demote_idle,
        dwell: Duration::ZERO,
        scan_interval,
        move_concurrency: 1,
        index_compact_percent: 30,
        gc_grace: Duration::ZERO,
    }
}

/// The cold plane over the shared database — **without** the ownership lock [`tinio_tier::open_pg`]
/// would take (T25's "testing constructor that skips ownership and the root check").
#[cfg(feature = "tier")]
async fn tier_cold_store(read_only: bool) -> ColdStore {
    let access = if read_only {
        Access::ReadOnly
    } else {
        Access::ReadWrite
    };
    let pool = if read_only {
        Pool::open_read_only(&pg_dsn(), 4)
    } else {
        Pool::open(&pg_dsn(), 4)
    }
    .expect("the tier cold plane's pool opens against TINIO_PG_DSN");
    ColdStore::connect_with_pool(pool, access)
        .await
        .expect("the tier cold plane connects to TINIO_PG_DSN")
}

/// The tier arm's background future: the mover pass + GC, the buffer flush and the lifecycle
/// pass, all joined on the serving half's one stop channel (T25).
#[cfg(feature = "tier")]
async fn tier_background(
    storage: TierStorage<ColdStore>,
    mover: Arc<Mover<ColdStore>>,
    config: TierConfig,
    stop: watch::Receiver<bool>,
) {
    if let Err(err) = mover.bind().await {
        eprintln!("the tier mover could not rebuild its warm table: {err}");
    }
    let passes = tier_passes(Arc::clone(&mover), config.scan_interval, stop.clone());
    let flush = tier_flush(storage.clone(), config.batch_interval, stop.clone());
    let lifecycle = tier_lifecycle(storage.clone(), stop);
    let (_, _, _) = tokio::join!(passes, flush, lifecycle);
    if let Err(err) = storage.shutdown().await {
        eprintln!("the tier buffer could not drain at shutdown: {err}");
    }
}

/// The mover's loop: one capacity-and-heat pass and the collection cadence every `interval`.
#[cfg(feature = "tier")]
async fn tier_passes(
    mover: Arc<Mover<ColdStore>>,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if !tier_wait(interval, &mut stop).await {
            return;
        }
        if let Err(err) = mover.pass().await {
            eprintln!("the tier mover's pass failed; the next pass retries: {err}");
        }
        if let Err(err) = mover.gc().await {
            eprintln!("the tier GC pass failed; its cadence retries it: {err}");
        }
    }
}

/// The buffer's loop: drain the batch buffer every `interval`.
#[cfg(feature = "tier")]
async fn tier_flush(
    storage: TierStorage<ColdStore>,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if !tier_wait(interval, &mut stop).await {
            return;
        }
        if let Err(err) = storage.shutdown().await {
            eprintln!("the tier buffer could not flush: {err}");
        }
    }
}

/// The lifecycle pass's loop: the shared [`Runner`] over the tier storage, woken every cycle.
///
/// **The first pass waits a cycle**, unlike [`Runner::run`]: a listing flushes the batch buffer
/// (T8), so a pass that ran at construction would publish a `@batch` scenario's write from under
/// its own assertion. The scenario drives the transitions it asserts through
/// [`Server::lifecycle_pass`]; this loop is the product's background half, started and stopped by
/// the server like the mover's.
#[cfg(feature = "tier")]
async fn tier_lifecycle(storage: TierStorage<ColdStore>, mut stop: watch::Receiver<bool>) {
    let runner = Runner::new(
        Arc::new(storage),
        LifecycleOptions {
            delay: Duration::ZERO,
            cycle: Duration::ZERO,
            concurrency: 1,
        },
        Arc::new(SystemClock),
        stop.clone(),
    );
    loop {
        if !tier_wait(Duration::from_secs(30), &mut stop).await {
            return;
        }
        runner.pass().await;
    }
}

/// Sleep `interval` unless the stop channel fires first; `false` once the run is stopping.
#[cfg(feature = "tier")]
async fn tier_wait(interval: Duration, stop: &mut watch::Receiver<bool>) -> bool {
    if *stop.borrow() {
        return false;
    }
    tokio::select! {
        _ = sleep(interval) => true,
        changed = stop.changed() => changed.is_ok() && !*stop.borrow(),
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

/// A raw HTTP response, the framing's own type ([`tinio_util::testing::http`]):
/// status, headers (lower-cased names), body bytes.
pub use tinio_util::testing::http::Response as LastResponse;

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

/// One HTTP/1.1 request on a fresh connection; the framing is
/// [`tinio_util::testing::http::exchange`]'s, shared with `tinio-cli`'s pg serving leg. The retry
/// budget is the Windows loopback accept race's (see that function).
pub async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> LastResponse {
    exchange(&addr.to_string(), addr, method, path, headers, body, 2).await
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
    eventually_within(cond, Duration::from_secs(5)).await
}

/// Poll `cond` (50 ms steps) until it holds or `budget` elapses — for an effect
/// whose own cadence sets the wait. The idle-demotion leg uses a wider budget
/// than the default five seconds, so a loaded host cannot flake between the
/// pass's threshold and the observation.
pub async fn eventually_within(mut cond: impl FnMut() -> bool, budget: Duration) -> bool {
    let steps = budget.as_millis().div_ceil(50) as usize;
    for _ in 0..steps {
        if cond() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    cond()
}

/// The text between the first `open`/`close` tag pair — empty when the
/// opener (or the closer) is absent, so a missing tag never yields a
/// garbage slice of the body. The rule is [`tinio_util::testing::extract`]'s,
/// re-exported rather than copied.
pub use tinio_util::testing::extract;

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
