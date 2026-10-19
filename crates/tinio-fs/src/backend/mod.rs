//! The `Storage` contract implementation over the local filesystem
//! (tasks T041/T042).
//!
//! [`FsStorage`] maps the contract onto the primitives of this crate:
//! `path` (mapping), `write` (atomic writes), `meta` (ETag store),
//! `buckets` (creation times), `listing` (walk + pagination), `multipart`
//! (parts storage). The operation groups live in `buckets.rs`
//! ([`BucketOps`]), `objects.rs` ([`ObjectOps`]), and `multipart.rs`
//! ([`MultipartOps`]).

use crate::_core::{acl::Acl, object::OBJECT_TAGS_MAX};
mod buckets;
mod multipart;
mod objects;
mod version;

// The version-reference epoch's atomics: only the versioning paths use them.
#[cfg(feature = "versioning")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::HashMap,
    fs::canonicalize,
    io::{self, ErrorKind},
    path::{Path, PathBuf, absolute},
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime},
};

// The content plane ([`crate::content`]) composes the same file-half
// primitives the object ops do: the removal tail and the bucket emptiness
// probe are re-exported because their modules are private to `backend`.
use async_trait::async_trait;
pub(crate) use buckets::bucket_is_empty;
use derive_more::Debug;
use garde::Validate;
use getset::{CopyGetters, Getters};
use lockmap::Map;
pub(crate) use objects::{StagedBytes, prune_parents, remove_object_file};
#[cfg(windows)]
use soft_canonicalize::soft_canonicalize;
use tokio::fs;
// The publish plan's own type: the feature-off build has the file-backed
// publish too, so the re-export is not gated (its module is).
pub(crate) use version::Publication;

#[cfg(feature = "versioning")]
use crate::_blob::{LocalBlocks, Store as BlobStore};
#[cfg(feature = "versioning")]
use crate::_core::version::State;
/// The staged body of this backend's `ObjectOps` implementation: the ops
/// engine's value (spec E8) over this crate's content-plane stage
/// (`content::FsStaged`) — re-exported so callers can name
/// `ObjectOps::StagedBody`
/// without depending on `tinio-ops` directly. This module's own staged value
/// (`objects::StagedBytes`) is the file layer's, crate-internal.
pub use crate::_ops::StagedBody;
#[cfg(feature = "versioning")]
use crate::_ops::VersionPlane;
#[cfg(feature = "versioning")]
use crate::_version::Versions;
pub use crate::error::Error;
#[cfg(feature = "versioning")]
use crate::error::projected;
pub(crate) use crate::error::{invalid_path, invalid_state_dir, invalid_value, root_not_directory};
#[cfg(feature = "versioning")]
use crate::version_adapter::Rows;
use crate::{
    _core::{
        ETag, acl,
        bucket::{Name, name},
        checksum, lifecycle,
        meta::BucketMeta,
        mode::AccessMode,
        object, pipeline,
        storage::{
            self, COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT,
            DEFAULT_MAX_CONCURRENT_UPLOADS, META_BATCH_BYTES_MAX, META_BATCH_BYTES_MIN,
            META_BATCH_SIZE_MAX, META_BATCH_SIZE_MIN, Storage, no_such_bucket,
        },
        to_nanos,
        version::Id,
    },
    _ops::{
        BucketOps as EngineBucketOps, BucketRow, Guard, Hooks, MetadataFailure,
        ObjectOps as EngineObjectOps,
    },
    _store_redb::{
        RedbMetadata, bucket, bucket_lifecycle, bucket_versioning, decode_acl_wire,
        decode_owner_wire, object_meta, object_part, upload, version as version_rows,
        version_index,
    },
    _util::{lockmap, path::contains_under_any_spelling},
    backend::version::stamp_superseded,
    bucket::Store as BucketStore,
    content::FsContent,
    database::{self, Driver, compact_if_needed},
    etag, fsutil,
    listing::FsListing,
    meta::Store as MetaStore,
    multipart::{CompletedPartRow, Store as MultipartStore, drain_bucket_uploads, drain_upload},
    path::{
        BLOBS_DIR_NAME, BoundaryCache, map_bucket_path, map_key_path, map_key_path_lexical,
        prove_key_contained, state_dir,
    },
    write::AtomicWriter,
};

/// Construction options of [`FsStorage`].
///
/// The three pipelines are **mandatory** (P4, pipeline-spec.md §7): the
/// cold list/scanner paths enqueue ETag-computation and batch meta-write
/// tasks into them, and delete enqueues tombstone removal — there is no
/// inline fallback. Offline contexts (doctor, benches, examples, unit
/// tests) pass [`pipeline::InlineRunner`] (tinio-core Q1); the server
/// passes its pipeline runtimes.
///
/// Each pipeline is typed to its task [`pipeline::Task::Output`] (P4/P7):
/// the IO pipeline accepts blocking ETag compute ([`etag::Result`]), the
/// removal pipeline accepts unit-success tombstone jobs
/// (`Result<(), Error>` — D-A, physically isolated from ETag compute),
/// and the DB pipeline accepts `write_task::MetaWriteBatchTask`
/// (`Result<(), Error>` — the original error, never boxed into
/// `RunOutput`).
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
///
/// use tinio_core::{
///     mode::AccessMode,
///     pipeline::InlineRunner,
///     storage::{
///         DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
///     },
/// };
/// use tinio_fs::FsOptions;
///
/// let options = FsOptions {
///     follow_symlinks: false, // default: reject symlinks
///     state_dir: None,
///     access: AccessMode::ReadWrite, // default: serve every operation
///     compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///     meta_batch_size: DEFAULT_META_BATCH_SIZE,
///     meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
///     owner_uids: std::collections::HashMap::new(),
///     io_pipeline: Arc::new(InlineRunner::default()),
///     remove_pipeline: Arc::new(InlineRunner::default()),
///     db_pipeline: Arc::new(InlineRunner::default()),
/// };
/// assert!(!options.follow_symlinks);
/// ```
#[derive(Clone, Debug, Validate)]
pub struct FsOptions {
    /// Follow symlinks in the storage root (default `false`: symlinks are
    /// rejected — access never resolves through a link, and link entries
    /// are excluded from listings — so a link inside a bucket cannot
    /// escape the storage root). Set `true` to follow them.
    #[garde(skip)]
    pub follow_symlinks: bool,
    /// State-dir override: where the private state lives. `None` (default)
    /// = `<root>/.tinio/`.
    ///
    /// **Read-only mode does not use this option** (FR-023 amended): the
    /// storage's one state dir there is the root's own `<root>/.tinio/`,
    /// opened read-only — the metadata source is the root's own database.
    /// The value a read-only caller carries is its own tool state (the
    /// CLI's relocated socket/`state`/log directory), which the storage
    /// never holds — and naming one **inside the root** is refused as it
    /// always was, because the tool state is what a run writes and the
    /// mode promises the root is never written.
    #[garde(skip)]
    pub state_dir: Option<PathBuf>,
    /// The plane's write policy (FR-023; see [`AccessMode`]). Read-only
    /// mode writes nothing through the storage: the root's own
    /// `<root>/.tinio/` database is read read-only, every mutating
    /// operation is refused upstream, and the scanner and sweeper are not
    /// run.
    #[garde(skip)]
    pub access: AccessMode,
    /// Compact trigger: the fragmentation percentage at which the state
    /// database is compacted at startup (`[storage.fs]
    /// compact_threshold_percent`, 5..=90).
    #[garde(
        range(
            min = COMPACT_THRESHOLD_MIN_PERCENT,
            max = COMPACT_THRESHOLD_MAX_PERCENT
        )
    )]
    pub compact_threshold_percent: u8,
    /// The meta-batch entry-count threshold (`[storage.fs]
    /// meta_batch_size`, 1..=4096): the scanner's producers flush one
    /// write-pipeline batch once it holds this many entries
    /// (pipeline-spec.md Q5; default from the task-2.5 benchmark, Q6). The
    /// listing path goes through the ops engine's own row batch instead and
    /// does not read this.
    #[garde(range(min = META_BATCH_SIZE_MIN, max = META_BATCH_SIZE_MAX))]
    pub meta_batch_size: u16,
    /// The meta-batch byte threshold (`[storage.fs] meta_batch_bytes`,
    /// 1024..=16 MiB): the scanner's producers flush once the estimated
    /// batch size (≈ 56 B + key length per entry) reaches this
    /// (pipeline-spec.md Q5).
    #[garde(range(min = META_BATCH_BYTES_MIN, max = META_BATCH_BYTES_MAX))]
    pub meta_batch_bytes: u32,
    /// The config-supplied `OwnerId → uid` map (unix ownership
    /// hardening, spec 2026-09-05 §5a): an object file lands chowned to
    /// its owner's mapped uid, and bucket/prefix dirs to the bucket
    /// owner's. An owner without an entry stays server-user-owned
    /// (still `0600`/`0700`). Plain injected data — tinio-fs takes no
    /// config or auth dependency; the config layer builds the
    /// map (Task 14). Inert on Windows (the creating user's ACLs
    /// isolate).
    #[garde(skip)]
    pub owner_uids: HashMap<acl::OwnerId, u32>,
    /// The IO pipeline (pipeline-spec.md §3.1): the cold scanner path
    /// enqueues `etag::ComputeTask` instances here. Mandatory
    /// (P4) — the pipeline (or `InlineRunner` in offline contexts) is a
    /// construction-time decision. Typed to [`etag::Result`] (P4/P7).
    #[garde(skip)]
    #[debug("<runner>")]
    pub io_pipeline: Arc<dyn pipeline::Runner<etag::Result>>,
    /// The removal pipeline (D-A, `[pipeline.remove]`): `delete_bucket`
    /// enqueues tombstone `remove_dir_all` here — physically isolated
    /// from ETag compute, so a large tree walk can never occupy the IO
    /// workers' capacity. Mandatory (P4). Typed to `Result<(), Error>`.
    #[garde(skip)]
    #[debug("<runner>")]
    pub remove_pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>,
    /// The batch meta-write pipeline (pipeline-spec.md §3.1): the
    /// producers enqueue `write_task::MetaWriteBatchTask` batches
    /// here. Mandatory (P4). Typed to the task's output
    /// `Result<(), Error>` (P4/P7).
    #[garde(skip)]
    #[debug("<runner>")]
    pub db_pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>,
}

/// The filesystem storage backend: buckets are top-level subdirectories of
/// the storage root, objects are files.
///
/// Must pass the `tinio-core` conformance harness (a test asserts it).
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
///
/// use tinio_core::{
///     bucket,
///     mode::AccessMode,
///     pipeline::InlineRunner,
///     storage::{
///         BucketOps, DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES,
///         DEFAULT_META_BATCH_SIZE, ObjectOps,
///     },
/// };
/// use tinio_fs::{FsOptions, FsStorage};
/// use tinio_util::testing::body;
/// use tokio::runtime::Runtime;
///
/// let root = tempfile::tempdir().unwrap();
/// let storage = FsStorage::new(
///     root.path(),
///     FsOptions {
///         follow_symlinks: false,
///         state_dir: None,
///         access: AccessMode::ReadWrite,
///         compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///         meta_batch_size: DEFAULT_META_BATCH_SIZE,
///         meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
///         owner_uids: std::collections::HashMap::new(),
///         io_pipeline: Arc::new(InlineRunner::default()),
///         remove_pipeline: Arc::new(InlineRunner::default()),
///         db_pipeline: Arc::new(InlineRunner::default()),
///     },
/// )
/// .unwrap();
/// let b = bucket::name("data").unwrap();
/// Runtime::new().unwrap().block_on(async {
///     storage
///         .create_bucket(&b, None, &tinio_core::acl::Acl::default_private(None))
///         .await
///         .unwrap();
///     storage
///         .put_object(&b, &"hello.txt".into(), body(b"hi"))
///         .await
///         .unwrap();
///     let head = storage.head_object(&b, &"hello.txt".into()).await.unwrap();
///     assert_eq!(head.size, 2);
/// });
/// ```
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct FsStorage {
    /// The canonical storage root (bucket dirs at the top level).
    #[getset(get = "pub fn root(&self) -> &Path")]
    root: PathBuf,
    /// The reserved state directory (`<root>/.tinio/` unless overridden).
    #[getset(get = "pub fn state_dir(&self) -> &Path")]
    state_dir: PathBuf,
    /// Whether symlinks are followed in the tree (see [`FsOptions`]).
    #[getset(get = "pub")]
    follow_symlinks: bool,
    /// The compact trigger threshold (see [`FsOptions`]).
    #[getset(get_copy = "pub(crate)")]
    compact_threshold_percent: u8,
    /// Bucket creation times (`BUCKETS` table).
    #[getset(get = "pub(crate)")]
    bucket_store: BucketStore,
    /// The ETag metadata store.
    #[getset(get = "pub(crate)")]
    meta_store: MetaStore,
    /// Multipart parts storage.
    #[getset(get = "pub(crate)")]
    multipart_store: MultipartStore,
    /// The shared state-database handle (the stores each hold a clone;
    /// kept here for cross-store single-transaction operations such as
    /// [`Self::remove_bucket_state`], and for the version adapter's tables).
    #[getset(get = "pub(crate)")]
    handle: Arc<Driver>,
    /// Atomic body writer (staging under `<state-dir>/tmp/`; the version
    /// adapter's materialize publishes through it).
    #[getset(get = "pub(crate)")]
    writer: AtomicWriter,
    /// The tree-walk listing (shared with the scanner).
    #[getset(get = "pub(crate)")]
    listing: FsListing,
    /// The config-supplied `OwnerId → uid` map (unix ownership
    /// hardening, spec 2026-09-05 §5a): object files land chowned to
    /// their owner's mapped uid, bucket/prefix dirs to the bucket
    /// owner's — an unmapped owner stays server-user-owned (still
    /// `0600`/`0700`). Inert on Windows.
    #[cfg_attr(
        not(unix),
        expect(
            dead_code,
            reason = "the field is inert on Windows — only the unix ownership hardening reads it, so off unix it is deliberately never read"
        )
    )]
    owner_uids: HashMap<acl::OwnerId, u32>,
    /// The removal pipeline (D-A): `delete_bucket` enqueues the tombstone
    /// `remove_dir_all` here — the tree walk is physically isolated from
    /// ETag compute on the IO pipeline.
    #[getset(skip)]
    #[debug("<runner>")]
    remove_pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>,
    /// Per-bucket directory-mutation locks (rename/create/remove). A
    /// `delete_bucket` of A cannot remove a just-written object of A
    /// (emptiness check and unpublish are one critical section against
    /// writes into A); mutations of B do not wait.
    #[getset(skip)]
    bucket_mutation_locks: lockmap::Map<Name>,
    /// Validated path boundaries (root + per-bucket), identity-checked
    /// and bounded — the containment proof of [`bucket_dir`](Self::bucket_dir)
    /// and [`key_path`](Self::key_path).
    #[getset(skip)]
    boundary_cache: BoundaryCache,
    /// The version-reference epoch: bumped by every commit that adds a
    /// version row (the one place a block reference enters the version
    /// tables) and read by the sweeper, which validates a walk's live set
    /// against it instead of re-reading every row. Monotonic and
    /// process-local; every bump and every read is sequenced by the
    /// bucket mutation lock the referencing commits run under.
    #[cfg(feature = "versioning")]
    #[getset(skip)]
    block_epoch: Arc<AtomicU64>,
    /// The group receivers' handles, built once on first use and shared by
    /// every clone of this storage ([`ReceiverHandles`]). The `Arc` around
    /// the `OnceLock` is what lets one operation's receiver construction be
    /// a pointer bump instead of a copy of every path and the owner map.
    #[getset(skip)]
    #[debug("<receivers>")]
    receivers: Arc<OnceLock<ReceiverHandles>>,
}

/// The handles the group receivers are built from, resolved once per storage
/// and shared by every clone of it.
///
/// Every group needs the same two things — a content plane over this storage
/// and the storage itself for the hooks — and the receivers are immutable, so
/// one build is observationally identical to the per-call one the extraction
/// introduced (which deep-copied the whole `FsStorage` for every operation;
/// `read_meta` pins that cost now).
///
/// Two shapes are deliberate. `storage` is a clone with its **own cache left
/// empty**: the content planes and the hooks each hold an `Arc<FsStorage>`,
/// so a cache field pointing back at the storage that filled it would be a
/// reference cycle, and that storage would never drop. And the clone is safe
/// because every field carrying *state* is shared through it — the database
/// handle, the three stores, the per-bucket mutation locks, the part locks,
/// the boundary cache, the block epoch and the pipelines are all `Arc`s
/// inside, so the receivers observe exactly the state the storage they were
/// built for does (the same property the per-call clone relied on).
#[derive(Debug)]
struct ReceiverHandles {
    /// The storage clone the content planes and the hooks hold.
    storage: Arc<FsStorage>,
    /// The non-owning content plane: the object group's, whose bodies run
    /// under the trait impl's own bucket-mutation hold
    /// ([`FsContent::for_engine`](crate::content::FsContent)).
    engine_content: Arc<FsContent>,
    /// The owning content plane: the bucket and multipart groups'.
    own_content: Arc<FsContent>,
}

/// The per-bucket mutation-lock wait above which [`FsStorage::lock_bucket_mutations`]
/// warns (D-E) — a second: delete/create/PUT-commit of the same name
/// should never hold the mutex that long in normal operation, so the
/// warn marks a stuck peer, not routine contention.
const MUTATION_LOCK_WARN_THRESHOLD: Duration = Duration::from_secs(1);

/// Canonicalize `path`, with `soft-canonicalize` standing in for **one**
/// failure: a process the platform refuses the volume-name lookup.
///
/// **The refusal this exists for is a property of the process and not of the
/// call.** A confined Windows process cannot name a volume in the DOS form —
/// that needs a device map an AppContainer token may not read — so
/// `std::fs::canonicalize` fails `ERROR_ACCESS_DENIED` on a path that opened
/// perfectly well one line earlier (the sandbox design's verified-evidence row,
/// measured 2026-09-21). `soft_canonicalize` resolves through
/// `read_link`/`symlink_metadata`, which the container does grant.
///
/// **The fallback's answer is accepted only when it names a directory**, which is
/// the judgement this function went looking for — the directory fact is what the
/// constructor asked for. Accepting any lexical answer would report a root that is
/// not there, and `resolve` reads the same fact one line later.
///
/// Every other failure is the platform canonicalizer's to report: a missing root
/// still answers `NotFound` from `canonicalize` itself, and a fallback reached
/// there would answer `Ok` for a path that is not on disk — which is what
/// `tests.rs`'s missing-root case pins.
///
/// **The remedy is Windows-only, and the `cfg` says so rather than a comment.**
/// The refusal it answers is the AppContainer's device map (`VOLUME_NAME_DOS`),
/// which is a Windows confinement fact — `soft-canonicalize` is not a better
/// canonicalizer here, it is one that resolves through calls that container
/// grants. Leaving the fallback armed on the other two platforms would be the
/// generic backend carrying one platform's remedy, and would silently change what
/// a `PermissionDenied` means there: on a unix host it is an unsearchable
/// ancestor, and a lexical answer for that path is a wrong answer rather than a
/// remedy.
#[cfg(windows)]
fn canonicalize_tolerating_a_denied_device_map(path: &Path) -> io::Result<PathBuf> {
    match canonicalize(path) {
        Err(err) if err.kind() == ErrorKind::PermissionDenied => match soft_canonicalize(path) {
            Ok(resolved) if resolved.is_dir() => Ok(resolved),
            _ => Err(err),
        },
        other => other,
    }
}

/// [`canonicalize`], on the platforms whose `PermissionDenied` has no device map
/// behind it.
#[cfg(not(windows))]
fn canonicalize_tolerating_a_denied_device_map(path: &Path) -> io::Result<PathBuf> {
    canonicalize(path)
}

impl FsStorage {
    /// Open (or create) the backend over `root` — the convenience path:
    /// open the state database, evaluate and run compaction while the
    /// handle is still exclusive, then construct the stores over the
    /// shared handle (meta-redb-spec §5.9, G1).
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the root does not exist or cannot be
    /// canonicalized; [`Error::InvalidValue`] when `options` fail
    /// validation; [`Error::RootNotDirectory`] when the root is not a
    /// directory.
    pub fn new(root: impl Into<PathBuf>, options: FsOptions) -> Result<Self, Error> {
        let (canonical, state_dir, options) = Self::resolve(root.into(), options)?;
        let handle = if options.access.is_read_only() {
            Self::read_only_source(&state_dir)?
        } else {
            // Compact strictly before sharing: `Database::compact` needs
            // `&mut`, and once wrapped in `Arc<Driver>` the mutable
            // reference is structurally impossible.
            let database::Open {
                mut db,
                compact_needed,
                stats,
                ..
            } = database::open(&state_dir)?;
            // The outcome is reported, not discarded: an operator seeing
            // repeated `Compacted`/`Unchanged` rounds can tune the threshold.
            let compaction = compact_if_needed(
                &mut db,
                compact_needed,
                stats,
                options.compact_threshold_percent,
            )?;
            tracing::info!(?compaction, "startup state-database compaction");
            Driver::new(db)
        };
        Self::from_resolved(canonical, state_dir, options, handle)
    }

    /// Construct the backend over `root` from an **already-opened** state
    /// database — the orchestration path (G1): `database::open` → (compact
    /// with the exclusive handle) → `new_from_db`. The database handle is
    /// wrapped once and shared by every store. The server startup and
    /// doctor orchestration (T068/T073/T074) use this path explicitly.
    ///
    /// **In read-only mode the passed handle is not the storage's metadata
    /// source** (FR-023 amended): a read-only storage reads the root's own
    /// database, so the caller's database is dropped here (releasing its
    /// lock) and the root's own is opened read-only under `resolve`'s
    /// rule — the same call [`FsStorage::new`] makes, so the two paths
    /// cannot disagree. That is what keeps `doctor`, which opens its store
    /// at the **tool-state** dir and hands it here, from answering the
    /// metadata legs out of a database that is not the root's (or from
    /// leaving a second database behind for this path to use).
    ///
    /// A read-write caller keeps its handle, exactly as before.
    pub fn new_from_db(
        root: impl Into<PathBuf>,
        options: FsOptions,
        db: redb::Database,
    ) -> Result<Self, Error> {
        let (canonical, state_dir, options) = Self::resolve(root.into(), options)?;
        if options.access.is_read_only() {
            drop(db);
            let handle = Self::read_only_source(&state_dir)?;
            return Self::from_resolved(canonical, state_dir, options, handle);
        }
        Self::from_resolved(canonical, state_dir, options, Driver::new(db))
    }

    /// The read-only run's metadata source: the root's own database, opened
    /// read-only, or the degraded empty view when that fails — the reason
    /// is reported (`tracing::warn!`, the one user-visible carrier of the
    /// degrade). The **one** construction of the read-only arm, shared by
    /// [`FsStorage::new`] and [`FsStorage::new_from_db`], so no path can
    /// read a database the other would not.
    fn read_only_source(state_dir: &Path) -> Result<Arc<Driver>, Error> {
        match Driver::open_read_only(state_dir) {
            Ok(handle) => Ok(handle),
            Err(err) => {
                // A failure — no database, a corrupt one, a format this
                // build refuses, or a read-write server holding the same
                // root — is the **degrade path**, not a construction
                // failure: objects keep serving from their files with an
                // empty metadata view.
                tracing::warn!(
                    error = %err,
                    state_dir = %state_dir.display(),
                    "the root's metadata database could not be read read-only; serving objects with an empty metadata view"
                );
                Ok(Driver::empty()?)
            }
        }
    }

    /// Hold `name`'s directory-mutation lock so a concurrent write's
    /// phase-2 commit into this bucket blocks until the guard is dropped.
    /// The scanner's orphan reclamation and the cleanup stale-bucket
    /// pruning hold it across their probe + remove (F02/F05 — a fresh
    /// row or a recreated bucket's state can never be destroyed by a
    /// stale probe); tests use it to retarget a followed bucket symlink
    /// between resolve and rename.
    ///
    /// A wait past [`MUTATION_LOCK_WARN_THRESHOLD`] is warned (D-E): a
    /// delete/create/commit of the same name is holding the mutex — long
    /// enough to see in the logs, never silently absorbed.
    /// The version-reference epoch (the field's doc explains it). Read it
    /// under the same bucket mutation lock the referencing commits run in,
    /// then compare it after the walk: equal means no reference landed in
    /// between.
    #[cfg(feature = "versioning")]
    pub(crate) fn block_epoch(&self) -> u64 {
        self.block_epoch.load(Ordering::SeqCst)
    }

    /// Record that a version row was committed — every reference-adding
    /// path calls this inside its bucket mutation lock, after the row's
    /// transaction committed.
    #[cfg(feature = "versioning")]
    pub(crate) fn bump_block_epoch(&self) {
        self.block_epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) async fn lock_bucket_mutations(&self, name: &Name) -> lockmap::Guard<Name> {
        let started = Instant::now();
        let guard = self.bucket_mutation_locks.lock(name.clone()).await;
        let waited = started.elapsed();
        if waited > MUTATION_LOCK_WARN_THRESHOLD {
            tracing::warn!(
                bucket = %name,
                waited_ms = waited.as_millis() as u64,
                "bucket mutation lock wait exceeded the warn threshold"
            );
        }
        guard
    }

    /// The removal lane (D-A) — the tombstone `remove_dir_all` runner.
    /// The cleanup stages and the scanner enqueue through it instead of
    /// clearing trees inline on their own paths.
    pub(crate) fn remove_pipeline(&self) -> Arc<dyn pipeline::Runner<Result<(), Error>>> {
        Arc::clone(&self.remove_pipeline)
    }

    /// The orphaned-block-subtree proof the sweep's reclamation and the
    /// doctor's residue check share — one home for the rule and its lock.
    ///
    /// **What makes a subtree provably orphaned**: the `blobs/<name>` entry
    /// is a directory and the bucket *directory* `<root>/<name>` is absent,
    /// proven under that name's mutation lock. The directory is the
    /// backend's own existence authority (`ensure_bucket` reads it, never
    /// the `BUCKETS` row), so a live bucket whose `BUCKETS` row was lost is
    /// still recoverable — `create_bucket` re-derives the row, and its
    /// history still needs these blocks — and is never reclaimed. The lock
    /// is the discipline the block pass already uses: `create_bucket` and
    /// `remove_bucket_state` hold the same per-name lock, so a bucket being
    /// created or deleted cannot interleave between the absence proof and
    /// the answer. An entry that is not a directory — an out-of-band *file*
    /// wearing a bucket's name — answers `false`. A name that is not a
    /// legal bucket name can never be created as a bucket, so nothing can
    /// race for it and no lock can be taken for it (`create_bucket`
    /// validates first).
    ///
    /// `reclaim` keeps the removal inside the same critical section the
    /// proof was taken under (the sweep's arm: the guard lives until the
    /// tree is gone); the doctor asks with `reclaim = false` and only
    /// reports, leaving the bytes inspectable until the sweep clears them.
    #[cfg(feature = "versioning")]
    pub(crate) async fn orphaned_block_subtree(
        &self,
        dir: &Path,
        dir_name: &str,
        reclaim: bool,
    ) -> Result<bool, Error> {
        if !fs::metadata(dir)
            .await
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
        {
            return Ok(false);
        }
        let named = name(dir_name).ok();
        let _guard = match &named {
            Some(named) => Some(self.lock_bucket_mutations(named).await),
            None => None,
        };
        if !fsutil::is_absent(&self.root().join(dir_name)).await? {
            return Ok(false);
        }
        if reclaim {
            fsutil::remove_tree(dir).await?;
        }
        Ok(true)
    }

    fn resolve(root: PathBuf, options: FsOptions) -> Result<(PathBuf, PathBuf, FsOptions), Error> {
        options.validate().map_err(invalid_value)?;
        // The helper accepts its fallback only for a directory, which is the one
        // fact this function went looking for; the check below is what reports a
        // root that is not there, fallback or not.
        let canonical = canonicalize_tolerating_a_denied_device_map(&root)?;
        if !canonical.is_dir() {
            return Err(root_not_directory(canonical));
        }
        // **Read-only mode has exactly one metadata source: the root's own
        // `<root>/.tinio`** (FR-023 amended, design 2026-10-03 **Read
        // paths**). The rows a read-write run wrote live there — objects,
        // tags, ACLs, CORS, the version index — so that is what a read-only
        // run reads, opened read-only, and nothing is created: the open's
        // failure is the degrade path (`FsStorage::new` serves objects from
        // their files with an empty metadata view).
        //
        // The configured `state_dir` is deliberately **not** used in this
        // mode: it is the caller's tool state (the CLI's socket, `state`
        // file and logs, resolved by `resolve_state_dir`), the storage
        // never holds it, and a read-only run writes nothing through the
        // storage at all (every mutating operation is refused upstream,
        // and the scanner and the sweeper are not run).
        //
        // **It is still refused inside the root**, as it always was: a
        // caller that names an in-root state dir is pointing the run's
        // tool state — the files a run *writes* — into the very tree the
        // mode promises to leave alone, and a writable root would never
        // complain. The rule itself is `tinio-util`'s
        // `contains_under_any_spelling` (the one definer, shared with the
        // server launcher's exposure set and the config layer's config
        // write), and it compares both spellings of either side — a
        // second copy of a case-folding rule is how the other owner ends
        // up comparing components literally, an in-root state dir in
        // another case reading as outside (FR-023, decision 17a).
        if options.access.is_read_only()
            && let Some(dir) = options.state_dir.as_deref()
        {
            let absolute = absolute(dir)
                .map_err(|_| invalid_state_dir("could not be resolved in read-only mode"))?;
            if contains_under_any_spelling(&absolute, &canonical) {
                return Err(invalid_state_dir(
                    "must be outside the storage root in read-only mode",
                ));
            }
        }
        // One tail, two rules: a read-only run's storage dir is always the
        // root's own (the configured one is ignored by design); otherwise
        // the configured dir wins and the root's own is the fallback.
        let state_dir = if options.access.is_read_only() {
            state_dir(&canonical)?
        } else {
            match options.state_dir {
                Some(ref dir) => dir.clone(),
                None => state_dir(&canonical)?,
            }
        };
        Ok((canonical, state_dir, options))
    }

    fn from_resolved(
        canonical: PathBuf,
        state_dir: PathBuf,
        options: FsOptions,
        handle: Arc<Driver>,
    ) -> Result<Self, Error> {
        let FsOptions {
            follow_symlinks,
            state_dir: _,
            access: _,
            compact_threshold_percent,
            meta_batch_size,
            meta_batch_bytes,
            owner_uids,
            io_pipeline,
            remove_pipeline,
            db_pipeline,
        } = options;

        Ok(Self {
            follow_symlinks,
            compact_threshold_percent,
            listing: FsListing::new(
                &canonical,
                follow_symlinks,
                io_pipeline,
                db_pipeline,
                meta_batch_size,
                meta_batch_bytes,
            ),
            owner_uids,
            remove_pipeline,
            bucket_store: BucketStore::from_handle(handle.clone()),
            meta_store: MetaStore::from_handle(handle.clone()),
            multipart_store: MultipartStore::from_handle(
                handle.clone(),
                &state_dir,
                DEFAULT_MAX_CONCURRENT_UPLOADS,
            ),
            handle,
            writer: AtomicWriter::new(&state_dir),
            bucket_mutation_locks: Map::new(),
            boundary_cache: BoundaryCache::new(),
            receivers: Arc::new(OnceLock::new()),
            #[cfg(feature = "versioning")]
            block_epoch: Arc::new(AtomicU64::new(0)),
            root: canonical,
            state_dir,
        })
    }

    /// Set the cap on concurrently in-progress multipart uploads
    /// (`[s3] max_concurrent_uploads`; the store default is
    /// [`DEFAULT_MAX_CONCURRENT_UPLOADS`]). Must be called before serving
    /// requests; a `create_multipart_upload` above the cap answers
    /// `TooManyMultipartUploads` (mapped to S3 `SlowDown`).
    pub fn set_max_concurrent_uploads(&mut self, max: u32) {
        self.multipart_store.set_max_concurrent_uploads(max);
    }

    /// The mapped uid of an owner element (unix ownership hardening):
    /// the chown target of a landed object. `None` when the owner is
    /// absent or has no configured uid — the file stays
    /// server-user-owned (still `0600`).
    #[cfg(unix)]
    fn owner_uid(&self, owner: Option<&acl::OwnerId>) -> Option<u32> {
        owner.and_then(|o| self.owner_uids.get(o)).copied()
    }

    /// The mapped uid of the bucket's recorded owner (unix ownership
    /// hardening): the chown target of an implicitly created bucket/
    /// prefix directory (a non-bucket-owner's PUT creating prefix dirs
    /// must not leave server-user-owned dirs). `None` when the bucket
    /// row has no owner or the owner has no configured uid — the dir
    /// stays server-user-owned (still `0700`).
    #[cfg(unix)]
    pub(crate) async fn bucket_owner_uid(&self, name: &Name) -> Option<u32> {
        self.bucket_store
            .owner(name)
            .await
            .ok()
            .flatten()
            .and_then(|o| self.owner_uids.get(&o))
            .copied()
    }

    /// The bucket directory `<root>/<bucket>`.
    ///
    /// With `follow_symlinks` enabled a symlinked/junction bucket
    /// directory is **resolved to its canonical target** — the bucket is
    /// the target (a legit way to place a bucket on another volume), and
    /// every proof and walk addresses the same resolved path. With
    /// following disabled the containment proof refuses the link (the
    /// bucket is invisible; callers map that to `NoSuchBucket`).
    /// **Async (item 7a)**: the symlink probe and canonicalize run
    /// through `tokio::fs` — no sync syscalls on the request threads
    /// (the old per-object-op `std::fs` pair is gone).
    pub(crate) async fn bucket_dir(&self, name: &Name) -> Result<PathBuf, Error> {
        if self.follow_symlinks {
            let lexical = self.root().join(&**name);
            match fs::symlink_metadata(&lexical).await {
                Ok(metadata) if fsutil::is_symlink_or_reparse(&metadata) => {
                    return fs::canonicalize(&lexical).await.map_err(|err| {
                        if err.kind() == ErrorKind::NotFound {
                            // A dangling bucket link has no target — no
                            // bucket.
                            Error::Storage(no_such_bucket(name))
                        } else {
                            err.into()
                        }
                    });
                }
                // A plain directory: the normal containment-proven path
                // below.
                Ok(_) => {}
                // Missing (a link swap is a two-step operation on
                // Windows — remove + recreate): the bucket does not
                // exist at this instant — NoSuchBucket, never a 500 from
                // the lexical-probe fallback.
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    return Err(no_such_bucket(name).into());
                }
                Err(err) => return Err(err.into()),
            }
        }
        map_bucket_path(&self.boundary_cache, self.root(), name).await
    }

    /// The object file path `<bucket>/<key>` — the cached form of
    /// [`path::key_path`](crate::path::key_path). `enforce_boundary` is
    /// `!follow_symlinks` for object operations; cleanup/scan paths always
    /// enforce (they must never address outside the bucket). Async
    /// (item 7a) — the boundary resolution runs off the request threads.
    pub(crate) async fn key_path(
        &self,
        bucket_dir: &Path,
        key: &object::Key,
        enforce_boundary: bool,
    ) -> Result<PathBuf, Error> {
        map_key_path(&self.boundary_cache, bucket_dir, key, enforce_boundary).await
    }

    /// Resolve `key` under `bucket_dir`: the pure lexical validation
    /// first (no filesystem access), then the symlink policy, then the
    /// containment proof.
    ///
    /// The lexical validation (defensive re-check, the reserved `.tinio`
    /// refusal — FR-020, both follow modes — and the Windows charset
    /// refusal) runs before any syscall, so a refused key never pays for
    /// the symlink walk (P5). The I/O-time policy fires **before** the
    /// proof: the proof would canonicalize through a link inside the
    /// bucket and report an escape (`InvalidPath` → 400), where the
    /// documented contract (s3-surface.md) answers `AccessDenied` (403)
    /// for a link when following is disabled. With following enabled the
    /// lexical-validated join is returned (the policy is off and the
    /// proof is skipped — the current object-op contract).
    pub(crate) async fn resolve_key(
        &self,
        bucket_dir: &Path,
        key: &object::Key,
    ) -> Result<PathBuf, Error> {
        // 1. Pure lexical validation — no syscalls: the defensive
        //    re-check, the reserved-segment refusal, the Windows
        //    charset/aliasing refusal, and the plain join (the path the
        //    symlink walk inspects). A key refused here never pays for
        //    the walk or the proof.
        let path = map_key_path_lexical(bucket_dir, key)?;
        if self.follow_symlinks {
            return Ok(path);
        }
        // 2. The I/O-time symlink policy: lstat every existing component
        //    (missing components are skipped — the parents may not exist
        //    yet).
        self.check_symlinks(key, &path).await?;
        // 3. The containment proof (canonicalize) — after the policy so
        //    a link inside the bucket answers AccessDenied (403), not
        //    InvalidPath (400) — s3-surface.md. Async (item 7a): the
        //    boundary resolution runs off the request threads.
        prove_key_contained(&self.boundary_cache, bucket_dir, key).await?;
        Ok(path)
    }

    /// The block-store root of this backend (`<state-dir>/blobs/`): one
    /// subtree per bucket, holding the content-addressed blocks of that
    /// bucket's superseded versions. The directory is created lazily by the
    /// first stage, like every other state-dir subtree.
    #[cfg(feature = "versioning")]
    pub(crate) fn blobs_dir(&self) -> PathBuf {
        self.state_dir.join(BLOBS_DIR_NAME)
    }

    /// The version state machine over `bucket`'s block store: the backend's
    /// contracts ([the row adapter](Rows), the content plane
    /// [`FsContent`](crate::content::FsContent)) and the shared blob engine over
    /// [`LocalBlocks`] rooted at this bucket's own subtree — so a bucket
    /// deletion drops its blocks with the rest of its state, and no block
    /// outside the bucket is ever referenced.
    #[cfg(feature = "versioning")]
    pub(crate) fn versions(&self, bucket: &Name) -> Versions<Rows, FsContent, LocalBlocks> {
        let storage = self.storage_handle();
        let rows = Rows::new(Arc::clone(&storage));
        Versions::new(
            rows.clone(),
            rows,
            FsContent::for_engine(storage),
            BlobStore::new(LocalBlocks::new(self.blobs_dir().join(bucket.as_ref()))),
        )
    }

    /// The buckets that record versions — state enabled or suspended — in
    /// name order: the sweeper's retention/GC pass and the startup block
    /// verify both iterate exactly these, from one home.
    #[cfg(feature = "versioning")]
    pub(crate) async fn versioned_buckets(&self) -> Result<Vec<Name>, Error> {
        let mut out = Vec::new();
        for name in self.bucket_names("").await? {
            let state = self.versions(&name).state(&name).await.map_err(projected)?;
            if matches!(state, State::Enabled | State::Suspended) {
                out.push(name);
            }
        }
        Ok(out)
    }

    /// Every bucket of the root matching `prefix`: top-level directories
    /// with valid names (the reserved `.tinio` state dir excluded), in
    /// name order. The scanner, cleanup, and `list_buckets` share this
    /// walk — one source of truth for what a bucket is. The prefix
    /// filter, UTF-8 validity, and name validity all run on the bare
    /// `file_name()` BEFORE any stat — only prefix-matching candidate
    /// names pay the entry classification, and a plain directory pays
    /// nothing at all (the free dirent type decides; the bucket-level
    /// analogue of the object walk's subtree pruning; `""` = no filter).
    ///
    /// A symlinked/junction bucket directory is a bucket only when
    /// `follow_symlinks` is enabled (it resolves to its target — the
    /// bucket *is* the target); with following disabled it is invisible.
    ///
    /// The collected form of the streaming [`Self::for_each_bucket_name`]
    /// — the scanner and cleanup need the full list; `list_buckets`
    /// feeds the stream into the bounded pagination engine instead.
    pub(crate) async fn bucket_names(&self, prefix: &str) -> Result<Vec<Name>, Error> {
        let mut out = Vec::new();
        self.for_each_bucket_name(prefix, |name| out.push(name))
            .await?;
        out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(out)
    }

    /// Visit every bucket of the root matching `prefix`, one at a time —
    /// the streaming form of [`Self::bucket_names`] (the same name
    /// checks, the same follow policy; one source of truth for what a
    /// bucket is). The `list_buckets` producer feeds the visitor into
    /// the bounded pagination engine, so a page costs O(page) memory —
    /// no full collection, no sort.
    async fn for_each_bucket_name(
        &self,
        prefix: &str,
        mut visit: impl FnMut(Name),
    ) -> Result<(), Error> {
        let mut entries = fs::read_dir(self.root()).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let Some(raw) = file_name.to_str() else {
                continue; // non-UTF8 names cannot be bucket names
            };
            // No I/O and no allocation before this point: a non-matching
            // name is dropped on the borrowed `&str`.
            if !raw.starts_with(prefix) {
                continue;
            }
            let Ok(name) = name(raw) else {
                continue; // invalid names (incl. `.tinio`) are not buckets
            };
            if self.entry_is_bucket_dir(&entry).await {
                visit(name);
            }
        }
        Ok(())
    }

    /// Whether a root entry is a bucket directory under the follow policy
    /// (the caller has already filtered the name). A plain directory is a
    /// bucket with NO stat — the entry type is free on both platforms
    /// (unix `d_type`, Windows `FindNextFile` data) — and a root-level
    /// file is not a bucket with no stat either; only a symlinked/junction
    /// entry pays, and only when following is enabled (its resolved
    /// target must be a directory; a broken link is skipped). The
    /// follow-policy classification itself lives in
    /// [`fsutil::dir_entry_kind`] (A1 — the same policy the object walk
    /// uses).
    async fn entry_is_bucket_dir(&self, entry: &fs::DirEntry) -> bool {
        match fsutil::dir_entry_kind(entry, self.follow_symlinks).await {
            Ok(kind) => kind.is_dir,
            Err(_) => false, // vanished mid-sweep — skipped
        }
    }

    /// Delete the whole derived state of a bucket — the `BUCKETS` row, the
    /// `OBJECT_META` / `OBJECT_PARTS` / `UPLOADS` / `PARTS` ranges, the
    /// version tables (`VERSIONS`, `VERSION_INDEX`, `BUCKET_VERSIONING`) and
    /// the bucket's lifecycle row (`BUCKET_LIFECYCLE`, LC19) — in one write
    /// transaction (meta-redb-spec G2: a bucket's state dies
    /// atomically), then drop the bucket's block subtree
    /// (`<state-dir>/blobs/<bucket>/` — the content-addressed blocks of its
    /// superseded versions). The bucket directory itself is removed by the
    /// caller under the mutation lock; errors propagate — a swallowed
    /// failure would leak `OBJECT_META` rows that no cleanup stage can see
    /// (the repair walk only visits live buckets).
    ///
    /// The version tables drain in **every** build, not only the ones
    /// carrying the version machinery: a build without it can still be
    /// pointed at a root a versioned build wrote, and a deleted bucket must
    /// leave neither rows nor blocks behind.
    pub(crate) async fn remove_bucket_state(&self, bucket: &Name) -> Result<(), Error> {
        let bucket = bucket.clone();
        // The subtree path is taken before the transaction consumes the
        // name. The blocks sit under the state dir (which may be on
        // another volume), so they cannot ride the caller's tombstone
        // rename — they are removed here, in the call the caller holds the
        // bucket's mutation lock for, after the rows that named them.
        let blocks = self.state_dir.join(BLOBS_DIR_NAME).join(bucket.as_ref());
        self.handle
            .write(move |txn| {
                {
                    let mut buckets = bucket::Table::open(txn)?;
                    buckets.remove(&bucket)?;
                }
                {
                    let mut object_meta = object_meta::Table::open(txn)?;
                    object_meta.drain_bucket(&bucket)?;
                }
                {
                    let mut parts = object_part::Table::open(txn)?;
                    parts.drain_bucket(&bucket)?;
                }
                {
                    let mut versions = version_rows::Table::open(txn)?;
                    versions.drain_bucket(&bucket)?;
                }
                {
                    let mut index = version_index::Table::open(txn)?;
                    index.drain_bucket(&bucket)?;
                }
                {
                    let mut state = bucket_versioning::Table::open(txn)?;
                    state.drain_bucket(&bucket)?;
                }
                // LC19: a rule never outlives its bucket. The bucket delete
                // here never reaches `BucketMeta::remove`, so this teardown is
                // the one place the row can leave (and the stale-bucket stage
                // of `Cleanup`, which runs this same call, gets its half).
                bucket_lifecycle::Table::open(txn)?.drain_bucket(&bucket)?;
                drain_bucket_uploads(txn, &bucket)?;
                Ok(())
            })
            .await
            .map_err(Error::from)?;
        fsutil::remove_tree(&blocks).await?;
        Ok(())
    }

    /// The post-rename state of a completed multipart upload: delete its
    /// `UPLOADS` + `PARTS` records, persist the object's `OBJECT_META`
    /// entry (with the completion's `tags` and the interface-computed
    /// composite `checksum` — the backends never hash, spec 2026-08-31 —
    /// and the upload's create-time owner/ACL, read from the upload row
    /// this transaction consumes, the tags precedent — never re-ferried
    /// through the interface — plus the completion's version identity and
    /// sequence, the write branch's `publish_versioned` answer, so the
    /// completed object's current version is recorded with its content,
    /// and the landed file's stamp — `size`/`mtime`/`identity` as the
    /// publish read them off the renamed file, so this call costs no
    /// second stat), and persist
    /// the assembled parts into
    /// `OBJECT_PARTS` — the completed object's retained part list, each
    /// row `(part number, size, algorithm wire, base64 checksum value)`;
    /// `parts` is `Store::complete`'s assembled rows (in part-number
    /// order), and any stale rows of the key are replaced (an
    /// overwriting completion leaves only its own parts). ONE write
    /// transaction (meta-redb-spec §5.3 — rename, then a single
    /// all-or-nothing state transaction). Idempotent: on a retry after a
    /// crash before this call the records are still there and get
    /// deleted; a concurrent abort that already removed them is a
    /// no-op. Errors propagate — the transaction rolls back as a unit,
    /// so a failed call leaves the upload records intact and a client
    /// retry re-runs the whole completion safely.
    #[expect(
        clippy::too_many_arguments,
        reason = "one argument per element the completion's record transaction writes — bucket, key, upload id, etag, size, mtime, identity, checksum, parts, version id, seq and supersede stamp — the removal and the record happen together"
    )]
    pub(crate) async fn complete_object_state(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
        identity: u64,
        checksum: Option<checksum::Recorded>,
        parts: Vec<CompletedPartRow>,
        version_id: Option<Id>,
        version_seq: u64,
        superseded: Option<u64>,
    ) -> Result<(object::Tags, Option<acl::OwnerId>, Acl), Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload_id.to_string();
        let etag = etag.clone();
        let checksum = checksum.clone();
        let parts = parts.clone();
        let version_id = version_id.clone();
        self.handle
            .write(move |txn| {
                // The create-time tags, owner and ACL ride in the upload
                // row this transaction consumes (spec 2026-08-31;
                // owner/ACL spec 2026-09-05) — read before the drain,
                // never re-ferried through the interface. A garbage wire
                // self-heals to the empty set / `None` / the private
                // default.
                let (tags, owner, acl) = {
                    let uploads = upload::Table::open(txn)?;
                    let (tags_wire, owner_wire, acl_wire) = uploads
                        .get_matching(&bucket, &key, &upload_id)?
                        .map_or_else(
                            || (String::new(), String::new(), String::new()),
                            |(_, _, tags_wire, owner_wire, acl_wire)| {
                                (tags_wire, owner_wire, acl_wire)
                            },
                        );
                    (
                        object::Tags::parse_wire_limited(&tags_wire, OBJECT_TAGS_MAX)
                            .unwrap_or_default(),
                        decode_owner_wire(&owner_wire),
                        decode_acl_wire(&acl_wire),
                    )
                };
                drain_upload(txn, &bucket, &upload_id)?;
                {
                    let mut table = object_meta::Table::open(txn)?;
                    table.put(
                        &bucket,
                        &key,
                        &object_meta::Stored {
                            version_id: version_id.clone(),
                            version_seq,
                            is_delete_marker: false,
                            etag: etag.clone(),
                            size,
                            mtime: to_nanos(mtime),
                            file_identity: identity,
                            tags: tags.clone(),
                            checksum: checksum.clone(),
                            owner: owner.clone(),
                            acl: acl.clone(),
                        },
                    )?;
                }
                {
                    // The retained part list: replace any stale rows of
                    // the key, then insert this completion's parts (a
                    // re-completed key must not accumulate rows).
                    let mut parts_table = object_part::Table::open(txn)?;
                    parts_table.remove_key(&bucket, &key)?;
                    for part in parts {
                        parts_table.put(&bucket, &key, &part)?;
                    }
                }
                // The transaction that moves the current slot records the
                // outgoing version as history in the same commit.
                stamp_superseded(txn, &bucket, &key, superseded)?;
                Ok((tags, owner, acl))
            })
            .await
            .map_err(Into::into)
    }

    /// Evaluate fragmentation and update the `compact_needed` marker (one
    /// write transaction; the sweep calls this once per round — the
    /// stats call takes the write lock, so it is low-frequency only).
    pub(crate) async fn evaluate_compact(&self, threshold_percent: u8) -> Result<bool, Error> {
        self.handle
            .evaluate_compact(threshold_percent)
            .await
            .map_err(Into::into)
    }

    /// The write-transaction timing snapshot behind the server's
    /// `tinio_write_lock_*` metrics (pipeline-spec.md §4; the `/metrics`
    /// scrape path reads it — a cheap atomic snapshot, never a lock).
    pub fn write_lock_stats(&self) -> database::WriteLockSnapshot {
        self.handle.write_lock_stats()
    }
}

impl Storage for FsStorage {
    type Error = Error;
}

#[async_trait::async_trait]
impl storage::LifecycleRules for FsStorage {
    async fn lifecycles(&self) -> Result<Vec<(Name, lifecycle::Config)>, storage::Error> {
        <RedbMetadata as BucketMeta>::lifecycles(&self.meta_store().engine())
            .await
            .map_err(Into::into)
    }
}

impl FsStorage {
    /// The bucket-group receiver over this backend's handles.
    ///
    /// The two policies are fs's, declared and never inferred: a metadata-row
    /// write that fails after the content is published is a warning (the object
    /// is committed and the next read heals the row — [`MetadataFailure::Warn`]),
    /// and a bucket row is recorded lazily ([`BucketRow::Lazy`]: a directory
    /// that was never sighted through the API is still a bucket, and its reads
    /// serve the empty defaults).
    ///
    /// The receiver carries no version plane (spec E16): fs's version handles
    /// are **per bucket** — the block root is `<state>/blobs/<bucket>` — so the
    /// version-touching bodies build one from [`Self::versions`] and take it
    /// beside the receiver.
    pub(crate) fn ops(&self) -> EngineBucketOps<RedbMetadata, FsContent, FsHooks> {
        // The content plane and the hooks share one `Arc` of this backend's
        // handles ([`ReceiverHandles`]): its root, state dir, single
        // state-database handle and per-bucket lock map (the receiver's clone
        // shares every `Arc` inside, so the locks the trait impls take are the
        // same slots) — built once for the storage rather than per operation.
        let handles = self.receiver_handles();
        EngineBucketOps {
            meta: self.meta_store().engine(),
            content: Arc::clone(&handles.own_content),
            hooks: FsHooks {
                storage: Arc::clone(&handles.storage),
            },
            metadata_failure: MetadataFailure::Warn,
            bucket_row: BucketRow::Lazy,
        }
    }

    /// The object-group receiver over this backend's handles.
    ///
    /// The two policies are fs's, declared and never inferred: a metadata-row
    /// write that fails after the content is published is a **warning** (the
    /// object is committed, the failure is logged, and the next read heals the
    /// row — [`MetadataFailure::Warn`], RI2), and a bucket row is recorded
    /// lazily ([`BucketRow::Lazy`]: a directory that was never sighted through
    /// the API is still a bucket).
    ///
    /// The content plane is the **non-owning** one
    /// ([`FsContent::for_engine`](crate::content::FsContent)): every object
    /// body here is called from the `ObjectOps` trait impl under the bucket
    /// mutation lock that impl takes, exactly the critical section the
    /// pre-engine bodies held, so the plane's publish must ride that lock
    /// instead of taking it again — the per-bucket lock is not re-entrant.
    ///
    /// The receiver's handles are the storage's own, resolved once
    /// ([`ReceiverHandles`]): the pre-engine bodies cloned nothing on a read,
    /// and the extraction's per-call `FsStorage` deep copy — the two
    /// `PathBuf`s, the owner map and the two `Arc` allocations per operation —
    /// is gone with it. The `read_meta` bench pins the result: its three legs
    /// drive `head_object` (plain, version-recording and version-addressed)
    /// through this constructor.
    pub(crate) fn objects(&self) -> EngineObjectOps<RedbMetadata, FsContent> {
        let handles = self.receiver_handles();
        EngineObjectOps {
            meta: self.meta_store().engine(),
            content: Arc::clone(&handles.engine_content),
            metadata_failure: MetadataFailure::Warn,
            bucket_row: BucketRow::Lazy,
        }
    }

    /// The handles the group receivers are built from, resolved on first use
    /// ([`ReceiverHandles`]).
    ///
    /// The clone inside is made **once per storage** and left with an empty
    /// cache of its own: the receivers it feeds hold `Arc<FsStorage>`s, so a
    /// cache pointing back at the storage that filled it would be a reference
    /// cycle, while the receivers' own clone only needs the shared handles
    /// (see the struct's note).
    fn receiver_handles(&self) -> &ReceiverHandles {
        self.receivers.get_or_init(|| {
            let storage = Arc::new(Self {
                receivers: Arc::new(OnceLock::new()),
                ..self.clone()
            });
            ReceiverHandles {
                engine_content: Arc::new(FsContent::for_engine(Arc::clone(&storage))),
                own_content: Arc::new(FsContent::new(Arc::clone(&storage))),
                storage,
            }
        })
    }

    /// The version plane a version-creating object body takes (spec E16,
    /// RW2).
    ///
    /// fs's version handles are **per bucket** — the block root is
    /// `<state-dir>/blobs/<bucket>` and the version state machine is built
    /// over that bucket's rows, adapter and block store — so the plane is
    /// built for the one bucket the body is about, from the same
    /// [`Self::versions`] the rest of the version paths use (the pre-engine
    /// versioned bodies built the same state machine per op, so the plane's
    /// own construction is not a new cost; its `Arc` wrapper and the
    /// `version_rows` handle are). The row handle
    /// beside it is the version adapter ([`Rows`]), whose `CurrentRows` port
    /// applies fs's staleness gate: the body reads the current slot the same
    /// way every read path does.
    #[cfg(feature = "versioning")]
    pub(crate) fn version_plane(
        &self,
        bucket: &Name,
    ) -> VersionPlane<Versions<Rows, FsContent, LocalBlocks>, Rows, LocalBlocks> {
        VersionPlane {
            versions: Arc::new(self.versions(bucket)),
            version_rows: Arc::new(Rows::new(self.storage_handle())),
            blobs: BlobStore::new(LocalBlocks::new(self.blobs_dir().join(bucket.as_ref()))),
        }
    }

    /// This backend as the shared `Arc<FsStorage>` the version planes hold —
    /// the one handle [`Self::receiver_handles`] builds per storage and every
    /// content plane and hook already rides, so a version-creating operation
    /// pays an Arc bump per plane rather than a deep copy of the root, the
    /// state dir and the owner map.
    #[cfg(feature = "versioning")]
    fn storage_handle(&self) -> Arc<FsStorage> {
        Arc::clone(&self.receiver_handles().storage)
    }
}

/// The `Hooks` implementation fs's bucket receiver carries.
///
/// The two steps only this backend can take: the bucket teardown — the
/// tombstone move, the derived state's removal and the removal pipeline's
/// reclaim — and the per-upload part lock, whose guard the part-write bodies
/// hold across their commit phase.
#[derive(Debug, Clone)]
pub(crate) struct FsHooks {
    /// The backend whose tombstone move and part lock the hooks take.
    #[debug("<storage>")]
    storage: Arc<FsStorage>,
}

#[async_trait]
impl Hooks for FsHooks {
    async fn before_bucket_delete(&self, bucket: &Name) -> Result<(), storage::Error> {
        // The caller — `delete_bucket`'s trait impl — holds this bucket's
        // mutation guard and has run the emptiness probes; this is the
        // teardown body under that guard (it takes no lock of its own: the
        // per-bucket lock is not re-entrant).
        self.storage
            .unpublish_bucket_locked(bucket)
            .await
            .map_err(Into::into)
    }

    async fn lock_part(&self, bucket: &Name, upload_id: &str) -> Box<dyn Guard> {
        // The part lock's one home is the multipart store's map; the guard
        // releases it on drop.
        Box::new(FsPartGuard {
            _lock: self
                .storage
                .multipart_store()
                .part_lock(bucket, upload_id)
                .await,
        })
    }
}

/// fs's part-write guard: the per-upload lock the multipart store hands out,
/// held for as long as this value lives.
pub(crate) struct FsPartGuard {
    /// The held lock (released when this guard drops). The field is never
    /// read: holding it IS the guard.
    _lock: lockmap::Guard<(String, String)>,
}

impl Guard for FsPartGuard {}

#[cfg(test)]
mod tests;
