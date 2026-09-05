//! The `Storage` contract implementation over the local filesystem
//! (tasks T041/T042).
//!
//! [`FsStorage`] maps the contract onto the primitives of this crate:
//! `path` (mapping), `write` (atomic writes), `meta` (ETag store),
//! `buckets` (creation times), `listing` (walk + pagination), `multipart`
//! (parts storage). The operation groups live in `buckets.rs`
//! ([`BucketOps`]), `objects.rs` ([`ObjectOps`]), and `multipart.rs`
//! ([`MultipartOps`]).

mod buckets;
mod multipart;
mod objects;

use std::{
    fs::{Metadata, canonicalize},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use derive_more::Debug;
use garde::Validate;
use getset::{CopyGetters, Getters};
use lockmap::Map;
pub use objects::StagedBody;
use tokio::fs;

pub use crate::error::Error;
pub(crate) use crate::error::{invalid_path, invalid_value, root_not_directory};
use crate::{
    _core::{
        ETag,
        bucket::{Name, name},
        checksum, object, pipeline,
        storage::{
            COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT,
            DEFAULT_MAX_CONCURRENT_UPLOADS, META_BATCH_BYTES_MAX, META_BATCH_BYTES_MIN,
            META_BATCH_SIZE_MAX, META_BATCH_SIZE_MIN, Storage, no_such_bucket,
        },
        to_nanos,
    },
    _store::{bucket, meta, object_part, upload},
    _util::lockmap,
    bucket::Store as BucketStore,
    database::{self, Handle, compact_if_needed},
    etag, fsutil,
    listing::FsListing,
    meta::Store as MetaStore,
    multipart::{CompletedPartRow, Store as MultipartStore, drain_bucket_uploads, drain_upload},
    path::{
        BoundaryCache, map_bucket_path, map_key_path, map_key_path_lexical, prove_key_contained,
        state_dir,
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
///     compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///     meta_batch_size: DEFAULT_META_BATCH_SIZE,
///     meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
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
    /// = `<root>/.tinio/`; read-only mode relocates it to
    /// `~/.tinio/roots/<sha1(root)>/` (FR-023).
    #[garde(skip)]
    pub state_dir: Option<PathBuf>,
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
    /// meta_batch_size`, 1..=4096): the cold list/scanner producers flush
    /// one write-pipeline batch once it holds this many entries
    /// (pipeline-spec.md Q5; default from the task-2.5 benchmark, Q6).
    #[garde(range(min = META_BATCH_SIZE_MIN, max = META_BATCH_SIZE_MAX))]
    pub meta_batch_size: u16,
    /// The meta-batch byte threshold (`[storage.fs] meta_batch_bytes`,
    /// 1024..=16 MiB): the producers flush once the estimated batch size
    /// (≈ 56 B + key length per entry) reaches this (pipeline-spec.md Q5).
    #[garde(range(min = META_BATCH_BYTES_MIN, max = META_BATCH_BYTES_MAX))]
    pub meta_batch_bytes: u32,
    /// The IO pipeline (pipeline-spec.md §3.1): the cold list/scanner
    /// paths enqueue `etag::ComputeTask` instances here. Mandatory
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
///         compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///         meta_batch_size: DEFAULT_META_BATCH_SIZE,
///         meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
///         io_pipeline: Arc::new(InlineRunner::default()),
///         remove_pipeline: Arc::new(InlineRunner::default()),
///         db_pipeline: Arc::new(InlineRunner::default()),
///     },
/// )
/// .unwrap();
/// let b = bucket::name("data").unwrap();
/// Runtime::new().unwrap().block_on(async {
///     storage.create_bucket(&b).await.unwrap();
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
    /// [`Self::remove_bucket_state`]).
    #[getset(skip)]
    handle: Arc<Handle>,
    /// Atomic body writer (staging under `<state-dir>/tmp/`).
    #[getset(skip)]
    writer: AtomicWriter,
    /// The tree-walk listing (shared with the scanner).
    #[getset(get = "pub(crate)")]
    listing: FsListing,
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
}

/// The per-bucket mutation-lock wait above which [`FsStorage::lock_bucket_mutations`]
/// warns (D-E) — a second: delete/create/PUT-commit of the same name
/// should never hold the mutex that long in normal operation, so the
/// warn marks a stuck peer, not routine contention.
const MUTATION_LOCK_WARN_THRESHOLD: Duration = Duration::from_secs(1);

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
        // Compact strictly before sharing: `Database::compact` needs
        // `&mut`, and once wrapped in `Arc<Handle>` the mutable
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
        Self::from_resolved(canonical, state_dir, options, db)
    }

    /// Construct the backend over `root` from an **already-opened** state
    /// database — the orchestration path (G1): `database::open` → (compact
    /// with the exclusive handle) → `new_from_db`. The database handle is
    /// wrapped once and shared by every store. The server startup and
    /// doctor orchestration (T068/T073/T074) use this path explicitly.
    pub fn new_from_db(
        root: impl Into<PathBuf>,
        options: FsOptions,
        db: redb::Database,
    ) -> Result<Self, Error> {
        let (canonical, state_dir, options) = Self::resolve(root.into(), options)?;
        Self::from_resolved(canonical, state_dir, options, db)
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

    fn resolve(root: PathBuf, options: FsOptions) -> Result<(PathBuf, PathBuf, FsOptions), Error> {
        options.validate().map_err(invalid_value)?;
        let canonical = canonicalize(&root)?;
        if !canonical.is_dir() {
            return Err(root_not_directory(canonical));
        }
        let state_dir = match options.state_dir {
            Some(ref dir) => dir.clone(),
            None => state_dir(&canonical)?,
        };
        Ok((canonical, state_dir, options))
    }

    fn from_resolved(
        canonical: PathBuf,
        state_dir: PathBuf,
        options: FsOptions,
        db: redb::Database,
    ) -> Result<Self, Error> {
        let FsOptions {
            follow_symlinks,
            state_dir: _,
            compact_threshold_percent,
            meta_batch_size,
            meta_batch_bytes,
            io_pipeline,
            remove_pipeline,
            db_pipeline,
        } = options;
        let handle = Handle::new(db);

        Ok(Self {
            follow_symlinks,
            compact_threshold_percent,
            listing: FsListing::new(
                &canonical,
                MetaStore::from_handle(handle.clone()),
                follow_symlinks,
                io_pipeline,
                db_pipeline,
                meta_batch_size,
                meta_batch_bytes,
            ),
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

    /// Delete the whole derived state of a bucket — the `BUCKETS` row and
    /// the `OBJECT_META` / `OBJECT_PARTS` / `UPLOADS` / `PARTS` ranges —
    /// in one write transaction (meta-redb-spec G2: a bucket's state dies
    /// atomically). The bucket directory itself is removed by the caller
    /// under the mutation lock; errors propagate — a swallowed failure
    /// would leak `OBJECT_META` rows that no cleanup stage can see (the
    /// repair walk only visits live buckets).
    pub(crate) async fn remove_bucket_state(&self, bucket: &Name) -> Result<(), Error> {
        let bucket = bucket.clone();
        self.handle
            .write(move |txn| {
                {
                    let mut buckets = bucket::Table::open(txn)?;
                    buckets.remove(&bucket)?;
                }
                {
                    let mut meta = meta::Table::open(txn)?;
                    meta.drain_bucket(&bucket)?;
                }
                {
                    let mut parts = object_part::Table::open(txn)?;
                    parts.drain_bucket(&bucket)?;
                }
                drain_bucket_uploads(txn, &bucket)?;
                Ok(())
            })
            .await
            .map_err(Into::into)
    }

    /// The post-rename state of a completed multipart upload: delete its
    /// `UPLOADS` + `PARTS` records, persist the object's `OBJECT_META`
    /// entry (with the completion's `tags` and the interface-computed
    /// composite `checksum` — the backends never hash, spec 2026-08-31),
    /// and persist the assembled parts into `OBJECT_PARTS` — the
    /// completed object's retained part list, each row `(part number,
    /// size, algorithm wire, base64 checksum value)`; `parts` is
    /// `Store::complete`'s assembled rows (in part-number order), and
    /// any stale rows of the key are replaced (an overwriting completion
    /// leaves only its own parts). ONE write transaction
    /// (meta-redb-spec §5.3 — rename, then a single all-or-nothing state
    /// transaction). Idempotent: on a retry after a crash before this
    /// call the records are still there and get deleted; a concurrent
    /// abort that already removed them is a no-op. Errors propagate —
    /// the transaction rolls back as a unit, so a failed call leaves the
    /// upload records intact and a client retry re-runs the whole
    /// completion safely.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn complete_object_state(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        etag: &ETag,
        path: &Path,
        metadata: &Metadata,
        checksum: Option<checksum::Recorded>,
        parts: Vec<CompletedPartRow>,
    ) -> Result<object::Tags, Error> {
        let size = metadata.len();
        let mtime = metadata.modified()?;
        let identity = fsutil::file_identity(path, metadata);
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload_id.to_string();
        let etag = etag.clone();
        let checksum = checksum.clone();
        let parts = parts.clone();
        self.handle
            .write(move |txn| {
                // The create-time tags ride in the upload row this
                // transaction consumes (spec 2026-08-31) — read before
                // the drain, never re-ferried through the interface. A
                // garbage wire self-heals to the empty set.
                let tags = {
                    let uploads = upload::Table::open(txn)?;
                    let tags_wire = uploads
                        .get_matching(&bucket, &key, &upload_id)?
                        .map_or_else(String::new, |(_, _, wire)| wire);
                    object::Tags::parse_wire_limited(&tags_wire, object::OBJECT_TAGS_MAX)
                        .unwrap_or_default()
                };
                drain_upload(txn, &bucket, &upload_id)?;
                {
                    let mut table = meta::Table::open(txn)?;
                    table.put(
                        &bucket,
                        &key,
                        &meta::Stored {
                            etag: etag.clone(),
                            size,
                            mtime: to_nanos(mtime),
                            file_identity: identity,
                            tags: tags.clone(),
                            checksum: checksum.clone(),
                        },
                    )?;
                }
                {
                    // The retained part list: replace any stale rows of
                    // the key, then insert this completion's parts (a
                    // re-completed key must not accumulate rows).
                    let mut parts_table = object_part::Table::open(txn)?;
                    parts_table.remove_key(&bucket, &key)?;
                    for (n, part_size, checksum_row) in &parts {
                        let (algorithm, value) = match checksum_row {
                            Some((algorithm, value)) => (algorithm.as_str(), value.as_str()),
                            None => ("", ""),
                        };
                        parts_table.put(&bucket, &key, *n, *part_size, algorithm, value)?;
                    }
                }
                Ok(tags)
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

#[cfg(test)]
mod tests;
