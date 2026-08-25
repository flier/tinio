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

pub use crate::error::Error;
pub(crate) use crate::error::{invalid_path, invalid_value, root_not_directory};
pub use objects::StagedBody;

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use garde::Validate;
use getset::{CopyGetters, Getters};
use smart_default::SmartDefault;
use tokio::sync;

use tinio_core::{
    ETag, object,
    storage::{
        COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT,
        DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_FOLLOW_SYMLINKS, Storage, no_such_bucket,
    },
};

use crate::{
    bucket,
    database::{self, BucketsTable, Handle, ObjectMetaTable, compact_if_needed},
    listing::FsListing,
    meta,
    multipart::{drain_bucket_uploads, drain_upload},
    path::{BoundaryCache, map_bucket_path, map_key_path, state_dir},
    write::AtomicWriter,
};

/// Construction options of [`FsStorage`].
///
/// # Examples
///
/// ```rust
/// use tinio_fs::FsOptions;
///
/// let options = FsOptions::default();
/// assert!(!options.follow_symlinks); // default: reject symlinks
/// assert_eq!(options.compact_threshold_percent, 20);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, SmartDefault, Validate)]
pub struct FsOptions {
    /// Follow symlinks in the storage root (default `false`: symlinks are
    /// rejected — access never resolves through a link, and link entries
    /// are excluded from listings — so a link inside a bucket cannot
    /// escape the storage root). Set `true` to follow them.
    #[default(_code = "DEFAULT_FOLLOW_SYMLINKS")]
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
    #[default(_code = "DEFAULT_COMPACT_THRESHOLD_PERCENT")]
    #[garde(
        range(
            min = COMPACT_THRESHOLD_MIN_PERCENT,
            max = COMPACT_THRESHOLD_MAX_PERCENT
        )
    )]
    pub compact_threshold_percent: u8,
}

/// The filesystem storage backend: buckets are top-level subdirectories of
/// the storage root, objects are files.
///
/// Must pass the `tinio-core` conformance harness (a test asserts it).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, storage::{BucketOps, ObjectOps}};
/// use tinio_fs::{FsOptions, FsStorage};
/// use tinio_util::testing::body;
///
/// let root = tempfile::tempdir().unwrap();
/// let storage = FsStorage::new(root.path(), FsOptions::default()).unwrap();
/// let b = bucket::name("data").unwrap();
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
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
    bucket_store: bucket::Store,
    /// The ETag metadata store.
    #[getset(get = "pub(crate)")]
    meta_store: meta::Store,
    /// Multipart parts storage.
    #[getset(get = "pub(crate)")]
    multipart_store: crate::multipart::Store,
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
    /// Serializes bucket-directory mutations (rename/create/remove) so a
    /// `delete_bucket` can never remove a just-written object (the
    /// emptiness check and `remove_dir_all` are one critical section
    /// against every write into the bucket).
    #[getset(skip)]
    bucket_mutation_lock: Arc<sync::Mutex<()>>,
    /// Validated path boundaries (root + per-bucket), identity-checked
    /// and bounded — the containment proof of [`bucket_dir`](Self::bucket_dir)
    /// and [`key_path`](Self::key_path).
    #[getset(skip)]
    boundary_cache: BoundaryCache,
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

    /// Hold the bucket-mutation lock so a concurrent write's phase-2
    /// commit blocks until the guard is dropped. Tests use this to
    /// retarget a followed bucket symlink between resolve and rename.
    #[cfg(test)]
    pub(crate) async fn lock_bucket_mutations(&self) -> sync::MutexGuard<'_, ()> {
        self.bucket_mutation_lock.lock().await
    }

    fn resolve(root: PathBuf, options: FsOptions) -> Result<(PathBuf, PathBuf, FsOptions), Error> {
        options.validate().map_err(invalid_value)?;
        let canonical = fs::canonicalize(&root)?;
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
            compact_threshold_percent,
            ..
        } = options;
        let handle = Handle::new(db);

        Ok(Self {
            follow_symlinks,
            compact_threshold_percent,
            listing: FsListing::new(
                &canonical,
                meta::Store::from_handle(handle.clone()),
                follow_symlinks,
            ),
            bucket_store: bucket::Store::from_handle(handle.clone()),
            meta_store: meta::Store::from_handle(handle.clone()),
            multipart_store: crate::multipart::Store::from_handle(handle.clone(), &state_dir),
            handle,
            writer: AtomicWriter::new(&state_dir),
            bucket_mutation_lock: Arc::new(sync::Mutex::new(())),
            boundary_cache: BoundaryCache::new(),
            root: canonical,
            state_dir,
        })
    }

    /// The bucket directory `<root>/<bucket>`.
    ///
    /// With `follow_symlinks` enabled a symlinked/junction bucket
    /// directory is **resolved to its canonical target** — the bucket is
    /// the target (a legit way to place a bucket on another volume), and
    /// every proof and walk addresses the same resolved path. With
    /// following disabled the containment proof refuses the link (the
    /// bucket is invisible; callers map that to `NoSuchBucket`).
    pub(crate) fn bucket_dir(&self, name: &bucket::Name) -> Result<PathBuf, Error> {
        if self.follow_symlinks {
            let lexical = self.root().join(&**name);
            match std::fs::symlink_metadata(&lexical) {
                Ok(metadata) if crate::fsutil::is_symlink_or_reparse(&metadata) => {
                    return std::fs::canonicalize(&lexical).map_err(|err| {
                        if err.kind() == io::ErrorKind::NotFound {
                            // A dangling bucket link has no target — no
                            // bucket.
                            crate::Error::Storage(no_such_bucket(name))
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
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    return Err(no_such_bucket(name).into());
                }
                Err(err) => return Err(err.into()),
            }
        }
        map_bucket_path(Some(&self.boundary_cache), self.root(), name)
    }

    /// The object file path `<bucket>/<key>` — the cached form of
    /// [`path::key_path`](crate::path::key_path). `enforce_boundary` is
    /// `!follow_symlinks` for object operations; cleanup/scan paths always
    /// enforce (they must never address outside the bucket).
    pub(crate) fn key_path(
        &self,
        bucket_dir: &Path,
        key: &object::Key,
        enforce_boundary: bool,
    ) -> Result<PathBuf, Error> {
        map_key_path(
            Some(&self.boundary_cache),
            bucket_dir,
            key,
            enforce_boundary,
        )
    }

    /// Resolve `key` under `bucket_dir` through the symlink policy first,
    /// then the containment proof.
    ///
    /// The I/O-time policy fires **before** the proof: the proof would
    /// canonicalize through a link inside the bucket and report an escape
    /// (`InvalidPath` → 400), where the documented contract (s3-surface.md)
    /// answers `AccessDenied` (403) for a link when following is disabled.
    /// With following enabled the plain lexical join is returned (the
    /// policy is off and the proof is skipped — the current object-op
    /// contract).
    pub(crate) async fn resolve_key(
        &self,
        bucket_dir: &Path,
        key: &object::Key,
    ) -> Result<PathBuf, Error> {
        // The lexical join is what the symlink walk inspects (the key is
        // already contract-validated; the supplements and the containment
        // proof re-run in the full mapping below).
        let lexical = bucket_dir.join(&**key);
        if self.follow_symlinks {
            return Ok(lexical);
        }
        self.check_symlinks(key, &lexical).await?;
        // One full mapping: supplements + the containment proof (the
        // `enforce_boundary = true` path). The proof runs AFTER the
        // symlink policy so a link inside the bucket answers AccessDenied
        // (403), not InvalidPath (400) — s3-surface.md.
        map_key_path(Some(&self.boundary_cache), bucket_dir, key, true)
    }

    /// Every bucket of the root: top-level directories with valid names
    /// (the reserved `.tinio` state dir excluded), in name order. The
    /// scanner and `list_buckets` share this walk — one source of truth
    /// for what a bucket is.
    ///
    /// A symlinked/junction bucket directory is a bucket only when
    /// `follow_symlinks` is enabled (it resolves to its target — the
    /// bucket *is* the target); with following disabled it is invisible.
    pub(crate) async fn bucket_names(&self) -> Result<Vec<bucket::Name>, Error> {
        let mut out = Vec::new();
        let mut entries = tokio::fs::read_dir(self.root()).await?;
        while let Some(entry) = entries.next_entry().await? {
            // lstat: a link entry is judged by its resolved target only
            // when following is enabled (a broken link is skipped).
            let lmeta = match tokio::fs::symlink_metadata(entry.path()).await {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let mut is_dir = lmeta.is_dir();
            if self.follow_symlinks && crate::fsutil::is_symlink_or_reparse(&lmeta) {
                is_dir = tokio::fs::metadata(entry.path())
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
            }
            if !is_dir {
                continue; // root-level files (and non-dir links) are not buckets
            }
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Ok(name) = bucket::name(name) else {
                continue; // invalid names (incl. `.tinio`) are not buckets
            };
            out.push(name);
        }
        out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(out)
    }

    /// Delete the whole derived state of a bucket — the `BUCKETS` row and
    /// the `OBJECT_META` / `UPLOADS` / `PARTS` ranges — in one write
    /// transaction (meta-redb-spec G2: a bucket's state dies atomically).
    /// The bucket directory itself is removed by the caller under the
    /// mutation lock; errors propagate — a swallowed failure would leak
    /// `OBJECT_META` rows that no cleanup stage can see (the repair walk
    /// only visits live buckets).
    pub(crate) async fn remove_bucket_state(&self, bucket: &bucket::Name) -> Result<(), Error> {
        self.handle
            .write(|txn| {
                {
                    let mut buckets = BucketsTable::open(txn)?;
                    buckets.remove(bucket)?;
                }
                {
                    let mut meta = ObjectMetaTable::open(txn)?;
                    meta.drain_bucket(bucket)?;
                }
                drain_bucket_uploads(txn, bucket)?;
                Ok(())
            })
            .map_err(Into::into)
    }

    /// The post-rename state of a completed multipart upload: delete its
    /// `UPLOADS` + `PARTS` records and persist the object's `OBJECT_META`
    /// entry in ONE write transaction (meta-redb-spec §5.3 — rename,
    /// then a single all-or-nothing state transaction). Idempotent: on a
    /// retry after a crash before this call the records are still there
    /// and get deleted; a concurrent abort that already removed them is
    /// a no-op. Errors propagate — the transaction rolls back as a unit,
    /// so a failed call leaves the upload records intact and a client
    /// retry re-runs the whole completion safely.
    pub(crate) async fn complete_object_state(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        etag: &ETag,
        path: &Path,
        metadata: &fs::Metadata,
    ) -> Result<(), Error> {
        let size = metadata.len();
        let mtime = metadata.modified()?;
        let identity = crate::fsutil::file_identity(path, metadata);
        self.handle
            .write(|txn| {
                drain_upload(txn, bucket, upload_id)?;
                ObjectMetaTable::open(txn)?.put(bucket, key, etag, size, mtime, identity)
            })
            .map_err(Into::into)
    }

    /// Evaluate fragmentation and update the `compact_needed` marker (one
    /// write transaction; the sweep calls this once per round — the
    /// stats call takes the write lock, so it is low-frequency only).
    pub(crate) fn evaluate_compact(&self, threshold_percent: u8) -> Result<bool, Error> {
        self.handle
            .evaluate_compact(threshold_percent)
            .map_err(Into::into)
    }
}

impl Storage for FsStorage {
    type Error = Error;
}

#[cfg(test)]
mod tests;
