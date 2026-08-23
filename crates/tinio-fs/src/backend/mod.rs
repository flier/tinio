//! The `Storage` contract implementation over the local filesystem
//! (tasks T041/T042).
//!
//! [`FsStorage`] maps the contract onto the primitives of this crate:
//! `path` (mapping), `write` (atomic writes), `meta` (ETag store),
//! `buckets` (creation times), `listing` (walk + pagination), `multipart`
//! (parts storage). The operation groups live in `buckets.rs`
//! ([`BucketOps`]) and `objects.rs` ([`ObjectOps`] + [`MultipartOps`]).

mod buckets;
mod objects;

pub use crate::error::Error;
pub(crate) use crate::error::{
    corrupt_state_file, invalid_path, root_not_directory, unsupported_state_version,
};

use std::{fs, path::PathBuf, sync::Arc};

use getset::Getters;
use tinio_core::{bucket, storage::Storage};

use crate::{
    buckets::BucketStore,
    listing::FsListing,
    meta::MetaStore,
    multipart::MultipartStore,
    path::{bucket_path, state_dir},
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
/// assert!(options.follow_symlinks); // default: follow
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsOptions {
    /// Follow symlinks in the storage root (default). `false` rejects
    /// access resolving through a link and excludes link entries from
    /// listings.
    pub follow_symlinks: bool,
    /// State-dir override: where the private state lives. `None` (default)
    /// = `<root>/.tinio/`; read-only mode relocates it to
    /// `~/.tinio/roots/<sha1(root)>/` (FR-023).
    pub state_dir: Option<PathBuf>,
}

impl Default for FsOptions {
    fn default() -> Self {
        // `[storage] follow_symlinks = true` is the config default
        // (contracts/config.md).
        Self {
            follow_symlinks: true,
            state_dir: None,
        }
    }
}

/// The filesystem storage backend: buckets are top-level subdirectories of
/// the storage root, objects are files.
///
/// Must pass the `tinio-core` conformance harness (a test asserts it).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, testing::body};
/// use tinio_core::storage::{BucketOps, ObjectOps};
/// use tinio_fs::{FsOptions, FsStorage};
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
#[derive(Debug, Clone, Getters)]
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
    /// Bucket creation times (`buckets.json`).
    #[getset(get = "pub(crate)")]
    bucket_store: BucketStore,
    /// The ETag metadata store.
    #[getset(get = "pub(crate)")]
    meta_store: MetaStore,
    /// Multipart parts storage.
    #[getset(get = "pub(crate)")]
    multipart_store: MultipartStore,
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
    bucket_mutation_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FsStorage {
    /// Open (or create) the backend over `root`. The root must exist and
    /// be a directory; it is canonicalized so the reserved `.tinio/` is
    /// always found at the same physical location.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the root does not exist or cannot be
    /// canonicalized; [`Error::RootNotDirectory`] when the root is not a directory.
    pub fn new(root: impl Into<PathBuf>, options: FsOptions) -> Result<Self, Error> {
        let root = root.into();
        let canonical = fs::canonicalize(&root)?;
        if !canonical.is_dir() {
            return Err(root_not_directory(canonical));
        }
        let state_dir = options.state_dir.unwrap_or_else(|| state_dir(&canonical));
        let meta = MetaStore::new(&state_dir);
        Ok(Self {
            follow_symlinks: options.follow_symlinks,
            listing: FsListing::new(&canonical, meta.clone(), options.follow_symlinks),
            bucket_store: BucketStore::new(&state_dir),
            meta_store: meta,
            multipart_store: MultipartStore::new(&state_dir),
            writer: AtomicWriter::new(&state_dir),
            bucket_mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            root: canonical,
            state_dir,
        })
    }

    /// The bucket directory `<root>/<bucket>`.
    pub(crate) fn bucket_dir(&self, name: &bucket::Name) -> Result<PathBuf, Error> {
        bucket_path(self.root(), name)
    }

    /// Every bucket of the root: top-level directories with valid names
    /// (the reserved `.tinio` state dir excluded), in name order. The
    /// scanner and `list_buckets` share this walk — one source of truth
    /// for what a bucket is.
    pub(crate) async fn bucket_names(&self) -> Result<Vec<bucket::Name>, Error> {
        let mut out = Vec::new();
        let mut entries = tokio::fs::read_dir(self.root()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue; // root-level files are not buckets
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
}

impl Storage for FsStorage {
    type Error = Error;
}
