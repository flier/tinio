//! The filesystem tree walk and the listing machinery around it (task T043).
//!
//! A directory-tree walk (buckets → files) with prefix filtering and
//! delimiter-based grouping (common-prefix roll-up) per S3 semantics
//! (FR-004), emitted one file at a time in walk order — the one source of
//! truth for what an object is, shared by the **content plane's `scan`**
//! (the window the ops engine's `list_objects` page walks through) and the
//! **scanner**. The page itself — prefix/delimiter roll-up, pagination and
//! the row batch — is the ops engine's (`tinio_ops`): nothing here
//! assembles one.
//!
//! The page's ETags and its heal are the engine's too: a missing or stale
//! row of the emitted page is recomputed from the content and persisted
//! through the row contract as the page is assembled. The **scanner** is
//! the path that still enqueues through the **IO pipeline**
//! (`etag::ComputeTask`, pipeline-spec.md §3.2) and the **write-pipeline
//! batches** (`MetaWriteBatchTask`) — the documented one-time full-content
//! pass over externally-added files. `.tinio` entries are always skipped
//! (FR-020); symlink entries are excluded when `follow_symlinks` is
//! disabled. No listing latency bound is promised; listings remain correct
//! and complete at all times.
//!
//! This module also hosts the **streaming batch accumulator** the scanner
//! producer uses: [`MetaBatchAccumulator`] groups computed entries into
//! write-pipeline batches, flushing on the `[storage.fs]
//! meta_batch_size` / `meta_batch_bytes` thresholds (Q5).

use std::{
    collections::HashSet,
    fs::Metadata,
    io::ErrorKind,
    mem::take,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::SystemTime,
};

use derive_more::Debug;
use futures::{Stream, stream};
use tokio::fs;
use tracing::instrument;

use crate::{
    _core::{
        bucket, object,
        pipeline::{self, Completion},
        storage,
    },
    Error, etag, fsutil, meta, path,
    path::STATE_DIR_NAME,
    write_task::MetaWriteBatchTask,
};

/// The per-entry meta-batch size estimate (pipeline-spec.md Q5): a stored
/// row is ≈ 56 B plus the key bytes. The scanner producer uses it for the
/// `meta_batch_bytes` flush trigger.
pub(crate) const META_ENTRY_ESTIMATE_BYTES: u64 = 56;

/// One walked object file: the key, the path, and the size/mtime of the
/// walk's own stat. The identity is **lazy** ([`WalkedFile::identity`]):
/// unix reads it from the stored metadata (zero extra cost); Windows
/// opens the path on demand — a LIST gate pays the open only for the
/// emitted page entries (≤ `max_keys`), never once per walked file. The
/// identity is the page's replacement detector (F01): a same-size
/// mtime-preserving replacement (`cp -p`, `rsync -a`) is invisible to
/// the size+mtime pair but changes the identity — a gate that never
/// consults it would serve the old ETag forever (the walk's consumer on the
/// page path is the engine's staleness gate, [`WalkedFile::identity`] being
/// how `content.scan` hands it over). The lazy form is as
/// conservative as the eager one: the identity is always the CURRENT
/// file's at fetch time, so a replacement anywhere between the walk and
/// the gate is a gate miss, never a false hit.
#[derive(Debug, Clone)]
pub struct WalkedFile {
    /// The object key.
    pub key: object::Key,
    /// The file path on disk.
    pub path: PathBuf,
    /// The object size at stat time.
    pub size: u64,
    /// The object mtime at stat time.
    pub mtime: SystemTime,
    /// The stat the size/mtime came from — the identity source.
    metadata: Metadata,
}

impl WalkedFile {
    /// The file identity of the SAME stat (F01): unix dev+inode from the
    /// stored metadata (free); Windows volume serial + file index via an
    /// open of the path (the walk has no handle in hand — only consumers
    /// that actually gate pay the open: the LIST page gate, or the
    /// scanner once per file).
    pub fn identity(&self) -> u64 {
        fsutil::file_identity(&self.path, &self.metadata)
    }
}

/// The streaming meta-batch accumulator the scanner producer drives
/// (pipeline-spec.md Q5): computed entries accumulate into the
/// current batch, which flushes once its entry count reaches
/// `meta_batch_size` **or** its estimated bytes reach `meta_batch_bytes`
/// (either trigger; estimate ≈ [`META_ENTRY_ESTIMATE_BYTES`] + key
/// length). One flush = one [`MetaWriteBatchTask`] = one write
/// transaction. The caller decides what to do with the returned
/// [`Completion`]: the scanner drops it (Q3b fire-and-forget).
pub(crate) struct MetaBatchAccumulator<'a> {
    bucket: &'a bucket::Name,
    meta: meta::Store,
    db: &'a Arc<dyn pipeline::Runner<Result<(), Error>>>,
    batch_size: u16,
    batch_bytes: u32,
    entries: Vec<meta::BatchEntry>,
    bytes: u64,
}

impl<'a> MetaBatchAccumulator<'a> {
    /// Create an accumulator over one bucket, flushing into `db`.
    pub(crate) fn new(
        bucket: &'a bucket::Name,
        meta: meta::Store,
        db: &'a Arc<dyn pipeline::Runner<Result<(), Error>>>,
        batch_size: u16,
        batch_bytes: u32,
    ) -> Self {
        Self {
            bucket,
            meta,
            db,
            batch_size,
            batch_bytes,
            entries: Vec::new(),
            bytes: 0,
        }
    }

    /// Add one computed entry; flush (and return the batch completion)
    /// when a threshold is reached. `Err` = the batch was not accepted
    /// (shutdown, Q3).
    pub(crate) async fn push(
        &mut self,
        entry: meta::BatchEntry,
    ) -> Result<Option<Completion<Result<(), Error>>>, pipeline::Error> {
        self.bytes += META_ENTRY_ESTIMATE_BYTES + entry.key.as_ref().len() as u64;
        self.entries.push(entry);
        if self.entries.len() >= usize::from(self.batch_size)
            || self.bytes >= u64::from(self.batch_bytes)
        {
            self.flush().await
        } else {
            Ok(None)
        }
    }

    /// Fold one [`etag::Outcome`] into the batch (F38 — the compute-result
    /// fold's Ok path; the failure policy stays at the call site). The batch entry uses the outcome's own
    /// hash-time metadata, so a row never pairs a hash-time ETag with
    /// walk-time size/mtime — a file changed between the walk and the
    /// hash yields a self-consistent row the next gate recomputes (F19).
    pub(crate) async fn push_outcome(
        &mut self,
        outcome: etag::Outcome,
    ) -> Result<Option<Completion<Result<(), Error>>>, pipeline::Error> {
        self.push(meta::BatchEntry {
            key: outcome.key,
            etag: outcome.etag,
            size: outcome.size,
            mtime: outcome.mtime,
            identity: outcome.identity,
        })
        .await
    }

    /// Enqueue the accumulated entries as one write-pipeline batch. An
    /// empty accumulator is a no-op — no task, no write transaction (the
    /// scanner flushes a possibly empty accumulator after the last
    /// result).
    pub(crate) async fn flush(
        &mut self,
    ) -> Result<Option<Completion<Result<(), Error>>>, pipeline::Error> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        self.bytes = 0;
        let entries = take(&mut self.entries);
        let task = MetaWriteBatchTask {
            meta: self.meta.clone(),
            bucket: self.bucket.clone(),
            entries,
        };
        self.db.enqueue(Box::new(task)).await.map(Some)
    }
}

/// Listing over one storage root (bucket dirs at the top level): the tree
/// walk the content plane's `scan` and the scanner share, the pipelines the
/// scanner produces into, and the batch accumulator that producer drives.
/// The **page** is the ops engine's (`tinio_ops::ObjectOps::list_objects`) —
/// it enumerates through `ObjectContent::scan`, which walks through this
/// value — so nothing here assembles one.
#[derive(Debug, Clone)]
pub struct FsListing {
    /// Storage root (bucket dirs at the top level).
    root: PathBuf,
    /// Exclude symlink entries (and do not descend symlink dirs) when
    /// `follow_symlinks` is disabled.
    follow_symlinks: bool,
    /// The IO pipeline (`etag::ComputeTask`, typed to [`etag::Result`] — P4).
    #[debug("<runner>")]
    io_pipeline: Arc<dyn pipeline::Runner<etag::Result>>,
    /// The DB write pipeline (`MetaWriteBatchTask` — P4).
    #[debug("<runner>")]
    db_pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>,
    /// The meta-batch entry-count flush threshold (`[storage.fs]
    /// meta_batch_size`).
    meta_batch_size: u16,
    /// The meta-batch byte flush threshold (`[storage.fs]
    /// meta_batch_bytes`).
    meta_batch_bytes: u32,
}

/// The listing's streaming walk answer: one boxed stream per walk (the
/// per-poll state machine borrows its own cursor across awaits, so it is
/// `!Unpin` by construction).
type WalkStream = Pin<Box<dyn Stream<Item = Result<WalkedFile, Error>> + Send + 'static>>;

impl FsListing {
    /// Create a listing over `root`. The pipelines are mandatory (P4) —
    /// the scanner producer enqueues into them; offline contexts pass
    /// [`pipeline::InlineRunner`]. `meta_batch_size`/`meta_batch_bytes`
    /// are the streaming flush thresholds (Q5, `[storage.fs]`).
    pub fn new(
        root: &Path,
        follow_symlinks: bool,
        io_pipeline: Arc<dyn pipeline::Runner<etag::Result>>,
        db_pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>,
        meta_batch_size: u16,
        meta_batch_bytes: u32,
    ) -> Self {
        Self {
            root: root.to_path_buf(),
            follow_symlinks,
            io_pipeline,
            db_pipeline,
            meta_batch_size,
            meta_batch_bytes,
        }
    }

    /// The IO pipeline — the scanner producer enqueues its
    /// [`etag::ComputeTask`]s through the same handle (one source of truth
    /// for the storage's pipelines).
    pub(crate) fn io_pipeline(&self) -> &Arc<dyn pipeline::Runner<etag::Result>> {
        &self.io_pipeline
    }

    /// The DB write pipeline (scanner producer).
    pub(crate) fn db_pipeline(&self) -> &Arc<dyn pipeline::Runner<Result<(), Error>>> {
        &self.db_pipeline
    }

    /// The meta-batch entry-count flush threshold (scanner producer).
    pub(crate) fn meta_batch_size(&self) -> u16 {
        self.meta_batch_size
    }

    /// The meta-batch byte flush threshold (scanner producer).
    pub(crate) fn meta_batch_bytes(&self) -> u32 {
        self.meta_batch_bytes
    }

    /// The **streaming** form of the walk: every object file of a bucket,
    /// one at a time, in walk order (a directory's `read_dir` order,
    /// worklist-depth-first — never a key sort; the scanner needs no
    /// order, pipeline-spec.md §3.7, and the ops engine's listing paginates
    /// what it scans through the bounded `UnorderedPager`, which holds only
    /// the page). Directories are never objects, `.tinio` entries are
    /// skipped at any depth (FR-020), symlink entries are excluded when
    /// `follow_symlinks` is disabled, and `key_prefix` prunes
    /// directories that cannot contain matching keys (a listing passes
    /// its prefix) — the one source of truth for what an object is.
    /// The bucket-existence check happens here, before the first
    /// item; a mid-walk error is the stream's terminal item. Memory is
    /// O(1) beyond the walk's own cursor state. The stream is boxed
    /// (one allocation per walk) because the per-poll state machine
    /// borrows its own cursor across awaits — `!Unpin` by construction.
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bucket = %bucket)
    )]
    pub(crate) async fn walk_files_streaming(
        &self,
        bucket: &bucket::Name,
        key_prefix: &str,
    ) -> Result<WalkStream, Error> {
        // The lexical bucket mapping (path.rs): the validation supplements
        // — the reserved `.tinio` refusal (FR-020) and, on Windows, the
        // charset/aliasing refusal (F21) — answer the same clean error the
        // object ops give for a `con`/`nul` bucket instead of a raw IO 500
        // from the console device. The symlink policy stays inline below
        // (the follow-enabled resolution must not run here).
        let bucket_dir = path::bucket_path_lexical(&self.root, bucket)?;
        let bucket_meta = match fs::symlink_metadata(&bucket_dir).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Err(storage::no_such_bucket(bucket).into());
            }
            Err(err) => return Err(err.into()),
        };
        let mut visited: HashSet<PathBuf> = HashSet::new();
        let mut stack: Vec<(PathBuf, PathBuf)> = Vec::new();
        // A bucket dir that is itself a symlink/junction must not be
        // walked when following is disabled — `read_dir`/`canonicalize`
        // would otherwise list the target (outside the storage root);
        // an unseeded worklist yields an empty stream. A DANGLING link
        // (target gone) is no bucket at all — the same answer the
        // follow-enabled path gives through its canonicalize (F13): a
        // symlink that resolves to nothing must not answer an empty,
        // untruncated 200.
        if fsutil::is_symlink_or_reparse(&bucket_meta)
            && !self.follow_symlinks
            && let Err(err) = fs::canonicalize(&bucket_dir).await
        {
            if err.kind() == ErrorKind::NotFound {
                return Err(storage::no_such_bucket(bucket).into());
            }
            return Err(err.into());
        }
        if !(fsutil::is_symlink_or_reparse(&bucket_meta) && !self.follow_symlinks) {
            // Resolved directory targets already descended into — a
            // symlink pointing at an ancestor would otherwise loop
            // forever.
            if self.follow_symlinks {
                visited.insert(match fs::canonicalize(&bucket_dir).await {
                    Ok(canonical) => canonical,
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        return Err(storage::no_such_bucket(bucket).into());
                    }
                    Err(err) => return Err(err.into()),
                });
            } else {
                visited.insert(bucket_dir.clone());
            }
            stack.push((bucket_dir, PathBuf::new()));
        }
        let state = WalkState {
            follow_symlinks: self.follow_symlinks,
            key_prefix: key_prefix.to_string(),
            bucket: bucket.clone(),
            visited,
            stack,
            current: None,
            done: false,
        };
        Ok(Box::pin(stream::unfold(state, |mut state| async move {
            state.next_file().await.map(|file| (file, state))
        })))
    }
}

/// The iterative tree-walk state of [`FsListing::walk_files_streaming`]:
/// the worklist of `(directory, relative-prefix)` pairs plus the
/// in-progress directory's entry cursor. One file is emitted per stream
/// poll; the walk order is a directory's `read_dir` order, depth-first
/// over the worklist — never a key sort (the scanner needs no order).
/// The captured policy fields make the stream `'static` (no borrow of
/// the listing).
struct WalkState {
    /// Symlink-following policy (captured from the listing).
    follow_symlinks: bool,
    /// The prefix that prunes whole subtrees ("" = no pruning).
    key_prefix: String,
    /// The walked bucket (the vanished-bucket-dir error names it).
    bucket: bucket::Name,
    /// Resolved directory targets already descended into.
    visited: HashSet<PathBuf>,
    /// Worklist of `(directory, relative-prefix)` pairs.
    stack: Vec<(PathBuf, PathBuf)>,
    /// The in-progress directory's entry cursor.
    current: Option<(PathBuf, PathBuf, fs::ReadDir)>,
    /// A fatal error was already emitted — the stream terminates.
    done: bool,
}

impl WalkState {
    /// Emit the next file, or `None` when the walk is exhausted. A fatal
    /// error is emitted once, then the stream ends (its consumers propagate
    /// it immediately).
    async fn next_file(&mut self) -> Option<Result<WalkedFile, Error>> {
        if self.done {
            return None;
        }
        loop {
            if let Some((dir, prefix, mut entries)) = self.current.take() {
                loop {
                    let entry = match entries.next_entry().await {
                        Ok(Some(entry)) => entry,
                        Ok(None) => break, // directory exhausted
                        Err(err) => return self.fatal(err.into()),
                    };
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else {
                        continue; // non-UTF8 names cannot be object keys
                    };
                    if name == STATE_DIR_NAME {
                        continue; // reserved at any depth (FR-020)
                    }
                    let path = entry.path(); // one join per entry (the old code re-joined up to six times)
                    // P02/A1: classify before any stat — the platform
                    // split and the follow policy live in
                    // `fsutil::dir_entry_kind` (one home, shared with the
                    // bucket-name sweep); `stat` carries the metadata a
                    // leaf reuses as its object stat — Windows find-data,
                    // or a followed symlink/junction's probe (E2: a
                    // link-to-file leaf pays ONE stat; the reuse widens
                    // the probe→stat window by one stat's duration — the
                    // same snapshot class as a regular file's
                    // classification-then-stat). The vanished-entry skip
                    // and the fatal-error mapping stay here, per the
                    // walk's policy.
                    let kind = match fsutil::dir_entry_kind(&entry, self.follow_symlinks).await {
                        Ok(kind) => kind,
                        Err(err) if err.kind() == ErrorKind::NotFound => continue,
                        Err(err) => return self.fatal(err.into()),
                    };
                    let is_symlink = kind.is_symlink;
                    if is_symlink && !self.follow_symlinks {
                        continue;
                    }
                    let rel = prefix.join(name);
                    let key = rel.to_string_lossy().into_owned();
                    // Windows renders joined path separators as `\` (the
                    // only place a backslash can appear on Windows); on
                    // Unix a literal `\` in a file name is a legal key
                    // character and must survive intact.
                    #[cfg(windows)]
                    let key = key.replace('\\', "/");
                    if kind.is_dir {
                        // A symlinked directory may point at an ancestor
                        // (a cycle): never descend into a resolved target
                        // twice.
                        if is_symlink {
                            let target = match fs::canonicalize(&path).await {
                                Ok(target) => target,
                                Err(err) => return self.fatal(err.into()),
                            };
                            if !self.visited.insert(target) {
                                continue;
                            }
                        }
                        // Prefix pruning: a directory neither inside the
                        // requested prefix nor an ancestor of it can
                        // never hold a matching key — skip the whole
                        // subtree (a `max_keys=1` listing of a huge
                        // bucket walks only the prefix's directories).
                        if !self.key_prefix.is_empty() {
                            // `starts_with` subsumes the equality case (a
                            // directory whose key IS the prefix contains
                            // matching keys).
                            let inside = key.starts_with(&self.key_prefix);
                            // The ancestor test (the prefix lies under
                            // this directory) avoids the per-directory
                            // `format!` (item 5): the prefix's bytes
                            // match this key's bytes and continue with a
                            // `/` — zero-allocation.
                            let ancestor = self.key_prefix.len() > key.len()
                                && self.key_prefix.as_bytes().starts_with(key.as_bytes())
                                && self.key_prefix.as_bytes().get(key.len()) == Some(&b'/');
                            if !inside && !ancestor {
                                continue;
                            }
                        }
                        self.stack.push((path, rel));
                        continue;
                    }
                    let Ok(key) = object::key(key) else {
                        continue; // unrepresentable as an object key
                    };
                    if key.is_reserved() || key.is_folder_marker() {
                        continue;
                    }
                    // The object's stat. A regular file pays ONE stat
                    // (P02 — the old lstat probe plus this second stat
                    // were two syscalls; d_type already ruled out a
                    // link, so size/mtime and the stored identity come
                    // from a single `stat`); on Windows the free
                    // find-data metadata from the classification is
                    // reused outright — zero syscalls for a regular
                    // file. A followed symlink reuses the directory
                    // probe's target stat (E2 — one stat for the leaf);
                    // a dangling link (target gone) is skipped — one
                    // broken link must not fail the whole bucket walk.
                    let metadata = match kind.stat {
                        Some(stat) => stat,
                        None => match fs::metadata(&path).await {
                            Ok(metadata) => metadata,
                            // The dangling-link skip is the symlink branch's
                            // today; a vanished REGULAR file was a fatal
                            // error before this change and stays one.
                            Err(err) if err.kind() == ErrorKind::NotFound && is_symlink => {
                                continue;
                            }
                            Err(err) => return self.fatal(err.into()),
                        },
                    };
                    let modified = match metadata.modified() {
                        Ok(modified) => modified,
                        Err(err) => return self.fatal(err.into()),
                    };
                    self.current = Some((dir, prefix, entries));
                    return Some(Ok(WalkedFile {
                        key,
                        path,
                        size: metadata.len(),
                        mtime: modified,
                        // The identity is lazy (see `WalkedFile`): the
                        // stat is stored, the Windows open is deferred to
                        // the consumers that actually gate.
                        metadata: Metadata::clone(&metadata),
                    }));
                }
                // The directory is exhausted — fall through to the
                // worklist (its cursor is dropped here).
            }
            // Iterative tree walk (no async recursion): worklist of
            // `(directory, relative-prefix)` pairs.
            let Some((dir, prefix)) = self.stack.pop() else {
                self.done = true;
                return None;
            };
            let entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    if prefix.as_os_str().is_empty() {
                        // The bucket directory itself is gone.
                        return self.fatal(storage::no_such_bucket(&self.bucket).into());
                    }
                    continue; // a nested dir vanished mid-walk — skip it
                }
                Err(err) => return self.fatal(err.into()),
            };
            self.current = Some((dir, prefix, entries));
        }
    }

    /// Emit one fatal error and terminate the stream (F37 — the
    /// once-only `done` marker shared by every fatal branch).
    fn fatal<T>(&mut self, err: Error) -> Option<Result<T, Error>> {
        self.done = true;
        Some(Err(err))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{
        fs::{File, OpenOptions, create_dir, create_dir_all, metadata, read_dir, write},
        time::Duration,
    };

    use futures::StreamExt;
    use tokio::fs;

    use super::*;
    use crate::{
        _core::{
            ETag,
            pipeline::InlineRunner,
            storage::{
                DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE, Error::NoSuchBucket,
                ListObjectsParams, ObjectListing, ObjectOps,
            },
        },
        _store_redb::object_meta::Table,
        _util::testing,
        FsOptions, FsStorage, database,
        testutil::{fs_options, link_directory},
    };

    /// The listing fixture: the walk value (the walk tests drive
    /// `walk_files_streaming` on it) and the backend that serves the page.
    ///
    /// The page's entry point is `FsStorage::list_objects` — the ops engine's
    /// page, which walks through exactly this `FsListing` (`content.scan` is
    /// the walk) — so `list` drives the backend, the same call a client's
    /// ListObjects makes.
    struct TestListing {
        /// The tree walk the fixtures build (`walk_files_streaming`).
        listing: FsListing,
        /// The backend the page is served from.
        storage: FsStorage,
    }

    impl TestListing {
        /// Both halves over one root (the walk and the backend the page is
        /// served from).
        fn over(root: &Path, follow_symlinks: bool) -> Self {
            Self {
                listing: FsListing::new(
                    root,
                    follow_symlinks,
                    Arc::new(InlineRunner::default()),
                    Arc::new(InlineRunner::default()),
                    DEFAULT_META_BATCH_SIZE,
                    DEFAULT_META_BATCH_BYTES,
                ),
                storage: FsStorage::new(
                    root,
                    FsOptions {
                        follow_symlinks,
                        ..fs_options()
                    },
                )
                .unwrap(),
            }
        }

        /// One page of the backend's own listing entry point.
        async fn list(
            &self,
            params: &ListObjectsParams,
            _records_versions: bool,
        ) -> Result<ObjectListing, Error> {
            // The versioning flag is the backend's own read now (its
            // `list_objects` resolves the bucket state and hands it to the
            // engine body); the callers' argument stays so the fixtures read
            // the shape they always have.
            self.storage.list_objects(params.clone()).await
        }

        /// The streaming walk, delegated so the walk tests read the shape
        /// they always have.
        async fn walk_files_streaming(
            &self,
            bucket: &bucket::Name,
            key_prefix: &str,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<WalkedFile, Error>> + Send + 'static>>, Error>
        {
            self.listing.walk_files_streaming(bucket, key_prefix).await
        }
    }

    // --- the storage-level page (the ops engine's listing) ---

    /// The composed-ETag keep rule at the listing's own entry point: a stored
    /// `MD5-of-MD5s-N` ETag on the same-size file survives a **touch** (an
    /// mtime drift inside the jitter window), and the page heals the row it
    /// served — the fs half of the engine's composed-keep rule
    /// (`crates/tinio-fs/src/meta.rs`'s family pins it on the read path).
    #[tokio::test]
    async fn a_stored_composed_etag_survives_a_touch_on_the_page() {
        let (root, listing, b) = fixture();
        let file = root.path().join("data/a.txt");
        let before = metadata(&file).unwrap();
        let composed = testing::etag("5d41402abc4b2a76b9719d911017c592-2");
        listing
            .storage
            .meta_store()
            .set(
                &b,
                &object::key("a.txt").unwrap(),
                &composed,
                before.len(),
                before.modified().unwrap(),
                0,
            )
            .await
            .unwrap();
        // A touch: the same file, mtime pushed forward inside the window.
        let handle = File::options().write(true).open(&file).unwrap();
        handle
            .set_modified(before.modified().unwrap() + Duration::from_secs(30))
            .unwrap();
        drop(handle);
        let page = listing
            .list(&params(&b, "a", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(page.objects[0].etag, composed);
        assert!(matches!(page.objects[0].etag, ETag::Composed(_, 2)));
        // The page healed the row it served: the next read is a hit.
        let now = metadata(&file).unwrap();
        let record = listing
            .storage
            .meta_store()
            .get(&b, &object::key("a.txt").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(record.matches(now.len(), now.modified().unwrap()));
    }

    /// A same-size replacement that restores the mtime (`cp -p`, `rsync -a`)
    /// is not a touch: the file identity changed, so the page recomputes the
    /// content ETag rather than serving the old one (F01's hole, closed).
    #[tokio::test]
    async fn a_mtime_preserving_replacement_recomputes_the_etag_on_the_page() {
        let (root, listing, b) = fixture();
        let file = root.path().join("data/a.txt");
        let first = listing
            .list(&params(&b, "a", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(first.objects[0].etag, ETag::from_content(b"a.txt!"));
        let before = metadata(&file).unwrap();
        let replacement = root.path().join("data/replacement.txt");
        write(&replacement, b"a.txt?").unwrap();
        let handle = OpenOptions::new().write(true).open(&replacement).unwrap();
        handle.set_modified(before.modified().unwrap()).unwrap();
        drop(handle);
        fs::rename(&replacement, &file).await.unwrap();
        let page = listing
            .list(&params(&b, "a", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(page.objects[0].etag, ETag::from_content(b"a.txt?"));
    }

    /// A row whose stored shape is corrupt (a domain-invalid ETag) reads as
    /// absent, so the page recomputes the entry instead of failing the
    /// listing — the same self-heal the read path takes.
    #[tokio::test]
    async fn a_corrupt_row_self_heals_on_the_page() {
        let root = tempfile::tempdir().unwrap();
        // Corrupt the row BEFORE the backend opens the state database (redb's
        // file lock is exclusive per handle).
        {
            let db = database::open(&root.path().join(STATE_DIR_NAME))
                .unwrap()
                .db;
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = Table::open(&mut txn).unwrap();
                table
                    .insert(
                        ("data", "a.txt"),
                        ("not-an-etag", 1, 1, 0, "", "", "", "", "null", 0, false),
                    )
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        create_dir(root.path().join("data")).unwrap();
        write(root.path().join("data/a.txt"), b"a").unwrap();
        let listing = TestListing::over(root.path(), true);
        let b = bucket::name("data").unwrap();
        let page = listing
            .list(&params(&b, "a", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].etag, ETag::from_content(b"a"));
        // The entry is valid again.
        assert!(
            listing
                .storage
                .meta_store()
                .get(&b, &object::key("a.txt").unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }

    /// A five-key tree (two flat keys, two under `dir/`, one under
    /// `dir/sub/`), planted directly: the namespace exists without a create
    /// call, exactly the shape the walk tests want.
    fn fixture() -> (tempfile::TempDir, TestListing, bucket::Name) {
        let root = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        create_dir(root.path().join("data")).unwrap();
        for key in ["a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt", "dir/e.txt"] {
            let path = root.path().join("data").join(key);
            create_dir_all(path.parent().unwrap()).unwrap();
            write(path, format!("{key}!")).unwrap();
        }
        let listing = TestListing::over(root.path(), true);
        (root, listing, b)
    }

    fn params(
        b: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        start: Option<&str>,
        max: usize,
    ) -> ListObjectsParams {
        ListObjectsParams {
            bucket: b.clone(),
            prefix: prefix.into(),
            delimiter: delimiter.map(str::to_string),
            start_after: start.map(str::to_string),
            max_keys: max,
        }
    }

    #[tokio::test]
    async fn missing_bucket_is_no_such_bucket() {
        let (_root, listing, b) = fixture();
        let missing = bucket::name("ghost").unwrap();
        let err = listing
            .list(&params(&missing, "", None, None, 1000), false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err:?}");
        // The empty-page request is the case the walk cannot answer: the
        // engine returns the page **before** its first scan, so the listing's
        // own namespace probe is what keeps `max-keys = 0` on a missing bucket
        // `NoSuchBucket` (I1's pin — with `[s3] allow_zero_page_size = true`
        // this is reachable from the wire).
        let err = listing
            .list(&params(&missing, "", None, None, 0), false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err:?}");
        // And the probe does not change a real bucket's empty page.
        let empty = listing
            .list(&params(&b, "nothing/", None, None, 0), false)
            .await
            .unwrap();
        assert!(
            empty.objects.is_empty() && empty.common_prefixes.is_empty() && !empty.truncated,
            "{empty:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dangling_symlink_is_skipped_not_fatal() {
        let (root, listing, b) = fixture();
        // A link whose target does not exist must not fail the whole
        // bucket listing (or the scanner pass).
        symlink(
            root.path().join("nope.txt"),
            root.path().join("data/broken"),
        )
        .unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000), false)
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .objects
            .iter()
            .map(|o| o.key.as_ref().as_str())
            .collect();
        assert_eq!(keys.len(), 5, "{keys:?}");
        assert!(!keys.contains(&"broken"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_cycles_terminate() {
        let (root, listing, b) = fixture();
        // `loop` points at the bucket itself: without cycle detection
        // the walk would descend forever.
        symlink(".", root.path().join("data/loop")).unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000), false)
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .objects
            .iter()
            .map(|o| o.key.as_ref().as_str())
            .collect();
        assert!(keys.contains(&"a.txt"), "{keys:?}");
    }

    #[tokio::test]
    async fn symlink_entries_excluded_when_disabled() {
        // The rule is a unix one, so the body's block is gated: on Windows it
        // compiles away and only the tree below is planted.
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("outside.txt"), b"out")
            .await
            .unwrap();
        create_dir(root.path().join("data")).unwrap();
        write(root.path().join("data/real.txt"), b"real").unwrap();
        #[cfg(unix)]
        {
            let b = bucket::name("data").unwrap();
            symlink(
                root.path().join("outside.txt"),
                root.path().join("data/link.txt"),
            )
            .unwrap();
            // The page, through the backend's own entry point, under both
            // symlink policies: the link is a key only when following is on.
            let hidden = TestListing::over(root.path(), false);
            let page = hidden
                .list(&params(&b, "", None, None, 1000), false)
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert_eq!(keys, ["real.txt"], "{keys:?}");
            // The two fixtures share one root and a backend holds the state
            // database's exclusive lock: the first must be gone before the
            // second opens it (a backend per policy cannot share a root).
            drop(hidden);
            let followed = TestListing::over(root.path(), true);
            let page = followed
                .list(&params(&b, "", None, None, 1000), false)
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert_eq!(keys, ["link.txt", "real.txt"], "{keys:?}");
        }
    }

    #[tokio::test]
    async fn bucket_dir_symlink_not_walked_when_disabled() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), b"secret")
            .await
            .unwrap();
        link_directory(outside.path(), &root.path().join("data"));
        let listing = TestListing::over(root.path(), false);
        let b = bucket::name("data").unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000), false)
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .objects
            .iter()
            .map(|o| o.key.as_ref().as_str())
            .collect();
        assert!(
            keys.is_empty(),
            "must not list through a bucket-dir symlink: {keys:?}"
        );
        // The empty-page request: the bucket-existence probe the listing runs
        // for it is the walk's own, so a namespace the symlink policy hides is
        // the same empty answer here — never `NoSuchBucket` (which the direct
        // object ops do give for it, and which `ensure_bucket` would have given
        // here before the probe took the walk's shape).
        let empty = listing
            .list(&params(&b, "", None, None, 0), false)
            .await
            .unwrap();
        assert!(
            empty.objects.is_empty() && !empty.truncated,
            "the symlink-hidden bucket answers its empty page: {empty:?}"
        );
    }

    // --- the streaming walk (P2) ---

    /// Every file of the tree from an independent recursive walk,
    /// mirroring the object rules (no directories, no `.tinio` at any
    /// depth, no dangling links) — the stream's expected SET.
    fn files_under(root: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![(root.to_path_buf(), PathBuf::new())];
        while let Some((dir, prefix)) = stack.pop() {
            for entry in read_dir(&dir).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if name == STATE_DIR_NAME {
                    continue;
                }
                let rel = prefix.join(name);
                let file_type = entry.file_type().unwrap();
                if file_type.is_dir() {
                    stack.push((entry.path(), rel));
                    continue;
                }
                if file_type.is_symlink() && metadata(entry.path()).is_err() {
                    continue; // dangling — skipped by the walk
                }
                let key = rel.to_string_lossy().into_owned();
                #[cfg(windows)]
                let key = key.replace('\\', "/");
                out.push(key);
            }
        }
        out
    }

    #[tokio::test]
    async fn walk_stream_emits_files_in_read_dir_order() {
        // The stream is the scanner's walk: files come out one at a
        // time, in the directory's OWN enumeration order — never a key
        // sort (the scanner needs no order; the page's sort is the pager's
        // bounded heap, not the walk). The order is
        // pinned against a
        // fresh `read_dir` of the same static directory — deterministic
        // on the same filesystem (NTFS enumerates B-tree order, ext4
        // hash order; neither is a key sort, so a future sort-regression
        // changes the stream's order).

        let root = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        // Created in non-lexicographic order on purpose.
        for key in ["b.txt", "a.txt", "c.txt"] {
            fs::write(root.path().join("data").join(key), key)
                .await
                .unwrap();
        }
        let expected: Vec<String> = read_dir(root.path().join("data"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        let listing = TestListing::over(root.path(), true);
        let mut walked = listing.walk_files_streaming(&b, "").await.unwrap();
        let mut keys = Vec::new();
        while let Some(file) = walked.next().await {
            keys.push(file.unwrap().key.to_string());
        }
        assert_eq!(
            keys, expected,
            "the stream must follow read_dir order, unsorted"
        );
    }

    #[tokio::test]
    async fn walk_stream_collects_every_object_of_the_tree() {
        let root = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        for key in [
            "a.txt",
            "dir/b.txt",
            "dir/sub/c.txt",
            "dir/e.txt",
            "deep/x/y.txt",
        ] {
            let path = root.path().join("data").join(key);
            fs::create_dir_all(path.parent().unwrap()).await.unwrap();
            fs::write(path, format!("{key}!")).await.unwrap();
        }
        fs::create_dir_all(root.path().join("data/dir/.tinio"))
            .await
            .unwrap();
        fs::write(root.path().join("data/dir/.tinio/state"), b"x")
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/empty"))
            .await
            .unwrap();
        #[cfg(unix)]
        symlink(root.path().join("gone"), root.path().join("data/broken")).unwrap();
        let listing = TestListing::over(root.path(), true);
        let mut walked = listing.walk_files_streaming(&b, "").await.unwrap();
        let mut got = Vec::new();
        while let Some(file) = walked.next().await {
            got.push(file.unwrap().key.to_string());
        }
        let mut want = files_under(&root.path().join("data"));
        got.sort();
        want.sort();
        assert_eq!(got, want);
    }

    #[tokio::test]
    async fn walk_stream_missing_bucket_is_no_such_bucket() {
        let root = tempfile::tempdir().unwrap();
        let listing = TestListing::over(root.path(), true);
        let missing = bucket::name("ghost").unwrap();
        let Err(err) = listing.walk_files_streaming(&missing, "").await else {
            panic!("a missing bucket must error at stream construction");
        };
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dangling_bucket_symlink_is_no_such_bucket() {
        let root = tempfile::tempdir().unwrap();
        symlink(root.path().join("gone"), root.path().join("data")).unwrap();
        let listing = TestListing::over(root.path(), true);
        let b = bucket::name("data").unwrap();
        let err = listing
            .list(&params(&b, "", None, None, 1000), false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn duplicate_symlink_targets_descend_once() {
        let (root, listing, b) = fixture();
        symlink(root.path().join("data/dir"), root.path().join("data/link1")).unwrap();
        symlink(root.path().join("data/dir"), root.path().join("data/link2")).unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000), false)
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .objects
            .iter()
            .map(|o| o.key.as_ref().as_str())
            .collect();
        // Readdir order decides which link the walk meets first — the
        // invariant is the target's contents surface exactly once, not
        // under a specific link name.
        let link_keys: Vec<&str> = keys
            .iter()
            .filter(|k| k.starts_with("link1/") || k.starts_with("link2/"))
            .copied()
            .collect();
        assert_eq!(
            link_keys.len(),
            3,
            "the shared target must be listed through exactly one link: {keys:?}"
        );
        assert!(
            !link_keys
                .iter()
                .any(|k| k.starts_with("link1/") && k.starts_with("link2/")),
            "keys from both links: {keys:?}"
        );
    }

    #[tokio::test]
    async fn walk_stream_bucket_dir_vanishing_mid_walk_is_no_such_bucket() {
        // The bucket directory vanishing between stream construction and
        // the first poll is no bucket at all: the walk's initial
        // `read_dir` answers NotFound and the empty relative prefix maps
        // it to NoSuchBucket — never an empty, untruncated 200.
        let root = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        create_dir(root.path().join("data")).unwrap();
        write(root.path().join("data/a.txt"), "x").unwrap();
        let listing = TestListing::over(root.path(), true);
        let mut stream = listing.walk_files_streaming(&b, "").await.unwrap();
        fs::remove_dir_all(root.path().join("data")).await.unwrap();
        let first = stream.next().await.unwrap();
        assert!(
            matches!(first, Err(Error::Storage(NoSuchBucket(_)))),
            "a vanished bucket dir must fail the walk: {first:?}"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn dangling_bucket_junction_is_no_such_bucket() {
        // A bucket dir that is a DANGLING junction resolves to no
        // target: the walk answers NoSuchBucket with following disabled
        // (the unseeded-worklist path must not answer an empty 200) and
        // with following enabled (the canonicalize path, F13).
        for follow in [false, true] {
            let root = tempfile::tempdir().unwrap();
            link_directory(&root.path().join("gone-target"), &root.path().join("data"));
            let listing = TestListing::over(root.path(), follow);
            let b = bucket::name("data").unwrap();
            // The Ok side is the stream — never Debug — so the error is
            // extracted through `map(|_| ())`.
            let err = listing
                .walk_files_streaming(&b, "")
                .await
                .map(|_| ())
                .unwrap_err();
            assert!(
                matches!(err, Error::Storage(NoSuchBucket(_))),
                "dangling junction (follow={follow}): {err:?}"
            );
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn junction_inside_bucket_is_followed_and_cycles_terminate() {
        // With following enabled a junction inside a bucket is descended
        // (the files behind it are objects); a cycle (the bucket linked
        // to itself) terminates through the visited set.
        let root = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        create_dir_all(root.path().join("data/real/sub")).unwrap();
        write(root.path().join("data/real/sub/x.txt"), "x").unwrap();
        link_directory(
            &root.path().join("data/real"),
            &root.path().join("data/jlink"),
        );
        // A self-cycle: the bucket linked to itself.
        link_directory(&root.path().join("data"), &root.path().join("data/cycle"));
        let listing = TestListing::over(root.path(), true);
        let page = listing
            .list(&params(&b, "", None, None, 1000), false)
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .objects
            .iter()
            .map(|o| o.key.as_ref().as_str())
            .collect();
        assert_eq!(
            keys,
            ["jlink/sub/x.txt", "real/sub/x.txt"],
            "junction descended, cycle skipped: {keys:?}"
        );
    }
}
