//! Object listing over the storage root (task T043).
//!
//! A directory-tree walk (buckets → files) with prefix filtering,
//! delimiter-based grouping (common-prefix roll-up), and pagination per
//! S3 semantics (FR-004) — the streaming walk fed into the shared
//! bounded [`UnorderedPager`] engine: the page comes out in key order
//! with only the `max_keys + 1` smallest entries ever held (no
//! full-bucket collect and sort, P01).
//!
//! ETags are included: missing/stale entries of the emitted page are
//! recomputed through the **IO pipeline** (`etag::ComputeTask`, pipeline-spec.md
//! §3.2) and persisted in **write-pipeline batches** (`MetaWriteBatchTask`)
//! — the documented one-time full-content pass over externally-added
//! files, mitigated by the background scanner. Pagination happens on the
//! walked keys first (P3), so only the emitted page's keys are gated and
//! enqueued. `.tinio` entries are always skipped (FR-020); symlink entries
//! are excluded when `follow_symlinks` is disabled. No listing latency
//! bound is promised; listings remain correct and complete at all times.
//!
//! The list producer and the scanner walk the same tree through
//! [`FsListing::walk_files_streaming`] — the one source of truth for
//! what an object is, emitted one file at a time in walk order (no
//! full-bucket collection, no sort: the scanner needs no order,
//! pipeline-spec.md §3.7, and the pager needs only the page).
//!
//! This module also hosts the **streaming batch accumulator** shared with
//! the scanner producer: [`MetaBatchAccumulator`] groups computed entries
//! into write-pipeline batches, flushing on the `[storage.fs]
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
use futures::{
    FutureExt, Stream, StreamExt,
    stream::{self, FuturesUnordered},
};
use tokio::fs;

use crate::{
    _core::{
        ETag, bucket,
        object::{self, Info},
        pipeline::{self, Completion},
        storage::{self, ListObjectsParams, ObjectListing, UnorderedPager},
    },
    Error, etag, fsutil, meta, path,
    path::STATE_DIR_NAME,
    write_task::MetaWriteBatchTask,
};

/// The per-entry meta-batch size estimate (pipeline-spec.md Q5): a stored
/// row is ≈ 56 B plus the key bytes. The producers use it for the
/// `meta_batch_bytes` flush trigger.
pub(crate) const META_ENTRY_ESTIMATE_BYTES: u64 = 56;

/// One walked object file: the key, the path, and the size/mtime of the
/// walk's own stat. The identity is **lazy** ([`WalkedFile::identity`]):
/// unix reads it from the stored metadata (zero extra cost); Windows
/// opens the path on demand — a LIST gate pays the open only for the
/// emitted page entries (≤ `max_keys`), never once per walked file. The
/// identity is the producers' replacement detector (F01): a same-size
/// mtime-preserving replacement (`cp -p`, `rsync -a`) is invisible to
/// the size+mtime pair but changes the identity — a gate that never
/// consults it would serve the old ETag forever. The lazy form is as
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

/// The streaming meta-batch accumulator shared by the list and scanner
/// producers (pipeline-spec.md Q5): computed entries accumulate into the
/// current batch, which flushes once its entry count reaches
/// `meta_batch_size` **or** its estimated bytes reach `meta_batch_bytes`
/// (either trigger; estimate ≈ [`META_ENTRY_ESTIMATE_BYTES`] + key
/// length). One flush = one [`MetaWriteBatchTask`] = one write
/// transaction. The caller decides what to do with the returned
/// [`Completion`]: the list awaits it (Q2 final drain), the scanner drops
/// it (Q3b fire-and-forget).
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

    /// Fold one [`etag::Outcome`] into the batch (F38 — the shared Ok path
    /// of both producers' compute-result folds; the failure policies stay
    /// at the call sites). The batch entry uses the outcome's own
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
    /// producers flush a possibly empty accumulator after the last
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

/// Listing over one storage root (bucket dirs at the top level).
///
/// # Examples
///
/// ```rust
/// use std::{
///     fs::{create_dir, write},
///     sync::Arc,
/// };
///
/// use tinio_core::{
///     bucket,
///     pipeline::InlineRunner,
///     storage::{DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE, ListObjectsParams},
/// };
/// use tinio_fs::{FsListing, meta};
/// use tokio::runtime::Runtime;
///
/// let root = tempfile::tempdir().unwrap();
/// let state = tempfile::tempdir().unwrap();
/// let store = meta::store(state.path()).unwrap();
/// let listing = FsListing::new(
///     root.path(),
///     store,
///     true,
///     Arc::new(InlineRunner::default()),
///     Arc::new(InlineRunner::default()),
///     DEFAULT_META_BATCH_SIZE,
///     DEFAULT_META_BATCH_BYTES,
/// );
/// Runtime::new().unwrap().block_on(async {
///     let b = bucket::name("data").unwrap();
///     create_dir(root.path().join("data")).unwrap();
///     write(root.path().join("data/a.txt"), b"a").unwrap();
///     create_dir(root.path().join("data/dir")).unwrap();
///     write(root.path().join("data/dir/b.txt"), b"b").unwrap();
///     let page = listing
///         .list(&ListObjectsParams {
///             bucket: b,
///             prefix: String::new(),
///             delimiter: Some("/".into()),
///             start_after: None,
///             max_keys: 1000,
///         })
///         .await
///         .unwrap();
///     assert_eq!(page.objects.len(), 1);
///     assert_eq!(page.common_prefixes, ["dir/"]);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct FsListing {
    /// Storage root (bucket dirs at the top level).
    root: PathBuf,
    /// The meta store (ETags; recompute-on-stale during the walk).
    meta: meta::Store,
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

impl FsListing {
    /// Create a listing over `root`. The pipelines are mandatory (P4) —
    /// the cold list path enqueues into them; offline contexts pass
    /// [`pipeline::InlineRunner`]. `meta_batch_size`/`meta_batch_bytes`
    /// are the streaming flush thresholds (Q5, `[storage.fs]`).
    pub fn new(
        root: &Path,
        meta: meta::Store,
        follow_symlinks: bool,
        io_pipeline: Arc<dyn pipeline::Runner<etag::Result>>,
        db_pipeline: Arc<dyn pipeline::Runner<Result<(), Error>>>,
        meta_batch_size: u16,
        meta_batch_bytes: u32,
    ) -> Self {
        Self {
            root: root.to_path_buf(),
            meta,
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

    /// List one page of objects per S3 semantics (prefix, delimiter,
    /// pagination). `NoSuchBucket` when the bucket does not exist.
    ///
    /// The page's ETags resolve through the pipelines (pipeline-spec.md
    /// §3.2): a gating load reads the page's keys in **one** read
    /// transaction (R1), matching entries are served from the store
    /// (P6 — no worker for a cache hit), and the missing/stale entries go
    /// through the IO pipeline as `etag::ComputeTask`s (concurrency = the
    /// pipeline's workers, Q4). The results stream back in COMPLETION
    /// order (pipeline-spec.md §3.2 — no head-of-line blocking) into
    /// write-pipeline batches (`MetaBatchAccumulator`, Q5) and — after
    /// ALL batches are enqueued — the batch completions are awaited (the
    /// final drain, Q2: every batch committed before the page is
    /// answered).
    ///
    /// The page is selected incrementally during the walk (the bounded
    /// [`UnorderedPager`] — P01): only the `max_keys + 1` smallest
    /// entries are held, never a full-bucket collect and sort.
    pub async fn list(&self, params: &ListObjectsParams) -> Result<ObjectListing, Error> {
        // The engine's empty-page contract, answered after the bucket
        // existence check (the walk's own await) but before the tree
        // walk: a `max_keys = 0` request never pays the full-bucket stat
        // sweep — it still answers NoSuchBucket for a missing bucket.
        if params.max_keys == 0 {
            // The stream is built purely for the existence probe and
            // dropped un-polled — `let _` so the `#[must_use]` stream
            // does not warn on every build (F10).
            let _ = self
                .walk_files_streaming(&params.bucket, &params.prefix)
                .await?;
            return Ok(ObjectListing {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                truncated: false,
                next_start_after: None,
            });
        }
        // Paginate on the walked keys first — grouping, the marker skip,
        // and the truncation probe need no per-object I/O. ETags are then
        // resolved for the emitted page only: a `max_keys=1` request
        // costs one meta read, not one per object in the bucket (P3).
        // The walk is the STREAMING form fed into the bounded
        // `UnorderedPager`: memory and the page sort drop to O(max_keys)
        // (P01 — the old collect + sort paid O(M) memory and an O(M log
        // M) sort per page). Syscalls stay O(M) per page — `read_dir`
        // cannot seek. The `starts_with` re-filter stays: the walk's
        // directory-level pruning does not guarantee every walked file
        // matches the prefix. The keyed `offer_keyed` compares the
        // borrowed key and materializes the order String only for
        // entries that enter the heap — O(page) allocations, not
        // O(entries).
        let mut walk = self
            .walk_files_streaming(&params.bucket, &params.prefix)
            .await?;
        let mut pager = UnorderedPager::new(
            &params.prefix,
            params.delimiter.as_deref(),
            params.start_after.as_deref(),
            params.max_keys,
            |w: &WalkedFile| w.key.as_ref(),
        );
        while let Some(file) = walk.next().await {
            let file = file?; // a mid-walk error is the stream's terminal item — propagates
            if file.key.starts_with(&params.prefix) {
                pager.offer_keyed(file);
            }
        }
        let (page, common_prefixes, truncated, next) = pager.finish();
        // R1: the page's keys in ONE read transaction; each slot aligns
        // with `get()` — a missing or corrupt entry reports `None`
        // (recompute + rewrite, self-healing, P2). The slots answer in
        // request order, so they are **index-aligned with the page** —
        // the assembly below never looks up by key, and the keys are
        // passed by reference (no key `Vec`, no per-row key clones; the
        // page's own key order IS the assembly order, item 3).
        let gated = self
            .meta
            .load_entries(&params.bucket, page.iter().map(|w| &w.key))
            .await?;
        debug_assert_eq!(
            gated.len(),
            page.len(),
            "the gating load is 1:1 with the page"
        );

        // One slot per page entry: `None` = missing/stale (compute
        // pending) or a vanished file; `Some(etag)` = the served entry (a
        // matching hit from the gate, or a compute result). The identity
        // lives only in the batch entries (F36 — the slot never needed it).
        let mut results: Vec<Option<ETag>> = vec![None; page.len()];
        let mut compute_done = FuturesUnordered::new();
        let mut batches: Vec<Completion<Result<(), Error>>> = Vec::new();
        let mut accumulator = MetaBatchAccumulator::new(
            &params.bucket,
            self.meta.clone(),
            &self.db_pipeline,
            self.meta_batch_size,
            self.meta_batch_bytes,
        );

        // P6: the in-memory matches gate — a matching entry is served
        // from the store and never enqueued (no worker, no IO task). The
        // gate consults the walked file identity (F01) — a same-size
        // mtime-preserving replacement is a gate miss, never a stale
        // serve.
        for (i, stored) in gated.into_iter().enumerate() {
            let walked = &page[i];
            match &stored {
                Some(stored)
                    if meta::entry_matches(
                        stored.size,
                        stored.mtime,
                        stored.file_identity,
                        walked.size,
                        walked.mtime,
                        walked.identity(),
                    ) =>
                {
                    results[i] = Some(stored.etag.clone());
                    continue;
                }
                _ => {} // missing or stale → compute through the IO pipeline
            }
            let task = etag::ComputeTask {
                key: page[i].key.clone(),
                path: walked.path.clone(),
                size: walked.size,
                stored,
                follow_symlinks: self.follow_symlinks,
            };
            // Enqueue waits only for queue capacity (backpressure) — the
            // completions resolve out of order below.
            compute_done.push(self.io_pipeline.enqueue(Box::new(task)).await?);
            // F17: drain whatever has already resolved while still
            // enqueueing — a slow in-flight hash must not
            // head-of-line-block the rest of the page's enqueues (the
            // scanner producer drains the same way).
            while let Some(Some(done)) = compute_done.next().now_or_never() {
                self.fold_compute(done, &page, &mut results, &mut accumulator, &mut batches)
                    .await?;
            }
        }

        // Stream-flush (Q5): the results stream in COMPLETION ORDER (§3.2
        // — a slow file never head-of-line-blocks the page); each lands
        // in the current batch, which flushes when its entry count or
        // estimated bytes reach the thresholds. A failed compute fails
        // the whole listing (the existing per-entry-failure semantics);
        // a vanished file (concurrent delete) skips the entry.
        while let Some(done) = compute_done.next().await {
            self.fold_compute(done, &page, &mut results, &mut accumulator, &mut batches)
                .await?;
        }
        if let Some(done) = accumulator.flush().await? {
            batches.push(done);
        }
        // Q2: the final drain — every enqueued batch commits before the
        // page is answered.
        for done in batches {
            done.await??;
        }
        // The page entries in page (key) order — the index-aligned
        // assembly replaces the old HashMap + `sort_by` (item 3: page
        // order IS key order; a vanished file's slot stays `None` and
        // the entry is skipped).
        let mut objects: Vec<Info> = Vec::with_capacity(page.len());
        // The page is consumed here — the assembly is its last use, so
        // the walked keys and paths are moved, not cloned.
        for (walked, result) in page.into_iter().zip(&results) {
            if let Some(etag) = result {
                objects.push(Info {
                    key: walked.key,
                    size: walked.size,
                    last_modified: walked.mtime,
                    etag: etag.clone(),
                });
            }
        }
        // T01: the pager's `(truncated, next)` propagate unconditionally —
        // a page whose entries ALL vanished between the walk and the hash
        // (mass concurrent deletion) still reports the resume marker. The
        // marker is exclusive-after and does not require the marker key to
        // exist: the client resumes past it and either finds the probe
        // entry that set `truncated` (still live — a truncated page always
        // has one) or an empty untruncated page. Either way the sweep
        // terminates; suppressing the marker here would hide live keys
        // (an F15-era guard did exactly that — the deleted keys are gone,
        // but the truncated page's probe is a key the client must still
        // see).
        Ok(ObjectListing {
            objects,
            common_prefixes,
            truncated,
            next_start_after: next,
        })
    }

    /// Fold one resolved compute completion into the page (the listing's
    /// failure policy: a vanished file skips the entry, any other failure
    /// fails the page). The Ok path is the shared
    /// [`MetaBatchAccumulator::push_outcome`] (F38 — the scanner's
    /// `fold_outcome` keeps its own failure policy).
    async fn fold_compute(
        &self,
        done: Result<etag::Result, pipeline::Error>,
        page: &[WalkedFile],
        results: &mut [Option<ETag>],
        accumulator: &mut MetaBatchAccumulator<'_>,
        batches: &mut Vec<Completion<Result<(), Error>>>,
    ) -> Result<(), Error> {
        let outcome = match done {
            Ok(outcome) => outcome,
            Err(err) => return Err(err.into()),
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(Error::Io(err)) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };
        // The result key is one of the page's keys (the task carried
        // it) — a binary search on the page (key order) finds its
        // slot, replacing the old HashMap lookup (item 3).
        let idx = match page.binary_search_by(|w| w.key.as_ref().cmp(outcome.key.as_ref())) {
            Ok(idx) => idx,
            // Unreachable — the task's key is a page key (defensive;
            // a foreign key is dropped like the old HashMap path).
            Err(_) => return Ok(()),
        };
        results[idx] = Some(outcome.etag.clone());
        if let Some(done) = accumulator.push_outcome(outcome).await? {
            batches.push(done);
        }
        Ok(())
    }

    /// The **streaming** form of the walk: every object file of a bucket,
    /// one at a time, in walk order (a directory's `read_dir` order,
    /// worklist-depth-first — never a key sort; the scanner needs no
    /// order, pipeline-spec.md §3.7, and the list producer paginates
    /// through the bounded `UnorderedPager`, which holds only the
    /// page). Directories are never objects, `.tinio` entries are
    /// skipped at any depth (FR-020), symlink entries are excluded when
    /// `follow_symlinks` is disabled, and `key_prefix` prunes
    /// directories that cannot contain matching keys (a listing passes
    /// its prefix) — the one source of truth for what an object is.
    /// The bucket-existence check happens here, before the first
    /// item; a mid-walk error is the stream's terminal item. Memory is
    /// O(1) beyond the walk's own cursor state. The stream is boxed
    /// (one allocation per walk) because the per-poll state machine
    /// borrows its own cursor across awaits — `!Unpin` by construction.
    pub(crate) async fn walk_files_streaming(
        &self,
        bucket: &bucket::Name,
        key_prefix: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WalkedFile, Error>> + Send + 'static>>, Error>
    {
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
    /// error is emitted once, then the stream ends (the producers
    /// propagate it immediately).
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
        fs::{File, OpenOptions, create_dir, create_dir_all, metadata, read, read_dir, write},
        time::{Duration, Instant},
    };

    use tokio::fs;

    use super::*;
    use crate::{
        _core::{
            pipeline::{Error::Dropped, InlineRunner},
            storage::{
                DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE, Error::NoSuchBucket,
                ListObjectsParams,
            },
        },
        _util::testing,
        database,
        database::ObjectMetaTable,
        testutil::{GatedRunner, LossyRunner, PacedRunner, files, link_directory, wait_for},
    };

    /// The standard test listing: the fs defaults plus the mandatory
    /// inline pipelines (P4/Q1).
    fn listing(root: &Path, store: meta::Store, follow_symlinks: bool) -> FsListing {
        listing_with(
            root,
            store,
            follow_symlinks,
            Arc::new(InlineRunner::default()),
            Arc::new(InlineRunner::default()),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        )
    }

    /// A listing over explicit pipelines and flush thresholds (the
    /// producer tests observe the pipelines).
    fn listing_with(
        root: &Path,
        store: meta::Store,
        follow_symlinks: bool,
        io: Arc<dyn pipeline::Runner<etag::Result>>,
        db: Arc<dyn pipeline::Runner<Result<(), Error>>>,
        batch_size: u16,
        batch_bytes: u32,
    ) -> FsListing {
        FsListing::new(
            root,
            store,
            follow_symlinks,
            io,
            db,
            batch_size,
            batch_bytes,
        )
    }

    fn fixture() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        FsListing,
        bucket::Name,
    ) {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        create_dir(root.path().join("data")).unwrap();
        for key in ["a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt", "dir/e.txt"] {
            let path = root.path().join("data").join(key);
            create_dir_all(path.parent().unwrap()).unwrap();
            write(path, format!("{key}!")).unwrap();
        }
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), true);
        (root, state, listing, b)
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
        let (_, _, listing, _) = fixture();
        let missing = bucket::name("ghost").unwrap();
        let err = listing
            .list(&params(&missing, "", None, None, 1000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dangling_symlink_is_skipped_not_fatal() {
        let (root, _state, listing, b) = fixture();
        // A link whose target does not exist must not fail the whole
        // bucket listing (or the scanner pass).
        symlink(
            root.path().join("nope.txt"),
            root.path().join("data/broken"),
        )
        .unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000))
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
        let (root, _state, listing, b) = fixture();
        // `loop` points at the bucket itself: without cycle detection
        // the walk would descend forever.
        symlink(".", root.path().join("data/loop")).unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000))
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
        let (root, _state, _, _b) = fixture();
        fs::write(root.path().join("outside.txt"), b"out")
            .await
            .unwrap();
        #[cfg(unix)]
        {
            let state = _state;
            let b = _b;
            symlink(
                root.path().join("outside.txt"),
                root.path().join("data/link.txt"),
            )
            .unwrap();
            let no_link = listing(root.path(), meta::store(state.path()).unwrap(), false);
            let page = no_link
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            assert!(!page.objects.iter().any(|o| o.key.as_ref() == "link.txt"));
            drop(no_link);
            let with_link = listing(root.path(), meta::store(state.path()).unwrap(), true);
            let page = with_link
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            assert!(page.objects.iter().any(|o| o.key.as_ref() == "link.txt"));
        }
    }

    #[tokio::test]
    async fn bucket_dir_symlink_not_walked_when_disabled() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), b"secret")
            .await
            .unwrap();
        link_directory(outside.path(), &root.path().join("data"));
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), false);
        let b = bucket::name("data").unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000))
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
        // sort (the scanner needs no order; the list producer's page
        // sort is the pager's bounded heap, not the walk). The order is
        // pinned against a
        // fresh `read_dir` of the same static directory — deterministic
        // on the same filesystem (NTFS enumerates B-tree order, ext4
        // hash order; neither is a key sort, so a future sort-regression
        // changes the stream's order).

        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
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
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), true);
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
        let state = tempfile::tempdir().unwrap();
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
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), true);
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
        let state = tempfile::tempdir().unwrap();
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), true);
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
        let state = tempfile::tempdir().unwrap();
        symlink(root.path().join("gone"), root.path().join("data")).unwrap();
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), true);
        let b = bucket::name("data").unwrap();
        let err = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn duplicate_symlink_targets_descend_once() {
        let (root, _state, listing, b) = fixture();
        symlink(root.path().join("data/dir"), root.path().join("data/link1")).unwrap();
        symlink(root.path().join("data/dir"), root.path().join("data/link2")).unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000))
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

    // --- the pipeline producers (pipeline-spec.md task 4) ---

    /// A bucket with `n` files `f00.txt..`, each with distinct content.
    /// The meta store lives under the caller's `root` tempdir (the state
    /// dir must outlive the test).
    fn files_fixture(root: &Path, n: usize) -> (meta::Store, bucket::Name) {
        let state = root.join("state");
        // The shared producer fixture (F39) — this test adds its own
        // state store.
        files(root, n);
        (meta::store(&state).unwrap(), bucket::name("data").unwrap())
    }

    /// The etag of one fixture file.
    fn file_etag(root: &Path, key: &str) -> crate::_core::ETag {
        ETag::from_content(&read(root.join("data").join(key)).unwrap())
    }

    #[tokio::test]
    async fn cold_list_writes_one_batch_per_flush_threshold() {
        let root = tempfile::tempdir().unwrap();
        let (_, b) = files_fixture(root.path(), 5);
        for (size, expected) in [(2u16, 3usize), (1, 5)] {
            let store = meta::store(&root.path().join(format!("state{size}"))).unwrap();
            let db = PacedRunner::<Result<(), Error>>::new(1, 8, Duration::ZERO);
            let listing = listing_with(
                root.path(),
                store.clone(),
                true,
                Arc::new(InlineRunner::default()),
                db.clone(),
                size,
                DEFAULT_META_BATCH_BYTES,
            );
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            assert_eq!(page.objects.len(), 5);
            assert_eq!(
                db.enqueued(),
                expected,
                "batch size {size}: one batch per flush threshold"
            );
            // Every entry landed (the batch tasks ran inline on the
            // paced worker).
            assert_eq!(store.walk(&b).await.unwrap().len(), 5);
        }
    }

    #[tokio::test]
    async fn hot_list_enqueues_nothing() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 3);
        let io = PacedRunner::<etag::Result>::new(1, 8, Duration::ZERO);
        let db = PacedRunner::<Result<(), Error>>::new(1, 8, Duration::ZERO);
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            io.clone(),
            db.clone(),
            1,
            DEFAULT_META_BATCH_BYTES,
        );
        listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(io.enqueued(), 3);
        assert_eq!(db.enqueued(), 3);

        let page = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 3);
        assert_eq!(io.enqueued(), 3, "hot path: no compute tasks (P6)");
        assert_eq!(db.enqueued(), 3, "hot path: no write transactions");
        for info in &page.objects {
            assert_eq!(info.etag, file_etag(root.path(), info.key.as_ref()));
        }
    }

    #[tokio::test]
    async fn pagination_happens_before_enqueue() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 3);
        let io = PacedRunner::<etag::Result>::new(1, 8, Duration::ZERO);
        let db = PacedRunner::<Result<(), Error>>::new(1, 8, Duration::ZERO);
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            io.clone(),
            db.clone(),
            1,
            DEFAULT_META_BATCH_BYTES,
        );
        let page = listing.list(&params(&b, "", None, None, 1)).await.unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(io.enqueued(), 1, "only the emitted page is enqueued (P3)");
        assert_eq!(db.enqueued(), 1);
        assert!(page.truncated);

        // The second page rolls over with its own single enqueue.
        let resume = page.next_start_after.unwrap();
        listing
            .list(&params(&b, "", None, Some(&resume), 1))
            .await
            .unwrap();
        assert_eq!(io.enqueued(), 2);
    }

    #[tokio::test]
    async fn io_concurrency_equals_the_workers() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 4);
        let io = PacedRunner::<etag::Result>::new(2, 8, Duration::from_millis(40));
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            io.clone(),
            Arc::new(InlineRunner::default()),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        );
        listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(
            io.max_in_run(),
            2,
            "four tasks over two workers must run two at a time"
        );

        // A fresh store (the first listing populated the previous one
        // — a hot list enqueues nothing).
        let store1 = meta::store(&root.path().join("state1")).unwrap();
        let io1 = PacedRunner::<etag::Result>::new(1, 8, Duration::from_millis(40));
        let listing1 = listing_with(
            root.path(),
            store1,
            true,
            io1.clone(),
            Arc::new(InlineRunner::default()),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        );
        listing1
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(io1.max_in_run(), 1, "one worker serializes");
    }

    #[tokio::test]
    async fn slow_db_pipeline_backpressures_the_producer() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 3);
        let db = PacedRunner::<Result<(), Error>>::new(1, 1, Duration::from_millis(150));
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            Arc::new(InlineRunner::default()),
            db.clone(),
            1,
            DEFAULT_META_BATCH_BYTES,
        );
        let started = Instant::now();
        let page = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(page.objects.len(), 3);
        // Capacity 1, one worker: batch 2's enqueue does not block —
        // the slot frees when the worker dequeues batch 1 (the queue
        // slot, not the worker, is the backpressure bound). The
        // enqueue that truly blocks is batch 3's (the slot holds
        // batch 2 until batch 1's ≈ 150 ms run ends); that block plus
        // the remaining serial drain meets the elapsed bound below.
        assert!(
            elapsed >= Duration::from_millis(250),
            "the producer must block on the full DB queue: {elapsed:?}"
        );
        assert_eq!(store.walk(&b).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn vanished_file_skips_the_entry() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 1);
        let io = GatedRunner::<etag::Result>::new(1, 8);
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            io.clone(),
            Arc::new(InlineRunner::default()),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        );
        let listing2 = listing.clone();
        let b2 = b.clone();
        let page =
            tokio::spawn(async move { listing2.list(&params(&b2, "", None, None, 1000)).await });
        wait_for(|| io.enqueued() == 1).await;
        // The walk is done (the task is parked); delete the file in
        // the walk-to-hash window.
        fs::remove_file(root.path().join("data/f00.txt"))
            .await
            .unwrap();
        io.open_gate();
        let page = page.await.unwrap().unwrap();
        assert!(
            page.objects.is_empty(),
            "the vanished file must be skipped, not fail the page"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_compute_fails_the_list() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 1);
        let io = GatedRunner::<etag::Result>::new(1, 8);
        // follow=false: the swapped-in link is REFUSED by the nofollow
        // open (R3 — PermissionDenied), which must fail the listing. A
        // follow-enabled listing would instead resolve the dangling
        // link to NotFound and skip the vanished object (fold_compute's
        // NotFound short-circuit) — correct snapshot semantics, not the
        // escape signal this test drives.
        let listing = listing_with(
            root.path(),
            store.clone(),
            false,
            io.clone(),
            Arc::new(InlineRunner::default()),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        );
        let listing2 = listing.clone();
        let b2 = b.clone();
        let page =
            tokio::spawn(async move { listing2.list(&params(&b2, "", None, None, 1000)).await });
        wait_for(|| io.enqueued() == 1).await;
        let path = root.path().join("data/f00.txt");
        fs::remove_file(&path).await.unwrap();
        symlink(root.path().join("gone"), &path).unwrap();
        io.open_gate();
        let err = page.await.unwrap().unwrap_err();
        assert!(
            matches!(err, Error::Io(ref e) if e.kind() == ErrorKind::PermissionDenied),
            "a swapped-in symlink must fail the list: {err:?}"
        );
    }

    #[tokio::test]
    async fn lost_batches_error_the_list_and_self_heal_next_pass() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 2);
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            Arc::new(InlineRunner::default()),
            Arc::new(LossyRunner),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        );
        let err = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Pipeline(Dropped)),
            "a lost batch must fail the list: {err:?}"
        );
        assert!(store.walk(&b).await.unwrap().is_empty());

        // The next pass (healthy pipeline) recomputes and persists.
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            Arc::new(InlineRunner::default()),
            Arc::new(InlineRunner::default()),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        );
        let page = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(store.walk(&b).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn composed_etag_kept_by_the_producer_on_identity_less_storage() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 1);
        let file = root.path().join("data/f00.txt");
        let metadata = fs::metadata(&file).await.unwrap();
        let composed = testing::etag("5d41402abc4b2a76b9719d911017c592-2");
        store
            .set(
                &b,
                &object::key("f00.txt").unwrap(),
                &composed,
                metadata.len(),
                metadata.modified().unwrap(),
                0,
            )
            .await
            .unwrap();
        // A touch: same file, mtime pushed forward.
        let handle = File::options().write(true).open(&file).unwrap();
        handle
            .set_modified(metadata.modified().unwrap() + Duration::from_secs(30))
            .unwrap();
        drop(handle);
        let now = fs::metadata(&file).await.unwrap();
        let listing = listing(root.path(), store.clone(), true);
        let page = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(page.objects[0].etag, composed);
        assert!(matches!(page.objects[0].etag, ETag::Composed(_, 2)));
        // The batch refreshed the entry — the next list is a hit.
        let record = store
            .get(&b, &object::key("f00.txt").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(record.matches(now.len(), now.modified().unwrap()));
    }

    #[tokio::test]
    async fn mtime_preserving_replacement_recomputes_the_etag() {
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 1);
        let file = root.path().join("data/f00.txt");
        let listing = listing(root.path(), store.clone(), true);
        let page = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(page.objects[0].etag, ETag::from_content(b"payload 0"));
        // Replace with a NEW file, same size, mtime restored.
        let metadata = fs::metadata(&file).await.unwrap();
        let replacement = root.path().join("data/replacement.txt");
        fs::write(&replacement, b"payload 9").await.unwrap();
        let handle = OpenOptions::new().write(true).open(&replacement).unwrap();
        handle.set_modified(metadata.modified().unwrap()).unwrap();
        drop(handle);
        fs::rename(&replacement, &file).await.unwrap();
        let page = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(page.objects[0].etag, ETag::from_content(b"payload 9"));
    }

    #[tokio::test]
    async fn page_whose_entries_all_vanish_keeps_the_resume_marker() {
        // T01: a page whose entries ALL vanished between the walk and
        // the hash (mass concurrent deletion) still reports the resume
        // marker — the marker is exclusive-after and needs no live key:
        // resuming past it either finds the truncation probe (still
        // live — `truncated` was set by a real key) or an empty
        // untruncated page, so the sweep terminates either way. The old
        // F15 guard suppressed the marker over a dead page, which hid
        // exactly those live keys from the client.
        let root = tempfile::tempdir().unwrap();
        let (store, b) = files_fixture(root.path(), 3);
        let io = GatedRunner::<etag::Result>::new(1, 8);
        let listing = listing_with(
            root.path(),
            store.clone(),
            true,
            io.clone(),
            Arc::new(InlineRunner::default()),
            DEFAULT_META_BATCH_SIZE,
            DEFAULT_META_BATCH_BYTES,
        );
        let listing2 = listing.clone();
        let b2 = b.clone();
        let page =
            tokio::spawn(async move { listing2.list(&params(&b2, "", None, None, 2)).await });
        wait_for(|| io.enqueued() == 2).await;
        // Delete every file in the walk-to-hash window.
        for i in 0..3 {
            fs::remove_file(root.path().join("data").join(format!("f{i:02}.txt")))
                .await
                .unwrap();
        }
        io.open_gate();
        let page = page.await.unwrap().unwrap();
        assert!(page.objects.is_empty());
        assert!(
            page.truncated,
            "the probe entry set truncation, and it may still be live: {page:?}"
        );
        assert_eq!(page.next_start_after.as_deref(), Some("f01.txt"));
    }

    #[tokio::test]
    async fn corrupt_entry_self_heals_through_the_producer() {
        let root = tempfile::tempdir().unwrap();
        // Corrupt the row BEFORE the store opens (redb's file lock is
        // exclusive per handle).
        let state = tempfile::tempdir().unwrap();
        {
            let db = database::open(state.path()).unwrap().db;
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = ObjectMetaTable::open(&mut txn).unwrap();
                table
                    .insert(("data", "f00.txt"), ("not-an-etag", 1, 1, 0))
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        let b = bucket::name("data").unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        fs::write(root.path().join("data/f00.txt"), b"payload 0")
            .await
            .unwrap();
        let store = meta::store(state.path()).unwrap();
        let listing = listing(root.path(), store.clone(), true);
        let page = listing
            .list(&params(&b, "", None, None, 1000))
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].etag, ETag::from_content(b"payload 0"));
        // The entry is valid again.
        assert!(
            store
                .get(&b, &object::key("f00.txt").unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn walk_stream_bucket_dir_vanishing_mid_walk_is_no_such_bucket() {
        // The bucket directory vanishing between stream construction and
        // the first poll is no bucket at all: the walk's initial
        // `read_dir` answers NotFound and the empty relative prefix maps
        // it to NoSuchBucket — never an empty, untruncated 200.
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        create_dir(root.path().join("data")).unwrap();
        write(root.path().join("data/a.txt"), "x").unwrap();
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), true);
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
            let state = tempfile::tempdir().unwrap();
            link_directory(&root.path().join("gone-target"), &root.path().join("data"));
            let listing = listing(root.path(), meta::store(state.path()).unwrap(), follow);
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
        let state = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        create_dir_all(root.path().join("data/real/sub")).unwrap();
        write(root.path().join("data/real/sub/x.txt"), "x").unwrap();
        link_directory(
            &root.path().join("data/real"),
            &root.path().join("data/jlink"),
        );
        // A self-cycle: the bucket linked to itself.
        link_directory(&root.path().join("data"), &root.path().join("data/cycle"));
        let listing = listing(root.path(), meta::store(state.path()).unwrap(), true);
        let page = listing
            .list(&params(&b, "", None, None, 1000))
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
