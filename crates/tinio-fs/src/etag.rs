//! The IO-pipeline ETag computation task (pipeline-spec.md §3.1, task 2).
//!
//! `ComputeTask` computes the ETag of one object file over
//! `tokio::fs` (Q4 — async: 64 KiB bounded streaming reads, incremental
//! MD5; the file IO runs on the tokio blocking pool, and one task still
//! occupies one worker thread, so the hash concurrency and open
//! file-handle count stay bounded by the IO pipeline's worker count).
//! The result is [`Result`] —
//! [`Outcome`]: the key, the (possibly kept) ETag, and the
//! hash-time metadata (size, mtime, identity) the persisted row must
//! describe — sent through [`pipeline::Completion`]. Matching entries never reach this task: the
//! producer gates on `meta::entry_matches` in memory and only
//! enqueues missing/stale keys (pipeline-spec.md §3.2). The task
//! carries the stored entry when one exists and shares the
//! `ensure_etag` keep decision (P1, `meta::composed_keep` — one home
//! for the rule): a composed `MD5-of-MD5s-N` entry on the same file is
//! kept only when the platform identity matches and the mtime is
//! unchanged (the jitter window is the identity-less fallback), so an
//! in-place same-size rewrite is never served a stale ETag (F04);
//! everything else is re-hashed.
//! With `follow_symlinks` disabled the file is opened nofollow (R3) —
//! a swap to a symlink between the walk and the hash is rejected with
//! `PermissionDenied`, and the file identity comes from that same
//! already-open handle (no second path-based open).
//!
//! The hash carries a torn-file verification (F19): the size is checked
//! before and after the hash, a mid-hash size change discards the result
//! (retried once), and the reported metadata is always the hashed file's
//! own — a row never pairs a hash-time ETag with walk-time size/mtime.

use std::{
    io, mem,
    path::{Path, PathBuf},
    result,
    sync::Mutex,
    time::SystemTime,
};

use async_trait::async_trait;
use tokio::fs;

use crate::{
    _core::{ETag, object, pipeline},
    _store::meta,
    Error, fsutil,
    meta::{composed_gate, composed_keep},
    write::CHUNK_SIZE,
};

/// One compute outcome: the key, the (possibly kept) ETag, and the
/// **hash-time** metadata (size, mtime, identity) that the persisted row
/// must describe — the producer's batch entry (P1). The metadata comes
/// from the hashed file itself, so a row never pairs a hash-time ETag
/// with walk-time size/mtime: a file changed between the walk and the
/// hash yields a self-consistent row that the next gate recomputes
/// (F19). `kept` marks a composed-ETag keep (no hash ran) — the
/// scanner's consecutive-failure streak treats keeps as neutral (F14).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The object key.
    pub key: object::Key,
    /// The (possibly kept) ETag.
    pub etag: ETag,
    /// The object size at hash time.
    pub size: u64,
    /// The object mtime at hash time.
    pub mtime: SystemTime,
    /// The file identity at hash time (`0` marks an unavailable platform
    /// identity; see `fsutil::file_identity`).
    pub identity: u64,
    /// Whether the composed form was kept without hashing (P1).
    pub kept: bool,
}

impl Outcome {
    /// A composed-ETag keep (no hash ran): the stored form plus the
    /// probe's hash-time metadata.
    fn keep(key: &object::Key, etag: &ETag, size: u64, mtime: SystemTime, identity: u64) -> Self {
        Self {
            key: key.clone(),
            etag: etag.clone(),
            size,
            mtime,
            identity,
            kept: true,
        }
    }

    /// A re-hash: the content MD5 of the hashed bytes plus that file's
    /// own hash-time metadata.
    fn hashed(
        key: &object::Key,
        digest: [u8; 16],
        size: u64,
        mtime: SystemTime,
        identity: u64,
    ) -> Self {
        Self {
            key: key.clone(),
            etag: ETag::Single(digest),
            size,
            mtime,
            identity,
            kept: false,
        }
    }
}

/// The per-file compute result: [`Outcome`] or the failure. This is the
/// IO pipeline's [`pipeline::Task::Output`] (`pipeline-spec.md` P4/P7);
/// `pipeline::Outcome` comes from the blanket `result::Result` impl
/// (pipeline.rs) — `Error` is a `StdError`, so the IO-pipeline runtime
/// logs compute failures through it (R8) with the original error kept
/// (P7). Tombstone reclaim lives on the removal pipeline
/// (`Result<(), Error>`), not here.
pub type Result = result::Result<Outcome, Error>;

/// A unit of IO-pipeline work: the blocking ETag computation of one
/// object file (pipeline-spec.md §3.1, P1, R3).
///
/// The result is an [`Outcome`] — the producer's batch entry; `set`
/// needs the identity (and the hash-time size/mtime, F19), so they are
/// computed here from the already-open handle (P1).
/// [`Task::run`] returns [`Result`]; the runner
/// [`Reply::send`]s it to [`pipeline::Completion`].
///
/// Constructed by the task-4 producers (list/scanner); the unit tests
/// construct it directly.
pub(crate) struct ComputeTask {
    /// The object key.
    pub key: object::Key,
    /// The object file path (mapped and boundary-proven by the producer).
    pub path: PathBuf,
    /// The object size at walk time (the keep gate's size check).
    pub size: u64,
    /// The stored entry when the producer's gate found a stale one
    /// (P1 — the composed-ETag keep decision reads it).
    pub stored: Option<meta::Stored>,
    /// Whether the file may be opened through symlinks.
    pub follow_symlinks: bool,
}

/// A 64 KiB hash read buffer checked out of the shared pool (item 4,
/// data-path review 2026-08-27): one allocation per hash CONCURRENCY,
/// never per hashed file — a cold list of 1000 files allocates a few
/// buffers, not 64 MiB of alloc/free churn. The old `thread_local`
/// per-worker pool cannot survive the async compute (the future can
/// migrate between worker threads across `.await` points), so the pool
/// is shared and owns the buffers; the guard returns its buffer on drop.
pub(crate) struct HashBuffer(Vec<u8>);

/// The shared pool of returned buffers (bounded by peak hash
/// concurrency; a buffer is never freed once pooled, matching the old
/// per-thread buffers' lifetime).
static HASH_BUFFER_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

impl HashBuffer {
    /// Check a buffer out of the pool (allocating a fresh one when the
    /// pool is empty).
    pub(crate) fn acquire() -> Self {
        Self(
            HASH_BUFFER_POOL
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| vec![0u8; CHUNK_SIZE]),
        )
    }

    /// The read buffer (the hash overwrites it — never zeroed between
    /// hashes).
    pub(crate) fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl Drop for HashBuffer {
    fn drop(&mut self) {
        HASH_BUFFER_POOL
            .lock()
            .unwrap()
            .push(mem::take(&mut self.0));
    }
}

impl ComputeTask {
    /// The compute core: open the file under the symlink policy (async,
    /// nofollow via the tokio blocking pool), then the shared
    /// `ensure_etag` decision (P1, [`crate::meta::composed_keep`] — one home
    /// for the rule): composed + same size + same file with an unchanged
    /// mtime (the jitter window is the identity-less fallback) → keep the
    /// composed form; else stream re-hash (64 KiB bounded, incremental
    /// MD5, torn-file verified — F19). Matching entries are gated by the
    /// producer before enqueue (no worker for a cache hit). The hash
    /// reads into a pooled 64 KiB buffer ([`HashBuffer`], item 4) — one
    /// allocation per hash concurrency, never per task.
    async fn hash(&self) -> Result {
        let mut buf = HashBuffer::acquire();
        self.hash_into(buf.as_mut()).await
    }

    /// The [`Self::hash`] body over the caller's buffer.
    async fn hash_into(&self, buf: &mut [u8]) -> Result {
        if let Some(stored) = &self.stored {
            // Timestamp jitter (antivirus, indexer) must not rewrite a
            // multipart `MD5-of-MD5s-N` ETag into a content MD5. The keep
            // decision is [`crate::meta::composed_keep`]: on platforms with a
            // file identity the identity must match AND the mtime must be
            // unchanged — any drift re-hashes, because an in-place
            // same-size rewrite keeps the identity and is
            // indistinguishable from a touch without hashing (F04). Where
            // the platform exposes no identity (stored 0), the mtime
            // jitter window is the fallback.
            if composed_gate(&stored.etag, stored.size, self.size) {
                let mut file = open_policy(&self.path, self.follow_symlinks).await?;
                // The path's ONE metadata fetch (data-path review
                // 2026-08-29, finding 5): the probe's metadata doubles as
                // the re-hash path's identity source — the identity of an
                // already-open handle is stable, so the hash never
                // re-fetches it.
                let before = file.metadata().await?;
                let mtime = before.modified()?;
                // The identity comes from the ALREADY-OPEN handle (R3 —
                // never a second path-based open;
                // [`fsutil::file_identity_async`]).
                let current = fsutil::file_identity_async(&mut file, &before).await;
                // The keep also requires the CURRENT size to match the
                // stored one — a file that changed size between the walk
                // and the open is a content change, never a touch (the
                // shared shape+size gate, meta.rs).
                if composed_gate(&stored.etag, stored.size, before.len())
                    && composed_keep(stored.file_identity, stored.mtime, current, mtime)
                {
                    return Ok(Outcome::keep(
                        &self.key,
                        &stored.etag,
                        before.len(),
                        mtime,
                        current,
                    ));
                }
                let digest = md5_of_handle(&mut file, buf).await?;
                let after = file.metadata().await?;
                if after.len() == before.len() {
                    let mtime = after.modified()?;
                    let identity = fsutil::file_identity_async(&mut file, &after).await;
                    return Ok(Outcome::hashed(
                        &self.key,
                        digest,
                        after.len(),
                        mtime,
                        identity,
                    ));
                }
                // The size changed mid-hash — discard and fall through to
                // the verified re-open path (F19).
            }
        }
        let (digest, size, mtime, identity) =
            md5_of_path(&self.path, self.follow_symlinks, buf).await?;
        Ok(Outcome::hashed(&self.key, digest, size, mtime, identity))
    }
}

/// Open `path` for reading under the symlink policy (async): with
/// following disabled, the nofollow open rejects a link with
/// `PermissionDenied` — a swap between the walk and the hash cannot
/// escape the storage root (R3). The nofollow open runs on the tokio
/// blocking pool ([`fsutil::open_file`]).
async fn open_policy(path: &Path, follow_symlinks: bool) -> io::Result<fs::File> {
    fsutil::open_file(path, follow_symlinks).await
}

/// The blocking streaming content MD5 of the file at `path`, opened
/// under the symlink policy ([`open_policy`], R3), plus the hash-time
/// size, mtime, and file identity of the same handle — one open serves
/// the hash, the verification, and the identity (no second path-based
/// open). `buf` is the caller's 64 KiB read buffer. The read-path share
/// of the task compute: `meta::ensure_etag` awaits it directly.
///
/// Torn-file verification (F19): the size is checked before and after
/// the hash; a size change mid-hash discards the result and the file is
/// re-opened and re-hashed once. A second change is accepted — the
/// reported metadata then still describes the hashed bytes, so the row
/// is self-consistent and the next gate recomputes once the file
/// settles.
pub(crate) async fn md5_of_path(
    path: &Path,
    follow_symlinks: bool,
    buf: &mut [u8],
) -> result::Result<([u8; 16], u64, SystemTime, u64), Error> {
    let mut file = open_policy(path, follow_symlinks).await?;
    let before = file.metadata().await?;
    let digest = md5_of_handle(&mut file, buf).await?;
    let after = file.metadata().await?;
    if after.len() == before.len() {
        let mtime = after.modified()?;
        let identity = fsutil::file_identity_async(&mut file, &after).await;
        return Ok((digest, after.len(), mtime, identity));
    }
    // The size changed mid-hash — discard the torn result and re-hash
    // (once; a second change is accepted as described above).
    let mut file = open_policy(path, follow_symlinks).await?;
    let digest = md5_of_handle(&mut file, buf).await?;
    let after = file.metadata().await?;
    let mtime = after.modified()?;
    let identity = fsutil::file_identity_async(&mut file, &after).await;
    Ok((digest, after.len(), mtime, identity))
}

/// The async streaming content MD5 of an already-open file (64 KiB
/// bounded reads into the caller's buffer over `fs::File` — the
/// IO runs on the tokio blocking pool, pipeline-spec.md Q4). No
/// metadata is fetched here (data-path review 2026-08-29, finding 5):
/// the caller's identity comes from metadata it already has — the
/// composed path reuses its probe's, [`md5_of_path`] verifies with its
/// own before/after pair. The read loop is the shared
/// [`fsutil::md5_stream_async`]
/// (F43).
async fn md5_of_handle(file: &mut fs::File, buf: &mut [u8]) -> result::Result<[u8; 16], Error> {
    Ok(fsutil::md5_stream_async(file, buf).await?)
}

#[async_trait]
impl pipeline::Task for ComputeTask {
    type Output = Result;

    fn kind(&self) -> &'static str {
        "etag"
    }

    async fn run(&mut self) -> Result {
        self.hash().await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::io::ErrorKind;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{
        fs::{self as std_fs, File},
        io::Write,
        path::PathBuf,
        thread::sleep,
        time::Duration,
    };

    use tokio::runtime::Builder;

    use super::*;
    use crate::{
        _core::{
            ETag, object,
            pipeline::{InlineRunner, Runner, Task},
            to_nanos,
        },
        _util::testing::etag,
    };

    fn task(
        key: &str,
        path: PathBuf,
        size: u64,
        stored: Option<meta::Stored>,
        follow_symlinks: bool,
    ) -> ComputeTask {
        ComputeTask {
            key: object::key(key).unwrap(),
            path,
            size,
            stored,
            follow_symlinks,
        }
    }

    /// Run the task on the inline runner and await [`pipeline::Completion`].
    async fn run(task: ComputeTask) -> Result {
        let runner = InlineRunner::default();
        runner.enqueue(Box::new(task)).await.unwrap().await.unwrap()
    }

    #[test]
    fn kind_is_etag() {
        let task = task("a.txt", PathBuf::from("nope"), 0, None, false);
        assert_eq!(task.kind(), "etag");
    }

    #[tokio::test]
    async fn recomputes_content_md5_when_nothing_is_stored() {
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("a.txt");
        std_fs::write(&file, b"hello").unwrap();
        let metadata = std_fs::metadata(&file).unwrap();
        let task = task("a.txt", file.clone(), metadata.len(), None, false);
        let outcome = run(task).await.unwrap();
        assert_eq!(&*outcome.key, "a.txt");
        assert_eq!(outcome.etag, ETag::from_content(b"hello"));
        assert!(!outcome.kept);
        // The identity of the opened file round-trips (the producer's
        // batch refresh records it), and the outcome carries the
        // hash-time metadata — the row the producer persists describes
        // the hashed file itself (F19).
        assert_eq!(outcome.identity, fsutil::file_identity(&file, &metadata));
        assert_eq!(outcome.size, metadata.len());
        assert_eq!(outcome.mtime, metadata.modified().unwrap());
    }

    #[tokio::test]
    async fn composed_etag_kept_on_identity_less_storage_within_the_jitter_window() {
        // The identity-less fallback (stored identity 0 — platforms or
        // filesystems without a file ID): a touch within the mtime jitter
        // window is kept — the composed `MD5-of-MD5s-N` form survives and
        // the batch refreshes the entry (P1; the strict identity check is
        // impossible without an identity — the documented risk).
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("mp.bin");
        std_fs::write(&file, b"hello").unwrap();
        let metadata = std_fs::metadata(&file).unwrap();
        let composed = etag("5d41402abc4b2a76b9719d911017c592-2");
        let stored = meta::Stored {
            etag: composed.clone(),
            size: metadata.len(),
            mtime: to_nanos(metadata.modified().unwrap()),
            file_identity: 0,
            tags: object::Tags::empty(),
            checksum: None,
        };
        let handle = File::options().write(true).open(&file).unwrap();
        handle
            .set_modified(metadata.modified().unwrap() + Duration::from_secs(30))
            .unwrap();
        drop(handle);
        let now = std_fs::metadata(&file).unwrap();
        let task = task("mp.bin", file.clone(), now.len(), Some(stored), false);
        let outcome = run(task).await.unwrap();
        assert_eq!(outcome.etag, composed);
        assert!(matches!(outcome.etag, ETag::Composed(_, 2)));
        assert!(outcome.kept);
        assert_eq!(outcome.identity, fsutil::file_identity(&file, &now));
    }

    #[tokio::test]
    async fn composed_etag_rehashes_on_touch_when_identity_is_available() {
        // F04 on identity platforms: a touch (same file, mtime pushed
        // forward) re-hashes — an in-place same-size rewrite keeps the
        // identity and is indistinguishable from a touch without hashing,
        // so any mtime drift re-hashes (correctness first: a stale
        // composed ETag is never served; the composed form degrades to
        // the content MD5).
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("mp.bin");
        std_fs::write(&file, b"hello").unwrap();
        let metadata = std_fs::metadata(&file).unwrap();
        let composed = etag("5d41402abc4b2a76b9719d911017c592-2");
        let stored = meta::Stored {
            etag: composed,
            size: metadata.len(),
            mtime: to_nanos(metadata.modified().unwrap()),
            file_identity: fsutil::file_identity(&file, &metadata),
            tags: object::Tags::empty(),
            checksum: None,
        };
        let handle = File::options().write(true).open(&file).unwrap();
        handle
            .set_modified(metadata.modified().unwrap() + Duration::from_secs(30))
            .unwrap();
        drop(handle);
        let now = std_fs::metadata(&file).unwrap();
        let task = task("mp.bin", file.clone(), now.len(), Some(stored), false);
        let outcome = run(task).await.unwrap();
        assert_eq!(outcome.etag, ETag::from_content(b"hello"));
        assert!(!outcome.kept);
    }

    #[tokio::test]
    async fn composed_etag_rehashes_on_in_place_same_size_rewrite() {
        // F04 regression: an in-place rewrite (SAME file — identity
        // unchanged — different same-size content) within the old 60 s
        // jitter window must re-hash — the stale composed ETag is never
        // kept and served forever.
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("mp.bin");
        std_fs::write(&file, b"hello").unwrap();
        let metadata = std_fs::metadata(&file).unwrap();
        let composed = etag("5d41402abc4b2a76b9719d911017c592-2");
        let stored = meta::Stored {
            etag: composed,
            size: metadata.len(),
            mtime: to_nanos(metadata.modified().unwrap()),
            file_identity: fsutil::file_identity(&file, &metadata),
            tags: object::Tags::empty(),
            checksum: None,
        };
        // Overwrite in place with different same-size content. The sleep
        // lands the rewrite in a later Windows FILETIME tick (~16 ms
        // clock granularity) — a rewrite within the SAME tick as the
        // original write keeps the old mtime and is indistinguishable
        // even from the strict keep (the documented platform limitation).
        sleep(Duration::from_millis(30));
        let mut handle = File::options()
            .write(true)
            .truncate(true)
            .open(&file)
            .unwrap();
        handle.write_all(b"world").unwrap();
        handle.sync_all().unwrap();
        drop(handle);
        let now = std_fs::metadata(&file).unwrap();
        let task = task("mp.bin", file, now.len(), Some(stored), false);
        let outcome = run(task).await.unwrap();
        assert_eq!(outcome.etag, ETag::from_content(b"world"));
        assert!(!outcome.kept);
    }

    #[tokio::test]
    async fn composed_etag_rehashes_on_replacement() {
        // A same-size out-of-band replacement (a NEW file renamed over):
        // the identity changed and the mtime drift is beyond the jitter
        // window — re-hashed, never the stale composed form.
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("mp.bin");
        std_fs::write(&file, b"hello").unwrap();
        let metadata = std_fs::metadata(&file).unwrap();
        let composed = etag("5d41402abc4b2a76b9719d911017c592-2");
        let stored = meta::Stored {
            etag: composed,
            size: metadata.len(),
            mtime: to_nanos(metadata.modified().unwrap()),
            file_identity: fsutil::file_identity(&file, &metadata),
            tags: object::Tags::empty(),
            checksum: None,
        };
        let replacement = state.path().join("replacement.bin");
        std_fs::write(&replacement, b"hello").unwrap();
        let handle = File::options().write(true).open(&replacement).unwrap();
        handle
            .set_modified(metadata.modified().unwrap() + Duration::from_secs(120))
            .unwrap();
        drop(handle);
        std_fs::rename(&replacement, &file).unwrap();
        let now = std_fs::metadata(&file).unwrap();
        let task = task("mp.bin", file, now.len(), Some(stored), false);
        let outcome = run(task).await.unwrap();
        assert_eq!(outcome.etag, ETag::from_content(b"hello"));
        assert!(!outcome.kept);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn composed_etag_rehashes_on_quick_replacement() {
        // The identity beats the clock: a same-size replacement within
        // the jitter window is still detected (new inode), where the
        // mtime fallback would wrongly keep the composed form.
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("mp.bin");
        std_fs::write(&file, b"hello").unwrap();
        let metadata = std_fs::metadata(&file).unwrap();
        let composed = etag("5d41402abc4b2a76b9719d911017c592-2");
        let stored = meta::Stored {
            etag: composed,
            size: metadata.len(),
            mtime: to_nanos(metadata.modified().unwrap()),
            file_identity: fsutil::file_identity(&file, &metadata),
            tags: object::Tags::empty(),
            checksum: None,
        };
        let replacement = state.path().join("replacement.bin");
        std_fs::write(&replacement, b"hello").unwrap();
        // Fresh mtime — inside the jitter window.
        std_fs::rename(&replacement, &file).unwrap();
        let now = std_fs::metadata(&file).unwrap();
        let task = task("mp.bin", file, now.len(), Some(stored), false);
        let outcome = run(task).await.unwrap();
        assert_eq!(outcome.etag, ETag::from_content(b"hello"));
        assert!(!outcome.kept);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_a_symlink_with_permission_denied() {
        // R3: with following disabled the task opens nofollow — a file
        // swapped for a symlink between the walk and the hash is
        // rejected, never followed.
        let state = tempfile::tempdir().unwrap();
        let real = state.path().join("real.bin");
        let link = state.path().join("link.bin");
        std_fs::write(&real, b"hello").unwrap();
        symlink(&real, &link).unwrap();
        let metadata = std_fs::metadata(&real).unwrap();
        let task = task("link.bin", link, metadata.len(), None, false);
        let err = run(task).await.unwrap_err();
        assert!(
            matches!(err, Error::Io(ref e) if e.kind() == ErrorKind::PermissionDenied),
            "{err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn follows_a_symlink_when_enabled() {
        // With following enabled the task opens through the link and
        // hashes the target.
        let state = tempfile::tempdir().unwrap();
        let real = state.path().join("real.bin");
        let link = state.path().join("link.bin");
        std_fs::write(&real, b"hello").unwrap();
        symlink(&real, &link).unwrap();
        let metadata = std_fs::metadata(&real).unwrap();
        let task = task("link.bin", link, metadata.len(), None, true);
        let outcome = run(task).await.unwrap();
        assert_eq!(outcome.etag, ETag::from_content(b"hello"));
    }

    #[tokio::test]
    async fn failure_is_reported_through_the_completion() {
        let state = tempfile::tempdir().unwrap();
        let missing = state.path().join("missing.bin");
        let task = task("missing.bin", missing, 0, None, false);
        let err = run(task).await.unwrap_err();
        assert!(matches!(err, Error::Io(_)));
    }

    #[tokio::test]
    async fn completion_carries_the_run_failure() {
        let state = tempfile::tempdir().unwrap();
        let missing = state.path().join("missing.bin");
        let task = ComputeTask {
            key: object::key("missing.bin").unwrap(),
            path: missing,
            size: 0,
            stored: None,
            follow_symlinks: false,
        };
        let err = run(task).await.unwrap_err();
        assert!(matches!(err, Error::Io(_)));
    }

    #[tokio::test]
    async fn completion_carries_the_run_success() {
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("a.txt");
        std_fs::write(&file, b"hello").unwrap();
        let metadata = std_fs::metadata(&file).unwrap();
        let task = ComputeTask {
            key: object::key("a.txt").unwrap(),
            path: file,
            size: metadata.len(),
            stored: None,
            follow_symlinks: false,
        };
        assert!(run(task).await.is_ok());
    }

    #[test]
    fn compute_core_completes_on_a_single_threaded_runtime() {
        // Q4 async semantics: the compute core is `tokio::fs` — the hash
        // IO runs on the tokio blocking pool, so a big-file hash
        // completes without deadlock on a single-threaded runtime (the
        // blocking pool is independent of the worker driver).
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let file = state.path().join("big.bin");
            fs::write(&file, vec![b'x'; 4 * 1024 * 1024]).await.unwrap();
            let metadata = fs::metadata(&file).await.unwrap();
            let task = task("big.bin", file, metadata.len(), None, false);
            let runner = InlineRunner::default();
            let done = runner.enqueue(Box::new(task)).await.unwrap();
            let outcome = done.await.unwrap().unwrap();
            assert_eq!(outcome.size, 4 * 1024 * 1024);
            assert_eq!(
                outcome.etag,
                ETag::from_content(&vec![b'x'; 4 * 1024 * 1024])
            );
        });
    }
}
