//! Streaming atomic writes (task T038).
//!
//! Every object write streams into a temp file under `<state-dir>/tmp/`,
//! then `fs::rename` to the final path — atomic on the same volume
//! (FR-011): a reader never sees a torn mix, only the previous complete
//! object or the new one. The ETag MD5 is computed while streaming
//! (FR-010/022) with bounded buffers (constitution V).
//!
//! An interrupted upload leaves only a temp file: invisible to listings,
//! reclaimed at startup (full `tmp/` clear) or by the sweep after the
//! mtime TTL (failure-handling.md §2D). Last-write-wins: the last
//! completed rename wins; no per-object allocation.
//!
//! Durability (D1, pipeline-spec.md §7): object bytes are made durable,
//! aligned with the redb `Immediate` metadata — the staged content is
//! fsynced before the rename, and the directory entries are fsynced after
//! it (unix; the Windows limitation is documented in the spec row). The
//! rename is the commit point (F06): a post-rename sync failure is
//! warned, never a failed write — the object is visible and correct, and
//! both crash directions self-heal. The first commit into a new prefix
//! also syncs the freshly created ancestor chain up to the bucket root
//! (F03), so "no durable meta row without durable bytes" holds for the
//! first PUT into a prefix too.

#[cfg(unix)]
use std::fs::{File as StdFile, OpenOptions, remove_file};
#[cfg(unix)]
use std::io::Seek;
use std::{
    fs::Metadata,
    io,
    path::{Path, PathBuf},
    pin::pin,
};

use futures::StreamExt;
use md5::{Digest, Md5};
#[cfg(unix)]
use tokio::task;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tracing::instrument;
use uuid::Uuid;

// One definer for the bounded-chunk constant (the ranged body stream serves in
// the same bound): the value's rationale lives in `tinio_util::fsutil`.
pub(crate) use crate::_util::fsutil::CHUNK_SIZE;
use crate::{
    _core::{BodyStream, ETag, checksum},
    Error, fsutil,
    fsutil::DirOwnerUid,
    path::TMP_DIR_NAME,
};

/// The streaming content MD5 of the file at `path` plus the metadata of
/// the opened file (bounded buffers; the metadata is the caller's file
/// identity — one open serves both, no second stat of the path).
pub(crate) async fn md5_of_file(path: &Path) -> Result<([u8; 16], Metadata), Error> {
    let mut file = File::open(path).await?;
    let mut buf = vec![0u8; CHUNK_SIZE];
    // The shared async hashing core (F43); the per-call buffer stays —
    // the write path hashes one object per PUT, unlike the scanner's
    // per-file loop that shares the pooled worker buffer.
    let digest = fsutil::md5_stream_async(&mut file, &mut buf).await?;
    let metadata = file.metadata().await?;
    Ok((digest, metadata))
}

/// Atomic object-body writer over the state-dir `tmp/` staging area.
///
/// # Examples
///
/// ```rust
/// use std::fs::read;
///
/// use tinio_fs::AtomicWriter;
/// use tinio_util::testing::body;
/// use tokio::runtime::Runtime;
///
/// let state = tempfile::tempdir().unwrap();
/// let writer = AtomicWriter::new(state.path());
/// let target = state.path().join("obj.bin");
/// let (etag, size) = Runtime::new()
///     .unwrap()
///     .block_on(writer.write(&target, body(b"hello")))
///     .unwrap();
/// assert_eq!(etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
/// assert_eq!(size, 5);
/// assert_eq!(read(&target).unwrap(), b"hello");
/// ```
/// An incremental staged write: a temp file under `tmp/` plus the content
/// MD5 computed while the bytes arrive. This is the one home of the staged
/// temp-file rules — [`AtomicWriter::write_temp`]'s streamed stage and the
/// content contract's chunked stage share it: a fresh `upload-<uuid>` name,
/// `0600` on unix regardless of the umask, one MD5 over the bytes actually
/// written, and the per-write content sync (D1) before the rename makes the
/// name visible.
#[derive(Debug)]
pub(crate) struct StagedFile {
    file: File,
    temp: PathBuf,
    /// `None` when a tee promises the digest (the etag cell) — the write
    /// skips its own hash, exactly like a streamed stage.
    hasher: Option<Md5>,
    written: u64,
}

impl StagedFile {
    /// Open a fresh temp file at `temp` (`0600` on unix) and hash its bytes
    /// when `hash` is set.
    async fn open(temp: PathBuf, hash: bool) -> Result<Self, Error> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        options.mode(fsutil::FILE_MODE_PRIVATE);
        let file = options.open(&temp).await?;
        Ok(Self {
            file,
            temp,
            hasher: hash.then(Md5::new),
            written: 0,
        })
    }

    /// Consume the handle and answer the temp path: the caller renames it
    /// into place or removes it (an abandoned stage leaves no residue).
    pub(crate) fn into_temp(self) -> PathBuf {
        self.temp
    }

    /// The temp path, while this stage still owns the file. Unix chown reads
    /// it before the publish's rename, which keeps the uid.
    #[cfg(unix)]
    pub(crate) fn path(&self) -> &Path {
        &self.temp
    }

    /// Append one chunk. Bounded slices: a single oversized chunk never
    /// buffers whole.
    pub(crate) async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Error> {
        for slice in chunk.chunks(CHUNK_SIZE) {
            self.file.write_all(slice).await?;
            self.written += slice.len() as u64;
            if let Some(hasher) = &mut self.hasher {
                hasher.update(slice);
            }
        }
        Ok(())
    }

    /// Flush and sync the bytes (D1) and answer the temp path, the content
    /// ETag and the byte count. `tee_etag` is the digest a tee promised when
    /// the write was not hashed.
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bytes = self.written)
    )]
    pub(crate) async fn finish(
        mut self,
        tee_etag: Option<[u8; 16]>,
    ) -> Result<(PathBuf, ETag, u64), Error> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        let etag = match self.hasher {
            Some(hasher) => ETag::Single(hasher.finalize().into()),
            None => {
                // The tee's MD5: the raw digest lands in the etag cell before
                // the stream's final `None`, so the value is there. A caller
                // that promised the digest without draining the tee through
                // this stage is answered with the crate's own error rather
                // than a panic (design 2026-10-09 D1) — the write fails and
                // the staged temp is removed by the caller.
                let digest = tee_etag.ok_or(Error::Invariant(
                    "the etag tee's promise is filled when the body it wraps drains",
                ))?;
                ETag::Single(digest)
            }
        };
        Ok((self.temp, etag, self.written))
    }
}

#[derive(Debug, Clone)]
pub struct AtomicWriter {
    /// `<state-dir>/tmp/` — the staging directory for in-flight writes.
    tmp_dir: PathBuf,
}

impl AtomicWriter {
    /// Create a writer staging under `<state-dir>/tmp/`.
    pub fn new(state_dir: &Path) -> Self {
        Self {
            tmp_dir: state_dir.join(TMP_DIR_NAME),
        }
    }

    /// The staging directory (`<state-dir>/tmp/`).
    pub(crate) fn tmp_dir(&self) -> &Path {
        &self.tmp_dir
    }

    /// Stream `body` into a temp file and atomically rename it onto
    /// `target` (creating parent directories). Returns the content MD5
    /// ETag and the number of bytes streamed — the values a caller
    /// records about what it just wrote without re-opening, re-statting
    /// or re-hashing the file. On failure the target is untouched; the
    /// temp file is left for the sweep / startup repair
    /// (failure-handling.md §2C).
    pub async fn write(&self, target: &Path, body: BodyStream) -> Result<(ETag, u64), Error> {
        let (temp, etag, size) = self.stage(body, None).await?;
        Self::commit(&temp, target, None, DirOwnerUid::new(None)).await?;
        Ok((etag, size))
    }

    /// Rename a staged temp onto `target` (creating parent directories);
    /// the temp is removed best-effort on failure. Callers that serialize
    /// bucket mutations hold their lock across this call.
    ///
    /// The protocol itself — parent creation, the rename, the EXDEV
    /// fallback and the D1/F03 directory syncs — is
    /// [`tinio_util::fsutil::commit`](crate::_util::fsutil::commit)'s; this
    /// method keeps the write path's own spelling of it (the `Error`
    /// mapping included). `sync_root` bounds the ancestor sync: the
    /// production callers pass the bucket directory, standalone users
    /// (`AtomicWriter::write`) pass `None`. `dir_owner_uid` is the unix
    /// dir-hardening context of the parent creation (spec 2026-09-05 §5a).
    pub(crate) async fn commit(
        temp: &Path,
        target: &Path,
        sync_root: Option<&Path>,
        dir_owner_uid: DirOwnerUid,
    ) -> Result<(), Error> {
        fsutil::commit(temp, target, sync_root, dir_owner_uid)
            .await
            .map_err(Error::from)
    }

    /// Sync `dir` (D1); a failure is warned, never fatal (F06). The
    /// discipline ([`crate::_util::fsutil::sync_dir_warned`]) is shared with
    /// every backend now; this method is the write path's own spelling.
    pub(crate) async fn sync_dir_warned(dir: &Path) {
        fsutil::sync_dir_warned(dir).await
    }

    /// Sync every ancestor of `leaf` up to and including `sync_root` (F03).
    /// The chain walk ([`crate::_util::fsutil::sync_ancestor_chain`]) is
    /// shared with every backend now; this method is the write path's own
    /// spelling.
    pub(crate) async fn sync_ancestor_chain(leaf: &Path, sync_root: Option<&Path>) {
        fsutil::sync_ancestor_chain(leaf, sync_root).await
    }

    /// The tmp dir exists in steady state — one probe instead of the
    /// per-component walk (the create still runs when the sweep cleared
    /// it). Shared by [`Self::stage`] and [`Self::stage_copy`].
    /// Private state, server-user-owned: the mode hardening only
    /// (spec 2026-09-05 §5a — no chown).
    async fn ensure_tmp_dir(&self) -> io::Result<()> {
        fsutil::ensure_dir(&self.tmp_dir, DirOwnerUid::new(None)).await?;
        Ok(())
    }

    /// Stream `body` into a fresh temp file under `tmp/`, returning the
    /// temp path, the content MD5 and the number of bytes written. The
    /// caller controls when the temp becomes visible (rename under its
    /// own lock); on failure the temp is removed best-effort. `checksum`
    /// is the server's tee slot: with the `etag` cell the slot already
    /// holds the content MD5 (a part's ETag IS its content MD5), so the
    /// write is not hashed a second time.
    #[instrument(target = "tinio::storage", level = "debug", skip_all)]
    pub(crate) async fn stage(
        &self,
        body: BodyStream,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(PathBuf, ETag, u64), Error> {
        self.ensure_tmp_dir().await?;
        let temp = self.tmp_dir.join(format!("upload-{}", Uuid::new_v4()));
        let result = self.write_temp(&temp, body, checksum).await;
        match result {
            Ok((etag, size)) => Ok((temp, etag, size)),
            Err(err) => {
                let _ = fs::remove_file(&temp).await;
                Err(err)
            }
        }
    }

    /// Open a fresh incremental staged temp under `tmp/` — the content
    /// contract's chunked stage ([`StagedFile::write_chunk`]). The caller
    /// renames it into place or discards it.
    pub(crate) async fn stage_file(&self) -> Result<StagedFile, Error> {
        self.ensure_tmp_dir().await?;
        let temp = self.tmp_dir.join(format!("upload-{}", Uuid::new_v4()));
        StagedFile::open(temp, true).await
    }

    /// The stream+hash core: drain `body` into `temp` with bounded
    /// buffers, returning the content MD5 (from the tee's `etag` cell
    /// when promised — the write skips its own hash) and the number of
    /// bytes written. The temp is the future object file — created at
    /// `0600` on unix regardless of the umask (spec 2026-09-05 §5a; the
    /// rename preserves the mode).
    async fn write_temp(
        &self,
        temp: &Path,
        body: BodyStream,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(ETag, u64), Error> {
        // The tee's `etag` cell promises the content MD5 (a part's ETag IS
        // its content MD5) — the write then skips its own hash.
        let hash = !checksum.is_some_and(|c| c.etag.is_some());
        let mut staged = StagedFile::open(temp.to_path_buf(), hash).await?;
        let mut stream = pin!(body);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            // Bound the copy: a single oversized chunk never buffers
            // whole — it is drained in bounded slices.
            staged.write_chunk(chunk.as_ref()).await?;
        }
        // D1 — content durability: the bytes must be on disk before the
        // rename makes the name visible (the rename's directory entry is
        // synced by `commit`). Also covers the multipart part files —
        // "parts survive restarts" (multipart.rs) now holds for content.
        // The per-part sync doubles the part's documented flush cost (the
        // UPLOADS/PARTS row commits with `Durability::Immediate`), a
        // deliberate tradeoff: parts are re-uploadable, so an operator
        // could drop this sync if the cost matters — the assemble-time
        // sync still covers the completed object (F26, documented).
        let tee_etag = checksum.and_then(|c| c.etag_digest());
        let (_temp, etag, written) = staged.finish(tee_etag).await?;
        Ok((etag, written))
    }

    /// Stage `len` bytes at `offset` of an already-open `source` into a
    /// fresh temp under `tmp/` — the copy primitives' fast path
    /// (CopyObject/UploadPartCopy): kernel-side `copy_file_range` (unix
    /// only — the primitives fall back to the contract's stream path
    /// elsewhere), then the content MD5 of the copied bytes (a second
    /// read pass over the temp — the kernel did the copy, so the hash
    /// has no write side to piggyback on) and a content sync (D1 — the
    /// same durability the streaming stage gives). Returns
    /// `(temp, etag)`; the temp is removed best-effort on the closure's
    /// failure — the join path's own temp is left to the cleanup sweep's
    /// full `tmp/` clear.
    #[cfg(unix)]
    pub(crate) async fn stage_copy(
        &self,
        source: StdFile,
        offset: u64,
        len: u64,
    ) -> Result<(PathBuf, ETag), Error> {
        self.ensure_tmp_dir().await?;
        let temp = self.tmp_dir.join(format!("upload-{}", Uuid::new_v4()));
        let temp_task = temp.clone();
        let result = task::spawn_blocking(move || {
            // Read+write: the hash pass reads the temp back after the
            // kernel copy (a write-only handle would fail it with EBADF).
            // The temp is the future object file — created at `0600` on
            // unix regardless of the umask (spec 2026-09-05 §5a).
            let mut options = OpenOptions::new();
            options.read(true).write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(fsutil::FILE_MODE_PRIVATE);
            }
            let mut dst = options.open(&temp_task).map_err(Error::from)?;
            fsutil::copy_file_range(&source, offset, len, &dst)?;
            dst.sync_all()?;
            // The kernel copy advanced the file position — rewind for
            // the hash pass.
            dst.rewind()?;
            // The shared streaming MD5 (F43) — one home for the loop,
            // like every other hashing site.
            let mut buf = vec![0u8; CHUNK_SIZE];
            let digest = fsutil::md5_stream(&mut dst, &mut buf)?;
            Ok::<_, Error>(ETag::Single(digest))
        })
        .await
        // A panicking (or cancelled) copy closure is answered as this call's
        // failure instead of re-panicking the caller (design 2026-10-09 D1):
        // nothing is published — the join error returns before the removal
        // below, so this path's temp is left to the cleanup sweep's full
        // `tmp/` clear — and the join's own detail goes to the log, while the
        // error carries only the broken invariant (D5). A task that did not
        // report a result is a bug, so it is neither swallowed nor classified
        // as I/O.
        .map_err(|join| {
            tracing::error!(
                error = %join,
                "the file-copy task did not report a result; the copy is failed, not published"
            );
            Error::Invariant("the file-copy task reports its result")
        })?;
        match result {
            Ok(etag) => Ok((temp, etag)),
            Err(err) => {
                let _ = remove_file(&temp);
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::stream;
    use io::Error as IoError;

    use super::*;
    use crate::{
        _util::testing::{body, etag},
        fsutil::DirOwnerUid,
    };

    #[tokio::test]
    async fn write_stores_content_and_etag() {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("obj.bin");
        let got = writer.write(&target, body(b"hello world")).await.unwrap();
        assert_eq!(got.0, etag("5eb63bbbe01eeed093cb22bb8f5acdc3"));
        assert_eq!(got.1, 11, "the write answers the bytes it streamed");
        assert_eq!(fs::read(&target).await.unwrap(), b"hello world");
        // No temp files left behind on success.
        let tmp = state.path().join("tmp");
        let mut entries = fs::read_dir(&tmp).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn write_creates_parent_directories() {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("dir/sub/deep/obj.txt");
        writer.write(&target, body(b"x")).await.unwrap();
        assert_eq!(fs::read(&target).await.unwrap(), b"x");
    }

    #[tokio::test]
    async fn write_zero_bytes() {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("empty");
        let got = writer.write(&target, body(b"")).await.unwrap();
        assert_eq!(got.0, etag("d41d8cd98f00b204e9800998ecf8427e"));
        assert_eq!(got.1, 0, "an empty body streams zero bytes");
        assert_eq!(fs::metadata(&target).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn write_last_writer_wins() {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("obj");
        writer.write(&target, body(b"first")).await.unwrap();
        writer.write(&target, body(b"second")).await.unwrap();
        assert_eq!(fs::read(&target).await.unwrap(), b"second");
    }

    #[tokio::test]
    async fn interrupted_upload_leaves_no_partial_object() {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("obj");
        // A stream that yields one good chunk then fails.
        let stream = stream::iter(vec![
            Ok(Bytes::from_static(b"partial")),
            Err(IoError::other("connection reset")),
        ]);
        let body: BodyStream = Box::pin(stream);
        let err = writer.write(&target, body).await.unwrap_err();
        assert!(matches!(err, Error::Io(_)));
        // The target never appears (previous version absent); the temp
        // file is removed best-effort (or swept later if cleanup raced).
        assert!(fs::metadata(&target).await.is_err());
    }

    #[tokio::test]
    async fn commit_fails_onto_a_directory_and_cleans_the_temp() {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("existing-dir");
        fs::create_dir(&target).await.unwrap();
        let (temp, _, _) = writer.stage(body(b"x"), None).await.unwrap();
        // rename(file, existing-directory) fails on every platform — the
        // target stays untouched and the failed temp is removed (item 7c).
        let err = AtomicWriter::commit(&temp, &target, None, DirOwnerUid::new(None))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)));
        assert!(
            !fs::try_exists(&temp).await.unwrap(),
            "the failed commit must remove its temp residue"
        );
    }

    /// The dir fsync is not silently dropped: when the parent directory
    /// cannot be opened for the sync (no read permission — a rename needs
    /// only write+execute), the write SUCCEEDS (the rename is the commit
    /// point, F06) — the sync failure is warned, never a failed write:
    /// the object is visible and correct, and both crash directions
    /// self-heal. (Requires an unprivileged run — root bypasses
    /// permission checks.)
    #[cfg(unix)]
    #[tokio::test]
    async fn write_succeeds_when_parent_dir_cannot_be_synced() {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let dir = state.path().join("locked");
        fs::create_dir(&dir).await.unwrap();
        fs::set_permissions(&dir, Permissions::from_mode(0o300))
            .await
            .unwrap();
        let target = dir.join("obj.bin");
        let result = writer.write(&target, body(b"x")).await;
        fs::set_permissions(&dir, Permissions::from_mode(0o755))
            .await
            .unwrap();
        assert!(
            result.is_ok(),
            "the rename is the commit point — an unsyncable parent must not fail the write (F06)"
        );
        assert_eq!(fs::read(&target).await.unwrap(), b"x");
    }

    /// F03: the first commit into a NEW prefix syncs the whole ancestor
    /// chain, not just the leaf parent — the leaf entry alone would leave
    /// the newly created ancestor directory entries unsynced, inverting
    /// the durability promise ("no durable meta row without durable
    /// bytes"). The sync is unobservable on Windows (a documented
    /// no-op), so the test pins the chain-walk shape on unix.
    #[cfg(unix)]
    #[tokio::test]
    async fn first_commit_syncs_the_new_ancestor_chain() {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};

        use crate::fsutil::DirOwnerUid;

        let state = tempfile::tempdir().unwrap();
        // `sync_root` = the "bucket" root under the state dir; the
        // leaf parent is TWO levels deep — both new ancestors are
        // created by this commit. Making the ROOT unsyncable must
        // not fail the write: the chain walk warns per failed sync
        // and continues (the closest syncable entry is the strongest
        // durability available — F06/F03).
        let root = state.path().join("bucket");
        fs::create_dir(&root).await.unwrap();
        let target = root.join("a").join("b").join("obj.txt");
        let temp = state.path().join("staged.tmp");
        fs::write(&temp, b"x").await.unwrap();
        fs::set_permissions(&root, Permissions::from_mode(0o300))
            .await
            .unwrap();
        let result =
            AtomicWriter::commit(&temp, &target, Some(&root), DirOwnerUid::new(None)).await;
        fs::set_permissions(&root, Permissions::from_mode(0o755))
            .await
            .unwrap();
        // The rename landed and the commit succeeded — the unsyncable
        // ancestor was warned, not fatal.
        assert_eq!(fs::read(&target).await.unwrap(), b"x");
        assert!(result.is_ok());
    }
}
