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
    io::{self, ErrorKind},
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
use uuid::Uuid;

use crate::{
    _core::{BodyStream, ETag, checksum, object::RESERVED_SEGMENT},
    Error, fsutil,
    path::TMP_DIR_NAME,
};

/// Bounded chunk size for the streaming copy/hash loops (constitution V:
/// no per-object buffering; hyper chunks are typically ≤ 64 KiB anyway).
pub(crate) const CHUNK_SIZE: usize = 64 * 1024;

/// Sync `parent` so a rename inside it is durable (D1 — the standard
/// durable-write pattern: content fsynced before the rename, the
/// directory fsynced after). On unix the parent directory is opened
/// (read-only) and fsynced. On Windows std cannot open a directory
/// handle and no heavyweight dependency is added for a directory flush —
/// the directory entry is not fsynced there (a documented limitation,
/// pipeline-spec.md §7 D1). Tokio's `sync_all` runs the blocking fsync
/// on the blocking pool — the request path never blocks.
async fn sync_parent_dir(parent: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(parent).await?.sync_all().await
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        Ok(())
    }
}

/// Copy `temp` onto `target` via a unique staging file inside the target
/// directory's `.tinio/` reserved segment, then rename it (same volume —
/// atomic). The EXDEV fallback of [`AtomicWriter::commit`].
///
/// Staging inside the reserved segment keeps a crash residual **invisible
/// to the data plane** (FR-020: `.tinio` segments are never served or
/// listed at any depth); the staging file is removed best-effort on
/// failure, and the staging directory is removed when empty.
async fn copy_across_volumes(temp: &Path, target: &Path) -> Result<(), Error> {
    let staging_dir = target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(RESERVED_SEGMENT);
    fs::create_dir_all(&staging_dir).await?;
    let staging = staging_dir.join(Uuid::new_v4().to_string());
    let copied = async {
        fs::copy(temp, &staging).await?;
        // D1 — the staging copy is what the rename makes visible; `copy`
        // does not sync. Sync its content before the name (the same
        // content-durability promise as [`AtomicWriter::stage`]).
        // Write access: Windows `FlushFileBuffers` refuses read-only
        // handles (the staging file is private to this rename).
        File::options()
            .write(true)
            .open(&staging)
            .await?
            .sync_all()
            .await?;
        fs::rename(&staging, target).await?;
        Ok::<_, Error>(())
    }
    .await;
    if copied.is_err() {
        let _ = fs::remove_file(&staging).await;
    }
    // Remove the staging directory when empty (a concurrent EXDEV write
    // may still be staging in it — the removal then fails harmlessly).
    let _ = fs::remove_dir(&staging_dir).await;
    copied
}

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
/// let etag = Runtime::new()
///     .unwrap()
///     .block_on(writer.write(&target, body(b"hello")))
///     .unwrap();
/// assert_eq!(etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
/// assert_eq!(read(&target).unwrap(), b"hello");
/// ```
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
    /// ETag. On failure the target is untouched; the temp file is left for
    /// the sweep / startup repair (failure-handling.md §2C).
    pub async fn write(&self, target: &Path, body: BodyStream) -> Result<ETag, Error> {
        let (temp, etag) = self.stage(body, None).await?;
        Self::commit(&temp, target, None).await?;
        Ok(etag)
    }

    /// Rename a staged temp onto `target` (creating parent directories);
    /// the temp is removed best-effort on failure. Callers that serialize
    /// bucket mutations hold their lock across this call.
    ///
    /// `sync_root` bounds the D1 ancestor sync (F03): the first commit
    /// into a NEW prefix created the whole ancestor chain with
    /// `create_dir_all`, and only the chain's directory entries — up to
    /// and including the bucket root — make the rename's path durable.
    /// The production callers pass the bucket directory; standalone users
    /// (`AtomicWriter::write`) pass `None` and get the leaf-only sync.
    ///
    /// A cross-volume state dir (FR-023 relocation) makes `rename` fail
    /// with `CrossesDevices` — the fallback copies the temp through a
    /// unique staging file **on the target volume**, then renames (atomic
    /// there; readers still see the old object or the new one, never a
    /// torn mix). The staging file lives in the target directory's
    /// `.tinio/` reserved segment — a crash between copy and rename
    /// leaves invisible residue (never served or listed, FR-020), not a
    /// stray object.
    pub(crate) async fn commit(
        temp: &Path,
        target: &Path,
        sync_root: Option<&Path>,
    ) -> Result<(), Error> {
        // Every commit target is `bucket_dir.join(key)` — a parent always
        // exists (item 8: a `None` here would silently skip the dir
        // creation AND the durability sync — unreachable in practice,
        // pinned for debug builds).
        debug_assert!(
            target.parent().is_some(),
            "commit target without a parent: {target:?}"
        );
        let mut fallback = false;
        let mut created_parent = false;
        let result = async {
            if let Some(parent) = target.parent() {
                // One probe instead of `create_dir_all`'s per-component
                // walk (item 7d): the parent exists in steady state; the
                // create still runs when the parent is missing (the
                // first PUT into a new prefix), and the created flag
                // drives the ancestor-chain sync (F03).
                created_parent = fsutil::ensure_dir(parent).await?;
            }
            match fs::rename(temp, target).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == ErrorKind::CrossesDevices => {
                    fallback = true;
                    copy_across_volumes(temp, target).await
                }
                Err(err) => Err(err.into()),
            }
        }
        .await;
        // Remove the source temp only where it survives (item 7c): on
        // the rename success path the temp is already gone — the old
        // unconditional remove was a per-PUT NotFound probe; on failure
        // it is partial residue, and on the EXDEV fallback success the
        // copy did not consume it.
        if fallback || result.is_err() {
            let _ = fs::remove_file(temp).await;
        }
        // D1 — the rename is not durable until the directory entries are
        // synced (the content was synced before the rename). The rename
        // is the COMMIT POINT (F06): a post-rename sync failure no longer
        // fails the write — the object is visible and correct, and the
        // failure is warned. Both crash directions self-heal (a durable
        // meta row for a file lost on power loss is reclaimed as an
        // orphan; a lost rename's row is recomputed from the content,
        // FR-022), while failing the request would leave the object
        // visible WITHOUT a success (or a row) — strictly worse.
        if let Ok(()) = result
            && let Some(parent) = target.parent()
        {
            if created_parent {
                // F03: the first PUT into a new prefix — the whole
                // ancestor chain is new, and only the chain's directory
                // entries make the rename's path durable. Sync every
                // ancestor up to (and including) `sync_root`; the leaf
                // alone would leave the parent-chain entries unsynced and
                // invert the durability promise.
                Self::sync_ancestor_chain(parent, sync_root).await;
            } else {
                Self::sync_dir_warned(parent).await;
            }
        }
        result
    }

    /// Sync `dir` (D1); a failure is warned, never fatal (F06 — see
    /// [`Self::commit`]).
    async fn sync_dir_warned(dir: &Path) {
        if let Err(err) = sync_parent_dir(dir).await {
            tracing::warn!(path = %dir.display(), error = %err, "directory sync failed after a committed rename");
        }
    }

    /// Sync every ancestor of `leaf` up to and including `sync_root`
    /// (F03 — the first commit into a new prefix created the chain). Each
    /// sync failure is warned and the chain walk continues (the closest
    /// surviving entry is the strongest durability the filesystem allows
    /// at that point; F06).
    async fn sync_ancestor_chain(leaf: &Path, sync_root: Option<&Path>) {
        let mut dir = Some(leaf);
        while let Some(d) = dir {
            Self::sync_dir_warned(d).await;
            if Some(d) == sync_root {
                break;
            }
            dir = d.parent();
        }
    }

    /// The tmp dir exists in steady state — one probe instead of the
    /// per-component walk (the create still runs when the sweep cleared
    /// it). Shared by [`Self::stage`] and [`Self::stage_copy`].
    async fn ensure_tmp_dir(&self) -> io::Result<()> {
        fsutil::ensure_dir(&self.tmp_dir).await?;
        Ok(())
    }

    /// Stream `body` into a fresh temp file under `tmp/`, returning the
    /// temp path and content MD5. The caller controls when the temp
    /// becomes visible (rename under its own lock); on failure the temp is
    /// removed best-effort. `checksum` is the server's tee slot: with
    /// `etag_md5` the slot already holds the content MD5 (a part's ETag
    /// IS its content MD5), so the write is not hashed a second time.
    pub(crate) async fn stage(
        &self,
        body: BodyStream,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(PathBuf, ETag), Error> {
        self.ensure_tmp_dir().await?;
        let temp = self.tmp_dir.join(format!("upload-{}", Uuid::new_v4()));
        let result = self.write_temp(&temp, body, checksum).await;
        match result {
            Ok(etag) => Ok((temp, etag)),
            Err(err) => {
                let _ = fs::remove_file(&temp).await;
                Err(err)
            }
        }
    }

    /// The stream+hash core: drain `body` into `temp` with bounded
    /// buffers, returning the content MD5 (from the tee slot when
    /// `etag_md5` — the write skips its own hash).
    async fn write_temp(
        &self,
        temp: &Path,
        body: BodyStream,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<ETag, Error> {
        let mut file = File::create(temp).await?;
        let mut hasher = (!checksum.is_some_and(|c| c.etag_md5)).then(Md5::new);
        let mut stream = pin!(body);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            // Bound the copy: a single oversized chunk never buffers
            // whole — it is drained in bounded slices.
            for slice in chunk.as_ref().chunks(CHUNK_SIZE) {
                file.write_all(slice).await?;
                if let Some(hasher) = &mut hasher {
                    hasher.update(slice);
                }
            }
        }
        file.flush().await?;
        // D1 — content durability: the bytes must be on disk before the
        // rename makes the name visible (the rename's directory entry is
        // synced by `commit`). Also covers the multipart part files —
        // "parts survive restarts" (multipart.rs) now holds for content.
        // The per-part sync doubles the part's documented flush cost (the
        // UPLOADS/PARTS row commits with `Durability::Immediate`), a
        // deliberate tradeoff: parts are re-uploadable, so an operator
        // could drop this sync if the cost matters — the assemble-time
        // sync still covers the completed object (F26, documented).
        file.sync_all().await?;
        match hasher {
            // The write was hashed inline.
            Some(hasher) => Ok(ETag::Single(hasher.finalize().into())),
            // The tee's MD5 (etag_md5): decode the wire base64 into the
            // raw digest — the tee fills the slot before the stream's
            // final `None`, so the value is there.
            None => {
                let slot = checksum
                    .and_then(|c| c.digest.get())
                    .expect("the etag_md5 tee fills the slot at stream end");
                let digest = slot
                    .value
                    .md5_raw()
                    .expect("the tee's md5 is valid and 16 bytes");
                Ok(ETag::Single(digest))
            }
        }
    }

    /// Stage `len` bytes at `offset` of an already-open `source` into a
    /// fresh temp under `tmp/` — the copy primitives' fast path
    /// (CopyObject/UploadPartCopy): kernel-side `copy_file_range` (unix
    /// only — the primitives fall back to the contract's stream path
    /// elsewhere), then the content MD5 of the copied bytes (a second
    /// read pass over the temp — the kernel did the copy, so the hash
    /// has no write side to piggyback on) and a content sync (D1 — the
    /// same durability the streaming stage gives). Returns
    /// `(temp, etag)`; the temp is removed best-effort on failure.
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
            let mut dst = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_task)
                .map_err(Error::from)?;
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
        // A panicking copy closure re-panics the caller, consistent with
        // `meta::ensure_etag`'s blocking-pool hash (a panic is a bug,
        // not a recoverable IO error).
        .unwrap_or_else(|join| panic!("the file-copy task panicked: {join}"));
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
    use std::fs::read_dir;

    use bytes::Bytes;
    use futures::stream;
    use io::Error as IoError;

    use super::*;
    use crate::_util::testing::{body, etag};

    #[tokio::test]
    async fn write_stores_content_and_etag() {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("obj.bin");
        let got = writer.write(&target, body(b"hello world")).await.unwrap();
        assert_eq!(got, etag("5eb63bbbe01eeed093cb22bb8f5acdc3"));
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
        assert_eq!(got, etag("d41d8cd98f00b204e9800998ecf8427e"));
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
        let (temp, _) = writer.stage(body(b"x"), None).await.unwrap();
        // rename(file, existing-directory) fails on every platform — the
        // target stays untouched and the failed temp is removed (item 7c).
        let err = AtomicWriter::commit(&temp, &target, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)));
        assert!(
            !fs::try_exists(&temp).await.unwrap(),
            "the failed commit must remove its temp residue"
        );
    }

    /// The parent-dir fsync of a real directory succeeds (D1 — the
    /// durability step behind every committed object).
    #[cfg(unix)]
    #[tokio::test]
    async fn sync_parent_dir_syncs_a_real_directory() {
        let dir = tempfile::tempdir().unwrap();
        sync_parent_dir(dir.path()).await.unwrap();
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
        let result = AtomicWriter::commit(&temp, &target, Some(&root)).await;
        fs::set_permissions(&root, Permissions::from_mode(0o755))
            .await
            .unwrap();
        // The rename landed and the commit succeeded — the unsyncable
        // ancestor was warned, not fatal.
        assert_eq!(fs::read(&target).await.unwrap(), b"x");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn copy_across_volumes_lands_content_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path().join("staged.bin");
        fs::write(&temp, b"cross-volume payload").await.unwrap();
        let target = dir.path().join("sub").join("obj.bin");
        fs::create_dir_all(target.parent().unwrap()).await.unwrap();
        copy_across_volumes(&temp, &target).await.unwrap();
        assert_eq!(fs::read(&target).await.unwrap(), b"cross-volume payload");
        // No staging file left behind on success.
        let entries: Vec<_> = read_dir(dir.path().join("sub"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, ["obj.bin"], "{entries:?}");
        // A failed copy cleans up the staging file and directory
        // (no `.tinio` residue).
        let target2 = dir.path().join("sub2").join("obj.bin");
        fs::create_dir_all(target2.parent().unwrap()).await.unwrap();
        let err = copy_across_volumes(&dir.path().join("missing"), &target2)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert!(!target2.parent().unwrap().join(".tinio").exists());
    }
}
