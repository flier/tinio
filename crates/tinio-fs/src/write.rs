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

use std::{
    io,
    path::{Path, PathBuf},
    pin::pin,
};

use futures::StreamExt;
use md5::{Digest, Md5};
use tinio_core::{BodyStream, ETag, object::RESERVED_SEGMENT};

use crate::Error;
use crate::path::TMP_DIR_NAME;

/// Bounded chunk size for the streaming copy/hash loops (constitution V:
/// no per-object buffering; hyper chunks are typically ≤ 64 KiB anyway).
pub(crate) const CHUNK_SIZE: usize = 64 * 1024;

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
    tokio::fs::create_dir_all(&staging_dir).await?;
    let staging = staging_dir.join(uuid::Uuid::new_v4().to_string());
    let copied = async {
        tokio::fs::copy(temp, &staging).await?;
        tokio::fs::rename(&staging, target).await?;
        Ok::<_, Error>(())
    }
    .await;
    if copied.is_err() {
        let _ = tokio::fs::remove_file(&staging).await;
    }
    // Remove the staging directory when empty (a concurrent EXDEV write
    // may still be staging in it — the removal then fails harmlessly).
    let _ = tokio::fs::remove_dir(&staging_dir).await;
    copied
}

/// The streaming content MD5 of the file at `path` plus the metadata of
/// the opened file (bounded buffers; the metadata is the caller's file
/// identity — one open serves both, no second stat of the path).
pub(crate) async fn md5_of_file(path: &Path) -> Result<([u8; 16], std::fs::Metadata), Error> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Md5::new();
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let metadata = file.metadata().await?;
    Ok((hasher.finalize().into(), metadata))
}

/// Atomic object-body writer over the state-dir `tmp/` staging area.
///
/// # Examples
///
/// ```rust
/// use tinio_util::testing::body;
/// use tinio_fs::AtomicWriter;
///
/// let state = tempfile::tempdir().unwrap();
/// let writer = AtomicWriter::new(state.path());
/// let target = state.path().join("obj.bin");
/// let etag = tokio::runtime::Runtime::new()
///     .unwrap()
///     .block_on(writer.write(&target, body(b"hello")))
///     .unwrap();
/// assert_eq!(etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
/// assert_eq!(std::fs::read(&target).unwrap(), b"hello");
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
        let (temp, etag) = self.stage(body).await?;
        Self::commit(&temp, target).await?;
        Ok(etag)
    }

    /// Rename a staged temp onto `target` (creating parent directories);
    /// the temp is removed best-effort on failure. Callers that serialize
    /// bucket mutations hold their lock across this call.
    ///
    /// A cross-volume state dir (FR-023 relocation) makes `rename` fail
    /// with `CrossesDevices` — the fallback copies the temp through a
    /// unique staging file **on the target volume**, then renames (atomic
    /// there; readers still see the old object or the new one, never a
    /// torn mix). The staging file lives in the target directory's
    /// `.tinio/` reserved segment — a crash between copy and rename
    /// leaves invisible residue (never served or listed, FR-020), not a
    /// stray object.
    pub(crate) async fn commit(temp: &Path, target: &Path) -> Result<(), Error> {
        let result = async {
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            match tokio::fs::rename(temp, target).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == io::ErrorKind::CrossesDevices => {
                    copy_across_volumes(temp, target).await
                }
                Err(err) => Err(err.into()),
            }
        }
        .await;
        // Remove the source temp on every outcome: on failure it is
        // partial residue; on the EXDEV fallback success the copy did not
        // consume it (on the rename success path it is already gone — a
        // harmless NotFound).
        let _ = tokio::fs::remove_file(temp).await;
        result
    }

    /// Stream `body` into a fresh temp file under `tmp/`, returning the
    /// temp path and content MD5. The caller controls when the temp
    /// becomes visible (rename under its own lock); on failure the temp is
    /// removed best-effort.
    pub(crate) async fn stage(&self, body: BodyStream) -> Result<(PathBuf, ETag), Error> {
        tokio::fs::create_dir_all(&self.tmp_dir).await?;
        let temp = self
            .tmp_dir
            .join(format!("upload-{}", uuid::Uuid::new_v4()));
        let result = self.write_temp(&temp, body).await;
        match result {
            Ok(etag) => Ok((temp, etag)),
            Err(err) => {
                let _ = tokio::fs::remove_file(&temp).await;
                Err(err)
            }
        }
    }

    /// The stream+hash core: drain `body` into `temp` with bounded
    /// buffers, returning the content MD5.
    async fn write_temp(&self, temp: &Path, body: BodyStream) -> Result<ETag, Error> {
        let mut file = tokio::fs::File::create(temp).await?;
        let mut hasher = Md5::new();
        let mut stream = pin!(body);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            // Bound the copy: a single oversized chunk never buffers
            // whole — it is drained in bounded slices.
            for slice in chunk.as_ref().chunks(CHUNK_SIZE) {
                tokio::io::AsyncWriteExt::write_all(&mut file, slice).await?;
                hasher.update(slice);
            }
        }
        tokio::io::AsyncWriteExt::flush(&mut file).await?;
        Ok(ETag::Single(hasher.finalize().into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use std::io;
    use tinio_util::testing::{body, etag};

    #[test]
    fn write_stores_content_and_etag() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let writer = AtomicWriter::new(state.path());
            let target = state.path().join("obj.bin");
            let got = writer.write(&target, body(b"hello world")).await.unwrap();
            assert_eq!(got, etag("5eb63bbbe01eeed093cb22bb8f5acdc3"));
            assert_eq!(tokio::fs::read(&target).await.unwrap(), b"hello world");
            // No temp files left behind on success.
            let tmp = state.path().join("tmp");
            let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
            assert!(entries.next_entry().await.unwrap().is_none());
        });
    }

    #[test]
    fn write_creates_parent_directories() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let writer = AtomicWriter::new(state.path());
            let target = state.path().join("dir/sub/deep/obj.txt");
            writer.write(&target, body(b"x")).await.unwrap();
            assert_eq!(tokio::fs::read(&target).await.unwrap(), b"x");
        });
    }

    #[test]
    fn write_zero_bytes() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let writer = AtomicWriter::new(state.path());
            let target = state.path().join("empty");
            let got = writer.write(&target, body(b"")).await.unwrap();
            assert_eq!(got, etag("d41d8cd98f00b204e9800998ecf8427e"));
            assert_eq!(tokio::fs::metadata(&target).await.unwrap().len(), 0);
        });
    }

    #[test]
    fn write_last_writer_wins() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let writer = AtomicWriter::new(state.path());
            let target = state.path().join("obj");
            writer.write(&target, body(b"first")).await.unwrap();
            writer.write(&target, body(b"second")).await.unwrap();
            assert_eq!(tokio::fs::read(&target).await.unwrap(), b"second");
        });
    }

    #[test]
    fn interrupted_upload_leaves_no_partial_object() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let writer = AtomicWriter::new(state.path());
            let target = state.path().join("obj");
            // A stream that yields one good chunk then fails.
            let stream = futures::stream::iter(vec![
                Ok(bytes::Bytes::from_static(b"partial")),
                Err(io::Error::other("connection reset")),
            ]);
            let body: BodyStream = Box::pin(stream);
            let err = writer.write(&target, body).await.unwrap_err();
            assert!(matches!(err, Error::Io(_)));
            // The target never appears (previous version absent); the temp
            // file is removed best-effort (or swept later if cleanup raced).
            assert!(tokio::fs::metadata(&target).await.is_err());
        });
    }

    #[test]
    fn copy_across_volumes_lands_content_atomically() {
        // The EXDEV fallback (a cross-volume state dir cannot `rename`):
        // copy through a staging file next to the target, then rename —
        // the mechanism test; the trigger is the `CrossesDevices` match.
        rt(async {
            let dir = tempfile::tempdir().unwrap();
            let temp = dir.path().join("staged.bin");
            tokio::fs::write(&temp, b"cross-volume payload")
                .await
                .unwrap();
            let target = dir.path().join("sub").join("obj.bin");
            tokio::fs::create_dir_all(target.parent().unwrap())
                .await
                .unwrap();
            copy_across_volumes(&temp, &target).await.unwrap();
            assert_eq!(
                tokio::fs::read(&target).await.unwrap(),
                b"cross-volume payload"
            );
            // No staging file left behind on success.
            let entries: Vec<_> = std::fs::read_dir(dir.path().join("sub"))
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            assert_eq!(entries, ["obj.bin"], "{entries:?}");
            // A failed copy cleans up the staging file and directory
            // (no `.tinio` residue).
            let target2 = dir.path().join("sub2").join("obj.bin");
            tokio::fs::create_dir_all(target2.parent().unwrap())
                .await
                .unwrap();
            let err = copy_across_volumes(&dir.path().join("missing"), &target2)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Io(_)), "{err:?}");
            assert!(!target2.parent().unwrap().join(".tinio").exists());
        });
    }
}
