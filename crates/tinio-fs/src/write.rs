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
    path::{Path, PathBuf},
    pin::pin,
};

use futures::StreamExt;
use md5::{Digest, Md5};
use tinio_core::{BodyStream, ETag};

use crate::Error;
use crate::path::TMP_DIR_NAME;

/// Bounded chunk size for the streaming copy/hash loops (constitution V:
/// no per-object buffering; hyper chunks are typically ≤ 64 KiB anyway).
pub(crate) const CHUNK_SIZE: usize = 64 * 1024;

/// The streaming content MD5 of the file at `path` (bounded buffers).
pub(crate) async fn md5_of_file(path: &Path) -> Result<[u8; 16], Error> {
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
    Ok(hasher.finalize().into())
}

/// Atomic object-body writer over the state-dir `tmp/` staging area.
///
/// # Examples
///
/// ```rust
/// use tinio_core::testing::body;
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
    pub(crate) async fn commit(temp: &Path, target: &Path) -> Result<(), Error> {
        let result = async {
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::rename(temp, target).await?;
            Ok::<_, Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(temp).await;
        }
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

    /// Atomically write a small byte payload onto `target` (temp file
    /// under `tmp/` + rename — same volume by construction).
    ///
    /// Used by the meta store, `buckets.json`, and multipart sidecars;
    /// callers hold their own in-process lock so concurrent writers never
    /// produce torn JSON. Staging under `tmp/` means a crash between
    /// temp-write and rename leaves a file the startup repair / sweep
    /// reclaims.
    pub async fn write_bytes(&self, target: &Path, bytes: &[u8]) -> Result<(), Error> {
        tokio::fs::create_dir_all(&self.tmp_dir).await?;
        let temp = self.tmp_dir.join(format!("state-{}", uuid::Uuid::new_v4()));
        if let Err(err) = tokio::fs::write(&temp, bytes).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err.into());
        }
        Self::commit(&temp, target).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use std::io;
    use tinio_core::testing::{body, etag};

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
    fn write_bytes_writes_and_replaces() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let writer = AtomicWriter::new(state.path());
            let target = state.path().join("meta.json");
            writer.write_bytes(&target, b"{\"v\":1}").await.unwrap();
            assert_eq!(
                tokio::fs::read_to_string(&target).await.unwrap(),
                "{\"v\":1}"
            );
            writer.write_bytes(&target, b"{\"v\":2}").await.unwrap();
            assert_eq!(
                tokio::fs::read_to_string(&target).await.unwrap(),
                "{\"v\":2}"
            );
            // No stray temp files under tmp/.
            let tmp = state.path().join("tmp");
            let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
            assert!(entries.next_entry().await.unwrap().is_none());
        });
    }

    #[test]
    fn write_bytes_concurrent_writers_never_torn() {
        rt(async {
            let state = tempfile::tempdir().unwrap();
            let writer = AtomicWriter::new(state.path());
            let target = state.path().join("meta.json");
            let mut handles = Vec::new();
            for i in 0..8u32 {
                let target = target.clone();
                let writer = writer.clone();
                handles.push(tokio::spawn(async move {
                    let payload = format!(r#"{{"writer":{i},"pad":"{}"}}"#, "x".repeat(1000));
                    writer
                        .write_bytes(&target, payload.as_bytes())
                        .await
                        .unwrap();
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            // The final file is exactly one writer's complete payload.
            let final_content = tokio::fs::read_to_string(&target).await.unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&final_content).unwrap();
            let writer = parsed["writer"].as_u64().unwrap();
            let pad = "x".repeat(1000);
            assert_eq!(parsed["pad"].as_str().unwrap(), pad);
            assert!(writer < 8);
        });
    }
}
