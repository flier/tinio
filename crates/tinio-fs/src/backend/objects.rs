//! Object operations of the fs backend (task T042).
//!
//! Objects are files under `<root>/<bucket>/`. Writes stream through the
//! atomic writer (last-write-wins, never a torn object, FR-011); reads
//! stream with bounded buffers and support byte ranges; ETags come from the
//! meta store with streaming recompute on missing/stale entries (FR-022).
//! Folder-marker keys (ending in `/`) never become objects: PUT creates the
//! directory, GET/HEAD report `NoSuchKey`, DELETE removes an empty
//! directory and always succeeds. Reserved `.tinio` segments are refused
//! (FR-020); symlink resolution is rejected when `follow_symlinks` is
//! disabled.

use std::{
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    time::SystemTime,
};

use bytes::BufMut;

use tinio_core::{
    BodyStream, ETag, bucket,
    object::{self, Info},
    storage::{
        ByteRange, GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps, PutObjectResult,
        access_denied, no_such_bucket, no_such_key,
    },
};
use tokio::fs::File;

use crate::write::{AtomicWriter, CHUNK_SIZE};

use super::{Error, FsStorage};

/// An empty body stream (a zero-byte object with no range).
fn empty_stream() -> BodyStream {
    Box::pin(futures::stream::empty())
}

/// A staged object body: a temp file under `tmp/` (with its streaming
/// MD5), or the folder-marker sentinel (no body — the commit creates the
/// directory).
///
/// Dropping a staged body that was never committed removes the temp
/// file: a rejected conditional PUT (412) or an aborted request must not
/// leave its full body in `tmp/` for the sweep.
///
/// The `ObjectOps::StagedBody` associated type of the fs backend (the
/// server's two-phase put staging contract); construction happens only
/// through [`ObjectOps::stage_body`](tinio_core::storage::ObjectOps).
#[derive(Debug)]
pub struct StagedBody {
    temp: Option<PathBuf>,
    etag: ETag,
}

impl StagedBody {
    /// The content ETag (or the folder-marker sentinel's empty ETag).
    pub(crate) fn etag(&self) -> &ETag {
        &self.etag
    }

    /// The staged temp file, if any — consumed by the commit (a dropped
    /// `StagedBody` then cleans nothing).
    pub(crate) fn into_temp(mut self) -> Option<PathBuf> {
        self.temp.take()
    }
}

impl Drop for StagedBody {
    fn drop(&mut self) {
        // Never committed: remove the temp file best-effort (the startup
        // repair would sweep it otherwise). The unlink runs off the
        // request thread when a runtime is present (item 7a — the drop
        // can land on an async thread, e.g. a rejected conditional PUT);
        // without a runtime (tests, sync contexts) the sync fallback
        // keeps the best-effort cleanup. The temp name is a fresh UUID —
        // the async removal cannot race a later writer.
        if let Some(temp) = self.temp.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                // Detached on purpose: the blocking task runs the unlink
                // off the request thread (dropping the handle detaches).
                // catch_unwind: a shutting-down runtime still reports a
                // current handle, but `spawn_blocking` then panics — a
                // staged body dropped in a cancelled request during
                // teardown must not panic inside `Drop`. The caught
                // panic loses only the best-effort unlink; the startup
                // sweep is the backstop.
                let _ = std::panic::catch_unwind(|| {
                    std::mem::drop(tokio::task::spawn_blocking(move || {
                        std::fs::remove_file(&temp)
                    }));
                });
            } else {
                let _ = std::fs::remove_file(&temp);
            }
        }
    }
}

/// Stream the inclusive byte range `start..=end` of an open file in bounded
/// chunks (constitution V: no per-object buffering). The stream state
/// holds one `BytesMut`: each chunk is read into spare capacity via
/// `read_buf` and handed off via `split().freeze()` — the frozen `Bytes`
/// owns its memory, so reusing the buffer never invalidates a chunk the
/// consumer still holds. The old per-chunk copy into a fresh `Bytes` is
/// gone, but the read is not fully zero-copy: tokio's `File` reads into
/// an internal buffer on the blocking pool and copies once into the
/// caller's buffer (`Buf::read_from` + `copy_to`, tokio `src/fs/file.rs`
/// / `src/io/blocking.rs`); a true zero-copy read would need
/// `std::fs::File` + manual `spawn_blocking` + `Read::read_buf` (out of
/// scope).
async fn file_stream(file: File, start: u64, end: u64) -> BodyStream {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = file;
    if let Err(err) = file.seek(SeekFrom::Start(start)).await {
        return Box::pin(futures::stream::once(async move { Err(err) }));
    }
    let remaining = end.saturating_sub(start) + 1;
    Box::pin(futures::stream::try_unfold(
        (file, remaining, bytes::BytesMut::with_capacity(CHUNK_SIZE)),
        |(mut file, remaining, mut buf)| async move {
            if remaining == 0 {
                return Ok(None);
            }
            let want = remaining.min(CHUNK_SIZE as u64) as usize;
            // The previous chunk's `split` moved the filled prefix out of
            // `buf`, so make room for a full `want` again: when the
            // consumer already released the frozen chunk the reserve
            // reclaims the tail; a consumer still holding it forces a
            // fresh allocation (inherent to ownership handoff —
            // transiently two live buffers, the same shape as the old
            // per-chunk `Bytes`).
            buf.reserve(want);
            // Cap the read at `want`: `read_buf` would otherwise fill the
            // whole spare capacity and overrun the range end.
            let mut limited = (&mut buf).limit(want);
            match file.read_buf(&mut limited).await {
                Ok(0) => Ok(None),
                Ok(n) => {
                    let left = remaining - n as u64;
                    let chunk = buf.split().freeze();
                    Ok(Some((chunk, (file, left, buf))))
                }
                Err(err) => Err(err),
            }
        },
    ))
}

impl FsStorage {
    /// The bucket directory must exist (`NoSuchBucket`). A symlinked/
    /// junction bucket directory fails the containment proof when
    /// `follow_symlinks` is disabled — it is invisible to the data plane,
    /// so the direct ops answer `NoSuchBucket` (matching the listing's
    /// empty answer and its absence from `list_buckets`).
    pub(crate) async fn ensure_bucket(&self, name: &bucket::Name) -> Result<PathBuf, Error> {
        let dir = match self.bucket_dir(name).await {
            Ok(dir) => dir,
            Err(err) => {
                if !self.follow_symlinks
                    && tokio::fs::symlink_metadata(self.root().join(&**name))
                        .await
                        .map(|m| crate::fsutil::is_symlink_or_reparse(&m))
                        .unwrap_or(false)
                {
                    return Err(no_such_bucket(name).into());
                }
                return Err(err);
            }
        };
        match tokio::fs::metadata(&dir).await {
            Ok(metadata) if metadata.is_dir() => Ok(dir),
            Ok(_) => Err(no_such_bucket(name).into()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(no_such_bucket(name).into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Reject access resolving through a symlink when `follow_symlinks` is
    /// disabled (s3-surface.md). Checks every existing component of `path`
    /// (missing components are skipped — the parents may not exist yet).
    pub(crate) async fn check_symlinks(&self, key: &object::Key, path: &Path) -> Result<(), Error> {
        if self.follow_symlinks {
            return Ok(());
        }
        let mut current = path.to_path_buf();
        // Walk from the file up to (but excluding) the storage root.
        loop {
            match tokio::fs::symlink_metadata(&current).await {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(access_denied(key).into());
                }
                Ok(_) => {}
                Err(_) => {} // missing component: keep checking ancestors
            }
            if current == *self.root() {
                return Ok(());
            }
            if !current.pop() {
                return Ok(());
            }
        }
    }

    /// Resolve the object file for a read or a copy — the shared head of
    /// `get_object`/`head_object`/the copy primitives. One open serves
    /// the metadata and the bytes (P5 — the old path-based stat +
    /// separate open is a single policy open + `file.metadata()` on the
    /// handle), so size, mtime, and the streamed bytes all describe the
    /// same file — a swap between the stat and the open can no longer
    /// split them. Folder markers (and directories) are never objects
    /// (`NoSuchKey`). The ETag resolution (the meta store) is the
    /// caller's — the copy primitives need the file without it.
    pub(crate) async fn resolve_object_file(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(PathBuf, File, u64, SystemTime, u64), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Reads of reserved keys report NoSuchKey (FR-020).
        if key.is_reserved() {
            return Err(no_such_key(key).into());
        }
        let path = self.resolve_key(&bucket_dir, key).await?;
        // One policy open: nofollow when following is disabled (a swap
        // to a symlink between the resolve and the open is rejected, R3
        // — the ELOOP → PermissionDenied normalization included).
        let file = match crate::fsutil::open_file(&path, self.follow_symlinks).await {
            Ok(file) => file,
            // Missing: the old path-stat's NotFound mapping moves to the
            // open (the merged read resolves the object in one step).
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(no_such_key(key).into());
            }
            // Windows opens a directory handle only with
            // FILE_FLAG_BACKUP_SEMANTICS, so a folder-marker open fails
            // exactly like a permission denial. One stat classifies: a
            // directory is a folder marker (NoSuchKey — never an error),
            // anything else keeps the original error (the symlink-policy
            // answer when following is disabled, the I/O error
            // otherwise). Both-fail corner (the stat too — an
            // unsearchable parent): the follow=false answer is
            // AccessDenied, where the old path-stat reported Io (500).
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                let is_dir = crate::fsutil::object_metadata(&path, self.follow_symlinks)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                if is_dir {
                    return Err(no_such_key(key).into());
                }
                if !self.follow_symlinks {
                    return Err(access_denied(key).into());
                }
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };
        let metadata = match file.metadata().await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(no_such_key(key).into());
            }
            Err(err) => return Err(err.into()),
        };
        // Folder markers (and directories) are never objects. (The open
        // already rejected a reparse-point leaf when following is
        // disabled — the old lstat leaf check is the open's job now.)
        if metadata.is_dir() {
            return Err(no_such_key(key).into());
        }
        let size = metadata.len();
        let mtime = metadata.modified()?;
        // The file identity the gate consults (F01) — a same-size
        // mtime-preserving replacement is never served a stale ETag. It
        // comes from the ALREADY-OPEN handle above (R3 — never a second
        // path-based open; `tokio::fs::File::into_std()` bridges the
        // handle safely, so a replacement between two opens is
        // impossible by construction).
        let std_file = file.into_std().await;
        let identity = crate::fsutil::file_identity_handle(&std_file, &metadata);
        let file = tokio::fs::File::from_std(std_file);
        Ok((path, file, size, mtime, identity))
    }

    /// Resolve the object for a read — the shared head of
    /// `get_object`/`head_object`: the file resolution plus the ETag
    /// resolution against the meta store.
    async fn resolve_object_info(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(File, u64, SystemTime, ETag), Error> {
        let (path, file, size, mtime, identity) = self.resolve_object_file(bucket, key).await?;
        let etag = self
            .meta_store
            .etag_for_file(
                bucket,
                key,
                &path,
                size,
                mtime,
                identity,
                self.follow_symlinks,
            )
            .await?;
        Ok((file, size, mtime, etag))
    }

    /// The unix fast path of the copy primitive: the kernel's
    /// `copy_file_range` moves the bytes (zero userspace buffering), the
    /// source's single-form ETag is reused when the file is provably
    /// unchanged (AWS semantics — the content MD5 of a full copy is the
    /// source's), and the shared commit publishes the destination under
    /// the mutation lock.
    #[cfg(unix)]
    async fn copy_object_fast(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
    ) -> Result<PutObjectResult, Error> {
        // Folder-marker destination: the sentinel commit (mirrors
        // `stage_body`'s marker handling) — no bytes are copied.
        if dst_key.is_folder_marker() {
            return self
                .commit_object(
                    dst_bucket,
                    dst_key,
                    StagedBody {
                        temp: None,
                        etag: ETag::EMPTY,
                    },
                )
                .await;
        }
        let (path, file, size, mtime, identity) =
            self.resolve_object_file(src_bucket, src_key).await?;
        // The source's stored ETag when it still matches the open file
        // — reused only in its single form: a composed source's copy is
        // a fresh single-part object whose canonical ETag is the content
        // MD5 of the copied bytes.
        let stored = self
            .meta_store
            .etag_matching(src_bucket, src_key, size, mtime, identity)
            .await?;
        let std_file = file.into_std().await;
        let (temp, staged_etag) = self.writer.stage_copy(std_file, 0, size).await?;
        // Torn-copy guard: the reuse is valid only when the source is
        // byte-identical to the pre-copy stat — a mid-copy change makes
        // the staged bytes self-consistent with their own hash, never
        // with the source's stale ETag.
        let unchanged = tokio::fs::metadata(&path).await.is_ok_and(|now| {
            now.len() == size
                && now.modified().ok() == Some(mtime)
                && crate::fsutil::file_identity(&path, &now) == identity
        });
        let etag = match stored {
            Some(etag @ ETag::Single(_)) if unchanged => etag,
            _ => staged_etag,
        };
        self.commit_object(
            dst_bucket,
            dst_key,
            StagedBody {
                temp: Some(temp),
                etag,
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl ObjectOps for FsStorage {
    /// A staged body: a temp file under `tmp/` (with its streaming MD5),
    /// or the folder-marker sentinel (no body — the commit creates the
    /// directory). Dropping an uncommitted staged body removes the temp.
    type StagedBody = StagedBody;

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<StagedBody, Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Symlink policy applies to markers too — a PUT `sub/dir/` whose
        // parent is a link must not create a directory outside the root.
        // The resolution is a validation gate here (commit re-resolves).
        self.resolve_key(&bucket_dir, key).await?;
        // Folder markers are never objects (s3-surface.md): no body is
        // staged — the commit creates the directory. The sentinel
        // carries the marker's empty-content ETag.
        if key.is_folder_marker() {
            return Ok(StagedBody {
                temp: None,
                etag: ETag::EMPTY,
            });
        }
        let (temp, etag) = self.writer.stage(body).await?;
        Ok(StagedBody {
            temp: Some(temp),
            etag,
        })
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody,
    ) -> Result<PutObjectResult, Error> {
        // The single resolve runs under the mutation lock (P5): the
        // pre-lock fail-fast pair was redundant — `stage_body` already
        // validated the key before the body streamed, and the
        // authoritative check must re-run under the lock anyway (a
        // followed bucket symlink retargeted between stage and rename
        // must not send the write to a stale target).
        let etag = staged.etag().clone();
        // Folder markers are never objects (s3-surface.md): PUT creates
        // the directory, idempotently. Under the mutation lock with the
        // same guards as a real object — a concurrent delete_bucket must
        // not be resurrected, and the symlink policy applies to markers
        // too.
        if key.is_folder_marker() {
            let _guard = self.lock_bucket_mutations(bucket).await;
            let bucket_dir = self.ensure_bucket(bucket).await?;
            let target = self.resolve_key(&bucket_dir, key).await?;
            tokio::fs::create_dir_all(&target).await?;
            return Ok(PutObjectResult { etag });
        }
        // A real object always arrives with a staged temp (the marker
        // branch above consumed the sentinel).
        let Some(temp) = staged.into_temp() else {
            return Err(io::Error::other("staged body without a temp file").into());
        };
        // The rename is the bucket-mutating step: under the mutation
        // lock it cannot race a `delete_bucket` (a PUT that loses
        // reports an error instead of silently losing the object with
        // 200). A failed commit removes the staged temp — a rejected
        // write leaves no residue.
        let result = async {
            let _guard = self.lock_bucket_mutations(bucket).await;
            let bucket_dir = self.ensure_bucket(bucket).await?;
            let target = self.resolve_key(&bucket_dir, key).await?;
            // F03: the bucket root bounds the first-into-a-new-prefix
            // ancestor sync.
            AtomicWriter::commit(&temp, &target, Some(&bucket_dir)).await?;
            let metadata = tokio::fs::metadata(&target).await?;
            // The object is committed — a meta-write failure (full
            // state dir) must not fail the PUT: the entry is recomputed
            // from the content on the next read (self-healing, FR-022).
            let identity = crate::fsutil::file_identity(&target, &metadata);
            if let Err(err) = self
                .meta_store
                .set(
                    bucket,
                    key,
                    &etag,
                    metadata.len(),
                    metadata.modified()?,
                    identity,
                )
                .await
            {
                tracing::warn!(error = %err, "meta entry not persisted after commit");
            }
            Ok::<_, Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temp).await;
        }
        result?;
        Ok(PutObjectResult { etag })
    }

    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
    ) -> Result<PutObjectResult, Error> {
        #[cfg(unix)]
        {
            self.copy_object_fast(src_bucket, src_key, dst_bucket, dst_key)
                .await
        }
        #[cfg(not(unix))]
        {
            // No kernel copy primitive on Windows — the contract's
            // stream default (get → put).
            let get = self.get_object(src_bucket, src_key, None).await?;
            self.put_object(dst_bucket, dst_key, get.body).await
        }
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        let (file, size, mtime, etag) = self.resolve_object_info(bucket, key).await?;
        let (body, served_range) = match range {
            Some(range) => {
                let (start, end) = range.resolve(size)?;
                (file_stream(file, start, end).await, Some((start, end)))
            }
            None if size == 0 => {
                // No bytes exist to stream — not a 1-byte read, which
                // could overshoot if the file grew after the stat.
                (empty_stream(), None)
            }
            None => (file_stream(file, 0, size - 1).await, None),
        };
        Ok(GetObjectResult {
            info: Info {
                key: key.clone(),
                size,
                last_modified: mtime,
                etag,
            },
            body,
            served_range,
        })
    }

    async fn head_object(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Info, Error> {
        // The open is the metadata source too (the same merge as
        // get_object) — the handle is dropped right away.
        let (_file, size, mtime, etag) = self.resolve_object_info(bucket, key).await?;
        Ok(Info {
            key: key.clone(),
            size,
            last_modified: mtime,
            etag,
        })
    }

    async fn delete_object(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Like every other object op: never resolve through a symlink
        // when `follow_symlinks` is disabled (a DELETE through a linked
        // directory would remove a file outside the storage root).
        let path = self.resolve_key(&bucket_dir, key).await?;
        if key.is_folder_marker() {
            // Remove the directory only when it is empty; a non-empty
            // directory is left in place (s3-surface.md). Always 204 —
            // a missing or non-directory path (e.g. a symlink) is
            // idempotently "deleted".
            match tokio::fs::remove_dir(&path).await {
                Ok(()) => {}
                Err(err)
                    if err.kind() == io::ErrorKind::NotFound
                        || err.kind() == io::ErrorKind::DirectoryNotEmpty
                        || err.kind() == io::ErrorKind::NotADirectory =>
                {
                    // Idempotent / non-empty / wrong type: nothing to do.
                }
                Err(err) => return Err(err.into()),
            }
            return Ok(());
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            // Missing or a directory (DELETE of a marker key without the
            // trailing slash) — DELETE is idempotent, always 204.
            Err(err)
                if err.kind() == io::ErrorKind::NotFound
                    || err.kind() == io::ErrorKind::IsADirectory =>
            {
                // Nothing to do.
            }
            // Windows reports a directory as PermissionDenied: the
            // wrong-type no-op, not an I/O failure.
            Err(err)
                if err.kind() == io::ErrorKind::PermissionDenied
                    && tokio::fs::metadata(&path)
                        .await
                        .map(|m| m.is_dir())
                        .unwrap_or(false) =>
            {
                // Nothing to do.
            }
            Err(err) => return Err(err.into()),
        }
        self.meta_store.remove(bucket, key).await?;
        // Prune now-empty parent directories (best-effort): deleting the
        // last object under a prefix leaves no residue, so a bucket that
        // only ever held deleted objects is empty again (S3 semantics).
        let mut parent = path.parent().map(Path::to_path_buf);
        while let Some(dir) = parent {
            if dir == bucket_dir {
                break;
            }
            match tokio::fs::remove_dir(&dir).await {
                Ok(()) => parent = dir.parent().map(Path::to_path_buf),
                Err(_) => break, // non-empty or gone: stop pruning
            }
        }
        Ok(())
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        self.listing.list(&params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{fs_options, rt, storage};
    use futures::StreamExt;
    use md5::Digest;
    use tinio_core::object;
    use tinio_core::storage::BucketOps;
    use tinio_core::storage::Error as StorageError;
    #[cfg(unix)]
    use tinio_core::storage::MultipartOps;
    use tinio_util::testing::{assert_conformance, body, etag, read_body};

    #[test]
    fn conformance_green() {
        rt(async {
            let (_root, storage) = storage();
            assert_conformance(&storage).await;
        });
    }

    #[test]
    fn put_get_head_delete_round_trip() {
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("dir/a.txt").unwrap();

            let put = storage.put_object(&b, &k, body(b"hello")).await.unwrap();
            assert_eq!(put.etag, etag("5d41402abc4b2a76b9719d911017c592"));

            // The file physically appears in the directory.
            assert_eq!(
                tokio::fs::read(root.path().join("data/dir/a.txt"))
                    .await
                    .unwrap(),
                b"hello"
            );

            let head = storage.head_object(&b, &k).await.unwrap();
            assert_eq!(head.size, 5);
            assert_eq!(head.etag, put.etag);

            let get = storage.get_object(&b, &k, None).await.unwrap();
            assert_eq!(read_body(get.body).await.unwrap(), b"hello");
            assert!(get.served_range.is_none());
            assert_eq!(get.info.etag, put.etag);

            storage.delete_object(&b, &k).await.unwrap();
            let err = storage.head_object(&b, &k).await.unwrap_err();
            assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
        });
    }

    #[cfg(unix)]
    #[test]
    fn copy_object_fast_path_preserves_content_and_etag() {
        // The unix fast path (`copy_file_range` + single-form ETag
        // reuse): a cross-bucket copy is byte-identical and its ETag is
        // the content MD5 (the reused source ETag — the wire value is
        // the same either way).
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            let b2 = bucket::name("other").unwrap();
            storage.create_bucket(&b).await.unwrap();
            storage.create_bucket(&b2).await.unwrap();
            let src = object::key("src.bin").unwrap();
            let dst = object::key("dir/dst.bin").unwrap();
            storage
                .put_object(&b, &src, body(b"cross-bucket copy"))
                .await
                .unwrap();
            let put = storage.copy_object(&b, &src, &b2, &dst).await.unwrap();
            assert_eq!(put.etag, ETag::from_content(b"cross-bucket copy"));
            let get = storage.get_object(&b2, &dst, None).await.unwrap();
            assert_eq!(read_body(get.body).await.unwrap(), b"cross-bucket copy");
            assert_eq!(get.info.etag, put.etag);
        });
    }

    #[cfg(unix)]
    #[test]
    fn copy_of_a_multipart_source_yields_the_content_md5() {
        // The fast path reuses the source ETag only in its SINGLE form:
        // a composed source's copy is a fresh single-part object whose
        // canonical ETag is the content MD5 of the copied bytes — never
        // the stale `MD5-of-MD5s-N` form (the old stream path's re-hash
        // behavior, preserved).
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("big.bin").unwrap();
            let dst = object::key("copy.bin").unwrap();
            let upload = storage.create_multipart_upload(&b, &k).await.unwrap();
            let p1 = storage
                .upload_part(&b, &k, &upload.upload_id, 1.into(), body(b"part-one-"))
                .await
                .unwrap();
            let p2 = storage
                .upload_part(&b, &k, &upload.upload_id, 2.into(), body(b"part-two-"))
                .await
                .unwrap();
            let completed = [
                tinio_core::CompletedPart {
                    part_number: p1.part_number,
                    etag: p1.etag,
                },
                tinio_core::CompletedPart {
                    part_number: p2.part_number,
                    etag: p2.etag,
                },
            ];
            let info = storage
                .complete_multipart_upload(&b, &k, &upload.upload_id, &completed)
                .await
                .unwrap();
            assert!(matches!(info.etag, ETag::Composed(_, 2)));
            let put = storage.copy_object(&b, &k, &b, &dst).await.unwrap();
            assert_eq!(put.etag, ETag::from_content(b"part-one-part-two-"));
            let head = storage.head_object(&b, &dst).await.unwrap();
            assert_eq!(head.etag, ETag::from_content(b"part-one-part-two-"));
        });
    }

    #[test]
    fn copy_of_a_missing_source_is_no_such_key() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let missing = object::key("ghost.bin").unwrap();
            let dst = object::key("dst.bin").unwrap();
            let err = storage
                .copy_object(&b, &missing, &b, &dst)
                .await
                .unwrap_err();
            assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
        });
    }

    #[test]
    fn missing_bucket_is_no_such_bucket() {
        rt(async {
            let (_root, storage) = storage();
            let ghost = bucket::name("ghost").unwrap();
            let err: StorageError = storage
                .put_object(&ghost, &"a".into(), body(b"x"))
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::NoSuchBucket(_)));
        });
    }

    #[test]
    fn get_ranges() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("digits").unwrap();
            storage
                .put_object(&b, &k, body(b"0123456789"))
                .await
                .unwrap();

            let get = storage
                .get_object(&b, &k, Some(ByteRange::Inclusive(2, 5)))
                .await
                .unwrap();
            assert_eq!(get.served_range, Some((2, 5)));
            assert_eq!(read_body(get.body).await.unwrap(), b"2345");

            let get = storage
                .get_object(&b, &k, Some(ByteRange::From(7)))
                .await
                .unwrap();
            assert_eq!(read_body(get.body).await.unwrap(), b"789");

            let get = storage
                .get_object(&b, &k, Some(ByteRange::Suffix(3)))
                .await
                .unwrap();
            assert_eq!(read_body(get.body).await.unwrap(), b"789");

            let err: StorageError = storage
                .get_object(&b, &k, Some(ByteRange::From(99)))
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::InvalidRange { .. }));
        });
    }

    #[test]
    fn get_stream_chunk_sequence() {
        // The GET stream's chunk sequence (P4, data-path review 2026-08-27):
        // the zero-copy rewrite must keep the same chunking as the copied
        // stream — full CHUNK_SIZE chunks with the tail chunk exactly the
        // remaining bytes (a behavioral equivalence pin; memory-copy
        // assertions are impractical).
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("chunked.bin").unwrap();
            let payload: Vec<u8> = (0..(2 * CHUNK_SIZE + 1234) as u32)
                .map(|i| (i % 251) as u8)
                .collect();
            storage
                .put_object(&b, &k, body(payload.clone()))
                .await
                .unwrap();

            let get = storage.get_object(&b, &k, None).await.unwrap();
            let mut stream = get.body;
            let mut chunks = Vec::new();
            while let Some(chunk) = stream.next().await {
                chunks.push(chunk.unwrap());
            }
            let sizes: Vec<usize> = chunks.iter().map(|chunk| chunk.len()).collect();
            assert_eq!(sizes, vec![CHUNK_SIZE, CHUNK_SIZE, 1234]);
            assert_eq!(chunks.concat(), payload);
        });
    }

    #[test]
    fn get_stream_stops_at_the_range_end() {
        // A range ending mid-buffer must not read past its end: the stream
        // yields exactly the remaining bytes even when the read buffer
        // could hold more (a limit-less `read_buf` would overrun the
        // range and corrupt the chunk tail).
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("ranged.bin").unwrap();
            let payload: Vec<u8> = (0..(CHUNK_SIZE + 100) as u32)
                .map(|i| (i % 251) as u8)
                .collect();
            storage
                .put_object(&b, &k, body(payload.clone()))
                .await
                .unwrap();

            let get = storage
                .get_object(
                    &b,
                    &k,
                    Some(ByteRange::Inclusive(
                        CHUNK_SIZE as u64 - 10,
                        CHUNK_SIZE as u64 + 4,
                    )),
                )
                .await
                .unwrap();
            assert_eq!(
                read_body(get.body).await.unwrap(),
                &payload[CHUNK_SIZE - 10..CHUNK_SIZE + 5]
            );
        });
    }

    #[test]
    fn streamed_chunks_own_their_memory() {
        // The stream reuses one read buffer; each frozen chunk must stay
        // valid after the next chunk is pulled (a naive clear-and-refill
        // reuse would corrupt a chunk the consumer still holds — `split`
        // gives the filled prefix to the chunk, the buffer reuses only
        // the tail).
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("owned.bin").unwrap();
            let payload = [vec![0xAB; CHUNK_SIZE], vec![0xCD; 100]].concat();
            storage.put_object(&b, &k, body(payload)).await.unwrap();

            let get = storage.get_object(&b, &k, None).await.unwrap();
            let mut stream = get.body;
            let first = stream.next().await.unwrap().unwrap();
            // Pull the second chunk before inspecting the first.
            let second = stream.next().await.unwrap().unwrap();
            assert_eq!(first.len(), CHUNK_SIZE);
            assert!(first.iter().all(|&byte| byte == 0xAB));
            assert_eq!(second.len(), 100);
            assert!(second.iter().all(|&byte| byte == 0xCD));
        });
    }

    #[test]
    fn delete_of_a_directory_without_slash_is_204() {
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            // 'dir' exists as a directory (a folder marker was PUT).
            storage
                .put_object(&b, &"dir/".into(), body(b""))
                .await
                .unwrap();
            // DELETE 'dir' (no trailing slash): idempotent 204, the
            // directory itself is untouched.
            storage
                .delete_object(&b, &object::key("dir").unwrap())
                .await
                .unwrap();
            assert!(root.path().join("data/dir").is_dir());
            storage
                .delete_object(&b, &object::key("dir").unwrap())
                .await
                .unwrap();
        });
    }

    #[test]
    fn folder_marker_semantics() {
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let marker = object::key("dir/").unwrap();

            // PUT creates the directory, idempotently.
            storage.put_object(&b, &marker, body(b"")).await.unwrap();
            storage.put_object(&b, &marker, body(b"")).await.unwrap();
            assert!(root.path().join("data/dir").is_dir());

            // GET/HEAD → NoSuchKey.
            let err: StorageError = storage
                .get_object(&b, &marker, None)
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::NoSuchKey(_)));

            // DELETE removes the empty directory, idempotently.
            storage.delete_object(&b, &marker).await.unwrap();
            storage.delete_object(&b, &marker).await.unwrap();
            assert!(!root.path().join("data/dir").exists());

            // A non-empty directory is left in place.
            storage.put_object(&b, &marker, body(b"")).await.unwrap();
            storage
                .put_object(&b, &"dir/file.txt".into(), body(b"x"))
                .await
                .unwrap();
            storage.delete_object(&b, &marker).await.unwrap();
            assert!(root.path().join("data/dir").is_dir());
            assert!(root.path().join("data/dir/file.txt").is_file());
        });
    }

    #[test]
    fn reserved_keys_denied() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            for key in [".tinio", ".tinio/x", "a/.tinio", "a/.tinio/b"] {
                let k = object::key(key).unwrap();
                let err: StorageError = storage
                    .put_object(&b, &k, body(b"x"))
                    .await
                    .unwrap_err()
                    .into();
                assert!(matches!(err, StorageError::AccessDenied(_)), "{key}");
                let err: StorageError = storage.get_object(&b, &k, None).await.unwrap_err().into();
                assert!(matches!(err, StorageError::NoSuchKey(_)), "{key}");
            }
        });
    }

    #[test]
    fn reserved_key_refused_with_follow_enabled() {
        // FR-020 in BOTH follow modes (P5): the follow-enabled resolve
        // used to skip the lexical mapping entirely — a `.tinio` PUT
        // slipped through and wrote `<bucket>/.tinio/x`. The lexical
        // validation now runs before the follow shortcut, refusing
        // AccessDenied like every other op (contract doc: "refuse writes
        // whose key is reserved ... with AccessDenied").
        use crate::FsOptions;
        rt(async {
            let (root, _) = storage();
            let b = bucket::name("data").unwrap();
            std::fs::create_dir(root.path().join("data")).unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    ..fs_options()
                },
            )
            .unwrap();
            let k = object::key(".tinio/x").unwrap();
            let err: StorageError = storage
                .put_object(&b, &k, body(b"x"))
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            assert!(
                !root.path().join("data/.tinio").exists(),
                "the reserved segment must never be written"
            );
        });
    }

    #[cfg(windows)]
    #[test]
    fn lexical_validation_precedes_the_symlink_walk() {
        // P5 ordering pin: the pure lexical validation runs before the
        // symlink walk, so a key the mapping refuses (Windows-invalid
        // chars) answers InvalidKey even when a path component is a link
        // — the documented order ("rejected before any filesystem
        // access", path.rs), where the walk-first code answered
        // AccessDenied (the walk syscalled first).
        use crate::FsOptions;
        rt(async {
            let (root, _) = storage();
            let b = bucket::name("data").unwrap();
            std::fs::create_dir(root.path().join("data")).unwrap();
            let outside = tempfile::tempdir().unwrap();
            std::os::windows::fs::symlink_dir(outside.path(), root.path().join("data/a")).unwrap();
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: false,
                    ..fs_options()
                },
            )
            .unwrap();
            let k = object::key("a/b<c").unwrap();
            let err: StorageError = storage
                .put_object(&b, &k, body(b"x"))
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::InvalidKey(_)), "{err:?}");
        });
    }

    #[test]
    fn head_of_folder_marker_is_no_such_key() {
        // HEAD shares resolve_object_info with GET, which since P5 opens
        // the object first: on Windows the directory open fails like a
        // permission denial, and the classification stat must still
        // answer NoSuchKey (the folder-marker pin of the merged open).
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            storage
                .put_object(&b, &"dir/".into(), body(b""))
                .await
                .unwrap();
            let err: StorageError = storage
                .head_object(&b, &object::key("dir/").unwrap())
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::NoSuchKey(_)));
        });
    }

    #[test]
    fn commit_after_bucket_deleted_is_no_such_bucket() {
        // The commit's under-lock ensure_bucket is the only bucket check
        // (P5 — the pre-lock fail-fast pair was removed): a bucket
        // deleted between stage and commit reports NoSuchBucket, never a
        // silent 200 resurrecting it — and the staged temp is cleaned up
        // (a rejected commit leaves no residue).
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("a.txt").unwrap();
            let staged = storage.stage_body(&b, &k, body(b"hello")).await.unwrap();
            storage.delete_bucket(&b).await.unwrap();
            let err: StorageError = storage
                .commit_object(&b, &k, staged)
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::NoSuchBucket(_)), "{err:?}");
            assert!(!root.path().join("data").exists());
            let tmp = root.path().join(".tinio/tmp");
            let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
            assert!(
                entries.next_entry().await.unwrap().is_none(),
                "no temp files may remain"
            );
        });
    }

    #[test]
    fn dropped_staged_body_leaves_no_temp() {
        // A rejected conditional PUT (412) drops the staged body without
        // a commit — the full body must not stay in `tmp/` for the sweep.
        use tinio_core::storage::ObjectOps;
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("big.bin").unwrap();
            let staged = storage
                .stage_body(&b, &k, body(b"x".repeat(1024)))
                .await
                .unwrap();
            drop(staged); // the server's precondition-failure path
            // The removal is async (item 7a — the unlink runs on the
            // blocking pool); wait for it.
            let tmp = root.path().join(".tinio/tmp");
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
                    if entries.next_entry().await.unwrap().is_none() {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the dropped staged body's temp removal");
        });
    }

    #[test]
    fn out_of_band_change_served_immediately() {
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            // Hand-dropped file (SC-006).
            tokio::fs::write(root.path().join("data/dropped.txt"), b"out-of-band")
                .await
                .unwrap();
            let k = object::key("dropped.txt").unwrap();
            let head = storage.head_object(&b, &k).await.unwrap();
            assert_eq!(head.size, 11);
            assert_eq!(
                head.etag,
                etag(&hex::encode(md5::Md5::digest(b"out-of-band")))
            );
            let get = storage.get_object(&b, &k, None).await.unwrap();
            assert_eq!(read_body(get.body).await.unwrap(), b"out-of-band");
        });
    }

    #[test]
    fn interrupted_upload_leaves_no_object() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("partial").unwrap();
            let stream = futures::stream::iter(vec![
                Ok(bytes::Bytes::from_static(b"data")),
                Err(io::Error::other("boom")),
            ]);
            let err = storage
                .put_object(&b, &k, Box::pin(stream))
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Io(_)));
            let err = storage.head_object(&b, &k).await.unwrap_err();
            assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
        });
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_rejected_when_disabled() {
        use crate::FsOptions;
        use std::fs;
        rt(async {
            let (root, _) = storage();
            let b = bucket::name("data").unwrap();
            fs::create_dir(root.path().join("data")).unwrap();
            fs::write(root.path().join("outside.txt"), b"secret").unwrap();
            std::os::unix::fs::symlink(
                root.path().join("outside.txt"),
                root.path().join("data/link.txt"),
            )
            .unwrap();

            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: false,
                    state_dir: None,
                    ..fs_options()
                },
            )
            .unwrap();
            let k = object::key("link.txt").unwrap();
            let err: StorageError = storage.head_object(&b, &k).await.unwrap_err().into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            let err: StorageError = storage.get_object(&b, &k, None).await.unwrap_err().into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            // The PUT path rejects the same way (the stage gate refuses
            // before the body is staged).
            let err: StorageError = storage
                .put_object(&b, &k, body(b"x"))
                .await
                .unwrap_err()
                .into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            // DELETE through the link is refused the same way (it would
            // otherwise remove a file outside the storage root).
            let err: StorageError = storage.delete_object(&b, &k).await.unwrap_err().into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            assert!(root.path().join("outside.txt").exists());

            // With following enabled (default), the link is served.
            let storage = FsStorage::new(root.path(), fs_options()).unwrap();
            let head = storage.head_object(&b, &k).await.unwrap();
            assert_eq!(head.size, 6);
            // ... and DELETE resolves through it (the follow policy).
            storage.delete_object(&b, &k).await.unwrap();
            assert!(!root.path().join("outside.txt").exists());
        });
    }

    #[test]
    fn commit_object_writes_to_bucket_target_at_rename() {
        // Same race as multipart complete: a followed bucket symlink
        // retargeted between commit's resolve and the rename must not
        // leave the object on the stale path (stage_body is a validation
        // gate; commit re-resolves under the mutation lock).
        use crate::FsOptions;
        use crate::testutil::{link_dir, retarget_bucket_during_commit, wait_for_lock_waiter};
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let target_a = tempfile::tempdir().unwrap();
            let target_b = tempfile::tempdir().unwrap();
            let link = root.path().join("data");
            link_dir(target_a.path(), &link);
            let storage = FsStorage::new(
                root.path(),
                FsOptions {
                    follow_symlinks: true,
                    ..fs_options()
                },
            )
            .unwrap();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.txt").unwrap();
            let staged = storage.stage_body(&b, &k, body(b"hello")).await.unwrap();
            let storage2 = storage.clone();
            let b2 = b.clone();
            let k2 = k.clone();
            retarget_bucket_during_commit(
                &storage,
                &b,
                &link,
                target_b.path(),
                wait_for_lock_waiter(),
                move || async move { storage2.commit_object(&b2, &k2, staged).await.unwrap() },
            )
            .await;
            assert!(
                target_b.path().join("a.txt").exists(),
                "object must land under the bucket target at rename"
            );
            assert!(
                !target_a.path().join("a.txt").exists(),
                "stale pre-lock path must not receive the object"
            );
            let get = storage.get_object(&b, &k, None).await.unwrap();
            assert_eq!(read_body(get.body).await.unwrap(), b"hello");
        });
    }
}
