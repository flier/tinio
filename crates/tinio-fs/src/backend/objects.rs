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
        // repair would sweep it otherwise).
        if let Some(temp) = self.temp.take() {
            let _ = std::fs::remove_file(&temp);
        }
    }
}

/// Stream the inclusive byte range `start..=end` of an open file in bounded
/// chunks (constitution V: no per-object buffering). One read buffer for
/// the whole stream — no per-chunk allocation or zeroing.
async fn file_stream(file: File, start: u64, end: u64) -> BodyStream {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = file;
    if let Err(err) = file.seek(SeekFrom::Start(start)).await {
        return Box::pin(futures::stream::once(async move { Err(err) }));
    }
    let remaining = end.saturating_sub(start) + 1;
    Box::pin(futures::stream::try_unfold(
        (file, remaining, Vec::with_capacity(CHUNK_SIZE)),
        |(mut file, remaining, mut buf)| async move {
            if remaining == 0 {
                return Ok(None);
            }
            let want = remaining.min(CHUNK_SIZE as u64) as usize;
            buf.resize(want, 0);
            match file.read(&mut buf).await {
                Ok(0) => Ok(None),
                Ok(n) => {
                    let left = remaining - n as u64;
                    Ok(Some((
                        bytes::Bytes::copy_from_slice(&buf[..n]),
                        (file, left, buf),
                    )))
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
        let dir = match self.bucket_dir(name) {
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

    /// Resolve the object metadata for a read — the shared head of
    /// `get_object`/`head_object`. Folder markers (and directories) are
    /// never objects (`NoSuchKey`).
    async fn resolve_object_info(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(PathBuf, u64, SystemTime, ETag), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Reads of reserved keys report NoSuchKey (FR-020).
        if key.is_reserved() {
            return Err(no_such_key(key).into());
        }
        let path = self.resolve_key(&bucket_dir, key).await?;
        let metadata = match crate::fsutil::object_metadata(&path, self.follow_symlinks).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(no_such_key(key).into());
            }
            Err(err) => return Err(err.into()),
        };
        if metadata.is_dir() {
            // Folder markers (and directories) are never objects.
            return Err(no_such_key(key).into());
        }
        if crate::fsutil::is_symlink_or_reparse(&metadata) {
            return Err(access_denied(key).into());
        }
        let size = metadata.len();
        let mtime = metadata.modified()?;
        let etag = self
            .meta_store
            .etag_for_file(bucket, key, &path, size, mtime)
            .await?;
        Ok((path, size, mtime, etag))
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
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Fail-fast: reject an unresolvable key before taking the
        // mutation lock. The path is not kept — the lock section
        // re-resolves so a followed bucket symlink retargeted between
        // stage and rename cannot send the write to a stale target
        // (`stage_body` is the same validation gate).
        let _ = self.resolve_key(&bucket_dir, key).await?;
        let etag = staged.etag().clone();
        // Folder markers are never objects (s3-surface.md): PUT creates
        // the directory, idempotently. Under the mutation lock with the
        // same guards as a real object — a concurrent delete_bucket must
        // not be resurrected, and the symlink policy applies to markers
        // too.
        if key.is_folder_marker() {
            let _guard = self.bucket_mutation_lock.lock().await;
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
            let _guard = self.bucket_mutation_lock.lock().await;
            let bucket_dir = self.ensure_bucket(bucket).await?;
            let target = self.resolve_key(&bucket_dir, key).await?;
            AtomicWriter::commit(&temp, &target).await?;
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

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        let (path, size, mtime, etag) = self.resolve_object_info(bucket, key).await?;
        let file = match crate::fsutil::open_file(&path, self.follow_symlinks).await {
            Ok(file) => file,
            Err(err) if !self.follow_symlinks && err.kind() == io::ErrorKind::PermissionDenied => {
                return Err(access_denied(key).into());
            }
            Err(err) => return Err(err.into()),
        };
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
        let (_, size, mtime, etag) = self.resolve_object_info(bucket, key).await?;
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
    use crate::testutil::{rt, storage};
    use md5::Digest;
    use tinio_core::object;
    use tinio_core::storage::BucketOps;
    use tinio_core::storage::Error as StorageError;
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
            let tmp = root.path().join(".tinio/tmp");
            let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
            assert!(
                entries.next_entry().await.unwrap().is_none(),
                "no temp files may remain"
            );
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
                },
            )
            .unwrap();
            let k = object::key("link.txt").unwrap();
            let err: StorageError = storage.head_object(&b, &k).await.unwrap_err().into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            let err: StorageError = storage.get_object(&b, &k, None).await.unwrap_err().into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            // DELETE through the link is refused the same way (it would
            // otherwise remove a file outside the storage root).
            let err: StorageError = storage.delete_object(&b, &k).await.unwrap_err().into();
            assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
            assert!(root.path().join("outside.txt").exists());

            // With following enabled (default), the link is served.
            let storage = FsStorage::new(root.path(), Default::default()).unwrap();
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
                    ..Default::default()
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
