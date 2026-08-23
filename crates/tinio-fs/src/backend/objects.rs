//! Object and multipart operations of the fs backend (task T042/T044).
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
    multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber},
    object::{Info, Key},
    storage::{
        ByteRange, GetObjectResult, ListObjectsParams, ListPartsParams, ListUploadsParams,
        MultipartOps, ObjectListing, ObjectOps, PartsListing, PutObjectResult, UploadsListing,
        access_denied, group_and_paginate_ordered, invalid_key, no_such_bucket, no_such_key,
        split_uploads_order, uploads_order,
    },
};
use tokio::fs::File;

use crate::path::key_path;
use crate::write::{AtomicWriter, CHUNK_SIZE};

use super::{Error, FsStorage};

/// An empty body stream (a zero-byte object with no range).
fn empty_stream() -> BodyStream {
    Box::pin(futures::stream::empty())
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
    /// The bucket directory must exist (`NoSuchBucket`).
    pub(crate) async fn ensure_bucket(&self, name: &bucket::Name) -> Result<PathBuf, Error> {
        let dir = self.bucket_dir(name)?;
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
    async fn check_symlinks(&self, key: &Key, path: &Path) -> Result<(), Error> {
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
        key: &Key,
    ) -> Result<(PathBuf, u64, SystemTime, ETag), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Reads of reserved keys report NoSuchKey (FR-020).
        if key.is_reserved() {
            return Err(no_such_key(key).into());
        }
        let path = key_path(&bucket_dir, key)?;
        self.check_symlinks(key, &path).await?;
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
    /// directory).
    type StagedBody = (Option<PathBuf>, ETag);

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        body: BodyStream,
    ) -> Result<(Option<PathBuf>, ETag), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let target = key_path(&bucket_dir, key)?;
        // Symlink policy applies to markers too — a PUT `sub/dir/` whose
        // parent is a link must not create a directory outside the root.
        self.check_symlinks(key, &target).await?;
        // Folder markers are never objects (s3-surface.md): no body is
        // staged — the commit creates the directory. The sentinel
        // carries the marker's empty-content ETag.
        if key.is_folder_marker() {
            return Ok((None, ETag::from_content(b"")));
        }
        let (temp, etag) = self.writer.stage(body).await?;
        Ok((Some(temp), etag))
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        staged: (Option<PathBuf>, ETag),
    ) -> Result<PutObjectResult, Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let target = key_path(&bucket_dir, key)?;
        let (temp, etag) = staged;
        // Folder markers are never objects (s3-surface.md): PUT creates
        // the directory, idempotently. Under the mutation lock with the
        // same guards as a real object — a concurrent delete_bucket must
        // not be resurrected, and the symlink policy applies to markers
        // too.
        if key.is_folder_marker() {
            let _guard = self.bucket_mutation_lock.lock().await;
            self.check_symlinks(key, &target).await?;
            self.ensure_bucket(bucket).await?;
            tokio::fs::create_dir_all(&target).await?;
            return Ok(PutObjectResult { etag });
        }
        // A real object always arrives with a staged temp (the marker
        // branch above consumed the sentinel).
        let Some(temp) = temp else {
            return Err(io::Error::other("staged body without a temp file").into());
        };
        // The rename is the bucket-mutating step: under the mutation
        // lock it cannot race a `delete_bucket` (a PUT that loses
        // reports an error instead of silently losing the object with
        // 200). A failed commit removes the staged temp — a rejected
        // write leaves no residue.
        let result = async {
            self.check_symlinks(key, &target).await?;
            let _guard = self.bucket_mutation_lock.lock().await;
            self.ensure_bucket(bucket).await?;
            AtomicWriter::commit(&temp, &target).await?;
            let metadata = tokio::fs::metadata(&target).await?;
            // The object is committed — a meta-write failure (full
            // state dir) must not fail the PUT: the entry is recomputed
            // from the content on the next read (self-healing, FR-022).
            if let Err(err) = self
                .meta_store
                .set(bucket, key, &etag, metadata.len(), metadata.modified()?)
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
        key: &Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        let (path, size, mtime, etag) = self.resolve_object_info(bucket, key).await?;
        let file = match crate::fsutil::open_file(&path, self.follow_symlinks).await {
            Ok(file) => file,
            Err(err)
                if !self.follow_symlinks && err.kind() == io::ErrorKind::PermissionDenied =>
            {
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

    async fn head_object(&self, bucket: &bucket::Name, key: &Key) -> Result<Info, Error> {
        let (_, size, mtime, etag) = self.resolve_object_info(bucket, key).await?;
        Ok(Info {
            key: key.clone(),
            size,
            last_modified: mtime,
            etag,
        })
    }

    async fn delete_object(&self, bucket: &bucket::Name, key: &Key) -> Result<(), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let path = key_path(&bucket_dir, key)?;
        // Like every other object op: never resolve through a symlink
        // when `follow_symlinks` is disabled (a DELETE through a linked
        // directory would remove a file outside the storage root).
        self.check_symlinks(key, &path).await?;
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

#[async_trait::async_trait]
impl MultipartOps for FsStorage {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &Key,
    ) -> Result<MultipartUpload, Error> {
        self.ensure_bucket(bucket).await?;
        // The multipart path must not be a backdoor for `.tinio` (FR-020).
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        // Folder markers are never objects: refuse the upload up front
        // (completion would have nowhere legal to materialize it).
        if key.is_folder_marker() {
            return Err(invalid_key(key.to_string()).into());
        }
        self.multipart_store.create(bucket, key).await
    }

    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
    ) -> Result<PartInfo, Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        self.multipart_store
            .put_part(bucket, key, upload_id, part_number, body)
            .await
    }

    async fn list_parts(&self, params: ListPartsParams) -> Result<PartsListing, Error> {
        self.ensure_bucket(&params.bucket).await?;
        // The store applies the marker skip and the page cut inside its
        // scan (a page costs O(page) reads); `max_parts = 0` is an empty,
        // untruncated page with no marker (the store's contract).
        let (parts, truncated) = self
            .multipart_store
            .list_parts(
                &params.bucket,
                &params.key,
                &params.upload_id,
                params.part_number_marker,
                params.max_parts,
            )
            .await?;
        let next = if truncated {
            parts.last().map(|p| u32::from(p.part_number))
        } else {
            None
        };
        Ok(PartsListing {
            parts,
            truncated,
            next_part_number_marker: next,
        })
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<Info, Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        // Folder markers are never objects — a multipart upload cannot
        // materialize one (the dir branch of `put_object` would be the
        // only legal mapping, and completion is not it).
        if key.is_folder_marker() {
            return Err(invalid_key(key.to_string()).into());
        }
        let target = key_path(&bucket_dir, key)?;
        self.check_symlinks(key, &target).await?;
        // Phase 1 (the store's own lock): verify + assemble into a temp
        // file; the upload is consumed.
        let (temp, etag) = self
            .multipart_store
            .complete(bucket, key, upload_id, parts)
            .await?;
        // Phase 2 (the mutation lock): the rename cannot race a
        // `delete_bucket` — and a bucket deleted between the phases is
        // reported, not silently recreated.
        let _guard = self.bucket_mutation_lock.lock().await;
        self.ensure_bucket(bucket).await?;
        AtomicWriter::commit(&temp, &target).await?;
        let metadata = tokio::fs::metadata(&target).await?;
        let info = Info {
            key: key.clone(),
            size: metadata.len(),
            last_modified: metadata.modified()?,
            etag,
        };
        self.meta_store
            .set(bucket, key, &info.etag, info.size, info.last_modified)
            .await?;
        Ok(info)
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        self.multipart_store.abort(bucket, key, upload_id).await
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, Error> {
        self.ensure_bucket(&params.bucket).await?;
        // Prefix filter first (the engine uses the prefix only for
        // delimiter rollups — without the filter, non-matching uploads
        // would leak onto the page).
        let uploads = self
            .multipart_store
            .list_uploads(&params.bucket)
            .await?
            .into_iter()
            .filter(|u| u.key.starts_with(&params.prefix))
            .collect::<Vec<_>>();
        // The order is the composite `key\0upload_id` (see
        // `tinio_core::storage::uploads_order`), so the resume marker can
        // position inside a same-key group (S3 `upload-id-marker`). A
        // bare key marker skips the whole key group (S3: only keys
        // strictly greater than `key-marker` are listed) — the sentinel
        // upload id sorts after every real one.
        let marker = match (&params.key_marker, &params.upload_id_marker) {
            (Some(key), Some(upload_id)) => Some(uploads_order(key, upload_id)),
            (Some(key), None) => Some(uploads_order(key, "\u{10FFFF}")),
            _ => None,
        };
        let (uploads, common_prefixes, truncated, next) = group_and_paginate_ordered(
            uploads,
            &params.prefix,
            params.delimiter.as_deref(),
            marker.as_deref(),
            params.max_uploads,
            |u| u.key.as_ref(),
            |u| uploads_order(&u.key, &u.upload_id),
        );
        let (next_key_marker, next_upload_id_marker) = match next {
            Some(next) => {
                let (key, upload_id) = split_uploads_order(&next);
                (Some(key.to_string()), upload_id.map(str::to_string))
            }
            None => (None, None),
        };
        Ok(UploadsListing {
            uploads,
            common_prefixes,
            truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use md5::Digest;
    use tinio_core::object;
    use tinio_core::storage::BucketOps;
    use tinio_core::storage::Error as StorageError;
    use tinio_core::testing::{assert_conformance, body, etag, read_body};

    fn storage() -> (tempfile::TempDir, FsStorage) {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), Default::default()).unwrap();
        (root, storage)
    }

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

    #[test]
    fn multipart_lifecycle_via_contract() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = storage.create_multipart_upload(&b, &k).await.unwrap();
            let mut parts = Vec::new();
            let parts_data: [&[u8]; 3] = [b"abc", b"defgh", b"ij"];
            for (i, data) in parts_data.iter().enumerate() {
                let part = storage
                    .upload_part(
                        &b,
                        &k,
                        &upload.upload_id,
                        ((i + 1) as u32).into(),
                        body(data.to_vec()),
                    )
                    .await
                    .unwrap();
                parts.push(part);
            }
            let listing = storage
                .list_parts(ListPartsParams {
                    bucket: b.clone(),
                    key: k.clone(),
                    upload_id: upload.upload_id.clone(),
                    max_parts: 2,
                    part_number_marker: None,
                })
                .await
                .unwrap();
            assert_eq!(listing.parts.len(), 2);
            assert!(listing.truncated);
            assert_eq!(listing.next_part_number_marker, Some(2));

            let completed: Vec<_> = parts
                .iter()
                .map(|p| tinio_core::multipart::CompletedPart {
                    part_number: p.part_number,
                    etag: p.etag.clone(),
                })
                .collect();
            let info = storage
                .complete_multipart_upload(&b, &k, &upload.upload_id, &completed)
                .await
                .unwrap();
            assert_eq!(info.size, 10);
            // MD5-of-MD5s-3 reference (computed from raw part digests).
            assert_eq!(info.etag.as_str(), "3bad9a9cef9eca7c4de3f13d00832b7e-3");

            let get = storage.get_object(&b, &k, None).await.unwrap();
            assert_eq!(read_body(get.body).await.unwrap(), b"abcdefghij");
            assert_eq!(get.info.etag.as_str(), "3bad9a9cef9eca7c4de3f13d00832b7e-3");

            storage.delete_object(&b, &k).await.unwrap();
            storage.delete_bucket(&b).await.unwrap();
        });
    }

    #[test]
    fn part_number_marker_pagination() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = storage.create_multipart_upload(&b, &k).await.unwrap();
            for i in 1..=5u32 {
                storage
                    .upload_part(&b, &k, &upload.upload_id, i.into(), body(b"x"))
                    .await
                    .unwrap();
            }
            let page1 = storage
                .list_parts(ListPartsParams {
                    bucket: b.clone(),
                    key: k.clone(),
                    upload_id: upload.upload_id.clone(),
                    max_parts: 2,
                    part_number_marker: None,
                })
                .await
                .unwrap();
            let page2 = storage
                .list_parts(ListPartsParams {
                    bucket: b.clone(),
                    key: k.clone(),
                    upload_id: upload.upload_id.clone(),
                    max_parts: 2,
                    part_number_marker: page1.next_part_number_marker,
                })
                .await
                .unwrap();
            let page3 = storage
                .list_parts(ListPartsParams {
                    bucket: b.clone(),
                    key: k.clone(),
                    upload_id: upload.upload_id.clone(),
                    max_parts: 2,
                    part_number_marker: page2.next_part_number_marker,
                })
                .await
                .unwrap();
            assert_eq!(page1.parts.len() + page2.parts.len() + page3.parts.len(), 5);
            assert!(!page3.truncated);
            storage
                .abort_multipart_upload(&b, &k, &upload.upload_id)
                .await
                .unwrap();
        });
    }

    #[test]
    fn list_multipart_uploads_filters_by_prefix() {
        // The prefix filter must apply to the page (the engine uses the
        // prefix only for delimiter rollups) — same behavior as the mem
        // backend.
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            storage
                .create_multipart_upload(&b, &object::key("a.bin").unwrap())
                .await
                .unwrap();
            storage
                .create_multipart_upload(&b, &object::key("b.bin").unwrap())
                .await
                .unwrap();
            let page = storage
                .list_multipart_uploads(ListUploadsParams {
                    bucket: b.clone(),
                    prefix: "b".into(),
                    delimiter: None,
                    key_marker: None,
                    upload_id_marker: None,
                    max_uploads: 1000,
                })
                .await
                .unwrap();
            let keys: Vec<&str> = page.uploads.iter().map(|u| u.key.as_ref().as_str()).collect();
            assert_eq!(keys, ["b.bin"]);
        });
    }

    #[test]
    fn bare_key_marker_skips_the_whole_key_group() {
        // A key-marker without an upload-id-marker skips the entire
        // same-key group (S3: only keys strictly greater than the marker
        // are listed) — resuming after a page cut must not re-list it.
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("same.bin").unwrap();
            let u1 = storage.create_multipart_upload(&b, &k).await.unwrap();
            storage.create_multipart_upload(&b, &k).await.unwrap();
            let page = storage
                .list_multipart_uploads(ListUploadsParams {
                    bucket: b.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    key_marker: Some(u1.key.to_string()),
                    upload_id_marker: None,
                    max_uploads: 10,
                })
                .await
                .unwrap();
            assert!(page.uploads.is_empty(), "{:?}", page.uploads);
            assert!(!page.truncated);
        });
    }

    #[test]
    fn same_key_uploads_paginate_without_skipping() {
        // Two uploads of one key: a page cut at max_uploads=1 must never
        // lose the second upload — the resume marker positions inside the
        // same-key group (`(key, upload_id)` order).
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let k = object::key("same.bin").unwrap();
            storage.create_multipart_upload(&b, &k).await.unwrap();
            storage.create_multipart_upload(&b, &k).await.unwrap();
            let page1 = storage
                .list_multipart_uploads(ListUploadsParams {
                    bucket: b.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    key_marker: None,
                    upload_id_marker: None,
                    max_uploads: 1,
                })
                .await
                .unwrap();
            assert!(page1.truncated);
            let page2 = storage
                .list_multipart_uploads(ListUploadsParams {
                    bucket: b.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    key_marker: page1.next_key_marker.clone(),
                    upload_id_marker: page1.next_upload_id_marker.clone(),
                    max_uploads: 10,
                })
                .await
                .unwrap();
            let ids: Vec<String> = page2
                .uploads
                .iter()
                .map(|u| u.upload_id.clone())
                .collect();
            assert_eq!(ids.len(), 1, "{ids:?}");
            assert_ne!(ids[0], page1.uploads[0].upload_id);
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
}
