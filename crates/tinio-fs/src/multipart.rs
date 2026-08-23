//! Multipart upload storage (task T044).
//!
//! Parts live at `<state-dir>/multipart/<bucket>/<uploadId>/part-<n>` with
//! an ETag sidecar `part-<n>.etag` (hex) and an `upload.json` record
//! (`{upload_id, bucket, key, initiated_at}`, data-model.md). Assembly
//! streams all parts into a temp file, then renames atomically onto the
//! object path; the composed ETag `MD5-of-MD5s-N` matches the AWS
//! reference composition. Abort removes the parts subtree. Parts survive
//! restarts, so cross-restart completion/abort is legal (quickstart §7).
//! No 5 MB minimum is enforced (FR-014).
//!
//! All mutating operations serialize on an in-process lock, so a part
//! upload racing a completion can never interleave into a torn state.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use tinio_core::{
    BodyStream, ETag, bucket, from_nanos,
    multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber},
    object::{self, Key},
    storage::{self, paginate_ordered},
    to_nanos,
};
use tokio::sync::Mutex;

use crate::{
    Error,
    backend::corrupt_state_file,
    fsutil::ok_if_missing,
    path::MULTIPART_DIR_NAME,
    write::{AtomicWriter, md5_of_file},
};

/// The per-upload record (`upload.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UploadFile {
    upload_id: String,
    bucket: String,
    key: String,
    initiated_at: u64,
}

/// The on-disk path of a part file: `<upload-dir>/part-<n>`.
fn part_path(dir: &Path, n: u32) -> PathBuf {
    dir.join(format!("part-{n}"))
}

/// Whether a directory entry name is a part content file (not an `.etag`
/// sidecar).
fn is_part_file(name: &str) -> bool {
    name.starts_with("part-") && !name.ends_with(".etag")
}

/// Sort uploads by the composite `(key, upload_id)` order the pagination
/// engine requires.
fn sort_uploads(uploads: &mut [MultipartUpload]) {
    uploads.sort_by(|a, b| {
        (a.key.as_ref(), a.upload_id.as_str()).cmp(&(b.key.as_ref(), b.upload_id.as_str()))
    });
}

/// Multipart parts storage of a state dir.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{
///     bucket,
///     multipart::{CompletedPart, part_number},
///     object,
///     testing::body,
/// };
/// use tinio_fs::MultipartStore;
///
/// let state = tempfile::tempdir().unwrap();
/// let store = MultipartStore::new(state.path());
/// let bucket = bucket::name("data").unwrap();
/// let key = object::key("big.bin").unwrap();
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     let upload = store.create(&bucket, &key).await.unwrap();
///     let part = store
///         .put_part(&bucket, &key, &upload.upload_id, part_number(1).unwrap(), body(b"abc"))
///         .await
///         .unwrap();
///     assert_eq!(u32::from(part.part_number), 1);
///     let target = state.path().join("assembled.bin");
///     let completed = CompletedPart {
///         part_number: part.part_number,
///         etag: part.etag,
///     };
///     let (temp, _etag) = store
///         .complete(&bucket, &key, &upload.upload_id, &[completed])
///         .await
///         .unwrap();
///     tokio::fs::rename(&temp, &target).await.unwrap();
///     assert_eq!(tokio::fs::metadata(&target).await.unwrap().len(), 3);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct MultipartStore {
    /// `<state-dir>/multipart/`.
    root: PathBuf,
    /// Atomic writer (staging under `<state-dir>/tmp/`).
    writer: AtomicWriter,
    /// In-process lock: serializes create/put/complete/abort/list.
    lock: Arc<Mutex<()>>,
}

impl MultipartStore {
    /// Create a store rooted at `<state_dir>/multipart/`.
    pub fn new(state_dir: &Path) -> Self {
        Self {
            root: state_dir.join(MULTIPART_DIR_NAME),
            writer: AtomicWriter::new(state_dir),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Start a multipart upload (fresh UUID v4 id; `upload.json` written
    /// before the store answers). The bucket must already exist — callers
    /// check `head_bucket` first.
    pub async fn create(&self, bucket: &bucket::Name, key: &Key) -> Result<MultipartUpload, Error> {
        let _guard = self.lock.lock().await;
        // Fresh UUID v4 (122 random bits): collisions cannot happen in
        // practice, so a failed create_dir is a real I/O error.
        let upload_id = uuid::Uuid::new_v4().to_string();
        let dir = self.upload_dir(bucket, &upload_id)?;
        if let Some(parent) = dir.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir(&dir).await?;
        let upload = MultipartUpload {
            upload_id,
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: SystemTime::now(),
        };
        let file = UploadFile {
            upload_id: upload.upload_id.clone(),
            bucket: upload.bucket.to_string(),
            key: upload.key.to_string(),
            initiated_at: to_nanos(upload.initiated_at),
        };
        let json = serde_json::to_vec(&file)?;
        if let Err(err) = self
            .writer
            .write_bytes(&dir.join("upload.json"), &json)
            .await
        {
            // Never leave a zombie upload dir behind (no upload.json).
            let _ = tokio::fs::remove_dir_all(&dir).await;
            return Err(err);
        }
        Ok(upload)
    }

    /// Stream one part (number `1..=10000`) into the upload. `NoSuchUpload`
    /// when the upload does not exist.
    pub async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
    ) -> Result<PartInfo, Error> {
        let dir = self.require_upload(bucket, key, upload_id).await?;
        let n = u32::from(part_number);
        let part = part_path(&dir, n);
        // Stream the part body outside the lock — a slow client must not
        // stall every multipart operation (create/complete/abort/list).
        // The temp+rename happens under the lock, so a part file only ever
        // becomes visible whole.
        let (temp, etag) = match self.writer.stage(body).await {
            Ok(staged) => staged,
            Err(err) => {
                // An abort may have removed the upload mid-stream.
                if self.ensure_upload(&dir).await.is_err() {
                    return Err(storage::no_such_upload(upload_id).into());
                }
                return Err(err);
            }
        };
        let _guard = self.lock.lock().await;
        // The upload may have been aborted while the body streamed — the
        // staged temp is discarded, not left for the sweep.
        if let Err(err) = self.require_upload(bucket, key, upload_id).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err);
        }
        if let Err(err) = tokio::fs::rename(&temp, &part).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err.into());
        }
        // Sidecar: the part ETag, written under the same lock so a
        // concurrent overwrite of the same part can never mismatch.
        let sidecar = dir.join(format!("part-{n}.etag"));
        self.writer
            .write_bytes(&sidecar, etag.as_str().as_bytes())
            .await?;
        let metadata = tokio::fs::metadata(&part).await?;
        Ok(PartInfo {
            part_number,
            size: metadata.len(),
            etag,
            last_modified: metadata.modified()?,
        })
    }

    /// List the parts of an upload, in part-number order, paginated by
    /// the shared engine (`tinio_core::storage::paginate_ordered`): the
    /// marker skip and the page cut happen on the part numbers (entry
    /// names only — no stat), so a page costs O(page) reads, not
    /// O(total parts). `max_parts = 0` returns an empty, untruncated
    /// page (an exclusive-after marker would skip the first part of the
    /// next page forever). Returns the page and whether more parts
    /// follow.
    pub async fn list_parts(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
        marker: Option<u32>,
        max_parts: usize,
    ) -> Result<(Vec<PartInfo>, bool), Error> {
        let _guard = self.lock.lock().await;
        let dir = self.require_upload(bucket, key, upload_id).await?;
        // Pass 1: the part numbers present (entry names only — no stat).
        let mut numbers = Vec::new();
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !is_part_file(name) {
                continue;
            }
            let Some(rest) = name.strip_prefix("part-") else { continue };
            if let Ok(n) = rest.parse::<u32>() {
                numbers.push(n);
            }
        }
        numbers.sort_unstable();
        let (page, truncated, _next) =
            paginate_ordered(numbers, marker.as_ref(), max_parts, |n| *n);
        // Pass 2: metadata + ETag for the page only.
        let mut parts = Vec::with_capacity(page.len());
        for n in page {
            let path = part_path(&dir, n);
            let metadata = match tokio::fs::metadata(&path).await {
                Ok(metadata) => metadata,
                // A part can vanish between the passes (a concurrent
                // abort) — skip it rather than fail the listing.
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            let etag = self.part_etag(&dir, n).await?;
            parts.push(PartInfo {
                part_number: n.into(),
                size: metadata.len(),
                etag,
                last_modified: metadata.modified()?,
            });
        }
        Ok((parts, truncated))
    }

    /// The ETag of one part (sidecar, or recomputed streaming when the
    /// sidecar is missing — a crash between rename and sidecar write).
    async fn part_etag(&self, dir: &Path, n: u32) -> Result<ETag, Error> {
        let sidecar = dir.join(format!("part-{n}.etag"));
        if let Ok(hex) = tokio::fs::read_to_string(&sidecar).await
            && let Ok(etag) = ETag::new(hex.trim())
        {
            return Ok(etag);
        }
        let part = part_path(dir, n);
        Ok(ETag::Single(md5_of_file(&part).await?))
    }

    /// Verify the listed parts and assemble them into a fresh temp file
    /// (the caller renames it onto the object path under its own lock —
    /// bucket mutations serialize with `delete_bucket`). The upload is
    /// consumed: its parts subtree is removed before this returns.
    ///
    /// - empty `parts` → `storage::Error::NoParts`;
    /// - missing / mismatched / out-of-order part → `InvalidPart`;
    /// - the upload's recorded key differs from `key` → `NoSuchUpload`.
    ///
    /// Returns the temp path and the composed ETag (`MD5-of-MD5s-N`).
    pub async fn complete(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<(PathBuf, ETag), Error> {
        let (dir, infos) = {
            let _guard = self.lock.lock().await;
            let dir = self.require_upload(bucket, key, upload_id).await?;
            if parts.is_empty() {
                return Err(storage::no_parts().into());
            }
            // Verify: strictly ascending, each part exists with a matching ETag.
            let mut infos = Vec::with_capacity(parts.len());
            let mut last = 0u32;
            for part in parts {
                let n = u32::from(part.part_number);
                if n <= last {
                    return Err(storage::invalid_part(n).into());
                }
                last = n;
                // A missing part file (NotFound) is an InvalidPart — not an I/O
                // failure.
                let path = part_path(&dir, n);
                let metadata = match tokio::fs::metadata(&path).await {
                    Ok(metadata) if !metadata.is_dir() => metadata,
                    Ok(_) | Err(_) => return Err(storage::invalid_part(n).into()),
                };
                let stored = self.part_etag(&dir, n).await?;
                if stored != part.etag {
                    return Err(storage::invalid_part(n).into());
                }
                infos.push(PartInfo {
                    part_number: part.part_number,
                    size: metadata.len(),
                    etag: stored,
                    last_modified: metadata.modified()?,
                });
            }
            (dir, infos)
        };
        // Assemble outside the store lock so other multipart ops are not
        // stalled for the duration of the copy.
        let tmp_dir = self.writer.tmp_dir();
        tokio::fs::create_dir_all(tmp_dir).await?;
        let temp = tmp_dir.join(format!("multipart-{}", uuid::Uuid::new_v4()));
        let assemble = async {
            let mut out = tokio::fs::File::create(&temp).await?;
            for info in &infos {
                let mut file = tokio::fs::File::open(part_path(&dir, u32::from(info.part_number)))
                    .await?;
                tokio::io::copy(&mut file, &mut out).await?;
            }
            tokio::io::AsyncWriteExt::flush(&mut out).await?;
            Ok::<_, Error>(())
        }
        .await;
        if let Err(err) = assemble {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err);
        }
        // Abort (or another complete) may have consumed the upload while
        // we copied. Re-bind to the same (bucket, key, id) before
        // dropping the parts, or discard the temp as lost.
        if let Err(err) = async {
            let _guard = self.lock.lock().await;
            self.require_upload(bucket, key, upload_id).await?;
            let _ = tokio::fs::remove_dir_all(&dir).await;
            Ok::<_, Error>(())
        }
        .await
        {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(err);
        }
        let etag =
            ETag::composed_from_parts(&infos).ok_or_else(|| Error::from(storage::no_parts()))?;
        Ok((temp, etag))
    }

    /// Abort an upload and remove its parts subtree. `NoSuchUpload` when
    /// the upload does not exist.
    pub async fn abort(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        let _guard = self.lock.lock().await;
        let dir = self.require_upload(bucket, key, upload_id).await?;
        tokio::fs::remove_dir_all(&dir).await?;
        Ok(())
    }

    /// Every in-progress upload of a bucket, in `(key, upload_id)` order
    /// — the composite order the pagination engine requires, so a page
    /// can resume inside a same-key group (from the `upload.json`
    /// records).
    pub async fn list_uploads(&self, bucket: &bucket::Name) -> Result<Vec<MultipartUpload>, Error> {
        let _guard = self.lock.lock().await;
        let mut uploads = self.read_uploads(&self.root.join(&**bucket)).await?;
        sort_uploads(&mut uploads);
        Ok(uploads)
    }

    /// The latest part mtime of an upload (`UNIX_EPOCH` when no parts exist
    /// yet) — the sweep's idle computation (idle = max(initiated_at,
    /// latest part mtime), data-model.md).
    pub async fn idle_since(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<SystemTime, Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        let mut latest = SystemTime::UNIX_EPOCH;
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(latest),
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !is_part_file(name) {
                continue;
            }
            if let Ok(metadata) = tokio::fs::metadata(entry.path()).await
                && let Ok(modified) = metadata.modified()
                && modified > latest
            {
                latest = modified;
            }
        }
        Ok(latest)
    }

    /// Whether a bucket has any in-progress upload (bucket-delete check:
    /// in-progress uploads make the bucket non-empty).
    pub async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let dir = self.root.join(&**bucket);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Remove the whole multipart subtree of a bucket (bucket delete).
    pub async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let dir = self.root.join(&**bucket);
        ok_if_missing(tokio::fs::remove_dir_all(&dir).await)?;
        Ok(())
    }

    /// Upload directories of every bucket, in `(key, upload_id)` order —
    /// the sweep's idle-expiry walk.
    pub async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, Error> {
        let _guard = self.lock.lock().await;
        let mut uploads = Vec::new();
        let mut buckets = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(uploads),
            Err(err) => return Err(err.into()),
        };
        while let Some(bucket_entry) = buckets.next_entry().await? {
            if !bucket_entry.file_type().await?.is_dir() {
                continue;
            }
            uploads.extend(self.read_uploads(&bucket_entry.path()).await?);
        }
        sort_uploads(&mut uploads);
        Ok(uploads)
    }

    /// The uploads recorded under one directory (the per-bucket scan
    /// shared by `list_uploads` and `walk_uploads`), in scan order — the
    /// callers sort once (`(key, upload_id)`), so the sweep's multi-bucket
    /// walk does not re-sort per bucket.
    async fn read_uploads(&self, dir: &Path) -> Result<Vec<MultipartUpload>, Error> {
        let mut uploads = Vec::new();
        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(uploads),
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            if let Some(upload) = self.read_upload(&entry.path()).await? {
                uploads.push(upload);
            }
        }
        uploads.sort_by(|a, b| {
            (a.key.as_ref(), a.upload_id.as_str()).cmp(&(b.key.as_ref(), b.upload_id.as_str()))
        });
        Ok(uploads)
    }

    async fn ensure_upload(&self, dir: &Path) -> Result<(), Error> {
        match tokio::fs::metadata(dir.join("upload.json")).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(storage::no_such_upload(
                dir.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("<unknown>"),
            )
            .into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Resolve the upload directory and refuse a key mismatch as
    /// `NoSuchUpload` (S3 identity is `(bucket, key, uploadId)`).
    async fn require_upload(
        &self,
        bucket: &bucket::Name,
        key: &Key,
        upload_id: &str,
    ) -> Result<PathBuf, Error> {
        let dir = self.upload_dir(bucket, upload_id)?;
        self.ensure_upload(&dir).await?;
        match self.read_upload(&dir).await? {
            Some(record) if record.key == *key => Ok(dir),
            _ => Err(storage::no_such_upload(upload_id).into()),
        }
    }

    /// The ETag of one part (sidecar, or recomputed streaming when the
    /// sidecar is missing — a crash between rename and sidecar write).
    async fn read_upload(&self, dir: &Path) -> Result<Option<MultipartUpload>, Error> {
        let bytes = match tokio::fs::read(dir.join("upload.json")).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let file: UploadFile = serde_json::from_slice(&bytes)
            .map_err(|err| corrupt_state_file(dir.join("upload.json"), err))?;
        let Ok(bucket) = bucket::name(file.bucket) else {
            return Ok(None);
        };
        let Ok(key) = object::key(file.key) else {
            return Ok(None);
        };
        Ok(Some(MultipartUpload {
            upload_id: file.upload_id,
            bucket,
            key,
            initiated_at: from_nanos(file.initiated_at),
        }))
    }

    /// `<state-dir>/multipart/<bucket>/<upload_id>` — the id is
    /// client-supplied, so only UUIDs (as `create` allocates) may map to a
    /// state-dir path; anything else (e.g. `../`) answers `NoSuchUpload`.
    fn upload_dir(&self, bucket: &bucket::Name, upload_id: &str) -> Result<PathBuf, Error> {
        if uuid::Uuid::parse_str(upload_id).is_err() {
            return Err(storage::no_such_upload(upload_id).into());
        }
        Ok(self.root.join(&**bucket).join(upload_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use tinio_core::storage::Error as StorageError;
    use tinio_core::testing::{body, etag};

    fn store() -> (tempfile::TempDir, MultipartStore) {
        let state = tempfile::tempdir().unwrap();
        let store = MultipartStore::new(state.path());
        (state, store)
    }

    #[test]
    fn create_writes_upload_json() {
        rt(async {
            let (state, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            assert!(!upload.upload_id.is_empty());
            let json = tokio::fs::read_to_string(
                state
                    .path()
                    .join("multipart/data")
                    .join(&upload.upload_id)
                    .join("upload.json"),
            )
            .await
            .unwrap();
            assert!(json.contains(&k.to_string()));
        });
    }

    #[test]
    fn put_list_part_round_trip() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let part = store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"part-one"))
                .await
                .unwrap();
            assert_eq!(part.size, 8);
            assert_eq!(part.etag, etag("dede9db222ee612853f44e6e6b1ca792"));
            let (parts, truncated) =
                store.list_parts(&b, &k, &upload.upload_id, None, 1000).await.unwrap();
            assert_eq!(parts.len(), 1);
            assert!(!truncated);
            assert_eq!(parts[0].etag, part.etag);
        });
    }

    #[test]
    fn put_part_missing_upload_is_no_such_upload() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = MultipartUpload {
                upload_id: "ghost".into(),
                bucket: b.clone(),
                key: k.clone(),
                initiated_at: SystemTime::now(),
            };
            let err = store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn non_uuid_upload_ids_are_no_such_upload() {
        // The upload id is client-supplied and maps into the state-dir
        // path — only UUIDs may do so (no `../` traversal).
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            for evil in ["../victim/abc", "a/b", "..", ""] {
                let err = store
                    .put_part(&b, &k, evil, 1.into(), body(b"x"))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, Error::Storage(StorageError::NoSuchUpload(_))),
                    "{evil}"
                );
            }
        });
    }

    #[test]
    fn complete_under_a_different_key_is_no_such_upload() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            let completed = [CompletedPart {
                part_number: 1.into(),
                etag: etag("9dd4e461268c8034f5c8564e155c67a6"),
            }];
            let err = store
                .complete(
                    &b,
                    &object::key("b.bin").unwrap(),
                    &upload.upload_id,
                    &completed,
                )
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn put_part_under_a_different_key_is_no_such_upload() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let err = store
                .put_part(
                    &b,
                    &object::key("b.bin").unwrap(),
                    &upload.upload_id,
                    1.into(),
                    body(b"x"),
                )
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn complete_assembles_byte_exact_with_composed_etag() {
        rt(async {
            let (state, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let mut parts = Vec::new();
            let parts_data: [&[u8]; 3] = [b"part-one-", b"part-two-", b"part-three"];
            for (i, data) in parts_data.iter().enumerate() {
                let part = store
                    .put_part(
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
            let completed: Vec<CompletedPart> = parts
                .iter()
                .map(|p| CompletedPart {
                    part_number: p.part_number,
                    etag: p.etag.clone(),
                })
                .collect();
            let target = state.path().join("out.bin");
            let (temp, etag) = store
                .complete(&b, &k, &upload.upload_id, &completed)
                .await
                .unwrap();
            // Hard-coded reference composition (MD5 of raw part digests).
            assert_eq!(etag.as_str(), "aed23cbfc502f1e851e828efe2ca50d0-3");
            tokio::fs::rename(&temp, &target).await.unwrap();
            assert_eq!(
                tokio::fs::read(&target).await.unwrap(),
                b"part-one-part-two-part-three"
            );
            // The upload subtree is gone.
            assert!(
                tokio::fs::metadata(state.path().join("multipart/data").join(&upload.upload_id))
                    .await
                    .is_err()
            );
            // The object is retrievable as a regular object.
            let content = tokio::fs::read(&target).await.unwrap();
            assert_eq!(content, b"part-one-part-two-part-three");
        });
    }

    #[test]
    fn complete_no_parts_is_error() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let err = store
                .complete(&b, &k, &upload.upload_id, &[])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoParts)));
        });
    }

    #[test]
    fn complete_mismatched_or_missing_part_is_invalid_part() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();

            // Wrong ETag.
            let wrong = CompletedPart {
                part_number: 1.into(),
                etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
            };
            let err = store
                .complete(&b, &k, &upload.upload_id, &[wrong])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::InvalidPart(1))));

            // Missing part number 2 after 1.
            let good = CompletedPart {
                part_number: 1.into(),
                etag: etag("9dd4e461268c8034f5c8564e155c67a6"),
            };
            let missing = CompletedPart {
                part_number: 3.into(),
                etag: etag("9dd4e461268c8034f5c8564e155c67a6"),
            };
            let err = store
                .complete(&b, &k, &upload.upload_id, &[good.clone(), missing.clone()])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::InvalidPart(3))));

            // Out of order (a repeated part number is not strictly
            // ascending).
            let err = store
                .complete(&b, &k, &upload.upload_id, &[good.clone(), good.clone()])
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::InvalidPart(1))));
        });
    }

    #[test]
    fn abort_removes_parts_and_is_no_such_upload_after() {
        rt(async {
            let (state, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
                .await
                .unwrap();
            store.abort(&b, &k, &upload.upload_id).await.unwrap();
            assert!(
                tokio::fs::metadata(state.path().join("multipart/data").join(&upload.upload_id))
                    .await
                    .is_err()
            );
            let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn complete_and_abort_are_mutually_exclusive() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("big.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            let mut completed = Vec::new();
            for i in 1..=4u32 {
                let data = vec![i as u8; 32 * 1024];
                let part = store
                    .put_part(&b, &k, &upload.upload_id, i.into(), body(data))
                    .await
                    .unwrap();
                completed.push(CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                });
            }
            let (complete, abort) = tokio::join!(
                store.complete(&b, &k, &upload.upload_id, &completed),
                store.abort(&b, &k, &upload.upload_id),
            );
            assert!(
                complete.is_ok() ^ abort.is_ok(),
                "exactly one of complete/abort must win, complete={complete:?} abort={abort:?}"
            );
        });
    }

    #[test]
    fn list_uploads_and_has_uploads() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k1 = object::key("a.bin").unwrap();
            let k2 = object::key("b.bin").unwrap();
            let u1 = store.create(&b, &k1).await.unwrap();
            let u2 = store.create(&b, &k2).await.unwrap();
            let uploads = store.list_uploads(&b).await.unwrap();
            assert_eq!(uploads.len(), 2);
            assert!(store.has_uploads(&b).await.unwrap());
            store.abort(&b, &k1, &u1.upload_id).await.unwrap();
            store.abort(&b, &k2, &u2.upload_id).await.unwrap();
            assert!(!store.has_uploads(&b).await.unwrap());
        });
    }

    #[test]
    fn list_uploads_orders_same_key_group_by_upload_id() {
        // The composite `key\0upload_id` order positions a page inside a
        // same-key group: pagination must never skip an upload because a
        // page cut between two uploads of one key.
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("same.bin").unwrap();
            store.create(&b, &k).await.unwrap();
            store.create(&b, &k).await.unwrap();
            let uploads = store.list_uploads(&b).await.unwrap();
            assert_eq!(uploads.len(), 2);
            assert!(
                uploads[0].upload_id < uploads[1].upload_id,
                "same-key uploads must be ordered by upload id: {:?}",
                uploads.iter().map(|u| &u.upload_id).collect::<Vec<_>>()
            );
        });
    }

    #[test]
    fn remove_bucket_clears_uploads() {
        rt(async {
            let (_, store) = store();
            let b = bucket::name("data").unwrap();
            let k = object::key("a.bin").unwrap();
            let upload = store.create(&b, &k).await.unwrap();
            store.remove_bucket(&b).await.unwrap();
            assert!(!store.has_uploads(&b).await.unwrap());
            let err = store.abort(&b, &k, &upload.upload_id).await.unwrap_err();
            assert!(matches!(err, Error::Storage(StorageError::NoSuchUpload(_))));
        });
    }

    #[test]
    fn walk_uploads_finds_all() {
        rt(async {
            let (_, store) = store();
            let b1 = bucket::name("alpha").unwrap();
            let b2 = bucket::name("zeta").unwrap();
            store
                .create(&b1, &object::key("a.bin").unwrap())
                .await
                .unwrap();
            store
                .create(&b2, &object::key("z.bin").unwrap())
                .await
                .unwrap();
            let uploads = store.walk_uploads().await.unwrap();
            assert_eq!(uploads.len(), 2);
        });
    }
}
