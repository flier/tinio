//! The filesystem content plane (D23).
//!
//! [`FsContent`] is this backend's [`ObjectContent`] implementation: the
//! bytes half of the storage, split from the metadata facade
//! ([`crate::meta::Store`]) at the contract seam. It is a named handle over
//! the same [`FsStorage`] — built by [`FsStorage::content`] — so the
//! file-half primitives keep their one home in `backend/`, and the contract
//! methods are thin compositions of them: the path mapping, the atomic
//! staged write, the version engine's publish, the multipart part layout and
//! the listing walk.
//!
//! Nothing here touches the state tables: the row describing the bytes is
//! the ops' (the D37 split). The content plane's answers are therefore
//! byte-truths — `stat` is the file's stamp, `is_empty` counts files — which
//! is exactly the contract's plane (it holds no rows to consult).

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::remove_file,
    io::{Error as IoError, ErrorKind as IoErrorKind},
    panic,
    path::PathBuf,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use tokio::{fs, runtime::Handle, task};
use tracing::instrument;

use crate::{
    _core::{
        ETag, bucket,
        content::{ContentEntry, ContentKey, ObjectContent, StagedObject, stage_body},
        object,
        storage::{self, BodyStream, BucketOps, ByteRange, access_denied},
    },
    _util::metrics::WriteOp,
    Error,
    backend::{
        FsStorage, Publication, StagedBytes, bucket_is_empty, prune_parents, remove_object_file,
    },
    error::record_failed_write,
    fsutil,
    listing::WalkedFile,
    write::StagedFile,
};

/// The filesystem content plane: [`ObjectContent`] over one [`FsStorage`].
///
/// Cheap to build and idempotent — it borrows the store's root, state dir
/// and single state-database handle through one `Arc`, so two content
/// handles over the same store share every lock and pipeline. No field
/// lives on [`FsStorage`], which keeps the two planes acyclic.
#[derive(Debug, Clone)]
pub struct FsContent {
    storage: Arc<FsStorage>,
    /// Whether the lock-taking operations (`publish`, `rename`) take the
    /// bucket mutation lock themselves. The public content plane's own
    /// paths hold no lock when they publish or move (copy), while the
    /// version engine's port is only ever called from under the caller's
    /// bucket mutation lock (a restore rides the delete path's one critical
    /// section) — taking it again there is a deadlock on the non-re-entrant
    /// per-key lock, not a critical section.
    own_lock: bool,
}

impl FsStorage {
    /// The content plane of this storage — the bytes half of the same
    /// store (the metadata facade is [`crate::meta::Store`]).
    #[must_use]
    pub fn content(self: &Arc<Self>) -> FsContent {
        FsContent::new(Arc::clone(self))
    }
}

impl FsContent {
    /// The content plane over one backend — the same handle the public
    /// [`FsStorage::content`] builds, reachable from a bare `&FsStorage`
    /// (the version wiring needs it without an `Arc`).
    pub(crate) fn new(storage: Arc<FsStorage>) -> Self {
        Self {
            storage,
            own_lock: true,
        }
    }

    /// The **non-owning** plane: the same handles, with the publishes and
    /// moves that would take the bucket mutation lock riding the lock their
    /// **caller** already holds instead. Two callers need it — the version
    /// engine's port (a restore rides the delete path's one critical
    /// section) and the ops engine's object group (`FsStorage::objects`,
    /// whose bodies run inside the trait impl's own hold of that lock).
    /// Taking the lock again there would
    /// be a deadlock on the non-re-entrant per-bucket lock, not a critical
    /// section.
    pub(crate) fn for_engine(storage: Arc<FsStorage>) -> Self {
        Self {
            storage,
            own_lock: false,
        }
    }

    /// The multipart layout's part path for a part key (the upload id
    /// validated as the UUID the create allocated).
    fn part_file(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        number: u32,
    ) -> Result<PathBuf, Error> {
        self.storage
            .multipart_store()
            .part_file(bucket, upload_id, number)
    }
}

/// The staged write of the content plane: a temp file under `tmp/` the
/// chunks land in, or the folder-marker sentinel (a marker is a directory —
/// `publish` creates it and staged bytes are dropped).
#[derive(Debug)]
pub struct FsStaged {
    staged: Option<StagedFile>,
    etag: ETag,
}

impl FsStaged {
    /// Flush and content-sync the temp (D1) and answer its path plus the
    /// content ETag — `None` for the marker sentinel.
    ///
    /// The temp passes to the caller: this stage no longer owns it, so the
    /// caller is the one that must remove it when its publish fails (see
    /// the part arm of [`FsContent`]'s `publish`).
    async fn finish(mut self) -> Result<(Option<PathBuf>, ETag), Error> {
        match self.staged.take() {
            Some(staged) => {
                let (temp, etag, _len) = staged.finish(None).await?;
                Ok((Some(temp), etag))
            }
            None => Ok((None, self.etag.clone())),
        }
    }

    /// Chown the staged temp before it is renamed into place. The rename
    /// keeps the uid, so a refusal here never publishes and never moves a
    /// version. A marker stage holds no file.
    #[cfg(unix)]
    pub(crate) async fn chown_temp(&self, uid: u32) -> Result<(), Error> {
        let Some(staged) = &self.staged else {
            return Ok(());
        };
        fsutil::chown_file(staged.path(), uid)
            .await
            .map_err(Into::into)
    }
}

impl Drop for FsStaged {
    fn drop(&mut self) {
        // Never published: remove the staged temp best-effort (the sweep
        // would reclaim it otherwise). The same rule as `StagedBytes`'s drop:
        // the unlink runs off the async thread when a runtime is present, and
        // a shutting-down runtime's panicking `spawn_blocking` is caught — a
        // cancelled request's drop must not panic (the startup sweep is the
        // backstop).
        let Some(staged) = self.staged.take() else {
            return;
        };
        let temp = staged.into_temp();
        if Handle::try_current().is_ok() {
            let _ = panic::catch_unwind(|| {
                drop(task::spawn_blocking(move || remove_file(&temp)));
            });
        } else {
            let _ = remove_file(&temp);
        }
    }
}

#[async_trait]
impl StagedObject for FsStaged {
    type Error = Error;

    async fn write(&mut self, chunk: Bytes) -> Result<(), Error> {
        match &mut self.staged {
            // A marker's bytes are dropped: a marker is a directory.
            None => Ok(()),
            Some(staged) => staged.write_chunk(&chunk).await,
        }
    }
}

/// Whether the failure is the content plane's "no such bucket": the
/// contract's probes answer an absent namespace without an error (`Ok(None)`,
/// `Ok(())`, `Ok(true)`) — the same face the row-backed plane shows, where an
/// absent bucket holds no row.
fn is_no_such_bucket(err: &Error) -> bool {
    matches!(err, Error::Storage(storage::Error::NoSuchBucket(_)))
}

/// The destination rules `rename` and `copy` share — the S3 surface's,
/// applied before any byte moves: a self-move is the identity (a
/// read-then-write-then-remove implementation would delete the bytes it just
/// moved, R14), a reserved segment is never a target, and a marker
/// destination would turn the key into a directory. `false` when `from` and
/// `to` name the same key — the caller then stops without touching the bytes.
fn destination_clear(from: &object::Key, to: &object::Key) -> Result<bool, Error> {
    if from == to {
        return Ok(false);
    }
    if to.is_reserved() {
        return Err(access_denied(to).into());
    }
    if to.is_folder_marker() {
        return Err(storage::invalid_key(to.to_string()).into());
    }
    Ok(true)
}

/// One walk result in the `scan` page accumulator, ordered by key: a max-heap
/// of the page keeps the `limit` smallest keys above `after` while the walk
/// streams (the walk's own order is a directory walk, not lexicographic).
struct PageEntry(WalkedFile);

impl PartialEq for PageEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for PageEntry {}

impl PartialOrd for PageEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PageEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.key.as_ref().cmp(other.0.key.as_ref())
    }
}

#[async_trait]
impl ObjectContent for FsContent {
    type Error = Error;
    type Staged = FsStaged;

    async fn stage(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<FsStaged, Error> {
        match key {
            ContentKey::Object(key) => {
                // The mapping is validated before any bytes move (the symlink
                // policy included); the publish re-resolves under the mutation
                // lock, so a retargeted link cannot send the write astray.
                let bucket_dir = self.storage.ensure_bucket(bucket).await?;
                self.storage.resolve_key(&bucket_dir, key).await?;
                if key.is_folder_marker() {
                    // A marker is a directory: nothing to stage — publish
                    // creates it (the sentinel's empty ETag).
                    return Ok(FsStaged {
                        staged: None,
                        etag: ETag::EMPTY,
                    });
                }
            }
            ContentKey::Part { .. } => {
                self.storage.ensure_bucket(bucket).await?;
            }
        }
        Ok(FsStaged {
            staged: Some(self.storage.writer().stage_file().await?),
            etag: ETag::EMPTY,
        })
    }

    #[instrument(
        level = "debug",
        target = "tinio::storage",
        name = "fs.publish",
        skip(self, staged),
        fields(
            bucket = %bucket,
            op = if key.is_part() { "part" } else { "object" },
        )
    )]
    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: FsStaged,
        key: &ContentKey,
    ) -> Result<(), Error> {
        match key {
            ContentKey::Object(key) => {
                // The visibility point: the temp is renamed onto the key
                // under the bucket mutation lock, through the version
                // engine's supersede protocol. The row describing the bytes
                // is the op's (D37) — publish writes none — and the contract
                // carries no owner, so the landed file keeps the store
                // user's ownership at `0600` (the row's owner element is the
                // caller's own). `StagedBytes` owns the temp from here and
                // removes it on every failure path (its own drop).
                let (temp, etag) = staged.finish().await?;
                // A marker is a directory, not a publish: its commit creates
                // it (the staged bytes are dropped — a marker stage holds
                // none) and no row describes it.
                if key.is_folder_marker() {
                    self.storage.commit_marker(bucket, key).await?;
                    return Ok(());
                }
                // A real object's stage always holds a temp (`finish` returns
                // `None` only for the marker sentinel, taken above).
                let Some(temp) = temp else {
                    return Err(IoError::other("a staged object always holds a temp file").into());
                };
                let publication = if self.own_lock {
                    Publication::Bytes
                } else {
                    Publication::BytesUnderLock
                };
                self.storage
                    .publish_object_file(
                        bucket,
                        key,
                        StagedBytes::staged(temp, etag),
                        None,
                        publication,
                    )
                    .await?;
                Ok(())
            }
            ContentKey::Part { upload_id, number } => {
                // The layout validates the upload id BEFORE the stage is
                // consumed: an id that is not the UUID a create allocated
                // refuses with `NoSuchUpload` while the dropped stage still
                // owns — and removes — its temp.
                let target = self.part_file(bucket, upload_id, u32::from(*number))?;
                let (temp, _etag) = staged.finish().await?;
                let Some(temp) = temp else {
                    return Err(IoError::other("a part stage always holds bytes").into());
                };
                // The part's atomic publish is the multipart write path's
                // rename (same volume — the state dir); the temp was already
                // flushed and content-synced (D1). The `PARTS` row is the
                // row plane's, written by the ops engine's part body in the
                // same critical section as this rename — the
                // `tinio_ops::Hooks::lock_part` guard, whose home is the
                // multipart store's per-upload map.
                //
                // `finish` took the temp out of the stage, so its drop no
                // longer cleans anything up: the rename below must leave no
                // residue on any failure path (the same rule the completion
                // assembly and the multipart write path keep). The success
                // path consumes the temp — the name moves onto the part
                // path — and needs no cleanup.
                let publish = async {
                    if let Some(parent) = target.parent() {
                        fs::create_dir_all(parent).await?;
                    }
                    fs::rename(&temp, &target).await?;
                    Ok::<_, Error>(())
                }
                .await;
                if let Err(err) = publish {
                    let _ = fs::remove_file(&temp).await;
                    return Err(err);
                }
                Ok(())
            }
        }
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        let (file, size) = match key {
            ContentKey::Object(key) => {
                let (_path, file, size, _mtime, _identity) =
                    self.storage.resolve_object_file(bucket, key).await?;
                (file, size)
            }
            ContentKey::Part { upload_id, number } => {
                let number = u32::from(*number);
                let path = self.part_file(bucket, upload_id, number)?;
                let file = match fsutil::open_file(&path, *self.storage.follow_symlinks()).await {
                    Ok(file) => file,
                    // The multipart contract's absent-part answer.
                    Err(err) if err.kind() == IoErrorKind::NotFound => {
                        return Err(storage::invalid_part(number).into());
                    }
                    Err(err) => return Err(err.into()),
                };
                let size = file.metadata().await?.len();
                (file, size)
            }
        };
        // One home for the body shape: the shared clamp (a range past the
        // end is empty, never an overshoot) and the end-of-body policy.
        Ok(FsStorage::object_body(file, size, range).await?.0)
    }

    #[instrument(
        level = "debug",
        target = "tinio::storage",
        skip(self),
        fields(
            bucket = %bucket,
            op = if key.is_part() { "part" } else { "object" },
        )
    )]
    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Error> {
        let result = match key {
            ContentKey::Object(key) => {
                // Idempotent: an absent bucket or object is a no-op, like
                // the row-backed plane's removal of an absent row.
                let bucket_dir = match self.storage.ensure_bucket(bucket).await {
                    Ok(dir) => dir,
                    Err(err) if is_no_such_bucket(&err) => return Ok(()),
                    Err(err) => return Err(err),
                };
                let path = self.storage.resolve_key(&bucket_dir, key).await?;
                remove_object_file(&path, *self.storage.follow_symlinks()).await?;
                prune_parents(&path, &bucket_dir).await;
                Ok(())
            }
            ContentKey::Part { upload_id, number } => {
                let path = self.part_file(bucket, upload_id, u32::from(*number))?;
                match fs::remove_file(&path).await {
                    Ok(()) => Ok(()),
                    Err(err) if err.kind() == IoErrorKind::NotFound => Ok(()),
                    Err(err) => Err(err.into()),
                }
            }
        };
        if let Err(err) = &result {
            record_failed_write(WriteOp::Delete, err);
        }
        result
    }

    #[instrument(
        level = "debug",
        target = "tinio::storage",
        skip(self, from),
        fields(bucket = %bucket, key = %to)
    )]
    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        // The S3 destination rules, one home for the rename/copy pair: a
        // self-move stops without touching the bytes.
        if !destination_clear(from, to)? {
            return Ok(());
        }
        // The bytes half of `rename_object`: the file move, under the bucket
        // mutation lock this handle takes only when it owns it (the
        // `Publication::BytesUnderLock` rule, on the rename path).
        // `move_object_file` requires its caller to hold that lock and takes
        // none itself; the version engine's port is called from under the
        // caller's lock — the delete path's restore holds it across the whole
        // move — and the per-key lock is not re-entrant, so taking it again
        // there is a deadlock, not a critical section. The row migration is
        // the op's.
        let _guard = if self.own_lock {
            Some(self.storage.lock_bucket_mutations(bucket).await)
        } else {
            None
        };
        self.storage.move_object_file(bucket, from, to).await?;
        Ok(())
    }

    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if !destination_clear(from, to)? {
            return Ok(());
        }
        // The portable byte route through the contract's own calls: read the
        // source as served bytes (whatever the policy/vendor path the store
        // resolves) and publish a fresh destination. The ops keep their
        // `copy_object_fast` inode fast path; no row is written here — the
        // destination's recorded checksum and owner are the op's.
        let body = self
            .open(bucket, &ContentKey::Object(from.clone()), None)
            .await?;
        let mut staged = self.stage(bucket, &ContentKey::Object(to.clone())).await?;
        // The shared streaming pump (the version engine's own): a chunk's
        // error projects onto the contract error, and the landed count is the
        // caller's to ignore (a copy records no row here).
        stage_body(&mut staged, body).await?;
        self.publish(bucket, staged, &ContentKey::Object(to.clone()))
            .await
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // The shared streaming walk the scanner uses: the reserved private
        // paths and the folder markers are filtered inside it, and the prefix
        // is a directory bound — the walk never descends past it.
        let mut walk = self
            .storage
            .listing()
            .walk_files_streaming(bucket, prefix)
            .await?;
        let after = after.cloned();
        let mut page: BinaryHeap<PageEntry> = BinaryHeap::new();
        while let Some(item) = walk.next().await {
            let walked = item?;
            // The prefix is a bound, not a filter the walk may soften: the
            // walk prunes directories by it, and the page re-checks every key
            // ("alpine" is outside "alpha") — the contract's rule.
            if !walked.key.as_ref().starts_with(prefix) {
                continue;
            }
            if let Some(after) = &after
                && walked.key.as_ref() <= after.as_ref()
            {
                continue;
            }
            page.push(PageEntry(walked));
            if page.len() > limit {
                let _ = page.pop();
            }
        }
        // The accumulator is a max-heap, so its sorted drain is the page in
        // ascending key order (the contract's ordering).
        Ok(page
            .into_sorted_vec()
            .into_iter()
            .map(|entry| {
                // The identity borrows the stat before the key moves out.
                let identity = entry.0.identity();
                ContentEntry {
                    key: entry.0.key,
                    len: entry.0.size,
                    identity,
                    mtime: Some(entry.0.mtime),
                }
            })
            .collect())
    }

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Error> {
        // A reserved path is never an object (`NoSuchKey` on the surface).
        if key.is_reserved() {
            return Ok(None);
        }
        let bucket_dir = match self.storage.ensure_bucket(bucket).await {
            Ok(dir) => dir,
            Err(err) if is_no_such_bucket(&err) => return Ok(None),
            Err(err) => return Err(err),
        };
        let path = self.storage.resolve_key(&bucket_dir, key).await?;
        let metadata = match fsutil::object_metadata(&path, *self.storage.follow_symlinks()).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == IoErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        if !metadata.is_file() {
            // A folder marker (a directory) holds no bytes.
            return Ok(None);
        }
        Ok(Some(ContentEntry {
            key: key.clone(),
            len: metadata.len(),
            identity: fsutil::file_identity(&path, &metadata),
            mtime: metadata.modified().ok(),
        }))
    }

    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        // The namespace only: the `BUCKETS` record is the metadata facade's
        // (`BucketMeta::create` calls the same directory helper).
        self.storage.create_bucket_dir(bucket, None).await
    }

    /// The content plane's bucket teardown, through the same path the S3
    /// surface's `DeleteBucket` takes: this backend's own
    /// [`BucketOps::delete_bucket`](tinio_core::storage::BucketOps::delete_bucket)
    /// — the bucket mutation guard, the namespace probe and the engine call —
    /// rather than a teardown of its own.
    ///
    /// Emptiness is **not** re-decided here. It has one home, the ops
    /// engine's [`BucketOps::delete_bucket`](tinio_ops::BucketOps::delete_bucket),
    /// which composes this plane's `is_empty` with the in-progress uploads
    /// ([`UploadMeta::has_uploads`](tinio_core::meta::UploadMeta::has_uploads))
    /// and the object rows
    /// ([`ObjectMeta::has_objects`](tinio_core::meta::ObjectMeta::has_objects)).
    /// Routing through it is what keeps this path from tearing down a bucket
    /// the composition refuses: a **delete marker** writes no bytes and an
    /// in-progress upload writes none, so a content-only probe reads such a
    /// bucket empty and the namespace would be gone.
    ///
    /// Idempotent: an absent namespace is already removed (the row-backed
    /// plane's no-op remove).
    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        match BucketOps::delete_bucket(&*self.storage, bucket).await {
            Ok(()) => Ok(()),
            Err(err) if is_no_such_bucket(&err) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        match bucket_is_empty(&self.storage, bucket).await {
            Ok(empty) => Ok(empty),
            // An absent namespace holds nothing.
            Err(err) if is_no_such_bucket(&err) => Ok(true),
            Err(err) => Err(err),
        }
    }
}
#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, path::Path, sync::Arc, time::SystemTime};

    use bytes::Bytes;
    use futures::StreamExt;
    use tokio::{fs, task::yield_now};

    use super::FsContent;
    use crate::{
        _core::{
            CompletedPart, ETag,
            acl::Acl,
            bucket,
            content::{ContentKey, ObjectContent, StagedObject},
            meta::{ObjectMeta, object as meta_object},
            multipart, object,
            storage::{
                BucketOps, ByteRange, Error as StorageError, ListObjectsParams, MultipartOps,
                ObjectOps,
            },
        },
        _util::testing::{body, metadata::assert_content_conformance, read_body},
        Error, fsutil,
        testutil::storage,
    };

    /// A whole content body (all chunks).
    async fn read(
        content: &FsContent,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Vec<u8> {
        let mut body = content.open(bucket, key, range).await.unwrap();
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    /// The keys of one `scan` page.
    async fn walked(
        content: &FsContent,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Vec<object::Key> {
        content
            .scan(bucket, prefix, after, limit)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.key)
            .collect()
    }

    /// The shared D23 leg against this backend's content plane:
    /// `assert_content_conformance` runs every rule [`ObjectContent`]
    /// carries — the same suite the row-backed planes run, so an
    /// fs-specific drift is caught here too.
    #[tokio::test]
    async fn shared_content_conformance_is_green() {
        let (_root, storage) = storage();
        let storage = Arc::new(storage);
        assert_content_conformance(&storage.content()).await;
    }

    /// The content plane's whole surface over one real store: the staged
    /// write and its visibility point, the ranged read, the part-key layout,
    /// the move/copy/remove primitives, the walk and its page bound, and the
    /// bucket lifecycle — `assert_content_conformance`'s legs at this
    /// backend's own shapes (the shared leg's synthetic part ids do not
    /// satisfy the multipart layout's UUID rule, so the part leg runs on a
    /// real upload id here).
    #[tokio::test]
    async fn content_plane_round_trips_the_contract() {
        let (_root, storage) = storage();
        let storage = Arc::new(storage);
        let content = storage.content();
        let bucket = bucket::name("conf-content").unwrap();
        content.create_bucket(&bucket).await.unwrap();
        assert!(
            content.is_empty(&bucket).await.unwrap(),
            "a fresh content bucket is empty"
        );

        let a = object::key("a.bin").unwrap();
        let a_key = ContentKey::Object(a.clone());
        assert!(
            content.stat(&bucket, &a).await.unwrap().is_none(),
            "a key with no bytes has no entry"
        );
        assert!(
            content.open(&bucket, &a_key, None).await.is_err(),
            "a missing key is an error: D23's open has no None arm"
        );

        // Publish is the visibility point.
        let mut staged = content.stage(&bucket, &a_key).await.unwrap();
        staged.write(Bytes::from_static(b"hello")).await.unwrap();
        assert!(
            content.open(&bucket, &a_key, None).await.is_err(),
            "a staged write is invisible before publish"
        );
        content.publish(&bucket, staged, &a_key).await.unwrap();
        assert_eq!(read(&content, &bucket, &a_key, None).await, b"hello");
        let entry = content.stat(&bucket, &a).await.unwrap().unwrap();
        assert_eq!(entry.len, 5);
        assert_eq!(entry.key, a, "stat names the key it was asked for");
        assert!(!content.is_empty(&bucket).await.unwrap());

        // The range semantics: inclusive, open-ended and suffix.
        assert_eq!(
            read(&content, &bucket, &a_key, Some(ByteRange::Inclusive(1, 3))).await,
            b"ell"
        );
        assert_eq!(
            read(&content, &bucket, &a_key, Some(ByteRange::From(3))).await,
            b"lo"
        );
        assert_eq!(
            read(&content, &bucket, &a_key, Some(ByteRange::Suffix(2))).await,
            b"lo"
        );

        // A re-publish replaces the bytes.
        let mut again = content.stage(&bucket, &a_key).await.unwrap();
        again.write(Bytes::from_static(b"HELLO")).await.unwrap();
        content.publish(&bucket, again, &a_key).await.unwrap();
        assert_eq!(read(&content, &bucket, &a_key, None).await, b"HELLO");

        // The part key shape addresses the upload's part bytes (the layout
        // validates the id as the UUID the multipart create allocates).
        let part_key = ContentKey::Part {
            upload_id: uuid::Uuid::new_v4().to_string(),
            number: multipart::part_number(2).unwrap(),
        };
        let mut staged = content.stage(&bucket, &part_key).await.unwrap();
        staged
            .write(Bytes::from_static(b"part-bytes"))
            .await
            .unwrap();
        content.publish(&bucket, staged, &part_key).await.unwrap();
        assert_eq!(
            read(&content, &bucket, &part_key, None).await,
            b"part-bytes"
        );

        // Copy duplicates, rename moves, and a self-move is the identity.
        let b = object::key("b.bin").unwrap();
        let b_key = ContentKey::Object(b.clone());
        content.copy(&bucket, &a, &b).await.unwrap();
        assert_eq!(read(&content, &bucket, &b_key, None).await, b"HELLO");
        let c = object::key("c.bin").unwrap();
        let c_key = ContentKey::Object(c.clone());
        content.rename(&bucket, &b, &c).await.unwrap();
        assert!(content.open(&bucket, &b_key, None).await.is_err());
        assert_eq!(read(&content, &bucket, &c_key, None).await, b"HELLO");
        content.rename(&bucket, &c, &c).await.unwrap();
        content.copy(&bucket, &c, &c).await.unwrap();
        assert_eq!(
            read(&content, &bucket, &c_key, None).await,
            b"HELLO",
            "a self-move/self-copy leaves the bytes where they are"
        );

        // Remove is idempotent; the part's bytes count as content.
        content.remove(&bucket, &c_key).await.unwrap();
        content.remove(&bucket, &c_key).await.unwrap();
        assert!(content.open(&bucket, &c_key, None).await.is_err());
        assert!(
            !content.is_empty(&bucket).await.unwrap(),
            "a staged part's bytes are content"
        );
        content.remove(&bucket, &a_key).await.unwrap();
        content.remove(&bucket, &part_key).await.unwrap();
        assert!(
            content.is_empty(&bucket).await.unwrap(),
            "with every object and part gone the bucket is empty"
        );
        content.remove_bucket(&bucket).await.unwrap();
        content.remove_bucket(&bucket).await.unwrap();

        // The walk: ascending keys, the prefix bound, `after` exclusivity,
        // the page cap, and the point probe agreeing with the page.
        let scan = bucket::name("conf-scan").unwrap();
        content.create_bucket(&scan).await.unwrap();
        for name in ["alpha", "alpha-1", "alpine", "beta"] {
            let k = ContentKey::Object(object::key(name).unwrap());
            let mut staged = content.stage(&scan, &k).await.unwrap();
            staged.write(Bytes::from_static(b"x")).await.unwrap();
            content.publish(&scan, staged, &k).await.unwrap();
        }
        assert_eq!(
            walked(&content, &scan, "", None, 100).await,
            vec![
                object::key("alpha").unwrap(),
                object::key("alpha-1").unwrap(),
                object::key("alpine").unwrap(),
                object::key("beta").unwrap(),
            ],
            "scan is ascending by the key's bytes"
        );
        assert_eq!(
            walked(&content, &scan, "alpha", None, 100).await,
            vec![
                object::key("alpha").unwrap(),
                object::key("alpha-1").unwrap(),
            ],
            "prefix is a bound the walk must not pass"
        );
        assert_eq!(
            walked(
                &content,
                &scan,
                "alpha",
                Some(&object::key("alpha").unwrap()),
                100
            )
            .await,
            vec![object::key("alpha-1").unwrap()],
            "after is exclusive"
        );
        assert_eq!(content.scan(&scan, "", None, 2).await.unwrap().len(), 2);
        assert!(content.scan(&scan, "", None, 0).await.unwrap().is_empty());
        let page = content.scan(&scan, "alpha", None, 100).await.unwrap();
        let probe = content
            .stat(&scan, &object::key("alpha").unwrap())
            .await
            .unwrap()
            .unwrap();
        let entry = page
            .iter()
            .find(|entry| entry.key == object::key("alpha").unwrap())
            .unwrap();
        assert_eq!(
            (entry.len, entry.identity, entry.mtime),
            (probe.len, probe.identity, probe.mtime),
            "the page and the point probe agree on the stamp (B1)"
        );
    }

    /// `remove_bucket` enforces the **whole** emptiness rule, not this
    /// plane's half of it. The rule has one home — the ops engine's
    /// `BucketOps::delete_bucket`, which composes `is_empty` with the upload
    /// and object-row probes — and this path reaches it through fs's own
    /// `delete_bucket`, so a bucket whose only state is a row or an upload is
    /// refused here too. Both buckets below read **empty** to the content
    /// plane (a delete marker writes no bytes, and a fresh upload writes
    /// none), so against the content-only teardown this path used to run both
    /// legs go green by succeeding where they must refuse.
    #[tokio::test]
    async fn remove_bucket_refuses_a_bucket_the_composition_refuses() {
        let (_root, storage) = storage();
        let storage = Arc::new(storage);
        let content = storage.content();
        let acl = Acl::default_private(None);

        // The **object-row** probe: a row with no content behind it — a
        // delete marker's shape (the recorded marker bit is the backend's
        // own and never crosses the contract).
        let marker = bucket::name("row-only").unwrap();
        storage.create_bucket(&marker, None, &acl).await.unwrap();
        ObjectMeta::put(
            &*storage.meta_store().engine(),
            &marker,
            &object::key("notes.txt").unwrap(),
            &detached_row(),
        )
        .await
        .unwrap();
        assert!(
            content.is_empty(&marker).await.unwrap(),
            "the row is the bucket's only state: the content plane holds nothing"
        );
        assert!(
            matches!(
                content.remove_bucket(&marker).await,
                Err(Error::Storage(StorageError::NotEmpty(_)))
            ),
            "a bucket holding only an object row is not empty"
        );
        storage
            .head_bucket(&marker)
            .await
            .expect("the refused teardown left the namespace in place");

        // The **upload** probe: an upload in progress, before any part lands.
        let pending = bucket::name("upload-only").unwrap();
        storage.create_bucket(&pending, None, &acl).await.unwrap();
        storage
            .create_multipart_upload(
                &pending,
                &object::key("big.bin").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &acl,
            )
            .await
            .unwrap();
        assert!(
            content.is_empty(&pending).await.unwrap(),
            "an upload with no part written holds no content yet"
        );
        assert!(
            matches!(
                content.remove_bucket(&pending).await,
                Err(Error::Storage(StorageError::NotEmpty(_)))
            ),
            "a bucket holding only an in-progress upload is not empty"
        );
    }

    /// One object row with nothing behind it — a **delete marker's** shape.
    fn detached_row() -> meta_object::Row {
        meta_object::Row {
            etag: "d41d8cd98f00b204e9800998ecf8427e".parse::<ETag>().unwrap(),
            size: 0,
            mtime: SystemTime::now(),
            identity: 0,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        }
    }

    /// A completed multipart object's metadata is present **exactly when**
    /// its bytes are served: while the upload is in flight its part bytes are
    /// content but the object key has neither a file nor a row, and the moment
    /// completion returns the assembled bytes and the row are both there —
    /// never one without the other (D37's order: the bytes land first, the row
    /// with them).
    #[tokio::test]
    async fn a_completed_multipart_objects_metadata_appears_with_its_bytes() {
        let (_root, storage) = storage();
        let storage = Arc::new(storage);
        let content = storage.content();
        let bucket = bucket::name("conf-multipart").unwrap();
        storage
            .create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("assembled.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &bucket,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"assembled"),
                None,
            )
            .await
            .unwrap();

        // In flight: the part's bytes are content, while the object key has no
        // file and no row — both halves agree the object is not there yet.
        let part_key = ContentKey::Part {
            upload_id: upload.upload_id.clone(),
            number: part.part_number,
        };
        assert_eq!(
            read(&content, &bucket, &part_key, None).await,
            b"assembled",
            "the part's bytes are content before completion"
        );
        assert!(
            content.stat(&bucket, &key).await.unwrap().is_none(),
            "no object file exists before completion"
        );
        let err: StorageError = storage.head_object(&bucket, &key).await.unwrap_err().into();
        assert!(
            matches!(err, StorageError::NoSuchKey(_)),
            "no object row exists before completion"
        );
        assert!(
            storage.get_object(&bucket, &key, None).await.is_err(),
            "and no object body is served before completion"
        );
        assert!(
            storage.list_object_parts(&bucket, &key).await.is_err(),
            "and no retained-part metadata exists before completion"
        );

        // Completion is the visibility point of both halves.
        storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag,
                }],
                None,
            )
            .await
            .unwrap();

        let head = storage.head_object(&bucket, &key).await.unwrap();
        assert_eq!(head.size, 9, "the row describes the assembled bytes");
        let parts = storage.list_object_parts(&bucket, &key).await.unwrap();
        assert_eq!(
            parts.len(),
            1,
            "the retained-part rows land with the assembled bytes"
        );
        assert_eq!(parts[0].size, 9);
        let served = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(
            read_body(served.body).await.unwrap(),
            b"assembled",
            "the completed object's bytes are served"
        );
        let entry = content
            .stat(&bucket, &key)
            .await
            .unwrap()
            .expect("the completed object's file");
        assert_eq!(
            entry.len, head.size,
            "the row and the file agree on the length"
        );
        assert!(
            walked(&content, &bucket, "", None, 10).await.contains(&key),
            "the completed object is walked"
        );

        // Not after: with the bytes gone the row goes too.
        storage.delete_object(&bucket, &key).await.unwrap();
        assert!(
            storage.head_object(&bucket, &key).await.is_err(),
            "the row goes with the bytes"
        );
        assert!(
            content.stat(&bucket, &key).await.unwrap().is_none(),
            "and so does the file"
        );
    }

    /// A content key with no metadata row is still content: the walk lists
    /// it and `stat` answers the stamp the ETag source needs (F01's
    /// identity) — the cold case a listing/scanner migration must not lose.
    #[tokio::test]
    async fn a_row_less_content_key_is_listed_and_statted() {
        let (root, storage) = storage();
        let storage = Arc::new(storage);
        let content = storage.content();
        let bucket = bucket::name("cold").unwrap();
        storage
            .create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .unwrap();
        // A hand-placed file inside the bucket: nothing here ever wrote an
        // `OBJECT_META` row for the key, and every read below derives its
        // ETag from the content instead of reading one.
        let path = root.path().join("cold").join("hand.txt");
        fs::write(&path, b"cold").await.unwrap();
        let key = object::key("hand.txt").unwrap();
        let before = storage
            .head_object(&bucket, &key)
            .await
            .expect("a row-less key is read from its file");
        assert_eq!(before.size, 4, "the file's length");
        assert_eq!(
            before.etag,
            ETag::from_content(b"cold"),
            "a row-less read's ETag is the content's"
        );

        // The cold listing serves it too: the walk finds the file and the
        // ETag is the content's, computed through the pipeline.
        let listing = storage
            .list_objects(ListObjectsParams {
                bucket: bucket.clone(),
                prefix: String::new(),
                delimiter: None,
                start_after: None,
                max_keys: 10,
            })
            .await
            .unwrap();
        let listed = listing
            .objects
            .iter()
            .find(|info| info.key == key)
            .expect("a row-less content key is listed");
        assert_eq!(listed.size, 4, "the listing reads the file's length");
        assert_eq!(
            listed.etag,
            ETag::from_content(b"cold"),
            "the listing's ETag is the content's"
        );

        // `stat` is the content ETag source: the plane's own stamp for the
        // key, independent of any row.
        let metadata = fs::metadata(&path).await.unwrap();
        let entry = content.stat(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(entry.len, 4);
        assert_eq!(
            entry.identity,
            fsutil::file_identity(&path, &metadata),
            "stat answers the ETag source's identity"
        );
        assert_eq!(
            walked(&content, &bucket, "", None, 10).await,
            vec![key.clone()],
            "a row-less key is still walked"
        );

        // The cold listing heals the row it served: the same key, the same
        // ETag, now readable through the metadata facade.
        let head = storage
            .head_object(&bucket, &key)
            .await
            .expect("the cold listing wrote the row it served");
        assert_eq!(head.etag, listed.etag, "the healed row carries that ETag");
    }

    /// Whether the store's staging area (`<state-dir>/tmp/`) holds no entry.
    async fn tmp_is_empty(tmp: &Path) -> bool {
        match fs::read_dir(tmp).await {
            Ok(mut entries) => entries.next_entry().await.ok().flatten().is_none(),
            // The area is created by the first stage; an absent one holds
            // nothing.
            Err(err) if err.kind() == ErrorKind::NotFound => true,
            Err(_) => false,
        }
    }

    /// The engine's content port (`for_engine`) rides the bucket mutation
    /// lock its **caller** holds instead of taking it again: the version
    /// engine's restore calls `rename` from inside the delete path's one
    /// critical section, and the per-key lock is not re-entrant, so taking
    /// it again there would be a deadlock — the exact rule
    /// `Publication::BytesUnderLock` carries for `publish`.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_non_owning_rename_rides_the_callers_bucket_lock() {
        use std::time::Duration;

        use tokio::time::timeout;

        let (_root, storage) = storage();
        let storage = Arc::new(storage);
        let content = FsContent::for_engine(Arc::clone(&storage));
        let bucket = bucket::name("engine-rename").unwrap();
        content.create_bucket(&bucket).await.unwrap();
        let from = object::key("a.bin").unwrap();
        let to = object::key("b.bin").unwrap();
        let from_key = ContentKey::Object(from.clone());
        // The whole sequence runs inside the caller's critical section, the
        // way the delete path's restore uses the port.
        let _guard = storage.lock_bucket_mutations(&bucket).await;
        let mut staged = content.stage(&bucket, &from_key).await.unwrap();
        staged.write(Bytes::from_static(b"moved")).await.unwrap();
        content.publish(&bucket, staged, &from_key).await.unwrap();
        let moved = timeout(Duration::from_secs(5), content.rename(&bucket, &from, &to)).await;
        assert!(
            moved.is_ok(),
            "a rename under a held bucket lock must not deadlock"
        );
        moved.unwrap().unwrap();
        assert_eq!(
            read(&content, &bucket, &ContentKey::Object(to.clone()), None).await,
            b"moved"
        );
        assert!(
            content.stat(&bucket, &from).await.unwrap().is_none(),
            "the source key holds no bytes after the move"
        );
    }

    /// A failed part publish leaves no temp behind. The stage creates its
    /// temp before any upload validation, so every refusal on the way to the
    /// part path — an id the layout rejects, or a rename that cannot land —
    /// must remove it: nothing may orphan a stage under `<state-dir>/tmp/`
    /// for the sweep.
    #[tokio::test]
    async fn a_failed_part_publish_leaves_no_temp() {
        use std::time::Duration;

        use tokio::time::timeout;

        let (_root, storage) = storage();
        let storage = Arc::new(storage);
        let content = storage.content();
        let bucket = bucket::name("conf-part-publish").unwrap();
        content.create_bucket(&bucket).await.unwrap();
        let tmp = storage.state_dir().join("tmp");

        // An id the part layout rejects (`NoSuchUpload`): validated BEFORE
        // the temp is taken out of the stage, so the dropped stage's own
        // guard removes it.
        let bogus = ContentKey::Part {
            upload_id: "../escape".to_string(),
            number: multipart::part_number(1).unwrap(),
        };
        let mut staged = content.stage(&bucket, &bogus).await.unwrap();
        staged.write(Bytes::from_static(b"x")).await.unwrap();
        let err = content.publish(&bucket, staged, &bogus).await.unwrap_err();
        assert!(
            matches!(&err, crate::Error::Storage(StorageError::NoSuchUpload(_))),
            "a non-UUID upload id is NoSuchUpload, got {err:?}"
        );
        // The guard's unlink runs off the async thread (item 7a): wait for
        // it, like the dropped-stage test in `backend::objects`.
        timeout(Duration::from_secs(5), async {
            loop {
                if tmp_is_empty(&tmp).await {
                    return;
                }
                yield_now().await;
            }
        })
        .await
        .expect("the refused part stage's temp removal");

        // A rename that cannot land (the target is an existing directory):
        // `finish` has already taken the temp out of the stage, so the
        // publish's own failure path must remove it.
        let upload_id = uuid::Uuid::new_v4().to_string();
        let part = ContentKey::Part {
            upload_id: upload_id.clone(),
            number: multipart::part_number(1).unwrap(),
        };
        let target = storage
            .multipart_store()
            .part_file(&bucket, &upload_id, 1)
            .unwrap();
        fs::create_dir_all(&target).await.unwrap();
        let mut staged = content.stage(&bucket, &part).await.unwrap();
        staged.write(Bytes::from_static(b"x")).await.unwrap();
        assert!(
            content.publish(&bucket, staged, &part).await.is_err(),
            "a part publish onto an occupied path must fail"
        );
        assert!(
            tmp_is_empty(&tmp).await,
            "a failed part rename must not orphan the stage's temp"
        );
    }
}
