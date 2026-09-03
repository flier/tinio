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
    fs::remove_file,
    io::{Error as IoError, ErrorKind, SeekFrom},
    mem, panic,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use bytes::{BufMut, BytesMut};
use futures::stream;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    runtime::Handle,
    task,
};

use super::{Error, FsStorage};
use crate::{
    _core::{
        BodyStream, ETag, bucket, checksum,
        multipart::ObjectPart,
        object::{self, Info, Tags},
        storage::{
            ByteRange, GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps, access_denied,
            no_such_bucket, no_such_key,
        },
        to_nanos,
    },
    database::{self, ObjectMetaTable, ObjectPartsTable},
    fsutil,
    write::{AtomicWriter, CHUNK_SIZE},
};

/// An empty body stream (a zero-byte object with no range).
fn empty_stream() -> BodyStream {
    Box::pin(stream::empty())
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
    /// The stage's tee digest (spec 2026-08-31): the validated checksum
    /// of the staged content, computed while the body streamed under the
    /// server's `checksum` slot — committed as the object's recorded
    /// checksum (`FULL_OBJECT` kind) with no re-hashing. `None` when the
    /// stage carried no tee slot.
    checksum: Option<checksum::Part>,
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

    /// The stage's tee digest, when the stage ran under a checksum slot
    /// (the digest cell is filled at stream end — the commit records it
    /// as the object's `FULL_OBJECT` checksum).
    pub(crate) fn checksum(&self) -> Option<&checksum::Part> {
        self.checksum.as_ref()
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
            if Handle::try_current().is_ok() {
                // Detached on purpose: the blocking task runs the unlink
                // off the request thread (dropping the handle detaches).
                // catch_unwind: a shutting-down runtime still reports a
                // current handle, but `spawn_blocking` then panics — a
                // staged body dropped in a cancelled request during
                // teardown must not panic inside `Drop`. The caught
                // panic loses only the best-effort unlink; the startup
                // sweep is the backstop.
                let _ = panic::catch_unwind(|| {
                    mem::drop(task::spawn_blocking(move || remove_file(&temp)));
                });
            } else {
                let _ = remove_file(&temp);
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
/// `File` + manual `spawn_blocking` + `Read::read_buf` (out of
/// scope).
async fn file_stream(file: fs::File, start: u64, end: u64) -> BodyStream {
    let mut file = file;
    if let Err(err) = file.seek(SeekFrom::Start(start)).await {
        return Box::pin(stream::once(async move { Err(err) }));
    }
    let remaining = end.saturating_sub(start) + 1;
    Box::pin(stream::try_unfold(
        (file, remaining, BytesMut::with_capacity(CHUNK_SIZE)),
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
                    && fs::symlink_metadata(self.root().join(&**name))
                        .await
                        .map(|m| fsutil::is_symlink_or_reparse(&m))
                        .unwrap_or(false)
                {
                    return Err(no_such_bucket(name).into());
                }
                return Err(err);
            }
        };
        match fs::metadata(&dir).await {
            Ok(metadata) if metadata.is_dir() => Ok(dir),
            Ok(_) => Err(no_such_bucket(name).into()),
            Err(err) if err.kind() == ErrorKind::NotFound => Err(no_such_bucket(name).into()),
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
            match fs::symlink_metadata(&current).await {
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
    ) -> Result<(PathBuf, fs::File, u64, SystemTime, u64), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Reads of reserved keys report NoSuchKey (FR-020).
        if key.is_reserved() {
            return Err(no_such_key(key).into());
        }
        let path = self.resolve_key(&bucket_dir, key).await?;
        // One policy open: nofollow when following is disabled (a swap
        // to a symlink between the resolve and the open is rejected, R3
        // — the ELOOP → PermissionDenied normalization included).
        let mut file = match fsutil::open_file(&path, self.follow_symlinks).await {
            Ok(file) => file,
            // Missing: the old path-stat's NotFound mapping moves to the
            // open (the merged read resolves the object in one step).
            Err(err) if err.kind() == ErrorKind::NotFound => {
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
            Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                let is_dir = fsutil::object_metadata(&path, self.follow_symlinks)
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
            Err(err) if err.kind() == ErrorKind::NotFound => {
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
        // path-based open; [`fsutil::file_identity_async`] bridges the
        // handle safely, so a replacement between two opens is
        // impossible by construction).
        let identity = fsutil::file_identity_async(&mut file, &metadata).await;
        Ok((path, file, size, mtime, identity))
    }

    /// The stat-only object existence gate of the metadata-only ops
    /// (`get_object_tags`/`list_object_parts` — the meta row supplies
    /// the data, the file only proves existence): the
    /// `resolve_object_file` gate minus the open. Same answers: a
    /// missing file / folder marker (directory) / reserved key →
    /// `NoSuchKey`; a symlink leaf under nofollow → `AccessDenied`
    /// (the policy open's ELOOP answer, R3 — the `object_metadata`
    /// lstat sees the link itself).
    pub(crate) async fn ensure_object_file(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(no_such_key(key).into());
        }
        let path = self.resolve_key(&bucket_dir, key).await?;
        let metadata = match fsutil::object_metadata(&path, self.follow_symlinks).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == ErrorKind::NotFound => return Err(no_such_key(key).into()),
            Err(err) if err.kind() == ErrorKind::PermissionDenied && !self.follow_symlinks => {
                return Err(access_denied(key).into());
            }
            Err(err) => return Err(err.into()),
        };
        if metadata.is_dir() {
            return Err(no_such_key(key).into());
        }
        if !self.follow_symlinks && metadata.file_type().is_symlink() {
            return Err(access_denied(key).into());
        }
        Ok(())
    }

    /// Resolve the object for a read — the shared head of
    /// `get_object`/`head_object`: the file resolution plus the ensured
    /// meta row (the ETag — and the tags/checksum elements the read
    /// paths serve — against the meta store; spec 2026-08-31).
    async fn resolve_object_info(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(fs::File, u64, SystemTime, database::StoredMeta), Error> {
        let (path, file, size, mtime, identity) = self.resolve_object_file(bucket, key).await?;
        let (row, _) = self
            .meta_store
            .ensure_row(
                bucket,
                key,
                &path,
                size,
                mtime,
                identity,
                self.follow_symlinks,
            )
            .await?;
        Ok((file, size, mtime, row))
    }

    /// The unix fast path of the copy primitive: the kernel's
    /// `copy_file_range` moves the bytes (zero userspace buffering), the
    /// source's single-form ETag is reused for a full copy when the file
    /// is provably unchanged (AWS semantics — the content MD5 of a full
    /// copy is the source's), and the shared commit publishes the
    /// destination under the mutation lock with the caller's tags and
    /// recorded checksum.
    #[cfg(unix)]
    #[allow(clippy::too_many_arguments)]
    async fn copy_object_fast(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        tags: object::Tags,
        checksum: Option<checksum::Recorded>,
    ) -> Result<Info, Error> {
        // Folder-marker destination: the sentinel commit (mirrors
        // `stage_body`'s marker handling) — no bytes are copied.
        if dst_key.is_folder_marker() {
            return self
                .commit_staged(
                    dst_bucket,
                    dst_key,
                    StagedBody {
                        temp: None,
                        etag: ETag::EMPTY,
                        checksum: None,
                    },
                    tags,
                    checksum,
                )
                .await;
        }
        let (path, file, size, mtime, identity) =
            self.resolve_object_file(src_bucket, src_key).await?;
        // A copy may reuse the source's stored ETag when it still
        // matches the open file — reused only in its single form: a
        // composed source's copy is a fresh single-part object whose
        // canonical ETag is the content MD5 of the copied bytes. The
        // stored-ETag read and the copy are independent (the copy is
        // needed regardless of the ETag outcome) — run them
        // concurrently.
        let (stored, staged) = tokio::join!(
            async {
                self.meta_store
                    .etag_matching(src_bucket, src_key, size, mtime, identity)
                    .await
            },
            async {
                let std_file = file.into_std().await;
                self.writer.stage_copy(std_file, 0, size).await
            },
        );
        let stored = stored?;
        let (temp, staged_etag) = staged?;
        // Torn-copy guard: the reuse is valid only when the source is
        // byte-identical to the pre-copy stat — a mid-copy change makes
        // the staged bytes self-consistent with their own hash, never
        // with the source's stale ETag.
        let stable = (matches!(stored, Some(ETag::Single(_))) || checksum.is_some())
            && fs::metadata(&path).await.is_ok_and(|now| {
                now.len() == size
                    && now.modified().ok() == Some(mtime)
                    && fsutil::file_identity(&path, &now) == identity
            });
        let etag = match stored {
            Some(etag @ ETag::Single(_)) if stable => etag,
            _ => staged_etag,
        };
        // The carried checksum rides only while the guard holds: it
        // describes the source as the interface headed it, and a source
        // that changed mid-copy makes it stale — the staged bytes are
        // self-consistent with their own hash (the ETag above), never
        // with a digest of other content. Recording none (the read paths
        // self-heal a missing element) beats recording a wrong digest.
        let checksum = if stable { checksum } else { None };
        self.commit_staged(
            dst_bucket,
            dst_key,
            StagedBody {
                temp: Some(temp),
                etag,
                checksum: None,
            },
            tags,
            checksum,
        )
        .await
    }

    /// The shared commit tail of the object write paths (`commit_object`
    /// and the copy primitives): atomically publish a staged body onto
    /// `key` under the mutation lock (the same re-checks and guards as a
    /// plain commit), then write the object's `OBJECT_META` entry — the
    /// interface-validated `tags` and the recorded `checksum` (the
    /// FULL_OBJECT tee digest of a plain commit, or the copy's carried
    /// value — kind included) ride in the same row — and remove any
    /// stale `OBJECT_PARTS` rows of the key in ONE write transaction (a
    /// fresh object is single-part: overwriting a previously
    /// multipart-completed object must not leave its parts behind).
    /// Returns the committed object metadata.
    async fn commit_staged(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody,
        tags: object::Tags,
        checksum: Option<checksum::Recorded>,
    ) -> Result<Info, Error> {
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
        // too. Markers hold no row: the tags/checksum params are
        // accept-and-dropped here.
        if key.is_folder_marker() {
            let _guard = self.lock_bucket_mutations(bucket).await;
            let bucket_dir = self.ensure_bucket(bucket).await?;
            let target = self.resolve_key(&bucket_dir, key).await?;
            fs::create_dir_all(&target).await?;
            let mtime = fs::metadata(&target).await?.modified()?;
            return Ok(Info {
                key: key.clone(),
                size: 0,
                last_modified: mtime,
                etag,
                tags: Tags::empty(),
                checksum: None,
            });
        }
        // A real object always arrives with a staged temp (the marker
        // branch above consumed the sentinel).
        let Some(temp) = staged.into_temp() else {
            return Err(IoError::other("staged body without a temp file").into());
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
            let metadata = fs::metadata(&target).await?;
            // The object is committed — a meta-write failure (full
            // state dir) must not fail the PUT: the entry is recomputed
            // from the content on the next read (self-healing, FR-022).
            // The stale-part removal rides in the same transaction.
            let identity = fsutil::file_identity(&target, &metadata);
            let mtime = metadata.modified()?;
            self.record_committed(
                bucket,
                key,
                &etag,
                metadata.len(),
                mtime,
                identity,
                &tags,
                checksum.as_ref(),
            )
            .await;
            Ok::<_, Error>((metadata.len(), mtime))
        }
        .await;
        if result.is_err() {
            let _ = fs::remove_file(&temp).await;
        }
        let (size, mtime) = result?;
        Ok(Info {
            key: key.clone(),
            size,
            last_modified: mtime,
            etag,
            tags,
            checksum,
        })
    }

    /// The committed object's row write: the `OBJECT_META` entry (tags +
    /// checksum elements) and the removal of any stale `OBJECT_PARTS`
    /// rows of the key in ONE write transaction. Best-effort — a failed
    /// meta write must not fail the PUT (the entry self-heals on the
    /// next read, FR-022); the failure is warned.
    #[allow(clippy::too_many_arguments)]
    async fn record_committed(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
        identity: u64,
        tags: &object::Tags,
        checksum: Option<&checksum::Recorded>,
    ) {
        let bucket = bucket.clone();
        let key = key.clone();
        let etag = etag.clone();
        let tags = tags.clone();
        let checksum = checksum.cloned();
        if let Err(err) = self
            .handle
            .write(move |txn| {
                ObjectMetaTable::open(txn)?.put(
                    &bucket,
                    &key,
                    &database::StoredMeta {
                        etag: etag.clone(),
                        size,
                        mtime: to_nanos(mtime),
                        file_identity: identity,
                        tags: tags.clone(),
                        checksum: checksum.clone(),
                    },
                )?;
                ObjectPartsTable::open(txn)?.remove_key(&bucket, &key)?;
                Ok(())
            })
            .await
        {
            tracing::warn!(error = %err, "meta entry not persisted after commit");
        }
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
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<StagedBody, Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Symlink policy applies to markers too — a PUT `sub/dir/` whose
        // parent is a link must not create a directory outside the root.
        // The resolution is a validation gate here (commit re-resolves).
        self.resolve_key(&bucket_dir, key).await?;
        // Folder markers are never objects (s3-surface.md): no body is
        // staged — the commit creates the directory. The sentinel
        // carries the marker's empty-content ETag (and no tee digest —
        // no bytes streamed).
        if key.is_folder_marker() {
            return Ok(StagedBody {
                temp: None,
                etag: ETag::EMPTY,
                checksum: None,
            });
        }
        // `checksum` is the server's tee slot (spec 2026-08-31 — the
        // `upload_part` pattern): the interface wraps the body when the
        // client sent a single `x-amz-checksum-*` header, the digest is
        // computed while the body streams, a mismatch fails the staging
        // as the tee's stream error (propagated here like any body
        // failure — the fs adds no error of its own), and the validated
        // digest rides into the commit. Absent, no digest is computed.
        let (temp, etag) = self.writer.stage(body, checksum.as_deref()).await?;
        let computed = checksum.as_ref().and_then(|c| c.digest.get()).cloned();
        Ok(StagedBody {
            temp: Some(temp),
            etag,
            checksum: computed,
        })
    }

    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody,
        tags: object::Tags,
    ) -> Result<Info, Error> {
        // The stage's tee digest records as the object's FULL_OBJECT
        // checksum (the kind is fixed by the write path — a plain PUT's
        // digest is over the whole content).
        let checksum = staged.checksum().cloned().map(|part| checksum::Recorded {
            part,
            kind: checksum::Type::FullObject,
        });
        self.commit_staged(bucket, key, staged, tags, checksum)
            .await
    }

    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        tags: object::Tags,
        checksum: Option<checksum::Recorded>,
    ) -> Result<Info, Error> {
        #[cfg(unix)]
        {
            self.copy_object_fast(src_bucket, src_key, dst_bucket, dst_key, tags, checksum)
                .await
        }
        #[cfg(not(unix))]
        {
            // No kernel copy primitive on Windows — stream the source
            // through the body contract (get → stage → commit). The
            // commit tail is the shared `commit_staged` so the copy's
            // `checksum` (kind included) is recorded and the destination
            // never inherits the source's retained parts.
            let get = self.get_object(src_bucket, src_key, None).await?;
            let staged = self.stage_body(dst_bucket, dst_key, get.body, None).await?;
            self.commit_staged(dst_bucket, dst_key, staged, tags, checksum)
                .await
        }
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        let (file, size, mtime, row) = self.resolve_object_info(bucket, key).await?;
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
                etag: row.etag,
                tags: row.tags,
                checksum: row.checksum,
            },
            body,
            served_range,
        })
    }

    async fn head_object(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Info, Error> {
        // The open is the metadata source too (the same merge as
        // get_object) — the handle is dropped right away.
        let (_file, size, mtime, row) = self.resolve_object_info(bucket, key).await?;
        Ok(Info {
            key: key.clone(),
            size,
            last_modified: mtime,
            etag: row.etag,
            tags: row.tags,
            checksum: row.checksum,
        })
    }

    async fn rename_object(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<Info, Error> {
        // A rename is a bucket-mutating pair (file move + state move):
        // one critical section against `delete_bucket` — a concurrent
        // delete must not remove a bucket a rename just wrote into. The
        // file move and the row migration are the same rename semantics
        // as every other write: file first (atomic), then the single
        // all-or-nothing state transaction (a crash in between self-heals
        // — the destination's row is recomputed on the next read).
        if src == dst {
            // Degenerate — the interface answers 412 before calling; a
            // backend-level guard keeps the move idempotent.
            return self.head_object(bucket, src).await;
        }
        // A reserved destination is never a legal rename target (FR-020
        // — a write through the reserved segment, refused like every
        // other write path); a marker destination cannot hold an object
        // (mirror `complete_multipart_upload`'s refusal). A reserved or
        // marker source is never an object — `NoSuchKey` like head.
        if dst.is_reserved() {
            return Err(access_denied(dst).into());
        }
        if dst.is_folder_marker() {
            return Err(crate::_core::storage::invalid_key(dst.to_string()).into());
        }
        let _guard = self.lock_bucket_mutations(bucket).await;
        // The source must be a live object (a marker source is never an
        // object — `NoSuchKey` like head).
        let (src_path, _file, size, mtime, identity) =
            self.resolve_object_file(bucket, src).await?;
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let dst_path = self.resolve_key(&bucket_dir, dst).await?;
        // The source's row is ensured before the move (a hand-dropped
        // source gets its etag healed — the moved row must be complete).
        let (ensured, _) = self
            .meta_store
            .ensure_row(
                bucket,
                src,
                &src_path,
                size,
                mtime,
                identity,
                self.follow_symlinks,
            )
            .await?;
        // The file move: same-volume (one bucket), atomic, overwriting an
        // existing destination. New ancestor directories are synced like
        // the first commit into a new prefix (F03).
        let created_parent = match dst_path.parent() {
            Some(parent) => fsutil::ensure_dir(parent).await?,
            None => false,
        };
        match fs::rename(&src_path, &dst_path).await {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Err(no_such_key(src).into());
            }
            Err(err) => return Err(err.into()),
        }
        if let Some(parent) = dst_path.parent() {
            if created_parent {
                AtomicWriter::sync_ancestor_chain(parent, Some(&bucket_dir)).await;
            } else {
                AtomicWriter::sync_dir_warned(parent).await;
            }
        }
        // The state move in ONE transaction: the src `OBJECT_META` row →
        // dst (metadata — mtime, tags, checksum — moves with the record,
        // a rename is not a fresh object), the src `OBJECT_PARTS` rows →
        // dst, and the dst's own stale parts rows removed (an overwritten
        // destination's part list dies). A missing src row (a concurrent
        // delete_object removed it between the ensure and here) leaves
        // the dst file to self-heal — nothing to migrate.
        let migrated: Option<(ETag, Tags, Option<checksum::Recorded>)> = {
            let bucket = bucket.clone();
            let src = src.clone();
            let dst = dst.clone();
            self.handle
                .write(move |txn| {
                    // The meta-row half: read the src row, remove it, and
                    // re-insert it under dst (metadata — mtime, tags,
                    // checksum — moves with the record). One handle per
                    // table per transaction (redb refuses a second open of
                    // one table in a transaction) — the two halves run in
                    // separate scopes.
                    let row = {
                        let mut meta = ObjectMetaTable::open(txn)?;
                        let Some(row) = meta.get(&bucket, &src)? else {
                            return Ok(None);
                        };
                        meta.remove(&bucket, &src)?;
                        meta.put(&bucket, &dst, &row)?;
                        row
                    };
                    // The parts rows migrate with the record: list the
                    // src's rows, clear the dst's stale rows, re-key
                    // under dst, drop the src's (one table handle serves
                    // the read and the writes).
                    let mut parts = ObjectPartsTable::open(txn)?;
                    let rows = parts.list(&bucket, &src)?;
                    // Both range deletes precede the inserts — a redb
                    // 4.2.0 debug build asserts when an insert precedes
                    // another key's range delete in one transaction.
                    parts.remove_key(&bucket, &dst)?;
                    parts.remove_key(&bucket, &src)?;
                    for (n, part_size, algorithm, value) in rows {
                        parts.put(&bucket, &dst, n, part_size, &algorithm, &value)?;
                    }
                    Ok(Some((row.etag, row.tags, row.checksum)))
                })
                .await
                .map_err(Error::from)?
        };
        let (etag, tags, checksum) = match migrated {
            Some((etag, tags, checksum)) => (etag, tags, checksum),
            // No row to migrate (a concurrent delete): the ensured etag
            // describes the moved file; the row self-heals on the next
            // read.
            None => (ensured.etag, ensured.tags, ensured.checksum),
        };
        Ok(Info {
            key: dst.clone(),
            size,
            last_modified: mtime,
            etag,
            tags,
            checksum,
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
            match fs::remove_dir(&path).await {
                Ok(()) => {}
                Err(err)
                    if err.kind() == ErrorKind::NotFound
                        || err.kind() == ErrorKind::DirectoryNotEmpty
                        || err.kind() == ErrorKind::NotADirectory =>
                {
                    // Idempotent / non-empty / wrong type: nothing to do.
                }
                Err(err) => return Err(err.into()),
            }
            return Ok(());
        }
        // With following enabled, a leaf link resolves to the object it
        // aliases — DELETE removes the target (the bytes get/head
        // serve), not the link (a dangling link is removed as itself:
        // the object is already gone). Mirrors put/get, which address
        // the target.
        let remove_path = match fs::symlink_metadata(&path).await {
            Ok(m) if self.follow_symlinks && fsutil::is_symlink_or_reparse(&m) => {
                fs::canonicalize(&path).await.unwrap_or(path.clone())
            }
            _ => path.clone(),
        };
        match fs::remove_file(&remove_path).await {
            Ok(()) => {}
            // Missing or a directory (DELETE of a marker key without the
            // trailing slash) — DELETE is idempotent, always 204.
            Err(err)
                if err.kind() == ErrorKind::NotFound || err.kind() == ErrorKind::IsADirectory =>
            {
                // Nothing to do.
            }
            // Windows reports a directory as PermissionDenied: the
            // wrong-type no-op, not an I/O failure.
            Err(err)
                if err.kind() == ErrorKind::PermissionDenied
                    && fs::metadata(&path)
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
            match fs::remove_dir(&dir).await {
                Ok(()) => parent = dir.parent().map(Path::to_path_buf),
                Err(_) => break, // non-empty or gone: stop pruning
            }
        }
        Ok(())
    }

    async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        // Existence is the object file (folder markers, reserved keys,
        // and missing files answer `NoSuchKey` — mirroring head_object);
        // the tags come from the stored row, empty when the file has no
        // row yet (a hand-dropped object has never been tagged).
        self.ensure_object_file(bucket, key).await?;
        self.meta_store.tags(bucket, key).await
    }

    async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), Error> {
        // Existence is the object file (`NoSuchKey` when missing —
        // mirroring head_object). The row update preserves the row's
        // other elements; a file without a row (a hand-dropped object)
        // is healed and tagged in one transaction (`ensure_tags`).
        let (path, _file, _, _, _) = self.resolve_object_file(bucket, key).await?;
        self.meta_store
            .ensure_tags(bucket, key, &path, self.follow_symlinks, tags)
            .await
            .map_err(|err| match err {
                Error::Io(err) if err.kind() == ErrorKind::NotFound => no_such_key(key).into(),
                err => err,
            })?;
        Ok(())
    }

    async fn delete_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(), Error> {
        // Idempotent like `delete_object`: only the bucket must exist
        // (`NoSuchBucket` when missing) and a reserved key is refused the
        // same way (FR-020 — mirroring `delete_object`); a missing
        // object — file, marker, or row — is a no-op.
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let _ = self.resolve_key(&bucket_dir, key).await?;
        self.meta_store.clear_tags(bucket, key).await
    }

    async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, Error> {
        // Existence is the object file (T2-B ruling: a missing object
        // answers `NoSuchKey`, mirroring `get_object_tags`); the
        // retained rows of a multipart-completed object are served in
        // part-number order — empty for an object that was never
        // multipart-completed (a plain put or copy has no parts).
        self.ensure_object_file(bucket, key).await?;
        let rows = self
            .handle
            .read(|txn| ObjectPartsTable::open_readonly(txn)?.list(bucket, key))
            .map_err(Error::from)?;
        let parts = rows
            .into_iter()
            .map(|(part_number, size, algorithm, value)| ObjectPart {
                part_number: part_number.into(),
                size,
                // A domain-invalid checksum row self-heals: the part is
                // served without a checksum (F07 — the `""` algorithm of
                // a checksum-less part parses to `None` the same way).
                checksum: checksum::Part::from_wire_opt(&algorithm, value),
            })
            .collect();
        Ok(parts)
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        self.listing.list(&params).await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    #[cfg(windows)]
    use std::os::windows::fs::symlink_dir;
    use std::{io::Error as IoError, time::Duration};

    use bytes::Bytes;
    use futures::StreamExt;
    use md5::{Digest, Md5};
    use tokio::{fs, time::timeout};

    use super::*;
    use crate::{
        _core::{
            object,
            storage::{BucketOps, Error as StorageError},
        },
        _util::testing::{assert_conformance, body, complete_single_part, etag, read_body},
        testutil::{checksum_tee, fs_options, md5_wire, storage},
    };

    #[tokio::test]
    async fn conformance_green() {
        let (_root, storage) = storage();
        assert_conformance(&storage).await;
    }

    #[tokio::test]
    async fn put_get_head_delete_round_trip() {
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("dir/a.txt").unwrap();

        let put = storage.put_object(&b, &k, body(b"hello")).await.unwrap();
        assert_eq!(put.etag, etag("5d41402abc4b2a76b9719d911017c592"));

        // The file physically appears in the directory.
        assert_eq!(
            fs::read(root.path().join("data/dir/a.txt")).await.unwrap(),
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
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn copy_object_fast_path_preserves_content_and_etag() {
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
        let put = storage
            .copy_object(&b, &src, &b2, &dst, object::Tags::empty(), None)
            .await
            .unwrap();
        assert_eq!(put.etag, ETag::from_content(b"cross-bucket copy"));
        let get = storage.get_object(&b2, &dst, None).await.unwrap();
        assert_eq!(read_body(get.body).await.unwrap(), b"cross-bucket copy");
        assert_eq!(get.info.etag, put.etag);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn copy_of_a_multipart_source_yields_the_content_md5() {
        // The fast path reuses the source ETag only in its SINGLE form:
        // a composed source's copy is a fresh single-part object whose
        // canonical ETag is the content MD5 of the copied bytes — never
        // the stale `MD5-of-MD5s-N` form (the old stream path's re-hash
        // behavior, preserved).

        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("big.bin").unwrap();
        let dst = object::key("copy.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        // The non-final part must meet the backend's 5 MiB minimum at
        // complete (the shared `check_part_minimum`); only the final
        // part may be small.
        let min = crate::_core::multipart::MIN_PART_BYTES as usize;
        let mut concat = vec![b'a'; min];
        concat.extend_from_slice(b"part-two-");
        let p1 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                1.into(),
                body(vec![b'a'; min]),
                None,
            )
            .await
            .unwrap();
        let p2 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                2.into(),
                body(b"part-two-"),
                None,
            )
            .await
            .unwrap();
        let completed = [
            crate::_core::CompletedPart {
                part_number: p1.part_number,
                etag: p1.etag,
            },
            crate::_core::CompletedPart {
                part_number: p2.part_number,
                etag: p2.etag,
            },
        ];
        let info = storage
            .complete_multipart_upload(&b, &k, &upload.upload_id, &completed, None)
            .await
            .unwrap();
        assert!(matches!(info.etag, ETag::Composed(_, 2)));
        let put = storage
            .copy_object(&b, &k, &b, &dst, object::Tags::empty(), None)
            .await
            .unwrap();
        assert_eq!(put.etag, ETag::from_content(&concat));
        let head = storage.head_object(&b, &dst).await.unwrap();
        assert_eq!(head.etag, ETag::from_content(&concat));
    }

    #[tokio::test]
    async fn copy_of_a_missing_source_is_no_such_key() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let missing = object::key("ghost.bin").unwrap();
        let dst = object::key("dst.bin").unwrap();
        let err = storage
            .copy_object(&b, &missing, &b, &dst, object::Tags::empty(), None)
            .await
            .unwrap_err();
        assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
    }

    #[tokio::test]
    async fn missing_bucket_is_no_such_bucket() {
        let (_root, storage) = storage();
        let ghost = bucket::name("ghost").unwrap();
        let err: StorageError = storage
            .put_object(&ghost, &"a".into(), body(b"x"))
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::NoSuchBucket(_)));
    }

    #[tokio::test]
    async fn get_ranges() {
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
    }

    #[tokio::test]
    async fn get_stream_chunk_sequence() {
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
    }

    #[tokio::test]
    async fn get_stream_stops_at_the_range_end() {
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
    }

    #[tokio::test]
    async fn streamed_chunks_own_their_memory() {
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
    }

    #[tokio::test]
    async fn delete_of_a_directory_without_slash_is_204() {
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
    }

    #[tokio::test]
    async fn folder_marker_semantics() {
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
    }

    #[tokio::test]
    async fn reserved_keys_denied() {
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
    }

    #[tokio::test]
    async fn reserved_key_refused_with_follow_enabled() {
        // FR-020 in BOTH follow modes (P5): the follow-enabled resolve
        // used to skip the lexical mapping entirely — a `.tinio` PUT
        // slipped through and wrote `<bucket>/.tinio/x`. The lexical
        // validation now runs before the follow shortcut, refusing
        // AccessDenied like every other op (contract doc: "refuse writes
        // whose key is reserved ... with AccessDenied").
        use crate::FsOptions;
        let (root, _) = storage();
        let b = bucket::name("data").unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
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
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn lexical_validation_precedes_the_symlink_walk() {
        // P5 ordering pin: the pure lexical validation runs before the
        // symlink walk, so a key the mapping refuses (Windows-invalid
        // chars) answers InvalidKey even when a path component is a link
        // — the documented order ("rejected before any filesystem
        // access", path.rs), where the walk-first code answered
        // AccessDenied (the walk syscalled first).
        use crate::FsOptions;
        let (root, _) = storage();
        let b = bucket::name("data").unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink_dir(outside.path(), root.path().join("data/a")).unwrap();
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
    }

    #[tokio::test]
    async fn head_of_folder_marker_is_no_such_key() {
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
    }

    #[tokio::test]
    async fn commit_after_bucket_deleted_is_no_such_bucket() {
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("a.txt").unwrap();
        let staged = storage
            .stage_body(&b, &k, body(b"hello"), None)
            .await
            .unwrap();
        storage.delete_bucket(&b).await.unwrap();
        let err: StorageError = storage
            .commit_object(&b, &k, staged, object::Tags::empty())
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::NoSuchBucket(_)), "{err:?}");
        assert!(!root.path().join("data").exists());
        let tmp = root.path().join(".tinio/tmp");
        let mut entries = fs::read_dir(&tmp).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "no temp files may remain"
        );
    }

    #[tokio::test]
    async fn dropped_staged_body_leaves_no_temp() {
        // A rejected conditional PUT (412) drops the staged body without
        // a commit — the full body must not stay in `tmp/` for the sweep.
        use crate::_core::storage::ObjectOps;
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("big.bin").unwrap();
        let staged = storage
            .stage_body(&b, &k, body(b"x".repeat(1024)), None)
            .await
            .unwrap();
        drop(staged); // the server's precondition-failure path
        // The removal is async (item 7a — the unlink runs on the
        // blocking pool); wait for it.
        let tmp = root.path().join(".tinio/tmp");
        timeout(Duration::from_secs(5), async {
            loop {
                let mut entries = fs::read_dir(&tmp).await.unwrap();
                if entries.next_entry().await.unwrap().is_none() {
                    return;
                }
                task::yield_now().await;
            }
        })
        .await
        .expect("the dropped staged body's temp removal");
    }

    #[tokio::test]
    async fn out_of_band_change_served_immediately() {
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        // Hand-dropped file (SC-006).
        fs::write(root.path().join("data/dropped.txt"), b"out-of-band")
            .await
            .unwrap();
        let k = object::key("dropped.txt").unwrap();
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(head.size, 11);
        assert_eq!(head.etag, etag(&hex::encode(Md5::digest(b"out-of-band"))));
        let get = storage.get_object(&b, &k, None).await.unwrap();
        assert_eq!(read_body(get.body).await.unwrap(), b"out-of-band");
    }

    #[tokio::test]
    async fn interrupted_upload_leaves_no_object() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("partial").unwrap();
        let stream = stream::iter(vec![
            Ok(Bytes::from_static(b"data")),
            Err(IoError::other("boom")),
        ]);
        let err = storage
            .put_object(&b, &k, Box::pin(stream))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)));
        let err = storage.head_object(&b, &k).await.unwrap_err();
        assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_rejected_when_disabled() {
        use crate::FsOptions;
        let (root, _) = storage();
        let b = bucket::name("data").unwrap();
        fs::create_dir(root.path().join("data")).await.unwrap();
        fs::write(root.path().join("outside.txt"), b"secret")
            .await
            .unwrap();
        symlink(
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

        // With following enabled, the link is served.
        drop(storage);
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                ..fs_options()
            },
        )
        .unwrap();
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(head.size, 6);
        // ... and DELETE resolves through it (the follow policy).
        storage.delete_object(&b, &k).await.unwrap();
        assert!(!root.path().join("outside.txt").exists());
    }

    #[tokio::test]
    async fn commit_object_writes_to_bucket_target_at_rename() {
        // Same race as multipart complete: a followed bucket symlink
        // retargeted between commit's resolve and the rename must not
        // leave the object on the stale path (stage_body is a validation
        // gate; commit re-resolves under the mutation lock).
        use crate::{
            FsOptions,
            testutil::{link_dir, retarget_bucket_during_commit, wait_for_lock_waiter},
        };

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
        let staged = storage
            .stage_body(&b, &k, body(b"hello"), None)
            .await
            .unwrap();
        let storage2 = storage.clone();
        let b2 = b.clone();
        let k2 = k.clone();
        retarget_bucket_during_commit(
            &storage,
            &b,
            &link,
            target_b.path(),
            wait_for_lock_waiter(),
            move || async move {
                storage2
                    .commit_object(&b2, &k2, staged, object::Tags::empty())
                    .await
                    .unwrap()
            },
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
    }

    // --- tags + recorded checksums + retained parts (spec 2026-08-31) ---

    #[tokio::test]
    async fn fs_object_tags_round_trip_and_replace() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("t.txt").unwrap();
        storage.put_object(&b, &k, body(b"x")).await.unwrap();
        assert!(
            storage.get_object_tags(&b, &k).await.unwrap().is_empty(),
            "an untagged object answers the empty set"
        );

        // Put → Get round-trip (replace-all, no merge).
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        storage.put_object_tags(&b, &k, &tags).await.unwrap();
        assert_eq!(storage.get_object_tags(&b, &k).await.unwrap(), tags);
        let replaced = object::Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
        storage.put_object_tags(&b, &k, &replaced).await.unwrap();
        assert_eq!(storage.get_object_tags(&b, &k).await.unwrap(), replaced);
        // head/get carry the same tags (Info.tags — one source of truth).
        assert_eq!(storage.head_object(&b, &k).await.unwrap().tags, replaced);

        // Delete clears; the row's other elements survive the tag write.
        storage.delete_object_tags(&b, &k).await.unwrap();
        assert!(storage.get_object_tags(&b, &k).await.unwrap().is_empty());
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(head.size, 1, "the etag/size row must survive tag ops");
        assert!(head.tags.is_empty());

        // Missing object: get/put → NoSuchKey, delete succeeds.
        let missing = object::key("missing.txt").unwrap();
        let err: StorageError = storage
            .get_object_tags(&b, &missing)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::NoSuchKey(_)));
        let err: StorageError = storage
            .put_object_tags(&b, &missing, &tags)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::NoSuchKey(_)));
        storage.delete_object_tags(&b, &missing).await.unwrap();
        // A missing bucket answers NoSuchBucket (get and delete alike).
        let ghost = bucket::name("ghost").unwrap();
        let err: StorageError = storage
            .get_object_tags(&ghost, &k)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::NoSuchBucket(_)));
        let err: StorageError = storage
            .delete_object_tags(&ghost, &k)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::NoSuchBucket(_)));
    }

    #[tokio::test]
    async fn fs_commit_and_copy_carry_tags() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let a = object::key("a.txt").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        // The commit records the tags with the object — no post-commit
        // tag window.
        let staged = storage.stage_body(&b, &a, body(b"hi"), None).await.unwrap();
        let info = storage
            .commit_object(&b, &a, staged, tags.clone())
            .await
            .unwrap();
        assert_eq!(info.etag, ETag::from_content(b"hi"));
        assert_eq!(info.tags, tags);
        assert_eq!(storage.head_object(&b, &a).await.unwrap().tags, tags);

        // A copy is a fresh object whose tags are the caller's — never
        // the source's.
        let dst = object::key("b.txt").unwrap();
        let copy_tags = object::Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
        storage
            .copy_object(&b, &a, &b, &dst, copy_tags.clone(), None)
            .await
            .unwrap();
        assert_eq!(storage.get_object_tags(&b, &dst).await.unwrap(), copy_tags);
        assert_eq!(storage.get_object_tags(&b, &a).await.unwrap(), tags);
    }

    #[tokio::test]
    async fn fs_commit_records_the_stage_tee_checksum() {
        // spec 2026-08-31: a plain PUT under the checksum toggle records
        // the tee's validated digest as the object's FULL_OBJECT checksum
        // — the backend never re-hashes.
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("c.txt").unwrap();
        let staged = storage
            .stage_body(
                &b,
                &k,
                body(b"hello"),
                Some(checksum_tee(checksum::Algorithm::Crc32, "NhCmhg==")),
            )
            .await
            .unwrap();
        storage
            .commit_object(&b, &k, staged, object::Tags::empty())
            .await
            .unwrap();
        let head = storage.head_object(&b, &k).await.unwrap();
        let recorded = head.checksum.expect("the tee digest must be recorded");
        assert_eq!(recorded.part.algorithm, checksum::Algorithm::Crc32);
        assert_eq!(recorded.part.value.0, "NhCmhg==");
        assert_eq!(recorded.kind, checksum::Type::FullObject);
        // A put without a tee records none.
        storage
            .put_object(&b, &object::key("plain.txt").unwrap(), body(b"x"))
            .await
            .unwrap();
        let head = storage
            .head_object(&b, &object::key("plain.txt").unwrap())
            .await
            .unwrap();
        assert!(head.checksum.is_none());
    }

    #[tokio::test]
    async fn fs_content_recompute_preserves_tags_and_checksum() {
        // The row's tags/checksum elements are NOT recomputable from the
        // object file (spec 2026-08-31): a content-derived rewrite — an
        // out-of-band file change forces an ETag recompute on the next
        // read — carries them over. Only an API write clears them.
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("a.txt").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        let staged = storage
            .stage_body(
                &b,
                &k,
                body(b"hello"),
                Some(checksum_tee(checksum::Algorithm::Md5, &md5_wire(b"hello"))),
            )
            .await
            .unwrap();
        storage
            .commit_object(&b, &k, staged, tags.clone())
            .await
            .unwrap();

        // Out-of-band in-place rewrite (same size — only the identity
        // gate and mtime detect it). The sleep lands the rewrite in a
        // later Windows FILETIME tick (~16 ms granularity).
        std::thread::sleep(std::time::Duration::from_millis(30));
        fs::write(root.path().join("data/a.txt"), b"world")
            .await
            .unwrap();
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(
            head.etag,
            ETag::from_content(b"world"),
            "the etag recomputes"
        );
        assert_eq!(
            head.tags, tags,
            "the API-written tags survive the recompute"
        );
        assert_eq!(
            head.checksum.as_ref().map(|c| c.part.value.0.as_str()),
            Some(md5_wire(b"hello").as_str()),
            "the recorded checksum survives the recompute"
        );
    }

    #[tokio::test]
    async fn fs_object_parts_lifecycle() {
        // The OBJECT_PARTS lifecycle (spec 2026-08-31): an overwrite via
        // commit removes the rows, delete removes them, rename migrates
        // them with the record, and copy never inherits them.
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("mp.bin").unwrap();
        complete_single_part(&storage, &b, &k).await;
        assert_eq!(storage.list_object_parts(&b, &k).await.unwrap().len(), 1);

        // (a) An overwriting commit is a fresh single-part object: its
        // parts rows are gone.
        let staged = storage
            .stage_body(&b, &k, body(b"plain"), None)
            .await
            .unwrap();
        storage
            .commit_object(&b, &k, staged, object::Tags::empty())
            .await
            .unwrap();
        assert!(
            storage.list_object_parts(&b, &k).await.unwrap().is_empty(),
            "an overwrite must not leave the completed object's parts"
        );

        // (c) rename migrates the parts rows with the record.
        complete_single_part(&storage, &b, &k).await;
        let moved = object::key("moved.bin").unwrap();
        storage.rename_object(&b, &k, &moved).await.unwrap();
        assert!(
            root.path().join("data/moved.bin").exists(),
            "the file moves"
        );
        assert!(
            !root.path().join("data/mp.bin").exists(),
            "the source file is gone"
        );
        assert_eq!(
            storage.list_object_parts(&b, &moved).await.unwrap().len(),
            1
        );
        let err: StorageError = storage.list_object_parts(&b, &k).await.unwrap_err().into();
        assert!(matches!(err, StorageError::NoSuchKey(_)));
        // A rename over an existing destination replaces it (the dst's
        // own stale rows die).
        let dst = object::key("dst.bin").unwrap();
        complete_single_part(&storage, &b, &dst).await;
        storage.rename_object(&b, &moved, &dst).await.unwrap();
        assert_eq!(storage.list_object_parts(&b, &dst).await.unwrap().len(), 1);

        // (d) copy_object never inherits the source's parts.
        let copy = object::key("copy.bin").unwrap();
        storage
            .copy_object(&b, &dst, &b, &copy, object::Tags::empty(), None)
            .await
            .unwrap();
        assert!(
            storage
                .list_object_parts(&b, &copy)
                .await
                .unwrap()
                .is_empty(),
            "a copy is single-part: the source's rows must not follow"
        );

        // (b) delete removes the rows — proved straight in the database
        // (the object is gone, so the parts list answers NoSuchKey).
        storage.delete_object(&b, &dst).await.unwrap();
        let err: StorageError = storage
            .list_object_parts(&b, &dst)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::NoSuchKey(_)));
        drop(storage); // release the redb file lock
        let db = crate::database::open(&root.path().join(".tinio"))
            .unwrap()
            .db;
        let txn = redb::ReadableDatabase::begin_read(&db).unwrap();
        let parts = crate::database::ObjectPartsTable::open_readonly(&txn).unwrap();
        assert!(
            parts.list(&b, &dst).unwrap().is_empty(),
            "delete must drain the rows"
        );
        assert!(
            parts.list(&b, &copy).unwrap().is_empty(),
            "copy never wrote rows"
        );
        assert!(
            parts.list(&b, &moved).unwrap().is_empty(),
            "rename migrated the rows away"
        );
    }
}
