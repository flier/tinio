//! Object operations of the fs backend (task T042).
//!
//! The bodies themselves are the ops engine's (`tinio_ops`): this module
//! holds what only this backend can do — the file layer (the path mapping,
//! the atomic writer, the versioned publish, the copy's inode fast path, the
//! folder-marker directory) and the adapter steps the engine's plane-free
//! bodies leave to their caller (the bucket mutation lock, the namespace
//! probe, the marker branch and the version facts). Objects are files under
//! `<root>/<bucket>/`; writes stream through the atomic writer
//! (last-write-wins, never a torn object, FR-011) and reads stream with
//! bounded buffers and byte ranges; a missing or stale row is healed from
//! the content (FR-022) by the engine's read path. Folder-marker keys
//! (ending in `/`) never become objects: PUT creates the directory, GET/HEAD
//! report `NoSuchKey`, DELETE removes an empty directory and always succeeds.
//! Reserved `.tinio` segments are refused (FR-020); symlink resolution is
//! rejected when `follow_symlinks` is disabled.

use std::{
    fs::remove_file,
    io::{Error as IoError, ErrorKind},
    mem, panic,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use futures::stream;
use tokio::{fs, fs::File, runtime::Handle, task};

use super::{Error, FsStorage};
/// The rename guard's home: `rename_object` calls the contract's defaulted
/// [`VersionOps::ensure_rename_allowed`] so the rule is never spelled here.
#[cfg(feature = "versioning")]
use crate::_core::storage::VersionOps;
// The row type has one user in every build — the gate's judgement on one
// stored row (`gated_live`), taken by the un-gated tag answer as well as by
// the versioning-gated slot resolutions — and two in some: the
// versioning-gated helpers name it (`resolve_object_slot`'s signature,
// `info_of`), and the **unix**-gated copy record tail
// (`commit_staged`/`record_committed`) writes the table directly.
use crate::_store_redb::object_meta;
// The copy fast path's record tail (`commit_staged`/`record_committed`) is
// unix-only — Windows has no kernel copy primitive, so its copy arm is the
// engine-backed portable route — and these are its imports.
#[cfg(unix)]
use crate::{_core::to_nanos, _store_redb::object_part, backend::version::stamp_superseded};
use crate::{
    _core::{
        BodyStream, ETag, acl,
        acl::{Acl, AclGrants},
        bucket, checksum,
        multipart::ObjectPart,
        object::{self, Info, Tags},
        storage,
        storage::{
            ByteRange, GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps,
            TagWriteResult, access_denied, no_such_bucket, no_such_key,
        },
        version,
    },
    _ops::StagedBody as EngineStagedBody,
    backend::version::{Publication, Published},
    content::FsStaged,
    fsutil,
    meta::{Content, Verdict, gate},
    write::AtomicWriter,
};

/// The version facts a plane-free engine body cannot fill (G3), as fs's own
/// rows answer them — with the file stamp the same resolution judged, kept
/// for the callers that answer with it instead of re-opening the file.
///
/// The engine's object bodies serve `version_id: None` and
/// `bucket_records_versions: false` — the object row carries no version
/// columns (D37) and the plane-free receiver has no version handle — so the
/// caller supplies what it already knows: the key's **gated** current slot
/// (the recorded identity is served only while the content still matches the
/// row's stamp, F01) and the bucket's versioning state. The stamp rides
/// beside them because the resolution that produced the two already read it:
/// the tag write's answer gate (`tag_answer`) judges the row against the very
/// stamp the pre-step's gate did.
struct VersionFacts {
    /// The current version's identity, gated: the null version when the
    /// recorded identity no longer names the content.
    version_id: Option<version::Id>,
    /// Whether the bucket records versions (state enabled or suspended).
    records_versions: bool,
    /// The file stamp the resolution read, when it read one. `None` means the
    /// step resolved no file (an unversioned bucket, a stripped build, or a
    /// file that was not there) — never "a file it found absent" — so a
    /// caller that needs a stamp falls back to its own resolve.
    stamp: Option<FileStamp>,
}

/// The three facts the staleness gate judges a row against, as the read
/// paths' own resolution reads them (the already-open handle's stamp).
#[derive(Clone, Copy)]
struct FileStamp {
    /// The file's length in bytes.
    size: u64,
    /// The file's modification time.
    mtime: SystemTime,
    /// The file's identity (F01).
    identity: u64,
}

/// The staleness gate's verdict on one stored row against one file stamp, as
/// boolean: "the recorded identity still names the content". The one home of
/// the question every current-slot reader asks — the slot resolution that
/// answers a read's version facts (`resolve_object_slot_stamped`) and the tag
/// write's answer gate (`tag_answer`) both take it, so a row the tag write
/// left is judged by the rule the read judges it by, and the two cannot
/// disagree about a row the write never touched.
fn gated_live(row: Option<&object_meta::Stored>, stamp: FileStamp) -> bool {
    row.is_some_and(|row| {
        gate(
            row,
            Content::Present {
                size: stamp.size,
                mtime: stamp.mtime,
                identity: stamp.identity,
            },
        ) == Verdict::Live
    })
}

impl VersionFacts {
    /// The answer for a bucket that records no versions (or a stripped
    /// build, where no row carries an identity): nothing to patch.
    fn none() -> Self {
        Self {
            version_id: None,
            records_versions: false,
            stamp: None,
        }
    }

    /// Ride the facts onto one served [`Info`].
    fn apply(&self, info: &mut Info) {
        info.version_id = self.version_id.clone();
        info.bucket_records_versions = self.records_versions;
    }
}

/// An empty body stream (a zero-byte object with no range).
fn empty_stream() -> BodyStream {
    Box::pin(stream::empty())
}

/// Remove the object at `path` — the deletion tail `delete_object` and the
/// versioned delete share. With following enabled, a leaf link resolves to
/// the object it aliases: DELETE removes the target (the bytes get/head
/// serve), not the link (a dangling link is removed as itself — the object is
/// already gone). Missing, a directory, and Windows' directory-reported-as-
/// permission-denied are the idempotent no-ops DELETE is.
pub(crate) async fn remove_object_file(path: &Path, follow_symlinks: bool) -> Result<(), Error> {
    let remove_path = match fs::symlink_metadata(path).await {
        Ok(m) if follow_symlinks && fsutil::is_symlink_or_reparse(&m) => fs::canonicalize(path)
            .await
            .unwrap_or_else(|_| path.to_path_buf()),
        _ => path.to_path_buf(),
    };
    match fs::remove_file(&remove_path).await {
        Ok(()) => {}
        // Missing or a directory (DELETE of a marker key without the
        // trailing slash) — DELETE is idempotent, always 204.
        Err(err) if err.kind() == ErrorKind::NotFound || err.kind() == ErrorKind::IsADirectory => {
            // Nothing to do.
        }
        // Windows reports a directory as PermissionDenied: the wrong-type
        // no-op, not an I/O failure.
        Err(err)
            if err.kind() == ErrorKind::PermissionDenied
                && fs::metadata(path)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false) =>
        {
            // Nothing to do.
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

/// Prune now-empty parent directories (best-effort): deleting the last object
/// under a prefix leaves no residue, so a bucket that only ever held deleted
/// objects is empty again (S3 semantics).
pub(crate) async fn prune_parents(path: &Path, bucket_dir: &Path) {
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
}

/// The bytes one fs publish carries — this file layer's own staging value,
/// **not** the trait's staged body: `ObjectOps::StagedBody` is the engine's
/// [`tinio_ops::StagedBody`] (spec E8), which owns the streaming and holds
/// the content plane's own stage ([`FsStaged`], whose drop removes an
/// uncommitted temp).
///
/// This value is what the file layer passes inside one commit: the temp file
/// the staged write left (absent for the folder-marker sentinel, whose commit
/// creates the directory), the content ETag computed while the body streamed,
/// and the stage's recorded checksum.
///
/// Dropping one that was never committed removes the temp file: a failed
/// publish (a rejected conditional PUT's 412, an aborted request) must not
/// leave its full body in `tmp/` for the sweep.
#[derive(Debug)]
pub(crate) struct StagedBytes {
    temp: Option<PathBuf>,
    etag: ETag,
}

impl StagedBytes {
    /// The content ETag (or the folder-marker sentinel's empty ETag).
    pub(crate) fn etag(&self) -> &ETag {
        &self.etag
    }

    /// The staged temp file, if any — consumed by the commit (a dropped
    /// `StagedBytes` then cleans nothing).
    pub(crate) fn into_temp(mut self) -> Option<PathBuf> {
        self.temp.take()
    }

    /// The staged bytes of a real object: the temp file plus the ETag computed
    /// while it was written. A folder marker never gets this far — its commit
    /// is [`FsStorage::commit_marker`]'s.
    pub(crate) fn staged(temp: PathBuf, etag: ETag) -> Self {
        Self {
            temp: Some(temp),
            etag,
        }
    }
}

impl Drop for StagedBytes {
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
    ) -> Result<(PathBuf, File, u64, SystemTime, u64), Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        // Reads of reserved keys report NoSuchKey (FR-020).
        if key.is_reserved() {
            return Err(no_such_key(key).into());
        }
        let path = self.resolve_key(&bucket_dir, key).await?;
        let mut file = self.open_object(&path, key).await?;
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

    /// One policy open of an **already resolved** object path: nofollow
    /// when following is disabled (a swap to a symlink between the resolve
    /// and the open is rejected, R3 — the ELOOP → PermissionDenied
    /// normalization included). The error mapping every read path's open
    /// shares: missing is `NoSuchKey` (the old path-stat's NotFound moved
    /// to the open), a folder marker (a directory — Windows opens one only
    /// with `FILE_FLAG_BACKUP_SEMANTICS`, so the open fails exactly like a
    /// permission denial) is `NoSuchKey` too, and the remaining denial is
    /// the symlink policy's `AccessDenied` under nofollow. The both-fail
    /// corner (the classifying stat fails too — an unsearchable parent)
    /// answers `AccessDenied` under nofollow, where the old path-stat
    /// reported Io (500).
    ///
    /// [`resolve_object_file`](Self::resolve_object_file) is this open plus
    /// the handle's stamp, and every read path takes the two together: the
    /// plain read through the content plane, the version-addressed read
    /// through the gate's own judgement (the version module's
    /// `retire_degraded_identity`, which the stripped build does not carry).
    async fn open_object(&self, path: &Path, key: &object::Key) -> Result<File, Error> {
        match fsutil::open_file(path, self.follow_symlinks).await {
            Ok(file) => Ok(file),
            Err(err) if err.kind() == ErrorKind::NotFound => Err(no_such_key(key).into()),
            Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                let is_dir = fsutil::object_metadata(path, self.follow_symlinks)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                if is_dir {
                    return Err(no_such_key(key).into());
                }
                if !self.follow_symlinks {
                    return Err(access_denied(key).into());
                }
                Err(err.into())
            }
            Err(err) => Err(err.into()),
        }
    }

    /// The response body of one resolved object: the caller's range clamped
    /// to `size`, or the whole object — never a 1-byte read that could
    /// overshoot if the file grew after the resolution. One home for the
    /// body shape [`get_object`](ObjectOps::get_object) and the version
    /// read share.
    pub(crate) async fn object_body(
        file: File,
        size: u64,
        range: Option<ByteRange>,
    ) -> Result<(BodyStream, Option<(u64, u64)>), Error> {
        Ok(match range {
            Some(range) => {
                let (start, end) = range.resolve(size)?;
                (
                    fsutil::file_stream(file, start, end, fsutil::ShortRead::EndBody).await,
                    Some((start, end)),
                )
            }
            None if size == 0 => {
                // No bytes exist to stream — not a 1-byte read, which
                // could overshoot if the file grew after the stat.
                (empty_stream(), None)
            }
            // The end-of-body policy: the range is a clamp on an object
            // served as-is, so a file ending early is a normal end.
            None => (
                fsutil::file_stream(file, 0, size - 1, fsutil::ShortRead::EndBody).await,
                None,
            ),
        })
    }

    /// The key's gated current slot and the row the gate judged, without the
    /// versioning-state read: the version listing resolves each key's
    /// current slot with this (its entries carry no `Info`, so the carrier
    /// would be discarded) and never pays a state read per key.
    #[cfg(feature = "versioning")]
    pub(crate) async fn resolve_object_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<
        (
            File,
            u64,
            SystemTime,
            object_meta::Stored,
            Option<version::Id>,
        ),
        Error,
    > {
        let (file, size, mtime, _identity, row, version_id) =
            self.resolve_object_slot_stamped(bucket, key).await?;
        Ok((file, size, mtime, row, version_id))
    }

    /// **The G3 pre-step, its one home**: judge the key's current slot with
    /// the staleness gate — the rewrite that **clears** an identity the gate
    /// judged stale — and answer the slot and the stamp it read.
    ///
    /// The order this call must keep is the rule's whole point: the engine's
    /// read-heal writes through the contract's **preserving** write, which
    /// carries the row's recorded identity over, while this judgement's own
    /// rewrite clears the identity it judged stale (F01) — so an engine body
    /// served first would leave the retired identifier inside the refreshed
    /// row, and the next request would resolve it against content it no
    /// longer names.
    ///
    /// Two addresses take this step, and each keeps its **guard** where its
    /// own question is asked, because the two questions are not the same:
    /// [`Self::version_facts`]/[`Self::heal_degraded_identity`] serve "the
    /// key's current version, whatever it is" — the read and element paths —
    /// whenever the bucket records versions, while `backend::version`'s
    /// `retire_degraded_identity` serves the version a request **addresses**,
    /// only while the slot still names it (the engine serves the current arm
    /// through its read-heal alone).
    ///
    /// The file stamp rides on the answer: the read path's version facts and
    /// the tag write's answer gate both judge against it, so the element
    /// pre-step hands it on instead of re-opening the unchanged file
    /// ([`Self::resolve_object_slot`] is this answer without it).
    #[cfg(feature = "versioning")]
    pub(crate) async fn resolve_object_slot_stamped(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<
        (
            File,
            u64,
            SystemTime,
            u64,
            object_meta::Stored,
            Option<version::Id>,
        ),
        Error,
    > {
        let (path, file, size, mtime, identity) = self.resolve_object_file(bucket, key).await?;
        let judged = self.meta_store.stored_entry(bucket, key).await?;
        // The staleness gate, from its one home in the meta store (the same
        // call the version adapter's `current_with_row` takes): a recorded
        // identity the content no longer matches is stale, and the version
        // adapter reads that state as the null version — the two answer one
        // verdict, never one each. The judgement itself is `gated_live`'s,
        // shared with the tag write's answer gate.
        let matched = gated_live(
            judged.as_ref(),
            FileStamp {
                size,
                mtime,
                identity,
            },
        );
        let stale = if matched {
            None
        } else {
            judged
                .as_ref()
                .and_then(|stored| stored.version_id.as_ref())
        };
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
                stale,
            )
            .await?;
        let version_id = matched.then(|| row.version_id.clone()).flatten();
        Ok((file, size, mtime, identity, row, version_id))
    }

    /// The unix fast path of the copy primitive: the kernel's
    /// `copy_file_range` moves the bytes (zero userspace buffering), the
    /// source's single-form ETag is reused for a full copy when the file
    /// is provably unchanged (AWS semantics — the content MD5 of a full
    /// copy is the source's), and the shared commit publishes the
    /// destination under the mutation lock with the caller's tags, owner
    /// and ACL (a copy is a fresh object — never the source's) and
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
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        checksum: Option<checksum::Recorded>,
    ) -> Result<Info, Error> {
        // Folder-marker destination: a marker is a directory, and its commit
        // is the marker branch's (`commit_marker`, the same one the ops
        // adapter takes) — no bytes are copied and the
        // tags/owner/acl/checksum params are accept-and-dropped.
        if dst_key.is_folder_marker() {
            return self.commit_marker(dst_bucket, dst_key).await;
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
            StagedBytes::staged(temp, etag),
            tags,
            owner,
            acl,
            checksum,
        )
        .await
    }

    /// The bytes half of a **real object's** commit: publish the staged temp
    /// with the version engine's rename (its supersede commits the outgoing
    /// version's row BEFORE the rename) and apply the unix ownership
    /// hardening, answering the staged ETag and the publish's own stamp. The
    /// row describing the bytes is [`Self::record_committed`]'s —
    /// [`Self::commit_staged`] calls both, while the content contract's
    /// `publish` calls only this (publish is the visibility point; nothing
    /// observes a half-published object).
    ///
    /// A folder marker never reaches here: it is a directory, not a staged
    /// temp, and its commit is [`Self::commit_marker`]'s — taken by every
    /// caller before the publish (the ops engine's `commit_object` adapter,
    /// the content plane's `publish`, the copy fast path).
    pub(crate) async fn publish_object_file(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBytes,
        owner: Option<&acl::OwnerId>,
        publication: Publication,
    ) -> Result<(ETag, Published), Error> {
        // The owner is the unix dir/object hardening input only.
        #[cfg(not(unix))]
        let _ = owner;
        // The single resolve runs under the mutation lock (P5): the
        // pre-lock fail-fast pair was redundant — `stage_body` already
        // validated the key before the body streamed, and the
        // authoritative check must re-run under the lock anyway (a
        // followed bucket symlink retargeted between stage and rename
        // must not send the write to a stale target).
        let etag = staged.etag().clone();
        // A real object always arrives with a staged temp: the folder-marker
        // sentinel is the callers' branch (`commit_marker`) and never gets
        // here.
        let Some(temp) = staged.into_temp() else {
            return Err(IoError::other("staged body without a temp file").into());
        };
        // The rename is the bucket-mutating step: under the mutation
        // lock it cannot race a `delete_bucket` (a PUT that loses
        // reports an error instead of silently losing the object with
        // 200). A failed commit removes the staged temp — a rejected
        // write leaves no residue.
        // The publish is `publish_versioned`'s: the bucket mutation lock, the
        // re-resolved target, the version branch (its supersede commits the
        // outgoing version's row BEFORE the rename, so a crash between the
        // two is recovered by adopting that row, never by minting a second
        // identity for the same content — the engine's supersede protocol)
        // and the rename, in one call every write path shares. A failed
        // publish removes the staged temp — a rejected write leaves no
        // residue.
        let published = match self
            .publish_versioned(bucket, key, &temp, publication)
            .await
        {
            Ok(published) => published,
            Err(err) => {
                let _ = fs::remove_file(&temp).await;
                return Err(err);
            }
        };
        // The unix ownership hardening (spec 2026-09-05 §5a): the
        // landed object chowns to the NEW owner's mapped uid (an
        // overwrite replaces the owner; a copy's owner is the
        // caller's). A chown failure fails closed — the rename
        // already landed, so a wrongly-owned 0600 file must not be
        // left behind: remove it and error the request (the row was
        // not written yet; the upload-side temp was consumed by the
        // rename). An unmapped owner stays server-user-owned, still
        // `0600`.
        #[cfg(unix)]
        if let Some(uid) = self.owner_uid(owner)
            && let Err(err) = fsutil::chown_file(&published.path, uid).await
        {
            let _ = fs::remove_file(&published.path).await;
            return Err(err.into());
        }
        Ok((etag, published))
    }

    /// The folder-marker commit: PUT on a key ending in `/` creates the
    /// directory, idempotently, under the same guards a real object's commit
    /// runs under — the bucket mutation lock (a concurrent `delete_bucket`
    /// must not be resurrected) and the symlink policy (a marker under a
    /// linked parent must not create a directory outside the root).
    ///
    /// A marker holds no row and no content: the tags/owner/acl/checksum the
    /// commit params carry are accept-and-dropped, and the answer's
    /// `last_modified` is the **directory's own mtime** (D2) — the engine's
    /// body answers the wall clock for the marker branch it owns, which no fs
    /// caller may reach.
    ///
    /// The marker's answer still carries the bucket's versioning state (the
    /// server's identity rule reads it off the `Info`): no write plan runs
    /// here, so the state is read — the one point read the marker path pays.
    pub(crate) async fn commit_marker(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Info, Error> {
        #[cfg(unix)]
        use crate::fsutil::DirOwnerUid;
        #[cfg(not(unix))]
        use crate::fsutil::DirOwnerUid;

        let _guard = self.lock_bucket_mutations(bucket).await;
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let target = self.resolve_key(&bucket_dir, key).await?;
        // The unix dir hardening (spec 2026-09-05 §5a): the marker dir is
        // bucket content — `0700`, chowned to the bucket owner's mapped uid.
        #[cfg(unix)]
        let dir_owner_uid = self.bucket_owner_uid(bucket).await;
        fsutil::ensure_dir(
            &target,
            #[cfg(unix)]
            DirOwnerUid(dir_owner_uid),
            #[cfg(not(unix))]
            DirOwnerUid,
        )
        .await?;
        let mtime = fs::metadata(&target).await?.modified()?;
        let records_versions = self.records_versions(bucket).await?;
        Ok(Info {
            key: key.clone(),
            size: 0,
            last_modified: mtime,
            etag: ETag::EMPTY,
            tags: Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            version_id: None,
            bucket_records_versions: records_versions,
        })
    }

    /// The version facts a read must supply (G3): the bucket's state and, for
    /// a bucket that records versions, the key's gated current identity —
    /// plus the file stamp the same resolution read, which the callers that
    /// answer with it take from here rather than re-open the file for it.
    ///
    /// The address this entry serves is "the key's current version, whatever
    /// it is", and its guard is the bucket's state: a bucket whose state
    /// reads `Unversioned` needs no step at all — a row only ever receives an
    /// identity from a write that saw a versioning state, and no API path
    /// sets a bucket back to `Unversioned` — so the common (unversioned) read
    /// pays one state read, the same point read the pre-engine read path paid
    /// for its `Info` carrier. The step itself, and why it must run before
    /// the engine body, is [`Self::resolve_object_slot_stamped`]'s.
    #[cfg(feature = "versioning")]
    async fn version_facts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<VersionFacts, Error> {
        let records_versions = self.records_versions(bucket).await?;
        if !records_versions {
            return Ok(VersionFacts::none());
        }
        let (_file, size, mtime, identity, _row, version_id) =
            self.resolve_object_slot_stamped(bucket, key).await?;
        Ok(VersionFacts {
            version_id,
            records_versions,
            stamp: Some(FileStamp {
                size,
                mtime,
                identity,
            }),
        })
    }

    /// The same facts in a `versioning`-less build: the state is always
    /// `Unversioned` there (no row can carry an identity), so the answer is
    /// the empty one and no read rides.
    #[cfg(not(feature = "versioning"))]
    async fn version_facts(
        &self,
        _bucket: &bucket::Name,
        _key: &object::Key,
    ) -> Result<VersionFacts, Error> {
        Ok(VersionFacts::none())
    }

    /// The version fields a tag write answers (G3): the row the write left,
    /// gated against the object file's own stamp (the pre-engine body's rule —
    /// an identity whose content no longer matches is not served), plus the
    /// bucket's state.
    ///
    /// The file's absence is the caller's to read: `put_object_tags` fails
    /// with `NoSuchKey` (the object must be there), while
    /// `delete_object_tags` answers the null identity (a lenient delete names
    /// nothing).
    ///
    /// The carrier flag is read **after** the element write, in its own point
    /// read, where the pre-engine body read the row and the state in the
    /// write's own transaction: a versioning-state flip landing between the two
    /// can make this answer report the new state instead of the one the write
    /// saw. The window is a state flip racing a tag write — the flip takes the
    /// bucket mutation lock and the tag write takes none, exactly as before —
    /// and the flag only decides whether the answer carries a version-id
    /// member, so a client in that window sees the state it just set rather
    /// than a stale one. The row is read here too, and must be: it is the row
    /// the write left, not the one the pre-step judged.
    ///
    /// The file stamp, in contrast, is the pre-step's own (`facts`) whenever
    /// the step resolved one: the tag write touches the element only, so the
    /// file's stamp is the same one a post-write open would read — bar a
    /// concurrent writer replacing the bytes, whose own answer the pre-engine
    /// body's second resolve could report just as well. The fallback arms are
    /// the buckets and builds whose pre-step resolves no file: an
    /// unversioned bucket, a `versioning`-less build, and a file the step did
    /// not find (its `NoSuchKey` is the caller's to answer), each re-resolving
    /// here exactly as before.
    async fn tag_answer(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        facts: &VersionFacts,
    ) -> Result<TagWriteResult, Error> {
        let records_versions = self.records_versions(bucket).await?;
        let FileStamp {
            size,
            mtime,
            identity,
        } = match facts.stamp {
            Some(stamp) => stamp,
            None => {
                let (_path, _file, size, mtime, identity) =
                    self.resolve_object_file(bucket, key).await?;
                FileStamp {
                    size,
                    mtime,
                    identity,
                }
            }
        };
        let row = self.meta_store.stored_entry(bucket, key).await?;
        // The gate's judgement, from its one home (`gated_live`) — the same
        // call the slot resolution that carried this stamp makes, so the two
        // answer one verdict for one row.
        let live = gated_live(
            row.as_ref(),
            FileStamp {
                size,
                mtime,
                identity,
            },
        );
        Ok(TagWriteResult {
            version_id: live.then(|| row.and_then(|row| row.version_id)).flatten(),
            bucket_records_versions: records_versions,
        })
    }

    /// [`Self::version_facts`] with the missing-file arm answered here: an
    /// element path whose key has no file takes the engine body as its answer
    /// — tagging a key that is not there is `NoSuchKey`, and deleting its
    /// tags is the idempotent success — because the gate's judgement needs a
    /// file to hash. The step itself is the one `version_facts` runs
    /// ([`Self::resolve_object_slot_stamped`]); the returned facts are the
    /// caller's to carry where it answers with them (the tag pair's
    /// `tag_answer` gates against the stamp this step already read), and an
    /// element read that carries no stamp of its own drops them.
    async fn heal_degraded_identity(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<VersionFacts, Error> {
        match self.version_facts(bucket, key).await {
            Ok(facts) => Ok(facts),
            Err(Error::Storage(storage::Error::NoSuchKey(_))) => Ok(VersionFacts::none()),
            Err(err) => Err(err),
        }
    }

    /// The bytes half of an object rename: resolve the live source (a
    /// missing source answers `NoSuchKey`), ensure the destination's parent
    /// chain (the unix dir hardening included), move the file and sync the
    /// directories. The caller holds the bucket mutation lock and owns the
    /// state move — `rename_object` migrates the rows, while the content
    /// contract's `rename` moves the bytes alone.
    pub(crate) async fn move_object_file(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<(PathBuf, u64, SystemTime, u64), Error> {
        let (src_path, _file, size, mtime, identity) =
            self.resolve_object_file(bucket, src).await?;
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let dst_path = self.resolve_key(&bucket_dir, dst).await?;
        // The file move: same-volume (one bucket), atomic, overwriting an
        // existing destination. New ancestor directories are synced like
        // the first commit into a new prefix (F03); the unix dir
        // hardening (spec 2026-09-05 §5a) applies like a PUT's — the
        // created prefix dirs take the bucket owner's mapped uid.
        // Ownership of the OBJECT itself is preserved naturally (the
        // inode moves — no chown, a rename is not a fresh object).
        #[cfg(unix)]
        let dir_owner_uid = self.bucket_owner_uid(bucket).await;
        let created_parent = match dst_path.parent() {
            Some(parent) => {
                #[cfg(unix)]
                use crate::fsutil::DirOwnerUid;
                #[cfg(not(unix))]
                use crate::fsutil::DirOwnerUid;

                fsutil::ensure_dir(
                    parent,
                    #[cfg(unix)]
                    DirOwnerUid(dir_owner_uid),
                    #[cfg(not(unix))]
                    DirOwnerUid,
                )
                .await?
            }
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
        Ok((src_path, size, mtime, identity))
    }

    /// The **copy fast path's** commit tail (the portable arm and the
    /// engine-backed `commit_object` path do not use it): atomically publish a
    /// staged body onto
    /// `key` under the mutation lock (the same re-checks and guards as a
    /// plain commit), then write the object's `OBJECT_META` entry — the
    /// interface-validated `tags`, the caller's `owner`/`acl` (the owner
    /// rides its own row element; the stored ACL holds the grant set
    /// only — `Stored.acl.owner` stays `None` by construction) and the
    /// recorded `checksum` (the copy's carried value — kind included) rides
    /// in the same row — and remove any stale `OBJECT_PARTS` rows of the key in
    /// ONE write transaction (a fresh object is single-part:
    /// overwriting a previously multipart-completed object must not
    /// leave its parts behind). Returns the committed object metadata.
    #[cfg(unix)]
    #[allow(clippy::too_many_arguments)]
    async fn commit_staged(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBytes,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        checksum: Option<checksum::Recorded>,
    ) -> Result<Info, Error> {
        // The bytes half: the publish's own stamp is what the row below
        // records.
        let (etag, published) = self
            .publish_object_file(bucket, key, staged, owner, Publication::Versioned)
            .await?;
        // The object is committed. The stale-part removal and the
        // outgoing version's stamp ride in the same transaction; what a
        // meta-write failure means is decided below (I7). The guard the
        // publish holds stays alive until this write is done: the
        // branch, the rename and the row are one critical section.
        if let Err(err) = self
            .record_committed(
                bucket,
                key,
                &etag,
                published.size,
                published.mtime,
                published.identity,
                &tags,
                checksum.as_ref(),
                owner,
                acl,
                published.marker.id.as_ref(),
                published.seq,
                published.superseded,
            )
            .await
        {
            // I7 (the 2026-10-06 review): a versioned write's identity is
            // real only once the row that records it is durable — answering
            // it after a failed row write hands the client an id no read
            // resolves (`?versionId=` answers NoSuchVersion and the slot
            // reads as the null version). The publish fails instead; the
            // file at the key path stays (the rename already happened) and
            // reads serve it as the null version, the same shape an
            // out-of-band edit produces, and a retry re-publishes. An
            // unversioned write keeps FR-022's best-effort rule: its answer
            // carries no identity.
            if published.records_versions {
                return Err(err);
            }
            tracing::warn!(error = %err, "meta entry not persisted after commit");
        }
        Ok(Info {
            key: key.clone(),
            size: published.size,
            last_modified: published.mtime,
            etag,
            tags,
            checksum,
            owner: owner.cloned(),
            acl: Acl {
                owner: None,
                grants: acl.grants.clone(),
            },
            version_id: published.marker.id,
            // The publish's plan resolved the state; no second read.
            bucket_records_versions: published.records_versions,
        })
    }

    /// The committed object's row write: the `OBJECT_META` entry (tags +
    /// checksum + owner + ACL elements — the stored ACL holds the grant
    /// set only, the owner rides its own element — plus the current
    /// version's identity and sequence, the write branch's
    /// `publish_versioned` answer), the removal of any stale
    /// `OBJECT_PARTS` rows of the key, and the outgoing version's row stamp
    /// (`superseded`) in ONE write transaction: the transaction that moves
    /// the current slot off that row is the one that makes it history, so the
    /// two can never disagree. The caller decides what a failure means: a
    /// versioned write must not answer an identity whose row never landed
    /// (I7, the 2026-10-06 review) and fails, while an unversioned write
    /// keeps FR-022's best-effort rule — its entry self-heals on the next
    /// read and the failure is warned.
    #[cfg(unix)]
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
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        version_id: Option<&version::Id>,
        version_seq: u64,
        superseded: Option<u64>,
    ) -> Result<(), Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let etag = etag.clone();
        let tags = tags.clone();
        let checksum = checksum.cloned();
        let owner = owner.cloned();
        let version_id = version_id.cloned();
        let acl = Acl {
            owner: None,
            grants: acl.grants.clone(),
        };
        self.handle
            .write(move |txn| {
                object_meta::Table::open(txn)?.put(
                    &bucket,
                    &key,
                    &object_meta::Stored {
                        version_id: version_id.clone(),
                        version_seq,
                        is_delete_marker: false,
                        etag: etag.clone(),
                        size,
                        mtime: to_nanos(mtime),
                        file_identity: identity,
                        tags: tags.clone(),
                        checksum: checksum.clone(),
                        owner: owner.clone(),
                        acl: acl.clone(),
                    },
                )?;
                object_part::Table::open(txn)?.remove_key(&bucket, &key)?;
                stamp_superseded(txn, &bucket, &key, superseded)?;
                Ok(())
            })
            .await
            .map_err(Error::from)
    }
}

// The object group: every body is the ops engine's (`tinio_ops`), called from
// here with the adapter steps only this backend can take.
//
// What stays fs-side, and why:
//
// - the **bucket mutation lock**: the trait impl takes it around each
//   mutating body, the same critical section the pre-engine bodies held (the
//   receiver's content plane is the non-owning one, so nothing inside takes
//   the lock a second time — it is not re-entrant);
// - the **file layer** ([`StagedBytes`], `publish_object_file`,
//   `move_object_file`, the copy's inode fast path, the versioned publish in
//   `backend::version`), which is what the content plane composes;
// - the **folder-marker branch** (D2): a marker is this crate's directory
//   namespace and its `last_modified` is the directory's mtime, which the
//   engine's marker branch (a wall clock) cannot answer;
// - the **version facts** (G3): the engine's plane-free bodies serve
//   `version_id: None` / `bucket_records_versions: false`, so the adapter
//   fills them from this backend's own rows;
// - the **guards**: `VersionOps::ensure_rename_allowed`, the reserved and
//   marker destination checks, the namespace probe, and every error variant
//   with its projection.
#[async_trait::async_trait]
impl ObjectOps for FsStorage {
    /// The trait's staged body is the engine's (spec E8): this crate's content
    /// plane supplies the stage ([`FsStaged`] — a temp file under `tmp/`, or
    /// the folder-marker sentinel) and the engine owns everything else.
    ///
    /// Dropping an uncommitted staged body removes the temp file through the
    /// stage's own drop: a rejected conditional PUT (412) or an aborted
    /// request must not leave its full body in `tmp/` for the sweep.
    type StagedBody = EngineStagedBody<FsStaged>;

    async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<Self::StagedBody, Error> {
        // The engine owns the streaming: the stage opens through the content
        // plane (which applies the bucket existence, the path mapping and the
        // symlink policy before a byte moves — the checks the pre-engine body
        // ran first, from the same calls), the content MD5 the commit records
        // is computed while the body streams, and the tee's digest rides in
        // under the write path's FULL_OBJECT rule. `carried` — a
        // version-addressed copy's source checksum, kind included — wins over
        // the streamed digest.
        self.objects()
            .stage_body(bucket, key, body, tee, carried)
            .await
            .map_err(Error::from)
    }

    /// The mutation phase: publish the staged bytes at `key` and record the
    /// row describing them.
    ///
    /// Two adapter steps decide which engine body runs:
    ///
    /// - a folder-marker key takes this crate's own branch (D2): the marker is
    ///   the directory namespace and its `last_modified` is the **directory's
    ///   mtime** — an engine answer of the wall clock must never reach the
    ///   wire for fs;
    /// - a bucket that records versions takes the version-creating shape
    ///   (`put_object_versioned`, RI3), which supersedes the outgoing version,
    ///   publishes, writes the object row **and its cleared retained part
    ///   list** and then advances the current slot. Steps 4 and 5 are two
    ///   transactions — accepted as such (RI3): the contract exposes no single
    ///   call that writes the object row and moves the slot, and a crash
    ///   between them leaves a superseded version's parts uncleared, a stale
    ///   retained list rather than a lost object, for the repair pipeline.
    ///
    /// The bucket mutation lock spans the whole body: the supersede's rows,
    /// the rename and the row write are one critical section against a
    /// concurrent `delete_bucket`, exactly as the pre-engine commit held it
    /// (the receiver's content plane is the non-owning one, so the publish
    /// inside rides this lock instead of taking it again).
    ///
    /// fs's `MetadataFailure` is `Warn` (RI2): a row write refused after the
    /// publish is logged and the answer is still the committed object — the
    /// next read heals the row — and the published bytes are never taken back.
    async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: Self::StagedBody,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<Info, Error> {
        if key.is_folder_marker() {
            return self.commit_marker(bucket, key).await;
        }
        let _guard = self.lock_bucket_mutations(bucket).await;
        // Ownership is decided on the temp, before any version moves. A
        // failed chown drops the stage and leaves the current version in
        // place; the publish's rename keeps the uid.
        #[cfg(unix)]
        if let Some(uid) = self.owner_uid(owner) {
            staged.staged().chown_temp(uid).await?;
        }
        let info = {
            #[cfg(feature = "versioning")]
            {
                let plane = self.version_plane(bucket);
                self.objects()
                    .put_object_versioned(&plane, bucket, key, staged, tags, owner, acl)
                    .await
                    .map_err(Error::from)?
            }
            #[cfg(not(feature = "versioning"))]
            {
                self.objects()
                    .commit_object(bucket, key, staged, tags, owner, acl)
                    .await
                    .map_err(Error::from)?
            }
        };
        Ok(info)
    }

    /// Server-side copy of `src` into `dst` (S3 CopyObject).
    ///
    /// The copy keeps this crate's own byte route — the contract's
    /// `copy_object` doc spells the override out ("a backend may override with
    /// a filesystem-level copy"): on unix the kernel's `copy_file_range` moves
    /// the source into the staged temp with no userspace buffering, the
    /// source's single-form ETag is reused while the file is provably
    /// unchanged (a mid-copy change is caught by the torn-copy guard), and the
    /// carried checksum rides the same guard. The engine's content-side copy
    /// is the contract's portable read-and-publish route, which would give
    /// every same-filesystem copy a full userspace pass.
    ///
    /// The portable arm (no kernel copy primitive) is the contract's own
    /// default shape over this backend's stage/commit pair, which is
    /// engine-backed: read the source through the plain read, stage the
    /// destination carrying the caller's checksum, commit it with the caller's
    /// tags/owner/ACL. A copy is a fresh object either way — it inherits
    /// neither the source's retained parts nor its owner/ACL, and its mtime is
    /// the copy time.
    #[allow(clippy::too_many_arguments)]
    async fn copy_object(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        checksum: Option<checksum::Recorded>,
    ) -> Result<Info, Error> {
        #[cfg(unix)]
        {
            self.copy_object_fast(
                src_bucket, src_key, dst_bucket, dst_key, tags, owner, acl, checksum,
            )
            .await
        }
        #[cfg(not(unix))]
        {
            let get = self.get_object(src_bucket, src_key, None).await?;
            let staged = self
                .stage_body(dst_bucket, dst_key, get.body, None, checksum)
                .await?;
            self.commit_object(dst_bucket, dst_key, staged, tags, owner, acl)
                .await
        }
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        // The namespace probe first: fs has always answered `NoSuchBucket` for
        // a key in a bucket that is not there, while the content plane's own
        // `stat` answers "no key" for an absent namespace (it swallows the
        // missing bucket so a bucket-scoped probe stays total).
        self.ensure_bucket(bucket).await?;
        // The version facts (G3) are read **before** the engine body: the
        // step's heal clears a stale recorded identity, and the engine's own
        // heal preserves it, so an order with the engine first would leave the
        // stale identifier in the row for the next read to resurrect.
        let facts = self.version_facts(bucket, key).await?;
        let mut got = self
            .objects()
            .get_object(bucket, key, range)
            .await
            .map_err(Error::from)?;
        facts.apply(&mut got.info);
        Ok(got)
    }

    async fn head_object(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Info, Error> {
        // The same two adapter steps as the get, without the body.
        self.ensure_bucket(bucket).await?;
        let facts = self.version_facts(bucket, key).await?;
        let mut info = self
            .objects()
            .head_object(bucket, key)
            .await
            .map_err(Error::from)?;
        facts.apply(&mut info);
        Ok(info)
    }

    /// Rename `src` to `dst` (S3 RenameObject).
    ///
    /// The guards run first, exactly as they always have: a degenerate
    /// self-move is the head (the interface answers 412 before calling, and a
    /// read-then-move would delete the bytes), a bucket that records versions
    /// refuses the key-moving operation (`VersionOps::ensure_rename_allowed`
    /// — the engine's plane-free body cannot consult the version handles), a
    /// reserved destination is refused like every other write (FR-020) and a
    /// marker destination cannot hold an object.
    ///
    /// The bucket mutation lock then spans the engine body: the file move and
    /// the state move are one critical section against a concurrent
    /// `delete_bucket`. The engine moves the bytes by `ObjectContent::rename`
    /// (the destination's parent chain, the unix dir hardening and the
    /// directory sync are this crate's `move_object_file`) and the metadata by
    /// `ObjectMeta::move_`, one contract call each — the row, its retained
    /// parts and the source's metadata move together.
    async fn rename_object(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<Info, Error> {
        if src == dst {
            return self.head_object(bucket, src).await;
        }
        #[cfg(feature = "versioning")]
        self.ensure_rename_allowed(bucket, dst).await?;
        if dst.is_reserved() {
            return Err(access_denied(dst).into());
        }
        if dst.is_folder_marker() {
            return Err(storage::invalid_key(dst.to_string()).into());
        }
        let _guard = self.lock_bucket_mutations(bucket).await;
        self.objects()
            .rename_object(bucket, src, dst)
            .await
            .map_err(Error::from)
    }

    async fn delete_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        // The namespace probe: DELETE has always answered `NoSuchBucket` for a
        // bucket that is not there, while the content plane's own removal is
        // idempotent by contract (an absent namespace is "already deleted").
        let bucket_dir = self.ensure_bucket(bucket).await?;
        if key.is_folder_marker() {
            // Remove the directory only when it is empty; a non-empty
            // directory is left in place (s3-surface.md). Always 204 —
            // a missing or non-directory path (e.g. a symlink) is
            // idempotently "deleted".
            let path = self.resolve_key(&bucket_dir, key).await?;
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
            // Folder markers stay outside the version model (the design's
            // ruling): a delete removes the empty directory, creates no
            // marker.
            return Ok(version::Marker::none());
        }
        #[cfg(feature = "versioning")]
        let path = self.resolve_key(&bucket_dir, key).await?;
        // The versioned delete stays **backend-side** (`delete_versioned`), and
        // the reason is a contract-level shape, not a lock or a file path: a
        // shared body that wrote the delete marker as a **version row** — the
        // shape whose slot resolves the marker bit off that row — would let the
        // store's one-null-version rule displace the superseded null version's
        // own row, losing a version the shared conformance leg pins ("the
        // superseded null version must survive a suspended DELETE"). fs's model
        // keeps the current version in the **object row** (D37) and the version
        // table for history alone, so its delete writes the marker there and
        // leaves the null's history row untouched. An unversioned bucket
        // answers `None` and falls through to the engine's body below.
        #[cfg(feature = "versioning")]
        if let Some(marker) = self.delete_versioned(bucket, key, &path).await? {
            prune_parents(&path, &bucket_dir).await;
            return Ok(marker);
        }
        // The version-free delete: the engine's body — the content plane's own
        // removal (the file and the parent pruning) and the row removal, both
        // idempotent.
        self.objects()
            .delete_object(bucket, key)
            .await
            .map_err(Error::from)
    }

    async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        // The namespace probe first (NoSuchBucket, as every fs object op has
        // answered); the existence rule itself is the engine's: a key the
        // content plane does not hold is `NoSuchKey` on either row policy.
        self.ensure_bucket(bucket).await?;
        self.heal_degraded_identity(bucket, key).await?;
        self.objects()
            .get_object_tags(bucket, key)
            .await
            .map_err(Error::from)
    }

    async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<TagWriteResult, Error> {
        self.ensure_bucket(bucket).await?;
        // The pre-step's facts ride to the answer: its stamp is what the
        // answer's gate judges the written row against, so the tag write does
        // not re-open the file it did not touch.
        let facts = self.heal_degraded_identity(bucket, key).await?;
        // The element write is the engine's; the answer's version fields are
        // this backend's (G3), from the row the write left (the engine body
        // returns none of them).
        self.objects()
            .put_object_tags(bucket, key, tags)
            .await
            .map_err(Error::from)?;
        self.tag_answer(bucket, key, &facts).await
    }

    async fn delete_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<TagWriteResult, Error> {
        self.ensure_bucket(bucket).await?;
        // The same hand-off as the put: the pre-step's stamp is the answer's,
        // when the step resolved one.
        let facts = self.heal_degraded_identity(bucket, key).await?;
        // The clear is the engine's one lenient call (its `Ok(false)` *is* the
        // missing object, so no existence probe is paid before it); the
        // answer's version fields are this backend's, and a file that is not
        // there names nothing — the pre-engine body's missing-object arm.
        self.objects()
            .delete_object_tags(bucket, key)
            .await
            .map_err(Error::from)?;
        match self.tag_answer(bucket, key, &facts).await {
            Ok(answer) => Ok(answer),
            Err(Error::Storage(storage::Error::NoSuchKey(_))) => Ok(TagWriteResult::untouched()),
            Err(err) => Err(err),
        }
    }

    async fn get_object_acl(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Acl, Error> {
        self.ensure_bucket(bucket).await?;
        self.heal_degraded_identity(bucket, key).await?;
        self.objects()
            .get_object_acl(bucket, key)
            .await
            .map_err(Error::from)
    }

    async fn put_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<(), Error> {
        self.ensure_bucket(bucket).await?;
        self.heal_degraded_identity(bucket, key).await?;
        self.objects()
            .put_object_acl(bucket, key, grants)
            .await
            .map_err(Error::from)
    }

    async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, Error> {
        // The retained rows of a multipart-completed object, in part-number
        // order — empty for an object that was never multipart-completed (a
        // plain put or copy has no parts). The engine's body serves them
        // through the contract's own projection, so a stored number outside
        // the API's range refuses with `InvalidPartNumber` rather than
        // panicking on a row this crate did not write.
        self.ensure_bucket(bucket).await?;
        self.heal_degraded_identity(bucket, key).await?;
        self.objects()
            .list_object_parts(bucket, key)
            .await
            .map_err(Error::from)
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        // The bucket-existence check the pre-engine listing made before its
        // empty-page shortcut: the engine's body answers the empty page for
        // `max_keys == 0` **before** its first scan, so that one request needs
        // the walk's own probe here — the same `NoSuchBucket` for a bucket
        // that is not there, and the same empty-stream answer for a bucket
        // directory the symlink policy hides (`ensure_bucket` would refuse
        // that one, which the listing never did). A larger page needs no
        // probe: the engine's scan walks, and the walk answers for itself.
        if params.max_keys == 0 {
            // Built for the probe and dropped un-polled (F10's `let _`).
            let _ = self
                .listing()
                .walk_files_streaming(&params.bucket, &params.prefix)
                .await?;
        }
        // The page's `Info` rows all report the bucket's versioning state — one
        // read for the page, resolved here (the engine's listing answers no
        // version identity of its own, and neither did fs's: a listed entry
        // names the current version implicitly).
        let records_versions = self.records_versions(&params.bucket).await?;
        self.objects()
            .list_objects(params, records_versions)
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
