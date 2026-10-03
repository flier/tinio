//! Filesystem primitives shared by the storage backends: the atomic commit
//! protocol, the ranged body stream, the staged-temp drop discipline, and the
//! directory/file helpers around them.
//!
//! **This module exists because the copies it replaced had already drifted.**
//! The filesystem backend's commit protocol, its staged-temp `Drop`, its
//! ranged file stream and its helper trio were written once and then copied
//! into the block store — and the copies were not equal. The block store's
//! publish had the parent-directory fsync but not the cross-volume fallback;
//! its staged-temp `Drop` unlinked inline on a runtime worker instead of off
//! it; its read loop allocated a fresh buffer per chunk instead of reusing
//! one; and its directory creation, idempotent removal and missing-directory
//! listing were spelled by hand. A primitive with two owners is a primitive
//! with one owner's bugs; the fix is one definer per primitive, called by
//! every backend, and no backend keeps a copy of it now.
//!
//! What the module may use is deliberately narrow: `tinio-core` (the
//! [`BodyStream`] the ranged stream hands chunks out as), `tokio`, `std` and
//! this crate — never `tinio-fs`, so the primitives sit *below* both backends
//! rather than beside one of them.
//!
//! The promises each piece carries:
//!
//! - [`commit`] — the atomic commit protocol: create the target's parent
//!   mode-hardened ([`ensure_dir`]), rename the staged temp onto the target
//!   (with the EXDEV fallback of `copy_across_volumes`), then sync the parent
//!   directory — and, when this call created it, the whole ancestor chain up
//!   to the caller's `sync_root`. A post-rename sync failure is warned, never
//!   fatal: the rename is the commit point.
//! - [`remove_staged_temp`] — the staged-temp `Drop` discipline: a best-effort
//!   unlink that runs off the async worker and never panics, not even inside a
//!   shutting-down runtime.
//! - [`file_stream`] — the bounded-chunk ranged body stream, one `BytesMut`
//!   reused across chunks.
//! - [`ensure_dir`], [`ok_if_missing`], [`entries_of`] — probe-then-create
//!   with mode hardening, the removal that tolerates `NotFound`, and the
//!   listing that reads a missing directory as empty.

use std::{
    fs::remove_file,
    io::{self, ErrorKind, SeekFrom},
    mem, panic,
    path::{Path, PathBuf},
};

use bytes::{BufMut, BytesMut};
use futures::stream;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt},
    runtime::Handle,
    task,
};
use uuid::Uuid;

use crate::_core::{object::RESERVED_SEGMENT, storage::BodyStream};

/// Bounded chunk size for the shared IO loops (constitution V: no per-object
/// buffering; hyper chunks are typically ≤ 64 KiB anyway). The write path's
/// copy/hash loops and the ranged stream below both read and hand off in
/// pieces of at most this size.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// The unix mode of object files the backends create (spec 2026-09-05 §5a):
/// files land `0600` regardless of the process umask — the create-then-chmod
/// shape of the `STATE_DIR_MODE` precedent in the fs backend's
/// `database::open`. Pre-existing files are never touched (no retroactive
/// chmod pass); on Windows the created files inherit the creating user's ACLs
/// (std has no ACL API).
#[cfg(unix)]
pub const FILE_MODE_PRIVATE: u32 = 0o600;

/// The unix mode of the private directories the backends create (spec
/// 2026-09-05 §5a): directories land `0700` regardless of the process umask,
/// the directory half of [`FILE_MODE_PRIVATE`]'s rule.
#[cfg(unix)]
pub const DIR_MODE_PRIVATE: u32 = 0o700;

/// Sync `parent` so a rename inside it is durable (D1 — the standard
/// durable-write pattern: content fsynced before the rename, the directory
/// fsynced after). On unix the parent directory is opened (read-only) and
/// fsynced. On Windows std cannot open a directory handle and no heavyweight
/// dependency is added for a directory flush — the directory entry is not
/// fsynced there (a documented limitation, pipeline-spec.md §7 D1). Tokio's
/// `sync_all` runs the blocking fsync on the blocking pool — the request path
/// never blocks.
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

/// Sync `dir` (D1); a failure is warned, never fatal (F06 — see [`commit`]).
///
/// # Examples
///
/// ```rust
/// use tinio_util::fsutil::sync_dir_warned;
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// // A real directory syncs; a failure would be warned, never returned.
/// Runtime::new()
///     .unwrap()
///     .block_on(sync_dir_warned(dir.path()));
/// ```
pub async fn sync_dir_warned(dir: &Path) {
    if let Err(err) = sync_parent_dir(dir).await {
        tracing::warn!(path = %dir.display(), error = %err, "directory sync failed after a committed rename");
    }
}

/// Sync every ancestor of `leaf` up to and including `sync_root` (F03 — the
/// first commit into a new prefix created the chain). Each sync failure is
/// warned and the chain walk continues (the closest surviving entry is the
/// strongest durability the filesystem allows at that point; F06). Shared
/// with the fs backend's `rename_object` file move (the same
/// first-into-a-new-prefix durability).
///
/// With `sync_root` `None` the walk has no bound and climbs to the filesystem
/// root; a standalone caller that means "leaf directory only" takes
/// [`sync_dir_warned`] instead.
///
/// # Examples
///
/// ```rust
/// use std::fs;
///
/// use tinio_util::fsutil::sync_ancestor_chain;
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// let leaf = dir.path().join("a").join("b");
/// fs::create_dir_all(&leaf).unwrap();
/// // Syncs `b` and `a`, stopping at the bound; each failure is warned.
/// Runtime::new()
///     .unwrap()
///     .block_on(sync_ancestor_chain(&leaf, Some(dir.path())));
/// ```
pub async fn sync_ancestor_chain(leaf: &Path, sync_root: Option<&Path>) {
    let mut dir = Some(leaf);
    while let Some(d) = dir {
        sync_dir_warned(d).await;
        if Some(d) == sync_root {
            break;
        }
        dir = d.parent();
    }
}

/// Copy `temp` onto `target` via a unique staging file inside the target
/// directory's `.tinio/` reserved segment, then rename it (same volume —
/// atomic). The EXDEV fallback of [`commit`].
///
/// Staging inside the reserved segment keeps a crash residual **invisible to
/// the data plane** (FR-020: `.tinio` segments are never served or listed at
/// any depth); the staging file is removed best-effort on failure, and the
/// staging directory is removed when empty.
async fn copy_across_volumes(temp: &Path, target: &Path) -> io::Result<()> {
    let staging_dir = target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(RESERVED_SEGMENT);
    // Private residue, server-user-owned: the mode hardening only
    // (spec 2026-09-05 §5a — no chown; `.tinio` dirs stay the server
    // user's).
    ensure_dir(&staging_dir, DirOwnerUid::new(None)).await?;
    let staging = staging_dir.join(Uuid::new_v4().to_string());
    let copied = async {
        fs::copy(temp, &staging).await?;
        // D1 — the staging copy is what the rename makes visible; `copy`
        // does not sync. Sync its content before the name (the same
        // content-durability promise the write path's stage gives).
        // Write access: Windows `FlushFileBuffers` refuses read-only
        // handles (the staging file is private to this rename).
        File::options()
            .write(true)
            .open(&staging)
            .await?
            .sync_all()
            .await?;
        fs::rename(&staging, target).await?;
        Ok::<_, io::Error>(())
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

/// Atomically publish a staged temp file onto `target` (the commit protocol
/// every durable write is written on).
///
/// The steps are the durable-write pattern (D1) plus the F03 ancestor sync:
/// create the target's parent ([`ensure_dir`] — mode-hardened, and its
/// created flag drives the ancestor walk), rename the temp onto the target
/// (atomic on one volume), then fsync the parent directory (the rename is not
/// durable until its directory entry is). A failure before the rename leaves
/// the target untouched and removes the temp best-effort; a **post-rename**
/// sync failure is warned, never fatal (F06 — the object is visible and
/// correct, and both crash directions self-heal, while failing the request
/// would leave it visible without a success).
///
/// `sync_root` bounds the D1 ancestor sync (F03): the first commit into a NEW
/// prefix created the whole ancestor chain with `ensure_dir`, and only the
/// chain's directory entries — up to and including the caller's root — make
/// the rename's path durable. A caller with no such bound passes `None` and
/// gets the leaf parent's own sync (or, when this call created the parent,
/// the unbounded chain walk — see [`sync_ancestor_chain`]).
///
/// `dir_owner_uid` is the unix dir-hardening context of the parent creation
/// (spec 2026-09-05 §5a): a backend passes the bucket owner's mapped uid; a
/// private/standalone commit passes the default (`0700` only,
/// server-user-owned).
///
/// A cross-volume state dir (FR-023 relocation) makes `rename` fail with
/// `CrossesDevices` — the fallback copies the temp through a unique staging
/// file **on the target volume**, then renames (atomic there; readers still
/// see the old object or the new one, never a torn mix). The staging file
/// lives in the target directory's `.tinio/` reserved segment — a crash
/// between copy and rename leaves invisible residue (never served or listed,
/// FR-020), not a stray object.
///
/// # Examples
///
/// ```rust
/// use std::fs;
///
/// use tinio_util::fsutil::{DirOwnerUid, commit};
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// let temp = dir.path().join("staged");
/// fs::write(&temp, b"payload").unwrap();
/// let target = dir.path().join("sub").join("obj.bin");
///
/// Runtime::new()
///     .unwrap()
///     .block_on(commit(
///         &temp,
///         &target,
///         Some(dir.path()),
///         DirOwnerUid::new(None),
///     ))
///     .unwrap();
/// assert_eq!(fs::read(&target).unwrap(), b"payload");
/// assert!(!temp.exists(), "the rename consumed the staged temp");
/// ```
pub async fn commit(
    temp: &Path,
    target: &Path,
    sync_root: Option<&Path>,
    dir_owner_uid: DirOwnerUid,
) -> io::Result<()> {
    // Every commit target is `parent.join(name)` — a parent always exists
    // (a `None` here would silently skip the dir creation AND the durability
    // sync — unreachable in practice, pinned for debug builds).
    debug_assert!(
        target.parent().is_some(),
        "commit target without a parent: {target:?}"
    );
    let mut fallback = false;
    let mut created_parent = false;
    let result = async {
        if let Some(parent) = target.parent() {
            // One probe instead of `create_dir_all`'s per-component walk:
            // the parent exists in steady state; the create still runs when
            // the parent is missing (the first PUT into a new prefix), and
            // the created flag drives the ancestor-chain sync (F03).
            created_parent = ensure_dir(parent, dir_owner_uid).await?;
        }
        match fs::rename(temp, target).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::CrossesDevices => {
                fallback = true;
                copy_across_volumes(temp, target).await
            }
            Err(err) => Err(err),
        }
    }
    .await;
    // Remove the source temp only where it survives: on the rename success
    // path the temp is already gone — the old unconditional remove was a
    // per-PUT NotFound probe; on failure it is partial residue, and on the
    // EXDEV fallback success the copy did not consume it.
    if fallback || result.is_err() {
        let _ = fs::remove_file(temp).await;
    }
    // D1 — the rename is not durable until the directory entries are synced
    // (the content was synced before the rename). The rename is the COMMIT
    // POINT (F06): a post-rename sync failure does not fail the write.
    if let Ok(()) = result
        && let Some(parent) = target.parent()
    {
        if created_parent {
            // F03: the first commit into a new prefix — the whole ancestor
            // chain is new, and only the chain's directory entries make the
            // rename's path durable. Sync every ancestor up to (and
            // including) `sync_root`; the leaf alone would leave the
            // parent-chain entries unsynced and invert the durability
            // promise.
            sync_ancestor_chain(parent, sync_root).await;
        } else {
            sync_dir_warned(parent).await;
        }
    }
    result
}

/// Remove a staged temp file that was never committed, best-effort: a guard's
/// `Drop` cannot report a failure, and the startup repair / sweep is the
/// backstop for whatever this misses.
///
/// The unlink runs off the request thread when a runtime is present (item 7a
/// — the drop can land on an async thread, e.g. a rejected conditional PUT);
/// without a runtime (tests, sync contexts) the sync fallback keeps the
/// best-effort cleanup. `catch_unwind` guards the runtime teardown case: a
/// shutting-down runtime still reports a current handle, but `spawn_blocking`
/// then panics — a staged temp dropped in a cancelled request during teardown
/// must not panic inside `Drop`. The caught panic loses only the unlink.
///
/// The call is **detached**: it does not wait for the unlink to complete (the
/// task runs on the blocking pool and is dropped at the end of the call), so a
/// test asserting the file is gone must wait for it.
///
/// # Examples
///
/// ```rust
/// use std::fs;
///
/// use tinio_util::fsutil::remove_staged_temp;
///
/// let dir = tempfile::tempdir().unwrap();
/// let temp = dir.path().join("staged");
/// fs::write(&temp, b"x").unwrap();
/// remove_staged_temp(temp.clone());
/// assert!(!temp.exists(), "the temp is unlinked");
/// ```
pub fn remove_staged_temp(temp: PathBuf) {
    if Handle::try_current().is_ok() {
        // Detached on purpose: the blocking task runs the unlink off the
        // request thread (dropping the handle detaches).
        let _ = panic::catch_unwind(|| {
            mem::drop(task::spawn_blocking(move || remove_file(&temp)));
        });
    } else {
        let _ = remove_file(&temp);
    }
}

/// What a read that reaches the end of the file before the requested range is
/// met means. The two backends genuinely differ here, so the policy is the
/// caller's and the chunking loop is shared:
///
/// - [`ShortRead::EndBody`] is the fs backend's served-object rule — the body
///   ends where the file ends (the range is treated as clamped);
/// - [`ShortRead::Error`] is the block store's rule — the range was resolved
///   against this same file's own length, so a shortfall is truncation or
///   corruption, never a normal end of the block.
///
/// # Examples
///
/// ```rust
/// use std::io::ErrorKind;
///
/// use tinio_core::storage::collect_body;
/// use tinio_util::fsutil::{ShortRead, file_stream};
/// use tokio::{fs::File, runtime::Runtime};
///
/// let dir = tempfile::tempdir().unwrap();
/// let path = dir.path().join("body.bin");
/// std::fs::write(&path, b"abc").unwrap();
///
/// let served = Runtime::new().unwrap().block_on(async {
///     let file = File::open(&path).await.unwrap();
///     // The file holds three bytes; asking for five ends the body after them.
///     collect_body(file_stream(file, 0, 4, ShortRead::EndBody).await)
///         .await
///         .unwrap()
/// });
/// assert_eq!(served, b"abc");
///
/// let err = Runtime::new().unwrap().block_on(async {
///     let file = File::open(&path).await.unwrap();
///     collect_body(file_stream(file, 0, 4, ShortRead::Error).await)
///         .await
///         .unwrap_err()
/// });
/// assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortRead {
    /// The body ends at the file's end — the bytes the file holds are served
    /// and the stream finishes normally.
    EndBody,
    /// The body fails with `UnexpectedEof` — the requested range was resolved
    /// against this file's own length, so a shortfall means the file was
    /// truncated under the reader.
    Error,
}

/// Stream the inclusive byte range `start..=end` of an open file in bounded
/// chunks (constitution V: no per-object buffering). The stream state holds
/// one [`BytesMut`]: each chunk is read into spare capacity via `read_buf` and
/// handed off via `split().freeze()` — the frozen `Bytes` owns its memory, so
/// reusing the buffer never invalidates a chunk the consumer still holds. The
/// read is not fully zero-copy: tokio's `File` reads into an internal buffer on
/// the blocking pool and copies once into the caller's buffer (`Buf::read_from`
/// then `copy_to`, tokio `src/fs/file.rs` / `src/io/blocking.rs`); a true
/// zero-copy read would need `File`, a manual `spawn_blocking` and
/// `Read::read_buf` (out of scope).
///
/// A short read (the file ends before `end`) follows `short_read` — see
/// [`ShortRead`] for why the policy is the caller's. A seek failure is
/// answered as a one-item error stream, so the caller always gets a body.
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage::collect_body;
/// use tinio_util::fsutil::{ShortRead, file_stream};
/// use tokio::{fs::File, runtime::Runtime};
///
/// let dir = tempfile::tempdir().unwrap();
/// let path = dir.path().join("body.bin");
/// std::fs::write(&path, b"hello world").unwrap();
///
/// let served = Runtime::new().unwrap().block_on(async {
///     let file = File::open(&path).await.unwrap();
///     collect_body(file_stream(file, 6, 10, ShortRead::EndBody).await)
///         .await
///         .unwrap()
/// });
/// assert_eq!(served, b"world");
/// ```
pub async fn file_stream(file: File, start: u64, end: u64, short_read: ShortRead) -> BodyStream {
    let mut file = file;
    if let Err(err) = file.seek(SeekFrom::Start(start)).await {
        return Box::pin(stream::once(async move { Err(err) }));
    }
    let remaining = end.saturating_sub(start) + 1;
    Box::pin(stream::try_unfold(
        (file, remaining, BytesMut::with_capacity(CHUNK_SIZE)),
        move |(mut file, remaining, mut buf)| async move {
            if remaining == 0 {
                return Ok(None);
            }
            let want = remaining.min(CHUNK_SIZE as u64) as usize;
            // The previous chunk's `split` moved the filled prefix out of
            // `buf`, so make room for a full `want` again: when the consumer
            // already released the frozen chunk the reserve reclaims the
            // tail; a consumer still holding it forces a fresh allocation
            // (inherent to ownership handoff — transiently two live buffers).
            buf.reserve(want);
            // Cap the read at `want`: `read_buf` would otherwise fill the
            // whole spare capacity and overrun the range end.
            let mut limited = (&mut buf).limit(want);
            match file.read_buf(&mut limited).await {
                Ok(0) => match short_read {
                    ShortRead::EndBody => Ok(None),
                    ShortRead::Error => Err(io::Error::from(ErrorKind::UnexpectedEof)),
                },
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

/// The dir-hardening context of a directory creation (spec 2026-09-05 §5a):
/// the uid every newly created component chowns to (`None` = leave
/// server-user-owned, still `0700` — the private-state dirs and unmapped
/// owners). Unix-only in effect; off-unix the value is inert (the creating
/// user's platform ACLs isolate) but the type keeps the call sites uniform.
///
/// # Examples
///
/// ```rust
/// use tinio_util::fsutil::DirOwnerUid;
///
/// // The private-state context: mode hardening only, no chown.
/// let _owner = DirOwnerUid::new(None);
/// ```
#[cfg(unix)]
#[derive(Debug, Clone, Copy)]
pub struct DirOwnerUid(pub Option<u32>);

/// The dir-hardening context of a directory creation (spec 2026-09-05 §5a):
/// the uid every newly created component chowns to (`None` = leave
/// server-user-owned, still `0700` — the private-state dirs and unmapped
/// owners). Unix-only in effect; off-unix the value is inert (the creating
/// user's platform ACLs isolate) but the type keeps the call sites uniform.
///
/// # Examples
///
/// ```rust
/// use tinio_util::fsutil::DirOwnerUid;
///
/// // The private-state context: mode hardening only, no chown.
/// let _owner = DirOwnerUid::new(None);
/// ```
#[cfg(not(unix))]
#[derive(Debug, Clone, Copy)]
pub struct DirOwnerUid;

impl DirOwnerUid {
    /// The dir-hardening context: the uid every newly created component
    /// chowns to (`None` = server-user-owned, still `0700` — the
    /// private-state dirs and unmapped owners). Inert off-unix.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_util::fsutil::DirOwnerUid;
    ///
    /// let owner = DirOwnerUid::new(None);
    /// let _ = owner;
    /// ```
    pub fn new(owner_uid: Option<u32>) -> Self {
        #[cfg(unix)]
        {
            Self(owner_uid)
        }
        #[cfg(not(unix))]
        {
            let _ = owner_uid;
            Self
        }
    }
}

/// Ensure `dir` exists — one probe instead of the per-component walk of
/// `create_dir_all` (the backends' directory idiom: the dir exists in steady
/// state — the write tmp dir, a PUT's parent chain, the tombstone staging
/// dir). Returns whether it was created (the commit path's ancestor-chain
/// sync decision, F03).
///
/// Unix hardening (spec 2026-09-05 §5a): every created component lands `0700`
/// and — when `owner_uid` carries a mapped uid — chowned to it (an implicitly
/// created bucket/prefix dir takes the bucket owner's uid; private-state dirs
/// pass `None` and stay server-user-owned). A pre-existing component is never
/// chmodded or chowned. FAIL-CLOSED (review round 1): a failure hardening a
/// NEW component — the create, the mode set, the chown, or the chown task
/// itself — removes the components THIS call created (deepest first, so each
/// removal finds an empty dir) and errors the request. A wrongly-owned `0700`
/// prefix dir must not be left behind with the write reporting success: it
/// would permanently strand the mapped owner's own `0600` objects (no
/// retroactive chmod, no self-heal). An unmapped owner performs no chown, so
/// no failure path fires; EPERM-at-startup is the backend's concern, not a
/// runtime carve-out.
///
/// # Examples
///
/// ```rust
/// use tinio_util::fsutil::{DirOwnerUid, ensure_dir};
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// let fresh = dir.path().join("a").join("b");
///
/// let runtime = Runtime::new().unwrap();
/// assert!(
///     runtime
///         .block_on(ensure_dir(&fresh, DirOwnerUid::new(None)))
///         .unwrap()
/// );
/// assert!(fresh.is_dir());
/// // The probe is what makes a second call a no-op.
/// assert!(
///     !runtime
///         .block_on(ensure_dir(&fresh, DirOwnerUid::new(None)))
///         .unwrap()
/// );
/// ```
pub async fn ensure_dir(dir: &Path, owner_uid: DirOwnerUid) -> io::Result<bool> {
    match fs::try_exists(dir).await {
        Ok(true) => Ok(false),
        Ok(false) => {
            #[cfg(unix)]
            ensure_dir_unix(dir, owner_uid).await?;
            #[cfg(not(unix))]
            {
                let _ = owner_uid;
                fs::create_dir_all(dir).await?;
            }
            Ok(true)
        }
        Err(err) => Err(err),
    }
}

/// The unix creation tail of [`ensure_dir`]: create each missing component
/// (deepest ancestor first) at `0700`, then chown it to the mapped uid when
/// one was passed. Fail-closed — see [`ensure_dir`].
#[cfg(unix)]
async fn ensure_dir_unix(dir: &Path, owner_uid: DirOwnerUid) -> io::Result<()> {
    use std::{
        fs::Permissions,
        os::unix::fs::{PermissionsExt, chown},
    };

    // The missing chain, deepest ancestor first — the caller proved `dir`
    // absent; every component up to the first existing ancestor lands private
    // and chowned.
    let mut missing: Vec<PathBuf> = vec![dir.to_path_buf()];
    let mut current = dir;
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() || fs::try_exists(parent).await? {
            break;
        }
        missing.push(parent.to_path_buf());
        current = parent;
    }
    // The components THIS call created, in creation order — the fail-closed
    // cleanup removes them deepest-first on any hardening failure.
    // Pre-existing components and a concurrent creator's dir
    // (`AlreadyExists`) are never touched.
    let mut created: Vec<PathBuf> = Vec::with_capacity(missing.len());
    let mut outcome: io::Result<()> = Ok(());
    for component in missing.iter().rev() {
        match fs::create_dir(component).await {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                // A concurrent creator won the race — its dir counts as
                // pre-existing: untouched.
                continue;
            }
            Err(err) => {
                outcome = Err(err);
                break;
            }
        }
        created.push(component.clone());
        if let Err(err) =
            fs::set_permissions(component, Permissions::from_mode(DIR_MODE_PRIVATE)).await
        {
            outcome = Err(err);
            break;
        }
        if let Some(uid) = owner_uid.0 {
            let path = component.clone();
            let chowned = task::spawn_blocking(move || chown(&path, Some(uid), None)).await;
            match chowned {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    outcome = Err(err);
                    break;
                }
                Err(join) => {
                    outcome = Err(io::Error::other(format!("chown task join failed: {join}")));
                    break;
                }
            }
        }
    }
    if let Err(err) = outcome {
        // Deepest first — each created dir is still empty, so the plain rmdir
        // removes the chain. A concurrent interference (a racing creator's
        // entry) makes the removal fail harmlessly; the original error is what
        // the caller reports.
        for component in created.iter().rev() {
            let _ = fs::remove_dir(component).await;
        }
        return Err(err);
    }
    Ok(())
}

/// A removal is idempotent: a missing target is success, any other failure
/// propagates.
///
/// # Examples
///
/// ```rust
/// use tinio_util::fsutil::ok_if_missing;
///
/// let dir = tempfile::tempdir().unwrap();
/// assert!(ok_if_missing(std::fs::remove_file(dir.path().join("gone"))).is_ok());
/// ```
pub fn ok_if_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// The entries of `dir` (a missing directory is empty), as `(path, name)`
/// pairs. Shared by the startup repair, the sweep, the tombstone leftover
/// enumeration and the block store's fan-out walk.
///
/// # Examples
///
/// ```rust
/// use std::fs;
///
/// use tinio_util::fsutil::entries_of;
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// fs::write(dir.path().join("a.txt"), b"1").unwrap();
///
/// let runtime = Runtime::new().unwrap();
/// let entries = runtime.block_on(entries_of(dir.path())).unwrap();
/// assert_eq!(entries[0].1, "a.txt");
/// // A missing directory reads as empty, never as an error.
/// assert!(
///     runtime
///         .block_on(entries_of(&dir.path().join("nope")))
///         .unwrap()
///         .is_empty()
/// );
/// ```
pub async fn entries_of(dir: &Path) -> io::Result<Vec<(PathBuf, String)>> {
    let mut out = Vec::new();
    let mut entries = match fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err),
    };
    while let Some(entry) = entries.next_entry().await? {
        out.push((
            entry.path(),
            entry.file_name().to_string_lossy().into_owned(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::{fs::read_dir, time::Duration};

    use tokio::time::timeout;

    use super::*;
    use crate::_core::storage::collect_body;

    /// The parent-dir fsync of a real directory succeeds (D1 — the durability
    /// step behind every committed object). Moved here with the function it
    /// tests.
    #[cfg(unix)]
    #[tokio::test]
    async fn sync_parent_dir_syncs_a_real_directory() {
        let dir = tempfile::tempdir().unwrap();
        sync_parent_dir(dir.path()).await.unwrap();
    }

    /// Moved here with the function it tests: the EXDEV fallback lands the
    /// content atomically, leaves no staging residue on success, and cleans up
    /// after a failed copy.
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
        assert_eq!(err.kind(), ErrorKind::NotFound, "{err:?}");
        assert!(!target2.parent().unwrap().join(".tinio").exists());
    }

    #[tokio::test]
    async fn ensure_dir_creates_the_chain_and_reports_it() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("a").join("b");
        assert!(ensure_dir(&fresh, DirOwnerUid::new(None)).await.unwrap());
        assert!(fresh.is_dir());
        // The steady-state probe: a second call creates nothing.
        assert!(!ensure_dir(&fresh, DirOwnerUid::new(None)).await.unwrap());
        // The created components land private regardless of the umask
        // (spec 2026-09-05 §5a).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = fs::metadata(&fresh).await.unwrap().permissions().mode();
            assert_eq!(mode & 0o777, DIR_MODE_PRIVATE);
        }
    }

    #[tokio::test]
    async fn ok_if_missing_tolerates_a_missing_target() {
        let dir = tempfile::tempdir().unwrap();
        // A missing target is success; a present one is removed.
        assert!(ok_if_missing(fs::remove_file(dir.path().join("gone")).await).is_ok());
        let file = dir.path().join("f");
        fs::write(&file, b"x").await.unwrap();
        assert!(ok_if_missing(fs::remove_file(&file).await).is_ok());
    }

    #[tokio::test]
    async fn entries_of_reads_a_missing_directory_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            entries_of(&dir.path().join("nope"))
                .await
                .unwrap()
                .is_empty()
        );
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).await.unwrap();
        fs::write(sub.join("a.txt"), b"1").await.unwrap();
        let out = entries_of(&sub).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, sub.join("a.txt"));
        assert_eq!(out[0].1, "a.txt");
    }

    #[tokio::test]
    async fn file_stream_serves_an_inclusive_range_in_bounded_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("body.bin");
        let payload: Vec<u8> = (0..(CHUNK_SIZE * 2 + 7)).map(|i| (i % 251) as u8).collect();
        fs::write(&path, &payload).await.unwrap();

        let file = File::open(&path).await.unwrap();
        let whole =
            collect_body(file_stream(file, 0, payload.len() as u64 - 1, ShortRead::EndBody).await)
                .await
                .unwrap();
        assert_eq!(whole, payload);
        // A range spanning the chunk boundary serves exactly its own bytes.
        let file = File::open(&path).await.unwrap();
        let across =
            collect_body(file_stream(file, 1, CHUNK_SIZE as u64 + 1, ShortRead::EndBody).await)
                .await
                .unwrap();
        assert_eq!(across, &payload[1..CHUNK_SIZE + 2]);
    }

    #[tokio::test]
    async fn a_short_file_ends_the_body_or_fails_according_to_the_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("body.bin");
        fs::write(&path, b"abc").await.unwrap();

        let file = File::open(&path).await.unwrap();
        let served = collect_body(file_stream(file, 0, 4, ShortRead::EndBody).await)
            .await
            .unwrap();
        assert_eq!(served, b"abc", "the body ends where the file ends");

        let file = File::open(&path).await.unwrap();
        let err = collect_body(file_stream(file, 0, 4, ShortRead::Error).await)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn a_staged_temp_removes_off_the_worker() {
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path().join("staged");
        fs::write(&temp, b"x").await.unwrap();
        remove_staged_temp(temp.clone());
        // The unlink runs on the blocking pool; wait for it (the call is
        // detached by design).
        timeout(Duration::from_secs(5), async {
            while fs::try_exists(&temp).await.unwrap() {
                task::yield_now().await;
            }
        })
        .await
        .expect("the staged temp's removal");
    }

    #[tokio::test]
    async fn commit_publishes_and_removes_the_staged_temp() {
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path().join("staged");
        fs::write(&temp, b"payload").await.unwrap();
        let target = dir.path().join("sub").join("deep").join("obj.bin");
        commit(&temp, &target, Some(dir.path()), DirOwnerUid::new(None))
            .await
            .unwrap();
        assert_eq!(fs::read(&target).await.unwrap(), b"payload");
        assert!(!fs::try_exists(&temp).await.unwrap());
        // A commit that cannot land removes its temp and errors.
        let blocked = dir.path().join("existing-dir");
        fs::create_dir(&blocked).await.unwrap();
        let stranded = dir.path().join("stranded");
        fs::write(&stranded, b"x").await.unwrap();
        assert!(
            commit(
                &stranded,
                &blocked,
                Some(dir.path()),
                DirOwnerUid::new(None)
            )
            .await
            .is_err()
        );
        assert!(!fs::try_exists(&stranded).await.unwrap());
    }
}
