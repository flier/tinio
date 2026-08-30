//! Small filesystem helpers shared across the store modules.

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::{
    fs::{File, Metadata, OpenOptions},
    io::{self, Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    time::SystemTime,
};

use md5::Md5;
#[cfg(unix)]
use rustix::fs as rustix_fs;
#[cfg(unix)]
use rustix::io::Errno;
use tokio::{fs, fs::File as TokioFile, io::AsyncRead, task::spawn_blocking};
#[cfg(windows)]
use winapi_util::file;

use crate::error::Error;

#[cfg(windows)]
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;

/// True for a symlink, and on Windows also for junctions / other reparse
/// points (`is_symlink()` is only `IO_REPARSE_TAG_SYMLINK`).
pub(crate) fn is_symlink_or_reparse(metadata: &Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

/// Open `path` for reading. When `follow_symlinks` is false, the leaf
/// is opened with `O_NOFOLLOW` / `FILE_FLAG_OPEN_REPARSE_POINT` so a
/// TOCTOU swap to a symlink cannot escape the storage root.
pub(crate) async fn open_file(path: &Path, follow_symlinks: bool) -> io::Result<fs::File> {
    if follow_symlinks {
        return TokioFile::open(path).await;
    }
    let path = path.to_path_buf();
    let std_file = spawn_blocking(move || open_nofollow_std(&path))
        .await
        .map_err(IoError::other)??;
    Ok(TokioFile::from_std(std_file))
}

/// `lstat` when not following, `stat` when following.
pub(crate) async fn object_metadata(path: &Path, follow_symlinks: bool) -> io::Result<Metadata> {
    if follow_symlinks {
        fs::metadata(path).await
    } else {
        fs::symlink_metadata(path).await
    }
}

/// Open `path` for reading with the leaf open through the symlink policy
/// (blocking): `O_NOFOLLOW` / `FILE_FLAG_OPEN_REPARSE_POINT` so a TOCTOU
/// swap to a symlink cannot escape the storage root, then reject the
/// handle when it still resolves to a link. The blocking half of
/// [`open_file`]; also the [`crate::etag::ComputeTask`] open
/// (R3 — one open serves the hash and the file identity).
pub(crate) fn open_nofollow_std(path: &Path) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        opts.custom_flags(o_nofollow());
    }
    #[cfg(windows)]
    {
        // FILE_FLAG_OPEN_REPARSE_POINT: open the reparse point itself.
        opts.custom_flags(0x0020_0000);
    }
    let file = match opts.open(path) {
        // O_NOFOLLOW rejects the link at open time on unix (ELOOP) —
        // normalized to the documented `PermissionDenied` so both
        // platforms answer the rejection the same way (R3). The original
        // ELOOP is preserved as the error's source (F24), so an
        // intermediate-chain loop (an out-of-band symlink chain beyond
        // the kernel resolution limit) is distinguishable from a leaf
        // O_NOFOLLOW rejection through the error chain. The raw OS error
        // is consulted directly — `ErrorKind::FilesystemLoop` is not
        // stable on every toolchain.
        #[cfg(unix)]
        Err(err) if err.raw_os_error() == Some(Errno::LOOP.raw_os_error()) => {
            return Err(IoError::new(ErrorKind::PermissionDenied, err));
        }
        result => result?,
    };
    // The post-open check is Windows-only (data-path review 2026-08-29,
    // finding 5): on unix O_NOFOLLOW already guarantees the opened file
    // is not a symlink — a post-open metadata() would be one dead syscall
    // per hashed file. On Windows FILE_FLAG_OPEN_REPARSE_POINT *succeeds*
    // on a link, so this check is the only thing that rejects it here.
    #[cfg(windows)]
    if is_symlink_or_reparse(&file.metadata()?) {
        return Err(IoError::new(ErrorKind::PermissionDenied, "symlink"));
    }
    Ok(file)
}

#[cfg(unix)]
const fn o_nofollow() -> i32 {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        0x20000
    }
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    {
        0x0100
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    )))]
    {
        0
    }
}

/// The latest part-file mtime of an upload directory (`None` when it
/// holds no part files — the callers apply their own fallback). Shared by
/// the sweep's idle computation and the cleanup orphan stage.
pub(crate) async fn latest_part_mtime(dir: &Path) -> io::Result<Option<SystemTime>> {
    let mut latest = SystemTime::UNIX_EPOCH;
    let mut found = false;
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("part-") {
            continue;
        }
        if let Ok(metadata) = fs::metadata(entry.path()).await
            && let Ok(modified) = metadata.modified()
            && modified > latest
        {
            latest = modified;
            found = true;
        }
    }
    Ok(found.then_some(latest))
}

/// The stable identity of a file: unix dev+inode; Windows volume serial
/// combined with file index (`GetFileInformationByHandle` — std's
/// `MetadataExt` equivalents are nightly-gated). `0` where the platform
/// exposes none (a filesystem without file IDs — the composed-ETag mtime
/// jitter window is the fallback there, meta.rs). A touch (antivirus,
/// indexer) keeps it; a replacement (a new file renamed over) changes it
/// — the exact signal for distinguishing mtime jitter from an
/// out-of-band rewrite (meta-redb-spec §5.5).
pub(crate) fn file_identity(path: &Path, metadata: &Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = path;
        metadata.dev() ^ metadata.ino().rotate_left(32)
    }
    #[cfg(windows)]
    {
        // The identity comes from the open handle, not the metadata —
        // open the file by path (a `0` on any error falls back to the
        // jitter window).
        let _ = metadata;
        windows_file_identity(path).unwrap_or(0)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, metadata);
        0
    }
}

/// The stable identity of an **already-open** file (R3 — the identity
/// comes from the handle that was opened under the symlink policy, never
/// a second path-based open). Same rules as [`file_identity`] — unix
/// dev+inode from `metadata`, Windows volume serial + file index from
/// the handle. The async sites use the [`file_identity_async`] bridge;
/// the remaining path-based [`file_identity`] callers are the walks and
/// post-commit stats, which have no handle in hand.
pub(crate) fn file_identity_handle(file: &File, metadata: &Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = file;
        metadata.dev() ^ metadata.ino().rotate_left(32)
    }
    #[cfg(windows)]
    {
        let _ = metadata;
        windows_handle_identity(file).unwrap_or(0)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, metadata);
        0
    }
}

/// The async bridge of [`file_identity_handle`]: the identity of an
/// already-open **tokio** file. On unix the identity comes from the
/// metadata alone (dev+inode — the handle adds nothing); on Windows the
/// handle is the identity source — `try_clone` + `into_std` bridges to
/// [`file_identity_handle`] without consuming the caller's file.
pub(crate) async fn file_identity_async(file: &mut fs::File, metadata: &Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = file;
        metadata.dev() ^ metadata.ino().rotate_left(32)
    }
    #[cfg(windows)]
    {
        // `into_std` takes ownership — clone the handle so the caller's
        // tokio file stays open for the next read.
        let Ok(cloned) = file.try_clone().await else {
            return 0;
        };
        let std_file = cloned.into_std().await;
        file_identity_handle(&std_file, metadata)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, metadata);
        0
    }
}

/// The Windows file identity: volume serial + file index from
/// `GetFileInformationByHandle` (stable Win32, via the safe `winapi-util`
/// wrapper). `0` on filesystems without a file ID (FAT/exFAT).
#[cfg(windows)]
fn windows_file_identity(path: &Path) -> io::Result<u64> {
    let file = File::open(path)?;
    windows_handle_identity(&file)
}

/// The Windows identity of an already-open handle (one open serves the
/// symlink policy, the hash, and the identity — R3).
#[cfg(windows)]
fn windows_handle_identity(file: &File) -> io::Result<u64> {
    let info = file::information(file)?;
    let volume = info.volume_serial_number();
    let index = info.file_index();
    Ok(if volume == 0 && index == 0 {
        0
    } else {
        volume ^ index.rotate_left(32)
    })
}

/// Whether `path` is absent — the exact "object file / bucket dir no
/// longer exists" test of the reclaim paths (scanner orphan reclamation,
/// stale-bucket pruning). `NotFound` answers `true`; **any other error
/// (EACCES, EIO, …) propagates** — an IO error must never be treated as
/// "gone", or a live object whose path is temporarily unreadable would
/// have its meta row (or its bucket's whole derived state) removed (F11).
pub(crate) async fn is_absent(path: &Path) -> io::Result<bool> {
    match fs::try_exists(path).await {
        Ok(exists) => Ok(!exists),
        Err(err) => Err(err),
    }
}

/// Copy `len` bytes at `offset` of `src` into `dst` with the kernel's
/// `copy_file_range` (unix; zero userspace buffering — the copy
/// primitives' fast path, E3). Copies exactly `len` bytes: a short
/// kernel copy is retried from where it stopped, and a zero-progress
/// round is an unexpected EOF (the source shrank mid-copy — the callers'
/// torn guard then treats the staged bytes as self-consistent).
#[cfg(unix)]
pub(crate) fn copy_file_range(src: &File, offset: u64, len: u64, dst: &File) -> io::Result<()> {
    let mut off = offset;
    let mut remaining = len;
    while remaining > 0 {
        let copied = rustix_fs::copy_file_range(src, Some(&mut off), dst, None, remaining as usize)
            .map_err(IoError::from)?;
        if copied == 0 {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "copy_file_range made no progress",
            ));
        }
        remaining -= copied as u64;
    }
    Ok(())
}

/// The sync streaming content MD5 of a `Read` (F43) — the blocking
/// counterpart of [`md5_stream_async`], used where the hash already
/// runs on a blocking thread (`write.rs`'s `stage_copy` copy closure).
pub(crate) fn md5_stream<R: io::Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<[u8; 16]> {
    use md5::Digest;
    let mut hasher = Md5::new();
    loop {
        let n = reader.read(buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

/// The async streaming content MD5 of a tokio reader (F43) — the etag
/// compute and the atomic-write staging path ([`md5_stream`] is the
/// sync counterpart for blocking contexts). The multipart assembly
/// deliberately does NOT use it — its copy+hash loop is fused by design
/// (the part bytes must stream into the output file).
pub(crate) async fn md5_stream_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> io::Result<[u8; 16]> {
    use md5::Digest;
    use tokio::io::AsyncReadExt;
    let mut hasher = Md5::new();
    loop {
        let n = reader.read(buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

/// A removal is idempotent: a missing target is success, any other
/// failure propagates.
pub(crate) fn ok_if_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Remove a directory tree — or a file if `path` is not a directory —
/// treating a missing target as success. The async home of the rule
/// (the IO runs on the tokio blocking pool via `tokio::fs`); shared by
/// the removal pipeline's [`RemoveTask`](crate::tombstone::RemoveTask)
/// (which runs on the removal-pipeline runtime) and the cleanup stages.
pub(crate) async fn remove_tree(path: &Path) -> io::Result<()> {
    match ok_if_missing(fs::remove_dir_all(path).await) {
        Ok(()) => Ok(()),
        // A `.tinio` *file* planted out-of-band: remove as a file.
        Err(err) if err.kind() == ErrorKind::NotADirectory => {
            ok_if_missing(fs::remove_file(path).await)
        }
        Err(err) => Err(err),
    }
}

/// The entries of `dir` (a missing directory is empty), as `(path, name)`
/// pairs. Shared by the startup repair, the sweep, and the tombstone
/// leftover enumeration.
pub(crate) async fn entries_of(dir: &Path) -> Result<Vec<(PathBuf, String)>, Error> {
    let mut out = Vec::new();
    let mut entries = match fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err.into()),
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
    use std::{
        fs::{File, metadata, rename, write},
        time::{Duration, SystemTime},
    };

    use super::*;

    #[cfg(windows)]
    #[test]
    fn windows_identity_distinguishes_replacement_from_touch() {
        // The composed-ETag refresh (meta.rs) depends on the exact
        // signal: an identity that never changes would serve the stale
        // ETag forever; one that changes on every touch would rewrite it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obj.bin");
        write(&path, b"original").unwrap();
        let first = file_identity(&path, &metadata(&path).unwrap());
        assert_ne!(first, 0, "NTFS must expose a file identity");
        // A touch (mtime rewrite) keeps the identity.
        let file = File::options().write(true).open(&path).unwrap();
        file.set_modified(SystemTime::now() + Duration::from_secs(5))
            .unwrap();
        drop(file);
        assert_eq!(first, file_identity(&path, &metadata(&path).unwrap()));
        // A replacement (a new file renamed over) changes it.
        let fresh = dir.path().join("fresh.bin");
        write(&fresh, b"replacement").unwrap();
        rename(&fresh, &path).unwrap();
        let replaced = file_identity(&path, &metadata(&path).unwrap());
        assert_ne!(first, replaced, "a replacement must change the identity");
    }
}
