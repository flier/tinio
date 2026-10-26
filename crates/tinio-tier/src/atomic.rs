//! The layer's owner-only atomic file writes.
//!
//! The marker, a quarantine pair and the index file are all private state: each
//! is created owner-only, a write lands in a sibling temporary file that is
//! fsynced and renamed into place, and a rename is followed by a directory
//! fsync so the entry is durable. One home keeps the mode and the durability
//! order from drifting between the callers.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    path::Path,
};

/// Create `path` (truncating it) and open it for writing, owner-only on Unix.
///
/// The marker and the quarantine pairs are written through this so neither
/// ever exists with a wider mode (T17).
#[cfg(unix)]
pub(crate) fn create_private(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
}

/// See the Unix arm: the platform provides no POSIX mode, exactly as the
/// marker and the index file were created before.
#[cfg(not(unix))]
pub(crate) fn create_private(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
}

/// Open `path` for appending, creating it owner-only on Unix, so the index file
/// never exists with a wider mode (T17).
///
/// The platform provides no POSIX mode; the format gate and redb's own open
/// flags protect the file there, exactly as before.
#[cfg(unix)]
pub(crate) fn append_private(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
}

/// Write `bytes` to a fresh `path` and fsync it.
pub(crate) fn write_and_sync(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = create_private(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Write `bytes` to `temp`, fsync it, rename it over `path` in the same
/// directory, and fsync that directory.
///
/// The destination's own inode is never truncated, so a crash leaves either the
/// old content or the new, never a partial record.
pub(crate) fn write_atomic(path: &Path, temp: &Path, bytes: &[u8]) -> io::Result<()> {
    write_and_sync(temp, bytes)?;
    fs::rename(temp, path)?;
    sync_parent(path)
}

/// One half of an atomic pair: the destination the rename lands on, the
/// temporary sibling its bytes are written to, and the bytes themselves.
///
/// The named fields keep a pair's order readable at the call site — the
/// destination and the temporary cannot be swapped the way two bare triples
/// could.
pub(crate) struct PairHalf<'a> {
    /// The path the pair's reader looks for.
    pub(crate) destination: &'a Path,
    /// The sibling temporary the bytes are written to and fsynced through.
    pub(crate) temp: &'a Path,
    /// The record's bytes.
    pub(crate) bytes: &'a [u8],
}

/// Write both halves of a pair through their temporary siblings, rename each
/// into place in order, and fsync the first path's directory once both
/// renames landed.
///
/// The first half is renamed before the second, so a crash between the two
/// leaves a half the pair's reader ignores.
pub(crate) fn write_atomic_pair(first: PairHalf<'_>, second: PairHalf<'_>) -> io::Result<()> {
    write_and_sync(first.temp, first.bytes)?;
    write_and_sync(second.temp, second.bytes)?;
    fs::rename(first.temp, first.destination)?;
    fs::rename(second.temp, second.destination)?;
    sync_parent(first.destination)
}

/// Sync `path`'s directory on Unix so its entries are durable; the platform
/// provides no portable directory sync.
#[cfg(unix)]
pub(crate) fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// See the Unix arm.
#[cfg(not(unix))]
pub(crate) fn sync_parent(_path: &Path) -> io::Result<()> {
    Ok(())
}
