//! Small filesystem helpers shared across the store modules.

use std::{
    fs::{Metadata, OpenOptions},
    io,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};

use crate::{error::Error, path::TMP_DIR_NAME};

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
pub(crate) async fn open_file(path: &Path, follow_symlinks: bool) -> io::Result<tokio::fs::File> {
    if follow_symlinks {
        return tokio::fs::File::open(path).await;
    }
    let path = path.to_path_buf();
    let std_file = tokio::task::spawn_blocking(move || open_nofollow_std(&path))
        .await
        .map_err(io::Error::other)??;
    Ok(tokio::fs::File::from_std(std_file))
}

/// `lstat` when not following, `stat` when following.
pub(crate) async fn object_metadata(
    path: &Path,
    follow_symlinks: bool,
) -> io::Result<std::fs::Metadata> {
    if follow_symlinks {
        tokio::fs::metadata(path).await
    } else {
        tokio::fs::symlink_metadata(path).await
    }
}

fn open_nofollow_std(path: &Path) -> io::Result<std::fs::File> {
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
    let file = opts.open(path)?;
    if is_symlink_or_reparse(&file.metadata()?) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "symlink",
        ));
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

/// A removal is idempotent: a missing target is success, any other
/// failure propagates.
pub(crate) fn ok_if_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// The entries of `<state-dir>/tmp/` (a missing directory is empty), as
/// `(path, name)` pairs. Shared by the startup repair and the sweep.
pub(crate) async fn tmp_entries(state_dir: &Path) -> Result<Vec<(PathBuf, String)>, Error> {
    let tmp = state_dir.join(TMP_DIR_NAME);
    let mut out = Vec::new();
    let mut entries = match tokio::fs::read_dir(&tmp).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(out),
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
