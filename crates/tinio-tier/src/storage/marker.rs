//! The root marker and the index openers that bind a root to its identity
//! (T17).

use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use tinio_util::fsutil::ok_if_missing;

use super::{ROOT_ID_LEN, index_path, marker_path, path_present};
use crate::{_fs, Error, Index, atomic};

/// The root identity a construction records, and whether this run created the
/// marker carrying it.
pub(super) struct Marker {
    /// The identity the marker records.
    id: [u8; ROOT_ID_LEN],
    /// Whether this construction wrote the marker.
    fresh: bool,
}

/// The marker's recorded identity, or `None` when no marker exists.
///
/// A marker that exists but cannot be read is an error, never "absent": the
/// marker is the root's first record, and silently replacing it could bind the
/// root to a different identity (T17).
pub(super) fn read_marker(path: &Path) -> Result<Option<[u8; ROOT_ID_LEN]>, Error> {
    match fs::read(path) {
        Ok(bytes) => {
            let id = <[u8; ROOT_ID_LEN]>::try_from(bytes.as_slice()).map_err(|_| {
                Error::CorruptMarker {
                    path: path.to_path_buf(),
                    len: bytes.len() as u64,
                }
            })?;
            Ok(Some(id))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(Error::Io(err)),
    }
}

/// The temporary file a marker write lands in before its rename.
///
/// The name is fixed because the marker is written only by a construction: a
/// leftover is a crash's, is ignored by the next read (which opens the marker's
/// own name), and is removed by the next writable open.
pub(super) const MARKER_TEMP: &str = "tier.mode.tmp";

/// Record the root identity in the marker before any index is opened (T17).
///
/// An existing marker is reused as it is. A fresh root reuses the identity an
/// older index records when there is one, and mints a UUID otherwise. The file
/// is written once — through a temporary file renamed into place, owner-only,
/// and synced with its directory — so it survives the crash that follows it,
/// before the index exists, and a crash mid-write never leaves a truncated
/// marker.
pub(super) fn record_marker(root: &Path) -> Result<Marker, Error> {
    let path = marker_path(root);
    remove_marker_temp(&path)?;
    if let Some(id) = read_marker(&path)? {
        return Ok(Marker { id, fresh: false });
    }
    let id = match index_identity_if_present(&index_path(root))? {
        Some(recorded) => recorded,
        None => *uuid::Uuid::new_v4().as_bytes(),
    };
    write_marker(&path, id)?;
    Ok(Marker { id, fresh: true })
}

/// The identity an existing index records, or `None` when there is no index.
///
/// An index that exists but cannot be opened is an error, never "no
/// identity": minting a UUID over an unreadable record would write a marker
/// that disagrees with it, so the construction refuses before its one marker
/// write (T17).
pub(super) fn index_identity_if_present(path: &Path) -> Result<Option<[u8; ROOT_ID_LEN]>, Error> {
    if !path_present(path)? {
        return Ok(None);
    }
    let index = Index::open_read_only(path)?;
    index.root_id()
}

/// Write `identity` to the marker at `path`, creating the layer directory.
///
/// The bytes land in a sibling temporary file that is fsynced and renamed over
/// the marker, and the directory is fsynced after the rename: the marker's own
/// inode is never truncated, so a crash leaves either the old identity or the
/// new one, never an empty or partial record.
pub(super) fn write_marker(path: &Path, identity: [u8; ROOT_ID_LEN]) -> Result<(), Error> {
    let parent = path.parent().ok_or_else(|| {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the marker path has no directory",
        ))
    })?;
    fs::create_dir_all(parent)?;
    // The marker is fsynced before its rename: it is the one record a crash
    // between the two leaves behind.
    atomic::write_atomic(path, &marker_temp(path), &identity)?;
    Ok(())
}

/// The temporary path a marker write lands in, beside the marker itself so the
/// rename stays within one directory.
pub(super) fn marker_temp(path: &Path) -> PathBuf {
    path.with_file_name(MARKER_TEMP)
}

/// Remove a temporary marker a crashed construction left behind.
///
/// Only a writable open calls this: a read-only run never writes, and it never
/// reads the temporary name either.
pub(super) fn remove_marker_temp(path: &Path) -> Result<(), Error> {
    ok_if_missing(fs::remove_file(marker_temp(path))).map_err(Error::Io)
}

/// Write the marker only when it does not already record `identity`.
pub(super) fn write_marker_if_different(
    path: &Path,
    identity: [u8; ROOT_ID_LEN],
) -> Result<(), Error> {
    if read_marker(path)? == Some(identity) {
        return Ok(());
    }
    write_marker(path, identity)
}

/// Open the writable index and make its recorded identity the marker's.
///
/// The marker is the root's one authoritative record once it is written, so an
/// index that already records a different identity is refused rather than
/// reconciled: a second marker write would break the create-before-index rule
/// T17 relies on.
pub(super) fn open_index_writable(
    root: &Path,
    marker: &Marker,
    compact_percent: u8,
) -> Result<Index, Error> {
    let index = Index::open_with_compact_percent(index_path(root), compact_percent)?;
    match (marker.fresh, index.root_id()?) {
        (false, Some(recorded)) if recorded != marker.id => {
            return Err(marker_mismatch(marker.id, recorded));
        }
        (false, None) => index.set_root_id(marker.id)?,
        // A fresh marker should have reused the identity the read-only probe
        // read; an index that hid it from the probe is refused, never given a
        // second marker write.
        (true, Some(recorded)) if recorded != marker.id => {
            return Err(marker_mismatch(marker.id, recorded));
        }
        (true, None) => index.set_root_id(marker.id)?,
        _ => {}
    }
    Ok(index)
}

/// Open the writable index for the pg bind: an identity the index already
/// records must agree with `marker`, while a fresh index's identity is the
/// bind's to resolve (T18).
pub(super) fn open_index_for_bind(
    root: &Path,
    marker: Option<[u8; ROOT_ID_LEN]>,
    compact_percent: u8,
) -> Result<Index, Error> {
    let index = Index::open_with_compact_percent(index_path(root), compact_percent)?;
    if let (Some(marker), Some(recorded)) = (marker, index.root_id()?)
        && recorded != marker
    {
        return Err(marker_mismatch(marker, recorded));
    }
    Ok(index)
}

/// Open the existing index read-only (T27), refusing a missing index and an
/// identity that disagrees with the marker.
pub(super) fn open_index_read_only(
    root: &Path,
    marker: Option<[u8; ROOT_ID_LEN]>,
) -> Result<Index, Error> {
    let path = index_path(root);
    if !path_present(&path)? {
        return Err(Error::IndexMissing { path });
    }
    let index = Index::open_read_only(&path)?;
    match (marker, index.root_id()?) {
        (Some(marker), Some(recorded)) if recorded != marker => {
            return Err(marker_mismatch(marker, recorded));
        }
        (Some(marker), None) => {
            return Err(Error::MarkerMismatch {
                marker: root_wire(marker),
                index: "none".to_owned(),
            });
        }
        _ => {}
    }
    Ok(index)
}

/// The blocking half of [`crate::TierStorage::open_mutation`]: open the leaf
/// write-only under `follow`, rejecting a link at open time when following is
/// disabled.
pub(super) fn open_mutation_file(path: &Path, follow: bool) -> io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.write(true);
    if !follow {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(no_follow());
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            // FILE_FLAG_OPEN_REPARSE_POINT: open the reparse point itself.
            options.custom_flags(0x0020_0000);
        }
    }
    let file = options.open(path)?;
    // On Unix `O_NOFOLLOW` already rejects the link at open time; on Windows
    // `FILE_FLAG_OPEN_REPARSE_POINT` succeeds on a link, so the handle itself
    // is the only rejection.
    #[cfg(windows)]
    if !follow && _fs::is_symlink_or_reparse(&file.metadata()?) {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "symlink"));
    }
    Ok(file)
}

/// `O_NOFOLLOW` on this Unix target (0 where the target has no such flag).
#[cfg(unix)]
pub(super) const fn no_follow() -> i32 {
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

/// The marker/index disagreement refusal, with both identities rendered.
pub(super) fn marker_mismatch(marker: [u8; ROOT_ID_LEN], index: [u8; ROOT_ID_LEN]) -> Error {
    Error::MarkerMismatch {
        marker: root_wire(marker),
        index: root_wire(index),
    }
}

/// A root identity's wire rendering: the UUID the operator reads in a refusal.
pub(super) fn root_wire(id: [u8; 16]) -> String {
    uuid::Uuid::from_bytes(id).to_string()
}
