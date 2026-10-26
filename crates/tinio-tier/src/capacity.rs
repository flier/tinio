//! The volume-pressure probe the mover's capacity pass reads (T21).
//!
//! [`Capacity`] answers the two questions the capacity pass asks: how full the
//! warm plane's volume is, and how many bytes it holds in total. The second
//! lets a pass translate the watermark gap into the bytes it must reclaim,
//! because a settled demote's warm file is garbage collection's (gated by
//! `gc_grace`) and the used percentage cannot move during the pass.
//! [`VolumeCapacity`] answers both from the operating system — `statvfs` on
//! Unix and `GetDiskFreeSpaceExW` on Windows — so the mover measures pressure
//! and never invents an admission policy (T21). A test substitutes a probe
//! that reports a scripted sequence.

use std::{
    fmt, io,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use rustix::fs as rustix_fs;
#[cfg(windows)]
use {
    std::os::windows::ffi::OsStrExt as _, std::ptr,
    windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW,
};

use crate::Error;

/// The warm volume's used percentage, as the capacity pass reads it.
///
/// A probe answers the used share of the filesystem holding the warm plane,
/// `0..=100`. An error is the probe's own failure (the volume could not be
/// measured); the pass surfaces it rather than guessing a pressure.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::{Capacity, VolumeCapacity};
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// let capacity = VolumeCapacity::new(root.path());
/// let used = capacity.used_percent().expect("the volume answers");
/// assert!(used <= 100);
/// ```
pub trait Capacity: Send + Sync + fmt::Debug {
    /// The percentage of the volume currently in use.
    ///
    /// # Errors
    ///
    /// The operating system's measurement failure, or a volume that reports
    /// no capacity to measure against.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::{Capacity, VolumeCapacity};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let capacity = VolumeCapacity::new(root.path());
    /// assert!(capacity.used_percent().expect("the volume answers") <= 100);
    /// ```
    fn used_percent(&self) -> Result<u8, Error>;

    /// The volume's total capacity in bytes.
    ///
    /// The capacity pass translates the watermark gap into the bytes a pass
    /// must reclaim; without the total it could only re-read a probe that a
    /// settled demote cannot move until garbage collection runs.
    ///
    /// # Errors
    ///
    /// The operating system's measurement failure, or a volume that reports
    /// no capacity to measure against.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::{Capacity, VolumeCapacity};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let capacity = VolumeCapacity::new(root.path());
    /// assert!(capacity.total_bytes().expect("the volume answers") > 0);
    /// ```
    fn total_bytes(&self) -> Result<u64, Error>;
}

/// The warm root's real volume, measured through the operating system (T21).
///
/// The probe is stateless: every call measures the filesystem holding the
/// configured root, so a pass sees the pressure at the moment it asks.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::VolumeCapacity;
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// let capacity = VolumeCapacity::new(root.path());
/// assert!(format!("{capacity:?}").contains("VolumeCapacity"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeCapacity {
    /// The warm plane's root.
    root: PathBuf,
}

impl VolumeCapacity {
    /// Measure `root`'s filesystem.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::{Capacity, VolumeCapacity};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let capacity = VolumeCapacity::new(root.path());
    /// assert!(capacity.used_percent().expect("the volume answers") <= 100);
    /// ```
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The root the probe measures.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::VolumeCapacity;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let capacity = VolumeCapacity::new(root.path());
    /// assert_eq!(capacity.root(), root.path());
    /// ```
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl Capacity for VolumeCapacity {
    fn used_percent(&self) -> Result<u8, Error> {
        let (available, total) = space(&self.root)?;
        if total == 0 {
            return Err(Error::Io(io::Error::other(
                "the volume reports zero capacity",
            )));
        }
        let used = u128::from(total.saturating_sub(available));
        let percent = used * 100 / u128::from(total);
        Ok(u8::try_from(percent.min(100)).unwrap_or(100))
    }

    fn total_bytes(&self) -> Result<u64, Error> {
        let (_, total) = space(&self.root)?;
        if total == 0 {
            return Err(Error::Io(io::Error::other(
                "the volume reports zero capacity",
            )));
        }
        Ok(total)
    }
}

/// `(available, total)` bytes on `root`'s filesystem: `f_bavail × f_frsize`
/// and `f_blocks × f_frsize` on Unix.
#[cfg(unix)]
fn space(root: &Path) -> Result<(u64, u64), Error> {
    let stat = rustix_fs::statvfs(root)
        .map_err(|err| Error::Io(io::Error::from_raw_os_error(err.raw_os_error())))?;
    let fragment = stat.f_frsize;
    Ok((
        stat.f_bavail.saturating_mul(fragment),
        stat.f_blocks.saturating_mul(fragment),
    ))
}

/// `(available, total)` bytes on `root`'s filesystem:
/// `lpFreeBytesAvailableToCaller` and `lpTotalNumberOfBytes` of
/// `GetDiskFreeSpaceExW`.
///
/// The lint exemption is scoped to this one call: Windows writes the two out
/// parameters only on success, and the path reaches it as a NUL-terminated
/// wide string, the encoding the call requires.
#[cfg(windows)]
#[expect(
    unsafe_code,
    reason = "argued: Windows's `GetDiskFreeSpaceExW` writes its two out parameters only on success, and the path reaches it as a NUL-terminated wide string, the encoding the call requires"
)]
fn space(root: &Path) -> Result<(u64, u64), Error> {
    let mut path: Vec<u16> = root.as_os_str().encode_wide().collect();
    if path.contains(&0) {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the volume path contains a NUL",
        )));
    }
    path.push(0);
    let mut available = 0u64;
    let mut total = 0u64;
    // SAFETY: `path` is a live, NUL-terminated wide string, and both out
    // parameters are live `u64`s for the duration of the call. The API writes
    // them only on success.
    let ok =
        unsafe { GetDiskFreeSpaceExW(path.as_ptr(), &mut available, &mut total, ptr::null_mut()) };
    if ok == 0 {
        return Err(Error::Io(io::Error::last_os_error()));
    }
    Ok((available, total))
}

/// A platform with no volume probe: the pass is told so instead of panicking
/// or reporting a fabricated pressure.
#[cfg(not(any(unix, windows)))]
fn space(_root: &Path) -> Result<(u64, u64), Error> {
    Err(Error::Io(io::Error::new(
        io::ErrorKind::Unsupported,
        "the volume capacity probe is unsupported on this platform",
    )))
}
