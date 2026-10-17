//! The NUL-terminated UTF-16 form every `*W` call takes, spelled once.
//!
//! **This module owns the writer half of the wide-string rule and `tinio-sandbox-windows` owns
//! the reader**, which is the split the dissolved `tinio-winsec` left behind: [`to_wide`] is
//! where every Windows crate here turns a path or a name into the units a `*W` call takes — the
//! management plane's pipe name, the sandbox's profile names, capability strings, image paths and
//! sweep paths — while the inverse direction (`tinio_sandbox_windows::from_wide_units` and
//! `from_wide`, which read a name *back out of* the kernel) stays in the crate whose FFI it is.
//! Each side names the other, so the pair is findable from either end.
//!
//! **The module is gated on the target and not on a feature**, the way `job_object` beside it is:
//! both of its callers run in a production build — the management plane names its pipe and the
//! sandbox's own Win32 calls encode their paths — so `#[cfg(windows)]` on the declaration in
//! `lib.rs` is the whole gate and `tinio-util`'s `windows-sys` edge is a plain target
//! dependency.

use std::{ffi::OsStr, iter, os::windows::ffi::OsStrExt};

/// The UTF-16, NUL-terminated form every `*W` call takes — the file path `CreateFileW`,
/// `SetNamedSecurityInfoW` and `GetNamedSecurityInfoW` want, and the pipe name `CreateNamedPipeW`
/// wants (which never names a file, so it goes in as an `OsStr`).
///
/// **The terminator is added here and nowhere else**, so every call site hands the same shape to
/// the API and no caller writes the trailing zero itself: the returned `Vec` owns its units, and
/// a caller that needs it alive across a call binds it (an `as_ptr()` on a temporary would
/// dangle at the end of the statement).
///
/// # Examples
///
/// ```
/// use std::ffi::OsStr;
///
/// use tinio_util::wide::to_wide;
///
/// let path = to_wide(r"C:\state\state");
/// assert_eq!(path.len(), r"C:\state\state".encode_utf16().count() + 1);
/// assert_eq!(
///     path.last(),
///     Some(&0),
///     "every `*W` call takes a NUL-terminated form"
/// );
///
/// // A name that is not text goes in as an `OsStr`, and is terminated the same way.
/// assert_eq!(to_wide(OsStr::new("")).as_slice(), &[0]);
/// ```
pub fn to_wide(name: impl AsRef<OsStr>) -> Vec<u16> {
    name.as_ref().encode_wide().chain(iter::once(0)).collect()
}
