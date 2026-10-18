//! The one sentence a failed Win32 call is reported with, and the system directory the kernel
//! reports rather than an environment variable spells.
//!
//! **Both are facts whose second copy would drift.** The sentence's readers are the management
//! plane's named-pipe create and the sandbox's ACE walk: the crate that held it for both
//! (`tinio-winsec`) dissolved with the file descriptors it managed, and the wording moved here
//! rather than being copied. The directory's readers are the sandbox's `icacls` oracle and the
//! CLI config root's independent DACL test, which before this module each spelled their own
//! answer — one asking the kernel, one reading `%SystemRoot%`.
//!
//! **Attribution stays with each caller**: the sentence is returned alone, because a caller
//! whose error type has a path field puts it there rather than splicing it into this text.
//!
//! `#[cfg(windows)]` and **not** feature-gated, like [`wide`](crate::wide): both callers
//! compile it in a production build.

#![allow(unsafe_code)]

use std::{ffi::OsString, io, os::windows::ffi::OsStringExt, path::PathBuf};

use windows_sys::Win32::{
    Foundation::ERROR_SUCCESS, System::SystemInformation::GetSystemDirectoryW,
};

/// The sentence a failed Win32 call is reported with.
///
/// A `BOOL`-returning call need not set a last error, and status `0` would then render as
/// the success message; say what is known rather than repeat that text.
///
/// # Examples
///
/// ```
/// let refused = tinio_util::win32::error_message("CreateNamedPipeW", 5);
///
/// assert!(
///     refused.starts_with("CreateNamedPipeW failed: "),
///     "{refused}"
/// );
/// assert!(refused.ends_with(" (status 5)"), "{refused}");
///
/// // A call that set no last error is reported as that, never as a success.
/// assert_eq!(
///     tinio_util::win32::error_message("CreateNamedPipeW", 0),
///     "CreateNamedPipeW failed without a status"
/// );
/// ```
pub fn error_message(call: &str, status: u32) -> String {
    if status == ERROR_SUCCESS {
        format!("{call} failed without a status")
    } else {
        let meaning = io::Error::from_raw_os_error(status as i32);
        format!("{call} failed: {meaning} (status {status})")
    }
}

/// The system directory, as the kernel reports it rather than as an environment variable
/// spells it.
///
/// `GetSystemDirectoryW` answers with the directory the loader itself searches —
/// `…\System32` under the installation wherever it is — so a tool a caller runs is not
/// selectable through the environment. **That is robustness rather than defence**: an actor
/// who can set a process's environment is a local actor, and local actors are outside this
/// feature's threat model, so what the rule buys is that the path cannot be wrong for an
/// environment reason. Its readers are the sandbox's `icacls` oracle and the CLI config root's
/// independent DACL test.
///
/// **Infallible on purpose.** A system directory is far shorter than the buffer below, and a
/// host that answered a longer one gets a second call sized by the length the first reported
/// rather than a guessed path — the documented default location answers only when the API
/// itself reports nothing, which is why the return type is not a `Result`.
///
/// # Examples
///
/// ```
/// let system = tinio_util::win32::system_directory();
/// assert!(system.is_absolute(), "{}", system.display());
/// ```
pub fn system_directory() -> PathBuf {
    // `MAX_PATH` is 260 and a system directory is a fraction of it, so this buffer cannot be
    // too small on any ordinary host; one that answers a longer path gets the second call
    // below, sized by the length the first reported.
    let mut buffer = [0u16; 260];
    // SAFETY: `buffer` is a valid, writable array of `buffer.len()` UTF-16 units and stays
    // alive for the call; the call writes at most that many and touches nothing else.
    let len = unsafe { GetSystemDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32) };
    if len as usize >= buffer.len() {
        let mut grown = vec![0u16; len as usize];
        // SAFETY: as above, with `grown` in place of `buffer`.
        let written = unsafe { GetSystemDirectoryW(grown.as_mut_ptr(), grown.len() as u32) };
        if written == 0 || written as usize >= grown.len() {
            return PathBuf::from(r"C:\Windows\System32");
        }
        return PathBuf::from(OsString::from_wide(&grown[..written as usize]));
    }
    if len == 0 {
        return PathBuf::from(r"C:\Windows\System32");
    }
    // **`OsString::from_wide` and not `String::from_utf16_lossy`, and the difference is the one
    // a path must not make**: the lossy form replaces a unit that is not valid UTF-16 with
    // U+FFFD, so a system directory holding one — an unusual but legal installation — would
    // come back as a *different* path, and a caller would run something the operator never
    // had. A string needs the lossy form and a path must not have it.
    PathBuf::from(OsString::from_wide(&buffer[..len as usize]))
}

#[cfg(test)]
mod tests {
    use windows_sys::Win32::Foundation::ERROR_SUCCESS;

    use super::error_message;

    /// **The sentence names the call, the OS's message and the decimal status** — the three
    /// things a log reader needs to act on a refused `*W` call rather than look its number up.
    ///
    /// It carries **no path**: a caller whose error type has a path field puts it there, so a
    /// sentence splicing an object's name in would render the same object twice.
    #[test]
    fn a_failed_call_is_reported_with_the_call_the_message_and_the_status() {
        let message = error_message("CreateNamedPipeW", 5);
        assert!(
            message.starts_with("CreateNamedPipeW failed: "),
            "the sentence names the call that failed: {message}"
        );
        assert!(
            message.ends_with(" (status 5)"),
            "it closes with the decimal status, so a log reader can act on the code: {message}"
        );
        assert!(
            !message.contains(r"\\.\pipe"),
            "the sentence carries no object of its own: {message}"
        );

        // A call that set no last error must not be handed the success message.
        assert_eq!(
            error_message("CreateNamedPipeW", ERROR_SUCCESS),
            "CreateNamedPipeW failed without a status"
        );
    }
}
