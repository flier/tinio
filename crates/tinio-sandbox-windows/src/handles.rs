//! The raw handle plumbing: which handles a child inherits, and how the
//! parent's own handles are made inheritable at all.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design
//! decision 14): it exists so that every other module can name a handle, make it
//! inheritable, create a pipe, report a Win32 error and take the system
//! directory from the kernel without an unsafe call of its own. Every unsafe
//! block below carries its safety argument.
//!
//! **The failure builder is not here any more, and that is where it belongs.**
//! This module used to carry `step(operation, path, source)`, building
//! `Error::Launch` and emitting the `error!` event in one call; the core carries
//! it now as `launch`, beside the other platforms' shapes, so the log's `step`
//! and the error's `step` cannot drift between them. The
//! recording-subscriber test that pinned that property moved with it.
//!
//! **The Win32 calls below are `windows_sys::…`'s**, each named at the top of
//! this module rather than declared here or routed through an intermediate
//! module: the ABI is a dependency's to maintain, and what this crate keeps is
//! the safety argument for each call (design Bookkeeping).

#![allow(unsafe_code)]

use std::{
    ffi::OsString,
    fs::File,
    io::{self, Write},
    mem::size_of,
    os::windows::{ffi::OsStringExt, io::FromRawHandle},
    path::PathBuf,
    ptr::null_mut,
};

use windows_sys::Win32::{
    Foundation::{
        CloseHandle, GetLastError, HANDLE, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE,
        SetHandleInformation,
    },
    Security::SECURITY_ATTRIBUTES,
    System::{
        Console::{GetStdHandle, STD_ERROR_HANDLE},
        Pipes::CreatePipe,
        SystemInformation::GetSystemDirectoryW,
    },
};

use crate::_core::{Result, launch};

/// The Win32 error the last failing call set.
///
/// This is the crate's only `GetLastError` call site, kept here so that the
/// other modules report a Win32 error without their own unsafe call.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::handles;
///
/// let err = handles::last_error();
/// assert!(err.raw_os_error().is_some());
/// ```
pub fn last_error() -> io::Error {
    // SAFETY: GetLastError reads the calling thread's last-error value; it takes
    // no arguments and has no preconditions.
    unsafe { io::Error::from_raw_os_error(GetLastError() as i32) }
}

/// The system directory, as the kernel reports it rather than as an environment
/// variable spells it.
///
/// `GetSystemDirectoryW` answers with the directory the loader itself searches —
/// `…\System32` under the installation wherever it is — so a tool the crate runs
/// is not selectable through the environment. **That is robustness rather than
/// defence**: an actor who can set this launcher's environment is a local actor,
/// and local actors are outside this feature's threat model, so what the rule
/// buys is that the path cannot be wrong for an environment reason. The one
/// caller is `crate::acl::icacls_binary`.
///
/// **Infallible on purpose.** A system directory is far shorter than the buffer
/// below, so the call cannot fail here, and every caller wants a path rather than
/// an error it would have to invent a fallback for — the documented default
/// location answers in the branch nothing reaches, which is why the return type
/// is not a `Result`.
///
/// # Examples
///
/// ```no_run
/// let system = tinio_sandbox_windows::handles::system_directory();
/// assert!(system.is_absolute(), "{}", system.display());
/// ```
pub fn system_directory() -> PathBuf {
    // `MAX_PATH` is 260 and a system directory is a fraction of it, so this
    // buffer cannot be too small; the call reports the length it needs instead of
    // overrunning if that ever stopped being true.
    let mut buffer = [0u16; 260];
    // SAFETY: `buffer` is a valid, writable array of `buffer.len()` UTF-16 units
    // and stays alive for the call; the call writes at most that many and touches
    // nothing else.
    let len = unsafe { GetSystemDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32) };
    if len == 0 || len as usize >= buffer.len() {
        return PathBuf::from(r"C:\Windows\System32");
    }
    // SAFETY: the call above copied `len` UTF-16 units into `buffer`, which is
    // alive and unmodified here, `len` is the count it reported — bounded by the
    // buffer's length by the branch above — and `GetSystemDirectoryW` terminates
    // what it wrote, which is the contract `from_wide_units` scans to.
    let units = unsafe { crate::from_wide_units(buffer.as_ptr()) };
    // **`OsString::from_wide` and not `String::from_utf16_lossy`, and the
    // difference is the one this crate refuses to make on a path**: the lossy
    // form replaces a unit that is not valid UTF-8 with U+FFFD, so a system
    // directory holding one — an unusual but legal installation — would come
    // back as a *different* path, and `icacls_binary` would hand `CreateProcessW`
    // something the operator never had. A string needs the lossy form and a path
    // must not have it, which is why the two spellings exist.
    PathBuf::from(OsString::from_wide(units))
}

/// Make `handle` inheritable, so a `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` may name
/// it.
///
/// Windows 8 and later create sockets non-inheritable, so this call is required
/// rather than defensive (design evidence table).
///
/// # Errors
///
/// The Win32 error when `SetHandleInformation` refuses.
// The raw-pointer argument is this module's reason to exist rather than a hazard
// to hide: `HANDLE` is windows-sys's public handle type (later tasks name it in
// their signatures), and the contract — a live handle the caller owns — is
// proven by the caller. Making this an `unsafe fn` would put an `unsafe` block
// in every module that marks a handle inheritable, which is what design decision
// 14 confines to this one.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn mark_inheritable(handle: HANDLE) -> io::Result<()> {
    // SAFETY: `handle` is a live handle the caller owns for the duration of the
    // call, and SetHandleInformation with HANDLE_FLAG_INHERIT only sets the
    // inherit bit on that handle.
    let ok = unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(())
}

/// The parent's standard error, marked inheritable.
///
/// The confined child inherits it untouched and unmediated, which is where every
/// log goes (decision 19). An inherited handle needs no DACL check on use, which
/// is why the child can write to the operator's console or to a redirect the
/// shell set up.
///
/// The child's **stdin and stdout** are the launcher's pipes, not the launcher's
/// own handles: stdin carries the payload, stdout carries the event stream the
/// parent relays. Taking them from this process would mean writing into the
/// operator's console and reading whatever it types.
///
/// The inherit bit is left set on the launcher's own handle: it is a
/// process-wide flag and putting it back would be tidiness nothing depends on —
/// the launcher lives exactly as long as the child it supervises.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) when there is no standard
/// error at all: a process with nowhere to log cannot host a confined server,
/// and the remedy belongs in the message rather than in a child that dies on its
/// first log write.
pub fn inherited_stderr() -> Result<HANDLE> {
    // SAFETY: GetStdHandle takes an index and returns the process's handle for
    // it; it has no other precondition.
    let handle = unsafe { GetStdHandle(STD_ERROR_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(launch(
            "read the standard handles",
            PathBuf::from("stderr"),
            io::Error::other(
                "no standard error: run the server from a console, or redirect its output to a file",
            ),
        ));
    }
    mark_inheritable(handle)
        .map_err(|source| launch("mark a standard handle inheritable", "stderr", source))?;
    Ok(handle)
}

/// An anonymous pipe between the launcher and the child.
///
/// The launcher creates two of these and each one carries a std stream of the
/// child: for the child's **stdin** the child takes `read` and the parent keeps
/// `write`, and for the child's **stdout** the child takes `write` and the parent
/// keeps `read` — the payload flows one way and the events the other.
///
/// Both ends are created inheritable, and the handle list is what decides which
/// one the child actually gets: clearing the other's inherit bit would be a
/// second mechanism for a guarantee `CreateProcessW` already applies.
pub struct Pipe {
    /// The end the child reads: the payload pipe's child side (its stdin).
    pub read: HANDLE,
    /// The end the child writes: the event pipe's child side.
    pub write: HANDLE,
}

/// Create a pipe with both ends inheritable.
///
/// # Errors
///
/// The Win32 error from `CreatePipe`.
pub fn pipe() -> io::Result<Pipe> {
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: 1,
    };
    let mut read: HANDLE = null_mut();
    let mut write: HANDLE = null_mut();
    // SAFETY: both out-parameters are valid, aligned and writable for the call,
    // and `attributes` is a correctly sized SECURITY_ATTRIBUTES that lives until
    // the call returns.
    let ok = unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) };
    if ok == 0 {
        return Err(last_error());
    }
    // The caller names which std stream this pipe carries when it maps the
    // failure above (`launch("CreatePipe", "stdin"/"stdout", …)`, the core's);
    // the event has the two handle values, which is what identifies a pipe in a
    // log.
    tracing::debug!(step = "CreatePipe", read = ?read, write = ?write);
    Ok(Pipe { read, write })
}

/// The launcher's end of a pipe it just created, as a [`File`].
///
/// The returned `File` closes the handle when it drops, which is what the
/// launcher wants at both ends of the child's std streams: it writes the payload
/// and lets go, and it reads the event stream until the child's stdout closes.
///
/// Keeping this safe is deliberate: the contract — one `File` per handle, on an
/// end `Self::pipe` just returned — is proven by the call site, so the crate's
/// other modules never carry an `unsafe` block of their own.
///
/// **The other end of the same pipe is the caller's to close, and forgetting it
/// is how a launcher hangs.** `CreateProcessW` duplicates the child's ends into
/// the child's handle table, so the launcher's copies are dead weight the moment
/// it returns — and one of them is not merely dead: with `event_pipe.write` still
/// open, the child's stdout never reaches end-of-stream, the relay's read never
/// returns, and `Child::wait`'s join with it never finishes. [`close`] is the
/// call, and the launch site makes it immediately after `CreateProcessW`.
pub fn parent_end(handle: HANDLE) -> File {
    // SAFETY: `handle` is one end of a pipe this process created and handed
    // over; no other `File` owns it, and nothing else closes it.
    unsafe { File::from_raw_handle(handle.cast()) }
}

/// Close a handle the launcher no longer needs.
///
/// Used for the child's own ends of the two pipes once `CreateProcessW` has
/// duplicated them into the child: see [`parent_end`] for why leaving the event
/// pipe's write end open is a hang rather than a leak.
// The allow is [`mark_inheritable`]'s, for the same reason and with the same
// contract: the caller owns the handle and gives it up exactly once, here.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn close(handle: HANDLE) {
    // SAFETY: `handle` is a live handle this process owns and is giving up; it
    // is closed exactly once, here, and nothing else holds a copy of it.
    unsafe { CloseHandle(handle) };
}

/// Write the launch frame into the child's stdin and close it (design §3 step 7).
///
/// The close is what makes the child's read terminate: dropping the `File` closes
/// this process's copy of the write end, so the child's read of stdin reaches EOF
/// after the frame.
///
/// **This is a blocking write, and what bounds it is the child rather than the
/// size of the frame.** The frame is the launcher's **whole configuration text**
/// (`LaunchPayload { config }`, decision 17), and no document bounds how large
/// that is; a Win32 anonymous pipe created without a size buffers 4 KiB
/// (`CreatePipe`'s default, and [`pipe`] passes 0), so a frame larger than that
/// returns only as the child drains it. The child does read: its `adopt` reads
/// the frame before it does anything else, and the launch site writes *after*
/// `CreateProcessW`, so the two are a producer and a consumer that meet — the
/// write completes as long as the child runs and reads. What can stall is a child
/// that neither reads nor exits, and it stalls here, before the launcher has
/// reported anything: the 4 KiB figure is the point at which the write stops
/// being a memcpy into the pipe buffer, not a bound on what may be sent. No
/// caller imposes a size limit today, and a caller that wants one has to add it
/// either here or at the plan that builds the configuration text.
///
/// # Errors
///
/// [`Error::Launch`](crate::_core::Error::Launch) when the write or the flush
/// fails — including a child that died before reading, which a pipe write
/// reports as a broken pipe.
pub fn write_payload(writer: HANDLE, bytes: &[u8]) -> Result<()> {
    let mut file = parent_end(writer);
    file.write_all(bytes)
        .map_err(|source| launch("write the launch payload", "stdin", source))?;
    file.flush()
        .map_err(|source| launch("flush the launch payload", "stdin", source))?;
    tracing::debug!(step = "write the launch payload", bytes = bytes.len());
    Ok(())
}
