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

use crate::_core::{Error, Result, launch};

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
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::handles;
///
/// let pipe = handles::pipe().unwrap();
/// // What `CreateProcessW`'s handle list requires of every handle it is given.
/// handles::mark_inheritable(pipe.read().unwrap()).unwrap();
/// ```
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
/// [`Error::Launch`] when there is no standard error at all: a process with nowhere to log cannot host a confined server,
/// and the remedy belongs in the message rather than in a child that dies on its
/// first log write.
///
/// # Examples
///
/// ```no_run
/// // The child's own stderr is the launcher's, unmediated (decision 19): what
/// // the confined server logs reaches whoever started it.
/// let stderr = tinio_sandbox_windows::handles::inherited_stderr().unwrap();
/// # let _ = stderr;
/// ```
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

/// An anonymous pipe between the launcher and the child, **owning both of its
/// ends**.
///
/// The launcher creates two of these and each one carries a std stream of the
/// child: for the child's **stdin** the child takes the read end and the parent
/// keeps the write end, and for the child's **stdout** the child takes the write
/// end and the parent keeps the read — the payload flows one way and the events
/// the other.
///
/// **Both ends are created inheritable**, and the handle list is what decides
/// which one the child actually gets: clearing the other's inherit bit would be
/// a second mechanism for a guarantee `CreateProcessW` already applies.
///
/// **Ownership is the whole point of the two `Option`s.** Closing the ends by
/// hand after `CreateProcessW` meant every failure before that line — the second
/// [`pipe`], [`inherited_stderr`], the job, the launch, the payload write, the
/// relay's arming — returned with them still open, and one of them is worse than
/// a leak ([`parent_end`] records that a surviving event pipe's write end is a
/// **hang**: the child's stdout never reaches end-of-stream, so the relay's read
/// never returns). So the ends are given up explicitly — [`Self::take_read`] and
/// [`Self::take_write`] hand one to a new owner and clear it here, while
/// [`Self::close_read`] and [`Self::close_write`] close one this process is done
/// with — and this type's `Drop` closes whatever is still its own. An end that
/// has been handed over does not come back: the accessors answer `None` for it,
/// which is what keeps a second hand-off from closing a handle a `File` owns.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::handles;
///
/// let mut pipe = handles::pipe().unwrap();
/// // Hand the child's end to the file that will read it: the pipe no longer
/// // owns that handle, and its drop closes only what is left.
/// let events = pipe.take_read_file().unwrap();
/// assert!(pipe.read().is_none());
/// drop(events);
/// ```
pub struct Pipe {
    /// The end the child reads: the payload pipe's child side (its stdin), or
    /// `None` once it has been closed or handed over.
    read: Option<HANDLE>,
    /// The end the child writes: the event pipe's child side, or `None` once it
    /// has been closed or handed over.
    write: Option<HANDLE>,
}

impl Pipe {
    /// The end the child reads, while this pipe still owns it.
    #[must_use]
    pub fn read(&self) -> Option<HANDLE> {
        self.read
    }

    /// The end the child writes, while this pipe still owns it.
    #[must_use]
    pub fn write(&self) -> Option<HANDLE> {
        self.write
    }

    /// Give up the read end to a new owner, which closes it from now on.
    pub fn take_read(&mut self) -> Option<HANDLE> {
        self.read.take()
    }

    /// Give up the write end to a new owner, which closes it from now on.
    pub fn take_write(&mut self) -> Option<HANDLE> {
        self.write.take()
    }

    /// Close this process's copy of the read end, if it still has one.
    ///
    /// The child's own copy is `CreateProcessW`'s, so this is the call that
    /// leaves the launcher holding nothing of that end — the state the launch
    /// site needs before it waits on anything the child writes.
    pub fn close_read(&mut self) {
        if let Some(handle) = self.read.take() {
            close(handle);
        }
    }

    /// Close this process's copy of the write end, if it still has one.
    pub fn close_write(&mut self) {
        if let Some(handle) = self.write.take() {
            close(handle);
        }
    }

    /// Take the read end as a [`File`], which closes it when it drops.
    ///
    /// The event stream's reader: the launch site hands this to the relay, which
    /// reads the child's stdout to end-of-stream.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] when this pipe no longer holds that end — a call-site
    /// bug rather than a state an operator can reach, and reported rather than
    /// panicked on for the reason the helper beside it states: a panic is what
    /// this crate's rules forbid in library code (constitution II).
    pub fn take_read_file(&mut self) -> Result<File> {
        let handle = self.take_read().ok_or_else(|| end_gone("stdout"))?;
        Ok(parent_end(handle))
    }
}

/// The end a caller asked for is not this pipe's any more.
///
/// **A call-site invariant rather than an operator-facing state**: [`pipe`]
/// hands over both ends and nothing gives one up before the launch reads it, so
/// reaching this is a bug in the caller. It is an [`Error`](crate::_core::Error)
/// and not a panic because this crate's rules forbid a panic in library code
/// (constitution II), and because the caller that reaches it — the launcher —
/// has a shape to report it in rather than a reason to bring the process down.
pub(crate) fn end_gone(stream: &'static str) -> Error {
    launch(
        "hand the pipe over",
        stream,
        io::Error::other("this pipe end was already given up"),
    )
}

impl Drop for Pipe {
    fn drop(&mut self) {
        // What is still here is what no one else was given, so this is the last
        // owner in this process and the handle goes with it. Both closes are
        // [`Self::close_*`]'s, so an end that was handed over is not closed
        // twice — the field is `None` for it by then.
        self.close_read();
        self.close_write();
    }
}

/// Create a pipe with both ends inheritable.
///
/// # Errors
///
/// The Win32 error from `CreatePipe`.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::handles;
///
/// let pipe = handles::pipe().unwrap();
/// assert!(pipe.read().is_some() && pipe.write().is_some());
/// ```
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
    Ok(Pipe {
        read: Some(read),
        write: Some(write),
    })
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
/// it returns — and one of them is not merely dead: with the event pipe's write
/// end still open, the child's stdout never reaches end-of-stream, the relay's
/// read never returns, and `Child::wait`'s join with it never finishes.
/// [`close`] is the call, and the launch site reaches it through the pipe that
/// owns the end ([`Pipe::close_read`]/[`Pipe::close_write`]) immediately after
/// `CreateProcessW`.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::handles;
///
/// let mut pipe = handles::pipe().unwrap();
/// // The `File` owns the handle from here: dropping it closes the end.
/// let writer = handles::parent_end(pipe.take_write().unwrap());
/// drop(writer);
/// ```
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
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::handles;
///
/// let mut pipe = handles::pipe().unwrap();
/// // The child's copy is `CreateProcessW`'s from the moment it returns, so this
/// // process's is closed rather than left to the pipe's drop.
/// pipe.close_read();
/// assert!(pipe.read().is_none());
/// ```
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
/// **The pipe is the argument rather than a raw handle**, because the write end's
/// ownership is what this call takes: the frame is written through a [`File`]
/// that closes the handle when it drops, and [`Pipe`] is what keeps a second
/// close — or a leak on the paths above this one — from being possible.
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
/// [`Error::Launch`] when the write or the flush fails — including a child that
/// died before reading, which a pipe write reports as a broken pipe.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::handles;
///
/// let mut pipe = handles::pipe().unwrap();
/// // The frame the launcher sends: the child's configuration text, serialized
/// // by the core (decision 17). A reader is what bounds the write, and no child
/// // reads this one — the frame fits the pipe's 4 KiB buffer, which is the
/// // figure the doc above says the write stops being a memcpy at.
/// handles::write_payload(&mut pipe, b"version = 1\n").unwrap();
/// assert!(pipe.write().is_none(), "the end went with the frame");
/// ```
pub fn write_payload(pipe: &mut Pipe, bytes: &[u8]) -> Result<()> {
    // Taken out of the pipe, so the `File` below is the write end's only owner:
    // this call closes it, and the pipe's `Drop` has nothing left to close for
    // it.
    let writer = pipe.take_write().ok_or_else(|| end_gone("stdin"))?;
    let mut file = parent_end(writer);
    file.write_all(bytes)
        .map_err(|source| launch("write the launch payload", "stdin", source))?;
    file.flush()
        .map_err(|source| launch("flush the launch payload", "stdin", source))?;
    tracing::debug!(step = "write the launch payload", bytes = bytes.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    //! The raw handle plumbing: what a pipe owns, and what a hand-off gives away.
    //!
    //! **These tests exist because a [`Pipe`] used to own nothing.** [`pipe`]
    //! handed back two raw `HANDLE`s and the launch site closed them by hand after
    //! `CreateProcessW`, so every failure before that point — the second
    //! [`pipe`], [`inherited_stderr`], the job, the launch, the payload write, the
    //! relay's arming — returned with four handles still open. One of them is worse
    //! than a leak: with the event pipe's write end still open in the launcher, the
    //! child's stdout never reaches end-of-stream and the relay's read never
    //! returns, which is a hang ([`parent_end`]'s doc states it). The lookups below
    //! are how a closed handle is told from an open one without an `unsafe` block of
    //! this module's own: `SetHandleInformation` refuses a handle that is not there.

    use super::*;

    /// **One test and not two, because the second half depends on the first's
    /// handles being gone**: a handle value freed by one test and reused by another
    /// would make an assertion about a *specific* value meaningless, and this binary
    /// runs its tests in parallel.
    #[test]
    fn a_pipe_closes_the_ends_it_still_owns_and_gives_away_the_ones_it_hands_over() {
        // Both ends are closed by the owner's drop.
        let pipe = pipe().expect("a pipe");
        let (read, write) = match (pipe.read(), pipe.write()) {
            (Some(read), Some(write)) => (read, write),
            _ => panic!("a pipe just created owns both of its ends"),
        };
        drop(pipe);
        assert!(
            mark_inheritable(read).is_err(),
            "the read end outlived the pipe that owned it: a launch that fails after \
             the pipes are created leaks it"
        );
        assert!(
            mark_inheritable(write).is_err(),
            "the write end outlived the pipe that owned it — and a surviving event \
             pipe's write end is not a leak but a hang"
        );

        // An end that is handed over is the new owner's, and the pipe's drop no
        // longer closes it — while the end left behind still goes with the pipe.
        // The call is spelled through the module because the binding above already
        // shadows the name in this scope.
        let mut pipe = super::pipe().expect("a pipe");
        let handed = pipe.take_write().expect("the write end");
        let file = parent_end(handed);
        let left = pipe.read().expect("the read end");
        drop(pipe);
        assert!(
            mark_inheritable(handed).is_ok(),
            "a handed-off end must survive the pipe it came from"
        );
        assert!(
            mark_inheritable(left).is_err(),
            "the end the pipe kept goes with it"
        );
        drop(file);
        assert!(
            mark_inheritable(handed).is_err(),
            "the new owner closes the end it was given"
        );
    }
}
