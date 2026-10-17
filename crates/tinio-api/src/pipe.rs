//! The crate's one raw-Windows module: named-pipe creation and adoption. Nothing here is
//! reachable off Windows, and the single `#[expect(unsafe_code)]` below is the crate's only
//! `unsafe` site (design decision 20's pattern, `docs/cargo.md`): every caller sees a safe
//! wrapper whose checks are exhaustive and whose failures are `Error`s, never panics, because
//! the invariant is the one Windows imposes rather than one this crate chooses.
//!
//! **The channel installs no descriptor.** `CreateNamedPipeW` is called with a null
//! `SECURITY_ATTRIBUTES`, so each instance takes Windows' own default named-pipe descriptor — the
//! platform's own answer: SYSTEM, Administrators and the creating user at full control, Everyone
//! and ANONYMOUS LOGON at read (measured on this host), rather than one this crate chooses. What
//! holds the channel local is `PIPE_REJECT_REMOTE_CLIENTS` and the access token that guards
//! `/status` and `/stop` (design decision 8); what holds it single-instance is
//! `FILE_FLAG_FIRST_PIPE_INSTANCE` on the first create.

#![allow(unsafe_code)]

use std::{
    ffi::OsStr,
    io,
    os::windows::io::{FromRawHandle, IntoRawHandle, OwnedHandle, RawHandle},
    ptr,
};

use tokio::net::windows::named_pipe::NamedPipeServer;
use windows_sys::Win32::{
    Foundation::INVALID_HANDLE_VALUE,
    Storage::FileSystem::{
        FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, PIPE_ACCESS_DUPLEX,
    },
    System::Pipes::{
        CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
        PIPE_UNLIMITED_INSTANCES,
    },
};

use crate::_util::{wide::to_wide, win32::error_message};

/// Creates the named pipe `name` and hands it back as an owned handle — adopted inside the runtime
/// by `adopt_pipe` (Task 4), because only there does the reactor exist that a `NamedPipeServer`
/// needs. `first_instance` is `FILE_FLAG_FIRST_PIPE_INSTANCE`: the single-instance claim every
/// later bind of the same name must fail on.
///
/// **No `SECURITY_ATTRIBUTES` travels with the create.** `CreateNamedPipeW` is handed a null
/// pointer, so the instance takes Windows' own default named-pipe descriptor — the module doc
/// states the measured entries — the same answer a plain `ServerOptions::create` gets, and **wider**
/// than the creating token's own default DACL rather than narrower. The pipe is duplex and
/// byte-mode with remote clients refused, which is the
/// **local-only** rule `ServerOptions::reject_remote_clients` asks for, from the layer that can set
/// the pipe mode at the same time.
///
/// # Examples
///
/// ```no_run
/// use tinio_api::pipe::create_pipe;
///
/// let handle = create_pipe(r"\\.\pipe\tinio-mine", true).expect("first instance");
/// drop(handle);
/// ```
pub fn create_pipe(name: &str, first_instance: bool) -> io::Result<OwnedHandle> {
    let open_mode = FILE_FLAG_OVERLAPPED
        | PIPE_ACCESS_DUPLEX
        | if first_instance {
            FILE_FLAG_FIRST_PIPE_INSTANCE
        } else {
            0
        };
    // `PIPE_REJECT_REMOTE_CLIENTS` is a **pipe-mode** bit — `dwPipeMode`, the third argument —
    // although it reads like an open-mode one: `ServerOptions` sets it in its pipe mode too
    // (`tokio/src/net/windows/named_pipe.rs`), and a bit Windows does not define for `dwOpenMode`
    // is not the local-only rule this channel needs.
    let pipe_mode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_REJECT_REMOTE_CLIENTS;
    let handle = unsafe {
        CreateNamedPipeW(
            to_wide(OsStr::new(name)).as_ptr(),
            open_mode,
            pipe_mode,
            PIPE_UNLIMITED_INSTANCES,
            65536,
            65536,
            0,
            // A null `SECURITY_ATTRIBUTES`: Windows' own default named-pipe descriptor, and
            // nothing this crate installs.
            ptr::null(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        // **The failure names the call, the OS's message and the decimal status**
        // ([`error_message`]'s sentence), which is what a log reader of a refused bind needs:
        // `os error 5` alone would leave the status to be looked up. **The kind is preserved**,
        // because `bind_pipe` reads it to tell a name someone else owns from a refusal of another
        // sort.
        let err = io::Error::last_os_error();
        return Err(io::Error::new(
            err.kind(),
            error_message("CreateNamedPipeW", err.raw_os_error().unwrap_or(0) as u32),
        ));
    }
    // Ownership moves to the caller on success, and there is no descriptor buffer to outlive the
    // call.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

/// Adopts the handle [`create_pipe`] made into tokio's own server type. It is called **inside
/// the runtime the pipe is served in** — `transport::serve_pipe` is the caller — because the
/// constructor it wraps ends in `PollEvented::new`, the same reactor requirement that keeps the
/// bind's pipe a raw handle (`crate::bind`) and the reason creation and adoption are two
/// functions rather than one.
///
/// It is `pub(crate)`, and a doctest is compiled against the public API alone, so unlike the
/// wrappers above it carries no example of its own: the Rust code would be `adopt_pipe(handle)?`
/// on the handle [`create_pipe`]'s example returns.
pub(crate) fn adopt_pipe(handle: OwnedHandle) -> io::Result<NamedPipeServer> {
    // SAFETY: the handle is one `create_pipe` created and this call is its only consumer.
    // `into_raw_handle` gives up `handle`'s own drop, so ownership moves to the returned server
    // alone — and `NamedPipeServer`'s constructor closes the handle itself when it cannot build,
    // so no path here leaks it.
    unsafe { NamedPipeServer::from_raw_handle(handle.into_raw_handle()) }
}

/// Adopts a named-pipe handle a **launcher** created and handed to this process, as the
/// owned handle the channel carries ([`create_pipe`]'s return type, and this function's
/// mirror image).
///
/// **The conversion is the whole function, and it lives here because `unsafe` does.**
/// `pipe` is the crate's one raw-Windows module (its own module doc says so), and
/// `OwnedHandle::from_raw_handle` is unsafe by construction: it promises that the value
/// names a live handle of the right kind and that ownership moves with it. Both halves of
/// that promise are the *launcher's* here rather than this crate's. The launcher ran
/// unconfined, created the pipe with [`create_pipe`] and passed the raw value across
/// `CreateProcessW`; this process inherited it, nothing else in this process has touched
/// it, and no second owner of the value exists — which is what makes the conversion sound.
///
/// **Why the child needs it at all**: inside a container this process's identity is the
/// package SID, not the user, so `CreateNamedPipeW` for an instance of the channel's name is
/// refused (`ERROR_ACCESS_DENIED`) — the launcher's pipe carries Windows' own default named-pipe
/// descriptor, which names the user (with SYSTEM, Administrators, Everyone and ANONYMOUS LOGON)
/// and not the package SID, so the child cannot make these objects
/// and adopts the ones its launcher made. See `bind::Channel::raw_handles` for the launcher's
/// half; one call per instance, because the channel is a pool and the child serves on every
/// instance it was handed.
///
/// # Errors
///
/// [`io::ErrorKind::InvalidInput`] for a null handle: a value that names no object is not
/// an inherited pipe, and adopting it would put a garbage handle into the channel. Nothing
/// else here can fail — the kernel performs no work in this conversion.
// `clippy::not_unsafe_ptr_arg_deref` is allowed **deliberately, and this is a place its premise
// does not hold**: the lint reads any public function taking a raw pointer as one that might be
// handed a caller's dangling value and dereferences it, while what happens here is
// `from_raw_handle`, which *stores* the value — this function dereferences nothing, and the
// kernel's handle table is what the value is later resolved against. Marking it `unsafe` would
// move the launcher's handoff proof out of the doc above and into the signature of every
// caller, `Plane::adopt` included, which is a safe operation for exactly the same reason.
#[expect(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "the lint's premise does not hold: this function stores the caller's handle through `from_raw_handle` and dereferences nothing, so marking it `unsafe` would move the launcher's handoff proof into every caller's signature"
)]
pub fn adopted_pipe(raw: RawHandle) -> io::Result<OwnedHandle> {
    if raw.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the launch handed over no control-channel handle",
        ));
    }
    // SAFETY: `raw` is the pipe handle the confined child's launcher created and passed
    // through the handle list `CreateProcessW` was given, so it names a live named-pipe
    // server handle this process now owns and no other `OwnedHandle` in this process
    // wraps: `Channel::raw_handles` only borrows the launcher's own, and those values never
    // cross the process boundary. The null case is refused above, so this is never
    // `INVALID_HANDLE_VALUE`'s invalid-input spelling of "no object".
    Ok(unsafe { OwnedHandle::from_raw_handle(raw) })
}
