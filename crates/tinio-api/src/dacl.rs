//! The crate's one raw-Windows module: named-pipe DACL creation, adoption, and the pipe
//! read-back. File DACLs live in [`tinio_winsec`]; nothing here is reachable off Windows, and the
//! single `#[allow(unsafe_code)]` below is the crate's only `unsafe` site (design decision 20's
//! pattern, `docs/cargo.md`): every caller sees a safe wrapper whose checks are exhaustive and
//! whose failures are `Error`s, never panics, because the invariant is the one Windows imposes
//! rather than one this crate chooses.
//!
//! Task 10's Windows leg checks both installs live — the file's
//! ([`tinio_winsec::acl_is_current_user_only`]) and the pipe's ([`pipe_acl_is_current_user_only`])
//! — so the DACLs the plane's state file and its control channel carry are asserted rather than
//! trusted, and each is built on the same `CurrentUserDacl`/`full_control_acl` pair that wrote them.

#![allow(unsafe_code)]

use std::{
    ffi::OsStr,
    io,
    os::windows::io::{AsRawHandle as _, FromRawHandle, IntoRawHandle, OwnedHandle, RawHandle},
    path::Path,
    ptr,
};

use tokio::net::windows::named_pipe::NamedPipeServer;
use windows_sys::Win32::{
    Foundation::{ERROR_SUCCESS, INVALID_HANDLE_VALUE, LocalFree},
    Security::{
        Authorization::{GetSecurityInfo, SE_KERNEL_OBJECT},
        DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
    },
    Storage::FileSystem::{
        CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, OPEN_EXISTING,
        PIPE_ACCESS_DUPLEX, READ_CONTROL,
    },
    System::Pipes::{
        CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
        PIPE_UNLIMITED_INSTANCES,
    },
};

use crate::{
    _winsec::{CurrentUserDacl, descriptor_is_current_user_only, to_wide, win32_error_message},
    Error,
};

/// Creates the named pipe `name` with a DACL that grants the current user full control and
/// nothing to anyone else, and hands it back as an owned handle — adopted inside the runtime by
/// `adopt_pipe` (Task 4), because only there does the reactor exist that a `NamedPipeServer`
/// needs. `first_instance` is `FILE_FLAG_FIRST_PIPE_INSTANCE`: the single-instance claim every
/// later bind of the same name must fail on.
///
/// The DACL travels in the `SECURITY_ATTRIBUTES` of `CreateNamedPipeW` for the same reason
/// [`tinio_winsec::create_restricted`] attaches one at file create, and the pipe is duplex and
/// byte-mode with remote clients refused — the local-only rule
/// `ServerOptions::reject_remote_clients` asks for, from the layer that can carry the DACL at the
/// same time.
///
/// # Examples
///
/// ```no_run
/// use tinio_api::dacl::create_pipe;
///
/// let handle = create_pipe(r"\\.\pipe\tinio-mine", true).expect("first instance");
/// drop(handle);
/// ```
pub fn create_pipe(name: &str, first_instance: bool) -> io::Result<OwnedHandle> {
    let dacl = CurrentUserDacl::new()?;
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
            &dacl.attributes(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // Ownership moves to the caller on success; the DACL buffer is dropped with `dacl`, which is
    // the point of building the descriptor on the stack: Windows copies it at creation.
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
/// `dacl` is the crate's one raw-Windows module (its own module doc says so), and
/// `OwnedHandle::from_raw_handle` is unsafe by construction: it promises that the value
/// names a live handle of the right kind and that ownership moves with it. Both halves of
/// that promise are the *launcher's* here rather than this crate's. The launcher ran
/// unconfined, created the pipe with [`create_pipe`] and passed the raw value across
/// `CreateProcessW`; this process inherited it, nothing else in this process has touched
/// it, and no second owner of the value exists — which is what makes the conversion sound.
///
/// **Why the child needs it at all**: inside a container this process's identity is the
/// package SID, not the user, so `CreateNamedPipeW` on a current-user-only DACL is refused
/// (`ERROR_ACCESS_DENIED`) — the child cannot make these objects, so it adopts the ones its
/// launcher made. See `bind::Channel::raw_handles` for the launcher's half; one call per
/// instance, because the channel is a pool and the child serves on every instance it was
/// handed.
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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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

/// Reads the named pipe's DACL back and answers whether every ACE names the current user's SID
/// with full control — the pipe twin of [`tinio_winsec::acl_is_current_user_only`], with the same
/// trustee rule, the same `Ok(false)` for an absent or empty DACL, the same [`Error::State`]
/// mapping and the same `LocalFree` on every path. Task 10's Windows-gated leg is the caller: it
/// is what proves the channel `Auth::Local` leaves `/metrics` and `/openapi.json` open on is as
/// private as the unix socket.
///
/// **The descriptor is read through a handle, not a name**: `GetSecurityInfo` with
/// `SE_KERNEL_OBJECT` is what a pipe object answers to — `GetNamedSecurityInfoW`, which
/// [`tinio_winsec::acl_is_current_user_only`] uses, takes a filesystem path and has no pipe form.
/// The handle is opened for `READ_CONTROL` alone, which is the one right this call needs, and it
/// is an [`OwnedHandle`], so it is closed on every path including the failures below.
///
/// # Examples
///
/// ```no_run
/// use tinio_api::dacl::{create_pipe, pipe_acl_is_current_user_only};
///
/// let name = r"\\.\pipe\tinio-mine";
/// let handle = create_pipe(name, true).expect("first instance");
/// assert!(pipe_acl_is_current_user_only(name).expect("read back"));
/// # drop(handle);
/// ```
pub fn pipe_acl_is_current_user_only(name: &str) -> Result<bool, Error> {
    let path = Path::new(name);
    let handle = open_for_read_control(name).map_err(|err| Error::State {
        path: path.to_path_buf(),
        message: format!("the pipe cannot be opened to read its DACL back: {err}"),
    })?;
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    let status = unsafe {
        GetSecurityInfo(
            handle.as_raw_handle().cast(),
            SE_KERNEL_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != ERROR_SUCCESS {
        // Nothing was allocated: on a failed call the out-parameter stays null. The handle goes
        // with `handle`'s own drop, on this path and on every one below it.
        return Err(win32_error(path, "GetSecurityInfo", status));
    }
    // The walk is the file read-back's, unchanged: the ACEs, the mask and the trustee rule are
    // the same ones `full_control_acl` wrote into this pipe's descriptor at creation.
    //
    // SAFETY: `descriptor` is the allocation `GetSecurityInfo` returned above — the
    // `status != ERROR_SUCCESS` return refused every path on which the out-parameter was not
    // written, so what is left here is that call's own output: a live, aligned, readable
    // security descriptor, with its DACL and ACEs inside the same allocation, which is exactly
    // what `descriptor_is_current_user_only`'s `# Safety` section asks for. This function owns
    // it and gives it up in the `LocalFree` below, after the walk.
    let verdict = unsafe { descriptor_is_current_user_only(path, descriptor) }
        .map_err(|err| state_error(path, err));
    unsafe { LocalFree(descriptor) };
    verdict
}

/// Opens `name` for `READ_CONTROL` — the one access right the pipe's read-back needs, and no
/// more: the check asks for no **data** right, so it cannot act on what it inspects. It is a
/// client connect like any other, and that is not a contradiction — the `CreateFileW` below
/// consumes one of the server's listening instances, exactly as the Windows leg in
/// `tests/management.rs` documents, and the descriptor it then reads is the one the create call
/// installed.
///
/// `READ_CONTROL` is `Win32::Storage::FileSystem`'s constant, which is where it belongs rather
/// than in `Security`: a named pipe is a file object, and its access rights are the filesystem
/// module's `FILE_ACCESS_RIGHTS` — the same module and feature this file already takes
/// `CreateFileW`, `FILE_ALL_ACCESS` and `OPEN_EXISTING` from.
///
/// The share mode is `0`: a pipe instance is a client's exclusive connection for as long as its
/// handle lives, and the server's own handle to the same instance is not a second open of it.
fn open_for_read_control(name: &str) -> io::Result<OwnedHandle> {
    let handle = unsafe {
        CreateFileW(
            to_wide(OsStr::new(name)).as_ptr(),
            READ_CONTROL,
            // An exclusive share mode is the pipe client's own, and the flags are none: the
            // server's instances are overlapped, but that is a property of *its* handle.
            0,
            ptr::null(),
            OPEN_EXISTING,
            0,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

/// The [`Error::State`] a failed Win32 call reports, in the one wording
/// [`tinio_winsec::win32_error_message`] defines: the sentence is the shared part, and the path is
/// this error type's own field rather than text spliced in front of it — which is what keeps the
/// rendered error from naming the pipe twice.
fn win32_error(path: &Path, call: &str, status: u32) -> Error {
    Error::State {
        path: path.to_path_buf(),
        message: win32_error_message(call, status),
    }
}

fn state_error(path: &Path, err: io::Error) -> Error {
    Error::State {
        path: path.to_path_buf(),
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{Error, win32_error};

    /// **A read-back failure is `Error::State`'s and the path is its own field**, so the message is
    /// [`tinio_winsec::win32_error_message`]'s sentence alone: this is the half of the wording
    /// split that stays here, and what would break it — a message carrying the path as well —
    /// renders the pipe's name twice, once from the field and once from the text.
    #[test]
    fn a_failed_call_reports_the_path_as_the_state_fields_own() {
        let err = win32_error(Path::new(r"\\.\pipe\tinio-mine"), "GetSecurityInfo", 5);
        let Error::State { path, message } = &err else {
            panic!("a failed Win32 call is a state failure");
        };
        assert_eq!(path.as_path(), Path::new(r"\\.\pipe\tinio-mine"));
        assert!(
            message.starts_with("GetSecurityInfo failed: "),
            "the shared sentence names the call that failed: {message}"
        );
        assert!(
            message.ends_with(" (status 5)"),
            "it closes with the decimal status, so a log reader can act on the code: {message}"
        );
        assert_eq!(
            err.to_string().matches(r"\\.\pipe\tinio-mine").count(),
            1,
            "the path is the field's alone, so the rendered error must not repeat it: {err}"
        );
    }
}
