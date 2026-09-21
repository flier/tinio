//! The crate's one raw-Windows module: the DACL helpers, and — from Task 2 on — the named
//! pipe's DACL-carrying creation and adoption. Nothing here is reachable off Windows, and the
//! single `#[allow(unsafe_code)]` below is the crate's only `unsafe` site (design decision 20's
//! pattern, `docs/cargo.md`): every caller sees a safe wrapper whose checks are exhaustive and
//! whose failures are `Error`s, never panics, because the invariant is the one Windows imposes
//! rather than one this crate chooses.
//!
//! Both read-backs Task 10's Windows leg checks the installs with live here — the file's
//! ([`acl_is_current_user_only`]) and the pipe's ([`pipe_acl_is_current_user_only`]) — so the
//! DACLs the plane's two files and its control channel carry are asserted rather than trusted,
//! and each is built on the same `CurrentUserDacl`/`full_control_acl` pair that wrote them.

#![allow(unsafe_code)]

use std::{
    ffi::OsStr,
    fs::File,
    io, iter,
    mem::size_of,
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle as _, FromRawHandle, IntoRawHandle, OwnedHandle},
    },
    path::Path,
    ptr,
};

use tokio::net::windows::named_pipe::NamedPipeServer;
use windows_sys::{
    Win32::{
        Foundation::{
            CloseHandle, ERROR_SUCCESS, GENERIC_WRITE, GetLastError, HANDLE, HLOCAL,
            INVALID_HANDLE_VALUE, LocalFree,
        },
        Security::{
            ACCESS_ALLOWED_ACE, ACL,
            Authorization::{
                EXPLICIT_ACCESS_W, GetNamedSecurityInfoW, GetSecurityInfo, SE_FILE_OBJECT,
                SE_KERNEL_OBJECT, SET_ACCESS, SetEntriesInAclW, SetNamedSecurityInfoW,
                TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
            },
            DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetSecurityDescriptorDacl,
            GetTokenInformation, INHERIT_ONLY_ACE, InitializeSecurityDescriptor, NO_INHERITANCE,
            PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
            SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR, SetSecurityDescriptorDacl, TOKEN_QUERY,
            TOKEN_USER, TokenUser,
        },
        Storage::FileSystem::{
            CREATE_NEW, CreateFileW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_NORMAL,
            FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING, PIPE_ACCESS_DUPLEX, READ_CONTROL,
        },
        System::{
            Memory::{LPTR, LocalAlloc},
            Pipes::{
                CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
                PIPE_UNLIMITED_INSTANCES,
            },
            Threading::{GetCurrentProcess, OpenProcessToken},
        },
    },
    core::BOOL,
};

use crate::Error;

/// `ACCESS_ALLOWED_ACE_TYPE` (`winnt.h`, `0`): the only ACE type a restricted DACL may carry.
///
/// `windows-sys` declares it in `Win32::System::SystemServices`, whose feature is not in this
/// crate's list and whose module is 5338 generated lines in 0.61.2 — one constant is not worth
/// compiling it — so the fixed ABI value is written here, narrowed to the `u8` the ACE header
/// field it is compared against is.
const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

/// `SECURITY_DESCRIPTOR_REVISION` (`winnt.h`, `1`), for the same reason as the ACE type above:
/// the same `SystemServices` module, and `InitializeSecurityDescriptor` is the only caller.
const SECURITY_DESCRIPTOR_REVISION: u32 = 1;

/// Creates `path` — which must not exist yet — with the current-user-only DACL attached from its
/// first instant, and hands back the write handle.
///
/// `CREATE_NEW` is what makes the promise hold: Windows applies `lpSecurityAttributes` only when
/// it creates, so a call that opens an object someone else put there would truncate it and keep
/// its security descriptor — a pre-planted file under a directory with inherited grants would
/// keep them, and the token would land in it. The caller removes a stale temp first and this call
/// refuses anything that appeared in the meantime (`ERROR_FILE_EXISTS`, an `Err`, never a reuse).
///
/// Windows decides access when a handle is **opened**, so the create-time form is what makes the
/// invariant hold: a file that existed under the directory's inherited ACL for even a moment could
/// be opened by a reader that keeps the handle's access, and the token is what that file holds.
/// `State::write` still runs the path form ([`restrict_to_current_user`]) before the first byte,
/// as the re-assertion that refuses a descriptor this call could not install. The DACL therefore
/// travels in the `SECURITY_ATTRIBUTES` of `CreateFileW` — the same value Task 2's `create_pipe`
/// passes to `CreateNamedPipeW`.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::dacl::create_restricted;
///
/// let mut file = create_restricted(Path::new(r"C:\state\state.tmp")).expect("restricted file");
/// std::io::copy(&mut std::io::stdin(), &mut file).expect("write");
/// ```
pub fn create_restricted(path: &Path) -> io::Result<File> {
    let dacl = CurrentUserDacl::new()?;
    let wide = to_wide(path);
    // The share mode is the one `std::fs::OpenOptions` uses by default, and it has to be: the
    // rename in `State::write` fails with a sharing violation unless the open handle permits
    // delete. The handle is private by DACL, not by share mode.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            &dacl.attributes(),
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32
        ));
    }
    // The handle is ours now, and `File` closes it; the DACL is already how the file exists.
    Ok(unsafe { File::from_raw_handle(handle) })
}

/// Creates the named pipe `name` with a DACL that grants the current user full control and
/// nothing to anyone else, and hands it back as an owned handle — adopted inside the runtime by
/// `adopt_pipe` (Task 4), because only there does the reactor exist that a `NamedPipeServer`
/// needs. `first_instance` is `FILE_FLAG_FIRST_PIPE_INSTANCE`: the single-instance claim every
/// later bind of the same name must fail on.
///
/// The DACL travels in the `SECURITY_ATTRIBUTES` of `CreateNamedPipeW` for the reason
/// [`create_restricted`] gives, and the pipe is duplex and byte-mode with remote clients refused —
/// the local-only rule `ServerOptions::reject_remote_clients` asks for, from the layer that can
/// carry the DACL at the same time.
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

/// Installs a DACL that grants the current user full control and nothing to anyone else on a path
/// that already exists.
///
/// The path form, and every Windows `state` write runs it on its temp file before a byte is
/// written. It is the second line rather than the only one: [`create_restricted`] created that
/// file with the same DACL, and this call re-asserts it, so a descriptor that did not take at
/// creation is refused here instead of being published by the rename. It cannot replace the
/// create-time form — a DACL installed after the create leaves a window in which the file exists
/// under the directory's inherited ACL — and a failure is an `Error`, never a partial write.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::dacl::restrict_to_current_user;
///
/// restrict_to_current_user(Path::new(r"C:\state\state")).expect("DACL");
/// ```
pub fn restrict_to_current_user(path: &Path) -> Result<(), Error> {
    let user = current_user()?; // TOKEN_USER, freed on every path
    let acl =
        full_control_acl(&user).map_err(|status| win32_error(path, "SetEntriesInAclW", status))?;
    let wide = to_wide(path);
    let status = unsafe {
        SetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            // `PROTECTED_DACL_SECURITY_INFORMATION` is what makes `DACL_SECURITY_INFORMATION` mean
            // *this* list. Without it Windows merges the parent directory's inheritable ACEs into
            // the DACL we just set — measured on this host, a file under `%TEMP%` came back with
            // `CodexSandboxUsers:(I)(M)` and `BUILTIN\Administrators:(I)(F)` beside our entry —
            // so the file that holds the token would keep whatever access the directory grants,
            // and the read-back below would answer `false` about the DACL this call just wrote.
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            acl,
            ptr::null_mut(),
        )
    };
    unsafe { LocalFree(acl as HLOCAL) }; // freed on both paths, before the branch
    if status != ERROR_SUCCESS {
        return Err(win32_error(path, "SetNamedSecurityInfoW", status));
    }
    Ok(())
}

/// Reads the file's DACL back and answers whether every ACE names the current user's SID with
/// full control — an absent or empty DACL is `false`, so "restricted" is never inferred from
/// silence. Task 10's Windows-gated leg is the caller: without it the DACL is a claim.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::dacl::acl_is_current_user_only;
///
/// assert!(acl_is_current_user_only(Path::new(r"C:\state\state")).expect("read back"));
/// ```
pub fn acl_is_current_user_only(path: &Path) -> Result<bool, Error> {
    let user = current_user()?;
    let wide = to_wide(path);
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // The DACL comes out of the returned descriptor rather than through `ppdacl`, so the
    // pointer the walk holds and the allocation this call frees are the same object.
    let status = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != ERROR_SUCCESS {
        // Nothing was allocated: on a failed call the out-parameter stays null.
        return Err(win32_error(path, "GetNamedSecurityInfoW", status));
    }
    let verdict = inspect(path, descriptor, &user);
    unsafe { LocalFree(descriptor) }; // ours since the call above, released on both paths
    verdict
}

/// Reads the named pipe's DACL back and answers whether every ACE names the current user's SID
/// with full control — the pipe twin of [`acl_is_current_user_only`], with the same trustee rule,
/// the same `Ok(false)` for an absent or empty DACL, the same [`Error::State`] mapping and the
/// same `LocalFree` on every path. Task 10's Windows-gated leg is the caller: it is what proves
/// the channel `Auth::Local` leaves `/metrics` and `/openapi.json` open on is as private as the
/// unix socket.
///
/// **The descriptor is read through a handle, not a name**: `GetSecurityInfo` with
/// `SE_KERNEL_OBJECT` is what a pipe object answers to — `GetNamedSecurityInfoW`, which
/// [`acl_is_current_user_only`] uses, takes a filesystem path and has no pipe form. The handle is
/// opened for `READ_CONTROL` alone, which is the one right this call needs, and it is an
/// [`OwnedHandle`], so it is closed on every path including the failures below.
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
    let user = current_user()?;
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
    let verdict = inspect(path, descriptor, &user);
    unsafe { LocalFree(descriptor) }; // ours since the call above, released on both paths
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

/// The current process's user: the SID points into a `TOKEN_USER` buffer this value owns, so it
/// cannot outlive it, and the token handle is closed with it.
#[allow(non_snake_case)] // the field is Win32's own name for it
struct CurrentUser {
    /// The current user's SID, inside `buffer`.
    Sid: PSID,
    /// The `TOKEN_USER` the SID lives in, allocated by `LocalAlloc` — the pair `LocalFree` wants;
    /// the manifest's `Win32_System_Memory` feature is what compiles that module for this one call.
    buffer: HLOCAL,
    /// The process token the buffer was read through.
    token: HANDLE,
}

impl Drop for CurrentUser {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.buffer);
            CloseHandle(self.token);
        }
    }
}

/// The current process's own user token, as a SID.
fn current_user() -> Result<CurrentUser, Error> {
    let mut token: HANDLE = ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(win32_error(token_path(), "OpenProcessToken", unsafe {
            GetLastError()
        }));
    }
    // Sized first, then read: a SID's length is not a constant, and the first call's job is to
    // answer `needed` (its own return is `FALSE` with `ERROR_INSUFFICIENT_BUFFER` by design).
    let mut needed = 0u32;
    unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut needed) };
    if needed == 0 {
        let status = unsafe { GetLastError() };
        unsafe { CloseHandle(token) };
        return Err(win32_error(token_path(), "GetTokenInformation", status));
    }
    let buffer = unsafe { LocalAlloc(LPTR, needed as usize) };
    if buffer.is_null() {
        let status = unsafe { GetLastError() };
        unsafe { CloseHandle(token) };
        return Err(win32_error(token_path(), "LocalAlloc", status));
    }
    if unsafe { GetTokenInformation(token, TokenUser, buffer, needed, &mut needed) } == 0 {
        let status = unsafe { GetLastError() };
        unsafe {
            LocalFree(buffer);
            CloseHandle(token);
        }
        return Err(win32_error(token_path(), "GetTokenInformation", status));
    }
    let user = unsafe { &*(buffer as *const TOKEN_USER) };
    Ok(CurrentUser {
        Sid: user.User.Sid,
        buffer,
        token,
    })
}

/// The one ACE list this module ever installs: the current user, `FILE_ALL_ACCESS`, no
/// inheritance. `SetEntriesInAclW` allocates it — the caller `LocalFree`s it — and both the
/// path form ([`restrict_to_current_user`]) and the create form ([`CurrentUserDacl`]) go through
/// here, so the list the read-back recognises and the list the crate writes cannot drift apart.
/// The Win32 status comes back on failure.
fn full_control_acl(user: &CurrentUser) -> Result<*mut ACL, u32> {
    let entry = EXPLICIT_ACCESS_W {
        // `FILE_ALL_ACCESS` (`0x1F_01_FF`), never `GENERIC_ALL`: these bits are the file
        // object's, and the read-back below compares against this same constant.
        grfAccessPermissions: FILE_ALL_ACCESS,
        grfAccessMode: SET_ACCESS,
        grfInheritance: NO_INHERITANCE, // the whole list, not a delta
        Trustee: TRUSTEE_W {
            // A SID, not a name — and this is the pair of lines a reader gets wrong, because
            // `TRUSTEE_IS_NAME` is `1` and `TRUSTEE_IS_USER` is `1` too (as a `TRUSTEE_TYPE`),
            // so writing the type constant in the form field compiles and then makes
            // `SetEntriesInAclW` read the SID's bytes as a name string: `ERROR_NONE_MAPPED`,
            // and every Windows `state` write fails. `TRUSTEE_IS_SID` is `0`.
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: user.Sid as *mut u16,
            // `pMultipleTrustee` and `MultipleTrusteeOperation`, both defaulted: no trustee
            // chain (and `windows-sys` provides the `Default`).
            ..Default::default()
        },
    };
    let mut acl: *mut ACL = ptr::null_mut();
    let status = unsafe { SetEntriesInAclW(1, &entry, ptr::null_mut(), &mut acl) };
    if status != ERROR_SUCCESS {
        return Err(status);
    }
    Ok(acl)
}

/// The [`full_control_acl`] list as the [`SECURITY_ATTRIBUTES`] a create call takes, for
/// `CreateFileW` here and Task 2's `CreateNamedPipeW`.
///
/// The descriptor is absolute-format and marked `SE_DACL_PROTECTED`, which is what keeps the
/// parent directory's inheritable ACEs out of the new object's DACL — the same protection
/// [`restrict_to_current_user`] asks for through `PROTECTED_DACL_SECURITY_INFORMATION`, and for
/// the same measured reason.
struct CurrentUserDacl {
    /// The descriptor [`CurrentUserDacl::attributes`] points at.
    descriptor: SECURITY_DESCRIPTOR,
    /// The ACL `descriptor.Dacl` points at, freed by `Drop`.
    acl: *mut ACL,
}

impl CurrentUserDacl {
    fn new() -> io::Result<Self> {
        let user = current_user().map_err(io::Error::other)?;
        let mut descriptor = SECURITY_DESCRIPTOR::default();
        // The descriptor is built before the ACL is allocated, so no early return here owns
        // anything to free.
        if unsafe {
            InitializeSecurityDescriptor(
                ptr::from_mut(&mut descriptor).cast(),
                SECURITY_DESCRIPTOR_REVISION,
            )
        } == 0
        {
            return Err(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32
            ));
        }
        let acl = full_control_acl(&user)
            .map_err(|status| io::Error::from_raw_os_error(status as i32))?;
        if unsafe { SetSecurityDescriptorDacl(ptr::from_mut(&mut descriptor).cast(), 1, acl, 0) }
            == 0
        {
            let status = unsafe { GetLastError() };
            unsafe { LocalFree(acl as HLOCAL) };
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        descriptor.Control |= SE_DACL_PROTECTED;
        Ok(Self { descriptor, acl })
    }

    /// The value a create call takes; valid for as long as `self` lives, since the descriptor it
    /// points at lives here and not in the caller's stack frame.
    fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: ptr::addr_of!(self.descriptor).cast_mut().cast(),
            // `FALSE`: a child process has no business holding this handle.
            bInheritHandle: 0,
        }
    }
}

impl Drop for CurrentUserDacl {
    fn drop(&mut self) {
        unsafe { LocalFree(self.acl as HLOCAL) };
    }
}

/// The ACE walk behind [`acl_is_current_user_only`]: `true` only when the descriptor's DACL is
/// present, non-empty, and every ACE in it is an access-allowed ACE that grants the current user
/// `FILE_ALL_ACCESS` without inheriting it.
fn inspect(
    path: &Path,
    descriptor: PSECURITY_DESCRIPTOR,
    user: &CurrentUser,
) -> Result<bool, Error> {
    let mut present: BOOL = 0;
    let mut dacl: *mut ACL = ptr::null_mut();
    let mut defaulted: BOOL = 0;
    let ok =
        unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) };
    if ok == 0 {
        return Err(win32_error(path, "GetSecurityDescriptorDacl", unsafe {
            GetLastError()
        }));
    }
    if present == 0 || dacl.is_null() {
        // No DACL is every access to every trustee, so it is the opposite of restricted.
        return Ok(false);
    }
    let count = unsafe { (*dacl).AceCount };
    if count == 0 {
        return Ok(false);
    }
    for index in 0..u32::from(count) {
        let mut raw = ptr::null_mut();
        if unsafe { GetAce(dacl, index, &mut raw) } == 0 {
            return Err(win32_error(path, "GetAce", unsafe { GetLastError() }));
        }
        // The type first, through the raw pointer: a deny or audit ACE is not read as an
        // access-allowed one, and anything but an allow fails the verdict on the spot.
        let ace_type = unsafe { (*raw.cast::<ACCESS_ALLOWED_ACE>()).Header.AceType };
        if ace_type != ACCESS_ALLOWED_ACE_TYPE {
            return Ok(false);
        }
        let ace = unsafe { &*(raw as *const ACCESS_ALLOWED_ACE) };
        if u32::from(ace.Header.AceFlags) & INHERIT_ONLY_ACE != 0
            || ace.Mask != FILE_ALL_ACCESS
            || unsafe { EqualSid(ptr::addr_of!(ace.SidStart) as PSID, user.Sid) } == 0
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The pseudo-path the token calls report under: they act on the process, not on a file.
fn token_path() -> &'static Path {
    Path::new("<process token>")
}

/// Builds the [`Error::State`] a failed Win32 call reports: the call's name, the decimal status
/// and the OS's own message for it, so a log reader does not have to look the number up. `path` is
/// what the failure is attributed to — a file path, a pipe name, or [`token_path`] for the calls
/// that act on the process rather than on an object.
fn win32_error(path: &Path, call: &str, status: u32) -> Error {
    // A `BOOL`-returning call need not set a last error, and status `0` would then render as the
    // success message; say what is known rather than repeat that text.
    let message = if status == ERROR_SUCCESS {
        format!("{call} failed without a status")
    } else {
        let meaning = io::Error::from_raw_os_error(status as i32);
        format!("{call} failed: {meaning} (status {status})")
    };
    Error::State {
        path: path.to_path_buf(),
        message,
    }
}

/// The UTF-16, NUL-terminated form every `*W` call takes — the file path `CreateFileW`,
/// `SetNamedSecurityInfoW` and `GetNamedSecurityInfoW` want, and the pipe name `CreateNamedPipeW`
/// wants (which never names a file, so it goes in as an `OsStr`).
fn to_wide(name: impl AsRef<OsStr>) -> Vec<u16> {
    name.as_ref().encode_wide().chain(iter::once(0)).collect()
}
