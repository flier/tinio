//! File DACLs. `tinio-api` keeps named-pipe creation and calls [`CurrentUserDacl`] for the
//! same descriptor.


use std::{
    ffi::OsStr,
    fs::File,
    io, iter,
    mem::size_of,
    os::windows::{
        ffi::OsStrExt,
        io::FromRawHandle,
    },
    path::Path,
    ptr,
};

use windows_sys::{
    Win32::{
        Foundation::{
            CloseHandle, ERROR_SUCCESS, GENERIC_WRITE, GetLastError, HANDLE, HLOCAL,
            INVALID_HANDLE_VALUE, LocalFree,
        },
        Security::{
            ACCESS_ALLOWED_ACE, ACL,
            Authorization::{
                EXPLICIT_ACCESS_W, GetNamedSecurityInfoW, SE_FILE_OBJECT, SET_ACCESS,
                SetEntriesInAclW, SetNamedSecurityInfoW,
                TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
            },
            DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetSecurityDescriptorDacl,
            GetTokenInformation, INHERIT_ONLY_ACE, InitializeSecurityDescriptor, NO_INHERITANCE,
            PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
            SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR, SetSecurityDescriptorDacl, TOKEN_QUERY,
            TOKEN_USER, TokenUser,
        },
        Storage::FileSystem::{
            CREATE_NEW, CreateFileW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE,
        },
        System::{
            Memory::{LPTR, LocalAlloc},
            Threading::{GetCurrentProcess, OpenProcessToken},
        },
    },
    core::BOOL,
};

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
/// use tinio_winsec::create_restricted;
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
/// use tinio_winsec::restrict_to_current_user;
///
/// restrict_to_current_user(Path::new(r"C:\state\state")).expect("DACL");
/// ```
pub fn restrict_to_current_user(path: &Path) -> io::Result<()> {
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
/// use tinio_winsec::acl_is_current_user_only;
///
/// assert!(acl_is_current_user_only(Path::new(r"C:\state\state")).expect("read back"));
/// ```
pub fn acl_is_current_user_only(path: &Path) -> io::Result<bool> {
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
fn current_user() -> io::Result<CurrentUser> {
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
pub struct CurrentUserDacl {
    /// The descriptor [`CurrentUserDacl::attributes`] points at.
    descriptor: SECURITY_DESCRIPTOR,
    /// The ACL `descriptor.Dacl` points at, freed by `Drop`.
    acl: *mut ACL,
}

impl CurrentUserDacl {
    pub fn new() -> io::Result<Self> {
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
    pub fn attributes(&self) -> SECURITY_ATTRIBUTES {
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
pub fn descriptor_is_current_user_only(
    path: &Path,
    descriptor: PSECURITY_DESCRIPTOR,
) -> io::Result<bool> {
    let user = current_user()?;
    inspect(path, descriptor, &user)
}

fn inspect(
    path: &Path,
    descriptor: PSECURITY_DESCRIPTOR,
    user: &CurrentUser,
) -> io::Result<bool> {
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
fn win32_error(path: &Path, call: &str, status: u32) -> io::Error {
    // A `BOOL`-returning call need not set a last error, and status `0` would then render as the
    // success message; say what is known rather than repeat that text.
    let message = if status == ERROR_SUCCESS {
        format!("{call} failed without a status")
    } else {
        let meaning = io::Error::from_raw_os_error(status as i32);
        format!("{call} failed: {meaning} (status {status})")
    };
    io::Error::other(format!("{path}: {message}", path = path.display()))
}

/// The UTF-16, NUL-terminated form every `*W` call takes — the file path `CreateFileW`,
/// `SetNamedSecurityInfoW` and `GetNamedSecurityInfoW` want, and the pipe name `CreateNamedPipeW`
/// wants (which never names a file, so it goes in as an `OsStr`).
pub fn to_wide(name: impl AsRef<OsStr>) -> Vec<u16> {
    name.as_ref().encode_wide().chain(iter::once(0)).collect()
}
