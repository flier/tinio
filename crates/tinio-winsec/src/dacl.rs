//! File DACLs. `tinio-api` keeps named-pipe creation and calls [`CurrentUserDacl`] for the
//! same descriptor.
//!
//! **The manifest denies `unsafe_code` and the exception sits on this module, because this
//! module *is* the crate's Win32 surface**: every function below is a call into the same
//! security API, so a per-site attribute would repeat one permit a dozen times without adding
//! an audit step — the substance is the `// SAFETY:` argument each block carries, and those
//! are per site. The permit is exactly one module wide (`lib.rs` and any module added beside
//! this one stay under the manifest's `deny`), which is what the crate-level `allow` this
//! replaces was not.
#![allow(unsafe_code)]

use std::{
    ffi::OsStr,
    fs::File,
    io, iter,
    mem::size_of,
    ops::ControlFlow,
    os::windows::{ffi::OsStrExt, io::FromRawHandle},
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
                SetEntriesInAclW, SetNamedSecurityInfoW, TRUSTEE_IS_SID, TRUSTEE_IS_USER,
                TRUSTEE_W,
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
///
/// **Public because it is one fact with more than one reader**: [`walk_dacl`] has to tell an
/// access-allowed entry from every other kind before it reads the entry's fields, and
/// `tinio-sandbox-windows`'s fixtures build ACEs carrying the same type code rather than
/// transcribing the zero beside it. Exported under the arm's own name rather than re-named, so
/// a reader meets the header's spelling.
pub const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

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
    //
    // SAFETY: each argument is one this statement owns. `wide` is the NUL-terminated UTF-16
    // path built above and lives to the end of the function; `dacl` is a live local and the
    // kernel reads the `SECURITY_ATTRIBUTES` the temporary borrows *during* the call —
    // copying the descriptor into the new file's own, which is why the DACL outlives it (the
    // test below drops the descriptor, reads the DACL back and finds it there). The four
    // remaining arguments are an access mask, a share mode, a creation disposition and two
    // constants; the template handle is null, which is the documented "no template". What
    // would break it: a `path` that does not name a file object — this function's contract —
    // or a `dacl` moved between `attributes()` and this call, which the borrow's lifetime
    // inside this statement excludes.
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
        // SAFETY: a thread-local read with no preconditions, taken as the first call after the
        // failure so nothing has overwritten the status it reports.
        Err(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32
        ))
    } else {
        // The handle is ours now, and `File` closes it; the DACL is already how the file exists.
        //
        // SAFETY: `handle` is the owned handle `CreateFileW` returned, and the `if` above refused
        // `INVALID_HANDLE_VALUE`, so it is a live file handle this process alone owns — nothing
        // else closes it, and `File` is what will. `GENERIC_WRITE` is the access the write path
        // that consumes the returned value needs. What would break it: a `path` naming something
        // other than a file object — a device namespace, say — which this function's contract
        // excludes; its callers pass state and config paths.
        Ok(unsafe { File::from_raw_handle(handle) })
    }
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
    // SAFETY: `wide` is the NUL-terminated path built above and alive for the call; `acl` is
    // the `full_control_acl` allocation this function owns and releases on both paths below —
    // the call copies the ACL into the object's own descriptor, which is what lets the
    // `LocalFree` follow it without the object losing its DACL. The owner, group and SACL
    // arguments are null because only the DACL is being replaced, and that is their documented
    // "not asked for". What would break it: releasing `acl` before this call, or handing in a
    // list `full_control_acl` did not build.
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
    // SAFETY: `acl` is this call's own `full_control_acl` allocation — `SetEntriesInAclW`
    // allocates and `LocalFree` is its documented release — and this is the one free, placed
    // after the status is read and before the pointer's last use would have been.
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
    //
    // SAFETY: `wide` is the NUL-terminated path built above and alive for the call, and
    // `descriptor` is a valid out-parameter — the call allocates the security descriptor and
    // writes it there. The owner, group, DACL and SACL out-pointers are null, the documented
    // "not asked for", which is what keeps this call's one allocation the descriptor alone:
    // the walk below pulls the DACL out of it, and the single `LocalFree` that follows is that
    // allocation's only release.
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
    // SAFETY: `descriptor` is the allocation the call above returned — `GetNamedSecurityInfoW`
    // documents `LocalFree` as its release — and this is its one free, after the walk that
    // borrows it has finished and before the verdict travels out.
    unsafe { LocalFree(descriptor) }; // ours since the call above, released on both paths
    verdict
}

/// The current process's user: the SID points into a `TOKEN_USER` buffer this value owns, so it
/// cannot outlive it, and the token handle is closed with it.
#[expect(
    non_snake_case,
    reason = "`Sid` is Win32's own name for the field, and the struct mirrors the API's `TOKEN_USER` shape"
)]
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
        // SAFETY: both values are this value's own, each produced by a call that documents its
        // release — `LocalAlloc`/`LocalFree` for the buffer, `OpenProcessToken`/`CloseHandle`
        // for the token — and `Drop` runs exactly once, since the struct is neither `Copy` nor
        // `Clone` and every path that builds one hands it the two allocations it closes over.
        // What would break it: a `CurrentUser` assembled by any other route, which would make
        // both of these some other owner's resource.
        unsafe {
            LocalFree(self.buffer);
            CloseHandle(self.token);
        }
    }
}

/// The current process's own user token, as a SID.
fn current_user() -> io::Result<CurrentUser> {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: `GetCurrentProcess` is the pseudo-handle Win32 defines for this process — always
    // valid here and deliberately never closed, which is why no `CloseHandle` follows it — and
    // `token` is a valid out-parameter for the handle the call opens. The failure path opens
    // nothing, so the early return below closes nothing.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        // SAFETY: a thread-local read with no preconditions. `token_path` and the string
        // beside it are ordinary Rust that touches no Win32 state, so the status is still the
        // failing call's.
        return Err(win32_error(token_path(), "OpenProcessToken", unsafe {
            GetLastError()
        }));
    }
    // Sized first, then read: a SID's length is not a constant, and the first call's job is to
    // answer `needed` (its own return is `FALSE` with `ERROR_INSUFFICIENT_BUFFER` by design).
    let mut needed = 0u32;
    // SAFETY: this is the documented sizing call — a null buffer with a zero length asks for
    // the size alone — and `needed` is the out-parameter it writes. `token` is the handle the
    // call above returned and is open here; its own release is the `CloseHandle` every path
    // below either reaches or is the success path of.
    unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut needed) };
    if needed == 0 {
        // SAFETY: a thread-local read with no preconditions, as the first call after the
        // failure.
        let status = unsafe { GetLastError() };
        // SAFETY: `token` is this function's own handle from `OpenProcessToken`, and this is
        // its one `CloseHandle` on this path: the early return below means the buffer path
        // never sees it.
        unsafe { CloseHandle(token) };
        return Err(win32_error(token_path(), "GetTokenInformation", status));
    }
    // SAFETY: `needed` is the byte count the sizing call just reported, and `LPTR` is the
    // zero-initialized fixed-memory flavour this buffer wants (the local heap's alignment
    // covers the `TOKEN_USER` read below). The result is this function's own allocation,
    // released exactly once: by the `LocalFree` below on the failure path, and by
    // `CurrentUser::drop` on the success one.
    let buffer = unsafe { LocalAlloc(LPTR, needed as usize) };
    if buffer.is_null() {
        // SAFETY: a thread-local read with no preconditions beyond the failing call being what
        // last set it.
        let status = unsafe { GetLastError() };
        // SAFETY: as above — this path's own single `CloseHandle` for the token.
        unsafe { CloseHandle(token) };
        return Err(win32_error(token_path(), "LocalAlloc", status));
    }
    // SAFETY: `buffer` is the allocation above — `needed` bytes, zero-initialized, aligned for
    // any type the heap hands out — and the length passed is that same allocation's, which is
    // the buffer contract this call's `TokenUser` class asks for.
    if unsafe { GetTokenInformation(token, TokenUser, buffer, needed, &mut needed) } == 0 {
        // SAFETY: a thread-local read with no preconditions.
        let status = unsafe { GetLastError() };
        // SAFETY: this path owns both values and keeps neither: the buffer is freed once here,
        // the token closed once here, and the early return below means neither reaches the
        // success path where `CurrentUser` would own them.
        unsafe {
            LocalFree(buffer);
            CloseHandle(token);
        }
        return Err(win32_error(token_path(), "GetTokenInformation", status));
    }
    // SAFETY: the call above succeeded, so `buffer` holds the `TOKEN_USER` it wrote — the
    // documented shape of the `TokenUser` class, read back with the same pointer and length it
    // was written through. The cast is aligned (a heap block is) and the value lies inside
    // this function's own allocation. The reference is not stored: what leaves this frame is
    // `user.User.Sid`, a pointer *into* `buffer`, and `CurrentUser` owns the buffer for
    // exactly as long as it hands that SID out — its own doc states the pairing.
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
    // SAFETY: `entry` is the initialized `EXPLICIT_ACCESS_W` above, and its one `ptstrName` is
    // the caller's live SID — `user` is borrowed for the whole call, so the trustee cannot
    // dangle mid-call, and the form/type pair beside it is what makes the API read that field
    // as a SID rather than as a name. The null old ACL is the documented "no existing list to
    // merge with", and `acl` is a valid out-parameter. On success the value is this caller's
    // own allocation: `SetEntriesInAclW` allocates it and documents `LocalFree` as its
    // release, which is what both `Drop`s and `restrict_to_current_user` do.
    let status = unsafe { SetEntriesInAclW(1, &entry, ptr::null_mut(), &mut acl) };
    if status != ERROR_SUCCESS {
        return Err(status);
    }
    Ok(acl)
}

/// The `full_control_acl` list as the [`SECURITY_ATTRIBUTES`] a create call takes, for
/// `CreateFileW` here and Task 2's `CreateNamedPipeW`.
///
/// The descriptor is absolute-format and marked `SE_DACL_PROTECTED`, which is what keeps the
/// parent directory's inheritable ACEs out of the new object's DACL — the same protection
/// [`restrict_to_current_user`] asks for through `PROTECTED_DACL_SECURITY_INFORMATION`, and for
/// the same measured reason.
///
/// **The value owns both things the descriptor is built from** — the `SECURITY_DESCRIPTOR` in
/// its own storage and the ACL that descriptor points at — so it must outlive every call that
/// reads them, and [`attributes`](Self::attributes) is what hands them to a create call. A
/// pointer taken after this value drops names freed memory, and one taken before a move names
/// the old address: this value *is* the lifetime the descriptor is valid for.
///
/// # Examples
///
/// ```
/// use tinio_winsec::CurrentUserDacl;
///
/// let dacl = CurrentUserDacl::new().expect("the current user's DACL");
/// let attributes = dacl.attributes();
/// // The structure and the ACL behind it are the ones a create call gets.
/// assert!(!attributes.lpSecurityDescriptor.is_null());
/// assert_eq!(
///     attributes.bInheritHandle, 0,
///     "a child process gets no handle"
/// );
/// ```
pub struct CurrentUserDacl {
    /// The descriptor [`CurrentUserDacl::attributes`] points at.
    descriptor: SECURITY_DESCRIPTOR,
    /// The ACL `descriptor.Dacl` points at, freed by `Drop`.
    acl: *mut ACL,
}

impl CurrentUserDacl {
    /// The current user's full-control DACL, as the value a create call takes.
    ///
    /// The two Win32 allocations it owns are the descriptor — built here, in this struct's
    /// storage — and the ACL `full_control_acl` returns; both are released by this value's
    /// `Drop`, and neither outlives it. A failure leaves nothing allocated: the descriptor is
    /// built before the ACL exists, and the one path that has an ACL and then fails frees it
    /// before returning.
    ///
    /// # Errors
    ///
    /// `io::Error::other` when the process token cannot be read — the message names the
    /// failing call and the Win32 status — and the raw OS error of
    /// `InitializeSecurityDescriptor` or `SetSecurityDescriptorDacl`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_winsec::CurrentUserDacl;
    ///
    /// let dacl = CurrentUserDacl::new().expect("the current user's DACL");
    /// # let _ = dacl;
    /// ```
    pub fn new() -> io::Result<Self> {
        let user = current_user().map_err(io::Error::other)?;
        let mut descriptor = SECURITY_DESCRIPTOR::default();
        // The descriptor is built before the ACL is allocated, so no early return here owns
        // anything to free.
        //
        // SAFETY: the pointer is to `descriptor`, the local of the very type this call
        // initializes — 8-byte aligned as that type requires and at least the
        // `SECURITY_DESCRIPTOR_MIN_LENGTH` the call asks for, since it is the Win32 structure
        // itself. The call only writes into that storage, and nothing has published its
        // address yet: `attributes` is where it becomes a pointer a caller may use.
        if unsafe {
            InitializeSecurityDescriptor(
                ptr::from_mut(&mut descriptor).cast(),
                SECURITY_DESCRIPTOR_REVISION,
            )
        } == 0
        {
            // SAFETY: a thread-local read with no preconditions, taken as the first call after
            // the failure.
            return Err(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32
            ));
        }
        let acl = full_control_acl(&user)
            .map_err(|status| io::Error::from_raw_os_error(status as i32))?;
        // SAFETY: `descriptor` is the storage initialized above and `acl` is the list
        // `full_control_acl` just allocated — the pair this call's contract asks for: a
        // writable descriptor whose control word has `SE_SELF_RELATIVE` clear (the initializer
        // above wrote it) and a valid ACL. **The descriptor stores the ACL's pointer rather
        // than a copy of it**, which is why `acl` is a field of the value this function
        // returns and why `Drop` releases it: the ACL has to outlive every read of the
        // descriptor, and the value that owns both is what makes that true.
        if unsafe { SetSecurityDescriptorDacl(ptr::from_mut(&mut descriptor).cast(), 1, acl, 0) }
            == 0
        {
            // SAFETY: a thread-local read with no preconditions, taken as the first call after
            // the failure.
            let status = unsafe { GetLastError() };
            // SAFETY: the call above failed, so it stored nothing and this list is still this
            // function's alone — its one `LocalFree`, on the path that returns without a value
            // to own it.
            unsafe { LocalFree(acl as HLOCAL) };
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        descriptor.Control |= SE_DACL_PROTECTED;
        Ok(Self { descriptor, acl })
    }

    /// The value a create call takes; valid for as long as `self` lives **and stays in
    /// place** — the pointer is into this value's own field, so a move relocates what it names,
    /// and the create call that consumes it is the shape this contract has in mind.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_winsec::CurrentUserDacl;
    ///
    /// let dacl = CurrentUserDacl::new().expect("the current user's DACL");
    /// let attributes = dacl.attributes();
    /// assert!(!attributes.lpSecurityDescriptor.is_null());
    /// assert_eq!(
    ///     attributes.bInheritHandle, 0,
    ///     "a child process gets no handle"
    /// );
    /// ```
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
        // SAFETY: `self.acl` is this value's own `SetEntriesInAclW` allocation, whose
        // documented release is `LocalFree`, and `Drop` runs exactly once — the value is
        // neither `Copy` nor `Clone`. It is released here, after the descriptor that borrows
        // it has stopped being handed out: what would break it is a caller holding a copy of
        // `attributes()` past this value's life, which that method's contract forbids.
        unsafe { LocalFree(self.acl as HLOCAL) };
    }
}

/// The ACE walk behind [`acl_is_current_user_only`]: `true` only when the descriptor's DACL is
/// present, non-empty, and every ACE in it is an access-allowed ACE that grants the current user
/// `FILE_ALL_ACCESS` without inheriting it.
///
/// **The descriptor is the caller's, and this call neither owns nor frees it.** It is a raw
/// `PSECURITY_DESCRIPTOR` because the reader that needs it may hold one from an API this crate
/// does not call — `tinio-api`'s pipe read-back gets its own from `GetSecurityInfo` — and that
/// reach is what makes this an `unsafe fn`: no line of the body below can tell a live
/// descriptor from any other pointer, so the pointer's guarantees are the caller's to make and
/// the `# Safety` section is the whole of what it must promise. A caller that cannot make that
/// promise has the handle-based readers as its alternatives, not this entry point.
///
/// # Safety
///
/// **`descriptor` must name a live, aligned, readable security descriptor that the caller owns,
/// and it must stay that way for the whole call.** Concretely: it is non-null and aligned for
/// the Win32 `SECURITY_DESCRIPTOR` type — the allocation `GetNamedSecurityInfoW` or
/// `GetSecurityInfo` returned, or a `SECURITY_DESCRIPTOR` the caller built with
/// `InitializeSecurityDescriptor`, satisfies that, while an arbitrary address, a descriptor
/// whose owner has already freed it, or one that has moved since its address was taken does
/// not. Everything the walk reaches through the pointer must stay live for the same call: the
/// descriptor is handed to `GetSecurityDescriptorDacl`, and the DACL pointer that comes back
/// out of it is read for its ACE count and walked with `GetAce`, each ACE's header, access mask
/// and inline SID being read as the walk goes — so the DACL and its ACEs must still lie inside
/// the same live allocation when the walk returns. Both absolute and self-relative descriptors
/// are accepted, since `GetSecurityDescriptorDacl` resolves either, which is why the contract
/// names no format. Ownership does not move: this call writes nothing, allocates nothing and
/// frees nothing, and the caller releases the descriptor afterwards by whatever route produced
/// it (`LocalFree` for the two `*SecurityInfo` calls). A caller that cannot show all of that
/// about its pointer must not call this function.
///
/// # Examples
///
/// ```no_run
/// use std::{os::windows::ffi::OsStrExt, path::Path, ptr};
///
/// use tinio_winsec::descriptor_is_current_user_only;
/// use windows_sys::Win32::{
///     Foundation::{ERROR_SUCCESS, LocalFree},
///     Security::{
///         Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT},
///         DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
///     },
/// };
///
/// let path = Path::new(r"C:\state\state");
/// let name: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
/// let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
/// let status = unsafe {
///     GetNamedSecurityInfoW(
///         name.as_ptr(),
///         SE_FILE_OBJECT,
///         DACL_SECURITY_INFORMATION,
///         ptr::null_mut(),
///         ptr::null_mut(),
///         ptr::null_mut(),
///         ptr::null_mut(),
///         &mut descriptor,
///     )
/// };
/// assert_eq!(status, ERROR_SUCCESS);
/// // The descriptor belongs to whoever read it: this call reads it and frees nothing.
/// //
/// // SAFETY: `descriptor` is the allocation `GetNamedSecurityInfoW` just returned — non-null
/// // and aligned as the call's own output, and live, together with the DACL and ACEs the walk
/// // reaches through it, until the `LocalFree` below releases it.
/// let verdict =
///     unsafe { descriptor_is_current_user_only(path, descriptor) }.expect("the walk runs");
/// unsafe { LocalFree(descriptor) };
/// assert!(verdict);
/// ```
pub unsafe fn descriptor_is_current_user_only(
    path: &Path,
    descriptor: PSECURITY_DESCRIPTOR,
) -> io::Result<bool> {
    let user = current_user()?;
    inspect(path, descriptor, &user)
}

fn inspect(path: &Path, descriptor: PSECURITY_DESCRIPTOR, user: &CurrentUser) -> io::Result<bool> {
    let mut present: BOOL = 0;
    let mut dacl: *mut ACL = ptr::null_mut();
    let mut defaulted: BOOL = 0;
    // SAFETY: `descriptor` is the caller's live security descriptor — [`acl_is_current_user_only`]
    // passes the allocation its own `GetNamedSecurityInfoW` returned, and
    // [`descriptor_is_current_user_only`]'s `# Safety` section is the contract its caller made —
    // and the three out-parameters are locals of the types this call writes. The DACL pointer
    // comes back *into* that descriptor, so this call allocates nothing and frees nothing: the
    // descriptor's release stays with its owner, and the ACL inside it is never freed on its own.
    let ok =
        unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) };
    if ok == 0 {
        // SAFETY: a thread-local read with no preconditions, taken as the first call after the
        // failure.
        return Err(win32_error(path, "GetSecurityDescriptorDacl", unsafe {
            GetLastError()
        }));
    }
    if present == 0 || dacl.is_null() {
        // No DACL is every access to every trustee, so it is the opposite of restricted.
        return Ok(false);
    }
    let mut aces = 0u32;
    // **The walk is [`walk_dacl`]'s and the verdict is this function's.** The question is
    // whether the DACL is the current user's *alone*, so the quantifier is "every ACE", and
    // the first entry that is anything other than an access-allowed ACE naming the current
    // user with `FILE_ALL_ACCESS` and no `INHERIT_ONLY` — an entry for another trustee, a
    // deny, another type, an inherited-only grant that reaches nothing here — ends the walk
    // with that verdict. `visit` counts as it goes, because the walk's `Continue` also comes
    // back from a DACL that had no entries at all.
    //
    // SAFETY: `dacl` is the non-null ACL `GetSecurityDescriptorDacl` reported as present, so
    // it lies inside a descriptor whose owner keeps it alive for this whole walk. `user.Sid`
    // is the live SID of the `CurrentUser` this function was handed, which owns the buffer
    // that SID points into for as long as the borrow lasts.
    let verdict = unsafe {
        walk_dacl(path, dacl, user.Sid, &mut |ace| {
            aces += 1;
            match ace {
                Ace::Named(ace)
                    if u32::from(ace.Header.AceFlags) & INHERIT_ONLY_ACE == 0
                        && ace.Mask == FILE_ALL_ACCESS =>
                {
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Break(()),
            }
        })
    }?;
    // A DACL nobody was granted on is not a restricted DACL, and an empty one hands `visit`
    // nothing at all — so the count is what tells "every entry passed" from "there were none".
    Ok(aces > 0 && verdict.is_continue())
}

/// One ACE of a DACL, as [`walk_dacl`] hands it to a caller's verdict.
///
/// **The ACE types share their header and nothing else**, so this value names which case a
/// reader has instead of leaving it to a cast: an access-allowed entry is the one layout this
/// crate reads past the header — a mask, and then the trustee's SID inline — while the body of
/// any other type is a different structure.
///
/// **The two allowed cases are apart because their verdicts are.** A reader asking whether its
/// own SID is in the DACL folds them together; a reader asking whether the DACL is *nothing
/// but* its own grants — the question [`descriptor_is_current_user_only`] answers — has to see
/// another trustee's entry to refuse it, and `tinio-sandbox-windows`'s skip reads both for the
/// same reason: an inherited entry for a well-known SID is in most DACLs on this platform and
/// says nothing about the grant it is looking for.
///
/// # Examples
///
/// ```
/// use tinio_winsec::Ace;
///
/// // What a verdict can ask of one entry: whether it is the reader's own, another
/// // trustee's, or an ACE of some other type altogether.
/// let owns = |ace: &Ace<'_>| matches!(ace, Ace::Named(_));
///
/// assert!(!owns(&Ace::Other));
/// ```
pub enum Ace<'a> {
    /// An access-allowed ACE that names `sid`, the SID the walk was given.
    Named(&'a ACCESS_ALLOWED_ACE),
    /// An access-allowed ACE that names any other trustee: the same layout as
    /// [`Named`](Self::Named), so the entry's mask, flags and SID are readable too.
    OtherTrustee(&'a ACCESS_ALLOWED_ACE),
    /// An ACE of any other type — a deny, an audit, an object-typed entry, a mandatory
    /// label. The walk reports the fact and reads no field past the header, which is the
    /// only part every ACE layout has.
    Other,
}

/// Walks one live DACL's entries and hands each to `visit`, the caller's own verdict.
///
/// **The one ACE walk the workspace has.** Reading a Windows DACL is always the same
/// sequence — `GetAce` over the ACL's own count, the ACE-type test before any field is read,
/// `EqualSid` against the trustee the question is about — and it is asked by two crates here:
/// this module's verdict ([`acl_is_current_user_only`]) and `tinio-sandbox-windows`'s ACL
/// whitelist, which reads by name for its oracle and by handle for its skip. What differs
/// between them is the *quantifier*, not the walk — "every entry is the current user's
/// own full-control grant" against "some entry is the package SID's grant of this shape" —
/// so what lives here is the walk and the verdict stays the caller's: `visit` is called once
/// per ACE in the DACL's own order with [`Ace`] saying which of the three cases it is, and
/// answers [`ControlFlow::Break`] to end the walk there or [`ControlFlow::Continue`] for the
/// next entry, which is how a caller expresses both quantifiers without a second traversal.
///
/// **The return value is the walk's own end**: the break `visit` asked for, or
/// [`ControlFlow::Continue`] when the DACL ran out of ACEs. The two are different answers —
/// a walk that ran out was one a DACL with no entries also produces — so a caller whose "yes"
/// needs an entry to be there reads the count too.
///
/// **A `GetAce` that fails is an error and not a `Continue`, and that is a decision rather
/// than the shape the code happened to take.** The walk that lived in `tinio-sandbox-windows`
/// before the move into this module skipped such an entry silently — as it skipped a `GetAce`
/// that returned success and left a null pointer, a shape a successful call does not produce
/// and this walk therefore does not test for; the merge kept the `Err` because an entry
/// nothing could read is not an entry the caller's verdict may be asked about, and answering
/// "no such ACE" for it would let a caller act on a DACL it never saw.
/// It is also the rule the workspace already writes down elsewhere — a failed read is an error
/// and not an `Ok(false)`, in `tinio-sandbox-windows`'s `has_ace` as many words, and here in
/// [`acl_is_current_user_only`], whose failed `GetNamedSecurityInfoW` is an `Err` too — and it
/// is **fail-closed for both readers**: `tinio-sandbox-windows`'s `Object::granted` travels
/// the `Err` out as a failed launch *before* any grant is called satisfied, never as a skip,
/// and `inspect` below propagates it rather than answering `true` about a DACL that was only
/// partly walked.
///
/// **No test covers this path, and the record is worth more than the gap.** `GetAce` fails for
/// an index at or past the ACL's own `AceCount` — which this loop cannot ask for, since the
/// count it reads is the bound it walks — or for an ACL that is not valid, which nothing the
/// security APIs hand out can be: the kernel validates a DACL when it is set, and every
/// producer the two readers use (`GetNamedSecurityInfoW`, `GetSecurityInfo`, and the
/// `GetSecurityDescriptorDacl` that resolves the ACL inside the descriptor they return) gives
/// back the object's own. Reaching the failure therefore means calling this function on a
/// hand-built, malformed ACL, which the `# Safety` section below does not permit: the branch is
/// a contract-violation path rather than a reviewed one. Written down so that the next reader
/// does not take its absence from the coverage for an oversight, and so that a change which
/// makes it reachable has to bring a test with it.
///
/// `path` is what the failure is attributed to; the walk has no other use for a name.
///
/// # Safety
///
/// **`dacl` must name a live ACL inside a descriptor the caller owns, and `sid` must name a
/// live SID; both must stay live for the whole call.** Concretely: `dacl` is a pointer
/// `GetNamedSecurityInfoW`, `GetSecurityInfo` or `GetSecurityDescriptorDacl` wrote into its
/// out-parameter — an ACL that lies inside the descriptor the same call returned, which the
/// same owner keeps alive — and not one whose descriptor is already `LocalFree`d, not one
/// that has moved since its address was taken, and not an arbitrary address. Everything the
/// walk reaches through it must stay live for the same call: it reads the ACL's own
/// `AceCount`, then each ACE that count covers — the entry's header, and for an
/// access-allowed entry its access mask and the SID inline in it — so the ACEs must still lie
/// inside the same live allocation when the walk returns. `sid` is read by `EqualSid` for the
/// length its own header declares, so it must be a SID that is live for the call: a null,
/// freed or arbitrary pointer is the caller's undefined behaviour, never "no entry names it".
/// This call writes nothing, allocates nothing and frees nothing, and the ACL is never
/// released on its own (`LocalFree` takes the descriptor that holds it). A caller that cannot
/// show all of that about its pointers must not call this function; [`acl_is_current_user_only`]
/// takes the path instead.
///
/// # Examples
///
/// ```no_run
/// use std::{ops::ControlFlow, path::Path, ptr};
///
/// use tinio_winsec::{Ace, to_wide, walk_dacl};
/// use windows_sys::Win32::{
///     Foundation::{ERROR_SUCCESS, LocalFree},
///     Security::{
///         ACL,
///         Authorization::{ConvertStringSidToSidW, GetNamedSecurityInfoW, SE_FILE_OBJECT},
///         DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
///     },
/// };
///
/// let path = Path::new(r"C:\state\state");
/// let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
/// let mut dacl: *mut ACL = ptr::null_mut();
/// // The DACL pointer comes back *into* the descriptor, and only the descriptor is freed.
/// let status = unsafe {
///     GetNamedSecurityInfoW(
///         to_wide(path).as_ptr(),
///         SE_FILE_OBJECT,
///         DACL_SECURITY_INFORMATION,
///         ptr::null_mut(),
///         ptr::null_mut(),
///         &mut dacl,
///         ptr::null_mut(),
///         &mut descriptor,
///     )
/// };
/// assert_eq!(status, ERROR_SUCCESS);
///
/// // The trustee the question is about: the walk compares it with `EqualSid`.
/// let mut sid: PSID = ptr::null_mut();
/// assert_ne!(
///     unsafe { ConvertStringSidToSidW(to_wide("S-1-15-2-1").as_ptr(), &mut sid) },
///     0
/// );
///
/// // "Does some entry name that trustee?" — the quantifier is this call's, and the walk
/// // hands every ACE of the DACL over one at a time.
/// //
/// // SAFETY: `dacl` is the ACL inside the descriptor the call above returned, `descriptor`
/// // was not moved and is released once below, and `sid` is the SID the conversion above
/// // allocated — all live for the walk.
/// let verdict = unsafe {
///     walk_dacl(path, dacl, sid, &mut |ace| match ace {
///         Ace::Named(_) => ControlFlow::Break(()),
///         _ => ControlFlow::Continue(()),
///     })
/// }
/// .expect("the walk runs");
/// // The walk's two ends are `Break` — a call accepted an entry — and `Continue` — the DACL
/// // ran out, which is the end an empty one gives too. Which one this file produces is the
/// // caller's question; what the walk guarantees is that every entry was offered.
/// let _ = verdict;
/// // SAFETY: both values are this example's own allocations, each released once here.
/// unsafe {
///     LocalFree(sid);
///     LocalFree(descriptor);
/// }
/// ```
pub unsafe fn walk_dacl(
    path: &Path,
    dacl: *const ACL,
    sid: PSID,
    visit: &mut dyn FnMut(Ace<'_>) -> ControlFlow<()>,
) -> io::Result<ControlFlow<()>> {
    // SAFETY: `dacl` is the caller's live ACL — this call's contract — so its header is in
    // bounds and aligned, which is what reading the count it covers asks for.
    let count = unsafe { (*dacl).AceCount };
    for index in 0..u32::from(count) {
        let mut raw = ptr::null_mut();
        // SAFETY: `dacl` is alive and `index` is below the count just read, which is this
        // call's own bound; `raw` is a valid out-parameter.
        if unsafe { GetAce(dacl, index, &mut raw) } == 0 {
            // SAFETY: a thread-local read with no preconditions, as the first call after the
            // failure.
            return Err(win32_error(path, "GetAce", unsafe { GetLastError() }));
        }
        // The type first, through the raw pointer: a deny or audit ACE is not read as an
        // access-allowed one, and the caller's verdict is asked what its presence means
        // rather than being handed a layout it does not have.
        //
        // SAFETY: `GetAce` returned a pointer to this ACE inside the caller's live ACL, so its
        // header is readable and the cast is aligned. Only the header is read here; the
        // structure is read in full below, once this test has established which one it is —
        // the ACE layouts agree on the header and differ after it.
        let ace_type = unsafe { (*raw.cast::<ACCESS_ALLOWED_ACE>()).Header.AceType };
        let control = if ace_type != ACCESS_ALLOWED_ACE_TYPE {
            visit(Ace::Other)
        } else {
            // SAFETY: the type test above is what makes this structure's layout the right one
            // for this pointer — an `ACCESS_ALLOWED_ACE` inside the ACL the caller owns and
            // keeps alive for the whole walk.
            let ace = unsafe { &*raw.cast::<ACCESS_ALLOWED_ACE>() };
            // SAFETY: `ace` is an access-allowed ACE — the test two lines up is what
            // established it — so `SidStart` is the first byte of the SID that layout puts
            // inline, inside that ACE. `EqualSid` reads it for the length the SID's own header
            // declares, and `sid` is the caller's live SID.
            let names = unsafe { EqualSid(ptr::addr_of!(ace.SidStart) as PSID, sid) } != 0;
            visit(if names {
                Ace::Named(ace)
            } else {
                Ace::OtherTrustee(ace)
            })
        };
        if control.is_break() {
            return Ok(control);
        }
    }
    Ok(ControlFlow::Continue(()))
}

/// The pseudo-path the token calls report under: they act on the process, not on a file.
fn token_path() -> &'static Path {
    Path::new("<process token>")
}

/// The one sentence a failed Win32 call is reported with: the call's own name, the OS's message
/// for the status, and the decimal status beside it, so a log reader does not have to look the
/// number up.
///
/// **The rule is written here and nowhere else**: `tinio-api`'s pipe read-back reports its
/// failures through this same sentence — its own `win32_error` wraps it in that crate's
/// `Error::State` fields — and a second copy of the wording would be a second place for it to
/// drift one edit at a time. What stays with each caller is the **attribution**: which path the
/// failure belongs to, and which error type carries it. That is why the return value is the
/// sentence alone — a caller whose error type has a path field puts it there rather than splicing
/// it into this text, and a caller whose type has none ([`io::Error`] here) prefixes it itself.
///
/// # Examples
///
/// ```
/// use tinio_winsec::win32_error_message;
///
/// // The OS's own text for the status travels with the number, so whoever has the log line has
/// // the meaning of the code too.
/// let message = win32_error_message("GetAce", 5);
/// assert!(message.starts_with("GetAce failed: "), "{message}");
/// assert!(message.ends_with(" (status 5)"), "{message}");
///
/// // A call that set no last error must not be handed the success message.
/// assert_eq!(
///     win32_error_message("GetAce", 0),
///     "GetAce failed without a status"
/// );
/// ```
pub fn win32_error_message(call: &str, status: u32) -> String {
    // A `BOOL`-returning call need not set a last error, and status `0` would then render as the
    // success message; say what is known rather than repeat that text.
    if status == ERROR_SUCCESS {
        format!("{call} failed without a status")
    } else {
        let meaning = io::Error::from_raw_os_error(status as i32);
        format!("{call} failed: {meaning} (status {status})")
    }
}

/// The [`io::Error`] this crate's own Win32 calls report, in the wording above. `path` is what the
/// failure is attributed to — a file path, a pipe name, or [`token_path`] for the calls that act
/// on the process rather than on an object — and it is spliced into the sentence because this
/// error type has no field to carry it.
fn win32_error(path: &Path, call: &str, status: u32) -> io::Error {
    io::Error::other(format!(
        "{path}: {message}",
        path = path.display(),
        message = win32_error_message(call, status)
    ))
}

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
/// use tinio_winsec::to_wide;
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

#[cfg(test)]
mod tests {
    use std::{
        env, fs, io,
        ops::ControlFlow,
        path::{Path, PathBuf},
        process::{self, Command},
        ptr, thread,
    };

    use windows_sys::{
        Win32::{
            Foundation::{ERROR_SUCCESS, HLOCAL, LocalFree},
            Security::{
                ACE_OBJECT_TYPE_PRESENT, ACL,
                Authorization::{
                    DENY_ACCESS, EXPLICIT_ACCESS_W, GRANT_ACCESS, GetNamedSecurityInfoW,
                    OBJECTS_AND_SID, SE_FILE_OBJECT, SET_ACCESS, SET_AUDIT_SUCCESS,
                    SetEntriesInAclW, SetNamedSecurityInfoW, TRUSTEE_IS_OBJECTS_AND_SID,
                    TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
                },
                CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, GetSecurityDescriptorDacl,
                NO_INHERITANCE, OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION,
                PSECURITY_DESCRIPTOR, PSID,
            },
            Storage::FileSystem::FILE_ALL_ACCESS,
        },
        core::{BOOL, GUID},
    };

    use super::{
        Ace, CurrentUser, acl_is_current_user_only, create_restricted, current_user,
        restrict_to_current_user, to_wide, walk_dacl, win32_error,
    };

    /// A scratch directory that owns its teardown.
    ///
    /// **The residue knowledge lives here, once.** A test that installs an ACE can leave a tree
    /// the process's own delete cannot remove — a deny for the current user being the case that
    /// forced this guard: `state.tmp` carrying the current user's `FILE_ALL_ACCESS` as a `DENY`
    /// ahead of its allow needs `FILE_DELETE_CHILD` on the scratch directory instead, and a
    /// scratch directory that grants only `Modify` does not carry that right, because
    /// `FILE_GENERIC_WRITE` is `FILE_ADD_FILE` and `FILE_ADD_SUBDIRECTORY` and deliberately not
    /// delete-child. Measured 2026-10-02 on this host: the deny test with `TEMP` on the Dev Drive
    /// left `tinio-winsec-deny-<pid>\state.tmp` behind, and the same test with `TEMP` under
    /// `%LOCALAPPDATA%`, where the user's own SID holds full control, left nothing.
    /// `remove_dir_all` reports that failure and a `let _ =` swallows it, so the residue outlives
    /// every repeat — the next run's own cleanup cannot clear it either, because the deny is
    /// still on the file.
    ///
    /// Writing the DACL back to the current user's alone — [`restrict_to_current_user`], the
    /// shape every writer in this crate installs — is what puts a deny out of the delete's way;
    /// the owner's implicit `WRITE_DAC` is why the call gets through a descriptor that denies
    /// that same user everything. A test that installed one records its path with
    /// [`ScratchDir::restore_dacl`], and `Drop` puts those back first and removes the tree
    /// second. The teardown **reports a failure rather than swallowing it**: a dropped guard is
    /// the last chance to say the residue is there, and a `let _ =` is how it stayed invisible
    /// in the first place. On a thread that is already unwinding it prints instead of panicking,
    /// because a panicking `Drop` would abort the process the test's own red is trying to
    /// report.
    struct ScratchDir {
        /// The scratch directory.
        dir: PathBuf,
        /// The paths whose DACL a test installed, put back before the tree is removed.
        restore: Vec<PathBuf>,
    }

    impl ScratchDir {
        /// The process's own scratch directory for `tag`, with the residue of an earlier run
        /// cleared first — a run that was killed rather than unwound leaves the tree behind, and
        /// the tests assert on what a tree holds.
        fn new(tag: &str) -> Self {
            let dir = env::temp_dir().join(format!("tinio-winsec-{tag}-{}", process::id()));
            match fs::remove_dir_all(&dir) {
                Ok(()) => {}
                // Nothing there is the ordinary case. Anything else is a residue this run cannot
                // clear, and asserting on a tree that still holds the last run's files would be
                // asserting about the wrong fixture.
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => panic!(
                    "the residue of an earlier run at `{}` cannot be removed: {err}",
                    dir.display()
                ),
            }
            fs::create_dir_all(&dir).expect("scratch directory");
            Self {
                dir,
                restore: Vec::new(),
            }
        }

        /// The scratch directory.
        fn path(&self) -> &Path {
            &self.dir
        }

        /// Records `path` as carrying a DACL a test installed: `Drop` puts the current user's
        /// alone back before the tree goes, because the entry can be one that refuses the delete.
        fn restore_dacl(&mut self, path: PathBuf) {
            self.restore.push(path);
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let mut failures: Vec<String> = Vec::new();
            for path in &self.restore {
                if let Err(err) = restrict_to_current_user(path) {
                    failures.push(format!("{}: {err}", path.display()));
                }
            }
            if let Err(err) = fs::remove_dir_all(&self.dir) {
                failures.push(format!("{}: {err}", self.dir.display()));
            }
            if failures.is_empty() {
                return;
            }
            let report = format!(
                "the scratch teardown failed, so this run leaves the residue the next one has to \
                 live with: {}",
                failures.join("; ")
            );
            if thread::panicking() {
                eprintln!("{report}");
            } else {
                panic!("{report}");
            }
        }
    }

    /// **The create-time descriptor keeps an inheriting parent's ACEs out on its own**, which is
    /// the property a confined `state` write depends on.
    ///
    /// `CurrentUserDacl` marks the descriptor `SE_DACL_PROTECTED`, and that is what makes the
    /// create call sufficient. It is load-bearing rather than tidy: a file that inherited the
    /// exposure set's `(OI)(CI)M` (the launcher grants it to the container's package SID) could
    /// not be repaired from inside the confinement, because `SetNamedSecurityInfoW` needs
    /// `WRITE_DAC` and `M` deliberately excludes it — which is why `tinio-api`'s `State::write`
    /// **verifies** the descriptor instead of re-installing it, and why that verification is only
    /// sound while this test passes.
    ///
    /// The ACE is installed through `icacls` rather than through this crate, because it has to be
    /// one this crate would never install: an inheritable grant for a principal that is not the
    /// current user. `*S-1-5-32-545` is BUILTIN\Users by SID, so nothing here depends on the
    /// machine's display language.
    #[test]
    fn a_created_file_keeps_an_inheriting_parents_aces_out() {
        let scratch = ScratchDir::new("inherit");
        let dir = scratch.path();
        let granted = Command::new("icacls")
            .arg(dir)
            .args(["/grant", "*S-1-5-32-545:(OI)(CI)M", "/q"])
            .status()
            .expect("icacls runs");
        assert!(
            granted.success(),
            "the scratch directory takes the inheritable ACE"
        );

        let path = dir.join("state.tmp");
        drop(create_restricted(&path).expect("a restricted file"));
        // Read back **without** `restrict_to_current_user`: what is asserted is what the create
        // call installed, not what a later repair could impose.
        let verdict = acl_is_current_user_only(&path).expect("read the DACL back");

        assert!(
            verdict,
            "a file created with `create_restricted` must not inherit the parent's ACEs: one that \
             did would be left unrepairable inside the confinement"
        );
    }

    /// **A DACL that also names another trustee is not the current user's alone**, which is the
    /// branch the verdict exists for: the read-back is "every entry is mine", and a single entry
    /// for somebody else makes it `false` — never the fail-open a "some entry is mine" reading
    /// would answer.
    ///
    /// The ACE is installed through `icacls` for the same reason the test above uses it: it has
    /// to be one this crate would never write — a grant for a principal that is not the current
    /// user. `*S-1-5-32-545` is BUILTIN\Users by SID, so nothing here depends on the machine's
    /// display language.
    #[test]
    fn an_ace_for_another_trustee_is_not_the_current_users_dacl() {
        let scratch = ScratchDir::new("foreign");
        let dir = scratch.path();

        let path = dir.join("state.tmp");
        drop(create_restricted(&path).expect("a restricted file"));
        assert!(
            acl_is_current_user_only(&path).expect("read the DACL back"),
            "the create-time DACL is the current user's alone"
        );

        let granted = Command::new("icacls")
            .arg(&path)
            .args(["/grant", "*S-1-5-32-545:RX", "/q"])
            .status()
            .expect("icacls runs");
        assert!(granted.success(), "the scratch file takes the ACE");
        let verdict = acl_is_current_user_only(&path).expect("read the DACL back");

        assert!(
            !verdict,
            "an entry for another trustee makes the descriptor shared, and a `true` here is the \
             fail-open this verdict exists to refuse"
        );
    }

    /// **A deny entry naming the current user is not the current user's DACL**, and the
    /// judgement it pins is the one [`Ace::Other`] exists for: an `ACCESS_DENIED_ACE`
    /// (type 1) has the same header, mask and trustee layout an access-allowed entry
    /// has, and the one this test installs is `FILE_ALL_ACCESS` (measured on this host
    /// 2026-09-28: raw type 1, flags 0, mask `0x1F_01FF`) — the value [`inspect`]
    /// compares for equality — so a read that did not ask the entry's type would answer
    /// `true` about a DACL that forbids the very user it claims is the only one allowed.
    ///
    /// **Both entries name this process's own SID on purpose.** A deny for another
    /// trustee would leave the verdict `false` even from a read that ignored the type, so
    /// it would discriminate nothing; and the allow is there so that the `false` cannot
    /// come from a DACL that merely lost its grant — the entry the verdict wants is
    /// present, and the deny beside it is what has to be refused.
    ///
    /// **The DACL is built through `SetEntriesInAclW`, and the tool cannot build this
    /// one** (measured on this host 2026-09-28): `icacls /deny` for a trustee that
    /// already holds an explicit allow *replaces* that entry — the `create_restricted`
    /// file whose DACL was `[allow]` came back from `/deny *<sid>:(F)` as `[deny]`, and
    /// the `/remove:d` that followed left it empty — so the pair this test is about, an
    /// allow the verdict wants beside a deny it must refuse, is not a state the tool
    /// produces. `SetEntriesInAclW` takes the same two entries — `SET_ACCESS` and
    /// `DENY_ACCESS`, the merge [`restrict_to_current_user`] uses — and the kernel keeps
    /// both.
    ///
    /// The classification is what shows the branch was reached: the walk has to hand the
    /// deny over as [`Ace::Other`] and the allow as [`Ace::Named`], in the ACL's own
    /// order (deny entries come first).
    #[test]
    fn a_deny_entry_for_the_current_user_is_not_the_current_users_dacl() {
        let mut scratch = ScratchDir::new("deny");
        let dir = scratch.path();
        let user = current_user().expect("the process token");
        let denied = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            // The deny flavour of the same list: the entry it builds is an
            // `ACCESS_DENIED_ACE` where `SET_ACCESS` builds an `ACCESS_ALLOWED_ACE`.
            grfAccessMode: DENY_ACCESS,
            grfInheritance: NO_INHERITANCE,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: user.Sid.cast::<u16>(),
                ..Default::default()
            },
        };
        let seen = dacl_with_an_extra_ace(denied, dir, &user);
        let state = dir.join("state.tmp");
        let verdict = acl_is_current_user_only(&state).expect("read the DACL back");
        // **The deny this test installs is what needs the guard's restore**: it refuses the
        // current user the delete, so the tree cannot go until the DACL is back to that user's
        // alone — [`ScratchDir`]'s `Drop` is where that happens, and its doc carries the
        // measurement and the delete-child rule that make it necessary.
        scratch.restore_dacl(state);

        assert_eq!(
            seen,
            ["other", "named"],
            "the DACL carries the current user's allow and a deny entry beside it, and the walk \
             has to hand the deny over as an ACE of another type rather than read it as the \
             access-allowed entry it is shaped like"
        );
        assert!(
            !verdict,
            "a deny entry naming the current user carries the mask this verdict requires, so a \
             read that did not ask the entry's type would call this DACL restricted — the \
             fail-open this verdict exists to refuse"
        );
    }

    /// **An audit entry is not the current user's grant**, and it is one of the types
    /// [`Ace::Other`] is named for: a `SYSTEM_AUDIT_ACE` (type 2) carries the same mask
    /// and the same trustee an access-allowed entry does, so a walk that skipped the
    /// type test would hand the verdict a `FILE_ALL_ACCESS` entry naming the current
    /// user — the very entry it is looking for.
    ///
    /// **Windows never puts an audit ACE in a DACL, and the API still accepts one**,
    /// which is why this is a test and not a paragraph: `SetEntriesInAclW`'s audit
    /// access mode builds the entry and `SetNamedSecurityInfoW` installs it in the DACL
    /// slot, and the descriptor read back carries it as a type-2 entry, measured on
    /// this host 2026-09-28. The tool cannot write one there at all — `icacls /audit`
    /// is a SACL write — and a SACL never reaches this walk, since every caller hands
    /// it the DACL `GetSecurityDescriptorDacl` resolved ([`inspect`]'s does, and so
    /// does `tinio-sandbox-windows`'s reader), so this is the only route to the branch
    /// with an audit-typed entry on it.
    #[test]
    fn an_audit_entry_is_not_the_current_users_dacl() {
        let scratch = ScratchDir::new("audit");
        let dir = scratch.path();
        let user = current_user().expect("the process token");
        let audited = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            // The audit flavour of the same list: the entry it builds is a
            // `SYSTEM_AUDIT_ACE` where `SET_ACCESS` builds an `ACCESS_ALLOWED_ACE`.
            grfAccessMode: SET_AUDIT_SUCCESS,
            grfInheritance: NO_INHERITANCE,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: user.Sid.cast::<u16>(),
                ..Default::default()
            },
        };
        let seen = dacl_with_an_extra_ace(audited, dir, &user);
        let verdict = acl_is_current_user_only(&dir.join("state.tmp")).expect("read the DACL back");

        assert_eq!(
            seen,
            ["other", "named"],
            "the DACL carries the current user's allow and an audit entry, and the walk has to \
             hand the second over as an ACE of another type rather than read it as an \
             access-allowed one"
        );
        assert!(
            !verdict,
            "an audit entry is not the current user's grant, so a DACL carrying one is not the \
             current user's alone"
        );
    }

    /// **An object-typed ACE is not the current user's grant, and its layout is the
    /// reason the type test has to come first**: an `ACCESS_ALLOWED_OBJECT_ACE` (type 5)
    /// carries the object-type GUIDs between its mask and its SID, so the `SidStart` an
    /// access-allowed entry's trustee is read at is a GUID's first bytes here — a read
    /// that skipped the type test would compare a SID that is not one, for a length
    /// those bytes happen to claim.
    ///
    /// **The entry is built through `SetEntriesInAclW`'s object form**
    /// (`TRUSTEE_IS_OBJECTS_AND_SID` with `ACE_OBJECT_TYPE_PRESENT` in the inheritance
    /// word, which is the pair that form requires), and Windows stores it in the file's
    /// DACL — measured on this host 2026-09-28, as the audit case above is. The GUID is
    /// this test's own: nothing resolves an object type on a file, and what the entry
    /// has to be is an object-typed ACE naming the current user.
    #[test]
    fn an_object_typed_ace_is_not_the_current_users_dacl() {
        let scratch = ScratchDir::new("object");
        let dir = scratch.path();
        let user = current_user().expect("the process token");
        let mut objects = OBJECTS_AND_SID {
            ObjectsPresent: ACE_OBJECT_TYPE_PRESENT,
            ObjectTypeGuid: GUID::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff),
            pSid: user.Sid.cast(),
            ..Default::default()
        };
        let extra = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            grfAccessMode: GRANT_ACCESS,
            grfInheritance: OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE | ACE_OBJECT_TYPE_PRESENT,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_OBJECTS_AND_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: ptr::from_mut(&mut objects).cast::<u16>(),
                ..Default::default()
            },
        };
        let seen = dacl_with_an_extra_ace(extra, dir, &user);
        let verdict = acl_is_current_user_only(&dir.join("state.tmp")).expect("read the DACL back");

        assert_eq!(
            seen,
            ["named", "other"],
            "the DACL carries the current user's allow and an object-typed entry, and the walk \
             has to hand the second over as an ACE of another type: reading its `SidStart` \
             would be reading a GUID"
        );
        assert!(
            !verdict,
            "an object-typed entry is not the current user's grant, so a DACL carrying one is \
             not the current user's alone"
        );
    }

    /// **The path is this crate's own part of a failure and the sentence is not**, which is the
    /// split the shared wording exists for: `tinio-api`'s pipe read-back reports the same sentence
    /// under its `Error::State` path field, so the two crates agree on the text and differ only in
    /// how the attribution is carried. What this asserts is the half that stays here — the
    /// prefixed path — and the sentence's two shapes, including the no-status one a caller cannot
    /// produce on demand: a `BOOL`-returning call that set no last error is not reported as one
    /// that succeeded.
    #[test]
    fn a_failed_call_reports_the_path_beside_the_shared_wording() {
        let path = Path::new(r"C:\state\state");
        let text = win32_error(path, "GetAce", 5).to_string();
        assert!(
            text.starts_with(r"C:\state\state: "),
            "the path the failure is attributed to leads the report: {text}"
        );
        assert!(
            text.contains("GetAce failed: ") && text.ends_with(" (status 5)"),
            "the sentence names the call and closes with the decimal status, so a log reader can \
             act on the code: {text}"
        );

        let no_status = win32_error(path, "GetAce", 0).to_string();
        assert!(
            no_status.ends_with("GetAce failed without a status"),
            "a call that set no last error must not borrow the success message: {no_status}"
        );
    }

    /// **One file carrying the current user's full-control allow and one ACE of another
    /// type on top**, built the way this crate's own writers build a DACL —
    /// `SetEntriesInAclW` merges an `EXPLICIT_ACCESS_W` list, `SetNamedSecurityInfoW`
    /// installs it with the DACL protected, so the file carries exactly the entries this
    /// test named and none the scratch directory's own ACL would otherwise have
    /// contributed as inherited entries.
    ///
    /// The `extra` entry is the caller's, because that is the point: a deny, an audit ACE
    /// and an object-typed ACE are three different lists to merge and one question for the
    /// walk. What comes back is [`classification`]'s reading of the DACL that landed, so
    /// the tests assert on what the object carries rather than on what they asked for.
    fn dacl_with_an_extra_ace(
        extra: EXPLICIT_ACCESS_W,
        dir: &Path,
        user: &CurrentUser,
    ) -> Vec<&'static str> {
        let path = dir.join("state.tmp");
        let _ = fs::remove_file(&path);
        fs::write(&path, b"the token a `state` file holds").expect("the layer");
        let allow = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            grfAccessMode: SET_ACCESS,
            grfInheritance: NO_INHERITANCE,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: user.Sid.cast::<u16>(),
                ..Default::default()
            },
        };
        let entries = [allow, extra];
        let mut acl: *mut ACL = ptr::null_mut();
        // SAFETY: `entries` is the two initialized `EXPLICIT_ACCESS_W` values above and is
        // alive for the call; each trustee pointer they carry is a live value the caller
        // keeps alive — `user.Sid` for the first, and whatever `extra` was built over —
        // and the form/type pair beside each says which kind that pointer is, which is
        // what makes the API read it as one. The null old ACL is the documented "no
        // existing list to merge with", and `acl` is a valid out-parameter. On success the
        // value is this function's own allocation: `SetEntriesInAclW` allocates it and
        // documents `LocalFree` as its release.
        let status = unsafe { SetEntriesInAclW(2, entries.as_ptr(), ptr::null_mut(), &mut acl) };
        assert_eq!(status, ERROR_SUCCESS, "SetEntriesInAclW");
        let wide = to_wide(&path);
        // SAFETY: `wide` is the NUL-terminated path built above and alive for the call;
        // `acl` is the list the call above returned and is released below, after this call
        // has copied it into the object's own descriptor, which is what lets the free
        // follow it. `PROTECTED_DACL_SECURITY_INFORMATION` is what keeps the directory's
        // inheritable ACEs out, so the entries the walk reads are the ones named here. The
        // owner, group and SACL arguments are null, the documented "not asked for".
        let status = unsafe {
            SetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                acl,
                ptr::null_mut(),
            )
        };
        // SAFETY: `acl` is this function's own `SetEntriesInAclW` allocation and this is
        // its one free, after the status has been read.
        unsafe { LocalFree(acl as HLOCAL) };
        assert_eq!(status, ERROR_SUCCESS, "SetNamedSecurityInfoW");
        classification(&path, user.Sid)
    }

    /// How [`walk_dacl`] classified every entry of `path`'s DACL, in the DACL's own
    /// order — the three cases [`Ace`] names, as text a failed assertion can print.
    ///
    /// **This is the branch's own measurement**: the verdicts these tests assert answer
    /// `false` for reasons a broken walk could also produce, and what tells the two apart
    /// is that the entry arrived as [`Ace::Other`] rather than as the allow it resembles.
    fn classification(path: &Path, sid: PSID) -> Vec<&'static str> {
        let wide = to_wide(path);
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: `wide` is the NUL-terminated path built above and alive for the call;
        // `descriptor` is a valid out-parameter for the allocation the call returns, which
        // is released once below. The four middle arguments are null, the documented "not
        // asked for", which is what keeps the descriptor's own allocation the only one this
        // function has to free.
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
        assert_eq!(
            status,
            ERROR_SUCCESS,
            "GetNamedSecurityInfoW on {}",
            path.display()
        );
        let mut present: BOOL = 0;
        let mut dacl: *mut ACL = ptr::null_mut();
        let mut defaulted: BOOL = 0;
        // SAFETY: `descriptor` is the live allocation the call above returned; the three
        // out-parameters are locals of the types this call writes. The DACL comes back
        // *into* that descriptor, so nothing here is freed on its own.
        let read = unsafe {
            GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted)
        };
        assert_ne!(read, 0, "the DACL of {}", path.display());
        assert_ne!(present, 0, "the DACL of {} is present", path.display());
        let mut seen = Vec::new();
        // SAFETY: `dacl` is the non-null ACL the call above resolved inside the descriptor
        // this function owns and frees below, so it and its ACEs stay live for the walk;
        // `sid` is the caller's live SID, which the `CurrentUser` it came from owns for the
        // whole call.
        let walked = unsafe {
            walk_dacl(path, dacl, sid, &mut |ace| {
                seen.push(match ace {
                    Ace::Named(_) => "named",
                    Ace::OtherTrustee(_) => "other-trustee",
                    Ace::Other => "other",
                });
                ControlFlow::Continue(())
            })
        };
        // SAFETY: `descriptor` is this function's own allocation — `GetNamedSecurityInfoW`
        // documents `LocalFree` as its release — and this is its one free, after the walk
        // that borrows the ACL inside it has returned.
        unsafe { LocalFree(descriptor) };
        assert!(walked.is_ok(), "the walk of {} runs", path.display());
        seen
    }
}
