//! Launching the confined child: the attribute list, the handle list and
//! `CreateProcessW` (design §3 step 6).
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14). Its invariants are the ones the evidence table measured:
//! `SID_AND_ATTRIBUTES` is 16 bytes on x64, and the stride is what that row
//! fixed — a stride of 8 makes `CreateProcessW` return `ERROR_NOACCESS` (998)
//! and produce **no child**. The `ERROR_INVALID_PARAMETER` (87) an earlier note
//! cited here belongs to a different failure with a worse outcome: a truncated
//! `cbSize` passed to `UpdateProcThreadAttribute`, which sets no attribute at
//! all and still returns a child that runs **unconfined**. Beyond the layout,
//! `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` requires `bInheritHandles`, and every
//! value named by `UpdateProcThreadAttribute` outlives the attribute list,
//! because the kernel reads it again at `CreateProcessW` time rather than
//! copying it.

#![allow(unsafe_code)]

use std::{
    ffi::{OsStr, OsString, c_void},
    mem::size_of,
    os::windows::{ffi::OsStrExt, process::ExitStatusExt},
    path::Path,
    process::ExitStatus,
    ptr,
};

// **The core's builder is imported under an alias here, and it has to be.**
// `use tinio_sandbox_core::launch;` beside this module's own `pub fn launch`
// below is `E0255` — one name cannot bind two functions — and `launch` is what
// both the core's builder and this module's `CreateProcessW` entry are called.
// `launch_error` says which one it is: the builder, at every call site here.
use windows_sys::Win32::Foundation::{HANDLE, WAIT_OBJECT_0};
use windows_sys::Win32::{
    Security::{PSID, SECURITY_CAPABILITIES},
    System::Threading::{
        CREATE_NEW_PROCESS_GROUP, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
        DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess, INFINITE,
        InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
        PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES, PROCESS_INFORMATION, STARTF_USESTDHANDLES,
        STARTUPINFOEXW, STARTUPINFOW, TerminateProcess, UpdateProcThreadAttribute,
        WaitForSingleObject,
    },
};

use crate::{
    _core::{Result, launch as launch_error},
    handles::{self, last_error},
    sid::CapabilitySids,
};

/// The two code units the `CommandLineToArgvW` convention gives meaning to,
/// spelled once so the quoting predicate and the escaper cannot disagree about
/// which unit a quote is. Both are ASCII, and so exactly one unit each.
const BACKSLASH: u16 = b'\\' as u16;
const QUOTE: u16 = b'"' as u16;

/// The AppContainer a child is created in.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_windows::{
///     process::Container,
///     sid::{CapabilitySids, PackageSid, package_name},
/// };
/// # fn main() -> Result<(), tinio_sandbox_core::Error> {
/// let sid = PackageSid::derive(&package_name(Path::new(r"E:\srv\tinio")))?;
/// let container = Container {
///     package_sid: sid.raw(),
///     // Empty, which is the default and the configuration the outbound
///     // guarantee rests on (design §7).
///     capabilities: CapabilitySids::resolve(&[])?,
/// };
/// assert!(container.capabilities.is_empty());
/// # Ok(())
/// # }
/// ```
pub struct Container {
    /// The package SID, kept alive by the caller until the launch returns.
    pub package_sid: PSID,
    /// The capability SIDs. Empty is the default, and the configuration the
    /// outbound guarantee rests on (design §7).
    pub capabilities: CapabilitySids,
}

/// What the launcher hands to `CreateProcessW`.
///
/// # Examples
///
/// ```no_run
/// use std::{path::Path, ptr};
///
/// use tinio_sandbox_windows::process::LaunchSpec;
///
/// let spec = LaunchSpec {
///     program: Path::new(r"C:\srv\tinio\serve.exe"),
///     args: &[],
///     stdin: ptr::null_mut(),
///     stdout: ptr::null_mut(),
///     stderr: ptr::null_mut(),
///     inherited: &[],
/// };
/// assert!(spec.args.is_empty());
/// ```
pub struct LaunchSpec<'a> {
    /// The executable, as an absolute path. Also `argv[0]`.
    pub program: &'a Path,
    /// The child's arguments, including the inherited listener's element.
    pub args: &'a [OsString],
    /// The child's standard input: the read end of the launcher's payload pipe
    /// (decision 17).
    pub stdin: HANDLE,
    /// The child's standard output: the write end of the launcher's event pipe
    /// (decision 19).
    pub stdout: HANDLE,
    /// The child's standard error: the launcher's own, inherited untouched.
    pub stderr: HANDLE,
    /// The extra handles the child inherits, on top of its standard handles —
    /// the inherited listener.
    pub inherited: &'a [HANDLE],
}

/// The child process, alive until [`LaunchedProcess::wait`] or `Drop`.
///
/// # Examples
///
/// [`launch`] is the only thing that returns one, and the whole of what a
/// caller does with it is wait for it, hand its handle to the job, and
/// terminate it on the two failure paths where nothing else can:
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_windows::{
///     job::Job,
///     process::{Container, LaunchSpec, launch},
///     sid::{CapabilitySids, PackageSid, package_name},
/// };
/// # fn main() -> Result<(), tinio_sandbox_core::Error> {
/// # let root = Path::new(r"E:\srv\tinio");
/// # let sid = PackageSid::derive(&package_name(root))?;
/// let mut child = launch(
///     &LaunchSpec {
///         program: Path::new(r"C:\srv\tinio\serve.exe"),
///         args: &[],
///         stdin: std::ptr::null_mut(),
///         stdout: std::ptr::null_mut(),
///         stderr: std::ptr::null_mut(),
///         inherited: &[],
///     },
///     &Container {
///         package_sid: sid.raw(),
///         capabilities: CapabilitySids::resolve(&[])?,
///     },
/// )?;
/// let job = Job::create()?;
/// job.assign(child.handle())?;
/// let status = child.wait()?;
/// # let _ = status;
/// # Ok(())
/// # }
/// ```
// `Debug` is derived because the launch's own failure path is asserted with
// `Result::expect_err`, which requires it of the success type, and because a
// value an operator meets on a failure should be printable.
#[derive(Debug)]
pub struct LaunchedProcess {
    process: HANDLE,
    pid: u32,
}

impl LaunchedProcess {
    /// The process handle, for [`crate::job::Job::assign`].
    ///
    /// # Examples
    ///
    /// The handle is what the job owns, and the job is what makes the launcher's
    /// death take the child with it (decision 20):
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::{job::Job, process::LaunchedProcess};
    ///
    /// # fn assign(child: &LaunchedProcess) -> Result<(), tinio_sandbox_core::Error> {
    /// let job = Job::create()?;
    /// job.assign(child.handle())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn handle(&self) -> HANDLE {
        self.process
    }

    /// The child's process id.
    ///
    /// Also its **process-group id**, because the child is created with
    /// `CREATE_NEW_PROCESS_GROUP`: that is the value the relay's
    /// `GenerateConsoleCtrlEvent` addresses (design §3 step 9).
    ///
    /// # Examples
    ///
    /// The pid a launch reports is the pid this returns, and the one the
    /// launch's own `info!` event carries:
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::process::LaunchedProcess;
    ///
    /// # fn relay(child: &LaunchedProcess) -> u32 {
    /// // The stop relay's target: the child is its own process group.
    /// child.pid()
    /// # }
    /// ```
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Wait for the child and return its exit status.
    ///
    /// The status is the core's currency: `supervise` forwards it and the call
    /// site exits with it (design §3 step 8). It is not the same obligation as
    /// the relay of §3 step 9 — that one runs *while* this waits.
    ///
    /// **`INFINITE`, so what bounds this call is the child's lifetime rather than
    /// any deadline of the launcher's.** A child that neither exits nor reports
    /// leaves the caller here for as long as that child lives: there is no status
    /// to forward, the call site's "exited without reporting" `Error` event is
    /// read only *after* a wait that has not returned, and the relay thread has
    /// nothing to re-emit while the child's stdout stays open — so the operator
    /// sees no failure, no readiness frame and no diagnostic. That window is
    /// recorded at the call site in Task 9 rather than closed here: a readiness
    /// deadline belongs to the readiness contract, not to this wait, and the
    /// design's §3 step 8 fixes the shape this function implements.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the wait or the
    /// exit-code query fails.
    ///
    /// # Examples
    ///
    /// The status is what `supervise` forwards and what the call site exits with:
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::process::LaunchedProcess;
    ///
    /// # fn exit_code(child: &mut LaunchedProcess) -> Result<Option<i32>, tinio_sandbox_core::Error> {
    /// let status = child.wait()?;
    /// Ok(status.code())
    /// # }
    /// ```
    pub fn wait(&mut self) -> Result<ExitStatus> {
        // SAFETY: `self.process` is a live process handle owned by this value for
        // the duration of the call.
        let waited = unsafe { WaitForSingleObject(self.process, INFINITE) };
        if waited != WAIT_OBJECT_0 {
            return Err(launch_error(
                "WaitForSingleObject",
                self.pid.to_string(),
                last_error(),
            ));
        }
        let mut code: u32 = 0;
        // SAFETY: as above, and `code` is a valid out-parameter.
        let ok = unsafe { GetExitCodeProcess(self.process, &mut code) };
        if ok == 0 {
            return Err(launch_error(
                "GetExitCodeProcess",
                self.pid.to_string(),
                last_error(),
            ));
        }
        // The other end of the launch's `info!` event: which child it was and how
        // it left, so a log read after the fact holds both halves of the pair
        // (shared §7).
        tracing::info!(step = "child exit", pid = self.pid, code = code);
        Ok(ExitStatus::from_raw(code))
    }

    /// Terminate the child now, without waiting for it.
    ///
    /// **Its one caller is Task 9's two job failures**, and it exists because
    /// neither of them may leave the child running: a child that is not in the
    /// job is one the launcher cannot guarantee to take with it, and the job is
    /// exactly what the two failed calls were building.
    /// [`crate::job::Job::assign`]'s own doc says the caller must terminate the
    /// process on a refused assignment; this is that call, made on a failure to
    /// create the job as well.
    ///
    /// `Drop` does not do this and must not. A `LaunchedProcess` dropped without
    /// a wait is either the failure path above — which has already terminated it
    /// — or a caller walking away from a child the job owns, where
    /// kill-on-close is the mechanism (decision 20, first half) and terminating
    /// here would be a second one.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the call is
    /// refused. The caller returns the failure that *led* here rather than this
    /// one — a launch that stopped must report why — and this error is not lost
    /// with it: [`launch_error`](tinio_sandbox_core::launch) logged it where it
    /// was built, which is the rule shared §7 states for every failure in this
    /// crate.
    ///
    /// # Examples
    ///
    /// Task 9's two job failures are where this is called, and neither of them
    /// returns this error — they return the failure that led to it:
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::process::LaunchedProcess;
    ///
    /// # fn on_failed_assignment(child: &LaunchedProcess, why: tinio_sandbox_core::Error)
    /// # -> Result<(), tinio_sandbox_core::Error> {
    /// // A child the job does not own must not outlive the failure.
    /// child.terminate(1)?;
    /// Err(why)
    /// # }
    /// ```
    pub fn terminate(&self, code: u32) -> Result<()> {
        // SAFETY: `self.process` is a live process handle owned by this value,
        // and TerminateProcess takes an exit code and dereferences nothing.
        let ok = unsafe { TerminateProcess(self.process, code) };
        if ok == 0 {
            return Err(launch_error(
                "TerminateProcess",
                self.pid.to_string(),
                last_error(),
            ));
        }
        // A documented degradation rather than a step that went well: the launch
        // failed and a child had to be destroyed to keep the failure from leaving
        // a process behind (shared §7).
        tracing::warn!(step = "TerminateProcess", pid = self.pid, code = code);
        Ok(())
    }
}

impl Drop for LaunchedProcess {
    fn drop(&mut self) {
        // Not terminating here is deliberate — see `terminate`.
        handles::close(self.process);
    }
}

/// Build a command line from a program and its arguments.
///
/// The quoting is the `CommandLineToArgvW` convention — the one the child's own
/// runtime parses — and not a shell's: a backslash run before a quote is
/// doubled, and only an argument that needs quoting is quoted.
///
/// **The buffer is the arguments' own code units, and a lossy conversion here is
/// not a conversion this crate may make**: an argument is arbitrary UTF-16, and
/// `to_string_lossy` would replace every unit that is not valid UTF-8 with U+FFFD
/// before the child's parser ever sees it, so the argv the child reads is not the
/// argv the operator passed — this launcher's own arguments filtered, with
/// `tinio-sandbox-fd=<n>` among them. The NUL is appended here rather than by
/// [`crate::wide`], because what this returns is units and not text.
///
/// The program path is the same rule through a second route, and it is not the
/// program's argv element that decides it: [`launch`] hands the path to
/// `CreateProcessW` as `lpApplicationName` through [`crate::wide_os`], so the image
/// the kernel opens is the one the caller named whatever this line says.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::process::command_line;
///
/// let line = command_line(Path::new(r"C:\s.exe"), &[]);
/// assert_eq!(line.last(), Some(&0));
/// ```
pub fn command_line(program: &Path, args: &[OsString]) -> Vec<u16> {
    let mut line = Vec::new();
    append_arg(&mut line, program.as_os_str());
    for arg in args {
        line.push(u16::from(b' '));
        append_arg(&mut line, arg);
    }
    line.push(0);
    line
}

/// Append one argument, quoted the way `CommandLineToArgvW` unquotes it.
///
/// **The predicate errs toward quoting, because quoting an argument that did
/// not need it costs nothing and failing to quote one that did splits it
/// silently.** It covers the separators a command line is split on and the
/// control characters a parser might treat as whitespace — a Windows *file*
/// name cannot carry one of those, so an exposed path never has to, but an
/// *argument* can, and the child's argv is the launcher's own argv filtered.
/// **Measured 2026-09-21 on this host: `CommandLineToArgvW` does not treat a
/// newline, a carriage return, a form feed or a vertical tab as a separator,
/// even unquoted** — so that half of the predicate is hardening rather than the
/// repair of a demonstrated corruption, and what the round trip below proves is
/// the property it rests on: quoting them changes nothing.
///
/// **It walks code units rather than `char`s, and that is what makes it
/// lossless**: every separator the predicate names is a BMP scalar and so exactly
/// one unit, an unpaired surrogate is neither one nor a separator, and it is
/// copied through rather than being replaced by U+FFFD. The escape this writes is
/// a unit too — [`BACKSLASH`] and [`QUOTE`] — which is the same distinction
/// [`command_line`]'s doc draws for the whole buffer.
fn append_arg(line: &mut Vec<u16>, arg: &OsStr) {
    let units: Vec<u16> = arg.encode_wide().collect();
    let needs_quotes = units.is_empty()
        || units
            .iter()
            .any(|&unit| matches!(unit, 0x20 | 0x09 | 0x0a | 0x0b | 0x0c | 0x0d | QUOTE));
    if !needs_quotes {
        line.extend_from_slice(&units);
        return;
    }
    line.push(QUOTE);
    let mut backslashes = 0usize;
    for unit in units {
        match unit {
            BACKSLASH => {
                backslashes += 1;
                line.push(BACKSLASH);
            }
            QUOTE => {
                // Double the pending backslashes, then escape the quote.
                for _ in 0..backslashes {
                    line.push(BACKSLASH);
                }
                backslashes = 0;
                line.push(BACKSLASH);
                line.push(QUOTE);
            }
            other => {
                backslashes = 0;
                line.push(other);
            }
        }
    }
    // A trailing backslash run would escape the closing quote.
    for _ in 0..backslashes {
        line.push(BACKSLASH);
    }
    line.push(QUOTE);
}

/// Launch `program` inside `container`.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) naming the program and
/// carrying the Win32 error, for a refused attribute list, attribute, or
/// `CreateProcessW`. The caller must roll the ACLs back on every one of them
/// (design §11).
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_windows::{
///     process::{Container, LaunchSpec, launch},
///     sid::{CapabilitySids, PackageSid, package_name},
/// };
/// # fn main() -> Result<(), tinio_sandbox_core::Error> {
/// let root = Path::new(r"E:\srv\tinio");
/// let sid = PackageSid::derive(&package_name(root))?;
/// let mut child = launch(
///     &LaunchSpec {
///         program: Path::new(r"C:\srv\tinio\serve.exe"),
///         args: &[],
///         stdin: std::ptr::null_mut(),
///         stdout: std::ptr::null_mut(),
///         stderr: std::ptr::null_mut(),
///         inherited: &[],
///     },
///     &Container {
///         package_sid: sid.raw(),
///         capabilities: CapabilitySids::resolve(&[])?,
///     },
/// )?;
/// let status = child.wait()?;
/// # Ok(())
/// # }
/// ```
pub fn launch(spec: &LaunchSpec<'_>, container: &Container) -> Result<LaunchedProcess> {
    let capabilities = SECURITY_CAPABILITIES {
        AppContainerSid: container.package_sid,
        // **Null where there is no capability, which is not the same thing as
        // the empty slice's own pointer.** `Vec::new().as_ptr()` is a dangling
        // *non-null* pointer, and measured on this host (2026-09-21): passing it
        // beside `CapabilityCount: 0` makes `CreateProcessW` fail `err=87`
        // (`ERROR_INVALID_PARAMETER`) — and the empty set is the **default**
        // configuration, the one design §7's outbound guarantee rests on. The
        // kernel reads this pointer even when the count is zero, so a container
        // with no capability is the one configuration that could not launch at
        // all. `CapabilitySids` is unchanged: what it hands back is its array,
        // and turning "no entries" into null is this structure's requirement
        // rather than that module's contract.
        //
        // `cast_mut` and not a signature change on `CapabilitySids::as_ptr`:
        // windows-sys declares the field `*mut` where the crate's own
        // declaration said `*const`, and nothing on either side of the boundary
        // writes through it.
        Capabilities: if container.capabilities.is_empty() {
            ptr::null_mut()
        } else {
            container.capabilities.as_ptr().cast_mut()
        },
        CapabilityCount: container.capabilities.len(),
        Reserved: 0,
    };

    // Two attributes: the container and the handle list.
    let mut size = 0usize;
    // SAFETY: a null list with a size out-parameter is the documented size query;
    // it fails with ERROR_INSUFFICIENT_BUFFER and fills `size`.
    let probed = unsafe { InitializeProcThreadAttributeList(ptr::null_mut(), 2, 0, &mut size) };
    if probed != 0 || size == 0 {
        return Err(launch_error(
            "InitializeProcThreadAttributeList",
            spec.program,
            last_error(),
        ));
    }
    // An over-aligned buffer: the list requires `max_align_t` alignment, and a
    // `Vec<usize>` gives it without a manual allocator.
    let mut list = vec![0usize; size.div_ceil(size_of::<usize>())];
    let list_ptr: *mut c_void = list.as_mut_ptr().cast();
    // SAFETY: `list_ptr` points at `size` bytes of suitably aligned storage that
    // outlives the list's use, as the API requires.
    let initialized = unsafe { InitializeProcThreadAttributeList(list_ptr, 2, 0, &mut size) };
    if initialized == 0 {
        return Err(launch_error(
            "InitializeProcThreadAttributeList",
            spec.program,
            last_error(),
        ));
    }

    let result = (|| -> Result<LaunchedProcess> {
        // SAFETY: `list_ptr` is an initialized list, and `capabilities` is a
        // local that outlives the list's use.
        let ok = unsafe {
            UpdateProcThreadAttribute(
                list_ptr,
                0,
                PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                (&capabilities as *const SECURITY_CAPABILITIES).cast(),
                size_of::<SECURITY_CAPABILITIES>(),
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if ok == 0 {
            return Err(launch_error(
                "set PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES",
                spec.program,
                last_error(),
            ));
        }

        // The child inherits its three standard streams and the listener, and
        // nothing else: `bInheritHandles` is TRUE because the handle list
        // requires it, and the list is what narrows that back down to these four.
        let mut handles: Vec<HANDLE> = Vec::with_capacity(spec.inherited.len() + 3);
        handles.extend_from_slice(spec.inherited);
        handles.push(spec.stdin);
        handles.push(spec.stdout);
        handles.push(spec.stderr);
        // SAFETY: as above: `handles` is a contiguous, live array that outlives
        // the list's use.
        let ok = unsafe {
            UpdateProcThreadAttribute(
                list_ptr,
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_mut_ptr().cast::<c_void>(),
                handles.len() * size_of::<HANDLE>(),
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if ok == 0 {
            return Err(launch_error(
                "set PROC_THREAD_ATTRIBUTE_HANDLE_LIST",
                spec.program,
                last_error(),
            ));
        }

        let startup = STARTUPINFOEXW {
            StartupInfo: STARTUPINFOW {
                cb: size_of::<STARTUPINFOEXW>() as u32,
                lpReserved: ptr::null_mut(),
                lpDesktop: ptr::null_mut(),
                lpTitle: ptr::null_mut(),
                dwX: 0,
                dwY: 0,
                dwXSize: 0,
                dwYSize: 0,
                dwXCountChars: 0,
                dwYCountChars: 0,
                dwFillAttribute: 0,
                dwFlags: STARTF_USESTDHANDLES,
                wShowWindow: 0,
                cbReserved2: 0,
                lpReserved2: ptr::null_mut(),
                hStdInput: spec.stdin,
                hStdOutput: spec.stdout,
                hStdError: spec.stderr,
            },
            lpAttributeList: list_ptr,
        };

        let mut line = command_line(spec.program, spec.args);
        let application = crate::wide_os(spec.program.as_os_str());
        let mut info = PROCESS_INFORMATION {
            hProcess: ptr::null_mut(),
            hThread: ptr::null_mut(),
            dwProcessId: 0,
            dwThreadId: 0,
        };
        // SAFETY: every pointer above is live for the call — the command line and
        // path buffers are locals, the attribute list is initialized, and
        // `startup` names it. `bInheritHandles` is TRUE, which
        // PROC_THREAD_ATTRIBUTE_HANDLE_LIST requires. CREATE_NEW_PROCESS_GROUP
        // makes the child a group leader so the relay can address it (design §3
        // step 9), and it disables Ctrl+C for that group, which is why the relay
        // sends CTRL_BREAK.
        //
        // **The two arguments after the creation flags are null, and each null is
        // a stated choice rather than a value nobody filled in.** `lpEnvironment`
        // null means the child inherits this process's environment block, which is
        // what `CREATE_UNICODE_ENVIRONMENT` is passed with. `lpCurrentDirectory`
        // null means it inherits this process's working directory: the child's
        // view of the tree is fixed by the exposure set rather than by a `chdir`,
        // and no caller of this function has ever named a different one, which is
        // why the spec carries no field for it.
        // `&startup` is a `STARTUPINFOEXW` and `CreateProcessW` takes a
        // `*const STARTUPINFOW`, which is the documented shape of the extended
        // form: the attribute list the child is created with is reached through
        // the `STARTUPINFOEXW` these two never see. The cast is the whole of
        // what makes the extended path work, and `EXTENDED_STARTUPINFO_PRESENT`
        // is what tells the kernel to read it that way.
        let created = unsafe {
            CreateProcessW(
                application.as_ptr(),
                line.as_mut_ptr(),
                ptr::null(),
                ptr::null(),
                1,
                EXTENDED_STARTUPINFO_PRESENT
                    | CREATE_UNICODE_ENVIRONMENT
                    | CREATE_NEW_PROCESS_GROUP,
                ptr::null(),
                ptr::null(),
                (&startup as *const STARTUPINFOEXW).cast(),
                &mut info,
            )
        };
        if created == 0 {
            return Err(launch_error("CreateProcessW", spec.program, last_error()));
        }
        // `info!`, and the single most useful line in the file: the program and
        // the pid, which is also the child's process-group id (it is created with
        // CREATE_NEW_PROCESS_GROUP) and so the value the stop relay addresses. A
        // run whose log lacks this line confined nothing (shared §7).
        tracing::info!(
            step = "CreateProcessW",
            program = %spec.program.display(),
            pid = info.dwProcessId
        );
        // The thread handle is not needed: the child runs on its own, and
        // `CreateProcessW` handed it over with nothing else owning it.
        handles::close(info.hThread);
        Ok(LaunchedProcess {
            process: info.hProcess,
            pid: info.dwProcessId,
        })
    })();
    // SAFETY: `list_ptr` is the list initialized above, deleted exactly once
    // here, after every value it named has been used.
    unsafe { DeleteProcThreadAttributeList(list_ptr) };
    result
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt, path::Path};

    use windows_sys::Win32::{Foundation::LocalFree, UI::Shell::CommandLineToArgvW};

    use super::*;

    /// Parse `line` with the API the child's own runtime uses.
    fn parse(line: &[u16]) -> Vec<String> {
        argv_units(line)
            .into_iter()
            .map(|units| String::from_utf16_lossy(&units))
            .collect()
    }

    /// The same parse, **without the lossy step**: what the child's runtime reads,
    /// as the units it read.
    fn argv_units(line: &[u16]) -> Vec<Vec<u16>> {
        let mut argc = 0i32;
        // SAFETY: `line` is NUL-terminated and alive for the call; the returned
        // argv block is freed below with LocalFree.
        let argv = unsafe { CommandLineToArgvW(line.as_ptr(), &mut argc) };
        assert!(!argv.is_null(), "CommandLineToArgvW refused the line");
        let mut parsed = Vec::new();
        for index in 0..argc as isize {
            // SAFETY: argv holds argc entries, each a NUL-terminated string owned
            // by the block returned above.
            let entry = unsafe { *argv.offset(index) };
            // SAFETY: `entry` is one of those NUL-terminated strings.
            parsed.push(unsafe { crate::from_wide_units(entry) }.to_vec());
        }
        // SAFETY: the block came from CommandLineToArgvW, which documents
        // LocalFree as its release.
        unsafe { LocalFree(argv.cast()) };
        parsed
    }

    #[test]
    fn the_command_line_round_trips_through_the_c_parser() {
        // The quoting must be the CommandLineToArgvW convention, because that is
        // what the child's argv is built from. Testing against the parser itself
        // beats hand-written expectations.
        let args: Vec<OsString> = [
            "",
            "plain",
            "with space",
            "with\ttab",
            "with\nnewline",
            "with\rcarriage",
            "with\u{c}form feed",
            r#"with"quote"#,
            r#"ends with backslash\"#,
            r#"trailing\"#,
            r#"\\"#,
            r#"a\"b"#,
            r#"\\\"#,
            "中文参数",
            "tinio-sandbox-fd=1040",
        ]
        .iter()
        .map(OsString::from)
        .collect();
        let line = command_line(Path::new(r"C:\Program Files\tinio\serve.exe"), &args);
        let parsed = parse(&line);
        assert_eq!(parsed[0], r"C:\Program Files\tinio\serve.exe");
        let expected: Vec<String> = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            &parsed[1..],
            &expected[..],
            "round trip through CommandLineToArgvW"
        );
        // The same fixture through the unit-level oracle, which is what the next
        // test needs and what pins that the lossless path is not a different
        // quoting: every one of these arguments is valid UTF-8, so the two
        // readings have to agree unit for unit.
        let units = argv_units(&line);
        let expected: Vec<Vec<u16>> = args.iter().map(|arg| arg.encode_wide().collect()).collect();
        assert_eq!(
            &units[1..],
            &expected[..],
            "the same round trip in code units"
        );
    }

    #[test]
    fn the_command_line_round_trips_units_that_are_not_utf8() {
        // **An argument is arbitrary UTF-16, and this is the round trip the lossy
        // form could not make**: `to_string_lossy` reads every one of the units
        // below as U+FFFD, so the child's parser would be handed a different
        // argument from the one the operator passed — and the child's argv is this
        // launcher's own argv filtered, with `tinio-sandbox-fd=<n>` among it. The
        // fixture covers the three shapes that matter: a bare surrogate, one that
        // forces quoting, and one that has to be escaped beside a quote and a
        // trailing backslash.
        let unpaired = OsString::from_wide(&[0xD800]);
        let spaced = OsString::from_wide(&[0x61, 0xD800, 0x20, 0x62]);
        let escaped = OsString::from_wide(&[0x5C, QUOTE, 0xDC00, 0x5C]);
        // A program path with the same unit in it, because the head of the line is
        // built by the same function and quoted by the same predicate.
        let mut program_units: Vec<u16> = r"C:\srv\".encode_utf16().collect();
        program_units.push(0xD800);
        program_units.extend(" serve.exe".encode_utf16());
        let program_os = OsString::from_wide(&program_units);
        let program = Path::new(&program_os);
        let args = [unpaired, spaced, escaped];
        let line = command_line(program, &args);
        assert_eq!(line.last(), Some(&0), "the buffer is NUL-terminated");
        let parsed = argv_units(&line);
        let mut expected = vec![program_units];
        expected.extend(args.iter().map(|arg| arg.encode_wide().collect::<Vec<_>>()));
        assert_eq!(
            parsed, expected,
            "the units the parser read are the units that went in"
        );
        // A fixture that somehow carried no surrogate would make the assertion
        // above hold for the wrong reason.
        assert!(parsed[1].contains(&0xD800), "{:?}", parsed[1]);
        assert!(parsed[3].contains(&0xDC00), "{:?}", parsed[3]);
    }

    #[test]
    fn the_command_line_is_nul_terminated() {
        let line = command_line(Path::new(r"C:\s.exe"), &[]);
        assert_eq!(line.last(), Some(&0));
    }
}
