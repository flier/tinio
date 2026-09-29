//! The Win32 job object, spelled once: create it, set its limits, hold it, close it.
//!
//! **The call sequence is shared and the policy is not, which is the whole reason this module
//! exists.** Two crates keep a job object for different reasons — `tinio_util::job`'s `GuardJob`
//! puts the *test* process in one, so that everything it spawns dies with it, and
//! `tinio_sandbox_windows::job::Job` puts a confined *child* in one, so that killing the launcher
//! takes the child with it (design decision 20) — and each carried its own copy of the same calls:
//! `CreateJobObjectW` with the same two null arguments, `SetInformationJobObject` with the same
//! `JOBOBJECT_EXTENDED_LIMIT_INFORMATION`, the handle held to a `Drop` that closes it, and the
//! same `Send`/`Sync` wrapper over a raw handle. What differs between the two is policy and stays
//! with them: which limits each job is created with, how long each holds it, and the error type
//! each surfaces (`io::Error` for the guard, the sandbox core's `Error::Launch` for the launcher).
//!
//! **The invariant this type exists to keep is one sentence**: the limits are set before the
//! handle escapes `JobObject::create` — a job that does not yet kill is not a value a caller can
//! hold — and `Drop` is the single close site for every path out of it. Closing the last handle to
//! a job is what fires `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` and terminates every process still in
//! it, so a second close site, or a path that handed a job back with its limits unset, would be a
//! hole in that guarantee rather than an untidiness.
//!
//! **This module is compiled into a production build on Windows, and it is deliberate.** The
//! launcher creates its job inside `tinio-sandbox-windows`'s own build — not a test build, and not
//! behind a feature — so the module is gated on the target alone (the `#[cfg(windows)]` on its
//! declaration in `lib.rs`) and `tinio-util`'s `windows-sys` edge is a plain target dependency
//! rather than one the `testing` feature turns on. **What stays out of production builds is the
//! guard**: `crate::job` is `#[cfg(any(test, feature = "testing"))]`, and the two calls that are
//! the guard's own rather than this sequence's — `GetCurrentProcess`, the adoption of the test
//! process, and Linux's `pre_exec` — stay in that module.
//!
//! **This module is one of the crate's enumerated `unsafe` sites (design decision 14)**, and the
//! `#![allow(unsafe_code)]` below is that site: everything under it is raw platform API — the six
//! `JobObjects` calls (creation, the limits, the assignment, the membership read, the accounting
//! query a leg asks for, and termination) and the `CloseHandle` that ends the job — and every block
//! carries the `// SAFETY:` argument for the call it makes.

#![allow(unsafe_code)]

// The two pointer helpers the accounting query below takes, gated with it because the query is
// **the one reading only a leg asks for**.
#[cfg(test)]
use std::ptr::{from_mut, null_mut};
use std::{
    ffi::c_void,
    io,
    mem::size_of,
    os::windows::io::AsRawHandle,
    process::Child,
    ptr::{from_ref, null},
};

// ...and the three `JobObjects` names that query is made of, gated for the same reason.
#[cfg(test)]
use windows_sys::Win32::System::JobObjects::{
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
    QueryInformationJobObject,
};
use windows_sys::{
    Win32::{
        Foundation::{CloseHandle, HANDLE},
        System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob,
            JOBOBJECT_BASIC_LIMIT_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectExtendedLimitInformation, SetInformationJobObject, TerminateJobObject,
        },
    },
    core::BOOL,
};

/// A Win32 job object, with the limits its creator asked for already set.
///
/// **Holding one is the guarantee**: the kernel terminates every process in the job when its last
/// handle closes, whichever way that closing came about — an orderly return, a panic, an abort, a
/// `TerminateProcess` from outside. Which limits are set, how long the value is held and what a
/// refusal is called are the caller's, and the two callers are named in the module doc.
///
/// **Existence is not kill-on-close.** `create(0)` is a valid job with no limits at all — nothing
/// kills when it closes — so a caller that wants that guarantee asks for
/// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` in `limits`, which is the one thing this type does not
/// decide.
///
/// # Examples
///
/// ```
/// use tinio_util::job_object::JobObject;
/// use windows_sys::Win32::System::JobObjects::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
///
/// // No process is ever assigned here, so the drop below has nothing to terminate — the
/// // guarantee a caller buys is about the processes it puts in.
/// let job = JobObject::create(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE).expect("a job object");
/// drop(job);
/// ```
#[derive(Debug)]
pub struct JobObject {
    handle: Handle,
}

impl JobObject {
    /// Create a job object with `limits` set — **the whole sequence in one call, so a job without
    /// its limits is not a value any caller can hold**.
    ///
    /// The `Self` is built before the limits are set so that the failure path below closes the
    /// handle it was just given; nothing observes the job in between, and [`Drop`] is the single
    /// close site for every path out of this function.
    ///
    /// # Errors
    ///
    /// The failing call's name and `io::Error`: `CreateJobObjectW` first, then
    /// `SetInformationJobObject`. The name travels in the [`Failure`] rather than being guessed at
    /// by the caller, because the two calls fail for different reasons and a caller that reports
    /// the step it failed in must not name the wrong one.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_util::job_object::JobObject;
    ///
    /// // A job with no limits at all — which of them a real caller wants is the caller's
    /// // policy, not this type's, and nothing is terminated when this one closes.
    /// let job = JobObject::create(0).expect("a job object");
    /// drop(job);
    /// ```
    pub fn create(limits: u32) -> Result<Self, Failure> {
        // SAFETY: a null security descriptor and a null name are both documented as valid, and
        // mean "unnamed, default security".
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        if handle.is_null() {
            return Err(Failure {
                call: "CreateJobObjectW",
                source: io::Error::last_os_error(),
            });
        }
        let job = Self {
            handle: Handle(handle),
        };
        let information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
            BasicLimitInformation: JOBOBJECT_BASIC_LIMIT_INFORMATION {
                LimitFlags: limits,
                ..Default::default()
            },
            ..Default::default()
        };
        // The information *class* and the structure it selects are two names that read alike:
        // `JobObjectExtendedLimitInformation` is this argument, the `9i32` that selects the layout,
        // and `JOBOBJECT_EXTENDED_LIMIT_INFORMATION` is the layout below. Swapping them compiles
        // into a wrong call rather than failing to compile, which is why the two are written
        // together here.
        //
        // SAFETY: `handle` is the job just created, `information` is a correctly sized
        // JOBOBJECT_EXTENDED_LIMIT_INFORMATION that outlives the call, and the class matches the
        // structure passed.
        let ok = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                from_ref(&information).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            return Err(Failure {
                call: "SetInformationJobObject",
                source: io::Error::last_os_error(),
            });
        }
        Ok(job)
    }

    /// Assign a live process handle to this job.
    ///
    /// **The job takes no ownership of `process`**: the caller keeps the handle it passed and
    /// stays responsible for closing it. What the job takes ownership of is the *process*, and it
    /// cannot be taken back — an assignment is one-way, and a process in a job is in it for the
    /// rest of its life.
    ///
    /// # Errors
    ///
    /// The call's `io::Error` — `ERROR_ACCESS_DENIED` on a host that will not nest this job inside
    /// the one the process is already in, which is the refusal the guard's per-child fallback
    /// exists for.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{os::windows::io::AsRawHandle, process::Command};
    ///
    /// use tinio_util::job_object::JobObject;
    /// use windows_sys::Win32::System::JobObjects::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    ///
    /// let mut child = Command::new("cmd.exe")
    ///     .args(["/c", "ping -n 60 127.0.0.1"])
    ///     .spawn()
    ///     .unwrap();
    /// let job = JobObject::create(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE).unwrap();
    /// job.assign(child.as_raw_handle().cast()).unwrap();
    /// // The child is the job's now: dropping the last handle to the job kills it.
    /// drop(job);
    /// let _ = child.wait();
    /// ```
    // The raw-pointer argument is this module's reason to exist rather than a hazard the
    // signature should hide: `HANDLE` is windows-sys's public handle type, named by both callers'
    // own signatures, and the contract — a live handle the caller owns — is proven by the caller.
    // An `unsafe fn` would put an `unsafe` block at every call site, which is what design decision
    // 14 confines to this module.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn assign(&self, process: HANDLE) -> io::Result<()> {
        // SAFETY: both handles are live and owned by the caller for the duration of the call; the
        // job takes no ownership of the process handle.
        let ok = unsafe { AssignProcessToJobObject(self.handle.0, process) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Whether `child` is in this job.
    ///
    /// **The job's own answer and not a bookkeeping of the caller's**: `IsProcessInJob` asks the
    /// kernel, so a child that reached the job by inheritance — the usual case for the guard — is
    /// answered for exactly as one that was assigned.
    ///
    /// # Errors
    ///
    /// The read's `io::Error`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::process::Command;
    ///
    /// use tinio_util::job_object::JobObject;
    ///
    /// let mut child = Command::new("cmd.exe")
    ///     .args(["/c", "exit"])
    ///     .spawn()
    ///     .unwrap();
    /// let job = JobObject::create(0).unwrap();
    /// assert!(!job.contains(&child).unwrap(), "nothing put it in this job");
    /// let _ = child.wait();
    /// ```
    pub fn contains(&self, child: &Child) -> io::Result<bool> {
        let mut result: BOOL = 0;
        // SAFETY: both handles are live — `child` owns its process handle and `self` its job — and
        // `result` is a live `i32` for the duration of the call.
        let ok = unsafe { IsProcessInJob(child.as_raw_handle(), self.handle.0, &mut result) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(result != 0)
    }

    /// Terminate every process in the job.
    ///
    /// **A terminating job does not fail for having nothing in it**, which is what lets a failure
    /// path call this without first asking whether the launch got as far as assigning a child.
    ///
    /// # Errors
    ///
    /// The call's `io::Error` when `TerminateJobObject` is refused.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_util::job_object::JobObject;
    ///
    /// let job = JobObject::create(0).expect("a job object");
    /// job.terminate(1)
    ///     .expect("nothing is in it, and that is not a failure");
    /// ```
    pub fn terminate(&self, code: u32) -> io::Result<()> {
        // SAFETY: `self.handle.0` is the job this value owns, and TerminateJobObject takes an exit
        // code and no other precondition.
        let ok = unsafe { TerminateJobObject(self.handle.0, code) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// How many processes the job holds right now — the membership a *descendant* reached, which
    /// no handle of this process's names.
    ///
    /// **A leg's reading rather than a product one**: the only question this answers that
    /// [`Self::contains`] does not is "did the grandchild arrive", and the product path never asks
    /// it — the job holds whatever the child spawned, and the kill is what acts on it.
    ///
    /// # Errors
    ///
    /// The query's `io::Error`.
    #[cfg(test)]
    pub fn active_processes(&self) -> io::Result<u32> {
        let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: `self.handle.0` is the live job, `info` is a correctly sized
        // JOBOBJECT_BASIC_ACCOUNTING_INFORMATION that outlives the call, and a null return-length
        // pointer is documented as accepted.
        let ok = unsafe {
            QueryInformationJobObject(
                self.handle.0,
                JobObjectBasicAccountingInformation,
                from_mut(&mut info).cast::<c_void>(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(info.ActiveProcesses)
    }
}

impl Drop for JobObject {
    // Close the job handle — the single close site, and what makes kill-on-close a guarantee rather
    // than a promise: every path out of this module reaches this, including the ones that never
    // return, which is a process that dies without unwinding and leaves the kernel to close the
    // handle for it.
    fn drop(&mut self) {
        // SAFETY: `self.handle.0` is the job this value owns and closes exactly once (nothing else
        // holds a copy: `Handle` is not `Copy`).
        unsafe {
            CloseHandle(self.handle.0);
        }
    }
}

/// A Win32 call that failed, carrying the call's own name — what lets a caller report the step it
/// failed in without a second copy of which call it was.
///
/// **Both callers report the step and neither re-derives it.**
/// `tinio_sandbox_windows::job::Job` passes [`call`](Self::call) straight into the core's
/// `launch(…)`, so the error's `step` field and the API that failed cannot drift apart; the guard
/// drops the name and keeps only [`source`](Self::source), which is the `io::Error` it has always
/// returned.
///
/// # Examples
///
/// ```no_run
/// use tinio_util::job_object::JobObject;
///
/// // The failing call names itself, so a caller reports the step it failed in.
/// if let Err(failure) = JobObject::create(0) {
///     eprintln!("{}: {}", failure.call, failure.source);
/// }
/// ```
#[derive(Debug)]
pub struct Failure {
    /// The call that failed, spelled as the Win32 API spells it: the string a caller's `step`
    /// field wants (`"CreateJobObjectW"`, `"SetInformationJobObject"`).
    pub call: &'static str,
    /// The error that call left behind.
    pub source: io::Error,
}

/// The job's handle, in the wrapper that carries the `Send`/`Sync` claim.
///
/// **The claim is about the raw pointer, not about the job's contents.** A kernel object's handle
/// is not thread-affine: every call this module makes on it — `AssignProcessToJobObject`,
/// `IsProcessInJob`, `QueryInformationJobObject`, `TerminateJobObject`, `CloseHandle` — may come
/// from any thread, and the kernel serializes them. The wrapper exists because `windows-sys`'s
/// `HANDLE` is a raw pointer, which is `!Send + !Sync` by construction, while both holders need
/// the claim: the guard lives in a `static`, whose type must be `Sync`, and the launcher's `Job`
/// rides back through the supervision a launch builds.
#[derive(Debug)]
struct Handle(HANDLE);

// SAFETY: see the type's doc — a job handle is usable from any thread of this process, and every
// method that touches one takes `&self`, so neither impl lets two threads see a `&mut`.
unsafe impl Send for Handle {}

// SAFETY: the same claim, for the shared case: `&Handle` reaches the kernel through calls that take
// the handle by value and mutate nothing this module owns.
unsafe impl Sync for Handle {}
