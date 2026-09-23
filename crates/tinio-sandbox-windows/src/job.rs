//! The Job object that ties the confined child's lifetime to the launcher's
//! (design decision 20, first half).
//!
//! `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` is what turns "the launcher exited" into
//! "the confined server is gone": the launcher holds this handle for the child's
//! whole lifetime, the kernel closes it however the launcher dies — a clean
//! return, a panic, a `TerminateProcess` — and the last handle to the job going
//! away terminates every process still in it. Without it a killed launcher leaves
//! an orphan holding a listener, which is the failure the job exists to make
//! impossible. The other half of the same obligation, the stop relay, is
//! `control`'s.
//!
//! **What the job does not cover is the window between the two calls that create
//! it, and the window is recorded here rather than claimed away.** The child is
//! created running (`CreateProcessW` without `CREATE_SUSPENDED`) and joins the
//! job only when `AssignProcessToJobObject` returns, so a launcher that dies
//! inside that window leaves a child the job never owned — and so does a failure
//! in either call, which the launch site handles by terminating the process on
//! both. Closing the window would take `CREATE_SUSPENDED` plus a `ResumeThread`
//! on the thread handle the launch closes today; it is not closed here because
//! what the child can do with it is bounded and already designed: it has
//! inherited the listener and the two pipe ends but **no payload**, so its stdin
//! read reaches EOF, `adopt` reports the launcher case, and it becomes a launcher
//! inside its own confinement — which fails closed at its first grant or bind
//! (design §11's "payload absent or malformed" row, decision 17). What a caller
//! must not conclude from this module is that the child is never unowned; what
//! they can conclude is that an unowned child is a child that cannot serve.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14).

#![allow(unsafe_code)]

use std::{
    ffi::c_void,
    mem::size_of,
    ptr::{from_ref, null},
};

use windows_sys::Win32::{
    Foundation::HANDLE,
    System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_BASIC_LIMIT_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, SetInformationJobObject, TerminateJobObject,
    },
};

use crate::{
    _core::{Result, launch},
    handles::{self, last_error},
};

/// A job object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` set.
///
/// Holding the job for the child's whole lifetime is the point: when the launcher
/// exits — cleanly, by panic, or by `TerminateProcess` — the kernel closes this
/// handle, the last handle to the job goes with it, and the confined server dies
/// rather than surviving as an orphan holding a listener.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::job::Job;
///
/// let job = Job::create()?;
/// // The child runs under the job's guarantee for as long as `job` lives; the
/// // `drop` that ends that lifetime is the kill.
/// let _ = job;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub struct Job {
    handle: HANDLE,
}

impl Job {
    /// Create a job with kill-on-close set.
    ///
    /// The limit is set before the handle is handed back, so no caller can hold a
    /// job that does not yet kill: a failure to set it drops the job — and closes
    /// the handle — rather than returning one whose guarantee is missing.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) naming the failing
    /// call.
    ///
    /// # Examples
    ///
    /// ```
    /// let job = tinio_sandbox_windows::job::Job::create();
    ///
    /// assert!(job.is_ok(), "{:?}", job.err());
    /// ```
    pub fn create() -> Result<Self> {
        // SAFETY: a null security descriptor and a null name are both documented
        // as valid, and mean "unnamed, default security".
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        if handle.is_null() {
            return Err(launch("CreateJobObjectW", "job", last_error()));
        }
        // The `Self` is built before the limit is set so that the failure path
        // below closes the handle it was just given; nothing observes the job in
        // between, and `Drop` is the single close site for every path out of this
        // function.
        let job = Self { handle };
        let limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
            BasicLimitInformation: JOBOBJECT_BASIC_LIMIT_INFORMATION {
                LimitFlags: JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                ..Default::default()
            },
            ..Default::default()
        };
        // The information *class* and the structure said class expects are two
        // different names that read alike: `JobObjectExtendedLimitInformation` is
        // this argument, the `9i32` that selects the layout, and
        // `JOBOBJECT_EXTENDED_LIMIT_INFORMATION` is the layout below. Swapping
        // them compiles into a wrong call rather than failing to compile, which is
        // why the two are written together here.
        //
        // SAFETY: `handle` is the job just created, `limits` is a correctly sized
        // JOBOBJECT_EXTENDED_LIMIT_INFORMATION that outlives the call, and the
        // class matches the structure passed.
        let ok = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                from_ref(&limits).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            return Err(launch("SetInformationJobObject", "job", last_error()));
        }
        // `info!`: the job is one of the two mechanisms that make the confinement
        // a *lifetime* rather than a permission, so it belongs on the "to what"
        // side of shared §7's split.
        tracing::info!(
            step = "CreateJobObjectW",
            limit = "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE"
        );
        Ok(job)
    }

    /// Assign `process` to the job.
    ///
    /// **The job takes no ownership of `process`**: the caller keeps the handle it
    /// passed and stays responsible for closing it. What the job takes ownership
    /// of is the *process*, and it cannot be taken back — an assignment is
    /// one-way, and a process in a job is in it for the rest of its life.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the assignment is
    /// refused. **The caller must terminate the process in that case** — a child
    /// outside the job is one the launcher cannot guarantee to take with it — and
    /// the launch site's composition is the only caller: it terminates the child
    /// on this error and on a failure to create the job at all, before it reports
    /// either.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{
    ///     os::windows::io::AsRawHandle,
    ///     process::{Command, Stdio},
    /// };
    ///
    /// use tinio_sandbox_windows::job::Job;
    ///
    /// let mut child = Command::new("cmd.exe")
    ///     .args(["/c", "ping -n 60 127.0.0.1 > nul"])
    ///     .stdout(Stdio::null())
    ///     .spawn()?;
    /// let job = Job::create()?;
    /// job.assign(child.as_raw_handle().cast())?;
    /// // The job holds the child now: dropping it kills the child with it.
    /// drop(job);
    /// let _ = child.wait();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    // `HANDLE` is windows-sys's public handle type rather than a hazard this
    // signature should hide: the contract — a live process handle the caller owns
    // and keeps — is proven by the caller, and marking the method `unsafe` would
    // put an unsafe block at every call site, which is what design decision 14
    // confines to this module. [`crate::handles::mark_inheritable`] carries the
    // same allow for the same reason.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn assign(&self, process: HANDLE) -> Result<()> {
        // SAFETY: both handles are live and owned by the caller for the duration
        // of the call; the job takes no ownership of the process handle.
        let ok = unsafe { AssignProcessToJobObject(self.handle, process) };
        if ok == 0 {
            return Err(launch("AssignProcessToJobObject", "job", last_error()));
        }
        tracing::info!(step = "AssignProcessToJobObject", process = ?process);
        Ok(())
    }

    /// Terminate every process in the job.
    ///
    /// The launch site's failure paths are the callers: a child that is running
    /// but that the launcher is about to abandon — because the assignment failed,
    /// or the job could not be created — is killed here rather than left unowned.
    /// A terminating job does not fail for having nothing in it.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the call is
    /// refused.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::job::Job;
    ///
    /// let job = Job::create()?;
    /// job.terminate(1)?;
    /// # Ok::<(), tinio_sandbox_core::Error>(())
    /// ```
    pub fn terminate(&self, code: u32) -> Result<()> {
        // SAFETY: `self.handle` is the job this value owns, and TerminateJobObject
        // takes an exit code and no other precondition.
        let ok = unsafe { TerminateJobObject(self.handle, code) };
        if ok == 0 {
            return Err(launch("TerminateJobObject", "job", last_error()));
        }
        Ok(())
    }
}

impl Drop for Job {
    // Close the job handle, which is what makes kill-on-close a guarantee rather
    // than a promise: every exit path from the launcher reaches this, including
    // the ones that never return.
    fn drop(&mut self) {
        handles::close(self.handle);
    }
}
