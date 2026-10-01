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
//! **The Win32 sequence is `tinio_util::job_object`'s, and this module is the policy over it.**
//! That primitive creates the job with the limits it is handed, hands back a value whose `Drop`
//! closes the handle exactly once, and is what both jobs in this workspace are made of — and it is
//! the part of `tinio-util` a production build on Windows compiles, which is why it is gated on the
//! target rather than on a feature. What stays here is this crate's own decisions: the one limit
//! (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, and deliberately not
//! `JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION`, which the test-side guard beside it sets), the
//! per-launch lifetime this value owns, the `step` each call reports under (design §7) and the
//! error type it surfaces ([`Error::Launch`](tinio_sandbox_core::Error::Launch)). **This module is
//! therefore no longer one of the crate's enumerated `unsafe` sites** (design decision 14): the
//! module-level `#![allow(unsafe_code)]` that used to sit at the top of this file moved with the
//! calls into `tinio-util`'s `job_object`, and a reader counting this crate's sites should count
//! that one as gone.

use tinio_util::job_object::JobObject;
use windows_sys::Win32::{
    Foundation::HANDLE, System::JobObjects::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};

use crate::_core::{Result, launch};

/// A job object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` set.
///
/// Holding the job for the child's whole lifetime is the point: when the launcher
/// exits — cleanly, by panic, or by `TerminateProcess` — the kernel closes this
/// handle, the last handle to the job goes with it, and the confined server dies
/// rather than surviving as an orphan holding a listener.
///
/// **The drop is the kill, and the close site is the primitive's.** This type carries no `Drop` of
/// its own, because [`JobObject`]'s closes the handle for it — which is what makes kill-on-close a
/// guarantee rather than a promise: every exit path from the launcher reaches that close, including
/// the ones that never return.
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
    object: JobObject,
}

impl Job {
    /// Create a job with kill-on-close set.
    ///
    /// The limit is set before the handle is handed back, so no caller can hold a
    /// job that does not yet kill: a failure to set it drops the job — and closes
    /// the handle — rather than returning one whose guarantee is missing. **The
    /// sequence itself is `tinio_util::job_object`'s**, which is where that
    /// invariant is kept and argued; what this call adds is the limit, and the
    /// step the failure is reported under.
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
        let object = JobObject::create(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
            .map_err(|failure| launch(failure.call, "job", failure.source))?;
        // `info!`: the job is one of the two mechanisms that make the confinement
        // a *lifetime* rather than a permission, so it belongs on the "to what"
        // side of shared §7's split.
        tracing::info!(
            step = "CreateJobObjectW",
            limit = "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE"
        );
        Ok(Self { object })
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
    // confines to the module that owns the raw calls —
    // `tinio_util::job_object`, which this method is a delegation to.
    // [`crate::handles::mark_inheritable`] carries the same allow for the same
    // reason.
    pub fn assign(&self, process: HANDLE) -> Result<()> {
        self.object
            .assign(process)
            .map_err(|source| launch("AssignProcessToJobObject", "job", source))?;
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
        self.object
            .terminate(code)
            .map_err(|source| launch("TerminateJobObject", "job", source))
    }
}

#[cfg(test)]
mod tests {
    //! The Job object, tested against an unconfined child: the primitive is
    //! independent of the AppContainer, and a failure here is then unambiguous.
    //!
    //! **The spawn site takes the guard, so this move costs `spawn_guard` nothing.**
    //! `sleeper` spawns through `tinio_util::job::spawn` rather than a raw
    //! `Command::spawn`, and the guard's scan reads `crates/*/tests/**` only — so
    //! there was no `ALLOWED` entry for the retired `tests/job.rs` to move and no
    //! hole to record. The 2026-10-01 placement cleanup moved these legs here from
    //! `tests/job.rs` (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`).

    use std::{
        os::windows::io::AsRawHandle,
        process::{Child, Command, Stdio},
        thread::sleep,
        time::{Duration, Instant},
    };

    use tinio_util::job;

    use super::*;

    /// The raw process handle behind a `std::process::Child`.
    fn raw_handle(child: &Child) -> HANDLE {
        child.as_raw_handle().cast()
    }

    /// A child that comfortably outlives the test.
    ///
    /// **Through the guard, like every other spawn site in this workspace** (`tinio_util::job`):
    /// the child outlives the test by design, so a run that died without unwinding would otherwise
    /// leave the `cmd.exe`/`ping.exe` pair behind. What the legs then do with it is unchanged —
    /// membership of the job this module tests is what [`Job::assign`] and the assertions below
    /// read, and the guard's job is a different one.
    fn sleeper() -> Child {
        let mut command = Command::new("cmd.exe");
        command
            .args(["/c", "ping -n 60 127.0.0.1 > nul"])
            .stdout(Stdio::null());
        job::spawn(&mut command).expect("spawn a child that outlives the test")
    }

    /// Assert `child` is still running, and fail the test if it is not.
    ///
    /// **Without this the two tests below can pass for the wrong reason.**
    /// [`expect_death`] returns the moment `try_wait` answers, so a child that had
    /// already exited on its own — `cmd.exe` refusing `/c ping …`, `ping.exe`
    /// missing from `PATH`, a machine where the child's own start failed — makes
    /// both tests green while the job object did nothing at all. Asserting liveness
    /// immediately before the kill is what makes the death afterwards the job's.
    fn expect_alive(child: &mut Child, what: &str) {
        assert!(
            child.try_wait().expect("poll the child").is_none(),
            "the child was already gone before {what}, so the job was never shown to kill anything"
        );
    }

    /// Wait for `child` to be gone, or fail loudly.
    fn expect_death(child: &mut Child, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match child.try_wait().expect("poll the child") {
                Some(_) => return,
                None if Instant::now() < deadline => sleep(Duration::from_millis(50)),
                None => panic!("the child outlived {what}"),
            }
        }
    }

    #[test]
    fn dropping_the_job_kills_the_process_it_owns() {
        let mut child = sleeper();
        let job = Job::create().expect("create the job");
        job.assign(raw_handle(&child)).expect("assign the child");
        expect_alive(&mut child, "the job's close");
        // The job closes here; KILL_ON_JOB_CLOSE takes the child with it.
        drop(job);
        expect_death(&mut child, "the job's close");
    }

    #[test]
    fn terminating_the_job_kills_the_process() {
        let mut child = sleeper();
        let job = Job::create().expect("create the job");
        job.assign(raw_handle(&child)).expect("assign the child");
        expect_alive(&mut child, "TerminateJobObject");
        job.terminate(1).expect("terminate the job");
        expect_death(&mut child, "TerminateJobObject");
    }
}
