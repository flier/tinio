//! The Job object, tested against an unconfined child: the primitive is
//! independent of the AppContainer, and a failure here is then unambiguous.

#![cfg(target_os = "windows")]

use std::{
    os::windows::io::AsRawHandle,
    process::{Child, Command, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

use tinio_sandbox_windows::job::Job;
use windows_sys::Win32::Foundation::HANDLE;

/// The raw process handle behind a `std::process::Child`.
fn raw_handle(child: &Child) -> HANDLE {
    child.as_raw_handle().cast()
}

/// A child that comfortably outlives the test.
fn sleeper() -> Child {
    Command::new("cmd.exe")
        .args(["/c", "ping -n 60 127.0.0.1 > nul"])
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn a child that outlives the test")
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
