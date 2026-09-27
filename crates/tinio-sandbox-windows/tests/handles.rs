//! The raw handle plumbing: what a pipe owns, and what a hand-off gives away.
//!
//! **This file exists because a `Pipe` used to own nothing.** `handles::pipe()`
//! handed back two raw `HANDLE`s and the launch site closed them by hand after
//! `CreateProcessW`, so every failure before that point — the second
//! `handles::pipe()`, `inherited_stderr()`, `Job::create()`, `process::launch`,
//! `job.assign`, `write_payload`, `control::arm` — returned with four handles
//! still open. One of them is worse than a leak: with the event pipe's write end
//! still open in the launcher, the child's stdout never reaches end-of-stream and
//! the relay's read never returns, which is a hang (`handles::parent_end`'s doc
//! states it). The lookups below are how a closed handle is told from an open
//! one without an `unsafe` block of this file's own: `SetHandleInformation`
//! refuses a handle that is not there.

#![cfg(target_os = "windows")]

use tinio_sandbox_windows::handles;

/// **One test and not two, because the second half depends on the first's
/// handles being gone**: a handle value freed by one test and reused by another
/// would make an assertion about a *specific* value meaningless, and this binary
/// runs its tests in parallel.
#[test]
fn a_pipe_closes_the_ends_it_still_owns_and_gives_away_the_ones_it_hands_over() {
    // Both ends are closed by the owner's drop.
    let pipe = handles::pipe().expect("a pipe");
    let (read, write) = match (pipe.read(), pipe.write()) {
        (Some(read), Some(write)) => (read, write),
        _ => panic!("a pipe just created owns both of its ends"),
    };
    drop(pipe);
    assert!(
        handles::mark_inheritable(read).is_err(),
        "the read end outlived the pipe that owned it: a launch that fails after \
         the pipes are created leaks it"
    );
    assert!(
        handles::mark_inheritable(write).is_err(),
        "the write end outlived the pipe that owned it — and a surviving event \
         pipe's write end is not a leak but a hang"
    );

    // An end that is handed over is the new owner's, and the pipe's drop no
    // longer closes it — while the end left behind still goes with the pipe.
    let mut pipe = handles::pipe().expect("a pipe");
    let handed = pipe.take_write().expect("the write end");
    let file = handles::parent_end(handed);
    let left = pipe.read().expect("the read end");
    drop(pipe);
    assert!(
        handles::mark_inheritable(handed).is_ok(),
        "a handed-off end must survive the pipe it came from"
    );
    assert!(
        handles::mark_inheritable(left).is_err(),
        "the end the pipe kept goes with it"
    );
    drop(file);
    assert!(
        handles::mark_inheritable(handed).is_err(),
        "the new owner closes the end it was given"
    );
}
