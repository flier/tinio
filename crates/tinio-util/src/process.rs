//! Process hygiene for a child this process spawned: the one silent kill-and-reap, and the bounded
//! poll every wait shape is written on.
//!
//! [`kill_and_reap`] is the whole rule, and it is one function because the sequence — and not
//! either of its two lines — is what every caller needs. A kill nobody waits on leaves a zombie;
//! a wait with no kill before it blocks for as long as the child feels like running; and the two
//! lines were spelled at every teardown and failure path in six crates, each of them a place where
//! a forgotten half is a suite that hangs or a server that keeps its listener, its state lock and
//! `target/<profile>/deps/<suite>-<hash>` open for the rest of the run (the last is a `LNK1104` on
//! the next Windows build).
//!
//! **Both answers are ignored on purpose.** `kill` on a child that has already exited and `wait`
//! on one already reaped both answer errors, and neither is this call's news: every caller is
//! already unwinding from a failure, and a second error raised there would bury the first. A
//! caller that needs the exit status does not want this function at all — [`poll_exit`] is the
//! bounded wait that returns one, and `tinio_util::testing`'s `wait_for_exit` and `wait_for_path`
//! are the two shapes built on it.
//!
//! **Why this is a module of its own rather than a name in `testing`.** `tinio_util::testing`
//! carries the same two lines on its own failing path and re-exports this one, so a test tree sees
//! one path either way. What must not happen is a *production* caller reaching them through that
//! feature: `tinio-cli`'s daemon parent kills a child it has just reported as unready
//! (`src/daemon.rs`), and turning `testing` on for a shipped binary would drag `tinio-core`,
//! `serde_json` and `tracing-subscriber` in for a `kill` and a `wait`. Nothing here needs more
//! than `std`, so nothing here is behind a feature — and [`poll_exit`] joined the pair on the same
//! argument (2026-09-28), when `tinio-util`'s job guard — a module outside the feature on purpose,
//! so that its tests run in the default graph — needed the bounded wait and could not name a
//! feature-gated one.

use std::{
    process::{Child, ExitStatus},
    thread,
    time::{Duration, Instant},
};

/// Kill `child` and reap it, ignoring both answers.
///
/// **Kill before wait, and never the other way round**: the wait is what reaps the zombie, and it
/// only returns once the kill has landed, so the pair is the correct order of two lines that a
/// caller could as easily write the other way round — which is the drift one definer prevents.
///
/// Cheap on a child that is already gone: both calls answer errors and both are ignored. The
/// callers lean on that — a `Drop` and an explicit stop both run on the same path.
pub fn kill_and_reap(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// The bounded `try_wait` poll every wait shape in the workspace is written on: `Some` when the
/// child exits inside `within`, `None` when the deadline passes with it not seen to exit.
///
/// **The poll is shared and the failing path is not, which is why this is public.** In
/// `tinio_util::testing`, `None` means kill the child and panic. A launcher leg cannot do only
/// that: the confined **grandchild** outlives a launcher that never exits (the accepted
/// no-job-object gap), so the leg has to sweep that process before it fails — `wait_for_exit`
/// cannot reach it and the leg cannot use `wait_for_exit`. One poll, two endings, neither
/// expressible as the other — and **this is the one home of that argument**, because this is the
/// definition; `tinio_util::testing`'s re-export points here rather than stating it again.
///
/// **A `try_wait` that fails is not an exit and not a panic either.** The bound is the whole
/// contract, so a read that could not be made is answered like a child that has not been seen to
/// exit: the loop runs to its deadline and returns `None`. Both wrong answers are avoided that
/// way — an `Err` is no evidence that the child exited (which a returned `Some` would claim), and
/// a status that cannot be read is not a reason to abort a wait the caller asked to be bounded
/// (`wait_for_exit`'s `None` path kills and reports, which is the right answer for it too).
///
/// **`try_wait` rather than `wait_with_output`**, which is the price of the bound: nothing here
/// needs the child's captured output, since a child that inherits this process's streams has
/// already written it where the reader of a red log will see it. **The interval is fixed at 50 ms
/// and is not a knob**: it is a resolution for a deadline measured in seconds, and a caller that
/// needs another one — `tinio_util::testing`'s `wait_for_path` is the case, whose three callers
/// legitimately differ — passes its own rather than tuning this.
///
/// # Examples
///
/// ```
/// use std::{process::Command, time::Duration};
///
/// use tinio_util::process::poll_exit;
///
/// // A child that exits at once, spelled for both platforms: `cmd /c exit` on Windows, `true`
/// // everywhere else.
/// let mut child = if cfg!(windows) {
///     Command::new("cmd.exe")
///         .args(["/c", "exit"])
///         .spawn()
///         .unwrap()
/// } else {
///     Command::new("true").spawn().unwrap()
/// };
/// let status = poll_exit(&mut child, Duration::from_secs(30)).expect("the child exits at once");
/// assert!(status.success());
/// ```
pub fn poll_exit(child: &mut Child, within: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(50));
    }
}
