//! Process hygiene for a child this process spawned: the one silent kill-and-reap.
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
//! caller that needs the exit status does not want this function at all — `tinio_util::testing`'s
//! `poll_exit`/`wait_for_exit` are the bounded waits that return one.
//!
//! **Why this is a module of its own rather than a name in `testing`.** `tinio_util::testing`
//! carries the same two lines on its own failing path and re-exports this one, so a test tree sees
//! one path either way. What must not happen is a *production* caller reaching them through that
//! feature: `tinio-cli`'s daemon parent kills a child it has just reported as unready
//! (`src/daemon.rs`), and turning `testing` on for a shipped binary would drag `tinio-core`,
//! `serde_json` and `tracing-subscriber` in for a `kill` and a `wait`. Nothing here needs more
//! than `std`, so nothing here is behind a feature.

use std::process::Child;

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
