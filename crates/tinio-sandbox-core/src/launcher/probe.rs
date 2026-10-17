//! The handshake a forked **probe** reports its verdict through.
//!
//! A backend that cannot ask a question of its own process — because the call it
//! would make is irreversible, or because it would confine the launcher itself —
//! asks it of a throwaway child instead. The child computes one boolean, writes
//! one byte down a pipe and exits without returning into Rust; the parent reads
//! that byte and ends the child. `tinio-sandbox-linux` asks whether this kernel
//! grants an unprivileged user namespace; `tinio-sandbox-macos` asks whether the
//! kernel compiles a rendered profile. One handshake, two implementations — and
//! they had already drifted: macOS's reap attempts a `SIGKILL` and tolerates a
//! reap failure when the verdict was sound, while Linux's did neither.
//!
//! **This module owns the parent's half of that pipe and nothing before it.**
//! The `pipe()` call and the `fork` stay with the caller, and for one reason
//! each: the pipe is created before the fork because its two descriptors are what
//! the fork separates — the child inherits one end and the parent keeps the other
//! — and the fork is `unsafe` (a child of a threaded process is not something a
//! signature can promise), so it is the platform crate's site with that crate's
//! own safety argument. This crate is `unsafe_code = "forbid"` throughout and
//! cannot own the second even if it wanted to. Everything after it — the
//! non-blocking flag, the bounded read, the kill and the bounded reap — is
//! ordinary, fail-closed, and was written twice for no reason.
//!
//! **macOS's semantics are the rule here, and Linux's probe gains them.** The
//! kill is attempted, an `ESRCH` is a `debug!` rather than a returned error, and a
//! reap that fails is tolerated when the verdict itself was sound. A probe that
//! refused an answer because its `kill` lost a race with an already-exited child
//! would fail a launch that was never wrong; the verdict is what the launch is
//! decided on, and the reap is cleanup.
//!
//! **Every wait is bounded, and every expiry is a refusal** (design §11's
//! fail-closed, applied to the question the probe asks). An EOF is a failed check
//! rather than a "no" — `Ok(false)` is a child that ran and reported a negative,
//! which is an ordinary outcome — and a timeout is never read as either answer: a
//! probe that answered "unavailable" on a hang would silently downgrade a
//! mechanism, and one that answered "accepted" would launch a child against a
//! profile nothing compiled.

use std::{
    fs::File,
    io::{self, Read as _, Write as _},
    os::fd::OwnedFd,
    thread::sleep,
    time::{Duration, Instant},
};

use rustix::{
    fs::{OFlags, fcntl_setfl},
    io::Errno,
    process::{Pid, Signal, WaitOptions, kill_process, waitpid},
};
use tracing::debug;

use crate::error::{Result, fail, step};

/// The `step` names one probe's own failures carry.
///
/// **One field per operation the handshake performs, so each caller keeps its own
/// words.** The failures are the probe's, but what they are *called* belongs to
/// the site that knows what was being probed: macOS names all three of them with
/// its `VALIDATOR_STEP`, while Linux distinguishes the flag, the read and the
/// reap. One name would have collapsed one of those two spellings into the
/// other's.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::ProbeSteps;
///
/// // One operation under one name: every failure of the handshake carries it.
/// let steps = ProbeSteps::named("validator");
/// assert_eq!(steps.nonblocking, "validator");
/// assert_eq!(steps.read, "validator");
/// assert_eq!(steps.reap, "validator");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProbeSteps {
    /// The `F_SETFL`/`O_NONBLOCK` that lets the read share the reap's deadline.
    pub nonblocking: &'static str,
    /// The bounded read of the child's one byte.
    pub read: &'static str,
    /// The `SIGKILL` and the bounded `waitpid` that end the child, whichever way
    /// the read went — and the name the debug events for a kill or a reap that
    /// was tolerated carry.
    pub reap: &'static str,
}

impl ProbeSteps {
    /// One name for the whole handshake, for a probe that names its failures
    /// alike.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::ProbeSteps;
    ///
    /// let steps = ProbeSteps::named("userns probe");
    /// assert_eq!(steps.read, steps.reap);
    /// ```
    pub const fn named(step: &'static str) -> Self {
        Self {
            nonblocking: step,
            read: step,
            reap: step,
        }
    }
}

/// Await the forked child's verdict, and end the child.
///
/// **The parent's whole half of the handshake**, called once the kernel has
/// returned the child's pid: `reader` is the pipe's read end — which this call
/// takes and closes — `child` the process the fork just made, and `timeout` how
/// long the handshake as a whole may take. `Ok(true)` is the child's byte saying
/// yes and `Ok(false)` its byte saying no; a refusal is
/// [`Error::Step`](crate::Error::Step) with one of [`ProbeSteps`]' names and the
/// underlying [`io::Error`].
///
/// **One deadline for both ends, and the reap gets whatever the read left of it.**
/// The read end is made non-blocking so a resting pipe is polled rather than
/// waited on, and the same instant ends both loops: a child that answered but
/// could not be reaped is a failure, and so is one that never answered, because
/// the point of the bound is that a wedged probe becomes an ordinary refusal
/// rather than a launcher that waits forever.
///
/// **The child is ended on every path, the read's included** — a validator whose
/// child outlived it, or lingered as a zombie, would outlive the check it was
/// for. The kill is attempted rather than required: this process forked that
/// child and it has almost always already exited, so `ESRCH` is the ordinary
/// answer and is logged rather than returned. A reap that fails for any other
/// reason is tolerated when the read itself came back with an answer, and
/// returned only when it did not — the verdict is the caller's diagnosis, and a
/// second failure must not replace it.
///
/// # Errors
///
/// [`Error::Step`](crate::Error::Step) when the non-blocking flag cannot be set,
/// when the read ends at EOF or outlives the deadline or fails for any other
/// reason, and when the reap outlives the deadline or fails while the verdict was
/// unsound.
///
/// # Examples
///
/// The fork is the caller's, so the call is shown at the shape a launcher makes
/// it in and not run: the read end, the child the kernel just made, the failure
/// names, and the bound both ends share.
///
/// ```no_run
/// use std::time::Duration;
///
/// use rustix::process::Pid;
/// use tinio_sandbox_core::{ProbeSteps, await_verdict};
///
/// # fn await_child(child: Pid, reader: std::os::fd::OwnedFd) -> tinio_sandbox_core::Result<bool> {
/// await_verdict(
///     reader,
///     child,
///     ProbeSteps::named("probe"),
///     Duration::from_secs(5),
/// )
/// # }
/// ```
pub fn await_verdict(
    reader: OwnedFd,
    child: Pid,
    steps: ProbeSteps,
    timeout: Duration,
) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    let verdict = read_verdict(reader, steps, deadline);
    // The read's own answer is what decides whether its failure is the diagnosis,
    // and the child is ended before either is reported: `end` may return the
    // deadline's refusal instead, which is why its `?` comes first.
    end(child, verdict.is_ok(), steps, deadline)?;
    verdict
}

/// Hand the verdict to the parent, or report that it could not be handed over.
///
/// **The child's whole half of the handshake**: [`bool`] as the one byte the
/// parent's read decodes, `0`/`1` through `u8::from`. One writer for the encoding,
/// so the two ends of a probe cannot come to different answers about what a byte
/// means.
///
/// **It returns rather than exiting, and that is the caller's to do.** The exit
/// has no shared spelling — it is `rustix::runtime::exit_group` on Linux and
/// `libc::_exit` on macOS, which is one of that crate's enumerated `unsafe`
/// sites — and it is not a detail a helper may hide: a forked child that returned
/// into arbitrary Rust could take a lock another thread held at fork time, and
/// the stdlib's `exit` would flush inherited stdio buffers and run `atexit`
/// handlers belonging to the parent's world. **The caller exits immediately after
/// this call, without returning into its own Rust.** An `Err` means the parent is
/// gone — a broken pipe — and exiting is the whole job either way.
///
/// # Errors
///
/// The write's own failure, which a child whose parent has already given up on
/// the handshake reports and then ignores.
///
/// # Examples
///
/// ```no_run
/// use std::os::fd::OwnedFd;
///
/// use tinio_sandbox_core::report_verdict;
///
/// # fn child(writer: OwnedFd) {
/// // The child half: the one byte, and then the caller's own exit, which never
/// // returns into this process's Rust.
/// let _ = report_verdict(writer, true);
/// # }
/// ```
pub fn report_verdict(writer: OwnedFd, verdict: bool) -> io::Result<()> {
    let mut writer = File::from(writer);
    writer.write_all(&[u8::from(verdict)])
}

/// The child's one byte, or a refusal when it does not arrive in time.
///
/// **A timeout is a refusal and never an answer** — neither an acceptance nor a
/// negative, both of which are readings of a byte this function never got.
fn read_verdict(reader: OwnedFd, steps: ProbeSteps, deadline: Instant) -> Result<bool> {
    let reader = File::from(reader);
    fcntl_setfl(&reader, OFlags::NONBLOCK).map_err(step(steps.nonblocking))?;
    let mut reader = reader;
    let mut answer = [0u8; 1];
    loop {
        match reader.read(&mut answer) {
            // EOF: the child died without answering. A failed check rather than a
            // "no", which is a child that ran and reported the kernel's refusal.
            Ok(0) => {
                return Err(fail(
                    steps.read,
                    io::Error::from(io::ErrorKind::UnexpectedEof),
                ));
            }
            Ok(_) => return Ok(answer[0] != 0),
            // `Interrupted` is retried because a signal that arrived during the
            // poll is not a reason to call the check failed, and `WouldBlock` is
            // what the non-blocking end reports between the child's write and the
            // deadline.
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(fail(steps.read, io::Error::from(io::ErrorKind::TimedOut)));
                }
                sleep(Duration::from_millis(1));
            }
            Err(source) => return Err(fail(steps.read, source)),
        }
    }
}

/// End and reap the child: a `SIGKILL` and a bounded `NOHANG` reap, whichever way
/// the read ended.
///
/// **`SIGKILL` rather than the child's own exit**, because the path that needs
/// this most is the deadline's: a child that has not answered inside the timeout
/// is one this process must not wait for, and the signal is what turns a wedged
/// check into an ordinary failure.
///
/// `happy` is whether the verdict itself was sound; the reap's own failures are
/// returned only when it was not, so a healthy check is not turned into a failure
/// by a refused `kill` on a child that had already exited.
fn end(child: Pid, happy: bool, steps: ProbeSteps, deadline: Instant) -> Result<()> {
    // A child that has already exited is the ordinary case here, so the signal is
    // attempted and its answer reported in the log rather than returned: `ESRCH`
    // is "it is gone", which is what this call wants.
    if let Err(errno) = kill_process(child, Signal::KILL) {
        debug!(
            step = steps.reap,
            ?errno,
            "the probe's child was already gone"
        );
    }
    loop {
        match waitpid(Some(child), WaitOptions::NOHANG) {
            Ok(Some(_)) => return Ok(()),
            Ok(None) if Instant::now() < deadline => sleep(Duration::from_millis(1)),
            Ok(None) => {
                return Err(fail(steps.reap, io::Error::from(io::ErrorKind::TimedOut)));
            }
            // A signal that arrived while this process was in `waitpid` is not a
            // failure of the reap; the poll is retried against the same deadline.
            Err(Errno::INTR) => {}
            // A child this process already reaped, or one it cannot wait for, is
            // reported only when the verdict was unsound anyway: the caller's own
            // refusal is the operator's diagnosis, and this is the second thing
            // that went wrong.
            Err(source) if happy => {
                debug!(step = steps.reap, error = %source, "the probe's child was not reaped");
                return Ok(());
            }
            Err(source) => return Err(fail(steps.reap, source)),
        }
    }
}

#[cfg(test)]
mod tests {
    //! **Every path here runs against a real child, because the handshake's
    //! whole subject is one.** The module owns no fork — that is the caller's,
    //! and it is `unsafe` in a crate that forbids `unsafe` — so the children
    //! below are `std::process`'s: a `sh` that writes one byte, one that writes
    //! nothing, one that never answers, and one whose byte has already been
    //! reaped by the time the handshake is called. `waitpid` answers only for a
    //! child of this process, so a process spawned any other way could not be
    //! used for the reap's half at all.
    //!
    //! **This is also the only cover `report_verdict` has.** Its one production
    //! caller is a probe's forked child, and a fork's counters live in pages the
    //! parent never sees: the Linux probe writes its byte on every run of
    //! `mechanism.rs`'s own test, and no coverage report shows it.

    use std::{
        fs::File,
        io::{self, Read as _, pipe},
        os::{fd::OwnedFd, unix::process::ExitStatusExt as _},
        process::{Child, Command, Stdio},
        time::Duration,
    };

    use rustix::{
        io::Errno,
        process::{Pid, Signal, WaitOptions, waitpid},
    };

    use super::{ProbeSteps, await_verdict, report_verdict};
    use crate::Error;

    /// The caller's own spelling of the three failures, which is the half
    /// [`ProbeSteps`] stays a parameter for.
    fn steps() -> ProbeSteps {
        ProbeSteps::named("test probe")
    }

    /// `sh -c script` with one end of a fresh pipe as its stdout: the child, the
    /// pid the handshake is given, and the reader it takes.
    ///
    /// **The parent's own copy of the write end is gone by the time this
    /// returns**, because the `Command` that held it is a temporary dropped at
    /// the end of the `let`. That is what makes a child that writes nothing
    /// read as EOF rather than as a pipe that is still open.
    fn child_writing(script: &str) -> (Child, Pid, OwnedFd) {
        let (reader, writer) = pipe().expect("a pipe");
        let child = Command::new("sh")
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
            .stdout(Stdio::from(writer))
            .spawn()
            .expect("spawn the probe's child");
        let pid = Pid::from_raw(i32::try_from(child.id()).expect("a pid fits in an i32"))
            .expect("a live pid");
        (child, pid, OwnedFd::from(reader))
    }

    /// Whichever of the two loops expired, the refusal is a timeout under the
    /// caller's own name for the step that ran out.
    fn assert_timed_out(err: &Error) {
        assert!(
            matches!(
                err,
                Error::Step { step, source }
                    if (*step == steps().read || *step == steps().reap)
                        && source.kind() == io::ErrorKind::TimedOut
            ),
            "{err:?}"
        );
    }

    #[test]
    fn the_childs_one_byte_is_the_verdict() {
        // **The two answers a probe can read, and the difference between them
        // matters more than the byte.** `Ok(true)` is the mechanism the caller
        // was asking about; `Ok(false)` is the kernel refusing, which is an
        // ordinary outcome the model table falls back from. Neither is a
        // failure, and neither is the EOF below.
        let (mut child, pid, reader) = child_writing("printf '\\001'");
        assert!(
            await_verdict(reader, pid, steps(), Duration::from_secs(5))
                .expect("a child that answered"),
            "a `1` byte is an acceptance"
        );
        let _ = child.wait();

        let (mut child, pid, reader) = child_writing("printf '\\000'");
        assert!(
            !await_verdict(reader, pid, steps(), Duration::from_secs(5))
                .expect("a child that answered"),
            "a `0` byte is a refusal the child reported, not a failed check"
        );
        let _ = child.wait();
    }

    #[test]
    fn a_child_that_dies_without_answering_refuses_rather_than_answering() {
        // **EOF is a failed check, never a "no"** — the design's own reading,
        // and the one an operator acts on differently: a `false` says the kernel
        // refused the namespace, while this says the probe never got to ask.
        // The child is a shell that exits without writing anything, and the
        // parent holds no other copy of the write end.
        let (mut child, pid, reader) = child_writing("true");
        let err = await_verdict(reader, pid, steps(), Duration::from_secs(5))
            .expect_err("a child that never answered");
        assert!(
            matches!(
                &err,
                Error::Step { step, source }
                    if *step == steps().read
                        && source.kind() == io::ErrorKind::UnexpectedEof
            ),
            "{err:?}"
        );
        let _ = child.wait();
    }

    #[test]
    fn a_wedged_child_is_refused_at_the_deadline_and_ended() {
        // **The bound is what turns a hang into an ordinary refusal**, and this
        // is the only path that reaches it: a child that neither answers nor
        // exits. The read polls an empty pipe until the deadline, and the reap
        // that follows inherits whatever is left of that same instant — which is
        // why which of the two refuses is the kernel's answer rather than this
        // test's, and both are the same refusal to the caller.
        let (mut child, pid, reader) = child_writing("exec sleep 5");
        let err = await_verdict(reader, pid, steps(), Duration::from_millis(50))
            .expect_err("a child that never answers");
        assert_timed_out(&err);

        // **The child is ended on this path too, and it is the path that needs
        // it most.** Either the handshake reaped it inside the deadline — and
        // then the pid no longer exists, which is what `kill` with no signal
        // asks — or the reap expired and this test's own `wait` is what reaps
        // it, whose status can only be the `SIGKILL` the handshake sent.
        match child.wait() {
            Ok(status) => assert_eq!(status.signal(), Some(Signal::KILL.as_raw())),
            Err(err) => assert_eq!(err.raw_os_error(), Some(Errno::CHILD.raw_os_error())),
        }
    }

    #[test]
    fn the_child_is_ended_on_the_reads_path_too() {
        // **A verdict does not end the handshake; the reap does.** The child
        // here answers and then stays alive, so the reap's `NOHANG` poll meets a
        // running process rather than the zombie a child that exits on its own
        // leaves behind — the branch that makes the poll bounded rather than an
        // assumption. A probe whose child outlived its own check would leave a
        // process behind on every launch.
        let (mut child, pid, reader) = child_writing("printf '\\001'; exec sleep 5");
        assert!(
            await_verdict(reader, pid, steps(), Duration::from_secs(5))
                .expect("a child that answered"),
            "the verdict is not what ends the child"
        );
        // Reaped, not merely dead: a zombie is still a child that answers a
        // `waitpid`, and only a reaped one answers `ECHILD`.
        assert_eq!(
            waitpid(Some(pid), WaitOptions::NOHANG).expect_err("the child was ended and reaped"),
            Errno::CHILD
        );
        let _ = child.wait();
    }

    #[test]
    fn a_reap_failure_is_tolerated_when_the_verdict_was_sound() {
        // **macOS's semantics, which this module makes the rule for both
        // platforms** (see the module doc). The child has already been reaped by
        // this process, so the handshake's `kill` answers `ESRCH` and its
        // `waitpid` `ECHILD` — the state a probe's child is in almost every
        // time, because it exits the instant it has written its byte. A probe
        // that refused the answer over that would fail a launch that was never
        // wrong.
        let (mut child, pid, reader) = child_writing("printf '\\001'");
        child.wait().expect("the shell that wrote its byte");
        assert!(
            await_verdict(reader, pid, steps(), Duration::from_secs(5))
                .expect("a sound verdict outlives the reap's failure"),
            "the byte is still in the pipe after the writer is gone"
        );
    }

    #[test]
    fn a_reap_failure_is_the_refusal_when_the_read_produced_none() {
        // **The other side of the same rule.** With no verdict to protect, the
        // reap's failure is the diagnosis the caller gets — and it is the one it
        // gets rather than the EOF, because the read had nothing to say and this
        // says why the child could not be ended.
        let (mut child, pid, reader) = child_writing("true");
        child.wait().expect("the shell that answered nothing");
        let err = await_verdict(reader, pid, steps(), Duration::from_secs(5))
            .expect_err("an unsound verdict does not excuse the reap");
        assert!(
            matches!(
                &err,
                Error::Step { step, source }
                    if *step == steps().reap
                        && source.raw_os_error() == Some(Errno::CHILD.raw_os_error())
            ),
            "{err:?}"
        );
    }

    #[test]
    fn the_reads_own_errno_is_a_refusal_rather_than_an_answer() {
        // **The arm that keeps a read this handshake did not expect from being
        // read as one of its two answers** — neither `Ok(true)` nor `Ok(false)`
        // is available to a descriptor the kernel refuses to read, and the
        // caller has to be told which of its three operations failed.
        //
        // The shape is synthetic: a pipe's *write* end is passed where the read
        // end belongs, because there is no other way to hand this function a
        // descriptor `read(2)` answers `EBADF` for. The property is not
        // synthetic — it is the half of `read_verdict` that says an errno is a
        // `step` and never a verdict.
        let (mut child, pid, reader) = child_writing("true");
        drop(reader);
        let (_reader, writer) = pipe().expect("a pipe");
        let err = await_verdict(OwnedFd::from(writer), pid, steps(), Duration::from_secs(5))
            .expect_err("a descriptor that cannot be read");
        assert!(
            matches!(
                &err,
                Error::Step { step, source }
                    if *step == steps().read
                        && source.raw_os_error() == Some(Errno::BADF.raw_os_error())
            ),
            "{err:?}"
        );
        let _ = child.wait();
    }

    #[test]
    fn the_verdict_is_written_as_one_byte() {
        // **The child's whole half, and the encoding both ends agree on.**
        // `report_verdict` has exactly one production caller — a forked child
        // that exits without returning into Rust — so the byte it writes is
        // observed here rather than through that child: a fork's counters are
        // the parent's pages, and nothing the child writes back is ever counted.
        let (reader, writer) = pipe().expect("a pipe");
        report_verdict(OwnedFd::from(writer), true).expect("one byte");
        let mut reader = File::from(OwnedFd::from(reader));
        let mut byte = [0u8; 1];
        assert_eq!(reader.read(&mut byte).expect("the verdict"), 1);
        assert_eq!(byte[0], 1, "`u8::from(true)` is the accepted verdict");

        let (reader, writer) = pipe().expect("a pipe");
        report_verdict(OwnedFd::from(writer), false).expect("one byte");
        let mut reader = File::from(OwnedFd::from(reader));
        assert_eq!(reader.read(&mut byte).expect("the verdict"), 1);
        assert_eq!(byte[0], 0, "and `u8::from(false)` the negative one");

        // **A parent that gave up on the handshake is a `BrokenPipe`, and the
        // child is to report it and exit anyway** — the caller's `let _ =` is
        // what makes a probe that lost its reader an exit rather than a
        // second failure to handle in a process that may only exit.
        let (reader, writer) = pipe().expect("a pipe");
        drop(reader);
        let err = report_verdict(OwnedFd::from(writer), true).expect_err("the parent is gone");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }
}
