//! Waiting for a confined child and undoing what its launch set up.

use std::{process::ExitStatus, thread::JoinHandle};

use tracing::debug;

use crate::{error::Result, launcher::Terminal};

/// A confined child the platform started. The core only ever waits for it, so
/// the platform keeps its own handle — a `std::process::Child` for
/// `sandbox-exec`, a Win32 process handle for an AppContainer — behind the
/// closure.
///
/// It also carries **the terminal event the relay saw**, because the two facts
/// are needed together and only the platform can join them. The relay runs on
/// its own thread and owns the `Observer`; [`Self::with_relay`] hands that thread
/// over, [`Self::wait`] joins it, and **the thread's return value is the
/// channel** — the answer comes back through the join rather than through a
/// shared cell. Without both halves the "parent emits `Event::Error` when the
/// child never became ready" rule (design decision 19) would have nothing to
/// consult, and the parent would be guessing.
///
/// There is no slot to create first, and that is the simplification the join
/// bought: with the answer travelling through `JoinHandle`, nothing has to exist
/// before the relay thread starts, there is no lock to poison, no value to clone
/// on every read, and no ordering rule for a launcher to remember.
///
/// **`Child` is not `Send`, and that is a property rather than an oversight.**
/// The wait closure is a bare `Box<dyn FnMut() -> Result<ExitStatus>>`, and the
/// Windows launcher's closure carries a raw process handle besides; nothing here
/// crosses a thread boundary, because every call site confines *before* the
/// runtime exists (design §10) on the thread that already owns the child. A
/// future caller that needs to move one would add `+ Send` to that closure —
/// and the Windows crate would then have to decide what to do about its handle,
/// which is a decision worth making when something needs it rather than now.
///
/// # Examples
///
/// ```
/// use std::{process::Command, thread};
///
/// use tinio_sandbox_core::Child;
///
/// # #[cfg(unix)] {
/// let mut process = Command::new("true").spawn().unwrap();
/// // A real relay returns what the child reported; this one reports nothing.
/// let relay = thread::spawn(|| None);
/// let mut child = Child::new(move || Ok(process.wait()?)).with_relay(relay);
/// assert!(child.terminal().is_none(), "nothing has relayed yet");
/// let _ = child.wait();
/// # }
/// ```
#[derive(derive_more::Debug)]
pub struct Child {
    /// `#[debug(skip)]` because the closure has no `Debug` impl and could not
    /// be printed if it did. `derive_more::Debug` rather than a hand-written
    /// `impl` so the skip is declared in the type rather than buried in a
    /// formatter — and because `Confinement` derives `Debug`, which needs this
    /// to exist: every `unwrap_err()` in the platform plans depends on it.
    #[debug(skip)]
    wait: Box<dyn FnMut() -> Result<ExitStatus>>,
    /// The launcher's relay thread — **and the channel its answer travels back
    /// through**: the thread returns what it recorded, and [`Self::wait`] takes
    /// that out of the join. `std::thread` rather than a runtime handle because
    /// the relay's own thread is a `std::thread` on both launcher platforms —
    /// the runtime lives *inside* it, not around it.
    relay: Option<JoinHandle<Option<Terminal>>>,
    /// Filled in by [`Self::wait`] from the join. `None` until then, which is
    /// also what a caller reads for a child that never reported.
    terminal: Option<Terminal>,
}

impl Child {
    /// Wrap a platform wait function.
    ///
    /// The relay goes in separately, with [`Self::with_relay`], because the two
    /// are not always both there: the core's own tests run a child with no relay
    /// at all, and a launcher never has one without a child.
    pub fn new(wait: impl FnMut() -> Result<ExitStatus> + 'static) -> Self {
        Self {
            wait: Box::new(wait),
            relay: None,
            terminal: None,
        }
    }

    /// Hand over the relay thread, so [`Self::wait`] can join it — and keep the
    /// terminal event it returns.
    pub fn with_relay(mut self, relay: JoinHandle<Option<Terminal>>) -> Self {
        self.relay = Some(relay);
        self
    }

    /// Block until the child exits, then until its relay has finished.
    ///
    /// **The join is what makes [`Self::terminal`] final.** The child's exit is
    /// what closes its stdout, and the relay ends when it reads EOF — but the
    /// process being reaped and the other thread noticing are two different
    /// events, with no ordering between them. A caller that read the answer as
    /// soon as the wait returned would be reading a snapshot, and on a healthy
    /// child it would usually read `None`: the answer to "did this child ever
    /// become ready?" would be wrong on the runs that went *right*. So the wait
    /// does not return until the relay has.
    ///
    /// That makes it the wait's business that the child's stdout really does
    /// reach EOF: a descendant left holding the pipe would keep the relay — and
    /// so this call — waiting. `serve` spawns no children (design's evidence
    /// table), and a launcher that can leave one behind must deal with it before
    /// its wait closure returns.
    ///
    /// What the wait returned is traced (`step = "wait"`) before the join, so a
    /// launch that hangs is diagnosable from the log of what it had already
    /// reached rather than only from a reproduction.
    ///
    /// # Errors
    ///
    /// Whatever the platform's wait reports. A relay that panicked is not one of
    /// them: the panic is in this crate's own code, and [`Self::terminal`]
    /// reports a dead relay as "nothing was recorded", which is the same answer
    /// a caller gets for a child that never reported.
    pub fn wait(&mut self) -> Result<ExitStatus> {
        let status = (self.wait)();
        // The child's own answer, which is what a stuck launcher needs to see:
        // an exit code that arrived, or the platform error that stopped one.
        match &status {
            Ok(status) => debug!(
                step = "wait",
                code = status.code(),
                success = status.success()
            ),
            Err(error) => debug!(step = "wait", error = %error),
        }
        if let Some(relay) = self.relay.take() {
            // A relay that panicked recorded nothing, which is the same answer a
            // child that never reported gives — and the same one the earlier
            // shared-slot design produced for a poisoned lock. There is no lock
            // here to poison.
            self.terminal = relay.join().ok().flatten();
        }
        status
    }

    /// The terminal event the relay saw, if the child sent one.
    ///
    /// **Read it after [`Self::wait`] returns** — the wait joins the relay, so
    /// by then the answer is final rather than a snapshot. The caller's decision
    /// ("did this child ever become ready?") is only meaningful once the child
    /// is gone.
    ///
    /// `None` means neither `Listening` nor `Error` arrived, which a launcher
    /// turns into its own `Event::Error` carrying the exit status. A relay that
    /// panicked is reported the same way — a bug in our own code rather than a
    /// condition to propagate, and a child whose relay died has certainly not
    /// reported readiness.
    pub fn terminal(&self) -> Option<&Terminal> {
        self.terminal.as_ref()
    }
}

/// Wait for the confined child, then undo the launch.
///
/// Teardown runs on every path — a clean exit, a failed child and a failed
/// wait alike — because a failed launch is exactly when an ACL grant or a
/// temporary directory most needs reverting (design decision 15). The status
/// returned is always the **child's**; teardown cannot mask it.
///
/// **It borrows the child rather than consuming it, and that is the whole reason
/// the signature is `&mut Child`.** The caller's next move is to ask
/// [`Child::terminal`] whether the child ever became ready (design decision 19),
/// and a by-value `supervise` takes that away: the only place left to read the
/// read would be *before* the wait, while the relay is still running, which is
/// the snapshot this API exists to avoid. The wait joins the relay, so by the
/// time this returns the answer is final.
///
/// **Teardown cannot report a failure, and that is deliberate rather than a
/// gap.** A rollback that can fail belongs in the platform's wait closure, which
/// owns the platform's error type and can name the directory it could not fix
/// (design §11's Windows row); a `Drop` guard covers the `Child` that is never
/// waited on at all. What is left for this callback is the work that cannot fail
/// — disarming a signal handler, removing a path the launcher created — which is
/// what both launcher platforms pass it.
///
/// That the teardown ran is traced (`step = "teardown"`), because a rollback
/// that silently did not happen is indistinguishable from one that did.
///
/// # Errors
///
/// The child's wait failure, if it failed. A teardown that panics is not
/// caught: it is a bug in our own code, not a runtime condition.
///
/// # Examples
///
/// ```
/// use std::process::Command;
///
/// use tinio_sandbox_core::{Child, supervise};
///
/// # #[cfg(unix)] {
/// let mut process = Command::new("true").spawn().unwrap();
/// let mut child = Child::new(move || Ok(process.wait()?));
/// let mut undone = false;
/// let status = supervise(&mut child, || undone = true).unwrap();
/// assert!(status.success());
/// assert!(undone);
/// // The child is still ours afterwards — which is what the terminal read
/// // needs, and what a by-value `supervise` would have taken away.
/// assert!(child.terminal().is_none(), "no relay ran in this example");
/// # }
/// ```
pub fn supervise(child: &mut Child, teardown: impl FnOnce()) -> Result<ExitStatus> {
    let status = child.wait();
    teardown();
    debug!(step = "teardown");
    status
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        net::SocketAddr,
        process::{Child as ProcessChild, ChildStdout, Command, ExitStatus, Stdio},
        thread,
        time::Duration,
    };

    use tokio::runtime::Builder;

    use super::*;
    use crate::{
        error::Error,
        event::Event,
        launcher::Observer,
        testing::{blocking, loopback},
    };

    /// A whole chunk per read, which is how a real pipe delivers a frame: the
    /// relay's own tests force the opposite case, and this one is here to run a
    /// child's real stdout through the same seam both launchers use.
    const PIPE_CHUNK: usize = 1024;

    /// A child that exits with `code`, portable across the CI matrix.
    fn exiting(script: &str) -> Command {
        #[cfg(windows)]
        {
            let mut command = Command::new("cmd");
            command.args(["/c", script]);
            command
        }
        #[cfg(not(windows))]
        {
            let mut command = Command::new("sh");
            command.args(["-c", script]);
            command
        }
    }

    fn child_for(script: &str) -> Child {
        let mut process = exiting(script).spawn().expect("spawn");
        Child::new(move || Ok(process.wait()?))
    }

    /// The terminal event a relayed child would have reported.
    fn listening() -> Terminal {
        Terminal::Listening(loopback(9000))
    }

    /// The environment variable a reporting child reads its data line from.
    ///
    /// **The line travels in the environment rather than in the script**, and on
    /// Windows that is what makes it arrive intact: `cmd` has no escape for a
    /// double quote, so a JSON body written into the command line comes back from
    /// `CreateProcess` with a backslash before every quote, `echo` prints those
    /// backslashes, and the frame decodes to nothing. A value the shell expands
    /// was never part of that command line.
    const DATA_VAR: &str = "TINIO_SANDBOX_TEST_DATA";

    /// A child that prints one terminal event's frame and nothing else.
    ///
    /// **Both field lines are `Event::encode()`'s own output**, split out of the
    /// frame rather than re-spelled here, so a change to the wire format reaches
    /// this test instead of being bypassed by it.
    ///
    /// `std::process::Child` goes by `ProcessChild` here because `Child` is this
    /// crate's own type in the same module — the alias is a collision, not a
    /// preference.
    fn reporting(addr: SocketAddr) -> ProcessChild {
        let frame = Event::Listening { addr }.encode();
        let mut lines = frame.lines();
        let event_line = lines.next().expect("the frame carries an event line");
        let data_line = lines.next().expect("the frame carries a data line");
        // **One command per line on Windows, because `cmd` prints a line at a
        // time**: it expands a variable only as far as the first newline, so a
        // whole frame in one variable arrives truncated to its first line. The
        // blank line the protocol dispatches on is `echo.`, the only way to print
        // one.
        #[cfg(windows)]
        let script = format!("echo {event_line}&echo %{DATA_VAR}%&echo.");
        // `printf` rather than `echo`, which would reinterpret the payload's
        // backslashes and swallow a leading `-`; the format string carries the
        // newlines and both values go through untouched.
        #[cfg(not(windows))]
        let script = format!("printf '%s\\n%s\\n\\n' '{event_line}' \"${DATA_VAR}\"");
        exiting(&script)
            .env(DATA_VAR, data_line)
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the reporting child")
    }

    /// The relay on a thread of its own — **the launcher shape, not a test
    /// device**, and the only place in this crate a runtime is built by hand.
    ///
    /// Both launcher platforms hand the child's stdout to `Observer` on a
    /// `std::thread` they spawned, whose runtime is that thread's own, and pass
    /// the join handle to [`Child::with_relay`]; a tokio task would not produce
    /// the handle that call consumes, so the thread has to be a real one. What
    /// `docs/tests.md` settles is how a *test* awaits — `#[tokio::test]` /
    /// `async fn`, no `Runtime::block_on` — and that is why the test below is
    /// async and why the one `block_on` here stands in the relay's own thread
    /// rather than in the test body.
    ///
    /// The child's stdout is a pipe, which is a `std::io::Read` while the relay
    /// wants an `AsyncBufRead`, so it goes through `crate::testing::blocking` —
    /// the same bridge the relay's own tests use for an in-memory reader. The
    /// launcher platforms bridge it instead with a runtime's own file type
    /// (`tokio::fs::File::from_std`, which is what reads a child's pipe on both
    /// of them), which a core test cannot: that is tokio's `fs` feature, and the
    /// core's dev-dependencies deliberately stop at `io-util`, `macros` and `rt`.
    fn spawn_relay(stdout: ChildStdout) -> thread::JoinHandle<Option<Terminal>> {
        thread::spawn(move || {
            let runtime = Builder::new_current_thread()
                .build()
                .expect("a current-thread runtime");
            runtime
                .block_on(Observer::new(Vec::new()).observe(blocking(stdout, PIPE_CHUNK)))
                .expect("the relay reads the child's stdout to its end")
                .terminal()
                .cloned()
        })
    }

    #[test]
    fn supervise_returns_the_child_status() {
        let mut teardown_ran = false;
        let status = supervise(&mut child_for("exit 3"), || teardown_ran = true).unwrap();
        assert_eq!(status.code(), Some(3));
        assert!(teardown_ran, "teardown must run after the child exits");
    }

    #[test]
    fn teardown_runs_even_when_the_child_fails() {
        // A failed child is exactly when the ACLs, the profile and the jail
        // directory most need undoing.
        let mut runs = 0;
        let _ = supervise(&mut child_for("exit 1"), || runs += 1);
        assert_eq!(runs, 1);
    }

    #[test]
    fn a_wait_failure_still_runs_teardown() {
        let mut runs = 0;
        let result = supervise(
            &mut Child::new(|| Err(Error::Wait(io::Error::other("boom")))),
            || runs += 1,
        );
        assert!(result.is_err());
        assert_eq!(runs, 1, "teardown is not conditional on a clean wait");
    }

    #[test]
    fn the_status_is_the_childs_not_the_teardowns() {
        let status: ExitStatus = supervise(&mut child_for("exit 0"), || ()).unwrap();
        assert!(status.success());
    }

    #[test]
    fn wait_joins_the_relay_so_the_terminal_is_final() {
        // Reading the answer after the wait is only worth something if the relay
        // has finished, and the two events have no ordering of their own: the
        // child is reaped by the wait, while the relay notices EOF on its stdout
        // whenever the scheduler gets to it. The relay here sleeps well past the
        // child's exit, so **without the join the read below happens first** —
        // and a perfectly healthy child is reported as never having become
        // ready, which is the failure the join exists to prevent.
        let relay = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            Some(listening())
        });
        let mut child = child_for("exit 0").with_relay(relay);
        let status = supervise(&mut child, || ()).unwrap();
        assert!(status.success());
        assert_eq!(
            child.terminal(),
            Some(&listening()),
            "the relay's answer must be final once the wait has returned"
        );
    }

    #[tokio::test]
    async fn a_real_childs_frame_reaches_the_terminal_through_the_relay() {
        // **The seam, end to end, and the one no task-scoped test could see.**
        // Both launcher platforms do exactly this and nothing else: they take the
        // child's stdout, run `Observer` over it on a `std::thread` whose runtime
        // is the thread's own, hand that handle to `Child::with_relay`, and read
        // the terminal after the wait. Every piece is tested above — the relay
        // against in-memory readers, the supervision against a fake closure and a
        // hand-made thread — and none of those tests joins the pieces, so a
        // mismatch between what one hands over and what the next one expects (the
        // reader's type, what the thread returns, when the join happens) is
        // visible here or nowhere.
        let addr = loopback(9000);
        let mut process = reporting(addr);
        let stdout = process.stdout.take().expect("stdout is piped");

        let mut child = Child::new(move || Ok(process.wait()?)).with_relay(spawn_relay(stdout));
        let status = supervise(&mut child, || ()).unwrap();
        assert!(status.success());
        assert_eq!(
            child.terminal(),
            Some(&Terminal::Listening(addr)),
            "the frame a real child printed must survive the relay and the wait"
        );
    }
}
