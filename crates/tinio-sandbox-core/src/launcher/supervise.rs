//! Waiting for a confined child and undoing what its launch set up.

use std::{io, process::ExitStatus, sync::mpsc::Receiver, thread::JoinHandle, time::Duration};

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
/// **That answer is final only once the child is gone, and one caller cannot wait
/// that long.** A launcher that has to *act* the moment its child becomes ready —
/// the Windows launcher publishes the child's `state` then, because the file's
/// DACL is current-user-only and the confined child cannot write it — needs the
/// announcement while the child is still serving. [`Self::with_ready`] carries the
/// mid-stream half: the same first terminal event, sent once by the relay's
/// `Observer` as it records it, read by [`Self::wait_for_ready`]. Two channels
/// rather than one because they answer at different times: "is it up?" versus "did
/// it ever come up?".
///
/// **The final half needs no slot to exist first, and that is the simplification
/// the join bought**: with the answer travelling through `JoinHandle`, nothing has
/// to exist before the relay thread starts for "did it ever come up?" — there is no
/// lock to poison, no value to clone on every read, and no ordering rule for a
/// launcher to remember.
///
/// **The mid-stream half is the exception, and it is exactly one thing: the
/// channel, which has to exist before the thread does.**
/// [`Self::with_ready`] takes the receiving end of an `mpsc` pair; the launcher
/// creates that pair **before it spawns the relay thread**, and the sender moves
/// into the thread with the child's stdout, where the closure builds the
/// [`Observer`] and hands the sender to it ([`Observer::with_notify`]). So the
/// installation is *inside* the thread and the pair is outside it — what the
/// ordering the join removed does not cover is one channel created early, not one
/// call written early. Both launcher platforms build the `Observer` that way.
///
/// That is the whole of it: a launcher that never wants the announcement creates
/// nothing, and one that does creates a channel. The record itself is unchanged
/// either way, which is what keeps the two channels two answers rather than two
/// copies.
///
/// [`Observer`]: crate::launcher::Observer
/// [`Observer::with_notify`]: crate::launcher::Observer::with_notify
///
/// **A launch can also fail after the child exists, and the child has to be
/// ended rather than dropped.** The macOS launcher publishes the child's `state`
/// the moment the child reports it is listening, and that publish is the one
/// fallible step left on the launch path: it writes into the state directory,
/// which can refuse between the launcher's pre-flight probe and the write. The
/// launcher's own `?` there would return the error and drop the [`Self`] — and
/// on a platform with no job object a dropped child is not an ended one, because
/// the handle the platform keeps is behind the wait closure, so the confined
/// server keeps listening with no `state` file for `tinio status`/`stop` to find
/// it. [`Self::with_kill`] is that path's half: the platform supplies the one
/// call that ends the process it started.
///
/// **`Child` is not `Send`, and that is a property rather than an oversight.**
/// The wait closure is a bare `Box<dyn FnMut() -> Result<ExitStatus>>`, the kill
/// closure beside it has the same shape, and the Windows launcher's wait closure
/// carries a raw process handle besides; nothing here crosses a thread boundary,
/// because every call site confines *before* the runtime exists (design §10) on
/// the thread that already owns the child. A future caller that needs to move
/// one would add `+ Send` to both closures — and the Windows crate would then
/// have to decide what to do about its handle, which is a decision worth making
/// when something needs it rather than now.
///
/// # Examples
///
/// **`no_run` because it spawns processes** (constitution III): the example is compiled
/// but not executed, so `cargo test --doc` neither starts a `true`, a `sleep 60` nor a
/// `sh` on a host whose purpose here is to check that the API is callable as written.
///
/// ```no_run
/// use std::{cell::RefCell, process::Command, rc::Rc, thread};
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
///
/// // The failure path's two halves: a killer that ends the process, and the wait
/// // that still reaps it — which is why the same process is shared through an
/// // `Rc<RefCell<_>>` instead of moving into the killer. A bare `RefCell` cannot
/// // be held by two closures, and a `std::process::Child` is not `Clone`: this
/// // example claimed `RefCell::clone` and therefore compiled on no unix host at
/// // all, which `#[cfg(unix)]` hid from every check that ran on Windows.
/// # #[cfg(unix)] {
/// let process = Rc::new(RefCell::new(
///     Command::new("sleep").arg("60").spawn().unwrap(),
/// ));
/// let killer = Rc::clone(&process);
/// let mut child = Child::new(move || Ok(process.borrow_mut().wait()?))
///     .with_kill(move || killer.borrow_mut().kill());
/// child.kill().expect("the killer ends the child");
/// let status = child.wait().expect("the wait still reaps it");
/// assert!(!status.success(), "the child was killed, not exited");
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
    /// The platform's way to end the child it started, when it has one (see
    /// [`Self::with_kill`]). `#[debug(skip)]` for the reason the wait closure
    /// above carries it.
    #[debug(skip)]
    kill: Option<Box<dyn FnMut() -> io::Result<()>>>,
    /// The launcher's relay thread — **and the channel its answer travels back
    /// through**: the thread returns what it recorded, and [`Self::wait`] takes
    /// that out of the join. `std::thread` rather than a runtime handle because
    /// the relay's own thread is a `std::thread` on both launcher platforms —
    /// the runtime lives *inside* it, not around it.
    relay: Option<JoinHandle<Option<Terminal>>>,
    /// Filled in by [`Self::wait`] from the join. `None` until then, which is
    /// also what a caller reads for a child that never reported.
    terminal: Option<Terminal>,
    /// The relay's **mid-stream** announcement, when the platform wired one (see
    /// [`Self::with_ready`]). `None` for a child nothing is waiting on — the core's
    /// own tests, and every platform that has no reason to act before the wait.
    ready: Option<Receiver<Terminal>>,
}

impl Child {
    /// Wrap a platform wait function.
    ///
    /// The relay goes in separately, with [`Self::with_relay`], because the two
    /// are not always both there: the core's own tests run a child with no relay
    /// at all, and a launcher never has one without a child. The same holds for
    /// the killer: [`Self::with_kill`] is a second, optional collaborator rather
    /// than part of this constructor, so a platform that has none — and every
    /// test that spawns a process it means to wait for — is unchanged.
    ///
    /// # Examples
    ///
    /// **`no_run` because it spawns a process** (constitution III):
    ///
    /// ```no_run
    /// use std::process::Command;
    ///
    /// use tinio_sandbox_core::Child;
    ///
    /// // The platform's own wait, wrapped: the closure owns whatever handle the platform
    /// // keeps, which is why nothing here names a `std::process::Child`.
    /// let mut process = Command::new("true").spawn().unwrap();
    /// let mut child = Child::new(move || Ok(process.wait()?));
    /// assert!(child.wait().expect("the child is reaped").success());
    /// ```
    pub fn new(wait: impl FnMut() -> Result<ExitStatus> + 'static) -> Self {
        Self {
            wait: Box::new(wait),
            kill: None,
            relay: None,
            terminal: None,
            ready: None,
        }
    }

    /// Hand over the relay thread, so [`Self::wait`] can join it — and keep the
    /// terminal event it returns.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::thread;
    ///
    /// use tinio_sandbox_core::{Child, Terminal};
    ///
    /// // A thread that already knows the answer stands in for the relay: what `wait` joins is
    /// // the handle, and what it keeps is the thread's own return value.
    /// let relay = thread::spawn(|| Some(Terminal::Listening("127.0.0.1:9000".parse().unwrap())));
    /// let child = Child::new(|| unreachable!("this child is never waited for")).with_relay(relay);
    /// assert!(child.terminal().is_none(), "the join has not happened yet");
    /// ```
    pub fn with_relay(mut self, relay: JoinHandle<Option<Terminal>>) -> Self {
        self.relay = Some(relay);
        self
    }

    /// Hand over the **readiness announcement** the relay's `Observer` fills, so a
    /// launcher can act the moment the child reports (see [`Self::wait_for_ready`]).
    ///
    /// The platform is what wires it: it builds the relay thread and the `Observer`
    /// inside it, so it is the only layer that can install the sender the thread
    /// announces on — and the receiver it keeps is this value's other half. A
    /// platform that has no use for the announcement simply never calls this, and
    /// [`Self::wait_for_ready`] then answers `None` at once rather than waiting for
    /// something no one will send.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::mpsc;
    ///
    /// use tinio_sandbox_core::Child;
    ///
    /// let (_tx, rx) = mpsc::channel();
    /// let child = Child::new(|| unreachable!("this child is never waited for")).with_ready(rx);
    /// // The shape is all this shows; `wait_for_ready`'s own example drives the value.
    /// assert!(child.terminal().is_none());
    /// ```
    pub fn with_ready(mut self, ready: Receiver<Terminal>) -> Self {
        self.ready = Some(ready);
        self
    }

    /// Give the child a way to be **ended** — the platform's one call that
    /// terminates the process this value waits for, for the launch that fails
    /// after the child exists (see [`Self::kill`]).
    ///
    /// **The child is not `kill`ed by dropping it, and that is why this is a
    /// platform-supplied closure rather than something the core can do itself.**
    /// On Windows a dropped [`Self`] closes the job object that owns the child
    /// (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, decision 20) and the process goes
    /// with it; on macOS there is no such object, and the only handle to the
    /// process is behind the wait closure — the core cannot reach through an
    /// opaque `FnMut`, and it may not call `kill(2)` itself: this crate is
    /// `unsafe_code = "forbid"` and the process is not the core's to know. So the
    /// platform that started it hands over the call, exactly as it hands over the
    /// wait and the relay.
    ///
    /// **This is a builder rather than part of [`Self::new`] because a killer may
    /// genuinely be absent, and `Option` says so in the type.** Windows leaves it
    /// unset deliberately: its `Child` already ends the child by dropping the job,
    /// so a killer there would duplicate a mechanism the platform has, and the
    /// `None` is a decision the platform states rather than an omission. A
    /// platform that launches no child at all never reaches either half.
    ///
    /// **The killer must not reap, and that is the one rule this closure has.**
    /// [`Self::kill`] carries the whole contract: it ends the process and stops
    /// there, [`Self::wait`] is still the caller's next call, and the wait is what
    /// reaps. A killer whose platform handle would also reap — a
    /// `std::process::Child::wait` written into this closure by mistake — reports
    /// the refusal it gets on the second wait rather than pretending it ended
    /// something.
    ///
    /// # Examples
    ///
    /// **`no_run` because it spawns a process** (constitution III): the example is compiled
    /// but not executed.
    ///
    /// ```no_run
    /// use std::process::Command;
    ///
    /// use tinio_sandbox_core::Child;
    ///
    /// let mut process = Command::new(if cfg!(windows) { "cmd" } else { "sh" })
    ///     .args(["/c", "exit 0"])
    ///     .spawn()
    ///     .unwrap();
    /// // The platform's own kill, whatever it is: `SIGKILL` to the pid, a
    /// // `TerminateProcess` on the handle.
    /// let mut child = Child::new(move || Ok(process.wait()?))
    ///     .with_kill(|| Err(std::io::Error::other("this platform has no killer")));
    /// assert!(child.kill().is_err(), "the closure's own answer comes back");
    /// let _ = child.wait();
    /// ```
    pub fn with_kill(mut self, kill: impl FnMut() -> io::Result<()> + 'static) -> Self {
        self.kill = Some(Box::new(kill));
        self
    }

    /// **End the child** — the launch-failure path, for a platform whose dropped
    /// [`Self`] would otherwise leave the confined process running.
    ///
    /// **This does not end the child's lifecycle; [`Self::wait`] does, and the
    /// caller calls it next.** The split is deliberate: the killer does the one
    /// thing a platform can express without owning the wait (a `SIGKILL` to a pid,
    /// a `TerminateProcess` on a handle), the wait is what **reaps** the process so
    /// no zombie is left and what joins the relay. So the sequence a caller on the
    /// failure path runs is `kill()` then `wait()` — the kill is what makes that
    /// wait return instead of blocking, and the wait is what makes it a reap
    /// rather than a leak.
    ///
    /// **After a kill, [`Self::wait`] reports the killed status and
    /// [`Self::terminal`] stays what it already was.** A child killed before it
    /// reported readiness has an empty relay, so the terminal record is `None`
    /// **for the rest of this value's life** — a null byte for its own byte, not
    /// the `None` that means "the relay has not been joined yet". A caller that
    /// reads the record to decide what the child said learns nothing new from
    /// this call, which is correct: a child ended by its launcher never reported.
    ///
    /// **A failure here must not mask the error the caller is already carrying**,
    /// and that is the caller's rule rather than this method's: the reason this
    /// exists is that a publish or a launch step failed after the child started,
    /// and *that* failure is what the operator has to see. The answered
    /// [`io::Error`] is the platform's own — the process already gone, a target
    /// that did not fit — and the caller reports it beside the original error
    /// rather than in place of it.
    ///
    /// **A child with no killer refuses rather than pretending.** [`Self::with_kill`]
    /// carries why a platform may leave it unset; what such a platform must *not*
    /// do is answer `Ok(())` to a caller that then believes a process it cannot
    /// reach has been ended. Reaching [`Self::kill`] on a child that never had a
    /// killer is a contract error in our own code, so it is reported as an
    /// [`io::Error`] — not [`Error::Wait`](crate::Error::Wait), which is the wait's own failure — and
    /// an operator reading it beside the original error can see that the child was
    /// left to the platform's own mechanism or to nothing.
    ///
    /// # Errors
    ///
    /// The killer's own failure, or the refusal of a child that never had one.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Child;
    ///
    /// // A child with no platform killer says so instead of silently succeeding.
    /// let mut child = Child::new(|| unreachable!("this child is never waited for"));
    /// let err = child.kill().expect_err("no killer was installed");
    /// assert!(err.to_string().contains("no killer"), "{err}");
    /// ```
    pub fn kill(&mut self) -> io::Result<()> {
        match self.kill.as_mut() {
            Some(kill) => kill(),
            None => Err(io::Error::other(
                "the platform installed no killer on this child: nothing was ended, and the \
                 process is whichever lifetime the platform ties it to",
            )),
        }
    }

    /// The first terminal event the child reported, **while it is still running** —
    /// `None` when it reported none, and `None` once `bound` elapses.
    ///
    /// **The wait for a launcher that has to act before the child exits**, and the
    /// reason it is not [`Self::wait`] plus [`Self::terminal`]: the record is final
    /// only after the relay has been joined, which happens when the child is gone.
    /// `Some(Terminal::Listening(addr))` is the child saying its listener is up —
    /// the moment a Windows launcher publishes the child's `state`, because the
    /// file's DACL is current-user-only and the confined child cannot write it
    /// itself.
    ///
    /// **A child that dies without reporting does not hold this call for the whole
    /// bound.** The relay's sender is dropped with the relay's `Observer`, so the
    /// channel disconnects and this answers `None` at the child's exit; the bound is
    /// for a child that is *still there* and simply has not reported — which the
    /// caller then hands to [`Self::wait`], whose answer is the status.
    ///
    /// **Reading it twice is not a second wait**, and that is the channel's
    /// property rather than a rule: the announcement is sent once and the receiver
    /// consumes it, so a second call answers what is left (nothing, once the first
    /// took it). A caller that needs the answer again reads [`Self::terminal`] after
    /// the wait, which is final.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{sync::mpsc, time::Duration};
    ///
    /// use tinio_sandbox_core::{Child, Terminal};
    ///
    /// let (tx, rx) = mpsc::channel();
    /// let child = Child::new(|| unreachable!("this child is never waited for")).with_ready(rx);
    /// let addr = "127.0.0.1:9000".parse().expect("addr");
    /// tx.send(Terminal::Listening(addr)).expect("send");
    /// assert_eq!(
    ///     child.wait_for_ready(Duration::from_secs(1)),
    ///     Some(Terminal::Listening(addr))
    /// );
    /// ```
    pub fn wait_for_ready(&self, bound: Duration) -> Option<Terminal> {
        // `ok()` folds the two failures into the one answer this API has: the bound elapsed, or
        // the relay ended without announcing — both are "nothing reported", and the caller's next
        // step is the same one either way.
        self.ready.as_ref()?.recv_timeout(bound).ok()
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
    ///
    /// # Examples
    ///
    /// **`no_run` because it spawns a process** (constitution III):
    ///
    /// ```no_run
    /// use std::process::Command;
    ///
    /// use tinio_sandbox_core::Child;
    ///
    /// let mut process = Command::new("true").spawn().unwrap();
    /// let mut child = Child::new(move || Ok(process.wait()?));
    /// // The child's own answer, which is what a launcher forwards as its own (design §3
    /// // step 8).
    /// assert_eq!(child.wait().expect("reaped").code(), Some(0));
    /// ```
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
    ///
    /// **[`Self::wait_for_ready`] is the other half, and the difference is the
    /// clock**: that one answers while the child is still running, for a caller
    /// that must act on readiness rather than only report on it afterwards. This
    /// one is the final word, and the only one that can say "it never reported".
    ///
    /// # Examples
    ///
    /// **`no_run` because it spawns a process** (constitution III):
    ///
    /// ```no_run
    /// use std::{process::Command, thread};
    ///
    /// use tinio_sandbox_core::{Child, Terminal};
    ///
    /// let mut process = Command::new("true").spawn().unwrap();
    /// let addr = "127.0.0.1:9000".parse().expect("addr");
    /// let relay = thread::spawn(move || Some(Terminal::Listening(addr)));
    /// let mut child = Child::new(move || Ok(process.wait()?)).with_relay(relay);
    /// child.wait().expect("the child is reaped");
    /// // The read is worth something only after the wait: by then the relay has been joined,
    /// // so this is the final answer a launcher frames a `None` on rather than a snapshot.
    /// assert!(child.terminal().is_some());
    /// ```
    pub fn terminal(&self) -> Option<&Terminal> {
        self.terminal.as_ref()
    }
}

/// Wait for the confined child, then undo the launch.
///
/// Teardown runs on every path — a clean exit, a failed child and a failed
/// wait alike — because a failed launch is exactly when the platform's own
/// teardown most needs to run: what a launcher set up before the child existed
/// is what has to come back down. **No ACE is reverted on any path**: every
/// grant the Windows launcher makes is standing (design decision 15), so a
/// failed launch leaves the ACEs it had already written for the next run on
/// that root to find rather than handing it a clean tree. The status returned
/// is always the **child's**; teardown cannot mask it.
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
/// gap.** A teardown that can fail belongs in the platform's wait closure, which
/// owns the platform's error type and can name the directory it could not fix
/// (design §11's Windows row); a `Child` that is never waited on at all is the
/// caller's to end, through [`Child::kill`] on the path that failed after the
/// child existed. What is left for this callback is the work that cannot fail
/// — disarming a signal handler, removing a path the launcher created — which is
/// what both launcher platforms pass it.
///
/// That the teardown ran is traced (`step = "teardown"`), because a teardown
/// that silently did not happen is indistinguishable from one that did.
///
/// # Errors
///
/// The child's wait failure, if it failed. A teardown that panics is not
/// caught: it is a bug in our own code, not a runtime condition.
///
/// # Examples
///
/// **`no_run` because it spawns a process** (constitution III): the example is compiled
/// but not executed.
///
/// ```no_run
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
        cell::RefCell,
        io,
        net::SocketAddr,
        process::{Child as ProcessChild, ChildStdout, Command, ExitStatus, Stdio},
        rc::Rc,
        sync::mpsc,
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

    /// Start a process that is still there a minute from now, and return it with
    /// the platform call that ends it.
    ///
    /// **The process is portable and the killer is not, which is the whole point
    /// of the split**: the child is `std::process`'s on both hosts — `sleep 60`, or
    /// `ping` against the loopback address for a minute, neither of which reads
    /// stdin — while the kill below is the platform's own.
    ///
    /// **The spawn takes a pipe rather than inheriting this process's stdin**, and
    /// on Windows that is not cosmetic: a child handed an inherited console handle
    /// can stall on it before its own work starts, which would make the kill below
    /// a race against a program that never ran. Both production launchers pipe
    /// their child's streams for their own reasons, so the piped shape is also the
    /// one a real `Child` has.
    fn lingering() -> Child {
        #[cfg(windows)]
        let script = "ping -n 60 127.0.0.1 > NUL";
        #[cfg(not(windows))]
        let script = "sleep 60";
        let process = exiting(script)
            .stdin(Stdio::piped())
            .spawn()
            .expect("spawn");
        // **The pinned process is shared because the kill and the wait are both
        // its callers and neither owns it.** `ProcessChild` owns the process, so a
        // killer that took it would leave the wait below with nothing to reap —
        // which is the `Clone`-free sharing `Rc` is for, and the same shape the
        // production launchers get from an `i32`.
        let process = Rc::new(RefCell::new(process));
        let waited = Rc::clone(&process);
        let killed = Rc::clone(&process);
        Child::new(move || Ok(waited.borrow_mut().wait()?))
            .with_kill(move || killed.borrow_mut().kill())
    }

    #[test]
    fn a_killed_child_is_reaped_and_reports_what_it_was() {
        // The launch-failure path, in the two calls the caller runs: `kill` ends a
        // child that is still serving, and the `wait` after it is what keeps that
        // from being a zombie — which is why the killer is forbidden to reap.
        let mut child = lingering();

        child.kill().expect("the platform's killer ends the child");
        let status = child.wait().expect("the wait still reaps the killed child");

        assert!(!status.success(), "a killed child did not exit cleanly");
        assert_eq!(
            child.terminal(),
            None,
            "the child was killed before it reported, so the relay recorded nothing"
        );
    }

    #[test]
    fn a_child_with_no_killer_refuses_rather_than_pretending() {
        // The `None` arm of `with_kill`, which a platform with its own lifetime tie
        // takes deliberately: the call must say that nothing was ended rather than
        // answer `Ok(())` to a caller that would then believe a process it cannot
        // reach was gone. The wait closure is never run, so this needs no process.
        let mut child = Child::new(|| unreachable!("this child is never waited for"));
        let err = child.kill().expect_err("no killer was installed");

        assert!(
            err.to_string().contains("no killer"),
            "the refusal says which half is missing: {err}"
        );
        assert_ne!(
            err.kind(),
            io::ErrorKind::NotFound,
            "a real killer's `NotFound` means the child had already gone, and a platform that \
             installed none must not answer as though one had: {err}"
        );
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
        // A failed child is exactly when the teardown most needs to run: what it
        // undoes is what the launcher set up — the profile and the job — and no
        // ACE is reverted on any path (every grant is standing, design decision 15).
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

    #[test]
    fn wait_for_ready_answers_the_announcement_and_the_absent_cases() {
        // The three answers this API has, in one test because they are one decision: a child whose
        // relay announced readiness, one nothing is waiting on, and one that announced nothing —
        // the first is `Some`, the other two are `None` rather than a wait on something that will
        // not come.
        let (tx, rx) = mpsc::channel();
        let announced = Child::new(|| unreachable!("never waited for")).with_ready(rx);
        tx.send(listening()).expect("announce");
        assert_eq!(
            announced.wait_for_ready(Duration::from_secs(1)),
            Some(listening())
        );

        let unwired = Child::new(|| unreachable!("never waited for"));
        assert_eq!(
            unwired.wait_for_ready(Duration::from_secs(1)),
            None,
            "a platform that wired no channel answers at once"
        );

        // The relay ended without announcing: the sender is gone with the observer, so this is
        // the disconnected arm rather than the bound elapsing.
        let (tx, rx) = mpsc::channel::<Terminal>();
        drop(tx);
        let quiet = Child::new(|| unreachable!("never waited for")).with_ready(rx);
        assert_eq!(quiet.wait_for_ready(Duration::from_secs(1)), None);
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
