//! Waiting for a confined child and undoing what its launch set up.

use std::{
    io,
    process::ExitStatus,
    sync::mpsc::{Receiver, RecvTimeoutError},
    thread::JoinHandle,
    time::Duration,
};

use tracing::{Span, debug, error, field, info, instrument};

use crate::{Attribution, error::Result, launcher::Terminal, metrics};

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
/// token and pid are the publisher's — needs the
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
/// ended rather than dropped.** The launcher publishes the child's `state` the
/// moment the child reports it is listening, and that publish is the one
/// fallible step left on the launch path: it writes into the state directory,
/// which can refuse between the launcher's pre-flight probe and the write. The
/// launcher's own `?` there would return the error and drop the [`Self`] — and a
/// dropped child is not an ended one, because the handle the platform keeps is
/// behind the wait closure, so the confined server keeps listening with no
/// `state` file for `tinio status`/`stop` to find it. [`Self::with_kill`] is that
/// path's half: the platform supplies the one call that ends the process it
/// started, and [`Self::abort`] is the caller's — **one call, on every launcher
/// platform**, which is what keeps the knowledge of *which* platform can end its
/// own child out of the caller's hands.
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

/// What waiting for a child's first frame ended on — **the three facts a single `Option` folds**,
/// and the caller's next step differs by which one it is.
///
/// A caller that only wants "did it report?" still reads [`Child::terminal`] after the wait, which
/// is final; this is for the caller that has to act *before* then, and whose action differs.
///
/// # Examples
///
/// ```
/// use std::{sync::mpsc, time::Duration};
///
/// use tinio_sandbox_core::{Child, Readiness};
///
/// let (_tx, rx) = mpsc::channel();
/// let child = Child::new(|| unreachable!("this child is never waited for")).with_ready(rx);
/// assert_eq!(
///     child.wait_for_ready(Duration::from_millis(1)),
///     Readiness::Elapsed
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    /// The child reported a terminal fact, in its own words.
    Reported(Terminal),
    /// **The relay ended without one**: the child is gone before it reported, so whatever it
    /// wrote and the exit status [`Child::wait`] reports are the run's report.
    Gone,
    /// **The bound elapsed with the child still there.** Nothing has been reported *yet*, and it
    /// is the caller's deadline — not the child — that has run out.
    Elapsed,
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
    /// genuinely be absent, and `Option` says so in the type** — but **every platform that
    /// launches a child installs one**, because [`Self::abort`] is what the launch-failure
    /// path calls on all of them and an `abort` with nothing to end would be a claim the
    /// platform cannot honour. Both launcher platforms therefore hand one over, and they
    /// are the mechanism each already had: a `SIGKILL` to the pid on macOS, and
    /// `TerminateJobObject` on the job the Windows launcher holds (the same object whose
    /// kill-on-close ties the child's lifetime to the launcher's, used here to end the child
    /// *now* rather than at the launcher's exit). What is left in the `None` arm is the
    /// core's own tests, and any future platform whose child genuinely cannot outlive the
    /// value — for which [`Self::abort`] states what it does instead.
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

    /// **End the child**, through the killer [`Self::with_kill`] installed.
    ///
    /// **This is the platform's half and not the failure path's whole**:
    /// [`Self::abort`] is the call a caller makes, and this is what it drives. The
    /// split is deliberate: the killer does the one thing a platform can express
    /// without owning the wait (a `SIGKILL` to a pid, a `TerminateJobObject` on the
    /// job), the wait is what **reaps** the process so no zombie is left and what
    /// joins the relay. So the sequence the failure path runs is `kill()` then
    /// `wait()` — the kill is what makes that wait return instead of blocking, and
    /// the wait is what makes it a reap rather than a leak.
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

    /// **End the child and reap it — the launch-failure path in one call, on every
    /// platform.**
    ///
    /// **The call site is one statement because the platform knowledge is not the caller's.** A
    /// launch that fails after its child exists — the `state` publish a launcher performs the
    /// moment the child reports it is listening, whose write can be refused between the
    /// launcher's pre-flight probe and the file — must end a child that would otherwise keep
    /// serving with nothing naming it. **How an ending is possible differs by platform, and that
    /// difference used to be a table the caller consulted before deciding whether to call
    /// [`Self::kill`] at all.** The table is gone: [`Self::with_kill`] is where each platform
    /// answers, this is where the answer is used, and `child.abort()` means the same thing on
    /// every launcher platform — a `SIGKILL` to the pid on macOS, `TerminateJobObject` on the job
    /// the Windows launcher holds.
    ///
    /// **Both halves are needed and the order is the contract**: the killer ends a child that is
    /// still serving, and the [`Self::wait`] after it is the reap — no zombie is left, the relay
    /// is joined, and [`Self::terminal`] is as final here as it is after a successful launch.
    ///
    /// **A child with no killer is not ended here, and the wait is skipped with it.** A platform
    /// that installs none is one whose child cannot outlive this value (see [`Self::with_kill`]),
    /// so there is nothing to end — and waiting for a process nothing has ended would hang the
    /// launcher instead of letting it report the failure it is carrying. That platform-shaped
    /// branch lives **here, in the core**, rather than at a call site that could not know it.
    ///
    /// **The errors are logged and not returned.** The caller is already carrying the failure
    /// that ended the launch, and that failure is what the operator has to see; a second error
    /// answered here would be a second answer for the same run. The logging is what the caller
    /// used to do around the pair.
    ///
    /// # Examples
    ///
    /// **`no_run` because it spawns a process** (constitution III):
    ///
    /// ```no_run
    /// use std::{cell::RefCell, process::Command, rc::Rc};
    ///
    /// use tinio_sandbox_core::Child;
    ///
    /// # #[cfg(unix)] {
    /// let process = Rc::new(RefCell::new(
    ///     Command::new("sleep").arg("60").spawn().unwrap(),
    /// ));
    /// let waited = Rc::clone(&process);
    /// let killed = Rc::clone(&process);
    /// let mut child = Child::new(move || Ok(waited.borrow_mut().wait()?))
    ///     .with_kill(move || killed.borrow_mut().kill());
    /// // The launch failed after this child existed: one call ends it and reaps it.
    /// child.abort();
    /// # }
    /// ```
    pub fn abort(&mut self) {
        // The platform's ending first, and the reap only when a killer was installed: a child
        // whose platform ties its lifetime elsewhere is not this value's to wait for.
        let ended = self.kill.as_mut().map(|kill| kill());
        match &ended {
            Some(Ok(())) => {}
            Some(Err(error)) => error!(
                step = "abort",
                error = %error,
                "the child of a failed launch could not be killed: it may still be serving, and \
                 the failure that ended the launch is what the operator sees"
            ),
            None => error!(
                step = "abort",
                "the platform installed no killer on this child, so nothing was ended here: its \
                 lifetime is tied to something this value does not own"
            ),
        }
        if ended.is_none() {
            return;
        }
        // The reap, whether or not the killer reported success: a killer that failed on the
        // platforms that install one means the process is already gone (`ESRCH`, a job handle
        // that no longer names it), and this wait is what reaps the status either way.
        match self.wait() {
            Ok(status) => info!(
                step = "abort",
                code = status.code(),
                "the child of a failed launch was killed and reaped"
            ),
            Err(error) => error!(step = "abort", error = %error, "the reaping wait failed"),
        }
    }

    /// The first terminal event the child reported, **while it is still running** —
    /// `None` when it reported none, and `None` once `bound` elapses.
    ///
    /// **The wait for a launcher that has to act before the child exits**, and the
    /// reason it is not [`Self::wait`] plus [`Self::terminal`]: the record is final
    /// only after the relay has been joined, which happens when the child is gone.
    /// `Some(Terminal::Listening(addr))` is the child saying its listener is up —
    /// the moment a Windows launcher publishes the child's `state`, whose token and
    /// pid are the publishing process's.
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
    /// use tinio_sandbox_core::{Child, Readiness, Terminal};
    ///
    /// let (tx, rx) = mpsc::channel();
    /// let child = Child::new(|| unreachable!("this child is never waited for")).with_ready(rx);
    /// let addr = "127.0.0.1:9000".parse().expect("addr");
    /// tx.send(Terminal::Listening(addr)).expect("send");
    /// assert_eq!(
    ///     child.wait_for_ready(Duration::from_secs(1)),
    ///     Readiness::Reported(Terminal::Listening(addr))
    /// );
    /// ```
    #[instrument(target = "tinio::sandbox", level = "info", skip_all)]
    pub fn wait_for_ready(&self, bound: Duration) -> Readiness {
        // **The three answers are read off the channel, not off a clock.** `recv_timeout` tells
        // the two failures apart itself — `Disconnected` is the relay ending, `Timeout` is the
        // bound running out — so a caller never has to time the call to recover the distinction,
        // which is what folding them into one `None` forced and what a `Disconnected` arriving
        // after the deadline got wrong.
        let readiness = match self.ready.as_ref() {
            // No channel was wired: nothing can ever be announced, so this is the relay-ended
            // answer rather than a wait on something that will not come.
            None => Readiness::Gone,
            Some(ready) => match ready.recv_timeout(bound) {
                Ok(terminal) => Readiness::Reported(terminal),
                Err(RecvTimeoutError::Disconnected) => Readiness::Gone,
                Err(RecvTimeoutError::Timeout) => Readiness::Elapsed,
            },
        };
        // Family 22: the answer is recorded where it is decided, so the value a caller reads and
        // the value the scrape reports are one — and it survives the launch's exit on a platform
        // whose launcher outlives the child.
        metrics::record_readiness(metrics::Ready::from(&readiness));
        readiness
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
    #[instrument(
        target = "tinio::sandbox",
        level = "info",
        skip_all,
        fields(outcome = field::Empty)
    )]
    pub fn wait(&mut self) -> Result<ExitStatus> {
        let status = (self.wait)();
        // Family 23: how the wait ended, recorded where the status arrives rather than after the
        // relay joins — an exit is the child's fact, and the join is this process's cleanup.
        let outcome = match &status {
            Ok(status) => metrics::Exit::of(status),
            Err(_) => metrics::Exit::Error,
        };
        metrics::record_exit(outcome);
        Span::current().record("outcome", field::display(outcome));
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

    /// What one exit says about this launch's confinement — **the launcher's own
    /// two facts read together**, because only a value that holds both can tell
    /// them apart.
    ///
    /// **The readiness half is [`Self::terminal`], not a parameter.** A caller that
    /// passed it in would be restating what this value already carries, and the one
    /// that hardcoded it would be pinning an arm no launch can produce: the two
    /// inputs are the exit status and *this* child's readiness, and both are here.
    ///
    /// **Ask it about an exit that went wrong, and the answer does not change what
    /// that exit is.** A child that reported and then exited successfully is a server
    /// that stopped — a run that worked — and [`Attribution::Ran`] is what this
    /// answers for it, exactly as for a refusal after readiness: nothing this process
    /// observed tells the two apart. What the answer is owed for is the exit a caller
    /// has to give its own caller words for, and only [`Attribution::Broken`] carries
    /// any.
    ///
    /// **Nothing here reads a refusal out of the status.** The child's stderr is the
    /// operator's, unmediated (decision 19), so a refusal after readiness and any
    /// other non-zero exit are one fact at this boundary — which is why a child that
    /// reported and then ended answers [`Attribution::Ran`], the arm that classifies
    /// nothing, rather than a denial whose subject this process never saw.
    ///
    /// # Examples
    ///
    /// **`no_run` because it spawns a process** (constitution III):
    ///
    /// ```no_run
    /// use std::{process::Command, thread};
    ///
    /// use tinio_sandbox_core::{Attribution, Child, Terminal};
    ///
    /// let mut process = Command::new("false").spawn().unwrap();
    /// let mut child = Child::new(move || Ok(process.wait()?));
    /// let status = child.wait().expect("the child is reaped");
    /// // The relay saw no announcement, so the confinement never applied.
    /// let attribution = child.attribution(&status);
    /// assert!(matches!(attribution, Attribution::Broken { .. }));
    ///
    /// // A relay that did announce is the other side of the decision: the child served
    /// // through the mechanism, and what ended it is nothing this side can classify.
    /// let mut process = Command::new("true").spawn().unwrap();
    /// let addr = "127.0.0.1:9000".parse().expect("addr");
    /// let relay = thread::spawn(move || Some(Terminal::Listening(addr)));
    /// let mut child = Child::new(move || Ok(process.wait()?)).with_relay(relay);
    /// let status = child.wait().expect("the child is reaped");
    /// assert!(matches!(child.attribution(&status), Attribution::Ran));
    /// ```
    #[instrument(target = "tinio::sandbox", level = "info", skip_all)]
    pub fn attribution(&self, status: &ExitStatus) -> Attribution {
        if self.terminal().is_none() {
            // **The mechanism never got the child as far as announcing a server**,
            // which is the one thing this process observed: the sentence is the
            // launcher's own, and the classification adds a value to it rather than
            // rewriting it.
            return Attribution::Broken {
                detail: format!("the confined child exited ({status}) without reporting readiness"),
            };
        }
        // **The child announced a server and then ended, and that is the other case
        // rather than a refusal**: the mechanism got it as far as serving, so nothing
        // here is broken, and a refusal after readiness is one fact with any other
        // non-zero exit at this boundary — the child's stderr is the operator's
        // (decision 19), so reading one out of the status would name a subject this
        // process never saw. The caller's rule that such a child gets no frame is one
        // match away rather than a second test of `terminal`.
        Attribution::Ran
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
/// caller's to end, through [`Child::abort`] on the path that failed after the
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

// **The stated allow D7 gives a test context clippy's in-tests keys cannot reach**
// (`docs/style.md` §Panics: the panic family is free in tests — this module is one — but
// `allow-unreachable-in-tests` does not exist, and clippy's three in-tests keys cover only
// `unwrap`/`expect`/`panic`). The six `unreachable!()` closures the fixtures below hand
// `Child::new` are each a `wait` arm a test deliberately never runs, and they are what the
// crate's new rows would otherwise fail the gate on. Scoped to the module rather than the
// file, so the production code above keeps the lint.
#[cfg(test)]
#[expect(
    clippy::unreachable,
    reason = "test context: `unreachable!` in the fixtures' never-run `wait` closures (docs/style.md §Panics, design 2026-10-09 D7)"
)]
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
        // `exec` so the shell *is* the sleeper: the kill below reaches the process the test
        // spawned, and no forked `sleep` outlives it (nextest reports such a survivor as a
        // leak, and a fixture that leaks is not testing what it says it is).
        #[cfg(not(windows))]
        let script = "exec sleep 60";
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
    fn abort_ends_the_child_and_reaps_it() {
        // **The launch-failure path in the one call a caller makes**, and the same two effects
        // `kill` and `wait` had when a call site spelled them for itself: the second wait below
        // returns a killed status rather than one the `sleep 60` would have taken a minute to
        // produce — which is the ending — and it returns at all, which is the reap.
        let mut child = lingering();

        child.abort();

        let status = child.wait().expect("the reaped status is readable again");
        assert!(!status.success(), "the child was killed, not exited");
        assert_eq!(child.terminal(), None, "the killed child never reported");
    }

    #[test]
    fn abort_without_a_killer_ends_nothing_and_does_not_wait() {
        // **The one arm a platform with its own lifetime tie takes**, and the reason `abort`
        // exists rather than the caller calling `kill` and `wait` itself: the wait closure is
        // `unreachable!`, so this test hangs (and then panics) if `abort` waits for a child that
        // nothing ended. The refusal is logged, which is all a caller already carrying the
        // launch failure can do with it.
        let mut child = Child::new(|| unreachable!("abort must not wait without a killer"));
        child.abort();
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
        // relay announced, one nothing is waiting on, and one that announced nothing — the first
        // is `Reported`, the second `Gone`, and only the last waits its bound out.
        let (tx, rx) = mpsc::channel();
        let announced = Child::new(|| unreachable!("never waited for")).with_ready(rx);
        tx.send(listening()).expect("announce");
        assert_eq!(
            announced.wait_for_ready(Duration::from_secs(1)),
            Readiness::Reported(listening())
        );

        let unwired = Child::new(|| unreachable!("never waited for"));
        assert_eq!(
            unwired.wait_for_ready(Duration::from_secs(1)),
            Readiness::Gone,
            "a platform that wired no channel answers at once"
        );

        // The relay ended without announcing: the sender is gone with the observer, so this is
        // the disconnected arm rather than the bound elapsing.
        let (tx, rx) = mpsc::channel::<Terminal>();
        drop(tx);
        let quiet = Child::new(|| unreachable!("never waited for")).with_ready(rx);
        assert_eq!(
            quiet.wait_for_ready(Duration::from_secs(1)),
            Readiness::Gone
        );

        // **The bound itself — the arm a caller used to have to time the call to recover.** A
        // channel that neither disconnects nor announces is the one case the clock was read for,
        // and it is the channel's own `Timeout` rather than an `Instant` comparison.
        let (_tx, rx) = mpsc::channel::<Terminal>();
        let silent = Child::new(|| unreachable!("never waited for")).with_ready(rx);
        assert_eq!(
            silent.wait_for_ready(Duration::from_millis(1)),
            Readiness::Elapsed
        );
    }

    #[test]
    fn an_expired_bound_reaches_the_abort() {
        // **The half of the bound the three-answer test above cannot show, and the one the e2e suite
        // cannot either (the plan's D8): a caller that reads `Elapsed` is holding a child that is
        // still there and silent, and what it does next is end it.** That caller is `tinio-cli`'s
        // `publish_state`, whose `Elapsed` arm returns the error `confine.rs` turns straight into
        // `Child::abort`; a launcher that only reported the bound would leave a server serving with
        // nothing naming it, which is exactly the state the `state` file exists to prevent.
        //
        // **The `lingering` fixture rather than the fakes above, because the abort has to be
        // observable.** Its child is a real process with a real killer, so the reap below is the
        // killed status and it arrives *now*; take the `abort` away and this test instead sits in
        // `wait` for the minute that child was started for and then reads the clean exit it produced
        // for itself.
        let (_tx, rx) = mpsc::channel::<Terminal>();
        let mut child = lingering().with_ready(rx);
        assert_eq!(
            child.wait_for_ready(Duration::from_millis(1)),
            Readiness::Elapsed,
            "the child is alive and silent: nothing announced and nothing disconnected, so the \
             clock is what answered rather than the child"
        );

        child.abort();

        let status = child.wait().expect("the reaped status is readable again");
        assert!(
            !status.success(),
            "the child the bound gave up on was killed, not left to exit on its own: {status}"
        );
        assert_eq!(
            child.terminal(),
            None,
            "the child was ended before it reported, so the relay recorded nothing"
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
