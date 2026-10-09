//! The guard that makes a test-spawned process die with the test process: a job object on Windows,
//! the parent-death signal on Linux, nothing on macOS.
//!
//! **The hole it closes is the one a `Drop` cannot reach.** Every harness in this tree kills and
//! reaps what it spawns — `kill_and_reap` is the one definer, and it is fine — but a process that
//! dies *without unwinding* runs no `Drop`: an abort, a panic inside a context that cannot unwind,
//! or a run killed from outside. Its children survive it, hold `target\<profile>\tinio.exe` open,
//! and the next `cargo build -p tinio` fails at the uplift step (`failed to remove …: access
//! denied`) while everything measured afterwards answers from a **stale binary** — which is what
//! one finished run did, leaving **19** `tinio server <temp root> --port 0` processes alive after
//! their parent had already died (`docs/superpowers/plans/2026-09-25-sandbox-followups.md` §13).
//!
//! **On Windows the mechanism is the product's launcher's**: a job object with
//! `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` (`crates/tinio-sandbox-windows/src/job.rs`, design
//! decision 20). **Both crates' jobs come from `crate::job_object`**, which is the one spelling of
//! the Win32 call sequence — create with the limits already set, hold, close on the last handle —
//! and what stays on this side is the policy: which limits this guard asks for, and the fact that
//! it is the guard's process and not a launched child that is put in the job.
//! The kernel closes the handle however the process dies, and the last handle to a
//! job going away terminates every process still in it. `JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION`
//! is set beside it, which the launcher's own job does not set: a member that would raise a crash
//! dialog dies instead, and a test run may not hang behind a dialog nobody is watching.
//!
//! **Who owns the job is what makes this a guard rather than a launcher.** The launcher creates
//! one job per launch and holds it for that child's lifetime; here there is **one job per test
//! process**, created at the first spawn and held in a `static` until the process ends — and **the
//! test process itself is a member of it** (`AssignProcessToJobObject(job,
//! GetCurrentProcess())`). Membership is inherited by everything created after that, because a new
//! process joins its parent's job and cannot leave it unless the job grants
//! `JOB_OBJECT_LIMIT_BREAKAWAY_OK` (this one does not) — so the launcher, the confined child the
//! launcher's own job ties to it, and a `--daemon` grandchild all die with the test process,
//! whichever way the test process dies.
//!
//! **Why adoption rather than a suspended start.** Both routes that put a child in a job *at its
//! creation* need something `std::process::Command` does not offer. `CREATE_SUSPENDED` +
//! `AssignProcessToJobObject` + `ResumeThread` needs the child's *thread* handle, and a
//! `std::process::Child` carries only the process handle (the Windows answer to "which thread did
//! `CreateProcessW` make?" is a Toolhelp snapshot walk — a second source of truth about a process
//! that has not started). `PROC_THREAD_ATTRIBUTE_JOB_LIST` needs a raw `CreateProcessW` with a
//! `STARTUPINFOEXW`, and the handles it returns cannot be turned into a `std::process::Child`: the
//! type has no constructor from them. Adoption needs neither, and once this process is a member the
//! window that route leaves is nothing rather than small — every child born afterwards is a member
//! from birth.
//!
//! **The one path that still has a window is the fallback, and it is loud.** If the assignment is
//! refused — a test process already inside a job that does not nest, which is a property of the
//! host rather than of this code — the guard warns once, through `tracing` (the paragraph on the
//! failure paths below), and [`spawn`] puts each child in the job *after* creating it. That is the launcher's own shape, and it carries the launcher's
//! own window: a process that dies between the two calls leaves that one child unowned. The
//! `Drop`-based reaping every harness already has stays in place either way — this is the backstop
//! for the case where no `Drop` runs, never a replacement for one.
//!
//! **On Linux the mechanism is the kernel's own, armed per child.** There is no job to join, so
//! [`spawn`] arms each child at its creation instead: `PR_SET_PDEATHSIG` with `SIGKILL`, set in
//! the child between `fork` and `exec`, so the kernel sends that child the signal when its parent
//! goes away — a kill, a panic, an abort or an orderly exit.
//!
//! **The signal is thread-scoped where the job is process-scoped, and that is the difference a
//! reader has to carry.** `PR_SET_PDEATHSIG` is delivered when the thread that created the child
//! terminates, not when the whole parent process does — measured on WSL2, kernel
//! `6.18.33.2-microsoft-standard`, 2026-09-28, with an armed `sleep` and `/proc/<pid>/stat`: a
//! child armed from a `thread::spawn` helper is dead the moment that helper returns, and one
//! armed from the test's own thread is untouched by a thread that merely exists and ends beside
//! it. **The arming has to be in place before the spawning thread ends** — a child that arms
//! itself after that moment is never signalled — and `std::process::Command::spawn` is what makes
//! the order safe here, because it waits for the child's `exec`, and therefore for the pre-exec
//! arming, before it returns.
//!
//! **What a libtest leg gets from that scoping is both the promise and the rule.** A worker
//! thread runs one test, so a child spawned from the test's own thread dies with the test; and a
//! **process** killed from outside takes every thread in it down, so the signal fires there as
//! well (measured: the same probe process `SIGKILL`ed with a keeper thread still alive — the
//! shape a killed `cargo test` has — left its armed child gone). **The rule for a harness is
//! therefore: spawn from the thread that lives as long as the child is needed, never from a
//! helper that returns.** Every spawn site in this tree does — a leg calls
//! `tinio_util::testing::PipedChild::spawn` itself, and the pump threads beside it only drain
//! pipes.
//!
//! **A grandchild is not covered, which is the second difference.** `fork` clears the
//! parent-death signal in the child (measured on WSL2, same date: `execve` preserves it, `fork`
//! clears it), so a `--daemon` server or a confined child the spawned process itself creates
//! outlives the test process exactly as it would with no guard at all. The Windows job covers
//! that descendant, because membership is inherited rather than armed — which is one thing the
//! job buys that this mechanism cannot.
//!
//! **macOS has no equivalent, and it is named here rather than left to a reader to discover.**
//! `prctl` is Linux's; what macOS does have (a process group, a `kqueue` watch on the parent's
//! exit) is a different design rather than a spelling of this one, and nothing here has measured
//! it. [`spawn`] is therefore `Command::spawn` and nothing more on macOS.
//!
//! **The crate's lint is `deny` so that this file can carry an allow**, on the site itself
//! (`docs/cargo.md`'s rule, and the sites list in the sandbox design's decision 14). What is left
//! here is raw platform API and nothing else in the crate is: the one `pre_exec` registration that
//! arms the Linux child, and the one `GetCurrentProcess` that puts *this* process in the guard's
//! job. **The job calls themselves and the handle wrapper moved to [`crate::job_object`]**, which
//! is the module a production build on Windows compiles; this module is the guard, and it stays
//! behind its own gate.
//!
//! **A refusal is a `warn!` rather than a silence, since a guard that is not there must not be
//! invisible.** `libtest` captures what a test writes to stderr, so an `eprintln!` here is a line
//! no test run shows unless someone passes `--nocapture` — which is how a suite can pass with no
//! guard at all. The creation and adoption failures therefore go through `tracing` (`warn`, at
//! this module's target), where a test device can capture and assert them; a reader of a red run
//! sees them under `--nocapture` like any other test output.
//!
//! **A member's own job nests inside this one, which is the situation the confined legs are in**:
//! the launcher creates a job for its child and assigns it, and the child is already a member here.
//! Nested jobs are Windows 8's, so the arrangement is one the platform supports rather than one
//! this guard works around; the two jobs' guarantees both hold, and this job's kill reaches the
//! nested child as well.
//!
//! **The capture recipe has a spelling of its own beside [`spawn`], and it is [`output`].** A site
//! that wants the child's whole output — an `icacls` read, a `pgrep`, this binary's own `--list` —
//! used to write the three stdio modes and `wait_with_output` out by hand at every such site, or
//! reach for `Command::output()` and lose the guard in the same line. [`output`] is that recipe
//! once, on the same guard, and neither half of the pair is a caller's to remember.
//!
//! **The one spelling is a gate, not a convention.** "Every harness spawns through [`spawn`]" is a
//! sentence anything can quietly falsify, so a check holds it up: `tests/spawn_guard.rs` scans the
//! test sources — every `*.rs` under `crates/*/tests/`, which is every workspace member's
//! integration suites, `support/`, `steps/` and harness modules — and fails on a `.spawn()` that is
//! not this function's, unless the gate's allowlist names the file with its reason. **The coverage
//! is the test sources and nothing else**: a `#[cfg(test)]` module inside `src/` is outside the
//! scan, and this module's own tests are the case that has to be — they spawn raw to prove the
//! adoption, which a child of [`spawn`] could not distinguish from an assignment after the fact.

#![allow(unsafe_code)]

use std::{
    io,
    process::{Child, Command, Output, Stdio},
};

/// Spawn `command` guarded — the one spelling a harness's spawn sites use.
///
/// **The one spelling, and the gate that makes it one**: a raw `.spawn()` under
/// `crates/*/tests/**` fails `tests/spawn_guard.rs` unless that gate's allowlist names the file
/// with its reason, which is the check this sentence is held up by rather than an assurance the
/// module doc asks a reader to take on faith.
///
/// **On Windows the child is created inside the guard's job**, because the guard put *this*
/// process in that job before the first child was spawned; see the module doc for the mechanism,
/// for the fallback when the guard cannot be created or cannot adopt this process, and for the
/// window that fallback keeps.
///
/// **On Linux the child is armed with the parent-death signal before it runs**, so the kernel
/// kills it when the thread that spawned it ends — the thread-scoped caveat, and the rule a
/// harness has to keep, are the module doc's.
///
/// **On macOS this is `Command::spawn` and nothing more.** There is no such call to make, the
/// hole the module doc names is open there, and nothing here closes it.
///
/// # Errors
///
/// The spawn's own error — and, on Windows, **the assignment's**: a child that cannot be put in
/// the job is killed and that error is returned instead of the child, because a server outside the
/// job is exactly the orphan this guard exists to prevent. A harness that cannot be guarded fails
/// where it spawned rather than leaking later. On Linux the arming's failure is the spawn's: a
/// `prctl` that will not arm the child fails the call rather than handing back a child nobody
/// guards.
pub fn spawn(command: &mut Command) -> io::Result<Child> {
    #[cfg(windows)]
    {
        match guard() {
            Some(job) => job.spawn(command),
            // The guard could not be created, and it has already warned. The spawn still happens:
            // a test that cannot be guarded is a test that runs with the `Drop`-based reaping the
            // harnesses already have, not a test that does not run.
            None => command.spawn(),
        }
    }
    #[cfg(target_os = "linux")]
    {
        arm_parent_death_signal(command);
        command.spawn()
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        command.spawn()
    }
}

/// Spawn `command` guarded and collect its whole output — the one spelling a site that wants a
/// child's captured output uses.
///
/// **The three stdio modes are set here rather than by the caller**, which is the rule
/// `tinio_util::testing::PipedChild::spawn` states for the two pipes it sets: the capture is this
/// function's contract, so a caller that forgot `Stdio::piped()` would get a child whose output
/// nobody reads — an empty `stdout` that reads as "the tool said nothing" — and a caller that
/// forgot `Stdio::null()` one that waits on a stdin no test will ever write. Null stdin, a pipe
/// on each of the other two, and the whole of it waited for: the shape `Command::output()` has,
/// which is why the sites this replaces were spelled that way.
///
/// **The spawn is [`spawn`]'s, so the guard still holds**: the child is a member of the job on
/// Windows and armed with the parent-death signal on Linux, where `Command::output()` was the
/// raw spawn and the wait with neither. **It is not a general replacement for
/// `Command::output()`**: a site whose child must inherit a stream or write to a file sets its
/// own stdio and calls [`spawn`] — this function sets all three modes itself, and would
/// overwrite them.
///
/// # Errors
///
/// [`spawn`]'s errors — the spawn's own, and on Windows the assignment's — then the wait's.
pub fn output(command: &mut Command) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    spawn(command)?.wait_with_output()
}

/// Arm the child `command` will create with Linux's parent-death signal, so the kernel sends it
/// `SIGKILL` when the thread spawning it ends.
///
/// **Set in the child, between `fork` and `exec`**, which is the only window `std` opens for it:
/// `pre_exec`'s closure runs there, and `PR_SET_PDEATHSIG` is a property of the process that
/// calls it — arming it here rather than in the parent is what makes it the child's own.
///
/// **The closure holds one call on purpose.** A process that has threads gives its forked child a
/// snapshot of *their* state, so only async-signal-safe work is allowed between `fork` and `exec`;
/// `prctl` is, and the rest of this module is not. **The one window the mechanism keeps** is the
/// gap between the fork and this call: a parent thread that dies inside it leaves the child
/// unarmed, which is the same shape as the Windows fallback's window and the same answer — the
/// `Drop`-based reaping every harness already has still runs.
#[cfg(target_os = "linux")]
fn arm_parent_death_signal(command: &mut Command) {
    // SAFETY: `pre_exec`'s contract is that the closure is async-signal-safe, because it runs in
    // the child of a possibly multi-threaded process where an allocation or a lock may be held by
    // a thread that does not exist there. This closure calls `prctl` and nothing else: no
    // allocation, no lock, no other thread's state — one raw syscall whose argument is a constant.
    unsafe {
        command.pre_exec(|| {
            set_parent_process_death_signal(Some(Signal::KILL)).map_err(io::Error::from)
        });
    }
}

/// The process-wide guard, created on the first call and held for the process's lifetime — or
/// `None` when this host would not give one, which is warned about once.
///
/// **Held in a `static`, and that lifetime is the guarantee.** A `GuardJob` that is dropped closes its
/// handle, and closing the last handle to a job with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` is what
/// terminates its members — so the handle must outlive every child, including the ones this process
/// leaves behind when it dies. Statics are never dropped, so the kernel closes this one as the
/// process goes away, which is the moment the job must fire.
///
/// **`Some` does not promise that this process is a member.** It promises a job; whether the
/// adoption succeeded is warned about separately because it changes only *which* children are
/// covered, not whether the guard exists.
#[cfg(windows)]
pub fn guard() -> Option<&'static GuardJob> {
    GUARD.get_or_init(create_guard).as_ref()
}

/// The one guard this process gets, or the reason it has none.
#[cfg(windows)]
static GUARD: OnceLock<Option<GuardJob>> = OnceLock::new();

/// Create the job and take this process into it, warning about either refusal once — the whole
/// once-per-process body of [`guard`].
#[cfg(windows)]
fn create_guard() -> Option<GuardJob> {
    create_guard_with(GuardJob::create)
}

/// The body above, with the job's construction as a parameter — the seam the guard's own test
/// induces a failure through.
///
/// **A creation failure is not something a host can be asked for on demand** (`CreateJobObjectW`
/// answers a job or the machine is out of resources), so the failure arm is reachable from a test
/// only if the call it fails in is a parameter. Passing `GuardJob::create` at the call site above
/// keeps the production route the same one the test exercises.
#[cfg(windows)]
fn create_guard_with(create: impl FnOnce() -> io::Result<GuardJob>) -> Option<GuardJob> {
    let job = match create() {
        Ok(job) => job,
        Err(error) => {
            // `warn` and not `eprintln!`: a test process's stderr is captured by `libtest`, so a
            // line written there is one no green run ever shows — a suite could pass with no guard
            // at all and say nothing. Through `tracing` the same fact is a test device's to
            // capture and assert, and `--nocapture` still prints it for a reader debugging the
            // guard.
            tracing::warn!(
                %error,
                "no job guard for this test process — servers spawned here are reaped by `Drop` \
                 alone, and a process that dies without unwinding leaves them"
            );
            return None;
        }
    };
    if let Err(error) = job.adopt_current_process() {
        // The job exists and stays: every child [`spawn`] creates is put in it explicitly. What is
        // lost is what adoption alone buys — the children born to *other* code, and the windowless
        // start.
        tracing::warn!(
            %error,
            "this process could not join its job guard — the job stays, and every child spawned \
             through `tinio_util::job::spawn` is put in it, but a child this process spawns any \
             other way is unguarded"
        );
    }
    Some(job)
}

/// The **test-side** job object: one with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` and
/// `JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION` set, held as the guard.
///
/// **Holding it is the whole guarantee**: the kernel terminates every process in the job when its
/// last handle closes, whichever way that closing came about. See the module doc for the guard
/// this backs and for the one the product's launcher creates per launch.
///
/// **The name is the point, because the product has a `Job` of its own.** The launcher's
/// (`tinio_sandbox_windows::job::Job`) is created per launch and is what ties one confined child
/// to its launcher; this one is created once per test process and is what ties every child spawned
/// through [`spawn`] to that process. Two lifetimes, two owners and two jobs — a reader who
/// confuses them would look for a per-launch lifetime here and not find one. **The third name in
/// the family is [`JobObject`]**, which is the primitive both of them are made of: it owns the
/// handle and its `Drop` is the close site, so a reader looking for where the job closes should
/// look there.
#[cfg(windows)]
#[derive(Debug)]
pub struct GuardJob {
    object: JobObject,
}

#[cfg(windows)]
impl GuardJob {
    /// Create a job with both limits set.
    ///
    /// The limits are set before the handle is handed back, so no caller can hold a job that does
    /// not kill: a failure to set them drops the job — closing the handle — rather than returning
    /// one whose guarantee is missing. **The sequence itself is [`JobObject::create`]'s**, which is
    /// where that invariant is kept and argued; what is here is the policy — the two limits this
    /// guard asks for, and the fact that the guard is the owner.
    ///
    /// # Errors
    ///
    /// The failing call's `io::Error` (`CreateJobObjectW`, then `SetInformationJobObject`), with
    /// the failing call's own name dropped: this module's answer has always been the bare OS error.
    pub fn create() -> io::Result<Self> {
        let object = JobObject::create(
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION,
        )
        .map_err(|failure| failure.source)?;
        Ok(Self { object })
    }

    /// Spawn `command` and make sure the child is in this job, or kill it and answer the error.
    ///
    /// **The usual path does nothing but read.** Once this process is a member, the child is a
    /// member from birth, so the membership read answers true and no assignment happens. The
    /// assignment is the fallback the module doc names, taken when the membership read says the
    /// child is not in the job — because this process never joined it — and it is what makes the
    /// guard work on a host that refused the adoption.
    ///
    /// # Errors
    ///
    /// The spawn's own error, or the assignment's. **The child is killed and reaped before the
    /// assignment's error is returned**: a process running outside the job is the orphan this
    /// module exists to prevent, and it is not returned to a caller that could forget it.
    pub fn spawn(&self, command: &mut Command) -> io::Result<Child> {
        let mut child = command.spawn()?;
        if self.contains(&child).unwrap_or(false) {
            return Ok(child);
        }
        if let Err(error) = self.object.assign(child.as_raw_handle()) {
            kill_and_reap(&mut child);
            return Err(error);
        }
        Ok(child)
    }

    /// Whether `child` is in this job.
    ///
    /// **The job's own answer and not a bookkeeping of this module's**: `IsProcessInJob` asks the
    /// kernel, so a child that reached the job by inheritance — the usual case — is answered for
    /// exactly as one that was assigned. The call is [`JobObject::contains`]'s, where the reading
    /// lives now that the product's launcher shares the primitive.
    ///
    /// # Errors
    ///
    /// The read's `io::Error`.
    pub fn contains(&self, child: &Child) -> io::Result<bool> {
        self.object.contains(child)
    }

    /// How many processes the job holds right now — the membership a *descendant* reached, which
    /// no handle of this process's names.
    ///
    /// **A leg's reading rather than a product one**: the only question this answers that
    /// [`Self::contains`] does not is "did the grandchild arrive", and the product path never asks
    /// it — the job holds whatever the child spawned, and the kill is what acts on it.
    ///
    /// # Errors
    ///
    /// The query's `io::Error`.
    #[cfg(test)]
    pub fn active_processes(&self) -> io::Result<u32> {
        self.object.active_processes()
    }

    /// Put this process itself in the job — the call that makes every child born afterwards a
    /// member before it can run.
    ///
    /// **The job takes no ownership of the process and the process cannot leave**: an assignment is
    /// one-way, and it is the point rather than a side effect.
    fn adopt_current_process(&self) -> io::Result<()> {
        // SAFETY: `GetCurrentProcess` answers a pseudo-handle, valid for the duration of the call
        // and never to be closed.
        let current = unsafe { GetCurrentProcess() };
        self.object.assign(current)
    }
}

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
#[cfg(windows)]
use std::{os::windows::io::AsRawHandle, sync::OnceLock};

#[cfg(target_os = "linux")]
use rustix::process::{Signal, set_parent_process_death_signal};
#[cfg(windows)]
use windows_sys::Win32::System::{
    JobObjects::{JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE},
    Threading::GetCurrentProcess,
};

#[cfg(windows)]
use crate::{job_object::JobObject, process::kill_and_reap};

#[cfg(all(test, windows))]
mod tests {
    //! The guard's own proof, on the host that runs it.
    //!
    //! **These tests are the reason the module is not behind the `testing` feature**: they must run
    //! in the default graph (`cargo test -p tinio-util`, which is what the Windows CI leg runs),
    //! and a feature-gated module's tests run nowhere unless someone asks for the feature.
    //!
    //! **The one order they are not independent of**: the guard is per process, so the test that
    //! calls [`guard`] takes the *test binary* into a job — and a job created afterwards is nested
    //! inside it. That is exactly the situation the confined legs are in (the launcher's job inside
    //! this one), and `a_job_created_after_the_guard_nests_inside_it` below asserts it deliberately
    //! rather than leaving it to whichever order the runner picked.
    use std::{
        fmt, io,
        process::{Command, Stdio},
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };

    use tracing::{
        Event, Metadata, Subscriber,
        field::{Field, Visit},
        span,
        subscriber::with_default,
    };

    use super::{GuardJob, create_guard_with, guard, spawn};
    use crate::process::{kill_and_reap, poll_exit};

    /// The bound every wait here is written against: the member is a `ping` sized to run **six
    /// times longer** than the bound, so nothing below can pass by the child finishing on its own.
    const WITHIN: Duration = Duration::from_secs(10);

    /// `cmd.exe` running `ping` for a minute, with both streams nulled.
    ///
    /// **Two members, not one**: `cmd.exe` starts `ping.exe` and waits for it, which is what makes
    /// a descendant's membership observable through the job's own count.
    fn a_long_running_member() -> Command {
        let mut command = Command::new("cmd.exe");
        command
            .args(["/c", "ping -n 60 127.0.0.1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    /// The sync, bounded poll on a condition these tests need. `testing::wait_for` is the async one
    /// and lives behind a feature this module's tests deliberately do not require, and
    /// [`poll_exit`] waits on a child rather than on a condition.
    fn poll_until(within: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + within;
        loop {
            if cond() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// The events one test dispatches, as `LEVEL target fields` — the device the warning test
    /// asserts through.
    ///
    /// **It is here rather than taken from `tinio_util::testing`** because that recorder is behind
    /// the `testing` feature this module's tests deliberately do not require, and because the one
    /// thing being asserted is a level and a message rather than a recording: `enabled` answers
    /// `true` for everything, and only [`event`](Subscriber::event) is implemented — the guard
    /// emits no spans, so the four span methods are the trait's own no-ops made explicit.
    #[derive(Clone, Default)]
    struct Events(Arc<Mutex<Vec<String>>>);

    impl Events {
        /// Everything dispatched through this subscriber, in order.
        fn dispatched(&self) -> Vec<String> {
            self.0.lock().expect("the events").clone()
        }
    }

    impl Subscriber for Events {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            // Never reached: the module under test opens no span, and an id is all the signature
            // requires.
            span::Id::from_u64(1)
        }

        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            self.0.lock().expect("the events").push(format!(
                "{} {} {}",
                event.metadata().level(),
                event.metadata().target(),
                fields.0.trim_end()
            ));
        }

        fn enter(&self, _: &span::Id) {}

        fn exit(&self, _: &span::Id) {}
    }

    /// The fields of one event, rendered `name=value` and space-separated — the visitor
    /// [`Events::event`] records through.
    struct Fields(String);

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            use fmt::Write as _;

            let _ = write!(self.0, "{}={value:?} ", field.name());
        }
    }

    /// A guard that cannot be created **says so where a test can hear it**, and answers `None`: the
    /// failure arm of [`create_guard_with`], taken through the seam that exists for it.
    ///
    /// **The warning is the subject, not the `None` beside it.** An `eprintln!` reaches a test run
    /// only under `--nocapture`, so a suite could run with no guard at all and report nothing —
    /// which is the state this test exists to keep impossible. `Dispatch::new` rebuilds the
    /// callsite cache when `with_default` installs the subscriber, so a `warn!` this process never
    /// evaluated before is dispatched to it.
    #[test]
    fn a_job_that_cannot_be_created_warns_and_answers_no_guard() {
        let events = Events::default();
        let refusal = io::Error::new(io::ErrorKind::PermissionDenied, "an induced refusal");
        let guard = with_default(events.clone(), || create_guard_with(|| Err(refusal)));
        assert!(
            guard.is_none(),
            "a job that cannot be created answers no guard, which is the fallback every spawn \
             still runs through"
        );
        let dispatched = events.dispatched();
        assert_eq!(
            dispatched.len(),
            1,
            "one refusal, one warning: {dispatched:?}"
        );
        let warning = &dispatched[0];
        assert!(
            warning.starts_with("WARN tinio_util::job "),
            "the warning is a `warn` at this module's own target: {warning}"
        );
        assert!(
            warning.contains("no job guard for this test process"),
            "it names what is missing: {warning}"
        );
        assert!(
            warning.contains("an induced refusal"),
            "and it carries the call's error text: {warning}"
        );
    }

    /// The guard's claim, in one test: this process is in its job, so everything it creates is too —
    /// whether it was spawned through [`spawn`] or not.
    #[test]
    fn the_guard_holds_this_process_and_everything_spawned_after_it() {
        let job = guard().expect("the guard creates a job");
        // A child spawned *without* the helper, which is the only shape that separates "the guard
        // adopted this process" from "the helper assigned this child": membership here can only
        // have been inherited, so this assertion is the adoption's.
        let mut plain = a_long_running_member()
            .spawn()
            .expect("spawn a member without the helper");
        assert!(
            job.contains(&plain).expect("read membership"),
            "a child of an adopted process is a member from birth — if this fails, \
             `AssignProcessToJobObject` was refused for this process and the guard is in its \
             per-child fallback"
        );
        kill_and_reap(&mut plain);

        let mut guarded = spawn(&mut a_long_running_member()).expect("spawn through the guard");
        assert!(
            job.contains(&guarded).expect("read membership"),
            "the child `spawn` was handed is a member of the guard's job"
        );
        kill_and_reap(&mut guarded);
    }

    /// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, on a job this test owns: closing its last handle
    /// terminates the member, which is the guarantee a test process that dies without unwinding
    /// leaves the kernel to exercise.
    #[test]
    fn closing_the_last_handle_to_the_job_kills_its_members() {
        let job = GuardJob::create().expect("create a job");
        let mut child = job
            .spawn(&mut a_long_running_member())
            .expect("spawn a member");
        assert!(
            job.contains(&child).expect("read membership"),
            "a child spawned through a job is a member of it"
        );
        assert!(
            child.try_wait().expect("poll the member").is_none(),
            "the member is still running before the job closes, so its exit below is the job's"
        );
        drop(job);
        assert!(
            poll_exit(&mut child, WITHIN).is_some(),
            "the member is gone within {WITHIN:?} of the last handle closing — a `ping` that runs \
             for a minute cannot have exited on its own"
        );
    }

    /// Membership is inherited, which is what makes one assignment cover a whole tree: the job
    /// counts a member's own child, and no handle in this process names it.
    #[test]
    fn a_members_own_child_is_a_member_too() {
        let job = GuardJob::create().expect("create a job");
        let mut child = job
            .spawn(&mut a_long_running_member())
            .expect("spawn a member");
        assert!(
            poll_until(WITHIN, || job
                .active_processes()
                .expect("read the job's count")
                >= 2),
            "the job holds `cmd.exe` and the `ping.exe` it started, which only inheritance can \
             have put there"
        );
        drop(job);
        assert!(
            poll_exit(&mut child, WITHIN).is_some(),
            "the job's close takes both members, not only the one that was named to it"
        );
    }

    /// The control the two tests above need: `contains` answers about the job asked for, so a
    /// process outside it is not counted as a member.
    #[test]
    fn a_job_holds_what_was_put_in_it_and_nothing_else() {
        let job = GuardJob::create().expect("create a job");
        let mut child = a_long_running_member()
            .spawn()
            .expect("spawn outside the job");
        assert!(
            !job.contains(&child).expect("read membership"),
            "a fresh job does not hold a process nobody put in it"
        );
        kill_and_reap(&mut child);
    }

    /// The launcher's own arrangement, which this guard must not break: a job created by a process
    /// that is already in one is **nested**, and both jobs' guarantees hold for a member of the
    /// inner one.
    ///
    /// **This is the confined legs' shape** — `tinio server --sandbox` creates a job for its child
    /// and assigns it, and that child is a member here from birth — so the assertion is about a
    /// product interaction rather than about Windows alone, and it is deliberate: an incidental one
    /// would pass or fail on which order the runner picked.
    #[test]
    fn a_job_created_after_the_guard_nests_inside_it() {
        let guard_job = guard().expect("the guard creates a job and adopts this process");
        let job = GuardJob::create().expect("create a second job");
        let mut child = job
            .spawn(&mut a_long_running_member())
            .expect("spawn a member of the inner job");
        assert!(
            job.contains(&child).expect("read membership"),
            "the member is in the job it was spawned into"
        );
        assert!(
            guard_job.contains(&child).expect("read membership"),
            "and it is in the guard's job too, which it inherited before the inner assignment — \
             a host that refused the nesting would have failed the assignment above"
        );
        drop(job);
        assert!(
            poll_exit(&mut child, WITHIN).is_some(),
            "the inner job's close takes the member, which is what the launcher relies on when its \
             own job closes"
        );
    }
}

/// The Linux arm's proof: the child is armed, the arming is per *thread* rather than per process,
/// and a parent that dies without unwinding takes its child with it.
///
/// **The middle claim is the caveat the module doc states, asserted rather than assumed**: it is
/// the one place this target's mechanism is weaker than the Windows job, so a run that stopped
/// distinguishing them would be a run whose guard kills a live server the moment a helper thread
/// returns.
#[cfg(all(test, target_os = "linux"))]
mod linux_tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process::{Command, Stdio, abort, id},
        thread,
        time::{Duration, Instant},
    };

    use super::spawn;
    use crate::process::{kill_and_reap, poll_exit};

    /// The bound every wait here is written against: the child is a `sleep` sized to run **six
    /// times longer** than the bound, so nothing below can pass by the child finishing on its own.
    const WITHIN: Duration = Duration::from_secs(10);

    /// The environment variable the re-executed helper reads its pid file from. **A name of this
    /// module's own**: `libtest` inherits the environment, so a variable another leg set must not
    /// be able to arm this probe.
    const PID_FILE_VAR: &str = "TINIO_UTIL_JOB_PROBE_PID_FILE";

    /// `sleep` for a minute, with both streams nulled — the armed child these tests measure.
    fn a_long_running_child() -> Command {
        let mut command = Command::new("sleep");
        command
            .arg("60")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    /// A pid file path no other test or run shares.
    fn pid_file() -> PathBuf {
        env::temp_dir().join(format!("tinio-util-job-probe-{}.pid", id()))
    }

    /// A child spawned from *this* thread is not touched by another thread's exit: the signal is
    /// the spawning thread's, and the test's own thread is still here.
    #[test]
    fn a_child_armed_on_this_thread_survives_the_other_threads() {
        let mut child = spawn(&mut a_long_running_child()).expect("spawn through the guard");
        thread::spawn(|| {})
            .join()
            .expect("a thread that comes and goes");
        assert!(
            child.try_wait().expect("poll the child").is_none(),
            "a child armed on this thread is untouched by a thread that merely existed beside it"
        );
        kill_and_reap(&mut child);
    }

    /// The other edge, which is the caveat: a child armed from a helper thread dies when that
    /// helper ends, because the kernel ties the signal to the thread that created the child rather
    /// than to the process. **This is the shape a harness must not use**, and the test is what
    /// keeps the limitation a measured fact rather than a sentence in a doc comment.
    #[test]
    fn a_child_spawned_from_a_helper_thread_dies_with_that_thread() {
        let mut command = a_long_running_child();
        let mut child =
            thread::spawn(move || spawn(&mut command).expect("spawn through the guard"))
                .join()
                .expect("the helper thread");
        assert!(
            poll_exit(&mut child, WITHIN).is_some(),
            "the helper thread ended, so the kernel sent its armed child `SIGKILL` within \
             {WITHIN:?} — a `sleep` sized to outlive this test cannot have exited on its own"
        );
    }

    /// The claim the whole module exists for, on this target: **a parent that dies without
    /// unwinding takes its armed child with it.**
    ///
    /// **The parent has to be another process**, because a test cannot kill its own, so this one
    /// re-executes the test binary and runs [`the_armed_child_probe`] in it: the probe arms a
    /// child through [`spawn`], writes its pid where this test can read it, and aborts — a death
    /// that runs no `Drop` anywhere. The pid file is the handshake; the assertion is that the
    /// process it names is gone. A leftover is killed before the failure is reported, so a
    /// regressed mechanism reddens the suite without leaking the server it failed to guard.
    #[test]
    fn a_process_that_dies_without_unwinding_takes_its_armed_child_with_it() {
        let pid_file = pid_file();
        let _ = fs::remove_file(&pid_file);
        let status = Command::new(env::current_exe().expect("this test binary"))
            // `libtest`'s filter takes the probe by its name alone, which is unique in this
            // binary — the whole module path is not spelled here, because it is not what the
            // filter matches and a `--exact` mistake would run no test at all, leaving a run that
            // answers success while asserting nothing.
            .args(["the_armed_child_probe", "--nocapture"])
            .env(PID_FILE_VAR, &pid_file)
            .status()
            .expect("run the probe in a process of its own");
        assert!(
            !status.success(),
            "the probe aborts, so its run cannot answer success: {status}"
        );
        let pid: u32 = fs::read_to_string(&pid_file)
            .expect("the probe wrote its child's pid before it died")
            .trim()
            .parse()
            .expect("the pid file holds a pid");
        let alive = |pid: u32| Path::new("/proc").join(pid.to_string()).exists();
        let deadline = Instant::now() + WITHIN;
        while alive(pid) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        if alive(pid) {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
        assert!(
            !alive(pid),
            "the armed child (pid {pid}) is gone within {WITHIN:?} of its parent's abort — \
             `PR_SET_PDEATHSIG` is delivered when the parent dies, whichever way it dies"
        );
        let _ = fs::remove_file(&pid_file);
    }

    /// The process the test above re-executes: **not a test of its own** when [`PID_FILE_VAR`] is
    /// unset, and a no-op in an ordinary run then. When it is set, this arms one child, leaves its
    /// pid behind, and aborts — the shape a killed run has, without needing the kill.
    #[test]
    fn the_armed_child_probe() {
        let Ok(pid_file) = env::var(PID_FILE_VAR) else {
            return;
        };
        let child = spawn(&mut a_long_running_child()).expect("spawn the armed child");
        fs::write(&pid_file, child.id().to_string()).expect("the pid file");
        abort();
    }
}
