//! Windows backend: whitelist, container, relay, and the supervision handed back.
//!
//! `Backend::confine` runs four stages: `prepare_launch`, `grant_whitelist`,
//! `spawn_supervised_child`, and `supervise_or_rollback`. Capability names live
//! in `capability.rs`; exposure policy in `exposures.rs`. Re-exports below keep
//! the old `backend::` paths. Submodules are private.
//!
//! `canonical_caller`, `shape_for`, and `CallerSet` are `pub(super)` so `confine`
//! and `backend::tests` can reach them. `CallerSet` is not `pub`: a raw spelling
//! must not be usable as the resolved one. This module makes no Win32 call, so
//! it is not an `unsafe` site (decision 14).

mod capability;
mod exposures;
#[cfg(test)]
mod tests;

use std::{
    env,
    ffi::OsString,
    fs, io,
    net::TcpListener,
    path::PathBuf,
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use tokio::{fs as tokio_fs, io as tokio_io, runtime::Builder};

use self::exposures::{CallerSet, canonical_caller, shape_for};
pub use self::{
    capability::Capability,
    exposures::{
        SiblingScan, canonical_exposures, describe, exposures, merge, served_root, sibling_dlls,
        with_siblings,
    },
};
use crate::{
    _core::{
        AccessMode, Backend as BackendTrait, Child, Confinement, Exposure, LaunchPayload,
        LaunchPlan, Observer, Result, Sandbox, Terminal, launch,
    },
    acl::Grants,
    control, handles,
    job::Job,
    launch::command_args,
    process::{self, Container, LaunchSpec, LaunchedProcess},
    sid::{CapabilitySids, PackageSid, package_name},
    socket,
};

/// What the launcher needs beyond the [`Sandbox`] and the [`LaunchPlan`].
///
/// [`LaunchPlan`]: crate::_core::LaunchPlan
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::backend::{Capability, Options};
///
/// // The defaults are the confinement the operator gets: no capability, the
/// // derived package name, this process's own executable, and its own arguments.
/// let mut options = Options::default();
/// assert!(options.capabilities.is_empty());
/// assert!(options.program.is_none());
///
/// options
///     .capabilities
///     .push(Capability::PrivateNetworkClientServer);
/// assert_eq!(options.capabilities.len(), 1);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The container's capabilities; empty by default (design §7).
    pub capabilities: Vec<Capability>,
    /// Override the program the launcher re-invokes. `None` means this process's
    /// own executable, which is what production uses (design §10 step 6).
    ///
    /// A test seam with one job: a launch that fails *after* the ACLs are on is
    /// the only way to exercise design §11's failed-launch rollback.
    pub program: Option<PathBuf>,
    /// Override the child's arguments. `None` means the launcher's own, minus
    /// `argv[0]`, with the payload element appended (design §3 step 1).
    ///
    /// A test seam: a test binary's own argv belongs to libtest, so the harness
    /// points its child at one role function instead of at the suite.
    pub args: Option<Vec<OsString>>,
}

/// The Windows backend.
///
/// P0 shipped this type as a stub whose `confine` returned
/// [`Error::Unsupported`]; P2 fills it in.
///
/// [`Error::Unsupported`]: crate::_core::Error::Unsupported
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::Backend;
///
/// let backend = Backend::default();
/// let _ = backend;
/// ```
#[derive(Debug, Clone, Default)]
pub struct Backend {
    options: Options,
}

impl Backend {
    /// A backend with the given options.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::backend::{Backend, Capability, Options};
    ///
    /// let backend = Backend::with_options(Options {
    ///     capabilities: vec![Capability::InternetClient],
    ///     ..Default::default()
    /// });
    /// assert_eq!(
    ///     backend.options().capabilities,
    ///     vec![Capability::InternetClient]
    /// );
    /// ```
    pub fn with_options(options: Options) -> Self {
        Self { options }
    }

    /// The options it was built with.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// use tinio_sandbox_windows::backend::{Backend, Options};
    ///
    /// let backend = Backend::with_options(Options {
    ///     program: Some(PathBuf::from(r"C:\srv\tinio\serve.exe")),
    ///     ..Default::default()
    /// });
    /// assert_eq!(
    ///     backend.options().program.as_deref(),
    ///     Some(Path::new(r"C:\srv\tinio\serve.exe"))
    /// );
    /// ```
    pub fn options(&self) -> &Options {
        &self.options
    }
}

impl BackendTrait for Backend {
    /// Launch an AppContainer child and hand it back for supervision.
    ///
    /// The stages are `prepare_launch`, `grant_whitelist`,
    /// `spawn_supervised_child`, and `supervise_or_rollback`.
    /// [`crate::acl::Grants`] moves into the wait closure only on success.
    /// Every other path drops it, which is the rollback (decision 15). The
    /// profile dies with the last `Arc<PackageSid>` those grants hold, after
    /// the child.
    ///
    /// # Errors
    ///
    /// [`Error::BadPath`] for an exposure that cannot be resolved (Task 3), and
    /// [`Error::Launch`] naming the failing step and the path for everything the
    /// launch does — **after** rolling back everything already granted.
    ///
    /// **Both links are written out as targets rather than imported**, because
    /// neither `Error` nor either variant is a name this file uses in code: the
    /// two variants are built in `exposures.rs` now, and an import kept for a
    /// doc link is an import the next edit has to keep alive for no reason.
    ///
    /// [`Error::BadPath`]: crate::_core::Error::BadPath
    /// [`Error::Launch`]: crate::_core::Error::Launch
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox_core::{Backend, LaunchPlan, Sandbox};
    /// use tinio_sandbox_windows::Backend as Windows;
    ///
    /// # fn main() -> Result<(), tinio_sandbox_core::Error> {
    /// let mut backend = Windows::default();
    /// let confined = Backend::confine(&mut backend, &Sandbox::new(), &LaunchPlan::new())?;
    /// let _ = confined;
    /// # Ok(())
    /// # }
    /// ```
    fn confine(&mut self, sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement> {
        let inputs = prepare_launch(&self.options, sandbox, plan)?;
        let grants = grant_whitelist(&inputs)?;
        let launched = spawn_supervised_child(&self.options, plan, inputs);
        supervise_or_rollback(launched, grants)
    }
}

/// Resolved launch inputs: one sibling scan, one canonical whitelist, one SID.
struct PreparedLaunch<'a> {
    program: PathBuf,
    siblings: Option<SiblingScan>,
    all: Vec<Exposure>,
    caller: CallerSet,
    listener: &'a TcpListener,
    sid: Arc<PackageSid>,
    capabilities: CapabilitySids,
}

/// Resolve the served root, the whitelist, the listener, and the package SID.
///
/// The package name is [`package_name`] of the served root as exposed, so a
/// later run of the same directory takes the residue over (decision 21).
/// `..` is not resolved there. A missing listener is a named launch step:
/// `Unsupported` is reserved for a mechanism this host does not have.
fn prepare_launch<'a>(
    options: &Options,
    sandbox: &Sandbox,
    plan: &'a LaunchPlan,
) -> Result<PreparedLaunch<'a>> {
    let served = served_root(sandbox)?;
    let program = options
        .program
        .clone()
        .unwrap_or(env::current_exe().map_err(|source| {
            launch(
                "locate the launcher's own executable",
                "current_exe",
                source,
            )
        })?);
    // One enumeration: the exposure set, the banner and the `*.dll` grant all
    // read this scan.
    let siblings = program.parent().map(SiblingScan::scan);
    let all = match &siblings {
        Some(siblings) => canonical_exposures(with_siblings(sandbox, siblings))?,
        None => canonical_exposures(exposures(sandbox, &program))?,
    };
    let caller = canonical_caller(sandbox)?;
    let listener = plan.listener().ok_or_else(|| {
        launch(
            "hand the listener over",
            "argv",
            io::Error::other("the launch plan carries no bound listener"),
        )
    })?;
    let package = package_name(&served);
    #[allow(clippy::arc_with_non_send_sync)]
    let sid = Arc::new(PackageSid::derive(&package)?);
    let capabilities = CapabilitySids::resolve(
        &options
            .capabilities
            .iter()
            .map(|capability| capability.sid())
            .collect::<Vec<_>>(),
    )?;
    Ok(PreparedLaunch {
        program,
        siblings,
        all,
        caller,
        listener,
        sid,
        capabilities,
    })
}

/// Grant the whitelist and return the guard that rolls it back.
///
/// Caller directories keep the inherited ACE. The program directory does not,
/// and sibling DLLs are one `*.dll` pattern. A caller's file never reaches the
/// loop: [`canonical_caller`] refused it. Failure returns before the phase
/// report; [`crate::acl::Grants`]'s `Drop` rolls back what was already applied.
fn grant_whitelist(inputs: &PreparedLaunch<'_>) -> Result<Grants> {
    eprintln!("sandbox: package SID {}", inputs.sid.as_str());
    eprintln!("sandbox: granting {}", describe(&inputs.all));
    let mut grants = Grants::new(Arc::clone(&inputs.sid));
    let granting = Instant::now();
    if let Some(siblings) = &inputs.siblings {
        grants.apply_siblings(siblings, AccessMode::ReadOnly)?;
    }
    let program_dir = inputs
        .siblings
        .as_ref()
        .and_then(|siblings| fs::canonicalize(siblings.dir()).ok());
    for exposure in inputs
        .all
        .iter()
        .filter(|exposure| !exposure.host.is_file())
    {
        let target = shape_for(&inputs.caller, exposure, program_dir.as_deref());
        grants.apply(&exposure.host, target, exposure.access)?;
    }
    report_grant_phase(granting.elapsed(), inputs.all.len());
    Ok(grants)
}

/// Create the job, launch the child into it, write the payload, arm the relay.
///
/// The job exists before `CreateProcessW`, so a failure to create it leaves no
/// child. Assignment stays fatal: a child that is not in the job is terminated.
/// Parent copies of the child's pipe ends are closed, or the relay never sees
/// end-of-stream. [`control::arm`] is the last fallible call, so an error return
/// does not leave a console handler installed.
fn spawn_supervised_child(
    options: &Options,
    plan: &LaunchPlan,
    inputs: PreparedLaunch<'_>,
) -> Result<(LaunchedProcess, Job, JoinHandle<Option<Terminal>>)> {
    let PreparedLaunch {
        program,
        listener,
        sid,
        capabilities,
        ..
    } = inputs;
    socket::mark_inheritable(listener)?;
    let payload_pipe = handles::pipe().map_err(|source| launch("CreatePipe", "stdin", source))?;
    let event_pipe = handles::pipe().map_err(|source| launch("CreatePipe", "stdout", source))?;
    let payload = serde_json::to_vec(&LaunchPayload {
        config: plan.config().to_owned(),
    })
    .map_err(|err| {
        launch(
            "serialize the launch payload",
            "stdin",
            io::Error::other(err),
        )
    })?;
    // `skip(1)`: `command_args` inserts the listener element at index 1, and the
    // child drops `argv[0]` before reading it.
    let argv = options
        .args
        .clone()
        .unwrap_or_else(|| env::args_os().skip(1).collect());
    let args = command_args(socket::handle(listener), &argv);
    let stderr = handles::inherited_stderr()?;
    let container = Container {
        package_sid: sid.raw(),
        capabilities,
    };
    let job = Job::create()?;
    let launched = process::launch(
        &LaunchSpec {
            program: &program,
            args: &args,
            stdin: payload_pipe.read,
            stdout: event_pipe.write,
            stderr,
            inherited: &[socket::handle(listener)],
        },
        &container,
    )?;
    if let Err(err) = job.assign(launched.handle()) {
        let _ = launched.terminate(1);
        return Err(err);
    }
    handles::close(payload_pipe.read);
    handles::close(event_pipe.write);
    handles::write_payload(payload_pipe.write, &payload)?;
    control::arm(launched.pid())?;
    let events = tokio_fs::File::from_std(handles::parent_end(event_pipe.read));
    let relay = thread::spawn(move || {
        let reader = tokio_io::BufReader::new(events);
        let Ok(runtime) = Builder::new_current_thread().build() else {
            return None;
        };
        runtime
            .block_on(Observer::new(tokio_io::stdout()).observe(reader))
            .ok()
            .and_then(|observer| observer.terminal().cloned())
    });
    Ok((launched, job, relay))
}

/// Move the grants into the wait closure, or drop them by returning the error.
///
/// The success arm is the only one that moves `grants`. The error arm returns
/// while still holding them, so `Drop` runs the same rollback. The job is held
/// by the closure so it outlives the child (decision 20).
fn supervise_or_rollback(
    launched: Result<(LaunchedProcess, Job, JoinHandle<Option<Terminal>>)>,
    mut grants: Grants,
) -> Result<Confinement> {
    match launched {
        Ok((mut launched, job, relay)) => Ok(Confinement::Supervise(
            Child::new(move || {
                let status = launched.wait();
                if let Err(err) = grants.rollback() {
                    tracing::error!(step = "roll back after the child exited", error = %err);
                }
                let _ = &job;
                status
            })
            .with_relay(relay),
        )),
        Err(err) => Err(err),
    }
}

/// The readiness budget a confined launch has to fit inside — **30 s**, the
/// number design §6's ACL measurement is stated against and the deadline a
/// consumer of a launch times it to. The number has one definer and two
/// restatements beside this one already: `crates/tinio-e2e/tests/steps/clients.rs`
/// defines it as `START_TIMEOUT` — the receive timeout `wait_for_ready` reads the
/// readiness frame under — and this crate's own suites restate it as
/// `tests/common/mod.rs`'s `TIMEOUT`, in the same words about why.
///
/// **It is a restatement and not a shared constant, which is the honest shape
/// rather than a shortcut.** The budget belongs to the consumer: a backend is
/// handed a [`Sandbox`] and a [`LaunchPlan`] and nothing else, so no launcher is
/// ever told how long it has. What this crate can do with the number is measure
/// its own phase against it and say so when the phase takes a share of it, which
/// is [`report_grant_phase`]'s job.
///
/// [`LaunchPlan`]: crate::_core::LaunchPlan
const READINESS_BUDGET: Duration = Duration::from_secs(30);

/// How long the ACL grant phase may take before the run says so at `warn!`:
/// **half the readiness budget**, so 15 s.
///
/// **A share of the budget rather than the budget, and the share is a
/// judgement** — stated as one, because no divisor of the budget is derivable
/// from the measurement (one 200,200-object tree's rollback differed by a factor
/// of 1.7 between two runs of it, so §6's numbers are magnitudes rather than
/// constants). Three reasons decide it, in the order they matter:
///
/// - **The grant is one phase of a launch and not the whole of it.**
///   `CreateProcessW`, the job, the relay, the child's own startup and its
///   readiness handshake all come after this phase and run against what it left,
///   so a phase that has spent half the budget has halved the room everything
///   else was given.
/// - **A threshold *at* the budget would speak only after the deadline the
///   number exists to explain.** A 20 s grant — a run that reads as a hang to
///   anyone watching it — would cross no line and leave nothing in the log.
/// - **The measured case is far past every share of it.** 200,200 objects cost
///   32–38 s of granting on NTFS (design §6, 2026-09-22), twice this threshold
///   and past the budget itself, so any divisor between a third and the whole of
///   it separates that case from a healthy run; half is the one that is
///   unambiguous rather than marginal.
///
/// **What the threshold is not is a pre-check's input.** It compares a duration
/// this launcher has already spent; nothing here walks a tree to predict one,
/// because that walk is the cost the budget is being spent on (the function
/// below states it at length).
///
/// **The arithmetic is spelled `15` rather than a division of the constant
/// above, because it cannot be the latter on this toolchain**: `Duration`'s
/// `Div<u32>` is stable for values and not for constants, and a `const` that
/// divides one is rejected `E0658` ("`Div` is not yet stable as a const trait",
/// measured 2026-09-23 on `rustc 1.98.1`). The half is therefore stated here and
/// in the doc above, which is the one place the two constants can drift apart —
/// hence the sentence rather than a silent `15`.
const GRANT_PHASE_WARN_AFTER: Duration = Duration::from_secs(15);

/// Report what the ACL grant phase cost, and say so out loud when it took a
/// share of the readiness budget.
///
/// **Every run gets the measurement; only a slow one gets the warning.** The
/// `debug!` carries the figure for any run whose subscriber enables it (shared
/// §7's per-operation event, so a debugging operator has the number whatever it
/// was), and [`GRANT_PHASE_WARN_AFTER`] decides the `warn!` beside it. That
/// split is what makes the loud half visible where an operator already is:
/// these events reach the launcher's stderr through the subscriber its caller
/// installed, and `serve` builds that one at `info` (`Verbosity::Info` in
/// `crates/tinio-server/examples/serve/main.rs`), so the warning is what a
/// default run shows and the measurement is opt-in.
///
/// **What the warning promises is the number and nothing else.** It cannot make
/// the run fit the budget and is written not to read as though it could: the
/// pass is the filesystem's own inheritance over every object under every
/// exposed directory (the `acl` module's doc carries the measurement — 200,200
/// objects, 32–38 s of granting and 27–47 s of rollback on NTFS against
/// 8.6–8.8 s each way on ReFS), so what shortens it is the operator's choice of
/// what to expose. The message therefore points at the design's own remedy — expose the
/// narrowest directory that serves the corpus, or accept the cost — and says in
/// as many words that this line reports the pass rather than shortening it.
///
/// **It is emitted when the phase ends and before the child exists**, which is
/// both the earliest it can be accurate and late enough to explain a wait after
/// the fact: a consumer that has already timed out leaves the figure in the
/// launcher's log for whoever reads it, and that is the whole difference between
/// "nothing said why" and "the ACL pass took 34 s". A heartbeat *during* the
/// phase would need a timer thread, and what an operator does with the answer
/// does not change with the progress it would show.
///
/// **The fields carry what is cheap and true, and what is missing is the point.**
/// `elapsed_ms` is the phase's whole-millisecond duration; `exposures` is the
/// resolved exposure set's length — the same set the `sandbox: granting …`
/// banner names, and deliberately not the number of `icacls` invocations, which
/// the batched sibling grant makes a different number — and `threshold_ms` rides
/// the warning alone, so the rule a run crossed is in the line rather than only
/// in this source. **No field states the size of any tree**, because this
/// launcher does not know it and cannot learn it cheaply: an estimate is a walk
/// of the corpus, which is the O(corpus) pass on the very path whose budget is
/// short, and one `read_dir` of a directory is not the size of the tree beneath
/// it. **The program's own directory is not a field either, and that is a
/// decision**: it is the one exposed directory whose ACE does not propagate
/// (decision 22, as Ruling 78 reshaped it — one ACE on the directory and one
/// batched ACE on the `*.dll` files directly beside the program), so naming it
/// in a message about cost would point the reader at the one entry that is not
/// one.
///
/// **It is a free function rather than a block inside `confine`, so that a test
/// can pin the threshold and the level without a launch**: `backend::tests` calls
/// it with a chosen duration and reads the level back off its capture, which is
/// the only way to assert both halves of the decision at once.
///
/// **Measured on this host, 2026-09-23** (`D:\Temp`, where `%TEMP%` resolves:
/// NTFS, the filesystem §6's slow half was measured on): a freshly built tree of
/// **5,201 objects** granted in **272 ms, 283 ms and 313 ms** across three runs,
/// with rollbacks of **261 ms, 292 ms and 397 ms** — 0.052–0.060 ms of granting
/// per object. **A tree of a few thousand objects does not reach this
/// threshold**, and the two measurements read together put it in the 10^5
/// objects: 15 s of granting is ~250,000 objects at that small tree's per-object
/// rate and ~90,000 at §6's for its 200,200-object tree (0.16 ms/object) — the
/// two rates differ by a factor of three, and §6 records that the shape of the
/// curve is not yet measured, so what to carry away is the order of magnitude
/// rather than a count. What this warning is for is the corpus §6 measured, not
/// the one a launcher is usually handed.
fn report_grant_phase(elapsed: Duration, exposures: usize) {
    let elapsed_ms = elapsed.as_millis() as u64;
    tracing::debug!(step = "grant the ACLs", elapsed_ms, exposures);
    if elapsed < GRANT_PHASE_WARN_AFTER {
        return;
    }
    let threshold_ms = GRANT_PHASE_WARN_AFTER.as_millis() as u64;
    tracing::warn!(
        step = "grant the ACLs",
        elapsed_ms,
        exposures,
        threshold_ms,
        "the ACL grant phase took {elapsed:?}, half or more of the {READINESS_BUDGET:?} readiness budget a confined launch has to fit inside; the grant and its rollback each touch every object under an exposed directory (the filesystem's inheritance, not a walk this launcher makes), so expose the narrowest directory that serves the corpus or accept the cost — this line reports the pass, it does not shorten it (design §6)"
    );
}
