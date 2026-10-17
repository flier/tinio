//! Windows backend: whitelist, container, relay, and the supervision handed back.
//!
//! `Backend::confine` runs three stages: `prepare_launch`, `grant_whitelist`,
//! and `spawn_supervised_child`, whose `Spawned` it turns into the `Confinement`
//! it hands back. Capability names live
//! in `capability.rs`; exposure policy in `exposures`; the grant phase's
//! budget and its report in `grant.rs`. Re-exports below keep the old
//! `backend::` paths. Submodules are private.
//!
//! `canonical_whitelist`, `shape_for`, and `Whitelist` are `pub(in crate::backend)` so
//! `confine` and `backend::tests` can reach them. `CallerSet` is
//! not `pub`: a raw spelling must not be usable as the resolved one. This module
//! makes no Win32 call, so it is not an `unsafe` site (decision 14).

mod capability;
mod exposures;
mod grant;
#[cfg(test)]
mod tests;

use std::{
    env,
    ffi::OsString,
    io,
    net::TcpListener,
    path::PathBuf,
    rc::Rc,
    sync::{
        Arc,
        mpsc::{self, Receiver},
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use tokio::{fs as tokio_fs, io as tokio_io, runtime::Builder};
use windows_sys::Win32::Foundation::HANDLE;

use self::exposures::{Whitelist, canonical_whitelist, shape_for};
// The stage this file orchestrates calls the report; the constants beside it are
// `grant.rs`'s and are read where they are measured against — `grant.rs` itself
// and `backend::tests`, which imports them from that module rather than through
// here (a re-export of a name only a test build reads is an unused import in
// every other build).
use self::grant::report_grant_phase;
pub use self::{
    capability::Capability,
    exposures::{
        SiblingScan, canonical_exposures, describe, exposures, merge, served_root, sibling_dlls,
        with_siblings,
    },
};
use crate::{
    _core::{
        AccessMode, Backend as BackendTrait, Child, Confinement, LaunchPayload, LaunchPlan,
        Observer, Result, Sandbox, Terminal, launch,
    },
    acl::{Grants, Target},
    capability::{Admission, admit},
    control, handles,
    job::Job,
    launch::command_args,
    process::{self, Container, LaunchSpec, LaunchedProcess},
    sid::{CapabilitySids, PackageSid, ProfileLock, package_name},
    socket,
    sweep::stale_profiles,
};

/// The exit code this launcher's own endings report — the launch that did not
/// happen, or the one whose next step refused it.
///
/// **One code for the two, and it is not the child's answer**: a terminated child
/// never had one to give, and both call sites are the launcher saying that
/// something on its own side failed. The failed-assignment path
/// ([`spawn_supervised_child`]) already ends its child with this code, and the
/// killer the core's `Child::abort` drives is the same ending reached from the
/// other side.
const ABORT_CODE: u32 = 1;

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
    /// A test seam with one job: a launch that fails *after* the ACLs are on,
    /// which is the only way to exercise the failure path design §11 describes
    /// — every grant written before it left standing, and the profile taken
    /// back with the value that held it (`tests/confine.rs`'s failed-launch
    /// test).
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
    /// The stages are `prepare_launch`, `grant_whitelist`, and
    /// `spawn_supervised_child`.
    /// [`crate::acl::Grants`] moves into the wait closure on success, and its job
    /// there is the profile's lifetime rather than any undo: **the ACLs are
    /// standing** ([`crate::acl::Grants`]'s doc states why and what that gives up),
    /// so every other path simply drops the guard, which deletes the profile with
    /// the last `Arc<PackageSid>` — a launch that failed has no user left for it.
    /// The job the launch created moves into **both** closures: the wait holds it so
    /// the child's lifetime is bounded by the launcher's (decision 20), and the
    /// killer the core's `Child::abort` calls terminates it, which is how a launch
    /// that failed *after* the child existed ends a server that is already serving.
    ///
    /// # Errors
    ///
    /// [`Error::BadPath`] for an exposure that cannot be resolved (Task 3), and
    /// [`Error::Launch`] naming the failing step and the path for everything the
    /// launch does — with every grant already written left standing, which is the
    /// same no-rollback rule the child's exit path follows.
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
        // **The host's own gate, and it sits above everything this crate does.** The CLI asks the
        // same question before it writes its state directory; this one protects every other
        // embedder of this crate, and keeps the stale-profile sweep, the ACL salt file, the
        // profile mutex and the profile itself from happening for a run that cannot be confined.
        //
        // **The order is `prepare_launch`'s parameter now, not this comment's subject.** The
        // witness below is the only value that function accepts and `admit` is the crate's only
        // constructor of it, so a later edit that moved the crate's first side effect above this
        // line would not compile. The source-reading test that used to pin the order is gone
        // with it.
        let admitted = admit()?;
        let inputs = prepare_launch(admitted, &self.options, sandbox, plan)?;
        let grants = grant_whitelist(&inputs)?;
        // **A failed spawn returns with the grants still held here**, which drops
        // the last `Arc<PackageSid>` and deletes the profile with it — the launch
        // failed, so the profile has no user. **Nothing is taken back**: every
        // grant is standing ([`crate::acl::Grants`]), so the wait closure's only
        // job is to keep the profile alive until the child has exited and to hold
        // the job so the child's lifetime is bounded by it (decision 20) — and the
        // job is the same value the killer below terminates through.
        let Spawned {
            mut launched,
            job,
            relay,
            ready,
        } = spawn_supervised_child(&self.options, plan, inputs)?;
        // **The job is shared with the killer instead of living in the wait closure alone**,
        // because the two are the same mechanism used from two sides: `TerminateJobObject` is
        // what ends the child *now*, and holding the job for the child's whole lifetime is what
        // ends it when the launcher does (decision 20). `Rc` rather than a move is what lets
        // both closures name it; it needs no `RefCell` because `terminate` borrows the job and
        // the two callers are strictly ordered (`Child::abort` ends the child, then waits).
        let job = Rc::new(job);
        let aborting = Rc::clone(&job);
        // The two `let _` lines are what make the closure **capture** both guards
        // rather than let them drop at the end of this call.
        Ok(Confinement::Supervise(
            Child::new(move || {
                let status = launched.wait();
                let _ = &grants;
                let _ = &job;
                status
            })
            // **The killer the core's failure path calls**, and the platform's own mechanism
            // rather than a second one: terminating the job ends the child the launcher put in
            // it, exactly as closing the job does when the launcher exits — the difference is
            // when. Without it a launcher that refused the `state` publish would have to exit
            // for the child to die, and the core would have no way to end it.
            .with_kill(move || aborting.terminate(ABORT_CODE).map_err(io::Error::other))
            .with_relay(relay)
            .with_ready(ready),
        ))
    }
}

/// Resolved launch inputs: one sibling scan, one canonical whitelist, one SID.
struct PreparedLaunch<'a> {
    program: PathBuf,
    siblings: Option<SiblingScan>,
    whitelist: Whitelist,
    listener: &'a TcpListener,
    /// **The launch window's lock, taken before the profile is derived and
    /// released once `CreateProcessW` has returned** — see [`sid::ProfileLock`]
    /// for what it excludes a peer's delete from, and
    /// [`spawn_supervised_child`] for where it is released.
    window: Option<ProfileLock>,
    sid: Arc<PackageSid>,
    capabilities: CapabilitySids,
}

/// Resolve the served root, the whitelist, the listener, and the package SID.
///
/// The package name is [`package_name`] of the served root as exposed, so a
/// later run of the same directory takes the residue over (decision 21).
/// `..` is not resolved there. A missing listener is a named launch step:
/// `Unsupported` is reserved for a mechanism this host does not have.
///
/// **`Admission` is the ordering proof and nothing else**: the body never reads it, and it is
/// taken by value so a caller must have asked the host first. It is what replaced the
/// source-reading test that pinned the gate above this call.
fn prepare_launch<'a>(
    _admitted: Admission,
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
    // **Every stale profile an unclean exit left is reaped here, before this
    // launch derives its own name** (`crate::sweep`, design decision 21): the profile
    // the run about to start will create cannot be a candidate, because it does not
    // exist yet — which is the ordering the sweep's own rules rest on. It is
    // best-effort hygiene and not a stage of the launch: it returns nothing, it
    // warns where it fails, and a machine it cannot sweep is a machine that keeps
    // its residue rather than a launch that fails.
    stale_profiles();
    // One enumeration: the whitelist's DLL entries, the banner and the `*.dll`
    // grant all read this scan.
    let siblings = program.parent().map(SiblingScan::scan);
    // **One resolution of the caller's paths, and the program's directory on the
    // same pass**: the two halves used to be resolved separately — the granted set
    // through `canonical_exposures`, the caller's set through a second walk of the
    // same paths — so every caller path was walked, canonicalised and logged
    // twice, and a third `fs::canonicalize` of the program's directory followed in
    // `grant_whitelist`.
    let whitelist = canonical_whitelist(sandbox, siblings.as_ref())?;
    let listener = plan.listener().ok_or_else(|| {
        launch(
            "hand the listener over",
            "argv",
            io::Error::other("the launch plan carries no bound listener"),
        )
    })?;
    let package = package_name(&served);
    // **The window opens here, before the profile is derived**, and closes once
    // the child exists ([`spawn_supervised_child`]): a peer's `Drop` deletes the
    // profile this root shares, and a delete that lands between this derive and
    // `CreateProcessW` is the one interleaving that damages anything — this
    // launch then fails `err=2 ERROR_FILE_NOT_FOUND` for a program that exists
    // (measured 2026-09-26). `None` means the kernel refused the lock, which the
    // acquisition warned about and which is the behaviour of a launcher that had
    // no lock at all.
    let window = ProfileLock::acquire(&package);
    #[expect(
        clippy::arc_with_non_send_sync,
        reason = "`PackageSid` owns a raw `PSID` and is deliberately neither `Send` nor `Sync`, so the `Arc` is shared ownership within this one launch rather than a thread handoff"
    )]
    let sid = Arc::new(PackageSid::derive(&package)?);
    // **The capability list is resolved straight from the options, which is the
    // only shape it can take**: [`CapabilitySids::resolve`] is handed the closed
    // enum, so nothing between an operator's `TINIO_SANDBOX_CAPABILITIES` and the
    // container's token is text a caller could spell.
    let capabilities = CapabilitySids::resolve(&options.capabilities)?;
    Ok(PreparedLaunch {
        program,
        siblings,
        whitelist,
        listener,
        window,
        sid,
        capabilities,
    })
}

/// Grant the whitelist and return the guard that keeps the profile alive.
///
/// Caller directories keep the inherited ACE. The program directory does not,
/// and every sibling DLL is granted on its own handle. A caller's file never
/// reaches the loop: [`canonical_whitelist`] refused it, and the loop walks that
/// whitelist's resolved entries alone, so no entry here is a file. Failure returns
/// before the phase report, and **nothing is taken back**: the grants are standing,
/// so what a partial pass leaves behind is exactly what a complete one does
/// ([`crate::acl::Grants`]'s doc states the contract and its measurement).
///
/// **It announces the pass once, as an `info!` event whose fields are the package
/// SID and the exposure set**, which is what an operator reads to see what the run
/// was about to grant and which profile it was about to grant it to. The event is
/// emitted *before* the first grant, so a run that dies mid-phase still says what
/// it was doing, and it is one structured event rather than the two `eprintln!`s it
/// replaced: a library that announced this with `eprintln!` could neither be tested
/// nor silenced (the same rule `crates/tinio-sandbox-linux/src/seccomp.rs` states
/// for its own line), and `[log] verbosity` is what decides whether it appears.
fn grant_whitelist(inputs: &PreparedLaunch<'_>) -> Result<Grants> {
    // **The banner lists every path the run is about, the sibling DLLs included,
    // and its count is that list's length** — the meaning both had before the
    // DLLs stopped being resolved, so an operator's `exposures=` does not silently
    // change under them. The DLLs come from `SiblingScan` and carry no resolution:
    // each is granted by handle, on the path the scan enumerated.
    tracing::info!(
        step = "grant the ACLs",
        sid = inputs.sid.as_str(),
        exposures = inputs.whitelist.listed_len(),
        granting = %inputs.whitelist.describe()
    );
    let grants = Grants::new(Arc::clone(&inputs.sid));
    let granting = Instant::now();
    if let Some(siblings) = &inputs.siblings {
        grants.apply_siblings(siblings.files(), AccessMode::ReadOnly)?;
    }
    for exposure in inputs.whitelist.granted() {
        let target = shape_for(
            inputs.whitelist.caller(),
            exposure,
            inputs.whitelist.program_dir(),
        );
        // **The type is the whole safety net here, and the one fact the choice
        // rests on is checked where it matters** — the `Target` and the object the
        // grant opens have to agree, or the ACE reaches the wrong thing: an
        // inheritance-flagged grant on a file is a silent no-op, and the plain
        // file form on a directory leaves every descendant ungranted. The check is
        // the grant's own (`acl::Object::verify`, on the opened handle), which is
        // the only place the question can be asked without a race: a shape that
        // disagreed with its path would still be checked there, against the object
        // the write will reach, and refused rather than written.
        //
        // **The program directory is the one entry that does not depend on the
        // root, so it is a standing grant for `ALL APPLICATION PACKAGES` rather
        // than for this run's package SID.** Every other entry belongs to one
        // served root, and a new root is a new SID — which is why authorizing the
        // program directory per root would rewrite its security descriptor on
        // every launch and make the write walk its whole subtree (10–20 s for this
        // workspace's `target\debug`; `sid::ALL_APPLICATION_PACKAGES` carries the
        // price and why the other entries must not take it).
        match target {
            Target::ProgramDirectory => {
                grants.apply_standing(&exposure.host, target, exposure.access)?
            }
            _ => grants.apply(&exposure.host, target, exposure.access)?,
        }
    }
    report_grant_phase(granting.elapsed(), inputs.whitelist.listed_len());
    Ok(grants)
}

/// What a successful spawn hands the supervision.
///
/// A named type rather than the tuple it grew out of, and not only because four elements made
/// clippy's `type_complexity` fire: these four travel together as one value through
/// [`confine`]'s stages, and the two channel halves are the two questions a launcher can ask
/// about readiness — `relay`'s return value is final at the child's exit
/// ([`Child::terminal`]), while `ready` is the first terminal event as it is recorded
/// ([`Child::wait_for_ready`]).
///
/// [`Child::terminal`]: crate::_core::Child::terminal
/// [`Child::wait_for_ready`]: crate::_core::Child::wait_for_ready
struct Spawned {
    /// The process, and the platform's handle to it.
    launched: LaunchedProcess,
    /// The job object that owns the child's lifetime (decision 20).
    job: Job,
    /// The relay thread, whose return value is what it recorded.
    relay: JoinHandle<Option<Terminal>>,
    /// The relay's mid-stream announcement: the first terminal event, once.
    ready: Receiver<Terminal>,
}

/// Create the job, launch the child into it, write the payload, arm the relay.
///
/// The job exists before `CreateProcessW`, so a failure to create it leaves no
/// child. Assignment stays fatal: a child that is not in the job is terminated.
/// Parent copies of the child's pipe ends are closed, or the relay never sees
/// end-of-stream. [`control::arm`] is the last fallible call, so an error return
/// does not leave a console handler installed.
///
/// **The payload carries the plan's token, and the relay announces readiness on a
/// channel of its own.** The token is the launcher's (it is the process that
/// publishes the child's `state`, which a confined child does not write), so
/// the frame is where it travels: `plan.token()`, serialized here, is the one
/// place the platform turns the plan into the child's stdin. See [`Spawned`] for
/// the two channel halves the relay hands back.
fn spawn_supervised_child(
    options: &Options,
    plan: &LaunchPlan,
    inputs: PreparedLaunch<'_>,
) -> Result<Spawned> {
    let PreparedLaunch {
        program,
        listener,
        window,
        sid,
        capabilities,
        ..
    } = inputs;
    socket::mark_inheritable(listener)?;
    // The control channel, when the plan carries one: the launcher bound it (only an
    // unrestricted process can — the pipe carries Windows' own default named-pipe descriptor,
    // and inside the container this child's identity is the package SID), so this process
    // hands the handles over
    // the way it hands the listener over, and the child serves on the objects rather than
    // creating one. **Every instance of the pool, not just the first**: the child cannot create
    // a pipe at all, so the handles it is handed are the whole of what it can ever serve on.
    // Marked inheritable for the reason the listener is: a handle list cannot name a handle
    // without the bit, and `spawn_supervised_child` is the last point before `CreateProcessW`
    // at which it can be set. **The value comes off the plan's own accessor**, which is where
    // this platform keeps the handle number the launcher's channel owns; Windows needs no clear
    // for the handoff, so nothing here has to reach an object the way macOS's launcher does.
    let controls: Vec<HANDLE> = plan
        .controls()
        .iter()
        .map(|handle| handle.value() as HANDLE)
        .collect();
    for control in &controls {
        handles::mark_inheritable(*control)
            .map_err(|source| launch("mark the control channel inheritable", "control", source))?;
    }
    // **Both pipes own their ends from here on**, so every failure between this
    // line and the closes below — the second `pipe()`, `inherited_stderr()`,
    // the job, the launch, the payload write, the relay's arming — returns with
    // nothing leaked, and the event pipe's write end in particular cannot
    // survive into a relay that would then never see end-of-stream
    // (`handles::Pipe`'s doc, and `handles::parent_end` for why that one hangs
    // rather than leaks).
    let mut payload_pipe =
        handles::pipe().map_err(|source| launch("CreatePipe", "stdin", source))?;
    let mut event_pipe =
        handles::pipe().map_err(|source| launch("CreatePipe", "stdout", source))?;
    // **Serialized from borrows** (`LaunchPayload::encode_frame`): the configuration text and the
    // plan's token go straight into the frame, where an owned `LaunchPayload` would have copied
    // both first — and this is the path that blocks until the child drains its stdin.
    //
    // The token is the launcher's, when it has one: the child adopts it instead of minting its
    // own, so the value a client reads out of `state` is the value the plane verifies. **The
    // confinement is absent on Windows by construction**: this platform's confinement is the
    // container the child was created in, so there is nothing for the child to apply to itself —
    // the field is the in-process mechanism's command, and a launcher that resolves one is the
    // only writer of it.
    let payload =
        LaunchPayload::encode_frame(plan.config(), plan.token(), None).map_err(|err| {
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
    let args = command_args(socket::handle(listener), &controls, &argv);
    let stderr = handles::inherited_stderr()?;
    let container = Container {
        package_sid: sid.raw(),
        capabilities,
    };
    let job = Job::create()?;
    // The handle list, in one vector: the listener always, then every control instance the
    // plan named. Built here rather than at the call so the two handoffs cannot name
    // different sets.
    let mut inherited = Vec::with_capacity(1 + controls.len());
    inherited.push(socket::handle(listener));
    inherited.extend(controls);
    let launched = process::launch(
        &LaunchSpec {
            program: &program,
            args: &args,
            // Borrowed, not handed over: `CreateProcessW` duplicates the child's
            // copy of each, so the pipes keep owning these until the closes
            // below. The `ok_or_else` cannot fire — nothing gives an end up
            // before this line — and it is the call-site bug's error shape
            // rather than a panic (`handles::end_gone`).
            stdin: payload_pipe
                .read()
                .ok_or_else(|| handles::end_gone("stdin"))?,
            stdout: event_pipe
                .write()
                .ok_or_else(|| handles::end_gone("stdout"))?,
            stderr,
            inherited: &inherited,
        },
        &container,
    )?;
    // **The window closes here, and this is the only place it is given up on the
    // success path.** The child exists from `CreateProcessW`'s return, so the
    // profile is no longer something this launch needs kept — and a peer's
    // delete, which was the whole reason the lock was held, can no longer turn
    // this launch into `err=2 ERROR_FILE_NOT_FOUND` for a program that exists.
    // Every failure above returns through `?`, which drops the guard with the
    // rest of the launch's locals.
    drop(window);
    if let Err(err) = job.assign(launched.handle()) {
        let _ = launched.terminate(1);
        return Err(err);
    }
    // The child's own ends are `CreateProcessW`'s copies now, so this process's
    // are closed — and the event pipe's write end in particular has to be, or
    // the relay's read never sees end-of-stream (`handles::parent_end`). Both
    // closes go through the pipes, which is what keeps a later failure from
    // closing them a second time.
    payload_pipe.close_read();
    event_pipe.close_write();
    handles::write_payload(&mut payload_pipe, &payload)?;
    control::arm(launched.pid())?;
    let events = tokio_fs::File::from_std(event_pipe.take_read_file()?);
    // The readiness announcement's two halves: the relay sends the first terminal event on the
    // sender as it records it, and the receiver rides the supervision back to the launcher. The
    // sender dies with the `Observer` when the stream ends, which is what turns a child that
    // exited without reporting into a disconnected wait rather than one that runs out its bound.
    let (ready_tx, ready_rx) = mpsc::channel();
    let relay = thread::spawn(move || {
        let reader = tokio_io::BufReader::new(events);
        let Ok(runtime) = Builder::new_current_thread().build() else {
            return None;
        };
        runtime
            .block_on(
                Observer::new(tokio_io::stdout())
                    .with_notify(ready_tx)
                    .observe(reader),
            )
            .ok()
            .and_then(|observer| observer.terminal().cloned())
    });
    Ok(Spawned {
        launched,
        job,
        relay,
        ready: ready_rx,
    })
}
