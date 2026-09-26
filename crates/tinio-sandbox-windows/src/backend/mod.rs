//! Windows backend: whitelist, container, relay, and the supervision handed back.
//!
//! `Backend::confine` runs four stages: `prepare_launch`, `grant_whitelist`,
//! `spawn_supervised_child`, and `supervise_child`. Capability names live
//! in `capability.rs`; exposure policy in `exposures.rs`; the grant phase's
//! budget and its report in `grant.rs`. Re-exports below keep the old
//! `backend::` paths. Submodules are private.
//!
//! `canonical_caller`, `shape_for`, and `CallerSet` are `pub(super)` so `confine`
//! and `backend::tests` can reach them. `CallerSet` is not `pub`: a raw spelling
//! must not be usable as the resolved one. This module makes no Win32 call, so
//! it is not an `unsafe` site (decision 14).

mod capability;
mod exposures;
mod grant;
#[cfg(test)]
mod tests;

use std::{
    env,
    ffi::OsString,
    fs, io,
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{self, Receiver},
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use tokio::{fs as tokio_fs, io as tokio_io, runtime::Builder};
use windows_sys::Win32::Foundation::HANDLE;

use self::exposures::{CallerSet, canonical_caller, shape_for};
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
        AccessMode, Backend as BackendTrait, Child, Confinement, Exposure, LaunchPayload,
        LaunchPlan, Observer, Result, Sandbox, Terminal, launch,
    },
    acl::{Grants, Target},
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
    /// `spawn_supervised_child`, and `supervise_child`.
    /// [`crate::acl::Grants`] moves into the wait closure on success, and its job
    /// there is the profile's lifetime rather than any undo: **the ACLs are
    /// standing** ([`crate::acl::Grants`]'s doc states why and what that gives up),
    /// so every other path simply drops the guard, which deletes the profile with
    /// the last `Arc<PackageSid>` — a launch that failed has no user left for it.
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
        let inputs = prepare_launch(&self.options, sandbox, plan)?;
        let grants = grant_whitelist(&inputs)?;
        let launched = spawn_supervised_child(&self.options, plan, inputs);
        supervise_child(launched, grants)
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
///
/// **It announces the pass once, as an `info!` event whose fields are the package
/// SID and the exposure set**, which is what an operator reads to see what the run
/// was about to grant and which profile it was about to grant it to. The event is
/// emitted *before* the first `icacls`, so a run that dies mid-phase still says what
/// it was doing, and it is one structured event rather than the two `eprintln!`s it
/// replaced: a library that announced this with `eprintln!` could neither be tested
/// nor silenced (the same rule `crates/tinio-sandbox-linux/src/seccomp.rs` states
/// for its own line), and `[log] verbosity` is what decides whether it appears.
fn grant_whitelist(inputs: &PreparedLaunch<'_>) -> Result<Grants> {
    tracing::info!(
        step = "grant the ACLs",
        sid = inputs.sid.as_str(),
        exposures = inputs.all.len(),
        granting = %describe(&inputs.all)
    );
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
        // **The type is the whole safety net here, so the one fact the choice
        // rests on is checked**: `acl::ace_spec` produces the same `RX`/`M` for
        // `Target::ProgramDirectory` and `Target::File`, so a shape that
        // disagreed with its path would be an `icacls` that exits 0 and writes
        // nothing — a confined child that cannot read a file its loader needs.
        // The loop above is also what keeps this from firing in a debug build:
        // every host reached here was resolved by `canonical_exposures`, so it
        // exists, and the file-shaped entries — the ones for which a directory
        // assertion would be wrong — were filtered out before it.
        debug_assert!(
            target != Target::ProgramDirectory || exposure.host.is_dir(),
            "Target::ProgramDirectory is for a directory, and {} is not one",
            exposure.host.display()
        );
        // **The program directory is the one entry that does not depend on the
        // root, so it is a standing grant for `ALL APPLICATION PACKAGES` rather
        // than for this run's package SID.** Every other entry belongs to one
        // served root, and a new root is a new SID — which is why authorizing the
        // program directory per root would rewrite its security descriptor on
        // every launch and make `SetNamedSecurityInfoW` walk its whole subtree
        // (10–20 s for this workspace's `target\debug`; `sid::ALL_APPLICATION_PACKAGES`
        // carries the price and why the other entries must not take it).
        match target {
            Target::ProgramDirectory => {
                grants.apply_standing(&exposure.host, target, exposure.access)?
            }
            _ => grants.apply(&exposure.host, target, exposure.access)?,
        }
    }
    report_grant_phase(granting.elapsed(), inputs.all.len());
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
/// publishes the child's `state`, whose DACL a confined child cannot write), so
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
        sid,
        capabilities,
        ..
    } = inputs;
    socket::mark_inheritable(listener)?;
    // The control channel, when the plan carries one: the launcher bound it (only an
    // unrestricted process can — its DACL is current-user-only, and inside the container
    // this child's identity is the package SID), so this process hands the handles over
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
    let payload_pipe = handles::pipe().map_err(|source| launch("CreatePipe", "stdin", source))?;
    let event_pipe = handles::pipe().map_err(|source| launch("CreatePipe", "stdout", source))?;
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
            stdin: payload_pipe.read,
            stdout: event_pipe.write,
            stderr,
            inherited: &inherited,
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

/// Hand the launched child to its supervision, keeping the SID alive for it.
///
/// The success arm is the only one that moves `grants`. The error arm returns
/// while still holding them, which drops the last `Arc<PackageSid>` and deletes
/// the profile with it — the launch failed, so the profile has no user. **Nothing
/// is taken back**: every grant is standing (`acl::Grants`), so the wait closure's
/// only job here is to keep the profile alive until the child has exited and to
/// hold the job so the child's lifetime is bounded by it (decision 20).
fn supervise_child(launched: Result<Spawned>, grants: Grants) -> Result<Confinement> {
    match launched {
        Ok(Spawned {
            mut launched,
            job,
            relay,
            ready,
        }) => Ok(Confinement::Supervise(
            Child::new(move || {
                let status = launched.wait();
                let _ = &grants;
                let _ = &job;
                status
            })
            .with_relay(relay)
            .with_ready(ready),
        )),
        Err(err) => Err(err),
    }
}
