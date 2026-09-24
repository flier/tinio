//! The `--sandbox` seam: three channels, one decision, one confinement (design §7, decision 10).
//!
//! The three channels are the `--sandbox` flag, `TINIO_SANDBOX` and `[server] sandbox`, and they
//! collapse to one answer in [`plan`], which is also where every *static* refusal is raised —
//! before the fork, so the operator's shell sees it. [`confine`] is the *dynamic* half, after
//! the fork and before the runtime exists: on Linux this process enters the jail **in place**;
//! on Windows the listener is bound here and a confined child is launched to serve on it.
//!
//! **This module is compiled in every build**, because the feature gates behaviour and not
//! linkage: `TINIO_SANDBOX` and `[server] sandbox` cannot be compiled away, and code that does
//! not exist cannot refuse. The feature takes the `--sandbox` *flag* and nothing else: the
//! variable keeps an argument of its own in that build (hidden, `start::Args::sandbox`'s
//! feature-off declaration), so the channel still reaches this module and still refuses.
//!
//! **The platform arms are submodules beside this file** — `linux` and `windows` — each gated by
//! the feature *and* its target, so nothing here names a platform crate.

use std::{
    env::consts::OS,
    ffi::OsString,
    io::{self, Read},
};
// **The two helper functions' gate, spelled here because their imports are held under it too.**
// `sandbox_request` and `refuse_in_root_state_dir` are the arms' own gate (`sandbox` *and* the
// platform) widened by `test` — which is what makes the two pure rules unit-testable on a host
// with no arm — and narrowed by the feature: a feature-off graph has no arm to call them and no
// test that wants them, so it compiles neither the helpers nor the names only they use. Without
// that conjunct the imports below are unused in every `--no-default-features` graph, which is a
// `-D warnings` error the moment anyone lints one.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "windows")
))]
use std::{error::Error as StdError, path::Path};

// Tokio's listener is the one this module's public surface names (`adopted_listener` hands the
// child's socket back to the serving half), and the standard library's is the one a launcher
// binds — a launcher binds before any runtime exists, which is §1's step i. Two types, one name
// each, and the alias is the honest spelling of "the std one".
use clap::parser::ValueSource;
use tokio::net::TcpListener;

/// The facade as a module, for the two child-side names it publishes **on the launcher platform
/// only**: there is no `launch` module on the in-place platforms, so a direct import of
/// `adopted_listener`/`on_stop` would not resolve there — and this module already owns wrappers
/// under both names, so the call sites go through the module rather than shadowing them.
#[cfg(target_os = "windows")]
use crate::_sandbox;
// The same condition as the two helpers' own gate above, for the crate-side names they read.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "windows")
))]
use crate::{_core::mode::AccessMode, _sandbox::Sandbox, _util::path::path_contains};
use crate::{
    _sandbox::{Adopted, Child, adopt, is_control_fd_arg, is_listener_fd_arg},
    Error,
    cli::Provenance,
    commands::start::Args,
    resolve::{Resolution, pick},
};

#[cfg(all(feature = "sandbox", target_os = "linux"))]
mod linux;
#[cfg(all(feature = "sandbox", target_os = "windows"))]
mod windows;

/// Whether this platform's backend is still a stub — `false` where [`confine`] can actually
/// confine, `true` where it cannot and a request must therefore be refused.
///
/// **A table here rather than a call into the backend**, because the backend cannot answer it
/// where the answer is needed: [`plan`] runs *before* [`confine`] by design (§1 step f before
/// step i), and on Linux the confinement is in-place, so there is no `confine` answer to read at
/// all. The flip is per target, and that mixed state is why this is a match rather than one
/// boolean: **Linux** (P1) and **Windows** (P2) have landed and say `false`; **macOS** (P3) is
/// still `crate::_sandbox`'s `Unsupported` stub and says `true`.
///
/// **A `false` arm is a promise that [`confine`] handles that target**, and the two are wired in
/// the same edit: the arm is what lets [`plan`] return an enabled [`Request`], and [`confine`] is
/// what then has to act on it. Flipping one without the other is the failure this constant's own
/// refusal exists to catch — a request that passed `plan` and reached a `confine` with nothing to
/// do would serve **unconfined**.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::BACKEND_IS_STUB;
///
/// // Linux and Windows confine; macOS's backend is still the stub, so a request there is refused
/// // with the platform named. An arm flipped without its `confine` arm fails this, and the
/// // failure names the constant to update.
/// #[cfg(not(target_os = "macos"))]
/// assert!(!BACKEND_IS_STUB);
/// #[cfg(target_os = "macos")]
/// assert!(BACKEND_IS_STUB);
/// ```
pub const BACKEND_IS_STUB: bool = {
    #[cfg(target_os = "linux")]
    {
        false
    }
    #[cfg(target_os = "macos")]
    {
        true
    }
    #[cfg(target_os = "windows")]
    {
        false
    }
};

/// Which of the three channels asked for the confinement (design decision 10).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::Channel;
///
/// // A refusal names the channel it came from, which is why the channel is a value and not a
/// // boolean.
/// assert!(matches!(Channel::Env, Channel::Env));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// `--sandbox`.
    Flag,
    /// `TINIO_SANDBOX`.
    Env,
    /// `[server] sandbox`.
    Config,
}

impl Channel {
    /// The channel as a refusal names it — the spelling the operator used, which is the first
    /// half of every message this module raises.
    fn name(self) -> &'static str {
        match self {
            Self::Flag => "`--sandbox`",
            Self::Env => "`TINIO_SANDBOX`",
            Self::Config => "`[server] sandbox`",
        }
    }
}

/// What the three channels resolved to.
///
/// The exposure set, device-file grant and OTLP trade land with the platform backend [`confine`]
/// hands the request to. [`plan`] answers a disabled one when no channel asked, and an enabled one
/// only after `refuse_unhonourable` has cleared both the feature and the target — so an enabled
/// `Request` is one this build is committed to applying, and the exposure set is rebuilt in the
/// platform arm rather than carried here.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::Request;
///
/// // No channel set: nothing is asked for.
/// let request = Request { enabled: false };
/// assert!(!request.enabled);
/// ```
#[derive(Debug)]
pub struct Request {
    /// Whether a confinement was asked for at all.
    pub enabled: bool,
}

/// Reads the launch payload out of `argv`, or decides this process is not a confined child.
///
/// The elements' spellings, their predicates and the adoption are all the **facade's**:
/// `LISTENER_FD_PREFIX`, `CONTROL_FD_PREFIX`, `is_listener_fd_arg` and `is_control_fd_arg`
/// are what name and recognise them, and `adopt(args, stdin)` is what reads the payload off
/// the reader — there is no second `LISTENER_PREFIX` constant here and no hand-rolled scan of
/// either prefix, because a second spelling is exactly how a launcher and its child drift
/// apart.
///
/// **Both elements are filtered, and the second one is why the filter is not a single
/// predicate.** The control element is a launcher's handoff like the listener's, so clap
/// must never see it: an unrecognised `tinio-sandbox-control-fd=…` would be a usage error
/// on every confined start.
///
/// # Examples
///
/// ```rust
/// use std::ffi::OsString;
///
/// use tinio_cli::sandbox::filter_argv;
///
/// // No listener element: this process is the launcher, and its stdin is never touched.
/// let argv = vec![OsString::from("server"), OsString::from("/srv/data")];
/// let (argv, adopted) = filter_argv(argv).expect("argv");
/// assert_eq!(argv.len(), 2);
/// assert!(adopted.is_none(), "no launcher put an element in this argv");
/// ```
pub fn filter_argv(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<Adopted>), Error> {
    adopt_argv(argv, io::stdin())
}

/// [`filter_argv`]'s body with the reader as a parameter — the seam a unit test drives.
///
/// **The reader is a parameter rather than the process's stdin, and that is a rule rather than
/// a convenience.** `adopt` reads `stdin` to end-of-stream whenever the listener element is in
/// argv, so a test that drove [`filter_argv`] with the element present would read whatever this
/// test binary inherited — a terminal, where it blocks until someone types an EOF. Nothing else
/// changes: the public entry point passes `io::stdin()`, which `adopt` leaves alone whenever the
/// element is absent (the launcher's ordinary case).
fn adopt_argv(
    argv: Vec<OsString>,
    stdin: impl Read,
) -> Result<(Vec<OsString>, Option<Adopted>), Error> {
    // `adopt` reads the elements out of the original list — it applies the two predicates
    // itself, and touches stdin only when the listener element is present — so it is called
    // **before** the elements are dropped from the argv clap will see.
    let adopted = adopt(argv.iter().cloned(), stdin)?;
    let filtered = argv
        .into_iter()
        .filter(|arg| !is_listener_fd_arg(arg) && !is_control_fd_arg(arg))
        .collect();
    Ok((filtered, adopted))
}

/// The sandbox channels, collapsed with FR-016's precedence: the flag and the variable (one
/// clap answer, `.env` included — the parse is two-pass), then the config file.
///
/// `source` is that answer's [`ValueSource`], and it is load-bearing rather than decorative:
/// `TINIO_SANDBOX=0` with `[server] sandbox = true` resolves **off**, which a plain `||` over a
/// `bool` could not express. Only clap's own default leaves the file's key in place.
///
/// # Examples
///
/// ```rust
/// use clap::parser::ValueSource;
/// use tinio_cli::sandbox::resolve_flag;
///
/// assert!(
///     resolve_flag(ValueSource::CommandLine, true, false),
///     "the flag wins"
/// );
/// assert!(
///     resolve_flag(ValueSource::EnvVariable, true, false),
///     "the variable answers next"
/// );
/// assert!(
///     resolve_flag(ValueSource::DefaultValue, false, true),
///     "the config file is the last level"
/// );
/// assert!(
///     !resolve_flag(ValueSource::EnvVariable, false, true),
///     "an explicit `false` wins over the file"
/// );
/// ```
pub fn resolve_flag(source: ValueSource, requested: bool, file: bool) -> bool {
    pick(source, Some(requested), Some(file)).unwrap_or(false)
}

/// The three **static** determinations, as §1 step f runs them — before the daemon fork, so
/// every refusal reaches the operator's shell.
///
/// No channel set answers "disabled" with an empty exposure set, and a channel that **is** set
/// is resolved in one place and then measured against the two facts this build cannot change:
///
/// - **the feature.** With `sandbox` off, `--sandbox` is not in the parser at all — so the
///   answer can only have come from `TINIO_SANDBOX` or `[server] sandbox`, and either is a
///   **usage** error (exit 2) naming the build: a stripped capability refuses the request rather
///   than silently ignoring it, because silently running unconfined is the failure the whole
///   feature exists to prevent (design decision 5).
/// - **the platform's backend.** With the feature on, [`BACKEND_IS_STUB`] is the only place that
///   records whether the backend for *this* target can confine anything, and while it can not,
///   the answer is a **startup** error (exit 1) naming `std::env::consts::OS`. It is raised
///   here rather than read off `Backend::confine`'s answer because that call cannot be asked:
///   `Backend` has one method and every platform says `Error::Unsupported` from inside it, while
///   on Linux the launcher arm is `#[cfg(any(target_os = "macos", target_os = "windows"))]` and
///   no `confine` call happens at all. The determination this runs *before* the fork is the same
///   one on every target, which is what makes it reachable and observable in every build.
///
/// Only a build that has both is allowed to answer an **enabled** [`Request`]; the exposure set at
/// the mode's access, plus the platform's own extras, is built in the platform arm [`confine`]
/// reaches rather than carried here.
///
/// # Errors
///
/// A usage error (2) for a channel this build cannot honour, a startup error (1) for a platform
/// whose backend is still a stub, and whatever reading the platform's own extras needs.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::start::Args,
///     resolve::{Resolution, locate, server},
///     sandbox::plan,
/// };
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let resolution: Resolution = server(located, &args, &Provenance::default())?;
/// let request = plan(&resolution, &args, &Provenance::default())?;
/// assert!(!request.enabled);
/// # Ok(())
/// # }
/// ```
pub fn plan(
    resolution: &Resolution,
    args: &Args,
    provenance: &Provenance,
) -> Result<Request, Error> {
    // FR-016's precedence, and nothing else rides on it. The flag and `TINIO_SANDBOX` are one
    // clap answer — the switch carries its variable through `env`, and a value `.env` supplied
    // arrives on the authoritative parse as an ordinary `EnvVariable` answer, which is what
    // makes `.env` the same channel one level lower — so the `ValueSource` is what names the
    // channel a refusal reports, and `[server] sandbox` is read only when clap answered nothing.
    let requested = resolve_flag(
        provenance.sandbox,
        args.sandbox_flag(),
        resolution.config.server.sandbox,
    );
    let channel = requested.then_some(match provenance.sandbox {
        ValueSource::CommandLine => Channel::Flag,
        ValueSource::EnvVariable => Channel::Env,
        _ => Channel::Config,
    });
    let Some(channel) = channel else {
        return Ok(Request { enabled: false });
    };
    // A request this build cannot honour is refused here, before any exposure set exists and
    // before the fork, so the operator's shell sees it. **What is left after the refusal is a
    // request this target can honour**: the feature is on and [`BACKEND_IS_STUB`] is `false`, and
    // [`confine`] is the arm that acts on it — the two are wired in the same edit, and a request
    // that reached a `confine` with nothing to do would serve unconfined.
    refuse_unhonourable(channel)?;
    Ok(Request { enabled: true })
}

/// The two facts this build cannot change, refused in one place so neither can be answered by
/// accident — the fail-closed half of [`plan`], and the only reader of [`BACKEND_IS_STUB`].
///
/// The messages are a matched pair and are written to be told apart by a test: the build's names
/// the build and never the platform, the platform's names `std::env::consts::OS` and never the
/// build. A leg that accepted either would pass just as well if the two were swapped, which is
/// why `cli_lifecycle.feature`'s variable scenario asserts each one's own words and the absence
/// of the other's (`crates/tinio-e2e/tests/features/cli_lifecycle.feature`).
fn refuse_unhonourable(channel: Channel) -> Result<(), Error> {
    // A channel this build has no feature for is a **usage** error: the request was well formed
    // and the answer is "not in this build" (design decision 5). This arm and the next are the
    // function's whole body, one cfg'd away at compile time — the same shape
    // `Args::sandbox_flag` uses, so no build has an unreachable tail.
    #[cfg(not(feature = "sandbox"))]
    {
        Err(Error::Usage(format!(
            "{} asked for a confinement, but this build has no `sandbox` feature: nothing would be confined, so the run is refused rather than served unconfined",
            channel.name()
        )))
    }
    // The platform's backend is a startup error: the argument was fine, the target cannot honour
    // it, and the message names which platform so the operator knows what the remedy is.
    #[cfg(feature = "sandbox")]
    {
        if BACKEND_IS_STUB {
            Err(Error::Operational(format!(
                "{} asked for a confinement, but this platform's `sandbox` backend (`{}`) is still a stub: nothing would be confined, so the run is refused rather than served unconfined",
                channel.name(),
                OS
            )))
        } else {
            // **The backend for this target is wired, so the request is honourable** and
            // [`confine`] is the arm that acts on it — the two are flipped in one edit, which is
            // what [`BACKEND_IS_STUB`]'s own doc promises. Refusing here as well would make the
            // confinement unreachable on every target that has one.
            Ok(())
        }
    }
}

/// Runs the confinement — §1 step i, the **dynamic** half, after the fork.
///
/// It contains no determination of its own: [`plan`] has already refused every request this
/// build cannot honour, so what is left is to rebuild what the [`Request`] carries and hand it to
/// the platform. Three values are assembled:
///
/// - the `Sandbox`, rebuilt through `Sandbox::from_exposures(exposures, device_files)` — the flag
///   travels as an argument rather than a re-derivation, which is the whole reason that
///   constructor takes two.
/// - the configuration's **text**, read from the file the resolution named and forwarded
///   unparsed. The launcher never builds a `Config`: it holds credential-bearing bytes for exactly
///   as long as it takes to hand them over (sandbox design decision 18), and the child re-derives
///   its answers from them.
/// - the **listener**, bound here so the plan owns it (sandbox design decision 16). The child
///   inherits a socket instead of the right to bind, which is what lets the platform's profile
///   or capability set drop that rule; the descriptor reaches the child in argv, spelled by the
///   facade's `listener_fd_arg`.
/// - `args` is threaded through to the launcher arms for one thing and nothing else: the
///   **control channel's** own handoff. The Windows launcher resolves the child's transport
///   selection from the configuration and the `--api*` flags (`start::api_options`, the one
///   definer of that rule), binds the local channel itself, and passes the handle — because a
///   process inside the container cannot create a pipe whose DACL names the user, and the child
///   would otherwise fail its own bind with a message about a second server. In-place platforms
///   read nothing here: Linux confines this process, which then binds its own socket.
///
/// The answer is the platform's: `Confinement::InPlace` is "this process is confined, carry on"
/// — Linux, whose `serve` then binds nothing and serves on what it has — and
/// `Confinement::Supervise(child)` is "a confined child is running", which the caller drives with
/// `tinio_sandbox::supervise`.
///
/// # Errors
///
/// A configuration the payload cannot be read from, a bind that fails, and every failure the
/// platform's own call reports — each fatal, because a confinement failure must not leave the
/// process running unconfined (design §11).
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_cli::{
///     commands::start::Args,
///     resolve::Resolution,
///     sandbox::{Request, confine},
/// };
/// use tinio_config::Config;
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let resolution = Resolution {
///     root: PathBuf::from("/srv/data"),
///     config: Config::default(),
///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
///     read_only: false,
///     state_dir: PathBuf::from("/srv/data/.tinio"),
///     config_error: None,
/// };
/// let args = Args::default();
/// // Nothing asked for the confinement: there is nothing to run.
/// let request = Request { enabled: false };
/// let child = confine(&resolution, &args, &request)?;
/// assert!(child.is_none());
/// # Ok(())
/// # }
/// ```
pub fn confine(
    resolution: &Resolution,
    args: &Args,
    request: &Request,
) -> Result<Option<Child>, Error> {
    if !request.enabled {
        return Ok(None);
    }
    // [`plan`] refuses every request this target cannot honour, so an enabled one is a target
    // whose backend is real:
    //
    // - **Linux** confines *this* process in place and answers "carry on" — `None`, so
    //   `start::run` goes on to its own storage, listener and readiness event (decision 6).
    // - **Windows** binds the listener and the control channel, builds the payload and launches
    //   the confined child — `Some(child)`, which the caller supervises and never serves behind.
    // - **Anything else** (macOS's stub arm, and every feature-off build) is unreachable behind
    //   `plan`'s refusal, and answered anyway rather than panicked: a library does not panic, and
    //   a request that fell through to here is served unconfined if this returns `Ok`.
    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    {
        // The in-place arm reads neither: it confines *this* process, which then binds its own
        // storage, listener and control channel as an ordinary run.
        let _ = args;
        linux::confine_in_place(resolution, AccessMode::from(resolution.read_only))?;
        Ok(None)
    }
    #[cfg(all(feature = "sandbox", target_os = "windows"))]
    {
        windows::confine_or_launch(resolution, args).map(Some)
    }
    #[cfg(not(all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))))]
    {
        // This arm reads neither: there is no backend to hand them to, which is the whole of
        // what it reports.
        let _ = (resolution, args);
        Err(Error::Operational(format!(
            "a confinement was requested, but no platform backend is wired to apply it on `{OS}`: \
             nothing would be confined, so the run is refused rather than served unconfined"
        )))
    }
}

/// The listener a confined child was handed by its launcher.
///
/// **A descriptor is not a socket**: turning one back into a `TcpListener` is the platform
/// crate's conversion — `from_raw_socket` on Windows — which is the one place that work may
/// live, because it is that crate's enumerated `unsafe` site and this crate adds none. On
/// Windows that is the facade's [`crate::_sandbox::adopted_listener`], called here; the child of
/// a launcher is the only process that ever reaches this with a real descriptor.
///
/// **On every other target the inherited element is a forgery, and the answer is a refusal**
/// rather than a fabricated listener. Linux confines its server **in place**, so no process
/// legitimately hands one over, and macOS's launcher has not landed — so an argv carrying the
/// listener element on those targets is something the operator or another process put there.
/// Binding the configured address instead would be the one silent answer available and the wrong
/// one: that socket is not the one the confinement's rules were written against (sandbox design
/// decision 16), so a process that was handed a descriptor is refused rather than served.
///
/// # Errors
///
/// The platform's own conversion failure, and — off Windows — always.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::sandbox::adopted_listener;
/// use tinio_sandbox::Adopted;
///
/// # fn listener(adopted: &Adopted) -> Result<(), tinio_cli::Error> {
/// let listener = adopted_listener(adopted)?;
/// # let _ = listener;
/// # Ok(())
/// # }
/// ```
pub fn adopted_listener(adopted: &Adopted) -> Result<TcpListener, Error> {
    #[cfg(target_os = "windows")]
    {
        let listener = _sandbox::adopted_listener(adopted)?;
        // **The inherited socket is blocking, and Windows' tokio cannot tell.** The launcher
        // binds with `std`'s listener, and a `std` socket is created non-blocking only on unix:
        // `mio`'s `from_std` "assumes nothing about the underlying listener", and tokio's own
        // guard (`check_socket_for_blocking`) is a no-op under `cfg(not(unix))`. Accepted
        // silently, the mode is inherited by every accepted connection and hyper's next read
        // blocks the worker thread inside `recv` instead of parking the task — the response is
        // never written (measured 2026-09-21). Set here, on the child's own copy, which is the
        // one it serves on.
        listener.set_nonblocking(true)?;
        Ok(TcpListener::from_std(listener)?)
    }
    #[cfg(not(target_os = "windows"))]
    {
        Err(Error::Operational(format!(
            "this process inherited listener descriptor {:?}, but this platform's confinement \
             never hands one over: refusing rather than serving on a socket this process did not \
             bind",
            adopted.listener()
        )))
    }
}

/// The child-side stop hook a confined child arms: where a stop its launcher forwards lands.
///
/// **The second of the two names the child side needs**, beside [`adopted_listener`] (design §3
/// step 9, decision 20's second half). Without it a forwarded stop meets the default
/// disposition, which is to die where the process stands instead of draining — and the launcher
/// forwards, rather than kills, precisely so a stop is a drain. The closure runs on a thread of
/// its own, so what it does may allocate and take locks: sending on the shutdown channel is what
/// it is for.
///
/// # Errors
///
/// The platform's own failure to install the relay.
#[cfg(target_os = "windows")]
pub(crate) fn on_stop(hook: impl Fn() + Send + 'static) -> Result<(), Error> {
    Ok(_sandbox::on_stop(hook)?)
}

/// The confinement request the arms make, built from the paths the operator gave and the mode
/// that was resolved for them.
///
/// **The exposure rule lives here and nowhere else** (design §9): the served root carries the
/// access the mode dictates, the state directory is always read-write — the access log, the redb
/// metadata and the staging areas live there — and an in-root state directory needs no entry of
/// its own, because the served root's exposure already covers it. `Sandbox::expose` keeps the
/// most permissive access for a path given twice, so two entries for one path could not fight
/// even if the rule above ever let them meet.
///
/// **"In-root" is [`path_contains`]'s answer and not a `Path::starts_with`'s**, and on Windows
/// that is the difference between an exposure set that holds and one that leaks: NTFS resolves
/// `e:\Data\.tinio` and `E:\Data\` to one directory while a literal component comparison calls
/// them unrelated, so a state directory the operator spelled in another case would be judged
/// outside the root and granted a **read-write** entry inside it — the hole this rule exists to
/// close, left open by the comparison the rule was once spelled with. The containment question
/// has one definer, which is what keeps this call site and the read-only refusal below from
/// answering it differently.
///
/// **One name for the construction, because the device-file flag is an argument rather than
/// something a rebuilt set remembers.** With the call inline in an arm and a test building its
/// own `Sandbox`, flipping the flag to `false` would leave every test green while the jail
/// silently lost `/dev/null` and friends — design decision 12's whole point, and invisible until
/// something reads a device file.
///
/// **The `.device_files()` below is a Linux concept and a no-op on Windows** (the exposures that
/// crate's backend computes are the whole whitelist), and it stays where it is: the flag is an
/// argument of the construction rather than something the set remembers, so the two arms share
/// one construction rather than each stating the flag (design decision 12).
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "windows")
))]
pub(crate) fn sandbox_request(root: &Path, state_dir: &Path, access: AccessMode) -> Sandbox {
    let sandbox = Sandbox::new().expose(root, access);
    let sandbox = if path_contains(state_dir, root) {
        sandbox
    } else {
        sandbox.expose(state_dir, AccessMode::ReadWrite)
    };
    sandbox.device_files()
}

/// Design §9's refused combination, refused in the launcher rather than inside the confinement:
/// a read-only root with a state directory under it cannot be served, and `FsStorage` would
/// reject it a process too late.
///
/// **Refused here because the launcher is what would have granted the wider access** — a
/// read-write entry inside a read-only root is a hole in the exposure set, not a storage error —
/// and it is the same containment question [`sandbox_request`] answers, through the same
/// definer: a state directory judged "outside" a root it is actually inside is exactly the case
/// this guard exists for, and on Windows two spellings of one directory are how that mistake
/// arrives (`TINIO_STATE_DIR=e:\Data\state` under a read-only `E:\Data`). A literal comparison
/// would let that run through to the exposure set, where the state directory would be granted
/// read-write inside a root the operator asked to be read-only.
///
/// **The two paths are the ones the operator wrote, not their canonical forms.** The refusal is
/// about the pair the run names — the root as the exposure set carries it, and `TINIO_STATE_DIR`
/// as written or the derived `<root>/.tinio` — because that is the pair the grant it prevents is
/// built from. A guard decided over canonical forms would be deciding about paths the exposure
/// set never sees.
///
/// **Nothing here is Windows-specific, and the gate says so** — it is the arms' gate widened by
/// `test` and narrowed by the `sandbox` feature. The example above names Windows because that is
/// where two spellings of one directory actually arrive, but the *rule* is the shared definer's,
/// and a guard under `cfg(target_os = "windows")` could be unit-tested on one host only — while
/// its call sites are both arms, which reach it with strings the operator wrote. Three pure
/// values in, a decision out.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "windows")
))]
pub(crate) fn refuse_in_root_state_dir(
    read_only: bool,
    root: &Path,
    state: &Path,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    if read_only && path_contains(state, root) {
        return Err(
            "read-only mode cannot serve a root that holds the state directory; set \
             TINIO_STATE_DIR outside the served root"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // `OS` is read only by the `sandbox`-feature leg of the refusal test below, so it is imported
    // only where that leg exists: a feature-off test build has no reader for it.
    #[cfg(feature = "sandbox")]
    use std::env::consts::OS;
    use std::{ffi::OsString, path::Path};

    use super::*;
    use crate::_config::Config;

    /// A resolution whose config asked for the confinement, with `root` and `state_dir` as given.
    fn requested(root: &Path, state_dir: &Path) -> Resolution {
        let mut config = Config::parse("version = 1\n[server]\nsandbox = true\n").expect("config");
        config.server.sandbox = true;
        Resolution {
            root: root.to_path_buf(),
            config,
            config_path: state_dir.join(".tinio.toml"),
            read_only: false,
            state_dir: state_dir.to_path_buf(),
            config_error: None,
        }
    }

    #[test]
    fn the_listener_element_is_taken_out_of_argv() {
        let argv = vec![
            OsString::from("server"),
            OsString::from("/srv/data"),
            OsString::from("tinio-sandbox-fd=7"),
            OsString::from("tinio-sandbox-control-fd=9"),
        ];
        // The reader is `adopt_argv`'s parameter rather than this binary's stdin: with the
        // element present, `adopt` reads the stream to its end, and a unit test that read the
        // inherited stdin would block on a terminal. The public signature is unchanged —
        // `filter_argv` passes `io::stdin()` — and this is the same body.
        let (filtered, _adopted) = adopt_argv(argv, io::empty()).expect("filter");
        assert_eq!(
            filtered,
            vec![OsString::from("server"), OsString::from("/srv/data")],
            "clap must never see either element"
        );
    }

    #[test]
    fn a_payload_on_the_element_is_adopted_and_the_element_is_gone() {
        // The other half of the same call: `adopt` is the facade's, so a decodable payload on
        // the stream is what travels out of here — never a hand-rolled scan of the prefix. The
        // two elements are the two inherited handles, and each is read for itself: the control
        // channel is the second descriptor a launcher hands over, and a child that lost it
        // would try to create a pipe its confinement forbids.
        let argv = vec![
            OsString::from("server"),
            OsString::from("/srv/data"),
            OsString::from("tinio-sandbox-fd=7"),
            OsString::from("tinio-sandbox-control-fd=9"),
        ];
        let (filtered, adopted) =
            adopt_argv(argv, io::Cursor::new(r#"{"config":"port = 9000"}"#)).expect("filter");
        assert_eq!(filtered.len(), 2, "both elements are out of argv");
        let adopted = adopted.expect("the payload decoded");
        assert_eq!(adopted.config(), "port = 9000");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(adopted.controls(), [9]);
    }

    #[test]
    fn a_pool_of_control_elements_is_filtered_the_same_way() {
        // **The pool, through the filter the CLI actually runs.** Every instance is one element
        // with the same prefix, and clap must never see any of them: the predicate is applied per
        // element, so a second (and third) control element is dropped by the same rule as the
        // first rather than needing a rule of its own.
        let argv = vec![
            OsString::from("server"),
            OsString::from("/srv/data"),
            OsString::from("tinio-sandbox-fd=7"),
            OsString::from("tinio-sandbox-control-fd=9"),
            OsString::from("tinio-sandbox-control-fd=13"),
        ];
        let (filtered, adopted) =
            adopt_argv(argv, io::Cursor::new(r#"{"config":"port = 9000"}"#)).expect("filter");
        assert_eq!(filtered.len(), 2, "every element is out of argv");
        let adopted = adopted.expect("the payload decoded");
        assert_eq!(
            adopted.controls(),
            [9, 13],
            "the child adopts the whole pool, in the launcher's order"
        );
    }

    #[test]
    fn every_channel_collapses_to_one_answer() {
        // The first argument is clap's answer's *source* — the flag, the variable (`.env`'s
        // value arrives as this one too: dotenvy puts it in the process environment before the
        // authoritative parse) or nothing at all — the second is that answer's value, and the
        // third is `config.server.sandbox`.
        assert!(
            !resolve_flag(ValueSource::DefaultValue, false, false),
            "nothing asked for it"
        );
        assert!(
            resolve_flag(ValueSource::CommandLine, true, false),
            "the flag"
        );
        assert!(
            resolve_flag(ValueSource::EnvVariable, true, false),
            "the variable"
        );
        assert!(
            resolve_flag(ValueSource::DefaultValue, false, true),
            "the config key"
        );
        assert!(
            !resolve_flag(ValueSource::EnvVariable, false, true),
            "the variable beats the config file"
        );
        assert!(
            !resolve_flag(ValueSource::CommandLine, false, true),
            "and so does a typed `false`"
        );
    }

    #[test]
    fn no_channel_is_a_disabled_request() {
        // The answer half: nothing asked for the confinement, so nothing is refused and
        // nothing is exposed.
        let resolution = Resolution {
            root: Path::new("/srv/data").to_path_buf(),
            config: Config::default(),
            config_path: Path::new("/srv/data/.tinio/.tinio.toml").to_path_buf(),
            read_only: false,
            state_dir: Path::new("/srv/data/.tinio").to_path_buf(),
            config_error: None,
        };
        let request = plan(
            &resolution,
            &Args::for_test(Path::new("/srv/data")),
            &Provenance::default(),
        )
        .expect("nothing to refuse");
        assert!(!request.enabled);
    }

    /// The value half of the ruling is the leg above; this is the *discrimination* half, and it
    /// is what decision 10 means by "the request names the channel it came from".
    ///
    /// **Two shapes, one rule.** Where no backend can honour the channel the request is refused,
    /// and the refusal's first words are the channel's own spelling — a typed flag names itself,
    /// the variable names itself, and a request nothing above the file made names the config key.
    /// Where this target's backend **is** wired ([`BACKEND_IS_STUB`] is `false`) the request is
    /// honoured instead, and the discrimination is the refusal's half alone: the [`Request`] is a
    /// plain "a confinement was asked for" and carries no channel.
    #[test]
    fn the_request_names_the_channel_it_came_from() {
        let root = Path::new("/srv/data");
        let state_dir = Path::new("/srv/data/.tinio");
        let bare = Args::for_test(root);
        let typed = Args {
            sandbox: true,
            ..Args::for_test(root)
        };
        for (args, source, named) in [
            (&typed, ValueSource::CommandLine, "`--sandbox`"),
            (&typed, ValueSource::EnvVariable, "`TINIO_SANDBOX`"),
            (&bare, ValueSource::DefaultValue, "`[server] sandbox`"),
        ] {
            let provenance = Provenance {
                sandbox: source,
                ..Provenance::default()
            };
            let answer = plan(&requested(root, state_dir), args, &provenance);
            #[cfg(not(feature = "sandbox"))]
            {
                let err =
                    answer.expect_err("a build with no `sandbox` feature never ignores a request");
                assert!(
                    format!("{err}").starts_with(named),
                    "the refusal names `{named}`: {err}"
                );
            }
            #[cfg(feature = "sandbox")]
            if BACKEND_IS_STUB {
                let err = answer.expect_err("a stub backend never ignores a request");
                assert!(
                    format!("{err}").starts_with(named),
                    "the refusal names `{named}`: {err}"
                );
            } else {
                let request =
                    answer.expect("this target's backend is wired, so the request is honoured");
                assert!(request.enabled, "`{named}` asked for a confinement");
            }
        }

        // An explicit `false` from either clap channel is a request **not made**, so the
        // config key's `true` is never reached and nothing is refused.
        let request = plan(
            &requested(root, state_dir),
            &bare,
            &Provenance {
                sandbox: ValueSource::EnvVariable,
                ..Provenance::default()
            },
        )
        .expect("an explicit `false` is not a request for a confinement");
        assert!(!request.enabled);
    }

    /// **The Linux arm is wired, and this leg is what keeps it from drifting back.**
    ///
    /// `commands::start::run` calls `sandbox::confine` on every target: in place on Linux, where
    /// the answer is `None` and the process carries on to serve. The flip of the Linux arm to
    /// `false` is therefore backed by an arm that acts on the request, and a future edit that
    /// gated that call away again would leave every Linux run that asks for a confinement
    /// starting normally and serving **unconfined** — the failure the whole feature exists to
    /// prevent.
    ///
    /// `clippy::assertions_on_constants` is allowed **deliberately, and this is the one place the
    /// lint's premise does not hold**: it reads a constant assertion as one that can never fail,
    /// while here the constant is the subject and a future edit is exactly what makes this leg
    /// fail. Nothing else is allowed for it.
    #[cfg(target_os = "linux")]
    #[allow(clippy::assertions_on_constants)]
    #[test]
    fn the_linux_arm_is_wired_because_a_confine_call_exists_on_this_target() {
        assert!(
            !BACKEND_IS_STUB,
            "the Linux arm of `BACKEND_IS_STUB` is `true`, so `plan` refuses every confinement \
             request on this target even though `tinio-cli/src/sandbox/linux.rs`'s in-place arm \
             is wired and `commands::start::run` reaches it: the constant is what promises \
             `confine` handles the target, and the two are flipped in one edit"
        );
    }

    /// A requested confinement is never silently ignored: it is **refused with its reason named**
    /// where nothing can honour it, and **honoured** where this target's backend is wired.
    ///
    /// The discrimination is the point — a build that answered with the wrong shape would be
    /// either silently permissive or misleading about what to fix — so the three shapes are
    /// asserted separately: a feature-off build names the build, a stub backend names the
    /// platform, and a wired backend carries the request through enabled.
    #[test]
    fn a_requested_confinement_is_refused_or_honoured_by_name() {
        // The channel here is the config key (`requested`'s fixture says `sandbox = true`, and
        // the provenance is every clap-backed key at its own default).
        let resolution = requested(Path::new("/srv/data"), Path::new("/srv/data/.tinio"));
        let answer = plan(
            &resolution,
            &Args::for_test(Path::new("/srv/data")),
            &Provenance::default(),
        );
        #[cfg(not(feature = "sandbox"))]
        {
            let err =
                answer.expect_err("a build with no `sandbox` feature never ignores a request");
            let text = format!("{err}");
            assert!(text.contains("sandbox"), "names the feature: {text}");
            assert!(
                text.contains("build"),
                "the feature-off refusal names the build: {text}"
            );
            assert!(
                !text.contains("platform"),
                "without the feature the platform is moot: {text}"
            );
            assert_eq!(err.exit_code(), 2, "a usage error naming the build");
        }
        #[cfg(feature = "sandbox")]
        if BACKEND_IS_STUB {
            let err = answer.expect_err("a stub backend refuses rather than serving unconfined");
            let text = format!("{err}");
            assert!(text.contains("sandbox"), "names the feature: {text}");
            assert!(text.contains("platform"), "the stub backend: {text}");
            assert!(
                text.contains(OS),
                "names the platform this build is on: {text}"
            );
            assert!(
                !text.contains("build"),
                "the feature is compiled in: {text}"
            );
            assert_eq!(err.exit_code(), 1, "a startup error, not a bad argument");
        } else {
            let request =
                answer.expect("this target's backend is wired, so the request is honoured");
            assert!(
                request.enabled,
                "`[server] sandbox` asked for a confinement"
            );
        }
    }
}
