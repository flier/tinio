//! The `--sandbox` seam: three channels, one decision, one confinement (design §7, decision 10).
//!
//! The three channels are the `--sandbox` flag, `TINIO_SANDBOX` and `[server] sandbox`, and they
//! collapse to one answer in [`plan`], which is also where every *static* refusal is raised —
//! before the fork, so the operator's shell sees it. [`confine`] is the *dynamic* half, after
//! the fork and before the runtime exists: on Linux this process enters the jail **in place**;
//! on Windows and macOS the listener is bound here and a confined child is launched to serve
//! on it.
//!
//! **This module is compiled in every build**, because the feature gates behaviour and not
//! linkage: `TINIO_SANDBOX` and `[server] sandbox` cannot be compiled away, and code that does
//! not exist cannot refuse. The feature takes the `--sandbox` *flag* and nothing else: the
//! variable keeps an argument of its own in that build (hidden, `start::Args::sandbox`'s
//! feature-off declaration), so the channel still reaches this module and still refuses.
//!
//! **The platform arms are submodules beside this file** — `linux`, `macos` and `windows` —
//! each gated by the feature *and* its target, so nothing here names a platform crate.

// The std control socket a macOS confined child adopts, beside the `std` listener a launcher
// binds: the plane takes the `std` type (`tinio_api::Plane::adopt`), and only the serving half
// converts one into tokio's.
#[cfg(target_os = "macos")]
use std::os::unix::net::UnixListener as StdUnixListener;
use std::{
    env::consts::OS,
    ffi::OsString,
    io::{self, Read},
};
// **The helper functions' gate, spelled here because their imports are held under it too.**
// `sandbox_request` and the two refusals beside it are the arms' own gate (`sandbox` *and* the
// platform) widened by `test` — which is what makes their pure rules unit-testable on a host with
// no arm — and narrowed by the feature: a feature-off graph has no arm to call them and no test
// that wants them, so it compiles neither the helpers nor the names only they use. Without that
// conjunct the imports below are unused in every `--no-default-features` graph, which is a
// `-D warnings` error the moment anyone lints one.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "macos", target_os = "windows")
))]
use std::{error::Error as StdError, path::Path};
// **The probe's own names, under the probe's own gate.** `refuse_unwritable_state_dir` is the one
// helper no target outside macOS compiles, so `fs`, `process` and the clock are named only where
// that helper or the host-runnable test that drives it exists. The wider gate above would leak
// three names into every Windows and Linux build, where `-D warnings` reads them as unused.
#[cfg(all(feature = "sandbox", any(test, target_os = "macos")))]
use std::{
    fs, process,
    time::{SystemTime, UNIX_EPOCH},
};

// Tokio's listener is the one this module's public surface names (`adopted_listener` hands the
// child's socket back to the serving half), and the standard library's is the one a launcher
// binds — a launcher binds before any runtime exists, which is §1's step i. Two types, one name
// each, and the alias is the honest spelling of "the std one".
use clap::parser::ValueSource;
use tokio::net::TcpListener;

/// The facade as a module, for the two child-side names it publishes **on the launcher platforms
/// only**: there is no `launch` module on the in-place platforms, so a direct import of
/// `adopted_listener`/`on_stop` would not resolve there — and this module already owns wrappers
/// under both names, so the call sites go through the module rather than shadowing them.
#[cfg(any(target_os = "macos", target_os = "windows"))]
use crate::_sandbox;
/// The platform's own **child-side step**, for the one process that has to apply its launcher's
/// frame to itself — the arm's crate, reached through the facade the way this file's other
/// platform calls are. Named as a module because the names the step speaks in live beside it: the
/// function that applies a frame, and the `Refused` a caller answers in its own words.
#[cfg(all(feature = "sandbox", target_os = "macos"))]
use crate::_sandbox::macos::apply;
// The same condition as the two helpers' own gate above, for the crate-side names they read.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "macos", target_os = "windows")
))]
use crate::{
    _core::mode::AccessMode,
    _sandbox::Sandbox,
    _util::path::{contains_under_any_spelling, path_contains},
};
use crate::{
    _sandbox::{Adopted, Child, adopt, is_launcher_fd_arg},
    Error,
    cli::Provenance,
    commands::start::Args,
    resolve::{Resolution, pick},
};

#[cfg(all(feature = "sandbox", any(target_os = "macos", target_os = "windows")))]
mod launch;
#[cfg(all(feature = "sandbox", target_os = "linux"))]
mod linux;
#[cfg(all(feature = "sandbox", target_os = "macos"))]
mod macos;
#[cfg(all(feature = "sandbox", target_os = "windows"))]
mod windows;

/// Whether this platform's backend is still a stub — `false` where [`confine`] can actually
/// confine, `true` where it cannot and a request must therefore be refused.
///
/// **A table here rather than a call into the backend**, because the backend cannot answer it
/// where the answer is needed: [`plan`] runs *before* [`confine`] by design (§1 step f before
/// step i), and on Linux the confinement is in-place, so there is no `confine` answer to read at
/// all. The flip is per target and one expression answers every one of them: **Linux** (P1),
/// **Windows** (P2) and **macOS** (P3) have all landed and each answers `false`, and any other
/// target — the other unixes, whose fallback is P4 — answers `true`, a stub, rather than failing to
/// name a value at all. The refusal below is what that `true` reaches.
///
/// **A `false` arm is a promise that [`confine`] handles that target**, and the two are wired in
/// the same edit: the arm is what lets [`plan`] return an enabled [`Request`], and [`confine`] is
/// what then has to act on it. Flipping one without the other is the failure this constant's own
/// refusal exists to catch — a request that passed `plan` and reached a `confine` with nothing to
/// do would serve **unconfined**. **macOS now delivers on that promise**: its arm is
/// `macos::confine_or_launch`, beside Windows's, and the two differ only in the backend they hand
/// the launch to.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::BACKEND_IS_STUB;
///
/// // Every target this workspace confines on says `false`, so a requested confinement is
/// // honoured rather than refused with the platform named. An arm flipped without its `confine`
/// // arm fails this, and the failure names the constant to update.
/// assert!(!BACKEND_IS_STUB);
/// ```
pub const BACKEND_IS_STUB: bool = !cfg!(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows"
));

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

/// Where the run's state directory came from: named by the operator (`TINIO_STATE_DIR`), or
/// derived by the resolution from the home directory.
///
/// **A value and not a boolean**, for [`Channel`]'s reason: what the refusal below reads is which
/// of the two channels answered, and a bare `!state_dir_given` at the call site reads as a
/// property of a path rather than of the parse that produced it. The distinction is the whole
/// rule — a *named* directory is exposed read-write wherever it points, so it is served; a
/// *derived* one lands under `<home>/.tinio`, which is the case macOS has no grant for.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::StateDirSource;
///
/// // The discovery parse's answer travels as one of two names, and the derived one is the case
/// // a read-only macOS confinement refuses.
/// assert!(StateDirSource::Derived.is_derived());
/// assert!(!StateDirSource::Named.is_derived());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateDirSource {
    /// An explicit `TINIO_STATE_DIR` named it.
    Named,
    /// No explicit variable: the resolution derived `<home>/.tinio/roots/<hash>/`.
    Derived,
}

impl StateDirSource {
    /// Whether the directory was derived rather than named — the case macOS cannot grant a
    /// parent for.
    pub fn is_derived(self) -> bool {
        matches!(self, Self::Derived)
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
    // **One pass, and one partition of it**: `is_launcher_fd_arg` is the facade's own predicate
    // over both prefixes, so the two lists it makes are exactly "the descriptor elements a
    // launcher handed over" and "the argv clap must see" — the first for `adopt`, the second for
    // the parse. The elements are **moved** into the arm each belongs to rather than cloned to
    // keep a copy of what is dropped: every `tinio` invocation goes through here, confined or
    // not, so the clone was paid on every run for the launcher's few elements.
    //
    // The partition is exact rather than convenient: `adopt` reads the two elements through the
    // same `listener_fd_from_arg`/`control_fd_from_arg` predicates `is_launcher_fd_arg` is the
    // disjunction of, so an element outside that arm is one `adopt` would have discarded too —
    // it touches stdin only when the listener element is present, which is what keeps a
    // launcher's own stdin out of this.
    let (launcher_args, filtered): (Vec<_>, Vec<_>) =
        argv.into_iter().partition(|arg| is_launcher_fd_arg(arg));
    let adopted = adopt(launcher_args, stdin)?;
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
///   `Backend` has one method, the arm that reaches it is per target — the launcher platforms'
///   `#[cfg(any(target_os = "macos", target_os = "windows"))]` call in `start::run`, Linux's
///   in-place one — and [`plan`] runs before all of them. The determination this runs *before*
///   the fork is the same one on every target, which is what makes it reachable and observable
///   in every build.
///
/// Only a build that has both is allowed to answer an **enabled** [`Request`]; the exposure set at
/// the mode's access, plus the platform's own extras, is built in the platform arm [`confine`]
/// reaches rather than carried here.
///
/// **The state directory's *channel* is read off the resolution, not taken as an argument**:
/// whether the discovery parse read an explicit `TINIO_STATE_DIR`
/// ([`crate::resolve::Located::state_dir_override`]'s presence) travels as a [`StateDirSource`] on
/// the [`Resolution`] that parse produced — [`Resolution::state_dir_source`] — so the one rule
/// that needs it (macOS's, below) reads it where it already is and no caller has to hand it
/// across the directory's consumption. A read-only confinement derives the directory from the
/// home when it was not named, and that derivation is one macOS cannot grant —
/// `refuse_underived_state_dir` states the rule and why the launcher is where it belongs.
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
    // A request this target **can** honour is still refused where the confinement it would build
    // cannot reach the state directory's parent — read-only with the directory derived rather than
    // named. It sits here, after the channel is known and before any exposure set or sandbox step,
    // for the same reason every refusal in this function does: the operator's shell sees it and
    // nothing is granted or launched. **The state directory itself may already exist** — the
    // resolution that produced this `Resolution` creates it — which is why the guard decides on
    // whether it was named rather than on whether it is there. The source comes off the resolution
    // rather than off a parameter: the parse that resolved the directory is the one that knew
    // whether it was named, and `Resolution` is where that answer survives the move. The gate
    // (`sandbox`) is the helpers' own; what narrows it further to the platform whose mechanism this
    // is lives in the guard's body, so the rule stays a pure function a unit test can drive.
    #[cfg(all(
        feature = "sandbox",
        any(test, target_os = "linux", target_os = "macos", target_os = "windows")
    ))]
    refuse_underived_state_dir(
        resolution.read_only,
        &resolution.root,
        &resolution.state_dir,
        resolution.state_dir_source(),
    )
    .map_err(|err| Error::Operational(err.to_string()))?;
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
/// - the configuration's **text**, carried on the resolution that read it
///   ([`Resolution::config_text`]) and forwarded unparsed. The launcher never builds a `Config`:
///   it holds credential-bearing bytes for exactly as long as it takes to hand them over (sandbox
///   design decision 18), and the child re-derives its answers from them. **It is not read here**:
///   a launcher that opened the file for itself would be taking a second answer to what the
///   document says, from a file that may have changed since the resolution ran.
/// - the **listener**, bound here so the plan owns it (sandbox design decision 16). The child
///   inherits a socket instead of the right to bind, which is what lets the platform's profile
///   or capability set drop that rule; the descriptor reaches the child in argv, spelled by the
///   facade's `listener_fd_arg`.
/// - `args` is threaded through to the launcher arms for one thing and nothing else: the
///   **control channel's** own handoff. Each launcher resolves the child's transport selection
///   from the configuration and the `--api*` flags (`start::api_options`, the one definer of that
///   rule), binds the local channel itself, and hands the child what it bound. **The two
///   platforms' reasons differ and the shape does not.** Windows' is identity: a process inside
///   the container cannot create a pipe whose DACL names the user, and the child would otherwise
///   fail its own bind with a message about a second server. macOS' is the profile: the confined
///   child's unix `bind()` is a network operation the profile grants nothing for, so a child that
///   bound its own control socket would die `Operation not permitted` — and the grant that used
///   to cover it permitted every bind, which is the grant this handoff removes. In-place platforms
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
///     config_text: String::new(),
///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
///     read_only: false,
///     state_dir: PathBuf::from("/srv/data/.tinio"),
///     state_dir_override: None,
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
    // - **macOS** binds the listener and the control channel, builds the payload and launches the
    //   confined child the same way — `Some(child)`: the control handoff is the profile's reason
    //   rather than the container's, and the child serves on both descriptors it inherited.
    // - **Anything else** (every feature-off build, and a target with no arm) is unreachable
    //   behind `plan`'s refusal, and answered anyway rather than panicked: a library does not
    //   panic, and a request that fell through to here is served unconfined if this returns `Ok`.
    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    {
        // The in-place arm reads neither: it confines *this* process, which then binds its own
        // storage, listener and control channel as an ordinary run.
        let _ = args;
        linux::confine_in_place(resolution, AccessMode::from(resolution.read_only))?;
        Ok(None)
    }
    #[cfg(all(feature = "sandbox", target_os = "macos"))]
    {
        macos::confine_or_launch(resolution, args).map(Some)
    }
    #[cfg(all(feature = "sandbox", target_os = "windows"))]
    {
        windows::confine_or_launch(resolution, args).map(Some)
    }
    #[cfg(not(all(
        feature = "sandbox",
        any(target_os = "linux", target_os = "macos", target_os = "windows")
    )))]
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

/// Confine **this** process, when its launcher's mechanism handed it that job.
///
/// **The child side of the in-process mechanism** (design decision 6): a process
/// whose launch payload carries a `SandboxSpec` applies it to itself here, before it
/// serves anything and before the runtime exists (decision 26). `Ok(())` is every
/// other process — a launcher, a `status` client, an unconfined run, and the child
/// of the mechanism that confines by wrapping its `exec`, whose confinement was
/// already applied to it at that exec.
///
/// **The step is the platform's** (`apply::self_confinement`), and the split is
/// deliberate: *which* mechanisms a child may apply, and what a payload naming the
/// `exec`-wrapping one means, is the platform crate's rule — one home, so the
/// harness that drives the same child asserts on the same answer rather than on a
/// copy of it. What is this function's is the **framing**: the `step =
/// "sandbox-init"` event an operator reads, and the refusal sentence that says what
/// a frame this process cannot honour means for the run.
///
/// **It fails closed, and that is the whole contract.** A child under a mechanism
/// that handed it a profile and could not apply it must not serve: the apply is
/// one-shot and irreversible, so a run that continued past a refusal would be a
/// server with **no** confinement under a launcher that reported one — the failure
/// this entire seam exists to prevent.
///
/// **The environment is never re-read** — the launcher already resolved the
/// mechanism, and a second reader would be a second answer.
///
/// **It is the facade's name for the platform's own** (design decision 2, §10), and
/// macOS is the only platform whose confinement a child applies: the in-place
/// platform confines inside `Backend::confine`, and the other launcher platform's
/// containment is fixed at process creation.
///
/// # Errors
///
/// [`Error::Operational`] when the apply is refused — carrying libSystem's own
/// diagnostic — and when the payload names a mechanism this build does not know or
/// one that cannot have produced such a payload.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::sandbox::apply_self_confinement;
/// use tinio_sandbox::Adopted;
///
/// // A launch that handed this process no confinement: nothing to apply.
/// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
/// apply_self_confinement(&adopted)?;
/// # Ok::<(), tinio_cli::Error>(())
/// ```
#[cfg(all(feature = "sandbox", target_os = "macos"))]
pub fn apply_self_confinement(adopted: &Adopted) -> Result<(), Error> {
    match apply::self_confinement(adopted) {
        // The helper's launch, and every process that is not a confined child.
        Ok(None) => Ok(()),
        // **What this process did, in the operator's own vocabulary** — the event the
        // platform cannot emit for it, because the log's `step` is this crate's contract (shared
        // §7) — with the counts the platform's apply is the one that knows.
        Ok(Some(applied)) => {
            tracing::info!(
                step = "sandbox-init",
                mechanism = %applied.mechanism,
                rules = applied.rules,
                params = applied.params,
                "this process confined itself"
            );
            Ok(())
        }
        // **The two refusals keep their own sentences** rather than the platform's: the platform
        // says what is wrong with the frame, and this crate says what it means for the run.
        Err(refused) => Err(Error::Operational(self_confinement_refused(&refused))),
    }
}

/// The refusal a payload this process cannot honour becomes: **the platform's own
/// diagnosis of the frame, and this process's own answer to it.**
///
/// A function rather than a literal at the call site for the reason the platform
/// crate's own refusals are: the message is a contract a test asserts, and one
/// spelling is what keeps the test and the message from drifting. **The two halves
/// are one sentence, and the seam is the colon**: the platform names which mechanism
/// the frame asked for and why this process may not apply it, and the tail is this
/// crate's — the apply is one-shot and irreversible, so a child that could not
/// confine itself must not serve, and what that means for the *run* is the CLI's to
/// say.
#[cfg(all(feature = "sandbox", target_os = "macos"))]
fn self_confinement_refused(refused: &apply::Refused) -> String {
    let tail = match refused {
        // A mechanism this build does not know: nothing was applied and nothing was confined,
        // because a frame this process cannot read is not a confinement to guess at.
        apply::Refused::UnknownMechanism { .. } => {
            "nothing was confined, so the run is refused rather than served unconfined"
        }
        // A frame naming the `exec`-wrapping mechanism: a launcher that resolved it cannot have
        // written this one, so the refusal is a disagreement between the two processes rather
        // than a confinement either of them chose.
        apply::Refused::WrapsTheExec { .. } => {
            "the launch is refused rather than served under a confinement neither process chose"
        }
        // **The kernel's own refusal is already the whole sentence** — the platform's apply
        // carries its `step` and libSystem's diagnostic — so this arm adds nothing to it, and the
        // `Refused`'s `Display` is transparent over it.
        apply::Refused::Apply(_) => return refused.to_string(),
    };
    format!("{refused}: {tail}")
}

/// The listener a confined child was handed by its launcher.
///
/// **A descriptor is not a socket**: turning one back into a `TcpListener` is the platform
/// crate's conversion — `from_raw_socket` on Windows, `BorrowedFd`/`OwnedFd` on macOS — which is
/// the one place that work may live, because it is that crate's enumerated `unsafe` site and this
/// crate adds none. On both launcher platforms that is the facade's
/// [`crate::_sandbox::adopted_listener`], called here; the child of a launcher is the only process
/// that ever reaches this with a real descriptor.
///
/// **On a target with no launcher the inherited element is a forgery, and the answer is a
/// refusal** rather than a fabricated listener. Linux confines its server **in place**, so no
/// process legitimately hands one over, and an argv carrying the listener element there is
/// something the operator or another process put there. Binding the configured address instead
/// would be the one silent answer available and the wrong one: that socket is not the one the
/// confinement's rules were written against (sandbox design decision 16), so a process that was
/// handed a descriptor is refused rather than served.
///
/// # Errors
///
/// The platform's own conversion failure, and — on a target with no launcher — always.
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
    // **Both launcher platforms need the same correction, for a cause of their own each.** The
    // inherited socket must be set non-blocking, on the child's own copy — the one it serves on —
    // before tokio registers it; the arm is one arm because the correction is identical where the
    // causes are not.
    //
    // Windows: the socket is blocking and tokio cannot tell. The launcher binds with `std`'s
    // listener, and a `std` socket is created non-blocking only on unix: `mio`'s `from_std`
    // "assumes nothing about the underlying listener", and tokio's own guard
    // (`check_socket_for_blocking`) is a no-op under `cfg(not(unix))`. Accepted silently, the mode
    // is inherited by every accepted connection and hyper's next read blocks the worker thread
    // inside `recv` instead of parking the task — the response is never written (measured
    // 2026-09-21).
    //
    // macOS: the socket is *not* blocking as `std` created it — a unix `std` socket is created
    // non-blocking — but this descriptor crossed an `execve` inside `sandbox-exec`, and tokio
    // refuses to register a descriptor it reads as blocking. Without the call the confined child
    // **panics** at the `from_std` below (measured 2026-09-25):
    //
    //     Registering a blocking socket with the tokio runtime is unsupported. If you wish to do
    //     anyways, please add --cfg tokio_allow_from_blocking_fd to your RUSTFLAGS. See
    //     github.com/tokio-rs/tokio/issues/7172 for details.
    //
    // and the launcher forwards the child's exit code `101`. The launcher's own half of the
    // handoff is the clear of `FD_CLOEXEC`, which is what lets the descriptor survive `execve` at
    // all.
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    {
        let listener = _sandbox::adopted_listener(adopted)?;
        listener.set_nonblocking(true)?;
        Ok(TcpListener::from_std(listener)?)
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        Err(Error::Operational(format!(
            "this process inherited listener descriptor {:?}, but this platform's confinement \
             never hands one over: refusing rather than serving on a socket this process did not \
             bind",
            adopted.listener()
        )))
    }
}

/// The control socket a launcher handed this confined child, rebuilt from the descriptor in its
/// argv.
///
/// **The child cannot bind its own control channel, and that is what this name is for.** A unix
/// `bind()` is a network operation the macOS profile grants nothing for — the grant it used to
/// carry was a bind grant rather than a filter, which is exactly what a `(deny default)` profile
/// exists to avoid — so the launcher binds the socket outside the sandbox, clears `FD_CLOEXEC` on
/// it, and puts the descriptor in argv; `tinio_api::Plane::adopt` is where the child serves on
/// what this returns. The rebuild is the platform's, and it is the inherited-descriptor rule
/// [`adopted_listener`] applies, so the facade publishes it under a platform-neutral name for the
/// same reason the listener's is (design decision 2, §10).
///
/// **One socket, and the platform's own refusal for anything else**: a unix control channel is one
/// socket file, so a handoff that carries no descriptor — or more than one — is refused rather
/// than served.
///
/// # Errors
///
/// The platform's own conversion failure: no inherited control descriptor, more than one, or a
/// number that does not fit the kernel's `RawFd`.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::sandbox::adopted_control;
/// use tinio_sandbox::Adopted;
///
/// # fn socket(adopted: &Adopted) -> Result<(), tinio_cli::Error> {
/// let socket = adopted_control(adopted)?;
/// # let _ = socket;
/// # Ok(())
/// # }
/// ```
#[cfg(target_os = "macos")]
pub fn adopted_control(adopted: &Adopted) -> Result<StdUnixListener, Error> {
    Ok(_sandbox::adopted_control(adopted)?)
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
/// **Both launcher platforms arm it**, because both forward the stop rather than killing the
/// child: Windows through its console-control handler, macOS through the signal relay. The two
/// differ in what arrives — a `CTRL_*` event there, a real signal here — and not in what the
/// child does with it.
///
/// # Errors
///
/// The platform's own failure to install the relay.
#[cfg(any(target_os = "macos", target_os = "windows"))]
pub(crate) fn on_stop(hook: impl Fn() + Send + 'static) -> Result<(), Error> {
    Ok(_sandbox::on_stop(hook)?)
}

/// The confinement request the arms make, built from the resolution the run produced and the mode
/// that was resolved for it — **the in-root refusal and the exposure set it stands in front of, as
/// one call**.
///
/// **The exposure rule lives here and nowhere else** (design §9): the served root carries the
/// access the mode dictates, the state directory is always read-write — the access log, the redb
/// metadata and the staging areas live there — and an in-root state directory needs no entry of
/// its own, because the served root's exposure already covers it. `Sandbox::expose` keeps the
/// most permissive access for a path given twice, so two entries for one path could not fight
/// even if the rule above ever let them meet.
///
/// **The guard is *in* the construction rather than a sentence in front of it.** A state directory
/// that is neither inside the root nor disjoint from it — the root's own ancestor — is judged
/// *outside* by [`exposures`], so the exposure set gains a **read-write entry naming that
/// ancestor**. The root is not the whole of what that entry covers: every sibling of the root
/// under the ancestor is exposed read-write too, which no part of the run asked for, and the entry
/// is wider than the root's own on a read-write run as well. On a read-only run the served root
/// itself is the other half — its read-only entry is covered by the ancestor's read-write one —
/// and [`refuse_in_root_state_dir`]'s mirror clause refuses the pair in **both** modes. That
/// refusal used to be a line each arm spelled for itself, with this function's doc as the only
/// thing holding the two together; now the only way to a `Sandbox` is through it, so no arm — and
/// no channel added later — can reach the exposure set without the refusal having been asked, and
/// [`exposures`] is private to this module for exactly that reason.
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
/// # Errors
///
/// [`refuse_in_root_state_dir`]'s — a read-only root that holds the state directory, or one the
/// state directory holds — as an operational failure (exit 1): the run is refused before any
/// exposure set exists and before the fork, because the entry the refusal stands in front of
/// would be a hole in the set rather than a storage error.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "macos", target_os = "windows")
))]
pub(crate) fn sandbox_request(
    resolution: &Resolution,
    access: AccessMode,
) -> Result<Sandbox, Error> {
    refuse_in_root_state_dir(
        access.is_read_only(),
        &resolution.root,
        &resolution.state_dir,
    )
    .map_err(|err| Error::Operational(err.to_string()))?;
    Ok(exposures(resolution, access))
}

/// [`sandbox_request`]'s exposure set with no refusal of its own — the half the guard stands in
/// front of, private so that the pair cannot be pulled apart at a call site.
///
/// **The `.device_files()` below is a Linux concept and a no-op on the other two arms** (the
/// exposures that crate's backend computes are the whole whitelist, and the macOS crate reads the
/// flag nowhere), and it stays where it is: the flag is an
/// argument of the construction rather than something the set remembers, so the two arms share
/// one construction rather than each stating the flag (design decision 12).
///
/// A test drives it to read **the entry [`refuse_in_root_state_dir`] exists to stand in front
/// of** — the ancestor's read-write exposure — which the guarded constructor can no longer hand
/// back, and a guard whose hole cannot be exhibited is a guard nobody can check.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "macos", target_os = "windows")
))]
fn exposures(resolution: &Resolution, access: AccessMode) -> Sandbox {
    let sandbox = Sandbox::new().expose(&resolution.root, access);
    let sandbox = if path_contains(&resolution.state_dir, &resolution.root) {
        sandbox
    } else {
        sandbox.expose(&resolution.state_dir, AccessMode::ReadWrite)
    };
    sandbox.device_files()
}

/// Design §9's refused combination, refused in the launcher rather than inside the confinement:
/// a read-only root **and a state directory nested with it** cannot be served, and `FsStorage`
/// would reject the in-root half a process too late. **The nesting has two directions, and only
/// one of them is read-only's**: a state directory *inside* the root is refused for a read-only
/// run alone (the in-root clause), while one that *holds* the root is refused on either run (the
/// mirror clause) — the mode's own paragraphs below say why each is what it is.
///
/// **Refused here because the launcher is what would have granted the wider access** — a
/// read-write entry inside a read-only root is a hole in the exposure set, not a storage error —
/// and it is the same containment question [`exposures`] answers, through the same
/// definer: a state directory judged "outside" a root it is actually inside is exactly the case
/// this guard exists for, and on Windows two spellings of one directory are how that mistake
/// arrives (`TINIO_STATE_DIR=e:\Data\state` under a read-only `E:\Data`).
///
/// **The mirror is the same hole from the other side, and it is refused in both modes.** A state
/// directory that **holds** the served root — a strict ancestor of it — is outside the root by
/// [`path_contains`], so [`exposures`]'s `else` branch exposes it **read-write**, and that
/// entry covers the served root *and everything beside it*: on a read-only run the operator's root
/// silently gains a writable subtree, which is the promise `--read-only` makes and the reason the
/// root is exposed at all, while on a read-write run the root's **siblings** under that ancestor
/// gain a write no part of the run asked for — the root's own entry, the wider one, reaches none
/// of them. One direction of the containment question is therefore not an answer at this seam —
/// the exposure set's judgement is deliberately one-directional, the ancestor is neither contained
/// nor disjoint, and this guard is the refusal that runs in front of the entry rather than a
/// second judgement inside [`exposures`].
///
/// **The mode narrows the in-root clause and nothing else, and that asymmetry is the whole
/// design.** A state directory *inside* the root is the run's normal one: in read-write mode the
/// resolution's answer **is** `<root>/.tinio`, and refusing it would refuse every read-write run
/// this project serves, so the early return sits **after** the in-root clause rather than in front
/// of both. The mirror clause is asked on either run, because the entry it stands in front of
/// names a path *above* the root and reaches paths no mode asked to expose. The earlier shape
/// returned `Ok` for the whole read-write mode, which left the mirror clause read-write-exempt
/// while [`exposures`] went on adding the ancestor's entry — the two sides of this seam
/// disagreeing.
///
/// **Every platform reaches the covered root by its own rule, and which half is measured is
/// stated rather than implied.** macOS: the exposure renders one `(allow file-read* file-write*
/// (subpath …))` rule per exposed path over a `(deny default)` base, Seatbelt's allow rules
/// union, and the read-write rule names a directory above the root's read-only one — the same
/// nesting as the measured `TINIO_STATE_DIR=/tmp/rw-hole/.tinio` case above, one level the other
/// way. Linux: the mount table is one recursive bind per exposure, in `Sandbox::exposures()`
/// order, and the root's read-only remount is performed before the ancestor's bind exists; a path
/// lookup then resolves to the topmost mount whose point contains it, so the later read-write
/// bind at the ancestor is what serves the nested path and the read-only bind under it is
/// shadowed (**read from `crates/tinio-sandbox-linux/src/{plan,enter}.rs`, not measured: no
/// Linux host was run for this pair**). Windows: an exposed directory is granted the inheritable
/// `(OI)(CI)M` form, and inheritance reaches descendants that already existed (§6's own
/// measurement), so the ancestor's ACE lands on the root and on everything under it beside the
/// root's own `(OI)(CI)RX` — access is the union, so `M` answers a write (**read from
/// `crates/tinio-sandbox-windows/src/{acl.rs,backend/mod.rs}`, not measured for this pair**).
///
/// **The equality case is decided once, and by the clause that already owned it.** The two
/// conditions are asked in order and [`path_contains`] counts a path as contained in itself, so
/// a state directory equal to the root never reaches the mirror clause: on a read-only run it is
/// the in-root clause's refusal with the sentence that pair already had, and on a read-write run
/// the in-root clause's early return accepts it — the pair is inert there, because a state
/// directory contained in the root adds no exposure entry at all. The mirror clause below
/// therefore sees strict ancestors alone, on either run.
///
/// **Both spellings of both paths are compared, and the narrower comparison is what failed to
/// guard.** The root arrives canonical — `resolve::resolve_root` canonicalises it before any
/// guard sees it — while the state directory is the operator's own spelling (`TINIO_STATE_DIR`
/// as typed) or a join onto the canonical root, so on macOS, where `/tmp` resolves to
/// `/private/tmp`, the pair `/tmp/<root>/.tinio` against `/private/tmp/<root>` has no literal
/// relationship at all: [`exposures`] below takes its `else` branch, the exposure set gains a
/// **read-write** entry, and the macOS arm's `with_canonical_paths` adds that entry's canonical
/// spelling while carrying the access flag over — so the rendered profile holds a read-write
/// `subpath` rule naming a directory inside a read-only root, and the served root silently
/// acquires a writable subtree. That is not
/// hypothetical and not Windows-only — measured on macOS 15.8, 2026-09-25, with
/// `tinio server /tmp/rw-hole --sandbox --read-only` and `TINIO_STATE_DIR=/tmp/rw-hole/.tinio`:
/// the run started, served, and grew `/private/tmp/rw-hole/.tinio/access.log` from 0 to 105
/// bytes with no `file-write` denial in the kernel log. `exposures`'s own comparison stays
/// the literal `path_contains(state_dir, root)`, and what bounds it is this guard rather than the
/// branch: its `else` only ever *adds* an exposure for a path it judges outside the root, and that
/// judgement is one-directional — the root's own ancestor is judged outside, which is the mirror
/// case above. This guard is the read-only promise for the in-root clause and the mirror refusal
/// for both modes, and it decides on the directory rather than on the spelling.
///
/// **The pairings compared are all four** — the literal state directory against the literal
/// root (what the narrow form did, and what still satisfies the Windows case-folding and
/// `./`-spelling legs `path_contains` owns), the canonical state directory against that root,
/// and each of those with the state directory's canonical form substituted for the root. Both
/// sides are canonicalised here rather than respelled once, because at this call site either one
/// can carry the symlink: the root is canonical in the product because `resolve_root` resolved
/// it, and the state directory carries the spelling the shell handed over.
///
/// **A path that does not exist yet never widens the answer, and it still resolves.** The state
/// directory is created by the serving chain before this guard runs, and its *leaf* — the config
/// file, the access log — is not, so a whole-path `canonicalize` answers `ENOENT` on exactly the
/// pair this guard is handed (measured on macOS 15.8, 2026-09-25: a first version that resolved
/// the leaf answered `false` for the exploit's own pair). The resolution therefore walks up to
/// the nearest existing ancestor and canonicalizes **that**, joining the components below it back
/// on — they do not exist, so they cannot be links, and the joined answer names the same
/// directory the leaf would. With no ancestor resolvable at all the literal spelling stands, which
/// is the narrow form's answer rather than a wider one.
///
/// **The rule has one owner, and every guard calls it.** The four pairings, the resolution behind
/// them and the per-pair comparison are
/// `tinio_util::path::contains_under_any_spelling`'s — the literal pair, the resolved candidate
/// against the literal root, the literal candidate against the resolved root, and both resolved —
/// so this guard names the question in both directions and nothing more. The two read-only guards
/// (`tinio-fs`'s and `tinio-config`'s, whose call sites reach the same function directly) ask it of
/// a root that is already canonical (`resolve_root`), and there the last two pairings repeat the
/// first two: the four-pairing set is exact for them rather than wider, which is why one function
/// serves all three call sites. **The `tinio-config` → `tinio-util` edge is what let its copy go**:
/// measured 2026-09-26, neither crate had named the other in its manifest in either direction, and
/// the edge that landed is `tinio-config` → `tinio-util`.
///
/// **Nothing here is Windows-specific, and the gate says so** — it is the arms' gate widened by
/// `test` and narrowed by the `sandbox` feature. Three pure values in, a decision out.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "macos", target_os = "windows")
))]
pub(crate) fn refuse_in_root_state_dir(
    read_only: bool,
    root: &Path,
    state: &Path,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    // **The in-root clause, and the only one the mode narrows.** A state directory inside the
    // served root is the run's own `<root>/.tinio` on a read-write run — the resolution's answer,
    // which must be served — so this clause refuses for read-only alone, and its early return is
    // what decides the **equal** pair too: `path_contains` counts a path as contained in itself,
    // so `state == root` is in-root here and never reaches the mirror clause. That pair adds no
    // exposure entry in either mode (the constructor's `if` skips it), which is why accepting it
    // read-write is inert rather than a hole.
    if contains_under_any_spelling(state, root) {
        if !read_only {
            return Ok(());
        }
        return Err(
            "read-only mode cannot serve a root that holds the state directory; set \
             TINIO_STATE_DIR outside the served root"
                .into(),
        );
    }
    // The mirror: the state directory's read-write entry names a path *above* the root, so it
    // covers the served root and the root's siblings under that ancestor — a widening the operator
    // asked for in neither mode — and this clause therefore refuses on both. It sees strict
    // ancestors alone: equality was answered above. A strict ancestor reaches here on a read-write
    // run as well, so the mode is named in the sentence rather than hardcoded read-only, and the
    // two clauses stay tellable apart by a test.
    if contains_under_any_spelling(root, state) {
        let mode = if read_only { "read-only" } else { "read-write" };
        return Err(format!(
            "{mode} mode cannot serve a root the state directory holds; set TINIO_STATE_DIR \
             outside the served root"
        )
        .into());
    }
    Ok(())
}

/// The state directory a read-only confinement **cannot grant a parent for**, refused in the
/// launcher rather than inside the confinement: with no explicit `TINIO_STATE_DIR` the state
/// directory is the home-derived `<home>/.tinio/roots/<hash>/`, whose creation needs
/// `file-write-mode` on `<home>/.tinio` — above the one entry [`sandbox_request`] exposes.
///
/// **Refused here because the launcher is what would have granted the access** (the neighbour's
/// reason, one level up): macOS exposes the *state directory itself* read-write and no rule spends
/// on a parent, so the only grant that would cover `<home>/.tinio` is `$HOME` — the grant this
/// project deliberately deleted (`docs/superpowers/specs/2026-09-18-sandbox-design.md` §5, its
/// `home` paragraph). The remedy is the
/// operator's `TINIO_STATE_DIR`, which names a directory the profile exposes whole; the refusal
/// names it rather than letting the child die on its own `EPERM`, which arrives as
/// `configuration error: failed to resolve the state directory at …: Operation not permitted` with
/// no way for the operator to tell that a grant was the missing thing.
///
/// **What decides is whether the state directory was named, not where it lands** — and *named*
/// rather than *exists*, because by the time this runs the launcher's own resolution has already
/// created the derived tree (measured 2026-09-25: a refused run leaves
/// `<home>/.tinio/roots/<hash>/.tinio.toml` behind). An explicit `TINIO_STATE_DIR` is exposed
/// read-write wherever it points, so it is served; deriving it is the case whose parent no entry
/// covers. [`refuse_in_root_state_dir`] answers the different question that pairs with this one — a
/// *named* state directory that happens to lie under the served root — and the two never fire
/// together: that guard needs the directory inside the root and this one needs it unnamed.
///
/// **macOS-only, checked in the body rather than by `cfg`, and the check is load-bearing.** The
/// mechanism is Seatbelt's: Linux confines in place *after* `start::run` has created the state
/// tree, and on Windows the launcher creates it as the user before the container child ever runs,
/// so a refusal there would reject a run whose own platform guide documents it working. A `cfg`
/// narrowed to `target_os = "macos"` would leave the rule untestable on every other host, which is
/// why the four arguments are pure values in and a decision out.
#[cfg(all(
    feature = "sandbox",
    any(test, target_os = "linux", target_os = "macos", target_os = "windows")
))]
pub(crate) fn refuse_underived_state_dir(
    read_only: bool,
    root: &Path,
    state: &Path,
    state_dir_source: StateDirSource,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    if cfg!(target_os = "macos") && read_only && state_dir_source.is_derived() {
        return Err(format!(
            "read-only mode confines `{root}` with the state directory derived from the home \
             directory (`{state}`), whose parent Seatbelt refuses `file-write-mode` on: the \
             confinement exposes the state directory itself and grants nothing for its parent. \
             Set an explicit, writable `TINIO_STATE_DIR` outside the served root",
            root = root.display(),
            state = state.display(),
        )
        .into());
    }
    Ok(())
}

/// The `state` publish's precondition, refused in the launcher rather than discovered after a
/// confined child is already serving: **the state directory must be able to take one new entry**,
/// proved by creating one and removing it before anything is launched.
///
/// **The failure this stands in front of leaves no serving process, and the probe is still the
/// cheaper half of the answer.** The launcher publishes `state` once the child reports it is
/// listening — that file is how `tinio status` and `tinio stop` find the run — and that publish is
/// the last fallible step with a child already running. **Since 2026-09-26 the launcher ends and
/// reaps that child before returning the publish error** (`Child::kill`, called by
/// `crate::sandbox::macos`'s `retract`), so the failure no longer leaves the Seatbelt-confined
/// server listening with nothing naming it — the platform's own accepted gap is a launcher killed
/// outright, which runs none of that code. What the probe still buys is the realistic arrival:
/// refusing a read-only, full or missing directory **here** costs a probe and a clear message while
/// no child exists, instead of a launched child that is then killed.
///
/// **Only a run that will publish asks, and the caller decides that.** A local management channel
/// and the token that guards it are what make a `state` file, so `--api http://…`,
/// `--no-api-unix` and every `api`-less build must never be refused for a directory they never
/// write. `macos.rs`'s call site sits in the arm that produces that pair — the channel it binds and
/// the token it mints — which is the same pair the publish at the end of that function reads, so
/// the run that never publishes cannot reach this probe at all.
///
/// **The probe is one `create_new` and one removal, and it creates no directory.** The state
/// directory is already there when this runs — `start::run`'s step h creates it before any
/// confinement, on every target — so a missing directory is a genuine refusal rather than
/// something to mint: creating it here would change what a refused run leaves behind, and it is
/// also what keeps the caller's *bind* from minting one, since a unix socket's parent is created
/// by the bind that this probe stands in front of. **Who makes the directory is that same step's**
/// — it hardens a derived read-only leaf with `tinio_config::state::prepare_home_derived` just
/// before the `create_dir_all` — and this probe only asks whether the directory that step produced
/// will take an entry; it neither creates nor hardens one. `create_new` rather than a plain create
/// is deliberate: a name already taken is refused instead of opened, so a symlink planted at the
/// probe's path is not followed and nothing this launcher did not create is truncated or removed.
/// The name carries the pid and the clock, so two probes — or one probe and a stale entry from a
/// process that is gone — cannot collide into a refusal of a directory that is in fact writable.
///
/// **What this does not close is a window, and it is named rather than claimed shut.** Between the
/// probe and the publish — which happens after the child reports `Listening`, seconds later — the
/// directory can fill, lose its writability, or be replaced. That publish is still the fallible
/// step with a child behind it, but it is no longer an unguarded one: the launcher kills and reaps
/// that child before it returns the publish error, and `macos.rs`'s `# Errors` says so. What the
/// probe removes is the realistic arrival — a read-only, full or absent state directory at startup,
/// refused while nothing has been confined — rather than the race.
///
/// # Errors
///
/// The create, which is the refusal the caller wants; or the removal of the entry the create made,
/// which is reported rather than swallowed because that branch is the one that leaves something
/// behind, and the message names it. The removal is attempted on every path that made an entry,
/// and no other failure can occur between the two calls.
#[cfg(all(feature = "sandbox", any(test, target_os = "macos")))]
pub(crate) fn refuse_unwritable_state_dir(
    state: &Path,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let probe = state.join(format!(
        "state-probe-{pid}-{stamp}",
        pid = process::id(),
        stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos()),
    ));
    if let Err(err) = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        return Err(format!(
            "the state directory `{state}` cannot take a new entry ({err}): the `state` file this \
             launcher publishes once the confined child is listening is written there, and a \
             publish that failed after the child exists would leave it serving undiscoverable — \
             this platform has no job object to end it. Set TINIO_STATE_DIR to a directory this \
             process can write",
            state = state.display(),
        )
        .into());
    }
    fs::remove_file(&probe).map_err(|err| {
        format!(
            "the state directory `{state}` would not give the probe entry `{probe}` back \
             ({err}): the entry is left there, and a `state` file that could not be removed is a \
             run `tinio status` would keep reporting. Remove it and set TINIO_STATE_DIR to a \
             directory this process can write",
            state = state.display(),
            probe = probe.display(),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    // `OS` is read only by the `sandbox`-feature leg of the refusal test below, so it is imported
    // only where that leg exists: a feature-off test build has no reader for it.
    #[cfg(feature = "sandbox")]
    use std::env::consts::OS;
    // **`fs` is every host's now; the other three are the unix legs'.** The probe test's residue
    // check reads a directory on any host, while the symlink and mode legs are unix's alone —
    // Windows has no directory mode for the permission leg to set, and `symlink` needs the unix
    // path — so gating all four together (as an earlier round did) is what would keep `fs` out of
    // this host's test build.
    #[cfg(feature = "sandbox")]
    use std::fs;
    use std::{ffi::OsString, path::Path};
    #[cfg(all(feature = "sandbox", unix))]
    use std::{
        fs::{OpenOptions, Permissions},
        os::unix::fs::{PermissionsExt, symlink},
    };

    use super::*;
    use crate::_config::Config;

    /// A resolution whose config asked for the confinement, with `root` and `state_dir` as given —
    /// and the state directory **named**, which is `TINIO_STATE_DIR`'s case.
    fn requested(root: &Path, state_dir: &Path) -> Resolution {
        let config_text = "version = 1\n[server]\nsandbox = true\n";
        let mut config = Config::parse(config_text).expect("config");
        config.server.sandbox = true;
        Resolution {
            root: root.to_path_buf(),
            config,
            config_text: config_text.to_owned(),
            config_path: state_dir.join(".tinio.toml"),
            read_only: false,
            state_dir: state_dir.to_path_buf(),
            state_dir_override: Some(state_dir.to_path_buf()),
            config_error: None,
        }
    }

    /// [`requested`] with **no** `TINIO_STATE_DIR` at all: the resolution derived the directory
    /// from the home — *underived* is the guard's own word for it — which is the case macOS has no
    /// grant for and the one [`refuse_underived_state_dir`] refuses on a read-only run.
    fn underived(root: &Path, state_dir: &Path) -> Resolution {
        Resolution {
            state_dir_override: None,
            ..requested(root, state_dir)
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
        // nothing is exposed. The config is the **default** one rather than [`requested`]'s,
        // which says `sandbox = true` — this leg is the one that has no channel at all.
        let resolution = Resolution {
            root: Path::new("/srv/data").to_path_buf(),
            config: Config::default(),
            config_text: String::new(),
            config_path: Path::new("/srv/data/.tinio/.tinio.toml").to_path_buf(),
            read_only: false,
            state_dir: Path::new("/srv/data/.tinio").to_path_buf(),
            state_dir_override: None,
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

    /// A read-only confinement over a root with no explicit `TINIO_STATE_DIR`: the state directory
    /// the run would derive is the one macOS has no grant for, so the launcher refuses it and names
    /// the remedy — while the same run **with** a named directory, and a read-only run that asks
    /// for no confinement at all, are honoured.
    ///
    /// **Read-only is spelled into the fixture rather than taken from [`requested`]**, whose
    /// config is a plain `sandbox = true`: the mode is the refused combination's first half, and a
    /// leg that left it out would assert the refusal on a read-write run — a run this guard must
    /// never touch, because its state directory is `<root>/.tinio`, which the served root's own
    /// exposure already covers. **The directory's channel is the fixture's other half**:
    /// [`underived`] is the resolution whose directory was derived, which is the fact the guard
    /// reads off the resolution — no parameter carries it here any more.
    ///
    /// **The refusal is asserted on macOS alone, and the other two shapes everywhere.** macOS is the
    /// one target whose mechanism refuses `file-write-mode` on `<home>/.tinio`; Linux creates the
    /// tree before it enters the jail and Windows creates it as the user before the container child
    /// runs, so refusing there would reject a run that works, and the accepted shapes are the legs
    /// that would catch it. [`refuse_underived_state_dir`] carries the rule; this is where it is
    /// driven through [`plan`].
    ///
    /// **Feature-gated with the guard, because a feature-off build never reaches it**: `plan`
    /// refuses *every* requested confinement with the build named (decision 5) before the guard's
    /// own gate is consulted, so this leg would read that refusal where it expects an enabled
    /// request. The `sandbox`-feature leg above is where a request is driven through `plan` on such
    /// a build.
    #[cfg(feature = "sandbox")]
    #[test]
    fn a_read_only_confinement_refuses_a_state_directory_it_must_derive() {
        let root = Path::new("/srv/data");
        let derived = Path::new("/home/svc/.tinio/roots/0123456789abcdef");
        let named = Path::new("/srv/state");
        // The run the task refuses, and the pair it is refused for: the root is served and the
        // state directory is *not* named, which is what makes `plan` the place the decision can be
        // taken at all. The mode is `resolution.read_only` — the value both channels of the chain
        // produced — and `args`/`provenance` carry only the confinement's own channels, which is
        // why nothing here sets a read-only flag.
        let (resolution, args, provenance) = {
            let mut resolution = underived(root, derived);
            resolution.read_only = true;
            (resolution, Args::for_test(root), Provenance::default())
        };
        let answer = plan(&resolution, &args, &provenance);
        if cfg!(target_os = "macos") {
            let err = answer.expect_err("macOS cannot grant the derived directory's parent");
            let text = format!("{err}");
            for part in [
                "read-only",
                "TINIO_STATE_DIR",
                "/srv/data",
                "/home/svc/.tinio/roots/0123456789abcdef",
            ] {
                assert!(
                    text.contains(part),
                    "the refusal must name `{part}`: {text}"
                );
            }
            assert_eq!(err.exit_code(), 1, "a startup error, not a bad argument");
        } else {
            let request =
                answer.expect("a read-only confinement this target can grant is honoured");
            assert!(
                request.enabled,
                "the refusal is macOS's mechanism and must not fire on {}",
                OS
            );
        }
        // Named, it is served: the state directory is always exposed read-write, so the operator
        // has a remedy that costs the confinement nothing.
        let mut named_resolution = requested(root, named);
        named_resolution.read_only = true;
        let request = plan(&named_resolution, &args, &provenance)
            .expect("an explicit writable TINIO_STATE_DIR is the remedy, not another refusal");
        assert!(request.enabled, "the confinement is still applied");
        // Unconfined, the same read-only run and the same derived directory are honoured: the CLI
        // is not always sandboxed, and this refusal is the launcher's.
        let mut unconfined = underived(root, derived);
        unconfined.read_only = true;
        unconfined.config.server.sandbox = false;
        let request = plan(&unconfined, &args, &provenance)
            .expect("a run that asks for no confinement is refused nothing");
        assert!(!request.enabled, "no channel asked for a confinement");
    }

    /// **The served root cannot silently acquire a writable subtree: the state directory is
    /// refused under either spelling of either path.** `root` reaches this guard canonical —
    /// `resolve::resolve_root` canonicalises it — while `state` is the operator's
    /// `TINIO_STATE_DIR` as their shell spelled it, so `/tmp/<root>/.tinio` against
    /// `/private/tmp/<root>` (macOS's `/tmp`) has no literal relationship, and the narrow
    /// `path_contains(state, root)` answered `false` for a directory that *is* inside the root.
    /// Measured on macOS 15.8, 2026-09-25, through the real binary: that run started, served,
    /// and wrote 105 bytes into `<canonical root>/.tinio/access.log` with no `file-write` denial
    /// in the kernel log.
    ///
    /// The explicit symlink is what makes the leg deterministic rather than dependent on a host
    /// whose `/tmp` happens to be one — and the two spellings of the *root* are asserted
    /// separately from the two of the state directory, because either side can be the symlinked
    /// one at this call site.
    ///
    /// **Feature-gated with the helpers it drives**, because a feature-off build compiles neither
    /// the guard nor the constructor: the module is in every build, but `refuse_in_root_state_dir`
    /// and its `exposures` half are the arms' own and exist only where an arm or a test can reach
    /// them — the same conjunct its newer siblings carry.
    #[cfg(all(feature = "sandbox", unix))]
    #[test]
    fn a_read_only_root_refuses_its_state_directory_under_either_spelling() {
        // The tempdir is held: dropping it would take the real tree with it.
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real");
        fs::create_dir_all(real.join(".tinio")).expect("the served root");
        let root = real.canonicalize().expect("canonicalize the served root");
        let link = dir.path().join("link");
        symlink(&real, &link).expect("symlink");

        // The exploit's pair, exactly: the canonical root the guard is handed, and the same
        // directory spelled through the link as `TINIO_STATE_DIR` is. The literal comparison is
        // asserted `false` first, so the leg cannot pass by accident on a host where the two
        // spellings coincide.
        let through_link = link.join(".tinio");
        assert!(
            !path_contains(&through_link, &root),
            "the leg is vacuous unless the narrow comparison answers `false`: {through_link:?} \
             vs {root:?}"
        );
        let err = refuse_in_root_state_dir(true, &root, &through_link)
            .expect_err("read-only cannot serve a root holding its state directory");
        assert!(
            format!("{err}").contains("read-only mode cannot serve a root"),
            "the refusal is the in-root guard's own: {err}"
        );

        // The mirror: a state directory spelled canonically under a root spelled through the
        // link. The same directory, so the same refusal — the guard decides on the directory
        // rather than on which side carries the link.
        let resolved_state = root.join(".tinio");
        assert!(!path_contains(&resolved_state, &link));
        refuse_in_root_state_dir(true, &link, &resolved_state)
            .expect_err("the root's spelling cannot decide the answer either");

        // And the guard still refuses what it refused before, through `path_contains`'s own
        // lexical rules: a `..` that stays inside the root names an in-root directory.
        refuse_in_root_state_dir(true, &root, &root.join("sub/../.tinio"))
            .expect_err("a lexically in-root spelling is still in-root");

        // **The other direction is the one that must stay open**, because widening a containment
        // guard past the question it asks refuses runs that work: a state directory beside the
        // root — created, so the canonical arm has something to resolve — is outside it.
        let beside = dir.path().join("state");
        fs::create_dir_all(&beside).expect("the state directory");
        refuse_in_root_state_dir(true, &root, &beside)
            .expect("a state directory beside the root is served, not refused");
        // The in-root pair stays accepted on a read-write run under **either** spelling, which is
        // the clause's early return rather than the mode's: `<root>/.tinio` is the state directory
        // every read-write run serves with.
        refuse_in_root_state_dir(false, &root, &through_link).expect(
            "a read-write run serves its own in-root state directory, whatever the spelling",
        );
    }

    /// **The mirror of the guard above: a state directory that holds the served root is refused,
    /// in either mode, and the exposure constructor is why.** `path_contains(state, root)` is
    /// `false` for an ancestor, so [`exposures`] judges it *outside* the root, takes its
    /// `else` branch and adds a **read-write** entry naming the ancestor — measured on macOS 15.8,
    /// 2026-09-26, through the real binary and re-run for this change: `TINIO_STATE_DIR=/tmp/knl1
    /// tinio server /tmp/knl1/root --read-only --sandbox` served, and re-applying the profile the
    /// launcher rendered, with the launcher's own `-D` values
    /// (`E0=/private/tmp/knl1/root`, `E1=/tmp/knl1`, `E2=/private/tmp/knl1`), made
    /// `touch /private/tmp/knl1/root/knl-anc2.txt` answer `rc=0` with the file there afterwards.
    /// The disjoint control renders the same three exposure rules and the same write is refused
    /// `Operation not permitted`, which is what isolates the ancestor relationship as the cause.
    ///
    /// **The width that entry has is the reason the clause is not read-only's alone**: it names a
    /// directory *above* the root, so it reaches the root's **siblings** as well — a write no part
    /// of a read-write run asked for either — and this test asserts the refusal on both modes. What
    /// genuinely is read-only's is the in-root clause, because `<root>/.tinio` is the state
    /// directory a read-write run serves with; the accepted shapes below therefore keep the
    /// descendant (and the equal pair) on a read-write run and move only the ancestor.
    ///
    /// **Feature-gated with the helpers it drives**, because a feature-off build compiles neither
    /// the guard nor the constructor: the module is in every build, but `refuse_in_root_state_dir`
    /// and its `exposures` half are the arms' own and exist only where an arm or a test can reach
    /// them.
    #[cfg(feature = "sandbox")]
    #[test]
    fn a_state_directory_that_holds_the_root_is_refused_in_either_mode() {
        let root = Path::new("/srv/data");

        // The ancestor: neither inside the root nor disjoint from it, and the pair the mirror
        // clause exists for. The refusal names the remedy as the in-root one does, and the mode it
        // names is the one that ran.
        let err = refuse_in_root_state_dir(true, root, Path::new("/srv"))
            .expect_err("read-only cannot serve a root the state directory holds");
        let text = format!("{err}");
        assert!(
            text.contains("read-only mode cannot serve a root the state directory holds"),
            "the mirror clause's own sentence: {text}"
        );
        assert!(
            text.contains("set TINIO_STATE_DIR outside the served root"),
            "the refusal names the remedy: {text}"
        );

        // **The same pair on a read-write run is refused too, with the same remedy.** The entry the
        // constructor would add is wider than the root's own — it reaches every sibling of the root
        // under the ancestor — so the root's wider access is not what makes it safe.
        let read_write = format!(
            "{}",
            refuse_in_root_state_dir(false, root, Path::new("/srv"))
                .expect_err("a read-write run may not expose the root's siblings either")
        );
        assert!(
            read_write.contains("read-write mode cannot serve a root the state directory holds"),
            "the mirror clause names the mode it fired on: {read_write}"
        );
        assert!(
            read_write.contains("set TINIO_STATE_DIR outside the served root"),
            "and the same remedy the read-only legs use: {read_write}"
        );

        // **Equality is decided once, and by the clause that already owned it.** Both conditions
        // would answer `true` for the equal pair, and the in-root clause is asked first, so what
        // comes back is the sentence that pair has always carried rather than a second refusal.
        let equal = format!(
            "{}",
            refuse_in_root_state_dir(true, root, root)
                .expect_err("a state directory equal to the root is in-root")
        );
        assert!(
            equal.contains("a root that holds the state directory"),
            "equality fires the in-root clause once: {equal}"
        );

        // The earlier round's refusal stays, unchanged.
        let inside = format!(
            "{}",
            refuse_in_root_state_dir(true, root, Path::new("/srv/data/.tinio"))
                .expect_err("a state directory under the root is still refused")
        );
        assert!(
            inside.contains("a root that holds the state directory"),
            "a descendant is the in-root clause's, not the mirror's: {inside}"
        );

        // The accepted shapes, and they are the in-root clause's alone: a sibling, the equal pair on
        // a read-write run (the in-root clause's early return — the pair adds no entry at all), and
        // the descendant a read-write run really serves with.
        refuse_in_root_state_dir(true, root, Path::new("/srv/state"))
            .expect("a state directory beside the root is served");
        refuse_in_root_state_dir(false, root, root)
            .expect("an equal pair is in-root, so a read-write run accepts it and adds nothing");
        refuse_in_root_state_dir(false, root, Path::new("/srv/data/.tinio"))
            .expect("and a descendant is the run's own state directory");

        // **The refusal runs in front of a construction that would really carry the entry**, and
        // both halves are read rather than argued: the guarded constructor refuses the pair, and
        // the exposure set it refuses to build is the one that names the ancestor read-write — the
        // entry whose `subpath` covers the read-only root. The set is [`exposures`]'s because the
        // guarded call can no longer hand it back, which is exactly what folding the guard into
        // that call means; the ancestor pair is the one no arm can reach a `Sandbox` with.
        let ancestor = Path::new("/srv");
        sandbox_request(&requested(root, ancestor), AccessMode::ReadOnly)
            .expect_err("the guarded construction refuses the pair the entry would come from");
        let would_carry = exposures(&requested(root, ancestor), AccessMode::ReadOnly);
        let carried = would_carry.exposures();
        assert_eq!(carried.len(), 2, "the root and its ancestor: {carried:?}");
        assert_eq!(carried[0].host, root.to_path_buf());
        assert_eq!(carried[0].access, AccessMode::ReadOnly);
        assert_eq!(carried[1].host, Path::new("/srv").to_path_buf());
        assert_eq!(
            carried[1].access,
            AccessMode::ReadWrite,
            "the entry that covers the read-only root"
        );

        // **And the same construction on a read-write run carries the same ancestor entry**, which
        // is the half that used to be reached: the root's own exposure is read-write here, so the
        // entry above is not wider *for the root* — it is wider for every sibling of the root under
        // that ancestor, and that is the widening the refusal in front of it now answers.
        let read_write_carry = exposures(&requested(root, ancestor), AccessMode::ReadWrite);
        let read_write_exposures = read_write_carry.exposures();
        assert_eq!(
            read_write_exposures.len(),
            2,
            "the same two paths: {read_write_exposures:?}"
        );
        assert_eq!(
            read_write_exposures[1].host,
            Path::new("/srv").to_path_buf()
        );
        assert_eq!(
            read_write_exposures[1].access,
            AccessMode::ReadWrite,
            "the ancestor entry a read-write run would carry too"
        );
    }

    /// The mirror under the two spellings, which is the half a literal comparison cannot see:
    /// the state directory is the **ancestor** spelled through a link while the root is
    /// canonical, so neither literal containment answer is `true` and only the resolved pairings
    /// refuse it.
    #[cfg(all(feature = "sandbox", unix))]
    #[test]
    fn a_state_directory_spelled_through_a_link_still_holds_the_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real");
        fs::create_dir_all(real.join("root")).expect("the served root");
        let root = real
            .join("root")
            .canonicalize()
            .expect("canonicalize the served root");
        let link = dir.path().join("link");
        symlink(&real, &link).expect("symlink");

        // One directory, two spellings, and the literal pairing answers `false` in both
        // directions — so a `true` from the guard can only come from a resolved pairing.
        assert!(
            !path_contains(&root, &link),
            "the literal clause is vacuous here unless it answers `false`: {root:?} vs {link:?}"
        );
        assert!(
            !path_contains(&link, &root),
            "and the other literal direction too: {link:?} vs {root:?}"
        );
        let err = refuse_in_root_state_dir(true, &root, &link)
            .expect_err("the link names the directory that holds the root");
        assert!(
            format!("{err}").contains("the state directory holds"),
            "the mirror clause is the one that fires: {err}"
        );

        // **The mirror clause is asked on a read-write run too, and the resolved pairing is what
        // answers it there as well** — this is the leg the read-write exemption used to accept,
        // asserted on the spelling that only the canonical arm can see.
        let read_write = format!(
            "{}",
            refuse_in_root_state_dir(false, &root, &link)
                .expect_err("read-write mode's ancestor entry reaches the root's siblings too")
        );
        assert!(
            read_write.contains("read-write mode cannot serve a root the state directory holds"),
            "and the read-write sentence is the mirror clause's: {read_write}"
        );

        // A sibling of the served root, in the same scratch tree: outside it under either
        // spelling, so the widened guard is not refusing everything under the state directory.
        let beside = dir.path().join("state");
        fs::create_dir_all(&beside).expect("the state directory");
        refuse_in_root_state_dir(true, &root, &beside)
            .expect("a state directory beside the root is served, not refused");
    }

    /// **The state directory is proved able to take the `state` entry before any child that needs
    /// it exists.** This is the half of the macOS launcher's publish guard a host without Seatbelt
    /// can hold: `refuse_unwritable_state_dir` is the whole decision, and `macos.rs` — macOS-only,
    /// so not compiled here — is only its call site, placed ahead of `backend.confine`. What the
    /// *call site* adds and this host cannot check is that placement, and the window the helper's
    /// doc names: a directory writable here and unwritable seconds later is the TOCTOU case no
    /// probe closes.
    ///
    /// Three legs, on the hosts each is real on: a writable directory takes the probe and keeps no
    /// residue; a path that is not a directory refuses; and — unix only, because Windows has no
    /// directory mode a create honours — a directory the process cannot write refuses. The last is
    /// asserted **only where the host enforces the mode it set**, because a container running
    /// these tests as root would otherwise fail the leg for the wrong reason and pass it for none.
    #[cfg(feature = "sandbox")]
    #[test]
    fn a_state_directory_is_proved_writable_before_a_child_can_need_it() {
        let dir = tempfile::tempdir().expect("tempdir");

        // The success leg, on every host: the entry is made and taken back out.
        refuse_unwritable_state_dir(dir.path()).expect("a writable state directory is served");
        let residue: Vec<_> = fs::read_dir(dir.path())
            .expect("read the state directory")
            .map(|entry| entry.expect("an entry").file_name())
            .collect();
        assert!(
            residue.is_empty(),
            "the probe must leave no entry behind: {residue:?}"
        );

        // The refusal leg, on every host: a state directory that is not there. `start::run` creates
        // it before any confinement, so this is the setup that never ran rather than a directory to
        // mint — and the probe must not mint it.
        let absent = dir.path().join("absent");
        let err = refuse_unwritable_state_dir(&absent).expect_err("an absent directory refuses");
        let text = format!("{err}");
        assert!(
            text.contains("cannot take a new entry"),
            "the refusal says what failed: {text}"
        );
        assert!(
            text.contains("Set TINIO_STATE_DIR"),
            "and names the remedy: {text}"
        );
        assert!(
            !absent.exists(),
            "the probe creates no directory on the refusal path either"
        );

        // The realistic leg, where a host can hold it: a directory whose mode refuses a create.
        #[cfg(unix)]
        {
            let locked = dir.path().join("locked");
            fs::create_dir(&locked).expect("the locked directory");
            fs::set_permissions(&locked, Permissions::from_mode(0o555)).expect("the mode");
            // Whether the host enforces the mode is asked of the filesystem rather than assumed.
            let enforced = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(locked.join("enforced"))
            {
                // A host that does not enforce it — a root container, or a filesystem that ignores
                // the bits — has no refusal for this leg to assert.
                Ok(file) => {
                    drop(file);
                    fs::remove_file(locked.join("enforced")).expect("the check's own entry");
                    false
                }
                Err(_) => true,
            };
            if enforced {
                let err = refuse_unwritable_state_dir(&locked)
                    .expect_err("a state directory this process cannot write refuses");
                let text = format!("{err}");
                assert!(
                    text.contains("cannot take a new entry")
                        && text.contains("Set TINIO_STATE_DIR"),
                    "the unix refusal is the other legs' shape: {text}"
                );
            }
            fs::set_permissions(&locked, Permissions::from_mode(0o755)).expect("restore the mode");
        }
    }
}
