//! `tinio service` — the supervisor's half of running a server (design decision 22).
//!
//! `service-manager` speaks systemd, launchd and the SCM; the one thing it does not do is
//! carry the unit *we* need, so the systemd unit travels in `ServiceInstallCtx::contents`
//! (its default template writes `Type=simple`, under which our readiness and watchdog
//! notifications are inert). The Windows arm additionally hands the process to
//! `windows-service`'s dispatcher, which is the only way the SCM can start us at all.
//!
//! **Every verb here is synchronous and builds no runtime**, which is why `run` dispatches
//! them outside `on_runtime` (its `pub(crate)` helper): installing, uninstalling, starting,
//! stopping and querying a service talks to the OS and to no server. The one exception is the
//! SCM's own entry point — that arm *is* the server, and it builds the process's one runtime.

use std::{
    env,
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
};

use clap::{ArgAction, ValueEnum};
use service_manager::{
    ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager, ServiceManagerKind,
    ServiceStartCtx, ServiceStatusCtx, ServiceStopCtx, ServiceUninstallCtx, TypedServiceManager,
};
use sha2::{Digest, Sha256};

#[cfg(unix)]
use crate::notify;
use crate::{
    Error,
    commands::{EnvArgs, parse_switch},
    resolve,
};

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::try_run_as_service;

/// The service's name, in one place: `windows-service`'s dispatcher entry point takes it, and
/// `notify.rs` does not declare a second copy (Task 6).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::service::SERVICE_NAME;
///
/// assert_eq!(SERVICE_NAME, "tinio");
/// ```
pub const SERVICE_NAME: &str = "tinio";

/// The five verbs the contract's users need.
///
/// # Examples
///
/// ```rust
/// use clap::ValueEnum;
/// use tinio_cli::service::Verb;
///
/// assert!(matches!(Verb::from_str("query", false), Ok(Verb::Query)));
/// ```
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Verb {
    /// Register the service with the platform's supervisor.
    Install,
    /// Remove it.
    Uninstall,
    /// Start it.
    Start,
    /// Stop it.
    Stop,
    /// Report its state.
    Query,
}

impl Verb {
    /// The verb as the operator types it — the word every refusal names, so the message and
    /// the command cannot drift.
    fn name(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Uninstall => "uninstall",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Query => "query",
        }
    }

    /// What the four acting verbs report once the backend accepted them: the past tense of
    /// [`name`](Self::name), spelled out because English is not going to do it for us.
    fn done(self) -> &'static str {
        match self {
            Self::Install => "installed",
            Self::Uninstall => "uninstalled",
            Self::Start => "started",
            Self::Stop => "stopped",
            Self::Query => "queried",
        }
    }
}

/// `tinio service`: the supervisor's own verbs.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     service::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "service", "query", "/srv/data"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Service(args) = cli.command else {
///     panic!("the service arm");
/// };
/// assert!(matches!(args.verb, Verb::Query));
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The verb to run.
    #[arg(value_name = "VERB")]
    pub verb: Verb,
    /// The storage root; absent: walk up from the current directory to the nearest ancestor
    /// containing `.tinio/`.
    #[arg(value_name = "DIR")]
    pub dir: Option<PathBuf>,
    /// The service's label (default: derived from the root, so several roots are several
    /// services).
    #[arg(long, value_name = "NAME")]
    pub name: Option<String>,
    /// Install at the user level (a systemd user unit or a launchd agent; no elevation).
    #[arg(long)]
    pub user: bool,
    /// The mode's environment channel (`TINIO_READ_ONLY`), which is what the installed unit's
    /// argv and `WorkingDirectory=` are resolved from.
    ///
    /// **Hidden, and the environment is why**: this command's flag table is `VERB [DIR]
    /// [--name] [--user]` (`contracts/cli.md:109`), and its mode channel is the environment's
    /// (`:131`) — the flag exists so the variable has one declaration, not so that it is typed.
    #[arg(
        long,
        env = "TINIO_READ_ONLY",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "true",
        action = ArgAction::Set,
        default_value_t = false,
        hide = true,
    )]
    pub read_only: bool,
    /// The environment-only channels. `service` reads no configuration file, so the credential
    /// pair is inert here; the state-dir override is what `WorkingDirectory=` is resolved from.
    #[command(flatten)]
    pub env: EnvArgs,
}

/// The service's label: derived from the root so several roots can be several services.
///
/// The input is the **canonical** root — `resolve::resolve_root` canonicalises, and every
/// caller passes what it returned — so `tinio service install ./data` and
/// `tinio service install /abs/data` name one service rather than two. The digest is
/// `sha2::Sha256` over the canonical path's bytes, hex-encoded and truncated to 16
/// characters, the same shape the control channel's own name uses.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::service::label_for;
///
/// let label = label_for(Path::new("/srv/data"), None);
/// assert!(label.starts_with("tinio-"));
/// assert_eq!(label.len(), "tinio-".len() + 16);
/// assert_eq!(label_for(Path::new("/srv/data"), Some("mine")), "mine");
/// ```
pub fn label_for(root: &Path, name: Option<&str>) -> String {
    if let Some(name) = name {
        return name.to_string();
    }
    let digest = Sha256::digest(root.as_os_str().as_encoded_bytes());
    format!("tinio-{}", hex::encode(&digest[..8]))
}

/// The level `--user` selects, in one place — the manager carries it, not the context.
///
/// # Examples
///
/// ```rust
/// use service_manager::ServiceLevel;
/// use tinio_cli::service::level_for;
///
/// assert_eq!(level_for(false), ServiceLevel::System);
/// assert_eq!(level_for(true), ServiceLevel::User);
/// ```
pub fn level_for(user: bool) -> ServiceLevel {
    if user {
        ServiceLevel::User
    } else {
        ServiceLevel::System
    }
}

/// The argv a supervised `server` is installed with.
///
/// Read-write state is `<root>/.tinio`, which the server re-derives from the root alone, so
/// the argv is `server <DIR>`. Read-only state is `TINIO_STATE_DIR` or the installing user's
/// home, and a supervisor starts the process with neither — `resolve_state_dir` does not read
/// the working directory — so the path install already resolved is recorded as `--state-dir`.
/// `WorkingDirectory=` stays that same path, for relative paths, and is not a second answer.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::service::server_argv;
///
/// let read_write = server_argv(Path::new("/srv/data"), false, Path::new("/srv/data/.tinio"));
/// assert_eq!(read_write, ["server", "/srv/data"]);
/// let read_only = server_argv(Path::new("/srv/data"), true, Path::new("/var/lib/tinio"));
/// assert_eq!(
///     read_only,
///     [
///         "server",
///         "/srv/data",
///         "--read-only",
///         "--state-dir",
///         "/var/lib/tinio"
///     ]
/// );
/// ```
pub fn server_argv(root: &Path, read_only: bool, state_dir: &Path) -> Vec<OsString> {
    let mut argv = vec![OsString::from("server"), root.as_os_str().to_os_string()];
    if read_only {
        argv.push(OsString::from("--read-only"));
        argv.push(OsString::from("--state-dir"));
        argv.push(state_dir.as_os_str().to_os_string());
    }
    argv
}

/// The install context, with our unit text attached — the private `for_backend` is what
/// decides which backends may actually receive it, and only systemd may.
///
/// `state_dir` is the **resolved** state dir, not `<root>/.tinio`: read-only mode relocates
/// it (decision 6), and it is what `WorkingDirectory=` must name — a unit pointing at the
/// served root would send every relative path into the tree that mode promises to leave
/// alone. `resolve::resolve_state_dir` is where the value comes from.
///
/// **A struct literal, not `ServiceInstallCtx::new`**: the crate has no such constructor
/// (`service-manager 0.11.0/src/lib.rs:343-353` carries only `cmd_iter`/`args_iter`) and no
/// `Default` to spread, so all nine fields are named, `restart_policy` included (`RestartPolicy`
/// does implement `Default` — `OnFailure`, `:189-193`). The label arrives as a `&str` because
/// that is what the operator types and what `--name` carries, and `ServiceLabel` parses it
/// through `FromStr` (`:262`), which is fallible in type terms and therefore a `Result` here
/// rather than an `expect` in library code (Global Constraints).
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::service::install_context;
///
/// let context = install_context(
///     Path::new("/srv/data/.tinio"),
///     Path::new("/usr/local/bin/tinio"),
///     vec!["server".into(), "/srv/data".into()],
///     "tinio-abc",
/// )?;
/// # #[cfg(unix)]
/// assert!(context.contents.expect("our unit").contains("Type=notify"));
/// # Ok::<(), tinio_cli::Error>(())
/// ```
pub fn install_context(
    state_dir: &Path,
    program: &Path,
    args: Vec<OsString>,
    label: &str,
) -> Result<ServiceInstallCtx, Error> {
    #[cfg(unix)]
    let contents = Some(notify::unit_text(program, &args, state_dir)?);
    #[cfg(not(unix))]
    let contents = None;
    Ok(ServiceInstallCtx {
        label: parse_label(label)?,
        program: program.to_path_buf(),
        args,
        contents,
        username: None,
        working_directory: Some(state_dir.to_path_buf()),
        environment: None,
        autostart: true,
        restart_policy: Default::default(),
    })
}

/// The remedy a permission failure names, instead of the raw `EACCES` / "Access is denied"
/// the three supervisors each spell differently (design decision 22's risk row, and §9's
/// one-refusal-shape rule: what was asked for, which channel it came from, what would work).
///
/// The SCM arm answers "run as Administrator"; the unix arms answer `--user`, except when the
/// call already carries `ServiceLevel::User` — then the remedy names the ownership of the
/// unit's paths, because repeating a flag the operator has already passed helps nobody.
///
/// # Examples
///
/// ```rust
/// use service_manager::{ServiceLevel, ServiceManagerKind};
/// use tinio_cli::service::permission_remedy;
///
/// let unix = permission_remedy(ServiceManagerKind::Systemd, ServiceLevel::System);
/// assert!(unix.contains("--user"));
/// let windows = permission_remedy(ServiceManagerKind::Sc, ServiceLevel::System);
/// assert!(windows.contains("Administrator"));
/// ```
pub fn permission_remedy(kind: ServiceManagerKind, level: ServiceLevel) -> &'static str {
    match (kind, level) {
        // Windows: the SCM is the platform's only supervisor, and it has no user level at all
        // — so elevation is the answer in both arms, and the user one has to say so.
        (ServiceManagerKind::Sc | ServiceManagerKind::WinSw, ServiceLevel::System) => {
            "needs Administrator: run this from an elevated prompt"
        }
        (ServiceManagerKind::Sc | ServiceManagerKind::WinSw, ServiceLevel::User) => {
            "has no user level on Windows: drop `--user` and run this from an elevated prompt"
        }
        // unix at the system level: the user level is the operator's no-elevation route.
        (_, ServiceLevel::System) => "needs root: pass `--user`, or re-run with elevation",
        // unix at the user level: the flag is already in effect, so the remedy names what a
        // *user* unit still needs rather than a flag the operator has already passed.
        (_, ServiceLevel::User) => {
            "needs this user to own the paths the unit names: check the program's and the state dir's ownership"
        }
    }
}

/// Runs the requested verb against the service for the root `args` names.
///
/// The root is resolved **canonicalised** (the label's input, and `resolve_root`'s contract),
/// the mode from the environment, and the state dir by the same
/// [`resolve::resolve_state_dir`] the server uses. Read-only argv records that path as
/// `--state-dir` ([`server_argv`]); `WorkingDirectory=` is the same path so relative paths
/// land there, and it is not what the server reads its state dir from. The level travels on the **manager** (`ServiceManager::set_level`), never on the
/// context, and our unit text reaches **systemd alone**: every other backend takes the crate's
/// own template for its platform (`for_backend` drops it), and the SCM ignores `contents`
/// entirely because it has no file form.
///
/// # Errors
///
/// A root that is not usable, no native supervisor on this platform, a supervisor that would
/// be WinSW (refused below), a label the crate cannot take, and every backend failure —
/// permission denials mapped to [`permission_remedy`]'s text, everything else carrying the
/// backend's own message.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_cli::service::{Args, Verb, run};
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let args = Args {
///     verb: Verb::Query,
///     dir: Some(PathBuf::from("/srv/data")),
///     name: None,
///     user: false,
///     read_only: false,
///     env: tinio_cli::commands::EnvArgs::default(),
/// };
/// let code = run(args)?;
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
pub fn run(args: Args) -> Result<ExitCode, Error> {
    let verb = args.verb;
    let root = resolve::resolve_root(args.dir.as_deref(), &resolve::current_dir()?)?;
    // The mode this verb can see is the environment's, and that is a rule rather than an
    // oversight: `service` reads no configuration — the unit it installs is what will read
    // one — so loading a config here would auto-create it as a side effect of an errand that
    // only talks to the supervisor. Both channels are the arguments' (`--read-only`/
    // `TINIO_READ_ONLY` and `TINIO_STATE_DIR`, the latter on the flattened `EnvArgs`), and they
    // were answered by the **discovery** parse: `run` dispatches this command before `.env`
    // is loaded, which is what keeps the mode the unit records the mode the server resolves.
    let read_only = args.read_only;
    let state_dir = resolve::resolve_state_dir(&root, read_only, args.env.state_dir.as_deref())?;
    let label = label_for(&root, args.name.as_deref());
    let level = level_for(args.user);

    let mut manager = TypedServiceManager::native().map_err(|err| {
        Error::Operational(format!(
            "this system has no service manager `tinio` can drive: {err} — the verbs need systemd, launchd or the SCM"
        ))
    })?;
    // `ServiceManagerKind::native()` prefers WinSW over the SCM whenever a `winsw.exe` is on
    // `PATH`, and WinSW is a wrapper executable this tool does not ship (decision 22 rejects
    // it on that ground alone). Selecting it would install a service pointing at a binary the
    // operator has to supply, so the honest answer is to refuse and name the route that works.
    if manager.is_winsw() {
        return Err(Error::Operational(
            "the supervisor this system would pick is WinSW, a wrapper executable `tinio` does \
             not ship: remove `winsw.exe` from `PATH` and re-run — the service is installed \
             through the SCM itself"
                .into(),
        ));
    }
    let kind = kind_of(&manager);
    manager
        .set_level(level)
        .map_err(|err| level_failure(kind, level, verb, err))?;

    match verb {
        Verb::Install => {
            // Only `install` needs the running executable's path: the other verbs name a
            // service the supervisor already holds.
            let program = env::current_exe().map_err(|err| {
                Error::Operational(format!(
                    "the running `tinio` executable's path could not be read: {err}"
                ))
            })?;
            // `server_argv` records `--state-dir` in read-only mode. The supervisor does not
            // keep `TINIO_STATE_DIR` or the installing user's home, and the server re-derives
            // its state dir from those — not from `WorkingDirectory=`.
            let service_argv = server_argv(&root, read_only, &state_dir);
            let context = install_context(&state_dir, &program, service_argv, &label)?;
            manager
                .install(for_backend(kind, context))
                .map_err(|err| failure(kind, level, verb, err))?;
        }
        Verb::Uninstall => {
            manager
                .uninstall(ServiceUninstallCtx {
                    label: parse_label(&label)?,
                })
                .map_err(|err| failure(kind, level, verb, err))?;
        }
        Verb::Start => {
            manager
                .start(ServiceStartCtx {
                    label: parse_label(&label)?,
                })
                .map_err(|err| failure(kind, level, verb, err))?;
        }
        Verb::Stop => {
            manager
                .stop(ServiceStopCtx {
                    label: parse_label(&label)?,
                })
                .map_err(|err| failure(kind, level, verb, err))?;
        }
        Verb::Query => {
            let status = manager
                .status(ServiceStatusCtx {
                    label: parse_label(&label)?,
                })
                .map_err(|err| failure(kind, level, verb, err))?;
            let (state, code) = described(&status);
            println!("{label}: {state}");
            return Ok(code);
        }
    }
    println!("{label}: {}", verb.done());
    Ok(ExitCode::SUCCESS)
}

/// The context as the chosen backend should receive it: **our unit text goes to systemd and to
/// nobody else**.
///
/// `ServiceInstallCtx::contents` is documented as "the contents of the service file for a given
/// ServiceManager to use instead of the default template" (`service-manager-0.11.0/src/lib.rs:308-310`),
/// and *the service file* is each backend's own format. systemd's is the INI unit
/// `notify::unit_text` renders; launchd's is a plist, which its `install` writes **verbatim**
/// as `<label>.plist` and then hands to `launchctl load` (`src/launchd.rs:115-127`); OpenRC's
/// and rc.d's are shell scripts. Our unit text handed to any of those is a file that supervisor
/// cannot read — a `.plist` full of `[Unit]`/`Type=notify` that fails to load and is left
/// behind — so every other backend takes the crate's own template for its platform.
///
/// That is also the honest answer on the merits, not merely a format accident: the readiness
/// and watchdog protocol our text declares (`READY=1`, `WATCHDOG=1`, `WatchdogSec=`) is
/// **systemd's alone**, and launchd has no `Type=notify` equivalent to receive it. The SCM
/// ignores `contents` entirely — it has no file form at all — so the arm it lands in is a
/// no-op rather than a wrong file.
fn for_backend(kind: ServiceManagerKind, mut context: ServiceInstallCtx) -> ServiceInstallCtx {
    if !matches!(kind, ServiceManagerKind::Systemd) {
        context.contents = None;
    }
    context
}

/// The label as `service-manager` needs it.
///
/// `ServiceLabel`'s only constructor from a name is `FromStr`
/// (`service-manager-0.11.0/src/lib.rs:262`), so every verb goes through here once and a name
/// the crate cannot take is one refusal rather than five.
fn parse_label(label: &str) -> Result<ServiceLabel, Error> {
    ServiceLabel::from_str(label).map_err(|err| {
        Error::Operational(format!("the service label `{label}` is not usable: {err}"))
    })
}

/// Which backend was chosen, so a failure's remedy names the platform it came from.
///
/// `TypedServiceManager` exposes its six kinds as `is_*` predicates rather than as a kind, and
/// [`run`] picks the manager with `TypedServiceManager::native()` (`src/typed.rs:95` — a bare
/// `target_or_native(None)` has no type to infer the parameter from). The last arm is WinSW by
/// elimination: the five predicates above cover the other five variants, and [`run`] refuses
/// WinSW before ever asking.
fn kind_of(manager: &TypedServiceManager) -> ServiceManagerKind {
    if manager.is_launchd() {
        ServiceManagerKind::Launchd
    } else if manager.is_openrc() {
        ServiceManagerKind::OpenRc
    } else if manager.is_rc_d() {
        ServiceManagerKind::Rcd
    } else if manager.is_sc() {
        ServiceManagerKind::Sc
    } else if manager.is_systemd() {
        ServiceManagerKind::Systemd
    } else {
        ServiceManagerKind::WinSw
    }
}

/// A backend failure, as this CLI reports it.
///
/// A **permission denial becomes the remedy**, never the raw error: three supervisors report
/// the same failure as `EACCES` or "Access is denied", and passing that through tells the
/// operator nothing (decision 22's risk row). Everything else keeps the backend's own message,
/// with the error and its kind logged at `debug!`, so the platform detail is demoted rather
/// than lost.
fn failure(kind: ServiceManagerKind, level: ServiceLevel, verb: Verb, err: io::Error) -> Error {
    tracing::debug!(error = %err, kind = ?err.kind(), "the service verb failed");
    if is_permission_denied(&err) {
        return Error::Operational(format!(
            "`tinio service {}` {}",
            verb.name(),
            permission_remedy(kind, level)
        ));
    }
    Error::Operational(format!("`tinio service {}` failed: {err}", verb.name()))
}

/// Whether a backend failure is the platform refusing on privilege grounds — the one
/// classification the remedy is selected by, in one place.
///
/// **The error kind alone cannot carry this, and that is a measured property of the crate
/// rather than a stylistic choice.** `service-manager` renders a failed command as
/// `io::Error::other(format!("Command failed with exit code {code}: {stderr}"))`
/// (`src/utils.rs:27-46`, the `not(encoding)` arm this build gets) and never constructs
/// `PermissionDenied` anywhere, so the SCM's refusal arrives with kind `Other` — the very
/// failure decision 22's risk row exists to suppress would fall through as raw text, and
/// `(Sc, System)`'s arm in [`permission_remedy`] would have no reachable trigger at all.
/// The file-writing backends *do* produce the kind
/// (`utils::write_file` → `OpenOptions::open` on a unit file the caller may not create), so
/// both channels are checked.
///
/// The markers are the platforms' own words, and two of the three are **measured** on a
/// non-elevated Windows host rather than transcribed from documentation: `sc.exe` answers a
/// create with exit code **5** and the text `[SC] OpenSCManager FAILED 5: Access is denied.`,
/// which the crate embeds verbatim after its own prefix, hence both the sentence and the
/// rendered `exit code 5` (they are the same refusal seen from two directions, and a
/// permission denial that is *not* this one still reads as a denial). `Permission denied` is
/// libc's `EACCES` text, which the crate embeds from a unix command's stderr the same way. A
/// backend that spells a privilege failure differently is a marker to add here — not a call
/// site to patch, and not a kind to hope for.
fn is_permission_denied(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::PermissionDenied
        || PERMISSION_MARKERS
            .into_iter()
            .any(|marker| err.to_string().contains(marker))
}

/// What the supervisors call it, one row per spelling: see [`is_permission_denied`] for each
/// row's provenance.
///
/// The 1060 this classifier must *not* claim is the other half of the contract: a service that
/// is not installed is a missing service, not a refused one, and it keeps the backend's own
/// message.
const PERMISSION_MARKERS: [&str; 3] = [
    // `sc.exe`, measured: `[SC] OpenSCManager FAILED 5:` / `Access is denied.`
    "Access is denied",
    // The same refusal as `wrap_output` renders it, measured the same way.
    "Command failed with exit code 5:",
    // libc's `EACCES` text, embedded from a unix command's stderr by the same `wrap_output`.
    "Permission denied",
];

/// `set_level`'s failure, where the operator's flag — not the system — is what the platform
/// cannot honour: the SCM has no user level at all (`sc.exe does not support user-level
/// services`, `service-manager-0.11.0/src/sc.rs:272-280`), which is a usage error (exit 2) on
/// the same rule `--api unix://` on Windows already follows.
fn level_failure(
    kind: ServiceManagerKind,
    level: ServiceLevel,
    verb: Verb,
    err: io::Error,
) -> Error {
    if err.kind() == io::ErrorKind::Unsupported {
        return Error::Usage(format!("`--user` is not available here: {err}"));
    }
    failure(kind, level, verb, err)
}

/// The state `query` prints, and the exit code it answers: the running state is the only
/// successful one, the same rule `tinio status` follows for a server.
fn described(status: &service_manager::ServiceStatus) -> (String, ExitCode) {
    match status {
        service_manager::ServiceStatus::Running => ("running".to_string(), ExitCode::SUCCESS),
        service_manager::ServiceStatus::Stopped(Some(reason)) => {
            (format!("stopped: {reason}"), ExitCode::FAILURE)
        }
        service_manager::ServiceStatus::Stopped(None) => ("stopped".to_string(), ExitCode::FAILURE),
        service_manager::ServiceStatus::NotInstalled => {
            ("not installed".to_string(), ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_util::testing::assert_send_sync;

    /// The context this test starts from: the brief's own values, so the split below is
    /// asserted on the context [`install_context`] really produces.
    fn context() -> ServiceInstallCtx {
        install_context(
            Path::new("/srv/data/.tinio"),
            Path::new("/usr/local/bin/tinio"),
            vec![OsString::from("server"), OsString::from("/srv/data")],
            "tinio-abc",
        )
        .expect("the install context")
    }

    #[test]
    fn only_systemd_is_handed_our_unit_text() {
        // `contents` is *the service file*, and the service file is each backend's own format:
        // launchd writes it verbatim as `<label>.plist` and then `launchctl load`s it
        // (`launchd.rs:115-127`), OpenRC and rc.d as shell scripts. Our INI unit in a plist is
        // a file launchd cannot load at all — and the readiness protocol it carries is
        // systemd's alone, so a plist that took it could never have worked either.
        for kind in [
            ServiceManagerKind::Launchd,
            ServiceManagerKind::OpenRc,
            ServiceManagerKind::Rcd,
            ServiceManagerKind::Sc,
            ServiceManagerKind::WinSw,
        ] {
            assert_eq!(
                for_backend(kind, context()).contents,
                None,
                "{kind:?} keeps the crate's own template for its platform"
            );
        }
        let systemd = for_backend(ServiceManagerKind::Systemd, context());
        #[cfg(unix)]
        {
            let unit = systemd.contents.expect("systemd's service file is ours");
            assert!(unit.contains("Type=notify"), "{unit}");
        }
        #[cfg(not(unix))]
        assert_eq!(
            systemd.contents, None,
            "the unit text is systemd's, and this crate does not compile it off unix"
        );
    }

    #[test]
    fn a_permission_failure_is_recognised_in_every_spelling_the_backends_yield() {
        // The SCM's refusal, as a non-elevated Windows host produces it (measured: `sc.exe
        // create …` answers exit code 5 with `[SC] OpenSCManager FAILED 5: Access is denied.`)
        // and as `wrap_output` renders it: kind `Other`, the exit code and the platform's words
        // inside one string.
        let scm = io::Error::other(
            "Command failed with exit code 5: [SC] OpenSCManager FAILED 5:\n\nAccess is denied.\n",
        );
        assert!(
            is_permission_denied(&scm),
            "the Windows arm of the remedy would have no reachable trigger otherwise"
        );
        // The file-writing backends' path, which does carry the kind (`utils::write_file`).
        let unit_file = io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Permission denied (os error 13)",
        );
        assert!(is_permission_denied(&unit_file));
        // And the refusal this classifier must *not* claim: a service that is not there is a
        // missing service, not a refused one (measured: exit code 1060, this text).
        let missing = io::Error::other(
            "Command failed with exit code 1060: [SC] EnumQueryServicesStatus:OpenService FAILED \
             1060:\n\nThe specified service does not exist as an installed service.\n",
        );
        assert!(
            !is_permission_denied(&missing),
            "a missing service keeps the backend's own message"
        );
    }

    #[test]
    fn the_public_surface_is_send_and_sync() {
        assert_send_sync::<Args>();
    }
}
