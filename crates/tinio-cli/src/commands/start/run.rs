//! §1's pre-runtime half: steps a–j, resolve through the control-channel bind.

#[cfg(any(target_os = "macos", target_os = "windows"))]
use std::process::ExitCode;
#[cfg(feature = "api")]
use std::sync::Arc;
use std::fs;

use tracing::subscriber::set_global_default;

#[cfg(feature = "api")]
use super::metrics::ServerMetrics;
use super::{Args, Ready, Server};
#[cfg(feature = "api")]
use crate::_api::{
    Options as ApiOptions, Plane,
    metrics::{DEFAULT_TTL, Gauges, MetricsSource},
};
#[cfg(any(target_os = "macos", target_os = "windows"))]
use crate::_sandbox::supervise;
#[cfg(unix)]
use crate::_server::identity;
#[cfg(feature = "api")]
use crate::daemon;
use crate::{
    _config::{
        Config,
        log::{AccessFormat, Format, Verbosity},
    },
    _fs::FsStorage,
    _sandbox::Adopted,
    _server::log,
    Error,
    cli::Provenance,
    resolve::{self, Located, Pipelines, Resolution},
    sandbox, signal,
};

/// §1's pre-runtime half: steps a–j, resolve through the control-channel bind.
///
/// **The order inside is the ruling, not a detail.** The control-channel bind is the plane's
/// single-instance gate, so it happens before the startup repair — nothing repairs the state a
/// live server is using — which is why the storage and the bound plane are built here and the
/// repair is the first thing [`serve()`](crate::commands::start::serve) does. The full order
/// is: the sandbox's static half (step f) → create the state dir and, read-write, the root
/// (step h, because the bind sources and the storage both need the paths to exist) →
/// daemonize (step g) → confine (step i) → the signal handlers (step j) → pipelines and
/// storage → single-instance bind → (in [`serve()`](crate::commands::start::serve)) startup
/// repair → data plane bound → api plane bound → `state` published → readiness line →
/// `READY=1`.
///
/// `sandbox::plan` precedes the directories rather than follows them — §1's own order. It is a
/// decision on the **channels** alone (`[server] sandbox`, `TINIO_SANDBOX`, `--sandbox`) and
/// reads no path, so a request this build refuses has not already created a state dir, or a
/// served root, inside the tree it refuses to serve. What needs the directories is the rest of
/// the startup: the bind sources must exist before anything binds, and the storage opens under
/// the root — which is why step h still lands before the fork.
///
/// The exposure set's own rules — every path canonicalised, a reparse point refused — belong to
/// the platform backend that would apply a confinement (`tinio-sandbox-windows`'
/// `canonical_exposures`), which no target wires yet: `plan` refuses every request before a set
/// exists, so there is nothing here for those rules to run on.
///
/// # Errors
///
/// Every resolution and startup failure, with the exit code its variant carries: a usage error
/// (2) for a flag/config contradiction, an operational one (1) for a state directory that
/// cannot be made, a confinement this build cannot honour, a state database a live server
/// already holds, or a control channel that is already bound.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::start::{Args, run},
///     resolve::locate,
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
/// let server = run(args, located, None, &Provenance::default())?;
/// # let _ = server;
/// # Ok(())
/// # }
/// ```
///
/// `args` and `provenance` are the **authoritative** parse's — the second one, run after
/// `<state dir>/.env` was loaded — and `located` is what the first one answered. Passing the
/// discovery parse's flags here would silently ignore `.env`, which is why the process entry
/// hands this function the pair it re-parsed rather than the one it dispatched on.
pub fn run(
    args: Args,
    located: Located,
    adopted: Option<Adopted>,
    provenance: &Provenance,
) -> Result<Server, Error> {
    // f, on the discovery answers and the authoritative flags: config path, load, and the fold.
    let resolution = resolve::server(located, &args, provenance)?;

    // f: the three static determinations, every unhonourable one refused here — before the
    // fork, so the refusal reaches the operator's shell, and before the directories below, so a
    // request this build refuses has not already created the state dir or the served root in the
    // tree it refuses to serve. **The plan runs on every target, because the refusal is the
    // point** — its answer is read again by the launcher platforms' `confine` arm below, and the
    // in-place platforms have nothing further to read it with: the exposure set and the OTLP
    // trade that once travelled out of this half belong to the backend that flips
    // `BACKEND_IS_STUB`, and no target has one yet.
    let request = sandbox::plan(&resolution, &args, provenance)?;
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let _ = &request; // read only by the launcher platforms' `confine` arm below

    // h: the bind sources must exist before anything binds — the data plane and the plane's
    // control channel both bind below — and the storage opens under the root, so a missing state
    // dir is refused here rather than surfacing as an unresolved-path failure later. Read-only
    // mode creates the state dir and never the root: creating the root would write the one thing
    // the mode promises to leave alone.
    fs::create_dir_all(&resolution.state_dir)?;
    if !resolution.read_only {
        fs::create_dir_all(&resolution.root)?;
    }

    // g: forks before any thread exists; the parent waits and exits (Task 5).
    // `api`-only: the module and the flag it serves are compiled out together (the flag's own
    // doc says why there is nothing to prove a daemon ready with in that build).
    #[cfg(feature = "api")]
    daemon::detach_if_requested(&args, &resolution)?;

    // `--daemon` child's launch token (parent set `TINIO_DAEMON_LAUNCH`); inert otherwise.
    #[cfg(feature = "api")]
    daemon::publish_launch_token(&resolution.state_dir)?;

    #[cfg(any(target_os = "macos", target_os = "windows"))]
    if let Some(mut child) = sandbox::confine(&resolution, &request)? {
        // The facade's **two-argument** `supervise(&mut Child, teardown)`: it waits for the
        // confined child, runs the teardown on every path, and answers the child's status,
        // which is this process's answer. The launcher stops here: it builds no runtime.
        let status = supervise(&mut child, || {})?;
        // A child killed by a signal has no exit code; 1 is the operational failure that is.
        let code = status.code().unwrap_or(1);
        return Ok(Server::Launched(ExitCode::from(
            u8::try_from(code).unwrap_or(1),
        )));
    }

    // FR-033's fail-closed privilege probe: any configured `[owner]`/`[[users]]` `local_uid`
    // mapping needs chown privilege (root or CAP_CHOWN) at startup, and a process without it
    // must refuse rather than silently degrade to server-user ownership. It runs here —
    // after the served root exists and before anything binds — and it is skipped read-only,
    // because the probe writes a private file into the served root (design decision 14).
    // Windows never carries a mapping: the config parser rejects the key there, so the
    // identity's uid map is empty by construction.
    #[cfg(unix)]
    if !resolution.read_only {
        identity::ensure_chown_privilege(&resolution.config, &resolution.root)?;
    }

    // j, before k: the handlers are registered before the runtime exists, so a signal that
    // arrives during startup reaches the same shutdown path as one that arrives later.
    let handles = signal::install()?;

    // `FsStorage::new` opens the state database (and takes its exclusive lock: this is where a
    // second server on the same root is refused, before the control channel is even reached),
    // and a launcher has already returned by this line — so this is the child, or the single
    // non-launching process.
    let pipelines = Pipelines::build(&resolution.config.pipeline.clone().unwrap_or_default())?;
    let mut storage = FsStorage::new(
        &resolution.root,
        resolve::fs_options(&resolution, Some(&pipelines)),
    )?;
    if let Some(s3) = resolution.config.s3.as_ref() {
        // `[s3] max_concurrent_uploads` caps in-progress multipart uploads, so an
        // authenticated client cannot accumulate an unbounded number of them.
        storage.set_max_concurrent_uploads(s3.max_concurrent_uploads);
    }

    // The subscriber's error is `Box<dyn Error + Send + Sync>`, which no variant of this
    // crate's `Error` converts from — so it is named, not propagated with a bare `?`. The
    // access-log file sits beside the state database, in the state dir the storage resolved.
    fs::create_dir_all(storage.state_dir())?;
    let subscriber = log::build_subscriber(
        verbosity(&resolution),
        format(&resolution),
        &access_format(&resolution),
        &storage.state_dir().join("access.log"),
        otlp_endpoint(&resolution.config),
    )
    .map_err(|err| Error::Log(err.to_string()))?;
    // Once per process by construction: the one call site is here, and a run that gets this
    // far has not installed one. The failure is still not fatal — a server with no subscriber
    // serves, it only logs nothing.
    set_global_default(subscriber).ok();

    #[cfg(feature = "api")]
    let plane = {
        let metrics = Arc::new(ServerMetrics::new(&pipelines, &storage));
        // Plan A's `Options::gauges` is what feeds the plane's `/metrics`. The field is not
        // optional in the literal, and omitting it would answer 404 on every scrape: the
        // source is this task's adapter, and the plane's `serve` is what drives it.
        let source: Arc<dyn MetricsSource> = metrics.clone();
        let gauges = Arc::new(Gauges::new(source, DEFAULT_TTL));
        // `[server] host`/`port`, the flags folded in — what `/status` reports as the host.
        let bind = resolution.address()?;
        // The plane's single-instance gate: the control-channel bind, before the repair.
        Plane::bind(ApiOptions {
            selection: super::api_options(&resolution, &args)?, // `[api]` + the `--api*` flags
            state_dir: resolution.state_dir.clone(), // the plane's only filesystem anchor
            root: resolution.root.clone(),           // canonical: `state.root` too
            host: bind.ip().to_string(),
            stop: handles.stopper.clone(), // `/stop` sends on the one shutdown channel
            gauges: Some(gauges),          // 404 on `/metrics` without it
        })?
    };

    Ok(Server::Ready(Box::new(Ready {
        resolution,
        args,
        adopted,
        handles,
        storage,
        pipelines,
        #[cfg(feature = "api")]
        plane,
        on_ready: None,
    })))
    // k is `run::on_runtime`'s: no runtime is created anywhere in this module.
}

/// The effective verbosity the subscriber is built with.
///
/// Read from the resolved config, which is where `resolve::server`'s `fold_flags` materialised
/// FR-016's chain for this key: `--verbosity`, `TINIO_LOG_LEVEL`, `.env` and `[log] verbosity`
/// are one value by the time a run reaches here, so re-running the chain would be a second
/// implementation of a rule that already has one.
fn verbosity(resolution: &Resolution) -> Verbosity {
    resolution
        .config
        .log
        .as_ref()
        .map_or_else(Verbosity::default, |log| log.verbosity)
}

/// The effective operational log format, resolved the same way [`verbosity`] is.
fn format(resolution: &Resolution) -> Format {
    resolution
        .config
        .log
        .as_ref()
        .map_or_else(Format::default, |log| log.server_log_format)
}

/// The access-log format (`[log] access_log_format`; `combined` by default).
///
/// There is deliberately no flag and no variable for it — `contracts/config.md`'s list has
/// none — so the config file is the only level there is.
fn access_format(resolution: &Resolution) -> AccessFormat {
    resolution
        .config
        .log
        .as_ref()
        .map_or_else(AccessFormat::default, |log| log.access_log_format.clone())
}

/// The OTLP endpoint, when `[telemetry]` names one — `None` without the section, which is what
/// switches the export layer off (`build_subscriber` builds it only when the `otel` feature is
/// compiled in **and** this is `Some`).
fn otlp_endpoint(config: &Config) -> Option<&str> {
    config
        .telemetry
        .as_ref()
        .map(|telemetry| telemetry.otlp_endpoint.as_str())
}
