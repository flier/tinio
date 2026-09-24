//! §1's pre-runtime half: steps a–j, resolve through the control-channel bind.

use std::fs;
// The inherited control handles the launcher put in argv: the type the plane's `adopt` takes
// them back as, and only the launcher arm has any.
#[cfg(all(feature = "api", target_os = "windows"))]
use std::os::windows::io::RawHandle;
// The launcher arm's half of the `state` lifecycle needs the module; nothing else in this file
// does, and neither does a target with no launcher (see `remove_launcher_state`).
#[cfg(all(feature = "api", any(target_os = "macos", target_os = "windows")))]
use std::process;
#[cfg(any(target_os = "macos", target_os = "windows"))]
use std::process::ExitCode;
#[cfg(feature = "api")]
use std::sync::Arc;

use tracing::subscriber::set_global_default;

#[cfg(feature = "api")]
use super::metrics::ServerMetrics;
use super::{Args, Ready, Server};
// Gated on the launcher arm rather than on the feature: the one caller is
// `remove_launcher_state`, which only a launching target reaches.
#[cfg(all(feature = "api", any(target_os = "macos", target_os = "windows")))]
use crate::_api::state::State;
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
        state::prepare_home_derived,
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
/// the platform backend that would apply a confinement: `tinio-sandbox-windows`'
/// `canonical_exposures` is what Windows wires and macOS does not — `plan` refuses a macOS
/// request before a set exists, so those rules run only on Windows, where Windows' `confine`
/// rebuilds them from the resolution below.
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
    // **A confined child takes the document from its launch payload** (decision 18): the text
    // travelled with the launch because the file it came from can sit outside every exposure,
    // so a child that read the path again would be denied by its own confinement.
    let resolution = match adopted.as_ref() {
        Some(adopted) => resolve::server_confined(located, &args, provenance, adopted.config())?,
        None => resolve::server(located, &args, provenance)?,
    };

    // f: the three static determinations, every unhonourable one refused here — before the
    // fork, so the refusal reaches the operator's shell, and before the directories below, so a
    // request this build refuses has not already created the state dir or the served root in the
    // tree it refuses to serve. **The plan runs on every target, because the refusal is the
    // point** — its answer is read again by the platforms' `confine` arm below, the two whose
    // `BACKEND_IS_STUB` is `false`: `sandbox/mod.rs` is that constant's one authority, and Linux
    // and Windows flip it, while macOS still refuses in `plan` with the platform named.
    // **A confined child asks for nothing**: it is already inside the container its launcher
    // built, and re-entering the launcher would launch a child of a child — the payload's
    // presence is the whole discriminator (decision 17). The static refusals `plan` runs are the
    // launcher's, and it has already passed them for this run.
    let request = if adopted.is_some() {
        sandbox::Request { enabled: false }
    } else {
        sandbox::plan(&resolution, &args, provenance)?
    };
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let _ = &request; // read only by the launcher platforms' `confine` arm below

    // h: the bind sources must exist before anything binds — the data plane and the plane's
    // control channel both bind below — and the storage opens under the root, so a missing state
    // dir is refused here rather than surfacing as an unresolved-path failure later. Read-only
    // mode creates the state dir and never the root: creating the root would write the one thing
    // the mode promises to leave alone. When the served dir is the home-derived leaf, harden
    // `<home>/.tinio`/`roots` first: resolve left them unmade so clients create nothing, and a
    // bare `create_dir_all` would inherit the umask and follow a planted symlink.
    prepare_home_derived(&resolution.root, &resolution.state_dir)?;
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

    // **The subscriber is installed before the confinement, and that placement is the fix for a
    // measured loss.** The launcher platforms emit the events an operator reconstructs a failed
    // launch with *inside* `sandbox::confine` — the backend's `step = "CreateProcessW"`, the
    // profile's `taken_over=`, the ACL grant the launcher reports before it makes it — and a
    // subscriber installed after that arm drops every one of them. The position is also the only
    // one that covers both processes: a launcher returns from this function at the `confine` arm
    // below, so it would never reach a subscriber built after it.
    //
    // It needs no storage. The access-log file lives in the state directory the **resolution**
    // named, which `resolve::fs_options` hands to `FsStorage` unchanged (`state_dir:
    // Some(resolution.state_dir.clone())`, and `FsStorage::resolve` clones it verbatim), so the
    // two spellings are one directory — and that directory exists a few lines above.
    //
    // **After `daemon::detach_if_requested`, not before**: that call is §1's step g, the
    // process's fork, and a subscriber built with an OTLP export layer is a thread the fork must
    // not meet. The subscriber's error is `Box<dyn Error + Send + Sync>`, which no variant of
    // this crate's `Error` converts from — so it is named, not propagated with a bare `?`.
    let subscriber = log::build_subscriber(
        verbosity(&resolution),
        format(&resolution),
        &access_format(&resolution),
        &resolution.state_dir.join("access.log"),
        otlp_endpoint(&resolution.config),
    )
    .map_err(|err| Error::Log(err.to_string()))?;
    // Once per process by construction: the one call site is here, and a run that gets this
    // far has not installed one. The failure is still not fatal — a server with no subscriber
    // serves, it only logs nothing.
    set_global_default(subscriber).ok();

    // i, on **every** target. The launcher platforms hand back a child this process supervises
    // and never serves behind; the in-place platforms confine **this** process and answer
    // `None`, which is "carry on to serve" (decision 6). The in-place call is not decoration:
    // it is where the jail is entered, so a target that skipped it would serve unconfined while
    // `plan` had already promised the confinement.
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    if let Some(mut child) = sandbox::confine(&resolution, &args, &request)? {
        // The facade's **two-argument** `supervise(&mut Child, teardown)`: it waits for the
        // confined child, runs the teardown on every path, and answers the child's status,
        // which is this process's answer. The launcher stops here: it builds no runtime.
        //
        // **The teardown is where this launcher's `state` file comes back out.** The Windows arm
        // published it — the child cannot, its DACL is current-user-only and the container's
        // identity is the package SID — and `supervise`'s callback is the contract's own home for
        // "removing a path the launcher created". It runs before this function reads the status,
        // so a launcher never exits leaving the file behind, and it is guarded by the pid the
        // file names (see `remove_launcher_state`), so a successor's file is never taken.
        #[cfg(feature = "api")]
        let status = supervise(&mut child, || remove_launcher_state(&resolution))?;
        #[cfg(not(feature = "api"))]
        let status = supervise(&mut child, || {})?;
        // A child killed by a signal has no exit code; 1 is the operational failure that is.
        let code = status.code().unwrap_or(1);
        return Ok(Server::Launched(ExitCode::from(
            u8::try_from(code).unwrap_or(1),
        )));
    }
    // The in-place shape: the call happens for its own side effect, and its `None` is the answer
    // this process acts on by continuing to the runtime that serves.
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let _ = sandbox::confine(&resolution, &args, &request)?;

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
        let options = ApiOptions {
            selection: super::api_options(&resolution, &args)?, // `[api]` + the `--api*` flags
            state_dir: resolution.state_dir.clone(), // the plane's only filesystem anchor
            root: resolution.root.clone(),           // canonical: `state.root` too
            host: bind.ip().to_string(),
            stop: handles.stopper.clone(), // `/stop` sends on the one shutdown channel
            gauges: Some(gauges),          // 404 on `/metrics` without it
            // **The launcher's token, when this process was launched by one** (design decision
            // 17's private channel, extended to the one secret the two processes share). The
            // launcher generated it *and* published `state` with it, because the file's DACL is
            // current-user-only and this process — inside the container, with the package SID as
            // its identity — could not write it. A plane here that minted its own token would
            // refuse every client that read that file: `/status` and `/stop` would answer 401
            // for a server that is running and discoverable. `None` is every run that owns its
            // own `state`: the unconfined ones, and the in-place platforms.
            token: adopted
                .as_ref()
                .and_then(|adopted| adopted.token().map(str::to_owned)),
        };
        // **A launcher already bound the control channel, and this process serves on the
        // instances it inherited** (`Adopted::controls`; the launcher's half is
        // `sandbox::windows::confine_or_launch`). The channel's pipes carry a
        // current-user-only DACL, and inside the container this process's identity is the
        // package SID, so `CreateNamedPipeW` here would be refused `ERROR_ACCESS_DENIED` —
        // which `bind_pipe` reports as "another server is already listening", a message
        // about a second server for a channel no one could create. The bind stays the
        // answer for every other launch: an empty list is unix, an `api`-less build, or a
        // selection with no local channel, and those processes are the ones that must own
        // the single-instance claim themselves.
        //
        // **Every inherited instance, not the first**: one instance answers a client and then
        // stops listening, and this process cannot create the next one, so the pool it was
        // handed is the whole of what it will ever serve on.
        #[cfg(windows)]
        {
            let inherited = adopted.as_ref().map(Adopted::controls).unwrap_or_default();
            if inherited.is_empty() {
                // The plane's single-instance gate: the control-channel bind, before the repair.
                Plane::bind(options)?
            } else {
                // The handle values the launcher put in argv, spelled back the way the
                // platform typed them (`RawHandle` is a pointer there, `as` is the cast).
                let raws: Vec<RawHandle> = inherited
                    .iter()
                    .map(|raw| *raw as usize as RawHandle)
                    .collect();
                Plane::adopt(options, &raws)?
            }
        }
        #[cfg(not(windows))]
        Plane::bind(options)? // the plane's single-instance gate, before the repair
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

/// The `state` file **this launcher** published for the child it just supervised, removed
/// best-effort on the way out.
///
/// **The file is the launcher's, and this is the launcher's half of that ruling** — the Windows
/// confinement arm's `confine_or_launch` is the other half. A confined child cannot create,
/// write or remove it: the DACL is current-user-only and `SE_DACL_PROTECTED`, and inside the
/// container the child's identity is the package SID, so every path-based access it makes to the
/// file is refused. The unconfined launcher therefore creates it once the child reports it is
/// listening, and takes it back here, before the child's status is this process's answer.
///
/// **The guard is the pid the file names, which is this process's own.** `State::new` writes
/// `process::id()`, and the launcher builds the file on its own side of the launch, so "the file
/// is mine" is exactly `state.pid == process::id()` — the same condition `Plane::remove_owned`
/// applies, and for the same reason: a successor that started while this child was draining has
/// published its own `state`, and deleting by name alone would strip a live server of its
/// discovery (and, with the name free, let a third one in). A file that cannot be read or does
/// not parse is left alone as well: an unproven claim is not a licence to delete.
///
/// Best-effort by contract — `supervise`'s teardown callback cannot report a failure, and a
/// launcher that refused to exit over a file the next `doctor --fix` reclaims would be the worse
/// failure.
#[cfg(all(feature = "api", any(target_os = "macos", target_os = "windows")))]
fn remove_launcher_state(resolution: &Resolution) {
    let Ok(Some(state)) = State::read(&resolution.state_dir) else {
        return;
    };
    if state.pid == process::id() {
        let _ = State::remove(&resolution.state_dir);
    }
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
