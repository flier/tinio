//! §1's pre-runtime half: steps a–j, resolve through the control-channel bind.

#[cfg(all(feature = "api", target_os = "macos"))]
use std::os::unix::net::UnixListener;
#[cfg(feature = "api")]
use std::process;
#[cfg(feature = "api")]
use std::sync::Arc;
use std::{
    fs,
    process::{ExitCode, ExitStatus},
};

use tracing::subscriber::set_global_default;

#[cfg(feature = "api")]
use super::metrics::ServerMetrics;
use super::{Args, Ready, Server};
#[cfg(feature = "api")]
use crate::_api::state::State;
#[cfg(feature = "api")]
use crate::_api::{
    Options as ApiOptions, Plane,
    metrics::{DEFAULT_TTL, Gauges, MetricsSource},
};
#[cfg(feature = "api")]
use crate::daemon;
use crate::{
    _config::{
        Config,
        log::{AccessFormat, Format, Verbosity},
        state::prepare_home_derived,
    },
    _fs::FsStorage,
    _sandbox::{Adopted, Terminal, disarm_signal_relay, supervise},
    _server::{identity, log},
    Error,
    cli::Provenance,
    events,
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
/// daemonize (step g) → confine (step i), which installs the subscriber at the position its own
/// doc states — around the launch or after the in-place confinement, and **not** a position this
/// function picks → FR-036's OTLP warning → the signal handlers (step j) → pipelines and
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
/// The exposure set's own rules belong to the platform crate `serve::Spec` confines with.
/// `tinio-sandbox-windows`' `canonical_exposures` canonicalises and **refuses** a path it cannot
/// resolve, while macOS rebuilds the set through `tinio-sandbox-macos`'s
/// `profile::with_canonical_paths` — the resolved spelling beside the operator's, an unresolvable
/// path left alone. [`crate::sandbox::confine`] applies one spec from this function's resolution.
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
    // request this build refuses has not already created the **served root** in the tree it
    // refuses to serve. **The plan runs on every target, because the refusal is the point** — its
    // answer is read again by the platforms' `confine` arm below, and no target is left out of
    // that: whether this target has a backend is `tinio_sandbox::backend_available`'s one answer,
    // it names Linux (P1), macOS (P3) and Windows (P2) alike, so every one of them honours a
    // request `plan` cleared.
    // **A confined child asks for nothing**: it is already inside the container its launcher
    // built, and re-entering the launcher would launch a child of a child — the payload's
    // presence is the whole discriminator (decision 17). The static refusals `plan` runs are the
    // launcher's, and it has already passed them for this run.
    //
    // **The *state* directory is the one path a refusal cannot claim to precede, and that is the
    // resolution's, not this function's**: `resolve::server` runs immediately above and creates a
    // read-only run's directory on its way to `<state dir>/.tinio.toml` — so a refused run has
    // already minted the derived leaf it was refused for (measured 2026-09-25: a fresh `$HOME`
    // holds `<home>/.tinio/roots/<hash>/.tinio.toml` after the refusal). The refusal is still the
    // launcher's to raise, and raising it here is what keeps the **root** and every sandbox step
    // untouched; what it does not do is undo a creation the resolution had already made. That is
    // also why `refuse_underived_state_dir` decides on whether the directory was *named* rather
    // than on whether it exists.
    let request = if adopted.is_some() {
        sandbox::Request { channel: None }
    } else {
        // The refusal's own input travels **on the resolution** (`Resolution::state_dir_override`):
        // a read-only confinement whose state directory would be derived rather than named is one
        // macOS cannot grant (`refuse_underived_state_dir`), and the launcher is where the remedy
        // is named. Nothing is threaded through this call site for it.
        sandbox::plan(&resolution, &args, provenance)?
    };

    // h: the bind sources must exist before anything binds — the data plane and the plane's
    // control channel both bind below — and the storage opens under the root, so a missing state
    // dir is refused here rather than surfacing as an unresolved-path failure later. Read-only
    // mode creates the state dir and never the root: creating the root would write the one thing
    // the mode promises to leave alone. When the served dir is the home-derived leaf, harden
    // `<home>/.tinio`/`roots` first: resolve left them unmade so clients create nothing, and a
    // bare `create_dir_all` would inherit the umask and follow a planted symlink.
    //
    // **The hardening call is the read-only branch's, and the function's own body is why.** In a
    // read-write run the state directory is `<root>/.tinio`, so
    // `prepare_home_derived_in`'s first check — `state_dir != home_state_dir_path(root, home)?` —
    // returns `Ok` before it can do anything: the call is a no-op whose only cost is a
    // `dirs::home_dir()` lookup and a `canonicalize` of the root, and on macOS that cost is paid
    // **inside the confined child too**, which runs this same function under a profile that has to
    // name every path it touches. Read-only is the mode whose state directory *is* the home-derived
    // leaf, so that branch keeps the call: there the parents really are unmade, and this is what
    // hardens them before the `create_dir_all` below. **The macOS profile is no longer the other
    // half**: its home grant is deleted and neither mode spends a rule on `$HOME`, so
    // `tinio_config::state::prepare_home_derived` is the whole of the hardening. A confined child's
    // own `$HOME`-derived leaf is still refused `file-write-mode` on `<home>/.tinio`, which sits
    // above the read-write state-directory exposure `sandbox_request` adds, and there the remedy is
    // an explicit writable `TINIO_STATE_DIR`
    // (`docs/superpowers/specs/2026-09-18-sandbox-design.md` §5, its `home` paragraph).
    if resolution.read_only {
        prepare_home_derived(&resolution.root, &resolution.state_dir)?;
    }
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

    // **The subscriber is `sandbox::confine`'s to install, and its doc is where the ordering rule
    // lives.** The two positions are facts about the confinement — a launcher emits the events an
    // operator reconstructs a failed launch with *inside* that call, while an in-place target
    // confines the **calling thread**, so a thread the subscriber started beforehand would stay
    // outside the namespaces, the capability set and the seccomp filter for the life of the
    // process — and neither is this caller's to know. `install_subscriber` therefore travels as
    // an action (`FnOnce`, so it cannot be run twice), and the position is the confinement's.
    //
    // It needs no storage. The access-log file lives in the state directory the **resolution**
    // named, which `resolve::fs_options` hands to `FsStorage` unchanged (`state_dir:
    // Some(resolution.state_dir.clone())`, and `FsStorage::resolve` clones it verbatim), so the
    // two spellings are one directory — and that directory exists a few lines above.
    //
    // **Read-only governs the data plane, and the state directory stays legitimately writable.**
    // The contract is that `--read-only` never writes the served root (FR-023); it says nothing
    // about the tool's own state, which is the resolved state directory and — in read-only mode
    // — always outside the root. That is where this subscriber opens the access log, appending
    // to it (`tinio_server::log::build_subscriber`), and where `meta.redb`, the staging areas
    // and `state` live. Refusing the append would not make the run more read-only; it would take
    // its access log away. `a_read_only_run_appends_its_access_log_in_the_state_directory` pins
    // the pair, so this reads as the contract rather than as a leak.
    //
    // **After `daemon::detach_if_requested`, not before**: that call is §1's step g, the process's
    // fork, and a subscriber built with an OTLP export layer is a thread the fork must not meet.
    // Both of `confine`'s positions are after this line, which is what keeps that true.
    //
    // i, on **every** target. A launcher hands back a child this process supervises and never
    // serves behind; an in-place target confines **this** process and answers `None`, which is
    // "carry on to serve" (decision 6). The `None` call is not decoration: it is where the jail
    // is entered, so a target that skipped it would serve unconfined while `plan` had already
    // promised the confinement. Which answer comes back is `platform_facts().launches_child`,
    // inside `confine` — this arm matches the child and does not name the OS.
    let launched = sandbox::confine(&resolution, &args, &request, || {
        install_subscriber(&resolution)
    })?;

    // **FR-036's trade: a configured OTLP endpoint and a confinement cannot both be honoured, and
    // the combination is warned about, naming both, rather than refused** (design decision 25,
    // `contracts/cli.md`). The confinement cuts the confined process's outbound connections and the
    // exporter is the only outbound path this product has, so what is lost is the export — and
    // losing an observer is not grounds for refusing to serve, so the run proceeds.
    //
    // **It sits on the one line both arms reach with the log live**, and that is now the
    // confinement's guarantee rather than this call's position: `confine` above installs the
    // subscriber on every path it returns `Ok` from — before a launch, after an in-place
    // confinement — so the log exists here whichever arm this target took. **Once per run by
    // construction**: a confined **child**
    // asks for nothing (`Request::enabled` is false for it — the payload's presence is the whole
    // discriminator, decision 17), so the warning is the launcher's alone and the child inherits the
    // stderr it is written on. A `warn!` and not a frame, because stdout carries event frames and
    // nothing else (decision 19) while this is the operational log's — and `sandbox::lost_export` is
    // the rule (both facts, one reader), so this call site only emits it.
    if let Some((channel, endpoint)) =
        sandbox::lost_export(&request, otlp_endpoint(&resolution.config))
    {
        tracing::warn!(
            channel = %channel,
            endpoint = %endpoint,
            "the confinement cuts the outbound path the OTLP exporter is the only user of, so \
             this run's export is lost: the confinement proceeds and the observer does not"
        );
    }

    if let Some(mut child) = launched {
        // The facade's **two-argument** `supervise(&mut Child, teardown)`: it waits for the
        // confined child, runs the teardown on every path, and answers the child's status,
        // which is this process's answer. The launcher stops here: it builds no runtime.
        //
        // **The teardown is where this launcher's `state` file comes back out, and where its
        // signal relay is disarmed.** The launcher platforms published the file — the child cannot
        // on Windows, whose DACL is current-user-only and whose container identity is the package
        // SID, and on macOS the token is the launcher's because it is the process that publishes
        // it — and `supervise`'s callback is the contract's own home for "removing a path the
        // launcher created". It runs before this function reads the status, so a launcher never
        // exits leaving the file behind, and it is guarded by the pid the file names (see
        // `remove_launcher_state`), so a successor's file is never taken.
        //
        // `disarm_signal_relay` is the plan's other teardown step (design §3 step 9, decision 15's
        // every-exit-path rule): it closes part of the pid-reuse window by making a signal that
        // arrives after the child was reaped relay nowhere. It is **not** `#[cfg]`-gated on the
        // *platform* because the facade presents it neutrally — a no-op wherever this process is
        // not a launcher that forwards signals — so the same call site exists on every target,
        // which is the shape decision 2 keeps. **The two feature arms spell the same teardown
        // differently for a lint rather than for a behaviour**: with `api` there are two statements
        // to run and a closure is the only shape; without it the teardown is one function call,
        // and `clippy::redundant_closure` — a `-D warnings` error here — refuses `|| f()` when `f`
        // is exactly the `FnOnce()` the parameter wants. Both call `disarm_signal_relay`.
        #[cfg(feature = "api")]
        let status = supervise(&mut child, || {
            remove_launcher_state(&resolution);
            disarm_signal_relay();
        })?;
        #[cfg(not(feature = "api"))]
        let status = supervise(&mut child, disarm_signal_relay)?;
        // **The launcher's last `info!` event, and the half of shared §7's record that no other
        // line can carry**: the spawn names the pid the launch produced, and this names what
        // became of it — the status the launcher is about to forward as its own answer (design
        // §3 step 8). It lands here rather than in either platform's launcher because the
        // supervision is shared: both launcher platforms reach this arm and both answer with the
        // child's status, so a `debug!`-only or per-platform spelling would leave one of them
        // without the record an operator reconstructs a run from. The level is the one the
        // subscriber `run` installs above enables, and `info!` rather than `debug!` deliberately:
        // see `serve.rs`'s note on the fixed `Verbosity::Info`.
        tracing::info!(
            step = "exit",
            status = %status,
            "the confined child exited; its status is forwarded"
        );
        // **§3 step 8's other half, and the one frame this process emits on its own behalf.**
        // The parent never emits `Listening` — that event means the data plane is serving, only
        // the child knows that, and a parent printing the endpoint it had bound would announce a
        // server that may never accept. What it does emit is its own `Event::Error` when the
        // child exited **without ever having reported**, and the answer to that question is the
        // terminal record read **after** the wait: `supervise` borrows the child and `wait` joins
        // the relay, so the record is final rather than a snapshot — reading it before the wait
        // would report a healthy child as never having started.
        //
        // Both launcher plans carried this as ticked work and the tree kept only the `info!`
        // beside it, which left a child that died on the way up reaching its caller as an
        // **absence** — exactly what decision 19's "a startup failure is an event rather than an
        // absence" exists to prevent, and what a harness waiting for a frame reads as a timeout
        // instead of a cause.
        //
        // **This is not a second frame for one failure.** The arm below returns
        // `Ok(Server::Launched(…))`, so `run`'s serving arm never sees an `Err` for this path and
        // never frames one: the sentence travels on stdout once, from here.
        let (refusal, code) = launcher_exit(status, child.terminal());
        if let Some(message) = refusal {
            events::emit_error(&message);
        }
        return Ok(Server::Launched(ExitCode::from(code)));
    }

    // FR-033's fail-closed privilege probe: any configured `[owner]`/`[[users]]` `local_uid`
    // mapping needs chown privilege (root or CAP_CHOWN) at startup, and a process without it
    // must refuse rather than silently degrade to server-user ownership. It runs here —
    // after the served root exists and before anything binds — and it is skipped read-only,
    // because the probe writes a private file into the served root (design decision 14).
    // Windows never carries a mapping: the config parser rejects the key there, so the
    // probe is a no-op and the identity's uid map is empty by construction.
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
            // launcher generated it *and* published `state` with it, and **the launcher platforms
            // reach that answer by different routes**. On Windows the cause is the file's DACL:
            // current-user-only and protected, so this process — inside the container, with the
            // package SID as its identity — could not write it. **On macOS nothing refuses the
            // write**, and the cause is ownership instead: `state` must name the launcher, whose
            // `--daemon` readiness check compares the file's pid against the pid it spawned and
            // whose `Plane::shutdown` removal guard expects itself, and the token is minted by the
            // process that publishes. `platform_facts` and `serve::Spec` are that reason's home,
            // as they are for the channel handoff below. Either way a plane here that minted its own
            // token would refuse every client that read that file: `/status` and `/stop` would
            // answer 401 for a server that is running and discoverable. `None` is every run that
            // owns its own `state`: the unconfined ones, and the in-place platforms.
            token: adopted
                .as_ref()
                .and_then(|adopted| adopted.token().map(str::to_owned)),
        };
        // **A launcher already bound the control channel, and this process serves on what it
        // inherited** (`Adopted::controls`). Windows' reason is identity: the channel's pipes carry
        // a current-user-only DACL, and inside the container this process's identity is the package
        // SID, so `CreateNamedPipeW` here would be refused `ERROR_ACCESS_DENIED` — which
        // `bind_pipe` reports as "another server is already listening", a message about a second
        // server for a channel no one could create. macOS' reason is the profile: the confined
        // child's unix `bind()` is a network operation the profile grants nothing for, so
        // `bind_unix` here would be refused `Operation not permitted` — and the rule that used to
        // cover it was a bind *grant* rather than a filter, which is what the handoff removes. The
        // bind stays the answer for every other launch: an empty list is an unconfined run, an
        // in-place platform, an `api`-less build, or a selection with no local channel, and those
        // processes are the ones that must own the single-instance claim themselves.
        //
        // **Every inherited instance, not the first** (Windows): one instance answers a client and
        // then stops listening, and this process cannot create the next one, so the pool it was
        // handed is the whole of what it will ever serve on. A unix channel is one socket, and the
        // platform crate refuses any other count. `Plane::open` is that split: empty controls bind,
        // Windows adopts the handle values, macOS adopts the socket `inherited_socket` rebuilt
        // (`sandbox::adopted_control` — an object, never a number this process turns back into a
        // socket), and an in-place target refuses a non-empty list.
        //
        // **The call is spelled twice because the signature is two signatures**: the socket
        // argument is `#[cfg(target_os = "macos")]` on `Plane::open`, so the targets that inherit
        // no unix socket name a shorter call rather than a placeholder value — which is the whole
        // of what the deleted `InheritedSocket::absent()` was.
        let controls = adopted.as_ref().map(Adopted::controls).unwrap_or(&[]);
        #[cfg(target_os = "macos")]
        let plane = Plane::open(options, controls, inherited_socket(adopted.as_ref())?)?;
        #[cfg(not(target_os = "macos"))]
        let plane = Plane::open(options, controls)?;
        plane
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

/// The unix socket [`Plane::open`] adopts — **macOS's function, because macOS is the one target
/// whose launcher hands a listener over.**
///
/// [`Plane::open`]'s parameter exists on macOS alone (it is `#[cfg(target_os = "macos")]` there,
/// and its doc carries the axis), so a target that inherits no unix control socket has neither the
/// argument nor this call: Windows' launcher passes pipe *handle values* in `controls`, and an
/// in-place target never gets this far — the process that serves is the one that bound.
/// [`sandbox::adopted_control`] turns the inherited descriptor into the listener; this function is
/// the shape of *when* to ask — a process that was launched at all.
///
/// [`Plane::open`]: crate::_api::Plane::open
#[cfg(all(feature = "api", target_os = "macos"))]
fn inherited_socket(adopted: Option<&Adopted>) -> Result<Option<UnixListener>, Error> {
    let Some(adopted) = adopted else {
        return Ok(None);
    };
    // A launcher hands the inherited descriptors over in the launch payload, so an empty list is
    // the process that owns the single-instance claim itself — and one that has no socket to
    // adopt.
    if adopted.controls().is_empty() {
        return Ok(None);
    }
    Ok(Some(sandbox::adopted_control(adopted)?))
}

/// What this launcher owes once the child it supervised is gone: the frame stdout gets, and the
/// exit code this process answers with.
///
/// **§3 step 8's other half, stated as the decision rather than as the write.** The parent never
/// emits `Listening` — that event means the data plane is serving, only the child knows that, and
/// a parent that printed the endpoint it had bound would be announcing a server that may never
/// accept. What it does emit is its own `Event::Error` when the child exited **without ever
/// having reported**, which is [`Child::terminal`]'s answer *after* the wait: the wait joins the
/// relay, so the record is final rather than a snapshot, and a child that never became ready is
/// that `None` rather than a guess.
///
/// **The status is forwarded either way, and the frame does not rewrite it.** §3 step 8 makes the
/// launcher's own answer the child's status, which is what every harness and supervisor that reads
/// this process's exit code is given — the CLI's `--daemon` parent is not one of them, because it
/// polls the published `state` file and the control channel and answers its own readiness timeout
/// (`crate::daemon`); the frame is added information about a startup the parent watched fail, not a
/// replacement for what the child said. `1` stands in only where there is no code to forward at all
/// — a child killed by a signal.
///
/// **It answers rather than emits because the emission is the caller's**: `crate::events` owns
/// stdout on this path (decision 19's "stdout carries event frames and nothing else"), so a
/// function that both decided the sentence and wrote it would put the one writer in two places —
/// and could not be driven by a test at all.
///
/// [`Child::terminal`]: crate::_sandbox::Child::terminal
fn launcher_exit(status: ExitStatus, reported: Option<&Terminal>) -> (Option<String>, u8) {
    // A child killed by a signal has no exit code; 1 is the operational failure that is. A code
    // outside `u8` is not this process's to invent either, and answers the same way — the shape
    // the exit-code mapping had before the decision moved here.
    let code = match status.code() {
        Some(code) => u8::try_from(code).unwrap_or(1),
        None => 1,
    };
    match reported {
        Some(_) => (None, code),
        None => (
            Some(format!(
                "the confined child exited ({status}) without reporting readiness"
            )),
            code,
        ),
    }
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
#[cfg(feature = "api")]
fn remove_launcher_state(resolution: &Resolution) {
    let Ok(Some(state)) = State::read(&resolution.state_dir) else {
        return;
    };
    if state.pid == process::id() {
        let _ = State::remove(&resolution.state_dir);
    }
}

/// Build this process's subscriber and install it as the global default — **the one construction
/// site, handed to `sandbox::confine` as the action it calls at the one position this target's
/// shape requires** (that function's doc is the ordering's home, and its `FnOnce` is what makes
/// the install exactly one).
///
/// **A subscriber that cannot be built is fatal; one that cannot be installed is not.** The build
/// reads the access log and, in an `otel` build with `[telemetry] otlp_endpoint` configured, starts
/// the exporter's thread — a failure there is a startup failure (`Error::Log`, exit 1), and the
/// error is named rather than propagated with a bare `?` because
/// `Box<dyn Error + Send + Sync>` is no variant of this crate's [`Error`]. `set_global_default`'s
/// own failure is ignored: a server with no subscriber serves, it only logs nothing, and a process
/// that got this far has installed none — `confine`'s `FnOnce` is what keeps it to one install
/// rather than a flag.
fn install_subscriber(resolution: &Resolution) -> Result<(), Error> {
    let subscriber = log::build_subscriber(
        verbosity(resolution),
        format(resolution),
        &access_format(resolution),
        &resolution.state_dir.join("access.log"),
        otlp_endpoint(&resolution.config),
    )
    .map_err(|err| Error::Log(err.to_string()))?;
    set_global_default(subscriber).ok();
    Ok(())
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
///
/// **Two readers, and they are the trade's two halves**: `install_subscriber` hands it to the
/// subscriber, and the FR-036 warning reads it to name what a confinement costs. One function, so
/// "an endpoint is configured" cannot be answered two ways.
fn otlp_endpoint(config: &Config) -> Option<&str> {
    config
        .telemetry
        .as_ref()
        .map(|telemetry| telemetry.otlp_endpoint.as_str())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{OpenOptions, create_dir_all, read_dir, read_to_string},
        io::Write as _,
        path::{Path, PathBuf},
        process::Command,
    };

    use tempfile::TempDir;

    use super::*;
    use crate::_sandbox::{Child, Terminal};

    /// A confined child that exits with `code` and relays nothing.
    ///
    /// **A real process on both hosts, and no mock anywhere in these legs.** The decision under
    /// test reads the `ExitStatus` and the terminal record, and this produces both the way the
    /// launcher produces them: `supervise` over a `Child` whose wait closure reaps a real child
    /// that carries no relay. That is also exactly the shape of a child which died before its
    /// first frame — nothing recorded a terminal event, so `terminal` answers `None` once the
    /// wait has returned rather than because the test said so.
    fn dying(code: u8) -> Child {
        let mut command = if cfg!(windows) {
            let mut command = Command::new("cmd");
            command.args(["/c", &format!("exit {code}")]);
            command
        } else {
            let mut command = Command::new("sh");
            command.args(["-c", &format!("exit {code}")]);
            command
        };
        let mut process = command.spawn().expect("spawn the child stand-in");
        Child::new(move || Ok(process.wait()?))
    }

    /// **A child that never reported readiness is the parent's own refusal** (design §3 step 8,
    /// decision 19), and this is the leg that fails without it: before the frame existed the run
    /// reached its caller as an absence, with nothing on stdout to say a confined server had
    /// died on the way up.
    ///
    /// **What this leg does not cover is the call site's two effects** — the `events::emit_error`
    /// write and the `Server::Launched` it returns — because both need the spawned binary rather
    /// than a library call. `crates/tinio-e2e`'s `@confinement` legs own that half (they spawn
    /// `tinio` and read its streams); this one pins the decision those effects are made on.
    #[test]
    fn a_child_that_never_reported_is_the_parents_own_refusal() {
        let mut child = dying(3);
        let status = supervise(&mut child, || ()).expect("the child is reaped");
        assert!(
            child.terminal().is_none(),
            "nothing relayed, so the record is the 'never reported' answer"
        );

        let (refusal, code) = launcher_exit(status, child.terminal());

        let expected = format!("the confined child exited ({status}) without reporting readiness");
        assert_eq!(
            refusal.as_deref(),
            Some(expected.as_str()),
            "the parent's own sentence, and the only frame it emits on its own behalf"
        );
        assert_eq!(
            code, 3,
            "the status is forwarded rather than rewritten: the frame is added information, and \
             the answer a `--daemon` parent or a harness reads stays the child's"
        );
    }

    /// **The other direction, and the one a launcher that framed unconditionally would break**: a
    /// child that did report is a server that started, so a perfectly good run must carry no
    /// `error` frame for its consumer to misread.
    #[test]
    fn a_child_that_reported_is_never_framed() {
        let mut child = dying(0);
        let status = supervise(&mut child, || ()).expect("the child is reaped");
        let reported = Terminal::Listening("127.0.0.1:9000".parse().expect("addr"));

        let (refusal, code) = launcher_exit(status, Some(&reported));

        assert_eq!(refusal, None, "a child that reported owes stdout nothing");
        assert_eq!(code, 0, "and its status is still the launcher's answer");
    }

    /// A resolution of the shape a read-only run resolves to: the served root and a state
    /// directory **beside** it, which is where read-only mode puts state and the only shape the
    /// launcher will serve (an in-root one is refused before this point).
    fn read_only(root: &Path, state_dir: &Path) -> Resolution {
        Resolution {
            root: root.to_path_buf(),
            config: Config::default(),
            config_text: String::new(),
            config_path: state_dir.join(".tinio.toml"),
            read_only: true,
            state_dir: state_dir.to_path_buf(),
            state_dir_override: Some(state_dir.to_path_buf()),
            config_error: None,
        }
    }

    /// **Read-only governs the data plane; the state directory stays writable, and this is the
    /// pair that says so.** The access-log sink is `resolution.state_dir.join("access.log")`,
    /// opened for **append** while the subscriber is built (the line that carries this module's
    /// contract note), and read-only mode relocates the state directory out of the served root
    /// — so the append is the tool writing its own state rather than the served root being
    /// written. Without this leg the behaviour reads as a leak: a process confined to a
    /// read-only root that grows a file. It is the contract, and FR-023's promise is about the
    /// root.
    ///
    /// **What the assertion breaks on is the append, not the mode**: a `state` directory that
    /// could not be written fails the second half with the operating system's own error, and the
    /// first half fails if read-only ever resolves a state directory inside the root it serves.
    #[test]
    fn a_read_only_run_appends_its_access_log_in_the_state_directory() {
        let dir = TempDir::new().expect("scratch");
        let root = dir.path().join("root");
        let state_dir = dir.path().join("state");
        // Read-only creates the state directory and never the root (`run`'s step h), and the
        // served root is required to exist already: mirror both, so what the test writes is what
        // a real read-only run has.
        create_dir_all(&root).expect("the served root");
        create_dir_all(&state_dir).expect("the state directory");
        let resolution = read_only(&root, &state_dir);

        let access_log: PathBuf = resolution.state_dir.join("access.log");
        assert!(
            !access_log.starts_with(&resolution.root),
            "read-only must keep the access log out of the served root it promises to leave \
             alone: {} is inside {}",
            access_log.display(),
            resolution.root.display()
        );

        // The server's own append, spelled the way `build_subscriber` spells it.
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&access_log)
            .expect("read-only must not take the access log away: the state directory is writable");
        file.write_all(b"GET / HTTP/1.1 403\n")
            .expect("append the line");
        drop(file);

        let written = read_to_string(&access_log).expect("the appended line is readable");
        assert_eq!(
            written, "GET / HTTP/1.1 403\n",
            "the append wrote exactly the line it was handed"
        );

        // And the served root was not written by any of it: the one thing the mode promises.
        let root_entries = read_dir(&resolution.root)
            .expect("read the served root")
            .count();
        assert_eq!(
            root_entries, 0,
            "nothing under the served root may be created by a read-only run"
        );
    }
}
