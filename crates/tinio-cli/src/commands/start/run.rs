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
use super::metrics::{ServerMetrics, WriteLockStatsSource};
use super::{Args, Ready, Served, Server};
#[cfg(feature = "api")]
use crate::_api::state::State;
#[cfg(feature = "api")]
use crate::_api::{
    Error as ApiError, Options as ApiOptions, Plane,
    metrics::{DEFAULT_TTL, Gauges, MetricsSource},
};
#[cfg(feature = "api")]
use crate::_core::storage::Storage;
#[cfg(feature = "api")]
use crate::daemon;
#[cfg(feature = "lifecycle")]
use crate::{
    _api::lifecycle::LifecycleAdmin,
    _core::{bucket, lifecycle, storage::Error as StorageError},
};
use crate::{
    _fs::{Error as FsError, FsStorage},
    _sandbox::{Adopted, Attribution, Child, Refused, disarm_signal_relay, supervise},
    _server::log,
    _util::state::{prepare_home_derived, prepare_synthetic_root},
    Error,
    cli::Provenance,
    commands::is_pg_root,
    config::{self, Config, storage::Backend},
    events,
    resolve::{self, Located, Pipelines, Resolution},
    sandbox, signal,
};
#[cfg(feature = "pg")]
use crate::{_pg, _util::secret::Secret, config::storage::pg::DEFAULT_POOL_SIZE};

/// §1's pre-runtime half: steps a–j, resolve through the control-channel bind.
///
/// **The order inside is the ruling, not a detail.** The control-channel bind is the plane's
/// single-instance gate, so it happens before the startup repair — nothing repairs the state a
/// live server is using — which is why the storage and the bound plane are built here and the
/// repair is the first thing [`serve()`](crate::commands::start::serve) does. The full order
/// is: the sandbox's static half (step f) → the credential policy's static refusal (audit
/// run-2, P2 — a non-loopback bind that would serve the built-in pair) → create the state dir
/// and, read-write, the root (step h, because the bind sources and the storage both need the
/// paths to exist) → daemonize (step g) → confine (step i), which installs the subscriber at
/// the position its own doc states — around the launch or after the in-place confinement, and
/// **not** a position this function picks → FR-036's OTLP warning → the signal handlers
/// (step j) → pipelines and storage → single-instance bind → (in
/// [`serve()`](crate::commands::start::serve)) startup repair → data plane bound → api plane
/// bound → `state` published → readiness line → `READY=1`.
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
///     pg_dsn: None,
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
    // **A synthetic pg root is served as pg only when the configuration selects pg** (ruling R8,
    // restated now that the backend is servable). `resolve::locate`'s pg arm derives the root
    // `~/.local/state/tinio/pg/<dsn identity>/` in two cases: a configuration that says
    // `[storage] type = "pg"`, and the fallback a bare `TINIO_PG_DSN=… tinio server` with no `DIR`
    // reaches. Only the first is a pg run; the second carries no `[storage]` section, so the chain
    // below would answer `fs` — and would **create** `<synthetic>/.tinio/.tinio.toml` on the way —
    // and serving a filesystem root inside a directory that exists only because a pg dsn was
    // addressed is silently the wrong backend.
    //
    // **The two arms are one condition** ("the root is the synthetic one") read where the fact is
    // knowable: with **no named configuration** the chain below would *mint* the document, so the
    // refusal is here, ahead of the resolution; with a named configuration that does not select
    // pg, the resolution reads (never creates) it, and the check after it refuses. A filesystem
    // run records no pg arm whatever the environment holds — `is_pg_root` reads that record and
    // nothing else — which is what keeps "`TINIO_PG_DSN` selects nothing" true.
    let synthetic = is_pg_root(&located.root, located.pg_root.as_deref());
    if synthetic && located.config.is_none() {
        return Err(pg_root_without_config());
    }

    // f, on the discovery answers and the authoritative flags: config path, load, and the fold.
    // **A confined child takes the document from its launch payload** (decision 18): the text
    // travelled with the launch because the file it came from can sit outside every exposure,
    // so a child that read the path again would be denied by its own confinement.
    let resolution = match adopted.as_ref() {
        Some(adopted) => resolve::server_confined(located, &args, provenance, adopted.config())?,
        None => resolve::server(located, &args, provenance)?,
    };

    // **The pg backend is served now** (PG5): `[storage] type = "pg"` reaches `Served::Pg` and the
    // storage is constructed inside the runtime (its `connect` is async and this half is the
    // pre-runtime one). A build that does **not** carry the `pg` feature cannot serve such a
    // configuration and refuses it truthfully — the keys parse in every build (§6), so this
    // refusal is what keeps a configuration the build cannot honour from being silently ignored.
    #[cfg(not(feature = "pg"))]
    if resolution.config.backend() == Backend::Pg {
        return Err(pg_not_servable());
    }
    // The other arm of the synthetic-root guard: a **named** configuration that does not select pg
    // resolved a filesystem run under the dsn's synthetic root. Nothing was created (the named
    // document was read, not minted) and serving it would be the wrong backend silently.
    if synthetic && resolution.config.backend() != Backend::Pg {
        return Err(pg_root_without_config());
    }

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

    // **A confinement and a pg store cannot be honoured together** (the sandbox crate's own
    // finding): the confinement cuts the confined process's outbound connections, and a pg run's
    // store connection is exactly such a path, so a jailed pg run could never serve. It is refused
    // **here** — after `plan` has answered which channel asked (the request is that record), and
    // before the fork and the directories — so a refused run neither forks nor confines itself.
    // The fs backend is unaffected: it serves from local files, and its confinement is the whole
    // point of the mode.
    if let Some(refusal) =
        sandbox::refuse_pg_under_confinement(&request, resolution.config.backend())
    {
        return Err(refusal);
    }

    // **The built-in credential pair's refusal is a static one too** (audit run-2, P2): the
    // predicate is `tinio_server::identity::uses_builtin_fallback` and the sentence is
    // `Resolution::credential_policy`'s, so the refusal and the loopback warning the serving half
    // still owes render one source. It used to be raised by `serve` — after the repair, the
    // daemon fork and the confinement — which left a refused start with the root repaired and
    // this process jailed; here it sits with the sandbox's own static refusals, before the fork
    // and before the directories below, and `serve` is left with the warning alone (the only arm
    // this refusal does not end).
    if let Some(detail) = resolution.credential_policy()?.refusal() {
        return Err(Error::Config(config::Error::invalid_value("auth", detail)));
    }

    // h: the bind sources must exist before anything binds — the data plane and the plane's
    // control channel both bind below — and the storage opens under the root, so a missing state
    // dir is refused here rather than surfacing as an unresolved-path failure later. Read-only
    // mode creates the state dir and never the root: creating the root would write the one thing
    // the mode promises to leave alone. Hardening the state tree comes first in both of the
    // shapes that need it: a home-derived leaf's parents (resolve left them unmade so clients
    // create nothing) and a pg run's synthetic root (no backend opens that leaf), so the
    // `create_dir_all`s below can neither inherit the umask nor follow a planted symlink.
    //
    // **Only the read-only filesystem branch needs `prepare_home_derived`, and the function's own
    // body is why.** In a read-write filesystem run the state directory is `<root>/.tinio`, so
    // `prepare_home_derived_in`'s first check — `state_dir != home_state_dir_path(root, home)?` —
    // returns `Ok` before it can do anything: the call is a no-op whose only cost is a
    // `dirs::home_dir()` lookup and a `canonicalize` of the root, and on macOS that cost is paid
    // **inside the confined child too**, which runs this same function under a profile that has to
    // name every path it touches. Read-only is the mode whose state directory *is* the home-derived
    // leaf, so that branch keeps the call: there the parents really are unmade, and this is what
    // hardens them before the `create_dir_all` below. **The macOS profile is no longer the other
    // half**: its home grant is deleted and neither mode spends a rule on `$HOME`, so
    // `tinio_util::state::prepare_home_derived` is the whole of the hardening. A confined child's
    // own `$HOME`-derived leaf is still refused `file-write-mode` on `<home>/.tinio`, which sits
    // above the read-write state-directory exposure `sandbox_request` adds, and there the remedy is
    // an explicit writable `TINIO_STATE_DIR`
    // (`docs/superpowers/specs/2026-09-18-sandbox-design.md` §5, its `home` paragraph).
    //
    // **A pg run takes the first shape, in either mode**: its root is not a served directory, so
    // nothing else in this crate opens (or hardens) `<root>/.tinio` — the filesystem backend's
    // 0700 state-database open is exactly what a pg run has not got.
    if synthetic {
        // **A pg run's synthetic root is the tool's own state tree, and nothing else hardens
        // it.** `prepare_home_derived` is the read-only *filesystem* leaf's hardening, and a
        // pg run's state directory is not that leaf, so before this call the `create_dir_all`
        // below minted `<root>/.tinio` with the process umask — 0755 on a 022 umask, listing
        // the tool's state and its access log to every local user. The filesystem backend
        // creates its own leaf 0700 (`tinio-fs`'s state-database open); a pg run opens no such
        // database, so the hardening is this call's (`tinio_util::state`).
        prepare_synthetic_root(&resolution.root)?;
    } else if resolution.read_only {
        prepare_home_derived(&resolution.root, &resolution.state_dir)?;
    }
    // **A confined child's permission refusal is the confinement's own answer, and the four
    // sites below are where it can arrive** (follow-up §4's wiring caveat): `?` on these calls
    // used to turn any of them into a path-less `Error::Io`, which left a confined operator
    // with "Permission denied" and nothing to expose. Each site names the subject — the state
    // directory, the served root, or the control channel — so a denial reaches the caller as a
    // value with a path in it rather than as a sentence to read.
    //
    // **The gate is `adopted.is_some()`, and it has to be**: a confined child is the one
    // process whose refusals can be the mechanism's, while a launcher's own io errors are the
    // operator's filesystem answering an unconfined process — the same discriminator the
    // process entry uses to identify a child at all (design decision 17).
    let confined = adopted.is_some();
    fs::create_dir_all(&resolution.state_dir).map_err(|err| {
        sandbox::classify_io(err, Refused::Path(resolution.state_dir.clone()), confined)
    })?;
    if !resolution.read_only {
        fs::create_dir_all(&resolution.root).map_err(|err| {
            sandbox::classify_io(err, Refused::Path(resolution.root.clone()), confined)
        })?;
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
    // `contracts/cli.md`). The confinement cuts the confined process's outbound connections. The
    // OTLP exporter is an outbound path this product has; **the pg store's database connection is
    // the other, and a pg run under a confinement is refused up front**
    // (`sandbox::refuse_pg_under_confinement`), so the only outbound path a run that reaches this
    // warning has is the export — and losing an observer is not grounds for refusing to serve, so
    // the run proceeds.
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
            "the confinement cuts the outbound path only the OTLP exporter uses (the pg store's \
             database connection is this product's other outbound path, and a pg run under a \
             confinement is refused before it is served), so this run's export is lost: the \
             confinement proceeds and the observer does not"
        );
    }

    if let Some(mut child) = launched {
        // The facade's **two-argument** `supervise(&mut Child, teardown)`: it waits for the
        // confined child, runs the teardown on every path, and answers the child's status,
        // which is this process's answer. The launcher stops here: it builds no runtime.
        //
        // **The teardown is where this launcher's `state` file comes back out, and where its
        // signal relay is disarmed.** The launcher publishes the file — the token is the
        // publishing process's, and the file has to name the launcher, whose pid the readiness
        // check and the removal guard both compare — and `supervise`'s callback is the contract's
        // own home for "removing a path the
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
        let (refusal, code) = launcher_exit(status, &child);
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
        super::chown::ensure_chown_privilege(resolution.identity(), &resolution.root)?;
    }

    // j, before k: the handlers are registered before the runtime exists, so a signal that
    // arrives during startup reaches the same shutdown path as one that arrives later.
    let handles = signal::install()?;

    // The pipelines and the backend. **The two backends' construction points differ, and that is
    // the pre-runtime problem stated rather than hidden**: the filesystem storage is opened
    // synchronously here — `FsStorage::new` opens the state database and takes its exclusive lock
    // (this is where a second server on the same root is refused, before the control channel is
    // even reached), and its management plane is bound with it, both before the runtime, as §1 has
    // it — while the pg backend is only **selected** here (its construction inputs travel on
    // `Served::Pg`), because `PgStorage::connect` is async and is built on the one runtime `serve`
    // is dispatched on. A launcher has already returned by this line, so this is the child, or the
    // single non-launching process.
    let pipeline_section = resolution.config.pipeline.clone().unwrap_or_default();
    let pipelines = Pipelines::build(&pipeline_section)?;
    let backend = match resolution.config.backend() {
        #[cfg(feature = "pg")]
        Backend::Pg => {
            let (config, access) = pg_backend_config(&resolution, args.env.pg_dsn.as_ref())?;
            Served::Pg { config, access }
        }
        _ => {
            let mut storage = FsStorage::new(
                &resolution.root,
                resolve::fs_options(&resolution, Some(&pipelines)),
            )
            .map_err(|err| match err {
                // The storage constructor's own io failure, classified like the two above: what it
                // opens under the root — the state database and the staging areas — is the path a
                // confinement refuses first, and it arrives wrapped in the backend's error rather
                // than as an io error of its own.
                FsError::Io(io) => {
                    sandbox::classify_io(io, Refused::Path(resolution.root.clone()), confined)
                }
                other => Error::from(other),
            })?;
            if let Some(s3) = resolution.config.s3.as_ref() {
                // `[s3] max_concurrent_uploads` caps in-progress multipart uploads, so an
                // authenticated client cannot accumulate an unbounded number of them.
                storage.set_max_concurrent_uploads(s3.max_concurrent_uploads);
            }
            #[cfg(feature = "api")]
            let plane = open_management_plane(
                &storage,
                &resolution,
                &args,
                adopted.as_ref(),
                &handles,
                &pipelines,
            )?;
            Served::Fs(Box::new(super::Prepared {
                storage,
                #[cfg(feature = "api")]
                plane,
            }))
        }
    };

    Ok(Server::Ready(Box::new(Ready {
        resolution,
        args,
        adopted,
        handles,
        backend,
        pipelines,
        on_ready: None,
    })))
    // k is `run::on_runtime`'s: no runtime is created anywhere in this module.
}

/// The management plane's lifecycle admin (LC14): the three `/buckets/{bucket}/lifecycle` routes
/// are served from it, so the plane carries this run's storage handle without `tinio-api` ever
/// depending on the crate that owns one.
///
/// **The bucket name is the storage contract's own** (`bucket::Name`), so a name the contract
/// rejects is refused before a store sees it; the plane maps the store's `AccessDenied` to `403`
/// (a read-only run) and its `NoSuchBucket` to `404`.
#[cfg(feature = "lifecycle")]
struct StorageAdmin<S> {
    storage: S,
}

#[cfg(feature = "lifecycle")]
#[async_trait::async_trait]
impl<S: Storage + Send + Sync> LifecycleAdmin for StorageAdmin<S> {
    async fn get(&self, bucket: &bucket::Name) -> Result<Option<lifecycle::Config>, StorageError> {
        self.storage
            .get_bucket_lifecycle(bucket)
            .await
            .map_err(Into::into)
    }

    async fn put(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<(), StorageError> {
        self.storage
            .put_bucket_lifecycle(bucket, config)
            .await
            .map_err(Into::into)
    }

    async fn delete(&self, bucket: &bucket::Name) -> Result<(), StorageError> {
        self.storage
            .delete_bucket_lifecycle(bucket)
            .await
            .map_err(Into::into)
    }
}

/// The plane's lifecycle admin, or `None` when this run injects none (LC23): `[s3] lifecycle =
/// false` turns off the three operations **and** the plane's routes, because rules that can be
/// edited but never run are the surprise that ruling removed. The capability is read through
/// [`serve::capabilities_for`](super::serve::capabilities_for) — the same reader the data plane
/// and the pass use — so the three cannot disagree about this run's answer.
#[cfg(feature = "lifecycle")]
fn lifecycle_admin<S: Storage + Clone + Send + Sync + 'static>(
    storage: &S,
    resolution: &Resolution,
) -> Option<Arc<dyn LifecycleAdmin>> {
    super::serve::capabilities_for(resolution)
        .lifecycle
        .then(|| {
            Arc::new(StorageAdmin {
                storage: storage.clone(),
            }) as Arc<dyn LifecycleAdmin>
        })
}

/// The management plane over `storage` — the control channel that is the plane's
/// single-instance gate, bound before the repair runs (design decision 12).
///
/// **One implementation, two call sites** (the backend-generic serving seam): a filesystem run
/// binds it here, in the pre-runtime half, so the gate still precedes the runtime; a pg run binds
/// it inside [`serve`](super::serve::serve), after its storage is constructed and before the
/// repair. Nothing about the bind is backend-specific — the storage is read only by the metrics
/// adapter's `/metrics` gauges — so it is written once and generic over the served backend.
///
/// The gauges' source is `ServerMetrics`, which reads the storage through the contract's generic
/// walk and takes the write-lock family through [`WriteLockStatsSource`] (fs only).
#[cfg(feature = "api")]
pub(super) fn open_management_plane<S>(
    storage: &S,
    resolution: &Resolution,
    args: &Args,
    adopted: Option<&Adopted>,
    handles: &signal::Handles,
    pipelines: &Pipelines,
) -> Result<Plane, Error>
where
    S: Storage + WriteLockStatsSource + Clone + 'static,
{
    let confined = adopted.is_some();
    let metrics = Arc::new(ServerMetrics::new(pipelines, storage));
    // Plan A's `Options::gauges` is what feeds the plane's `/metrics`. The field is not optional in
    // the literal, and omitting it would answer 404 on every scrape: the source is this task's
    // adapter, and the plane's `serve` is what drives it.
    let source: Arc<dyn MetricsSource> = metrics.clone();
    let gauges = Arc::new(Gauges::new(source, DEFAULT_TTL));
    // `[server] host`/`port`, the flags folded in — what `/status` reports as the host.
    let bind = resolution.address()?;
    let options = ApiOptions {
        selection: super::api_options(resolution, args)?, // `[api]` + the `--api*` flags
        state_dir: resolution.state_dir.clone(),          // the plane's only filesystem anchor
        root: resolution.root.clone(),                    // canonical: `state.root` too
        host: bind.ip().to_string(),
        stop: handles.stopper.clone(), // `/stop` sends on the one shutdown channel
        gauges: Some(gauges),          // 404 on `/metrics` without it
        // **The launcher's token, when this process was launched by one** (design decision 17's
        // private channel, extended to the one secret the two processes share). The launcher
        // generated it *and* published `state` with it, and **the reason is ownership on every
        // platform**: `state` must name the launcher, whose `--daemon` readiness check compares
        // the file's pid against the pid it spawned and whose `Plane::shutdown` removal guard
        // expects itself, and the token is minted by the process that publishes. That ownership
        // is the whole reason now — no descriptor keeps this process out of the file
        // (`docs/superpowers/specs/2026-10-15-windows-file-dacl-removal-design.md`).
        // `platform_facts` and `serve::Spec` are that reason's home, as they are for the channel
        // handoff below. Either way a plane here that minted its own token would refuse every
        // client that read that file: `/status` and `/stop` would answer 401 for a server that is
        // running and discoverable. `None` is every run that owns its own `state`: the unconfined
        // ones, and the in-place platforms.
        token: adopted.and_then(|adopted| adopted.token().map(str::to_owned)),
        #[cfg(feature = "lifecycle")]
        lifecycle: lifecycle_admin(storage, resolution),
    };
    // **A launcher already bound the control channel, and this process serves on what it
    // inherited** (`Adopted::controls`). Windows' reason is identity: the launcher's pipes carry
    // Windows' own default named-pipe descriptor rather than anything naming the package SID, and
    // inside the container
    // this process's identity *is* the package SID, so `CreateNamedPipeW` here would be refused
    // `ERROR_ACCESS_DENIED` — which `bind_pipe` reports
    // as "another server is already listening", a message about a second server for a channel no
    // one could create. macOS' reason is the profile: the confined child's unix `bind()` is a
    // network operation the profile grants nothing for, so `bind_unix` here would be refused
    // `Operation not permitted` — and the rule that used to cover it was a bind *grant* rather than
    // a filter, which is what the handoff removes. The bind stays the answer for every other
    // launch: an empty list is an unconfined run, an in-place platform, an `api`-less build, or a
    // selection with no local channel, and those processes are the ones that must own the
    // single-instance claim themselves.
    //
    // **Every inherited instance, not the first** (Windows): one instance answers a client and then
    // stops listening, and this process cannot create the next one, so the pool it was handed is
    // the whole of what it will ever serve on. A unix channel is one socket, and the platform crate
    // refuses any other count. `Plane::open` is that split: empty controls bind, Windows adopts the
    // handle values, macOS adopts the socket `inherited_socket` rebuilt (`sandbox::adopted_control`
    // — an object, never a number this process turns back into a socket), and an in-place target
    // refuses a non-empty list.
    //
    // **The call is spelled twice because the signature is two signatures**: the socket argument is
    // `#[cfg(target_os = "macos")]` on `Plane::open`, so the targets that inherit no unix socket
    // name a shorter call rather than a placeholder value — which is the whole of what the deleted
    // `InheritedSocket::absent()` was.
    let controls = adopted.map(Adopted::controls).unwrap_or(&[]);
    #[cfg(target_os = "macos")]
    let plane = Plane::open(options, controls, inherited_socket(adopted)?)
        .map_err(|err| classify_plane(err, confined))?;
    #[cfg(not(target_os = "macos"))]
    let plane = Plane::open(options, controls).map_err(|err| classify_plane(err, confined))?;
    Ok(plane)
}

/// The pg backend's runtime configuration and access mode, from the resolved config and the
/// authoritative flags (PG5/PG6/PG12).
///
/// **The dsn is `resolve::pg::resolve_dsn`'s answer** — the parsed `TINIO_PG_DSN` over
/// `[storage.pg] dsn` — so the value the storage opens and the value the synthetic root was
/// derived from are one; the pool is `[storage.pg] pool` (defaulting to the store's own
/// [`DEFAULT_POOL_SIZE`](crate::config::storage::pg::DEFAULT_POOL_SIZE)); and the mode is the
/// resolution's, `ReadWrite` for a run that takes the ownership guard (PG12) and `ReadOnly` for
/// one that does not. Nothing here connects — the storage is constructed on the runtime.
///
/// **The one `(Config, Access)` builder both the serving half and the doctor's pg leg call**, so
/// the dsn a run opens and the dsn it diagnoses cannot be resolved two ways.
#[cfg(feature = "pg")]
pub(crate) fn pg_backend_config(
    resolution: &Resolution,
    pg_dsn: Option<&Secret>,
) -> Result<(_pg::Config, _pg::Access), Error> {
    let section = resolution.config.pg_section();
    let dsn = resolve::pg::resolve_dsn(section, pg_dsn)?;
    let pool = section.map_or(DEFAULT_POOL_SIZE, |pg| pg.pool);
    let access = if resolution.read_only {
        _pg::Access::ReadOnly
    } else {
        _pg::Access::ReadWrite
    };
    Ok((_pg::Config::new(dsn, pool), access))
}

/// **The feature-off pg refusal** (PG15/§6): a build without the `pg` feature cannot serve
/// `[storage] type = "pg"`, and says so rather than ignoring a configuration it cannot honour.
///
/// The keys parse in every build, so a server that silently served `fs` for such a configuration
/// would be the worst outcome; this is the sentence that keeps it from being silent, and it names
/// the operator's two remedies (the build, or the backend).
///
/// **This is the whole body now.** The feature-on arm is gone: the same `Backend::Pg` that used
/// to answer here now selects [`Served::Pg`](super::Served) and is served. What remains is the
/// build that genuinely cannot, which is why the function is `#[cfg]`-free and the caller gates
/// it — the previous two-arm shape existed only while a pg configuration in a `pg` build was also
/// a refusal.
#[cfg(not(feature = "pg"))]
fn pg_not_servable() -> Error {
    Error::Usage(
        "`[storage] type = \"pg\"` needs the `pg` cargo feature, which this build does not \
         carry: rebuild with it, or set `[storage] type = \"fs\"`"
            .into(),
    )
}

/// The synthetic-root refusal both arms of the R8 guard answer with: this run's root is the leaf
/// a pg dsn derived, but its configuration does not select the pg backend, so serving it would be
/// a filesystem root inside a directory that exists only because a pg dsn was addressed.
///
/// **A usage error, and it names both remedies**: the configuration that makes the run a pg one
/// (`[storage] type = "pg"`), and the alternative a bare `TINIO_PG_DSN` with no `[storage]` may
/// actually have wanted (a positional `DIR`, to serve files). The two call sites — no named
/// configuration (refused before the chain could *create* one) and a named configuration that did
/// not select pg — are one condition (the root is synthetic, the backend is not pg), so the
/// sentence has one home rather than two.
fn pg_root_without_config() -> Error {
    Error::Usage(
        "this run's root is the synthetic one a pg dsn derived, but its configuration does not \
         select `[storage] type = \"pg\"`, so serving it would be a filesystem root inside a \
         directory that exists only because a pg dsn was addressed: add the pg backend to the \
         configuration (with `[storage.pg] dsn`, or the TINIO_PG_DSN this run already carries), \
         or name a positional DIR to serve a filesystem root"
            .into(),
    )
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

/// The control channel's own name, as a refusal about it names the subject: the plane's
/// transports are a pipe name or a unix socket, and the choice is `api_options`' — nothing here
/// re-derives which one this run resolved.
#[cfg(feature = "api")]
const CONTROL_CHANNEL: &str = "control";

/// [`Plane::open`]'s failure, classified — **the child's own control-channel step**, and the
/// fourth site §4's wiring caveat names.
///
/// A confined child that cannot open what it inherited (or, with no local channel in the
/// selection, what it must bind itself) is refused by its confinement rather than by the plane:
/// the launcher hands the channel over precisely because a process inside the container cannot
/// create the pipe at all (`CreateNamedPipeW` is refused there), and the child that tries anyway is answered
/// `ERROR_ACCESS_DENIED` — which arrives here as an io failure and would otherwise read as
/// "I/O error: Access is denied" with nothing naming the channel.
///
/// **The subject is the channel and not the state directory**, which is the one thing this site
/// says differently from the three path sites: naming the directory would be naming the wrong
/// thing on Windows, where the control channel is a pipe name and never a path under the root.
#[cfg(feature = "api")]
fn classify_plane(err: ApiError, confined: bool) -> Error {
    match err {
        ApiError::Io(io) => sandbox::classify_io(io, Refused::Channel(CONTROL_CHANNEL), confined),
        other => Error::from(other),
    }
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
/// **The refusal is a classification before it is a sentence** (follow-up §4): the child that
/// never reported readiness is [`Child::attribution`]'s `Broken` answer — the confinement never
/// applied, so nothing ran confined — and the text this function returns is that value rendered
/// by `sandbox::describe`, not a string assembled here from the status.
///
/// **Only the never-reported child is framed, and the classification says so rather than this
/// function re-testing it** (follow-up §4): the child's stderr is the operator's, unmediated
/// (decision 19), so a refusal that happened *after* readiness and any other non-zero exit are one
/// fact at this boundary, and a status read as a refusal would name a subject this process never
/// saw. The child that reported and then ended is [`Attribution::Ran`] — the arm that classifies
/// nothing — and the frame is withheld by matching that arm. Nothing is missing from stdout over
/// it: a child that reported already spoke on its own stream, and the status is forwarded beside
/// that rather than reclassified here.
///
/// **Both inputs are the child's, so the child is what this reads them from**: the exit status and
/// whether the relay ever saw a terminal event are one value's own state, and
/// [`Child::attribution`] reads both of them — a caller that tested the readiness record beside the
/// classification would be restating what that value already carries.
///
/// [`Attribution::Ran`]: crate::_sandbox::Attribution::Ran
/// [`Child::attribution`]: crate::_sandbox::Child::attribution
/// [`Child::terminal`]: crate::_sandbox::Child::terminal
fn launcher_exit(status: ExitStatus, child: &Child) -> (Option<String>, u8) {
    // A child killed by a signal has no exit code; 1 is the operational failure that is. A code
    // outside `u8` is not this process's to invent either, and answers the same way — the shape
    // the exit-code mapping had before the decision moved here.
    let code = match status.code() {
        Some(code) => u8::try_from(code).unwrap_or(1),
        None => 1,
    };
    // **The classification is the child's own, and this is the one match on it** (follow-up §4):
    // `attribution` answers `Broken` for the child that never reported and `Ran` for the child
    // that reported and then ended. Only the first is framed, and the text is that value rendered
    // by `describe` rather than a string assembled here — a second definer of one sentence would
    // let the classification and the operator's text drift.
    let attribution = child.attribution(&status);
    match &attribution {
        Attribution::Broken { .. } => (Some(sandbox::describe(&attribution)), code),
        // **The child that reported owes stdout nothing** — a listener, or a fatal startup
        // failure it already framed on its own stream: the code is forwarded alone, and
        // deliberately without a classification, because a refusal after readiness is
        // indistinguishable here from any other non-zero exit (decision 19 leaves this side no
        // text to read).
        Attribution::Ran => (None, code),
    }
}

/// The `state` file **this launcher** published for the child it just supervised, removed
/// best-effort on the way out.
///
/// **The file is the launcher's, and this is the launcher's half of that ruling** — the Windows
/// confinement arm's `confine_or_launch` is the other half. The launcher is the process that
/// publishes the token and the pid the file names, so it is the one that creates and takes the
/// file back. **Ownership is the whole rule** — no descriptor keeps the confined child out of the
/// file (`docs/superpowers/specs/2026-10-15-windows-file-dacl-removal-design.md`), so the
/// arrangement is the one the launch already had.
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
fn verbosity(resolution: &Resolution) -> log::Verbosity {
    resolution
        .config
        .log
        .as_ref()
        .map_or_else(log::Verbosity::default, |log| log.verbosity)
}

/// The effective operational log format, resolved the same way [`verbosity`] is.
fn format(resolution: &Resolution) -> log::Format {
    resolution
        .config
        .log
        .as_ref()
        .map_or_else(log::Format::default, |log| log.server_log_format)
}

/// The access-log format (`[log] access_log_format`; `combined` by default).
///
/// There is deliberately no flag and no variable for it — `contracts/config.md`'s list has
/// none — so the config file is the only level there is.
fn access_format(resolution: &Resolution) -> log::AccessFormat {
    resolution
        .config
        .log
        .as_ref()
        .map_or_else(log::AccessFormat::default, |log| {
            log.access_log_format.clone()
        })
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
        fs::{self, OpenOptions, create_dir_all, read_dir, read_to_string},
        io::Write as _,
        path::{Path, PathBuf},
        process::Command,
        thread,
    };

    use tempfile::TempDir;

    use super::*;
    use crate::_sandbox::{Attribution, Child, Terminal};
    #[cfg(feature = "lifecycle")]
    use crate::{_fs::testing, _server::Capabilities};

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

        let (refusal, code) = launcher_exit(status, &child);

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

    /// **The launcher's own refusal is a classification before it is a sentence** (follow-up
    /// §4): the exit that produces the frame above is [`Attribution::Broken`] — the confinement
    /// never applied, so nothing ran confined — which is the value [`Child::attribution`] answers
    /// for a child that never reported and the text `sandbox::describe` renders back out.
    ///
    /// **What this leg adds to the one above is the value rather than the text**: that leg pins
    /// the sentence an operator reads, while this one pins that the sentence is not assembled at
    /// the call site — a `launcher_exit` that built its own string would fail here even with the
    /// same words, because the classification behind them would have drifted out of reach.
    #[test]
    fn the_launchers_refusal_is_the_attributions_own_rendering() {
        let mut child = dying(3);
        let status = supervise(&mut child, || ()).expect("the child is reaped");
        assert!(
            child.terminal().is_none(),
            "nothing relayed, so the record is the 'never reported' answer"
        );

        let attribution = child.attribution(&status);
        assert!(
            matches!(attribution, Attribution::Broken { .. }),
            "a child that never reported is a confinement that never applied: {attribution:?}"
        );
        let expected = sandbox::describe(&attribution);

        let (refusal, code) = launcher_exit(status, &child);

        assert_eq!(
            refusal.as_deref(),
            Some(expected.as_str()),
            "the frame is the classification's rendering, not a second spelling of it"
        );
        assert_eq!(
            code, 3,
            "and the status is still forwarded rather than rewritten"
        );
    }

    /// **The other direction, and the one a launcher that framed unconditionally would break**: a
    /// child that did report is a server that started, so a perfectly good run must carry no
    /// `error` frame for its consumer to misread.
    ///
    /// **The relay is a real thread rather than an `Option` handed to the classifier**, which is
    /// the whole of what moved here: the readiness a caller reasoned about by hand is now the
    /// child's own record, joined by [`Child::wait`] exactly as a launch joins it — and the
    /// classification this leg pins beside the frame is the same value's own answer
    /// ([`Child::attribution`]'s `Ran`), so the rule that such a child is framed nothing has one
    /// home rather than one here and one at the match.
    #[test]
    fn a_child_that_reported_is_never_framed() {
        // A thread that already knows the answer stands in for the relay: what the wait joins is
        // the handle, and what it keeps is the thread's own return value.
        let relay =
            thread::spawn(|| Some(Terminal::Listening("127.0.0.1:9000".parse().expect("addr"))));
        let mut child = dying(0).with_relay(relay);
        let status = supervise(&mut child, || ()).expect("the child is reaped");
        assert!(
            child.terminal().is_some(),
            "the relay announced, so this is the child that served"
        );
        assert!(
            matches!(child.attribution(&status), Attribution::Ran),
            "a child that reported and then ended has nothing this side can classify"
        );

        let (refusal, code) = launcher_exit(status, &child);

        assert_eq!(refusal, None, "a child that reported owes stdout nothing");
        assert_eq!(code, 0, "and its status is still the launcher's answer");
    }

    /// A resolution of the shape a read-only run resolves to: the served root and a state
    /// directory **beside** it, which is where read-only mode puts state and the only shape the
    /// launcher will serve (an in-root one is refused before this point).
    fn read_only(root: &Path, state_dir: &Path) -> Resolution {
        Resolution {
            pg_root: None,
            root: root.to_path_buf(),
            config: Config::default(),
            identity: Default::default(),
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
    /// **A dsn-addressed pg run is refused, and refused before anything is written** (ruling R8).
    ///
    /// A bare `tinio server` with a pg dsn exported and no `DIR` resolves the dsn's synthetic root
    /// (`resolve::locate`'s fallback). Without this guard the run falls through to the filesystem
    /// backend — the synthetic root carries no `[storage]` section, so the backend check further
    /// down answers `fs` — and it *creates* a configuration under that leaf on the way. The
    /// subject here is a scratch leaf under a temp directory, so the leg is also the proof that the
    /// refusal precedes the resolution: the root must not exist afterwards.
    #[test]
    fn a_pg_dsn_with_no_dir_is_refused_before_anything_is_created() {
        use crate::{
            _util::secret::{Secret, SecretKey},
            commands::EnvArgs,
        };

        let scratch = TempDir::new().expect("temp");
        let root = scratch.path().join("pg-root");
        let dsn = Secret::from(SecretKey::from(
            "postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable".to_owned(),
        ));
        let located = Located {
            root: root.clone(),
            read_only: false,
            state_dir: root.join(".tinio"),
            // The pg arm ran: this is the record the guard reads.
            pg_root: Some(root.clone()),
            config: None,
            state_dir_override: Some(root.clone()),
            config_doc: None,
        };
        let args = Args {
            env: EnvArgs {
                pg_dsn: Some(dsn),
                ..EnvArgs::default()
            },
            ..Args::default()
        };

        let err = run(args, located, None, &Provenance::default())
            .err()
            .expect("a pg dsn-addressed run is not served");
        assert!(err.to_string().contains("pg"), "{err}");
        assert!(!root.exists(), "the refused run created {root:?}");
    }

    /// **F1's shape, on the serving half**: an fs run whose `TINIO_STATE_DIR` *is* the served root,
    /// with a pg dsn exported, must not be refused. The guard reads two values — `Located::pg_root`
    /// and the root — and the dsn is not one of them, so an operator who exported the variable
    /// keeps serving their directory. The leg states the pair exactly as `run` consumes it, with
    /// the scenario's arguments in hand.
    #[test]
    fn an_fs_run_that_names_its_own_root_as_the_state_dir_is_not_refused() {
        use crate::{
            _util::secret::{Secret, SecretKey},
            commands::EnvArgs,
        };

        let scratch = TempDir::new().expect("temp");
        let root = scratch.path().to_path_buf();
        let args = Args {
            env: EnvArgs {
                pg_dsn: Some(Secret::from(SecretKey::from(
                    "postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable".to_owned(),
                ))),
                ..EnvArgs::default()
            },
            ..Args::default()
        };
        let located = Located {
            root: root.clone(),
            read_only: false,
            state_dir: root.join(".tinio"),
            // The filesystem arm ran, whatever the environment holds.
            pg_root: None,
            config: None,
            state_dir_override: Some(root.clone()),
            config_doc: None,
        };

        assert!(
            args.env.pg_dsn.is_some(),
            "the scenario: the variable is exported"
        );
        assert!(
            !is_pg_root(&located.root, located.pg_root.as_deref()),
            "an fs run is not a pg one, so the guard does not fire and it serves normally"
        );
    }

    /// The **second** arm of the synthetic-root guard: a run whose root is the dsn's synthetic
    /// leaf but whose *named* configuration does not select pg is refused — serving it would be a
    /// filesystem root inside a directory that exists only because a pg dsn was addressed. The
    /// named document is read (never minted), so nothing is created.
    #[test]
    fn a_synthetic_root_with_a_non_pg_config_is_refused() {
        let scratch = TempDir::new().expect("temp");
        let root = scratch.path().join("pg-root");
        let config = scratch.path().join("fs.toml");
        fs::write(&config, "version = 1\n[storage]\ntype = \"fs\"\n")
            .expect("write the fs configuration");
        let located = Located {
            root: root.clone(),
            read_only: false,
            state_dir: root.join(".tinio"),
            // The pg arm ran (a dsn-addressed root), but the named document is not pg.
            pg_root: Some(root.clone()),
            config: Some(config),
            state_dir_override: Some(root.clone()),
            config_doc: None,
        };

        let err = run(Args::default(), located, None, &Provenance::default())
            .err()
            .expect("a synthetic root with a non-pg config is refused");
        assert!(err.to_string().contains("pg"), "{err}");
        assert!(!root.exists(), "the refused run created {root:?}");
    }

    /// **A pg configuration needs the `pg` feature** (PG15/§6): a build without it refuses rather
    /// than silently serving a filesystem root, and the sentence names the build. This is the case
    /// the interim refusal's own leg covered, kept because the keys parse in every build — the
    /// refusal is what keeps a configuration the build cannot honour from being ignored. Nothing
    /// is created: the named document is read, not minted, and the refusal precedes the
    /// directories.
    #[cfg(not(feature = "pg"))]
    #[test]
    fn a_pg_configuration_is_refused_without_the_feature() {
        let scratch = TempDir::new().expect("temp");
        let root = scratch.path().join("pg-root");
        let config = scratch.path().join("pg.toml");
        fs::write(
            &config,
            "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\n\
             dsn = \"postgres://user:password@db.example:5432/tinio\"\n",
        )
        .expect("write the pg configuration");
        let located = Located {
            root: root.clone(),
            read_only: false,
            state_dir: root.join(".tinio"),
            pg_root: Some(root.clone()),
            config: Some(config),
            state_dir_override: Some(root.clone()),
            config_doc: None,
        };

        let err = run(Args::default(), located, None, &Provenance::default())
            .err()
            .expect("a pg configuration needs the feature");
        let rendered = err.to_string();
        assert!(rendered.contains("pg"), "{rendered}");
        assert!(
            rendered.contains("feature"),
            "the remedy names the build: {rendered}"
        );
        assert!(!root.exists(), "the refused run created {root:?}");
    }

    /// **A pg configuration selects the pg backend, and its construction inputs resolve** — the
    /// two things `run` now does instead of refusing (the interim refusal this slice replaced).
    /// The leg connects to nothing: the storage is built on the runtime, so selection is a pure
    /// decision ([`pg_backend_config`]) and is exactly what a database-free run can prove. The
    /// round trip itself is a `serve`-level leg (`serve::tests`).
    #[cfg(feature = "pg")]
    #[test]
    fn a_pg_configuration_selects_the_pg_backend() {
        use secrecy::ExposeSecret;

        use crate::{
            _util::secret::{Secret, SecretKey},
            commands::EnvArgs,
        };

        const DSN: &str = "postgres://user:hunter2@db.example:5432/tinio";
        const OVERRIDE: &str = "postgres://user:envpw@db.example:5432/other";

        fn resolution(dsn: Option<&str>, read_only: bool) -> Resolution {
            let line = dsn.map_or(String::new(), |dsn| format!("dsn = \"{dsn}\"\n"));
            let text = format!("version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\n{line}");
            Resolution {
                pg_root: None,
                root: PathBuf::from("/srv/pg"),
                config: Config::parse(&text).expect("the pg configuration parses"),
                identity: Default::default(),
                config_text: text,
                config_path: PathBuf::from("/srv/pg/.tinio/.tinio.toml"),
                read_only,
                state_dir: PathBuf::from("/srv/pg/.tinio"),
                state_dir_override: None,
                config_error: None,
            }
        }

        let writable = resolution(Some(DSN), false);
        assert_eq!(
            writable.config.backend(),
            Backend::Pg,
            "the selector is the configuration's"
        );
        let (config, access) = pg_backend_config(&writable, None).expect("the section's dsn");
        assert_eq!(config.pool(), 8, "PG9's default pool size");
        assert_eq!(
            config.dsn().expose_secret().as_ref(),
            DSN,
            "the section's own dsn is the one the storage opens"
        );
        assert!(
            matches!(access, _pg::Access::ReadWrite),
            "a writable run takes the ownership guard (PG12)"
        );

        // A read-only run selects the mode the constructor takes no guard for (PG12).
        let (_, access) =
            pg_backend_config(&resolution(Some(DSN), true), None).expect("the section's dsn");
        assert!(matches!(access, _pg::Access::ReadOnly));

        // `TINIO_PG_DSN` overrides the section (PG5), and the override is what the synthetic
        // root and the storage both see.
        let args = Args {
            env: EnvArgs {
                pg_dsn: Some(Secret::from(SecretKey::from(OVERRIDE.to_owned()))),
                ..EnvArgs::default()
            },
            ..Args::default()
        };
        let (config, _) =
            pg_backend_config(&writable, args.env.pg_dsn.as_ref()).expect("the override answers");
        assert_eq!(config.dsn().expose_secret().as_ref(), OVERRIDE);
    }

    /// A pg configuration with no dsn from either source is refused — still by `resolve_dsn`,
    /// naming both remedies, unchanged by the backend now being served.
    #[cfg(feature = "pg")]
    #[test]
    fn a_pg_configuration_without_a_dsn_is_refused() {
        let text = "version = 1\n[storage]\ntype = \"pg\"\n";
        let resolution = Resolution {
            pg_root: None,
            root: PathBuf::from("/srv/pg"),
            config: Config::parse(text).expect("the pg configuration parses"),
            identity: Default::default(),
            config_text: text.to_owned(),
            config_path: PathBuf::from("/srv/pg/.tinio/.tinio.toml"),
            read_only: false,
            state_dir: PathBuf::from("/srv/pg/.tinio"),
            state_dir_override: None,
            config_error: None,
        };

        let err = pg_backend_config(&resolution, None).expect_err("no dsn: the run is refused");
        let rendered = err.to_string();
        assert!(rendered.contains("TINIO_PG_DSN"), "{rendered}");
        assert!(rendered.contains("dsn"), "{rendered}");
    }

    /// **A confinement and a pg store cannot be honoured together** (the sandbox crate's finding):
    /// the confinement cuts the confined process's outbound connections and the pg store's
    /// connection is such a path, so the combination is refused up front — naming both settings —
    /// rather than served unservable. Each half alone is not a refusal: a filesystem run keeps the
    /// confinement, and a pg run that asked for none is not refused.
    #[test]
    fn a_confinement_under_pg_is_refused_and_the_fs_or_unconfined_run_is_not() {
        use crate::sandbox::{Channel, Request};

        let asked = Request {
            channel: Some(Channel::Flag),
        };
        let refusal = sandbox::refuse_pg_under_confinement(&asked, Backend::Pg)
            .expect("a pg run under a confinement is refused");
        // A usage error: the operator asked for two settings that cannot both hold.
        assert!(matches!(refusal, Error::Usage(_)), "{refusal}");
        let text = refusal.to_string();
        assert!(text.contains("--sandbox"), "names the channel: {text}");
        assert!(text.contains("pg"), "names the backend: {text}");
        assert!(text.contains("confinement"), "{text}");

        assert!(
            sandbox::refuse_pg_under_confinement(&asked, Backend::Fs).is_none(),
            "a filesystem run serves from local files and keeps its confinement"
        );
        assert!(
            sandbox::refuse_pg_under_confinement(&Request { channel: None }, Backend::Pg).is_none(),
            "a pg run that asked for no confinement is not refused"
        );
    }

    /// **LC23's plane half**: `[s3] lifecycle = false` injects no admin, so the three
    /// `/buckets/{bucket}/lifecycle` routes answer the `404` a stripped build's do and nothing can
    /// replace a stored configuration the run will never enforce. The same document with the
    /// capability on injects one, so the switch — never a missing storage — is what the assertion
    /// sees.
    #[cfg(feature = "lifecycle")]
    #[tokio::test]
    async fn a_disabled_capability_injects_no_lifecycle_admin() {
        let dir = TempDir::new().expect("a scratch root");
        let storage = FsStorage::new(dir.path(), testing::fs_options()).expect("a storage");
        let resolution = |lifecycle: bool| Resolution {
            pg_root: None,
            root: PathBuf::from("/srv/data"),
            config: Config {
                s3: Some(config::s3::Config {
                    capabilities: Capabilities {
                        lifecycle,
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            identity: Default::default(),
            config_text: String::new(),
            config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
            read_only: false,
            state_dir: PathBuf::from("/srv/data/.tinio"),
            state_dir_override: None,
            config_error: None,
        };
        assert!(
            lifecycle_admin(&storage, &resolution(true)).is_some(),
            "the capability on injects the admin the plane's routes need"
        );
        assert!(
            lifecycle_admin(&storage, &resolution(false)).is_none(),
            "a disabled capability injects no admin: rules editable but never run are the \
             surprise the ruling removes"
        );
    }
}
