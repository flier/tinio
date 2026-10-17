//! §1's serving half — the steps after the runtime exists.

#[cfg(feature = "versioning")]
use std::future::Future;
#[cfg(feature = "api")]
use std::sync::Arc;
use std::{fmt::Display, net::SocketAddr, process::ExitCode, time::Duration};

use futures::future::{Either, select};
use tokio::{
    net::TcpListener,
    sync::watch,
    task::JoinError,
    time::{error::Elapsed, timeout},
};

#[cfg(all(test, feature = "pg"))]
use super::serving::pg_pass;
#[cfg(test)]
use super::serving::{fs_cleanup, multipart_grace_for};
use super::{
    Args, Prepared, Ready, Served,
    serving::{ServingBackend, sweep_options_for},
};
#[cfg(feature = "versioning")]
use crate::_fs::FsStorage;
#[cfg(test)]
use crate::_fs::sweep;
#[cfg(unix)]
use crate::notify;
use crate::{
    _config,
    _core::{mode::AccessMode, storage::Storage},
    _sandbox::{Adopted, platform_facts},
    _server::{
        Capabilities, DataPlane, Identity, PlaneConfig,
        identity::{self, IdentityFromConfig},
    },
    Error,
    commands::EnvArgs,
    events,
    resolve::{Pipelines, Resolution},
    sandbox, signal,
};
#[cfg(feature = "pg")]
use crate::{_core::storage::Error as StorageError, _pg::PgStorage};

/// The drain bound `contracts/cli.md:43` promises: "cease accepting, drain ≤ 10 s, remove `state`
/// and the socket, exit 0".
///
/// **The bound lives here because neither plane carries one.** `tinio-api`'s transports drain
/// every connection they already accepted ("runs to completion", `transport.rs`) and say so on
/// `Plane::shutdown`; the window is the CLI's, and this is the CLI.
const DRAIN_BOUND: Duration = Duration::from_secs(10);

/// How long the serving half waits for the background passes once the planes have drained and
/// `state` and the socket are gone.
///
/// The scanner sleeps in `max_wait` chunks (15 s, `scanner_and_sweeper` below), so its own exit
/// is up to a chunk away from the shutdown signal. This bound is what keeps the process's exit
/// inside `contracts/cli.md:43`'s window instead of following that pacing: a pass abandoned here
/// is idempotent work that resumes on the next start, so waiting longer buys nothing the next run
/// does not do anyway.
pub(super) const BACKGROUND_BOUND: Duration = Duration::from_secs(2);

/// How long the toggle-off warning's count may run before its message degrades to the count-free
/// spelling ([`bounded_versioning_warning`]).
///
/// **The same two seconds as [`BACKGROUND_BOUND`], and a name of its own anyway**: that bound
/// covers the scanner and the sweeper once the planes have drained, and its length is the
/// scanner's 15 s pacing chunk read against the contract's window; this one covers a single
/// whole-table read on the blocking pool, which has no chunking to pace. What the two share is
/// the reason: work started long before the stop must not outlive the exit — and this work is
/// awaited by the shutdown path's join, so an unbounded wedged read would hold the process open
/// past every other bound.
#[cfg(feature = "versioning")]
const WARNING_BOUND: Duration = Duration::from_secs(2);

/// §1's serving half — the steps after the runtime exists — dispatched by `run::on_runtime` on
/// the process's **one** runtime (decision 2).
///
/// The repair is deliberately the first thing here, and the plane's single-instance gate has
/// already happened in [`run()`](crate::commands::start::run): decision 12's order is
/// single-instance bind → repair → data plane bound → api plane bound → `state` → readiness, so
/// an instance that loses the bind never reaches a repair that would rewrite the state a live
/// server is using.
///
/// # Errors
///
/// A bind failure (the port is taken, or the inherited listener cannot be adopted), a
/// management transport that cannot be bound, a plane that stopped while the process was
/// still serving, and the plane's drain at the end of the run. A shutdown the `stop` channel
/// drove — the ordinary signal or `POST /stop` path — returns `ExitCode::SUCCESS`: every arm
/// ends cleanly on that channel, and a drain is not a failure.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::commands::start::{Ready, serve};
///
/// // A `Ready` is built by `run` and by nothing else, so this example takes one rather than
/// // fabricating it: what it shows is the call, which is all the serving half is from here.
/// # async fn drive(ready: Ready) -> Result<(), tinio_cli::Error> {
/// serve(ready).await?;
/// # Ok(())
/// # }
/// ```
/// The resolution and process state §1's serving half runs on, bundled so [`serve_backend`]
/// takes one value beside the constructed backend.
struct Serving {
    resolution: Resolution,
    args: Args,
    adopted: Option<Adopted>,
    handles: signal::Handles,
    pipelines: Pipelines,
    on_ready: Option<Box<dyn FnOnce() + Send>>,
}

impl Ready {
    /// The constructed backend and the serving state, split apart — **the field
    /// list `serve` used to destructure and rebuild by hand, written once**.
    /// `Ready`'s fields are private to the `start` module, so this lives beside
    /// [`Serving`], which carries the same six from the same move.
    fn split(self) -> (Served, Serving) {
        let Ready {
            resolution,
            args,
            adopted,
            handles,
            backend,
            pipelines,
            on_ready,
        } = self;
        (
            backend,
            Serving {
                resolution,
                args,
                adopted,
                handles,
                pipelines,
                on_ready,
            },
        )
    }
}

pub async fn serve(ready: Ready) -> Result<ExitCode, Error> {
    let (backend, serving) = ready.split();
    match backend {
        Served::Fs(prepared) => serve_backend(*prepared, serving).await,
        #[cfg(feature = "pg")]
        Served::Pg { config, access } => {
            // **The pg arm's whole startup is framed here, at one site** (decision 19). `serve` is
            // reached as `on_runtime(start::serve(*ready))`, so it never passes through the
            // framing arm that frames `start::run`'s error; a pg startup refusal raised below would
            // otherwise exit with no stdout frame, and a client waiting on a readiness frame would
            // time out instead of learning why. **Framing the call rather than each step** is what
            // makes it regression-proof: [`prepare_pg`] owns every fallible step of the pg startup
            // (the construction, the management-plane bind, and anything added later), so a new
            // step inherits this frame instead of having to remember it.
            let (prepared, serving) = prepare_pg(config, access, serving)
                .await
                .inspect_err(frame_startup)?;
            serve_backend(prepared, serving).await
        }
    }
}

/// Emit the stdout error frame a **pre-readiness serving failure** owes (decision 19), and hand
/// the error on unchanged.
///
/// **Why one function.** `serve` (and so `serve_backend`) is reached from `on_runtime`, not
/// through the arm that frames `start::run`'s error, so a fallible step that runs before `state`
/// is published would otherwise exit with only the `tinio: …` stderr line and a client waiting on
/// a readiness frame would time out instead of learning why. **Every pre-readiness `?` in [`serve`]
/// and [`serve_backend`] frames through this** — the pg construction's `inspect_err` on
/// [`prepare_pg`] included — so the wording has one writer, and the two halves' refusals read alike.
fn frame_startup<E: Display>(err: &E) {
    events::emit_error(&err.to_string());
}

/// The data-plane listener: the resolved bind address, the listener (a launcher's inherited one,
/// or a fresh bind), and its local address — **the pre-readiness fallible steps in one function**
/// so [`serve_backend`] frames them at one site ([`frame_startup`]), and a step added inside here
/// inherits that frame.
async fn bind_served_listener(
    resolution: &Resolution,
    adopted: Option<&Adopted>,
) -> Result<(TcpListener, SocketAddr), Error> {
    let bind = resolution.address()?;
    let listener = match adopted {
        // The listener a launcher handed this child over the inherited descriptor (Task 9).
        Some(adopted) => sandbox::adopted_listener(adopted)?,
        None => TcpListener::bind(bind).await?,
    };
    let address = listener.local_addr()?;
    Ok((listener, address))
}

/// Construct the pg backend and bind its management plane — **the pg arm's whole startup**, in one
/// function so its refusals are framed once by [`serve`]'s caller (decision 19).
///
/// Order is the ruling, and it is `run()`'s for a filesystem run: `PgStorage::connect` is the
/// single-instance gate (its ownership lock, PG12) and it runs first; the management plane's bind
/// follows, still before the repair `serve_backend` runs.
///
/// **`serving` is taken and returned by value** so the returned future holds no borrow of it across
/// the `connect` await — a `Serving` owns a `Box<dyn FnOnce() + Send>` (the readiness hook) and is
/// therefore `Send` but not `Sync`, so a `&Serving` held across an await would make [`serve`]'s
/// future non-`Send` and break the `tokio::spawn` that drives it.
#[cfg(feature = "pg")]
async fn prepare_pg(
    config: _pg::Config,
    access: _pg::Access,
    serving: Serving,
) -> Result<(Prepared<PgStorage>, Serving), Error> {
    let storage = PgStorage::connect(&config, access)
        .await
        .map_err(StorageError::from)?;
    #[cfg(feature = "api")]
    let plane = super::run::open_management_plane(
        &storage,
        &serving.resolution,
        &serving.args,
        serving.adopted.as_ref(),
        &serving.handles,
        &serving.pipelines,
    )?;
    #[cfg(feature = "api")]
    let prepared = Prepared { storage, plane };
    #[cfg(not(feature = "api"))]
    let prepared = Prepared { storage };
    Ok((prepared, serving))
}

/// §1's serving half over a constructed backend — **the one body, generic across backends**.
///
/// The backend-specific operation the serving half owes — the startup repair — travels through
/// [`ServingBackend`], which also carries the filesystem-only background passes; everything else —
/// the listener, the two planes, `state`, readiness, the drain and the shutdown — is the
/// contract's and is written once. A filesystem backend and a pg backend therefore run the **same**
/// serving code, which is the point of the seam.
///
/// The repair is deliberately the first thing here, and the plane's single-instance gate has
/// already happened: in the pre-runtime half for the filesystem backend (its bound control
/// channel), and in the pg constructor for a pg backend (its ownership lock, PG12). Decision 12's
/// order is single-instance bind → repair → data plane bound → api plane bound → `state` →
/// readiness.
///
/// # Errors
///
/// A bind failure (the port is taken, or the inherited listener cannot be adopted), a
/// management transport that cannot be bound, a plane that stopped while the process was
/// still serving, and the plane's drain at the end of the run. A shutdown the `stop` channel
/// drove — the ordinary signal or `POST /stop` path — returns `ExitCode::SUCCESS`: every arm
/// ends cleanly on that channel, and a drain is not a failure.
async fn serve_backend<S>(prepared: Prepared<S>, serving: Serving) -> Result<ExitCode, Error>
where
    S: ServingBackend,
{
    let storage = prepared.storage;
    #[cfg(feature = "api")]
    let plane = prepared.plane;
    let Serving {
        resolution,
        args,
        adopted,
        handles,
        pipelines,
        on_ready,
    } = serving;

    // The startup repair, post-bind (decision 12): the fast, deterministic items only, and
    // best-effort — a failed stage is warned and readiness proceeds, because the scanner
    // covers the residue. The backend decides what "repair" means.
    //
    // **The repair runs in every build; only the `versioning` feature reads its answer** (it arms
    // the optional post-repair block verify below), so the *binding* — not the call — is gated.
    #[cfg(feature = "versioning")]
    let repaired = storage.repair(&resolution).await;
    #[cfg(not(feature = "versioning"))]
    let _ = storage.repair(&resolution).await;
    // **The run's access mode, bound once** and read by the `versioning` block below and by the
    // data plane and the background pass further down — so it is unconditional (a `versioning`-off
    // or `api`-off build still reads it), and the earlier placement is what lets those readers
    // share one value instead of recomputing `resolution.read_only`.
    let access = AccessMode::from(resolution.read_only);

    // Crash evidence arms the design's optional post-unclean-shutdown verify (2026-10-06
    // review): when the repair actually cleared residue, every versioned bucket's stored
    // blocks are re-read and re-hashed in the background — report-only, the corrupt count on
    // `tinio_storage_blocks_corrupt_total`. Serving is not delayed: the trust-the-store read
    // path is compensated, never gated. The backend decides what "verify" means (the filesystem
    // backend re-hashes its blocks; a backend whose read path verifies does nothing).
    #[cfg(feature = "versioning")]
    if repaired {
        let storage = storage.clone();
        tokio::spawn(async move {
            storage.verify_blocks(access).await;
        });
    }

    // One shutdown path, three entry points: the signal handlers flip this channel, `/stop`
    // sends on it, and every serving arm below ends on it.
    let mut stop = handles.shutdown.clone();
    // **A confined child arms the forwarded-stop relay** (design §3 step 9, decision 20): its
    // launcher forwards a stop rather than killing it, and without this hook the stop would meet
    // the default disposition and end the process where it stands instead of draining — the child
    // dies by signal, `status.code()` is `None`, and the launcher's exit-status forwarding
    // (design §3 step 8) turns a graceful stop into an operational failure. Only a launched child
    // has a launcher to forward one, which is why the arm is the payload's.
    //
    // **Both launcher platforms, and the gate says so rather than listing Windows alone.** The
    // two differ in how the stop reaches them — a Windows container signals only its main process,
    // so the relay is required there, while macOS's `launchd` signals the whole job and the relay
    // is defensive (design decision 20's grade table) — but a child of either launcher is a
    // process whose supervisor forwards the stop, so both need somewhere for it to land. An
    // in-place platform never reaches this: Linux confines *this* process, so the supervisor
    // signals the server itself and there is nothing to forward.
    if adopted.is_some() && platform_facts().launches_child {
        let stopper = handles.stopper.clone();
        sandbox::on_stop(move || {
            let _ = stopper.send(true);
        })
        .inspect_err(frame_startup)?;
    }
    // **The built-in pair is a loopback-only development credential** (audit run-2, P2): a config
    // with no `[auth]` section serves `identity::root_auth_pair`'s documented fallback, which is
    // not a credential at all. **The refusal for a bind another host can reach is `run`'s static
    // half now** — raised before the fork and before the directories, so a refused start has not
    // repaired the root or jailed itself — which leaves the loopback warning as the whole of what
    // this line is: the same policy rule renders both sentences
    // (`Resolution::credential_policy`), and a configured pair is unaffected (the pair the
    // operator chose is theirs to expose, and `warn_if_non_loopback` below is that bind's
    // report).
    if let Some(message) = resolution.credential_policy()?.warning() {
        warn(&message);
    }
    // **The pre-readiness fallible steps are framed at their own sites** (decision 19): a client
    // waiting on a readiness frame must learn why none came, and `serve` is reached from
    // `on_runtime` rather than through the arm that frames `start::run`'s error — so without this
    // a port already in use exits 1 with only the `tinio: …` stderr line. `bind_served_listener`
    // gathers the resolved address, the bind and the local address, so a step added inside it
    // inherits the frame; [`frame_startup`] is the one wording. The filesystem path's **ordinary**
    // cases are unchanged: a successful bind emits nothing, and the management plane's refusal in
    // `start::run` is still framed by `run.rs` (this only adds a frame where there was none).
    let (listener, address) = bind_served_listener(&resolution, adopted.as_ref())
        .await
        .inspect_err(frame_startup)?;
    warn_if_non_loopback(address);
    let caps = capabilities_for(&resolution);
    // The fail-open defense's startup half (spec 2026-10-03): the feature is compiled in but
    // `[s3] versioning` is off, so a root that still holds version data keeps serving its current
    // versions while every version operation is refused. The run starts either way — refusing to
    // start would make a config flip strand served data — and this is the one place that says so.
    // **The count runs off the pre-serve path**: it is a whole-table scan of the
    // bucket-versioning table, so it rides a task of its own rather than delaying the listener,
    // the readiness event and both planes; the warning lands a moment later, and its ordinary
    // message is unchanged.
    //
    // **The scan carries `WARNING_BOUND`** (`bounded_versioning_warning` below): the task's own
    // work is bounded, which is what lets the shutdown path's join stay unbounded — a wedged read
    // degrades the message instead of holding the process open, and the degraded spelling still
    // names the condition rather than a count it does not have.
    //
    // **The handle is held, not dropped**: the warning is the task's whole purpose, and a run
    // torn down before the task is polled (a start that meets an immediate stop) would lose the
    // message with the handle. It is joined on the shutdown path below, where the other spawned
    // tasks are.
    #[cfg(feature = "versioning")]
    let versioning_warning = tokio::spawn({
        let storage = storage.clone();
        async move {
            let scan = storage.versioning_warning(&caps);
            if let Some(message) = bounded_versioning_warning(WARNING_BOUND, scan).await {
                warn(&message);
            }
        }
    });
    // The data plane carries no metrics hook: the interim `/metrics` route on its listener is
    // retired, so there would be no reader for one. The cheap families are still published — the
    // adapter above calls them on the management plane's reader, which is the same registry.
    let data = plane_for(storage.clone(), PlaneConfig { caps, access }, &resolution);

    // `STOPPING=1` belongs to the **moment the shutdown begins**, not to the moment it ends:
    // watching the channel is what catches every entry point (a signal, `POST /stop`) at the
    // instant it fires, which is exactly the window systemd would otherwise not see. Emitting
    // it after the join would announce the drain only once it had finished.
    #[cfg(unix)]
    tokio::spawn({
        let mut watching = stop.clone();
        async move {
            let _ = watching.changed().await;
            notify::stopping();
        }
    });

    // Both planes are up before `state` exists (decision 12), which is why the api plane is
    // spawned: its `serve` borrows the plane, and `state` is published after it. One `Arc`,
    // shared by the spawn, `publish` and `shutdown` — a second `Arc::new` would be a second
    // handle onto the same value and a moved-from one at that.
    #[cfg(feature = "api")]
    let plane = Arc::new(plane);
    #[cfg(feature = "api")]
    let api = tokio::spawn({
        let (plane, stop) = (plane.clone(), stop.clone());
        async move { plane.serve(stop).await }
    });
    let data = tokio::spawn(data.serve(listener, stop.clone()));
    let background =
        tokio::spawn(storage.background(stop.clone(), access, sweep_options_for(&resolution)));

    #[cfg(feature = "api")]
    // **A launched child never writes `state`.** Its launcher does, and the reason is the same on
    // every platform: `state` must name the launcher — whose `--daemon` readiness check compares
    // the file's pid against the pid it spawned, and whose `Plane::shutdown` removal guard expects
    // itself — and the launch token is minted by the publisher, which is the launcher
    // (`crate::sandbox::macos`'s module doc is that reason's home). Ownership is the whole reason
    // now: no descriptor keeps this process out of the file
    // (`docs/superpowers/specs/2026-10-15-windows-file-dacl-removal-design.md`). Either way the
    // launcher publishes the file once *this* process
    // reports it is listening. The skip is not a degradation: the file exists by the time any
    // client can act on the readiness event this line is part of. `adopted.is_none()` is every
    // process that owns its own `state`, which is every unconfined run and every in-place
    // platform.
    if adopted.is_none() {
        plane.publish(address.port()).inspect_err(frame_startup)?; // `state` is written here
    }
    events::ready(address); // the readiness event, on stdout (§8; decision 19)
    // The operational summary, on stderr: endpoint, root and credentials status — the event
    // stream stays stdout's, and this is why.
    eprintln!(
        "serving {root} on {address} ({mode}; credentials {origin})",
        root = resolution.root.display(),
        mode = if resolution.read_only {
            "read-only"
        } else {
            "read-write"
        },
        origin = credential_origin(&args.env, &resolution.config),
    );
    if let Some(on_ready) = on_ready {
        on_ready();
    }
    #[cfg(unix)]
    notify::ready(); // READY=1, inert off systemd
    #[cfg(unix)]
    notify::watchdog();

    // **No metrics ticker here**, and that is deliberate rather than an omission. The scrape's
    // cadence and the storage scan's schedule belong to the plane: its `serve` spawns the
    // reader (`gauges.spawn_reader(stop)`), `Gauges::refresh` is the only method a scrape calls
    // and it never scans, and `counts()` — this adapter's expensive half — is called by that
    // reader alone. A second ticker in this module would be a cache beside a reader, and the
    // two would have to agree on a period neither of them owns.

    // **The order of these three awaits is the contract's, not a preference.** The api and data
    // planes are joined first and `Plane::shutdown` — the only ordinary site that removes `state`
    // and the socket — runs before the background arm is awaited, because
    // `contracts/cli.md:56` promises "cease accepting, drain ≤ 10 s, remove `state` and the
    // socket, exit 0". The background arm's own exit is the scanner's pacing boundary (`max_wait`,
    // 15 s), so joining all three together put the removal wherever that chunk happened to land —
    // measured at ~15 s after an accepted `stop`, outside the promise. Do not simplify this back
    // into one `join!`; the removal window is the whole reason the three are apart.
    #[cfg(feature = "api")]
    let drain = async { tokio::join!(api, data) };
    // Not an `async { data.await }`: the single handle is already a future, and clippy's
    // `redundant_async_block` is a `-D warnings` error in the api-less graphs.
    #[cfg(not(feature = "api"))]
    let drain = data;
    tokio::pin!(drain);
    let stopped = stopping(&mut stop);
    tokio::pin!(stopped);
    // **The bound is armed by a stop — or by a plane that ended on its own — never by the mere
    // passage of time.** `DRAIN_BOUND` is the *drain's* limit, and a server nobody has asked to
    // stop has no drain in flight: arming the timeout before this wait ended every idle run ~12 s
    // after readiness (the 10 s bound here, then `BACKGROUND_BOUND`), removed `state` and exited —
    // the defect this wait closes. The drain's own arm is the other way a drain begins: a plane
    // that returned without a stop has already stopped serving, and `plane_arm` is what reports
    // it, rather than a timeout that would swallow the answer.
    //
    // **`futures::future::select` rather than `tokio::select!`, and the future it hands back is
    // why.** An `async` block that has already answered panics when it is polled again
    // (resume-after-completion), and these two events are the same event: a stop flips the very
    // channel the planes drain on, so "the stop arrived" and "the planes finished" race — a
    // `select!` that took the stop arm with an already-complete drain arm would re-poll it in the
    // timeout below. `select` returns the arm that did *not* finish, so the timeout only ever
    // wraps a drain that is still pending.
    let drained = match select(drain, stopped).await {
        Either::Left((outcome, _)) => Ok::<_, Elapsed>(outcome),
        Either::Right(((), drain)) => timeout(DRAIN_BOUND, drain).await,
    };
    // **The bound is the promise's, and this is where it is enforced.** A download in flight
    // would otherwise hold `state` and the socket past the window — and past `stop`'s own 15 s
    // refusal bound, which is how the operator meets it. An arm still draining at the bound is
    // *abandoned*, not failed: what is left is a client's connection, no state of ours is
    // inconsistent, and `plane.shutdown()` below still removes what this process owns. So the
    // bound costs a warning and never an exit code — a "drain ≤ 10 s, exit 0" that turned into
    // exit 1 would be the contract read backwards.
    if drained.is_err() {
        tracing::warn!(
            "the planes were still draining at the {DRAIN_BOUND:?} bound; abandoning them so `state` and the socket come out inside the contract's window"
        );
    }

    #[cfg(feature = "api")]
    plane.shutdown().await?; // drain, remove the socket and `state` — inside the drain window

    // Then the background passes, **bounded** so the process's exit is too: a scanner mid-chunk
    // is abandoned at the bound rather than holding the exit open for up to its full pacing
    // chunk. Abandonment is not a failure — the passes' work is idempotent and resumes on the
    // next start — so it leaves the exit code alone; a pass that panicked *inside* the bound
    // still reaches `background_arm` below.
    let background = match timeout(BACKGROUND_BOUND, background).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::warn!("the background passes were abandoned at the shutdown bound");
            Ok(())
        }
    };

    // **Held, not returned yet**, and inspected rather than discarded: every arm ends on the
    // same `stop` channel, so a clean return from all of them *is* the graceful path — while a
    // plane that returned an error (a data listener that stopped accepting, a control channel
    // that could not serve) or a task that did not finish is an operational failure. A run
    // that discarded these would exit 0 with a silent S3 outage on the arms still alive, and
    // the shutdown below still has to run either way: a failed plane must not leave its socket
    // and `state` behind.
    #[cfg(feature = "api")]
    let planes = match drained {
        Ok((api, data)) => {
            plane_arm("the management plane", api).and(plane_arm("the data plane", data))
        }
        // Abandoned at the bound, which is what the bound is *for*: a plane that did not finish
        // is the accepted outcome there, not the operational failure `plane_arm` reports for a
        // task that panicked. The warning above is that arm's whole report.
        Err(_) => Ok(()),
    };
    #[cfg(not(feature = "api"))]
    let planes = match drained {
        Ok(data) => plane_arm("the data plane", data),
        Err(_) => Ok(()),
    };
    let outcome = planes.and(background_arm("the background passes", background));

    // **The toggle-off warning's task is joined here unbounded, and the task's own work is what
    // makes that safe.** Its scan carries `WARNING_BOUND` (`bounded_versioning_warning` below), so
    // a wedged read cannot hold this join: by this line the task has either answered — by an
    // ordinary stop it has long answered, started as it is alongside the planes — or degraded to
    // the count-free message at its own bound. Bounding this *join* instead would re-create the
    // defect one step later: the warning is the task's whole purpose, and an abandoned task
    // prints nothing, while a bounded scan still warns. The two bounds above protect the drain
    // window, and this join sits *after* that window's removal (`plane.shutdown()` and the
    // background await), so waiting here cannot push `state` or the socket past the promise
    // either. Its join error is the other tasks' rule too: a task that did not finish is an
    // operational failure, whatever it was for.
    #[cfg(feature = "versioning")]
    let outcome = outcome.and(background_arm(
        "the toggle-off warning",
        versioning_warning.await,
    ));

    pipelines.shutdown(); // IO first, the DB write pipeline last
    pipelines.drain().await;
    outcome?; // the graceful path: every arm returned cleanly, and this is `Ok(())`
    Ok(ExitCode::SUCCESS)
}

/// Resolves when this process has been asked to stop: at once when the channel already says so,
/// otherwise on the next change.
///
/// **`changed()` alone would not do, and the first read is why.** A `watch` receiver created
/// *after* the flip — or one that has already marked the new value seen — would wait for the
/// *next* change and wait forever, so the question is asked of the current value first.
/// `borrow_and_update()` would be wrong in the other direction: it marks the value seen, which
/// turns a stop that has already arrived into one this function waits past. `borrow()` marks
/// nothing. A dropped sender is read as a stop: nothing is left to serve for, and every arm
/// below treats a clean channel as the graceful path.
async fn stopping(stop: &mut watch::Receiver<bool>) {
    if !*stop.borrow() {
        let _ = stop.changed().await;
    }
}

/// The one spelling of a start's stderr warning: the stream and the `tinio: warning:` prefix are
/// the contract's (`contracts/cli.md`), and every warning arm of this file writes through here so
/// a reword cannot reach one and miss another.
fn warn(message: &str) {
    eprintln!("tinio: warning: {message}");
}

/// The toggle-off warning's message (spec 2026-10-03): `Some` when this build carries versioning,
/// `[s3] versioning` is off, and the root holds version data — the number of buckets that do is
/// one range scan of the bucket-versioning table, never a per-bucket lookup.
///
/// A failed scan warns and answers no message: the count is a report about the run, and turning a
/// transient database failure into a second warning is more honest than silence.
///
/// **The bound on the scan is its caller's** ([`bounded_versioning_warning`]): this function
/// awaits the count without one, so a wedged read would stay pending here forever — the timeout
/// the task applies is what keeps it from holding the shutdown join.
#[cfg(feature = "versioning")]
pub(super) async fn versioning_disabled_warning(
    storage: &FsStorage,
    caps: &Capabilities,
) -> Option<String> {
    if caps.versioning {
        return None;
    }
    match storage.versioned_bucket_count().await {
        Ok(0) => None,
        Ok(count) => Some(format!(
            "[s3] versioning is off but {count} {} version data; current versions keep serving and every version operation is refused",
            if count == 1 {
                "bucket holds"
            } else {
                "buckets hold"
            }
        )),
        Err(err) => {
            tracing::warn!(error = %err, "could not count the buckets holding version data");
            None
        }
    }
}

/// [`versioning_disabled_warning`] with a bound of its own, because the task it runs in is joined
/// by the shutdown path and the scan it awaits — a whole-table read on the blocking pool — has no
/// bound inside it.
///
/// A scan that does not answer inside `bound` degrades to the count-free message ([`WARNING_BOUND`]
/// is the production bound) rather than waiting: the task always completes, the join below can
/// never hang on a wedged read, and the warning still says the toggle is off and the root **may**
/// hold version data — it just never pretends a count. An answered scan is returned untouched,
/// which is what keeps the ordinary message byte-for-byte what it was.
///
/// **Generic over the scan so the timed-out arm is a test's to drive**: the tests pass
/// [`std::future::pending`] under a short bound — the same `Err(Elapsed)` arm the spawn site
/// takes on a wedged storage read, which a real [`FsStorage`] has no seam to produce.
#[cfg(feature = "versioning")]
async fn bounded_versioning_warning<F>(bound: Duration, scan: F) -> Option<String>
where
    F: Future<Output = Option<String>>,
{
    match timeout(bound, scan).await {
        Ok(message) => message,
        Err(_) => Some(
            "[s3] versioning is off and this root may hold version data, but the count timed out; current versions keep serving and every version operation is refused"
                .to_owned(),
        ),
    }
}

/// `contracts/cli.md`: non-loopback bind warns on stderr. Anonymous escalation is US3 (T087)
/// and is not wired until that flag exists.
fn warn_if_non_loopback(address: SocketAddr) {
    if address.ip().is_loopback() {
        return;
    }
    warn(&format!(
        "binding a non-loopback address ({address}); the data plane is reachable from other hosts"
    ));
}

/// One serving arm's outcome, as this process's.
///
/// A plane that returned an error and a task that never finished (a panic inside it) are the
/// same answer: an operational failure the process exits 1 on, naming the arm so the log says
/// which plane stopped. A clean return is what every arm's `stop`-driven ending looks like,
/// and it is the only input that keeps the process's exit code 0.
fn plane_arm<E: Display>(
    name: &str,
    outcome: Result<Result<(), E>, JoinError>,
) -> Result<(), Error> {
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(Error::Operational(format!("{name} stopped: {err}"))),
        Err(err) => Err(Error::Operational(format!("{name} did not finish: {err}"))),
    }
}

/// The background passes' arm: the same rule as [`plane_arm`] with no plane error to report —
/// the scanner and the sweeper answer `()` or a `JoinError`.
fn background_arm(name: &str, outcome: Result<(), JoinError>) -> Result<(), Error> {
    outcome.map_err(|err| Error::Operational(format!("{name} did not finish: {err}")))
}

/// The `[s3]` capability toggles and page-size caps from the resolved config (FR-021); without
/// an `[s3]` section the defaults apply.
///
/// Narrowed to the [`Resolution`] rather than the `Option<Config>` the example took: the
/// resolution always carries a parsed config, so the absent-section case is the only branch
/// left.
fn capabilities_for(resolution: &Resolution) -> Capabilities {
    match resolution.config.s3.as_ref() {
        Some(s3) => Capabilities::from(s3),
        None => Capabilities::default(),
    }
}

/// The data plane's constructor selection (the same-condition rule, spec §6): `caps.acl` on →
/// the identity-backed plane (`new_with_acl` — the configured users and the owner element,
/// every enforced behavior wired together); else the static credential pair.
///
/// **The capability is the only gate.** The `acl` feature is named on this crate's
/// `tinio-server` dependency rather than declared as a feature of this crate, so the
/// constructor and these two identity names are compiled in every build of this crate — a
/// `#[cfg(feature = "acl")]` here would compile the branch out in the graphs nobody remembers
/// to forward the feature through, which is exactly how the identity plane went missing from
/// the shipped binary while `caps.acl` still defaulted to `true`. A config's `acl = false`
/// therefore reaches [`DataPlane::new_with_auth`] at runtime, and a build always has both
/// constructors to choose from.
fn plane_for<S: Storage>(storage: S, plane: PlaneConfig, resolution: &Resolution) -> DataPlane {
    if plane.caps.acl {
        // The identity is the single `Identity::from_config` assembly (`tinio-server`'s
        // `identity::IdentityFromConfig`), which is also what the root credential pair inside
        // it derives from — so the acl plane and the auth-only one cannot disagree about who
        // the root user is.
        return DataPlane::new_with_acl(storage, plane, Identity::from_config(&resolution.config));
    }
    let (access_key, secret_key) = credentials_of(resolution);
    DataPlane::new_with_auth(storage, plane, &access_key, &secret_key)
}

/// The data plane's credential pair: the environment over `.env` over `[auth]`, with the
/// contracted fallback when the file carries no section.
///
/// The one implementation is `tinio_server::identity::root_auth_pair` — [`credentials_of`] is its
/// call site here and the `acl` identity assembly reads the same pair — so the pair a run serves
/// and the pair a config edit changes are one rule. `resolve::server` has already folded
/// `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` (and `.env` with them) onto `config.auth`, which is
/// why the lookup is a read of the resolved config and not a second environment read.
fn credentials_of(resolution: &Resolution) -> (String, String) {
    identity::root_auth_pair(&resolution.config)
}

/// Where the served pair came from, for the stderr summary line.
///
/// The environment (or `.env`, which lands in the environment) answers when it supplied the
/// pair — and the arguments are what says so: `resolve::server` has already folded the pair onto
/// the config, so `env.access_key` being set *is* "the variables won", and there is no second
/// read of the environment to drift from the fold's. They are set together or not at all;
/// half a pair never reaches this line, because the fold refuses it.
///
/// **The absent `[auth]` section is an answer of its own**, and it is the one this line got
/// wrong: the pair a resolution without a section carries is [`identity::root_auth_pair`]'s
/// built-in development pair, which no config file holds — so "from the config file" put the
/// summary at odds with the loopback warning printed just above it. The predicate is
/// [`identity::uses_builtin_fallback`], the same one that warning and the non-loopback refusal
/// read.
fn credential_origin(env: &EnvArgs, config: &_config::Config) -> &'static str {
    if env.access_key.is_some() {
        "from TINIO_ACCESS_KEY/TINIO_SECRET_KEY"
    } else if identity::uses_builtin_fallback(config) {
        "the built-in development pair (no [auth] configured)"
    } else {
        "from the config file"
    }
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;
