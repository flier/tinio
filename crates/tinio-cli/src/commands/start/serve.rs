//! §1's serving half — the steps after the runtime exists.

#[cfg(feature = "api")]
use std::sync::Arc;
use std::{fmt::Display, net::SocketAddr, process::ExitCode, time::Duration};

use futures::{
    StreamExt,
    future::{Either, select},
};
use tokio::{
    net::TcpListener,
    sync::watch,
    task::JoinError,
    time::{error::Elapsed, timeout},
};

use super::Ready;
#[cfg(unix)]
use crate::notify;
use crate::{
    _core::{
        cleanup::{Cleanup, CleanupOptions, RepairKind},
        mode::AccessMode,
    },
    _fs::{
        FsCleanup, FsStorage, Scanner, ScannerOptions,
        sweep::{self, Sweeper},
    },
    _sandbox::platform_facts,
    _server::{
        Capabilities, DataPlane, Identity, PlaneConfig,
        identity::{self, IdentityFromConfig},
    },
    Error,
    commands::EnvArgs,
    events,
    resolve::Resolution,
    sandbox,
};

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
const BACKGROUND_BOUND: Duration = Duration::from_secs(2);

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
pub async fn serve(ready: Ready) -> Result<ExitCode, Error> {
    // One pattern, with the plane's field gated the same way the struct declares it.
    let Ready {
        resolution,
        args,
        adopted,
        handles,
        storage,
        pipelines,
        #[cfg(feature = "api")]
        plane,
        on_ready,
    } = ready;

    // The startup repair, post-bind (decision 12): the fast, deterministic items only, and
    // best-effort — a failed stage is warned and readiness proceeds, because the scanner
    // covers the residue.
    repair(&storage, &resolution).await;

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
        })?;
    }
    let bind = resolution.address()?;
    // **Borrowed, not taken**: the arm below asks whether this process is a launched child, which
    // is what decides who publishes `state`. A `match adopted` binding its inner value would move
    // it here and leave that question unanswerable at the publish site.
    let listener = match adopted.as_ref() {
        // The listener a launcher handed this child over the inherited descriptor (Task 9).
        Some(adopted) => sandbox::adopted_listener(adopted)?,
        None => TcpListener::bind(bind).await?,
    };
    let address = listener.local_addr()?;
    warn_if_non_loopback(address);
    let caps = capabilities_for(&resolution);
    let access = AccessMode::from(resolution.read_only);
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
    let background = tokio::spawn(scanner_and_sweeper(storage.clone(), stop.clone(), access));

    #[cfg(feature = "api")]
    // **A launched child never writes `state`.** Its launcher does, and the two launcher
    // platforms reach that answer by different routes. On Windows the cause is the file's
    // DACL: current-user-only and protected, so this process — inside the container, with the
    // package SID as its identity — is refused every path-based access to it. On macOS nothing
    // refuses the write, and the cause is ownership instead: `state` must name the launcher,
    // whose `--daemon` readiness check compares the file's pid against the pid it spawned and
    // whose `Plane::shutdown` removal guard expects itself, and the launch token is minted by
    // the publisher — the macOS arm's own module doc is that reason's home
    // (`crate::sandbox::macos`). Either way the launcher publishes the file once *this* process
    // reports it is listening. The skip is not a degradation: the file exists by the time any
    // client can act on the readiness event this line is part of. `adopted.is_none()` is every
    // process that owns its own `state`, which is every unconfined run and every in-place
    // platform.
    if adopted.is_none() {
        plane.publish(address.port())?; // `state` is written here
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
        origin = credential_origin(&args.env),
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
    // `contracts/cli.md:43` promises "cease accepting, drain ≤ 10 s, remove `state` and the
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

/// `contracts/cli.md`: non-loopback bind warns on stderr. Anonymous escalation is US3 (T087)
/// and is not wired until that flag exists.
fn warn_if_non_loopback(address: SocketAddr) {
    if address.ip().is_loopback() {
        return;
    }
    eprintln!(
        "tinio: warning: binding a non-loopback address ({address}); the data plane is reachable from other hosts"
    );
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

/// The restart repair: the `Cleanup` contract's startup kind, run before either plane serves.
///
/// Best-effort by contract (`failure-handling.md` §3): every stage is warned rather than
/// propagated, because the scanner covers whatever residue a failed stage leaves behind, and a
/// server that refuses to start over a full `tmp/` would be a worse failure than the residue.
async fn repair(storage: &FsStorage, resolution: &Resolution) {
    let cleanup = FsCleanup::new(
        storage,
        CleanupOptions {
            access: AccessMode::from(resolution.read_only),
            ..Default::default()
        },
    );
    match cleanup.repair(RepairKind::Startup).await {
        Ok(mut actions) => {
            while let Some(action) = actions.next().await {
                if let Err(err) = action {
                    tracing::warn!(
                        error = %err,
                        "startup repair step failed; the scanner covers the residue"
                    );
                }
            }
        }
        Err(err) => tracing::warn!(
            error = %err,
            "startup repair failed; the scanner covers the residue"
        ),
    }
}

/// The two background passes, on the serving half's one shutdown channel.
///
/// Both loops exit when `stop` flips, and this future completes only when **both** have — which
/// is what makes awaiting it the guarantee the shutdown sequence needs: no pass is mid-flight
/// when the pipelines are shut down, so a pass cannot keep enqueueing after them and hit
/// `Err(ShutDown)` on enqueue (a spurious "scanner pass failed" warning at stop).
///
/// **That guarantee belongs to the awaits that run to completion, and the serving half bounds
/// its own** (`BACKGROUND_BOUND`): both loops re-check the channel only *between* chunks — the
/// scanner's pacing sleeps in `max_wait` steps (`tinio_util::pacing::sleep_checked`) — so a
/// chunk up to 15 s long is what the bound is there to interrupt, and it does so on an ordinary
/// graceful stop. A pass abandoned that way which then enqueues into a shut-down pipeline hits
/// the same `Err(ShutDown)` and its loop logs it ("scanner pass failed"/"sweep pass failed") and
/// drops it. Nothing is lost: the work a pass does is idempotent and resumes on the next start.
///
/// The scanner's `TINIO_SCANNER=0`/`1` toggle is read inside `Scanner::new`, so the strict
/// `0`/`1` parsing lives with the gate it flips rather than here.
async fn scanner_and_sweeper(storage: FsStorage, stop: watch::Receiver<bool>, access: AccessMode) {
    let scanner = Scanner::new(
        storage.clone(),
        ScannerOptions {
            // The `[scanner]` section's own gate is Task 9's/its own wiring; what this task
            // lifts is the shape, and `TINIO_SCANNER=0` still switches the scanner off here.
            enabled: true,
            delay: Duration::from_secs(10),
            max_wait: Duration::from_secs(15),
            cycle: Duration::from_secs(24 * 3600),
            access,
        },
    );
    // Async sweep: temp 24 h, multipart 7 d.
    let sweeper = Sweeper::new(storage, sweep::Options::default());
    let (_, _) = tokio::join!(scanner.run(stop.clone()), sweeper.run(stop));
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
fn plane_for(storage: FsStorage, plane: PlaneConfig, resolution: &Resolution) -> DataPlane {
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
fn credential_origin(env: &EnvArgs) -> &'static str {
    if env.access_key.is_some() {
        "from TINIO_ACCESS_KEY/TINIO_SECRET_KEY"
    } else {
        "from the config file"
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        sync::watch,
        time::{sleep, timeout},
    };

    use super::stopping;

    /// **The drain bound is armed by a stop, never by the clock**, and this leg is the defect in
    /// the form a unit test can hold: with the channel still `false`, [`stopping`] must not
    /// resolve. `serve` used to arm `DRAIN_BOUND` at exactly this point, so an idle server walked
    /// the drain path ~12 s after readiness, removed `state` and exited.
    #[tokio::test]
    async fn an_unstopped_server_never_reaches_its_drain() {
        let (_sender, mut stop) = watch::channel(false);
        assert!(
            timeout(Duration::from_millis(50), stopping(&mut stop))
                .await
                .is_err(),
            "nothing asked for a stop, so the drain must not begin"
        );
    }

    /// **The two orders a stop can arrive in are equivalent, and the second is why the current
    /// value is read before `changed()` is awaited**: a receiver that already saw the flip would
    /// otherwise wait for the *next* change, which never comes.
    #[tokio::test]
    async fn a_stop_starts_the_drain_in_either_order() {
        // The flip lands during the wait.
        let (sender, mut stop) = watch::channel(false);
        let flip = tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            let _ = sender.send(true);
        });
        assert!(
            timeout(Duration::from_secs(5), stopping(&mut stop))
                .await
                .is_ok(),
            "a stop that arrived during the wait must start the drain"
        );
        flip.await.expect("the flip task");

        // The flip landed before the wait.
        let (sender, mut stop) = watch::channel(false);
        sender.send(true).expect("flip before the wait");
        assert!(
            timeout(Duration::from_millis(50), stopping(&mut stop))
                .await
                .is_ok(),
            "a stop that arrived before the wait must start the drain"
        );
    }
}
