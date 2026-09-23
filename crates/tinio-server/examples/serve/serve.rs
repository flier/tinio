//! Design §10 step 8: the storage, the lanes and the serving body.
//!
//! This is the half of the example that runs after the confinement, whichever
//! platform confined it: `main` reaches it on the in-place platforms and the
//! confined child reaches it on the launcher platforms, and both build the
//! identical storage because both call [`build_storage`] — the one construction
//! there is.
//!
//! [`serve`] itself is the body `main` used to carry inline: the startup
//! repair, the listener, the readiness frame, the scanner, the sweeper, the
//! metrics hook and the shutdown sequence, in that order.

use std::{
    error::Error,
    net::TcpListener as StdTcpListener,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use sweep::{Options, Sweeper};
use tinio_config::{Config, pipeline};
use tinio_core::{
    cleanup::{Cleanup, CleanupOptions, RepairKind},
    mode::AccessMode,
    pipeline::Runner,
    storage::{
        DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
    },
};
use tinio_fs::{
    Error as FsError, FsCleanup, FsOptions, FsStorage, Scanner, ScannerOptions,
    database::WriteLockSnapshot, etag, sweep,
};
use tinio_sandbox::Event;
use tinio_server::{
    Capabilities, DataPlane, PlaneConfig, identity,
    metrics::{self, WriteLockStats},
    pipeline::Pipelines,
};
#[cfg(feature = "acl")]
use tinio_server::{Identity, identity::IdentityFromConfig};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    args::Args,
    events::{emit, emit_on_error},
};

/// The three lanes `serve` drives, named once for the signature that carries
/// them.
///
/// The parameters are not decoration and are not this example's choice: the IO lane
/// is typed to `tinio-fs`'s ETag result, the removal and DB lanes to a unit
/// result, and `Pipelines::build` infers exactly this triple from the
/// `FsOptions` the storage is constructed with. Naming it here is what keeps
/// that triple out of a function signature twice over.
pub type Lanes = Pipelines<etag::Result, Result<(), FsError>, Result<(), FsError>>;

/// Build the lanes and the storage in one place, because two processes build
/// them and each one's copy is the whole set of options.
///
/// **`main` and `confined` build the identical storage** — Windows' confined
/// child is this same example re-invoked, and it reaches the same construction
/// after its confinement (design §10's ordering: the lanes are multi-threaded
/// runtimes, so they cannot exist above `unshare(CLONE_NEWUSER)`). One helper
/// rather than two copies because the two are not merely similar: a field added
/// to `FsOptions` and set in one of them is a child whose storage disagrees with
/// its launcher's about the state directory, the access mode or a batch size,
/// with nothing to read that says so.
///
/// **The caller keeps its own diagnostics**, which is the only thing the two
/// sites ever differed in: `main` prints the read-only state directory as it
/// resolves it, and the child derives the same answer from the payload it was
/// handed (design §9).
pub fn build_storage(
    root: &Path,
    config: &Option<Config>,
    state_dir: Option<PathBuf>,
    access: AccessMode,
) -> Result<(FsStorage, Lanes), Box<dyn Error + Send + Sync>> {
    // The lanes `serve` is handed beside the storage: `Pipelines::build` starts
    // the per-lane runtimes, which is why they are built after the confinement
    // rather than before it, and the same instances travel into the storage's
    // options and on to `serve` — a second `Pipelines::build` inside the body
    // could not be the ones the metrics hook reads.
    let pipeline_config = match config {
        Some(config) => config.pipeline.clone().unwrap_or_default(),
        None => pipeline::Config::default(),
    };
    let pipelines = emit_on_error(Pipelines::build(&pipeline_config))?;
    let storage = emit_on_error(FsStorage::new(
        root,
        FsOptions {
            follow_symlinks: false,
            state_dir,
            access,
            compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
            meta_batch_size: DEFAULT_META_BATCH_SIZE,
            meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
            io_pipeline: pipelines.io(),
            remove_pipeline: pipelines.remove(),
            db_pipeline: pipelines.db(),
            owner_uids: identity::owner_uids_for(config.as_ref().unwrap_or(&Config::default())),
        },
    ))?;
    Ok((storage, pipelines))
}

/// The write-lock snapshot of the fs storage into the metric layer's
/// plain-data form — the wiring-point conversion that keeps
/// `tinio-server`'s metrics decoupled from backend snapshot types.
fn write_lock_stats(snapshot: WriteLockSnapshot) -> WriteLockStats {
    WriteLockStats {
        wait_buckets: snapshot.wait_buckets,
        total_buckets: snapshot.total_buckets,
        count: snapshot.count,
        wait_sum_us: snapshot.wait_sum_us,
        wait_max_us: snapshot.wait_max_us,
        total_sum_us: snapshot.total_sum_us,
        total_max_us: snapshot.total_max_us,
    }
}

/// `[s3]` capability toggles and page-size caps from the parsed config
/// (FR-021); without an `[s3]` section the defaults apply. (The bool
/// toggles used to be dropped here — the plane was built with
/// `Capabilities::default()` even when a config set them.)
fn capabilities_for(config: &Option<Config>) -> Capabilities {
    match config.as_ref().and_then(|c| c.s3.as_ref()) {
        Some(s3) => Capabilities::from(s3),
        None => Capabilities::default(),
    }
}

/// The plane constructor selection (same-condition rule, Tasks 10-12):
/// feature `acl` compiled on AND `caps.acl` on → the identity-backed
/// plane (`new_with_acl` — ConfigAuth + AclAccess, every enforced
/// behavior wired together); else today's `new_with_auth` (no
/// identity, no enforced behavior). The identity is the single
/// `Identity::from_config` assembly (spec §6); the credential fallback
/// for the auth-only plane is the same `[auth]` pair that assembly uses.
fn plane_for(storage: FsStorage, plane: PlaneConfig, config: &Option<Config>) -> DataPlane {
    // An absent `--config` serves the default shape (the minioadmin pair
    // and the core owner element).
    #[cfg(feature = "acl")]
    if plane.caps.acl {
        return DataPlane::new_with_acl(
            storage,
            plane,
            Identity::from_config(config.as_ref().unwrap_or(&Config::default())),
        );
    }
    let (access_key, secret) =
        identity::root_auth_pair(config.as_ref().unwrap_or(&Config::default()));
    DataPlane::new_with_auth(storage, plane, &access_key, &secret)
}

/// **One signature across the three plans, and this plan owns it.**
///
/// `storage` is **not** optional, and an earlier version of this signature made
/// it so and had to be corrected: every caller that reaches `serve` serves from
/// a storage, so `None` had no meaning any platform could agree on — this plan
/// read it as a caller bug and the Windows plan read it as "the body builds
/// one", which is not the same function. Linux's `main` builds the storage
/// before the runtime exists (its state directory is one of the jail's
/// mounts), and a launcher's child builds it in `confined`; the launcher itself
/// never reaches `serve` at all.
///
/// **Two parameters carry what `main` has already resolved** rather than
/// letting the body resolve it a second time: `access` is FR-023's chain
/// (`[server] read_only` → `TINIO_READ_ONLY` → `--read-only`) folded into one
/// value, and `pipelines` are the lanes the storage was constructed with — the
/// same instances the metrics hook reads and the shutdown sequence drains, which
/// a second `Pipelines::build` here could not be.
///
/// The two optional ones are optional because the three platforms reach `serve`
/// differently, and each `None` is a fact about a platform rather than a policy
/// of `serve`: Linux serves in the process that confined itself, so it passes
/// `None` for both — there is no adopted listener, because this process binds
/// its own, and no stop channel, because nothing forwards a signal to something
/// that is not a child. A launcher's child passes `Some(listener)` and
/// `Some(stop)` instead.
///
/// **`inherited` is `std`'s listener rather than tokio's, and the conversion
/// below is the reason.** [`TcpListener::from_std`] panics outside a runtime, and
/// a launcher's child rebuilds its socket from the inherited descriptor before
/// any runtime exists — so a `tokio::net::TcpListener` parameter could only be
/// filled by converting at the call site, in the one place the call cannot be
/// made. The parameter carries what the child actually has, and the conversion
/// happens here instead: `serve` runs inside the runtime by construction, which
/// is the first point at which the two types can meet. `StdTcpListener` is
/// [`std::net::TcpListener`] under an alias because this example's `TcpListener`
/// is the `tokio` one everywhere else.
pub async fn serve(
    args: Args,
    config: Option<Config>,
    mut storage: FsStorage,
    access: AccessMode,
    pipelines: Lanes,
    inherited: Option<StdTcpListener>,
    stop: Option<watch::Receiver<bool>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // `[s3] max_concurrent_uploads` caps in-progress multipart uploads
    // (default 1000) — an authenticated client cannot accumulate an
    // unbounded number of uploads.
    if let Some(s3) = config.as_ref().and_then(|c| c.s3.as_ref()) {
        storage.set_max_concurrent_uploads(s3.max_concurrent_uploads);
    }
    // D-B: synchronous Startup repair — the fast, deterministic items
    // (tmp, staging residue, multipart orphans, stale bucket records)
    // run before readiness; the delete-tombstone stage routes through
    // the storage's removal lane (the `remove_pipeline` wired above).
    // Best-effort: a failed stage is warned and readiness proceeds — the
    // scanner covers the residue. The stale-bucket prune runs here,
    // pre-serving, so it stays lock-free (no request can race it yet).
    let cleanup = FsCleanup::new(
        &storage,
        CleanupOptions {
            access,
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

    // The launcher platforms' children take the listener their parent had
    // already bound; the platforms that confine in place bind their own, because
    // there is no parent to have bound one. The `Some` arm is where the child's
    // `std` socket becomes the async one this body serves on — see the parameter's
    // doc for why it cannot happen any earlier.
    let listener = match inherited {
        Some(inherited) => TcpListener::from_std(inherited)?,
        None => TcpListener::bind(args.address).await?,
    };
    // stdout is a protocol channel; every log goes to stderr. `emit` above is
    // the only writer, which is what makes the contract cheap to hold (design
    // decision 19) — and this is the serving process's own frame, beside the
    // `Error` frames a refusal sent through the same call before it got here.
    // `local_addr()` is already a `SocketAddr`, so the port travels as a value
    // rather than as something re-parsed from text.
    emit(&Event::Listening {
        addr: listener.local_addr()?,
    });

    // Background scanner (FR-024; `TINIO_SCANNER=0` disables).
    let scanner_options = ScannerOptions {
        enabled: true,
        delay: Duration::from_secs(10),
        max_wait: Duration::from_secs(15),
        cycle: Duration::from_secs(24 * 3600),
        access,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let scanner = Scanner::new(storage.clone(), scanner_options);
    let scanner_task = tokio::spawn(async move {
        scanner.run(shutdown_rx).await;
    });

    // Async sweep (temp 24 h, multipart 7 d).
    let (sweep_tx, sweep_rx) = watch::channel(false);
    let sweeper = Sweeper::new(storage.clone(), Options::default());
    let sweeper_task = tokio::spawn(async move {
        sweeper.run(sweep_rx).await;
    });

    let caps = capabilities_for(&config);

    // The `/metrics` scrape endpoint (F10): the plane's hook refreshes
    // the pipeline gauges and the write-lock histograms on every scrape.
    let metrics_storage = storage.clone();
    let metrics_io = pipelines.io();
    let metrics_db = pipelines.db();
    let plane = plane_for(storage, PlaneConfig { caps, access }, &config).with_metrics(Arc::new(
        move || {
            metrics::refresh(
                metrics_io.stats(),
                metrics_db.stats(),
                write_lock_stats(metrics_storage.write_lock_stats()),
            );
        },
    ));
    plane
        // **The data plane's shutdown receiver is the caller's when one was handed
        // in, and a subscription to this process's own sender otherwise** — the
        // one substitution the launcher platforms need from this body (design §3
        // step 9, "forward termination signals while waiting"). A forwarded stop is
        // what the caller's receiver carries: the child's `on_stop` hook sends on
        // the far end of it, so the server stops accepting. The plain run and the
        // in-place platforms have no such caller and keep the subscription they
        // had.
        //
        // **It is the plane's receiver and not the scanner's, and that is the whole
        // point of the line.** `scanner.run(shutdown_rx)` above and this call are
        // two independent subscribers: repointing `shutdown_rx` at the caller's
        // channel — the shape an earlier draft had — would hand a forwarded stop to
        // the **background scanner** and leave the server *accepting*, which is the
        // exact opposite of what the hook exists for. Both ends derive from one
        // sender in the child, so a stop from either source stops everything: the
        // hook's `send(true)` reaches this receiver, and the shutdown sequence below
        // reaches the scanner and the sweeper.
        .serve(listener, stop.unwrap_or_else(|| shutdown_tx.subscribe()))
        .await?;
    // Stop the scanner and the sweeper BEFORE the pipelines: the watch
    // signals take effect at the next pass boundary, so awaiting the
    // handles guarantees no pass is mid-flight when the pipelines shut
    // down — a running pass would otherwise keep enqueueing after
    // shutdown and hit Err(ShutDown) on enqueue (a spurious "scanner
    // pass failed" warn at stop, data-path review 2026-08-29 finding 7).
    let _ = shutdown_tx.send(true);
    let _ = sweep_tx.send(true);
    let _ = scanner_task.await;
    let _ = sweeper_task.await;
    // R5 (pipeline-spec.md §3.5): the server has stopped accepting new
    // requests and the background tasks are done — shut the pipelines
    // down in order, IO first and the DB write pipeline last, so
    // in-flight list batches can drain.
    pipelines.shutdown();
    // Await the workers' exit — the in-flight tasks shutdown already
    // guaranteed have visibly completed (item 6c — observability, not
    // new shutdown semantics).
    pipelines.drain().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_for_maps_s3_max_buckets() {
        // A regression back to `Capabilities::default()` here would make
        // this 10,000 and pass CI silently — this pins the wiring.
        let config = Config::parse("version = 1\n[s3]\nmax_buckets = 3").unwrap();
        assert_eq!(capabilities_for(&Some(config)).max_buckets, 3);
    }

    #[test]
    fn capabilities_for_defaults_without_s3_section() {
        assert_eq!(capabilities_for(&None), Capabilities::default());
    }
}
