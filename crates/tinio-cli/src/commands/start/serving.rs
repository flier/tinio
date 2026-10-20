//! The backend seam the serving half is generic over.
//!
//! [`ServingBackend`] is what a filesystem backend and a pg backend each supply — the startup
//! repair, the background passes, and the versioning warning. The listener, the two planes,
//! `state` and the drain stay in [`serve`](super::serve::serve): they are the same for every
//! backend.

#[cfg(feature = "lifecycle")]
use std::sync::Arc;
use std::{fmt::Display, future::ready, time::Duration};

use futures::{StreamExt, future::BoxFuture};
use tokio::sync::watch;
#[cfg(feature = "api")]
use tokio::task::JoinHandle;
use tracing::instrument;

#[cfg(feature = "api")]
use crate::_core::storage::WriteLockSnapshot;
#[cfg(feature = "lifecycle")]
use crate::_lifecycle::{Options as LifecycleOptions, Runner, Seed, SystemClock};
#[cfg(feature = "api")]
use crate::resolve::Pipelines;
use crate::{
    _core::{
        cleanup::{ActionStream, Cleanup, CleanupOptions, RepairAction, RepairKind},
        mode::AccessMode,
        storage::Storage,
    },
    _fs::{
        FsCleanup, FsStorage, Scanner, ScannerOptions,
        sweep::{self, Sweeper},
    },
    _server::Capabilities,
    config,
    resolve::Resolution,
};
#[cfg(feature = "pg")]
use crate::{
    _pg::{PgStorage, cleanup::PgCleanup},
    _util::pacing::sleep_checked,
};

/// What a run's lifecycle pass is built from (LC13, LC22): the `[lifecycle]` pacing and the seed
/// its first cycle applies.
#[cfg(feature = "lifecycle")]
pub(super) type LifecyclePass = (LifecycleOptions, Vec<Seed>);

/// A build without the `lifecycle` feature: the shared signature carries no work.
#[cfg(not(feature = "lifecycle"))]
pub(super) type LifecyclePass = ();

/// Whether this run builds the lifecycle pass: the feature, the capability and
/// the access mode, read once so the pass and the sweeper cannot disagree.
fn builds_lifecycle_pass(caps: &Capabilities, access: AccessMode) -> bool {
    cfg!(feature = "lifecycle") && caps.lifecycle && !access.is_read_only()
}

/// The lifecycle pass a run builds, or `None` when it builds none — **the one gate** (LC11,
/// LC23). `[s3] lifecycle = false` turns off the pass and its seed together, so a disabled
/// capability does not seed a stored configuration it will never enforce, and a read-only run
/// writes nothing, so it builds no pass over the state dir it promises to leave alone (LC18).
pub(super) fn lifecycle_pass(
    resolution: &Resolution,
    caps: &Capabilities,
    access: AccessMode,
) -> Option<LifecyclePass> {
    #[cfg(feature = "lifecycle")]
    {
        builds_lifecycle_pass(caps, access).then(|| config::lifecycle::options(resolution))
    }
    #[cfg(not(feature = "lifecycle"))]
    {
        let _ = (resolution, caps, access);
        None
    }
}

/// One lifecycle pass as a future: the file's seed once, then the pass's own loop (LC13). The
/// seed precedes the first pass, so a rule the file declares is enforced from the first cycle.
#[cfg(feature = "lifecycle")]
#[instrument(
    level = "info",
    target = "tinio::cli",
    name = "tinio.cli.lifecycle",
    skip_all
)]
async fn lifecycle<S: Storage + Clone>(
    storage: S,
    (options, seeds): LifecyclePass,
    stop: watch::Receiver<bool>,
) {
    let runner = Runner::new(Arc::new(storage), options, Arc::new(SystemClock), stop);
    runner.seed(&seeds).await;
    runner.run().await;
}

#[instrument(
    level = "info",
    target = "tinio::cli",
    name = "tinio.cli.scan",
    skip_all
)]
async fn scan(scanner: Scanner, stop: watch::Receiver<bool>) {
    scanner.run(stop).await;
}

#[instrument(
    level = "info",
    target = "tinio::cli",
    name = "tinio.cli.sweep",
    skip_all
)]
async fn sweep(sweeper: Sweeper, stop: watch::Receiver<bool>) {
    sweeper.run(stop).await;
}

/// The pg `Cleanup` handle both the startup repair and the background pass run — **one construction
/// so the two judge residue by the same configured clock**: `[s3] noncurrent_keep`/
/// `noncurrent_expire_days` reach `with_retention`, `[s3] block_grace_minutes` reaches `with_grace`
/// and a written `[s3] multipart_expire_days` reaches `with_multipart_ttl` (the
/// abandoned-upload stage [`pg_pass`] runs, enabled only when the operator wrote the key; an
/// absent key supplies `None`, which leaves the stage off). The write capability is the store's
/// own (`PgCleanup` reads `PgStore::access`), so a read-only store removes nothing even if a pass
/// is built over it.
#[cfg(feature = "pg")]
fn pg_cleanup(storage: &PgStorage, sweep: sweep::Options, access: AccessMode) -> PgCleanup {
    PgCleanup::new(
        storage.store().clone(),
        CleanupOptions {
            access,
            ..Default::default()
        },
    )
    .with_retention(sweep.noncurrent_keep, sweep.noncurrent_expire_days)
    .with_grace(sweep.block_grace)
    .with_multipart_ttl(sweep.multipart_ttl)
    .with_lifecycle_pass(sweep.lifecycle_pass)
}

/// How long after readiness the pg background pass runs its **first** pass — the filesystem
/// scanner's own `delay`, so a start's residue is reclaimed promptly on either backend.
#[cfg(feature = "pg")]
const PG_PASS_DELAY: Duration = Duration::from_secs(10);

/// How often the pg background pass re-runs after the first one — the filesystem scanner's own
/// `cycle` (24 h), so a pg instance and an fs instance sweep on the same clock. **Justified from
/// the `[s3]` clock it reclaims by**: the pass cannot reclaim a generation younger than
/// `block_grace_minutes` (default 10 min), so any cadence at or above that window loses nothing,
/// and 24 h keeps the whole-table scans off the serving path.
#[cfg(feature = "pg")]
const PG_PASS_CYCLE: Duration = Duration::from_secs(24 * 3600);

/// The chunk the pass sleeps in, so a `stop` is honoured within it — the filesystem scanner's
/// `max_wait`, which is also what [`BACKGROUND_BOUND`](super::serve::BACKGROUND_BOUND) is read
/// against on the shutdown path.
#[cfg(feature = "pg")]
const PG_PASS_CHUNK: Duration = Duration::from_secs(15);

/// The pg background pass: **the whole `Cleanup` pass on a cadence**, matching what the filesystem
/// sweeper runs on the serving path (PG7).
///
/// **`RepairKind::Full`, not `reclaim_meta_orphans` alone.** The fs serving path runs two passes —
/// the scanner's residue reclamation and the **sweeper**'s retention (`[s3] noncurrent_keep` /
/// `noncurrent_expire_days`) and block sweep — and the pg backend has no separate sweeper. So the
/// pass runs `repair(Full)`: the orphan-chunk and upload-gone-part scans (`reclaim_meta_orphans`'s
/// set) **plus** the retention pass and the unreferenced-block sweep. Calling only
/// `reclaim_meta_orphans` here would leave an operator's `noncurrent_keep`/`noncurrent_expire_days`
/// inert on a served pg instance while they take effect on a served fs one, and would make the
/// `with_retention` the pass's cleanup carries dead. Without any pass at all a served pg instance
/// reclaims residue only at startup.
///
/// It mirrors the filesystem arm's shape: it runs on the serving half's one shutdown channel (so it
/// exits on `stop`), sleeps in [`PG_PASS_CHUNK`] steps so a stop is honoured inside that chunk, and
/// is awaited under [`BACKGROUND_BOUND`](super::serve::BACKGROUND_BOUND) on the shutdown path.
///
/// **The abandoned-upload stage is on this loop too** ([`PgCleanup::abort_stale_uploads`]): when
/// the operator wrote `[s3] multipart_expire_days`, an upload idle past it is aborted here,
/// exactly as the fs sweeper aborts one on its own cadence, so that knob is not inert on a served
/// pg instance and a bucket whose stored lifecycle rules carry an abort action is left to them
/// (LC21). A default configuration keeps the behavior it had before this stage existed. The stage
/// runs beside
/// [`Cleanup::repair`] rather than inside it on purpose: a repair reclaims residue, while this is
/// the serving sweep's, so `doctor --fix` — the other `RepairKind::Full` caller — does not abort an
/// operator's uploads. Its failure is a warning and the next pass retries, like a repair stage's.
///
/// **A read-only instance builds no pass**, exactly as the fs passes do not: the mode's promise is
/// to write nothing, and this pass's whole purpose is to remove rows. The gate is the run's access
/// mode, read before the first sleep so the task completes at once rather than after a delay (the
/// leg that pins it reads that: a read-only pass under a long delay returns immediately).
///
/// **`delay` and `cycle` are parameters rather than the constants directly** so a test can inject a
/// tick and watch a planted orphan, an expired version, a dead block and an abandoned upload go
/// within it; the serving path names [`PG_PASS_DELAY`] and [`PG_PASS_CYCLE`].
#[cfg(feature = "pg")]
#[tracing::instrument(level = "info", target = "tinio::cli", skip_all)]
pub(super) async fn pg_pass(
    storage: PgStorage,
    stop: watch::Receiver<bool>,
    access: AccessMode,
    sweep: sweep::Options,
    delay: Duration,
    cycle: Duration,
) {
    if access.is_read_only() {
        return;
    }
    let cleanup = pg_cleanup(&storage, sweep, access);
    let mut wait = delay;
    loop {
        sleep_checked(wait, PG_PASS_CHUNK, &stop).await;
        if *stop.borrow() {
            return;
        }
        match cleanup.pass().await {
            Ok((actions, aborted)) => {
                drain_actions(Ok(actions), PG_PASS_WARNINGS, |action| {
                    tracing::info!(
                        description = %action.description,
                        "the pg background pass reclaimed residue"
                    );
                })
                .await;
                if aborted > 0 {
                    tracing::info!(
                        aborted,
                        "the pg background pass aborted abandoned multipart uploads"
                    );
                }
            }
            Err(err) => tracing::warn!(
                error = %err,
                "the pg background pass could not run; the next pass retries"
            ),
        }
        wait = cycle;
    }
}

/// The filesystem post-repair block verification (the `versioning` feature): re-read and re-hash
/// every versioned bucket's stored blocks after the repair cleared residue, report-only.
#[cfg(feature = "versioning")]
async fn verify_fs_blocks(storage: &FsStorage, access: AccessMode) {
    let cleanup = FsCleanup::new(
        storage,
        CleanupOptions {
            access,
            ..Default::default()
        },
    );
    match cleanup.verify_all_blocks().await {
        Ok((checked, corrupt)) => tracing::info!(
            blocks_checked = checked,
            blocks_corrupt = corrupt,
            "post-repair block verification finished"
        ),
        Err(err) => {
            tracing::warn!(error = %err, "post-repair block verification failed")
        }
    }
}

/// The two warnings a backend's **startup repair** writes — one per failure shape, since the
/// wording names what covers the residue and that answer is the backend's: the filesystem scanner
/// reclaims a root's residue, while a pg instance's next background pass ([`pg_pass`]) resumes a
/// row's. They are data rather than two trait methods so [`ServingBackend::repair`] threads one
/// value to both of its warning arms.
pub(super) struct RepairWarnings {
    /// A stage of a repair that ran failed; the rest of the repair still ran.
    stage: &'static str,
    /// The repair itself failed and no stage ran.
    repair: &'static str,
}

/// Drains one `Cleanup` repair stream — **the shared body of the startup repair
/// ([`ServingBackend::repair`]) and the pg background pass ([`pg_pass`])**, which
/// differ only in their warnings and in what they do with each `Ok` action.
///
/// Every `Ok` action is handed to `on_ok` and marks the stream as having acted;
/// every `Err` is warned with the caller's [`RepairWarnings`] — a stage inside
/// the stream with `stage`, the repair itself with `repair`. A repair that never
/// ran, or one that cleared nothing, answers `false`: the signal is "residue was
/// found and removed", never "the repair ran".
async fn drain_actions<E: Display>(
    repair: Result<ActionStream<E>, E>,
    warnings: RepairWarnings,
    mut on_ok: impl FnMut(&RepairAction),
) -> bool {
    let mut acted = false;
    match repair {
        Ok(mut actions) => {
            while let Some(action) = actions.next().await {
                match action {
                    Ok(action) => {
                        acted = true;
                        on_ok(&action);
                    }
                    Err(err) => tracing::warn!(error = %err, "{}", warnings.stage),
                }
            }
        }
        Err(err) => tracing::warn!(error = %err, "{}", warnings.repair),
    }
    acted
}

/// The filesystem startup repair's warnings: a root's residue is the scanner's to cover, so a
/// failed stage is a warning and never a refusal.
const FS_REPAIR_WARNINGS: RepairWarnings = RepairWarnings {
    stage: "startup repair step failed; the scanner covers the residue",
    repair: "startup repair failed; the scanner covers the residue",
};

/// The pg startup repair's warnings: a pg instance's residue is resumed by the next background
/// pass ([`pg_pass`]), so a failed stage is a warning and never a refusal.
#[cfg(feature = "pg")]
const PG_REPAIR_WARNINGS: RepairWarnings = RepairWarnings {
    stage: "the pg startup repair reported a failed step; the next pass resumes it",
    repair: "the pg startup repair failed; the next pass resumes it",
};

/// The pg background pass's warnings ([`pg_pass`]): the same two failure shapes,
/// worded for a recurring pass rather than a startup repair.
#[cfg(feature = "pg")]
const PG_PASS_WARNINGS: RepairWarnings = RepairWarnings {
    stage: "the pg background pass reported a failed step; the next pass resumes it",
    repair: "the pg background pass failed; the next pass resumes it",
};

/// The backend-specific operations §1's serving half performs — **the trait the one generic
/// serving body calls**.
///
/// The **filesystem-only** operations default to the "nothing to do" answer and only the
/// filesystem backend overrides them: the scanner and sweeper, the post-repair block verify and
/// the versioning warning all describe a **filesystem root** (its residue, its blocks, its
/// version-state rows). A pg backend has none of that as filesystem state — its facts are rows
/// the store owns, its residue is reclaimed by [`pg_cleanup`], and its read path verifies
/// itself — so it takes those defaults.
///
/// The **startup repair**, which every backend owes (design decision 12), is split in two: the
/// trait carries the repair's *shape* — the loop over the action stream, best-effort, ending in
/// the `repaired` signal ([`repair`](Self::repair), a default) — and each backend supplies only
/// what genuinely differs, its cleanup ([`repair_cleanup`](Self::repair_cleanup)) and its
/// warnings ([`repair_warnings`](Self::repair_warnings)).
///
/// **The methods answer `BoxFuture` rather than being `async fn`** so the trait needs no
/// `async-trait` dependency: the serving body is generic over `S: ServingBackend`, so the futures
/// are awaited at the trait's own call sites, and the box is the one place the backend-specific
/// future's concrete type would otherwise leak into the generic body.
pub(super) trait ServingBackend: Storage + Clone + Send + Sync + 'static {
    /// The [`Cleanup`] this backend's startup repair runs — the construction that carries the
    /// backend's own clocks ([`fs_cleanup`]'s multipart grace, [`pg_cleanup`]'s retention and
    /// grace), so a repair judges residue by the same configured clock the backend's passes do.
    type Repair: Cleanup;

    /// The startup repair (design decision 12's post-bind step): **best-effort by contract**
    /// (`failure-handling.md` §3) — every stage is warned rather than propagated, because the
    /// backend's own recovery covers whatever residue a failed stage leaves behind, and a server
    /// that refused to start over a full `tmp/` would be a worse failure than the residue.
    ///
    /// `true` when a stage actually cleared something — the unclean-shutdown signal the optional
    /// block verify arms on. The repair runs with `dry_run` off, so an `Ok` action is executed
    /// repair: residue a crash left (2026-10-06 review). A stage that cleared nothing, or a
    /// repair that never ran, answers `false`: the signal is "residue was found and removed",
    /// never "the repair ran".
    ///
    /// **The loop lives here rather than in each backend** because [`fs_cleanup`] and
    /// [`pg_cleanup`] differ only in the cleanup they build and the words they warn with
    /// ([`repair_warnings`](Self::repair_warnings)); the action stream's shape is the [`Cleanup`]
    /// contract's. The kind is the serving path's `Startup` on every backend — the `Full` kind
    /// belongs to `doctor --fix` and to the pg background pass ([`pg_pass`]), not to a start.
    fn repair<'a>(&'a self, resolution: &'a Resolution) -> BoxFuture<'a, bool> {
        Box::pin(startup_repair(self, resolution))
    }

    /// The cleanup [`repair`](Self::repair) runs — the backend-specific construction the loop
    /// needs and nothing else.
    fn repair_cleanup(&self, resolution: &Resolution) -> Self::Repair;

    /// The warnings [`repair`](Self::repair) writes: when a stage inside it fails, and when the
    /// repair itself cannot run at all.
    fn repair_warnings(&self) -> RepairWarnings;

    /// The write-lock snapshot this backend publishes to `/metrics`, when it has a filesystem
    /// write lane. Default: none — a backend whose writes are the store's rows (pg) hands out
    /// no wait-duration distribution, and the reader publishes zeros rather than a fiction.
    #[cfg(feature = "api")]
    fn write_lock_stats(&self) -> Option<WriteLockSnapshot> {
        None
    }

    /// The metrics reader over this backend (api builds only): one task refreshing the pipeline
    /// gauges, the write-lock histograms and the storage scan on one `ttl`. The serving half
    /// starts it before the planes serve and bounds its exit on the shutdown path.
    #[cfg(feature = "api")]
    fn metrics_reader(&self, pipelines: &Pipelines, stop: watch::Receiver<bool>) -> JoinHandle<()> {
        let this = self.clone();
        super::reader::spawn(pipelines, self.clone(), move || this.write_lock_stats(), stop)
    }

    /// The post-repair block verification (the `versioning` feature). Default: nothing.
    #[cfg(feature = "versioning")]
    fn verify_blocks<'a>(&'a self, access: AccessMode) -> BoxFuture<'a, ()> {
        let _ = access;
        Box::pin(ready(()))
    }

    /// The pre-serve versioning warning. Default: no warning — a backend whose version data is not
    /// a filesystem root's has its own fail-open story (the store's), not the fs warning's.
    #[cfg(feature = "versioning")]
    fn versioning_warning<'a>(&'a self, caps: &'a Capabilities) -> BoxFuture<'a, Option<String>> {
        let _ = caps;
        Box::pin(ready(None))
    }

    /// The background passes on the serving half's shutdown channel. Default: none (returns at
    /// once) — a backend whose residue is not a filesystem directory serves no scanner or sweeper.
    ///
    /// **The lifecycle pass is part of this future, never a task of its own** (LC11): each arm
    /// joins it with its own work, so a stop ends it on the same channel and the shutdown path's
    /// bound covers it. The parameter is the run's pass ([`lifecycle_pass`]) or `None` when the
    /// run builds none (LC23, and every read-only run); a build without the `lifecycle` feature
    /// has neither the parameter nor the pass (LC15).
    fn background(
        &self,
        stop: watch::Receiver<bool>,
        access: AccessMode,
        sweep: sweep::Options,
        scanner: ScannerOptions,
        lifecycle: Option<LifecyclePass>,
    ) -> BoxFuture<'static, ()> {
        let _ = (stop, access, sweep, scanner, lifecycle);
        Box::pin(ready(()))
    }
}

impl ServingBackend for FsStorage {
    type Repair = FsCleanup;

    fn repair_cleanup(&self, resolution: &Resolution) -> FsCleanup {
        fs_cleanup(self, resolution)
    }

    fn repair_warnings(&self) -> RepairWarnings {
        FS_REPAIR_WARNINGS
    }

    #[cfg(feature = "versioning")]
    fn verify_blocks<'a>(&'a self, access: AccessMode) -> BoxFuture<'a, ()> {
        Box::pin(verify_fs_blocks(self, access))
    }

    #[cfg(feature = "versioning")]
    fn versioning_warning<'a>(&'a self, caps: &'a Capabilities) -> BoxFuture<'a, Option<String>> {
        Box::pin(super::serve::versioning_disabled_warning(self, caps))
    }

    #[cfg(feature = "api")]
    fn write_lock_stats(&self) -> Option<WriteLockSnapshot> {
        Some(FsStorage::write_lock_stats(self))
    }

    fn background(
        &self,
        stop: watch::Receiver<bool>,
        access: AccessMode,
        sweep: sweep::Options,
        scanner: ScannerOptions,
        lifecycle: Option<LifecyclePass>,
    ) -> BoxFuture<'static, ()> {
        Box::pin(scanner_and_sweeper(
            self.clone(),
            stop,
            access,
            sweep,
            scanner,
            lifecycle,
        ))
    }
}

#[cfg(feature = "pg")]
impl ServingBackend for PgStorage {
    type Repair = PgCleanup;

    /// The pg cleanup carries the same configured clocks the background pass runs
    /// ([`pg_cleanup`]): the resolved `[s3] noncurrent_keep`/`noncurrent_expire_days` reach
    /// `with_retention` and `[s3] block_grace_minutes` reaches `with_grace`, so a repair that
    /// runs at startup judges an in-flight publish and a noncurrent version by the configured
    /// clock rather than a hardcoded one. The write capability is the store's own (a read-only
    /// instance removes nothing), so this needs no access argument of its own beyond what the
    /// store already carries.
    fn repair_cleanup(&self, resolution: &Resolution) -> PgCleanup {
        pg_cleanup(
            self,
            sweep_options_for(resolution),
            AccessMode::from(resolution.read_only),
        )
    }

    fn repair_warnings(&self) -> RepairWarnings {
        PG_REPAIR_WARNINGS
    }

    /// The pg residue, retention and block reclamation on its cadence ([`pg_pass`]) — **the pg
    /// backend's whole background pass, standing in for the filesystem scanner *and* sweeper**. It
    /// takes the default `verify_blocks` and `versioning_warning` (nothing to do: pg's blocks are
    /// rows the store reads, and its version state is the store's, not a filesystem root's).
    fn background(
        &self,
        stop: watch::Receiver<bool>,
        access: AccessMode,
        sweep: sweep::Options,
        _scanner: ScannerOptions,
        lifecycle: Option<LifecyclePass>,
    ) -> BoxFuture<'static, ()> {
        let storage = self.clone();
        Box::pin(async move {
            let residue = pg_pass(
                storage.clone(),
                stop.clone(),
                access,
                sweep,
                PG_PASS_DELAY,
                PG_PASS_CYCLE,
            );
            #[cfg(feature = "lifecycle")]
            if let Some(pass) = lifecycle {
                let pass = self::lifecycle(storage, pass, stop);
                let (_, _) = tokio::join!(residue, pass);
                return;
            }
            #[cfg(not(feature = "lifecycle"))]
            let _ = lifecycle;
            residue.await
        })
    }
}

/// The `FsCleanup` the filesystem startup repair runs ([`ServingBackend::repair_cleanup`]): the
/// access mode plus the pipeline's orphan-upload grace — the resolved `[s3] multipart_expire_days`
/// ([`multipart_grace_for`]), so the repair judges an abandoned upload by the same configured
/// clock the sweeper does instead of the hardcoded sweep default.
///
/// **A function of its own so the wiring is testable rather than only readable**: a test over
/// [`multipart_grace_for`] pins the value but not whether the trait's `repair_cleanup` chains it
/// onto the cleanup it builds, and this is the one construction both that call and the test drive.
pub(super) fn fs_cleanup(storage: &FsStorage, resolution: &Resolution) -> FsCleanup {
    FsCleanup::new(
        storage,
        CleanupOptions {
            access: AccessMode::from(resolution.read_only),
            ..Default::default()
        },
    )
    .with_multipart_grace(multipart_grace_for(resolution))
}

#[instrument(
    level = "info",
    target = "tinio::cli",
    name = "tinio.cli.repair",
    skip_all
)]
async fn startup_repair<B: ServingBackend>(backend: &B, resolution: &Resolution) -> bool {
    let cleanup = backend.repair_cleanup(resolution);
    drain_actions(
        cleanup.repair(RepairKind::Startup).await,
        backend.repair_warnings(),
        |_| {},
    )
    .await
}

/// The backend's background passes, on the serving half's one shutdown channel — the scanner and
/// the sweeper, and the lifecycle pass when this run builds one (LC11).
///
/// Every loop exits when `stop` flips, and this future completes only when **all** of them have —
/// which is what makes awaiting it the guarantee the shutdown sequence needs: no pass is
/// mid-flight when the pipelines are shut down, so a pass cannot keep enqueueing after them and
/// hit `Err(ShutDown)` on enqueue (a spurious "scanner pass failed" warning at stop).
///
/// **That guarantee belongs to the awaits that run to completion, and the serving half bounds
/// its own** (`BACKGROUND_BOUND`): each loop re-checks the channel only *between* chunks — the
/// scanner's pacing sleeps in `max_wait` steps (`tinio_util::pacing::sleep_checked`) — so a
/// chunk up to 15 s long is what the bound is there to interrupt, and it does so on an ordinary
/// graceful stop. A pass abandoned that way which then enqueues into a shut-down pipeline hits
/// the same `Err(ShutDown)` and its loop logs it ("scanner pass failed"/"sweep pass failed") and
/// drops it. Nothing is lost: the work a pass does is idempotent and resumes on the next start.
///
/// **A read-only run builds neither pass.** The early return below skips both, before any
/// `Scanner` is constructed — and the lifecycle pass with them, because its own gate answers
/// `None` for a read-only run ([`lifecycle_pass`]), so the seed cannot write either. The
/// scanner's purpose is to pre-warm ETags by *writing* rows, and a
/// read-only run reads the rows instead (the read paths recompute what they cannot hit without
/// writing it back, FR-023 amended); the sweeper's pass removes `<state-dir>/tmp` and
/// `multipart/` residue and writes the retention/compaction rows, and in this mode the state dir
/// is the **root's own** `.tinio/`, so running it would write the very tree the mode promises to
/// leave alone. The existing `Cleanup` access gate stays what covers the startup repair
/// ([`ServingBackend::repair`]), which is a different pass.
///
/// The scanner's `TINIO_SCANNER=0`/`1` toggle is read inside `Scanner::new`, so the strict
/// `0`/`1` parsing lives with the gate it flips rather than here.
async fn scanner_and_sweeper(
    storage: FsStorage,
    stop: watch::Receiver<bool>,
    access: AccessMode,
    sweep_options: sweep::Options,
    scanner_options: ScannerOptions,
    lifecycle: Option<LifecyclePass>,
) {
    #[cfg(not(feature = "lifecycle"))]
    let _ = lifecycle;
    if access.is_read_only() {
        // Read-only mode builds neither pass (see the doc above): the scanner
        // would only repeat what listings recompute on demand, and the
        // sweeper's passes would write the state dir the mode serves from.
        return;
    }
    let scan = scan(Scanner::new(storage.clone(), scanner_options), stop.clone());
    let sweep = sweep(Sweeper::new(storage.clone(), sweep_options), stop.clone());
    #[cfg(feature = "lifecycle")]
    if let Some(pass) = lifecycle {
        let (_, _, _) = tokio::join!(scan, sweep, self::lifecycle(storage, pass, stop));
        return;
    }
    let (_, _) = tokio::join!(scan, sweep);
}

/// The scanner options a resolved `[scanner]` section describes.
///
/// **Presence is the gate** (FR-024): the section present switches the scanner on, absent leaves
/// it off, and the three pacing keys carry their documented defaults when the section omits them.
/// The `TINIO_SCANNER` `0`/`1` override is not read here — it belongs to [`Scanner::new`], which
/// applies it on top of this `enabled`.
///
/// `delay` is seconds as a float in the config; a value [`Duration`] cannot represent (a TOML
/// `nan`/`inf` slips through the parser's lower-bound check) falls back to the default rather than
/// panicking in `Duration::from_secs_f64`.
pub(super) fn scanner_options(
    section: Option<&config::scanner::Config>,
    access: AccessMode,
) -> ScannerOptions {
    let enabled = section.is_some();
    let defaults = config::scanner::Config::default();
    let section = section.unwrap_or(&defaults);
    let default_delay = Duration::try_from_secs_f64(defaults.delay).unwrap_or(Duration::ZERO);
    ScannerOptions {
        enabled,
        delay: Duration::try_from_secs_f64(section.delay).unwrap_or(default_delay),
        max_wait: section.max_wait,
        cycle: section.cycle,
        access,
    }
}

/// [`scanner_options`] for the resolved configuration — the one reading of
/// `resolution.config.scanner` on the serving path.
pub(super) fn scanner_options_for(resolution: &Resolution, access: AccessMode) -> ScannerOptions {
    scanner_options(resolution.config.scanner.as_ref(), access)
}

/// The background sweep's options from the resolved `[s3]` section (R2): the two TTLs —
/// `temp_ttl_hours` and `multipart_expire_days` (hours and days in the config, a `Duration` at
/// the sweeper) — the three versioning keys — `noncurrent_keep`, `noncurrent_expire_days` and
/// `block_grace_minutes` (minutes in the config, a `Duration` at the sweeper; the key's floor of
/// 1 is the parser's) — and `block_bytes_warn` (bytes; 0 = off), the block store's size warning
/// threshold. Without an `[s3]` section every default applies, which is what an absent section
/// means everywhere else.
///
/// **`lifecycle_pass` is the same answer [`lifecycle_pass`] gates the pass on** — `[s3] lifecycle`
/// as this build honours it — so the sweeper's owner-per-action decision cannot disagree with the
/// pass it stands down for (LC21, LC23). A build without the `lifecycle` feature builds no pass
/// whatever the key says, so no bucket is governed there either.
pub(super) fn sweep_options_for(resolution: &Resolution) -> sweep::Options {
    let pass = builds_lifecycle_pass(
        &super::serve::capabilities_for(resolution),
        AccessMode::from(resolution.read_only),
    );
    match resolution.config.s3.as_ref() {
        Some(s3) => sweep::Options {
            temp_ttl: Duration::from_secs(s3.temp_ttl_hours.saturating_mul(3600)),
            multipart_ttl: s3
                .multipart_expire_days
                .map(|days| Duration::from_secs(days.saturating_mul(24 * 3600))),
            noncurrent_keep: s3.noncurrent_keep,
            noncurrent_expire_days: s3.noncurrent_expire_days,
            block_grace: Duration::from_secs(s3.block_grace_minutes.saturating_mul(60)),
            block_bytes_warn: s3.block_bytes_warn,
            lifecycle_pass: pass,
        },
        None => sweep::Options {
            lifecycle_pass: pass,
            ..sweep::Options::default()
        },
    }
}

/// The startup repair's orphan-upload idle grace: the resolved `[s3] multipart_expire_days` in
/// seconds — **the sweep's own `multipart_ttl`** ([`sweep_options_for`]), because the repair and
/// the sweeper judge the same abandoned upload and one configured clock must decide both.
/// Without an `[s3]` section the sweeper's default (7 days) applies, which is exactly what
/// [`FsCleanup::new`] would have used on its own.
pub(super) fn multipart_grace_for(resolution: &Resolution) -> Duration {
    sweep_options_for(resolution).multipart_ttl()
}
