//! The backend seam the serving half is generic over.
//!
//! [`ServingBackend`] is what a filesystem backend and a pg backend each supply — the startup
//! repair, the background passes, and the versioning warning. The listener, the two planes,
//! `state` and the drain stay in [`serve`](super::serve::serve): they are the same for every
//! backend.

use std::{fmt::Display, future::ready, time::Duration};

use futures::{StreamExt, future::BoxFuture};
use tokio::sync::watch;

#[cfg(feature = "versioning")]
use crate::_server::Capabilities;
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
    resolve::Resolution,
};
#[cfg(feature = "pg")]
use crate::{
    _pg::{PgStorage, cleanup::PgCleanup},
    _util::pacing::sleep_checked,
};

/// The pg `Cleanup` handle both the startup repair and the background pass run — **one construction
/// so the two judge residue by the same configured clock**: `[s3] noncurrent_keep`/
/// `noncurrent_expire_days` reach `with_retention` and `[s3] block_grace_minutes` reaches
/// `with_grace`. The write capability is the store's own (`PgCleanup` reads `PgStore::access`), so
/// a read-only store removes nothing even if a pass is built over it.
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
/// **A read-only instance builds no pass**, exactly as the fs passes do not: the mode's promise is
/// to write nothing, and this pass's whole purpose is to remove rows. The gate is the run's access
/// mode, read before the first sleep so the task completes at once rather than after a delay (the
/// leg that pins it reads that: a read-only pass under a long delay returns immediately).
///
/// **`delay` and `cycle` are parameters rather than the constants directly** so a test can inject a
/// tick and watch a planted orphan, an expired version and a dead block go within it; the serving
/// path names [`PG_PASS_DELAY`] and [`PG_PASS_CYCLE`].
#[cfg(feature = "pg")]
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
        let _ = drain_actions(
            cleanup.repair(RepairKind::Full).await,
            PG_PASS_WARNINGS,
            |action| {
                tracing::info!(
                    description = %action.description,
                    "the pg background pass reclaimed residue"
                );
            },
        )
        .await;
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
        Box::pin(async move {
            let cleanup = self.repair_cleanup(resolution);
            drain_actions(
                cleanup.repair(RepairKind::Startup).await,
                self.repair_warnings(),
                |_| {},
            )
            .await
        })
    }

    /// The cleanup [`repair`](Self::repair) runs — the backend-specific construction the loop
    /// needs and nothing else.
    fn repair_cleanup(&self, resolution: &Resolution) -> Self::Repair;

    /// The warnings [`repair`](Self::repair) writes: when a stage inside it fails, and when the
    /// repair itself cannot run at all.
    fn repair_warnings(&self) -> RepairWarnings;

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
    fn background(
        &self,
        stop: watch::Receiver<bool>,
        access: AccessMode,
        sweep: sweep::Options,
    ) -> BoxFuture<'static, ()> {
        let _ = (stop, access, sweep);
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

    fn background(
        &self,
        stop: watch::Receiver<bool>,
        access: AccessMode,
        sweep: sweep::Options,
    ) -> BoxFuture<'static, ()> {
        Box::pin(scanner_and_sweeper(self.clone(), stop, access, sweep))
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
    ) -> BoxFuture<'static, ()> {
        Box::pin(pg_pass(
            self.clone(),
            stop,
            access,
            sweep,
            PG_PASS_DELAY,
            PG_PASS_CYCLE,
        ))
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
/// **A read-only run builds neither pass.** The scanner's gate is `enabled: false` there — its
/// purpose is to pre-warm ETags by *writing* rows, and a read-only run reads the rows instead
/// (the read paths recompute what they cannot hit without writing it back, FR-023 amended) — and
/// the sweeper is not built at all: its pass removes `<state-dir>/tmp` and `multipart/` residue
/// and writes the retention/compaction rows, and in this mode the state dir is the **root's own**
/// `.tinio/`, so running it would write the very tree the mode promises to leave alone. The
/// existing `Cleanup` access gate stays what covers the startup repair
/// ([`ServingBackend::repair`]), which is a different pass.
///
/// The scanner's `TINIO_SCANNER=0`/`1` toggle is read inside `Scanner::new`, so the strict
/// `0`/`1` parsing lives with the gate it flips rather than here.
async fn scanner_and_sweeper(
    storage: FsStorage,
    stop: watch::Receiver<bool>,
    access: AccessMode,
    sweep_options: sweep::Options,
) {
    if access.is_read_only() {
        // Read-only mode builds neither pass (see the doc above): the scanner
        // would only repeat what listings recompute on demand, and the
        // sweeper's passes would write the state dir the mode serves from.
        return;
    }
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
    let sweeper = Sweeper::new(storage, sweep_options);
    let (_, _) = tokio::join!(scanner.run(stop.clone()), sweeper.run(stop));
}

/// The background sweep's options from the resolved `[s3]` section (R2): the two TTLs —
/// `temp_ttl_hours` and `multipart_expire_days` (hours and days in the config, a `Duration` at
/// the sweeper) — the three versioning keys — `noncurrent_keep`, `noncurrent_expire_days` and
/// `block_grace_minutes` (minutes in the config, a `Duration` at the sweeper; the key's floor of
/// 1 is the parser's) — and `block_bytes_warn` (bytes; 0 = off), the block store's size warning
/// threshold. Without an `[s3]` section every default applies, which is what an absent section
/// means everywhere else.
pub(super) fn sweep_options_for(resolution: &Resolution) -> sweep::Options {
    match resolution.config.s3.as_ref() {
        Some(s3) => sweep::Options {
            temp_ttl: Duration::from_secs(s3.temp_ttl_hours.saturating_mul(3600)),
            multipart_ttl: Duration::from_secs(s3.multipart_expire_days.saturating_mul(24 * 3600)),
            noncurrent_keep: s3.noncurrent_keep,
            noncurrent_expire_days: s3.noncurrent_expire_days,
            block_grace: Duration::from_secs(s3.block_grace_minutes.saturating_mul(60)),
            block_bytes_warn: s3.block_bytes_warn,
        },
        None => sweep::Options::default(),
    }
}

/// The startup repair's orphan-upload idle grace: the resolved `[s3] multipart_expire_days` in
/// seconds — **the sweep's own `multipart_ttl`** ([`sweep_options_for`]), because the repair and
/// the sweeper judge the same abandoned upload and one configured clock must decide both.
/// Without an `[s3]` section the sweeper's default (7 days) applies, which is exactly what
/// [`FsCleanup::new`] would have used on its own.
pub(super) fn multipart_grace_for(resolution: &Resolution) -> Duration {
    sweep_options_for(resolution).multipart_ttl
}
