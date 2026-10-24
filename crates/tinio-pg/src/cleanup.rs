//! The `Cleanup` implementation for the pg backend (PG7).
//!
//! The pg design's cleanup bullet names four scans: **orphan chunks** (content
//! rows no live head names), **rows whose upload is gone** (a multipart part's
//! content left behind by an abort or a crash that drained the `uploads` row
//! and never reached the bytes), **version rows past retention** (the
//! noncurrent history the retention thresholds retire), and **blobs with no
//! live reference** (the block plane, reclaimed through `tinio_blob`'s
//! [`LiveSet`] and sweep). This module holds the **policy** — which scans run,
//! the thresholds, the dry-run shape, the live-set fold and the block sweep —
//! and the row facts come from `tinio-store-pg`'s own cleanup methods, whose
//! statements live beside the tables they read. Repair yields the same
//! [`ActionStream`] shape `tinio-fs` yields — one [`RepairAction`] per item,
//! a report answering "would …" and a real run the applied action — so the
//! doctor's ledger and the JSON report render pg rows exactly as they render
//! fs rows.
//!
//! # The bound
//!
//! Every scan here is a **keyset page stream** — `tinio-store-pg`'s cleanup
//! statements each read `LIMIT cleanup::CLEANUP_BATCH_ROWS` rows from a cursor
//! that is strictly past the last row the previous page returned, in that
//! scan's own key order — so a pass holds **one page** of rows at a time and a
//! removal binds one page's arrays rather than the whole scan's. A pass reads
//! `⌈N / CLEANUP_BATCH_ROWS⌉` statements per scan, where `N` is the candidates
//! the scan's predicate admits, and a page that comes back short is the last
//! one. `tinio-store-pg`'s `cleanup` module states what each statement's page
//! costs the server, measured.
//!
//! Two things are still proportional to the answer rather than to a page, and
//! both are the report's own shape rather than a row array: a dry run names one
//! action per candidate (the same shape `tinio-fs`'s sweep yields), and the
//! block sweep's [`LiveSet`] holds one address per live block — a sweep cannot
//! judge a block without the set — while the version rows it folds in arrive
//! one page at a time.
//!
//! Four rules bind the module:
//!
//! - **A governed bucket keeps its own clock** (LC21): in a run that builds the
//!   lifecycle pass, a bucket whose stored lifecycle configuration carries
//!   `NoncurrentVersionExpiration` is governed by its rule for that action, so this
//!   module's `[s3] noncurrent_keep` / `noncurrent_expire_days` do not apply to it,
//!   and `AbortIncompleteMultipartUpload` likewise displaces `[s3]
//!   multipart_expire_days` for the serving pass's abandoned-upload stage
//!   ([`PgCleanup::abort_stale_uploads`]). A bucket carrying neither rule keeps the
//!   bucket-blind behaviour, and the rule is per action — never per bucket. **With
//!   no pass nothing is governed** (LC23): the clocks then do their normal work on
//!   every bucket, stored rules or not.
//! - **A live chunk is never reclaimed, and neither is a publish in flight.**
//!   A content row is live exactly when its `(bucket, kind, ckey, seq)` is the
//!   head's `live_seq`. A non-live row is protected while its own
//!   `content.published` stamp is younger than [`PgCleanup::with_grace`]'s
//!   window — any generation, superseded or current. The head's current
//!   `next_seq` generation is protected as a whole while `allocated_at` is
//!   inside that window, or while any chunk of that generation still is: a
//!   body that is still landing batches keeps the chunks it wrote earlier.
//!   `stage` commits those chunks while `live_seq` moves only at the flip
//!   (PG18). A generation a later allocation superseded loses the head gate
//!   and is protected only by its own stamps. The scan finds candidates. The
//!   removal deletes a candidate only when the same head gate still calls it
//!   residue: a publish does not change the generation's sequence, so the
//!   scan's key alone is not proof the rows are still dead.
//! - **A listing never shows residue** (PG23) is the store's, not this
//!   module's: `ObjectContent::scan` is row-backed (it reads `content_heads`
//!   with a live generation), so an orphan chunk is invisible to every listing
//!   whether or not this cleanup has run. Cleanup reclaims the bytes; the
//!   visibility rule is already the read path's.
//! - **The store's mode decides the writes.** The write capability is read from
//!   [`PgStore::access`] — the mode the instance was constructed with — never
//!   from the caller's [`CleanupOptions::access`], so a read-only store can
//!   never be asked to remove anything: its stages report what a fix would do
//!   (a diagnosis still diagnoses, which is what a `doctor --dry-run` against a
//!   replica is for) and write nothing.
//!
//! `dry_run` is fixed at construction ([`CleanupOptions`]) because the
//! [`Cleanup`] contract takes none, so the configurable value is a handle
//! ([`PgCleanup`]) rather than `PgStorage` itself — the same shape
//! `tinio-fs`'s `FsCleanup` has.

use std::{
    collections::HashSet,
    mem,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use tinio_blob::{LiveSet, Store};
use tinio_core::{
    blob::BlockHash,
    bucket,
    cleanup::{
        ActionStream, Cleanup, CleanupOptions, RepairAction, RepairActionLevel, RepairKind,
        retention_enabled, retires,
    },
    lifecycle,
    meta::{BucketMeta, VersionMeta},
    object, storage,
};

use crate::{
    _store_pg::{
        PgStore,
        blocks::PgBlocks,
        cleanup::{HistoryRow, OrphanPart},
    },
    _util::metrics::{
        LockOperation, Pass, PassTimer, PgSection, record_pg_pass_failure,
        record_protected_deletion,
    },
    Error,
};

/// The default reclaim grace (the fs sweep's `block_grace_minutes` default):
/// a block, or a chunk, younger than this is left for the next pass. The
/// current staged generation is left as well while any of its chunks is still
/// inside the window, so a large body that is still landing batches is not
/// orphaned after its allocation has aged. A generation whose newest chunk is
/// older than the window is residue.
const DEFAULT_GRACE: Duration = Duration::from_secs(10 * 60);

/// The pg cleanup: the store the scans run against and the run's policy.
///
/// No pool is held: every statement is `tinio-store-pg`'s own (it reads that
/// crate's tables), so the handle cannot diverge from the store it cleans.
///
/// # Examples
///
/// ```ignore
/// use tinio_core::cleanup::{Cleanup, CleanupOptions, RepairKind};
/// use tinio_pg::{Access, PgStorage, cleanup::PgCleanup};
///
/// # async fn example(storage: &PgStorage) -> Result<(), tinio_pg::Error> {
/// // A dry run reports what a fix would reclaim and removes nothing; the same
/// // handle with `dry_run: false` applies it.
/// let cleanup = PgCleanup::new(storage.store().clone(), CleanupOptions::default());
/// let mut actions = cleanup.repair(RepairKind::Full).await?;
/// while let Some(action) = futures::StreamExt::next(&mut actions).await {
///     let action = action?;
///     assert!(!action.description.is_empty());
/// }
/// # Ok(())
/// # }
/// ```
///
/// (`ignore`: every construction needs a live PostgreSQL server, so a runnable
/// example would be a fake — the constructor's own doc says what the handle
/// holds.)
#[derive(Debug, Clone)]
pub struct PgCleanup {
    /// The store the scans and the block plane run over.
    store: PgStore,
    /// Report-only mode: every stage answers "would …" and removes nothing.
    dry_run: bool,
    /// How many noncurrent versions of a key to retain (`[s3] noncurrent_keep`;
    /// the newest N). `0` = keep every version, the safe reading of an unset
    /// key.
    noncurrent_keep: u64,
    /// How many days a version may stay noncurrent before it is expired
    /// (`[s3] noncurrent_expire_days`, measured from `became_noncurrent`).
    /// `0` = never expire by age.
    noncurrent_expire: Duration,
    /// The minimum age before a staged generation or an unreferenced block may
    /// be reclaimed (`[s3] block_grace_minutes`).
    grace: Duration,
    /// The abandoned-upload timeout (`[s3] multipart_expire_days`): how long an
    /// upload may sit untouched before [`Self::abort_stale_uploads`] removes it.
    /// `None` leaves the stage off, so a default configuration keeps the
    /// behavior it had before the lifecycle surface existed (LC21).
    multipart_ttl: Option<Duration>,
    /// Whether this run builds the lifecycle pass (`[s3] lifecycle`, LC23): the
    /// condition under which a bucket's stored rules own the two actions above
    /// (LC21). With no pass nothing is governed, so the clocks do their normal
    /// work on every bucket, stored rules or not.
    lifecycle_pass: bool,
}

impl PgCleanup {
    /// Construct the cleanup over `store`, with `options` naming the run's
    /// report-only mode. The **write capability is the store's**, read at each
    /// stage from [`PgStore::access`], never `options`' own `access` hint — the
    /// two cannot diverge.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use tinio_core::cleanup::CleanupOptions;
    /// use tinio_pg::cleanup::PgCleanup;
    ///
    /// # fn example(store: tinio_store_pg::PgStore) {
    /// let cleanup = PgCleanup::new(store, CleanupOptions::default());
    /// # let _ = cleanup;
    /// # }
    /// ```
    ///
    /// (`ignore`: the store needs a live server; the constructor itself does
    /// nothing but hold it.)
    pub fn new(store: PgStore, options: CleanupOptions) -> Self {
        Self {
            store,
            dry_run: options.dry_run,
            noncurrent_keep: 0,
            noncurrent_expire: Duration::ZERO,
            grace: DEFAULT_GRACE,
            multipart_ttl: None,
            lifecycle_pass: true,
        }
    }

    /// Override the retention thresholds (`[s3] noncurrent_keep` and
    /// `noncurrent_expire_days`; `0` = unlimited, the unset-key reading).
    /// Constructor sugar, in `FsCleanup::with_multipart_grace`'s shape.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # fn example(cleanup: tinio_pg::cleanup::PgCleanup) {
    /// let cleanup = cleanup.with_retention(5, 30);
    /// # let _ = cleanup;
    /// # }
    /// ```
    pub fn with_retention(mut self, keep: u64, expire_days: u64) -> Self {
        self.noncurrent_keep = keep;
        self.noncurrent_expire = Duration::from_secs(expire_days.saturating_mul(86_400));
        self
    }

    /// Override the reclaim grace: the window in which a staged generation and
    /// an unreferenced block are left alone.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # fn example(cleanup: tinio_pg::cleanup::PgCleanup) {
    /// let cleanup = cleanup.with_grace(std::time::Duration::from_secs(1800));
    /// # let _ = cleanup;
    /// # }
    /// ```
    pub fn with_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Override the abandoned-upload timeout (`[s3] multipart_expire_days`) —
    /// the age past which the serving pass aborts an upload. Constructor sugar,
    /// in [`Self::with_grace`]'s shape.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example(cleanup: tinio_pg::cleanup::PgCleanup) {
    /// let cleanup = cleanup.with_multipart_ttl(Some(std::time::Duration::from_secs(86_400)));
    /// # let _ = cleanup;
    /// # }
    /// ```
    ///
    /// The example only constructs the value; a serving run with it would
    /// abort an upload left untouched for a day.
    pub fn with_multipart_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.multipart_ttl = ttl;
        self
    }

    /// Declare whether this run builds the lifecycle pass (`[s3] lifecycle`): with
    /// none, no bucket is governed, so the retained clocks above do their normal
    /// work on every bucket, stored rules or not (LC21, LC23). Constructor sugar,
    /// in [`Self::with_grace`]'s shape; the default is `true`, the capability's
    /// own default.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # fn example(cleanup: tinio_pg::cleanup::PgCleanup) {
    /// let cleanup = cleanup.with_lifecycle_pass(false);
    /// # let _ = cleanup;
    /// # }
    /// ```
    pub fn with_lifecycle_pass(mut self, pass: bool) -> Self {
        self.lifecycle_pass = pass;
        self
    }

    /// Abort the uploads abandoned past [`Self::with_multipart_ttl`]'s timeout
    /// and answer how many went — the pg arm of the filesystem sweeper's
    /// abandoned-upload stage (`[s3] multipart_expire_days`). No
    /// [`Cleanup::repair`] kind runs it: a repair reclaims residue, while this is
    /// the serving loop's, on the run's own shutdown channel (the fs sweeper's
    /// stage, on the pg loop).
    ///
    /// **The age is the upload's latest activity**, `max(initiated_at, latest
    /// part write)`, not the initiation alone: an upload whose last part landed
    /// inside the timeout is being written, and the stage spares it however long
    /// ago it was initiated. Those are the filesystem sweeper's own terms, so a
    /// client uploading against either backend keeps its upload. The store's
    /// page carries the rule (its `abort_stale_uploads` states the expression).
    ///
    /// **A governed bucket is skipped** (LC21): in a run that builds the lifecycle
    /// pass, a bucket whose stored lifecycle configuration carries
    /// `AbortIncompleteMultipartUpload` is governed by its rule for that action, so
    /// the server's timeout does not apply to it. The question is
    /// [`lifecycle::ActionOwners`], the same value the filesystem sweeper asks, so
    /// the two backends cannot disagree about which bucket is governed. A rules
    /// read that fails while the pass is built answers its error and aborts
    /// nothing. **With no pass the timeout still reaches every upload** (LC23),
    /// including when that read fails.
    ///
    /// A read-only store removes nothing — the mode's promise, read from the
    /// store rather than from the caller.
    #[tracing::instrument(
        target = "tinio::storage",
        level = "info",
        skip_all,
        fields(dry_run = self.dry_run)
    )]
    pub async fn abort_stale_uploads(&self) -> Result<u64, Error> {
        if self.multipart_ttl.is_none() {
            return Ok(0);
        }
        let owners = match BucketMeta::lifecycles(&self.store).await {
            Ok(configs) => lifecycle::ActionOwners::from_configs(&configs, self.lifecycle_pass),
            Err(err) if !self.lifecycle_pass => {
                tracing::warn!(
                    %err,
                    "lifecycle rules unreadable; the multipart timeout still applies"
                );
                lifecycle::ActionOwners::unread(false)
            }
            Err(err) => return Err(err.into()),
        };
        let Some(governed) = owners.abort_excluded() else {
            return Ok(0);
        };
        self.abort_with(&governed).await
    }

    /// The abandoned-upload stage over a governed set the caller already read:
    /// the one-transaction abort, with the read-only and clock guards.
    async fn abort_with(&self, governed: &[String]) -> Result<u64, Error> {
        if !self.store.access().is_writable() {
            return Ok(0);
        }
        // A clock below the timeout has no upload to judge: nothing was initiated
        // before an instant that is not on the timeline.
        let Some(ttl) = self.multipart_ttl else {
            return Ok(0);
        };
        let Some(cutoff) = SystemTime::now().checked_sub(ttl) else {
            return Ok(0);
        };
        Ok(self.store.abort_stale_uploads(cutoff, governed).await?)
    }

    /// The whole serving pass: the repair stages plus the abandoned-upload
    /// stage, over **one** read of the rules table. The CLI's background loop
    /// calls this; [`Self::repair`] and [`Self::abort_stale_uploads`] stay for
    /// the doctor and for callers that want one stage alone. The
    /// abandoned-upload stage runs only when [`Self::with_multipart_ttl`]
    /// supplied a clock.
    ///
    /// An abandoned-upload failure is reported through the action stream rather
    /// than failing the pass: the repair actions already collected are still
    /// the caller's to drain, exactly as the two-call form answered them.
    #[tracing::instrument(
        target = "tinio::storage",
        level = "info",
        skip_all,
        fields(dry_run = self.dry_run)
    )]
    pub async fn pass(&self) -> Result<(ActionStream<Error>, u64), Error> {
        let _pass = PassTimer::new(Pass::PgCleanup);
        let mut actions: Vec<Result<RepairAction, Error>> = Vec::new();
        let remove = self.may_remove();
        // A run with no lifecycle pass keeps the knobs when the rules cannot
        // be read (LC23). A run that builds the pass still fails the pass: it
        // must not judge a bucket by a knob its rule displaced.
        let owners = match self.lifecycle_configs().await {
            Ok(configs) => lifecycle::ActionOwners::from_configs(&configs, self.lifecycle_pass),
            Err(err) if !self.lifecycle_pass => {
                record_pg_pass_failure(PgSection::Rules);
                actions.push(Err(err));
                lifecycle::ActionOwners::unread(false)
            }
            Err(err) => {
                record_pg_pass_failure(PgSection::Rules);
                return Err(err);
            }
        };
        self.reclaim_orphan_chunks(remove, &mut actions).await;
        self.reclaim_orphan_parts(remove, &mut actions).await;
        self.expire_versions(remove, &owners, &mut actions).await;
        self.reclaim_blocks(remove, &mut actions).await;
        let aborted = match (self.multipart_ttl.is_some(), owners.abort_excluded()) {
            (false, _) | (true, None) => Ok(0),
            (true, Some(governed)) => self.abort_with(&governed).await,
        };
        let aborted = match aborted {
            Ok(aborted) => aborted,
            Err(err) => {
                record_pg_pass_failure(PgSection::Uploads);
                actions.push(Err(err));
                0
            }
        };
        Ok((Box::pin(stream::iter(actions)), aborted))
    }

    /// Whether this run may remove rows: the caller did not ask for a report
    /// **and** the store is writable. The two are one answer, so no stage can
    /// write a read-only store by reading only one of them.
    fn may_remove(&self) -> bool {
        !self.dry_run && self.store.access().is_writable()
    }

    /// Reclaim orphan chunks (content rows no live head names, older than the
    /// grace by their own `published` stamp, and outside the head's current
    /// allocation).
    ///
    /// The scan's pages are consumed one at a time. A report answers one "would
    /// reclaim …" action per candidate **as its page arrives**; a real run
    /// deletes each page's candidates in one statement, which the store answers
    /// only for the rows that page's predicate still calls residue, and the run
    /// reports the total it removed. Nothing is written before the page it
    /// belongs to is read, so a report is always the state the fix would have
    /// seen.
    #[tracing::instrument(target = "tinio::storage", level = "debug", skip_all)]
    async fn reclaim_orphan_chunks(
        &self,
        remove: bool,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) {
        let mut pages = self.store.orphan_chunk_pages(self.grace);
        let mut reclaimed = 0_u64;
        while let Some(page) = pages.next().await {
            let rows = match page {
                Ok(rows) => rows,
                Err(err) => {
                    record_pg_pass_failure(PgSection::Chunks);
                    actions.push(Err(Error::from(err)));
                    return;
                }
            };
            if !remove {
                for row in &rows {
                    actions.push(Ok(RepairAction {
                        level: RepairActionLevel::Warn,
                        description: format!(
                            "would reclaim orphan {} chunk `{}` (generation {}) of {}",
                            row.kind, row.ckey, row.seq, row.bucket
                        ),
                    }));
                }
                continue;
            }
            match self.store.reclaim_orphan_chunks(&rows, self.grace).await {
                Ok(gone) => reclaimed = reclaimed.saturating_add(gone),
                Err(err) => {
                    record_pg_pass_failure(PgSection::Chunks);
                    actions.push(Err(Error::from(err)));
                    return;
                }
            }
        }
        if reclaimed > 0 {
            actions.push(Ok(RepairAction {
                level: RepairActionLevel::Warn,
                description: format!("reclaimed {reclaimed} orphan chunk row(s)"),
            }));
        }
    }

    /// Reclaim staged parts whose upload is gone — one action per part, a
    /// report and a real run alike (the parts are a handful against the chunk
    /// table's rows, and the action names the part an operator would otherwise
    /// have to find by its key). Both paths read the store's pages, and a real
    /// run's removals are one statement per page, keyed by the keys it
    /// returned: a part's chunks and its head go in that one statement, so a
    /// page that fails part way cannot leave a head whose chunks are gone.
    #[tracing::instrument(target = "tinio::storage", level = "debug", skip_all)]
    async fn reclaim_orphan_parts(
        &self,
        remove: bool,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) {
        let mut pages = self.store.orphan_part_pages();
        while let Some(page) = pages.next().await {
            let rows: Vec<OrphanPart> = match page {
                Ok(rows) => rows,
                Err(err) => {
                    record_pg_pass_failure(PgSection::Parts);
                    actions.push(Err(Error::from(err)));
                    return;
                }
            };
            if remove && let Err(err) = self.store.reclaim_orphan_parts(&rows).await {
                record_pg_pass_failure(PgSection::Parts);
                actions.push(Err(Error::from(err)));
                return;
            }
            for row in &rows {
                let verb = if remove { "reclaimed" } else { "would reclaim" };
                actions.push(Ok(RepairAction {
                    level: RepairActionLevel::Warn,
                    description: format!(
                        "{verb} staged part `{}` of {} whose upload is gone",
                        row.ckey, row.bucket
                    ),
                }));
            }
        }
    }

    /// The stored rules. The caller keeps its own read-error policy; this
    /// helper owns the one place the rules table is read for the pass's stages.
    async fn lifecycle_configs(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, Error> {
        Ok(BucketMeta::lifecycles(&self.store).await?)
    }

    /// Expire the noncurrent versions past the retention thresholds.
    ///
    /// The store answers the history rows newest-first per key, one page at a
    /// time, and a page boundary can fall **inside** a key: the retention rule
    /// ([`retires`], `tinio-core`'s) ranks a row by its position among *all* of
    /// the key's history rows, so the trailing key of every page is held back
    /// until the page that carries its last row arrives. Each complete key is
    /// then one `VersionMeta::expire_many`, one durable commit per key rather
    /// than one per version.
    ///
    /// A bucket whose rules own this action is skipped (LC21). A pass that
    /// cannot read the rules stands every bucket down; a run with no pass
    /// does not (LC23).
    #[tracing::instrument(target = "tinio::storage", level = "debug", skip_all)]
    async fn expire_versions(
        &self,
        remove: bool,
        owners: &lifecycle::ActionOwners,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) {
        if !retention_enabled(self.noncurrent_keep, self.noncurrent_expire) {
            return; // the unset-key reading: keep every version
        }
        let now = SystemTime::now();
        let mut pages = self.store.retention_row_pages();
        let mut held: Vec<HistoryRow> = Vec::new();
        while let Some(page) = pages.next().await {
            let page = match page {
                Ok(page) => page,
                Err(err) => {
                    record_pg_pass_failure(PgSection::Versions);
                    actions.push(Err(Error::from(err)));
                    return;
                }
            };
            let mut run = mem::take(&mut held);
            run.extend(page);
            let Some(last) = run.last() else { continue };
            // The first row of the buffer's last key ends the part of it the
            // rule can already judge; the rest waits for the next page.
            let start = run
                .iter()
                .take(run.len() - 1)
                .rposition(|row| row.bucket != last.bucket || row.key != last.key)
                .map_or(0, |position| position + 1);
            held = run.split_off(start);
            self.expire_run(&run, remove, owners, now, actions).await;
        }
        self.expire_run(&held, remove, owners, now, actions).await;
    }

    /// Apply the retention rule to a run of history rows that begins and ends
    /// on a key boundary, one key at a time.
    async fn expire_run(
        &self,
        rows: &[HistoryRow],
        remove: bool,
        owners: &lifecycle::ActionOwners,
        now: SystemTime,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) {
        let mut index = 0;
        while index < rows.len() {
            let bucket = &rows[index].bucket;
            let key_wire = &rows[index].key;
            let mut end = index + 1;
            while end < rows.len() && rows[end].bucket == *bucket && rows[end].key == *key_wire {
                end += 1;
            }
            let history = &rows[index..end];
            index = end;
            if !owners.knob_applies_noncurrent(bucket) {
                continue; // LC21: the bucket's own rule owns this action
            }
            let seqs: Vec<u64> = history
                .iter()
                .enumerate()
                .filter(|(position, row)| {
                    retires(
                        *position,
                        row.became_noncurrent as u64,
                        row.created_nanos as u64,
                        self.noncurrent_keep,
                        self.noncurrent_expire,
                        now,
                    )
                })
                .map(|(_, row)| row.seq as u64)
                .collect();
            if seqs.is_empty() {
                continue;
            }
            // A key the domain refuses cannot be addressed by an expiry, so
            // its rows keep their blocks and retire nothing.
            let Ok(key) = object::key(key_wire) else {
                continue;
            };
            if remove {
                // The lock-aware primitive (OL8): a protected sequence is
                // skipped, never an error. The report's `removed` is the count
                // the action states — `seqs` lists candidates, not removals —
                // and each protected sequence is recorded for the
                // protected-deletions family under `operation = sweeper`
                // (OL19, OL25).
                match VersionMeta::expire_many_report(&self.store, bucket, &key, &seqs).await {
                    Ok(report) => {
                        for (_seq, reason) in &report.protected {
                            record_protected_deletion(LockOperation::Sweeper, *reason);
                        }
                        if report.removed > 0 {
                            actions.push(Ok(RepairAction {
                                level: RepairActionLevel::Warn,
                                description: format!(
                                    "expired {} noncurrent version(s) of `{key}` in {bucket}",
                                    report.removed
                                ),
                            }));
                        }
                    }
                    Err(err) => {
                        record_pg_pass_failure(PgSection::Versions);
                        actions.push(Err(Error::from(err)));
                    }
                }
                continue;
            }
            actions.push(Ok(RepairAction {
                level: RepairActionLevel::Warn,
                description: format!(
                    "would expire {} noncurrent version(s) of `{key}` in {bucket}",
                    seqs.len()
                ),
            }));
        }
    }

    /// Reclaim the blocks no live version row references.
    ///
    /// The live set is the union of every version row's block list, folded in
    /// one page at a time ([`PgStore::live_blob_pages`]); the sweep's walk then
    /// judges the store against it and the grace window, so a block a publish
    /// just stored but no row names yet is kept. A report names the candidates;
    /// a real run removes them and reports what went.
    #[tracing::instrument(target = "tinio::storage", level = "debug", skip_all)]
    async fn reclaim_blocks(&self, remove: bool, actions: &mut Vec<Result<RepairAction, Error>>) {
        let live = match self.live_set().await {
            Ok(live) => live,
            Err(err) => {
                record_pg_pass_failure(PgSection::Blocks);
                actions.push(Err(err));
                return;
            }
        };
        let blobs = Store::new(PgBlocks::new(self.store.clone()));
        let candidates = match blobs
            .sweep_candidates(&live, self.grace, SystemTime::now())
            .await
        {
            Ok(candidates) => candidates,
            Err(err) => {
                record_pg_pass_failure(PgSection::Blocks);
                actions.push(Err(Error::from(storage::Error::from(err))));
                return;
            }
        };
        if candidates.entries.is_empty() {
            return;
        }
        let bytes: u64 = candidates.entries.iter().map(|entry| entry.len).sum();
        if !remove {
            actions.push(Ok(RepairAction {
                level: RepairActionLevel::Warn,
                description: format!(
                    "would reclaim {} unreferenced block(s) ({bytes} bytes)",
                    candidates.entries.len()
                ),
            }));
            return;
        }
        match blobs.remove_candidates(&candidates).await {
            Ok(report) => actions.push(Ok(RepairAction {
                level: RepairActionLevel::Warn,
                description: format!(
                    "reclaimed {} unreferenced block(s) ({} bytes)",
                    report.removed, report.bytes
                ),
            })),
            Err(err) => {
                record_pg_pass_failure(PgSection::Blocks);
                actions.push(Err(Error::from(storage::Error::from(err))));
            }
        }
    }

    /// Every live block address: the union of the block lists of every version
    /// row of every bucket, folded from one page of rows at a time. The set
    /// itself is one address per live block — the sweep's own input — while the
    /// rows carrying them are the store's [`PgStore::live_blob_pages`], whose
    /// pages are bounded.
    async fn live_set(&self) -> Result<LiveSet, Error> {
        let mut pages = self.store.live_blob_pages();
        let mut hashes: HashSet<BlockHash> = HashSet::new();
        while let Some(page) = pages.next().await {
            for blob in page? {
                for block in blob.blocks() {
                    hashes.insert(block.hash);
                }
            }
        }
        Ok(LiveSet::from_hashes(hashes))
    }
}

#[async_trait]
impl Cleanup for PgCleanup {
    type Error = Error;

    /// Run the repair pipeline for `kind`.
    ///
    /// Both kinds run the two residue scans (orphan chunks, upload-gone
    /// parts) — the cheap, deterministic items — and [`RepairKind::Full`] adds
    /// the retention pass and the block sweep. The two costs are separated
    /// exactly as `tinio-fs` separates them: the residue scans read bounded
    /// tables, while the block sweep walks the whole block store.
    ///
    /// A run that may not write — `dry_run`, or a store constructed read-only —
    /// reports what it would do and removes nothing; the store's own mode is
    /// what decides, so a read-only replica's `--fix` still diagnoses.
    async fn repair(&self, kind: RepairKind) -> Result<ActionStream<Error>, Error> {
        let mut actions: Vec<Result<RepairAction, Error>> = Vec::new();
        let remove = self.may_remove();
        self.reclaim_orphan_chunks(remove, &mut actions).await;
        self.reclaim_orphan_parts(remove, &mut actions).await;
        if kind == RepairKind::Full {
            // LC21: the stored rules, read once for the whole pass. A failed
            // read stands the knobs down only when this run builds the pass.
            let owners = match self.lifecycle_configs().await {
                Ok(configs) => lifecycle::ActionOwners::from_configs(&configs, self.lifecycle_pass),
                Err(err) => {
                    actions.push(Err(err));
                    lifecycle::ActionOwners::unread(self.lifecycle_pass)
                }
            };
            self.expire_versions(remove, &owners, &mut actions).await;
            self.reclaim_blocks(remove, &mut actions).await;
        }
        Ok(Box::pin(stream::iter(actions)))
    }

    /// The background-scanner path: the residue scans alone.
    ///
    /// The fs scanner's per-pass path reclaims stale bucket records and meta
    /// orphans; pg's residue is the orphan chunk and the upload-gone part, so
    /// those are what this answers. The retention pass and the block sweep
    /// stay with [`Self::repair`] with [`RepairKind::Full`].
    ///
    /// **This is not the whole serving path's pass.** pg has no separate sweeper
    /// (the fs arm's serving path runs the scanner *and* the sweeper), so
    /// `tinio-cli`'s background pass runs [`Self::repair`] with
    /// [`RepairKind::Full`] — the set below plus the retention and the block
    /// sweep — rather than this method alone. A caller that wants only the
    /// residue scans (a cheap probe) still finds them here.
    async fn reclaim_meta_orphans(&self) -> Result<ActionStream<Error>, Error> {
        let mut actions: Vec<Result<RepairAction, Error>> = Vec::new();
        let remove = self.may_remove();
        self.reclaim_orphan_chunks(remove, &mut actions).await;
        self.reclaim_orphan_parts(remove, &mut actions).await;
        Ok(Box::pin(stream::iter(actions)))
    }
}
