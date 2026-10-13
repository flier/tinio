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
//! Three rules bind the module:
//!
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
    cleanup::{
        ActionStream, Cleanup, CleanupOptions, RepairAction, RepairActionLevel, RepairKind,
        retention_enabled, retires,
    },
    meta::VersionMeta,
    object, storage,
};

use crate::{
    _store_pg::{
        PgStore,
        blocks::PgBlocks,
        cleanup::{HistoryRow, OrphanPart},
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
                    actions.push(Err(Error::from(err)));
                    return;
                }
            };
            if remove && let Err(err) = self.store.reclaim_orphan_parts(&rows).await {
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

    /// Expire the noncurrent versions past the retention thresholds.
    ///
    /// The store answers the history rows newest-first per key, one page at a
    /// time, and a page boundary can fall **inside** a key: the retention rule
    /// ([`retires`], `tinio-core`'s) ranks a row by its position among *all* of
    /// the key's history rows, so the trailing key of every page is held back
    /// until the page that carries its last row arrives. Each complete key is
    /// then one `VersionMeta::expire_many`, one durable commit per key rather
    /// than one per version.
    async fn expire_versions(&self, remove: bool, actions: &mut Vec<Result<RepairAction, Error>>) {
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
            self.expire_run(&run, remove, now, actions).await;
        }
        self.expire_run(&held, remove, now, actions).await;
    }

    /// Apply the retention rule to a run of history rows that begins and ends
    /// on a key boundary, one key at a time.
    async fn expire_run(
        &self,
        rows: &[HistoryRow],
        remove: bool,
        now: SystemTime,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) {
        let mut index = 0;
        while index < rows.len() {
            let bucket = rows[index].bucket.clone();
            let key_wire = rows[index].key.clone();
            let mut end = index + 1;
            while end < rows.len() && rows[end].bucket == bucket && rows[end].key == key_wire {
                end += 1;
            }
            let history = &rows[index..end];
            index = end;
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
            let Ok(key) = object::key(&key_wire) else {
                continue;
            };
            if remove {
                match VersionMeta::expire_many(&self.store, &bucket, &key, &seqs).await {
                    Ok(()) => actions.push(Ok(RepairAction {
                        level: RepairActionLevel::Warn,
                        description: format!(
                            "expired {} noncurrent version(s) of `{key}` in {bucket}",
                            seqs.len()
                        ),
                    })),
                    Err(err) => actions.push(Err(Error::from(err))),
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
    async fn reclaim_blocks(&self, remove: bool, actions: &mut Vec<Result<RepairAction, Error>>) {
        let live = match self.live_set().await {
            Ok(live) => live,
            Err(err) => {
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
            Err(err) => actions.push(Err(Error::from(storage::Error::from(err)))),
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
            self.expire_versions(remove, &mut actions).await;
            self.reclaim_blocks(remove, &mut actions).await;
        }
        Ok(Box::pin(stream::iter(actions)))
    }

    /// The background-scanner path: the residue scans alone.
    ///
    /// The fs scanner's per-pass path reclaims stale bucket records and meta
    /// orphans; pg's residue is the orphan chunk and the upload-gone part, so
    /// those two are what this answers. The retention pass and the block sweep
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
