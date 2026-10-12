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
//!   and is protected only by its own stamps. The predicate is the store's,
//!   evaluated once by its own scan, and the removal deletes only the keys that
//!   scan returned, so the two cannot drift apart.
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

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::stream;
use tinio_blob::{LiveSet, Store};
use tinio_core::{
    bucket,
    cleanup::{
        ActionStream, Cleanup, CleanupOptions, RepairAction, RepairActionLevel, RepairKind,
        retention_enabled, retires,
    },
    meta::{BucketMeta, VersionMeta},
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
    /// The store's scan is the predicate's one home and drives both paths: a
    /// report answers one "would reclaim …" action per orphan generation, and a
    /// real run removes exactly the keys the scan returned, answering the count
    /// the store's own delete reports.
    async fn reclaim_orphan_chunks(
        &self,
        remove: bool,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) {
        let rows = match self.store.orphan_chunks(self.grace).await {
            Ok(rows) => rows,
            Err(err) => {
                actions.push(Err(Error::from(err)));
                return;
            }
        };
        if rows.is_empty() {
            return;
        }
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
            return;
        }
        match self.store.reclaim_orphan_chunks(&rows).await {
            Ok(gone) => actions.push(Ok(RepairAction {
                level: RepairActionLevel::Warn,
                description: format!("reclaimed {gone} orphan chunk row(s)"),
            })),
            Err(err) => actions.push(Err(Error::from(err))),
        }
    }

    /// Reclaim staged parts whose upload is gone — one action per part, a
    /// report and a real run alike (the parts are a handful against the chunk
    /// table's rows, and the action names the part an operator would otherwise
    /// have to find by its key). Both paths read the store's scan, and a real
    /// run's removals are the one statement keyed by the keys it returned.
    async fn reclaim_orphan_parts(
        &self,
        remove: bool,
        actions: &mut Vec<Result<RepairAction, Error>>,
    ) {
        let rows: Vec<OrphanPart> = match self.store.orphan_part_heads().await {
            Ok(rows) => rows,
            Err(err) => {
                actions.push(Err(Error::from(err)));
                return;
            }
        };
        if rows.is_empty() {
            return;
        }
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

    /// Expire the noncurrent versions past the retention thresholds.
    ///
    /// The store's scan answers every history row newest-first per key —
    /// contiguously, because that is the read's own order — so the retention
    /// rule ([`retires`], `tinio-core`'s) is applied per key in one pass,
    /// and one `VersionMeta::expire_many` retires them: one durable commit per
    /// key rather than one per version.
    async fn expire_versions(&self, remove: bool, actions: &mut Vec<Result<RepairAction, Error>>) {
        if !retention_enabled(self.noncurrent_keep, self.noncurrent_expire) {
            return; // the unset-key reading: keep every version
        }
        let rows: Vec<HistoryRow> = match self.store.retention_rows().await {
            Ok(rows) => rows,
            Err(err) => {
                actions.push(Err(Error::from(err)));
                return;
            }
        };
        let now = SystemTime::now();
        let mut index = 0;
        while index < rows.len() {
            let bucket = rows[index].bucket.clone();
            let key_wire = rows[index].key.clone();
            let mut end = index;
            while end < rows.len() && rows[end].bucket == bucket && rows[end].key == key_wire {
                end += 1;
            }
            let seqs: Vec<u64> = rows[index..end]
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
            index = end;
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
    /// The live set is the union of every version row's block list, built once
    /// ([`LiveSet::from_blobs`]); the sweep's walk then judges the store
    /// against it and the grace window, so a block a publish just stored but no
    /// row names yet is kept. A report names the candidates; a real run removes
    /// them and reports what went.
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
    /// row of every bucket. The rows are read per bucket through the store's
    /// contract (`VersionMeta::bucket_rows`), the same reduction
    /// `tinio_blob::LiveSet::from_blobs` performs for the fs sweep.
    async fn live_set(&self) -> Result<LiveSet, Error> {
        let buckets = BucketMeta::load_all(&self.store).await?;
        let mut blobs = Vec::new();
        for (name, _created) in buckets {
            let Ok(bucket) = bucket::name(&name) else {
                continue;
            };
            for row in VersionMeta::bucket_rows(&self.store, &bucket).await? {
                blobs.push(row.blob);
            }
        }
        Ok(LiveSet::from_blobs(blobs))
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

#[cfg(test)]
#[expect(
    clippy::absolute_paths,
    reason = "test support: the leg spells the crate's public path absolutely on purpose — a shortened path would not prove the public one"
)]
mod tests {
    use futures::StreamExt;
    use tinio_core::{
        blob::BlockHash,
        cleanup::CleanupOptions,
        content::{ContentKey, ObjectContent, StagedObject},
        storage::{ListObjectsParams, ObjectOps},
    };
    use tinio_util::testing::hex;

    use super::*;
    use crate::{
        _store_pg::{
            _sqlx::{query, query_scalar},
            pool::Pool,
            test_support::TestDb,
        },
        Access, PgStorage,
    };

    /// Drain an action stream into the actions it reported.
    async fn collect(actions: ActionStream<Error>) -> Vec<RepairAction> {
        let mut out = Vec::new();
        let mut actions = actions;
        while let Some(action) = actions.next().await {
            out.push(action.expect("a repair action"));
        }
        out
    }

    /// One scalar count, bound to the fixture bucket.
    async fn count(pool: &Pool, bucket: &str, sql: &'static str) -> i64 {
        query_scalar::<_, i64>(sql)
            .bind(bucket)
            .fetch_one(pool)
            .await
            .expect("a count")
    }

    /// The planted-orphan leg (PG7, PG23): a chunk no head names, a staged part whose upload
    /// is gone, an expired version row and a block no version row references are all reported
    /// in a dry run and reclaimed by the fix, while a live chunk and a **publish still in
    /// flight** are never reported and a listing never shows residue.
    #[tokio::test]
    async fn planted_orphans_report_in_a_dry_run_and_the_fix_reclaims_them() {
        let Ok(_) = std::env::var("TINIO_PG_DSN") else {
            eprintln!("skipping: TINIO_PG_DSN is not set");
            return;
        };
        let db = TestDb::create().await.expect("a scratch database");
        let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
        let store = PgStore::connect_with_pool(pool.clone(), Access::ReadWrite)
            .await
            .expect("a writable store (migrating the scratch database)");

        let bucket = bucket::name("cleanup-leg").expect("a valid bucket name");
        let wire = "cleanup-leg";
        let part_ckey = format!("p:{}:0001", hex(b"gone-upload"));
        let dead_block = BlockHash::of(b"dead-block").to_hex();

        // The fixture, planted through SQL: a bucket, one **live** object (`live.txt`), and
        // the four orphan shapes the scans are about.
        query(
            "INSERT INTO buckets (name, created, tags, acl, cors) \
             VALUES ($1, now(), '{}'::jsonb, ROW(NULL::text, ARRAY[]::acl_grant[])::acl, '')",
        )
        .bind(wire)
        .execute(&pool)
        .await
        .expect("the bucket row");
        query(
            "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len) \
             VALUES ($1, 'object', 'o:live.txt', 1, 1, 4)",
        )
        .bind(wire)
        .execute(&pool)
        .await
        .expect("the live head");
        query(
            "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk) \
             VALUES ($1, 'object', 'o:live.txt', 1, 0, $2)",
        )
        .bind(wire)
        .bind(b"live".as_slice())
        .execute(&pool)
        .await
        .expect("the live chunk");
        query(
            "INSERT INTO object_meta (bucket, key, etag, size, mtime, identity, tags, checksum, acl) \
             VALUES ($1, 'live.txt', 'e', 4, now(), 0, '{}'::jsonb, '', ROW(NULL::text, ARRAY[]::acl_grant[])::acl)",
        )
        .bind(wire)
        .execute(&pool)
        .await
        .expect("the live object row");
        // (a) an orphan chunk: a generation no head names, its row stamped past the grace
        // (an absent/current stamp would read as in flight under the sweep's age gate).
        query(
            "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published) \
             VALUES ($1, 'object', 'o:orphan.txt', 7, 0, $2, now() - interval '2 hours')",
        )
        .bind(wire)
        .bind(b"dead".as_slice())
        .execute(&pool)
        .await
        .expect("the orphan chunk");
        // (b) a staged part whose upload is gone: a head and a chunk, no `uploads` row.
        query(
            "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len) \
             VALUES ($1, 'part', $2, 1, 1, 1)",
        )
        .bind(wire)
        .bind(part_ckey.as_str())
        .execute(&pool)
        .await
        .expect("the part head");
        query(
            "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk) \
             VALUES ($1, 'part', $2, 1, 0, $3)",
        )
        .bind(wire)
        .bind(part_ckey.as_str())
        .bind(b"p".as_slice())
        .execute(&pool)
        .await
        .expect("the part chunk");
        // (c) two history version rows at a key with no current slot; the keep budget of one
        // expires the older.
        for (seq, id) in [(1_i64, "planted-v1"), (2_i64, "planted-v2")] {
            query(
                "INSERT INTO version_rows (bucket, key, seq, id, blob, size, etag, created, \
                 became_noncurrent, kind, tags, checksum, acl) \
                 VALUES ($1, 'v.txt', $2, $3, '', 0, 'planted', now(), 1, 'object', \
                 '{}'::jsonb, '', ROW(NULL::text, ARRAY[]::acl_grant[])::acl)",
            )
            .bind(wire)
            .bind(seq)
            .bind(id)
            .execute(&pool)
            .await
            .expect("a planted version row");
        }
        // (d) a block no version row references, published past the grace.
        query(
            "INSERT INTO blocks (hash, seq, chunk, len, published) \
             VALUES ($1, 0, $2, 1, now() - interval '1 hour')",
        )
        .bind(dead_block.as_str())
        .bind(b"b".as_slice())
        .execute(&pool)
        .await
        .expect("the dead block");

        // C1's fixture: a publish **still in flight**. `stage` allocates the generation and
        // commits its chunks while the head's `live_seq` is still NULL — exactly what the
        // orphan predicate must not read as residue.
        let inflight = object::key("inflight.txt").expect("a valid key");
        let mut staged = store
            .stage(&bucket, &ContentKey::Object(inflight.clone()))
            .await
            .expect("a staged generation");
        staged
            .write(vec![0_u8; 4 * 1024 * 1024].into())
            .await
            .expect("the staged body");
        drop(staged);

        // PG23: a listing is row-backed, so both the residue and the in-flight object are
        // invisible before any cleanup runs.
        let reader = PgStorage::connect_with_pool(pool.clone(), Access::ReadOnly)
            .await
            .expect("a read-only reader");
        let listed: Vec<String> = reader
            .list_objects(ListObjectsParams {
                bucket: bucket.clone(),
                prefix: String::new(),
                delimiter: None,
                start_after: None,
                max_keys: 100,
            })
            .await
            .expect("a listing")
            .objects
            .iter()
            .map(|info| info.key.to_string())
            .collect();
        assert!(
            listed.iter().any(|key| key == "live.txt"),
            "the live object is listed: {listed:?}"
        );
        assert!(
            !listed.iter().any(|key| key == "orphan.txt"),
            "a listing never shows residue: {listed:?}"
        );
        assert!(
            !listed.iter().any(|key| key == "inflight.txt"),
            "a listing never shows an unflipped publish: {listed:?}"
        );

        // I2: the store's own mode decides. A read-only store's report still diagnoses, and
        // its real run writes nothing at all.
        let read_only = PgStore::connect_with_pool(pool.clone(), Access::ReadOnly)
            .await
            .expect("a read-only store");
        let ro_report = PgCleanup::new(
            read_only.clone(),
            CleanupOptions {
                dry_run: true,
                ..Default::default()
            },
        );
        let actions = collect(ro_report.repair(RepairKind::Full).await.expect("a report")).await;
        assert!(
            actions
                .iter()
                .any(|action| action.description.contains("orphan.txt")),
            "a read-only dry run still reports: {actions:?}"
        );
        let ro_fix = PgCleanup::new(read_only.clone(), CleanupOptions::default());
        let _ = collect(ro_fix.repair(RepairKind::Full).await.expect("a run")).await;
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:orphan.txt'"
            )
            .await,
            1,
            "a read-only store's cleanup removes nothing, whatever the options say"
        );

        // The dry run: every orphan is named, the live chunk and the in-flight publish are
        // not, and nothing is removed.
        let dry = PgCleanup::new(
            store.clone(),
            CleanupOptions {
                dry_run: true,
                ..Default::default()
            },
        )
        .with_retention(1, 0);
        let actions = collect(dry.repair(RepairKind::Full).await.expect("a dry run")).await;
        let text = actions
            .iter()
            .map(|action| action.description.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("orphan.txt"), "the orphan chunk: {text}");
        assert!(text.contains(&part_ckey), "the upload-gone part: {text}");
        assert!(text.contains("v.txt"), "the expired version row: {text}");
        assert!(
            text.contains("unreferenced block"),
            "the dead block: {text}"
        );
        assert!(
            !text.contains("live.txt"),
            "a live chunk is never reported: {text}"
        );
        assert!(
            !text.contains("inflight.txt"),
            "a publish still in flight is never reported: {text}"
        );
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content WHERE bucket = $1"
            )
            .await,
            7,
            "a dry run removes nothing"
        );

        // The fix: the same handle in write mode reclaims exactly those rows.
        let fix = PgCleanup::new(store.clone(), CleanupOptions::default()).with_retention(1, 0);
        let actions = collect(fix.repair(RepairKind::Full).await.expect("a fix")).await;
        assert!(
            actions
                .iter()
                .any(|action| action.description.contains("reclaimed")),
            "the fix reports what it reclaimed: {actions:?}"
        );

        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:orphan.txt'"
            )
            .await,
            0,
            "the orphan chunk is reclaimed"
        );
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content_heads WHERE bucket = $1 AND kind = 'part'"
            )
            .await,
            0,
            "the upload-gone part's head is reclaimed"
        );
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content WHERE bucket = $1 AND kind = 'part'"
            )
            .await,
            0,
            "the upload-gone part's chunks are reclaimed"
        );
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM version_rows WHERE bucket = $1 AND seq = 1"
            )
            .await,
            0,
            "the older history row is expired"
        );
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM version_rows WHERE bucket = $1 AND seq = 2"
            )
            .await,
            1,
            "the keep budget spares the newest history row"
        );
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:live.txt'"
            )
            .await,
            1,
            "a live chunk is never reclaimed"
        );
        assert_eq!(
            count(&pool, wire, "SELECT count(*) FROM blocks WHERE hash = $1").await,
            0,
            "the dead block is removed, not merely reported"
        );
        assert!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:inflight.txt'"
            )
            .await
                > 0,
            "a publish still in flight keeps its chunks"
        );

        // The grace is a window, not a permanent shield: aged past it on **both** gates —
        // the head's allocation and the rows' own write stamp — a crashed partial publish's
        // chunks are reclaimed like any other residue.
        query(
            "UPDATE content_heads SET allocated_at = now() - interval '2 hours' \
             WHERE bucket = $1 AND ckey = 'o:inflight.txt'",
        )
        .bind(wire)
        .execute(&pool)
        .await
        .expect("the aged allocation");
        query(
            "UPDATE content SET published = now() - interval '2 hours' \
             WHERE bucket = $1 AND ckey = 'o:inflight.txt'",
        )
        .bind(wire)
        .execute(&pool)
        .await
        .expect("the aged rows");
        let aged = PgCleanup::new(store.clone(), CleanupOptions::default());
        let _ = collect(aged.repair(RepairKind::Full).await.expect("a control run")).await;
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:inflight.txt'"
            )
            .await,
            0,
            "past the grace, a crashed generation's chunks are reclaimed"
        );

        drop(reader);
        drop(read_only);
        drop(store);
        db.drop_db().await;
    }
}
