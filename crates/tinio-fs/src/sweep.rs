//! The async sweep (task T046, FR-014).
//!
//! Time-driven, mtime-based cleanup that runs while the server is live
//! (fs-backend.md §7): temp files older than `temp_ttl` (default 24 h),
//! multipart uploads idle longer than `multipart_ttl` (default 7 days,
//! idle = max(initiated_at, latest part mtime)), and — for the buckets
//! whose versioning state is enabled or suspended — the retention pass
//! (expire the noncurrent versions past `noncurrent_keep` /
//! `noncurrent_expire_days`) followed by the block-store pass (reclaim
//! the blocks no version row references, reap the bucket's own staging
//! residue, and reclaim the block subtrees of buckets that no longer
//! exist). **In a run that builds the lifecycle pass, a bucket whose stored
//! lifecycle configuration carries `NoncurrentVersionExpiration` or
//! `AbortIncompleteMultipartUpload` is governed by its rule for that action, so
//! the `[s3]` knob beside it does not apply to that bucket** (LC21, per action);
//! **with the pass off nothing is governed, so the knobs do their normal work on
//! every bucket, stored rules or not** (LC23).
//! Non-blocking: the sweep sleeps in bounded chunks and
//! re-checks shutdown, so it never delays request handling or shutdown.
//! Complements (does not replace) the event-driven `FsCleanup` startup
//! repair.

#[cfg(feature = "versioning")]
use std::collections::BTreeMap;
use std::{
    collections::HashSet,
    io::ErrorKind,
    path::Path,
    time::{Duration, SystemTime},
};

use smart_default::SmartDefault;
use tokio::{fs, sync::watch};

#[cfg(feature = "versioning")]
use crate::{
    _blob::{LiveSet, SweepCandidates, SweepReport},
    _core::{
        blob::{BlobRef, BlockHash},
        cleanup::{retention_enabled, retires},
        object,
    },
    _store_redb::{object_meta, version as rows},
    error::projected,
    metrics::{record_block_bytes, record_noncurrent_versions, record_sweep},
};
use crate::{
    _core::{bucket::Name, lifecycle, multipart::DEFAULT_EXPIRE_DAYS, storage},
    _util::pacing,
    backend::FsStorage,
    error::Error,
    fsutil::entries_of,
    path::TMP_DIR_NAME,
};

/// Sweep construction options (contracts/config.md `[s3]` TTLs and this
/// design's retention keys).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_fs::sweep::Options;
///
/// let options = Options::default();
/// assert_eq!(options.temp_ttl, Duration::from_secs(86400));
/// assert_eq!(options.noncurrent_keep, 0, "unset: keep every version");
/// assert_eq!(options.noncurrent_expire_days, 0, "unset: never expire");
/// assert_eq!(options.block_grace, Duration::from_secs(600));
/// assert!(options.lifecycle_pass, "the capability is on by default");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault)]
pub struct Options {
    /// Stale temp-file timeout (`[s3] temp_ttl_hours`).
    #[default(_code = "Duration::from_secs(24 * 3600)")]
    pub temp_ttl: Duration,
    /// Abandoned-upload timeout (`[s3] multipart_expire_days`). `None` when the
    /// operator did not write the key: the filesystem sweeper applies the
    /// default via [`Options::multipart_ttl`], while pg's stage stays off.
    #[default(_code = "None")]
    pub multipart_ttl: Option<Duration>,
    /// How many noncurrent versions of a key to retain (`[s3]
    /// noncurrent_keep`; the newest N). `0` = keep every version, the safe
    /// reading of an unset key.
    #[default(_code = "0")]
    pub noncurrent_keep: u64,
    /// How many days a version may stay noncurrent before it is expired
    /// (`[s3] noncurrent_expire_days`, measured from
    /// `became_noncurrent_at`; a delete marker counts like any version).
    /// `0` = never expire by age. With
    /// [`noncurrent_keep`](Self::noncurrent_keep) set too, whichever
    /// threshold is met first expires the version.
    #[default(_code = "0")]
    pub noncurrent_expire_days: u64,
    /// The minimum age before an unreferenced block may be reclaimed
    /// (`[s3] block_grace_minutes`, the Garage precedent — ten minutes in
    /// its implementation).
    ///
    /// The default is ten minutes because `Duration::default()` is zero,
    /// and a zero grace would silently disarm the guard it narrows: a
    /// publish already in flight may still be storing a block the live
    /// set does not know about. The grace is **not** the race guard — a
    /// dedup hit leaves the target block's timestamp untouched, so a
    /// publish that returns
    /// [`Published::Existing`](tinio_core::blob::Published::Existing)
    /// commits a row naming an old block the grace cannot cover; the
    /// re-check under the bucket mutation lock is what closes that race
    /// (see [`Sweeper`]) — but it is the only cover for a first publish
    /// still in flight, so it is never zero.
    #[default(_code = "Duration::from_secs(10 * 60)")]
    pub block_grace: Duration,
    /// Warn when the versioned buckets' block stores hold more than this
    /// many bytes (`[s3] block_bytes_warn`); `0` = no warning. The number
    /// compared is the sweep walk's own total ([`Summary::block_bytes`]) —
    /// the store's size as of the last pass — so the threshold is a
    /// capacity-planning signal, not a hard cap.
    #[default(_code = "0")]
    pub block_bytes_warn: u64,
    /// Whether this run builds the lifecycle pass (`[s3] lifecycle`, LC23) — the
    /// condition under which a bucket's stored rules take its two actions away
    /// from the knobs beside them (LC21). With no pass nothing is governed, so
    /// the knobs do their normal work on every bucket, stored rules or not;
    /// `true` is the default because an absent `[s3]` section leaves the
    /// capability on.
    #[default(_code = "true")]
    pub lifecycle_pass: bool,
}

impl Options {
    /// The clock the filesystem sweeper applies: the explicit
    /// `[s3] multipart_expire_days`, or seven days when the key was not written.
    pub fn multipart_ttl(&self) -> Duration {
        self.multipart_ttl
            .unwrap_or(Duration::from_secs(DEFAULT_EXPIRE_DAYS * 24 * 3600))
    }
}

/// Sweep cadence: one pass per hour. Not a config knob — the contract's
/// `[s3]` section defines only the two TTLs (contracts/config.md).
const SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

/// Remove the files of `dir` older than `max_age`, skipping directories and
/// entries that vanish underfoot — the shared walk of the state dir's `tmp/`
/// and each bucket's block-store staging dir. `label` names the file class in
/// the log.
async fn sweep_stale_files(
    dir: &Path,
    max_age: Duration,
    label: &str,
    now: SystemTime,
) -> Result<usize, Error> {
    let entries = entries_of(dir).await?;
    let mut removed = 0;
    for (path, name) in entries {
        let metadata = match fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if metadata.is_dir() {
            continue; // only files stage under these dirs
        }
        if let Ok(modified) = metadata.modified()
            && now
                .duration_since(modified)
                .map(|age| age >= max_age)
                .unwrap_or(false)
        {
            match fs::remove_file(&path).await {
                Ok(()) => {
                    tracing::info!("swept stale {label} file {name}");
                    removed += 1;
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
    }
    Ok(removed)
}

/// The async sweeper of a storage backend.
///
/// # The block reclamation race
///
/// A publish adds a block reference — its version row — while a sweep may
/// be judging what is unreferenced. The guard is the **re-check under the
/// bucket mutation lock**: every path that adds a reference (a PUT's
/// supersede, a versioned DELETE) holds that bucket's mutation lock across
/// both the block publish and the row commit, and the sweep takes the same
/// lock before it reads the live set the reclamation is judged against,
/// holding it until the store walk finishes. A reference added since any
/// earlier read therefore either cannot exist yet or is in the set. The
/// [`block_grace`](Options::block_grace) is not this guard: a dedup hit
/// leaves the block's timestamp untouched, so a publish that stored
/// nothing new can name a block older than the grace.
///
/// # Examples
///
/// ```rust
/// use std::{sync::Arc, time::Duration};
///
/// use tinio_core::{
///     mode::AccessMode,
///     pipeline::InlineRunner,
///     storage::{
///         DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
///     },
/// };
/// use tinio_fs::{
///     FsOptions, FsStorage,
///     sweep::{Options, Sweeper},
/// };
/// use tokio::{runtime::Runtime, sync::watch};
///
/// let root = tempfile::tempdir().unwrap();
/// let options = FsOptions {
///     follow_symlinks: false,
///     state_dir: None,
///     access: AccessMode::ReadWrite,
///     compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
///     meta_batch_size: DEFAULT_META_BATCH_SIZE,
///     meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
///     owner_uids: std::collections::HashMap::new(),
///     io_pipeline: Arc::new(InlineRunner::default()),
///     remove_pipeline: Arc::new(InlineRunner::default()),
///     db_pipeline: Arc::new(InlineRunner::default()),
/// };
/// let storage = FsStorage::new(root.path(), options).unwrap();
/// let sweeper = Sweeper::new(storage.clone(), Options::default());
/// let (tx, rx) = watch::channel(false);
/// Runtime::new().unwrap().block_on(async {
///     let task = tokio::spawn(async move { sweeper.run(rx).await });
///     tx.send(true).unwrap();
///     task.await.unwrap();
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Sweeper {
    storage: FsStorage,
    options: Options,
}

/// One key's retirement set: the key and the sequences the retention keys
/// retire from it.
#[cfg(feature = "versioning")]
type Retirement = (object::Key, Vec<u64>);

/// One buffered history row as the retention walk holds it: the sequence,
/// the noncurrent stamp, the creation time and the blob reference — the
/// columns [`retires`] reads and the live-set fold needs.
#[cfg(feature = "versioning")]
type BufferedRow = (u64, u64, u64, BlobRef);

/// One key's buffered history: the key and its rows, oldest first.
#[cfg(feature = "versioning")]
type HistoryBuffer = Option<(String, Vec<BufferedRow>)>;

/// One versioned bucket's retention view, from `Sweeper::plan_retention`'s
/// single read transaction.
#[cfg(feature = "versioning")]
struct BucketPlan {
    /// Every row's block addresses — the live set the block walk judges
    /// against.
    live: LiveSet,
    /// The history rows the walk saw, before the expiry — the gauge's
    /// number.
    noncurrent: u64,
    /// The retirements the thresholds imply, per key, applied under the
    /// bucket lock so nothing moves a slot between the decision and the
    /// removal.
    retire: Vec<Retirement>,
}

/// What `Sweeper::plan_retention`'s read transaction hands back.
#[cfg(feature = "versioning")]
struct RetentionScan {
    hashes: HashSet<BlockHash>,
    noncurrent: u64,
    retire: Vec<Retirement>,
}

/// Turn one key's history (oldest first) into its retirement decision —
/// the sequences the retention keys retire, indexed newest-first as
/// [`retires`] defines it — and fold the decision into the scan: a row the
/// keys retire contributes **no** live blocks (the same pass's walk must
/// see them as dead, or the reclamation would trail the expiry by a full
/// cadence), while every row that survives contributes its block list. A
/// key that is not a valid object key cannot be addressed by an expiry, so
/// its rows keep their blocks and retire nothing.
#[cfg(feature = "versioning")]
fn flush_retirement(
    history: HistoryBuffer,
    keep: u64,
    expire: Duration,
    now: SystemTime,
    scan: &mut RetentionScan,
) {
    let Some((key, history)) = history else {
        return;
    };
    // Only a valid object key can be addressed by an expiry.
    let key = object::key(&key).ok();
    let can_retire = key.is_some() && retention_enabled(keep, expire);
    let mut seqs = Vec::new();
    for (index, (seq, became, created, blob)) in history.iter().rev().enumerate() {
        if can_retire && retires(index, *became, *created, keep, expire, now) {
            seqs.push(*seq);
            continue;
        }
        for block in blob.blocks() {
            scan.hashes.insert(block.hash);
        }
    }
    if let Some(key) = key
        && !seqs.is_empty()
    {
        scan.retire.push((key, seqs));
    }
}

impl Sweeper {
    /// Construct the sweeper.
    pub fn new(storage: FsStorage, options: Options) -> Self {
        Self { storage, options }
    }

    /// Run the sweep loop until `shutdown` turns true. One sweep pass runs
    /// every hour (fixed cadence); the sleep is bounded by 1 s chunks so
    /// shutdown stays prompt.
    pub async fn run(self, shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            match self.sweep_once(SystemTime::now()).await {
                Ok(summary) => {
                    tracing::debug!(
                        temps = summary.temp_files,
                        uploads = summary.uploads,
                        expired_versions = summary.expired_versions,
                        reclaimed_blocks = summary.reclaimed_blocks,
                        reclaimed_bytes = summary.reclaimed_bytes,
                        reclaimed_subtrees = summary.reclaimed_subtrees,
                        block_bytes = summary.block_bytes,
                        "sweep pass complete"
                    );
                    // The metrics carrier for what the pass just did: the
                    // reclamation counters add this pass's numbers, and the
                    // noncurrent gauge is set to what the retention view
                    // holds now. Both are additive families declared in this
                    // crate (`crate::metrics`) — the layer that observes the
                    // numbers owns them, and the process-global prometheus
                    // registry carries them to `/metrics`.
                    #[cfg(feature = "versioning")]
                    {
                        record_sweep(&summary);
                        record_noncurrent_versions(summary.noncurrent_versions);
                        record_block_bytes(summary.block_bytes);
                        if self.options.block_bytes_warn > 0
                            && summary.block_bytes > self.options.block_bytes_warn
                        {
                            tracing::warn!(
                                block_bytes = summary.block_bytes,
                                warn_over = self.options.block_bytes_warn,
                                "the block store is above its configured warning threshold"
                            );
                        }
                    }
                }
                Err(err) => tracing::warn!(error = %err, "sweep pass failed"),
            }
            // Sleep the interval in bounded chunks, re-checking shutdown.
            pacing::sleep_checked(SWEEP_INTERVAL, Duration::from_secs(1), &shutdown).await;
        }
    }

    /// One sweep pass against a synthetic clock (`now`): removes temp
    /// files older than the TTL, multipart uploads idle longer than the
    /// TTL, and — per versioned bucket — the noncurrent versions past the
    /// retention thresholds, the blocks their expiry left unreferenced
    /// (plus any older than the grace), the bucket's own stale staging
    /// residue, and the block subtrees of buckets that no longer exist.
    /// Returns the counts.
    ///
    /// **One owner per bucket per action (LC21).** The pass reads the stored
    /// lifecycle rules once, at the top: a bucket whose configuration carries
    /// `NoncurrentVersionExpiration` is governed by its rule for that action, so
    /// `[s3] noncurrent_keep` / `noncurrent_expire_days` do not apply to it, and
    /// `AbortIncompleteMultipartUpload` likewise displaces
    /// `[s3] multipart_expire_days`. A bucket with neither rule keeps the
    /// pre-lifecycle behaviour exactly — and so does every bucket when this run
    /// builds no lifecycle pass (LC23): turning the capability off never stops the
    /// knobs' retention.
    ///
    /// Every pass ends with one low-frequency fragmentation evaluation
    /// (the stats call takes the write lock): over the `[storage.fs]
    /// compact_threshold_percent` threshold the `compact_needed` marker is
    /// set, and the next startup compacts (meta-redb-spec §5.9). Best
    /// effort — an evaluation failure is logged, never a sweep failure.
    pub async fn sweep_once(&self, now: SystemTime) -> Result<Summary, Error> {
        let mut summary = Summary::default();
        // The sections run independently: one failing section must not
        // cancel the others' work (one unreadable subtree would otherwise
        // skip the whole rest of the pass). The first error is reported
        // after every section ran.
        let mut first_error: Option<Error> = None;
        let governed = match self.governed_actions().await {
            Ok(governed) => governed,
            Err(err) => {
                // A pass that cannot see the rules stands every bucket down
                // rather than judge a governed bucket by the knob its rule
                // displaced — unless this run builds no pass at all, whose
                // knobs a rule was never going to displace (LC23).
                first_error = first_error.or(Some(err));
                lifecycle::ActionOwners::unread(self.options.lifecycle_pass)
            }
        };
        if let Err(err) = self.sweep_versioned(&mut summary, now, &governed).await {
            first_error = first_error.or(Some(err));
        }
        if let Err(err) = self.sweep_orphan_blobs(&mut summary).await {
            first_error = first_error.or(Some(err));
        }
        match self.sweep_tmp(now).await {
            Ok(temps) => summary.temp_files += temps,
            Err(err) => first_error = first_error.or(Some(err)),
        }
        match self.sweep_multipart(now, &governed).await {
            Ok(uploads) => summary.uploads = uploads,
            Err(err) => first_error = first_error.or(Some(err)),
        }
        self.evaluate_compact_needed().await;
        match first_error {
            Some(err) => Err(err),
            None => Ok(summary),
        }
    }

    /// The buckets whose stored lifecycle configuration governs one of the
    /// pass's two actions (LC21): one scan of the rules table, empty on every
    /// root that never configured a rule, and empty for every bucket in a run
    /// that builds no lifecycle pass (LC23).
    async fn governed_actions(&self) -> Result<lifecycle::ActionOwners, Error> {
        // A run that builds no pass governs nothing, so the scan is skipped.
        if !self.options.lifecycle_pass {
            return Ok(lifecycle::ActionOwners::from_configs(&[], false));
        }
        let configs = storage::LifecycleRules::lifecycles(&self.storage)
            .await
            .map_err(Error::from)?;
        Ok(lifecycle::ActionOwners::from_configs(&configs, true))
    }

    /// Reclaim the block subtrees of buckets that are gone. `remove_bucket_state`
    /// drains a bucket's version rows in one transaction and only then removes
    /// its block subtree — the blocks sit under the state dir, which may be on
    /// another volume, so they cannot ride the tombstone rename — and a crash
    /// between the two leaves blocks under a bucket name that no longer exists.
    /// No other pass sees them: the per-bucket walk above and `versioned_buckets`
    /// both iterate *live* buckets. Left alone, those bytes are a leak
    /// proportional to the deleted bucket's history.
    ///
    /// The proof itself — the absence of the bucket directory, the lock it
    /// is taken under, and the removal in the same critical section — is
    /// [`FsStorage::orphaned_block_subtree`]'s, shared with the doctor's
    /// residue check so neither can name a subtree the other would keep.
    #[cfg(feature = "versioning")]
    async fn sweep_orphan_blobs(&self, summary: &mut Summary) -> Result<(), Error> {
        for (path, name) in entries_of(&self.storage.blobs_dir()).await? {
            if self
                .storage
                .orphaned_block_subtree(&path, &name, true)
                .await?
            {
                summary.reclaimed_subtrees += 1;
            }
        }
        Ok(())
    }

    /// A build without the version machinery has no block store: no subtree to
    /// reclaim.
    #[cfg(not(feature = "versioning"))]
    async fn sweep_orphan_blobs(&self, _summary: &mut Summary) -> Result<(), Error> {
        Ok(())
    }

    /// The versioned half of one pass: one pass per bucket whose state is
    /// enabled or suspended. One bucket's failure is logged and the other
    /// buckets still run; the first error is reported after the loop.
    #[cfg(feature = "versioning")]
    async fn sweep_versioned(
        &self,
        summary: &mut Summary,
        now: SystemTime,
        governed: &lifecycle::ActionOwners,
    ) -> Result<(), Error> {
        let mut first_error: Option<Error> = None;
        for bucket in self.storage.versioned_buckets().await? {
            if let Err(err) = self.sweep_bucket(&bucket, summary, now, governed).await {
                tracing::warn!(
                    bucket = %bucket,
                    error = %err,
                    "the versioned sweep of one bucket failed; the other buckets still run"
                );
                first_error = first_error.or(Some(err));
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// One versioned bucket's pass, in three phases against the bucket's
    /// mutation lock.
    ///
    /// **Phase 1 (under the bucket mutation lock): the plan.** One read
    /// transaction decodes every row once — the live set the block walk
    /// judges against, the noncurrent count the gauge reports, and the
    /// retirement decisions the retention keys imply — and the retirements
    /// are applied before the lock is released, so nothing moves a slot
    /// between a decision and its removal.
    ///
    /// **Phase 2 (lock-free): the walk.** The whole block store is scanned
    /// and the dead, past-grace candidates collected — the seconds-long
    /// filesystem work of the pass, and the reason it no longer runs under
    /// the lock. Nothing is removed yet.
    ///
    /// **Phase 3 (locked again): the removal.** The version-reference
    /// epoch read at the end of phase 1 is re-checked under the bucket
    /// mutation lock, and the candidates are removed only when it is
    /// unchanged. The lock is what makes the check total: every
    /// reference-adding path commits inside its own hold of the same lock,
    /// so once this phase holds it, no publisher is in flight — an equal
    /// epoch means none committed since the plan was read, and every
    /// removal is safe. An unequal epoch (a publish landed during the walk
    /// — the dedup-hit race the grace cannot cover, because a dedup hit
    /// leaves the block's timestamp untouched) defers the whole candidate
    /// set to the next pass: a block has no reverse index to re-check
    /// individually.
    #[cfg(feature = "versioning")]
    async fn sweep_bucket(
        &self,
        bucket: &Name,
        summary: &mut Summary,
        now: SystemTime,
        governed: &lifecycle::ActionOwners,
    ) -> Result<(), Error> {
        let (plan, expired, epoch) = self.plan_bucket(bucket, now, governed).await?;
        summary.expired_versions += expired;
        summary.noncurrent_versions += plan.noncurrent.saturating_sub(expired as u64);
        let candidates = self
            .storage
            .versions(bucket)
            .blobs()
            .sweep_candidates(&plan.live, self.options.block_grace, now)
            .await
            .map_err(projected)?;
        // The walk measured every block's length on the way: the size gauge
        // reads the store's total off the pass that already stat'ed it.
        summary.block_bytes += candidates.bytes_total;
        if !candidates.entries.is_empty() {
            let report = self.apply_bucket(bucket, &candidates, epoch).await?;
            summary.reclaimed_blocks += report.removed;
            summary.reclaimed_bytes += report.bytes;
            if report.failed > 0 {
                tracing::warn!(
                    bucket = %bucket,
                    failed = report.failed,
                    "some dead blocks could not be removed; the next sweep retries"
                );
            }
        }
        summary.temp_files += self
            .sweep_block_staging(
                &self.storage.versions(bucket).blobs().blocks().temp_dir(),
                now,
            )
            .await?;
        Ok(())
    }

    /// Phase 1 of [`Self::sweep_bucket`]: the retention plan and its
    /// retirements under the bucket's mutation lock, and the
    /// version-reference epoch read before the lock is released — the
    /// token [`Self::apply_bucket`] validates. A method of its own so the
    /// tests can interleave a publish between the phases.
    #[cfg(feature = "versioning")]
    async fn plan_bucket(
        &self,
        bucket: &Name,
        now: SystemTime,
        governed: &lifecycle::ActionOwners,
    ) -> Result<(BucketPlan, usize, u64), Error> {
        let guard = self.storage.lock_bucket_mutations(bucket).await;
        let plan = self.plan_retention(bucket, now, governed).await?;
        let expired = self.apply_retirement(bucket, &plan.retire).await?;
        // Read last, still under the lock: every reference committed before
        // this point is inside `plan.live`, and every one after it bumps the
        // counter — during the walk, or while waiting on the lock this
        // phase is about to re-take.
        let epoch = self.storage.block_epoch();
        drop(guard);
        Ok((plan, expired, epoch))
    }

    /// Phase 3 of [`Self::sweep_bucket`]: remove the walk's candidates
    /// after validating the version-reference epoch under the bucket's
    /// mutation lock.
    #[cfg(feature = "versioning")]
    async fn apply_bucket(
        &self,
        bucket: &Name,
        candidates: &SweepCandidates,
        epoch: u64,
    ) -> Result<SweepReport, Error> {
        let guard = self.storage.lock_bucket_mutations(bucket).await;
        if self.storage.block_epoch() != epoch {
            tracing::warn!(
                bucket = %bucket,
                "a version reference was committed during the sweep; block removals are deferred to the next pass"
            );
            return Ok(SweepReport {
                scanned: candidates.scanned,
                kept_live: candidates.kept_live,
                kept_young: candidates.kept_young,
                ..Default::default()
            });
        }
        let report = self
            .storage
            .versions(bucket)
            .blobs()
            .remove_candidates(candidates)
            .await
            .map_err(projected)?;
        drop(guard);
        Ok(report)
    }

    /// A build without the version machinery has no version rows and no
    /// block store: the versioned half is empty.
    #[cfg(not(feature = "versioning"))]
    async fn sweep_versioned(
        &self,
        _summary: &mut Summary,
        _now: SystemTime,
        _governed: &lifecycle::ActionOwners,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// One read transaction answers the whole retention view of `bucket`:
    /// the live set the block walk judges against, the noncurrent count the
    /// gauge reports, and the retirement decisions the retention keys imply.
    /// Every row is decoded once, in the pass that needs all three — the
    /// shape this replaced decoded the whole table twice, on every pass,
    /// even with both retention keys unset.
    ///
    /// A row that is not history never retires, which the walk decides per
    /// key with the slot the bucket's `object_meta` rows record
    /// (`Row::is_history`): the row at the recorded sequence is the current
    /// version's own while the slot still holds it — the supersede crash
    /// window's adopted row, which retention must not retire — and a row at
    /// any other sequence, or one at a key with no recorded slot at all, is
    /// history whatever its stamp says (the residue of an aborted publish or
    /// an out-of-band removal). Every row's blocks feed the live set whether
    /// it is history or not: the row references them until it is removed.
    /// The slots and the rows come from the same transaction, so the walk
    /// never judges a row against a slot view a concurrent write has already
    /// moved. The table's own order is `(bucket, key, seq)` ascending, so
    /// one key's rows arrive contiguously and oldest first; a key whose
    /// stored string is not a valid object key cannot be expired (`expire`
    /// addresses a key), so its rows feed the live set but retire nothing.
    #[cfg(feature = "versioning")]
    async fn plan_retention(
        &self,
        bucket: &Name,
        now: SystemTime,
        governed: &lifecycle::ActionOwners,
    ) -> Result<BucketPlan, Error> {
        let handle = self.storage.handle().clone();
        let scanned = bucket.clone();
        let (keep, expire) = if !governed.knob_applies_noncurrent(bucket) {
            // LC21: the bucket's own rule owns this action, so the `[s3]`
            // thresholds are not this pass's to apply to it. Both zero is the
            // safe reading ("retire nothing"), the walk below unchanged.
            (0, Duration::ZERO)
        } else {
            (
                self.options.noncurrent_keep,
                Duration::from_secs(self.options.noncurrent_expire_days.saturating_mul(86_400)),
            )
        };
        let scan = handle
            .read_blocking(move |txn| {
                // The key's current slots, keyed the same way the rows below
                // are. A key with no recorded row (`None` — no slot, or one
                // whose etag is domain-invalid, which the readers treat as
                // absent too) has no current version: every one of its rows
                // is history.
                let mut slots: BTreeMap<String, rows::Slot> = BTreeMap::new();
                object_meta::Table::open_readonly(txn)?.for_bucket_gated(
                    scanned.as_ref(),
                    |key, stored| {
                        slots.insert(
                            key.as_ref().to_owned(),
                            stored.map_or(rows::Slot::Gone, |stored| {
                                rows::Slot::At(stored.version_seq)
                            }),
                        );
                        Ok(())
                    },
                )?;
                let table = rows::Table::open_readonly(txn)?;
                let mut scan = RetentionScan {
                    hashes: HashSet::new(),
                    noncurrent: 0,
                    retire: Vec::new(),
                };
                // The history rows of the key being walked, oldest first —
                // the table's own order — buffered until the key changes,
                // because the retirement decision reads the history
                // newest-first and the live set must exclude whatever the
                // keys retire (so the same pass's walk reclaims it).
                let mut current: HistoryBuffer = None;
                table.for_bucket_of(scanned.as_ref(), |key, row| {
                    let blob = row.blob();
                    let slot = slots.get(key).copied().unwrap_or(rows::Slot::Gone);
                    if !row.is_history(slot) {
                        // A row the walk does not consider history — the
                        // current slot's own row, the crash-window adopted
                        // row included — is live whatever the retention keys
                        // say.
                        for block in blob.blocks() {
                            scan.hashes.insert(block.hash);
                        }
                        return Ok(());
                    }
                    scan.noncurrent += 1;
                    let same_key = current
                        .as_ref()
                        .is_some_and(|(name, _)| name.as_str() == key);
                    if !same_key {
                        flush_retirement(current.take(), keep, expire, now, &mut scan);
                        current = Some((key.to_owned(), Vec::new()));
                    }
                    if let Some((_, history)) = current.as_mut() {
                        history.push((row.seq, row.became_noncurrent, row.created, blob));
                    }
                    Ok(())
                })?;
                flush_retirement(current.take(), keep, expire, now, &mut scan);
                Ok(scan)
            })
            .await
            .map_err(Error::from)?;
        Ok(BucketPlan {
            live: LiveSet::from_hashes(scan.hashes),
            noncurrent: scan.noncurrent,
            retire: scan.retire,
        })
    }

    /// Apply one plan's retirements — one port call per key, so a sweep
    /// over a long history pays one durable commit per key rather than one
    /// per version.
    #[cfg(feature = "versioning")]
    async fn apply_retirement(&self, bucket: &Name, retire: &[Retirement]) -> Result<usize, Error> {
        let versions = self.storage.versions(bucket);
        let mut expired = 0;
        for (key, seqs) in retire {
            versions
                .expire_many(bucket, key, seqs)
                .await
                .map_err(projected)?;
            expired += seqs.len();
        }
        Ok(expired)
    }

    /// Reap the staged block temps of one bucket's block store older than
    /// the grace — a publish interrupted by a crash leaves its stage
    /// behind, and no reader ever sees it (`tmp` is outside the address
    /// fan-out). The store's own commit removes a stage that completes, so
    /// only residue reaches this.
    #[cfg(feature = "versioning")]
    async fn sweep_block_staging(&self, tmp: &Path, now: SystemTime) -> Result<usize, Error> {
        sweep_stale_files(tmp, self.options.block_grace, "block staging", now).await
    }

    /// Evaluate fragmentation and set the compact-needed marker when the
    /// threshold is reached (best effort; one write transaction).
    async fn evaluate_compact_needed(&self) {
        if let Err(err) = self
            .storage
            .evaluate_compact(self.storage.compact_threshold_percent())
            .await
        {
            tracing::warn!(error = %err, "compact evaluation failed");
        }
    }

    async fn sweep_tmp(&self, now: SystemTime) -> Result<usize, Error> {
        let dir = self.storage.state_dir().join(TMP_DIR_NAME);
        sweep_stale_files(&dir, self.options.temp_ttl, "temp", now).await
    }

    async fn sweep_multipart(
        &self,
        now: SystemTime,
        governed: &lifecycle::ActionOwners,
    ) -> Result<usize, Error> {
        let uploads = self.storage.multipart_store().walk_uploads().await?;
        let mut removed = 0;
        for upload in uploads {
            if !governed.knob_applies_abort(&upload.bucket) {
                continue; // LC21: the bucket's own rule owns the abort
            }
            let idle_since = self
                .storage
                .multipart_store()
                .idle_since(&upload.bucket, &upload.upload_id)
                .await?;
            let idle = idle_since.max(upload.initiated_at);
            if now
                .duration_since(idle)
                .map(|age| age >= self.options.multipart_ttl())
                .unwrap_or(false)
            {
                tracing::info!(
                    upload_id = %upload.upload_id,
                    "swept abandoned multipart upload"
                );
                match self
                    .storage
                    .multipart_store()
                    .abort(&upload.bucket, &upload.key, &upload.upload_id)
                    .await
                {
                    Ok(()) => removed += 1,
                    // A concurrent complete/abort won the race — nothing to do.
                    Err(err) => tracing::warn!(error = %err, "sweep could not abort upload"),
                }
            }
        }
        Ok(removed)
    }
}

/// Result of one sweep pass (for logs and tests).
///
/// # Examples
///
/// ```rust
/// use tinio_fs::sweep::Summary;
///
/// let summary = Summary {
///     temp_files: 1,
///     uploads: 0,
///     expired_versions: 2,
///     reclaimed_blocks: 2,
///     reclaimed_bytes: 8,
///     reclaimed_subtrees: 0,
///     noncurrent_versions: 3,
///     block_bytes: 16,
/// };
/// assert_eq!(summary.temp_files, 1);
/// assert_eq!(summary.reclaimed_bytes, 8);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    /// Stale temp files removed — the state dir's `tmp/` plus each
    /// bucket's block-store `tmp/`.
    pub temp_files: usize,
    /// Abandoned multipart uploads removed.
    pub uploads: usize,
    /// Noncurrent versions expired by the retention thresholds.
    pub expired_versions: usize,
    /// Blocks reclaimed by the block-store pass.
    pub reclaimed_blocks: u64,
    /// Bytes reclaimed by the block-store pass.
    pub reclaimed_bytes: u64,
    /// Block subtrees of vanished buckets reclaimed whole (the crash window
    /// between a bucket's row drain and its subtree removal).
    pub reclaimed_subtrees: usize,
    /// The noncurrent versions the pass's retention walk saw across the
    /// versioned buckets, after the expiry — the gauge's number, derived in
    /// the pass that already decoded the rows (`0` in a build without the
    /// versioning feature, which walks no version table).
    pub noncurrent_versions: u64,
    /// The total bytes the versioned buckets' block stores held when the
    /// pass walked them — the size gauge's number, read off the walk that
    /// already stat'ed every block (`0` in a build without the versioning
    /// feature, which walks no block store).
    pub block_bytes: u64,
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "versioning")]
    use std::cmp::Reverse;
    use std::fs::{self, FileTimes, OpenOptions};

    use tokio::time;

    use super::*;
    #[cfg(feature = "versioning")]
    use crate::_core::blob::BlockHash;
    use crate::{
        _core::{
            acl::Acl,
            bucket, lifecycle,
            meta::BucketMeta,
            object,
            storage::{BucketOps, MultipartOps},
        },
        _store_redb::RedbMetadata,
        _util::testing::body,
        FsOptions,
        testutil::fs_options,
    };
    #[cfg(feature = "versioning")]
    use crate::{
        _core::{blob::BlockStore, collect_body, to_nanos},
        database::Error as DatabaseError,
        testutil::{wait_for_lock_waiter, wait_for_tmp},
    };

    fn old_ttl_options() -> Options {
        Options {
            temp_ttl: Duration::from_secs(60),
            multipart_ttl: Some(Duration::from_secs(60)),
            ..Options::default()
        }
    }

    /// The key's version rows, newest first — the retention assertions'
    /// view (the same order the sweep walks).
    #[cfg(feature = "versioning")]
    fn history(storage: &FsStorage, bucket: &bucket::Name, key: &object::Key) -> Vec<rows::Row> {
        let storage = storage.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        storage
            .handle()
            .read(move |txn| -> Result<Vec<rows::Row>, DatabaseError> {
                let table = rows::Table::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_key(&bucket, &key, |row| {
                    out.push(row);
                    Ok(())
                })?;
                Ok(out)
            })
            .unwrap()
    }

    /// A versioned bucket holding `notes.txt` written three times: `one`
    /// and `two` are noncurrent (in that order), `three` is current.
    #[cfg(feature = "versioning")]
    async fn versioned_fixture() -> (tempfile::TempDir, FsStorage, bucket::Name, object::Key) {
        use crate::{_core::version::State, testutil};

        let (root, storage) = testutil::storage();
        let (b, key) =
            testutil::versioned_bucket(&storage, State::Enabled, &[b"one", b"two", b"three"]).await;
        (root, storage, b, key)
    }

    /// A versioned bucket holding one current `notes.txt` and nothing else:
    /// the lock-ordering test's starting point (no history row yet).
    #[cfg(feature = "versioning")]
    async fn versioned_single_fixture() -> (tempfile::TempDir, FsStorage, bucket::Name, object::Key)
    {
        use crate::{_core::version::State, testutil};

        let (root, storage) = testutil::storage();
        let (b, key) = testutil::versioned_bucket(&storage, State::Enabled, &[b"hello"]).await;
        (root, storage, b, key)
    }

    /// Backdate one history row's `became_noncurrent` stamp by `age` — the
    /// day-threshold tests' way to age a version (the sweep reads the stored
    /// stamp, and the fixture versions were all written just now).
    #[cfg(feature = "versioning")]
    async fn backdate_noncurrent(
        storage: &FsStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
        age: Duration,
    ) {
        let storage = storage.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let stamp = to_nanos(SystemTime::now() - age);
        storage
            .handle()
            .write(move |txn| -> Result<(), DatabaseError> {
                let mut table = rows::Table::open(txn)?;
                let mut row = table
                    .get(&bucket, &key, seq)?
                    .expect("the history row exists");
                row.became_noncurrent = stamp;
                table.put(&bucket, &key, &row)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// The key's history rows, newest first (the retention assertions' own
    /// sort — the table walk is sequence-ascending).
    #[cfg(feature = "versioning")]
    fn history_newest_first(
        storage: &FsStorage,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Vec<rows::Row> {
        let mut rows = history(storage, bucket, key);
        rows.sort_by_key(|row| Reverse(row.seq));
        rows
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_sweep_expires_noncurrent_versions_beyond_the_keep_budget() {
        let (_root, storage, b, key) = versioned_fixture().await;
        let versions = storage.versions(&b);
        assert_eq!(
            history(&storage, &b, &key).len(),
            2,
            "two versions moved to history"
        );

        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 1,
                ..Options::default()
            },
        );
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.expired_versions, 1,
            "the budget keeps the newest one"
        );
        let left = history(&storage, &b, &key);
        assert_eq!(left.len(), 1, "one noncurrent version survives");
        assert_eq!(left[0].seq, 2, "the newest noncurrent version");
        let bytes = collect_body(versions.blobs().open(&left[0].blob(), None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(bytes, b"two", "the surviving version's content still reads");
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_sweep_reclaims_the_blocks_of_expired_versions_only() {
        let (_root, storage, b, _key) = versioned_fixture().await;
        let versions = storage.versions(&b);
        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 1,
                ..Options::default()
            },
        );

        // One pass past the grace: the retention drop and the reclamation
        // of the blocks it orphaned happen in the same pass.
        let past_grace = SystemTime::now() + Duration::from_secs(601);
        let summary = sweeper.sweep_once(past_grace).await.unwrap();
        assert_eq!(summary.expired_versions, 1);
        assert_eq!(summary.reclaimed_blocks, 1);
        assert_eq!(summary.reclaimed_bytes, 3);
        assert!(
            versions
                .blobs()
                .blocks()
                .contains(&BlockHash::of(b"two"))
                .await
                .unwrap(),
            "the surviving version's block is kept"
        );
        assert!(
            !versions
                .blobs()
                .blocks()
                .contains(&BlockHash::of(b"one"))
                .await
                .unwrap(),
            "the expired version's block is reclaimed"
        );

        // A block published inside the grace window survives a sweep whose
        // live set cannot know it yet (Review Focus 5's narrow half) — the
        // grace is what covers a first publish still in flight.
        let fresh = versions.blobs().put(body(b"fresh")).await.unwrap();
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(
            summary.reclaimed_blocks, 0,
            "a young block is never reclaimed"
        );
        assert!(
            versions
                .blobs()
                .blocks()
                .contains(fresh.hash())
                .await
                .unwrap()
        );
    }

    /// The crash window `remove_bucket_state` leaves when it is interrupted
    /// between the version-row drain and the subtree removal: blocks under a
    /// bucket name whose directory is gone. The orphan pass reclaims them —
    /// proving absence under the bucket's own mutation lock — while a live
    /// bucket's subtree is left alone even when its `BUCKETS` row is lost
    /// (the directory is the authority `ensure_bucket` reads, and the row is
    /// re-derived from it).
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_sweep_reclaims_the_block_subtree_of_a_vanished_bucket() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        // A subtree whose bucket directory does not exist: the crash window.
        let orphan = state.path().join("blobs/gone-bucket");
        fs::create_dir_all(orphan.join("blocks")).unwrap();
        fs::write(orphan.join("blocks/aaaa"), b"block").unwrap();
        // A live bucket's subtree — the directory exists, and the block pass
        // owns its contents.
        let live = bucket::name("live-bucket").unwrap();
        storage
            .create_bucket(&live, None, &Acl::default_private(None))
            .await
            .unwrap();
        let kept = state.path().join("blobs/live-bucket");
        fs::create_dir_all(kept.join("blocks")).unwrap();

        let sweeper = Sweeper::new(storage.clone(), Options::default());
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.reclaimed_subtrees, 1,
            "exactly the vanished bucket's subtree is reclaimed"
        );
        assert!(
            !orphan.exists(),
            "the vanished bucket's blocks are reclaimed by the pass"
        );
        assert!(
            kept.exists(),
            "a live bucket's block subtree is never residue"
        );
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_sweep_expires_only_noncurrent_versions_past_the_day_threshold() {
        // The day threshold alone (`noncurrent_keep` unset): a version that
        // became noncurrent before the window expires, one inside it does
        // not. The fixture's versions were all written just now, so the
        // older one is stamped two days back by hand.
        let (_root, storage, b, key) = versioned_fixture().await;
        backdate_noncurrent(&storage, &b, &key, 1, Duration::from_secs(2 * 86_400)).await;

        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_expire_days: 1,
                ..Options::default()
            },
        );
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.expired_versions, 1,
            "only the version past the day threshold expires"
        );
        let left = history(&storage, &b, &key);
        assert_eq!(left.len(), 1, "one noncurrent version survives");
        assert_eq!(left[0].seq, 2, "the one inside the window");
        let bytes = collect_body(
            storage
                .versions(&b)
                .blobs()
                .open(&left[0].blob(), None)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, b"two", "its content still reads");
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn the_keep_budget_expires_what_the_day_threshold_would_spare() {
        // Both thresholds set: whichever is met first expires. Here the
        // budget (one kept) is met and the day window (30 days) is not —
        // the expired version is minutes old.
        let (_root, storage, b, key) = versioned_fixture().await;
        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 1,
                noncurrent_expire_days: 30,
                ..Options::default()
            },
        );
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.expired_versions, 1,
            "the keep budget expires one of two noncurrent versions"
        );
        let left = history(&storage, &b, &key);
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].seq, 2, "the newest noncurrent version is kept");
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn the_day_threshold_expires_what_the_keep_budget_would_spare() {
        // Both thresholds set, the other way round: the budget (five kept)
        // is not met, the day window (one day) is — the aged version
        // expires while the fresh one stays.
        let (_root, storage, b, key) = versioned_fixture().await;
        backdate_noncurrent(&storage, &b, &key, 1, Duration::from_secs(2 * 86_400)).await;
        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 5,
                noncurrent_expire_days: 1,
                ..Options::default()
            },
        );
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.expired_versions, 1,
            "the day threshold expires the aged version"
        );
        let left = history(&storage, &b, &key);
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].seq, 2, "the version inside the window is kept");
    }

    /// A one-rule lifecycle configuration carrying exactly the actions
    /// `configure` sets — how the LC21 legs give a bucket a stored rule.
    fn lifecycle_config(
        status: lifecycle::Status,
        configure: impl FnOnce(&mut lifecycle::Rule),
    ) -> lifecycle::Config {
        let mut rule = lifecycle::Rule {
            id: None,
            status,
            filter: lifecycle::Filter::default(),
            expiration: None,
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: None,
        };
        configure(&mut rule);
        lifecycle::Config { rules: vec![rule] }
    }

    /// Store `config` as `bucket`'s lifecycle configuration — the row a pass
    /// reads back once.
    async fn store_lifecycle(
        storage: &FsStorage,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
    ) {
        let engine = storage.meta_store().engine();
        <RedbMetadata as BucketMeta>::set_lifecycle(&engine, bucket, config)
            .await
            .unwrap();
    }

    /// The `NoncurrentVersionExpiration` action, at a threshold the server's
    /// knob would beat.
    #[cfg(feature = "versioning")]
    fn noncurrent_rule(rule: &mut lifecycle::Rule) {
        rule.noncurrent_version_expiration = Some(lifecycle::NoncurrentVersionExpiration {
            noncurrent_days: Some(30),
            newer_noncurrent_versions: None,
        });
    }

    /// The `AbortIncompleteMultipartUpload` action, at a threshold the
    /// server's knob would beat.
    fn abort_rule(rule: &mut lifecycle::Rule) {
        rule.abort_incomplete_multipart_upload = Some(lifecycle::AbortIncompleteMultipartUpload {
            days_after_initiation: 7,
        });
    }

    /// LC21: a bucket whose stored configuration carries
    /// `NoncurrentVersionExpiration` is governed by its rule for that action,
    /// so `[s3] noncurrent_keep` does not apply to it — a knob that retires a
    /// version on every other bucket retires none here.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_bucket_with_a_noncurrent_rule_keeps_its_versions_past_the_server_knob() {
        let (_root, storage, b, key) = versioned_fixture().await;
        assert_eq!(
            history(&storage, &b, &key).len(),
            2,
            "two noncurrent versions to retire"
        );
        store_lifecycle(
            &storage,
            &b,
            &lifecycle_config(lifecycle::Status::Enabled, noncurrent_rule),
        )
        .await;

        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 1,
                ..Options::default()
            },
        );
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.expired_versions, 0,
            "the bucket's own rule owns noncurrent retirement"
        );
        assert_eq!(
            history(&storage, &b, &key).len(),
            2,
            "both history rows survive the server's keep budget"
        );
        assert_eq!(
            summary.reclaimed_blocks, 0,
            "both versions' blocks stay referenced"
        );
    }

    /// A bucket holding one in-progress upload and an `Enabled` rule carrying
    /// `AbortIncompleteMultipartUpload` — the abort action's two legs' fixture, which
    /// differ only in whether the run builds the lifecycle pass.
    async fn bucket_with_an_abort_rule_and_an_idle_upload()
    -> (tempfile::TempDir, FsStorage, bucket::Name) {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .create_multipart_upload(
                &b,
                &"big.bin".into(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store_lifecycle(
            &storage,
            &b,
            &lifecycle_config(lifecycle::Status::Enabled, abort_rule),
        )
        .await;
        (root, storage, b)
    }

    /// LC21: `AbortIncompleteMultipartUpload` in a bucket's configuration takes
    /// the abandoned-upload action away from `[s3] multipart_expire_days`.
    #[tokio::test]
    async fn a_bucket_with_an_abort_rule_is_not_swept_by_the_server_knob() {
        let (_root, storage, b) = bucket_with_an_abort_rule_and_an_idle_upload().await;

        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        // The server TTL is a minute and this clock is an hour on.
        let far = SystemTime::now() + Duration::from_secs(3600);
        let summary = sweeper.sweep_once(far).await.unwrap();

        assert_eq!(
            summary.uploads, 0,
            "the bucket's own rule owns the abort for it"
        );
        assert_eq!(
            storage
                .multipart_store()
                .list_uploads(&b)
                .await
                .unwrap()
                .len(),
            1,
            "the upload survives the server's TTL"
        );
    }

    /// LC23: with the capability off nothing is governed, so the same bucket is swept
    /// by `[s3] multipart_expire_days` like every other bucket — turning the pass off
    /// must not stop retention.
    #[tokio::test]
    async fn a_bucket_with_an_abort_rule_is_swept_by_the_server_knob_when_the_pass_is_off() {
        let (_root, storage, b) = bucket_with_an_abort_rule_and_an_idle_upload().await;

        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                lifecycle_pass: false,
                ..old_ttl_options()
            },
        );
        // The server TTL is a minute and this clock is an hour on.
        let far = SystemTime::now() + Duration::from_secs(3600);
        let summary = sweeper.sweep_once(far).await.unwrap();

        assert_eq!(
            summary.uploads, 1,
            "no pass, so no rule takes the abort away from the knob"
        );
        assert!(
            storage
                .multipart_store()
                .list_uploads(&b)
                .await
                .unwrap()
                .is_empty(),
            "the idle upload is aborted by the server's TTL"
        );
    }

    /// LC21 is **per action, never per bucket**: a bucket carrying only an
    /// `Expiration` keeps the server's noncurrent behaviour.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_bucket_with_an_expiration_rule_only_keeps_the_server_noncurrent_behaviour() {
        let (_root, storage, b, key) = versioned_fixture().await;
        store_lifecycle(
            &storage,
            &b,
            &lifecycle_config(lifecycle::Status::Enabled, |rule| {
                rule.expiration = Some(lifecycle::Expiration {
                    days: Some(1),
                    date: None,
                    expired_object_delete_marker: None,
                });
            }),
        )
        .await;

        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 1,
                ..Options::default()
            },
        );
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.expired_versions, 1,
            "an Expiration rule does not take the noncurrent action away"
        );
        assert_eq!(
            history(&storage, &b, &key).len(),
            1,
            "the server's keep budget still retires"
        );
    }

    /// A `Disabled` rule is stored and skipped, so it never stands the sweeper
    /// down: only an `Enabled` rule carrying the action does (LC21).
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_disabled_rule_does_not_stand_the_sweeper_down() {
        let (_root, storage, b, key) = versioned_fixture().await;
        storage
            .create_multipart_upload(
                &b,
                &"big.bin".into(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        store_lifecycle(
            &storage,
            &b,
            &lifecycle_config(lifecycle::Status::Disabled, |rule| {
                noncurrent_rule(rule);
                abort_rule(rule);
            }),
        )
        .await;

        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 1,
                ..old_ttl_options()
            },
        );
        let far = SystemTime::now() + Duration::from_secs(3600);
        let summary = sweeper.sweep_once(far).await.unwrap();

        assert_eq!(summary.expired_versions, 1, "the knob still retires");
        assert_eq!(
            history(&storage, &b, &key).len(),
            1,
            "the disabled rule took nothing away"
        );
        assert_eq!(summary.uploads, 1, "the knob still aborts");
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_versioned_bucket_with_neither_threshold_retires_nothing() {
        // Both keys unset is the safe reading of an unconfigured retention:
        // the pass is a no-op — no row expires and no block is reclaimed,
        // however old the sweep's clock.
        let (_root, storage, b, key) = versioned_fixture().await;
        let sweeper = Sweeper::new(storage.clone(), Options::default());
        let far = SystemTime::now() + Duration::from_secs(30 * 86_400);
        let summary = sweeper.sweep_once(far).await.unwrap();

        assert_eq!(summary.expired_versions, 0, "nothing expires by age");
        assert_eq!(summary.reclaimed_blocks, 0, "both blocks stay referenced");
        let left = history_newest_first(&storage, &b, &key);
        assert_eq!(left.len(), 2, "both history rows survive");
        for (row, bytes) in left.iter().zip([b"two".as_slice(), b"one"]) {
            let read = collect_body(
                storage
                    .versions(&b)
                    .blobs()
                    .open(&row.blob(), None)
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(
                &read[..],
                bytes,
                "the surviving version's content still reads"
            );
        }
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_delete_marker_consumes_a_keep_slot() {
        // A marker superseded by a fresh write moves into history as a row
        // like any other version, so the retention walk counts it against
        // the keep budget — the design's "a delete marker counts like any
        // version". Here the marker keeps the single slot and the object
        // versions behind it expire.
        use crate::_core::{
            storage::{ObjectOps, VersionOps},
            version::{self, State},
        };

        let (_root, storage, b, key) = versioned_fixture().await;
        let marker = storage
            .delete_object(&b, &key)
            .await
            .unwrap()
            .id
            .expect("an enabled bucket's marker carries an identity");
        storage.put_object(&b, &key, body(b"four")).await.unwrap();

        let rows = history_newest_first(&storage, &b, &key);
        assert_eq!(rows.len(), 4, "three object versions plus the marker");
        assert_eq!(rows[0].kind, rows::Kind::Marker, "the marker is newest");
        assert_eq!(rows[0].id(), Some(marker.clone()));

        let sweeper = Sweeper::new(
            storage.clone(),
            Options {
                noncurrent_keep: 1,
                ..Options::default()
            },
        );
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();

        assert_eq!(
            summary.expired_versions, 3,
            "the marker's slot leaves the three older versions past the budget"
        );
        let left = history(&storage, &b, &key);
        assert_eq!(left.len(), 1, "the marker occupies the keep slot");
        assert_eq!(left[0].kind, rows::Kind::Marker);
        assert_eq!(left[0].id(), Some(marker.clone()));
        // The surviving marker still answers its version-addressed read.
        match storage
            .get_object_version(&b, &key, Some(&marker), None)
            .await
            .unwrap()
        {
            version::ReadResult::Marker { id, .. } => assert_eq!(id.as_ref(), Some(&marker)),
            version::ReadResult::Object(_) => panic!("a marker is not content"),
        }
        assert_eq!(storage.bucket_versioning(&b).await.unwrap(), State::Enabled);
    }

    /// The reclamation guard's ordering, end to end: whatever order a
    /// versioned PUT and a sweep take when they race on the bucket mutation
    /// lock, the block the PUT's row references survives. PUT first: its
    /// reference is inside the plan the sweep reads (phase 1 takes the same
    /// lock). Sweep first: the walk meets the epoch the PUT bumped, and the
    /// removal phase defers the pass (2026-10-06 — the mark-epoch guard the
    /// design sanctions, in place of holding the lock across the walk).
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_publish_racing_the_sweep_never_loses_the_block_it_references() {
        use crate::_core::{
            storage::{ObjectOps, VersionOps},
            version,
        };

        let (_root, storage, b, key) = versioned_single_fixture().await;
        let previous = storage
            .head_object(&b, &key)
            .await
            .unwrap()
            .version_id
            .expect("an enabled write mints a version id");

        let guard = storage.lock_bucket_mutations(&b).await;
        let putter = storage.clone();
        let (put_bucket, put_key) = (b.clone(), key.clone());
        let put = tokio::spawn(async move {
            putter
                .put_object(&put_bucket, &put_key, body(b"second"))
                .await
        });
        // The PUT has staged its body and is waiting on the lock.
        wait_for_tmp(&storage).await;
        wait_for_lock_waiter().await;

        // A clock past the grace: any block the sweep judges unreferenced is
        // reclaimed in this same pass, so a stale live set loses the block
        // immediately.
        let sweeper = Sweeper::new(storage.clone(), Options::default());
        let sweep = tokio::spawn(async move {
            sweeper
                .sweep_once(SystemTime::now() + Duration::from_secs(601))
                .await
        });
        wait_for_lock_waiter().await; // the sweep parks too
        drop(guard);

        put.await.unwrap().unwrap();
        sweep.await.unwrap().unwrap();

        let versions = storage.versions(&b);
        assert!(
            versions
                .blobs()
                .blocks()
                .contains(&BlockHash::of(b"hello"))
                .await
                .unwrap(),
            "the block the just-committed version row references must survive the sweep"
        );
        match storage
            .get_object_version(&b, &key, Some(&previous), None)
            .await
            .unwrap()
        {
            version::ReadResult::Object(object) => {
                assert_eq!(
                    collect_body(object.body).await.unwrap(),
                    b"hello",
                    "the superseded version still reads after the sweep"
                );
            }
            version::ReadResult::Marker { .. } => panic!("the superseded version is content"),
        }
    }

    /// The race guard's decisive window, pinned end to end: a reference
    /// committed **after** the block walk — the dedup-hit case the grace
    /// cannot cover, because the re-upload finds the old block and leaves
    /// its timestamp untouched — must keep its block. The removal phase
    /// re-checks the version-reference epoch under the bucket mutation
    /// lock; the commit bumped it, so the pass defers and the block
    /// survives — without that check the walk's stale live set would
    /// delete a block a committed row now names.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_reference_committed_after_the_block_walk_keeps_its_block() {
        use crate::_core::storage::{ObjectOps, VersionOps};

        let (_root, storage, b, key) = versioned_single_fixture().await;
        let sweeper = Sweeper::new(storage.clone(), Options::default());
        let now = SystemTime::now() + Duration::from_secs(601);
        let dead = BlockHash::of(b"hello");

        // Make "hello"'s block dead: supersede it into the store (v1's
        // row), then delete v1 permanently — the block stays on disk,
        // unreferenced.
        let previous = storage
            .head_object(&b, &key)
            .await
            .unwrap()
            .version_id
            .expect("an enabled write mints a version id");
        storage.put_object(&b, &key, body(b"second")).await.unwrap();
        storage
            .delete_object_version(&b, &key, Some(&previous))
            .await
            .unwrap();

        // Phases 1 and 2 of a pass: the plan (with its epoch snapshot), then
        // the walk — which judges the block dead and past the grace.
        let (plan, _, epoch) = sweeper
            .plan_bucket(&b, now, &lifecycle::ActionOwners::default())
            .await
            .unwrap();
        let candidates = storage
            .versions(&b)
            .blobs()
            .sweep_candidates(&plan.live, sweeper.options.block_grace, now)
            .await
            .unwrap();
        assert!(
            candidates.entries.iter().any(|entry| entry.hash == dead),
            "the deleted version's block is a candidate: {candidates:?}"
        );

        // A publish commits a row naming the block before the removals run:
        // re-upload "hello", then supersede it — the supersede dedup-hits
        // the old block and commits a row that references it.
        storage.put_object(&b, &key, body(b"hello")).await.unwrap();
        storage.put_object(&b, &key, body(b"fourth")).await.unwrap();

        // Phase 3: the epoch check must defer the removals.
        let report = sweeper.apply_bucket(&b, &candidates, epoch).await.unwrap();
        assert_eq!(
            report.removed, 0,
            "the epoch check defers the re-referenced block's removal: {report:?}"
        );
        assert!(
            storage
                .versions(&b)
                .blobs()
                .blocks()
                .contains(&dead)
                .await
                .unwrap(),
            "the block a just-committed row names must survive the removal phase"
        );
    }

    #[tokio::test]
    async fn sweeps_stale_temps_only() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        // Stale temp: backdate its mtime deterministically (std
        // `set_times` — no extra dependency).
        fs::create_dir(state.path().join("tmp")).unwrap();
        fs::write(state.path().join("tmp/stale"), b"x").unwrap();
        fs::write(state.path().join("tmp/fresh"), b"x").unwrap();
        // Windows requires write access to set times.
        let f = OpenOptions::new()
            .write(true)
            .open(state.path().join("tmp/stale"))
            .unwrap();
        f.set_times(FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(3600)))
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let now = SystemTime::now();
        let summary = sweeper.sweep_once(now).await.unwrap();
        assert_eq!(summary.temp_files, 1);
        assert!(!state.path().join("tmp/stale").exists());
        assert!(state.path().join("tmp/fresh").exists());
    }

    #[tokio::test]
    async fn sweeps_idle_multipart_uploads() {
        let (root, storage) = {
            let root = tempfile::tempdir().unwrap();
            let storage = FsStorage::new(root.path(), fs_options()).unwrap();
            (root, storage)
        };
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .create_multipart_upload(
                &b,
                &"big.bin".into(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        // No parts exist, so idle = initiated_at (a moment ago): at the
        // real `now` the upload is newer than the TTL.
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.uploads, 0);
        // Well past the TTL, the upload is swept.
        let far = SystemTime::now() + Duration::from_secs(3600);
        let summary = sweeper.sweep_once(far).await.unwrap();
        assert_eq!(summary.uploads, 1);
        assert!(
            storage
                .multipart_store()
                .list_uploads(&b)
                .await
                .unwrap()
                .is_empty()
        );
        let _ = root;
    }

    #[tokio::test]
    async fn fresh_uploads_and_temps_survive() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                compact_threshold_percent: 20,
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        fs::create_dir(state.path().join("tmp")).unwrap();
        fs::write(state.path().join("tmp/fresh"), b"x").unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &"big.bin".into(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        storage
            .upload_part(
                &b,
                &"big.bin".into(),
                &upload.upload_id,
                1.into(),
                body(b"x"),
                None,
            )
            .await
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.temp_files, 0);
        assert_eq!(summary.uploads, 0);
    }

    #[tokio::test]
    async fn run_loop_stops_on_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            sweeper.run(rx).await;
        });
        tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn run_loop_executes_a_pass_then_stops_on_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            // One full pass runs (the Ok branch), then the 1 h interval
            // sleep notices the shutdown within one 1 s chunk.
            sweeper.run(rx).await;
        });
        time::sleep(Duration::from_millis(200)).await;
        tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn run_loop_warns_and_keeps_running_on_a_failed_pass() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        // `tmp` as a file: the pass fails at entries_of (NotADirectory) —
        // the loop must warn and continue, then stop on shutdown.
        fs::write(state.path().join("tmp"), b"not a directory").unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            sweeper.run(rx).await;
        });
        time::sleep(Duration::from_millis(200)).await;
        tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn directories_under_tmp_are_never_swept() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        fs::create_dir_all(state.path().join("tmp/nested")).unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.temp_files, 0);
        assert!(state.path().join("tmp/nested").is_dir());
    }

    #[tokio::test]
    async fn future_dated_temp_survives() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        fs::create_dir(state.path().join("tmp")).unwrap();
        let stale = state.path().join("tmp/future");
        fs::write(&stale, b"x").unwrap();
        let f = OpenOptions::new().write(true).open(&stale).unwrap();
        f.set_times(FileTimes::new().set_modified(SystemTime::now() + Duration::from_secs(3600)))
            .unwrap();
        let sweeper = Sweeper::new(storage.clone(), old_ttl_options());
        let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
        assert_eq!(summary.temp_files, 0);
        assert!(stale.exists());
    }
}
