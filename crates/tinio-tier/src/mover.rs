//! The move state machine: demotion, promotion, the delete tombstone and the
//! garbage pass (T12, T13, T14).
//!
//! A demote snapshots the warm row, records a `copying` placement, copies the
//! file into the cold plane and then, under the per-key write lock, revalidates
//! the facts and the placement before it settles the object as cold. The warm
//! file is deliberately left behind: the garbage pass removes it after the
//! grace. A promote copies the cold bytes to the filesystem's stage, then under
//! the same lock revalidates the placement and the cold ETag before it
//! publishes, restores the row from the cold authority, stamps the file with
//! the recorded modification time and removes the cold copy.
//!
//! Garbage collection never guesses. It finishes a `deleting` tombstone whose
//! row is already gone and that is older than the grace, removes the warm file
//! a settled cold object left behind (when no reader holds the key), and
//! settles an aged `copying` placement as either a completed demote (the cold
//! copy is complete and the warm snapshot still matches) or a discarded copy
//! that leaves the object warm. A case it cannot decide is left in place and
//! reported by [`TierStorage::verify`]. Cold content with no placement is never
//! deleted by the automatic pass.
//!
//! Every finalize takes the bucket's shared gate side and then `try_write`s
//! the key's lock, so a busy key — a reader streaming a body, or another move —
//! is skipped rather than waited for (T14); the copy is discarded and the move
//! answers [`MoveOutcome::Skipped`]. The shared side is exclusive with the
//! bucket's versioning change and teardown (which take the write side), so a
//! cold settle can never land inside a versioning pass; it is shared with a
//! listing, whose merged scan already resolves a key once even when the two
//! planes disagree mid-page. Because each state change is one index write and
//! every intermediate state is resolvable, a move whose future is dropped
//! mid-await leaves nothing a later pass cannot settle.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
#[cfg(feature = "testing")]
use std::{io, sync::atomic::AtomicUsize};

use futures::{StreamExt, stream};
use tinio_core::{
    bucket,
    content::{ContentKey, ObjectContent, stage_body},
    object,
    storage::{BucketOps, ListBucketsParams},
    version,
};
use tinio_util::metrics::{
    MoveDirection, record_tier_move, record_tier_move_duration, record_tier_orphans,
};
#[cfg(test)]
use tinio_util::testing::metrics::test_lock;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    Capacity, ColdInfo, Error, HeatEntry, Placement, State, Tier, TierStorage, WarmCandidate,
    storage::{ColdRemoval, Removal, cold_failure, row_matches_snapshot},
};

/// Books one move's duration on drop, so every exit path — a completion, a
/// skip, an abandoned copy or a failure — records exactly one sample.
#[must_use = "the sample is recorded on drop"]
pub(crate) struct MoveTimer {
    /// The direction the attempted move was travelling.
    direction: MoveDirection,
    /// When the move left its preconditions.
    started: Instant,
}

impl MoveTimer {
    /// Start timing one move in `direction`.
    pub(crate) fn new(direction: MoveDirection) -> Self {
        Self {
            direction,
            started: Instant::now(),
        }
    }
}

impl Drop for MoveTimer {
    fn drop(&mut self) {
        #[cfg(test)]
        let _gate = test_lock::writer();
        record_tier_move_duration(self.direction, self.started.elapsed());
    }
}

/// The placement scan's page size for the collection and verification passes.
const SCAN_PAGE: usize = 256;

/// The warm-candidate page a capacity selection starts from; a page with no
/// eligible candidate widens before the selection gives up.
const WARM_PAGE: usize = 256;

/// The heat-row page the post-bind promotion restore reads.
const HEAT_PAGE: usize = 256;

/// The row-scan page the post-bind warm-table rebuild reads.
const ROW_PAGE: usize = 256;

/// One pass's bucket-versioning answers: a bucket's answer must not change
/// within a pass, so the heat, idle and capacity legs share one lookup.
type VersionMemo = HashMap<bucket::Name, bool>;

/// What one demote or promote did.
///
/// [`Moved`](MoveOutcome::Moved) means the object's bytes changed plane.
/// [`Skipped`](MoveOutcome::Skipped) means the key was busy (a reader or
/// another move held it) and the mover passed it by.
/// [`Aborted`](MoveOutcome::Aborted) means the move was abandoned: the
/// snapshot no longer matched, the object was gone, or another operation
/// settled the key first. An abandoned demote's staged copy is discarded —
/// unless another pass already completed that very demote, in which case its
/// cold copy is the object and is kept. [`Unchanged`](MoveOutcome::Unchanged)
/// means there was nothing to do: the object was already in the target plane,
/// or no placement asked for a move.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::MoveOutcome;
///
/// let finished = MoveOutcome::Moved { bytes: 512 };
/// match finished {
///     MoveOutcome::Moved { bytes } => assert_eq!(bytes, 512),
///     other => panic!("unexpected outcome: {other:?}"),
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveOutcome {
    /// The object changed planes. `bytes` is the moved object's size, which
    /// the capacity pass sums to know how much of the volume this pass has
    /// taken out of the warm plane before garbage collection can reclaim the
    /// files.
    Moved {
        /// The moved object's size in bytes.
        bytes: u64,
    },
    /// The key was busy (a reader or another move held it) and the mover
    /// passed it by.
    Skipped,
    /// The move was abandoned: the snapshot no longer matched, the object was
    /// gone, or another operation settled the key first.
    Aborted,
    /// Nothing asked for a move: the object was already in the target plane,
    /// or no placement asked for a move.
    Unchanged,
}

/// What one garbage-collection pass did.
///
/// `tombstones` counts finished delete tombstones, `files` the leftover warm
/// files removed, `completed` the `copying` placements settled as demotes and
/// `reverted` the copies discarded in favour of the warm plane (a row that was
/// gone is cleared the same way). `unresolved` counts the copies the pass could
/// not decide, and `busy` the keys a reader or another move held. `orphans`
/// counts the cold copies the pass found unreferenced: a live cold placement
/// whose object row is gone and whose copy is still in the cold plane, which
/// the pass leaves for the read path and repair to settle and reports on the
/// orphan gauge. `failed` counts the objects whose own collection answered an
/// error: the pass logs each one and carries on with the next.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::GcSummary;
///
/// let quiet = GcSummary::default();
/// assert_eq!(quiet.files, 0);
/// assert_eq!(quiet.unresolved, 0);
/// assert_eq!(quiet.orphans, 0);
/// assert_eq!(quiet.failed, 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GcSummary {
    /// Delete tombstones finished.
    pub tombstones: usize,
    /// Leftover warm files removed.
    pub files: usize,
    /// `copying` placements settled as completed demotes.
    pub completed: usize,
    /// `copying` placements discarded in favour of the warm plane.
    pub reverted: usize,
    /// `copying` placements the pass could not decide.
    pub unresolved: usize,
    /// Keys skipped because a reader or another move held them.
    pub busy: usize,
    /// Cold copies the pass observed with no object row behind them.
    pub orphans: u64,
    /// Objects whose own collection failed; the pass continued past them.
    pub failed: u64,
}

/// What an offline verification found (T12).
///
/// The report is read-only: it names the `copying` placements garbage
/// collection could not settle, so an operator can inspect the keys. It never
/// moves or deletes anything.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::VerifyReport;
///
/// let report = VerifyReport {
///     unresolved_copying: Vec::new(),
/// };
/// assert!(report.is_clean());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyReport {
    /// The `(bucket, key)` pairs of the aged `copying` placements whose fate
    /// the collection pass could not decide.
    pub unresolved_copying: Vec<(bucket::Name, object::Key)>,
}

impl VerifyReport {
    /// Whether the verification found nothing to report.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::VerifyReport;
    ///
    /// let mut report = VerifyReport::default();
    /// assert!(report.is_clean());
    /// report.unresolved_copying.push((
    ///     bucket::name("data").expect("a bucket"),
    ///     object::key("a").expect("a key"),
    /// ));
    /// assert!(!report.is_clean());
    /// ```
    pub fn is_clean(&self) -> bool {
        self.unresolved_copying.is_empty()
    }
}

impl<C: ObjectContent> TierStorage<C> {
    /// Demote one warm object into the cold plane (T4, T13).
    ///
    /// The row is snapshotted, a `copying` placement records the cold metadata
    /// and the move's start time, and the file streams into the cold plane's
    /// stage outside the locks. Under the bucket's shared finalize gate and the
    /// key's write lock the row, placement and bucket versioning are
    /// revalidated, and only then is the staged copy published and the
    /// placement settled as `pg`/`live`. A row or placement that changed
    /// meanwhile discards the copy;
    /// a bucket that now records versions does the same, so a versioned bucket
    /// never ends up cold. The warm file is left for the garbage pass.
    ///
    /// A folder marker, an object already in the cold plane, and a placement
    /// another pass is resolving answer [`MoveOutcome::Unchanged`]. A busy key
    /// answers [`MoveOutcome::Skipped`].
    ///
    /// # Errors
    ///
    /// The row read's failure, the cold namespace or copy failure, the index
    /// failure, and the warm file's open failure. A copy failure restores the
    /// warm placement before the error is returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::{
    ///     Class,
    ///     acl::Acl,
    ///     bucket, object,
    ///     storage::{BucketOps, ObjectOps, Storage},
    /// };
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let key = object::key("a.bin").expect("a valid key");
    ///     tier.put_object(&name, &key, tinio_util::testing::body(b"hi"))
    ///         .await
    ///         .expect("the object is written");
    ///     let summary = tier
    ///         .demote(&name, &key, SystemTime::now())
    ///         .await
    ///         .expect("the demote runs");
    ///     assert!(matches!(summary, tinio_tier::MoveOutcome::Moved { .. }));
    ///     assert_eq!(
    ///         tier.storage_class(&name, &key)
    ///             .await
    ///             .expect("the class reads"),
    ///         Class::StandardIa
    ///     );
    /// });
    /// ```
    #[tracing::instrument(
        target = "tinio::tier",
        name = "tier.demote",
        level = "debug",
        skip_all
    )]
    pub async fn demote(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        now: SystemTime,
    ) -> Result<MoveOutcome, Error> {
        Self::refuse_reserved(bucket)?;
        if key.is_folder_marker() {
            return Ok(MoveOutcome::Unchanged);
        }
        self.flush_key(bucket, key).await?;
        let Some(snapshot) = self.rows.info(bucket, key).await? else {
            return Ok(MoveOutcome::Unchanged);
        };
        let previous = self.index.placement(bucket, key)?;
        match previous.as_ref() {
            Some(placement) if !placement.is_warm_live() => {
                return Ok(MoveOutcome::Unchanged);
            }
            _ => {}
        }
        let cold = ColdInfo::from_info(&snapshot);
        // Every sample from here on is an attempted demote: a copy that fails
        // or is abandoned books its duration too, while the move counter is
        // booked only when the object settles cold.
        let _timer = MoveTimer::new(MoveDirection::Demote);
        self.ensure_cold_bucket(bucket).await?;
        let copying = Placement {
            tier: Tier::Fs,
            state: State::Copying,
            cold: Some(cold.clone()),
            mtime: now,
        };
        self.index.set_placement(bucket, key, &copying)?;
        // T13's copy: the bytes are staged outside the locks. The visibility
        // point — `publish` — belongs to the locked finalize below, so two
        // demotes of one key cannot publish over each other.
        let staged = match self.stage_from_warm(bucket, key).await {
            Ok(staged) => staged,
            Err(err) => {
                self.abandon_demote(bucket, key, &copying, previous.as_ref())
                    .await?;
                return Err(err);
            }
        };

        let _gate = self.gates.read(bucket).await;
        let Some(_guard) = self.locks.try_write(bucket, key) else {
            self.abandon_demote(bucket, key, &copying, previous.as_ref())
                .await?;
            return Ok(MoveOutcome::Skipped);
        };
        let settled = self.index.placement(bucket, key)?.as_ref() == Some(&copying)
            && self.snapshot_matches(bucket, key, &snapshot).await?;
        if !settled || self.records_versions(bucket).await? {
            self.abandon_demote(bucket, key, &copying, previous.as_ref())
                .await?;
            return Ok(MoveOutcome::Aborted);
        }
        let content_key = ContentKey::Object(key.clone());
        if let Err(err) = self
            .cold
            .publish(bucket, staged, &content_key)
            .await
            .map_err(cold_failure)
        {
            self.abandon_demote(bucket, key, &copying, previous.as_ref())
                .await?;
            return Err(err);
        }
        self.index.set_placement(
            bucket,
            key,
            &Placement {
                tier: Tier::Pg,
                state: State::Live,
                cold: Some(cold),
                mtime: now,
            },
        )?;
        // The object is cold now; its warm candidate is dead (T21).
        self.index
            .warm_remove(bucket, key, snapshot.last_modified)?;
        // The migration point: the bytes changed plane, so this is one demote.
        #[cfg(test)]
        let _gate = test_lock::writer();
        record_tier_move(MoveDirection::Demote);
        Ok(MoveOutcome::Moved {
            bytes: snapshot.size,
        })
    }

    /// Promote one cold object back to the warm plane (T4, T13).
    ///
    /// The cold placement is snapshotted and its bytes are staged on the warm
    /// plane's temp outside the key's write lock, so a reader of the key never
    /// waits for the copy (T14). The key's write side is then taken without
    /// waiting — a busy key answers [`MoveOutcome::Skipped`] and the staged
    /// copy is dropped. The move finishes through the layer's one promotion
    /// sequence (`finish_promotion`, shared with the copy, rename and
    /// versioning paths), which re-checks the placement, retires a row that is
    /// gone, and applies T5's fs-won revalidation: a warm file whose row ETag
    /// differs from the cold snapshot means an overwrite already completed, so
    /// the warm object wins, the stale row is never restored and the cold copy
    /// is dropped.
    ///
    /// An object that is not cold or a busy key therefore never moves; a
    /// deleted key, a completed warm overwrite and a superseded placement
    /// answer [`MoveOutcome::Aborted`] without publishing anything.
    ///
    /// # Errors
    ///
    /// The cold open/copy failure (a missing copy or a length mismatch is
    /// reported by name), the filesystem stage or publish failure, and the row
    /// or index failure.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::{
    ///     Class,
    ///     acl::Acl,
    ///     bucket, object,
    ///     storage::{BucketOps, ObjectOps, Storage},
    /// };
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let key = object::key("a.bin").expect("a valid key");
    ///     tier.put_object(&name, &key, tinio_util::testing::body(b"hi"))
    ///         .await
    ///         .expect("the object is written");
    ///     tier.demote(&name, &key, SystemTime::now())
    ///         .await
    ///         .expect("the demote runs");
    ///     let summary = tier.promote(&name, &key).await.expect("the promote runs");
    ///     assert!(matches!(summary, tinio_tier::MoveOutcome::Moved { .. }));
    ///     assert_eq!(
    ///         tier.storage_class(&name, &key)
    ///             .await
    ///             .expect("the class reads"),
    ///         Class::Standard
    ///     );
    /// });
    /// ```
    #[tracing::instrument(
        target = "tinio::tier",
        name = "tier.promote",
        level = "debug",
        skip_all
    )]
    pub async fn promote(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<MoveOutcome, Error> {
        Self::refuse_reserved(bucket)?;
        if key.is_folder_marker() {
            return Ok(MoveOutcome::Unchanged);
        }
        self.flush_key(bucket, key).await?;
        let Some(placement) = self.index.placement(bucket, key)? else {
            return Ok(MoveOutcome::Unchanged);
        };
        if !placement.is_cold_live() {
            return Ok(MoveOutcome::Unchanged);
        }
        let _gate = self.gates.read(bucket).await;
        // T13/T14: the cold copy is staged outside the key's write lock, so
        // an in-flight read of the key never waits for the copy; only the
        // revalidation and the publish take the lock. The attempt's duration
        // is booked by the timer `prepare_promotion` starts.
        let prepared = self.prepare_promotion(bucket, key).await?;
        let Some(_guard) = self.locks.try_write(bucket, key) else {
            return Ok(MoveOutcome::Skipped);
        };
        let moved = self.finish_promotion(bucket, key, prepared).await?;
        if !moved {
            return Ok(MoveOutcome::Aborted);
        }
        self.invalidate(bucket, key);
        Ok(MoveOutcome::Moved {
            bytes: placement.cold.as_ref().map_or(0, |cold| cold.size),
        })
    }

    /// Delete one object under the write lock, leaving the tombstone garbage
    /// collection finishes after a crash (T13).
    ///
    /// The tombstone is written first, the filesystem delete removes the row
    /// and the file, the index is cleared and the cold copy removed. A failure
    /// settles the placement against the row the delete left: a surviving row
    /// restores the previous placement, an absent one keeps the tombstone.
    ///
    /// # Errors
    ///
    /// The buffered flush, the filesystem delete, the index and the cold plane
    /// failures.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_core::{
    ///     acl::Acl,
    ///     bucket, object,
    ///     storage::{BucketOps, ObjectOps},
    /// };
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let key = object::key("a.bin").expect("a valid key");
    ///     tier.put_object(&name, &key, tinio_util::testing::body(b"hi"))
    ///         .await
    ///         .expect("the object is written");
    ///     tier.delete_tombstone(&name, &key)
    ///         .await
    ///         .expect("the delete runs");
    ///     assert!(tier.head_object(&name, &key).await.is_err());
    /// });
    /// ```
    pub async fn delete_tombstone(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        Self::refuse_reserved(bucket)?;
        self.flush_key(bucket, key).await?;
        let _guard = self.locks.write(bucket, key).await;
        self.delete_locked(bucket, key).await
    }

    /// Run one garbage-collection pass over every bucket at `now` (T12).
    ///
    /// The pass finishes aged delete tombstones whose row is gone, removes the
    /// warm files settled cold objects left behind when no reader holds them,
    /// and settles aged `copying` placements: a complete cold copy with a
    /// matching warm snapshot finishes the demote, anything else is discarded
    /// and the object stays warm. A bucket that now records versions never
    /// settles a copy as cold.
    ///
    /// Cold content with no placement is never touched: an orphan there is
    /// `repair`'s explicit business, not the automatic pass's. The pass does
    /// report the orphan it can observe — a live cold placement whose object
    /// row is gone while the copy survives — on the orphan gauge, as the
    /// absolute count this pass found.
    ///
    /// One object's failure never aborts the pass: the object is logged by
    /// bucket and key, counted in [`GcSummary::failed`], and the scan carries
    /// on, so a single unreadable row or cold copy cannot wedge collection.
    ///
    /// # Errors
    ///
    /// The structural failures only: the bucket listing and the placement
    /// scan. A per-object failure is counted and logged, never returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::{acl::Acl, bucket, storage::BucketOps};
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let summary = tier.gc(SystemTime::now()).await.expect("the pass runs");
    ///     assert_eq!(summary, tinio_tier::GcSummary::default());
    /// });
    /// ```
    #[tracing::instrument(target = "tinio::tier", name = "tier.gc", level = "info", skip_all)]
    pub async fn gc(&self, now: SystemTime) -> Result<GcSummary, Error> {
        let mut summary = GcSummary::default();
        for bucket in self.live_buckets().await? {
            let mut after: Option<String> = None;
            loop {
                let page = self
                    .index
                    .scan_placement(&bucket, "", after.as_deref(), SCAN_PAGE)?;
                let done = page.len() < SCAN_PAGE;
                for (key, placement) in page {
                    after = Some(key.as_ref().to_owned());
                    if let Err(err) = self
                        .gc_one(&bucket, &key, &placement, now, &mut summary)
                        .await
                    {
                        // One object's collection cannot decide the pass: the
                        // rest still run, and the failed key is retried by a
                        // later pass.
                        summary.failed = summary.failed.saturating_add(1);
                        tracing::warn!(
                            bucket = %bucket,
                            key = %key,
                            error = %err,
                            "tier gc: one object's collection failed; the pass continues"
                        );
                    }
                }
                if done {
                    break;
                }
            }
        }
        // The gauge reports the absolute count this pass observed, so a served
        // process' `/metrics` reflects its last collection even when it found
        // nothing. The pass never scans the cold plane for unplaced content
        // (T12 leaves that to `repair`), so what it can observe is the live
        // cold placement whose object row is gone while its copy survives.
        #[cfg(test)]
        let _gate = test_lock::writer();
        record_tier_orphans(summary.orphans);
        Ok(summary)
    }

    /// Report the conditions an offline verification cannot settle (T12).
    ///
    /// The pass is read-only. Today it reports the `copying` placements older
    /// than the grace whose cold snapshot is missing, the state the collection
    /// pass refuses to guess at.
    ///
    /// # Errors
    ///
    /// The bucket listing and placement scan failures.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::{acl::Acl, bucket, storage::BucketOps};
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, TierStorage};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     let name = bucket::name("data").expect("a valid bucket name");
    ///     tier.create_bucket(&name, None, &Acl::default_private(None))
    ///         .await
    ///         .expect("the bucket is created");
    ///     let report = tier
    ///         .verify(SystemTime::now())
    ///         .await
    ///         .expect("the verification runs");
    ///     assert!(report.is_clean());
    /// });
    /// ```
    #[tracing::instrument(target = "tinio::tier", name = "tier.verify", level = "info", skip_all)]
    pub async fn verify(&self, now: SystemTime) -> Result<VerifyReport, Error> {
        let mut report = VerifyReport::default();
        for bucket in self.live_buckets().await? {
            let mut after: Option<String> = None;
            loop {
                let page = self
                    .index
                    .scan_placement(&bucket, "", after.as_deref(), SCAN_PAGE)?;
                let done = page.len() < SCAN_PAGE;
                for (key, placement) in page {
                    after = Some(key.as_ref().to_owned());
                    if placement.state == State::Copying
                        && placement.cold.is_none()
                        && self.grace_elapsed(now, placement.mtime)
                    {
                        report.unresolved_copying.push((bucket.clone(), key));
                    }
                }
                if done {
                    break;
                }
            }
        }
        Ok(report)
    }

    /// Stage the warm file at `key` into the cold plane, answering the staged
    /// write the finalize publishes (T13).
    ///
    /// Nothing is visible yet: the cold `publish` is the visibility point and
    /// the demote runs it inside the key's write lock, after the revalidation.
    async fn stage_from_warm(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<<C as ObjectContent>::Staged, Error> {
        let content_key = ContentKey::Object(key.clone());
        let body = self.fs_content().open(bucket, &content_key, None).await?;
        let mut staged = self
            .cold
            .stage(bucket, &content_key)
            .await
            .map_err(cold_failure)?;
        stage_body(&mut staged, body).await?;
        Ok(staged)
    }

    /// Whether the row still holds exactly the snapshot the demote took.
    async fn snapshot_matches(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        snapshot: &object::Info,
    ) -> Result<bool, Error> {
        Ok(self
            .rows
            .facts(bucket, key)
            .await?
            .is_some_and(|facts| row_matches_snapshot(&facts, snapshot)))
    }

    /// Discard the copy a demote produced and put the warm placement back.
    ///
    /// The copy belongs to the demote only while its `copying` placement is
    /// still the one on disk. A concurrent settle (another pass completing the
    /// demote), a newer demote, a write reset and a delete all own the key's
    /// state — and its cold copy — so the abandoned copy is discarded only when
    /// nobody else has taken over.
    async fn abandon_demote(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        copying: &Placement,
        previous: Option<&Placement>,
    ) -> Result<(), Error> {
        // The abandoned demote's own placement names the copy it produced.
        let had_cold = copying.cold.is_some();
        let Some(current) = self.index.placement(bucket, key)? else {
            // A delete finished and cleared the placement: the copy is dead.
            self.discard_cold(bucket, key, had_cold).await;
            return Ok(());
        };
        if current.is_cold_live() {
            // Another pass completed this very demote; its cold copy is the
            // object now.
            return Ok(());
        }
        if current != *copying {
            if current.state != State::Copying {
                // A write reset or a delete tombstone owns the key; discard
                // the copy this demote no longer represents.
                self.discard_cold(bucket, key, had_cold).await;
            }
            return Ok(());
        }
        self.discard_cold(bucket, key, had_cold).await;
        match self.rows.info(bucket, key).await? {
            Some(info) => match previous {
                Some(placement) => self.index.set_placement(
                    bucket,
                    key,
                    &Placement {
                        tier: placement.tier,
                        state: placement.state,
                        cold: placement.cold.clone(),
                        mtime: info.last_modified,
                    },
                )?,
                None => self.index.clear(bucket, key)?,
            },
            None => self.index.clear(bucket, key)?,
        }
        Ok(())
    }

    /// Every bucket the index can hold placements for, in name order — the
    /// reserved root namespace is never one (T18).
    async fn live_buckets(&self) -> Result<Vec<bucket::Name>, Error> {
        let mut buckets = Vec::new();
        let mut start_after: Option<String> = None;
        loop {
            let page = self
                .list_buckets(
                    ListBucketsParams {
                        prefix: String::new(),
                        start_after: start_after.clone(),
                        max_buckets: SCAN_PAGE,
                    },
                    None,
                    None,
                )
                .await?;
            buckets.extend(page.buckets.into_iter().map(|entry| entry.name));
            let Some(next) = page.next_start_after else {
                return Ok(buckets);
            };
            if !page.truncated || start_after.as_deref() == Some(next.as_str()) {
                return Ok(buckets);
            }
            start_after = Some(next);
        }
    }

    /// Settle one placement the collection pass found.
    async fn gc_one(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        placement: &Placement,
        now: SystemTime,
        summary: &mut GcSummary,
    ) -> Result<(), Error> {
        if placement.is_warm_live() {
            return Ok(());
        }
        if !self.grace_elapsed(now, placement.mtime) {
            return Ok(());
        }
        let _gate = self.gates.read(bucket).await;
        let Some(_guard) = self.locks.try_write(bucket, key) else {
            summary.busy += 1;
            return Ok(());
        };
        if self.index.placement(bucket, key)?.as_ref() != Some(placement) {
            return Ok(());
        }
        match placement.state {
            State::Deleting => self.finish_tombstone(bucket, key, placement, summary).await,
            State::Copying => {
                self.resolve_copying(bucket, key, placement, now, summary)
                    .await
            }
            State::Live if placement.is_cold_live() => {
                self.collect_cold(bucket, key, placement, summary).await
            }
            State::Live => Ok(()),
        }
    }

    /// Finish a delete tombstone whose row is already gone.
    async fn finish_tombstone(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        placement: &Placement,
        summary: &mut GcSummary,
    ) -> Result<(), Error> {
        if self.rows.facts(bucket, key).await?.is_some() {
            // The row survives: the delete never reached it. Task 10's
            // tombstone rollback owns this state, not the collection pass.
            return Ok(());
        }
        // A tombstone over a cold object carries the object's recorded time
        // in its cold snapshot; the removal retires the candidate it left
        // behind (T21) and the heat row with it.
        self.remove_traces(
            bucket,
            key,
            Removal {
                warm: placement.cold.as_ref().map(|cold| cold.last_modified),
                cold: ColdRemoval::Strict,
            },
        )
        .await?;
        summary.tombstones += 1;
        Ok(())
    }

    /// Settle an aged `copying` placement: complete the demote, or discard the
    /// copy and leave the object warm.
    async fn resolve_copying(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        placement: &Placement,
        now: SystemTime,
        summary: &mut GcSummary,
    ) -> Result<(), Error> {
        let Some(cold) = placement.cold.clone() else {
            summary.unresolved += 1;
            return Ok(());
        };
        // The placement being resolved carries the copy every branch discards.
        let had_cold = placement.cold.is_some();
        let expected = cold.to_info()?;
        let Some(facts) = self.rows.facts(bucket, key).await? else {
            // The row is gone: the object was deleted out from under the
            // copy. The dead placement, its candidate and its heat row are
            // cleared together and the partial copy is discarded.
            self.remove_traces(
                bucket,
                key,
                Removal {
                    warm: Some(cold.last_modified),
                    cold: ColdRemoval::BestEffort,
                },
            )
            .await?;
            summary.reverted += 1;
            return Ok(());
        };
        let snapshot_matches = row_matches_snapshot(&facts, &expected);
        let complete = self
            .cold
            .stat(bucket, key)
            .await
            .map_err(cold_failure)?
            .is_some_and(|entry| entry.len == expected.size);
        if complete && snapshot_matches && !self.records_versions(bucket).await? {
            self.index.set_placement(
                bucket,
                key,
                &Placement {
                    tier: Tier::Pg,
                    state: State::Live,
                    cold: Some(cold),
                    mtime: now,
                },
            )?;
            // The crash interrupted the demote before it retired the
            // candidate; the completed move owns that now (T21).
            self.index.warm_remove(bucket, key, facts.mtime)?;
            summary.completed += 1;
        } else {
            self.discard_cold(bucket, key, had_cold).await;
            self.index.set_placement(
                bucket,
                key,
                &Placement {
                    tier: Tier::Fs,
                    state: State::Live,
                    cold: None,
                    mtime: facts.mtime,
                },
            )?;
            // The object stays warm, so it remains a capacity candidate.
            self.index.warm_replace(bucket, key, facts.mtime)?;
            summary.reverted += 1;
        }
        Ok(())
    }

    /// Remove the warm file a settled cold object left behind, and reconcile a
    /// warm plane that won an overwrite (T5, T12).
    async fn collect_cold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        placement: &Placement,
        summary: &mut GcSummary,
    ) -> Result<(), Error> {
        let Some(cold) = placement.cold.as_ref() else {
            return Ok(());
        };
        let Some(facts) = self.rows.facts(bucket, key).await? else {
            // A live placement over an absent row: the read path retires it,
            // and the collection pass must not guess at the cold copy. A copy
            // that is actually there has no object behind it any more, so the
            // pass reports it as the orphan it observed and leaves it in place.
            // A stat failure here is only a missed observation; it never turns
            // the collection into a failure.
            if self
                .cold
                .stat(bucket, key)
                .await
                .is_ok_and(|entry| entry.is_some())
            {
                summary.orphans = summary.orphans.saturating_add(1);
            }
            return Ok(());
        };
        // Deliberately less than `row_matches_snapshot`: the modification
        // time is excluded because a crash may have restamped the warm file,
        // while the same ETag and size still name the same object.
        if facts.etag.as_str() != cold.etag || facts.size != cold.size {
            // A completed overwrite won on the warm plane: reconcile to warm
            // and drop the stale cold copy.
            self.reset_to_fs(bucket, key, facts.mtime).await?;
            summary.reverted += 1;
            return Ok(());
        }
        let Some(file) = self.fs_content().stat(bucket, key).await? else {
            return Ok(());
        };
        if file.len != cold.size {
            // Not the snapshot's file; leave it to the repair pass.
            return Ok(());
        }
        self.fs_content()
            .remove(bucket, &ContentKey::Object(key.clone()))
            .await?;
        summary.files += 1;
        Ok(())
    }

    /// Whether `since` is older than the configured collection grace.
    fn grace_elapsed(&self, now: SystemTime, since: SystemTime) -> bool {
        now.duration_since(since)
            .is_ok_and(|age| age >= self.config.gc_grace)
    }
}

/// What one mover pass did.
///
/// `promoted` counts objects whose bytes moved back to the warm plane.
/// `demoted` counts the capacity leg's demotions only, and `idle_demoted` the
/// idle leg's, so an operator can tell pressure-driven movement from
/// disuse-driven movement. `skipped` counts the moves the pass passed by (a
/// busy key, a key inside its dwell, a key whose candidate is no longer
/// movable), `aborted` the moves whose revalidation found a newer owner, and
/// `failed` the moves that answered an error. A failure never stops the pass:
/// the remaining candidates are still attempted, and the failed key keeps its
/// placement and its warm candidate for a later pass.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::PassSummary;
///
/// let quiet = PassSummary::default();
/// assert_eq!(quiet.demoted, 0);
/// assert_eq!(quiet.idle_demoted, 0);
/// assert_eq!(quiet.failed, 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PassSummary {
    /// Objects promoted from the cold plane.
    pub promoted: usize,
    /// Objects the capacity leg demoted into the cold plane.
    pub demoted: usize,
    /// Objects the idle leg demoted into the cold plane.
    pub idle_demoted: u64,
    /// Moves the pass passed by.
    pub skipped: usize,
    /// Moves whose revalidation abandoned the copy.
    pub aborted: usize,
    /// Moves that answered an error and left their placement alone.
    pub failed: usize,
}

impl PassSummary {
    /// Count one move's outcome.
    fn record(&mut self, outcome: Result<Step, Error>) {
        match outcome {
            Ok(Step::Promoted) => self.promoted += 1,
            Ok(Step::Demoted) => self.demoted += 1,
            Ok(Step::Skipped) => self.skipped += 1,
            Ok(Step::Aborted) => self.aborted += 1,
            Err(err) => {
                self.failed += 1;
                tracing::warn!(error = %err, "tier mover: a move failed");
            }
        }
    }
}

/// One move's outcome inside a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// The bytes moved to the warm plane.
    Promoted,
    /// The bytes moved to the cold plane.
    Demoted,
    /// The move was passed by.
    Skipped,
    /// The revalidation abandoned the copy.
    Aborted,
}

/// How the shared warm-candidate filter treats a candidate with no placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Absent {
    /// Eligible only while its object row survives: a stale candidate a
    /// delete left behind is skipped (the idle leg).
    RowRequired,
    /// Eligible as it stands: another move or a tombstone may own the key, and
    /// the key may come back warm (the capacity leg).
    Eligible,
}

/// The background pass: capacity demotion, heat promotion and collection
/// cadence (T13, T21).
///
/// The mover owns the warm layer's three periodic decisions. Capacity reads
/// [`Capacity::used_percent`] and, above `high_water_percent`, demotes the
/// oldest warm candidates until the probe reports `low_water_percent` or the
/// candidates run out. Heat drains the promotion queue the read path fills and
/// promotes the cold keys in it, deferring a key inside the configured dwell.
/// [`Mover::gc`] runs the collection pass at most once per `gc_grace`, the
/// interval at which its leftovers can become collectable, and the stats flush
/// makes the in-memory heat counters durable at most once per `stats_flush`.
///
/// The warm candidate table is rebuilt from the filesystem's row scan by
/// [`Mover::bind`] — the step a serving process runs after the cold plane is
/// bound (T21). Until the rebuild finishes, candidate selection waits; reads
/// and heat recording keep working throughout, because neither reads the
/// warm table.
///
/// # Examples
///
/// ```rust
/// use std::{sync::Arc, time::Duration};
///
/// use tinio_fs::testing;
/// use tinio_store_redb::RedbContent;
/// use tinio_tier::{Config, FlushMode, Mover, TierStorage, VolumeCapacity};
/// use tokio::runtime::Runtime;
///
/// let root = tempfile::tempdir().expect("a temporary root");
/// let tier = TierStorage::open(
///     root.path(),
///     testing::fs_options(),
///     Config {
///         capacity_bytes: 1024 * 1024,
///         max_object_bytes: 64 * 1024,
///         flush_mode: FlushMode::Sync,
///         batch_bytes: 8 * 1024,
///         batch_interval: Duration::from_secs(1),
///         heat_window: Duration::from_secs(60),
///         promote_reads: 3,
///         stats_flush: Duration::from_secs(10),
///         high_water_percent: 90,
///         low_water_percent: 80,
///         demote_idle: Duration::from_secs(3600),
///         dwell: Duration::from_secs(3600),
///         scan_interval: Duration::from_secs(60),
///         move_concurrency: 1,
///         index_compact_percent: 30,
///         gc_grace: Duration::from_secs(300),
///     },
///     RedbContent::in_memory().expect("an in-memory cold plane"),
/// )
/// .expect("the tier layer opens");
/// let mover = Mover::new(tier, Arc::new(VolumeCapacity::new(root.path())), 1);
/// Runtime::new().expect("a runtime").block_on(async {
///     mover.bind().await.expect("the warm table is rebuilt");
///     let summary = mover.pass().await.expect("the pass runs");
///     assert_eq!(summary.demoted, 0);
/// });
/// ```
pub struct Mover<C: ObjectContent> {
    /// The layer whose planes the pass moves objects between.
    storage: TierStorage<C>,
    /// The warm volume's pressure.
    capacity: Arc<dyn Capacity>,
    /// The number of moves one pass keeps in flight.
    concurrency: usize,
    /// Whether the post-bind warm-table rebuild has finished (T21).
    warm_ready: AtomicBool,
    /// When the collection pass last ran; the guard is held across the pass,
    /// so two concurrent callers cannot both run it.
    last_gc: AsyncMutex<Option<Instant>>,
    /// When the heat counters last reached the index.
    last_stats: Mutex<Option<Instant>>,
    /// The first instant a stored heat row can expire: the older of the last
    /// expiry scan's oldest survivor and one snapshot window below the scan,
    /// plus the retention. The scan walks the whole heat table, so the flush
    /// skips it while this instant is still ahead; `None` until a scan has
    /// run, so a restart scans once.
    next_heat_expiry: Mutex<Option<SystemTime>>,
    /// Serializes the whole heat flush — snapshot, absorb and forget — so two
    /// calls cannot snapshot the same counters and absorb them twice.
    flush_gate: Mutex<()>,
    /// The bind attempts a test may still fail before the rebuild proceeds
    /// (the `testing` feature's failure injection).
    #[cfg(feature = "testing")]
    bind_failures: AtomicUsize,
}

impl<C: ObjectContent> Mover<C> {
    /// Build the pass over `storage`, reading `capacity` and keeping
    /// `concurrency` moves in flight.
    ///
    /// A concurrency of zero is treated as one: a pass always makes progress
    /// on at least one candidate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Mover, TierStorage, VolumeCapacity};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// let mover = Mover::new(tier, Arc::new(VolumeCapacity::new(root.path())), 0);
    /// assert!(!mover.warm_ready());
    /// ```
    pub fn new(storage: TierStorage<C>, capacity: Arc<dyn Capacity>, concurrency: usize) -> Self {
        Self {
            storage,
            capacity,
            concurrency: concurrency.max(1),
            warm_ready: AtomicBool::new(false),
            last_gc: AsyncMutex::new(None),
            last_stats: Mutex::new(None),
            next_heat_expiry: Mutex::new(None),
            flush_gate: Mutex::new(()),
            #[cfg(feature = "testing")]
            bind_failures: AtomicUsize::new(0),
        }
    }

    /// Whether the warm-candidate rebuild has finished (T21).
    ///
    /// Capacity candidate selection is gated on this flag; reads and heat
    /// recording are not, because they never consult the warm table.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Mover, TierStorage, VolumeCapacity};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// let mover = Mover::new(tier, Arc::new(VolumeCapacity::new(root.path())), 1);
    /// assert!(!mover.warm_ready());
    /// ```
    pub fn warm_ready(&self) -> bool {
        self.warm_ready.load(Ordering::SeqCst)
    }

    /// Make the next [`Mover::bind`] fail — the `testing` feature's failure
    /// injection, so the pass's retry can be observed.
    #[cfg(feature = "testing")]
    pub fn fail_next_bind(&self) {
        self.bind_failures.store(1, Ordering::SeqCst);
    }

    /// Rebuild the warm-candidate table and restore the promotion queue
    /// (T21).
    ///
    /// Every warm object row — one with no placement or an `fs`/`live`
    /// placement — becomes a capacity candidate keyed by its recorded
    /// modification time; a folder marker and a row the cold plane owns are
    /// skipped. Every durable heat row inside the window whose count crossed
    /// `promote_reads` for an object the cold plane owns re-enters the
    /// promotion queue.
    ///
    /// The rebuild touches the index only. Reads and heat recording keep
    /// working while it runs; capacity candidate selection waits for the flag
    /// this method sets last.
    ///
    /// # Errors
    ///
    /// The bucket listing, row scan, placement or heat read, and the index
    /// write that records a candidate or expires a row.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Mover, TierStorage, VolumeCapacity};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// let mover = Mover::new(tier, Arc::new(VolumeCapacity::new(root.path())), 1);
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     mover.bind().await.expect("the warm table is rebuilt");
    ///     assert!(mover.warm_ready());
    /// });
    /// ```
    pub async fn bind(&self) -> Result<(), Error> {
        #[cfg(feature = "testing")]
        if self.bind_failures.load(Ordering::SeqCst) > 0 {
            self.bind_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::Io(io::Error::other("a scripted bind failure")));
        }
        for bucket in self.storage.live_buckets().await? {
            self.rebuild_bucket(&bucket).await?;
        }
        self.restore_promotions()?;
        self.warm_ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Run one capacity-and-heat pass.
    ///
    /// A warm-table rebuild that has not finished yet is retried here, at most
    /// once per pass: a failed [`Mover::bind`] never aborts the pass and never
    /// blocks heat — reads and heat keep working while the table is missing —
    /// and the next pass tries again until the table is bound.
    ///
    /// Heat is served first: every promotion queue entry the read path filled
    /// is promoted unless the key is still inside its dwell, the bucket now
    /// records versions, or the key is not cold. The idle leg then demotes the
    /// warm objects no read has touched inside `demote_idle` (unless that
    /// threshold is zero, which disables the leg). Capacity demotes the
    /// oldest warm candidates while the probe reports more than the high
    /// watermark, stopping at the low watermark. Every key the heat leg or
    /// the idle leg attempted is excluded from this pass's capacity
    /// candidates, so a promotion is never handed straight back to the cold
    /// plane by the demotion that follows it, and the idle leg cannot demote
    /// the same object twice in one pass; the exclusion lasts one pass. The
    /// stats flush runs last, so a read recorded just before the pass is
    /// visible to its promotion and the counters survive a restart.
    ///
    /// One failed move never stops the pass: the key keeps the placement the
    /// move revalidated away and its warm candidate, so the next pass retries
    /// it.
    ///
    /// # Errors
    ///
    /// The capacity probe, the bucket listing and versioning reads, and every
    /// index write the pass itself performs. A move's own failure and a failed
    /// warm-table retry are counted or logged, not returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Mover, TierStorage, VolumeCapacity};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// let mover = Mover::new(tier, Arc::new(VolumeCapacity::new(root.path())), 1);
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     mover.bind().await.expect("the warm table is rebuilt");
    ///     let summary = mover.pass().await.expect("the pass runs");
    ///     assert_eq!(summary.promoted + summary.demoted, 0);
    /// });
    /// ```
    #[tracing::instrument(target = "tinio::tier", name = "tier.pass", level = "info", skip_all)]
    pub async fn pass(&self) -> Result<PassSummary, Error> {
        self.ensure_bound().await;
        let mut summary = PassSummary::default();
        let mut versioned = VersionMemo::new();
        let mut attempted = self.promote_heat(&mut summary, &mut versioned).await?;
        self.demote_idle(&mut summary, &mut attempted, &mut versioned)
            .await?;
        self.demote_capacity(&mut summary, attempted, &mut versioned)
            .await?;
        self.flush_stats()?;
        Ok(summary)
    }

    /// Retry the warm-table rebuild when it has not finished, logging rather
    /// than returning the failure so a pass still serves heat.
    async fn ensure_bound(&self) {
        if self.warm_ready() {
            return;
        }
        if let Err(err) = self.bind().await {
            tracing::warn!(
                error = %err,
                "tier mover: the warm table rebuild failed; the next pass retries"
            );
        }
    }

    /// Run the collection pass when its cadence is due (T12, T13).
    ///
    /// Garbage collection's leftovers become collectable only after
    /// `gc_grace`, so the pass runs at most once per grace window: `None`
    /// means the cadence has not elapsed and nothing was scanned.
    ///
    /// # Errors
    ///
    /// The collection pass's own failures (the bucket listing, the placement
    /// scan, the row, cold-plane, file and index operations it settles).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Mover, TierStorage, VolumeCapacity};
    /// use tokio::runtime::Runtime;
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// let mover = Mover::new(tier, Arc::new(VolumeCapacity::new(root.path())), 1);
    /// Runtime::new().expect("a runtime").block_on(async {
    ///     assert!(mover.gc().await.expect("the first pass runs").is_some());
    ///     assert_eq!(
    ///         mover.gc().await.expect("the second call answers"),
    ///         None,
    ///         "the grace window gates the next pass",
    ///     );
    /// });
    /// ```
    pub async fn gc(&self) -> Result<Option<GcSummary>, Error> {
        // The guard spans the pass: a second caller waits, then sees the
        // stamp and answers `None`, so a pass runs at most once per grace.
        let mut last_gc = self.last_gc.lock().await;
        if last_gc.is_some_and(|at| at.elapsed() < self.storage.config.gc_grace) {
            return Ok(None);
        }
        let outcome = self.storage.gc(SystemTime::now()).await;
        // Stamp once the pass has finished, so a slow pass cannot make the
        // next tick look due; a failed pass also waits for the grace.
        *last_gc = Some(Instant::now());
        Ok(Some(outcome?))
    }

    /// Promote every key the promotion queue holds, at most `concurrency` at
    /// a time.
    ///
    /// Answers the keys this pass attempted, promoted or not; the capacity leg
    /// takes that answer as its starting exclusion set. Each queued bucket's
    /// versioning is looked up once, before the concurrent moves; a lookup
    /// that fails is retried per key, so its failure is booked on the key's
    /// own step exactly as before.
    async fn promote_heat(
        &self,
        summary: &mut PassSummary,
        versioned: &mut VersionMemo,
    ) -> Result<HashSet<(String, String)>, Error> {
        let queue = self.storage.heat().promote_queue();
        let mut attempted = HashSet::new();
        if queue.is_empty() {
            return Ok(attempted);
        }
        let mut seen = HashSet::new();
        let candidates: Vec<_> = queue
            .into_iter()
            .filter(|(bucket, key)| {
                seen.insert((bucket.as_ref().to_owned(), key.as_ref().to_owned()))
            })
            .collect();
        for (bucket, _) in &candidates {
            if versioned.contains_key(bucket) {
                continue;
            }
            if let Ok(records) = self.storage.records_versions(bucket).await {
                versioned.insert(bucket.clone(), records);
            }
        }
        let outcomes: Vec<((String, String), Result<Step, Error>)> = stream::iter(candidates)
            .map(|(bucket, key)| {
                let id = (bucket.as_ref().to_owned(), key.as_ref().to_owned());
                let records_versions = versioned.get(&bucket).copied();
                async move { (id, self.promote_one(&bucket, &key, records_versions).await) }
            })
            .buffer_unordered(self.concurrency)
            .collect()
            .await;
        for (id, outcome) in outcomes {
            // A promoted key re-enters the warm table at its old modification
            // time, so it would be the capacity leg's first candidate unless
            // this pass remembers that the heat leg already handled it.
            attempted.insert(id);
            summary.record(outcome);
        }
        Ok(attempted)
    }

    /// Decide and perform one queued promotion.
    ///
    /// `records_versions` is the pass's per-bucket answer when the heat leg
    /// resolved it; `None` retries the lookup, so a bucket read that failed
    /// is still booked on this key alone.
    async fn promote_one(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        records_versions: Option<bool>,
    ) -> Result<Step, Error> {
        if key.is_folder_marker() {
            return Ok(Step::Skipped);
        }
        let records_versions = match records_versions {
            Some(records) => records,
            None => self.storage.records_versions(bucket).await?,
        };
        if records_versions {
            return Ok(Step::Skipped);
        }
        let Some(placement) = self.storage.index.placement(bucket, key)? else {
            return Ok(Step::Skipped);
        };
        if !placement.is_cold_live() {
            return Ok(Step::Skipped);
        }
        if self.within_dwell(placement.mtime) {
            // Requeue it: the queue was drained by the pop, and the key is
            // still wanted once its dwell elapses.
            self.storage.heat().enqueue(bucket.clone(), key.clone());
            return Ok(Step::Skipped);
        }
        let moved = self.storage.promote(bucket, key).await?;
        match moved {
            MoveOutcome::Moved { .. } => {
                // `promote` records the promoted object as a warm candidate
                // itself.
                Ok(Step::Promoted)
            }
            MoveOutcome::Aborted => Ok(Step::Aborted),
            MoveOutcome::Skipped | MoveOutcome::Unchanged => Ok(Step::Skipped),
        }
    }

    /// Whether `moved_at` is inside the configured dwell.
    fn within_dwell(&self, moved_at: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(moved_at)
            .is_ok_and(|age| age < self.storage.config.dwell)
    }

    /// Whether `bucket` records versions, answered from the pass's memo so a
    /// bucket is read once across the legs.
    async fn records_versions_memo(
        &self,
        versioned: &mut VersionMemo,
        bucket: &bucket::Name,
    ) -> Result<bool, Error> {
        if let Some(records) = versioned.get(bucket) {
            return Ok(*records);
        }
        let records = self.storage.records_versions(bucket).await?;
        versioned.insert(bucket.clone(), records);
        Ok(records)
    }

    /// Demote the warm objects no read has touched inside `demote_idle`.
    ///
    /// The warm table is ordered by recorded modification time, so the scan
    /// stops at the first candidate younger than the threshold: every later
    /// row is at least as new. A zero threshold disables the leg. The
    /// eligibility is the capacity leg's — no marker, a bucket that records no
    /// versions, and an `fs`/`live` placement (or no placement over a row that
    /// still exists) — and a read recorded inside the threshold, in the
    /// in-memory counters or the durable heat rows, keeps the object warm.
    ///
    /// Every attempted candidate joins the pass's exclusion set, so the
    /// capacity leg cannot hand it to the cold plane again in the same pass.
    /// The idle leg's moves are booked on [`PassSummary::idle_demoted`], never
    /// on [`PassSummary::demoted`], which stays capacity-only.
    #[tracing::instrument(
        target = "tinio::tier",
        name = "tier.pass.demote_idle",
        level = "debug",
        skip_all
    )]
    async fn demote_idle(
        &self,
        summary: &mut PassSummary,
        attempted: &mut HashSet<(String, String)>,
        versioned: &mut VersionMemo,
    ) -> Result<(), Error> {
        let threshold = self.storage.config.demote_idle;
        if threshold.is_zero() || !self.warm_ready() {
            return Ok(());
        }
        let candidates = self
            .idle_candidates(attempted, threshold, versioned)
            .await?;
        if candidates.is_empty() {
            return Ok(());
        }
        for candidate in &candidates {
            attempted.insert((
                candidate.bucket.as_ref().to_owned(),
                candidate.key.as_ref().to_owned(),
            ));
        }
        let outcomes: Vec<Result<(Step, u64), Error>> = stream::iter(candidates)
            .map(|candidate| async move { self.demote_one(&candidate).await })
            .buffer_unordered(self.concurrency)
            .collect()
            .await;
        for outcome in outcomes {
            match outcome {
                Ok((Step::Demoted, _)) => {
                    summary.idle_demoted = summary.idle_demoted.saturating_add(1);
                }
                other => summary.record(other.map(|(step, _)| step)),
            }
        }
        Ok(())
    }

    /// The warm candidates idle past `threshold`, oldest first.
    ///
    /// A page is widened when it holds no younger candidate and no more rows,
    /// so a run of ineligible entries cannot hide the candidates behind it. A
    /// settled cold object's stale candidate and a folder marker are discarded
    /// from the table, exactly as the capacity selection discards them.
    async fn idle_candidates(
        &self,
        attempted: &HashSet<(String, String)>,
        threshold: Duration,
        versioned: &mut VersionMemo,
    ) -> Result<Vec<WarmCandidate>, Error> {
        let now = SystemTime::now();
        let mut candidates = Vec::new();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut limit = WARM_PAGE;
        loop {
            let page = self.storage.index.scan_warm(limit)?;
            let complete = page.len() < limit;
            for candidate in page {
                if !now
                    .duration_since(candidate.mtime)
                    .is_ok_and(|age| age >= threshold)
                {
                    // The table is ordered oldest-first, so every later row is
                    // at least as new as this one.
                    return Ok(candidates);
                }
                if !self
                    .candidate_is_eligible(
                        &candidate,
                        attempted,
                        &mut seen,
                        versioned,
                        Absent::RowRequired,
                    )
                    .await?
                {
                    continue;
                }
                if !self.is_idle(&candidate.bucket, &candidate.key, threshold)? {
                    continue;
                }
                candidates.push(candidate);
            }
            if complete {
                return Ok(candidates);
            }
            limit = limit.saturating_mul(2);
        }
    }

    /// Whether no read of `key` was recorded inside `threshold`.
    ///
    /// The in-memory counters answer for reads since the last drain; the
    /// durable heat rows answer for the reads already flushed. A counter that
    /// ages out of a heat window shorter than `threshold` leaves less
    /// evidence, which is the accepted cost of keeping the window small.
    fn is_idle(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        threshold: Duration,
    ) -> Result<bool, Error> {
        if self
            .storage
            .heat()
            .last_read(bucket, key)
            .is_some_and(|at| at.elapsed() < threshold)
        {
            return Ok(false);
        }
        let Some(last) = self.storage.index.heat_last_read(bucket, key)? else {
            return Ok(true);
        };
        // A row stamped in the future (a clock that moved backwards) cannot be
        // aged, and it is the one record that must not be read as disuse: the
        // object stays warm.
        Ok(SystemTime::now()
            .duration_since(last)
            .is_ok_and(|age| age >= threshold))
    }

    /// Demote the oldest warm candidates while the volume is over its high
    /// watermark, stopping at the low watermark.
    ///
    /// A settled demote leaves its warm file to garbage collection, which
    /// `gc_grace` gates, so the probe cannot fall during the pass. The pass
    /// therefore measures the bytes each snapshot demotes and stops once they
    /// cover the gap between the probe and the low watermark: an over-full
    /// volume is brought down without duplicating its whole warm set into the
    /// cold plane before any file is removed. A probe that does fall during
    /// the pass (a collection that ran) still ends the loop at the low
    /// watermark. An object is never counted twice: a candidate is attempted
    /// once per pass, and only a move that changed the plane contributes its
    /// bytes.
    ///
    /// `attempted` starts with the keys the heat leg handled this pass, so a
    /// key just promoted cannot be the first candidate demoted again; it grows
    /// with every candidate this leg queues. The selection's cursor lasts the
    /// pass, so the warm table is walked once however many batches the loop
    /// takes.
    #[tracing::instrument(
        target = "tinio::tier",
        name = "tier.pass.demote_capacity",
        level = "debug",
        skip_all
    )]
    async fn demote_capacity(
        &self,
        summary: &mut PassSummary,
        mut attempted: HashSet<(String, String)>,
        versioned: &mut VersionMemo,
    ) -> Result<(), Error> {
        if !self.warm_ready() {
            return Ok(());
        }
        let high = self.storage.config.high_water_percent;
        let low = self.storage.config.low_water_percent;
        let mut used = self.capacity.used_percent()?;
        if used <= high {
            return Ok(());
        }
        let total = self.capacity.total_bytes()?;
        let mut reclaimed: u128 = 0;
        let mut cursor = None;
        while used > low {
            // The gap still open at this probe, in bytes. Comparing the bytes
            // this pass has already taken off the warm plane against it stops
            // the loop when the pending reclaim covers the gap.
            let target = reclaim_target(used, low, total);
            if reclaimed >= target {
                break;
            }
            let batch = self
                .next_candidates(&attempted, &mut cursor, versioned)
                .await?;
            if batch.is_empty() {
                break;
            }
            for candidate in &batch {
                attempted.insert((
                    candidate.bucket.as_ref().to_owned(),
                    candidate.key.as_ref().to_owned(),
                ));
            }
            let outcomes: Vec<Result<(Step, u64), Error>> = stream::iter(batch)
                .map(|candidate| async move { self.demote_one(&candidate).await })
                .buffer_unordered(self.concurrency)
                .collect()
                .await;
            for outcome in outcomes {
                match outcome {
                    Ok((step, bytes)) => {
                        summary.record(Ok(step));
                        reclaimed = reclaimed.saturating_add(u128::from(bytes));
                    }
                    Err(err) => summary.record(Err(err)),
                }
            }
            used = self.capacity.used_percent()?;
        }
        Ok(())
    }

    /// The next `concurrency` eligible warm candidates, oldest first.
    ///
    /// A candidate is eligible while its bucket does not record versions and
    /// its object is warm. A marker or a candidate whose placement another
    /// state owns is discarded from the table: neither can ever be demoted.
    /// A page with nothing eligible widens before the selection gives up, so a
    /// run of ineligible entries cannot hide the candidates behind it.
    ///
    /// `cursor` is the last row an earlier batch examined; the scan resumes
    /// strictly after it, so one pass walks the warm table instead of
    /// re-reading its oldest rows for every batch. The cursor names a table
    /// position, so a row retired since the last batch cannot wedge it.
    ///
    /// `attempted` is this pass's handled set: the heat leg's keys and the
    /// candidates this leg already queued. A key in it is skipped for the rest
    /// of the pass and is eligible again in the next one.
    async fn next_candidates(
        &self,
        attempted: &HashSet<(String, String)>,
        cursor: &mut Option<WarmCandidate>,
        versioned: &mut VersionMemo,
    ) -> Result<Vec<WarmCandidate>, Error> {
        let want = self.concurrency;
        let mut batch = Vec::with_capacity(want);
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut limit = WARM_PAGE;
        loop {
            let page = self.storage.index.scan_warm_after(cursor.as_ref(), limit)?;
            let complete = page.len() < limit;
            let before = batch.len();
            // The page's last examined row, accepted or not. The cursor moves
            // to it once per page instead of cloning every row it walks past.
            let mut last: Option<WarmCandidate> = None;
            for candidate in page {
                if batch.len() >= want {
                    break;
                }
                last = Some(candidate);
                let eligible = if let Some(candidate) = last.as_ref() {
                    self.candidate_is_eligible(
                        candidate,
                        attempted,
                        &mut seen,
                        versioned,
                        Absent::Eligible,
                    )
                    .await?
                } else {
                    false
                };
                if eligible && let Some(taken) = last.take() {
                    batch.push(taken);
                }
            }
            if let Some(last) = last {
                *cursor = Some(last);
            } else if batch.len() > before {
                // The last examined row was accepted, so it is the batch's
                // last row; the one clone per page is the cursor's whole cost.
                *cursor = batch.last().cloned();
            }
            if complete {
                return Ok(batch);
            }
            limit = limit.saturating_mul(2);
        }
    }

    /// The filter both warm-candidate selections share.
    ///
    /// A key this pass already handled, a duplicate row, a folder marker (a
    /// directory for the mover's rebuild, retired from the table here) and a
    /// versioned bucket's key are never candidates. A settled cold object's
    /// stale candidate is retired from the table; any other placement must be
    /// a live warm one. Only the absent-placement case differs between the
    /// legs, and `Absent` names it.
    async fn candidate_is_eligible(
        &self,
        candidate: &WarmCandidate,
        attempted: &HashSet<(String, String)>,
        seen: &mut HashSet<(String, String)>,
        versioned: &mut VersionMemo,
        absent: Absent,
    ) -> Result<bool, Error> {
        let id = (
            candidate.bucket.as_ref().to_owned(),
            candidate.key.as_ref().to_owned(),
        );
        if attempted.contains(&id) || !seen.insert(id) {
            return Ok(false);
        }
        if candidate.key.is_folder_marker() {
            self.storage
                .index
                .warm_remove(&candidate.bucket, &candidate.key, candidate.mtime)?;
            return Ok(false);
        }
        if self
            .records_versions_memo(versioned, &candidate.bucket)
            .await?
        {
            return Ok(false);
        }
        match self
            .storage
            .index
            .placement(&candidate.bucket, &candidate.key)?
        {
            // A settled cold object is no longer a candidate.
            Some(placement) if placement.is_cold_live() => {
                self.storage.index.warm_remove(
                    &candidate.bucket,
                    &candidate.key,
                    candidate.mtime,
                )?;
                Ok(false)
            }
            Some(placement) => Ok(placement.is_warm_live()),
            None => Ok(match absent {
                Absent::Eligible => true,
                Absent::RowRequired => self
                    .storage
                    .rows
                    .info(&candidate.bucket, &candidate.key)
                    .await?
                    .is_some(),
            }),
        }
    }

    /// Demote one capacity candidate, keeping it for a retry when it did not
    /// move. Answers the step and, only when the object changed plane, the
    /// bytes the move took off the warm plane.
    async fn demote_one(&self, candidate: &WarmCandidate) -> Result<(Step, u64), Error> {
        let moved = self
            .storage
            .demote(&candidate.bucket, &candidate.key, SystemTime::now())
            .await?;
        match moved {
            MoveOutcome::Moved { bytes } => {
                self.storage.index.warm_remove(
                    &candidate.bucket,
                    &candidate.key,
                    candidate.mtime,
                )?;
                Ok((Step::Demoted, bytes))
            }
            MoveOutcome::Skipped => Ok((Step::Skipped, 0)),
            MoveOutcome::Aborted => Ok((Step::Aborted, 0)),
            // Nothing to do: the row is gone, so the candidate is stale.
            MoveOutcome::Unchanged => {
                self.storage.index.warm_remove(
                    &candidate.bucket,
                    &candidate.key,
                    candidate.mtime,
                )?;
                Ok((Step::Skipped, 0))
            }
        }
    }

    /// Record `bucket`'s warm object rows as capacity candidates.
    ///
    /// The row scan pages the bucket; each page is recorded in one index
    /// transaction, so the rebuild pays one commit per page instead of one
    /// placement read and one candidate commit per object. The rows, their
    /// order and the placement gate are the per-object rebuild's.
    async fn rebuild_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut after: Option<String> = None;
        loop {
            let page = self
                .storage
                .rows
                .scan(bucket, "", after.as_deref(), ROW_PAGE)
                .await?;
            let complete = page.len() < ROW_PAGE;
            if let Some(last) = page.last() {
                after = Some(last.key.as_ref().to_owned());
            }
            let rows: Vec<(object::Key, SystemTime)> = page
                .iter()
                .filter(|info| !info.key.is_folder_marker())
                .map(|info| (info.key.clone(), info.last_modified))
                .collect();
            self.storage.index.warm_rebuild_page(bucket, &rows)?;
            if complete {
                return Ok(());
            }
        }
    }

    /// Restore the promotion queue from the durable heat rows inside the
    /// window (T21).
    ///
    /// The heat table is read in [`HEAT_PAGE`] pages, so the restore never
    /// materializes every durable row at once. Each page filters the rows
    /// exactly as a whole-table scan did: a count at or above `promote_reads`
    /// and a read inside `heat_window` earn a placement check, and only a
    /// settled cold key is enqueued.
    fn restore_promotions(&self) -> Result<(), Error> {
        let promote_reads = u64::from(self.storage.config.promote_reads);
        if promote_reads == 0 {
            return Ok(());
        }
        let window = self.storage.config.heat_window;
        let now = SystemTime::now();
        let mut after: Option<(bucket::Name, object::Key)> = None;
        loop {
            let page = self
                .storage
                .index
                .scan_heat_after(after.as_ref().map(|(bucket, key)| (bucket, key)), HEAT_PAGE)?;
            let complete = page.len() < HEAT_PAGE;
            if let Some(last) = page.last() {
                after = Some((last.bucket.clone(), last.key.clone()));
            }
            for row in page {
                if row.reads < promote_reads {
                    continue;
                }
                if !now
                    .duration_since(row.last_read)
                    .is_ok_and(|age| age <= window)
                {
                    continue;
                }
                match self.storage.index.placement(&row.bucket, &row.key)? {
                    Some(placement) if placement.is_cold_live() => {
                        self.storage.heat().enqueue(row.bucket, row.key);
                    }
                    _ => {}
                }
            }
            if complete {
                return Ok(());
            }
        }
    }

    /// Flush the in-memory heat counters to the index and expire the rows
    /// that left the window, at most once per `stats_flush`.
    ///
    /// The flush is two-phase: the counters are snapshotted, the index absorbs
    /// the rows, and then exactly the committed rows are forgotten, so a read
    /// that lands while the batch is being written stays visible to an idle
    /// check. A counter a read touched in that window keeps only the delta the
    /// index has not stored. A partial absorb forgets its committed prefix and
    /// answers the error; the rest waits for the next flush. Whole flushes are
    /// serialized, so two calls cannot absorb the same counters twice.
    ///
    /// Durable rows are kept for the longer of `heat_window` and
    /// `demote_idle`: a row is the evidence an idle check reads, so raising
    /// the idle threshold above the heat window must not shorten the
    /// protection it buys. A stored row older than `heat_window` restarts its
    /// count from the incoming delta, so a single fresh read cannot revive an
    /// old total into a promotion. The promotion restore still filters by
    /// `heat_window` alone.
    ///
    /// The expiry scan decodes the whole heat table, so an in-memory watermark
    /// of the first instant a stored row can expire skips it while no row can
    /// have crossed the cutoff; the first flush after a restart scans once.
    ///
    /// The CLI calls this from its own background task on the `stats_flush`
    /// cadence, so heat reaches the durable table without waiting for a
    /// `scan_interval` pass; [`Mover::pass`] also calls it at its end, and the
    /// pass's own `stats_flush` gate makes the extra call a no-op.
    ///
    /// # Errors
    ///
    /// The index's heat write or expiry failure. A call outside the gate
    /// answers `Ok` without touching the index.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use tinio_fs::testing;
    /// use tinio_store_redb::RedbContent;
    /// use tinio_tier::{Config, FlushMode, Mover, TierStorage, VolumeCapacity};
    ///
    /// let root = tempfile::tempdir().expect("a temporary root");
    /// let tier = TierStorage::open(
    ///     root.path(),
    ///     testing::fs_options(),
    ///     Config {
    ///         capacity_bytes: 1024 * 1024,
    ///         max_object_bytes: 64 * 1024,
    ///         flush_mode: FlushMode::Sync,
    ///         batch_bytes: 8 * 1024,
    ///         batch_interval: Duration::from_secs(1),
    ///         heat_window: Duration::from_secs(60),
    ///         promote_reads: 3,
    ///         stats_flush: Duration::from_secs(10),
    ///         high_water_percent: 90,
    ///         low_water_percent: 80,
    ///         demote_idle: Duration::from_secs(3600),
    ///         dwell: Duration::from_secs(3600),
    ///         scan_interval: Duration::from_secs(60),
    ///         move_concurrency: 1,
    ///         index_compact_percent: 30,
    ///         gc_grace: Duration::from_secs(300),
    ///     },
    ///     RedbContent::in_memory().expect("an in-memory cold plane"),
    /// )
    /// .expect("the tier layer opens");
    /// let mover = Mover::new(tier, Arc::new(VolumeCapacity::new(root.path())), 1);
    /// mover.flush_stats().expect("the heat counters flush");
    /// ```
    #[tracing::instrument(target = "tinio::tier", name = "tier.stats", level = "info", skip_all)]
    pub fn flush_stats(&self) -> Result<(), Error> {
        // The whole flush is serialized: two calls must not snapshot the same
        // counters and absorb them twice. A second caller waits here and then
        // sees the gate closed (`stats_flush` has not elapsed).
        let _flush = self
            .flush_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let due = {
            let mut last = self
                .last_stats
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let due = last.is_none_or(|at| at.elapsed() >= self.storage.config.stats_flush);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if !due {
            return Ok(());
        }
        // Two-phase: the counters stay visible until the index has accepted
        // the rows, so an idle check landing between the snapshot and the
        // forget cannot see neither source. Only the rows the index committed
        // are forgotten; a partial failure leaves the rest for the next flush,
        // and a counter a read touched meanwhile keeps only its unabsorbed
        // delta.
        let taken = self.storage.heat().snapshot();
        if !taken.is_empty() {
            let rows: Vec<HeatEntry> = taken
                .iter()
                .map(|snapshot| snapshot.entry.clone())
                .collect();
            // A stored row older than the heat window starts a fresh count, so
            // a read that reaches an old row cannot revive its total.
            let stale_before = SystemTime::now()
                .checked_sub(self.storage.config.heat_window)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let absorbed = self.storage.index.absorb_heat_report(&rows, stale_before);
            self.storage.heat().forget(&taken[..absorbed.committed]);
            if let Some(err) = absorbed.error {
                return Err(err);
            }
        }
        self.storage.heat().expire();
        // A durable row is the evidence an idle check reads, so it must
        // outlive the heat window whenever the idle threshold is longer: a
        // raised `demote_idle` must not be capped by `heat_window`.
        let retention = self
            .storage
            .config
            .heat_window
            .max(self.storage.config.demote_idle);
        let now = SystemTime::now();
        if let Some(cutoff) = now.checked_sub(retention) {
            // The expiry scan decodes the whole heat table, so it only runs
            // once a stored row can actually have crossed the cutoff. The
            // first instant that can happen is the older of the last scan's
            // oldest survivor and one snapshot window below it — the oldest
            // stamp a later absorb can carry — plus the retention.
            let due = self
                .next_heat_expiry
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none_or(|next| now >= next);
            if due {
                let expired = self.storage.index.expire_heat_report(cutoff)?;
                let next = now
                    .checked_sub(self.storage.config.heat_window)
                    .and_then(|earliest| {
                        let oldest = expired.oldest.map_or(earliest, |at| at.min(earliest));
                        oldest.checked_add(retention)
                    });
                *self
                    .next_heat_expiry
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = next;
            }
        }
        Ok(())
    }
}

/// The bytes that bring `used` down to `low` on a volume of `total` bytes: the
/// watermark gap, translated through the volume's size.
fn reclaim_target(used: u8, low: u8, total: u64) -> u128 {
    u128::from(used.saturating_sub(low)).saturating_mul(u128::from(total)) / 100
}

#[cfg(test)]
mod idle_tests {
    use std::{
        collections::HashSet,
        sync::{Arc, Barrier},
        thread,
        time::{Duration, SystemTime},
    };

    use tinio_core::{
        Class,
        acl::Acl,
        bucket,
        multipart::{self, CompletedPart},
        object,
        storage::{BucketOps, MultipartOps, ObjectOps, Storage, VersionOps},
        version,
    };
    use tinio_fs::{FsStorage, testing};
    use tinio_store_redb::RedbContent;
    use tinio_util::testing::{
        body,
        metrics::{gauge, test_lock},
    };
    use tokio::time::sleep;

    use super::VersionMemo;
    use crate::{
        Capacity, Config, Error, FlushMode, HeatEntry, Index, MoveOutcome, Mover, TierStorage,
    };

    /// A volume under no pressure, so the capacity leg never moves anything
    /// and only the idle leg can account for a demotion.
    #[derive(Debug)]
    struct Quiet;

    impl Capacity for Quiet {
        fn used_percent(&self) -> Result<u8, Error> {
            Ok(0)
        }

        fn total_bytes(&self) -> Result<u64, Error> {
            Ok(1024 * 1024 * 1024)
        }
    }

    /// The idle threshold these legs use: long enough that a test's own writes
    /// and reads stay comfortably inside it on a loaded host.
    const IDLE: Duration = Duration::from_millis(1000);

    fn config() -> Config {
        Config {
            capacity_bytes: 1024 * 1024,
            max_object_bytes: 64 * 1024,
            flush_mode: FlushMode::Sync,
            batch_bytes: 8 * 1024,
            batch_interval: Duration::from_secs(1),
            heat_window: Duration::from_secs(60),
            promote_reads: 3,
            stats_flush: Duration::from_secs(10),
            high_water_percent: 90,
            low_water_percent: 80,
            demote_idle: IDLE,
            dwell: Duration::from_secs(3600),
            scan_interval: Duration::from_secs(60),
            move_concurrency: 1,
            index_compact_percent: 30,
            gc_grace: Duration::from_secs(300),
        }
    }

    fn data() -> bucket::Name {
        bucket::name("data").expect("a valid bucket name")
    }

    fn key(name: &str) -> object::Key {
        object::key(name).expect("a valid object key")
    }

    /// A tier over a temporary root and an in-memory cold plane, with the one
    /// bucket the legs write into.
    async fn tier() -> (tempfile::TempDir, TierStorage<RedbContent>) {
        tier_with(config()).await
    }

    /// The same fixture with a scripted configuration.
    async fn tier_with(config: Config) -> (tempfile::TempDir, TierStorage<RedbContent>) {
        let root = tempfile::tempdir().expect("a temporary root");
        let tier = TierStorage::open(
            root.path(),
            testing::fs_options(),
            config,
            RedbContent::in_memory().expect("an in-memory cold plane"),
        )
        .expect("the tier layer opens");
        tier.create_bucket(&data(), None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        (root, tier)
    }

    async fn put(tier: &TierStorage<RedbContent>, name: &str) {
        tier.put_object(&data(), &key(name), body(b"hi"))
            .await
            .expect("the object is written");
    }

    async fn class(tier: &TierStorage<RedbContent>, name: &str) -> Class {
        tier.storage_class(&data(), &key(name))
            .await
            .expect("the class reads")
    }

    /// An object that has gone unread past `demote_idle` is demoted by the
    /// idle leg, not by capacity pressure.
    #[tokio::test]
    async fn a_warm_object_idle_past_the_threshold_is_demoted() {
        let (_root, tier) = tier().await;
        put(&tier, "idle.bin").await;
        sleep(IDLE + Duration::from_millis(100)).await;

        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        let summary = mover.pass().await.expect("the pass runs");

        assert_eq!(summary.idle_demoted, 1, "the idle leg demotes the object");
        assert_eq!(summary.demoted, 0, "capacity pressure never ran");
        assert_eq!(class(&tier, "idle.bin").await, Class::StandardIa);
    }

    /// A read recorded inside `demote_idle` keeps the object warm — first from
    /// the in-memory counter, then (after the pass's own flush) from the
    /// durable heat row, so both sources must be consulted.
    #[tokio::test]
    async fn a_recently_read_object_is_not_idle_demoted() {
        let (_root, tier) = tier().await;
        put(&tier, "read.bin").await;
        sleep(IDLE + Duration::from_millis(100)).await;
        tier.get_object(&data(), &key("read.bin"), None)
            .await
            .expect("the object is read");
        assert_eq!(
            tier.heat().score(&data(), &key("read.bin")),
            1,
            "the read is held in memory"
        );

        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        let first = mover.pass().await.expect("the first pass runs");
        assert_eq!(
            first.idle_demoted, 0,
            "the in-memory counter keeps the object warm"
        );
        assert_eq!(class(&tier, "read.bin").await, Class::Standard);

        // The pass flushed the counters, so this pass can only see the durable
        // heat row.
        assert_eq!(
            tier.heat().score(&data(), &key("read.bin")),
            0,
            "the pass drained the in-memory counter"
        );
        assert!(
            tier.index()
                .heat_last_read(&data(), &key("read.bin"))
                .expect("the heat row reads")
                .is_some_and(|last| SystemTime::now().duration_since(last).is_ok()),
            "the durable row survives the flush"
        );
        let second = mover.pass().await.expect("the second pass runs");
        assert_eq!(
            second.idle_demoted, 0,
            "the durable heat row keeps the object warm"
        );
        assert_eq!(class(&tier, "read.bin").await, Class::Standard);
    }

    /// Of two aged warm objects, exactly the unread one is demoted: the read
    /// one keeps its candidate and stays warm.
    #[tokio::test]
    async fn the_idle_leg_demotes_only_the_unread_object() {
        let (_root, tier) = tier().await;
        put(&tier, "read.bin").await;
        put(&tier, "unread.bin").await;
        sleep(IDLE + Duration::from_millis(100)).await;
        tier.get_object(&data(), &key("read.bin"), None)
            .await
            .expect("the object is read");

        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        let summary = mover.pass().await.expect("the pass runs");

        assert_eq!(summary.idle_demoted, 1, "exactly one object is idle");
        assert_eq!(class(&tier, "read.bin").await, Class::Standard);
        assert_eq!(class(&tier, "unread.bin").await, Class::StandardIa);
        let warm = tier.index().scan_warm(10).expect("the scan runs");
        assert_eq!(warm.len(), 1, "the read object stays a candidate");
        assert_eq!(warm[0].key, key("read.bin"));
    }

    /// A durable heat row stamped in the future cannot be aged, and it is the
    /// one record that must not be read as disuse: the object stays warm.
    #[tokio::test]
    async fn a_future_dated_heat_row_keeps_the_object_warm() {
        let (_root, tier) = tier().await;
        put(&tier, "future.bin").await;
        tier.index()
            .absorb_heat(&[HeatEntry {
                bucket: data(),
                key: key("future.bin"),
                reads: 1,
                last_read: SystemTime::now() + Duration::from_secs(3600),
            }])
            .expect("the future row is stored");
        sleep(IDLE + Duration::from_millis(100)).await;

        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        let summary = mover.pass().await.expect("the pass runs");

        assert_eq!(
            summary.idle_demoted, 0,
            "a future read keeps the object warm"
        );
        assert_eq!(class(&tier, "future.bin").await, Class::Standard);
    }

    /// A key the heat leg promoted in this pass is excluded from the idle leg:
    /// its promoted candidate carries an old recorded time and it has no heat
    /// evidence left, so only the pass's exclusion set keeps it out of the
    /// cold plane.
    #[tokio::test]
    async fn a_promoted_key_is_excluded_from_the_idle_leg() {
        let mut config = config();
        config.dwell = Duration::ZERO;
        let (_root, tier) = tier_with(config).await;
        put(&tier, "promoted.bin").await;
        let moved = tier
            .demote(&data(), &key("promoted.bin"), SystemTime::now())
            .await
            .expect("the demote runs");
        assert!(
            matches!(moved, MoveOutcome::Moved { .. }),
            "the fixture object is cold"
        );
        // The row keeps the original recorded time, so the promoted candidate
        // is older than the idle threshold.
        sleep(IDLE + Duration::from_millis(200)).await;

        // A durable row inside the window fills the promotion queue at bind;
        // expiring it again leaves the promoted key with no heat evidence at
        // pass time, so only the exclusion set can keep it warm.
        tier.index()
            .absorb_heat(&[HeatEntry {
                bucket: data(),
                key: key("promoted.bin"),
                reads: 3,
                last_read: SystemTime::now() - Duration::from_secs(10),
            }])
            .expect("the durable heat row is planted");
        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        tier.index()
            .expire_heat(SystemTime::now() - Duration::from_secs(5))
            .expect("the planted row expires");
        assert_eq!(
            tier.index()
                .heat_last_read(&data(), &key("promoted.bin"))
                .expect("the heat row reads"),
            None,
            "the promoted key has no heat evidence left"
        );

        let summary = mover.pass().await.expect("the pass runs");

        assert_eq!(summary.promoted, 1, "the queued key is promoted");
        assert_eq!(
            summary.idle_demoted, 0,
            "the pass's exclusion keeps the promotion warm"
        );
        assert_eq!(summary.demoted, 0);
        assert_eq!(class(&tier, "promoted.bin").await, Class::Standard);
    }

    /// A durable heat row outlives a heat window shorter than `demote_idle`:
    /// the object it protects stays warm, while a row past the idle threshold
    /// still expires.
    #[tokio::test]
    async fn a_read_inside_a_long_idle_threshold_survives_the_heat_window() {
        let mut config = config();
        config.heat_window = Duration::from_millis(200);
        config.demote_idle = Duration::from_secs(3600);
        config.stats_flush = Duration::ZERO;
        let (_root, tier) = tier_with(config).await;
        put(&tier, "kept.bin").await;
        put(&tier, "lost.bin").await;
        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        let now = SystemTime::now();
        // Both candidates are older than the idle threshold; the durable rows
        // alone decide which one counts as read.
        let aged = now - Duration::from_secs(2 * 3600);
        for name in ["kept.bin", "lost.bin"] {
            tier.index()
                .warm_replace(&data(), &key(name), aged)
                .expect("the candidate is planted");
        }
        tier.index()
            .absorb_heat(&[
                HeatEntry {
                    bucket: data(),
                    key: key("kept.bin"),
                    reads: 1,
                    last_read: now - Duration::from_secs(600),
                },
                HeatEntry {
                    bucket: data(),
                    key: key("lost.bin"),
                    reads: 1,
                    last_read: now - Duration::from_secs(2 * 3600),
                },
            ])
            .expect("the durable rows are planted");

        mover.flush_stats().expect("the flush runs");

        assert!(
            tier.index()
                .heat_last_read(&data(), &key("kept.bin"))
                .expect("the row reads")
                .is_some(),
            "a row inside demote_idle survives the shorter heat window"
        );
        assert_eq!(
            tier.index()
                .heat_last_read(&data(), &key("lost.bin"))
                .expect("the row reads"),
            None,
            "a row past demote_idle expires"
        );

        let summary = mover.pass().await.expect("the pass runs");

        assert_eq!(
            summary.idle_demoted, 1,
            "only the unprotected object is idle"
        );
        assert_eq!(class(&tier, "kept.bin").await, Class::Standard);
        assert_eq!(class(&tier, "lost.bin").await, Class::StandardIa);
    }

    /// A failed absorb forgets nothing: the counter stays visible, so an idle
    /// check landing after the failure still sees the read.
    #[test]
    fn a_failed_absorb_keeps_the_counter_visible() {
        let root = tempfile::tempdir().expect("a temporary root");
        let path = root.path().join("tier.redb");
        drop(Index::open(&path).expect("the index is created"));
        let index = Index::open_read_only(&path).expect("the read-only index opens");
        let fs = FsStorage::new(root.path(), testing::fs_options()).expect("the fs store opens");
        let tier = TierStorage::from_parts(
            Arc::new(fs),
            RedbContent::in_memory().expect("an in-memory cold plane"),
            Arc::new(index),
            config(),
        );
        tier.heat().record_read(&data(), &key("read.bin"));
        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);

        let refused = mover.flush_stats();

        assert!(refused.is_err(), "a read-only index refuses the absorb");
        assert!(
            tier.heat().last_read(&data(), &key("read.bin")).is_some(),
            "the failed flush leaves the counter visible"
        );
        // A total failure commits nothing, so the retry re-sends the batch.
        let report = tier.index().absorb_heat_report(
            &[HeatEntry {
                bucket: data(),
                key: key("read.bin"),
                reads: 1,
                last_read: SystemTime::now(),
            }],
            SystemTime::now(),
        );
        assert_eq!(report.committed, 0);
        assert!(report.error.is_some(), "the read-only index refuses it");
    }

    /// A read that lands between the snapshot and the forget is stored once,
    /// not twice: the retained counter carries only its unabsorbed delta.
    #[tokio::test]
    async fn a_read_during_the_flush_is_absorbed_once() {
        let (_root, tier) = tier().await;
        let hot = key("hot.bin");
        tier.heat().record_read(&data(), &hot);
        let taken = tier.heat().snapshot();
        let rows: Vec<HeatEntry> = taken
            .iter()
            .map(|snapshot| snapshot.entry.clone())
            .collect();
        tier.index()
            .absorb_heat(&rows)
            .expect("the first batch is stored");

        // The read the two-phase flush exists to protect.
        tier.heat().record_read(&data(), &hot);
        tier.heat().forget(&taken);

        let next = tier.heat().snapshot();
        let rows: Vec<HeatEntry> = next.iter().map(|snapshot| snapshot.entry.clone()).collect();
        tier.index()
            .absorb_heat(&rows)
            .expect("the second batch is stored");
        tier.heat().forget(&next);

        let stored = tier.index().scan_heat(10).expect("the scan runs");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].reads, 2, "two reads are stored once each");
    }

    /// A counter whose window elapsed between the snapshot and the forget was
    /// reset by its new read: the forget keeps that read whole, and the next
    /// flush stores it once.
    #[tokio::test]
    async fn a_reset_counter_keeps_its_read_across_the_forget() {
        let mut config = config();
        // Wide enough that the new read, the score and the flush stay inside
        // the window even on a loaded host; the sleep below is what forces the
        // reset, so it must exceed the window.
        config.heat_window = Duration::from_secs(5);
        config.stats_flush = Duration::ZERO;
        let (_root, tier) = tier_with(config).await;
        let hot = key("hot.bin");
        tier.heat().record_read(&data(), &hot);
        let taken = tier.heat().snapshot();
        let rows: Vec<HeatEntry> = taken
            .iter()
            .map(|snapshot| snapshot.entry.clone())
            .collect();
        tier.index()
            .absorb_heat(&rows)
            .expect("the first batch is stored");

        // The counter's window elapses before the forget; the new read resets
        // it instead of adding to the absorbed count.
        sleep(Duration::from_millis(5100)).await;
        tier.heat().record_read(&data(), &hot);
        tier.heat().forget(&taken);

        assert_eq!(
            tier.heat().score(&data(), &hot),
            1,
            "the reset counter keeps its new read"
        );

        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.flush_stats().expect("the flush runs");

        let stored = tier.index().scan_heat(10).expect("the scan runs");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].reads, 1, "the new window's read is stored once");
    }

    /// A partial absorb forgets only its committed prefix, so the retry stores
    /// the remaining rows once and never re-adds what already landed.
    #[tokio::test]
    async fn a_partial_absorb_is_retried_without_recounting() {
        let (_root, tier) = tier().await;
        tier.heat().record_read(&data(), &key("first.bin"));
        tier.heat().record_read(&data(), &key("second.bin"));
        let taken = tier.heat().snapshot();
        assert_eq!(taken.len(), 2, "both counters are in the batch");

        // Composed coverage of the partial-failure contract (two rows cannot
        // straddle a 512-row chunk in a real run): the first row stands for a
        // committed chunk, and forgetting it stands for the prefix the flush
        // retires before retrying the rest.
        let committed = [taken[0].entry.clone()];
        tier.index()
            .absorb_heat(&committed)
            .expect("the first row is committed");
        tier.heat().forget(&taken[..1]);

        // The retry is the ordinary flush: it stores the rest and forgets it.
        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.flush_stats().expect("the retry runs");

        let stored = tier.index().scan_heat(10).expect("the scan runs");
        assert_eq!(stored.len(), 2);
        assert!(
            stored.iter().all(|row| row.reads == 1),
            "each read is stored once: {stored:?}"
        );
    }

    /// A durable row kept alive by the longer idle retention does not combine
    /// with a fresh read: the absorb starts a new window, so the stored total
    /// cannot re-enter the promotion queue on old reads.
    #[tokio::test]
    async fn an_old_heat_row_starts_a_fresh_window() {
        let mut config = config();
        // Wide enough that the flush and the bind that follows it stay inside
        // the window even on a loaded host.
        config.heat_window = Duration::from_secs(5);
        config.demote_idle = Duration::from_secs(3600);
        config.stats_flush = Duration::ZERO;
        let (_root, tier) = tier_with(config).await;
        put(&tier, "hot.bin").await;
        let moved = tier
            .demote(&data(), &key("hot.bin"), SystemTime::now())
            .await
            .expect("the demote runs");
        assert!(
            matches!(moved, MoveOutcome::Moved { .. }),
            "the fixture object is cold"
        );
        // A row kept alive by the idle retention, well outside the heat
        // window, already above the promotion threshold.
        tier.index()
            .absorb_heat(&[HeatEntry {
                bucket: data(),
                key: key("hot.bin"),
                reads: 3,
                last_read: SystemTime::now() - Duration::from_secs(20),
            }])
            .expect("the old heat row is planted");
        tier.heat().record_read(&data(), &key("hot.bin"));
        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);

        mover.flush_stats().expect("the flush runs");

        let stored = tier.index().scan_heat(10).expect("the scan runs");
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored[0].reads, 1,
            "the fresh read starts a new window instead of adding to the old total"
        );

        // The post-bind restore reads the durable total: one read cannot
        // re-enter the promotion queue, where the old total could.
        mover.bind().await.expect("the warm table is rebuilt");
        assert!(
            tier.heat().promote_queue().is_empty(),
            "old reads do not queue a promotion"
        );
    }

    /// The scan is mtime-ordered and stops at the first row inside the
    /// threshold, so a genuinely newer object is never offered as an idle
    /// candidate even though it sits behind the older one in the table.
    #[tokio::test]
    async fn the_idle_scan_stops_at_the_first_object_inside_the_threshold() {
        let (_root, tier) = tier().await;
        put(&tier, "older.bin").await;
        put(&tier, "newer.bin").await;
        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        // Bind rewrote both candidates at their row times, so the ordering is
        // planted after it: the older row first, a genuinely newer row behind
        // it, both in the same bucket.
        let now = SystemTime::now();
        tier.index()
            .warm_replace(&data(), &key("older.bin"), now - 2 * IDLE)
            .expect("the older candidate is planted");
        tier.index()
            .warm_replace(&data(), &key("newer.bin"), now - IDLE / 2)
            .expect("the newer candidate is planted");

        let mut versioned = VersionMemo::new();
        let offered = mover
            .idle_candidates(&HashSet::new(), IDLE, &mut versioned)
            .await
            .expect("the selection runs");

        assert_eq!(offered.len(), 1, "only the older row is offered");
        assert_eq!(offered[0].key, key("older.bin"));
        assert!(
            offered
                .iter()
                .all(|candidate| candidate.key != key("newer.bin")),
            "the newer row is never an idle candidate"
        );

        let summary = mover.pass().await.expect("the pass runs");

        assert_eq!(summary.idle_demoted, 1, "only the older row is demoted");
        assert_eq!(class(&tier, "older.bin").await, Class::StandardIa);
        assert_eq!(class(&tier, "newer.bin").await, Class::Standard);
    }

    /// A full page of ineligible candidates in front of the only eligible row
    /// widens the scan instead of hiding that row behind the page cut.
    #[tokio::test]
    async fn the_idle_scan_widens_past_ineligible_candidates() {
        let (_root, tier) = tier().await;
        let versioned = bucket::name("versioned").expect("a valid bucket name");
        tier.create_bucket(&versioned, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        tier.set_bucket_versioning(&versioned, version::State::Enabled)
            .await
            .expect("versioning is enabled");
        put(&tier, "eligible.bin").await;
        sleep(IDLE + Duration::from_millis(200)).await;

        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.bind().await.expect("the warm table is rebuilt");
        // One more than a page of versioned-bucket candidates, all older than
        // the eligible row: the scan must widen to reach it.
        let blockers = super::WARM_PAGE + 1;
        let blocker_mtime = SystemTime::now() - 3 * IDLE;
        for index in 0..blockers {
            tier.index()
                .warm_insert(
                    &versioned,
                    &object::key(format!("blocker-{index:04}")).expect("a valid object key"),
                    blocker_mtime,
                )
                .expect("the blocker candidate is planted");
        }

        let summary = mover.pass().await.expect("the pass runs");

        assert_eq!(
            summary.idle_demoted, 1,
            "the eligible row behind the full page is found"
        );
        assert_eq!(class(&tier, "eligible.bin").await, Class::StandardIa);
        assert_eq!(
            tier.index()
                .scan_warm(usize::MAX)
                .expect("the scan runs")
                .len(),
            blockers,
            "the versioned bucket's candidates are left in place"
        );
    }

    /// Plant one key's heat in every half a mutation must clear: a durable row,
    /// an in-memory counter, and the promotion crossing that queues it.
    fn plant_heat(tier: &TierStorage<RedbContent>, name: &str) -> object::Key {
        let key = key(name);
        tier.index()
            .absorb_heat(&[HeatEntry {
                bucket: data(),
                key: key.clone(),
                reads: 4,
                last_read: SystemTime::now(),
            }])
            .expect("the durable heat row is planted");
        for _ in 0..config().promote_reads {
            tier.heat().record_read(&data(), &key);
        }
        key
    }

    /// Assert the mutation left no heat evidence: no in-memory counter, no
    /// queued promotion and no durable row.
    fn assert_heat_gone(tier: &TierStorage<RedbContent>, key: &object::Key) {
        assert_eq!(tier.heat().score(&data(), key), 0, "no in-memory counter");
        assert!(
            !tier
                .heat()
                .promote_queue()
                .iter()
                .any(|(_, queued)| queued == key),
            "no queued promotion"
        );
        assert_eq!(
            tier.index()
                .heat_last_read(&data(), key)
                .expect("the durable row reads"),
            None,
            "no durable heat row"
        );
    }

    /// Run the stats flush after the mutation and re-assert, so no snapshot
    /// taken by a later pass resurrects the heat the mutation removed.
    fn assert_flush_does_not_resurrect(tier: &TierStorage<RedbContent>, key: &object::Key) {
        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        mover.flush_stats().expect("the stats flush runs");
        assert_heat_gone(tier, key);
    }

    /// An overwrite forgets the key's heat in all three halves (T9).
    #[tokio::test]
    async fn an_overwrite_forgets_the_keys_heat() {
        let (_root, tier) = tier().await;
        put(&tier, "hot.bin").await;
        let hot = plant_heat(&tier, "hot.bin");

        put(&tier, "hot.bin").await;

        assert_heat_gone(&tier, &hot);
        assert_flush_does_not_resurrect(&tier, &hot);
    }

    /// A delete forgets the key's heat in all three halves (T9).
    #[tokio::test]
    async fn a_delete_forgets_the_keys_heat() {
        let (_root, tier) = tier().await;
        put(&tier, "hot.bin").await;
        let hot = plant_heat(&tier, "hot.bin");

        tier.delete_object(&data(), &hot)
            .await
            .expect("the object is deleted");

        assert_heat_gone(&tier, &hot);
        assert_flush_does_not_resurrect(&tier, &hot);
    }

    /// A copy forgets the destination key's heat in all three halves (T9).
    #[tokio::test]
    async fn a_copy_forgets_the_destinations_heat() {
        let (_root, tier) = tier().await;
        put(&tier, "source.bin").await;
        let destination = plant_heat(&tier, "destination.bin");

        tier.copy_object(
            &data(),
            &key("source.bin"),
            &data(),
            &destination,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            None,
        )
        .await
        .expect("the copy settles");

        assert_heat_gone(&tier, &destination);
        assert_flush_does_not_resurrect(&tier, &destination);
    }

    /// A completed multipart upload forgets the key's heat in all three halves
    /// (T9).
    #[tokio::test]
    async fn a_multipart_complete_forgets_the_keys_heat() {
        let (_root, tier) = tier().await;
        let hot = plant_heat(&tier, "multipart.bin");
        let upload = tier
            .create_multipart_upload(
                &data(),
                &hot,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .expect("the upload starts");
        let part = tier
            .upload_part(
                &data(),
                &hot,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body(b"hi"),
                None,
            )
            .await
            .expect("the part uploads");

        tier.complete_multipart_upload(
            &data(),
            &hot,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag,
            }],
            None,
        )
        .await
        .expect("the upload completes");

        assert_heat_gone(&tier, &hot);
        assert_flush_does_not_resurrect(&tier, &hot);
    }

    /// A tag write is a mutation too: it forgets the key's heat in all three
    /// halves (T9), and a later stats flush cannot resurrect the durable row.
    #[tokio::test]
    async fn a_tag_write_forgets_the_keys_heat() {
        let (_root, tier) = tier().await;
        put(&tier, "tagged.bin").await;
        let hot = plant_heat(&tier, "tagged.bin");

        tier.put_object_tags(&data(), &hot, &object::Tags::empty())
            .await
            .expect("the tag write settles");

        assert_heat_gone(&tier, &hot);
        assert_flush_does_not_resurrect(&tier, &hot);
    }

    /// An ACL write forgets the key's heat in all three halves (T9), so the
    /// reads recorded against the bytes the mutation replaced cannot promote
    /// it afterwards.
    #[tokio::test]
    async fn an_acl_write_forgets_the_keys_heat() {
        let (_root, tier) = tier().await;
        put(&tier, "acl.bin").await;
        let hot = plant_heat(&tier, "acl.bin");

        tier.put_object_acl(&data(), &hot, &Acl::default_private(None).grants)
            .await
            .expect("the ACL write settles");

        assert_heat_gone(&tier, &hot);
        assert_flush_does_not_resurrect(&tier, &hot);
    }

    /// Two callers that reach the flush together cannot absorb the same
    /// counters twice: the flush gate serializes snapshot, absorb and forget,
    /// so every counter reaches the durable table exactly once. A zero
    /// `stats_flush` makes both callers due; a gate-less flush would let the
    /// second snapshot the counters the first is still writing.
    #[tokio::test]
    async fn two_concurrent_stats_flushes_absorb_each_counter_once() {
        const COUNTERS: u32 = 1_000;
        let (_root, tier) = tier_with(Config {
            stats_flush: Duration::ZERO,
            ..config()
        })
        .await;
        for n in 0..COUNTERS {
            tier.heat()
                .record_read(&data(), &key(&format!("hot-{n:04}")));
        }

        let mover = Mover::new(tier.clone(), Arc::new(Quiet), 1);
        let barrier = Barrier::new(2);
        thread::scope(|scope| {
            for _ in 0..2 {
                let mover = &mover;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    mover.flush_stats().expect("the stats flush runs");
                });
            }
        });

        let rows = tier
            .index()
            .scan_heat(usize::MAX)
            .expect("the heat scan runs");
        let absorbed: u64 = rows.iter().map(|row| row.reads).sum();
        assert_eq!(
            rows.len(),
            COUNTERS as usize,
            "every counter reached the durable table"
        );
        assert_eq!(
            absorbed,
            u64::from(COUNTERS),
            "each counter was absorbed exactly once"
        );
        assert_eq!(
            tier.heat().score(&data(), &key("hot-0000")),
            0,
            "the flush forgot the counters it absorbed"
        );
    }

    /// The collection pass books the orphan it can observe — a live cold
    /// placement whose object row is gone while its copy survives — as the
    /// absolute count on the gauge, which is all a served `/metrics` can show.
    #[tokio::test]
    async fn gc_books_the_orphan_it_observes() {
        let _window = test_lock::window();
        let (_root, tier) = tier().await;
        put(&tier, "orphan.bin").await;
        let now = SystemTime::now();
        let moved = tier
            .demote(&data(), &key("orphan.bin"), now)
            .await
            .expect("the demote runs");
        assert!(
            matches!(moved, MoveOutcome::Moved { .. }),
            "the fixture object is cold"
        );
        // The row goes; the placement and the cold copy stay behind.
        tier.fs()
            .delete_object(&data(), &key("orphan.bin"))
            .await
            .expect("the row is deleted under the placement");
        let mut placement = tier
            .index()
            .placement(&data(), &key("orphan.bin"))
            .expect("the placement reads")
            .expect("the placement exists");
        placement.mtime = now - Duration::from_secs(301);
        tier.index()
            .set_placement(&data(), &key("orphan.bin"), &placement)
            .expect("the placement ages past the grace");

        let summary = tier.gc(now).await.expect("the collection runs");

        assert_eq!(
            summary.orphans, 1,
            "the pass observed the cold copy with no row behind it"
        );
        assert_eq!(
            gauge("tinio_tier_orphans", &[]),
            1.0,
            "the gauge is set to the absolute count the pass observed"
        );
    }
}
