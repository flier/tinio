//! The live-set sweep: reclaim addresses no reference points at.
//!
//! `LiveSet` and `SweepReport` are defined here — the reachability input the
//! sweep tests blocks against, and what one sweep reclaimed — with
//! `SweepCandidates` and the `Store` methods that plan a sweep
//! (`Store::sweep_candidates`) and apply it (`Store::remove_candidates`).

use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};

use futures::StreamExt;
use smart_default::SmartDefault;

use crate::{
    _core::blob::{BlobRef, BlockHash, BlockStore, Entry},
    Error,
};

/// The blocks a sweep must keep: the union of the block lists of every live
/// version. Built by the caller — the engine does not know what a version is.
///
/// # Examples
///
/// ```rust
/// use tinio_blob::LiveSet;
/// use tinio_core::blob::BlockHash;
///
/// let live = LiveSet::from_hashes([BlockHash::of(b"hello")]);
/// assert!(live.contains(&BlockHash::of(b"hello")));
/// assert_eq!(live.len(), 1);
/// ```
#[derive(Debug, Clone, SmartDefault)]
pub struct LiveSet {
    hashes: HashSet<BlockHash>,
}

impl LiveSet {
    /// Build the set from the live references' block lists.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::LiveSet;
    /// use tinio_core::blob::BlockHash;
    ///
    /// let live = LiveSet::from_hashes([BlockHash::of(b"one"), BlockHash::of(b"two")]);
    ///
    /// assert_eq!(live.len(), 2);
    /// assert!(live.contains(&BlockHash::of(b"two")));
    /// ```
    pub fn from_hashes(hashes: impl IntoIterator<Item = BlockHash>) -> Self {
        Self {
            hashes: hashes.into_iter().collect(),
        }
    }

    /// Build the set from a version history: the union of every row's blob
    /// block list. One home for the reduction the engine's `live_blocks`
    /// walk and the doctor's already-decoded rows both feed — the two can
    /// never disagree about what is live.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::LiveSet;
    /// use tinio_core::blob::{BlobRef, BlockHash};
    ///
    /// let blob = BlobRef::single(BlockHash::of(b"hello"), 5);
    /// let live = LiveSet::from_blobs([blob]);
    /// assert!(live.contains(&BlockHash::of(b"hello")));
    /// ```
    pub fn from_blobs(blobs: impl IntoIterator<Item = BlobRef>) -> Self {
        // The set is built in place: the `flat_map` + `collect` shape this
        // replaced allocated one `Vec` per blob, which is one heap
        // allocation per live version on the sweep's setup path.
        let mut hashes = HashSet::new();
        for blob in blobs {
            for block in blob.blocks() {
                hashes.insert(block.hash);
            }
        }
        Self::from_hashes(hashes)
    }

    /// Whether the sweep must keep this block.
    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.hashes.contains(hash)
    }

    /// How many blocks the set holds.
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }
}

/// What a sweep did.
///
/// # Examples
///
/// ```rust
/// use tinio_blob::SweepReport;
///
/// let report = SweepReport::default();
/// assert_eq!(report.removed, 0);
/// assert_eq!(report.bytes, 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault)]
pub struct SweepReport {
    /// Blocks examined.
    pub scanned: u64,
    /// Blocks kept because a live reference points at them.
    pub kept_live: u64,
    /// Blocks kept because they are younger than the grace.
    pub kept_young: u64,
    /// Blocks removed.
    pub removed: u64,
    /// Bytes removed.
    pub bytes: u64,
    /// Blocks whose removal failed (a sharing violation, a read-only
    /// residue): the pass logs and continues, and the next sweep retries
    /// them.
    pub failed: u64,
}

/// The blocks a sweep's walk judged reclaimable — its plan, before any
/// removal — with what the walk measured on the way: the counts and the
/// store's total byte size at walk time. Produced by
/// [`crate::Store::sweep_candidates`] and applied by
/// [`crate::Store::remove_candidates`]; a caller whose live set can go
/// stale between the two calls validates that itself before removing (the
/// fs backend checks a mutation epoch under its bucket lock).
///
/// # Examples
///
/// ```rust
/// use tinio_blob::SweepCandidates;
///
/// let candidates = SweepCandidates::default();
/// assert!(candidates.entries.is_empty());
/// assert_eq!(candidates.scanned, 0);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepCandidates {
    /// The reclaimable blocks, in the walk's order.
    pub entries: Vec<Entry>,
    /// Blocks examined.
    pub scanned: u64,
    /// Blocks kept because the live set holds them.
    pub kept_live: u64,
    /// Blocks kept because they are younger than the grace.
    pub kept_young: u64,
    /// The total bytes the store held when it was walked — every scanned
    /// entry's length, kept or reclaimable. The sweep's size gauge reads it
    /// off the walk that already stat'ed every block.
    pub bytes_total: u64,
}

impl<S: BlockStore> super::Store<S> {
    /// Remove every block that no live reference points at and that was
    /// published at least `min_age` before `now`.
    ///
    /// The grace narrows the window against a publish that happened after the
    /// caller computed `live` — a block written moments ago is not yet
    /// referenced, and deleting it would drop content a version row is about to
    /// name — but it is **not** the race guard the design pins (a re-check under
    /// the bucket mutation lock, or a mark epoch) and does not stand in for it:
    /// a dedup hit leaves the target block's timestamp untouched, so a publish
    /// that returns
    /// [`Published::Existing`](tinio_core::blob::Published::Existing) can
    /// commit a row naming a block older than the grace, which a sweep holding
    /// a stale live set then deletes. Only a live set that cannot be stale
    /// closes that window.
    ///
    /// This method is the composed pair [`Self::sweep_candidates`] +
    /// [`Self::remove_candidates`]; it validates nothing between them, so a
    /// caller whose live set can go stale while the walk runs drives the two
    /// phases itself and validates before removing (the fs backend checks a
    /// mutation epoch under the bucket mutation lock).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_blob::{LiveSet, LocalBlocks, Store};
    /// use tinio_util::testing::body;
    /// use tokio::runtime::Runtime;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = Store::new(LocalBlocks::new(dir.path()));
    /// let runtime = Runtime::new().unwrap();
    /// let (keep, drop) = runtime.block_on(async {
    ///     let keep = store.put(body(b"keep")).await.unwrap();
    ///     let drop = store.put(body(b"drop")).await.unwrap();
    ///     (keep, drop)
    /// });
    ///
    /// // Only `keep` is live, and the clock is an hour past the grace, so
    /// // `drop` is the one block the sweep reclaims.
    /// let live = LiveSet::from_hashes([*keep.hash()]);
    /// let report = runtime
    ///     .block_on(store.sweep(
    ///         &live,
    ///         Duration::from_secs(600),
    ///         SystemTime::now() + Duration::from_secs(3600),
    ///     ))
    ///     .unwrap();
    ///
    /// assert_eq!(report.scanned, 2);
    /// assert_eq!(report.kept_live, 1);
    /// assert_eq!(report.kept_young, 0);
    /// assert_eq!(report.removed, 1);
    /// assert_eq!(report.bytes, 4);
    /// ```
    pub async fn sweep(
        &self,
        live: &LiveSet,
        min_age: Duration,
        now: SystemTime,
    ) -> Result<SweepReport, Error> {
        let candidates = self.sweep_candidates(live, min_age, now).await?;
        self.remove_candidates(&candidates).await
    }

    /// Walk the whole store and collect the blocks no entry of `live`
    /// references and that were published at least `min_age` before `now` —
    /// the sweep's plan, computed without removing anything.
    ///
    /// The two-phase shape exists for callers whose live set can go stale
    /// between the walk and the removal: those validate between
    /// [`Self::sweep_candidates`] and [`Self::remove_candidates`]. The walk
    /// streams: the store's listing is consumed entry by entry and never
    /// held whole, and only the reclaimable entries are retained.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_blob::{LiveSet, LocalBlocks, Store};
    ///
    /// # async fn example() -> Result<(), tinio_blob::Error> {
    /// let store = Store::new(LocalBlocks::new("/var/lib/tinio/blobs/demo"));
    /// let candidates = store
    ///     .sweep_candidates(
    ///         &LiveSet::default(),
    ///         Duration::from_secs(600),
    ///         SystemTime::now(),
    ///     )
    ///     .await?;
    /// assert_eq!(candidates.scanned, 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sweep_candidates(
        &self,
        live: &LiveSet,
        min_age: Duration,
        now: SystemTime,
    ) -> Result<SweepCandidates, Error> {
        let mut candidates = SweepCandidates::default();
        // One walk of the whole store. An entry is judged before the next is
        // asked for, so the store's listing is consumed as a stream and never
        // held whole.
        let mut walk = self
            .blocks()
            .walk(None)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        while let Some(entry) = walk.next().await {
            let entry = entry.map_err(|err| Error::Storage(err.into()))?;
            candidates.scanned += 1;
            candidates.bytes_total += entry.len;
            if live.contains(&entry.hash) {
                candidates.kept_live += 1;
                continue;
            }
            let young = now
                .duration_since(entry.published)
                .map(|age| age < min_age)
                .unwrap_or(true);
            if young {
                candidates.kept_young += 1;
                continue;
            }
            candidates.entries.push(entry);
        }
        Ok(candidates)
    }

    /// Remove every block the walk judged reclaimable.
    ///
    /// The caller owns staleness: a reference committed after the walk made
    /// one of these entries live again, and this method does not re-check —
    /// the caller must have validated its live view between the two calls
    /// (the fs backend checks a mutation epoch under the bucket mutation
    /// lock, which every reference-adding path holds across its block
    /// publish and its row commit). A removal that fails (a sharing
    /// violation, a read-only residue) is logged and counted in
    /// [`SweepReport::failed`]; the pass continues, and the next one
    /// retries it.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_blob::{LiveSet, LocalBlocks, Store};
    ///
    /// # async fn example() -> Result<(), tinio_blob::Error> {
    /// let store = Store::new(LocalBlocks::new("/var/lib/tinio/blobs/demo"));
    /// let live = LiveSet::default();
    /// let candidates = store
    ///     .sweep_candidates(&live, Duration::from_secs(600), SystemTime::now())
    ///     .await?;
    /// let report = store.remove_candidates(&candidates).await?;
    /// assert_eq!(report.failed, 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_candidates(
        &self,
        candidates: &SweepCandidates,
    ) -> Result<SweepReport, Error> {
        let mut report = SweepReport {
            scanned: candidates.scanned,
            kept_live: candidates.kept_live,
            kept_young: candidates.kept_young,
            ..Default::default()
        };
        for entry in &candidates.entries {
            match self.blocks().remove(&entry.hash).await {
                Ok(()) => {
                    report.removed += 1;
                    report.bytes += entry.len;
                }
                Err(err) => {
                    // One undeletable block must not abort the pass: the
                    // sweep's other sections (and the next pass) still have
                    // work to do, and a transient sharing violation clears
                    // on its own.
                    report.failed += 1;
                    tracing::warn!(
                        hash = %entry.hash,
                        error = %err,
                        "a dead block could not be removed; the next sweep retries"
                    );
                }
            }
        }
        tracing::debug!(
            scanned = report.scanned,
            kept_live = report.kept_live,
            kept_young = report.kept_young,
            removed = report.removed,
            bytes = report.bytes,
            failed = report.failed,
            "block sweep finished"
        );
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tinio_util::testing::body;

    use super::*;
    use crate::{
        _core::{
            BodyStream, ByteRange,
            blob::{BlockStore, EntryStream, Published},
        },
        LocalBlocks, Store,
    };

    /// A store that refuses to remove one block — the undeletable-block case
    /// (a sharing violation, a read-only residue). Every other operation
    /// delegates to the inner store.
    struct Flaky {
        inner: LocalBlocks,
        fail: BlockHash,
    }

    #[async_trait::async_trait]
    impl BlockStore for Flaky {
        type Error = Error;
        type Staged = <LocalBlocks as BlockStore>::Staged;

        async fn stage(&self) -> Result<Self::Staged, Self::Error> {
            self.inner.stage().await
        }

        async fn publish(
            &self,
            staged: Self::Staged,
            hash: &BlockHash,
        ) -> Result<Published, Self::Error> {
            self.inner.publish(staged, hash).await
        }

        async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error> {
            self.inner.contains(hash).await
        }

        async fn open(
            &self,
            hash: &BlockHash,
            range: Option<ByteRange>,
        ) -> Result<BodyStream, Self::Error> {
            self.inner.open(hash, range).await
        }

        async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
            if *hash == self.fail {
                return Err(Error::Io(io::Error::other("the block is locked")));
            }
            self.inner.remove(hash).await
        }

        async fn list(
            &self,
            after: Option<&BlockHash>,
            limit: usize,
        ) -> Result<Vec<Entry>, Self::Error> {
            self.inner.list(after, limit).await
        }

        async fn walk(
            &self,
            after: Option<&BlockHash>,
        ) -> Result<EntryStream<Self::Error>, Self::Error> {
            self.inner.walk(after).await
        }
    }

    /// One undeletable block must not abort the pass: it is counted in
    /// [`SweepReport::failed`] and the other dead blocks still go.
    #[tokio::test]
    async fn an_undeletable_block_is_counted_and_does_not_abort_the_pass() {
        let dir = tempfile::tempdir().unwrap();
        let stuck = BlockHash::of(b"stuck");
        let doomed = BlockHash::of(b"doomed");
        let store = Store::new(Flaky {
            inner: LocalBlocks::new(dir.path()),
            fail: stuck,
        });
        let live = LiveSet::default();
        let now = SystemTime::now() + Duration::from_secs(3600);
        store.put(body(b"stuck")).await.unwrap();
        store.put(body(b"doomed")).await.unwrap();

        let report = store
            .sweep(&live, Duration::from_secs(600), now)
            .await
            .unwrap();

        assert_eq!(report.failed, 1, "the locked block is reported: {report:?}");
        assert_eq!(report.removed, 1, "the deletable block still goes");
        assert!(store.blocks().inner.contains(&stuck).await.unwrap());
        assert!(!store.blocks().inner.contains(&doomed).await.unwrap());
    }

    #[tokio::test]
    async fn a_sweep_removes_unreferenced_blocks_once_they_are_past_the_grace() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let keep = store.put(body(b"keep")).await.unwrap();
        let drop = store.put(body(b"drop")).await.unwrap();

        let live = LiveSet::from_hashes([*keep.hash()]);
        let now = SystemTime::now() + Duration::from_secs(3600);
        let report = store
            .sweep(&live, Duration::from_secs(600), now)
            .await
            .unwrap();

        assert_eq!(report.removed, 1);
        assert_eq!(report.bytes, 4);
        assert!(store.blocks().contains(keep.hash()).await.unwrap());
        assert!(!store.blocks().contains(drop.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn a_block_published_inside_the_grace_window_survives_a_sweep_that_does_not_know_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let fresh = store.put(body(b"fresh")).await.unwrap();

        let empty = LiveSet::from_hashes([]);
        let report = store
            .sweep(&empty, Duration::from_secs(600), SystemTime::now())
            .await
            .unwrap();

        assert_eq!(
            report.removed, 0,
            "a live set computed before the publish must not orphan it"
        );
        assert_eq!(report.kept_young, 1);
        assert!(store.blocks().contains(fresh.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn a_sweep_walks_the_whole_store_and_removes_every_stale_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        for bytes in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            store.put(body(bytes)).await.unwrap();
        }

        let live = LiveSet::from_hashes([]);
        let now = SystemTime::now() + Duration::from_secs(3600);
        // Five blocks spread over several fan-out directories: a walk that
        // stopped at the first one, or lost entries crossing a boundary, would
        // leave some behind.
        let report = store
            .sweep(&live, Duration::from_secs(600), now)
            .await
            .unwrap();

        assert_eq!(report.scanned, 5);
        assert_eq!(report.removed, 5);
        assert!(store.blocks().list(None, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_clock_behind_the_blocks_reads_an_unknown_age_as_young() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let first = store.put(body(b"first")).await.unwrap();
        let second = store.put(body(b"second")).await.unwrap();

        // `now.duration_since(published)` is `Err` whenever the clock precedes
        // the block, and an unknown age must classify as young: the other
        // direction reclaims a block no live set could have been told about,
        // which is the one outcome a sweep must never produce.
        let report = store
            .sweep(
                &LiveSet::from_hashes([]),
                Duration::from_secs(600),
                SystemTime::UNIX_EPOCH,
            )
            .await
            .unwrap();

        assert_eq!(
            report.removed, 0,
            "an unknown age is never a reason to delete"
        );
        assert_eq!(report.kept_young, 2);
        assert_eq!(report.scanned, 2);
        assert!(store.blocks().contains(first.hash()).await.unwrap());
        assert!(store.blocks().contains(second.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn the_grace_boundary_is_exclusive_so_a_block_exactly_min_age_old_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let min_age = Duration::from_secs(600);

        // The predicate is `age < min_age`, so a block exactly `min_age` old is
        // not young. `published` is read back from the store rather than
        // assumed, so the two sweeps sit exactly on the boundary instead of on
        // however much wall-clock time the publish happened to take.
        let at_boundary = store.put(body(b"at-boundary")).await.unwrap();
        let published = store.blocks().list(None, 10).await.unwrap()[0].published;
        let report = store
            .sweep(&LiveSet::from_hashes([]), min_age, published + min_age)
            .await
            .unwrap();
        assert_eq!(report.removed, 1, "age == min_age is not young");
        assert_eq!(report.kept_young, 0);
        assert!(!store.blocks().contains(at_boundary.hash()).await.unwrap());

        // One second inside the same boundary the block is young and survives.
        let inside = store.put(body(b"inside")).await.unwrap();
        let published = store.blocks().list(None, 10).await.unwrap()[0].published;
        let report = store
            .sweep(
                &LiveSet::from_hashes([]),
                min_age,
                published + min_age - Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(report.removed, 0, "one second short of the grace is young");
        assert_eq!(report.kept_young, 1);
        assert!(store.blocks().contains(inside.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn a_sweep_of_a_fully_live_store_visits_each_block_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let mut hashes = Vec::new();
        for bytes in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            hashes.push(*store.put(body(bytes)).await.unwrap().hash());
        }
        let live = LiveSet::from_hashes(hashes.clone());

        // Every block is live, so nothing is dropped and every count below is
        // the walk's own. `walk`'s cursor makes a re-visit structurally
        // impossible (each fan-out directory is read once into the stream's
        // state, so removing or keeping an entry cannot feed it back), and
        // `SweepReport` cannot name the visited set — but with all five blocks
        // known present, `scanned` and `kept_live` equal to the count is the
        // report's one way to say each was visited exactly once: a re-emitted
        // entry would push both past five.
        let report = store
            .sweep(&live, Duration::from_secs(600), SystemTime::now())
            .await
            .unwrap();

        assert_eq!(report.scanned, 5, "each block is visited exactly once");
        assert_eq!(report.kept_live, 5);
        assert_eq!(report.kept_young, 0);
        assert_eq!(report.removed, 0);
        for hash in &hashes {
            assert!(store.blocks().contains(hash).await.unwrap());
        }
    }

    #[test]
    fn an_empty_live_set_is_empty_and_holds_nothing() {
        let live = LiveSet::from_hashes([]);

        assert!(live.is_empty());
        assert_eq!(live.len(), 0);
        assert!(!live.contains(&BlockHash::of(b"hello")));
    }

    #[test]
    fn a_hash_named_twice_collapses_to_one_live_block() {
        let keep = BlockHash::of(b"keep");
        let live = LiveSet::from_hashes([keep, keep, BlockHash::of(b"other")]);

        assert!(!live.is_empty());
        assert_eq!(live.len(), 2, "a hash named twice is one live block");
        assert!(live.contains(&keep));
    }
}
